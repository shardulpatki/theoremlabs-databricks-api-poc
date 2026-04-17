"""Permissions collector.

Normalizes grants from two distinct API surfaces into one shape. Runs
*after* the concurrent phase so it can enumerate objects from
``prior`` (the merged results of jobs/clusters/serving/unity_catalog)
rather than re-listing.

Sources
-------
- ``/api/2.0/permissions/{workspace_object_type}/{object_id}`` —
  workspace ACLs. Principal type is encoded in the field name
  (``user_name`` / ``group_name`` / ``service_principal_name``). Each
  principal carries a list of permissions, each with its own
  ``inherited`` flag and optional ``inherited_from_object`` parent path.
- ``/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}`` —
  Unity Catalog grants. Principal type is **not** distinguished on UC —
  every principal is just a string. No inherited flag either
  (inheritance lives on ``effective-permissions`` which we do not call
  to keep the call count bounded).

Output per grant (one row per ``(principal, permission)`` pair)
---------------------------------------------------------------

::

    {
        "principal":        "alice@x.com",
        "principal_type":   "user" | "group" | "service_principal" | "unknown",
        "permission_level": "CAN_MANAGE" | "SELECT" | "EXECUTE" | ...,
        "source":           "workspace" | "uc",
        "object_type":      "serving-endpoint" | "catalog" | "schema" | "table" | "function" | "job" | "cluster",
        "object_id":        "/serving-endpoints/..." | "cat.schema.table",
        "inherited":        bool | None,   # None on UC rows
        "inherited_from":   str | None,
    }

Sampling
--------
``--sample-permissions N`` caps calls per object type per surface.
Upper-bound calls per run: ``7 * N`` (3 workspace + 4 UC kinds).
"""

from __future__ import annotations

import logging
from collections import Counter

from ..client import ClientError

log = logging.getLogger("dbx_metrics.collectors.permissions")


_WORKSPACE_ACL_PRINCIPAL_FIELDS = (
    ("user_name", "user"),
    ("group_name", "group"),
    ("service_principal_name", "service_principal"),
)

_NO_GRANTS_DETAIL_CAP = 25  # limits the size of objects_with_no_grants in detail


def collect(client, config, prior=None, **kwargs) -> dict:
    prior = prior or {}
    workspace_targets = _gather_workspace_targets(prior)
    uc_targets = _gather_uc_targets(prior)

    if not workspace_targets and not uc_targets:
        return _unavailable(
            "no accessible object sources from prior results — "
            "run with at least one of jobs/clusters/serving/unity_catalog"
        )

    sample_cap = max(0, int(config.sample_permissions or 10))

    grants: list[dict] = []
    objects_sampled = {"workspace": {}, "uc": {}}
    objects_with_no_grants: list[dict] = []
    lookup_errors: list[dict] = []

    for obj_type, ids in workspace_targets.items():
        sampled = ids[:sample_cap]
        objects_sampled["workspace"][obj_type] = len(sampled)
        for obj_id in sampled:
            path = f"/api/2.0/permissions/{obj_type}/{obj_id}"
            try:
                resp = client.get(path)
            except ClientError as exc:
                lookup_errors.append({
                    "source": "workspace", "object_type": obj_type,
                    "object_id": obj_id, "status": exc.status,
                })
                log.info("workspace ACL %s -> %s", path, exc.status)
                continue
            rows = _normalize_workspace(resp, obj_type, obj_id)
            if rows:
                grants.extend(rows)
            else:
                _note_no_grants(objects_with_no_grants, "workspace", obj_type, obj_id)

    for sec_type, names in uc_targets.items():
        sampled = names[:sample_cap]
        objects_sampled["uc"][sec_type] = len(sampled)
        for full_name in sampled:
            path = f"/api/2.1/unity-catalog/permissions/{sec_type}/{full_name}"
            try:
                resp = client.get(path)
            except ClientError as exc:
                lookup_errors.append({
                    "source": "uc", "object_type": sec_type,
                    "object_id": full_name, "status": exc.status,
                })
                log.info("UC grant %s -> %s", path, exc.status)
                continue
            rows = _normalize_uc(resp, sec_type, full_name)
            if rows:
                grants.extend(rows)
            else:
                _note_no_grants(objects_with_no_grants, "uc", sec_type, full_name)

    total_sampled = sum(objects_sampled["workspace"].values()) + sum(objects_sampled["uc"].values())
    workspace_principal_type_counts: Counter = Counter()
    uc_principal_set: set[str] = set()
    for g in grants:
        if g["source"] == "workspace":
            workspace_principal_type_counts[g["principal_type"]] += 1
        else:
            uc_principal_set.add(g["principal"])

    summary = {
        "sample_cap": sample_cap,
        "objects_sampled": objects_sampled,
        "objects_sampled_total": total_sampled,
        "grants_found": {
            "workspace": sum(1 for g in grants if g["source"] == "workspace"),
            "uc": sum(1 for g in grants if g["source"] == "uc"),
        },
        "principals_by_source": {
            "workspace": dict(workspace_principal_type_counts),
            "uc_unique": len(uc_principal_set),
        },
        "permission_levels": dict(Counter(g["permission_level"] for g in grants)),
        "objects_with_no_grants_count": len(objects_with_no_grants),
        "lookup_errors_count": len(lookup_errors),
    }

    status = "partial" if lookup_errors else "available"
    reason = (
        f"{len(lookup_errors)} permissions lookup(s) failed"
        if lookup_errors else None
    )

    detail = {
        "grants": grants,
        "objects_with_no_grants": objects_with_no_grants,
        "lookup_errors": lookup_errors,
    }

    return {
        "status": status,
        "reason": reason,
        "summary": summary,
        "detail": detail,
    }


def _gather_workspace_targets(prior: dict) -> dict[str, list[str]]:
    """Map workspace API path segment -> list of object IDs from prior results."""
    out: dict[str, list[str]] = {}

    jobs_detail = (prior.get("jobs") or {}).get("detail") or []
    job_ids = [str(j["job_id"]) for j in jobs_detail if j.get("job_id") is not None]
    if job_ids:
        out["jobs"] = job_ids

    clusters_detail = (prior.get("clusters") or {}).get("detail") or []
    cluster_ids = [c["cluster_id"] for c in clusters_detail if c.get("cluster_id")]
    if cluster_ids:
        out["clusters"] = cluster_ids

    serving_detail = (prior.get("serving") or {}).get("detail") or []
    serving_ids = [e["id"] for e in serving_detail if e.get("id")]
    if serving_ids:
        out["serving-endpoints"] = serving_ids

    return out


def _gather_uc_targets(prior: dict) -> dict[str, list[str]]:
    """Map UC securable type -> list of full names from prior results."""
    out: dict[str, list[str]] = {"catalog": [], "schema": [], "table": [], "function": []}

    uc_detail = (prior.get("unity_catalog") or {}).get("detail") or []
    for cat in uc_detail:
        name = cat.get("name")
        if name:
            out["catalog"].append(name)
        for sch in cat.get("schemas") or []:
            full = sch.get("full_name")
            if full:
                out["schema"].append(full)
            for t in sch.get("tables_sample") or []:
                fn = t.get("full_name")
                if fn:
                    out["table"].append(fn)
            for f in sch.get("functions_sample") or []:
                fn = f.get("full_name")
                if fn:
                    out["function"].append(fn)

    return {k: v for k, v in out.items() if v}


def _normalize_workspace(resp: dict, queried_type: str, obj_id: str) -> list[dict]:
    rows: list[dict] = []
    # The API often normalizes object_type (e.g. "serving-endpoint" singular);
    # prefer that when present so downstream consumers see the canonical form.
    canonical_type = resp.get("object_type") or queried_type
    for ac in resp.get("access_control_list") or []:
        principal, principal_type = _extract_workspace_principal(ac)
        if principal is None:
            continue
        for perm in ac.get("all_permissions") or []:
            inherited_from = perm.get("inherited_from_object")
            rows.append({
                "principal": principal,
                "principal_type": principal_type,
                "permission_level": perm.get("permission_level"),
                "source": "workspace",
                "object_type": canonical_type,
                "object_id": obj_id,
                "inherited": bool(perm.get("inherited")),
                "inherited_from": inherited_from[0] if isinstance(inherited_from, list) and inherited_from else None,
            })
    return rows


def _extract_workspace_principal(ac_entry: dict) -> tuple[str | None, str | None]:
    for field, kind in _WORKSPACE_ACL_PRINCIPAL_FIELDS:
        value = ac_entry.get(field)
        if value:
            return value, kind
    return None, None


def _normalize_uc(resp: dict, sec_type: str, full_name: str) -> list[dict]:
    rows: list[dict] = []
    for assignment in resp.get("privilege_assignments") or []:
        principal = assignment.get("principal")
        if not principal:
            continue
        for privilege in assignment.get("privileges") or []:
            rows.append({
                "principal": principal,
                "principal_type": "unknown",
                "permission_level": privilege,
                "source": "uc",
                "object_type": sec_type,
                "object_id": full_name,
                "inherited": None,
                "inherited_from": None,
            })
    return rows


def _note_no_grants(bucket: list[dict], source: str, obj_type: str, obj_id: str) -> None:
    if len(bucket) < _NO_GRANTS_DETAIL_CAP:
        bucket.append({"source": source, "object_type": obj_type, "object_id": obj_id})


def _unavailable(reason: str) -> dict:
    return {"status": "unavailable", "reason": reason, "summary": {}, "detail": {}}
