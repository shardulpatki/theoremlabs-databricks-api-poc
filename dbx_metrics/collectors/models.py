"""Models and agents collector.

Two registry sources and one heuristic.

Registry sources
----------------
- Unity Catalog: ``/api/2.1/unity-catalog/models``. Returns registered
  models whose ``securable_type`` is ``FUNCTION`` (per CLAUDE.md: "Registered
  models are FUNCTION securables — not a separate type"). This is the
  modern path and the one that works on this workspace.
- Workspace MLflow registry: ``/api/2.0/mlflow/registered-models/list``.
  Attempted for completeness, but on newer workspaces Databricks returns
  403 with an explicit "legacy workspace model registry is disabled"
  message. Captured verbatim in ``detail.workspace_registry.reason``.

Agent-endpoint heuristic
------------------------
Classifies each serving endpoint from ``prior["serving"]`` as:

- ``foundation`` — ``endpoint_type == "FOUNDATION_MODEL_API"`` (the
  ``databricks-*`` pay-per-token routes).
- ``agent`` — any ``served_entity.entity_name`` matches a UC registered
  model's ``full_name``. These are custom endpoints serving a UC model,
  which on this workspace are the ``-agent-endpoint`` named ones.
- ``custom`` — anything else (external models, custom model serving
  without a UC registry link).

Rationale for dropping the original plan's other signals:
- ``task: "llm/v1/chat"`` marks foundation models, not agents (opposite
  of the plan's guess). It gets collapsed into ``foundation`` via
  ``endpoint_type``.
- ``ai_gateway`` config doesn't appear on any endpoint on this workspace.

The heuristic is deliberately conservative: an endpoint whose
``served_entity`` points at a UC model we couldn't enumerate (e.g. UC
list was truncated) would fall into ``custom`` rather than being
guessed into ``agent``. Worth reporting but not a correctness bug.
"""

from __future__ import annotations

import logging
from collections import Counter

from ..client import ClientError

log = logging.getLogger("dbx_metrics.collectors.models")


def collect(client, config, prior=None, **kwargs) -> dict:
    prior = prior or {}

    uc_models, uc_error = _list_uc_models(client)
    uc_full_names = {m["full_name"] for m in uc_models if m.get("full_name")}

    workspace_registry = _probe_workspace_registry(client)

    serving_prior = prior.get("serving") or {}
    serving_detail = serving_prior.get("detail") or []
    classifications = [_classify_endpoint(ep, uc_full_names) for ep in serving_detail]

    class_counts: Counter = Counter(c["classification"] for c in classifications)
    agent_linked_uc = {c["linked_uc_model"] for c in classifications if c["classification"] == "agent" and c["linked_uc_model"]}

    summary = {
        "uc_model_count": len(uc_models),
        "workspace_registry_status": workspace_registry["status"],
        "workspace_registry_count": workspace_registry["count"],
        "serving_endpoints_classified": len(classifications),
        "endpoint_classification_counts": dict(class_counts),
        "agent_endpoint_count": class_counts.get("agent", 0),
        "foundation_endpoint_count": class_counts.get("foundation", 0),
        "custom_endpoint_count": class_counts.get("custom", 0),
        "uc_models_backing_agents_count": len(agent_linked_uc),
    }

    detail = {
        "uc_models": uc_models,
        "workspace_registry": workspace_registry,
        "serving_classifications": classifications,
    }

    if uc_error and not uc_models:
        return {
            "status": "unavailable",
            "reason": f"UC models list failed: {uc_error}",
            "summary": summary,
            "detail": detail,
        }

    if uc_error:
        status = "partial"
        reason = f"UC models list partial: {uc_error}"
    elif not serving_detail:
        status = "partial"
        reason = "serving prior missing or empty — classifications skipped"
    else:
        status = "available"
        reason = None

    return {"status": status, "reason": reason, "summary": summary, "detail": detail}


def _list_uc_models(client) -> tuple[list[dict], str | None]:
    """List UC registered models. Returns (models, error_reason)."""
    try:
        raw = list(client.paginate(
            "/api/2.1/unity-catalog/models",
            items_key="registered_models",
        ))
    except ClientError as exc:
        log.info("UC models list failed: HTTP %s", exc.status)
        return [], f"HTTP {exc.status}"

    out: list[dict] = []
    for m in raw:
        out.append({
            "full_name": m.get("full_name"),
            "name": m.get("name"),
            "catalog_name": m.get("catalog_name"),
            "schema_name": m.get("schema_name"),
            "owner": m.get("owner"),
            "securable_kind": m.get("securable_kind"),
            "created_at": m.get("created_at"),
            "created_by": m.get("created_by"),
            "updated_at": m.get("updated_at"),
        })
    return out, None


def _probe_workspace_registry(client) -> dict:
    """Try one cheap MLflow registry call and classify the outcome.

    Returns ``{status, count, reason}`` where status is
    ``available`` / ``disabled`` / ``unavailable``. ``disabled`` is
    distinguished from ``unavailable`` because Databricks explicitly
    tells us the legacy registry is off on modern workspaces, which is
    a different signal from "you lack permission".
    """
    try:
        resp = client.get("/api/2.0/mlflow/registered-models/list", params={"max_results": 1})
    except ClientError as exc:
        body = (exc.body or "").lower()
        if exc.status == 403 and "disabled" in body and "legacy" in body:
            return {"status": "disabled", "count": 0, "reason": "legacy workspace model registry disabled"}
        return {"status": "unavailable", "count": 0, "reason": f"HTTP {exc.status}"}
    models = resp.get("registered_models") or []
    return {"status": "available", "count": len(models), "reason": None}


def _classify_endpoint(ep: dict, uc_full_names: set[str]) -> dict:
    name = ep.get("name")
    entity_names = ep.get("served_entities") or []
    linked = next((e for e in entity_names if e and e in uc_full_names), None)

    # Primary signal is endpoint_type; task is a secondary signal kept
    # for workspaces that don't surface endpoint_type on every record.
    endpoint_type = ep.get("endpoint_type")
    task = ep.get("task")
    is_foundation = (
        endpoint_type == "FOUNDATION_MODEL_API"
        or task in ("llm/v1/chat", "llm/v1/embeddings")
    )

    if is_foundation:
        classification = "foundation"
    elif linked:
        classification = "agent"
    else:
        classification = "custom"

    return {
        "name": name,
        "classification": classification,
        "linked_uc_model": linked,
        "task": task,
        "served_entities": entity_names,
    }
