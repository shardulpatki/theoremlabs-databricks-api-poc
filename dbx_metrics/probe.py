"""Capability probe.

One cheap call per domain to classify the workspace's effective
permissions before running the full collectors. Failed probes let us
return ``status="unavailable"`` without consuming the collector's time
budget.
"""

from __future__ import annotations

import logging

from .client import AuthError, ClientError, DatabricksClient

log = logging.getLogger("dbx_metrics.probe")


_PROBES: dict[str, tuple[str, dict | None]] = {
    "jobs": ("/api/2.1/jobs/list", {"limit": 1}),
    "clusters": ("/api/2.0/clusters/list-node-types", None),
    "serving": ("/api/2.0/serving-endpoints", None),
    "unity_catalog": ("/api/2.1/unity-catalog/metastore_summary", None),
    # UC models, not MLflow: the legacy workspace model registry is
    # disabled on newer workspaces (including this one — /api/2.0/mlflow
    # returns 403 with "legacy workspace model registry is disabled").
    # UC models is the modern path. The collector still *tries* MLflow
    # as a secondary source for completeness.
    "models": ("/api/2.1/unity-catalog/models", {"max_results": 1}),
}

_PERMISSION_SOURCES = ("jobs", "clusters", "serving", "unity_catalog")


def probe_capabilities(client: DatabricksClient) -> dict[str, dict]:
    """Return ``{domain: {"available": bool, "reason": str | None}}``.

    Raises :class:`AuthError` on 401 — bad credentials fail the whole run.
    """
    out: dict[str, dict] = {}
    for domain, (path, params) in _PROBES.items():
        try:
            client.get(path, params=params)
        except AuthError:
            raise
        except ClientError as exc:
            reason = f"HTTP {exc.status} on probe"
            log.info("probe %s -> unavailable (%s)", domain, reason)
            out[domain] = {"available": False, "reason": reason}
            continue
        except Exception as exc:
            log.warning("probe %s -> unavailable (%s)", domain, exc)
            out[domain] = {"available": False, "reason": str(exc)}
            continue
        log.info("probe %s -> available", domain)
        out[domain] = {"available": True, "reason": None}

    perm_ok = any(out.get(d, {}).get("available") for d in _PERMISSION_SOURCES)
    out["permissions"] = {
        "available": perm_ok,
        "reason": None if perm_ok else "no accessible object sources",
    }
    return out
