"""Collector orchestration.

Runs the four independent collectors in a ``ThreadPoolExecutor`` and
then invokes the two dependent collectors (``models`` and
``permissions``) with their combined results as ``prior``. ``models``
cross-references serving endpoints against UC registered models to
classify agent endpoints; ``permissions`` enumerates objects from
jobs/clusters/serving/unity_catalog. Each collector is wrapped with a
safe handler so an unexpected exception becomes ``status="unavailable"``
rather than crashing the run.
"""

from __future__ import annotations

import concurrent.futures
import logging
from typing import Callable

from .client import DatabricksClient
from .collectors import (
    clusters,
    jobs,
    models,
    permissions,
    serving,
    unity_catalog,
)
from .config import CollectorConfig
from .probe import probe_capabilities

log = logging.getLogger("dbx_metrics.orchestrator")

_INDEPENDENT_COLLECTORS: dict[str, Callable] = {
    "jobs": jobs.collect,
    "clusters": clusters.collect,
    "serving": serving.collect,
    "unity_catalog": unity_catalog.collect,
}

# Ordered: models first so permissions can see classification if ever
# useful; today permissions ignores models' output, but the order is
# stable and cheap either way.
_DEPENDENT_COLLECTORS: dict[str, Callable] = {
    "models": models.collect,
    "permissions": permissions.collect,
}

_ALL_DOMAINS = tuple(list(_INDEPENDENT_COLLECTORS.keys()) + list(_DEPENDENT_COLLECTORS.keys()))
ALL_DOMAINS = frozenset(_ALL_DOMAINS)


def run(
    client: DatabricksClient,
    config: CollectorConfig,
    only: tuple[str, ...] | None = None,
) -> dict:
    selected = frozenset(only) if only else ALL_DOMAINS
    caps = probe_capabilities(client)
    results: dict[str, dict] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {}
        for domain, fn in _INDEPENDENT_COLLECTORS.items():
            if domain not in selected:
                continue
            if not caps[domain]["available"]:
                results[domain] = _unavailable(caps[domain]["reason"])
                continue
            futures[domain] = ex.submit(_safe_collect, domain, fn, client, config)
        for domain, fut in futures.items():
            results[domain] = fut.result()

    for domain, fn in _DEPENDENT_COLLECTORS.items():
        if domain not in selected:
            continue
        cap = caps[domain]
        if cap["available"]:
            results[domain] = _safe_collect(domain, fn, client, config, prior=results)
        else:
            results[domain] = _unavailable(cap["reason"])

    return {"capabilities": caps, "domains": results}


def _safe_collect(domain: str, fn: Callable, client, config, **kwargs) -> dict:
    try:
        return fn(client, config, **kwargs)
    except Exception as exc:
        log.exception("collector %s crashed", domain)
        return _unavailable(f"unexpected error: {exc!r}")


def _unavailable(reason: str | None) -> dict:
    return {
        "status": "unavailable",
        "reason": reason,
        "summary": {},
        "detail": [],
    }
