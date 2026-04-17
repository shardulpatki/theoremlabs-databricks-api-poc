"""Clusters collector.

Lists all cluster configurations from ``/api/2.1/clusters/list``
(paginated via ``page_token`` query param; response carries
``next_page_token`` and ``prev_page_token``, both empty strings at the
tail). Summarizes by state, DBR version, node type, and cluster
source (UI / JOB / API).
"""

from __future__ import annotations

import logging
from collections import Counter

from ..client import ClientError

log = logging.getLogger("dbx_metrics.collectors.clusters")

_PAGE_SIZE = 100


def collect(client, config, **kwargs) -> dict:
    try:
        clusters = list(
            client.paginate(
                "/api/2.1/clusters/list",
                items_key="clusters",
                params={"page_size": _PAGE_SIZE},
            )
        )
    except ClientError as exc:
        return {
            "status": "unavailable",
            "reason": f"clusters list failed: HTTP {exc.status}",
            "summary": {},
            "detail": [],
        }

    state_counts: Counter = Counter()
    dbr_counts: Counter = Counter()
    node_counts: Counter = Counter()
    source_counts: Counter = Counter()
    detail: list[dict] = []

    for c in clusters:
        state = c.get("state") or "UNKNOWN"
        state_counts[state] += 1
        dbr_counts[c.get("spark_version") or "unknown"] += 1
        node_counts[c.get("node_type_id") or "unknown"] += 1
        source_counts[c.get("cluster_source") or "unknown"] += 1
        detail.append({
            "cluster_id": c.get("cluster_id"),
            "cluster_name": c.get("cluster_name"),
            "state": state,
            "spark_version": c.get("spark_version"),
            "node_type_id": c.get("node_type_id"),
            "driver_node_type_id": c.get("driver_node_type_id"),
            "cluster_source": c.get("cluster_source"),
            "autotermination_minutes": c.get("autotermination_minutes"),
            "num_workers": c.get("num_workers"),
        })

    summary = {
        "total": len(clusters),
        "running": state_counts.get("RUNNING", 0),
        "terminated": state_counts.get("TERMINATED", 0),
        "pending": state_counts.get("PENDING", 0),
        "state_counts": dict(state_counts),
        "dbr_versions": dict(dbr_counts),
        "node_types": dict(node_counts),
        "cluster_sources": dict(source_counts),
    }

    return {
        "status": "available",
        "reason": None,
        "summary": summary,
        "detail": detail,
    }
