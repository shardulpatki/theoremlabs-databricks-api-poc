"""Serving endpoints collector.

Lists serving endpoints from ``/api/2.0/serving-endpoints`` and, for
each, fetches the OpenMetrics text from
``/api/2.0/serving-endpoints/{name}/metrics``.

**What this workspace actually exposes (stage 5 probe):**

- Only custom endpoints (the ``*-agent-endpoint`` ones here) return a
  populated metrics payload. Foundation-model endpoints (``databricks-gpt-*``,
  ``databricks-meta-llama-*``, etc.) 404 on ``/metrics``.
- The metrics returned are three *gauges*, not histograms:
  ``request_count_total``, ``request_4xx_count_total``,
  ``request_5xx_count_total``. Each is the count for the last minute
  with a unix-ms timestamp suffix. Despite ``_total`` in the name, they
  are not cumulative counters.
- CLAUDE.md calls for p50/p95 from a ``request_latency_ms`` histogram.
  That family is **not present** on this workspace's endpoints. We
  still parse histograms when they appear (future-proof) but this
  workspace's ``p50_ms`` / ``p95_ms`` come back ``None``.
"""

from __future__ import annotations

import logging
from collections import Counter

from prometheus_client.parser import text_string_to_metric_families

from ..client import ClientError

log = logging.getLogger("dbx_metrics.collectors.serving")

_MAX_ENDPOINTS_METRICS = 50  # cap /metrics calls per run


def collect(client, config, **kwargs) -> dict:
    try:
        resp = client.get("/api/2.0/serving-endpoints")
    except ClientError as exc:
        return _unavailable(f"serving-endpoints list failed: HTTP {exc.status}")

    endpoints = resp.get("endpoints", []) or []
    detail: list[dict] = []
    task_counts: Counter = Counter()
    ready_count = 0
    metrics_covered = 0
    total_req = 0.0
    total_4xx = 0.0
    total_5xx = 0.0

    for ep in endpoints[:_MAX_ENDPOINTS_METRICS]:
        name = ep.get("name")
        state = ep.get("state") or {}
        task = ep.get("task")
        task_counts[task or "none"] += 1
        if state.get("ready") == "READY":
            ready_count += 1

        ep_summary = _fetch_endpoint_metrics(client, name)
        if ep_summary["metrics_available"]:
            metrics_covered += 1
            total_req += ep_summary["requests_last_min"] or 0
            total_4xx += ep_summary["errors_4xx_last_min"] or 0
            total_5xx += ep_summary["errors_5xx_last_min"] or 0

        detail.append({
            "name": name,
            "id": ep.get("id"),
            "state_ready": state.get("ready"),
            "config_update": state.get("config_update"),
            "task": task,
            "endpoint_type": ep.get("endpoint_type"),
            "route_optimized": ep.get("route_optimized"),
            "permission_level": ep.get("permission_level"),
            "creator": ep.get("creator"),
            "served_entities": _served_entity_names(ep),
            **ep_summary,
        })

    # Any endpoints we didn't sample at all (over the cap).
    skipped = max(0, len(endpoints) - _MAX_ENDPOINTS_METRICS)

    summary = {
        "endpoint_count": len(endpoints),
        "ready_count": ready_count,
        "tasks": dict(task_counts),
        "metrics_covered_count": metrics_covered,
        "metrics_missing_count": len(detail) - metrics_covered,
        "endpoints_not_probed": skipped,
        "total_requests_last_min": total_req,
        "total_errors_4xx_last_min": total_4xx,
        "total_errors_5xx_last_min": total_5xx,
        "total_error_rate_last_min": (
            (total_4xx + total_5xx) / total_req if total_req else None
        ),
    }

    return {
        "status": "partial" if skipped else "available",
        "reason": (
            f"{skipped} endpoint(s) beyond cap not sampled for metrics"
            if skipped else None
        ),
        "summary": summary,
        "detail": detail,
    }


def _served_entity_names(ep: dict) -> list[str]:
    entities = (ep.get("config") or {}).get("served_entities") or []
    return [e.get("entity_name") or e.get("name") for e in entities if isinstance(e, dict)]


def _fetch_endpoint_metrics(client, name: str) -> dict:
    blank = {
        "metrics_available": False,
        "requests_last_min": None,
        "errors_4xx_last_min": None,
        "errors_5xx_last_min": None,
        "error_rate_last_min": None,
        "p50_ms": None,
        "p95_ms": None,
        "metric_families_seen": [],
    }
    try:
        text = client.get_text(f"/api/2.0/serving-endpoints/{name}/metrics")
    except ClientError as exc:
        log.info("metrics unavailable for %s: HTTP %s", name, exc.status)
        return blank

    requests_last_min = None
    err_4xx = None
    err_5xx = None
    p50 = None
    p95 = None
    families_seen: list[str] = []

    for family in text_string_to_metric_families(text):
        families_seen.append(family.name)

        # The parser only strips ``_total`` when the family is declared
        # as a counter. These three are declared ``gauge`` on Databricks
        # serving endpoints, so the suffix is retained. Match both forms.
        if family.name in ("request_count_total", "request_count"):
            requests_last_min = _sum_samples(family)
        elif family.name in ("request_4xx_count_total", "request_4xx_count"):
            err_4xx = _sum_samples(family)
        elif family.name in ("request_5xx_count_total", "request_5xx_count"):
            err_5xx = _sum_samples(family)
        elif family.type == "histogram" and "latency" in family.name:
            # Future-proof: if a latency histogram ever appears, derive
            # p50/p95 from its buckets.
            p50 = _percentile_from_histogram(family, 0.50)
            p95 = _percentile_from_histogram(family, 0.95)

    error_rate = None
    if requests_last_min:
        num = (err_4xx or 0) + (err_5xx or 0)
        error_rate = num / requests_last_min

    return {
        "metrics_available": True,
        "requests_last_min": requests_last_min,
        "errors_4xx_last_min": err_4xx,
        "errors_5xx_last_min": err_5xx,
        "error_rate_last_min": error_rate,
        "p50_ms": p50,
        "p95_ms": p95,
        "metric_families_seen": families_seen,
    }


def _sum_samples(family) -> float:
    """Sum non-suffixed sample values across all label combos."""
    total = 0.0
    for sample in family.samples:
        # prometheus_client exposes Sample(name, labels, value, timestamp, exemplar)
        if sample.name.endswith(("_created", "_timestamp")):
            continue
        total += float(sample.value)
    return total


def _percentile_from_histogram(family, q: float) -> float | None:
    """Linear interpolation across histogram buckets.

    Bucket samples have name ``<family>_bucket`` and a label ``le``
    with the cumulative count up to that upper bound. We pool across
    all label combinations at the upper-bound level.
    """
    bucket_totals: dict[float, float] = {}
    count_samples = 0.0
    for s in family.samples:
        if s.name.endswith("_bucket"):
            le = s.labels.get("le")
            try:
                bound = float("inf") if le == "+Inf" else float(le)
            except (TypeError, ValueError):
                continue
            bucket_totals[bound] = bucket_totals.get(bound, 0.0) + float(s.value)
        elif s.name.endswith("_count"):
            count_samples += float(s.value)

    if not bucket_totals or not count_samples:
        return None

    ordered = sorted(bucket_totals.items())
    target = q * count_samples
    prev_bound = 0.0
    prev_cum = 0.0
    for bound, cum in ordered:
        if cum >= target:
            span = cum - prev_cum
            if span <= 0 or bound == float("inf"):
                return None if bound == float("inf") else bound
            fraction = (target - prev_cum) / span
            return prev_bound + (bound - prev_bound) * fraction
        prev_bound, prev_cum = bound, cum
    return None


def _unavailable(reason: str) -> dict:
    return {"status": "unavailable", "reason": reason, "summary": {}, "detail": []}
