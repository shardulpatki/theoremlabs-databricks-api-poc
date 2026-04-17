"""Jobs collector.

Strategy (stage 3, per plan decision #7):

- Paginate ``/api/2.1/jobs/list`` to get the workspace's job inventory.
- Sample the first page of ``/api/2.1/jobs/runs/list`` per job,
  capped at ``_JOBS_RUN_SAMPLE_LIMIT`` jobs to keep API calls bounded.
- Compute p50 / p95 of *successful-run durations* (``end_time -
  start_time`` where ``result_state == "SUCCESS"``) across the pooled
  workspace sample. Failure rate is computed over the full sample
  regardless of result state.
"""

from __future__ import annotations

import logging

from ..client import ClientError

log = logging.getLogger("dbx_metrics.collectors.jobs")

_JOBS_PAGE_SIZE = 25
_RUNS_PAGE_SIZE = 25
_JOBS_RUN_SAMPLE_LIMIT = 25
_FAILURE_STATES = frozenset({"FAILED", "TIMEDOUT"})


def collect(client, config, **kwargs) -> dict:
    try:
        jobs = list(
            client.paginate(
                "/api/2.1/jobs/list",
                items_key="jobs",
                params={"limit": _JOBS_PAGE_SIZE, "expand_tasks": "false"},
            )
        )
    except ClientError as exc:
        return _unavailable(f"jobs list failed: HTTP {exc.status}")

    if not jobs:
        return {
            "status": "available",
            "reason": None,
            "summary": _empty_summary(0),
            "detail": [],
        }

    active_schedules = sum(1 for j in jobs if _is_schedule_active(j))

    durations_ms: list[int] = []
    total_runs = 0
    total_failures = 0
    per_job: list[dict] = []
    runs_sample_errors = 0

    for job in jobs[:_JOBS_RUN_SAMPLE_LIMIT]:
        job_id = job.get("job_id")
        name = (job.get("settings") or {}).get("name")
        try:
            runs_resp = client.get(
                "/api/2.1/jobs/runs/list",
                params={
                    "job_id": job_id,
                    "completed_only": "true",
                    "limit": _RUNS_PAGE_SIZE,
                },
            )
        except ClientError as exc:
            runs_sample_errors += 1
            log.info("runs/list for job %s failed: HTTP %s", job_id, exc.status)
            per_job.append({
                "job_id": job_id,
                "name": name,
                "runs_sampled": 0,
                "p95_duration_ms": None,
                "failure_count": None,
                "reason": f"HTTP {exc.status}",
            })
            continue

        runs = runs_resp.get("runs", []) or []
        job_durations: list[int] = []
        job_failures = 0
        for run in runs:
            state = (run.get("state") or {}).get("result_state")
            start = run.get("start_time") or 0
            end = run.get("end_time") or 0
            if state == "SUCCESS" and end > start:
                job_durations.append(end - start)
            if state in _FAILURE_STATES:
                job_failures += 1

        durations_ms.extend(job_durations)
        total_runs += len(runs)
        total_failures += job_failures
        per_job.append({
            "job_id": job_id,
            "name": name,
            "runs_sampled": len(runs),
            "p95_duration_ms": _percentile(job_durations, 95),
            "failure_count": job_failures,
        })

    summary = {
        "total_jobs": len(jobs),
        "active_schedules": active_schedules,
        "jobs_sampled_for_runs": min(len(jobs), _JOBS_RUN_SAMPLE_LIMIT),
        "runs_sampled": total_runs,
        "p50_duration_ms": _percentile(durations_ms, 50),
        "p95_duration_ms": _percentile(durations_ms, 95),
        "failure_rate": (total_failures / total_runs) if total_runs else None,
        "runs_sample_errors": runs_sample_errors,
    }

    status = "partial" if runs_sample_errors and runs_sample_errors < len(per_job) else "available"
    reason = f"{runs_sample_errors} of {len(per_job)} sampled jobs failed runs/list" if status == "partial" else None

    return {
        "status": status,
        "reason": reason,
        "summary": summary,
        "detail": per_job,
    }


def _is_schedule_active(job: dict) -> bool:
    schedule = (job.get("settings") or {}).get("schedule") or {}
    return bool(schedule) and schedule.get("pause_status") != "PAUSED"


def _percentile(values: list[int], p: int) -> int | None:
    """Nearest-rank percentile. Good enough for a POC; explicit, no deps."""
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = int(round((p / 100) * (len(s) - 1)))
    k = max(0, min(len(s) - 1, k))
    return s[k]


def _empty_summary(total: int) -> dict:
    return {
        "total_jobs": total,
        "active_schedules": 0,
        "jobs_sampled_for_runs": 0,
        "runs_sampled": 0,
        "p50_duration_ms": None,
        "p95_duration_ms": None,
        "failure_rate": None,
        "runs_sample_errors": 0,
    }


def _unavailable(reason: str) -> dict:
    return {"status": "unavailable", "reason": reason, "summary": {}, "detail": []}
