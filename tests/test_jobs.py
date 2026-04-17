"""Jobs collector: p50/p95 from hand-computed fixture.

The collector's ``_percentile`` uses nearest-rank against successful-run
durations pooled across the workspace. For five runs with durations
[100, 200, 300, 400, 500] ms, nearest-rank gives:

- p50: index ``round(0.50 * 4) = 2`` -> 300 ms
- p95: index ``round(0.95 * 4) = 4`` -> 500 ms

Failed runs are counted for ``failure_rate`` but excluded from the
duration pool, so they don't distort percentiles.
"""

from __future__ import annotations

import responses

from dbx_metrics.collectors import jobs


def _run(start_ms: int, duration_ms: int, state: str = "SUCCESS") -> dict:
    return {
        "start_time": start_ms,
        "end_time": start_ms + duration_ms,
        "state": {"result_state": state},
    }


@responses.activate
def test_jobs_p95_from_fixture(client, host):
    responses.add(
        responses.GET,
        host + "/api/2.1/jobs/list",
        status=200,
        json={"jobs": [
            {"job_id": 1, "settings": {"name": "nightly_etl"}, "creator_user_name": "alice"},
        ]},
    )

    durations = [100, 200, 300, 400, 500]
    responses.add(
        responses.GET,
        host + "/api/2.1/jobs/runs/list",
        status=200,
        json={"runs": [_run(1_700_000_000_000 + i, d) for i, d in enumerate(durations)]},
    )

    class _Cfg: pass
    result = jobs.collect(client, _Cfg())

    assert result["status"] == "available"
    summary = result["summary"]
    assert summary["total_jobs"] == 1
    assert summary["runs_sampled"] == 5
    assert summary["p50_duration_ms"] == 300
    assert summary["p95_duration_ms"] == 500
    assert summary["failure_rate"] == 0.0


@responses.activate
def test_jobs_failure_rate_counts_failed_states(client, host):
    responses.add(
        responses.GET,
        host + "/api/2.1/jobs/list",
        status=200,
        json={"jobs": [{"job_id": 1, "settings": {"name": "etl"}}]},
    )
    responses.add(
        responses.GET,
        host + "/api/2.1/jobs/runs/list",
        status=200,
        json={"runs": [
            _run(1, 100, state="SUCCESS"),
            _run(2, 200, state="FAILED"),
            _run(3, 300, state="TIMEDOUT"),
            _run(4, 400, state="SUCCESS"),
        ]},
    )

    class _Cfg: pass
    result = jobs.collect(client, _Cfg())

    # 2 failures out of 4 runs = 0.5
    assert result["summary"]["failure_rate"] == 0.5
    # Only SUCCESS runs contribute to duration percentiles: [100, 400]
    # p95 on [100, 400] -> index round(0.95 * 1) = 1 -> 400
    assert result["summary"]["p95_duration_ms"] == 400


@responses.activate
def test_jobs_empty_workspace(client, host):
    responses.add(responses.GET, host + "/api/2.1/jobs/list", status=200, json={"jobs": []})

    class _Cfg: pass
    result = jobs.collect(client, _Cfg())

    assert result["status"] == "available"
    assert result["summary"]["total_jobs"] == 0
    assert result["summary"]["p95_duration_ms"] is None
    assert result["detail"] == []
