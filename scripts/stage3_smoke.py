"""Stage-3 mock smoke test.

Populates env vars against a fake host, uses ``responses`` to simulate
``/jobs/list`` and ``/jobs/runs/list``, then invokes ``main.main()``
as if from the CLI. Verifies the p95 matches hand calculation.

Throwaway — delete after stage 3 accepted.
"""

from __future__ import annotations

import io
import json
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import responses

from main import main  # noqa: E402

HOST = "https://fake.cloud.databricks.com"


def _install_env():
    os.environ["DATABRICKS_HOST"] = HOST
    os.environ["DATABRICKS_TOKEN"] = "fake-token"


def _install_mocks():
    # Capability probe hits: jobs, clusters, serving, uc, models.
    # Jobs collector also paginates /jobs/list and calls /runs/list per job.
    responses.add(responses.GET, f"{HOST}/api/2.0/clusters/list-node-types", json={"node_types": []})
    responses.add(responses.GET, f"{HOST}/api/2.0/serving-endpoints", json={"endpoints": []})
    responses.add(responses.GET, f"{HOST}/api/2.1/unity-catalog/metastore_summary", json={"metastore_id": "m"})
    responses.add(responses.GET, f"{HOST}/api/2.0/mlflow/registered-models/list", json={"registered_models": []})

    # /jobs/list — probe (limit=1) and collector (limit=25) both hit this
    # URL. `responses` matches registrations FIFO; register a probe-sized
    # response, then a collector-sized response.
    responses.add(
        responses.GET,
        f"{HOST}/api/2.1/jobs/list",
        json={"jobs": [{"job_id": 1}]},
    )
    responses.add(
        responses.GET,
        f"{HOST}/api/2.1/jobs/list",
        json={
            "jobs": [
                {"job_id": 101, "settings": {"name": "nightly_etl", "schedule": {"pause_status": "UNPAUSED"}}},
                {"job_id": 102, "settings": {"name": "ad_hoc"}},
            ]
        },
    )

    # Run samples:
    # job 101: three SUCCESS durations (1000, 2000, 3000) + one FAILED
    responses.add(
        responses.GET,
        f"{HOST}/api/2.1/jobs/runs/list",
        json={
            "runs": [
                {"state": {"result_state": "SUCCESS"}, "start_time": 0, "end_time": 1000},
                {"state": {"result_state": "SUCCESS"}, "start_time": 0, "end_time": 2000},
                {"state": {"result_state": "SUCCESS"}, "start_time": 0, "end_time": 3000},
                {"state": {"result_state": "FAILED"}, "start_time": 0, "end_time": 500},
            ]
        },
    )
    # job 102: two SUCCESS durations (4000, 5000)
    responses.add(
        responses.GET,
        f"{HOST}/api/2.1/jobs/runs/list",
        json={
            "runs": [
                {"state": {"result_state": "SUCCESS"}, "start_time": 0, "end_time": 4000},
                {"state": {"result_state": "SUCCESS"}, "start_time": 0, "end_time": 5000},
            ]
        },
    )


@responses.activate
def run() -> int:
    _install_env()
    _install_mocks()

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = main(["--only", "jobs", "--format", "json"])

    out = buf.getvalue()
    print("--- CLI stdout ---")
    print(out)
    print("--- CLI exit:", rc)

    report = json.loads(out)
    summary = report["domains"]["jobs"]["summary"]

    # Hand calc: pooled successful durations = [1000, 2000, 3000, 4000, 5000]
    # nearest-rank p50 on 5 values -> index round(0.5*4)=2 -> 3000
    # nearest-rank p95 on 5 values -> index round(0.95*4)=4 -> 5000
    assert summary["total_jobs"] == 2, summary
    assert summary["runs_sampled"] == 6, summary
    assert summary["p50_duration_ms"] == 3000, summary
    assert summary["p95_duration_ms"] == 5000, summary
    # 1 failure / 6 sampled runs
    assert abs(summary["failure_rate"] - (1 / 6)) < 1e-9, summary
    assert summary["active_schedules"] == 1, summary
    print("\nHand-check OK: p50=3000ms p95=5000ms failure_rate=1/6 active_schedules=1")
    return rc


if __name__ == "__main__":
    raise SystemExit(run())
