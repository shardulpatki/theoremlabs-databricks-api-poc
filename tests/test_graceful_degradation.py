"""Graceful degradation: each collector returns cleanly on a 403.

The spec requires the tool to still produce a useful report when the
workspace denies access to some domains. This test injects a 403 on
each domain's primary endpoint and asserts:

1. The domain is reported ``unavailable`` (or ``partial`` where the
   collector successfully returns a bounded result with errors noted).
2. A ``reason`` is populated — never ``None`` when the domain isn't
   fully available.
3. No exception escapes.

Permissions is tested separately because its "primary" call depends
on prior results — we exercise the "no sources available" path.
"""

from __future__ import annotations

from dataclasses import dataclass

import responses

from dbx_metrics.collectors import (
    clusters,
    jobs,
    models,
    permissions,
    serving,
    unity_catalog,
)


@dataclass
class _Cfg:
    uc_scope: str | None = None
    uc_max_schemas: int = 10
    sample_permissions: int = 5


def _assert_degraded(result):
    assert result["status"] in {"unavailable", "partial"}
    assert result["reason"], "reason must be populated when not available"


@responses.activate
def test_jobs_degrades_on_403(client, host):
    responses.add(responses.GET, host + "/api/2.1/jobs/list", status=403, body="denied")
    _assert_degraded(jobs.collect(client, _Cfg()))


@responses.activate
def test_clusters_degrades_on_403(client, host):
    responses.add(responses.GET, host + "/api/2.1/clusters/list", status=403, body="denied")
    _assert_degraded(clusters.collect(client, _Cfg()))


@responses.activate
def test_serving_degrades_on_403(client, host):
    responses.add(responses.GET, host + "/api/2.0/serving-endpoints", status=403, body="denied")
    _assert_degraded(serving.collect(client, _Cfg()))


@responses.activate
def test_unity_catalog_degrades_on_403(client, host):
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/metastore_summary",
        status=403,
        body="denied",
    )
    _assert_degraded(unity_catalog.collect(client, _Cfg()))


@responses.activate
def test_models_degrades_when_uc_models_403(client, host):
    # No serving prior, UC models 403, MLflow 403 -> unavailable.
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/models",
        status=403, body="denied",
    )
    responses.add(
        responses.GET,
        host + "/api/2.0/mlflow/registered-models/list",
        status=403, body="denied",
    )
    result = models.collect(client, _Cfg(), prior={})
    _assert_degraded(result)


def test_permissions_unavailable_when_no_sources():
    result = permissions.collect(client=None, config=_Cfg(), prior={})
    assert result["status"] == "unavailable"
    assert "no accessible object sources" in result["reason"]
