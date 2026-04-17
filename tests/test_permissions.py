"""Permissions: workspace ACL + UC grant normalize to the same shape.

Two API surfaces, one row shape. This test proves that regardless of
which surface a grant came from, the normalized output carries the
same keys and that principal-type is correctly derived from the field
name on the workspace side (user_name / group_name / service_principal_name).
"""

from __future__ import annotations

from dataclasses import dataclass

import responses

from dbx_metrics.collectors import permissions


@dataclass
class _Cfg:
    sample_permissions: int = 10


EXPECTED_KEYS = {
    "principal",
    "principal_type",
    "permission_level",
    "source",
    "object_type",
    "object_id",
    "inherited",
    "inherited_from",
}


@responses.activate
def test_workspace_acl_and_uc_grant_share_shape(client, host):
    # Prior results: one serving endpoint (for workspace ACL probe) and
    # one UC schema (for UC grant probe).
    prior = {
        "serving": {"detail": [{"id": "ep-1", "name": "rag-agent-endpoint"}]},
        "unity_catalog": {"detail": [{
            "name": "cat1",
            "schemas": [{
                "full_name": "cat1.sch_a",
                "tables_sample": [],
                "functions_sample": [],
            }],
        }]},
    }

    responses.add(
        responses.GET,
        host + "/api/2.0/permissions/serving-endpoints/ep-1",
        status=200,
        json={
            "object_type": "serving-endpoint",
            "access_control_list": [
                {
                    "user_name": "alice@example.com",
                    "all_permissions": [{"permission_level": "CAN_MANAGE", "inherited": False}],
                },
                {
                    "group_name": "admins",
                    "all_permissions": [{
                        "permission_level": "CAN_MANAGE",
                        "inherited": True,
                        "inherited_from_object": ["/serving-endpoints"],
                    }],
                },
                {
                    "service_principal_name": "sp-123",
                    "all_permissions": [{"permission_level": "CAN_QUERY", "inherited": False}],
                },
            ],
        },
    )
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/permissions/schema/cat1.sch_a",
        status=200,
        json={
            "privilege_assignments": [
                {"principal": "account users", "privileges": ["USE_SCHEMA", "SELECT"]},
            ],
        },
    )
    # Catalog + table + function calls return empty so the test stays focused.
    for path in (
        "/api/2.1/unity-catalog/permissions/catalog/cat1",
    ):
        responses.add(responses.GET, host + path, status=200,
                      json={"privilege_assignments": []})

    result = permissions.collect(client, _Cfg(sample_permissions=5), prior=prior)

    assert result["status"] == "available"
    grants = result["detail"]["grants"]

    # All rows share the same key set regardless of source.
    for g in grants:
        assert set(g) == EXPECTED_KEYS

    # Workspace side: principal type derived from field name.
    ws = [g for g in grants if g["source"] == "workspace"]
    types = {g["principal_type"] for g in ws}
    assert types == {"user", "group", "service_principal"}

    # Canonical object_type: API returned "serving-endpoint" (singular);
    # normalizer preferred that over the queried "serving-endpoints".
    assert all(g["object_type"] == "serving-endpoint" for g in ws)

    # Inherited ACL rows carry the parent path (first element of the list).
    inherited = [g for g in ws if g["inherited"]]
    assert len(inherited) == 1
    assert inherited[0]["inherited_from"] == "/serving-endpoints"

    # UC side: two grants expand from one principal's two privileges.
    uc = [g for g in grants if g["source"] == "uc"]
    assert {g["permission_level"] for g in uc} == {"USE_SCHEMA", "SELECT"}
    assert all(g["principal_type"] == "unknown" for g in uc)
    assert all(g["inherited"] is None for g in uc)


@responses.activate
def test_permissions_no_prior_sources_returns_unavailable(client, host):
    result = permissions.collect(client, _Cfg(), prior={})
    assert result["status"] == "unavailable"
    assert "no accessible object sources" in result["reason"]


@responses.activate
def test_permissions_lookup_error_becomes_partial(client, host):
    prior = {
        "serving": {"detail": [{"id": "ep-1", "name": "ep-1"}]},
    }
    responses.add(
        responses.GET,
        host + "/api/2.0/permissions/serving-endpoints/ep-1",
        status=403,
        body='{"error": "permission denied"}',
    )

    result = permissions.collect(client, _Cfg(sample_permissions=5), prior=prior)

    assert result["status"] == "partial"
    assert "1 permissions lookup(s) failed" in result["reason"]
    assert len(result["detail"]["lookup_errors"]) == 1
    assert result["detail"]["lookup_errors"][0]["status"] == 403
