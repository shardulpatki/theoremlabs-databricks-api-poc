"""Unity Catalog: nested traversal + --uc-max-schemas guardrail.

Exercises the real code path: catalogs list, per-catalog schemas list,
per-schema tables and functions. Schemas pagination is verified via a
two-page token cascade on one catalog. The guardrail is exercised by
setting ``uc_max_schemas=2`` and asserting the walker short-circuits
mid-stream.
"""

from __future__ import annotations

from dataclasses import dataclass

import responses

from dbx_metrics.collectors import unity_catalog


@dataclass
class _Cfg:
    uc_scope: str | None = None
    uc_max_schemas: int = 10


@responses.activate
def test_uc_nested_cascade_with_pagination(client, host):
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/metastore_summary",
        status=200,
        json={"metastore_id": "mstore", "name": "test", "cloud": "aws", "region": "us-west-2"},
    )
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/catalogs",
        status=200,
        json={"catalogs": [
            {"name": "cat1", "catalog_type": "MANAGED_CATALOG", "owner": "alice"},
        ]},
    )
    # Two-page schema list to exercise pagination at the schema level.
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/schemas",
        status=200,
        json={
            "schemas": [{"name": "sch_a", "full_name": "cat1.sch_a"}],
            "next_page_token": "sch_p2",
        },
    )
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/schemas",
        status=200,
        json={
            "schemas": [{"name": "sch_b", "full_name": "cat1.sch_b"}],
        },
    )
    # Tables + functions per schema.
    for _ in range(2):
        responses.add(
            responses.GET,
            host + "/api/2.1/unity-catalog/tables",
            status=200,
            json={"tables": [
                {"full_name": "cat1.sch.t1", "table_type": "MANAGED"},
                {"full_name": "cat1.sch.t2", "table_type": "VIEW"},
                {"full_name": "cat1.sch.t3", "table_type": "MANAGED"},
            ]},
        )
        responses.add(
            responses.GET,
            host + "/api/2.1/unity-catalog/functions",
            status=200,
            json={"functions": [{"full_name": "cat1.sch.fn1"}]},
        )

    result = unity_catalog.collect(client, _Cfg())

    assert result["status"] == "available"
    s = result["summary"]
    assert s["catalog_count"] == 1
    assert s["schemas_visited"] == 2
    assert s["table_count"] == 6
    assert s["table_kinds"] == {"MANAGED": 4, "VIEW": 2}
    assert s["function_count"] == 2
    assert s["schemas_truncated"] is False


@responses.activate
def test_uc_max_schemas_guardrail_truncates_and_returns_partial(client, host):
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/metastore_summary",
        status=200, json={"metastore_id": "mstore"},
    )
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/catalogs",
        status=200,
        json={"catalogs": [{"name": "cat1"}, {"name": "cat2"}]},
    )
    # cat1 has 3 schemas, cat2 has 2. With cap=2, we should visit 2 total
    # (both under cat1) and short-circuit.
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/schemas",
        status=200,
        json={"schemas": [
            {"name": "a", "full_name": "cat1.a"},
            {"name": "b", "full_name": "cat1.b"},
            {"name": "c", "full_name": "cat1.c"},
        ]},
    )
    # Tables + functions for the 2 schemas we WILL visit.
    for _ in range(2):
        responses.add(responses.GET, host + "/api/2.1/unity-catalog/tables",
                      status=200, json={"tables": []})
        responses.add(responses.GET, host + "/api/2.1/unity-catalog/functions",
                      status=200, json={"functions": []})
    # cat2 schemas are still listed (one call), but the walker short-
    # circuits before entering them — no further tables/functions calls.
    # Actually: the walker breaks out of the *outer* catalog loop by
    # checking `truncated` before issuing the next catalog's schema
    # list. So cat2 schemas should NOT be fetched at all.

    result = unity_catalog.collect(client, _Cfg(uc_max_schemas=2))

    assert result["status"] == "partial"
    assert "uc-max-schemas=2" in (result["reason"] or "")
    assert result["summary"]["schemas_visited"] == 2
    assert result["summary"]["schemas_truncated"] is True

    # cat2 appears in detail with a stub indicating it was skipped.
    names = [c["name"] for c in result["detail"]]
    assert names == ["cat1", "cat2"]
    cat2 = next(c for c in result["detail"] if c["name"] == "cat2")
    assert "skipped" in (cat2.get("reason") or "")


@responses.activate
def test_uc_scope_narrows_to_single_catalog(client, host):
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/metastore_summary",
        status=200, json={"metastore_id": "mstore"},
    )
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/catalogs",
        status=200,
        json={"catalogs": [{"name": "cat1"}, {"name": "cat2"}, {"name": "cat3"}]},
    )
    responses.add(
        responses.GET,
        host + "/api/2.1/unity-catalog/schemas",
        status=200, json={"schemas": [{"name": "only_sch", "full_name": "cat2.only_sch"}]},
    )
    responses.add(responses.GET, host + "/api/2.1/unity-catalog/tables",
                  status=200, json={"tables": []})
    responses.add(responses.GET, host + "/api/2.1/unity-catalog/functions",
                  status=200, json={"functions": []})

    result = unity_catalog.collect(client, _Cfg(uc_scope="cat2"))

    assert result["summary"]["catalog_count"] == 1
    assert result["summary"]["schemas_visited"] == 1
    assert result["detail"][0]["name"] == "cat2"
