"""Unity Catalog collector.

Walks catalogs → schemas → tables + functions while honoring the two
guardrails from the CLI:

- ``--uc-scope catalog`` or ``--uc-scope catalog.schema`` narrows the
  traversal to a single catalog, or a single schema within one.
- ``--uc-max-schemas N`` caps the *total* number of schemas inspected
  across the run (not per catalog). When hit, the collector short-
  circuits and returns ``status="partial"`` with a reason.

A per-schema network budget of two list calls (tables + functions) is
intentional; wider audits belong in system tables via the SQL
Statement Execution API, which is explicitly out of scope for the POC.
"""

from __future__ import annotations

import logging
from collections import Counter

from ..client import ClientError

log = logging.getLogger("dbx_metrics.collectors.unity_catalog")

_TABLES_SAMPLE_PER_SCHEMA = 10


def collect(client, config, **kwargs) -> dict:
    target_catalog, target_schema = _parse_scope(config.uc_scope)
    max_schemas = max(1, int(config.uc_max_schemas or 10))

    try:
        metastore = client.get("/api/2.1/unity-catalog/metastore_summary")
    except ClientError as exc:
        return _unavailable(f"metastore_summary failed: HTTP {exc.status}")

    try:
        all_catalogs = list(client.paginate(
            "/api/2.1/unity-catalog/catalogs", items_key="catalogs"
        ))
    except ClientError as exc:
        return _unavailable(f"catalogs list failed: HTTP {exc.status}")

    catalogs = all_catalogs
    if target_catalog:
        catalogs = [c for c in all_catalogs if c.get("name") == target_catalog]
        if not catalogs:
            return _unavailable(f"catalog {target_catalog!r} not found or not accessible")

    schemas_visited = 0
    table_kinds: Counter = Counter()
    total_tables = 0
    total_functions = 0
    per_catalog: list[dict] = []
    truncated = False

    for cat in catalogs:
        cat_name = cat.get("name")
        if truncated:
            per_catalog.append({
                "name": cat_name,
                "catalog_type": cat.get("catalog_type"),
                "owner": cat.get("owner"),
                "schemas": [],
                "reason": "skipped: schema cap reached earlier",
            })
            continue
        cat_schemas: list[dict] = []
        try:
            schema_iter = client.paginate(
                "/api/2.1/unity-catalog/schemas",
                items_key="schemas",
                params={"catalog_name": cat_name},
            )
            for sch in schema_iter:
                if target_schema and sch.get("name") != target_schema:
                    continue
                cat_schemas.append(sch)
        except ClientError as exc:
            per_catalog.append({
                "name": cat_name,
                "catalog_type": cat.get("catalog_type"),
                "schemas": [],
                "reason": f"schemas list failed: HTTP {exc.status}",
            })
            continue

        per_schemas: list[dict] = []
        for sch in cat_schemas:
            if schemas_visited >= max_schemas:
                truncated = True
                break
            schemas_visited += 1
            full_name = sch.get("full_name") or f"{cat_name}.{sch.get('name')}"
            schema_row = _collect_schema(client, cat_name, sch.get("name"), full_name)
            per_schemas.append(schema_row)
            total_tables += schema_row["table_count"]
            total_functions += schema_row["function_count"]
            for k, v in schema_row.get("table_kinds", {}).items():
                table_kinds[k] += v

        per_catalog.append({
            "name": cat_name,
            "catalog_type": cat.get("catalog_type"),
            "owner": cat.get("owner"),
            "schemas": per_schemas,
        })

    summary = {
        "metastore": {
            "id": metastore.get("metastore_id"),
            "name": metastore.get("name"),
            "cloud": metastore.get("cloud"),
            "region": metastore.get("region"),
        },
        "scope": config.uc_scope or "all",
        "catalog_count": len(catalogs),
        "catalogs_total_in_metastore": len(all_catalogs),
        "schemas_visited": schemas_visited,
        "schemas_cap": max_schemas,
        "schemas_truncated": truncated,
        "table_count": total_tables,
        "table_kinds": dict(table_kinds),
        "function_count": total_functions,
    }

    status = "partial" if truncated else "available"
    reason = f"reached --uc-max-schemas={max_schemas} guardrail" if truncated else None

    return {
        "status": status,
        "reason": reason,
        "summary": summary,
        "detail": per_catalog,
    }


def _collect_schema(client, cat_name: str, sch_name: str, full_name: str) -> dict:
    kinds: Counter = Counter()
    tables_sampled: list[dict] = []
    table_count = 0
    tables_reason: str | None = None

    try:
        for t in client.paginate(
            "/api/2.1/unity-catalog/tables",
            items_key="tables",
            params={"catalog_name": cat_name, "schema_name": sch_name},
        ):
            table_count += 1
            kind = t.get("table_type") or "UNKNOWN"
            kinds[kind] += 1
            if len(tables_sampled) < _TABLES_SAMPLE_PER_SCHEMA:
                tables_sampled.append({
                    "full_name": t.get("full_name"),
                    "table_type": kind,
                    "data_source_format": t.get("data_source_format"),
                })
    except ClientError as exc:
        tables_reason = f"HTTP {exc.status}"
        log.info("tables list for %s failed: %s", full_name, tables_reason)

    function_count = 0
    functions_sample: list[dict] = []
    functions_reason: str | None = None
    try:
        for f in client.paginate(
            "/api/2.1/unity-catalog/functions",
            items_key="functions",
            params={"catalog_name": cat_name, "schema_name": sch_name},
        ):
            function_count += 1
            if len(functions_sample) < _TABLES_SAMPLE_PER_SCHEMA:
                functions_sample.append({"full_name": f.get("full_name")})
    except ClientError as exc:
        functions_reason = f"HTTP {exc.status}"
        log.info("functions list for %s failed: %s", full_name, functions_reason)

    row = {
        "full_name": full_name,
        "table_count": table_count,
        "table_kinds": dict(kinds),
        "function_count": function_count,
        "tables_sample": tables_sampled,
        "functions_sample": functions_sample,
    }
    if tables_reason:
        row["tables_reason"] = tables_reason
    if functions_reason:
        row["functions_reason"] = functions_reason
    return row


def _parse_scope(scope: str | None) -> tuple[str | None, str | None]:
    if not scope:
        return None, None
    parts = scope.split(".", 1)
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[1]


def _unavailable(reason: str) -> dict:
    return {"status": "unavailable", "reason": reason, "summary": {}, "detail": []}
