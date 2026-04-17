# Databricks Metrics POC

A Python CLI that collects metrics from a Databricks workspace via REST
APIs across six domains: **jobs, clusters, serving endpoints, Unity
Catalog, permissions, and models/agents**. Demo-grade POC, but built to
professional standards: single HTTP client handling auth / retry /
pagination, independent domain collectors with a uniform return shape,
dumb formatters, capability probing, and graceful degradation on 403
and 404 so the tool produces a useful report even on Free Edition or
permission-limited workspaces.

## Quick start

```bash
cp .env.example .env                    # then fill in HOST and TOKEN
pip install -r requirements.txt
python main.py --format summary         # one-page card
python main.py --format table           # per-domain tables
python main.py --format json > out.json # for piping / inspection
```

## Authentication

Personal access token via environment variables loaded from `.env`:

```
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
```

- The token is never logged, never printed, and is redacted in
  `repr(DatabricksClient)`.
- `.env` is in `.gitignore`; only `.env.example` ships in the repo.
- **OAuth M2M with a service principal is the production path.** PAT
  is for the POC.

## CLI reference

```
python main.py [OPTIONS]

--format {json,table,summary}      Output format. Default: table.
--only jobs,clusters,...           Comma-separated subset of collectors.
                                   Available: jobs, clusters, serving,
                                   unity_catalog, models, permissions.
--uc-scope catalog[.schema]        Narrow UC traversal. Without this,
                                   all catalogs are walked.
--uc-max-schemas N                 Global cap on schemas inspected
                                   across the run. Default: 10.
                                   Hitting the cap yields status=partial.
--sample-permissions N             Cap permissions calls per object type
                                   per surface (workspace + UC).
                                   Upper bound per run: 7 * N calls.
                                   Default: 10.
--verbose                          DEBUG logging.
```

## Architecture

```
dbx_metrics/
├── client.py              HTTPS wrapper: auth, retries, pagination, timeouts
├── config.py              env loading + validation
├── probe.py               one cheap call per domain to classify capabilities
├── orchestrator.py        ThreadPoolExecutor for the independent 4,
│                          then sequential models -> permissions with prior
├── formatters.py          JSON, rich table, rich summary card
├── collectors/            one module per domain, independent
│   ├── jobs.py
│   ├── clusters.py
│   ├── serving.py
│   ├── unity_catalog.py
│   ├── models.py          (UC + MLflow + agent-endpoint heuristic)
│   └── permissions.py     (workspace ACL + UC grants, normalized)
main.py                    CLI entrypoint at repo root
tests/                     pytest + responses, 21 tests, no live calls
```

Separation of concerns is strict: collectors never touch HTTP
directly, never handle pagination, never format output. Formatters
never make network calls. This is what lets formatters stay dumb and
collectors stay testable.

### Collector contract

Every collector exposes the same signature and return shape:

```python
def collect(client, config, **kwargs) -> dict:
    return {
        "status": "available" | "unavailable" | "partial",
        "reason": str | None,   # populated when status != "available"
        "summary": {...},
        "detail": [...] | {...},
    }
```

`models` and `permissions` additionally accept `prior=<results_so_far>`
because they cross-reference other collectors' output (UC model
full_name matching vs serving, and object-ID enumeration
respectively). The orchestrator runs them sequentially after the
concurrent phase so `prior` is fully populated.

## Gotchas worth calling out

### Two permissions API surfaces

Databricks has two unrelated permissions APIs and both are needed for
a coherent picture:

- `/api/2.0/permissions/{object_type}/{object_id}` — workspace ACLs
  (jobs, clusters, serving endpoints, etc). Principal type is encoded
  in the field name (`user_name` / `group_name` /
  `service_principal_name`). Each permission carries its own
  `inherited` flag plus an `inherited_from_object` parent-path list.
- `/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}` —
  UC grants (catalogs, schemas, tables, functions). Principal type is
  **not** distinguished — every principal is just a string. No
  inherited flag either; inheritance lives on `effective-permissions`
  which we don't call to keep the call count bounded.

The `permissions` collector normalizes both into one per-grant shape:

```json
{
  "principal":        "alice@example.com",
  "principal_type":   "user | group | service_principal | unknown",
  "permission_level": "CAN_MANAGE | SELECT | ...",
  "source":           "workspace | uc",
  "object_type":      "serving-endpoint | catalog | schema | ...",
  "object_id":        "<id or full_name>",
  "inherited":        true | false | null,
  "inherited_from":   "/parent/path | null"
}
```

Permissions is **sampled**, not exhaustive. `--sample-permissions N`
caps calls per object type per surface. Full per-object audits belong
in system tables via the SQL Statement Execution API against a
serverless warehouse — that's the natural v2.

### Serving endpoints — OpenMetrics text, not JSON

`/api/2.0/serving-endpoints/{name}/metrics` returns OpenMetrics text,
parsed with `prometheus_client.parser.text_string_to_metric_families`.

On this workspace, three gauge families are exposed —
`request_count_total`, `request_4xx_count_total`,
`request_5xx_count_total` — all per-last-minute. Despite the `_total`
suffix, they're declared as gauges, not counters. The parser retains
`_total` on gauges and strips it on counters, so we match both forms.

The `request_latency_ms` histogram that CLAUDE.md calls for is **not
present** on this workspace's endpoints. The parser still handles
histograms when they appear (future-proof with linear interpolation
across bucket upper bounds), but `p50_ms` / `p95_ms` come back `None`
on Free Edition endpoints.

### Graceful degradation on 403 / 404

Each domain is probed with one cheap call before the full collector
runs. A 403 short-circuits that collector to `status="unavailable"`
with a reason — no time wasted, no exception thrown. On this
workspace today: MLflow workspace model registry is hard-disabled,
foundation-model serving endpoints 404 on `/metrics`, and Free Edition
has no jobs or clusters. All of these produce a clean report.

Specific case: the workspace MLflow registry returns 403 with body
text "legacy workspace model registry is disabled for the current
Databricks workspace". The `models` collector distinguishes this case
from a generic 403 and reports `workspace_registry_status="disabled"`
rather than `"unavailable"`.

## Testing

```bash
pytest tests/ -v
```

21 tests, all offline via `responses`:

- **client**: retry on 429/5xx with backoff, retry exhaustion, 401
  fast-fail (no retry), three-page pagination, one-page fallback,
  token redaction in `repr`.
- **jobs**: hand-computed p50/p95 from a 5-duration fixture; failure
  rate excludes `FAILED` / `TIMEDOUT` from the duration pool but
  counts them for the rate.
- **unity_catalog**: nested catalogs → schemas (paginated) → tables +
  functions; `--uc-max-schemas` guardrail short-circuits mid-stream
  and marks truncated catalogs in detail; `--uc-scope` narrows to one
  catalog.
- **permissions**: workspace ACL and UC grant produce rows with the
  same key set; principal type derived from field name on the
  workspace side; inherited parent paths captured.
- **graceful_degradation**: each collector returns `unavailable` (or
  `partial`) with a populated `reason` when its primary endpoint 403s.

Tests use `responses` to intercept `requests.Session` calls; no live
API is ever hit. A zero-backoff `Retry` override is mounted inside
retry tests so the suite doesn't wait the full 1s/2s/4s schedule.

## Out of scope for the POC

- Writing to Databricks (any create/update/delete)
- Historical data via system tables (v2 via SQL Statement Execution API)
- Cost metrics (requires billing system tables)
- OAuth M2M authentication (PAT only for the POC)
- Dashboards / alerting integrations
- Any use of the Databricks SDK — the point of this POC is to
  demonstrate raw REST understanding, which the SDK would hide

## v2 direction

The natural next step is moving audit-grade questions from REST onto
system tables. A serverless SQL warehouse + the SQL Statement
Execution API gives exhaustive, historical answers that the REST
permissions endpoints can only sample:

- `system.access.audit` for user activity
- `system.access.table_lineage` / `column_lineage` for data dependency
- `system.information_schema.table_privileges` for the exhaustive
  grant picture that `--sample-permissions` only approximates
- `system.billing.usage` for cost

Auth becomes OAuth M2M with a service principal — PAT is a human
credential and doesn't belong in a production collector.
