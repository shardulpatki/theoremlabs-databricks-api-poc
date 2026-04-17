"""Output formatters.

Pure functions over a report dict. Formatters never make network calls
and never decide what to collect — they only present what orchestrator
hands them. The report shape is:

    {
        "capabilities": {domain: {"available": bool, "reason": str|None}},
        "domains": {
            domain: {"status": ..., "reason": ..., "summary": {...}, "detail": ...},
            ...
        },
    }

Three formatters share the contract ``format_X(report) -> str``:

- ``format_json``: pretty-printed JSON, non-destructive fallback.
- ``format_summary``: one compact panel, one row per domain, headline
  metric per domain. Intended for the demo "one-page card".
- ``format_table``: per-domain section with a summary table and a
  bounded detail table (first ``_DETAIL_ROW_CAP`` rows, with a footer
  row noting any truncation).
"""

from __future__ import annotations

import json
from io import StringIO

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

_DETAIL_ROW_CAP = 20

_STATUS_STYLE = {
    "available": "green",
    "partial": "yellow",
    "unavailable": "red",
}


def format_json(report: dict) -> str:
    return json.dumps(report, indent=2, default=str, sort_keys=False)


def format_summary(report: dict) -> str:
    domains = report.get("domains") or {}
    table = Table(show_header=True, header_style="bold", box=None, pad_edge=False)
    table.add_column("Domain", style="bold")
    table.add_column("Status")
    table.add_column("Headline")
    table.add_column("Reason", overflow="fold")

    for name in _domain_order(domains):
        result = domains[name]
        status = result.get("status") or "unavailable"
        headline = _headline(name, result)
        reason = result.get("reason") or ""
        table.add_row(
            name,
            Text(status, style=_STATUS_STYLE.get(status, "")),
            headline,
            Text(reason, style="dim") if reason else "",
        )

    return _render(Panel(table, title="Databricks workspace — summary", expand=False))


def format_table(report: dict) -> str:
    domains = report.get("domains") or {}
    caps = report.get("capabilities") or {}

    console = _console()
    # Capabilities overview up top so the reader sees what was probed.
    cap_table = Table(title="Capability probe", show_header=True, header_style="bold")
    cap_table.add_column("Domain")
    cap_table.add_column("Available")
    cap_table.add_column("Reason", overflow="fold")
    for name in _domain_order(caps):
        entry = caps[name]
        avail = entry.get("available")
        cap_table.add_row(
            name,
            Text("yes" if avail else "no", style="green" if avail else "red"),
            entry.get("reason") or "",
        )
    console.print(cap_table)

    for name in _domain_order(domains):
        result = domains[name]
        console.print()
        console.print(_domain_header(name, result))
        summary_table = _render_summary_dict(result.get("summary") or {})
        if summary_table is not None:
            console.print(summary_table)
        detail_table = _render_detail(name, result)
        if detail_table is not None:
            console.print(detail_table)

    return console.file.getvalue()


# ---------- helpers ---------------------------------------------------------


def _console() -> Console:
    # Fixed width keeps table output stable across terminals; StringIO
    # captures rendered text while preserving the table layout. We
    # intentionally disable ANSI color so the output is legal in logs,
    # pipes, and redirection targets. The `summary` formatter keeps
    # colors on — it's meant for the terminal.
    return Console(
        file=StringIO(),
        force_terminal=False,
        color_system=None,
        width=120,
        record=False,
    )


def _render(obj) -> str:
    # Color on: `format_summary` is a human-facing card.
    c = Console(file=StringIO(), force_terminal=True, color_system="truecolor", width=120)
    c.print(obj)
    return c.file.getvalue()


def _domain_order(domains: dict) -> list[str]:
    preferred = ["jobs", "clusters", "serving", "unity_catalog", "models", "permissions"]
    seen = [d for d in preferred if d in domains]
    extras = [d for d in domains if d not in preferred]
    return seen + sorted(extras)


def _domain_header(name: str, result: dict) -> Panel:
    status = result.get("status") or "unavailable"
    reason = result.get("reason")
    text = Text.assemble(
        (name, "bold"),
        "  ",
        (status, _STATUS_STYLE.get(status, "")),
    )
    if reason:
        text.append(f"  — {reason}", style="dim")
    return Panel(text, expand=False, padding=(0, 1))


def _render_summary_dict(summary: dict) -> Table | None:
    if not summary:
        return None
    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column("Metric", style="bold")
    table.add_column("Value", overflow="fold")
    for key, value in summary.items():
        table.add_row(key, _scalar(value))
    return table


def _render_detail(name: str, result: dict) -> Table | None:
    detail = result.get("detail")
    if not detail:
        return None
    renderer = _DETAIL_RENDERERS.get(name)
    if renderer is None:
        return None
    return renderer(detail)


def _scalar(value) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.3f}" if abs(value) < 1000 else f"{value:,.0f}"
    if isinstance(value, dict):
        if not value:
            return "{}"
        return ", ".join(f"{k}={v}" for k, v in value.items())
    if isinstance(value, (list, tuple, set)):
        if not value:
            return "[]"
        head = list(value)[:5]
        tail = "" if len(value) <= 5 else f", +{len(value) - 5}"
        return ", ".join(str(x) for x in head) + tail
    return str(value)


def _bounded(items: list, table: Table, renderer) -> Table:
    shown = items[:_DETAIL_ROW_CAP]
    for row in shown:
        renderer(row)
    if len(items) > _DETAIL_ROW_CAP:
        spacer = [Text(f"… +{len(items) - _DETAIL_ROW_CAP} more", style="dim")]
        spacer += [""] * (len(table.columns) - 1)
        table.add_row(*spacer)
    return table


# ---------- headline per domain --------------------------------------------


def _headline(name: str, result: dict) -> str:
    s = result.get("summary") or {}
    if not s:
        return "—"
    if name == "jobs":
        return (
            f"jobs={s.get('total_jobs', 0)}  "
            f"runs_sampled={s.get('run_count_sampled', 0)}  "
            f"p95={_fmt_ms(s.get('p95_duration_ms'))}  "
            f"failure_rate={_fmt_pct(s.get('failure_rate'))}"
        )
    if name == "clusters":
        return (
            f"total={s.get('total', 0)}  "
            f"running={s.get('running', 0)}  "
            f"terminated={s.get('terminated', 0)}"
        )
    if name == "serving":
        return (
            f"endpoints={s.get('endpoint_count', 0)}  "
            f"ready={s.get('ready_count', 0)}  "
            f"metrics={s.get('metrics_covered_count', 0)}/{s.get('endpoint_count', 0)}  "
            f"req_last_min={int(s.get('total_requests_last_min') or 0)}"
        )
    if name == "unity_catalog":
        return (
            f"catalogs={s.get('catalog_count', 0)}/{s.get('catalogs_total_in_metastore', 0)}  "
            f"schemas={s.get('schemas_visited', 0)}/{s.get('schemas_cap', 0)}  "
            f"tables={s.get('table_count', 0)}  functions={s.get('function_count', 0)}"
        )
    if name == "permissions":
        gf = s.get("grants_found") or {}
        return (
            f"grants={gf.get('workspace', 0)}w+{gf.get('uc', 0)}uc  "
            f"sampled={s.get('objects_sampled_total', 0)}  "
            f"no_grants={s.get('objects_with_no_grants_count', 0)}  "
            f"errors={s.get('lookup_errors_count', 0)}"
        )
    if name == "models":
        return (
            f"uc_models={s.get('uc_model_count', 0)}  "
            f"agents={s.get('agent_endpoint_count', 0)}  "
            f"foundation={s.get('foundation_endpoint_count', 0)}  "
            f"custom={s.get('custom_endpoint_count', 0)}"
        )
    return _scalar(s)


def _fmt_ms(value) -> str:
    if value is None:
        return "—"
    return f"{int(value):,}ms"


def _fmt_pct(value) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"


# ---------- per-domain detail renderers ------------------------------------


def _detail_jobs(detail) -> Table:
    t = Table(title="Jobs (detail)", show_header=True, header_style="bold")
    t.add_column("job_id")
    t.add_column("name", overflow="fold")
    t.add_column("creator", overflow="fold")
    t.add_column("runs_sampled", justify="right")
    t.add_column("failure_rate", justify="right")
    t.add_column("active_schedule", justify="center")

    def add(row):
        t.add_row(
            str(row.get("job_id") or ""),
            str(row.get("name") or ""),
            str(row.get("creator") or ""),
            str(row.get("run_count") or 0),
            _fmt_pct(row.get("failure_rate")),
            "yes" if row.get("has_active_schedule") else "no",
        )

    return _bounded(list(detail), t, add)


def _detail_clusters(detail) -> Table:
    t = Table(title="Clusters (detail)", show_header=True, header_style="bold")
    t.add_column("cluster_id")
    t.add_column("name", overflow="fold")
    t.add_column("state")
    t.add_column("spark_version")
    t.add_column("node_type")
    t.add_column("source")

    def add(row):
        state = row.get("state") or ""
        style = "green" if state == "RUNNING" else "dim"
        t.add_row(
            str(row.get("cluster_id") or ""),
            str(row.get("cluster_name") or ""),
            Text(state, style=style),
            str(row.get("spark_version") or ""),
            str(row.get("node_type_id") or ""),
            str(row.get("cluster_source") or ""),
        )

    return _bounded(list(detail), t, add)


def _detail_serving(detail) -> Table:
    t = Table(title="Serving endpoints (detail)", show_header=True, header_style="bold")
    t.add_column("name", overflow="fold")
    t.add_column("ready")
    t.add_column("task")
    t.add_column("type")
    t.add_column("req/min", justify="right")
    t.add_column("err_rate", justify="right")
    t.add_column("p95_ms", justify="right")

    def add(row):
        t.add_row(
            str(row.get("name") or ""),
            str(row.get("state_ready") or ""),
            str(row.get("task") or "—"),
            str(row.get("endpoint_type") or "—"),
            _scalar(row.get("requests_last_min")),
            _fmt_pct(row.get("error_rate_last_min")),
            _fmt_ms(row.get("p95_ms")) if row.get("p95_ms") is not None else "—",
        )

    return _bounded(list(detail), t, add)


def _detail_uc(detail) -> Table:
    t = Table(title="Unity Catalog (per-catalog)", show_header=True, header_style="bold")
    t.add_column("catalog")
    t.add_column("type")
    t.add_column("owner", overflow="fold")
    t.add_column("schemas", justify="right")
    t.add_column("tables", justify="right")
    t.add_column("functions", justify="right")
    t.add_column("note", overflow="fold")

    def add(cat):
        schemas = cat.get("schemas") or []
        tables_total = sum((s or {}).get("table_count", 0) for s in schemas)
        fns_total = sum((s or {}).get("function_count", 0) for s in schemas)
        t.add_row(
            str(cat.get("name") or ""),
            str(cat.get("catalog_type") or ""),
            str(cat.get("owner") or ""),
            str(len(schemas)),
            str(tables_total),
            str(fns_total),
            cat.get("reason") or "",
        )

    return _bounded(list(detail), t, add)


def _detail_permissions(detail) -> Table:
    # Permissions `detail` is a dict, not a flat list — show a sample
    # of normalized grants. The structure: {grants, objects_with_no_grants,
    # lookup_errors}.
    grants = (detail or {}).get("grants") or []
    t = Table(title=f"Permissions — normalized grants (showing up to {_DETAIL_ROW_CAP})",
              show_header=True, header_style="bold")
    t.add_column("source")
    t.add_column("object_type")
    t.add_column("object_id", overflow="fold")
    t.add_column("principal", overflow="fold")
    t.add_column("principal_type")
    t.add_column("permission")
    t.add_column("inherited")

    def add(row):
        inh = row.get("inherited")
        inh_display = "—" if inh is None else ("yes" if inh else "no")
        t.add_row(
            str(row.get("source") or ""),
            str(row.get("object_type") or ""),
            str(row.get("object_id") or ""),
            str(row.get("principal") or ""),
            str(row.get("principal_type") or ""),
            str(row.get("permission_level") or ""),
            inh_display,
        )

    return _bounded(list(grants), t, add)


def _detail_models(detail) -> Table:
    # Models detail is a dict with uc_models + serving_classifications.
    # Show the agent/custom classifications (foundation models are the
    # noisy majority and already counted in summary).
    classifications = (detail or {}).get("serving_classifications") or []
    interesting = [c for c in classifications if c.get("classification") != "foundation"]

    t = Table(title="Models — non-foundation serving endpoints",
              show_header=True, header_style="bold")
    t.add_column("endpoint")
    t.add_column("classification")
    t.add_column("linked_uc_model", overflow="fold")
    t.add_column("task")

    def add(row):
        cls = row.get("classification") or ""
        style = "cyan" if cls == "agent" else "dim"
        t.add_row(
            str(row.get("name") or ""),
            Text(cls, style=style),
            str(row.get("linked_uc_model") or "—"),
            str(row.get("task") or "—"),
        )

    return _bounded(interesting, t, add)


_DETAIL_RENDERERS = {
    "jobs": _detail_jobs,
    "clusters": _detail_clusters,
    "serving": _detail_serving,
    "unity_catalog": _detail_uc,
    "permissions": _detail_permissions,
    "models": _detail_models,
}
