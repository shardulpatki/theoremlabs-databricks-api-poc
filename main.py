"""CLI entrypoint for the Databricks metrics POC."""

from __future__ import annotations

import argparse
import logging
import sys

from dbx_metrics.client import AuthError, DatabricksClient
from dbx_metrics.config import CollectorConfig, ConfigError, load_config
from dbx_metrics.formatters import format_json, format_summary, format_table
from dbx_metrics.orchestrator import ALL_DOMAINS, run

_FORMATTERS = {
    "json": format_json,
    "table": format_table,
    "summary": format_summary,
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="dbx-metrics",
        description="Collect metrics from a Databricks workspace via REST APIs.",
    )
    p.add_argument("--format", choices=tuple(_FORMATTERS), default="table")
    p.add_argument(
        "--only",
        type=str,
        default=None,
        help=f"Comma-separated collectors. Available: {','.join(sorted(ALL_DOMAINS))}",
    )
    p.add_argument("--uc-scope", type=str, default=None, metavar="catalog[.schema]")
    p.add_argument("--uc-max-schemas", type=int, default=10)
    p.add_argument("--sample-permissions", type=int, default=10)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def _parse_only(raw: str | None) -> tuple[str, ...] | None:
    if not raw:
        return None
    names = tuple(s.strip() for s in raw.split(",") if s.strip())
    unknown = [n for n in names if n not in ALL_DOMAINS]
    if unknown:
        raise ValueError(
            f"Unknown collector(s): {unknown}. Available: {sorted(ALL_DOMAINS)}"
        )
    return names


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # Rich's table/summary formatters emit box-drawing characters that
    # cp1252 (the Windows default) can't encode. Switch stdout to UTF-8
    # so `print(rendered)` doesn't blow up in PowerShell / cmd.
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        only = _parse_only(args.only)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        app = load_config()
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    cfg = CollectorConfig(
        uc_scope=args.uc_scope,
        uc_max_schemas=args.uc_max_schemas,
        sample_permissions=args.sample_permissions,
        only=only,
    )
    client = DatabricksClient.from_config(app)

    try:
        report = run(client, cfg, only=only)
    except AuthError as exc:
        print(f"Authentication failed: {exc}", file=sys.stderr)
        return 3

    try:
        rendered = _FORMATTERS[args.format](report)
    except NotImplementedError as exc:
        print(f"Formatter not ready: {exc}", file=sys.stderr)
        return 4

    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
