"""Throwaway smoke script for stage 2.

Proves config loading, client retry wiring, and pagination generator
against a real Databricks workspace. Delete or supersede once stage 3
(``main.py``) is runnable.

Run from repo root:

    python scripts/probe_smoke.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dbx_metrics.client import DatabricksClient
from dbx_metrics.config import ConfigError, load_config
from dbx_metrics.probe import probe_capabilities


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        cfg = load_config()
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    print(f"Host: {cfg.host}")
    client = DatabricksClient.from_config(cfg)

    # 1. One-shot GET — user-chosen smoke endpoint.
    resp = client.get("/api/2.0/clusters/list-node-types")
    node_types = resp.get("node_types", []) or []
    print(f"list-node-types returned {len(node_types)} node types.")
    if node_types:
        first = node_types[0]
        print(
            "  first:",
            f"node_type_id={first.get('node_type_id')}",
            f"memory_mb={first.get('memory_mb')}",
            f"num_cores={first.get('num_cores')}",
        )

    # 2. Exercise paginate() with a small page size — proves the
    #    generator works even if the response fits in one page.
    print("\nPaginating /api/2.1/jobs/list (limit=1, cap=3)...")
    count = 0
    for _ in client.paginate(
        "/api/2.1/jobs/list",
        items_key="jobs",
        params={"limit": 1},
    ):
        count += 1
        if count >= 3:
            break
    print(f"  iterated {count} item(s)")

    # 3. Show the retry wiring is actually installed on the session.
    retry = client.session.get_adapter("https://").max_retries
    print(
        "\nRetry: "
        f"total={retry.total} "
        f"backoff_factor={retry.backoff_factor} "
        f"status_forcelist={sorted(retry.status_forcelist)}"
    )

    # 4. Capability probe across all domains.
    print("\nCapability probe:")
    caps = probe_capabilities(client)
    for domain, state in caps.items():
        flag = "OK " if state["available"] else "no "
        reason = f" ({state['reason']})" if state["reason"] else ""
        print(f"  [{flag}] {domain}{reason}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
