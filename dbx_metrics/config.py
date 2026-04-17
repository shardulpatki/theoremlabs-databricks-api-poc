"""Config loading and validation.

Loads ``DATABRICKS_HOST`` and ``DATABRICKS_TOKEN`` from ``.env`` via
python-dotenv, strips a trailing slash from the host, and fails fast
with a message pointing at ``.env.example`` when required vars are
missing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


_REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class AppConfig:
    host: str
    token: str


@dataclass(frozen=True)
class CollectorConfig:
    uc_scope: str | None = None
    uc_max_schemas: int = 10
    sample_permissions: int = 10
    only: tuple[str, ...] | None = None
    extras: dict = field(default_factory=dict)


class ConfigError(RuntimeError):
    pass


def load_config() -> AppConfig:
    load_dotenv(_REPO_ROOT / ".env")
    host = os.environ.get("DATABRICKS_HOST", "").strip().rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()

    missing = [name for name, val in (("DATABRICKS_HOST", host), ("DATABRICKS_TOKEN", token)) if not val]
    if missing:
        raise ConfigError(
            f"Missing required env var(s): {', '.join(missing)}. "
            f"Copy .env.example to .env and fill in values."
        )
    if not (host.startswith("https://") or host.startswith("http://")):
        raise ConfigError(
            f"DATABRICKS_HOST must be a full URL starting with https:// — got {host!r}"
        )
    return AppConfig(host=host, token=token)
