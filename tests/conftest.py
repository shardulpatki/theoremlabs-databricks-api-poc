"""Shared test fixtures.

``make_client`` produces a ``DatabricksClient`` pointed at a fake host
and with a placeholder token — the ``responses`` library intercepts
every outbound call, so the token never hits the network.
"""

from __future__ import annotations

import pytest

from dbx_metrics.client import DatabricksClient

FAKE_HOST = "https://test.databricks.fake"


@pytest.fixture
def client() -> DatabricksClient:
    return DatabricksClient(host=FAKE_HOST, token="test-token")


@pytest.fixture
def host() -> str:
    return FAKE_HOST
