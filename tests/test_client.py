"""Client tests: retry policy and pagination.

Two behaviors here are load-bearing for the rest of the tool:

- 429/5xx retry with backoff: exercised by queuing three responses on
  the same URL (429, 429, 200) and asserting the client eventually
  returns the success payload. The retry adapter pops them in order.
- Pagination: a three-page cascade verifies that ``paginate()`` reads
  ``next_page_token`` from one response and writes it into the next
  request as ``page_token``, terminating on an empty/absent token.

We use an ``HTTPAdapter`` with a zero-backoff ``Retry`` override inside
the test so the retry *logic* is tested without the suite waiting two
minutes for the real backoff schedule. The production client still
uses ``backoff_factor=1.0``.
"""

from __future__ import annotations

import json

import pytest
import responses
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dbx_metrics.client import ClientError, DatabricksClient


def _zero_backoff(client: DatabricksClient) -> None:
    retry = Retry(
        total=3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        backoff_factor=0.0,
        respect_retry_after_header=False,
        raise_on_status=False,
    )
    client.session.mount("https://", HTTPAdapter(max_retries=retry))
    client.session.mount("http://", HTTPAdapter(max_retries=retry))


@responses.activate
def test_retry_recovers_after_two_429s(client, host):
    _zero_backoff(client)
    url = host + "/api/2.1/jobs/list"
    responses.add(responses.GET, url, status=429, body="slow down")
    responses.add(responses.GET, url, status=429, body="slow down")
    responses.add(responses.GET, url, status=200, json={"jobs": [{"job_id": 7}]})

    resp = client.get("/api/2.1/jobs/list")

    assert resp == {"jobs": [{"job_id": 7}]}
    assert len(responses.calls) == 3


@responses.activate
def test_retry_gives_up_after_total_attempts(client, host):
    _zero_backoff(client)
    url = host + "/api/2.1/jobs/list"
    for _ in range(5):
        responses.add(responses.GET, url, status=503, body="busy")

    with pytest.raises(ClientError) as exc_info:
        client.get("/api/2.1/jobs/list")

    assert exc_info.value.status == 503


@responses.activate
def test_401_raises_autherror_without_retry(client, host):
    from dbx_metrics.client import AuthError

    _zero_backoff(client)
    url = host + "/api/2.1/jobs/list"
    responses.add(responses.GET, url, status=401, body="bad token")

    with pytest.raises(AuthError):
        client.get("/api/2.1/jobs/list")

    # 401 is not in the retry forcelist, so exactly one call.
    assert len(responses.calls) == 1


@responses.activate
def test_paginate_three_pages(client, host):
    url = host + "/api/2.1/jobs/list"
    responses.add(responses.GET, url, status=200, json={
        "jobs": [{"job_id": 1}, {"job_id": 2}],
        "next_page_token": "tokA",
    })
    responses.add(responses.GET, url, status=200, json={
        "jobs": [{"job_id": 3}, {"job_id": 4}],
        "next_page_token": "tokB",
    })
    responses.add(responses.GET, url, status=200, json={
        "jobs": [{"job_id": 5}],
        "next_page_token": "",   # empty string terminates, matches real API
    })

    items = list(client.paginate("/api/2.1/jobs/list", items_key="jobs"))

    assert [i["job_id"] for i in items] == [1, 2, 3, 4, 5]
    # Three GETs, each with the previous page's token written into page_token.
    tokens = [call.request.url.split("page_token=")[-1] if "page_token=" in call.request.url else None
              for call in responses.calls]
    assert tokens == [None, "tokA", "tokB"]


@responses.activate
def test_paginate_terminates_on_absent_token(client, host):
    """An endpoint that returns no token at all should stop after one page."""
    url = host + "/api/2.0/clusters/list"
    responses.add(responses.GET, url, status=200, json={
        "clusters": [{"cluster_id": "a"}, {"cluster_id": "b"}],
    })

    items = list(client.paginate("/api/2.0/clusters/list", items_key="clusters"))

    assert [i["cluster_id"] for i in items] == ["a", "b"]
    assert len(responses.calls) == 1


def test_repr_redacts_token(client):
    assert "test-token" not in repr(client)
    assert "***" in repr(client)
