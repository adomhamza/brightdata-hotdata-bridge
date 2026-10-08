from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import respx

from brightdata_hotdata_bridge.brightdata import (
    create_brightdata_client,
    download_snapshot,
    trigger_collection,
    wait_for_snapshot,
)
from brightdata_hotdata_bridge.catalog import parse_catalog, resolve_method
from brightdata_hotdata_bridge.config import Settings
from brightdata_hotdata_bridge.errors import (
    BrightDataApiError,
    CollectionFailedError,
    RateLimitedError,
    SnapshotTimeoutError,
)
from brightdata_hotdata_bridge.models import CollectionMethod
from tests.conftest import AMAZON_ID, BASE_URL, CATALOG, SNAPSHOT_ID, FakeClock, ndjson

TRIGGER_URL = f"{BASE_URL}/datasets/v3/trigger"
PROGRESS_URL = f"{BASE_URL}/datasets/v3/progress/{SNAPSHOT_ID}"
SNAPSHOT_URL = f"{BASE_URL}/datasets/v3/snapshot/{SNAPSHOT_ID}"


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    async with create_brightdata_client(settings) as http_client:
        yield http_client


def method(name: str = "collect_by_url") -> CollectionMethod:
    return resolve_method(parse_catalog(json.dumps(CATALOG)), AMAZON_ID, name)[1]


@respx.mock
async def test_trigger_sends_inputs_and_auth(client: httpx.AsyncClient) -> None:
    route = respx.post(TRIGGER_URL).mock(
        return_value=httpx.Response(200, json={"snapshot_id": SNAPSHOT_ID})
    )

    snapshot_id = await trigger_collection(
        client, dataset_id=AMAZON_ID, method=method(), inputs=[{"url": "https://a.com"}]
    )

    request = route.calls.last.request
    assert snapshot_id == SNAPSHOT_ID
    assert request.headers["Authorization"] == "Bearer bd-key"
    assert dict(request.url.params) == {"dataset_id": AMAZON_ID, "include_errors": "true"}
    assert json.loads(request.content) == {"input": [{"url": "https://a.com"}]}


@respx.mock
async def test_discovery_trigger_sets_discover_params_and_limit(client: httpx.AsyncClient) -> None:
    route = respx.post(TRIGGER_URL).mock(
        return_value=httpx.Response(200, json={"snapshot_id": SNAPSHOT_ID})
    )

    await trigger_collection(
        client,
        dataset_id=AMAZON_ID,
        method=method("discover_by_keyword"),
        inputs=[{"keyword": "tv"}],
        limit_per_input=25,
    )

    request = route.calls.last.request
    assert request.url.params["type"] == "discover_new"
    assert request.url.params["discover_by"] == "keyword"
    assert json.loads(request.content)["limit_per_input"] == 25


@respx.mock
async def test_rate_limit_honours_retry_after(client: httpx.AsyncClient, clock: FakeClock) -> None:
    respx.post(TRIGGER_URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, json={"snapshot_id": SNAPSHOT_ID}),
        ]
    )

    await trigger_collection(
        client,
        dataset_id=AMAZON_ID,
        method=method(),
        inputs=[{"url": "https://a.com"}],
        sleep=clock.sleep,
    )

    assert clock.sleeps == [7.0]


@respx.mock
async def test_persistent_rate_limit_stops_after_backoff(
    client: httpx.AsyncClient, clock: FakeClock
) -> None:
    route = respx.post(TRIGGER_URL).mock(return_value=httpx.Response(429))

    with pytest.raises(RateLimitedError):
        await trigger_collection(
            client,
            dataset_id=AMAZON_ID,
            method=method(),
            inputs=[{"url": "https://a.com"}],
            sleep=clock.sleep,
        )

    assert clock.sleeps == [2, 4, 8, 16, 32]
    assert route.call_count == 6


@respx.mock
async def test_trigger_server_error_is_not_retried(client: httpx.AsyncClient) -> None:
    route = respx.post(TRIGGER_URL).mock(return_value=httpx.Response(500, text="boom"))

    with pytest.raises(BrightDataApiError, match="HTTP 500"):
        await trigger_collection(
            client, dataset_id=AMAZON_ID, method=method(), inputs=[{"url": "https://a.com"}]
        )

    assert route.call_count == 1


@respx.mock
async def test_wait_polls_until_ready(client: httpx.AsyncClient, clock: FakeClock) -> None:
    respx.get(PROGRESS_URL).mock(
        side_effect=[
            httpx.Response(200, json={"status": "starting"}),
            httpx.Response(200, json={"status": "running"}),
            httpx.Response(200, json={"status": "ready", "dataset_id": AMAZON_ID, "records": 3}),
        ]
    )
    seen: list[str] = []

    progress = await wait_for_snapshot(
        client,
        SNAPSHOT_ID,
        poll_interval=10,
        timeout=600,
        on_progress=lambda p: seen.append(p.status),
        sleep=clock.sleep,
        clock=clock,
    )

    assert progress.is_ready
    assert progress.records == 3
    assert seen == ["starting", "running", "ready"]
    assert clock.sleeps == [10, 10]


@respx.mock
async def test_failed_collection_raises(client: httpx.AsyncClient, clock: FakeClock) -> None:
    respx.get(PROGRESS_URL).mock(
        return_value=httpx.Response(200, json={"status": "failed", "error_message": "bad input"})
    )

    with pytest.raises(CollectionFailedError, match="failed: bad input") as excinfo:
        await wait_for_snapshot(
            client, SNAPSHOT_ID, poll_interval=1, timeout=60, sleep=clock.sleep, clock=clock
        )

    assert excinfo.value.snapshot_id == SNAPSHOT_ID


@respx.mock
async def test_wait_times_out(client: httpx.AsyncClient, clock: FakeClock) -> None:
    respx.get(PROGRESS_URL).mock(return_value=httpx.Response(200, json={"status": "running"}))

    with pytest.raises(SnapshotTimeoutError, match="last status: running"):
        await wait_for_snapshot(
            client, SNAPSHOT_ID, poll_interval=10, timeout=30, sleep=clock.sleep, clock=clock
        )

    assert clock.now <= 30


@respx.mock
async def test_download_waits_while_building_and_writes_atomically(
    client: httpx.AsyncClient, clock: FakeClock, tmp_path: Path
) -> None:
    body = ndjson({"title": "a"}, {"title": "b"})
    route = respx.get(SNAPSHOT_URL).mock(
        side_effect=[
            httpx.Response(202, json={"status": "building"}),
            httpx.Response(200, content=body),
        ]
    )
    destination = tmp_path / "raw.ndjson"

    await download_snapshot(
        client,
        SNAPSHOT_ID,
        destination,
        poll_interval=5,
        timeout=60,
        sleep=clock.sleep,
        clock=clock,
    )

    assert destination.read_bytes() == body
    assert not destination.with_suffix(".ndjson.part").exists()
    assert route.calls.last.request.url.params["format"] == "ndjson"
    assert clock.sleeps == [5]


@respx.mock
async def test_download_error_is_raised(client: httpx.AsyncClient, tmp_path: Path) -> None:
    respx.get(SNAPSHOT_URL).mock(return_value=httpx.Response(404, text="not found"))

    with pytest.raises(BrightDataApiError, match="HTTP 404"):
        await download_snapshot(
            client, SNAPSHOT_ID, tmp_path / "raw.ndjson", poll_interval=1, timeout=5
        )


async def test_snapshot_ids_cannot_escape_paths(client: httpx.AsyncClient, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid snapshot id"):
        await download_snapshot(
            client, "../../etc", tmp_path / "x.ndjson", poll_interval=1, timeout=1
        )
