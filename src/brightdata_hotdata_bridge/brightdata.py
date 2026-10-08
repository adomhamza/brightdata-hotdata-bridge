"""Async client for the Bright Data Scraper API (``/datasets/v3``).

Three calls drive a collection: ``trigger`` starts it and returns a ``snapshot_id``,
``progress`` reports its status, and ``snapshot`` downloads the results once ready.

Rate limiting is handled conservatively. Bright Data blacklists an IP after 25
``429`` responses in five minutes, so every ``429`` waits for ``Retry-After`` (or 2, 4,
8, 16 then 32 seconds) before one retry. ``trigger`` is not idempotent, so it is
only retried when the request provably never reached the server.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from http import HTTPStatus
from pathlib import Path
from typing import Any

import httpx

from brightdata_hotdata_bridge.__about__ import __version__
from brightdata_hotdata_bridge.config import Settings, require_setting
from brightdata_hotdata_bridge.errors import (
    BrightDataApiError,
    CollectionFailedError,
    RateLimitedError,
    SnapshotTimeoutError,
)
from brightdata_hotdata_bridge.models import (
    CollectionMethod,
    SnapshotProgress,
    validate_snapshot_id,
)

logger = logging.getLogger(__name__)

BACKOFF_SECONDS: tuple[float, ...] = (2, 4, 8, 16, 32)
MAX_RETRY_AFTER_SECONDS = 120.0
MAX_ERROR_BODY_CHARS = 2000
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
SNAPSHOT_FORMAT = "ndjson"

ProgressCallback = Callable[[SnapshotProgress], Awaitable[None] | None]
Sleep = Callable[[float], Awaitable[None]]


def create_brightdata_client(settings: Settings) -> httpx.AsyncClient:
    """Create an HTTP client authenticated against the Bright Data API.

    Args:
        settings: Supplies the API key, base URL and timeout.

    Returns:
        A client the caller must close (use it as an ``async with`` context manager).

    Raises:
        ConfigurationError: ``BRIGHTDATA_API_KEY`` is not set.
    """
    api_key = require_setting(settings.brightdata_api_key, "BRIGHTDATA_API_KEY")
    return httpx.AsyncClient(
        base_url=settings.brightdata_base_url,
        headers={
            "Authorization": f"Bearer {api_key.get_secret_value()}",
            "User-Agent": f"brightdata-hotdata-bridge/{__version__}",
        },
        timeout=httpx.Timeout(settings.http_timeout_seconds),
        follow_redirects=True,
    )


async def trigger_collection(
    client: httpx.AsyncClient,
    *,
    dataset_id: str,
    method: CollectionMethod,
    inputs: Sequence[dict[str, Any]],
    limit_per_input: int | None = None,
    sleep: Sleep = asyncio.sleep,
) -> str:
    """Start an asynchronous collection and return its snapshot id.

    Args:
        client: Client from :func:`create_brightdata_client`.
        dataset_id: Scraper to run.
        method: Collection method; discovery methods add ``type=discover_new`` and
            ``discover_by=<suffix>`` to the request.
        inputs: Validated input objects, one per URL or query.
        limit_per_input: Cap on records per input for discovery methods.
        sleep: Awaitable sleep, injectable for tests.

    Returns:
        The ``snapshot_id`` identifying the collection.

    Raises:
        BrightDataApiError: Bright Data rejected the request or returned no snapshot id.
        RateLimitedError: Still rate limited after every backoff step.

    Example:
        ``POST /datasets/v3/trigger?dataset_id=gd_...&include_errors=true``
        with body ``{"input": [{"url": "https://..."}]}``.
    """
    params: dict[str, str] = {"dataset_id": dataset_id, "include_errors": "true"}
    if method.discover_by is not None:
        params |= {"type": "discover_new", "discover_by": method.discover_by}
    body: dict[str, Any] = {"input": list(inputs)}
    if limit_per_input is not None:
        body["limit_per_input"] = limit_per_input

    response = await _send(
        client,
        "POST",
        "/datasets/v3/trigger",
        idempotent=False,
        sleep=sleep,
        params=params,
        json=body,
    )
    if response.status_code != HTTPStatus.OK:
        raise _api_error("Bright Data rejected the collection request", response)

    snapshot_id = _json(response).get("snapshot_id")
    if not isinstance(snapshot_id, str):
        raise _api_error("Bright Data did not return a snapshot_id", response)
    validate_snapshot_id(snapshot_id)
    logger.info("Collection triggered", extra={"snapshot_id": snapshot_id, "rows": len(inputs)})
    return snapshot_id


async def get_progress(
    client: httpx.AsyncClient, snapshot_id: str, *, sleep: Sleep = asyncio.sleep
) -> SnapshotProgress:
    """Fetch the current status of a snapshot.

    Args:
        client: Client from :func:`create_brightdata_client`.
        snapshot_id: Snapshot to inspect.
        sleep: Awaitable sleep, injectable for tests.

    Returns:
        The snapshot status: ``starting``, ``running``, ``ready``, ``failed`` or ``canceled``.

    Raises:
        BrightDataApiError: The status could not be retrieved.
        RateLimitedError: Still rate limited after every backoff step.
    """
    response = await _send(
        client,
        "GET",
        f"/datasets/v3/progress/{validate_snapshot_id(snapshot_id)}",
        idempotent=True,
        sleep=sleep,
    )
    if response.status_code != HTTPStatus.OK:
        raise _api_error("Could not read snapshot progress", response, snapshot_id=snapshot_id)
    return SnapshotProgress.model_validate({**_json(response), "snapshot_id": snapshot_id})


async def wait_for_snapshot(
    client: httpx.AsyncClient,
    snapshot_id: str,
    *,
    poll_interval: float,
    timeout: float,
    on_progress: ProgressCallback | None = None,
    sleep: Sleep = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> SnapshotProgress:
    """Poll a snapshot until it is ready, failed, or the timeout expires.

    Args:
        client: Client from :func:`create_brightdata_client`.
        snapshot_id: Snapshot to wait for.
        poll_interval: Seconds between status checks.
        timeout: Maximum seconds to wait.
        on_progress: Called with every status update (sync or async).
        sleep: Awaitable sleep, injectable for tests.
        clock: Monotonic clock, injectable for tests.

    Returns:
        The final ``ready`` progress.

    Raises:
        CollectionFailedError: Bright Data reports ``failed`` or ``canceled``. No data
            should be written for this snapshot.
        SnapshotTimeoutError: Still not ready after ``timeout`` seconds. Polling can be
            resumed later with the same snapshot id.
        BrightDataApiError: Status checks keep failing.
    """
    deadline = clock() + timeout
    while True:
        progress = await get_progress(client, snapshot_id, sleep=sleep)
        if on_progress is not None:
            outcome = on_progress(progress)
            if outcome is not None:
                await outcome
        if progress.is_ready:
            return progress
        if progress.is_failed:
            detail = f": {progress.error_message}" if progress.error_message else ""
            raise CollectionFailedError(
                f"Bright Data collection ended as {progress.status}{detail}",
                snapshot_id=snapshot_id,
                status=progress.status,
            )
        if clock() + poll_interval > deadline:
            raise SnapshotTimeoutError(
                f"Snapshot not ready after {timeout:.0f}s (last status: {progress.status}). "
                "Run `bdh push --snapshot-id` again later to resume.",
                snapshot_id=snapshot_id,
            )
        await sleep(poll_interval)


async def download_snapshot(
    client: httpx.AsyncClient,
    snapshot_id: str,
    destination: Path,
    *,
    poll_interval: float,
    timeout: float,
    sleep: Sleep = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Path:
    """Stream a ready snapshot to ``destination`` as NDJSON without buffering it in memory.

    The file is written to a temporary name and renamed on success, so a partial
    download never looks complete. A ``202`` (snapshot still being built) is retried
    until ``timeout``.

    Args:
        client: Client from :func:`create_brightdata_client`.
        snapshot_id: Ready snapshot to download.
        destination: Final file path.
        poll_interval: Seconds between retries while the snapshot is still building.
        timeout: Maximum seconds to wait for the snapshot to become downloadable.
        sleep: Awaitable sleep, injectable for tests.
        clock: Monotonic clock, injectable for tests.

    Returns:
        ``destination``.

    Raises:
        BrightDataApiError: The download failed.
        SnapshotTimeoutError: The snapshot stayed in the building state too long.
        RateLimitedError: Still rate limited after every backoff step.
    """
    url = f"/datasets/v3/snapshot/{validate_snapshot_id(snapshot_id)}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".part")
    deadline = clock() + timeout
    backoff = iter(BACKOFF_SECONDS)

    while True:
        try:
            async with client.stream("GET", url, params={"format": SNAPSHOT_FORMAT}) as response:
                if response.status_code == HTTPStatus.OK:
                    await _stream_to_file(response, temp_path)
                    temp_path.replace(destination)
                    logger.info("Snapshot downloaded", extra={"snapshot_id": snapshot_id})
                    return destination
                await response.aread()
        except httpx.TransportError as exc:
            delay = next(backoff, None)
            if delay is None:
                raise BrightDataApiError(
                    f"Snapshot download failed: {exc}", snapshot_id=snapshot_id
                ) from exc
            logger.warning("Download interrupted (%s); retrying in %ss", exc, delay)
            await sleep(delay)
            continue

        if response.status_code == HTTPStatus.ACCEPTED:
            if clock() + poll_interval > deadline:
                raise SnapshotTimeoutError(
                    "Snapshot is still being built; try again later", snapshot_id=snapshot_id
                )
            await sleep(poll_interval)
            continue
        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            await sleep(_rate_limit_delay(response, next(backoff, None), snapshot_id))
            continue
        raise _api_error("Snapshot download failed", response, snapshot_id=snapshot_id)


async def _stream_to_file(response: httpx.Response, path: Path) -> None:
    with path.open("wb") as handle:
        async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_BYTES):
            handle.write(chunk)


async def _send(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    idempotent: bool,
    sleep: Sleep,
    **kwargs: Any,
) -> httpx.Response:
    backoff = iter(BACKOFF_SECONDS)
    while True:
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            never_sent = isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout))
            delay = next(backoff, None)
            if not (idempotent or never_sent) or delay is None:
                raise BrightDataApiError(f"Could not reach Bright Data: {exc}") from exc
            logger.warning("Bright Data unreachable (%s); retrying in %ss", exc, delay)
            await sleep(delay)
            continue

        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            await sleep(_rate_limit_delay(response, next(backoff, None), None))
            continue
        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR and idempotent:
            delay = next(backoff, None)
            if delay is not None:
                logger.warning(
                    "Bright Data returned %s; retrying in %ss", response.status_code, delay
                )
                await sleep(delay)
                continue
        return response


def _rate_limit_delay(
    response: httpx.Response, fallback: float | None, snapshot_id: str | None
) -> float:
    if fallback is None:
        raise RateLimitedError(
            "Bright Data is still rate limiting after repeated backoff; slow down and retry later",
            status_code=response.status_code,
            snapshot_id=snapshot_id,
        )
    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
    delay = min(retry_after, MAX_RETRY_AFTER_SECONDS) if retry_after is not None else fallback
    logger.warning("Bright Data rate limit hit; waiting %ss", delay)
    return delay


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        return None


def _json(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _api_error(
    message: str, response: httpx.Response, *, snapshot_id: str | None = None
) -> BrightDataApiError:
    body = response.text[:MAX_ERROR_BODY_CHARS]
    return BrightDataApiError(
        f"{message} (HTTP {response.status_code}): {body}",
        status_code=response.status_code,
        body=body,
        snapshot_id=snapshot_id,
    )
