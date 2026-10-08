"""Publish files to Hotdata: upload to object storage, then load into a managed table.

The official ``hotdata`` SDK is used for both halves:

* ``UploadsApi.upload_file`` runs the whole upload sequence (open a session, single
  or multipart PUTs straight to storage with retries, then finalize).
* ``DatabasesApi.load_database_table`` publishes the upload into a table. Loads run as
  background jobs and are polled, so large files never hold an HTTP request open.

A load from an ``upload_id`` is idempotent (a consumed upload replays its original
result), so busy-table ``409`` responses, ``429`` and server errors are retried safely.

The SDK is synchronous; the pipeline calls these functions with ``asyncio.to_thread``.
"""

from __future__ import annotations

import functools
import json
import logging
import time
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path
from typing import Any, TypeVar

import urllib3
from hotdata import (
    ApiClient,
    Configuration,
    DatabasesApi,
    InformationSchemaApi,
    JobsApi,
    LoadManagedTableRequest,
    LoadManagedTableResponse,
    SubmitJobResponse,
)
from hotdata.exceptions import ApiException
from hotdata.models.job_status import JobStatus
from hotdata.uploads import SessionCreateError, UploadError, UploadsApi

from brightdata_hotdata_bridge.config import Settings, require_setting
from brightdata_hotdata_bridge.errors import ConfigurationError, HotdataWriteError
from brightdata_hotdata_bridge.models import HotdataDatabase, LoadMode, LoadResult, TableTarget

logger = logging.getLogger(__name__)

T = TypeVar("T")

NDJSON_CONTENT_TYPE = "application/x-ndjson"
LOAD_FORMAT = "json"
BACKOFF_SECONDS: tuple[float, ...] = (2, 4, 8, 16, 32)
MAX_RETRY_AFTER_SECONDS = 120.0
JOB_POLL_SECONDS: tuple[float, ...] = (1, 2, 3, 5)
JOB_POLL_MAX_SECONDS = 10.0
INSPECT_PAGE_SIZE = 100
DATABASE_PAGE_SIZE = 100
MAX_LISTED_DATABASES = 10
LOOKUP_STAGE = "database lookup"
_RETRYABLE_STATUSES = frozenset({409, 429, 500, 502, 503, 504})
_FINISHED_WITHOUT_RESULT = frozenset({JobStatus.FAILED, JobStatus.PARTIALLY_SUCCEEDED})


def create_hotdata_client(settings: Settings) -> ApiClient:
    """Create an authenticated Hotdata API client.

    Args:
        settings: Supplies the API key, workspace id and optional host override.

    Returns:
        A client the caller must close (it is a context manager).

    Raises:
        ConfigurationError: ``HOTDATA_API_KEY`` or ``HOTDATA_WORKSPACE_ID`` is not set.
    """
    api_key = require_setting(settings.hotdata_api_key, "HOTDATA_API_KEY")
    configuration = Configuration(
        host=settings.hotdata_host,
        api_key=api_key.get_secret_value(),
        workspace_id=require_setting(settings.hotdata_workspace_id, "HOTDATA_WORKSPACE_ID"),
    )
    return ApiClient(configuration)


def find_database(settings: Settings, *, recorded_id: str | None = None) -> HotdataDatabase:
    """Work out which Hotdata database to publish into.

    Precedence: ``HOTDATA_DATABASE_ID``, then ``HOTDATA_DATABASE_NAME``, then the database
    a previous run of the same snapshot used, then the workspace's only database.

    Args:
        settings: Supplies credentials and the optional database id or name.
        recorded_id: Database used by an earlier run of the same snapshot, so a replay
            keeps publishing to the same place.

    Returns:
        The database to publish into.

    Raises:
        ConfigurationError: The database cannot be identified unambiguously.
        HotdataWriteError: Hotdata could not be queried (``stage="database lookup"``).
    """
    database_id = settings.hotdata_database_id
    if database_id is None and settings.hotdata_database_name is None:
        database_id = recorded_id
    with create_hotdata_client(settings) as api_client:
        database = resolve_database(
            api_client, database_id=database_id, database_name=settings.hotdata_database_name
        )
    logger.info("Publishing to Hotdata database %s (%s)", database.id, database.name or "unnamed")
    return database


def resolve_database(
    api_client: ApiClient,
    *,
    database_id: str | None = None,
    database_name: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> HotdataDatabase:
    """Look up a database by id, by exact name, or as the workspace's only database.

    Args:
        api_client: Client from :func:`create_hotdata_client`.
        database_id: Exact database id; checked to exist.
        database_name: Exact display name, used when no id is given.
        sleep: Sleep function, injectable for tests.

    Returns:
        The matching database.

    Raises:
        ConfigurationError: The id does not exist, or zero or several databases match.
        HotdataWriteError: Hotdata could not be queried (``stage="database lookup"``).
    """
    if database_id is not None:
        return _get_database(api_client, database_id, sleep=sleep)
    databases = list_databases(api_client, sleep=sleep)
    candidates = (
        [database for database in databases if database.name == database_name]
        if database_name is not None
        else databases
    )
    if len(candidates) != 1:
        raise ConfigurationError(_ambiguous_database_message(candidates, database_name))
    return candidates[0]


def list_databases(
    api_client: ApiClient, *, sleep: Callable[[float], None] = time.sleep
) -> list[HotdataDatabase]:
    """List every database in the workspace, newest first.

    Args:
        api_client: Client from :func:`create_hotdata_client`.
        sleep: Sleep function, injectable for tests.

    Returns:
        All databases, across every page.

    Raises:
        HotdataWriteError: Hotdata could not be queried (``stage="database lookup"``).
    """
    databases = DatabasesApi(api_client)
    found: list[HotdataDatabase] = []
    cursor: str | None = None
    while True:
        page = _call_with_retry(
            functools.partial(databases.list_databases, limit=DATABASE_PAGE_SIZE, cursor=cursor),
            stage=LOOKUP_STAGE,
            sleep=sleep,
        )
        found.extend(
            HotdataDatabase(id=item.id, name=item.name, default_schema=item.default_schema)
            for item in page.databases
        )
        if not page.has_more or not page.next_cursor:
            return found
        cursor = page.next_cursor


def upload_file(
    api_client: ApiClient,
    path: Path,
    *,
    snapshot_id: str,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Upload an NDJSON file to Hotdata object storage and finalize it.

    Args:
        api_client: Client from :func:`create_hotdata_client`.
        path: File to upload.
        snapshot_id: Snapshot the file came from; used as the recorded file name.
        sleep: Sleep function, injectable for tests.

    Returns:
        The ``upload_id`` to load from.

    Raises:
        HotdataWriteError: The upload failed after the SDK's own retries (``stage="upload"``).
    """
    uploads = UploadsApi(api_client)

    def _upload() -> str:
        finalized = uploads.upload_file(
            path, content_type=NDJSON_CONTENT_TYPE, filename=f"{snapshot_id}.ndjson"
        )
        return finalized.upload_id

    backoff = iter(BACKOFF_SECONDS)
    while True:
        try:
            upload_id = _upload()
        except SessionCreateError as exc:
            delay = next(backoff, None)
            if exc.status not in _RETRYABLE_STATUSES or delay is None:
                raise _write_error("upload", exc.api_exception, snapshot_id) from exc
            logger.warning(
                "Opening the upload session failed (%s); retrying in %ss", exc.status, delay
            )
            sleep(delay)
            continue
        except UploadError as exc:
            raise HotdataWriteError(
                f"Upload to Hotdata failed: {exc}", stage="upload", snapshot_id=snapshot_id
            ) from exc
        except OSError as exc:
            raise HotdataWriteError(
                f"Cannot read {path} for upload: {exc}", stage="upload", snapshot_id=snapshot_id
            ) from exc
        logger.info(
            "Uploaded to Hotdata", extra={"snapshot_id": snapshot_id, "upload_id": upload_id}
        )
        return upload_id


def table_exists(
    api_client: ApiClient, *, database_id: str, schema: str, table: str
) -> bool | None:
    """Check whether a table already exists in the database's default catalog.

    Args:
        api_client: Client from :func:`create_hotdata_client`.
        database_id: Hotdata database id.
        schema: Schema name.
        table: Table name.

    Returns:
        ``True`` or ``False``, or ``None`` when the check itself failed.
    """
    try:
        database = DatabasesApi(api_client).get_database(database_id)
        information = InformationSchemaApi(api_client)
        cursor: str | None = None
        while True:
            page = information.information_schema(
                connection_id=database.default_connection_id,
                var_schema=schema,
                table=table,
                limit=INSPECT_PAGE_SIZE,
                cursor=cursor,
            )
            if any(item.var_schema == schema and item.table == table for item in page.tables):
                return True
            if not page.has_more or not page.next_cursor:
                return False
            cursor = page.next_cursor
    except (ApiException, urllib3.exceptions.HTTPError) as exc:
        logger.warning("Could not check whether %s.%s exists: %s", schema, table, exc)
        return None


def load_table(
    api_client: ApiClient,
    *,
    database_id: str,
    schema: str,
    target: TableTarget,
    upload_id: str,
    snapshot_id: str,
    timeout: float,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> LoadResult:
    """Publish an upload into a Hotdata table and wait for the load to finish.

    The first load into a table must be ``replace``. When another mode is requested and
    the table provably does not exist yet, ``replace`` is used instead (identical
    outcome for an empty table). If existence cannot be determined, the requested mode
    is sent unchanged so data is never overwritten by accident.

    Args:
        api_client: Client from :func:`create_hotdata_client`.
        database_id: Hotdata database id.
        schema: Schema to publish into.
        target: Table, load mode and optional key columns.
        upload_id: Finalized upload to publish.
        snapshot_id: Snapshot being published, used in errors.
        timeout: Maximum seconds to wait for the load job.
        sleep: Sleep function, injectable for tests.
        clock: Monotonic clock, injectable for tests.

    Returns:
        The load outcome, including the table's total row count afterwards.

    Raises:
        HotdataWriteError: Hotdata rejected the load, the job failed, or it did not finish
            within ``timeout`` (``stage="load"``). Safe to replay with the same upload id.
    """
    mode = _effective_mode(api_client, database_id=database_id, schema=schema, target=target)
    options: dict[str, Any] = {"key": list(target.key)} if target.key else {}
    request = LoadManagedTableRequest(
        mode=mode, upload_id=upload_id, format=LOAD_FORMAT, var_async=True, **options
    )
    databases = DatabasesApi(api_client)
    response: Any = _call_with_retry(
        lambda: databases.load_database_table(database_id, schema, target.table, request),
        snapshot_id=snapshot_id,
        sleep=sleep,
    )

    if isinstance(response, SubmitJobResponse):
        response = _wait_for_job(
            JobsApi(api_client),
            response.id,
            snapshot_id=snapshot_id,
            timeout=timeout,
            sleep=sleep,
            clock=clock,
        )
    if not isinstance(response, LoadManagedTableResponse):
        raise HotdataWriteError(
            f"Unexpected load response from Hotdata: {type(response).__name__}",
            stage="load",
            snapshot_id=snapshot_id,
        )
    logger.info(
        "Loaded into Hotdata",
        extra={"snapshot_id": snapshot_id, "table": f"{schema}.{target.table}", "mode": mode},
    )
    return LoadResult(
        table=response.table_name,
        schema_name=response.schema_name,
        mode=mode,
        row_count=response.row_count,
    )


def _effective_mode(
    api_client: ApiClient, *, database_id: str, schema: str, target: TableTarget
) -> LoadMode:
    if target.mode == "replace":
        return "replace"
    exists = table_exists(api_client, database_id=database_id, schema=schema, table=target.table)
    if exists is False:
        logger.info(
            "Table %s.%s does not exist yet; first load uses mode=replace", schema, target.table
        )
        return "replace"
    return target.mode


def _get_database(
    api_client: ApiClient, database_id: str, *, sleep: Callable[[float], None]
) -> HotdataDatabase:
    databases = DatabasesApi(api_client)
    try:
        detail = _call_with_retry(
            lambda: databases.get_database(database_id), stage=LOOKUP_STAGE, sleep=sleep
        )
    except HotdataWriteError as exc:
        if exc.status_code == HTTPStatus.NOT_FOUND:
            raise ConfigurationError(
                f"Hotdata database {database_id!r} does not exist in this workspace; "
                "check HOTDATA_DATABASE_ID or run `bdh databases`"
            ) from exc
        raise
    return HotdataDatabase(id=detail.id, name=detail.name, default_schema=detail.default_schema)


def _ambiguous_database_message(candidates: list[HotdataDatabase], name: str | None) -> str:
    if not candidates and name is not None:
        return f"No Hotdata database is named {name!r}; run `bdh databases` to see them"
    if not candidates:
        return (
            "The Hotdata workspace has no databases; create one, then set HOTDATA_DATABASE_ID "
            "or HOTDATA_DATABASE_NAME"
        )
    listed = ", ".join(
        f"{database.id} ({database.name or 'unnamed'})"
        for database in candidates[:MAX_LISTED_DATABASES]
    )
    more = len(candidates) - MAX_LISTED_DATABASES
    suffix = f" and {more} more" if more > 0 else ""
    matching = f" named {name!r}" if name is not None else ""
    return (
        f"{len(candidates)} Hotdata databases{matching} found; set HOTDATA_DATABASE_ID "
        f"to one of: {listed}{suffix}"
    )


def _call_with_retry(
    call: Callable[[], T],
    *,
    sleep: Callable[[float], None],
    snapshot_id: str | None = None,
    stage: str = "load",
) -> T:
    backoff = iter(BACKOFF_SECONDS)
    while True:
        try:
            return call()
        except ApiException as exc:
            delay = next(backoff, None)
            if exc.status not in _RETRYABLE_STATUSES or delay is None:
                raise _write_error(stage, exc, snapshot_id) from exc
            wait = _retry_after(exc) or delay
            logger.warning("Hotdata %s returned %s; retrying in %ss", stage, exc.status, wait)
            sleep(wait)
        except urllib3.exceptions.HTTPError as exc:
            delay = next(backoff, None)
            if delay is None:
                raise HotdataWriteError(
                    f"Could not reach Hotdata: {exc}", stage=stage, snapshot_id=snapshot_id
                ) from exc
            logger.warning("Hotdata unreachable (%s); retrying in %ss", exc, delay)
            sleep(delay)


def _wait_for_job(
    jobs: JobsApi,
    job_id: str,
    *,
    snapshot_id: str,
    timeout: float,
    sleep: Callable[[float], None],
    clock: Callable[[], float],
) -> LoadManagedTableResponse:
    deadline = clock() + timeout
    intervals = iter(JOB_POLL_SECONDS)
    while True:
        job = _call_with_retry(lambda: jobs.get_job(job_id), snapshot_id=snapshot_id, sleep=sleep)
        if job.status == JobStatus.SUCCEEDED or (
            job.status == JobStatus.PARTIALLY_SUCCEEDED and job.result is not None
        ):
            if job.error_message:
                logger.warning("Load job %s finished with warnings: %s", job_id, job.error_message)
            result = job.result.actual_instance if job.result is not None else None
            if isinstance(result, LoadManagedTableResponse):
                return result
            raise HotdataWriteError(
                f"Load job {job_id} finished without a load result",
                stage="load",
                snapshot_id=snapshot_id,
            )
        if job.status in _FINISHED_WITHOUT_RESULT:
            raise HotdataWriteError(
                f"Load job {job_id} failed: {job.error_message or 'no details'}",
                stage="load",
                snapshot_id=snapshot_id,
            )
        interval = next(intervals, JOB_POLL_MAX_SECONDS)
        if clock() + interval > deadline:
            raise HotdataWriteError(
                f"Load job {job_id} still {job.status.value} after {timeout:.0f}s; "
                "replay later to pick up its result",
                stage="load",
                snapshot_id=snapshot_id,
            )
        sleep(interval)


def _retry_after(exc: ApiException) -> float | None:
    headers = exc.headers or {}
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return min(max(float(value), 0.0), MAX_RETRY_AFTER_SECONDS)
    except (TypeError, ValueError):
        return None


def _write_error(stage: str, exc: ApiException, snapshot_id: str | None) -> HotdataWriteError:
    error_code, message = _parse_error_body(exc.body)
    headers = exc.headers or {}
    trace_id = headers.get("X-Trace-Id")
    summary = message or exc.reason or "request failed"
    trace = f" [trace {trace_id}]" if trace_id else ""
    return HotdataWriteError(
        f"Hotdata {stage} failed (HTTP {exc.status}, {error_code or 'no code'}): {summary}{trace}",
        stage=stage,
        snapshot_id=snapshot_id,
        status_code=exc.status,
        error_code=error_code,
        trace_id=trace_id,
    )


def _parse_error_body(body: str | None) -> tuple[str | None, str | None]:
    if not body:
        return None, None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None, body[:500]
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return None, body[:500]
    return error.get("code"), error.get("message")
