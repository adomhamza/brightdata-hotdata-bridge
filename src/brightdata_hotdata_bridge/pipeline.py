"""End-to-end pipeline: Bright Data collection to Hotdata table.

Stages run in order, and each completed stage is saved to the run record:

``trigger`` > ``wait`` (poll until ready) > ``download`` > ``validate`` > ``upload`` > ``load``

Re-running a snapshot resumes from the first unfinished stage, which is how a missed
completion (timeout), a schema mismatch or a failed Hotdata write is recovered. A
failed collection, a timeout or a validation failure never reaches the upload stage.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel

from brightdata_hotdata_bridge.brightdata import (
    create_brightdata_client,
    download_snapshot,
    get_progress,
    trigger_collection,
    wait_for_snapshot,
)
from brightdata_hotdata_bridge.catalog import load_catalog, resolve_method
from brightdata_hotdata_bridge.config import Settings, get_settings, require_publish_settings
from brightdata_hotdata_bridge.errors import BridgeError, RunStateError
from brightdata_hotdata_bridge.hotdata_client import (
    create_hotdata_client,
    find_database,
    load_table,
    upload_file,
)
from brightdata_hotdata_bridge.inputs import inputs_from_values, read_inputs_csv, validate_inputs
from brightdata_hotdata_bridge.models import (
    DEFAULT_METHOD,
    CollectionRequest,
    FieldSpec,
    HotdataDatabase,
    LoadMode,
    PushRequest,
    PushResult,
    SnapshotProgress,
    TableTarget,
    ValidationOptions,
)
from brightdata_hotdata_bridge.state import (
    RunError,
    RunPaths,
    RunRecord,
    RunStage,
    load_run,
    run_paths,
    save_run,
    utc_now,
)
from brightdata_hotdata_bridge.validation import validate_snapshot_file

logger = logging.getLogger(__name__)

T = TypeVar("T")

UPLOAD_REUSE_WINDOW = timedelta(hours=23)
DEFAULT_SCHEMA = "public"


class RunStatus(BaseModel):
    """Combined view of a snapshot: Bright Data's status and the local run record."""

    snapshot_id: str
    progress: SnapshotProgress | None = None
    record: RunRecord | None = None


@dataclass(frozen=True)
class _Plan:
    wait: bool
    download: bool
    validate: bool
    upload: bool


async def arun_pipeline(request: PushRequest, settings: Settings | None = None) -> PushResult:
    """Run the pipeline: collect (or reuse a snapshot), validate, upload and load.

    Args:
        request: What to collect (or which snapshot to publish), the target table and
            validation options.
        settings: Configuration; read from the environment and ``.env`` when omitted.

    Returns:
        Summary of what was published.

    Raises:
        ConfigurationError: Credentials are missing, or the target database cannot be
            identified; raised before any collection is triggered.
        BridgeError: Any pipeline failure. Subclasses identify the stage, for example
            :class:`~brightdata_hotdata_bridge.errors.CollectionFailedError` or
            :class:`~brightdata_hotdata_bridge.errors.HotdataWriteError`. The run record
            keeps the error so the run can be resumed with :func:`areplay_run`.

    Example:
        >>> request = PushRequest(
        ...     collection=CollectionRequest(
        ...         dataset_id="gd_l7q7dkf244hwjntr0",
        ...         inputs=[{"url": "https://www.amazon.com/dp/B0CRMZHDG8"}],
        ...     ),
        ...     target=TableTarget(table="amazon_products"),
        ... )
        >>> result = await arun_pipeline(request)  # doctest: +SKIP
    """
    settings = settings or get_settings()
    require_publish_settings(settings)
    existing = (
        load_run(settings.state_dir, str(request.snapshot_id))
        if request.snapshot_id is not None
        else None
    )
    database = await asyncio.to_thread(
        find_database, settings, recorded_id=existing.database_id if existing else None
    )
    async with create_brightdata_client(settings) as bright_data:
        if request.collection is not None:
            record = await _start(
                bright_data,
                request.collection,
                request.target,
                request.validation,
                settings,
                database_id=database.id,
            )
        else:
            record = _reconcile(existing, request, database_id=database.id)
        if record.stage is RunStage.LOADED:
            logger.info("Snapshot %s is already loaded; nothing to do", record.snapshot_id)
            return _result(record, already_loaded=True)
        return await _drive(bright_data, record, settings, database)


async def astart_collection(
    collection: CollectionRequest,
    *,
    target: TableTarget | None = None,
    validation: ValidationOptions | None = None,
    settings: Settings | None = None,
) -> RunRecord:
    """Validate inputs and trigger a Bright Data collection without waiting for it.

    The returned record keeps the snapshot id; finish the run later with
    :func:`areplay_run` or ``bdh push --snapshot-id``.

    Args:
        collection: Dataset id, method and inputs.
        target: Table to publish into later, if already known.
        validation: Validation options to apply later.
        settings: Configuration; read from the environment when omitted.

    Returns:
        The saved run record at stage ``triggered``.

    Raises:
        BridgeError: The dataset or inputs are invalid, or Bright Data rejected the request.
    """
    settings = settings or get_settings()
    async with create_brightdata_client(settings) as bright_data:
        return await _start(
            bright_data, collection, target, validation or ValidationOptions(), settings
        )


async def areplay_run(
    snapshot_id: str,
    *,
    target: TableTarget | None = None,
    settings: Settings | None = None,
) -> PushResult:
    """Resume a stored run from its first unfinished stage.

    This is the recovery path for failed Hotdata writes, timeouts and fixed schema
    problems. A run that is already loaded into the same table is not loaded twice.

    Args:
        snapshot_id: Snapshot whose run should be resumed.
        target: Publish to a different table (or set one for a trigger-only run).
        settings: Configuration; read from the environment when omitted.

    Returns:
        Summary of what was published.

    Raises:
        RunStateError: No run record exists for the snapshot, or no target table is known.
        BridgeError: The resumed stage failed again.
    """
    settings = settings or get_settings()
    record = load_run(settings.state_dir, snapshot_id)
    if record is None:
        raise RunStateError(
            "No local run record; use `bdh push --snapshot-id` to publish this snapshot",
            snapshot_id=snapshot_id,
        )
    resolved_target = target or record.target
    if resolved_target is None:
        raise RunStateError("No target table recorded; pass --table", snapshot_id=snapshot_id)
    request = PushRequest(
        snapshot_id=snapshot_id, target=resolved_target, validation=record.validation
    )
    return await arun_pipeline(request, settings)


async def aget_status(snapshot_id: str, settings: Settings | None = None) -> RunStatus:
    """Report Bright Data's status for a snapshot together with the local run record.

    Args:
        snapshot_id: Snapshot to inspect.
        settings: Configuration; read from the environment when omitted.

    Returns:
        The remote progress and the local record (either may be ``None`` if unavailable).

    Raises:
        BrightDataApiError: Bright Data could not be queried.
    """
    settings = settings or get_settings()
    record = load_run(settings.state_dir, snapshot_id)
    async with create_brightdata_client(settings) as bright_data:
        progress = await get_progress(bright_data, snapshot_id)
    return RunStatus(snapshot_id=snapshot_id, progress=progress, record=record)


def push_snapshot_to_hotdata(
    *,
    table_name: str,
    dataset_id: str | None = None,
    queries: Sequence[str] | None = None,
    input_file: Path | str | None = None,
    inputs: Sequence[dict[str, Any]] | None = None,
    snapshot_id: str | None = None,
    method: str = DEFAULT_METHOD,
    limit_per_input: int | None = None,
    schema_name: str | None = None,
    mode: LoadMode = "replace",
    key: Sequence[str] | None = None,
    required_fields: Sequence[str] = (),
    strict_unknown_fields: bool = False,
    stringify_nested: bool = False,
    settings: Settings | None = None,
) -> PushResult:
    """Collect from Bright Data and publish to Hotdata in one blocking call.

    Give ``dataset_id`` with exactly one of ``queries``, ``input_file`` or ``inputs`` to
    start a new collection, or give ``snapshot_id`` to publish an existing snapshot.

    Args:
        table_name: Hotdata table to publish into.
        dataset_id: Bright Data scraper id.
        queries: Values for the method's single required input (URLs for ``collect_by_url``).
        input_file: CSV whose header names the method's inputs.
        inputs: Input objects, already keyed by input name.
        snapshot_id: Existing snapshot to publish instead of starting a collection.
        method: Collection method, for example ``discover_by_keyword``.
        limit_per_input: Cap on records per input for discovery methods.
        schema_name: Hotdata schema; defaults to ``HOTDATA_SCHEMA``, then the database's
            default schema, then ``public``.
        mode: ``replace``, ``append``, ``upsert``, ``update`` or ``delete``.
        key: Key columns for keyed modes.
        required_fields: Output fields that must be present and non-null in every record.
        strict_unknown_fields: Fail on fields missing from the scraper's output schema.
        stringify_nested: Store nested objects and arrays as JSON text.
        settings: Configuration; read from the environment when omitted.

    Returns:
        Summary of what was published.

    Raises:
        BridgeError: Any pipeline failure (see :func:`arun_pipeline`).
        ValueError: The arguments are inconsistent.
        RuntimeError: Called from a running event loop; use :func:`arun_pipeline` there.

    Example:
        >>> result = push_snapshot_to_hotdata(  # doctest: +SKIP
        ...     dataset_id="gd_l7q7dkf244hwjntr0",
        ...     queries=["https://www.amazon.com/dp/B0CRMZHDG8"],
        ...     table_name="amazon_products",
        ... )
    """
    settings = settings or get_settings()
    target = TableTarget(
        table=table_name, schema_name=schema_name, mode=mode, key=tuple(key) if key else None
    )
    validation = ValidationOptions(
        required_fields=tuple(required_fields),
        strict_unknown_fields=strict_unknown_fields,
        stringify_nested=stringify_nested,
    )

    async def _run() -> PushResult:
        collection = None
        if snapshot_id is None:
            collection = await build_collection_request(
                dataset_id=dataset_id,
                method=method,
                queries=queries,
                input_file=input_file,
                inputs=inputs,
                limit_per_input=limit_per_input,
                settings=settings,
            )
        request = PushRequest(
            collection=collection, snapshot_id=snapshot_id, target=target, validation=validation
        )
        return await arun_pipeline(request, settings)

    return run_sync(_run())


def replay_run(
    snapshot_id: str, *, target: TableTarget | None = None, settings: Settings | None = None
) -> PushResult:
    """Blocking version of :func:`areplay_run`.

    Args:
        snapshot_id: Snapshot whose run should be resumed.
        target: Optional replacement target table.
        settings: Configuration; read from the environment when omitted.

    Returns:
        Summary of what was published.

    Raises:
        BridgeError: The run cannot be resumed or a stage failed again.
        RuntimeError: Called from a running event loop; use :func:`areplay_run` there.
    """
    return run_sync(areplay_run(snapshot_id, target=target, settings=settings))


async def build_collection_request(
    *,
    dataset_id: str | None,
    method: str = DEFAULT_METHOD,
    queries: Sequence[str] | None = None,
    input_file: Path | str | None = None,
    inputs: Sequence[dict[str, Any]] | None = None,
    limit_per_input: int | None = None,
    settings: Settings | None = None,
) -> CollectionRequest:
    """Resolve a dataset id and turn queries, a CSV or input objects into a request.

    Args:
        dataset_id: Bright Data scraper id.
        method: Collection method name.
        queries: Values for the method's single required input.
        input_file: CSV whose header names the method's inputs.
        inputs: Input objects keyed by input name.
        limit_per_input: Cap on records per input for discovery methods.
        settings: Configuration; read from the environment when omitted.

    Returns:
        A validated collection request.

    Raises:
        ValueError: ``dataset_id`` is missing, or not exactly one input source was given.
        BridgeError: The dataset, method or inputs are invalid.
    """
    if dataset_id is None:
        raise ValueError("dataset_id is required to start a collection")
    sources = [source for source in (queries, input_file, inputs) if source is not None]
    if len(sources) != 1:
        raise ValueError("provide exactly one of queries, input_file or inputs")

    settings = settings or get_settings()
    catalog = await load_catalog(settings)
    _, collection_method = resolve_method(catalog, dataset_id, method)
    if queries is not None:
        rows = inputs_from_values(queries, collection_method)
    elif input_file is not None:
        rows = read_inputs_csv(Path(input_file), collection_method)
    else:
        rows = validate_inputs(list(inputs or []), collection_method)
    return CollectionRequest(
        dataset_id=dataset_id, method=method, inputs=rows, limit_per_input=limit_per_input
    )


def run_sync(coroutine: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine to completion from synchronous code.

    Args:
        coroutine: The coroutine to run.

    Returns:
        The coroutine's result.

    Raises:
        RuntimeError: An event loop is already running in this thread (for example in a
            notebook); await the ``a``-prefixed function instead.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)
    coroutine.close()
    raise RuntimeError(
        "An event loop is already running; await the async API (arun_pipeline, "
        "areplay_run) instead of the blocking wrapper"
    )


async def _start(
    bright_data: httpx.AsyncClient,
    collection: CollectionRequest,
    target: TableTarget | None,
    validation: ValidationOptions,
    settings: Settings,
    *,
    database_id: str | None = None,
) -> RunRecord:
    catalog = await load_catalog(settings)
    _, method = resolve_method(catalog, collection.dataset_id, collection.method)
    rows = validate_inputs(collection.inputs, method)
    snapshot_id = await trigger_collection(
        bright_data,
        dataset_id=collection.dataset_id,
        method=method,
        inputs=rows,
        limit_per_input=collection.limit_per_input,
    )
    record = RunRecord(
        snapshot_id=snapshot_id,
        stage=RunStage.TRIGGERED,
        target=target,
        validation=validation,
        dataset_id=collection.dataset_id,
        method=collection.method,
        input_count=len(rows),
        database_id=database_id,
    )
    return save_run(settings.state_dir, record)


def _reconcile(existing: RunRecord | None, request: PushRequest, *, database_id: str) -> RunRecord:
    snapshot_id = str(request.snapshot_id)
    if existing is None:
        return RunRecord(
            snapshot_id=snapshot_id,
            stage=RunStage.TRIGGERED,
            target=request.target,
            validation=request.validation,
            database_id=database_id,
        )

    stage = existing.stage
    upload_id = existing.upload_id
    if request.validation != existing.validation and stage.reached(RunStage.VALIDATED):
        stage, upload_id = RunStage.DOWNLOADED, None
    is_new_destination = request.target != existing.target or database_id != existing.database_id
    if stage is RunStage.LOADED and is_new_destination:
        stage, upload_id = RunStage.VALIDATED, None
    return existing.model_copy(
        update={
            "stage": stage,
            "upload_id": upload_id,
            "target": request.target,
            "validation": request.validation,
            "database_id": database_id,
        }
    )


def _plan(record: RunRecord, paths: RunPaths) -> _Plan:
    upload = not record.stage.reached(RunStage.UPLOADED) or _upload_expired(record)
    validate = upload and (not record.stage.reached(RunStage.VALIDATED) or not paths.clean.exists())
    download = validate and (
        not record.stage.reached(RunStage.DOWNLOADED) or not paths.raw.exists()
    )
    wait = download and not record.stage.reached(RunStage.READY)
    return _Plan(wait=wait, download=download, validate=validate, upload=upload)


def _upload_expired(record: RunRecord) -> bool:
    if record.upload_id is None or record.uploaded_at is None:
        return True
    return utc_now() - record.uploaded_at > UPLOAD_REUSE_WINDOW


async def _drive(
    bright_data: httpx.AsyncClient,
    record: RunRecord,
    settings: Settings,
    database: HotdataDatabase,
) -> PushResult:
    target = record.target
    if target is None:
        raise RunStateError("No target table set for this run", snapshot_id=record.snapshot_id)
    schema = (
        target.schema_name or settings.hotdata_schema or database.default_schema or DEFAULT_SCHEMA
    )
    paths = run_paths(settings.state_dir, record.snapshot_id)
    paths.directory.mkdir(parents=True, exist_ok=True)
    record = save_run(settings.state_dir, record)
    plan = _plan(record, paths)
    snapshot_id = record.snapshot_id
    current = "wait"

    try:
        if plan.wait:
            progress = await wait_for_snapshot(
                bright_data,
                snapshot_id,
                poll_interval=settings.poll_interval_seconds,
                timeout=settings.poll_timeout_seconds,
                on_progress=_log_progress,
            )
            record = _advance(
                settings,
                record,
                RunStage.READY,
                dataset_id=record.dataset_id or progress.dataset_id,
            )

        current = "download"
        if plan.download:
            await download_snapshot(
                bright_data,
                snapshot_id,
                paths.raw,
                poll_interval=settings.poll_interval_seconds,
                timeout=settings.poll_timeout_seconds,
            )
            record = _advance(settings, record, RunStage.DOWNLOADED)

        current = "validate"
        if plan.validate:
            fields = await _output_fields(record, settings)
            report = await asyncio.to_thread(
                validate_snapshot_file,
                paths.raw,
                clean_path=paths.clean,
                rejected_path=paths.rejected,
                report_path=paths.report,
                fields=fields,
                options=record.validation,
                snapshot_id=snapshot_id,
            )
            record = _advance(settings, record, RunStage.VALIDATED, report=report)

        with create_hotdata_client(settings) as hotdata:
            current = "upload"
            if plan.upload:
                upload_id = await asyncio.to_thread(
                    upload_file, hotdata, paths.clean, snapshot_id=snapshot_id
                )
                record = _advance(
                    settings, record, RunStage.UPLOADED, upload_id=upload_id, uploaded_at=utc_now()
                )

            current = "load"
            load = await asyncio.to_thread(
                load_table,
                hotdata,
                database_id=database.id,
                schema=schema,
                target=target,
                upload_id=str(record.upload_id),
                snapshot_id=snapshot_id,
                timeout=settings.load_timeout_seconds,
            )
            record = _advance(
                settings,
                record,
                RunStage.LOADED,
                table_row_count=load.row_count,
                loaded_mode=load.mode,
                loaded_schema=load.schema_name,
            )
    except BridgeError as exc:
        _record_failure(settings, record, current, exc)
        raise
    return _result(record, already_loaded=False)


def _advance(settings: Settings, record: RunRecord, stage: RunStage, **changes: Any) -> RunRecord:
    updated = record.model_copy(update={"stage": stage, "last_error": None, **changes})
    logger.info("Stage %s complete", stage.value, extra={"snapshot_id": record.snapshot_id})
    return save_run(settings.state_dir, updated)


def _record_failure(settings: Settings, record: RunRecord, stage: str, exc: BridgeError) -> None:
    failure = RunError(
        stage=stage, error_type=type(exc).__name__, message=str(exc), occurred_at=utc_now()
    )
    save_run(settings.state_dir, record.model_copy(update={"last_error": failure}))
    logger.error("Stage %s failed: %s", stage, exc, extra={"snapshot_id": record.snapshot_id})


async def _output_fields(record: RunRecord, settings: Settings) -> tuple[FieldSpec, ...]:
    if record.dataset_id is None:
        logger.warning("Dataset id unknown; only structural validation is applied")
        return ()
    catalog = await load_catalog(settings)
    scraper = catalog.get(record.dataset_id)
    if scraper is None:
        logger.warning(
            "%s is not in the scraper catalog; only structural validation is applied",
            record.dataset_id,
        )
        return ()
    method = scraper.methods.get(record.method or DEFAULT_METHOD)
    if method is None:
        method = next(iter(scraper.methods.values()), None)
    return method.output_fields if method is not None else ()


def _log_progress(progress: SnapshotProgress) -> None:
    logger.info("Snapshot status: %s", progress.status, extra={"snapshot_id": progress.snapshot_id})


def _result(record: RunRecord, *, already_loaded: bool) -> PushResult:
    if record.target is None or record.upload_id is None:
        raise RunStateError("Run record is incomplete", snapshot_id=record.snapshot_id)
    report = record.report
    return PushResult(
        snapshot_id=record.snapshot_id,
        dataset_id=record.dataset_id,
        upload_id=record.upload_id,
        database_id=record.database_id,
        table=record.target.table,
        schema_name=record.loaded_schema or record.target.schema_name or "",
        mode=record.loaded_mode or record.target.mode,
        rows_published=report.valid_rows if report else 0,
        rows_rejected=report.error_rows if report else 0,
        table_row_count=record.table_row_count or 0,
        already_loaded=already_loaded,
    )
