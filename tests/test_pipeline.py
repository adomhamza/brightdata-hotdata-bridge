"""End-to-end pipeline tests mapped to the functional specification's use cases."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from brightdata_hotdata_bridge import pipeline
from brightdata_hotdata_bridge.config import Settings
from brightdata_hotdata_bridge.errors import (
    CollectionFailedError,
    ConfigurationError,
    HotdataWriteError,
    InputValidationError,
    RunStateError,
    SchemaMismatchError,
    SnapshotTimeoutError,
)
from brightdata_hotdata_bridge.models import (
    CollectionRequest,
    HotdataDatabase,
    LoadResult,
    PushRequest,
    TableTarget,
)
from brightdata_hotdata_bridge.pipeline import (
    areplay_run,
    arun_pipeline,
    build_collection_request,
    push_snapshot_to_hotdata,
)
from brightdata_hotdata_bridge.state import RunStage, load_run, run_paths
from tests.conftest import AMAZON_ID, BASE_URL, SNAPSHOT_ID, ndjson

DATABASE = HotdataDatabase(id="db_found", name="scrapes", default_schema="main")

GOOD_SNAPSHOT = ndjson(
    {"title": "Laptop", "url": "https://amazon.com/dp/1", "initial_price": 999.0},
    {"title": "Mouse", "url": "https://amazon.com/dp/2", "initial_price": 19},
    {"input": {"url": "https://amazon.com/dp/3"}, "error": "dead page", "error_code": "dead_page"},
)


@dataclass
class BrightDataMock:
    status: str = "ready"
    snapshot: bytes = GOOD_SNAPSHOT
    router: respx.MockRouter = field(default_factory=lambda: respx.mock(assert_all_called=False))

    def __post_init__(self) -> None:
        self.trigger = self.router.post(f"{BASE_URL}/datasets/v3/trigger").mock(
            return_value=httpx.Response(200, json={"snapshot_id": SNAPSHOT_ID})
        )
        self.progress = self.router.get(f"{BASE_URL}/datasets/v3/progress/{SNAPSHOT_ID}").mock(
            side_effect=lambda _request: httpx.Response(
                200, json={"status": self.status, "dataset_id": AMAZON_ID}
            )
        )
        self.download = self.router.get(f"{BASE_URL}/datasets/v3/snapshot/{SNAPSHOT_ID}").mock(
            side_effect=lambda _request: httpx.Response(200, content=self.snapshot)
        )


@dataclass
class HotdataRecorder:
    database: HotdataDatabase | Exception = DATABASE
    lookups: list[str | None] = field(default_factory=list)
    uploads: list[Path] = field(default_factory=list)
    loads: list[dict[str, Any]] = field(default_factory=list)
    load_errors: list[Exception] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> HotdataRecorder:
        monkeypatch.setattr(pipeline, "find_database", self.find)
        monkeypatch.setattr(pipeline, "upload_file", self.upload)
        monkeypatch.setattr(pipeline, "load_table", self.load)
        monkeypatch.setattr(
            pipeline, "create_hotdata_client", lambda _settings: contextlib.nullcontext(object())
        )
        return self

    def find(self, _settings: Settings, *, recorded_id: str | None = None) -> HotdataDatabase:
        self.lookups.append(recorded_id)
        if isinstance(self.database, Exception):
            raise self.database
        return self.database

    def upload(self, _client: Any, path: Path, *, snapshot_id: str) -> str:
        self.uploads.append(path)
        return f"upl_{len(self.uploads)}"

    def load(self, _client: Any, **kwargs: Any) -> LoadResult:
        if self.load_errors:
            raise self.load_errors.pop(0)
        self.loads.append(kwargs)
        target: TableTarget = kwargs["target"]
        return LoadResult(
            table=target.table, schema_name=kwargs["schema"], mode=target.mode, row_count=42
        )


@pytest.fixture
def bright_data() -> Any:
    mock = BrightDataMock()
    with mock.router:
        yield mock


@pytest.fixture
def hotdata(monkeypatch: pytest.MonkeyPatch) -> HotdataRecorder:
    return HotdataRecorder().install(monkeypatch)


def collect_request(table: str = "products") -> PushRequest:
    return PushRequest(
        collection=CollectionRequest(
            dataset_id=AMAZON_ID, inputs=[{"url": "https://amazon.com/dp/1"}]
        ),
        target=TableTarget(table=table),
    )


def snapshot_request(table: str = "products") -> PushRequest:
    return PushRequest(snapshot_id=SNAPSHOT_ID, target=TableTarget(table=table))


async def test_full_run_publishes_and_records_every_stage(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    """UC-1 to UC-5, FR-01 to FR-04."""
    result = await arun_pipeline(collect_request(), settings)

    assert result.snapshot_id == SNAPSHOT_ID
    assert (result.rows_published, result.rows_rejected, result.table_row_count) == (2, 1, 42)
    assert (result.database_id, result.schema_name) == ("db_found", "main")
    assert bright_data.trigger.call_count == 1
    assert hotdata.loads[0]["upload_id"] == "upl_1"
    assert hotdata.loads[0]["database_id"] == "db_found"
    assert hotdata.loads[0]["schema"] == "main"

    record = load_run(settings.state_dir, SNAPSHOT_ID)
    assert record is not None
    assert record.stage is RunStage.LOADED
    assert record.database_id == "db_found"
    assert record.dataset_id == AMAZON_ID
    assert record.last_error is None
    paths = run_paths(settings.state_dir, SNAPSHOT_ID)
    assert hotdata.uploads == [paths.clean]
    assert len(paths.clean.read_text().splitlines()) == 2
    assert len(paths.rejected.read_text().splitlines()) == 1


async def test_failed_collection_writes_nothing(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    """UC-6, FR-06: a failed collection writes nothing."""
    bright_data.status = "failed"

    with pytest.raises(CollectionFailedError):
        await arun_pipeline(collect_request(), settings)

    assert hotdata.uploads == []
    assert bright_data.download.call_count == 0
    record = load_run(settings.state_dir, SNAPSHOT_ID)
    assert record is not None
    assert record.last_error is not None
    assert record.last_error.stage == "wait"


async def test_timeout_is_resumed_by_snapshot_id(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    """Acceptance criterion "defined fallback": a polling timeout resumes by snapshot id."""
    impatient = settings.model_copy(update={"poll_timeout_seconds": 0.01})
    bright_data.status = "running"

    with pytest.raises(SnapshotTimeoutError):
        await arun_pipeline(collect_request(), impatient)

    bright_data.status = "ready"
    result = await arun_pipeline(snapshot_request(), settings)

    assert result.table_row_count == 42
    assert bright_data.trigger.call_count == 1


async def test_failed_write_is_replayed_without_reupload(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    """UC-7, FR-07: a failed Hotdata write is logged and can be replayed."""
    hotdata.load_errors.append(HotdataWriteError("busy", stage="load", snapshot_id=SNAPSHOT_ID))

    with pytest.raises(HotdataWriteError):
        await arun_pipeline(collect_request(), settings)

    failed = load_run(settings.state_dir, SNAPSHOT_ID)
    assert failed is not None
    assert failed.stage is RunStage.UPLOADED
    assert failed.last_error is not None
    assert failed.last_error.stage == "load"

    result = await areplay_run(SNAPSHOT_ID, settings=settings)

    assert result.upload_id == "upl_1"
    assert len(hotdata.uploads) == 1
    assert bright_data.download.call_count == 1


async def test_schema_mismatch_pauses_before_upload(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    """UC-4, UC-8, FR-05, FR-08: a schema mismatch pauses ingestion."""
    bright_data.snapshot = ndjson({"title": "ok"}, {"title": "x", "initial_price": "cheap"})

    with pytest.raises(SchemaMismatchError) as excinfo:
        await arun_pipeline(collect_request(), settings)

    assert hotdata.uploads == []
    assert excinfo.value.report_path.exists()
    record = load_run(settings.state_dir, SNAPSHOT_ID)
    assert record is not None
    assert record.stage is RunStage.DOWNLOADED


async def test_loaded_snapshot_is_not_loaded_twice(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    await arun_pipeline(collect_request(), settings)

    again = await areplay_run(SNAPSHOT_ID, settings=settings)

    assert again.already_loaded
    assert len(hotdata.loads) == 1


async def test_new_target_table_gets_a_fresh_upload(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    await arun_pipeline(collect_request("products"), settings)

    result = await arun_pipeline(snapshot_request("products_copy"), settings)

    assert result.table == "products_copy"
    assert [load["upload_id"] for load in hotdata.loads] == ["upl_1", "upl_2"]
    assert bright_data.download.call_count == 1


async def test_missing_hotdata_credentials_block_paid_trigger(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    incomplete = settings.model_copy(update={"hotdata_api_key": None})

    with pytest.raises(ConfigurationError, match="HOTDATA_API_KEY"):
        await arun_pipeline(collect_request(), incomplete)

    assert bright_data.trigger.call_count == 0
    assert hotdata.lookups == []


async def test_unresolvable_database_blocks_paid_trigger(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    hotdata.database = ConfigurationError("2 Hotdata databases found")

    with pytest.raises(ConfigurationError, match="databases found"):
        await arun_pipeline(collect_request(), settings)

    assert bright_data.trigger.call_count == 0


async def test_replay_reuses_the_recorded_database(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    hotdata.load_errors.append(HotdataWriteError("busy", stage="load", snapshot_id=SNAPSHOT_ID))
    with pytest.raises(HotdataWriteError):
        await arun_pipeline(collect_request(), settings)

    await areplay_run(SNAPSHOT_ID, settings=settings)

    assert hotdata.lookups == [None, "db_found"]


async def test_loading_into_another_database_is_a_new_load(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    await arun_pipeline(collect_request(), settings)
    hotdata.database = HotdataDatabase(id="db_other")

    result = await arun_pipeline(snapshot_request(), settings)

    assert not result.already_loaded
    assert (result.database_id, result.schema_name) == ("db_other", "public")
    assert [load["database_id"] for load in hotdata.loads] == ["db_found", "db_other"]


async def test_invalid_inputs_never_trigger(
    settings: Settings, bright_data: BrightDataMock
) -> None:
    with pytest.raises(InputValidationError):
        await build_collection_request(
            dataset_id=AMAZON_ID, queries=["not-a-url"], settings=settings
        )

    assert bright_data.trigger.call_count == 0


async def test_replay_requires_a_record(settings: Settings) -> None:
    with pytest.raises(RunStateError):
        await areplay_run("sd_unknown", settings=settings)


def test_blocking_wrapper_runs_the_pipeline(
    settings: Settings, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    result = push_snapshot_to_hotdata(
        dataset_id=AMAZON_ID,
        queries=["https://amazon.com/dp/1"],
        table_name="products",
        settings=settings,
    )

    assert result.rows_published == 2
    assert bright_data.trigger.calls.last.request.read() == (
        b'{"input":[{"url":"https://amazon.com/dp/1"}]}'
    )
