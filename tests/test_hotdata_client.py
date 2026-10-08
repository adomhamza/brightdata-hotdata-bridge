from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from hotdata import LoadManagedTableResponse, SubmitJobResponse
from hotdata.exceptions import ApiException
from hotdata.models.job_status import JobStatus
from hotdata.uploads import SessionCreateError, StorageError

from brightdata_hotdata_bridge import hotdata_client
from brightdata_hotdata_bridge.errors import HotdataWriteError
from brightdata_hotdata_bridge.hotdata_client import load_table, upload_file
from brightdata_hotdata_bridge.models import TableTarget
from tests.conftest import SNAPSHOT_ID, FakeClock

API_CLIENT: Any = object()


def load_response(row_count: int = 5) -> LoadManagedTableResponse:
    return LoadManagedTableResponse(
        arrow_schema_json="{}",
        connection_id="conn_1",
        row_count=row_count,
        schema_name="public",
        table_name="products",
    )


def api_error(status: int, code: str = "BAD_REQUEST", trace: str = "tr_1") -> ApiException:
    exc = ApiException(
        status=status, reason="err", body=f'{{"error": {{"code": "{code}", "message": "nope"}}}}'
    )
    exc.headers = {"X-Trace-Id": trace}
    return exc


@dataclass
class FakeHotdata:
    load_results: list[Any] = field(default_factory=list)
    job_results: list[Any] = field(default_factory=list)
    existing_tables: list[tuple[str, str]] | None = field(default_factory=list)
    upload_results: list[Any] = field(default_factory=list)
    load_requests: list[Any] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> FakeHotdata:
        fake = self

        class Databases:
            def __init__(self, _client: Any) -> None:
                pass

            def get_database(self, _database_id: str) -> Any:
                if fake.existing_tables is None:
                    raise api_error(500)
                return SimpleNamespace(default_connection_id="conn_1")

            def load_database_table(
                self, _database_id: str, _schema: str, _table: str, request: Any
            ) -> Any:
                fake.load_requests.append(request)
                return _next(fake.load_results)

        class Jobs:
            def __init__(self, _client: Any) -> None:
                pass

            def get_job(self, _job_id: str) -> Any:
                return _next(fake.job_results)

        class Information:
            def __init__(self, _client: Any) -> None:
                pass

            def information_schema(self, **_kwargs: Any) -> Any:
                tables = [
                    SimpleNamespace(var_schema=schema, table=table)
                    for schema, table in fake.existing_tables or []
                ]
                return SimpleNamespace(tables=tables, has_more=False, next_cursor=None)

        class Uploads:
            def __init__(self, _client: Any) -> None:
                pass

            def upload_file(self, _path: Any, **_kwargs: Any) -> Any:
                return _next(fake.upload_results)

        monkeypatch.setattr(hotdata_client, "DatabasesApi", Databases)
        monkeypatch.setattr(hotdata_client, "JobsApi", Jobs)
        monkeypatch.setattr(hotdata_client, "InformationSchemaApi", Information)
        monkeypatch.setattr(hotdata_client, "UploadsApi", Uploads)
        return self


def _next(results: list[Any]) -> Any:
    result = results.pop(0)
    if isinstance(result, BaseException):
        raise result
    return result


def job(status: JobStatus, result: Any = None, error: str | None = None) -> Any:
    wrapped = SimpleNamespace(actual_instance=result) if result is not None else None
    return SimpleNamespace(status=status, result=wrapped, error_message=error)


def run_load(
    fake: FakeHotdata, clock: FakeClock, target: TableTarget | None = None, timeout: float = 60
) -> Any:
    return load_table(
        API_CLIENT,
        database_id="db_1",
        schema="public",
        target=target or TableTarget(table="products"),
        upload_id="upl_1",
        snapshot_id=SNAPSHOT_ID,
        timeout=timeout,
        sleep=clock.sleep_sync,
        clock=clock,
    )


def test_load_request_is_async_ndjson_from_upload(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    fake = FakeHotdata(load_results=[load_response(7)]).install(monkeypatch)

    result = run_load(fake, clock)

    request = fake.load_requests[0]
    assert request.to_dict() == {
        "async": True,
        "format": "json",
        "mode": "replace",
        "upload_id": "upl_1",
    }
    assert result.row_count == 7
    assert result.mode == "replace"


def test_background_job_is_polled_until_done(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    submitted = SubmitJobResponse(id="job_1", status=JobStatus.PENDING, status_url="/v1/jobs/job_1")
    fake = FakeHotdata(
        load_results=[submitted],
        job_results=[job(JobStatus.RUNNING), job(JobStatus.SUCCEEDED, load_response(9))],
    ).install(monkeypatch)

    result = run_load(fake, clock)

    assert result.row_count == 9
    assert clock.sleeps == [1]


def test_failed_job_raises(monkeypatch: pytest.MonkeyPatch, clock: FakeClock) -> None:
    submitted = SubmitJobResponse(id="job_1", status=JobStatus.PENDING, status_url="/v1/jobs/job_1")
    fake = FakeHotdata(
        load_results=[submitted], job_results=[job(JobStatus.FAILED, error="type narrowing")]
    ).install(monkeypatch)

    with pytest.raises(HotdataWriteError, match="type narrowing"):
        run_load(fake, clock)


def test_busy_table_is_retried(monkeypatch: pytest.MonkeyPatch, clock: FakeClock) -> None:
    fake = FakeHotdata(load_results=[api_error(409), load_response()]).install(monkeypatch)

    run_load(fake, clock)

    assert len(fake.load_requests) == 2
    assert clock.sleeps == [2]


def test_bad_request_carries_code_and_trace(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    fake = FakeHotdata(load_results=[api_error(400, trace="tr_42")]).install(monkeypatch)

    with pytest.raises(HotdataWriteError) as excinfo:
        run_load(fake, clock)

    error = excinfo.value
    assert (error.stage, error.status_code, error.error_code, error.trace_id) == (
        "load",
        400,
        "BAD_REQUEST",
        "tr_42",
    )
    assert "trace tr_42" in str(error)


def test_append_to_missing_table_becomes_replace(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    fake = FakeHotdata(load_results=[load_response()], existing_tables=[]).install(monkeypatch)

    result = run_load(fake, clock, TableTarget(table="products", mode="append"))

    assert fake.load_requests[0].mode == "replace"
    assert result.mode == "replace"


def test_upsert_to_existing_table_keeps_mode_and_key(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    fake = FakeHotdata(
        load_results=[load_response()], existing_tables=[("public", "products")]
    ).install(monkeypatch)

    run_load(fake, clock, TableTarget(table="products", mode="upsert", key=("asin",)))

    assert fake.load_requests[0].mode == "upsert"
    assert fake.load_requests[0].key == ["asin"]


def test_unknown_existence_never_downgrades_to_replace(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    fake = FakeHotdata(load_results=[load_response()], existing_tables=None).install(monkeypatch)

    run_load(fake, clock, TableTarget(table="products", mode="append"))

    assert fake.load_requests[0].mode == "append"


def test_upload_retries_session_overload(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, tmp_path: Path
) -> None:
    FakeHotdata(
        upload_results=[
            SessionCreateError(api_error(503)),
            SimpleNamespace(upload_id="upl_9"),
        ]
    ).install(monkeypatch)

    upload_id = upload_file(
        API_CLIENT, tmp_path / "clean.ndjson", snapshot_id=SNAPSHOT_ID, sleep=clock.sleep_sync
    )

    assert upload_id == "upl_9"
    assert clock.sleeps == [2]


def test_storage_failure_is_a_write_error(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, tmp_path: Path
) -> None:
    FakeHotdata(upload_results=[StorageError(status=500, part_number=1, body="oops")]).install(
        monkeypatch
    )

    with pytest.raises(HotdataWriteError) as excinfo:
        upload_file(API_CLIENT, tmp_path / "f", snapshot_id=SNAPSHOT_ID, sleep=clock.sleep_sync)

    assert excinfo.value.stage == "upload"
