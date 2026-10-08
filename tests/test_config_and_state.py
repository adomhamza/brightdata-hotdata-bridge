from __future__ import annotations

from pathlib import Path

import pytest

from brightdata_hotdata_bridge.config import get_settings, require_publish_settings
from brightdata_hotdata_bridge.errors import ConfigurationError, RunStateError
from brightdata_hotdata_bridge.models import PushRequest, TableTarget
from brightdata_hotdata_bridge.state import (
    RunRecord,
    RunStage,
    list_runs,
    load_run,
    run_paths,
    save_run,
)
from tests.conftest import SNAPSHOT_ID


def test_env_file_is_read(tmp_path: Path) -> None:
    env_file = tmp_path / "custom.env"
    env_file.write_text("BRIGHTDATA_API_KEY=from-file\nHOTDATA_SCHEMA=analytics\n")

    settings = get_settings(env_file=env_file)

    assert settings.brightdata_api_key is not None
    assert settings.brightdata_api_key.get_secret_value() == "from-file"
    assert settings.hotdata_schema == "analytics"
    assert "from-file" not in repr(settings)


def test_blank_values_count_as_unset(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("HOTDATA_DATABASE_ID=\nHOTDATA_SCHEMA=\n")

    settings = get_settings(env_file=env_file)

    assert settings.hotdata_database_id is None
    assert settings.hotdata_schema is None


def test_invalid_values_name_the_variable(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="BDH_POLL_INTERVAL_SECONDS"):
        get_settings(env_file=tmp_path / "none.env", poll_interval_seconds=-1)


def test_publish_settings_list_everything_missing(tmp_path: Path) -> None:
    settings = get_settings(env_file=tmp_path / "none.env", brightdata_api_key="k")

    with pytest.raises(ConfigurationError) as excinfo:
        require_publish_settings(settings)

    message = str(excinfo.value)
    for name in ("HOTDATA_API_KEY", "HOTDATA_WORKSPACE_ID"):
        assert name in message
    assert "BRIGHTDATA_API_KEY" not in message
    assert "HOTDATA_DATABASE_ID" not in message


def test_records_round_trip_and_sort_by_recency(tmp_path: Path) -> None:
    first = save_run(tmp_path, RunRecord(snapshot_id="sd_a", stage=RunStage.TRIGGERED))
    second = save_run(
        tmp_path,
        RunRecord(snapshot_id="sd_b", stage=RunStage.LOADED, target=TableTarget(table="t")),
    )

    assert load_run(tmp_path, "sd_a") == first
    assert [record.snapshot_id for record in list_runs(tmp_path)] == [
        second.snapshot_id,
        first.snapshot_id,
    ]
    assert load_run(tmp_path, "sd_missing") is None


def test_corrupt_record_is_reported(tmp_path: Path) -> None:
    paths = run_paths(tmp_path, SNAPSHOT_ID)
    paths.directory.mkdir(parents=True)
    paths.record.write_text("{broken")

    with pytest.raises(RunStateError):
        load_run(tmp_path, SNAPSHOT_ID)
    assert list_runs(tmp_path) == []


def test_stage_ordering() -> None:
    assert RunStage.UPLOADED.reached(RunStage.VALIDATED)
    assert not RunStage.READY.reached(RunStage.DOWNLOADED)


@pytest.mark.parametrize("snapshot_id", ["../x", "a/b", "", "sd x"])
def test_unsafe_snapshot_ids_are_rejected(snapshot_id: str) -> None:
    with pytest.raises(ValueError, match="snapshot"):
        PushRequest(snapshot_id=snapshot_id, target=TableTarget(table="t"))


def test_push_request_needs_exactly_one_source() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        PushRequest(target=TableTarget(table="t"))
