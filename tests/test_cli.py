"""CLI tests: the same use cases through the `bdh` entry point, plus exit codes."""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from brightdata_hotdata_bridge import cli
from brightdata_hotdata_bridge.__about__ import __version__
from brightdata_hotdata_bridge.cli import app
from brightdata_hotdata_bridge.errors import ConfigurationError
from brightdata_hotdata_bridge.models import HotdataDatabase
from tests.conftest import AMAZON_ID, SNAPSHOT_ID
from tests.test_pipeline import BrightDataMock, HotdataRecorder

runner = CliRunner()


@pytest.fixture(autouse=True)
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, catalog_file: Path) -> None:
    values = {
        "BRIGHTDATA_API_KEY": "bd-key",
        "HOTDATA_API_KEY": "hd-key",
        "HOTDATA_WORKSPACE_ID": "ws_test",
        "BDH_STATE_DIR": str(tmp_path / "state"),
        "BDH_CATALOG_FILE": str(catalog_file),
        "BDH_POLL_INTERVAL_SECONDS": "0.001",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def bright_data() -> Any:
    mock = BrightDataMock()
    with mock.router:
        yield mock


@pytest.fixture
def hotdata(monkeypatch: pytest.MonkeyPatch) -> HotdataRecorder:
    return HotdataRecorder().install(monkeypatch)


def write_csv(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "urls.csv"
    path.write_text(content, encoding="utf-8")
    return path


def test_push_from_csv_prints_json_result(
    tmp_path: Path, bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    csv_path = write_csv(tmp_path, "url\nhttps://amazon.com/dp/1\n")

    result = runner.invoke(
        app, ["push", "-d", AMAZON_ID, "-i", str(csv_path), "-t", "products", "--json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["snapshot_id"] == SNAPSHOT_ID
    assert payload["rows_published"] == 2
    assert payload["table"] == "products"
    assert payload["database_id"] == "db_found"


def test_ambiguous_database_exits_2_before_triggering(
    bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    hotdata.database = ConfigurationError("2 Hotdata databases found; set HOTDATA_DATABASE_ID")

    result = runner.invoke(app, ["push", "-d", AMAZON_ID, "-q", "https://a.com/x", "-t", "p"])

    assert result.exit_code == 2
    assert "HOTDATA_DATABASE_ID" in result.output
    assert bright_data.trigger.call_count == 0


@pytest.mark.parametrize(
    ("found", "hint"),
    [
        ([HotdataDatabase(id="db_1", name="only", default_schema="main")], "used automatically"),
        ([HotdataDatabase(id="db_1"), HotdataDatabase(id="db_2")], "to choose one"),
    ],
)
def test_databases_lists_workspace_databases(
    monkeypatch: pytest.MonkeyPatch, found: list[HotdataDatabase], hint: str
) -> None:
    monkeypatch.setattr(
        cli, "create_hotdata_client", lambda _settings: contextlib.nullcontext(object())
    )
    monkeypatch.setattr(cli, "list_databases", lambda _client: found)

    table = runner.invoke(app, ["databases"])
    as_json = runner.invoke(app, ["databases", "--json"])

    assert table.exit_code == 0, table.output
    assert "db_1" in table.stdout
    assert hint in table.stdout
    assert [database["id"] for database in json.loads(as_json.stdout)] == [
        database.id for database in found
    ]


def test_push_existing_snapshot_then_list_runs(
    bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    pushed = runner.invoke(app, ["push", "--snapshot-id", SNAPSHOT_ID, "--table", "products"])
    listed = runner.invoke(app, ["runs", "--json"])

    assert pushed.exit_code == 0, pushed.output
    assert "Published to Hotdata" in pushed.stdout
    assert bright_data.trigger.call_count == 0
    assert json.loads(listed.stdout)[0]["stage"] == "loaded"


def test_trigger_then_replay(bright_data: BrightDataMock, hotdata: HotdataRecorder) -> None:
    triggered = runner.invoke(
        app, ["trigger", "-d", AMAZON_ID, "-q", "https://amazon.com/dp/1", "-t", "products"]
    )
    replayed = runner.invoke(app, ["replay", SNAPSHOT_ID, "--json"])

    assert triggered.exit_code == 0, triggered.output
    assert SNAPSHOT_ID in triggered.stdout
    assert replayed.exit_code == 0, replayed.output
    assert json.loads(replayed.stdout)["table_row_count"] == 42


def test_failed_collection_exits_5_with_recovery_hint(
    bright_data: BrightDataMock, hotdata: HotdataRecorder
) -> None:
    bright_data.status = "failed"

    result = runner.invoke(app, ["push", "-d", AMAZON_ID, "-q", "https://a.com/x", "-t", "p"])

    assert result.exit_code == 5
    assert f"bdh status {SNAPSHOT_ID}" in result.output
    assert hotdata.uploads == []


def test_bad_csv_exits_3_and_lists_issues(tmp_path: Path, bright_data: BrightDataMock) -> None:
    csv_path = write_csv(tmp_path, "url\nnot-a-url\n")

    result = runner.invoke(app, ["push", "-d", AMAZON_ID, "-i", str(csv_path), "-t", "p"])

    assert result.exit_code == 3
    assert "line 2: 'url' must be an http(s) URL" in result.output
    assert bright_data.trigger.call_count == 0


def test_marketplace_dataset_exits_3() -> None:
    result = runner.invoke(app, ["push", "-d", "gd_l1vil1d81g0u8763b2", "-q", "x", "-t", "p"])

    assert result.exit_code == 3
    assert "not a triggerable Bright Data scraper" in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["push", "-s", SNAPSHOT_ID, "-d", AMAZON_ID, "-t", "p"],
        ["push", "-s", SNAPSHOT_ID, "-t", "bad-name"],
        ["push", "-s", SNAPSHOT_ID, "-t", "p", "--mode", "merge"],
        ["push", "-t", "p"],
    ],
)
def test_usage_errors_exit_2(args: list[str]) -> None:
    result = runner.invoke(app, args)

    assert result.exit_code == 2, result.output


def test_replay_without_record_exits_9() -> None:
    result = runner.invoke(app, ["replay", "sd_missing"])

    assert result.exit_code == 9


def test_missing_credentials_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HOTDATA_API_KEY")

    result = runner.invoke(app, ["push", "-d", AMAZON_ID, "-q", "https://a.com/x", "-t", "p"])

    assert result.exit_code == 2
    assert "HOTDATA_API_KEY" in result.output


def test_scrapers_show_lists_required_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRIGHTDATA_API_KEY")

    result = runner.invoke(app, ["scrapers", "show", AMAZON_ID])

    assert result.exit_code == 0, result.output
    assert "discover_by_keyword" in result.stdout
    assert "keyword" in result.stdout


def test_version() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.stdout.strip() == f"bdh {__version__}"
