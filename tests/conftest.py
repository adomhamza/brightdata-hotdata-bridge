from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from brightdata_hotdata_bridge.config import Settings, get_settings
from brightdata_hotdata_bridge.logging_setup import PACKAGE_LOGGER

BASE_URL = "https://api.brightdata.com"
AMAZON_ID = "gd_l7q7dkf244hwjntr0"
SNAPSHOT_ID = "sd_test123"

CATALOG: list[dict[str, Any]] = [
    {
        "id": AMAZON_ID,
        "name": "Amazon products",
        "domain": "amazon.com",
        "scrapers": {
            "collect_by_url": {
                "input_schema": [
                    {"name": "url", "required": True, "type": "url"},
                    {"name": "zipcode", "required": False, "type": "text"},
                    {"name": "all_variations", "required": False, "type": "boolean"},
                ],
                "output_fields": [
                    {"name": "title", "type": "text"},
                    {"name": "url", "type": "url"},
                    {"name": "initial_price", "type": "price"},
                    {"name": "reviews_count", "type": "number"},
                    {"name": "categories", "type": "array"},
                    {"name": "is_available", "type": "boolean"},
                    {"name": "input", "type": "input"},
                    {"name": "error", "type": "error"},
                    {"name": "error_code", "type": "text"},
                ],
            },
            "discover_by_keyword": {
                "input_schema": [
                    {"name": "keyword", "required": True, "type": "text"},
                    {"name": "pages", "required": False, "type": "number"},
                ],
                "output_fields": [{"name": "title", "type": "text"}],
            },
        },
    },
    {
        "id": "gd_l1vikfch901nx3by4",
        "name": "Instagram - Profiles",
        "domain": "instagram.com",
        "scrapers": {
            "collect_by_url": {
                "input_schema": [{"name": "url", "required": True, "type": "url"}],
                "output_fields": [{"name": "account", "type": "text"}],
            },
            "discover_by_user_name": {
                "input_schema": [{"name": "user_name", "required": True, "type": "text"}],
                "output_fields": [{"name": "account", "type": "text"}],
            },
        },
    },
]


@pytest.fixture
def catalog_file(tmp_path: Path) -> Path:
    path = tmp_path / "scrapers-full.json"
    path.write_text(json.dumps(CATALOG), encoding="utf-8")
    return path


@pytest.fixture
def settings(tmp_path: Path, catalog_file: Path) -> Settings:
    return get_settings(
        env_file=tmp_path / "missing.env",
        brightdata_api_key="bd-key",
        hotdata_api_key="hd-key",
        hotdata_workspace_id="ws_test",
        hotdata_database_id="db_test",
        state_dir=tmp_path / "state",
        catalog_file=catalog_file,
        poll_interval_seconds=0.001,
        poll_timeout_seconds=5,
    )


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    for name in (
        "BRIGHTDATA_API_KEY",
        "HOTDATA_API_KEY",
        "HOTDATA_WORKSPACE_ID",
        "HOTDATA_DATABASE_ID",
        "HOTDATA_SCHEMA",
        "BDH_STATE_DIR",
        "BDH_CATALOG_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    yield
    package_logger = logging.getLogger(PACKAGE_LOGGER)
    package_logger.handlers.clear()
    package_logger.propagate = True
    package_logger.setLevel(logging.NOTSET)


class FakeClock:
    """Deterministic clock and sleep for polling tests."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def sleep_sync(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def ndjson(*records: dict[str, Any]) -> bytes:
    return "".join(json.dumps(record) + "\n" for record in records).encode()
