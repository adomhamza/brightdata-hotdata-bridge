"""Choosing the Hotdata database when HOTDATA_DATABASE_ID is not configured."""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any

import pytest
from hotdata.exceptions import ApiException

from brightdata_hotdata_bridge import hotdata_client
from brightdata_hotdata_bridge.config import Settings
from brightdata_hotdata_bridge.errors import ConfigurationError, HotdataWriteError
from brightdata_hotdata_bridge.hotdata_client import find_database, resolve_database
from brightdata_hotdata_bridge.models import HotdataDatabase

API_CLIENT: Any = object()


def summary(database_id: str, name: str | None = None) -> Any:
    return SimpleNamespace(id=database_id, name=name, default_schema="main")


def install(
    monkeypatch: pytest.MonkeyPatch,
    pages: list[list[Any]] | None = None,
    detail: Any = None,
) -> list[str | None]:
    """Fake ``DatabasesApi``; returns the cursors that list calls were made with."""
    cursors: list[str | None] = []
    remaining = list(pages or [[]])

    class Databases:
        def __init__(self, _client: Any) -> None:
            pass

        def list_databases(self, *, limit: int, cursor: str | None) -> Any:
            cursors.append(cursor)
            items = remaining.pop(0)
            has_more = bool(remaining)
            return SimpleNamespace(
                databases=items, has_more=has_more, next_cursor=f"c{len(cursors)}"
            )

        def get_database(self, _database_id: str) -> Any:
            if isinstance(detail, BaseException):
                raise detail
            return detail

    monkeypatch.setattr(hotdata_client, "DatabasesApi", Databases)
    return cursors


def test_configured_id_is_checked_and_described(monkeypatch: pytest.MonkeyPatch) -> None:
    install(monkeypatch, detail=summary("db_1", "scrapes"))

    database = resolve_database(API_CLIENT, database_id="db_1")

    assert database == HotdataDatabase(id="db_1", name="scrapes", default_schema="main")


def test_unknown_id_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    install(monkeypatch, detail=ApiException(status=404, reason="Not Found"))

    with pytest.raises(ConfigurationError, match="db_gone"):
        resolve_database(API_CLIENT, database_id="db_gone")


def test_lookup_failure_is_reported_as_hotdata_error(monkeypatch: pytest.MonkeyPatch) -> None:
    install(monkeypatch, detail=ApiException(status=401, reason="Unauthorized"))

    with pytest.raises(HotdataWriteError) as excinfo:
        resolve_database(API_CLIENT, database_id="db_1")

    assert (excinfo.value.stage, excinfo.value.status_code) == ("database lookup", 401)


def test_only_database_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    install(monkeypatch, pages=[[summary("db_only")]])

    assert resolve_database(API_CLIENT).id == "db_only"


def test_every_page_is_read_before_choosing(monkeypatch: pytest.MonkeyPatch) -> None:
    cursors = install(monkeypatch, pages=[[summary("db_new")], [summary("db_old")]])

    with pytest.raises(ConfigurationError, match="2 Hotdata databases found") as excinfo:
        resolve_database(API_CLIENT)

    assert cursors == [None, "c1"]
    assert "db_new" in str(excinfo.value)
    assert "db_old" in str(excinfo.value)


def test_name_selects_the_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    install(monkeypatch, pages=[[summary("db_1", "scrapes-dev"), summary("db_2", "scrapes")]])

    assert resolve_database(API_CLIENT, database_name="scrapes").id == "db_2"


@pytest.mark.parametrize(
    ("pages", "name", "message"),
    [
        ([[]], None, "has no databases"),
        ([[summary("db_1", "other")]], "scrapes", "No Hotdata database is named 'scrapes'"),
        ([[summary(f"db_{n}") for n in range(12)]], None, "and 2 more"),
    ],
)
def test_unresolvable_choices_explain_what_to_set(
    monkeypatch: pytest.MonkeyPatch, pages: list[list[Any]], name: str | None, message: str
) -> None:
    install(monkeypatch, pages=pages)

    with pytest.raises(ConfigurationError, match=message):
        resolve_database(API_CLIENT, database_name=name)


@pytest.mark.parametrize(
    ("overrides", "recorded_id", "expected"),
    [
        ({"hotdata_database_id": "db_env"}, "db_recorded", ("db_env", None)),
        ({"hotdata_database_name": "scrapes"}, "db_recorded", (None, "scrapes")),
        ({}, "db_recorded", ("db_recorded", None)),
        ({}, None, (None, None)),
    ],
)
def test_find_database_precedence(
    monkeypatch: pytest.MonkeyPatch,
    settings: Settings,
    overrides: dict[str, str],
    recorded_id: str | None,
    expected: tuple[str | None, str | None],
) -> None:
    calls: list[tuple[str | None, str | None]] = []

    def fake_resolve(
        _client: Any, *, database_id: str | None, database_name: str | None
    ) -> HotdataDatabase:
        calls.append((database_id, database_name))
        return HotdataDatabase(id=database_id or "db_any")

    monkeypatch.setattr(hotdata_client, "resolve_database", fake_resolve)
    monkeypatch.setattr(
        hotdata_client, "create_hotdata_client", lambda _s: contextlib.nullcontext(object())
    )
    configured = settings.model_copy(update={"hotdata_database_id": None, **overrides})

    find_database(configured, recorded_id=recorded_id)

    assert calls == [expected]
