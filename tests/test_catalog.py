from __future__ import annotations

import json
import os
import time
from pathlib import Path

import httpx
import pytest
import respx

from brightdata_hotdata_bridge.catalog import (
    load_catalog,
    parse_catalog,
    resolve_method,
    search_scrapers,
)
from brightdata_hotdata_bridge.config import DEFAULT_CATALOG_URL, Settings
from brightdata_hotdata_bridge.errors import (
    CatalogError,
    UnknownDatasetError,
    UnsupportedMethodError,
)
from tests.conftest import AMAZON_ID, CATALOG


@pytest.fixture
def online_settings(settings: Settings) -> Settings:
    return settings.model_copy(update={"catalog_file": None})


def test_parse_exposes_methods_and_discover_suffix() -> None:
    catalog = parse_catalog(json.dumps(CATALOG))

    scraper, method = resolve_method(catalog, "gd_l1vikfch901nx3by4", "discover_by_user_name")

    assert scraper.name == "Instagram - Profiles"
    assert method.discover_by == "user_name"
    assert method.required_inputs == ("user_name",)
    assert catalog[AMAZON_ID].methods["collect_by_url"].discover_by is None


def test_unknown_dataset_explains_marketplace_ids() -> None:
    with pytest.raises(UnknownDatasetError, match="Marketplace datasets"):
        resolve_method(parse_catalog(json.dumps(CATALOG)), "gd_notascraper", "collect_by_url")


def test_unsupported_method_lists_alternatives() -> None:
    with pytest.raises(UnsupportedMethodError, match="collect_by_url, discover_by_keyword"):
        resolve_method(parse_catalog(json.dumps(CATALOG)), AMAZON_ID, "discover_by_upc")


def test_search_matches_name_domain_and_id() -> None:
    catalog = parse_catalog(json.dumps(CATALOG))

    assert [s.dataset_id for s in search_scrapers(catalog, "INSTAGRAM.com")] == [
        "gd_l1vikfch901nx3by4"
    ]
    assert len(search_scrapers(catalog)) == 2


@pytest.mark.parametrize("payload", ["not json", '{"id": "x"}'])
def test_malformed_catalog_is_rejected(payload: str) -> None:
    with pytest.raises(CatalogError):
        parse_catalog(payload)


@respx.mock
async def test_download_is_cached_and_reused(online_settings: Settings) -> None:
    route = respx.get(DEFAULT_CATALOG_URL).mock(return_value=httpx.Response(200, json=CATALOG))

    first = await load_catalog(online_settings)
    second = await load_catalog(online_settings)

    assert route.call_count == 1
    assert set(first) == set(second) == {AMAZON_ID, "gd_l1vikfch901nx3by4"}
    assert (online_settings.state_dir / "catalog" / "scrapers-full.json").exists()


@respx.mock
async def test_stale_cache_is_used_when_download_fails(online_settings: Settings) -> None:
    cache = online_settings.state_dir / "catalog" / "scrapers-full.json"
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps(CATALOG))
    two_days_ago = time.time() - 48 * 3600
    os.utime(cache, (two_days_ago, two_days_ago))
    respx.get(DEFAULT_CATALOG_URL).mock(return_value=httpx.Response(503))

    catalog = await load_catalog(online_settings)

    assert AMAZON_ID in catalog


@respx.mock
async def test_download_failure_without_cache_raises(online_settings: Settings) -> None:
    respx.get(DEFAULT_CATALOG_URL).mock(side_effect=httpx.ConnectError("offline"))

    with pytest.raises(CatalogError, match="Could not download"):
        await load_catalog(online_settings)


async def test_pinned_catalog_file_is_used(settings: Settings, catalog_file: Path) -> None:
    catalog = await load_catalog(settings)

    assert settings.catalog_file == catalog_file
    assert AMAZON_ID in catalog
