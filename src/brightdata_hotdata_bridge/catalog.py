"""Bright Data scraper catalog: which dataset ids can be triggered and with what inputs.

Bright Data publishes ``scrapers-full.json`` (refreshed daily) describing every pre-built
scraper: its collection methods, the input schema of each method and the output fields
it produces. The bridge uses it to validate CSV inputs before spending money on a
collection, and to validate downloaded records before publishing them.

The file is cached under ``<state_dir>/catalog/`` and refreshed after ``catalog_ttl_hours``.
A pinned copy can be used instead through ``BDH_CATALOG_FILE``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx
from pydantic import ValidationError

from brightdata_hotdata_bridge.config import Settings
from brightdata_hotdata_bridge.errors import (
    CatalogError,
    UnknownDatasetError,
    UnsupportedMethodError,
)
from brightdata_hotdata_bridge.models import CollectionMethod, FieldSpec, Scraper

logger = logging.getLogger(__name__)

Catalog = Mapping[str, Scraper]
CACHE_FILENAME = "scrapers-full.json"


async def load_catalog(
    settings: Settings,
    *,
    refresh: bool = False,
    http_client: httpx.AsyncClient | None = None,
) -> Catalog:
    """Load the scraper catalog, downloading it when the cache is missing or stale.

    Args:
        settings: Supplies the catalog URL, cache location, TTL and optional pinned file.
        refresh: Ignore a fresh cache and download again.
        http_client: Client to download with; a short-lived one is created if omitted.

    Returns:
        Mapping of dataset id to scraper description.

    Raises:
        CatalogError: The catalog could not be downloaded and no cached copy exists, or
            the file is not valid catalog JSON.

    Example:
        >>> catalog = await load_catalog(get_settings())  # doctest: +SKIP
        >>> catalog["gd_l7q7dkf244hwjntr0"].name  # doctest: +SKIP
        'Amazon products'
    """
    if settings.catalog_file is not None:
        return await asyncio.to_thread(_read_catalog_file, settings.catalog_file)

    cache_path = settings.state_dir / "catalog" / CACHE_FILENAME
    if not refresh and _is_fresh(cache_path, settings.catalog_ttl_hours):
        return await asyncio.to_thread(_read_catalog_file, cache_path)

    try:
        payload = await _download(settings, http_client)
        catalog = parse_catalog(payload)
    except (httpx.HTTPError, CatalogError) as exc:
        if not cache_path.exists():
            raise CatalogError(
                f"Could not download the scraper catalog from {settings.catalog_url}: {exc}"
            ) from exc
        logger.warning("Catalog download failed (%s); using the stale cached copy", exc)
        return await asyncio.to_thread(_read_catalog_file, cache_path)

    await asyncio.to_thread(_write_atomic, cache_path, payload)
    logger.info("Scraper catalog refreshed: %d scrapers", len(catalog))
    return catalog


def parse_catalog(raw: bytes | str) -> Catalog:
    """Parse ``scrapers-full.json`` content into scraper models.

    Args:
        raw: The JSON document (a list of scraper objects).

    Returns:
        Mapping of dataset id to scraper description.

    Raises:
        CatalogError: The content is not JSON or not shaped like the catalog.
    """
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CatalogError(f"Scraper catalog is not valid JSON: {exc}") from exc
    if not isinstance(entries, list):
        raise CatalogError("Scraper catalog must be a JSON array of scrapers")

    try:
        scrapers = [_to_scraper(entry) for entry in entries if isinstance(entry, dict)]
    except (ValidationError, KeyError, TypeError) as exc:
        raise CatalogError(f"Scraper catalog has an unexpected shape: {exc}") from exc
    return {scraper.dataset_id: scraper for scraper in scrapers}


def get_scraper(catalog: Catalog, dataset_id: str) -> Scraper:
    """Look up a triggerable scraper by dataset id.

    Args:
        catalog: Loaded scraper catalog.
        dataset_id: Bright Data dataset id, for example ``gd_l7q7dkf244hwjntr0``.

    Returns:
        The scraper description.

    Raises:
        UnknownDatasetError: The id is not a triggerable scraper. Dataset Marketplace ids
            (pre-collected data) are not in the catalog and cannot be collected from inputs.
    """
    scraper = catalog.get(dataset_id)
    if scraper is None:
        raise UnknownDatasetError(
            f"{dataset_id} is not a triggerable Bright Data scraper. Marketplace datasets "
            "(pre-collected data) cannot be collected from URLs or queries; run "
            "`bdh scrapers list --search <site>` to find a scraper id."
        )
    return scraper


def resolve_method(
    catalog: Catalog, dataset_id: str, method_name: str
) -> tuple[Scraper, CollectionMethod]:
    """Find a scraper and one of its collection methods.

    Args:
        catalog: Loaded scraper catalog.
        dataset_id: Bright Data dataset id, for example ``gd_l7q7dkf244hwjntr0``.
        method_name: Collection method, for example ``collect_by_url``.

    Returns:
        The scraper and the requested method.

    Raises:
        UnknownDatasetError: The id is not a triggerable scraper.
        UnsupportedMethodError: The scraper does not offer ``method_name``.
    """
    scraper = get_scraper(catalog, dataset_id)
    method = scraper.methods.get(method_name)
    if method is None:
        available = ", ".join(sorted(scraper.methods))
        raise UnsupportedMethodError(
            f"{scraper.name} ({dataset_id}) has no method {method_name!r}. Available: {available}"
        )
    return scraper, method


def search_scrapers(catalog: Catalog, query: str | None = None) -> list[Scraper]:
    """List scrapers whose id, name or domain contains ``query`` (case-insensitive).

    Args:
        catalog: Loaded scraper catalog.
        query: Text to look for. Omit to list everything.

    Returns:
        Matching scrapers sorted by name.
    """
    scrapers = sorted(catalog.values(), key=lambda scraper: scraper.name.lower())
    if not query:
        return scrapers
    needle = query.lower()
    return [
        scraper
        for scraper in scrapers
        if needle in scraper.name.lower()
        or needle in scraper.dataset_id.lower()
        or needle in (scraper.domain or "").lower()
    ]


def _to_scraper(entry: dict[str, Any]) -> Scraper:
    methods = {
        name: CollectionMethod(
            name=name,
            input_schema=tuple(_to_fields(spec.get("input_schema"))),
            output_fields=tuple(_to_fields(spec.get("output_fields"))),
        )
        for name, spec in (entry.get("scrapers") or {}).items()
        if isinstance(spec, dict)
    }
    return Scraper(
        dataset_id=entry["id"],
        name=str(entry.get("name") or entry["id"]).strip(),
        domain=entry.get("domain"),
        methods=methods,
    )


def _to_fields(raw: Any) -> list[FieldSpec]:
    if not isinstance(raw, list):
        return []
    return [
        FieldSpec(
            name=item["name"],
            type=str(item.get("type") or "text"),
            required=bool(item.get("required", False)),
            description=item.get("description"),
        )
        for item in raw
        if isinstance(item, dict) and item.get("name")
    ]


async def _download(settings: Settings, http_client: httpx.AsyncClient | None) -> bytes:
    if http_client is not None:
        return await _fetch(http_client, settings.catalog_url)
    async with httpx.AsyncClient(
        timeout=settings.http_timeout_seconds, follow_redirects=True
    ) as client:
        return await _fetch(client, settings.catalog_url)


async def _fetch(client: httpx.AsyncClient, url: str) -> bytes:
    response = await client.get(url)
    response.raise_for_status()
    return response.content


def _is_fresh(path: Path, ttl_hours: float) -> bool:
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < ttl_hours * 3600


def _read_catalog_file(path: Path) -> Catalog:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise CatalogError(f"Cannot read scraper catalog {path}: {exc}") from exc
    return parse_catalog(raw)


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_bytes(payload)
    temp_path.replace(path)
