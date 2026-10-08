"""Collect data with Bright Data scrapers and publish it into Hotdata tables.

Quick start::

    from brightdata_hotdata_bridge import push_snapshot_to_hotdata

    result = push_snapshot_to_hotdata(
        dataset_id="gd_l7q7dkf244hwjntr0",
        queries=["https://www.amazon.com/dp/B0CRMZHDG8"],
        table_name="amazon_products",
    )
    print(result.table_row_count)

Async code should use :func:`arun_pipeline` with a :class:`PushRequest` instead.
"""

from brightdata_hotdata_bridge.__about__ import __version__
from brightdata_hotdata_bridge.catalog import (
    get_scraper,
    load_catalog,
    resolve_method,
    search_scrapers,
)
from brightdata_hotdata_bridge.config import Settings, get_settings
from brightdata_hotdata_bridge.errors import (
    BridgeError,
    BrightDataApiError,
    CatalogError,
    CollectionFailedError,
    ConfigurationError,
    EmptySnapshotError,
    HotdataWriteError,
    InputValidationError,
    RateLimitedError,
    RunStateError,
    SchemaMismatchError,
    SnapshotTimeoutError,
    UnknownDatasetError,
    UnsupportedMethodError,
)
from brightdata_hotdata_bridge.hotdata_client import find_database
from brightdata_hotdata_bridge.models import (
    CollectionRequest,
    HotdataDatabase,
    PushRequest,
    PushResult,
    TableTarget,
    ValidationOptions,
    ValidationReport,
)
from brightdata_hotdata_bridge.pipeline import (
    RunStatus,
    aget_status,
    areplay_run,
    arun_pipeline,
    astart_collection,
    build_collection_request,
    push_snapshot_to_hotdata,
    replay_run,
)
from brightdata_hotdata_bridge.state import RunRecord, RunStage, list_runs, load_run

__all__ = [
    "BridgeError",
    "BrightDataApiError",
    "CatalogError",
    "CollectionFailedError",
    "CollectionRequest",
    "ConfigurationError",
    "EmptySnapshotError",
    "HotdataDatabase",
    "HotdataWriteError",
    "InputValidationError",
    "PushRequest",
    "PushResult",
    "RateLimitedError",
    "RunRecord",
    "RunStage",
    "RunStateError",
    "RunStatus",
    "SchemaMismatchError",
    "Settings",
    "SnapshotTimeoutError",
    "TableTarget",
    "UnknownDatasetError",
    "UnsupportedMethodError",
    "ValidationOptions",
    "ValidationReport",
    "__version__",
    "aget_status",
    "areplay_run",
    "arun_pipeline",
    "astart_collection",
    "build_collection_request",
    "find_database",
    "get_scraper",
    "get_settings",
    "list_runs",
    "load_catalog",
    "load_run",
    "push_snapshot_to_hotdata",
    "replay_run",
    "resolve_method",
    "search_scrapers",
]
