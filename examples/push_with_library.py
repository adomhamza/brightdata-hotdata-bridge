"""Collect with Bright Data and publish to Hotdata from Python, handling each failure.

Run from the project root with the virtual environment active and ``.env`` filled in::

    python examples/push_with_library.py -d gd_l7q7dkf244hwjntr0 -u https://amazon.com/dp/1 -t p
    python examples/push_with_library.py -d gd_l7q7dkf244hwjntr0 --csv urls.csv -t products
    python examples/push_with_library.py --snapshot sd_m1a2b3c4d5e6f7g8h -t products

This is the library equivalent of ``bdh push``. Each ``except`` branch shows how a
scheduler or service would react to that failure.
"""

import argparse
import sys
from pathlib import Path

from brightdata_hotdata_bridge import (
    BridgeError,
    CollectionFailedError,
    HotdataWriteError,
    PushResult,
    SchemaMismatchError,
    SnapshotTimeoutError,
    push_snapshot_to_hotdata,
    replay_run,
)

USAGE_ERROR_EXIT_CODE = 2


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Read the command-line arguments.

    Args:
        argv: Arguments to parse; ``sys.argv[1:]`` when omitted.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-t", "--table", required=True, help="Hotdata table to publish into")
    parser.add_argument("-d", "--dataset-id", help="Bright Data scraper id (gd_...)")
    parser.add_argument("-u", "--url", action="append", dest="urls", help="URL (repeatable)")
    parser.add_argument("--csv", type=Path, help="CSV whose header names the scraper inputs")
    parser.add_argument("--snapshot", help="Publish an existing snapshot instead of collecting")
    parser.add_argument("--mode", default="replace", choices=["replace", "append"])
    return parser.parse_args(argv)


def push(args: argparse.Namespace) -> PushResult:
    """Run the pipeline once, retrying a failed Hotdata write a single time.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Summary of what was published.

    Raises:
        HotdataWriteError: The write failed again on replay.
    """
    try:
        return push_snapshot_to_hotdata(
            table_name=args.table,
            dataset_id=args.dataset_id,
            queries=args.urls,
            input_file=args.csv,
            snapshot_id=args.snapshot,
            mode=args.mode,
        )
    except HotdataWriteError as exc:
        if exc.snapshot_id is None:
            raise
        print(f"Hotdata {exc.stage} failed (trace id {exc.trace_id}); replaying once")
        return replay_run(exc.snapshot_id)


def main(argv: list[str] | None = None) -> int:
    """Push one collection and report the outcome.

    Args:
        argv: Arguments to parse; ``sys.argv[1:]`` when omitted.

    Returns:
        Process exit code: 0 on success, otherwise the error's CLI exit code.
    """
    args = parse_args(argv)
    try:
        result = push(args)
    except CollectionFailedError as exc:
        print(f"Bright Data collection {exc.status}; nothing was written: {exc}")
        return exc.exit_code
    except SnapshotTimeoutError as exc:
        print(f"Not ready yet. Re-run later with --snapshot {exc.snapshot_id}")
        return exc.exit_code
    except SchemaMismatchError as exc:
        print(f"Ingestion paused; review {exc.report_path}, then replay the snapshot")
        return exc.exit_code
    except BridgeError as exc:
        print(f"Failed: {exc}")
        return exc.exit_code
    except ValueError as exc:
        print(f"Usage error: {exc}")
        return USAGE_ERROR_EXIT_CODE

    print(
        f"Loaded {result.row_count} rows into {result.schema_name}.{result.table} "
        f"({result.rows_rejected} rejected by Bright Data); "
        f"the table now has {result.table_row_count} rows"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
