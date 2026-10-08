"""``bdh`` command-line interface, a thin wrapper over the library.

Exit codes: 0 success, 1 unexpected error, 2 configuration or usage error, 3 invalid
dataset or inputs, 4 Bright Data API error, 5 collection failed, 6 snapshot timeout,
7 schema mismatch or empty snapshot, 8 Hotdata write failed, 9 run state error.
"""

import asyncio
import json
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, NoReturn, TypeVar

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from brightdata_hotdata_bridge.__about__ import __version__
from brightdata_hotdata_bridge.catalog import get_scraper, load_catalog, search_scrapers
from brightdata_hotdata_bridge.config import Settings, get_settings
from brightdata_hotdata_bridge.errors import BridgeError, InputValidationError, SchemaMismatchError
from brightdata_hotdata_bridge.hotdata_client import create_hotdata_client, list_databases
from brightdata_hotdata_bridge.logging_setup import configure_logging
from brightdata_hotdata_bridge.models import (
    DEFAULT_METHOD,
    HotdataDatabase,
    LoadMode,
    PushRequest,
    PushResult,
    TableTarget,
    ValidationOptions,
)
from brightdata_hotdata_bridge.pipeline import (
    aget_status,
    areplay_run,
    arun_pipeline,
    astart_collection,
    build_collection_request,
)
from brightdata_hotdata_bridge.state import RunRecord, list_runs

T = TypeVar("T")

app = typer.Typer(
    name="bdh",
    help="Collect data with Bright Data scrapers and publish it into Hotdata tables.",
    no_args_is_help=True,
    add_completion=False,
)
scrapers_app = typer.Typer(help="Browse the Bright Data scraper catalog.", no_args_is_help=True)
app.add_typer(scrapers_app, name="scrapers")

stdout = Console()
stderr = Console(stderr=True)

DatasetOption = Annotated[
    str | None, typer.Option("--dataset-id", "-d", help="Bright Data scraper id (gd_...).")
]
InputFileOption = Annotated[
    Path | None,
    typer.Option(
        "--input-file",
        "-i",
        help="CSV whose header names the scraper inputs.",
        exists=True,
        dir_okay=False,
        readable=True,
    ),
]
QueriesOption = Annotated[
    str | None,
    typer.Option("--queries", "-q", help="Comma-separated values for a single required input."),
]
MethodOption = Annotated[
    str, typer.Option("--method", "-m", help="Collection method, e.g. discover_by_keyword.")
]
LimitOption = Annotated[
    int | None, typer.Option("--limit-per-input", min=1, help="Max records per input.")
]
SchemaOption = Annotated[
    str | None, typer.Option("--schema", help="Hotdata schema (default: HOTDATA_SCHEMA).")
]
ModeOption = Annotated[
    str,
    typer.Option(
        "--mode",
        help="replace (default; discards existing rows), append, upsert, update or delete.",
    ),
]
KeyOption = Annotated[
    list[str] | None, typer.Option("--key", "-k", help="Key column for keyed modes (repeatable).")
]
JsonOption = Annotated[bool, typer.Option("--json", help="Print the result as JSON.")]


@dataclass(frozen=True)
class CliState:
    """Options shared by every command."""

    env_file: Path | None


def _version(value: bool) -> None:
    if value:
        stdout.print(f"bdh {__version__}")
        raise typer.Exit


@app.callback()
def main_callback(
    ctx: typer.Context,
    env_file: Annotated[
        Path | None,
        typer.Option(
            "--env-file",
            help="Read settings from this file instead of ./.env.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    log_level: Annotated[
        str, typer.Option("--log-level", help="DEBUG, INFO, WARNING, ERROR.")
    ] = "INFO",
    log_json: Annotated[bool, typer.Option("--log-json", help="Emit logs as JSON lines.")] = False,
    _version_flag: Annotated[
        bool, typer.Option("--version", callback=_version, is_eager=True, help="Show version.")
    ] = False,
) -> None:
    """Configure logging and settings for all commands."""
    configure_logging(level=log_level, json_output=log_json)
    ctx.obj = CliState(env_file=env_file)


@app.command()
def push(
    ctx: typer.Context,
    table: Annotated[str, typer.Option("--table", "-t", help="Hotdata table to publish into.")],
    dataset_id: DatasetOption = None,
    input_file: InputFileOption = None,
    queries: QueriesOption = None,
    snapshot_id: Annotated[
        str | None, typer.Option("--snapshot-id", "-s", help="Publish an existing snapshot.")
    ] = None,
    method: MethodOption = DEFAULT_METHOD,
    limit_per_input: LimitOption = None,
    schema: SchemaOption = None,
    mode: ModeOption = "replace",
    key: KeyOption = None,
    require_field: Annotated[
        list[str] | None,
        typer.Option("--require-field", help="Output field that must be non-null (repeatable)."),
    ] = None,
    strict_fields: Annotated[
        bool, typer.Option("--strict-fields", help="Fail on fields not in the scraper schema.")
    ] = False,
    stringify_nested: Annotated[
        bool, typer.Option("--stringify-nested", help="Store nested objects/arrays as JSON text.")
    ] = False,
    as_json: JsonOption = False,
) -> None:
    """Collect from Bright Data (or reuse --snapshot-id), validate, and publish to Hotdata.

    Examples:
        bdh push -d gd_l7q7dkf244hwjntr0 -i urls.csv -t amazon_products
        bdh push -d gd_l7q7dkf244hwjntr0 -q "https://www.amazon.com/dp/B0CRMZHDG8" -t products
        bdh push -s sd_m1a2b3c4d5e6f7g8h -t amazon_products
    """
    if snapshot_id is not None and any(
        value is not None for value in (dataset_id, input_file, queries)
    ):
        _usage_error(
            "--snapshot-id cannot be combined with --dataset-id, --input-file or --queries"
        )
    settings = _settings(ctx)
    target = _target(table, schema, mode, key)
    validation = ValidationOptions(
        required_fields=tuple(require_field or ()),
        strict_unknown_fields=strict_fields,
        stringify_nested=stringify_nested,
    )

    async def _run() -> PushResult:
        collection = None
        if snapshot_id is None:
            collection = await build_collection_request(
                dataset_id=dataset_id,
                method=method,
                queries=_split(queries),
                input_file=input_file,
                limit_per_input=limit_per_input,
                settings=settings,
            )
        request = PushRequest(
            collection=collection, snapshot_id=snapshot_id, target=target, validation=validation
        )
        return await arun_pipeline(request, settings)

    _print_result(_execute(_run()), as_json=as_json)


@app.command()
def trigger(
    ctx: typer.Context,
    dataset_id: DatasetOption = None,
    input_file: InputFileOption = None,
    queries: QueriesOption = None,
    method: MethodOption = DEFAULT_METHOD,
    limit_per_input: LimitOption = None,
    table: Annotated[
        str | None, typer.Option("--table", "-t", help="Remember a target table for replay.")
    ] = None,
    schema: SchemaOption = None,
    mode: ModeOption = "replace",
    key: KeyOption = None,
    as_json: JsonOption = False,
) -> None:
    """Start a collection and print its snapshot id without waiting for it.

    Finish later with `bdh replay SNAPSHOT_ID` (if --table was given) or
    `bdh push --snapshot-id SNAPSHOT_ID --table ...`.
    """
    settings = _settings(ctx)
    target = _target(table, schema, mode, key) if table else None

    async def _run() -> RunRecord:
        collection = await build_collection_request(
            dataset_id=dataset_id,
            method=method,
            queries=_split(queries),
            input_file=input_file,
            limit_per_input=limit_per_input,
            settings=settings,
        )
        return await astart_collection(collection, target=target, settings=settings)

    record = _execute(_run())
    if as_json:
        typer.echo(record.model_dump_json())
        return
    stdout.print(f"Snapshot started: [bold]{record.snapshot_id}[/bold]")


@app.command()
def status(
    ctx: typer.Context,
    snapshot_id: Annotated[str, typer.Argument(help="Snapshot to inspect.")],
    as_json: JsonOption = False,
) -> None:
    """Show Bright Data's status for a snapshot and the local run record."""
    settings = _settings(ctx)
    result = _execute(aget_status(snapshot_id, settings))
    if as_json:
        typer.echo(result.model_dump_json())
        return
    table = Table(title=f"Snapshot {snapshot_id}", show_header=False)
    if result.progress is not None:
        table.add_row("Bright Data status", result.progress.status)
        table.add_row("Records", str(result.progress.records or "-"))
    if result.record is not None:
        table.add_row("Local stage", result.record.stage.value)
        target = result.record.target
        table.add_row("Target", target.table if target else "-")
        if result.record.last_error is not None:
            error = result.record.last_error
            table.add_row("Last error", f"[red]{error.stage}: {error.message}[/red]")
    stdout.print(table)


@app.command()
def replay(
    ctx: typer.Context,
    snapshot_id: Annotated[str, typer.Argument(help="Snapshot whose run should be resumed.")],
    table: Annotated[
        str | None, typer.Option("--table", "-t", help="Publish to a different table.")
    ] = None,
    schema: SchemaOption = None,
    mode: ModeOption = "replace",
    key: KeyOption = None,
    as_json: JsonOption = False,
) -> None:
    """Resume a run from its first unfinished stage, e.g. after a failed Hotdata write."""
    settings = _settings(ctx)
    target = _target(table, schema, mode, key) if table else None
    _print_result(
        _execute(areplay_run(snapshot_id, target=target, settings=settings)), as_json=as_json
    )


@app.command()
def runs(ctx: typer.Context, as_json: JsonOption = False) -> None:
    """List local runs, most recent first."""
    settings = _settings(ctx)
    records = list_runs(settings.state_dir)
    if as_json:
        typer.echo(json.dumps([record.model_dump(mode="json") for record in records]))
        return
    table = Table("Snapshot", "Dataset", "Stage", "Target", "Last error", "Updated")
    for record in records:
        table.add_row(
            record.snapshot_id,
            record.dataset_id or "-",
            record.stage.value,
            record.target.table if record.target else "-",
            record.last_error.stage if record.last_error else "",
            record.updated_at.strftime("%Y-%m-%d %H:%M"),
        )
    stdout.print(table)


@app.command()
def databases(ctx: typer.Context, as_json: JsonOption = False) -> None:
    """List the Hotdata databases in the workspace and show which one `push` would use."""
    settings = _settings(ctx)
    found = _execute_sync(lambda: _list_databases(settings))
    if as_json:
        typer.echo(json.dumps([database.model_dump() for database in found]))
        return
    table = Table("Database id", "Name", "Default schema")
    for database in found:
        table.add_row(database.id, database.name or "-", database.default_schema or "-")
    stdout.print(table)
    if settings.hotdata_database_id or settings.hotdata_database_name:
        stdout.print("Chosen by HOTDATA_DATABASE_ID or HOTDATA_DATABASE_NAME.")
    elif len(found) == 1:
        stdout.print("This is the only database, so it is used automatically.")
    else:
        stdout.print("Set HOTDATA_DATABASE_ID or HOTDATA_DATABASE_NAME to choose one.")


@scrapers_app.command("list")
def scrapers_list(
    ctx: typer.Context,
    search: Annotated[str | None, typer.Option("--search", "-s", help="Filter by text.")] = None,
    refresh: Annotated[bool, typer.Option("--refresh", help="Re-download the catalog.")] = False,
    limit: Annotated[int, typer.Option("--limit", min=1, help="Max rows to show.")] = 50,
) -> None:
    """List triggerable scrapers, optionally filtered by name, domain or id."""
    settings = _settings(ctx)
    catalog = _execute(load_catalog(settings, refresh=refresh))
    matches = search_scrapers(catalog, search)
    table = Table()
    table.add_column("Dataset id", no_wrap=True)
    for header in ("Name", "Domain", "Methods"):
        table.add_column(header)
    for scraper in matches[:limit]:
        table.add_row(
            scraper.dataset_id, scraper.name, scraper.domain or "", ", ".join(scraper.methods)
        )
    stdout.print(table)
    stdout.print(f"{len(matches)} match(es); showing {min(len(matches), limit)}")


@scrapers_app.command("show")
def scrapers_show(
    ctx: typer.Context,
    dataset_id: Annotated[str, typer.Argument(help="Scraper id (gd_...).")],
) -> None:
    """Show a scraper's collection methods and the CSV columns each one needs."""
    settings = _settings(ctx)
    catalog = _execute(load_catalog(settings))
    scraper = _execute_sync(lambda: get_scraper(catalog, dataset_id))
    stdout.print(f"[bold]{scraper.name}[/bold] ({scraper.dataset_id}) {scraper.domain or ''}")
    for method in scraper.methods.values():
        table = Table("Column", "Type", "Required", title=method.name, title_justify="left")
        for spec in method.input_schema:
            table.add_row(spec.name, spec.type, "yes" if spec.required else "")
        stdout.print(table)


def main() -> None:
    """Entry point for the ``bdh`` console script."""
    app()


def _settings(ctx: typer.Context) -> Settings:
    state: CliState = ctx.obj
    return _execute_sync(lambda: get_settings(env_file=state.env_file))


def _list_databases(settings: Settings) -> list[HotdataDatabase]:
    with create_hotdata_client(settings) as api_client:
        return list_databases(api_client)


def _target(table: str, schema: str | None, mode: str, key: list[str] | None) -> TableTarget:
    return _execute_sync(
        lambda: TableTarget(
            table=table,
            schema_name=schema,
            mode=_load_mode(mode),
            key=tuple(key) if key else None,
        )
    )


def _load_mode(value: str) -> LoadMode:
    modes: dict[str, LoadMode] = {
        "replace": "replace",
        "append": "append",
        "upsert": "upsert",
        "update": "update",
        "delete": "delete",
    }
    mode = modes.get(value.lower())
    if mode is None:
        _usage_error(f"--mode must be one of {', '.join(modes)}")
    return mode


def _split(queries: str | None) -> list[str] | None:
    if queries is None:
        return None
    return [value.strip() for value in queries.split(",") if value.strip()]


def _execute(coroutine: Coroutine[Any, Any, T]) -> T:
    return _execute_sync(lambda: asyncio.run(coroutine))


def _execute_sync(func: Callable[[], T]) -> T:
    try:
        return func()
    except BridgeError as exc:
        _report_error(exc)
        raise typer.Exit(code=exc.exit_code) from exc
    except (ValueError, ValidationError) as exc:
        _usage_error(str(exc))


def _report_error(exc: BridgeError) -> None:
    stderr.print(f"[bold red]Error:[/bold red] {exc}", highlight=False)
    if isinstance(exc, InputValidationError):
        for issue in exc.issues:
            stderr.print(f"  - {issue}", highlight=False)
    if isinstance(exc, SchemaMismatchError):
        stderr.print(f"  Report: {exc.report_path}", highlight=False)
    if exc.snapshot_id is not None:
        snapshot_id = exc.snapshot_id
        stderr.print(
            f"  Inspect with `bdh status {snapshot_id}`; resume with `bdh replay {snapshot_id}`.",
            highlight=False,
        )


def _usage_error(message: str) -> NoReturn:
    stderr.print(f"[bold red]Usage error:[/bold red] {message}", highlight=False)
    raise typer.Exit(code=2)


def _print_result(result: PushResult, *, as_json: bool) -> None:
    if as_json:
        typer.echo(result.model_dump_json())
        return
    if result.already_loaded:
        stdout.print(f"Snapshot {result.snapshot_id} was already loaded; nothing to do.")
    table = Table(title="Published to Hotdata", show_header=False)
    table.add_row("Snapshot", result.snapshot_id)
    table.add_row("Database", result.database_id or "-")
    table.add_row("Table", f"{result.schema_name}.{result.table}")
    table.add_row("Mode", result.mode)
    table.add_row("Rows published", str(result.rows_published))
    table.add_row("Rows rejected by Bright Data", str(result.rows_rejected))
    table.add_row("Table row count", str(result.table_row_count))
    table.add_row("Upload id", result.upload_id)
    stdout.print(table)


if __name__ == "__main__":
    main()
