"""Run SQL against the Hotdata database the bridge publishes into.

Run from the project root with the virtual environment active::

    python examples/query_hotdata.py "SELECT name, city FROM public.linkedin"
    python examples/query_hotdata.py --format json "SELECT * FROM public.linkedin LIMIT 5"
    python examples/query_hotdata.py --file report.sql --format csv > report.csv

Credentials and the target database come from the same ``.env`` as the ``bdh`` CLI.
Queries are read-only. Nested columns are addressed with ``struct_col.field`` and arrays
are expanded with ``unnest(array_col)``.
"""

import csv
import json
import sys
from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import typer
from hotdata import QueryRequest
from hotdata.exceptions import ApiException
from hotdata.query import QueryApi, ResultError
from rich.console import Console
from rich.table import Table

from brightdata_hotdata_bridge import BridgeError, find_database, get_settings
from brightdata_hotdata_bridge.hotdata_client import create_hotdata_client

DEFAULT_MAX_ROWS = 100_000

stdout = Console()
stderr = Console(stderr=True)


class OutputFormat(str, Enum):
    """How query results are printed."""

    TABLE = "table"
    JSON = "json"
    CSV = "csv"


def run_query(
    sql: str, *, env_file: Path | None = None, max_rows: int = DEFAULT_MAX_ROWS
) -> tuple[list[str], list[list[Any]]]:
    """Run a query against the bridge's Hotdata database and fetch every row.

    Args:
        sql: The SQL statement.
        env_file: Alternative ``.env`` path; defaults to ``./.env``.
        max_rows: Refuse to download results with more rows than this.

    Returns:
        The column names and the rows, in column order.

    Raises:
        BridgeError: Configuration is missing or the database cannot be identified.
        ApiException: Hotdata rejected the query, for example a SQL error.
        ResultError: The full result could not be retrieved or exceeds ``max_rows``.
    """
    settings = get_settings(env_file=env_file)
    database = find_database(settings)
    with create_hotdata_client(settings) as api_client:
        response = QueryApi(api_client).query(
            QueryRequest(sql=sql, database_id=database.id),
            auto_follow=True,
            max_auto_rows=max_rows,
        )
    if response.warning:
        stderr.print(f"[yellow]Warning:[/yellow] {response.warning}")
    return list(response.columns), [list(row) for row in response.rows]


def main(
    sql: Annotated[str | None, typer.Argument(help="SQL to run.")] = None,
    file: Annotated[
        Path | None,
        typer.Option("--file", "-f", exists=True, dir_okay=False, help="Read SQL from a file."),
    ] = None,
    output: Annotated[
        OutputFormat, typer.Option("--format", help="table, json or csv.")
    ] = OutputFormat.TABLE,
    max_rows: Annotated[
        int, typer.Option("--max-rows", min=1, help="Refuse larger results.")
    ] = DEFAULT_MAX_ROWS,
    env_file: Annotated[
        Path | None, typer.Option("--env-file", help="Read settings from this file.")
    ] = None,
) -> None:
    """Run a read-only SQL query against Hotdata and print the result.

    Args:
        sql: SQL given on the command line.
        file: File containing the SQL, instead of ``sql``.
        output: Output format.
        max_rows: Refuse to download results with more rows than this.
        env_file: Alternative ``.env`` path.

    Raises:
        typer.BadParameter: Neither or both of ``sql`` and ``--file`` were given.
        typer.Exit: The query failed (exit code 1).
    """
    if (sql is None) == (file is None):
        raise typer.BadParameter("give the SQL as an argument or with --file, not both")
    statement = sql if sql is not None else Path(str(file)).read_text(encoding="utf-8")
    try:
        columns, rows = run_query(statement, env_file=env_file, max_rows=max_rows)
    except (BridgeError, ApiException, ResultError) as exc:
        stderr.print(f"[red]Query failed:[/red] {_describe(exc)}")
        raise typer.Exit(1) from exc
    _render(columns, rows, output)


def _render(columns: list[str], rows: list[list[Any]], output: OutputFormat) -> None:
    if output is OutputFormat.JSON:
        records = [dict(zip(columns, row, strict=True)) for row in rows]
        typer.echo(json.dumps(records, indent=2, default=str, ensure_ascii=False))
        return
    if output is OutputFormat.CSV:
        writer = csv.writer(sys.stdout)
        writer.writerow(columns)
        writer.writerows([_cell(value) for value in row] for row in rows)
        return
    table = Table(*columns)
    for row in rows:
        table.add_row(*(_cell(value) for value in row))
    stdout.print(table)
    stderr.print(f"{len(rows)} row(s)")


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, ensure_ascii=False)
    return str(value)


def _describe(exc: Exception) -> str:
    if not isinstance(exc, ApiException) or not exc.body:
        return str(exc)
    try:
        error = json.loads(exc.body).get("error", {})
    except (json.JSONDecodeError, AttributeError):
        return f"HTTP {exc.status}: {exc.body}"
    return f"HTTP {exc.status}: {error.get('message') or exc.body}"


if __name__ == "__main__":
    typer.run(main)
