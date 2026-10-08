"""Turn CSV files and plain values into validated Bright Data collection inputs.

Each CSV row becomes one input object. Columns are checked against the input schema of
the chosen collection method before anything is sent to Bright Data, so a malformed file
never starts a paid collection.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from brightdata_hotdata_bridge.errors import InputValidationError
from brightdata_hotdata_bridge.models import CollectionMethod, FieldSpec

MAX_REPORTED_ISSUES = 20

_TRUE = frozenset({"true", "1", "yes", "y"})
_FALSE = frozenset({"false", "0", "no", "n"})


def read_inputs_csv(path: Path, method: CollectionMethod) -> list[dict[str, Any]]:
    """Read a CSV of collection inputs and validate it against ``method``.

    The header row names the inputs (for example ``url`` or ``keyword``). Empty cells
    in optional columns are omitted; a byte-order mark from spreadsheet exports is
    tolerated.

    Args:
        path: CSV file with a header row.
        method: Collection method whose input schema the rows must satisfy.

    Returns:
        One input object per data row, with values converted to their declared types.

    Raises:
        InputValidationError: The file is unreadable, empty, has unknown or missing
            columns, or contains values of the wrong type.

    Example:
        ``inputs.csv``::

            url
            https://www.amazon.com/dp/B0CRMZHDG8
    """
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            header = [name.strip() for name in reader.fieldnames or []]
            rows = [{(key or "").strip(): value for key, value in row.items()} for row in reader]
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise InputValidationError(f"Cannot read input file {path}: {exc}", issues=[]) from exc

    if not header:
        raise InputValidationError(f"Input file {path} has no header row", issues=[])
    header_issues = _check_columns(header, method)
    if header_issues:
        raise InputValidationError(
            f"Input file {path} does not match {method.name}", issues=header_issues
        )
    return validate_inputs(rows, method, first_line=2)


def inputs_from_values(values: Iterable[str], method: CollectionMethod) -> list[dict[str, Any]]:
    """Build inputs from bare values, for methods with exactly one required input.

    This backs the ``--queries`` shortcut: ``["https://a", "https://b"]`` becomes
    ``[{"url": "https://a"}, {"url": "https://b"}]`` for ``collect_by_url``.

    Args:
        values: One value per input row. Blank values are skipped.
        method: Collection method to build inputs for.

    Returns:
        Validated input objects.

    Raises:
        InputValidationError: The method needs more than one required input, or a value
            has the wrong type.
    """
    required = method.required_inputs
    if len(required) != 1:
        needed = ", ".join(required) or "none"
        raise InputValidationError(
            f"{method.name} needs these inputs: {needed}. Use a CSV file instead.", issues=[]
        )
    field_name = required[0]
    rows = [{field_name: value.strip()} for value in values if value.strip()]
    return validate_inputs(rows, method)


def validate_inputs(
    rows: Sequence[Mapping[str, Any]], method: CollectionMethod, *, first_line: int = 1
) -> list[dict[str, Any]]:
    """Validate and normalise input rows against a method's input schema.

    Strings are converted to the declared type (numbers, booleans, JSON arrays and
    objects); native Python values must already have the right type.

    Args:
        rows: Input rows keyed by input name.
        method: Collection method whose input schema applies.
        first_line: Line number of the first row, used in error messages.

    Returns:
        Normalised copies of the rows.

    Raises:
        InputValidationError: No rows were given, or any row is invalid. All problems are
            reported at once (up to a limit) so they can be fixed in one pass.
    """
    if not rows:
        raise InputValidationError("No collection inputs were provided", issues=[])

    specs = {spec.name: spec for spec in method.input_schema}
    issues: list[str] = []
    normalised: list[dict[str, Any]] = []
    for offset, row in enumerate(rows):
        line = first_line + offset
        clean_row, row_issues = _validate_row(row, specs, line)
        issues.extend(row_issues)
        normalised.append(clean_row)

    if issues:
        shown = issues[:MAX_REPORTED_ISSUES]
        hidden = len(issues) - len(shown)
        suffix = f" (+{hidden} more)" if hidden else ""
        raise InputValidationError(
            f"{len(issues)} invalid input value(s) for {method.name}{suffix}", issues=shown
        )
    return normalised


def _check_columns(header: Sequence[str], method: CollectionMethod) -> list[str]:
    known = {spec.name for spec in method.input_schema}
    issues = [
        f"unknown column {name!r} (allowed: {', '.join(sorted(known))})"
        for name in header
        if name not in known
    ]
    issues.extend(
        f"missing required column {name!r}" for name in method.required_inputs if name not in header
    )
    return issues


def _validate_row(
    row: Mapping[str, Any], specs: Mapping[str, FieldSpec], line: int
) -> tuple[dict[str, Any], list[str]]:
    issues = [f"line {line}: unknown input {name!r}" for name in row if name not in specs]
    clean: dict[str, Any] = {}
    for name, spec in specs.items():
        value = row.get(name)
        if _is_blank(value):
            if spec.required:
                issues.append(f"line {line}: {name!r} is required")
            continue
        try:
            clean[name] = _coerce(value, spec.type)
        except (TypeError, ValueError) as exc:
            issues.append(f"line {line}: {name!r} {exc}")
    return clean, issues


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _coerce(value: Any, field_type: str) -> Any:
    converter = _CONVERTERS.get(field_type, _to_text)
    return converter(value)


def _to_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (bool, int, float)):
        return str(value)
    raise TypeError(f"must be text, got {type(value).__name__}")


def _to_url(value: Any) -> str:
    text = _to_text(value)
    parts = urlsplit(text)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError(f"must be an http(s) URL, got {text!r}")
    return text


def _to_number(value: Any) -> int | float:
    if isinstance(value, bool):
        raise TypeError("must be a number, got a boolean")
    if isinstance(value, (int, float)):
        return value
    text = _to_text(value)
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        raise ValueError(f"must be a number, got {text!r}") from None


def _to_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _to_text(value).lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"must be true or false, got {text!r}")


def _to_array(value: Any) -> list[Any]:
    parsed = _parse_json(value)
    if not isinstance(parsed, list):
        raise TypeError("must be a JSON array")
    return parsed


def _to_object(value: Any) -> dict[str, Any]:
    parsed = _parse_json(value)
    if not isinstance(parsed, dict):
        raise TypeError("must be a JSON object")
    return parsed


def _parse_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"is not valid JSON ({exc.msg})") from exc


_CONVERTERS = {
    "url": _to_url,
    "number": _to_number,
    "boolean": _to_boolean,
    "array": _to_array,
    "object": _to_object,
}
