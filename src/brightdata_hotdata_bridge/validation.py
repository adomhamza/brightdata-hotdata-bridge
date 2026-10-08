"""Validate a downloaded snapshot before anything is written to Hotdata.

The file is streamed line by line, so snapshots of any size use constant memory:

* Records Bright Data marked as failed (``error`` / ``error_code`` set, which happens
  with ``include_errors=true``) go to a separate *rejected* file and are not published.
* Every other record is type-checked against the scraper's catalog output fields.
  Scalars are normalised so a column keeps one type across rows (a number in a text
  field becomes its string form).
* Any mismatch stops the run before upload, and a JSON report lists what was found.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, TextIO

from brightdata_hotdata_bridge.errors import EmptySnapshotError, SchemaMismatchError
from brightdata_hotdata_bridge.models import (
    FieldSpec,
    ValidationIssue,
    ValidationOptions,
    ValidationReport,
)

logger = logging.getLogger(__name__)

MAX_REPORTED_ISSUES = 200
ERROR_MARKERS = ("error", "error_code")
METADATA_FIELDS = frozenset(
    {"input", "discovery_input", "timestamp", "warning", "warning_code", *ERROR_MARKERS}
)

_TEXT_TYPES = frozenset(
    {
        "text",
        "string",
        "url",
        "image",
        "country",
        "date",
        "warning",
        "warning_code",
        "error",
        "html2markdown",
        "html2text",
        "html2html",
        "html2ldjson",
    }
)
_NUMBER_TYPES = frozenset({"number", "price"})
_OBJECT_TYPES = frozenset({"object", "input"})


class _MismatchError(ValueError):
    """Raised internally when a value does not fit its declared type."""


def validate_snapshot_file(
    source: Path,
    *,
    clean_path: Path,
    rejected_path: Path,
    report_path: Path,
    fields: Iterable[FieldSpec],
    options: ValidationOptions,
    snapshot_id: str,
) -> ValidationReport:
    """Validate an NDJSON snapshot and write the publishable records to ``clean_path``.

    Args:
        source: Downloaded NDJSON snapshot.
        clean_path: Where valid records are written, one JSON object per line.
        rejected_path: Where records Bright Data marked as errors are written.
        report_path: Where the JSON validation report is written.
        fields: Expected output fields with their catalog types. Fields not listed are
            treated as unknown, except the metadata Bright Data adds to every record
            (``METADATA_FIELDS``), which is accepted without a type check.
        options: Required fields, unknown-field strictness and nested-value handling.
        snapshot_id: Snapshot being validated, used in errors.

    Returns:
        The validation report for a snapshot that can be published.

    Raises:
        SchemaMismatchError: Records are malformed, miss required fields, have values of
            the wrong type, or (when strict) contain unknown fields. Nothing is published,
            and the report at ``report_path`` lists the issues.
        EmptySnapshotError: No publishable records remain.
    """
    expected = {spec.name: spec.type for spec in fields}
    report = ValidationReport()
    unknown: set[str] = set()

    with (
        source.open(encoding="utf-8") as reader,
        clean_path.open("w", encoding="utf-8") as clean,
        rejected_path.open("w", encoding="utf-8") as rejected,
    ):
        _process_lines(
            reader,
            clean=clean,
            rejected=rejected,
            expected=expected,
            options=options,
            report=report,
            unknown=unknown,
        )

    report.unknown_fields = sorted(unknown)
    _write_report(report_path, report)

    if not report.is_valid:
        clean_path.unlink(missing_ok=True)
        raise SchemaMismatchError(
            f"{report.issue_count} schema issue(s) in {report.total_rows} record(s); "
            f"ingestion paused. See {report_path}",
            snapshot_id=snapshot_id,
            report_path=report_path,
        )
    if report.valid_rows == 0:
        clean_path.unlink(missing_ok=True)
        raise EmptySnapshotError(
            f"Snapshot has no publishable records ({report.error_rows} error record(s)); "
            "nothing was written",
            snapshot_id=snapshot_id,
        )
    if unknown:
        logger.warning(
            "Snapshot has fields not in the catalog: %s", ", ".join(report.unknown_fields)
        )
    return report


def _process_lines(
    reader: TextIO,
    *,
    clean: TextIO,
    rejected: TextIO,
    expected: dict[str, str],
    options: ValidationOptions,
    report: ValidationReport,
    unknown: set[str],
) -> None:
    for line_number, line in enumerate(reader, start=1):
        if not line.strip():
            continue
        report.total_rows += 1
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            _add_issue(report, line_number, None, f"not valid JSON ({exc.msg})")
            continue
        if not isinstance(record, dict):
            _add_issue(report, line_number, None, "record is not a JSON object")
            continue
        if _is_error_record(record):
            report.error_rows += 1
            rejected.write(line if line.endswith("\n") else f"{line}\n")
            continue

        normalised, issues = _check_record(record, expected, options, unknown)
        for field_name, problem in issues:
            _add_issue(report, line_number, field_name, problem)
        if issues:
            continue
        report.valid_rows += 1
        clean.write(json.dumps(normalised, ensure_ascii=False, separators=(",", ":")) + "\n")


def _check_record(
    record: dict[str, Any],
    expected: dict[str, str],
    options: ValidationOptions,
    unknown: set[str],
) -> tuple[dict[str, Any], list[tuple[str | None, str]]]:
    issues: list[tuple[str | None, str]] = [
        (name, "required field is missing or null")
        for name in options.required_fields
        if record.get(name) is None
    ]
    normalised: dict[str, Any] = {}
    for name, value in record.items():
        field_type = expected.get(name)
        if field_type is None and name not in METADATA_FIELDS:
            if options.strict_unknown_fields and name not in unknown:
                issues.append((name, "field is not in the scraper's output schema"))
            unknown.add(name)
        try:
            clean_value = _normalise(value, field_type)
        except _MismatchError as exc:
            issues.append((name, str(exc)))
            continue
        normalised[name] = _stringify(clean_value) if options.stringify_nested else clean_value
    return normalised, issues


def _normalise(value: Any, field_type: str | None) -> Any:
    if value is None or field_type is None:
        return value
    checker = _checker_for(field_type)
    return value if checker is None else checker(value)


def _checker_for(field_type: str) -> Callable[[Any], Any] | None:
    if field_type in _TEXT_TYPES:
        return _as_text
    if field_type in _NUMBER_TYPES:
        return _as_number
    if field_type == "boolean":
        return _as_boolean
    if field_type == "array":
        return _as_array
    if field_type in _OBJECT_TYPES:
        return _as_object
    return None


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int, float)):
        return json.dumps(value)
    raise _MismatchError(f"expected text, got {_type_name(value)}")


def _as_number(value: Any) -> int | float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    raise _MismatchError(f"expected number, got {_type_name(value)}")


def _as_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise _MismatchError(f"expected boolean, got {_type_name(value)}")


def _as_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    raise _MismatchError(f"expected array, got {_type_name(value)}")


def _as_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    raise _MismatchError(f"expected object, got {_type_name(value)}")


def _stringify(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _type_name(value: Any) -> str:
    names = {dict: "object", list: "array", str: "text", bool: "boolean"}
    return names.get(type(value), "number" if isinstance(value, (int, float)) else "unknown")


def _is_error_record(record: dict[str, Any]) -> bool:
    return any(record.get(marker) for marker in ERROR_MARKERS)


def _add_issue(report: ValidationReport, line: int, field: str | None, problem: str) -> None:
    report.issue_count += 1
    if len(report.issues) < MAX_REPORTED_ISSUES:
        report.issues.append(ValidationIssue(line=line, field=field, problem=problem))


def _write_report(path: Path, report: ValidationReport) -> None:
    path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
