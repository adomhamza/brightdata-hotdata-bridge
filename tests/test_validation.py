from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from brightdata_hotdata_bridge.catalog import parse_catalog
from brightdata_hotdata_bridge.errors import EmptySnapshotError, SchemaMismatchError
from brightdata_hotdata_bridge.models import FieldSpec, ValidationOptions, ValidationReport
from brightdata_hotdata_bridge.validation import validate_snapshot_file
from tests.conftest import AMAZON_ID, CATALOG, SNAPSHOT_ID, ndjson

FIELDS = parse_catalog(json.dumps(CATALOG))[AMAZON_ID].methods["collect_by_url"].output_fields


def run(
    tmp_path: Path,
    *records: dict[str, Any],
    options: ValidationOptions | None = None,
    fields: tuple[FieldSpec, ...] = FIELDS,
    raw: bytes | None = None,
) -> ValidationReport:
    source = tmp_path / "raw.ndjson"
    source.write_bytes(raw if raw is not None else ndjson(*records))
    return validate_snapshot_file(
        source,
        clean_path=tmp_path / "clean.ndjson",
        rejected_path=tmp_path / "rejected.ndjson",
        report_path=tmp_path / "report.json",
        fields=fields,
        options=options or ValidationOptions(),
        snapshot_id=SNAPSHOT_ID,
    )


def read_lines(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_valid_records_are_normalised_and_error_records_set_aside(tmp_path: Path) -> None:
    report = run(
        tmp_path,
        {"title": 123, "initial_price": 9.5, "categories": ["a"], "is_available": True},
        {"title": "ok", "initial_price": None, "reviews_count": 4, "brand_new_field": "x"},
        {"input": {"url": "https://a.com"}, "error": "Page not found", "error_code": "dead_page"},
    )

    assert report.total_rows == 3
    assert report.valid_rows == 2
    assert report.error_rows == 1
    assert report.unknown_fields == ["brand_new_field"]
    clean = read_lines(tmp_path / "clean.ndjson")
    assert clean[0]["title"] == "123"
    assert clean[1]["initial_price"] is None
    assert read_lines(tmp_path / "rejected.ndjson")[0]["error_code"] == "dead_page"


def test_type_mismatch_pauses_ingestion_with_report(tmp_path: Path) -> None:
    with pytest.raises(SchemaMismatchError) as excinfo:
        run(
            tmp_path,
            {"title": "fine"},
            {"title": {"nested": True}, "reviews_count": "many"},
        )

    report = json.loads(excinfo.value.report_path.read_text())
    assert report["issue_count"] == 2
    assert {issue["field"] for issue in report["issues"]} == {"title", "reviews_count"}
    assert all(issue["line"] == 2 for issue in report["issues"])
    assert not (tmp_path / "clean.ndjson").exists()


def test_required_fields_are_enforced(tmp_path: Path) -> None:
    with pytest.raises(SchemaMismatchError):
        run(
            tmp_path,
            {"title": "a", "url": "https://a.com"},
            {"title": "b", "url": None},
            options=ValidationOptions(required_fields=("url",)),
        )


def test_strict_mode_rejects_unknown_fields_once_per_field(tmp_path: Path) -> None:
    with pytest.raises(SchemaMismatchError) as excinfo:
        run(
            tmp_path,
            {"title": "a", "surprise": 1},
            {"title": "b", "surprise": 2},
            options=ValidationOptions(strict_unknown_fields=True),
        )

    report = json.loads(excinfo.value.report_path.read_text())
    assert report["issue_count"] == 1


def test_bright_data_metadata_is_not_schema_drift(tmp_path: Path) -> None:
    report = run(
        tmp_path,
        {
            "title": "a",
            "input": {"url": "https://www.linkedin.com/in/x"},
            "timestamp": "2026-10-08T13:14:02.000Z",
            "warning": "partial profile",
            "warning_code": "partial",
        },
        options=ValidationOptions(strict_unknown_fields=True),
    )

    assert report.unknown_fields == []
    assert report.valid_rows == 1


def test_nested_values_can_be_stringified(tmp_path: Path) -> None:
    run(
        tmp_path,
        {"title": "a", "categories": ["x", "y"], "input": {"url": "https://a.com"}},
        options=ValidationOptions(stringify_nested=True),
    )

    clean = read_lines(tmp_path / "clean.ndjson")[0]
    assert clean["categories"] == '["x","y"]'
    assert clean["input"] == '{"url":"https://a.com"}'


def test_malformed_lines_are_reported(tmp_path: Path) -> None:
    with pytest.raises(SchemaMismatchError):
        run(tmp_path, raw=b'{"title": "a"}\nnot json\n[1, 2]\n\n')

    report = json.loads((tmp_path / "report.json").read_text())
    assert [issue["line"] for issue in report["issues"]] == [2, 3]


def test_snapshot_with_only_errors_is_not_published(tmp_path: Path) -> None:
    with pytest.raises(EmptySnapshotError):
        run(tmp_path, {"error": "blocked", "error_code": "blocked"})


def test_unknown_dataset_still_gets_structural_validation(tmp_path: Path) -> None:
    report = run(tmp_path, {"anything": {"goes": 1}}, fields=())

    assert report.valid_rows == 1
    assert report.unknown_fields == ["anything"]
