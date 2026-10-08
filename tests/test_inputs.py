from __future__ import annotations

import json
from pathlib import Path

import pytest

from brightdata_hotdata_bridge.catalog import parse_catalog, resolve_method
from brightdata_hotdata_bridge.errors import InputValidationError
from brightdata_hotdata_bridge.inputs import inputs_from_values, read_inputs_csv, validate_inputs
from brightdata_hotdata_bridge.models import CollectionMethod
from tests.conftest import AMAZON_ID, CATALOG


@pytest.fixture
def collect_by_url() -> CollectionMethod:
    return resolve_method(parse_catalog(json.dumps(CATALOG)), AMAZON_ID, "collect_by_url")[1]


@pytest.fixture
def discover_by_keyword() -> CollectionMethod:
    return resolve_method(parse_catalog(json.dumps(CATALOG)), AMAZON_ID, "discover_by_keyword")[1]


def write_csv(tmp_path: Path, content: str, *, bom: bool = False) -> Path:
    path = tmp_path / "inputs.csv"
    path.write_text(("\ufeff" if bom else "") + content, encoding="utf-8")
    return path


def test_csv_rows_are_coerced_to_declared_types(
    tmp_path: Path, collect_by_url: CollectionMethod
) -> None:
    path = write_csv(
        tmp_path,
        "url,zipcode,all_variations\n"
        "https://www.amazon.com/dp/B1,10001,true\n"
        "https://www.amazon.com/dp/B2,,\n",
        bom=True,
    )

    rows = read_inputs_csv(path, collect_by_url)

    assert rows == [
        {"url": "https://www.amazon.com/dp/B1", "zipcode": "10001", "all_variations": True},
        {"url": "https://www.amazon.com/dp/B2"},
    ]


def test_unknown_and_missing_columns_are_rejected_before_any_row(
    tmp_path: Path, collect_by_url: CollectionMethod
) -> None:
    path = write_csv(tmp_path, "link,zipcode\nhttps://x.com,1\n")

    with pytest.raises(InputValidationError) as excinfo:
        read_inputs_csv(path, collect_by_url)

    assert any("unknown column 'link'" in issue for issue in excinfo.value.issues)
    assert any("missing required column 'url'" in issue for issue in excinfo.value.issues)


def test_every_bad_value_is_reported_with_its_line(
    tmp_path: Path, collect_by_url: CollectionMethod
) -> None:
    path = write_csv(tmp_path, "url,all_variations\nftp://bad,maybe\n,true\nhttps://ok.com,false\n")

    with pytest.raises(InputValidationError) as excinfo:
        read_inputs_csv(path, collect_by_url)

    issues = excinfo.value.issues
    assert "line 2: 'url' must be an http(s) URL, got 'ftp://bad'" in issues
    assert "line 2: 'all_variations' must be true or false, got 'maybe'" in issues
    assert "line 3: 'url' is required" in issues
    assert len(issues) == 3


def test_header_only_file_has_no_inputs(tmp_path: Path, collect_by_url: CollectionMethod) -> None:
    with pytest.raises(InputValidationError, match="No collection inputs"):
        read_inputs_csv(write_csv(tmp_path, "url\n"), collect_by_url)


def test_empty_file_has_no_header(tmp_path: Path, collect_by_url: CollectionMethod) -> None:
    with pytest.raises(InputValidationError, match="no header row"):
        read_inputs_csv(write_csv(tmp_path, ""), collect_by_url)


def test_queries_fill_the_single_required_input(discover_by_keyword: CollectionMethod) -> None:
    assert inputs_from_values(["laptop", " ", "phone "], discover_by_keyword) == [
        {"keyword": "laptop"},
        {"keyword": "phone"},
    ]


def test_native_values_are_validated(discover_by_keyword: CollectionMethod) -> None:
    rows = validate_inputs([{"keyword": "tv", "pages": "3"}], discover_by_keyword)
    assert rows == [{"keyword": "tv", "pages": 3}]

    with pytest.raises(InputValidationError):
        validate_inputs([{"keyword": "tv", "pages": True}], discover_by_keyword)
