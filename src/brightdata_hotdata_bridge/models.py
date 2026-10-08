"""Typed request, response and catalog models shared across the package."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LoadMode = Literal["replace", "append", "upsert", "update", "delete"]
KEYED_LOAD_MODES: frozenset[str] = frozenset({"upsert", "update", "delete"})

DEFAULT_METHOD = "collect_by_url"
DISCOVER_PREFIX = "discover_by_"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_DATASET_ID = re.compile(r"^gd_[A-Za-z0-9]{1,64}$")


def validate_snapshot_id(value: str) -> str:
    """Reject snapshot ids that are not plain tokens.

    Snapshot ids are used in URLs and local file paths, so anything other than letters,
    digits, ``_`` and ``-`` is refused to rule out path traversal and request smuggling.

    Args:
        value: Candidate snapshot id.

    Returns:
        The unchanged id.

    Raises:
        ValueError: The id contains disallowed characters or is empty.
    """
    if not _SNAPSHOT_ID.fullmatch(value):
        raise ValueError(f"invalid snapshot id {value!r}")
    return value


def validate_dataset_id(value: str) -> str:
    """Check that a dataset id has Bright Data's ``gd_...`` shape.

    Args:
        value: Candidate dataset id.

    Returns:
        The unchanged id.

    Raises:
        ValueError: The id is malformed.
    """
    if not _DATASET_ID.fullmatch(value):
        raise ValueError(
            f"invalid dataset id {value!r}; expected something like gd_l7q7dkf244hwjntr0"
        )
    return value


def validate_identifier(value: str) -> str:
    """Check that a schema, table or column name is a plain SQL identifier.

    Args:
        value: Candidate identifier.

    Returns:
        The unchanged identifier.

    Raises:
        ValueError: The name is empty, too long, or contains characters other than
            letters, digits and underscores (or starts with a digit).
    """
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"invalid identifier {value!r}; use letters, digits and underscores, "
            "starting with a letter or underscore"
        )
    return value


class FieldSpec(BaseModel):
    """One input or output field of a scraper, as described by the catalog."""

    model_config = ConfigDict(frozen=True)

    name: str
    type: str
    required: bool = False
    description: str | None = None


class CollectionMethod(BaseModel):
    """A way of running a scraper, such as ``collect_by_url`` or ``discover_by_keyword``."""

    model_config = ConfigDict(frozen=True)

    name: str
    input_schema: tuple[FieldSpec, ...] = ()
    output_fields: tuple[FieldSpec, ...] = ()

    @property
    def discover_by(self) -> str | None:
        """The ``discover_by`` query value for discovery methods, else ``None``."""
        if not self.name.startswith(DISCOVER_PREFIX):
            return None
        return self.name.removeprefix(DISCOVER_PREFIX)

    @property
    def required_inputs(self) -> tuple[str, ...]:
        """Names of the inputs every row must provide."""
        return tuple(spec.name for spec in self.input_schema if spec.required)


class Scraper(BaseModel):
    """A triggerable Bright Data scraper and its collection methods."""

    model_config = ConfigDict(frozen=True)

    dataset_id: str
    name: str
    domain: str | None = None
    methods: dict[str, CollectionMethod]


class SnapshotProgress(BaseModel):
    """Status of a Bright Data snapshot as reported by the progress endpoint."""

    model_config = ConfigDict(extra="ignore")

    snapshot_id: str
    status: str
    dataset_id: str | None = None
    records: int | None = None
    errors: int | None = None
    error_message: str | None = None

    @property
    def is_ready(self) -> bool:
        """Whether results can be downloaded."""
        return self.status == "ready"

    @property
    def is_failed(self) -> bool:
        """Whether the collection ended without usable results."""
        return self.status in {"failed", "canceled"}


class CollectionRequest(BaseModel):
    """What to collect from Bright Data."""

    dataset_id: str
    inputs: list[dict[str, Any]] = Field(min_length=1)
    method: str = DEFAULT_METHOD
    limit_per_input: int | None = Field(default=None, ge=1)

    @field_validator("dataset_id")
    @classmethod
    def _check_dataset_id(cls, value: str) -> str:
        return validate_dataset_id(value)


class TableTarget(BaseModel):
    """Where and how the snapshot is published in Hotdata."""

    model_config = ConfigDict(frozen=True)

    table: str
    schema_name: str | None = None
    mode: LoadMode = "replace"
    key: tuple[str, ...] | None = None

    @field_validator("table", "schema_name")
    @classmethod
    def _check_names(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_identifier(value)

    @field_validator("key")
    @classmethod
    def _check_key(cls, value: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("key must name at least one column")
        return tuple(validate_identifier(column) for column in value)


class ValidationOptions(BaseModel):
    """How strictly downloaded records are checked before they are published."""

    model_config = ConfigDict(frozen=True)

    required_fields: tuple[str, ...] = ()
    strict_unknown_fields: bool = False
    stringify_nested: bool = False


class PushRequest(BaseModel):
    """A complete pipeline run: collect (or reuse a snapshot), validate, publish.

    Provide exactly one of ``collection`` (start a new Bright Data job) or
    ``snapshot_id`` (publish an existing snapshot).
    """

    target: TableTarget
    collection: CollectionRequest | None = None
    snapshot_id: str | None = None
    validation: ValidationOptions = ValidationOptions()

    @field_validator("snapshot_id")
    @classmethod
    def _check_snapshot_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_snapshot_id(value)

    @model_validator(mode="after")
    def _exactly_one_source(self) -> PushRequest:
        if (self.collection is None) == (self.snapshot_id is None):
            raise ValueError("provide exactly one of 'collection' or 'snapshot_id'")
        return self


class ValidationIssue(BaseModel):
    """A single problem found in the downloaded snapshot."""

    line: int
    field: str | None = None
    problem: str


class ValidationReport(BaseModel):
    """Outcome of validating a downloaded snapshot file."""

    total_rows: int = 0
    valid_rows: int = 0
    error_rows: int = 0
    issue_count: int = 0
    issues: list[ValidationIssue] = Field(default_factory=list)
    unknown_fields: list[str] = Field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Whether the snapshot can be published."""
        return self.issue_count == 0


class HotdataDatabase(BaseModel):
    """A Hotdata managed database that loads are published into."""

    model_config = ConfigDict(frozen=True)

    id: str
    name: str | None = None
    default_schema: str | None = None


class LoadResult(BaseModel):
    """What Hotdata reported after publishing."""

    table: str
    schema_name: str
    mode: LoadMode
    row_count: int


class PushResult(BaseModel):
    """Summary of a completed pipeline run."""

    snapshot_id: str
    dataset_id: str | None
    upload_id: str
    database_id: str | None = None
    table: str
    schema_name: str
    mode: LoadMode
    rows_published: int
    rows_rejected: int
    table_row_count: int
    already_loaded: bool = False
