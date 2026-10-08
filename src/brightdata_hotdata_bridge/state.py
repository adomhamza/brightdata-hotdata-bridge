"""Local run records that make every pipeline stage resumable and replayable.

Each snapshot gets a directory under ``<state_dir>/runs/<snapshot_id>/`` holding:

* ``run.json``: the run record (stage reached, target table, upload id, last error)
* ``raw.ndjson``: the snapshot as downloaded from Bright Data
* ``clean.ndjson``: validated records, the file that is uploaded to Hotdata
* ``rejected.ndjson``: records Bright Data marked as errors
* ``report.json``: the validation report

Records are written atomically (temp file + rename), so a crash never leaves a
half-written record behind.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field, ValidationError

from brightdata_hotdata_bridge.errors import RunStateError
from brightdata_hotdata_bridge.models import (
    LoadMode,
    TableTarget,
    ValidationOptions,
    ValidationReport,
    validate_snapshot_id,
)

RECORD_FILENAME = "run.json"


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class RunStage(str, enum.Enum):
    """Furthest stage a run has completed, in pipeline order."""

    TRIGGERED = "triggered"
    READY = "ready"
    DOWNLOADED = "downloaded"
    VALIDATED = "validated"
    UPLOADED = "uploaded"
    LOADED = "loaded"

    @property
    def order(self) -> int:
        """Position of the stage in the pipeline, for comparisons."""
        return list(RunStage).index(self)

    def reached(self, other: RunStage) -> bool:
        """Whether this stage is ``other`` or later.

        Args:
            other: Stage to compare against.

        Returns:
            ``True`` when ``other`` has already been completed.
        """
        return self.order >= other.order


class RunError(BaseModel):
    """The most recent failure of a run."""

    stage: str
    error_type: str
    message: str
    occurred_at: datetime


class RunRecord(BaseModel):
    """Everything needed to resume or replay a run for one snapshot."""

    snapshot_id: str
    stage: RunStage
    target: TableTarget | None = None
    validation: ValidationOptions = ValidationOptions()
    dataset_id: str | None = None
    method: str | None = None
    input_count: int | None = None
    database_id: str | None = None
    upload_id: str | None = None
    uploaded_at: datetime | None = None
    table_row_count: int | None = None
    loaded_mode: LoadMode | None = None
    loaded_schema: str | None = None
    report: ValidationReport | None = None
    last_error: RunError | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


@dataclass(frozen=True)
class RunPaths:
    """File locations for one run."""

    directory: Path
    record: Path
    raw: Path
    clean: Path
    rejected: Path
    report: Path


def run_paths(state_dir: Path, snapshot_id: str) -> RunPaths:
    """Compute the file locations for a snapshot's run.

    Args:
        state_dir: Root state directory.
        snapshot_id: Snapshot the run belongs to. Validated, so it cannot escape
            ``state_dir``.

    Returns:
        Paths for the record and data files. Nothing is created.
    """
    directory = state_dir / "runs" / validate_snapshot_id(snapshot_id)
    return RunPaths(
        directory=directory,
        record=directory / RECORD_FILENAME,
        raw=directory / "raw.ndjson",
        clean=directory / "clean.ndjson",
        rejected=directory / "rejected.ndjson",
        report=directory / "report.json",
    )


def load_run(state_dir: Path, snapshot_id: str) -> RunRecord | None:
    """Read a run record if one exists.

    Args:
        state_dir: Root state directory.
        snapshot_id: Snapshot to look up.

    Returns:
        The record, or ``None`` when the snapshot has never been seen locally.

    Raises:
        RunStateError: The record exists but cannot be read or parsed.
    """
    path = run_paths(state_dir, snapshot_id).record
    if not path.exists():
        return None
    try:
        return RunRecord.model_validate_json(path.read_bytes())
    except (OSError, ValidationError) as exc:
        raise RunStateError(
            f"Run record {path} is unreadable: {exc}", snapshot_id=snapshot_id
        ) from exc


def save_run(state_dir: Path, record: RunRecord) -> RunRecord:
    """Persist a run record atomically, stamping ``updated_at``.

    Args:
        state_dir: Root state directory.
        record: Record to store.

    Returns:
        The stored record (a copy with the new ``updated_at``).
    """
    stored = record.model_copy(update={"updated_at": utc_now()})
    paths = run_paths(state_dir, record.snapshot_id)
    paths.directory.mkdir(parents=True, exist_ok=True)
    temp_path = paths.record.with_suffix(".json.tmp")
    temp_path.write_text(stored.model_dump_json(indent=2), encoding="utf-8")
    temp_path.replace(paths.record)
    return stored


def list_runs(state_dir: Path) -> list[RunRecord]:
    """List all local run records, most recently updated first.

    Args:
        state_dir: Root state directory.

    Returns:
        Readable run records. Unreadable ones are skipped.
    """
    runs_dir = state_dir / "runs"
    if not runs_dir.exists():
        return []
    records: list[RunRecord] = []
    for record_path in runs_dir.glob(f"*/{RECORD_FILENAME}"):
        try:
            records.append(RunRecord.model_validate_json(record_path.read_bytes()))
        except (OSError, ValidationError):
            continue
    return sorted(records, key=lambda record: record.updated_at, reverse=True)
