"""Error hierarchy for the bridge.

Every error raised on purpose derives from :class:`BridgeError`, so callers can catch the
whole family with one ``except``. Each subclass carries a distinct ``exit_code`` that the
CLI uses, which lets schedulers and shell scripts branch on the failure type.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar


class BridgeError(Exception):
    """Base class for all errors raised by this package."""

    exit_code: ClassVar[int] = 1

    def __init__(self, message: str, *, snapshot_id: str | None = None) -> None:
        """Create the error.

        Args:
            message: Human-readable description of what went wrong.
            snapshot_id: The Bright Data snapshot the failure relates to, when known.
        """
        super().__init__(message)
        self.message = message
        self.snapshot_id = snapshot_id

    def __str__(self) -> str:
        """Return the message, suffixed with the snapshot id when one is attached."""
        if self.snapshot_id is None:
            return self.message
        return f"{self.message} (snapshot_id={self.snapshot_id})"


class ConfigurationError(BridgeError):
    """Required settings are missing or invalid."""

    exit_code: ClassVar[int] = 2


class CatalogError(BridgeError):
    """The Bright Data scraper catalog could not be loaded."""

    exit_code: ClassVar[int] = 3


class UnknownDatasetError(CatalogError):
    """The dataset id is not a triggerable Bright Data scraper."""


class UnsupportedMethodError(CatalogError):
    """The scraper does not offer the requested collection method."""


class InputValidationError(BridgeError):
    """Collection inputs do not match the scraper's input schema."""

    exit_code: ClassVar[int] = 3

    def __init__(self, message: str, *, issues: list[str]) -> None:
        """Create the error.

        Args:
            message: Summary of the failure.
            issues: One entry per offending row or column.
        """
        super().__init__(message)
        self.issues = issues


class BrightDataApiError(BridgeError):
    """Bright Data returned an unexpected HTTP response or could not be reached."""

    exit_code: ClassVar[int] = 4

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body: str | None = None,
        snapshot_id: str | None = None,
    ) -> None:
        """Create the error.

        Args:
            message: Summary of the failure.
            status_code: HTTP status returned by Bright Data, if any response arrived.
            body: Response body (truncated) for diagnosis.
            snapshot_id: Related snapshot, when known.
        """
        super().__init__(message, snapshot_id=snapshot_id)
        self.status_code = status_code
        self.body = body


class RateLimitedError(BrightDataApiError):
    """Bright Data kept answering 429 after every backoff attempt."""


class CollectionFailedError(BridgeError):
    """The Bright Data collection ended as ``failed`` or ``canceled``."""

    exit_code: ClassVar[int] = 5

    def __init__(self, message: str, *, snapshot_id: str, status: str) -> None:
        """Create the error.

        Args:
            message: Summary including Bright Data's error message when available.
            snapshot_id: The failed snapshot.
            status: Terminal status reported by Bright Data.
        """
        super().__init__(message, snapshot_id=snapshot_id)
        self.status = status


class SnapshotTimeoutError(BridgeError):
    """The snapshot did not become ready within the configured time."""

    exit_code: ClassVar[int] = 6


class SchemaMismatchError(BridgeError):
    """Downloaded records do not match the expected fields or types."""

    exit_code: ClassVar[int] = 7

    def __init__(self, message: str, *, snapshot_id: str, report_path: Path) -> None:
        """Create the error.

        Args:
            message: Summary of the mismatch.
            snapshot_id: The snapshot whose data was rejected.
            report_path: JSON report listing every issue found.
        """
        super().__init__(message, snapshot_id=snapshot_id)
        self.report_path = report_path


class EmptySnapshotError(BridgeError):
    """The snapshot contained no loadable records."""

    exit_code: ClassVar[int] = 7


class HotdataWriteError(BridgeError):
    """Uploading to or loading into Hotdata failed."""

    exit_code: ClassVar[int] = 8

    def __init__(
        self,
        message: str,
        *,
        stage: str,
        snapshot_id: str | None = None,
        status_code: int | None = None,
        error_code: str | None = None,
        trace_id: str | None = None,
    ) -> None:
        """Create the error.

        Args:
            message: Summary of the failure.
            stage: Which step failed: ``database lookup``, ``upload`` or ``load``.
            snapshot_id: Related snapshot, when known.
            status_code: HTTP status returned by Hotdata, if any.
            error_code: Hotdata's machine-readable error code, if any.
            trace_id: Hotdata ``X-Trace-Id`` to quote in support requests.
        """
        super().__init__(message, snapshot_id=snapshot_id)
        self.stage = stage
        self.status_code = status_code
        self.error_code = error_code
        self.trace_id = trace_id


class RunStateError(BridgeError):
    """A local run record is missing or cannot be resumed."""

    exit_code: ClassVar[int] = 9
