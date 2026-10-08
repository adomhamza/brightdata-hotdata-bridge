"""Logging configuration: readable text for terminals, JSON lines for log pipelines.

Pipeline log calls attach context such as ``snapshot_id`` and ``upload_id`` through
``extra=``. Both formats render that context, so every failure can be traced back to
the affected snapshot.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

CONTEXT_FIELDS = ("snapshot_id", "upload_id", "table", "mode", "rows")
PACKAGE_LOGGER = "brightdata_hotdata_bridge"


class JsonFormatter(logging.Formatter):
    """Render each record as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialise a log record, including context fields and exception details.

        Args:
            record: The record to format.

        Returns:
            A single-line JSON document.
        """
        payload: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_context(record))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ContextFormatter(logging.Formatter):
    """Plain-text formatter that appends context fields as ``key=value`` pairs."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a record and append its context fields.

        Args:
            record: The record to format.

        Returns:
            The formatted line.
        """
        line = super().format(record)
        context = _context(record)
        if not context:
            return line
        pairs = " ".join(f"{key}={value}" for key, value in context.items())
        return f"{line} [{pairs}]"


class _StderrHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """Writes to whatever ``sys.stderr`` is at emit time, so redirection is respected."""

    @property
    def stream(self) -> object:
        return sys.stderr

    @stream.setter
    def stream(self, _value: object) -> None:
        """Ignore the stream pinned by ``StreamHandler.__init__``."""


def configure_logging(*, level: str = "INFO", json_output: bool = False) -> None:
    """Send package logs to stderr in the chosen format.

    Calling it again replaces the previous configuration.

    Args:
        level: Minimum level name, for example ``INFO`` or ``DEBUG``.
        json_output: Emit JSON lines instead of human-readable text.
    """
    handler = _StderrHandler()
    formatter: logging.Formatter = (
        JsonFormatter()
        if json_output
        else ContextFormatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    )
    handler.setFormatter(formatter)

    package_logger = logging.getLogger(PACKAGE_LOGGER)
    package_logger.handlers.clear()
    package_logger.addHandler(handler)
    package_logger.setLevel(level.upper())
    package_logger.propagate = False


def _context(record: logging.LogRecord) -> dict[str, object]:
    return {
        field: getattr(record, field)
        for field in CONTEXT_FIELDS
        if getattr(record, field, None) is not None
    }
