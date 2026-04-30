"""
Append-only error log for the Celery worker.

Writes structured entries to <repo_root>/error_log.txt whenever a chapter is
marked as errored or skipped. The file lives next to the source tree (NOT
under data_dir) so it stays on the worker's local disk even when data_dir
points to a network share.

Each entry includes a timestamp, the phase that failed, full chapter context,
the reason, and any extra fields supplied by the caller (text snippets,
exception traceback, etc.).
"""

from __future__ import annotations

import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

# <repo_root>/error_log.txt — resolved from this file's location, so it is
# independent of cwd and unaffected by config.data_dir.
_LOG_PATH = Path(__file__).resolve().parent.parent.parent / "error_log.txt"

_lock = threading.Lock()


def _format_value(value: Any) -> str:
    if isinstance(value, str):
        # Indent multi-line strings so the log stays readable
        if "\n" in value:
            indented = "\n    ".join(value.splitlines())
            return f"\n    {indented}"
        return value
    return repr(value)


def record_error(
    *,
    novel_id: str | None,
    chapter_number: int | None,
    chapter_id: str | None,
    phase: str,
    reason: str,
    exc: BaseException | None = None,
    **extra: Any,
) -> None:
    """Append a structured error entry to error_log.txt.

    Args:
        novel_id: Novel identifier, if known.
        chapter_number: Chapter number, if known.
        chapter_id: Database chapter ID, if known.
        phase: Pipeline phase that failed (e.g. "translate", "tts",
            "missing_zh", "empty_translation", "dispatcher").
        reason: Short human-readable reason.
        exc: Exception object — its traceback will be included.
        **extra: Additional context fields (text snippets, durations, etc.).
    """
    timestamp = datetime.now().isoformat(timespec="seconds")

    lines = [
        f"[{timestamp}] {phase.upper()}: {reason}",
        f"  novel_id={novel_id} chapter_number={chapter_number} chapter_id={chapter_id}",
    ]
    for key, value in extra.items():
        lines.append(f"  {key}={_format_value(value)}")

    if exc is not None:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        indented_tb = "    " + tb.rstrip().replace("\n", "\n    ")
        lines.append("  traceback:\n" + indented_tb)

    entry = "\n".join(lines) + "\n\n"

    with _lock:
        try:
            _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with _LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(entry)
        except OSError:
            # Logging must never crash the worker
            pass


def get_error_log_path() -> Path:
    """Return the path the error log will be written to."""
    return _LOG_PATH
