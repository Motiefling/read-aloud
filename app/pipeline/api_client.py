"""
HTTP client the Celery worker uses to talk to the FastAPI server.

Every piece of database mutation the worker used to do against SQLite now
goes through here — /internal/* endpoints on the FastAPI server. This is
what lets the server be the sole writer of the DB file.
"""
import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class ApiError(RuntimeError):
    """Raised when an /internal/* call fails in a way the caller should handle."""


def _base_url() -> str:
    return settings.internal_api.base_url.rstrip("/")


def _timeout() -> float:
    return settings.internal_api.request_timeout_seconds


def _client() -> httpx.Client:
    return httpx.Client(base_url=_base_url(), timeout=_timeout())


def get_next_work() -> dict | None:
    """Lease the next chapter. Returns None when there's no work."""
    with _client() as c:
        r = c.get("/internal/work/next")
        r.raise_for_status()
        if r.status_code == 204 or not r.content:
            return None
        data = r.json()
        return data or None


def update_novel_title(novel_id: str, title: str) -> None:
    with _client() as c:
        r = c.patch(f"/internal/novels/{novel_id}/title", json={"title": title})
        r.raise_for_status()


def mark_chapter_translated(
    chapter_id: str,
    title_english: str | None,
    pre_replacements_hash: str | None = None,
) -> None:
    with _client() as c:
        r = c.patch(
            f"/internal/chapters/{chapter_id}/translated",
            json={
                "title_english": title_english,
                "pre_replacements_hash": pre_replacements_hash,
            },
        )
        r.raise_for_status()


def mark_chapter_audio_ready(
    chapter_id: str,
    audio_path: str,
    duration_seconds: float,
    file_size_bytes: int,
    post_replacements_hash: str | None = None,
) -> dict:
    with _client() as c:
        r = c.patch(
            f"/internal/chapters/{chapter_id}/audio-ready",
            json={
                "audio_path": audio_path,
                "duration_seconds": duration_seconds,
                "file_size_bytes": file_size_bytes,
                "post_replacements_hash": post_replacements_hash,
            },
        )
        r.raise_for_status()
        return r.json()


def get_replacements(novel_id: str, kind: str) -> tuple[list[tuple[str, str]], str]:
    """Fetch the active rule list (novel-scoped + global) plus its hash.

    ``kind`` is ``"pre"`` or ``"post"``.  Returns ``([(find, replace)...], hash)``.
    """
    with _client() as c:
        r = c.get(f"/internal/novels/{novel_id}/replacements/{kind}")
        r.raise_for_status()
        data = r.json()
    rules = [(row["find_text"], row["replace_text"] or "") for row in data["rules"]]
    return rules, data["hash"]


def mark_chapter_error(chapter_id: str) -> None:
    with _client() as c:
        r = c.patch(f"/internal/chapters/{chapter_id}/error")
        r.raise_for_status()


def update_job(
    job_id: str,
    *,
    status: str | None = None,
    current_step: str | None = None,
    progress_percent: float | None = None,
    error_message: str | None = None,
) -> None:
    payload = {
        k: v
        for k, v in {
            "status": status,
            "current_step": current_step,
            "progress_percent": progress_percent,
            "error_message": error_message,
        }.items()
        if v is not None
    }
    if not payload:
        return
    with _client() as c:
        r = c.patch(f"/internal/jobs/{job_id}", json=payload)
        r.raise_for_status()
