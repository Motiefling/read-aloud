"""
Internal API for the Celery worker.

These endpoints exist so the worker never needs to open the SQLite database
directly. Two processes (FastAPI on the server, Celery on the GPU box)
writing to one SQLite file over a network share leads to corruption; having
FastAPI be the sole DB writer eliminates that class of failure.

Not authenticated — reachable only over Tailscale. Not for browser clients.
"""
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.database import get_db
from app.queue_signal import notify_queue_changed

router = APIRouter()


# ===================== Schemas =====================

class WorkChapter(BaseModel):
    id: str
    novel_id: str
    chapter_number: int
    title: str | None = None


class WorkNovel(BaseModel):
    id: str
    title: str | None = None


class WorkProgress(BaseModel):
    ready: int
    total: int


class WorkResponse(BaseModel):
    chapter: WorkChapter
    novel: WorkNovel
    job_id: str
    progress: WorkProgress


class NovelTitleUpdate(BaseModel):
    title: str


class ChapterTranslatedUpdate(BaseModel):
    title_english: str | None = None


class ChapterAudioReadyUpdate(BaseModel):
    audio_path: str
    duration_seconds: float
    file_size_bytes: int


class JobUpdate(BaseModel):
    status: str | None = None
    current_step: str | None = None
    progress_percent: float | None = None
    error_message: str | None = None


# ===================== Endpoints =====================

@router.get("/work/next")
async def get_next_work() -> dict[str, Any] | None:
    """Lease the next chapter for processing.

    Atomically: picks the highest-priority scraped chapter, marks its novel
    active + processing, and finds/creates the tracking job. Returns 204 when
    no work is available so the worker can block on pub/sub.
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT c.id, c.novel_id, c.chapter_number, c.title "
            "FROM chapters c "
            "JOIN novels n ON c.novel_id = n.id "
            "WHERE n.queue_position IS NOT NULL "
            "  AND n.queue_status IN ('queued', 'active') "
            "  AND c.status = 'scraped' "
            "ORDER BY n.queue_position ASC, c.chapter_number ASC "
            "LIMIT 1"
        )
        chapter_row = await cursor.fetchone()
        if chapter_row is None:
            return None

        novel_id = chapter_row["novel_id"]

        await db.execute(
            "UPDATE novels SET queue_status = 'active', status = 'processing', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (novel_id,),
        )

        cursor = await db.execute(
            "SELECT id FROM jobs WHERE novel_id = ? AND job_type = 'processing' "
            "AND status IN ('queued', 'running') ORDER BY created_at DESC LIMIT 1",
            (novel_id,),
        )
        job_row = await cursor.fetchone()
        if job_row:
            job_id = job_row["id"]
        else:
            job_id = str(uuid.uuid4())
            await db.execute(
                "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
                "VALUES (?, ?, 'processing', 'running', 'Processing')",
                (job_id, novel_id),
            )

        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
            (novel_id,),
        )
        total = (await cursor.fetchone())["cnt"]
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM chapters "
            "WHERE novel_id = ? AND status = 'audio_ready'",
            (novel_id,),
        )
        ready = (await cursor.fetchone())["cnt"]

        cursor = await db.execute(
            "SELECT title FROM novels WHERE id = ?", (novel_id,),
        )
        novel_row = await cursor.fetchone()
        novel_title = novel_row["title"] if novel_row else None

        await db.commit()

        return {
            "chapter": {
                "id": chapter_row["id"],
                "novel_id": novel_id,
                "chapter_number": chapter_row["chapter_number"],
                "title": chapter_row["title"],
            },
            "novel": {"id": novel_id, "title": novel_title},
            "job_id": job_id,
            "progress": {"ready": ready, "total": total},
        }
    finally:
        await db.close()


@router.patch("/novels/{novel_id}/title")
async def update_novel_title(novel_id: str, update: NovelTitleUpdate):
    """Replace a novel's title (used by the worker after translating a Chinese title)."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "UPDATE novels SET title = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (update.title, novel_id),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Novel not found")
        await db.commit()
    finally:
        await db.close()
    return {"status": "ok"}


@router.patch("/chapters/{chapter_id}/translated")
async def mark_chapter_translated(chapter_id: str, update: ChapterTranslatedUpdate):
    """Mark a chapter as translated. English text is written to disk by the worker."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "UPDATE chapters SET title_english = ?, status = 'translated' WHERE id = ?",
            (update.title_english, chapter_id),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Chapter not found")
        await db.commit()
    finally:
        await db.close()
    return {"status": "ok"}


@router.patch("/chapters/{chapter_id}/audio-ready")
async def mark_chapter_audio_ready(chapter_id: str, update: ChapterAudioReadyUpdate):
    """Mark a chapter as audio_ready, recount novel progress, and close the job if done."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "UPDATE chapters SET audio_path = ?, audio_duration_seconds = ?, "
            "audio_file_size_bytes = ?, status = 'audio_ready' WHERE id = ?",
            (update.audio_path, update.duration_seconds, update.file_size_bytes,
             chapter_id),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Chapter not found")

        cursor = await db.execute(
            "SELECT novel_id FROM chapters WHERE id = ?", (chapter_id,),
        )
        novel_id = (await cursor.fetchone())["novel_id"]

        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM chapters "
            "WHERE novel_id = ? AND status = 'audio_ready'",
            (novel_id,),
        )
        ready = (await cursor.fetchone())["cnt"]
        await db.execute(
            "UPDATE novels SET processed_chapters = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (ready, novel_id),
        )

        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM chapters "
            "WHERE novel_id = ? AND status = 'scraped'",
            (novel_id,),
        )
        remaining = (await cursor.fetchone())["cnt"]

        completed = False
        if remaining == 0:
            cursor = await db.execute(
                "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
                (novel_id,),
            )
            total = (await cursor.fetchone())["cnt"]
            await db.execute(
                "UPDATE novels SET queue_position = NULL, queue_status = NULL, "
                "status = 'completed', total_chapters = ?, processed_chapters = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (total, ready, novel_id),
            )
            await db.execute(
                "UPDATE jobs SET status = 'completed', "
                "current_step = 'Done', progress_percent = 100, "
                "updated_at = CURRENT_TIMESTAMP "
                "WHERE novel_id = ? AND job_type = 'processing' "
                "AND status IN ('queued', 'running')",
                (novel_id,),
            )
            completed = True

        await db.commit()
    finally:
        await db.close()

    return {"status": "ok", "novel_completed": completed, "processed_chapters": ready}


@router.patch("/chapters/{chapter_id}/error")
async def mark_chapter_error(chapter_id: str):
    """Mark a chapter as errored (translation or TTS failed)."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "UPDATE chapters SET status = 'error' WHERE id = ?", (chapter_id,),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Chapter not found")
        await db.commit()
    finally:
        await db.close()
    return {"status": "ok"}


@router.patch("/jobs/{job_id}")
async def update_job(job_id: str, update: JobUpdate):
    """Update mutable job fields (status, current_step, progress_percent, error_message)."""
    fields = update.model_dump(exclude_none=True)
    if not fields:
        return {"status": "noop"}

    sets = ", ".join(f"{k} = ?" for k in fields)
    sets += ", updated_at = CURRENT_TIMESTAMP"
    db = await get_db()
    try:
        cursor = await db.execute(
            f"UPDATE jobs SET {sets} WHERE id = ?",
            (*fields.values(), job_id),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Job not found")
        await db.commit()
    finally:
        await db.close()
    return {"status": "ok"}


@router.post("/queue/wake")
async def wake_queue():
    """Wake the dispatcher. Currently unused — dispatcher subscribes to Redis directly —
    but exposed so the worker could poke itself via HTTP if needed."""
    notify_queue_changed()
    return {"status": "ok"}
