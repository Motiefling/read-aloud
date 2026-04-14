import uuid

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.database import get_db
from app.models import NovelRequest, NovelResponse, AddChaptersRequest, RenameRequest
from app.config import get_data_dir

router = APIRouter()


@router.post("", response_model=dict)
async def request_novel(request: NovelRequest):
    """Accept a novel URL, create a job, and start processing."""
    from app.pipeline.tasks import process_novel

    novel_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    title = request.title or "Untitled Novel"

    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO novels (id, title, source_url, dictionary_id, status) "
            "VALUES (?, ?, ?, ?, 'pending')",
            (novel_id, title, request.url, request.dictionary_id),
        )
        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'full_pipeline', 'queued', 'Queued')",
            (job_id, novel_id),
        )
        await db.commit()
    finally:
        await db.close()

    start_url = request.start_chapter_url or request.url
    process_novel.apply_async(
        args=[job_id, novel_id, start_url, request.max_chapters],
        task_id=job_id,
    )

    return {"novel_id": novel_id, "job_id": job_id}


@router.get("", response_model=list[NovelResponse])
async def list_novels():
    """List all novels."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, title, source_url, dictionary_id, cover_image_path, total_chapters, "
            "processed_chapters, status, created_at, updated_at "
            "FROM novels ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


@router.get("/{novel_id}", response_model=NovelResponse)
async def get_novel(novel_id: str):
    """Get novel details."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, title, source_url, dictionary_id, cover_image_path, total_chapters, "
            "processed_chapters, status, created_at, updated_at "
            "FROM novels WHERE id = ?",
            (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")
        return dict(row)
    finally:
        await db.close()


@router.post("/{novel_id}/check-updates", response_model=dict)
async def check_updates(novel_id: str):
    """Check for new chapters (scrape only, no translation or TTS)."""
    from app.pipeline.tasks import check_updates_task

    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM novels WHERE id = ?", (novel_id,))
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")

        job_id = str(uuid.uuid4())
        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'check_updates', 'queued', 'Queued — checking for new chapters')",
            (job_id, novel_id),
        )
        await db.commit()
    finally:
        await db.close()

    check_updates_task.apply_async(args=[job_id, novel_id], task_id=job_id)
    return {"job_id": job_id}


@router.post("/{novel_id}/add-chapters", response_model=dict)
async def add_chapters(novel_id: str, request: AddChaptersRequest):
    """Add more chapters to an existing novel (continue from where it left off)."""
    from app.pipeline.tasks import add_chapters_task

    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM novels WHERE id = ?", (novel_id,))
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")

        job_id = str(uuid.uuid4())
        step = "Queued — downloading more chapters"
        if request.max_chapters:
            step = f"Queued — downloading {request.max_chapters} more chapters"

        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'add_chapters', 'queued', ?)",
            (job_id, novel_id, step),
        )
        await db.commit()
    finally:
        await db.close()

    add_chapters_task.apply_async(
        args=[job_id, novel_id, request.max_chapters, request.start_url],
        task_id=job_id,
    )
    return {"job_id": job_id}


@router.patch("/{novel_id}")
async def rename_novel(novel_id: str, request: RenameRequest):
    """Rename a novel."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM novels WHERE id = ?", (novel_id,))
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")
        await db.execute(
            "UPDATE novels SET title = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (request.title, novel_id),
        )
        await db.commit()
    finally:
        await db.close()
    return {"status": "updated"}


@router.get("/{novel_id}/cover")
async def get_cover_image(novel_id: str):
    """Serve the cover image for a novel."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT cover_image_path FROM novels WHERE id = ?", (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")
        if not row["cover_image_path"]:
            raise HTTPException(404, "No cover image available")
    finally:
        await db.close()

    cover_path = get_data_dir() / row["cover_image_path"]
    if not cover_path.exists():
        raise HTTPException(404, "Cover image file not found on disk")

    media_type = "image/jpeg"
    if cover_path.suffix == ".png":
        media_type = "image/png"
    elif cover_path.suffix == ".webp":
        media_type = "image/webp"

    return FileResponse(str(cover_path), media_type=media_type)


@router.delete("/{novel_id}")
async def delete_novel(novel_id: str):
    """Delete a novel, cancel active jobs, and remove audio files."""
    import shutil
    from app.pipeline.tasks import celery_app

    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM novels WHERE id = ?", (novel_id,))
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")

        # Revoke any active Celery tasks for this novel before deleting
        cursor = await db.execute(
            "SELECT id FROM jobs WHERE novel_id = ? AND status IN ('queued', 'running')",
            (novel_id,),
        )
        active_jobs = await cursor.fetchall()
        for job_row in active_jobs:
            celery_app.control.revoke(job_row["id"], terminate=True)

        await db.execute("DELETE FROM chapters WHERE novel_id = ?", (novel_id,))
        await db.execute("DELETE FROM jobs WHERE novel_id = ?", (novel_id,))
        await db.execute("DELETE FROM playback_state WHERE novel_id = ?", (novel_id,))
        await db.execute("DELETE FROM novels WHERE id = ?", (novel_id,))
        await db.commit()
    finally:
        await db.close()

    audio_dir = get_data_dir() / "novels" / novel_id
    if audio_dir.exists():
        shutil.rmtree(audio_dir)

    return {"status": "deleted"}
