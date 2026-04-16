import asyncio
import logging
import uuid

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.database import get_db
from app.models import NovelRequest, NovelResponse, AddChaptersRequest, RenameRequest
from app.config import get_data_dir

logger = logging.getLogger(__name__)
router = APIRouter()


def _fire_background(coro):
    """Launch a coroutine as a background task with exception logging."""
    async def _wrapper():
        try:
            await coro
        except Exception:
            logger.exception("Background task failed")
    asyncio.create_task(_wrapper())


@router.post("", response_model=dict)
async def request_novel(request: NovelRequest):
    """Accept a novel URL, create a job, and start scraping + queue for processing."""
    from app.scrape_worker import scrape_and_store

    novel_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    title = request.title or "Untitled Novel"

    db = await get_db()
    try:
        # Assign next queue position
        cursor = await db.execute(
            "SELECT COALESCE(MAX(queue_position), 0) + 1 as next_pos FROM novels"
        )
        next_pos = (await cursor.fetchone())["next_pos"]

        await db.execute(
            "INSERT INTO novels (id, title, source_url, dictionary_id, status, "
            "queue_position, queue_status) "
            "VALUES (?, ?, ?, ?, 'pending', ?, 'scraping')",
            (novel_id, title, request.url, request.dictionary_id, next_pos),
        )
        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'scrape', 'queued', 'Queued — scraping chapters')",
            (job_id, novel_id),
        )
        await db.commit()
    finally:
        await db.close()

    start_url = request.start_chapter_url or request.url

    # Run scraping as a background task on the server
    logger.info("Launching scrape task for novel %s (%s)", novel_id, start_url)
    _fire_background(
        scrape_and_store(novel_id, job_id, start_url, request.max_chapters)
    )

    return {"novel_id": novel_id, "job_id": job_id}


@router.get("", response_model=list[NovelResponse])
async def list_novels():
    """List all novels."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, title, source_url, dictionary_id, cover_image_path, total_chapters, "
            "processed_chapters, status, queue_position, queue_status, created_at, updated_at "
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
            "processed_chapters, status, queue_position, queue_status, created_at, updated_at "
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
    """Check for new chapters (lightweight scrape check, no translation or TTS)."""
    from app.pipeline.scraper import check_for_updates

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id FROM novels WHERE id = ?", (novel_id,),
        )
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")

        # Find the last chapter's source URL
        cursor = await db.execute(
            "SELECT source_url FROM chapters WHERE novel_id = ? "
            "ORDER BY chapter_number DESC LIMIT 1",
            (novel_id,),
        )
        last_chapter = await cursor.fetchone()
        if last_chapter is None or not last_chapter["source_url"]:
            raise HTTPException(400, "No chapters to check updates from")

        job_id = str(uuid.uuid4())
        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'check_updates', 'running', 'Checking for new chapters')",
            (job_id, novel_id),
        )
        await db.commit()
    finally:
        await db.close()

    # Run the check as a background task on the server
    async def _run_check():
        db2 = await get_db()
        try:
            has_updates = await check_for_updates(last_chapter["source_url"])
            await db2.execute(
                "UPDATE jobs SET status = 'completed', progress_percent = 100, "
                "current_step = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (
                    "New chapters available" if has_updates else "No new chapters",
                    job_id,
                ),
            )
            await db2.commit()
        except Exception as e:
            await db2.execute(
                "UPDATE jobs SET status = 'failed', error_message = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (str(e), job_id),
            )
            await db2.commit()
        finally:
            await db2.close()

    _fire_background(_run_check())
    return {"job_id": job_id}


@router.post("/{novel_id}/add-chapters", response_model=dict)
async def add_chapters(novel_id: str, request: AddChaptersRequest):
    """Add more chapters to an existing novel (continue from where it left off)."""
    from app.scrape_worker import scrape_additional_chapters

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, queue_position FROM novels WHERE id = ?", (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")

        job_id = str(uuid.uuid4())
        step = "Queued — scraping more chapters"
        if request.max_chapters:
            step = f"Queued — scraping {request.max_chapters} more chapters"

        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'scrape', 'queued', ?)",
            (job_id, novel_id, step),
        )

        # Ensure novel is in the queue
        if row["queue_position"] is None:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(queue_position), 0) + 1 as next_pos FROM novels"
            )
            next_pos = (await cursor.fetchone())["next_pos"]
            await db.execute(
                "UPDATE novels SET queue_position = ?, queue_status = 'scraping', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (next_pos, novel_id),
            )
        else:
            await db.execute(
                "UPDATE novels SET queue_status = 'scraping', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (novel_id,),
            )

        await db.commit()
    finally:
        await db.close()

    # Run scraping as a background task on the server
    logger.info("Launching additional chapter scrape for novel %s", novel_id)
    _fire_background(
        scrape_additional_chapters(novel_id, job_id, request.max_chapters, request.start_url)
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
    """Delete a novel, remove from queue, and remove audio files."""
    import shutil
    from app.queue_signal import notify_queue_changed

    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM novels WHERE id = ?", (novel_id,))
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")

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

    # Wake dispatcher so it skips this novel if it was active
    notify_queue_changed()

    return {"status": "deleted"}
