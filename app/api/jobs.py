import asyncio
import logging
import uuid as _uuid

from fastapi import APIRouter, HTTPException

from app.database import get_db
from app.models import JobResponse
from app.queue_signal import notify_queue_changed

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


@router.get("", response_model=list[JobResponse])
async def list_jobs():
    """List all jobs (active, completed, failed)."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, novel_id, job_type, status, progress_percent, "
            "current_step, error_message, created_at, updated_at "
            "FROM jobs ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    """Get job status and progress."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, novel_id, job_type, status, progress_percent, "
            "current_step, error_message, created_at, updated_at "
            "FROM jobs WHERE id = ?",
            (job_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")
        return dict(row)
    finally:
        await db.close()


@router.post("/{job_id}/retry", response_model=dict)
async def retry_job(job_id: str):
    """Retry an interrupted or failed job.

    For scrape jobs: re-runs scraping on the server.
    For processing jobs: re-queues the novel for the dispatcher.
    For check_updates: re-dispatches to Celery.
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, novel_id, job_type, status FROM jobs WHERE id = ?",
            (job_id,),
        )
        old_job = await cursor.fetchone()
        if old_job is None:
            raise HTTPException(404, "Job not found")
        if old_job["status"] not in ("interrupted", "failed"):
            raise HTTPException(400, f"Job is {old_job['status']}, not retryable")

        novel_id = old_job["novel_id"]

        cursor = await db.execute(
            "SELECT id, source_url, queue_position FROM novels WHERE id = ?",
            (novel_id,),
        )
        novel_row = await cursor.fetchone()
        if novel_row is None:
            await db.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            await db.commit()
            raise HTTPException(410, "Novel no longer exists")

        # Mark old job as superseded
        await db.execute(
            "UPDATE jobs SET status = 'cancelled', "
            "current_step = 'Superseded by retry', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )

        job_type = old_job["job_type"]

        if job_type == "check_updates":
            # check_updates runs on the server (lightweight, no GPU)
            from app.pipeline.scraper import check_for_updates
            new_job_id = str(_uuid.uuid4())

            # Find last chapter URL
            cursor = await db.execute(
                "SELECT source_url FROM chapters WHERE novel_id = ? "
                "ORDER BY chapter_number DESC LIMIT 1",
                (novel_id,),
            )
            last_ch = await cursor.fetchone()
            if last_ch is None or not last_ch["source_url"]:
                raise HTTPException(400, "No chapters to check updates from")

            await db.execute(
                "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
                "VALUES (?, ?, 'check_updates', 'running', 'Checking for new chapters')",
                (new_job_id, novel_id),
            )
            await db.commit()

            last_url = last_ch["source_url"]

            async def _run_check():
                db2 = await get_db()
                try:
                    has_updates = await check_for_updates(last_url)
                    await db2.execute(
                        "UPDATE jobs SET status = 'completed', progress_percent = 100, "
                        "current_step = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        ("New chapters available" if has_updates else "No new chapters",
                         new_job_id),
                    )
                    await db2.commit()
                except Exception as e:
                    await db2.execute(
                        "UPDATE jobs SET status = 'failed', error_message = ?, "
                        "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (str(e), new_job_id),
                    )
                    await db2.commit()
                finally:
                    await db2.close()

            _fire_background(_run_check())
            return {"job_id": new_job_id, "status": "running"}

        if job_type in ("scrape", "scrape_only"):
            # Re-run scrape on the server
            from app.scrape_worker import scrape_and_store
            new_job_id = str(_uuid.uuid4())
            await db.execute(
                "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
                "VALUES (?, ?, 'scrape', 'queued', 'Queued — retrying scrape')",
                (new_job_id, novel_id),
            )
            if novel_row["queue_position"] is None:
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

            logger.info("Retrying scrape for novel %s", novel_id)
            _fire_background(
                scrape_and_store(
                    novel_id, new_job_id, novel_row["source_url"],
                )
            )
            return {"job_id": new_job_id, "status": "queued"}

        # For processing/full_pipeline/add_chapters: ensure novel is in queue
        # Reset errored chapters back to 'scraped' so the dispatcher retries them
        await db.execute(
            "UPDATE chapters SET status = 'scraped' "
            "WHERE novel_id = ? AND status = 'error'",
            (novel_id,),
        )

        if novel_row["queue_position"] is None:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(queue_position), 0) + 1 as next_pos FROM novels"
            )
            next_pos = (await cursor.fetchone())["next_pos"]
            await db.execute(
                "UPDATE novels SET queue_position = ?, queue_status = 'queued', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (next_pos, novel_id),
            )
        else:
            await db.execute(
                "UPDATE novels SET queue_status = 'queued', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (novel_id,),
            )

        await db.commit()

        notify_queue_changed()
        return {"job_id": job_id, "status": "requeued"}
    finally:
        await db.close()


@router.delete("/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running or queued job."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, status, novel_id, job_type FROM jobs WHERE id = ?",
            (job_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")
        if row["status"] in ("completed", "failed"):
            raise HTTPException(400, f"Job already {row['status']}")

        novel_id = row["novel_id"]

        # Mark job as cancelled
        await db.execute(
            "UPDATE jobs SET status = 'cancelled', "
            "current_step = 'Cancelled by user', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )

        # Remove novel from queue (dispatcher will skip it on next iteration)
        await db.execute(
            "UPDATE novels SET queue_position = NULL, queue_status = NULL, "
            "status = CASE "
            "  WHEN processed_chapters > 0 THEN 'completed' "
            "  ELSE 'pending' "
            "END, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (novel_id,),
        )

        # Clean up incomplete chapters
        await db.execute(
            "DELETE FROM chapters WHERE novel_id = ? AND status NOT IN ('audio_ready', 'translated')",
            (novel_id,),
        )
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
            (novel_id,),
        )
        count = (await cursor.fetchone())["cnt"]
        await db.execute(
            "UPDATE novels SET total_chapters = ?, processed_chapters = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (count, count, novel_id),
        )

        await db.commit()
    finally:
        await db.close()

    notify_queue_changed()
    return {"status": "cancelled"}
