from fastapi import APIRouter, HTTPException

from app.database import get_db
from app.models import JobResponse

router = APIRouter()


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
    """Retry an interrupted or failed job by creating a new one."""
    from app.pipeline.tasks import (
        process_novel, scrape_novel_task, add_chapters_task, check_updates_task,
    )
    import uuid as _uuid

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

        # Verify the novel still exists
        cursor = await db.execute(
            "SELECT id, source_url FROM novels WHERE id = ?", (novel_id,),
        )
        novel_row = await cursor.fetchone()
        if novel_row is None:
            # Novel was deleted — clean up the orphaned job
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

        # Create new job
        new_job_id = str(_uuid.uuid4())
        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, ?, 'queued', 'Queued — retrying')",
            (new_job_id, novel_id, old_job["job_type"]),
        )
        await db.commit()
    finally:
        await db.close()

    # Dispatch the appropriate Celery task
    job_type = old_job["job_type"]
    source_url = novel_row["source_url"]
    if job_type == "full_pipeline":
        process_novel.apply_async(
            args=[new_job_id, novel_id, source_url], task_id=new_job_id,
        )
    elif job_type == "scrape_only":
        scrape_novel_task.apply_async(
            args=[new_job_id, novel_id, source_url], task_id=new_job_id,
        )
    elif job_type == "add_chapters":
        add_chapters_task.apply_async(
            args=[new_job_id, novel_id], task_id=new_job_id,
        )
    elif job_type == "check_updates":
        check_updates_task.apply_async(
            args=[new_job_id, novel_id], task_id=new_job_id,
        )
    else:
        # Unknown job type — just re-run full pipeline
        process_novel.apply_async(
            args=[new_job_id, novel_id, source_url], task_id=new_job_id,
        )

    return {"job_id": new_job_id, "status": "queued"}


@router.delete("/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running or queued job."""
    from app.pipeline.tasks import celery_app

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, status, novel_id FROM jobs WHERE id = ?", (job_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")
        if row["status"] in ("completed", "failed"):
            raise HTTPException(400, f"Job already {row['status']}")

        # For interrupted jobs, just mark cancelled (no Celery task to revoke)
        if row["status"] == "interrupted":
            await db.execute(
                "UPDATE jobs SET status = 'cancelled', "
                "current_step = 'Dismissed by user', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (job_id,),
            )
            await db.commit()
            return {"status": "cancelled"}

        novel_id = row["novel_id"]

        # Revoke the Celery task (task_id matches job_id via apply_async)
        celery_app.control.revoke(job_id, terminate=True)

        # Mark job as cancelled
        await db.execute(
            "UPDATE jobs SET status = 'cancelled', "
            "current_step = 'Cancelled by user', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )

        # Clean up incomplete chapters and reset novel status
        await db.execute(
            "DELETE FROM chapters WHERE novel_id = ? AND status != 'audio_ready'",
            (novel_id,),
        )
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
            (novel_id,),
        )
        count = (await cursor.fetchone())["cnt"]
        novel_status = "completed" if count > 0 else "pending"
        await db.execute(
            "UPDATE novels SET total_chapters = ?, processed_chapters = ?, "
            "status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (count, count, novel_status, novel_id),
        )
        await db.commit()
    finally:
        await db.close()

    return {"status": "cancelled"}
