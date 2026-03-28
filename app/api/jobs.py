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


@router.delete("/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running or queued job."""
    from app.pipeline.tasks import celery_app

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, status FROM jobs WHERE id = ?", (job_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")
        if row["status"] in ("completed", "failed"):
            raise HTTPException(400, f"Job already {row['status']}")

        celery_app.control.revoke(job_id, terminate=True)

        await db.execute(
            "UPDATE jobs SET status = 'cancelled', "
            "current_step = 'Cancelled by user', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )
        await db.commit()
    finally:
        await db.close()

    return {"status": "cancelled"}
