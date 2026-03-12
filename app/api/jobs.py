from fastapi import APIRouter, HTTPException

from app.models import JobResponse

router = APIRouter()


@router.get("", response_model=list[JobResponse])
async def list_jobs():
    """List all jobs (active, completed, failed)."""
    # TODO: Query all jobs from DB
    raise HTTPException(501, "Not implemented")


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    """Get job status and progress."""
    # TODO: Query job by ID from DB
    raise HTTPException(501, "Not implemented")


@router.delete("/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running or queued job."""
    # TODO: Cancel Celery task and update DB
    raise HTTPException(501, "Not implemented")
