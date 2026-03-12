from fastapi import APIRouter, HTTPException

from app.models import NovelRequest, NovelResponse

router = APIRouter()


@router.post("", response_model=dict)
async def request_novel(request: NovelRequest):
    """Accept a novel URL, create a job, and start processing."""
    # TODO: Create novel record in DB
    # TODO: Create job and dispatch to Celery task queue
    raise HTTPException(501, "Not implemented")


@router.get("", response_model=list[NovelResponse])
async def list_novels():
    """List all novels."""
    # TODO: Query all novels from DB
    raise HTTPException(501, "Not implemented")


@router.get("/{novel_id}", response_model=NovelResponse)
async def get_novel(novel_id: str):
    """Get novel details and chapter list."""
    # TODO: Query novel by ID from DB
    raise HTTPException(501, "Not implemented")


@router.delete("/{novel_id}")
async def delete_novel(novel_id: str):
    """Delete a novel and its audio files."""
    # TODO: Delete novel, chapters, audio files, and DB records
    raise HTTPException(501, "Not implemented")
