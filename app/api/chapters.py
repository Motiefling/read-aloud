from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.models import ChapterResponse, PlaybackStateUpdate, PlaybackStateResponse

router = APIRouter()


@router.get("/{novel_id}/chapters", response_model=list[ChapterResponse])
async def list_chapters(novel_id: str):
    """List all chapters for a novel."""
    # TODO: Query chapters for novel_id from DB
    raise HTTPException(501, "Not implemented")


@router.get("/{novel_id}/chapters/{chapter_num}", response_model=ChapterResponse)
async def get_chapter(novel_id: str, chapter_num: int):
    """Get chapter metadata."""
    # TODO: Query chapter by novel_id and chapter_number from DB
    raise HTTPException(501, "Not implemented")


@router.get("/{novel_id}/chapters/{chapter_num}/audio")
async def stream_chapter_audio(novel_id: str, chapter_num: int):
    """Stream or download the audio file for a specific chapter."""
    # TODO: Look up audio_path from DB, return FileResponse
    # return FileResponse(audio_path, media_type="audio/mpeg", headers={"Accept-Ranges": "bytes"})
    raise HTTPException(501, "Not implemented")


# --- Playback State ---

@router.get("/{novel_id}/playback", response_model=PlaybackStateResponse)
async def get_playback_state(novel_id: str):
    """Get the last playback position for a novel."""
    # TODO: Query playback_state from DB
    raise HTTPException(501, "Not implemented")


@router.put("/{novel_id}/playback")
async def save_playback_state(novel_id: str, state: PlaybackStateUpdate):
    """Save playback position for a novel."""
    # TODO: Upsert playback_state in DB
    raise HTTPException(501, "Not implemented")
