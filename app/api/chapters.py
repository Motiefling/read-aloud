from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.config import BASE_DIR
from app.database import get_db
from app.models import ChapterResponse, PlaybackStateUpdate, PlaybackStateResponse, RenameRequest

router = APIRouter()


@router.get("/{novel_id}/chapters", response_model=list[ChapterResponse])
async def list_chapters(novel_id: str):
    """List all chapters for a novel."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, novel_id, chapter_number, title, title_english, status, "
            "audio_duration_seconds, audio_file_size_bytes "
            "FROM chapters WHERE novel_id = ? ORDER BY chapter_number",
            (novel_id,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


@router.get("/{novel_id}/chapters/{chapter_num}", response_model=ChapterResponse)
async def get_chapter(novel_id: str, chapter_num: int):
    """Get chapter metadata."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, novel_id, chapter_number, title, title_english, status, "
            "audio_duration_seconds, audio_file_size_bytes "
            "FROM chapters WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Chapter not found")
        return dict(row)
    finally:
        await db.close()


@router.patch("/{novel_id}/chapters/{chapter_num}")
async def rename_chapter(novel_id: str, chapter_num: int, request: RenameRequest):
    """Rename a chapter."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id FROM chapters WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Chapter not found")
        await db.execute(
            "UPDATE chapters SET title = ? WHERE novel_id = ? AND chapter_number = ?",
            (request.title, novel_id, chapter_num),
        )
        await db.commit()
    finally:
        await db.close()
    return {"status": "updated"}


@router.delete("/{novel_id}/chapters/{chapter_num}")
async def delete_chapter(novel_id: str, chapter_num: int):
    """Delete a single chapter and its audio file."""
    import os

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, audio_path FROM chapters "
            "WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Chapter not found")

        audio_path = row["audio_path"]
        await db.execute(
            "DELETE FROM chapters WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        await db.execute(
            "UPDATE novels SET total_chapters = total_chapters - 1 WHERE id = ?",
            (novel_id,),
        )
        await db.commit()
    finally:
        await db.close()

    # Clean up audio file on disk
    if audio_path:
        full_path = BASE_DIR / audio_path
        if full_path.exists():
            os.remove(full_path)

    return {"status": "deleted"}


@router.get("/{novel_id}/chapters/{chapter_num}/audio")
async def stream_chapter_audio(novel_id: str, chapter_num: int):
    """Stream or download the audio file for a specific chapter."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT audio_path FROM chapters "
            "WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Chapter not found")
        if not row["audio_path"]:
            raise HTTPException(404, "Audio not yet generated for this chapter")
    finally:
        await db.close()

    audio_path = BASE_DIR / row["audio_path"]
    if not audio_path.exists():
        raise HTTPException(404, "Audio file not found on disk")

    return FileResponse(
        str(audio_path),
        media_type="audio/mpeg",
        filename=f"chapter_{chapter_num:04d}.mp3",
        headers={"Accept-Ranges": "bytes"},
    )


# --- Playback State ---

@router.get("/{novel_id}/playback", response_model=PlaybackStateResponse)
async def get_playback_state(novel_id: str):
    """Get the last playback position for a novel."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT novel_id, chapter_number, position_seconds, "
            "playback_speed, updated_at "
            "FROM playback_state WHERE novel_id = ?",
            (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return PlaybackStateResponse(
                novel_id=novel_id, chapter_number=1, position_seconds=0, playback_speed=2.0,
            )
        return dict(row)
    finally:
        await db.close()


@router.put("/{novel_id}/playback")
async def save_playback_state(novel_id: str, state: PlaybackStateUpdate):
    """Save playback position for a novel."""
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO playback_state (novel_id, chapter_number, position_seconds, playback_speed) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(novel_id) DO UPDATE SET "
            "chapter_number = excluded.chapter_number, "
            "position_seconds = excluded.position_seconds, "
            "playback_speed = excluded.playback_speed, "
            "updated_at = CURRENT_TIMESTAMP",
            (novel_id, state.chapter_number, state.position_seconds, state.playback_speed),
        )
        await db.commit()
    finally:
        await db.close()

    return {"status": "saved"}
