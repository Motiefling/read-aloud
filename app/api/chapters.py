from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.api.replacements import fetch_rules
from app.config import get_data_dir
from app.database import get_db
from app.models import ChapterResponse, PlaybackStateUpdate, PlaybackStateResponse, RenameRequest
from app.queue_signal import notify_queue_changed
from app.utils.replacements import hash_rules

router = APIRouter()


_CHAPTER_COLS = (
    "id, novel_id, chapter_number, title, title_english, status, "
    "audio_duration_seconds, audio_file_size_bytes, "
    "pre_replacements_hash, post_replacements_hash"
)


def _annotate_staleness(rows, current_pre_hash: str, current_post_hash: str) -> list[dict]:
    """Add ``translation_stale`` and ``audio_stale`` flags to chapter rows.

    A chapter is stale when its stored hash exists and differs from the
    current rule-set hash.  Chapters that have never been processed have a
    NULL stored hash and are not marked stale -- their state is whatever the
    ``status`` field already says.
    """
    out = []
    for row in rows:
        d = dict(row)
        stored_pre = d.get("pre_replacements_hash")
        stored_post = d.get("post_replacements_hash")
        d["translation_stale"] = bool(stored_pre) and stored_pre != current_pre_hash
        d["audio_stale"] = bool(stored_post) and stored_post != current_post_hash
        out.append(d)
    return out


async def _current_hashes(db, novel_id: str) -> tuple[str, str]:
    pre_rules = await fetch_rules(db, "pre", novel_id)
    post_rules = await fetch_rules(db, "post", novel_id)
    return hash_rules(pre_rules), hash_rules(post_rules)


@router.get("/{novel_id}/chapters", response_model=list[ChapterResponse])
async def list_chapters(novel_id: str):
    """List all chapters for a novel."""
    db = await get_db()
    try:
        cursor = await db.execute(
            f"SELECT {_CHAPTER_COLS} "
            "FROM chapters WHERE novel_id = ? ORDER BY chapter_number",
            (novel_id,),
        )
        rows = await cursor.fetchall()
        pre_hash, post_hash = await _current_hashes(db, novel_id)
        return _annotate_staleness(rows, pre_hash, post_hash)
    finally:
        await db.close()


@router.get("/{novel_id}/chapters/{chapter_num}", response_model=ChapterResponse)
async def get_chapter(novel_id: str, chapter_num: int):
    """Get chapter metadata."""
    db = await get_db()
    try:
        cursor = await db.execute(
            f"SELECT {_CHAPTER_COLS} "
            "FROM chapters WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Chapter not found")
        pre_hash, post_hash = await _current_hashes(db, novel_id)
        return _annotate_staleness([row], pre_hash, post_hash)[0]
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
            "UPDATE chapters SET title_english = ? WHERE novel_id = ? AND chapter_number = ?",
            (request.title, novel_id, chapter_num),
        )
        await db.commit()
    finally:
        await db.close()
    return {"status": "updated"}


@router.delete("/{novel_id}/chapters/{chapter_num}")
async def delete_chapter(novel_id: str, chapter_num: int):
    """Delete a single chapter and its audio + text files."""
    import os
    from app.pipeline.chapter_storage import delete_chapter_text

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
        full_path = get_data_dir() / audio_path
        if full_path.exists():
            os.remove(full_path)

    # Clean up chapter text files (.zh.txt / .en.txt)
    delete_chapter_text(novel_id, chapter_num)

    return {"status": "deleted"}


@router.post("/{novel_id}/chapters/{chapter_num}/retry")
async def retry_chapter(novel_id: str, chapter_num: int):
    """Retry translate + TTS for a single chapter by resetting it to 'scraped'
    and ensuring the novel is queued, so the dispatcher picks it up next."""
    from app.pipeline.chapter_storage import has_zh

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, status FROM chapters "
            "WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Chapter not found")
        if row["status"] not in ("error", "scraped", "audio_ready", "translated"):
            raise HTTPException(400, f"Chapter is {row['status']}, not retryable")
        if not has_zh(novel_id, chapter_num):
            raise HTTPException(400, "Chapter has no Chinese text file to process")

        await db.execute(
            "UPDATE chapters SET status = 'scraped' WHERE id = ?", (row["id"],),
        )

        # Make sure the novel is in the queue so the dispatcher picks this up.
        cursor = await db.execute(
            "SELECT queue_position FROM novels WHERE id = ?", (novel_id,),
        )
        novel_row = await cursor.fetchone()
        if novel_row and novel_row["queue_position"] is None:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(queue_position), 0) + 1 as next_pos FROM novels"
            )
            next_pos = (await cursor.fetchone())["next_pos"]
            await db.execute(
                "UPDATE novels SET queue_position = ?, queue_status = 'queued', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (next_pos, novel_id),
            )

        await db.commit()
    finally:
        await db.close()

    notify_queue_changed()
    return {"status": "queued"}


@router.post("/{novel_id}/chapters/{chapter_num}/reprocess-tts")
async def reprocess_chapter_tts(novel_id: str, chapter_num: int):
    """Re-run only the TTS phase for a single chapter.

    Resets the chapter to ``translated`` so the dispatcher picks it up but
    skips Qwen and reuses the existing English text on disk.  The ``.en.txt``
    must already exist; otherwise the user has to re-translate from scratch.
    """
    from app.pipeline.chapter_storage import has_en

    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, status FROM chapters "
            "WHERE novel_id = ? AND chapter_number = ?",
            (novel_id, chapter_num),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Chapter not found")
        if row["status"] not in ("audio_ready", "translated", "error"):
            raise HTTPException(400, f"Chapter is {row['status']}, cannot re-TTS")
        if not has_en(novel_id, chapter_num):
            raise HTTPException(
                400,
                "No English text on disk for this chapter; use the regular "
                "Re-process to re-translate.",
            )

        await db.execute(
            "UPDATE chapters SET status = 'translated' WHERE id = ?",
            (row["id"],),
        )

        cursor = await db.execute(
            "SELECT queue_position FROM novels WHERE id = ?", (novel_id,),
        )
        novel_row = await cursor.fetchone()
        if novel_row and novel_row["queue_position"] is None:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(queue_position), 0) + 1 as next_pos FROM novels"
            )
            next_pos = (await cursor.fetchone())["next_pos"]
            await db.execute(
                "UPDATE novels SET queue_position = ?, queue_status = 'queued', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (next_pos, novel_id),
            )

        await db.commit()
    finally:
        await db.close()

    notify_queue_changed()
    return {"status": "queued"}


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

    audio_path = get_data_dir() / row["audio_path"]
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
