from fastapi import APIRouter, HTTPException

from app.database import get_db
from app.models import QueueReorderRequest, QueueItemResponse
from app.queue_signal import notify_queue_changed

router = APIRouter()


@router.get("", response_model=list[QueueItemResponse])
async def get_queue():
    """List novels in processing queue order."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT n.id as novel_id, n.title, n.queue_position, n.queue_status, "
            "n.total_chapters, n.processed_chapters, "
            "(SELECT COUNT(*) FROM chapters c "
            " WHERE c.novel_id = n.id AND c.status = 'scraped') as scraped_chapters "
            "FROM novels n "
            "WHERE n.queue_position IS NOT NULL "
            "ORDER BY n.queue_position ASC"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


@router.put("/reorder")
async def reorder_queue(request: QueueReorderRequest):
    """Reorder the processing queue. Accepts novel IDs in desired order."""
    db = await get_db()
    try:
        for position, novel_id in enumerate(request.novel_ids, 1):
            cursor = await db.execute(
                "SELECT id FROM novels WHERE id = ? AND queue_position IS NOT NULL",
                (novel_id,),
            )
            if await cursor.fetchone() is None:
                raise HTTPException(
                    400, f"Novel {novel_id} is not in the queue"
                )
            await db.execute(
                "UPDATE novels SET queue_position = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (position, novel_id),
            )
        await db.commit()
    finally:
        await db.close()

    notify_queue_changed()
    return {"status": "reordered"}


@router.delete("/{novel_id}")
async def remove_from_queue(novel_id: str):
    """Remove a novel from the processing queue.

    Already-processed chapters are preserved.
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, queue_position FROM novels WHERE id = ?", (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")
        if row["queue_position"] is None:
            raise HTTPException(400, "Novel is not in the queue")

        # Remove from queue
        await db.execute(
            "UPDATE novels SET queue_position = NULL, queue_status = NULL, "
            "status = CASE "
            "  WHEN processed_chapters > 0 THEN 'completed' "
            "  ELSE 'pending' "
            "END, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (novel_id,),
        )

        # Mark any active processing jobs as cancelled
        await db.execute(
            "UPDATE jobs SET status = 'cancelled', "
            "current_step = 'Removed from queue', "
            "updated_at = CURRENT_TIMESTAMP "
            "WHERE novel_id = ? AND status IN ('queued', 'running')",
            (novel_id,),
        )

        # Clean up incomplete chapters (scraped but not processed)
        await db.execute(
            "DELETE FROM chapters WHERE novel_id = ? AND status NOT IN ('audio_ready', 'translated')",
            (novel_id,),
        )

        await db.commit()
    finally:
        await db.close()

    notify_queue_changed()
    return {"status": "removed"}


@router.put("/{novel_id}/pause")
async def pause_novel(novel_id: str):
    """Pause processing for a novel. It stays in the queue but the dispatcher skips it."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, queue_position, queue_status FROM novels WHERE id = ?",
            (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")
        if row["queue_position"] is None:
            raise HTTPException(400, "Novel is not in the queue")
        if row["queue_status"] == "paused":
            return {"status": "already_paused"}

        await db.execute(
            "UPDATE novels SET queue_status = 'paused', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (novel_id,),
        )
        await db.commit()
    finally:
        await db.close()

    return {"status": "paused"}


@router.put("/{novel_id}/resume")
async def resume_novel(novel_id: str):
    """Resume processing for a paused novel."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, queue_position, queue_status FROM novels WHERE id = ?",
            (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")
        if row["queue_position"] is None:
            raise HTTPException(400, "Novel is not in the queue")
        if row["queue_status"] != "paused":
            return {"status": "not_paused"}

        await db.execute(
            "UPDATE novels SET queue_status = 'queued', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (novel_id,),
        )
        await db.commit()
    finally:
        await db.close()

    notify_queue_changed()
    return {"status": "resumed"}


@router.post("/{novel_id}")
async def add_to_queue(novel_id: str):
    """Re-add a novel to the processing queue (e.g. to resume after cancel)."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, queue_position FROM novels WHERE id = ?", (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")
        if row["queue_position"] is not None:
            raise HTTPException(400, "Novel is already in the queue")

        # Check that there are unprocessed chapters to work on
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM chapters "
            "WHERE novel_id = ? AND status = 'scraped'",
            (novel_id,),
        )
        scraped = (await cursor.fetchone())["cnt"]
        if scraped == 0:
            raise HTTPException(
                400, "No scraped chapters to process — scrape first"
            )

        # Assign next queue position
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
    return {"status": "queued", "queue_position": next_pos}
