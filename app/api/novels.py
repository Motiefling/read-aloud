import uuid

from fastapi import APIRouter, HTTPException

from app.database import get_db
from app.models import NovelRequest, NovelResponse, AddChaptersRequest
from app.config import BASE_DIR, settings

router = APIRouter()


@router.post("", response_model=dict)
async def request_novel(request: NovelRequest):
    """Accept a novel URL, create a job, and start processing."""
    from app.pipeline.tasks import process_novel

    novel_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    title = request.title or "Untitled Novel"

    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO novels (id, title, source_url, dictionary_id, status) "
            "VALUES (?, ?, ?, ?, 'pending')",
            (novel_id, title, request.url, request.dictionary_id),
        )
        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'full_pipeline', 'queued', 'Queued')",
            (job_id, novel_id),
        )
        await db.commit()
    finally:
        await db.close()

    process_novel.delay(job_id, novel_id, request.url, request.max_chapters)

    return {"novel_id": novel_id, "job_id": job_id}


@router.get("", response_model=list[NovelResponse])
async def list_novels():
    """List all novels."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, title, source_url, dictionary_id, total_chapters, "
            "processed_chapters, status, created_at, updated_at "
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
            "SELECT id, title, source_url, dictionary_id, total_chapters, "
            "processed_chapters, status, created_at, updated_at "
            "FROM novels WHERE id = ?",
            (novel_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Novel not found")
        return dict(row)
    finally:
        await db.close()


@router.post("/{novel_id}/update", response_model=dict)
async def update_novel(novel_id: str):
    """Check for new chapters and process only the new ones."""
    from app.pipeline.tasks import update_novel as update_novel_task

    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM novels WHERE id = ?", (novel_id,))
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")

        job_id = str(uuid.uuid4())
        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'update', 'queued', 'Queued — checking for new chapters')",
            (job_id, novel_id),
        )
        await db.commit()
    finally:
        await db.close()

    update_novel_task.delay(job_id, novel_id)
    return {"job_id": job_id}


@router.post("/{novel_id}/add-chapters", response_model=dict)
async def add_chapters(novel_id: str, request: AddChaptersRequest):
    """Add more chapters to an existing novel (continue from where it left off)."""
    from app.pipeline.tasks import add_chapters_task

    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM novels WHERE id = ?", (novel_id,))
        if await cursor.fetchone() is None:
            raise HTTPException(404, "Novel not found")

        job_id = str(uuid.uuid4())
        step = "Queued — downloading more chapters"
        if request.max_chapters:
            step = f"Queued — downloading {request.max_chapters} more chapters"

        await db.execute(
            "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
            "VALUES (?, ?, 'add_chapters', 'queued', ?)",
            (job_id, novel_id, step),
        )
        await db.commit()
    finally:
        await db.close()

    add_chapters_task.delay(job_id, novel_id, request.max_chapters)
    return {"job_id": job_id}


@router.delete("/{novel_id}")
async def delete_novel(novel_id: str):
    """Delete a novel and its audio files."""
    import shutil

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

    audio_dir = BASE_DIR / settings.server.data_dir / "novels" / novel_id
    if audio_dir.exists():
        shutil.rmtree(audio_dir)

    return {"status": "deleted"}
