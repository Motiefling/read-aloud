from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import BASE_DIR
from app.database import init_db
from app.api.novels import router as novels_router
from app.api.chapters import router as chapters_router
from app.api.jobs import router as jobs_router
from app.api.dictionaries import router as dictionaries_router
from app.api.settings import router as settings_router
from app.api.websocket import router as websocket_router
from app.api.queue import router as queue_router
from app.api.internal import router as internal_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    await init_db()
    await _mark_stale_jobs_interrupted()
    yield


async def _mark_stale_jobs_interrupted():
    """Mark any queued/running jobs from a previous session as interrupted.

    On server restart, these jobs are no longer being executed by Celery
    (the queue was purged on startup). The user must manually retry them.
    """
    from app.database import get_db

    import logging
    log = logging.getLogger(__name__)

    db = await get_db()
    try:
        cursor = await db.execute(
            "UPDATE jobs SET status = 'interrupted', "
            "current_step = 'Server restarted — waiting for you to resume', "
            "updated_at = CURRENT_TIMESTAMP "
            "WHERE status IN ('queued', 'running')"
        )
        if cursor.rowcount > 0:
            # Also reset novel status so they don't show as "processing"
            await db.execute(
                "UPDATE novels SET status = CASE "
                "  WHEN (SELECT COUNT(*) FROM chapters WHERE chapters.novel_id = novels.id AND chapters.status = 'audio_ready') > 0 "
                "    THEN 'completed' "
                "  ELSE 'pending' "
                "END, updated_at = CURRENT_TIMESTAMP "
                "WHERE status IN ('processing', 'scraped')"
            )
            await db.commit()
        log.info(
            "Marked %d stale jobs as interrupted on startup", cursor.rowcount
        )

        # Reset 'active' novels back to 'queued' so the dispatcher picks them up
        cursor = await db.execute(
            "UPDATE novels SET queue_status = 'queued', "
            "updated_at = CURRENT_TIMESTAMP "
            "WHERE queue_status = 'active'"
        )
        if cursor.rowcount > 0:
            await db.commit()
            log.info(
                "Reset %d active-queue novels to queued on startup",
                cursor.rowcount,
            )

        # Clear 'scraping' novels that didn't finish (server-side scrape was interrupted)
        cursor = await db.execute(
            "UPDATE novels SET queue_status = 'queued', "
            "updated_at = CURRENT_TIMESTAMP "
            "WHERE queue_status = 'scraping'"
        )
        if cursor.rowcount > 0:
            await db.commit()
            log.info(
                "Reset %d scraping novels on startup", cursor.rowcount,
            )
    finally:
        await db.close()


app = FastAPI(
    title="Light Novel Audiobook Server",
    description="Converts Chinese light novels into English audiobooks.",
    version="0.1.0",
    lifespan=lifespan,
)

# --- API Routes ---
app.include_router(novels_router, prefix="/api/novels", tags=["novels"])
app.include_router(chapters_router, prefix="/api/novels", tags=["chapters"])
app.include_router(jobs_router, prefix="/api/jobs", tags=["jobs"])
app.include_router(dictionaries_router, prefix="/api/dictionaries", tags=["dictionaries"])
app.include_router(settings_router, prefix="/api/settings", tags=["settings"])
app.include_router(queue_router, prefix="/api/queue", tags=["queue"])
app.include_router(internal_router, prefix="/internal", tags=["internal"])
app.include_router(websocket_router, tags=["websocket"])

# --- Serve PWA static files ---
pwa_dir = BASE_DIR / "pwa"
if pwa_dir.exists():
    app.mount("/", StaticFiles(directory=str(pwa_dir), html=True), name="pwa")
