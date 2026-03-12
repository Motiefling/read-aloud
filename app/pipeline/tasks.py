"""
Celery task definitions for the background processing pipeline.

Orchestrates the full pipeline: scrape -> translate -> TTS for each novel.
"""

from celery import Celery

from app.config import settings

celery_app = Celery(
    "audiobook",
    broker=settings.celery.broker_url,
    backend=settings.celery.result_backend,
)


@celery_app.task(bind=True)
def process_novel(self, job_id: str, novel_id: str, start_url: str):
    """
    Master task: orchestrates the full pipeline for a novel.

    Steps:
    1. Scrape all chapters
    2. For each chapter: translate -> generate TTS audio
    3. Notify client as each chapter becomes available
    """
    # TODO: Implement full pipeline orchestration
    # - Update job status to "running"
    # - Scrape all chapters
    # - For each chapter:
    #   - Translate
    #   - Generate TTS audio
    #   - Notify client via WebSocket
    #   - Update job progress
    # - Mark job as "completed"
    raise NotImplementedError


@celery_app.task(bind=True)
def scrape_novel_task(self, job_id: str, novel_id: str, start_url: str):
    """Scrape-only task for a novel."""
    # TODO: Implement scrape-only pipeline step
    raise NotImplementedError


@celery_app.task(bind=True)
def translate_chapter_task(self, job_id: str, chapter_id: str):
    """Translate a single chapter."""
    # TODO: Implement translation pipeline step
    raise NotImplementedError


@celery_app.task(bind=True)
def generate_audio_task(self, job_id: str, chapter_id: str):
    """Generate TTS audio for a single chapter."""
    # TODO: Implement TTS pipeline step
    raise NotImplementedError
