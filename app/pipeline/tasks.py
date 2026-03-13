"""
Celery task definitions for the background processing pipeline.

Orchestrates the full pipeline: scrape -> translate -> TTS for each novel.
"""

import asyncio
import logging
import sqlite3
import uuid

from celery import Celery

from app.config import settings, BASE_DIR

logger = logging.getLogger(__name__)

_DATABASE_PATH = str(BASE_DIR / settings.server.database_path)

celery_app = Celery(
    "audiobook",
    broker=settings.celery.broker_url,
    backend=settings.celery.result_backend,
)


def _update_job(conn, job_id: str, **fields):
    """Helper to update job fields."""
    sets = ", ".join(f"{k} = ?" for k in fields)
    sets += ", updated_at = CURRENT_TIMESTAMP"
    conn.execute(f"UPDATE jobs SET {sets} WHERE id = ?", (*fields.values(), job_id))
    conn.commit()


@celery_app.task(bind=True)
def process_novel(self, job_id: str, novel_id: str, start_url: str):
    """
    Master task: orchestrates the full pipeline for a novel.

    Steps:
    1. Scrape all chapters
    2. For each chapter: translate
    3. (TTS will be added later)
    """
    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        _update_job(conn, job_id, status="running", current_step="Scraping chapters")

        # --- Step 1: Scrape ---
        chapter_ids = _scrape_and_store(conn, job_id, novel_id, start_url)

        if not chapter_ids:
            _update_job(conn, job_id, status="completed", current_step="No chapters found")
            return {"job_id": job_id, "status": "completed", "chapters": 0}

        # --- Step 2: Translate each chapter ---
        total = len(chapter_ids)
        _update_job(conn, job_id, current_step=f"Translating 0/{total}")

        from app.pipeline.translator import get_translator, TranslationError
        from app.utils.term_dictionary import load_dictionary

        translator = get_translator()
        term_dict = load_dictionary(novel_id)

        for i, chapter_id in enumerate(chapter_ids, 1):
            row = conn.execute(
                "SELECT chinese_text, chapter_number FROM chapters WHERE id = ?",
                (chapter_id,),
            ).fetchone()

            if not row or not row["chinese_text"]:
                logger.warning("Chapter %s has no text, skipping translation", chapter_id)
                continue

            logger.info(
                "Translating chapter %d/%d (chapter #%d)",
                i, total, row["chapter_number"],
            )
            try:
                english_text = translator.translate_chapter(
                    row["chinese_text"], term_dict
                )
                conn.execute(
                    "UPDATE chapters SET english_text = ?, status = 'translated' WHERE id = ?",
                    (english_text, chapter_id),
                )
                conn.execute(
                    "UPDATE novels SET processed_chapters = ? WHERE id = ?",
                    (i, novel_id),
                )
                progress = (i / total) * 100
                _update_job(
                    conn, job_id,
                    current_step=f"Translated {i}/{total}",
                    progress_percent=progress,
                )
            except TranslationError:
                logger.exception("Translation failed for chapter %s", chapter_id)
                conn.execute(
                    "UPDATE chapters SET status = 'error' WHERE id = ?",
                    (chapter_id,),
                )
                conn.commit()

        _update_job(
            conn, job_id,
            status="completed",
            current_step=f"Done — {total} chapters translated",
            progress_percent=100,
        )
        logger.info("Pipeline complete for novel %s: %d chapters", novel_id, total)
        return {"job_id": job_id, "status": "completed", "chapters": total}

    except Exception as e:
        logger.exception("Pipeline failed for novel %s", novel_id)
        try:
            _update_job(
                conn, job_id,
                status="failed",
                error_message=str(e),
            )
        except Exception:
            logger.exception("Failed to update error status in DB")
        raise
    finally:
        conn.close()


def _scrape_and_store(
    conn: sqlite3.Connection,
    job_id: str,
    novel_id: str,
    start_url: str,
) -> list[str]:
    """
    Run the async scraper and store each chapter in the DB.
    Returns list of chapter IDs in order.
    """
    from app.pipeline.scraper import scrape_novel

    chapters = asyncio.run(scrape_novel(start_url, novel_id))

    chapter_ids = []
    for ch in chapters:
        chapter_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO chapters (id, novel_id, chapter_number, title, source_url, chinese_text, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'scraped')",
            (
                chapter_id,
                novel_id,
                ch["chapter_number"],
                ch["title"],
                ch["source_url"],
                ch["chinese_text"],
            ),
        )
        chapter_ids.append(chapter_id)

    # Update novel with total chapter count
    conn.execute(
        "UPDATE novels SET total_chapters = ?, status = 'scraped' WHERE id = ?",
        (len(chapters), novel_id),
    )
    _update_job(
        conn, job_id,
        current_step=f"Scraped {len(chapters)} chapters",
    )

    conn.commit()
    logger.info("Stored %d scraped chapters for novel %s", len(chapters), novel_id)
    return chapter_ids


@celery_app.task(bind=True)
def scrape_novel_task(self, job_id: str, novel_id: str, start_url: str):
    """Scrape-only task (no translation)."""
    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        _update_job(conn, job_id, status="running", current_step="Scraping chapters")
        chapter_ids = _scrape_and_store(conn, job_id, novel_id, start_url)
        _update_job(
            conn, job_id,
            status="completed",
            current_step=f"Scraped {len(chapter_ids)} chapters",
            progress_percent=100,
        )
        return {"job_id": job_id, "status": "completed", "chapters": len(chapter_ids)}
    except Exception as e:
        logger.exception("Scrape failed for novel %s", novel_id)
        try:
            _update_job(conn, job_id, status="failed", error_message=str(e))
        except Exception:
            logger.exception("Failed to update error status in DB")
        raise
    finally:
        conn.close()


@celery_app.task(bind=True)
def translate_chapter_task(self, job_id: str, chapter_id: str):
    """Translate a single chapter."""
    from app.pipeline.translator import get_translator, TranslationError
    from app.utils.term_dictionary import load_dictionary

    logger.info("Starting translation for chapter %s (job %s)", chapter_id, job_id)

    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT id, novel_id, chinese_text, chapter_number "
            "FROM chapters WHERE id = ?",
            (chapter_id,),
        ).fetchone()

        if row is None:
            raise TranslationError(f"Chapter {chapter_id} not found in database")

        if not row["chinese_text"]:
            raise TranslationError(
                f"Chapter {chapter_id} has no Chinese text to translate"
            )

        novel_id = row["novel_id"]
        chinese_text = row["chinese_text"]
        chapter_number = row["chapter_number"]

        # Load term dictionary (global + novel-specific, merged)
        term_dict = load_dictionary(novel_id)

        # Translate
        translator = get_translator()
        logger.info(
            "Translating chapter %d of novel %s (%d chars)",
            chapter_number,
            novel_id,
            len(chinese_text),
        )
        english_text = translator.translate_chapter(chinese_text, term_dict)

        # Store result
        conn.execute(
            "UPDATE chapters SET english_text = ?, status = 'translated' WHERE id = ?",
            (english_text, chapter_id),
        )
        conn.execute(
            "UPDATE jobs SET current_step = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (f"Translated chapter {chapter_number}", job_id),
        )
        conn.commit()

        logger.info("Successfully translated chapter %s", chapter_id)
        return {"chapter_id": chapter_id, "status": "translated"}

    except Exception as e:
        logger.exception("Translation failed for chapter %s", chapter_id)
        try:
            conn.execute(
                "UPDATE chapters SET status = 'error' WHERE id = ?", (chapter_id,)
            )
            conn.execute(
                "UPDATE jobs SET status = 'failed', error_message = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (str(e), job_id),
            )
            conn.commit()
        except Exception:
            logger.exception("Failed to update error status in DB")
        raise
    finally:
        conn.close()


@celery_app.task(bind=True)
def generate_audio_task(self, job_id: str, chapter_id: str):
    """Generate TTS audio for a single chapter."""
    # TODO: Implement TTS pipeline step
    raise NotImplementedError
