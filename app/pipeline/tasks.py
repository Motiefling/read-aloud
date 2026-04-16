"""
Celery task definitions for the background processing pipeline.

The dispatcher loop processes chapters one at a time, checking queue order
between each chapter so the user can reorder novels on the fly.
"""

import logging
import re
import sqlite3
import time
import uuid

from celery import Celery

from app.config import settings, get_data_dir, get_database_path

logger = logging.getLogger(__name__)

_DATABASE_PATH = str(get_database_path())

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


def _is_job_cancelled(conn, job_id: str) -> bool:
    """Check if a job has been cancelled (by the API cancel endpoint)."""
    row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return row is not None and row["status"] == "cancelled"


def _novel_exists(conn, novel_id: str) -> bool:
    """Check if the novel still exists in the database (may have been deleted)."""
    row = conn.execute("SELECT id FROM novels WHERE id = ?", (novel_id,)).fetchone()
    return row is not None


def _cleanup_incomplete_chapters(conn, novel_id: str):
    """Remove non-audio_ready chapters and reset novel state to match."""
    conn.execute(
        "DELETE FROM chapters WHERE novel_id = ? AND status != 'audio_ready'",
        (novel_id,),
    )
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
        (novel_id,),
    ).fetchone()
    count = row["cnt"]
    status = "completed" if count > 0 else "pending"
    conn.execute(
        "UPDATE novels SET total_chapters = ?, processed_chapters = ?, "
        "status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (count, count, status, novel_id),
    )
    conn.commit()


def _translate_and_tts_chapter(
    conn: sqlite3.Connection,
    chapter_id: str,
    chapter_number: int,
    chinese_text: str,
    title: str | None,
    novel_id: str,
    translator,
    term_dict: dict,
    tts_engine,
    output_dir,
) -> bool:
    """Run translate → TTS for a single chapter. Updates the DB rows.

    Returns True if the chapter reached ``audio_ready``, False on error.
    """
    from app.pipeline.translator import TranslationError
    from app.pipeline.tts import generate_chapter_audio, TTSError
    from app.pipeline.audio_processing import get_audio_duration

    # --- Translate ---
    try:
        translate_start = time.time()
        english_text = translator.translate_chapter(chinese_text, term_dict)
        title_english = (
            translator.translate_title(title, term_dict) if title else None
        )
        translate_elapsed = time.time() - translate_start
        logger.info(
            "TIMING: Translation chapter #%d done in %.1fs (%d chars -> %d chars)",
            chapter_number, translate_elapsed,
            len(chinese_text), len(english_text),
        )
        conn.execute(
            "UPDATE chapters SET english_text = ?, title_english = ?, "
            "status = 'translated' WHERE id = ?",
            (english_text, title_english, chapter_id),
        )
        conn.commit()
    except TranslationError:
        logger.exception("Translation failed for chapter %s", chapter_id)
        conn.execute(
            "UPDATE chapters SET status = 'error' WHERE id = ?", (chapter_id,)
        )
        conn.commit()
        return False

    # --- Validate translation has English content ---
    if not re.search(r"[a-zA-Z]", english_text):
        logger.warning(
            "Chapter #%d has no English words after translation, skipping TTS: %r",
            chapter_number, english_text[:80],
        )
        conn.execute(
            "UPDATE chapters SET status = 'error' WHERE id = ?", (chapter_id,)
        )
        conn.commit()
        return False

    # --- TTS ---
    try:
        tts_start = time.time()
        audio_path = generate_chapter_audio(
            english_text=english_text,
            tts_engine=tts_engine,
            output_dir=output_dir,
            novel_id=novel_id,
            chapter_number=chapter_number,
        )
        relative_path = str(audio_path.relative_to(get_data_dir())).replace("\\", "/")
        duration = get_audio_duration(audio_path)
        file_size = audio_path.stat().st_size
        tts_elapsed = time.time() - tts_start
        logger.info(
            "TIMING: TTS chapter #%d done in %.1fs (%.1fs audio, %.1f MB)",
            chapter_number, tts_elapsed, duration, file_size / 1024 / 1024,
        )
        conn.execute(
            "UPDATE chapters SET audio_path = ?, audio_duration_seconds = ?, "
            "audio_file_size_bytes = ?, status = 'audio_ready' WHERE id = ?",
            (relative_path, duration, file_size, chapter_id),
        )
        conn.commit()
        return True
    except (TTSError, RuntimeError):
        logger.exception("Audio generation failed for chapter %s", chapter_id)
        conn.execute(
            "UPDATE chapters SET status = 'error' WHERE id = ?", (chapter_id,)
        )
        conn.commit()
        return False



# ---------------------------------------------------------------------------
# Dispatcher loop — processes chapters one at a time, respecting queue order
# ---------------------------------------------------------------------------

def _pick_next_chapter(conn):
    """Find the next chapter to process based on novel queue order.

    Returns a Row with (id, novel_id, chapter_number, chinese_text, title)
    or None if no work is available.
    """
    return conn.execute(
        "SELECT c.id, c.novel_id, c.chapter_number, c.chinese_text, c.title "
        "FROM chapters c "
        "JOIN novels n ON c.novel_id = n.id "
        "WHERE n.queue_position IS NOT NULL "
        "  AND n.queue_status IN ('queued', 'active') "
        "  AND c.status = 'scraped' "
        "ORDER BY n.queue_position ASC, c.chapter_number ASC "
        "LIMIT 1",
    ).fetchone()


def _find_or_create_processing_job(conn, novel_id):
    """Find the active processing job for a novel, or create one.

    Returns the job_id.
    """
    row = conn.execute(
        "SELECT id FROM jobs WHERE novel_id = ? AND job_type = 'processing' "
        "AND status IN ('queued', 'running') ORDER BY created_at DESC LIMIT 1",
        (novel_id,),
    ).fetchone()
    if row:
        return row["id"]

    job_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO jobs (id, novel_id, job_type, status, current_step) "
        "VALUES (?, ?, 'processing', 'running', 'Processing')",
        (job_id, novel_id),
    )
    conn.commit()
    return job_id


def _update_novel_progress(conn, novel_id):
    """Recount processed chapters and update the novel row."""
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM chapters "
        "WHERE novel_id = ? AND status = 'audio_ready'",
        (novel_id,),
    ).fetchone()
    conn.execute(
        "UPDATE novels SET processed_chapters = ?, updated_at = CURRENT_TIMESTAMP "
        "WHERE id = ?",
        (row["cnt"], novel_id),
    )
    conn.commit()


def _translate_novel_title(conn, novel_id, translator, term_dict):
    """Translate the novel title if it contains Chinese characters."""
    from app.utils.chinese_detect import contains_chinese

    row = conn.execute(
        "SELECT title FROM novels WHERE id = ?", (novel_id,),
    ).fetchone()
    if row is None or not row["title"]:
        return

    title = row["title"]
    if not contains_chinese(title):
        return  # Already in English or not Chinese

    try:
        translated = translator.translate_title(title, term_dict)
        if translated and translated != title:
            logger.info("Novel title translated: %s -> %s", title, translated)
            conn.execute(
                "UPDATE novels SET title = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (translated, novel_id),
            )
            conn.commit()
    except Exception:
        logger.exception("Failed to translate novel title for %s", novel_id)


def _check_novel_completion(conn, novel_id):
    """Check if all scraped chapters are processed. If so, remove from queue."""
    remaining = conn.execute(
        "SELECT COUNT(*) as cnt FROM chapters "
        "WHERE novel_id = ? AND status = 'scraped'",
        (novel_id,),
    ).fetchone()["cnt"]

    if remaining == 0:
        # All chapters processed — remove from queue
        total = conn.execute(
            "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
            (novel_id,),
        ).fetchone()["cnt"]
        ready = conn.execute(
            "SELECT COUNT(*) as cnt FROM chapters "
            "WHERE novel_id = ? AND status = 'audio_ready'",
            (novel_id,),
        ).fetchone()["cnt"]

        conn.execute(
            "UPDATE novels SET queue_position = NULL, queue_status = NULL, "
            "status = 'completed', total_chapters = ?, processed_chapters = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (total, ready, novel_id),
        )

        # Mark the processing job as completed
        conn.execute(
            "UPDATE jobs SET status = 'completed', "
            "current_step = 'Done', progress_percent = 100, "
            "updated_at = CURRENT_TIMESTAMP "
            "WHERE novel_id = ? AND job_type = 'processing' "
            "AND status IN ('queued', 'running')",
            (novel_id,),
        )
        conn.commit()
        logger.info("Novel %s fully processed, removed from queue", novel_id)


@celery_app.task(bind=True)
def dispatcher_loop(self):
    """
    Persistent worker loop. Loads GPU models once, then processes
    chapters one at a time, always respecting current queue order.

    Between each chapter, re-queries the database so that queue reordering
    takes effect immediately.
    """
    import redis as redis_lib
    from app.pipeline.translator import get_translator
    from app.pipeline.tts import get_tts_engine
    from app.utils.term_dictionary import load_dictionary

    logger.info("Dispatcher loop starting — loading GPU models")

    translator = get_translator()
    tts_engine = get_tts_engine()
    output_dir = get_data_dir() / "novels"

    # Term dict cache — reload only when the active novel changes
    _cached_novel_id = None
    _cached_term_dict = {}

    # Redis pub/sub for wake-up signals
    redis_conn = redis_lib.Redis.from_url(settings.celery.broker_url)
    pubsub = redis_conn.pubsub()
    pubsub.subscribe("queue:work_available")

    logger.info("Dispatcher loop ready — waiting for work")

    while True:
        conn = sqlite3.connect(_DATABASE_PATH)
        conn.row_factory = sqlite3.Row

        try:
            chapter = _pick_next_chapter(conn)

            if chapter is None:
                conn.close()
                # No work — wait for a signal or timeout
                try:
                    msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=30)
                except Exception:
                    time.sleep(5)
                continue

            novel_id = chapter["novel_id"]
            chapter_id = chapter["id"]
            chapter_number = chapter["chapter_number"]

            # Load term dictionary (cached per novel) and translate novel title
            if novel_id != _cached_novel_id:
                _cached_term_dict = load_dictionary(novel_id)
                _cached_novel_id = novel_id
                _translate_novel_title(conn, novel_id, translator, _cached_term_dict)

            # Mark novel as actively processing
            conn.execute(
                "UPDATE novels SET queue_status = 'active', status = 'processing', "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (novel_id,),
            )
            conn.commit()

            # Find or create the processing job for progress tracking
            job_id = _find_or_create_processing_job(conn, novel_id)

            # Count total and remaining for progress
            total_scraped = conn.execute(
                "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
                (novel_id,),
            ).fetchone()["cnt"]
            already_ready = conn.execute(
                "SELECT COUNT(*) as cnt FROM chapters "
                "WHERE novel_id = ? AND status = 'audio_ready'",
                (novel_id,),
            ).fetchone()["cnt"]

            _update_job(
                conn, job_id, status="running",
                current_step=f"Processing chapter {chapter_number} ({already_ready}/{total_scraped})",
                progress_percent=(already_ready / total_scraped * 100) if total_scraped else 0,
            )

            logger.info(
                "Dispatcher: processing novel %s chapter #%d (%d/%d ready)",
                novel_id, chapter_number, already_ready, total_scraped,
            )

            ch_start = time.time()
            success = _translate_and_tts_chapter(
                conn, chapter_id, chapter_number,
                chapter["chinese_text"], chapter["title"],
                novel_id, translator, _cached_term_dict, tts_engine, output_dir,
            )

            if success:
                _update_novel_progress(conn, novel_id)
                logger.info(
                    "TIMING: Chapter #%d done in %.1fs",
                    chapter_number, time.time() - ch_start,
                )

            # Check if this novel is fully done
            _check_novel_completion(conn, novel_id)

        except Exception:
            logger.exception("Dispatcher loop error — will retry in 5s")
            time.sleep(5)
        finally:
            try:
                conn.close()
            except Exception:
                pass


# Auto-start the dispatcher when the Celery worker is ready
from celery.signals import worker_ready  # noqa: E402


@worker_ready.connect
def _on_worker_ready(**kwargs):
    logger.info("Worker ready — starting dispatcher loop")
    dispatcher_loop.delay()
