"""
Celery task definitions for the background processing pipeline.

Orchestrates the full pipeline: scrape -> translate -> TTS for each novel.
"""

import asyncio
import logging
import re
import sqlite3
import threading
import time
import uuid
from queue import Queue, Empty

from celery import Celery

from app.config import settings, BASE_DIR, get_data_dir, get_database_path

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


# Sentinel value to signal that the scraper thread has finished.
_SENTINEL = object()


def _scrape_in_thread(
    q: Queue,
    start_url: str,
    novel_id: str,
    max_chapters: int | None,
    start_number: int,
    cancel_check,
):
    """Run the async scraper in a background thread, pushing chapters to a queue.

    Each scraped chapter dict is put into *q* as it arrives.
    On completion, ``_SENTINEL`` is put.  On error, the exception is put
    followed by ``_SENTINEL``.
    """

    async def _run():
        from app.pipeline.scraper import scrape_novel

        async def on_chapter(_chapter_number, chapter_dict):
            q.put(chapter_dict)

        try:
            await scrape_novel(
                start_url,
                novel_id,
                max_chapters=max_chapters,
                on_chapter=on_chapter,
                start_number=start_number,
                cancel_check=cancel_check,
            )
        except Exception as exc:
            q.put(exc)
        finally:
            q.put(_SENTINEL)

    asyncio.run(_run())


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


@celery_app.task(bind=True)
def process_novel(self, job_id: str, novel_id: str, start_url: str, max_chapters: int | None = None):
    """
    Master task: pipelined processing for a novel.

    Runs scraping in a background thread while the main thread processes
    each chapter through translate → TTS as soon as it's scraped.
    Chapters become ``audio_ready`` (and visible in the UI) one at a time
    instead of waiting for the entire novel to finish.
    """
    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        if not _novel_exists(conn, novel_id):
            logger.info("Novel %s no longer exists, aborting job %s", novel_id, job_id)
            _update_job(conn, job_id, status="failed",
                        error_message="Novel was deleted")
            return {"job_id": job_id, "status": "failed", "reason": "novel_deleted"}

        pipeline_start = time.time()
        _update_job(conn, job_id, status="running", current_step="Starting pipeline")

        # Scrape cover image and novel title (best-effort)
        try:
            from app.pipeline.scraper import scrape_cover_image, scrape_novel_title
            cover_path = asyncio.run(scrape_cover_image(start_url, novel_id))
            if cover_path:
                conn.execute(
                    "UPDATE novels SET cover_image_path = ? WHERE id = ?",
                    (cover_path, novel_id),
                )
                conn.commit()
        except Exception as e:
            logger.warning("Cover image scraping failed (non-fatal): %s", e)

        # Auto-populate title if user left it blank
        try:
            row = conn.execute(
                "SELECT title FROM novels WHERE id = ?", (novel_id,),
            ).fetchone()
            if row and row["title"] in ("Untitled Novel", ""):
                chinese_title = asyncio.run(scrape_novel_title(start_url))
                if chinese_title:
                    from app.pipeline.translator import get_translator
                    translator = get_translator()
                    english_title = translator.translate_title(chinese_title)
                    if english_title:
                        logger.info("Auto-set novel title: %s -> %s", chinese_title, english_title)
                        conn.execute(
                            "UPDATE novels SET title = ? WHERE id = ?",
                            (english_title, novel_id),
                        )
                        conn.commit()
        except Exception as e:
            logger.warning("Novel title auto-detection failed (non-fatal): %s", e)

        # Load GPU models and resources up front
        from app.pipeline.translator import get_translator
        from app.pipeline.tts import get_tts_engine
        from app.utils.term_dictionary import load_dictionary

        translator = get_translator()
        tts_engine = get_tts_engine()
        term_dict = load_dictionary(novel_id)
        output_dir = get_data_dir() / "novels"

        # Start scraper in a background thread — it pushes chapters to
        # the queue as they are scraped, overlapping with GPU work.
        # The cancel_check must open its own DB connection because SQLite
        # connections cannot be shared across threads.
        def _cancel_check_threadsafe():
            c = sqlite3.connect(_DATABASE_PATH)
            c.row_factory = sqlite3.Row
            try:
                return _is_job_cancelled(c, job_id)
            finally:
                c.close()

        q: Queue = Queue()
        scrape_thread = threading.Thread(
            target=_scrape_in_thread,
            args=(q, start_url, novel_id, max_chapters, 1,
                  _cancel_check_threadsafe),
            daemon=True,
        )
        scrape_thread.start()

        scraped_count = 0
        ready_count = 0

        while True:
            # Periodic cancellation check while waiting for the next chapter
            if _is_job_cancelled(conn, job_id):
                logger.info("Job %s cancelled, cleaning up", job_id)
                _cleanup_incomplete_chapters(conn, novel_id)
                scrape_thread.join(timeout=15)
                return {"job_id": job_id, "status": "cancelled"}

            try:
                item = q.get(timeout=5)
            except Empty:
                continue  # loop back to cancel check

            # Scraping finished
            if item is _SENTINEL:
                break

            # Scraping error
            if isinstance(item, Exception):
                if scraped_count == 0:
                    raise item
                logger.error("Scraping error after %d chapters: %s", scraped_count, item)
                break

            # --- Store scraped chapter in DB ---
            ch = item
            chapter_id = str(uuid.uuid4())

            # Check if this chapter already exists (e.g. from an interrupted job)
            existing = conn.execute(
                "SELECT id, status FROM chapters "
                "WHERE novel_id = ? AND chapter_number = ?",
                (novel_id, ch["chapter_number"]),
            ).fetchone()

            if existing:
                # Chapter already in DB — skip if already processed
                if existing[1] in ("audio_ready", "translated"):
                    logger.info(
                        "Chapter #%d already %s — skipping",
                        ch["chapter_number"], existing[1],
                    )
                    scraped_count += 1
                    if existing[1] == "audio_ready":
                        ready_count += 1
                    continue
                # Re-use existing row for retry (was scraped/pending/error)
                chapter_id = existing[0]
                conn.execute(
                    "UPDATE chapters SET chinese_text = ?, title = ?, "
                    "source_url = ?, status = 'scraped' WHERE id = ?",
                    (ch["chinese_text"], ch["title"], ch["source_url"], chapter_id),
                )
            else:
                conn.execute(
                    "INSERT INTO chapters "
                    "(id, novel_id, chapter_number, title, source_url, chinese_text, status) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'scraped')",
                    (chapter_id, novel_id, ch["chapter_number"], ch["title"],
                     ch["source_url"], ch["chinese_text"]),
                )

            scraped_count += 1
            conn.execute(
                "UPDATE novels SET total_chapters = ?, status = 'processing' WHERE id = ?",
                (scraped_count, novel_id),
            )
            conn.commit()

            # --- Translate + TTS this chapter immediately ---
            ch_start = time.time()
            _update_job(
                conn, job_id,
                current_step=f"Processing chapter {ch['chapter_number']}",
            )
            logger.info(
                "Processing chapter #%d (%d chars)",
                ch["chapter_number"], len(ch["chinese_text"]),
            )

            success = _translate_and_tts_chapter(
                conn, chapter_id, ch["chapter_number"],
                ch["chinese_text"], ch["title"],
                novel_id, translator, term_dict, tts_engine, output_dir,
            )

            if success:
                ready_count += 1
                conn.execute(
                    "UPDATE novels SET processed_chapters = ? WHERE id = ?",
                    (ready_count, novel_id),
                )
                conn.commit()

            ch_elapsed = time.time() - ch_start
            logger.info(
                "TIMING: Chapter #%d pipeline done in %.1fs (%s)",
                ch["chapter_number"], ch_elapsed,
                "ready" if success else "error",
            )

            _update_job(
                conn, job_id,
                current_step=f"{ready_count} chapters ready ({scraped_count} scraped)",
                progress_percent=(ready_count / scraped_count * 100) if scraped_count else 0,
            )

        scrape_thread.join(timeout=30)
        pipeline_elapsed = time.time() - pipeline_start

        if scraped_count == 0:
            _update_job(conn, job_id, status="completed",
                        current_step="No chapters found", progress_percent=100)
            return {"job_id": job_id, "status": "completed", "chapters": 0}

        # Finalize
        conn.execute(
            "UPDATE novels SET status = 'completed', total_chapters = ?, "
            "processed_chapters = ? WHERE id = ?",
            (scraped_count, ready_count, novel_id),
        )
        _update_job(
            conn, job_id,
            status="completed",
            current_step=f"Done — {ready_count}/{scraped_count} chapters ready",
            progress_percent=100,
        )
        conn.commit()

        logger.info(
            "TIMING: Full pipeline completed in %.1fs — %d/%d chapters ready",
            pipeline_elapsed, ready_count, scraped_count,
        )
        return {"job_id": job_id, "status": "completed", "chapters": ready_count}

    except Exception as e:
        logger.exception("Pipeline failed for novel %s", novel_id)
        try:
            _update_job(conn, job_id, status="failed", error_message=str(e))
            count = conn.execute(
                "SELECT COUNT(*) as cnt FROM chapters "
                "WHERE novel_id = ? AND status = 'audio_ready'",
                (novel_id,),
            ).fetchone()["cnt"]
            conn.execute(
                "UPDATE novels SET status = ?, processed_chapters = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                ("completed" if count > 0 else "failed", count, novel_id),
            )
            conn.commit()
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
    max_chapters: int | None = None,
) -> list[str]:
    """
    Run the async scraper and store each chapter in the DB.
    Returns list of chapter IDs in order.

    Used by scrape-only tasks (not the main pipeline).
    """
    from app.pipeline.scraper import scrape_novel

    chapters = asyncio.run(scrape_novel(
        start_url, novel_id, max_chapters=max_chapters,
        cancel_check=lambda: _is_job_cancelled(conn, job_id),
    ))

    if _is_job_cancelled(conn, job_id):
        return []

    chapter_ids = []
    for ch in chapters:
        chapter_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO chapters (id, novel_id, chapter_number, title, source_url, chinese_text, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'scraped')",
            (chapter_id, novel_id, ch["chapter_number"], ch["title"],
             ch["source_url"], ch["chinese_text"]),
        )
        chapter_ids.append(chapter_id)

    conn.execute(
        "UPDATE novels SET total_chapters = ?, status = 'scraped' WHERE id = ?",
        (len(chapters), novel_id),
    )
    _update_job(conn, job_id, current_step=f"Scraped {len(chapters)} chapters")
    conn.commit()
    logger.info("Stored %d scraped chapters for novel %s", len(chapters), novel_id)
    return chapter_ids


@celery_app.task(bind=True)
def scrape_novel_task(self, job_id: str, novel_id: str, start_url: str):
    """Scrape-only task (no translation)."""
    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        if not _novel_exists(conn, novel_id):
            logger.info("Novel %s no longer exists, aborting job %s", novel_id, job_id)
            _update_job(conn, job_id, status="failed",
                        error_message="Novel was deleted")
            return {"job_id": job_id, "status": "failed", "reason": "novel_deleted"}

        _update_job(conn, job_id, status="running", current_step="Scraping chapters")
        chapter_ids = _scrape_and_store(conn, job_id, novel_id, start_url)

        if _is_job_cancelled(conn, job_id):
            logger.info("Job %s cancelled during scraping", job_id)
            _cleanup_incomplete_chapters(conn, novel_id)
            return {"job_id": job_id, "status": "cancelled"}

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
            "SELECT id, novel_id, chinese_text, chapter_number, title "
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
        title_english = translator.translate_title(
            row["title"], term_dict
        ) if row["title"] else None

        # Store result
        conn.execute(
            "UPDATE chapters SET english_text = ?, title_english = ?, status = 'translated' WHERE id = ?",
            (english_text, title_english, chapter_id),
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
    from app.pipeline.tts import get_tts_engine, generate_chapter_audio, TTSError
    from app.pipeline.audio_processing import get_audio_duration

    logger.info("Generating audio for chapter %s (job %s)", chapter_id, job_id)

    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT id, novel_id, english_text, chapter_number "
            "FROM chapters WHERE id = ?",
            (chapter_id,),
        ).fetchone()

        if row is None:
            raise TTSError(f"Chapter {chapter_id} not found in database")
        if not row["english_text"]:
            raise TTSError(f"Chapter {chapter_id} has no English text for TTS")

        novel_id = row["novel_id"]
        chapter_number = row["chapter_number"]

        tts_engine = get_tts_engine()
        output_dir = get_data_dir() / "novels"

        audio_path = generate_chapter_audio(
            english_text=row["english_text"],
            tts_engine=tts_engine,
            output_dir=output_dir,
            novel_id=novel_id,
            chapter_number=chapter_number,
        )

        relative_path = str(audio_path.relative_to(get_data_dir())).replace("\\", "/")
        duration = get_audio_duration(audio_path)
        file_size = audio_path.stat().st_size
        conn.execute(
            "UPDATE chapters SET audio_path = ?, audio_duration_seconds = ?, "
            "audio_file_size_bytes = ?, status = 'audio_ready' WHERE id = ?",
            (relative_path, duration, file_size, chapter_id),
        )
        conn.execute(
            "UPDATE jobs SET current_step = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (f"Generated audio for chapter {chapter_number}", job_id),
        )
        conn.commit()

        logger.info("Successfully generated audio for chapter %s", chapter_id)
        return {
            "chapter_id": chapter_id,
            "status": "audio_ready",
            "audio_path": relative_path,
            "duration_seconds": duration,
        }

    except Exception as e:
        logger.exception("Audio generation failed for chapter %s", chapter_id)
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
def retry_chapter_task(self, job_id: str, novel_id: str, chapter_id: str):
    """Re-run translate + TTS for a single error chapter."""
    from app.pipeline.translator import get_translator
    from app.pipeline.tts import get_tts_engine
    from app.utils.term_dictionary import load_dictionary

    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        if not _novel_exists(conn, novel_id):
            _update_job(conn, job_id, status="failed",
                        error_message="Novel was deleted")
            return {"job_id": job_id, "status": "failed"}

        row = conn.execute(
            "SELECT id, chapter_number, chinese_text, title FROM chapters WHERE id = ?",
            (chapter_id,),
        ).fetchone()
        if row is None:
            _update_job(conn, job_id, status="failed",
                        error_message="Chapter not found")
            return {"job_id": job_id, "status": "failed"}

        _update_job(conn, job_id, status="running",
                    current_step=f"Retrying chapter {row['chapter_number']}")

        translator = get_translator()
        tts_engine = get_tts_engine()
        term_dict = load_dictionary(novel_id)
        output_dir = get_data_dir() / "novels"

        # Reset chapter status so it gets re-processed
        conn.execute(
            "UPDATE chapters SET status = 'scraped' WHERE id = ?", (chapter_id,)
        )
        conn.commit()

        success = _translate_and_tts_chapter(
            conn, chapter_id, row["chapter_number"],
            row["chinese_text"], row["title"],
            novel_id, translator, term_dict, tts_engine, output_dir,
        )

        if success:
            # Update novel processed count
            count = conn.execute(
                "SELECT COUNT(*) as cnt FROM chapters "
                "WHERE novel_id = ? AND status = 'audio_ready'",
                (novel_id,),
            ).fetchone()["cnt"]
            conn.execute(
                "UPDATE novels SET processed_chapters = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (count, novel_id),
            )
            _update_job(conn, job_id, status="completed",
                        current_step=f"Chapter {row['chapter_number']} ready",
                        progress_percent=100)
            conn.commit()
        else:
            _update_job(conn, job_id, status="failed",
                        error_message=f"Chapter {row['chapter_number']} failed again")

        return {"job_id": job_id, "status": "completed" if success else "failed"}

    except Exception as e:
        logger.exception("Retry failed for chapter %s", chapter_id)
        try:
            _update_job(conn, job_id, status="failed", error_message=str(e))
        except Exception:
            logger.exception("Failed to update error status in DB")
        raise
    finally:
        conn.close()


@celery_app.task(bind=True)
def add_chapters_task(self, job_id: str, novel_id: str, max_chapters: int | None = None, start_url: str | None = None):
    """Add more chapters to an existing novel. Wrapper around _process_chapters."""
    return _process_chapters(self, job_id, novel_id, max_chapters=max_chapters, start_url=start_url)


@celery_app.task(bind=True)
def check_updates_task(self, job_id: str, novel_id: str):
    """Lightweight check: see if the last chapter has a next-chapter link."""
    from app.pipeline.scraper import check_for_updates

    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        if not _novel_exists(conn, novel_id):
            logger.info("Novel %s no longer exists, aborting job %s", novel_id, job_id)
            _update_job(conn, job_id, status="failed",
                        error_message="Novel was deleted")
            return {"job_id": job_id, "status": "failed", "reason": "novel_deleted"}

        _update_job(conn, job_id, status="running", current_step="Checking for new chapters")

        # Find the last chapter URL
        last_row = conn.execute(
            "SELECT chapter_number, source_url FROM chapters "
            "WHERE novel_id = ? ORDER BY chapter_number DESC LIMIT 1",
            (novel_id,),
        ).fetchone()

        if last_row is None:
            _update_job(conn, job_id, status="failed",
                        error_message="No existing chapters found")
            return {"job_id": job_id, "status": "failed"}

        last_url = last_row["source_url"]

        # Just check if a next chapter link exists — no content scraping
        has_updates = asyncio.run(check_for_updates(last_url))

        if has_updates:
            step_text = "Updates available"
        else:
            step_text = "No new chapters found"

        _update_job(conn, job_id, status="completed",
                    current_step=step_text, progress_percent=100)

        logger.info("Update check for novel %s: %s", novel_id, step_text)
        return {"job_id": job_id, "status": "completed", "has_updates": has_updates}

    except Exception as e:
        logger.exception("Check updates failed for novel %s", novel_id)
        try:
            _update_job(conn, job_id, status="failed", error_message=str(e))
        except Exception:
            logger.exception("Failed to update error status in DB")
        raise
    finally:
        conn.close()


def _process_chapters(self, job_id: str, novel_id: str, max_chapters: int | None = None, start_url: str | None = None):
    """
    Process chapters for an existing novel (translate + TTS).

    If there are already-scraped chapters in the DB (from check_updates_task),
    processes those.  Otherwise, scrapes forward from the last completed chapter
    (or from start_url if provided) using the pipelined producer-consumer
    approach so chapters become ``audio_ready`` one at a time.
    """
    from app.pipeline.translator import get_translator
    from app.utils.term_dictionary import load_dictionary
    from app.pipeline.tts import get_tts_engine

    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        if not _novel_exists(conn, novel_id):
            logger.info("Novel %s no longer exists, aborting job %s", novel_id, job_id)
            _update_job(conn, job_id, status="failed",
                        error_message="Novel was deleted")
            return {"job_id": job_id, "status": "failed", "reason": "novel_deleted"}

        pipeline_start = time.time()
        _update_job(conn, job_id, status="running", current_step="Preparing")

        # Load GPU models up front
        translator = get_translator()
        tts_engine = get_tts_engine()
        term_dict = load_dictionary(novel_id)
        output_dir = get_data_dir() / "novels"

        # ------------------------------------------------------------------
        # Path A: pre-scraped chapters already in the DB
        # ------------------------------------------------------------------
        unprocessed_rows = conn.execute(
            "SELECT id, chapter_number, chinese_text, title, status FROM chapters "
            "WHERE novel_id = ? AND status IN ('scraped', 'translated') "
            "ORDER BY chapter_number",
            (novel_id,),
        ).fetchall()

        if unprocessed_rows:
            chapters_to_process = list(unprocessed_rows)
            if max_chapters:
                chapters_to_process = chapters_to_process[:max_chapters]
            total_new = len(chapters_to_process)
            logger.info("Found %d pre-scraped chapters to process", total_new)
            _update_job(conn, job_id, current_step=f"Processing {total_new} chapters")
            conn.execute(
                "UPDATE novels SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (novel_id,),
            )
            conn.commit()

            ready_count = 0
            for i, row in enumerate(chapters_to_process, 1):
                if _is_job_cancelled(conn, job_id):
                    logger.info("Job %s cancelled during processing", job_id)
                    _cleanup_incomplete_chapters(conn, novel_id)
                    return {"job_id": job_id, "status": "cancelled"}

                if not row["chinese_text"]:
                    continue
                if row["status"] == "audio_ready":
                    ready_count += 1
                    continue

                ch_start = time.time()
                _update_job(conn, job_id,
                            current_step=f"Processing chapter {row['chapter_number']} ({i}/{total_new})",
                            progress_percent=(i / total_new) * 100)

                success = _translate_and_tts_chapter(
                    conn, row["id"], row["chapter_number"],
                    row["chinese_text"], row["title"],
                    novel_id, translator, term_dict, tts_engine, output_dir,
                )
                if success:
                    ready_count += 1
                    conn.execute(
                        "UPDATE novels SET processed_chapters = ? WHERE id = ?",
                        (ready_count, novel_id),
                    )
                    conn.commit()

                logger.info("TIMING: Chapter #%d done in %.1fs (%s)",
                            row["chapter_number"], time.time() - ch_start,
                            "ready" if success else "error")

            # Finalize
            return _finalize_process_chapters(
                conn, job_id, novel_id, total_new, pipeline_start)

        # ------------------------------------------------------------------
        # Path B: need to scrape first — use pipelined producer-consumer
        # ------------------------------------------------------------------
        _update_job(conn, job_id, current_step="Scraping new chapters")

        # Determine the scrape start URL and chapter number
        scrape_url, start_number, filter_after = _resolve_scrape_start(
            conn, job_id, novel_id, start_url, max_chapters,
        )
        if scrape_url is None:
            # _resolve_scrape_start already updated the job status
            return {"job_id": job_id, "status": conn.execute(
                "SELECT status FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()["status"]}

        conn.execute(
            "UPDATE novels SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (novel_id,),
        )
        conn.commit()

        # Start scraper in background thread
        # When scraping from the last completed chapter (filter_after is set),
        # we scrape one extra because the starting chapter is already in the DB.
        scrape_limit = max_chapters
        if scrape_limit and filter_after is not None:
            scrape_limit += 1

        def _cancel_check_threadsafe():
            c = sqlite3.connect(_DATABASE_PATH)
            c.row_factory = sqlite3.Row
            try:
                return _is_job_cancelled(c, job_id)
            finally:
                c.close()

        q: Queue = Queue()
        scrape_thread = threading.Thread(
            target=_scrape_in_thread,
            args=(q, scrape_url, novel_id, scrape_limit, start_number,
                  _cancel_check_threadsafe),
            daemon=True,
        )
        scrape_thread.start()

        scraped_count = 0
        ready_count = 0

        while True:
            if _is_job_cancelled(conn, job_id):
                logger.info("Job %s cancelled, cleaning up", job_id)
                _cleanup_incomplete_chapters(conn, novel_id)
                scrape_thread.join(timeout=15)
                return {"job_id": job_id, "status": "cancelled"}

            try:
                item = q.get(timeout=5)
            except Empty:
                continue

            if item is _SENTINEL:
                break
            if isinstance(item, Exception):
                if scraped_count == 0:
                    raise item
                logger.error("Scraping error after %d chapters: %s", scraped_count, item)
                break

            ch = item

            # Filter out the starting chapter when scraping from last completed
            if filter_after is not None and ch["chapter_number"] <= filter_after:
                continue

            # Store scraped chapter
            chapter_id = str(uuid.uuid4())
            conn.execute(
                "INSERT OR IGNORE INTO chapters "
                "(id, novel_id, chapter_number, title, source_url, chinese_text, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'scraped')",
                (chapter_id, novel_id, ch["chapter_number"], ch["title"],
                 ch["source_url"], ch["chinese_text"]),
            )
            scraped_count += 1
            total_now = conn.execute(
                "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
                (novel_id,),
            ).fetchone()["cnt"]
            conn.execute(
                "UPDATE novels SET total_chapters = ? WHERE id = ?",
                (total_now, novel_id),
            )
            conn.commit()

            # Process through translate + TTS immediately
            ch_start = time.time()
            _update_job(conn, job_id,
                        current_step=f"Processing chapter {ch['chapter_number']}")

            success = _translate_and_tts_chapter(
                conn, chapter_id, ch["chapter_number"],
                ch["chinese_text"], ch["title"],
                novel_id, translator, term_dict, tts_engine, output_dir,
            )
            if success:
                ready_count += 1
                conn.execute(
                    "UPDATE novels SET processed_chapters = ? WHERE id = ?",
                    (ready_count, novel_id),
                )
                conn.commit()

            logger.info("TIMING: Chapter #%d done in %.1fs (%s)",
                        ch["chapter_number"], time.time() - ch_start,
                        "ready" if success else "error")
            _update_job(conn, job_id,
                        current_step=f"{ready_count} chapters ready ({scraped_count} scraped)",
                        progress_percent=(ready_count / scraped_count * 100) if scraped_count else 0)

        scrape_thread.join(timeout=30)

        if scraped_count == 0:
            _update_job(conn, job_id, status="completed",
                        current_step="No new chapters found", progress_percent=100)
            return {"job_id": job_id, "status": "completed", "new_chapters": 0}

        return _finalize_process_chapters(
            conn, job_id, novel_id, scraped_count, pipeline_start)

    except Exception as e:
        logger.exception("Processing failed for novel %s", novel_id)
        try:
            _update_job(conn, job_id, status="failed", error_message=str(e))
            count = conn.execute(
                "SELECT COUNT(*) as cnt FROM chapters "
                "WHERE novel_id = ? AND status = 'audio_ready'",
                (novel_id,),
            ).fetchone()["cnt"]
            conn.execute(
                "UPDATE novels SET status = ?, processed_chapters = ?, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                ("completed" if count > 0 else "failed", count, novel_id),
            )
            conn.commit()
        except Exception:
            logger.exception("Failed to update error status in DB")
        raise
    finally:
        conn.close()


def _resolve_scrape_start(conn, job_id, novel_id, start_url, max_chapters):
    """Determine where to start scraping for _process_chapters.

    Returns ``(scrape_url, start_number, filter_after)`` where
    *filter_after* is a chapter number to skip (the starting chapter that
    is already in the DB), or ``None`` if no filtering is needed.

    Returns ``(None, ...)`` if there is nothing to scrape (job is updated).
    """
    if start_url:
        existing_max = conn.execute(
            "SELECT COALESCE(MAX(chapter_number), 0) as mx FROM chapters WHERE novel_id = ?",
            (novel_id,),
        ).fetchone()["mx"]
        return start_url, existing_max + 1, None

    last_row = conn.execute(
        "SELECT chapter_number, source_url FROM chapters "
        "WHERE novel_id = ? AND status = 'audio_ready' "
        "ORDER BY chapter_number DESC LIMIT 1",
        (novel_id,),
    ).fetchone()

    if last_row is None:
        # No completed chapters — fall back to novel's original source URL
        novel_row = conn.execute(
            "SELECT source_url FROM novels WHERE id = ?",
            (novel_id,),
        ).fetchone()
        if novel_row is None or not novel_row["source_url"]:
            _update_job(conn, job_id, status="failed",
                        error_message="No existing chapters and no source URL to scrape from")
            return None, 0, None
        return novel_row["source_url"], 1, None

    # Scrape forward from last completed chapter (include it so the
    # scraper can follow its next-page link, then filter it out).
    last_num = last_row["chapter_number"]
    scrape_limit = max_chapters + 1 if max_chapters else None
    return last_row["source_url"], last_num, last_num


def _finalize_process_chapters(conn, job_id, novel_id, total_new, pipeline_start):
    """Update novel and job to 'completed' after _process_chapters finishes."""
    completed_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM chapters "
        "WHERE novel_id = ? AND status = 'audio_ready'",
        (novel_id,),
    ).fetchone()["cnt"]
    total_chapters = conn.execute(
        "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ?",
        (novel_id,),
    ).fetchone()["cnt"]

    conn.execute(
        "UPDATE novels SET status = 'completed', total_chapters = ?, "
        "processed_chapters = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (total_chapters, completed_count, novel_id),
    )
    _update_job(conn, job_id, status="completed",
                current_step=f"Done — {total_new} chapters processed",
                progress_percent=100)
    conn.commit()

    pipeline_elapsed = time.time() - pipeline_start
    logger.info(
        "TIMING: Processing completed in %.1fs — %d chapters",
        pipeline_elapsed, total_new,
    )
    return {"job_id": job_id, "status": "completed", "new_chapters": total_new}
