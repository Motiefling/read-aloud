"""
Celery task definitions for the background processing pipeline.

Orchestrates the full pipeline: scrape -> translate -> TTS for each novel.
"""

import asyncio
import logging
import sqlite3
import time
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
def process_novel(self, job_id: str, novel_id: str, start_url: str, max_chapters: int | None = None):
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
        pipeline_start = time.time()
        _update_job(conn, job_id, status="running", current_step="Scraping chapters")

        # --- Step 1: Scrape ---
        scrape_start = time.time()
        chapter_ids = _scrape_and_store(conn, job_id, novel_id, start_url, max_chapters=max_chapters)
        scrape_elapsed = time.time() - scrape_start
        logger.info(
            "TIMING: Scraping completed in %.1fs (%d chapters, %.1fs/chapter)",
            scrape_elapsed, len(chapter_ids),
            scrape_elapsed / len(chapter_ids) if chapter_ids else 0,
        )

        if not chapter_ids:
            _update_job(conn, job_id, status="completed", current_step="No chapters found")
            return {"job_id": job_id, "status": "completed", "chapters": 0}

        # --- Step 2: Translate each chapter ---
        total = len(chapter_ids)
        _update_job(conn, job_id, current_step=f"Translating 0/{total}")

        from app.pipeline.translator import get_translator, TranslationError
        from app.utils.term_dictionary import load_dictionary

        translate_start = time.time()
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

            ch_start = time.time()
            logger.info(
                "Translating chapter %d/%d (chapter #%d, %d chars)",
                i, total, row["chapter_number"], len(row["chinese_text"]),
            )
            try:
                english_text = translator.translate_chapter(
                    row["chinese_text"], term_dict
                )
                ch_elapsed = time.time() - ch_start
                logger.info(
                    "TIMING: Translation chapter #%d done in %.1fs (%d chars -> %d chars)",
                    row["chapter_number"], ch_elapsed,
                    len(row["chinese_text"]), len(english_text),
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

        translate_elapsed = time.time() - translate_start
        logger.info(
            "TIMING: All translation completed in %.1fs (%.1fs/chapter avg)",
            translate_elapsed, translate_elapsed / total,
        )

        # --- Step 3: Generate audio for each chapter ---
        _update_job(conn, job_id, current_step=f"Generating audio 0/{total}")

        from app.pipeline.tts import get_tts_engine, generate_chapter_audio, TTSError
        from app.pipeline.audio_processing import get_audio_duration

        tts_start = time.time()
        tts_engine = get_tts_engine()
        output_dir = BASE_DIR / settings.server.data_dir / "novels"

        for i, chapter_id in enumerate(chapter_ids, 1):
            row = conn.execute(
                "SELECT english_text, chapter_number FROM chapters WHERE id = ?",
                (chapter_id,),
            ).fetchone()

            if not row or not row["english_text"]:
                logger.warning("Chapter %s has no English text, skipping audio", chapter_id)
                continue

            ch_start = time.time()
            logger.info(
                "Generating audio %d/%d (chapter #%d, %d chars)",
                i, total, row["chapter_number"], len(row["english_text"]),
            )
            try:
                audio_path = generate_chapter_audio(
                    english_text=row["english_text"],
                    tts_engine=tts_engine,
                    output_dir=output_dir,
                    novel_id=novel_id,
                    chapter_number=row["chapter_number"],
                )
                relative_path = str(audio_path.relative_to(BASE_DIR))
                duration = get_audio_duration(audio_path)
                file_size = audio_path.stat().st_size
                ch_elapsed = time.time() - ch_start
                logger.info(
                    "TIMING: TTS chapter #%d done in %.1fs (%.1fs audio, %.1f MB)",
                    row["chapter_number"], ch_elapsed, duration, file_size / 1024 / 1024,
                )
                conn.execute(
                    "UPDATE chapters SET audio_path = ?, audio_duration_seconds = ?, "
                    "audio_file_size_bytes = ?, status = 'audio_ready' WHERE id = ?",
                    (relative_path, duration, file_size, chapter_id),
                )
                progress = (i / total) * 100
                _update_job(
                    conn, job_id,
                    current_step=f"Audio {i}/{total}",
                    progress_percent=progress,
                )
            except (TTSError, RuntimeError):
                logger.exception("Audio generation failed for chapter %s", chapter_id)
                conn.execute(
                    "UPDATE chapters SET status = 'error' WHERE id = ?",
                    (chapter_id,),
                )
                conn.commit()

        tts_elapsed = time.time() - tts_start
        pipeline_elapsed = time.time() - pipeline_start
        logger.info(
            "TIMING: All TTS completed in %.1fs (%.1fs/chapter avg)",
            tts_elapsed, tts_elapsed / total,
        )
        logger.info(
            "TIMING: Full pipeline completed in %.1fs "
            "(scrape: %.1fs, translate: %.1fs, TTS: %.1fs)",
            pipeline_elapsed, scrape_elapsed, translate_elapsed, tts_elapsed,
        )

        # Mark novel as completed
        conn.execute(
            "UPDATE novels SET status = 'completed', processed_chapters = ? WHERE id = ?",
            (total, novel_id),
        )
        _update_job(
            conn, job_id,
            status="completed",
            current_step=f"Done — {total} chapters processed",
            progress_percent=100,
        )
        conn.commit()
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
    max_chapters: int | None = None,
) -> list[str]:
    """
    Run the async scraper and store each chapter in the DB.
    Returns list of chapter IDs in order.
    """
    from app.pipeline.scraper import scrape_novel

    chapters = asyncio.run(scrape_novel(start_url, novel_id, max_chapters=max_chapters))

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
        output_dir = BASE_DIR / settings.server.data_dir / "novels"

        audio_path = generate_chapter_audio(
            english_text=row["english_text"],
            tts_engine=tts_engine,
            output_dir=output_dir,
            novel_id=novel_id,
            chapter_number=chapter_number,
        )

        relative_path = str(audio_path.relative_to(BASE_DIR))
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
def add_chapters_task(self, job_id: str, novel_id: str, max_chapters: int | None = None):
    """Add more chapters to an existing novel. Wrapper around update_novel."""
    return update_novel(self, job_id, novel_id, max_chapters=max_chapters)


@celery_app.task(bind=True)
def update_novel(self, job_id: str, novel_id: str, max_chapters: int | None = None):
    """
    Check for new chapters of an existing novel and process only the new ones.

    Finds the last chapter's source URL, scrapes forward from there,
    then translates and generates TTS for any new chapters found.
    Optionally limited to max_chapters new chapters.
    """
    from app.pipeline.scraper import scrape_novel, ScrapingError
    from app.pipeline.translator import get_translator, TranslationError
    from app.pipeline.tts import get_tts_engine, generate_chapter_audio, TTSError
    from app.pipeline.audio_processing import get_audio_duration
    from app.utils.term_dictionary import load_dictionary

    conn = sqlite3.connect(_DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    try:
        pipeline_start = time.time()
        _update_job(conn, job_id, status="running", current_step="Checking for new chapters")

        # Find the last chapter we have
        last_row = conn.execute(
            "SELECT chapter_number, source_url FROM chapters "
            "WHERE novel_id = ? ORDER BY chapter_number DESC LIMIT 1",
            (novel_id,),
        ).fetchone()

        if last_row is None:
            _update_job(conn, job_id, status="failed",
                        error_message="No existing chapters found — use full pipeline instead")
            return {"job_id": job_id, "status": "failed"}

        last_chapter_num = last_row["chapter_number"]
        last_url = last_row["source_url"]
        logger.info(
            "Update check: last chapter is #%d (%s)",
            last_chapter_num, last_url,
        )

        # Scrape starting from the last known chapter
        # The scraper will return it plus any new ones after it
        # Add 1 to max_chapters because the first result is the last known chapter (skipped)
        scrape_limit = max_chapters + 1 if max_chapters else None
        scrape_start = time.time()
        all_chapters = asyncio.run(scrape_novel(last_url, novel_id, max_chapters=scrape_limit))
        scrape_elapsed = time.time() - scrape_start

        # Skip chapters we already have (first result is the last known chapter)
        new_chapters = [ch for ch in all_chapters if ch["chapter_number"] > 1]
        # Re-number: the scraper numbers from 1, but we need to continue from last_chapter_num
        for i, ch in enumerate(new_chapters):
            ch["chapter_number"] = last_chapter_num + i + 1

        if not new_chapters:
            _update_job(conn, job_id, status="completed",
                        current_step="No new chapters found", progress_percent=100)
            logger.info("TIMING: Update check completed in %.1fs — no new chapters", scrape_elapsed)
            return {"job_id": job_id, "status": "completed", "new_chapters": 0}

        logger.info(
            "TIMING: Found %d new chapters in %.1fs",
            len(new_chapters), scrape_elapsed,
        )

        # Store new chapters
        new_chapter_ids = []
        for ch in new_chapters:
            chapter_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO chapters (id, novel_id, chapter_number, title, source_url, chinese_text, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'scraped')",
                (chapter_id, novel_id, ch["chapter_number"], ch["title"],
                 ch["source_url"], ch["chinese_text"]),
            )
            new_chapter_ids.append(chapter_id)

        total_now = last_chapter_num + len(new_chapters)
        conn.execute(
            "UPDATE novels SET total_chapters = ?, status = 'processing' WHERE id = ?",
            (total_now, novel_id),
        )
        conn.commit()

        total_new = len(new_chapter_ids)
        _update_job(conn, job_id,
                    current_step=f"Translating new chapters 0/{total_new}")

        # Translate new chapters
        translate_start = time.time()
        translator = get_translator()
        term_dict = load_dictionary(novel_id)

        for i, chapter_id in enumerate(new_chapter_ids, 1):
            row = conn.execute(
                "SELECT chinese_text, chapter_number FROM chapters WHERE id = ?",
                (chapter_id,),
            ).fetchone()
            if not row or not row["chinese_text"]:
                continue

            ch_start = time.time()
            try:
                english_text = translator.translate_chapter(
                    row["chinese_text"], term_dict)
                ch_elapsed = time.time() - ch_start
                logger.info(
                    "TIMING: Translation chapter #%d done in %.1fs",
                    row["chapter_number"], ch_elapsed,
                )
                conn.execute(
                    "UPDATE chapters SET english_text = ?, status = 'translated' WHERE id = ?",
                    (english_text, chapter_id),
                )
                _update_job(conn, job_id,
                            current_step=f"Translated new {i}/{total_new}",
                            progress_percent=(i / total_new) * 50)
            except TranslationError:
                logger.exception("Translation failed for chapter %s", chapter_id)
                conn.execute(
                    "UPDATE chapters SET status = 'error' WHERE id = ?",
                    (chapter_id,),
                )
            conn.commit()

        translate_elapsed = time.time() - translate_start

        # TTS for new chapters
        tts_start = time.time()
        tts_engine = get_tts_engine()
        output_dir = BASE_DIR / settings.server.data_dir / "novels"

        for i, chapter_id in enumerate(new_chapter_ids, 1):
            row = conn.execute(
                "SELECT english_text, chapter_number FROM chapters WHERE id = ?",
                (chapter_id,),
            ).fetchone()
            if not row or not row["english_text"]:
                continue

            ch_start = time.time()
            try:
                audio_path = generate_chapter_audio(
                    english_text=row["english_text"],
                    tts_engine=tts_engine,
                    output_dir=output_dir,
                    novel_id=novel_id,
                    chapter_number=row["chapter_number"],
                )
                relative_path = str(audio_path.relative_to(BASE_DIR))
                duration = get_audio_duration(audio_path)
                file_size = audio_path.stat().st_size
                ch_elapsed = time.time() - ch_start
                logger.info(
                    "TIMING: TTS chapter #%d done in %.1fs (%.1fs audio)",
                    row["chapter_number"], ch_elapsed, duration,
                )
                conn.execute(
                    "UPDATE chapters SET audio_path = ?, audio_duration_seconds = ?, "
                    "audio_file_size_bytes = ?, status = 'audio_ready' WHERE id = ?",
                    (relative_path, duration, file_size, chapter_id),
                )
                _update_job(conn, job_id,
                            current_step=f"Audio new {i}/{total_new}",
                            progress_percent=50 + (i / total_new) * 50)
            except (TTSError, RuntimeError):
                logger.exception("Audio generation failed for chapter %s", chapter_id)
                conn.execute(
                    "UPDATE chapters SET status = 'error' WHERE id = ?",
                    (chapter_id,),
                )
            conn.commit()

        tts_elapsed = time.time() - tts_start
        pipeline_elapsed = time.time() - pipeline_start

        conn.execute(
            "UPDATE novels SET status = 'completed', total_chapters = ?, "
            "processed_chapters = ? WHERE id = ?",
            (total_now, total_now, novel_id),
        )
        _update_job(conn, job_id, status="completed",
                    current_step=f"Done — {total_new} new chapters added",
                    progress_percent=100)
        conn.commit()

        logger.info(
            "TIMING: Update completed in %.1fs "
            "(scrape: %.1fs, translate: %.1fs, TTS: %.1fs) — %d new chapters",
            pipeline_elapsed, scrape_elapsed, translate_elapsed, tts_elapsed, total_new,
        )
        return {"job_id": job_id, "status": "completed", "new_chapters": total_new}

    except Exception as e:
        logger.exception("Update failed for novel %s", novel_id)
        try:
            _update_job(conn, job_id, status="failed", error_message=str(e))
        except Exception:
            logger.exception("Failed to update error status in DB")
        raise
    finally:
        conn.close()
