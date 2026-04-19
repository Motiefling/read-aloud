"""
Server-side scrape worker.

Runs scraping as an async background task on the FastAPI server,
keeping the Celery GPU worker dedicated to translate + TTS.
"""

import logging
import uuid

import aiosqlite

from app.config import get_database_path, get_data_dir
from app.queue_signal import notify_queue_changed

logger = logging.getLogger(__name__)

DATABASE_PATH = str(get_database_path())


async def scrape_and_store(
    novel_id: str,
    job_id: str,
    start_url: str,
    max_chapters: int | None = None,
    start_number: int = 1,
    filter_after: int | None = None,
):
    """
    Scrape chapters and store them in the database.

    This runs on the FastAPI server as a background task. On completion,
    it sets the novel's queue_status to 'queued' and wakes the dispatcher.

    Args:
        novel_id: The novel to scrape chapters for.
        job_id: The job tracking this scrape operation.
        start_url: URL to start scraping from (chapter or TOC URL).
        max_chapters: Optional cap on chapters to scrape.
        start_number: Chapter number to start counting from.
        filter_after: If set, skip chapters with chapter_number <= this value
                      (used when continuing from the last completed chapter).
    """
    from app.pipeline.scraper import scrape_novel, scrape_cover_image, scrape_novel_title

    logger.info(
        "Scrape started for novel %s from %s (max_chapters=%s, start_number=%d)",
        novel_id, start_url, max_chapters, start_number,
    )

    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row

    try:
        await _update_job(db, job_id, status="running", current_step="Scraping chapters")

        # Scrape cover image (best-effort)
        try:
            logger.info("Scraping cover image for novel %s...", novel_id)
            cover_path = await scrape_cover_image(start_url, novel_id)
            if cover_path:
                logger.info("Cover image saved: %s", cover_path)
                await db.execute(
                    "UPDATE novels SET cover_image_path = ? WHERE id = ?",
                    (cover_path, novel_id),
                )
                await db.commit()
            else:
                logger.info("No cover image found for novel %s", novel_id)
        except Exception as e:
            logger.warning("Cover image scraping failed (non-fatal): %s", e)

        # Auto-populate title if blank
        try:
            cursor = await db.execute(
                "SELECT title FROM novels WHERE id = ?", (novel_id,),
            )
            row = await cursor.fetchone()
            if row and row["title"] in ("Untitled Novel", ""):
                logger.info("Scraping novel title from %s...", start_url)
                chinese_title = await scrape_novel_title(start_url)
                if chinese_title:
                    logger.info("Scraped novel title: %s", chinese_title)
                    await db.execute(
                        "UPDATE novels SET title = ? WHERE id = ?",
                        (chinese_title, novel_id),
                    )
                    await db.commit()
                else:
                    logger.info("No title found on page for novel %s", novel_id)
        except Exception as e:
            logger.warning("Novel title auto-detection failed (non-fatal): %s", e)

        # Check for cancellation
        async def _cancel_check():
            cursor = await db.execute(
                "SELECT queue_status FROM novels WHERE id = ?", (novel_id,),
            )
            row = await cursor.fetchone()
            return row is None or row["queue_status"] is None

        scraped_count = 0

        async def _on_chapter(_chapter_number, chapter_dict):
            nonlocal scraped_count

            ch_num = chapter_dict["chapter_number"]

            # Skip chapters we already have (when continuing from last completed)
            if filter_after is not None and ch_num <= filter_after:
                logger.debug("Skipping chapter %d (filter_after=%d)", ch_num, filter_after)
                return

            chapter_id = str(uuid.uuid4())

            # Check if chapter already exists
            cursor = await db.execute(
                "SELECT id, status FROM chapters WHERE novel_id = ? AND chapter_number = ?",
                (novel_id, ch_num),
            )
            existing = await cursor.fetchone()

            from app.pipeline.chapter_storage import write_zh

            ch_title = chapter_dict.get("title", "")
            chinese_text = chapter_dict.get("chinese_text", "") or ""
            ch_text_len = len(chinese_text)

            # Write Chinese text to disk first — idempotent, safe to re-run
            write_zh(novel_id, ch_num, chinese_text)

            if existing:
                if existing["status"] in ("audio_ready", "translated"):
                    logger.debug("Skipping chapter %d — already %s", ch_num, existing["status"])
                    return  # Already processed, skip
                # Re-scrape into existing row
                await db.execute(
                    "UPDATE chapters SET title = ?, source_url = ?, status = 'scraped' "
                    "WHERE id = ?",
                    (chapter_dict.get("title"), chapter_dict.get("source_url"),
                     existing["id"]),
                )
                logger.info(
                    "Re-scraped chapter %d: %s (%d chars)",
                    ch_num, ch_title, ch_text_len,
                )
            else:
                await db.execute(
                    "INSERT INTO chapters (id, novel_id, chapter_number, title, "
                    "source_url, status) VALUES (?, ?, ?, ?, ?, 'scraped')",
                    (chapter_id, novel_id, ch_num, chapter_dict.get("title"),
                     chapter_dict.get("source_url")),
                )
                logger.info(
                    "Scraped chapter %d: %s (%d chars)",
                    ch_num, ch_title, ch_text_len,
                )

            scraped_count += 1
            await db.execute(
                "UPDATE novels SET total_chapters = total_chapters + 1, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (novel_id,),
            )
            await db.commit()

            # Update job progress
            if max_chapters:
                pct = (scraped_count / max_chapters) * 100
            else:
                pct = 0
            await _update_job(
                db, job_id,
                current_step=f"Scraped {scraped_count} chapters",
                progress_percent=pct,
            )

        # Run the scraper
        await scrape_novel(
            start_url,
            novel_id,
            max_chapters=max_chapters,
            on_chapter=_on_chapter,
            start_number=start_number,
            cancel_check=lambda: False,  # Cancellation handled via queue_status check in _on_chapter
        )

        # Scraping complete — mark novel as ready for processing
        await db.execute(
            "UPDATE novels SET queue_status = 'queued', "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (novel_id,),
        )
        await _update_job(
            db, job_id, status="completed",
            current_step=f"Scraped {scraped_count} chapters",
            progress_percent=100,
        )
        await db.commit()

        logger.info("Scrape complete for novel %s: %d chapters", novel_id, scraped_count)

        # Wake the dispatcher to start processing
        notify_queue_changed()

    except Exception as e:
        logger.exception("Scrape failed for novel %s: %s", novel_id, e)
        try:
            await db.execute(
                "UPDATE novels SET queue_status = NULL, queue_position = NULL, "
                "status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (novel_id,),
            )
            await _update_job(
                db, job_id, status="failed",
                error_message=str(e),
                current_step="Scraping failed",
            )
            await db.commit()
        except Exception:
            logger.exception("Failed to update DB after scrape error")
    finally:
        await db.close()


async def scrape_additional_chapters(
    novel_id: str,
    job_id: str,
    max_chapters: int | None = None,
    start_url: str | None = None,
):
    """
    Scrape additional chapters for an existing novel.

    Determines the start URL from the last completed chapter if not provided.
    """
    logger.info(
        "Scrape additional chapters for novel %s (max=%s, start_url=%s)",
        novel_id, max_chapters, start_url,
    )

    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row

    try:
        if start_url:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(chapter_number), 0) as mx "
                "FROM chapters WHERE novel_id = ?",
                (novel_id,),
            )
            row = await cursor.fetchone()
            start_number = row["mx"] + 1
            filter_after = None
        else:
            # Continue from last completed chapter
            cursor = await db.execute(
                "SELECT chapter_number, source_url FROM chapters "
                "WHERE novel_id = ? AND status = 'audio_ready' "
                "ORDER BY chapter_number DESC LIMIT 1",
                (novel_id,),
            )
            last_row = await cursor.fetchone()

            if last_row is None:
                # No completed chapters — fall back to novel's source URL
                cursor = await db.execute(
                    "SELECT source_url FROM novels WHERE id = ?", (novel_id,),
                )
                novel_row = await cursor.fetchone()
                if novel_row is None or not novel_row["source_url"]:
                    await _update_job(
                        db, job_id, status="failed",
                        error_message="No chapters and no source URL to scrape from",
                    )
                    await db.commit()
                    return
                start_url = novel_row["source_url"]
                start_number = 1
                filter_after = None
            else:
                start_url = last_row["source_url"]
                start_number = last_row["chapter_number"]
                filter_after = last_row["chapter_number"]
                # Scrape one extra since we start from the last completed chapter
                if max_chapters:
                    max_chapters += 1
    finally:
        await db.close()

    await scrape_and_store(
        novel_id, job_id, start_url,
        max_chapters=max_chapters,
        start_number=start_number,
        filter_after=filter_after,
    )


async def _update_job(db: aiosqlite.Connection, job_id: str, **fields):
    """Update job fields."""
    sets = ", ".join(f"{k} = ?" for k in fields)
    sets += ", updated_at = CURRENT_TIMESTAMP"
    await db.execute(
        f"UPDATE jobs SET {sets} WHERE id = ?", (*fields.values(), job_id)
    )
    await db.commit()
