"""
Quick end-to-end test: scrape a few chapters and translate them.
Bypasses Celery/Redis — calls the pipeline functions directly.

Usage:
    python test_pipeline.py <chapter_url> [max_chapters]

Example:
    python test_pipeline.py "https://funs.me/mtext/2058/15620087.html" 2
"""

import asyncio
import logging
import sqlite3
import sys
import uuid

from app.config import settings, BASE_DIR
from app.database import DATABASE_PATH, SQL_CREATE_TABLES
from app.pipeline.chapter_storage import write_zh, write_en
from app.pipeline.scraper import scrape_novel
from app.pipeline.translator import get_translator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    start_url = sys.argv[1]
    max_chapters = int(sys.argv[2]) if len(sys.argv) > 2 else 2

    novel_id = str(uuid.uuid4())
    logger.info("Novel ID: %s", novel_id)
    logger.info("Start URL: %s", start_url)
    logger.info("Max chapters: %d", max_chapters)

    # --- Init DB ---
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SQL_CREATE_TABLES)

    # Create novel record
    conn.execute(
        "INSERT INTO novels (id, title, source_url, status) VALUES (?, ?, ?, 'pending')",
        (novel_id, "Test Novel", start_url),
    )
    conn.commit()

    # --- Step 1: Scrape ---
    logger.info("=== SCRAPING ===")
    chapters = asyncio.run(scrape_novel(start_url, novel_id, max_chapters=max_chapters))
    logger.info("Scraped %d chapters", len(chapters))

    # Store in DB + on disk
    chapter_ids = []
    for ch in chapters:
        chapter_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO chapters (id, novel_id, chapter_number, title, source_url, status) "
            "VALUES (?, ?, ?, ?, ?, 'scraped')",
            (chapter_id, novel_id, ch["chapter_number"], ch["title"], ch["source_url"]),
        )
        write_zh(novel_id, ch["chapter_number"], ch["chinese_text"])
        chapter_ids.append(chapter_id)
    conn.commit()

    for ch in chapters:
        print(f"\n  Chapter {ch['chapter_number']}: {ch['title']}")
        print(f"  URL: {ch['source_url']}")
        print(f"  Text length: {len(ch['chinese_text'])} chars")
        print(f"  Preview: {ch['chinese_text'][:100]}...")

    # --- Step 2: Translate ---
    logger.info("\n=== TRANSLATING ===")
    translator = get_translator()

    for i, (chapter_id, ch) in enumerate(zip(chapter_ids, chapters), 1):
        logger.info(
            "Translating chapter %d/%d (#%d: %s)...",
            i, len(chapters), ch["chapter_number"], ch["title"],
        )
        english_text = translator.translate_chapter(ch["chinese_text"])

        write_en(novel_id, ch["chapter_number"], english_text)
        conn.execute(
            "UPDATE chapters SET status = 'translated' WHERE id = ?",
            (chapter_id,),
        )
        conn.commit()

        print(f"\n{'='*60}")
        print(f"Chapter {ch['chapter_number']}: {ch['title']}")
        print(f"{'='*60}")
        print(english_text[:500])
        if len(english_text) > 500:
            print(f"\n... ({len(english_text)} chars total)")

    conn.close()
    logger.info("\nDone! Results saved to %s", DATABASE_PATH)


if __name__ == "__main__":
    main()
