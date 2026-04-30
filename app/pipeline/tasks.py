"""
Celery task definitions for the background processing pipeline.

The dispatcher loop processes chapters one at a time, asking FastAPI for the
next piece of work via /internal/work/next between each iteration so queue
reordering takes effect immediately.

The worker no longer opens SQLite directly — all DB mutation happens through
HTTP calls to the FastAPI server (see app.pipeline.api_client). This keeps
the DB file owned by a single process and prevents corruption when the
worker runs on a different host.
"""

import logging
import re
import time

from celery import Celery

from app.config import settings, get_data_dir
from app.pipeline import api_client
from app.utils.error_log import record_error
from app.utils.replacements import apply_pre_replacements, apply_post_replacements

logger = logging.getLogger(__name__)

celery_app = Celery(
    "audiobook",
    broker=settings.celery.broker_url,
    backend=settings.celery.result_backend,
)


def _run_tts_phase(
    chapter_id: str,
    chapter_number: int,
    english_text: str,
    novel_id: str,
    tts_engine,
    output_dir,
    post_rules: list[tuple[str, str]],
    post_hash: str,
    chapter_url: str | None = None,
    novel_url: str | None = None,
    title: str | None = None,
) -> bool:
    """Apply post-translation rules and synthesize audio for ``english_text``.

    Returns True on success, False on TTS error (already reported).
    """
    from app.pipeline.tts import generate_chapter_audio, TTSError
    from app.pipeline.audio_processing import get_audio_duration

    english_for_tts = apply_post_replacements(english_text, post_rules)

    try:
        tts_start = time.time()
        audio_path = generate_chapter_audio(
            english_text=english_for_tts,
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
        api_client.mark_chapter_audio_ready(
            chapter_id, relative_path, duration, file_size,
            post_replacements_hash=post_hash,
        )
        return True
    except (TTSError, RuntimeError) as exc:
        logger.error(
            "SKIPPED chapter #%d (tts failed): %s", chapter_number, exc,
        )
        record_error(
            novel_id=novel_id,
            chapter_number=chapter_number,
            chapter_id=chapter_id,
            phase="tts",
            reason=str(exc) or type(exc).__name__,
            exc=exc,
            title=title,
            chapter_url=chapter_url,
            novel_url=novel_url,
            english_chars=len(english_for_tts),
            english_snippet=english_for_tts[:300],
        )
        api_client.mark_chapter_error(chapter_id)
        return False


def _translate_and_tts_chapter(
    chapter_id: str,
    chapter_number: int,
    chinese_text: str,
    title: str | None,
    novel_id: str,
    job_id: str,
    translator,
    tts_engine,
    output_dir,
    pre_rules: list[tuple[str, str]],
    pre_hash: str,
    post_rules: list[tuple[str, str]],
    post_hash: str,
    chapter_url: str | None = None,
    novel_url: str | None = None,
) -> bool:
    """Run translate → TTS for a single chapter. Reports results via api_client.

    Returns True if the chapter reached ``audio_ready``, False on error.
    """
    from app.pipeline.translator import TranslationError
    from app.pipeline.chapter_storage import write_en

    # Apply pre-translation rules (e.g. inject English placeholders so Qwen
    # passes them through verbatim).
    chinese_for_translation = apply_pre_replacements(chinese_text, pre_rules)

    # --- Translate ---
    try:
        translate_start = time.time()
        english_text = translator.translate_chapter(chinese_for_translation)
        title_english = (
            translator.translate_title(title) if title else None
        )
        translate_elapsed = time.time() - translate_start
        logger.info(
            "TIMING: Translation chapter #%d done in %.1fs (%d chars -> %d chars)",
            chapter_number, translate_elapsed,
            len(chinese_text), len(english_text),
        )
        write_en(novel_id, chapter_number, english_text)
        api_client.mark_chapter_translated(
            chapter_id, title_english, pre_replacements_hash=pre_hash,
        )
    except TranslationError as exc:
        logger.error(
            "SKIPPED chapter #%d (translate failed): %s", chapter_number, exc,
        )
        record_error(
            novel_id=novel_id,
            chapter_number=chapter_number,
            chapter_id=chapter_id,
            phase="translate",
            reason=str(exc) or "TranslationError",
            exc=exc,
            title=title,
            chapter_url=chapter_url,
            novel_url=novel_url,
            chinese_chars=len(chinese_text),
            chinese_snippet=chinese_text[:300],
        )
        api_client.mark_chapter_error(chapter_id)
        return False

    # --- Validate translation has English content ---
    if not re.search(r"[a-zA-Z]", english_text):
        logger.error(
            "SKIPPED chapter #%d (empty translation — no English chars)",
            chapter_number,
        )
        record_error(
            novel_id=novel_id,
            chapter_number=chapter_number,
            chapter_id=chapter_id,
            phase="empty_translation",
            reason="Translation produced no English characters",
            title=title,
            chapter_url=chapter_url,
            novel_url=novel_url,
            chinese_chars=len(chinese_text),
            chinese_snippet=chinese_text[:300],
            english_output=english_text[:300],
        )
        api_client.mark_chapter_error(chapter_id)
        return False

    return _run_tts_phase(
        chapter_id, chapter_number, english_text, novel_id,
        tts_engine, output_dir, post_rules, post_hash,
        chapter_url=chapter_url, novel_url=novel_url, title=title,
    )


# ---------------------------------------------------------------------------
# Dispatcher loop — processes chapters one at a time, respecting queue order
# ---------------------------------------------------------------------------

@celery_app.task(bind=True)
def dispatcher_loop(self):
    """
    Persistent worker loop. Loads GPU models once, then polls
    /internal/work/next for chapters to process.

    Between each chapter, re-queries the server so that queue reordering
    takes effect immediately.
    """
    import redis as redis_lib
    from app.pipeline.translator import get_translator
    from app.pipeline.tts import get_tts_engine
    from app.pipeline.chapter_storage import read_zh, read_en
    from app.utils.chinese_detect import contains_chinese
    from app.utils.error_log import get_error_log_path

    # Quiet down noisy third-party libraries so the worker terminal only shows
    # signal — model load, chapter progress, errors. Set per-logger so we keep
    # WARNING+ from any of them if something actually goes wrong.
    for noisy in ("transformers", "urllib3", "httpx", "httpcore", "asyncio", "filelock"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logger.info("=" * 60)
    logger.info("Dispatcher loop starting — loading GPU models")
    logger.info("Error log: %s", get_error_log_path())
    logger.info("=" * 60)

    translator = get_translator()
    tts_engine = get_tts_engine()
    output_dir = get_data_dir() / "novels"

    # Per-novel state cached across chapters
    cached_novel_id: str | None = None
    cached_pre_rules: list[tuple[str, str]] = []
    cached_pre_hash: str = ""
    cached_post_rules: list[tuple[str, str]] = []
    cached_post_hash: str = ""
    translated_title_novels: set[str] = set()

    # Redis pub/sub for wake-up signals
    redis_conn = redis_lib.Redis.from_url(settings.celery.broker_url)
    pubsub = redis_conn.pubsub()
    pubsub.subscribe("queue:work_available")

    logger.info("Dispatcher loop ready — waiting for work")

    while True:
        try:
            work = api_client.get_next_work()

            if work is None:
                # No work — wait for a signal or timeout
                try:
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=30)
                except Exception:
                    time.sleep(5)
                continue

            chapter = work["chapter"]
            novel = work["novel"]
            progress = work["progress"]
            job_id = work["job_id"]

            novel_id = chapter["novel_id"]
            chapter_id = chapter["id"]
            chapter_number = chapter["chapter_number"]
            chapter_status = chapter.get("status", "scraped")
            chapter_url = chapter.get("source_url")
            novel_url = novel.get("source_url")

            # Refresh replacement rules whenever we switch novels.  We always
            # refetch on novel switch (not cached forever) so the rules match
            # what the user has saved most recently.
            if novel_id != cached_novel_id:
                cached_pre_rules, cached_pre_hash = api_client.get_replacements(novel_id, "pre")
                cached_post_rules, cached_post_hash = api_client.get_replacements(novel_id, "post")
                cached_novel_id = novel_id

            # Translate novel title once per novel if it contains Chinese
            if novel_id not in translated_title_novels:
                title = novel.get("title")
                if title and contains_chinese(title):
                    try:
                        translated = translator.translate_title(title)
                        if translated and translated != title:
                            logger.info("Novel title translated: %s -> %s", title, translated)
                            api_client.update_novel_title(novel_id, translated)
                    except Exception:
                        logger.exception("Failed to translate novel title for %s", novel_id)
                translated_title_novels.add(novel_id)

            ready = progress["ready"]
            total = progress["total"]
            api_client.update_job(
                job_id,
                status="running",
                current_step=(
                    f"Processing chapter {chapter_number} ({ready}/{total})"
                ),
                progress_percent=(ready / total * 100) if total else 0.0,
            )

            logger.info(
                "Dispatcher: processing novel %s chapter #%d (%d/%d ready, status=%s)",
                novel_id, chapter_number, ready, total, chapter_status,
            )

            ch_start = time.time()

            if chapter_status == "translated":
                # Re-TTS path: skip Qwen, reuse the existing English text.
                english_text = read_en(novel_id, chapter_number)
                if not english_text:
                    logger.error(
                        "SKIPPED chapter #%d (translated status but no .en.txt)",
                        chapter_number,
                    )
                    record_error(
                        novel_id=novel_id,
                        chapter_number=chapter_number,
                        chapter_id=chapter_id,
                        phase="missing_en",
                        reason="Chapter English text (.en.txt) not found on disk",
                        title=chapter.get("title"),
                        chapter_url=chapter_url,
                        novel_url=novel_url,
                    )
                    api_client.mark_chapter_error(chapter_id)
                    continue

                success = _run_tts_phase(
                    chapter_id, chapter_number, english_text, novel_id,
                    tts_engine, output_dir,
                    cached_post_rules, cached_post_hash,
                    chapter_url=chapter_url, novel_url=novel_url,
                    title=chapter.get("title"),
                )
            else:
                chinese_text = read_zh(novel_id, chapter_number)
                if not chinese_text:
                    logger.error(
                        "SKIPPED chapter #%d (missing .zh.txt on disk)",
                        chapter_number,
                    )
                    record_error(
                        novel_id=novel_id,
                        chapter_number=chapter_number,
                        chapter_id=chapter_id,
                        phase="missing_zh",
                        reason="Chapter source text (.zh.txt) not found on disk",
                        title=chapter.get("title"),
                        chapter_url=chapter_url,
                        novel_url=novel_url,
                    )
                    api_client.mark_chapter_error(chapter_id)
                    continue

                success = _translate_and_tts_chapter(
                    chapter_id, chapter_number,
                    chinese_text, chapter.get("title"),
                    novel_id, job_id,
                    translator, tts_engine, output_dir,
                    cached_pre_rules, cached_pre_hash,
                    cached_post_rules, cached_post_hash,
                    chapter_url=chapter_url,
                    novel_url=novel_url,
                )

            if success:
                logger.info(
                    "TIMING: Chapter #%d done in %.1fs",
                    chapter_number, time.time() - ch_start,
                )

        except Exception as exc:
            logger.exception("Dispatcher loop error — will retry in 5s")
            record_error(
                novel_id=None,
                chapter_number=None,
                chapter_id=None,
                phase="dispatcher",
                reason=str(exc) or type(exc).__name__,
                exc=exc,
            )
            time.sleep(5)


# Auto-start the dispatcher when the Celery worker is ready
from celery.signals import worker_ready  # noqa: E402


@worker_ready.connect
def _on_worker_ready(**kwargs):
    logger.info("Worker ready — starting dispatcher loop")
    dispatcher_loop.delay()
