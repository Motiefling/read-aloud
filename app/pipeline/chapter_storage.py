"""
On-disk storage for chapter text (Chinese + English).

Text was previously stored in SQLite columns; it's now on disk alongside
audio to keep the DB small and movable to local storage.
"""
from pathlib import Path
import logging

from app.config import get_data_dir

logger = logging.getLogger(__name__)


def _chapter_dir(novel_id: str) -> Path:
    return get_data_dir() / "novels" / novel_id


def zh_path(novel_id: str, chapter_number: int) -> Path:
    return _chapter_dir(novel_id) / f"chapter_{chapter_number:04d}.zh.txt"


def en_path(novel_id: str, chapter_number: int) -> Path:
    return _chapter_dir(novel_id) / f"chapter_{chapter_number:04d}.en.txt"


def write_zh(novel_id: str, chapter_number: int, text: str) -> None:
    p = zh_path(novel_id, chapter_number)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text or "", encoding="utf-8")


def write_en(novel_id: str, chapter_number: int, text: str) -> None:
    p = en_path(novel_id, chapter_number)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text or "", encoding="utf-8")


def read_zh(novel_id: str, chapter_number: int) -> str | None:
    p = zh_path(novel_id, chapter_number)
    return p.read_text(encoding="utf-8") if p.exists() else None


def read_en(novel_id: str, chapter_number: int) -> str | None:
    p = en_path(novel_id, chapter_number)
    return p.read_text(encoding="utf-8") if p.exists() else None


def has_zh(novel_id: str, chapter_number: int) -> bool:
    return zh_path(novel_id, chapter_number).exists()


def has_en(novel_id: str, chapter_number: int) -> bool:
    return en_path(novel_id, chapter_number).exists()


def delete_chapter_text(novel_id: str, chapter_number: int) -> None:
    for p in (zh_path(novel_id, chapter_number), en_path(novel_id, chapter_number)):
        if p.exists():
            try:
                p.unlink()
            except OSError:
                logger.exception("Failed to delete %s", p)
