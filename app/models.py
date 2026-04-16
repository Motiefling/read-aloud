from datetime import datetime

from pydantic import BaseModel


# ===================== Request Schemas =====================

class NovelRequest(BaseModel):
    """Request to process a new novel."""
    url: str
    title: str | None = None
    dictionary_id: str | None = None
    max_chapters: int | None = None  # None = download all
    start_chapter_url: str | None = None  # Override: start scraping from this chapter URL


class AddChaptersRequest(BaseModel):
    """Request to add more chapters to an existing novel."""
    max_chapters: int | None = None  # None = download all remaining
    start_url: str | None = None  # Override: start scraping from this URL instead of last chapter


class RenameRequest(BaseModel):
    """Rename a novel or chapter."""
    title: str


class PlaybackStateUpdate(BaseModel):
    """Update playback position for a novel."""
    chapter_number: int
    position_seconds: float
    playback_speed: float = 2.0


class TermDictionaryUpdate(BaseModel):
    """Update a term dictionary."""
    name: str
    entries: dict  # {"characters": {...}, "locations": {...}, "terms": {...}}


class QueueReorderRequest(BaseModel):
    """Reorder the processing queue."""
    novel_ids: list[str]  # Ordered list of novel IDs (first = highest priority)


# ===================== Response Schemas =====================

class NovelResponse(BaseModel):
    id: str
    title: str
    source_url: str
    dictionary_id: str | None = None
    cover_image_path: str | None = None
    total_chapters: int = 0
    processed_chapters: int = 0
    status: str = "pending"
    queue_position: int | None = None
    queue_status: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ChapterResponse(BaseModel):
    id: str
    novel_id: str
    chapter_number: int
    title: str | None = None
    title_english: str | None = None
    status: str = "pending"
    audio_duration_seconds: float | None = None
    audio_file_size_bytes: int | None = None


class JobResponse(BaseModel):
    id: str
    novel_id: str
    job_type: str
    status: str = "queued"
    progress_percent: float = 0
    current_step: str | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PlaybackStateResponse(BaseModel):
    novel_id: str
    chapter_number: int
    position_seconds: float = 0
    playback_speed: float = 2.0
    updated_at: datetime | None = None


class QueueItemResponse(BaseModel):
    novel_id: str
    title: str
    queue_position: int
    queue_status: str
    total_chapters: int = 0
    processed_chapters: int = 0
    scraped_chapters: int = 0


class TermDictionaryResponse(BaseModel):
    id: str
    novel_id: str | None = None
    name: str
    entries: dict
    created_at: datetime | None = None
    updated_at: datetime | None = None
