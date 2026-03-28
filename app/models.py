from datetime import datetime

from pydantic import BaseModel


# ===================== Request Schemas =====================

class NovelRequest(BaseModel):
    """Request to process a new novel."""
    url: str
    title: str | None = None
    dictionary_id: str | None = None
    max_chapters: int | None = None  # None = download all


class AddChaptersRequest(BaseModel):
    """Request to add more chapters to an existing novel."""
    max_chapters: int | None = None  # None = download all remaining


class PlaybackStateUpdate(BaseModel):
    """Update playback position for a novel."""
    chapter_number: int
    position_seconds: float
    playback_speed: float = 2.0


class TermDictionaryUpdate(BaseModel):
    """Update a term dictionary."""
    name: str
    entries: dict  # {"characters": {...}, "locations": {...}, "terms": {...}}


# ===================== Response Schemas =====================

class NovelResponse(BaseModel):
    id: str
    title: str
    source_url: str
    dictionary_id: str | None = None
    total_chapters: int = 0
    processed_chapters: int = 0
    status: str = "pending"
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ChapterResponse(BaseModel):
    id: str
    novel_id: str
    chapter_number: int
    title: str | None = None
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


class TermDictionaryResponse(BaseModel):
    id: str
    novel_id: str | None = None
    name: str
    entries: dict
    created_at: datetime | None = None
    updated_at: datetime | None = None
