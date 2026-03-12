import aiosqlite

from app.config import settings, BASE_DIR

DATABASE_PATH = BASE_DIR / settings.server.database_path

SQL_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS novels (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    dictionary_id TEXT,
    total_chapters INTEGER DEFAULT 0,
    processed_chapters INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chapters (
    id TEXT PRIMARY KEY,
    novel_id TEXT NOT NULL REFERENCES novels(id),
    chapter_number INTEGER NOT NULL,
    title TEXT,
    source_url TEXT,
    chinese_text TEXT,
    english_text TEXT,
    audio_path TEXT,
    audio_duration_seconds REAL,
    audio_file_size_bytes INTEGER,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(novel_id, chapter_number)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    novel_id TEXT NOT NULL REFERENCES novels(id),
    job_type TEXT NOT NULL,
    status TEXT DEFAULT 'queued',
    progress_percent REAL DEFAULT 0,
    current_step TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS playback_state (
    novel_id TEXT PRIMARY KEY REFERENCES novels(id),
    chapter_number INTEGER NOT NULL,
    position_seconds REAL NOT NULL DEFAULT 0,
    playback_speed REAL NOT NULL DEFAULT 2.0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS term_dictionaries (
    id TEXT PRIMARY KEY,
    novel_id TEXT REFERENCES novels(id),
    name TEXT NOT NULL,
    entries_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


async def get_db() -> aiosqlite.Connection:
    """Get an async database connection."""
    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db() -> None:
    """Initialize the database — create tables if they don't exist."""
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = await get_db()
    try:
        await db.executescript(SQL_CREATE_TABLES)
        await db.commit()
    finally:
        await db.close()
