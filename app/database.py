import aiosqlite

from app.config import get_database_path

DATABASE_PATH = get_database_path()

SQL_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS novels (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    dictionary_id TEXT,
    total_chapters INTEGER DEFAULT 0,
    processed_chapters INTEGER DEFAULT 0,
    cover_image_path TEXT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chapters (
    id TEXT PRIMARY KEY,
    novel_id TEXT NOT NULL REFERENCES novels(id),
    chapter_number INTEGER NOT NULL,
    title TEXT,
    title_english TEXT,
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
        # Migrations for existing databases
        cursor = await db.execute("PRAGMA table_info(chapters)")
        columns = {row[1] for row in await cursor.fetchall()}
        if "title_english" not in columns:
            await db.execute("ALTER TABLE chapters ADD COLUMN title_english TEXT")

        # Migrate paths from BASE_DIR-relative (data\novels\... or data/novels/...)
        # to data_dir-relative (novels\...) for portable data directory support
        await db.execute(
            "UPDATE chapters SET audio_path = SUBSTR(audio_path, 6) "
            "WHERE audio_path LIKE 'data\\novels\\%'"
        )
        await db.execute(
            "UPDATE chapters SET audio_path = SUBSTR(audio_path, 6) "
            "WHERE audio_path LIKE 'data/novels/%'"
        )
        await db.execute(
            "UPDATE novels SET cover_image_path = SUBSTR(cover_image_path, 6) "
            "WHERE cover_image_path LIKE 'data\\novels\\%'"
        )
        await db.execute(
            "UPDATE novels SET cover_image_path = SUBSTR(cover_image_path, 6) "
            "WHERE cover_image_path LIKE 'data/novels/%'"
        )

        await db.commit()
    finally:
        await db.close()
