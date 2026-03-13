# Read Aloud

Chinese light novel to English audiobook pipeline. Scrapes novels from supported sites, translates Chinese to English using Opus-MT, and (soon) generates TTS audio — all served via a FastAPI + PWA interface.

## Current Status

**Working:**
- Web scraper — multi-site support with per-domain CSS selector profiles (funs.me implemented)
- Translation — Opus-MT (`Helsinki-NLP/opus-mt-zh-en`) via HuggingFace Transformers, with Traditional→Simplified Chinese conversion (OpenCC), term dictionary post-processing, and Chinese name annotation for TTS
- FastAPI server skeleton with API routes and SQLite database
- Celery task definitions for pipeline orchestration
- PWA frontend shell

**Not yet implemented:**
- TTS engine (pending model evaluation)
- Audio processing / concatenation
- API endpoint logic (stubs only)
- Full end-to-end pipeline

## Requirements

- Python 3.11+
- NVIDIA GPU with CUDA (tested on RTX 5090)
- Redis (runs via Docker Compose)
- FFmpeg (for future audio processing)

## Setup

```bash
# Create and activate virtual environment
py -3.11 -m venv venv
.\venv\Scripts\activate        # Windows
# source venv/bin/activate     # Linux/macOS

# Install dependencies
pip install -r requirements.txt

# Additional dependencies not in requirements.txt yet:
pip install opencc-python-reimplemented torch
```

## Running

### 1. Start Redis

```bash
docker compose up -d
```

### 2. Start the FastAPI server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8800 --reload
```

### 3. Start a Celery worker

```bash
celery -A app.pipeline.tasks worker --loglevel=info --pool=solo
```

The PWA frontend is served at `http://localhost:8800/`.

## Configuration

All settings are in `config.yaml`:

- **scraper** — per-site CSS selectors, request delay, retry count, user agent
- **translation** — model path, beam size, max decoding length, device
- **tts** — engine (TBD), sample rate, output format
- **server** — host, port, data directory, database path
- **celery** — Redis broker/backend URLs

## Project Structure

```
app/
  main.py              # FastAPI entry point
  config.py            # Pydantic settings loaded from config.yaml
  models.py            # Request/response schemas
  database.py          # SQLite via aiosqlite
  api/                 # Route modules (novels, chapters, jobs, dictionaries, websocket)
  pipeline/
    scraper.py         # Multi-site web scraper
    translator.py      # Opus-MT translation with OpenCC + term dict
    tts.py             # TTS engine (TBD)
    audio_processing.py
    tasks.py           # Celery task definitions
  utils/
    chinese_detect.py
    term_dictionary.py
pwa/                   # Vanilla PWA frontend
data/                  # Runtime data (novels, dictionaries, SQLite DB)
config.yaml
docker-compose.yml     # Redis only (app runs on host for GPU access)
```

## Tests

```bash
pytest tests/
```

The translator has a comprehensive test suite (`tests/test_translator.py`). Scraper and TTS tests exist but are minimal.
