# Read Aloud

Chinese light novel to English audiobook pipeline. Scrapes novels from supported sites, translates Chinese to English using Opus-MT, generates speech with Kokoro TTS, and serves everything via a FastAPI + PWA interface.

## Project Board

Track progress and planned features on the [Trello board](https://trello.com/b/T7eSVh01/read-aloud-tts).

## Current Status

The full end-to-end pipeline is working: scrape → translate → TTS → audio.

**Working:**
- **Web scraper** — multi-site support with per-domain CSS selector profiles (funs.me implemented, ttkan.co placeholder)
- **Translation** — Opus-MT (`Helsinki-NLP/opus-mt-zh-en`) via HuggingFace Transformers, with Traditional→Simplified Chinese conversion (OpenCC), term dictionary post-processing, and Chinese name annotation for TTS
- **TTS** — Kokoro (`kokoro` package) with 25 English voices (American and British, male and female), voice preview, and voice selection via API
- **Audio processing** — WAV→MP3 conversion via FFmpeg, playback speed adjustment, duration detection
- **Celery pipeline** — full orchestration (scrape → translate → TTS) with progress tracking, plus individual tasks for scraping, translation, and audio generation
- **API** — fully implemented REST endpoints for novels (CRUD, update/add chapters), chapters (list, audio streaming), jobs (status/progress), dictionaries (CRUD), settings (voice list/select/preview), and WebSocket for real-time updates
- **PWA frontend** — vanilla HTML/CSS/JS with Service Worker for offline support
- **One-command launcher** — `python start.py` starts Redis, Celery, and FastAPI together

## Requirements

- Python 3.11+
- NVIDIA GPU with CUDA (tested on RTX 5090)
- Redis
- FFmpeg

## Setup

```bash
# Create and activate virtual environment
py -3.11 -m venv venv
.\venv\Scripts\activate        # Windows
# source venv/bin/activate     # Linux/macOS

# Install dependencies
pip install -r requirements.txt

# Additional dependencies not in requirements.txt:
pip install opencc-python-reimplemented torch
```

## Running

### Quick start

```bash
python start.py
```

This starts Redis, a Celery worker, and the FastAPI server in one command. Press Ctrl+C to stop everything.

### Manual start

```bash
# 1. Start Redis
docker compose up -d

# 2. Start a Celery worker
celery -A app.pipeline.tasks worker --loglevel=info --pool=solo

# 3. Start the FastAPI server
uvicorn app.main:app --host 0.0.0.0 --port 8800 --reload
```

The PWA frontend is served at `http://localhost:8800/` and the API docs at `http://localhost:8800/docs`.

## Configuration

All settings are in `config.yaml`:

- **scraper** — per-site CSS selectors, request delay, retry count, user agent
- **translation** — model path, beam size, max decoding length, device
- **tts** — engine (Kokoro), voice, speed, language code, device, sample rate, output format, paragraph pause duration
- **server** — host, port, data directory, database path
- **celery** — Redis broker/backend URLs

## Project Structure

```
app/
  main.py              # FastAPI entry point
  config.py            # Pydantic settings loaded from config.yaml
  models.py            # Request/response schemas
  database.py          # SQLite via aiosqlite
  api/
    novels.py          # Novel CRUD + pipeline triggers
    chapters.py        # Chapter listing + audio streaming
    jobs.py            # Job status and progress
    dictionaries.py    # Term dictionary management
    settings.py        # Voice list, preview, and selection
    websocket.py       # Real-time job updates
  pipeline/
    scraper.py         # Multi-site web scraper
    translator.py      # Opus-MT translation with OpenCC + term dict
    tts.py             # Kokoro TTS engine + chapter audio generation
    audio_processing.py # FFmpeg: MP3 conversion, speed adjust, duration
    tasks.py           # Celery tasks: full pipeline, scrape, translate, TTS
  utils/
    chinese_detect.py
    term_dictionary.py
pwa/                   # Vanilla PWA frontend (HTML/CSS/JS, Service Worker)
data/                  # Runtime data (novels, dictionaries, SQLite DB)
config.yaml
docker-compose.yml     # Redis only (app runs on host for GPU access)
start.py               # One-command launcher (Redis + Celery + FastAPI)
```

## Tests

```bash
pytest tests/
```

The translator has a comprehensive test suite (`tests/test_translator.py`). Scraper and TTS tests exist but are minimal.
