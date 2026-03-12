from pathlib import Path

import yaml
from pydantic import BaseModel


# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent


def load_config(config_path: Path | None = None) -> dict:
    """Load configuration from config.yaml."""
    if config_path is None:
        config_path = BASE_DIR / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# --------------- Scraper Settings ---------------

class ScraperSettings(BaseModel):
    content_selector: str = "div.chapter-content"
    next_page_selector: str = "a.next-chapter"
    title_selector: str = "h1.chapter-title"
    request_delay_seconds: float = 1.5
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (compatible; personal audiobook project)"
    default_encoding: str = "utf-8"


# --------------- Translation Settings ---------------

class TranslationSettings(BaseModel):
    model_path: str = "nllb-1.3b-ct2"
    tokenizer_path: str = "nllb_tokenizer.model"
    source_lang: str = "zho_Hans"
    target_lang: str = "eng_Latn"
    beam_size: int = 4
    max_decoding_length: int = 512
    device: str = "cuda"


# --------------- TTS Settings ---------------

class TTSSettings(BaseModel):
    engine: str = "tbd"
    sample_rate: int = 22050
    output_format: str = "mp3"
    pause_between_chunks_ms: int = 500


# --------------- Server Settings ---------------

class ServerSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8800
    data_dir: str = "data"
    database_path: str = "data/audiobooks.db"


# --------------- Celery Settings ---------------

class CelerySettings(BaseModel):
    broker_url: str = "redis://localhost:6379/0"
    result_backend: str = "redis://localhost:6379/0"


# --------------- Aggregate Config ---------------

class AppConfig(BaseModel):
    scraper: ScraperSettings = ScraperSettings()
    translation: TranslationSettings = TranslationSettings()
    tts: TTSSettings = TTSSettings()
    server: ServerSettings = ServerSettings()
    celery: CelerySettings = CelerySettings()

    @classmethod
    def from_yaml(cls, config_path: Path | None = None) -> "AppConfig":
        raw = load_config(config_path)
        return cls(
            scraper=ScraperSettings(**raw.get("scraper", {})),
            translation=TranslationSettings(**raw.get("translation", {})),
            tts=TTSSettings(**raw.get("tts", {})),
            server=ServerSettings(**raw.get("server", {})),
            celery=CelerySettings(**raw.get("celery", {})),
        )


# Global config instance — initialized on first import
settings = AppConfig.from_yaml()
