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

class SiteProfile(BaseModel):
    """CSS selectors and URL patterns for a specific light novel site."""
    content_selector: str | None = None
    next_page_selector: str | None = None
    title_selector: str | None = None
    toc_selector: str | None = None
    toc_url_pattern: str | None = None
    chapter_url_pattern: str | None = None
    encoding: str | None = None  # Override response encoding (e.g. "gbk")
    use_browser: bool = False  # Use Playwright headless browser instead of httpx
    browser_wait_time: int = 5000  # Milliseconds to wait after page load for JS execution
    notes: str | None = None


class ScraperSettings(BaseModel):
    request_delay_seconds: float = 1.5
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (compatible; personal audiobook project)"
    default_encoding: str = "utf-8"
    sites: dict[str, SiteProfile] = {}

    def get_site_profile(self, domain: str) -> SiteProfile:
        """Look up the site profile for a given domain. Raises KeyError if not found."""
        if domain not in self.sites:
            raise KeyError(
                f"No scraper profile configured for '{domain}'. "
                f"Supported sites: {', '.join(self.sites.keys())}"
            )
        return self.sites[domain]


# --------------- Translation Settings ---------------

class TranslationSettings(BaseModel):
    model_path: str = "Qwen/Qwen2.5-7B-Instruct"
    max_new_tokens: int = 4096
    temperature: float = 0.3
    device: str = "cuda"
    system_prompt: str = (
        "You are a professional translator specializing in Chinese web novels "
        "and light novels. Translate the provided Chinese text into natural, "
        "fluent English.\n"
        "Rules:\n"
        "- Translate EVERY sentence completely. Never leave any Chinese text "
        "untranslated. Never echo back the original Chinese. Every single "
        "sentence in the input must appear as English in the output.\n"
        "- Translate naturally and idiomatically.\n"
        "- Prioritize readability over literal accuracy.\n"
        "- Preserve the tone and atmosphere of the original -- action scenes "
        "should feel tense, dialogue should feel natural, introspective "
        "passages should feel contemplative.\n"
        "- Do not add commentary, notes, explanations, or anything not present "
        "in the original text.\n"
        "- Do not summarize. Translate everything.\n"
        "- Preserve all character names in pinyin romanization (e.g. 林墨 → "
        "Lin Mo). Do not translate names into English words.\n"
        "- Your output must contain ZERO Chinese characters. Romanize all "
        "names, places, and terms that you cannot translate. If unsure how "
        "to romanize, use standard pinyin.\n"
        "- Preserve paragraph breaks from the original.\n"
        "- Output only the translated text. Nothing else."
    )


# --------------- TTS Settings ---------------

class TTSSettings(BaseModel):
    engine: str = "kokoro"
    voice: str = "af_heart"
    speed: float = 1.0
    lang_code: str = "a"
    device: str = "cuda"
    sample_rate: int = 24000
    output_format: str = "mp3"
    pause_between_paragraphs_ms: int = 700


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
        scraper_raw = raw.get("scraper", {})
        # Parse site profiles from nested dict
        sites_raw = scraper_raw.pop("sites", {})
        sites = {domain: SiteProfile(**profile) for domain, profile in sites_raw.items()}
        return cls(
            scraper=ScraperSettings(**scraper_raw, sites=sites),
            translation=TranslationSettings(**raw.get("translation", {})),
            tts=TTSSettings(**raw.get("tts", {})),
            server=ServerSettings(**raw.get("server", {})),
            celery=CelerySettings(**raw.get("celery", {})),
        )


# Global config instance — initialized on first import
settings = AppConfig.from_yaml()


def get_data_dir() -> Path:
    """Resolve the data directory as an absolute Path.

    If the configured data_dir is absolute (e.g. 'N:/Read Aloud'), it is used
    as-is. If relative (e.g. 'data'), it is resolved relative to BASE_DIR.
    """
    p = Path(settings.server.data_dir)
    if p.is_absolute():
        return p
    return BASE_DIR / p


def get_database_path() -> Path:
    """Resolve the database path as an absolute Path."""
    p = Path(settings.server.database_path)
    if p.is_absolute():
        return p
    return BASE_DIR / p
