"""
Text-to-Speech pipeline using Kokoro TTS.

Converts translated English text (with Chinese name annotations)
into audio files. Chinese name annotations like {{zh:林墨}} are
stripped for now, but the architecture supports adding phoneme-
switching for Chinese names in the future.
"""

import logging
import re
import time
import warnings
from abc import ABC, abstractmethod
from pathlib import Path

import numpy as np
import soundfile as sf

from app.config import settings

logger = logging.getLogger(__name__)

SAMPLE_RATE = 24000  # Kokoro outputs 24kHz, fixed

# Matches {{zh:...}} annotation markers from the translator
_ZH_ANNOTATION_RE = re.compile(r"\s*\{\{zh:[^}]+\}\}")

# Sentence-end punctuation for splitting utterances
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?;])\s+")

# A line is pronounceable only if it contains at least one letter or digit.
# Lines of pure punctuation/symbols (e.g. "----...----" separators common in
# Chinese web novels) produce no audio in Kokoro and would otherwise crash.
_PRONOUNCEABLE_RE = re.compile(r"\w", re.UNICODE)

# Max characters per utterance — keeps Kokoro output quality high
_MAX_UTTERANCE_LENGTH = 400


class TTSError(Exception):
    """Raised when TTS synthesis fails."""


class TTSEngine(ABC):
    """Abstract base class for TTS engines."""

    @abstractmethod
    def load_model(self) -> None:
        """Load the TTS model into memory."""
        ...

    @abstractmethod
    def synthesize(self, text: str, output_path: Path) -> None:
        """Generate audio for a text chunk and save to output_path."""
        ...

    @abstractmethod
    def synthesize_to_array(self, text: str) -> np.ndarray:
        """Generate audio and return as a numpy array (float32, mono)."""
        ...


class KokoroTTS(TTSEngine):
    """Kokoro TTS engine — fast, lightweight, GPU-accelerated."""

    def __init__(self):
        self._pipeline = None

    def load_model(self) -> None:
        # Suppress phonemizer/espeak noise. The "words count mismatch" warning
        # means espeak's punctuation-restoration step couldn't 1:1 align tokens
        # with the input — harmless, audio still synthesizes correctly.
        for name in ("phonemizer", "phonemizer.backend", "phonemizer.backend.espeak"):
            logging.getLogger(name).setLevel(logging.CRITICAL)
        warnings.filterwarnings("ignore", message=r".*words count mismatch.*")

        from kokoro import KPipeline

        cfg = settings.tts
        logger.info(
            "Loading Kokoro TTS (voice=%s, lang=%s, device=%s)",
            cfg.voice, cfg.lang_code, cfg.device,
        )
        self._pipeline = KPipeline(lang_code=cfg.lang_code, device=cfg.device)
        logger.info("Kokoro TTS loaded successfully")

    def synthesize(self, text: str, output_path: Path) -> None:
        audio = self.synthesize_to_array(text)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sf.write(str(output_path), audio, SAMPLE_RATE)

    def synthesize_to_array(self, text: str) -> np.ndarray:
        import torch

        if self._pipeline is None:
            raise TTSError("Model not loaded. Call load_model() first.")

        if not text.strip():
            return np.array([], dtype=np.float32)

        cfg = settings.tts
        generator = self._pipeline(
            text,
            voice=cfg.voice,
            speed=cfg.speed,
            split_pattern=r"\n+",
        )

        segments = []
        for result in generator:
            if result.audio is not None:
                segments.append(result.audio)

        if not segments:
            raise TTSError(f"Kokoro produced no audio for: {text[:80]!r}")

        audio = torch.cat(segments, dim=-1).cpu().numpy()
        return audio.astype(np.float32)


# --------------- Module-level singleton ---------------

_tts_instance: KokoroTTS | None = None


def get_tts_engine() -> KokoroTTS:
    """Get or create the module-level KokoroTTS singleton."""
    global _tts_instance
    if _tts_instance is None:
        _tts_instance = KokoroTTS()
        _tts_instance.load_model()
    return _tts_instance


# --------------- Text preparation ---------------


def prepare_for_tts(text: str) -> str:
    """
    Pre-process translated text for TTS.

    Currently strips {{zh:...}} Chinese name annotations, leaving just
    the English name. The annotation pattern is preserved in the translator
    output so phoneme-switching can be added later.
    """
    return _ZH_ANNOTATION_RE.sub("", text)


def split_into_utterances(text: str) -> list[str]:
    """
    Split translated text into utterance-sized chunks for TTS.

    Splits on paragraph boundaries first, then breaks long paragraphs
    at sentence boundaries to stay under _MAX_UTTERANCE_LENGTH.
    """
    paragraphs = text.split("\n")
    utterances = []

    for para in paragraphs:
        stripped = para.strip()
        if not stripped:
            continue

        if not _PRONOUNCEABLE_RE.search(stripped):
            logger.debug("Skipping unpronounceable line: %r", stripped[:80])
            continue

        if len(stripped) <= _MAX_UTTERANCE_LENGTH:
            utterances.append(stripped)
        else:
            # Split long paragraphs at sentence boundaries
            sentences = _SENTENCE_SPLIT_RE.split(stripped)
            current_chunk = ""
            for sentence in sentences:
                if current_chunk and len(current_chunk) + len(sentence) + 1 > _MAX_UTTERANCE_LENGTH:
                    utterances.append(current_chunk.strip())
                    current_chunk = sentence
                else:
                    current_chunk = f"{current_chunk} {sentence}" if current_chunk else sentence
            if current_chunk.strip():
                utterances.append(current_chunk.strip())

    return utterances


# --------------- Chapter-level generation ---------------


def generate_chapter_audio(
    english_text: str,
    tts_engine: TTSEngine,
    output_dir: Path,
    novel_id: str,
    chapter_number: int,
) -> Path:
    """
    Generate a complete audio file for a chapter.

    Steps:
    1. Strip TTS annotations from text
    2. Split into paragraph-level utterances
    3. Synthesize each utterance to a numpy array
    4. Concatenate all arrays with silence gaps between paragraphs
    5. Save as WAV, convert to MP3
    """
    from app.pipeline.audio_processing import convert_to_mp3

    prepared = prepare_for_tts(english_text)
    utterances = split_into_utterances(prepared)

    if not utterances:
        raise TTSError("No utterances to synthesize after text preparation")

    cfg = settings.tts
    pause_samples = int(SAMPLE_RATE * cfg.pause_between_paragraphs_ms / 1000)
    silence = np.zeros(pause_samples, dtype=np.float32)

    total_chars = sum(len(u) for u in utterances)
    logger.info(
        "TTS chapter #%d: synthesizing %d utterances (%d chars)",
        chapter_number, len(utterances), total_chars,
    )

    synth_start = time.time()
    all_audio = []
    empty_utterances = 0
    for i, utterance in enumerate(utterances):
        try:
            audio = tts_engine.synthesize_to_array(utterance)
        except TTSError as exc:
            empty_utterances += 1
            logger.warning(
                "Utterance %d/%d failed, skipping (len=%d): %s | text=%r",
                i + 1, len(utterances), len(utterance), exc, utterance[:200],
            )
            continue
        if len(audio) > 0:
            all_audio.append(audio)
            all_audio.append(silence)
        else:
            empty_utterances += 1
            logger.debug(
                "Utterance %d/%d produced empty audio: %r",
                i + 1, len(utterances), utterance[:80],
            )

    if not all_audio:
        raise TTSError("All utterances produced empty audio")

    # Remove trailing silence
    if len(all_audio) > 1:
        all_audio = all_audio[:-1]

    full_audio = np.concatenate(all_audio)

    # Peak-normalize so the loudest sample sits just under 0 dBFS. Kokoro's
    # raw output is quiet (peaks well below 1.0); without this, the MP3 plays
    # noticeably softer than typical audio content.
    peak = float(np.max(np.abs(full_audio)))
    if peak > 0:
        full_audio = full_audio * (0.99 / peak)

    synth_elapsed = time.time() - synth_start
    audio_duration = len(full_audio) / SAMPLE_RATE
    success_count = len(utterances) - empty_utterances
    logger.info(
        "TTS chapter #%d: synthesized %d/%d utterances in %.1fs (%.1fs audio)",
        chapter_number, success_count, len(utterances), synth_elapsed, audio_duration,
    )

    # Save as WAV first, then convert to MP3
    chapter_dir = output_dir / novel_id
    chapter_dir.mkdir(parents=True, exist_ok=True)

    wav_path = chapter_dir / f"chapter_{chapter_number:04d}.wav"
    sf.write(str(wav_path), full_audio, SAMPLE_RATE)

    mp3_path = chapter_dir / f"chapter_{chapter_number:04d}.mp3"
    convert_to_mp3(wav_path, mp3_path)
    wav_path.unlink()

    return mp3_path
