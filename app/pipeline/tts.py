"""
Text-to-Speech pipeline.

Converts translated English text (with Chinese name annotations)
into audio files. The TTS engine is abstracted behind a base class
so different models can be swapped in after evaluation.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from app.config import settings


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


class PlaceholderTTS(TTSEngine):
    """Placeholder TTS engine — to be replaced after model evaluation."""

    def load_model(self) -> None:
        # TODO: Replace with actual TTS model loading
        pass

    def synthesize(self, text: str, output_path: Path) -> None:
        # TODO: Replace with actual TTS synthesis
        raise NotImplementedError("TTS engine not yet selected. Evaluate candidate models first.")


def split_into_utterances(text: str) -> list[str]:
    """
    Split translated text into utterance-sized chunks for TTS.
    Most TTS models handle shorter inputs better.
    """
    # TODO: Implement sentence/paragraph splitting logic
    raise NotImplementedError


def prepare_for_tts(text: str, engine: TTSEngine) -> str:
    """
    Pre-process text for the specific TTS engine.
    Parse Chinese name annotations into whatever format the engine expects.
    """
    # TODO: Implement TTS-specific text preprocessing
    raise NotImplementedError


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
    1. Split text into utterances
    2. Synthesize each utterance
    3. Concatenate with pauses
    4. Save as MP3
    """
    # TODO: Implement full chapter audio generation
    raise NotImplementedError
