"""
Translation pipeline using Opus-MT (Helsinki-NLP) via HuggingFace Transformers.

Translates Chinese text to English. Traditional Chinese is automatically
converted to Simplified via OpenCC before translation.

Post-processing applies term dictionaries for consistent terminology
and annotates Chinese names for TTS pronunciation hints.
"""

import logging
import re

from opencc import OpenCC
from transformers import MarianMTModel, MarianTokenizer

from app.config import settings

logger = logging.getLogger(__name__)

_SENTENCE_SPLITTER = re.compile(r"(?<=[。！？；…])")
_LONG_PARAGRAPH_THRESHOLD = 200  # characters


class TranslationError(Exception):
    """Raised when translation fails."""


class Translator:
    """Handles Chinese-to-English translation using Opus-MT."""

    def __init__(self, config=None):
        if config is None:
            config = settings.translation
        self.config = config
        self._model = None
        self._tokenizer = None
        self._cc = OpenCC("t2s")  # Traditional -> Simplified converter

    def load_model(self):
        """Load the Opus-MT model and tokenizer."""
        logger.info("Loading translation model: %s", self.config.model_path)
        try:
            self._tokenizer = MarianTokenizer.from_pretrained(
                self.config.model_path,
            )
            self._model = MarianMTModel.from_pretrained(
                self.config.model_path,
            )
            if self.config.device != "cpu":
                self._model = self._model.to(self.config.device)
            self._model.eval()

            logger.info(
                "Translation model loaded successfully on %s", self.config.device
            )
        except RuntimeError as e:
            raise TranslationError(f"Failed to load translation model: {e}") from e
        except OSError as e:
            raise TranslationError(
                f"Model not found: '{self.config.model_path}': {e}"
            ) from e

    def translate_text(self, text: str) -> str:
        """Translate a single text segment from Chinese to English."""
        if self._model is None or self._tokenizer is None:
            raise TranslationError("Model not loaded. Call load_model() first.")

        if not text.strip():
            return ""

        # Convert Traditional Chinese to Simplified for better translation
        simplified = self._cc.convert(text)

        try:
            inputs = self._tokenizer(
                simplified, return_tensors="pt", truncation=True, max_length=512
            )
            if self.config.device != "cpu":
                inputs = {k: v.to(self.config.device) for k, v in inputs.items()}

            outputs = self._model.generate(
                **inputs,
                max_length=self.config.max_decoding_length,
                num_beams=self.config.beam_size,
                no_repeat_ngram_size=3,
            )
            return self._tokenizer.decode(outputs[0], skip_special_tokens=True)
        except RuntimeError as e:
            raise TranslationError(f"Translation inference failed: {e}") from e

    def _translate_all(self, texts: list[str]) -> list[str]:
        """Translate segments one at a time."""
        results = []
        for i, text in enumerate(texts):
            logger.debug(
                "Translating segment %d/%d (%d chars)", i + 1, len(texts), len(text)
            )
            results.append(self.translate_text(text))
        return results

    def _split_long_paragraph(self, text: str) -> list[str]:
        """Split a long paragraph into sentences for better translation quality."""
        if len(text) <= _LONG_PARAGRAPH_THRESHOLD:
            return [text]

        sentences = _SENTENCE_SPLITTER.split(text)
        return [s.strip() for s in sentences if s.strip()]

    def translate_chapter(
        self, chinese_text: str, term_dict: dict | None = None
    ) -> str:
        """
        Translate an entire chapter's text.

        Steps:
        1. Split into paragraphs
        2. Split long paragraphs into sentences
        3. Translate all segments
        4. Reassemble into paragraphs
        5. Apply term dictionary post-processing
        6. Annotate Chinese names for TTS
        """
        if not chinese_text or not chinese_text.strip():
            return ""

        paragraphs = chinese_text.split("\n")

        # Build a flat list of segments to translate, tracking which paragraph
        # each belongs to so we can reassemble later.
        segments: list[str] = []
        paragraph_map: list[list[int]] = []  # paragraph_map[i] = [segment_indices]

        for para in paragraphs:
            stripped = para.strip()
            if not stripped:
                paragraph_map.append([])
                continue

            sub_segments = self._split_long_paragraph(stripped)
            indices = []
            for seg in sub_segments:
                indices.append(len(segments))
                segments.append(seg)
            paragraph_map.append(indices)

        if not segments:
            return ""

        logger.info(
            "Translating chapter: %d paragraphs, %d segments",
            len(paragraphs),
            len(segments),
        )
        translated_segments = self._translate_all(segments)

        # Reassemble into paragraphs
        translated_paragraphs = []
        for indices in paragraph_map:
            if not indices:
                translated_paragraphs.append("")
            else:
                translated_paragraphs.append(
                    " ".join(translated_segments[i] for i in indices)
                )

        full_translation = "\n".join(translated_paragraphs)

        # Post-processing
        if term_dict:
            full_translation = apply_term_dictionary(full_translation, term_dict)
            full_translation = annotate_chinese_names(full_translation, term_dict)

        return full_translation


# --------------- Module-level singleton ---------------

_translator_instance: "Translator | None" = None


def get_translator() -> Translator:
    """Get or create the module-level Translator singleton."""
    global _translator_instance
    if _translator_instance is None:
        _translator_instance = Translator()
        _translator_instance.load_model()
    return _translator_instance


# --------------- Post-processing helpers ---------------


def apply_term_dictionary(text: str, term_dict: dict) -> str:
    """
    Replace inconsistent translations with preferred terms.
    Operates on the English output — replaces Chinese characters that the model
    may have left untranslated, using the term dictionary mappings.
    """
    if not term_dict:
        return text

    # Collect all Chinese->English mappings from all categories.
    # Sort longest-first to avoid partial matches (e.g. "天剑宗" before "天").
    replacements: list[tuple[str, str]] = []
    for category in term_dict.values():
        if isinstance(category, dict):
            for chinese, english in category.items():
                replacements.append((chinese, english))

    replacements.sort(key=lambda pair: len(pair[0]), reverse=True)

    for chinese, english in replacements:
        text = text.replace(chinese, english)

    return text


def annotate_chinese_names(text: str, term_dict: dict) -> str:
    """
    Add pronunciation hints for Chinese names so the TTS engine
    can switch to Chinese phonemes for proper nouns.

    Format: "Lin Mo {{zh:林墨}}"
    """
    characters = term_dict.get("characters", {})
    if not characters:
        return text

    for chinese, english in characters.items():
        marker = f"{english} {{{{zh:{chinese}}}}}"
        if marker not in text:
            text = text.replace(english, marker)

    return text
