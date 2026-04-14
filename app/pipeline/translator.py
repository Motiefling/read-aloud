"""
Translation pipeline using Qwen 2.5 7B Instruct via HuggingFace Transformers.

Translates Chinese text to English using a chat-based LLM approach.
Traditional Chinese is automatically converted to Simplified via OpenCC
before translation.

Post-processing applies term dictionaries for consistent terminology
and annotates Chinese names for TTS pronunciation hints.
"""

import logging
import re

import torch
from opencc import OpenCC
from transformers import AutoModelForCausalLM, AutoTokenizer

from app.config import settings

logger = logging.getLogger(__name__)

# Rough token-to-character ratio for Chinese text. Used to estimate
# whether a chapter fits in a single inference call.
_MAX_CHAPTER_CHARS = 6000


class TranslationError(Exception):
    """Raised when translation fails."""


class Translator:
    """Handles Chinese-to-English translation using Qwen 2.5 7B Instruct."""

    def __init__(self, config=None):
        if config is None:
            config = settings.translation
        self.config = config
        self._model = None
        self._tokenizer = None
        self._cc = OpenCC("t2s")  # Traditional -> Simplified converter

    def load_model(self):
        """Load the Qwen model and tokenizer."""
        logger.info("Loading translation model: %s", self.config.model_path)
        try:
            self._tokenizer = AutoTokenizer.from_pretrained(
                self.config.model_path,
                trust_remote_code=True,
            )
            self._model = AutoModelForCausalLM.from_pretrained(
                self.config.model_path,
                dtype=torch.bfloat16,
                trust_remote_code=True,
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

    def _generate(self, user_message: str) -> str:
        """Run a single chat completion with the system prompt and user message."""
        import time

        messages = [
            {"role": "system", "content": self.config.system_prompt},
            {"role": "user", "content": user_message},
        ]
        text = self._tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
        inputs = self._tokenizer(text, return_tensors="pt").to(self._model.device)
        input_len = inputs["input_ids"].shape[1]

        logger.info("Translating %d chars (%d input tokens)...", len(user_message), input_len)
        start = time.time()

        with torch.no_grad():
            outputs = self._model.generate(
                **inputs,
                max_new_tokens=self.config.max_new_tokens,
                temperature=self.config.temperature,
                do_sample=self.config.temperature > 0,
                top_p=0.9,
            )

        output_tokens = outputs[0].shape[0] - input_len
        elapsed = time.time() - start
        logger.info(
            "Translation done: %d output tokens in %.1fs (%.0f tok/s)",
            output_tokens, elapsed, output_tokens / elapsed if elapsed > 0 else 0,
        )

        # Decode only the newly generated tokens
        result = self._tokenizer.decode(
            outputs[0][input_len:], skip_special_tokens=True
        ).strip()

        # Warn if Chinese leaked through
        from app.utils.chinese_detect import is_chinese_char
        chinese_count = sum(1 for c in result if is_chinese_char(c))
        if chinese_count > 0:
            logger.warning(
                "Translation output contains %d Chinese characters — may have untranslated text",
                chinese_count,
            )

        return result

    def translate_text(self, text: str) -> str:
        """Translate a single text segment from Chinese to English."""
        if self._model is None or self._tokenizer is None:
            raise TranslationError("Model not loaded. Call load_model() first.")

        if not text.strip():
            return ""

        simplified = self._cc.convert(text)

        try:
            return self._generate(simplified)
        except RuntimeError as e:
            raise TranslationError(f"Translation inference failed: {e}") from e

    def translate_title(self, title: str, term_dict: dict | None = None) -> str:
        """Translate a chapter title (short text, title-specific prompt)."""
        if not title or not title.strip():
            return title or ""
        logger.info("Translating title: %s", title[:60])
        translated = self._generate_title(title)
        logger.info("Title translated: %s", translated[:60])
        if term_dict:
            translated = apply_term_dictionary(translated, term_dict)
        return translated

    def _generate_title(self, title: str) -> str:
        """Translate a short title using a title-specific prompt and limited tokens."""
        import time

        system_prompt = (
            "You are a translator. Translate the given Chinese chapter title "
            "into English. Output ONLY the translated title, nothing else. "
            "Keep it concise. Preserve names in pinyin (e.g. 林墨 → Lin Mo)."
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": title},
        ]
        text = self._tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
        inputs = self._tokenizer(text, return_tensors="pt").to(self._model.device)
        input_len = inputs["input_ids"].shape[1]

        start = time.time()
        with torch.no_grad():
            outputs = self._model.generate(
                **inputs,
                max_new_tokens=64,
                temperature=0.1,
                do_sample=True,
                top_p=0.9,
            )
        elapsed = time.time() - start
        logger.info("Title translation done in %.1fs", elapsed)

        result = self._tokenizer.decode(
            outputs[0][input_len:], skip_special_tokens=True
        ).strip()
        # Take only the first line in case the model added extras
        result = result.split("\n")[0].strip()
        return result

    def translate_chapter(
        self, chinese_text: str, term_dict: dict | None = None
    ) -> str:
        """
        Translate an entire chapter's text.

        For chapters that fit within the model's context window, translates
        in a single pass to maintain coherence. Falls back to paragraph-by-
        paragraph translation for very long chapters.

        Steps:
        1. Convert Traditional -> Simplified Chinese
        2. Translate (single pass or chunked)
        3. Apply term dictionary post-processing
        4. Annotate Chinese names for TTS
        """
        if not chinese_text or not chinese_text.strip():
            return ""

        if self._model is None or self._tokenizer is None:
            raise TranslationError("Model not loaded. Call load_model() first.")

        simplified = self._cc.convert(chinese_text)
        logger.info(
            "Translating chapter: %d chars (%s)",
            len(simplified),
            "single pass" if len(simplified) <= _MAX_CHAPTER_CHARS else "chunked",
        )

        try:
            if len(simplified) <= _MAX_CHAPTER_CHARS:
                full_translation = self._generate(simplified)
            else:
                full_translation = self._translate_chunked(simplified)
        except RuntimeError as e:
            raise TranslationError(f"Translation inference failed: {e}") from e

        # Post-processing
        if term_dict:
            full_translation = apply_term_dictionary(full_translation, term_dict)
            full_translation = annotate_chinese_names(full_translation, term_dict)

        return full_translation

    def _translate_chunked(self, text: str) -> list[str]:
        """Translate long text by splitting into paragraph-sized chunks."""
        paragraphs = text.split("\n")
        chunk = []
        chunk_len = 0
        translated_parts = []

        for para in paragraphs:
            # If adding this paragraph would exceed the limit, flush the chunk
            if chunk and chunk_len + len(para) > _MAX_CHAPTER_CHARS:
                logger.info("Translating chunk %d (%d chars)", len(translated_parts) + 1, chunk_len)
                translated_parts.append(self._generate("\n".join(chunk)))
                chunk = []
                chunk_len = 0

            chunk.append(para)
            chunk_len += len(para)

        # Flush remaining
        if chunk:
            logger.info("Translating final chunk %d (%d chars)", len(translated_parts) + 1, chunk_len)
            translated_parts.append(self._generate("\n".join(chunk)))

        return "\n\n".join(translated_parts)


# --------------- Module-level singleton ---------------

_translator_instance: "Translator | None" = None


def get_translator() -> Translator:
    """Get or create the module-level Translator singleton."""
    global _translator_instance
    if _translator_instance is None:
        translator = Translator()
        translator.load_model()
        _translator_instance = translator
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
