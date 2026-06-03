"""
Translation pipeline using Qwen 2.5 7B Instruct via HuggingFace Transformers.

Translates Chinese text to English using a chat-based LLM approach.
Traditional Chinese is automatically converted to Simplified via OpenCC
before translation.

User-managed find/replace rules are applied around this pipeline (pre-rules
on the Chinese before Qwen, post-rules on the English before Kokoro).  See
``app/utils/replacements.py``.  Any Chinese characters Qwen leaves behind
get one more pass through Opus-MT as a safety net.
"""

import logging
import re

import torch
from opencc import OpenCC
from transformers import AutoModelForCausalLM, AutoTokenizer, MarianMTModel, MarianTokenizer

from app.config import settings
from app.utils.chinese_detect import is_chinese_char, extract_chinese_segments

logger = logging.getLogger(__name__)

# Max Chinese characters per translation chunk.  Keeping this moderate
# reduces the chance of Qwen hallucinating on long context.
_MAX_CHAPTER_CHARS = 3000

# Chinese sentence-ending punctuation used for splitting long paragraphs.
_SENTENCE_ENDINGS = re.compile(r'(?<=[。！？；…」』])')

# Opus-MT model for fallback translation of Chinese that Qwen misses
_OPUS_MT_MODEL_NAME = "Helsinki-NLP/opus-mt-zh-en"

# Pinyin tone-mark vowels.  If Qwen output is dense with these it has slipped
# into "romanize the chapter" mode instead of translating — see
# ``_is_pinyin_output``.
_PINYIN_DIACRITICS = "āēīōūǖáéíóúǘǎěǐǒǔǚàèìòùǜ"
_PINYIN_DIACRITIC_RE = re.compile(f"[{_PINYIN_DIACRITICS}]")
_PINYIN_DENSITY_THRESHOLD = 0.01  # >1% diacritics ⇒ treat as pinyin


def _pinyin_density(text: str) -> float:
    """Fraction of characters in ``text`` that are pinyin tone-mark vowels.

    Used to detect when Qwen has slipped into "romanize the chapter" mode
    instead of translating.  Real English narrative occasionally carries a
    diacritic (a romanized name, a borrowed word), so callers flag only when
    density crosses ``_PINYIN_DENSITY_THRESHOLD`` (~1%).
    """
    if not text:
        return 0.0
    return len(_PINYIN_DIACRITIC_RE.findall(text)) / len(text)


def _split_long_paragraph(text: str, max_chars: int) -> list[str]:
    """Split a long paragraph into pieces on Chinese sentence boundaries.

    Tries sentence-ending punctuation first. Falls back to splitting on
    any Chinese punctuation, and ultimately on a hard character limit.
    """
    if len(text) <= max_chars:
        return [text]

    # Split on sentence-ending punctuation
    sentences = _SENTENCE_ENDINGS.split(text)
    # Filter empty strings from the split
    sentences = [s for s in sentences if s.strip()]

    if len(sentences) <= 1:
        # No sentence boundaries found — hard split at max_chars
        pieces = []
        for i in range(0, len(text), max_chars):
            pieces.append(text[i:i + max_chars])
        return pieces

    # Recombine sentences into chunks that fit within max_chars
    pieces = []
    current = ""
    for sentence in sentences:
        if current and len(current) + len(sentence) > max_chars:
            pieces.append(current)
            current = sentence
        else:
            current += sentence
    if current:
        pieces.append(current)

    return pieces


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

    def _generate(self, user_message: str, *, temperature: float | None = None) -> str:
        """Run a single chat completion with the system prompt and user message."""
        import time

        temp: float = self.config.temperature if temperature is None else temperature

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
                temperature=temp,
                do_sample=temp > 0,
                top_p=0.9,
                repetition_penalty=1.15,
                no_repeat_ngram_size=6,
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

    def translate_title(self, title: str) -> str:
        """Translate a chapter title (short text, title-specific prompt)."""
        if not title or not title.strip():
            return title or ""
        logger.info("Translating title: %s", title[:60])
        translated = self._generate_title(title)
        logger.info("Title translated: %s", translated[:60])
        return translated

    def _generate_title(self, title: str) -> str:
        """Translate a short title using a title-specific prompt and limited tokens."""
        import time

        system_prompt = (
            "You are a translator. Translate the given Chinese chapter title "
            "into English. Output ONLY the translated title, nothing else. "
            "Keep it concise. Preserve names in pinyin."
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

    def translate_chapter(self, chinese_text: str) -> str:
        """
        Translate an entire chapter's text.

        For chapters that fit within the model's context window, translates
        in a single pass to maintain coherence. Falls back to paragraph-by-
        paragraph translation for very long chapters.

        Steps:
        1. Convert Traditional -> Simplified Chinese
        2. Translate (single pass or chunked)
        3. Run Opus-MT fallback on any Chinese characters Qwen left behind

        Pre-translation user rules should be applied to ``chinese_text`` by
        the caller; post-translation user rules are applied later (between
        this function and TTS).
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
                density = _pinyin_density(full_translation)
                if density > _PINYIN_DENSITY_THRESHOLD:
                    logger.warning(
                        "PINYIN DETECTED: %.2f%% diacritic density in Qwen "
                        "output (threshold %.2f%%, len=%d). Retrying at "
                        "temperature=0.9. Snippet: %r",
                        density * 100,
                        _PINYIN_DENSITY_THRESHOLD * 100,
                        len(full_translation),
                        full_translation[:200],
                    )
                    full_translation = self._generate(simplified, temperature=0.9)
                    retry_density = _pinyin_density(full_translation)
                    if retry_density > _PINYIN_DENSITY_THRESHOLD:
                        logger.error(
                            "PINYIN RETRY FAILED: %.2f%% diacritic density on "
                            "second attempt. Snippet: %r",
                            retry_density * 100,
                            full_translation[:200],
                        )
                        raise TranslationError(
                            f"Qwen produced pinyin instead of English on both "
                            f"attempts (densities {density:.1%} then "
                            f"{retry_density:.1%}); chapter cannot be translated."
                        )
                    logger.info(
                        "PINYIN RETRY OK: density dropped to %.2f%%",
                        retry_density * 100,
                    )
            else:
                full_translation = self._translate_chunked(simplified)
        except RuntimeError as e:
            raise TranslationError(f"Translation inference failed: {e}") from e

        remaining_chinese = sum(1 for c in full_translation if is_chinese_char(c))
        if remaining_chinese > 0:
            logger.info(
                "Chapter still has %d Chinese chars after Qwen — running Opus-MT fallback",
                remaining_chinese,
            )
            full_translation = fallback_translate_chinese(full_translation)

        return full_translation

    def _translate_chunked(self, text: str) -> str:
        """Translate long text by splitting into paragraph-sized chunks.

        Paragraphs that exceed ``_MAX_CHAPTER_CHARS`` on their own are
        further split on Chinese sentence-ending punctuation so that each
        piece stays within the limit.
        """
        paragraphs = text.split("\n")
        chunk: list[str] = []
        chunk_len = 0
        translated_parts: list[str] = []

        def _flush():
            nonlocal chunk, chunk_len
            if not chunk:
                return
            chunk_text = "\n".join(chunk)
            chunk_idx = len(translated_parts) + 1
            logger.info("Translating chunk %d (%d chars)", chunk_idx, chunk_len)
            translated = self._generate(chunk_text)
            density = _pinyin_density(translated)
            if density > _PINYIN_DENSITY_THRESHOLD:
                logger.warning(
                    "PINYIN DETECTED on chunk %d: %.2f%% diacritic density "
                    "(threshold %.2f%%, len=%d). Retrying at temperature=0.9. "
                    "Snippet: %r",
                    chunk_idx,
                    density * 100,
                    _PINYIN_DENSITY_THRESHOLD * 100,
                    len(translated),
                    translated[:200],
                )
                translated = self._generate(chunk_text, temperature=0.9)
                retry_density = _pinyin_density(translated)
                if retry_density > _PINYIN_DENSITY_THRESHOLD:
                    logger.error(
                        "PINYIN RETRY FAILED on chunk %d: %.2f%% diacritic "
                        "density on second attempt. Snippet: %r",
                        chunk_idx,
                        retry_density * 100,
                        translated[:200],
                    )
                    raise TranslationError(
                        f"Qwen produced pinyin instead of English on chunk "
                        f"{chunk_idx} on both attempts (densities "
                        f"{density:.1%} then {retry_density:.1%})."
                    )
                logger.info(
                    "PINYIN RETRY OK on chunk %d: density dropped to %.2f%%",
                    chunk_idx, retry_density * 100,
                )
            translated_parts.append(translated)
            chunk = []
            chunk_len = 0

        for para in paragraphs:
            # Split oversized paragraphs on Chinese sentence boundaries
            pieces = _split_long_paragraph(para, _MAX_CHAPTER_CHARS) if len(para) > _MAX_CHAPTER_CHARS else [para]

            for piece in pieces:
                if chunk and chunk_len + len(piece) > _MAX_CHAPTER_CHARS:
                    _flush()
                chunk.append(piece)
                chunk_len += len(piece)

        _flush()
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


# --------------- Opus-MT fallback ---------------

_opus_model: "MarianMTModel | None" = None
_opus_tokenizer: "MarianTokenizer | None" = None


def _get_opus_mt():
    """Lazy-load the Opus-MT zh→en model (used only when Qwen leaks Chinese)."""
    global _opus_model, _opus_tokenizer
    if _opus_model is None:
        logger.info("Loading Opus-MT fallback model: %s", _OPUS_MT_MODEL_NAME)
        _opus_tokenizer = MarianTokenizer.from_pretrained(_OPUS_MT_MODEL_NAME)
        _opus_model = MarianMTModel.from_pretrained(_OPUS_MT_MODEL_NAME)
        _opus_model.eval()
        logger.info("Opus-MT fallback model loaded")
    return _opus_model, _opus_tokenizer


def fallback_translate_chinese(text: str) -> str:
    """
    Find runs of Chinese characters in text and translate them via Opus-MT.

    Operates on the Qwen output — finds contiguous Chinese segments,
    translates each one, and splices the English back into position.
    This is a safety net, not a primary translator.
    """
    segments = extract_chinese_segments(text)
    if not segments:
        return text

    model, tokenizer = _get_opus_mt()

    # Collect unique Chinese strings to translate (avoid duplicates)
    unique_chinese = list({seg[2] for seg in segments})
    logger.info(
        "Opus-MT fallback: translating %d unique Chinese segment(s)", len(unique_chinese)
    )

    translations = {}
    for zh_text in unique_chinese:
        inputs = tokenizer(zh_text, return_tensors="pt", padding=True, truncation=True)
        with torch.no_grad():
            output_ids = model.generate(**inputs, max_new_tokens=128, max_length=None)
        en_text = tokenizer.decode(output_ids[0], skip_special_tokens=True).strip()
        translations[zh_text] = en_text
        logger.debug("Opus-MT: '%s' → '%s'", zh_text, en_text)

    # Replace Chinese segments in reverse order to preserve positions
    result = text
    for start, end, zh_text in reversed(segments):
        result = result[:start] + translations[zh_text] + result[end:]

    return result
