"""
Translation pipeline using NLLB 1.3B via CTranslate2.

Translates Chinese text to English with post-processing
for consistent terminology via term dictionaries.
"""

from app.config import settings


class Translator:
    """Handles Chinese-to-English translation using NLLB via CTranslate2."""

    def __init__(self, config=None):
        if config is None:
            config = settings.translation
        self.config = config
        self._model = None
        self._tokenizer = None

    def load_model(self):
        """Load the CTranslate2 model and SentencePiece tokenizer."""
        # TODO: Load model and tokenizer
        # self._model = ctranslate2.Translator(self.config.model_path, device=self.config.device)
        # self._tokenizer = spm.SentencePieceProcessor(self.config.tokenizer_path)
        raise NotImplementedError

    def translate_text(self, chinese_text: str) -> str:
        """Translate a single text segment from Chinese to English."""
        # TODO: Tokenize, translate via CTranslate2, decode
        raise NotImplementedError

    def translate_chapter(self, chinese_text: str, term_dict: dict | None = None) -> str:
        """
        Translate an entire chapter's text.

        Steps:
        1. Split into paragraphs
        2. Translate each paragraph
        3. Apply term dictionary post-processing
        4. Annotate Chinese names for TTS
        """
        # TODO: Implement full chapter translation pipeline
        raise NotImplementedError


def apply_term_dictionary(text: str, term_dict: dict) -> str:
    """
    Replace inconsistent translations with preferred terms.
    Operates on the English output.
    """
    # TODO: Implement term dictionary replacement
    raise NotImplementedError


def annotate_chinese_names(text: str, term_dict: dict) -> str:
    """
    Add pronunciation hints for Chinese names so the TTS engine
    can switch to Chinese phonemes for proper nouns.
    """
    # TODO: Implement Chinese name annotation
    raise NotImplementedError
