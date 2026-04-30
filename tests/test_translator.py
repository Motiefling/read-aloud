"""Tests for the translation pipeline."""

import torch
from unittest.mock import MagicMock

import pytest

from app.pipeline.translator import (
    Translator,
    TranslationError,
)


def _make_mock_translator():
    """Create a Translator with mocked model and tokenizer."""
    t = Translator()
    t._model = MagicMock()
    t._tokenizer = MagicMock()

    # Mock model.device
    t._model.device = torch.device("cpu")

    # Mock tokenizer.apply_chat_template: returns a string
    t._tokenizer.apply_chat_template.return_value = "<|im_start|>system\n...<|im_end|>\n<|im_start|>user\n...<|im_end|>\n<|im_start|>assistant\n"

    # Mock tokenizer __call__: returns dict with input_ids tensor
    mock_inputs = MagicMock()
    mock_inputs.__getitem__ = lambda self, key: torch.tensor([[1, 2, 3]]) if key == "input_ids" else None
    mock_inputs.to.return_value = {"input_ids": torch.tensor([[1, 2, 3]])}
    t._tokenizer.return_value = mock_inputs

    # Mock model.generate: returns tokens including the input prefix + generated
    t._model.generate.return_value = torch.tensor([[1, 2, 3, 100, 200, 2]])

    # Mock decode: returns "translated"
    t._tokenizer.decode.return_value = "translated"

    return t


class TestTranslator:
    """Tests for the Qwen translation pipeline."""

    def test_translate_simple_text(self):
        """Test basic translation flow with mocked model."""
        t = _make_mock_translator()
        result = t.translate_text("你好世界")
        assert result == "translated"
        t._model.generate.assert_called_once()

    def test_translate_empty_text(self):
        """Test that empty text returns empty string without calling model."""
        t = _make_mock_translator()
        result = t.translate_text("")
        assert result == ""
        t._model.generate.assert_not_called()

    def test_translate_chapter_empty(self):
        """Test that empty chapter returns empty string."""
        t = _make_mock_translator()
        assert t.translate_chapter("") == ""
        assert t.translate_chapter("   ") == ""

    def test_translate_chapter_calls_model(self):
        """Test that translate_chapter routes through Qwen for non-empty text."""
        t = _make_mock_translator()
        t._tokenizer.decode.return_value = "Hello world"
        result = t.translate_chapter("一些中文")
        assert "Hello world" in result
        t._model.generate.assert_called()

    def test_model_not_loaded_raises(self):
        """Test that calling translate without loading model raises error."""
        t = Translator()
        with pytest.raises(TranslationError, match="Model not loaded"):
            t.translate_text("你好")

    def test_traditional_chinese_converted(self):
        """Test that Traditional Chinese is converted to Simplified before translation."""
        t = _make_mock_translator()
        # The OpenCC converter is real (not mocked), so it will convert
        t.translate_text("開門")
        # Verify the chat template was called (model was invoked)
        t._tokenizer.apply_chat_template.assert_called_once()


class TestChineseDetect:
    """Tests for Chinese character detection utilities."""

    def test_contains_chinese_true(self):
        """Test detection of Chinese characters in mixed text."""
        from app.utils.chinese_detect import contains_chinese

        assert contains_chinese("Hello 林墨 World") is True

    def test_contains_chinese_false(self):
        """Test that pure English returns False."""
        from app.utils.chinese_detect import contains_chinese

        assert contains_chinese("Hello World") is False

    def test_extract_chinese_segments(self):
        """Test extraction of Chinese character runs from mixed text."""
        from app.utils.chinese_detect import extract_chinese_segments

        segments = extract_chinese_segments("Hello 林墨 said 你好")
        assert len(segments) == 2
        assert segments[0][2] == "林墨"
        assert segments[1][2] == "你好"
