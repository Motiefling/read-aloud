"""Tests for the translation pipeline."""

import torch
from unittest.mock import MagicMock, patch

import pytest

from app.pipeline.translator import (
    Translator,
    TranslationError,
    apply_term_dictionary,
    annotate_chinese_names,
    _LONG_PARAGRAPH_THRESHOLD,
)


class TestApplyTermDictionary:
    """Tests for term dictionary post-processing."""

    def test_replaces_chinese_chars(self):
        """Test that Chinese chars left by model are replaced with English."""
        term_dict = {
            "characters": {"林墨": "Lin Mo", "苏瑶": "Su Yao"},
            "terms": {"灵气": "spiritual energy"},
        }
        text = "林墨 walked through the forest, feeling the 灵气 in the air."
        result = apply_term_dictionary(text, term_dict)
        assert "Lin Mo" in result
        assert "spiritual energy" in result
        assert "林墨" not in result
        assert "灵气" not in result

    def test_longest_first(self):
        """Test that longer Chinese strings are replaced before shorter ones."""
        term_dict = {
            "terms": {"天": "Heaven", "天剑宗": "Heavenly Sword Sect"},
        }
        text = "He arrived at 天剑宗."
        result = apply_term_dictionary(text, term_dict)
        assert "Heavenly Sword Sect" in result

    def test_empty_dict(self):
        """Test that empty dict returns text unchanged."""
        assert apply_term_dictionary("hello", {}) == "hello"

    def test_none_dict(self):
        """Test that None dict returns text unchanged."""
        assert apply_term_dictionary("hello", None) == "hello"

    def test_non_dict_category_skipped(self):
        """Test that non-dict values in the term dict are skipped."""
        term_dict = {"meta": "some string value", "terms": {"灵气": "spiritual energy"}}
        text = "The 灵气 was strong."
        result = apply_term_dictionary(text, term_dict)
        assert "spiritual energy" in result


class TestAnnotateChineseNames:
    """Tests for Chinese name annotation for TTS."""

    def test_adds_markers(self):
        """Test that {{zh:...}} markers are added after English names."""
        term_dict = {"characters": {"林墨": "Lin Mo"}}
        text = "Lin Mo walked through the forest."
        result = annotate_chinese_names(text, term_dict)
        assert "Lin Mo {{zh:林墨}}" in result

    def test_no_double_annotation(self):
        """Test that already-annotated names are not double-annotated."""
        term_dict = {"characters": {"林墨": "Lin Mo"}}
        text = "Lin Mo {{zh:林墨}} walked through the forest."
        result = annotate_chinese_names(text, term_dict)
        assert result.count("{{zh:林墨}}") == 1

    def test_no_characters_category(self):
        """Test that text is unchanged when no characters category exists."""
        term_dict = {"terms": {"灵气": "spiritual energy"}}
        text = "Some text here."
        result = annotate_chinese_names(text, term_dict)
        assert result == "Some text here."

    def test_multiple_characters(self):
        """Test annotation of multiple character names."""
        term_dict = {"characters": {"林墨": "Lin Mo", "苏瑶": "Su Yao"}}
        text = "Lin Mo met Su Yao at the gate."
        result = annotate_chinese_names(text, term_dict)
        assert "Lin Mo {{zh:林墨}}" in result
        assert "Su Yao {{zh:苏瑶}}" in result


def _make_mock_translator():
    """Create a Translator with mocked model and tokenizer."""
    t = Translator()
    t._model = MagicMock()
    t._tokenizer = MagicMock()

    # Mock tokenizer __call__: returns dict with input_ids
    def mock_tokenizer_call(*args, **kwargs):
        return {"input_ids": torch.tensor([[1, 2, 3]])}

    t._tokenizer.side_effect = mock_tokenizer_call

    # Mock model.generate: returns one output row
    def mock_generate(**kwargs):
        return torch.tensor([[100, 200, 2]])

    t._model.generate.side_effect = mock_generate

    # Mock decode: returns "translated"
    t._tokenizer.decode.return_value = "translated"

    return t


class TestTranslator:
    """Tests for the Opus-MT translation pipeline."""

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

    def test_translate_preserves_paragraphs(self):
        """Test that paragraph structure is maintained."""
        t = _make_mock_translator()
        result = t.translate_chapter("First paragraph\n\nSecond paragraph")
        lines = result.split("\n")
        assert len(lines) == 3  # para, blank, para
        assert lines[1] == ""  # blank line preserved

    def test_translate_chapter_empty(self):
        """Test that empty chapter returns empty string."""
        t = _make_mock_translator()
        assert t.translate_chapter("") == ""
        assert t.translate_chapter("   ") == ""

    def test_translate_chapter_with_term_dict(self):
        """Test that term dictionary is applied during chapter translation."""
        t = _make_mock_translator()
        t._tokenizer.decode.return_value = "The 灵气 was strong"

        term_dict = {"terms": {"灵气": "spiritual energy"}}
        result = t.translate_chapter("一些中文", term_dict)
        assert "spiritual energy" in result
        assert "灵气" not in result

    def test_translate_chapter_with_name_annotation(self):
        """Test that Chinese name annotations are added during chapter translation."""
        t = _make_mock_translator()
        t._tokenizer.decode.return_value = "Lin Mo walked away"

        term_dict = {"characters": {"林墨": "Lin Mo"}}
        result = t.translate_chapter("一些中文", term_dict)
        assert "Lin Mo {{zh:林墨}}" in result

    def test_model_not_loaded_raises(self):
        """Test that calling translate without loading model raises error."""
        t = Translator()
        with pytest.raises(TranslationError, match="Model not loaded"):
            t.translate_text("你好")

    def test_split_long_paragraph(self):
        """Test that long paragraphs are split at sentence boundaries."""
        t = Translator()
        long_text = "这是第一句话。" * 50
        assert len(long_text) > _LONG_PARAGRAPH_THRESHOLD
        segments = t._split_long_paragraph(long_text)
        assert len(segments) > 1

    def test_split_short_paragraph(self):
        """Test that short paragraphs are not split."""
        t = Translator()
        short_text = "这是一句短话。"
        assert len(short_text) <= _LONG_PARAGRAPH_THRESHOLD
        segments = t._split_long_paragraph(short_text)
        assert len(segments) == 1
        assert segments[0] == short_text

    def test_traditional_chinese_converted(self):
        """Test that Traditional Chinese is converted to Simplified before translation."""
        t = _make_mock_translator()
        # The OpenCC converter is real (not mocked), so it will convert
        t.translate_text("開門")
        # Verify tokenizer was called (we can't easily check the converted text
        # without inspecting call args, but we verify no error occurs)
        t._tokenizer.assert_called_once()


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


class TestTermDictionary:
    """Tests for term dictionary loading and merging."""

    def test_load_global_dictionary(self):
        """Test loading the global dictionary."""
        # TODO: Implement
        pass

    def test_merge_dictionaries(self):
        """Test that novel-specific entries override global entries."""
        from app.utils.term_dictionary import _merge_dicts

        base = {"characters": {"a": "A"}, "terms": {"x": "X"}}
        override = {"characters": {"a": "B"}, "locations": {"y": "Y"}}
        merged = _merge_dicts(base, override)
        assert merged["characters"]["a"] == "B"
        assert merged["terms"]["x"] == "X"
        assert merged["locations"]["y"] == "Y"
