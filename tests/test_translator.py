"""Tests for the translation pipeline."""

import pytest


class TestTranslator:
    """Tests for the NLLB translation pipeline."""

    def test_translate_simple_text(self):
        """Test basic Chinese to English translation."""
        # TODO: Implement (requires model or mock)
        pass

    def test_translate_preserves_paragraphs(self):
        """Test that paragraph structure is maintained."""
        # TODO: Implement
        pass

    def test_apply_term_dictionary(self):
        """Test that term dictionary replacements are applied correctly."""
        # TODO: Implement
        pass

    def test_annotate_chinese_names(self):
        """Test that Chinese names get TTS pronunciation annotations."""
        # TODO: Implement
        pass

    def test_term_dictionary_overrides_translation(self):
        """Test that dictionary terms take precedence over raw translation."""
        # TODO: Implement
        pass


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
