"""Tests for the find/replace helpers."""

from app.utils.replacements import (
    apply_pre_replacements,
    apply_post_replacements,
    hash_rules,
)


class TestApplyPreReplacements:
    """Pre-translation rules: plain longest-first substring on Chinese text."""

    def test_basic_chinese_substitution(self):
        rules = [("風水", "feng shui"), ("水", "sun")]
        result = apply_pre_replacements("這是風水之術。", rules)
        assert "feng shui" in result
        assert "風水" not in result

    def test_longest_first(self):
        """Longer rules win so prefixes don't shadow them."""
        rules = [("天", "Heaven"), ("天劍宗", "Heavenly Sword Sect")]
        result = apply_pre_replacements("到了天劍宗。", rules)
        assert "Heavenly Sword Sect" in result
        # "天" inside the longer match must not be re-substituted
        assert "Heaven Sword" not in result

    def test_empty_rules(self):
        assert apply_pre_replacements("hello", []) == "hello"

    def test_skips_empty_find(self):
        rules = [("", "x"), ("a", "z")]
        assert apply_pre_replacements("abc", rules) == "zbc"

    def test_case_insensitive_for_latin_finds(self):
        rules = [("API", "interface")]
        assert apply_pre_replacements("the api works", rules) == "the interface works"


class TestApplyPostReplacements:
    """Post-translation rules: whole-word, case-insensitive on English text."""

    def test_whole_word_match(self):
        rules = [("Sissy", "Ceci")]
        # mid-word should not match
        assert apply_post_replacements("classy thing", rules) == "classy thing"
        # whole word should match
        assert apply_post_replacements("Sissy went home", rules) == "Ceci went home"

    def test_case_insensitive(self):
        rules = [("sissy", "Ceci")]
        assert apply_post_replacements("SISSY arrived", rules) == "Ceci arrived"

    def test_multiword_find(self):
        rules = [("feng shui", "fĕng shwā'")]
        result = apply_post_replacements("a feng shui master", rules)
        assert "fĕng shwā'" in result

    def test_punctuation_anchored_find(self):
        rules = [("Mr.", "Mister")]
        # The trailing "." is non-word, so no \b suffix; should still match.
        assert apply_post_replacements("Mr. Lee", rules) == "Mister Lee"

    def test_longest_first(self):
        rules = [("Lin", "Lyn"), ("Lin Mo", "Linmo")]
        assert apply_post_replacements("Lin Mo walked", rules) == "Linmo walked"

    def test_empty_rules(self):
        assert apply_post_replacements("hello", []) == "hello"


class TestHashRules:
    """The rule-set hash drives the staleness flag in the chapter response."""

    def test_order_independent(self):
        a = [("x", "1"), ("y", "2")]
        b = [("y", "2"), ("x", "1")]
        assert hash_rules(a) == hash_rules(b)

    def test_changes_on_edit(self):
        a = [("x", "1")]
        b = [("x", "2")]
        assert hash_rules(a) != hash_rules(b)

    def test_empty_is_stable(self):
        assert hash_rules([]) == hash_rules([])

    def test_distinguishes_find_vs_replace(self):
        # The unit separator inside the canonical form must prevent collisions
        # like "ab" / "" colliding with "a" / "b".
        a = [("ab", "")]
        b = [("a", "b")]
        assert hash_rules(a) != hash_rules(b)
