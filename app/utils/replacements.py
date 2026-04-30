"""
Find/replace helpers for the user-managed replacement system.

There are two kinds of replacements:

* **Pre-translation** rules operate on the raw Chinese chapter text BEFORE it
  is sent to Qwen.  Plain longest-first substring substitution -- "whole word"
  is meaningless for Chinese, and case-insensitivity is harmless because
  Chinese has no case.

* **Post-translation** rules operate on Qwen's English output BEFORE Kokoro
  sees it.  These match whole words, case-insensitively, so a rule like
  ``Sissy -> Ceci`` does not also rewrite ``classy``.

A stable SHA-1 of the rule list is stored on each chapter so the UI can flag
audio that was generated against an older rule set.
"""

from __future__ import annotations

import hashlib
import re
from typing import Iterable


Rule = tuple[str, str]  # (find_text, replace_text)


def _is_word_char(ch: str) -> bool:
    """True for chars that should anchor a word boundary on whole-word match."""
    return ch.isalnum() or ch == "_"


def apply_pre_replacements(text: str, rules: Iterable[Rule]) -> str:
    """Apply Chinese-source replacements (plain substring, longest-first).

    Used to inject English placeholders into Chinese text so Qwen passes them
    through verbatim (e.g. 風水 -> "feng shui").  Replacement is
    case-insensitive in case the find text contains Latin characters.
    """
    rule_list = [(f, r) for (f, r) in rules if f]
    rule_list.sort(key=lambda fr: len(fr[0]), reverse=True)
    for find, replace in rule_list:
        text = re.sub(re.escape(find), lambda _m, r=replace: r, text, flags=re.IGNORECASE)
    return text


def apply_post_replacements(text: str, rules: Iterable[Rule]) -> str:
    """Apply English-target replacements (whole-word, case-insensitive).

    Word boundaries are only required at edges where the find text starts/ends
    with a word character -- that lets users write rules like ``"feng shui"``
    (multiword) or ``"Mr."`` (trailing punctuation) without manual escaping.
    """
    rule_list = [(f, r) for (f, r) in rules if f]
    rule_list.sort(key=lambda fr: len(fr[0]), reverse=True)
    for find, replace in rule_list:
        prefix = r"\b" if _is_word_char(find[0]) else ""
        suffix = r"\b" if _is_word_char(find[-1]) else ""
        pattern = prefix + re.escape(find) + suffix
        text = re.sub(pattern, lambda _m, r=replace: r, text, flags=re.IGNORECASE)
    return text


def hash_rules(rules: Iterable[Rule]) -> str:
    """Stable hash of a rule list, used for staleness detection.

    The hash ignores the order rules were added in -- only the resulting
    set of (find, replace) pairs matters.  An empty rule list hashes to a
    fixed sentinel so chapters processed with no rules can be compared too.
    """
    canonical = sorted(f"{f}\x1f{r}" for f, r in rules)
    h = hashlib.sha1()
    for entry in canonical:
        h.update(entry.encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()
