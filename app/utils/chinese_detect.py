"""
Utility functions for detecting and extracting Chinese characters from text.
"""


def contains_chinese(text: str) -> bool:
    """Check if text contains any Chinese characters."""
    for char in text:
        cp = ord(char)
        if (0x4E00 <= cp <= 0x9FFF or      # CJK Unified Ideographs
            0x3400 <= cp <= 0x4DBF or       # CJK Extension A
            0x20000 <= cp <= 0x2A6DF or     # CJK Extension B
            0xF900 <= cp <= 0xFAFF):        # CJK Compatibility Ideographs
            return True
    return False


def is_chinese_char(char: str) -> bool:
    """Check if a single character is a Chinese character."""
    cp = ord(char)
    return (0x4E00 <= cp <= 0x9FFF or
            0x3400 <= cp <= 0x4DBF or
            0x20000 <= cp <= 0x2A6DF or
            0xF900 <= cp <= 0xFAFF)


def extract_chinese_segments(text: str) -> list[tuple[int, int, str]]:
    """
    Find runs of Chinese characters in mixed text.

    Returns:
        List of (start_index, end_index, chinese_text) tuples.
        Useful for the TTS engine to know where to switch to Chinese phonemes.
    """
    segments = []
    in_chinese = False
    start = 0

    for i, char in enumerate(text):
        if is_chinese_char(char) and not in_chinese:
            start = i
            in_chinese = True
        elif not is_chinese_char(char) and in_chinese:
            segments.append((start, i, text[start:i]))
            in_chinese = False

    if in_chinese:
        segments.append((start, len(text), text[start:]))

    return segments
