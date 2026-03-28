"""
Audio processing utilities using FFmpeg.

Handles format conversion, speed adjustment, and duration detection.
Audio concatenation with silence gaps is handled in-memory via numpy
in the TTS pipeline (tts.py) to avoid unnecessary disk I/O.
"""

import json
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def convert_to_mp3(input_path: Path, output_path: Path, quality: int = 2) -> None:
    """
    Convert an audio file to MP3 format using FFmpeg.

    Args:
        input_path: Source audio file (WAV, etc.)
        output_path: Destination MP3 file.
        quality: VBR quality level (0=best, 9=worst). Default 2 (~190kbps).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-codec:a", "libmp3lame",
        "-qscale:a", str(quality),
        "-vn",
        str(output_path),
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        logger.error("FFmpeg conversion failed: %s", result.stderr)
        raise RuntimeError(f"FFmpeg failed (exit {result.returncode}): {result.stderr[:500]}")


def adjust_playback_speed(input_path: Path, output_path: Path, speed: float = 2.0) -> None:
    """
    Generate a speed-adjusted copy of an audio file using FFmpeg's atempo filter.
    Does not change pitch. atempo range is 0.5–100.0; values outside 0.5–2.0
    are chained automatically.
    """
    # atempo only accepts 0.5..2.0 per filter, so chain multiple for larger values
    filters = []
    remaining = speed
    while remaining > 2.0:
        filters.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        filters.append("atempo=0.5")
        remaining /= 0.5
    filters.append(f"atempo={remaining:.4f}")

    filter_str = ",".join(filters)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-filter:a", filter_str,
        "-vn",
        str(output_path),
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        logger.error("FFmpeg speed adjustment failed: %s", result.stderr)
        raise RuntimeError(f"FFmpeg failed (exit {result.returncode}): {result.stderr[:500]}")


def get_audio_duration(file_path: Path) -> float:
    """Get the duration of an audio file in seconds using ffprobe."""
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        str(file_path),
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed (exit {result.returncode}): {result.stderr[:500]}")

    info = json.loads(result.stdout)
    return float(info["format"]["duration"])
