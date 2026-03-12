"""
Audio processing utilities using FFmpeg.

Handles audio concatenation, format conversion, and speed adjustment.
"""

from pathlib import Path


def concatenate_audio(chunk_paths: list[Path], output_path: Path, pause_ms: int = 500) -> None:
    """
    Concatenate WAV chunks into a single MP3 with pauses between them.

    Uses FFmpeg concat demuxer to join audio files with silence gaps.
    """
    # TODO: Implement FFmpeg-based audio concatenation
    # - Generate a silence file for pauses
    # - Create a concat list file
    # - Run ffmpeg -f concat to produce final MP3
    raise NotImplementedError


def adjust_playback_speed(input_path: Path, output_path: Path, speed: float = 2.0) -> None:
    """
    Generate a speed-adjusted copy of an audio file using FFmpeg's atempo filter.
    Does not change pitch.
    """
    # TODO: Implement FFmpeg atempo speed adjustment
    # ffmpeg -i input.wav -filter:a "atempo=2.0" -vn output.wav
    raise NotImplementedError


def convert_to_mp3(input_path: Path, output_path: Path, quality: int = 2) -> None:
    """Convert an audio file to MP3 format using FFmpeg."""
    # TODO: Implement format conversion
    raise NotImplementedError


def get_audio_duration(file_path: Path) -> float:
    """Get the duration of an audio file in seconds using FFmpeg."""
    # TODO: Implement duration detection via ffprobe
    raise NotImplementedError
