"""Tests for the TTS pipeline."""

import pytest


class TestTTSEngine:
    """Tests for the TTS engine interface."""

    def test_placeholder_tts_raises(self):
        """Test that the placeholder engine raises NotImplementedError."""
        from pathlib import Path
        from app.pipeline.tts import PlaceholderTTS
        engine = PlaceholderTTS()
        with pytest.raises(NotImplementedError):
            engine.synthesize("Hello world", Path("/tmp/test.wav"))

    def test_split_into_utterances(self):
        """Test that text is split into sentence-sized chunks."""
        # TODO: Implement once split_into_utterances is implemented
        pass

    def test_generate_chapter_audio(self):
        """Test the full chapter audio generation pipeline."""
        # TODO: Implement with mock TTS engine
        pass


class TestAudioProcessing:
    """Tests for FFmpeg audio processing utilities."""

    def test_concatenate_audio(self):
        """Test audio concatenation with pauses."""
        # TODO: Implement (requires test WAV files)
        pass

    def test_adjust_playback_speed(self):
        """Test playback speed adjustment."""
        # TODO: Implement
        pass

    def test_get_audio_duration(self):
        """Test audio duration detection."""
        # TODO: Implement
        pass
