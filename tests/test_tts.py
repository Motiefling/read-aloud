"""Tests for the TTS pipeline."""

import numpy as np
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestPrepareForTTS:
    """Tests for text preparation / annotation stripping."""

    def test_strips_chinese_annotations(self):
        from app.pipeline.tts import prepare_for_tts
        text = "Lin Mo {{zh:林墨}} walked down the street."
        assert prepare_for_tts(text) == "Lin Mo walked down the street."

    def test_strips_multiple_annotations(self):
        from app.pipeline.tts import prepare_for_tts
        text = "Lin Mo {{zh:林墨}} met Zhao Yu {{zh:趙宇}} at the gate."
        assert prepare_for_tts(text) == "Lin Mo met Zhao Yu at the gate."

    def test_no_annotations_unchanged(self):
        from app.pipeline.tts import prepare_for_tts
        text = "A normal sentence with no annotations."
        assert prepare_for_tts(text) == text

    def test_empty_text(self):
        from app.pipeline.tts import prepare_for_tts
        assert prepare_for_tts("") == ""


class TestSplitIntoUtterances:
    """Tests for utterance splitting."""

    def test_splits_on_newlines(self):
        from app.pipeline.tts import split_into_utterances
        text = "First paragraph.\n\nSecond paragraph.\n\nThird."
        result = split_into_utterances(text)
        assert result == ["First paragraph.", "Second paragraph.", "Third."]

    def test_skips_empty_lines(self):
        from app.pipeline.tts import split_into_utterances
        text = "\n\n\nOnly content here.\n\n\n"
        result = split_into_utterances(text)
        assert result == ["Only content here."]

    def test_splits_long_paragraph(self):
        from app.pipeline.tts import split_into_utterances, _MAX_UTTERANCE_LENGTH
        # Build a paragraph longer than the threshold
        long_para = "This is a sentence. " * 30  # ~600 chars
        assert len(long_para) > _MAX_UTTERANCE_LENGTH
        result = split_into_utterances(long_para)
        assert len(result) > 1
        for chunk in result:
            assert len(chunk) <= _MAX_UTTERANCE_LENGTH + 50  # allow some margin

    def test_empty_text_returns_empty(self):
        from app.pipeline.tts import split_into_utterances
        assert split_into_utterances("") == []
        assert split_into_utterances("\n\n\n") == []


class TestKokoroTTS:
    """Tests for the Kokoro TTS engine (mocked)."""

    def test_synthesize_to_array_not_loaded_raises(self):
        from app.pipeline.tts import KokoroTTS, TTSError
        engine = KokoroTTS()
        with pytest.raises(TTSError, match="not loaded"):
            engine.synthesize_to_array("Hello")

    def test_synthesize_to_array_empty_text(self):
        from app.pipeline.tts import KokoroTTS
        engine = KokoroTTS()
        engine._pipeline = MagicMock()  # pretend loaded
        result = engine.synthesize_to_array("")
        assert len(result) == 0

    @patch("app.pipeline.tts.settings")
    def test_synthesize_to_array_calls_pipeline(self, mock_settings):
        import torch
        from app.pipeline.tts import KokoroTTS

        mock_settings.tts.voice = "af_heart"
        mock_settings.tts.speed = 1.0

        engine = KokoroTTS()
        engine._pipeline = MagicMock()

        # Simulate pipeline yielding one result with audio
        mock_result = MagicMock()
        mock_result.audio = torch.randn(24000)  # 1 second of audio
        engine._pipeline.return_value = iter([mock_result])

        audio = engine.synthesize_to_array("Hello world.")
        assert isinstance(audio, np.ndarray)
        assert audio.dtype == np.float32
        assert len(audio) == 24000

    @patch("app.pipeline.tts.settings")
    def test_synthesize_no_output_raises(self, mock_settings):
        from app.pipeline.tts import KokoroTTS, TTSError

        mock_settings.tts.voice = "af_heart"
        mock_settings.tts.speed = 1.0

        engine = KokoroTTS()
        engine._pipeline = MagicMock()

        # Simulate pipeline yielding a result with no audio
        mock_result = MagicMock()
        mock_result.audio = None
        engine._pipeline.return_value = iter([mock_result])

        with pytest.raises(TTSError, match="no audio"):
            engine.synthesize_to_array("Hello world.")


class TestGenerateChapterAudio:
    """Tests for full chapter audio generation."""

    @patch("app.pipeline.tts.get_tts_engine")
    @patch("app.pipeline.audio_processing.convert_to_mp3")
    @patch("app.pipeline.tts.sf")
    @patch("app.pipeline.tts.settings")
    def test_generates_and_converts(self, mock_settings, mock_sf, mock_convert, mock_get_engine, tmp_path):
        from app.pipeline.tts import generate_chapter_audio

        mock_settings.tts.pause_between_paragraphs_ms = 500
        mock_settings.tts.voice = "af_heart"
        mock_settings.tts.speed = 1.0

        mock_engine = MagicMock()
        mock_engine.synthesize_to_array.return_value = np.ones(24000, dtype=np.float32)

        result = generate_chapter_audio(
            english_text="First paragraph.\n\nSecond paragraph.",
            tts_engine=mock_engine,
            output_dir=tmp_path,
            novel_id="test-novel",
            chapter_number=1,
        )

        assert mock_engine.synthesize_to_array.call_count == 2
        assert mock_sf.write.called
        assert mock_convert.called
        assert "chapter_0001.mp3" in str(result)


class TestAudioProcessing:
    """Tests for FFmpeg audio processing utilities."""

    @patch("app.pipeline.audio_processing.subprocess.run")
    def test_convert_to_mp3(self, mock_run, tmp_path):
        from app.pipeline.audio_processing import convert_to_mp3

        mock_run.return_value = MagicMock(returncode=0)
        input_path = tmp_path / "test.wav"
        output_path = tmp_path / "test.mp3"
        input_path.touch()

        convert_to_mp3(input_path, output_path)
        assert mock_run.called
        cmd_args = mock_run.call_args[0][0]
        assert "ffmpeg" in cmd_args[0]
        assert "libmp3lame" in cmd_args

    @patch("app.pipeline.audio_processing.subprocess.run")
    def test_convert_to_mp3_failure_raises(self, mock_run, tmp_path):
        from app.pipeline.audio_processing import convert_to_mp3

        mock_run.return_value = MagicMock(returncode=1, stderr="codec error")

        with pytest.raises(RuntimeError, match="FFmpeg failed"):
            convert_to_mp3(tmp_path / "in.wav", tmp_path / "out.mp3")

    @patch("app.pipeline.audio_processing.subprocess.run")
    def test_get_audio_duration(self, mock_run):
        from app.pipeline.audio_processing import get_audio_duration

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"format": {"duration": "123.456"}}',
        )
        duration = get_audio_duration(Path("/fake/audio.mp3"))
        assert duration == pytest.approx(123.456)

    @patch("app.pipeline.audio_processing.subprocess.run")
    def test_adjust_playback_speed(self, mock_run):
        from app.pipeline.audio_processing import adjust_playback_speed

        mock_run.return_value = MagicMock(returncode=0)
        adjust_playback_speed(Path("/fake/in.wav"), Path("/fake/out.wav"), speed=2.0)

        cmd_args = mock_run.call_args[0][0]
        assert "atempo" in " ".join(cmd_args)

    @patch("app.pipeline.audio_processing.subprocess.run")
    def test_high_speed_chains_atempo(self, mock_run):
        from app.pipeline.audio_processing import adjust_playback_speed

        mock_run.return_value = MagicMock(returncode=0)
        adjust_playback_speed(Path("/fake/in.wav"), Path("/fake/out.wav"), speed=4.0)

        cmd_args = mock_run.call_args[0][0]
        filter_str = cmd_args[cmd_args.index("-filter:a") + 1]
        assert filter_str.count("atempo") == 2  # chained: 2.0 * 2.0
