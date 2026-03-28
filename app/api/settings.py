import io

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.config import settings, BASE_DIR, load_config

router = APIRouter()

# All available Kokoro English voices with metadata
VOICES = [
    {"id": "af_heart", "name": "Heart", "accent": "American", "gender": "Female", "grade": "A"},
    {"id": "af_bella", "name": "Bella", "accent": "American", "gender": "Female", "grade": "A-"},
    {"id": "af_nicole", "name": "Nicole", "accent": "American", "gender": "Female", "grade": "B-"},
    {"id": "af_aoede", "name": "Aoede", "accent": "American", "gender": "Female", "grade": "C+"},
    {"id": "af_kore", "name": "Kore", "accent": "American", "gender": "Female", "grade": "C+"},
    {"id": "af_sarah", "name": "Sarah", "accent": "American", "gender": "Female", "grade": "C+"},
    {"id": "af_alloy", "name": "Alloy", "accent": "American", "gender": "Female", "grade": "C"},
    {"id": "af_nova", "name": "Nova", "accent": "American", "gender": "Female", "grade": "C"},
    {"id": "af_sky", "name": "Sky", "accent": "American", "gender": "Female", "grade": "C-"},
    {"id": "af_river", "name": "River", "accent": "American", "gender": "Female", "grade": "D"},
    {"id": "af_jessica", "name": "Jessica", "accent": "American", "gender": "Female", "grade": "D"},
    {"id": "am_adam", "name": "Adam", "accent": "American", "gender": "Male", "grade": "A"},
    {"id": "am_michael", "name": "Michael", "accent": "American", "gender": "Male", "grade": "B-"},
    {"id": "am_fenrir", "name": "Fenrir", "accent": "American", "gender": "Male", "grade": "C+"},
    {"id": "am_puck", "name": "Puck", "accent": "American", "gender": "Male", "grade": "C+"},
    {"id": "am_echo", "name": "Echo", "accent": "American", "gender": "Male", "grade": "C"},
    {"id": "bf_emma", "name": "Emma", "accent": "British", "gender": "Female", "grade": "B-"},
    {"id": "bf_isabella", "name": "Isabella", "accent": "British", "gender": "Female", "grade": "C"},
    {"id": "bf_alice", "name": "Alice", "accent": "British", "gender": "Female", "grade": "D"},
    {"id": "bf_lily", "name": "Lily", "accent": "British", "gender": "Female", "grade": "D"},
    {"id": "bm_george", "name": "George", "accent": "British", "gender": "Male", "grade": "B-"},
    {"id": "bm_lewis", "name": "Lewis", "accent": "British", "gender": "Male", "grade": "B-"},
    {"id": "bm_daniel", "name": "Daniel", "accent": "British", "gender": "Male", "grade": "C+"},
    {"id": "bm_fable", "name": "Fable", "accent": "British", "gender": "Male", "grade": "C+"},
    {"id": "bm_onyx", "name": "Onyx", "accent": "British", "gender": "Male", "grade": "C"},
]

PREVIEW_TEXT = (
    "The night was dark and full of terrors. She crept along the hallway, "
    "her fingers trailing the cold stone wall. Behind her, footsteps echoed."
)


@router.get("/voices")
async def list_voices():
    """List all available TTS voices with the currently active one."""
    return {
        "current_voice": settings.tts.voice,
        "voices": VOICES,
    }


@router.get("/voices/{voice_id}/preview")
async def preview_voice(voice_id: str):
    """Generate a short audio preview of a voice."""
    if not any(v["id"] == voice_id for v in VOICES):
        raise HTTPException(404, f"Unknown voice: {voice_id}")

    from app.pipeline.tts import SAMPLE_RATE, TTSError
    import soundfile as sf
    import numpy as np
    import torch

    try:
        pipeline = _get_preview_pipeline(voice_id)

        generator = pipeline(
            PREVIEW_TEXT,
            voice=voice_id,
            speed=settings.tts.speed,
        )
        segments = []
        for result in generator:
            if result.audio is not None:
                segments.append(result.audio)

        if not segments:
            raise TTSError("No audio generated for preview")

        audio = torch.cat(segments, dim=-1).cpu().numpy().astype(np.float32)

        buf = io.BytesIO()
        sf.write(buf, audio, SAMPLE_RATE, format="WAV")
        buf.seek(0)

        return StreamingResponse(buf, media_type="audio/wav")
    except Exception as e:
        raise HTTPException(500, f"Preview generation failed: {e}")


# Cache pipelines by lang_code so we don't reload for every preview
_preview_pipelines = {}

def _get_preview_pipeline(voice_id: str):
    """Get or create a KPipeline for the given voice's language."""
    lang_code = "b" if voice_id.startswith("b") else "a"
    if lang_code not in _preview_pipelines:
        from kokoro import KPipeline
        _preview_pipelines[lang_code] = KPipeline(
            lang_code=lang_code, device=settings.tts.device,
        )
    return _preview_pipelines[lang_code]


class VoiceSelection(BaseModel):
    voice: str


@router.put("/voices/select")
async def select_voice(selection: VoiceSelection):
    """Set the active TTS voice. Updates config.yaml."""
    import yaml

    if not any(v["id"] == selection.voice for v in VOICES):
        raise HTTPException(404, f"Unknown voice: {selection.voice}")

    # Update the in-memory settings
    settings.tts.voice = selection.voice

    # Persist to config.yaml
    config_path = BASE_DIR / "config.yaml"
    raw = load_config(config_path)
    raw["tts"]["voice"] = selection.voice
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(raw, f, default_flow_style=False, allow_unicode=True)

    return {"status": "ok", "voice": selection.voice}
