import time
import os
import torch

from torch.serialization import add_safe_globals
from TTS.tts.configs.xtts_config import XttsConfig
from TTS.tts.models.xtts import XttsAudioConfig

# Allow these XTTS classes to be unpickled when weights_only=True
add_safe_globals([XttsConfig, XttsAudioConfig])

# --- PyTorch 2.6+ "weights_only" workaround on torch.load itself ---
_real_torch_load = torch.load

def trusted_torch_load(*args, **kwargs):
    # If caller didn't explicitly set weights_only, force old behavior
    kwargs.setdefault("weights_only", False)
    return _real_torch_load(*args, **kwargs)

torch.load = trusted_torch_load
# --- end workaround ---

from TTS.api import TTS


def main():
    print("CUDA available:", torch.cuda.is_available())
    if torch.cuda.is_available():
        print("Device:", torch.cuda.get_device_name(0))

    print("Loading XTTS v2 model...")
    t0 = time.time()
    tts = TTS("tts_models/multilingual/multi-dataset/xtts_v2", gpu=True)
    t1 = time.time()
    print(f"Model loaded in {t1 - t0:.2f} seconds")

    text = "Shi Er forced himself to interject, 'Principal Zhang used to be a regimental commander. He transferred to the university to manage logistics.' I was secretly thrilled. It seems that my skills in reading people have improved quite a bit in the past year or so. Principal Zhang continued, 'Please don't take offense, Master. I'm a rough man who doesn't know how to pretend. I just say what I think. However, although I'm a rough man, I've always respected cultured people. To show my sincerity, I'll drink three cups first.' After Principal Zhang finished his first cup, Shi Er and others strained it again, and then strained it once more. Master picked up his cup, habitually stroked his beard, and laughed loudly: 'If Commander Zhang only spoke politely, who would lead the troops and defend the country?"

    print("Generating audio and saving to file...")
    g0 = time.time()
    out_path = os.path.join(os.getcwd(), "xtts_test.wav")

    tts.tts_to_file(
        text=text,
        file_path=out_path,
        speaker="Ana Florence",
        language="en",
        speed=2.0,
    )
    g1 = time.time()
    print(f"Generated audio in {g1 - g0:.2f} seconds")
    print("Saved to:", out_path)



if __name__ == "__main__":
    main()
