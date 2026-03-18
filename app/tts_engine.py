"""
Tol TTS Engine
Loads the best VITS checkpoint and synthesizes speech from Tol text.
"""

import io
import os
import wave
import struct
import threading
from pathlib import Path

os.environ["COQUI_TOS_AGREED"] = "1"

_synth = None
_lock = threading.Lock()
_load_error = None

TTS_BASE = Path(__file__).resolve().parent.parent / "TTS_Model"


def _find_latest_checkpoint():
    """Find the newest run directory and best available checkpoint."""
    if not TTS_BASE.exists():
        return None, None
    runs = sorted([d for d in TTS_BASE.iterdir() if d.is_dir() and d.name.startswith("run-")])
    if not runs:
        return None, None
    run_dir = runs[-1]
    config = run_dir / "config.json"
    if not config.exists():
        return None, None
    best = run_dir / "best_model.pth"
    if best.exists():
        return best, config
    pths = sorted(run_dir.glob("checkpoint_*.pth"))
    if pths:
        return pths[-1], config
    return None, None


def _get_synthesizer():
    global _synth, _load_error
    if _synth is not None:
        return _synth
    with _lock:
        if _synth is not None:
            return _synth
        if _load_error:
            raise _load_error
        try:
            from TTS.utils.synthesizer import Synthesizer
            ckpt, cfg = _find_latest_checkpoint()
            if not ckpt or not cfg:
                raise FileNotFoundError("No TTS checkpoint found")
            _synth = Synthesizer(
                tts_checkpoint=str(ckpt),
                tts_config_path=str(cfg),
                use_cuda=False,
            )
            return _synth
        except Exception as e:
            _load_error = e
            raise


def is_available() -> bool:
    ckpt, cfg = _find_latest_checkpoint()
    return ckpt is not None and cfg is not None


def synthesize(text: str) -> bytes:
    """Synthesize Tol text to WAV bytes."""
    synth = _get_synthesizer()
    wav_list = synth.tts(text)

    buf = io.BytesIO()
    sample_rate = synth.output_sample_rate
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        for sample in wav_list:
            clamped = max(-1.0, min(1.0, sample))
            wf.writeframes(struct.pack("<h", int(clamped * 32767)))
    return buf.getvalue()
