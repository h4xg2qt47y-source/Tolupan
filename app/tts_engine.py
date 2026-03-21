"""
Tol TTS Engine
Loads the best available VITS checkpoint (preferring TTS_Model_v2, the
Spanish-finetuned model) and synthesizes speech from Tol text.

Auto-reloads when a newer checkpoint appears so the web app always
uses the latest model while training is in progress.
"""

import io
import os
import wave
import struct
import threading
import time
from pathlib import Path

os.environ["COQUI_TOS_AGREED"] = "1"

_synth = None
_lock = threading.Lock()
_loaded_ckpt_path = None
_loaded_ckpt_mtime = 0
_last_scan_time = 0
_SCAN_INTERVAL = 60  # re-scan for new checkpoints every 60s

APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent

MODEL_DIRS = [
    PROJECT_DIR / "TTS_Model_v3",
    PROJECT_DIR / "TTS_Model_v2",
    PROJECT_DIR / "TTS_Model",
    PROJECT_DIR / "TTS_Model_v1_archive",
]


def _find_latest_checkpoint():
    """Scan model directories for the newest usable checkpoint."""
    for model_dir in MODEL_DIRS:
        if not model_dir.exists():
            continue
        runs = sorted(
            [d for d in model_dir.iterdir() if d.is_dir()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for run_dir in runs:
            config = run_dir / "config.json"
            if not config.exists():
                continue
            best = run_dir / "best_model.pth"
            if best.exists():
                return best, config
            ckpts = sorted(run_dir.glob("checkpoint_*.pth"),
                           key=lambda p: p.stat().st_mtime)
            if ckpts:
                return ckpts[-1], config
    return None, None


def _needs_reload():
    """Check whether a newer checkpoint has appeared since last load."""
    global _last_scan_time
    now = time.time()
    if now - _last_scan_time < _SCAN_INTERVAL:
        return False
    _last_scan_time = now

    ckpt, _ = _find_latest_checkpoint()
    if ckpt is None:
        return False
    if _loaded_ckpt_path is None:
        return True
    if str(ckpt) != str(_loaded_ckpt_path):
        return True
    try:
        mtime = ckpt.stat().st_mtime
        if mtime > _loaded_ckpt_mtime:
            return True
    except OSError:
        pass
    return False


def _patch_config(cfg_path: Path) -> Path:
    """Create a patched config that fixes the num_speakers mismatch.

    The Spanish base config has num_speakers=1 at the top level, but
    our fine-tuned model doesn't have a speaker embedding (model_args
    has num_speakers=0).  The Synthesizer's load_checkpoint crashes on
    the missing emb_g key, so we align the config before loading.
    """
    import json, tempfile
    with open(cfg_path) as f:
        cfg = json.load(f)

    ma = cfg.get("model_args", {})

    # Align top-level speaker/language settings with model_args
    # so the Synthesizer doesn't create layers the checkpoint lacks.
    cfg["num_speakers"] = ma.get("num_speakers", 0)
    if ma.get("num_speakers", 0) == 0:
        cfg["use_speaker_embedding"] = False
        cfg["speakers_file"] = None
        ma["use_speaker_embedding"] = False
        ma["speakers_file"] = None
    cfg["model_args"] = ma

    patched = cfg_path.parent / "config_inference.json"
    with open(patched, "w") as f:
        json.dump(cfg, f, indent=2)
    return patched


def _load_synthesizer():
    global _synth, _loaded_ckpt_path, _loaded_ckpt_mtime
    from TTS.utils.synthesizer import Synthesizer

    ckpt, cfg = _find_latest_checkpoint()
    if not ckpt or not cfg:
        raise FileNotFoundError("No TTS checkpoint found in any model directory")

    patched_cfg = _patch_config(cfg)

    _synth = Synthesizer(
        tts_checkpoint=str(ckpt),
        tts_config_path=str(patched_cfg),
        use_cuda=False,
    )
    _loaded_ckpt_path = ckpt
    _loaded_ckpt_mtime = ckpt.stat().st_mtime
    return _synth


def _get_synthesizer():
    global _synth
    if _synth is not None and not _needs_reload():
        return _synth
    with _lock:
        if _synth is not None and not _needs_reload():
            return _synth
        old = _loaded_ckpt_path
        synth = _load_synthesizer()
        if old and str(old) != str(_loaded_ckpt_path):
            print(f"  [TTS] Hot-reloaded → {_loaded_ckpt_path.name}")
        elif old is None:
            print(f"  [TTS] Loaded → {_loaded_ckpt_path.name}")
        return synth


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
