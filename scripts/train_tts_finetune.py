#!/usr/bin/env python3
"""
Tol TTS — Fine-tune from pre-trained Spanish VITS
====================================================
Loads the Coqui `tts_models/es/css10/vits` config + weights and fine-tunes
on the Tol audio dataset.  By loading the Spanish config directly (and only
overriding training hyper-parameters), the architecture matches exactly,
ensuring every weight tensor transfers cleanly.

Because the SIL orthography for Tol is Spanish-based, every Tol grapheme
already exists in the Spanish model's 129-token vocabulary.  The model
starts with a strong Spanish acoustic prior and adapts to Tol-specific
sounds (glottalized stops via ', aspirated digraphs, ü [ɨ], etc.).
"""

import os
import sys
import json
import torch
from pathlib import Path

os.environ["COQUI_TOS_AGREED"] = "1"

from TTS.tts.configs.shared_configs import BaseDatasetConfig
from TTS.tts.configs.vits_config import VitsConfig
from TTS.tts.datasets import load_tts_samples
from TTS.tts.models.vits import Vits
from TTS.tts.utils.text.tokenizer import TTSTokenizer
from TTS.utils.audio import AudioProcessor
from TTS.utils.manage import ModelManager
from trainer import Trainer, TrainerArgs


def _project_root() -> Path:
    """Project root whether we run from `scripts/` or a copied script under TTS_Model_v2/."""
    p = Path(__file__).resolve()
    cand = p.parent.parent
    if (cand / "TTS_Dataset_v2").is_dir():
        return cand
    for anc in p.parents:
        if (anc / "TTS_Dataset_v2").is_dir() and (anc / "scripts" / "tts_progress_writer.py").exists():
            return anc
    return cand


BASE = _project_root()
sys.path.insert(0, str(BASE / "scripts"))
from tts_progress_writer import make_on_train_step_end

DATASET_DIR = BASE / "TTS_Dataset_v2"
OUTPUT_DIR = BASE / "TTS_Model_v2"

ES_MODEL_NAME = "tts_models/es/css10/vits"


def get_spanish_model():
    manager = ModelManager()
    model_path, config_path, _ = manager.download_model(ES_MODEL_NAME)
    return model_path, config_path


def formatter(root_path, meta_file, **kwargs):
    items = []
    with open(os.path.join(root_path, meta_file), "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("file|"):
                continue
            parts = line.split("|")
            if len(parts) < 3:
                continue
            wav_file = parts[0]
            text = parts[1]
            dur = float(parts[2]) if len(parts) > 2 else 0
            if dur < 0.8 or dur > 15.0:
                continue
            wav_path = os.path.join(root_path, "wavs", wav_file)
            if os.path.exists(wav_path):
                items.append({
                    "text": text,
                    "audio_file": wav_path,
                    "speaker_name": "tol_speaker",
                    "root_path": root_path,
                })
    return items


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    es_model_path, es_config_path = get_spanish_model()
    print(f"\n  Spanish base: {es_model_path}")

    # ── 1. Load Spanish config to get exact architecture ─────────────
    config = VitsConfig()
    config.load_json(es_config_path)

    # Override training hyper-parameters for fine-tuning
    config.output_path = str(OUTPUT_DIR)
    config.batch_size = 8
    config.eval_batch_size = 4
    config.num_loader_workers = 2
    config.num_eval_loader_workers = 1
    config.run_eval = True
    config.test_delay_epochs = 3
    config.epochs = 100
    config.lr_gen = 0.00005
    config.lr_disc = 0.00005
    config.mixed_precision = False
    config.print_step = 25
    config.print_eval = True
    config.save_step = 500
    config.save_best_after = 250
    config.save_n_checkpoints = 5

    # Point to our Tol dataset
    config.datasets = [BaseDatasetConfig(
        formatter="",
        meta_file_train="metadata.csv",
        path=str(DATASET_DIR),
        language="tol",
    )]

    # Ensure the discriminator gets built (Spanish checkpoint has
    # init_discriminator=False since it was a final export)
    config.model_args.init_discriminator = True

    # The Spanish config caps audio at ~6s which discards 95% of our
    # Tol data (median 9.7s).  Raise to 15s to use all samples.
    config.max_audio_len = 330750     # 15s × 22050 Hz
    config.min_audio_len = 17640      # 0.8s × 22050 Hz

    # Point to our own speaker/language ID files so the model creates
    # the embedding layers with the right dimensions (matching Spanish).
    config.speakers_file = str(DATASET_DIR / "speaker_ids.json")
    config.model_args.speakers_file = str(DATASET_DIR / "speaker_ids.json")
    config.language_ids_file = str(DATASET_DIR / "language_ids.json")
    config.model_args.language_ids_file = str(DATASET_DIR / "language_ids.json")

    ap = AudioProcessor.init_from_config(config)
    tokenizer, config = TTSTokenizer.init_from_config(config)

    train_samples, eval_samples = load_tts_samples(
        config.datasets,
        eval_split=True,
        eval_split_max_size=200,
        eval_split_size=0.05,
        formatter=formatter,
    )

    print(f"\n  Training samples: {len(train_samples)}")
    print(f"  Eval samples:     {len(eval_samples)}")

    # ── 2. Create model and transfer Spanish generator weights ───────
    model = Vits(config, ap, tokenizer, speaker_manager=None)

    es_ckpt = torch.load(es_model_path, map_location="cpu")
    es_state = es_ckpt["model"]

    # The Spanish checkpoint uses old-style weight norm (weight_g / weight_v)
    # while our model uses torch.nn.utils.parametrizations (original0 / original1).
    # Remap keys so the weights transfer correctly.
    remapped = {}
    for k, v in es_state.items():
        new_k = k
        if k.endswith(".weight_g"):
            base = k[:-len(".weight_g")]
            new_k = f"{base}.parametrizations.weight.original0"
        elif k.endswith(".weight_v"):
            base = k[:-len(".weight_v")]
            new_k = f"{base}.parametrizations.weight.original1"
        remapped[new_k] = v

    model_state = model.state_dict()
    loaded, skipped_shape, skipped_missing = 0, 0, 0
    for k, v in remapped.items():
        if k in model_state:
            if model_state[k].shape == v.shape:
                model_state[k] = v
                loaded += 1
            else:
                skipped_shape += 1
        else:
            skipped_missing += 1

    model.load_state_dict(model_state)
    total = loaded + skipped_shape + skipped_missing
    print(f"\n  Transferred {loaded}/{total} weight tensors from Spanish model")
    if skipped_shape:
        print(f"  ({skipped_shape} shape mismatches)")
    if skipped_missing:
        print(f"  ({skipped_missing} keys in Spanish not present in Tol model)")

    # ── 3. Resume from existing fine-tune checkpoint if available ────
    run_dirs = sorted(
        OUTPUT_DIR.glob("run-*"),
        key=lambda p: p.stat().st_mtime, reverse=True,
    )
    restore_path = None
    if run_dirs:
        ckpts = sorted(
            run_dirs[0].glob("checkpoint_*.pth"),
            key=lambda p: p.stat().st_mtime, reverse=True,
        )
        if ckpts:
            restore_path = str(ckpts[0])
            print(f"  Resuming fine-tune from: {restore_path}")

    # ── 4. Launch training ───────────────────────────────────────────
    trainer = Trainer(
        TrainerArgs(
            restore_path=restore_path,
            skip_train_epoch=False,
        ),
        config,
        output_path=str(OUTPUT_DIR),
        model=model,
        train_samples=train_samples,
        eval_samples=eval_samples,
        callbacks={
            "on_train_step_end": make_on_train_step_end(OUTPUT_DIR, every_n_steps=250),
        },
    )

    print("\n  ╔══════════════════════════════════════════════════════════╗")
    print("  ║   Tol VITS — Fine-tuning from Spanish baseline          ║")
    print("  ║   LR: 5e-5  (1/4 of scratch rate)                       ║")
    print("  ║   Epochs: 100  (vs 200 from-scratch)                     ║")
    print("  ║   Strategy: Spanish acoustics → Tol adaptation           ║")
    print("  ╚══════════════════════════════════════════════════════════╝")
    print(f"  Output: {OUTPUT_DIR}\n")
    trainer.fit()


if __name__ == "__main__":
    main()
