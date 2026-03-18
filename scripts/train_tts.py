#!/usr/bin/env python3
"""
Tol TTS Model Training Script
================================
Fine-tunes a VITS model on the refined Tol audio dataset.
"""

import os
import json
from pathlib import Path

os.environ["COQUI_TOS_AGREED"] = "1"

from TTS.tts.configs.shared_configs import BaseDatasetConfig, CharactersConfig
from TTS.tts.configs.vits_config import VitsConfig, VitsAudioConfig
from TTS.tts.datasets import load_tts_samples
from TTS.tts.models.vits import Vits
from TTS.tts.utils.text.tokenizer import TTSTokenizer
from TTS.utils.audio import AudioProcessor
from trainer import Trainer, TrainerArgs

BASE = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE / "TTS_Dataset_v2"
OUTPUT_DIR = BASE / "TTS_Model"


def build_charset():
    meta_json = DATASET_DIR / "metadata.json"
    meta_csv = DATASET_DIR / "metadata.csv"
    texts = []
    if meta_json.exists():
        meta = json.loads(meta_json.read_text())
        texts = [e["text"] for e in meta]
    elif meta_csv.exists():
        with open(meta_csv, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 2 and parts[0] != "file":
                    texts.append(parts[1])
    chars = set()
    for t in texts:
        for c in t.lower():
            if c not in "\n\r":
                chars.add(c)
    chars = sorted(chars)
    # Separate punctuation from letters
    puncts = set("!'(),-.:;?[] «»¡¿\"")
    letter_chars = [c for c in chars if c not in puncts and c != " "]
    punct_chars = [c for c in chars if c in puncts and c != " "]
    result = "".join(letter_chars)
    punct_str = "".join(punct_chars) + " "
    print(f"  Letters ({len(letter_chars)}): {result}")
    print(f"  Punctuation ({len(punct_chars)}): {punct_str}")
    return result, punct_str


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
            if dur < 1.0 or dur > 15.0:
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

    charset, punctuation = build_charset()

    audio_config = VitsAudioConfig(
        sample_rate=22050,
        win_length=1024,
        hop_length=256,
        num_mels=80,
        mel_fmin=0,
        mel_fmax=None,
    )

    characters_config = CharactersConfig(
        characters_class="TTS.tts.utils.text.characters.Graphemes",
        vocab_dict=None,
        pad="<PAD>",
        eos="<EOS>",
        bos="<BOS>",
        blank="<BLNK>",
        characters=charset,
        punctuations=punctuation,
        phonemes=None,
        is_unique=True,
        is_sorted=True,
    )

    dataset_config = BaseDatasetConfig(
        formatter="",
        meta_file_train="metadata.csv",
        path=str(DATASET_DIR),
        language="tol",
    )

    config = VitsConfig(
        audio=audio_config,
        batch_size=8,
        eval_batch_size=4,
        num_loader_workers=2,
        num_eval_loader_workers=1,
        run_eval=True,
        test_delay_epochs=5,
        epochs=200,
        lr_gen=0.0002,
        lr_disc=0.0002,
        text_cleaner="basic_cleaners",
        use_phonemes=False,
        characters=characters_config,
        output_path=str(OUTPUT_DIR),
        datasets=[dataset_config],
        mixed_precision=False,
        print_step=25,
        print_eval=True,
        save_step=500,
        save_best_after=500,
        save_n_checkpoints=3,
    )

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

    model = Vits(config, ap, tokenizer, speaker_manager=None)

    # Resume from latest checkpoint if one exists
    run_dirs = sorted(OUTPUT_DIR.glob("run-*"), key=lambda p: p.stat().st_mtime, reverse=True)
    restore_path = None
    if run_dirs:
        checkpoints = sorted(run_dirs[0].glob("checkpoint_*.pth"), key=lambda p: p.stat().st_mtime, reverse=True)
        if checkpoints:
            restore_path = str(checkpoints[0])
            print(f"\n  Resuming from: {restore_path}")

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
    )

    print("\n  Starting VITS training on Tol dataset...")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"  Epochs: {config.epochs}")
    trainer.fit()


if __name__ == "__main__":
    main()
