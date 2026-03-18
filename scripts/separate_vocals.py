#!/usr/bin/env python3
"""
Vocal Separation Pipeline
=========================
Uses Demucs (Meta Research) to strip background music from all
ScriptureEarth audio Bible MP3s, keeping only the vocal track.

Demucs htdemucs model separates into: vocals, drums, bass, other.
We extract only the vocals stem and save as MP3 to replace the
contaminated originals for TTS training.
"""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
AUDIO_DIR = BASE / "Tol Audio" / "ScriptureEarth_NT_Audio"
CLEAN_DIR = BASE / "Tol Audio" / "ScriptureEarth_NT_Audio_CleanVocals"
DEMUCS_OUT = BASE / "Tol Audio" / "_demucs_tmp"

PYTHON = sys.executable


def run_demucs_batch(files, batch_size=5):
    """Process files through Demucs in batches."""
    total = len(files)
    processed = 0
    failed = []

    for i in range(0, total, batch_size):
        batch = files[i:i + batch_size]
        batch_strs = [str(f) for f in batch]

        try:
            result = subprocess.run(
                [PYTHON, "-m", "demucs",
                 "--two-stems", "vocals",
                 "-n", "htdemucs",
                 "--mp3",
                 "-o", str(DEMUCS_OUT),
                 ] + batch_strs,
                capture_output=True, text=True, timeout=600,
            )
            if result.returncode != 0:
                print(f"    WARNING: Demucs error on batch {i//batch_size + 1}: {result.stderr[-200:]}")
                for f in batch:
                    failed.append(f.name)
            else:
                processed += len(batch)
        except subprocess.TimeoutExpired:
            print(f"    WARNING: Timeout on batch {i//batch_size + 1}")
            for f in batch:
                failed.append(f.name)
        except Exception as e:
            print(f"    ERROR: {e}")
            for f in batch:
                failed.append(f.name)

        done = min(i + batch_size, total)
        pct = done / total * 100
        print(f"  [{done}/{total}] ({pct:.0f}%) processed")

    return processed, failed


def collect_vocals():
    """Move extracted vocal stems to clean directory."""
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    htdemucs_dir = DEMUCS_OUT / "htdemucs"
    if not htdemucs_dir.exists():
        print(f"  ERROR: Demucs output not found at {htdemucs_dir}")
        return 0

    count = 0
    for track_dir in sorted(htdemucs_dir.iterdir()):
        if not track_dir.is_dir():
            continue
        vocal_file = track_dir / "vocals.mp3"
        if not vocal_file.exists():
            vocal_file = track_dir / "vocals.wav"
        if not vocal_file.exists():
            continue
        ext = vocal_file.suffix
        out_name = track_dir.name + ext
        shutil.copy2(vocal_file, CLEAN_DIR / out_name)
        count += 1

    return count


def main():
    t0 = time.time()
    print("=" * 70)
    print("  VOCAL SEPARATION — Demucs htdemucs")
    print("=" * 70)

    mp3s = sorted(AUDIO_DIR.glob("*.mp3"))
    print(f"  Audio files found: {len(mp3s)}")
    print(f"  Output directory:  {CLEAN_DIR}")
    print()

    if not mp3s:
        print("  No MP3 files found!")
        return

    already_done = set()
    if CLEAN_DIR.exists():
        already_done = {f.stem for f in CLEAN_DIR.glob("*.mp3")}
        already_done |= {f.stem for f in CLEAN_DIR.glob("*.wav")}

    to_process = [f for f in mp3s if f.stem not in already_done]
    print(f"  Already separated: {len(already_done)}")
    print(f"  Remaining:         {len(to_process)}")
    print()

    if not to_process:
        print("  All files already separated!")
    else:
        print("  Phase 1: Running Demucs vocal separation...")
        processed, failed = run_demucs_batch(to_process, batch_size=3)
        print(f"  Demucs complete: {processed} processed, {len(failed)} failed")
        if failed:
            print(f"  Failed files: {failed[:10]}{'...' if len(failed) > 10 else ''}")
        print()

        print("  Phase 2: Collecting vocal stems...")
        collected = collect_vocals()
        print(f"  Collected {collected} vocal tracks to {CLEAN_DIR}")

    total_clean = len(list(CLEAN_DIR.glob("*.mp3"))) + len(list(CLEAN_DIR.glob("*.wav")))

    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  VOCAL SEPARATION COMPLETE")
    print(f"{'='*70}")
    print(f"  Clean vocal files: {total_clean}")
    print(f"  Output:            {CLEAN_DIR}")
    print(f"  Time:              {elapsed/60:.1f} minutes")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
