#!/usr/bin/env python3
"""
Full TTS Pipeline Rebuild — Parallel Version
=============================================
1. Separate vocals from all ScriptureEarth audio using Demucs (3 parallel workers)
2. Rebuild TTS dataset from clean vocals
3. Refine the dataset
4. Restart VITS training
"""

import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

BASE = Path(__file__).resolve().parent.parent
AUDIO_DIR = BASE / "Tol Audio" / "ScriptureEarth_NT_Audio"
CLEAN_DIR = BASE / "Tol Audio" / "ScriptureEarth_NT_Audio_CleanVocals"
PYTHON = sys.executable

NUM_WORKERS = 4


def separate_one_file(mp3_path: str) -> dict:
    """Run Demucs on a single file. Called in a worker process."""
    mp3 = Path(mp3_path)
    work_dir = Path(mp3_path).parent.parent / "_demucs_work" / mp3.stem
    work_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = "2"
    env["MKL_NUM_THREADS"] = "2"

    try:
        result = subprocess.run(
            [sys.executable, "-m", "demucs",
             "--two-stems", "vocals",
             "-n", "htdemucs",
             "--mp3",
             "--segment", "7",
             "-o", str(work_dir),
             str(mp3)],
            capture_output=True, text=True, timeout=1800,
            env=env,
        )

        if result.returncode != 0:
            return {"file": mp3.name, "ok": False, "error": result.stderr[-200:]}

        vocal = work_dir / "htdemucs" / mp3.stem / "vocals.mp3"
        if not vocal.exists():
            return {"file": mp3.name, "ok": False, "error": "vocals.mp3 not found"}

        clean_dir = Path(mp3_path).parent.parent / "ScriptureEarth_NT_Audio_CleanVocals"
        clean_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(vocal, clean_dir / mp3.name)

        shutil.rmtree(work_dir, ignore_errors=True)
        return {"file": mp3.name, "ok": True}

    except subprocess.TimeoutExpired:
        return {"file": mp3.name, "ok": False, "error": "timeout"}
    except Exception as e:
        return {"file": mp3.name, "ok": False, "error": str(e)}


# ------------------------------------------------------------------ #
#  PHASE 1 — Vocal Separation with Demucs (Parallel)
# ------------------------------------------------------------------ #
def phase1_separate():
    print("\n" + "=" * 70)
    print(f"  PHASE 1: VOCAL SEPARATION (Demucs htdemucs, {NUM_WORKERS} workers)")
    print("=" * 70)

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    mp3s = sorted(AUDIO_DIR.glob("*.mp3"))
    print(f"  Source files:  {len(mp3s)}")

    already = {f.stem for f in CLEAN_DIR.glob("*.mp3")}
    remaining = [f for f in mp3s if f.stem not in already]
    print(f"  Already done:  {len(already)}")
    print(f"  To process:    {len(remaining)}")
    print(f"  Workers:       {NUM_WORKERS}")
    print()

    if not remaining:
        print("  All files already separated!")
        return

    t0 = time.time()
    completed = 0
    failed = []
    total = len(remaining)

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(separate_one_file, str(f)): f.name
            for f in remaining
        }

        for future in as_completed(futures):
            result = future.result()
            if result["ok"]:
                completed += 1
            else:
                failed.append(result["file"])
                print(f"    FAILED: {result['file']} — {result.get('error','')[:80]}")

            done = completed + len(failed)
            elapsed = time.time() - t0
            rate = elapsed / max(done, 1)
            eta_min = rate * (total - done) / 60
            pct = done / total * 100
            print(f"  [{done}/{total}] ({pct:.0f}%) "
                  f"ok={completed} fail={len(failed)} "
                  f"ETA ~{eta_min:.0f}m ({eta_min/60:.1f}h)")

    elapsed = time.time() - t0
    total_clean = len(list(CLEAN_DIR.glob("*.mp3")))
    print(f"\n  Separation complete!")
    print(f"  Clean vocal files: {total_clean}")
    print(f"  Failed: {len(failed)}")
    print(f"  Time: {elapsed/60:.1f} minutes ({elapsed/3600:.1f} hours)")

    if failed:
        print(f"  Copying {len(failed)} originals as fallback...")
        for name in failed:
            src = AUDIO_DIR / name
            if src.exists() and not (CLEAN_DIR / name).exists():
                shutil.copy2(src, CLEAN_DIR / name)


# ------------------------------------------------------------------ #
#  PHASE 2 — Rebuild TTS Dataset
# ------------------------------------------------------------------ #
def phase2_rebuild_dataset():
    print("\n" + "=" * 70)
    print("  PHASE 2: REBUILD TTS DATASET (clean audio)")
    print("=" * 70)

    build_script = BASE / "scripts" / "build_tts_dataset.py"

    old_dataset = BASE / "TTS_Dataset"
    old_backup = BASE / "TTS_Dataset_old_with_music"
    if old_dataset.exists() and not old_backup.exists():
        print("  Backing up old dataset...")
        shutil.move(str(old_dataset), str(old_backup))
    elif old_dataset.exists():
        print("  Removing old dataset...")
        shutil.rmtree(old_dataset, ignore_errors=True)

    env = os.environ.copy()
    env["TOL_AUDIO_DIR"] = str(CLEAN_DIR)

    print("  Running build_tts_dataset.py with clean vocals...")
    result = subprocess.run(
        [PYTHON, str(build_script)],
        env=env, text=True, timeout=1200,
    )
    if result.returncode != 0:
        print(f"  ERROR: build failed with code {result.returncode}")
        return False
    return True


# ------------------------------------------------------------------ #
#  PHASE 3 — Refine Dataset
# ------------------------------------------------------------------ #
def phase3_refine():
    print("\n" + "=" * 70)
    print("  PHASE 3: REFINE TTS DATASET")
    print("=" * 70)

    refine_script = BASE / "scripts" / "refine_tts_dataset.py"
    result = subprocess.run(
        [PYTHON, str(refine_script)],
        text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"  ERROR: refinement failed with code {result.returncode}")
        return False
    return True


# ------------------------------------------------------------------ #
#  PHASE 4 — Restart Training
# ------------------------------------------------------------------ #
def phase4_train():
    print("\n" + "=" * 70)
    print("  PHASE 4: RESTART VITS TRAINING (clean data)")
    print("=" * 70)

    train_script = BASE / "scripts" / "train_tts.py"
    print(f"  Starting training: {train_script}")
    print("  Training will run indefinitely — check progress in terminal.")
    sys.stdout.flush()

    os.execv(PYTHON, [PYTHON, str(train_script)])


# ------------------------------------------------------------------ #
def main():
    t0 = time.time()
    print("=" * 70)
    print("  FULL TTS PIPELINE REBUILD")
    print("  Stripping background music → Rebuilding dataset → Retraining")
    print("=" * 70)

    phase1_separate()

    ok = phase2_rebuild_dataset()
    if not ok:
        print("\n  PIPELINE STOPPED: Dataset rebuild failed.")
        return

    ok = phase3_refine()
    if not ok:
        print("\n  PIPELINE STOPPED: Dataset refinement failed.")
        return

    elapsed = time.time() - t0
    print(f"\n  Pipeline prep complete in {elapsed/60:.1f} minutes ({elapsed/3600:.1f} hours)")
    print("  Starting training...")
    sys.stdout.flush()

    phase4_train()


if __name__ == "__main__":
    main()
