#!/usr/bin/env python3
"""
Refine TTS Dataset — Split long segments, filter by duration
=============================================================
Takes the raw verse-aligned segments and:
  1. Sub-splits segments >15s at internal silence points
  2. Splits text at sentence boundaries to match
  3. Filters to 1-15s range optimal for TTS training
  4. Exports refined metadata + LJSpeech format
"""

import csv
import json
import os
import re
import struct
import subprocess
import time
import wave
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
RAW_DIR = BASE / "TTS_Dataset"
REFINED_DIR = BASE / "TTS_Dataset" / "refined"
WAVS_DIR = RAW_DIR / "wavs"
FFMPEG = (
    Path(os.path.expanduser("~"))
    / "Library/Python/3.9/lib/python/site-packages/imageio_ffmpeg/binaries/ffmpeg-macos-aarch64-v7.1"
)

MAX_DURATION = 15.0
MIN_DURATION = 0.8
TARGET_SPLIT = 8.0


def read_wav_samples(wav_path):
    with wave.open(str(wav_path), 'rb') as wf:
        n = wf.getnframes()
        rate = wf.getframerate()
        raw = wf.readframes(n)
        samples = struct.unpack(f'<{n}h', raw)
        return list(samples), rate


def find_silence_midpoints(samples, rate, min_silence_ms=150, thresh=400):
    min_samp = int(rate * min_silence_ms / 1000)
    step = int(rate * 0.005)
    midpoints = []
    in_silence = False
    start = 0
    for i in range(0, len(samples), step):
        chunk = samples[i:i + step]
        if not chunk:
            break
        rms = (sum(s * s for s in chunk) / len(chunk)) ** 0.5
        if rms < thresh:
            if not in_silence:
                in_silence = True
                start = i
        else:
            if in_silence:
                length = i - start
                if length >= min_samp:
                    mid_ms = int((start + i) / 2 * 1000 / rate)
                    midpoints.append(mid_ms)
                in_silence = False
    return midpoints


def split_text_proportionally(text, n_parts):
    """Split text into roughly n_parts pieces at sentence/phrase boundaries."""
    boundaries = [m.start() + 1 for m in re.finditer(r'[.,:;!?]\s', text)]
    if not boundaries:
        boundaries = [m.start() + 1 for m in re.finditer(r'\s', text)]

    if len(boundaries) < n_parts - 1:
        words = text.split()
        chunk_size = max(1, len(words) // n_parts)
        parts = []
        for i in range(n_parts):
            start = i * chunk_size
            end = start + chunk_size if i < n_parts - 1 else len(words)
            parts.append(" ".join(words[start:end]))
        return [p for p in parts if p.strip()]

    ideal = [int(len(text) * (i + 1) / n_parts) for i in range(n_parts - 1)]
    chosen = []
    used = set()
    for target in ideal:
        best_idx = None
        best_dist = float('inf')
        for j, b in enumerate(boundaries):
            if j not in used and abs(b - target) < best_dist:
                best_dist = abs(b - target)
                best_idx = j
        if best_idx is not None:
            chosen.append(boundaries[best_idx])
            used.add(best_idx)
    chosen.sort()

    parts = []
    prev = 0
    for c in chosen:
        parts.append(text[prev:c].strip())
        prev = c
    parts.append(text[prev:].strip())
    return [p for p in parts if p.strip()]


def extract_wav_segment(src_wav, out_wav, start_ms, end_ms):
    start_s = start_ms / 1000.0
    dur_s = (end_ms - start_ms) / 1000.0
    subprocess.run(
        [str(FFMPEG), "-y", "-i", str(src_wav),
         "-ss", f"{start_s:.3f}", "-t", f"{dur_s:.3f}",
         "-ar", "22050", "-ac", "1", "-sample_fmt", "s16",
         str(out_wav)],
        capture_output=True, check=True,
    )


def main():
    t0 = time.time()
    print("=" * 70)
    print("  REFINE TTS DATASET")
    print("=" * 70)

    refined_wavs = REFINED_DIR / "wavs"
    refined_wavs.mkdir(parents=True, exist_ok=True)

    metadata = json.loads((RAW_DIR / "metadata.json").read_text())
    print(f"  Raw segments: {len(metadata):,}")

    refined = []
    skipped_short = 0
    skipped_long = 0
    split_count = 0

    for entry in metadata:
        wav_path = WAVS_DIR / entry["file"]
        if not wav_path.exists():
            continue

        dur = entry["duration_s"]

        if dur < MIN_DURATION:
            skipped_short += 1
            continue

        if dur <= MAX_DURATION:
            out_file = entry["file"].replace(".wav", "_r.wav")
            out_path = refined_wavs / out_file
            subprocess.run(
                [str(FFMPEG), "-y", "-i", str(wav_path),
                 "-ar", "22050", "-ac", "1", "-sample_fmt", "s16",
                 str(out_path)],
                capture_output=True, check=True,
            )
            refined.append({
                "file": out_file,
                "text": entry["text"],
                "book": entry["book"],
                "chapter": entry["chapter"],
                "verse": entry["verse"],
                "duration_s": dur,
            })
            continue

        # Long segment: try sub-splitting at internal silence
        try:
            samples, rate = read_wav_samples(wav_path)
        except Exception:
            skipped_long += 1
            continue

        total_ms = int(len(samples) * 1000 / rate)
        n_parts = max(2, int(dur / TARGET_SPLIT))
        if n_parts > 10:
            n_parts = 10

        mids = find_silence_midpoints(samples, rate, min_silence_ms=120, thresh=450)

        if len(mids) >= n_parts - 1:
            ideal = [int(total_ms * (i + 1) / n_parts) for i in range(n_parts - 1)]
            chosen = []
            used = set()
            for target in ideal:
                best = None
                best_d = float('inf')
                for j, m in enumerate(mids):
                    if j not in used and abs(m - target) < best_d:
                        best_d = abs(m - target)
                        best = j
                if best is not None:
                    chosen.append(mids[best])
                    used.add(best)
            chosen.sort()
        else:
            chosen = sorted(mids[:n_parts - 1]) if mids else []
            if not chosen:
                chosen = [int(total_ms * (i + 1) / n_parts) for i in range(n_parts - 1)]

        boundaries = [0] + chosen + [total_ms]
        text_parts = split_text_proportionally(entry["text"], len(boundaries) - 1)

        for idx in range(len(boundaries) - 1):
            seg_start = boundaries[idx]
            seg_end = boundaries[idx + 1]
            seg_dur = (seg_end - seg_start) / 1000.0

            if seg_dur < MIN_DURATION:
                continue
            if seg_dur > MAX_DURATION * 2:
                skipped_long += 1
                continue

            sub_file = entry["file"].replace(".wav", f"_p{idx:02d}.wav")
            sub_path = refined_wavs / sub_file

            try:
                extract_wav_segment(wav_path, sub_path, seg_start, seg_end)
            except Exception:
                continue

            text = text_parts[idx] if idx < len(text_parts) else entry["text"]
            refined.append({
                "file": sub_file,
                "text": text,
                "book": entry["book"],
                "chapter": entry["chapter"],
                "verse": entry["verse"],
                "sub_part": idx,
                "duration_s": round(seg_dur, 2),
            })
            split_count += 1

    with open(REFINED_DIR / "metadata.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="|")
        w.writerow(["file", "text", "duration_s"])
        for e in refined:
            w.writerow([e["file"], e["text"], e["duration_s"]])

    with open(REFINED_DIR / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(refined, f, ensure_ascii=False, indent=1)

    with open(REFINED_DIR / "train.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="|")
        for e in refined:
            w.writerow([e["file"].replace(".wav", ""), e["text"], e["text"]])

    durations = [e["duration_s"] for e in refined]
    total_s = sum(durations)

    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  REFINED DATASET RESULTS")
    print(f"{'='*70}")
    print(f"  Total refined segments: {len(refined):,}")
    print(f"  Sub-split from long:    {split_count:,}")
    print(f"  Skipped (too short):    {skipped_short:,}")
    print(f"  Skipped (too long):     {skipped_long:,}")
    print(f"  Total audio:            {total_s/3600:.1f} hours ({total_s:.0f}s)")
    print(f"  Mean duration:          {total_s/max(1,len(refined)):.1f}s")
    print(f"  Median duration:        {sorted(durations)[len(durations)//2]:.1f}s")
    print(f"  <2s:                    {sum(1 for d in durations if d < 2):,}")
    print(f"  2-5s:                   {sum(1 for d in durations if 2 <= d < 5):,}")
    print(f"  5-10s:                  {sum(1 for d in durations if 5 <= d < 10):,}")
    print(f"  10-15s:                 {sum(1 for d in durations if 10 <= d < 15):,}")
    print(f"  >15s:                   {sum(1 for d in durations if d >= 15):,}")
    print(f"  Output: {REFINED_DIR}")
    print(f"  Time: {elapsed:.1f}s")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
