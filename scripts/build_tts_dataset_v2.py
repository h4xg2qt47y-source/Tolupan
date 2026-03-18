#!/usr/bin/env python3
"""
TTS Dataset Builder v2 — Tol Language
======================================
Improved alignment: uses text-proportional timing with dynamic programming
to select silence-based split points that best match expected verse durations.

Key improvements over v1:
  - Text length drives expected duration (Tol is roughly phonetic)
  - Dynamic programming finds optimal silence-aligned split points
  - Long segments (>15s) are sub-split at internal silences
  - Quality filtering rejects implausible alignments
  - Outputs both full-verse and TTS-ready (sub-split) datasets
"""

import csv
import json
import math
import os
import re
import struct
import subprocess
import sys
import time
import wave
from html import unescape
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
_default_audio = BASE / "Tol Audio" / "ScriptureEarth_NT_Audio_CleanVocals"
if not _default_audio.exists():
    _default_audio = BASE / "Tol Audio" / "ScriptureEarth_NT_Audio"
AUDIO_DIR = Path(os.environ.get("TOL_AUDIO_DIR", str(_default_audio)))
TEXT_DIR = BASE / "Tol Translation" / "NT_Text"
OUTPUT_DIR = BASE / "TTS_Dataset_v2"
FFMPEG = (
    Path(os.path.expanduser("~"))
    / "Library/Python/3.9/lib/python/site-packages/imageio_ffmpeg/binaries/ffmpeg-macos-aarch64-v7.1"
)

BOOK_MAP = {
    "MAT": ("41-MATjic", 28), "MRK": ("42-MRKjic", 16), "LUK": ("43-LUKjic", 24),
    "JHN": ("44-JHNjic", 21), "ACT": ("45-ACTjic", 28), "ROM": ("46-ROMjic", 16),
    "1CO": ("47-1COjic", 16), "2CO": ("48-2COjic", 13), "GAL": ("49-GALjic", 6),
    "EPH": ("50-EPHjic", 6), "PHP": ("51-PHPjic", 4), "COL": ("52-COLjic", 4),
    "1TH": ("53-1THjic", 5), "2TH": ("54-2THjic", 3), "1TI": ("55-1TIjic", 6),
    "2TI": ("56-2TIjic", 4), "TIT": ("57-TITjic", 3), "PHM": ("58-PHMjic", 1),
    "HEB": ("59-HEBjic", 13), "JAS": ("60-JASjic", 5), "1PE": ("61-1PEjic", 5),
    "2PE": ("62-2PEjic", 3), "1JN": ("63-1JNjic", 5), "2JN": ("64-2JNjic", 1),
    "3JN": ("65-3JNjic", 1), "JUD": ("66-JUDjic", 1), "REV": ("67-REVjic", 22),
}

MAX_TTS_DURATION = 15.0
MIN_TTS_DURATION = 1.0
MIN_CPS = 3.0
MAX_CPS = 30.0


def parse_verses(html_path):
    """Extract verses from Tol NT HTML file, handling combined verses like '1-3'."""
    text = html_path.read_text(encoding="utf-8")
    text = text.replace("&#160;", " ").replace("&nbsp;", " ")
    text = unescape(text)
    text = re.sub(r'<span class="popup">.*?</span>', '', text)
    text = re.sub(r'<a[^>]*class="notemark"[^>]*>.*?</a>', '', text)
    text = re.sub(r"<span class=['\"]add['\"]>(.*?)</span>", r"\1", text)

    verses = {}
    for m in re.finditer(
        r'<span\s+class="verse"\s+id="V(\d+)">\s*([\d\-]+)\s*</span>(.*?)'
        r'(?=<span\s+class="verse"|<ul\s|<div\s+class=[\'"](?:footnote|copyright|tnav|s\b|r\b))',
        text, re.DOTALL
    ):
        vnum = int(m.group(1))
        verse_label = m.group(2).strip()
        clean = re.sub(r'<[^>]+>', ' ', m.group(3))
        clean = re.sub(r'\s+', ' ', clean).strip()
        if not clean:
            continue

        if '-' in verse_label:
            parts = verse_label.split('-')
            start_v, end_v = int(parts[0]), int(parts[1])
            for v in range(start_v, end_v + 1):
                verses[v] = clean
        else:
            verses[vnum] = clean

    return verses


def mp3_to_wav(mp3_path, wav_path):
    subprocess.run(
        [str(FFMPEG), "-y", "-i", str(mp3_path),
         "-ar", "16000", "-ac", "1", "-sample_fmt", "s16",
         str(wav_path)],
        capture_output=True, check=True,
    )
    return wav_path


def read_wav_samples(wav_path):
    with wave.open(str(wav_path), 'rb') as wf:
        n_frames = wf.getnframes()
        rate = wf.getframerate()
        raw = wf.readframes(n_frames)
        samples = struct.unpack(f'<{n_frames}h', raw)
        return list(samples), rate


def detect_silences(samples, rate, min_silence_ms=200, silence_thresh=400):
    """Find silence regions. Returns list of (start_ms, end_ms, midpoint_ms)."""
    min_samples = int(rate * min_silence_ms / 1000)
    chunk_size = int(rate * 0.01)
    silences = []
    in_silence = False
    start = 0

    for i in range(0, len(samples), chunk_size):
        chunk = samples[i:i + chunk_size]
        if not chunk:
            break
        rms = (sum(s * s for s in chunk) / len(chunk)) ** 0.5
        if rms < silence_thresh:
            if not in_silence:
                in_silence = True
                start = i
        else:
            if in_silence:
                length = i - start
                if length >= min_samples:
                    start_ms = int(start * 1000 / rate)
                    end_ms = int(i * 1000 / rate)
                    mid_ms = (start_ms + end_ms) // 2
                    silences.append((start_ms, end_ms, mid_ms))
                in_silence = False

    if in_silence and (len(samples) - start) >= min_samples:
        start_ms = int(start * 1000 / rate)
        end_ms = int(len(samples) * 1000 / rate)
        silences.append((start_ms, end_ms, (start_ms + end_ms) // 2))

    return silences


def dp_align(verse_char_counts, silence_midpoints, total_duration_ms):
    """
    Dynamic programming to find optimal N-1 split points from silence midpoints
    that minimize total squared error vs text-proportional timing.

    verse_char_counts: list of char counts per verse
    silence_midpoints: sorted list of silence midpoint times in ms
    total_duration_ms: total audio duration

    Returns: list of N segment boundaries [(start_ms, end_ms), ...]
    """
    n_verses = len(verse_char_counts)
    if n_verses <= 1:
        return [(0, total_duration_ms)]

    total_chars = sum(verse_char_counts)
    if total_chars == 0:
        return [(0, total_duration_ms)]

    cumulative_chars = [0]
    for c in verse_char_counts:
        cumulative_chars.append(cumulative_chars[-1] + c)

    target_times = []
    for i in range(1, n_verses):
        t = int(total_duration_ms * cumulative_chars[i] / total_chars)
        target_times.append(t)

    n_splits_needed = n_verses - 1
    sil = sorted(silence_midpoints)

    if len(sil) < n_splits_needed:
        ideal = target_times
        chosen = []
        for t in ideal:
            if sil:
                best = min(sil, key=lambda s: abs(s - t))
                chosen.append(best)
            else:
                chosen.append(t)
        chosen.sort()
    else:
        S = len(sil)
        INF = float('inf')
        dp = [[INF] * (n_splits_needed + 1) for _ in range(S + 1)]
        parent = [[-1] * (n_splits_needed + 1) for _ in range(S + 1)]

        for j in range(S + 1):
            dp[j][0] = 0

        for k in range(1, n_splits_needed + 1):
            target = target_times[k - 1]
            for j in range(k, S + 1):
                cost = (sil[j - 1] - target) ** 2
                for prev_j in range(k - 1, j):
                    total = dp[prev_j][k - 1] + cost
                    if total < dp[j][k]:
                        dp[j][k] = total
                        parent[j][k] = prev_j

        best_cost = INF
        best_end = -1
        for j in range(n_splits_needed, S + 1):
            if dp[j][n_splits_needed] < best_cost:
                best_cost = dp[j][n_splits_needed]
                best_end = j

        chosen = []
        j = best_end
        for k in range(n_splits_needed, 0, -1):
            chosen.append(sil[j - 1])
            j = parent[j][k]
        chosen.sort()

    boundaries = [0] + chosen + [total_duration_ms]
    segments = []
    for i in range(len(boundaries) - 1):
        segments.append((boundaries[i], boundaries[i + 1]))
    return segments


def sub_split_segment(samples, rate, start_ms, end_ms, text, max_dur=MAX_TTS_DURATION):
    """Split a segment that's too long into sub-segments at internal silences."""
    dur_s = (end_ms - start_ms) / 1000.0
    if dur_s <= max_dur:
        return [(start_ms, end_ms, text)]

    start_sample = int(start_ms * rate / 1000)
    end_sample = int(end_ms * rate / 1000)
    sub_samples = samples[start_sample:end_sample]

    sils = detect_silences(sub_samples, rate, min_silence_ms=150, silence_thresh=350)
    if not sils:
        sils = detect_silences(sub_samples, rate, min_silence_ms=100, silence_thresh=300)

    abs_sils = [(s + start_ms, e + start_ms, m + start_ms) for s, e, m in sils]

    words = text.split()
    n_parts = max(2, math.ceil(dur_s / (max_dur * 0.8)))

    if len(abs_sils) < n_parts - 1:
        chunk_dur = (end_ms - start_ms) / n_parts
        parts = []
        for i in range(n_parts):
            p_start = start_ms + int(i * chunk_dur)
            p_end = start_ms + int((i + 1) * chunk_dur)
            if i == n_parts - 1:
                p_end = end_ms
            w_start = int(len(words) * i / n_parts)
            w_end = int(len(words) * (i + 1) / n_parts)
            p_text = ' '.join(words[w_start:w_end])
            parts.append((p_start, p_end, p_text))
        return parts

    midpoints = [m for _, _, m in abs_sils]
    chunk_dur = (end_ms - start_ms) / n_parts
    targets = [start_ms + int((i + 1) * chunk_dur) for i in range(n_parts - 1)]

    chosen = []
    used = set()
    for t in targets:
        best_idx = min(range(len(midpoints)), key=lambda j: abs(midpoints[j] - t) if j not in used else float('inf'))
        chosen.append(midpoints[best_idx])
        used.add(best_idx)
    chosen.sort()

    splits = [start_ms] + chosen + [end_ms]
    parts = []
    for i in range(len(splits) - 1):
        frac_start = (splits[i] - start_ms) / max(end_ms - start_ms, 1)
        frac_end = (splits[i + 1] - start_ms) / max(end_ms - start_ms, 1)
        w_start = int(len(words) * frac_start)
        w_end = int(len(words) * frac_end)
        if i == len(splits) - 2:
            w_end = len(words)
        p_text = ' '.join(words[w_start:w_end])
        if p_text.strip():
            parts.append((splits[i], splits[i + 1], p_text))

    return parts if parts else [(start_ms, end_ms, text)]


def extract_segment(wav_path, out_path, start_ms, end_ms):
    start_s = start_ms / 1000.0
    duration_s = (end_ms - start_ms) / 1000.0
    subprocess.run(
        [str(FFMPEG), "-y", "-i", str(wav_path),
         "-ss", f"{start_s:.3f}", "-t", f"{duration_s:.3f}",
         "-ar", "22050", "-ac", "1", "-sample_fmt", "s16",
         str(out_path)],
        capture_output=True, check=True,
    )


def process_chapter(book, ch, audio_path, text_path, wavs_dir, full_verse_dir):
    """Process one chapter with improved alignment."""
    verses = parse_verses(text_path)
    if not verses:
        return [], []

    sorted_vnums = sorted(verses.keys())
    n_verses = len(sorted_vnums)

    wav_tmp = wavs_dir / f"_tmp_{book}{ch:02d}.wav"
    try:
        mp3_to_wav(audio_path, wav_tmp)
    except subprocess.CalledProcessError:
        return [], []

    samples, rate = read_wav_samples(wav_tmp)
    total_ms = int(len(samples) * 1000 / rate)

    silences = detect_silences(samples, rate, min_silence_ms=250, silence_thresh=450)
    if len(silences) < n_verses - 1:
        silences = detect_silences(samples, rate, min_silence_ms=150, silence_thresh=350)
    if len(silences) < n_verses - 1:
        silences = detect_silences(samples, rate, min_silence_ms=100, silence_thresh=250)

    char_counts = [len(verses[v]) for v in sorted_vnums]
    midpoints = [m for _, _, m in silences]

    segments = dp_align(char_counts, midpoints, total_ms)

    full_verse_entries = []
    tts_entries = []

    for idx, vnum in enumerate(sorted_vnums):
        if idx >= len(segments):
            break

        start_ms, end_ms = segments[idx]
        duration_s = (end_ms - start_ms) / 1000.0
        text = verses[vnum]

        if duration_s < 0.3:
            continue

        cps = len(text) / max(duration_s, 0.01)
        quality = "good" if MIN_CPS <= cps <= MAX_CPS else "suspect"

        fname = f"{book}_{ch:02d}_v{vnum:03d}.wav"
        fv_path = full_verse_dir / fname

        try:
            extract_segment(wav_tmp, fv_path, start_ms, end_ms)
        except subprocess.CalledProcessError:
            continue

        full_verse_entries.append({
            "file": fname,
            "book": book,
            "chapter": ch,
            "verse": vnum,
            "text": text,
            "duration_s": round(duration_s, 2),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "chars_per_sec": round(cps, 1),
            "quality": quality,
        })

        sub_parts = sub_split_segment(samples, rate, start_ms, end_ms, text)
        for part_idx, (ps, pe, ptext) in enumerate(sub_parts):
            pdur = (pe - ps) / 1000.0
            if pdur < MIN_TTS_DURATION or not ptext.strip():
                continue

            pcps = len(ptext) / max(pdur, 0.01)
            if pcps < MIN_CPS or pcps > MAX_CPS:
                continue

            if len(sub_parts) == 1:
                tts_fname = fname
                tts_path = wavs_dir / tts_fname
                try:
                    extract_segment(wav_tmp, tts_path, ps, pe)
                except subprocess.CalledProcessError:
                    continue
            else:
                tts_fname = f"{book}_{ch:02d}_v{vnum:03d}_p{part_idx:02d}.wav"
                tts_path = wavs_dir / tts_fname
                try:
                    extract_segment(wav_tmp, tts_path, ps, pe)
                except subprocess.CalledProcessError:
                    continue

            tts_entries.append({
                "file": tts_fname,
                "text": ptext,
                "duration_s": round(pdur, 2),
            })

    wav_tmp.unlink(missing_ok=True)
    return full_verse_entries, tts_entries


def main():
    t0 = time.time()
    print("=" * 70)
    print("  TOL TTS DATASET BUILDER v2 — Text-Proportional DP Alignment")
    print("=" * 70)

    wavs_dir = OUTPUT_DIR / "wavs"
    full_verse_dir = OUTPUT_DIR / "full_verses"
    wavs_dir.mkdir(parents=True, exist_ok=True)
    full_verse_dir.mkdir(parents=True, exist_ok=True)

    all_full_verses = []
    all_tts = []
    stats = {"chapters": 0, "good": 0, "suspect": 0, "total_dur": 0}

    for book, (audio_prefix, n_chapters) in BOOK_MAP.items():
        print(f"\n[{book}] Processing {n_chapters} chapters...")
        book_fv, book_tts = 0, 0

        for ch in range(1, n_chapters + 1):
            audio_file = AUDIO_DIR / f"{audio_prefix}-{ch:02d}.mp3"
            text_file = TEXT_DIR / f"{book}{ch:02d}.htm"

            if not audio_file.exists() or not text_file.exists():
                continue

            fv_entries, tts_entries = process_chapter(book, ch, audio_file, text_file, wavs_dir, full_verse_dir)
            all_full_verses.extend(fv_entries)
            all_tts.extend(tts_entries)
            book_fv += len(fv_entries)
            book_tts += len(tts_entries)
            stats["chapters"] += 1

            for e in fv_entries:
                stats["total_dur"] += e["duration_s"]
                if e["quality"] == "good":
                    stats["good"] += 1
                else:
                    stats["suspect"] += 1

        print(f"  {book}: {book_fv} verses, {book_tts} TTS segments")

    fv_json = OUTPUT_DIR / "full_verses_metadata.json"
    with open(fv_json, "w", encoding="utf-8") as f:
        json.dump(all_full_verses, f, ensure_ascii=False, indent=1)

    tts_csv = OUTPUT_DIR / "metadata.csv"
    with open(tts_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|")
        writer.writerow(["file", "text", "duration_s"])
        for e in all_tts:
            writer.writerow([e["file"], e["text"], e["duration_s"]])

    tts_json = OUTPUT_DIR / "metadata.json"
    with open(tts_json, "w", encoding="utf-8") as f:
        json.dump(all_tts, f, ensure_ascii=False, indent=1)

    ljspeech = OUTPUT_DIR / "ljspeech_format.csv"
    with open(ljspeech, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|")
        for e in all_tts:
            writer.writerow([e["file"].replace(".wav", ""), e["text"], e["text"]])

    elapsed = time.time() - t0
    tts_dur = sum(e["duration_s"] for e in all_tts)
    print(f"\n{'='*70}")
    print(f"  RESULTS — v2 Text-Proportional DP Alignment")
    print(f"{'='*70}")
    print(f"  Chapters processed:    {stats['chapters']}")
    print(f"  Full-verse segments:   {len(all_full_verses):,}")
    print(f"    Good quality:        {stats['good']:,} ({stats['good']/max(1,len(all_full_verses))*100:.1f}%)")
    print(f"    Suspect quality:     {stats['suspect']:,} ({stats['suspect']/max(1,len(all_full_verses))*100:.1f}%)")
    print(f"  TTS training segments: {len(all_tts):,}")
    print(f"  Total verse audio:     {stats['total_dur']/3600:.1f} hours")
    print(f"  Total TTS audio:       {tts_dur/3600:.1f} hours")
    print(f"  Avg TTS segment:       {tts_dur/max(1,len(all_tts)):.1f}s")
    print(f"  Output:                {OUTPUT_DIR}")
    print(f"  Processing time:       {elapsed:.1f}s")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
