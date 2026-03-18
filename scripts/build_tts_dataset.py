#!/usr/bin/env python3
"""
TTS Dataset Builder — Tol Language
====================================
Aligns chapter-level audio with verse-level text to create
training data for a Text-to-Speech model.

Pipeline:
  1. Parse written Tol NT text → per-chapter verse lists
  2. Split chapter audio using silence detection → segments
  3. Map segments to verses (proportional + heuristic alignment)
  4. Export: WAV snippets + metadata CSV (text | audio_path | duration)
"""

import csv
import json
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
OUTPUT_DIR = BASE / "TTS_Dataset"
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


def parse_verses(html_path):
    """Extract verses from Tol NT HTML file."""
    text = html_path.read_text(encoding="utf-8")
    text = text.replace("&#160;", " ").replace("&nbsp;", " ")
    text = unescape(text)
    text = re.sub(r'<span class="popup">.*?</span>', '', text)
    text = re.sub(r'<a[^>]*class="notemark"[^>]*>.*?</a>', '', text)
    text = re.sub(r"<span class=['\"]add['\"]>(.*?)</span>", r"\1", text)

    verses = {}
    for m in re.finditer(
        r'<span\s+class="verse"\s+id="V(\d+)">\s*\d+\s*</span>(.*?)'
        r'(?=<span\s+class="verse"|<ul\s|<div\s+class=[\'"](?:footnote|copyright|tnav|s\b|r\b))',
        text, re.DOTALL
    ):
        vnum = int(m.group(1))
        clean = re.sub(r'<[^>]+>', ' ', m.group(2))
        clean = re.sub(r'\s+', ' ', clean).strip()
        if clean:
            verses[vnum] = clean
    return verses


def mp3_to_wav(mp3_path, wav_path):
    """Convert MP3 to 16kHz mono WAV."""
    subprocess.run(
        [str(FFMPEG), "-y", "-i", str(mp3_path),
         "-ar", "16000", "-ac", "1", "-sample_fmt", "s16",
         str(wav_path)],
        capture_output=True, check=True,
    )
    return wav_path


def read_wav_samples(wav_path):
    """Read raw PCM samples from 16-bit mono WAV."""
    with wave.open(str(wav_path), 'rb') as wf:
        n_frames = wf.getnframes()
        rate = wf.getframerate()
        raw = wf.readframes(n_frames)
        samples = struct.unpack(f'<{n_frames}h', raw)
        return list(samples), rate


def detect_silences(samples, rate, min_silence_ms=350, silence_thresh=400):
    """
    Find silence regions in PCM samples.
    Returns list of (start_ms, end_ms) for each silence gap.
    """
    min_samples = int(rate * min_silence_ms / 1000)
    silences = []
    in_silence = False
    start = 0

    for i in range(0, len(samples), int(rate * 0.01)):
        chunk = samples[i:i + int(rate * 0.01)]
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
                    silences.append((start_ms, end_ms))
                in_silence = False

    if in_silence and (len(samples) - start) >= min_samples:
        silences.append((int(start * 1000 / rate), int(len(samples) * 1000 / rate)))

    return silences


def split_at_silences(silences, total_duration_ms, n_verses):
    """
    Given silence gaps, find the best N-1 split points to divide audio
    into N segments corresponding to N verses.
    Uses proportional distribution when there are more silences than needed.
    """
    if not silences or n_verses <= 1:
        return [(0, total_duration_ms)]

    midpoints = [(s + e) // 2 for s, e in silences]

    if len(midpoints) >= n_verses - 1:
        ideal = [int(total_duration_ms * (i + 1) / n_verses) for i in range(n_verses - 1)]
        chosen = []
        used = set()
        for target in ideal:
            best_idx = None
            best_dist = float('inf')
            for j, mp in enumerate(midpoints):
                if j not in used:
                    d = abs(mp - target)
                    if d < best_dist:
                        best_dist = d
                        best_idx = j
            if best_idx is not None:
                chosen.append(midpoints[best_idx])
                used.add(best_idx)
        chosen.sort()
    else:
        chosen = sorted(midpoints)

    boundaries = [0] + chosen + [total_duration_ms]
    segments = []
    for i in range(len(boundaries) - 1):
        segments.append((boundaries[i], boundaries[i + 1]))
    return segments


def extract_segment(wav_path, out_path, start_ms, end_ms):
    """Extract a segment from a WAV file using ffmpeg."""
    start_s = start_ms / 1000.0
    duration_s = (end_ms - start_ms) / 1000.0
    subprocess.run(
        [str(FFMPEG), "-y", "-i", str(wav_path),
         "-ss", f"{start_s:.3f}", "-t", f"{duration_s:.3f}",
         "-ar", "22050", "-ac", "1", "-sample_fmt", "s16",
         str(out_path)],
        capture_output=True, check=True,
    )


def process_chapter(book, ch, audio_path, text_path, out_dir):
    """Process one chapter: align audio with text, export segments."""
    verses = parse_verses(text_path)
    if not verses:
        return []

    sorted_vnums = sorted(verses.keys())
    n_verses = len(sorted_vnums)

    wav_tmp = out_dir / f"_tmp_{book}{ch:02d}.wav"
    try:
        mp3_to_wav(audio_path, wav_tmp)
    except subprocess.CalledProcessError:
        return []

    samples, rate = read_wav_samples(wav_tmp)
    total_ms = int(len(samples) * 1000 / rate)

    silences = detect_silences(samples, rate, min_silence_ms=300, silence_thresh=500)

    if len(silences) < n_verses - 1:
        silences = detect_silences(samples, rate, min_silence_ms=200, silence_thresh=350)

    segments = split_at_silences(silences, total_ms, n_verses)

    results = []
    for idx, vnum in enumerate(sorted_vnums):
        if idx < len(segments):
            start_ms, end_ms = segments[idx]
        else:
            start_ms = segments[-1][1] if segments else 0
            end_ms = total_ms

        duration_s = (end_ms - start_ms) / 1000.0
        if duration_s < 0.3:
            continue

        fname = f"{book}_{ch:02d}_v{vnum:03d}.wav"
        seg_path = out_dir / fname

        try:
            extract_segment(wav_tmp, seg_path, start_ms, end_ms)
        except subprocess.CalledProcessError:
            continue

        results.append({
            "file": fname,
            "book": book,
            "chapter": ch,
            "verse": vnum,
            "text": verses[vnum],
            "duration_s": round(duration_s, 2),
            "start_ms": start_ms,
            "end_ms": end_ms,
        })

    wav_tmp.unlink(missing_ok=True)
    return results


def main():
    t0 = time.time()
    print("=" * 70)
    print("  TOL TTS DATASET BUILDER")
    print("=" * 70)

    wavs_dir = OUTPUT_DIR / "wavs"
    wavs_dir.mkdir(parents=True, exist_ok=True)

    all_entries = []
    total_chapters = 0
    total_duration = 0

    for book, (audio_prefix, n_chapters) in BOOK_MAP.items():
        print(f"\n[{book}] Processing {n_chapters} chapters...")
        book_entries = 0

        for ch in range(1, n_chapters + 1):
            audio_file = AUDIO_DIR / f"{audio_prefix}-{ch:02d}.mp3"
            text_file = TEXT_DIR / f"{book}{ch:02d}.htm"

            if not audio_file.exists():
                continue
            if not text_file.exists():
                continue

            entries = process_chapter(book, ch, audio_file, text_file, wavs_dir)
            all_entries.extend(entries)
            book_entries += len(entries)
            total_chapters += 1

            ch_dur = sum(e["duration_s"] for e in entries)
            total_duration += ch_dur

        print(f"  {book}: {book_entries} verse segments extracted")

    metadata_csv = OUTPUT_DIR / "metadata.csv"
    with open(metadata_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|")
        writer.writerow(["file", "text", "book", "chapter", "verse", "duration_s"])
        for e in all_entries:
            writer.writerow([e["file"], e["text"], e["book"], e["chapter"], e["verse"], e["duration_s"]])

    metadata_json = OUTPUT_DIR / "metadata.json"
    with open(metadata_json, "w", encoding="utf-8") as f:
        json.dump(all_entries, f, ensure_ascii=False, indent=1)

    ljspeech_csv = OUTPUT_DIR / "ljspeech_format.csv"
    with open(ljspeech_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="|")
        for e in all_entries:
            writer.writerow([e["file"].replace(".wav", ""), e["text"], e["text"]])

    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  RESULTS")
    print(f"{'='*70}")
    print(f"  Chapters processed: {total_chapters}")
    print(f"  Verse segments:     {len(all_entries):,}")
    print(f"  Total audio:        {total_duration/3600:.1f} hours ({total_duration:.0f}s)")
    print(f"  Average segment:    {total_duration/max(1,len(all_entries)):.1f}s")
    print(f"  Output directory:   {OUTPUT_DIR}")
    print(f"  Metadata CSV:       {metadata_csv}")
    print(f"  LJSpeech format:    {ljspeech_csv}")
    print(f"  Processing time:    {elapsed:.1f}s")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
