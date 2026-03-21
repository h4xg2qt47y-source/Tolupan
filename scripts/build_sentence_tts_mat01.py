#!/usr/bin/env python3
"""
Build sentence-level TTS training data from Matthew chapter 1 by using
word-level alignment scores to extract only high-quality sentence audio.

Reads the word alignment manifest, splits verse text into sentences,
scores each sentence by its constituent word alignment quality, and
extracts verified sentence audio from the original verse WAVs.

Output: TTS_Verified_MAT01/
  - wavs/*.wav           (sentence-level audio clips)
  - metadata.csv         (file|text|duration_s — compatible with TTS training)
  - manifest.json        (full details including quality scores)
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import torch
import torchaudio

BASE = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE / "TTS_Dataset_v2"
METADATA = DATASET_DIR / "metadata.csv"
WAV_DIR = DATASET_DIR / "wavs"
WORD_MANIFEST = BASE / "Word_Audio_MAT01" / "manifest.json"
OUTPUT_DIR = BASE / "TTS_Verified_MAT01"

MIN_AVG_QUALITY = 0.60
MIN_WORD_QUALITY = 0.10
MIN_SENTENCE_WORDS = 3
MIN_DURATION_SEC = 0.8
MAX_DURATION_SEC = 15.0

TARGET_SR = 16000

# Same intro detection as word splitter
BOOK_INTROS = {"MAT": "el evangelio segun san mateo"}
SPANISH_NUMBERS = {
    1: "uno", 2: "dos", 3: "tres", 4: "cuatro", 5: "cinco",
    6: "seis", 7: "siete", 8: "ocho", 9: "nueve", 10: "diez",
}


def detect_and_trim_intro(waveform: torch.Tensor, target_sr: int) -> torch.Tensor:
    total_samples = waveform.shape[1]
    search_limit = total_samples // 2
    win_samples = int(target_sr * 0.025)
    segment = waveform[0, :search_limit]
    if segment.numel() < win_samples * 4:
        return waveform
    energy = segment.unfold(0, win_samples, win_samples).pow(2).mean(dim=1)
    threshold = energy.median() * 0.02
    best_gap_start, best_gap_len = -1, 0
    gap_start, gap_len = -1, 0
    for i in range(len(energy)):
        if energy[i] < threshold:
            if gap_start < 0:
                gap_start = i
            gap_len = i - gap_start + 1
        else:
            if gap_len > best_gap_len and gap_start > 20:
                best_gap_start = gap_start
                best_gap_len = gap_len
            gap_start, gap_len = -1, 0
    if gap_len > best_gap_len and gap_start > 20:
        best_gap_start, best_gap_len = gap_start, gap_len
    if best_gap_len < 8:
        return waveform
    trim_sample = min((best_gap_start + best_gap_len) * win_samples, int(total_samples * 0.8))
    print(f"    Trimmed intro: {trim_sample/target_sr:.2f}s")
    return waveform[:, trim_sample:]


def load_segment_texts() -> dict[str, str]:
    texts = {}
    with open(METADATA, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("file|"):
                continue
            parts = line.split("|")
            if parts[0].startswith("MAT_01_"):
                texts[parts[0]] = parts[1]
    return texts


def split_into_sentences(text: str) -> list[str]:
    """Split text into sentences, keeping short fragments attached."""
    raw = re.split(r"(?<=[.!?])\s+", text)
    sentences = []
    carry = ""
    for s in raw:
        s = s.strip()
        if not s:
            continue
        combined = (carry + " " + s).strip() if carry else s
        if len(combined.split()) < MIN_SENTENCE_WORDS and not combined.endswith((".", "!", "?")):
            carry = combined
        else:
            sentences.append(combined)
            carry = ""
    if carry:
        if sentences:
            sentences[-1] = sentences[-1] + " " + carry
        else:
            sentences.append(carry)
    return sentences


def main():
    t0 = time.time()

    word_manifest = json.load(open(WORD_MANIFEST, encoding="utf-8"))
    seg_texts = load_segment_texts()
    print(f"Loaded {len(word_manifest)} word alignments across {len(seg_texts)} segments")

    by_segment: dict[str, list[dict]] = {}
    for w in word_manifest:
        by_segment.setdefault(w["source_segment"], []).append(w)
    for k in by_segment:
        by_segment[k].sort(key=lambda x: x["word_num"])

    out_wavs = OUTPUT_DIR / "wavs"
    out_wavs.mkdir(parents=True, exist_ok=True)

    all_entries = []
    skipped_quality = 0
    skipped_duration = 0
    skipped_words = 0
    clip_id = 0

    for seg_file in sorted(by_segment.keys()):
        words = by_segment[seg_file]
        text = seg_texts.get(seg_file, "")
        if not text:
            continue

        wav_path = WAV_DIR / seg_file
        if not wav_path.exists():
            continue

        waveform, sr = torchaudio.load(str(wav_path))
        if sr != TARGET_SR:
            waveform = torchaudio.functional.resample(waveform, sr, TARGET_SR)

        verse_m = re.match(r"MAT_01_v(\d+)", seg_file)
        verse_num = int(verse_m.group(1)) if verse_m else 0
        is_first = seg_file == "MAT_01_v001_p00.wav"
        if is_first:
            waveform = detect_and_trim_intro(waveform, TARGET_SR)

        sentences = split_into_sentences(text)
        word_idx = 0

        for sent in sentences:
            sent_words = sent.split()
            n = len(sent_words)
            if word_idx + n > len(words):
                break

            sent_word_data = words[word_idx:word_idx + n]
            word_idx += n

            qualities = [w["match_quality"] for w in sent_word_data]
            avg_q = sum(qualities) / len(qualities)
            min_q = min(qualities)

            if n < MIN_SENTENCE_WORDS:
                skipped_words += 1
                continue

            if avg_q < MIN_AVG_QUALITY or min_q < MIN_WORD_QUALITY:
                skipped_quality += 1
                continue

            start_sec = sent_word_data[0]["start_sec"]
            end_sec = sent_word_data[-1]["end_sec"]
            # Add small padding
            start_sample = max(0, int(start_sec * TARGET_SR) - int(TARGET_SR * 0.05))
            end_sample = min(waveform.shape[1], int(end_sec * TARGET_SR) + int(TARGET_SR * 0.15))

            duration = (end_sample - start_sample) / TARGET_SR
            if duration < MIN_DURATION_SEC:
                skipped_duration += 1
                continue
            if duration > MAX_DURATION_SEC:
                skipped_duration += 1
                continue

            clip_id += 1
            sent_audio = waveform[:, start_sample:end_sample]
            out_fname = f"MAT01_s{clip_id:03d}.wav"
            out_path = out_wavs / out_fname
            torchaudio.save(str(out_path), sent_audio, TARGET_SR)

            clean_text = re.sub(r"\s+", " ", sent).strip()

            entry = {
                "file": out_fname,
                "text": clean_text,
                "verse": verse_num,
                "duration_s": round(duration, 2),
                "avg_quality": round(avg_q, 3),
                "min_quality": round(min_q, 3),
                "n_words": n,
                "source_segment": seg_file,
            }
            all_entries.append(entry)

    # Write metadata.csv (TTS-compatible)
    meta_path = OUTPUT_DIR / "metadata.csv"
    with open(meta_path, "w", encoding="utf-8") as f:
        f.write("file|text|duration_s\n")
        for e in all_entries:
            f.write(f"{e['file']}|{e['text']}|{e['duration_s']}\n")

    # Write full manifest
    manifest_path = OUTPUT_DIR / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(all_entries, f, ensure_ascii=False, indent=2)

    total_dur = sum(e["duration_s"] for e in all_entries)
    avg_q_all = sum(e["avg_quality"] for e in all_entries) / len(all_entries) if all_entries else 0

    print(f"\n{'='*60}")
    print(f"  Output:            {OUTPUT_DIR}")
    print(f"  Sentences kept:    {len(all_entries)}")
    print(f"  Skipped (quality): {skipped_quality}")
    print(f"  Skipped (too short/long): {skipped_duration}")
    print(f"  Skipped (few words): {skipped_words}")
    print(f"  Total audio:       {total_dur:.1f}s ({total_dur/60:.1f} min)")
    print(f"  Avg quality:       {avg_q_all:.1%}")
    print(f"  metadata.csv:      {meta_path}")
    print(f"  Done in {time.time() - t0:.1f}s")

    # Show sample entries
    print(f"\nSample entries:")
    for e in all_entries[:8]:
        print(f"  [{e['duration_s']:5.2f}s q={e['avg_quality']:.0%}] {e['text'][:75]}")


if __name__ == "__main__":
    main()
