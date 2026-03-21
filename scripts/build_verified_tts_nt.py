#!/usr/bin/env python3
"""
Build verified sentence-level TTS training data from the ENTIRE New Testament.

Pipeline (per segment):
  1. Load audio + Tol text from TTS_Dataset_v2/metadata.csv
  2. Detect & trim Spanish preamble on the first segment of each chapter
  3. Spanish ASR (VoxPopuli) → CTC character-level transcription
  4. DTW alignment of transcription → phonetically-normalized Tol text
  5. Score each word, group into sentences, keep sentences ≥ 90% avg quality
  6. Slice verified sentence audio from original WAVs

Output: TTS_Verified/
  - wavs/*.wav         (sentence-level clips, 16 kHz mono)
  - metadata.csv       (file|text|duration_s — drop-in for TTS training)
  - manifest.json      (full per-sentence quality metadata)
  - stats.json         (summary statistics)
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

import numpy as np
import torch
import torchaudio

BASE = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE / "TTS_Dataset_v2"
METADATA = DATASET_DIR / "metadata.csv"
WAV_DIR = DATASET_DIR / "wavs"
OUTPUT_DIR = BASE / "TTS_Verified"

MIN_AVG_QUALITY = 0.90
MIN_WORD_QUALITY = 0.05
MIN_SENTENCE_WORDS = 3
MIN_DURATION_SEC = 0.8
MAX_DURATION_SEC = 15.0
TARGET_SR = 16000

ACCENT_MAP = str.maketrans(
    "áéíóúüǘÁÉÍÓÚÜ",
    "aeiouuuaeiouu",
)
STRIP_CHARS = set(
    '.,;:!?¿¡()[]{}«»\u201c\u201d\u2018\u2019\u0022\u0027\u2014\u2013'
)


# ---------------------------------------------------------------------------
# Phonetic normalization
# ---------------------------------------------------------------------------

def tol_to_phonetic(text: str) -> str:
    t = text.lower()
    t = t.replace("tsj", "ch").replace("tj", "ch").replace("cj", "ch")
    t = t.replace("pj", "p").replace("ts'", "s").replace("ts", "s")
    for ch in ("'", "\u2019", "\u2018", "\u0027", "\u2032"):
        t = t.replace(ch, "")
    t = t.translate(ACCENT_MAP).replace("ñ", "n").replace("ü", "u")
    t = re.sub(r'[.,;:!?¿¡"""\-—–()\[\]{}«»\u201c\u201d]', "", t)
    return t


# ---------------------------------------------------------------------------
# CTC decode + DTW alignment (from split_words_matthew1.py)
# ---------------------------------------------------------------------------

def ctc_decode_with_timestamps(
    emission: torch.Tensor, labels: list[str]
) -> list[tuple[str, int, float]]:
    log_probs = torch.log_softmax(emission, dim=-1)
    vals, pred_ids = log_probs.max(dim=-1)
    decoded = []
    prev = -1
    for frame_idx in range(len(pred_ids)):
        p = pred_ids[frame_idx].item()
        if p != 0 and p != prev:
            ch = labels[p] if p < len(labels) else "?"
            score = torch.exp(vals[frame_idx]).item()
            decoded.append((ch, frame_idx, score))
        prev = p
    return decoded


def dtw_align(
    trans_chars: list[str], target_chars: list[str]
) -> dict[int, int]:
    n, m = len(trans_chars), len(target_chars)
    if n == 0 or m == 0:
        return {}

    VOWELS = set("aeiou")
    FRIC = set("szchf")
    STOPS = set("ptckbdg")
    NASALS = set("nm")

    def char_cost(c1: str, c2: str) -> float:
        if c1 == c2:
            return 0.0
        if c1 in VOWELS and c2 in VOWELS:
            return 0.5
        if c1 in FRIC and c2 in FRIC:
            return 0.5
        if c1 in STOPS and c2 in STOPS:
            return 0.5
        if c1 in NASALS and c2 in NASALS:
            return 0.3
        if {c1, c2} == {"l", "r"}:
            return 0.4
        return 1.0

    INF = 1e9
    dp = np.full((n + 1, m + 1), INF, dtype=np.float32)
    bt = np.zeros((n + 1, m + 1, 2), dtype=np.int32)
    dp[0][0] = 0

    for i in range(1, n + 1):
        dp[i][0] = dp[i - 1][0] + 0.5
        bt[i][0] = [i - 1, 0]
    for j in range(1, m + 1):
        dp[0][j] = dp[0][j - 1] + 1.5
        bt[0][j] = [0, j - 1]

    for i in range(1, n + 1):
        for j in range(1, m + 1):
            match = dp[i - 1][j - 1] + char_cost(
                trans_chars[i - 1], target_chars[j - 1]
            )
            skip_t = dp[i - 1][j] + 0.5
            skip_g = dp[i][j - 1] + 1.5
            if match <= skip_t and match <= skip_g:
                dp[i][j] = match
                bt[i][j] = [i - 1, j - 1]
            elif skip_t <= skip_g:
                dp[i][j] = skip_t
                bt[i][j] = [i - 1, j]
            else:
                dp[i][j] = skip_g
                bt[i][j] = [i, j - 1]

    alignment: dict[int, int] = {}
    i, j = n, m
    while i > 0 or j > 0:
        pi, pj = int(bt[i][j][0]), int(bt[i][j][1])
        if pi == i - 1 and pj == j - 1:
            alignment[j - 1] = i - 1
        i, j = pi, pj
    return alignment


def align_words(
    waveform: torch.Tensor,
    target_sr: int,
    tol_text: str,
    spa_model,
    spa_labels: list[str],
) -> list[dict]:
    with torch.inference_mode():
        emission, _ = spa_model(waveform)
    ratio = waveform.shape[1] / emission.shape[1]

    decoded = ctc_decode_with_timestamps(emission[0], spa_labels)
    trans_chars = [(ch, frame, sc) for ch, frame, sc in decoded if ch != "|"]

    tol_words = tol_text.split()
    tol_phonetic = tol_to_phonetic(tol_text)
    target_flat = list(tol_phonetic.replace(" ", ""))

    tc = [c for c, _, _ in trans_chars]
    alignment = dtw_align(tc, target_flat)

    pos = 0
    word_results = []
    for word in tol_words:
        w_phon = tol_to_phonetic(word)
        w_len = len(w_phon)
        start_idx = pos
        end_idx = pos + w_len
        pos = end_idx

        matched_frames = []
        for tgt_i in range(start_idx, end_idx):
            if tgt_i in alignment:
                matched_frames.append(trans_chars[alignment[tgt_i]][1])

        if matched_frames:
            start_frame = min(matched_frames)
            end_frame = max(matched_frames)
            start_sec = start_frame * ratio / target_sr
            end_sec = end_frame * ratio / target_sr
            match_pct = len(matched_frames) / max(w_len, 1)
        else:
            start_sec = end_sec = -1
            match_pct = 0.0

        word_results.append({
            "word": word,
            "start_sec": round(start_sec, 4),
            "end_sec": round(end_sec, 4),
            "match_quality": round(match_pct, 3),
        })

    for i, wr in enumerate(word_results):
        if wr["start_sec"] < 0:
            prev_end = word_results[i - 1]["end_sec"] if i > 0 else 0
            next_start = None
            for j in range(i + 1, len(word_results)):
                if word_results[j]["start_sec"] >= 0:
                    next_start = word_results[j]["start_sec"]
                    break
            if next_start is None:
                next_start = waveform.shape[1] / target_sr
            wr["start_sec"] = prev_end
            wr["end_sec"] = (prev_end + next_start) / 2

    return word_results


# ---------------------------------------------------------------------------
# Intro trimming (energy-based silence detection)
# ---------------------------------------------------------------------------

def detect_and_trim_intro(waveform: torch.Tensor, sr: int) -> torch.Tensor:
    total_samples = waveform.shape[1]
    search_limit = total_samples // 2
    win_samples = int(sr * 0.025)
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

    trim_sample = min(
        (best_gap_start + best_gap_len) * win_samples,
        int(total_samples * 0.8),
    )
    return waveform[:, trim_sample:]


# ---------------------------------------------------------------------------
# Sentence splitting
# ---------------------------------------------------------------------------

def split_into_sentences(text: str) -> list[str]:
    raw = re.split(r"(?<=[.!?])\s+", text)
    sentences = []
    carry = ""
    for s in raw:
        s = s.strip()
        if not s:
            continue
        combined = (carry + " " + s).strip() if carry else s
        if (
            len(combined.split()) < MIN_SENTENCE_WORDS
            and not combined.endswith((".", "!", "?"))
        ):
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


# ---------------------------------------------------------------------------
# Load all metadata
# ---------------------------------------------------------------------------

def load_all_segments() -> list[dict]:
    segments = []
    with open(METADATA, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("file|"):
                continue
            parts = line.split("|")
            if len(parts) < 3:
                continue
            fname, text, dur = parts[0], parts[1], float(parts[2])
            m = re.match(r"([A-Z0-9]+)_(\d+)_v(\d+)", fname)
            if not m:
                continue
            book = m.group(1)
            chapter = int(m.group(2))
            verse = int(m.group(3))
            wav_path = WAV_DIR / fname
            if not wav_path.exists():
                continue
            segments.append({
                "file": fname,
                "path": wav_path,
                "text": text,
                "book": book,
                "chapter": chapter,
                "verse": verse,
                "duration": dur,
            })
    return segments


def is_first_segment_of_chapter(seg: dict, all_segs_by_chapter: dict) -> bool:
    """True if this is the first audio segment of its chapter."""
    key = (seg["book"], seg["chapter"])
    chapter_segs = all_segs_by_chapter.get(key, [])
    if not chapter_segs:
        return False
    return seg["file"] == chapter_segs[0]["file"]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    t0 = time.time()

    print("Loading Spanish ASR model...")
    spa_bundle = torchaudio.pipelines.VOXPOPULI_ASR_BASE_10K_ES
    spa_model = spa_bundle.get_model()
    spa_labels = spa_bundle.get_labels()
    target_sr = spa_bundle.sample_rate
    print(f"  Model loaded ({time.time() - t0:.1f}s), sample rate = {target_sr}")

    segments = load_all_segments()
    print(f"  Loaded {len(segments)} segments from metadata.csv")

    # Group by (book, chapter) to find first segments
    by_chapter: dict[tuple, list[dict]] = {}
    for seg in segments:
        key = (seg["book"], seg["chapter"])
        by_chapter.setdefault(key, []).append(seg)
    for k in by_chapter:
        by_chapter[k].sort(key=lambda s: s["file"])

    total_chapters = len(by_chapter)
    print(f"  {total_chapters} chapters across {len(set(s['book'] for s in segments))} books")

    out_wavs = OUTPUT_DIR / "wavs"
    out_wavs.mkdir(parents=True, exist_ok=True)

    all_entries = []
    stats = {
        "skipped_quality": 0,
        "skipped_duration": 0,
        "skipped_short_sentence": 0,
        "alignment_errors": 0,
        "total_segments_processed": 0,
        "total_words_aligned": 0,
        "intros_trimmed": 0,
    }
    clip_id = 0
    progress_interval = 500

    for seg_idx, seg in enumerate(segments):
        if seg_idx % progress_interval == 0:
            elapsed = time.time() - t0
            pct = seg_idx / len(segments) * 100
            print(
                f"  [{seg_idx:>6}/{len(segments)}] {pct:5.1f}%  "
                f"{elapsed:>6.0f}s  clips={len(all_entries)}  "
                f"book={seg['book']} ch={seg['chapter']}"
            )
            sys.stdout.flush()

        wav_path = seg["path"]
        try:
            waveform, sr = torchaudio.load(str(wav_path))
        except Exception:
            stats["alignment_errors"] += 1
            continue

        if sr != target_sr:
            waveform = torchaudio.functional.resample(waveform, sr, target_sr)

        if waveform.shape[0] > 1:
            waveform = waveform[:1]

        # Trim Spanish intro on first segment of each chapter
        if is_first_segment_of_chapter(seg, by_chapter):
            original_len = waveform.shape[1]
            waveform = detect_and_trim_intro(waveform, target_sr)
            if waveform.shape[1] < original_len:
                stats["intros_trimmed"] += 1

        # Run word alignment
        try:
            word_results = align_words(
                waveform, target_sr, seg["text"], spa_model, spa_labels
            )
        except Exception:
            stats["alignment_errors"] += 1
            continue

        stats["total_segments_processed"] += 1
        stats["total_words_aligned"] += len(word_results)

        # Split text into sentences and map to aligned words
        sentences = split_into_sentences(seg["text"])
        word_idx = 0

        for sent in sentences:
            sent_words = sent.split()
            n = len(sent_words)
            if word_idx + n > len(word_results):
                break

            sent_word_data = word_results[word_idx:word_idx + n]
            word_idx += n

            if n < MIN_SENTENCE_WORDS:
                stats["skipped_short_sentence"] += 1
                continue

            qualities = [w["match_quality"] for w in sent_word_data]
            avg_q = sum(qualities) / len(qualities)
            min_q = min(qualities)

            if avg_q < MIN_AVG_QUALITY or min_q < MIN_WORD_QUALITY:
                stats["skipped_quality"] += 1
                continue

            start_sec = sent_word_data[0]["start_sec"]
            end_sec = sent_word_data[-1]["end_sec"]
            pad_start = int(target_sr * 0.05)
            pad_end = int(target_sr * 0.15)
            start_sample = max(0, int(start_sec * target_sr) - pad_start)
            end_sample = min(
                waveform.shape[1], int(end_sec * target_sr) + pad_end
            )

            duration = (end_sample - start_sample) / target_sr
            if duration < MIN_DURATION_SEC or duration > MAX_DURATION_SEC:
                stats["skipped_duration"] += 1
                continue

            clip_id += 1
            sent_audio = waveform[:, start_sample:end_sample]
            out_fname = f"{seg['book']}_{seg['chapter']:02d}_s{clip_id:06d}.wav"
            torchaudio.save(str(out_wavs / out_fname), sent_audio, target_sr)

            clean_text = re.sub(r"\s+", " ", sent).strip()
            all_entries.append({
                "file": out_fname,
                "text": clean_text,
                "duration_s": round(duration, 2),
                "avg_quality": round(avg_q, 3),
                "min_quality": round(min_q, 3),
                "n_words": n,
                "book": seg["book"],
                "chapter": seg["chapter"],
                "verse": seg["verse"],
                "source_segment": seg["file"],
            })

    # --- Write outputs ---
    meta_path = OUTPUT_DIR / "metadata.csv"
    with open(meta_path, "w", encoding="utf-8") as f:
        f.write("file|text|duration_s\n")
        for e in all_entries:
            f.write(f"{e['file']}|{e['text']}|{e['duration_s']}\n")

    manifest_path = OUTPUT_DIR / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(all_entries, f, ensure_ascii=False, indent=2)

    # --- Summary ---
    total_dur = sum(e["duration_s"] for e in all_entries)
    avg_q = (
        sum(e["avg_quality"] for e in all_entries) / len(all_entries)
        if all_entries
        else 0
    )

    by_book_stats: dict[str, dict] = {}
    for e in all_entries:
        b = e["book"]
        by_book_stats.setdefault(b, {"count": 0, "dur": 0.0})
        by_book_stats[b]["count"] += 1
        by_book_stats[b]["dur"] += e["duration_s"]

    summary = {
        "total_sentences": len(all_entries),
        "total_duration_sec": round(total_dur, 1),
        "total_duration_min": round(total_dur / 60, 1),
        "avg_quality": round(avg_q, 3),
        "min_avg_quality_threshold": MIN_AVG_QUALITY,
        "processing_time_sec": round(time.time() - t0, 1),
        "segments_processed": stats["total_segments_processed"],
        "words_aligned": stats["total_words_aligned"],
        "intros_trimmed": stats["intros_trimmed"],
        "skipped_quality": stats["skipped_quality"],
        "skipped_duration": stats["skipped_duration"],
        "skipped_short": stats["skipped_short_sentence"],
        "alignment_errors": stats["alignment_errors"],
        "by_book": {
            b: {"sentences": d["count"], "duration_min": round(d["dur"] / 60, 1)}
            for b, d in sorted(by_book_stats.items())
        },
    }
    stats_path = OUTPUT_DIR / "stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - t0
    print(f"\n{'=' * 65}")
    print(f"  VERIFIED TTS DATASET — COMPLETE")
    print(f"{'=' * 65}")
    print(f"  Output:              {OUTPUT_DIR}")
    print(f"  Sentences kept:      {len(all_entries)}")
    print(f"  Total audio:         {total_dur:.0f}s ({total_dur / 60:.1f} min)")
    print(f"  Avg quality:         {avg_q:.1%}")
    print(f"  Quality threshold:   >= {MIN_AVG_QUALITY:.0%} avg")
    print(f"  Intros trimmed:      {stats['intros_trimmed']}")
    print(f"  Skipped (quality):   {stats['skipped_quality']}")
    print(f"  Skipped (duration):  {stats['skipped_duration']}")
    print(f"  Skipped (short):     {stats['skipped_short_sentence']}")
    print(f"  Errors:              {stats['alignment_errors']}")
    print(f"  Processing time:     {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    print(f"\n  Per-book breakdown:")
    for b, d in sorted(by_book_stats.items()):
        print(f"    {b:>5}: {d['count']:>5} sentences, {d['dur'] / 60:>6.1f} min")
    print(f"\n  metadata.csv: {meta_path}")
    print(f"  manifest:     {manifest_path}")
    print(f"  stats:        {stats_path}")


if __name__ == "__main__":
    main()
