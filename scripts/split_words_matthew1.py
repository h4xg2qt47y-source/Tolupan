#!/usr/bin/env python3
"""
Split Matthew chapter 1 audio into individual word-level WAV clips.

Uses a two-step approach for accurate alignment:
  1. Spanish ASR (VoxPopuli) transcribes what the audio sounds like
  2. DTW character alignment maps the transcription back to known Tol text
  3. Word boundaries from the alignment are used to slice the audio

Detects and trims the Spanish preamble from the first segment.

Output: Word_Audio_MAT01/Matthew.1.{verse}.{wordnum}.{tol_word}.wav
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import numpy as np
import torch
import torchaudio

BASE = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE / "TTS_Dataset_v2"
METADATA = DATASET_DIR / "metadata.csv"
WAV_DIR = DATASET_DIR / "wavs"
OUTPUT_DIR = BASE / "Word_Audio_MAT01"

ACCENT_MAP = str.maketrans("áéíóúüǘÁÉÍÓÚÜ", "aeiouuuaeiouu")
STRIP_CHARS = set('.,;:!?¿¡()[]{}«»\u201c\u201d\u2018\u2019\u0022\u0027\u2014\u2013')

SPANISH_NUMBERS = {
    1: "uno", 2: "dos", 3: "tres", 4: "cuatro", 5: "cinco",
    6: "seis", 7: "siete", 8: "ocho", 9: "nueve", 10: "diez",
    11: "once", 12: "doce", 13: "trece", 14: "catorce", 15: "quince",
    16: "dieciseis", 17: "diecisiete", 18: "dieciocho", 19: "diecinueve",
    20: "veinte", 21: "veintiuno", 22: "veintidos", 23: "veintitres",
    24: "veinticuatro", 25: "veinticinco", 26: "veintiseis",
    27: "veintisiete", 28: "veintiocho",
}

BOOK_INTROS = {
    "MAT": "el evangelio segun san mateo",
}


def tol_to_phonetic(text: str) -> str:
    """Normalize Tol text to a phonetic form close to Spanish pronunciation."""
    t = text.lower()
    t = t.replace("tsj", "ch").replace("tj", "ch").replace("cj", "ch")
    t = t.replace("pj", "p").replace("ts'", "s").replace("ts", "s")
    for ch in ("'", "\u2019", "\u2018", "\u0027", "\u2032"):
        t = t.replace(ch, "")
    t = t.translate(ACCENT_MAP).replace("ñ", "n")
    t = re.sub(r'[.,;:!?¿¡"""\-—()\[\]{}«»\u201c\u201d]', "", t)
    return t


def load_mat01_segments() -> list[dict]:
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
            if not fname.startswith("MAT_01_"):
                continue
            m = re.match(r"MAT_01_v(\d+)", fname)
            if not m:
                continue
            verse = int(m.group(1))
            wav_path = WAV_DIR / fname
            if wav_path.exists():
                segments.append({
                    "file": fname,
                    "path": wav_path,
                    "text": text,
                    "verse": verse,
                    "duration": dur,
                })
    return segments


def ctc_decode_with_timestamps(
    emission: torch.Tensor, labels: list[str]
) -> list[tuple[str, int, float]]:
    """CTC greedy decode → list of (character, frame_index, confidence)."""
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
    """DTW character alignment. Returns target_idx → transcribed_idx mapping."""
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
            match = dp[i - 1][j - 1] + char_cost(trans_chars[i - 1], target_chars[j - 1])
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


def align_segment_spanish_dtw(
    waveform: torch.Tensor,
    target_sr: int,
    tol_text: str,
    spa_model,
    spa_labels: list[str],
) -> list[dict]:
    """Align Tol words to audio using Spanish ASR + DTW.
    Returns list of {word, start_sec, end_sec, match_quality} dicts."""
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

    # Map word boundaries
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
            "start_sec": start_sec,
            "end_sec": end_sec,
            "match_quality": match_pct,
        })

    # Fill gaps: if a word has no alignment, interpolate from neighbors
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


def detect_and_trim_intro(
    waveform: torch.Tensor,
    target_sr: int,
    book: str,
    chapter: int,
) -> torch.Tensor:
    """Detect a Spanish preamble by looking for a long silence gap in the
    first half of the audio, which separates the intro from Tol content."""
    book_intro = BOOK_INTROS.get(book)
    if not book_intro:
        return waveform

    total_samples = waveform.shape[1]
    search_limit = total_samples // 2

    win_ms = 25
    win_samples = int(target_sr * win_ms / 1000)
    segment = waveform[0, :search_limit]
    if segment.numel() < win_samples * 4:
        return waveform

    energy = segment.unfold(0, win_samples, win_samples).pow(2).mean(dim=1)
    threshold = energy.median() * 0.02

    best_gap_start = -1
    best_gap_len = 0
    gap_start = -1
    gap_len = 0

    for i in range(len(energy)):
        if energy[i] < threshold:
            if gap_start < 0:
                gap_start = i
            gap_len = i - gap_start + 1
        else:
            if gap_len > best_gap_len and gap_start > 20:
                best_gap_start = gap_start
                best_gap_len = gap_len
            gap_start = -1
            gap_len = 0
    if gap_len > best_gap_len and gap_start > 20:
        best_gap_start = gap_start
        best_gap_len = gap_len

    if best_gap_len < 8:
        return waveform

    trim_frame = best_gap_start + best_gap_len
    trim_sample = trim_frame * win_samples
    trim_sample = min(trim_sample, int(total_samples * 0.8))
    trimmed_sec = trim_sample / target_sr
    total_sec = total_samples / target_sr
    print(f"    Trimmed Spanish intro: {trimmed_sec:.2f}s of {total_sec:.2f}s removed")
    return waveform[:, trim_sample:]


def sanitize_filename(word: str) -> str:
    safe = word.replace("/", "_").replace("\\", "_")
    safe = re.sub(r'[<>:"|?*]', "", safe)
    return safe or "_"


def main():
    print("Loading models...")
    t0 = time.time()

    spa_bundle = torchaudio.pipelines.VOXPOPULI_ASR_BASE_10K_ES
    spa_model = spa_bundle.get_model()
    spa_labels = spa_bundle.get_labels()
    target_sr = spa_bundle.sample_rate
    print(f"  Spanish ASR model loaded in {time.time() - t0:.1f}s")

    segments = load_mat01_segments()
    print(f"  Found {len(segments)} MAT_01 segments")

    verse_segments: dict[int, list[dict]] = {}
    for seg in segments:
        verse_segments.setdefault(seg["verse"], []).append(seg)
    for v in verse_segments:
        verse_segments[v].sort(key=lambda s: s["file"])

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    manifest = []
    total_clips = 0
    errors = []

    for verse_num in sorted(verse_segments.keys()):
        parts = verse_segments[verse_num]
        word_offset = 0

        for part_idx, seg in enumerate(parts):
            waveform, sr = torchaudio.load(str(seg["path"]))
            if sr != target_sr:
                waveform = torchaudio.functional.resample(waveform, sr, target_sr)

            if verse_num == 1 and part_idx == 0:
                waveform = detect_and_trim_intro(waveform, target_sr, "MAT", 1)

            try:
                word_alignments = align_segment_spanish_dtw(
                    waveform, target_sr, seg["text"], spa_model, spa_labels
                )
            except Exception as e:
                errors.append(f"  Alignment failed for {seg['file']}: {e}")
                word_offset += len(seg["text"].split())
                continue

            num_samples = waveform.shape[1]

            for i, wa in enumerate(word_alignments):
                start_sample = max(0, int(wa["start_sec"] * target_sr) - 400)
                end_sample = min(num_samples, int(wa["end_sec"] * target_sr) + 400)

                # Extend end to midpoint with next word if words are adjacent
                if i + 1 < len(word_alignments):
                    next_start = int(word_alignments[i + 1]["start_sec"] * target_sr)
                    current_end = int(wa["end_sec"] * target_sr)
                    mid = (current_end + next_start) // 2
                    end_sample = min(num_samples, mid + 200)

                if end_sample <= start_sample + 100:
                    word_offset += 1
                    continue

                word_audio = waveform[:, start_sample:end_sample]
                word_num = word_offset + 1
                orig_word = re.sub(r"[.,;:!?¿¡()\"]+", "", wa["word"]).strip()
                if not orig_word:
                    orig_word = wa["word"]

                safe_word = sanitize_filename(orig_word)
                out_name = f"Matthew.1.{verse_num}.{word_num}.{safe_word}.wav"
                out_path = OUTPUT_DIR / out_name

                torchaudio.save(str(out_path), word_audio, target_sr)

                manifest.append({
                    "file": out_name,
                    "verse": verse_num,
                    "word_num": word_num,
                    "tol_word": orig_word,
                    "start_sec": round(wa["start_sec"], 3),
                    "end_sec": round(wa["end_sec"], 3),
                    "duration_sec": round((end_sample - start_sample) / target_sr, 3),
                    "match_quality": round(wa["match_quality"], 3),
                    "source_segment": seg["file"],
                })
                total_clips += 1
                word_offset += 1

    manifest_path = OUTPUT_DIR / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    # Stats
    qualities = [m["match_quality"] for m in manifest]
    good = sum(1 for q in qualities if q >= 0.5)
    fair = sum(1 for q in qualities if 0.3 <= q < 0.5)
    poor = sum(1 for q in qualities if q < 0.3)
    avg_q = sum(qualities) / len(qualities) if qualities else 0

    print(f"\n{'='*60}")
    print(f"  Output directory: {OUTPUT_DIR}")
    print(f"  Total verses:     {len(verse_segments)}")
    print(f"  Total word clips: {total_clips}")
    print(f"  Match quality:    avg={avg_q:.1%}  good={good}  fair={fair}  poor={poor}")
    print(f"  Manifest:         {manifest_path}")
    if errors:
        print(f"\n  Errors ({len(errors)}):")
        for e in errors:
            print(e)
    print(f"\n  Done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
