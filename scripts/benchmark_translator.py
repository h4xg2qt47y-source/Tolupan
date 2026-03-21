#!/usr/bin/env python3
"""
Benchmark: translate 1000 random English NT phrases to Tol,
then score against the actual Tol Bible verse to see if the
translated words appear in the same order.
"""

import json
import random
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
from translator import TolTranslator, tol_phonetic_normalize

SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 42
NUM_SAMPLES = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
MIN_WORDS = int(sys.argv[3]) if len(sys.argv) > 3 else 2
MAX_WORDS = int(sys.argv[4]) if len(sys.argv) > 4 else 10
random.seed(SEED)

DB_PATH = Path(__file__).resolve().parent.parent / "app" / "data" / "tol.db"
t = TolTranslator()

db = sqlite3.connect(str(DB_PATH))
db.row_factory = sqlite3.Row

rows = db.execute("""
    SELECT source, tol, spanish, english FROM parallel_sentences
    WHERE source LIKE 'bible_align:%' AND source LIKE '%:%:%'
      AND english IS NOT NULL AND english != ''
      AND tol IS NOT NULL AND tol != ''
""").fetchall()

verses = []
for r in rows:
    m = re.match(r"bible_align:([A-Z0-9]+?)(\d{2}):(\d+)", r["source"])
    if not m or int(m.group(3)) == 0:
        continue
    verses.append({
        "book": m.group(1),
        "chapter": int(m.group(2)),
        "verse": int(m.group(3)),
        "tol": r["tol"],
        "english": r["english"],
        "source": r["source"],
    })

# ── Generate test phrases ──────────────────────────────────────────

_SENTENCE_END = re.compile(r'[.!?]')

def _extract_sentences(text):
    """Split English text into sentence-like chunks at . ! ? boundaries."""
    text = re.sub(r'["""\u201c\u201d\u2018\u2019]', '', text)
    parts = _SENTENCE_END.split(text)
    return [p.strip() for p in parts if p.strip()]

samples = []
attempts = 0
while len(samples) < NUM_SAMPLES and attempts < NUM_SAMPLES * 20:
    attempts += 1
    v = random.choice(verses)
    sentences = _extract_sentences(v["english"])
    if not sentences:
        continue
    sent = random.choice(sentences)
    words = sent.split()
    if len(words) < MIN_WORDS:
        continue
    if len(words) <= MAX_WORDS:
        phrase = sent
    else:
        start = random.randint(0, len(words) - MIN_WORDS)
        end = min(start + random.randint(MIN_WORDS, MAX_WORDS), len(words))
        phrase = " ".join(words[start:end])
    phrase = phrase.strip()
    if len(phrase) < 5 or len(phrase.split()) < MIN_WORDS:
        continue
    samples.append({"phrase": phrase, "verse": v})


def normalize_tol(text):
    """Normalize Tol text for comparison: lowercase, strip punctuation, normalize phonetics."""
    text = text.lower()
    text = re.sub(r'["""\u201c\u201d\u2018\u2019,;:!?\.\(\)\[\]\{\}\-=]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def score_ordered_word_match(translated_tol, reference_tol):
    """
    Score how many words from the translation appear in the reference
    in the same relative order. Returns (matched_count, total_translated, score).
    
    Uses longest common subsequence of words.
    """
    trans_norm = normalize_tol(translated_tol)
    ref_norm = normalize_tol(reference_tol)

    trans_words = [w for w in trans_norm.split() if len(w) > 1]
    ref_words = ref_norm.split()

    if not trans_words:
        return 0, 0, 0.0

    # Filter out bracket-wrapped untranslated words like [the], [and]
    real_trans = [w for w in trans_words if not w.startswith('[') and not w.endswith(']')]
    if not real_trans:
        return 0, len(trans_words), 0.0

    # LCS (longest common subsequence) for ordered matching
    n, m = len(real_trans), len(ref_words)
    dp = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            if real_trans[i - 1] == ref_words[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])

    lcs_len = dp[n][m]
    score = lcs_len / len(real_trans) if real_trans else 0.0

    return lcs_len, len(real_trans), score


def score_unordered_word_match(translated_tol, reference_tol):
    """Score how many translated words appear anywhere in the reference (bag of words)."""
    trans_norm = normalize_tol(translated_tol)
    ref_norm = normalize_tol(reference_tol)

    trans_words = [w for w in trans_norm.split() if len(w) > 1 and not w.startswith('[')]
    ref_words_set = set(ref_norm.split())

    if not trans_words:
        return 0, 0, 0.0

    hits = sum(1 for w in trans_words if w in ref_words_set)
    return hits, len(trans_words), hits / len(trans_words)


# ── Run the benchmark ────────────────────────────────────────────────

print("=" * 80)
print(f"TRANSLATOR BENCHMARK: {NUM_SAMPLES} Random English NT Phrases ({MIN_WORDS}-{MAX_WORDS} words) → Tol  [seed={SEED}]")
print("=" * 80)
print()

results = []
t0 = time.time()

for i, s in enumerate(samples):
    phrase = s["phrase"]
    ref_tol = s["verse"]["tol"]
    ref_source = s["verse"]["source"]

    result = t.translate(phrase, "en", "tol")
    translated = result["translation"]
    method = result["method"]

    lcs_matched, lcs_total, lcs_score = score_ordered_word_match(translated, ref_tol)
    bow_matched, bow_total, bow_score = score_unordered_word_match(translated, ref_tol)

    results.append({
        "phrase": phrase,
        "translated": translated,
        "reference": ref_tol,
        "source": ref_source,
        "method": method,
        "lcs_score": lcs_score,
        "lcs_matched": lcs_matched,
        "lcs_total": lcs_total,
        "bow_score": bow_score,
        "bow_matched": bow_matched,
        "bow_total": bow_total,
        "word_count": len(phrase.split()),
    })

    if (i + 1) % 500 == 0:
        elapsed = time.time() - t0
        print(f"  ... {i+1}/{NUM_SAMPLES} done ({elapsed:.1f}s)")

elapsed = time.time() - t0
print(f"\nCompleted {NUM_SAMPLES} translations in {elapsed:.1f}s ({elapsed/NUM_SAMPLES*1000:.1f}ms avg)\n")

# ── Aggregate Results ────────────────────────────────────────────────

lcs_scores = [r["lcs_score"] for r in results]
bow_scores = [r["bow_score"] for r in results]

N = len(results)
print("─── ORDERED MATCH (LCS) ───")
print(f"  Average score:     {sum(lcs_scores)/N:.1%}")
print(f"  Perfect (100%):    {sum(1 for s in lcs_scores if s == 1.0):>5} / {N}")
print(f"  Good (≥50%):       {sum(1 for s in lcs_scores if s >= 0.5):>5} / {N}")
print(f"  Some (≥25%):       {sum(1 for s in lcs_scores if s >= 0.25):>5} / {N}")
print(f"  Any match (>0%):   {sum(1 for s in lcs_scores if s > 0):>5} / {N}")
print(f"  Zero match:        {sum(1 for s in lcs_scores if s == 0):>5} / {N}")

print(f"\n─── UNORDERED MATCH (Bag of Words) ───")
print(f"  Average score:     {sum(bow_scores)/N:.1%}")
print(f"  Perfect (100%):    {sum(1 for s in bow_scores if s == 1.0):>5} / {N}")
print(f"  Good (≥50%):       {sum(1 for s in bow_scores if s >= 0.5):>5} / {N}")
print(f"  Some (≥25%):       {sum(1 for s in bow_scores if s >= 0.25):>5} / {N}")
print(f"  Any match (>0%):   {sum(1 for s in bow_scores if s > 0):>5} / {N}")
print(f"  Zero match:        {sum(1 for s in bow_scores if s == 0):>5} / {N}")

# ── By phrase length ─────────────────────────────────────────────────

print(f"\n─── SCORES BY PHRASE LENGTH ───")
print(f"  {'Words':>5}  {'Count':>5}  {'LCS avg':>8}  {'BoW avg':>8}  {'LCS≥50%':>8}  {'BoW≥50%':>8}")
for wc in range(MIN_WORDS, MAX_WORDS + 1):
    subset = [r for r in results if r["word_count"] == wc]
    if not subset:
        continue
    avg_lcs = sum(r["lcs_score"] for r in subset) / len(subset)
    avg_bow = sum(r["bow_score"] for r in subset) / len(subset)
    lcs_good = sum(1 for r in subset if r["lcs_score"] >= 0.5)
    bow_good = sum(1 for r in subset if r["bow_score"] >= 0.5)
    print(f"  {wc:>5}  {len(subset):>5}  {avg_lcs:>7.1%}  {avg_bow:>7.1%}  {lcs_good:>5}/{len(subset):<3}  {bow_good:>5}/{len(subset):<3}")

# ── By translation method ────────────────────────────────────────────

print(f"\n─── SCORES BY TRANSLATION METHOD ───")
method_groups = defaultdict(list)
for r in results:
    method_groups[r["method"]].append(r)

print(f"  {'Method':40s}  {'Count':>5}  {'LCS avg':>8}  {'BoW avg':>8}")
for method in sorted(method_groups, key=lambda m: -len(method_groups[m])):
    subset = method_groups[method]
    avg_lcs = sum(r["lcs_score"] for r in subset) / len(subset)
    avg_bow = sum(r["bow_score"] for r in subset) / len(subset)
    print(f"  {method:40s}  {len(subset):>5}  {avg_lcs:>7.1%}  {avg_bow:>7.1%}")

# ── Score distribution histogram ─────────────────────────────────────

print(f"\n─── LCS SCORE DISTRIBUTION ───")
buckets = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]
for i in range(len(buckets) - 1):
    lo, hi = buckets[i], buckets[i + 1]
    count = sum(1 for s in lcs_scores if lo <= s < hi)
    bar = "█" * (count // 5)
    label = f"{lo:.0%}-{min(hi, 1.0):.0%}"
    print(f"  {label:>8}  {count:>4}  {bar}")

# ── Sample good and bad translations ─────────────────────────────────

print(f"\n─── TOP 15 BEST TRANSLATIONS (by LCS) ───")
best = sorted(results, key=lambda r: -r["lcs_score"])
for r in best[:15]:
    print(f"  [{r['lcs_score']:.0%}] EN: \"{r['phrase']}\"")
    print(f"       Tol translated: {r['translated'][:80]}")
    print(f"       Tol reference:  {r['reference'][:80]}")
    print(f"       Method: {r['method']}")
    print()

print(f"\n─── 15 WORST TRANSLATIONS (by LCS, excluding zero) ───")
nonzero = [r for r in results if r["lcs_score"] > 0]
worst = sorted(nonzero, key=lambda r: r["lcs_score"])
for r in worst[:15]:
    print(f"  [{r['lcs_score']:.0%}] EN: \"{r['phrase']}\"")
    print(f"       Tol translated: {r['translated'][:80]}")
    print(f"       Tol reference:  {r['reference'][:80]}")
    print(f"       Method: {r['method']}")
    print()

print(f"\n─── 10 SAMPLE ZERO-MATCH TRANSLATIONS ───")
zeros = [r for r in results if r["lcs_score"] == 0]
for r in random.sample(zeros, min(10, len(zeros))):
    print(f"  EN: \"{r['phrase']}\"")
    print(f"       Tol translated: {r['translated'][:80]}")
    print(f"       Tol reference:  {r['reference'][:80]}")
    print(f"       Method: {r['method']}")
    print()

# Save full results
with open("/tmp/benchmark_results.json", "w") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print(f"Full results saved to /tmp/benchmark_results.json")
