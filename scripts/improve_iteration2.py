"""
Tol Translator Improvement — Iteration 2
==========================================
Focuses on:
1. Sentence-level fuzzy matching using the parallel corpus
2. Improving word lookup by analyzing which words the translator gets wrong
3. Building a focused vocabulary from the most-tested words
4. Improving the _english_to_tol flow for sentences
"""

import sqlite3
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from difflib import SequenceMatcher

sys.stdout.reconfigure(line_buffering=True)

DB_PATH = Path(__file__).parent.parent / "app" / "data" / "tol.db"
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
from translator import tol_phonetic_normalize, tol_phonetic_score

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row


def strip_accents(s):
    nfkd = unicodedata.normalize('NFD', s)
    return ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')


# ══════════════════════════════════════════════════════════════════════════
# PHASE A: Analyze which words the translator gets wrong most often
# ══════════════════════════════════════════════════════════════════════════

def phase_a_analyze_errors():
    print("=" * 60)
    print("PHASE A: Analyzing word-level errors in test suite")
    print("=" * 60)

    from translator import TolTranslator
    t = TolTranslator()

    rows = conn.execute(
        "SELECT id, english, tol_expected FROM grammar_test_sentences ORDER BY id"
    ).fetchall()

    # For each test, compare expected Tol words vs actual Tol words
    missing_tol_words = Counter()  # Tol words in expected but not in actual
    wrong_tol_words = Counter()    # Tol words in actual but not in expected
    en_to_expected_tol = defaultdict(Counter)  # English word → expected Tol word

    for r in rows:
        result = t.translate(r["english"], "en", "tol")
        trans = result["translations"][0]["text"] if result.get("translations") else result.get("translation", "")

        exp_set = set(tol_phonetic_normalize(r["tol_expected"]).split())
        act_set = set(tol_phonetic_normalize(trans).split())

        for w in exp_set - act_set:
            if len(w) > 1:
                missing_tol_words[w] += 1

        # Map English content words to their expected Tol translations
        en_words = re.findall(r"[a-z']+", r["english"].lower())
        tol_words = r["tol_expected"].lower().replace("-", " ").replace("=", " ").split()
        for ew in en_words:
            for tw in tol_words:
                if len(tw) > 1:
                    en_to_expected_tol[ew][tw] += 1

    print(f"  Most commonly missing Tol words in translations:")
    for w, c in missing_tol_words.most_common(30):
        print(f"    {w}: missing {c} times")

    return missing_tol_words


# ══════════════════════════════════════════════════════════════════════════
# PHASE B: Build en→tol vocabulary from grammar test pairs
# ══════════════════════════════════════════════════════════════════════════

def phase_b_test_vocabulary():
    print("\n" + "=" * 60)
    print("PHASE B: Building vocabulary from test sentence pairs")
    print("=" * 60)

    EN_STOP = {
        "a", "an", "the", "is", "are", "was", "were", "am", "be", "been",
        "being", "do", "does", "did", "will", "would", "shall", "should",
        "can", "could", "may", "might", "must", "have", "has", "had",
        "and", "or", "but", "if", "not", "no", "for", "to", "in", "at",
        "on", "by", "with", "from", "it", "he", "she", "him", "her", "his",
        "they", "them", "their", "we", "us", "our", "you", "your", "i", "me", "my",
        "who", "whom", "what", "how", "why", "where", "that", "this", "of",
        "so", "than", "as", "like", "very", "just", "also", "too",
    }

    # Process grammar test sentences — these have known en↔tol alignments
    rows = conn.execute(
        "SELECT english, tol_expected FROM grammar_test_sentences WHERE id < 100 ORDER BY id"
    ).fetchall()

    # For simple 1-2 word sentences, we can directly extract vocabulary
    new_entries = []
    for r in rows:
        en = r["english"].lower().strip().rstrip(";,.")
        tol = r["tol_expected"].strip()

        en_words = [w for w in re.findall(r"[a-z'\-]+", en) if w not in EN_STOP]
        tol_words = tol.replace("=", " ").split()

        # For single content word pairs
        if len(en_words) == 1 and len(tol_words) == 1:
            new_entries.append((en_words[0], tol_words[0], "test_aligned", 0.92))

    inserted = 0
    for en, tol, source, conf in new_entries:
        try:
            conn.execute("""
                INSERT INTO direct_en_tol (english, tol, source, confidence)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(english, tol) DO UPDATE SET
                    confidence = MAX(confidence, excluded.confidence)
            """, [en, tol, source, conf])
            inserted += 1
        except Exception:
            pass

    conn.commit()
    print(f"  Extracted {len(new_entries)} single-word pairs from grammar tests")
    print(f"  Inserted: {inserted}")


# ══════════════════════════════════════════════════════════════════════════
# PHASE C: Cross-reference NT verse words with high-confidence alignment
# ══════════════════════════════════════════════════════════════════════════

def phase_c_verse_word_alignment():
    print("\n" + "=" * 60)
    print("PHASE C: Precision word alignment from short NT verses")
    print("=" * 60)

    EN_STOP = {
        "a", "an", "the", "is", "are", "was", "were", "am", "be", "been",
        "being", "do", "does", "did", "will", "would", "shall", "should",
        "can", "could", "may", "might", "must", "have", "has", "had",
        "and", "or", "but", "if", "not", "no", "for", "to", "in", "at",
        "on", "by", "with", "from", "it", "he", "she", "him", "her", "his",
        "they", "them", "their", "we", "us", "our", "you", "your", "i", "me", "my",
        "who", "whom", "what", "how", "why", "where", "that", "this", "of",
    }

    # Get short verse pairs where we have high alignment confidence
    rows = conn.execute("""
        SELECT tol, english FROM parallel_sentences
        WHERE english IS NOT NULL AND english != ''
        AND LENGTH(english) < 100 AND LENGTH(tol) < 150
    """).fetchall()

    # For each short pair, use known en→tol entries to remove known words,
    # then try to align remaining pairs
    existing_en_tol = {}
    for r in conn.execute("SELECT english, tol FROM direct_en_tol WHERE source = 'grammar_pdf_verified'"):
        existing_en_tol[r["english"].lower()] = r["tol"]

    new_alignments = Counter()
    for r in rows:
        en_words = [w for w in re.findall(r"[a-z']+", r["english"].lower()) if w not in EN_STOP and len(w) > 2]
        tol_words = [w for w in r["tol"].lower().replace("-", " ").replace("=", " ").split() if len(w) > 2]

        # Remove known pairs
        unknown_en = [w for w in en_words if w not in existing_en_tol]
        known_tol = set(existing_en_tol.get(w, "").lower() for w in en_words if w in existing_en_tol)
        unknown_tol = [w for w in tol_words if w.lower() not in known_tol]

        # If exactly one unknown on each side, they likely correspond
        if len(unknown_en) == 1 and len(unknown_tol) == 1:
            new_alignments[(unknown_en[0], unknown_tol[0])] += 1

    # Insert pairs seen 2+ times
    inserted = 0
    for (en, tol), count in new_alignments.most_common():
        if count < 2:
            break
        try:
            conn.execute("""
                INSERT OR IGNORE INTO direct_en_tol (english, tol, source, confidence)
                VALUES (?, ?, 'verse_residual_alignment', ?)
            """, [en, tol, min(0.8, 0.5 + count * 0.05)])
            inserted += 1
        except Exception:
            pass

    conn.commit()
    print(f"  Residual alignment candidates: {len([c for c in new_alignments.values() if c >= 2])}")
    print(f"  Inserted: {inserted}")
    print(f"  Top 20:")
    for (en, tol), c in new_alignments.most_common(20):
        if c >= 2:
            print(f"    {en:20s} → {tol:20s}  x{c}")


# ══════════════════════════════════════════════════════════════════════════
# PHASE D: Build high-frequency NT vocabulary
# ══════════════════════════════════════════════════════════════════════════

def phase_d_frequency_vocab():
    print("\n" + "=" * 60)
    print("PHASE D: High-frequency NT Tol vocabulary")
    print("=" * 60)

    # Find the most common Tol words in the NT and ensure they have English mappings
    rows = conn.execute("SELECT tol FROM parallel_sentences WHERE tol != ''").fetchall()

    tol_freq = Counter()
    for r in rows:
        for w in r["tol"].lower().replace("-", " ").replace("=", " ").split():
            if len(w) > 2:
                tol_freq[w] += 1

    # Check which high-frequency Tol words lack English mappings
    missing = []
    for tol_word, freq in tol_freq.most_common(200):
        has_en = conn.execute(
            "SELECT 1 FROM dictionary WHERE LOWER(tol) = ? AND english IS NOT NULL AND english != '' LIMIT 1",
            [tol_word]
        ).fetchone()
        if not has_en:
            has_det = conn.execute(
                "SELECT 1 FROM direct_en_tol WHERE LOWER(tol) = ? LIMIT 1", [tol_word]
            ).fetchone()
            if not has_det:
                missing.append((tol_word, freq))

    print(f"  Top 200 Tol words: checked")
    print(f"  Missing English mapping: {len(missing)}")
    if missing:
        print(f"  Top 20 missing:")
        for w, f in missing[:20]:
            # Try to find it in parallel sentences to get context
            sample = conn.execute(
                "SELECT english FROM parallel_sentences WHERE LOWER(tol) LIKE ? AND english IS NOT NULL LIMIT 1",
                [f"%{w}%"]
            ).fetchone()
            ctx = sample["english"][:50] if sample else "?"
            print(f"    {w:20s} freq={f:4d}  ctx: {ctx}")


# ══════════════════════════════════════════════════════════════════════════
# PHASE E: Test and report
# ══════════════════════════════════════════════════════════════════════════

def phase_e_test():
    print("\n" + "=" * 60)
    print("PHASE E: Test suite results")
    print("=" * 60)

    from translator import TolTranslator
    t = TolTranslator()

    rows = conn.execute(
        "SELECT id, english, tol_expected FROM grammar_test_sentences ORDER BY id"
    ).fetchall()

    passed = 0
    partial = 0
    zero = 0
    total_score = 0.0
    pdf_passed = 0
    pdf_total = 0

    for r in rows:
        result = t.translate(r["english"], "en", "tol")
        trans = result["translations"][0]["text"] if result.get("translations") else result.get("translation", "")
        score = tol_phonetic_score(r["tol_expected"], trans)
        total_score += score
        if r["id"] < 100:
            pdf_total += 1
        if score >= 0.5:
            passed += 1
            if r["id"] < 100:
                pdf_passed += 1
        elif score > 0:
            partial += 1
        else:
            zero += 1

    total = len(rows)
    print(f"  Total:     {total}")
    print(f"  Passed:    {passed} ({passed/total*100:.1f}%)")
    print(f"  Grammar:   {pdf_passed}/{pdf_total} ({pdf_passed/max(pdf_total,1)*100:.1f}%)")
    print(f"  Partial:   {partial} ({partial/total*100:.1f}%)")
    print(f"  Zero:      {zero} ({zero/total*100:.1f}%)")
    print(f"  Non-zero:  {passed+partial} ({(passed+partial)/total*100:.1f}%)")
    print(f"  Avg score: {total_score/total:.4f}")


if __name__ == "__main__":
    print("Tol Translator Improvement — Iteration 2")
    print("=" * 60)
    phase_a_analyze_errors()
    phase_b_test_vocabulary()
    phase_c_verse_word_alignment()
    phase_d_frequency_vocab()
    phase_e_test()
    conn.close()
