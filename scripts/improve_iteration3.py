"""
Tol Translator Improvement — Iteration 3
==========================================
1. Build Tol n-gram language model from NT corpus
2. Use n-gram model to rank/rerank translation candidates
3. Extract sentence-level patterns (common Tol constructions)
4. Improve verb conjugation handling
5. Add more cross-language alignments via pivoting
"""

import sqlite3
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)
DB_PATH = Path(__file__).parent.parent / "app" / "data" / "tol.db"
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row


# ══════════════════════════════════════════════════════════════════════════
# PHASE 1: Build Tol n-gram model and store common patterns
# ══════════════════════════════════════════════════════════════════════════

def phase1_ngram_model():
    print("=" * 60)
    print("PHASE 1: Building Tol n-gram language model")
    print("=" * 60)

    rows = conn.execute("SELECT tol FROM parallel_sentences WHERE tol != ''").fetchall()

    unigrams = Counter()
    bigrams = Counter()
    trigrams = Counter()

    for r in rows:
        words = r["tol"].lower().strip().split()
        for w in words:
            unigrams[w] += 1
        for i in range(len(words) - 1):
            bigrams[(words[i], words[i + 1])] += 1
        for i in range(len(words) - 2):
            trigrams[(words[i], words[i + 1], words[i + 2])] += 1

    print(f"  Unigrams: {len(unigrams)}")
    print(f"  Bigrams: {len(bigrams)}")
    print(f"  Trigrams: {len(trigrams)}")

    # Most common bigrams (excluding function words at start/end)
    tol_func = {"way", "na", "nin", "ca", "ma", "la", "le", "wa", "ne"}
    print(f"\n  Top 20 content bigrams:")
    for (a, b), c in bigrams.most_common(100):
        if a in tol_func or b in tol_func:
            continue
        print(f"    {a} {b}: {c}")
        if len([x for x in bigrams.most_common(100) if x[0][0] not in tol_func and x[0][1] not in tol_func]) >= 20:
            break

    return unigrams, bigrams, trigrams


# ══════════════════════════════════════════════════════════════════════════
# PHASE 2: Extract verb conjugation patterns from NT
# ══════════════════════════════════════════════════════════════════════════

def phase2_verb_patterns():
    print("\n" + "=" * 60)
    print("PHASE 2: Extracting verb conjugation patterns")
    print("=" * 60)

    # Look for Tol words with person prefixes: thi-, tha-, thü-, the-
    rows = conn.execute("SELECT tol, english FROM parallel_sentences WHERE tol != '' AND english != ''").fetchall()

    prefix_patterns = Counter()
    suffix_patterns = Counter()

    for r in rows:
        for w in r["tol"].lower().split():
            # Person prefixes
            for prefix in ["thi-", "tha-", "thü-", "the-", "tho-"]:
                if w.startswith(prefix):
                    stem = w[len(prefix):]
                    prefix_patterns[prefix + stem] += 1
            # Aspect suffixes
            for suffix in ["-ph", "-kh", "-n", "-s", "-a"]:
                if w.endswith(suffix) and len(w) > len(suffix) + 1:
                    suffix_patterns[w] += 1

    print(f"  Verb prefix patterns: {len(prefix_patterns)}")
    print(f"  Top 20 prefixed verbs:")
    for form, c in prefix_patterns.most_common(20):
        print(f"    {form}: {c}")

    print(f"\n  Verb suffix patterns: {len(suffix_patterns)}")
    print(f"  Top 20 suffixed verbs:")
    for form, c in suffix_patterns.most_common(20):
        print(f"    {form}: {c}")


# ══════════════════════════════════════════════════════════════════════════
# PHASE 3: Cross-language pivot alignment (en→spa→tol back-verification)
# ══════════════════════════════════════════════════════════════════════════

def phase3_pivot_verification():
    print("\n" + "=" * 60)
    print("PHASE 3: Cross-language pivot verification")
    print("=" * 60)

    # For each parallel sentence, verify: does our en→tol translation
    # produce words that also appear in the spa→tol alignment?
    # This double-checks our word alignments.

    # Get direct en→tol lookup
    en_tol = {}
    for r in conn.execute("SELECT english, tol, confidence FROM direct_en_tol WHERE confidence >= 0.8"):
        en = r["english"].lower()
        if en not in en_tol or r["confidence"] > en_tol[en][1]:
            en_tol[en] = (r["tol"], r["confidence"])

    # Get spa→tol lookup
    spa_tol = {}
    for r in conn.execute("SELECT spanish, tol FROM dictionary WHERE spanish != ''"):
        spa = r["spanish"].lower().strip()
        if spa not in spa_tol:
            spa_tol[spa] = r["tol"]

    # Cross-check: for each English word with a Tol translation,
    # see if the Tol word appears in sentences where the Spanish equivalent also appears
    en_spa = {}
    for r in conn.execute("SELECT english, spanish FROM en_es_dictionary LIMIT 50000"):
        en_spa[r["english"].lower()] = r["spanish"].lower()

    verified = 0
    for en_word, (tol_word, conf) in en_tol.items():
        spa_word = en_spa.get(en_word)
        if not spa_word:
            continue
        # Check if tol_word and spa_word co-occur in any parallel sentence
        check = conn.execute("""
            SELECT COUNT(*) FROM parallel_sentences
            WHERE LOWER(tol) LIKE ? AND LOWER(spanish) LIKE ?
        """, [f"%{tol_word}%", f"%{spa_word}%"]).fetchone()[0]
        if check > 0:
            verified += 1

    print(f"  En→Tol entries with high confidence: {len(en_tol)}")
    print(f"  Cross-verified via Spanish: {verified}")


# ══════════════════════════════════════════════════════════════════════════
# PHASE 4: Add Tol-specific grammatical constructions to direct_en_tol
# ══════════════════════════════════════════════════════════════════════════

def phase4_grammatical_patterns():
    print("\n" + "=" * 60)
    print("PHASE 4: Adding Tol grammatical construction patterns")
    print("=" * 60)

    # Tol uses postpositions: X nt'a = "to/at/in X", X lal = "with X", X mpes = "because of X"
    # Also: "la" as future/irrealis marker, "nin" as demonstrative, "jupj" as 3sg pronoun
    # These are structural patterns that help translation

    patterns = [
        # Common Tol constructions for translation
        ("because of", "mpes", "grammar_pattern", 0.90),
        ("that is why", "nin mpes", "grammar_pattern", 0.90),
        ("in order to", "la", "grammar_pattern", 0.85),
        ("used to", "=cha", "grammar_pattern", 0.85),
        ("long time", "pülükh", "grammar_pattern", 0.85),
        ("how many", "nol", "grammar_pattern", 0.90),
        ("how much", "nol", "grammar_pattern", 0.90),
    ]

    inserted = 0
    for en, tol, source, conf in patterns:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO direct_en_tol (english, tol, source, confidence)
                VALUES (?, ?, ?, ?)
            """, [en, tol, source, conf])
            inserted += 1
        except Exception:
            pass

    conn.commit()
    print(f"  Added {inserted} grammatical patterns")


# ══════════════════════════════════════════════════════════════════════════
# PHASE 5: Improve corpus index for faster fuzzy matching
# ══════════════════════════════════════════════════════════════════════════

def phase5_corpus_index():
    print("\n" + "=" * 60)
    print("PHASE 5: Building corpus search indices")
    print("=" * 60)

    # Create FTS5 index on parallel_sentences for faster search
    try:
        conn.execute("DROP TABLE IF EXISTS parallel_fts")
        conn.execute("""
            CREATE VIRTUAL TABLE parallel_fts USING fts5(
                english, spanish, tol,
                content='parallel_sentences',
                content_rowid='id'
            )
        """)
        conn.execute("""
            INSERT INTO parallel_fts(rowid, english, spanish, tol)
            SELECT id, COALESCE(english, ''), COALESCE(spanish, ''), COALESCE(tol, '')
            FROM parallel_sentences
        """)
        conn.commit()
        print("  Created FTS5 index on parallel_sentences")
    except Exception as e:
        print(f"  FTS5 index creation failed: {e}")

    # Also create standard indexes
    for idx, col in [("idx_par_tol", "tol"), ("idx_par_spa", "spanish"), ("idx_par_eng", "english")]:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON parallel_sentences({col})")
        except Exception:
            pass
    conn.commit()
    print("  Standard indexes verified")


# ══════════════════════════════════════════════════════════════════════════
# PHASE 6: Test and report
# ══════════════════════════════════════════════════════════════════════════

def phase6_test():
    print("\n" + "=" * 60)
    print("PHASE 6: Test results")
    print("=" * 60)

    from translator import TolTranslator, tol_phonetic_score
    t = TolTranslator()

    rows = conn.execute("SELECT id, english, tol_expected FROM grammar_test_sentences ORDER BY id").fetchall()

    passed = 0; partial = 0; zero = 0; total_score = 0.0
    pdf_passed = 0; pdf_total = 0; nt_passed = 0; nt_total = 0

    for r in rows:
        result = t.translate(r["english"], "en", "tol")
        trans = result["translations"][0]["text"] if result.get("translations") else result.get("translation", "")
        score = tol_phonetic_score(r["tol_expected"], trans)
        total_score += score
        is_pdf = r["id"] < 100
        if is_pdf: pdf_total += 1
        else: nt_total += 1
        if score >= 0.5:
            passed += 1
            if is_pdf: pdf_passed += 1
            else: nt_passed += 1
        elif score > 0: partial += 1
        else: zero += 1

    total = len(rows)
    print(f"  Total:     {total}")
    print(f"  Passed:    {passed} ({passed/total*100:.1f}%)")
    print(f"  Grammar:   {pdf_passed}/{pdf_total} ({pdf_passed/max(pdf_total,1)*100:.1f}%)")
    print(f"  NT verses: {nt_passed}/{nt_total} ({nt_passed/max(nt_total,1)*100:.1f}%)")
    print(f"  Non-zero:  {passed+partial+zero-zero}/{total} ({(passed+partial)/total*100:.1f}%)")
    print(f"  Avg score: {total_score/total:.4f}")


if __name__ == "__main__":
    print("Tol Translator Improvement — Iteration 3")
    print("=" * 60)
    phase1_ngram_model()
    phase2_verb_patterns()
    phase3_pivot_verification()
    phase4_grammatical_patterns()
    phase5_corpus_index()
    phase6_test()
    conn.close()
