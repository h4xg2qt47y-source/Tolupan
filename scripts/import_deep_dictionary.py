#!/usr/bin/env python3
"""
Import deep-parsed dictionary into the database.
1. Import headword entries (Spanish↔Tol)
2. Process example sentences for word-level mappings
3. Translate Spanish→English using the en_es_dictionary table
4. Build English↔Tol mappings
5. Normalize Tol to NT Bible spelling
"""

import json, re, sqlite3, sys, unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from difflib import SequenceMatcher

PROJ = Path(__file__).resolve().parent.parent
DB_PATH = PROJ / "app" / "data" / "tol.db"
PARSED = PROJ / "scripts" / "deep_parsed_dictionary.json"

# ── Load data ────────────────────────────────────────────────────────────────

def load_parsed():
    return json.load(open(PARSED))

def build_bible_vocab():
    db = sqlite3.connect(str(DB_PATH))
    rows = db.execute("""
        SELECT tol FROM parallel_sentences
        WHERE source LIKE 'bible_align:%' AND tol IS NOT NULL AND tol != ''
    """).fetchall()
    db.close()
    vocab = Counter()
    for r in rows:
        for w in r[0].lower().split():
            w = re.sub(r'[.,;:!?"""\u201c\u201d\(\)\[\]—]', '', w)
            if len(w) > 1:
                vocab[w] += 1
    return vocab

def build_es_en_dict():
    """Build Spanish→English lookup from en_es_dictionary."""
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    rows = db.execute("SELECT english, spanish FROM en_es_dictionary").fetchall()
    db.close()
    es_to_en = {}
    for r in rows:
        spa = r["spanish"].lower().strip()
        eng = r["english"].lower().strip()
        if spa and eng and len(spa) > 1 and len(eng) > 1:
            if spa not in es_to_en:
                es_to_en[spa] = eng
    return es_to_en


def normalize_tol_word(word, bible_vocab):
    """Normalize a Tol word to its Bible spelling."""
    w = word.lower().strip()
    w = re.sub(r'[.,;:!?]', '', w)
    if not w:
        return w

    if w in bible_vocab:
        return w

    subs = [
        ("ph", "pj"), ("kh", "cj"), ("th", "tj"), ("sh", "sj"),
        ("ch", "cj"),
    ]
    for old, new in subs:
        v = w.replace(old, new)
        if v in bible_vocab:
            return v

    best = None
    best_r = 0.0
    for bw in bible_vocab:
        if abs(len(bw) - len(w)) > 2:
            continue
        if bw[0] != w[0]:
            continue
        r = SequenceMatcher(None, w, bw).ratio()
        if r > best_r and r >= 0.88:
            best_r = r
            best = bw
    return best or w


# ── Main import ──────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Import Deep-Parsed Dictionary")
    print("=" * 70)

    data = load_parsed()
    entries = data["entries"]
    examples = data["examples"]
    word_pairs = data.get("word_pairs", [])

    print(f"Loaded: {len(entries)} entries, {len(examples)} examples, {len(word_pairs)} word pairs")

    bible_vocab = build_bible_vocab()
    print(f"Bible vocabulary: {len(bible_vocab)} words")

    es_en = build_es_en_dict()
    print(f"Spanish→English dictionary: {len(es_en)} entries")

    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    # Count existing
    existing_dict = db.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    existing_direct = db.execute("SELECT COUNT(*) FROM direct_en_tol").fetchone()[0]
    print(f"\nExisting DB: {existing_dict} dictionary, {existing_direct} direct_en_tol")

    # ── 1. Import headword entries ───────────────────────────────────────────

    print("\n─── Importing headword entries ───")
    added_dict = 0
    skipped_dict = 0
    updated_dict = 0

    for entry in entries:
        tol = entry["tol"].strip()
        spanish = entry["spanish"].strip()
        category = entry.get("category", "")

        if not tol or not spanish or len(tol) < 2 or len(spanish) < 2:
            skipped_dict += 1
            continue

        # Normalize Tol to Bible spelling (word by word for multi-word)
        tol_words = tol.split()
        tol_normalized = " ".join(normalize_tol_word(w, bible_vocab) for w in tol_words)

        # Check if already exists
        existing = db.execute(
            "SELECT id FROM dictionary WHERE tol=? AND spanish=?",
            [tol_normalized, spanish]
        ).fetchone()
        if existing:
            skipped_dict += 1
            continue

        # Also check the original spelling
        existing2 = db.execute(
            "SELECT id FROM dictionary WHERE tol=? AND spanish=?",
            [tol, spanish]
        ).fetchone()
        if existing2:
            # Update to Bible spelling
            db.execute("UPDATE dictionary SET tol=? WHERE id=?",
                      [tol_normalized, existing2["id"]])
            updated_dict += 1
            continue

        try:
            db.execute(
                "INSERT INTO dictionary (tol, spanish, category, source) VALUES (?, ?, ?, ?)",
                [tol_normalized, spanish, category, "SIL_Dictionary_Deep_OCR"]
            )
            added_dict += 1
        except sqlite3.IntegrityError:
            skipped_dict += 1

    db.commit()
    print(f"  Added: {added_dict}, Updated: {updated_dict}, Skipped: {skipped_dict}")

    # ── 2. Import word pairs from examples ───────────────────────────────────

    print("\n─── Importing word pairs from examples ───")
    added_wp = 0
    for wp in word_pairs:
        tol = normalize_tol_word(wp["tol"], bible_vocab)
        spanish = wp["spanish"].lower().strip()
        if not tol or not spanish or len(tol) < 2 or len(spanish) < 2:
            continue
        existing = db.execute(
            "SELECT id FROM dictionary WHERE tol=? AND spanish=?",
            [tol, spanish]
        ).fetchone()
        if existing:
            continue
        try:
            db.execute(
                "INSERT INTO dictionary (tol, spanish, category, source) VALUES (?, ?, ?, ?)",
                [tol, spanish, "", "SIL_Example_Extraction"]
            )
            added_wp += 1
        except sqlite3.IntegrityError:
            pass

    db.commit()
    print(f"  Added: {added_wp} word pairs")

    # ── 3. Translate Spanish→English for all dictionary entries ───────────────

    print("\n─── Translating Spanish→English ───")
    untranslated = db.execute(
        "SELECT id, spanish, tol FROM dictionary WHERE (english IS NULL OR english = '')"
    ).fetchall()
    print(f"  {len(untranslated)} entries need English translation")

    translated = 0
    for row in untranslated:
        spa = row["spanish"].lower().strip()
        eng = es_en.get(spa)

        if not eng:
            # Try without accents
            spa_no_acc = unicodedata.normalize("NFD", spa)
            spa_no_acc = "".join(c for c in spa_no_acc if unicodedata.category(c) != "Mn")
            eng = es_en.get(spa_no_acc)

        if not eng:
            # Try first word
            first = spa.split()[0] if spa.split() else ""
            eng = es_en.get(first)

        if eng:
            db.execute("UPDATE dictionary SET english=? WHERE id=?", [eng, row["id"]])
            translated += 1

    db.commit()
    print(f"  Translated: {translated}")

    # ── 4. Build English→Tol mappings from dictionary ────────────────────────

    print("\n─── Building English↔Tol mappings ───")
    dict_rows = db.execute(
        "SELECT tol, spanish, english FROM dictionary WHERE english IS NOT NULL AND english != ''"
    ).fetchall()
    print(f"  {len(dict_rows)} dictionary entries with English")

    added_en_tol = 0
    for row in dict_rows:
        eng = row["english"].lower().strip()
        tol = row["tol"].lower().strip()
        spa = row["spanish"].lower().strip()

        if not eng or not tol or len(eng) < 2 or len(tol) < 2:
            continue

        # Check Bible attestation
        tol_words = tol.split()
        bible_score = sum(1 for w in tol_words if w in bible_vocab)

        # Use higher confidence for Bible-attested words
        confidence = 0.85 if bible_score > 0 else 0.65

        existing = db.execute(
            "SELECT id, confidence FROM direct_en_tol WHERE english=? AND tol=?",
            [eng, tol]
        ).fetchone()
        if existing:
            # Boost confidence if from dictionary
            if existing["confidence"] < confidence:
                db.execute(
                    "UPDATE direct_en_tol SET confidence=?, source=? WHERE id=?",
                    [confidence, "sil_dictionary_verified", existing["id"]]
                )
            continue

        try:
            db.execute(
                "INSERT INTO direct_en_tol (english, tol, spanish, confidence, source) "
                "VALUES (?, ?, ?, ?, ?)",
                [eng, tol, spa, confidence, "sil_dictionary_deep_ocr"]
            )
            added_en_tol += 1
        except sqlite3.IntegrityError:
            pass

    db.commit()
    print(f"  Added: {added_en_tol} English→Tol entries")

    # ── 5. Process example sentences for phrase-level translations ────────────

    print("\n─── Processing example sentences ───")
    added_examples = 0
    for ex in examples:
        tol = ex["tol"].strip()
        spa = ex["spanish"].strip()
        if not tol or not spa or len(tol) < 5 or len(spa) < 5:
            continue

        # Normalize Tol words
        tol_words = tol.split()
        tol_norm = " ".join(normalize_tol_word(w.lower(), bible_vocab) for w in tol_words)

        existing = db.execute(
            "SELECT id FROM parallel_sentences WHERE tol=? AND spanish=?",
            [tol_norm, spa]
        ).fetchone()
        if existing:
            continue

        try:
            db.execute(
                "INSERT INTO parallel_sentences (tol, spanish, english, source) VALUES (?, ?, ?, ?)",
                [tol_norm, spa, "", "SIL_Dictionary_Example"]
            )
            added_examples += 1
        except sqlite3.IntegrityError:
            pass

    db.commit()
    print(f"  Added: {added_examples} example sentence pairs")

    # ── Final stats ──────────────────────────────────────────────────────────

    final_dict = db.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    final_direct = db.execute("SELECT COUNT(*) FROM direct_en_tol").fetchone()[0]
    final_parallel = db.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]

    print(f"\n{'='*70}")
    print("FINAL DATABASE STATS")
    print(f"{'='*70}")
    print(f"  dictionary:          {existing_dict:>6} → {final_dict:>6}  (+{final_dict-existing_dict})")
    print(f"  direct_en_tol:       {existing_direct:>6} → {final_direct:>6}  (+{final_direct-existing_direct})")
    print(f"  parallel_sentences:  {final_parallel:>6}")

    # Check coverage for user's example
    print(f"\n─── Coverage check ───")
    for word in ["bonita", "bonito", "mariposa", "beautiful", "pretty"]:
        d = db.execute(
            "SELECT tol, spanish, english FROM dictionary WHERE spanish LIKE ? OR english LIKE ? LIMIT 3",
            [f"%{word}%", f"%{word}%"]
        ).fetchall()
        if d:
            for r in d:
                print(f"  '{word}' → tol='{r['tol']}', spa='{r['spanish']}', en='{r['english']}'")
        else:
            e = db.execute(
                "SELECT english, tol FROM direct_en_tol WHERE english LIKE ? LIMIT 3",
                [f"%{word}%"]
            ).fetchall()
            if e:
                for r in e:
                    print(f"  '{word}' → en→tol: '{r['english']}' → '{r['tol']}'")
            else:
                print(f"  '{word}' → NOT FOUND")

    db.close()


if __name__ == "__main__":
    main()
