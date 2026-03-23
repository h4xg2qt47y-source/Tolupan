#!/usr/bin/env python3
"""
Comprehensive database cleanup, re-translation, deduplication, ranking,
ratings table creation, and bad-data purge.
"""
import sqlite3
import re
import sys
import os
import json
from collections import Counter, defaultdict
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "app" / "data" / "tol.db"
RATINGS_DB_PATH = Path(__file__).resolve().parent.parent / "app" / "data" / "ratings.db"

FIRST_CLASS_SOURCES = {
    "SIL_Dictionary_OCR", "SIL_Dictionary_Deep_OCR", "SIL_Example_Extraction",
    "Elicited_Grammar_Aurelio", "curated_bible_cooccurrence",
    "sil_dictionary_verified", "bible_align:MAT01",
}

INFERRED_SOURCES = {
    "full_nt_align", "nt_align_v2_p1", "nt_align_v2_p2",
}

SOURCE_RANK = {
    "sil_dictionary_verified": 10,
    "Elicited_Grammar_Aurelio": 9,
    "SIL_Dictionary_OCR": 8,
    "SIL_Dictionary_Deep_OCR": 7,
    "curated_bible_cooccurrence": 7,
    "SIL_Example_Extraction": 6,
    "bible_align:MAT01": 5,
    "grammar_pdf_verified": 5,
    "grammar_pdf_deep": 4,
    "bible_alignment_verified": 4,
    "full_nt_align": 2,
    "nt_align_v2_p1": 2,
    "nt_align_v2_p2": 2,
}


def build_es_to_en_lookup(conn):
    """Build a Spanish→English dictionary from the en_es_dictionary table."""
    print("  Building Spanish→English lookup from en_es_dictionary table...")
    es_to_en = {}
    rows = conn.execute("SELECT english, spanish FROM en_es_dictionary").fetchall()
    for en, es in rows:
        es_lower = es.lower().strip()
        if es_lower not in es_to_en:
            es_to_en[es_lower] = en.strip()
    print(f"  Built lookup with {len(es_to_en):,} Spanish→English mappings")
    return es_to_en


def translate_with_argos(texts):
    """Batch translate Spanish→English using argostranslate."""
    try:
        import argostranslate.package
        import argostranslate.translate
        argostranslate.package.update_package_index()
        available = argostranslate.package.get_available_packages()
        pkg = next((p for p in available if p.from_code == "es" and p.to_code == "en"), None)
        if pkg:
            argostranslate.package.install_from_path(pkg.download())
        results = {}
        for text in texts:
            try:
                results[text] = argostranslate.translate.translate(text, "es", "en")
            except Exception:
                results[text] = None
        return results
    except Exception as e:
        print(f"  Warning: argostranslate failed: {e}")
        return {}


def step1_retranslate_spanish_to_english(conn):
    """Re-translate dictionary entries that originated in Spanish but have empty/bad English."""
    print("\n=== STEP 1: Re-translate Spanish→English for dictionary entries ===")

    es_to_en = build_es_to_en_lookup(conn)

    rows = conn.execute("""
        SELECT id, tol, spanish, english, source FROM dictionary
        WHERE spanish IS NOT NULL AND spanish != ''
    """).fetchall()

    updated = 0
    needs_argos = []

    for row_id, tol, spanish, english, source in rows:
        spa_lower = spanish.lower().strip()

        if english and english.strip() and not _is_garbage_english(english):
            continue

        en_translation = es_to_en.get(spa_lower)
        if not en_translation:
            words = re.sub(r'[.,;:!?¿¡()"]', '', spa_lower).split()
            translated_words = []
            for w in words:
                if w in es_to_en:
                    translated_words.append(es_to_en[w])
                else:
                    translated_words.append(w)
            if any(w != orig for w, orig in zip(translated_words, words)):
                en_translation = " ".join(translated_words)

        if en_translation:
            conn.execute("UPDATE dictionary SET english = ? WHERE id = ?", (en_translation, row_id))
            updated += 1
        else:
            needs_argos.append((row_id, spanish))

    if needs_argos:
        print(f"  {len(needs_argos)} entries need argostranslate...")
        argos_results = translate_with_argos([s for _, s in needs_argos])
        for row_id, spanish in needs_argos:
            en = argos_results.get(spanish)
            if en and en.strip():
                conn.execute("UPDATE dictionary SET english = ? WHERE id = ?", (en.strip(), row_id))
                updated += 1

    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(*) FROM dictionary WHERE english IS NULL OR english = ''"
    ).fetchone()[0]
    print(f"  Updated {updated} entries. Remaining without English: {remaining}")


def _is_garbage_english(text):
    """Detect garbage English text (OCR artifacts, non-words)."""
    if not text or len(text.strip()) < 2:
        return True
    clean = re.sub(r'[^a-zA-Z\s]', '', text).strip()
    if len(clean) < 2:
        return True
    if re.match(r'^[^a-zA-Z]*$', text):
        return True
    words = clean.split()
    if not words:
        return True
    if all(len(w) == 1 for w in words) and len(words) <= 2:
        return True
    return False


def step2_remove_duplicates(conn):
    """Remove exact duplicate dictionary rows, keeping the one with the best source."""
    print("\n=== STEP 2: Remove duplicate dictionary entries ===")

    rows = conn.execute("SELECT id, tol, spanish, english, source FROM dictionary").fetchall()
    seen = {}
    to_delete = []

    for row_id, tol, spanish, english, source in rows:
        key = (tol.lower().strip(), (spanish or "").lower().strip())
        rank = SOURCE_RANK.get(source, 1)

        if key in seen:
            existing_id, existing_rank = seen[key]
            if rank > existing_rank:
                to_delete.append(existing_id)
                seen[key] = (row_id, rank)
            else:
                to_delete.append(row_id)
        else:
            seen[key] = (row_id, rank)

    if to_delete:
        for batch_start in range(0, len(to_delete), 500):
            batch = to_delete[batch_start:batch_start + 500]
            placeholders = ",".join("?" * len(batch))
            conn.execute(f"DELETE FROM dictionary WHERE id IN ({placeholders})", batch)
        conn.commit()

    print(f"  Removed {len(to_delete)} duplicate entries")
    remaining = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    print(f"  Dictionary now has {remaining} entries")


def step3_purge_bad_data(conn):
    """Purge clearly wrong entries from dictionary, inferred_translations, and direct_en_tol."""
    print("\n=== STEP 3: Purge bad/garbage data ===")

    deleted_dict = 0
    deleted_direct = 0
    deleted_inferred = 0
    deleted_phrases = 0

    rows = conn.execute("SELECT id, tol, spanish, english, source FROM dictionary").fetchall()
    bad_dict_ids = []
    for row_id, tol, spanish, english, source in rows:
        if _is_garbage_tol(tol):
            bad_dict_ids.append(row_id)
            continue
        if spanish and _is_garbage_spanish(spanish) and source in INFERRED_SOURCES:
            bad_dict_ids.append(row_id)
            continue
        if english and _is_garbage_english(english) and source in INFERRED_SOURCES:
            bad_dict_ids.append(row_id)

    for batch_start in range(0, len(bad_dict_ids), 500):
        batch = bad_dict_ids[batch_start:batch_start + 500]
        placeholders = ",".join("?" * len(batch))
        conn.execute(f"DELETE FROM dictionary WHERE id IN ({placeholders})", batch)
    deleted_dict = len(bad_dict_ids)

    low_conf = conn.execute("""
        DELETE FROM direct_en_tol WHERE source IN (
            'nt_phrase_alignment', 'nt_statistical_alignment', 'nt_spa_chain_alignment'
        ) AND confidence < 0.6
    """)
    deleted_direct = low_conf.rowcount

    bad_inferred = conn.execute("""
        DELETE FROM inferred_translations WHERE confidence < 0.5
    """)
    deleted_inferred = bad_inferred.rowcount

    bad_phrases = conn.execute("""
        DELETE FROM phrase_translations WHERE score < 0.4
    """)
    deleted_phrases = bad_phrases.rowcount

    conn.commit()
    print(f"  Deleted {deleted_dict} bad dictionary entries")
    print(f"  Deleted {deleted_direct} low-confidence direct_en_tol entries")
    print(f"  Deleted {deleted_inferred} low-confidence inferred entries")
    print(f"  Deleted {deleted_phrases} low-score phrase entries")


def _is_garbage_tol(text):
    if not text or len(text.strip()) < 2:
        return True
    if len(text) > 100:
        return True
    if re.match(r'^[\d\s.,;:!?]+$', text):
        return True
    return False


def _is_garbage_spanish(text):
    if not text or len(text.strip()) < 1:
        return True
    if re.match(r'^[\d\s.,;:!?]+$', text):
        return True
    return False


def step4_create_ratings_tables():
    """Create ratings tables in a separate ratings.db (survives code pushes)."""
    print("\n=== STEP 4: Create ratings tables in ratings.db ===")

    conn = sqlite3.connect(str(RATINGS_DB_PATH))

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS dictionary_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tol TEXT NOT NULL,
            from_lang TEXT NOT NULL,
            to_lang TEXT NOT NULL,
            from_text TEXT NOT NULL,
            to_text TEXT NOT NULL,
            rating INTEGER NOT NULL CHECK(rating IN (-1, 1)),
            rated_at TEXT NOT NULL DEFAULT (datetime('now')),
            ip_address TEXT,
            country TEXT
        );

        CREATE TABLE IF NOT EXISTS phrase_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tol TEXT,
            from_lang TEXT NOT NULL,
            to_lang TEXT NOT NULL,
            from_text TEXT NOT NULL,
            to_text TEXT NOT NULL,
            rating INTEGER NOT NULL CHECK(rating IN (-1, 1)),
            rated_at TEXT NOT NULL DEFAULT (datetime('now')),
            ip_address TEXT,
            country TEXT
        );

        CREATE TABLE IF NOT EXISTS translator_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_lang TEXT NOT NULL,
            to_lang TEXT NOT NULL,
            from_text TEXT NOT NULL,
            to_text TEXT NOT NULL,
            method TEXT,
            rating INTEGER NOT NULL CHECK(rating IN (-1, 1)),
            rated_at TEXT NOT NULL DEFAULT (datetime('now')),
            ip_address TEXT,
            country TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_dict_ratings_tol ON dictionary_ratings(tol);
        CREATE INDEX IF NOT EXISTS idx_phrase_ratings_from ON phrase_ratings(from_text);
        CREATE INDEX IF NOT EXISTS idx_translator_ratings_from ON translator_ratings(from_text);
    """)
    conn.commit()
    conn.close()
    print(f"  Created ratings tables in {RATINGS_DB_PATH}")


def step5_print_summary(conn):
    """Print final database summary."""
    print("\n=== FINAL SUMMARY ===")
    tables = ["dictionary", "direct_en_tol", "parallel_sentences",
              "inferred_translations", "phrase_translations", "verb_conjugations"]
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    print("\n  Dictionary by source (ranked):")
    for source, count in conn.execute(
        "SELECT source, COUNT(*) as c FROM dictionary GROUP BY source ORDER BY c DESC"
    ).fetchall():
        rank = SOURCE_RANK.get(source, 1)
        tier = "TIER-1 (first-class)" if rank >= 5 else "TIER-2 (inferred)"
        print(f"    {source:40s} {count:>5} rows  rank={rank}  {tier}")

    rconn = sqlite3.connect(str(RATINGS_DB_PATH))
    for table in ["dictionary_ratings", "phrase_ratings", "translator_ratings"]:
        try:
            count = rconn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} rows")
        except Exception:
            print(f"  {table}: table exists")
    rconn.close()


def main():
    print(f"Database: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = lambda c, r: r

    step1_retranslate_spanish_to_english(conn)
    step2_remove_duplicates(conn)
    step3_purge_bad_data(conn)
    step4_create_ratings_tables()
    step5_print_summary(conn)
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
