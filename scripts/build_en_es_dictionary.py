#!/usr/bin/env python3
"""
Build a comprehensive local English↔Spanish dictionary from three open sources:
  1. MUSE (Meta/Facebook Research) – 112K+ word pairs in each direction
  2. nhershy/SpanishDictionaryCreator – 49K entries with POS and prevalence
  3. doozan/spanish_data (Wiktionary extract) – 111K entries with rich glosses

Creates/populates the `en_es_dictionary` table in the existing tol.db.
"""

import csv
import re
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "app" / "data" / "tol.db"

MUSE_EN_ES = Path("/tmp/muse_en_es_full.txt")
MUSE_ES_EN = Path("/tmp/muse_es_en_full.txt")
NHERSHY = Path("/tmp/nhershy_spa_dict.csv")
DOOZAN = Path("/tmp/doozan_es_en.data")


def create_table(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS en_es_dictionary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            english TEXT NOT NULL,
            spanish TEXT NOT NULL,
            pos TEXT,
            source TEXT,
            UNIQUE(english, spanish)
        );
        CREATE INDEX IF NOT EXISTS idx_enes_eng ON en_es_dictionary(english);
        CREATE INDEX IF NOT EXISTS idx_enes_spa ON en_es_dictionary(spanish);
    """)


# ── Source 1: MUSE ────────────────────────────────────────────────────────

def load_muse(conn: sqlite3.Connection):
    """Load MUSE bilingual word pairs (tab-separated: word1 \\t word2)."""
    inserted = 0

    # English → Spanish
    if MUSE_EN_ES.exists():
        for line in MUSE_EN_ES.read_text().splitlines():
            parts = line.strip().split("\t")
            if len(parts) != 2:
                parts = line.strip().split()
                if len(parts) != 2:
                    continue
            eng, spa = parts[0].strip().lower(), parts[1].strip().lower()
            if not eng or not spa or len(eng) < 2 or len(spa) < 2:
                continue
            # Skip if they're identical (proper nouns, cognates)
            if eng == spa:
                continue
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO en_es_dictionary (english, spanish, source) VALUES (?, ?, ?)",
                    (eng, spa, "muse")
                )
                inserted += 1
            except:
                pass

    # Spanish → English (reverse direction, same table)
    if MUSE_ES_EN.exists():
        for line in MUSE_ES_EN.read_text().splitlines():
            parts = line.strip().split("\t")
            if len(parts) != 2:
                parts = line.strip().split()
                if len(parts) != 2:
                    continue
            spa, eng = parts[0].strip().lower(), parts[1].strip().lower()
            if not eng or not spa or len(eng) < 2 or len(spa) < 2:
                continue
            if eng == spa:
                continue
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO en_es_dictionary (english, spanish, source) VALUES (?, ?, ?)",
                    (eng, spa, "muse")
                )
                inserted += 1
            except:
                pass

    return inserted


# ── Source 2: nhershy ─────────────────────────────────────────────────────

POS_MAP = {
    "NOUN": "n", "VERB": "v", "ADJ": "adj", "ADV": "adv",
    "ADP": "prep", "CONJ": "conj", "DET": "det", "PRON": "pron",
    "INTJ": "intj", "NUM": "num", "PROPN": "propn",
}

def load_nhershy(conn: sqlite3.Connection):
    """Load nhershy Spanish Dictionary Creator CSV."""
    if not NHERSHY.exists():
        return 0
    inserted = 0
    with open(NHERSHY, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            spa = row.get("Spanish Word", "").strip().lower()
            eng_raw = row.get("Translation", "").strip()
            pos = POS_MAP.get(row.get("Part-of-Speech", "").strip(), "")

            if not spa or not eng_raw:
                continue

            # Translation field can have multiple meanings separated by comma
            for eng_part in eng_raw.split(","):
                eng = eng_part.strip().lower()
                # Clean up "To verb" patterns
                if eng.startswith("to "):
                    eng_clean = eng[3:]
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO en_es_dictionary (english, spanish, pos, source) VALUES (?, ?, ?, ?)",
                            (eng_clean, spa, pos or "v", "nhershy")
                        )
                    except:
                        pass
                if len(eng) < 2 or len(spa) < 2:
                    continue
                if eng == spa:
                    continue
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO en_es_dictionary (english, spanish, pos, source) VALUES (?, ?, ?, ?)",
                        (eng, spa, pos, "nhershy")
                    )
                    inserted += 1
                except:
                    pass
    return inserted


# ── Source 3: doozan (Wiktionary) ─────────────────────────────────────────

def extract_english_from_gloss(gloss: str) -> list[str]:
    """
    Extract English translation words/phrases from a Wiktionary gloss string.
    Glosses look like: 'house; home; dwelling' or 'to run; to flee'
    Also handles: 'the act of running' or 'obsolete form of "X"'
    """
    # Remove template references and qualifiers
    gloss = re.sub(r'\([^)]*\)', '', gloss)
    gloss = re.sub(r'\{[^}]*\}', '', gloss)
    gloss = re.sub(r'\[[^\]]*\]', '', gloss)
    
    # Handle "form of X" patterns
    form_match = re.search(r'(?:form|spelling|variant) of "([^"]+)"', gloss)
    if form_match:
        return [form_match.group(1).strip().lower()]

    # Split on semicolons and commas
    results = []
    for part in re.split(r'[;]', gloss):
        part = part.strip()
        if not part or len(part) > 60:
            continue
        # Remove leading "to " for verbs (keep it for the entry but also add without)
        clean = re.sub(r'^(the |an? )', '', part.lower()).strip()
        if clean and len(clean) >= 2:
            results.append(clean)
        if part.lower().startswith("to "):
            verb = part[3:].strip().lower()
            if verb and verb not in results:
                results.append(verb)
    return results


def load_doozan(conn: sqlite3.Connection):
    """Parse doozan Wiktionary-extracted Spanish→English dictionary."""
    if not DOOZAN.exists():
        return 0

    text = DOOZAN.read_text(encoding="utf-8")
    entries = text.split("_____\n")
    inserted = 0

    for entry in entries:
        lines = entry.strip().split("\n")
        if not lines or not lines[0].strip():
            continue

        headword = lines[0].strip().lower()
        if len(headword) < 2 or headword.startswith("-") or headword.startswith("*"):
            continue

        current_pos = ""
        for line in lines[1:]:
            line = line.strip()
            if line.startswith("pos: "):
                pos_raw = line[5:].strip()
                pos_map_doozan = {
                    "n": "n", "v": "v", "adj": "adj", "adv": "adv",
                    "prop": "propn", "conj": "conj", "prep": "prep",
                    "pron": "pron", "suffix": "suffix", "prefix": "prefix",
                    "intj": "intj", "det": "det", "num": "num",
                    "phrase": "phrase", "name": "propn",
                }
                current_pos = pos_map_doozan.get(pos_raw, pos_raw)

            elif line.startswith("gloss: "):
                gloss = line[7:].strip()
                english_words = extract_english_from_gloss(gloss)
                for eng in english_words:
                    if eng == headword:
                        continue
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO en_es_dictionary (english, spanish, pos, source) VALUES (?, ?, ?, ?)",
                            (eng, headword, current_pos, "wiktionary")
                        )
                        inserted += 1
                    except:
                        pass

    return inserted


# ── Also back-fill English into existing Tol dictionary ───────────────────

def backfill_tol_english(conn: sqlite3.Connection):
    """
    For Tol dictionary entries that have Spanish but no English,
    use the new en_es_dictionary to fill in English translations.
    """
    rows = conn.execute(
        "SELECT id, spanish FROM dictionary WHERE (english IS NULL OR english = '') AND spanish != ''"
    ).fetchall()

    updated = 0
    for row_id, spa in rows:
        spa_lower = spa.lower().strip()
        eng_row = conn.execute(
            "SELECT english FROM en_es_dictionary WHERE spanish = ? LIMIT 1",
            (spa_lower,)
        ).fetchone()
        if eng_row:
            conn.execute(
                "UPDATE dictionary SET english = ? WHERE id = ?",
                (eng_row[0], row_id)
            )
            updated += 1
    return updated


def main():
    conn = sqlite3.connect(str(DB_PATH))

    print("=" * 60)
    print("  BUILDING LOCAL ENGLISH ↔ SPANISH DICTIONARY")
    print("=" * 60)

    # Create table
    create_table(conn)
    conn.commit()

    before = conn.execute("SELECT COUNT(*) FROM en_es_dictionary").fetchone()[0]
    print(f"\n  Existing en_es entries: {before}")

    # Source 1: MUSE
    print("\n  [1/3] Loading MUSE (Meta Research)...")
    n = load_muse(conn)
    conn.commit()
    after_muse = conn.execute("SELECT COUNT(*) FROM en_es_dictionary").fetchone()[0]
    print(f"    Processed: {n:,} lines → {after_muse - before:,} new entries")

    # Source 2: nhershy
    print("\n  [2/3] Loading nhershy Spanish Dictionary...")
    n = load_nhershy(conn)
    conn.commit()
    after_nhershy = conn.execute("SELECT COUNT(*) FROM en_es_dictionary").fetchone()[0]
    print(f"    Processed: {n:,} lines → {after_nhershy - after_muse:,} new entries")

    # Source 3: doozan (Wiktionary)
    print("\n  [3/3] Loading doozan Wiktionary extract...")
    n = load_doozan(conn)
    conn.commit()
    after_doozan = conn.execute("SELECT COUNT(*) FROM en_es_dictionary").fetchone()[0]
    print(f"    Processed: {n:,} lines → {after_doozan - after_nhershy:,} new entries")

    total = conn.execute("SELECT COUNT(*) FROM en_es_dictionary").fetchone()[0]

    # Statistics
    print(f"\n{'='*60}")
    print(f"  ENGLISH ↔ SPANISH DICTIONARY COMPLETE")
    print(f"{'='*60}")
    print(f"  Total entries: {total:,}")

    by_source = conn.execute(
        "SELECT source, COUNT(*) FROM en_es_dictionary GROUP BY source ORDER BY COUNT(*) DESC"
    ).fetchall()
    for src, cnt in by_source:
        print(f"    {src:20s}: {cnt:,}")

    unique_eng = conn.execute("SELECT COUNT(DISTINCT english) FROM en_es_dictionary").fetchone()[0]
    unique_spa = conn.execute("SELECT COUNT(DISTINCT spanish) FROM en_es_dictionary").fetchone()[0]
    print(f"\n  Unique English words: {unique_eng:,}")
    print(f"  Unique Spanish words: {unique_spa:,}")

    with_pos = conn.execute("SELECT COUNT(*) FROM en_es_dictionary WHERE pos IS NOT NULL AND pos != ''").fetchone()[0]
    print(f"  Entries with POS tag: {with_pos:,}")

    # Sample entries
    print(f"\n  Sample entries:")
    for row in conn.execute("SELECT english, spanish, pos, source FROM en_es_dictionary ORDER BY RANDOM() LIMIT 15"):
        pos = f" ({row[2]})" if row[2] else ""
        print(f"    {row[0]:25s} ↔ {row[1]:25s}{pos:10s}  [{row[3]}]")

    # Back-fill English into Tol dictionary
    print(f"\n{'='*60}")
    print(f"  BACK-FILLING ENGLISH INTO TOL DICTIONARY")
    print(f"{'='*60}")
    tol_before = conn.execute(
        "SELECT COUNT(*) FROM dictionary WHERE english IS NOT NULL AND english != ''"
    ).fetchone()[0]
    updated = backfill_tol_english(conn)
    conn.commit()
    tol_after = conn.execute(
        "SELECT COUNT(*) FROM dictionary WHERE english IS NOT NULL AND english != ''"
    ).fetchone()[0]
    print(f"  Tol entries with English: {tol_before} → {tol_after} (+{tol_after - tol_before})")

    # Show some backfilled entries
    print(f"\n  Sample newly backfilled Tol entries:")
    for row in conn.execute(
        "SELECT tol, spanish, english FROM dictionary WHERE source = 'full_nt_align' AND english != '' ORDER BY RANDOM() LIMIT 10"
    ):
        print(f"    {row[0]:22s} → spa:{row[1]:18s} → eng:{row[2]}")

    conn.close()
    print(f"\n{'='*60}")
    print(f"  DONE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
