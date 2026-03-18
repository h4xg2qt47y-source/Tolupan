#!/usr/bin/env python3
"""
Build synonym database and use it to massively expand Tol translation coverage.

Sources:
  - Moby Thesaurus II (30K+ English root words, 2.5M+ synonym links)
  - edublancas/sinonimos (8K Spanish synonym groups, 22K unique words)

Strategy for expanding Tol coverage:
  1. Build synonym lookup tables
  2. For every English word that translates to Tol (via dictionary),
     find all English synonyms → each synonym becomes a new path to that Tol word
  3. Same for Spanish synonyms → Tol
  4. Cross-language expansion: English synonym → English→Spanish dict → Spanish→Tol
  5. Store inferred translations with confidence scores based on synonym distance
"""

import csv
import json
import re
import sqlite3
import sys
import time
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "app" / "data" / "tol.db"
MOBY_PATH = Path("/tmp/mthesaur.txt")
SINONIMOS_PATH = Path("/tmp/sinonimos_es.json")


def create_tables(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS synonyms_en (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL,
            synonym TEXT NOT NULL,
            UNIQUE(word, synonym)
        );
        CREATE INDEX IF NOT EXISTS idx_synen_word ON synonyms_en(word);
        CREATE INDEX IF NOT EXISTS idx_synen_syn ON synonyms_en(synonym);

        CREATE TABLE IF NOT EXISTS synonyms_es (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL,
            synonym TEXT NOT NULL,
            UNIQUE(word, synonym)
        );
        CREATE INDEX IF NOT EXISTS idx_synes_word ON synonyms_es(word);
        CREATE INDEX IF NOT EXISTS idx_synes_syn ON synonyms_es(synonym);

        CREATE TABLE IF NOT EXISTS inferred_translations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_word TEXT NOT NULL,
            source_lang TEXT NOT NULL,
            tol_word TEXT NOT NULL,
            confidence REAL NOT NULL,
            path TEXT NOT NULL,
            UNIQUE(source_word, source_lang, tol_word)
        );
        CREATE INDEX IF NOT EXISTS idx_inferred_src ON inferred_translations(source_word, source_lang);
        CREATE INDEX IF NOT EXISTS idx_inferred_tol ON inferred_translations(tol_word);
    """)


# ── Load Moby Thesaurus ──────────────────────────────────────────────────

def load_moby(conn: sqlite3.Connection):
    """Load Moby Thesaurus II. Format: root_word,syn1,syn2,..."""
    if not MOBY_PATH.exists():
        print("  ERROR: Moby Thesaurus not found at", MOBY_PATH)
        return 0

    inserted = 0
    batch = []
    for line in MOBY_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = [w.strip().lower() for w in line.split(",") if w.strip()]
        if len(parts) < 2:
            continue
        root = parts[0]
        for syn in parts[1:]:
            if syn == root or len(syn) < 2 or len(root) < 2:
                continue
            batch.append((root, syn))
            if len(batch) >= 10000:
                conn.executemany("INSERT OR IGNORE INTO synonyms_en (word, synonym) VALUES (?, ?)", batch)
                inserted += len(batch)
                batch = []

    if batch:
        conn.executemany("INSERT OR IGNORE INTO synonyms_en (word, synonym) VALUES (?, ?)", batch)
        inserted += len(batch)

    conn.commit()
    return inserted


# ── Load Spanish Synonyms ────────────────────────────────────────────────

def load_spanish_synonyms(conn: sqlite3.Connection):
    """Load edublancas/sinonimos. Format: list of lists of synonym groups."""
    if not SINONIMOS_PATH.exists():
        print("  ERROR: Spanish synonyms not found at", SINONIMOS_PATH)
        return 0

    data = json.load(open(SINONIMOS_PATH, encoding="utf-8"))
    inserted = 0
    batch = []

    for group in data:
        words = []
        for item in group:
            for w in item.split(","):
                w = w.strip().lower()
                if w and len(w) >= 2:
                    words.append(w)

        # All words in a group are synonyms of each other
        for i, w1 in enumerate(words):
            for w2 in words[i + 1:]:
                if w1 == w2:
                    continue
                batch.append((w1, w2))
                batch.append((w2, w1))  # bidirectional

                if len(batch) >= 10000:
                    conn.executemany("INSERT OR IGNORE INTO synonyms_es (word, synonym) VALUES (?, ?)", batch)
                    inserted += len(batch)
                    batch = []

    if batch:
        conn.executemany("INSERT OR IGNORE INTO synonyms_es (word, synonym) VALUES (?, ?)", batch)
        inserted += len(batch)

    conn.commit()
    return inserted


# ── Expansion Engine ──────────────────────────────────────────────────────

def expand_tol_coverage(conn: sqlite3.Connection):
    """
    Use synonym chains to create new translation paths to Tol.

    Strategy (in order of confidence):
      1. DIRECT SYNONYM EXPANSION (confidence 0.80):
         English word A → Tol (known)
         English word B is synonym of A
         → B → Tol (inferred, conf=0.80)

      2. SPANISH SYNONYM EXPANSION (confidence 0.80):
         Spanish word X → Tol (known)
         Spanish word Y is synonym of X
         → Y → Tol (inferred, conf=0.80)

      3. CROSS-LANGUAGE SYNONYM BRIDGE (confidence 0.65):
         English word A → Spanish word X (en_es_dict)
         Spanish word X → Tol (known)
         English word B is synonym of A
         → B → A → X → Tol (inferred, conf=0.65)

      4. REVERSE CROSS-LANGUAGE (confidence 0.65):
         Spanish word X → English word A (en_es_dict)
         English word A → Tol (known)
         Spanish word Y is synonym of X
         → Y → X → A → Tol (inferred, conf=0.65)
    """

    # Load existing Tol dictionary mappings
    eng_to_tol = {}
    spa_to_tol = {}
    tol_to_eng = defaultdict(set)
    tol_to_spa = defaultdict(set)

    for row in conn.execute("SELECT tol, spanish, english FROM dictionary"):
        tol = row[0].lower().strip()
        spa = row[1].lower().strip()
        eng = (row[2] or "").lower().strip()
        if spa:
            spa_to_tol[spa] = tol
            tol_to_spa[tol].add(spa)
        if eng:
            for ew in eng.split(","):
                ew = ew.strip()
                if ew:
                    eng_to_tol[ew] = tol
                    tol_to_eng[tol].add(ew)

    # Load English↔Spanish local dict
    eng_to_spa_dict = defaultdict(set)
    spa_to_eng_dict = defaultdict(set)
    has_en_es = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='en_es_dictionary'").fetchone()
    if has_en_es:
        for row in conn.execute("SELECT english, spanish FROM en_es_dictionary"):
            eng_to_spa_dict[row[0].lower()].add(row[1].lower())
            spa_to_eng_dict[row[1].lower()].add(row[0].lower())

    # Load synonym lookups
    en_synonyms = defaultdict(set)
    for row in conn.execute("SELECT word, synonym FROM synonyms_en"):
        en_synonyms[row[0]].add(row[1])
        en_synonyms[row[1]].add(row[0])  # bidirectional

    es_synonyms = defaultdict(set)
    for row in conn.execute("SELECT word, synonym FROM synonyms_es"):
        es_synonyms[row[0]].add(row[1])

    print(f"  Loaded: {len(eng_to_tol)} eng→tol, {len(spa_to_tol)} spa→tol")
    print(f"  Loaded: {len(en_synonyms)} English words with synonyms")
    print(f"  Loaded: {len(es_synonyms)} Spanish words with synonyms")
    print(f"  Loaded: {len(eng_to_spa_dict)} eng→spa, {len(spa_to_eng_dict)} spa→eng")

    inferred = []
    seen = set()

    # ── Strategy 1: English synonym → Tol (direct) ──
    print("\n  [Strategy 1] English synonym → Tol (direct)...")
    count1 = 0
    for eng_word, tol_word in eng_to_tol.items():
        if eng_word not in en_synonyms:
            continue
        for syn in en_synonyms[eng_word]:
            if syn in eng_to_tol:
                continue  # already known
            key = (syn, "en", tol_word)
            if key in seen:
                continue
            seen.add(key)
            inferred.append({
                "source_word": syn, "source_lang": "en", "tol_word": tol_word,
                "confidence": 0.80,
                "path": f"en:{syn}→syn→en:{eng_word}→tol:{tol_word}",
            })
            count1 += 1
    print(f"    → {count1:,} new English→Tol paths")

    # ── Strategy 2: Spanish synonym → Tol (direct) ──
    print("  [Strategy 2] Spanish synonym → Tol (direct)...")
    count2 = 0
    for spa_word, tol_word in spa_to_tol.items():
        if spa_word not in es_synonyms:
            continue
        for syn in es_synonyms[spa_word]:
            if syn in spa_to_tol:
                continue
            key = (syn, "es", tol_word)
            if key in seen:
                continue
            seen.add(key)
            inferred.append({
                "source_word": syn, "source_lang": "es", "tol_word": tol_word,
                "confidence": 0.80,
                "path": f"es:{syn}→syn→es:{spa_word}→tol:{tol_word}",
            })
            count2 += 1
    print(f"    → {count2:,} new Spanish→Tol paths")

    # ── Strategy 3: English synonym → En→Es dict → Tol ──
    print("  [Strategy 3] English synonym → bridge via En→Es → Tol...")
    count3 = 0
    for eng_word, spa_set in eng_to_spa_dict.items():
        for spa_word in spa_set:
            if spa_word not in spa_to_tol:
                continue
            tol_word = spa_to_tol[spa_word]
            # Now expand via English synonyms
            if eng_word not in en_synonyms:
                continue
            for syn in en_synonyms[eng_word]:
                if syn in eng_to_tol:
                    continue
                key = (syn, "en", tol_word)
                if key in seen:
                    continue
                seen.add(key)
                inferred.append({
                    "source_word": syn, "source_lang": "en", "tol_word": tol_word,
                    "confidence": 0.65,
                    "path": f"en:{syn}→syn→en:{eng_word}→es:{spa_word}→tol:{tol_word}",
                })
                count3 += 1
                if count3 >= 500000:
                    break
            if count3 >= 500000:
                break
        if count3 >= 500000:
            break
    print(f"    → {count3:,} new English→(bridge)→Tol paths")

    # ── Strategy 4: Spanish synonym → Es→En → Tol ──
    print("  [Strategy 4] Spanish synonym → bridge via Es→En → Tol...")
    count4 = 0
    for spa_word, eng_set in spa_to_eng_dict.items():
        for eng_word in eng_set:
            if eng_word not in eng_to_tol:
                continue
            tol_word = eng_to_tol[eng_word]
            if spa_word not in es_synonyms:
                continue
            for syn in es_synonyms[spa_word]:
                if syn in spa_to_tol:
                    continue
                key = (syn, "es", tol_word)
                if key in seen:
                    continue
                seen.add(key)
                inferred.append({
                    "source_word": syn, "source_lang": "es", "tol_word": tol_word,
                    "confidence": 0.65,
                    "path": f"es:{syn}→syn→es:{spa_word}→en:{eng_word}→tol:{tol_word}",
                })
                count4 += 1
    print(f"    → {count4:,} new Spanish→(bridge)→Tol paths")

    # ── Insert inferred translations ──
    print(f"\n  Total inferred translations: {len(inferred):,}")
    print("  Inserting into database...")

    batch = [(i["source_word"], i["source_lang"], i["tol_word"], i["confidence"], i["path"])
             for i in inferred]
    conn.executemany(
        "INSERT OR IGNORE INTO inferred_translations (source_word, source_lang, tol_word, confidence, path) VALUES (?, ?, ?, ?, ?)",
        batch
    )
    conn.commit()

    final = conn.execute("SELECT COUNT(*) FROM inferred_translations").fetchone()[0]
    return {
        "strategy1_en_syn": count1,
        "strategy2_es_syn": count2,
        "strategy3_en_bridge": count3,
        "strategy4_es_bridge": count4,
        "total_inferred": final,
    }


def main():
    t0 = time.time()
    conn = sqlite3.connect(str(DB_PATH))

    print("=" * 65)
    print("  SYNONYM DATABASE & TRANSLATION EXPANSION ENGINE")
    print("=" * 65)

    create_tables(conn)
    conn.commit()

    # Load English synonyms (Moby)
    print("\n[1/4] Loading Moby Thesaurus (English synonyms)...")
    n = load_moby(conn)
    en_total = conn.execute("SELECT COUNT(*) FROM synonyms_en").fetchone()[0]
    en_words = conn.execute("SELECT COUNT(DISTINCT word) FROM synonyms_en").fetchone()[0]
    print(f"  Processed {n:,} lines")
    print(f"  Total synonym pairs: {en_total:,}")
    print(f"  Unique root words: {en_words:,}")

    # Load Spanish synonyms
    print("\n[2/4] Loading Spanish synonyms...")
    n = load_spanish_synonyms(conn)
    es_total = conn.execute("SELECT COUNT(*) FROM synonyms_es").fetchone()[0]
    es_words = conn.execute("SELECT COUNT(DISTINCT word) FROM synonyms_es").fetchone()[0]
    print(f"  Processed {n:,} lines")
    print(f"  Total synonym pairs: {es_total:,}")
    print(f"  Unique words: {es_words:,}")

    # Expand Tol coverage
    print("\n[3/4] Expanding Tol translation coverage via synonyms...")
    results = expand_tol_coverage(conn)

    # Report
    print(f"\n[4/4] Final report...")
    print(f"\n{'='*65}")
    print("  SYNONYM EXPANSION RESULTS")
    print(f"{'='*65}")
    print(f"  Strategy 1 (English direct synonym):     {results['strategy1_en_syn']:>8,}")
    print(f"  Strategy 2 (Spanish direct synonym):     {results['strategy2_es_syn']:>8,}")
    print(f"  Strategy 3 (English→bridge→Tol):         {results['strategy3_en_bridge']:>8,}")
    print(f"  Strategy 4 (Spanish→bridge→Tol):         {results['strategy4_es_bridge']:>8,}")
    print(f"  ────────────────────────────────────── ────────")
    print(f"  TOTAL NEW CONNECTIVE PATHS:              {results['total_inferred']:>8,}")

    # Coverage comparison
    direct_en = conn.execute("SELECT COUNT(DISTINCT source_word) FROM inferred_translations WHERE source_lang='en'").fetchone()[0]
    direct_es = conn.execute("SELECT COUNT(DISTINCT source_word) FROM inferred_translations WHERE source_lang='es'").fetchone()[0]
    direct_tol = conn.execute("SELECT COUNT(DISTINCT tol_word) FROM inferred_translations").fetchone()[0]
    orig_tol = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]

    print(f"\n  New English words that now reach Tol: {direct_en:,}")
    print(f"  New Spanish words that now reach Tol: {direct_es:,}")
    print(f"  Tol words reachable via synonyms:     {direct_tol:,}")
    print(f"  Original Tol dictionary entries:       {orig_tol:,}")

    # High-confidence samples
    print(f"\n  Sample inferred translations (conf ≥ 0.80):")
    for row in conn.execute(
        "SELECT source_word, source_lang, tol_word, confidence, path FROM inferred_translations WHERE confidence >= 0.80 ORDER BY RANDOM() LIMIT 20"
    ):
        print(f"    {row[0]:22s} ({row[1]}) → tol:{row[2]:20s}  [{row[4][:60]}]")

    # Cross-language bridge samples
    print(f"\n  Sample bridge translations (conf = 0.65):")
    for row in conn.execute(
        "SELECT source_word, source_lang, tol_word, confidence, path FROM inferred_translations WHERE confidence = 0.65 ORDER BY RANDOM() LIMIT 15"
    ):
        print(f"    {row[0]:22s} ({row[1]}) → tol:{row[2]:20s}  [{row[4][:70]}]")

    elapsed = time.time() - t0
    print(f"\n  Processing time: {elapsed:.1f}s")

    conn.close()
    print(f"\n{'='*65}")
    print("  DONE")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
