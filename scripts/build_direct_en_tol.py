#!/usr/bin/env python3
"""
Build Direct English → Tol Dictionary
========================================
For the ~50,000 most common English words:
  1. Check if already in the English→Tol direct dictionary
  2. If not, check if the word exists in English→Spanish→Tol chain
  3. If found via chain, add as a direct English→Tol entry
  4. Also add from inferred_translations table

This eliminates the runtime En→Spa→Tol hop for the most common words.
"""

import sqlite3
import time
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "app" / "data" / "tol.db"

COMMON_WORDS_URL = "https://raw.githubusercontent.com/first20hours/google-10000-english/master/google-10000-english-usa-no-swears.txt"

def load_common_words():
    """Load a large common English word list.
    Uses the database's en_es_dictionary as the source of 'real' English words,
    sorted by estimated frequency (shorter/simpler words first).
    """
    import urllib.request
    words = []
    try:
        resp = urllib.request.urlopen(COMMON_WORDS_URL, timeout=10)
        for line in resp.read().decode().splitlines():
            w = line.strip().lower()
            if w and len(w) > 1:
                words.append(w)
        print(f"  Loaded {len(words):,} words from Google 10K list")
    except Exception as e:
        print(f"  Could not download word list: {e}")

    return words


def main():
    t0 = time.time()
    conn = sqlite3.connect(str(DB_PATH))

    print("=" * 70)
    print("  BUILD DIRECT ENGLISH → TOL DICTIONARY")
    print("=" * 70)

    # ── Load existing data ──
    print("\n[1] Loading existing data...")

    # Direct Tol dictionary (tol→spanish→english)
    tol_dict = {}  # english_lower → {tol, spanish}
    spa_to_tol = {}  # spanish_lower → {tol, english}
    for r in conn.execute("SELECT tol, spanish, english FROM dictionary"):
        tol, spa, eng = r[0], r[1] or "", r[2] or ""
        spa_l = spa.lower().strip()
        if spa_l:
            if spa_l not in spa_to_tol:
                spa_to_tol[spa_l] = {"tol": tol, "english": eng}
        if eng:
            for w in eng.lower().strip().split(","):
                w = w.strip()
                if w and w not in tol_dict:
                    tol_dict[w] = {"tol": tol, "spanish": spa}

    print(f"  Direct En→Tol entries: {len(tol_dict):,}")
    print(f"  Spa→Tol entries:       {len(spa_to_tol):,}")

    # English→Spanish dictionary
    en_to_spa = defaultdict(list)
    for r in conn.execute("SELECT english, spanish, pos FROM en_es_dictionary"):
        en_to_spa[r[0].lower().strip()].append({"spanish": r[1].lower().strip(), "pos": r[2] or ""})

    print(f"  En→Spa dictionary:     {len(en_to_spa):,}")

    # Inferred translations
    inferred_en = {}
    for r in conn.execute("SELECT source_word, tol_word, confidence FROM inferred_translations WHERE source_lang='en' ORDER BY confidence DESC"):
        w = r[0].lower().strip()
        if w not in inferred_en:
            inferred_en[w] = {"tol": r[1], "confidence": r[2]}

    print(f"  Inferred En→Tol:       {len(inferred_en):,}")

    # ── Load common words ──
    print("\n[2] Loading common English words...")
    common_words = load_common_words()

    # Also get all unique English words from en_es_dictionary, sorted by length (proxy for frequency)
    all_en_words = sorted(en_to_spa.keys(), key=lambda w: (len(w), w))
    target_words = []
    seen = set()

    # Priority 1: Google 10K common words
    for w in common_words:
        if w not in seen:
            target_words.append(w)
            seen.add(w)

    # Priority 2: Short English words from en_es_dictionary (likely common)
    for w in all_en_words:
        if w not in seen and len(w) <= 8:
            target_words.append(w)
            seen.add(w)
        if len(target_words) >= 50000:
            break

    # If still under 50K, add more
    for w in all_en_words:
        if w not in seen:
            target_words.append(w)
            seen.add(w)
        if len(target_words) >= 50000:
            break

    print(f"  Target words to check: {len(target_words):,}")

    # ── Build direct En→Tol ──
    print("\n[3] Building direct English→Tol mappings...")

    already_direct = 0
    chain_found = 0
    inferred_found = 0
    not_found = 0

    new_entries = []  # (english, tol, spanish, source, confidence)

    for word in target_words:
        # Already have direct?
        if word in tol_dict:
            already_direct += 1
            continue

        # Try chain: English → Spanish → Tol
        found_via_chain = False
        if word in en_to_spa:
            for spa_entry in en_to_spa[word]:
                spa_word = spa_entry["spanish"]
                if spa_word in spa_to_tol:
                    tol_entry = spa_to_tol[spa_word]
                    new_entries.append({
                        "english": word,
                        "tol": tol_entry["tol"],
                        "spanish": spa_word,
                        "source": "en_spa_tol_chain",
                        "confidence": 0.85,
                    })
                    chain_found += 1
                    found_via_chain = True
                    break

        if found_via_chain:
            continue

        # Try inferred translations
        if word in inferred_en:
            inf = inferred_en[word]
            new_entries.append({
                "english": word,
                "tol": inf["tol"],
                "spanish": "",
                "source": "inferred_promotion",
                "confidence": inf["confidence"],
            })
            inferred_found += 1
            continue

        not_found += 1

    print(f"  Already direct:   {already_direct:,}")
    print(f"  Found via chain:  {chain_found:,}")
    print(f"  Found inferred:   {inferred_found:,}")
    print(f"  Not found:        {not_found:,}")
    print(f"  New entries:      {len(new_entries):,}")

    # ── Insert into database ──
    print("\n[4] Creating direct_en_tol table...")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS direct_en_tol (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            english TEXT NOT NULL,
            tol TEXT NOT NULL,
            spanish TEXT,
            source TEXT,
            confidence REAL DEFAULT 0.8,
            UNIQUE(english, tol)
        );
        CREATE INDEX IF NOT EXISTS idx_det_en ON direct_en_tol(english);
        CREATE INDEX IF NOT EXISTS idx_det_tol ON direct_en_tol(tol);
    """)

    # First insert existing direct entries from the main dictionary
    existing_added = 0
    for r in conn.execute("SELECT tol, spanish, english FROM dictionary WHERE english IS NOT NULL AND english != ''"):
        tol, spa, eng = r[0], r[1] or "", r[2] or ""
        for w in eng.lower().strip().split(","):
            w = w.strip()
            if w:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO direct_en_tol (english, tol, spanish, source, confidence) VALUES (?,?,?,?,?)",
                        (w, tol, spa, "dictionary_direct", 0.95)
                    )
                    existing_added += 1
                except:
                    pass

    # Insert new chain/inferred entries
    new_added = 0
    for e in new_entries:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO direct_en_tol (english, tol, spanish, source, confidence) VALUES (?,?,?,?,?)",
                (e["english"], e["tol"], e["spanish"], e["source"], e["confidence"])
            )
            new_added += 1
        except:
            pass

    # Also update the main dictionary: add English where missing
    backfilled = 0
    for e in new_entries:
        if e["source"] == "en_spa_tol_chain" and e["spanish"]:
            updated = conn.execute(
                "UPDATE dictionary SET english = ? WHERE tol = ? AND spanish = ? AND (english IS NULL OR english = '')",
                (e["english"], e["tol"], e["spanish"])
            ).rowcount
            backfilled += updated

    conn.commit()

    total_det = conn.execute("SELECT COUNT(*) FROM direct_en_tol").fetchone()[0]
    unique_en = conn.execute("SELECT COUNT(DISTINCT english) FROM direct_en_tol").fetchone()[0]
    unique_tol = conn.execute("SELECT COUNT(DISTINCT tol) FROM direct_en_tol").fetchone()[0]

    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  RESULTS")
    print(f"{'='*70}")
    print(f"  Existing dict entries added: {existing_added:,}")
    print(f"  New chain entries added:     {new_added:,}")
    print(f"  Main dict backfilled:        {backfilled:,}")
    print(f"  Total direct_en_tol:         {total_det:,}")
    print(f"  Unique English words:        {unique_en:,}")
    print(f"  Unique Tol words:            {unique_tol:,}")
    print(f"  Time: {elapsed:.1f}s")
    print(f"{'='*70}")

    conn.close()


if __name__ == "__main__":
    main()
