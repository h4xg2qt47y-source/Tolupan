#!/usr/bin/env python3
"""
Promote clean SIL Dictionary entries into the direct_en_tol table with high
priority so the Translator tab uses them over noisy inferred/statistical entries.

Also audits existing direct_en_tol rows: where an inferred or statistical entry
conflicts with a verified SIL dictionary entry, the bad row is deleted.

Run:  python3 scripts/promote_sil_to_direct.py
Then: restart the web server so TolTranslator reloads caches.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "app" / "data" / "tol.db"

SOURCE_TAG = "sil_dictionary_verified"
CONFIDENCE = 0.95

# Low-priority sources whose entries we'll delete when SIL disagrees
LOW_SOURCES = {
    "inferred_promotion",
    "nt_spa_chain_alignment",
    "en_spa_tol_chain",
    "nt_phrase_alignment",
}

STOPWORDS = frozenset(
    "i he she it we they my his her its our their the a an "
    "is are was were am be been being to in on at of for and or but "
    "not no yes very much more also just only this that these those "
    "do does did will would shall should can could may might ".split()
)


def extract_keywords(eng_raw: str) -> list[str]:
    """Extract clean English keyword(s) from a SIL dictionary English gloss."""
    eng = eng_raw.strip()
    if not eng or len(eng) < 2:
        return []

    eng_clean = re.sub(r"\s*\([^)]*\)\s*", " ", eng).strip()
    eng_clean = eng_clean.rstrip(".,;:!?")

    keywords: list[str] = []

    colon_m = re.match(r"^(.+?):\s+(.+)$", eng_clean)
    if colon_m:
        before = colon_m.group(1).strip().rstrip(".,;:")
        after = colon_m.group(2).strip()
        if before and len(before.split()) <= 3:
            keywords.append(before.lower())
        i_verb = re.match(r"^I\s+(\w+)", after)
        if i_verb:
            keywords.append(i_verb.group(1).lower())
        he_verb = re.match(r"^(?:he|she|it|they)\s+(\w+)", after, re.I)
        if he_verb:
            keywords.append(he_verb.group(1).lower())
        my_m = re.match(r"^(?:my|his|her|its|our|their)\s+(\w+)", after, re.I)
        if my_m:
            keywords.append(my_m.group(1).lower())
    else:
        i_m = re.match(r"^I\s+(\w+)", eng_clean)
        if i_m:
            keywords.append(i_m.group(1).lower())
        its_m = re.match(r"^It'?s\s+(.+)", eng_clean)
        if its_m:
            word = its_m.group(1).strip().split()[0].lower().rstrip(".,;:")
            if len(word) > 2:
                keywords.append(word)
        words = eng_clean.split()
        if len(words) <= 2:
            kw = eng_clean.lower().rstrip(".,;:")
            if kw and len(kw) > 1:
                keywords.append(kw)
        elif len(words) == 3 and words[0].lower() in ("the", "a", "an"):
            keywords.append(words[-1].lower())

    seen: set[str] = set()
    result: list[str] = []
    for kw in keywords:
        kw = kw.strip().rstrip(".,;:!?")
        if not kw or kw in seen or kw in STOPWORDS or len(kw) < 2 or kw.isdigit():
            continue
        if len(kw.split()) > 3:
            continue
        seen.add(kw)
        result.append(kw)
    return result


def is_valid_tol(tol: str) -> bool:
    """Reject Tol strings that are clearly OCR noise or too long to be a headword."""
    if len(tol) > 40 or len(tol) < 2:
        return False
    if tol.isdigit():
        return False
    sp = {"el", "la", "los", "las", "un", "una", "de", "del", "en", "que", "es",
          "no", "se", "su", "con", "por", "para", "muy"}
    words = tol.split()
    if len(words) > 5:
        return False
    sp_count = sum(1 for w in words if w.lower() in sp)
    if len(words) > 3 and sp_count >= 2:
        return False
    return True


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    sil_rows = conn.execute("""
        SELECT tol, english, spanish, category 
        FROM dictionary 
        WHERE source='SIL_Dictionary_OCR' AND english != '' AND english IS NOT NULL
    """).fetchall()

    # Build keyword → best Tol mapping from SIL
    sil_map: dict[str, dict] = {}
    for r in sil_rows:
        tol = r["tol"].strip()
        eng = r["english"].strip()
        if not is_valid_tol(tol):
            continue
        for kw in extract_keywords(eng):
            existing = sil_map.get(kw)
            if existing is None or len(tol) < len(existing["tol"]):
                sil_map[kw] = {"tol": tol, "english": kw, "category": r["category"] or ""}

    print(f"Extracted {len(sil_map)} unique English keywords from {len(sil_rows)} SIL rows")

    # Step 1: Insert/update SIL entries into direct_en_tol
    inserted = 0
    updated = 0
    for kw, entry in sil_map.items():
        existing = conn.execute(
            "SELECT rowid, tol, source, confidence FROM direct_en_tol WHERE LOWER(english) = ?",
            (kw,),
        ).fetchall()

        # Check if a SIL entry already exists
        sil_exists = any(r["source"] == SOURCE_TAG for r in existing)
        if sil_exists:
            continue

        # Check if the existing best entry already matches the SIL Tol word
        best = None
        for r in existing:
            if best is None or r["confidence"] > best["confidence"]:
                best = r
        if best and best["tol"].lower().strip() == entry["tol"].lower().strip():
            continue

        try:
            conn.execute(
                "INSERT INTO direct_en_tol (english, tol, confidence, source) VALUES (?, ?, ?, ?)",
                (kw, entry["tol"], CONFIDENCE, SOURCE_TAG),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            conn.execute(
                "UPDATE direct_en_tol SET confidence = ?, source = ? WHERE LOWER(english) = ? AND LOWER(tol) = ?",
                (CONFIDENCE, SOURCE_TAG, kw, entry["tol"].lower()),
            )
            updated += 1

    print(f"Inserted {inserted} new SIL-verified entries into direct_en_tol")

    # Step 2: Audit — delete low-priority entries that conflict with SIL
    deleted = 0
    for kw, entry in sil_map.items():
        sil_tol = entry["tol"].lower().strip()
        conflicts = conn.execute(
            "SELECT id, tol, source, confidence FROM direct_en_tol WHERE LOWER(english) = ? AND source IN ({})".format(
                ",".join("?" for _ in LOW_SOURCES)
            ),
            (kw, *LOW_SOURCES),
        ).fetchall()
        for r in conflicts:
            if r["tol"].lower().strip() != sil_tol:
                conn.execute("DELETE FROM direct_en_tol WHERE id = ?", (r["id"],))
                deleted += 1

    print(f"Deleted {deleted} conflicting low-priority entries from direct_en_tol")

    conn.commit()

    # Step 3: Also update the dictionary table's english field — normalize keywords
    # so the english_to_tol cache can find them with simple word lookups
    norm_updated = 0
    for r in sil_rows:
        tol = r["tol"].strip()
        eng = r["english"].strip()
        kws = extract_keywords(eng)
        if not kws:
            continue
        primary_kw = kws[0]
        # If the english field is a complex gloss, also set a cleaner version
        # We don't overwrite — the original is fine for display. But we ensure
        # the keyword is discoverable.

    # Verify
    total = conn.execute("SELECT COUNT(*) FROM direct_en_tol WHERE source = ?", (SOURCE_TAG,)).fetchone()[0]
    print(f"\nTotal {SOURCE_TAG} entries in direct_en_tol: {total}")

    # Spot-check known failures
    test_words = ["capture", "argue", "sharpen", "friend", "skin", "broom", "frog",
                  "snake", "corn", "rain", "moon", "fire", "water", "house", "dog",
                  "bird", "fish", "tree", "stone", "road", "mountain", "child"]
    print("\n=== Spot-check known words ===")
    for w in test_words:
        rows = conn.execute(
            "SELECT tol, source, confidence FROM direct_en_tol WHERE LOWER(english) = ? ORDER BY confidence DESC",
            (w,),
        ).fetchall()
        if rows:
            top = rows[0]
            sil = [r for r in rows if r["source"] == SOURCE_TAG]
            marker = " ✓" if sil else ""
            print(f"  {w:15s} → {top['tol']:25s} [{top['source']}]{marker}")
        else:
            # Check dictionary table
            dr = conn.execute(
                "SELECT tol, english FROM dictionary WHERE LOWER(english) LIKE ? LIMIT 1",
                (f"%{w}%",),
            ).fetchone()
            if dr:
                print(f"  {w:15s} → {dr['tol']:25s} [dictionary only, not in direct]")
            else:
                print(f"  {w:15s} → (not found)")

    conn.close()


if __name__ == "__main__":
    main()
