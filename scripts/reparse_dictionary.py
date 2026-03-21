#!/usr/bin/env python3
"""
Re-parse the Dennis & Dennis 1983 Tol–Spanish dictionary from cached OCR.

The PDF is two-column, but macOS Vision OCR returns lines interleaved
from both columns.  The original parser (`ocr_dictionary.py`) used
simple heuristics that produced ~330 garbage entries (Spanish sentences
as Tol headwords, embedded POS tags, junk categories, etc.).

This script:
  1. Loads ocr_raw_pages.json (already OCR'd).
  2. Re-parses with stricter, dictionary-format-aware heuristics.
  3. Merges with the old parse where the old data is clean.
  4. Writes scripts/parsed_dictionary.json (overwrite).

Run:  python3 scripts/reparse_dictionary.py
Then: python3 scripts/build_en_tol_dictionary.py   (to re-translate + rebuild PDF + DB)
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
OCR_CACHE = BASE / "scripts" / "ocr_raw_pages.json"
OLD_PARSED = BASE / "scripts" / "parsed_dictionary.json"
OUT = BASE / "scripts" / "parsed_dictionary.json"

# ── Canonical POS map ──────────────────────────────────────────────────
POS_CANONICAL = {
    "s": "sustantivo", "f": "sustantivo", "m": "sustantivo",
    "ve": "verbo", "vi": "verbo", "vt": "verbo", "vr": "verbo",
    "vi: i": "verbo", "vi: ii": "verbo", "vt: i": "verbo", "vt: ii": "verbo",
    "vi: 1": "verbo", "vi: 2": "verbo", "vt: 1": "verbo", "vt: 2": "verbo",
    "vt: i, 2": "verbo", "vt: 1,2": "verbo", "vt: i,2": "verbo",
    "vi: i, 2": "verbo", "vc: i": "verbo", "v aux": "verbo",
    "adj": "adjetivo", "adj int": "adjetivo",
    "adv": "adverbio", "adv d": "adverbio", "adv t": "adverbio",
    "adv m": "adverbio", "adv l": "adverbio", "adv 1": "adverbio",
    "adv ca": "adverbio", "adv co": "adverbio", "adv a": "adverbio",
    "adv n": "adverbio", "adv o": "adverbio", "adv int": "adverbio",
    "ady int": "adverbio", "adv rel": "adverbio", "ady ca": "adverbio",
    "pro": "pronombre", "pro dem": "pronombre", "pro int": "pronombre",
    "pron dem": "pronombre", "pron int": "pronombre", "pron per": "pronombre",
    "pron ind": "pronombre", "pron pos": "pronombre", "pron rel": "pronombre",
    "conj": "conjunción", "prep": "preposición", "int": "interjección",
}

# Regex that matches a POS tag in parentheses
_POS_RE = re.compile(
    r'\(\s*('
    + '|'.join(re.escape(k) for k in sorted(POS_CANONICAL, key=len, reverse=True))
    + r')\s*\)',
    re.IGNORECASE,
)

# Spanish function words — if ≥2 of these appear in a "Tol" headword with >4 words, it's Spanish
_SP_FUNC = frozenset(
    "el la los las un una de del en que es no se su con por para muy más como "
    "cuando yo tú él me mi le lo al y a".split()
)

# Tol-characteristic characters
_TOL_CHARS = set("'üïöëṽ'ʼ")


def _fix_cyrillic(s: str) -> str:
    """Replace Cyrillic lookalikes that Vision OCR sometimes produces."""
    table = str.maketrans({
        "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y",
        "х": "x", "А": "A", "Е": "E", "О": "O", "Р": "P", "С": "C",
        "У": "Y", "Х": "X", "т": "t", "п": "p", "н": "n",
        "к": "k", "м": "m", "и": "i", "й": "ü",
        "К": "K", "М": "M", "И": "I", "Й": "Ü",
        "в": "v", "В": "V", "д": "d", "з": "z",
        "л": "l", "ц": "ts", "б": "b", "г": "g",
        "ж": "zh", "ф": "f", "ш": "sh", "щ": "shch",
        "ы": "y", "э": "e", "ю": "yu", "я": "ya",
        "ь": "'", "ъ": "",
        "ї": "i", "і": "i", "є": "e", "ґ": "g",
    })
    return s.translate(table)


def _normalise(s: str) -> str:
    s = _fix_cyrillic(s)
    s = s.replace("\u2018", "'").replace("\u2019", "'").replace("\u02BC", "'")
    s = s.replace("…", "...").replace("\u00AD", "")
    return s.strip()


def _is_spanish_sentence(text: str) -> bool:
    words = text.split()
    if len(words) < 4:
        return False
    sp_count = sum(1 for w in words if w.lower().rstrip(".,;:!?") in _SP_FUNC)
    return sp_count >= 2


def _has_tol_chars(text: str) -> bool:
    return bool(set(text) & _TOL_CHARS)


def _clean_headword(hw: str) -> str:
    """Strip trailing punctuation, leading/trailing junk."""
    hw = hw.strip().rstrip(".,:;!?")
    hw = re.sub(r'^[\s=*•"]+', '', hw)
    hw = re.sub(r'[\s=*•"]+$', '', hw)
    return hw.strip()


def _clean_definition(d: str) -> str:
    """Strip leading numbering artifacts from OCR (e.g. '1 yo tiro' → 'yo tiro')."""
    d = re.sub(r'^\d+\s+', '', d.strip())
    d = re.sub(r'^[=*•"]+\s*', '', d)
    return d.strip().rstrip(".")


# ── Tol→Spanish parser ─────────────────────────────────────────────────

def parse_tol_spanish_section(pages: dict) -> tuple[list[dict], list[dict]]:
    """Parse pages 12-80 (Tol→Spanish).  Returns (entries, sentences)."""
    entries: list[dict] = []
    sentences: list[dict] = []

    text = "\n".join(pages.get(str(i), "") for i in range(12, 81))
    lines = [_normalise(l) for l in text.split("\n")]

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Skip page numbers, blanks, short junk
        if not line or line.isdigit() or len(line) < 3 or line in ("A", "B", "C", "D", "E", "F",
                "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
                "U", "V", "W", "X", "Y", "Z"):
            i += 1
            continue

        # Try to match:  headword (POS) definition
        m = _POS_RE.search(line)
        if m:
            pos_raw = m.group(1).strip().lower()
            pos_cat = POS_CANONICAL.get(pos_raw, pos_raw)
            before = line[:m.start()].strip()
            after = line[m.end():].strip()

            headword = _clean_headword(before)
            definition = _clean_definition(after)

            # Sometimes definition wraps to next line
            if not definition and i + 1 < len(lines):
                next_l = lines[i + 1].strip()
                if next_l and not _POS_RE.search(next_l) and len(next_l) < 120:
                    definition = _clean_definition(next_l)
                    i += 1

            # Validate
            if (headword and definition
                    and 1 < len(headword) < 60
                    and not _is_spanish_sentence(headword)
                    and not headword.isdigit()
                    and not re.match(r'^[A-Z][A-Z\s]+$', headword)):
                entries.append({
                    "tol": headword,
                    "spanish": definition,
                    "category": pos_cat,
                    "section": "tol_to_spanish",
                })
        else:
            # Check for example sentence pairs (Tol line, then Spanish line)
            if (_has_tol_chars(line) and not _is_spanish_sentence(line)
                    and 8 < len(line) < 200):
                # Look ahead for Spanish translation
                if i + 1 < len(lines):
                    next_l = lines[i + 1].strip()
                    if (next_l and _is_spanish_sentence(next_l)
                            and not _POS_RE.search(next_l)
                            and 5 < len(next_l) < 200):
                        sentences.append({
                            "tol": line.rstrip("."),
                            "spanish": next_l.rstrip("."),
                            "source": "SIL_Dictionary_Example",
                        })
                        i += 2
                        continue
        i += 1

    return entries, sentences


# ── Spanish→Tol parser ─────────────────────────────────────────────────

_SPA_ENTRY_RE = re.compile(
    r'^([a-záéíóúñü¿¡][\w\s,\'áéíóúñü¿¡]{1,60}?)\s*'  # Spanish headword
    r'(?:\([^)]*\)\s*)?'                                   # optional POS in parens
    r'(?::\s*(?:yo\s+\w+|él\s+\w+|me\s+\w+|se\s+\w+)\s*)?'  # optional "yo verbo" gloss
    r'\(('
    + '|'.join(re.escape(k) for k in sorted(POS_CANONICAL, key=len, reverse=True))
    + r')\)\s*(.+)',
    re.IGNORECASE,
)


def parse_spanish_tol_section(pages: dict) -> tuple[list[dict], list[dict]]:
    """Parse pages 84-128 (Spanish→Tol).  Returns (entries, sentences)."""
    entries: list[dict] = []
    sentences: list[dict] = []

    text = "\n".join(pages.get(str(i), "") for i in range(84, 129))
    lines = [_normalise(l) for l in text.split("\n")]

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if not line or line.isdigit() or len(line) < 3:
            i += 1
            continue

        # Pattern 1: "Spanish word (POS) tol_word"
        m = _SPA_ENTRY_RE.match(line)
        if m:
            spanish = _clean_definition(m.group(1).strip().rstrip(":;,. "))
            pos_raw = m.group(2).strip().lower()
            tol_part = _clean_headword(m.group(3).strip())
            pos_cat = POS_CANONICAL.get(pos_raw, pos_raw)

            if (spanish and tol_part
                    and not spanish.isdigit()
                    and not re.match(r'^[A-Z]{2,}', spanish)
                    and len(tol_part) < 80
                    and not _is_spanish_sentence(tol_part)):
                entries.append({
                    "tol": tol_part,
                    "spanish": spanish,
                    "category": pos_cat,
                    "section": "spanish_to_tol",
                })
                i += 1
                continue

        # Pattern 2: simple "headword: ... (POS) tol"
        m2 = _POS_RE.search(line)
        if m2:
            before = line[:m2.start()].strip()
            after = line[m2.end():].strip()
            pos_raw = m2.group(1).strip().lower()
            pos_cat = POS_CANONICAL.get(pos_raw, pos_raw)

            # In Spanish→Tol, before is Spanish, after is Tol
            spanish = _clean_definition(before.rstrip(":;,. "))
            tol_part = _clean_headword(after)

            # Quick validation: before should look Spanish-ish, after should not be a Spanish sentence
            if (spanish and tol_part
                    and len(spanish) < 80 and len(tol_part) < 80
                    and not _is_spanish_sentence(tol_part)
                    and not spanish.isdigit()):
                entries.append({
                    "tol": tol_part,
                    "spanish": spanish,
                    "category": pos_cat,
                    "section": "spanish_to_tol",
                })
                i += 1
                continue

        # Example sentences
        if (_has_tol_chars(line) and not _is_spanish_sentence(line)
                and 8 < len(line) < 200 and not _POS_RE.search(line)):
            if i + 1 < len(lines):
                next_l = lines[i + 1].strip()
                if (next_l and _is_spanish_sentence(next_l)
                        and not _POS_RE.search(next_l)
                        and 5 < len(next_l) < 200):
                    sentences.append({
                        "tol": line.rstrip("."),
                        "spanish": next_l.rstrip("."),
                        "source": "SIL_Dictionary_Example",
                    })
                    i += 2
                    continue

        i += 1

    return entries, sentences


# ── Post-processing / validation ───────────────────────────────────────

def _validate_entry(e: dict) -> bool:
    tol = e["tol"].strip()
    spa = e["spanish"].strip()

    if not tol or not spa:
        return False
    if len(tol) < 2 or len(spa) < 2:
        return False
    if tol.isdigit() or spa.isdigit():
        return False
    # Tol field shouldn't be a full Spanish sentence
    if _is_spanish_sentence(tol):
        return False
    # Reject if Tol headword contains common Spanish words that indicate OCR line merge
    _SP_MIXED = {"nosotros", "ustedes", "ellos", "porque", "cuando", "también", "entonces"}
    tol_words = set(w.lower() for w in tol.split())
    if tol_words & _SP_MIXED:
        return False
    # Multi-word Tol with trailing Spanish fragments
    tw = tol.split()
    if len(tw) > 3:
        sp_tail = sum(1 for w in tw[1:] if w.lower() in _SP_FUNC)
        if sp_tail >= 1:
            return False
    # Tol shouldn't start with a bare POS tag (but allow Tol words like vi'ün, vi'il)
    if re.match(r'^\((?:s|vi|vt|adj|adv|pro|int|conj|prep|ve|vr|vc)\b', tol, re.I):
        return False
    if re.match(r'^(?:s|vi|vt|adj|adv|pro|int|conj|prep|ve|vr|vc)\s', tol, re.I):
        # Only reject if not followed by a Tol apostrophe (vi'ün is legit Tol)
        if "'" not in tol[:6] and "'" not in tol[:6]:
            return False
    # Tol shouldn't contain embedded POS pattern like "word (s) word"
    if re.search(r'\s\((?:s|vi|vt|adj|adv|pro|int|conj|prep|ve|vr|vc)\)\s', tol, re.I):
        return False
    # Category should be a real POS
    cat = e.get("category", "")
    if cat and cat not in ("sustantivo", "verbo", "adjetivo", "adverbio",
                           "pronombre", "conjunción", "preposición", "interjección"):
        return False
    return True


def _validate_sentence(s: dict) -> bool:
    tol = s["tol"].strip()
    spa = s["spanish"].strip()
    if not tol or not spa or len(tol) < 5 or len(spa) < 5:
        return False
    # Tol side shouldn't be entirely Spanish
    if _is_spanish_sentence(tol) and not _has_tol_chars(tol):
        return False
    # Both sides shouldn't be identical
    if tol.casefold() == spa.casefold():
        return False
    # Tol side shouldn't contain POS tags
    if _POS_RE.search(tol):
        return False
    return True


def dedup(items: list[dict], key_fn) -> list[dict]:
    seen: set = set()
    out: list[dict] = []
    for item in items:
        k = key_fn(item)
        if k not in seen:
            seen.add(k)
            out.append(item)
    return out


def main() -> None:
    pages = json.loads(Path(OCR_CACHE).read_text(encoding="utf-8"))
    print(f"Loaded {len(pages)} OCR pages")

    ts_entries, ts_sents = parse_tol_spanish_section(pages)
    print(f"Tol→Spanish: {len(ts_entries)} entries, {len(ts_sents)} example sentences")

    st_entries, st_sents = parse_spanish_tol_section(pages)
    print(f"Spanish→Tol: {len(st_entries)} entries, {len(st_sents)} example sentences")

    all_entries = ts_entries + st_entries
    all_sents = ts_sents + st_sents

    # Validate
    valid_entries = [e for e in all_entries if _validate_entry(e)]
    valid_sents = [s for s in all_sents if _validate_sentence(s)]
    print(f"After validation: {len(valid_entries)} entries (dropped {len(all_entries)-len(valid_entries)}), "
          f"{len(valid_sents)} sentences (dropped {len(all_sents)-len(valid_sents)})")

    # Dedup
    valid_entries = dedup(valid_entries, lambda e: (e["tol"].casefold(), e["spanish"].casefold()))
    valid_sents = dedup(valid_sents, lambda s: (s["tol"].casefold(), s["spanish"].casefold()))
    print(f"After dedup: {len(valid_entries)} entries, {len(valid_sents)} sentences")

    # Also salvage clean entries from old parse that we might have missed
    if OLD_PARSED.exists():
        old = json.loads(OLD_PARSED.read_text(encoding="utf-8"))
        old_entries = old.get("dictionary", [])
        old_sents = old.get("sentences", [])
        existing_keys = {(e["tol"].casefold(), e["spanish"].casefold()) for e in valid_entries}
        existing_sent_keys = {(s["tol"].casefold(), s["spanish"].casefold()) for s in valid_sents}
        salvaged_e = 0
        for oe in old_entries:
            oe["tol"] = _clean_headword(_normalise(oe.get("tol", "")))
            oe["spanish"] = _clean_definition(_normalise(oe.get("spanish", "")))
            raw_cat = oe.get("category", "").strip().lower()
            oe["category"] = POS_CANONICAL.get(raw_cat, raw_cat)
            k = (oe["tol"].casefold(), oe["spanish"].casefold())
            if k not in existing_keys and _validate_entry(oe):
                valid_entries.append(oe)
                existing_keys.add(k)
                salvaged_e += 1
        salvaged_s = 0
        for os_ in old_sents:
            os_["tol"] = _normalise(os_.get("tol", ""))
            os_["spanish"] = _normalise(os_.get("spanish", ""))
            k = (os_["tol"].casefold(), os_["spanish"].casefold())
            if k not in existing_sent_keys and _validate_sentence(os_):
                valid_sents.append(os_)
                existing_sent_keys.add(k)
                salvaged_s += 1
        print(f"Salvaged from old parse: {salvaged_e} entries, {salvaged_s} sentences")

    print(f"\nFinal: {len(valid_entries)} dictionary entries, {len(valid_sents)} example sentences")

    # Spot check
    print("\n=== Sample entries ===")
    import random
    random.seed(42)
    for e in random.sample(valid_entries, min(10, len(valid_entries))):
        print(f"  {e['tol'][:40]:40s}  →  {e['spanish'][:45]:45s}  [{e['category']}]")

    OUT.write_text(
        json.dumps({"dictionary": valid_entries, "sentences": valid_sents},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
