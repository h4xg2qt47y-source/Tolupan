#!/usr/bin/env python3
"""
Deep parse of the Dennis & Dennis 1983 Tol-Spanish dictionary.
Combines two OCR passes and extracts:
  1. Headword entries (Tol → Spanish, Spanish → Tol)
  2. Example sentences (Tol ↔ Spanish pairs)
  3. Adjective/adverb definitions embedded in entries
  4. Conjugation forms mentioned in entries

Then normalizes Tol spelling to match the NT Bible.
"""

import json, re, sqlite3, sys, unicodedata
from collections import defaultdict, Counter
from pathlib import Path
from difflib import SequenceMatcher

PROJ = Path(__file__).resolve().parent.parent
DB_PATH = PROJ / "app" / "data" / "tol.db"

# ── Load OCR passes ─────────────────────────────────────────────────────────

def load_ocr():
    p1 = json.load(open(PROJ / "scripts" / "ocr_raw_pages.json"))
    p3 = json.load(open(PROJ / "scripts" / "ocr_vision_600dpi_pages.json"))
    return p1, p3


# ── Character normalization ─────────────────────────────────────────────────

_CYRILLIC = str.maketrans({
    '\u0430': 'a', '\u0435': 'e', '\u043e': 'o', '\u0441': 'c',
    '\u0440': 'p', '\u0443': 'y', '\u044b': 'bl', '\u0456': 'i',
    '\u0410': 'A', '\u0415': 'E', '\u041e': 'O', '\u0421': 'C',
    '\u0420': 'P', '\u0443': 'y',
})

def _fix(text):
    text = text.translate(_CYRILLIC)
    text = text.replace('\u2019', "'").replace('\u2018', "'")
    text = text.replace('\u201c', '"').replace('\u201d', '"')
    text = text.replace('\u00b4', "'").replace('\u02bc', "'")
    text = text.replace('\u0060', "'").replace('\u00b7', "'")
    text = unicodedata.normalize("NFC", text)
    return text


# ── POS tags ────────────────────────────────────────────────────────────────

POS_CANONICAL = {
    "s": "sustantivo", "sust": "sustantivo", "sustantivo": "sustantivo",
    "n": "sustantivo",
    "v": "verbo", "vt": "verbo", "vi": "verbo", "ve": "verbo",
    "verbo": "verbo",
    "adj": "adjetivo", "adjetivo": "adjetivo",
    "adv": "adverbio", "adverbio": "adverbio",
    "prep": "preposición", "preposición": "preposición",
    "conj": "conjunción", "conjunción": "conjunción",
    "pron": "pronombre", "pronombre": "pronombre",
    "interj": "interjección", "interjección": "interjección",
    "part": "partícula", "partícula": "partícula",
    "num": "numeral",
}

_POS_RE = re.compile(
    r'\((?:' + '|'.join(sorted(POS_CANONICAL.keys(), key=len, reverse=True)) + r')[\s:;,)]*',
    re.IGNORECASE,
)

# ── Spanish function words (for filtering) ──────────────────────────────────

_SPA_FUNC = {
    "el", "la", "los", "las", "un", "una", "unos", "unas",
    "de", "del", "al", "a", "en", "con", "por", "para",
    "es", "son", "está", "están", "ser", "fue", "era",
    "y", "o", "pero", "que", "como", "no", "muy", "más",
    "yo", "tú", "él", "ella", "nosotros", "ellos",
    "mi", "tu", "su", "se", "lo", "le", "me", "te",
}


def _is_spanish_sentence(text):
    words = text.lower().split()
    if len(words) < 4:
        return False
    func_count = sum(1 for w in words if w.rstrip(".,;:!?") in _SPA_FUNC)
    return func_count >= 2


def _has_tol_chars(text):
    return bool(re.search(r"[üïöëṽ'ʼ]", text.lower()))


# ── Parse Tol→Spanish section (pages 12-80) ─────────────────────────────────

def parse_tol_section(pages_300, pages_600, page_nums):
    entries = []
    examples = []

    for pg in page_nums:
        text_300 = _fix(pages_300.get(str(pg), ""))
        text_600 = _fix(pages_600.get(str(pg), ""))

        for text in [text_300, text_600]:
            lines = text.split("\n")
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                if not line or len(line) < 3:
                    i += 1
                    continue

                # Try to match headword entry: "tol_word (POS) spanish_definition"
                m = re.match(
                    r"^([a-záéíóúüïöëñ'ʼ\-\s]{2,40}?)\s*"
                    r"\(([^)]{1,30})\)\s*"
                    r"(.+)$",
                    line, re.IGNORECASE,
                )
                if m:
                    tol = m.group(1).strip().lower()
                    pos_raw = m.group(2).strip().lower()
                    spanish = m.group(3).strip()

                    # Validate
                    if _is_spanish_sentence(tol):
                        i += 1
                        continue
                    if len(tol) < 2 or len(spanish) < 2:
                        i += 1
                        continue

                    pos = POS_CANONICAL.get(pos_raw.split(":")[0].split(",")[0].strip(), "")

                    # Clean spanish - remove trailing POS markers, numbers
                    spanish = re.sub(r'\s*\(\w+\)\s*$', '', spanish)
                    spanish = re.sub(r'^\d+[\.\)]\s*', '', spanish)
                    spanish = spanish.strip(" .,;:")

                    if spanish and len(spanish) > 1:
                        entries.append({
                            "tol": tol, "spanish": spanish.lower(),
                            "category": pos, "section": "tol_to_spanish",
                            "page": pg,
                        })

                # Check for example sentence pairs
                # Pattern: Tol sentence (with Tol chars) followed by Spanish translation
                if _has_tol_chars(line) and not line.startswith("("):
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        if next_line and not _has_tol_chars(next_line) and len(next_line) > 5:
                            if not re.match(r'^[a-záéíóúüïöëñ\'\-]+\s*\(', next_line):
                                tol_sent = line.strip()
                                spa_sent = next_line.strip()
                                if 5 < len(tol_sent) < 200 and 5 < len(spa_sent) < 200:
                                    examples.append({
                                        "tol": tol_sent, "spanish": spa_sent,
                                        "page": pg,
                                    })
                                    i += 2
                                    continue

                i += 1

    return entries, examples


# ── Parse Spanish→Tol section (pages 84-128) ────────────────────────────────

def parse_spanish_section(pages_300, pages_600, page_nums):
    entries = []
    examples = []

    # Spanish→Tol entries follow pattern: "spanish_word: tol_translation"
    # or "spanish_word (POS) tol_translation"
    _SPA_ENTRY = re.compile(
        r'^([a-záéíóúñ\s\-]{2,40}?)(?:\s*\(([^)]*)\))?\s*[:=]\s*(.+)$',
        re.IGNORECASE,
    )
    _SPA_ENTRY2 = re.compile(
        r'^([a-záéíóúñ\s\-]{2,40}?)\s*\(([^)]*)\)\s+(.+)$',
        re.IGNORECASE,
    )

    for pg in page_nums:
        text_300 = _fix(pages_300.get(str(pg), ""))
        text_600 = _fix(pages_600.get(str(pg), ""))

        for text in [text_300, text_600]:
            lines = text.split("\n")
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                if not line or len(line) < 3:
                    i += 1
                    continue

                # Try standard entry pattern
                for pat in [_SPA_ENTRY, _SPA_ENTRY2]:
                    m = pat.match(line)
                    if m:
                        spanish = m.group(1).strip().lower()
                        pos_raw = (m.group(2) or "").strip().lower()
                        tol = m.group(3).strip()

                        if _is_spanish_sentence(spanish):
                            break
                        if len(spanish) < 2 or len(tol) < 2:
                            break

                        pos = POS_CANONICAL.get(pos_raw.split(":")[0].split(",")[0].strip(), "")

                        tol = re.sub(r'\s*\(\w+[:\s].*\)$', '', tol)
                        tol = tol.strip(" .,;:")
                        tol_lower = tol.lower()

                        if tol_lower and not _is_spanish_sentence(tol_lower):
                            entries.append({
                                "tol": tol_lower, "spanish": spanish,
                                "category": pos, "section": "spanish_to_tol",
                                "page": pg,
                            })
                        break

                # Check for example pairs (same as above)
                if _has_tol_chars(line) and not line.startswith("("):
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        if next_line and not _has_tol_chars(next_line) and len(next_line) > 5:
                            tol_sent = line.strip()
                            spa_sent = next_line.strip()
                            if 5 < len(tol_sent) < 200 and 5 < len(spa_sent) < 200:
                                examples.append({
                                    "tol": tol_sent, "spanish": spa_sent,
                                    "page": pg,
                                })
                                i += 2
                                continue

                i += 1

    return entries, examples


# ── Also extract with a simpler "any line with Tol chars" heuristic ──────────

def extract_all_tol_spanish_pairs(pages_300, pages_600, all_pages):
    """Brute-force: find every adjacent line pair where first has Tol chars
    and second looks like Spanish."""
    pairs = []
    seen = set()

    for pg in all_pages:
        for text in [pages_300.get(str(pg), ""), pages_600.get(str(pg), "")]:
            text = _fix(text)
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            for i in range(len(lines) - 1):
                l1, l2 = lines[i], lines[i + 1]
                # l1 should be Tol (has Tol special chars), l2 should be Spanish
                if not _has_tol_chars(l1):
                    continue
                if _has_tol_chars(l2):
                    continue
                if len(l1) < 3 or len(l2) < 3:
                    continue
                # l2 should have Spanish chars
                if not re.search(r'[a-záéíóúñ]', l2.lower()):
                    continue
                key = (l1.lower()[:30], l2.lower()[:30])
                if key in seen:
                    continue
                seen.add(key)
                pairs.append({"tol": l1, "spanish": l2, "page": pg})

    return pairs


# ── Build NT Bible Tol word index for spelling normalization ─────────────────

def build_bible_tol_vocab():
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


def normalize_tol_to_bible(tol_word, bible_vocab):
    """Try to find the NT Bible spelling of a Tol word."""
    tol_lower = tol_word.lower().strip()
    if tol_lower in bible_vocab:
        return tol_lower

    # Try common substitutions
    variants = [tol_lower]

    # Common OCR/spelling differences
    subs = [
        ("ph", "pj"), ("kh", "cj"), ("th", "tj"), ("sh", "sj"),
        ("ch", "cj"), ("ü", "u"), ("ö", "o"), ("ï", "i"),
        ("ë", "e"), ("'", "'"), ("ʼ", "'"), ("j", "h"),
        ("h", "j"), ("k", "c"), ("c", "k"),
    ]

    for old, new in subs:
        if old in tol_lower:
            variants.append(tol_lower.replace(old, new))

    for v in variants:
        if v in bible_vocab:
            return v

    # Fuzzy match within Bible vocab (only for close matches)
    best_match = None
    best_ratio = 0.0
    for bw in bible_vocab:
        if abs(len(bw) - len(tol_lower)) > 3:
            continue
        if bw[0] != tol_lower[0]:
            continue
        ratio = SequenceMatcher(None, tol_lower, bw).ratio()
        if ratio > best_ratio and ratio >= 0.85:
            best_ratio = ratio
            best_match = bw

    if best_match:
        return best_match

    return tol_lower


# ── Deduplicate entries ──────────────────────────────────────────────────────

def dedup_entries(entries):
    seen = {}
    for e in entries:
        key = (e["tol"].lower().strip(), e["spanish"].lower().strip())
        if key not in seen:
            seen[key] = e
    return list(seen.values())


# ── Extract word-level translations from example sentences ───────────────────

def extract_words_from_examples(examples, bible_vocab):
    """For short example pairs, try to extract word-level Tol↔Spanish mappings."""
    word_pairs = []

    for ex in examples:
        tol_words = [re.sub(r'[.,;:!?]', '', w).lower() for w in ex["tol"].split()
                     if len(re.sub(r'[.,;:!?]', '', w)) > 1]
        spa_words = [re.sub(r'[.,;:!?]', '', w).lower() for w in ex["spanish"].split()
                     if len(re.sub(r'[.,;:!?]', '', w)) > 1]

        # Only for short, well-matched sentences (2-5 content words)
        tol_content = [w for w in tol_words if _has_tol_chars(w) or w in bible_vocab]
        spa_content = [w for w in spa_words if w not in _SPA_FUNC]

        # If single content word in both, it's a direct mapping
        if len(tol_content) == 1 and len(spa_content) == 1:
            word_pairs.append({
                "tol": tol_content[0], "spanish": spa_content[0],
                "source": "example_extraction",
            })

    return word_pairs


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Deep Parse: Dennis & Dennis 1983 Tol-Spanish Dictionary")
    print("=" * 70)

    print("\nLoading OCR passes...")
    p1, p3 = load_ocr()
    print(f"  Pass 1 (300dpi): {len(p1)} pages")
    print(f"  Pass 3 (600dpi): {len(p3)} pages")

    print("\nBuilding NT Bible Tol vocabulary...")
    bible_vocab = build_bible_tol_vocab()
    print(f"  {len(bible_vocab)} unique Bible Tol words")

    TOL_PAGES = list(range(12, 81))
    SPA_PAGES = list(range(84, 129))
    ALL_PAGES = TOL_PAGES + SPA_PAGES

    print("\nParsing Tol→Spanish section (pages 12-80)...")
    tol_entries, tol_examples = parse_tol_section(p1, p3, TOL_PAGES)
    print(f"  {len(tol_entries)} headword entries, {len(tol_examples)} examples")

    print("\nParsing Spanish→Tol section (pages 84-128)...")
    spa_entries, spa_examples = parse_spanish_section(p1, p3, SPA_PAGES)
    print(f"  {len(spa_entries)} headword entries, {len(spa_examples)} examples")

    print("\nBrute-force Tol↔Spanish pair extraction...")
    brute_pairs = extract_all_tol_spanish_pairs(p1, p3, ALL_PAGES)
    print(f"  {len(brute_pairs)} Tol↔Spanish line pairs")

    # Combine all entries
    all_entries = tol_entries + spa_entries
    all_examples = tol_examples + spa_examples + brute_pairs

    # Dedup
    all_entries = dedup_entries(all_entries)
    print(f"\nAfter dedup: {len(all_entries)} unique headword entries")

    # Normalize Tol to Bible spelling
    print("\nNormalizing Tol to NT Bible spelling...")
    normalized = 0
    for e in all_entries:
        original = e["tol"]
        bible_form = normalize_tol_to_bible(e["tol"], bible_vocab)
        if bible_form != original.lower().strip():
            e["tol"] = bible_form
            e["tol_original"] = original
            normalized += 1
        else:
            e["tol"] = original.lower().strip()
    print(f"  {normalized} entries normalized to Bible spelling")

    # Extract word pairs from examples
    print("\nExtracting word-level translations from examples...")
    word_pairs = extract_words_from_examples(all_examples, bible_vocab)
    print(f"  {len(word_pairs)} word-level pairs extracted")

    # Normalize word pairs
    for wp in word_pairs:
        wp["tol"] = normalize_tol_to_bible(wp["tol"], bible_vocab)

    # Summary
    print(f"\n{'='*70}")
    print("EXTRACTION SUMMARY")
    print(f"{'='*70}")
    print(f"  Headword entries:  {len(all_entries)}")
    print(f"  Example sentences: {len(all_examples)}")
    print(f"  Word-level pairs:  {len(word_pairs)}")

    # Save
    output = {
        "entries": all_entries,
        "examples": [{"tol": e["tol"], "spanish": e["spanish"], "page": e.get("page", 0)} for e in all_examples],
        "word_pairs": word_pairs,
    }
    out_path = PROJ / "scripts" / "deep_parsed_dictionary.json"
    with open(out_path, "w") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nSaved to {out_path}")

    # Show some stats
    cats = Counter(e.get("category", "unknown") for e in all_entries)
    print(f"\nBy category:")
    for cat, cnt in cats.most_common():
        print(f"  {cat or 'uncategorized':20s} {cnt:>5}")

    # Show samples
    print(f"\nSample entries:")
    for e in all_entries[:10]:
        print(f"  {e['tol']:25s} → {e['spanish']:30s} [{e.get('category','')}]")

    print(f"\nSample examples:")
    for e in all_examples[:5]:
        print(f"  Tol: {e['tol'][:60]}")
        print(f"  Spa: {e['spanish'][:60]}")
        print()

    return output


if __name__ == "__main__":
    output = main()
