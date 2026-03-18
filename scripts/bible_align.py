#!/usr/bin/env python3
"""
Bible Verse Alignment Engine for Tol-Spanish-English parallel corpus extraction.

Multi-strategy approach:
1. Parse HTML verse markup from all 3 translations
2. Align by verse ID (V1, V2, ...)
3. PATTERN EXTRACTION: Detect repeating structural patterns (e.g., genealogies)
4. KNOWN-WORD ELIMINATION: For each verse, remove known translations → remaining
   words on each side are candidate pairs
5. CO-OCCURRENCE: Standard Dice coefficient across all verses in scope
6. PHRASE ALIGNMENT: Use sequential word positions within aligned verses
7. CROSS-VERIFICATION: Tol→Spanish and Tol→English candidates that triangulate
   (Spanish↔English are known) get a confidence boost
"""

import re
import json
import sqlite3
import sys
from pathlib import Path
from collections import Counter, defaultdict
from html import unescape
from difflib import SequenceMatcher
from itertools import product as cartprod

BASE = Path(__file__).resolve().parent.parent
TOL_NT = BASE / "Tol Translation" / "NT_Text"
ENG_NT = BASE / "Tol Translation" / "English_NT"
SPA_NT = BASE / "Tol Translation" / "Spanish_NT"
DB_PATH = BASE / "app" / "data" / "tol.db"

STOPWORDS_ES = {
    "a", "al", "ante", "bajo", "con", "contra", "de", "del", "desde",
    "durante", "e", "el", "ella", "ellas", "ellos", "en", "entre", "era",
    "es", "esa", "esas", "ese", "eso", "esos", "esta", "estas", "este",
    "esto", "estos", "fue", "ha", "hacia", "hasta", "la", "las", "le",
    "les", "lo", "los", "me", "mi", "muy", "más", "ni", "no", "nos",
    "o", "para", "pero", "por", "que", "quien", "se", "si", "sin", "sobre",
    "son", "su", "sus", "también", "te", "ti", "tu", "tus", "un", "una",
    "unas", "uno", "unos", "y", "ya", "yo", "él", "como",
    "había", "ser", "cuando", "así", "aquel", "aquí", "estas", "esto",
}

STOPWORDS_EN = {
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but",
    "by", "did", "do", "does", "for", "from", "had", "has", "have", "he",
    "her", "him", "his", "how", "i", "if", "in", "into", "is", "it",
    "its", "may", "me", "my", "no", "nor", "not", "of", "on", "or",
    "our", "out", "own", "say", "she", "so", "some", "than", "that",
    "the", "their", "them", "then", "there", "these", "they", "this",
    "those", "to", "up", "us", "was", "we", "were", "what", "when",
    "which", "who", "whom", "will", "with", "would", "you", "your",
    "shall", "all", "about", "after", "before", "like",
}

# Light Tol stopwords (function words that appear very frequently)
STOPWORDS_TOL = {
    "la", "na", "ne", "nin", "ca", "wa", "way", "ya",
    "ma", "mpes", "p'in",
}


# ── HTML Parsing ──────────────────────────────────────────────────────────

def parse_verses(html_path: Path) -> dict[int, str]:
    """Extract {verse_num: clean_text} from an eBible HTML chapter file."""
    text = html_path.read_text(encoding="utf-8")
    text = text.replace("&#160;", " ").replace("&nbsp;", " ")
    text = unescape(text)

    text = re.sub(r'<span class="popup">.*?</span>', '', text)
    text = re.sub(r'<a[^>]*class="notemark"[^>]*>.*?</a>', '', text)
    text = re.sub(r"<span class=['\"]add['\"]>(.*?)</span>", r"\1", text)

    verses = {}
    pattern = (
        r'<span\s+class="verse"\s+id="V(\d+)">\s*\d+\s*</span>'
        r'(.*?)'
        r'(?=<span\s+class="verse"|<ul\s|<div\s+class=[\'"](?:footnote|copyright|tnav|s\b|r\b))'
    )
    for m in re.finditer(pattern, text, re.DOTALL):
        vnum = int(m.group(1))
        raw = m.group(2)
        clean = re.sub(r'<[^>]+>', ' ', raw)
        clean = re.sub(r'\s+', ' ', clean).strip()
        if clean:
            verses[vnum] = clean
    return verses


def tokenize(text: str) -> list[str]:
    return [w for w in re.findall(r"[a-záéíóúüñ'']+", text.lower()) if len(w) > 1]


def tokenize_keep_order(text: str) -> list[str]:
    return re.findall(r"[a-záéíóúüñ']+", text.lower())


# ── Database Helpers ──────────────────────────────────────────────────────

def load_existing_dictionary():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    tol_to_spa = defaultdict(set)
    spa_to_tol = defaultdict(set)
    tol_to_eng = defaultdict(set)
    eng_to_tol = defaultdict(set)
    known_tol_words = set()
    known_spa_words = set()
    known_eng_words = set()

    for row in conn.execute("SELECT tol, spanish, english FROM dictionary"):
        t = row["tol"].lower().strip()
        s = row["spanish"].lower().strip()
        e = (row["english"] or "").lower().strip()
        for tw in tokenize(t):
            tol_to_spa[tw].add(s)
            known_tol_words.add(tw)
        for sw in tokenize(s):
            spa_to_tol[sw].add(t)
            known_spa_words.add(sw)
        if e:
            for ew in tokenize(e):
                tol_to_eng[tw].add(e)
                eng_to_tol[ew].add(t)
                known_eng_words.add(ew)

    conn.close()
    return {
        "tol_to_spa": dict(tol_to_spa),
        "spa_to_tol": dict(spa_to_tol),
        "tol_to_eng": dict(tol_to_eng),
        "eng_to_tol": dict(eng_to_tol),
        "known_tol": known_tol_words,
        "known_spa": known_spa_words,
        "known_eng": known_eng_words,
    }


# ── Proper Noun Detection ────────────────────────────────────────────────

def find_proper_nouns(tol_verses, spa_verses, eng_verses):
    proper_nouns = set()
    for vnum in tol_verses:
        if vnum not in spa_verses or vnum not in eng_verses:
            continue
        tol_caps = set(re.findall(r"[A-ZÁÉÍÓÚÜ][a-záéíóúüñ']{2,}", tol_verses[vnum]))
        spa_caps = set(re.findall(r"[A-ZÁÉÍÓÚÜ][a-záéíóúüñ']{2,}", spa_verses[vnum]))
        eng_caps = set(re.findall(r"[A-Z][a-z']{2,}", eng_verses[vnum]))
        all_caps = tol_caps | spa_caps | eng_caps
        for w1 in all_caps:
            for w2 in all_caps:
                if w1 == w2:
                    continue
                if SequenceMatcher(None, w1.lower(), w2.lower()).ratio() >= 0.7:
                    proper_nouns.add(w1.lower())
                    proper_nouns.add(w2.lower())
        # Single-word matches
        for tw in tol_caps:
            if tw.lower() in {sw.lower() for sw in spa_caps} or tw.lower() in {ew.lower() for ew in eng_caps}:
                proper_nouns.add(tw.lower())

    # Also add well-known biblical names
    biblical_names = {
        "dios", "jesús", "jesucristo", "cristo", "maría", "josé",
        "god", "jesus", "christ", "mary", "joseph", "lord",
        "espíritu", "santo", "spirit", "holy", "angel", "ángel",
    }
    proper_nouns |= biblical_names
    return proper_nouns


# ── Strategy 1: Pattern-Based Extraction ──────────────────────────────────

def extract_patterns(tol_verses, spa_verses, eng_verses, proper_nouns):
    """
    Find repeating structural patterns across verses and extract vocabulary.
    E.g., genealogy pattern: "X Y popay waytsja" = "Y engendró a X" = "Y became the father of X"
    """
    discoveries = []

    # Collect all Tol tokens (minus proper nouns) and their verse contexts
    tol_patterns = defaultdict(list)  # word -> [verse_nums where it appears]
    for vnum, text in tol_verses.items():
        for w in set(tokenize(text)) - proper_nouns - STOPWORDS_TOL:
            tol_patterns[w].append(vnum)

    spa_patterns = defaultdict(list)
    for vnum, text in spa_verses.items():
        for w in set(tokenize(text)) - proper_nouns - STOPWORDS_ES:
            spa_patterns[w].append(vnum)

    eng_patterns = defaultdict(list)
    for vnum, text in eng_verses.items():
        for w in set(tokenize(text)) - proper_nouns - STOPWORDS_EN:
            eng_patterns[w].append(vnum)

    # Find Tol words that appear in many verses and check which Spanish/English
    # words have the most similar verse distributions
    for tw, t_verses in tol_patterns.items():
        if len(t_verses) < 2:
            continue
        t_set = set(t_verses)

        # Score Spanish candidates by Jaccard similarity of verse sets
        best_spa = []
        for sw, s_verses in spa_patterns.items():
            s_set = set(s_verses)
            inter = len(t_set & s_set)
            if inter < 2:
                continue
            union = len(t_set | s_set)
            jaccard = inter / union
            best_spa.append((sw, jaccard, inter))

        best_eng = []
        for ew, e_verses in eng_patterns.items():
            e_set = set(e_verses)
            inter = len(t_set & e_set)
            if inter < 2:
                continue
            union = len(t_set | e_set)
            jaccard = inter / union
            best_eng.append((ew, jaccard, inter))

        best_spa.sort(key=lambda x: x[1], reverse=True)
        best_eng.sort(key=lambda x: x[1], reverse=True)

        if best_spa:
            sw, jac, co = best_spa[0]
            if jac >= 0.3:
                discoveries.append({
                    "tol": tw, "other": sw, "lang": "spanish",
                    "jaccard": round(jac, 4), "cooccur": co,
                    "tol_freq": len(t_verses),
                    "method": "pattern",
                })
        if best_eng:
            ew, jac, co = best_eng[0]
            if jac >= 0.3:
                discoveries.append({
                    "tol": tw, "other": ew, "lang": "english",
                    "jaccard": round(jac, 4), "cooccur": co,
                    "tol_freq": len(t_verses),
                    "method": "pattern",
                })

    return discoveries


# ── Strategy 2: Known-Word Elimination ────────────────────────────────────

def known_word_elimination(tol_verses, spa_verses, eng_verses, existing, proper_nouns):
    """
    For each verse pair, remove all known translations from both sides.
    What remains are candidate new translations.
    
    If there's only 1 unknown Tol word and 1 unknown Spanish word remaining,
    that's a HIGH-CONFIDENCE new translation.
    """
    discoveries = []

    for vnum in tol_verses:
        # ── Tol ↔ Spanish ──
        if vnum in spa_verses:
            tol_tokens = [w for w in tokenize(tol_verses[vnum])
                          if w not in proper_nouns and w not in STOPWORDS_TOL and len(w) > 2]
            spa_tokens = [w for w in tokenize(spa_verses[vnum])
                          if w not in proper_nouns and w not in STOPWORDS_ES and len(w) > 2]

            unknown_tol = [w for w in tol_tokens if w not in existing["known_tol"]]
            unknown_spa = [w for w in spa_tokens if w not in existing["known_spa"]]

            # Remove duplicates while preserving order
            unknown_tol = list(dict.fromkeys(unknown_tol))
            unknown_spa = list(dict.fromkeys(unknown_spa))

            if len(unknown_tol) == 1 and len(unknown_spa) == 1:
                discoveries.append({
                    "tol": unknown_tol[0], "other": unknown_spa[0],
                    "lang": "spanish", "confidence": "high",
                    "verse": vnum, "method": "elimination_1to1",
                })
            elif 1 <= len(unknown_tol) <= 2 and 1 <= len(unknown_spa) <= 2:
                for tw in unknown_tol:
                    for sw in unknown_spa:
                        discoveries.append({
                            "tol": tw, "other": sw,
                            "lang": "spanish", "confidence": "medium",
                            "verse": vnum, "method": "elimination_few",
                        })
            elif 1 <= len(unknown_tol) <= 3 and len(unknown_spa) >= 1:
                for tw in unknown_tol:
                    for sw in unknown_spa[:3]:
                        discoveries.append({
                            "tol": tw, "other": sw,
                            "lang": "spanish", "confidence": "low",
                            "verse": vnum, "method": "elimination_partial",
                        })

        # ── Tol ↔ English ──
        if vnum in eng_verses:
            tol_tokens = [w for w in tokenize(tol_verses[vnum])
                          if w not in proper_nouns and w not in STOPWORDS_TOL and len(w) > 2]
            eng_tokens = [w for w in tokenize(eng_verses[vnum])
                          if w not in proper_nouns and w not in STOPWORDS_EN and len(w) > 2]

            unknown_tol = list(dict.fromkeys([w for w in tol_tokens if w not in existing["known_tol"]]))
            unknown_eng = list(dict.fromkeys([w for w in eng_tokens if w not in existing["known_eng"]]))

            if len(unknown_tol) == 1 and len(unknown_eng) == 1:
                discoveries.append({
                    "tol": unknown_tol[0], "other": unknown_eng[0],
                    "lang": "english", "confidence": "high",
                    "verse": vnum, "method": "elimination_1to1",
                })
            elif 1 <= len(unknown_tol) <= 2 and 1 <= len(unknown_eng) <= 2:
                for tw in unknown_tol:
                    for ew in unknown_eng:
                        discoveries.append({
                            "tol": tw, "other": ew,
                            "lang": "english", "confidence": "medium",
                            "verse": vnum, "method": "elimination_few",
                        })

    return discoveries


# ── Strategy 3: Cross-verification / Triangulation ────────────────────────

def triangulate(spa_candidates, eng_candidates):
    """
    If we find Tol word X → Spanish word S, and Tol word X → English word E,
    and S↔E are known translations, boost confidence.
    """
    boosted = []

    # Build quick lookups
    spa_by_tol = defaultdict(list)
    for c in spa_candidates:
        spa_by_tol[c["tol"]].append(c)

    eng_by_tol = defaultdict(list)
    for c in eng_candidates:
        eng_by_tol[c["tol"]].append(c)

    # Simple Spanish↔English pairs we know are translations
    known_pairs = {
        ("hijo", "son"), ("padre", "father"), ("madre", "mother"),
        ("nombre", "name"), ("llamó", "named"), ("llamarás", "name"),
        ("nacimiento", "birth"), ("nació", "born"), ("engendró", "became"),
        ("esposa", "wife"), ("marido", "husband"), ("virgen", "virgin"),
        ("pueblo", "people"), ("pecados", "sins"), ("sueño", "dream"),
        ("generaciones", "generations"), ("generación", "genealogy"),
        ("profeta", "prophet"), ("primogénito", "firstborn"),
        ("justo", "righteous"), ("libro", "book"), ("tiempo", "time"),
        ("rey", "king"), ("hermanos", "brothers"), ("ángel", "angel"),
        ("hombre", "man"), ("mujer", "woman"), ("espíritu", "spirit"),
        ("dará", "give"), ("luz", "birth"), ("salvará", "save"),
        ("concebido", "conceived"), ("desposada", "engaged"),
        ("cumplir", "fulfill"), ("dicho", "spoken"),
        ("secretamente", "secretly"), ("apareció", "appeared"),
        ("mandado", "commanded"), ("recibir", "take"),
        ("conoció", "know"), ("aconteció", "happened"),
        ("temas", "afraid"), ("miedo", "afraid"),
    }

    for tw in spa_by_tol:
        if tw not in eng_by_tol:
            continue
        for sc in spa_by_tol[tw]:
            for ec in eng_by_tol[tw]:
                pair = (sc["other"], ec["other"])
                rpair = (ec["other"], sc["other"])
                if pair in known_pairs or rpair in known_pairs:
                    boosted.append({
                        "tol": tw,
                        "spanish": sc["other"],
                        "english": ec["other"],
                        "method": "triangulated",
                        "confidence": "high",
                    })

    return boosted


# ── Strategy 4: Full NT Co-occurrence (for multi-chapter runs) ────────────

def build_cooccurrence_matrix(tol_verses, other_verses, other_stopwords, proper_nouns):
    cooccur = Counter()
    tol_freq = Counter()
    other_freq = Counter()

    for vnum in tol_verses:
        if vnum not in other_verses:
            continue
        tw_set = set(tokenize(tol_verses[vnum])) - proper_nouns - STOPWORDS_TOL
        ow_set = set(tokenize(other_verses[vnum])) - other_stopwords - proper_nouns

        for tw in tw_set:
            tol_freq[tw] += 1
        for ow in ow_set:
            other_freq[ow] += 1
        for tw in tw_set:
            for ow in ow_set:
                cooccur[(tw, ow)] += 1

    return cooccur, tol_freq, other_freq


def score_candidates_dice(cooccur, tol_freq, other_freq, min_cooccur=2):
    scores = []
    for (tw, ow), count in cooccur.items():
        if count < min_cooccur:
            continue
        tf = tol_freq[tw]
        of = other_freq[ow]
        dice = (2 * count) / (tf + of)
        freq_ratio = min(tf, of) / max(tf, of) if max(tf, of) > 0 else 0
        combined = dice * 0.7 + freq_ratio * 0.3
        scores.append({
            "tol": tw, "other": ow, "cooccur": count,
            "tol_freq": tf, "other_freq": of,
            "dice": round(dice, 4), "score": round(combined, 4),
            "method": "cooccurrence",
        })
    scores.sort(key=lambda x: x["score"], reverse=True)
    return scores


# ── Consolidation & Deduplication ─────────────────────────────────────────

def consolidate_discoveries(pattern_disc, elim_disc, cooc_scores, existing, proper_nouns):
    """Merge all discovery strategies, deduplicate, and rank."""
    # Aggregate by (tol_word, other_word, lang) with best confidence
    candidates = defaultdict(lambda: {"score": 0, "methods": set(), "confidence": "low", "count": 0})

    confidence_scores = {"high": 3, "medium": 2, "low": 1}

    for d in pattern_disc:
        key = (d["tol"], d["other"], d["lang"])
        c = candidates[key]
        c["score"] += d["jaccard"]
        c["methods"].add(d["method"])
        c["count"] += 1

    for d in elim_disc:
        key = (d["tol"], d["other"], d["lang"])
        c = candidates[key]
        c["score"] += confidence_scores.get(d["confidence"], 1)
        c["methods"].add(d["method"])
        c["count"] += 1
        if confidence_scores.get(d["confidence"], 0) > confidence_scores.get(c["confidence"], 0):
            c["confidence"] = d["confidence"]

    for d in cooc_scores:
        for lang in ["spanish", "english"]:
            key = (d["tol"], d["other"], lang)
            if key in candidates:
                candidates[key]["score"] += d["score"]
                candidates[key]["methods"].add(d["method"])

    # Filter and rank
    results = []
    for (tw, ow, lang), info in candidates.items():
        if tw in proper_nouns or ow in proper_nouns:
            continue
        if len(tw) <= 2 or len(ow) <= 2:
            continue
        results.append({
            "tol": tw,
            "other": ow,
            "lang": lang,
            "score": round(info["score"], 4),
            "confidence": info["confidence"],
            "methods": sorted(info["methods"]),
            "support": info["count"],
        })

    results.sort(key=lambda x: (x["confidence"] == "high", x["score"]), reverse=True)
    return results


# ── Parallel Sentence Extraction ──────────────────────────────────────────

def extract_parallel_sentences(tol_verses, spa_verses, eng_verses, book_code, chapter):
    sentences = []
    for vnum in sorted(tol_verses.keys()):
        entry = {
            "verse_ref": f"{book_code} {chapter}:{vnum}",
            "verse": vnum,
            "tol": tol_verses.get(vnum, ""),
            "spanish": spa_verses.get(vnum, ""),
            "english": eng_verses.get(vnum, ""),
        }
        if entry["tol"] and (entry["spanish"] or entry["english"]):
            sentences.append(entry)
    return sentences


# ── Database Operations ───────────────────────────────────────────────────

def insert_new_data(new_candidates, parallel_sents, book_code, chapter):
    conn = sqlite3.connect(str(DB_PATH))
    source = f"bible_align:{book_code}{chapter:02d}"

    dict_added = 0
    for entry in new_candidates:
        if entry["lang"] == "spanish":
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                    (entry["tol"], entry["other"], "", "bible_alignment", source)
                )
                if conn.total_changes > dict_added:
                    dict_added = conn.total_changes
            except:
                pass
        elif entry["lang"] == "english":
            cur = conn.execute(
                "SELECT id FROM dictionary WHERE tol = ? AND source = ? AND (english = '' OR english IS NULL)",
                (entry["tol"], source)
            )
            row = cur.fetchone()
            if row:
                conn.execute("UPDATE dictionary SET english = ? WHERE id = ?", (entry["other"], row[0]))

    sent_added = 0
    for s in parallel_sents:
        try:
            conn.execute(
                "INSERT INTO parallel_sentences (tol, spanish, english, source) VALUES (?, ?, ?, ?)",
                (s["tol"], s["spanish"], s["english"], source)
            )
            sent_added += 1
        except:
            pass

    conn.commit()

    total_dict = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    total_sent = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
    conn.close()

    return {
        "new_dict_entries": dict_added,
        "new_parallel_sentences": sent_added,
        "total_dictionary": total_dict,
        "total_sentences": total_sent,
    }


def get_db_stats():
    conn = sqlite3.connect(str(DB_PATH))
    d = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    s = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
    conn.close()
    return {"dictionary": d, "sentences": s}


# ── Main Alignment Pipeline ──────────────────────────────────────────────

def align_chapter(book_code: str, chapter: int, verbose=True):
    ch_str = f"{chapter:02d}"
    tol_file = TOL_NT / f"{book_code}{ch_str}.htm"
    eng_file = ENG_NT / f"{book_code}{ch_str}.htm"
    spa_file = SPA_NT / f"{book_code}{ch_str}.htm"

    for f, label in [(tol_file, "Tol"), (eng_file, "English"), (spa_file, "Spanish")]:
        if not f.exists():
            if verbose:
                print(f"  SKIP: {label} file not found: {f.name}")
            return None

    if verbose:
        print(f"\n{'='*70}")
        print(f"  ALIGNING {book_code} Chapter {chapter}")
        print(f"{'='*70}")

    tol_v = parse_verses(tol_file)
    eng_v = parse_verses(eng_file)
    spa_v = parse_verses(spa_file)

    if verbose:
        common = set(tol_v) & set(eng_v) & set(spa_v)
        print(f"  Verses: Tol={len(tol_v)}, Eng={len(eng_v)}, Spa={len(spa_v)}, All3={len(common)}")

    existing = load_existing_dictionary()
    if verbose:
        print(f"  Known Tol words: {len(existing['known_tol'])}, Spa: {len(existing['known_spa'])}, Eng: {len(existing['known_eng'])}")

    proper_nouns = find_proper_nouns(tol_v, spa_v, eng_v)
    if verbose:
        print(f"  Proper nouns excluded: {len(proper_nouns)}")

    # Strategy 1: Pattern-based
    pattern_disc = extract_patterns(tol_v, spa_v, eng_v, proper_nouns)
    if verbose:
        print(f"\n  Strategy 1 (Pattern): {len(pattern_disc)} candidates")

    # Strategy 2: Known-word elimination
    elim_disc = known_word_elimination(tol_v, spa_v, eng_v, existing, proper_nouns)
    if verbose:
        high = sum(1 for d in elim_disc if d["confidence"] == "high")
        med = sum(1 for d in elim_disc if d["confidence"] == "medium")
        low = sum(1 for d in elim_disc if d["confidence"] == "low")
        print(f"  Strategy 2 (Elimination): {len(elim_disc)} candidates (high={high}, med={med}, low={low})")

    # Strategy 3: Co-occurrence (uses Dice but with min_cooccur=1 for single chapter)
    cooc_spa, tf_spa, of_spa = build_cooccurrence_matrix(tol_v, spa_v, STOPWORDS_ES, proper_nouns)
    cooc_eng, tf_eng, of_eng = build_cooccurrence_matrix(tol_v, eng_v, STOPWORDS_EN, proper_nouns)
    spa_cooc = score_candidates_dice(cooc_spa, tf_spa, of_spa, min_cooccur=2)
    eng_cooc = score_candidates_dice(cooc_eng, tf_eng, of_eng, min_cooccur=2)
    if verbose:
        print(f"  Strategy 3 (Co-occurrence): Spa={len(spa_cooc)}, Eng={len(eng_cooc)} candidates")

    # Consolidate all strategies
    all_candidates = consolidate_discoveries(
        pattern_disc, elim_disc, spa_cooc + eng_cooc, existing, proper_nouns
    )

    # Strategy 4: Triangulation boost
    spa_cands = [c for c in all_candidates if c["lang"] == "spanish"]
    eng_cands = [c for c in all_candidates if c["lang"] == "english"]
    triangulated = triangulate(spa_cands, eng_cands)
    if verbose:
        print(f"  Strategy 4 (Triangulation): {len(triangulated)} cross-verified")

    # Extract parallel sentences
    parallel = extract_parallel_sentences(tol_v, spa_v, eng_v, book_code, chapter)

    if verbose:
        print(f"\n  ── CONSOLIDATED NEW TRANSLATIONS ──")
        spa_new = [c for c in all_candidates if c["lang"] == "spanish"]
        eng_new = [c for c in all_candidates if c["lang"] == "english"]

        print(f"\n  Tol → Spanish ({len(spa_new)} candidates):")
        for i, c in enumerate(spa_new[:40]):
            methods = "+".join(c["methods"])
            print(f"    {i+1:3d}. {c['tol']:22s} → {c['other']:22s}  "
                  f"conf={c['confidence']:6s}  score={c['score']:.3f}  "
                  f"support={c['support']}  [{methods}]")

        print(f"\n  Tol → English ({len(eng_new)} candidates):")
        for i, c in enumerate(eng_new[:40]):
            methods = "+".join(c["methods"])
            print(f"    {i+1:3d}. {c['tol']:22s} → {c['other']:22s}  "
                  f"conf={c['confidence']:6s}  score={c['score']:.3f}  "
                  f"support={c['support']}  [{methods}]")

        if triangulated:
            print(f"\n  ── TRIANGULATED (Tol→Spanish→English verified): ──")
            for t in triangulated:
                print(f"    {t['tol']:22s} → spa:{t['spanish']:18s} eng:{t['english']:18s}")

        print(f"\n  Parallel sentence triples: {len(parallel)}")

        # Show a few interesting verses
        print(f"\n  ── SAMPLE ALIGNMENTS ──")
        for vnum in sorted(tol_v.keys()):
            if vnum < 17:
                continue
            if vnum > 22:
                break
            print(f"\n  V{vnum}:")
            print(f"    TOL: {tol_v.get(vnum, '')[:120]}...")
            print(f"    SPA: {spa_v.get(vnum, '')[:120]}...")
            print(f"    ENG: {eng_v.get(vnum, '')[:120]}...")

    return {
        "book": book_code,
        "chapter": chapter,
        "candidates": all_candidates,
        "triangulated": triangulated,
        "parallel_sentences": parallel,
        "proper_nouns": proper_nouns,
    }


# ── Multi-chapter alignment ──────────────────────────────────────────────

def align_book(book_code: str, chapters: list[int] = None, verbose=True, commit=False):
    """Align an entire book or specific chapters."""
    if chapters is None:
        # Auto-detect available chapters
        chapters = []
        for i in range(1, 150):
            f = TOL_NT / f"{book_code}{i:02d}.htm"
            if f.exists():
                chapters.append(i)

    if verbose:
        print(f"\n{'#'*70}")
        print(f"  BOOK: {book_code} — {len(chapters)} chapters to align")
        print(f"{'#'*70}")

    all_candidates = []
    all_parallel = []
    all_triangulated = []

    for ch in chapters:
        result = align_chapter(book_code, ch, verbose=verbose)
        if result:
            all_candidates.extend(result["candidates"])
            all_parallel.extend(result["parallel_sentences"])
            all_triangulated.extend(result["triangulated"])

    if verbose:
        spa_total = sum(1 for c in all_candidates if c["lang"] == "spanish")
        eng_total = sum(1 for c in all_candidates if c["lang"] == "english")
        print(f"\n{'#'*70}")
        print(f"  BOOK TOTALS: {book_code}")
        print(f"{'#'*70}")
        print(f"  Tol→Spanish candidates: {spa_total}")
        print(f"  Tol→English candidates: {eng_total}")
        print(f"  Triangulated pairs: {len(all_triangulated)}")
        print(f"  Parallel sentences: {len(all_parallel)}")

    if commit:
        stats_before = get_db_stats()
        for ch in chapters:
            ch_cands = [c for c in all_candidates if True]  # all
            ch_parallel = [s for s in all_parallel if s["verse_ref"].startswith(f"{book_code} {ch}:")]
            insert_new_data(
                [c for c in all_candidates],
                ch_parallel, book_code, ch
            )
        stats_after = get_db_stats()
        print(f"\n  DATABASE: dict {stats_before['dictionary']} → {stats_after['dictionary']}")
        print(f"  DATABASE: sent {stats_before['sentences']} → {stats_after['sentences']}")

    return all_candidates, all_parallel, all_triangulated


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("book", nargs="?", default="MAT")
    parser.add_argument("chapter", nargs="?", type=int, default=None)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--all-chapters", action="store_true")
    args = parser.parse_args()

    if args.chapter is not None:
        result = align_chapter(args.book, args.chapter, verbose=True)
        if result:
            spa_n = sum(1 for c in result["candidates"] if c["lang"] == "spanish")
            eng_n = sum(1 for c in result["candidates"] if c["lang"] == "english")
            print(f"\n{'='*70}")
            print(f"  SUMMARY: {args.book} ch.{args.chapter}")
            print(f"{'='*70}")
            print(f"  New Tol→Spanish: {spa_n}")
            print(f"  New Tol→English: {eng_n}")
            print(f"  Triangulated: {len(result['triangulated'])}")
            print(f"  Parallel sentences: {len(result['parallel_sentences'])}")

            if args.commit:
                stats = insert_new_data(
                    result["candidates"],
                    result["parallel_sentences"],
                    args.book, args.chapter
                )
                print(f"\n  DB UPDATED: +{stats['new_dict_entries']} dict, "
                      f"+{stats['new_parallel_sentences']} sentences")
                print(f"  TOTALS: {stats['total_dictionary']} dict, {stats['total_sentences']} sentences")
    elif args.all_chapters:
        align_book(args.book, verbose=True, commit=args.commit)
    else:
        result = align_chapter(args.book, 1, verbose=True)
