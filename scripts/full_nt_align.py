#!/usr/bin/env python3
"""
Full New Testament Alignment Engine
====================================
Processes all 27 books (~290 chapters, ~7900 verses) simultaneously.

Two-pass approach:
  Pass 1: Parse all verses, build GLOBAL co-occurrence matrix across entire NT
  Pass 2: Score using global statistics + per-verse elimination + triangulation
  
This is far more powerful than per-chapter alignment because statistical
patterns emerge from thousands of verses that are invisible in 25.
"""

import re
import sqlite3
import sys
import time
import json
from pathlib import Path
from collections import Counter, defaultdict
from html import unescape
from difflib import SequenceMatcher

BASE = Path(__file__).resolve().parent.parent
TOL_NT = BASE / "Tol Translation" / "NT_Text"
ENG_NT = BASE / "Tol Translation" / "English_NT"
SPA_NT = BASE / "Tol Translation" / "Spanish_NT"
DB_PATH = BASE / "app" / "data" / "tol.db"

NT_BOOKS = [
    "MAT", "MRK", "LUK", "JHN", "ACT", "ROM",
    "1CO", "2CO", "GAL", "EPH", "PHP", "COL",
    "1TH", "2TH", "1TI", "2TI", "TIT", "PHM",
    "HEB", "JAS", "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV",
]

STOPWORDS_ES = {
    "a", "al", "ante", "bajo", "con", "contra", "de", "del", "desde",
    "durante", "e", "el", "ella", "ellas", "ellos", "en", "entre", "era",
    "es", "esa", "esas", "ese", "eso", "esos", "esta", "estas", "este",
    "esto", "estos", "fue", "ha", "hacia", "hasta", "la", "las", "le",
    "les", "lo", "los", "me", "mi", "muy", "más", "ni", "no", "nos",
    "o", "para", "pero", "por", "que", "quien", "se", "si", "sin", "sobre",
    "son", "su", "sus", "también", "te", "ti", "tu", "tus", "un", "una",
    "unas", "uno", "unos", "y", "ya", "yo", "él", "como", "aquí",
    "había", "ser", "cuando", "así", "aquel", "estas", "dijo", "dice",
    "pues", "porque", "todo", "todos", "toda", "todas", "otro", "otra",
    "otros", "han", "hizo", "hecho", "hay", "siendo", "cada",
    "eso", "está", "están", "tenía", "tiene", "tienen",
}

STOPWORDS_EN = {
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but",
    "by", "came", "come", "did", "do", "does", "done", "down", "each",
    "even", "every", "for", "from", "get", "go", "going", "gone", "got",
    "had", "has", "have", "he", "her", "here", "him", "his", "how",
    "i", "if", "in", "into", "is", "it", "its", "just", "let", "like",
    "made", "make", "many", "may", "me", "more", "most", "much", "must",
    "my", "no", "nor", "not", "now", "of", "on", "one", "only", "or",
    "other", "our", "out", "over", "own", "put", "said", "same", "saw",
    "say", "says", "see", "set", "she", "so", "some", "still", "such",
    "take", "than", "that", "the", "their", "them", "then", "there",
    "therefore", "these", "they", "this", "those", "through", "to",
    "told", "too", "two", "under", "until", "up", "upon", "us", "very",
    "was", "way", "we", "well", "went", "were", "what", "when", "where",
    "which", "while", "who", "whom", "why", "will", "with", "without",
    "would", "you", "your", "also", "yet", "again", "after", "before",
    "shall", "all", "about", "away", "back", "because", "both", "bring",
    "brought", "called", "can", "could", "day", "days",
}

STOPWORDS_TOL = {
    "la", "na", "ne", "nin", "ca", "wa", "way", "ya", "ma", "mpes",
    "p'in", "nt'a", "lal", "jis",
}

# ── HTML Parsing ──────────────────────────────────────────────────────────

def parse_verses(html_path: Path) -> dict[int, str]:
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


# ── Database ──────────────────────────────────────────────────────────────

def load_existing_dictionary():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    tol_to_spa = defaultdict(set)
    spa_to_tol = defaultdict(set)
    tol_to_eng = defaultdict(set)
    eng_to_tol = defaultdict(set)
    known_tol = set()
    known_spa = set()
    known_eng = set()

    for row in conn.execute("SELECT tol, spanish, english FROM dictionary"):
        t_raw, s_raw = row["tol"].lower().strip(), row["spanish"].lower().strip()
        e_raw = (row["english"] or "").lower().strip()
        for tw in tokenize(t_raw):
            known_tol.add(tw)
            tol_to_spa[tw].add(s_raw)
        for sw in tokenize(s_raw):
            known_spa.add(sw)
            spa_to_tol[sw].add(t_raw)
        if e_raw:
            for ew in tokenize(e_raw):
                known_eng.add(ew)
                eng_to_tol[ew].add(t_raw)
            for tw in tokenize(t_raw):
                tol_to_eng[tw].add(e_raw)

    conn.close()
    return {
        "tol_to_spa": dict(tol_to_spa),
        "spa_to_tol": dict(spa_to_tol),
        "tol_to_eng": dict(tol_to_eng),
        "eng_to_tol": dict(eng_to_tol),
        "known_tol": known_tol,
        "known_spa": known_spa,
        "known_eng": known_eng,
    }


# ── Pass 1: Parse entire NT ──────────────────────────────────────────────

def parse_full_nt():
    """Parse all three NTs into unified verse stores keyed by (book, chapter, verse)."""
    tol_all = {}
    eng_all = {}
    spa_all = {}
    stats = {"tol_chapters": 0, "eng_chapters": 0, "spa_chapters": 0}

    for book in NT_BOOKS:
        for ch in range(0, 200):
            ch_str = f"{ch:02d}"
            tol_f = TOL_NT / f"{book}{ch_str}.htm"
            eng_f = ENG_NT / f"{book}{ch_str}.htm"
            spa_f = SPA_NT / f"{book}{ch_str}.htm"

            if not tol_f.exists():
                continue

            tol_v = parse_verses(tol_f)
            if not tol_v:
                continue
            stats["tol_chapters"] += 1

            for vnum, text in tol_v.items():
                tol_all[(book, ch, vnum)] = text

            if eng_f.exists():
                eng_v = parse_verses(eng_f)
                if eng_v:
                    stats["eng_chapters"] += 1
                for vnum, text in eng_v.items():
                    eng_all[(book, ch, vnum)] = text

            if spa_f.exists():
                spa_v = parse_verses(spa_f)
                if spa_v:
                    stats["spa_chapters"] += 1
                for vnum, text in spa_v.items():
                    spa_all[(book, ch, vnum)] = text

    return tol_all, eng_all, spa_all, stats


# ── Proper noun detection (global) ───────────────────────────────────────

def detect_proper_nouns_global(tol_all, spa_all, eng_all):
    proper = set()
    # Collect capitalized words from each language
    tol_caps = Counter()
    spa_caps = Counter()
    eng_caps = Counter()

    for text in tol_all.values():
        for w in re.findall(r"[A-ZÁÉÍÓÚÜ][a-záéíóúüñ']{2,}", text):
            tol_caps[w.lower()] += 1
    for text in spa_all.values():
        for w in re.findall(r"[A-ZÁÉÍÓÚÜ][a-záéíóúüñ']{2,}", text):
            spa_caps[w.lower()] += 1
    for text in eng_all.values():
        for w in re.findall(r"[A-Z][a-z']{2,}", text):
            eng_caps[w.lower()] += 1

    # Words appearing capitalized in 2+ languages with similar spelling
    all_caps = set(tol_caps) | set(spa_caps) | set(eng_caps)
    for w in all_caps:
        langs = 0
        if w in tol_caps:
            langs += 1
        if w in spa_caps:
            langs += 1
        if w in eng_caps:
            langs += 1
        if langs >= 2:
            proper.add(w)

    # Also check for near-matches between languages
    for tw in list(tol_caps.keys())[:500]:
        for sw in list(spa_caps.keys())[:500]:
            if SequenceMatcher(None, tw, sw).ratio() >= 0.75:
                proper.add(tw)
                proper.add(sw)

    # Add known biblical terms
    proper |= {
        "dios", "jesús", "jesucristo", "cristo", "maría", "josé",
        "god", "jesus", "christ", "mary", "joseph", "lord", "señor",
        "espíritu", "santo", "spirit", "holy", "angel", "ángel",
        "moisés", "moses", "israel", "jerusalem", "jerusalén",
        "pablo", "paul", "pedro", "peter", "simon", "simón",
        "juan", "john", "santiago", "james", "judas",
        "yomen", "cjües", "emanuel", "immanuel",
    }
    return proper


# ── Pass 2: Global co-occurrence ──────────────────────────────────────────

def global_cooccurrence(tol_all, other_all, stopwords_other, proper_nouns):
    """Build co-occurrence across the entire NT at once."""
    cooccur = Counter()
    tol_freq = Counter()
    other_freq = Counter()
    verse_count = 0

    keys = set(tol_all.keys()) & set(other_all.keys())
    for key in keys:
        tol_tokens = set(tokenize(tol_all[key])) - proper_nouns - STOPWORDS_TOL
        other_tokens = set(tokenize(other_all[key])) - stopwords_other - proper_nouns

        # Only keep content words (len > 2)
        tol_tokens = {w for w in tol_tokens if len(w) > 2}
        other_tokens = {w for w in other_tokens if len(w) > 2}

        for tw in tol_tokens:
            tol_freq[tw] += 1
        for ow in other_tokens:
            other_freq[ow] += 1
        for tw in tol_tokens:
            for ow in other_tokens:
                cooccur[(tw, ow)] += 1
        verse_count += 1

    return cooccur, tol_freq, other_freq, verse_count


def score_global(cooccur, tol_freq, other_freq, total_verses, min_cooccur=3):
    """
    Score using multiple metrics:
    - Dice coefficient
    - Pointwise Mutual Information (PMI)
    - Frequency-adjusted score
    """
    scores = []
    for (tw, ow), count in cooccur.items():
        if count < min_cooccur:
            continue

        tf = tol_freq[tw]
        of = other_freq[ow]

        # Dice
        dice = (2 * count) / (tf + of)

        # PMI: log(P(tw,ow) / (P(tw) * P(ow)))
        import math
        p_joint = count / total_verses
        p_tw = tf / total_verses
        p_ow = of / total_verses
        pmi = math.log2(p_joint / (p_tw * p_ow)) if p_tw > 0 and p_ow > 0 else 0

        # Frequency ratio
        freq_ratio = min(tf, of) / max(tf, of) if max(tf, of) > 0 else 0

        # Combined score: weight Dice (translation signal) + PMI (association) + freq_ratio
        combined = dice * 0.4 + max(0, pmi / 10) * 0.4 + freq_ratio * 0.2

        scores.append({
            "tol": tw, "other": ow, "cooccur": count,
            "tol_freq": tf, "other_freq": of,
            "dice": round(dice, 4),
            "pmi": round(pmi, 4),
            "freq_ratio": round(freq_ratio, 4),
            "score": round(combined, 4),
        })

    scores.sort(key=lambda x: x["score"], reverse=True)
    return scores


# ── Known-word elimination (global) ──────────────────────────────────────

def global_elimination(tol_all, spa_all, eng_all, existing, proper_nouns):
    """
    For EVERY verse in the NT, remove known words and find new pairs.
    With 7900 verses, we'll find many 1-to-1 elimination matches.
    """
    discoveries = []

    keys_spa = set(tol_all.keys()) & set(spa_all.keys())
    for key in keys_spa:
        tol_tokens = [w for w in tokenize(tol_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_TOL and len(w) > 2]
        spa_tokens = [w for w in tokenize(spa_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_ES and len(w) > 2]

        unknown_tol = list(dict.fromkeys([w for w in tol_tokens if w not in existing["known_tol"]]))
        unknown_spa = list(dict.fromkeys([w for w in spa_tokens if w not in existing["known_spa"]]))

        if len(unknown_tol) == 1 and len(unknown_spa) == 1:
            discoveries.append({
                "tol": unknown_tol[0], "other": unknown_spa[0],
                "lang": "spanish", "confidence": "high",
                "ref": f"{key[0]} {key[1]}:{key[2]}",
            })
        elif len(unknown_tol) == 1 and 1 < len(unknown_spa) <= 3:
            for sw in unknown_spa:
                discoveries.append({
                    "tol": unknown_tol[0], "other": sw,
                    "lang": "spanish", "confidence": "medium",
                    "ref": f"{key[0]} {key[1]}:{key[2]}",
                })
        elif len(unknown_tol) == 2 and len(unknown_spa) == 2:
            for tw in unknown_tol:
                for sw in unknown_spa:
                    discoveries.append({
                        "tol": tw, "other": sw,
                        "lang": "spanish", "confidence": "medium",
                        "ref": f"{key[0]} {key[1]}:{key[2]}",
                    })

    keys_eng = set(tol_all.keys()) & set(eng_all.keys())
    for key in keys_eng:
        tol_tokens = [w for w in tokenize(tol_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_TOL and len(w) > 2]
        eng_tokens = [w for w in tokenize(eng_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_EN and len(w) > 2]

        unknown_tol = list(dict.fromkeys([w for w in tol_tokens if w not in existing["known_tol"]]))
        unknown_eng = list(dict.fromkeys([w for w in eng_tokens if w not in existing["known_eng"]]))

        if len(unknown_tol) == 1 and len(unknown_eng) == 1:
            discoveries.append({
                "tol": unknown_tol[0], "other": unknown_eng[0],
                "lang": "english", "confidence": "high",
                "ref": f"{key[0]} {key[1]}:{key[2]}",
            })
        elif len(unknown_tol) == 1 and 1 < len(unknown_eng) <= 3:
            for ew in unknown_eng:
                discoveries.append({
                    "tol": unknown_tol[0], "other": ew,
                    "lang": "english", "confidence": "medium",
                    "ref": f"{key[0]} {key[1]}:{key[2]}",
                })

    return discoveries


# ── Consolidation ─────────────────────────────────────────────────────────

def consolidate_all(cooc_results_spa, cooc_results_eng, elim_results, existing, proper_nouns, min_score=0.15):
    """Merge co-occurrence and elimination results. Rank by confidence."""
    candidates = defaultdict(lambda: {
        "cooc_score": 0, "elim_high": 0, "elim_med": 0, "elim_total": 0,
        "cooccur": 0, "methods": set(),
    })

    for c in cooc_results_spa:
        key = (c["tol"], c["other"], "spanish")
        candidates[key]["cooc_score"] = c["score"]
        candidates[key]["cooccur"] = c["cooccur"]
        candidates[key]["methods"].add("cooccurrence")
        candidates[key]["dice"] = c["dice"]
        candidates[key]["pmi"] = c["pmi"]

    for c in cooc_results_eng:
        key = (c["tol"], c["other"], "english")
        candidates[key]["cooc_score"] = c["score"]
        candidates[key]["cooccur"] = c["cooccur"]
        candidates[key]["methods"].add("cooccurrence")
        candidates[key]["dice"] = c["dice"]
        candidates[key]["pmi"] = c["pmi"]

    for e in elim_results:
        key = (e["tol"], e["other"], e["lang"])
        candidates[key]["methods"].add("elimination")
        candidates[key]["elim_total"] += 1
        if e["confidence"] == "high":
            candidates[key]["elim_high"] += 1
        else:
            candidates[key]["elim_med"] += 1

    # Score and filter
    results = []
    for (tw, ow, lang), info in candidates.items():
        if tw in proper_nouns or ow in proper_nouns:
            continue
        if len(tw) <= 2 or len(ow) <= 2:
            continue

        # Combined scoring
        score = info["cooc_score"]
        if info["elim_high"] > 0:
            score += info["elim_high"] * 2.0
        if info["elim_med"] > 0:
            score += info["elim_med"] * 0.5

        # Determine confidence level
        if info["elim_high"] >= 2 and info["cooc_score"] > 0.1:
            confidence = "very_high"
        elif info["elim_high"] >= 1 and info["cooc_score"] > 0.05:
            confidence = "high"
        elif info["cooc_score"] > 0.25 or info["elim_high"] >= 1:
            confidence = "medium"
        elif info["cooc_score"] > min_score:
            confidence = "low"
        else:
            continue

        results.append({
            "tol": tw, "other": ow, "lang": lang,
            "score": round(score, 4),
            "confidence": confidence,
            "cooccur": info.get("cooccur", 0),
            "dice": info.get("dice", 0),
            "pmi": info.get("pmi", 0),
            "elim_high": info["elim_high"],
            "elim_total": info["elim_total"],
            "methods": sorted(info["methods"]),
        })

    results.sort(key=lambda x: (
        {"very_high": 4, "high": 3, "medium": 2, "low": 1}[x["confidence"]],
        x["score"]
    ), reverse=True)

    return results


# ── Triangulation ─────────────────────────────────────────────────────────

def triangulate_global(candidates):
    """Cross-verify: Tol→Spa + Tol→Eng where Spa↔Eng are known translations."""
    spa_by_tol = defaultdict(list)
    eng_by_tol = defaultdict(list)
    for c in candidates:
        if c["lang"] == "spanish":
            spa_by_tol[c["tol"]].append(c)
        elif c["lang"] == "english":
            eng_by_tol[c["tol"]].append(c)

    # We'll use Google Translate offline knowledge: common word pairs
    # Build a quick Spanish↔English lookup from the candidate pairs themselves
    # Plus known high-frequency pairs
    spa_eng_pairs = set()
    try:
        conn = sqlite3.connect(str(DB_PATH))
        # Check if we have any spa→eng from existing data
        rows = conn.execute(
            "SELECT DISTINCT spanish, english FROM dictionary WHERE english != '' AND english IS NOT NULL"
        ).fetchall()
        for r in rows:
            for sw in tokenize(r[0]):
                for ew in tokenize(r[1]):
                    spa_eng_pairs.add((sw, ew))
        conn.close()
    except:
        pass

    # Add common word pairs manually for broader coverage
    common = [
        ("hijo", "son"), ("padre", "father"), ("madre", "mother"),
        ("hermano", "brother"), ("hermanos", "brothers"), ("hermana", "sister"),
        ("hombre", "man"), ("mujer", "woman"), ("pueblo", "people"),
        ("nombre", "name"), ("casa", "house"), ("tierra", "land"), ("earth", "earth"),
        ("cielo", "heaven"), ("agua", "water"), ("pan", "bread"), ("vino", "wine"),
        ("vida", "life"), ("muerte", "death"), ("pecado", "sin"), ("pecados", "sins"),
        ("ley", "law"), ("verdad", "truth"), ("camino", "way"), ("luz", "light"),
        ("palabra", "word"), ("rey", "king"), ("reino", "kingdom"),
        ("espíritu", "spirit"), ("corazón", "heart"), ("mano", "hand"),
        ("ojo", "eye"), ("ojos", "eyes"), ("oído", "ear"),
        ("cuerpo", "body"), ("sangre", "blood"), ("fuego", "fire"),
        ("grande", "great"), ("bueno", "good"), ("malo", "evil"),
        ("primero", "first"), ("último", "last"), ("nuevo", "new"),
        ("viejo", "old"), ("justo", "righteous"), ("santo", "holy"),
        ("poder", "power"), ("gloria", "glory"), ("gracia", "grace"),
        ("fe", "faith"), ("esperanza", "hope"), ("amor", "love"),
        ("paz", "peace"), ("guerra", "war"), ("templo", "temple"),
        ("iglesia", "church"), ("discípulos", "disciples"),
        ("apóstol", "apostle"), ("profeta", "prophet"),
        ("nacimiento", "birth"), ("bautismo", "baptism"),
        ("mandamiento", "commandment"), ("mandamientos", "commandments"),
        ("oración", "prayer"), ("milagro", "miracle"),
        ("parábola", "parable"), ("noche", "night"), ("día", "day"),
        ("mar", "sea"), ("monte", "mountain"), ("desierto", "wilderness"),
        ("ciudad", "city"), ("puerta", "gate"), ("árbol", "tree"),
        ("fruto", "fruit"), ("semilla", "seed"), ("oveja", "sheep"),
        ("perdonar", "forgive"), ("salvar", "save"), ("creer", "believe"),
        ("conocer", "know"), ("amar", "love"), ("enviar", "send"),
        ("dar", "give"), ("recibir", "receive"), ("morir", "die"),
        ("vivir", "live"), ("hablar", "speak"), ("escribir", "write"),
        ("venir", "come"), ("ver", "see"), ("oír", "hear"),
    ]
    for s, e in common:
        spa_eng_pairs.add((s, e))

    triangulated = []
    for tw in spa_by_tol:
        if tw not in eng_by_tol:
            continue
        for sc in spa_by_tol[tw][:5]:  # top 5 per language
            for ec in eng_by_tol[tw][:5]:
                if (sc["other"], ec["other"]) in spa_eng_pairs:
                    triangulated.append({
                        "tol": tw,
                        "spanish": sc["other"],
                        "english": ec["other"],
                        "spa_score": sc["score"],
                        "eng_score": ec["score"],
                        "combined_score": sc["score"] + ec["score"],
                    })

    triangulated.sort(key=lambda x: x["combined_score"], reverse=True)
    return triangulated


# ── Database Insert ───────────────────────────────────────────────────────

def insert_results(candidates, triangulated, parallel_sentences):
    conn = sqlite3.connect(str(DB_PATH))

    before_dict = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    before_sent = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]

    # Insert dictionary entries from Spanish candidates
    dict_inserted = 0
    for c in candidates:
        if c["lang"] != "spanish":
            continue
        # very_high/high: always insert
        # medium: require cooccur >= 5 and score >= 0.35
        # low: skip
        if c["confidence"] in ("very_high", "high"):
            pass  # always insert
        elif c["confidence"] == "medium" and c["cooccur"] >= 5 and c["score"] >= 0.35:
            pass  # strong medium
        else:
            continue
        try:
            conn.execute(
                "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                (c["tol"], c["other"], "", "bible_alignment", "full_nt_align")
            )
            dict_inserted += 1
        except:
            pass

    # Insert triangulated pairs as high-quality entries with both languages
    eng_updated = 0
    for t in triangulated:
        # First try to insert as new entry
        try:
            conn.execute(
                "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                (t["tol"], t["spanish"], t["english"], "triangulated", "full_nt_align")
            )
        except:
            pass
        # Also update any existing entries missing English
        conn.execute(
            "UPDATE dictionary SET english = ? WHERE tol = ? AND spanish = ? AND (english = '' OR english IS NULL)",
            (t["english"], t["tol"], t["spanish"])
        )
        if conn.total_changes:
            eng_updated += 1

    # Also insert English-only candidates with high confidence
    for c in candidates:
        if c["lang"] == "english" and c["confidence"] in ("very_high", "high"):
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                    (c["tol"], "", c["other"], "bible_alignment_eng", "full_nt_align")
                )
            except:
                pass

    # Insert parallel sentences
    sent_inserted = 0
    for s in parallel_sentences:
        try:
            conn.execute(
                "INSERT INTO parallel_sentences (tol, spanish, english, source) VALUES (?, ?, ?, ?)",
                (s["tol"], s["spanish"], s["english"], s["source"])
            )
            sent_inserted += 1
        except:
            pass

    conn.commit()

    after_dict = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    after_sent = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
    conn.close()

    return {
        "dict_before": before_dict, "dict_after": after_dict,
        "dict_new": after_dict - before_dict,
        "sent_before": before_sent, "sent_after": after_sent,
        "sent_new": after_sent - before_sent,
        "eng_updated": eng_updated,
    }


# ── Main Pipeline ─────────────────────────────────────────────────────────

def main():
    commit = "--commit" in sys.argv
    t0 = time.time()

    print("=" * 70)
    print("  FULL NEW TESTAMENT ALIGNMENT ENGINE")
    print("=" * 70)

    # ── Pass 1: Parse ──
    print("\n[1/6] Parsing all three New Testaments...")
    tol_all, eng_all, spa_all, stats = parse_full_nt()
    print(f"  Tol chapters: {stats['tol_chapters']}")
    print(f"  English chapters: {stats['eng_chapters']}")
    print(f"  Spanish chapters: {stats['spa_chapters']}")
    print(f"  Tol verses: {len(tol_all)}")
    print(f"  English verses: {len(eng_all)}")
    print(f"  Spanish verses: {len(spa_all)}")

    triple_keys = set(tol_all.keys()) & set(eng_all.keys()) & set(spa_all.keys())
    print(f"  Triple-aligned verses (all 3 languages): {len(triple_keys)}")

    # ── Load dictionary ──
    print("\n[2/6] Loading existing dictionary...")
    existing = load_existing_dictionary()
    print(f"  Known Tol words: {len(existing['known_tol'])}")
    print(f"  Known Spanish words: {len(existing['known_spa'])}")
    print(f"  Known English words: {len(existing['known_eng'])}")

    # ── Proper nouns ──
    print("\n[3/6] Detecting proper nouns...")
    proper_nouns = detect_proper_nouns_global(tol_all, spa_all, eng_all)
    print(f"  Proper nouns excluded: {len(proper_nouns)}")

    # ── Global co-occurrence ──
    print("\n[4/6] Building global co-occurrence matrices...")
    t1 = time.time()
    cooc_spa, tf_spa, of_spa, nv_spa = global_cooccurrence(tol_all, spa_all, STOPWORDS_ES, proper_nouns)
    cooc_eng, tf_eng, of_eng, nv_eng = global_cooccurrence(tol_all, eng_all, STOPWORDS_EN, proper_nouns)
    print(f"  Tol↔Spanish: {nv_spa} verse pairs, {len(cooc_spa)} word pairs")
    print(f"  Tol↔English: {nv_eng} verse pairs, {len(cooc_eng)} word pairs")
    print(f"  Time: {time.time()-t1:.1f}s")

    print("\n  Scoring co-occurrence candidates...")
    spa_scored = score_global(cooc_spa, tf_spa, of_spa, nv_spa, min_cooccur=3)
    eng_scored = score_global(cooc_eng, tf_eng, of_eng, nv_eng, min_cooccur=3)
    print(f"  Tol→Spanish scored: {len(spa_scored)}")
    print(f"  Tol→English scored: {len(eng_scored)}")

    # ── Known-word elimination ──
    print("\n[5/6] Running known-word elimination across all verses...")
    t2 = time.time()
    elim_results = global_elimination(tol_all, spa_all, eng_all, existing, proper_nouns)
    elim_high = sum(1 for e in elim_results if e["confidence"] == "high")
    elim_med = sum(1 for e in elim_results if e["confidence"] == "medium")
    print(f"  Elimination candidates: {len(elim_results)} (high={elim_high}, medium={elim_med})")
    print(f"  Time: {time.time()-t2:.1f}s")

    # ── Consolidate ──
    print("\n[6/6] Consolidating and ranking...")
    candidates = consolidate_all(spa_scored, eng_scored, elim_results, existing, proper_nouns)
    spa_cands = [c for c in candidates if c["lang"] == "spanish"]
    eng_cands = [c for c in candidates if c["lang"] == "english"]

    vh_spa = sum(1 for c in spa_cands if c["confidence"] == "very_high")
    h_spa = sum(1 for c in spa_cands if c["confidence"] == "high")
    m_spa = sum(1 for c in spa_cands if c["confidence"] == "medium")
    l_spa = sum(1 for c in spa_cands if c["confidence"] == "low")

    vh_eng = sum(1 for c in eng_cands if c["confidence"] == "very_high")
    h_eng = sum(1 for c in eng_cands if c["confidence"] == "high")
    m_eng = sum(1 for c in eng_cands if c["confidence"] == "medium")
    l_eng = sum(1 for c in eng_cands if c["confidence"] == "low")

    print(f"\n  Tol → Spanish: {len(spa_cands)} total")
    print(f"    very_high={vh_spa}, high={h_spa}, medium={m_spa}, low={l_spa}")
    print(f"\n  Tol → English: {len(eng_cands)} total")
    print(f"    very_high={vh_eng}, high={h_eng}, medium={m_eng}, low={l_eng}")

    # Triangulation
    triangulated = triangulate_global(candidates)
    print(f"\n  Triangulated (cross-verified Spa↔Eng): {len(triangulated)}")

    # ── Display top results ──
    print(f"\n{'='*70}")
    print("  TOP Tol→Spanish DISCOVERIES (very_high + high confidence)")
    print(f"{'='*70}")
    shown = 0
    for c in spa_cands:
        if c["confidence"] not in ("very_high", "high"):
            continue
        if shown >= 60:
            break
        methods = "+".join(c["methods"])
        print(f"  {shown+1:3d}. {c['tol']:22s} → {c['other']:22s}  "
              f"conf={c['confidence']:10s}  score={c['score']:.3f}  "
              f"co={c['cooccur']:4d}  elim_h={c['elim_high']}  [{methods}]")
        shown += 1

    print(f"\n{'='*70}")
    print("  TOP Tol→English DISCOVERIES (very_high + high confidence)")
    print(f"{'='*70}")
    shown = 0
    for c in eng_cands:
        if c["confidence"] not in ("very_high", "high"):
            continue
        if shown >= 60:
            break
        methods = "+".join(c["methods"])
        print(f"  {shown+1:3d}. {c['tol']:22s} → {c['other']:22s}  "
              f"conf={c['confidence']:10s}  score={c['score']:.3f}  "
              f"co={c['cooccur']:4d}  elim_h={c['elim_high']}  [{methods}]")
        shown += 1

    if triangulated:
        print(f"\n{'='*70}")
        print("  TRIANGULATED TRANSLATIONS (top 40)")
        print(f"{'='*70}")
        for i, t in enumerate(triangulated[:40]):
            print(f"  {i+1:3d}. {t['tol']:22s} → spa:{t['spanish']:18s} eng:{t['english']:18s}  "
                  f"score={t['combined_score']:.3f}")

    # ── Parallel sentences ──
    parallel = []
    for key in sorted(triple_keys):
        book, ch, vnum = key
        parallel.append({
            "tol": tol_all[key],
            "spanish": spa_all.get(key, ""),
            "english": eng_all.get(key, ""),
            "source": f"bible_align:{book}{ch:02d}:{vnum}",
        })
    print(f"\n  Total parallel sentence triples: {len(parallel)}")

    # ── Commit ──
    elapsed = time.time() - t0
    print(f"\n  Total processing time: {elapsed:.1f}s")

    if commit:
        print(f"\n{'='*70}")
        print("  COMMITTING TO DATABASE...")
        print(f"{'='*70}")
        db_stats = insert_results(candidates, triangulated, parallel)
        print(f"  Dictionary: {db_stats['dict_before']} → {db_stats['dict_after']} (+{db_stats['dict_new']})")
        print(f"  Sentences:  {db_stats['sent_before']} → {db_stats['sent_after']} (+{db_stats['sent_new']})")
        print(f"  English translations updated: {db_stats['eng_updated']}")
    else:
        insertable_spa = sum(1 for c in spa_cands if c["confidence"] in ("very_high", "high", "medium"))
        insertable_eng = sum(1 for c in eng_cands if c["confidence"] in ("very_high", "high"))
        print(f"\n  Would insert: ~{insertable_spa} Spa dict + ~{insertable_eng} Eng dict + {len(parallel)} sentences")
        print(f"  Run with --commit to write to database.")

    print(f"\n{'='*70}")
    print("  DONE")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
