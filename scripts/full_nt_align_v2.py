#!/usr/bin/env python3
"""
Full NT Alignment Engine v2 — Synonym-Enhanced Multi-Pass
==========================================================
Uses ALL available data to maximize word discovery:
  - 2,988 Tol dictionary entries
  - 249K English↔Spanish dictionary entries
  - 336K synonym-inferred translation paths (62K English + 13K Spanish → Tol)
  - 2.5M English synonyms + 248K Spanish synonyms

Multi-pass strategy:
  Pass 1: Align with full enriched vocabulary → discover new words
  Pass 2: Re-align with Pass 1 additions → compound gains
  
The massive increase in "known words" means the elimination strategy
now works on verses that previously had too many unknowns.
"""

import re
import math
import sqlite3
import sys
import time
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
    "había", "ser", "cuando", "así", "aquel", "dijo", "dice",
    "pues", "porque", "todo", "todos", "toda", "todas", "otro", "otra",
    "otros", "han", "hizo", "hecho", "hay", "siendo", "cada",
    "está", "están", "tenía", "tiene", "tienen", "esto",
    "qué", "él", "nos", "les", "ese", "esos", "esa", "esas",
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
    "brought", "called", "can", "could", "day", "days", "himself",
    "themselves", "herself", "itself", "myself", "yourself",
}

STOPWORDS_TOL = {
    "la", "na", "ne", "nin", "ca", "wa", "way", "ya", "ma", "mpes",
    "p'in", "nt'a", "lal", "jis", "jupj", "yupj",
}


def parse_verses(html_path: Path) -> dict[int, str]:
    text = html_path.read_text(encoding="utf-8")
    text = text.replace("&#160;", " ").replace("&nbsp;", " ")
    text = unescape(text)
    text = re.sub(r'<span class="popup">.*?</span>', '', text)
    text = re.sub(r'<a[^>]*class="notemark"[^>]*>.*?</a>', '', text)
    text = re.sub(r"<span class=['\"]add['\"]>(.*?)</span>", r"\1", text)
    verses = {}
    for m in re.finditer(
        r'<span\s+class="verse"\s+id="V(\d+)">\s*\d+\s*</span>'
        r'(.*?)'
        r'(?=<span\s+class="verse"|<ul\s|<div\s+class=[\'"](?:footnote|copyright|tnav|s\b|r\b))',
        text, re.DOTALL
    ):
        vnum = int(m.group(1))
        raw = m.group(2)
        clean = re.sub(r'<[^>]+>', ' ', raw)
        clean = re.sub(r'\s+', ' ', clean).strip()
        if clean:
            verses[vnum] = clean
    return verses


def tokenize(text: str) -> list[str]:
    return [w for w in re.findall(r"[a-záéíóúüñ'']+", text.lower()) if len(w) > 1]


# ── Load ALL known vocabulary ─────────────────────────────────────────────

def load_all_known(conn):
    """Load every known word from ALL tables — dictionary, en_es, inferred, synonyms."""
    known_tol = set()
    known_spa = set()
    known_eng = set()

    # Tol dictionary
    tol_to_spa = {}
    spa_to_tol = {}
    tol_to_eng = {}
    for row in conn.execute("SELECT tol, spanish, english FROM dictionary"):
        t, s, e = row[0].lower().strip(), row[1].lower().strip(), (row[2] or "").lower().strip()
        for tw in tokenize(t):
            known_tol.add(tw)
            tol_to_spa[tw] = s
        for sw in tokenize(s):
            known_spa.add(sw)
            spa_to_tol[sw] = t
        if e:
            for ew in tokenize(e):
                known_eng.add(ew)
                tol_to_eng[tw] = e

    # En↔Es dictionary (249K)
    has = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='en_es_dictionary'").fetchone()
    if has:
        for row in conn.execute("SELECT english, spanish FROM en_es_dictionary"):
            for w in tokenize(row[0]):
                known_eng.add(w)
            for w in tokenize(row[1]):
                known_spa.add(w)

    # Inferred translations (synonym paths)
    has = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inferred_translations'").fetchone()
    if has:
        for row in conn.execute("SELECT source_word, source_lang, tol_word FROM inferred_translations"):
            w = row[0].lower().strip()
            tol_w = row[2].lower().strip()
            known_tol.add(tol_w)
            if row[1] == "en":
                known_eng.add(w)
            elif row[1] == "es":
                known_spa.add(w)

    return {
        "known_tol": known_tol,
        "known_spa": known_spa,
        "known_eng": known_eng,
        "tol_to_spa": tol_to_spa,
        "spa_to_tol": spa_to_tol,
        "tol_to_eng": tol_to_eng,
    }


def detect_proper_nouns(tol_all, spa_all, eng_all):
    proper = set()
    tol_caps, spa_caps, eng_caps = Counter(), Counter(), Counter()
    for text in tol_all.values():
        for w in re.findall(r"[A-ZÁÉÍÓÚÜ][a-záéíóúüñ']{2,}", text):
            tol_caps[w.lower()] += 1
    for text in spa_all.values():
        for w in re.findall(r"[A-ZÁÉÍÓÚÜ][a-záéíóúüñ']{2,}", text):
            spa_caps[w.lower()] += 1
    for text in eng_all.values():
        for w in re.findall(r"[A-Z][a-z']{2,}", text):
            eng_caps[w.lower()] += 1

    all_caps = set(tol_caps) | set(spa_caps) | set(eng_caps)
    for w in all_caps:
        langs = sum([w in tol_caps, w in spa_caps, w in eng_caps])
        if langs >= 2:
            proper.add(w)

    for tw in list(tol_caps.keys())[:500]:
        for sw in list(spa_caps.keys())[:500]:
            if SequenceMatcher(None, tw, sw).ratio() >= 0.75:
                proper.add(tw)
                proper.add(sw)

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


# ── Parse entire NT ──────────────────────────────────────────────────────

def parse_full_nt():
    tol_all, eng_all, spa_all = {}, {}, {}
    ch_count = 0
    for book in NT_BOOKS:
        for ch in range(0, 200):
            tol_f = TOL_NT / f"{book}{ch:02d}.htm"
            if not tol_f.exists():
                continue
            tol_v = parse_verses(tol_f)
            if not tol_v:
                continue
            ch_count += 1
            for vnum, text in tol_v.items():
                tol_all[(book, ch, vnum)] = text

            eng_f = ENG_NT / f"{book}{ch:02d}.htm"
            if eng_f.exists():
                for vnum, text in parse_verses(eng_f).items():
                    eng_all[(book, ch, vnum)] = text

            spa_f = SPA_NT / f"{book}{ch:02d}.htm"
            if spa_f.exists():
                for vnum, text in parse_verses(spa_f).items():
                    spa_all[(book, ch, vnum)] = text

    return tol_all, eng_all, spa_all, ch_count


# ── Global co-occurrence ──────────────────────────────────────────────────

def global_cooccurrence(tol_all, other_all, stopwords, proper_nouns):
    cooccur = Counter()
    tol_freq, other_freq = Counter(), Counter()
    n_verses = 0
    keys = set(tol_all.keys()) & set(other_all.keys())
    for key in keys:
        tw_set = {w for w in tokenize(tol_all[key]) if w not in proper_nouns and w not in STOPWORDS_TOL and len(w) > 2}
        ow_set = {w for w in tokenize(other_all[key]) if w not in stopwords and w not in proper_nouns and len(w) > 2}
        for tw in tw_set:
            tol_freq[tw] += 1
        for ow in ow_set:
            other_freq[ow] += 1
        for tw in tw_set:
            for ow in ow_set:
                cooccur[(tw, ow)] += 1
        n_verses += 1
    return cooccur, tol_freq, other_freq, n_verses


def score_global(cooccur, tol_freq, other_freq, total_verses, min_cooccur=3):
    scores = []
    for (tw, ow), count in cooccur.items():
        if count < min_cooccur:
            continue
        tf, of = tol_freq[tw], other_freq[ow]
        dice = (2 * count) / (tf + of)
        p_joint = count / total_verses
        p_tw = tf / total_verses
        p_ow = of / total_verses
        pmi = math.log2(p_joint / (p_tw * p_ow)) if p_tw > 0 and p_ow > 0 else 0
        freq_ratio = min(tf, of) / max(tf, of) if max(tf, of) > 0 else 0
        combined = dice * 0.4 + max(0, pmi / 10) * 0.4 + freq_ratio * 0.2
        scores.append({
            "tol": tw, "other": ow, "cooccur": count,
            "tol_freq": tf, "other_freq": of,
            "dice": round(dice, 4), "pmi": round(pmi, 4),
            "score": round(combined, 4),
        })
    scores.sort(key=lambda x: x["score"], reverse=True)
    return scores


# ── Enhanced known-word elimination ──────────────────────────────────────

def enhanced_elimination(tol_all, spa_all, eng_all, known, proper_nouns):
    """
    With 62K+ known English and 13K+ known Spanish words,
    many more verses will have only 1-2 unknowns per side.
    """
    discoveries = []

    # Tol ↔ Spanish
    for key in set(tol_all.keys()) & set(spa_all.keys()):
        tol_tokens = [w for w in tokenize(tol_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_TOL and len(w) > 2]
        spa_tokens = [w for w in tokenize(spa_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_ES and len(w) > 2]

        unknown_tol = list(dict.fromkeys([w for w in tol_tokens if w not in known["known_tol"]]))
        unknown_spa = list(dict.fromkeys([w for w in spa_tokens if w not in known["known_spa"]]))

        if len(unknown_tol) == 1 and len(unknown_spa) == 1:
            discoveries.append({
                "tol": unknown_tol[0], "other": unknown_spa[0],
                "lang": "spanish", "confidence": "very_high",
                "ref": f"{key[0]}{key[1]}:{key[2]}",
            })
        elif len(unknown_tol) == 1 and 2 <= len(unknown_spa) <= 3:
            for sw in unknown_spa:
                discoveries.append({
                    "tol": unknown_tol[0], "other": sw,
                    "lang": "spanish", "confidence": "high",
                    "ref": f"{key[0]}{key[1]}:{key[2]}",
                })
        elif len(unknown_tol) == 2 and len(unknown_spa) == 2:
            for tw in unknown_tol:
                for sw in unknown_spa:
                    discoveries.append({
                        "tol": tw, "other": sw,
                        "lang": "spanish", "confidence": "medium",
                        "ref": f"{key[0]}{key[1]}:{key[2]}",
                    })
        elif len(unknown_tol) == 1 and len(unknown_spa) == 0:
            # All Spanish known, but one Tol word is new — check if context gives a match
            pass
        elif len(unknown_tol) == 0 and len(unknown_spa) == 1:
            pass

    # Tol ↔ English
    for key in set(tol_all.keys()) & set(eng_all.keys()):
        tol_tokens = [w for w in tokenize(tol_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_TOL and len(w) > 2]
        eng_tokens = [w for w in tokenize(eng_all[key])
                      if w not in proper_nouns and w not in STOPWORDS_EN and len(w) > 2]

        unknown_tol = list(dict.fromkeys([w for w in tol_tokens if w not in known["known_tol"]]))
        unknown_eng = list(dict.fromkeys([w for w in eng_tokens if w not in known["known_eng"]]))

        if len(unknown_tol) == 1 and len(unknown_eng) == 1:
            discoveries.append({
                "tol": unknown_tol[0], "other": unknown_eng[0],
                "lang": "english", "confidence": "very_high",
                "ref": f"{key[0]}{key[1]}:{key[2]}",
            })
        elif len(unknown_tol) == 1 and 2 <= len(unknown_eng) <= 3:
            for ew in unknown_eng:
                discoveries.append({
                    "tol": unknown_tol[0], "other": ew,
                    "lang": "english", "confidence": "high",
                    "ref": f"{key[0]}{key[1]}:{key[2]}",
                })
        elif len(unknown_tol) == 2 and len(unknown_eng) == 2:
            for tw in unknown_tol:
                for ew in unknown_eng:
                    discoveries.append({
                        "tol": tw, "other": ew,
                        "lang": "english", "confidence": "medium",
                        "ref": f"{key[0]}{key[1]}:{key[2]}",
                    })

    return discoveries


# ── Consolidation ─────────────────────────────────────────────────────────

def consolidate(cooc_spa, cooc_eng, elim, known, proper_nouns, min_score=0.15):
    candidates = defaultdict(lambda: {
        "cooc_score": 0, "elim_vh": 0, "elim_h": 0, "elim_m": 0, "elim_total": 0,
        "cooccur": 0, "dice": 0, "pmi": 0, "methods": set(),
    })

    for c in cooc_spa:
        key = (c["tol"], c["other"], "spanish")
        candidates[key]["cooc_score"] = c["score"]
        candidates[key]["cooccur"] = c["cooccur"]
        candidates[key]["dice"] = c["dice"]
        candidates[key]["pmi"] = c["pmi"]
        candidates[key]["methods"].add("cooccurrence")

    for c in cooc_eng:
        key = (c["tol"], c["other"], "english")
        candidates[key]["cooc_score"] = c["score"]
        candidates[key]["cooccur"] = c["cooccur"]
        candidates[key]["dice"] = c["dice"]
        candidates[key]["pmi"] = c["pmi"]
        candidates[key]["methods"].add("cooccurrence")

    conf_map = {"very_high": 3, "high": 2, "medium": 1}
    for e in elim:
        key = (e["tol"], e["other"], e["lang"])
        candidates[key]["methods"].add("elimination")
        candidates[key]["elim_total"] += 1
        if e["confidence"] == "very_high":
            candidates[key]["elim_vh"] += 1
        elif e["confidence"] == "high":
            candidates[key]["elim_h"] += 1
        else:
            candidates[key]["elim_m"] += 1

    results = []
    for (tw, ow, lang), info in candidates.items():
        if tw in proper_nouns or ow in proper_nouns:
            continue
        if len(tw) <= 2 or len(ow) <= 2:
            continue

        score = info["cooc_score"]
        score += info["elim_vh"] * 3.0
        score += info["elim_h"] * 1.5
        score += info["elim_m"] * 0.5

        if info["elim_vh"] >= 2 and info["cooc_score"] > 0.05:
            confidence = "very_high"
        elif info["elim_vh"] >= 1:
            confidence = "high"
        elif info["elim_h"] >= 2 and info["cooc_score"] > 0.1:
            confidence = "high"
        elif info["cooc_score"] > 0.25 or info["elim_h"] >= 1:
            confidence = "medium"
        elif info["cooc_score"] > min_score:
            confidence = "low"
        else:
            continue

        results.append({
            "tol": tw, "other": ow, "lang": lang,
            "score": round(score, 4), "confidence": confidence,
            "cooccur": info["cooccur"], "dice": info["dice"], "pmi": info["pmi"],
            "elim_vh": info["elim_vh"], "elim_h": info["elim_h"],
            "elim_total": info["elim_total"],
            "methods": sorted(info["methods"]),
        })

    results.sort(key=lambda x: (
        {"very_high": 4, "high": 3, "medium": 2, "low": 1}[x["confidence"]],
        x["score"]
    ), reverse=True)
    return results


# ── Triangulation ─────────────────────────────────────────────────────────

def triangulate(candidates, conn):
    spa_by_tol = defaultdict(list)
    eng_by_tol = defaultdict(list)
    for c in candidates:
        if c["lang"] == "spanish":
            spa_by_tol[c["tol"]].append(c)
        else:
            eng_by_tol[c["tol"]].append(c)

    spa_eng_pairs = set()
    has = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='en_es_dictionary'").fetchone()
    if has:
        for row in conn.execute("SELECT english, spanish FROM en_es_dictionary"):
            for sw in tokenize(row[1]):
                for ew in tokenize(row[0]):
                    spa_eng_pairs.add((sw, ew))

    # Also add synonym links: if eng1 is synonym of eng2, and spa→eng2 known, then (spa, eng1) too
    has_syn = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='synonyms_en'").fetchone()
    if has_syn:
        for (sw, ew) in list(spa_eng_pairs)[:5000]:
            syns = conn.execute("SELECT synonym FROM synonyms_en WHERE word = ? LIMIT 10", (ew,)).fetchall()
            for s in syns:
                spa_eng_pairs.add((sw, s[0]))

    result = []
    for tw in spa_by_tol:
        if tw not in eng_by_tol:
            continue
        for sc in spa_by_tol[tw][:5]:
            for ec in eng_by_tol[tw][:5]:
                if (sc["other"], ec["other"]) in spa_eng_pairs:
                    result.append({
                        "tol": tw, "spanish": sc["other"], "english": ec["other"],
                        "score": sc["score"] + ec["score"],
                    })
    result.sort(key=lambda x: x["score"], reverse=True)
    return result


# ── Database insert ───────────────────────────────────────────────────────

def insert_results(candidates, triangulated, parallel, conn, pass_num):
    source_tag = f"nt_align_v2_p{pass_num}"
    before_d = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    before_s = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]

    # Dictionary: Spanish candidates (very_high, high, medium with strong cooccurrence)
    for c in candidates:
        if c["lang"] != "spanish":
            continue
        if c["confidence"] in ("very_high", "high"):
            pass
        elif c["confidence"] == "medium" and c["cooccur"] >= 5 and c["score"] >= 0.35:
            pass
        else:
            continue
        try:
            conn.execute(
                "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                (c["tol"], c["other"], "", "bible_alignment_v2", source_tag)
            )
        except:
            pass

    # Triangulated entries (gold quality with both languages)
    for t in triangulated:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                (t["tol"], t["spanish"], t["english"], "triangulated_v2", source_tag)
            )
        except:
            pass
        conn.execute(
            "UPDATE dictionary SET english = ? WHERE tol = ? AND spanish = ? AND (english = '' OR english IS NULL)",
            (t["english"], t["tol"], t["spanish"])
        )

    # English high-confidence
    for c in candidates:
        if c["lang"] == "english" and c["confidence"] in ("very_high", "high"):
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                    (c["tol"], "", c["other"], "bible_alignment_v2_eng", source_tag)
                )
            except:
                pass

    # Parallel sentences (skip if already exists from v1)
    existing = conn.execute("SELECT COUNT(*) FROM parallel_sentences WHERE source LIKE 'bible_align:%'").fetchone()[0]
    sent_added = 0
    if existing < len(parallel):
        # Clear old and re-insert
        conn.execute("DELETE FROM parallel_sentences WHERE source LIKE 'bible_align:%'")
        for s in parallel:
            try:
                conn.execute(
                    "INSERT INTO parallel_sentences (tol, spanish, english, source) VALUES (?, ?, ?, ?)",
                    (s["tol"], s["spanish"], s["english"], s["source"])
                )
                sent_added += 1
            except:
                pass

    conn.commit()
    after_d = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    after_s = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
    return {
        "dict_before": before_d, "dict_after": after_d, "dict_new": after_d - before_d,
        "sent_before": before_s, "sent_after": after_s, "sent_new": sent_added,
    }


# ── Main ──────────────────────────────────────────────────────────────────

def run_pass(pass_num, tol_all, eng_all, spa_all, proper_nouns, conn, commit=False):
    print(f"\n{'#'*70}")
    print(f"  PASS {pass_num}")
    print(f"{'#'*70}")

    known = load_all_known(conn)
    print(f"  Known vocabulary: Tol={len(known['known_tol']):,}, Spa={len(known['known_spa']):,}, Eng={len(known['known_eng']):,}")

    # Co-occurrence
    t1 = time.time()
    cooc_spa, tf_s, of_s, nv_s = global_cooccurrence(tol_all, spa_all, STOPWORDS_ES, proper_nouns)
    cooc_eng, tf_e, of_e, nv_e = global_cooccurrence(tol_all, eng_all, STOPWORDS_EN, proper_nouns)
    spa_scored = score_global(cooc_spa, tf_s, of_s, nv_s, min_cooccur=3)
    eng_scored = score_global(cooc_eng, tf_e, of_e, nv_e, min_cooccur=3)
    print(f"  Co-occurrence: {len(spa_scored):,} Spa + {len(eng_scored):,} Eng scored ({time.time()-t1:.1f}s)")

    # Enhanced elimination
    t2 = time.time()
    elim = enhanced_elimination(tol_all, spa_all, eng_all, known, proper_nouns)
    vh = sum(1 for e in elim if e["confidence"] == "very_high")
    h = sum(1 for e in elim if e["confidence"] == "high")
    m = sum(1 for e in elim if e["confidence"] == "medium")
    print(f"  Elimination: {len(elim):,} candidates (very_high={vh}, high={h}, medium={m}) ({time.time()-t2:.1f}s)")

    # Consolidate
    all_cands = consolidate(spa_scored, eng_scored, elim, known, proper_nouns)
    spa_c = [c for c in all_cands if c["lang"] == "spanish"]
    eng_c = [c for c in all_cands if c["lang"] == "english"]

    print(f"  Consolidated: {len(spa_c):,} Tol→Spa, {len(eng_c):,} Tol→Eng")
    for conf in ["very_high", "high", "medium", "low"]:
        ns = sum(1 for c in spa_c if c["confidence"] == conf)
        ne = sum(1 for c in eng_c if c["confidence"] == conf)
        print(f"    {conf:10s}: Spa={ns:,}, Eng={ne:,}")

    # Triangulate
    tri = triangulate(all_cands, conn)
    print(f"  Triangulated: {len(tri):,}")

    # Parallel sentences
    triple_keys = set(tol_all.keys()) & set(eng_all.keys()) & set(spa_all.keys())
    parallel = []
    for key in sorted(triple_keys):
        parallel.append({
            "tol": tol_all[key], "spanish": spa_all[key], "english": eng_all[key],
            "source": f"bible_align:{key[0]}{key[1]:02d}:{key[2]}",
        })
    print(f"  Parallel sentences: {len(parallel):,}")

    # Top discoveries
    print(f"\n  TOP Tol→Spanish (very_high + high):")
    shown = 0
    for c in spa_c:
        if c["confidence"] not in ("very_high", "high"):
            continue
        if shown >= 30:
            break
        print(f"    {c['tol']:22s} → {c['other']:22s}  conf={c['confidence']:10s}  score={c['score']:.2f}  co={c['cooccur']}  ev={c['elim_vh']}+{c['elim_h']}")
        shown += 1

    print(f"\n  TOP Tol→English (very_high + high):")
    shown = 0
    for c in eng_c:
        if c["confidence"] not in ("very_high", "high"):
            continue
        if shown >= 30:
            break
        print(f"    {c['tol']:22s} → {c['other']:22s}  conf={c['confidence']:10s}  score={c['score']:.2f}  co={c['cooccur']}  ev={c['elim_vh']}+{c['elim_h']}")
        shown += 1

    if tri:
        print(f"\n  TOP Triangulated (20):")
        for t in tri[:20]:
            print(f"    {t['tol']:22s} → spa:{t['spanish']:18s} eng:{t['english']}")

    if commit:
        print(f"\n  Committing pass {pass_num}...")
        stats = insert_results(all_cands, tri, parallel, conn, pass_num)
        print(f"  Dictionary: {stats['dict_before']:,} → {stats['dict_after']:,} (+{stats['dict_new']:,})")
        print(f"  Sentences:  {stats['sent_before']:,} → {stats['sent_after']:,} (+{stats['sent_new']:,})")

    return all_cands, tri, parallel


def main():
    commit = "--commit" in sys.argv
    t0 = time.time()

    print("=" * 70)
    print("  FULL NT ALIGNMENT v2 — SYNONYM-ENHANCED MULTI-PASS")
    print("=" * 70)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Parse
    print("\n[1] Parsing all three NTs...")
    tol_all, eng_all, spa_all, ch_count = parse_full_nt()
    triple = set(tol_all.keys()) & set(eng_all.keys()) & set(spa_all.keys())
    print(f"  {ch_count} chapters, {len(tol_all):,} Tol, {len(eng_all):,} Eng, {len(spa_all):,} Spa verses")
    print(f"  Triple-aligned: {len(triple):,}")

    # Proper nouns
    print("\n[2] Detecting proper nouns...")
    proper_nouns = detect_proper_nouns(tol_all, spa_all, eng_all)
    print(f"  Excluded: {len(proper_nouns):,}")

    # Pass 1
    print("\n[3] Running Pass 1...")
    c1, t1, p1 = run_pass(1, tol_all, eng_all, spa_all, proper_nouns, conn, commit=commit)

    # Pass 2 (uses Pass 1 discoveries)
    if commit:
        print("\n[4] Running Pass 2 (using Pass 1 discoveries)...")
        c2, t2, p2 = run_pass(2, tol_all, eng_all, spa_all, proper_nouns, conn, commit=commit)
    else:
        print("\n[4] Skipping Pass 2 (run with --commit for multi-pass)")

    # Final report
    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  COMPLETE — {elapsed:.1f}s")
    print(f"{'='*70}")

    d = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    s = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
    print(f"  Final dictionary: {d:,}")
    print(f"  Final sentences:  {s:,}")

    if commit:
        # Backfill English using en_es dictionary
        print("\n  Backfilling English into new Tol entries...")
        updated = 0
        rows = conn.execute(
            "SELECT id, spanish FROM dictionary WHERE (english IS NULL OR english = '') AND spanish != ''"
        ).fetchall()
        for rid, spa in rows:
            spa_lower = spa.lower().strip()
            eng_row = conn.execute(
                "SELECT english FROM en_es_dictionary WHERE spanish = ? LIMIT 1", (spa_lower,)
            ).fetchone()
            if eng_row:
                conn.execute("UPDATE dictionary SET english = ? WHERE id = ?", (eng_row[0], rid))
                updated += 1
        conn.commit()
        print(f"  Backfilled English into {updated:,} entries")

        with_eng = conn.execute("SELECT COUNT(*) FROM dictionary WHERE english != '' AND english IS NOT NULL").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
        print(f"  Tol entries with English: {with_eng:,} / {total:,} ({100*with_eng/total:.0f}%)")

    conn.close()

if __name__ == "__main__":
    main()
