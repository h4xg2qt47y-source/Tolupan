"""
Tol Translation API Server
FastAPI backend serving the translation engine and static frontend.
"""

import re
import os
import json
import smtplib
import ssl
import uuid
from datetime import datetime, timezone
from email.message import EmailMessage
import sqlite3
import logging
from typing import Optional, Tuple
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
from pathlib import Path

from translator import TolTranslator, tol_phonetic_score
import tts_engine

logger = logging.getLogger(__name__)

app = FastAPI(title="Tol Translation Engine", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

translator = TolTranslator()

STATIC_DIR = Path(__file__).parent / "static"
DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "tol.db"
FEEDBACK_INBOX = DATA_DIR / "feedback_inbox.jsonl"
FEEDBACK_EMAIL_DEFAULT = "tooling_village_30@icloud.com"


def _db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


class TranslateRequest(BaseModel):
    text: str
    source_lang: str
    target_lang: str


class DictionaryRequest(BaseModel):
    query: str
    lang: str = "tol"


@app.get("/")
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/dictionary")
async def dictionary_page():
    return FileResponse(str(STATIC_DIR / "dictionary.html"))


@app.get("/feedback")
async def feedback_page():
    return FileResponse(str(STATIC_DIR / "feedback.html"))


@app.post("/api/translate")
async def translate(req: TranslateRequest):
    if req.source_lang not in ("tol", "es", "en") or req.target_lang not in ("tol", "es", "en"):
        raise HTTPException(400, "Languages must be 'tol', 'es', or 'en'")
    if not req.text.strip():
        raise HTTPException(400, "Text cannot be empty")
    result = translator.translate(req.text, req.source_lang, req.target_lang)
    return result


@app.post("/api/dictionary")
async def dictionary(req: DictionaryRequest):
    results = translator.dictionary_lookup(req.query, req.lang)
    return {"results": results, "count": len(results)}


@app.get("/api/stats")
async def stats():
    return translator.get_stats()


@app.get("/api/browse/words")
async def browse_words(
    search: str = "",
    lang: str = "tol",
    category: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=10, le=500),
):
    conn = _db()
    offset = (page - 1) * per_page
    conditions, params = [], []

    if search:
        col = {"tol": "tol", "es": "spanish", "en": "english"}.get(lang, "tol")
        conditions.append(f"LOWER({col}) LIKE ?")
        params.append(f"%{search.lower()}%")
    if category:
        conditions.append("LOWER(category) LIKE ?")
        params.append(f"%{category.lower()}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    total = conn.execute(f"SELECT COUNT(*) FROM dictionary {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT tol, spanish, english, category, source FROM dictionary {where} ORDER BY tol COLLATE NOCASE LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()
    conn.close()
    return {
        "items": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
    }


@app.get("/api/browse/phrases")
async def browse_phrases(
    search: str = "",
    phrase_type: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=10, le=500),
):
    conn = _db()
    offset = (page - 1) * per_page
    conditions, params = [], []

    if search:
        conditions.append("(LOWER(tol_phrase) LIKE ? OR LOWER(spanish_phrase) LIKE ? OR LOWER(english_phrase) LIKE ?)")
        p = f"%{search.lower()}%"
        params += [p, p, p]
    if phrase_type:
        conditions.append("phrase_type = ?")
        params.append(phrase_type)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    total = conn.execute(f"SELECT COUNT(*) FROM phrase_translations {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT tol_phrase, spanish_phrase, english_phrase, phrase_type, cooccur, score FROM phrase_translations {where} ORDER BY score DESC LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()
    conn.close()
    return {
        "items": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
    }


@app.get("/api/browse/verbs")
async def browse_verbs(
    search: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=10, le=500),
):
    conn = _db()
    offset = (page - 1) * per_page
    conditions, params = [], []

    if search:
        conditions.append("(LOWER(tol_form) LIKE ? OR LOWER(spanish_form) LIKE ? OR LOWER(english_form) LIKE ?)")
        p = f"%{search.lower()}%"
        params += [p, p, p]

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    total = conn.execute(f"SELECT COUNT(*) FROM verb_conjugations {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT tol_form, spanish_form, english_form, tense, person FROM verb_conjugations {where} ORDER BY tol_form COLLATE NOCASE LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()
    conn.close()
    return {
        "items": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
    }


@app.get("/api/browse/sentences")
async def browse_sentences(
    search: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
):
    conn = _db()
    offset = (page - 1) * per_page
    conditions, params = [], []

    if search:
        conditions.append("(LOWER(tol) LIKE ? OR LOWER(spanish) LIKE ? OR LOWER(english) LIKE ?)")
        p = f"%{search.lower()}%"
        params += [p, p, p]

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    total = conn.execute(f"SELECT COUNT(*) FROM parallel_sentences {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT tol, spanish, english, source FROM parallel_sentences {where} ORDER BY id LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()
    conn.close()
    return {
        "items": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
    }


@app.get("/api/browse/categories")
async def browse_categories():
    conn = _db()
    rows = conn.execute(
        "SELECT COALESCE(category, '') as cat, COUNT(*) as cnt FROM dictionary GROUP BY cat ORDER BY cnt DESC"
    ).fetchall()
    conn.close()
    return [{"category": r["cat"] or "(uncategorized)", "count": r["cnt"]} for r in rows]


@app.get("/api/dict/stats")
async def dict_stats():
    """Pre-aggregated stats for the dictionary page."""
    conn = _db()
    d = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    det = conn.execute("SELECT COUNT(*) FROM direct_en_tol").fetchone()[0]
    ps = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
    vc = conn.execute("SELECT COUNT(*) FROM verb_conjugations").fetchone()[0]
    hw = conn.execute("SELECT COUNT(DISTINCT LOWER(tol)) FROM dictionary").fetchone()[0]
    conn.close()
    return {"headwords": hw, "dictionary": d, "direct_en_tol": det,
            "sentences": ps, "verbs": vc}


@app.get("/api/dict/search")
async def dict_search(
    q: str = "",
    page: int = Query(1, ge=1),
    per_page: int = Query(30, ge=5, le=100),
):
    """Fast unified dictionary search. Requires a search term (returns nothing for empty query)."""
    q_lower = q.strip().lower()
    if not q_lower:
        return {"entries": [], "total": 0, "page": 1, "per_page": per_page, "pages": 0}

    conn = _db()
    offset = (page - 1) * per_page
    like = f"%{q_lower}%"
    like_start = f"{q_lower}%"

    rows = conn.execute("""
        SELECT headword, MIN(priority) as pri FROM (
            SELECT tol as headword, CASE WHEN LOWER(tol) = ? THEN 0
                                         WHEN LOWER(tol) LIKE ? THEN 1 ELSE 2 END as priority
            FROM dictionary WHERE LOWER(tol) LIKE ?
            UNION ALL
            SELECT tol as headword, CASE WHEN LOWER(tol) = ? THEN 0
                                         WHEN LOWER(tol) LIKE ? THEN 1 ELSE 2 END as priority
            FROM direct_en_tol WHERE LOWER(tol) LIKE ?
            UNION ALL
            SELECT tol as headword, 3 as priority
            FROM dictionary WHERE LOWER(english) LIKE ?
            UNION ALL
            SELECT tol as headword, 3 as priority
            FROM direct_en_tol WHERE LOWER(english) LIKE ?
            UNION ALL
            SELECT tol as headword, 4 as priority
            FROM dictionary WHERE LOWER(spanish) LIKE ?
        ) GROUP BY LOWER(headword)
        ORDER BY pri, headword COLLATE NOCASE
        LIMIT ? OFFSET ?
    """, [q_lower, like_start, like, q_lower, like_start, like, like, like, like,
          per_page + 1, offset]).fetchall()

    has_more = len(rows) > per_page
    rows = rows[:per_page]

    entries = []
    for r in rows:
        entry = _build_dict_entry_fast(conn, r["headword"])
        if entry:
            entries.append(entry)

    total_est = offset + len(rows) + (1 if has_more else 0)
    pages_est = page + (1 if has_more else 0)

    conn.close()
    return {
        "entries": entries,
        "total": total_est,
        "page": page,
        "per_page": per_page,
        "pages": pages_est,
    }


@app.get("/api/dict/entry/{tol_word:path}")
async def dict_entry(tol_word: str):
    """Get a single rich dictionary entry for a Tol word."""
    conn = _db()
    entry = _build_dict_entry_full(conn, tol_word.strip())
    conn.close()
    if not entry:
        raise HTTPException(404, "Word not found")
    return entry


def _build_dict_entry_fast(conn, headword: str) -> Optional[dict]:
    """Lightweight entry for search results list — just meanings, no samples."""
    hw_lower = headword.lower()
    meanings = []
    seen = set()

    for r in conn.execute(
        "SELECT tol, spanish, english, category FROM dictionary WHERE LOWER(tol) = ? LIMIT 8",
        [hw_lower],
    ).fetchall():
        key = ((r["english"] or "").lower(), (r["spanish"] or "").lower())
        if key not in seen:
            seen.add(key)
            meanings.append({"english": r["english"] or "", "spanish": r["spanish"] or "",
                             "category": r["category"] or ""})
        if len(meanings) >= 5:
            break

    if len(meanings) < 5:
        for r in conn.execute(
            "SELECT tol, english, spanish FROM direct_en_tol WHERE LOWER(tol) = ? AND source IN ('grammar_pdf_verified','dictionary_direct') ORDER BY confidence DESC LIMIT 5",
            [hw_lower],
        ).fetchall():
            key = ((r["english"] or "").lower(), (r["spanish"] or "").lower())
            if key not in seen:
                seen.add(key)
                meanings.append({"english": r["english"] or "", "spanish": r["spanish"] or "", "category": ""})
            if len(meanings) >= 5:
                break

    if not meanings:
        return None

    return {"headword": headword, "headword_lower": hw_lower, "meanings": meanings[:5]}


def _build_dict_entry_full(conn, headword: str) -> Optional[dict]:
    """Full entry with meanings + sample sentences for the detail modal."""
    hw_lower = headword.lower()
    meanings = []
    seen = set()
    display_form = headword

    dict_rows = conn.execute(
        "SELECT tol, spanish, english, category FROM dictionary WHERE LOWER(tol) = ?", [hw_lower],
    ).fetchall()
    if dict_rows:
        display_form = dict_rows[0]["tol"]
    for r in dict_rows:
        key = ((r["english"] or "").lower(), (r["spanish"] or "").lower())
        if key not in seen:
            seen.add(key)
            meanings.append({"spanish": r["spanish"] or "", "english": r["english"] or "",
                             "category": r["category"] or ""})
        if len(meanings) >= 10:
            break

    det_rows = conn.execute(
        "SELECT tol, english, spanish, confidence FROM direct_en_tol WHERE LOWER(tol) = ? ORDER BY confidence DESC LIMIT 10",
        [hw_lower],
    ).fetchall()
    if not display_form or display_form == hw_lower:
        if det_rows:
            display_form = det_rows[0]["tol"]
    for r in det_rows:
        key = ((r["english"] or "").lower(), (r["spanish"] or "").lower())
        if key not in seen:
            seen.add(key)
            meanings.append({"spanish": r["spanish"] or "", "english": r["english"] or "", "category": ""})
        if len(meanings) >= 10:
            break

    samples = []
    hw_words = set(hw_lower.replace("-", " ").replace("=", " ").split())

    for r in conn.execute(
        "SELECT tol, spanish, english, source FROM parallel_sentences WHERE LOWER(tol) LIKE ? ORDER BY LENGTH(tol) LIMIT 12",
        [f"% {hw_lower} %"] if len(hw_lower) < 4 else [f"%{hw_lower}%"],
    ).fetchall():
        words = set(r["tol"].lower().replace("-", " ").replace("=", " ").split())
        if hw_words & words:
            samples.append({"tol": r["tol"], "spanish": r["spanish"] or "",
                            "english": r["english"] or "", "source": r["source"] or ""})
        if len(samples) >= 4:
            break

    if len(samples) < 4:
        for r in conn.execute(
            "SELECT tol_expected, spanish, english, source FROM grammar_test_sentences WHERE LOWER(tol_expected) LIKE ? LIMIT ?",
            [f"%{hw_lower}%", 4 - len(samples)],
        ).fetchall():
            samples.append({"tol": r["tol_expected"], "spanish": r["spanish"] or "",
                            "english": r["english"] or "", "source": r["source"] or "grammar"})
            if len(samples) >= 4:
                break

    if not meanings and not samples:
        return None

    return {"headword": display_form, "headword_lower": hw_lower,
            "meanings": meanings[:10], "samples": samples[:4]}


@app.get("/test")
async def test_page():
    return FileResponse(str(STATIC_DIR / "test.html"))


@app.get("/api/test-sentences")
async def test_sentences():
    """Run all grammar test sentences through the translator and return results."""
    conn = _db()
    rows = conn.execute(
        "SELECT id, english, tol_expected, spanish, source, grammar_notes FROM grammar_test_sentences ORDER BY id"
    ).fetchall()
    conn.close()

    results = []
    for r in rows:
        actual = translator.translate(r["english"], "en", "tol")
        translation = actual["translations"][0]["text"] if actual.get("translations") else actual.get("translation", "")
        score = tol_phonetic_score(r["tol_expected"], translation)

        results.append({
            "id": r["id"],
            "english": r["english"],
            "tol_expected": r["tol_expected"],
            "tol_actual": translation,
            "spanish": r["spanish"],
            "method": actual.get("method", actual["translations"][0]["method"] if actual.get("translations") else "unknown"),
            "confidence": actual.get("confidence", actual["translations"][0]["confidence"] if actual.get("translations") else 0),
            "source": r["source"],
            "grammar_notes": r["grammar_notes"],
            "match_score": round(score, 2),
        })

    passed = sum(1 for r in results if r["match_score"] >= 0.5)
    return {
        "results": results,
        "total": len(results),
        "passed": passed,
        "pass_rate": round(passed / max(len(results), 1) * 100, 1),
    }


NT_BOOK_ORDER = [
    "MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO", "GAL", "EPH",
    "PHP", "COL", "1TH", "2TH", "1TI", "2TI", "TIT", "PHM", "HEB", "JAS",
    "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV",
]

NT_BOOK_NAMES = {
    "MAT": {"tol": "Mateo", "es": "Mateo", "en": "Matthew"},
    "MRK": {"tol": "Marcos", "es": "Marcos", "en": "Mark"},
    "LUK": {"tol": "Lucas", "es": "Lucas", "en": "Luke"},
    "JHN": {"tol": "Juan", "es": "Juan", "en": "John"},
    "ACT": {"tol": "Hechos", "es": "Hechos", "en": "Acts"},
    "ROM": {"tol": "Romanos", "es": "Romanos", "en": "Romans"},
    "1CO": {"tol": "1 Corintios", "es": "1 Corintios", "en": "1 Corinthians"},
    "2CO": {"tol": "2 Corintios", "es": "2 Corintios", "en": "2 Corinthians"},
    "GAL": {"tol": "Gálatas", "es": "Gálatas", "en": "Galatians"},
    "EPH": {"tol": "Efesios", "es": "Efesios", "en": "Ephesians"},
    "PHP": {"tol": "Filipenses", "es": "Filipenses", "en": "Philippians"},
    "COL": {"tol": "Colosenses", "es": "Colosenses", "en": "Colossians"},
    "1TH": {"tol": "1 Tesalonicenses", "es": "1 Tesalonicenses", "en": "1 Thessalonians"},
    "2TH": {"tol": "2 Tesalonicenses", "es": "2 Tesalonicenses", "en": "2 Thessalonians"},
    "1TI": {"tol": "1 Timoteo", "es": "1 Timoteo", "en": "1 Timothy"},
    "2TI": {"tol": "2 Timoteo", "es": "2 Timoteo", "en": "2 Timothy"},
    "TIT": {"tol": "Tito", "es": "Tito", "en": "Titus"},
    "PHM": {"tol": "Filemón", "es": "Filemón", "en": "Philemon"},
    "HEB": {"tol": "Hebreos", "es": "Hebreos", "en": "Hebrews"},
    "JAS": {"tol": "Santiago", "es": "Santiago", "en": "James"},
    "1PE": {"tol": "1 Pedro", "es": "1 Pedro", "en": "1 Peter"},
    "2PE": {"tol": "2 Pedro", "es": "2 Pedro", "en": "2 Peter"},
    "1JN": {"tol": "1 Juan", "es": "1 Juan", "en": "1 John"},
    "2JN": {"tol": "2 Juan", "es": "2 Juan", "en": "2 John"},
    "3JN": {"tol": "3 Juan", "es": "3 Juan", "en": "3 John"},
    "JUD": {"tol": "Judas", "es": "Judas", "en": "Jude"},
    "REV": {"tol": "Apocalipsis", "es": "Apocalipsis", "en": "Revelation"},
}

TOL_AUDIO_DIR = Path(__file__).resolve().parent.parent / "Tol_Chapter_Audio"
ENG_AUDIO_DIR = Path(__file__).resolve().parent.parent / "English_Audio"


@app.get("/bible")
async def bible_page():
    return FileResponse(str(STATIC_DIR / "bible.html"))


@app.get("/api/bible/books")
async def bible_books():
    """Return all NT books with chapter counts."""
    conn = _db()
    rows = conn.execute("""
        SELECT source FROM parallel_sentences
        WHERE source LIKE 'bible_align:%' AND source LIKE '%:%:%'
    """).fetchall()
    conn.close()

    book_chapters: dict[str, set[int]] = {}
    for r in rows:
        m = re.match(r"bible_align:([A-Z0-9]+?)(\d{2}):(\d+)", r["source"])
        if m:
            bk, ch = m.group(1), int(m.group(2))
            book_chapters.setdefault(bk, set()).add(ch)

    books = []
    for code in NT_BOOK_ORDER:
        if code in book_chapters:
            chapters = sorted(book_chapters[code])
            books.append({
                "code": code,
                "names": NT_BOOK_NAMES.get(code, {"tol": code, "es": code, "en": code}),
                "chapters": chapters,
                "chapter_count": len(chapters),
            })
    return {"books": books}


@app.get("/api/bible/{book}/{chapter}")
async def bible_chapter(book: str, chapter: int):
    """Return all verses for a book+chapter in Tol, Spanish, English."""
    book = book.upper()
    if book not in NT_BOOK_NAMES:
        raise HTTPException(404, "Book not found")

    conn = _db()
    ch_str = f"{chapter:02d}"
    pattern = f"bible_align:{book}{ch_str}:%"
    rows = conn.execute(
        "SELECT source, tol, spanish, english FROM parallel_sentences WHERE source LIKE ? ORDER BY id",
        [pattern],
    ).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(404, f"No verses found for {book} {chapter}")

    verses = []
    for r in rows:
        m = re.match(r"bible_align:[A-Z0-9]+\d{2}:(\d+)", r["source"])
        verse_num = int(m.group(1)) if m else 0
        if verse_num == 0:
            continue
        verses.append({
            "verse": verse_num,
            "tol": r["tol"] or "",
            "spanish": r["spanish"] or "",
            "english": r["english"] or "",
        })

    verses.sort(key=lambda v: v["verse"])
    seen = set()
    unique_verses = []
    for v in verses:
        if v["verse"] not in seen:
            seen.add(v["verse"])
            unique_verses.append(v)

    all_chapters = _get_book_chapters(book)

    tol_chapter_mp3 = TOL_AUDIO_DIR / f"{book}_{ch_str}.mp3"
    has_tol_audio = tol_chapter_mp3.exists()
    eng_chapter_mp3 = ENG_AUDIO_DIR / f"{book}_{ch_str}.mp3"
    has_eng_audio = eng_chapter_mp3.exists()

    return {
        "book": book,
        "chapter": chapter,
        "names": NT_BOOK_NAMES.get(book, {"tol": book, "es": book, "en": book}),
        "verses": unique_verses,
        "total_chapters": len(all_chapters),
        "chapters": all_chapters,
        "tol_chapter_audio": f"/api/bible/audio/tol/{book}/{chapter}" if has_tol_audio else None,
        "english_chapter_audio": f"/api/bible/audio/en/{book}/{chapter}" if has_eng_audio else None,
    }


def _get_book_chapters(book: str) -> list[int]:
    conn = _db()
    rows = conn.execute(
        "SELECT DISTINCT source FROM parallel_sentences WHERE source LIKE ? AND source LIKE '%:%:%'",
        [f"bible_align:{book}%"],
    ).fetchall()
    conn.close()
    chapters = set()
    for r in rows:
        m = re.match(r"bible_align:[A-Z0-9]+?(\d{2}):", r["source"])
        if m:
            chapters.add(int(m.group(1)))
    return sorted(chapters)


@app.get("/api/bible/audio/tol/{book}/{chapter}")
async def bible_tol_chapter_audio(book: str, chapter: int):
    """Serve the Tol chapter-level MP3 audio (original ScriptureEarth recording)."""
    book = book.upper()
    mp3_name = f"{book}_{chapter:02d}.mp3"
    mp3_path = TOL_AUDIO_DIR / mp3_name
    if not mp3_path.exists():
        raise HTTPException(404, "Tol audio not found for this chapter")
    return FileResponse(str(mp3_path), media_type="audio/mpeg")


@app.get("/api/bible/audio/en/{book}/{chapter}")
async def bible_english_chapter_audio(book: str, chapter: int):
    """Serve the English WEB chapter-level MP3 audio."""
    book = book.upper()
    mp3_name = f"{book}_{chapter:02d}.mp3"
    mp3_path = ENG_AUDIO_DIR / mp3_name
    if not mp3_path.exists():
        raise HTTPException(404, "English audio not found for this chapter")
    return FileResponse(str(mp3_path), media_type="audio/mpeg")


PROJ_ROOT = Path(__file__).resolve().parent.parent

SOURCE_PDFS = [
    {"file": "Tol Pronunciation/Tol_Jicaque_Language_Overview_41p.pdf",
     "title": "Tol (Jicaque) Language Overview", "author": "Haurholm-Larsen", "pages": 41,
     "category": "Linguistic / Grammar",
     "description": "Comprehensive overview of Tol phonology, morphology, syntax, and sociolinguistics."},
    {"file": "Tol Pronunciation/Tol_Jicaque_Grammar_Holt.pdf",
     "title": "Tol Grammar Description", "author": "Holt", "pages": 33,
     "category": "Linguistic / Grammar",
     "description": "Phonological and grammatical description of Tol with verb morphology analysis."},
    {"file": "Tol Pronunciation/El_Alfabeto_Tol_1975.pdf",
     "title": "El Alfabeto Tol", "author": "Dennis, Dennis & Fleming, 1975", "pages": 20,
     "category": "Linguistic / Grammar",
     "description": "Tol alphabet and orthography guide produced by SIL/IHAH."},
    {"file": "Tol Pronunciation/Haurholm-Larsen_GrammaticalCategories_Slides.pdf",
     "title": "Grammatical Categories in Tol", "author": "Haurholm-Larsen (MPI)", "pages": 46,
     "category": "Linguistic / Grammar",
     "description": "Presentation slides on Tol grammatical categories from Max Planck Institute."},
    {"file": "Tol Pronunciation/Jicaque_Hokan_Classification_1953.pdf",
     "title": "Jicaque as a Hokan Language", "author": "Greenberg & Swadesh, 1953", "pages": 8,
     "category": "Linguistic / Grammar",
     "description": "Comparative linguistic analysis of Jicaque classification within Hokan language family."},
    {"file": "Tol Pronunciation/Tol_Language_Cozemius_1923.pdf",
     "title": "Tol Language Documentation", "author": "Cozemius, 1923", "pages": 8,
     "category": "Historical",
     "description": "Early 20th-century documentation of the Tol language and vocabulary."},
    {"file": "Tol Pronunciation/Jicaque_Torrupan_Indians_VonHagen_1943.pdf",
     "title": "The Jicaque (Torrupan) Indians of Honduras", "author": "Von Hagen, 1943", "pages": 132,
     "category": "Historical",
     "description": "Ethnographic and linguistic study of the Jicaque/Torrupan people."},
    {"file": "Tol Translation/DiccTol_Jicaque_Espanol_Dennis_1983.pdf",
     "title": "Diccionario Tol (Jicaque) — Español", "author": "Dennis & Dennis, 1983", "pages": 139,
     "category": "Dictionary",
     "description": "Tol-Spanish and Spanish-Tol bilingual dictionary compiled by SIL missionaries."},
    {"file": "Tol Translation/English_Tol_Dictionary_Dennis_1983.pdf",
     "title": "English–Tol Dictionary (from Dennis & Dennis 1983)", "author": "Tol Language Project", "pages": None,
     "category": "Dictionary",
     "description": "English glosses aligned to Tol headwords, derived from the Spanish–Tol SIL dictionary via neural Spanish→English (Argos Translate) and project OCR."},
    {"file": "Tol Translation/Tol_NT_Wycliffe_803p.pdf",
     "title": "Tol New Testament (Wycliffe)", "author": "Wycliffe Bible Translators", "pages": 803,
     "category": "Bible Texts",
     "description": "Complete Tol New Testament, Wycliffe edition with detailed formatting."},
    {"file": "Tol Translation/Tol_NT_Complete.pdf",
     "title": "Tol New Testament (eBible)", "author": "eBible.org", "pages": 483,
     "category": "Bible Texts",
     "description": "Complete Tol New Testament, eBible edition, text-extractable."},
    {"file": "Tol Translation/Tol_NT_Marcos.pdf",
     "title": "Gospel of Mark in Tol", "author": "eBible.org", "pages": None,
     "category": "Bible Texts",
     "description": "The Gospel of Mark translated into Tol."},
    {"file": "Tol Translation/Tol_NT_Juan.pdf",
     "title": "Gospel of John in Tol", "author": "eBible.org", "pages": None,
     "category": "Bible Texts",
     "description": "The Gospel of John translated into Tol."},
]


@app.get("/sources")
async def sources_page():
    return FileResponse(str(STATIC_DIR / "sources.html"))


@app.get("/api/sources")
async def sources_data():
    """Return catalog of all data sources."""
    pdfs = []
    for p in SOURCE_PDFS:
        full = PROJ_ROOT / p["file"]
        pdfs.append({**p, "available": full.exists(),
                     "url": f"/sources/pdf/{p['file']}" if full.exists() else None})
    return {"pdfs": pdfs}


@app.get("/sources/pdf/{filepath:path}")
async def serve_pdf(filepath: str):
    """Serve a source PDF file."""
    full = PROJ_ROOT / filepath
    if not full.exists() or not str(full).endswith(".pdf"):
        raise HTTPException(404, "PDF not found")
    if not str(full.resolve()).startswith(str(PROJ_ROOT.resolve())):
        raise HTTPException(403, "Access denied")
    return FileResponse(str(full), media_type="application/pdf")


LEARN_VOCAB_PATH = Path(__file__).parent / "data" / "learn_vocab.json"
_learn_cache = {}


def _load_learn_vocab():
    if "data" not in _learn_cache or _learn_cache.get("mtime") != LEARN_VOCAB_PATH.stat().st_mtime:
        import json as _json
        _learn_cache["data"] = _json.loads(LEARN_VOCAB_PATH.read_text(encoding="utf-8"))
        _learn_cache["mtime"] = LEARN_VOCAB_PATH.stat().st_mtime
    return _learn_cache["data"]


@app.get("/learn")
async def learn_page():
    return FileResponse(str(STATIC_DIR / "learn.html"))


@app.get("/api/learn/stats")
async def learn_stats():
    data = _load_learn_vocab()
    return data["stats"]


@app.get("/api/learn/categories")
async def learn_categories():
    data = _load_learn_vocab()
    cats = {}
    for w in data["vocabulary"]:
        cat = w["category"]
        if cat not in cats:
            cats[cat] = {"count": 0, "easy": 0, "medium": 0, "hard": 0}
        cats[cat]["count"] += 1
        if w["difficulty"] == 1:
            cats[cat]["easy"] += 1
        elif w["difficulty"] == 2:
            cats[cat]["medium"] += 1
        else:
            cats[cat]["hard"] += 1
    return {"categories": cats}


@app.get("/api/learn/words")
async def learn_words(
    category: Optional[str] = None,
    difficulty: Optional[int] = None,
    limit: int = Query(default=20, le=100),
    offset: int = 0,
):
    data = _load_learn_vocab()
    words = data["vocabulary"]
    if category:
        words = [w for w in words if w["category"] == category]
    if difficulty:
        words = [w for w in words if w["difficulty"] == difficulty]
    total = len(words)
    words = words[offset:offset + limit]
    return {"words": words, "total": total, "offset": offset, "limit": limit}


@app.get("/api/learn/quiz")
async def learn_quiz(
    category: Optional[str] = None,
    difficulty: Optional[int] = None,
    count: int = Query(default=10, le=30),
    mode: str = Query(default="spa_to_tol"),
):
    """Generate a quiz with multiple-choice questions.
    Modes: spa_to_tol, tol_to_spa, listen_pick (future)
    """
    import random
    data = _load_learn_vocab()
    words = [w for w in data["vocabulary"] if w["spanish"] and w["tol"]]
    if category:
        words = [w for w in words if w["category"] == category]
    if difficulty:
        words = [w for w in words if w["difficulty"] == difficulty]
    if len(words) < 4:
        raise HTTPException(400, "Not enough words in this category for a quiz")

    quiz_words = random.sample(words, min(count, len(words)))
    questions = []

    spa_to_eng = {w["spanish"]: w.get("english", "") for w in words if w["spanish"]}

    for qw in quiz_words:
        if mode == "spa_to_tol":
            prompt = qw["spanish"]
            correct = qw["tol"]
            distractors_pool = [w["tol"] for w in words if w["tol"] != correct]
        else:
            prompt = qw["tol"]
            correct = qw["spanish"]
            distractors_pool = [w["spanish"] for w in words if w["spanish"] != correct]

        distractors = random.sample(distractors_pool, min(3, len(distractors_pool)))
        options = [correct] + distractors
        random.shuffle(options)

        if mode == "spa_to_tol":
            options_english = None
            correct_english = None
            prompt_english = qw.get("english", "")
        else:
            options_english = [spa_to_eng.get(o, "") for o in options]
            correct_english = spa_to_eng.get(correct, "")
            prompt_english = ""

        questions.append({
            "id": qw["id"],
            "prompt": prompt,
            "options": options,
            "options_english": options_english,
            "correct": correct,
            "correct_english": correct_english,
            "english": qw.get("english", ""),
            "prompt_english": prompt_english,
            "category": qw["category"],
        })

    return {"questions": questions, "mode": mode, "count": len(questions)}


@app.get("/api/learn/match")
async def learn_match(
    category: Optional[str] = None,
    difficulty: Optional[int] = None,
    count: int = Query(default=6, le=12),
):
    """Generate matching pairs for a memory/match game."""
    import random
    data = _load_learn_vocab()
    words = [w for w in data["vocabulary"] if w["spanish"] and w["tol"] and w["word_count"] <= 3]
    if category:
        words = [w for w in words if w["category"] == category]
    if difficulty:
        words = [w for w in words if w["difficulty"] == difficulty]
    if len(words) < count:
        count = len(words)
    if count < 3:
        raise HTTPException(400, "Not enough words for matching")

    selected = random.sample(words, count)
    pairs = [{"tol": w["tol"], "spanish": w["spanish"], "english": w.get("english", "")} for w in selected]
    return {"pairs": pairs}


@app.get("/api/learn/verb-challenge")
async def learn_verb_challenge(count: int = Query(default=5, le=15)):
    """Generate verb conjugation fill-in-the-blank exercises."""
    import random
    data = _load_learn_vocab()
    conjs = data.get("verb_conjugations", [])
    if len(conjs) < 4:
        raise HTTPException(400, "Not enough verb data")
    selected = random.sample(conjs, min(count, len(conjs)))
    questions = []
    for vc in selected:
        pool = [c["tol"] for c in conjs if c["tol"] != vc["tol"] and c["base_tol"] == vc["base_tol"]]
        if len(pool) < 3:
            pool = [c["tol"] for c in conjs if c["tol"] != vc["tol"]]
        distractors = random.sample(pool, min(3, len(pool)))
        options = [vc["tol"]] + distractors
        random.shuffle(options)
        questions.append({
            "prompt_spanish": vc["spanish"],
            "prompt_english": vc.get("english", ""),
            "base_verb": vc.get("base_spanish", ""),
            "tense": vc.get("tense", ""),
            "person": vc.get("person", ""),
            "correct": vc["tol"],
            "options": options,
        })
    return {"questions": questions}


class FeedbackSubmit(BaseModel):
    """User feedback for translations, dictionary, bugs, and feature ideas."""

    category: str = "other"
    from_page: str = "unknown"
    contact: Optional[str] = None
    notes: str = ""
    structured: dict = {}
    cursor_block: str
    website: str = ""  # honeypot — must be empty


def _append_feedback_record(record: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with open(FEEDBACK_INBOX, "a", encoding="utf-8") as f:
        f.write(line)


def _send_feedback_email(subject: str, body: str) -> Tuple[bool, Optional[str]]:
    """Send via SMTP if FEEDBACK_SMTP_* env vars are set. Returns (ok, error_message)."""
    to_addr = os.environ.get("FEEDBACK_EMAIL_TO", FEEDBACK_EMAIL_DEFAULT).strip()
    host = os.environ.get("FEEDBACK_SMTP_HOST", "").strip()
    user = os.environ.get("FEEDBACK_SMTP_USER", "").strip()
    password = os.environ.get("FEEDBACK_SMTP_PASSWORD", "").strip()
    if not host or not user or not password:
        return False, "SMTP not configured (set FEEDBACK_SMTP_HOST, FEEDBACK_SMTP_USER, FEEDBACK_SMTP_PASSWORD)"
    port = int(os.environ.get("FEEDBACK_SMTP_PORT", "587"))
    from_addr = os.environ.get("FEEDBACK_EMAIL_FROM", user).strip()
    msg = EmailMessage()
    msg["Subject"] = subject[:900]
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(body)
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.starttls(context=context)
            server.login(user, password)
            server.send_message(msg)
        return True, None
    except Exception as e:
        logger.exception("Feedback email SMTP failed")
        return False, str(e)


@app.post("/api/feedback")
async def submit_feedback(req: FeedbackSubmit):
    # Honeypot: silently accept but do not store or email
    if req.website and req.website.strip():
        return {"ok": True, "emailed": False, "stored": False}

    rid = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    record = {
        "id": rid,
        "ts": ts,
        "category": req.category,
        "from_page": req.from_page,
        "contact": (req.contact or "").strip() or None,
        "notes": (req.notes or "").strip(),
        "structured": req.structured or {},
        "cursor_block": (req.cursor_block or "").strip(),
    }
    _append_feedback_record(record)

    subject = f"[Tol app feedback] {req.category} — {req.from_page} ({rid[:8]})"
    body = (
        f"Feedback ID: {rid}\n"
        f"Time (UTC): {ts}\n"
        f"Category: {req.category}\n"
        f"Page: {req.from_page}\n"
        f"Contact: {record['contact'] or '(none)'}\n"
        f"\n--- Notes ---\n{req.notes.strip() or '(none)'}\n"
        f"\n--- Structured fields (JSON) ---\n{json.dumps(req.structured or {}, indent=2, ensure_ascii=False)}\n"
        f"\n--- Cursor / maintainer block ---\n\n{req.cursor_block.strip()}\n"
    )

    emailed, err = _send_feedback_email(subject, body)
    if not emailed:
        logger.info("Feedback %s saved locally; email skipped: %s", rid, err)

    return {
        "ok": True,
        "id": rid,
        "emailed": emailed,
        "stored": True,
        "email_error": err if not emailed else None,
    }


class TTSRequest(BaseModel):
    text: str


@app.get("/api/tts-status")
async def tts_status():
    return {"available": tts_engine.is_available()}


@app.post("/api/tts")
async def tts_synthesize(req: TTSRequest):
    if not req.text.strip():
        raise HTTPException(400, "Text cannot be empty")
    if not tts_engine.is_available():
        raise HTTPException(503, "TTS model not available yet — still training")
    try:
        wav_bytes = tts_engine.synthesize(req.text.strip())
        return Response(content=wav_bytes, media_type="audio/wav")
    except Exception as e:
        logger.exception("TTS synthesis failed")
        raise HTTPException(500, f"TTS synthesis error: {e}")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
