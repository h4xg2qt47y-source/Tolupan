#!/usr/bin/env python3
"""
Build English ↔ Tol reference from the parsed Spanish–Tol dictionary (Dennis & Dennis 1983).

1. Loads scripts/parsed_dictionary.json (from OCR pipeline).
2. Translates Spanish glosses to English using Argos Translate (offline neural MT, es→en).
3. Writes:
   - Tol Translation/English_Tol_Dictionary_Dennis_1983.pdf
   - app/data/en_tol_dictionary_import.json (for review / reproducibility)
   - app/data/es_to_en_cache.json (translation cache)
4. Optionally applies UPDATEs to tol.db (dictionary + SIL_Dictionary_Example sentences).

First run: pip install fpdf2 argostranslate
           (Argos will download the es→en model on first translate() call.)
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
PARSED = BASE / "scripts" / "parsed_dictionary.json"
CACHE_PATH = BASE / "app" / "data" / "es_to_en_cache.json"
IMPORT_JSON = BASE / "app" / "data" / "en_tol_dictionary_import.json"
DB_PATH = BASE / "app" / "data" / "tol.db"
PDF_OUT = BASE / "Tol Translation" / "English_Tol_Dictionary_Dennis_1983.pdf"

# macOS / Linux font for Tol orthography (Unicode)
FONT_CANDIDATES = [
    Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
    Path("/Library/Fonts/Arial Unicode.ttf"),
    Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
]


def _ensure_argos_es_en() -> None:
    import argostranslate.package

    argostranslate.package.update_package_index()
    pkgs = argostranslate.package.get_available_packages()
    for p in pkgs:
        if p.from_code == "es" and p.to_code == "en":
            argostranslate.package.install_from_path(p.download())
            return
    raise RuntimeError("No Argos es→en package found")


def _load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=0), encoding="utf-8")


def translate_es_en(text: str, cache: dict) -> str:
    """Spanish → English via Argos (cached)."""
    t = text.strip()
    if not t:
        return ""
    key = t.casefold()
    if key in cache:
        return cache[key]

    from argostranslate.translate import translate

    # Chunk very long glosses (Argos works best on sentence-sized input)
    parts = re.split(r"(?<=[.!?])\s+", t)
    if len(t) > 400 and len(parts) > 1:
        out = " ".join(translate(p.strip(), "es", "en") for p in parts if p.strip())
    else:
        out = translate(t, "es", "en")

    cache[key] = out.strip()
    return cache[key]


def build_rows(parsed: dict, cache: dict, progress_every: int = 100) -> tuple[list[dict], list[dict]]:
    """Returns (dictionary_rows, sentence_rows) with English filled."""
    dict_out = []
    seen = set()
    n = 0
    for e in parsed.get("dictionary", []):
        tol = (e.get("tol") or "").strip()
        spa = (e.get("spanish") or "").strip()
        if not tol or not spa:
            continue
        key = (tol.casefold(), spa.casefold())
        if key in seen:
            continue
        seen.add(key)
        en = translate_es_en(spa, cache)
        dict_out.append(
            {
                "tol": tol,
                "spanish": spa,
                "english": en,
                "category": e.get("category") or "",
                "section": e.get("section") or "",
            }
        )
        n += 1
        if n % progress_every == 0:
            print(f"  … translated {n} dictionary glosses", flush=True)
            _save_cache(cache)

    sent_out = []
    for s in parsed.get("sentences", []):
        tol = (s.get("tol") or "").strip()
        spa = (s.get("spanish") or "").strip()
        if not tol or not spa:
            continue
        en = translate_es_en(spa, cache)
        sent_out.append({"tol": tol, "spanish": spa, "english": en, "source": s.get("source") or "SIL_Dictionary_Example"})

    return dict_out, sent_out


def render_pdf(rows: list[dict], out_path: Path) -> None:
    from fpdf import FPDF

    font_path = next((p for p in FONT_CANDIDATES if p.exists()), None)
    if not font_path:
        raise FileNotFoundError("No Unicode TTF font found (need Arial Unicode or DejaVuSans)")

    rows = sorted(rows, key=lambda r: (r.get("english") or "").casefold())

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.add_page()
    pdf.add_font("TolFont", "", str(font_path))
    pdf.set_font("TolFont", "", 11)

    title = "English–Tol Dictionary"
    subtitle = (
        "Derived from Dennis & Dennis (1983) Tol–Spanish / Spanish–Tol dictionary. "
        "English glosses produced with Argos Translate (neural es→en). "
        "Tol and Spanish headwords from project OCR (parsed_dictionary.json)."
    )
    pdf.cell(0, 8, title, ln=1)
    pdf.set_font("TolFont", "", 8)
    pdf.multi_cell(0, 4, subtitle)
    pdf.ln(3)

    for i, r in enumerate(rows, start=1):
        if pdf.get_y() > 270:
            pdf.add_page()
            pdf.set_font("TolFont", "", 8)

        en = r.get("english") or ""
        tol = r.get("tol") or ""
        spa = r.get("spanish") or ""
        cat = r.get("category") or ""

        pdf.set_font("TolFont", "", 9)
        pdf.set_text_color(40, 90, 40)
        pdf.cell(0, 5, f"{i}.", ln=1)
        pdf.set_text_color(0, 0, 0)
        block = f"English: {en}\nTol: {tol}\nSpanish: {spa}"
        if cat:
            block += f"\nCategory: {cat}"
        pdf.set_font("TolFont", "", 8)
        pdf.multi_cell(0, 4, block)
        pdf.ln(2)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(out_path))


def apply_database(dict_rows: list[dict], sent_rows: list[dict]) -> None:
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    d_updated = 0
    for r in dict_rows:
        tol, spa, en = r["tol"], r["spanish"], r["english"]
        if not en:
            continue
        cur.execute(
            """
            UPDATE dictionary
            SET english = ?
            WHERE tol = ? AND spanish = ? AND source = 'SIL_Dictionary_OCR'
            """,
            (en, tol, spa),
        )
        d_updated += cur.rowcount

    s_updated = 0
    for r in sent_rows:
        tol, spa, en = r["tol"], r["spanish"], r["english"]
        if not en:
            continue
        cur.execute(
            """
            UPDATE parallel_sentences
            SET english = ?
            WHERE tol = ? AND spanish = ? AND source = 'SIL_Dictionary_Example'
            """,
            (en, tol, spa),
        )
        s_updated += cur.rowcount

    conn.commit()
    conn.close()
    print(f"  Database: updated {d_updated} dictionary rows, {s_updated} parallel_sentences rows.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-pdf", action="store_true", help="JSON + DB only")
    ap.add_argument("--skip-db", action="store_true", help="Do not write tol.db")
    ap.add_argument("--pdf-only", action="store_true", help="Rebuild PDF from existing import JSON")
    args = ap.parse_args()

    if not PARSED.exists():
        print(f"Missing {PARSED} — run OCR parse pipeline first.", file=sys.stderr)
        sys.exit(1)

    print("Ensuring Argos Translate es→en model…")
    _ensure_argos_es_en()

    cache = _load_cache()

    if args.pdf_only:
        data = json.loads(IMPORT_JSON.read_text(encoding="utf-8"))
        dict_rows = data["dictionary"]
        print(f"Rendering PDF from {IMPORT_JSON} ({len(dict_rows)} rows)…")
        render_pdf(dict_rows, PDF_OUT)
        print(f"Wrote {PDF_OUT}")
        return

    print(f"Loading {PARSED}…")
    parsed = json.loads(PARSED.read_text(encoding="utf-8"))

    t0 = time.time()
    dict_rows, sent_rows = build_rows(parsed, cache)
    _save_cache(cache)
    print(f"Translated {len(dict_rows)} dictionary + {len(sent_rows)} example rows in {time.time()-t0:.1f}s")

    IMPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    IMPORT_JSON.write_text(
        json.dumps(
            {
                "meta": {
                    "source_pdf": "Tol Translation/DiccTol_Jicaque_Espanol_Dennis_1983.pdf",
                    "mt_engine": "Argos Translate (es→en)",
                    "parsed_input": str(PARSED.relative_to(BASE)),
                },
                "dictionary": dict_rows,
                "sentences": sent_rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Wrote {IMPORT_JSON}")

    if not args.skip_pdf:
        print("Rendering PDF (may take a minute)…")
        render_pdf(dict_rows, PDF_OUT)
        print(f"Wrote {PDF_OUT}")

    if not args.skip_db:
        apply_database(dict_rows, sent_rows)


if __name__ == "__main__":
    main()
