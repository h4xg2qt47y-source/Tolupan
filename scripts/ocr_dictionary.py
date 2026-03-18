#!/usr/bin/env python3
"""
OCR the 139-page SIL Tol-Spanish dictionary PDF using macOS Vision framework,
then parse all entries into structured data and insert into tol.db.
"""

import fitz
import Vision
import Quartz
from Foundation import NSData
import json
import re
import sqlite3
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
PDF_PATH = BASE / "Tol Translation" / "DiccTol_Jicaque_Espanol_Dennis_1983.pdf"
DB_PATH = BASE / "app" / "data" / "tol.db"
OCR_CACHE = BASE / "scripts" / "ocr_raw_pages.json"
PARSED_OUTPUT = BASE / "scripts" / "parsed_dictionary.json"


def ocr_page(pix):
    """OCR a pymupdf pixmap using macOS Vision."""
    img_data = pix.tobytes("png")
    ns_data = NSData.dataWithBytes_length_(img_data, len(img_data))
    ci_image = Quartz.CIImage.imageWithData_(ns_data)
    handler = Vision.VNImageRequestHandler.alloc().initWithCIImage_options_(ci_image, None)
    request = Vision.VNRecognizeTextRequest.alloc().init()
    request.setRecognitionLevel_(Vision.VNRequestTextRecognitionLevelAccurate)
    request.setRecognitionLanguages_(["es", "en"])
    request.setUsesLanguageCorrection_(False)
    success, error = handler.performRequests_error_([request], None)
    if not success:
        return ""
    results = request.results()
    lines = []
    for obs in sorted(results, key=lambda o: -o.boundingBox().origin.y):
        text = obs.topCandidates_(1)[0].string()
        lines.append(text)
    return "\n".join(lines)


def ocr_all_pages():
    """OCR every page and cache results."""
    if OCR_CACHE.exists():
        print(f"Loading cached OCR from {OCR_CACHE}")
        return json.load(open(OCR_CACHE, "r", encoding="utf-8"))

    doc = fitz.open(str(PDF_PATH))
    pages = {}
    total = len(doc)
    for i in range(total):
        page = doc[i]
        pix = page.get_pixmap(dpi=300)
        text = ocr_page(pix)
        pages[str(i)] = text
        pct = (i + 1) / total * 100
        sys.stdout.write(f"\r  OCR page {i+1}/{total} ({pct:.0f}%)")
        sys.stdout.flush()
    doc.close()
    print()

    with open(OCR_CACHE, "w", encoding="utf-8") as f:
        json.dump(pages, f, ensure_ascii=False, indent=2)
    print(f"  Cached OCR to {OCR_CACHE}")
    return pages


POS_MAP = {
    "s": "sustantivo",
    "f": "sustantivo",
    "m": "sustantivo",
    "ve": "verbo",
    "vi": "verbo",
    "vt": "verbo",
    "vi: I": "verbo",
    "vi: II": "verbo",
    "vt: I": "verbo",
    "vt: II": "verbo",
    "adj": "adjetivo",
    "adv": "adverbio",
    "adv d": "adverbio",
    "adv t": "adverbio",
    "pro": "pronombre",
    "pro dem": "pronombre",
    "pro int": "pronombre",
    "conj": "conjunción",
    "prep": "preposición",
    "int": "interjección",
    "ady int": "adverbio",
    "adj int": "adjetivo",
}


def parse_tol_to_spanish(text):
    """Parse Tol→Spanish section entries."""
    entries = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.isdigit() or len(line) < 3:
            i += 1
            continue

        # Match patterns like: headword \n (pos) definition
        # Or: headword (pos) definition on same line
        # Or: definition starting with (pos) after a headword
        pos_match = re.search(r'\(([^)]+)\)\s*(.+)', line)
        if pos_match:
            pos_raw = pos_match.group(1).strip()
            definition = pos_match.group(2).strip()
            pos_cat = POS_MAP.get(pos_raw, pos_raw)

            # The headword might be on this line before the (pos) or on previous line
            before_pos = line[:pos_match.start()].strip()
            if before_pos and len(before_pos) < 50:
                headword = before_pos
            elif i > 0:
                headword = lines[i - 1].strip() if lines[i - 1].strip() and not lines[i - 1].strip().isdigit() else ""
            else:
                headword = ""

            if headword and definition and len(headword) < 60 and len(definition) < 200:
                # Check if this looks like a Tol word (contains special chars or is lowercase)
                if not re.match(r'^[A-Z][A-Z\s]+$', headword):  # Skip page headers
                    entries.append({
                        "tol": headword.rstrip(".,:;"),
                        "spanish": definition.rstrip("."),
                        "category": pos_cat,
                        "section": "tol_to_spanish",
                    })
        i += 1
    return entries


def parse_spanish_to_tol(text):
    """Parse Spanish→Tol section entries."""
    entries = []
    lines = text.split("\n")

    for i, line in enumerate(lines):
        line = line.strip()
        if not line or line.isdigit() or len(line) < 3:
            continue

        # Pattern: "Spanish word (pos) Tol translation."
        # Or: "Spanish word: yo verbo (vt) tol_word."
        m = re.match(r'^(.+?)\s*\(([^)]+)\)\s*(.+?)\.?\s*$', line)
        if m:
            spanish = m.group(1).strip().rstrip(":;,")
            pos_raw = m.group(2).strip()
            tol = m.group(3).strip().rstrip(".")
            pos_cat = POS_MAP.get(pos_raw, pos_raw)

            if (spanish and tol and len(spanish) < 80 and len(tol) < 80
                    and not re.match(r'^[A-Z]{2,}', spanish)
                    and not spanish.isdigit()):
                entries.append({
                    "tol": tol,
                    "spanish": spanish,
                    "category": pos_cat,
                    "section": "spanish_to_tol",
                })

        # Also match simpler patterns: "word: translation"
        m2 = re.match(r'^([a-záéíóúñü][\w\s]{1,40}):\s*(?:yo\s+\w+\s+)?\((\w+[^)]*)\)\s+(.+?)\.?\s*$', line, re.I)
        if m2 and not m:
            spanish = m2.group(1).strip()
            pos_raw = m2.group(2).strip()
            tol = m2.group(3).strip().rstrip(".")
            pos_cat = POS_MAP.get(pos_raw, pos_raw)
            if spanish and tol and len(tol) < 80:
                entries.append({
                    "tol": tol,
                    "spanish": spanish,
                    "category": pos_cat,
                    "section": "spanish_to_tol",
                })

    return entries


def extract_example_sentences(text):
    """Extract Tol-Spanish example sentence pairs from the OCR text."""
    entries = []
    lines = text.split("\n")

    for i in range(len(lines) - 1):
        current = lines[i].strip()
        next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

        if not current or not next_line:
            continue

        # Heuristic: a Tol sentence often contains apostrophes and special chars
        # followed by a Spanish translation
        has_tol_chars = any(c in current for c in "'üïöëṽ") or "'" in current
        is_spanish = any(c in next_line for c in "áéíóúñ") or re.search(r'\b(el|la|los|las|un|una|yo|de|en|que|es|no|se|su|con|por|del)\b', next_line, re.I)

        if has_tol_chars and is_spanish and 5 < len(current) < 200 and 5 < len(next_line) < 200:
            # Skip lines that are clearly headwords or metadata
            if not re.match(r'^\(', current) and not current.isdigit():
                entries.append({
                    "tol": current,
                    "spanish": next_line,
                    "source": "SIL_Dictionary_Example",
                })

    return entries


def parse_all(pages):
    """Parse all OCR pages into structured entries."""
    all_dict_entries = []
    all_sentences = []

    # Tol→Spanish section: roughly pages 12-80 (0-indexed)
    tol_spa_text = "\n".join(pages.get(str(i), "") for i in range(12, 81))
    entries_ts = parse_tol_to_spanish(tol_spa_text)
    all_dict_entries.extend(entries_ts)
    print(f"  Tol→Spanish section: {len(entries_ts)} entries")

    # Spanish→Tol section: roughly pages 82-134
    spa_tol_text = "\n".join(pages.get(str(i), "") for i in range(82, 135))
    entries_st = parse_spanish_to_tol(spa_tol_text)
    all_dict_entries.extend(entries_st)
    print(f"  Spanish→Tol section: {len(entries_st)} entries")

    # Example sentences from all dictionary pages
    all_text = "\n".join(pages.get(str(i), "") for i in range(12, 135))
    sentences = extract_example_sentences(all_text)
    all_sentences.extend(sentences)
    print(f"  Example sentences: {len(sentences)} pairs")

    # Deduplicate entries
    seen = set()
    unique_entries = []
    for e in all_dict_entries:
        key = (e["tol"].lower().strip(), e["spanish"].lower().strip())
        if key not in seen and e["tol"] and e["spanish"]:
            seen.add(key)
            unique_entries.append(e)

    seen_sent = set()
    unique_sentences = []
    for s in all_sentences:
        key = (s["tol"].lower().strip(), s["spanish"].lower().strip())
        if key not in seen_sent:
            seen_sent.add(key)
            unique_sentences.append(s)

    print(f"  After dedup: {len(unique_entries)} dict entries, {len(unique_sentences)} sentences")
    return unique_entries, unique_sentences


SPANISH_ENGLISH = {
    "cabello": "hair", "cabeza": "head", "oreja": "ear", "ojo": "eye",
    "boca": "mouth", "diente": "tooth", "lengua": "tongue", "uña": "nail",
    "pie": "foot", "pierna": "leg", "rodilla": "knee", "mano": "hand",
    "ala": "wing", "barriga": "belly", "tripa": "gut", "cuello": "neck",
    "pecho": "chest", "corazón": "heart", "agua": "water", "fuego": "fire",
    "tierra": "earth", "sol": "sun", "luna": "moon", "estrella": "star",
    "árbol": "tree", "piedra": "stone", "casa": "house", "camino": "road",
    "montaña": "mountain", "río": "river", "hombre": "man", "mujer": "woman",
    "niño": "child", "persona": "person", "gente": "people", "perro": "dog",
    "pájaro": "bird", "pescado": "fish", "maíz": "corn", "frijol": "bean",
    "lluvia": "rain", "viento": "wind", "nube": "cloud", "noche": "night",
    "día": "day", "mañana": "morning", "año": "year", "mes": "month",
    "grande": "big", "chiquito": "small", "bueno": "good", "malo": "bad",
    "viejo": "old", "nuevo": "new", "largo": "long", "corto": "short",
    "alto": "tall", "hondo": "deep", "gordo": "fat", "flaco": "thin",
    "blanco": "white", "negro": "black", "rojo": "red", "verde": "green",
    "sí": "yes", "no": "no", "uno": "one", "dos": "two", "tres": "three",
    "cuatro": "four", "cinco": "five", "diez": "ten",
    "comer": "to eat", "beber": "to drink", "dormir": "to sleep",
    "caminar": "to walk", "correr": "to run", "hablar": "to speak",
    "ver": "to see", "oír": "to hear", "saber": "to know", "morir": "to die",
    "vivir": "to live", "matar": "to kill", "ir": "to go", "venir": "to come",
    "dar": "to give", "lavar": "to wash", "llorar": "to cry",
    "cantar": "to sing", "reír": "to laugh", "jugar": "to play",
    "nadar": "to swim", "volar": "to fly", "pescar": "to fish",
    "sembrar": "to plant", "cosechar": "to harvest", "cocinar": "to cook",
    "quemar": "to burn", "caer": "to fall", "romper": "to break",
    "abrir": "to open", "cerrar": "to close", "comprar": "to buy",
    "vender": "to sell", "trabajar": "to work", "buscar": "to look for",
    "encontrar": "to find", "llevar": "to carry", "tirar": "to throw",
    "subir": "to climb", "bajar": "to go down", "entrar": "to enter",
    "salir": "to go out", "sentarse": "to sit", "pararse": "to stand",
    "abril": "April", "agosto": "August", "enero": "January",
    "azúcar": "sugar", "café": "coffee", "miel": "honey",
    "madera": "wood", "milpa": "cornfield", "tortilla": "tortilla",
    "sandalia": "sandal", "sombrero": "hat", "machete": "machete",
    "canasta": "basket", "cuchillo": "knife", "puente": "bridge",
    "cerro": "hill", "cueva": "cave", "arroyo": "stream",
    "serpiente": "snake", "araña": "spider", "grillo": "cricket",
    "caballo": "horse", "vaca": "cow", "gallina": "chicken",
    "paloma": "dove", "ardilla": "squirrel", "danto": "tapir",
    "fuerte": "strong", "débil": "weak", "sucio": "dirty", "limpio": "clean",
    "caliente": "hot", "frío": "cold", "seco": "dry", "mojado": "wet",
    "fantasma": "ghost", "espíritu": "spirit", "medicina": "medicine",
    "enfermo": "sick", "cansado": "tired", "hambre": "hunger",
    "cárcel": "jail", "ladrón": "thief", "pueblo": "town",
}


def insert_into_db(dict_entries, sentences):
    """Insert parsed entries into the SQLite database."""
    conn = sqlite3.connect(str(DB_PATH))

    inserted_dict = 0
    skipped_dict = 0
    for e in dict_entries:
        spa_lower = e["spanish"].lower().strip()
        english = SPANISH_ENGLISH.get(spa_lower, "")
        try:
            conn.execute(
                "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                (e["tol"], e["spanish"], english, e["category"], "SIL_Dictionary_OCR"),
            )
            if conn.total_changes:
                inserted_dict += 1
        except Exception:
            skipped_dict += 1

    inserted_sent = 0
    for s in sentences:
        try:
            conn.execute(
                "INSERT INTO parallel_sentences (tol, spanish, english, source) VALUES (?, ?, ?, ?)",
                (s["tol"], s["spanish"], "", s["source"]),
            )
            inserted_sent += 1
        except Exception:
            pass

    conn.commit()

    d = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
    s = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
    v = conn.execute("SELECT COUNT(*) FROM verb_conjugations").fetchone()[0]

    conn.close()
    return inserted_dict, inserted_sent, d, s, v


def main():
    print("=" * 60)
    print("SIL Dictionary OCR & Parse Pipeline")
    print("=" * 60)

    print("\n1. OCR all pages...")
    pages = ocr_all_pages()
    print(f"   OCR complete: {len(pages)} pages")

    print("\n2. Parsing entries...")
    dict_entries, sentences = parse_all(pages)

    with open(PARSED_OUTPUT, "w", encoding="utf-8") as f:
        json.dump({"dictionary": dict_entries, "sentences": sentences}, f, ensure_ascii=False, indent=2)
    print(f"   Saved parsed data to {PARSED_OUTPUT}")

    print("\n3. Inserting into database...")
    ins_d, ins_s, total_d, total_s, total_v = insert_into_db(dict_entries, sentences)
    print(f"   New dictionary entries inserted: {ins_d}")
    print(f"   New sentences inserted: {ins_s}")

    print(f"\n{'='*60}")
    print(f"DATABASE TOTALS (after OCR import)")
    print(f"{'='*60}")
    print(f"  Dictionary entries:    {total_d}")
    print(f"  Parallel sentences:    {total_s}")
    print(f"  Verb conjugations:     {total_v}")
    print(f"  GRAND TOTAL:           {total_d + total_s + total_v}")


if __name__ == "__main__":
    main()
