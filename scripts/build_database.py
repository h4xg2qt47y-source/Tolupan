#!/usr/bin/env python3
"""
Build the Tol translation SQLite database from all available data sources.
Sources:
  1. ELAN parallel transcripts (Tol-Spanish, 752 entries)
  2. NT chapter text files (317 chapters of Tol text)
  3. Hardcoded core vocabulary from elicitation data
"""

import json
import os
import re
import sqlite3
import html
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "app" / "data" / "tol.db"
ELAN_JSON = BASE / "Tol Audio" / "ELAN_Transcripts" / "tol_spanish_parallel.json"
NT_TEXT_DIR = BASE / "Tol Translation" / "NT_Text"

SPANISH_ENGLISH_CORE = {
    "cabello": "hair", "cabeza": "head", "oreja": "ear", "ojo": "eye",
    "boca": "mouth", "diente": "tooth", "lengua": "tongue", "uña": "nail",
    "pie": "foot", "pierna": "leg", "rodilla": "knee", "mano": "hand",
    "ala": "wing", "barriga": "belly", "tripa": "gut", "cuello": "neck",
    "pecho": "chest", "corazón": "heart", "beber": "to drink", "comer": "to eat",
    "chupar": "to suck", "escupir": "to spit", "vomitar": "to vomit",
    "soplar": "to blow", "respirar": "to breathe", "reír": "to laugh",
    "ver": "to see", "oír": "to hear", "saber": "to know", "pensar": "to think",
    "oler": "to smell", "temer": "to fear", "dormir": "to sleep",
    "vivir": "to live", "morir": "to die", "matar": "to kill",
    "pelear": "to fight", "golpear": "to hit", "pescar": "to fish",
    "escabar": "to dig", "nadar": "to swim", "volar": "to fly",
    "caminar": "to walk", "venir": "to come", "sentarse": "to sit",
    "levantarse": "to stand up", "caer": "to fall", "dar": "to give",
    "tener": "to have", "frotar": "to rub", "lavar": "to wash",
    "limpiar": "to clean", "tirar": "to throw", "cocer": "to cook",
    "contar": "to count", "decir": "to say", "cantar": "to sing",
    "jugar": "to play", "helar": "to freeze", "hincharse": "to swell",
    "poner": "to put", "escribir": "to write",
    "yo": "I", "tú": "you", "él": "he", "nosotros": "we",
    "ustedes": "you (pl.)", "ellos": "they",
    "mi": "my", "tu": "your", "su": "his/her",
    "agua": "water", "fuego": "fire", "tierra": "earth", "sol": "sun",
    "luna": "moon", "estrella": "star", "árbol": "tree", "piedra": "stone",
    "casa": "house", "camino": "road", "montaña": "mountain", "río": "river",
    "hombre": "man", "mujer": "woman", "niño": "child", "persona": "person",
    "gente": "people", "animal": "animal", "perro": "dog", "pájaro": "bird",
    "pescado": "fish", "maíz": "corn", "frijol": "bean",
    "grande": "big", "chiquito": "small", "bueno": "good", "malo": "bad",
    "uno": "one", "dos": "two", "tres": "three",
    "sí": "yes", "no": "no", "dónde": "where", "cómo": "how",
    "qué": "what", "quién": "who", "cuándo": "when", "por qué": "why",
    "allí": "there", "aquí": "here", "ahora": "now", "antes": "before",
    "después": "after", "día": "day", "noche": "night",
    "fantasma": "ghost", "miel": "honey", "abeja": "bee",
    "sombrero": "hat", "machete": "machete", "canasta": "basket",
    "arriba": "up", "abajo": "down", "cerca": "near", "lejos": "far",
}


def clean_html_entities(text: str) -> str:
    return html.unescape(text)


def create_tables(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS dictionary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tol TEXT NOT NULL,
            spanish TEXT NOT NULL,
            english TEXT,
            category TEXT,
            source TEXT,
            UNIQUE(tol, spanish)
        );
        CREATE TABLE IF NOT EXISTS parallel_sentences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tol TEXT NOT NULL,
            spanish TEXT NOT NULL,
            english TEXT,
            source TEXT
        );
        CREATE TABLE IF NOT EXISTS verb_conjugations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tol_form TEXT NOT NULL,
            spanish_form TEXT NOT NULL,
            english_form TEXT,
            base_verb_tol TEXT,
            base_verb_spanish TEXT,
            tense TEXT,
            person TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_dict_tol ON dictionary(tol);
        CREATE INDEX IF NOT EXISTS idx_dict_spanish ON dictionary(spanish);
        CREATE INDEX IF NOT EXISTS idx_dict_english ON dictionary(english);
        CREATE INDEX IF NOT EXISTS idx_par_tol ON parallel_sentences(tol);
        CREATE INDEX IF NOT EXISTS idx_par_spanish ON parallel_sentences(spanish);
        CREATE INDEX IF NOT EXISTS idx_verb_tol ON verb_conjugations(tol_form);
        CREATE INDEX IF NOT EXISTS idx_verb_spanish ON verb_conjugations(spanish_form);
    """)


def load_elan_data(conn: sqlite3.Connection):
    if not ELAN_JSON.exists():
        print(f"  ELAN JSON not found at {ELAN_JSON}")
        return 0, 0

    data = json.load(open(ELAN_JSON, "r", encoding="utf-8"))
    dict_count = 0
    sent_count = 0

    for entry in data:
        tol = clean_html_entities(entry["tol"]).strip()
        spanish = clean_html_entities(entry["spanish"]).strip()
        if not tol or not spanish:
            continue

        cat = entry.get("grammar_category", "")
        source = entry.get("source", "ELAN")
        english = SPANISH_ENGLISH_CORE.get(spanish.lower(), "")

        if cat in ("sustantivo", "verbo"):
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO dictionary (tol, spanish, english, category, source) VALUES (?, ?, ?, ?, ?)",
                    (tol, spanish, english, cat, source),
                )
                dict_count += 1
            except sqlite3.IntegrityError:
                pass
        else:
            conn.execute(
                "INSERT INTO parallel_sentences (tol, spanish, english, source) VALUES (?, ?, ?, ?)",
                (tol, spanish, english, source),
            )
            sent_count += 1

    conn.commit()
    return dict_count, sent_count


def load_verb_conjugations(conn: sqlite3.Connection):
    """Extract structured verb conjugations from the Aurelio elicitation data."""
    data = json.load(open(ELAN_JSON, "r", encoding="utf-8"))

    person_map = {
        "yo": "1s", "tú": "2s", "él": "3s",
        "nosotros": "1p", "ustedes": "2p", "ellos": "3p",
    }
    tense_markers = {
        "digo": ("decir", "present"), "digas": ("decir", "present"),
        "dice": ("decir", "present"), "decimos": ("decir", "present"),
        "dicen": ("decir", "present"),
        "canto": ("cantar", "present"), "cantas": ("cantar", "present"),
        "canta": ("cantar", "present"), "cantamos": ("cantar", "present"),
        "cantan": ("cantar", "present"),
        "canté": ("cantar", "past"), "cantaste": ("cantar", "past"),
        "cantó": ("cantar", "past"),
        "cantaré": ("cantar", "future"), "cantarás": ("cantar", "future"),
        "cantará": ("cantar", "future"), "cantaremos": ("cantar", "future"),
        "cantarán": ("cantar", "future"),
        "oigo": ("oír", "present"), "oyes": ("oír", "present"),
        "oye": ("oír", "present"), "oímos": ("oír", "present"),
        "oyen": ("oír", "present"),
        "oí": ("oír", "past"), "oíste": ("oír", "past"),
        "oyó": ("oír", "past"), "oyeron": ("oír", "past"),
        "oiré": ("oír", "future"), "oirás": ("oír", "future"),
        "oirá": ("oír", "future"), "oiremos": ("oír", "future"),
        "oirán": ("oír", "future"),
        "como": ("comer", "present"), "comes": ("comer", "present"),
        "come": ("comer", "present"), "comemos": ("comer", "present"),
        "comen": ("comer", "present"),
        "comí": ("comer", "past"), "comiste": ("comer", "past"),
        "comió": ("comer", "past"), "comimos": ("comer", "past"),
        "comieron": ("comer", "past"),
        "comeré": ("comer", "future"), "comerás": ("comer", "future"),
        "comerá": ("comer", "future"), "comeremos": ("comer", "future"),
        "comerán": ("comer", "future"),
        "pongo": ("poner", "present"), "pones": ("poner", "present"),
        "pone": ("poner", "present"), "ponemos": ("poner", "present"),
        "ponen": ("poner", "present"),
        "puse": ("poner", "past"), "pusiste": ("poner", "past"),
        "puso": ("poner", "past"), "pusimos": ("poner", "past"),
        "pusieron": ("poner", "past"),
        "pondré": ("poner", "future"), "pondrás": ("poner", "future"),
        "pondrá": ("poner", "future"), "pondremos": ("poner", "future"),
        "pondrán": ("poner", "future"),
    }

    count = 0
    for entry in data:
        if entry.get("source") != "Elicited_Grammar_Aurelio":
            continue
        cat = entry.get("grammar_category", "")
        if cat != "verbo":
            continue

        tol = clean_html_entities(entry["tol"]).strip()
        spanish = clean_html_entities(entry["spanish"]).strip()
        spa_lower = spanish.lower().strip()

        person = ""
        for p_spa, p_code in person_map.items():
            if spa_lower.startswith(p_spa + " "):
                person = p_code
                spa_lower = spa_lower[len(p_spa) + 1:].strip()
                break

        base_verb_spa = ""
        tense = ""
        for form, (verb, t) in tense_markers.items():
            if form in spa_lower:
                base_verb_spa = verb
                tense = t
                break

        if not tense:
            tense = "infinitive" if spa_lower.startswith("to ") or not person else "present"

        conn.execute(
            "INSERT INTO verb_conjugations (tol_form, spanish_form, english_form, base_verb_tol, base_verb_spanish, tense, person) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tol, spanish, "", "", base_verb_spa, tense, person),
        )
        count += 1

    conn.commit()
    return count


def load_nt_sample_sentences(conn: sqlite3.Connection):
    """Load a sample of NT text as Tol-only sentences for reference."""
    if not NT_TEXT_DIR.exists():
        return 0

    count = 0
    for htm_file in sorted(NT_TEXT_DIR.glob("*.htm")):
        if htm_file.name.endswith("00.htm") or not re.match(r"[A-Z0-9]+\d+\.htm", htm_file.name):
            continue
        try:
            text = htm_file.read_text(encoding="utf-8")
            verses = re.findall(r'class="verse[^"]*"[^>]*>(.*?)</span>', text, re.DOTALL)
            for verse in verses[:3]:
                clean = re.sub(r"<[^>]+>", "", verse).strip()
                clean = re.sub(r"\s+", " ", clean)
                if len(clean) > 10:
                    conn.execute(
                        "INSERT INTO parallel_sentences (tol, spanish, english, source) VALUES (?, ?, ?, ?)",
                        (clean, "", "", f"NT:{htm_file.stem}"),
                    )
                    count += 1
        except Exception:
            continue

    conn.commit()
    return count


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    print("Creating tables...")
    create_tables(conn)

    print("Loading ELAN parallel data...")
    d, s = load_elan_data(conn)
    print(f"  Dictionary entries: {d}")
    print(f"  Parallel sentences: {s}")

    print("Loading verb conjugations...")
    v = load_verb_conjugations(conn)
    print(f"  Verb forms: {v}")

    print("Loading NT sample sentences...")
    n = load_nt_sample_sentences(conn)
    print(f"  NT verse samples: {n}")

    row = conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()
    print(f"\nTotal dictionary entries: {row[0]}")
    row = conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()
    print(f"Total parallel sentences: {row[0]}")
    row = conn.execute("SELECT COUNT(*) FROM verb_conjugations").fetchone()
    print(f"Total verb conjugations: {row[0]}")

    conn.close()
    print(f"\nDatabase saved to: {DB_PATH}")


if __name__ == "__main__":
    main()
