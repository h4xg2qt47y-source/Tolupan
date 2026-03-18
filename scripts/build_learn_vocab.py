#!/usr/bin/env python3
"""
Build the curated Learn vocabulary database.
Only uses VERIFIED translations — no statistical inferences.

Sources:
  - SIL Dennis & Dennis 1983 Dictionary (OCR)
  - Elicited Grammar with Aurelio (native speaker)
  - PDF grammar/vocabulary extractions
  - Verb conjugation paradigms

Output: app/data/learn_vocab.json
"""

import json
import re
import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "app" / "data" / "tol.db"
PDF_VOCAB = BASE / "Tol Pronunciation" / "extracted_text" / "tol_vocabulary.json"
PARSED_DICT = BASE / "scripts" / "parsed_dictionary.json"
OUTPUT = BASE / "app" / "data" / "learn_vocab.json"

CATEGORY_MAP = {
    "sustantivo": "noun",
    "verbo": "verb",
    "adjetivo": "adjective",
    "adverbio": "adverb",
    "preposición": "preposition",
    "conjunción": "conjunction",
    "pronombre": "pronoun",
    "pron per": "pronoun",
    "pron pos": "pronoun",
    "pron ind": "pronoun",
    "pron dem": "pronoun",
    "pron int": "pronoun",
    "s pos": "noun",
}

SEMANTIC_KEYWORDS = {
    "animals": [
        "perro", "gato", "caballo", "gallina", "gallo", "vaca", "toro", "pájaro", "ave",
        "pez", "pescado", "serpiente", "culebra", "mono", "tigre", "jaguar", "venado",
        "cerdo", "conejo", "ratón", "rata", "tortuga", "iguana", "rana", "sapo",
        "mosca", "mariposa", "abeja", "hormiga", "araña", "zancudo", "mosquito",
        "garrobo", "zorro", "danto", "tapir", "coyote", "ardilla", "loro",
        "tucán", "guara", "búho", "lechuza", "gavilán", "águila", "murciélago",
        "cucaracha", "insect", "animal", "frog", "bird", "fish", "snake", "dog",
        "cat", "horse", "hen", "chicken", "cow", "bull", "monkey", "deer", "pig",
        "rabbit", "mouse", "turtle", "iguana", "ant", "bee", "butterfly", "spider",
        "bat", "owl", "eagle", "parrot", "fly", "mosquito",
    ],
    "body": [
        "cabeza", "ojo", "oreja", "nariz", "boca", "diente", "lengua", "pelo",
        "cabello", "mano", "brazo", "pie", "pierna", "dedo", "uña", "corazón",
        "pecho", "estómago", "espalda", "cuello", "rodilla", "codo", "hombro",
        "sangre", "hueso", "piel", "carne", "head", "eye", "ear", "nose", "mouth",
        "tooth", "tongue", "hair", "hand", "arm", "foot", "leg", "finger", "nail",
        "heart", "chest", "stomach", "back", "neck", "knee", "elbow", "shoulder",
        "blood", "bone", "skin", "flesh", "lip", "face",
    ],
    "family": [
        "madre", "padre", "hijo", "hija", "hermano", "hermana", "abuelo", "abuela",
        "tío", "tía", "primo", "prima", "esposo", "esposa", "mujer", "hombre",
        "niño", "niña", "bebé", "suegro", "suegra", "cuñado", "cuñada",
        "mother", "father", "son", "daughter", "brother", "sister", "grandfather",
        "grandmother", "uncle", "aunt", "cousin", "husband", "wife", "woman", "man",
        "boy", "girl", "baby", "child", "parent", "family",
    ],
    "food": [
        "maíz", "frijol", "arroz", "yuca", "plátano", "banano", "café", "azúcar",
        "sal", "agua", "leche", "carne", "huevo", "tortilla", "pan", "fruta",
        "mango", "aguacate", "cacao", "miel", "comida", "corn", "bean", "rice",
        "banana", "coffee", "sugar", "salt", "water", "milk", "meat", "egg",
        "bread", "fruit", "food", "honey", "chocolate",
    ],
    "nature": [
        "sol", "luna", "estrella", "cielo", "tierra", "agua", "río", "mar",
        "montaña", "cerro", "bosque", "selva", "árbol", "flor", "piedra",
        "lluvia", "viento", "nube", "fuego", "humo", "noche", "día",
        "sun", "moon", "star", "sky", "earth", "water", "river", "sea",
        "mountain", "forest", "tree", "flower", "stone", "rain", "wind",
        "cloud", "fire", "smoke", "night", "day", "wood", "leaf",
    ],
    "colors": [
        "rojo", "azul", "verde", "amarillo", "blanco", "negro", "color",
        "red", "blue", "green", "yellow", "white", "black",
    ],
    "numbers": [
        "uno", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho",
        "nueve", "diez", "once", "doce", "trece", "catorce", "quince",
        "veinte", "cien", "mil",
        "one", "two", "three", "four", "five", "six", "seven", "eight",
        "nine", "ten", "eleven", "twelve", "twenty", "hundred", "thousand",
    ],
    "house": [
        "casa", "puerta", "techo", "pared", "piso", "cocina", "cama",
        "mesa", "silla", "hamaca", "olla", "jícara", "machete", "cuchillo",
        "house", "door", "roof", "wall", "floor", "kitchen", "bed",
        "table", "chair", "pot", "knife",
    ],
    "greetings": [
        "hola", "adiós", "buenos", "buenas", "gracias",
        "hello", "goodbye", "thanks",
    ],
}

# Difficulty: 1=basic/concrete/short, 2=intermediate, 3=advanced/abstract
def assign_difficulty(tol, spanish, english, semantic_cat, pos):
    text = f"{spanish or ''} {english or ''}".lower()
    if semantic_cat in ("numbers", "colors", "greetings", "family"):
        return 1
    if semantic_cat in ("body", "animals", "food"):
        if len(tol.split()) <= 2:
            return 1
        return 2
    if semantic_cat in ("nature", "house"):
        return 2
    if pos == "verb":
        return 2
    if len(tol.split()) > 3:
        return 3
    return 2


def classify_semantic(spanish, english):
    text = f"{spanish or ''} {english or ''}".lower()
    words = set(re.findall(r'[a-záéíóúñü]+', text))
    for cat, keywords in SEMANTIC_KEYWORDS.items():
        for kw in keywords:
            if kw in words:
                return cat
    return "other"


def normalize_pos(raw_cat):
    if not raw_cat:
        return "other"
    raw = raw_cat.strip().lower()
    if raw in CATEGORY_MAP:
        return CATEGORY_MAP[raw]
    if "verb" in raw or raw.startswith("v"):
        return "verb"
    if "adj" in raw or raw.startswith("ady"):
        return "adjective"
    if "adv" in raw:
        return "adverb"
    if "pron" in raw:
        return "pronoun"
    return "noun"


def clean_tol(t):
    if not t:
        return ""
    t = t.strip()
    t = re.sub(r'\s+', ' ', t)
    t = re.sub(r'^[\d\.\)\-]+\s*', '', t)
    return t


def clean_spanish(s):
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r'\s+', ' ', s)
    return s


def is_valid_word(tol, spanish):
    if not tol or not spanish:
        return False
    if len(tol) < 2 or len(spanish) < 2:
        return False
    if len(tol) > 60 or len(spanish) > 80:
        return False
    if re.search(r'[0-9]{3,}', tol):
        return False
    if tol.lower() == spanish.lower():
        if tol.lower() not in ("café", "animal"):
            return False
    if re.match(r'^[\(\)\[\]]+$', tol.strip()):
        return False
    if tol.startswith("(") and tol.endswith(")") and " " not in tol:
        return False
    # Filter Spanish/English sentences misidentified as Tol words
    spanish_indicators = {"el", "la", "los", "las", "de", "del", "en", "es", "un", "una",
                          "que", "por", "con", "para", "cuando", "muy", "trajo", "puso",
                          "se", "yo", "tu", "su", "al", "lo", "nos", "les", "ustedes",
                          "muchos", "oiga", "llevarán", "congregaron",
                          "the", "is", "he", "she", "it", "and", "was", "his", "her"}
    tol_words_set = set(tol.lower().split())
    overlap = tol_words_set & spanish_indicators
    if len(overlap) >= 2:
        return False
    if len(tol.split()) > 3 and len(overlap) >= 1:
        return False
    # Entries starting with "(m)" are OCR artifacts from the dictionary
    if tol.strip().startswith("(m)") or tol.strip().startswith("(f)"):
        return False
    return True


def main():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    vocab = {}  # keyed by normalized tol form

    # Source 1: DB dictionary (SIL + Aurelio)
    c = db.cursor()
    c.execute("""SELECT tol, spanish, english, category, source FROM dictionary 
                 WHERE source IN ('SIL_Dictionary_OCR', 'Elicited_Grammar_Aurelio')""")
    for row in c.fetchall():
        tol = clean_tol(row['tol'])
        spanish = clean_spanish(row['spanish'])
        english = (row['english'] or '').strip()
        if not is_valid_word(tol, spanish):
            continue
        key = tol.lower()
        if key not in vocab:
            pos = normalize_pos(row['category'])
            semantic = classify_semantic(spanish, english)
            vocab[key] = {
                "tol": tol,
                "spanish": spanish,
                "english": english,
                "pos": pos,
                "semantic": semantic,
                "sources": [row['source']],
            }
        else:
            if row['source'] not in vocab[key]['sources']:
                vocab[key]['sources'].append(row['source'])
            if not vocab[key]['english'] and english:
                vocab[key]['english'] = english

    print(f"After DB dictionary: {len(vocab)} entries")

    # Source 2: Direct EN-TOL (dictionary_direct + grammar_pdf_verified)
    c.execute("""SELECT english, tol, spanish FROM direct_en_tol 
                 WHERE source IN ('dictionary_direct', 'grammar_pdf_verified')""")
    for row in c.fetchall():
        tol = clean_tol(row['tol'])
        spanish = clean_spanish(row['spanish'] or '')
        english = (row['english'] or '').strip()
        if not tol or len(tol) < 2:
            continue
        key = tol.lower()
        if key not in vocab:
            if not spanish and not english:
                continue
            semantic = classify_semantic(spanish, english)
            vocab[key] = {
                "tol": tol,
                "spanish": spanish,
                "english": english,
                "pos": "other",
                "semantic": semantic,
                "sources": ["direct_en_tol"],
            }
        else:
            if not vocab[key]['english'] and english:
                vocab[key]['english'] = english
            if not vocab[key]['spanish'] and spanish:
                vocab[key]['spanish'] = spanish

    print(f"After direct_en_tol: {len(vocab)} entries")

    # Source 3: PDF extracted vocabulary
    if PDF_VOCAB.exists():
        pdf_vocab = json.loads(PDF_VOCAB.read_text())
        for entry in pdf_vocab:
            tol = clean_tol(entry.get('tol', ''))
            spanish = clean_spanish(entry.get('spanish', ''))
            english = (entry.get('english', '') or '').strip()
            if not is_valid_word(tol, spanish or english or 'x'):
                continue
            key = tol.lower()
            src = entry.get('source', 'pdf_grammar')
            if key not in vocab:
                pos = entry.get('pos', 'noun')
                if pos not in ('noun', 'verb', 'adjective', 'adverb', 'pronoun', 'number', 'phrase'):
                    pos = 'noun'
                semantic = entry.get('category', '') or classify_semantic(spanish, english)
                vocab[key] = {
                    "tol": tol,
                    "spanish": spanish,
                    "english": english,
                    "pos": pos,
                    "semantic": semantic,
                    "sources": [src],
                }
            else:
                if src not in vocab[key]['sources']:
                    vocab[key]['sources'].append(src)
                if not vocab[key]['english'] and english:
                    vocab[key]['english'] = english
                if not vocab[key]['spanish'] and spanish:
                    vocab[key]['spanish'] = spanish

    print(f"After PDF vocab: {len(vocab)} entries")

    # Source 4: Verb conjugations
    c.execute("SELECT tol_form, spanish_form, english_form, base_verb_tol, base_verb_spanish, tense, person FROM verb_conjugations")
    verb_entries = []
    for row in c.fetchall():
        tol = clean_tol(row['tol_form'])
        spanish = clean_spanish(row['spanish_form'] or '')
        english = (row['english_form'] or '').strip()
        if not tol or not (spanish or english):
            continue
        verb_entries.append({
            "tol": tol, "spanish": spanish, "english": english,
            "base_tol": row['base_verb_tol'], "base_spanish": row['base_verb_spanish'],
            "tense": row['tense'], "person": row['person'],
        })

    print(f"Verb conjugation forms: {len(verb_entries)}")

    # Build final curated list — require Spanish (target audience is Spanish-speaking kids)
    # Apply is_valid_word filter here too to catch entries added without it
    final = []
    for key, entry in vocab.items():
        tol = entry['tol']
        spanish = entry['spanish']
        english = entry['english']

        if not spanish:
            continue
        if not is_valid_word(tol, spanish):
            continue

        difficulty = assign_difficulty(tol, spanish, english, entry['semantic'], entry['pos'])

        final.append({
            "id": len(final) + 1,
            "tol": tol,
            "spanish": spanish,
            "english": english,
            "pos": entry['pos'],
            "category": entry['semantic'],
            "difficulty": difficulty,
            "sources": entry['sources'],
            "word_count": len(tol.split()),
        })

    # Sort: difficulty 1 first, then by category, then alphabetically
    final.sort(key=lambda e: (e['difficulty'], e['category'], e['tol']))

    # Re-number IDs
    for i, entry in enumerate(final):
        entry['id'] = i + 1

    # Build verb conjugation section
    verb_conj = []
    for ve in verb_entries:
        verb_conj.append({
            "tol": ve['tol'],
            "spanish": ve['spanish'],
            "english": ve['english'],
            "base_tol": ve['base_tol'],
            "base_spanish": ve['base_spanish'],
            "tense": ve['tense'],
            "person": ve['person'],
        })

    output = {
        "vocabulary": final,
        "verb_conjugations": verb_conj,
        "stats": {
            "total_words": len(final),
            "total_verb_forms": len(verb_conj),
            "by_difficulty": {
                1: sum(1 for e in final if e['difficulty'] == 1),
                2: sum(1 for e in final if e['difficulty'] == 2),
                3: sum(1 for e in final if e['difficulty'] == 3),
            },
            "by_category": {},
        }
    }

    cats = {}
    for e in final:
        cat = e['category']
        cats[cat] = cats.get(cat, 0) + 1
    output['stats']['by_category'] = dict(sorted(cats.items(), key=lambda x: -x[1]))

    OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=1))

    print(f"\n{'='*50}")
    print(f"CURATED LEARN VOCABULARY")
    print(f"{'='*50}")
    print(f"Total words:           {len(final)}")
    print(f"  Difficulty 1 (easy): {output['stats']['by_difficulty'][1]}")
    print(f"  Difficulty 2 (med):  {output['stats']['by_difficulty'][2]}")
    print(f"  Difficulty 3 (hard): {output['stats']['by_difficulty'][3]}")
    print(f"Verb conjugations:     {len(verb_conj)}")
    print(f"\nBy category:")
    for cat, cnt in output['stats']['by_category'].items():
        print(f"  {cat:15s}: {cnt}")
    print(f"\nOutput: {OUTPUT}")


if __name__ == "__main__":
    main()
