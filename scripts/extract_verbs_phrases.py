#!/usr/bin/env python3
"""
Verb & Multi-Word Phrase Extraction from NT Verse Alignments
=============================================================
Focuses on:
  1. Verb forms: Tol verbs mapped to Spanish/English verb forms via verse alignment
  2. 2-3 word collocations: Frequent Tol multi-word units mapped to Spanish/English phrases
  3. Bigram/trigram co-occurrence across aligned verses

Strategy:
  - For each aligned verse, extract Tol bigrams/trigrams
  - Check which Spanish/English bigrams/trigrams appear in the same verse
  - Score by co-occurrence frequency (Dice/PMI) across the full NT
  - For verbs: identify Spanish verbs via common conjugation suffixes,
    then map co-occurring Tol words to those verb forms
"""

import re
import math
import sqlite3
import time
from pathlib import Path
from collections import Counter, defaultdict
from html import unescape

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
    "a", "al", "ante", "con", "de", "del", "desde", "e", "el", "ella",
    "ellos", "en", "entre", "es", "esa", "ese", "esta", "este", "fue",
    "ha", "hasta", "la", "las", "le", "les", "lo", "los", "me", "más",
    "ni", "no", "nos", "o", "para", "pero", "por", "que", "se", "si",
    "sin", "su", "sus", "un", "una", "y", "ya", "él",
}

STOPWORDS_EN = {
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by",
    "did", "do", "for", "from", "had", "has", "have", "he", "her",
    "him", "his", "i", "if", "in", "into", "is", "it", "its", "me",
    "my", "no", "not", "of", "on", "or", "our", "she", "so", "than",
    "that", "the", "their", "them", "then", "there", "they", "this",
    "to", "up", "us", "was", "we", "were", "what", "which", "who",
    "will", "with", "you", "your",
}

STOPWORDS_TOL = {"la", "na", "ne", "nin", "ca", "wa", "way", "ya", "ma", "mpes", "jis"}

# Spanish verb conjugation endings (for identifying verbs)
SPA_VERB_SUFFIXES = [
    "aba", "aban", "ado", "amos", "ando", "ará", "arán", "aron", "ando",
    "emos", "endo", "eron", "ía", "ían", "ido", "iendo", "ieron",
    "imos", "irá", "irán", "iste", "ó", "ás", "éis",
]

ENG_VERB_PATTERNS = re.compile(
    r'\b\w+(ed|ing|es|ied|ying)\b', re.I
)


def parse_verses(html_path):
    text = html_path.read_text(encoding="utf-8")
    text = text.replace("&#160;", " ").replace("&nbsp;", " ")
    text = unescape(text)
    text = re.sub(r'<span class="popup">.*?</span>', '', text)
    text = re.sub(r'<a[^>]*class="notemark"[^>]*>.*?</a>', '', text)
    text = re.sub(r"<span class=['\"]add['\"]>(.*?)</span>", r"\1", text)
    verses = {}
    for m in re.finditer(
        r'<span\s+class="verse"\s+id="V(\d+)">\s*\d+\s*</span>(.*?)'
        r'(?=<span\s+class="verse"|<ul\s|<div\s+class=[\'"](?:footnote|copyright|tnav|s\b|r\b))',
        text, re.DOTALL
    ):
        vnum = int(m.group(1))
        clean = re.sub(r'<[^>]+>', ' ', m.group(2))
        clean = re.sub(r'\s+', ' ', clean).strip()
        if clean:
            verses[vnum] = clean
    return verses


def tokenize(text):
    return [w for w in re.findall(r"[a-záéíóúüñ'']+", text.lower()) if len(w) > 1]


def get_ngrams(tokens, n):
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def is_spanish_verb(word):
    word = word.lower()
    for s in SPA_VERB_SUFFIXES:
        if word.endswith(s) and len(word) > len(s) + 2:
            return True
    return False


def parse_full_nt():
    tol_all, eng_all, spa_all = {}, {}, {}
    for book in NT_BOOKS:
        for ch in range(0, 200):
            tol_f = TOL_NT / f"{book}{ch:02d}.htm"
            if not tol_f.exists():
                continue
            tol_v = parse_verses(tol_f)
            if not tol_v:
                continue
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
    return tol_all, eng_all, spa_all


def load_proper_nouns(conn):
    proper = set()
    has = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inferred_translations'").fetchone()
    # Just use a simple set of biblical names
    proper |= {
        "dios", "jesús", "jesucristo", "cristo", "maría", "josé",
        "god", "jesus", "christ", "mary", "joseph", "lord", "señor",
        "espíritu", "santo", "spirit", "holy", "angel", "ángel",
        "moisés", "moses", "israel", "jerusalem", "jerusalén",
        "pablo", "paul", "pedro", "peter", "simon", "simón",
        "juan", "john", "santiago", "james", "judas", "david",
        "abraham", "yomen", "cjües",
    }
    return proper


def build_verb_and_phrase_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS phrase_translations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tol_phrase TEXT NOT NULL,
            spanish_phrase TEXT,
            english_phrase TEXT,
            phrase_type TEXT,
            cooccur INTEGER DEFAULT 0,
            score REAL DEFAULT 0,
            source TEXT,
            UNIQUE(tol_phrase, spanish_phrase, english_phrase)
        );
        CREATE INDEX IF NOT EXISTS idx_phr_tol ON phrase_translations(tol_phrase);
        CREATE INDEX IF NOT EXISTS idx_phr_spa ON phrase_translations(spanish_phrase);
        CREATE INDEX IF NOT EXISTS idx_phr_eng ON phrase_translations(english_phrase);
        CREATE INDEX IF NOT EXISTS idx_phr_type ON phrase_translations(phrase_type);
    """)


def main():
    t0 = time.time()
    conn = sqlite3.connect(str(DB_PATH))

    print("=" * 70)
    print("  VERB & PHRASE EXTRACTION FROM NT ALIGNMENTS")
    print("=" * 70)

    build_verb_and_phrase_tables(conn)

    print("\n[1] Parsing NTs...")
    tol_all, eng_all, spa_all = parse_full_nt()
    keys = set(tol_all.keys()) & set(spa_all.keys()) & set(eng_all.keys())
    print(f"  Triple-aligned verses: {len(keys):,}")

    proper = load_proper_nouns(conn)

    # Load existing known single words to filter
    known_tol = set()
    for row in conn.execute("SELECT tol FROM dictionary"):
        known_tol.add(row[0].lower().strip())

    # ── Extract bigrams & trigrams co-occurrences ──
    print("\n[2] Building ngram co-occurrence matrices...")

    # Tol bigrams ↔ Spanish unigrams/bigrams
    tol_bi_spa = Counter()  # (tol_bigram, spa_word_or_bigram) -> count
    tol_bi_eng = Counter()
    tol_tri_spa = Counter()
    tol_tri_eng = Counter()
    tol_bi_freq = Counter()
    tol_tri_freq = Counter()
    spa_word_freq = Counter()
    eng_word_freq = Counter()
    spa_bi_freq = Counter()
    eng_bi_freq = Counter()

    # Also: Tol unigram ↔ Spanish verb forms
    tol_spa_verb = Counter()
    spa_verb_freq = Counter()
    tol_eng_verb = Counter()
    eng_verb_freq = Counter()

    for key in keys:
        tol_tokens = [w for w in tokenize(tol_all[key]) if w not in STOPWORDS_TOL and w not in proper and len(w) > 2]
        spa_tokens = [w for w in tokenize(spa_all[key]) if w not in STOPWORDS_ES and w not in proper and len(w) > 2]
        eng_tokens = [w for w in tokenize(eng_all[key]) if w not in STOPWORDS_EN and w not in proper and len(w) > 2]

        tol_bigrams = set(get_ngrams(tol_tokens, 2))
        tol_trigrams = set(get_ngrams(tol_tokens, 3))
        spa_words = set(spa_tokens)
        eng_words = set(eng_tokens)
        spa_bigrams = set(get_ngrams(spa_tokens, 2))
        eng_bigrams = set(get_ngrams(eng_tokens, 2))

        # Verb detection
        spa_verbs = {w for w in spa_words if is_spanish_verb(w)}
        eng_verbs = {w for w in eng_words if ENG_VERB_PATTERNS.match(w)}

        for tb in tol_bigrams:
            tol_bi_freq[tb] += 1
            for sw in spa_words:
                tol_bi_spa[(tb, sw)] += 1
            for ew in eng_words:
                tol_bi_eng[(tb, ew)] += 1
            for sb in spa_bigrams:
                tol_bi_spa[(tb, sb)] += 1
            for eb in eng_bigrams:
                tol_bi_eng[(tb, eb)] += 1

        for tt in tol_trigrams:
            tol_tri_freq[tt] += 1
            for sw in spa_words:
                tol_tri_spa[(tt, sw)] += 1
            for ew in eng_words:
                tol_tri_eng[(tt, ew)] += 1

        for sw in spa_words:
            spa_word_freq[sw] += 1
        for ew in eng_words:
            eng_word_freq[ew] += 1
        for sb in spa_bigrams:
            spa_bi_freq[sb] += 1
        for eb in eng_bigrams:
            eng_bi_freq[eb] += 1

        # Tol unigrams ↔ verb forms
        tol_unis = set(tol_tokens)
        for tw in tol_unis:
            for sv in spa_verbs:
                tol_spa_verb[(tw, sv)] += 1
                spa_verb_freq[sv] += 1
            for ev in eng_verbs:
                tol_eng_verb[(tw, ev)] += 1
                eng_verb_freq[ev] += 1

    n_verses = len(keys)
    print(f"  Tol bigrams seen: {len(tol_bi_freq):,}")
    print(f"  Tol trigrams seen: {len(tol_tri_freq):,}")

    # ── Score Tol bigrams ↔ Spanish/English ──
    print("\n[3] Scoring bigram alignments...")

    def score_ngram_pairs(cooccur_map, tol_freq_map, other_freq_map, min_co=3):
        results = []
        for (tol_ng, other), count in cooccur_map.items():
            if count < min_co:
                continue
            tf = tol_freq_map[tol_ng]
            of = other_freq_map.get(other, 0) or 1
            dice = (2 * count) / (tf + of)
            if dice < 0.15:
                continue
            freq_ratio = min(tf, of) / max(tf, of) if max(tf, of) > 0 else 0
            score = dice * 0.6 + freq_ratio * 0.4
            results.append({
                "tol": tol_ng, "other": other, "cooccur": count,
                "dice": round(dice, 4), "score": round(score, 4),
            })
        results.sort(key=lambda x: x["score"], reverse=True)
        return results

    # Merge spa word and bigram freqs
    spa_all_freq = {**spa_word_freq, **spa_bi_freq}
    eng_all_freq = {**eng_word_freq, **eng_bi_freq}

    bi_spa = score_ngram_pairs(tol_bi_spa, tol_bi_freq, spa_all_freq, min_co=3)
    bi_eng = score_ngram_pairs(tol_bi_eng, tol_bi_freq, eng_all_freq, min_co=3)
    tri_spa = score_ngram_pairs(tol_tri_spa, tol_tri_freq, spa_word_freq, min_co=2)
    tri_eng = score_ngram_pairs(tol_tri_eng, tol_tri_freq, eng_word_freq, min_co=2)

    print(f"  Tol bigram → Spanish candidates: {len(bi_spa):,}")
    print(f"  Tol bigram → English candidates: {len(bi_eng):,}")
    print(f"  Tol trigram → Spanish candidates: {len(tri_spa):,}")
    print(f"  Tol trigram → English candidates: {len(tri_eng):,}")

    # ── Score verb alignments ──
    print("\n[4] Scoring verb alignments...")
    tol_word_freq = Counter()
    for key in keys:
        for w in tokenize(tol_all[key]):
            if w not in STOPWORDS_TOL and len(w) > 2:
                tol_word_freq[w] += 1

    verb_spa = score_ngram_pairs(tol_spa_verb, tol_word_freq, spa_verb_freq, min_co=3)
    verb_eng = score_ngram_pairs(tol_eng_verb, tol_word_freq, eng_verb_freq, min_co=3)
    print(f"  Tol → Spanish verb candidates: {len(verb_spa):,}")
    print(f"  Tol → English verb candidates: {len(verb_eng):,}")

    # ── Deduplicate: keep best match per Tol phrase ──
    print("\n[5] Deduplicating and selecting best matches...")

    def best_per_tol(scored_list, top_n=3):
        by_tol = defaultdict(list)
        for s in scored_list:
            by_tol[s["tol"]].append(s)
        results = []
        for tol_key, entries in by_tol.items():
            entries.sort(key=lambda x: x["score"], reverse=True)
            for e in entries[:top_n]:
                results.append(e)
        return results

    bi_spa_best = best_per_tol(bi_spa, 2)
    bi_eng_best = best_per_tol(bi_eng, 2)
    tri_spa_best = best_per_tol(tri_spa, 2)
    tri_eng_best = best_per_tol(tri_eng, 2)
    verb_spa_best = best_per_tol(verb_spa, 2)
    verb_eng_best = best_per_tol(verb_eng, 2)

    # ── Insert into database ──
    print("\n[6] Inserting into database...")

    def insert_phrases(entries, lang, phrase_type, source):
        count = 0
        for e in entries:
            spa = e["other"] if lang == "spanish" else ""
            eng = e["other"] if lang == "english" else ""
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO phrase_translations (tol_phrase, spanish_phrase, english_phrase, phrase_type, cooccur, score, source) VALUES (?,?,?,?,?,?,?)",
                    (e["tol"], spa, eng, phrase_type, e["cooccur"], e["score"], source)
                )
                count += 1
            except:
                pass
        return count

    n = 0
    n += insert_phrases(bi_spa_best, "spanish", "bigram", "nt_bigram")
    n += insert_phrases(bi_eng_best, "english", "bigram", "nt_bigram")
    n += insert_phrases(tri_spa_best, "spanish", "trigram", "nt_trigram")
    n += insert_phrases(tri_eng_best, "english", "trigram", "nt_trigram")
    n += insert_phrases(verb_spa_best, "spanish", "verb", "nt_verb")
    n += insert_phrases(verb_eng_best, "english", "verb", "nt_verb")

    # Also insert verb pairs into verb_conjugations table
    verb_conj_added = 0
    for e in verb_spa_best:
        if e["score"] >= 0.2:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO verb_conjugations (tol_form, spanish_form, english_form, base_verb_tol, base_verb_spanish, tense, person) VALUES (?,?,?,?,?,?,?)",
                    (e["tol"], e["other"], "", "", "", "aligned", "")
                )
                verb_conj_added += 1
            except:
                pass
    for e in verb_eng_best:
        if e["score"] >= 0.2:
            conn.execute(
                "UPDATE verb_conjugations SET english_form = ? WHERE tol_form = ? AND english_form = ''",
                (e["other"], e["tol"])
            )

    conn.commit()

    total_phrases = conn.execute("SELECT COUNT(*) FROM phrase_translations").fetchone()[0]
    total_verbs = conn.execute("SELECT COUNT(*) FROM verb_conjugations").fetchone()[0]

    print(f"  Phrase entries added: {n:,}")
    print(f"  Verb conjugations added: {verb_conj_added:,}")
    print(f"  Total phrase_translations: {total_phrases:,}")
    print(f"  Total verb_conjugations: {total_verbs:,}")

    # Samples
    print(f"\n  ── TOP BIGRAMS (Tol→Spanish): ──")
    for e in bi_spa_best[:15]:
        print(f"    {e['tol']:30s} → {e['other']:25s}  co={e['cooccur']}  score={e['score']:.3f}")

    print(f"\n  ── TOP BIGRAMS (Tol→English): ──")
    for e in bi_eng_best[:15]:
        print(f"    {e['tol']:30s} → {e['other']:25s}  co={e['cooccur']}  score={e['score']:.3f}")

    print(f"\n  ── TOP TRIGRAMS (Tol→Spanish): ──")
    for e in tri_spa_best[:10]:
        print(f"    {e['tol']:35s} → {e['other']:20s}  co={e['cooccur']}  score={e['score']:.3f}")

    print(f"\n  ── TOP VERBS (Tol→Spanish): ──")
    for e in verb_spa_best[:15]:
        print(f"    {e['tol']:22s} → {e['other']:22s}  co={e['cooccur']}  score={e['score']:.3f}")

    print(f"\n  ── TOP VERBS (Tol→English): ──")
    for e in verb_eng_best[:15]:
        print(f"    {e['tol']:22s} → {e['other']:22s}  co={e['cooccur']}  score={e['score']:.3f}")

    elapsed = time.time() - t0
    print(f"\n  Processing time: {elapsed:.1f}s")
    conn.close()

    print(f"\n{'='*70}")
    print("  DONE")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
