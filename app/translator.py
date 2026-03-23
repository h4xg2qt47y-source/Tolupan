"""
Tol Translation Engine — Fully Local / Offline
Handles translation between Tol, Spanish, and English using:
  - Direct English→Tol dictionary (22K+ entries, no Spanish hop)
  - Dictionary lookups (Tol↔Spanish, ~5.5K entries)
  - Local English↔Spanish dictionary (249K+ entries)
  - Synonym-inferred translations (445K+ connective paths)
  - Tol grammar engine (SOV word order, pronoun system, negation, postpositions)
  - Parallel corpus fuzzy matching (10K+ aligned sentences)
  - Verb conjugation matching
  - Chain translation fallback: Tol↔Spanish↔English
"""

import re
import sqlite3
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional
from collections import defaultdict

DB_PATH = Path(__file__).parent / "data" / "tol.db"


# ── Tol Phonetic Normalization ────────────────────────────────────────────
# Tol has no standardized orthography. Multiple sources use different
# conventions for the same sounds. This normalizer collapses spelling
# variants into a canonical phonetic form for comparison.

def tol_phonetic_normalize(text: str) -> str:
    """Normalize Tol text to a canonical phonetic form for fuzzy comparison."""
    s = text.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = re.sub(r"[\u0300-\u036f]", "", s)  # strip accents
    s = unicodedata.normalize("NFC", s)
    s = s.replace("-", "").replace("=", "")
    s = s.replace("'", "").replace("\u2018", "").replace("\u2019", "")
    s = s.replace("\u0027", "").replace("\u02bc", "")
    # Consonant equivalences (multi-char before single-char)
    s = s.replace("kh", "c")
    s = s.replace("ph", "p")
    s = s.replace("th", "t")
    s = s.replace("tj", "ch")
    s = s.replace("qu", "c")
    s = s.replace("k", "c")
    # Nasal prefix equivalence: word-initial m/n are interchangeable
    # Handle at word level below
    s = re.sub(r"ü", "u", s)
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Normalize m/n prefix at word boundaries
    words = s.split()
    normalized = []
    for w in words:
        if len(w) > 1 and w[0] in ("m", "n") and w[1] not in "aeiou":
            w = "n" + w[1:]
        normalized.append(w)
    return " ".join(normalized)


def tol_phonetic_word_set(text: str) -> set:
    """Split Tol text into a set of phonetically normalized words."""
    return set(tol_phonetic_normalize(text).split())


def tol_phonetic_score(expected: str, actual: str) -> float:
    """Score overlap between expected and actual Tol using phonetic normalization.
    Returns 0.0-1.0 based on normalized word overlap.
    Also gives partial credit for stem matches (e.g. vele vs velecha)."""
    exp_words = tol_phonetic_word_set(expected)
    act_words = tol_phonetic_word_set(actual)
    act_words = {w for w in act_words if len(w) > 1}
    if not exp_words:
        return 0.0
    # Exact word matches
    overlap = exp_words & act_words
    # Partial stem matches: if an expected word starts with / is contained in an actual word
    unmatched_exp = exp_words - overlap
    unmatched_act = act_words - overlap
    stem_matches = 0
    for ew in list(unmatched_exp):
        if len(ew) < 3:
            continue
        for aw in unmatched_act:
            if len(aw) < 3:
                continue
            if ew in aw or aw in ew:
                stem_matches += 0.5
                unmatched_act.discard(aw)
                break
    return min(1.0, (len(overlap) + stem_matches) / len(exp_words))

# ── Tol Grammar Constants ──────────────────────────────────────────────────

TOL_PRONOUNS = {
    "i": "napj", "me": "napj",
    "you": "nun",
    "he": "jupj", "him": "jupj",
    "she": "jupj", "her": "jupj",
    "it": "jupj",
    "we": "cupj", "us": "cupj",
    "they": "yupj", "them": "yupj",
}

# Map subject pronouns to person codes for verb conjugation
EN_PRONOUN_PERSON = {
    "i": "1sg", "me": "1sg",
    "you": "2sg",
    "he": "3sg", "him": "3sg", "she": "3sg", "her": "3sg", "it": "3sg",
    "we": "1pl", "us": "1pl",
    "they": "3pl", "them": "3pl",
}

# Possessive prefixes — phonologically conditioned
# 1sg before p,b → m-; before k,g,t,d → n-; before glottal/h/m/w/n/l/s/y/vowels → na-
# 3sg before m,w,p,b,l → ho-; before k,g,t,'(glottal),h,vowels → hu-
TOL_POSSESSIVES = {
    "my": "na-", "mine": "na-",
    "your": "nu-", "yours": "nu-",
    "his": "jupj", "her": "jupj", "its": "jupj",
    "our": "cupj", "ours": "cupj",
    "their": "yupj", "theirs": "yupj",
}

_1SG_M = set("pb")
_1SG_N = set("kgtd")
_3SG_HO = set("mwpbl")


def tol_possessive_prefix(en_possessive: str, tol_stem: str) -> str:
    """Select the correct possessive form based on the stem's first phoneme."""
    poss = en_possessive.lower()
    first = ""
    for c in tol_stem:
        if c not in "-=_ ":
            first = c
            break
    if not first:
        return tol_stem

    if poss in ("my", "mine"):
        return "napj " + tol_stem
    if poss in ("your", "yours"):
        return "nun " + tol_stem
    # 3sg, 1pl, 3pl use standalone pronoun + noun (not prefix)
    if poss in ("his", "her", "its"):
        return "jupj " + tol_stem
    if poss in ("our", "ours"):
        return "cupj " + tol_stem
    if poss in ("their", "theirs"):
        return "yupj " + tol_stem
    prefix = TOL_POSSESSIVES.get(poss, "")
    if prefix:
        return prefix + tol_stem
    return tol_stem

TOL_POSTPOSITIONS = {
    # nt'a: goal, containment, vertical contact
    "to": "nt'a", "in": "nt'a", "at": "nt'a", "on": "nt'a",
    # mo'ó: containment, inside, temporal
    "inside": "mo'ó", "into": "mo'ó",
    # lal: comitative, recipient, comparative standard
    "with": "lal", "together": "lal", "than": "lal",
    # mpes: cause, purpose, instrument
    "for": "mpes", "because": "mpes",
    # spatial
    "from": "lal",
    "under": "'alá", "below": "'alá", "beneath": "'alá",
    # "above"/"over" removed — Tol uses different constructions
    "behind": "phyapha'a",
    "after": "khüil",
    "around": "t'asiyú",
}

# Words that introduce prepositional phrases in English
EN_PREPOSITIONS = {
    "to", "in", "at", "on", "inside", "into", "with", "for", "because",
    "from", "around", "under", "below", "beneath",
    "behind", "after", "than",
}

# Tol comparative template: N1 mas ADJ N2 lal ("N1 is more ADJ than N2")
# Tol equal-to template: N1 N2 hin husta ("N1 is like N2")
TOL_COMPARATIVE_MORE = "mas"
TOL_EQUAL_LIKE = "hin husta"

# Tol copula (positive)
TOL_COPULA = "way"

# TAM clitics
TOL_IRREALIS = "ka"    # unrealized events, often with future
TOL_IMPERFECTIVE = "=cha"  # past imperfective
TOL_PLURACTIONAL = "=pan"  # repeated actions

# Subordinator
TOL_WHEN = "na"  # adverbial subordinator 'when', placed at clause end

# Polar question marker
TOL_QUESTION_MARKER = "nku"

TOL_QUESTION_WORDS = {
    "what": "tsjan", "which": "tsjan",
    "who": "pjacj", "whom": "pjacj",
    "where": "jolaj",
    "when": "'ona",
    "how": "tsjan",
    "why": "tsjan mpes",
    "how many": "nol", "how much": "nol",
}

TOL_NEGATION = "ma"
TOL_NEG_COPULA = "tulucj"

EN_STOPWORDS = {
    "a", "an", "the", "is", "are", "was", "were", "am", "be", "been",
    "being", "do", "does", "did", "will", "would", "shall", "should",
    "can", "could", "may", "might", "must", "have", "has", "had",
    "having", "of", "this",
    "there", "here", "very", "so",
    "then", "order", "more", "like", "as",
    "but", "yet", "or", "both", "either",
    "if", "although", "though", "while", "however", "therefore",
    "about", "upon", "through", "before", "between", "up", "down",
    "away", "out", "off", "back", "own", "by", "let's", "lets",
    "himself", "herself", "itself", "themselves", "ourselves", "yourself",
}

EN_NEGATION_WORDS = {
    "not", "no", "never", "neither", "nor",
    "don't", "doesn't", "didn't", "won't", "can't", "isn't", "aren't",
    "wasn't", "weren't", "haven't", "hasn't", "hadn't",
    "dont", "doesnt", "didnt", "wont", "cant", "isnt", "arent",
    "wasnt", "werent", "havent", "hasnt", "hadnt",
    "wouldn't", "couldn't", "shouldn't",
    "wouldnt", "couldnt", "shouldnt",
}

# Common English words that map to Tol function words (not in dictionary, but frequent)
EN_TO_TOL_FUNCTION = {
    "also": "wa", "even": "wa",
    "when": "na", "then": "lovin",
    "all": "pjü", "every": "pjü",
    "again": "niswá", "already": "lovin",
    "still": "más", "now": "quinam",
    "only": "p'in", "just": "p'in",
    "many": "pülücj", "much": "pülücj",
    "other": "p'a", "another": "p'a",
    "because": "mpes",
    "and": "jis",
    "people": "gente", "person": "gente",
    "things": "'yüsa", "thing": "'yüsa",
    "those": "nin", "these": "nin",
    "one": "jin", "some": "nepénowa",
    "true": "t'üc'", "truly": "t'üc'",
    "same": "nin",
    "great": "pajal", "good": "'üsüs",
}

# Relative pronouns that map to Tol "nin" when used mid-sentence
EN_RELATIVE_PRONOUNS = {"who", "whom", "which", "that"}

EN_BE_FORMS = {"is", "are", "was", "were", "am", "be", "been", "being"}


class TolTranslator:
    def __init__(self):
        self.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._build_bible_word_freq()
        self._build_lookup_caches()

    def _build_bible_word_freq(self):
        """Build a frequency index of Tol words from the Bible corpus."""
        from collections import Counter as _Counter
        rows = self.conn.execute(
            "SELECT tol FROM parallel_sentences WHERE source LIKE 'bible_align:%' AND tol IS NOT NULL AND tol != ''"
        ).fetchall()
        words = []
        for r in rows:
            words.extend(r["tol"].lower().split())
        self._tol_bible_freq = _Counter(words)

    def _tol_word_bible_score(self, tol_text: str) -> int:
        """Return sum of Bible frequencies for words in a Tol phrase."""
        total = 0
        for w in tol_text.lower().split():
            w_clean = w.strip(".,;:!?")
            total += self._tol_bible_freq.get(w_clean, 0)
        return total

    @staticmethod
    def _strip_apostrophes(text: str) -> str:
        """Remove leading/embedded apostrophes for fuzzy Tol matching."""
        return text.replace("'", "").replace("\u2019", "").replace("\u02bc", "")

    def _tol_lookup_spa(self, word: str) -> Optional[dict]:
        """Apostrophe-insensitive Tol→Spanish lookup.

        Try: exact → with leading apostrophe → stripped index.
        """
        if word in self.tol_to_spanish:
            return self.tol_to_spanish[word]
        prefixed = "'" + word
        if prefixed in self.tol_to_spanish:
            return self.tol_to_spanish[prefixed]
        stripped = self._strip_apostrophes(word)
        if stripped != word:
            return self._tol_no_apos_spa.get(stripped)
        return None

    def _tol_lookup_en(self, word: str) -> Optional[str]:
        """Apostrophe-insensitive Tol→English lookup.

        Try: exact → with leading apostrophe → stripped index.
        """
        if word in self.tol_to_english:
            return self.tol_to_english[word]
        prefixed = "'" + word
        if prefixed in self.tol_to_english:
            return self.tol_to_english[prefixed]
        stripped = self._strip_apostrophes(word)
        if stripped != word:
            return self._tol_no_apos_en.get(stripped)
        return None

    def _build_lookup_caches(self):
        self.tol_to_spanish = {}
        self.spanish_to_tol = {}
        self.tol_to_english = {}
        self.english_to_tol = {}
        self._tol_no_apos_spa = {}
        self._tol_no_apos_en = {}

        _dict_source_priority = {
            "SIL_Dictionary_OCR": 5,
            "Elicited_Grammar_Aurelio": 4,
        }
        _en_tol_source: dict[str, int] = {}
        rows = self.conn.execute("SELECT tol, spanish, english, category, source FROM dictionary").fetchall()
        for r in rows:
            tol_lower = r["tol"].lower().strip()
            spa_lower = r["spanish"].lower().strip()
            entry_spa = {"spanish": r["spanish"], "english": r["english"] or "", "category": r["category"] or ""}
            self.tol_to_spanish[tol_lower] = entry_spa
            tol_stripped = self._strip_apostrophes(tol_lower)
            if tol_stripped not in self._tol_no_apos_spa:
                self._tol_no_apos_spa[tol_stripped] = entry_spa
            self.spanish_to_tol[spa_lower] = {
                "tol": r["tol"], "english": r["english"] or "", "category": r["category"] or ""
            }
            if r["english"]:
                eng_lower = r["english"].lower().strip()
                self.tol_to_english[tol_lower] = r["english"]
                if tol_stripped not in self._tol_no_apos_en:
                    self._tol_no_apos_en[tol_stripped] = r["english"]
                src_pri = _dict_source_priority.get(r["source"] or "", 1)
                if src_pri >= _en_tol_source.get(eng_lower, 0):
                    self.english_to_tol[eng_lower] = r["tol"]
                    _en_tol_source[eng_lower] = src_pri
                for kw in self._extract_en_keywords(eng_lower):
                    if src_pri >= _en_tol_source.get(kw, 0):
                        self.english_to_tol[kw] = r["tol"]
                        _en_tol_source[kw] = src_pri

        rows = self.conn.execute("SELECT tol_form, spanish_form FROM verb_conjugations").fetchall()
        for r in rows:
            tol_lower = r["tol_form"].lower().strip()
            spa_lower = r["spanish_form"].lower().strip()
            if tol_lower not in self.tol_to_spanish:
                self.tol_to_spanish[tol_lower] = {"spanish": r["spanish_form"], "english": "", "category": "verbo"}
            if spa_lower not in self.spanish_to_tol:
                self.spanish_to_tol[spa_lower] = {"tol": r["tol_form"], "english": "", "category": "verbo"}

        # Direct English → Tol — priority: sil_dictionary > grammar_pdf > dictionary > nt_statistical > chain > inferred
        self.direct_en_tol = {}
        self.direct_en_tol_all = defaultdict(list)  # all candidates per English word
        if self._table_exists("direct_en_tol"):
            source_priority = {
                "sil_dictionary_verified": 7,
                "grammar_pdf_verified": 6, "dictionary_direct": 5,
                "nt_statistical_alignment": 4, "nt_phrase_alignment": 3,
                "en_spa_tol_chain": 2, "nt_spa_chain_alignment": 2,
                "inferred_promotion": 1,
            }
            for r in self.conn.execute("SELECT english, tol, confidence, source FROM direct_en_tol"):
                eng = r["english"].lower().strip()
                priority = source_priority.get(r["source"] or "", 0)
                bible_freq = self._tol_word_bible_score(r["tol"])
                entry = {"tol": r["tol"], "confidence": r["confidence"], "_priority": priority,
                         "source": r["source"] or "", "_bible_freq": bible_freq}
                self.direct_en_tol_all[eng].append(entry)
                existing = self.direct_en_tol.get(eng)
                if not existing:
                    self.direct_en_tol[eng] = entry
                elif priority > existing.get("_priority", 0):
                    self.direct_en_tol[eng] = entry
                elif priority == existing.get("_priority", 0):
                    if r["confidence"] > existing.get("confidence", 0):
                        self.direct_en_tol[eng] = entry
                    elif (r["confidence"] == existing.get("confidence", 0)
                          and bible_freq > existing.get("_bible_freq", 0)):
                        self.direct_en_tol[eng] = entry

            # Post-process: if top pick has 0 Bible frequency, swap with best attested alternative
            for eng, entry in list(self.direct_en_tol.items()):
                if entry.get("_bible_freq", 0) > 0:
                    continue
                candidates = self.direct_en_tol_all.get(eng, [])
                attested = [c for c in candidates if c.get("_bible_freq", 0) > 0 and c["_priority"] >= 4]
                if attested:
                    best = max(attested, key=lambda c: (c["_priority"], c["confidence"], c["_bible_freq"]))
                    self.direct_en_tol[eng] = best

        # English ↔ Spanish local dictionary
        self.eng_to_spa = defaultdict(list)
        self.spa_to_eng = defaultdict(list)
        if self._table_exists("en_es_dictionary"):
            for r in self.conn.execute("SELECT english, spanish, pos FROM en_es_dictionary").fetchall():
                eng = r["english"].lower().strip()
                spa = r["spanish"].lower().strip()
                pos = r["pos"] or ""
                self.eng_to_spa[eng].append({"spanish": spa, "pos": pos})
                self.spa_to_eng[spa].append({"english": eng, "pos": pos})

        # Verb paradigms (person-specific conjugation)
        self.verb_paradigms = {}  # english_verb → {person → tol_form}
        if self._table_exists("tol_verb_paradigms"):
            for r in self.conn.execute("SELECT english_verb, person, tol_form FROM tol_verb_paradigms"):
                verb = r["english_verb"].lower()
                if verb not in self.verb_paradigms:
                    self.verb_paradigms[verb] = {}
                self.verb_paradigms[verb][r["person"]] = r["tol_form"]

        self._en_known_verbs = self._build_english_verb_set()

        # Synonym-inferred translations
        self.inferred_en_to_tol = defaultdict(list)
        self.inferred_es_to_tol = defaultdict(list)
        self.inferred_tol_from_en = defaultdict(list)
        self.inferred_tol_from_es = defaultdict(list)

        if self._table_exists("inferred_translations"):
            for r in self.conn.execute(
                "SELECT source_word, source_lang, tol_word, confidence, path FROM inferred_translations"
            ).fetchall():
                entry = {"tol": r["tol_word"], "confidence": r["confidence"], "path": r["path"]}
                if r["source_lang"] == "en":
                    self.inferred_en_to_tol[r["source_word"].lower()].append(entry)
                    self.inferred_tol_from_en[r["tol_word"].lower()].append({
                        "english": r["source_word"], "confidence": r["confidence"], "path": r["path"]
                    })
                elif r["source_lang"] == "es":
                    self.inferred_es_to_tol[r["source_word"].lower()].append(entry)
                    self.inferred_tol_from_es[r["tol_word"].lower()].append({
                        "spanish": r["source_word"], "confidence": r["confidence"], "path": r["path"]
                    })

    @staticmethod
    def _extract_en_keywords(eng: str) -> list[str]:
        """Extract clean single keywords from a dictionary English gloss like 'capture: I capture'."""
        eng = re.sub(r"\s*\([^)]*\)\s*", " ", eng).strip().rstrip(".,;:!?")
        keywords: list[str] = []
        _stop = {"i", "he", "she", "it", "we", "they", "my", "his", "her", "its", "our", "their",
                 "the", "a", "an", "is", "are", "to", "of", "and", "or"}
        m = re.match(r"^(.+?):\s+(.+)$", eng)
        if m:
            before = m.group(1).strip().rstrip(".,;:").lower()
            after = m.group(2).strip()
            if before and len(before.split()) <= 2 and before not in _stop:
                keywords.append(before)
            vm = re.match(r"^(?:I|he|she|it|they|my|his|her)\s+(\w+)", after, re.I)
            if vm and vm.group(1).lower() not in _stop:
                keywords.append(vm.group(1).lower())
        else:
            vm = re.match(r"^I\s+(\w+)", eng, re.I)
            if vm and vm.group(1).lower() not in _stop:
                keywords.append(vm.group(1).lower())
            words = eng.split()
            if 1 <= len(words) <= 2:
                kw = eng.lower().rstrip(".,;:")
                if kw not in _stop and len(kw) > 1:
                    keywords.append(kw)
        return [k for k in dict.fromkeys(keywords) if k]

    def _table_exists(self, name: str) -> bool:
        return bool(self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone())

    # ── Word-level English → Tol lookup (multi-source) ─────────────────────

    def _lookup_en_word(self, word: str) -> Optional[dict]:
        """Look up a single English word in all available sources.
        Pronouns and grammar words are handled by the grammar engine, not here.
        """
        w = word.lower().strip()

        # Pronouns/grammar words are handled by grammar engine
        if w in TOL_PRONOUNS or w in TOL_POSSESSIVES or w in TOL_POSTPOSITIONS or w in TOL_QUESTION_WORDS:
            return None
        if w in EN_RELATIVE_PRONOUNS:
            return {"tol": "nin", "confidence": 0.7, "method": "function_word"}
        if w in EN_NEGATION_WORDS:
            return None
        if w in EN_TO_TOL_FUNCTION:
            return {"tol": EN_TO_TOL_FUNCTION[w], "confidence": 0.7, "method": "function_word"}
        if w in EN_STOPWORDS:
            return None

        if w in self.direct_en_tol:
            e = self.direct_en_tol[w]
            return {"tol": e["tol"], "confidence": e["confidence"], "method": "direct_en_tol"}

        if w in self.english_to_tol:
            return {"tol": self.english_to_tol[w], "confidence": 0.9, "method": "dictionary"}

        if w in self.inferred_en_to_tol:
            best = max(self.inferred_en_to_tol[w], key=lambda x: x["confidence"])
            if best["confidence"] >= 0.75 and self._tol_word_bible_score(best["tol"]) > 0:
                return {"tol": best["tol"], "confidence": best["confidence"], "method": "inferred"}

        return None

    # ── Tol Grammar Engine ─────────────────────────────────────────────────

    def _apply_tol_grammar(self, en_words: list, tol_parts: list, word_map: dict) -> str:
        """
        Restructure English SVO into Tol SOV using attested Tol grammar rules.

        Based on the framework from Pinhanez et al. (2024) "Harnessing AI to
        Vitalize Endangered Indigenous Languages" — apply minimal, well-attested
        grammar transformations rather than complex heuristics.

        Tol (Jicaque) grammar rules applied (Holt grammar, Dennis 1983):
          1. Basic word order: SOV (Subject – Object – Verb)
          2. Postpositions follow their noun phrase (English "in house" → "wo nt'a")
          3. Negation: ma precedes verb; tulucj for negative copula
          4. Question words: sentence-initial position
          5. Possessives: pronoun precedes possessed noun
          6. Adjectives: typically follow the noun in Tol
          7. Copula "way" for equative sentences; omitted for predicate adjectives
          8. Drop English articles (a, an, the) — Tol has no articles
        """
        en_lower = [w.lower() for w in en_words]
        n = len(en_lower)
        if n == 0:
            return ""

        is_negated = any(w in EN_NEGATION_WORDS for w in en_lower)
        has_be = any(w in EN_BE_FORMS for w in en_lower)

        # --- Phase 1: Possessive merging (before role assignment) ---
        for i, w in enumerate(en_lower):
            if w in TOL_POSSESSIVES:
                for j in range(i + 1, n):
                    if en_lower[j] in EN_STOPWORDS:
                        continue
                    tol_noun = word_map.get(j)
                    if tol_noun and not tol_noun.startswith("["):
                        word_map[j] = tol_possessive_prefix(w, tol_noun)
                    break

        # --- Phase 2: Role assignment using simple SVO state machine ---
        S, V, O = "S", "V", "O"
        roles = {}
        phase = "pre_subj"

        for i, w in enumerate(en_lower):
            if w in TOL_POSSESSIVES:
                roles[i] = "skip"
                continue
            if w in EN_NEGATION_WORDS:
                roles[i] = "neg"
                continue
            if w in TOL_QUESTION_WORDS:
                roles[i] = "Q"
                continue
            if w in EN_RELATIVE_PRONOUNS and i > 0:
                word_map[i] = "nin"
                roles[i] = O
                continue
            if w in EN_TO_TOL_FUNCTION:
                word_map[i] = EN_TO_TOL_FUNCTION[w]
                roles[i] = O
                continue
            if w in EN_STOPWORDS and w not in TOL_PRONOUNS and w not in EN_PREPOSITIONS:
                roles[i] = "skip"
                continue
            if w in EN_PREPOSITIONS:
                roles[i] = "prep"
                continue

            is_verb = (w in EN_BE_FORMS
                       or w in self._en_known_verbs
                       or (word_map.get(i) and self._is_tol_verb(word_map.get(i, ""))))

            if phase == "pre_subj":
                if is_verb:
                    roles[i] = V
                    phase = "obj"
                elif w in TOL_PRONOUNS or word_map.get(i):
                    roles[i] = S
                    phase = "subj"
                else:
                    roles[i] = S
                    phase = "subj"
            elif phase == "subj":
                if is_verb:
                    roles[i] = V
                    phase = "obj"
                else:
                    roles[i] = S
            elif phase == "obj":
                roles[i] = O

        # --- Phase 3: Collect parts by role ---
        question_parts, subject_parts, verb_parts, object_parts, pp_phrases = [], [], [], [], []

        i = 0
        while i < n:
            w = en_lower[i]
            role = roles.get(i, "skip")

            if role == "Q":
                q = w
                if i + 1 < n and (w + " " + en_lower[i + 1]) in TOL_QUESTION_WORDS:
                    q = w + " " + en_lower[i + 1]
                    i += 1
                question_parts.append(TOL_QUESTION_WORDS.get(q, q))

            elif role == "prep":
                postp = TOL_POSTPOSITIONS.get(w, "")
                if postp:
                    pp_noun = self._collect_pp_noun(en_lower, word_map, i + 1, n)
                    if pp_noun:
                        pp_phrases.append((pp_noun, postp))

            elif role == S:
                tol_w = self._resolve_tol_word(w, i, word_map)
                if tol_w:
                    subject_parts.append(tol_w)

            elif role == V:
                if w not in EN_BE_FORMS:
                    tol_v = word_map.get(i)
                    if tol_v and not tol_v.startswith("["):
                        verb_parts.append(tol_v)

            elif role == O:
                tol_w = self._resolve_tol_word(w, i, word_map)
                if tol_w:
                    object_parts.append(tol_w)

            i += 1

        # --- Phase 4: Verb conjugation ---
        subject_person = None
        for w in en_lower:
            if w in EN_PRONOUN_PERSON:
                subject_person = EN_PRONOUN_PERSON[w]
                break

        if subject_person and verb_parts and self.verb_paradigms:
            en_word_set = set(en_lower)
            conjugated = []
            for v_tol in verb_parts:
                matched = False
                for en_verb, paradigm in self.verb_paradigms.items():
                    if v_tol in set(paradigm.values()) or en_verb in en_word_set:
                        if subject_person in paradigm:
                            conjugated.append(paradigm[subject_person])
                            matched = True
                            break
                conjugated.append(v_tol) if not matched else None
            verb_parts = conjugated

        # --- Phase 5: Assemble in Tol SOV order ---
        result = []
        result.extend(question_parts)
        result.extend(subject_parts)
        for noun_tol, postp in pp_phrases:
            result.append(noun_tol)
            result.append(postp)
        result.extend(object_parts)

        if is_negated:
            if has_be and not verb_parts:
                result.append(TOL_NEG_COPULA)
            else:
                result.append(TOL_NEGATION)
                result.extend(verb_parts)
        elif has_be and not verb_parts and object_parts:
            result.append(TOL_COPULA)
        else:
            result.extend(verb_parts)

        result = [r for r in result if r and r.strip() and not r.startswith("[")]
        output = " ".join(result) if result else " ".join(p for p in tol_parts if not p.startswith("["))
        return self._capitalize_proper_nouns(output)

    def _build_english_verb_set(self) -> set:
        """Build a set of known English verb forms from paradigm data, verb conjugations, and common verbs."""
        verbs = set(EN_BE_FORMS)
        verbs.update(self.verb_paradigms.keys())
        _pronoun_noise = {"i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them",
                           "my", "your", "his", "its", "our", "their", "a", "an", "the", "to", "not"}
        if self._table_exists("verb_conjugations"):
            for r in self.conn.execute("SELECT DISTINCT english_form FROM verb_conjugations WHERE english_form IS NOT NULL"):
                for w in r["english_form"].lower().split():
                    if w not in _pronoun_noise and len(w) > 1:
                        verbs.add(w)
        # Core verbs attested in Tol Bible translation
        _core = {
            "go", "goes", "went", "gone", "going", "come", "comes", "came", "coming",
            "see", "sees", "saw", "seen", "seeing", "eat", "eats", "ate", "eaten", "eating",
            "say", "says", "said", "saying", "speak", "speaks", "spoke", "speaking",
            "give", "gives", "gave", "given", "giving", "take", "takes", "took", "taken",
            "make", "makes", "made", "making", "know", "knows", "knew", "known",
            "think", "thinks", "thought", "thinking", "want", "wants", "wanted",
            "love", "loves", "loved", "loving", "like", "likes", "liked",
            "die", "dies", "died", "dying", "kill", "kills", "killed",
            "walk", "walks", "walked", "run", "runs", "ran", "running",
            "sit", "sits", "sat", "stand", "stands", "stood",
            "live", "lives", "lived", "living", "sleep", "sleeps", "slept",
            "work", "works", "worked", "working",
            "find", "finds", "found", "tell", "tells", "told",
            "call", "calls", "called", "send", "sends", "sent",
            "write", "writes", "wrote", "written", "read", "reads",
            "hear", "hears", "heard", "believe", "believes", "believed",
            "put", "puts", "get", "gets", "got", "bring", "brings", "brought",
            "leave", "leaves", "left", "fall", "falls", "fell",
            "hold", "holds", "held", "build", "builds", "built",
            "open", "opens", "opened", "close", "closes", "closed",
            "teach", "teaches", "taught", "learn", "learns", "learned",
            "help", "helps", "helped", "pray", "prays", "prayed",
            "born", "heal", "heals", "healed", "follow", "follows", "followed",
            "receive", "receives", "received", "enter", "enters", "entered",
        }
        verbs.update(_core)
        return verbs

    def _is_tol_verb(self, tol_word: str) -> bool:
        """Check if a Tol word is likely a verb based on paradigm data."""
        if not tol_word or tol_word.startswith("["):
            return False
        for paradigm in self.verb_paradigms.values():
            if tol_word in set(paradigm.values()):
                return True
        return False

    def _resolve_tol_word(self, en_word: str, idx: int, word_map: dict) -> Optional[str]:
        """Get the Tol translation for an English word at a given position."""
        if en_word in TOL_PRONOUNS:
            return TOL_PRONOUNS[en_word]
        tol = word_map.get(idx)
        if tol and not tol.startswith("["):
            return tol
        return None

    def _collect_pp_noun(self, en_lower: list, word_map: dict, start: int, n: int) -> Optional[str]:
        """Collect the noun phrase following a preposition for postposition conversion."""
        j = start
        while j < n:
            w = en_lower[j]
            if w in EN_STOPWORDS and w not in TOL_PRONOUNS:
                j += 1
                continue
            if word_map.get(j) and not word_map[j].startswith("["):
                noun = word_map[j]
                word_map[j] = None
                return noun
            if w in TOL_PRONOUNS:
                return TOL_PRONOUNS[w]
            break
        return None

    _PROPER_NOUNS = {
        "dios": "Dios", "jesús": "Jesús", "jesucristo": "Jesucristo",
        "cristo": "Cristo", "maría": "María", "pedro": "Pedro",
        "pablo": "Pablo", "juan": "Juan", "santiago": "Santiago",
        "david": "David", "abraham": "Abraham", "moisés": "Moisés",
        "israel": "Israel", "jerusalén": "Jerusalén",
    }

    @staticmethod
    def _capitalize_proper_nouns(text: str) -> str:
        if not text:
            return text
        words = text.split()
        result = []
        for i, w in enumerate(words):
            cap = TolTranslator._PROPER_NOUNS.get(w.lower())
            if cap:
                result.append(cap)
            elif i == 0:
                result.append(w[0].upper() + w[1:] if len(w) > 1 else w.upper())
            else:
                result.append(w)
        return " ".join(result)

    # ── Main entry point ──────────────────────────────────────────────────

    def translate(self, text: str, source_lang: str, target_lang: str) -> dict:
        text = text.strip()
        if not text:
            return {"translation": "", "method": "empty", "confidence": 0, "details": {}, "translations": []}
        if source_lang == target_lang:
            return {"translation": text, "method": "identity", "confidence": 1.0, "details": {},
                    "translations": [{"text": text, "method": "identity", "confidence": 1.0}]}

        routes = {
            ("tol", "es"): self._tol_to_spanish,
            ("es", "tol"): self._spanish_to_tol,
            ("tol", "en"): self._tol_to_english,
            ("en", "tol"): self._english_to_tol,
            ("es", "en"): self._spanish_to_english,
            ("en", "es"): self._english_to_spanish,
        }
        fn = routes.get((source_lang, target_lang))
        if fn:
            result = fn(text)
            if "translations" not in result:
                result["translations"] = [{"text": result["translation"], "method": result["method"], "confidence": result["confidence"]}]
            return result
        return {"translation": text, "method": "unsupported", "confidence": 0, "details": {}, "translations": []}

    # ── Tol → Spanish ─────────────────────────────────────────────────────

    _TOL_TO_SPA_FUNCTION = {
        "mas": "muy", "más": "muy",
        "ma": "no", "mpes": "entonces", "wa": "también",
        "na": "cuando", "lovin": "después", "püna": "antes",
        "hola": "hola", "gracias": "gracias",
    }

    _TOL_TO_EN_FUNCTION = {
        "mas": "very", "más": "very",
        "ma": "no", "mpes": "then", "wa": "also",
        "na": "when", "lovin": "after", "püna": "before",
        "hola": "hello", "gracias": "thank you",
    }

    def _tol_to_spanish(self, text: str) -> dict:
        text_lower = text.lower().strip()

        entry = self._tol_lookup_spa(text_lower)
        if entry:
            return {
                "translation": entry["spanish"],
                "method": "dictionary",
                "confidence": 0.95,
                "details": {"category": entry["category"]},
            }

        match = self._fuzzy_match_corpus(text, "tol")
        if match:
            return match

        words = text_lower.split()
        if len(words) > 1:
            translated, untranslated = [], []
            for w in words:
                if w in self._TOL_TO_SPA_FUNCTION:
                    translated.append(self._TOL_TO_SPA_FUNCTION[w])
                else:
                    hit = self._tol_lookup_spa(w)
                    if hit:
                        translated.append(hit["spanish"])
                    elif w in self.inferred_tol_from_es:
                        best = max(self.inferred_tol_from_es[w], key=lambda x: x["confidence"])
                        translated.append(best["spanish"])
                    else:
                        translated.append(f"[{w}]")
                        untranslated.append(w)
            if len(untranslated) < len(words):
                return {
                    "translation": " ".join(translated),
                    "method": "word-by-word",
                    "confidence": round((1 - len(untranslated) / len(words)) * 0.6, 2),
                    "details": {"untranslated": untranslated},
                }

        if text_lower in self._TOL_TO_SPA_FUNCTION:
            return {
                "translation": self._TOL_TO_SPA_FUNCTION[text_lower],
                "method": "function_word",
                "confidence": 0.8,
                "details": {},
            }

        return {"translation": f"[No se encontró traducción para: {text}]", "method": "not_found", "confidence": 0, "details": {}}

    # ── Spanish → Tol ─────────────────────────────────────────────────────

    _SPA_STOPWORDS = {
        # Articles and determiners
        "el", "la", "los", "las", "un", "una", "unos", "unas",
        "este", "esta", "estos", "estas", "ese", "esa", "esos", "esas",
        "esto", "eso", "aquel", "aquella", "aquellos", "aquellas",
        # Prepositions
        "de", "del", "al", "a", "en", "con", "por", "para",
        "sobre", "entre", "hasta", "desde", "hacia", "según", "sin",
        "tras", "durante", "mediante", "contra",
        # Conjunctions / discourse markers
        "y", "o", "e", "pero", "sino", "ni", "pues", "aunque", "mientras",
        "luego", "mas",
        # Pronouns (subject, object, reflexive)
        "yo", "tú", "él", "ella", "nosotros", "vosotros", "ellos", "ellas",
        "me", "te", "se", "lo", "la", "le", "nos", "os", "les",
        "mí", "ti", "sí", "consigo",
        # Possessives
        "mi", "tu", "su", "mis", "tus", "sus",
        "nuestro", "nuestra", "nuestros", "nuestras",
        "vuestro", "vuestra", "vuestros", "vuestras",
        # Copula / auxiliary verb forms
        "es", "son", "está", "están", "ser", "era", "eran",
        "sido", "siendo", "ha", "han", "he", "has", "había", "habían",
        "haber", "habiendo", "haya", "hayan",
        "tiene", "tienen", "tenía", "tenían", "teniendo", "tienes",
        "puede", "pueden", "podía", "podían",
        "estaba", "estaban", "soy", "somos", "sois", "esté", "estamos",
        "sea", "sean", "fuese", "fuera",
        # Common adverbs/function words
        "muy", "más", "tan", "ya", "aun",
        "como", "así", "aquí", "allí",
        "oh", "ay", "tanto", "tales", "mismo", "misma", "mismos", "mismas",
        "bien", "acerca", "medio", "manera",
        "dentro", "juntamente", "sé",
        # Relative pronouns
        "que", "cual", "cuales", "cuyo", "cuya", "cuyos", "quienes",
    }

    _SPA_TO_TOL_FUNCTION = {
        # Question words
        "qué": "tsjan", "quién": "pjacj", "quien": "pjacj",
        "dónde": "jolaj", "cuándo": "'ona", "cuánto": "nol",
        "cómo": "tsjan",
        # Common verbs → Bible-attested Tol forms
        "dijo": "tjevele", "dijeron": "tjowelepj", "dice": "tjevele",
        "diciendo": "tjevele", "decía": "tjevele", "decir": "tjevele",
        "hablar": "tjevele", "habló": "tjevele", "hablando": "tjevele",
        "vino": "tjiquil", "vinieron": "tjiquil", "viene": "jac'", "venir": "tjiquil",
        "dio": "tje'yaya", "dieron": "tje'yaya", "dar": "tje'yaya", "dando": "tje'yaya",
        "hizo": "tjiji", "hicieron": "tjiji", "hecho": "tjiji", "hacen": "tjiji", "hacer": "tjiji",
        "fue": "tjemey", "fueron": "tjemey", "ido": "tjemey", "ir": "tjemey",
        "salió": "tjemey", "salieron": "tjemey", "salir": "tjemey",
        "vio": "tjinyuca", "vieron": "tjinyuca", "ver": "tjinyuca",
        "oyó": "tjapjacas", "oyeron": "tjapjacas", "oír": "tjapjacas",
        "envió": "tjejyama", "enviado": "tjejyama", "enviar": "tjejyama",
        "entrar": "cjuwá", "entró": "cjuwá", "entraron": "cjuwá",
        "tomar": "ta'es", "tomó": "ta'es", "tomaron": "ta'es",
        "morir": "cjüele", "murió": "cjüele", "muerto": "cjüele",
        "creer": "japon", "creyeron": "japon", "cree": "japon",
        "recibir": "ma'ayas", "recibió": "ma'ayas", "recibieron": "ma'ayas",
        "conocer": "yas", "conoció": "yas", "saber": "yas", "sabéis": "solejé",
        "llamar": "tjacuwis", "llamó": "tjacuwis", "llamado": "tjacuwis",
        "escribir": "tepyaca", "enseñar": "lajay", "enseñaba": "lajay",
        "mirando": "tjinyuca", "mirar": "tjinyuca",
        "llegó": "tjiquil", "llegaron": "tjiquil",
        "puso": "tjiji", "haciendo": "tjiji",
        "saliendo": "tjemey",
        # Adverbs / function → Tol
        "también": "wa", "entonces": "mpes", "después": "lovin",
        "antes": "püna", "siempre": "lovin", "nunca": "ma",
        "cada": "pjü", "otro": "p'a", "otra": "p'a", "otros": "p'a", "otras": "p'a",
        "todo": "pjü", "todos": "pjü", "toda": "pjü", "todas": "pjü",
        "cuando": "na", "donde": "ne'aj", "delante": "ne",
        "mucho": "pülücj", "muchos": "pülücj", "muchas": "pülücj", "mucha": "pülücj",
        "gran": "pajal", "aún": "custjay",
        "ninguno": "ma", "ninguna": "ma",
        "alguno": "nepénowa", "alguna": "nepénowa",
        "cierto": "t'üc'", "verdaderamente": "t'üc'",
        "mirad": "tjajama",
        # Negation
        "no": "ma", "nada": "tulucj", "nadie": "ma",
        # Nouns: Bible-attested Tol forms
        "dios": "dios", "jesús": "jesús", "jesucristo": "jesucristo",
        "cristo": "jesucristo",
        "señor": "jepa",
        "padre": "papay", "madre": "napay",
        "hijo": "jatjam", "hijos": "ts'uyupj", "hija": "jucucus",
        "hermano": "catjam", "hermanos": "natjampan",
        "hombre": "yom", "hombres": "niyom",
        "mujer": "quepj", "mujeres": "quepan",
        "espíritu": "cjües", "santo": "cjües",
        "pueblo": "gente", "gente": "gente",
        "casa": "wo", "templo": "wo",
        "cielo": "tsjun", "cielos": "tsjun",
        "tierra": "nosis", "mundo": "nosis",
        "día": "jawas", "días": "ts'ac'", "noche": "püste",
        "vida": "mpatjam", "muerte": "cjüele", "muertos": "müjünsücj",
        "agua": "'üsǘ", "pan": "pansas",
        "nombre": "ló", "palabra": "tjevelá", "palabras": "tjevelé",
        "camino": "jümücj",
        "mano": "mos", "manos": "mos", "cuerpo": "jüp'üy",
        "ojo": "nyuc", "ojos": "nyuc", "corazón": "yola",
        "sangre": "'os", "carne": "p'üy",
        "ley": "tjijyü'tá", "rey": "jepa", "reino": "jütüta",
        "gloria": "püné", "gracia": "najas", "paz": "nujola",
        "verdad": "t'üc'", "fe": "tepyona",
        "pecado": "malala", "pecados": "malala",
        "mal": "malala", "malo": "malala", "mala": "malala",
        "bueno": "'üsüs", "buena": "'üsüs",
        "bonito": "'üsüs", "bonita": "'üsüs",
        "hermoso": "'üsüs", "hermosa": "'üsüs",
        "lindo": "'üsüs", "linda": "'üsüs",
        "feo": "malala", "fea": "malala",
        "grande": "pajal", "pequeño": "tsjücjüim", "pequeña": "tsjücjüim",
        "fuerte": "tjiyocj", "débil": "mop'in",
        "rico": "jatatj", "pobre": "jatütsja",
        "enfermo": "pü'as", "enferma": "pü'as",
        "joven": "tjamacjas", "viejo": "tjüjitsja",
        "vivo": "pü'üs", "muerto": "tepe'e",
        "limpio": "quip", "sucio": "til",
        "lleno": "tjíjitsja",
        "ciego": "cyacaná", "sordo": "toptsja", "mudo": "lapanen",
        "caliente": "nyuca", "frío": "nejay",
        "seco": "tinca", "mojado": "wüsvits'",
        "negro": "tjyam", "blanco": "püts'üw", "rojo": "canatj",
        "nuevo": "seyasa", "nueva": "seyasa",
        "primero": "mwalá",
        "poder": "liji",
        "hora": "las", "ciudad": "patja", "puerta": "vilicj",
        "mar": "'üsǘ", "cosa": "javelepj", "cosas": "javelepj",
        "ángel": "ángel", "ángeles": "angelpan",
        "escrito": "tepyaca",
        "doce": "dóceya", "voz": "pjactsja",
        "tiempo": "püna",
        "obras": "lajay", "causa": "mpes",
        "varones": "niyom",
        # Religious roles → Bible-attested
        "discípulo": "discípulo", "discípulos": "discipulopan",
        "judíos": "judiopan", "fariseos": "fariseopan",
        "sacerdote": "sacerdote", "sacerdotes": "sacerdote",
        "profeta": "profeta", "profetas": "profetapan",
        "apóstol": "apóstol", "apóstoles": "apostolpan",
        "siervo": "jomozo", "siervos": "jomozopan",
        "iglesia": "majaman",
        # Proper nouns
        "jerusalén": "jerusalén", "pedro": "pedro", "pablo": "pablo",
        "juan": "juan", "moisés": "moisés", "david": "david",
        "abraham": "abraham", "israel": "israel",
        "galilea": "galilea", "judea": "judea",
        "pilato": "pilato", "herodes": "herodes",
        "simón": "simón", "jacobo": "jacobo", "josé": "josé",
        "felipe": "felipe", "bernabé": "bernabé", "timoteo": "timoteo",
        "antioquía": "antioquía", "samaria": "samaria",
        # Common greetings (loanwords/passthroughs)
        "hola": "hola", "adiós": "adiós", "adios": "adios",
        "gracias": "gracias",
        # buenos/buenas for greetings: "buenos días" → "'üsüs ts'ac'"
        "buenos": "'üsüs", "buenas": "'üsüs",
        "tardes": "nala", "noches": "püste",
    }

    def _spanish_to_tol(self, text: str) -> dict:
        text_lower = text.lower().strip()
        candidates = []

        # 0. Function-word single-word quick check
        if text_lower in self._SPA_TO_TOL_FUNCTION:
            tol = self._SPA_TO_TOL_FUNCTION[text_lower]
            if text.strip() and text.strip()[0].isupper():
                tol = tol[0].upper() + tol[1:] if tol else tol
            candidates.append({"text": tol, "method": "function_word", "confidence": 0.80})

        # 1. Full-phrase dictionary lookup
        if text_lower in self.spanish_to_tol:
            entry = self.spanish_to_tol[text_lower]
            return {
                "translation": entry["tol"],
                "method": "dictionary",
                "confidence": 0.95,
                "details": {"category": entry["category"]},
                "translations": [{"text": entry["tol"], "method": "dictionary", "confidence": 0.95}],
            }

        # 2. Full-phrase inferred (only if Bible-attested and high confidence)
        if text_lower in self.inferred_es_to_tol:
            best = max(self.inferred_es_to_tol[text_lower], key=lambda x: x["confidence"])
            if best["confidence"] >= 0.75 and self._tol_word_bible_score(best["tol"]) > 0:
                candidates.append({"text": best["tol"], "method": "synonym_inferred", "confidence": best["confidence"]})

        # 3. Word-by-word translation (try BEFORE corpus to avoid false positives)
        original_words = text.strip().split()
        words = [re.sub(r'[.,;:!?¿¡"""\u201c\u201d\(\)\[\]]', '', w) for w in text_lower.split()]
        if len(words) > 1:
            translated, untranslated = [], []
            for i, w in enumerate(words):
                if not w:
                    continue
                if w in self._SPA_TO_TOL_FUNCTION:
                    tol_w = self._SPA_TO_TOL_FUNCTION[w]
                    orig = original_words[i] if i < len(original_words) else w
                    if orig and orig[0].isupper():
                        tol_w = tol_w[0].upper() + tol_w[1:] if tol_w else tol_w
                    translated.append(tol_w)
                elif w in self._SPA_STOPWORDS:
                    continue
                elif w in self.spanish_to_tol:
                    translated.append(self.spanish_to_tol[w]["tol"])
                elif w in self.inferred_es_to_tol:
                    best = max(self.inferred_es_to_tol[w], key=lambda x: x["confidence"])
                    if best["confidence"] >= 0.75 and self._tol_word_bible_score(best["tol"]) > 0:
                        translated.append(best["tol"])
                    else:
                        orig = original_words[i] if i < len(original_words) else w
                        if orig and orig[0].isupper():
                            translated.append(orig)
                        else:
                            translated.append(f"[{w}]")
                            untranslated.append(w)
                else:
                    orig = original_words[i] if i < len(original_words) else w
                    if orig and orig[0].isupper():
                        translated.append(orig)
                    else:
                        translated.append(f"[{w}]")
                        untranslated.append(w)

            content_words = [w for w in words if w not in self._SPA_STOPWORDS or w in self._SPA_TO_TOL_FUNCTION]
            coverage = (len(content_words) - len(untranslated)) / max(len(content_words), 1)
            if translated and coverage > 0:
                wbw_text = " ".join(translated)
                wbw_conf = round(coverage * 0.7, 2)
                candidates.append({"text": wbw_text, "method": "word-by-word+synonym", "confidence": wbw_conf})

        # 4. Corpus fuzzy match (only if word-by-word coverage is low)
        best_wbw_conf = max((c["confidence"] for c in candidates), default=0)
        if best_wbw_conf < 0.6:
            match = self._fuzzy_match_corpus(text, "spanish")
            if match:
                seen = {tol_phonetic_normalize(c["text"]) for c in candidates}
                if tol_phonetic_normalize(match["translation"]) not in seen:
                    candidates.append({"text": match["translation"], "method": "corpus_match",
                                       "confidence": match["confidence"]})

        if not candidates:
            if len(words) == 1:
                return {"translation": f"[Traducción no encontrada: {text}]", "method": "not_found",
                        "confidence": 0, "details": {}, "translations": []}
            return {"translation": f"[Traducción no encontrada: {text}]", "method": "not_found",
                    "confidence": 0, "details": {}, "translations": []}

        candidates.sort(key=lambda c: -c["confidence"])
        best = candidates[0]
        return {
            "translation": best["text"],
            "method": best["method"],
            "confidence": best["confidence"],
            "details": {},
            "translations": candidates[:3],
        }

    # ── Tol → English ─────────────────────────────────────────────────────

    def _tol_to_english(self, text: str) -> dict:
        text_lower = text.lower().strip()

        en_hit = self._tol_lookup_en(text_lower)
        if en_hit:
            return {
                "translation": en_hit,
                "method": "dictionary_direct",
                "confidence": 0.9,
                "details": {},
            }

        if text_lower in self.inferred_tol_from_en:
            best = max(self.inferred_tol_from_en[text_lower], key=lambda x: x["confidence"])
            return {
                "translation": best["english"],
                "method": "synonym_inferred",
                "confidence": best["confidence"],
                "details": {"path": best["path"]},
            }
        stripped = self._strip_apostrophes(text_lower)
        if stripped in self.inferred_tol_from_en:
            best = max(self.inferred_tol_from_en[stripped], key=lambda x: x["confidence"])
            return {
                "translation": best["english"],
                "method": "synonym_inferred",
                "confidence": best["confidence"],
                "details": {"path": best["path"]},
            }

        if text_lower in self._TOL_TO_EN_FUNCTION:
            return {
                "translation": self._TOL_TO_EN_FUNCTION[text_lower],
                "method": "function_word",
                "confidence": 0.8,
                "details": {},
            }

        words = text_lower.split()
        if len(words) > 1:
            translated, untranslated = [], []
            for w in words:
                if w in self._TOL_TO_EN_FUNCTION:
                    translated.append(self._TOL_TO_EN_FUNCTION[w])
                else:
                    en_w = self._tol_lookup_en(w)
                    if en_w:
                        translated.append(en_w)
                    elif w in self.inferred_tol_from_en:
                        best = max(self.inferred_tol_from_en[w], key=lambda x: x["confidence"])
                        translated.append(best["english"])
                    elif self._strip_apostrophes(w) in self.inferred_tol_from_en:
                        best = max(self.inferred_tol_from_en[self._strip_apostrophes(w)], key=lambda x: x["confidence"])
                        translated.append(best["english"])
                    else:
                        translated.append(f"[{w}]")
                        untranslated.append(w)
            if len(untranslated) < len(words):
                return {
                    "translation": " ".join(translated),
                    "method": "word-by-word",
                    "confidence": round((1 - len(untranslated) / len(words)) * 0.7, 2),
                    "details": {"untranslated": untranslated},
                }

        spa_result = self._tol_to_spanish(text)
        if spa_result["method"] == "not_found":
            return {"translation": f"[Translation not found for: {text}]", "method": "not_found", "confidence": 0, "details": {}}

        spa_text = spa_result["translation"]
        eng_result = self._spanish_to_english(spa_text)
        confidence = round(spa_result["confidence"] * eng_result["confidence"], 2)
        return {
            "translation": eng_result["translation"],
            "method": f"chain:tol→es({spa_result['method']})→en({eng_result['method']})",
            "confidence": confidence,
            "details": {"intermediate_spanish": spa_text},
        }

    # ── English → Tol (major rewrite: direct + grammar-aware) ─────────────

    def _english_to_tol(self, text: str) -> dict:
        text_lower = text.lower().strip()
        candidates = []

        # 1. Full-phrase direct lookup — pull top candidates by priority
        if text_lower in self.direct_en_tol_all:
            seen_phonetic = set()
            sorted_entries = sorted(self.direct_en_tol_all[text_lower], key=lambda e: (-e["_priority"], -e["confidence"]))
            for e in sorted_entries:
                norm = tol_phonetic_normalize(e["tol"])
                if norm not in seen_phonetic:
                    candidates.append({"text": e["tol"], "method": "direct_en_tol", "confidence": e["confidence"]})
                    seen_phonetic.add(norm)
                if len(candidates) >= 3:
                    break

        if text_lower in self.english_to_tol:
            tol = self.english_to_tol[text_lower]
            seen_phonetic = {tol_phonetic_normalize(c["text"]) for c in candidates}
            if tol_phonetic_normalize(tol) not in seen_phonetic:
                candidates.append({"text": tol, "method": "dictionary_direct", "confidence": 0.9})

        if len(candidates) < 3 and text_lower in self.inferred_en_to_tol:
            seen_phonetic = {tol_phonetic_normalize(c["text"]) for c in candidates}
            for inf in sorted(self.inferred_en_to_tol[text_lower], key=lambda x: -x["confidence"]):
                norm = tol_phonetic_normalize(inf["tol"])
                if norm not in seen_phonetic:
                    candidates.append({"text": inf["tol"], "method": "synonym_inferred", "confidence": inf["confidence"]})
                    seen_phonetic.add(norm)
                if len(candidates) >= 3:
                    break

        # 2. Corpus fuzzy match — highest quality for known Bible verses
        words = re.findall(r"[a-z''\-]+", text_lower)
        words = [w.strip("-") for w in words if w.strip("-")]
        if len(words) > 1:
            corpus_match = self._fuzzy_match_corpus(text, "english")
            if corpus_match:
                seen_phonetic = {tol_phonetic_normalize(c["text"]) for c in candidates}
                if tol_phonetic_normalize(corpus_match["translation"]) not in seen_phonetic:
                    candidates.append({"text": corpus_match["translation"], "method": "corpus_match",
                                       "confidence": corpus_match["confidence"]})

        # 3. Sentence-level: translate each word directly, then apply grammar
        if len(words) > 1:
            grammar_result = self._build_grammar_candidate(text, text_lower, words)
            if grammar_result:
                seen_phonetic = {tol_phonetic_normalize(c["text"]) for c in candidates}
                norm = tol_phonetic_normalize(grammar_result["text"])
                if norm not in seen_phonetic:
                    candidates.append(grammar_result)

        # 4. Spanish chain fallback
        if len(candidates) < 3:
            spa_result = self._english_to_spanish(text)
            if spa_result["confidence"] > 0:
                tol_result = self._spanish_to_tol(spa_result["translation"])
                if tol_result["method"] != "not_found":
                    chain_conf = round(spa_result["confidence"] * tol_result["confidence"], 2)
                    chain_text = tol_result["translation"]
                    seen_phonetic = {tol_phonetic_normalize(c["text"]) for c in candidates}
                    if tol_phonetic_normalize(chain_text) not in seen_phonetic:
                        candidates.append({"text": chain_text, "method": "chain:en→es→tol", "confidence": chain_conf})

        if not candidates:
            if len(words) <= 1:
                # Proper nouns with no translation: preserve as-is
                if text.strip() and text.strip()[0].isupper():
                    return {"translation": text.strip(), "method": "proper_noun_passthrough", "confidence": 0.7,
                            "details": {}, "translations": [{"text": text.strip(), "method": "proper_noun_passthrough", "confidence": 0.7}]}
                return {"translation": f"[Translation not found for: {text}]", "method": "not_found", "confidence": 0,
                        "details": {}, "translations": []}
            # Try grammar-only even if nothing else worked
            grammar_result = self._build_grammar_candidate(text, text_lower, words)
            if grammar_result:
                candidates.append(grammar_result)
            else:
                return {"translation": f"[Translation not found for: {text}]", "method": "not_found", "confidence": 0,
                        "details": {}, "translations": []}

        candidates.sort(key=lambda c: -c["confidence"])
        candidates = candidates[:3]
        best = candidates[0]
        return {
            "translation": best["text"],
            "method": best["method"],
            "confidence": best["confidence"],
            "details": {},
            "translations": candidates,
        }

    _COMMON_SENTENCE_STARTERS = {
        "the", "a", "an", "i", "he", "she", "it", "we", "they", "you",
        "this", "that", "these", "those", "my", "his", "her", "its", "our", "their",
        "what", "where", "when", "who", "how", "why", "which",
        "do", "does", "did", "is", "are", "was", "were", "will", "would",
        "can", "could", "should", "may", "might", "must", "have", "has", "had",
        "all", "some", "many", "most", "each", "every", "no", "not", "don't",
        "if", "but", "and", "or", "so", "then", "now", "here", "there",
        "one", "two", "three", "four", "five", "first", "last",
        "let", "come", "go", "get", "give", "take", "make", "say", "said",
        "because", "since", "after", "before", "while", "until",
        "from", "on", "in", "at", "to", "for", "with", "by", "about",
        "just", "also", "very", "still", "already", "even", "only",
    }

    @staticmethod
    def _is_proper_noun(word: str, position: int, original_words: list) -> bool:
        """Detect likely proper nouns: capitalized words not recognized as common English."""
        if not word or not word[0].isupper():
            return False
        if position == 0:
            # Sentence-initial: proper noun if it's not a common sentence-starter
            return word.lower() not in TolTranslator._COMMON_SENTENCE_STARTERS
        return True

    def _build_grammar_candidate(self, text: str, text_lower: str, words: list) -> Optional[dict]:
        """Build a grammar-engine translation candidate from word-level lookups."""
        word_map = {}
        methods_used = set()
        untranslated = []

        # Extract original-cased words for proper noun detection
        original_words = re.findall(r"[A-Za-z''\-]+", text)
        original_words = [w.strip("-") for w in original_words if w.strip("-")]

        for idx, w in enumerate(words):
            result = self._lookup_en_word(w)
            if result:
                word_map[idx] = result["tol"]
                methods_used.add(result["method"])
                continue

            if w in TOL_PRONOUNS or w in TOL_POSSESSIVES or w in TOL_POSTPOSITIONS or w in TOL_QUESTION_WORDS:
                continue
            if w in EN_RELATIVE_PRONOUNS:
                word_map[idx] = "nin"
                methods_used.add("function_word")
                continue
            if w in EN_TO_TOL_FUNCTION:
                word_map[idx] = EN_TO_TOL_FUNCTION[w]
                methods_used.add("function_word")
                continue
            if w in EN_STOPWORDS or w in EN_NEGATION_WORDS:
                continue
            if w in self.eng_to_spa:
                spa_word = self.eng_to_spa[w][0]["spanish"]
                if spa_word in self.spanish_to_tol:
                    word_map[idx] = self.spanish_to_tol[spa_word]["tol"]
                    methods_used.add("chain_word")
                    continue

            # Proper noun preservation: if the original word was capitalized
            # and has no known translation, keep it as-is
            orig = original_words[idx] if idx < len(original_words) else w
            if self._is_proper_noun(orig, idx, original_words):
                word_map[idx] = orig
                continue

            untranslated.append(w)
            word_map[idx] = f"[{w}]"

        content_words = [w for w in words if w not in EN_STOPWORDS and w not in EN_NEGATION_WORDS
                         and w not in TOL_PRONOUNS and w not in TOL_POSTPOSITIONS and w not in TOL_QUESTION_WORDS]
        if not content_words:
            content_words = words

        translated_count = len(content_words) - len(untranslated)
        if translated_count <= 0:
            return None

        tol_parts = [word_map.get(i, "") for i in range(len(words)) if word_map.get(i)]
        tol_sentence = self._apply_tol_grammar(words, tol_parts, word_map)
        coverage = translated_count / max(len(content_words), 1)
        confidence = round(coverage * 0.75, 2)
        method_str = "grammar_engine+" + "+".join(sorted(methods_used))

        return {"text": tol_sentence, "method": method_str, "confidence": confidence}

    # ── Spanish ↔ English (fully local) ───────────────────────────────────

    def _spanish_to_english(self, text: str) -> dict:
        text_lower = text.lower().strip()

        if text_lower in self.spa_to_eng:
            entries = self.spa_to_eng[text_lower]
            best = entries[0]["english"]
            return {"translation": best, "method": "local_dict", "confidence": 0.9, "details": {"alternatives": [e["english"] for e in entries[:5]]}}

        words = re.findall(r'[a-záéíóúüñ]+', text_lower)
        if not words:
            return {"translation": text, "method": "passthrough", "confidence": 0.1, "details": {}}

        translated, untranslated = [], []
        for w in words:
            if w in self.spa_to_eng:
                translated.append(self.spa_to_eng[w][0]["english"])
            else:
                translated.append(w)
                untranslated.append(w)

        if not untranslated:
            return {"translation": " ".join(translated), "method": "local_dict_wbw", "confidence": 0.85, "details": {}}
        elif len(untranslated) < len(words):
            return {
                "translation": " ".join(translated),
                "method": "local_dict_partial",
                "confidence": round((1 - len(untranslated) / len(words)) * 0.7, 2),
                "details": {"untranslated_spa": untranslated},
            }

        return {"translation": text, "method": "passthrough", "confidence": 0.1, "details": {"note": "No local translations found"}}

    def _english_to_spanish(self, text: str) -> dict:
        text_lower = text.lower().strip()

        if text_lower in self.eng_to_spa:
            entries = self.eng_to_spa[text_lower]
            best = entries[0]["spanish"]
            return {"translation": best, "method": "local_dict", "confidence": 0.9, "details": {"alternatives": [e["spanish"] for e in entries[:5]]}}

        words = re.findall(r"[a-z']+", text_lower)
        if not words:
            return {"translation": text, "method": "passthrough", "confidence": 0.1, "details": {}}

        translated, untranslated = [], []
        for w in words:
            if w in self.eng_to_spa:
                translated.append(self.eng_to_spa[w][0]["spanish"])
            else:
                translated.append(w)
                untranslated.append(w)

        if not untranslated:
            return {"translation": " ".join(translated), "method": "local_dict_wbw", "confidence": 0.85, "details": {}}
        elif len(untranslated) < len(words):
            return {
                "translation": " ".join(translated),
                "method": "local_dict_partial",
                "confidence": round((1 - len(untranslated) / len(words)) * 0.7, 2),
                "details": {"untranslated_eng": untranslated},
            }

        return {"translation": text, "method": "passthrough", "confidence": 0.1, "details": {"note": "No local translations found"}}

    # ── Fuzzy corpus matching ─────────────────────────────────────────────

    _FTS_STOP = {
        "a", "an", "the", "is", "are", "was", "were", "am", "be", "been",
        "do", "does", "did", "will", "would", "shall", "should", "can", "could",
        "have", "has", "had", "and", "or", "but", "if", "not", "no", "for", "to",
        "in", "at", "on", "by", "with", "from", "it", "he", "she", "him", "her",
        "his", "they", "them", "their", "we", "us", "our", "you", "your", "i",
        "me", "my", "of", "that", "this", "so", "as", "up", "out",
    }

    def _fuzzy_match_corpus(self, text: str, field: str) -> Optional[dict]:
        text_lower = text.lower().strip()

        # Try exact lookup first (fastest path)
        exact = self.conn.execute(
            f"SELECT tol, spanish, english FROM parallel_sentences WHERE LOWER({field}) = ? LIMIT 1",
            [text_lower],
        ).fetchone()
        if exact:
            target_field = "spanish" if field == "tol" else "tol"
            return {
                "translation": exact[target_field],
                "method": "corpus_exact",
                "confidence": 0.95,
                "details": {"matched": exact[field], "score": 1.0},
            }

        # Extract meaningful content words for search
        all_words = re.findall(r'[a-záéíóúüñ]+', text_lower)
        content_words = [w for w in all_words if w not in self._FTS_STOP and len(w) > 2]

        rows = []
        # Try FTS5: use AND for top distinctive words (finds exact/near-exact matches)
        if content_words:
            try:
                top_terms = content_words[:4]
                # AND query on the specific field — finds sentences containing all terms
                fts_and = " ".join(top_terms)
                rows = self.conn.execute(
                    f"""SELECT p.tol, p.spanish, p.english
                        FROM parallel_fts f
                        JOIN parallel_sentences p ON f.rowid = p.id
                        WHERE parallel_fts MATCH ? AND p.{field} != ''
                        LIMIT 200""",
                    [fts_and],
                ).fetchall()
                # If AND is too restrictive, fall back to OR with more terms
                if len(rows) < 5 and len(content_words) > 2:
                    fts_or = " OR ".join(content_words[:6])
                    more = self.conn.execute(
                        f"""SELECT p.tol, p.spanish, p.english
                            FROM parallel_fts f
                            JOIN parallel_sentences p ON f.rowid = p.id
                            WHERE parallel_fts MATCH ? AND p.{field} != ''
                            LIMIT 200""",
                        [fts_or],
                    ).fetchall()
                    seen = {id(r) for r in rows}
                    rows.extend(r for r in more if id(r) not in seen)
            except Exception:
                pass

        # Fallback to LIKE with content words
        if not rows and content_words:
            like_words = content_words[:4]
            clauses = " OR ".join(f"LOWER({field}) LIKE ?" for _ in like_words)
            params = [f"%{w}%" for w in like_words]
            rows = self.conn.execute(
                f"SELECT tol, spanish, english FROM parallel_sentences WHERE {field} != '' AND ({clauses}) LIMIT 500",
                params,
            ).fetchall()

        if not rows:
            return None

        best_score = 0
        best_match = None
        for r in rows:
            candidate = r[field].lower().strip()
            score = SequenceMatcher(None, text_lower, candidate).ratio()
            if score > best_score:
                best_score = score
                best_match = r

        if best_score >= 0.75 and best_match:
            target_field = "spanish" if field == "tol" else "tol"
            return {
                "translation": best_match[target_field],
                "method": "corpus_match",
                "confidence": round(best_score * 0.8, 2),
                "details": {"matched": best_match[field], "score": round(best_score, 2)},
            }
        return None

    # ── Dictionary lookup ─────────────────────────────────────────────────

    def dictionary_lookup(self, query: str, lang: str = "tol") -> list:
        query_lower = query.lower().strip()
        results = []

        if lang == "tol":
            rows = self.conn.execute(
                "SELECT tol, spanish, english, category FROM dictionary WHERE LOWER(tol) LIKE ?",
                (f"%{query_lower}%",),
            ).fetchall()
        elif lang == "es":
            rows = self.conn.execute(
                "SELECT tol, spanish, english, category FROM dictionary WHERE LOWER(spanish) LIKE ?",
                (f"%{query_lower}%",),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT tol, spanish, english, category FROM dictionary WHERE LOWER(english) LIKE ?",
                (f"%{query_lower}%",),
            ).fetchall()

        for r in rows:
            results.append({"tol": r["tol"], "spanish": r["spanish"], "english": r["english"] or "", "category": r["category"] or ""})
        return results

    # ── Stats ─────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        d = self.conn.execute("SELECT COUNT(*) FROM dictionary").fetchone()[0]
        s = self.conn.execute("SELECT COUNT(*) FROM parallel_sentences").fetchone()[0]
        v = self.conn.execute("SELECT COUNT(*) FROM verb_conjugations").fetchone()[0]

        ph = 0
        if self._table_exists("phrase_translations"):
            ph = self.conn.execute("SELECT COUNT(*) FROM phrase_translations").fetchone()[0]

        det = 0
        if self._table_exists("direct_en_tol"):
            det = self.conn.execute("SELECT COUNT(*) FROM direct_en_tol").fetchone()[0]

        en_es = 0
        if self._table_exists("en_es_dictionary"):
            en_es = self.conn.execute("SELECT COUNT(*) FROM en_es_dictionary").fetchone()[0]

        syn_en = syn_es = inferred = 0
        if self._table_exists("synonyms_en"):
            syn_en = self.conn.execute("SELECT COUNT(*) FROM synonyms_en").fetchone()[0]
        if self._table_exists("synonyms_es"):
            syn_es = self.conn.execute("SELECT COUNT(*) FROM synonyms_es").fetchone()[0]
        if self._table_exists("inferred_translations"):
            inferred = self.conn.execute("SELECT COUNT(*) FROM inferred_translations").fetchone()[0]

        return {
            "dictionary_entries": d,
            "parallel_sentences": s,
            "verb_conjugations": v,
            "phrase_translations": ph,
            "direct_en_tol": det,
            "en_es_dictionary": en_es,
            "synonyms_english": syn_en,
            "synonyms_spanish": syn_es,
            "inferred_translations": inferred,
            "total": d + s + v + ph + det + en_es + syn_en + syn_es + inferred,
        }
