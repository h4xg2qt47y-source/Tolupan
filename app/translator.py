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
    "i": "naph", "me": "naph",
    "you": "hiph",
    "he": "huph", "him": "huph",
    "she": "huph", "her": "huph",
    "it": "huph",
    "we": "kuph", "us": "kuph",
    "they": "yuph", "them": "yuph",
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
    "your": "he-", "yours": "he-",
    "his": "hu-", "her": "hu-", "its": "hu-",
    "our": "ka-", "ours": "ka-",
    "their": "sya-", "theirs": "sya-",
}

_1SG_M = set("pb")
_1SG_N = set("kgtd")
_3SG_HO = set("mwpbl")


def tol_possessive_prefix(en_possessive: str, tol_stem: str) -> str:
    """Select the correct possessive prefix based on the stem's first phoneme."""
    poss = en_possessive.lower()
    first = ""
    for c in tol_stem:
        if c not in "-=_ ":
            first = c
            break
    if not first:
        return tol_stem

    if poss in ("my", "mine"):
        if first in _1SG_M:
            return "m-" + tol_stem
        if first in _1SG_N:
            return "n-" + tol_stem
        return "na-" + tol_stem
    if poss in ("his", "her", "its"):
        if first in _3SG_HO:
            return "ho-" + tol_stem
        return "hu-" + tol_stem
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
    "from": "napé",
    "under": "'alá", "below": "'alá", "beneath": "'alá",
    "above": "hay", "over": "hay",
    "behind": "phyapha'a",
    "after": "khüil",
    "around": "t'asiyú",
}

# Words that introduce prepositional phrases in English
EN_PREPOSITIONS = {
    "to", "in", "at", "on", "inside", "into", "with", "for", "because",
    "from", "around", "under", "below", "beneath", "above", "over",
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
    "what": "chan", "which": "chan",
    "who": "phakh", "whom": "phakh",
    "where": "ka'ah",
    "when": "'ona",
    "how": "'oyn",
    "why": "chanmpes",
    "how many": "nol", "how much": "nol",
}

TOL_NEGATION = "ma"
TOL_NEG_COPULA = "tulukh"

EN_STOPWORDS = {
    "a", "an", "the", "is", "are", "was", "were", "am", "be", "been",
    "being", "do", "does", "did", "will", "would", "shall", "should",
    "can", "could", "may", "might", "must", "have", "has", "had",
    "having", "of", "that", "this", "these", "those", "which",
    "there", "here", "very", "just", "also", "too", "so",
    "when", "then", "order", "more", "like", "as",
}

EN_NEGATION_WORDS = {"not", "no", "never", "neither", "nor", "don't", "doesn't", "didn't", "won't", "can't", "isn't", "aren't", "wasn't", "weren't", "haven't", "hasn't", "hadn't"}

EN_BE_FORMS = {"is", "are", "was", "were", "am", "be", "been", "being"}


class TolTranslator:
    def __init__(self):
        self.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._build_lookup_caches()

    def _build_lookup_caches(self):
        self.tol_to_spanish = {}
        self.spanish_to_tol = {}
        self.tol_to_english = {}
        self.english_to_tol = {}

        _dict_source_priority = {
            "SIL_Dictionary_OCR": 5,
            "Elicited_Grammar_Aurelio": 4,
        }
        _en_tol_source: dict[str, int] = {}
        rows = self.conn.execute("SELECT tol, spanish, english, category, source FROM dictionary").fetchall()
        for r in rows:
            tol_lower = r["tol"].lower().strip()
            spa_lower = r["spanish"].lower().strip()
            self.tol_to_spanish[tol_lower] = {
                "spanish": r["spanish"], "english": r["english"] or "", "category": r["category"] or ""
            }
            self.spanish_to_tol[spa_lower] = {
                "tol": r["tol"], "english": r["english"] or "", "category": r["category"] or ""
            }
            if r["english"]:
                eng_lower = r["english"].lower().strip()
                self.tol_to_english[tol_lower] = r["english"]
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
                entry = {"tol": r["tol"], "confidence": r["confidence"], "_priority": priority, "source": r["source"] or ""}
                self.direct_en_tol_all[eng].append(entry)
                existing = self.direct_en_tol.get(eng)
                if (not existing
                    or priority > existing.get("_priority", 0)
                    or (priority == existing.get("_priority", 0) and r["confidence"] > existing.get("confidence", 0))):
                    self.direct_en_tol[eng] = entry

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
        if w in EN_STOPWORDS or w in EN_NEGATION_WORDS:
            return None

        if w in self.direct_en_tol:
            e = self.direct_en_tol[w]
            return {"tol": e["tol"], "confidence": e["confidence"], "method": "direct_en_tol"}

        if w in self.english_to_tol:
            return {"tol": self.english_to_tol[w], "confidence": 0.9, "method": "dictionary"}

        if w in self.inferred_en_to_tol:
            best = max(self.inferred_en_to_tol[w], key=lambda x: x["confidence"])
            return {"tol": best["tol"], "confidence": best["confidence"], "method": "inferred"}

        return None

    # ── Tol Grammar Engine ─────────────────────────────────────────────────

    def _apply_tol_grammar(self, en_words: list, tol_parts: list, word_map: dict) -> str:
        """
        Restructure translated words according to Tol grammar rules.

        Tol is SOV with postpositions. The grammar engine:
        1. Identifies subject, verb, and object from English SVO structure
        2. Detects English verbs via a simple heuristic (position after subject, BE forms)
        3. Converts prepositions to postpositions (noun + postposition)
        4. Places question words sentence-initially
        5. Places negation particle ma= before verb, or tulukh for copular negation
        6. Outputs Subject + PP-phrases + Object + (ma=) + Verb
        """
        en_lower = [w.lower() for w in en_words]
        n = len(en_lower)

        is_negated = any(w in EN_NEGATION_WORDS for w in en_lower)
        has_be = any(w in EN_BE_FORMS for w in en_lower)

        # "has no X" / "have no X" → X tulukh (negative existential)
        en_has_forms = {"has", "have", "had"}
        has_no_pattern = is_negated and any(w in en_has_forms for w in en_lower) and not has_be

        # Find the verb position in English. In SVO, verb follows the first noun phrase.
        # Strategy: scan left-to-right; first content word = subject, first BE/auxiliary or
        # content word after subject = verb, rest = object/complements.
        en_verb_forms = EN_BE_FORMS | {"go", "goes", "went", "gone", "going",
            "come", "comes", "came", "coming", "see", "sees", "saw", "seen", "seeing",
            "eat", "eats", "ate", "eaten", "eating", "drink", "drinks", "drank", "drunk", "drinking",
            "kill", "kills", "killed", "killing", "speak", "speaks", "spoke", "spoken", "speaking",
            "say", "says", "said", "saying", "walk", "walks", "walked", "walking",
            "live", "lives", "lived", "living", "sleep", "sleeps", "slept", "sleeping",
            "work", "works", "worked", "working", "run", "runs", "ran", "running",
            "sit", "sits", "sat", "sitting", "stand", "stands", "stood", "standing",
            "die", "dies", "died", "dying", "give", "gives", "gave", "given", "giving",
            "take", "takes", "took", "taken", "taking", "make", "makes", "made", "making",
            "know", "knows", "knew", "known", "knowing", "think", "thinks", "thought", "thinking",
            "want", "wants", "wanted", "wanting", "need", "needs", "needed", "needing",
            "like", "likes", "liked", "liking", "love", "loves", "loved", "loving",
            "put", "puts", "putting", "get", "gets", "got", "gotten", "getting",
            "find", "finds", "found", "finding", "tell", "tells", "told", "telling",
            "call", "calls", "called", "calling", "try", "tries", "tried", "trying",
            "leave", "leaves", "left", "leaving", "bring", "brings", "brought", "bringing",
            "fall", "falls", "fell", "fallen", "falling", "burn", "burns", "burned", "burning",
            "cut", "cuts", "cutting", "plant", "plants", "planted", "planting",
            "split", "splits", "splitting", "carry", "carries", "carried", "carrying",
            "teach", "teaches", "taught", "teaching", "learn", "learns", "learned", "learning",
            "buy", "buys", "bought", "buying", "sell", "sells", "sold", "selling",
            "write", "writes", "wrote", "written", "writing",
            "read", "reads", "reading", "sing", "sings", "sang", "sung", "singing",
            "play", "plays", "played", "playing", "help", "helps", "helped", "helping",
            "open", "opens", "opened", "opening", "close", "closes", "closed", "closing",
            "break", "breaks", "broke", "broken", "breaking",
            "hold", "holds", "held", "holding", "build", "builds", "built", "building",
            "born", "roam", "roams", "roamed"}

        # Pre-process possessives: merge "my X" → prefix+X in word_map
        for i, w in enumerate(en_lower):
            if w in TOL_POSSESSIVES:
                for j in range(i + 1, n):
                    wj = en_lower[j]
                    if wj in EN_STOPWORDS:
                        continue
                    tol_noun = word_map.get(j)
                    if tol_noun and not tol_noun.startswith("["):
                        word_map[j] = tol_possessive_prefix(w, tol_noun)
                    break

        # Classify each English word
        ROLE_SUBJ = "S"
        ROLE_VERB = "V"
        ROLE_OBJ = "O"

        roles = {}
        phase = "pre_subj"  # pre_subj → subj → verb → obj

        for i, w in enumerate(en_lower):
            if w in TOL_POSSESSIVES:
                roles[i] = "skip"
                continue
            if w in EN_PREPOSITIONS:
                pass  # handled below by phase logic
            elif w in EN_STOPWORDS and w not in TOL_PRONOUNS:
                roles[i] = "skip"
                continue
            if w in EN_NEGATION_WORDS:
                roles[i] = "neg"
                continue
            if w in TOL_QUESTION_WORDS:
                roles[i] = "question"
                continue

            if phase == "pre_subj":
                if w in EN_PREPOSITIONS:
                    roles[i] = "prep"
                elif w in TOL_PRONOUNS or word_map.get(i):
                    roles[i] = ROLE_SUBJ
                    phase = "subj"
                elif w in en_verb_forms:
                    roles[i] = ROLE_VERB
                    phase = "obj"
                else:
                    roles[i] = ROLE_SUBJ
                    phase = "subj"
            elif phase == "subj":
                if w in en_verb_forms:
                    roles[i] = ROLE_VERB
                    phase = "obj"
                elif w in EN_PREPOSITIONS:
                    roles[i] = "prep"
                else:
                    roles[i] = ROLE_SUBJ
            elif phase == "obj":
                if w in EN_PREPOSITIONS:
                    roles[i] = "prep"
                else:
                    roles[i] = ROLE_OBJ

        # Build output parts
        question_parts = []
        subject_parts = []
        verb_parts = []
        object_parts = []
        pp_phrases = []

        i = 0
        while i < n:
            w = en_lower[i]
            role = roles.get(i, "skip")

            if role == "question":
                q = w
                if i + 1 < n and (w + " " + en_lower[i + 1]) in TOL_QUESTION_WORDS:
                    q = w + " " + en_lower[i + 1]
                    i += 1
                question_parts.append(TOL_QUESTION_WORDS.get(q, q))

            elif role == "neg":
                pass

            elif role == "skip":
                if w in TOL_POSTPOSITIONS:
                    postp = TOL_POSTPOSITIONS[w]
                    pp_nouns = []
                    j = i + 1
                    while j < n:
                        wj = en_lower[j]
                        if wj in TOL_POSSESSIVES or (wj in EN_STOPWORDS and wj not in TOL_PRONOUNS):
                            j += 1
                            continue
                        if word_map.get(j):
                            pp_nouns.append(word_map[j])
                            word_map[j] = None
                            j += 1
                            break
                        elif wj in TOL_PRONOUNS:
                            pp_nouns.append(TOL_PRONOUNS[wj])
                            j += 1
                            break
                        else:
                            break
                    if pp_nouns:
                        pp_phrases.append((" ".join(pp_nouns), postp))

            elif role == ROLE_SUBJ:
                if w in TOL_PRONOUNS:
                    subject_parts.append(TOL_PRONOUNS[w])
                elif word_map.get(i):
                    subject_parts.append(word_map[i])

            elif role == ROLE_VERB:
                tol_v = word_map.get(i)
                if tol_v:
                    verb_parts.append(tol_v)

            elif role == ROLE_OBJ:
                if w in TOL_PRONOUNS:
                    object_parts.append(TOL_PRONOUNS[w])
                elif word_map.get(i):
                    object_parts.append(word_map[i])

            elif role == "prep":
                postp = TOL_POSTPOSITIONS.get(w, w)
                pp_nouns = []
                j = i + 1
                while j < n:
                    wj = en_lower[j]
                    if wj in TOL_POSSESSIVES or (wj in EN_STOPWORDS and wj not in TOL_PRONOUNS):
                        j += 1
                        continue
                    if word_map.get(j):
                        pp_nouns.append(word_map[j])
                        word_map[j] = None
                        j += 1
                        break
                    elif wj in TOL_PRONOUNS:
                        pp_nouns.append(TOL_PRONOUNS[wj])
                        j += 1
                        break
                    else:
                        break
                if pp_nouns:
                    pp_phrases.append((" ".join(pp_nouns), postp))

            i += 1

        # Determine subject person for verb conjugation
        subject_person = None
        for w in en_lower:
            if w in EN_PRONOUN_PERSON:
                subject_person = EN_PRONOUN_PERSON[w]
                break

        # Conjugate verbs if paradigm data available
        if subject_person and verb_parts and self.verb_paradigms:
            en_word_set = set(en_lower)
            conjugated = []
            for v_tol in verb_parts:
                matched = False
                for en_verb, paradigm in self.verb_paradigms.items():
                    tol_forms = set(paradigm.values())
                    if v_tol in tol_forms or en_verb in en_word_set:
                        if subject_person in paradigm:
                            conjugated.append(paradigm[subject_person])
                            matched = True
                            break
                if not matched:
                    conjugated.append(v_tol)
            verb_parts = conjugated

        # Assemble in Tol SOV order
        result = []

        if question_parts:
            result.extend(question_parts)

        result.extend(subject_parts)

        for noun_tol, postp in pp_phrases:
            result.append(noun_tol)
            result.append(postp)

        result.extend(object_parts)

        # Detect comparative pattern
        is_comparative = "than" in en_lower or "more" in en_lower
        en_like_words = {"like", "similar", "same"}
        is_equal = any(w in en_like_words for w in en_lower)

        if is_negated:
            if has_no_pattern:
                result.append(TOL_NEG_COPULA)
            elif has_be and not verb_parts:
                result.append(TOL_NEG_COPULA)
            else:
                result.append(TOL_NEGATION)
                result.extend(verb_parts)
        elif has_be and not verb_parts and not is_negated and object_parts:
            if is_equal:
                result.append(TOL_EQUAL_LIKE)
            else:
                result.append(TOL_COPULA)
        else:
            result.extend(verb_parts)

        result = [r for r in result if r and r.strip() and not r.startswith("[")]

        output = " ".join(result) if result else " ".join([p for p in tol_parts if not p.startswith("[")])
        return self._capitalize_proper_nouns(output)

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

    def _tol_to_spanish(self, text: str) -> dict:
        text_lower = text.lower().strip()

        if text_lower in self.tol_to_spanish:
            entry = self.tol_to_spanish[text_lower]
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
                if w in self.tol_to_spanish:
                    translated.append(self.tol_to_spanish[w]["spanish"])
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

        return {"translation": f"[No se encontró traducción para: {text}]", "method": "not_found", "confidence": 0, "details": {}}

    # ── Spanish → Tol ─────────────────────────────────────────────────────

    _SPA_STOPWORDS = {
        "el", "la", "los", "las", "un", "una", "unos", "unas",
        "de", "del", "al", "a", "en", "con", "por", "para",
        "es", "son", "está", "están", "ser", "fue", "era",
        "y", "o", "pero", "que", "como", "muy", "más", "no",
        "yo", "tú", "él", "ella", "nosotros", "ellos", "ellas",
        "mi", "tu", "su", "nuestro", "nuestra", "sus", "mis",
        "se", "lo", "le", "me", "te", "nos", "les",
        "este", "esta", "ese", "esa", "esto", "eso",
    }

    def _spanish_to_tol(self, text: str) -> dict:
        text_lower = text.lower().strip()
        candidates = []

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

        # 2. Full-phrase inferred
        if text_lower in self.inferred_es_to_tol:
            best = max(self.inferred_es_to_tol[text_lower], key=lambda x: x["confidence"])
            candidates.append({"text": best["tol"], "method": "synonym_inferred", "confidence": best["confidence"]})

        # 3. Word-by-word translation (try BEFORE corpus to avoid false positives)
        original_words = text.strip().split()
        words = text_lower.split()
        if len(words) > 1:
            translated, untranslated = [], []
            for i, w in enumerate(words):
                if w in self._SPA_STOPWORDS:
                    continue
                if w in self.spanish_to_tol:
                    translated.append(self.spanish_to_tol[w]["tol"])
                elif w in self.inferred_es_to_tol:
                    best = max(self.inferred_es_to_tol[w], key=lambda x: x["confidence"])
                    translated.append(best["tol"])
                else:
                    orig = original_words[i] if i < len(original_words) else w
                    if orig and orig[0].isupper():
                        translated.append(orig)
                    else:
                        translated.append(f"[{w}]")
                        untranslated.append(w)

            content_words = [w for w in words if w not in self._SPA_STOPWORDS]
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

        if text_lower in self.tol_to_english:
            return {
                "translation": self.tol_to_english[text_lower],
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
