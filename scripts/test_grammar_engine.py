#!/usr/bin/env python3
"""
Generate 500 novel English→Tol and 500 novel Spanish→Tol test sentences
using only verified vocabulary. Runs them through the grammar engine
(corpus matching disabled) and reports accuracy.
"""

from __future__ import annotations

import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
from translator import (
    TolTranslator,
    tol_phonetic_normalize,
    TOL_PRONOUNS,
    TOL_POSTPOSITIONS,
    TOL_POSSESSIVES,
    TOL_NEGATION,
    TOL_COPULA,
    TOL_QUESTION_WORDS,
)

random.seed(42)

t = TolTranslator()
original_fuzzy = t._fuzzy_match_corpus
t._fuzzy_match_corpus = lambda *a, **kw: None

# ── Verified vocabulary ─────────────────────────────────────────────────

NOUNS = {}
VERBS = {}
ADJS = {}

for w in ["water", "fish", "fire", "house", "mountain", "river", "food", "tree",
          "man", "woman", "child", "father", "mother", "brother", "sister",
          "god", "people", "animal", "dog", "bird", "corn", "earth", "stone",
          "village", "road", "field", "night", "day", "sun", "moon", "rain",
          "wind", "hand", "foot", "head", "eye", "heart", "blood", "mouth",
          "song", "word", "truth", "sin", "spirit", "soul", "body", "land",
          "heaven", "king", "servant", "church", "book", "name", "friend",
          "enemy", "money", "bread", "fruit", "seed", "door", "boat", "cross",
          "temple", "law", "prophet", "apostle", "angel", "sheep", "snake",
          "clothes", "rope", "knife", "pot", "salt", "honey", "milk", "oil",
          "bone", "skin", "hair", "nose", "ear", "arm", "leg", "shoulder",
          "husband", "wife", "son", "daughter"]:
    r = t._lookup_en_word(w)
    if r:
        NOUNS[w] = r["tol"]

for w in ["eat", "drink", "sleep", "walk", "speak", "see", "kill", "give",
          "come", "go", "know", "think", "want", "need", "love", "make",
          "take", "find", "tell", "call", "leave", "bring", "fall", "burn",
          "cut", "plant", "teach", "learn", "buy", "sell", "write", "read",
          "sing", "play", "help", "open", "close", "break", "hold", "build",
          "work", "cook", "wash", "dance", "fight", "cry", "pray",
          "die", "live", "sit", "stand", "fear", "serve", "praise", "heal",
          "forgive", "save", "follow", "obey", "believe", "send", "receive",
          "hear", "worship", "enter", "sow", "gather", "destroy",
          "hide", "steal", "lead", "judge", "run", "carry"]:
    r = t._lookup_en_word(w)
    if r:
        VERBS[w] = r["tol"]

for w in ["good", "bad", "big", "small", "old", "new", "hot", "cold",
          "strong", "weak", "clean", "dirty", "beautiful", "rich", "poor",
          "happy", "sad", "angry", "hungry", "thirsty", "tired", "sick",
          "wise", "foolish", "great", "holy", "righteous", "evil", "true",
          "white", "black", "red", "young", "hard", "long", "short",
          "fast", "slow", "dark", "bright", "empty", "full", "dead", "alive"]:
    r = t._lookup_en_word(w)
    if r:
        ADJS[w] = r["tol"]

SUBJECTS = {
    "I": "naph", "he": "huph", "she": "huph", "we": "kuph",
    "they": "yuph", "you": "hiph",
}
POSSESSIVES = {
    "my": "na-", "his": "hu-", "her": "hu-", "our": "ka-",
    "their": "sya-", "your": "he-",
}
PREPS = {
    "to": "nt'a", "in": "nt'a", "with": "lal", "for": "mpes", "from": "napé",
}

noun_list = list(NOUNS.keys())
verb_list = list(VERBS.keys())
adj_list = list(ADJS.keys())
subj_list = list(SUBJECTS.keys())
prep_list = list(PREPS.keys())
poss_list = list(POSSESSIVES.keys())


# ── Sentence templates ──────────────────────────────────────────────────

def gen_svo():
    """Subject Verb Object: 'I eat fish'"""
    s = random.choice(subj_list)
    v = random.choice(verb_list)
    o = random.choice(noun_list)
    en = f"{s} {v} the {o}"
    tol_expected = [SUBJECTS[s], NOUNS[o], VERBS[v]]
    return en, tol_expected

def gen_svo_no_article():
    """Subject Verb Object without article: 'He eats food'"""
    s = random.choice(subj_list)
    v = random.choice(verb_list)
    o = random.choice(noun_list)
    en = f"{s} {v} {o}"
    tol_expected = [SUBJECTS[s], NOUNS[o], VERBS[v]]
    return en, tol_expected

def gen_adj_s_v_o():
    """Adjective Subject Verb Object: 'The good man eats fish'"""
    adj = random.choice(adj_list)
    s_noun = random.choice(noun_list)
    v = random.choice(verb_list)
    o = random.choice(noun_list)
    while o == s_noun:
        o = random.choice(noun_list)
    en = f"the {adj} {s_noun} {v} the {o}"
    tol_expected = [ADJS[adj], NOUNS[s_noun], NOUNS[o], VERBS[v]]
    return en, tol_expected

def gen_s_v_adj_o():
    """Subject Verb Adjective Object: 'He eats good food'"""
    s = random.choice(subj_list)
    v = random.choice(verb_list)
    adj = random.choice(adj_list)
    o = random.choice(noun_list)
    en = f"{s} {v} the {adj} {o}"
    tol_expected = [SUBJECTS[s], ADJS[adj], NOUNS[o], VERBS[v]]
    return en, tol_expected

def gen_s_v_pp():
    """Subject Verb Prep Object: 'I go to the house'"""
    s = random.choice(subj_list)
    v = random.choice(verb_list)
    p = random.choice(prep_list)
    o = random.choice(noun_list)
    en = f"{s} {v} {p} the {o}"
    tol_expected = [SUBJECTS[s], NOUNS[o], PREPS[p], VERBS[v]]
    return en, tol_expected

def gen_s_v_o_pp():
    """Subject Verb Object Prep Object: 'He gives food to the man'"""
    s = random.choice(subj_list)
    v = random.choice(verb_list)
    o1 = random.choice(noun_list)
    p = random.choice(prep_list)
    o2 = random.choice(noun_list)
    while o2 == o1:
        o2 = random.choice(noun_list)
    en = f"{s} {v} the {o1} {p} the {o2}"
    tol_expected = [SUBJECTS[s], NOUNS[o2], PREPS[p], NOUNS[o1], VERBS[v]]
    return en, tol_expected

def gen_poss_s_v():
    """Possessive Subject Verb: 'My father speaks'"""
    poss = random.choice(poss_list)
    n = random.choice(noun_list)
    v = random.choice(verb_list)
    en = f"{poss} {n} {v}"
    tol_expected = [NOUNS[n], VERBS[v]]
    return en, tol_expected

def gen_s_v_poss_o():
    """Subject Verb Possessive Object: 'I eat my food'"""
    s = random.choice(subj_list)
    v = random.choice(verb_list)
    poss = random.choice(poss_list)
    o = random.choice(noun_list)
    en = f"{s} {v} {poss} {o}"
    tol_expected = [SUBJECTS[s], NOUNS[o], VERBS[v]]
    return en, tol_expected

def gen_copula():
    """Subject is Adjective: 'The man is good'"""
    s_noun = random.choice(noun_list)
    adj = random.choice(adj_list)
    en = f"the {s_noun} is {adj}"
    tol_expected = [NOUNS[s_noun], ADJS[adj]]
    return en, tol_expected

def gen_neg_svo():
    """Negated SVO: 'I don't eat fish'"""
    s = random.choice(subj_list)
    v = random.choice(verb_list)
    o = random.choice(noun_list)
    neg = random.choice(["don't", "doesn't", "didn't"])
    en = f"{s} {neg} {v} the {o}"
    tol_expected = [SUBJECTS[s], NOUNS[o], "ma", VERBS[v]]
    return en, tol_expected

def gen_question():
    """Question: 'Where is the water?'"""
    qw = random.choice(["where", "what", "who"])
    n = random.choice(noun_list)
    en = f"{qw} is the {n}?"
    tol_q = TOL_QUESTION_WORDS[qw]
    tol_expected = [tol_q, NOUNS[n]]
    return en, tol_expected


TEMPLATES = [
    (gen_svo, 80),
    (gen_svo_no_article, 60),
    (gen_adj_s_v_o, 60),
    (gen_s_v_adj_o, 60),
    (gen_s_v_pp, 50),
    (gen_s_v_o_pp, 40),
    (gen_poss_s_v, 30),
    (gen_s_v_poss_o, 40),
    (gen_copula, 40),
    (gen_neg_svo, 25),
    (gen_question, 15),
]

# ── Generate and test ───────────────────────────────────────────────────

def check_translation(en: str, tol_expected_words: list[str]) -> dict:
    result = t.translate(en, "en", "tol")
    actual = result["translation"]
    actual_norm = tol_phonetic_normalize(actual)
    actual_words = set(actual_norm.split())

    hits = 0
    misses = []
    for ew in tol_expected_words:
        ew_norm = tol_phonetic_normalize(ew)
        ew_parts = ew_norm.split()
        found = False
        for ep in ew_parts:
            if ep in actual_words:
                found = True
                break
            for aw in actual_words:
                if len(ep) >= 3 and (ep in aw or aw in ep):
                    found = True
                    break
            if found:
                break
        if found:
            hits += 1
        else:
            misses.append(ew)

    score = hits / len(tol_expected_words) if tol_expected_words else 1.0
    return {
        "english": en,
        "expected_words": tol_expected_words,
        "actual": actual,
        "method": result["method"],
        "score": score,
        "misses": misses,
    }


print("=" * 80)
print("ENGLISH → TOL: 500 Novel Sentence Tests")
print("=" * 80)

all_results = []
template_stats = defaultdict(lambda: {"total": 0, "scores": []})

for template_fn, count in TEMPLATES:
    for _ in range(count):
        en, expected = template_fn()
        result = check_translation(en, expected)
        result["template"] = template_fn.__name__
        all_results.append(result)
        template_stats[template_fn.__name__]["total"] += 1
        template_stats[template_fn.__name__]["scores"].append(result["score"])

total = len(all_results)
avg_score = sum(r["score"] for r in all_results) / total
perfect = sum(1 for r in all_results if r["score"] == 1.0)
good = sum(1 for r in all_results if r["score"] >= 0.75)
ok = sum(1 for r in all_results if r["score"] >= 0.5)
bad = sum(1 for r in all_results if r["score"] < 0.5)

print(f"\n--- Overall Results ---")
print(f"  Total sentences:   {total}")
print(f"  Avg word-hit score: {avg_score:.1%}")
print(f"  Perfect (100%):    {perfect:>4} ({perfect/total*100:.1f}%)")
print(f"  Good (>=75%):      {good:>4} ({good/total*100:.1f}%)")
print(f"  OK (>=50%):        {ok:>4} ({ok/total*100:.1f}%)")
print(f"  Bad (<50%):        {bad:>4} ({bad/total*100:.1f}%)")

print(f"\n--- Per-Template Breakdown ---")
for name, stats in sorted(template_stats.items()):
    avg = sum(stats["scores"]) / len(stats["scores"])
    perf = sum(1 for s in stats["scores"] if s == 1.0)
    print(f"  {name:25s}  n={stats['total']:>3}  avg={avg:.0%}  perfect={perf}/{stats['total']}")

# Analyze common failure patterns
miss_counter = Counter()
for r in all_results:
    for m in r["misses"]:
        miss_counter[m] += 1

print(f"\n--- Most Commonly Missed Tol Words ---")
for word, count in miss_counter.most_common(20):
    # Find the English word that maps to this
    eng_for = [k for k, v in {**NOUNS, **VERBS, **ADJS}.items() if v == word]
    print(f"  '{word}' (en: {eng_for[:3]}) missed {count} times")

# Show sample failures
failures = [r for r in all_results if r["score"] < 0.5]
print(f"\n--- Sample Failures (score < 50%) ---")
for r in failures[:25]:
    print(f"  [{r['score']:.0%}] EN: {r['english']}")
    print(f"       Got: {r['actual']}")
    print(f"       Expected words: {r['expected_words']}")
    print(f"       Missed: {r['misses']}")
    print(f"       Template: {r['template']}")
    print()

# Save full results
with open("/tmp/en_tol_test_results.json", "w") as f:
    json.dump(all_results, f, ensure_ascii=False, indent=2)

print(f"\nFull results saved to /tmp/en_tol_test_results.json")

PYEOF
