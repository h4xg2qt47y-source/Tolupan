#!/usr/bin/env python3
"""
OCR Tol Grammar & Pronunciation PDFs
=======================================
Uses macOS Vision framework to OCR all PDFs in "Tol Pronunciation" folder,
extracting:
  1. Full text content for grammar analysis
  2. Example sentences (Tol + Spanish/English translations)
  3. Grammar rules (word order, verb conjugation patterns, etc.)
"""

import json
import re
import sqlite3
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
PDF_DIR = BASE / "Tol Pronunciation"
OUTPUT_DIR = BASE / "Tol Pronunciation" / "extracted_text"
DB_PATH = BASE / "app" / "data" / "tol.db"


def ocr_pdf_page(pdf_path, page_num):
    """OCR a single PDF page using macOS Vision framework."""
    import fitz  # pymupdf
    import objc
    from Quartz import (
        CGDataProviderCreateWithCFData,
        CGImageCreate,
        kCGColorSpaceGenericRGB,
        CGColorSpaceCreateWithName,
        kCGBitmapByteOrder32Big,
        kCGImageAlphaPremultipliedLast,
    )
    from Foundation import NSData
    import Vision

    doc = fitz.open(str(pdf_path))
    if page_num >= len(doc):
        return ""

    page = doc[page_num]
    pix = page.get_pixmap(dpi=300)
    img_data = pix.tobytes("png")
    doc.close()

    ns_data = NSData.dataWithBytes_length_(img_data, len(img_data))
    request = Vision.VNRecognizeTextRequest.alloc().init()
    request.setRecognitionLevel_(Vision.VNRequestTextRecognitionLevelAccurate)
    request.setRecognitionLanguages_(["es", "en"])
    request.setUsesLanguageCorrection_(True)

    handler = Vision.VNImageRequestHandler.alloc().initWithData_options_(ns_data, None)
    success = handler.performRequests_error_([request], None)

    if not success[0]:
        return ""

    results = request.results()
    if not results:
        return ""

    lines = []
    for obs in results:
        text = obs.topCandidates_(1)[0].string()
        lines.append(text)

    return "\n".join(lines)


def ocr_full_pdf(pdf_path):
    """OCR all pages of a PDF."""
    import fitz
    doc = fitz.open(str(pdf_path))
    n_pages = len(doc)
    doc.close()

    all_text = []
    for i in range(n_pages):
        page_text = ocr_pdf_page(pdf_path, i)
        if page_text.strip():
            all_text.append(f"--- PAGE {i+1} ---\n{page_text}")

    return "\n\n".join(all_text), n_pages


def extract_text_direct(pdf_path):
    """Try direct text extraction first (faster, for non-scanned PDFs)."""
    import fitz
    doc = fitz.open(str(pdf_path))
    all_text = []
    for page in doc:
        text = page.get_text()
        if text.strip():
            all_text.append(text)
    doc.close()
    return "\n".join(all_text)


def extract_example_sentences(text):
    """Extract bilingual example sentences from grammar text."""
    examples = []

    # Pattern: Tol text followed by Spanish/English in quotes or parentheses
    # Common patterns in grammars:
    # "Tol sentence" = 'Translation'
    # Tol: xyz   Spa: xyz
    # (1) Tol sentence
    #     'translation'

    # Look for numbered examples
    for m in re.finditer(
        r'(?:^|\n)\s*\(?(\d+)\)?\s*([^\n]+)\n\s*[\'"]([^\'"]+)[\'"]',
        text, re.M
    ):
        examples.append({
            "num": m.group(1),
            "tol": m.group(2).strip(),
            "translation": m.group(3).strip(),
        })

    # Look for Tol text with quoted translations
    for m in re.finditer(
        r"['\"]([^'\"]{5,80})['\"][\s.,:]*['\"]([^'\"]{5,80})['\"]",
        text
    ):
        examples.append({
            "tol": m.group(1).strip(),
            "translation": m.group(2).strip(),
        })

    # Look for pattern: Tol text / translation or Tol text = translation
    for m in re.finditer(
        r"([A-Za-záéíóúüñ'']{3,}(?:\s+[A-Za-záéíóúüñ'']+){1,10})\s*[/=]\s*([A-Za-záéíóúüñ'']{3,}(?:\s+[A-Za-záéíóúüñ'']+){1,10})",
        text
    ):
        examples.append({
            "tol": m.group(1).strip(),
            "translation": m.group(2).strip(),
        })

    return examples


def extract_grammar_rules(text):
    """Extract key grammar patterns from the text."""
    rules = []

    # Word order patterns
    wo_patterns = [
        (r'(?i)(SOV|SVO|VSO|OVS|VOS|OSV)\b.*?order', "word_order"),
        (r'(?i)word\s+order[:\s]+([^\n.]+)', "word_order"),
        (r'(?i)(?:subject|verb|object)\s+(?:precedes|follows|comes?\s+(?:before|after))\s+[^\n.]+', "word_order"),
        (r'(?i)(?:basic|typical|normal|canonical)\s+(?:sentence|clause)\s+(?:order|structure|pattern)[:\s]+([^\n.]+)', "word_order"),
    ]

    for pattern, rule_type in wo_patterns:
        for m in re.finditer(pattern, text):
            rules.append({"type": rule_type, "text": m.group(0).strip()[:200]})

    # Verb patterns
    verb_patterns = [
        (r'(?i)verb\s+(?:prefix|suffix|affix|marker|conjugat)[^\n.]*', "verb_morphology"),
        (r'(?i)(?:tense|aspect|mood)\s+(?:marker|suffix|prefix)[^\n.]*', "verb_morphology"),
        (r'(?i)(?:past|present|future)\s+tense[:\s]+[^\n.]+', "verb_morphology"),
    ]

    for pattern, rule_type in verb_patterns:
        for m in re.finditer(pattern, text):
            rules.append({"type": rule_type, "text": m.group(0).strip()[:200]})

    # Noun/pronoun patterns
    noun_patterns = [
        (r'(?i)(?:personal\s+)?pronoun[s]?\s*[:\-][^\n]+', "pronouns"),
        (r'(?i)(?:possessive|demonstrative)\s+[^\n.]+', "determiners"),
        (r'(?i)(?:plural|number)\s+(?:marker|suffix|prefix)[^\n.]+', "number_marking"),
    ]

    for pattern, rule_type in noun_patterns:
        for m in re.finditer(pattern, text):
            rules.append({"type": rule_type, "text": m.group(0).strip()[:200]})

    # Negation patterns
    for m in re.finditer(r'(?i)negat(?:ion|ive)[:\s]+[^\n.]+', text):
        rules.append({"type": "negation", "text": m.group(0).strip()[:200]})

    return rules


def main():
    t0 = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  OCR TOL GRAMMAR & PRONUNCIATION PDFs")
    print("=" * 70)

    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    print(f"\n  Found {len(pdfs)} PDF files")

    all_examples = []
    all_rules = []
    all_texts = {}

    for pdf in pdfs:
        print(f"\n  Processing: {pdf.name}")

        # Try direct text extraction first
        direct_text = extract_text_direct(pdf)
        if len(direct_text.strip()) > 500:
            print(f"    Direct extraction: {len(direct_text):,} chars")
            text = direct_text
        else:
            print(f"    Direct extraction sparse ({len(direct_text)} chars), running OCR...")
            text, n_pages = ocr_full_pdf(pdf)
            print(f"    OCR: {n_pages} pages → {len(text):,} chars")

        # Save text
        out_file = OUTPUT_DIR / f"{pdf.stem}.txt"
        out_file.write_text(text, encoding="utf-8")
        all_texts[pdf.name] = text

        # Extract examples and rules
        examples = extract_example_sentences(text)
        rules = extract_grammar_rules(text)

        print(f"    Examples found: {len(examples)}")
        print(f"    Grammar rules:  {len(rules)}")

        for e in examples:
            e["source"] = pdf.stem
        for r in rules:
            r["source"] = pdf.stem

        all_examples.extend(examples)
        all_rules.extend(rules)

    # Also process the HTML file
    html_file = PDF_DIR / "Tol_Jicaque_Wikipedia.html"
    if html_file.exists():
        print(f"\n  Processing: {html_file.name}")
        html_text = html_file.read_text(encoding="utf-8")
        clean = re.sub(r'<[^>]+>', ' ', html_text)
        clean = re.sub(r'\s+', ' ', clean)
        (OUTPUT_DIR / "Tol_Jicaque_Wikipedia.txt").write_text(clean, encoding="utf-8")
        examples = extract_example_sentences(clean)
        rules = extract_grammar_rules(clean)
        print(f"    Examples: {len(examples)}, Rules: {len(rules)}")
        for e in examples:
            e["source"] = "Wikipedia"
        for r in rules:
            r["source"] = "Wikipedia"
        all_examples.extend(examples)
        all_rules.extend(rules)

    # Save extracted data
    (OUTPUT_DIR / "all_examples.json").write_text(
        json.dumps(all_examples, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (OUTPUT_DIR / "all_grammar_rules.json").write_text(
        json.dumps(all_rules, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  RESULTS")
    print(f"{'='*70}")
    print(f"  PDFs processed:       {len(pdfs)}")
    print(f"  Example sentences:    {len(all_examples)}")
    print(f"  Grammar rules:        {len(all_rules)}")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"  Time: {elapsed:.1f}s")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
