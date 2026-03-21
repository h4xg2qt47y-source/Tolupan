#!/usr/bin/env python3
"""
Multi-pass OCR of the Dennis & Dennis 1983 Tol-Spanish Dictionary.

Pass 1: macOS Vision (cached in ocr_raw_pages.json)
Pass 2: EasyOCR with Spanish model
Pass 3: macOS Vision at 600 DPI (higher resolution re-scan)

Each pass produces character-level text per page. The merge step combines
all passes to maximize word-level accuracy.
"""

import json, os, sys, re, time, unicodedata
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent
PDF_PATH = PROJ / "Tol Translation" / "DiccTol_Jicaque_Espanol_Dennis_1983.pdf"
CACHE_DIR = PROJ / "scripts"
PASS1_CACHE = CACHE_DIR / "ocr_raw_pages.json"
PASS2_CACHE = CACHE_DIR / "ocr_easyocr_pages.json"
PASS3_CACHE = CACHE_DIR / "ocr_vision_600dpi_pages.json"
MERGED_CACHE = CACHE_DIR / "ocr_merged_pages.json"

TOL_PAGES = list(range(12, 81))      # pages 12-80: Tol→Spanish
SPA_PAGES = list(range(84, 129))     # pages 84-128: Spanish→Tol
ALL_DICT_PAGES = TOL_PAGES + SPA_PAGES


def run_easyocr_pass():
    """Pass 2: EasyOCR with Spanish language model."""
    if PASS2_CACHE.exists():
        print(f"  EasyOCR cache exists ({PASS2_CACHE.name}), loading...")
        return json.load(open(PASS2_CACHE))

    import fitz
    import easyocr

    print("  Initializing EasyOCR (Spanish + English)...")
    reader = easyocr.Reader(["es", "en"], gpu=False)
    doc = fitz.open(str(PDF_PATH))

    results = {}
    t0 = time.time()
    for pg_num in ALL_DICT_PAGES:
        page = doc[pg_num]
        mat = fitz.Matrix(3.0, 3.0)  # 300 DPI
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")

        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            f.write(img_bytes)
            tmp_path = f.name

        try:
            detections = reader.readtext(tmp_path, detail=1, paragraph=True)
            lines = []
            for det in sorted(detections, key=lambda d: (d[0][0][1], d[0][0][0])):
                lines.append(det[1])
            results[str(pg_num)] = "\n".join(lines)
        finally:
            os.unlink(tmp_path)

        if (pg_num - ALL_DICT_PAGES[0] + 1) % 10 == 0:
            elapsed = time.time() - t0
            done = pg_num - ALL_DICT_PAGES[0] + 1
            print(f"    EasyOCR: {done}/{len(ALL_DICT_PAGES)} pages ({elapsed:.0f}s)")

    with open(PASS2_CACHE, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"  EasyOCR done: {len(results)} pages in {time.time()-t0:.0f}s")
    return results


def run_vision_600dpi_pass():
    """Pass 3: macOS Vision at 600 DPI."""
    if PASS3_CACHE.exists():
        print(f"  Vision 600DPI cache exists ({PASS3_CACHE.name}), loading...")
        return json.load(open(PASS3_CACHE))

    import fitz
    try:
        import Vision
        import Quartz
        from Foundation import NSData
    except ImportError:
        print("  macOS Vision not available, skipping pass 3")
        return {}

    doc = fitz.open(str(PDF_PATH))
    results = {}
    t0 = time.time()

    for pg_num in ALL_DICT_PAGES:
        page = doc[pg_num]
        mat = fitz.Matrix(6.0, 6.0)  # 600 DPI
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")

        ns_data = NSData.dataWithBytes_length_(img_bytes, len(img_bytes))
        ci_image = Quartz.CIImage.imageWithData_(ns_data)

        request = Vision.VNRecognizeTextRequest.alloc().init()
        request.setRecognitionLevel_(Vision.VNRequestTextRecognitionLevelAccurate)
        request.setRecognitionLanguages_(["es", "en"])
        request.setUsesLanguageCorrection_(True)

        handler = Vision.VNImageRequestHandler.alloc().initWithCIImage_options_(ci_image, None)
        success = handler.performRequests_error_([request], None)

        observations = request.results() or []
        lines = []
        sorted_obs = sorted(observations, key=lambda o: -o.boundingBox().origin.y)
        for obs in sorted_obs:
            top = obs.topCandidates_(1)
            if top:
                lines.append(top[0].string())

        results[str(pg_num)] = "\n".join(lines)

        if (pg_num - ALL_DICT_PAGES[0] + 1) % 10 == 0:
            elapsed = time.time() - t0
            done = pg_num - ALL_DICT_PAGES[0] + 1
            print(f"    Vision 600DPI: {done}/{len(ALL_DICT_PAGES)} pages ({elapsed:.0f}s)")

    with open(PASS3_CACHE, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"  Vision 600DPI done: {len(results)} pages in {time.time()-t0:.0f}s")
    return results


def merge_ocr_passes(pass1, pass2, pass3):
    """Merge all OCR passes, preferring lines with more valid characters."""
    if MERGED_CACHE.exists():
        print(f"  Merged cache exists ({MERGED_CACHE.name}), loading...")
        return json.load(open(MERGED_CACHE))

    def line_quality(line):
        if not line.strip():
            return 0
        valid = sum(1 for c in line if c.isalpha() or c in "''ʼüïöëñáéíóú'")
        ratio = valid / max(len(line), 1)
        return ratio * len(line)

    merged = {}
    all_pages = set()
    for d in [pass1, pass2, pass3]:
        all_pages.update(d.keys())

    for pg in sorted(all_pages, key=int):
        texts = []
        for d, name in [(pass1, "vision300"), (pass2, "easyocr"), (pass3, "vision600")]:
            if pg in d and d[pg].strip():
                texts.append((name, d[pg]))

        if not texts:
            merged[pg] = ""
            continue

        if len(texts) == 1:
            merged[pg] = texts[0][1]
            continue

        best_lines = []
        all_line_sets = []
        for name, text in texts:
            lines = text.split("\n")
            all_line_sets.append((name, lines))

        max_lines = max(len(ls) for _, ls in all_line_sets)
        for i in range(max_lines):
            candidates = []
            for name, lines in all_line_sets:
                if i < len(lines):
                    candidates.append(lines[i])
            if candidates:
                best = max(candidates, key=line_quality)
                best_lines.append(best)

        merged[pg] = "\n".join(best_lines)

    with open(MERGED_CACHE, "w") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"  Merged {len(merged)} pages")
    return merged


if __name__ == "__main__":
    print("=" * 60)
    print("Multi-pass OCR of Tol-Spanish Dictionary")
    print("=" * 60)

    # Pass 1: existing macOS Vision 300 DPI
    print("\nPass 1: macOS Vision 300 DPI (cached)")
    pass1 = json.load(open(PASS1_CACHE)) if PASS1_CACHE.exists() else {}
    print(f"  {len(pass1)} pages loaded")

    # Pass 2: EasyOCR
    print("\nPass 2: EasyOCR")
    pass2 = run_easyocr_pass()

    # Pass 3: macOS Vision 600 DPI
    print("\nPass 3: macOS Vision 600 DPI")
    pass3 = run_vision_600dpi_pass()

    # Merge
    print("\nMerging all passes...")
    merged = merge_ocr_passes(pass1, pass2, pass3)

    print(f"\nDone. Merged text for {len(merged)} pages saved to {MERGED_CACHE}")
