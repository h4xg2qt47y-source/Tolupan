#!/usr/bin/env python3
"""Download WEB English NT audio chapter files from ebible.org (public domain)."""

import os
import urllib.request
from pathlib import Path

BASE_URL = "https://ebible.org/eng-web/mp3/"
OUT_DIR = Path(__file__).resolve().parent.parent / "English_Audio"

BOOK_MAP = {
    "MAT": ("01", "Matthew"),
    "MRK": ("02", "Mark"),
    "LUK": ("03", "Luke"),
    "JHN": ("04", "John"),
    "ACT": ("05", "Acts"),
    "ROM": ("06", "Romans"),
    "1CO": ("07", "1Corinthians"),
    "2CO": ("08", "2Corinthians"),
    "GAL": ("09", "Galatians"),
    "EPH": ("10", "Ephesians"),
    "PHP": ("11", "Philippians"),
    "COL": ("12", "Colossians"),
    "1TH": ("13", "1Thess"),
    "2TH": ("14", "2Thess"),
    "1TI": ("15", "1Timothy"),
    "2TI": ("16", "2Timothy"),
    "TIT": ("17", "Titus"),
    "PHM": ("18", "Philemon"),
    "HEB": ("19", "Hebrews"),
    "JAS": ("20", "James"),
    "1PE": ("21", "1Peter"),
    "2PE": ("22", "2Peter"),
    "1JN": ("23", "1John"),
    "2JN": ("24", "2John"),
    "3JN": ("25", "3John"),
    "JUD": ("26", "Jude"),
    "REV": ("27", "Revelation"),
}

CHAPTER_COUNTS = {
    "MAT": 28, "MRK": 16, "LUK": 24, "JHN": 21, "ACT": 28,
    "ROM": 16, "1CO": 16, "2CO": 13, "GAL": 6, "EPH": 6,
    "PHP": 4, "COL": 4, "1TH": 5, "2TH": 3, "1TI": 6, "2TI": 4,
    "TIT": 3, "PHM": 1, "HEB": 13, "JAS": 5, "1PE": 5, "2PE": 3,
    "1JN": 5, "2JN": 1, "3JN": 1, "JUD": 1, "REV": 22,
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    total = sum(CHAPTER_COUNTS.values())
    done = 0

    for book_code, (num, name) in BOOK_MAP.items():
        chapters = CHAPTER_COUNTS[book_code]
        for ch in range(1, chapters + 1):
            remote_name = f"{num}_{ch:02d}_{name}.mp3"
            local_name = f"{book_code}_{ch:02d}.mp3"
            local_path = OUT_DIR / local_name
            url = BASE_URL + remote_name

            if local_path.exists():
                done += 1
                continue

            try:
                print(f"  [{done+1}/{total}] Downloading {remote_name} -> {local_name}")
                urllib.request.urlretrieve(url, str(local_path))
                done += 1
            except Exception as e:
                print(f"  FAILED: {url} -> {e}")
                done += 1

    print(f"\nDone! {len(list(OUT_DIR.glob('*.mp3')))} files in {OUT_DIR}")


if __name__ == "__main__":
    main()
