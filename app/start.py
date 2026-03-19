"""
Startup script for Railway deployment.
Reassembles the compressed database from split parts, then launches uvicorn.
"""

import gzip
import os
import subprocess
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "tol.db"


def reassemble_db():
    if DB_PATH.exists() and DB_PATH.stat().st_size > 1000:
        print(f"[startup] Database already exists ({DB_PATH.stat().st_size / 1024 / 1024:.1f} MB), skipping rebuild.")
        return

    parts = sorted(DATA_DIR.glob("tol.db.gz.part-*"))
    if not parts:
        print("[startup] ERROR: No database parts found! App will fail.", file=sys.stderr)
        return

    print(f"[startup] Reassembling database from {len(parts)} compressed parts...")

    gz_path = DATA_DIR / "tol.db.gz"
    with open(gz_path, "wb") as out:
        for part in parts:
            print(f"  - {part.name} ({part.stat().st_size / 1024 / 1024:.1f} MB)")
            out.write(part.read_bytes())

    print("[startup] Decompressing...")
    with gzip.open(gz_path, "rb") as f_in, open(DB_PATH, "wb") as f_out:
        while chunk := f_in.read(8 * 1024 * 1024):
            f_out.write(chunk)

    gz_path.unlink()
    print(f"[startup] Database ready: {DB_PATH.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    reassemble_db()

    port = os.environ.get("PORT", "8080")
    print(f"[startup] Launching uvicorn on port {port}...")
    subprocess.run([
        sys.executable, "-m", "uvicorn",
        "server:app",
        "--host", "0.0.0.0",
        "--port", port,
    ])
