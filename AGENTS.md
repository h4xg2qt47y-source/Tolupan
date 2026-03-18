# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a **single-service Python/FastAPI** web application for preserving the Tol (Jicaque) language. The entire app is one FastAPI process backed by one SQLite database (~384 MB, tracked via Git LFS).

### Running the app

```bash
cd app && uvicorn server:app --reload --port 8000
```

The server serves both the API and the static frontend (vanilla HTML/CSS/JS — no build step). Open `http://localhost:8000` in a browser.

### Key caveats

- **Git LFS**: The SQLite database `app/data/tol.db` is tracked via Git LFS. After cloning, run `git lfs pull` if the file is just a pointer (the update script handles this). Without the real DB file, the server will crash on startup.
- **`uvicorn` binary location**: `pip install --user` places the `uvicorn` binary in `~/.local/bin`, which may not be on `PATH`. Either use `python3 -m uvicorn` or prepend `~/.local/bin` to `PATH`.
- **TTS model**: The TTS engine (`tts_engine.py`) requires a ~3 GB model checkpoint in `TTS_Model/` that is gitignored. The app degrades gracefully (returns "not available") when it's absent — this is expected.
- **Audio files**: Chapter audio files (`Tol_Chapter_Audio/`, `English_Audio/`) are gitignored (~5.5 GB). Bible reader works without them — audio playback buttons will simply be hidden.
- **No linter / test framework configured**: There is no `pyproject.toml`, no `pytest`, no `flake8`/`ruff` configuration. The only automated "tests" are grammar test sentences run via the `/api/test-sentences` endpoint.
- **No npm / frontend build**: The frontend is plain HTML/CSS/JS served as static files from `app/static/`. No build step is needed.

### Testing

- **API smoke test**: `curl http://localhost:8000/api/stats` should return JSON with data counts.
- **Translation test**: `POST /api/translate` with `{"text": "hello", "source_lang": "en", "target_lang": "tol"}`.
- **Grammar tests**: `GET /api/test-sentences` runs all grammar test sentences through the translator and returns pass/fail results.
- **Manual UI testing**: Open `http://localhost:8000` in Chrome and use the Translator, Dictionary, Dios Vele (Bible), and Aprende (Learn) tabs.
