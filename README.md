# Tolupan (Tol/Jicaque) Language Preservation Project

A web application for preserving and teaching the **Tol language** (ISO 639-3: `jic`), spoken by roughly 200–600 remaining speakers in the Montaña de la Flor region of Honduras. The project provides translation tools, a Bible reader, and gamified language learning — all powered by parallel New Testament texts, linguistic reference PDFs, and a custom text-to-speech engine.

## Features

- **Translator** — Tol ↔ Spanish ↔ English translation engine backed by parallel NT texts, a parsed Tol–Spanish dictionary, verb conjugation tables, synonym expansion, and phonetic scoring.
- **Dictionary** — Searchable dictionary with part-of-speech tags, example sentences, and verb conjugations.
- **Dios Vele (Bible)** — Side-by-side New Testament reader (Tol, Spanish, English) with chapter-level audio playback for Tol and English.
- **Aprende (Learn)** — Gamified vocabulary learning tab with quizzes, matching games, and flashcards. Supports Spanish ↔ Tol and English ↔ Tol modes. Uses only verified translations.
- **Sources** — Catalog of all reference PDFs, websites, and datasets used in the project.
- **Feedback** — Report wrong translations, dictionary fixes, bugs, and ideas; generates a Cursor-friendly text block and emails maintainers when SMTP is configured.
- **TTS Engine** — Custom Coqui VITS text-to-speech model trained on ~20 hours of Tol New Testament audio, with dynamic-programming verse alignment.

## Project Structure

```
app/                    Web application
  server.py             FastAPI backend
  translator.py         Translation engine
  tts_engine.py         TTS inference wrapper
  static/               Frontend (HTML, CSS, JS)
  data/
    tol.db              SQLite database (parallel sentences, dictionary, verbs)
    learn_vocab.json    Curated vocabulary for the Learn tab

scripts/                Data processing & training scripts
  build_database.py     Build the SQLite database from source texts
  build_learn_vocab.py  Generate verified vocabulary for the Learn tab
  build_tts_dataset_v2.py  Align audio to text for TTS training
  train_tts.py          Train the Coqui VITS TTS model
  ...

Tol Pronunciation/     Linguistic reference PDFs and extracted text
Tol Translation/       NT source texts (Tol, Spanish, English) and dictionaries
```

## Excluded from Git (large files — rebuild locally)

| Directory | Size | Contents |
|-----------|------|----------|
| `Tol Audio/` | ~6 GB | ScriptureEarth NT audio, ELAN transcripts |
| `Tol_Chapter_Audio/` | ~5 GB | Cleaned chapter-level Tol MP3s |
| `English_Audio/` | ~500 MB | English WEB NT chapter MP3s |
| `TTS_Dataset_v2/` | ~11 GB | Aligned audio segments for TTS training |
| `TTS_Model/` | ~3 GB | Trained VITS model checkpoints |
| `ffmpeg` | 76 MB | Static binary (download separately) |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the web app
cd app
uvicorn server:app --reload --port 8000
```

Then open http://localhost:8000 in your browser.

## User feedback (email)

The **Feedback** page (`/feedback`) POSTs to `/api/feedback`. Every submission is appended to `app/data/feedback_inbox.jsonl` on the server.

To **also send email** (e.g. to `tooling_village_30@icloud.com`), set SMTP environment variables on the host (Railway, etc.):

| Variable | Example |
|----------|---------|
| `FEEDBACK_EMAIL_TO` | `tooling_village_30@icloud.com` (default if unset) |
| `FEEDBACK_SMTP_HOST` | `smtp.mail.me.com` (iCloud) or your provider |
| `FEEDBACK_SMTP_PORT` | `587` |
| `FEEDBACK_SMTP_USER` | full email for the sending account |
| `FEEDBACK_SMTP_PASSWORD` | app-specific password |
| `FEEDBACK_EMAIL_FROM` | optional; defaults to `FEEDBACK_SMTP_USER` |

Without SMTP, feedback is still stored locally and the UI offers **mailto** + copy-to-clipboard.

## Data Sources

See [RESOURCE_CATALOG.md](RESOURCE_CATALOG.md) for a full listing of all audio files, PDFs, parallel texts, and web sources used in this project.

## License

This project is for language preservation and educational purposes. Individual data sources retain their original copyrights — see the Sources tab in the web app for attribution details.
