# Democracy Podcast Analysis Pipeline

This project ingests a curated list of democracy-focused podcasts, downloads every episode, transcribes the audio with Deepgram (including speaker diarization), stores rich metadata in SQLite, and produces an HTML analysis dashboard plus an optional Flask web app for exploration.

## Project Layout

- `data/podcasts.csv` – seed list of podcast RSS feeds (`Democracy Innovators`, `Democracy Works`, `How to Fix Democracy`). Update or extend this file to track additional shows.
- `src/rss_transcriber.py` – end-to-end pipeline: fetch RSS feeds, download audio, transcribe via Deepgram, persist metadata/transcripts/keywords, and generate a static report in `public/index.html`.
- `src/webapp.py` – Flask app for browsing podcasts, episodes, transcripts, and diarization metadata stored in the database.
- `templates/` & `static/` – HTML templates and CSS used by the Flask app.
- `requirements.txt` – Python dependencies.

## Prerequisites

1. Python 3.10+ recommended.
2. A Deepgram API key. Set it via environment variable (`DEEPGRAM_API_KEY`) or pass `--deepgram-api-key` to the pipeline.
3. Adequate disk space for temporary audio downloads (audio files are deleted after transcription).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Option 1: export API key manually
export DEEPGRAM_API_KEY="dg_your_key"
# Option 2: copy the example env file and edit secrets in place
cp .env.example .env && edit .env
```

- `.env.example` ships with placeholder values and is safe to commit. Your filled `.env`/`.env.local` files are ignored by git via the provided `.gitignore`.

## Run the Pipeline

```bash
# defaults: data/podcasts.csv, data/podcasts.sqlite, public/, logs/
python src/rss_transcriber.py
```

- Each invocation writes detailed logs under `logs/runs/`. The newest run is always available via `logs/latest.log`.

- By default the script ingests every episode (`max_episodes=0`), writes to `data/podcasts.sqlite`, and renders `public/index.html`.
- Override anything by passing flags (e.g. `--max-episodes 5` or `--log-level DEBUG`); otherwise the defaults shown above are used automatically.
- Logs stream to stdout and rotate inside `logs/pipeline.log`. Adjust verbosity via `PIPELINE_LOG_LEVEL` or `--log-level`.
- Environment variables are loaded from `.env` (or a custom path via `--env-file`). Keep API keys out of version control when sharing the project.
- The generated HTML report now includes an interactive force-directed graph that visualises cross-podcast episode connections based on shared TF–IDF keywords.
- Every pipeline execution drops a timestamped file under `logs/runs/` and points `logs/latest.log` at the newest run. The same log layout is reused by the database import/export utility described below.
- Feed language metadata (e.g. `de-DE`) is normalised to Deepgram’s accepted codes automatically, so keeping the language column up to date improves transcription accuracy.
- The web admin now exposes a cancel button for active ingest jobs; cancelling waits for the current episode download/transcription to finish before the pipeline shuts down cleanly.
- Need to re-run a single source after changing its metadata? Use the new **Reprocess** action next to the feed row—this launches the pipeline for that feed only, clears existing transcripts, and rebuilds them with the latest language settings.
- Anyone spotting gaps can now submit RSS suggestions from the overview page; admins review them under “Proposed Podcasts” before promoting to the tracked list.

Keyword analytics are re-generated after each ingest using TF–IDF with an expanded stopword set tuned for conversational audio. Episode detail pages now ship with an expandable transcript viewer, a diarization table, and the Flask UI exposes the graph plus a JSON feed at `/api/graph` for downstream analysis.

### Importing Metagov Seminar Transcripts

The `scripts/import_metagov.py` helper ingests the text transcripts from `metagov/` (downloaded from archive.org) and stores them as if they were sourced from an RSS feed:

```bash
source .venv/bin/activate
python scripts/import_metagov.py --register-feed
```

Use `--overwrite` to refresh previously imported items. The script can target a different directory or database via `--source-dir` / `--db`.

## Database Import/Export

Use `src/import_export.py` to snapshot or restore the SQLite database:

```bash
# 1. Activate your environment
source .venv/bin/activate

# Create a SQL dump (written to backup.sql) and record the run in logs/runs/
python src/import_export.py export --output backup.sql

# Restore from a dump, backing up the existing DB first
python src/import_export.py import --input backup.sql --force
```

- The script defaults to `data/podcasts.sqlite`; override with `--db`.
- Logs are written to the shared `logs/` directory (rotating `pipeline.log`, per-run files in `logs/runs/`, and a `latest.log` symlink). Use `--log-level DEBUG` or `-v` for verbose output, and `--log-dir` to redirect the log location.
- When importing, a timestamped `.bak` copy of the original database is created automatically unless `--backup` specifies a path or the file does not exist.
- Typical workflow:
  1. `python src/import_export.py export --output backups/podcasts-$(date +%Y%m%d).sql`
  2. Verify the dump (`head backups/podcasts-*.sql`) and commit/move it to secure storage.
  3. To restore: `python src/import_export.py import --input backups/podcasts-YYYYMMDD.sql --force`
  4. Review the generated backup noted in the log output (e.g. `data/podcasts.sqlite.20250101T120000.bak`) before deleting old copies.
- All operations share the pipeline log format, so `tail logs/latest.log` shows both pipeline and import/export activity.

## Explore with the Web App

```bash
export FLASK_APP=src.webapp:create_app
flask run --port 8000
```

Open `http://localhost:8000` to browse podcasts, inspect transcripts, and review diarization metadata per episode. The navigation provides:

- `/` – Overview dashboard with stats, latest transcribed episodes, and quick links.
- `/podcasts` – Full catalog of tracked podcasts with episode counts and recent releases.
- `/transcripts` – Complete transcript library with confidence scores and keyword highlights.
- `/episodes/<id>` – Episode detail page with transcript text and diarization payload.

## Extending the System

- **More podcasts**: Append rows to `data/podcasts.csv`.
- **Alternative storage**: Swap the SQLite connection string (`sqlite:///...`) in `rss_transcriber.py` for PostgreSQL/MySQL if needed; SQLAlchemy models are portable.
- **Analytics**: Use the stored transcripts and keywords to build downstream NLP features (topic modeling, clustering, etc.). The generated HTML report already highlights cross-podcast keyword overlaps.

## Operational Notes

- Deepgram billing applies per minute of transcribed audio. Consider limiting `--max-episodes` on the first run.
- Some feeds may lack downloadable audio URLs or enforce geo/IP restrictions; such episodes are logged and skipped.
- The pipeline logs to stdout. Adjust logging level near the top of `src/rss_transcriber.py` if required.
