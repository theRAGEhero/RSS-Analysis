from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple


# Ensure project src directory is importable when the script is executed directly.

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from rss_transcriber import (
    Base,
    Episode,
    FeedSubscription,
    Podcast,
    Transcript,
    ensure_transcript_metadata_columns,
    setup_logging,
)


DEFAULT_PODCAST_NAME = "Metagovernance Seminar Archive"
DEFAULT_RSS_URL = "metagov://archive"
DEFAULT_CATEGORY = "Governance"
DEFAULT_LANGUAGE = "en"
TRANSCRIPT_EXT = ".transcription.txt"

DATE_PATTERN = re.compile(r"(\d{4})(\d{2})(\d{2})")
SPEAKER_PATTERN = re.compile(r"^(?:Speaker\s*\d+|[A-Z][\w .,'-]{1,40}):\s*(.+)$")

ProgressCallback = Callable[[Dict[str, object]], None]


logger = logging.getLogger("metagov.importer")


@dataclass
class EpisodePayload:
    title: str
    slug: str
    guid: str
    audio_url: str
    published_at: Optional[dt.datetime]
    description: Optional[str]
    transcript_text: str
    diarization: List[Dict[str, Optional[str]]]


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def infer_title(stem: str) -> str:
    tokens = stem.replace("_", "-").split("-")
    cleaned = []
    for token in tokens:
        if not token:
            continue
        if token.isdigit():
            cleaned.append(token)
        else:
            cleaned.append(token.capitalize())
    return " ".join(cleaned) or stem


def infer_date(stem: str) -> Optional[dt.datetime]:
    match = DATE_PATTERN.search(stem)
    if not match:
        return None
    try:
        year, month, day = map(int, match.groups())
        return dt.datetime(year, month, day)
    except ValueError:
        return None


def summarise_description(text: str, max_len: int = 400) -> Optional[str]:
    stripped = text.strip()
    if not stripped:
        return None
    first_paragraph = stripped.split("\n\n", 1)[0]
    if len(first_paragraph) <= max_len:
        return first_paragraph
    return first_paragraph[: max_len - 3].rsplit(" ", 1)[0] + "..."


def parse_transcript_segments(text: str) -> List[Dict[str, Optional[str]]]:
    segments: List[Dict[str, Optional[str]]] = []
    current: Optional[Dict[str, Optional[str]]] = None
    inferred_time = 0.0
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = SPEAKER_PATTERN.match(line)
        if match:
            speaker_label = line.split(":", 1)[0].strip()
            content = match.group(1).strip()
            current = {
                "speaker": speaker_label,
                "start": inferred_time,
                "end": inferred_time,
                "transcript": content,
            }
            segments.append(current)
            inferred_time += 15.0
        elif current:
            current["transcript"] = f"{current['transcript']} {line}".strip()
    return segments


def build_raw_response(payload: EpisodePayload) -> Dict[str, object]:
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    return {
        "metadata": {
            "transaction_key": None,
            "request_id": payload.guid,
            "sha256": None,
            "created": now_iso,
            "duration": None,
            "channels": 1 if payload.diarization else 0,
            "models": ["metagov-manual"],
            "model_info": {
                "metagov-manual": {
                    "name": "metagov-manual",
                    "version": "2025-10-01",
                    "arch": "manual",
                }
            },
            "warnings": None,
            "summary_info": None,
        },
        "results": {
            "channels": [],
            "utterances": payload.diarization,
            "summary": None,
        },
    }


def collect_payloads(
    source_dir: Path,
    file_paths: Optional[Iterable[Path]] = None,
) -> Iterable[Tuple[Path, EpisodePayload]]:
    candidates = file_paths if file_paths is not None else sorted(
        source_dir.glob(f"*{TRANSCRIPT_EXT}")
    )
    for file_path in candidates:
        name = file_path.name
        if not name.endswith(TRANSCRIPT_EXT):
            continue
        stem = name[: -len(TRANSCRIPT_EXT)]
        slug = slugify(stem)
        if not slug:
            continue
        guid = f"metagov:{slug}"
        audio_url = f"metagov://{slug}"
        title = infer_title(stem)

        text = file_path.read_text(encoding="utf-8", errors="ignore")
        diarization = parse_transcript_segments(text)

        published_at = infer_date(stem)
        if not published_at:
            published_at = dt.datetime.fromtimestamp(file_path.stat().st_mtime)

        payload = EpisodePayload(
            title=title,
            slug=slug,
            guid=guid,
            audio_url=audio_url,
            published_at=published_at,
            description=summarise_description(text),
            transcript_text=text,
            diarization=diarization,
        )
        logger.debug("Prepared payload for %s", guid)
        yield file_path, payload


def ensure_podcast(session: Session, name: str, rss_url: str, category: Optional[str], language: Optional[str]) -> Podcast:
    podcast = (
        session.execute(select(Podcast).where(Podcast.rss_url == rss_url))
        .scalar_one_or_none()
    )
    if podcast:
        return podcast

    podcast = Podcast(
        name=name,
        rss_url=rss_url,
        category=category,
        language=language,
        description="Imported seminars from the Metagovernance archive (archive.org).",
    )
    session.add(podcast)
    session.commit()
    session.refresh(podcast)
    return podcast


def ensure_feed_subscription(session: Session, name: str, rss_url: str, category: Optional[str], language: Optional[str]) -> None:
    existing = (
        session.execute(select(FeedSubscription).where(FeedSubscription.rss_url == rss_url))
        .scalar_one_or_none()
    )
    if existing:
        return
    session.add(
        FeedSubscription(
            name=name,
            rss_url=rss_url,
            category=category,
            language=language,
        )
    )
    session.commit()


def import_payload(session: Session, podcast: Podcast, payload: EpisodePayload, overwrite: bool = False) -> Tuple[bool, bool]:
    episode = (
        session.execute(select(Episode).where(Episode.guid == payload.guid))
        .scalar_one_or_none()
    )

    created = False
    updated = False

    published_at = payload.published_at or dt.datetime.now()
    if episode is None:
        episode = Episode(
            podcast_id=podcast.id,
            guid=payload.guid,
            title=payload.title,
            description=payload.description,
            published_at=published_at,
            audio_url=payload.audio_url,
            image_url=None,
            duration=None,
        )
        session.add(episode)
        session.flush()
        created = True
    elif not overwrite and episode.transcript:
        return False, False
    else:
        episode.title = payload.title
        episode.description = payload.description or episode.description
        episode.published_at = published_at or episode.published_at
        episode.audio_url = episode.audio_url or payload.audio_url
        updated = True

    metadata_created = dt.datetime.now(dt.timezone.utc)
    raw_response = build_raw_response(payload)

    transcript = episode.transcript or Transcript(episode=episode, raw_response={})
    transcript.transcript_text = payload.transcript_text
    transcript.confidence = None
    transcript.seconds = None
    transcript.diarization = payload.diarization or None
    transcript.raw_response = raw_response
    transcript.transaction_key = raw_response["metadata"]["transaction_key"]
    transcript.request_id = raw_response["metadata"]["request_id"]
    transcript.sha256 = raw_response["metadata"]["sha256"]
    transcript.metadata_created = metadata_created
    transcript.channel_count = raw_response["metadata"]["channels"]
    transcript.models = raw_response["metadata"]["models"]
    transcript.model_primary_name = "metagov-manual"
    transcript.model_primary_version = "2025-10-01"
    transcript.model_primary_arch = "manual"
    transcript.model_info = raw_response["metadata"]["model_info"]
    transcript.summary_info = raw_response["metadata"]["summary_info"]
    transcript.channels = raw_response["results"]["channels"]
    transcript.summary = raw_response["results"]["summary"]

    episode.transcript = transcript
    episode.transcribed_at = metadata_created
    episode.deepgram_request_id = transcript.request_id

    if transcript.seconds:
        episode.duration = int(transcript.seconds)
    elif episode.duration is None and transcript.diarization:
        approx_total = 0.0
        for segment in transcript.diarization:
            start = segment.get("start") or 0
            end = segment.get("end") or start
            approx_total += max(float(end) - float(start), 0)
        if approx_total > 0:
            episode.duration = int(approx_total)

    session.add(transcript)
    session.commit()
    return created, updated


def run_import(
    db_path: Path,
    source_dir: Path,
    podcast_name: str,
    rss_url: str,
    category: Optional[str],
    language: Optional[str],
    register_feed: bool,
    overwrite: bool,
    progress_callback: Optional[ProgressCallback] = None,
) -> Tuple[int, int, int]:
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    ensure_transcript_metadata_columns(engine)
    SessionLocal = sessionmaker(bind=engine)

    created = 0
    updated = 0
    skipped = 0

    logger.info(
        "Starting metagov import | source=%s | db=%s | overwrite=%s",
        source_dir,
        db_path,
        overwrite,
    )

    file_paths = sorted(source_dir.glob(f"*{TRANSCRIPT_EXT}"))
    total_files = len(file_paths)
    if progress_callback:
        progress_callback(
            {
                "stage": "initialising",
                "total": total_files,
                "processed": 0,
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "current_file": None,
                "message": f"Found {total_files} transcript file(s) in {source_dir}.",
            }
        )

    with SessionLocal() as session:
        podcast = ensure_podcast(session, podcast_name, rss_url, category, language)
        if register_feed:
            ensure_feed_subscription(session, podcast_name, rss_url, category, language)

        for index, (file_path, payload) in enumerate(
            collect_payloads(source_dir, file_paths), start=1
        ):
            if progress_callback:
                progress_callback(
                    {
                        "stage": "processing",
                        "total": total_files,
                        "processed": index - 1,
                        "created": created,
                        "updated": updated,
                        "skipped": skipped,
                        "current_file": file_path.name,
                        "message": f"Processing {file_path.name}",
                    }
                )
            was_created, was_updated = import_payload(session, podcast, payload, overwrite=overwrite)
            if was_created:
                created += 1
                event_stage = "created"
                event_message = f"Created episode for {file_path.name}"
            elif was_updated:
                updated += 1
                event_stage = "updated"
                event_message = f"Updated episode for {file_path.name}"
            else:
                skipped += 1
                event_stage = "skipped"
                event_message = f"Skipped existing transcript for {file_path.name}"
            if index % 50 == 0:
                logger.info(
                    "Progress: processed %d files (created=%d, updated=%d, skipped=%d)",
                    index,
                    created,
                    updated,
                    skipped,
                )
            if progress_callback:
                progress_callback(
                    {
                        "stage": event_stage,
                        "total": total_files,
                        "processed": index,
                        "created": created,
                        "updated": updated,
                        "skipped": skipped,
                        "current_file": file_path.name,
                        "message": event_message,
                    }
                )

    engine.dispose()
    logger.info(
        "Metagov import finished | created=%d updated=%d skipped=%d",
        created,
        updated,
        skipped,
    )
    if progress_callback:
        progress_callback(
            {
                "stage": "finished",
                "total": total_files,
                "processed": total_files,
                "created": created,
                "updated": updated,
                "skipped": skipped,
                "current_file": None,
                "message": (
                    f"Import finished: created={created}, updated={updated}, skipped={skipped}."
                ),
            }
        )
    return created, updated, skipped


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Metagov transcript text files into the RSS Analysis database.")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("metagov"),
        help="Directory containing *.transcription.txt files (default: ./metagov).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("data/podcasts.sqlite"),
        help="Path to the SQLite database (default: data/podcasts.sqlite).",
    )
    parser.add_argument(
        "--podcast-name",
        type=str,
        default=DEFAULT_PODCAST_NAME,
        help=f"Podcast name to use/create (default: {DEFAULT_PODCAST_NAME}).",
    )
    parser.add_argument(
        "--rss-url",
        type=str,
        default=DEFAULT_RSS_URL,
        help=f"Stable RSS URL identifier to store (default: {DEFAULT_RSS_URL}).",
    )
    parser.add_argument(
        "--category",
        type=str,
        default=DEFAULT_CATEGORY,
        help=f"Podcast category (default: {DEFAULT_CATEGORY}).",
    )
    parser.add_argument(
        "--language",
        type=str,
        default=DEFAULT_LANGUAGE,
        help=f"Language code (default: {DEFAULT_LANGUAGE}).",
    )
    parser.add_argument(
        "--register-feed",
        action="store_true",
        help="Register the pseudo-RSS source in feed_subscriptions so future runs include it.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-import files even when a transcript already exists for the generated GUID.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging verbosity (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    source_dir = args.source_dir.expanduser().resolve()
    db_path = args.db.expanduser().resolve()

    run_label = f"metagov-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    setup_logging(PROJECT_ROOT / "logs", args.log_level, run_label=run_label)

    logger.info(
        "Metagov import run %s initialised | source=%s | db=%s",
        run_label,
        source_dir,
        db_path,
    )

    if not source_dir.exists() or not source_dir.is_dir():
        raise SystemExit(f"Source directory {source_dir} does not exist or is not a directory.")

    created, updated, skipped = run_import(
        db_path=db_path,
        source_dir=source_dir,
        podcast_name=args.podcast_name,
        rss_url=args.rss_url,
        category=args.category,
        language=args.language,
        register_feed=args.register_feed,
        overwrite=args.overwrite,
    )

    logger.info(
        "Metagov import run %s complete | created=%d updated=%d skipped=%d",
        run_label,
        created,
        updated,
        skipped,
    )
    print(
        json.dumps(
            {"created": created, "updated": updated, "skipped": skipped, "source": str(source_dir), "database": str(db_path)},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
