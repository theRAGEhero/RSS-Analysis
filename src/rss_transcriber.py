from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
import mimetypes
import os
import sys
import tempfile
import threading
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set
from urllib.parse import urlparse

import json
import math
import re
from collections import Counter

import requests
import feedparser
try:
    from deepgram import DeepgramClient, DeepgramClientOptions, DeepgramError  # type: ignore[attr-defined]
except ImportError:
    from deepgram import DeepgramClient, DeepgramClientOptions  # type: ignore[no-redef]

    try:
        from deepgram._exceptions import DeepgramException as DeepgramError  # type: ignore
    except Exception:  # pragma: no cover - fallback when SDK structure changes
        class DeepgramError(Exception):  # type: ignore[no-redef]
            """Fallback Deepgram error type."""
            pass

try:
    from deepgram.clients.prerecorded.v1.options import PrerecordedOptions  # type: ignore
except Exception:  # pragma: no cover - fallback for older SDKs
    PrerecordedOptions = None  # type: ignore
from dotenv import load_dotenv
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    select,
    func,
    inspect,
    text,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, selectinload


logger = logging.getLogger(__name__)
RUN_ID: Optional[str] = None


class RunAwareFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        if not hasattr(record, "run"):
            record.run = RUN_ID or "-"
        return super().format(record)

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CSV_PATH = BASE_DIR / "data" / "podcasts.csv"
DEFAULT_DB_PATH = BASE_DIR / "data" / "podcasts.sqlite"
DEFAULT_OUTPUT_DIR = BASE_DIR / "public"
DEFAULT_LOG_DIR = BASE_DIR / "logs"
DEFAULT_LOG_LEVEL = os.environ.get("PIPELINE_LOG_LEVEL", "INFO")
DEFAULT_MAX_EPISODES = 0
DEFAULT_MODEL = "nova-2"

Base = declarative_base(metadata=MetaData())


class PipelineCancelled(Exception):
    """Raised when an ingest run is cancelled."""

DEEPGRAM_LANGUAGE_ALIASES: Dict[str, str] = {
    "en": "en",
    "en-us": "en-US",
    "en-gb": "en-GB",
    "es": "es",
    "es-es": "es",
    "es-mx": "es",
    "fr": "fr",
    "fr-fr": "fr",
    "de": "de",
    "de-de": "de",
    "it": "it",
    "it-it": "it",
    "pt": "pt",
    "pt-pt": "pt-PT",
    "pt-br": "pt-BR",
    "nl": "nl",
    "nl-nl": "nl",
    "pl": "pl",
    "pl-pl": "pl",
    "sv": "sv",
    "sv-se": "sv",
    "nb": "nb",
    "nb-no": "nb",
    "da": "da",
    "da-dk": "da",
    "fi": "fi",
    "fi-fi": "fi",
}


def parse_iso_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
        if isinstance(value, dt.datetime):
            return value
        value_str = str(value)
        if value_str.endswith("Z"):
            value_str = value_str[:-1] + "+00:00"
        return dt.datetime.fromisoformat(value_str)
    except Exception:
        return None


def extract_deepgram_metadata(response_data: Dict[str, Any]) -> Dict[str, Any]:
    metadata = response_data.get("metadata") or {}
    results = response_data.get("results") or {}

    models = metadata.get("models")
    if not isinstance(models, list):
        models = None

    model_info = metadata.get("model_info") or {}
    primary_name = primary_version = primary_arch = None
    if models:
        primary_id = models[0]
        primary_details = model_info.get(primary_id) if isinstance(model_info, dict) else None
        if isinstance(primary_details, dict):
            primary_name = primary_details.get("name")
            primary_version = primary_details.get("version")
            primary_arch = primary_details.get("arch")

    channel_count = metadata.get("channels")
    try:
        channel_count = int(channel_count) if channel_count is not None else None
    except (TypeError, ValueError):
        channel_count = None

    return {
        "transaction_key": metadata.get("transaction_key"),
        "request_id": metadata.get("request_id"),
        "sha256": metadata.get("sha256"),
        "metadata_created": parse_iso_datetime(metadata.get("created")),
        "channel_count": channel_count,
        "models": models,
        "model_primary_name": primary_name,
        "model_primary_version": primary_version,
        "model_primary_arch": primary_arch,
        "model_info": model_info or None,
        "summary_info": metadata.get("summary_info"),
        "channels": results.get("channels"),
        "summary": results.get("summary"),
    }


def ensure_transcript_metadata_columns(engine) -> None:
    """Backfill new transcript metadata columns on existing databases."""
    inspector = inspect(engine)
    try:
        columns = {column["name"] for column in inspector.get_columns("transcripts")}
    except Exception:
        return

    column_definitions = {
        "transaction_key": "TEXT",
        "request_id": "TEXT",
        "sha256": "TEXT",
        "metadata_created": "TEXT",
        "channel_count": "INTEGER",
        "models": "TEXT",
        "model_primary_name": "TEXT",
        "model_primary_version": "TEXT",
        "model_primary_arch": "TEXT",
        "model_info": "TEXT",
        "summary_info": "TEXT",
        "channels": "TEXT",
        "summary": "TEXT",
    }

    with engine.begin() as connection:
        for name, ddl_type in column_definitions.items():
            if name not in columns:
                connection.execute(text(f"ALTER TABLE transcripts ADD COLUMN {name} {ddl_type}"))
class FeedSubscription(Base):
    __tablename__ = "feed_subscriptions"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    rss_url = Column(String(500), nullable=False, unique=True)
    category = Column(String(128), nullable=True)
    language = Column(String(16), nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class Podcast(Base):
    __tablename__ = "podcasts"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    rss_url = Column(String(500), nullable=False, unique=True)
    category = Column(String(128), nullable=True)
    language = Column(String(16), nullable=True)
    description = Column(Text, nullable=True)
    website = Column(String(500), nullable=True)
    image_url = Column(String(500), nullable=True)
    last_build_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: dt.datetime.now(dt.timezone.utc), nullable=False)
    updated_at = Column(
        DateTime,
        default=lambda: dt.datetime.now(dt.timezone.utc),
        onupdate=lambda: dt.datetime.now(dt.timezone.utc),
        nullable=False,
    )

    episodes = relationship("Episode", back_populates="podcast", cascade="all, delete-orphan")


class Episode(Base):
    __tablename__ = "episodes"

    id = Column(Integer, primary_key=True)
    podcast_id = Column(Integer, ForeignKey("podcasts.id"), nullable=False)
    guid = Column(String(512), nullable=False)
    title = Column(String(512), nullable=False)
    description = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=True)
    audio_url = Column(String(1000), nullable=False)
    image_url = Column(String(500), nullable=True)
    duration = Column(Integer, nullable=True)  # seconds
    deepgram_request_id = Column(String(128), nullable=True)
    transcribed_at = Column(DateTime, nullable=True)

    podcast = relationship("Podcast", back_populates="episodes")
    transcript = relationship("Transcript", back_populates="episode", uselist=False, cascade="all, delete-orphan")
    keywords = relationship(
        "EpisodeKeyword",
        back_populates="episode",
        cascade="all, delete-orphan",
        order_by="EpisodeKeyword.weight.desc()",
    )

    __table_args__ = (UniqueConstraint("podcast_id", "guid", name="uq_episode_guid"),)


class Transcript(Base):
    __tablename__ = "transcripts"

    id = Column(Integer, primary_key=True)
    episode_id = Column(Integer, ForeignKey("episodes.id"), nullable=False)
    transcript_text = Column(Text, nullable=False)
    confidence = Column(Float, nullable=True)
    seconds = Column(Float, nullable=True)
    diarization = Column(JSON, nullable=True)
    raw_response = Column(JSON, nullable=False)
    transaction_key = Column(String(128), nullable=True)
    request_id = Column(String(128), nullable=True, index=True)
    sha256 = Column(String(128), nullable=True)
    metadata_created = Column(DateTime, nullable=True)
    channel_count = Column(Integer, nullable=True)
    models = Column(JSON, nullable=True)
    model_primary_name = Column(String(128), nullable=True)
    model_primary_version = Column(String(64), nullable=True)
    model_primary_arch = Column(String(64), nullable=True)
    model_info = Column(JSON, nullable=True)
    summary_info = Column(JSON, nullable=True)
    channels = Column(JSON, nullable=True)
    summary = Column(JSON, nullable=True)

    episode = relationship("Episode", back_populates="transcript")


class EpisodeKeyword(Base):
    __tablename__ = "episode_keywords"

    id = Column(Integer, primary_key=True)
    episode_id = Column(Integer, ForeignKey("episodes.id"), nullable=False)
    keyword = Column(String(64), nullable=False)
    weight = Column(Float, nullable=False)

    episode = relationship("Episode", back_populates="keywords")

    __table_args__ = (UniqueConstraint("episode_id", "keyword", name="uq_episode_keyword"),)


@dataclass(frozen=True)
class PodcastFeed:
    name: str
    rss_url: str
    category: Optional[str] = None
    language: Optional[str] = None
    id: Optional[int] = None


BASE_STOPWORDS = {
    "the",
    "and",
    "a",
    "to",
    "of",
    "in",
    "for",
    "on",
    "is",
    "that",
    "with",
    "as",
    "at",
    "be",
    "this",
    "by",
    "it",
    "are",
    "from",
    "an",
    "was",
    "or",
    "we",
    "you",
    "they",
    "he",
    "she",
    "i",
    "but",
    "have",
    "has",
    "had",
    "not",
    "their",
    "our",
    "your",
    "will",
    "can",
    "its",
    "about",
}

ENGLISH_STOP_WORDS = {
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "aren",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "could",
    "couldn",
    "did",
    "didn",
    "do",
    "does",
    "doesn",
    "doing",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "hadn",
    "has",
    "hasn",
    "have",
    "haven",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "isn",
    "it",
    "itself",
    "just",
    "ll",
    "m",
    "ma",
    "me",
    "mightn",
    "more",
    "most",
    "mustn",
    "my",
    "myself",
    "needn",
    "no",
    "nor",
    "not",
    "now",
    "o",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "re",
    "same",
    "shan",
    "she",
    "should",
    "shouldn",
    "so",
    "some",
    "such",
    "t",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "ve",
    "very",
    "was",
    "wasn",
    "we",
    "were",
    "weren",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "won",
    "wouldn",
    "y",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
}

FILLER_STOPWORDS = {
    "uh",
    "uhh",
    "uhm",
    "umm",
    "erm",
    "er",
    "hmm",
    "mm",
    "mhm",
    "mmhm",
    "ah",
    "oh",
    "yeah",
    "yep",
    "nope",
    "alright",
    "okay",
    "ok",
    "kinda",
    "sorta",
    "yknow",
    "ya",
    "gonna",
    "wanna",
    "like",
    "right",
    "just",
    "it's",
    "thats",
    "that's",
    "were",
    "we're",
    "they're",
    "you're",
    "i'm",
    "don't",
    "won't",
    "can't",
    "didn't",
    "doesn't",
    "isn't",
    "aren't",
    "shouldn't",
    "wouldn't",
    "couldn't",
    "hasn't",
    "hadn't",
    "haven't",
    "speaker",
}

EXTRA_STOPWORDS = {
    "episode",
    "podcast",
    "people",
    "really",
    "actually",
    "would",
    "could",
    "should",
    "think",
    "thank",
    "thanks",
    "also",
    "kind",
    "sort",
    "going",
    "want",
    "today",
    "know",
    "guest",
    "question",
    "questions",
    "conversation",
    "interview",
    "minute",
    "seconds",
    "welcome",
    "hello",
    "hi",
    "great",
    "awesome",
    "feel",
    "little",
    "bit",
    "maybe",
    "someone",
    "something",
    "thing",
    "things",
}

KEYWORD_STOPWORDS = {
    word.lower()
    for word in ENGLISH_STOP_WORDS.union(BASE_STOPWORDS, EXTRA_STOPWORDS, FILLER_STOPWORDS)
}


def normalise_language_code(*candidates: Optional[str]) -> Optional[str]:
    """Normalise RSS language hints to Deepgram-compatible identifiers."""
    for candidate in candidates:
        if not candidate:
            continue
        normalised = str(candidate).strip()
        if not normalised:
            continue
        normalised = normalised.replace("_", "-")
        lower_key = normalised.lower()
        if lower_key in DEEPGRAM_LANGUAGE_ALIASES:
            return DEEPGRAM_LANGUAGE_ALIASES[lower_key]
        if "-" in lower_key:
            base = lower_key.split("-", 1)[0]
            if base in DEEPGRAM_LANGUAGE_ALIASES:
                return DEEPGRAM_LANGUAGE_ALIASES[base]
            return base
        return lower_key
    return None


def setup_logging(log_dir: Path, level: str, run_label: Optional[str] = None) -> str:
    global RUN_ID

    log_dir.mkdir(parents=True, exist_ok=True)
    runs_dir = log_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    if run_label is None:
        run_label = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    RUN_ID = run_label

    log_level = getattr(logging, level.upper(), logging.INFO)

    run_log_path = runs_dir / f"{run_label}.log"
    rotating_log_path = log_dir / "pipeline.log"
    latest_link = log_dir / "latest.log"

    formatter = RunAwareFormatter(
        "%(asctime)s %(levelname)s [%(run)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(log_level)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    run_file_handler = logging.FileHandler(run_log_path, encoding="utf-8")
    run_file_handler.setLevel(log_level)
    run_file_handler.setFormatter(formatter)
    root.addHandler(run_file_handler)

    rotating_handler = RotatingFileHandler(
        rotating_log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    rotating_handler.setLevel(log_level)
    rotating_handler.setFormatter(formatter)
    root.addHandler(rotating_handler)

    try:
        if latest_link.exists() or latest_link.is_symlink():
            latest_link.unlink()
        latest_link.symlink_to(run_log_path)
    except OSError:
        root.debug("Unable to update latest log symlink", exc_info=True)

    root.debug("Logging configured. Run log: %s", run_log_path)
    return run_label


class RssTranscriptionPipeline:
    def __init__(
        self,
        feeds: Sequence[PodcastFeed],
        database_url: str,
        deepgram_api_key: str,
        output_dir: Path,
        model: str = "nova-2",
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        stop_event: Optional[threading.Event] = None,
        force_reprocess: bool = False,
    ) -> None:
        self.feeds = feeds
        self.database_url = database_url
        self.deepgram_api_key = deepgram_api_key
        self.output_dir = output_dir
        self.model = model
        self.keywords_dirty = True
        self.progress_callback = progress_callback
        self._stop_event = stop_event
        self.force_reprocess = force_reprocess

        self.output_dir.mkdir(parents=True, exist_ok=True)

        engine = create_engine(self.database_url)
        Base.metadata.create_all(engine)
        ensure_transcript_metadata_columns(engine)
        self.SessionLocal = sessionmaker(bind=engine)

        options = DeepgramClientOptions(options={"keepalive": True})
        self.deepgram = DeepgramClient(self.deepgram_api_key, options)
        self._progress_state: Dict[str, Any] = {}

    def _emit_progress(self, **payload: Any) -> None:
        if not self.progress_callback:
            return
        try:
            self.progress_callback(payload)
        except Exception:  # pragma: no cover - defensive callback handling
            logger.debug("Progress callback failed", exc_info=True)

    def _should_stop(self) -> bool:
        return bool(self._stop_event and self._stop_event.is_set())

    def _check_cancel(self) -> None:
        if self._should_stop():
            logger.info("Cancellation requested. Halting pipeline…")
            raise PipelineCancelled("Pipeline run cancelled by user request.")

    def run(self, max_episodes: int = 0) -> None:
        feed_total = len(self.feeds)
        feeds_processed = 0
        episodes_processed = 0
        transcripts_created = 0
        transcripts_skipped = 0
        transcripts_failed = 0

        logger.info("Starting pipeline for %d feeds", feed_total)
        self._emit_progress(
            stage="start",
            feeds_total=feed_total,
            feeds_processed=feeds_processed,
            episodes_processed=episodes_processed,
            transcripts_created=transcripts_created,
            transcripts_skipped=transcripts_skipped,
            transcripts_failed=transcripts_failed,
            message=f"Pipeline initialised for {feed_total} feed(s).",
        )

        self._check_cancel()

        with self.SessionLocal() as session:
            transcript_count = session.execute(select(func.count(Transcript.id))).scalar() or 0
            keyword_count = session.execute(select(func.count(EpisodeKeyword.id))).scalar() or 0
            if transcript_count and keyword_count == 0:
                self.keywords_dirty = True
            for feed in self.feeds:
                self._check_cancel()
                self._emit_progress(
                    stage="fetch_feed",
                    feeds_total=feed_total,
                    feeds_processed=feeds_processed,
                    episodes_processed=episodes_processed,
                    transcripts_created=transcripts_created,
                    transcripts_skipped=transcripts_skipped,
                    transcripts_failed=transcripts_failed,
                    current_feed=feed.rss_url,
                    message=f"Fetching feed {feed.rss_url}",
                )
                parsed_url = urlparse(feed.rss_url or "")
                if parsed_url.scheme not in {"http", "https"}:
                    logger.warning(
                        "Skipping feed %s: unsupported URL scheme '%s'",
                        feed.rss_url,
                        parsed_url.scheme or "(none)",
                    )
                    continue
                try:
                    logger.info("Processing feed %s", feed.rss_url)
                    feed_data = self._fetch_feed(feed.rss_url)
                except Exception as exc:
                    logger.exception("Failed to parse feed %s: %s", feed.rss_url, exc)
                    continue

                podcast = self._upsert_podcast(session, feed, feed_data)
                entries = list(feed_data.entries or [])
                total_entries = len(entries)

                logger.info("Feed %s yielded %d episodes", feed.rss_url, len(entries))
                if not entries:
                    logger.warning("Feed %s returned no episodes; check the RSS URL or network access.", feed.rss_url)
                    self._emit_progress(
                        stage="feed_empty",
                        feeds_total=feed_total,
                        feeds_processed=feeds_processed,
                        episodes_processed=episodes_processed,
                        transcripts_created=transcripts_created,
                        transcripts_skipped=transcripts_skipped,
                        transcripts_failed=transcripts_failed,
                        current_feed=feed.rss_url,
                        message=f"No episodes found for {feed.rss_url}.",
                    )

                if max_episodes > 0:
                    entries = entries[:max_episodes]

                for entry in entries:
                    self._check_cancel()
                    episode_title = entry.get("title") or "Untitled episode"
                    self._emit_progress(
                        stage="episode_start",
                        feeds_total=feed_total,
                        feeds_processed=feeds_processed,
                        episodes_processed=episodes_processed,
                        transcripts_created=transcripts_created,
                        transcripts_skipped=transcripts_skipped,
                        transcripts_failed=transcripts_failed,
                        current_feed=feed.rss_url,
                        current_episode=episode_title,
                        message=f"Processing episode {episode_title}",
                    )
                    try:
                        outcome = self._process_episode(session, podcast, entry, feed.language)
                    except Exception as exc:
                        logger.exception("Failed to process episode %s: %s", entry.get("title"), exc)
                        session.rollback()
                        transcripts_failed += 1
                        episodes_processed += 1
                        self._emit_progress(
                            stage="episode_failed",
                            feeds_total=feed_total,
                            feeds_processed=feeds_processed,
                            episodes_processed=episodes_processed,
                            transcripts_created=transcripts_created,
                            transcripts_skipped=transcripts_skipped,
                            transcripts_failed=transcripts_failed,
                            current_feed=feed.rss_url,
                            current_episode=episode_title,
                            message=f"Failed to process episode {episode_title}: {exc}",
                        )
                    else:
                        session.commit()
                        episodes_processed += 1
                        if outcome == "transcribed":
                            transcripts_created += 1
                            stage = "episode_transcribed"
                            message = f"Stored transcript for {episode_title}"
                        else:
                            transcripts_skipped += 1
                            stage = "episode_skipped"
                            message = f"Skipped existing transcript for {episode_title}"
                        self._emit_progress(
                            stage=stage,
                            feeds_total=feed_total,
                            feeds_processed=feeds_processed,
                            episodes_processed=episodes_processed,
                            transcripts_created=transcripts_created,
                            transcripts_skipped=transcripts_skipped,
                            transcripts_failed=transcripts_failed,
                            current_feed=feed.rss_url,
                            current_episode=episode_title,
                            message=message,
                        )

                feeds_processed += 1
                self._emit_progress(
                    stage="feed_complete",
                    feeds_total=feed_total,
                    feeds_processed=feeds_processed,
                    episodes_processed=episodes_processed,
                    transcripts_created=transcripts_created,
                    transcripts_skipped=transcripts_skipped,
                    transcripts_failed=transcripts_failed,
                    current_feed=feed.rss_url,
                    message=f"Completed feed {feed.rss_url} ({total_entries} episode(s) visited).",
                )

        self._check_cancel()

        if self.keywords_dirty:
            try:
                logger.info("Refreshing keyword analysis using TF-IDF")
                self._recompute_keywords()
                self.keywords_dirty = False
            except Exception as exc:
                logger.exception("Failed to recompute keywords: %s", exc)

        self._check_cancel()

        self.generate_report()
        self._write_topic_network_cache()
        self._emit_progress(
            stage="completed",
            feeds_total=feed_total,
            feeds_processed=feeds_processed,
            episodes_processed=episodes_processed,
            transcripts_created=transcripts_created,
            transcripts_skipped=transcripts_skipped,
            transcripts_failed=transcripts_failed,
            message="Pipeline run finished.",
        )

    def _write_topic_network_cache(self) -> None:
        cache_path = self.output_dir / "topic_network.json"
        try:
            with self.SessionLocal() as session:
                graph, summary = build_topic_network(session, max_keywords=45, top_keywords_per_episode=6)
            cache_payload = {
                "generated_at": dt.datetime.utcnow().isoformat(),
                "graph": graph,
                "summary": summary,
            }
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(cache_payload, ensure_ascii=False), encoding="utf-8")
            logger.info("Topic network cache written to %s", cache_path)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to write topic network cache: %s", exc)

    def _upsert_podcast(self, session, feed: PodcastFeed, feed_data) -> Podcast:
        stmt = select(Podcast).where(Podcast.rss_url == feed.rss_url)
        podcast = session.execute(stmt).scalar_one_or_none()

        feed_meta = getattr(feed_data, "feed", {})
        description = feed_meta.get("summary") or feed_meta.get("subtitle")
        image_url = (feed_meta.get("image") or {}).get("href") if isinstance(feed_meta.get("image"), dict) else None

        last_build_date = None
        if feed_meta.get("published_parsed"):
            last_build_date = dt.datetime.fromtimestamp(
                dt.datetime(*feed_meta.published_parsed[:6]).timestamp(), tz=dt.timezone.utc
            )

        if not podcast and feed.name:
            name_match_stmt = select(Podcast).where(func.lower(Podcast.name) == func.lower(feed.name))
            podcast = session.execute(name_match_stmt).scalar_one_or_none()
            if podcast:
                logger.info(
                    "Reusing existing podcast %s based on name match and updating RSS URL to %s",
                    podcast.name,
                    feed.rss_url,
                )
                podcast.rss_url = feed.rss_url

        if podcast:
            podcast.name = feed.name or podcast.name
            podcast.category = feed.category or podcast.category
            podcast.language = feed.language or podcast.language
            podcast.description = description or podcast.description
            podcast.website = feed_meta.get("link") or podcast.website
            podcast.image_url = image_url or podcast.image_url
            podcast.last_build_date = last_build_date or podcast.last_build_date
            logger.debug("Updated podcast %s", podcast.name)
        else:
            podcast = Podcast(
                name=feed.name,
                rss_url=feed.rss_url,
                category=feed.category,
                language=feed.language,
                description=description,
                website=feed_meta.get("link"),
                image_url=image_url,
                last_build_date=last_build_date,
            )
            session.add(podcast)
            session.flush()
            logger.info("Created podcast %s", podcast.name)

        session.commit()
        return podcast

    def _process_episode(self, session, podcast: Podcast, entry, feed_language: Optional[str]) -> str:
        self._check_cancel()
        guid = entry.get("guid") or entry.get("id") or entry.get("link")
        if not guid:
            raise ValueError("Episode entry missing GUID/ID")

        stmt = select(Episode).where(Episode.podcast_id == podcast.id, Episode.guid == guid)
        episode = session.execute(stmt).scalar_one_or_none()

        published_at = None
        published_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
        if published_parsed:
            published_at = dt.datetime(*published_parsed[:6], tzinfo=dt.timezone.utc)

        audio_url = self._extract_audio_url(entry)
        if not audio_url:
            raise ValueError(f"No audio enclosure found for entry {guid}")

        duration = self._parse_duration(entry.get("itunes_duration"))
        image_url = None
        if entry.get("image"):
            image = entry.get("image")
            image_url = image.get("href") if isinstance(image, dict) else image
        elif entry.get("itunes_image"):
            itunes_image = entry.get("itunes_image")
            image_url = itunes_image.get("href") if isinstance(itunes_image, dict) else itunes_image

        language_hint = normalise_language_code(
            entry.get("language") if hasattr(entry, "get") else None,
            feed_language,
            podcast.language,
        )

        if episode is None:
            episode = Episode(
                podcast_id=podcast.id,
                guid=guid,
                title=entry.get("title", "Untitled Episode"),
                description=entry.get("summary"),
                published_at=published_at,
                audio_url=audio_url,
                image_url=image_url,
                duration=duration,
            )
            session.add(episode)
            session.flush()
            logger.info("Added episode %s - %s", podcast.name, episode.title)
        else:
            episode.title = entry.get("title") or episode.title
            episode.description = entry.get("summary") or episode.description
            episode.published_at = published_at or episode.published_at
            episode.audio_url = audio_url or episode.audio_url
            episode.image_url = image_url or episode.image_url
            episode.duration = duration or episode.duration

        if episode.transcribed_at:
            if self.force_reprocess:
                logger.info("Reprocessing episode %s; clearing existing transcript.", episode.title)
                if episode.transcript:
                    session.delete(episode.transcript)
                    episode.transcript = None
                session.execute(delete(EpisodeKeyword).where(EpisodeKeyword.episode_id == episode.id))
                episode.transcribed_at = None
                episode.deepgram_request_id = None
                self.keywords_dirty = True
                session.flush()
            else:
                logger.info("Episode %s already transcribed; skipping", episode.title)
                session.commit()
                return "skipped"

        with tempfile.NamedTemporaryFile(delete=False, suffix=self._guess_extension(audio_url)) as tmp_file:
            self._check_cancel()
            logger.info("Downloading %s", audio_url)
            response = requests.get(audio_url, stream=True, timeout=60)
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp_file.write(chunk)
                if self._should_stop():
                    raise PipelineCancelled("Cancellation requested during download.")
            tmp_path = Path(tmp_file.name)

        try:
            self._check_cancel()
            transcript_data = self._transcribe(tmp_path, language_hint)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Failed to remove temporary file %s", tmp_path)

        self._check_cancel()
        metadata_kwargs = {
            "transaction_key": transcript_data.get("transaction_key"),
            "request_id": transcript_data.get("request_id"),
            "sha256": transcript_data.get("sha256"),
            "metadata_created": transcript_data.get("metadata_created"),
            "channel_count": transcript_data.get("channel_count"),
            "models": transcript_data.get("models"),
            "model_primary_name": transcript_data.get("model_primary_name"),
            "model_primary_version": transcript_data.get("model_primary_version"),
            "model_primary_arch": transcript_data.get("model_primary_arch"),
            "model_info": transcript_data.get("model_info"),
            "summary_info": transcript_data.get("summary_info"),
            "channels": transcript_data.get("channels"),
            "summary": transcript_data.get("summary"),
        }

        transcript = Transcript(
            episode_id=episode.id,
            transcript_text=transcript_data["text"],
            confidence=transcript_data["confidence"],
            seconds=transcript_data["duration"],
            diarization=transcript_data["diarization"],
            raw_response=transcript_data["raw"],
            **metadata_kwargs,
        )
        episode.transcript = transcript
        episode.transcribed_at = dt.datetime.now(dt.timezone.utc)
        episode.deepgram_request_id = transcript_data.get("request_id")

        session.add(transcript)

        self.keywords_dirty = True

        logger.info("Stored transcript for %s", episode.title)
        return "transcribed"

    def _extract_audio_url(self, entry) -> Optional[str]:
        enclosures = entry.get("enclosures") or entry.get("links", [])
        for enclosure in enclosures:
            href = enclosure.get("href") if isinstance(enclosure, dict) else enclosure
            if not href:
                continue
            enclosure_type = enclosure.get("type") if isinstance(enclosure, dict) else ""
            if enclosure_type and enclosure_type.startswith("audio"):
                return href
            if href.lower().endswith((".mp3", ".m4a", ".wav", ".aac", ".ogg")):
                return href
        return None

    def _parse_duration(self, duration_value) -> Optional[int]:
        if not duration_value:
            return None
        if isinstance(duration_value, int):
            return duration_value
        if isinstance(duration_value, str):
            parts = duration_value.split(":")
            try:
                parts = [int(p) for p in parts]
            except ValueError:
                return None
            if len(parts) == 3:
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
            if len(parts) == 2:
                return parts[0] * 60 + parts[1]
            if len(parts) == 1:
                return parts[0]
        return None

    def _guess_extension(self, url: str) -> str:
        extension = Path(url).suffix
        if extension:
            return extension
        return ".mp3"

    def _transcribe(self, file_path: Path, language_hint: Optional[str]) -> Dict[str, object]:
        mimetype, _ = mimetypes.guess_type(file_path.name)
        source = {"buffer": open(file_path, "rb"), "mimetype": mimetype or "audio/mpeg"}
        option_kwargs = {
            "model": self.model,
            "smart_format": True,
            "punctuate": True,
            "diarize": True,
            "paragraphs": True,
            "utterances": True,
        }
        if language_hint:
            option_kwargs["language"] = language_hint

        options = PrerecordedOptions(**option_kwargs) if PrerecordedOptions else option_kwargs

        timeout = None
        try:  # Optional extended timeout if httpx is installed
            import httpx  # type: ignore

            timeout = httpx.Timeout(300.0, read=300.0)
        except Exception:
            timeout = None

        try:
            if timeout:
                response = self.deepgram.listen.prerecorded.v("1").transcribe_file(source, options, timeout=timeout)
            else:
                response = self.deepgram.listen.prerecorded.v("1").transcribe_file(source, options)
        except DeepgramError as exc:
            raise RuntimeError(f"Deepgram transcription failed: {exc}") from exc
        finally:
            source["buffer"].close()

        if hasattr(response, "to_dict"):
            response_data = response.to_dict()
        elif hasattr(response, "to_json"):
            response_data = json.loads(response.to_json())
        elif isinstance(response, dict):
            response_data = response
        else:
            raise RuntimeError("Unexpected Deepgram response format")

        channels = response_data.get("results", {}).get("channels", [])
        transcript_text = ""
        confidence = None
        duration = None
        diarization = response_data.get("results", {}).get("utterances", [])

        if channels:
            alternative = channels[0].get("alternatives", [{}])[0]
            transcript_text = alternative.get("transcript", "")
            confidence = alternative.get("confidence")
            words = alternative.get("words", [])
            if words:
                duration = words[-1].get("end", 0.0)
            else:
                duration = response_data.get("metadata", {}).get("duration")

        if not transcript_text:
            raise RuntimeError("Deepgram response missing transcript text")

        metadata_fields = extract_deepgram_metadata(response_data)
        if metadata_fields["channel_count"] is None and channels:
            metadata_fields["channel_count"] = len(channels)

        result_payload = {
            "text": transcript_text,
            "confidence": confidence,
            "duration": duration,
            "diarization": diarization,
            "raw": response_data,
        }
        result_payload.update(metadata_fields)
        return result_payload

    def _normalize_token(self, token: str) -> Optional[str]:
        token = (token or "").lower().strip("'\u2019")
        if not token:
            return None

        if token.endswith("n't"):
            base = token[:-3]
            if base and len(base) > 2:
                token = base
            else:
                token = "not"

        for suffix in ("'re", "\u2019re", "'ve", "\u2019ve", "'ll", "\u2019ll", "'d", "\u2019d", "'m", "\u2019m", "'s", "\u2019s"):
            if token.endswith(suffix):
                token = token[: -len(suffix)]
                break

        token = token.replace("'", "").replace("\u2019", "").strip()
        if not token or len(token) <= 3:
            return None
        if not token.isalpha():
            return None
        return token

    def _tokenize_for_keywords(self, text: str) -> List[str]:
        raw_tokens = re.findall(r"[a-zA-Z']+", text.lower())
        filtered: List[str] = []
        for raw in raw_tokens:
            token = self._normalize_token(raw)
            if not token or token in KEYWORD_STOPWORDS:
                continue
            filtered.append(token)

        bigrams = [" ".join(pair) for pair in zip(filtered, filtered[1:])]
        return filtered + bigrams

    def _recompute_keywords(self, top_k: int = 12) -> None:
        with self.SessionLocal() as session:
            episodes = (
                session.execute(
                    select(Episode)
                    .join(Transcript)
                    .options(
                        selectinload(Episode.transcript),
                    )
                )
                .scalars()
                .unique()
                .all()
            )

            if not episodes:
                logger.info("No transcripts available for keyword analysis yet.")
                return

            documents: List[Counter] = []
            episode_ids: List[int] = []
            document_frequencies: Counter = Counter()

            for episode in episodes:
                transcript_text = (episode.transcript.transcript_text or "").strip()
                if not transcript_text:
                    continue
                terms = self._tokenize_for_keywords(transcript_text)
                if not terms:
                    continue
                term_counts = Counter(terms)
                documents.append(term_counts)
                episode_ids.append(episode.id)
                document_frequencies.update(set(term_counts.keys()))

            if not documents:
                logger.info("Skipping keyword analysis because transcripts are empty.")
                return

            session.query(EpisodeKeyword).filter(EpisodeKeyword.episode_id.in_(episode_ids)).delete(
                synchronize_session=False
            )

            num_docs = len(documents)
            for idx, episode_id in enumerate(episode_ids):
                term_counts = documents[idx]
                total_terms = sum(term_counts.values())
                if total_terms == 0:
                    continue
                scores: Dict[str, float] = {}
                for term, count in term_counts.items():
                    df = document_frequencies[term]
                    idf = math.log((num_docs + 1) / (df + 1), 10) + 1.0
                    tf = count / total_terms
                    scores[term] = tf * idf

                top_terms = sorted(scores.items(), key=lambda item: item[1], reverse=True)[:top_k]
                for term, score in top_terms:
                    session.add(
                        EpisodeKeyword(
                            episode_id=episode_id,
                            keyword=term,
                            weight=round(score, 4),
                        )
                    )

            session.commit()
            logger.info("Recomputed keywords for %d episodes", len(episode_ids))

    def generate_report(self) -> Path:
        with self.SessionLocal() as session:
            podcast_rows: List[Podcast] = session.execute(select(Podcast)).scalars().all()

            connections = self._build_connections(session)
            graph_data = build_episode_graph(session)
            report_path = self.output_dir / "index.html"

            html = self._render_report(podcast_rows, connections, graph_data)
            report_path.write_text(html, encoding="utf-8")
            logger.info("Analysis report saved to %s", report_path)
            return report_path

    def _render_report(
        self,
        podcasts: Sequence[Podcast],
        connections: Sequence[Dict[str, object]],
        graph_data: Dict[str, List[Dict[str, object]]],
    ) -> str:
        return render_report_html(podcasts, connections, graph_data)

    def _fetch_feed(self, url: str):
        return fetch_feed(url)

    def _build_connections(self, session) -> List[Dict[str, object]]:
        return build_episode_connections(session)


def build_episode_connections(session, limit: int = 50) -> List[Dict[str, object]]:
    episodes = session.execute(
        select(Episode).options(
            selectinload(Episode.keywords),
            selectinload(Episode.podcast),
            selectinload(Episode.transcript),
        )
    ).scalars().all()
    connections: List[Dict[str, object]] = []

    def keyword_map(ep: Episode) -> Dict[str, float]:
        return {kw.keyword: kw.weight for kw in ep.keywords}

    for idx, left in enumerate(episodes):
        left_keywords = keyword_map(left)
        if not left_keywords:
            continue
        for right in episodes[idx + 1 :]:
            if left.podcast_id == right.podcast_id:
                continue
            right_keywords = keyword_map(right)
            if not right_keywords:
                continue
            shared = set(left_keywords) & set(right_keywords)
            if not shared:
                continue
            score = sum((left_keywords[key] + right_keywords[key]) / 2 for key in shared)
            connections.append(
                {
                    "left_episode": left,
                    "right_episode": right,
                    "shared_keywords": sorted(shared),
                    "score": round(score, 4),
                }
            )

    connections.sort(key=lambda item: item["score"], reverse=True)
    if limit:
        return connections[:limit]
    return connections


def build_episode_graph(session, limit: int = 50) -> Dict[str, List[Dict[str, object]]]:
    connections = build_episode_connections(session, limit)
    nodes: Dict[int, Dict[str, object]] = {}
    links: List[Dict[str, object]] = []

    def ensure_node(episode: Episode) -> None:
        if episode.id in nodes:
            return
        confidence = None
        if episode.transcript and episode.transcript.confidence is not None:
            try:
                confidence = round(float(episode.transcript.confidence), 4)
            except (TypeError, ValueError):
                confidence = None
        nodes[episode.id] = {
            "id": episode.id,
            "title": episode.title,
            "podcast": episode.podcast.name,
            "podcast_id": episode.podcast.id,
            "published_at": episode.published_at.isoformat() if episode.published_at else None,
            "confidence": confidence,
        }

    for connection in connections:
        left = connection["left_episode"]
        right = connection["right_episode"]
        ensure_node(left)
        ensure_node(right)
        links.append(
            {
                "source": left.id,
                "target": right.id,
                "score": connection["score"],
                "keywords": connection["shared_keywords"],
            }
        )

    return {"nodes": list(nodes.values()), "links": links}

def render_report_html(
    podcasts: Sequence[Podcast],
    connections: Sequence[Dict[str, object]],
    graph_data: Dict[str, List[Dict[str, object]]],
) -> str:
    def fmt_date(value: Optional[dt.datetime]) -> str:
        if not value:
            return "Unknown"
        return value.strftime("%Y-%m-%d")

    graph_json = json.dumps(graph_data)

    def fmt_confidence(transcript: Optional[Transcript]) -> str:
        if not transcript or transcript.confidence is None:
            return "N/A"
        try:
            return f"{float(transcript.confidence):.3f}"
        except (TypeError, ValueError):
            return "N/A"

    rows = []
    for podcast in podcasts:
        episode_rows = []
        for episode in sorted(podcast.episodes, key=lambda ep: ep.published_at or dt.datetime.min, reverse=True):
            keywords = ", ".join(kw.keyword for kw in episode.keywords[:6])
            episode_rows.append(
                f"""
                <tr>
                    <td>{fmt_date(episode.published_at)}</td>
                    <td>{episode.title}</td>
                    <td>{episode.duration or "?"}s</td>
                    <td>{fmt_confidence(episode.transcript)}</td>
                    <td>{keywords}</td>
                </tr>
                """
            )

        rows.append(
            f"""
            <section class="podcast">
                <h2>{podcast.name}</h2>
                <div class="meta">
                    <span><strong>Category:</strong> {podcast.category or "Unknown"}</span>
                    <span><strong>Language:</strong> {podcast.language or "Unknown"}</span>
                    <span><strong>Episodes:</strong> {len(podcast.episodes)}</span>
                    <span><strong>Website:</strong> <a href="{podcast.website or '#'}">{podcast.website or "n/a"}</a></span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Title</th>
                            <th>Duration</th>
                            <th>Confidence</th>
                            <th>Top Keywords</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(episode_rows)}
                    </tbody>
                </table>
            </section>
            """
        )

    connection_rows = []
    for connection in connections:
        left = connection["left_episode"]
        right = connection["right_episode"]
        shared = ", ".join(connection["shared_keywords"])
        connection_rows.append(
            f"""
            <tr>
                <td>{left.podcast.name} — {left.title}</td>
                <td>{right.podcast.name} — {right.title}</td>
                <td>{shared}</td>
                <td>{connection["score"]}</td>
            </tr>
            """
        )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Podcast Transcript Analysis</title>
<style>
    body {{
        font-family: Arial, sans-serif;
        margin: 2rem;
        background-color: #f8f9fa;
    }}
    h1 {{
        margin-bottom: 0.5rem;
    }}
    section.podcast {{
        background: #ffffff;
        border-radius: 8px;
        padding: 1.5rem;
        margin-bottom: 2rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
    }}
    section.podcast table {{
        width: 100%;
        border-collapse: collapse;
        margin-top: 1rem;
    }}
    table th, table td {{
        text-align: left;
        padding: 0.5rem;
        border-bottom: 1px solid #e2e6ea;
    }}
    .meta {{
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        color: #555;
    }}
    #connections {{
        background: #ffffff;
        border-radius: 8px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
    }}
    #connections table {{
        width: 100%;
        border-collapse: collapse;
    }}
    #graph {{
        background: #ffffff;
        border-radius: 8px;
        padding: 1.5rem;
        margin-bottom: 2rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
    }}
    #topic-graph {{
        width: 100%;
        height: 520px;
    }}
    footer {{
        margin-top: 2rem;
        color: #777;
        font-size: 0.9rem;
    }}
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js" integrity="sha512-pD7n0ANKPO/tUMGAfJOyrUo9qeycGQ21MCH2RKDWE/YXdz/BPZt6r9Ga6IpiObOqYkbK7gm+Yw2rZsf1CcfCOw==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
</head>
<body>
<h1>Podcast Transcript Analysis</h1>
<p>This report highlights metadata, transcription confidence, and keyword overlaps across curated democracy podcasts.</p>
<section id="graph">
    <h2>Episode Connection Graph</h2>
    <p>Nodes represent episodes; edges connect episodes from different podcasts sharing strong keyword overlap. Drag nodes to explore the landscape.</p>
    <svg id="topic-graph"></svg>
</section>
{''.join(rows)}
<section id="connections">
    <h2>Cross-Podcast Episode Connections</h2>
    <table>
        <thead>
            <tr>
                <th>Episode A</th>
                <th>Episode B</th>
                <th>Shared Keywords</th>
                <th>Score</th>
            </tr>
        </thead>
        <tbody>
            {''.join(connection_rows) if connection_rows else '<tr><td colspan="4">No cross-podcast connections identified yet.</td></tr>'}
        </tbody>
    </table>
</section>
<footer>
    Generated on {dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
</footer>
<script>
const graphData = {graph_json};
const svg = d3.select('#topic-graph');
const width = svg.node().getBoundingClientRect().width;
const height = svg.node().getBoundingClientRect().height;

const color = d3.scaleOrdinal(d3.schemeSet2);

const simulation = d3.forceSimulation(graphData.nodes)
    .force('link', d3.forceLink(graphData.links).id(d => d.id).distance(d => Math.max(120, 240 - (d.score * 80))).strength(0.4))
    .force('charge', d3.forceManyBody().strength(-220))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide().radius(70));

const link = svg.append('g')
    .attr('stroke', '#cbd5f5')
    .attr('stroke-width', 1.5)
    .selectAll('line')
    .data(graphData.links)
    .enter()
    .append('line')
    .attr('stroke-width', d => 1 + d.score * 4)
    .attr('stroke', '#94a3b8')
    .attr('opacity', 0.85);

const nodeGroup = svg.append('g')
    .selectAll('g')
    .data(graphData.nodes)
    .enter()
    .append('g')
    .call(d3.drag()
        .on('start', event => {{
            if (!event.active) simulation.alphaTarget(0.3).restart();
            event.subject.fx = event.subject.x;
            event.subject.fy = event.subject.y;
        }})
        .on('drag', event => {{
            event.subject.fx = event.x;
            event.subject.fy = event.y;
        }})
        .on('end', event => {{
            if (!event.active) simulation.alphaTarget(0);
            event.subject.fx = null;
            event.subject.fy = null;
        }})
    );

nodeGroup.append('circle')
    .attr('r', 18)
    .attr('fill', d => color(d.podcast))
    .attr('stroke', '#0f172a')
    .attr('stroke-width', 1.5);

nodeGroup.append('text')
    .attr('x', 22)
    .attr('y', 5)
    .attr('font-size', '12px')
    .attr('fill', '#0f172a')
    .text(d => `${{d.podcast}} — ${{d.title}}`)
    .each(function(d) {{
        const text = d3.select(this);
        if (this.getComputedTextLength() > width * 0.35) {{
            text.attr('font-size', '11px');
        }}
    }});

nodeGroup.append('title')
    .text(d => `${{d.podcast}}\n${{d.title}}`);

link.append('title')
    .text(d => `Shared keywords: ${{d.keywords.join(', ')}}`);

simulation.on('tick', () => {{
    link
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y);

    nodeGroup.attr('transform', d => `translate(${{d.x}}, ${{d.y}})`);
}});
</script>
</body>
</html>
"""

def fetch_feed(url: str):
    headers = {
        "User-Agent": "rss-analysis/1.0 (+https://example.com)",
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return feedparser.parse(response.content)


def load_feeds_from_csv(csv_path: Path) -> List[PodcastFeed]:
    feeds: List[PodcastFeed] = []
    with csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            feeds.append(
                PodcastFeed(
                    name=row["podcast_name"].strip(),
                    rss_url=row["rss_url"].strip(),
                    category=row.get("category", "").strip() or None,
                    language=row.get("language", "").strip() or None,
                )
            )
    return feeds


def load_feeds_from_db(database_url: str, feed_ids: Optional[Sequence[int]] = None) -> List[PodcastFeed]:
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    ensure_transcript_metadata_columns(engine)
    SessionLocal = sessionmaker(bind=engine)
    try:
        with SessionLocal() as session:
            query = select(FeedSubscription).order_by(FeedSubscription.name.asc())
            if feed_ids:
                query = query.where(FeedSubscription.id.in_(feed_ids))
            rows = session.execute(query).scalars().all()
            feeds = [
                PodcastFeed(
                    id=row.id,
                    name=row.name,
                    rss_url=row.rss_url,
                    category=row.category,
                    language=row.language,
                )
                for row in rows
            ]
            return feeds
    finally:
        engine.dispose()


def backfill_transcript_metadata(database_url: str) -> int:
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    ensure_transcript_metadata_columns(engine)
    SessionLocal = sessionmaker(bind=engine)

    updated = 0
    with SessionLocal() as session:
        transcripts = (
            session.execute(select(Transcript))
            .scalars()
            .all()
        )
        for transcript in transcripts:
            raw = transcript.raw_response
            if not raw:
                continue
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    continue

            metadata_fields = extract_deepgram_metadata(raw)
            channels = raw.get("results", {}).get("channels", []) if isinstance(raw, dict) else []
            if metadata_fields["channel_count"] is None and channels:
                metadata_fields["channel_count"] = len(channels)

            changed = False
            for key, value in metadata_fields.items():
                if getattr(transcript, key) != value:
                    setattr(transcript, key, value)
                    changed = True
            if changed:
                updated += 1
        session.commit()

    engine.dispose()
    return updated


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download and transcribe podcast episodes via Deepgram.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Optional CSV file containing podcast RSS feeds (columns: podcast_name,rss_url,category,language). "
        "When omitted, feeds are sourced from the feed_subscriptions table.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=f"Path to the SQLite database file (default: {DEFAULT_DB_PATH}).",
    )
    parser.add_argument(
        "--max-episodes",
        type=int,
        default=None,
        help="Maximum number of episodes per podcast to process (default: process all).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=f"Directory where generated artifacts (reports) will be stored (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--deepgram-api-key",
        type=str,
        default=os.environ.get("DEEPGRAM_API_KEY"),
        help="Deepgram API key. Defaults to the DEEPGRAM_API_KEY environment variable.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help=f"Deepgram model name to use for transcription (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to an environment file to load (falls back to .env and .env.local).",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help=f"Directory where pipeline logs will be written (default: {DEFAULT_LOG_DIR}).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        help=f"Logging verbosity (DEBUG, INFO, WARNING, ERROR). Defaults to {DEFAULT_LOG_LEVEL}.",
    )
    parser.add_argument(
        "--backfill-transcript-metadata",
        action="store_true",
        help="Populate transcript metadata columns from previously stored Deepgram payloads and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    env_candidates: List[Path] = []
    if args.env_file:
        env_candidates.append(Path(args.env_file).expanduser().resolve())
    env_candidates.extend(
        [
            BASE_DIR / ".env",
            BASE_DIR / ".env.local",
            Path(".env").resolve(),
            Path(".env.local").resolve(),
        ]
    )

    loaded_any = False
    for env_path in env_candidates:
        if env_path.exists():
            load_dotenv(env_path, override=False)
            loaded_any = True

    if not loaded_any and args.env_file:
        print(f"Warning: requested env file {args.env_file} not found; falling back to environment variables.", file=sys.stderr)

    db_path = (args.db or DEFAULT_DB_PATH).expanduser().resolve()
    output_dir = (args.output_dir or DEFAULT_OUTPUT_DIR).expanduser().resolve()
    log_dir = (args.log_dir or DEFAULT_LOG_DIR).expanduser().resolve()
    log_level = (args.log_level or DEFAULT_LOG_LEVEL).upper()
    model = args.model or DEFAULT_MODEL
    max_episodes = args.max_episodes if args.max_episodes is not None else DEFAULT_MAX_EPISODES

    csv_path = args.csv.expanduser().resolve() if args.csv else None

    run_id = setup_logging(log_dir, log_level)
    logger.info(
        "Pipeline run %s initialised | database=%s | output_dir=%s",
        run_id,
        db_path,
        output_dir,
    )

    database_url = f"sqlite:///{db_path}"

    if args.backfill_transcript_metadata:
        updated = backfill_transcript_metadata(database_url)
        logger.info(
            "Pipeline run %s backfilled transcript metadata for %d transcript(s).",
            run_id,
            updated,
        )
        return

    if not args.deepgram_api_key:
        args.deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY")

    if not args.deepgram_api_key:
        raise SystemExit("Deepgram API key required. Provide via --deepgram-api-key or DEEPGRAM_API_KEY env var.")

    if csv_path:
        feeds = load_feeds_from_csv(csv_path)
    else:
        feeds = load_feeds_from_db(database_url)

    if not feeds:
        hint = (
            "Add feeds via the web interface (/feeds) or provide a CSV file with "
            "columns podcast_name,rss_url,category,language."
        )
        if not csv_path and DEFAULT_CSV_PATH.exists():
            hint += f" Legacy CSV detected at {DEFAULT_CSV_PATH}; run with --csv {DEFAULT_CSV_PATH} to import."
        raise SystemExit(f"No feeds configured. {hint}")

    logger.info(
        "Run %s launching pipeline | feeds=%d | model=%s | max_episodes=%s",
        run_id,
        len(feeds),
        model,
        "all" if max_episodes == 0 else max_episodes,
    )

    pipeline = RssTranscriptionPipeline(
        feeds=feeds,
        database_url=database_url,
        deepgram_api_key=args.deepgram_api_key,
        output_dir=output_dir,
        model=model,
    )
    start_time = dt.datetime.now(dt.timezone.utc)
    pipeline.run(max_episodes=max_episodes)
    duration = dt.datetime.now(dt.timezone.utc) - start_time
    logger.info(
        "Run %s completed successfully in %.2f seconds",
        run_id,
        duration.total_seconds(),
    )


if __name__ == "__main__":
    main()
