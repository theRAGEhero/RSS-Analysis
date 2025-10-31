from __future__ import annotations

import csv
import datetime as dt
import io
import json
import logging
import os
import re
import sys
import threading
import uuid
from collections import Counter, defaultdict
from itertools import combinations
from functools import wraps

try:
    import numpy as np
except ImportError:  # pragma: no cover - optional dependency
    np = None  # type: ignore
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional, Any, Dict, List, Tuple, Set

SRC_DIR = Path(__file__).resolve().parent
BASE_DIR = SRC_DIR.parent
for path in (BASE_DIR, SRC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from flask import Flask, render_template, jsonify, request, Response, redirect, url_for
from dotenv import load_dotenv
from markupsafe import Markup, escape
from sqlalchemy import create_engine, select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker, selectinload, load_only, defer

from rss_transcriber import (
    Base,
    Episode,
    EpisodeKeyword,
    FeedSubscription,
    Podcast,
    Transcript,
    RssTranscriptionPipeline,
    PipelineCancelled,
    build_episode_graph,
    DEFAULT_MODEL,
    DEFAULT_DB_PATH,
    DEFAULT_LOG_DIR,
    DEFAULT_OUTPUT_DIR,
    ensure_transcript_metadata_columns,
    fetch_feed,
    load_feeds_from_db,
    setup_logging,
)


@dataclass
class PipelineJobState:
    id: str
    status: str = "pending"
    feeds_total: int = 0
    feeds_processed: int = 0
    episodes_processed: int = 0
    transcripts_created: int = 0
    transcripts_skipped: int = 0
    transcripts_failed: int = 0
    started_at: Optional[dt.datetime] = None
    finished_at: Optional[dt.datetime] = None
    current_feed: Optional[str] = None
    current_episode: Optional[str] = None
    stage: Optional[str] = None
    error: Optional[str] = None
    run_label: Optional[str] = None
    log_path: Optional[Path] = None
    log_level: str = "INFO"
    messages: List[str] = field(default_factory=list)
    cancel_requested: bool = False
    cancelled_at: Optional[dt.datetime] = None
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _cancel_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)

    def mark_started(self) -> None:
        with self._lock:
            self.status = "running"
            self.stage = "initialising"
            self.started_at = dt.datetime.utcnow()

    def mark_completed(self) -> None:
        with self._lock:
            self.status = "completed"
            self.stage = "completed"
            self.finished_at = dt.datetime.utcnow()

    def mark_failed(self, message: str) -> None:
        with self._lock:
            self.status = "failed"
            self.stage = "failed"
            self.error = message
            self.finished_at = dt.datetime.utcnow()
            self._append_message(f"Pipeline failed: {message}")

    def mark_cancelled(self, message: str = "Pipeline run cancelled.") -> None:
        with self._lock:
            self.status = "cancelled"
            self.stage = "cancelled"
            self.finished_at = dt.datetime.utcnow()
            self.cancelled_at = self.finished_at
            self._append_message(message)

    def _append_message(self, message: str) -> None:
        if not message:
            return
        self.messages.append(message)
        if len(self.messages) > 25:
            self.messages = self.messages[-25:]

    def apply_event(self, event: Dict[str, Any]) -> None:
        def _coerce_int(value: Any) -> Optional[int]:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        with self._lock:
            for attr in (
                "feeds_total",
                "feeds_processed",
                "episodes_processed",
                "transcripts_created",
                "transcripts_skipped",
                "transcripts_failed",
            ):
                if attr in event:
                    coerced = _coerce_int(event.get(attr))
                    if coerced is not None:
                        setattr(self, attr, coerced)

            if "current_feed" in event:
                self.current_feed = event.get("current_feed") or None
            if "current_episode" in event:
                self.current_episode = event.get("current_episode") or None

            stage = event.get("stage")
            if stage:
                self.stage = str(stage)
                if self.stage == "completed" and self.status != "failed":
                    self.status = "completed"
                    if self.finished_at is None:
                        self.finished_at = dt.datetime.utcnow()
                elif self.stage == "failed":
                    self.status = "failed"
                    if self.finished_at is None:
                        self.finished_at = dt.datetime.utcnow()
                elif self.stage == "cancelled":
                    self.status = "cancelled"
                    if self.finished_at is None:
                        self.finished_at = dt.datetime.utcnow()
                elif self.stage == "cancelling":
                    self.status = "cancelling"
                elif self.stage == "start":
                    self.status = "running"

            message = event.get("message")
            if message:
                self._append_message(str(message))

    def as_dict(self) -> Dict[str, Any]:
        with self._lock:
            percent = 0
            if self.feeds_total:
                percent = int(round((self.feeds_processed / self.feeds_total) * 100))
            elif self.episodes_processed:
                percent = min(int(self.episodes_processed), 100)

            data: Dict[str, Any] = {
                "id": self.id,
                "status": self.status,
                "stage": self.stage,
                "feeds_total": self.feeds_total,
                "feeds_processed": self.feeds_processed,
                "episodes_processed": self.episodes_processed,
                "transcripts_created": self.transcripts_created,
                "transcripts_skipped": self.transcripts_skipped,
                "transcripts_failed": self.transcripts_failed,
                "percent": percent,
                "current_feed": self.current_feed,
                "current_episode": self.current_episode,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "error": self.error,
                "run_label": self.run_label,
                "log_level": self.log_level,
                "messages": list(self.messages),
                "cancel_requested": self.cancel_requested,
            }
            if self.log_path:
                try:
                    data["log_path"] = str(self.log_path.relative_to(BASE_DIR))
                except ValueError:
                    data["log_path"] = str(self.log_path)
            else:
                data["log_path"] = None
        return data

    def get_log_path(self) -> Optional[Path]:
        with self._lock:
            return self.log_path

    def get_cancel_event(self) -> threading.Event:
        return self._cancel_event

    def request_cancel(self) -> bool:
        with self._lock:
            if self.status in {"completed", "failed", "cancelled"}:
                return False
            if self.cancel_requested:
                return False
            self.cancel_requested = True
            self.stage = "cancelling"
            self._append_message("Cancellation requested. Stopping safely…")
        self._cancel_event.set()
        return True


@dataclass
class PipelineJobConfig:
    db_path: Path
    output_dir: Path
    log_dir: Path
    deepgram_api_key: str
    model: str
    log_level: str
    max_episodes: int = 0
    feed_ids: Optional[List[int]] = None
    force_reprocess: bool = False


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def create_app(database_path: Optional[Path] = None) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
    )
    app.jinja_env.filters.setdefault("slugify", slugify)

    def format_timestamp(value: Optional[float]) -> str:
        if value is None:
            return "0:00"
        total_seconds = max(float(value), 0.0)
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)
        return f"{minutes}:{seconds:02d}"

    app.jinja_env.filters.setdefault("format_timestamp", format_timestamp)

    def format_duration(value: Optional[float]) -> str:
        if value is None:
            return "Unknown"
        try:
            total_seconds = int(float(value))
        except (TypeError, ValueError):
            return "Unknown"
        hours, remainder = divmod(max(total_seconds, 0), 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:d}:{seconds:02d}"

    app.jinja_env.filters.setdefault("format_duration", format_duration)

    def render_description(value: Optional[str]) -> Markup:
        if not value:
            return Markup("")
        if "<" in value and ">" in value:
            return Markup(value)
        escaped = escape(value)
        return Markup(escaped.replace("\n", "<br>"))

    app.jinja_env.filters.setdefault("render_description", render_description)
    app.context_processor(lambda: {"current_year": dt.datetime.now().year})

    env_candidates = [
        BASE_DIR / ".env",
        BASE_DIR / ".env.local",
        Path(".env").resolve(),
        Path(".env.local").resolve(),
    ]
    for env_path in env_candidates:
        if env_path.exists():
            load_dotenv(env_path, override=False)

    db_path = Path(database_path) if database_path is not None else DEFAULT_DB_PATH
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    ensure_transcript_metadata_columns(engine)
    SessionLocal = sessionmaker(bind=engine)
    topic_cache_path = DEFAULT_OUTPUT_DIR / "topic_network.json"
    feed_admin_user = os.getenv("FEED_ADMIN_USER")
    feed_admin_pass = os.getenv("FEED_ADMIN_PASS")

    resolved_db_path = Path(db_path).expanduser().resolve()
    pipeline_defaults = {
        "db_path": str(resolved_db_path),
        "output_dir": str((DEFAULT_OUTPUT_DIR if DEFAULT_OUTPUT_DIR.is_absolute() else (BASE_DIR / DEFAULT_OUTPUT_DIR)).resolve()),
        "log_dir": str((DEFAULT_LOG_DIR if DEFAULT_LOG_DIR.is_absolute() else (BASE_DIR / DEFAULT_LOG_DIR)).resolve()),
        "model": os.getenv("PIPELINE_MODEL", DEFAULT_MODEL),
        "max_episodes": 0,
    }
    pipeline_jobs: Dict[str, PipelineJobState] = {}
    job_lock = threading.Lock()
    active_pipeline_job_id: Optional[str] = None
    valid_log_levels = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}

    def tail_file(path: Optional[Path], max_lines: int = 40) -> List[str]:
        if not path or not path.exists():
            return []
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                lines = handle.readlines()
        except OSError:
            return []
        return [line.rstrip("\n") for line in lines[-max_lines:]]

    def resolve_path(value: Optional[str], default: str) -> Path:
        base_path = Path(default)
        if base_path and not base_path.is_absolute():
            base_path = (BASE_DIR / base_path).resolve()
        if not value:
            return base_path
        candidate = Path(str(value)).expanduser()
        if not candidate.is_absolute():
            candidate = (BASE_DIR / candidate).resolve()
        return candidate

    def get_pipeline_job(job_id: str) -> Optional[PipelineJobState]:
        with job_lock:
            return pipeline_jobs.get(job_id)

    def build_pipeline_config(payload: Dict[str, Any]) -> PipelineJobConfig:
        db_path_value = resolve_path(payload.get("db_path"), pipeline_defaults["db_path"])
        output_dir_value = resolve_path(payload.get("output_dir"), pipeline_defaults["output_dir"])
        log_dir_value = resolve_path(payload.get("log_dir"), pipeline_defaults["log_dir"])
        model = (payload.get("model") or pipeline_defaults["model"]).strip() or pipeline_defaults["model"]
        log_level = str(payload.get("log_level") or "INFO").upper()
        if log_level not in valid_log_levels:
            log_level = "INFO"
        try:
            max_episodes = int(payload.get("max_episodes", pipeline_defaults["max_episodes"]))
            max_episodes = max(max_episodes, 0)
        except (TypeError, ValueError):
            max_episodes = pipeline_defaults["max_episodes"]

        deepgram_api_key = (payload.get("deepgram_api_key") or os.getenv("DEEPGRAM_API_KEY") or "").strip()
        if not deepgram_api_key:
            raise ValueError("Deepgram API key is required to run the pipeline.")

        feed_ids: List[int] = []
        single_feed_id = payload.get("feed_id")
        if single_feed_id is not None:
            try:
                feed_ids.append(int(single_feed_id))
            except (TypeError, ValueError):
                raise ValueError("Invalid feed_id provided; expected an integer.")

        payload_feed_ids = payload.get("feed_ids")
        if isinstance(payload_feed_ids, list):
            for value in payload_feed_ids:
                try:
                    feed_ids.append(int(value))
                except (TypeError, ValueError):
                    raise ValueError("feed_ids must contain integers only.")
        elif payload_feed_ids is not None and not isinstance(payload_feed_ids, list):
            raise ValueError("feed_ids must be provided as a list of integers.")

        unique_feed_ids = sorted({feed_id for feed_id in feed_ids if feed_id is not None})
        force_reprocess = bool(payload.get("force_reprocess"))
        if force_reprocess and not unique_feed_ids:
            raise ValueError("force_reprocess requires at least one feed_id.")

        return PipelineJobConfig(
            db_path=db_path_value,
            output_dir=output_dir_value,
            log_dir=log_dir_value,
            deepgram_api_key=deepgram_api_key,
            model=model,
            log_level=log_level,
            max_episodes=max_episodes,
            feed_ids=unique_feed_ids or None,
            force_reprocess=force_reprocess,
        )

    def _pipeline_job_thread(job: PipelineJobState, config: PipelineJobConfig) -> None:
        nonlocal active_pipeline_job_id
        root_logger = logging.getLogger()
        previous_handlers = list(root_logger.handlers)
        previous_level = root_logger.level
        job.mark_started()
        job.log_level = config.log_level
        log_dir = config.log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        run_label_seed = f"pipeline-web-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S')}-{job.id[:8]}"
        try:
            configured_run_label = setup_logging(log_dir, config.log_level, run_label=run_label_seed)
            runs_dir = log_dir / "runs"
            with job._lock:
                job.run_label = configured_run_label
                job.log_path = runs_dir / f"{configured_run_label}.log"

            database_url = f"sqlite:///{config.db_path}"
            feeds = load_feeds_from_db(database_url, config.feed_ids)
            if config.feed_ids:
                found_ids = {feed.id for feed in feeds if feed.id is not None}
                missing = [str(feed_id) for feed_id in config.feed_ids if feed_id not in found_ids]
                if missing:
                    raise RuntimeError(f"Feed(s) not found: {', '.join(missing)}")
            if not feeds:
                raise RuntimeError("No feeds configured. Add feeds in the Manage Feeds page before running the pipeline.")

            feed_names_preview = ", ".join(feed.name for feed in feeds[:5])
            message = f"Loaded {len(feeds)} feed(s); starting pipeline."
            if config.feed_ids:
                message += f" Target feeds: {feed_names_preview}."
            if config.force_reprocess:
                message += " Reprocessing transcripts."

            job.apply_event(
                {
                    "stage": "start",
                    "feeds_total": len(feeds),
                    "message": message,
                }
            )

            pipeline = RssTranscriptionPipeline(
                feeds=feeds,
                database_url=database_url,
                deepgram_api_key=config.deepgram_api_key,
                output_dir=config.output_dir,
                model=config.model,
                progress_callback=job.apply_event,
                stop_event=job.get_cancel_event(),
                force_reprocess=config.force_reprocess,
            )
            pipeline.run(max_episodes=config.max_episodes)
            if job.cancel_requested and job.get_cancel_event().is_set():
                job.mark_cancelled("Pipeline run cancelled.")
                job.apply_event({"stage": "cancelled", "message": "Pipeline run cancelled."})
            else:
                job.mark_completed()
                job.apply_event({"message": "Pipeline run completed successfully."})
        except PipelineCancelled as exc:
            message = str(exc) or "Pipeline run cancelled."
            job.mark_cancelled(message)
            job.apply_event({"stage": "cancelled", "message": message})
        except Exception as exc:  # pragma: no cover - defensive
            logging.getLogger(__name__).exception("Pipeline job %s failed", job.id)
            job.mark_failed(str(exc))
        finally:
            current_handlers = list(root_logger.handlers)
            for handler in current_handlers:
                if handler not in previous_handlers:
                    try:
                        handler.close()
                    except Exception:
                        pass
                root_logger.removeHandler(handler)
            for handler in previous_handlers:
                if handler not in root_logger.handlers:
                    root_logger.addHandler(handler)
            root_logger.setLevel(previous_level)
            with job_lock:
                if active_pipeline_job_id == job.id:
                    active_pipeline_job_id = None

    def start_pipeline_job(config: PipelineJobConfig) -> PipelineJobState:
        nonlocal active_pipeline_job_id
        job = PipelineJobState(id=str(uuid.uuid4()))
        job.log_level = config.log_level
        job.apply_event({"stage": "queued", "message": "Pipeline job queued."})
        worker = threading.Thread(
            target=_pipeline_job_thread,
            args=(job, config),
            name=f"pipeline-run-{job.id}",
            daemon=True,
        )
        with job_lock:
            pipeline_jobs[job.id] = job
            active_pipeline_job_id = job.id
        worker.start()
        return job

    def feeds_auth_required() -> bool:
        return bool(feed_admin_user)

    def check_feed_auth() -> bool:
        if not feeds_auth_required():
            return True
        auth = request.authorization
        if not auth:
            return False
        return auth.username == feed_admin_user and auth.password == feed_admin_pass

    def require_feed_auth(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not check_feed_auth():
                return Response(
                    "Authentication required",
                    401,
                    {"WWW-Authenticate": 'Basic realm="Manage Feeds"'},
                )
            return func(*args, **kwargs)

        return wrapper

    def get_session() -> Session:
        return SessionLocal()

    @app.route("/feeds/import/pipeline/start", methods=["POST"])
    @require_feed_auth
    def start_pipeline_run():
        payload = request.get_json(silent=True) or {}
        try:
            config = build_pipeline_config(payload)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        with job_lock:
            current_active = active_pipeline_job_id
            if current_active and current_active in pipeline_jobs:
                return jsonify({"error": "Another pipeline run is already in progress.", "job_id": current_active}), 409
        job = start_pipeline_job(config)
        return jsonify({"job_id": job.id}), 202

    @app.route("/feeds/import/pipeline/status", methods=["GET"])
    @require_feed_auth
    def pipeline_status():
        job_id = request.args.get("job_id")
        if not job_id:
            return jsonify({"error": "Parameter 'job_id' is required."}), 400
        job = get_pipeline_job(job_id)
        if not job:
            return jsonify({"error": "Job not found."}), 404
        snapshot = job.as_dict()
        snapshot["log_tail"] = tail_file(job.get_log_path())
        return jsonify(snapshot)

    @app.route("/feeds/import/pipeline/stop", methods=["POST"])
    @require_feed_auth
    def stop_pipeline_run():
        payload = request.get_json(silent=True) or {}
        job_id = payload.get("job_id")
        if not job_id:
            with job_lock:
                job_id = active_pipeline_job_id
        if not job_id:
            return jsonify({"error": "No active pipeline run."}), 404
        job = get_pipeline_job(job_id)
        if not job:
            return jsonify({"error": "Job not found."}), 404
        if not job.request_cancel():
            return jsonify({"error": "Unable to cancel run.", "status": job.status}), 409
        job.apply_event({"stage": "cancelling", "message": "Cancellation requested."})
        return jsonify({"job_id": job.id, "status": "cancelling"}), 202

    @app.route("/feeds/import/pipeline/jobs", methods=["GET"])
    @require_feed_auth
    def list_pipeline_jobs():
        with job_lock:
            jobs = list(pipeline_jobs.values())
            current_active = active_pipeline_job_id
        jobs.sort(key=lambda job: job.started_at or dt.datetime.min, reverse=True)
        payload = []
        for job in jobs[:8]:
            snapshot = job.as_dict()
            if job.id == current_active:
                snapshot["log_tail"] = tail_file(job.get_log_path())
            payload.append(snapshot)
        return jsonify({"jobs": payload, "active_job_id": current_active})

    def build_topic_network(
        session: Session,
        max_keywords: int = 40,
        top_keywords_per_episode: int = 5,
        max_edges: int = 400,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        episodes = (
            session.execute(
                select(Episode)
                .options(
                    load_only(
                        Episode.id,
                        Episode.title,
                        Episode.published_at,
                        Episode.podcast_id,
                    ),
                    selectinload(Episode.podcast).options(
                        load_only(Podcast.id, Podcast.name, Podcast.category)
                    ),
                    selectinload(Episode.keywords).options(
                        load_only(EpisodeKeyword.keyword, EpisodeKeyword.weight)
                    ),
                    selectinload(Episode.transcript).options(
                        load_only(Transcript.id, Transcript.transcript_text)
                    ),
                )
                .where(Episode.keywords.any())
            )
            .scalars()
            .unique()
            .all()
        )

        call_to_action_patterns = [
            "sign up",
            "sign-up",
            "join us",
            "join our",
            "subscribe",
            "share your",
            "submit your",
            "submit comments",
            "volunteer",
            "register",
            "donate",
            "support us",
            "support our",
            "apply now",
            "take action",
            "call to action",
            "reach out",
            "get involved",
        ]
        call_to_action_patterns = [pattern.lower() for pattern in call_to_action_patterns]

        stakeholder_stopwords = {
            "Thank You",
            "Welcome Back",
            "Open Source",
            "Digital Democracy",
            "Public Policy",
            "City Hall",
            "United States",
            "European Commission",
            "Local Government",
            "Public Sector",
            "Civic Tech",
        }

        if not episodes:
            empty = {"nodes": [], "links": []}
            summary = {
                "keyword_count": 0,
                "connection_count": 0,
                "episode_coverage": 0,
                "podcast_coverage": 0,
                "top_keywords": [],
            }
            return empty, summary

        keyword_totals: Counter[str] = Counter()
        keyword_episode_map: Dict[str, set[int]] = defaultdict(set)
        keyword_podcast_map: Dict[str, set[int]] = defaultdict(set)
        keyword_podcast_weights: Dict[str, Dict[str, float]] = defaultdict(dict)
        episode_keyword_weights: Dict[int, List[Tuple[str, float]]] = {}
        episode_lookup: Dict[int, Dict[str, Any]] = {}

        for episode in episodes:
            if not episode.keywords:
                continue

            transcript_text = ""
            if episode.transcript and episode.transcript.transcript_text:
                transcript_text = episode.transcript.transcript_text
            text_lower = transcript_text.lower()
            call_to_action = any(pattern in text_lower for pattern in call_to_action_patterns)

            stakeholders: Set[str] = set()
            if transcript_text:
                for match in re.finditer(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b", transcript_text):
                    phrase = match.group(0).strip()
                    if phrase in stakeholder_stopwords:
                        continue
                    if len(phrase.split()) > 5:
                        continue
                    stakeholders.add(phrase)

            ordered = sorted(episode.keywords, key=lambda kw: kw.weight, reverse=True)
            trimmed = ordered[:top_keywords_per_episode]
            if not trimmed:
                continue
            episode_lookup[episode.id] = {
                "id": episode.id,
                "title": episode.title,
                "podcast": episode.podcast.name if episode.podcast else "Unknown",
                "podcast_id": episode.podcast.id if episode.podcast else None,
                "published": episode.published_at.isoformat() if episode.published_at else None,
                "call_to_action": call_to_action,
                "stakeholders": sorted(stakeholders)[:6],
            }
            weights = [(kw.keyword, float(kw.weight)) for kw in trimmed if kw.keyword]
            if not weights:
                continue
            episode_keyword_weights[episode.id] = weights
            for keyword, weight in weights:
                keyword_totals[keyword] += weight
                keyword_episode_map[keyword].add(episode.id)
                if episode.podcast:
                    keyword_podcast_map[keyword].add(episode.podcast.id)
                    keyword_podcast_weights[keyword][episode.podcast.name] = (
                        keyword_podcast_weights[keyword].get(episode.podcast.name, 0.0) + weight
                    )

        if not keyword_totals:
            empty = {"nodes": [], "links": []}
            summary = {
                "keyword_count": 0,
                "connection_count": 0,
                "episode_coverage": 0,
                "podcast_coverage": 0,
                "top_keywords": [],
            }
            return empty, summary

        top_keywords = [keyword for keyword, _ in keyword_totals.most_common(max_keywords)]
        keyword_set = set(top_keywords)

        nodes: List[Dict[str, Any]] = []
        top_keyword_cards: List[Dict[str, Any]] = []
        for keyword in top_keywords:
            total_weight = keyword_totals[keyword]
            episodes_for_keyword = keyword_episode_map[keyword]
            podcasts_for_keyword = keyword_podcast_map.get(keyword, set())
            keyword_podcast_distribution = keyword_podcast_weights.get(keyword, {})
            top_podcasts = sorted(
                keyword_podcast_distribution.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:4]
            nodes.append(
                {
                    "id": keyword,
                    "label": keyword,
                    "value": round(total_weight, 4),
                    "episode_count": len(episodes_for_keyword),
                    "podcast_count": len(podcasts_for_keyword),
                    "top_podcasts": [
                        {"name": name, "weight": round(score, 3)} for name, score in top_podcasts
                    ],
                }
            )
            top_keyword_cards.append(
                {
                    "keyword": keyword,
                    "score": round(total_weight, 2),
                    "episodes": len(episodes_for_keyword),
                    "podcasts": len(podcasts_for_keyword),
                }
            )

        edge_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for episode_id, keyword_pairs in episode_keyword_weights.items():
            filtered = [(kw, weight) for kw, weight in keyword_pairs if kw in keyword_set]
            if len(filtered) < 2:
                continue
            for (left_kw, left_weight), (right_kw, right_weight) in combinations(filtered, 2):
                if left_kw == right_kw:
                    continue
                source, target = sorted((left_kw, right_kw))
                key = (source, target)
                if key not in edge_map:
                    edge_map[key] = {
                        "weight": 0.0,
                        "episodes": set(),
                        "podcast_weights": {},
                        "keywords": {source, target},
                    }
                combined = (left_weight + right_weight) / 2
                edge_map[key]["weight"] += combined
                edge_map[key]["episodes"].add(episode_id)
                podcast_name = episode_lookup.get(episode_id, {}).get("podcast")
                if podcast_name:
                    podcast_weights: Dict[str, float] = edge_map[key]["podcast_weights"]  # type: ignore[assignment]
                    podcast_weights[podcast_name] = podcast_weights.get(podcast_name, 0.0) + combined

        links: List[Dict[str, Any]] = []
        connection_summaries: List[Dict[str, Any]] = []
        podcast_bridge_weights: Dict[str, float] = defaultdict(float)

        heatmap_keywords = [item["keyword"] for item in top_keyword_cards[:10]]
        podcast_heatmap_totals: Dict[str, float] = defaultdict(float)

        for (source, target), payload in edge_map.items():
            weight = float(payload["weight"])
            episode_ids = list(payload["episodes"])
            podcast_weights: Dict[str, float] = payload["podcast_weights"]  # type: ignore[assignment]
            top_edge_podcasts = sorted(podcast_weights.items(), key=lambda item: item[1], reverse=True)[:3]
            ordered_episode_ids = sorted(
                episode_ids,
                key=lambda eid: episode_lookup.get(eid, {}).get("published") or "",
                reverse=True,
            )
            sample_episodes = [
                episode_lookup[ep_id]
                for ep_id in ordered_episode_ids
                if ep_id in episode_lookup
            ][:4]

            keywords_pair = sorted(payload.get("keywords", {source, target}))
            links.append(
                {
                    "source": source,
                    "target": target,
                    "weight": round(weight, 4),
                    "episode_count": len(episode_ids),
                    "podcast_count": len(podcast_weights),
                    "keywords": keywords_pair,
                    "top_podcasts": [
                        {"name": name, "weight": round(score, 3)} for name, score in top_edge_podcasts
                    ],
                    "sample_episodes": sample_episodes,
                }
            )

            connection_summaries.append(
                {
                    "source": source,
                    "target": target,
                    "episode_count": len(episode_ids),
                    "weight": round(weight, 3),
                    "keywords": keywords_pair,
                    "samples": sample_episodes[:2],
                    "top_podcasts": [
                        {"name": name, "weight": round(score, 3)} for name, score in top_edge_podcasts
                    ],
                    "episode_ids": episode_ids,
                }
            )

            for name, score in podcast_weights.items():
                podcast_bridge_weights[name] += score

        for keyword in heatmap_keywords:
            keyword_distribution = keyword_podcast_weights.get(keyword, {}) or {}
            for name, weight in keyword_distribution.items():
                podcast_heatmap_totals[name] += weight

        heatmap_podcasts = [
            name
            for name, _ in sorted(podcast_heatmap_totals.items(), key=lambda item: item[1], reverse=True)[:10]
        ]

        heatmap_matrix: List[List[float]] = []
        max_heatmap_value = 0.0
        for keyword in heatmap_keywords:
            distribution = keyword_podcast_weights.get(keyword, {}) or {}
            row: List[float] = []
            for podcast_name in heatmap_podcasts:
                value = float(distribution.get(podcast_name, 0.0))
                max_heatmap_value = max(max_heatmap_value, value)
                row.append(round(value, 4))
            heatmap_matrix.append(row)

        pca_points: List[Dict[str, Any]] = []
        if (
            np is not None
            and len(heatmap_podcasts) >= 2
            and heatmap_keywords
        ):
            try:
                matrix = np.array(
                    [
                        [keyword_podcast_weights.get(keyword, {}).get(podcast, 0.0) for keyword in heatmap_keywords]
                        for podcast in heatmap_podcasts
                    ],
                    dtype=float,
                )
                if matrix.size and np.any(matrix):
                    matrix -= matrix.mean(axis=0, keepdims=True)
                    u, s, _ = np.linalg.svd(matrix, full_matrices=False)
                    coords = u[:, :2] * s[:2]
                    if coords.shape[1] == 1:
                        coords = np.hstack([coords, np.zeros((coords.shape[0], 1))])
                    for idx, podcast_name in enumerate(heatmap_podcasts):
                        top_kw = sorted(
                            (
                                (keyword, keyword_podcast_weights.get(keyword, {}).get(podcast_name, 0.0))
                                for keyword in heatmap_keywords
                            ),
                            key=lambda item: item[1],
                            reverse=True,
                        )[:4]
                        pca_points.append(
                            {
                                "podcast": podcast_name,
                                "x": round(float(coords[idx, 0]), 4),
                                "y": round(float(coords[idx, 1]), 4),
                                "keywords": [
                                    {"keyword": kw, "weight": round(val, 3)}
                                    for kw, val in top_kw if val > 0
                                ],
                            }
                        )
            except Exception:  # pragma: no cover
                pca_points = []

        collaboration_suggestions: List[Dict[str, Any]] = []
        for entry in connection_summaries:
            episode_ids = entry.get("episode_ids", [])
            if not episode_ids:
                continue
            action_count = 0
            stakeholders_accum: Set[str] = set()
            for episode_id in episode_ids:
                episode_info = episode_lookup.get(episode_id)
                if not episode_info:
                    continue
                if episode_info.get("call_to_action"):
                    action_count += 1
                stakeholders_accum.update(episode_info.get("stakeholders", []))

            if action_count == 0 and not stakeholders_accum:
                continue

            suggestion_score = (
                action_count * 2.0
                + len(stakeholders_accum) * 0.5
                + entry["episode_count"] * 0.1
            )

            collaboration_suggestions.append(
                {
                    "source": entry["source"],
                    "target": entry["target"],
                    "score": round(suggestion_score, 3),
                    "call_to_action_episodes": action_count,
                    "stakeholders": sorted(stakeholders_accum)[:8],
                    "shared_keywords": entry.get("keywords", [])[:4],
                    "sample_episodes": entry.get("samples", []),
                    "top_podcasts": entry.get("top_podcasts", [])[:3],
                }
            )

        collaboration_suggestions.sort(
            key=lambda item: (
                item["call_to_action_episodes"],
                len(item["stakeholders"]),
                item["score"],
            ),
            reverse=True,
        )

        links.sort(key=lambda item: item["weight"], reverse=True)
        if max_edges and len(links) > max_edges:
            links = links[:max_edges]

        covered_episode_ids: set[int] = set()
        covered_podcast_ids: set[int] = set()
        for keyword in keyword_set:
            covered_episode_ids.update(keyword_episode_map.get(keyword, set()))
            covered_podcast_ids.update(keyword_podcast_map.get(keyword, set()))

        graph = {"nodes": nodes, "links": links}
        summary = {
            "keyword_count": len(nodes),
            "connection_count": len(links),
            "episode_coverage": len(covered_episode_ids),
            "podcast_coverage": len(covered_podcast_ids),
            "top_keywords": top_keyword_cards[:8],
            "top_connections": [
                {
                    "source": entry["source"],
                    "target": entry["target"],
                    "episode_count": entry["episode_count"],
                    "weight": entry["weight"],
                    "keywords": entry.get("keywords", []),
                }
                for entry in sorted(
                    connection_summaries,
                    key=lambda item: (item["episode_count"], item["weight"]),
                    reverse=True,
                )[:8]
            ],
            "bridge_podcasts": [
                {"podcast": name, "score": round(score, 3)}
                for name, score in sorted(podcast_bridge_weights.items(), key=lambda item: item[1], reverse=True)[:8]
            ],
            "keyword_heatmap": {
                "keywords": heatmap_keywords,
                "podcasts": heatmap_podcasts,
                "matrix": heatmap_matrix,
                "max": max_heatmap_value,
            },
            "pca_scatter": pca_points,
            "bridge_leaderboard": [
                {
                    "source": item["source"],
                    "target": item["target"],
                    "keywords": ", ".join(item.get("keywords", [])[:4]),
                    "top_podcasts": item.get("top_podcasts", [])[:3],
                    "episodes": item["episode_count"],
                    "score": item["weight"],
                    "samples": item.get("samples", []),
                }
                for item in sorted(
                    connection_summaries,
                    key=lambda entry: (entry["episode_count"], entry["weight"]),
                    reverse=True,
                )[:12]
            ],
            "collaboration_suggestions": collaboration_suggestions[:10],
        }
        return graph, summary

    def load_topic_network(session: Session, force_refresh: bool = False) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        cache_path = topic_cache_path
        if not force_refresh and cache_path.exists():
            try:
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
                graph_cached = payload.get("graph")
                summary_cached = payload.get("summary")
                if graph_cached and summary_cached:
                    return graph_cached, summary_cached
            except Exception:  # pragma: no cover
                app.logger.warning("Failed to read topic network cache", exc_info=True)

        graph, summary = build_topic_network(session, max_keywords=45, top_keywords_per_episode=6, max_edges=400)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_payload = {
                "generated_at": dt.datetime.utcnow().isoformat(),
                "graph": graph,
                "summary": summary,
            }
            cache_path.write_text(json.dumps(cache_payload, ensure_ascii=False), encoding="utf-8")
        except Exception:  # pragma: no cover
            app.logger.warning("Failed to write topic network cache", exc_info=True)
        return graph, summary

    @app.route("/")
    def index():
        with get_session() as session:
            episode_loader = selectinload(Podcast.episodes).options(
                load_only(
                    Episode.id,
                    Episode.title,
                    Episode.published_at,
                    Episode.duration,
                    Episode.transcribed_at,
                    Episode.podcast_id,
                ),
                selectinload(Episode.transcript).options(
                    load_only(Transcript.id, Transcript.confidence, Transcript.seconds),
                    defer(Transcript.transcript_text),
                    defer(Transcript.raw_response),
                    defer(Transcript.diarization),
                ),
                selectinload(Episode.keywords).options(
                    load_only(EpisodeKeyword.id, EpisodeKeyword.keyword, EpisodeKeyword.weight)
                ),
            )

            podcasts = (
                session.execute(
                    select(Podcast)
                    .options(
                        load_only(
                            Podcast.id,
                            Podcast.name,
                            Podcast.description,
                            Podcast.category,
                            Podcast.language,
                            Podcast.website,
                        ),
                        episode_loader,
                    )
                )
                .scalars()
                .unique()
                .all()
            )
            recent_episodes = (
                session.execute(
                    select(Episode)
                    .join(Transcript)
                    .options(
                        load_only(
                            Episode.id,
                            Episode.title,
                            Episode.published_at,
                            Episode.podcast_id,
                            Episode.transcribed_at,
                        ),
                        selectinload(Episode.podcast).options(
                            load_only(Podcast.id, Podcast.name)
                        ),
                        selectinload(Episode.transcript).options(
                            load_only(Transcript.id, Transcript.confidence),
                            defer(Transcript.transcript_text),
                            defer(Transcript.raw_response),
                            defer(Transcript.diarization),
                        ),
                        selectinload(Episode.keywords).options(
                            load_only(EpisodeKeyword.id, EpisodeKeyword.keyword, EpisodeKeyword.weight)
                        ),
                    )
                    .where(Episode.transcribed_at.isnot(None))
                    .order_by(
                        Episode.transcribed_at.desc().nullslast(),
                        Transcript.metadata_created.desc().nullslast(),
                        Episode.published_at.desc().nullslast(),
                    )
                    .limit(12)
                )
                .scalars()
                .unique()
                .all()
            )
            episode_count = sum(len(podcast.episodes) for podcast in podcasts)
            transcript_count = sum(
                1 for podcast in podcasts for episode in podcast.episodes if episode.transcript
            )

            graph, summary = load_topic_network(session)
            summary_top_keywords = [
                (item["keyword"], item.get("score", 0.0))
                for item in summary.get("top_keywords", [])[:12]
            ]

            return render_template(
                "index.html",
                podcasts=podcasts,
                recent_episodes=recent_episodes,
                stats={
                    "podcast_count": len(podcasts),
                    "episode_count": episode_count,
                    "transcript_count": transcript_count,
                    "top_keywords": summary_top_keywords,
                },
                graph=graph,
            )

    @app.route("/graph")
    def topic_graph():
        with get_session() as session:
            graph, summary = load_topic_network(session)
            return render_template("graph.html", graph=graph, summary=summary)

    @app.route("/graph/recompute", methods=["POST"])
    def graph_recompute():
        pipeline = RssTranscriptionPipeline(
            feeds=[],
            database_url=f"sqlite:///{db_path}",
            deepgram_api_key=os.getenv("DEEPGRAM_API_KEY", ""),
            output_dir=DEFAULT_OUTPUT_DIR,
        )
        try:
            pipeline._recompute_keywords()
            pipeline._write_topic_network_cache()
        except Exception as exc:  # pragma: no cover - defensive logging
            app.logger.exception("Keyword recompute failed via graph endpoint: %s", exc)
            return jsonify({"error": str(exc)}), 500

        with get_session() as session:
            graph, summary = load_topic_network(session)
            return jsonify({"status": "ok", "graph": graph, "summary": summary})

    @app.route("/podcasts")
    def podcast_list():
        with get_session() as session:
            episode_loader = selectinload(Podcast.episodes).options(
                load_only(
                    Episode.id,
                    Episode.title,
                    Episode.published_at,
                    Episode.duration,
                    Episode.podcast_id,
                ),
                selectinload(Episode.transcript).options(
                    load_only(Transcript.id, Transcript.confidence, Transcript.seconds),
                    defer(Transcript.transcript_text),
                    defer(Transcript.raw_response),
                    defer(Transcript.diarization),
                ),
                selectinload(Episode.keywords).options(
                    load_only(EpisodeKeyword.id, EpisodeKeyword.keyword, EpisodeKeyword.weight)
                ),
            )

            podcasts = (
                session.execute(
                    select(Podcast)
                    .options(
                        load_only(
                            Podcast.id,
                            Podcast.name,
                            Podcast.description,
                            Podcast.category,
                            Podcast.language,
                            Podcast.website,
                        ),
                        episode_loader,
                    )
                    .order_by(Podcast.name.asc())
                )
                .scalars()
                .unique()
                .all()
            )
            podcast_payload = []
            total_transcribed = 0
            for podcast in podcasts:
                ordered_episodes = sorted(
                    podcast.episodes,
                    key=lambda ep: ep.published_at or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
                    reverse=True,
                )
                transcribed_count = sum(1 for episode in ordered_episodes if episode.transcript)
                total_transcribed += transcribed_count
                podcast_payload.append(
                    {
                        "podcast": podcast,
                        "episodes": ordered_episodes,
                        "transcribed_count": transcribed_count,
                    }
                )
            return render_template(
                "podcasts.html",
                podcasts=podcast_payload,
                stats={
                    "podcast_count": len(podcasts),
                    "episode_count": sum(len(podcast.episodes) for podcast in podcasts),
                    "transcribed_count": total_transcribed,
                },
            )

    @app.route("/transcripts")
    def transcript_list():
        with get_session() as session:
            page = max(request.args.get("page", 1, type=int), 1)
            per_page = 50
            total = session.execute(
                select(func.count()).select_from(Episode).where(Episode.transcript != None)  # noqa: E711
            ).scalar() or 0
            total_pages = max((total + per_page - 1) // per_page, 1)
            if page > total_pages:
                page = total_pages

            episodes = (
                session.execute(
                    select(Episode)
                    .where(Episode.transcript != None)  # noqa: E711
                    .options(
                        load_only(
                            Episode.id,
                            Episode.title,
                            Episode.published_at,
                            Episode.duration,
                            Episode.podcast_id,
                        ),
                        selectinload(Episode.podcast).options(
                            load_only(Podcast.id, Podcast.name)
                        ),
                        selectinload(Episode.transcript).options(
                            load_only(Transcript.id, Transcript.confidence, Transcript.seconds),
                            defer(Transcript.transcript_text),
                            defer(Transcript.raw_response),
                            defer(Transcript.diarization),
                        ),
                        selectinload(Episode.keywords).options(
                            load_only(EpisodeKeyword.id, EpisodeKeyword.keyword, EpisodeKeyword.weight)
                        ),
                    )
                    .order_by(Episode.published_at.desc().nullslast())
                    .offset((page - 1) * per_page)
                    .limit(per_page)
                )
                .scalars()
                .all()
            )
            return render_template(
                "transcripts.html",
                episodes=episodes,
                page=page,
                total_pages=total_pages,
            )

    @app.route("/podcasts/<podcast_slug>")
    def podcast_detail(podcast_slug: str):
        with get_session() as session:
            candidates = session.execute(select(Podcast.id, Podcast.name)).all()
            target = next((row for row in candidates if slugify(row.name) == podcast_slug), None)
            if target is None:
                return render_template("404.html"), 404

            podcast = (
                session.execute(
                    select(Podcast)
                    .options(
                        load_only(
                            Podcast.id,
                            Podcast.name,
                            Podcast.description,
                            Podcast.category,
                            Podcast.language,
                            Podcast.website,
                        )
                    )
                    .where(Podcast.id == target.id)
                )
                .scalars()
                .first()
            )
            if podcast is None:
                return render_template("404.html"), 404

            page = max(request.args.get("page", 1, type=int), 1)
            per_page = 40

            total = (
                session.execute(
                    select(func.count())
                    .select_from(Episode)
                    .where(Episode.podcast_id == podcast.id)
                ).scalar()
                or 0
            )
            total_pages = max((total + per_page - 1) // per_page, 1)
            if page > total_pages:
                page = total_pages

            episodes = (
                session.execute(
                    select(Episode)
                    .where(Episode.podcast_id == podcast.id)
                    .options(
                        load_only(
                            Episode.id,
                            Episode.title,
                            Episode.published_at,
                            Episode.duration,
                            Episode.podcast_id,
                        ),
                        selectinload(Episode.transcript).options(
                            load_only(Transcript.id, Transcript.confidence, Transcript.seconds),
                            defer(Transcript.transcript_text),
                            defer(Transcript.raw_response),
                            defer(Transcript.diarization),
                        ),
                        selectinload(Episode.keywords).options(
                            load_only(EpisodeKeyword.id, EpisodeKeyword.keyword, EpisodeKeyword.weight)
                        ),
                    )
                    .order_by(Episode.published_at.desc().nullslast())
                    .offset((page - 1) * per_page)
                    .limit(per_page)
                )
                .scalars()
                .all()
            )

            transcribed_count = (
                session.execute(
                    select(func.count())
                    .select_from(Episode)
                    .where(Episode.podcast_id == podcast.id)
                    .where(Episode.transcript != None)  # noqa: E711
                ).scalar()
                or 0
            )

            return render_template(
                "podcast_detail.html",
                podcast=podcast,
                episodes=episodes,
                stats={
                    "episode_count": total,
                    "transcribed_count": transcribed_count,
                },
                page=page,
                total_pages=total_pages,
            )

    @app.route("/feeds", methods=["GET", "POST"])
    @require_feed_auth
    def manage_feeds():
        errors: list[str] = []
        success: Optional[str] = None
        import_stats: Optional[dict] = None

        with get_session() as session:
            if request.method == "POST":
                form_type = request.form.get("form_type", "")
                if form_type == "single":
                    rss_url = (request.form.get("rss_url") or "").strip()
                    if not rss_url:
                        errors.append("RSS URL is required.")
                    else:
                        name = (request.form.get("name") or "").strip()
                        category = (request.form.get("category") or "").strip() or None
                        language = (request.form.get("language") or "").strip() or None

                        feed_data = None
                        if not errors:
                            try:
                                feed_data = fetch_feed(rss_url)
                            except Exception as exc:
                                errors.append(f"Unable to fetch feed metadata: {exc}")

                        if feed_data and not name and not errors:
                            channel = getattr(feed_data, "feed", {}) or {}
                            name = channel.get("title", "")
                            language = language or channel.get("language") or channel.get("dc_language")
                            raw_categories = channel.get("categories") or channel.get("tags")
                            if not category and raw_categories:
                                if isinstance(raw_categories, list):
                                    first = raw_categories[0]
                                    if isinstance(first, dict):
                                        category = first.get("term") or first.get("label")
                                    else:
                                        category = str(first)
                                else:
                                    category = str(raw_categories)

                        if feed_data and not errors:
                            entries = getattr(feed_data, "entries", []) or []
                            if not entries:
                                errors.append("Feed has no entries. Please verify the RSS URL.")

                        if not name and not errors:
                            parsed = urlparse(rss_url)
                            host = (parsed.hostname or "rss.feed").replace("www.", "")
                            base = host.split(".", 1)[0].replace("-", " ").title()
                            name = f"{base} Podcast"

                        if not errors:
                            feed = FeedSubscription(
                                name=name,
                                rss_url=rss_url,
                                category=category,
                                language=language,
                            )
                            session.add(feed)
                            try:
                                session.commit()
                            except IntegrityError:
                                session.rollback()
                                errors.append("A feed with that RSS URL already exists.")
                            else:
                                success = f"Added feed '{name}'."
                elif form_type == "csv":
                    file = request.files.get("csv_file")
                    if not file or not file.filename:
                        errors.append("Select a CSV file to upload.")
                    else:
                        try:
                            content = file.stream.read().decode("utf-8-sig")
                        except UnicodeDecodeError:
                            errors.append("Unable to decode CSV. Ensure the file is UTF-8 encoded.")
                        else:
                            reader = csv.DictReader(io.StringIO(content))
                            if not reader.fieldnames:
                                errors.append("CSV header row is missing.")
                            else:
                                normalized = {(name or "").strip().lower() for name in reader.fieldnames}
                                required = {"podcast_name", "rss_url"}
                                missing = required - normalized
                                if missing:
                                    errors.append(
                                        f"CSV missing required columns: {', '.join(sorted(missing))}."
                                    )
                                else:
                                    added = 0
                                    skipped = 0
                                    for row in reader:
                                        normalized_row = {
                                            (key or "").strip().lower(): value
                                            for key, value in (row.items() if row else [])
                                        }
                                        name = (normalized_row.get("podcast_name") or "").strip()
                                        rss_url = (normalized_row.get("rss_url") or "").strip()
                                        category = (normalized_row.get("category") or "").strip() or None
                                        language = (normalized_row.get("language") or "").strip() or None

                                        if not name or not rss_url:
                                            skipped += 1
                                            continue

                                        feed = FeedSubscription(
                                            name=name,
                                            rss_url=rss_url,
                                            category=category,
                                            language=language,
                                        )
                                        session.add(feed)
                                        try:
                                            session.commit()
                                        except IntegrityError:
                                            session.rollback()
                                            skipped += 1
                                        else:
                                            added += 1
                                    if added:
                                        success = f"Imported {added} feed(s)."
                                        if skipped:
                                            import_stats = {"skipped": skipped}
                                    elif skipped:
                                        import_stats = {"skipped": skipped}
                                        errors.append(
                                            "No new feeds imported. All rows were duplicates or invalid."
                                        )
                                    else:
                                        errors.append("No valid feed rows were found in the CSV.")
                
                elif form_type == "update":
                    feed_id = request.form.get("feed_id")
                    if not feed_id:
                        errors.append("Missing feed identifier.")
                    else:
                        feed = session.get(FeedSubscription, int(feed_id))
                        if not feed:
                            errors.append("Feed not found.")
                        else:
                            new_name = (request.form.get("name") or "").strip() or feed.name
                            new_url = (request.form.get("rss_url") or "").strip()
                            new_category = (request.form.get("category") or "").strip() or None
                            new_language = (request.form.get("language") or "").strip() or None

                            if not new_url:
                                errors.append("RSS URL cannot be empty.")
                            else:
                                feed.name = new_name
                                feed.rss_url = new_url
                                feed.category = new_category
                                feed.language = new_language
                                try:
                                    session.commit()
                                except IntegrityError:
                                    session.rollback()
                                    errors.append("Another feed already uses that RSS URL.")
                                else:
                                    success = f"Updated feed '{feed.name}'."
                elif form_type == "delete":
                    feed_id = request.form.get("feed_id")
                    if not feed_id:
                        errors.append("Missing feed identifier.")
                    else:
                        feed = session.get(FeedSubscription, int(feed_id))
                        if not feed:
                            errors.append("Feed not found.")
                        else:
                            feed_name = feed.name
                            podcast_deleted = False
                            podcast_name = None
                            removed_episode_count = 0
                            removed_transcript_count = 0

                            podcast_match = session.execute(
                                select(Podcast).where(func.lower(Podcast.rss_url) == func.lower(feed.rss_url))
                            ).scalar_one_or_none()
                            if podcast_match:
                                podcast_name = podcast_match.name
                                removed_episode_count = (
                                    session.execute(
                                        select(func.count(Episode.id)).where(Episode.podcast_id == podcast_match.id)
                                    ).scalar()
                                    or 0
                                )
                                removed_transcript_count = (
                                    session.execute(
                                        select(func.count(Transcript.id))
                                        .join(Episode, Transcript.episode_id == Episode.id)
                                        .where(Episode.podcast_id == podcast_match.id)
                                    ).scalar()
                                    or 0
                                )
                                session.delete(podcast_match)
                                podcast_deleted = True

                            session.delete(feed)
                            session.commit()
                            if topic_cache_path.exists():
                                try:
                                    topic_cache_path.unlink()
                                except OSError:
                                    app.logger.debug("Failed to remove cached topic network during feed deletion.", exc_info=True)

                            if podcast_deleted and podcast_name:
                                success = (
                                    f"Removed feed '{feed_name}' and deleted podcast '{podcast_name}' "
                                    f"({removed_episode_count} episode(s), {removed_transcript_count} transcript(s))."
                                )
                            else:
                                success = f"Removed feed '{feed_name}'."

            feeds = (
                session.execute(
                    select(FeedSubscription).order_by(FeedSubscription.created_at.desc())
                )
                .scalars()
                .all()
            )

        with job_lock:
            current_active_job_id = active_pipeline_job_id

        return render_template(
            "feeds.html",
            feeds=feeds,
            errors=errors,
            success=success,
            import_stats=import_stats,
            pipeline_defaults=pipeline_defaults,
            active_pipeline_job_id=current_active_job_id,
        )
    
    @app.route("/feeds/preview", methods=["POST"])
    @require_feed_auth
    def feed_preview():
        payload = request.get_json(silent=True) or {}
        url = (payload.get("rss_url") or "").strip()
        if not url:
            return jsonify({"error": "RSS URL is required."}), 400
        try:
            feed_data = fetch_feed(url)
        except Exception as exc:
            return jsonify({"error": f"Failed to fetch feed: {exc}"}), 502

        channel = getattr(feed_data, "feed", {}) or {}
        title = channel.get("title")
        language = channel.get("language") or channel.get("dc_language")

        category = None
        raw_categories = channel.get("categories") or channel.get("tags")
        if raw_categories:
            if isinstance(raw_categories, list):
                first = raw_categories[0]
                if isinstance(first, dict):
                    category = first.get("term") or first.get("label")
                else:
                    category = str(first)
            else:
                category = str(raw_categories)

        return jsonify(
            {
                "title": title,
                "language": language,
                "category": category,
            }
        )

    @app.route("/podcasts/<podcast_slug>/<int:episode_id>")
    def episode_detail(podcast_slug: str, episode_id: int):
        with get_session() as session:
            episode = session.get(Episode, episode_id)
            if episode is None:
                return render_template("404.html"), 404

            expected_slug = slugify(episode.podcast.name)
            if podcast_slug != expected_slug:
                return redirect(
                    url_for("episode_detail", podcast_slug=expected_slug, episode_id=episode_id),
                    code=301,
                )

            keywords = (
                session.query(EpisodeKeyword)
                .filter(EpisodeKeyword.episode_id == episode.id)
                .order_by(EpisodeKeyword.weight.desc())
                .all()
            )
            transcript = session.get(Transcript, episode.transcript.id) if episode.transcript else None
            chat_segments: List[Dict[str, Any]] = []
            speaker_index_map: Dict[str, int] = {}
            if transcript and transcript.diarization:
                def _coerce_float(value: Any) -> Optional[float]:
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        return None

                for raw_segment in transcript.diarization:
                    if isinstance(raw_segment, dict):
                        speaker_value = raw_segment.get("speaker", 0)
                        start = _coerce_float(raw_segment.get("start"))
                        end = _coerce_float(raw_segment.get("end"))
                        text = (raw_segment.get("transcript") or "").strip()
                    else:
                        speaker_value = getattr(raw_segment, "speaker", 0)
                        start = _coerce_float(getattr(raw_segment, "start", None))
                        end = _coerce_float(getattr(raw_segment, "end", None))
                        text = (getattr(raw_segment, "transcript", "") or "").strip()
                    if not text:
                        continue
                    speaker_label = str(speaker_value).strip() or "Speaker"
                    speaker_key = speaker_label.lower()
                    if speaker_key not in speaker_index_map:
                        speaker_index_map[speaker_key] = len(speaker_index_map)
                    speaker_index = speaker_index_map[speaker_key]

                    if speaker_label.isdigit():
                        speaker_display = f"Speaker {int(speaker_label)}"
                    elif speaker_label.lower().startswith("speaker"):
                        speaker_display = speaker_label
                    else:
                        speaker_display = speaker_label or f"Speaker {speaker_index + 1}"

                    if chat_segments and chat_segments[-1]["speaker_index"] == speaker_index:
                        existing = chat_segments[-1]
                        if start is not None:
                            if existing["start"] is None:
                                existing["start"] = start
                            else:
                                existing["start"] = min(existing["start"], start)
                        if end is not None:
                            if existing["end"] is None:
                                existing["end"] = end
                            else:
                                existing["end"] = max(existing["end"], end)
                        existing["texts"].append(text)
                    else:
                        chat_segments.append(
                            {
                                "speaker_label": speaker_label,
                                "speaker_display": speaker_display,
                                "speaker_index": speaker_index,
                                "start": start,
                                "end": end,
                                "texts": [text],
                            }
                        )
                for segment in chat_segments:
                    segment["text"] = " ".join(segment.pop("texts"))

            return render_template(
                "episode.html",
                episode=episode,
                keywords=keywords,
                transcript=transcript,
                chat_segments=chat_segments,
            )

    @app.route("/episodes/<int:episode_id>")
    def episode_detail_legacy(episode_id: int):
        with get_session() as session:
            episode = session.get(Episode, episode_id)
            if episode is None:
                return render_template("404.html"), 404
            return redirect(
                url_for("episode_detail", podcast_slug=slugify(episode.podcast.name), episode_id=episode_id),
                code=301,
            )

    @app.route("/episodes/<int:episode_id>/export.json")
    def export_transcript(episode_id: int) -> Response:
        with get_session() as session:
            episode = session.get(Episode, episode_id)
            if episode is None or episode.transcript is None:
                return Response(status=404)
            title = episode.title
            raw_payload = episode.transcript.raw_response
            if not raw_payload:
                return Response(status=404)
            if isinstance(raw_payload, str):
                try:
                    raw_payload = json.loads(raw_payload)
                except json.JSONDecodeError:
                    return Response(status=500)

        default_slug = f"episode_{episode_id}"
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", title or default_slug).strip("_") or default_slug
        filename = f"{slug}_raw.json"

        body = json.dumps(raw_payload, ensure_ascii=False, indent=2)
        response = Response(body, mimetype="application/json")
        response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return response

    @app.route("/api/graph")
    def episode_graph():
        with get_session() as session:
            graph = build_episode_graph(session, limit=200)
            return jsonify(graph)

    return app


if __name__ == "__main__":
    flask_app = create_app()
    flask_app.run(host="0.0.0.0", port=8000, debug=True)
