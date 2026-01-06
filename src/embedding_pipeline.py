from __future__ import annotations

import argparse
import hashlib
import datetime as dt
import os
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Callable, Any

import requests
import chromadb

from rss_transcriber import (
    Base,
    Episode,
    Podcast,
    Transcript,
    EmbeddingStatus,
)
from sqlalchemy import create_engine, select, or_
from sqlalchemy.orm import Session


DEFAULT_MODEL = "bge-m3"
DEFAULT_CHUNK_SIZE = 450
DEFAULT_CHUNK_OVERLAP = 75
DEFAULT_COLLECTION = "transcripts"
DEFAULT_CHROMA_DIR = Path("data/chroma")
DEFAULT_DB_PATH = Path("data/podcasts.sqlite")
DEFAULT_DB_URL = f"sqlite:///{DEFAULT_DB_PATH}"


def chunk_words(text: str, chunk_size: int, overlap: int) -> Iterator[Tuple[str, int, int]]:
    words = text.split()
    if not words:
        return

    step = max(chunk_size - overlap, 1)
    for start in range(0, len(words), step):
        end = min(start + chunk_size, len(words))
        chunk = " ".join(words[start:end]).strip()
        if chunk:
            yield chunk, start, end
        if end == len(words):
            break


def deterministic_chunk_id(source_id: str, chunk_index: int, chunk_text: str) -> str:
    payload = f"{source_id}:{chunk_index}:{chunk_text}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def get_ollama_embedding(text: str, model: str, base_url: str) -> List[float]:
    response = requests.post(
        f"{base_url}/api/embeddings",
        json={"model": model, "prompt": text},
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    embedding = payload.get("embedding")
    if not isinstance(embedding, list):
        raise ValueError("Ollama did not return embeddings.")
    return embedding


def iter_db_transcripts(session: Session, podcast_name: Optional[str] = None) -> Iterator[Tuple[Episode, Podcast, Transcript, Optional[EmbeddingStatus]]]:
    stmt = (
        select(Episode, Podcast, Transcript, EmbeddingStatus)
        .join(Podcast, Podcast.id == Episode.podcast_id)
        .join(Transcript, Transcript.episode_id == Episode.id)
        .outerjoin(EmbeddingStatus, EmbeddingStatus.episode_id == Episode.id)
    )
    if podcast_name:
        stmt = stmt.where(Podcast.name == podcast_name)

    pending_statuses = or_(
        EmbeddingStatus.episode_id.is_(None),
        EmbeddingStatus.status.in_(["pending", "failed", "running"]),
    )
    stmt = stmt.where(pending_statuses).order_by(Episode.published_at.desc().nullslast())

    for episode, podcast, transcript, status in session.execute(stmt).all():
        if transcript and transcript.transcript_text:
            yield episode, podcast, transcript, status


def embed_transcripts(
    database_url: str,
    chroma_dir: Path,
    collection_name: str,
    model: str,
    chunk_size: int,
    overlap: int,
    base_url: str,
    podcast_name: Optional[str] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    flush_every: int = 50,
) -> Dict[str, int]:
    if overlap >= chunk_size:
        overlap = max(chunk_size - 1, 0)
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    client = chromadb.PersistentClient(path=str(chroma_dir))
    collection = client.get_or_create_collection(collection_name)

    totals = {
        "episodes_total": 0,
        "episodes_embedded": 0,
        "episodes_failed": 0,
    }

    def emit(event: Dict[str, Any]) -> None:
        if progress_callback:
            progress_callback(event)

    with Session(engine) as session:
        pending_rows = list(iter_db_transcripts(session, podcast_name=podcast_name))
        totals["episodes_total"] = len(pending_rows)
        emit(
            {
                "stage": "start",
                "episodes_total": totals["episodes_total"],
                "episodes_embedded": totals["episodes_embedded"],
                "episodes_failed": totals["episodes_failed"],
                "message": "Embedding run started.",
            }
        )

        for episode, podcast, transcript, status in pending_rows:
            episode_title = episode.title or f"Episode {episode.id}"
            emit(
                {
                    "stage": "episode_start",
                    "episodes_total": totals["episodes_total"],
                    "episodes_embedded": totals["episodes_embedded"],
                    "episodes_failed": totals["episodes_failed"],
                    "current_episode": episode_title,
                    "message": f"Embedding {episode_title}",
                }
            )

            if status is None:
                status = EmbeddingStatus(episode_id=episode.id, status="pending")
                session.add(status)

            status.status = "running"
            status.error_message = None
            session.commit()

            batch_ids: List[str] = []
            batch_embeddings: List[List[float]] = []
            batch_documents: List[str] = []
            batch_metadatas: List[Dict[str, str]] = []

            def flush_batch() -> None:
                if not batch_ids:
                    return
                collection.upsert(
                    ids=batch_ids,
                    embeddings=batch_embeddings,
                    documents=batch_documents,
                    metadatas=batch_metadatas,
                )
                batch_ids.clear()
                batch_embeddings.clear()
                batch_documents.clear()
                batch_metadatas.clear()

            try:
                words = transcript.transcript_text.split()
                word_count = len(words)
                chunk_total = 0
                source_id = f"episode-{episode.id}"
                for idx, (chunk, start_word, end_word) in enumerate(
                    chunk_words(transcript.transcript_text, chunk_size, overlap)
                ):
                    chunk_total += 1
                    chunk_id = deterministic_chunk_id(source_id, idx, chunk)
                    embedding = get_ollama_embedding(chunk, model=model, base_url=base_url)
                    metadata = {
                        "conversation_id": source_id,
                        "podcast": podcast.name or "",
                        "episode_title": episode_title,
                        "episode_id": str(episode.id),
                        "transcript_id": str(transcript.id),
                        "published_at": episode.published_at.isoformat() if episode.published_at else "",
                        "chunk_index": str(idx),
                        "start_word": str(start_word),
                        "end_word": str(end_word),
                    }

                    batch_ids.append(chunk_id)
                    batch_embeddings.append(embedding)
                    batch_documents.append(chunk)
                    batch_metadatas.append(metadata)

                    if len(batch_ids) >= flush_every:
                        flush_batch()

                flush_batch()

                status.status = "embedded"
                status.model = model
                status.collection = collection_name
                status.chunk_count = chunk_total
                status.word_count = word_count
                status.last_embedded_at = dt.datetime.now(dt.timezone.utc)
                status.error_message = None
                session.commit()

                totals["episodes_embedded"] += 1
                emit(
                    {
                        "stage": "episode_complete",
                        "episodes_total": totals["episodes_total"],
                        "episodes_embedded": totals["episodes_embedded"],
                        "episodes_failed": totals["episodes_failed"],
                        "current_episode": episode_title,
                        "message": f"Embedded {episode_title}",
                    }
                )
            except Exception as exc:
                session.rollback()
                status.status = "failed"
                status.error_message = str(exc)
                session.add(status)
                session.commit()
                totals["episodes_failed"] += 1
                emit(
                    {
                        "stage": "episode_failed",
                        "episodes_total": totals["episodes_total"],
                        "episodes_embedded": totals["episodes_embedded"],
                        "episodes_failed": totals["episodes_failed"],
                        "current_episode": episode_title,
                        "message": f"Failed to embed {episode_title}: {exc}",
                    }
                )

        emit(
            {
                "stage": "completed",
                "episodes_total": totals["episodes_total"],
                "episodes_embedded": totals["episodes_embedded"],
                "episodes_failed": totals["episodes_failed"],
                "message": "Embedding run completed.",
            }
        )

    return totals


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Embed transcript chunks into ChromaDB via Ollama.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    embed_parser = subparsers.add_parser("embed", help="Chunk + embed transcripts from SQLite.")
    embed_parser.add_argument(
        "--db",
        type=str,
        default=os.getenv("EMBEDDING_DATABASE_URL", DEFAULT_DB_URL),
        help="Database URL for SQLite or other SQLAlchemy-supported databases.",
    )
    embed_parser.add_argument(
        "--chroma-dir",
        type=Path,
        default=DEFAULT_CHROMA_DIR,
        help="Directory for Chroma persistent storage.",
    )
    embed_parser.add_argument(
        "--collection",
        type=str,
        default=DEFAULT_COLLECTION,
        help="Chroma collection name.",
    )
    embed_parser.add_argument(
        "--model",
        type=str,
        default=os.getenv("OLLAMA_EMBED_MODEL", DEFAULT_MODEL),
        help="Ollama embedding model name.",
    )
    embed_parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Target chunk size in words.",
    )
    embed_parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=DEFAULT_CHUNK_OVERLAP,
        help="Overlap between chunks in words.",
    )
    embed_parser.add_argument(
        "--ollama-url",
        type=str,
        default=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        help="Base URL for the Ollama server.",
    )
    embed_parser.add_argument(
        "--podcast",
        type=str,
        default=None,
        help="Optional podcast name filter to embed a single show.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "embed":
        if args.chunk_overlap >= args.chunk_size:
            raise SystemExit("chunk-overlap must be smaller than chunk-size.")

        embed_transcripts(
            database_url=args.db,
            chroma_dir=args.chroma_dir,
            collection_name=args.collection,
            model=args.model,
            chunk_size=args.chunk_size,
            overlap=args.chunk_overlap,
            base_url=args.ollama_url.rstrip("/"),
            podcast_name=args.podcast,
        )
        print("Embedding complete.")


if __name__ == "__main__":
    main()
