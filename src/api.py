"""
REST API module for Democracy Podcast Analysis Platform
Provides programmatic access to podcasts, episodes, and transcripts
"""
from __future__ import annotations

import datetime as dt
from functools import wraps
from typing import Optional, Dict, Any, List

from flask import Blueprint, jsonify, request, g
from sqlalchemy import create_engine, select, func, or_, and_
from sqlalchemy.orm import Session, sessionmaker, selectinload

from rss_transcriber import (
    Base,
    Episode,
    EpisodeKeyword,
    Podcast,
    Transcript,
    ApiKey,
)

# Create API blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api/v1')

# Global session maker (will be set by init_api)
SessionLocal = None


def init_api(engine):
    """Initialize API with database engine"""
    global SessionLocal
    SessionLocal = sessionmaker(bind=engine)


def get_db():
    """Get database session"""
    if not hasattr(g, 'db'):
        g.db = SessionLocal()
    return g.db


@api_bp.teardown_request
def close_db(error):
    """Close database session after request"""
    if hasattr(g, 'db'):
        g.db.close()


def require_api_key(f):
    """Decorator to require API key authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')

        if not api_key:
            return jsonify({
                'error': 'Missing API key',
                'message': 'Please provide X-API-Key header'
            }), 401

        db = get_db()
        key_record = db.execute(
            select(ApiKey).where(ApiKey.key == api_key, ApiKey.is_active == 1)
        ).scalar_one_or_none()

        if not key_record:
            return jsonify({
                'error': 'Invalid API key',
                'message': 'The provided API key is invalid or has been revoked'
            }), 401

        # Update last_used_at
        key_record.last_used_at = dt.datetime.now(dt.timezone.utc)
        db.commit()

        # Store key info in request context
        g.api_key = key_record

        return f(*args, **kwargs)

    return decorated_function


def get_pagination_params():
    """Extract and validate pagination parameters from request"""
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))

        # Validate limits
        if limit < 1:
            limit = 50
        if limit > 200:
            limit = 200
        if offset < 0:
            offset = 0

        return limit, offset
    except ValueError:
        return 50, 0


def serialize_podcast(podcast: Podcast, include_episodes: bool = False) -> Dict[str, Any]:
    """Serialize podcast to JSON-compatible dict"""
    data = {
        'id': podcast.id,
        'name': podcast.name,
        'rss_url': podcast.rss_url,
        'category': podcast.category,
        'language': podcast.language,
        'description': podcast.description,
        'website': podcast.website,
        'image_url': podcast.image_url,
        'last_build_date': podcast.last_build_date.isoformat() if podcast.last_build_date else None,
        'created_at': podcast.created_at.isoformat() if podcast.created_at else None,
        'updated_at': podcast.updated_at.isoformat() if podcast.updated_at else None,
    }

    if include_episodes:
        data['episodes'] = [serialize_episode(ep, include_transcript=False) for ep in podcast.episodes]

    return data


def serialize_episode(episode: Episode, include_transcript: bool = False, include_keywords: bool = False) -> Dict[str, Any]:
    """Serialize episode to JSON-compatible dict"""
    data = {
        'id': episode.id,
        'podcast_id': episode.podcast_id,
        'guid': episode.guid,
        'title': episode.title,
        'description': episode.description,
        'published_at': episode.published_at.isoformat() if episode.published_at else None,
        'audio_url': episode.audio_url,
        'image_url': episode.image_url,
        'duration': episode.duration,
        'deepgram_request_id': episode.deepgram_request_id,
        'transcribed_at': episode.transcribed_at.isoformat() if episode.transcribed_at else None,
    }

    if hasattr(episode, 'podcast') and episode.podcast:
        data['podcast'] = {
            'id': episode.podcast.id,
            'name': episode.podcast.name,
            'category': episode.podcast.category,
        }

    if include_transcript and episode.transcript:
        data['transcript'] = serialize_transcript(episode.transcript)

    if include_keywords and episode.keywords:
        data['keywords'] = [
            {'keyword': kw.keyword, 'weight': kw.weight}
            for kw in episode.keywords[:20]  # Limit to top 20 keywords
        ]

    return data


def serialize_transcript(transcript: Transcript, include_diarization: bool = True) -> Dict[str, Any]:
    """Serialize transcript to JSON-compatible dict"""
    data = {
        'id': transcript.id,
        'episode_id': transcript.episode_id,
        'transcript_text': transcript.transcript_text,
        'confidence': transcript.confidence,
        'seconds': transcript.seconds,
        'request_id': transcript.request_id,
        'channel_count': transcript.channel_count,
        'model_primary_name': transcript.model_primary_name,
        'model_primary_version': transcript.model_primary_version,
    }

    if include_diarization and transcript.diarization:
        data['diarization'] = transcript.diarization

    return data


# ============================================================================
# API ENDPOINTS
# ============================================================================

@api_bp.route('/podcasts', methods=['GET'])
@require_api_key
def list_podcasts():
    """List all podcasts with pagination and filtering"""
    db = get_db()
    limit, offset = get_pagination_params()

    # Build query
    query = select(Podcast)

    # Filter by category
    category = request.args.get('category')
    if category:
        query = query.where(Podcast.category == category)

    # Filter by language
    language = request.args.get('language')
    if language:
        query = query.where(Podcast.language == language)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = db.execute(count_query).scalar()

    # Get paginated results
    query = query.order_by(Podcast.name).limit(limit).offset(offset)
    podcasts = db.execute(query).scalars().all()

    return jsonify({
        'data': [serialize_podcast(p) for p in podcasts],
        'pagination': {
            'limit': limit,
            'offset': offset,
            'total': total,
            'count': len(podcasts),
        }
    })


@api_bp.route('/podcasts/<int:podcast_id>', methods=['GET'])
@require_api_key
def get_podcast(podcast_id: int):
    """Get specific podcast by ID"""
    db = get_db()

    podcast = db.execute(
        select(Podcast).where(Podcast.id == podcast_id)
    ).scalar_one_or_none()

    if not podcast:
        return jsonify({'error': 'Podcast not found'}), 404

    return jsonify(serialize_podcast(podcast))


@api_bp.route('/podcasts/<int:podcast_id>/episodes', methods=['GET'])
@require_api_key
def list_podcast_episodes(podcast_id: int):
    """List episodes for a specific podcast"""
    db = get_db()
    limit, offset = get_pagination_params()

    # Verify podcast exists
    podcast = db.execute(
        select(Podcast).where(Podcast.id == podcast_id)
    ).scalar_one_or_none()

    if not podcast:
        return jsonify({'error': 'Podcast not found'}), 404

    # Build query
    query = select(Episode).where(Episode.podcast_id == podcast_id)

    # Date filtering
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    if date_from:
        try:
            date_from_dt = dt.datetime.fromisoformat(date_from)
            query = query.where(Episode.published_at >= date_from_dt)
        except ValueError:
            pass

    if date_to:
        try:
            date_to_dt = dt.datetime.fromisoformat(date_to)
            query = query.where(Episode.published_at <= date_to_dt)
        except ValueError:
            pass

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = db.execute(count_query).scalar()

    # Get paginated results
    query = query.options(selectinload(Episode.podcast)).order_by(Episode.published_at.desc()).limit(limit).offset(offset)
    episodes = db.execute(query).scalars().all()

    return jsonify({
        'data': [serialize_episode(ep, include_keywords=True) for ep in episodes],
        'pagination': {
            'limit': limit,
            'offset': offset,
            'total': total,
            'count': len(episodes),
        },
        'podcast': {
            'id': podcast.id,
            'name': podcast.name,
        }
    })


@api_bp.route('/episodes', methods=['GET'])
@require_api_key
def list_episodes():
    """List all episodes with pagination and filtering"""
    db = get_db()
    limit, offset = get_pagination_params()

    # Build query
    query = select(Episode).options(selectinload(Episode.podcast))

    # Filter by podcast
    podcast_id = request.args.get('podcast_id')
    if podcast_id:
        try:
            query = query.where(Episode.podcast_id == int(podcast_id))
        except ValueError:
            pass

    # Filter by category (through podcast)
    category = request.args.get('category')
    if category:
        query = query.join(Podcast).where(Podcast.category == category)

    # Date filtering
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    if date_from:
        try:
            date_from_dt = dt.datetime.fromisoformat(date_from)
            query = query.where(Episode.published_at >= date_from_dt)
        except ValueError:
            pass

    if date_to:
        try:
            date_to_dt = dt.datetime.fromisoformat(date_to)
            query = query.where(Episode.published_at <= date_to_dt)
        except ValueError:
            pass

    # Only transcribed episodes
    transcribed_only = request.args.get('transcribed_only', 'false').lower() == 'true'
    if transcribed_only:
        query = query.where(Episode.transcribed_at.isnot(None))

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = db.execute(count_query).scalar()

    # Get paginated results
    query = query.order_by(Episode.published_at.desc()).limit(limit).offset(offset)
    episodes = db.execute(query).scalars().all()

    return jsonify({
        'data': [serialize_episode(ep, include_keywords=True) for ep in episodes],
        'pagination': {
            'limit': limit,
            'offset': offset,
            'total': total,
            'count': len(episodes),
        }
    })


@api_bp.route('/episodes/<int:episode_id>', methods=['GET'])
@require_api_key
def get_episode(episode_id: int):
    """Get specific episode by ID with full transcript"""
    db = get_db()

    # Get include_diarization parameter (default true)
    include_diarization = request.args.get('include_diarization', 'true').lower() == 'true'

    episode = db.execute(
        select(Episode)
        .options(selectinload(Episode.podcast))
        .options(selectinload(Episode.transcript))
        .options(selectinload(Episode.keywords))
        .where(Episode.id == episode_id)
    ).scalar_one_or_none()

    if not episode:
        return jsonify({'error': 'Episode not found'}), 404

    data = serialize_episode(episode, include_transcript=True, include_keywords=True)

    # Override diarization inclusion based on query param
    if data.get('transcript') and not include_diarization:
        data['transcript'].pop('diarization', None)

    return jsonify(data)


@api_bp.route('/transcripts', methods=['GET'])
@require_api_key
def list_transcripts():
    """List all transcripts with pagination"""
    db = get_db()
    limit, offset = get_pagination_params()

    # Build query
    query = select(Transcript).join(Episode).options(selectinload(Transcript.episode))

    # Filter by podcast
    podcast_id = request.args.get('podcast_id')
    if podcast_id:
        try:
            query = query.where(Episode.podcast_id == int(podcast_id))
        except ValueError:
            pass

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = db.execute(count_query).scalar()

    # Get paginated results
    query = query.order_by(Transcript.id.desc()).limit(limit).offset(offset)
    transcripts = db.execute(query).scalars().all()

    # Get include_diarization parameter (default true)
    include_diarization = request.args.get('include_diarization', 'true').lower() == 'true'

    return jsonify({
        'data': [
            {
                **serialize_transcript(t, include_diarization=include_diarization),
                'episode': {
                    'id': t.episode.id,
                    'title': t.episode.title,
                    'podcast_id': t.episode.podcast_id,
                } if t.episode else None
            }
            for t in transcripts
        ],
        'pagination': {
            'limit': limit,
            'offset': offset,
            'total': total,
            'count': len(transcripts),
        }
    })


@api_bp.route('/transcripts/<int:transcript_id>', methods=['GET'])
@require_api_key
def get_transcript(transcript_id: int):
    """Get specific transcript by ID"""
    db = get_db()

    # Get include_diarization parameter (default true)
    include_diarization = request.args.get('include_diarization', 'true').lower() == 'true'

    transcript = db.execute(
        select(Transcript)
        .options(selectinload(Transcript.episode))
        .where(Transcript.id == transcript_id)
    ).scalar_one_or_none()

    if not transcript:
        return jsonify({'error': 'Transcript not found'}), 404

    data = serialize_transcript(transcript, include_diarization=include_diarization)

    if transcript.episode:
        data['episode'] = {
            'id': transcript.episode.id,
            'title': transcript.episode.title,
            'podcast_id': transcript.episode.podcast_id,
            'published_at': transcript.episode.published_at.isoformat() if transcript.episode.published_at else None,
        }

    return jsonify(data)


@api_bp.route('/search', methods=['GET'])
@require_api_key
def search_transcripts():
    """Search through transcript text"""
    db = get_db()
    limit, offset = get_pagination_params()

    # Get search query
    search_query = request.args.get('q', '').strip()

    if not search_query:
        return jsonify({
            'error': 'Missing search query',
            'message': 'Please provide "q" parameter with search text'
        }), 400

    if len(search_query) < 3:
        return jsonify({
            'error': 'Search query too short',
            'message': 'Search query must be at least 3 characters'
        }), 400

    # Build search query (case-insensitive LIKE search)
    # For better performance with large datasets, consider implementing SQLite FTS
    search_pattern = f"%{search_query}%"

    query = (
        select(Transcript)
        .join(Episode)
        .options(selectinload(Transcript.episode).selectinload(Episode.podcast))
        .where(Transcript.transcript_text.ilike(search_pattern))
    )

    # Filter by podcast
    podcast_id = request.args.get('podcast_id')
    if podcast_id:
        try:
            query = query.where(Episode.podcast_id == int(podcast_id))
        except ValueError:
            pass

    # Filter by category
    category = request.args.get('category')
    if category:
        query = query.join(Podcast).where(Podcast.category == category)

    # Date filtering
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    if date_from:
        try:
            date_from_dt = dt.datetime.fromisoformat(date_from)
            query = query.where(Episode.published_at >= date_from_dt)
        except ValueError:
            pass

    if date_to:
        try:
            date_to_dt = dt.datetime.fromisoformat(date_to)
            query = query.where(Episode.published_at <= date_to_dt)
        except ValueError:
            pass

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = db.execute(count_query).scalar()

    # Get paginated results
    query = query.order_by(Episode.published_at.desc()).limit(limit).offset(offset)
    transcripts = db.execute(query).scalars().all()

    # Get include_diarization parameter (default false for search)
    include_diarization = request.args.get('include_diarization', 'false').lower() == 'true'

    return jsonify({
        'data': [
            {
                **serialize_transcript(t, include_diarization=include_diarization),
                'episode': serialize_episode(t.episode) if t.episode else None
            }
            for t in transcripts
        ],
        'pagination': {
            'limit': limit,
            'offset': offset,
            'total': total,
            'count': len(transcripts),
        },
        'search': {
            'query': search_query,
        }
    })


@api_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint (no auth required)"""
    return jsonify({
        'status': 'ok',
        'version': 'v1',
        'service': 'Democracy Podcast Analysis API'
    })


# Error handlers
@api_bp.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@api_bp.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500
