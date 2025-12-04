# REST API Documentation

The Democracy Podcast Analysis Platform provides a comprehensive REST API for programmatic access to podcast metadata, episodes, and transcripts with speaker diarization.

## Requesting API Access

**API access is restricted and requires manual approval.**

To request an API key:

1. Contact the platform administrator at [your-contact-email]
2. Provide:
   - Your name/organization
   - Intended use case
   - Expected request volume
3. You will receive a unique API key via secure communication
4. **Store your API key securely** - it will only be shown once and cannot be retrieved later

## Authentication

All API endpoints (except `/health`) require authentication using an API key.

### Authentication Header

Include your API key in every request:

```http
X-API-Key: your-api-key-here
```

### Example Request

```bash
curl -H "X-API-Key: your-api-key-here" \
  "https://your-domain.com/api/v1/podcasts"
```

### Error Responses

**401 Unauthorized - Missing API Key**
```json
{
  "error": "Missing API key",
  "message": "Please provide X-API-Key header"
}
```

**401 Unauthorized - Invalid API Key**
```json
{
  "error": "Invalid API key",
  "message": "The provided API key is invalid or has been revoked"
}
```

## Base URL

- **Development**: `http://localhost:8000/api/v1`
- **Production**: `https://your-domain.com/api/v1`

All endpoints are prefixed with `/api/v1`.

## Common Parameters

Most list endpoints support the following query parameters:

| Parameter | Type | Default | Max | Description |
|-----------|------|---------|-----|-------------|
| `limit` | integer | 50 | 200 | Number of results per page |
| `offset` | integer | 0 | - | Number of results to skip (for pagination) |
| `include_diarization` | boolean | true | - | Include speaker diarization data |

## Response Format

All successful responses return JSON with consistent structure:

### List Responses

```json
{
  "data": [...],
  "pagination": {
    "limit": 50,
    "offset": 0,
    "total": 150,
    "count": 50
  }
}
```

### Single Item Responses

```json
{
  "id": 123,
  "field1": "value1",
  ...
}
```

### Error Responses

```json
{
  "error": "Error type",
  "message": "Detailed error message"
}
```

## Endpoints

### Health Check

**No authentication required**

```http
GET /api/v1/health
```

Check API availability and version.

**Response:**
```json
{
  "status": "ok",
  "version": "v1",
  "service": "Democracy Podcast Analysis API"
}
```

---

### List Podcasts

```http
GET /api/v1/podcasts
```

Retrieve a paginated list of all podcasts.

**Query Parameters:**
- `limit` - Results per page (default: 50, max: 200)
- `offset` - Pagination offset (default: 0)
- `category` - Filter by category (optional)
- `language` - Filter by language code (optional, e.g., "en", "it")

**Example Request:**
```bash
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/podcasts?category=Technology&limit=10"
```

**Example Response:**
```json
{
  "data": [
    {
      "id": 9,
      "name": "Democracy Innovators Podcast",
      "rss_url": "https://example.com/feed.xml",
      "category": "Technology",
      "language": "en",
      "description": "Exploring democratic innovation...",
      "website": "https://example.com",
      "image_url": "https://example.com/image.jpg",
      "last_build_date": "2025-10-31T08:16:21",
      "created_at": "2025-10-31T08:16:21.872592",
      "updated_at": "2025-10-31T08:16:21.872596"
    }
  ],
  "pagination": {
    "limit": 10,
    "offset": 0,
    "total": 9,
    "count": 1
  }
}
```

---

### Get Podcast

```http
GET /api/v1/podcasts/{id}
```

Retrieve details for a specific podcast.

**Path Parameters:**
- `id` - Podcast ID (integer)

**Example Response:**
```json
{
  "id": 9,
  "name": "Democracy Innovators Podcast",
  "rss_url": "https://example.com/feed.xml",
  "category": "Technology",
  "language": "en",
  "description": "Exploring democratic innovation...",
  "website": "https://example.com",
  "image_url": "https://example.com/image.jpg",
  "last_build_date": "2025-10-31T08:16:21",
  "created_at": "2025-10-31T08:16:21.872592",
  "updated_at": "2025-10-31T08:16:21.872596"
}
```

---

### List Episodes for Podcast

```http
GET /api/v1/podcasts/{id}/episodes
```

Retrieve all episodes for a specific podcast.

**Path Parameters:**
- `id` - Podcast ID (integer)

**Query Parameters:**
- `limit` - Results per page (default: 50, max: 200)
- `offset` - Pagination offset (default: 0)
- `date_from` - Filter episodes from this date (ISO 8601 format: YYYY-MM-DD)
- `date_to` - Filter episodes until this date (ISO 8601 format: YYYY-MM-DD)

**Example Request:**
```bash
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/podcasts/9/episodes?date_from=2025-01-01&limit=5"
```

**Example Response:**
```json
{
  "data": [
    {
      "id": 477,
      "podcast_id": 9,
      "guid": "episode-guid-123",
      "title": "Episode Title",
      "description": "Episode description...",
      "published_at": "2025-10-30T13:44:49",
      "audio_url": "https://example.com/audio.mp3",
      "image_url": "https://example.com/episode-image.jpg",
      "duration": 3600,
      "deepgram_request_id": "request-id-123",
      "transcribed_at": "2025-10-30T22:01:57.783801",
      "podcast": {
        "id": 9,
        "name": "Democracy Innovators Podcast",
        "category": "Technology"
      },
      "keywords": [
        {"keyword": "democracy", "weight": 0.0152},
        {"keyword": "governance", "weight": 0.0101}
      ]
    }
  ],
  "pagination": {
    "limit": 5,
    "offset": 0,
    "total": 25,
    "count": 5
  },
  "podcast": {
    "id": 9,
    "name": "Democracy Innovators Podcast"
  }
}
```

---

### List Episodes

```http
GET /api/v1/episodes
```

Retrieve all episodes across all podcasts.

**Query Parameters:**
- `limit` - Results per page (default: 50, max: 200)
- `offset` - Pagination offset (default: 0)
- `podcast_id` - Filter by podcast ID (optional)
- `category` - Filter by podcast category (optional)
- `date_from` - Filter episodes from this date (ISO 8601)
- `date_to` - Filter episodes until this date (ISO 8601)
- `transcribed_only` - Only return transcribed episodes (true/false, default: false)

**Example Request:**
```bash
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/episodes?transcribed_only=true&limit=10"
```

---

### Get Episode

```http
GET /api/v1/episodes/{id}
```

Retrieve a specific episode with full transcript and speaker diarization.

**Path Parameters:**
- `id` - Episode ID (integer)

**Query Parameters:**
- `include_diarization` - Include speaker diarization segments (true/false, default: true)

**Example Request:**
```bash
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/episodes/477?include_diarization=true"
```

**Example Response:**
```json
{
  "id": 477,
  "podcast_id": 9,
  "title": "Episode Title",
  "description": "Episode description...",
  "published_at": "2025-10-30T13:44:49",
  "audio_url": "https://example.com/audio.mp3",
  "duration": 3600,
  "transcribed_at": "2025-10-30T22:01:57.783801",
  "podcast": {
    "id": 9,
    "name": "Democracy Innovators Podcast",
    "category": "Technology"
  },
  "keywords": [
    {"keyword": "democracy", "weight": 0.0152}
  ],
  "transcript": {
    "id": 477,
    "episode_id": 477,
    "transcript_text": "Full transcript text...",
    "confidence": 0.9984,
    "seconds": 3598.5,
    "model_primary_name": "2-general-nova",
    "model_primary_version": "2024-01-06.5664",
    "diarization": [
      {
        "speaker": 0,
        "start": 0.5,
        "end": 5.2,
        "transcript": "Hello and welcome..."
      },
      {
        "speaker": 1,
        "start": 5.5,
        "end": 12.3,
        "transcript": "Thanks for having me..."
      }
    ]
  }
}
```

---

### List Transcripts

```http
GET /api/v1/transcripts
```

Retrieve all transcripts with basic episode information.

**Query Parameters:**
- `limit` - Results per page (default: 50, max: 200)
- `offset` - Pagination offset (default: 0)
- `podcast_id` - Filter by podcast ID (optional)
- `include_diarization` - Include speaker segments (true/false, default: true)

**Example Response:**
```json
{
  "data": [
    {
      "id": 477,
      "episode_id": 477,
      "transcript_text": "Full transcript...",
      "confidence": 0.9984,
      "seconds": 3598.5,
      "channel_count": 1,
      "model_primary_name": "2-general-nova",
      "diarization": [...],
      "episode": {
        "id": 477,
        "title": "Episode Title",
        "podcast_id": 9
      }
    }
  ],
  "pagination": {
    "limit": 50,
    "offset": 0,
    "total": 150,
    "count": 50
  }
}
```

---

### Get Transcript

```http
GET /api/v1/transcripts/{id}
```

Retrieve a specific transcript by ID.

**Path Parameters:**
- `id` - Transcript ID (integer)

**Query Parameters:**
- `include_diarization` - Include speaker segments (true/false, default: true)

---

### Search Transcripts

```http
GET /api/v1/search
```

Full-text search through all transcript content.

**Query Parameters (Required):**
- `q` - Search query (minimum 3 characters)

**Query Parameters (Optional):**
- `limit` - Results per page (default: 50, max: 200)
- `offset` - Pagination offset (default: 0)
- `podcast_id` - Filter by podcast ID
- `category` - Filter by podcast category
- `date_from` - Filter episodes from this date (ISO 8601)
- `date_to` - Filter episodes until this date (ISO 8601)
- `include_diarization` - Include speaker segments (true/false, default: false for search)

**Example Request:**
```bash
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/search?q=democracy&category=Technology&limit=10"
```

**Example Response:**
```json
{
  "data": [
    {
      "id": 477,
      "episode_id": 477,
      "transcript_text": "...mentions democracy multiple times...",
      "confidence": 0.9984,
      "seconds": 3598.5,
      "episode": {
        "id": 477,
        "title": "Episode Title",
        "podcast_id": 9,
        "published_at": "2025-10-30T13:44:49",
        "podcast": {
          "id": 9,
          "name": "Democracy Innovators Podcast",
          "category": "Technology"
        }
      }
    }
  ],
  "pagination": {
    "limit": 10,
    "offset": 0,
    "total": 45,
    "count": 10
  },
  "search": {
    "query": "democracy"
  }
}
```

---

## Speaker Diarization

When `include_diarization=true`, transcript responses include speaker-segmented data:

```json
{
  "diarization": [
    {
      "speaker": 0,
      "start": 0.5,
      "end": 5.2,
      "transcript": "Hello and welcome to the show."
    },
    {
      "speaker": 1,
      "start": 5.5,
      "end": 12.3,
      "transcript": "Thanks for having me. Great to be here."
    }
  ]
}
```

**Fields:**
- `speaker` - Speaker identifier (integer, 0-indexed)
- `start` - Start time in seconds (float)
- `end` - End time in seconds (float)
- `transcript` - Text spoken by this speaker in this segment

---

## Rate Limits

Currently, there are no hard rate limits enforced. However, please be respectful:

- Avoid excessive concurrent requests
- Implement exponential backoff for retries
- Cache responses when appropriate
- Contact the administrator if you need higher throughput

Abuse of the API may result in key revocation.

---

## Best Practices

### Pagination

Always use pagination for list endpoints:

```bash
# Get first page
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/episodes?limit=50&offset=0"

# Get second page
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/episodes?limit=50&offset=50"
```

### Error Handling

Always check HTTP status codes:

- **200** - Success
- **400** - Bad Request (invalid parameters)
- **401** - Unauthorized (missing or invalid API key)
- **404** - Not Found
- **500** - Internal Server Error

```python
import requests

response = requests.get(
    'https://api.example.com/api/v1/podcasts',
    headers={'X-API-Key': 'your-key'}
)

if response.status_code == 200:
    data = response.json()
    # Process data
elif response.status_code == 401:
    print("Invalid API key")
else:
    print(f"Error: {response.status_code}")
```

### Filtering and Search

Combine filters for precise results:

```bash
# Get technology podcasts' episodes from 2025 that mention "governance"
curl -H "X-API-Key: your-key" \
  "https://api.example.com/api/v1/search?q=governance&category=Technology&date_from=2025-01-01"
```

---

## Example Use Cases

### 1. Build a Podcast Search Engine

```python
import requests

API_KEY = 'your-api-key'
BASE_URL = 'https://api.example.com/api/v1'

def search_transcripts(query, category=None):
    """Search transcripts and return episode titles"""
    params = {'q': query, 'limit': 20}
    if category:
        params['category'] = category

    response = requests.get(
        f'{BASE_URL}/search',
        headers={'X-API-Key': API_KEY},
        params=params
    )

    if response.status_code == 200:
        results = response.json()
        for item in results['data']:
            episode = item['episode']
            print(f"- {episode['title']}")
            print(f"  Podcast: {episode['podcast']['name']}")
            print(f"  Published: {episode['published_at']}")
            print()

search_transcripts('democracy', category='Technology')
```

### 2. Export All Transcripts

```python
import requests
import json

API_KEY = 'your-api-key'
BASE_URL = 'https://api.example.com/api/v1'

def export_all_transcripts():
    """Export all transcripts to JSON file"""
    all_transcripts = []
    offset = 0
    limit = 100

    while True:
        response = requests.get(
            f'{BASE_URL}/transcripts',
            headers={'X-API-Key': API_KEY},
            params={'limit': limit, 'offset': offset}
        )

        data = response.json()
        all_transcripts.extend(data['data'])

        if len(data['data']) < limit:
            break

        offset += limit

    with open('transcripts_export.json', 'w') as f:
        json.dump(all_transcripts, f, indent=2)

    print(f"Exported {len(all_transcripts)} transcripts")

export_all_transcripts()
```

### 3. Analyze Speaker Distribution

```python
import requests
from collections import Counter

API_KEY = 'your-api-key'
BASE_URL = 'https://api.example.com/api/v1'

def analyze_speakers(episode_id):
    """Analyze speaker time distribution in an episode"""
    response = requests.get(
        f'{BASE_URL}/episodes/{episode_id}',
        headers={'X-API-Key': API_KEY},
        params={'include_diarization': 'true'}
    )

    data = response.json()
    diarization = data['transcript']['diarization']

    # Calculate speaking time per speaker
    speaker_time = Counter()
    for segment in diarization:
        duration = segment['end'] - segment['start']
        speaker_time[segment['speaker']] += duration

    print(f"Episode: {data['title']}")
    for speaker, time in speaker_time.most_common():
        minutes = int(time // 60)
        seconds = int(time % 60)
        print(f"  Speaker {speaker}: {minutes}m {seconds}s")

analyze_speakers(477)
```

---

## Support

For API issues, feature requests, or access questions:

- Email: [your-support-email]
- Documentation: This file
- Web Interface: Access the admin panel at `/admin/api-keys` (requires authentication)

---

## Changelog

### v1.0.0 (2025-12-02)
- Initial API release
- Endpoints for podcasts, episodes, transcripts, and search
- API key authentication
- Speaker diarization support
- Full-text search capability
