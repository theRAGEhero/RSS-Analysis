FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install runtime dependencies.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

# Prepare runtime directories for the pipeline and web app.
RUN mkdir -p data logs public \
    && groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --home /app --shell /bin/bash app \
    && chown -R app:app /app

USER app

EXPOSE 8000

ENV FLASK_APP=src.webapp:create_app

VOLUME ["/app/data", "/app/logs", "/app/public"]

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "3", "--threads", "4", "--timeout", "120", "src.webapp:create_app()"]
