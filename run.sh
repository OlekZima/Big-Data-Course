#!/usr/bin/env bash
set -euo pipefail

# Load environment variables from .env if present
if [[ -f .env ]]; then
    set -a
    source .env
    set +a
    echo "[RUN] Loaded environment variables from .env"
else
    echo "[RUN] No .env file found, using defaults"
fi

echo "[RUN] Starting PostgreSQL via Docker..."
docker compose up -d

echo "[RUN] Waiting for PostgreSQL to be ready..."
until docker compose exec -T db pg_isready -U ${POSTGRES_USER:-user} -d ${POSTGRES_DB:-bigdata} -q; do
    echo "[RUN]   ... not ready yet, retrying in 1s"
    sleep 1
done
echo "[RUN] PostgreSQL is ready."

echo "[RUN] Running pipeline..."
uv run python -m src.main

echo "[RUN] Done."
