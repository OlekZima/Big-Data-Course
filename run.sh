#!/usr/bin/env bash
set -euo pipefail

echo "[RUN] Starting PostgreSQL via Docker..."
docker compose up -d

echo "[RUN] Waiting for PostgreSQL to be ready..."
until docker compose exec -T db pg_isready -U user -d bigdata -q; do
    echo "[RUN]   ... not ready yet, retrying in 1s"
    sleep 1
done
echo "[RUN] PostgreSQL is ready."

echo "[RUN] Running pipeline..."
uv run python -m src.main

echo "[RUN] Done."
