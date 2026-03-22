#!/usr/bin/env bash
set -euo pipefail

echo "[RUN] Starting PostgreSQL via Docker..."
docker compose up -d

echo "[RUN] Running pipeline..."
uv run python -m src.main

echo "[RUN] Done."
