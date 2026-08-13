#!/usr/bin/env bash
# stack.sh — prepare the eBull dev stack (postgres + migrations).
#
# POSIX equivalent of stack.ps1. macOS / Linux path used by the
# Makefile and VS Code tasks. Windows continues to use stack.ps1.
#
# What it does:
#   1. clears stale port holders on :8000 and :5173 (lsof / kill)
#   2. docker compose up -d   (postgres + redis)
#   3. waits for pg_isready
#   4. applies pending migrations
#
# The backend (uvicorn) and frontend (vite) are launched as separate
# VS Code tasks ("stack: backend" / "stack: frontend") so they live
# in integrated terminal tabs. Run them via the "dev: start stack"
# task, which depends on this script.
#
# To stop postgres: ./stack-stop.sh

set -euo pipefail

cd "$(dirname "$0")"

clear_port() {
  local port="$1"
  local max_wait="${2:-30}"

  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi

  local elapsed=0
  while [[ "$elapsed" -lt "$max_wait" ]]; do
    local pids
    pids="$(lsof -ti tcp:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -z "$pids" ]]; then
      return 0
    fi
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      echo "  killing pid $pid on :$port"
      kill -9 "$pid" 2>/dev/null || true
    done <<<"$pids"
    sleep 1
    elapsed=$((elapsed + 1))
  done

  if [[ -n "$(lsof -ti tcp:"$port" -sTCP:LISTEN 2>/dev/null || true)" ]]; then
    echo "  warning: port $port still held after ${max_wait}s" >&2
  fi
}

echo "[0/3] Clearing stale ports..."
clear_port 8000
clear_port 5173

echo "[1/3] Starting postgres..."
docker compose up -d

echo "[2/3] Waiting for postgres to be ready..."
elapsed=0
timeout=60
while [[ "$elapsed" -lt "$timeout" ]]; do
  if docker exec ebull-postgres pg_isready -U postgres -d ebull >/dev/null 2>&1; then
    break
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done
if [[ "$elapsed" -ge "$timeout" ]]; then
  echo "Postgres did not become ready in ${timeout}s. Check: docker logs ebull-postgres" >&2
  exit 1
fi
echo "      Postgres ready."

echo "[3/3] Applying migrations..."
PYTHONPATH="$(pwd)" uv run python scripts/migrate.py

echo
echo "Postgres is up and migrations are applied."
echo "Backend and frontend are launched by the VS Code task 'dev: start stack'."
echo "To stop postgres: ./stack-stop.sh"
