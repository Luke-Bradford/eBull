#!/usr/bin/env bash
# stack-stop.sh — stop the full eBull dev stack.
#
# POSIX equivalent of stack-stop.ps1. Kills any process holding
# ports 8000 (uvicorn) and 5173 (vite), then stops the postgres
# container. Postgres data is preserved in the `pgdata` docker
# volume.

set -euo pipefail

cd "$(dirname "$0")"

kill_port() {
  local port="$1"
  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi
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
}

# Match the .vscode/tasks.json group `ebull-stack` membership: backend
# (:8000), frontend (:5173), AND the jobs process (#719 — runs the
# scheduler in a separate process from the API). The jobs process has
# no port; match it by command line.
kill_jobs_process() {
  if ! command -v pgrep >/dev/null 2>&1; then
    return 0
  fi
  local pids
  pids="$(pgrep -f 'python.*-m[[:space:]]+app\.jobs' || true)"
  [[ -z "$pids" ]] && return 0
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    echo "  stopping app.jobs (pid $pid)"
    kill -9 "$pid" 2>/dev/null || true
  done <<<"$pids"
}

echo "Stopping backend (:8000), frontend (:5173), and jobs process..."
kill_port 8000
kill_port 5173
kill_jobs_process

echo "Stopping postgres..."
docker compose stop

echo "Stack stopped."
