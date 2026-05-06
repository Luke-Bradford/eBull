#!/usr/bin/env bash
# stack-restart.sh — restart the backend, jobs process, and/or frontend.
#
# POSIX equivalent of stack-restart.ps1.
#
# Usage:
#   ./stack-restart.sh                # restart all three
#   ./stack-restart.sh --backend      # restart backend only
#   ./stack-restart.sh --frontend     # restart frontend only
#   ./stack-restart.sh --jobs         # restart jobs process only
#
# Why: after pulling or merging, run this to pick up the latest
# code without touching postgres or migrations. The jobs process
# (#719) runs APScheduler + the manual-trigger executor + the queue
# dispatcher in a separate process from the FastAPI API; it does
# not auto-reload, so it must be restarted explicitly when its
# source changes.
#
# Each restarted service is launched detached via `nohup ... &`
# with stdout / stderr redirected to ~/Library/Logs/ebull/<svc>.log
# (macOS) or $TMPDIR/ebull/<svc>.log (Linux fallback). The Windows
# script opens visible PowerShell windows; on macOS the operator
# typically tails the log files or uses VS Code task panels, so a
# detached + logged model matches that surface.

set -euo pipefail

cd "$(dirname "$0")"

backend=0
frontend=0
jobs=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backend)  backend=1 ;;
    --frontend) frontend=1 ;;
    --jobs)     jobs=1 ;;
    -h|--help)
      grep -E '^# ' "$0" | sed 's/^# //'
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      exit 2
      ;;
  esac
  shift
done

if [[ "$backend" -eq 0 && "$frontend" -eq 0 && "$jobs" -eq 0 ]]; then
  backend=1
  frontend=1
  jobs=1
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  log_dir="${HOME}/Library/Logs/ebull"
else
  log_dir="${TMPDIR:-/tmp}/ebull"
fi
mkdir -p "$log_dir"

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

# uvicorn --reload runs a watchdog parent that respawns the worker
# child. Killing only the port listener leaves the parent alive, and
# it immediately starts a new worker that races our nohup'd
# replacement. Match the PS1 behaviour: kill every uvicorn-related
# process for this app before clearing the port.
kill_uvicorn_tree() {
  if ! command -v pgrep >/dev/null 2>&1; then
    return 0
  fi
  local pids
  pids="$(pgrep -f 'uvicorn.*app\.main:app' || true)"
  [[ -z "$pids" ]] && return 0
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    echo "  stopping uvicorn (pid $pid)"
    kill -9 "$pid" 2>/dev/null || true
  done <<<"$pids"
  # Brief pause so the kernel reclaims the listener before we start
  # the replacement and lsof cleans up any lingering child sockets.
  sleep 1
}

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
  # Give Postgres time to detect the dead session and release the
  # singleton advisory lock before the new process tries to acquire.
  sleep 1
}

start_detached() {
  local name="$1"
  shift
  local logfile="${log_dir}/${name}.log"
  echo "  starting ${name}; log=${logfile}"
  nohup "$@" >"$logfile" 2>&1 &
  local pid="$!"
  sleep 2
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "  ${name} failed to start; tail of ${logfile}:" >&2
    tail -n 20 "$logfile" >&2 || true
    return 1
  fi
  echo "  ${name} started (pid ${pid})"
}

if [[ "$backend" -eq 1 ]]; then
  echo "Restarting backend..."
  kill_uvicorn_tree
  kill_port 8000
  start_detached "backend" \
    uv run uvicorn app.main:app --reload --reload-dir app --host 127.0.0.1 --port 8000
fi

if [[ "$jobs" -eq 1 ]]; then
  echo "Restarting jobs process..."
  kill_jobs_process
  start_detached "jobs" \
    uv run python -m app.jobs
fi

if [[ "$frontend" -eq 1 ]]; then
  echo "Restarting frontend..."
  kill_port 5173
  ( cd frontend && start_detached "frontend" pnpm dev )
fi

echo
echo "Done. Services restarted. Logs: ${log_dir}"
