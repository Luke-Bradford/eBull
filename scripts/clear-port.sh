#!/usr/bin/env bash
# scripts/clear-port.sh — kill any process LISTENING on a TCP port.
#
# Used by .vscode/tasks.json `stack: backend` + `stack: frontend` to
# reap orphaned vite / uvicorn processes before launching new ones.
# Without this, closing the VS Code window without "Terminate Task"
# leaves the background process holding the port; the next session
# either silently port-hops (vite default) or fails to bind.
#
# Mirrors the Windows Clear-Port logic in stack-restart.ps1. Both
# stack-restart.sh and the VS Code task path now share this script.
#
# Usage:
#   ./scripts/clear-port.sh 5173
#   ./scripts/clear-port.sh 8000

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <port>" >&2
    exit 2
fi

port="$1"

if ! command -v lsof >/dev/null 2>&1; then
    # lsof missing — silently skip (CI / minimal environments).
    exit 0
fi

pids="$(lsof -ti tcp:"$port" -sTCP:LISTEN 2>/dev/null || true)"

if [[ -z "$pids" ]]; then
    exit 0
fi

while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    echo "clear-port: killing pid $pid on :$port"
    kill -9 "$pid" 2>/dev/null || true
done <<<"$pids"

# Brief pause so the kernel releases the socket before the next bind.
sleep 0.5
