#!/usr/bin/env bash
# Headless autonomy-loop runner (one fresh `claude` session per invocation).
#
# Drains the eBull engineering board per scripts/autonomy/loop_prompt.md. Meant
# to be fired on a schedule (launchd / cron) ON THE DEV MACHINE — the loop needs
# the local dev stack (DB :5432, API :8000, vite :5173) + the headless browser,
# which Anthropic cloud routines can't reach.
#
#   - Lockfile: only ONE loop runs at a time (a session can run for hours;
#     overlapping firings would race the git tree).
#   - Logs each run under var/autonomy-logs/.
#   - --dangerously-skip-permissions so the unattended session isn't blocked on
#     edit/commit prompts; the SAFETY RAILS (never trade, never merge around the
#     review bot) live in loop_prompt.md AND the appended system prompt below,
#     and the execution-guard + Claude review bot remain the hard gates.
#
# Manual run:  bash scripts/autonomy/run_loop.sh

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

# Refuse to run anywhere but the dev checkout.
[ -f "$REPO/.claude/CLAUDE.md" ] || { echo "not the eBull repo root: $REPO" >&2; exit 1; }

LOCK="$REPO/var/autonomy-loop.lock"
LOGDIR="$REPO/var/autonomy-logs"
mkdir -p "$LOGDIR"
MAX_LOCK_AGE=$((12 * 3600)) # a session shouldn't run >12h; an older lock is wedged

# Acquire an exclusive lock. `mkdir` is atomic. A stale lock (dead pid OR older
# than MAX_LOCK_AGE — guards PID reuse) is claimed by ONE racer via an atomic
# `mv` of the stale dir (Codex ckpt-2 HIGH: the old rm+mkdir was racey).
if ! mkdir "$LOCK" 2>/dev/null; then
  pid="$(cat "$LOCK/pid" 2>/dev/null || echo)"
  age=$(($(date +%s) - $(stat -f %m "$LOCK" 2>/dev/null || echo 0)))
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && [ "$age" -lt "$MAX_LOCK_AGE" ]; then
    echo "autonomy loop already running (pid $pid, age ${age}s); exiting." >&2
    exit 0
  fi
  dead="$LOCK.dead.$$"
  if mv "$LOCK" "$dead" 2>/dev/null; then rm -rf "$dead"; fi
  mkdir "$LOCK" 2>/dev/null || { echo "lost stale-lock claim race; exiting." >&2; exit 0; }
fi
echo $$ >"$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT

LOG="$LOGDIR/loop-$(date +%Y%m%dT%H%M%S).log"
echo "=== autonomy loop start $(date -u +%FT%TZ) -> $LOG ==="

# Clean-state preflight (Codex ckpt-2 MED): each session starts on clean, latest
# main. If a prior crash left a dirty tree or an un-fast-forwardable main, ABORT
# and leave it for inspection — never start a session on top of half-done work.
git fetch origin -q 2>>"$LOG" || { echo "preflight: git fetch failed (network?) — abort, won't run on stale state" | tee -a "$LOG" >&2; exit 1; }
git checkout main -q 2>>"$LOG" || { echo "preflight: cannot checkout main — abort" | tee -a "$LOG" >&2; exit 1; }
if ! git pull -q --ff-only 2>>"$LOG"; then
  echo "preflight: main not fast-forward — abort (manual inspection)" | tee -a "$LOG" >&2; exit 1
fi
if [ -n "$(git status --porcelain)" ]; then
  echo "preflight: working tree dirty on main — abort, leaving for inspection" | tee -a "$LOG" >&2; exit 1
fi

SAFETY="Unattended run. HARD RULES: never execute/approve/simulate a trade, never POST order endpoints, never touch the kill-switch, never close a position; merge ONLY after the Claude review bot APPROVES the latest commit with CI green; never push --no-verify; never restart the :8000/:5173 tasks. Follow .claude/CLAUDE.md and scripts/autonomy/loop_prompt.md exactly."

# stream-json keeps a parseable transcript; tee for live tail.
claude -p "$(cat "$REPO/scripts/autonomy/loop_prompt.md")" \
  --dangerously-skip-permissions \
  --append-system-prompt "$SAFETY" \
  --output-format stream-json --verbose \
  >> "$LOG" 2>&1 || echo "claude exited non-zero ($?)" >> "$LOG"

echo "=== autonomy loop end $(date -u +%FT%TZ) ===" >> "$LOG"
