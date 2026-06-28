#!/usr/bin/env bash
# Autonomy SUPERVISOR — run board-drain sessions back-to-back for days,
# unattended, with usage-limit backoff. This is the "walk away, come back to the
# board worked through" entry point.
#
# Loop: check the board → run ONE fresh headless `claude` session (drains as many
# tickets as fit in its context) → classify the outcome → wait → repeat, forever:
#   - usage/rate limit hit  → back off (exponential, capped ~5h ≈ the usage
#                             window) then retry when capacity returns.
#   - other error           → exponential backoff (capped 1h).
#   - clean finish          → short pace, next session.
#   - board empty           → idle-poll every 30 min.
#
# Kept alive across crashes/reboots by launchd KeepAlive (com.ebull.autonomy.
# supervisor.plist). Single instance via lock. Each session starts on clean
# latest main (preflight); a dirty tree from a crash aborts that iteration
# rather than building on half-done work.
#
# Run:  bash scripts/autonomy/supervisor.sh   (or via launchd — see setup.md)

set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"
[ -f "$REPO/.claude/CLAUDE.md" ] || { echo "not the eBull repo root: $REPO" >&2; exit 1; }

LOCK="$REPO/var/autonomy-supervisor.lock"
LOGDIR="$REPO/var/autonomy-logs"
mkdir -p "$LOGDIR"
SUPLOG="$LOGDIR/supervisor.log"

# --- timing knobs (seconds) ---
PACE=120                    # gap between back-to-back clean sessions
EMPTY_IDLE=1800            # board empty → poll every 30 min
ERR_BACKOFF_START=300; ERR_BACKOFF_MAX=3600
LIMIT_BACKOFF_START=1800; LIMIT_BACKOFF_MAX=18000   # 30 min → cap 5 h (usage window)

FALLBACK_MODEL="claude-sonnet-4-6"   # keep working if the primary model is throttled
SAFETY="Unattended run. HARD RULES: never execute/approve/simulate a trade, never POST order endpoints, never touch the kill-switch, never close a position; merge ONLY via scripts/autonomy/safe_merge.sh; never push --no-verify; never restart the :8000/:5173 tasks. Follow .claude/CLAUDE.md and scripts/autonomy/loop_prompt.md exactly."

log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$SUPLOG"; }

# --- single-instance lock (atomic mkdir; stale = dead pid; supervisor is long-lived) ---
if ! mkdir "$LOCK" 2>/dev/null; then
  pid="$(cat "$LOCK/pid" 2>/dev/null || echo)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    log "supervisor already running (pid $pid); exiting."; exit 0
  fi
  rm -rf "$LOCK"; mkdir "$LOCK" || { log "lost lock race; exiting."; exit 0; }
fi
echo $$ >"$LOCK/pid"
trap 'rm -rf "$LOCK"; log "supervisor stopped."; exit 0' EXIT INT TERM

log "=== supervisor start (pid $$) ==="
err_backoff=$ERR_BACKOFF_START
limit_backoff=$LIMIT_BACKOFF_START

run_session() {
  # Preflight: clean, latest main, or skip this iteration.
  git fetch origin -q 2>>"$SUPLOG" || { log "preflight: fetch failed"; return 2; }
  git checkout main -q 2>>"$SUPLOG" || { log "preflight: checkout main failed"; return 2; }
  git pull -q --ff-only 2>>"$SUPLOG" || { log "preflight: main not fast-forward"; return 2; }
  [ -z "$(git status --porcelain)" ] || { log "preflight: tree dirty on main — skip"; return 2; }

  local log_file; log_file="$LOGDIR/session-$(date +%Y%m%dT%H%M%S).log"
  log "session start -> $log_file"
  claude -p "$(cat "$REPO/scripts/autonomy/loop_prompt.md")" \
    --dangerously-skip-permissions \
    --fallback-model "$FALLBACK_MODEL" \
    --append-system-prompt "$SAFETY" \
    --output-format stream-json --verbose \
    >>"$log_file" 2>&1
  local rc=$?
  # Usage/rate-limit signal anywhere in the session output.
  if grep -qiE 'usage limit|rate limit|resets at|overloaded_error|too many requests|"?429"?|exceeded your' "$log_file"; then
    return 3
  fi
  return $rc
}

while true; do
  open_count="$(gh issue list --state open --json number -q 'length' 2>/dev/null || echo -1)"
  if [ "$open_count" = "0" ]; then
    log "board empty — idle ${EMPTY_IDLE}s"; sleep "$EMPTY_IDLE"; continue
  fi

  run_session; outcome=$?
  case $outcome in
    0) log "session clean (open issues ~$open_count). pace ${PACE}s"
       err_backoff=$ERR_BACKOFF_START; limit_backoff=$LIMIT_BACKOFF_START
       sleep "$PACE" ;;
    3) jitter=$((RANDOM % 120))
       log "USAGE LIMIT — backoff $((limit_backoff + jitter))s then retry"
       sleep $((limit_backoff + jitter))
       limit_backoff=$(( limit_backoff*2 < LIMIT_BACKOFF_MAX ? limit_backoff*2 : LIMIT_BACKOFF_MAX )) ;;
    2) log "preflight skip — wait ${ERR_BACKOFF_START}s"; sleep "$ERR_BACKOFF_START" ;;
    *) log "session error (rc=$outcome) — backoff ${err_backoff}s"
       sleep "$err_backoff"
       err_backoff=$(( err_backoff*2 < ERR_BACKOFF_MAX ? err_backoff*2 : ERR_BACKOFF_MAX )) ;;
  esac
done
