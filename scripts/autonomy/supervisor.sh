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
cd "$REPO" || exit 1
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
PREFLIGHT_RECOVERY_AFTER=2  # consecutive dirty-tree skips before stashing WIP + proceeding (#1801)
dirty_skips=0               # supervisor-global counter (reset on any clean-tree observation)

FALLBACK_MODEL="claude-sonnet-4-6"   # keep working if the primary model is throttled
SAFETY="Unattended run. HARD RULES: never execute/approve/simulate a trade, never POST order endpoints, never touch the kill-switch, never close a position; merge ONLY via scripts/autonomy/safe_merge.sh; never push --no-verify; never restart the :8000/:5173 tasks. Follow .claude/CLAUDE.md and scripts/autonomy/loop_prompt.md exactly."

log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$SUPLOG"; }

# Preflight: put the tree on clean, latest main — or return 2 to skip this
# iteration. Extracted as its own function so the dirty-tree recovery is unit-
# testable against a throwaway repo (test_preflight_recovery.sh) without the loop.
preflight() {
  # GUARD an in-progress git operation FIRST: a `git checkout`/`git stash` could
  # discard or mask that work (review #1768). These are NEVER auto-recovered — a
  # human must resolve them. Cover rebase|cherry-pick|merge|revert|bisect; resolve
  # the git dir via rev-parse so a non-standard layout still works.
  local gitdir; gitdir="$(git rev-parse --git-dir 2>/dev/null || echo .git)"
  if [ -d "$gitdir/rebase-merge" ] || [ -d "$gitdir/rebase-apply" ] \
     || [ -f "$gitdir/CHERRY_PICK_HEAD" ] || [ -f "$gitdir/MERGE_HEAD" ] \
     || [ -f "$gitdir/REVERT_HEAD" ] || [ -f "$gitdir/BISECT_LOG" ]; then
    log "preflight: rebase/cherry-pick/merge/revert/bisect in progress — skip (needs a human)"; return 2
  fi

  # Dirty tree: a mid-edit session that ended before committing (context
  # exhausted) leaves WIP. Skip the first PREFLIGHT_RECOVERY_AFTER-1 iterations
  # (the session may still be finishing, or the monitor may commit the WIP), but
  # DON'T wedge forever (#1801): on the K-th consecutive dirty skip, stash the WIP
  # (tracked + untracked, discoverable via `git stash list`) and proceed onto
  # clean main so the board keeps draining.
  if [ -n "$(git status --porcelain)" ]; then
    dirty_skips=$((dirty_skips + 1))
    if [ "$dirty_skips" -lt "$PREFLIGHT_RECOVERY_AFTER" ]; then
      log "preflight: tree dirty — skip ${dirty_skips}/${PREFLIGHT_RECOVERY_AFTER} (won't checkout over uncommitted work yet)"
      return 2
    fi
    local stash_msg; stash_msg="autonomy-preflight-recovery $(date -u +%FT%TZ)"
    if ! git stash push -u -m "$stash_msg" >>"$SUPLOG" 2>&1; then
      log "preflight: tree dirty ${dirty_skips}× but 'git stash' FAILED — cannot auto-recover; skip (needs a human)"; return 2
    fi
    # Don't trust the stash exit alone — confirm the tree is actually clean now.
    if [ -n "$(git status --porcelain)" ]; then
      log "preflight: stashed WIP but tree still dirty — cannot auto-recover; skip (needs a human)"; return 2
    fi
    log "preflight: tree dirty ${dirty_skips}× — stashed WIP ('$stash_msg'; recover via 'git stash list') and proceeding onto main"
  fi
  dirty_skips=0   # clean tree (or just recovered) — reset BEFORE fetch so a one-off fetch fail keeps no stale count

  git fetch origin -q 2>>"$SUPLOG" || { log "preflight: fetch failed"; return 2; }
  git checkout main -q 2>>"$SUPLOG" || { log "preflight: checkout main failed"; return 2; }
  git pull -q --ff-only 2>>"$SUPLOG" || { log "preflight: main not fast-forward"; return 2; }
  [ -z "$(git status --porcelain)" ] || { log "preflight: tree dirty on main — skip"; return 2; }
  return 0
}

run_session() {
  preflight || return $?

  local log_file; log_file="$LOGDIR/session-$(date +%Y%m%dT%H%M%S).log"
  log "session start -> $log_file"
  claude -p "$(cat "$REPO/scripts/autonomy/loop_prompt.md")" \
    --dangerously-skip-permissions \
    --fallback-model "$FALLBACK_MODEL" \
    --append-system-prompt "$SAFETY" \
    --output-format stream-json --verbose \
    >>"$log_file" 2>&1
  local rc=$?
  # Usage/rate-limit signal from STRUCTURED stream-json events ONLY — never from
  # assistant/tool text (#1770: a session editing SEC retry code is full of '429'/
  # 'rate limit' literals; the old whole-log grep tripped on code content + slept
  # ~31m for nothing on a session that SUCCEEDED). A real block = the session did
  # NOT succeed AND a rate_limit_event's primary-window status is "rejected" and
  # not covered by overage. A successful session was never blocked, whatever
  # literals its content carried.
  if is_usage_limit_hit "$log_file"; then
    return 3
  fi
  return $rc
}

# Classify a usage/rate-limit block from the session's stream-json log. Exit 0 =
# blocked (caller maps to the limit backoff), 1 = not blocked. Parses events by
# `type`; reads ONLY the structured rate_limit_info + the terminal result's
# is_error — never greps content text (the #1770 false-positive class).
is_usage_limit_hit() {
  python3 - "$1" <<'PY'
import json, sys

rejected = False
result = None
for line in open(sys.argv[1], errors="replace"):
    if '"type"' not in line:
        continue
    try:
        o = json.loads(line)
    except Exception:
        continue
    t = o.get("type")
    if t == "rate_limit_event":
        rli = o.get("rate_limit_info") or {}
        # Primary window rejected AND overage isn't carrying the request.
        if rli.get("status") == "rejected" and not rli.get("isUsingOverage"):
            rejected = True
    elif t == "result":
        result = o

# A session that produced a non-error terminal result was NOT blocked, regardless
# of any rate_limit_event noise (overage/window allowed it through). Only treat as
# a usage-limit hit when the run failed (errored or no terminal result) AND a
# rejected rate_limit_event is present.
succeeded = result is not None and not result.get("is_error")
sys.exit(0 if (rejected and not succeeded) else 1)
PY
}

# --- main loop (skipped when this file is SOURCED, e.g. by test_preflight_recovery.sh) ---
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  # single-instance lock (atomic mkdir; stale = dead pid; supervisor is long-lived)
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

  while true; do
    open_count="$(gh issue list --state open --json number -q 'length' 2>/dev/null || echo -1)"
    if [ "$open_count" = "0" ]; then
      dirty_skips=0   # idle bypasses preflight() — don't carry a stale dirty count across the gap
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
fi
