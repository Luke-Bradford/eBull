#!/usr/bin/env bash
# Autonomy SUPERVISOR — run board-drain sessions back-to-back for days,
# unattended, with usage-limit backoff. This is the "walk away, come back to the
# board worked through" entry point.
#
# Loop: check the board → run ONE fresh headless `claude` session (drains as many
# tickets as fit in its context) → classify the outcome → wait → repeat, forever:
#   - usage/rate limit hit  → sleep until the API-reported reset time (parsed
#                             from the rejected rate_limit_event, persisted to
#                             disk so a later no-event fast-fail still wakes at
#                             the right moment); exponential backoff is only the
#                             fallback when no reset signal is available. NOTE:
#                             the supervisor is plain bash — only the `claude`
#                             CHILD is rate-limited; the parent that decides when
#                             to wake never is, so it always survives to retry.
#   - other error           → exponential backoff (capped 1h).
#   - clean finish          → short pace, next session.
#   - board empty           → idle-poll every 30 min.
#
# Kept alive across crashes/reboots by launchd KeepAlive (com.ebull.autonomy.
# supervisor.plist). Single instance via lock. Each session starts on clean
# latest origin/main in DETACHED HEAD (preflight); a dirty tree from a crash
# aborts that iteration rather than building on half-done work.
#
# Runs in its OWN dedicated git worktree (scripts/autonomy/setup_worktree.sh) so
# it never shares a working tree with the operator's interactive checkout — two
# agents on one tree race on branch/index state (#1874).
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
RESET_STATE="$LOGDIR/.last_usage_reset"   # epoch secs of the last API-reported rate-limit reset

# --- timing knobs (seconds) ---
PACE=120                    # gap between back-to-back clean sessions
EMPTY_IDLE=1800            # board empty → poll every 30 min
ERR_BACKOFF_START=300; ERR_BACKOFF_MAX=3600
# Exponential backoff is now only the FALLBACK for a usage-limit hit with no
# parseable reset time (see compute_limit_wait). When the API reports a reset we
# sleep until it directly — which also handles the WEEKLY window (>5h), where the
# old 5h cap below would wake too early, fail, and sleep again.
LIMIT_BACKOFF_START=1800; LIMIT_BACKOFF_MAX=18000   # 30 min → cap 5 h (fallback only)
LIMIT_RESET_MAX_HORIZON=691200   # 8 days: reject implausibly-far reset epochs as parse garbage
PREFLIGHT_RECOVERY_AFTER=2  # consecutive dirty-tree skips before stashing WIP + proceeding (#1801)
dirty_skips=0               # supervisor-global counter (reset on any clean-tree observation)

FALLBACK_MODEL="claude-sonnet-4-6"   # keep working if the primary model is throttled
SAFETY="Unattended run. HARD RULES: never execute/approve/simulate a trade, never POST order endpoints, never touch the kill-switch, never close a position; merge ONLY via scripts/autonomy/safe_merge.sh; never push --no-verify; never restart the :8000/:5173 tasks. Follow .claude/CLAUDE.md and scripts/autonomy/loop_prompt.md exactly."

log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$SUPLOG"; }

# Preflight: put the tree on clean, latest origin/main (detached HEAD — see the
# git switch below) or return 2 to skip this iteration. Extracted as its own
# function so the dirty-tree recovery is unit-testable against a throwaway repo
# (test_preflight_recovery.sh) without the loop.
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
  # Detached HEAD at origin/main — NOT the local `main` branch ref. The loop runs
  # in its own dedicated git worktree (scripts/autonomy/setup_worktree.sh): a
  # linked worktree may not check out a branch already checked out in another
  # worktree (e.g. the operator's primary checkout sitting on main), and the loop
  # wants the latest PUSHED main regardless. Sessions still cut their feature
  # branches off this commit, so nothing downstream changes.
  git switch --detach origin/main -q 2>>"$SUPLOG" || { log "preflight: switch to origin/main failed"; return 2; }
  [ -z "$(git status --porcelain)" ] || { log "preflight: tree dirty on origin/main — skip"; return 2; }
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
    # Persist the API-reported reset time (if the rejected event carried one) so
    # the main loop can sleep until exactly then.
    local epoch; epoch="$(extract_reset_epoch "$log_file")"
    if [ -n "$epoch" ]; then
      printf '%s\n' "$epoch" >"$RESET_STATE"
    fi
    return 3
  fi
  # No structured rejection in THIS log, but the child failed AND a sane future
  # reset marker still stands (no clean session has cleared it since the last
  # block). That's almost certainly the same ongoing limit window emitting no
  # event this time — classify as a usage-limit hit so the loop waits until the
  # recorded reset instead of burning the generic-error backoff. The marker
  # self-expires (compute_limit_wait rejects a past/garbage epoch), so a genuine
  # post-reset error falls through to normal error handling.
  if [ "$rc" -ne 0 ] && compute_limit_wait >/dev/null; then
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

# Extract the API-reported reset time from the LAST rejected rate_limit_event in
# the session log and print it as epoch-seconds (nothing if absent/unparseable).
# Defensive on field name + format — the stream-json envelope is not contract-
# stable, so accept any "*reset*" key (ISO-8601 or epoch s/ms) plus a relative
# "retry_after"/"retryAfter" seconds value. Sanity-bounding (future, <8d) is done
# by the bash caller (compute_limit_wait), not here.
extract_reset_epoch() {
  python3 - "$1" <<'PY'
import json, math, sys, time
from datetime import datetime, timezone

def to_epoch(v):
    """Coerce a value to absolute epoch-seconds, or None."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if not math.isfinite(x):              # inf/nan → int() would OverflowError
            return None
        if x > 1e12:      # milliseconds
            x /= 1000.0
        return int(x) if x > 1e9 else None   # plausibly an absolute epoch
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:                                  # bare numeric string?
            x = float(s)
            if math.isfinite(x):
                if x > 1e12:
                    x /= 1000.0
                if x > 1e9:
                    return int(x)
        except ValueError:
            pass
        try:                                  # ISO-8601 (tolerate trailing Z)
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None
    return None

reset = None
for line in open(sys.argv[1], errors="replace"):
    if '"type"' not in line:
        continue
    try:
        o = json.loads(line)
    except Exception:
        continue
    if o.get("type") != "rate_limit_event":
        continue
    rli = o.get("rate_limit_info") or {}
    if rli.get("status") != "rejected" or rli.get("isUsingOverage"):
        continue
    for k, val in rli.items():
        kl = k.lower()
        if kl in ("retryafter", "retry_after"):
            secs = None
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                secs = float(val)
            elif isinstance(val, str):
                try:                              # string-valued seconds, e.g. "1800"
                    secs = float(val.strip())
                except ValueError:
                    secs = None
            if secs is not None and math.isfinite(secs):
                reset = int(time.time() + secs)
        elif "reset" in kl:
            e = to_epoch(val)
            if e is not None:
                reset = e

if reset is not None:
    print(reset)
PY
}

# Seconds to sleep on a usage-limit hit, derived from the persisted API reset
# time. Prints the wait + exits 0 when a SANE future reset is on record
# (now < reset <= now + 8d); exits 1 otherwise so the caller uses exponential
# backoff. Bounding here guards against a stale/garbage epoch sleeping forever.
compute_limit_wait() {
  [ -f "$RESET_STATE" ] || return 1
  local reset now
  reset="$(cat "$RESET_STATE" 2>/dev/null)"
  case "$reset" in
    ''|*[!0-9]*) return 1 ;;   # must be a bare integer epoch
  esac
  now="$(date +%s)"
  if [ "$reset" -gt "$now" ] && [ "$reset" -le "$((now + LIMIT_RESET_MAX_HORIZON))" ]; then
    echo "$((reset - now))"
    return 0
  fi
  return 1
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
         rm -f "$RESET_STATE"   # capacity confirmed back — drop any stale reset marker
         sleep "$PACE" ;;
      3) jitter=$((RANDOM % 120))
         if reset_wait="$(compute_limit_wait)"; then
           reset_wait=$((reset_wait + jitter))
           log "USAGE LIMIT — sleeping ${reset_wait}s until API-reported reset, then retry"
           sleep "$reset_wait"
           limit_backoff=$LIMIT_BACKOFF_START   # precise wake — reset the fallback ladder
         else
           log "USAGE LIMIT (no reset signal) — exp backoff $((limit_backoff + jitter))s then retry"
           sleep $((limit_backoff + jitter))
           limit_backoff=$(( limit_backoff*2 < LIMIT_BACKOFF_MAX ? limit_backoff*2 : LIMIT_BACKOFF_MAX ))
         fi ;;
      2) log "preflight skip — wait ${ERR_BACKOFF_START}s"; sleep "$ERR_BACKOFF_START" ;;
      *) log "session error (rc=$outcome) — backoff ${err_backoff}s"
         sleep "$err_backoff"
         err_backoff=$(( err_backoff*2 < ERR_BACKOFF_MAX ? err_backoff*2 : ERR_BACKOFF_MAX )) ;;
    esac
  done
fi
