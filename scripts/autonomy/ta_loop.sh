#!/usr/bin/env bash
# eBull TA loop — deliberately dumb.
#
# WHY THIS IS DUMB ON PURPOSE
# ---------------------------
# The previous eBull loop ran through the autonomy-engine's supervisor and
# died on 2026-07-23 spinning on:
#
#   WARN cannot determine kind for in-flight 'coder' (state unreadable)
#        -- skipping this tick
#
# It wedged on its own state machine and nobody noticed for two weeks. So this
# has no state machine. One iteration = one `claude -p` invocation. If an
# iteration dies, the next one starts clean. There is nothing to become
# unreadable.
#
# USAGE
#   scripts/autonomy/ta_loop.sh            # run until stopped
#   touch var/autonomy/PAUSE               # graceful stop after current iteration
#   tail -f var/autonomy/loop.log          # watch
#   cat var/autonomy/status.md             # what it is doing / last did
#
# INSTALLED COPY — what launchd actually runs
#   mkdir -p <worktree>/var/autonomy/bin
#   cp scripts/autonomy/ta_loop.sh scripts/autonomy/ta_loop_prompt.md \
#      <worktree>/var/autonomy/bin/
#
# ⚠ Run the INSTALLED copy, never the tracked one, for anything unattended.
# The driver drives a worktree that changes branch every iteration; a driver
# read from a tracked path is deleted by its own `git checkout` the moment it
# branches off a commit older than itself. `/var/*` is gitignored, so a copy
# under var/autonomy/bin/ is invisible to every checkout and cannot be moved
# out from under a running loop.
#
# ⚠ Runs in its OWN git worktree. Two Claude instances on ~/Dev/eBull will
# clobber each other's uncommitted work — there is a prevention-log entry for
# exactly that race (2026-07-16).

set -uo pipefail

WORKTREE="${TA_LOOP_WORKTREE:-/Users/lukebradford/Dev/.ebull-autonomy}"
STATE_DIR="${TA_LOOP_STATE:-$WORKTREE/var/autonomy}"

# ⚠ The prompt is resolved next to THIS script, not at a fixed path inside the
# checkout. The driver drives a worktree whose branch changes every iteration,
# so anything it reads from a tracked path can vanish under it — check out a
# branch based on a commit predating this PR and the loop deletes its own
# prompt mid-run. Installed copies live in var/autonomy/bin/, which `/var/*`
# in .gitignore keeps out of git's reach entirely.
SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PROMPT="${TA_LOOP_PROMPT:-$SELF_DIR/ta_loop_prompt.md}"
PAUSE="$STATE_DIR/PAUSE"
LOG="$STATE_DIR/loop.log"
STATUS="$STATE_DIR/status.md"
MAX_ITERATIONS="${TA_LOOP_MAX:-0}"        # 0 = unbounded
COOLDOWN_SECONDS="${TA_LOOP_COOLDOWN:-60}"

mkdir -p "$STATE_DIR"

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG"; }

if [[ ! -d "$WORKTREE/.git" && ! -f "$WORKTREE/.git" ]]; then
  log "FATAL worktree $WORKTREE is not a git checkout"
  exit 1
fi
if [[ ! -f "$PROMPT" ]]; then
  log "FATAL prompt not found at $PROMPT"
  exit 1
fi

# ⚠ ONE driver per worktree. The dedicated worktree stops this loop clobbering
# ~/Dev/eBull; it does nothing about a second copy of the loop itself, and
# launchd's KeepAlive makes that reachable in one step — agent running, human
# starts a manual run, two Claudes in one checkout (prevention log 2026-07-16).
# mkdir is the atomic test-and-set available everywhere; macOS ships no flock(1).
LOCK="$STATE_DIR/loop.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  # ⚠ `mkdir` and the pid write are two operations, so a holder that has taken
  # the lock microseconds ago has not written its pid yet. Reading that gap as
  # "stale" would have the newcomer steal the lock from a live loop — the exact
  # race the lock exists to prevent, reintroduced by the lock. So: retry the
  # read for a second, and only fall back to the directory's age.
  holder=""
  for _ in 1 2 3 4 5; do
    holder="$(cat "$LOCK/pid" 2>/dev/null || true)"
    [[ -n "$holder" ]] && break
    sleep 0.2
  done

  if [[ -n "$holder" ]]; then
    if kill -0 "$holder" 2>/dev/null; then
      log "ABORT another ta_loop.sh holds $LOCK (pid $holder)"
      exit 1
    fi
    log "clearing stale lock $LOCK (holder $holder is gone)"
  else
    # No pid after a second. Either a holder was killed between mkdir and the
    # write, or the lock predates a reboot. Age decides: under a minute is
    # assumed live, because refusing to start is recoverable and stealing is
    # not.
    if [[ -z "$(find "$LOCK" -maxdepth 0 -mmin +1 2>/dev/null)" ]]; then
      log "ABORT $LOCK exists with no pid and is under a minute old -- assuming a live holder"
      exit 1
    fi
    log "clearing stale lock $LOCK (no pid file, older than a minute)"
  fi

  rm -rf "$LOCK"
  mkdir "$LOCK" || { log "FATAL cannot take lock $LOCK"; exit 1; }
fi
echo "$$" > "$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT

log "=== ta_loop start (worktree=$WORKTREE, max=$MAX_ITERATIONS, cooldown=${COOLDOWN_SECONDS}s, pid=$$) ==="

iteration=0
consecutive_failures=0

while true; do
  # ⚠ PAUSE is checked FIRST and always announced. The engine's loop sat
  # paused for a month printing the same line into a log nobody read; this one
  # also writes it into status.md, which is the file a human actually opens.
  if [[ -f "$PAUSE" ]]; then
    log "PAUSED by sentinel $PAUSE -- remove it to resume"
    { echo "# TA loop: PAUSED"; echo; echo "Sentinel present: \`$PAUSE\`"; echo; echo "Remove it to resume. Last update $(date -u +%Y-%m-%dT%H:%M:%SZ)."; } > "$STATUS"
    sleep "$COOLDOWN_SECONDS"
    continue
  fi

  if [[ "$MAX_ITERATIONS" -gt 0 && "$iteration" -ge "$MAX_ITERATIONS" ]]; then
    log "reached max iterations ($MAX_ITERATIONS) -- stopping"
    break
  fi

  iteration=$((iteration + 1))
  started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # pid+iteration suffix: the timestamp alone is 1-second resolution, so a
  # fast-failing iteration could otherwise overwrite the transcript that
  # explains why it failed.
  transcript="$STATE_DIR/iteration-$(date -u +%Y%m%dT%H%M%SZ)-$$.$iteration.log"
  log "--- iteration $iteration start"

  # ⚠ status.md is written BEFORE the iteration as well as after. Written only
  # at the end, the file a human is told to open does not exist at all during
  # the first run — which is the same "instrumentation is silent exactly when
  # you need it" failure as the month-long unnoticed PAUSE.
  {
    echo "# TA loop status"
    echo
    echo "- iteration: **$iteration** — IN FLIGHT"
    echo "- started: $started"
    echo "- pid: $$"
    echo "- transcript: \`$transcript\`"
  } > "$STATUS"

  # ⚠ NOT piped into head/tail. A pipe returns the pipe's status and buffers
  # the output, which cost 7 minutes of misdiagnosis on 2026-08-05. Redirect
  # to a file; read the file.
  # ⚠ --output-format=stream-json so the transcript fills LIVE, not at exit.
  # The first version buffered everything until the process ended, which made
  # "is it progressing?" unanswerable from the loop's own instrumentation —
  # you had to go and read git. That is the same invisibility failure as the
  # month-long silent PAUSE, one layer down.
  ( cd "$WORKTREE" && claude -p "$(cat "$PROMPT")" \
        --permission-mode acceptEdits \
        --output-format=stream-json --verbose --include-partial-messages ) \
      > "$transcript" 2>&1
  rc=$?

  # ⚠ rc is not the whole answer under --output-format=stream-json. The stream
  # is a sequence of JSON events and its LAST line is the result event, which
  # carries the task-level verdict independently of how the process exited.
  # Verified 2026-08-06 against the installed CLI: a successful run ends with
  # one object containing "type":"result" and "is_error":false, and exits 0.
  # Require both, so a stream that terminates tidily on a failed task still
  # counts toward the 3-strike stop rather than looking like progress.
  # (grep is the DOWNSTREAM command here and its status is the one being read
  # — this is not the `gate | tail` pattern that hides an exit code.)
  result_ok=0
  if tail -1 "$transcript" | grep -q '"is_error":false'; then
    result_ok=1
  fi

  if [[ $rc -eq 0 && $result_ok -eq 1 ]]; then
    consecutive_failures=0
    log "--- iteration $iteration OK (transcript: $(basename "$transcript"))"
  else
    consecutive_failures=$((consecutive_failures + 1))
    log "--- iteration $iteration FAILED rc=$rc result_ok=$result_ok (failures in a row: $consecutive_failures)"
  fi

  {
    echo "# TA loop status"
    echo
    echo "- iteration: **$iteration**"
    echo "- started: $started"
    echo "- finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- exit code: $rc"
    echo "- result event ok: $result_ok"
    echo "- consecutive failures: $consecutive_failures"
    echo "- transcript: \`$transcript\`"
    echo
    echo "## Last 40 lines"
    echo '```'
    tail -40 "$transcript" 2>/dev/null
    echo '```'
  } > "$STATUS"

  # ⚠ Back off on repeated failure rather than hammering. Three in a row means
  # something structural (auth, quota, a wedged repo) that another immediate
  # attempt will not fix.
  if [[ $consecutive_failures -ge 3 ]]; then
    log "STOPPING: 3 consecutive failures -- see $transcript"
    break
  fi

  sleep "$COOLDOWN_SECONDS"
done

log "=== ta_loop exit after $iteration iteration(s) ==="
