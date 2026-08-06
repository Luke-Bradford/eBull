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
#   cat var/autonomy/status.md             # what it last did
#
# ⚠ Runs in its OWN git worktree. Two Claude instances on ~/Dev/eBull will
# clobber each other's uncommitted work — there is a prevention-log entry for
# exactly that race (2026-07-16).

set -uo pipefail

WORKTREE="${TA_LOOP_WORKTREE:-/Users/lukebradford/Dev/.ebull-autonomy}"
STATE_DIR="${TA_LOOP_STATE:-$WORKTREE/var/autonomy}"
PROMPT="$WORKTREE/scripts/autonomy/ta_loop_prompt.md"
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

log "=== ta_loop start (worktree=$WORKTREE, max=$MAX_ITERATIONS, cooldown=${COOLDOWN_SECONDS}s) ==="

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
  transcript="$STATE_DIR/iteration-$(date -u +%Y%m%dT%H%M%SZ).log"
  log "--- iteration $iteration start"

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

  if [[ $rc -eq 0 ]]; then
    consecutive_failures=0
    log "--- iteration $iteration OK (transcript: $(basename "$transcript"))"
  else
    consecutive_failures=$((consecutive_failures + 1))
    log "--- iteration $iteration FAILED rc=$rc (failures in a row: $consecutive_failures)"
  fi

  {
    echo "# TA loop status"
    echo
    echo "- iteration: **$iteration**"
    echo "- started: $started"
    echo "- finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- exit code: $rc"
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
