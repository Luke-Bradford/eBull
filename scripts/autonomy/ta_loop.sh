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
#   cp scripts/autonomy/ta_loop.sh <worktree>/var/autonomy/bin/
#
# ⚠ The DRIVER is the only thing you copy. The prompt installs itself: every
# iteration the driver materialises it from git (see PROMPT_SOURCE below) and
# replaces the installed copy when it differs. Copying a prompt by hand is what
# broke for seven days (#2658) — one existed, nothing compared it to anything.
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

# ⚠ #2658: the installed prompt is a COPY, and for seven days nothing compared
# it to anything. `.autonomy/loop_prompt.md` is the file everyone maintains and
# reviews; `var/autonomy/bin/ta_loop_prompt.md` is the file the loop reads. A
# merge to the first changed nothing about the second, silently, for 33
# iterations and ~$543. So the copy is now DERIVED every iteration instead of
# installed by hand.
#
# Read from the git OBJECT STORE, not the worktree's filesystem: the branch
# changes every iteration, so `$WORKTREE/.autonomy/loop_prompt.md` is whatever
# the current branch happens to hold (or absent, on a branch older than the
# file). `origin/main:` is branch-independent AND is the merged, reviewed text —
# never a local edit that has not been through the bot.
PROMPT_REF="${TA_LOOP_PROMPT_REF:-origin/main}"
# Defaulted from the installed prompt's NAME so the fix binds with no plist
# change and no launchd re-bootstrap — needing an operator step is how #2658
# survived being "fixed" once already (#2604 re-aimed a file nothing read).
case "$(basename "$PROMPT")" in
  ownership_loop_prompt.md) _default_prompt_source="scripts/autonomy/ownership_loop_prompt.md" ;;
  *)                        _default_prompt_source=".autonomy/loop_prompt.md" ;;
esac
PROMPT_SOURCE="${TA_LOOP_PROMPT_SOURCE:-$_default_prompt_source}"
PAUSE="$STATE_DIR/PAUSE"
LOG="$STATE_DIR/loop.log"
STATUS="$STATE_DIR/status.md"
MAX_ITERATIONS="${TA_LOOP_MAX:-0}"        # 0 = unbounded
COOLDOWN_SECONDS="${TA_LOOP_COOLDOWN:-60}"

# ⚠ An installed driver lives at <worktree>/var/autonomy/bin, so its own path
# states which loop it is. If that disagrees with TA_LOOP_WORKTREE, the agent
# is about to drive somebody else's checkout — refuse rather than discover it
# through the other loop's lock. Observed 2026-08-06 standing up the second
# loop: the plist set WorkingDirectory and the prompt but not the worktree, so
# the ownership agent drove the TA worktree and hit its lock three times in 21
# seconds under KeepAlive. The lock held, but nothing SAID what was wrong.
#
# ⚠ BOTH homes count. The first version stripped only `/var/autonomy/bin`, and
# `${x%suffix}` returns `x` UNCHANGED when the suffix is absent — so for a
# directly-run tracked script the two sides were equal, the first conjunct was
# false, and the guard was skipped entirely (#2324). The bypass was the tracked
# path, which is the one a human runs by hand: exactly the case where a wrong
# TA_LOOP_WORKTREE is most likely and least expected.
inferred_worktree=""
case "$SELF_DIR" in
  */var/autonomy/bin) inferred_worktree="${SELF_DIR%/var/autonomy/bin}" ;;
  */scripts/autonomy) inferred_worktree="${SELF_DIR%/scripts/autonomy}" ;;
esac
# A checkout at the filesystem root strips to the empty string rather than to
# "/", and the emptiness test below would then read as "no path to infer" and
# let the guard pass — silently, which is the one behaviour this guard exists
# to rule out. Absurd layout, one line to close.
[[ -z "$inferred_worktree" && -n "${SELF_DIR:-}" ]] && case "$SELF_DIR" in
  /var/autonomy/bin|/scripts/autonomy) inferred_worktree="/" ;;
esac
if [[ -n "$inferred_worktree" && "$inferred_worktree" != "$WORKTREE" ]]; then
  echo "FATAL running from $inferred_worktree but TA_LOOP_WORKTREE=$WORKTREE" >&2
  echo "      refusing to drive another loop's checkout -- set TA_LOOP_WORKTREE to match," >&2
  echo "      or run the driver installed under that worktree's var/autonomy/bin" >&2
  exit 1
fi

mkdir -p "$STATE_DIR"

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG"; }

if [[ ! -d "$WORKTREE/.git" && ! -f "$WORKTREE/.git" ]]; then
  log "FATAL worktree $WORKTREE is not a git checkout"
  exit 1
fi

# sha256 of a file, or the empty string when it cannot be read. Not piped into
# anything whose status is then read — `cut` is the downstream command here.
file_sha() { shasum -a 256 < "$1" 2>/dev/null | cut -d' ' -f1; }

# Extract "<ref>:<path>" into $2. Returns non-zero (and leaves nothing behind)
# when git cannot produce it. `>` is a redirect, not a pipe, so $? is git's.
git_show_to() {
  git -C "$WORKTREE" show "$1" > "$2" 2>/dev/null || { rm -f "$2"; return 1; }
  [[ -s "$2" ]] || { rm -f "$2"; return 1; }
}

# One line, rewritten every iteration and echoed into status.md — the whole
# point of #2658 is that a human opening status.md can see WHICH prompt ran.
prompt_status="not yet checked"

# ⚠ `git show origin/main:…` reads the LOCAL remote-tracking ref and contacts
# nothing. A merge landing on GitHub — which is where every prompt change lands,
# since the loop merges from a different branch — leaves this worktree's
# origin/main untouched, so without this the sync would compare the installed
# copy against a snapshot as old as the last fetch and call it "in sync". That
# is #2658 again one layer down: a check that passes because its reference point
# is stale. Caught by Codex at checkpoint 2.
#
# Best-effort: a network failure warns and falls through to the last-known ref
# rather than halting the loop, on the same reasoning as the UNVERIFIED path.
refresh_prompt_ref() {
  local remote branch
  case "$PROMPT_REF" in
    */*/*) return 0 ;;   # refs/remotes/... or a path-shaped ref: not <remote>/<branch>
    */*)   remote="${PROMPT_REF%%/*}"; branch="${PROMPT_REF#*/}" ;;
    *)     return 0 ;;   # a bare sha or tag — nothing to fetch
  esac
  git -C "$WORKTREE" remote get-url "$remote" >/dev/null 2>&1 || return 0
  # Explicit refspec: `git fetch <remote> <branch>` updates the tracking ref only
  # when the configured refspec happens to match, and this must not depend on
  # how the remote was set up.
  git -C "$WORKTREE" fetch --quiet "$remote" \
      "+refs/heads/$branch:refs/remotes/$remote/$branch" 2>/dev/null \
    || log "WARN could not fetch $remote/$branch -- comparing against the last-known $PROMPT_REF"
}

sync_prompt() {
  local tmp canon_sha installed_sha
  refresh_prompt_ref
  installed_sha="$(file_sha "$PROMPT")"
  # Same directory as $PROMPT so the replacement below is a rename, not a copy:
  # an iteration must never read a half-written prompt.
  tmp="$PROMPT.canonical.$$"
  if ! git_show_to "$PROMPT_REF:$PROMPT_SOURCE" "$tmp"; then
    # Not fatal. A missing remote-tracking ref or an unfetched repo is an
    # environment problem, and halting the loop over it is a self-inflicted
    # outage — but it is announced, because running an UNVERIFIED prompt is
    # exactly the state this function exists to make visible.
    prompt_status="UNVERIFIED against $PROMPT_REF:$PROMPT_SOURCE — ran installed ${installed_sha:0:12}"
    log "WARN prompt $prompt_status"
    return
  fi
  canon_sha="$(file_sha "$tmp")"
  if [[ -n "$installed_sha" && "$canon_sha" == "$installed_sha" ]]; then
    rm -f "$tmp"
    prompt_status="in sync with $PROMPT_REF:$PROMPT_SOURCE (${canon_sha:0:12})"
    log "prompt $prompt_status"
    return
  fi
  if mv "$tmp" "$PROMPT"; then
    prompt_status="RESYNCED from $PROMPT_REF:$PROMPT_SOURCE — ${installed_sha:0:12} -> ${canon_sha:0:12}"
    log "prompt $prompt_status"
  else
    rm -f "$tmp"
    prompt_status="STALE ${installed_sha:0:12} != $PROMPT_REF ${canon_sha:0:12}, and $PROMPT could not be replaced"
    log "WARN prompt $prompt_status"
  fi
}

# #2658 item 4: the prompt was not the only thing installed once and never
# looked at again. The driver is checked but NEVER auto-replaced — this process
# is already running the old bytes, so swapping the file would change what a
# reader sees without changing what ran, which is worse than being stale.
check_driver_freshness() {
  local tmp installed tracked
  tmp="$STATE_DIR/.driver.canonical.$$"
  if ! git_show_to "$PROMPT_REF:scripts/autonomy/ta_loop.sh" "$tmp"; then
    log "WARN driver UNVERIFIED against $PROMPT_REF:scripts/autonomy/ta_loop.sh"
    return
  fi
  installed="$(file_sha "${BASH_SOURCE[0]}")"
  tracked="$(file_sha "$tmp")"
  rm -f "$tmp"
  if [[ "$installed" == "$tracked" ]]; then
    log "driver in sync with $PROMPT_REF (${installed:0:12})"
  else
    log "WARN driver STALE — installed ${installed:0:12} != $PROMPT_REF ${tracked:0:12};"
    log "WARN   re-copy scripts/autonomy/ta_loop.sh into $SELF_DIR and restart (a running driver is not auto-replaced)"
  fi
}


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

# ⚠ AFTER the lock. A second driver that is about to ABORT must not rewrite the
# live one's prompt on its way out — the sync is a state mutation, and only the
# lock holder may make it.
#
# The prompt sync runs before the existence check on purpose: a fresh install is
# now one `cp` of the driver, and the first sync writes the prompt beside it.
# sync_prompt first: it refreshes the remote-tracking ref that the driver check
# then compares against.
sync_prompt
check_driver_freshness

if [[ ! -f "$PROMPT" ]]; then
  log "FATAL prompt not found at $PROMPT and none recoverable from $PROMPT_REF:$PROMPT_SOURCE"
  exit 1
fi

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

  # ⚠ EVERY iteration, not once at startup. This loop stays up for days; a
  # prompt merged on day three has to reach the iteration after it, and the
  # seven-day #2658 stall is what happens when nothing re-reads the source.
  sync_prompt

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
    echo "- prompt: $prompt_status"
  } > "$STATUS"

  # ⚠ NOT piped into head/tail. A pipe returns the pipe's status and buffers
  # the output, which cost 7 minutes of misdiagnosis on 2026-08-05. Redirect
  # to a file; read the file.
  # ⚠ --output-format=stream-json so the transcript fills LIVE, not at exit.
  # The first version buffered everything until the process ended, which made
  # "is it progressing?" unanswerable from the loop's own instrumentation —
  # you had to go and read git. That is the same invisibility failure as the
  # month-long silent PAUSE, one layer down.
  # ⚠ stderr goes to its OWN file. Merged into the transcript it lands wherever
  # it lands — the CLI's settings warnings arrive before the first event, and
  # nothing stops a shutdown message arriving after the last one. The transcript
  # is parsed below, so anything non-JSON in it is a correctness problem, not
  # noise. Verified 2026-08-06: a plain run writes 157 bytes to stderr.
  ( cd "$WORKTREE" && claude -p "$(cat "$PROMPT")" \
        --permission-mode acceptEdits \
        --output-format=stream-json --verbose --include-partial-messages ) \
      > "$transcript" 2> "$transcript.err"
  rc=$?

  # ⚠ rc is not the whole answer under --output-format=stream-json. The stream
  # is a sequence of JSON events and its LAST line is the result event, which
  # carries the task-level verdict independently of how the process exited.
  # Verified 2026-08-06 against the installed CLI: a successful run ends with
  # one object containing "type":"result" and "is_error":false, and exits 0.
  # Require both, so a stream that terminates tidily on a failed task still
  # counts toward the 3-strike stop rather than looking like progress.
  # ⚠ Find the result event by TYPE; do not trust the last line to be it, and
  # do not match `is_error` anywhere in the stream. Under
  # --include-partial-messages the transcript carries stream_event, message,
  # text_delta and system events too, and any of them may grow an `is_error`
  # field — matching the wrong event would report a verdict for something that
  # is not the task. Anchored to a whole-line JSON object so a nested
  # occurrence inside a larger payload cannot match.
  # (grep is the DOWNSTREAM command here and its status is the one being read
  # — this is not the `gate | tail` pattern that hides an exit code.)
  # jq when it is there (/usr/bin/jq ships with macOS, so it normally is):
  # `.type` is then the TOP-LEVEL field and a nested "type":"result" inside
  # quoted tool output cannot match at all. `fromjson?` skips any line that is
  # not valid JSON rather than aborting the scan. The regex below is the
  # fallback, not the primary.
  result_ok=0
  if command -v jq >/dev/null 2>&1; then
    verdict="$(jq -R -r 'fromjson? | select(.type=="result") | .is_error' "$transcript" 2>/dev/null | tail -1)"
    [[ "$verdict" == "false" ]] && result_ok=1
  else
    result_line="$(grep -E '^\{.*"type":"result".*\}$' "$transcript" | tail -1)"
    if [[ -n "$result_line" ]] && printf '%s\n' "$result_line" | grep -q '"is_error":false'; then
      result_ok=1
    fi
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
    echo "- prompt: $prompt_status"
    echo
    echo "## Last 40 lines"
    echo '```'
    tail -40 "$transcript" 2>/dev/null
    echo '```'
    # stderr is its own file now, so it needs its own window or it becomes the
    # thing nobody looks at.
    if [[ -s "$transcript.err" ]]; then
      echo
      echo "## stderr (last 20)"
      echo '```'
      tail -20 "$transcript.err" 2>/dev/null
      echo '```'
    fi
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
