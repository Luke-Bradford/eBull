#!/usr/bin/env bash
# What is the TA loop doing RIGHT NOW?
#
# ⚠ Exists because the first version of this loop could not answer that. The
# transcript only filled at process exit and status.md only wrote at iteration
# end, so mid-iteration the only way to tell progress from a hang was to go and
# read git by hand. That is the same invisibility failure as the month-long
# silent PAUSE, one layer down — so the fix is a command that reads every
# signal at once.

set -uo pipefail

WORKTREE="${TA_LOOP_WORKTREE:-/Users/lukebradford/Dev/.ebull-autonomy}"
STATE_DIR="${TA_LOOP_STATE:-$WORKTREE/var/autonomy}"

echo "=== TA loop status @ $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo

# 1. Is it alive, and is it paused?
if [[ -f "$STATE_DIR/PAUSE" ]]; then
  echo "STATE: ** PAUSED ** (sentinel: $STATE_DIR/PAUSE — remove to resume)"
elif pgrep -f "ta_loop.sh" >/dev/null 2>&1; then
  if pgrep -f "claude -p" >/dev/null 2>&1; then
    pid=$(pgrep -f "claude -p" | head -1)
    elapsed=$(ps -o etime= -p "$pid" 2>/dev/null | tr -d ' ')
    echo "STATE: RUNNING — iteration in flight for ${elapsed:-?}"
  else
    echo "STATE: RUNNING — between iterations (cooldown)"
  fi
else
  echo "STATE: NOT RUNNING"
fi
echo

# 2. What has it actually produced? This is the signal that cannot lie — files
#    on disk and a branch name beat any self-report.
echo "--- worktree"
if [[ -d "$WORKTREE" ]]; then
  branch=$(git -C "$WORKTREE" branch --show-current 2>/dev/null)
  echo "branch : ${branch:-<detached>}"
  echo "head   : $(git -C "$WORKTREE" log --oneline -1 2>/dev/null)"
  changed=$(git -C "$WORKTREE" status --short 2>/dev/null | wc -l | tr -d ' ')
  echo "changed: $changed file(s)"
  git -C "$WORKTREE" status --short 2>/dev/null | head -10 | sed 's/^/         /'
else
  echo "(worktree missing: $WORKTREE)"
fi
echo

# 3. Open PRs it may be waiting on.
echo "--- open PRs"
gh pr list --state open --limit 5 --json number,title,headRefName \
  --jq '.[] | "  #\(.number)  \(.headRefName)  \(.title[0:60])"' 2>/dev/null \
  || echo "  (gh unavailable)"
echo

# 4. Recent driver activity.
echo "--- loop.log (last 8)"
tail -8 "$STATE_DIR/loop.log" 2>/dev/null | sed 's/^/  /' || echo "  (no log yet)"
echo

# 5. Live transcript. stream-json is one JSON object per line, so the tail is
#    only useful decoded — show the assistant's most recent text.
newest=$(ls -t "$STATE_DIR"/iteration-*.log 2>/dev/null | head -1)
if [[ -n "${newest:-}" ]]; then
  echo "--- latest transcript: $(basename "$newest") ($(wc -c < "$newest" | tr -d ' ') bytes)"
  tail -200 "$newest" 2>/dev/null | python3 -c '
import json, sys
texts = []
for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        event = json.loads(line)
    except ValueError:
        continue
    message = event.get("message") or {}
    for block in message.get("content") or []:
        if isinstance(block, dict):
            if block.get("type") == "text" and block.get("text", "").strip():
                texts.append(block["text"].strip())
            elif block.get("type") == "tool_use":
                texts.append(f"[tool: {block.get(\"name\")}]")
for entry in texts[-6:]:
    print("  " + entry[:220].replace("\n", " "))
' 2>/dev/null || tail -4 "$newest" | cut -c1-200 | sed 's/^/  /'
fi
