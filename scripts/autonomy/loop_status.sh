#!/usr/bin/env bash
# What are the autonomy loops doing RIGHT NOW?
#
# ⚠ Exists because the first version of this loop could not answer that. The
# transcript only filled at process exit and status.md only wrote at iteration
# end, so mid-iteration the only way to tell progress from a hang was to go and
# read git by hand. That is the same invisibility failure as the month-long
# silent PAUSE, one layer down — so the fix is a command that reads every
# signal at once.
#
# ⚠ MULTI-LOOP. The single-loop version matched `pgrep -f "ta_loop.sh"` and
# took `pgrep -f "claude -p" | head -1` for elapsed time. With a second loop
# running — or with the autonomy-engine's own loop alive on this Mac, which it
# was — both match the wrong process and the elapsed figure belongs to whoever
# started first. Every probe here is scoped to one loop's worktree path.
#
# USAGE
#   scripts/autonomy/loop_status.sh            # every registered loop
#   scripts/autonomy/loop_status.sh ta         # one of them

set -uo pipefail

# name|worktree|branch-globs
#
# ⚠ THREE fields, but `|` appears more than twice: the third field is the
# REMAINDER of the line, so any `|` after the second is part of the glob's own
# alternation rather than a fourth field. That is a real property of
# `IFS='|' read -r name worktree pr_glob` — the last variable takes what is
# left, delimiters included — not an accident, and it is written down here
# because the row does not look like it parses (#2324). Verified: the
# `ownership` row yields the full `fix/*|chore/ownership-*|docs/ownership-*`,
# not just `fix/*`. If it kept only the first, that loop's `chore/` and `docs/`
# PRs would vanish from its own status view.
#
# ⚠ The glob must match ONLY the branches that loop opens, or the section is
# worse than useless — it shows another loop's PR under this loop's heading and
# reads as "mine". `*` is never right here. `|` alternates; each loop's prompt
# fixes its branch naming, so these two lists have to move together with them.
LOOPS=(
  "ta|/Users/lukebradford/Dev/.ebull-autonomy|feature/2240-*"
  "ownership|/Users/lukebradford/Dev/.ebull-ownership|fix/*|chore/ownership-*|docs/ownership-*"
)

want="${1:-}"

report_loop() {
  local name="$1" worktree="$2" pr_glob="$3"
  local state_dir="$worktree/var/autonomy"
  local driver="$worktree/var/autonomy/bin/ta_loop.sh"

  echo "=============================================================="
  echo "=== loop: $name   ($worktree)"
  echo "=============================================================="

  # 1. Alive, paused, or stopped. Scoped to THIS loop's driver path, which is
  #    what makes the answer trustworthy when more than one loop exists.
  if [[ -f "$state_dir/PAUSE" ]]; then
    echo "STATE: ** PAUSED ** (sentinel: $state_dir/PAUSE — remove to resume)"
  elif pgrep -f "$driver" >/dev/null 2>&1; then
    # Elapsed comes from status.md's own `started:` line, not from a pgrep on
    # `claude -p` — that matched any Claude process on the machine.
    local started in_flight
    in_flight=$(grep -c "IN FLIGHT" "$state_dir/status.md" 2>/dev/null || echo 0)
    started=$(sed -n 's/^- started: //p' "$state_dir/status.md" 2>/dev/null | head -1)
    if [[ "$in_flight" -gt 0 && -n "$started" ]]; then
      local began now mins
      began=$(date -j -u -f "%Y-%m-%dT%H:%M:%SZ" "$started" +%s 2>/dev/null)
      now=$(date -u +%s)
      if [[ -n "$began" ]]; then
        mins=$(( (now - began) / 60 ))
        echo "STATE: RUNNING — iteration in flight ${mins}m (since $started)"
      else
        echo "STATE: RUNNING — iteration in flight (since $started)"
      fi
    else
      echo "STATE: RUNNING — between iterations (cooldown)"
    fi
  else
    echo "STATE: NOT RUNNING"
  fi
  echo

  # 2. What it has actually produced. Files on disk and a branch name beat any
  #    self-report.
  echo "--- worktree"
  if [[ -d "$worktree" ]]; then
    local branch changed
    branch=$(git -C "$worktree" branch --show-current 2>/dev/null)
    echo "branch : ${branch:-<detached>}"
    echo "head   : $(git -C "$worktree" log --oneline -1 2>/dev/null)"
    changed=$(git -C "$worktree" status --short 2>/dev/null | wc -l | tr -d ' ')
    echo "changed: $changed file(s)"
    git -C "$worktree" status --short 2>/dev/null | head -10 | sed 's/^/         /'
  else
    echo "(worktree missing: $worktree)"
  fi
  echo

  # 3. Open PRs it may be waiting on, narrowed to the branches it owns.
  echo "--- open PRs (headRef $pr_glob)"
  # Anchored: an unanchored `fix/.*` matches any branch merely CONTAINING
  # "fix/", which is how a filter starts lying quietly.
  local pattern
  pattern="^($(printf '%s' "$pr_glob" | sed 's/\*/.*/g'))$"
  gh pr list --state open --limit 20 --json number,title,headRefName \
    --jq ".[] | select(.headRefName | test(\"$pattern\")) | \"  #\(.number)  \(.headRefName)  \(.title[0:60])\"" 2>/dev/null \
    || echo "  (gh unavailable — check \`gh auth status\`)"
  echo

  # 4. Recent driver activity.
  echo "--- loop.log (last 8)"
  tail -8 "$state_dir/loop.log" 2>/dev/null | sed 's/^/  /' || echo "  (no log yet)"
  echo

  # 5. Live transcript. stream-json is one JSON object per line, so the tail is
  #    only useful decoded — show the assistant's most recent text.
  local newest
  newest=$(ls -t "$state_dir"/iteration-*.log 2>/dev/null | head -1)
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
  echo
}

echo "=== autonomy loops @ $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo

for entry in "${LOOPS[@]}"; do
  IFS='|' read -r name worktree pr_glob <<< "$entry"
  if [[ -n "$want" && "$want" != "$name" ]]; then
    continue
  fi
  report_loop "$name" "$worktree" "$pr_glob"
done
