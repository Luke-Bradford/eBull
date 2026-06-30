#!/usr/bin/env bash
# Create (idempotently) the dedicated git worktree the autonomy SUPERVISOR runs
# in, and install its launchd plist pointed there — so the loop never shares a
# working tree with the operator's interactive checkout (#1874). Two agents on
# one tree race on branch/index state (observed live: the loop's `git switch`
# yanked the tree off an operator branch; a session merged an operator PR).
#
# The worktree is PERSISTENT and loop/agent-specific → KEPT, reused across every
# session (each session cuts ephemeral feature branches off detached origin/main
# inside it). Branch-specific cleanup is worktree_gc.sh's job, not this one.
#
# Run from the primary checkout:
#   bash scripts/autonomy/setup_worktree.sh [WORKTREE_PATH]
# Default WORKTREE_PATH: a dotted sibling of the primary checkout (less visible
# to others on the machine, per operator preference).

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"
[ -f "$REPO/.claude/CLAUDE.md" ] || { echo "not the eBull repo root: $REPO" >&2; exit 1; }

WORKTREE="${1:-$(cd "$REPO/.." && pwd)/.eBull-autonomy}"
PLIST_SRC="$REPO/scripts/autonomy/com.ebull.autonomy.supervisor.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.ebull.autonomy.supervisor.plist"

echo "primary checkout : $REPO"
echo "loop worktree    : $WORKTREE"
[ "$WORKTREE" = "$REPO" ] && { echo "refuse: worktree path equals the primary checkout" >&2; exit 1; }

git fetch origin -q

# 1) Worktree, detached @ origin/main (never the `main` branch ref → no collision
#    with the primary checkout holding it). Idempotent: keep an existing one.
if git worktree list --porcelain | grep -Fxq "worktree $WORKTREE"; then
  echo "worktree already registered — leaving as-is (persistent/loop-specific)."
else
  git worktree add --detach "$WORKTREE" origin/main
  echo "worktree created (detached @ origin/main)."
fi

# 2) Logs dir must exist before launchd opens StandardOut/ErrorPath.
mkdir -p "$WORKTREE/var/autonomy-logs"

# 3) Install the plist pointed at the WORKTREE (not the primary checkout). The
#    template ships path-agnostic; __REPO__ is the worktree here.
sed "s#__REPO__#$WORKTREE#g" "$PLIST_SRC" > "$PLIST_DST"
echo "installed plist  -> $PLIST_DST (__REPO__ = $WORKTREE)"

cat <<EOF

Next (operator) — stop any supervisor bound to the OLD shared checkout, load the
worktree-pointed one (survives reboot via the plist's RunAtLoad):
  launchctl bootout   gui/\$(id -u)/com.ebull.autonomy.supervisor 2>/dev/null || true
  launchctl bootstrap gui/\$(id -u) "$PLIST_DST"
  launchctl list | grep autonomy.supervisor          # confirm loaded
  tail -f "$WORKTREE/var/autonomy-logs/supervisor.log"
EOF
