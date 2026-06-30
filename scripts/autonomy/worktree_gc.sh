#!/usr/bin/env bash
# Tidy the autonomy git worktrees + branches (#1874 lifecycle rule):
#   - KEEP the persistent loop/agent worktree (the supervisor's tree) — it's
#     reused across sessions, never torn down here.
#   - PRUNE stale worktree admin entries (a removed dir, or a branch-specific
#     worktree someone tore down) via `git worktree prune`.
#   - DELETE local feature branches already merged into origin/main (the loop's
#     leftovers; remote branches are deleted by safe_merge --delete-branch).
#
# Only fully-merged branches are removed (tip is an ancestor of origin/main), so
# this can never drop unmerged work. Safe to run anytime. Run from any checkout.
#
# Run:  bash scripts/autonomy/worktree_gc.sh

set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO" || exit 1

echo "== prune stale worktree admin entries =="
git worktree prune -v   # always safe — only drops admin entries for vanished dirs

# Branch deletion (`-D`, force) is gated ENTIRELY on a CURRENT origin/main: the
# only safety proof is `--is-ancestor <branch> origin/main`. If the fetch fails
# (offline) or origin/main can't resolve, a stale remote-tracking ref could say
# a branch is merged when the real upstream — possibly force-corrected — has not.
# So skip the whole phase rather than delete against a stale ref (Codex #1874).
echo "== delete local branches merged into origin/main =="
if ! git fetch origin -q 2>/dev/null || ! git rev-parse --verify -q origin/main >/dev/null 2>&1; then
  echo "  SKIP: 'git fetch origin' failed or origin/main unresolved — not deleting against a stale ref"
  echo "== remaining worktrees (loop/agent-specific = KEEP) =="
  git worktree list
  exit 0
fi
current="$(git branch --show-current 2>/dev/null || echo)"
deleted=0
while IFS= read -r b; do
  case "$b" in main|"$current"|'') continue ;; esac
  # Proven merged into origin/main → safe to force-delete the local ref. (`-d`
  # alone can refuse when the branch's upstream was already deleted by
  # safe_merge; the ancestor check is the real safety gate.)
  if git merge-base --is-ancestor "$b" origin/main 2>/dev/null; then
    git branch -D "$b" >/dev/null && { echo "  deleted merged branch: $b"; deleted=$((deleted + 1)); }
  fi
done < <(git for-each-ref --format='%(refname:short)' refs/heads)
echo "  ($deleted merged branch(es) removed)"

echo "== remaining worktrees (loop/agent-specific = KEEP) =="
git worktree list
