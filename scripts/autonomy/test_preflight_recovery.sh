#!/usr/bin/env bash
# Scenario test for supervisor.sh preflight() dirty-tree auto-recovery (#1801).
# Sources the REAL supervisor.sh (its main loop is guarded by a BASH_SOURCE==$0
# check, so sourcing only defines functions) and exercises preflight() against a
# throwaway repo with a local bare origin. Proves: grace skips, K-th-skip stash
# recovery, in-progress-op skip, and counter reset — no re-implementation drift.
#
# Run:  bash scripts/autonomy/test_preflight_recovery.sh   (exit 0 = all pass)

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source the real script. It cd's to the eBull repo root and sets globals; we
# override the noisy bits and move into a throwaway repo below.
# shellcheck source=/dev/null
source "$HERE/supervisor.sh"
# shellcheck disable=SC2034  # SUPLOG is read by the sourced preflight() (stash redirect)
SUPLOG=/dev/null          # don't append to the real supervisor log
log() { :; }             # silence preflight's log() during the test (invoked via sourced preflight)

fails=0
check() { # check <desc> <expected> <actual>
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
origin="$tmp/origin.git"; work="$tmp/work"
git init -q --bare "$origin"
git -c init.defaultBranch=main init -q "$work"
cd "$work" || exit 1
git config user.email "t@t.t"; git config user.name "t"
git commit -q --allow-empty -m init
git branch -M main
git remote add origin "$origin"
git push -q -u origin main 2>/dev/null

# Scenario 1: clean tree → proceed (0), counter stays 0.
dirty_skips=0
preflight; rc=$?
check "clean tree proceeds" 0 "$rc"
check "clean tree leaves counter 0" 0 "$dirty_skips"

# Scenario 2: first dirty skip is grace (return 2, no stash).
dirty_skips=0
echo "wip" > wip.txt
preflight; rc=$?
check "1st dirty skip returns 2 (grace)" 2 "$rc"
check "1st dirty skip increments counter" 1 "$dirty_skips"
check "1st dirty skip does NOT stash" 0 "$(git stash list | wc -l | tr -d ' ')"

# Scenario 3: K-th (2nd) dirty skip stashes WIP, cleans tree, resets counter, proceeds.
preflight; rc=$?
check "K-th dirty skip proceeds (0)" 0 "$rc"
check "K-th dirty skip resets counter" 0 "$dirty_skips"
check "K-th dirty skip created a stash" 1 "$(git stash list | wc -l | tr -d ' ')"
check "K-th dirty skip stash message tagged" 1 "$(git stash list | grep -c 'autonomy-preflight-recovery')"
check "tree clean after recovery" "" "$(git status --porcelain)"
git stash drop -q 2>/dev/null  # untracked wip recovered into stash; clear it

# Scenario 4: in-progress git op (REVERT_HEAD) always skips, never stashes, even when dirty.
dirty_skips=5            # already past K — recovery would fire if the guard were absent
echo "midrevert" > wip2.txt
: > "$(git rev-parse --git-dir)/REVERT_HEAD"
preflight; rc=$?
check "in-progress op returns 2" 2 "$rc"
check "in-progress op does NOT stash" 0 "$(git stash list | wc -l | tr -d ' ')"
rm -f "$(git rev-parse --git-dir)/REVERT_HEAD" wip2.txt

# Scenario 5: a single dirty skip then a clean tree resets the counter to 0.
dirty_skips=0
echo "wip3" > wip3.txt
preflight >/dev/null 2>&1   # counter -> 1
check "counter is 1 after one dirty skip" 1 "$dirty_skips"
rm -f wip3.txt
preflight; rc=$?
check "clean observation resets counter" 0 "$dirty_skips"
check "clean observation proceeds (0)" 0 "$rc"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
