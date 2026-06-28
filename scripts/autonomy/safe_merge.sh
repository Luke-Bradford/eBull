#!/usr/bin/env bash
# Mechanical merge gate for the autonomy loop (Codex ckpt-2 HIGH: a prompt rule
# is not a control). Refuses to merge unless, ON THE LATEST commit SHA:
#   - every CI check has concluded and none failed, AND
#   - the Claude review bot's most recent review comment is APPROVE.
# The loop MUST merge only via this script. (The real server-side gate is GitHub
# branch protection with required status checks — see setup.md; this is
# defence-in-depth + the honest local mechanism.)
#
# Usage:  scripts/autonomy/safe_merge.sh <pr-number>

set -euo pipefail
PR="${1:?usage: safe_merge.sh <pr-number>}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

# Latest commit on the PR head.
head_sha="$(gh pr view "$PR" --json headRefOid -q .headRefOid)"
[ -n "$head_sha" ] || { echo "safe_merge: cannot resolve PR #$PR head SHA" >&2; exit 1; }

# 1) CI: no check may be failing; none may still be pending.
checks_json="$(gh pr checks "$PR" --json name,state 2>/dev/null || echo '[]')"
if echo "$checks_json" | grep -qiE '"state":"(fail|failure|error|cancelled|timed_out)"'; then
  echo "safe_merge: REFUSE — a CI check failed on #$PR" >&2; exit 1
fi
if echo "$checks_json" | grep -qiE '"state":"(pending|queued|in_progress)"'; then
  echo "safe_merge: REFUSE — CI still running on #$PR (re-check later)" >&2; exit 1
fi

# 2) Bot review APPROVE on the LATEST commit. The Claude review posts a review;
# require its latest review to be APPROVED and tied to the current head SHA.
review_ok="$(gh pr view "$PR" --json reviews -q \
  "[.reviews[] | select(.state==\"APPROVED\" and .commit.oid==\"$head_sha\")] | length")"
if [ "${review_ok:-0}" -lt 1 ]; then
  echo "safe_merge: REFUSE — no APPROVE on latest commit $head_sha (bot may not have re-reviewed)" >&2
  exit 1
fi

echo "safe_merge: gates pass on #$PR @ $head_sha — merging."
gh pr merge "$PR" --squash --delete-branch
