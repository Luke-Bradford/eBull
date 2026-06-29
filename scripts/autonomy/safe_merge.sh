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

# Doc-only predicate (#1863). Returns 0 iff the newline-separated file list is
# non-empty AND every path ends `.md`. A single non-`.md` path → non-zero. Pure
# string logic, unit-tested via `test_safe_merge_doc_only.sh` (sources this file;
# the executable body below is guarded so sourcing only defines this function).
is_doc_only() {
  local files="$1"
  [ -n "$files" ] && ! printf '%s\n' "$files" | grep -qvE '\.md$'
}

# When sourced (e.g. by the unit test) stop here — only the function is wanted.
[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0

PR="${1:?usage: safe_merge.sh <pr-number>}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

# Timestamp of the latest commit on the PR head. The Claude review bot posts its
# verdict as an ISSUE COMMENT (NOT a GitHub review object — `.reviews` is empty),
# so we can't tie it to a SHA directly; instead require the latest bot comment to
# be NEWER than the latest commit (i.e. it re-reviewed after the last push) AND
# to read APPROVE. Empirically verified gh schema (Codex/bot ckpt-2).
head_time="$(gh pr view "$PR" --json commits -q '.commits[-1].committedDate')"
[ -n "$head_time" ] || { echo "safe_merge: cannot resolve PR #$PR head commit time" >&2; exit 1; }

# 1) CI: no check may be failing; none may still be pending.
checks_json="$(gh pr checks "$PR" --json name,state 2>/dev/null || echo '[]')"
if echo "$checks_json" | grep -qiE '"state":"(fail|failure|error|cancelled|timed_out)"'; then
  echo "safe_merge: REFUSE — a CI check failed on #$PR" >&2; exit 1
fi
if echo "$checks_json" | grep -qiE '"state":"(pending|queued|in_progress)"'; then
  echo "safe_merge: REFUSE — CI still running on #$PR (re-check later)" >&2; exit 1
fi

# 1b) Doc-only fast path (#1863). The review bot deliberately SKIPS doc-only
# diffs → never posts an APPROVE → the strict gate below would refuse forever and
# the loop piles up un-mergeable doc PRs (e.g. #1848). NARROW rule: if EVERY
# changed file ends `.md` AND CI is green (asserted above), merge — UNLESS the
# latest bot comment explicitly blocks. A single non-`.md` file disqualifies the
# doc path and falls through to the strict APPROVE-on-latest-SHA gate (unchanged).
# This is not merging around a withheld verdict: the bot CHOSE not to review.
# Authoritative COMPLETE file list via the paginated REST endpoint — `gh pr view
# --json files` caps at the first GraphQL page (~100), so a large mixed PR whose
# first page is all `.md` could slip code through (Codex ckpt-2 HIGH). Belt: the
# listed count must equal `changedFiles`; any mismatch → fall through to strict.
files="$(gh api --paginate "repos/{owner}/{repo}/pulls/$PR/files" --jq '.[].filename')"
n_listed="$(printf '%s\n' "$files" | grep -c . || true)"
n_changed="$(gh pr view "$PR" --json changedFiles -q '.changedFiles')"
if [ "$n_listed" = "$n_changed" ] && is_doc_only "$files"; then
  doc_block="$(gh pr view "$PR" --json comments -q \
    '[.comments[] | select(.author.login=="github-actions" and (.body|contains("Claude Code Review")))]
     | sort_by(.createdAt) | last | .body // ""')"
  if printf '%s' "$doc_block" | grep -qiE 'REQUEST CHANGES|\[BLOCKING\]|must fix before merge'; then
    echo "safe_merge: REFUSE — doc-only PR #$PR but latest bot comment blocks" >&2; exit 1
  fi
  echo "safe_merge: doc-only PR #$PR (every changed file .md), CI green, no blocking comment — merging."
  gh pr merge "$PR" --squash --delete-branch
  exit 0
fi

# 2) Latest Claude-review bot comment, by createdAt.
latest="$(gh pr view "$PR" --json comments -q \
  '[.comments[] | select(.author.login=="github-actions" and (.body|contains("Claude Code Review")))]
   | sort_by(.createdAt) | last')"
[ -n "$latest" ] && [ "$latest" != "null" ] || {
  echo "safe_merge: REFUSE — no Claude review comment on #$PR yet" >&2; exit 1; }
review_time="$(printf '%s' "$latest" | python3 -c 'import sys,json;print(json.load(sys.stdin)["createdAt"])')"
review_body="$(printf '%s' "$latest" | python3 -c 'import sys,json;print(json.load(sys.stdin)["body"])')"

# 2a) The review must POST-DATE the latest commit (re-reviewed after last push).
if [[ "$review_time" < "$head_time" ]]; then
  echo "safe_merge: REFUSE — latest review ($review_time) predates head commit ($head_time); push reset the gate" >&2
  exit 1
fi
# 2b) The verdict must be APPROVE, with no blocking/changes language.
if printf '%s' "$review_body" | grep -qiE 'REQUEST CHANGES|\[BLOCKING\]|must fix before merge'; then
  echo "safe_merge: REFUSE — latest review requests changes / has blocking findings" >&2; exit 1
fi
if ! printf '%s' "$review_body" | grep -qiE 'APPROVE'; then
  echo "safe_merge: REFUSE — latest review is not an APPROVE" >&2; exit 1
fi

echo "safe_merge: gates pass on #$PR (review $review_time ≥ head $head_time) — merging."
gh pr merge "$PR" --squash --delete-branch
