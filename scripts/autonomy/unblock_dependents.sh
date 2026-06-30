#!/usr/bin/env bash
# scripts/autonomy/unblock_dependents.sh — post-merge dependent notifier (#1866).
#
# When a PR merges and closes issue #X, any open ticket whose body says
# "Blocked by #X" was previously cleared BY HAND (the P2-P4/#1823-#1825 case in
# #1866). This surfaces those dependents automatically: for every issue the PR
# closed, it finds open tickets referencing "blocked by #X" and posts a durable
# comment so none is forgotten.
#
# DELIBERATELY NOTIFY-ONLY — it does NOT move board cards or edit issue bodies.
# A full-population scan (#1866 research) falsified the naive "strip the block
# line + move to Todo" approach: #1822 ("Blocked by #1820 ... + P2 analytics")
# and #1815 (umbrella, lists "blocked by #1820" in a table) both match the
# phrase yet are NOT unblocked by #1820 merging — #1822 still waits on backtest
# infra that no issue tracks; #1815 is the parent. Auto-moving either would
# corrupt the board. So we surface the event and let the operator/loop judge.
# The comment states whether ALL issue-referenced blockers are now closed vs
# which remain open, so the decision is one glance.
#
# BEST-EFFORT BY DESIGN: this runs AFTER the merge already happened (called from
# safe_merge.sh). It must NEVER fail the caller — every path warns to stderr and
# exits 0. A notifier hiccup is not a merge problem.
#
# Usage:  scripts/autonomy/unblock_dependents.sh <merged-pr-number>
set -uo pipefail

warn() { echo "unblock_dependents: $*" >&2; }

# ── Pure matching helpers (unit-tested via test_unblock_dependents.sh, which
#    sources this file). No gh / network — string logic only. ──────────────────

# The blocker CLAUSE(s) of an issue body: for every line containing "blocked
# by", the text AFTER that phrase. Only what FOLLOWS "blocked by" is a blocker —
# numbers before it are the parent ("Part of #1815.") or, in #1815's status
# table, the row's own subject ("| #1823 | blocked by #1820 |"). Stripping the
# prefix is what keeps those out of the blocker set (#1866 full-body validation).
blocker_clauses_of() {
  # Lowercase the matched line BEFORE stripping so the cut is case-insensitive to
  # match the `grep -i` (e.g. "**BLOCKED BY**"); digits are case-neutral, so the
  # downstream `#N` extraction is unaffected (Codex ckpt-2 F1).
  printf '%s\n' "$1" | grep -iE 'blocked[ -]by' \
    | tr '[:upper:]' '[:lower:]' | sed -E 's/^.*blocked[ -]by//'
}

# 0 iff some blocker clause of body ($1) references issue #N ($2) as a WHOLE
# token — `#182` must NOT match `#1820` (trailing digit), so the boundary is
# "#N then a non-digit or end-of-line". `>/dev/null` (not `grep -q`) so grep
# drains its stdin — under `pipefail`, an early-closing `-q` can SIGPIPE the
# upstream pipe and return nonzero on a real match (Codex ckpt-2 F2).
confirms_block() { blocker_clauses_of "$1" | grep -E "#$2([^0-9]|$)" >/dev/null; }

# Sorted-unique bare issue numbers named in the blocker clause(s) of body ($1).
extract_blockers() { blocker_clauses_of "$1" | grep -oE '#[0-9]+' | tr -d '#' | sort -u; }

# When sourced (by the unit test) stop here — only the helpers are wanted.
[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0

PR="${1:-}"
if [ -z "$PR" ]; then warn 'usage: unblock_dependents.sh <pr-number>'; exit 0; fi

REPO_SLUG="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)"
if [ -z "$REPO_SLUG" ]; then warn "cannot resolve repo slug (skip)"; exit 0; fi

# Issues this PR closed (populated from "Closes #N" in the PR body).
closed="$(gh pr view "$PR" --json closingIssuesReferences \
  -q '.closingIssuesReferences[].number' 2>/dev/null || true)"
if [ -z "$closed" ]; then warn "PR #$PR closed no tracked issues (nothing to do)"; exit 0; fi

# Returns 0 if issue $1 is OPEN, 1 otherwise (closed / unknown — fail safe to
# "not open" so we never claim a still-open blocker is cleared).
issue_is_open() {
  [ "$(gh issue view "$1" --json state -q .state 2>/dev/null || echo CLOSED)" = "OPEN" ]
}

for X in $closed; do
  # Candidate dependents: open issues whose full text mentions the phrase. The
  # search is fuzzy, so each candidate is re-confirmed against its body below.
  candidates="$(gh search issues --repo "$REPO_SLUG" --state open "blocked by #$X" \
    --json number -q '.[].number' 2>/dev/null || true)"
  [ -n "$candidates" ] || continue

  for D in $candidates; do
    [ "$D" = "$X" ] && continue

    body="$(gh issue view "$D" --json body -q .body 2>/dev/null || true)"
    [ -n "$body" ] || continue

    # Only a "blocked by #X" clause counts — a passing mention elsewhere in the
    # body (or #X as a parent / table-row subject) is not a dependency.
    confirms_block "$body" "$X" || continue

    # Idempotency: skip if we already posted a notice for this blocker. The scan
    # MUST paginate — `gh issue view --json comments` caps at the first GraphQL
    # page (≤100), so on a busy ticket the marker could be missed and a manual
    # safe_merge re-run would double-post (bot WARNING + Codex F3). `--paginate`
    # over the REST comments endpoint reads them all (same pattern safe_merge
    # uses for the file list). `>/dev/null` not `-q` (F2).
    marker="<!-- autonomy:unblock-notice blocker=#$X -->"
    if gh api --paginate "repos/{owner}/{repo}/issues/$D/comments" \
        --jq '.[].body' 2>/dev/null | grep -F "$marker" >/dev/null; then
      warn "#$D already notified for blocker #$X (skip)"
      continue
    fi

    # Which OTHER issue-referenced blockers (from the blocker clause(s)) are
    # still open? `#123` tokens, minus #X (just closed) and #D (self).
    others="$(extract_blockers "$body")"
    remaining=""
    for B in $others; do
      { [ "$B" = "$X" ] || [ "$B" = "$D" ]; } && continue
      if issue_is_open "$B"; then remaining="$remaining #$B"; fi
    done

    if [ -n "$remaining" ]; then
      status_line="Still blocked by:$remaining (open)."
    else
      status_line="No other issue-referenced blockers remain — ready to move to **Todo** if nothing out-of-band blocks it (e.g. infra/decision not tracked by an issue)."
    fi

    comment="🔓 Blocker #$X merged (PR #$PR). $status_line

$marker"
    if gh issue comment "$D" --body "$comment" >/dev/null 2>&1; then
      echo "unblock_dependents: notified #$D (blocker #$X merged; $status_line)"
    else
      warn "failed to comment on #$D (skip)"
    fi
  done
done

exit 0
