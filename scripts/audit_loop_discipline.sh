#!/usr/bin/env bash
# Audit the autonomy loop against the 2026-09-21 discipline rules (PR #3276).
#
# Answers two questions the loop prompt asserts are auditable, so the assertion
# is a command rather than a claim (review bot WARNING on #3276):
#
#   1. BACK-OFF: did an iteration report a blocked queue and then open a
#      board-fallback PR anyway? (P0)
#   2. RUNG: does every merged PR declare the review rung it worked at? (P2)
#
# Read-only. Exit 1 if any violation is found, so it can gate a weekly check.
#
# Usage: scripts/audit_loop_discipline.sh [days]   (default 7)

set -uo pipefail

DAYS="${1:-7}"
# The rung-declaration rule lands with PR #3276; PRs merged before it cannot
# have obeyed it, so auditing them reports noise instead of violations.
# 09-22 is the first FULL day under the rule; PRs merged on 09-21 predate its merge.
RULE_EFFECTIVE="${LOOP_RUNG_RULE_EFFECTIVE:-2026-09-22}"
STATE_DIR="${TA_LOOP_STATE:-/Users/lukebradford/Dev/.ebull-autonomy/var/autonomy}"
SINCE=$(date -u -v-"${DAYS}"d +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "${DAYS} days ago" +%Y-%m-%dT%H:%M:%SZ)
violations=0

echo "== loop discipline audit, since $SINCE"

# ---- P2: every merged PR declares its rung -------------------------------
echo
echo "-- rung declarations (P2)"
missing=0
while read -r num merged title; do
  [ -z "$num" ] && continue
  [ "${merged%%T*}" \< "$RULE_EFFECTIVE" ] && continue
  body=$(gh pr view "$num" --json body --jq .body 2>/dev/null)
  if printf '%s' "$body" | grep -qiE '^[^a-z]*rung:'; then
    :
  else
    printf '  MISSING rung declaration: #%s %s\n' "$num" "${title:0:60}"
    missing=$((missing + 1))
  fi
done < <(gh pr list --state merged --search "merged:>=${SINCE%%T*}" --limit 60 \
           --json number,mergedAt,title --jq '.[] | "\(.number) \(.mergedAt) \(.title)"' 2>/dev/null)
if [ "$missing" -gt 0 ]; then
  echo "  → $missing merged PR(s) without a declared rung"
  violations=$((violations + missing))
else
  echo "  all merged PRs since $RULE_EFFECTIVE declare a rung"
fi

# ---- P0: blocked-queue runs must not open board-fallback PRs -------------
echo
echo "-- back-off discipline (P0)"
blocked_runs=0
for t in "$STATE_DIR"/iteration-*.log; do
  case "$t" in *.err) continue ;; esac
  [ -e "$t" ] || continue
  # only iterations inside the window
  ts=$(basename "$t" | sed -n 's/iteration-\([0-9TZ]*\)-.*/\1/p')
  [ "$ts" \< "$(printf '%s' "$SINCE" | tr -d ':-')" ] && continue
  # the run's own verdict text is the last result event
  verdict=$(grep '"type":"result"' "$t" 2>/dev/null | tail -1)
  [ -z "$verdict" ] && continue
  if printf '%s' "$verdict" | grep -qiE 'queue (is )?(fully )?blocked|no eligible (queue )?ticket|backed off'; then
    blocked_runs=$((blocked_runs + 1))
    if printf '%s' "$verdict" | grep -qiE 'opened PR|merged .#[0-9]+|board fallback'; then
      printf '  CONTRADICTION: %s reports a blocked queue AND PR activity\n' "$(basename "$t")"
      violations=$((violations + 1))
    fi
  fi
done
echo "  $blocked_runs iteration(s) reported a blocked queue in window"

echo
if [ "$violations" -gt 0 ]; then
  echo "RESULT: $violations violation(s)"
  exit 1
fi
echo "RESULT: clean"
