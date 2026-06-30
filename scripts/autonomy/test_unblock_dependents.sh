#!/usr/bin/env bash
# Unit test for unblock_dependents.sh pure matchers (#1866). Sources the REAL
# script (its gh-driven body is guarded by a BASH_SOURCE==$0 check, so sourcing
# only defines the helpers) and table-tests the regexes that decide whether a
# ticket is "blocked by #X" and which other blockers remain. No gh, no network.
#
# Run:  bash scripts/autonomy/test_unblock_dependents.sh   (exit 0 = all pass)

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/unblock_dependents.sh"

fails=0
judge() { # <got> <want> <desc>
  if [ "$1" = "$2" ]; then echo "ok   - $3"; else
    echo "FAIL - $3 (expected '$2', got '$1')"; fails=$((fails + 1)); fi
}

# Real #1822 / #1815 bodies (the #1866 falsification cases) — verbatim shapes.
B1822='Part of #1815. **Blocked by** #1820 (P0 foundation) + P2 analytics — the signals must be computed and stored before they can be backtested.'
B1815=$'> | P2 — new signals | #1823 | blocked by #1820 |\n> | P5a — backtest harness | #1822 | blocked by #1820 + #1823 |'

# confirms_block: does a blocker CLAUSE of the body reference #X as a whole token?
check_confirm() { # <expect: yes|no> <desc> <body> <X>
  local want="$1" desc="$2" body="$3" x="$4" got
  if confirms_block "$body" "$x"; then got=yes; else got=no; fi
  judge "$got" "$want" "$desc"
}

check_confirm yes "plain blocked by"            "Blocked by #1820"                       1820
check_confirm yes "markdown-bold blocked by"    "Part of #1815. **Blocked by** #1820 (P0)" 1820
check_confirm yes "hyphenated blocked-by"       "blocked-by #843"                         843
check_confirm yes "trailing punctuation"        "Blocked by #1820."                       1820
check_confirm yes "one of several blockers"     "Blocked by #1820 and #1823"              1823
check_confirm no  "prefix-digit must not match" "Blocked by #1820"                        182
check_confirm no  "suffix-digit must not match" "Blocked by #182"                         1820
check_confirm no  "mention outside blocked line" $'Implements #1820\nsee notes'           1820
check_confirm no  "no blocked-by line at all"    "Part of #1815. Closes #1820."           1820
# Over-capture guards: parent / row-subject before "blocked by" are NOT blockers.
check_confirm no  "parent (Part of #X) not a block"  "$B1822"                             1815
check_confirm yes "real blocker after the phrase"    "$B1822"                             1820
# F1: uppercase "BLOCKED BY" must still strip the prefix (case-insensitive cut).
check_confirm no  "UPPERCASE: parent not a block"    "Part of #1815. **BLOCKED BY** #1820" 1815
check_confirm yes "UPPERCASE: real blocker"          "Part of #1815. **BLOCKED BY** #1820" 1820
check_confirm no  "table row-subject (only) not a block" "| P2 | #1823 | blocked by #1820 |" 1823
check_confirm yes "#1815: #1823 is a real P5a blocker"   "$B1815"                            1823
check_confirm yes "#1815 table: #1820 blocks every row"  "$B1815"                            1820

# extract_blockers: sorted-unique bare numbers from the blocker clause(s) only.
check_extract() { # <expect: space-joined> <desc> <body>
  local want="$1" desc="$2" body="$3" got
  got="$(extract_blockers "$body" | tr '\n' ' ' | sed 's/ *$//')"
  judge "$got" "$want" "$desc"
}

check_extract "1820"      "single blocker"            "Blocked by #1820 (P0 foundation)"
check_extract "1820 1823" "two blockers, sorted"      "Blocked by #1823 and #1820"
check_extract "843"       "ignores non-blocked refs"  $'Part of #1815. Closes #999.\nBlocked by #843'
check_extract ""          "no blocked-by line"        "Part of #1815. Closes #1820."
check_extract "1820"      "#1822: parent #1815 excluded" "$B1822"
check_extract "1820 1823" "#1815 P5a: subject excluded"  "$B1815"
check_extract "1820"      "UPPERCASE: parent excluded"   "Part of #1815. **BLOCKED BY** #1820"

if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails FAILED"; exit 1; fi
