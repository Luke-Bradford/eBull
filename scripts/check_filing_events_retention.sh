#!/usr/bin/env bash
#
# Lint guard: every `filing_events` writer chokepoint MUST honour the
# 10-year rolling retention cap (#1233 §4.2 PR3).
#
# Two target files, three invariants:
#
# A. `app/services/filings.py` — TWO `INSERT INTO filing_events` writer
#    chokepoints (the canonical filing-event upsert helpers). Each MUST
#    be preceded by a call to `filing_within_retention(...)`. Parity is
#    enforced as a count equality: number of `INSERT INTO filing_events`
#    blocks MUST equal number of `filing_within_retention(` call sites
#    (definitions excluded). Anyone adding a new INSERT also adds a
#    gate, or parity fails.
#
# B. `app/services/fundamentals/__init__.py` — ONE `INSERT INTO filing_events`
#    chokepoint (`_upsert_filing_from_master_index`). Same parity
#    contract: one INSERT, one gate call.
#
# C. Cross-cutting — NO `INSERT INTO filing_events` anywhere else under
#    `app/`. The 3 chokepoints above are the entire writer surface.
#    UPDATE statements (e.g. `sec_filing_items::_run_cik_upsert` which
#    refreshes `filing_events.items`) are out of scope — they touch
#    already-existing rows and cannot introduce pre-cap data.
#
# Exits non-zero on the first invariant violation. Wired into
# `.githooks/pre-push` after `check_form4_retention.sh`.
#
# Pattern mirrors `scripts/check_form4_retention.sh` (PR4) — awk block
# parsing + literal counts + `def`-line exclusion on call-site counts.

set -euo pipefail

FILE_FILINGS="app/services/filings.py"
# ``fundamentals`` is a package — the master-index writer lives in its
# __init__.py (was a flat module when this guard was written; the path
# went stale on the package refactor and silently passed nothing).
FILE_FUNDAMENTALS="app/services/fundamentals/__init__.py"

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

count_literal() {
  local file="$1" pat="$2"
  grep -Fc "$pat" "$file" || true
}

# Count parenthesis-suffixed helper call-sites, EXCLUDING `def` lines
# (helper definitions) AND pure-comment lines. Mirrors
# check_form4_retention.sh::count_call_sites. Note: triple-quoted
# docstring lines are NOT stripped; in practice the helper names are
# never mentioned in docstrings of the 2 target files, but a future
# docstring drop-in could inflate the count. A stronger triple-quote
# scanner can be added if that ever surfaces (out of scope today).
count_call_sites() {
  local file="$1" symbol="$2"
  awk -v sym="$symbol" '
    BEGIN { sym_open = sym "(" }
    {
      stripped = $0
      sub(/^[[:space:]]+/, "", stripped)
      # Skip def lines
      if (substr(stripped, 1, 4) == "def ") next
      # Skip pure # comment lines
      if (substr(stripped, 1, 1) == "#") next
      # Match symbol(
      if (index($0, sym_open) > 0) print
    }
  ' "$file" | wc -l | tr -d ' '
}

# ----------------------------------------------------------------------
# A. filings.py — 2 INSERT chokepoints + 2 retention-gate calls
# ----------------------------------------------------------------------

if [[ ! -f "$FILE_FILINGS" ]]; then
  fail "missing file: $FILE_FILINGS"
else
  insert_count=$(count_literal "$FILE_FILINGS" "INSERT INTO filing_events")
  gate_count=$(count_call_sites "$FILE_FILINGS" "filing_within_retention")

  if (( insert_count != 2 )); then
    fail "$FILE_FILINGS: expected exactly 2 'INSERT INTO filing_events' chokepoint(s), found ${insert_count}. Update the lint guard if you intentionally added/removed a chokepoint AND add a matching filing_within_retention(...) gate."
  fi
  if (( gate_count != insert_count )); then
    fail "$FILE_FILINGS: filing_events chokepoint parity broken — ${insert_count} 'INSERT INTO filing_events' block(s) but ${gate_count} filing_within_retention(...) call-site(s). Every chokepoint must call the gate before the INSERT."
  fi
fi

# ----------------------------------------------------------------------
# B. fundamentals/__init__.py — 1 INSERT chokepoint + 1 retention-gate call
# ----------------------------------------------------------------------

if [[ ! -f "$FILE_FUNDAMENTALS" ]]; then
  fail "missing file: $FILE_FUNDAMENTALS"
else
  insert_count=$(count_literal "$FILE_FUNDAMENTALS" "INSERT INTO filing_events")
  gate_count=$(count_call_sites "$FILE_FUNDAMENTALS" "filing_within_retention")

  if (( insert_count != 1 )); then
    fail "$FILE_FUNDAMENTALS: expected exactly 1 'INSERT INTO filing_events' chokepoint, found ${insert_count}. The single writer here is _upsert_filing_from_master_index. Update the lint guard if you intentionally added/removed a chokepoint AND add a matching filing_within_retention(...) gate."
  fi
  if (( gate_count != insert_count )); then
    fail "$FILE_FUNDAMENTALS: filing_events chokepoint parity broken — ${insert_count} 'INSERT INTO filing_events' block(s) but ${gate_count} filing_within_retention(...) call-site(s). The single chokepoint must call the gate before the INSERT."
  fi
fi

# ----------------------------------------------------------------------
# C. Cross-cutting — no INSERT INTO filing_events elsewhere under app/
# ----------------------------------------------------------------------

# `grep -rc` returns one `<file>:<count>` line per file. Sum the counts
# from non-canonical files so the error message reports OCCURRENCE
# count (not FILE count — bot review iter 1 NITPICK).
stray_occurrences=$(grep -rc "INSERT INTO filing_events" app --include="*.py" 2>/dev/null \
    | awk -F: '
        $1 != "app/services/filings.py" && $1 != "app/services/fundamentals/__init__.py" && $2 > 0 {
          sum += $2
        }
        END { print (sum ? sum : 0) }
      ')
if (( stray_occurrences != 0 )); then
  fail "C: found ${stray_occurrences} 'INSERT INTO filing_events' occurrence(s) outside the 2 canonical files. Every writer must route through filings.py or fundamentals.py to inherit the retention gate."
  grep -rln "INSERT INTO filing_events" app --include="*.py" 2>/dev/null \
    | grep -v -E "^(app/services/filings\.py|app/services/fundamentals/__init__\.py)$" >&2 || true
fi

# ----------------------------------------------------------------------
# Verdict
# ----------------------------------------------------------------------

if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} filing_events retention-cap invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.2" >&2
  echo "for the 10y rolling cap rationale." >&2
  exit 1
fi

echo "OK: filing_events retention cap honoured at every chokepoint."
