#!/usr/bin/env bash
#
# Lint guard: the Form 3 read-side latest-per-pair contract (#1233
# §4.4 PR10b) MUST be honoured inside
# ``list_baseline_only_insider_holdings``.
#
# Form 3 is observations-shaped at write (PK = accession_number; each
# Form 3 / 3-A amendment appends one row), so latest-per-pair is
# enforced at READ time via the SQL inside
# ``list_baseline_only_insider_holdings``. Three structural anchors
# pin the contract:
#
#  A. Function defined exactly once at the module level in
#     ``app/services/insider_form3_ingest.py``.
#  B. DISTINCT ON over the canonical pair key
#     ``(iih.filer_cik, iih.security_title, iih.is_derivative)``
#     (whitespace-normalised — the column list may span multiple
#     lines and the lint must not depend on the literal source
#     formatting; Codex 1a HIGH).
#  C. ORDER BY tie-break in priority order — latest as_of_date wins,
#     then highest accession_number (same-day 3/A amendment), then
#     lowest row_num (option-vs-underlying-equity on a single
#     filing). Each of the three sort keys appears.
#  D. ``NOT EXISTS`` anti-join against non-tombstoned
#     ``insider_transactions`` for the same filer (excludes filers
#     with Form 4 activity from the baseline-only output — Codex 1a
#     MED).
#
# The awk walker isolates the function body bounded by the next
# top-level ``def`` / ``class``, strips ``-- SQL line comments``,
# collapses all whitespace to single spaces, and asserts each anchor
# as a literal substring of the normalised text. Resilient against
# reformatting; brittle only against semantic changes (the desired
# behaviour).
#
# Exits non-zero on the first invariant violation. Wired into
# ``.githooks/pre-push`` after ``check_form5_retention.sh``.

set -euo pipefail

FILE_FORM3="app/services/insider_form3_ingest.py"
FUNC="list_baseline_only_insider_holdings"

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

if [[ ! -f "$FILE_FORM3" ]]; then
  fail "missing file: $FILE_FORM3"
  echo "FAIL: ${violations} Form 3 latest-per-pair invariant violation(s)." >&2
  exit 1
fi

# ----------------------------------------------------------------------
# A — function defined exactly once.
# ----------------------------------------------------------------------
def_count=$(grep -Ec "^def ${FUNC}\(" "$FILE_FORM3" || true)
if (( def_count != 1 )); then
  fail "$FILE_FORM3: expected exactly 1 'def ${FUNC}(' line, found ${def_count}."
fi

# ----------------------------------------------------------------------
# Extract the function body (between the def line and the next
# top-level def/class or EOF), strip SQL line comments, collapse
# whitespace, into a single normalised string.
# ----------------------------------------------------------------------
normalised=$(awk -v fname="$FUNC" '
  BEGIN { in_scope = 0 }
  /^def [A-Za-z_]/ || /^class [A-Za-z_]/ {
    if (in_scope == 1) in_scope = 0
  }
  $0 ~ ("^def " fname "\\(") { in_scope = 1; next }
  {
    if (in_scope == 1) {
      line = $0
      # Strip SQL line comments (`-- ...` to end of line).
      sub(/--.*$/, "", line)
      print line
    }
  }
' "$FILE_FORM3" | tr '\n' ' ' | tr -s '[:space:]' ' ')

if [[ -z "$normalised" ]]; then
  fail "$FILE_FORM3: could not extract function body for ${FUNC}."
  echo "FAIL: ${violations} Form 3 latest-per-pair invariant violation(s)." >&2
  exit 1
fi

# Helper: count whitespace-tolerant literal occurrences inside the
# normalised body string. The needle is matched literally; spaces in
# the needle match exactly one space in the normalised body
# (whitespace already collapsed by tr -s).
count_in_body() {
  local needle="$1"
  # awk-based substring counter — POSIX-portable, no PCRE.
  awk -v hay="$normalised" -v ndl="$needle" '
    BEGIN {
      n = 0
      i = 1
      while (1) {
        pos = index(substr(hay, i), ndl)
        if (pos == 0) break
        n++
        i = i + pos + length(ndl) - 1
      }
      print n
    }
  '
}

# ----------------------------------------------------------------------
# B — DISTINCT ON over the canonical pair key.
# ----------------------------------------------------------------------
distinct_on_count=$(count_in_body "SELECT DISTINCT ON (")
if (( distinct_on_count != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'SELECT DISTINCT ON (' opener, found ${distinct_on_count}."
fi

pair_key_count=$(count_in_body "iih.filer_cik, iih.security_title, iih.is_derivative")
# The pair-key tuple MUST appear exactly twice — once as the
# DISTINCT ON argument, once as the leading ORDER BY prefix. SQL
# semantics require the ORDER BY to lead with the DISTINCT ON
# columns or the dedup picks an arbitrary row per group. A single
# match would mean either:
#   - DISTINCT ON columns drifted away from ORDER BY (silent
#     latest-per-pair break), or
#   - ORDER BY pair prefix dropped (same break).
if (( pair_key_count != 2 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 2 'iih.filer_cik, iih.security_title, iih.is_derivative' pair-key tuples (DISTINCT ON + leading ORDER BY prefix), found ${pair_key_count}."
fi

# ----------------------------------------------------------------------
# C — ORDER BY tie-break in priority order.
# ----------------------------------------------------------------------
as_of_count=$(count_in_body "iih.as_of_date DESC,")
if (( as_of_count != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'iih.as_of_date DESC,' tie-break sort key, found ${as_of_count}."
fi
accn_count=$(count_in_body "iih.accession_number DESC,")
if (( accn_count != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'iih.accession_number DESC,' tie-break sort key (same-day 3/A amendment precedence), found ${accn_count}."
fi
row_num_count=$(count_in_body "iih.row_num ASC")
if (( row_num_count != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'iih.row_num ASC' final tie-break sort key, found ${row_num_count}."
fi

# ----------------------------------------------------------------------
# D — NOT EXISTS anti-join (Form-4-active filer exclusion).
# ----------------------------------------------------------------------
anti_not_exists=$(count_in_body "WHERE NOT EXISTS (")
if (( anti_not_exists != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'WHERE NOT EXISTS (' anti-join opener, found ${anti_not_exists}."
fi
anti_join_join=$(count_in_body "FROM insider_transactions it INNER JOIN insider_filings ft")
if (( anti_join_join != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'FROM insider_transactions it INNER JOIN insider_filings ft' shape (whitespace-normalised), found ${anti_join_join}."
fi
anti_tomb=$(count_in_body "ft.is_tombstone = FALSE")
if (( anti_tomb != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'ft.is_tombstone = FALSE' anti-join tombstone exclusion, found ${anti_tomb}."
fi
anti_filer=$(count_in_body "it.filer_cik = b.filer_cik")
if (( anti_filer != 1 )); then
  fail "$FILE_FORM3:${FUNC}: expected exactly 1 'it.filer_cik = b.filer_cik' anti-join filer correlation, found ${anti_filer}."
fi

# ----------------------------------------------------------------------
# Verdict
# ----------------------------------------------------------------------
if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} Form 3 latest-per-pair invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.4 +" >&2
  echo "docs/superpowers/plans/2026-05-20-pr10b-form35-cap.md §3.4 for the rules." >&2
  exit 1
fi

echo "OK: Form 3 latest-per-pair invariants A / B / C / D satisfied."
