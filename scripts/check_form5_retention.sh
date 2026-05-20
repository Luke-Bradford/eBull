#!/usr/bin/env bash
#
# Lint guard: every Form 5 / 5-A writer chokepoint MUST honour the
# 18-month retention cap (#1233 §4.4 PR10b).
#
# Form 5 is observations-shaped at write (PK = accession_number +
# txn_row_num), so latest-per-pair is not enforceable at the schema
# layer. The cap is instead an ingest-time ``filed_at`` window
# applied at every writer chokepoint. Four invariants:
#
#  A. ``app/services/insider_transactions.py`` defines all three
#     symbols: ``form5_retention_cutoff(``, ``form5_within_retention(``,
#     and the constant ``INSIDER_FORM5_RETENTION_MONTHS``. Exactly one
#     ``def`` line each + at least one constant assignment. Pinning
#     the canonical module avoids duplicate / parallel definitions in
#     other modules.
#  B. ``app/services/manifest_parsers/insider_345.py::_parse_form5``
#     body calls ``form5_within_retention(`` AT LEAST ONCE (pre-fetch
#     gate). Awk function-scope walker bounded by the next top-level
#     ``def`` / ``class``.
#  C. ``app/services/sec_insider_dataset_ingest.py`` calls
#     ``form5_retention_cutoff(`` exactly once (archive-level anchor)
#     AND contains EXACTLY 2 occurrences of the literal
#     ``filed_at.date() < retention_cutoff_form5`` (one per write loop
#     — NONDERIV_TRANS + NONDERIV_HOLDING). Mirrors PR4's per-loop
#     parity check; Codex 1a MED required two concrete predicates,
#     not a single "alongside" check.
#  D. ``app/services/ownership_observations_sync.py`` (the
#     ``sync_insiders`` writer) contains ``form5_retention_cutoff(``
#     AT LEAST ONCE AND each of these literals at least once:
#       - ``LEFT JOIN filing_events fe`` (Codex 1b MED — LEFT, not
#         INNER, so Form 4 rows lacking a manifest entry still sync)
#       - ``f.document_type IN ('5','5/A')``
#       - ``fe.filing_date >= %(form5_cutoff)s``
#
# Exits non-zero on the first invariant violation. Wired into
# ``.githooks/pre-push`` after ``check_business_summary_latest_only.sh``.
#
# Awk-based block parsing (BSD vs GNU ``grep -P`` portability — PR4
# Codex 1c lesson). Empty-grep ``wc -l`` guarded
# (docs/review-prevention-log.md "Empty-grep wc -l false-positive in
# bash idioms").

set -euo pipefail

FILE_HELPERS="app/services/insider_transactions.py"
FILE_MANIFEST="app/services/manifest_parsers/insider_345.py"
FILE_BULK="app/services/sec_insider_dataset_ingest.py"
FILE_SYNC="app/services/ownership_observations_sync.py"

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

# Count literal-substring occurrences inside a file.
count_literal() {
  local file="$1" pat="$2"
  grep -Fc "$pat" "$file" || true
}

# Count regex matches across a file (ERE).
count_regex() {
  local file="$1" pattern="$2"
  grep -Ec "$pattern" "$file" || true
}

# Count parenthesis-suffixed helper call-sites inside a function scope.
# Walks the file with awk, entering scope on a ``def <name>(`` line and
# exiting on the next top-level ``def `` / ``class `` (matched at
# column 0) or EOF. Skips pure-comment lines.
count_in_function() {
  local file="$1" func="$2" symbol="$3"
  awk -v fname="$func" -v sym="$symbol" '
    function trim(s) { sub(/^[[:space:]]+/, "", s); sub(/[[:space:]]+$/, "", s); return s }
    BEGIN { in_scope = 0; n = 0 }
    /^def [A-Za-z_]/ || /^class [A-Za-z_]/ {
      if (in_scope == 1) in_scope = 0
    }
    {
      if (in_scope == 1) {
        line = $0
        s = trim(line)
        if (substr(s, 1, 1) == "#") next
        pat = sym "("
        if (index(line, pat) > 0) n++
      }
    }
    $0 ~ ("^def " fname "\\(") { in_scope = 1 }
    END { print n + 0 }
  ' "$file"
}

# ======================================================================
# A — helpers + constant defined in canonical module
# ======================================================================
echo "Checking invariant A (Form 5 helpers + constant in insider_transactions.py)..."

if [[ ! -f "$FILE_HELPERS" ]]; then
  fail "missing file: $FILE_HELPERS"
else
  cutoff_def=$(count_regex "$FILE_HELPERS" "^def form5_retention_cutoff\(")
  within_def=$(count_regex "$FILE_HELPERS" "^def form5_within_retention\(")
  const_assigns=$(count_regex "$FILE_HELPERS" "^INSIDER_FORM5_RETENTION_MONTHS[[:space:]]*:")
  if (( cutoff_def != 1 )); then
    fail "$FILE_HELPERS: expected exactly 1 'def form5_retention_cutoff(' line, found ${cutoff_def}."
  fi
  if (( within_def != 1 )); then
    fail "$FILE_HELPERS: expected exactly 1 'def form5_within_retention(' line, found ${within_def}."
  fi
  if (( const_assigns < 1 )); then
    fail "$FILE_HELPERS: expected at least 1 'INSIDER_FORM5_RETENTION_MONTHS:' assignment, found ${const_assigns}."
  fi
fi

# ======================================================================
# B — manifest-worker pre-fetch gate in _parse_form5
# ======================================================================
echo "Checking invariant B (manifest-worker _parse_form5 calls form5_within_retention)..."

if [[ ! -f "$FILE_MANIFEST" ]]; then
  fail "missing file: $FILE_MANIFEST"
else
  within_calls=$(count_in_function "$FILE_MANIFEST" "_parse_form5" "form5_within_retention")
  if (( within_calls < 1 )); then
    fail "$FILE_MANIFEST: missing form5_within_retention(...) pre-fetch gate inside _parse_form5. PR10b (#1233 §4.4) requires the manifest worker to honour the 18-month cap before fetching."
  fi
fi

# ======================================================================
# C — bulk dataset archive anchor + per-loop parity
# ======================================================================
echo "Checking invariant C (bulk dataset Form 5 retention cutoff parity)..."

if [[ ! -f "$FILE_BULK" ]]; then
  fail "missing file: $FILE_BULK"
else
  cutoff_calls=$(count_regex "$FILE_BULK" "form5_retention_cutoff\(")
  per_loop_predicate=$(count_literal "$FILE_BULK" "filed_at.date() < retention_cutoff_form5")

  if (( cutoff_calls != 1 )); then
    fail "$FILE_BULK: expected exactly 1 form5_retention_cutoff(...) call (archive-level anchor); found ${cutoff_calls}."
  fi
  if (( per_loop_predicate != 2 )); then
    fail "$FILE_BULK: expected exactly 2 'filed_at.date() < retention_cutoff_form5' predicate uses (one per write loop — NONDERIV_TRANS + NONDERIV_HOLDING); found ${per_loop_predicate}."
  fi
fi

# ======================================================================
# D — sync_insiders LEFT JOIN + Form 5 doc-type + cutoff predicate
# ======================================================================
echo "Checking invariant D (sync_insiders LEFT JOIN + Form 5 gate)..."

if [[ ! -f "$FILE_SYNC" ]]; then
  fail "missing file: $FILE_SYNC"
else
  sync_cutoff_calls=$(count_regex "$FILE_SYNC" "form5_retention_cutoff\(")
  left_join_count=$(count_literal "$FILE_SYNC" "LEFT JOIN filing_events fe")
  form5_doctype_count=$(count_literal "$FILE_SYNC" "f.document_type IN ('5','5/A')")
  cutoff_predicate_count=$(count_literal "$FILE_SYNC" "fe.filing_date >= %(form5_cutoff)s")

  if (( sync_cutoff_calls < 1 )); then
    fail "$FILE_SYNC: missing form5_retention_cutoff(...) call inside sync_insiders. PR10b sync chokepoint."
  fi
  if (( left_join_count < 1 )); then
    fail "$FILE_SYNC: missing 'LEFT JOIN filing_events fe' literal. Codex 1b MED — must NOT be INNER JOIN (Form 4 rows lacking manifest entry must still sync)."
  fi
  if (( form5_doctype_count < 1 )); then
    fail "$FILE_SYNC: missing \"f.document_type IN ('5','5/A')\" literal in sync_insiders Form 5 branch."
  fi
  if (( cutoff_predicate_count < 1 )); then
    fail "$FILE_SYNC: missing 'fe.filing_date >= %(form5_cutoff)s' literal in sync_insiders Form 5 branch."
  fi
fi

# ======================================================================
# Summary
# ======================================================================
if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} Form 5 retention-cap invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.4 +" >&2
  echo "docs/superpowers/plans/2026-05-20-pr10b-form35-cap.md §3 for the rules." >&2
  exit 1
fi

echo "OK: Form 5 retention cap honoured at every chokepoint."
