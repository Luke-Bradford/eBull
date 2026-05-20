#!/usr/bin/env bash
#
# Lint guard: every Form 4 / 4-A writer chokepoint MUST honour the 3y
# retention cap (#1233 §4.3 PR4).
#
# Three target files, two invariants:
#
# A. ``app/services/insider_transactions.py`` — every SQL block that
#    selects from ``filing_events`` joined to ``insider_filings`` for
#    Form 4 / 4-A MUST contain the ``%(retention_cutoff)s`` parameter
#    binding. Parity is enforced as a count equality: number of
#    chokepoint blocks (markers ``filing_type IN ('4', '4/A')``) MUST
#    equal number of ``%(retention_cutoff)s`` parameter bindings.
#    Anyone who adds a new SELECT must also add a helper-bound
#    predicate, or the parity check fails.
#
# B. ``app/services/manifest_parsers/insider_345.py`` — the
#    ``_parse_form4`` function MUST call ``form4_within_retention(``
#    AT LEAST ONCE. Pre-fetch gate.
#
# C. ``app/services/sec_insider_dataset_ingest.py`` — MUST call
#    ``form4_retention_cutoff(`` exactly once (archive-level cutoff
#    used by both per-row loops to gate Form 4 / 4-A only). The
#    ``form4_*`` symbols are counted by call-site
#    (parenthesis-suffixed), with ``def`` lines excluded so the helper
#    definitions themselves don't inflate the count.
#
# Exits non-zero on the first invariant violation. Wired into
# ``.githooks/pre-push`` so a violation blocks the push.
#
# Codex 1c portability fix: ``awk`` block parsing rather than
# multiline ``grep -P`` (BSD vs GNU grep diverge on ``-P``).

set -euo pipefail

FILE_INSIDER_TXNS="app/services/insider_transactions.py"
FILE_MANIFEST_FORM4="app/services/manifest_parsers/insider_345.py"
FILE_BULK_DATASET="app/services/sec_insider_dataset_ingest.py"

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

# Count occurrences of a literal substring inside a file. ``-F`` for
# literal (no regex), ``-c`` returns 0 on no matches (we discard the
# non-zero exit code from grep when count is zero).
count_literal() {
  local file="$1" pat="$2"
  grep -Fc "$pat" "$file" || true
}

# Count parenthesis-suffixed helper call-sites, EXCLUDING ``def`` lines
# (helper definitions themselves) and pure-comment lines. The awk
# pattern: line must contain ``<symbol>(`` AND must NOT start with
# ``def`` after optional leading whitespace AND must NOT be a pure
# ``#`` comment.
count_call_sites() {
  local file="$1" symbol="$2"
  awk -v sym="$symbol" '
    /^[[:space:]]*#/   { next }       # skip pure-comment lines
    /^[[:space:]]*def[[:space:]]/ { next }  # skip def lines
    {
      pat = sym "("
      if (index($0, pat) > 0) n++
    }
    END { print n + 0 }
  ' "$file"
}

# ----------------------------------------------------------------------
# A. insider_transactions.py — block parity
# ----------------------------------------------------------------------

if [[ ! -f "$FILE_INSIDER_TXNS" ]]; then
  fail "missing file: $FILE_INSIDER_TXNS"
else
  # Count only SQL blocks — docstring backtick-quoted prose mentions
  # use a `` `` wrap and don't have the ``AND fe.`` prefix used in
  # actual SQL chokepoints. Counting on the ``AND fe.filing_type``
  # form excludes docstrings without parsing block structure.
  chokepoint_count=$(count_literal "$FILE_INSIDER_TXNS" "AND fe.filing_type IN ('4', '4/A')")
  # Count the FULL retention predicate, not bare ``%(retention_cutoff)s``.
  # Bare-param counting let unused params or comment mentions inflate
  # the count and satisfy parity even if no SQL block actually compared
  # against the cutoff (Codex 2 MED finding).
  predicate_count=$(count_literal "$FILE_INSIDER_TXNS" "AND fe.filing_date >= %(retention_cutoff)s")

  expected=4
  if (( chokepoint_count != expected )); then
    fail "$FILE_INSIDER_TXNS: expected ${expected} Form 4 SQL chokepoint blocks, found ${chokepoint_count}. Update the lint guard if you intentionally added/removed a chokepoint."
  fi
  if (( predicate_count != chokepoint_count )); then
    fail "$FILE_INSIDER_TXNS: Form 4 chokepoint parity broken — ${chokepoint_count} \"filing_type IN ('4', '4/A')\" block(s) but ${predicate_count} \"%(retention_cutoff)s\" binding(s). Every block must reference the retention cutoff param."
  fi
fi

# ----------------------------------------------------------------------
# B. insider_345.py — manifest-worker pre-fetch gate
# ----------------------------------------------------------------------

if [[ ! -f "$FILE_MANIFEST_FORM4" ]]; then
  fail "missing file: $FILE_MANIFEST_FORM4"
else
  within_calls=$(count_call_sites "$FILE_MANIFEST_FORM4" "form4_within_retention")
  if (( within_calls < 1 )); then
    fail "$FILE_MANIFEST_FORM4: missing form4_within_retention(...) pre-fetch gate in _parse_form4. PR4 (#1233 §4.3) requires every Form 4 manifest-worker dispatch to honour the 3y cap before fetching."
  fi
fi

# ----------------------------------------------------------------------
# C. sec_insider_dataset_ingest.py — bulk archive cutoff + per-loop gate
# ----------------------------------------------------------------------

if [[ ! -f "$FILE_BULK_DATASET" ]]; then
  fail "missing file: $FILE_BULK_DATASET"
else
  cutoff_calls=$(count_call_sites "$FILE_BULK_DATASET" "form4_retention_cutoff")
  cutoff_predicate=$(count_literal "$FILE_BULK_DATASET" "filed_at.date() < retention_cutoff")

  if (( cutoff_calls != 1 )); then
    fail "$FILE_BULK_DATASET: expected exactly 1 form4_retention_cutoff(...) call (archive-level anchor); found ${cutoff_calls}."
  fi
  if (( cutoff_predicate < 2 )); then
    fail "$FILE_BULK_DATASET: expected ≥2 'filed_at.date() < retention_cutoff' predicate uses (one per write loop — NONDERIV_TRANS + NONDERIV_HOLDING); found ${cutoff_predicate}."
  fi
fi

# ----------------------------------------------------------------------
# Verdict
# ----------------------------------------------------------------------

if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} Form 4 retention-cap invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.3 +" >&2
  echo "docs/superpowers/plans/2026-05-20-pr4-form4-3y-cap.md §6 for the rules." >&2
  exit 1
fi

echo "OK: Form 4 retention cap honoured at every chokepoint."
