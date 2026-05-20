#!/usr/bin/env bash
#
# Lint guard: the 730d N-CSR / N-CSRS filed-at retention cap (#1233
# §4.12 / PR8) MUST be honoured at every writer chokepoint.
#
# Letter labels A / B / D / I preserved from the PR6 (13F-HR) + PR7
# (N-PORT) guard alphabet for grep-ability. The omitted letters
# (C / E / F / G / H) correspond to chokepoints that DO NOT EXIST for
# N-CSR:
#
#   C. _ingest_single_accession defensive post-parse gate
#      → N-CSR has no per-accession one-shot endpoint.
#   E. rewash _apply_n_csr_* rescue branch
#      → no rewash function exists for N-CSR.
#   F. bulk dataset archive-level + per-row gate
#      → SEC publishes no bulk N-CSR archive.
#   G. SQL repair sweep (sync_fund_metadata)
#      → no repair-sweep function exists for N-CSR.
#   H. repo-wide n_csr_within_retention(...) call-site count
#      → redundant with D. Helper definitions AND parser gate live
#        in the same module (sec_n_csr.py), so a global call-site
#        sweep cannot distinguish "the chokepoint" from "the helper
#        body". Reinstate H if a future PR adds a second N-CSR
#        chokepoint in a different module.
#
# Future PRs adding any of those chokepoints MUST extend this guard.
#
# Invariants:
#
#  A. Helpers + retention constant defined exactly once, in the
#     canonical module (app/services/manifest_parsers/sec_n_csr.py).
#       - "def n_csr_retention_cutoff(" appears exactly 1 time.
#       - "def n_csr_within_retention(" appears exactly 1 time.
#       - "N_CSR_RETENTION_DAYS:" appears exactly 1 time at column 0
#         (annotated assignment line — docstring / comment / usage
#         references do not count).
#  B. bootstrap_n_csr_drain (app/jobs/sec_first_install_drain.py) uses
#     the shared helper.
#       - "n_csr_retention_cutoff(" appears at least 1 time inside the
#         function body.
#       - "timedelta(days=730)" MUST NOT appear inside the body
#         (forbidden inlined math — single source of truth must be in
#         the helper).
#  D. manifest-worker _parse_sec_n_csr (
#     app/services/manifest_parsers/sec_n_csr.py) pre-fetch retention
#     gate placement.
#       - "n_csr_within_retention(" appears inside _parse_sec_n_csr.
#       - The call MUST sit BEFORE the first "_fetch_ixbrl(" call so
#         pre-cap accessions are tombstoned BEFORE the HTTP fetch
#         spends SEC budget.
#  I. Exactly 1 production *.py file under app/ contains
#     "INSERT INTO fund_metadata_observations" (note: literal text
#     match — mirrors PR7 N-PORT invariant I).
#
# Exits non-zero on the first invariant violation. Wired into
# .githooks/pre-push after check_nport_retention.sh.
#
# Awk-based block parsing (BSD vs GNU `grep -P` portability — PR4
# Codex 1c lesson). Helpers re-implemented locally so this script
# stays self-contained.

set -euo pipefail

FILE_N_CSR_PARSER="app/services/manifest_parsers/sec_n_csr.py"
FILE_BOOTSTRAP_DRAIN="app/jobs/sec_first_install_drain.py"

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

# Count exact regex matches anchored to column 0 (used for invariant A
# to count ONLY the assignment line for N_CSR_RETENTION_DAYS).
count_regex_at_col0() {
  local file="$1" pattern="$2"
  grep -Ec "^${pattern}" "$file" || true
}

# Line number of first literal-substring match inside a function body.
# Body bounded by ``def <name>(`` (at any indent) and the next column-0
# ``def `` or EOF.
first_line_in_function() {
  local file="$1" fname="$2" literal="$3"
  awk -v fname="$fname" -v literal="$literal" '
    BEGIN {
      in_fn = 0
      anchor = "def " fname "("
    }
    {
      if (!in_fn) {
        stripped = $0
        sub(/^[[:space:]]+/, "", stripped)
        if (index(stripped, anchor) == 1) {
          in_fn = 1
          fn_start_nr = NR
        }
        next
      }
      if (NR > fn_start_nr && substr($0, 1, 4) == "def ") {
        in_fn = 0
        next
      }
      if (index($0, literal) > 0) { print NR; exit }
    }
  ' "$file"
}

# Count literal-substring occurrences inside a function body. Same
# bounds as first_line_in_function.
count_in_function() {
  local file="$1" fname="$2" literal="$3"
  awk -v fname="$fname" -v literal="$literal" '
    BEGIN {
      in_fn = 0
      anchor = "def " fname "("
    }
    {
      if (!in_fn) {
        stripped = $0
        sub(/^[[:space:]]+/, "", stripped)
        if (index(stripped, anchor) == 1) {
          in_fn = 1
          fn_start_nr = NR
        }
        next
      }
      if (NR > fn_start_nr && substr($0, 1, 4) == "def ") {
        in_fn = 0
        next
      }
      if (index($0, literal) > 0) { n++ }
    }
    END { print n + 0 }
  ' "$file"
}

# ======================================================================
# A — helpers defined exactly once
# ======================================================================
echo "Checking invariant A (helper definitions + constant)..."

if [[ ! -f "$FILE_N_CSR_PARSER" ]]; then
  fail "missing file: $FILE_N_CSR_PARSER"
else
  cutoff_defs=$(count_literal "$FILE_N_CSR_PARSER" "def n_csr_retention_cutoff(")
  within_defs=$(count_literal "$FILE_N_CSR_PARSER" "def n_csr_within_retention(")
  const_assigns=$(count_regex_at_col0 "$FILE_N_CSR_PARSER" "N_CSR_RETENTION_DAYS:")

  if (( cutoff_defs != 1 )); then
    fail "$FILE_N_CSR_PARSER: expected exactly 1 'def n_csr_retention_cutoff(', found ${cutoff_defs}."
  fi
  if (( within_defs != 1 )); then
    fail "$FILE_N_CSR_PARSER: expected exactly 1 'def n_csr_within_retention(', found ${within_defs}."
  fi
  if (( const_assigns != 1 )); then
    fail "$FILE_N_CSR_PARSER: expected exactly 1 column-0 'N_CSR_RETENTION_DAYS:' annotated assignment, found ${const_assigns}."
  fi
fi

# ======================================================================
# B — bootstrap_n_csr_drain uses the shared helper
# ======================================================================
echo "Checking invariant B (bootstrap_n_csr_drain uses shared helper)..."

if [[ ! -f "$FILE_BOOTSTRAP_DRAIN" ]]; then
  fail "missing file: $FILE_BOOTSTRAP_DRAIN"
else
  cutoff_call_in_drain=$(count_in_function "$FILE_BOOTSTRAP_DRAIN" "bootstrap_n_csr_drain" "n_csr_retention_cutoff(")
  inlined_math_in_drain=$(count_in_function "$FILE_BOOTSTRAP_DRAIN" "bootstrap_n_csr_drain" "timedelta(days=730)")

  if (( cutoff_call_in_drain < 1 )); then
    fail "$FILE_BOOTSTRAP_DRAIN: bootstrap_n_csr_drain missing n_csr_retention_cutoff(...) call — shared helper not wired."
  fi
  if (( inlined_math_in_drain > 0 )); then
    fail "$FILE_BOOTSTRAP_DRAIN: bootstrap_n_csr_drain contains forbidden inlined 'timedelta(days=730)' math — use the shared helper."
  fi
fi

# ======================================================================
# D — manifest-worker pre-fetch retention gate placement
# ======================================================================
echo "Checking invariant D (manifest-worker pre-fetch gate placement)..."

if [[ -f "$FILE_N_CSR_PARSER" ]]; then
  cap_line=$(first_line_in_function "$FILE_N_CSR_PARSER" "_parse_sec_n_csr" "n_csr_within_retention(" || true)
  fetch_line=$(first_line_in_function "$FILE_N_CSR_PARSER" "_parse_sec_n_csr" "_fetch_ixbrl(" || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_N_CSR_PARSER: _parse_sec_n_csr missing n_csr_within_retention(...) pre-fetch gate."
  elif [[ -z "$fetch_line" || "$fetch_line" == "0" ]]; then
    fail "$FILE_N_CSR_PARSER: _parse_sec_n_csr missing '_fetch_ixbrl(' anchor — cannot validate gate placement."
  elif (( cap_line >= fetch_line )); then
    fail "$FILE_N_CSR_PARSER:$cap_line: pre-fetch gate appears AT/AFTER '_fetch_ixbrl(' at line $fetch_line — must short-circuit BEFORE the HTTP fetch."
  fi
fi

# ======================================================================
# I — exactly 1 production *.py file under app/ contains
# 'INSERT INTO fund_metadata_observations'
# ======================================================================
echo "Checking invariant I (single INSERT INTO fund_metadata_observations writer)..."

writer_files=$(grep -rl "INSERT INTO fund_metadata_observations" app --include="*.py" 2>/dev/null || true)
writer_count=0
if [[ -n "$writer_files" ]]; then
  writer_count=$(echo "$writer_files" | wc -l | tr -d ' ')
fi

if (( writer_count != 1 )); then
  fail "expected exactly 1 production *.py file under app/ with 'INSERT INTO fund_metadata_observations', found ${writer_count}:"
  echo "$writer_files" >&2
fi

# ======================================================================
# Summary
# ======================================================================
if (( violations > 0 )); then
  echo "FAIL: ${violations} invariant violation(s) — N-CSR retention cap drift detected." >&2
  exit 1
fi

echo "OK: N-CSR retention cap invariants A / B / D / I satisfied."
