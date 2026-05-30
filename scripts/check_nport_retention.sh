#!/usr/bin/env bash
#
# Lint guard: every N-PORT writer chokepoint MUST honour the 8-quarter
# (24-month) retention cap (#1233 §4.6 PR7).
#
# Mirror of ``check_13f_hr_retention.sh`` (PR6) with the chokepoints
# that don't exist for N-PORT removed: there is no rewash function
# (no PR6-equivalent invariant F) and no ``sync_funds`` repair sweep
# in ``ownership_observations_sync.py`` (no PR6-equivalent invariant
# G). Letter labels A/B/C/D/F/H/I preserved across both guards so
# letter-to-chokepoint mapping is stable for grep-ability:
#
#  A. Helpers defined exactly once, in the canonical module
#     (app/services/n_port_ingest.py).
#  B. ``parse_submissions_index`` wires the intrinsic floor via
#     ``n_port_retention_cutoff(`` AND honours
#     ``period < effective_floor`` (proves the cutoff feeds the
#     existing per-accession filter).
#  C. ``_ingest_single_accession`` defensive post-parse gate placement.
#     ``n_port_within_retention(parsed.period_end`` MUST sit between
#     ``parsed = parse_n_port_payload(`` and the first
#     ``record_fund_observation(`` call.
#  D. Manifest-worker ``_parse_n_port`` post-parse gate placement.
#     ``n_port_within_retention(`` MUST sit between
#     ``parsed = parse_n_port_payload(`` and the first
#     ``record_fund_observation(`` call.
#  F. Bulk dataset archive-level + per-row gate placement.
#       - ``n_port_retention_cutoff(`` exactly 1 call inside
#         ``ingest_nport_dataset_archive`` AND its line number MUST
#         be < the line containing ``for holding in _iter_tsv(``.
#       - ``period_end_early < retention_cutoff`` predicate MUST sit
#         between the cutoff resolution and the first
#         ``record_fund_observation(`` call.
#       - ``rows_skipped_retention`` field on ``NPortIngestResult`` +
#         ≥ 1 increment.
#  H. Repo-wide writer discovery — exactly 2 ``record_fund_observation(``
#     call-sites total across production *.py files under app/
#     (excluding ownership_observations.py and tests): the manifest-worker
#     parser (``sec_n_port.py``) + the single-accession ingest
#     (``n_port_ingest.py``). The guard sums per-file call-sites; adding a
#     NEW per-row call-site (anywhere) trips the guard. (N-PORT's helper
#     hardcodes ``source='nport'`` so there is no per-call ``source='nport'``
#     co-located marker as in PR6 invariant H — plain call-site discovery
#     suffices.) NOTE the bulk-dataset ingester
#     (``sec_nport_dataset_ingest.py``) does NOT call the per-row helper;
#     it writes via a direct bulk ``INSERT … SELECT FROM _stg_nport`` and is
#     retention-gated independently by invariant F (the per-row
#     ``period_end_early < retention_cutoff`` ``continue`` upstream of the
#     staging table). Count relaxed 3→2 when that bulk path landed (#1387;
#     same class as #1382's 13F-HR invariant H).
#  I. Repo-wide writer discovery — exactly 2 production *.py files
#     under app/ contain ``INSERT INTO ownership_funds_observations (``
#     (note trailing column-list paren): the canonical per-row helper in
#     ``ownership_observations.py`` AND the retention-gated bulk
#     ``INSERT … SELECT`` in ``sec_nport_dataset_ingest.py`` (invariant F
#     proves its gate). Count relaxed 1→2 when the bulk path landed
#     (#1387).
#
# Exits non-zero on the first invariant violation. Wired into
# ``.githooks/pre-push`` after ``check_13f_hr_retention.sh``.
#
# Awk-based block parsing (BSD vs GNU ``grep -P`` portability — PR4
# Codex 1c lesson). FULL retention predicate counted, not bare
# ``%(retention_cutoff)s`` token (PR4 Codex 2 MED).

set -euo pipefail

FILE_N_PORT_INGEST="app/services/n_port_ingest.py"
FILE_MANIFEST_NPORT="app/services/manifest_parsers/sec_n_port.py"
FILE_BULK_DATASET="app/services/sec_nport_dataset_ingest.py"

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

# Count occurrences of a literal substring inside a file.
count_literal() {
  local file="$1" pat="$2"
  grep -Fc "$pat" "$file" || true
}

# Count parenthesis-suffixed helper call-sites EXCLUDING ``def`` lines
# (helper definitions) and pure-comment lines.
count_call_sites() {
  local file="$1" symbol="$2"
  awk -v sym="$symbol" '
    /^[[:space:]]*#/   { next }
    /^[[:space:]]*def[[:space:]]/ { next }
    {
      pat = sym "("
      if (index($0, pat) > 0) n++
    }
    END { print n + 0 }
  ' "$file"
}

# Line number of first literal-substring match inside a function body.
# Body bounded by ``def <name>(`` and the next column-0 ``def `` or EOF.
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

# Count call-sites of a literal substring inside a function body.
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
echo "Checking invariant A (helper definitions)..."

if [[ ! -f "$FILE_N_PORT_INGEST" ]]; then
  fail "missing file: $FILE_N_PORT_INGEST"
else
  cutoff_defs=$(count_literal "$FILE_N_PORT_INGEST" "def n_port_retention_cutoff(")
  within_defs=$(count_literal "$FILE_N_PORT_INGEST" "def n_port_within_retention(")
  if (( cutoff_defs != 1 )); then
    fail "$FILE_N_PORT_INGEST: expected exactly 1 def n_port_retention_cutoff(...), found ${cutoff_defs}."
  fi
  if (( within_defs != 1 )); then
    fail "$FILE_N_PORT_INGEST: expected exactly 1 def n_port_within_retention(...), found ${within_defs}."
  fi
fi

# ======================================================================
# B — parse_submissions_index wires the intrinsic floor
# ======================================================================
echo "Checking invariant B (parse_submissions_index floor)..."

if [[ -f "$FILE_N_PORT_INGEST" ]]; then
  cutoff_call_in_psi=$(count_in_function "$FILE_N_PORT_INGEST" "parse_submissions_index" "n_port_retention_cutoff(")
  filter_marker_in_psi=$(count_in_function "$FILE_N_PORT_INGEST" "parse_submissions_index" "period < effective_floor")
  if (( cutoff_call_in_psi < 1 )); then
    fail "$FILE_N_PORT_INGEST: parse_submissions_index missing n_port_retention_cutoff(...) call — intrinsic floor not wired."
  fi
  if (( filter_marker_in_psi < 1 )); then
    fail "$FILE_N_PORT_INGEST: parse_submissions_index missing 'period < effective_floor' filter — cutoff not connected to the per-accession gate."
  fi
fi

# ======================================================================
# C — _ingest_single_accession defensive post-parse gate placement
# ======================================================================
echo "Checking invariant C (_ingest_single_accession post-parse gate)..."

if [[ -f "$FILE_N_PORT_INGEST" ]]; then
  cap_line=$(first_line_in_function "$FILE_N_PORT_INGEST" "_ingest_single_accession" "n_port_within_retention(parsed.period_end" || true)
  parse_line=$(first_line_in_function "$FILE_N_PORT_INGEST" "_ingest_single_accession" "parsed = parse_n_port_payload(" || true)
  write_line=$(first_line_in_function "$FILE_N_PORT_INGEST" "_ingest_single_accession" "record_fund_observation(" || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_N_PORT_INGEST: _ingest_single_accession missing n_port_within_retention(parsed.period_end ...) post-parse gate."
  elif [[ -z "$parse_line" || "$parse_line" == "0" ]]; then
    fail "$FILE_N_PORT_INGEST: _ingest_single_accession missing 'parsed = parse_n_port_payload(' anchor — cannot validate cap placement."
  elif [[ -z "$write_line" || "$write_line" == "0" ]]; then
    fail "$FILE_N_PORT_INGEST: _ingest_single_accession missing 'record_fund_observation(' anchor — cannot validate cap placement."
  elif (( cap_line <= parse_line )); then
    fail "$FILE_N_PORT_INGEST:$cap_line: post-parse gate appears BEFORE 'parsed = parse_n_port_payload(' at line $parse_line."
  elif (( cap_line >= write_line )); then
    fail "$FILE_N_PORT_INGEST:$cap_line: post-parse gate appears AT/AFTER 'record_fund_observation(' at line $write_line — must short-circuit before write."
  fi
fi

# ======================================================================
# D — manifest-worker post-parse gate placement
# ======================================================================
echo "Checking invariant D (manifest-worker _parse_n_port gate)..."

if [[ ! -f "$FILE_MANIFEST_NPORT" ]]; then
  fail "missing file: $FILE_MANIFEST_NPORT"
else
  cap_line=$(first_line_in_function "$FILE_MANIFEST_NPORT" "_parse_n_port" "n_port_within_retention(" || true)
  parse_line=$(first_line_in_function "$FILE_MANIFEST_NPORT" "_parse_n_port" "parsed = parse_n_port_payload(" || true)
  write_line=$(first_line_in_function "$FILE_MANIFEST_NPORT" "_parse_n_port" "record_fund_observation(" || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_MANIFEST_NPORT: _parse_n_port missing n_port_within_retention(...) post-parse gate."
  elif [[ -z "$parse_line" || "$parse_line" == "0" ]]; then
    fail "$FILE_MANIFEST_NPORT: _parse_n_port missing 'parsed = parse_n_port_payload(' anchor."
  elif [[ -z "$write_line" || "$write_line" == "0" ]]; then
    fail "$FILE_MANIFEST_NPORT: _parse_n_port missing 'record_fund_observation(' anchor."
  elif (( cap_line <= parse_line )); then
    fail "$FILE_MANIFEST_NPORT:$cap_line: gate appears BEFORE 'parsed = parse_n_port_payload(' at line $parse_line."
  elif (( cap_line >= write_line )); then
    fail "$FILE_MANIFEST_NPORT:$cap_line: gate appears AT/AFTER 'record_fund_observation(' at line $write_line — must short-circuit before write."
  fi
fi

# ======================================================================
# F — bulk dataset archive-level + per-row gate
# (PR7 mirror of PR6 invariant E — relabeled F to keep the
#  letter→chokepoint mapping stable across guards.)
# ======================================================================
echo "Checking invariant F (bulk dataset gate)..."

if [[ ! -f "$FILE_BULK_DATASET" ]]; then
  fail "missing file: $FILE_BULK_DATASET"
else
  cutoff_calls=$(count_call_sites "$FILE_BULK_DATASET" "n_port_retention_cutoff")
  predicate_count=$(count_literal "$FILE_BULK_DATASET" "period_end_early < retention_cutoff")
  field_def=$(count_literal "$FILE_BULK_DATASET" "rows_skipped_retention: int")
  inc_count=$(count_literal "$FILE_BULK_DATASET" "result.rows_skipped_retention += 1")

  if (( cutoff_calls != 1 )); then
    fail "$FILE_BULK_DATASET: expected exactly 1 n_port_retention_cutoff(...) call (archive-level anchor), found ${cutoff_calls}."
  fi
  if (( predicate_count < 1 )); then
    fail "$FILE_BULK_DATASET: missing 'period_end_early < retention_cutoff' per-row gate predicate."
  fi
  if (( field_def != 1 )); then
    fail "$FILE_BULK_DATASET: NPortIngestResult missing 'rows_skipped_retention: int' field."
  fi
  if (( inc_count < 1 )); then
    fail "$FILE_BULK_DATASET: missing 'result.rows_skipped_retention += 1' increment in the gate branch."
  fi

  cutoff_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_nport_dataset_archive" "n_port_retention_cutoff(" || true)
  for_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_nport_dataset_archive" "for holding in _iter_tsv(" || true)
  pred_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_nport_dataset_archive" "period_end_early < retention_cutoff" || true)
  reg_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_nport_dataset_archive" "reg_by_accn.get(" || true)
  write_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_nport_dataset_archive" "record_fund_observation(" || true)

  if [[ -n "$cutoff_line" && -n "$for_line" && "$cutoff_line" != "0" && "$for_line" != "0" ]]; then
    if (( cutoff_line >= for_line )); then
      fail "$FILE_BULK_DATASET:$cutoff_line: n_port_retention_cutoff(...) appears at/after 'for holding in _iter_tsv(' at line $for_line — must be hoisted out of the per-row loop."
    fi
  fi
  # Codex 2 WARN on PR7: the gate must short-circuit BEFORE the
  # reg/fund/series/CUSIP lookups, not just before
  # ``record_fund_observation``. A placement that gates only before
  # write would let pre-cap rows whose REGISTRANT/FUND rows are
  # missing land in ``rows_skipped_orphan_accession`` instead of
  # ``rows_skipped_retention``.
  if [[ -n "$pred_line" && -n "$reg_line" && "$pred_line" != "0" && "$reg_line" != "0" ]]; then
    if (( pred_line >= reg_line )); then
      fail "$FILE_BULK_DATASET:$pred_line: 'period_end_early < retention_cutoff' appears at/after 'reg_by_accn.get(' at line $reg_line — gate must short-circuit BEFORE the reg/fund lookup so pre-cap rows with missing REGISTRANT/FUND don't land in the orphan bucket (Codex 2 WARN PR7)."
    fi
  fi
  if [[ -n "$pred_line" && -n "$write_line" && "$pred_line" != "0" && "$write_line" != "0" ]]; then
    if (( pred_line >= write_line )); then
      fail "$FILE_BULK_DATASET:$pred_line: 'period_end_early < retention_cutoff' appears at/after 'record_fund_observation(' at line $write_line — gate must short-circuit before write."
    fi
  fi
fi

# ======================================================================
# H — repo-wide record_fund_observation writer discovery
# ======================================================================
echo "Checking invariant H (repo-wide record_fund_observation discovery)..."

# Count CALL-SITES, not files. ``record_fund_observation`` hardcodes
# ``source='nport'`` inside the helper, so unlike PR6's invariant H
# we don't need a co-located ``source='13f'`` filter — every call-site
# is a fund-observation writer by construction.
total_writer_calls=0
declare -a writer_call_files=()
for cand in $(find app -type f -name '*.py' 2>/dev/null | grep -v 'app/services/ownership_observations.py' | sort -u); do
  file_calls=$(awk '
    BEGIN { rc_count=0 }
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*def[[:space:]]/ { next }
    {
      if (index($0, "record_fund_observation(") > 0) rc_count++
    }
    END { print rc_count + 0 }
  ' "$cand")
  if (( file_calls > 0 )); then
    total_writer_calls=$((total_writer_calls + file_calls))
    writer_call_files+=("$cand:$file_calls")
  fi
done

# 2: sec_n_port.py (manifest parser) + n_port_ingest.py (single-accession).
# The bulk ingester sec_nport_dataset_ingest.py does NOT use the per-row
# helper (direct bulk INSERT…SELECT, retention-gated by invariant F) — see
# header note. Relaxed 3→2 with the bulk path (#1387; cf. #1382).
expected_writers=2
if (( total_writer_calls != expected_writers )); then
  fail "expected exactly ${expected_writers} call-sites of record_fund_observation( in app/, found ${total_writer_calls}: ${writer_call_files[*]}. Update the lint guard if you intentionally added/removed a writer."
fi

# ======================================================================
# I — repo-wide INSERT INTO ownership_funds_observations ( discovery
# ======================================================================
echo "Checking invariant I (INSERT INTO ownership_funds_observations ( discovery)..."

# grep -r over app/ rather than `find … -exec grep` so a single
# stderr redirect applies cleanly (shellcheck SC2261/SC2227/SC2046:
# the find form had two competing `2>/dev/null` and an unquoted
# command-substitution file list). `grep -rc` prints `path:N` per
# .py file (0 for non-matching); awk sums the count field. The
# trailing `|| true` keeps the zero-match case (writer removed —
# grep exits 1, propagated by `set -o pipefail`) from aborting the
# assignment before the count-mismatch check below reports it. #1257.
total_insert_calls=$(grep -rcE "INSERT INTO ownership_funds_observations \(" app --include='*.py' 2>/dev/null | awk -F: '{s+=$NF} END {print s+0}' || true)
insert_files=$(grep -rlE "INSERT INTO ownership_funds_observations \(" app --include='*.py' 2>/dev/null | sort -u || true)

# 2: ownership_observations.py (canonical per-row helper) +
# sec_nport_dataset_ingest.py (retention-gated bulk INSERT…SELECT, proven by
# invariant F). Relaxed 1→2 with the bulk path (#1387).
expected_inserts=2
if (( total_insert_calls != expected_inserts )); then
  fail "expected exactly ${expected_inserts} 'INSERT INTO ownership_funds_observations (' call-sites in app/, found ${total_insert_calls} across files: ${insert_files}. Update the lint guard if you intentionally added/removed a writer."
fi

# ======================================================================
# Verdict
# ======================================================================

if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} N-PORT retention-cap invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.6 +" >&2
  echo "docs/superpowers/plans/2026-05-20-pr7-nport-8q-cap.md §6 for the rules." >&2
  exit 1
fi

echo "OK: N-PORT 8-quarter retention cap honoured at every chokepoint."
