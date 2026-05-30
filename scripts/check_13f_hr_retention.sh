#!/usr/bin/env bash
#
# Lint guard: every 13F-HR writer chokepoint MUST honour the 8-quarter
# retention cap (#1233 §4.5 PR6).
#
# Nine PR5-style invariants (A-I), all enforced at PLACEMENT or
# repo-wide writer discovery — not just file-level presence:
#
#  A. Helpers defined exactly once, in the canonical module
#     (app/services/institutional_holdings.py).
#  B. ``parse_submissions_index`` wires the intrinsic floor via
#     ``thirteen_f_retention_cutoff(`` AND honours
#     ``period < effective_floor`` (proves the cutoff feeds the
#     existing per-accession filter).
#  C. ``_ingest_single_accession`` defensive post-parse gate placement.
#     ``thirteen_f_within_retention(info.period_of_report`` MUST sit
#     between ``info = parse_primary_doc(`` and the first
#     ``infotable_xml = sec.fetch_document_text(`` call.
#  D. Manifest-worker ``_parse_13f_hr`` post-parse gate placement.
#     ``thirteen_f_within_retention(`` MUST sit between
#     ``info = parse_primary_doc(`` and the first
#     ``infotable_xml =`` line.
#  E. Bulk dataset archive-level + per-row gate placement.
#       - ``thirteen_f_retention_cutoff(`` exactly 1 call AND its line
#         number MUST be < the line containing ``for row in _iter_tsv``.
#       - ``period_end < retention_cutoff`` predicate MUST appear
#         between ``period_end = _parse_period_end`` and
#         ``record_institution_observation(``.
#       - ``rows_skipped_retention`` field on ``Form13FIngestResult`` +
#         ≥ 1 increment.
#  F. Rewash ``_apply_13f_infotable`` rescue-branch-only gate.
#     ``thirteen_f_within_retention(`` exactly 1 call AND its line
#     number MUST sit between the rescue-fallback SQL anchor
#     (``JOIN institutional_filers f ON f.cik = log.filer_cik``) and
#     the parser call ``parse_infotable(raw_doc.payload)``.
#  G. ``ownership_observations_sync.sync_institutions`` SQL predicate.
#     ``thirteen_f_retention_cutoff(`` ≥ 1 call AND SQL fragment
#     ``ih.period_of_report >= %(retention_cutoff)s`` ≥ 1.
#  H. Repo-wide writer discovery — exactly 3 production *.py files
#     under app/ (excluding ownership_observations.py and tests) call
#     ``record_institution_observation(`` within ±20 lines of a
#     quote-style-agnostic ``source = '13f'`` / ``source = "13f"``
#     match.
#  I. Repo-wide writer discovery — exactly 1 production *.py file
#     under app/ contains ``INSERT INTO institutional_holdings (``
#     (note trailing column-list paren; distinguishes from the
#     ``_ingest_log`` table).
#
# Exits non-zero on the first invariant violation. Wired into
# ``.githooks/pre-push`` after ``check_def14a_cap.sh``.
#
# Awk-based block parsing (BSD vs GNU ``grep -P`` portability — PR4
# Codex 1c lesson). The FULL retention predicate is counted, not bare
# ``%(retention_cutoff)s`` (PR4 Codex 2 MED — bare-param counting let
# unused params satisfy parity).

set -euo pipefail

FILE_INSTITUTIONAL_HOLDINGS="app/services/institutional_holdings.py"
FILE_MANIFEST_13F="app/services/manifest_parsers/sec_13f_hr.py"
FILE_BULK_DATASET="app/services/sec_13f_dataset_ingest.py"
FILE_REWASH="app/services/rewash_filings.py"
FILE_OBS_SYNC="app/services/ownership_observations_sync.py"

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

# Count occurrences of a literal substring inside a file. ``-F`` for
# literal (no regex), ``-c`` returns 0 on no matches.
count_literal() {
  local file="$1" pat="$2"
  grep -Fc "$pat" "$file" || true
}

# Count parenthesis-suffixed helper call-sites EXCLUDING ``def`` lines
# (helper definitions themselves) and pure-comment lines. Mirrors the
# PR4 ``count_call_sites`` helper.
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
# Body bounded by ``def <name>(`` and the next column-0 ``def `` or
# EOF. BSD awk doesn't accept ``(`` in regex — use ``index()`` for
# literal substring matching.
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

if [[ ! -f "$FILE_INSTITUTIONAL_HOLDINGS" ]]; then
  fail "missing file: $FILE_INSTITUTIONAL_HOLDINGS"
else
  cutoff_defs=$(count_literal "$FILE_INSTITUTIONAL_HOLDINGS" "def thirteen_f_retention_cutoff(")
  within_defs=$(count_literal "$FILE_INSTITUTIONAL_HOLDINGS" "def thirteen_f_within_retention(")
  if (( cutoff_defs != 1 )); then
    fail "$FILE_INSTITUTIONAL_HOLDINGS: expected exactly 1 def thirteen_f_retention_cutoff(...), found ${cutoff_defs}."
  fi
  if (( within_defs != 1 )); then
    fail "$FILE_INSTITUTIONAL_HOLDINGS: expected exactly 1 def thirteen_f_within_retention(...), found ${within_defs}."
  fi
fi

# ======================================================================
# B — parse_submissions_index wires the intrinsic floor
# ======================================================================
echo "Checking invariant B (parse_submissions_index floor)..."

if [[ -f "$FILE_INSTITUTIONAL_HOLDINGS" ]]; then
  cutoff_call_in_psi=$(count_in_function "$FILE_INSTITUTIONAL_HOLDINGS" "parse_submissions_index" "thirteen_f_retention_cutoff(")
  filter_marker_in_psi=$(count_in_function "$FILE_INSTITUTIONAL_HOLDINGS" "parse_submissions_index" "period < effective_floor")
  if (( cutoff_call_in_psi < 1 )); then
    fail "$FILE_INSTITUTIONAL_HOLDINGS: parse_submissions_index missing thirteen_f_retention_cutoff(...) call — intrinsic floor not wired."
  fi
  if (( filter_marker_in_psi < 1 )); then
    fail "$FILE_INSTITUTIONAL_HOLDINGS: parse_submissions_index missing 'period < effective_floor' filter — cutoff not connected to the per-accession gate."
  fi
fi

# ======================================================================
# C — _ingest_single_accession defensive post-parse gate placement
# ======================================================================
echo "Checking invariant C (_ingest_single_accession post-parse gate)..."

if [[ -f "$FILE_INSTITUTIONAL_HOLDINGS" ]]; then
  cap_line=$(first_line_in_function "$FILE_INSTITUTIONAL_HOLDINGS" "_ingest_single_accession" "thirteen_f_within_retention(info.period_of_report" || true)
  parse_line=$(first_line_in_function "$FILE_INSTITUTIONAL_HOLDINGS" "_ingest_single_accession" "info = parse_primary_doc(" || true)
  infotable_line=$(first_line_in_function "$FILE_INSTITUTIONAL_HOLDINGS" "_ingest_single_accession" "infotable_xml = sec.fetch_document_text(" || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_INSTITUTIONAL_HOLDINGS: _ingest_single_accession missing thirteen_f_within_retention(info.period_of_report ...) post-parse gate."
  elif [[ -z "$parse_line" || "$parse_line" == "0" ]]; then
    fail "$FILE_INSTITUTIONAL_HOLDINGS: _ingest_single_accession missing 'info = parse_primary_doc(' anchor — cannot validate cap placement."
  elif [[ -z "$infotable_line" || "$infotable_line" == "0" ]]; then
    fail "$FILE_INSTITUTIONAL_HOLDINGS: _ingest_single_accession missing 'infotable_xml = sec.fetch_document_text(' anchor — cannot validate cap placement."
  elif (( cap_line <= parse_line )); then
    fail "$FILE_INSTITUTIONAL_HOLDINGS:$cap_line: post-parse gate appears BEFORE 'info = parse_primary_doc(' at line $parse_line."
  elif (( cap_line >= infotable_line )); then
    fail "$FILE_INSTITUTIONAL_HOLDINGS:$cap_line: post-parse gate appears AFTER 'infotable_xml = sec.fetch_document_text(' at line $infotable_line — pre-fetch ordering violated."
  fi
fi

# ======================================================================
# D — manifest-worker post-parse gate placement
# ======================================================================
echo "Checking invariant D (manifest-worker _parse_13f_hr gate)..."

if [[ ! -f "$FILE_MANIFEST_13F" ]]; then
  fail "missing file: $FILE_MANIFEST_13F"
else
  cap_line=$(first_line_in_function "$FILE_MANIFEST_13F" "_parse_13f_hr" "thirteen_f_within_retention(" || true)
  parse_line=$(first_line_in_function "$FILE_MANIFEST_13F" "_parse_13f_hr" "info = parse_primary_doc(" || true)
  infotable_line=$(first_line_in_function "$FILE_MANIFEST_13F" "_parse_13f_hr" "infotable_xml =" || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_MANIFEST_13F: _parse_13f_hr missing thirteen_f_within_retention(...) post-parse gate."
  elif [[ -z "$parse_line" || "$parse_line" == "0" ]]; then
    fail "$FILE_MANIFEST_13F: _parse_13f_hr missing 'info = parse_primary_doc(' anchor."
  elif [[ -z "$infotable_line" || "$infotable_line" == "0" ]]; then
    fail "$FILE_MANIFEST_13F: _parse_13f_hr missing 'infotable_xml =' anchor."
  elif (( cap_line <= parse_line )); then
    fail "$FILE_MANIFEST_13F:$cap_line: gate appears BEFORE 'info = parse_primary_doc(' at line $parse_line."
  elif (( cap_line >= infotable_line )); then
    fail "$FILE_MANIFEST_13F:$cap_line: gate appears AFTER 'infotable_xml =' at line $infotable_line — pre-fetch ordering violated."
  fi
fi

# ======================================================================
# E — bulk dataset archive-level + per-row gate
# ======================================================================
echo "Checking invariant E (bulk dataset gate)..."

if [[ ! -f "$FILE_BULK_DATASET" ]]; then
  fail "missing file: $FILE_BULK_DATASET"
else
  cutoff_calls=$(count_call_sites "$FILE_BULK_DATASET" "thirteen_f_retention_cutoff")
  predicate_count=$(count_literal "$FILE_BULK_DATASET" "period_end < retention_cutoff")
  field_def=$(count_literal "$FILE_BULK_DATASET" "rows_skipped_retention: int")
  inc_count=$(count_literal "$FILE_BULK_DATASET" "result.rows_skipped_retention += 1")

  if (( cutoff_calls != 1 )); then
    fail "$FILE_BULK_DATASET: expected exactly 1 thirteen_f_retention_cutoff(...) call (archive-level anchor), found ${cutoff_calls}."
  fi
  if (( predicate_count < 1 )); then
    fail "$FILE_BULK_DATASET: missing 'period_end < retention_cutoff' per-row gate predicate."
  fi
  if (( field_def != 1 )); then
    fail "$FILE_BULK_DATASET: Form13FIngestResult missing 'rows_skipped_retention: int' field."
  fi
  if (( inc_count < 1 )); then
    fail "$FILE_BULK_DATASET: missing 'result.rows_skipped_retention += 1' increment in the gate branch."
  fi

  cutoff_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_13f_dataset_archive" "thirteen_f_retention_cutoff(" || true)
  for_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_13f_dataset_archive" "for row in _iter_tsv" || true)
  pred_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_13f_dataset_archive" "period_end < retention_cutoff" || true)
  parse_pe_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_13f_dataset_archive" "period_end = _parse_period_end" || true)
  write_line=$(first_line_in_function "$FILE_BULK_DATASET" "ingest_13f_dataset_archive" "record_institution_observation(" || true)

  if [[ -n "$cutoff_line" && -n "$for_line" && "$cutoff_line" != "0" && "$for_line" != "0" ]]; then
    if (( cutoff_line >= for_line )); then
      fail "$FILE_BULK_DATASET:$cutoff_line: thirteen_f_retention_cutoff(...) appears at/after 'for row in _iter_tsv' at line $for_line — must be hoisted out of the per-row loop."
    fi
  fi
  if [[ -n "$pred_line" && -n "$parse_pe_line" && -n "$write_line" && "$pred_line" != "0" ]]; then
    if (( pred_line <= parse_pe_line )); then
      fail "$FILE_BULK_DATASET:$pred_line: 'period_end < retention_cutoff' appears BEFORE 'period_end = _parse_period_end' at line $parse_pe_line."
    elif (( pred_line >= write_line )); then
      fail "$FILE_BULK_DATASET:$pred_line: 'period_end < retention_cutoff' appears at/after 'record_institution_observation(' at line $write_line — gate must short-circuit before write."
    fi
  fi
fi

# ======================================================================
# F — rewash _apply_13f_infotable rescue-branch-only gate
# ======================================================================
echo "Checking invariant F (rewash rescue-branch gate)..."

if [[ ! -f "$FILE_REWASH" ]]; then
  fail "missing file: $FILE_REWASH"
else
  cap_count=$(count_in_function "$FILE_REWASH" "_apply_13f_infotable" "thirteen_f_within_retention(")
  if (( cap_count != 1 )); then
    fail "$FILE_REWASH: _apply_13f_infotable should contain exactly 1 thirteen_f_within_retention(...) call (rescue-branch gate); found ${cap_count}."
  else
    cap_line=$(first_line_in_function "$FILE_REWASH" "_apply_13f_infotable" "thirteen_f_within_retention(" || true)
    anchor_line=$(first_line_in_function "$FILE_REWASH" "_apply_13f_infotable" "JOIN institutional_filers f ON f.cik = log.filer_cik" || true)
    parse_line=$(first_line_in_function "$FILE_REWASH" "_apply_13f_infotable" "parse_infotable(raw_doc.payload)" || true)

    if [[ -z "$anchor_line" || "$anchor_line" == "0" ]]; then
      fail "$FILE_REWASH: _apply_13f_infotable missing rescue-fallback 'JOIN institutional_filers f ON f.cik = log.filer_cik' anchor."
    elif [[ -z "$parse_line" || "$parse_line" == "0" ]]; then
      fail "$FILE_REWASH: _apply_13f_infotable missing 'parse_infotable(raw_doc.payload)' anchor."
    elif (( cap_line <= anchor_line )); then
      fail "$FILE_REWASH:$cap_line: thirteen_f_within_retention(...) appears at/before the rescue SQL anchor at line $anchor_line — happy-path branch must NOT be capped."
    elif (( cap_line >= parse_line )); then
      fail "$FILE_REWASH:$cap_line: thirteen_f_within_retention(...) appears at/after parse_infotable(...) at line $parse_line — rescue gate must short-circuit before parse."
    fi
  fi
fi

# ======================================================================
# G — ownership_observations_sync.sync_institutions SQL predicate
# ======================================================================
echo "Checking invariant G (sync_institutions cap predicate)..."

if [[ ! -f "$FILE_OBS_SYNC" ]]; then
  fail "missing file: $FILE_OBS_SYNC"
else
  cutoff_calls=$(count_in_function "$FILE_OBS_SYNC" "sync_institutions" "thirteen_f_retention_cutoff(")
  predicate_count=$(count_in_function "$FILE_OBS_SYNC" "sync_institutions" "ih.period_of_report >= %(retention_cutoff)s")
  if (( cutoff_calls < 1 )); then
    fail "$FILE_OBS_SYNC: sync_institutions missing thirteen_f_retention_cutoff(...) call."
  fi
  if (( predicate_count < 1 )); then
    fail "$FILE_OBS_SYNC: sync_institutions missing 'ih.period_of_report >= %(retention_cutoff)s' SQL predicate."
  fi
fi

# ======================================================================
# H — repo-wide writer discovery for source = '13f' / "13f"
# ======================================================================
echo "Checking invariant H (repo-wide record_institution_observation source='13f' discovery)..."

# Codex 2 MED — count CALL-SITES, not files. A new
# ``record_institution_observation(... source='13f')`` added inside an
# already-approved file would otherwise slip past a path-set check.
# Each approved file currently has exactly ONE such call-site:
#   * app/services/institutional_holdings.py (1)
#   * app/services/ownership_observations_sync.py (1)
# Total = 2. (#1382 fix: was 3 — sec_13f_dataset_ingest.py no longer
# routes through the per-row ``record_institution_observation`` helper;
# the bulk dataset path uses a bulk INSERT writer whose 8-quarter
# retention gate is asserted independently by invariant E above
# (``ingest_13f_dataset_archive`` short-circuits before write). So H's
# per-row count is legitimately 2, not 3 — relaxing it does not weaken
# coverage of the bulk path, which E owns.) Tighten the check to count
# ``record_institution_observation(`` call-sites that sit within ±20
# lines of a ``source = '13f'`` / ``source = "13f"`` kwarg match.
total_writer_calls=0
declare -a writer_call_files=()
for cand in $(find app -type f -name '*.py' 2>/dev/null | grep -v 'app/services/ownership_observations.py' | sort -u); do
  # Count call-sites in this file that have ``source = '13f'`` within
  # ±20 lines. awk: collect line numbers of both patterns, then count
  # ``record_*`` lines that have a ``source='13f'`` line within ±20.
  file_calls=$(awk '
    BEGIN { rc_count=0 }
    {
      lines[NR] = $0
    }
    END {
      for (i=1; i<=NR; i++) {
        if (index(lines[i], "record_institution_observation(") > 0) {
          lo = i - 20; if (lo < 1) lo = 1
          hi = i + 20; if (hi > NR) hi = NR
          for (j=lo; j<=hi; j++) {
            if (match(lines[j], /source[[:space:]]*=[[:space:]]*["'\'']13f["'\'']/) > 0) {
              rc_count++
              break
            }
          }
        }
      }
      print rc_count + 0
    }
  ' "$cand")
  if (( file_calls > 0 )); then
    total_writer_calls=$((total_writer_calls + file_calls))
    writer_call_files+=("$cand:$file_calls")
  fi
done

expected_writers=2
if (( total_writer_calls != expected_writers )); then
  fail "expected exactly ${expected_writers} call-sites of record_institution_observation(... source='13f'|\"13f\") in app/, found ${total_writer_calls}: ${writer_call_files[*]}. Update the lint guard if you intentionally added/removed a writer."
fi

# ======================================================================
# I — repo-wide INSERT INTO institutional_holdings ( discovery
# ======================================================================
echo "Checking invariant I (INSERT INTO institutional_holdings ( discovery)..."

# Codex 2 MED — count CALL-SITES (literal matches), not files. A
# second ``INSERT INTO institutional_holdings (`` added inside the
# same already-approved file would otherwise slip past a path-set
# check. The current canonical writer is ``_upsert_holding`` in
# ``app/services/institutional_holdings.py`` — exactly one match
# repo-wide. Note the trailing column-list paren distinguishes from
# the ``_ingest_log`` table.
# grep -r over app/ rather than `find … -exec grep` so a single
# stderr redirect applies cleanly (shellcheck SC2261/SC2227/SC2046:
# the find form had two competing `2>/dev/null` and an unquoted
# command-substitution file list). `grep -rc` prints `path:N` per
# .py file (0 for non-matching); awk sums the count field. The
# trailing `|| true` keeps the zero-match case (writer removed —
# grep exits 1, propagated by `set -o pipefail`) from aborting the
# assignment before the count-mismatch check below reports it. #1257.
total_insert_calls=$(grep -rcE "INSERT INTO institutional_holdings \(" app --include='*.py' 2>/dev/null | awk -F: '{s+=$NF} END {print s+0}' || true)
insert_files=$(grep -rlE "INSERT INTO institutional_holdings \(" app --include='*.py' 2>/dev/null | sort -u || true)

expected_inserts=1
if (( total_insert_calls != expected_inserts )); then
  fail "expected exactly ${expected_inserts} 'INSERT INTO institutional_holdings (' call-site in app/, found ${total_insert_calls} across files: ${insert_files}. Update the lint guard if you intentionally added/removed a writer."
fi

# ======================================================================
# Verdict
# ======================================================================

if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} 13F-HR retention-cap invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.5 +" >&2
  echo "docs/superpowers/plans/2026-05-20-pr6-13f-hr-8q-cap.md §6 for the rules." >&2
  exit 1
fi

echo "OK: 13F-HR 8-quarter retention cap honoured at every chokepoint."
