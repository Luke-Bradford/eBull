#!/usr/bin/env bash
#
# Lint guard: every SC 13D/G writer chokepoint MUST honour the 3y
# filed_at retention cap (#1233 §4.8 PR11). Seven placement invariants
# (A/C/D/E/F/G/H) covering helpers, manifest pre-fetch gate, sync gate,
# refresh-current exemption, writer surface, dormant-code retirement,
# and rewash rescue gate.
#
# Empirical-pivot history (2026-05-21): PR11 originally landed 12
# invariants (A-L) covering an additional universe-issuer-CIK
# discovery layer (``app/services/sec_13dg_discovery.py``) + a
# discovery-time issuer-hint side table
# (``sec_13dg_discovery_issuer_hint``) + a 5-case CUSIP-vs-hint
# cross-validation branch in the manifest parser. Operator smoke
# against AAPL/GME/MSFT/JPM/HD revealed that ``efts.sec.gov/LATEST/
# search-index`` post-2024-12-18 does NOT index SC 13D/G by SUBJECT
# CIK, only by FILER CIK — the discovery layer + hint table were
# therefore retired and invariants B (discovery wires the cutoff
# helper) / I (discovery uses provider throttle) / J (discovery uses
# record_manifest_entry) / K (hint + manifest atomic) / L (hint UPSERT
# uses DO UPDATE) were pruned with them. The legacy daily-index path
# (``app/services/filings_history.py``) remains the discovery
# mechanism and already ships ~575k SC 13D/G manifest rows in dev DB;
# the seven surviving invariants cover the entire post-pivot writer
# surface.
#
# Invariants (spec §3.6 v8 post-pivot):
#
#  A. ``app/services/blockholders.py`` defines exactly one
#     ``def blockholders_retention_cutoff(`` AND exactly one
#     ``def blockholders_within_retention(``. Greps for the literal
#     ``def `` prefix on both names.
#  C. ``app/services/manifest_parsers/sec_13dg.py::_parse_13dg`` calls
#     ``blockholders_within_retention(`` on a line whose line number
#     precedes BOTH the first ``fetch_document_text(`` call AND the
#     first ``store_raw(`` call inside the same function block.
#  D. ``app/services/ownership_observations_sync.py::sync_blockholders``
#     body contains (a) ``blockholders_retention_cutoff()`` AT LEAST
#     ONCE AND (b) a ``bf.filed_at >= `` predicate AT LEAST ONCE; AND
#     simultaneously FORBIDS any ``fe.filing_date >=`` /
#     ``filing_events.filing_date >=`` predicate anywhere in the
#     function body. Codex 1a HIGH #4 pin: the LEFT-JOIN-WHERE pattern
#     null-rejects rows missing a ``filing_events`` entry.
#  E. ``app/services/ownership_observations.py::refresh_blockholders_current``
#     body does NOT reference ``blockholders_retention_cutoff`` or
#     ``blockholders_within_retention`` anywhere. Refresh-current is
#     EXEMPT per parent spec §6.3 — capping refresh would actively
#     delete pre-wipe rows from ``_current``.
#  F. No raw ``INSERT INTO ownership_blockholders_observations`` AND no
#     raw ``INSERT INTO blockholder_filings`` outside the helper-gated
#     chokepoints. Allow-list:
#       * ``app/services/ownership_observations.py`` —
#         ``record_blockholder_observation`` (the canonical
#         write-through helper for observations).
#       * ``app/services/blockholders.py`` — ``_upsert_filing_row``
#         (the lower-level helper for ``blockholder_filings``).
#     Word-bounded so ``blockholder_filings_ingest_log`` writes inside
#     ``blockholders.py`` don't inflate the count.
#  G. Dormant entrypoints stay deleted. The four 13D/G symbols
#     ``ingest_all_active_filers``, ``ingest_filer_blockholders``,
#     ``_list_active_filer_seeds``, ``seed_filer`` MUST NOT re-appear
#     inside any of the surviving blockholder-specific modules:
#       * ``app/services/blockholders.py``
#       * ``app/services/manifest_parsers/sec_13dg.py``
#     Module-scoped check because the SAME identifier names also live
#     in ``app/services/institutional_holdings.py`` (13F-HR variant) +
#     ``app/services/ncen_classifier.py`` (N-CEN variant); a blunt
#     repo-wide grep would false-fail. The 13D/G resurrection vector
#     is re-introducing these names into the blockholder module set.
#     Exception inside ``blockholders.py``: the module-level docstring
#     names them in the retirement note — that's a documented
#     historical reference, not a resurrection. The exception is
#     scoped to the file's top-of-file docstring lines via grep on
#     the four PARENTHESISED occurrences.
#  H. ``app/services/rewash_filings.py::_apply_blockholders`` function
#     body has ``blockholders_within_retention(`` AND the call line
#     precedes ANY ``DELETE FROM blockholder_filings`` OR
#     ``_upsert_filing_row(`` invocation inside the same function
#     block. Codex 1b MEDIUM branch-order pin: rescue-path gate must
#     fire BEFORE the destructive replace-then-insert sequence.
#
# Exits non-zero on the first invariant violation. Wired into
# ``.githooks/pre-push`` after ``check_form3_latest_per_pair.sh`` and
# before ``check_archive_url_agent_guard.sh``.
#
# Awk-based block parsing (BSD vs GNU ``grep -P`` portability — PR4
# Codex 1c lesson). Empty-grep ``wc -l`` guarded throughout (per
# PR10a Codex iter 1 lesson: ``echo "" | wc -l`` returns 1).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FILE_HELPERS="app/services/blockholders.py"
FILE_MANIFEST="app/services/manifest_parsers/sec_13dg.py"
FILE_SYNC="app/services/ownership_observations_sync.py"
FILE_REFRESH="app/services/ownership_observations.py"
FILE_REWASH="app/services/rewash_filings.py"

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

# Line number of first literal-substring match inside a function body.
# Body bounded by ``def <name>(`` (at any indent) and the next column-0
# ``def ``/``class `` or EOF. BSD awk doesn't accept ``(`` in regex —
# use ``index()`` for literal substring matching.
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
      if (NR > fn_start_nr && (substr($0, 1, 4) == "def " || substr($0, 1, 6) == "class ")) {
        in_fn = 0
        next
      }
      if (index($0, literal) > 0) { print NR; exit }
    }
  ' "$file"
}

# Count literal-substring occurrences inside a function body. Same
# bounds as ``first_line_in_function``.
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
      if (NR > fn_start_nr && (substr($0, 1, 4) == "def " || substr($0, 1, 6) == "class ")) {
        in_fn = 0
        next
      }
      if (index($0, literal) > 0) { n++ }
    }
    END { print n + 0 }
  ' "$file"
}

# ======================================================================
# A — helpers defined exactly once in the canonical module
# ======================================================================
echo "Checking invariant A (blockholders helper definitions)..."

if [[ ! -f "$FILE_HELPERS" ]]; then
  fail "missing file: $FILE_HELPERS"
else
  cutoff_defs=$(count_literal "$FILE_HELPERS" "def blockholders_retention_cutoff(")
  within_defs=$(count_literal "$FILE_HELPERS" "def blockholders_within_retention(")
  if (( cutoff_defs != 1 )); then
    fail "$FILE_HELPERS: expected exactly 1 'def blockholders_retention_cutoff(', found ${cutoff_defs}."
  fi
  if (( within_defs != 1 )); then
    fail "$FILE_HELPERS: expected exactly 1 'def blockholders_within_retention(', found ${within_defs}."
  fi
fi

# ======================================================================
# C — manifest-worker pre-fetch gate placement
# ======================================================================
echo "Checking invariant C (_parse_13dg pre-fetch retention gate)..."

if [[ ! -f "$FILE_MANIFEST" ]]; then
  fail "missing file: $FILE_MANIFEST"
else
  cap_line=$(first_line_in_function "$FILE_MANIFEST" "_parse_13dg" "blockholders_within_retention(" || true)
  fetch_line=$(first_line_in_function "$FILE_MANIFEST" "_parse_13dg" "fetch_document_text(" || true)
  store_line=$(first_line_in_function "$FILE_MANIFEST" "_parse_13dg" "store_raw(" || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_MANIFEST: _parse_13dg missing 'blockholders_within_retention(' pre-fetch gate."
  elif [[ -z "$fetch_line" || "$fetch_line" == "0" ]]; then
    fail "$FILE_MANIFEST: _parse_13dg missing 'fetch_document_text(' anchor — cannot validate gate placement."
  elif [[ -z "$store_line" || "$store_line" == "0" ]]; then
    fail "$FILE_MANIFEST: _parse_13dg missing 'store_raw(' anchor — cannot validate gate placement."
  else
    if (( cap_line >= fetch_line )); then
      fail "$FILE_MANIFEST:$cap_line: pre-fetch gate appears AT/AFTER 'fetch_document_text(' at line $fetch_line — must short-circuit BEFORE the HTTP fetch."
    fi
    if (( cap_line >= store_line )); then
      fail "$FILE_MANIFEST:$cap_line: pre-fetch gate appears AT/AFTER 'store_raw(' at line $store_line — must short-circuit BEFORE the raw-body write."
    fi
  fi
fi

# ======================================================================
# D — sync_blockholders gates on bf.filed_at directly (Codex 1a HIGH #4)
# ======================================================================
echo "Checking invariant D (sync_blockholders bf.filed_at gate; no fe.filing_date)..."

# Body-extraction walker that ALSO strips docstring contents (triple-
# quoted ``"""..."""`` / ``'''...'''`` runs). The sync_blockholders
# docstring cites the forbidden ``WHERE fe.filing_date >= cutoff``
# pattern as a NEGATIVE EXAMPLE in prose explaining why we don't do it
# (post-Codex-1a HIGH #4 lesson). A naive walker would treat that prose
# as a real SQL predicate and false-fail.
#
# The walker:
#   - opens scope at ``def <name>(``;
#   - closes scope at the next column-0 ``def `` / ``class ``;
#   - inside scope, tracks whether the current line is inside an open
#     triple-quoted string by counting ``"""`` / ``'''`` openers on a
#     simple toggle (sufficient because Python's grammar disallows
#     mixed-quote concatenation inside a single docstring line);
#   - prints only non-docstring code lines to stdout.
# The returned text is then grepped with literal strings to count
# real-code occurrences only.
sync_body=$(awk -v fname="sync_blockholders" '
  BEGIN {
    in_fn = 0
    in_doc = 0
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
    if (NR > fn_start_nr && (substr($0, 1, 4) == "def " || substr($0, 1, 6) == "class ")) {
      in_fn = 0
      next
    }
    # Count triple-quote runs on this line. Even count → docstring
    # state unchanged; odd count → docstring state toggles. Print only
    # when NOT inside a docstring AND when the line itself isn'\''t the
    # opener/closer of one (i.e. final state matches pre-line state and
    # was OFF).
    line = $0
    n3 = gsub(/"""/, "&", line)
    line = $0
    n3s = gsub(/'\''\''\''/, "&", line)
    total_toggles = n3 + n3s
    pre_doc = in_doc
    if ((total_toggles % 2) == 1) {
      in_doc = (in_doc == 0) ? 1 : 0
    }
    # Skip the line if it carried any triple-quote run (opener / closer
    # / inline-pair) OR if either pre/post state was inside a docstring.
    if (pre_doc == 0 && in_doc == 0 && total_toggles == 0) {
      print $0
    }
  }
' "$FILE_SYNC")

if [[ ! -f "$FILE_SYNC" ]]; then
  fail "missing file: $FILE_SYNC"
elif [[ -z "$sync_body" ]]; then
  fail "$FILE_SYNC: could not extract sync_blockholders code body — function may be missing or renamed."
else
  # Empty-grep wc -l guarded (PR10a Codex iter 1 lesson):
  # ``echo "" | grep -Fc "<pat>"`` returns 0 cleanly via the ``|| true``
  # tail, so these counts are safe on empty input.
  sync_cutoff_calls=$(printf '%s\n' "$sync_body" | grep -Fc "blockholders_retention_cutoff(" || true)
  sync_bf_pred=$(printf '%s\n' "$sync_body" | grep -Fc "bf.filed_at >= " || true)
  sync_fe_pred=$(printf '%s\n' "$sync_body" | grep -Fc "fe.filing_date >=" || true)
  sync_fe_long_pred=$(printf '%s\n' "$sync_body" | grep -Fc "filing_events.filing_date >=" || true)

  if (( sync_cutoff_calls < 1 )); then
    fail "$FILE_SYNC: sync_blockholders missing 'blockholders_retention_cutoff()' call — cap not wired."
  fi
  if (( sync_bf_pred < 1 )); then
    fail "$FILE_SYNC: sync_blockholders missing 'bf.filed_at >= ' predicate — cap must gate on the raw chain's own column, not on filing_events."
  fi
  if (( sync_fe_pred > 0 )); then
    fail "$FILE_SYNC: sync_blockholders contains forbidden 'fe.filing_date >=' predicate in CODE (${sync_fe_pred} occurrence(s)) — Codex 1a HIGH #4: a LEFT-JOIN-WHERE pattern null-rejects rows missing a filing_events entry. Gate on bf.filed_at instead. Docstring mentions are stripped before counting."
  fi
  if (( sync_fe_long_pred > 0 )); then
    fail "$FILE_SYNC: sync_blockholders contains forbidden 'filing_events.filing_date >=' predicate in CODE (${sync_fe_long_pred} occurrence(s)) — same Codex 1a HIGH #4 lesson as above."
  fi
fi

# ======================================================================
# E — refresh_blockholders_current EXEMPT (no cap helpers inside body)
# ======================================================================
echo "Checking invariant E (refresh_blockholders_current exempt from cap)..."

if [[ ! -f "$FILE_REFRESH" ]]; then
  fail "missing file: $FILE_REFRESH"
else
  refresh_cutoff_refs=$(count_in_function "$FILE_REFRESH" "refresh_blockholders_current" "blockholders_retention_cutoff")
  refresh_within_refs=$(count_in_function "$FILE_REFRESH" "refresh_blockholders_current" "blockholders_within_retention")
  if (( refresh_cutoff_refs > 0 )); then
    fail "$FILE_REFRESH: refresh_blockholders_current body references 'blockholders_retention_cutoff' (${refresh_cutoff_refs} occurrence(s)). Parent spec §6.3: refresh-current must NOT cap — capping would delete pre-wipe rows from _current. Remove the reference."
  fi
  if (( refresh_within_refs > 0 )); then
    fail "$FILE_REFRESH: refresh_blockholders_current body references 'blockholders_within_retention' (${refresh_within_refs} occurrence(s)). Same parent spec §6.3 exemption — remove the reference."
  fi
fi

# ======================================================================
# F — no raw INSERT writers outside the helper-gated chokepoints
# ======================================================================
echo "Checking invariant F (no append writers outside helper-gated chokepoints)..."

# F.1 — INSERT INTO ownership_blockholders_observations. Allowed in
# exactly 1 file (the helper). Empty-grep wc -l guarded per PR10a
# Codex iter 1 lesson.
obs_files=$(grep -rl "INSERT INTO ownership_blockholders_observations" app --include="*.py" 2>/dev/null || true)
obs_count=0
if [[ -n "$obs_files" ]]; then
  obs_count=$(echo "$obs_files" | wc -l | tr -d ' ')
fi
allowed_obs_file="$FILE_REFRESH"
if (( obs_count != 1 )); then
  fail "expected exactly 1 production *.py file under app/ with 'INSERT INTO ownership_blockholders_observations' (the helper in ${allowed_obs_file}), found ${obs_count}:"
  echo "$obs_files" >&2
elif [[ "$obs_files" != "$allowed_obs_file" ]]; then
  fail "the sole 'INSERT INTO ownership_blockholders_observations' writer must be ${allowed_obs_file}, found: $obs_files"
fi

# F.2 — INSERT INTO blockholder_filings. Word-bounded so the sibling
# table ``blockholder_filings_ingest_log`` doesn't inflate the count.
# Allowed in exactly 1 file (the helper inside ``blockholders.py``).
BF_INSERT_REGEX="INSERT INTO blockholder_filings([^A-Za-z0-9_]|$)"
bf_files=$(grep -rlE "$BF_INSERT_REGEX" app --include="*.py" 2>/dev/null || true)
bf_count=0
if [[ -n "$bf_files" ]]; then
  bf_count=$(echo "$bf_files" | wc -l | tr -d ' ')
fi
allowed_bf_file="$FILE_HELPERS"
if (( bf_count != 1 )); then
  fail "expected exactly 1 production *.py file under app/ with 'INSERT INTO blockholder_filings' (word-bounded; the helper in ${allowed_bf_file}), found ${bf_count}:"
  echo "$bf_files" >&2
elif [[ "$bf_files" != "$allowed_bf_file" ]]; then
  fail "the sole 'INSERT INTO blockholder_filings' writer must be ${allowed_bf_file}, found: $bf_files"
fi

# ======================================================================
# G — dormant entrypoints stay deleted (blockholder-module scope)
# ======================================================================
echo "Checking invariant G (dormant 13D/G entrypoints stay deleted)..."

# Module scope: blockholder-specific files only. The same identifier
# names are LIVE inside the 13F-HR module (institutional_holdings.py)
# and the N-CEN module (ncen_classifier.py) — those are co-tenants of
# the function names, NOT 13D/G resurrection. Resurrection vector =
# re-introducing the names into a blockholder-specific module.
#
# Exception inside ``blockholders.py``: the module-level docstring
# names the retired symbols in the historical retirement note (line
# 3-5 in the post-Phase-9 file). Each historical mention occurs
# wrapped in DOUBLE BACKTICKS (``ingest_all_active_filers``); a
# resurrection would be a bare identifier (``ingest_all_active_filers``
# WITHOUT backticks) at a definition / call / import site. We count
# total occurrences then subtract the docstring backtick-wrapped
# count; the remainder MUST be zero.
#
# v8 empirical pivot 2026-05-21: ``sec_13dg_discovery.py`` was deleted,
# so the surviving blockholder module set is just helpers + manifest
# parser. Re-introduction of any dormant entrypoint in either file is
# still forbidden.
DORMANT_SYMBOLS=(
  "ingest_all_active_filers"
  "ingest_filer_blockholders"
  "_list_active_filer_seeds"
  "seed_filer"
)
BLOCKHOLDER_MODULES=(
  "$FILE_HELPERS"
  "$FILE_MANIFEST"
)

for mod in "${BLOCKHOLDER_MODULES[@]}"; do
  [[ -f "$mod" ]] || continue
  for sym in "${DORMANT_SYMBOLS[@]}"; do
    total=$(count_literal "$mod" "$sym")
    # Count docstring backtick-wrapped historical mentions.
    backticked=$(count_literal "$mod" "\`\`${sym}\`\`")
    bare=$((total - backticked))
    if (( bare > 0 )); then
      fail "$mod: dormant 13D/G entrypoint '${sym}' re-appears (${bare} bare occurrence(s), ${backticked} historical-doc mention(s)). PR11 retired this symbol; resurrection forbidden. If you genuinely need a new entrypoint, give it a distinct name AND update this lint."
    fi
  done
done

# ======================================================================
# H — rewash _apply_blockholders rescue gate precedes destructive ops
# ======================================================================
echo "Checking invariant H (rewash rescue gate precedes DELETE / _upsert_filing_row)..."

# Docstring-stripped line-number walker (mirrors D's docstring
# handling). The _apply_blockholders docstring NAMES the gate function
# in backticks as part of the branch-order explanation; a naive
# walker would treat the docstring mention as the gate's actual call-
# site and pass even after the real gate is deleted. Mutation test
# 2026-05-21 confirmed the bug; this walker fixes it by stripping
# triple-quoted runs before searching for the literal.
#
# Returns the FIRST line number inside the function body (excluding
# docstring lines) where ``$2`` appears, or empty when not found.
first_code_line_in_function() {
  local file="$1" fname="$2" literal="$3"
  awk -v fname="$fname" -v literal="$literal" '
    BEGIN {
      in_fn = 0
      in_doc = 0
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
      if (NR > fn_start_nr && (substr($0, 1, 4) == "def " || substr($0, 1, 6) == "class ")) {
        in_fn = 0
        next
      }
      # Toggle docstring state on triple-quote runs.
      line_copy = $0
      n3 = gsub(/"""/, "&", line_copy)
      line_copy = $0
      n3s = gsub(/'\''\''\''/, "&", line_copy)
      total_toggles = n3 + n3s
      pre_doc = in_doc
      if ((total_toggles % 2) == 1) {
        in_doc = (in_doc == 0) ? 1 : 0
      }
      # Only check non-docstring CODE lines (no triple-quote run AND
      # neither pre/post state inside a docstring).
      if (pre_doc == 0 && in_doc == 0 && total_toggles == 0) {
        if (index($0, literal) > 0) { print NR; exit }
      }
    }
  ' "$file"
}

if [[ ! -f "$FILE_REWASH" ]]; then
  fail "missing file: $FILE_REWASH"
else
  cap_line=$(first_code_line_in_function "$FILE_REWASH" "_apply_blockholders" "blockholders_within_retention(" || true)
  delete_line=$(first_code_line_in_function "$FILE_REWASH" "_apply_blockholders" "DELETE FROM blockholder_filings" || true)
  upsert_line=$(first_code_line_in_function "$FILE_REWASH" "_apply_blockholders" "_upsert_filing_row(" || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_REWASH: _apply_blockholders missing 'blockholders_within_retention(' rescue-path gate (CODE line; docstring mentions are stripped before the walker matches). Codex 1b MEDIUM branch-order pin requires the gate to fire on the rescue path BEFORE the destructive replace-then-insert."
  else
    if [[ -n "$delete_line" && "$delete_line" != "0" ]]; then
      if (( cap_line >= delete_line )); then
        fail "$FILE_REWASH:$cap_line: rescue gate appears AT/AFTER 'DELETE FROM blockholder_filings' at line $delete_line — Codex 1b MEDIUM branch-order pin: gate MUST precede the destructive replace."
      fi
    fi
    if [[ -n "$upsert_line" && "$upsert_line" != "0" ]]; then
      if (( cap_line >= upsert_line )); then
        fail "$FILE_REWASH:$cap_line: rescue gate appears AT/AFTER '_upsert_filing_row(' at line $upsert_line — gate MUST precede the re-insert."
      fi
    fi
  fi
fi

# ======================================================================
# Verdict
# ======================================================================
if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} 13D/G placement invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-21-pr11-blockholders-activation-design.md §3.6 +" >&2
  echo "docs/superpowers/plans/2026-05-21-1233-pr11-blockholders-activation.md §Phase 10 for the rules." >&2
  exit 1
fi

echo "check_13dg_retention: OK"
