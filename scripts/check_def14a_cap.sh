#!/usr/bin/env bash
#
# Lint guard: every DEF 14A writer chokepoint MUST honour the
# latest-2-primary-proxies-per-filer cap (#1233 §4.7 PR5).
#
# Four block-level invariants (NOT file-level):
#
# A. Repo-wide DEF14A SQL block parity. Every SQL block under app/
#    that contains a DEF14A signal (literal form-type strings OR
#    parameterised `ANY(%(forms)s)` near `_DEF14A_FORM_TYPES`) MUST
#    contain ONE of three block-level compliance markers:
#      1. `rank_within_form <= %(cap)s` predicate (inline CTE
#         pattern).
#      2. `def14a_within_cap(` Python call within ±5 lines of the
#         SQL block boundary (per-row helper pattern).
#      3. `-- DEF14A-CAP-EXEMPT: <reason>` SQL comment inside the
#         block (explicit operator override).
#
# B. `app/services/def14a_ingest.py::discover_pending_def14a` per-
#    block parity. Same rule as A but scoped to that one function so
#    a single-block regression is detected without parsing the whole
#    repo.
#
# C. `app/services/manifest_parsers/def14a.py::_parse_def14a` pre-
#    fetch placement. The `def14a_within_cap(` line MUST be > the
#    missing-instrument_id check line AND < the first
#    `provider.fetch_document_text(` call line.
#
# D. `app/services/rewash_filings.py::_apply_def14a` rescue-branch
#    placement. Exactly one `def14a_within_cap(` call; its line MUST
#    be > the rescue-fallback `SELECT log.issuer_cik` anchor AND <
#    the `parse_beneficial_ownership_table(` call.
#
# Exits non-zero on the first invariant violation. Wired into
# `.githooks/pre-push`.
#
# Block-parsing uses awk (BSD vs GNU `grep -P` portability — PR4
# Codex 1c lesson).

set -euo pipefail

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

# ----------------------------------------------------------------------
# Helper: extract every Python triple-quoted block (start_line:end_line)
# for a single file. Detects both ``"""`` and ``'''`` quote styles
# (single-line same-quote opens-and-closes excluded). Mixed-quote SQL
# is vanishingly rare in this repo and a future hit would surface as a
# parity failure rather than silent miss.
# ----------------------------------------------------------------------
extract_blocks() {
  local file="$1"
  awk '
    {
      n_dq = gsub(/"""/, "&", $0)
      n_sq = gsub(/'\'''\'''\''/, "&", $0)
      if (n_dq == 1) {
        if (in_dq) { print start_dq ":" NR; in_dq = 0 }
        else       { start_dq = NR;       in_dq = 1 }
      }
      if (n_sq == 1) {
        if (in_sq) { print start_sq ":" NR; in_sq = 0 }
        else       { start_sq = NR;       in_sq = 1 }
      }
    }
  ' "$file"
}

# ----------------------------------------------------------------------
# Helper: scan an arbitrary range of lines and return 1 if any marker
# (literal form-type | ANY(%(forms)s) + _DEF14A_FORM_TYPES nearby) is
# present. Used by invariants A + B.
# ----------------------------------------------------------------------
block_has_def14a_signal() {
  local file="$1" start="$2" end="$3"
  local block
  block=$(awk -v s="$start" -v e="$end" 'NR>=s && NR<=e' "$file")

  # Signal 1: literal form-type string inside the block.
  if echo "$block" | grep -qE "'DEF 14A'|'DEFA14A'|'DEFM14A'|'DEFR14A'"; then
    echo 1
    return
  fi

  # Signal 2: `filing_type = ANY(%(forms)s)` inside the block AND
  # `_DEF14A_FORM_TYPES` referenced within ±20 lines of the block.
  if echo "$block" | grep -qE 'filing_type = ANY\(%\(forms\)s\)'; then
    local ctx_start ctx_end
    ctx_start=$((start - 20))
    if (( ctx_start < 1 )); then ctx_start=1; fi
    ctx_end=$((end + 20))
    if awk -v s="$ctx_start" -v e="$ctx_end" 'NR>=s && NR<=e' "$file" \
       | grep -q '_DEF14A_FORM_TYPES'; then
      echo 1
      return
    fi
  fi

  echo 0
}

# ----------------------------------------------------------------------
# Helper: check whether a block has at least one of the three
# compliance markers (rank predicate, nearby def14a_within_cap call,
# inline exemption marker).
# ----------------------------------------------------------------------
block_is_compliant() {
  local file="$1" start="$2" end="$3"
  local block
  block=$(awk -v s="$start" -v e="$end" 'NR>=s && NR<=e' "$file")

  # Marker 1: rank predicate inside the block.
  if echo "$block" | grep -qE 'rank_within_form <= %\(cap\)s'; then
    echo 1
    return
  fi

  # Marker 3: inline exemption comment inside the block.
  if echo "$block" | grep -qE -- '-- DEF14A-CAP-EXEMPT:'; then
    echo 1
    return
  fi

  # Marker 2: `def14a_within_cap(` Python call within ±5 lines of the
  # block boundary.
  local ctx_start ctx_end
  ctx_start=$((start - 5))
  if (( ctx_start < 1 )); then ctx_start=1; fi
  ctx_end=$((end + 5))
  if awk -v s="$ctx_start" -v e="$ctx_end" 'NR>=s && NR<=e' "$file" \
     | grep -q 'def14a_within_cap('; then
    echo 1
    return
  fi

  echo 0
}

# ----------------------------------------------------------------------
# Helper: line number of first match (literal substring) inside a
# function body. BSD awk doesn't accept ``(`` in regex patterns — use
# index() for literal substring matching.
#
# Body bounded by ``def <name>(`` and the next top-level ``def``
# (column 0) or end-of-file. We don't track indent levels — every
# top-level ``def`` ends the previous function.
# ----------------------------------------------------------------------
first_line_in_function() {
  local file="$1" fname="$2" literal="$3"
  awk -v fname="$fname" -v literal="$literal" '
    BEGIN {
      in_fn = 0
      anchor = "def " fname "("
    }
    {
      # Function start anchor — match leading ``def <fname>(``
      # against the line. Use index() to avoid regex escaping of
      # the open-paren in awk on BSD.
      if (!in_fn) {
        # Strip leading whitespace, then check prefix.
        stripped = $0
        sub(/^[[:space:]]+/, "", stripped)
        if (index(stripped, anchor) == 1) {
          in_fn = 1
          fn_start_nr = NR
        }
        next
      }
      # In-function. Top-level ``def `` (column 0) ends the body.
      if (NR > fn_start_nr && substr($0, 1, 4) == "def ") {
        in_fn = 0
        next
      }
      if (index($0, literal) > 0) { print NR; exit }
    }
  ' "$file"
}

# ======================================================================
# Invariant A — repo-wide DEF14A SQL block parity (block-level)
# ======================================================================

echo "Checking invariant A (repo-wide DEF14A block parity)..."

# Scan every Python file under app/ (tests excluded). Helper definition
# in def14a_ingest.py is allowed an exemption marker on its own SQL
# (the DISTINCT ON CTE inside ``def14a_within_cap`` IS the cap source
# of truth).
files_to_check=$(find app -type f -name '*.py' 2>/dev/null | sort)

for file in $files_to_check; do
  # Codex 2 MED — a here-string (``<<< "$blocks"``) needs a
  # writable temp dir; on a read-only FS the loop is silently
  # skipped while the script still exits 0. Process substitution
  # (``< <(...)``) uses a FIFO, no temp file, so the loop runs
  # in every environment. ``shopt -s lastpipe`` is unavailable
  # in macOS bash 3.2, so we keep ``violations`` updates inside
  # the subshell-less ``done < <(...)`` form (no pipeline) —
  # the while body runs in the parent shell, fail() updates
  # carry out correctly.
  while IFS= read -r block_range; do
    if [[ -z "$block_range" ]]; then continue; fi
    start=$(echo "$block_range" | cut -d: -f1)
    end=$(echo "$block_range" | cut -d: -f2)

    has_signal=$(block_has_def14a_signal "$file" "$start" "$end")
    if [[ "$has_signal" != "1" ]]; then continue; fi

    is_compliant=$(block_is_compliant "$file" "$start" "$end")
    if [[ "$is_compliant" != "1" ]]; then
      fail "$file:$start: DEF14A chokepoint block (ends line $end) lacks compliance marker. Add rank_within_form <= %(cap)s predicate, OR def14a_within_cap(...) call within ±5 lines, OR -- DEF14A-CAP-EXEMPT: <reason> inline."
    fi
  done < <(extract_blocks "$file")
done

# ======================================================================
# Invariant B — discover_pending_def14a per-block parity
# ======================================================================
# B is a strict subset of A (same per-block check, but scoped only to
# def14a_ingest.py). Invariant A's full-file scan already covers
# discover_pending_def14a's blocks, so B is exercised implicitly. We
# keep B as a documented narrower regression check by re-scanning the
# function explicitly — useful when developing in isolation against a
# single file. Logic is the same as A; the re-run is a no-op when A
# passes.

echo "Checking invariant B (discover_pending_def14a per-block parity)..."

FILE_DEF14A_INGEST="app/services/def14a_ingest.py"
if [[ ! -f "$FILE_DEF14A_INGEST" ]]; then
  fail "missing file: $FILE_DEF14A_INGEST"
else
  # Extract function body line range. BSD awk doesn't allow ``(``
  # in regex — use literal-substring anchor on stripped line.
  fn_range=$(awk '
    BEGIN { in_fn = 0; start = 0; anchor = "def discover_pending_def14a(" }
    {
      if (!in_fn) {
        stripped = $0
        sub(/^[[:space:]]+/, "", stripped)
        if (index(stripped, anchor) == 1) {
          in_fn = 1; start = NR; next
        }
      } else {
        if (NR > start && substr($0, 1, 4) == "def ") {
          print start ":" (NR - 1); exit
        }
      }
    }
    END { if (in_fn && start > 0) print start ":" NR }
  ' "$FILE_DEF14A_INGEST" | head -1)

  if [[ -z "$fn_range" ]]; then
    fail "$FILE_DEF14A_INGEST: discover_pending_def14a function not found"
  else
    fn_start=$(echo "$fn_range" | cut -d: -f1)
    fn_end=$(echo "$fn_range" | cut -d: -f2)

    while IFS= read -r block_range; do
      if [[ -z "$block_range" ]]; then continue; fi
      start=$(echo "$block_range" | cut -d: -f1)
      end=$(echo "$block_range" | cut -d: -f2)
      # Block must be inside the function body.
      if (( start < fn_start || end > fn_end )); then continue; fi

      has_signal=$(block_has_def14a_signal "$FILE_DEF14A_INGEST" "$start" "$end")
      if [[ "$has_signal" != "1" ]]; then continue; fi

      is_compliant=$(block_is_compliant "$FILE_DEF14A_INGEST" "$start" "$end")
      if [[ "$is_compliant" != "1" ]]; then
        fail "$FILE_DEF14A_INGEST:$start: discover_pending_def14a chokepoint block lacks compliance marker."
      fi
    done < <(extract_blocks "$FILE_DEF14A_INGEST")
  fi
fi

# ======================================================================
# Invariant C — _parse_def14a pre-fetch placement
# ======================================================================

echo "Checking invariant C (_parse_def14a pre-fetch placement)..."

FILE_MANIFEST_DEF14A="app/services/manifest_parsers/def14a.py"
if [[ ! -f "$FILE_MANIFEST_DEF14A" ]]; then
  fail "missing file: $FILE_MANIFEST_DEF14A"
else
  cap_line=$(first_line_in_function "$FILE_MANIFEST_DEF14A" "_parse_def14a" 'def14a_within_cap(' || true)
  iid_line=$(first_line_in_function "$FILE_MANIFEST_DEF14A" "_parse_def14a" 'if instrument_id is None' || true)
  fetch_line=$(first_line_in_function "$FILE_MANIFEST_DEF14A" "_parse_def14a" 'fetch_document_text(' || true)

  if [[ -z "$cap_line" || "$cap_line" == "0" ]]; then
    fail "$FILE_MANIFEST_DEF14A: _parse_def14a missing def14a_within_cap(...) pre-fetch gate (#1233 PR5 §4.2)."
  elif [[ -z "$iid_line" || "$iid_line" == "0" ]]; then
    fail "$FILE_MANIFEST_DEF14A: _parse_def14a missing 'if instrument_id is None' anchor — cannot validate cap placement."
  elif [[ -z "$fetch_line" || "$fetch_line" == "0" ]]; then
    fail "$FILE_MANIFEST_DEF14A: _parse_def14a missing fetch_document_text() call — cannot validate cap placement."
  elif (( cap_line < iid_line )); then
    fail "$FILE_MANIFEST_DEF14A:$cap_line: def14a_within_cap(...) appears BEFORE the missing-instrument_id check at line $iid_line — helper needs a non-None iid."
  elif (( cap_line > fetch_line )); then
    fail "$FILE_MANIFEST_DEF14A:$cap_line: def14a_within_cap(...) appears AFTER fetch_document_text() at line $fetch_line — pre-fetch gate ordering violated."
  fi
fi

# ======================================================================
# Invariant D — _apply_def14a rescue-branch placement
# ======================================================================

echo "Checking invariant D (_apply_def14a rescue-branch placement)..."

FILE_REWASH="app/services/rewash_filings.py"
if [[ ! -f "$FILE_REWASH" ]]; then
  fail "missing file: $FILE_REWASH"
else
  # Count cap calls in _apply_def14a. BSD awk pattern uses index()
  # via literal substring (no parens in regex).
  cap_count=$(awk '
    BEGIN { in_fn = 0; start = 0; count = 0; anchor = "def _apply_def14a(" }
    {
      if (!in_fn) {
        stripped = $0
        sub(/^[[:space:]]+/, "", stripped)
        if (index(stripped, anchor) == 1) { in_fn = 1; start = NR; next }
      } else {
        if (NR > start && substr($0, 1, 4) == "def ") { in_fn = 0; next }
        if (index($0, "def14a_within_cap(") > 0) { count++ }
      }
    }
    END { print count + 0 }
  ' "$FILE_REWASH")

  if [[ "$cap_count" != "1" ]]; then
    fail "$FILE_REWASH: _apply_def14a should contain exactly 1 def14a_within_cap(...) call (rescue-branch gate, #1233 PR5 §4.3); found $cap_count."
  else
    cap_line=$(first_line_in_function "$FILE_REWASH" "_apply_def14a" 'def14a_within_cap(' || true)
    anchor_line=$(first_line_in_function "$FILE_REWASH" "_apply_def14a" 'SELECT log.issuer_cik' || true)
    parse_line=$(first_line_in_function "$FILE_REWASH" "_apply_def14a" 'parse_beneficial_ownership_table(' || true)

    if [[ -z "$anchor_line" || "$anchor_line" == "0" ]]; then
      fail "$FILE_REWASH: _apply_def14a missing rescue-fallback 'SELECT log.issuer_cik' anchor — cannot validate cap placement."
    elif [[ -z "$parse_line" || "$parse_line" == "0" ]]; then
      fail "$FILE_REWASH: _apply_def14a missing parse_beneficial_ownership_table() call — cannot validate cap placement."
    elif (( cap_line <= anchor_line )); then
      fail "$FILE_REWASH:$cap_line: def14a_within_cap(...) appears BEFORE the rescue-fallback SELECT at line $anchor_line — happy-path branch must NOT be capped."
    elif (( cap_line >= parse_line )); then
      fail "$FILE_REWASH:$cap_line: def14a_within_cap(...) appears AFTER parse_beneficial_ownership_table() at line $parse_line — rescue gate must short-circuit before parse."
    fi
  fi
fi

# ----------------------------------------------------------------------
# Verdict
# ----------------------------------------------------------------------

if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} DEF 14A latest-N cap invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.7 +" >&2
  echo "docs/superpowers/plans/2026-05-20-pr5-def14a-latest-2-cap.md §5 for the rules." >&2
  exit 1
fi

echo "OK: DEF 14A latest-N primary cap honoured at every chokepoint."
