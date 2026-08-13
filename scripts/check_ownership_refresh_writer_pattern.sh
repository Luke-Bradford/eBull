#!/usr/bin/env bash
#
# Lint guard: every ownership_*_current MERGE writer chokepoint MUST
# honour the structural contract established by PR12 (#1233 §5).
#
# Seven helpers (insiders / institutions / blockholders / treasury /
# def14a / funds / esop) are each pinned by 12 per-helper invariants
# (A-L) plus 12 cross-cutting invariants (M / N / O1-O3 / P1-P7).
# Total clause-count: 93.
#
# Per-helper invariants A-L:
#
#  A. Helper defined exactly once in ownership_observations.py.
#  B. No legacy DELETE FROM ownership_X_current inside helper body.
#  C. MERGE INTO ownership_X_current AS tgt opener present (×1).
#  D1. Const clamp tgt.instrument_id = %(iid)s in ON clause — present
#      for 6 helpers; absent for treasury (PG MERGE FULL JOIN requires
#      equi-joinable tgt↔src column; treasury single-col PK uses
#      tgt.instrument_id = src.instrument_id instead). SQL comment
#      lines (--) are stripped before this check.
#  D2. Const clamp tgt.instrument_id = %(iid)s in NOT MATCHED BY
#      SOURCE clause — present for ALL 7 helpers.
#  E. refreshed_at NOT inside IS DISTINCT FROM diff tuple.
#  F. pg_advisory_xact_lock / hashtextextended present (×1).
#  G. DISTINCT ON columns match per-helper expected tuple.
#  H. ORDER BY tuple matches per-helper expected list.
#  I. 5-axis full-column-set invariant (#1256): (a) UPDATE SET cols ==
#     diff LHS ∪ {refreshed_at}; (b) diff LHS == diff RHS ordered;
#     (c) refreshed_at exactly-once in UPDATE SET, never in diff;
#     (d) no duplicate cols in any span; (e) UPDATE assignment
#     LHS == RHS. Delegates to scripts/_check_ownership_writer_columns.py.
#  J. INSERT column list omits refreshed_at (DEFAULT now() fires):
#     refreshed_at appears exactly once in body (UPDATE SET only).
#  K. Per-helper extra WHERE filter clauses (treasury K1, def14a K1-K3,
#     others zero).
#  L. Helper UPSERTs into ownership_refresh_state with correct category.
#
# Cross-cutting invariants M / N / O1-O3 / P1-P7:
#
#  M. No DELETE FROM ownership_*_current in app/ outside MERGE clause.
#  N. No DELETE FROM ownership_*_current in scripts/ or app/jobs/.
#  O1. No c.refreshed_at < in repair file (legacy predicate form).
#  O2. WITH obs_max AS present in repair file (CTE aggregate pin).
#  O3. No LATERAL in repair file (no per-row regression).
#  P1-P7. sql/163_ownership_refresh_state.sql structural pins.
#
# Exits non-zero on the first invariant violation.
# Wired into .githooks/pre-push after check_13dg_retention.sh.
#
# Awk-based block parsing (BSD vs GNU grep -P portability). Empty-grep
# wc -l guarded throughout (PR10a Codex iter 1 lesson).
# Written for POSIX sh / bash 3.2 (macOS system bash) — no bash 4+
# associative arrays.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FILE_OBS="app/services/ownership_observations.py"
FILE_REPAIR="app/jobs/ownership_observations_repair.py"
FILE_SQL163="sql/163_ownership_refresh_state.sql"

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

# Extract the body of refresh_<helper>_current from FILE_OBS.
# Body spans from the `def refresh_<helper>_current(` line (stripped of
# leading whitespace) to the next column-0 `def ` or `class ` or EOF.
extract_helper_body() {
  local helper="$1"
  awk -v helper="$helper" '
    BEGIN {
      in_fn = 0
      anchor = "def refresh_" helper "_current("
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
      print $0
    }
  ' "$FILE_OBS"
}

# Extract the body of an EXACT function name (e.g. refresh_def14a_current_batch)
# from FILE_OBS. Spans from `def <fn>(` to the next column-0 def/class/EOF.
# Used for the batch helpers (invariant Q) — the short-name extractor above
# appends `_current(` and so cannot match `_current_batch(`.
extract_named_fn_body() {
  local fn="$1"
  awk -v fn="$fn" '
    BEGIN {
      in_fn = 0
      anchor = "def " fn "("
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
      print $0
    }
  ' "$FILE_OBS"
}

# Extract SQL-only span of helper body: skip the function docstring (text
# between the first `"""` and the matching closing `"""`). Symbols inside
# the docstring (e.g. `refreshed_at` in prose) must NOT count toward
# code-shape invariants like J. Triple-quoted SQL strings passed to
# cur.execute() are emitted by this filter, as is non-docstring code.
extract_helper_body_no_docstring() {
  local helper="$1"
  extract_helper_body "$helper" | awk '
    BEGIN { in_doc = 0; doc_done = 0 }
    {
      stripped = $0
      sub(/^[[:space:]]+/, "", stripped)
      if (!doc_done) {
        # First """ on a line opens the docstring; second """ closes it.
        # Handle the rare case of an opener that also closes on the same
        # line (single-line docstring): two """ on the same line.
        n_quotes = gsub(/"""/, "&", stripped)
        if (!in_doc && n_quotes >= 1) {
          if (n_quotes >= 2) { doc_done = 1 } else { in_doc = 1 }
          next
        }
        if (in_doc) {
          if (n_quotes >= 1) { in_doc = 0; doc_done = 1 }
          next
        }
      }
      print $0
    }
  '
}

# Extract the ON clause span (from MERGE INTO line to first WHEN line),
# stripping SQL comment lines so D1 does not count comment mentions.
extract_on_clause_no_comments() {
  local helper="$1"
  local body
  body=$(extract_helper_body "$helper")
  if [[ -z "$body" ]]; then
    return
  fi
  printf '%s\n' "$body" | awk -v helper="$helper" '
    BEGIN { in_on = 0; started = 0 }
    {
      stripped = $0
      sub(/^[[:space:]]+/, "", stripped)
      # Start capturing at MERGE INTO ownership_<helper>_current AS tgt.
      if (!in_on) {
        needle = "MERGE INTO ownership_" helper "_current AS tgt"
        if (index(stripped, needle) > 0) {
          in_on = 1
          started = 1
        }
      }
      if (!in_on) next
      # Stop (exclusive) at the first MERGE clause (WHEN MATCHED ... / WHEN
      # NOT MATCHED ...). Plain "WHEN" matches `WHEN '\''form4'\''` etc. inside
      # the ORDER BY CASE expression in the USING subquery, which would
      # truncate the ON-clause span before we ever reach it.
      if (started && (index(stripped, "WHEN MATCHED AND") == 1 \
                   || index(stripped, "WHEN NOT MATCHED") == 1)) { exit }
      # Strip SQL comment lines (lines whose first non-space chars are --).
      if (substr(stripped, 1, 2) == "--") next
      print $0
    }
  '
}

# Extract the NOT MATCHED BY SOURCE ... THEN DELETE clause span.
extract_not_matched_by_source() {
  local helper="$1"
  local body
  body=$(extract_helper_body "$helper")
  if [[ -z "$body" ]]; then
    return
  fi
  printf '%s\n' "$body" | awk '
    BEGIN { in_clause = 0 }
    {
      stripped = $0
      sub(/^[[:space:]]+/, "", stripped)
      if (!in_clause && index(stripped, "WHEN NOT MATCHED BY SOURCE") == 1) {
        in_clause = 1
      }
      if (!in_clause) next
      print $0
      if (index(stripped, "THEN DELETE") > 0) exit
    }
  '
}

# Extract the WHEN MATCHED AND (...) IS DISTINCT FROM (...) THEN UPDATE
# span for invariant E.
extract_diff_predicate_span() {
  local helper="$1"
  local body
  body=$(extract_helper_body "$helper")
  if [[ -z "$body" ]]; then
    return
  fi
  printf '%s\n' "$body" | awk '
    BEGIN { in_pred = 0 }
    {
      stripped = $0
      sub(/^[[:space:]]+/, "", stripped)
      if (!in_pred && index(stripped, "WHEN MATCHED AND (") == 1) {
        in_pred = 1
      }
      if (!in_pred) next
      print $0
      if (index(stripped, ") THEN UPDATE SET") > 0) exit
    }
  '
}

# Return per-helper config values via stdout for a given key.
# Usage: get_helper_config <helper> <key>
# Keys: distinct_on | order_by | k_clauses | category
get_helper_config() {
  local helper="$1" key="$2"
  case "${helper}:${key}" in
    # insiders
    insiders:distinct_on)
      printf '%s' "holder_identity_key, ownership_nature" ;;
    insiders:order_by)
      printf '%s' "holder_identity_key, ownership_nature, CASE source WHEN 'form4' THEN 1 WHEN 'form3' THEN 2 WHEN '13d' THEN 3 WHEN '13g' THEN 3 WHEN 'def14a' THEN 4 WHEN '13f' THEN 5 WHEN 'nport' THEN 6 WHEN 'ncsr' THEN 6 WHEN 'xbrl_dei' THEN 7 WHEN '10k_note' THEN 8 WHEN 'finra_si' THEN 9 ELSE 10 END ASC, period_end DESC, filed_at DESC, source ASC, source_document_id ASC" ;;
    insiders:k_clauses)
      printf '%s' "" ;;
    insiders:category)
      printf '%s' "insiders" ;;
    # institutions
    institutions:distinct_on)
      printf '%s' "filer_cik, ownership_nature, exposure_kind" ;;
    institutions:order_by)
      printf '%s' "filer_cik, ownership_nature, exposure_kind, period_end DESC, filed_at DESC, source_document_id ASC" ;;
    institutions:k_clauses)
      printf '%s' "" ;;
    institutions:category)
      printf '%s' "institutions" ;;
    # blockholders
    blockholders:distinct_on)
      printf '%s' "reporter_cik, ownership_nature" ;;
    blockholders:order_by)
      printf '%s' "reporter_cik, ownership_nature, filed_at DESC, period_end DESC, source_document_id ASC" ;;
    blockholders:k_clauses)
      printf '%s' "" ;;
    blockholders:category)
      printf '%s' "blockholders" ;;
    # treasury
    treasury:distinct_on)
      printf '%s' "instrument_id" ;;
    treasury:order_by)
      printf '%s' "instrument_id, period_end DESC, filed_at DESC, source_document_id ASC" ;;
    treasury:k_clauses)
      printf '%s' "AND treasury_shares IS NOT NULL" ;;
    treasury:category)
      printf '%s' "treasury" ;;
    # def14a
    def14a:distinct_on)
      printf '%s' "holder_name_key, ownership_nature" ;;
    def14a:order_by)
      printf '%s' "holder_name_key, ownership_nature, period_end DESC, filed_at DESC, source_document_id ASC" ;;
    def14a:k_clauses)
      # Semicolon-separated
      printf '%s' "AND shares IS NOT NULL;AND holder_role IS DISTINCT FROM 'esop';AND holder_name !~* %(esop_regex)s" ;;
    def14a:category)
      printf '%s' "def14a" ;;
    # funds
    funds:distinct_on)
      printf '%s' "fund_series_id" ;;
    funds:order_by)
      printf '%s' "fund_series_id, filed_at DESC, period_end DESC, source_document_id ASC" ;;
    funds:k_clauses)
      printf '%s' "" ;;
    funds:category)
      printf '%s' "funds" ;;
    # esop
    esop:distinct_on)
      printf '%s' "plan_name" ;;
    esop:order_by)
      printf '%s' "plan_name, filed_at DESC, period_end DESC, source_document_id ASC" ;;
    esop:k_clauses)
      printf '%s' "" ;;
    esop:category)
      printf '%s' "esop" ;;
    *)
      printf '%s' "" ;;
  esac
}

HELPERS="insiders institutions blockholders treasury def14a funds esop"

# ======================================================================
# Preflight: source file must exist
# ======================================================================
if [[ ! -f "$FILE_OBS" ]]; then
  fail "missing file: $FILE_OBS"
  echo "FAIL: ${violations} violation(s)." >&2
  exit 1
fi

# ======================================================================
# Per-helper loop: invariants A-L
# ======================================================================
for helper in $HELPERS; do
  echo "Checking invariants A-L for helper: ${helper}..."

  # ------------------------------------------------------------------
  # A — helper defined exactly once
  # ------------------------------------------------------------------
  a_count=$(count_literal "$FILE_OBS" "def refresh_${helper}_current(")
  if (( a_count != 1 )); then
    fail "A helper=${helper}: expected exactly 1 'def refresh_${helper}_current(' in ${FILE_OBS}, found ${a_count}."
  fi

  body=$(extract_helper_body "$helper")
  if [[ -z "$body" ]]; then
    fail "A helper=${helper}: could not extract body — function missing or renamed."
    continue
  fi

  # ------------------------------------------------------------------
  # B — no legacy DELETE FROM ownership_X_current inside body
  # ------------------------------------------------------------------
  b_raw=$(printf '%s\n' "$body" | grep -Fc "DELETE FROM ownership_${helper}_current" || true)
  if (( b_raw != 0 )); then
    fail "B helper=${helper}: found ${b_raw} 'DELETE FROM ownership_${helper}_current' inside helper body — legacy pattern must be gone."
  fi

  # ------------------------------------------------------------------
  # C — MERGE INTO opener present exactly once
  # ------------------------------------------------------------------
  c_count=$(printf '%s\n' "$body" | grep -Fc "MERGE INTO ownership_${helper}_current AS tgt" || true)
  if (( c_count != 1 )); then
    fail "C helper=${helper}: expected 1 'MERGE INTO ownership_${helper}_current AS tgt', found ${c_count}."
  fi

  # ------------------------------------------------------------------
  # D1 — const clamp in ON clause (absent for treasury)
  # ------------------------------------------------------------------
  on_span=$(extract_on_clause_no_comments "$helper")
  d1_count=0
  if [[ -n "$on_span" ]]; then
    d1_count=$(printf '%s\n' "$on_span" | grep -Fc "tgt.instrument_id = %(iid)s" || true)
  fi
  if [[ "$helper" = "treasury" ]]; then
    if (( d1_count != 0 )); then
      fail "D1 helper=treasury: expected 0 'tgt.instrument_id = %(iid)s' in ON clause (non-comment lines), found ${d1_count}. Treasury uses tgt.instrument_id = src.instrument_id."
    fi
  else
    if (( d1_count != 1 )); then
      fail "D1 helper=${helper}: expected 1 'tgt.instrument_id = %(iid)s' in ON clause (non-comment lines), found ${d1_count}."
    fi
  fi

  # ------------------------------------------------------------------
  # D2 — const clamp in NOT MATCHED BY SOURCE clause (all 7 helpers)
  # ------------------------------------------------------------------
  nmbs_span=$(extract_not_matched_by_source "$helper")
  d2_count=0
  if [[ -n "$nmbs_span" ]]; then
    d2_count=$(printf '%s\n' "$nmbs_span" | grep -Fc "tgt.instrument_id = %(iid)s" || true)
  fi
  if (( d2_count != 1 )); then
    fail "D2 helper=${helper}: expected 1 'tgt.instrument_id = %(iid)s' in NOT MATCHED BY SOURCE clause, found ${d2_count}."
  fi

  # ------------------------------------------------------------------
  # E — refreshed_at NOT inside IS DISTINCT FROM diff predicate
  # ------------------------------------------------------------------
  diff_span=$(extract_diff_predicate_span "$helper")
  e_count=0
  if [[ -n "$diff_span" ]]; then
    e_count=$(printf '%s\n' "$diff_span" | grep -Fc "refreshed_at" || true)
  fi
  if (( e_count != 0 )); then
    fail "E helper=${helper}: 'refreshed_at' appears inside IS DISTINCT FROM diff predicate (${e_count} occurrence(s)) — refreshed_at must NOT be a diff column."
  fi

  # ------------------------------------------------------------------
  # F — pg_advisory_xact_lock / hashtextextended preserved
  # ------------------------------------------------------------------
  f_count=$(printf '%s\n' "$body" | grep -Fc "hashtextextended('refresh_${helper}_current'" || true)
  if (( f_count != 1 )); then
    fail "F helper=${helper}: expected 1 \"hashtextextended('refresh_${helper}_current'\" in body, found ${f_count}."
  fi

  # ------------------------------------------------------------------
  # G — DISTINCT ON columns match per-helper expected tuple
  # ------------------------------------------------------------------
  expected_distinct=$(get_helper_config "$helper" "distinct_on")
  g_count=$(printf '%s\n' "$body" | grep -Fc "SELECT DISTINCT ON (${expected_distinct})" || true)
  if (( g_count != 1 )); then
    fail "G helper=${helper}: expected 1 'SELECT DISTINCT ON (${expected_distinct})', found ${g_count}."
  fi

  # ------------------------------------------------------------------
  # H — ORDER BY tuple matches per-helper expected list (whitespace-
  #     normalised: collapse runs of whitespace + newlines to single space)
  # ------------------------------------------------------------------
  expected_order=$(get_helper_config "$helper" "order_by")
  # Extract the ORDER BY block from the USING subquery and normalise.
  order_block=$(printf '%s\n' "$body" | awk '
    BEGIN { in_ob = 0; in_using = 0 }
    {
      stripped = $0
      sub(/^[[:space:]]+/, "", stripped)
      if (!in_using && index(stripped, "USING (") == 1) { in_using = 1 }
      if (!in_using) next
      if (!in_ob && index(stripped, "ORDER BY") == 1) { in_ob = 1 }
      if (!in_ob) next
      # Terminate at the first line that opens with ")": for helpers whose USING
      # subquery is a bare SELECT this is ") AS src"; for insiders (#1805) the
      # DISTINCT-ON is wrapped in a `winners` CTE so the ORDER BY is closed first
      # by the CTE-closing ")" (then "SELECT w.* FROM winners w {filter} ) AS src"
      # follows). No ORDER BY tuple line starts with ")", so this captures the
      # full tuple in both shapes.
      if (index(stripped, ")") == 1) exit
      print stripped
    }
  ')
  # Collapse all whitespace to single space and trim.
  actual_order=$(printf '%s\n' "$order_block" | tr '\n' ' ' | tr -s ' ' \
    | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
  # Strip leading "ORDER BY " prefix.
  actual_order="${actual_order#ORDER BY }"
  # Normalise expected.
  expected_order_norm=$(printf '%s' "$expected_order" | tr -s ' ' \
    | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

  if [[ "$actual_order" != "$expected_order_norm" ]]; then
    fail "H helper=${helper}: ORDER BY mismatch.
  expected: ${expected_order_norm}
  actual:   ${actual_order}"
  fi

  # ------------------------------------------------------------------
  # I — 5-axis full-column-set invariant (#1256).
  #
  # Delegates to Python helper. See
  # scripts/_check_ownership_writer_columns.py for spec.
  # Pins: (a) UPDATE SET cols == diff LHS ∪ {refreshed_at};
  #       (b) diff LHS == diff RHS (ordered);
  #       (c) refreshed_at exactly-once in UPDATE SET, never in diff;
  #       (d) no duplicate cols in any span;
  #       (e) UPDATE assignment LHS == RHS (modulo refreshed_at = now()).
  # ------------------------------------------------------------------
  if ! uv run python scripts/_check_ownership_writer_columns.py \
       --function "refresh_${helper}_current" "$FILE_OBS"; then
    fail "I helper=${helper}: see python output above"
  fi

  # ------------------------------------------------------------------
  # J — INSERT column list omits refreshed_at (DEFAULT now() fires):
  #     refreshed_at appears exactly once in body (UPDATE SET only).
  #
  # Strip the docstring before counting — every helper's docstring
  # mentions `refreshed_at` in prose, which would otherwise inflate the
  # count to 2 and trip a false positive.
  # ------------------------------------------------------------------
  body_no_doc=$(extract_helper_body_no_docstring "$helper")
  j_count=$(printf '%s\n' "$body_no_doc" | grep -Fc "refreshed_at" || true)
  if (( j_count != 1 )); then
    fail "J helper=${helper}: 'refreshed_at' appears ${j_count} time(s) in helper SQL body (excluding docstring) — expected exactly 1 (UPDATE SET only). INSERT must omit refreshed_at so DEFAULT now() fires."
  fi

  # ------------------------------------------------------------------
  # K — per-helper extra WHERE filter clauses
  # ------------------------------------------------------------------
  k_clauses=$(get_helper_config "$helper" "k_clauses")
  if [[ -n "$k_clauses" ]]; then
    # Split on semicolon using IFS in a subshell-safe way (bash 3.2 compat).
    k_idx=1
    # Use awk to split by semicolon.
    while IFS= read -r k_clause; do
      [[ -z "$k_clause" ]] && continue
      k_count=$(printf '%s\n' "$body" | grep -Fc "$k_clause" || true)
      if (( k_count != 1 )); then
        fail "K${k_idx} helper=${helper}: expected 1 '${k_clause}' in body, found ${k_count}."
      fi
      k_idx=$((k_idx + 1))
    done < <(printf '%s\n' "$k_clauses" | tr ';' '\n')
  fi

  # ------------------------------------------------------------------
  # L — helper UPSERTs into ownership_refresh_state with correct category
  # ------------------------------------------------------------------
  l_insert_count=$(printf '%s\n' "$body" | grep -Fc "INSERT INTO ownership_refresh_state" || true)
  if (( l_insert_count != 1 )); then
    fail "L helper=${helper}: expected 1 'INSERT INTO ownership_refresh_state' in body, found ${l_insert_count}."
  fi
  expected_cat=$(get_helper_config "$helper" "category")
  l_cat_count=$(printf '%s\n' "$body" | grep -Fc "'${expected_cat}'" || true)
  if (( l_cat_count == 0 )); then
    fail "L helper=${helper}: category literal '${expected_cat}' not found in body."
  fi

done  # end per-helper loop

# ======================================================================
# Invariant I — BATCH HELPERS (#1256).
#
# 7 batch helpers (refresh_*_current_batch) share the diff-aware MERGE
# shape but operate on instrument_id lists. Same drift risk as singles.
# Codex iter-1 BLOCKING-B2 + iter-3 BLOCKING-1 fold. #1345 PR-A added
# blockholders/treasury/def14a/esop batch variants.
# ======================================================================
BATCH_HELPERS="refresh_insiders_current_batch refresh_institutions_current_batch refresh_funds_current_batch refresh_blockholders_current_batch refresh_treasury_current_batch refresh_def14a_current_batch refresh_esop_current_batch"
echo "Checking invariant I for 7 batch helpers..."
for batch_fn in $BATCH_HELPERS; do
  if ! uv run python scripts/_check_ownership_writer_columns.py \
       --function "${batch_fn}" "$FILE_OBS"; then
    fail "I batch=${batch_fn}: see python output above"
  fi
done

# ======================================================================
# Invariant Q — BATCH-SPECIFIC structural pins (#1345 PR-A).
#
# Invariant I only checks the column-set diff shape. These pins guard
# the batch-specific failure modes the committee flagged:
#   Q1. SET LOCAL jit = off present (×1 per batch helper).
#   Q2. ordered advisory-lock pass over unnest(%(ids)s) (×1).
#   Q3. ANY(%(ids)s::bigint[]) in the NOT MATCHED BY SOURCE DELETE clamp
#       (L1496 batch form — guards catastrophic cross-batch delete).
#   Q4. DISTINCT ON leads with instrument_id (guards cross-instrument
#       row-collapse — DE BLOCKING). Exception: single-PK helpers whose
#       DISTINCT ON is exactly (instrument_id) already satisfy this.
#   Q5. ORDER BY leads with instrument_id (same guard).
#   Q6. def14a batch carries the %(esop_regex)s placeholder + the 3
#       extra WHERE clauses (#843 ESOP double-count guard).
# ======================================================================
echo "Checking invariant Q (batch-specific structural pins)..."
for batch_fn in $BATCH_HELPERS; do
  body=$(extract_named_fn_body "$batch_fn")

  q1=$(printf '%s\n' "$body" | grep -Fc "SET LOCAL jit = off" || true)
  (( q1 == 1 )) || fail "Q1 batch=${batch_fn}: expected 1 'SET LOCAL jit = off', found ${q1}."

  q2=$(printf '%s\n' "$body" | grep -Fc "unnest(%(ids)s::bigint[]) AS iid" || true)
  (( q2 == 1 )) || fail "Q2 batch=${batch_fn}: expected 1 ordered-lock 'unnest(%(ids)s::bigint[]) AS iid', found ${q2}."

  q3=$(printf '%s\n' "$body" | grep -Fc "WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = ANY(%(ids)s::bigint[]) THEN DELETE" || true)
  (( q3 == 1 )) || fail "Q3 batch=${batch_fn}: expected 1 'NOT MATCHED BY SOURCE ... ANY(%(ids)s::bigint[]) THEN DELETE' clamp, found ${q3}."

  q4=$(printf '%s\n' "$body" | grep -Ec "SELECT DISTINCT ON \(instrument_id[,)]" || true)
  (( q4 == 1 )) || fail "Q4 batch=${batch_fn}: expected 1 'SELECT DISTINCT ON (instrument_id' (instrument_id-led), found ${q4}. Guards cross-instrument row-collapse."

  # Q5 — leading ORDER BY key is instrument_id. The source ORDER BY block
  # opens with `ORDER BY` then `instrument_id,` on the next non-blank line.
  q5=$(printf '%s\n' "$body" | grep -A1 -E "^\s*ORDER BY\s*$" | grep -Ec "^\s*instrument_id," || true)
  (( q5 >= 1 )) || fail "Q5 batch=${batch_fn}: source ORDER BY must lead with 'instrument_id,' (cross-instrument collapse guard)."
done

# Q6 — def14a batch ESOP-exclusion filter (verbatim from single helper).
def14a_batch_body=$(extract_named_fn_body "refresh_def14a_current_batch")
q6_regex=$(printf '%s\n' "$def14a_batch_body" | grep -Fc "holder_name !~* %(esop_regex)s" || true)
(( q6_regex == 1 )) || fail "Q6 def14a batch: expected 1 'holder_name !~* %(esop_regex)s', found ${q6_regex} (#843 ESOP double-count guard)."
q6_role=$(printf '%s\n' "$def14a_batch_body" | grep -Fc "holder_role IS DISTINCT FROM 'esop'" || true)
(( q6_role == 1 )) || fail "Q6 def14a batch: expected 1 \"holder_role IS DISTINCT FROM 'esop'\", found ${q6_role}."
q6_shares=$(printf '%s\n' "$def14a_batch_body" | grep -Fc "shares IS NOT NULL" || true)
(( q6_shares == 1 )) || fail "Q6 def14a batch: expected 1 'shares IS NOT NULL', found ${q6_shares}."

# Final coverage audit (#1256 Codex iter-3 BLOCKING-1 fold; bot PR #1353
# review iter-1 BLOCKING fold) — defend against silent double-checking
# by asserting the Python helper inspected the expected count of
# functions AND that all of them passed invariant I.
#
# CRITICAL: capture exit code via `if !` form. Bare `$()` swallows the
# Python process's exit, so a helper failing invariant I.a-e would still
# print "10 functions covered (expected 10)" (because len(found)=10) and
# the grep would pass — defeating the audit. The exit-code check is
# load-bearing; the grep is a secondary guard.
echo "Checking invariant I coverage audit (expected 14 functions, all passing)..."
if ! coverage_output=$(uv run python scripts/_check_ownership_writer_columns.py \
    --coverage-report "$FILE_OBS"); then
  printf '%s\n' "$coverage_output" >&2
  fail "I coverage audit: Python helper exited non-zero — at least one helper failed invariant I"
fi
if ! grep -q "14 functions covered (expected 14)" <<<"$coverage_output"; then
  printf '%s\n' "$coverage_output" >&2
  fail "I coverage audit: expected '10 functions covered (expected 10)' line"
fi

# ======================================================================
# Cross-cutting invariant M — no raw DELETE FROM ownership_*_current in
# app/ (MERGE THEN DELETE uses "THEN DELETE", not "DELETE FROM")
# ======================================================================
echo "Checking invariant M (no raw DELETE FROM ownership_*_current in app/)..."

m_count=$(grep -r "DELETE FROM ownership_" app --include="*.py" 2>/dev/null \
  | grep "_current" \
  | grep -vc "WHEN NOT MATCHED BY SOURCE" || true)
if (( m_count != 0 )); then
  fail "M: found ${m_count} raw 'DELETE FROM ownership_*_current' line(s) in app/**/*.py (outside MERGE clause):"
  grep -r "DELETE FROM ownership_" app --include="*.py" 2>/dev/null \
    | grep "_current" \
    | grep -v "WHEN NOT MATCHED BY SOURCE" >&2 || true
fi

# ======================================================================
# Cross-cutting invariant N — no DELETE FROM ownership_*_current in
# scripts/ or app/jobs/
#
# Self-exclude check_ownership_refresh_writer_pattern.sh — this lint
# script contains the forbidden literal as grep patterns + documentation,
# which would otherwise self-match.
# ======================================================================
echo "Checking invariant N (no DELETE FROM ownership_*_current in scripts/ or app/jobs/)..."

n_scripts=0
if [[ -d scripts ]]; then
  n_scripts=$(grep -r "DELETE FROM ownership_" scripts \
      --include="*.sh" --include="*.py" \
      --exclude="check_ownership_refresh_writer_pattern.sh" 2>/dev/null \
    | grep "_current" | grep -vc "WHEN NOT MATCHED BY SOURCE" || true)
fi
n_jobs=0
if [[ -d app/jobs ]]; then
  n_jobs=$(grep -r "DELETE FROM ownership_" app/jobs --include="*.py" 2>/dev/null \
    | grep "_current" | grep -vc "WHEN NOT MATCHED BY SOURCE" || true)
fi
if (( n_scripts != 0 || n_jobs != 0 )); then
  fail "N: found $(( n_scripts + n_jobs )) 'DELETE FROM ownership_*_current' occurrence(s) in scripts/ or app/jobs/."
fi

# ======================================================================
# Cross-cutting invariants O1 / O2 / O3 — repair file predicates
# ======================================================================
echo "Checking invariants O1/O2/O3 (repair file predicate shape)..."

if [[ ! -f "$FILE_REPAIR" ]]; then
  fail "missing file: $FILE_REPAIR"
else
  # O1 — no legacy c.refreshed_at < pattern
  o1_count=$(count_literal "$FILE_REPAIR" "c.refreshed_at <")
  if (( o1_count != 0 )); then
    fail "O1: ${FILE_REPAIR} contains ${o1_count} 'c.refreshed_at <' occurrence(s) — legacy predicate form. Use obs-anchored CTE (WITH obs_max AS ...) instead."
  fi

  # O2 — WITH obs_max AS CTE present (positive shape pin)
  o2_count=$(count_literal "$FILE_REPAIR" "WITH obs_max AS")
  if (( o2_count < 1 )); then
    fail "O2: ${FILE_REPAIR} missing 'WITH obs_max AS' — obs-anchored CTE aggregate required for repair predicate."
  fi

  # O3 — no LATERAL (no per-row regression)
  o3_count=$(count_literal "$FILE_REPAIR" "LATERAL")
  if (( o3_count != 0 )); then
    fail "O3: ${FILE_REPAIR} contains ${o3_count} 'LATERAL' occurrence(s) — per-row LATERAL regression forbidden."
  fi
fi

# ======================================================================
# Cross-cutting invariants P1-P7 — sql/163 structural pins
# ======================================================================
echo "Checking invariants P1-P7 (sql/163_ownership_refresh_state.sql)..."

if [[ ! -f "$FILE_SQL163" ]]; then
  fail "missing file: $FILE_SQL163"
else
  # P1 — CREATE TABLE opener
  p1_count=$(count_literal "$FILE_SQL163" "CREATE TABLE IF NOT EXISTS ownership_refresh_state")
  if (( p1_count != 1 )); then
    fail "P1: ${FILE_SQL163}: expected 1 'CREATE TABLE IF NOT EXISTS ownership_refresh_state', found ${p1_count}."
  fi

  # P2 — PRIMARY KEY (instrument_id, category)
  p2_count=$(count_literal "$FILE_SQL163" "PRIMARY KEY (instrument_id, category)")
  if (( p2_count != 1 )); then
    fail "P2: ${FILE_SQL163}: expected 1 'PRIMARY KEY (instrument_id, category)', found ${p2_count}."
  fi

  # P3 — CHECK with 7 categories (pin the category domain)
  p3_count=$(count_literal "$FILE_SQL163" "'insiders', 'institutions', 'blockholders', 'treasury', 'def14a', 'funds', 'esop'")
  if (( p3_count != 1 )); then
    fail "P3: ${FILE_SQL163}: expected 1 CHECK listing all 7 categories, found ${p3_count}."
  fi

  # P4 — CREATE INDEX idx_funds_obs_instrument_ingested
  p4_count=$(count_literal "$FILE_SQL163" "CREATE INDEX IF NOT EXISTS idx_funds_obs_instrument_ingested")
  if (( p4_count != 1 )); then
    fail "P4: ${FILE_SQL163}: expected 1 'CREATE INDEX IF NOT EXISTS idx_funds_obs_instrument_ingested', found ${p4_count}."
  fi

  # P5 — CREATE INDEX idx_esop_obs_instrument_ingested
  p5_count=$(count_literal "$FILE_SQL163" "CREATE INDEX IF NOT EXISTS idx_esop_obs_instrument_ingested")
  if (( p5_count != 1 )); then
    fail "P5: ${FILE_SQL163}: expected 1 'CREATE INDEX IF NOT EXISTS idx_esop_obs_instrument_ingested', found ${p5_count}."
  fi

  # P6 — backfill uses MAX(c.refreshed_at). Single-line grep cannot span
  # the SELECT \n FROM line break in the migration, so count occurrences
  # of MAX(c.refreshed_at) directly. P7 already pins the SELECT shape +
  # category literal per block; the GROUP BY pin is handled by P7's
  # row-count check (7 backfill blocks).
  p6_count=$(count_literal "$FILE_SQL163" "MAX(c.refreshed_at)")
  if (( p6_count < 7 )); then
    fail "P6: ${FILE_SQL163}: expected at least 7 'MAX(c.refreshed_at)' occurrences (one per backfill block), found ${p6_count}."
  fi

  # P7 — backfill SELECT repeated 7 times (one per category)
  p7_count=$(grep -c "SELECT c\.instrument_id, '.*', MAX(c\.refreshed_at)" "$FILE_SQL163" || true)
  if (( p7_count != 7 )); then
    fail "P7: ${FILE_SQL163}: expected 7 backfill SELECT blocks (one per category), found ${p7_count}."
  fi
fi

# ======================================================================
# Verdict
# ======================================================================
if (( violations > 0 )); then
  echo "" >&2
  echo "FAIL: ${violations} ownership_refresh_writer_pattern invariant violation(s)." >&2
  echo "See docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §5" >&2
  echo "and docs/superpowers/plans/2026-05-21-pr12-ownership-current-writer-merge-impl.md §Task 8" >&2
  echo "for the full rule set." >&2
  exit 1
fi

echo "check_ownership_refresh_writer_pattern: OK"
