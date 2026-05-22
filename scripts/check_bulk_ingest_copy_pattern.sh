#!/usr/bin/env bash
#
# Lint guard: PR-3 (#1233 v3 §7) — bulk dataset ingesters use the
# per-archive COPY + INSERT...ON CONFLICT pattern. The pre-PR-3 path
# was per-row INSERT + ``with conn.transaction()`` SAVEPOINT (~1500
# rows/s ceiling); the new pattern stages rows into a TEMP table via
# ``cur.copy()`` then drains via a single ``INSERT...SELECT...ON
# CONFLICT`` so a 5M-row archive completes at ~15-30k rows/s.
#
# Invariants (path whitelist:
# app/services/sec_13f_dataset_ingest.py + sec_nport_dataset_ingest.py
# + sec_insider_dataset_ingest.py):
#
#  A. Each ingester uses ``cur.copy(`` to stream rows into staging.
#     A.1 ``cur.copy(`` appears at least once per file.
#  B. Each ingester creates a ``_stg_*`` TEMP TABLE with
#     ``ON COMMIT DROP`` lifecycle.
#     B.1 ``CREATE TEMP TABLE _stg_`` appears at least once per file.
#     B.2 ``ON COMMIT DROP`` appears at least once per file.
#  C. No per-row SAVEPOINT remains inside the row-streaming loop.
#     C.1 NO ``with conn.transaction()`` appears inside an
#         ``_iter_tsv`` loop or a ``for ... in transactions:`` /
#         ``for ... in holdings:`` row-streaming loop within each
#         whitelisted ingester. NPORT keeps ``with conn.transaction()``
#         around the per-archive series upsert pre-pass (which is
#         OUTSIDE the row loop) — the awk scope walker below caps the
#         scan at the first ``cur.copy(`` opener, after which the
#         per-row hot loop begins. Series upserts happen before that
#         line and are explicitly allowed.
#  D. Each ingester drains staging via INSERT...SELECT...FROM _stg_*
#     ON CONFLICT.
#     D.1 ``INSERT INTO ownership_*_observations`` appears at least
#         once per file (the drain).
#     D.2 ``ON CONFLICT`` appears at least once per file (paired with
#         the drain).
#     D.3 ``FROM _stg_`` appears at least once per file (the source
#         of the SELECT branch).
#
# Empty-grep guard: the path whitelist must resolve to non-empty —
# a moved file would otherwise silently skip the lint (the empty
# loop passes vacuously). The script exits 1 if no file matches.
#
# Wired into ``.githooks/pre-push`` after
# ``check_business_summary_latest_only.sh``.
#
# Shellcheck-clean (verified ``shellcheck scripts/check_bulk_ingest_copy_pattern.sh``).

set -euo pipefail

# Path whitelist — every file that PR-3 refactored. A new bulk dataset
# ingester (e.g. a future bulk N-CSR or 13D/G TSV path) MUST be added
# to this list AND honour the same invariants.
WHITELIST=(
  "app/services/sec_13f_dataset_ingest.py"
  "app/services/sec_nport_dataset_ingest.py"
  "app/services/sec_insider_dataset_ingest.py"
)

violations=0
fail() {
  echo "::error::$1" >&2
  violations=$((violations + 1))
}

# Count literal-substring occurrences inside a file. Returns 0 on no
# match (without ``|| true`` grep's exit 1 would terminate the
# pipeline under ``set -e``).
count_literal() {
  local file="$1" pat="$2"
  grep -Fc "$pat" "$file" || true
}

# Count exact regex matches across a file.
count_regex() {
  local file="$1" pattern="$2"
  grep -Ec "$pattern" "$file" || true
}

# Empty-whitelist guard: a refactor that renames every bulk ingester
# would otherwise leave this script with a no-op for-loop body and
# silently pass. Fail-closed.
matched_files=0
for f in "${WHITELIST[@]}"; do
  if [[ -f "$f" ]]; then
    matched_files=$((matched_files + 1))
  fi
done
if (( matched_files == 0 )); then
  fail "no file in the whitelist exists — every bulk dataset ingester was renamed or removed. Update the WHITELIST in this script."
  echo "FAIL: ${violations} invariant violation(s)." >&2
  exit 1
fi

for FILE in "${WHITELIST[@]}"; do
  if [[ ! -f "$FILE" ]]; then
    fail "missing file: $FILE"
    continue
  fi

  echo "Checking ${FILE}..."

  # ====================================================================
  # A — cur.copy() present (staging stream)
  # ====================================================================
  copy_calls=$(count_literal "$FILE" "cur.copy(")
  if (( copy_calls < 1 )); then
    fail "$FILE: expected at least 1 'cur.copy(' streaming write, found ${copy_calls}. The per-archive COPY refactor requires cursor-level COPY to stream validated rows into the staging table."
  fi

  # ====================================================================
  # B — CREATE TEMP TABLE _stg_* ON COMMIT DROP
  # ====================================================================
  temp_create=$(count_literal "$FILE" "CREATE TEMP TABLE _stg_")
  if (( temp_create < 1 )); then
    fail "$FILE: expected at least 1 'CREATE TEMP TABLE _stg_' declaration, found ${temp_create}. The COPY pattern requires a per-archive TEMP staging table with ON COMMIT DROP lifecycle."
  fi
  on_commit_drop=$(count_literal "$FILE" "ON COMMIT DROP")
  if (( on_commit_drop < 1 )); then
    fail "$FILE: expected at least 1 'ON COMMIT DROP' clause, found ${on_commit_drop}. The TEMP table MUST be tied to the per-archive transaction commit boundary."
  fi

  # ====================================================================
  # C — no per-row SAVEPOINT inside the COPY hot-loop block
  # ====================================================================
  # The awk walker tracks whether we're INSIDE the `with ... cur.copy(...)
  # as copy:` block (textual indentation scope). The pre-pass series
  # upsert (NPORT) keeps a legitimate ``with conn.transaction()``
  # BEFORE the COPY context opens. The post-stream unresolved-CUSIP
  # flush ALSO uses ``with conn.transaction()`` legitimately — that
  # block sits AFTER the COPY context closes but in a non-COPY
  # cursor. The hot-loop check fires only when a ``with
  # conn.transaction()`` appears at a deeper indentation than the
  # `with ... cur.copy(...) as copy:` opener (i.e. inside its body).
  #
  # Indentation algorithm:
  #   1. Find the line with `cur.copy(` AND the `as copy:` suffix —
  #      that's the COPY scope's `with` opener. Record its indent.
  #   2. Scan subsequent lines. While a line's indent is STRICTLY
  #      GREATER than the opener's indent, we are inside the COPY
  #      block body. The first line whose indent is <= the opener's
  #      indent and is non-blank ends the COPY block.
  #   3. Flag any `with conn.transaction()` that appears inside the
  #      block body.
  hot_loop_savepoints=$(awk '
    function indent_of(line,    i, c) {
      i = 0
      for (c = 1; c <= length(line); c++) {
        if (substr(line, c, 1) == " ") i++
        else if (substr(line, c, 1) == "\t") i += 8
        else break
      }
      return i
    }
    BEGIN { in_block = 0; opener_indent = -1; n = 0 }
    # Detect the COPY context opener — a line containing both
    # ``cur.copy(`` and the ``as copy:`` binding. The block body is
    # textually indented strictly more than the opener.
    /cur\.copy\(.*as copy:/ {
      in_block = 1
      opener_indent = indent_of($0)
      next
    }
    in_block {
      # Blank lines do not break the block scope.
      if ($0 ~ /^[[:space:]]*$/) next
      cur_indent = indent_of($0)
      if (cur_indent <= opener_indent) {
        # Left the COPY block body.
        in_block = 0
        next
      }
      if ($0 ~ /with[[:space:]]+conn\.transaction\(/) n++
    }
    END { print n + 0 }
  ' "$FILE")
  if (( hot_loop_savepoints > 0 )); then
    fail "$FILE: 'with conn.transaction()' appears ${hot_loop_savepoints} time(s) inside the 'with cur.copy(...) as copy:' block body — this re-introduces the per-row SAVEPOINT pattern PR-3 eliminated. Move the savepoint OUTSIDE the COPY block (e.g. pre-pass or post-stream)."
  fi

  # ====================================================================
  # D — drain via INSERT...SELECT...FROM _stg_* ON CONFLICT
  # ====================================================================
  insert_into_obs=$(count_regex "$FILE" "INSERT INTO ownership_[a-z_]+_observations")
  if (( insert_into_obs < 1 )); then
    fail "$FILE: expected at least 1 'INSERT INTO ownership_*_observations' drain, found ${insert_into_obs}."
  fi
  on_conflict=$(count_literal "$FILE" "ON CONFLICT")
  if (( on_conflict < 1 )); then
    fail "$FILE: expected at least 1 'ON CONFLICT' clause paired with the drain, found ${on_conflict}."
  fi
  from_stg=$(count_literal "$FILE" "FROM _stg_")
  if (( from_stg < 1 )); then
    fail "$FILE: expected at least 1 'FROM _stg_' SELECT source, found ${from_stg}. The drain must read from the staging TEMP table."
  fi
done

# ======================================================================
# Summary
# ======================================================================
if (( violations > 0 )); then
  echo "FAIL: ${violations} invariant violation(s) — bulk-ingest COPY pattern drift detected." >&2
  exit 1
fi

echo "OK: bulk-ingest COPY pattern invariants A / B / C / D satisfied across ${matched_files} file(s)."
