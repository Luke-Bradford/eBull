#!/usr/bin/env bash
#
# Lint guard: the latest-N-CEN-per-CIK structural cap (#1233 §4.13 /
# PR9) MUST be honoured at every layer.
#
# Unlike PR4-PR8 (which gate observations-table ingest with horizon
# helpers like ``form4_retention_cutoff`` / ``n_csr_within_retention``
# called at multiple chokepoints), N-CEN's cap is enforced
# structurally — by the schema PK + UPSERT clause + newest-first
# discovery walk. This guard pins those structural invariants so a
# future refactor cannot silently relax them.
#
# Letter labels A / B / C / D are local to this guard (no
# cross-mapping to PR6/PR7/PR8 alphabets — those guards count
# chokepoint placements; this guard counts structural anchors).
#
# Invariants:
#
#  A. Schema PK on cik.
#       - sql/100_ncen_filer_classifications.sql contains exactly 1
#         line matching ``cik <whitespace> TEXT <whitespace> PRIMARY
#         KEY``. A future migration that drops the PK, demotes it to
#         UNIQUE, or adds a composite key MUST also extend the cap
#         story (N-CEN is a classification table — adding a second
#         row per CIK turns it into an observations table and
#         §6.4 two-layer model does NOT apply).
#  B. Sole writer.
#       - Exactly 1 production *.py file under app/ contains the
#         literal ``INSERT INTO ncen_filer_classifications``. That
#         file is app/services/ncen_classifier.py.
#  C. UPSERT clause present (with no-demotion predicate + same-day
#     tie-break).
#       - app/services/ncen_classifier.py contains the literal
#         ``ON CONFLICT (cik) DO UPDATE SET`` exactly 1 time. The
#         ``SET`` suffix anchors the match to the actual SQL clause
#         (the surrounding docstring's prose reference to
#         ``ON CONFLICT (cik) DO UPDATE`` deliberately omits the
#         ``SET`` keyword so the lint counter does not pick it up).
#       - The same file contains the literal
#         ``WHERE (EXCLUDED.filed_at, EXCLUDED.accession_number)``
#         exactly 1 time — the no-demotion monotonicity predicate
#         with same-day accession_number tie-break (Codex 1a HIGH +
#         Codex 2 Medium on PR9). Without the predicate at all, a
#         stale call passing an older N-CEN could overwrite the
#         newer row; without the tie-break, a same-day older
#         accession could clobber a same-day newer one (the
#         N-CEN/A-same-day-as-original case — both filed_at values
#         resolve to midnight UTC of the same calendar day).
#  D. Newest-first discovery + early return.
#       - app/services/ncen_classifier.py function
#         ``_find_latest_ncen`` body contains exactly 1
#         ``return _NCenAccessionRef(`` line. A refactor that
#         accumulates all N-CEN refs and picks the newest at the end
#         (instead of returning on first match) would pay SEC HTTP
#         budget for every historical N-CEN per filer — a regression
#         the structural cap is meant to lock out.
#       - The same function body MUST NOT contain ``reversed(``,
#         ``sorted(``, or ``.sort(`` (Codex 2 Medium on PR9). The
#         newest-first ordering comes from SEC's submissions array
#         convention; any iteration-order reversal or local re-sort
#         would silently flip "first match" to "oldest match" while
#         still passing the single-return count. The forbid-list is
#         the strongest static-grep proxy for newest-first ordering.
#
# Future PRs that add a SECOND writer (e.g. a manifest-worker parser
# at app/services/manifest_parsers/sec_n_cen.py, a bulk-dataset
# ingester, a rewash rescue branch) MUST extend this guard with the
# new chokepoint's invariant AND keep the at-most-one-row-per-CIK
# contract (every new writer must upsert on cik, never append).
#
# Exits non-zero on the first invariant violation. Wired into
# .githooks/pre-push after check_n_csr_retention.sh.
#
# Awk-based block parsing (BSD vs GNU `grep -P` portability — PR4
# Codex 1c lesson). Helpers re-implemented locally so this script
# stays self-contained.

set -euo pipefail

FILE_SCHEMA="sql/100_ncen_filer_classifications.sql"
FILE_CLASSIFIER="app/services/ncen_classifier.py"

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

# Count exact regex matches across a file.
count_regex() {
  local file="$1" pattern="$2"
  grep -Ec "$pattern" "$file" || true
}

# Count literal-substring occurrences inside a function body. Body
# bounded by ``def <name>(`` (at any indent) and the next column-0
# ``def `` or EOF.
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
# A — schema PK on cik
# ======================================================================
echo "Checking invariant A (schema PK on cik)..."

if [[ ! -f "$FILE_SCHEMA" ]]; then
  fail "missing file: $FILE_SCHEMA"
else
  # Regex: ``cik`` followed by whitespace, ``TEXT``, whitespace,
  # ``PRIMARY KEY``. Tolerates the canonical formatting from the
  # CREATE TABLE block. Forbids variants like UNIQUE-only or a
  # composite PRIMARY KEY clause (caught by the count).
  pk_lines=$(count_regex "$FILE_SCHEMA" "^[[:space:]]*cik[[:space:]]+TEXT[[:space:]]+PRIMARY[[:space:]]+KEY[[:space:]]*,?[[:space:]]*$")
  if (( pk_lines != 1 )); then
    fail "$FILE_SCHEMA: expected exactly 1 'cik TEXT PRIMARY KEY' line, found ${pk_lines}."
  fi
fi

# ======================================================================
# B — sole writer (one *.py file under app/ with the INSERT)
# ======================================================================
echo "Checking invariant B (single INSERT INTO ncen_filer_classifications writer)..."

writer_files=$(grep -rl "INSERT INTO ncen_filer_classifications" app --include="*.py" 2>/dev/null || true)
writer_count=0
if [[ -n "$writer_files" ]]; then
  writer_count=$(echo "$writer_files" | wc -l | tr -d ' ')
fi

if (( writer_count != 1 )); then
  fail "expected exactly 1 production *.py file under app/ with 'INSERT INTO ncen_filer_classifications', found ${writer_count}:"
  echo "$writer_files" >&2
fi

# ======================================================================
# C — UPSERT clause present in the classifier (with no-demotion predicate)
# ======================================================================
echo "Checking invariant C (UPSERT clause + no-demotion predicate)..."

if [[ ! -f "$FILE_CLASSIFIER" ]]; then
  fail "missing file: $FILE_CLASSIFIER"
else
  # The 'SET' suffix anchors the match to the actual SQL clause; the
  # docstring's prose reference to 'ON CONFLICT (cik) DO UPDATE'
  # deliberately omits 'SET' so this counter ignores it.
  upsert_count=$(count_literal "$FILE_CLASSIFIER" "ON CONFLICT (cik) DO UPDATE SET")
  if (( upsert_count != 1 )); then
    fail "$FILE_CLASSIFIER: expected exactly 1 'ON CONFLICT (cik) DO UPDATE SET' clause, found ${upsert_count}."
  fi

  # No-demotion predicate with same-day tie-break (Codex 1a HIGH +
  # Codex 2 Medium on PR9). The row-constructor opener
  # ``WHERE (EXCLUDED.filed_at, EXCLUDED.accession_number)`` is unique
  # enough to be unambiguous across the file.
  demotion_guard_count=$(count_literal "$FILE_CLASSIFIER" "WHERE (EXCLUDED.filed_at, EXCLUDED.accession_number)")
  if (( demotion_guard_count != 1 )); then
    fail "$FILE_CLASSIFIER: expected exactly 1 no-demotion predicate opener 'WHERE (EXCLUDED.filed_at, EXCLUDED.accession_number)', found ${demotion_guard_count}."
  fi
fi

# ======================================================================
# D — newest-first early return in _find_latest_ncen
# ======================================================================
echo "Checking invariant D (newest-first early return in _find_latest_ncen)..."

if [[ -f "$FILE_CLASSIFIER" ]]; then
  return_count=$(count_in_function "$FILE_CLASSIFIER" "_find_latest_ncen" "return _NCenAccessionRef(")
  if (( return_count != 1 )); then
    fail "$FILE_CLASSIFIER: _find_latest_ncen body expected exactly 1 'return _NCenAccessionRef(' (early return on first match), found ${return_count}."
  fi

  # Forbid iteration-order reversal / local re-sort. The newest-first
  # ordering comes from SEC's submissions array convention; any
  # ``reversed(`` / ``sorted(`` / ``.sort(`` inside _find_latest_ncen
  # would silently flip "first match" to "oldest match" while still
  # passing the single-return count above. Codex 2 Medium (PR9).
  for forbidden in "reversed(" "sorted(" ".sort("; do
    hits=$(count_in_function "$FILE_CLASSIFIER" "_find_latest_ncen" "$forbidden")
    if (( hits > 0 )); then
      fail "$FILE_CLASSIFIER: _find_latest_ncen body must not contain '${forbidden}' — newest-first iteration order would be flipped."
    fi
  done
fi

# ======================================================================
# Summary
# ======================================================================
if (( violations > 0 )); then
  echo "FAIL: ${violations} invariant violation(s) — N-CEN latest-only cap drift detected." >&2
  exit 1
fi

echo "OK: N-CEN latest-only invariants A / B / C / D satisfied."
