#!/usr/bin/env bash
#
# Lint guard: the latest-10-K-per-instrument structural cap (#1233
# §4.10 / PR10a) MUST be honoured at every layer.
#
# Like PR9 (N-CEN) — and unlike PR4-PR8 horizon helpers —
# ``instrument_business_summary`` is a classification-shaped table:
# ``instrument_id`` is PRIMARY KEY, so the DB physically refuses a
# second row per instrument. The cap is enforced structurally by
# (A) the schema PK, (B) the canonical UPSERT writer carrying a
# no-demotion predicate on ``(filed_at, source_accession)``, (C) the
# count of UPSERT writer paths against the table (upsert path +
# tombstone path = exactly 2), and (D) the manifest worker never
# routing through the source_accession-mutating tombstone helper
# ``record_parse_attempt`` (filed_at-ASC drains would otherwise
# corrupt the incumbent's provenance — pinned by the existing test
# ``test_tombstone_path_does_not_mutate_existing_body_summary_row``).
#
# Letter labels A / B / C / D are local to this guard (no
# cross-mapping to PR4-PR9 alphabets — those guards count chokepoint
# placements or structural anchors specific to their source).
#
# Invariants:
#
#  A. Schema PK on instrument_id — declaration AND no later mutation.
#       A.1 sql/055_instrument_business_summary.sql contains exactly 1
#           line matching ``instrument_id <ws> BIGINT <ws> PRIMARY KEY``.
#       A.2 NO sql/*.sql file (other than 055 itself) contains a PK
#           mutation against ``instrument_business_summary`` — concretely:
#           any ``ALTER TABLE instrument_business_summary`` statement
#           that touches ``PRIMARY KEY``, the auto-named
#           ``instrument_business_summary_pkey`` constraint, or the
#           ``instrument_id`` column's type. Codex 2 round 1 HIGH —
#           without this scan the declaration check on 055 is bypassable
#           by a later migration that drops the PK or replaces it with
#           a composite key.
#  B. No-demotion predicate present (sole-writer anchor) — full WHERE
#     shape pinned, not just the tuple.
#       B.1 The literal ``(EXCLUDED.filed_at, EXCLUDED.source_accession)``
#           appears exactly 1 time across the entire repository under
#           ``app/`` — in ``app/services/business_summary.py``'s
#           ``upsert_business_summary`` UPSERT clause.
#       B.2 The same file contains
#           ``instrument_business_summary.filed_at IS NULL`` exactly 1
#           time — the NULL-incumbent re-baseline branch.
#       B.3 The same file contains
#           ``>= (instrument_business_summary.filed_at,`` exactly 1 time
#           — the comparator + LHS-of-RHS opener of the row-constructor
#           tuple comparison. Codex 2 round 1 HIGH — without this anchor
#           a future refactor could flip ``>=`` to ``<=`` (which would
#           demote rather than promote) and B.1+B.2 would still pass.
#       B.4 The same file contains
#           ``instrument_business_summary.source_accession))`` exactly 1
#           time — the closing of the no-demotion comparison's RHS
#           tuple. Pins the full shape end-to-end.
#  C. UPSERT writer surface bounded — INSERT AND UPDATE chokepoints.
#       C.1 ``app/services/business_summary.py`` contains exactly 2
#           canonical ``INSERT INTO instrument_business_summary``
#           chokepoints (word-bounded so the sibling
#           ``instrument_business_summary_sections`` writes don't
#           inflate the count) — ``upsert_business_summary`` (the
#           gated writer; B above) and ``record_parse_attempt`` (the
#           tombstone helper; D below). The matching
#           ``ON CONFLICT (instrument_id) DO UPDATE SET`` count MUST
#           equal the INSERT count.
#       C.2 NO other production *.py file under ``app/`` contains a
#           canonical ``INSERT INTO instrument_business_summary``
#           (word-bounded). A new writer path must either share the
#           gated upsert or extend this guard with its own invariant.
#       C.3 NO production *.py file under ``app/`` contains an
#           ``UPDATE instrument_business_summary`` statement (i.e.
#           ungated UPDATE bypassing the no-demotion clause). Codex 2
#           round 1 MEDIUM — the spec's ``no ungated UPDATEs`` clause
#           was not statically pinned; this check closes that gap.
#  D. Manifest worker stays away from the source_accession-mutating
#     tombstone helper — single comprehensive check covering call,
#     import (single-line OR multi-line block), and aliased import.
#       D.1 ``app/services/manifest_parsers/sec_10k.py`` MUST NOT
#           reference the identifier ``record_parse_attempt`` anywhere
#           — calls, imports (single-line, multi-line block, or
#           aliased ``record_parse_attempt as stamp_attempt``), or
#           docstring mentions. Codex 2 round 3 MEDIUM — a narrower
#           call+single-line-import check missed
#           ``from app.services.business_summary import (
#               record_parse_attempt as stamp_attempt,
#           )``. The simplest robust guard is "the identifier name
#           never appears in this file"; the file's existing
#           docstring (lines 1-55) deliberately avoids naming the
#           helper. A future docstring author must use indirect prose
#           (e.g. "the tombstone helper") rather than the bare name
#           — that constraint is the price of the lint's coverage.
#
# Future PRs that add a new writer (e.g. a bulk-dataset 10-K ingester,
# a rewash rescue branch) MUST extend this guard with the new
# chokepoint's invariant AND share the gated ``upsert_business_summary``
# (no append paths, no ungated UPDATEs against
# ``instrument_business_summary``).
#
# Exits non-zero on the first invariant violation. Wired into
# ``.githooks/pre-push`` after ``check_ncen_latest_only.sh``.
#
# Awk-based block parsing (BSD vs GNU ``grep -P`` portability — PR4
# Codex 1c lesson). Helpers re-implemented locally so this script
# stays self-contained.

set -euo pipefail

FILE_SCHEMA="sql/055_instrument_business_summary.sql"
FILE_SERVICE="app/services/business_summary.py"
FILE_MANIFEST="app/services/manifest_parsers/sec_10k.py"

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

# ======================================================================
# A — schema PK on instrument_id (declaration + no later mutation)
# ======================================================================
echo "Checking invariant A (schema PK on instrument_id)..."

# A.1 — declaration
if [[ ! -f "$FILE_SCHEMA" ]]; then
  fail "missing file: $FILE_SCHEMA"
else
  # Regex: ``instrument_id`` followed by whitespace, ``BIGINT``,
  # whitespace, ``PRIMARY KEY`` at end of line. The PK declaration
  # spans two source lines (``REFERENCES instruments(...)`` is on the
  # next line) but only the first line carries the ``PRIMARY KEY``
  # token. Forbids a composite primary key or UNIQUE-only demotion.
  pk_lines=$(count_regex "$FILE_SCHEMA" "^[[:space:]]*instrument_id[[:space:]]+BIGINT[[:space:]]+PRIMARY[[:space:]]+KEY[[:space:]]*$")
  if (( pk_lines != 1 )); then
    fail "$FILE_SCHEMA: expected exactly 1 'instrument_id BIGINT PRIMARY KEY' line, found ${pk_lines}."
  fi
fi

# A.2 — forbid PK mutation in any later migration. A two-pass scan:
# (a) collect every sql/*.sql file that mentions ``ALTER TABLE
# instrument_business_summary`` (excluding 055 itself, which declares
# the table), then (b) within those files reject any line touching
# ``PRIMARY KEY``, the auto-named constraint ``instrument_business_summary_pkey``,
# or a type change against the ``instrument_id`` column. Each
# forbidden pattern is independently checked so a future PK mutation
# can't slip in via any of the three vectors.
# Portable enumeration — macOS ships bash 3.2 which lacks ``mapfile``,
# and ``<<<`` here-strings can fail silently when /tmp is unwritable
# (Codex 2 round 2 HIGH: the sandbox emitted ``cannot create temp file
# for here document`` while the script still exited 0). Replace both
# with an IFS-newline ``for`` loop so iteration uses no temp file and
# any failure in the grep pipeline propagates via ``set -euo pipefail``.
alter_files_raw=$(grep -rl "ALTER TABLE instrument_business_summary" sql --include="*.sql" 2>/dev/null | grep -v "^${FILE_SCHEMA}$" || true)
if [[ -n "$alter_files_raw" ]]; then
  saved_ifs=$IFS
  IFS=$'\n'
  for alter_file in $alter_files_raw; do
    [[ -z "$alter_file" ]] && continue
    # Scope the scan to ALTER TABLE blocks (between an ALTER TABLE line
    # and the next ``;`` terminator) so an unrelated comment elsewhere
    # in the file mentioning ``PRIMARY KEY`` doesn't false-fail (Codex
    # 2 round 2 LOW). Same awk pattern as C.3's UPDATE-block walker;
    # fall-through (no ``next`` on the opener) lets single-line ALTER
    # statements be checked on the entry line.
    gated_pk_hits=$(awk '
      BEGIN { in_alter = 0; n = 0 }
      /ALTER[[:space:]]+TABLE[[:space:]]+instrument_business_summary([^A-Za-z0-9_]|$)/ {
        in_alter = 1
      }
      in_alter {
        if ($0 ~ /PRIMARY[[:space:]]+KEY/) n++
        if (index($0, "instrument_business_summary_pkey") > 0) n++
        if ($0 ~ /ALTER[[:space:]]+COLUMN[[:space:]]+instrument_id/) n++
        if ($0 ~ /;[[:space:]]*$/) in_alter = 0
      }
      END { print n + 0 }
    ' "$alter_file")
    if (( gated_pk_hits > 0 )); then
      fail "$alter_file: forbidden PK mutation against instrument_business_summary detected inside an ALTER TABLE block (hits=${gated_pk_hits}). The structural cap depends on the PK declaration in $FILE_SCHEMA — extend this guard if you genuinely need to reshape the table."
    fi
  done
  IFS=$saved_ifs
fi

# ======================================================================
# B — no-demotion predicate present (full WHERE shape pinned)
# ======================================================================
echo "Checking invariant B (no-demotion predicate full shape)..."

# B.1 — tuple opener (sole-anchor)
tuple_files=$(grep -rl "(EXCLUDED.filed_at, EXCLUDED.source_accession)" app --include="*.py" 2>/dev/null || true)
tuple_count=0
if [[ -n "$tuple_files" ]]; then
  tuple_count=$(echo "$tuple_files" | wc -l | tr -d ' ')
fi

if (( tuple_count != 1 )); then
  fail "expected exactly 1 production *.py file under app/ with '(EXCLUDED.filed_at, EXCLUDED.source_accession)', found ${tuple_count}:"
  echo "$tuple_files" >&2
fi

if [[ -f "$FILE_SERVICE" ]]; then
  # B.1 — tuple opener (count in the canonical file)
  tuple_in_service=$(count_literal "$FILE_SERVICE" "(EXCLUDED.filed_at, EXCLUDED.source_accession)")
  if (( tuple_in_service != 1 )); then
    fail "$FILE_SERVICE: expected exactly 1 '(EXCLUDED.filed_at, EXCLUDED.source_accession)' tuple-comparison opener, found ${tuple_in_service}."
  fi

  # B.2 — NULL-incumbent re-baseline branch
  null_branch_count=$(count_literal "$FILE_SERVICE" "instrument_business_summary.filed_at IS NULL")
  if (( null_branch_count != 1 )); then
    fail "$FILE_SERVICE: expected exactly 1 'instrument_business_summary.filed_at IS NULL' WHERE branch (NULL-incumbent re-baseline), found ${null_branch_count}."
  fi

  # B.3 — comparator + RHS opener. The canonical predicate is
  #       ``(EXCLUDED.filed_at, EXCLUDED.source_accession) >= (instrument_business_summary.filed_at, instrument_business_summary.source_accession)``.
  # Pinning the literal ``>= (instrument_business_summary.filed_at,`` catches
  # comparator flips (``<=`` / ``<`` / ``>``) and RHS swaps (e.g. EXCLUDED-vs-EXCLUDED
  # self-comparison) while still tolerating whitespace inside the row-constructor.
  comparator_count=$(count_literal "$FILE_SERVICE" ">= (instrument_business_summary.filed_at,")
  if (( comparator_count != 1 )); then
    fail "$FILE_SERVICE: expected exactly 1 '>= (instrument_business_summary.filed_at,' comparator + RHS opener (no-demotion direction), found ${comparator_count}."
  fi

  # B.4 — RHS closing of the row-constructor comparison. Together with
  # B.1+B.3 this pins the full row-constructor tuple end-to-end.
  rhs_close_count=$(count_literal "$FILE_SERVICE" "instrument_business_summary.source_accession))")
  if (( rhs_close_count != 1 )); then
    fail "$FILE_SERVICE: expected exactly 1 'instrument_business_summary.source_accession))' RHS-tuple closer, found ${rhs_close_count}."
  fi
fi

# ======================================================================
# C — UPSERT writer surface bounded (INSERT + UPDATE)
# ======================================================================
echo "Checking invariant C (UPSERT writer surface bounded)..."

# Canonical-INSERT regex — match ``INSERT INTO instrument_business_summary``
# followed by a word boundary so the sibling
# ``instrument_business_summary_sections`` table doesn't inflate the
# count. Word boundary expressed as ``[^A-Za-z0-9_]`` OR end-of-line so
# both ``INSERT INTO instrument_business_summary\n`` (column list on
# next line) and ``INSERT INTO instrument_business_summary (...)``
# (column list inline) match, while ``_sections`` does not.
#
# Per-canonical-INSERT we expect a paired ``ON CONFLICT (instrument_id)
# DO UPDATE SET`` clause — so the UPSERT-clause count inside the
# service module MUST equal the INSERT count to prove every INSERT
# survives a conflict via an in-place UPDATE (no append paths).
TABLE_INSERT_REGEX="INSERT INTO instrument_business_summary([^A-Za-z0-9_]|$)"
TABLE_UPDATE_REGEX="UPDATE instrument_business_summary([^A-Za-z0-9_]|$)"

if [[ ! -f "$FILE_SERVICE" ]]; then
  fail "missing file: $FILE_SERVICE"
else
  # C.1 — exactly 2 canonical INSERTs in the service module
  inserts_in_service=$(count_regex "$FILE_SERVICE" "$TABLE_INSERT_REGEX")
  if (( inserts_in_service != 2 )); then
    fail "$FILE_SERVICE: expected exactly 2 canonical 'INSERT INTO instrument_business_summary' chokepoints (upsert_business_summary + record_parse_attempt), found ${inserts_in_service}."
  fi

  upsert_in_service=$(count_literal "$FILE_SERVICE" "ON CONFLICT (instrument_id) DO UPDATE SET")
  if (( upsert_in_service != inserts_in_service )); then
    fail "$FILE_SERVICE: 'ON CONFLICT (instrument_id) DO UPDATE SET' count (${upsert_in_service}) must equal INSERT count (${inserts_in_service}) — every INSERT must carry an UPSERT clause."
  fi
fi

# C.2 — no other writer surface under app/. Word-bounded scan so the
# sibling ``_sections`` table isn't flagged.
other_writers=$(grep -rlE "$TABLE_INSERT_REGEX" app --include="*.py" 2>/dev/null | grep -v "^${FILE_SERVICE}$" || true)
if [[ -n "$other_writers" ]]; then
  fail "found additional INSERT INTO instrument_business_summary writer(s) outside ${FILE_SERVICE}:"
  echo "$other_writers" >&2
fi

# C.3 — no ungated UPDATEs of the GATED columns (body /
# source_accession / filed_at). The no-demotion clause lives in the
# UPSERT branch of an INSERT statement; a bare ``UPDATE
# instrument_business_summary SET body=..., source_accession=...,
# filed_at=...`` would bypass the gate entirely.
#
# Failure-tracking columns (``attempt_count``, ``last_failure_reason``,
# ``next_retry_at``) are NOT gated by the no-demotion invariant — the
# admin reset endpoint at ``app/api/business_summary_admin.py:209``
# legitimately UPDATEs them without going through the upsert. So the
# check inspects UPDATE blocks and only fails when the SET clause
# touches one of the three gated columns.
#
# Each candidate file is scanned via awk: it tracks ``in_update``
# inside an UPDATE statement against this table (multi-line SQL
# strings under ``"""..."""`` or trailing-``;``), and flags any line
# matching ``(body|source_accession|filed_at)[[:space:]]*=`` while in
# that block. Plain Python kwargs like ``source_accession=str(...)`` in
# a row constructor never appear inside an UPDATE block (which exists
# only inside ``cur.execute("""...""")`` SQL strings), so the scope
# narrows naturally without an allowlist.
update_files=$(grep -rlE "$TABLE_UPDATE_REGEX" app --include="*.py" 2>/dev/null || true)
if [[ -n "$update_files" ]]; then
  # IFS=newline for loop — same portability rationale as A.2 above
  # (Codex 2 round 2 HIGH).
  saved_ifs=$IFS
  IFS=$'\n'
  for upd_file in $update_files; do
    [[ -z "$upd_file" ]] && continue
    gated_hits=$(awk '
      BEGIN { in_update = 0; n = 0 }
      # Detect entry to an UPDATE block. NO ``next`` — fall through to
      # the in_update block so the SAME line is also checked for gated-
      # column assignments. Without fall-through, a single-line
      # ``UPDATE instrument_business_summary SET body = ...`` bypasses
      # the check entirely (Codex 2 round 2 MEDIUM).
      /UPDATE[[:space:]]+instrument_business_summary([^A-Za-z0-9_]|$)/ {
        in_update = 1
      }
      in_update {
        # Gated-column SET assignment check runs FIRST so an end-of-
        # block line that ALSO carries a gated assignment is caught
        # before we exit the block. Multi-column SET clauses keep
        # ``in_update`` true across subsequent lines until the closer.
        if (match($0, /(body|source_accession|filed_at)[[:space:]]*=/) > 0) {
          n++
        }
        # End of SQL string block: line is just """ (optionally with
        # comma / paren) — covers both ``""")`` and ``""",`` closers.
        if ($0 ~ /^[[:space:]]*"""[[:space:]]*[,)]?[[:space:]]*$/) {
          in_update = 0
        }
        # Bare SQL terminator at end of a string (rare; defensive).
        # Also covers the single-line UPDATE form once the trailing
        # ``;`` is present.
        else if ($0 ~ /;[[:space:]]*$/) {
          in_update = 0
        }
      }
      END { print n + 0 }
    ' "$upd_file")
    if (( gated_hits > 0 )); then
      fail "$upd_file: UPDATE statement against instrument_business_summary touches a gated column (body / source_accession / filed_at) — every gated-column write MUST go through the no-demotion upsert in ${FILE_SERVICE}. Hits: ${gated_hits}."
    fi
  done
  IFS=$saved_ifs
fi

# ======================================================================
# D — manifest worker stays away from record_parse_attempt (call+import)
# ======================================================================
echo "Checking invariant D (manifest worker excludes record_parse_attempt)..."

if [[ ! -f "$FILE_MANIFEST" ]]; then
  fail "missing file: $FILE_MANIFEST"
else
  # Comprehensive bare-identifier scan — catches calls
  # (``record_parse_attempt(``), single-line imports
  # (``from … import record_parse_attempt``), multi-line import blocks
  # (one identifier per line inside ``import (...)``), and aliased
  # imports (``record_parse_attempt as stamp_attempt``). The constraint
  # is conservative: docstring mentions also trip the lint. The
  # existing module-level docstring deliberately omits the helper's
  # name; future authors who need to reference it in prose must use
  # indirect language. Codex 2 round 3 MEDIUM consolidation.
  total_refs=$(count_literal "$FILE_MANIFEST" "record_parse_attempt")
  if (( total_refs != 0 )); then
    fail "$FILE_MANIFEST: must not reference 'record_parse_attempt' anywhere (manifest drains filed_at-ASC; the helper mutates source_accession on conflict, so any import or call is unsafe — aliased or otherwise). Found ${total_refs} occurrence(s):"
    grep -nF "record_parse_attempt" "$FILE_MANIFEST" >&2 || true
  fi
fi

# ======================================================================
# Summary
# ======================================================================
if (( violations > 0 )); then
  echo "FAIL: ${violations} invariant violation(s) — business_summary latest-only cap drift detected." >&2
  exit 1
fi

echo "OK: business_summary latest-only invariants A / B / C / D satisfied."
