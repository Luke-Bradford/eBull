#!/usr/bin/env bash
#
# Lint guard for #1346: every ownership_*_current MERGE helper MUST
# execute `SET LOCAL jit = off` as the first statement inside its
# `with conn.transaction(), conn.cursor() as cur:` block.
#
# Rationale: the MERGE is partition-pruned (sql/177 institutions
# observations have 125+ partitions to 2040q4). PG's planner+JIT
# overhead (771 functions ≈ 307 ms) dominates the small per-instrument
# query work (≈ 1 ms). `SET LOCAL jit = off` is transaction-scoped and
# saves ≈ 430 ms per call × ≈ 10k helper invocations during S22 ≈ 70 min
# wall-clock per bootstrap. Verified 1.86× speedup at #1345.
#
# Invariants:
#
#   I1. The helper file (app/services/ownership_observations.py) MUST
#       contain EXACTLY 14 occurrences of the literal
#       `cur.execute("SET LOCAL jit = off")` — one per helper transaction:
#         refresh_insiders_current
#         refresh_institutions_current
#         refresh_blockholders_current
#         refresh_treasury_current
#         refresh_def14a_current
#         refresh_funds_current
#         refresh_esop_current
#         refresh_insiders_current_batch
#         refresh_institutions_current_batch
#         refresh_funds_current_batch
#         refresh_blockholders_current_batch  (#1345 PR-A)
#         refresh_treasury_current_batch      (#1345 PR-A)
#         refresh_def14a_current_batch        (#1345 PR-A)
#         refresh_esop_current_batch          (#1345 PR-A)
#
#   I2. Every `with conn.transaction(), conn.cursor() as cur:` block in
#       the helper file MUST be IMMEDIATELY followed by the jit=off
#       statement (no other `cur.execute` may precede it inside the
#       same transaction — otherwise the MERGE plan would be compiled
#       with JIT before the GUC change).
#
# Exits non-zero on the first invariant violation.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Optional override for unit tests. Default to the canonical helper file.
HELPER_FILE="${1:-$REPO_ROOT/app/services/ownership_observations.py}"

if [[ ! -f "$HELPER_FILE" ]]; then
  echo "ERROR: helper file not found: $HELPER_FILE" >&2
  exit 1
fi

EXPECTED_COUNT=14

# I1 — count of jit=off statements
JIT_COUNT=$(grep -c '^[[:space:]]*cur\.execute("SET LOCAL jit = off")' "$HELPER_FILE" || true)
if [[ "$JIT_COUNT" -ne "$EXPECTED_COUNT" ]]; then
  echo "FAIL (I1): expected exactly $EXPECTED_COUNT 'SET LOCAL jit = off' statements in $HELPER_FILE, found $JIT_COUNT" >&2
  echo "Per #1346, every ownership_*_current MERGE helper transaction must disable JIT (partition-pruned MERGE too small for amortisation)." >&2
  exit 1
fi

# I2 — every `with conn.transaction(), conn.cursor() as cur:` block must
# have jit=off as the FIRST cur.execute() statement. Walk the file line
# by line: each time we hit the `with ...` opener, advance through any
# blank/comment lines and require the next executable line is the jit=off
# literal.
HELPER_PATH="$HELPER_FILE" python3 - <<'PY'
import os
import sys
from pathlib import Path

HELPER = Path(os.environ["HELPER_PATH"])
OPENER = "    with conn.transaction(), conn.cursor() as cur:"
JIT_LINE = '        cur.execute("SET LOCAL jit = off")'

lines = HELPER.read_text().splitlines()
violations: list[str] = []
opener_count = 0
for idx, line in enumerate(lines):
    if line.rstrip() != OPENER:
        continue
    opener_count += 1
    # Scan forward skipping blank lines and comment-only lines until first
    # executable statement. Stop after a small window — the jit=off must
    # be near the opener; large gaps indicate a violation regardless.
    found_jit = False
    for next_line in lines[idx + 1 : idx + 8]:
        stripped = next_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if next_line.startswith(JIT_LINE):
            found_jit = True
        break
    if not found_jit:
        violations.append(
            f"line {idx + 1}: `with conn.transaction(), conn.cursor() as cur:` block does not have `SET LOCAL jit = off` as the first executable statement"
        )

if opener_count != 14:
    print(
        f"FAIL (I2): expected exactly 14 `with conn.transaction(), conn.cursor() as cur:` openers, found {opener_count}",
        file=sys.stderr,
    )
    sys.exit(1)
if violations:
    print("FAIL (I2): jit=off must be the first executable statement inside every helper transaction:", file=sys.stderr)
    for v in violations:
        print(f"  - {v}", file=sys.stderr)
    sys.exit(1)
PY

echo "OK: $EXPECTED_COUNT/$EXPECTED_COUNT helper transactions have jit=off"
