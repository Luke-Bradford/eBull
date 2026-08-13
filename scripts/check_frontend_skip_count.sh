#!/usr/bin/env bash
#
# #990 (follow-up to #971/#989) — skip-test accumulation gate.
#
# Counts ``describe.skip`` / ``it.skip`` occurrences under frontend/src
# and fails if the count EXCEEDS the stored baseline. A skipped frontend
# test is invisible coverage debt: it passes CI green while asserting
# nothing. #989 introduced a batch of ``describe.skip`` blocks "pending
# follow-up cleanup"; without a gate those silently accumulate across
# future PRs.
#
# Design (per #990): a stored baseline + explicit bump, NOT a hard upper
# bound of 0. Adding a justified skip is allowed — the author bumps
# scripts/frontend_skip_baseline.txt in the same PR, which makes the new
# skip visible in the diff and reviewable. Removing skips is always free
# (count <= baseline passes); re-baseline downward opportunistically.
#
# Exits non-zero when count > baseline. Pure shell + grep. ~30ms. Wired
# into .githooks/pre-push AND .github/workflows/ci.yml (parity guard
# #1329). Skips cleanly when frontend/ is absent (backend-only worktree).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Paths default to the real tree; optional positional args let the test
# suite point the gate at synthetic fixtures (same convention as
# check_ci_mirrors_prepush.sh).
src="${1:-$root/frontend/src}"
baseline_file="${2:-$root/scripts/frontend_skip_baseline.txt}"

echo "==> check_frontend_skip_count: counting describe.skip / it.skip in frontend/src"

if [[ ! -d "$src" ]]; then
  echo "==> check_frontend_skip_count: frontend/src absent — skipped."
  exit 0
fi

if [[ ! -f "$baseline_file" ]]; then
  echo "ERROR: missing baseline file $baseline_file" >&2
  exit 1
fi

baseline="$(tr -d '[:space:]' < "$baseline_file")"
if [[ ! "$baseline" =~ ^[0-9]+$ ]]; then
  echo "ERROR: baseline file $baseline_file does not contain a non-negative integer (got '$baseline')" >&2
  exit 1
fi

# grep -r over the source tree; -E for the (describe|it).skip alternation.
# || true because grep exits 1 when there are zero matches, which under
# set -e would kill the script on the (legitimate) clean state.
count="$(grep -rEo '(describe|it)\.skip' "$src" | grep -c . || true)"

echo "==> check_frontend_skip_count: count=$count baseline=$baseline"

if [[ "$count" -gt "$baseline" ]]; then
  echo "ERROR: frontend skip count ($count) exceeds baseline ($baseline)." >&2
  echo "       New describe.skip / it.skip blocks:" >&2
  grep -rEn '(describe|it)\.skip' "$src" >&2 || true
  echo "       Either un-skip the test, or — if the skip is justified —" >&2
  echo "       bump $baseline_file to $count in the same PR so the new" >&2
  echo "       skip is visible in the diff and reviewable." >&2
  exit 1
fi

if [[ "$count" -lt "$baseline" ]]; then
  echo "==> check_frontend_skip_count: count below baseline — consider lowering" \
       "$baseline_file to $count to ratchet the gate."
fi

echo "==> check_frontend_skip_count: OK ($count <= $baseline)."
