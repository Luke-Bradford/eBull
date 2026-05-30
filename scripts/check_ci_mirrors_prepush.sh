#!/usr/bin/env bash
# #1329 — parity guard: every chokepoint-lint script invoked by the
# pre-push hook (.githooks/pre-push) MUST also run in CI
# (.github/workflows/ci.yml), and vice versa.
#
# Why: a `--no-verify` push of a chokepoint regression lands green at
# CI if the matching lint is pre-push-only. That is exactly how #1382
# and #1387 drifted unnoticed. Mirroring the scripts once (#1329) does
# not prevent the NEXT hook script from re-opening the gap — this guard
# makes the mirror self-enforcing: add a `bash scripts/check_*.sh` line
# to one file and forget the other, the push fails here.
#
# Compares the set of `bash scripts/check_*.sh` invocations in each
# file (this comparator excludes itself). Exit 0 = parity. Exit 1 =
# drift, with the offending side printed. Pure shell. ~20ms.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Paths default to the real files; optional positional args let the
# test suite point the comparator at synthetic fixtures.
self="check_ci_mirrors_prepush.sh"
hook="${1:-$root/.githooks/pre-push}"
ci="${2:-$root/.github/workflows/ci.yml}"

echo "==> check_ci_mirrors_prepush: comparing pre-push hook vs ci.yml lint sets"

# Extract sorted-unique check_*.sh basenames invoked via `bash scripts/`,
# dropping this comparator so it need not mirror itself.
extract() {
  grep -oE "bash scripts/check_[a-z0-9_]+\.sh" "$1" \
    | sed -E 's#^bash scripts/##' \
    | grep -vxF "$self" \
    | sort -u
}

hook_set="$(extract "$hook")"
ci_set="$(extract "$ci")"

hook_only="$(comm -23 <(printf '%s\n' "$hook_set") <(printf '%s\n' "$ci_set"))"
ci_only="$(comm -13 <(printf '%s\n' "$hook_set") <(printf '%s\n' "$ci_set"))"

status=0
if [[ -n "$hook_only" ]]; then
  status=1
  echo "ERROR: lint scripts in $hook but NOT mirrored in $ci:" >&2
  printf '  - %s\n' $hook_only >&2
fi
if [[ -n "$ci_only" ]]; then
  status=1
  echo "ERROR: lint scripts in $ci but NOT in $hook:" >&2
  printf '  - %s\n' $ci_only >&2
fi

if [[ "$status" -ne 0 ]]; then
  echo "==> check_ci_mirrors_prepush: DRIFT. Mirror the missing lint(s) so a --no-verify push cannot dodge them." >&2
  exit 1
fi

echo "==> check_ci_mirrors_prepush: parity OK ($(printf '%s\n' "$hook_set" | grep -c .) lints in both)."
