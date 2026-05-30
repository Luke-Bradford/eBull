#!/usr/bin/env bash
#
# #1257 — shellcheck gate over the repo's shell scripts.
#
# Why: the scripts/ directory holds ~20 chokepoint-lint guards (the
# PR4-PR12 retention / latest-only / MERGE-writer invariants). Those
# scripts are awk- and grep-heavy, exactly the territory where silent
# shell bugs hide — a clobbered variable (SC2034), a stderr redirect
# that competes with another (SC2261), an unquoted command-sub that
# word-splits unexpectedly (SC2046). PR #1255 shipped a dead variable
# assignment (overwritten by the next line) that shellcheck's SC2034
# would have caught before the bot-review round-trip. This gate closes
# that gap.
#
# Severity floor: ``-S warning`` (errors + warnings). The repo's
# scripts contain a few INTENTIONAL ``info``/``style`` patterns that we
# do NOT want to fix:
#   - SC2086 word-splitting in check_ci_mirrors_prepush.sh's
#     ``printf '  - %s\n' $hook_only`` (the splitting is the point —
#     it prints one line per drifted lint).
#   - SC1003 single-quote-escape notes inside printf format strings.
# Gating at ``warning`` keeps the meaningful defect classes (the codes
# #1257 was filed against: SC2034 / SC2155 / SC2086-when-a-bug / SC2046
# / SC2261) without forcing churn on by-design idioms. Tighten to
# ``-S info`` later only after auditing every note.
#
# Scope: scripts/*.sh (the whole shell-script tree is warning-clean as
# of #1257). Pure invocation of the shellcheck binary — no DB / docker
# / Python. ~200ms.
#
# Wired into BOTH .githooks/pre-push and .github/workflows/ci.yml so a
# --no-verify push cannot dodge it; the parity guard
# (check_ci_mirrors_prepush.sh) enforces that mirroring.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> check_shellcheck: linting scripts/*.sh at -S warning"

# Prefer the system binary (GitHub ubuntu runners ship shellcheck; brew
# installs it locally). Fall back to `uv run --with shellcheck-py` —
# ephemeral, so we don't add a permanent dep to pyproject/uv.lock for a
# binary most boxes already have (CLAUDE.md: no casual libraries). A
# bare `uv run shellcheck` would only resolve via the inherited system
# PATH, so it is NOT a real fallback when the binary is genuinely
# absent — #1257 Codex finding.
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck_cmd=(shellcheck)
elif command -v uv >/dev/null 2>&1; then
  shellcheck_cmd=(uv run --quiet --with shellcheck-py shellcheck)
else
  echo "ERROR: shellcheck not found (no system binary, no uv)." >&2
  exit 1
fi

# Default target = scripts/*.sh; optional positional args let the test
# suite point the gate at a synthetic fixture file (same convention as
# check_ci_mirrors_prepush.sh).
if [[ $# -gt 0 ]]; then
  targets=("$@")
else
  # cd to root so the glob and any ::error annotations carry
  # repo-relative paths.
  cd "$root"
  targets=(scripts/*.sh)
fi

"${shellcheck_cmd[@]}" -S warning "${targets[@]}"

echo "==> check_shellcheck: clean."
