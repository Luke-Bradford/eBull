#!/usr/bin/env bash
#
# Thin wrapper, same shape as the sibling check_*.sh so .githooks/pre-push
# and ci.yml chain it identically. The rule and its limits live in
# scripts/check_money_path_size.py.
#
# Rule (#3613 item 5): a change may add plus delete at most 400 lines across
# the money-path files; over that, split it into separately reviewed PRs.
# No args = this branch against `git merge-base origin/main HEAD`.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

exec uv run python scripts/check_money_path_size.py "$@"
