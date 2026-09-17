#!/usr/bin/env bash
#
# CAVEMAN: thin wrapper. Match shape of sibling check_*.sh so
# .githooks/pre-push and ci.yml can chain it identically. Heavy lifting
# lives in the Python AST script — see
# scripts/check_ownership_rollup_snapshot.py.
#
# Rule (#2789): every call to get_ownership_rollup under app/ or
# scripts/ MUST be lexically inside a `with snapshot_read(...)` block.
# The reader issues many separate reads and does NOT open its own
# transaction, so a concurrent ownership refresh can otherwise return a
# denominator and a wedge from different commits — a torn render that
# raises nothing.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

exec uv run python scripts/check_ownership_rollup_snapshot.py "$@"
