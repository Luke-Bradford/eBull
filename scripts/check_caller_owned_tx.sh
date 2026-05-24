#!/usr/bin/env bash
#
# CAVEMAN: thin wrapper. Match shape of sibling check_*.sh so
# .githooks/pre-push can chain it identically. Heavy lifting lives in
# the Python AST script — see scripts/check_caller_owned_tx.py.
#
# Rule (#1233 run-8-readiness-fixes Item 8): FINRA caller-owned ingest
# modules under app/services/finra_*_ingest.py MUST NOT enter their own
# `with conn.transaction():` block. Manifest parsers under
# app/services/manifest_parsers/ legitimately DO use it and are NOT in
# scope (different transaction-ownership contract).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

exec uv run python scripts/check_caller_owned_tx.py "$@"
