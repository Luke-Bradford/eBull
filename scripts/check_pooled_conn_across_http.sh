#!/usr/bin/env bash
#
# CAVEMAN: thin wrapper. Match shape of sibling check_*.sh so
# .githooks/pre-push + ci.yml can chain it identically. Heavy lifting
# lives in the Python AST script — see check_pooled_conn_across_http.py.
#
# Rule (#1472 PR2): a route declaring ``conn = Depends(get_conn)`` holds
# the pooled connection for its whole body. It MUST NOT reach an external
# provider's HTTP client (eToro / SEC EDGAR) while holding it — a conn
# pinned across slow external I/O stalls a small pool (block-then-
# PoolTimeout, max_waiting=0). Drive get_conn by hand and release before
# the external call instead (prevention-log #267).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

exec uv run python scripts/check_pooled_conn_across_http.py "$@"
