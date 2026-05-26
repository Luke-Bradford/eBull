#!/usr/bin/env bash
#
# perf-bench harness. Single command per ticket; produces 3 artifacts
# under var/perf_baselines/<ticket>-<sha>.{txt,json,manifest.yaml} that
# the perf-claim-lint CI job (.github/workflows/ci.yml) validates.
#
# Spec: docs/proposals/etl/phase-0-instrumentation.md §2.6 NEW-A.1 +
# master plan docs/proposals/etl/bootstrap-sub-1h-plan.md §4.
#
# Thin wrapper; heavy lifting in scripts.perf_bench._run_explain. Mirror
# of the sibling check_*.sh shape (set -euo pipefail + REPO_ROOT + exec
# uv run python) so .githooks/pre-push can chain it identically when a
# perf-claim PR lands.
#
# Usage:
#   scripts/perf_bench/run_explain.sh <ticket_id>
#   scripts/perf_bench/run_explain.sh <ticket_id> --check-floors-only
#
# Refusals (delegated to the python module):
#   - EBULL_BENCH_DB_URL unset
#   - scripts/perf_bench/<ticket_id>.yaml missing
#   - psql not on PATH
#   - dirty working tree (artifact SHA must be authoritative)
#   - --check-floors-only + target_table row count below floors.yaml

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

exec uv run python -m scripts.perf_bench._run_explain "$@"
