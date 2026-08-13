#!/usr/bin/env bash
#
# Lint guard for #1479 PR2: every Anthropic SDK client under app/ MUST be
# constructed through ``app/services/anthropic_client.make_anthropic_client``,
# which applies a bounded ``timeout=`` + ``max_retries=``.
#
# Why: the Anthropic SDK's default per-request timeout is 600s on the
# read/write/pool phases with 2 auto-retries. A black-holed outbound read
# therefore hangs a worker thread ~3×600s ≈ 30 min — the outbound analogue
# of the unbounded ``psycopg.connect`` the PGCONNECT_TIMEOUT guard (#1475)
# closed. A boot-reachable instance of exactly this (daily_research_refresh
# → cascade_refresh) wedged the jobs-process boot ~43 min on 2026-06-04.
# A raw ``anthropic.Anthropic(...)`` anywhere else silently reintroduces the
# unbounded default; this guard makes the single-chokepoint contract
# self-enforcing.
#
# Invariant: NO ``anthropic.Anthropic(`` / ``anthropic.AsyncAnthropic(``
# construction may appear under app/ EXCEPT inside the factory module
# ``app/services/anthropic_client.py``. (Bare type annotations like
# ``-> anthropic.Anthropic:`` carry no open-paren and are not matched.)
#
# Exits non-zero on the first violation. Pure shell.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Optional override for unit tests. Default to the canonical app/ tree.
SCAN_ROOT="${1:-$REPO_ROOT/app}"
FACTORY_REL="services/anthropic_client.py"

if [[ ! -d "$SCAN_ROOT" ]]; then
  echo "ERROR: scan root not found: $SCAN_ROOT" >&2
  exit 1
fi

# Match a construction call: ``anthropic.Anthropic(`` or
# ``anthropic.AsyncAnthropic(`` (open-paren required → excludes the
# ``-> anthropic.Anthropic:`` return annotation and ``import anthropic``).
PATTERN='anthropic\.(Async)?Anthropic\('

# grep -rn over *.py, excluding the factory file. ``|| true`` so a clean
# tree (grep exit 1 = no matches) does not trip ``set -e``.
HITS=$(grep -rnE --include='*.py' "$PATTERN" "$SCAN_ROOT" \
  | grep -vF "$FACTORY_REL" \
  || true)

if [[ -n "$HITS" ]]; then
  echo "FAIL (#1479): raw Anthropic client construction outside the factory:" >&2
  printf '%s\n' "$HITS" >&2
  echo >&2
  echo "Construct via app.services.anthropic_client.make_anthropic_client(...)" >&2
  echo "so the bounded timeout + max_retries (#1479) are applied — never the" >&2
  echo "unbounded 600s SDK default." >&2
  exit 1
fi

echo "==> check_anthropic_timeout: OK (all app/ Anthropic clients go through the factory)"
