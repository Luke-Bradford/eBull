#!/usr/bin/env bash
#
# Lint guard for #1919 (sibling of check_anthropic_timeout.sh, same #1479
# hang class): every OpenAI-compatible LLM call under app/ MUST be
# constructed through ``app/services/llm_client.py``, which applies a
# bounded ``httpx.Timeout`` (read=600s sized for local 14B decode speeds)
# plus the per-process call semaphore.
#
# Why: a raw ``httpx.post(".../chat/completions", ...)`` anywhere else can
# silently omit the timeout — the outbound analogue of the unbounded
# ``anthropic.Anthropic(...)`` default (600s×3 retries ≈ 30 min hang) that
# wedged the jobs boot ~43 min on 2026-06-04.
#
# Invariant: the string ``chat/completions`` (the OpenAI-compatible
# completion route) may not appear under app/ outside the provider module
# ``app/services/llm_client.py``. httpx itself is used legitimately across
# app/ (SEC, eToro, news) — the completion route is the LLM-specific marker.
#
# Exits non-zero on the first violation. Pure shell.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Optional override for unit tests. Default to the canonical app/ tree.
SCAN_ROOT="${1:-$REPO_ROOT/app}"
FACTORY_REL="services/llm_client.py"

if [[ ! -d "$SCAN_ROOT" ]]; then
  echo "ERROR: scan root not found: $SCAN_ROOT" >&2
  exit 1
fi

PATTERN='chat/completions'

# grep -rn over *.py, excluding the provider module. ``|| true`` so a
# clean tree (grep exit 1 = no matches) does not trip ``set -e``.
HITS=$(grep -rnF --include='*.py' "$PATTERN" "$SCAN_ROOT" \
  | grep -vF "$FACTORY_REL" \
  || true)

if [[ -n "$HITS" ]]; then
  echo "FAIL (#1919): OpenAI-compatible LLM call outside app/services/llm_client.py:" >&2
  printf '%s\n' "$HITS" >&2
  echo >&2
  echo "Route all LLM completions through app.services.llm_client.make_llm_clients(...)" >&2
  echo "so the bounded timeout + per-process semaphore (#1919 / #1479 class) are" >&2
  echo "applied — never a raw httpx call with a default timeout." >&2
  exit 1
fi

echo "==> check_llm_chokepoint: OK (all app/ LLM completions go through llm_client.py)"
