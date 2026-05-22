#!/usr/bin/env bash
#
# Issue #1233 — PR12: out-of-band orphan-reconciliation audit for
# ``ownership_refresh_state``.
# Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §3.3 + §9 DoD #12.
#
# Pins the write-through invariant that every production
# ``record_*_observation`` caller also fires ``refresh_*_current``
# (which UPSERTs into ``ownership_refresh_state``). On a healthy install
# this returns ``orphan_count_total=0``. Non-zero output indicates a
# write-through regression (an observation writer landed without firing
# the matching ``refresh_*_current``) and exits non-zero so the script
# is safe to wire into a separate cron later.
#
# Connects to the dev DB via ``docker exec ebull-postgres psql`` so it
# matches the rest of the operator runbook surface. Override env:
#   EBULL_PG_CONTAINER (default: ebull-postgres)
#   EBULL_PG_USER      (default: postgres)
#   EBULL_PG_DATABASE  (default: ebull)

set -euo pipefail

PG_CONTAINER="${EBULL_PG_CONTAINER:-ebull-postgres}"
PG_USER="${EBULL_PG_USER:-postgres}"
PG_DATABASE="${EBULL_PG_DATABASE:-ebull}"

# Per-category pairs: <category_literal>:<observations_table>. Order
# matches the 7-tuple in ``ownership_refresh_state`` CHECK constraint
# + ``app/jobs/ownership_observations_repair.py::_CATEGORIES`` so a
# reader can map output lines to source code 1:1.
CATEGORIES=(
    "insiders:ownership_insiders_observations"
    "institutions:ownership_institutions_observations"
    "blockholders:ownership_blockholders_observations"
    "treasury:ownership_treasury_observations"
    "def14a:ownership_def14a_observations"
    "funds:ownership_funds_observations"
    "esop:ownership_esop_observations"
)

orphan_count_total=0
for pair in "${CATEGORIES[@]}"; do
    category="${pair%%:*}"
    obs_table="${pair##*:}"
    count=$(docker exec "${PG_CONTAINER}" psql -w -U "${PG_USER}" -d "${PG_DATABASE}" -tAc "
        SELECT count(DISTINCT o.instrument_id)
        FROM ${obs_table} o
        WHERE o.instrument_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM ownership_refresh_state s
              WHERE s.instrument_id = o.instrument_id
                AND s.category = '${category}'
          )
    " | tr -d ' ')
    # Defensive: tolerate transient blip (empty / non-numeric) without
    # killing the loop under set -euo pipefail.
    if [[ ! "${count}" =~ ^[0-9]+$ ]]; then
        echo "WARN: category=${category} obs_table=${obs_table} probe returned non-numeric '${count}' — skipping" >&2
        continue
    fi
    echo "category=${category} obs_table=${obs_table} orphan_instruments=${count}"
    orphan_count_total=$((orphan_count_total + count))
done

echo "---"
echo "orphan_count_total=${orphan_count_total}"

if [[ "${orphan_count_total}" -gt 0 ]]; then
    cat >&2 <<'MSG'
FAIL: write-through invariant breach — at least one observation writer
landed without firing the matching refresh_*_current. Investigate the
per-category counts above. Likely causes:
  - new ingest path added record_<cat>_observation but skipped
    refresh_<cat>_current(instrument_id)
  - bootstrap re-ingest backfilled observations under an older code
    version that predated PR12's state-table UPSERT
  - manual SQL INSERT into ownership_*_observations bypassing the
    service-layer write-through helper
Re-run sql/163 backfill (idempotent ON CONFLICT DO NOTHING) to recover
state rows for in-place observations; or trigger refresh_<cat>_current
for the affected instrument_ids via POST /jobs/sec_rebuild/run.
MSG
    exit 1
fi

echo "OK: zero orphans across all 7 categories."
exit 0
