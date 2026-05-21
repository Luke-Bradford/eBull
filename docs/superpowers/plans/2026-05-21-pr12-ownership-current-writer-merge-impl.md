# PR12 ownership_*_current writer rewrite (MERGE + watermark side-table) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the DELETE+INSERT-per-instrument writer in all 7 `refresh_*_current` helpers with a single-statement PG17 diff-aware MERGE + introduce `ownership_refresh_state` side-table so drift-watermark semantics live outside `_current` row data, eliminating bloat-feeding rewrites and the post-MERGE repair-sweep forever-loop.

**Architecture:** Each helper becomes one `MERGE … WHEN MATCHED AND (...) IS DISTINCT FROM (...) THEN UPDATE … WHEN NOT MATCHED BY TARGET THEN INSERT … WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE` inside the existing `with conn.transaction()` + advisory-lock contract, followed by an UPSERT into the new `ownership_refresh_state` table carrying the per-instrument `MAX(observations.ingested_at)` watermark captured pre-MERGE (race-safe). Repair sweep predicate switches to an obs-anchored CTE aggregate (`WITH obs_max AS (SELECT instrument_id, MAX(ingested_at) FROM ownership_X_observations GROUP BY instrument_id) SELECT s.instrument_id FROM ownership_refresh_state s LEFT JOIN obs_max ON … WHERE s.last_drained_observations_max_ingested_at IS DISTINCT FROM obs_max.m`). `_CATEGORIES` list grows from 5 to 7 (adds `funds` + `esop`). PG ≥ 17 boot-time guard added at lifespan. 93-clause lint guard + 52 parametrised tests + out-of-band orphan-audit script pin the contract.

**Tech Stack:** Python 3.13, psycopg3, PG17.9 (MERGE WHEN NOT MATCHED BY SOURCE, pgstattuple, xmin probes), pytest+xdist+testmon, uv, ruff, pyright. Spec: `docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md` (commit `40e0220` on main).

**Branch:** `feature/1233-pr12-impl` (already checked out at plan start).

---

## File map

**Create:**
- `sql/163_ownership_refresh_state.sql` — new table + 2 indexes + 7 backfill blocks
- `scripts/check_ownership_refresh_writer_pattern.sh` — lint guard, 93 clause-counts
- `scripts/ownership_refresh_state_orphan_audit.sh` — operator-triggered orphan reconciliation
- `tests/test_ownership_refresh_writer_merge.py` — 52 parametrised cases
- `app/system/postgres_version_guard.py` — PG ≥ 17 boot-time assertion (new helper module)
- `tests/test_postgres_version_guard.py` — guard unit test

**Modify:**
- `app/services/ownership_observations.py` — rewrite 7 `refresh_*_current` helpers (~890 lines edited across function bodies)
- `app/jobs/ownership_observations_repair.py` — switch predicate to CTE aggregate + expand `_CATEGORIES` from 5 to 7
- `app/main.py` (or equivalent lifespan owner — verify path during Task 7) — wire `postgres_version_guard` into lifespan startup
- `tests/fixtures/ebull_test_db.py` — provision `pgstattuple` extension in template
- `tests/smoke/test_app_boots.py` — assert version guard fires (smoke gate)
- `.githooks/pre-push` — wire new lint script after `check_13dg_retention.sh`
- `docs/superpowers/specs/2026-05-19-data-retention-rubric.md` — parent spec amendment §4.5 / §4.6 / §7 / §8 (PR12 flipped to SHIPPED)
- `docs/review-prevention-log.md` — 2 new entries (MERGE clamp + diff-predicate watermark separation)
- `.claude/skills/data-engineer/SKILL.md` — write-through section gains diff-aware MERGE rule + watermark side-table pointer

**Read-only references (no edits — used for context):**
- Spec: `docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md`
- Existing helpers: `app/services/ownership_observations.py` lines 181-284, 390-448, 568-624, 689-739, 843-905, 1040-1101, 1194-1255
- Existing repair sweep: `app/jobs/ownership_observations_repair.py`
- Existing migrations: `sql/119_ownership_observations_ingested_at.sql`, `sql/114_ownership_institutions_observations.sql`, `sql/123_ownership_funds.sql`, `sql/127_ownership_esop.sql`, `sql/113_ownership_insiders_observations.sql`, `sql/115_ownership_blockholders_observations.sql`, `sql/116_ownership_treasury_def14a_observations.sql`

---

## Task 0: branch + plan commit + pre-flight

**Files:**
- Modify: none yet (sanity-check only)

- [ ] **Step 0.1: Verify branch state**

Run: `git status && git log --oneline -3`
Expected: on `feature/1233-pr12-impl`, working tree clean, HEAD reflects merged doc-PR `40e0220 docs(#1233): PR12 design spec`.

- [ ] **Step 0.2: Verify dev DB postgres version + state**

Run:
```bash
docker exec ebull-postgres psql -U postgres -d ebull -c "SELECT version()"
docker exec ebull-postgres psql -U postgres -d ebull -c "SELECT pg_size_pretty(pg_total_relation_size('ownership_funds_current'))"
```
Expected: PG 17.x; `ownership_funds_current` is 2.0+ GB (pre-fix baseline; will not shrink in this PR).

- [ ] **Step 0.3: Commit this plan**

```bash
git add docs/superpowers/plans/2026-05-21-pr12-ownership-current-writer-merge-impl.md
git commit -m "docs(#1233): PR12 implementation plan

Plan doc for PR12 — ownership_*_current writer rewrite. Follows the
shipped design spec (40e0220 / docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md).

Sequenced for subagent-driven dispatch:
- Task 1: sql/163 migration (table + 2 indexes + 7 backfill blocks)
- Task 2: pgstattuple provisioning in ebull_test
- Task 3-4: tests scaffold + first MERGE helper (funds)
- Task 5: remaining 6 helpers
- Task 6: repair-sweep predicate + _CATEGORIES expansion
- Task 7: PG >= 17 boot guard
- Task 8: lint guard
- Task 9: orphan-audit script
- Task 10: smoke + verification
- Task 11: docs amendment + prevention log + skill update
- Task 12: PR push + bot review + merge

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 1: sql/163 migration — table + indexes + backfill

**Files:**
- Create: `sql/163_ownership_refresh_state.sql`

- [ ] **Step 1.1: Write migration**

Create `sql/163_ownership_refresh_state.sql` with the following content (matches spec §3.3 verbatim — table schema, 2 indexes scoped to funds + esop only since sql/119 covers the other 5, and 7 backfill blocks aggregating per-instrument `MAX(c.refreshed_at)`):

```sql
-- 163_ownership_refresh_state.sql
--
-- Issue #1233 — PR12: separate drift watermark from `_current` row data so
-- diff-aware MERGE writers do not feed the repair-sweep forever-loop.
-- Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §3.3
--
-- Side-table holds one row per (instrument_id, category) carrying the
-- last `MAX(observations.ingested_at)` value the matching
-- `refresh_X_current` helper drained AND the wall-clock of the most
-- recent refresh attempt. Repair-sweep predicate JOINs this table to a
-- per-observations-table `MAX(ingested_at) GROUP BY instrument_id` CTE
-- aggregate; `IS DISTINCT FROM` is NULL-safe.
--
-- Backfill from `MAX(_current.refreshed_at) GROUP BY instrument_id`:
-- false-positive (first sweep refreshes more instruments than strictly
-- necessary due to tx-time vs clock_timestamp skew, each call is a
-- MERGE no-op) is benign; false-negative (state claiming reconciliation
-- that never happened) would mask real drift — unacceptable for the
-- safety-net job. See Codex 1d HIGH-1 in the spec for the trade-off
-- discussion.
--
-- Indexes scoped to funds + esop only — sql/119 already provisioned
-- `(instrument_id, ingested_at DESC)` for insiders / institutions /
-- blockholders / treasury / def14a. See Codex 1d MED-1.

BEGIN;

CREATE TABLE IF NOT EXISTS ownership_refresh_state (
    instrument_id                             BIGINT      NOT NULL,
    category                                  TEXT        NOT NULL CHECK (category IN (
        'insiders', 'institutions', 'blockholders', 'treasury', 'def14a', 'funds', 'esop'
    )),
    last_drained_observations_max_ingested_at TIMESTAMPTZ,
    last_refresh_attempted_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, category)
);

COMMENT ON TABLE ownership_refresh_state IS
    'Per-(instrument, category) drift watermark for the ownership repair sweep (#1233 PR12). Decoupled from _current.refreshed_at so diff-aware MERGE writers do not freeze the watermark on no-op refreshes.';

CREATE INDEX IF NOT EXISTS idx_ownership_refresh_state_category
    ON ownership_refresh_state (category, last_drained_observations_max_ingested_at);

CREATE INDEX IF NOT EXISTS idx_funds_obs_instrument_ingested
    ON ownership_funds_observations (instrument_id, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_esop_obs_instrument_ingested
    ON ownership_esop_observations (instrument_id, ingested_at DESC);

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'insiders', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_insiders_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'institutions', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_institutions_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'blockholders', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_blockholders_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'treasury', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_treasury_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'def14a', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_def14a_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'funds', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_funds_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'esop', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_esop_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

COMMIT;
```

- [ ] **Step 1.2: Apply migration via the runner (preferred) or manually**

Codex 1a MED-4: prefer the standard migration runner over manual psql so the migration goes through the same path production will use + `schema_migrations` is updated automatically. If the runner is invoked at lifespan start, the cleanest path is to restart the stack (or the API service):

```bash
# Preferred: trigger via lifespan restart of the API service.
# If your dev stack uses VS Code tasks, restart the relevant task.
# Otherwise run the runner CLI directly if one exists:
grep -rn "def run_migrations\|class MigrationRunner\|def apply_migrations" /Users/lukebradford/Dev/eBull/app/db 2>&1 | head -5
# If the runner has a Python entrypoint, invoke it; otherwise fall back to:
docker exec -i ebull-postgres psql -U postgres -d ebull < sql/163_ownership_refresh_state.sql
```
Expected: `BEGIN`, table + 3 index creations (or `NOTICE: relation … already exists, skipping` on re-run), 7 `INSERT 0 N` lines, `COMMIT`. No errors. If the runner applied it, `schema_migrations` already records `163_ownership_refresh_state.sql` (Step 1.5 below becomes a no-op verification).

If applying manually, the runner will detect the table is already present on next boot and (depending on runner shape) either skip or fail — see Step 1.5 for the explicit `schema_migrations` insert that prevents double-apply.

- [ ] **Step 1.3: Verify schema**

Run:
```bash
docker exec ebull-postgres psql -U postgres -d ebull -c "\d ownership_refresh_state"
docker exec ebull-postgres psql -U postgres -d ebull -c "SELECT category, count(*), pg_size_pretty(SUM(pg_column_size(t))) FROM ownership_refresh_state t GROUP BY category ORDER BY category"
```
Expected: PK `(instrument_id, category)`, CHECK with 7 category literals, 3 indexes; per-category counts ≤ |distinct-instrument-ids-with-_current-rows| per category.

- [ ] **Step 1.4: Verify backfill from `_current.refreshed_at`**

Codex 1a MED-3: backfill verification must aggregate `_current` per instrument (multi-row categories like funds have many `_current` rows per instrument with potentially different `refreshed_at` values; a plain JOIN can compare against any single row's value, not the MAX). Use grouped CTE:

```bash
docker exec ebull-postgres psql -U postgres -d ebull -c "
WITH expected AS (
    SELECT instrument_id, MAX(refreshed_at) AS expected_watermark
    FROM ownership_funds_current
    GROUP BY instrument_id
)
SELECT
    s.instrument_id,
    s.last_drained_observations_max_ingested_at AS stored,
    expected.expected_watermark AS expected,
    s.last_drained_observations_max_ingested_at IS NOT DISTINCT FROM expected.expected_watermark AS ok
FROM ownership_refresh_state s
JOIN expected ON expected.instrument_id = s.instrument_id
WHERE s.category = 'funds'
LIMIT 5"
```
Expected: `ok = t` for every returned row; if any `ok = f`, the backfill failed for that instrument — investigate before proceeding.

- [ ] **Step 1.5: Register migration in `schema_migrations`**

Run (mirrors how the migration runner records applied migrations — verify the table name + INSERT shape against an existing `INSERT INTO schema_migrations` in the repo):
```bash
grep -rn "schema_migrations" /Users/lukebradford/Dev/eBull/app/db /Users/lukebradford/Dev/eBull/app/main.py 2>&1 | head -5
```
If the runner records by basename (`'163_ownership_refresh_state.sql'`), insert it manually (this matches the runner's at-boot behaviour but lets us proceed without restarting the stack):
```bash
docker exec ebull-postgres psql -U postgres -d ebull -c "INSERT INTO schema_migrations(filename) VALUES ('163_ownership_refresh_state.sql') ON CONFLICT DO NOTHING"
```
Adjust the column name if the table uses something other than `filename` (verify via `\d schema_migrations`).
Expected: `INSERT 0 1` or `INSERT 0 0` (already present).

- [ ] **Step 1.6: Commit**

```bash
git add sql/163_ownership_refresh_state.sql
git commit -m "feat(#1233): sql/163 ownership_refresh_state side-table + funds/esop ingested_at indexes

Spec §3.3. Backfill from MAX(_current.refreshed_at) GROUP BY instrument_id
per category (false-positive over false-negative per Codex 1d HIGH-1).
Indexes scoped to funds + esop only — sql/119 already covers 5 of 7.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: ebull_test template — pgstattuple extension + state-table truncate

**Files:**
- Modify: `tests/fixtures/ebull_test_db.py`

- [ ] **Step 2.1: Locate the template-provisioning function + `_PLANNER_TABLES`**

Run: `grep -n "CREATE EXTENSION\|_PLANNER_TABLES\|template" /Users/lukebradford/Dev/eBull/tests/fixtures/ebull_test_db.py | head -20`
Expected: existing `CREATE EXTENSION IF NOT EXISTS …` calls (e.g. for pg_trgm) — gives the pattern + the exact insertion site. Also locates `_PLANNER_TABLES` tuple (Codex 1a HIGH-3 — `ownership_refresh_state` has no FK cascade from `instruments`, so per-test TRUNCATE must include it explicitly or state rows leak between tests).

- [ ] **Step 2.2: Add pgstattuple provisioning**

In `tests/fixtures/ebull_test_db.py`, add `CREATE EXTENSION IF NOT EXISTS pgstattuple` immediately after the existing `CREATE EXTENSION` block in the template-provisioning function. Use the same cursor + same connection.

- [ ] **Step 2.3: Add `ownership_refresh_state` to `_PLANNER_TABLES`**

In `tests/fixtures/ebull_test_db.py`, find the `_PLANNER_TABLES: tuple[str, ...] = (...)` declaration and add `"ownership_refresh_state",` in alphabetical order (or wherever the existing pattern places child-to-parent ordering — Codex 1a HIGH-3: state-table rows leak between tests without this because there is no FK cascade from `instruments`). The TRUNCATE block uses `RESTART IDENTITY CASCADE` so order is not load-bearing, but follow the existing convention.

- [ ] **Step 2.4: Re-create the test template + smoke**

Run (drops the template DB if present + re-provisions on next test):
```bash
docker exec ebull-postgres psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS ebull_test_template" 2>&1 | head -5
uv run pytest tests/fixtures -x -q 2>&1 | tail -10
```
Expected: pytest provisions a fresh template with `pgstattuple` extension; no errors.

- [ ] **Step 2.5: Verify extension + truncate-list entry**

Run:
```bash
docker exec ebull-postgres psql -U postgres -d ebull_test_template -c "SELECT extname FROM pg_extension WHERE extname = 'pgstattuple'"
grep -n "ownership_refresh_state" /Users/lukebradford/Dev/eBull/tests/fixtures/ebull_test_db.py
```
Expected: 1 row with `pgstattuple`; grep returns the new entry in `_PLANNER_TABLES`.

- [ ] **Step 2.6: Commit**

```bash
git add tests/fixtures/ebull_test_db.py
git commit -m "test(#1233): provision pgstattuple + truncate ownership_refresh_state per test

Required by PR12 test case 2 (no-op churn) for authoritative table_len
+ dead_tuple_count measurements. Spec §6 + §9 DoD #6 — failure to
provision must trigger pytest.fail in the test (no silent skip).

ownership_refresh_state added to _PLANNER_TABLES (Codex 1a HIGH-3 — no
FK cascade from instruments, so state rows leak between tests
without explicit TRUNCATE).

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: tests/test_ownership_refresh_writer_merge.py — scaffold + funds-only baseline

**Files:**
- Create: `tests/test_ownership_refresh_writer_merge.py`

**Strategy:** TDD-first. Write the test module with all 7 helpers + 9 cases (per spec §6) parametrised, BUT initially only the funds helper exists with the new MERGE shape (Task 4). The remaining 6 helpers will be migrated in Task 5; their tests fail until then. This is the failing-test scaffold.

- [ ] **Step 3.1: Read existing test patterns**

Run: `ls tests/test_ownership_observations*.py && head -50 tests/test_ownership_observations_repair.py`
Expected: existing observation-helper tests use `conn` fixture from `tests/fixtures/ebull_test_db.py` + per-test instrument seeding.

- [ ] **Step 3.2: Write the test module scaffold**

Create `tests/test_ownership_refresh_writer_merge.py` with the following content (52 parametrised cases per spec §6). Use `pytest.fixture` for per-helper observation/_current setup. Helper-specific filters tested in case 7 use the per-helper SQL filters from spec §4.1. Test 6 (priority chain) applies to insiders only — parametrise with `@pytest.mark.parametrize("helper", [...])` and `@pytest.mark.skipif(helper != 'insiders', ...)`. Tests 7a/7b apply to treasury/def14a only.

```python
"""PR12 writer-rewrite contract tests (#1233).

Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md
§6 (52 parametrised cases across 7 helpers + helper-specific overlays).

Pinned invariants per case:

* insert (1):           one obs → one row, fresh xmin
* no-op churn (2):      LOAD-BEARING. xmin stable + pgstattuple
                        table_len unchanged + dead_tuple delta 0;
                        state-table tuple_count delta 0, dead_tuple
                        delta <= 1. refreshed_at unchanged.
* update / amendment (3): xmin changes; refreshed_at advances.
* delete / known_to (4): MERGE NOT MATCHED BY SOURCE → DELETE.
* scope clamp (5):      A's refresh leaves B's xmin stable. Pins
                        the literal `tgt.instrument_id = %(iid)s`
                        clamp in ON + DELETE clauses.
* priority chain (6):   INSIDERS only — Form 4 wins over 13d.
* per-helper filter (7): TREASURY null guard + DEF14A 3-clause ESOP
                        exclusion (regex + holder_role + shares NOT
                        NULL).
* repair-sweep no-loop (8): same-obs UPSERT bumps ingested_at; refresh
                        no-op; _drifted_instruments returns empty.
* known_to expiry watermark (9): expire active obs (SET known_to +
                        ingested_at = clock_timestamp() explicitly,
                        Codex 1d MED-2); refresh deletes _current
                        row + advances state watermark; sweep empty.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo

# Per-helper test contract.
@dataclass(frozen=True)
class HelperCase:
    name: str                              # 'funds', 'institutions', ...
    refresh_fn: Callable[[psycopg.Connection[Any], int], int]
    current_table: str
    observations_table: str
    category_literal: str
    has_priority_chain: bool               # only insiders
    has_per_helper_filter: bool            # treasury + def14a

ALL_HELPERS: list[HelperCase] = [
    HelperCase('insiders',
               lambda c, i: oo.refresh_insiders_current(c, instrument_id=i),
               'ownership_insiders_current',
               'ownership_insiders_observations',
               'insiders', True, False),
    HelperCase('institutions',
               lambda c, i: oo.refresh_institutions_current(c, instrument_id=i),
               'ownership_institutions_current',
               'ownership_institutions_observations',
               'institutions', False, False),
    HelperCase('blockholders',
               lambda c, i: oo.refresh_blockholders_current(c, instrument_id=i),
               'ownership_blockholders_current',
               'ownership_blockholders_observations',
               'blockholders', False, False),
    HelperCase('treasury',
               lambda c, i: oo.refresh_treasury_current(c, instrument_id=i),
               'ownership_treasury_current',
               'ownership_treasury_observations',
               'treasury', False, True),
    HelperCase('def14a',
               lambda c, i: oo.refresh_def14a_current(c, instrument_id=i),
               'ownership_def14a_current',
               'ownership_def14a_observations',
               'def14a', False, True),
    HelperCase('funds',
               lambda c, i: oo.refresh_funds_current(c, instrument_id=i),
               'ownership_funds_current',
               'ownership_funds_observations',
               'funds', False, False),
    HelperCase('esop',
               lambda c, i: oo.refresh_esop_current(c, instrument_id=i),
               'ownership_esop_current',
               'ownership_esop_observations',
               'esop', False, False),
]


@pytest.fixture
def conn(ebull_test_conn):
    """Reuse the existing per-worker test DB connection fixture
    (`ebull_test_conn` from `tests/fixtures/ebull_test_db.py`)."""
    return ebull_test_conn


def _pgstattuple(conn, table: str) -> dict[str, int]:
    """Return pgstattuple measurements; fail loud on missing extension.

    Uses `%s::regclass` cast so the table-name parameter resolves to a
    regclass OID exactly as pgstattuple expects (text-parameter form
    can fail function resolution under some psycopg modes)."""
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT * FROM pgstattuple(%s::regclass)", (table,))
        except psycopg.errors.UndefinedFunction:
            pytest.fail(
                f"pgstattuple extension missing in test DB — provisioning "
                f"bug, do NOT skip (spec §6 CI-fail-loud contract)."
            )
        row = cur.fetchone()
        cols = [d.name for d in cur.description]
        return dict(zip(cols, row))


def _xmin_text_for_instrument(conn, current_table: str, instrument_id: int) -> list[str]:
    """Return per-row xmin::text for an instrument (deterministic order)."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT xmin::text FROM {current_table} WHERE instrument_id = %s ORDER BY 1",
            (instrument_id,),
        )
        return [r[0] for r in cur.fetchall()]


def _seed_one_observation(conn, helper: HelperCase, instrument_id: int, *, fixture_idx: int = 0) -> str:
    """Insert one observation appropriate to the helper's natural key.
    Returns the source_document_id used (for test setup chaining)."""
    # Map per-helper to the matching record_*_observation call signature.
    # This routes through the production writer so DO UPDATE / ingested_at
    # semantics are exercised exactly as production.
    run_id = uuid4()
    doc_id = f"PR12-{helper.name}-{instrument_id}-{fixture_idx}"
    filed = datetime(2025, 1, 1 + fixture_idx, tzinfo=timezone.utc)
    period_end = date(2024, 12, 31)
    if helper.name == 'insiders':
        # holder_identity_key is a schema-generated column (not a param).
        # Verified against app/services/ownership_observations.py:110-127.
        oo.record_insider_observation(conn,
            instrument_id=instrument_id, holder_cik='0000000001',
            holder_name='Test Holder',
            ownership_nature='direct', source='form4',
            source_document_id=doc_id, source_accession=None,
            source_field=None, source_url=None,
            filed_at=filed, period_start=None, period_end=period_end,
            ingest_run_id=run_id, shares=Decimal('100'))
    elif helper.name == 'institutions':
        oo.record_institution_observation(conn,
            instrument_id=instrument_id, filer_cik='0000000002',
            filer_name='Test Filer', filer_type='ETF',
            ownership_nature='economic', source='13f',
            source_document_id=doc_id, source_accession=None,
            source_field=None, source_url=None,
            filed_at=filed, period_start=None, period_end=period_end,
            ingest_run_id=run_id, shares=Decimal('1000'),
            market_value_usd=Decimal('50000'), voting_authority='SOLE',
            exposure_kind='EQUITY')
    elif helper.name == 'blockholders':
        oo.record_blockholder_observation(conn,
            instrument_id=instrument_id, reporter_cik='0000000003',
            reporter_name='Test Reporter', ownership_nature='beneficial',
            submission_type='SC 13G', status_flag=None, source='13g',
            source_document_id=doc_id, source_accession=None,
            source_field=None, source_url=None,
            filed_at=filed, period_start=None, period_end=period_end,
            ingest_run_id=run_id, aggregate_amount_owned=Decimal('200'),
            percent_of_class=Decimal('5.25'))
    elif helper.name == 'treasury':
        oo.record_treasury_observation(conn,
            instrument_id=instrument_id, source='xbrl_dei',
            source_document_id=doc_id, source_accession=None,
            source_field=None, source_url=None,
            filed_at=filed, period_start=None, period_end=period_end,
            ingest_run_id=run_id, treasury_shares=Decimal('300'))
    elif helper.name == 'def14a':
        oo.record_def14a_observation(conn,
            instrument_id=instrument_id, holder_name='Vanguard Group',
            holder_role='principal', ownership_nature='beneficial',
            source='def14a', source_document_id=doc_id,
            source_accession=None, source_field=None, source_url=None,
            filed_at=filed, period_start=None, period_end=period_end,
            ingest_run_id=run_id, shares=Decimal('400'),
            percent_of_class=Decimal('3.5'))
    elif helper.name == 'funds':
        # ownership_nature + source are fixed by schema CHECK constraints
        # and NOT accepted as params (app/services/ownership_observations.py:913-933).
        oo.record_fund_observation(conn,
            instrument_id=instrument_id, fund_series_id='S000000001',
            fund_series_name='Test Fund', fund_filer_cik='0000000004',
            source_document_id=doc_id, source_accession=None,
            source_field=None, source_url=None,
            filed_at=filed, period_start=None, period_end=period_end,
            ingest_run_id=run_id, shares=Decimal('500'),
            market_value_usd=Decimal('25000'), payoff_profile='Long',
            asset_category='EC')
    elif helper.name == 'esop':
        # No ownership_nature / source params — same pattern as funds.
        oo.record_esop_observation(conn,
            instrument_id=instrument_id, plan_name='Test ESOP',
            plan_trustee_name='Fidelity', plan_trustee_cik='0000000005',
            source_document_id=doc_id, source_accession=None,
            source_field=None, source_url=None,
            filed_at=filed, period_start=None, period_end=period_end,
            ingest_run_id=run_id, shares=Decimal('600'),
            percent_of_class=Decimal('2.1'))
    else:
        pytest.fail(f"unknown helper: {helper.name}")
    conn.commit()
    return doc_id


# ----------------------------------------------------------------------
# Case 1: insert
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_insert(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id)
    pre_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    post_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    assert len(post_xmin) >= 1
    assert post_xmin != pre_xmin  # row was created


# ----------------------------------------------------------------------
# Case 2: no-op churn (load-bearing)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_no_op_churn(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    pre_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    pre_current_stat = _pgstattuple(conn, helper.current_table)
    pre_state_stat = _pgstattuple(conn, 'ownership_refresh_state')
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        pre_refreshed = cur.fetchall()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    post_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    post_current_stat = _pgstattuple(conn, helper.current_table)
    post_state_stat = _pgstattuple(conn, 'ownership_refresh_state')
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        post_refreshed = cur.fetchall()
    assert post_xmin == pre_xmin, "no-op refresh rewrote rows"
    assert post_current_stat['table_len'] == pre_current_stat['table_len']
    assert post_current_stat['dead_tuple_count'] - pre_current_stat['dead_tuple_count'] == 0
    assert post_refreshed == pre_refreshed, "refreshed_at advanced on no-op"
    state_dead_delta = post_state_stat['dead_tuple_count'] - pre_state_stat['dead_tuple_count']
    assert state_dead_delta <= 1, f"state-table churn > 1 dead tuple: {state_dead_delta}"
    state_live_delta = post_state_stat['tuple_count'] - pre_state_stat['tuple_count']
    assert state_live_delta == 0, "state-table row count grew on no-op refresh"


# ----------------------------------------------------------------------
# Case 3: update (amendment)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_update_amendment(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id, fixture_idx=0)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    pre_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        pre_refreshed = cur.fetchall()
    # Second obs with later filed_at + different shares — same natural key.
    _seed_one_observation(conn, helper, seeded_instrument_id, fixture_idx=1)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    post_xmin = _xmin_text_for_instrument(conn, helper.current_table, seeded_instrument_id)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT refreshed_at FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        post_refreshed = cur.fetchall()
    assert post_xmin != pre_xmin
    assert post_refreshed > pre_refreshed


# ----------------------------------------------------------------------
# Case 4: delete (known_to expiry → MERGE NOT MATCHED BY SOURCE → DELETE)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_delete_via_known_to(conn, seeded_instrument_id, helper):
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {helper.observations_table} "
            f"SET known_to = now(), ingested_at = clock_timestamp() "
            f"WHERE instrument_id = %s AND known_to IS NULL",
            (seeded_instrument_id,),
        )
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {helper.current_table} WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        assert cur.fetchone()[0] == 0, "MERGE NOT MATCHED BY SOURCE did not DELETE"


# ----------------------------------------------------------------------
# Case 5: scope clamp (other-instrument xmin stable)
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_scope_clamp_other_instrument_untouched(conn, two_seeded_instrument_ids, helper):
    a, b = two_seeded_instrument_ids
    _seed_one_observation(conn, helper, a)
    _seed_one_observation(conn, helper, b)
    helper.refresh_fn(conn, a)
    helper.refresh_fn(conn, b)
    conn.commit()
    pre_b_xmin = _xmin_text_for_instrument(conn, helper.current_table, b)
    helper.refresh_fn(conn, a)
    conn.commit()
    post_b_xmin = _xmin_text_for_instrument(conn, helper.current_table, b)
    assert post_b_xmin == pre_b_xmin, "scope clamp leaked — other instrument rewritten"


# ----------------------------------------------------------------------
# Case 6: insiders priority chain (Form 4 wins over 13d)
# ----------------------------------------------------------------------
def test_insiders_priority_chain(conn, seeded_instrument_id):
    helper = next(h for h in ALL_HELPERS if h.name == 'insiders')
    run_id = uuid4()
    period = date(2024, 12, 31)
    # Two observations same (holder_cik, ownership_nature) — the schema
    # generates holder_identity_key from holder_cik (NULL-safe) — but
    # different source priority. Form 4 (priority 1) must win.
    oo.record_insider_observation(conn,
        instrument_id=seeded_instrument_id, holder_cik='0000000007',
        holder_name='Insider X',
        ownership_nature='direct', source='13d',  # priority 3
        source_document_id='13d-doc', source_accession=None,
        source_field=None, source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        period_start=None, period_end=period, ingest_run_id=run_id,
        shares=Decimal('100'))
    oo.record_insider_observation(conn,
        instrument_id=seeded_instrument_id, holder_cik='0000000007',
        holder_name='Insider X',
        ownership_nature='direct', source='form4',  # priority 1
        source_document_id='form4-doc', source_accession=None,
        source_field=None, source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        period_start=None, period_end=period, ingest_run_id=run_id,
        shares=Decimal('200'))
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT source, shares FROM {helper.current_table} "
            f"WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        row = cur.fetchone()
    assert row[0] == 'form4', f"priority chain broken: got source={row[0]!r}"
    assert row[1] == Decimal('200'), f"wrong row picked: shares={row[1]}"


# ----------------------------------------------------------------------
# Case 7a: treasury null-displacement guard
# ----------------------------------------------------------------------
def test_treasury_null_guard(conn, seeded_instrument_id):
    helper = next(h for h in ALL_HELPERS if h.name == 'treasury')
    run_id = uuid4()
    period = date(2024, 12, 31)
    # Null observation arrives first, non-null second. Null must not displace.
    oo.record_treasury_observation(conn,
        instrument_id=seeded_instrument_id, source='xbrl_dei',
        source_document_id='null-doc', source_accession=None,
        source_field=None, source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        period_start=None, period_end=period, ingest_run_id=run_id,
        treasury_shares=None)
    oo.record_treasury_observation(conn,
        instrument_id=seeded_instrument_id, source='xbrl_dei',
        source_document_id='good-doc', source_accession=None,
        source_field=None, source_url=None,
        filed_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        period_start=None, period_end=period, ingest_run_id=run_id,
        treasury_shares=Decimal('12345'))
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT treasury_shares FROM ownership_treasury_current WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        assert cur.fetchone()[0] == Decimal('12345')


# ----------------------------------------------------------------------
# Case 7b: def14a ESOP 3-clause filter (holder_role + name regex + shares)
# ----------------------------------------------------------------------
def test_def14a_esop_exclusion(conn, seeded_instrument_id):
    helper = next(h for h in ALL_HELPERS if h.name == 'def14a')
    run_id = uuid4()
    period = date(2024, 12, 31)
    filed = datetime(2025, 1, 1, tzinfo=timezone.utc)
    # (a) holder_role='esop' — excluded.
    oo.record_def14a_observation(conn,
        instrument_id=seeded_instrument_id, holder_name='Acme ESOP Trust',
        holder_role='esop', ownership_nature='beneficial',
        source='def14a', source_document_id='esop-role-doc',
        source_accession=None, source_field=None, source_url=None,
        filed_at=filed, period_start=None, period_end=period,
        ingest_run_id=run_id, shares=Decimal('100'),
        percent_of_class=Decimal('1.0'))
    # (b) holder_role='principal' but name matches ESOP regex — excluded.
    oo.record_def14a_observation(conn,
        instrument_id=seeded_instrument_id,
        holder_name='Acme Employee Stock Ownership Plan',
        holder_role='principal', ownership_nature='beneficial',
        source='def14a', source_document_id='esop-name-doc',
        source_accession=None, source_field=None, source_url=None,
        filed_at=filed, period_start=None, period_end=period,
        ingest_run_id=run_id, shares=Decimal('100'),
        percent_of_class=Decimal('1.0'))
    # (c) holder_role='principal', name benign — included.
    oo.record_def14a_observation(conn,
        instrument_id=seeded_instrument_id, holder_name='Vanguard Group',
        holder_role='principal', ownership_nature='beneficial',
        source='def14a', source_document_id='vanguard-doc',
        source_accession=None, source_field=None, source_url=None,
        filed_at=filed, period_start=None, period_end=period,
        ingest_run_id=run_id, shares=Decimal('500'),
        percent_of_class=Decimal('3.5'))
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT holder_name FROM ownership_def14a_current "
            "WHERE instrument_id = %s",
            (seeded_instrument_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 'Vanguard Group'


# ----------------------------------------------------------------------
# Case 8: repair-sweep no-loop
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_repair_sweep_no_loop(conn, seeded_instrument_id, helper):
    from app.jobs.ownership_observations_repair import _drifted_instruments
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    # Re-UPSERT the same obs — DO UPDATE bumps ingested_at via clock_timestamp().
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    drifted = _drifted_instruments(
        conn, helper.current_table, helper.observations_table, helper.category_literal
    )
    assert seeded_instrument_id not in drifted, (
        f"repair sweep would re-select {helper.name} instrument forever "
        f"despite no-op MERGE"
    )


# ----------------------------------------------------------------------
# Case 9: known_to expiry watermark alignment
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", ALL_HELPERS, ids=lambda h: h.name)
def test_known_to_expiry_watermark_alignment(conn, seeded_instrument_id, helper):
    from app.jobs.ownership_observations_repair import _drifted_instruments
    _seed_one_observation(conn, helper, seeded_instrument_id)
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    # Explicit `ingested_at = clock_timestamp()` bump alongside known_to
    # mirrors the production ingest path's DO UPDATE clause (Codex 1d MED-2).
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {helper.observations_table} "
            f"SET known_to = now(), ingested_at = clock_timestamp() "
            f"WHERE instrument_id = %s AND known_to IS NULL",
            (seeded_instrument_id,),
        )
    conn.commit()
    helper.refresh_fn(conn, seeded_instrument_id)
    conn.commit()
    drifted = _drifted_instruments(
        conn, helper.current_table, helper.observations_table, helper.category_literal
    )
    assert seeded_instrument_id not in drifted
```

- [ ] **Step 3.3: Add the missing fixtures**

Inspect `tests/conftest.py` + `tests/fixtures/ebull_test_db.py` for the `ebull_test_conn` connection fixture (verified to exist) + the `seeded_instrument_id` / `two_seeded_instrument_ids` patterns. If the seeded-instrument fixtures do not yet exist, add them to `tests/conftest.py` (single-line factory each — produce a fresh `instruments` row with a unique `etoro_id` per test). Run `grep -n "seeded_instrument_id\|ebull_test_conn\|two_seeded_instrument_ids" tests/conftest.py tests/fixtures/*.py 2>&1 | head -20` to confirm location.

If fixtures need to be added, follow the pattern of the existing per-test instrument-seeding helpers in the test suite. Each fixture should insert a single fresh instrument row + return its `instrument_id`.

- [ ] **Step 3.4: Run the test module — expect failures**

Run: `uv run pytest tests/test_ownership_refresh_writer_merge.py -q --no-cov 2>&1 | tail -20`
Expected: tests fail. They will fail because (a) the helpers still use DELETE+INSERT (case 2 will fail on xmin stability) and (b) `_drifted_instruments` signature in the repair job needs the new third `category_literal` parameter (cases 8 + 9). This is the **failing-test scaffold**; Tasks 4-6 turn them green.

- [ ] **Step 3.5: Commit failing scaffold**

```bash
git add tests/test_ownership_refresh_writer_merge.py tests/conftest.py tests/fixtures/ebull_test_db.py
git commit -m "test(#1233): PR12 failing-test scaffold (52 parametrised cases)

Adds 7 helpers × 5 base cases + insiders priority chain + treasury null
guard + def14a ESOP exclusion + 7 repair-sweep no-loop + 7 known_to
expiry watermark alignment = 52 cases per spec §6.

Tests fail intentionally — Task 4 rewrites refresh_funds_current to
the MERGE shape; Task 5 the other 6 helpers; Task 6 the repair-sweep
predicate. Each step in the impl plan flips a subset green.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: refresh_funds_current — first MERGE helper

**Files:**
- Modify: `app/services/ownership_observations.py` lines 1040-1101

**Rationale:** funds is the load-bearing helper for the bloat fix (10.22% tuple density today). Rewriting funds first proves the template; Task 5 mechanically replicates across the other 6 helpers.

- [ ] **Step 4.1: Replace `refresh_funds_current` with the MERGE template**

Replace the existing function body (lines 1040-1101 in `app/services/ownership_observations.py`) with the §4 spec template verbatim. Capture watermark pre-MERGE in a Python var, then MERGE, then state UPSERT with the captured watermark. Use `psycopg.sql` literals for table/column names embedded inside the SQL string (no string concatenation).

```python
def refresh_funds_current(conn: psycopg.Connection[Any], *, instrument_id: int) -> int:
    """Diff-aware MERGE reconciler for ``ownership_funds_current``.

    Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §4.

    UPDATE only when business cols IS DISTINCT FROM the new set; INSERT
    new rows; DELETE rows that fall out of the latest set (NOT MATCHED
    BY SOURCE scope-clamped to this instrument via the ON clause AND
    the DELETE clause for defence-in-depth). ``refreshed_at`` is
    advanced on the UPDATE path only; the operator-visible drift
    watermark for repair-sweep lives in ``ownership_refresh_state``
    (§3.3 — separates write-side dead-tuple budget from watermark
    semantics so no-op refreshes do not trigger forever-loops in
    the repair sweep).

    Watermark captured pre-MERGE in a Python var so the state UPSERT
    cannot advance past observations the MERGE did not see (Codex 1b
    HIGH-2 race fix)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_funds_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        cur.execute(
            "SELECT MAX(ingested_at) FROM ownership_funds_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        wm_row = cur.fetchone()
        watermark = wm_row[0] if wm_row else None
        cur.execute(
            """
            MERGE INTO ownership_funds_current AS tgt
            USING (
                SELECT DISTINCT ON (fund_series_id)
                    instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                    ownership_nature,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end,
                    shares, market_value_usd, payoff_profile, asset_category
                FROM ownership_funds_observations
                WHERE instrument_id = %(iid)s AND known_to IS NULL
                ORDER BY
                    fund_series_id,
                    filed_at DESC,
                    period_end DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.fund_series_id = src.fund_series_id
            WHEN MATCHED AND (
                tgt.fund_series_name, tgt.fund_filer_cik, tgt.ownership_nature,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.shares, tgt.market_value_usd, tgt.payoff_profile, tgt.asset_category
            ) IS DISTINCT FROM (
                src.fund_series_name, src.fund_filer_cik, src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.payoff_profile, src.asset_category
            ) THEN UPDATE SET
                fund_series_name   = src.fund_series_name,
                fund_filer_cik     = src.fund_filer_cik,
                ownership_nature   = src.ownership_nature,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                shares             = src.shares,
                market_value_usd   = src.market_value_usd,
                payoff_profile     = src.payoff_profile,
                asset_category     = src.asset_category,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                ownership_nature, source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, market_value_usd, payoff_profile, asset_category
            ) VALUES (
                src.instrument_id, src.fund_series_id, src.fund_series_name, src.fund_filer_cik,
                src.ownership_nature, src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.payoff_profile, src.asset_category
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (%(iid)s, 'funds', %(watermark)s, now())
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_funds_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
```

- [ ] **Step 4.2: Run the funds tests**

Run: `uv run pytest tests/test_ownership_refresh_writer_merge.py -k funds -v --no-cov 2>&1 | tail -25`
Expected: cases 1-5 + 7 (funds-applicable) PASS. Cases 8 + 9 still fail because the repair-sweep predicate hasn't been switched yet (Task 6) and `_drifted_instruments` signature mismatch.

- [ ] **Step 4.3: Run typecheck**

Run: `uv run pyright app/services/ownership_observations.py 2>&1 | tail -10`
Expected: 0 errors.

- [ ] **Step 4.4: Commit funds rewrite**

```bash
git add app/services/ownership_observations.py
git commit -m "feat(#1233): refresh_funds_current → diff-aware MERGE + state UPSERT

First MERGE helper in PR12. Spec §4 template applied verbatim.
Watermark captured pre-MERGE in Python var (Codex 1b HIGH-2 race fix).
Funds tests 1-5 + 7 green; tests 8-9 await Task 6 repair-sweep switch.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: rewrite remaining 6 helpers (insiders, institutions, blockholders, treasury, def14a, esop)

**Files:**
- Modify: `app/services/ownership_observations.py` lines 181-284 (insiders), 390-448 (institutions), 568-624 (blockholders), 689-739 (treasury), 843-905 (def14a), 1194-1255 (esop)

**Strategy:** apply the same template as Task 4, with per-helper differences from spec §4.1:

| Helper | Differences from funds template |
| --- | --- |
| `refresh_insiders_current` | PK is `(instrument_id, holder_identity_key, ownership_nature)`. ON clause: `tgt.instrument_id = %(iid)s AND tgt.holder_identity_key = src.holder_identity_key AND tgt.ownership_nature = src.ownership_nature`. DISTINCT ON `(holder_identity_key, ownership_nature)`; ORDER BY keeps the source-priority `CASE source WHEN 'form4' THEN 1 ... END ASC` chain verbatim from existing code lines 256-274. Diff tuple = NON-PK + NON-refreshed_at cols: `(holder_cik, holder_name, source, source_document_id, source_accession, source_url, filed_at, period_start, period_end, shares)`. `holder_identity_key` is the schema-generated PK column — appears in ON + INSERT cols + DISTINCT ON, NEVER in diff tuple or UPDATE SET (Codex 1b plan-rev2 HIGH-1: PK cols ∩ diff cols = ∅ per spec §4.2). Category literal `'insiders'`. |
| `refresh_institutions_current` | DISTINCT ON `(filer_cik, ownership_nature, exposure_kind)`. Business cols `(filer_name, filer_type, source, source_document_id, source_accession, source_url, filed_at, period_start, period_end, shares, market_value_usd, voting_authority)` (exposure_kind is PK). Category `'institutions'`. |
| `refresh_blockholders_current` | DISTINCT ON `(reporter_cik, ownership_nature)`. Business cols `(reporter_name, submission_type, status_flag, source, source_document_id, source_accession, source_url, filed_at, period_start, period_end, aggregate_amount_owned, percent_of_class)`. Category `'blockholders'`. |
| `refresh_treasury_current` | DISTINCT ON `(instrument_id)`. **Extra WHERE filter** `AND treasury_shares IS NOT NULL`. Business cols `(ownership_nature, source, source_document_id, source_accession, source_url, filed_at, period_start, period_end, treasury_shares)`. Category `'treasury'`. PK is `(instrument_id)` only → ON clause is just `tgt.instrument_id = %(iid)s`. |
| `refresh_def14a_current` | PK is `(instrument_id, holder_name_key, ownership_nature)`. ON clause: `tgt.instrument_id = %(iid)s AND tgt.holder_name_key = src.holder_name_key AND tgt.ownership_nature = src.ownership_nature`. DISTINCT ON `(holder_name_key, ownership_nature)`. **Extra WHERE filter (3 clauses, all bound as named placeholders so the MERGE statement remains all-named-parameter — Codex 1b plan-rev2 MED-2)**: `AND shares IS NOT NULL AND holder_role IS DISTINCT FROM 'esop' AND holder_name !~* %(esop_regex)s` — bind the existing module-level `_ESOP_HOLDER_NAME_SQL_REGEX` constant. Pass via `{"iid": instrument_id, "esop_regex": _ESOP_HOLDER_NAME_SQL_REGEX}`. Diff tuple = NON-PK + NON-refreshed_at + NON-generated cols: `(holder_name, holder_role, source, source_document_id, source_accession, source_url, filed_at, period_start, period_end, shares, percent_of_class)`. `holder_name_key` is the schema-generated PK column (NOT in diff tuple, NOT in UPDATE SET — Codex 1b plan-rev2 HIGH-1). Lint K3 must accept the named placeholder `%(esop_regex)s` (not bare `%s`) — see Step 8.2 config update below. Category `'def14a'`. Watermark capture uses the full observations population (`SELECT MAX(ingested_at) FROM ownership_def14a_observations WHERE instrument_id = %s`); the ESOP filter applies only to the MERGE source subquery, NOT to the watermark statement. |
| `refresh_esop_current` | DISTINCT ON `(plan_name)`. Business cols `(plan_trustee_name, plan_trustee_cik, ownership_nature, source, source_document_id, source_accession, source_url, filed_at, period_start, period_end, shares, percent_of_class)`. Category `'esop'`. |

- [ ] **Step 5.1: Rewrite `refresh_insiders_current`**

Apply MERGE template + per-helper differences above. Preserve the source-priority `CASE` chain inside the USING subquery's ORDER BY. PR diff for this function should be a clean replacement of lines 181-284 (function-body span).

- [ ] **Step 5.2: Rewrite `refresh_institutions_current`** (lines 390-448).

- [ ] **Step 5.3: Rewrite `refresh_blockholders_current`** (lines 568-624).

- [ ] **Step 5.4: Rewrite `refresh_treasury_current`** (lines 689-739).

- [ ] **Step 5.5: Rewrite `refresh_def14a_current`** (lines 843-905). Reuse the module-level `_ESOP_HOLDER_NAME_SQL_REGEX` constant via a named SQL parameter.

- [ ] **Step 5.6: Rewrite `refresh_esop_current`** (lines 1194-1255).

- [ ] **Step 5.7: Run non-repair-sweep tests for all 7 helpers**

Run: `uv run pytest tests/test_ownership_refresh_writer_merge.py -k "not repair_sweep and not known_to_expiry" -v --no-cov 2>&1 | tail -30`
Expected: cases 1-7 green for all 7 helpers. Cases 8 + 9 still red (await Task 6).

- [ ] **Step 5.8: Run typecheck + ruff**

Run: `uv run pyright app/services/ownership_observations.py 2>&1 | tail -5 && uv run ruff check app/services/ownership_observations.py`
Expected: 0 errors, 0 warnings.

- [ ] **Step 5.9: Commit all 6 helpers**

```bash
git add app/services/ownership_observations.py
git commit -m "feat(#1233): rewrite 6 remaining refresh_*_current helpers to diff-aware MERGE

insiders / institutions / blockholders / treasury / def14a / esop
all migrated to the spec §4 template. Per-helper differences per §4.1:
- insiders: source-priority CASE chain preserved in ORDER BY
- treasury: AND treasury_shares IS NOT NULL filter
- def14a: 3-clause ESOP exclusion (role + name regex + shares NOT NULL)
- other 4: known_to IS NULL only

Cases 1-7 green for all 7 helpers; cases 8-9 await Task 6 repair-sweep
predicate switch.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: ownership_observations_repair.py — predicate switch + 7-category expansion

**Files:**
- Modify: `app/jobs/ownership_observations_repair.py`

- [ ] **Step 6.1: Read current file**

Run: `cat /Users/lukebradford/Dev/eBull/app/jobs/ownership_observations_repair.py | head -170`
Expected: confirms `_CATEGORIES` list (currently 5), `_drifted_instruments(conn, current_table, observations_table)` signature.

- [ ] **Step 6.2: Rewrite the predicate + signature**

Replace the body of `_drifted_instruments` (currently lines ~106-125) with the obs-anchored CTE aggregate against the state table. Extend the signature to take `category_literal: str`:

```python
def _drifted_instruments(
    conn: psycopg.Connection[Any],
    current_table: str,            # retained for log lines; not used in predicate
    observations_table: str,
    category_literal: str,
) -> list[int]:
    """Return instrument_ids whose ownership_refresh_state watermark is
    distinct from MAX(observations.ingested_at) for that instrument
    (#1233 PR12). Obs-anchored CTE aggregate keeps the sweep cost
    bounded by a single full-index scan per category per tick instead
    of 87k partition-fanout LATERAL probes (Codex 1e MED-1)."""
    query = sql.SQL(
        "WITH obs_max AS ("
        "    SELECT instrument_id, MAX(ingested_at) AS m"
        "    FROM {obs_t}"
        "    GROUP BY instrument_id"
        ") "
        "SELECT s.instrument_id "
        "FROM ownership_refresh_state s "
        "LEFT JOIN obs_max ON obs_max.instrument_id = s.instrument_id "
        "WHERE s.category = %s "
        "  AND s.last_drained_observations_max_ingested_at IS DISTINCT FROM obs_max.m"
    ).format(obs_t=sql.Identifier(observations_table))
    with conn.cursor() as cur:
        cur.execute(query, (category_literal,))
        return [int(row[0]) for row in cur.fetchall()]
```

- [ ] **Step 6.3: Expand `_CATEGORIES` to 7**

In the same file, extend `_CATEGORIES` list (currently 5 entries — insiders / institutions / blockholders / treasury / def14a) to 7 by adding `funds` + `esop` lambdas. Each tuple now also carries the matching `category_literal` so the predicate can use it. Update the tuple type annotation:

```python
_CATEGORIES: list[tuple[str, str, str, Callable[[psycopg.Connection[Any], int], int]]] = [
    (
        'ownership_insiders_current',
        'ownership_insiders_observations',
        'insiders',
        lambda c, i: refresh_insiders_current(c, instrument_id=i),
    ),
    (
        'ownership_institutions_current',
        'ownership_institutions_observations',
        'institutions',
        lambda c, i: refresh_institutions_current(c, instrument_id=i),
    ),
    (
        'ownership_blockholders_current',
        'ownership_blockholders_observations',
        'blockholders',
        lambda c, i: refresh_blockholders_current(c, instrument_id=i),
    ),
    (
        'ownership_treasury_current',
        'ownership_treasury_observations',
        'treasury',
        lambda c, i: refresh_treasury_current(c, instrument_id=i),
    ),
    (
        'ownership_def14a_current',
        'ownership_def14a_observations',
        'def14a',
        lambda c, i: refresh_def14a_current(c, instrument_id=i),
    ),
    (
        'ownership_funds_current',
        'ownership_funds_observations',
        'funds',
        lambda c, i: refresh_funds_current(c, instrument_id=i),
    ),
    (
        'ownership_esop_current',
        'ownership_esop_observations',
        'esop',
        lambda c, i: refresh_esop_current(c, instrument_id=i),
    ),
]
```

Add the matching imports at top of file: `refresh_funds_current`, `refresh_esop_current`.

- [ ] **Step 6.4: Update `run_observations_repair_sweep` loop**

Update the iteration in `run_observations_repair_sweep` to unpack the new 4-tuple and pass `category_literal` to `_drifted_instruments`:

```python
for current_table, observations_table, category_literal, refresh_fn in _CATEGORIES:
    drifted = _drifted_instruments(conn, current_table, observations_table, category_literal)
    ...
```

- [ ] **Step 6.5: Run all 52 tests**

Run: `uv run pytest tests/test_ownership_refresh_writer_merge.py -v --no-cov 2>&1 | tail -30`
Expected: all 52 cases PASS.

- [ ] **Step 6.6: Run existing repair-sweep tests**

Run: `uv run pytest tests/ -k "repair" --no-cov 2>&1 | tail -15`
Expected: existing tests still pass (may need adjustment if they were testing the legacy 3-arg signature — fix in this commit if so).

- [ ] **Step 6.7: Typecheck + ruff**

Run: `uv run pyright app/jobs/ownership_observations_repair.py && uv run ruff check app/jobs/ownership_observations_repair.py`
Expected: 0 errors.

- [ ] **Step 6.8: Commit**

```bash
git add app/jobs/ownership_observations_repair.py tests/test_ownership_refresh_writer_merge.py
git commit -m "feat(#1233): repair-sweep predicate → obs-anchored CTE aggregate + 7 categories

_drifted_instruments now uses CTE aggregate join to ownership_refresh_state
with IS DISTINCT FROM (Codex 1e MED-1 — avoids per-row LATERAL partition
fanout). _CATEGORIES list expanded from 5 to 7 (adds funds + esop) so
the sweep stays uniform with the new state-table CHECK constraint
(Codex 1b MED-4).

All 52 PR12 tests pass.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: PG ≥ 17 boot-time guard

**Files:**
- Create: `app/system/postgres_version_guard.py`
- Create: `tests/test_postgres_version_guard.py`
- Modify: `app/main.py` (verify path; or wherever lifespan startup lives)
- Modify: `tests/smoke/test_app_boots.py` — assert guard fires

- [ ] **Step 7.1: Locate lifespan owner**

Run: `grep -rn "lifespan\|startup\|on_event.*startup" /Users/lukebradford/Dev/eBull/app/main.py /Users/lukebradford/Dev/eBull/app/__init__.py 2>&1 | head -10`
Expected: identifies the function that runs at startup (likely `lifespan` in `app/main.py` per #1208 + #1187 prior pattern).

- [ ] **Step 7.2: Write failing test**

Create `tests/test_postgres_version_guard.py`:

```python
"""PR12 PG >= 17 boot-time guard (#1233 spec §7)."""
from __future__ import annotations

import pytest

from app.system.postgres_version_guard import assert_postgres_min_version


def test_guard_passes_on_pg17(ebull_test_conn):
    # Dev DB is PG17.9; guard must not raise.
    assert_postgres_min_version(ebull_test_conn, min_version_num=170000)


def test_guard_fails_on_simulated_pg16():
    # Patch the version probe to return PG16's server_version_num.
    # No DB needed — FakeConn duck-types the .cursor() contract.
    class FakeCursor:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): pass
        def fetchone(self): return (160005,)
    class FakeConn:
        def cursor(self): return FakeCursor()
    with pytest.raises(RuntimeError, match="PG >= 17"):
        assert_postgres_min_version(FakeConn(), min_version_num=170000)
```

Codex 1b plan-rev2 MED-1: `ebull_test_conn` (not `test_db_conn`) — matches the actual fixture in `tests/fixtures/ebull_test_db.py`. Codex 1b plan-rev2 LOW-2: unused `import psycopg` removed; the FakeConn duck-types only `.cursor()` which is all the guard touches.

- [ ] **Step 7.3: Run test — verify fail**

Run: `uv run pytest tests/test_postgres_version_guard.py -v --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.system.postgres_version_guard'`.

- [ ] **Step 7.4: Implement guard**

Create `app/system/__init__.py` if not present (empty file). Create `app/system/postgres_version_guard.py`:

```python
"""PR12 — PG >= 17 boot-time guard (#1233 spec §7).

MERGE WHEN NOT MATCHED BY SOURCE is PG17+. Without this guard a PG <
17 deployment would pass lint + crash at first refresh with
`syntax error at or near "BY"`. Fail-closed at lifespan startup
mirrors the #1187 max_locks_per_transaction pattern."""
from __future__ import annotations

import psycopg


def assert_postgres_min_version(
    conn: psycopg.Connection,
    *,
    min_version_num: int = 170000,
) -> None:
    """Raise RuntimeError if connected PG server is older than min_version_num.

    min_version_num uses Postgres `server_version_num` encoding (major *
    10000 + minor). 170000 = PG 17.0.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('server_version_num')::int")
        actual = cur.fetchone()[0]
    if actual < min_version_num:
        raise RuntimeError(
            f"Postgres {actual} detected — PR12 requires PG >= 17 "
            f"(MERGE WHEN NOT MATCHED BY SOURCE). Configured minimum: "
            f"{min_version_num}. See #1233 / spec §7."
        )
```

- [ ] **Step 7.5: Wire guard into lifespan startup**

In `app/main.py` (or wherever Task 7.1 identified), add the call inside lifespan startup, after the existing master-key bootstrap + master_locks_per_transaction guard:

```python
from app.system.postgres_version_guard import assert_postgres_min_version

# inside lifespan startup, with the existing DB conn:
assert_postgres_min_version(conn)
```

- [ ] **Step 7.6: Update smoke test**

In `tests/smoke/test_app_boots.py`, no new assertion is needed if the smoke test just exercises lifespan — the guard runs on PG17 dev DB and passes silently. Verify smoke still passes:

Run: `uv run pytest tests/smoke/test_app_boots.py -v --no-cov 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 7.7: Run guard unit tests**

Run: `uv run pytest tests/test_postgres_version_guard.py -v --no-cov 2>&1 | tail -10`
Expected: both tests PASS.

- [ ] **Step 7.8: Typecheck + ruff**

Run: `uv run pyright app/system/postgres_version_guard.py app/main.py && uv run ruff check app/system/postgres_version_guard.py app/main.py`
Expected: 0 errors.

- [ ] **Step 7.9: Commit**

```bash
git add app/system/__init__.py app/system/postgres_version_guard.py tests/test_postgres_version_guard.py app/main.py
git commit -m "feat(#1233): PG >= 17 boot-time guard at lifespan startup

PR12 requires PG17 MERGE WHEN NOT MATCHED BY SOURCE (PG15/16 only have
WHEN NOT MATCHED BY TARGET). Mirror #1187 max_locks_per_transaction
boot-pattern: assert at lifespan, fail-closed, pinned by smoke gate.

Spec §7.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: lint guard — scripts/check_ownership_refresh_writer_pattern.sh (93 clause-counts)

**Files:**
- Create: `scripts/check_ownership_refresh_writer_pattern.sh`
- Modify: `.githooks/pre-push`

- [ ] **Step 8.1: Read existing lint guard for the awk pattern**

Run: `cat /Users/lukebradford/Dev/eBull/scripts/check_13dg_retention.sh | head -80`
Expected: confirms the awk-based function-body block walker + empty-grep `wc -l` guard pattern to mirror.

- [ ] **Step 8.2: Write lint guard with per-helper expected-literal table**

Create `scripts/check_ownership_refresh_writer_pattern.sh`. Codex 1a MED-5 — inline the per-helper expected literal table so the implementer is not free to drift. Mirror `scripts/check_13dg_retention.sh` exactly for the awk + grep + exact-count-assertion harness; the per-helper data drives a loop.

Per-helper expected-literal table (verbatim from spec §4.1 — paste this into the script as a bash associative-array config block):

```bash
# Per-helper config: name → (distinct_on_cols, order_by_normalised, extra_where_filter, category_literal)
declare -A HELPERS=(
    [insiders]="$(cat <<'CFG'
distinct_on=holder_identity_key, ownership_nature
# ORDER BY is the canonical newest-priority-source-first chain; whitespace-normalised.
order_by=holder_identity_key, ownership_nature, CASE source WHEN 'form4' THEN 1 WHEN 'form3' THEN 2 WHEN '13d' THEN 3 WHEN '13g' THEN 3 WHEN 'def14a' THEN 4 WHEN '13f' THEN 5 WHEN 'nport' THEN 6 WHEN 'ncsr' THEN 6 WHEN 'xbrl_dei' THEN 7 WHEN '10k_note' THEN 8 WHEN 'finra_si' THEN 9 ELSE 10 END ASC, period_end DESC, filed_at DESC, source ASC, source_document_id ASC
extra_where=
category=insiders
CFG
)"
    [institutions]="$(cat <<'CFG'
distinct_on=filer_cik, ownership_nature, exposure_kind
order_by=filer_cik, ownership_nature, exposure_kind, period_end DESC, filed_at DESC, source_document_id ASC
extra_where=
category=institutions
CFG
)"
    [blockholders]="$(cat <<'CFG'
distinct_on=reporter_cik, ownership_nature
order_by=reporter_cik, ownership_nature, filed_at DESC, period_end DESC, source_document_id ASC
extra_where=
category=blockholders
CFG
)"
    [treasury]="$(cat <<'CFG'
distinct_on=instrument_id
order_by=instrument_id, period_end DESC, filed_at DESC, source_document_id ASC
extra_where=AND treasury_shares IS NOT NULL
category=treasury
CFG
)"
    [def14a]="$(cat <<'CFG'
distinct_on=holder_name_key, ownership_nature
order_by=holder_name_key, ownership_nature, period_end DESC, filed_at DESC, source_document_id ASC
# Three independent clauses; lint K1/K2/K3 each grep one clause.
extra_where=AND shares IS NOT NULL AND holder_role IS DISTINCT FROM 'esop' AND holder_name !~* %(esop_regex)s
category=def14a
CFG
)"
    [funds]="$(cat <<'CFG'
distinct_on=fund_series_id
order_by=fund_series_id, filed_at DESC, period_end DESC, source_document_id ASC
extra_where=
category=funds
CFG
)"
    [esop]="$(cat <<'CFG'
distinct_on=plan_name
order_by=plan_name, filed_at DESC, period_end DESC, source_document_id ASC
extra_where=
category=esop
CFG
)"
)
```

The script must:
1. For each helper key in `HELPERS`, extract the body span (`awk` between `^def refresh_${helper}_current\(` and the next `^def `) into a temp file.
2. Apply invariants A-L per spec §5 against the body-span temp file, using the per-helper config values for G (DISTINCT ON literal), H (ORDER BY normalised), K (extra WHERE filter literal — split into K1/K2/K3 for def14a), L (category literal).
3. For invariant D, awk-extract the ON-clause span (between `MERGE INTO ownership_${helper}_current AS tgt` and the first `WHEN`) and the WHEN NOT MATCHED BY SOURCE clause separately; assert literal `tgt.instrument_id = %(iid)s` count == 1 in each.
4. Apply cross-cutting invariants M, N, O1-O3, P1-P7 against the repo tree (not per-helper).
5. Emit `FAIL: invariant <ID> helper=<name> expected=<N> actual=<M>` on mismatch; exit non-zero.
6. Mirror `scripts/check_13dg_retention.sh`'s awk-helper-function shape (extracting the body span via line range from the named function-def to the next line starting `^def `).

Estimated script length: 250-400 lines of bash. Use shellcheck before committing.

- [ ] **Step 8.3: Run lint guard against the implemented code**

Run: `bash scripts/check_ownership_refresh_writer_pattern.sh`
Expected: 0 failures. If any invariant fails, fix the helper or the lint expected literal (whichever is wrong per spec §4.1).

- [ ] **Step 8.4: Wire into pre-push hook**

In `.githooks/pre-push`, add a new check after the existing `check_13dg_retention.sh` invocation:

```bash
echo "==> check_ownership_refresh_writer_pattern.sh"
bash "$(dirname "$0")/../scripts/check_ownership_refresh_writer_pattern.sh" || exit 1
```

- [ ] **Step 8.5: Smoke pre-push hook**

Run: `bash .githooks/pre-push 2>&1 | tail -10`
Expected: all checks pass.

- [ ] **Step 8.6: Commit**

```bash
git add scripts/check_ownership_refresh_writer_pattern.sh .githooks/pre-push
git commit -m "lint(#1233): scripts/check_ownership_refresh_writer_pattern.sh (93 clause-counts)

Breakdown per spec §5: 81 per-helper (7 × 10 A-J + 7 × 1 L + 1 treasury K
+ 3 def14a K1/K2/K3) + 12 cross-cutting (M/N/O1-O3/P1-P7) = 93. K-class
is conditional, not a flat ×7 multiplier (Codex 1b plan-rev2 MED-3 —
prior commit-message shorthand '7 × 12 + 12 = 96' was wrong; spec is
authoritative). Awk-based function-body block walker + exact-count grep
assertions mirror the PR11 check_13dg_retention.sh pattern.

Wired into .githooks/pre-push after check_13dg_retention.sh.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: orphan-audit script

**Files:**
- Create: `scripts/ownership_refresh_state_orphan_audit.sh`

- [ ] **Step 9.1: Write the audit script**

Create `scripts/ownership_refresh_state_orphan_audit.sh`:

```bash
#!/usr/bin/env bash
# scripts/ownership_refresh_state_orphan_audit.sh
#
# PR12 (#1233) — out-of-band orphan-reconciliation audit.
# Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §3.3 + §9 DoD #12.
#
# Pins the write-through invariant that every record_*_observation
# production caller also fires refresh_*_current (which UPSERTs into
# ownership_refresh_state). On a healthy install this returns zero
# rows per category. Non-zero output indicates a write-through
# regression and exits non-zero so the script is safe to wire into
# a separate cron later.

set -euo pipefail

DB_HOST="${EBULL_PG_HOST:-localhost}"
DB_PORT="${EBULL_PG_PORT:-5432}"
DB_USER="${EBULL_PG_USER:-postgres}"
DB_NAME="${EBULL_PG_DATABASE:-ebull}"

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
    count=$(
        PGPASSWORD="${EBULL_PG_PASSWORD:-}" psql \
            -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
            -t -A -c "
            SELECT count(DISTINCT o.instrument_id)
            FROM ${obs_table} o
            WHERE NOT EXISTS (
                SELECT 1 FROM ownership_refresh_state s
                WHERE s.instrument_id = o.instrument_id
                  AND s.category = '${category}'
            )
            "
    )
    echo "category=${category} orphan_instruments=${count}"
    orphan_count_total=$((orphan_count_total + count))
done

echo "---"
echo "orphan_count_total=${orphan_count_total}"

if [ "$orphan_count_total" -gt 0 ]; then
    echo "FAIL: write-through invariant breach — at least one observation"
    echo "       writer landed without firing the matching refresh_*_current."
    echo "       Investigate the per-category counts above."
    exit 1
fi

echo "OK: zero orphans across all 7 categories."
exit 0
```

- [ ] **Step 9.2: Make executable + run**

Run:
```bash
chmod +x scripts/ownership_refresh_state_orphan_audit.sh
bash scripts/ownership_refresh_state_orphan_audit.sh
```
Expected: zero orphan rows across all 7 categories (per the §3.3 backfill from `_current.refreshed_at`) — should output `orphan_count_total=0`.

If non-zero: PR12 backfill is incomplete OR write-through has been broken pre-PR12. Re-run sql/163. If still non-zero, investigate which categories are non-zero — possible causes: instruments in observations that have never had a `_current` row (e.g. parser added obs but never called refresh).

- [ ] **Step 9.3: Capture output for PR description**

Save the run output for PR description verification (per spec §9 DoD #12):
```bash
bash scripts/ownership_refresh_state_orphan_audit.sh > /tmp/pr12_orphan_audit.txt 2>&1
cat /tmp/pr12_orphan_audit.txt
```

- [ ] **Step 9.4: Commit**

```bash
git add scripts/ownership_refresh_state_orphan_audit.sh
git commit -m "feat(#1233): orphan-audit script for ownership_refresh_state

scripts/ownership_refresh_state_orphan_audit.sh — operator-triggered,
scans per category for instruments with observations but no
ownership_refresh_state row. Exits non-zero on orphans so the script
is safe to wire into a separate cron later.

Pins the write-through invariant the dropped UNION orphan tail
(§3.3 sweep predicate) is delegating to.

Spec §9 DoD #12 (Codex 1e MED-2 + 1f MED-1).

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: smoke + verification on dev DB

**Files:**
- Modify: (none; just runs verification commands)

- [ ] **Step 10.1: Smoke 5-instrument panel**

For each of AAPL / GME / MSFT / JPM / HD, resolve the `instrument_id` and call `refresh_funds_current` + `refresh_institutions_current` twice in succession. Capture `pgstattuple.table_len` delta + per-row `xmin::text` stability across the second call. Also call `refresh_treasury_current` + `refresh_def14a_current` for at least AAPL to exercise small-table helpers + per-helper filters.

Run (one-liner, repeated per instrument — adjust ticker → instrument_id resolution to match the local DB):

```bash
docker exec ebull-postgres psql -U postgres -d ebull -c "
WITH iids AS (
  SELECT instrument_id, symbol FROM instruments
  WHERE symbol IN ('AAPL', 'GME', 'MSFT', 'JPM', 'HD')
)
SELECT * FROM iids ORDER BY symbol"
```

Then for each instrument run (substituting `<IID>`):

```bash
docker exec ebull-postgres psql -U postgres -d ebull -c "
SELECT pgstattuple('ownership_funds_current'), pgstattuple('ownership_institutions_current')"
# call refresh helpers via a Python one-liner that opens a psycopg connection
uv run python -c "
import psycopg
from app.services.ownership_observations import refresh_funds_current, refresh_institutions_current
with psycopg.connect('host=localhost dbname=ebull user=postgres') as c:
    refresh_funds_current(c, instrument_id=<IID>)
    refresh_institutions_current(c, instrument_id=<IID>)
    c.commit()
"
# second call
uv run python -c "
import psycopg
from app.services.ownership_observations import refresh_funds_current, refresh_institutions_current
with psycopg.connect('host=localhost dbname=ebull user=postgres') as c:
    refresh_funds_current(c, instrument_id=<IID>)
    refresh_institutions_current(c, instrument_id=<IID>)
    c.commit()
"
# final pgstattuple — table_len must equal pre-first-call value
```

Capture results in a markdown table for the PR description.

- [ ] **Step 10.2: EXPLAIN ANALYZE the repair-sweep predicate**

Run for each category (substituting the table name):

```bash
docker exec ebull-postgres psql -U postgres -d ebull -c "
EXPLAIN (ANALYZE, BUFFERS)
WITH obs_max AS (
    SELECT instrument_id, MAX(ingested_at) AS m
    FROM ownership_funds_observations
    GROUP BY instrument_id
)
SELECT s.instrument_id
FROM ownership_refresh_state s
LEFT JOIN obs_max ON obs_max.instrument_id = s.instrument_id
WHERE s.category = 'funds'
  AND s.last_drained_observations_max_ingested_at IS DISTINCT FROM obs_max.m"
```

Expected: total time ≤ 5s on funds (largest category). If exceeded, investigate index health, partition pruning, or revisit the predicate shape per spec §3.3.

- [ ] **Step 10.3: Run orphan audit one more time**

Run: `bash scripts/ownership_refresh_state_orphan_audit.sh`
Expected: `orphan_count_total=0`.

- [ ] **Step 10.4: Run full pytest + smoke gate**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```
All four must pass. Last command includes the smoke gate.

- [ ] **Step 10.5: Capture verification artefacts**

Save the following to a local `/tmp/pr12_verification.md` for the PR description:
- 5-instrument smoke table (instrument_id, helper, pre/post table_len, pre/post xmin equality, refresh count)
- EXPLAIN ANALYZE wall-clock per category
- Orphan audit output
- Pre-push checklist all-pass confirmation

- [ ] **Step 10.6: No commit — verification only**

This task does not produce a commit; results feed Task 12 PR description.

---

## Task 11: documentation amendments + prevention log + skill update

**Files:**
- Modify: `docs/superpowers/specs/2026-05-19-data-retention-rubric.md`
- Modify: `docs/review-prevention-log.md`
- Modify: `.claude/skills/data-engineer/SKILL.md`

- [ ] **Step 11.1: Amend parent spec — flip PR12 to SHIPPED**

In `docs/superpowers/specs/2026-05-19-data-retention-rubric.md`, find §7 "Implementation sequence" and change `PR12 — pending.` to:

```markdown
- **PR12 — SHIPPED.** `ownership_*_current` writer rewrite (DELETE+INSERT → diff-aware MERGE) + new `ownership_refresh_state` watermark side-table. All 7 helpers migrated; repair-sweep predicate switched to obs-anchored CTE aggregate; `_CATEGORIES` expanded from 5 to 7; PG >= 17 boot guard; 93-clause lint guard wired into pre-push; 52 parametrised tests; orphan-audit script. Closes the §4.5 / §4.6 / §6.4 "ownership_*_current size oddity" question. Implementation spec: `docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md`.
```

Find §4.5 + §4.6 + §6.4 + §8 and add "**Resolved by PR12 (this commit)**" notes pointing at the impl spec. Update §8 Phase 2 acceptance tier to reference PR12 as the unblocker.

- [ ] **Step 11.2: Add prevention-log entries**

In `docs/review-prevention-log.md`, append two new entries at the bottom (or in their topical section if one exists):

**Entry 1: MERGE clamp**

```markdown
### MERGE WHEN NOT MATCHED BY SOURCE must carry per-scope clamp on BOTH the ON clause AND the DELETE clause

- Symptom: a future refactor relaxes the ON clause from `tgt.instrument_id = %(iid)s AND tgt.<pk_col> = src.<pk_col>` to `tgt.instrument_id = src.instrument_id AND tgt.<pk_col> = src.<pk_col>`. Source subquery returns rows for one instrument; MERGE evaluates target rows for ALL instruments under the relaxed ON; `WHEN NOT MATCHED BY SOURCE THEN DELETE` deletes every target row not in src — catastrophic global data loss.
- Prevention: the scope clamp `AND tgt.<scope_col> = %(scope)s` MUST appear in BOTH the ON clause AND the `WHEN NOT MATCHED BY SOURCE` clause. Lint pins both literals via block-local awk extraction + exact-count assertions.
- First seen in: #1233 PR12 spec (Codex 1a HIGH-2 + HIGH-4; rev 4 lint invariant D split into D1 + D2).
- Enforced in: `scripts/check_ownership_refresh_writer_pattern.sh` invariants D1 + D2; this prevention log.
```

**Entry 2: diff-predicate watermark separation**

```markdown
### Diff-aware writers (MERGE … IS DISTINCT FROM) MUST NOT include update-timestamp columns in the diff predicate — drift watermarks belong in a separate side-table

- Symptom: `refresh_X_current` uses MERGE with `WHEN MATCHED AND (cols, refreshed_at) IS DISTINCT FROM (excluded.cols, now())`. `now()` always differs from the stored `refreshed_at` → every MATCHED row re-fires UPDATE → N dead tuples per call → bloat returns to the DELETE+INSERT baseline. OR: same predicate without `refreshed_at` — `refreshed_at` stays frozen on no-op refresh → repair-sweep `c.refreshed_at < MAX(obs.ingested_at)` is permanently true on re-ingested instruments → sweep selects forever.
- Prevention: `refreshed_at` (and any update-timestamp column) MUST be EXCLUDED from the diff predicate on both LHS and RHS. The drift watermark for repair-sweep MUST live in a separate side-table (`ownership_refresh_state.last_drained_observations_max_ingested_at`) updated on every refresh attempt regardless of MERGE outcome.
- First seen in: #1233 PR12 spec (Codex 1a HIGH-1 + Codex 1b HIGH-2 race fix; rev 3 introduced the side-table + pre-MERGE watermark capture).
- Enforced in: `scripts/check_ownership_refresh_writer_pattern.sh` invariant E (refreshed_at not in IS DISTINCT FROM tuples); `tests/test_ownership_refresh_writer_merge.py` case 2 (no-op churn) + case 8 (repair-sweep no-loop).
```

- [ ] **Step 11.3: Update data-engineer skill + memory pointer**

Update BOTH files (Codex 1a LOW-1 — file map only listed the skill, not the memory pointer):
- `.claude/skills/data-engineer/SKILL.md` (in-repo skill body)
- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/skill_ebull_data_engineer.md` (operator memory pointer — already exists; add a single-line `- Diff-aware MERGE replaces DELETE+INSERT in refresh_*_current (#1233 PR12 — see spec docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md)` under the existing write-through pattern bullet)

In `.claude/skills/data-engineer/SKILL.md`, find the write-through pattern section and add:

```markdown
### Diff-aware MERGE replaces DELETE+INSERT in refresh_*_current helpers (#1233 PR12)

Every `refresh_X_current(conn, *, instrument_id)` helper uses a single-statement PG17 `MERGE … WHEN NOT MATCHED BY SOURCE` with:

- `WHEN MATCHED AND (business_cols) IS DISTINCT FROM (...) THEN UPDATE`  — skips writes on identical rows (no dead tuples, no `refreshed_at` advance).
- `WHEN NOT MATCHED BY TARGET THEN INSERT`  — adds rows that don't yet exist.
- `WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE`  — removes rows that fell out of the latest set.
- `refreshed_at` is excluded from the diff predicate (would defeat the no-op optimisation).
- Watermark capture (`SELECT MAX(ingested_at) FROM observations`) runs BEFORE the MERGE in a separate statement; the captured value is passed to `ownership_refresh_state` UPSERT after the MERGE (Codex 1b HIGH-2 race fix).
- Drift watermark for the repair sweep lives in `ownership_refresh_state(instrument_id, category, last_drained_observations_max_ingested_at, last_refresh_attempted_at)` — keeps storage-layer churn separate from consumer-side drift detection.

Lint guard: `scripts/check_ownership_refresh_writer_pattern.sh` (93 clause-counts). Spec: `docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md`.
```

- [ ] **Step 11.4: Commit**

```bash
git add docs/superpowers/specs/2026-05-19-data-retention-rubric.md docs/review-prevention-log.md .claude/skills/data-engineer/SKILL.md /Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/skill_ebull_data_engineer.md 2>/dev/null || git add docs/superpowers/specs/2026-05-19-data-retention-rubric.md docs/review-prevention-log.md .claude/skills/data-engineer/SKILL.md
# (Memory file lives outside the repo; track its update manually if so —
# the conditional git add ignores it if the path is outside the work tree.)
git commit -m "docs(#1233): PR12 — parent spec amendment + prevention log + skill update

- Parent rubric spec §7: PR12 flipped to SHIPPED.
- Parent rubric spec §4.5 / §4.6 / §6.4 / §8: PR12 resolution notes.
- Prevention log: 2 new entries (MERGE clamp + diff-predicate watermark
  separation).
- Data-engineer skill: write-through section gains diff-aware MERGE rule.

Refs #1233.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: PR push + bot review + merge

**Files:**
- (none; PR plumbing only)

- [ ] **Step 12.1: Pre-push checklist**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
bash .githooks/pre-push 2>&1 | tail -20
```
All five must pass.

- [ ] **Step 12.2: Codex 2 pre-push review**

Run:
```bash
codex exec --output-last-message /tmp/codex_pr12_impl_2.txt \
  "Pre-push review on branch feature/1233-pr12-impl. PR12 implementation of \
   spec at docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md. \
   Focus: correctness gaps; race conditions; SQL injection; performance \
   regressions; spec drift between code and spec §3.3/§4/§5/§6/§9. Be terse, \
   HIGH/MED/LOW with line numbers." < /dev/null
cat /tmp/codex_pr12_impl_2.txt
```
Fold any HIGH/MED findings before pushing.

- [ ] **Step 12.3: Push branch**

Run: `git push -u origin feature/1233-pr12-impl`

- [ ] **Step 12.4: Create PR**

Run:
```bash
gh pr create --title "feat(#1233): PR12 — ownership_*_current writer rewrite (MERGE + watermark side-table)" --body "$(cat /tmp/pr12_pr_description.md)"
```

PR body content (write to `/tmp/pr12_pr_description.md` first):

```markdown
## Summary

- Rewrites all 7 `refresh_*_current` helpers from DELETE+INSERT to diff-aware PG17 MERGE.
- New `ownership_refresh_state` side-table separates drift-watermark from `_current.refreshed_at` (avoids defeating bloat fix per spec §3.3).
- Repair sweep predicate → obs-anchored CTE aggregate; `_CATEGORIES` expanded 5 → 7.
- 93 lint clause-counts; 52 parametrised tests; PG ≥ 17 boot guard; orphan-audit script.
- Spec: `docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md` (rev 7, 7 Codex review rounds).

Refs #1233.

## Verification

| Instrument | refresh_funds_current pre→post table_len | xmin stable on no-op | refresh_institutions_current pre→post table_len | xmin stable on no-op |
| --- | --- | --- | --- | --- |
| AAPL | <fill from Task 10.1> | yes/no | <fill> | yes/no |
| GME | … | … | … | … |
| MSFT | … | … | … | … |
| JPM | … | … | … | … |
| HD | … | … | … | … |

- EXPLAIN ANALYZE repair sweep: funds <Xs>, institutions <Ys>, insiders <Zs>, ... (all under 5s).
- Orphan audit: `orphan_count_total=0` across all 7 categories at commit SHA <fill>.
- Pre-push: lint + ruff + pyright + pytest + .githooks/pre-push all pass.

## Test plan

- [x] All 52 parametrised cases in `tests/test_ownership_refresh_writer_merge.py` pass.
- [x] Existing `tests/test_ownership_observations_repair.py` updated for 4-tuple signature; passes.
- [x] Smoke `tests/smoke/test_app_boots.py` passes (PG ≥ 17 guard fires).
- [x] Orphan audit script returns `orphan_count_total=0`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- [ ] **Step 12.5: Poll CI + bot review**

Run: `gh pr checks <PR#> --watch`
Then: `gh pr view <PR#> --comments | tail -100`

Resolve every review comment using the FIXED {sha} / DEFERRED #{issue} / REBUTTED {reason} contract per CLAUDE.md.

- [ ] **Step 12.6: Iterate until APPROVE on latest commit + CI green**

Standard #1208 cadence: read every comment, fix or rebut, push follow-up, re-run pre-push checklist + Codex on rebuttal-only rounds, wait for bot APPROVE, merge.

- [ ] **Step 12.7: Merge**

```bash
gh pr merge <PR#> --squash --delete-branch \
  --subject "feat(#1233): PR12 — ownership_*_current writer rewrite (MERGE + watermark side-table) (#<PR#>)" \
  --body "$(cat /tmp/pr12_merge_body.md)"
```

Merge body content captures the verification artefacts from Task 10.5 + final review trail summary.

- [ ] **Step 12.8: Verify main**

Run: `git checkout main && git pull --ff-only && git log --oneline -3`
Expected: PR12 merge commit at HEAD, parent spec amendment landed.

- [ ] **Step 12.9: Operator handoff for §6.3 pre-wipe**

PR12 is the LAST entry in #1233 §7 implementation sequence. After this merge, the operator triggers the §6.3 pre-wipe + clean re-run per parent spec §8 acceptance. Document the readiness in `docs/superpowers/plans/2026-05-21-pr12-ownership-current-writer-merge-impl.md` as a completion note.

---

## Self-Review

Pre-execution self-review of this plan against the spec:

**1. Spec coverage:**
- §3.3 ownership_refresh_state table + indexes + backfill → Task 1.
- §4 MERGE template → Task 4 (funds) + Task 5 (other 6).
- §4.1 per-helper differences → Task 5 task-by-task per helper.
- §4.2 diff-predicate contract → enforced by lint Task 8 invariant E + I + J.
- §4.3 scope clamp → enforced by lint Task 8 invariants D1 + D2.
- §4.4 concurrency → preserved by Task 4-5 (advisory lock + tx contract unchanged).
- §5 lint (93 clauses) → Task 8.
- §6 tests (52 cases) → Task 3 (scaffold) + Tasks 4-6 (turn green).
- §7 PG ≥ 17 boot guard → Task 7.
- §8 operator semantics → covered in Task 11 parent spec amendment.
- §9 DoD #1-#12 → Tasks 1-11 each cover one or more DoD items; Task 12 covers DoD #13 (Codex 2 + bot + merge).
- §10 acceptance protocol → Task 10.
- §11 review gate → Task 12.2.

No gaps.

**2. Placeholder scan:** none found.

**3. Type consistency:** `refresh_X_current(conn, *, instrument_id) -> int` preserved everywhere; `_drifted_instruments` signature extended consistently in Task 6.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-21-pr12-ownership-current-writer-merge-impl.md`. Two execution options:

**1. Subagent-Driven (recommended)** — Fresh subagent per task, review between tasks, fast iteration. Best fit here: tasks 4 + 5 + 6 are mechanically similar but distinct files; tasks 7-9 are independent; task 10 is verification-only; task 11 is docs. Parallelism opportunities at tasks 7 + 8 + 9 + 11.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.
