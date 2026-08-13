# Coverage status transition log — design spec

**Ticket:** #397
**Depends on:** #394 (PR #394 merged — guard-rejection strip pattern), #396 (PR #401 merged — position-alert event persistence)
**Follow-up:** #399 (AlertsStrip UI wire-up — consumes this endpoint)

## Problem

`coverage.filings_status` (migration 036) is current-state only. No transition history. Dashboard strip needs to show "this stock dropped from `analysable`" as an alert type mirroring the guard-rejection and position-alert feeds already shipped in #394 / #396.

## Decisions (brainstorm 2026-04-22)

| # | Decision | Reason |
| --- | --- | --- |
| 1 | **Database trigger** owns transition detection. No changes to any of the 3 Python mutator sites in `app/services/coverage.py`. | CASE-path UPDATEs on bulk audit write `filings_status = filings_status` (demote-guard preserving `structurally_young`); `IS DISTINCT FROM` guard inside the trigger filters those for free. Future fourth mutator auto-covered. |
| 2 | **Log all transitions, endpoint filters drops-from-analysable.** | Strip query is narrow; other slices (promotions, NULL→terminal first audit, audit history) stay in table for future UIs without schema change. Transitions are rare enough that table size is a non-issue. |
| 3 | **7-day query window, no pruning.** | Matches #394 + #396 precedent. Cursor + empty-window arithmetic stays identical across all three strip feeds. |
| 4 | **Partial index on drops-from-analysable.** | Only strip-query shape. Other slices scan full table — acceptable volume. No `now()` predicate (STABLE, not IMMUTABLE; Postgres rejects — same rationale as `045_position_alerts.sql`). |
| 5 | **Trigger-scoped advisory xact lock serializes coverage writers.** | `coverage.filings_status` has no global single-writer guarantee — `audit_instrument` is called standalone from #268 Chunk G (universe-sync hook) and Chunk E (post-backfill re-audit), concurrently with `audit_all_instruments` (daily scheduler) and `_apply_backfill_outcome` (backfill worker). Without serialization, commit order can diverge from `event_id` order → cursor skips rows. Trigger takes `pg_advisory_xact_lock(hashtext('coverage_status_events_writer'))` before `INSERT`; blocks concurrent writers until commit. Matches #396's single-writer prerequisite that `alerts.py:322` documents. |

## Architecture

Append-only event log in Postgres. Strip endpoint + cursor endpoints mirror the position-alerts pattern (#396) 1:1.

Three surfaces:

1. `sql/047_coverage_status_events.sql` — table, partial index, trigger function + trigger, operator cursor column.
2. `app/api/alerts.py` — 3 new routes: `GET /alerts/coverage-status-drops`, `POST /alerts/coverage-status-drops/seen`, `POST /alerts/coverage-status-drops/dismiss-all`. Module docstring updated to document the third feed.
3. CLAUDE.md formatting polish — bundled 3-space indent fix for `## Branch and PR workflow` step 3 (pending since 2026-04-06 per memory `project_pending_polish.md`).

No changes to `app/services/coverage.py`. No changes to existing mutator sites.

## Schema + Trigger

`sql/047_coverage_status_events.sql`:

```sql
-- Migration 047: coverage.filings_status transition log + operator read cursor
--
-- 1. coverage_status_events — append-only row per filings_status transition.
--    event_id BIGSERIAL PK for strict-> cursor semantics (matches
--    decision_audit / position_alerts rationale; clock-skew-safe).
-- 2. Trigger logs ALL transitions (including NULL->terminal first audit).
--    Endpoint filters to drops-from-analysable. Other slices reserved
--    for future audit UIs without schema change.
-- 3. Partial index narrows strip-fetch to drops-from-analysable. No
--    now()-based predicate (STABLE, not IMMUTABLE; Postgres rejects —
--    same rationale as 045_position_alerts.sql).
-- 4. operators.alerts_last_seen_coverage_event_id — parallel cursor to
--    existing alerts_last_seen_decision_id + alerts_last_seen_position_alert_id
--    columns. NULL = never acknowledged.

CREATE TABLE IF NOT EXISTS coverage_status_events (
    event_id      BIGSERIAL    PRIMARY KEY,
    instrument_id BIGINT       NOT NULL REFERENCES instruments(instrument_id),
    changed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    old_status    TEXT         NULL,
    new_status    TEXT         NULL
);

CREATE INDEX IF NOT EXISTS idx_coverage_status_events_drops
    ON coverage_status_events (event_id DESC)
    WHERE old_status = 'analysable' AND new_status IS DISTINCT FROM 'analysable';

-- Paired changed_at index mirroring the sql/046_position_alerts_opened_at_index.sql
-- fix: the strip-query filters BOTH on the cursor/index-key AND on the 7-day
-- changed_at window. Without this, event_id-DESC scans walk arbitrarily far
-- back through drops-from-analysable that have aged out of the window before
-- reaching 500 current rows. Same partial predicate so the two indexes cover
-- the same slice.
CREATE INDEX IF NOT EXISTS idx_coverage_status_events_drops_changed_at
    ON coverage_status_events (changed_at DESC)
    WHERE old_status = 'analysable' AND new_status IS DISTINCT FROM 'analysable';

CREATE OR REPLACE FUNCTION log_coverage_status_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.filings_status IS DISTINCT FROM OLD.filings_status THEN
        -- Xact-scoped advisory lock serializes concurrent coverage writers so
        -- commit order matches event_id order (#396-style cursor safety).
        -- Idempotent within a single txn (bulk UPDATE takes it once per
        -- transitioning row — stacking is harmless; all refs release on commit).
        PERFORM pg_advisory_xact_lock(hashtext('coverage_status_events_writer'));
        INSERT INTO coverage_status_events (instrument_id, old_status, new_status)
        VALUES (NEW.instrument_id, OLD.filings_status, NEW.filings_status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_coverage_filings_status_transition
    AFTER UPDATE OF filings_status ON coverage
    FOR EACH ROW
    EXECUTE FUNCTION log_coverage_status_transition();

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_coverage_event_id BIGINT;
```

### Trigger behaviour notes

- `AFTER UPDATE OF filings_status` — trigger only fires when `filings_status` appears on the SET list. UPDATEs that touch only `filings_audit_at` / `filings_backfill_*` do NOT fire (e.g. the `status=None` preservation branch in `_apply_backfill_outcome` at `app/services/coverage.py:1470-1480`).
- Bulk `audit_all_instruments` UPDATE sets `filings_status` always (CASE expression), so trigger fires N times per bulk UPDATE. Each row is row-level-evaluated; only transitioning rows write events via the `IS DISTINCT FROM` guard. Performance fine at universe scale (~2k rows daily).
- First-audit path (NULL→terminal) writes an event. Endpoint filter `old_status = 'analysable'` excludes from strip.
- Demote-guard preservation (`structurally_young` kept on `insufficient` classifier output) writes `filings_status = filings_status`; trigger guard skips insert.
- **INSERT path NOT covered by trigger.** Coverage rows land via `seed_coverage()` at `app/services/coverage.py:788` (`filings_status` defaults NULL) and `bootstrap_missing_coverage_rows()` at `app/services/coverage.py:841` (`'unknown'`). Neither fires the trigger. First `UPDATE` of `filings_status` on that row will fire — that's the first logged event. Consequence: the event log is a **UPDATE-transition log, not a full audit history of every status state the row ever held**. For the #397 scope (drops-from-analysable) this is moot — no INSERT path writes `'analysable'` directly. Documented here for any future endpoint that consumes the full `coverage_status_events` table. If a full-history requirement arrives, extend with an AFTER INSERT trigger and a synthetic-pre-state convention.

## API

`app/api/alerts.py` — three new routes, module docstring updated.

### Response models

```python
class CoverageStatusDrop(BaseModel):
    event_id: int
    instrument_id: int
    symbol: str
    changed_at: datetime
    old_status: str           # always 'analysable' by endpoint filter
    new_status: str | None    # terminal value post-drop (nullable defensive; CHECK normally prevents)

class CoverageStatusDropsResponse(BaseModel):
    alerts_last_seen_coverage_event_id: int | None
    unseen_count: int
    drops: list[CoverageStatusDrop]

class CoverageStatusDropsMarkSeenRequest(BaseModel):
    seen_through_event_id: int = Field(gt=0)
```

### `GET /alerts/coverage-status-drops`

```sql
-- 1. Read operator cursor
SELECT alerts_last_seen_coverage_event_id FROM operators WHERE operator_id = %(op)s

-- 2. Count unseen in-window drops (uncapped)
SELECT COUNT(*) AS unseen_count
FROM coverage_status_events
WHERE old_status = 'analysable'
  AND new_status IS DISTINCT FROM 'analysable'
  AND changed_at >= now() - INTERVAL '7 days'
  AND (%(last_id)s::BIGINT IS NULL OR event_id > %(last_id)s::BIGINT)

-- 3. Fetch list capped at 500. ORDER BY event_id DESC — BIGSERIAL PK is
-- race-safe (matches #394 + #396 rationale). Partial index serves this scan.
SELECT
    e.event_id,
    e.instrument_id,
    i.symbol,
    e.changed_at,
    e.old_status,
    e.new_status
FROM coverage_status_events e
JOIN instruments i ON i.instrument_id = e.instrument_id
WHERE e.old_status = 'analysable'
  AND e.new_status IS DISTINCT FROM 'analysable'
  AND e.changed_at >= now() - INTERVAL '7 days'
ORDER BY e.event_id DESC
LIMIT 500
```

### `POST /alerts/coverage-status-drops/seen`

```sql
UPDATE operators AS op
SET alerts_last_seen_coverage_event_id = GREATEST(
    COALESCE(op.alerts_last_seen_coverage_event_id, 0),
    LEAST(%(seen_through_event_id)s, m.max_id)
)
FROM (
    SELECT MAX(event_id) AS max_id
    FROM coverage_status_events
    WHERE old_status = 'analysable'
      AND new_status IS DISTINCT FROM 'analysable'
      AND changed_at >= now() - INTERVAL '7 days'
) AS m
WHERE op.operator_id = %(op)s
  AND m.max_id IS NOT NULL
```

Follows position-alerts pattern (post-#395 correct shape). No guard-rejection divergence — `m.max_id IS NOT NULL` guard preserves NULL cursor on empty window.

### `POST /alerts/coverage-status-drops/dismiss-all`

```sql
UPDATE operators AS op
SET alerts_last_seen_coverage_event_id = GREATEST(
    COALESCE(op.alerts_last_seen_coverage_event_id, 0),
    m.max_id
)
FROM (
    SELECT MAX(event_id) AS max_id
    FROM coverage_status_events
    WHERE old_status = 'analysable'
      AND new_status IS DISTINCT FROM 'analysable'
      AND changed_at >= now() - INTERVAL '7 days'
) AS m
WHERE op.operator_id = %(op)s
  AND m.max_id IS NOT NULL
```

Auth: router-level `Depends(require_session_or_service_token)` — already applied, no change. Operator resolution via existing `_resolve_operator` helper.

## Edge cases

| Case | Behaviour |
| --- | --- |
| First audit (NULL→terminal) | Trigger logs event. Endpoint filter `old_status = 'analysable'` excludes from strip. Kept in table for future history views. |
| CASE-path no-op on bulk UPDATE (`filings_status = filings_status`) | `IS DISTINCT FROM` guard skips insert. No spurious event. |
| Backfill transient retryable error (`status=None` branch) | UPDATE doesn't touch `filings_status`; `AFTER UPDATE OF filings_status` does NOT fire. No spurious event. |
| Demote-guard preserves `structurally_young` | Writes `filings_status = filings_status`; trigger sees OLD == NEW, skips insert. |
| Empty window + NULL cursor on /seen | `m.max_id IS NOT NULL` short-circuits; cursor stays NULL. No #395 divergence. |
| Operator with no drops in 7d | Endpoint returns `{ unseen_count: 0, drops: [] }`. Cursor untouched. |
| Concurrent re-audit | Append-only table. `pg_advisory_xact_lock(hashtext('coverage_status_events_writer'))` in trigger function serializes concurrent writers — commit order matches `event_id` order, so cursor can't skip a later-visible lower id. Required because `coverage` has multiple writer paths (see Decision 5 in the table above). |
| Multi-query snapshot drift on GET | Inherited from #395. GET executes 3 statements in sequence (read cursor, count unseen, fetch list) without transaction bracket; an advance of the cursor between statements could leave counts/list inconsistent by one row. Same behaviour + risk profile as position-alerts + guard-rejections. Tracked under tech-debt #395. Not a #397 regression. |
| Instrument delisted / coverage row deleted | FK `REFERENCES instruments(instrument_id)` without `ON DELETE CASCADE`. Delete blocks if history exists. Acceptable; instrument delete is rare + manual. Called out in PR body. |

## Testing

### `tests/test_migration_047_coverage_status_events.py` — migration assertions

Matches naming convention of existing `tests/test_etoro_credential_migration.py`. Applies migration against test DB. Asserts:

- `coverage_status_events` table columns + types + NOT NULL where specified.
- FK `instrument_id → instruments(instrument_id)` present.
- Partial index `idx_coverage_status_events_drops` exists with predicate matching `old_status = 'analysable' AND new_status IS DISTINCT FROM 'analysable'` (via `pg_indexes.indexdef`), indexed on `event_id DESC`.
- Partial index `idx_coverage_status_events_drops_changed_at` exists with the same predicate, indexed on `changed_at DESC`.
- Trigger `trg_coverage_filings_status_transition` exists on `coverage` with `AFTER UPDATE OF filings_status` (via `pg_trigger` + `pg_attribute`).
- Trigger function body references `pg_advisory_xact_lock` with the sentinel string (via `pg_proc.prosrc` LIKE match).
- `operators.alerts_last_seen_coverage_event_id` column exists, type BIGINT, nullable.

### `tests/test_coverage_status_transition_trigger.py` — trigger behaviour (real DB)

Matches naming convention of existing `tests/test_coverage_audit_integration.py`:

- NULL→'analysable' writes event with `old_status IS NULL`.
- 'analysable'→'insufficient' writes event.
- 'analysable'→'analysable' (no-op UPDATE assigning same value) writes nothing.
- Bulk CASE-path UPDATE preserving `structurally_young` across a row that classifier output `insufficient` writes nothing.
- UPDATE of unrelated column only (e.g. `filings_audit_at`) does NOT fire trigger — zero new events.
- Multiple row bulk UPDATE with mixed transitioning + non-transitioning rows writes exactly N events where N = count of actual transitions.
- **INSERT into `coverage` does NOT fire the trigger.** Verify that inserting a row with `filings_status = 'unknown'` (mirrors `bootstrap_missing_coverage_rows` at `coverage.py:841`) writes zero events. First subsequent UPDATE fires the trigger normally.
- **Concurrent writer serialization.** Two connections each open a txn, each UPDATE a different instrument's `filings_status` to a transition-worthy value. Assert that `event_id` order of the two inserted rows matches commit order. Exercise via `psycopg` explicit transactions + holding one open while the second attempts its UPDATE; second blocks on advisory lock until first commits.

### `tests/test_api_alerts.py` — endpoint tests (extend existing file)

Existing file already covers guard-rejection + position-alert endpoints for this module. Add a `class TestCoverageStatusDropsEndpoint` alongside:

- `GET` returns drops, `unseen_count`, `alerts_last_seen_coverage_event_id`.
- `GET` filters to `old_status = 'analysable'` — promotions (`insufficient → analysable`) excluded.
- `GET` excludes first-audit rows (NULL → terminal).
- `GET` respects 7-day window (rows older than 7d not returned).
- `GET` orders by `event_id DESC`; caps at LIMIT 500.
- `/seen` cursor idempotency (replaying same event_id is no-op via GREATEST).
- `/seen` non-regression (sending smaller seen_through_event_id does not move cursor backward).
- `/seen` on empty window + NULL cursor is no-op (cursor stays NULL — validates no #395 divergence).
- `/dismiss-all` advances cursor to MAX in-window drop event_id.
- `/dismiss-all` empty-window no-op.
- All endpoints require auth (session-or-service-token).

### Test DB isolation

All destructive tests run against `ebull_test` per memory `feedback_test_db_isolation`. Never `settings.database_url`. Pattern per `tests/test_operator_setup_race.py`.

### No mocked-cursor tests

Trigger is DB-native. Mock-cursor tests would lie. Real-DB integration only.

### Smoke gate

`tests/smoke/test_app_boots.py` — no change. Migration 047 runs via existing apply path in lifespan; smoke test exercises it.

## Pre-push checklist

Matches CLAUDE.md. All four must pass before push:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Frontend not touched — no `pnpm --dir frontend` needed this PR.

## PR description skeleton

Title: `feat(#397): coverage status transition log`

Body:

> **What**
>
> - New migration `sql/047_coverage_status_events.sql` — append-only event table, partial index on drops-from-analysable, row-level AFTER UPDATE trigger on `coverage.filings_status`, operator cursor column.
> - Three routes in `app/api/alerts.py` — `GET /alerts/coverage-status-drops`, `POST /alerts/coverage-status-drops/seen`, `POST /alerts/coverage-status-drops/dismiss-all`. Mirror position-alerts pattern (#396).
> - CLAUDE.md polish — step 3 indent fix bundled in (pending from 2026-04-06).
>
> **Why**
>
> - Prereq for #399 (AlertsStrip wire-up). Completes the third alert feed; guard-rejection + position-alert shipped in #394 + #401.
>
> **Test plan**
>
> - Migration assertions test (columns, FK, partial-index predicate, trigger).
> - Trigger behaviour test (transitions + no-ops + unrelated-column UPDATE).
> - Endpoint tests extending `tests/test_api_alerts.py`.
>
> **Called out**
>
> - `instrument_id` FK has no `ON DELETE CASCADE` — instrument delete blocks if history exists. Deliberate; instrument deletes are rare and manual.
> - Trigger takes `pg_advisory_xact_lock(hashtext('coverage_status_events_writer'))` to serialize concurrent coverage writers. Matches #396's single-writer prerequisite. Cheap (one lock per txn; stacking harmless).
> - Event log covers UPDATE transitions only — INSERT-created rows (`seed_coverage`, `bootstrap_missing_coverage_rows`) don't fire the trigger. Orthogonal to drops-from-analysable scope (no INSERT path writes `'analysable'`). Documented in spec for future full-history UI needs.
> - Inherits #395 multi-query GET snapshot drift; see spec edge-case table.

## Bundled polish — CLAUDE.md indent

`.claude/CLAUDE.md` `## Branch and PR workflow` step 3 — indent prose block under step 3 by 3 spaces so steps 4-7 render as siblings, not collapsed. Per memory `project_pending_polish.md`. No content change.
