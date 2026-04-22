# Position-alert event persistence (#396)

**Date:** 2026-04-21
**Issue:** [#396](https://github.com/Luke-Bradford/eBull/issues/396) — position-alert event persistence (enables thesis/SL/TP strip row)
**Parent plan:** [docs/superpowers/plans/2026-04-18-product-visibility-pivot.md](../plans/2026-04-18-product-visibility-pivot.md)
**Predecessor:** PR #394 (guard-rejection alerts strip) — established the cursor, window, and response-envelope patterns this spec mirrors.
**Peer:** #397 (coverage-status transition log) — independent, runs in parallel; both feed the strip extension in #399.

---

## Context

[`position_monitor.check_position_health`](../../app/services/position_monitor.py) runs hourly from [`monitor_positions_job`](../../app/workers/scheduler.py) (`JOB_MONITOR_POSITIONS`). It evaluates every open position for three breach types — `sl_breach`, `tp_breach`, `thesis_break` — and returns a `MonitorResult` tuple that is currently **only logged to stderr**. No DB rows, no history, no "since last visit" semantics.

The dashboard alerts strip shipped in #394 displays guard rejections with a seven-day window and a BIGSERIAL cursor on [`operators.alerts_last_seen_decision_id`](../../sql/044_operators_alerts_seen.sql). This spec defines the parallel persistence and read-path layer for position alerts so #399 can render them in the same strip.

Scope intentionally matches #394's surface — same window, same clamp-and-greatest cursor semantics, same 500-row cap, same silent-on-error frontend contract. Divergences are called out where the episode model requires them.

---

## Scope

**In:**
- New migration `sql/045_position_alerts.sql`: `position_alerts` table + partial unique index + partial query index + new operator cursor column.
- Writer: new function `persist_position_alerts(conn, result)` in `app/services/position_monitor.py`, invoked from `monitor_positions_job` after `check_position_health`.
- Read API: `GET /alerts/position-alerts`, `POST /alerts/position-alerts/seen`, `POST /alerts/position-alerts/dismiss-all` — added to `app/api/alerts.py` alongside the existing guard-rejection endpoints.
- Tests: unit tests for `persist_position_alerts` diff logic; API tests mirroring `tests/test_api_alerts.py`; smoke-test pass of the writer via scheduler invocation.

**Out (explicit non-assertions):**
- Frontend integration into `AlertsStrip.tsx` — deferred to #399.
- Retention pruning job — 7-day strip window is enforced at read time; row purging is deferred until volume forces it.
- Per-operator alert preferences or mute lists.
- New breach types beyond the three already emitted by `check_position_health`.
- Changes to `check_position_health` return shape or breach logic.
- Changes to `monitor_positions_job` scheduling or job-tracking surface.
- Operator actions on alerts beyond cursor ack (no "create EXIT from alert", no dismiss-one).
- Unified cross-source alerts endpoint (a design-decision for #399).

---

## Design decisions (resolved in brainstorm)

| Decision | Choice | Why |
|---|---|---|
| Dedupe policy | **Transition-only episodes** — one row per breach onset, `resolved_at` flipped when the breach clears. | Matches `recommendation persistence — do not spam identical HOLD rows every run` from settled-decisions. Hourly still-breaching evaluations stay silent; operator isn't paged repeatedly for the same signal. |
| Retention window | **7 days** (same as guard rejections). | Strip mental model is single: "last 7 days of things that needed you". Transition-only keeps volume tiny either way. |
| Cursor shape | **New column `alerts_last_seen_position_alert_id BIGINT`** on `operators`, parallel to existing guard cursor. | Mirrors #394 exactly; avoids the `decision_time`-race #394 already solved; #399 unions three independent feeds each with its own BIGSERIAL cursor. |
| Transition detection | **Episode diff with `resolved_at`.** At-most-one-open-row-per-(instrument, alert_type) invariant enforced by partial unique index. Insert on current-breach-with-no-open-episode; UPDATE `resolved_at = now()` on no-current-breach-with-open-episode; no-op on the other two combinations. | Single atomic transaction; partial unique index blocks concurrent-writer races; crash-safe (process restart re-derives state from DB). |
| Strip visibility | **Open episodes AND resolved episodes within the 7-day window**, ordered by `alert_id DESC`. | Matches guard-rejection mental model (shows historical FAILs, not just "currently failing"). Transition-only bounds volume. |
| Cursor target | **Insert row `alert_id` only.** Resolve updates are not tracked by the cursor. | Ack means "I've seen the onset of this breach". A resolve annotation on a row the operator already ack'd is not a new event; showing the resolved-at timestamp on a historically-seen row is expected, not a surprise. |

---

## Schema

New migration `sql/045_position_alerts.sql`:

```sql
-- Migration 045: position-alert episode persistence + operator read cursor
--
-- 1. position_alerts — one row per breach EPISODE (not per hourly evaluation).
--    opened_at = onset detection time. resolved_at = clearance detection time
--    (NULL while still breaching). alert_id is BIGSERIAL for strict-> cursor
--    semantics mirroring operators.alerts_last_seen_decision_id (#394 rationale).
-- 2. Partial unique index enforces at-most-one-open-episode per (instrument_id,
--    alert_type). The writer's INSERT path relies on this as the concurrency
--    backstop — without it, two overlapping monitor_positions_job invocations
--    could both observe "no open episode" and double-insert. Single-threaded
--    scheduler makes overlap unlikely; the constraint is the safety net.
-- 3. idx_position_alerts_recent on alert_id DESC supports the strip scan in
--    GET /alerts/position-alerts (ORDER BY alert_id DESC + LIMIT 500).
--    No WHERE predicate: partial-index predicates must be IMMUTABLE in
--    PostgreSQL, and now()-based predicates use a STABLE function which
--    would be rejected. Transition-only writes keep the table small enough
--    that a full index on alert_id DESC is cheap.
-- 4. operators.alerts_last_seen_position_alert_id — parallel cursor to the
--    existing alerts_last_seen_decision_id column. NULL = never acknowledged.

CREATE TABLE IF NOT EXISTS position_alerts (
    alert_id      BIGSERIAL    PRIMARY KEY,
    instrument_id BIGINT       NOT NULL REFERENCES instruments(instrument_id),
    alert_type    TEXT         NOT NULL
                               CHECK (alert_type IN ('sl_breach', 'tp_breach', 'thesis_break')),
    opened_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ  NULL,
    detail        TEXT         NOT NULL,
    current_bid   NUMERIC      NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_position_alerts_open
    ON position_alerts (instrument_id, alert_type)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_position_alerts_recent
    ON position_alerts (alert_id DESC);

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_position_alert_id BIGINT;
```

### Scheduler serialization (cursor-race + resolves-race rebuttal)

Both the `alert_id` cursor semantics and the writer's read-then-diff-then-write flow depend on **single-threaded writer** invariants. Those invariants are guaranteed by the existing job runtime:

- [`app/jobs/runtime.py:224`](../../app/jobs/runtime.py) — `self._inflight: dict[str, threading.Lock] = {name: threading.Lock() for name in self._invokers}` — per-job in-process lock.
- [`app/jobs/runtime.py:243`](../../app/jobs/runtime.py) — APScheduler `max_instances=1` per job — scheduler-level overlap prevention.
- Comment at [`app/jobs/runtime.py:237-244`](../../app/jobs/runtime.py) names the intent explicitly: "advisory lock is the source of truth for serialisation; this is a defensive second layer".

Under this model:

1. **BIGSERIAL commit-order race** (Codex #2 — sequence allocated pre-commit, so concurrent writers can commit out-of-order and leave lower `alert_id` rows to commit *after* the cursor has advanced past them): cannot occur. Only one `monitor_positions_job` invocation runs at a time; sequence values are allocated and committed in the same serial order. This mirrors the rationale from [`sql/044_operators_alerts_seen.sql`](../../sql/044_operators_alerts_seen.sql) comment L7-9 for `decision_id`: "decision_id is monotonic for the guard stage (single-threaded scheduler invocations; no concurrent writers), so a strict `>` comparison fully closes the race".
2. **Writer resolves race** (Codex #3 — stale run reads an open episode, decides "no longer breaching", UPDATEs `resolved_at` while a fresher run would have kept it open): cannot occur. Only one invocation of `persist_position_alerts` is running at a time. If a future reader needs parallelism here, add `pg_try_advisory_xact_lock(<position_monitor_lock_key>)` at the top of the writer's transaction and bail on `false`.

If and when the scheduler model changes (autoscaled workers, multi-process deployment), both invariants must be re-validated. Add a pointer to this paragraph from any future multi-worker spec.

Notes:
- `alert_type` CHECK mirrors [`position_monitor.AlertType`](../../app/services/position_monitor.py) `Literal["sl_breach", "tp_breach", "thesis_break"]` exactly. Prevention: `New TEXT columns in migrations need CHECK constraints or Literal types`.
- `opened_at` defaults to DB `now()` on INSERT — writer does not pass an app-side timestamp. Prevention: `datetime.now(UTC)` vs DB `now()` in freshness windows.
- `resolved_at` is UPDATE-only; writer never inserts a row with `resolved_at` already populated.
- No `ON DELETE CASCADE` from `instruments` — prevention: `ON DELETE CASCADE on *_audit / *_log tables destroys forensic history`. Manual purge is the retention path.
- Partial index `WHERE opened_at >= now() - INTERVAL '7 days'` uses an immutable-at-definition interval; the predicate is evaluated at index maintenance time per row, so aged-out rows fall out of the index automatically. This matches the pattern from #394's `idx_decision_audit_guard_failed_recent`.
- No backfill. Pre-existing logged breaches stay unrecorded; the writer starts fresh on the first scheduler tick after migration.

---

## Writer: `persist_position_alerts`

New function in [`app/services/position_monitor.py`](../../app/services/position_monitor.py), called from `monitor_positions_job` immediately after `check_position_health`:

```python
def persist_position_alerts(
    conn: psycopg.Connection[Any],
    result: MonitorResult,
) -> PersistStats:
    """Reconcile open breach episodes against the current MonitorResult.

    Contract: for each (instrument_id, alert_type) pair:
      - current breach AND no open episode   -> INSERT new row
      - current breach AND open episode      -> no-op (still breaching)
      - no current breach AND open episode   -> UPDATE resolved_at = now()
      - no current breach AND no open episode -> no-op

    All four branches execute inside a single conn.transaction() block.
    Returns PersistStats(opened, resolved, unchanged) for the scheduler log line.
    """
```

Return type:

```python
@dataclass(frozen=True)
class PersistStats:
    opened: int       # new episodes inserted
    resolved: int     # existing episodes closed
    unchanged: int    # still-open episodes left alone
```

Algorithm (inside one `with conn.transaction():` block — prevention: `Read-then-write cap enforcement outside transaction`, `conn.transaction() without conn.commit() silently rolls back in psycopg v3`):

1. **Read current open episodes.**
   ```sql
   SELECT instrument_id, alert_type
   FROM position_alerts
   WHERE resolved_at IS NULL;
   ```
   Returned set: `OPEN = { (instrument_id, alert_type) -> row is open }`.

2. **Build current-breach set from `result.alerts`:**
   `CURRENT = { (a.instrument_id, a.alert_type): a for a in result.alerts }`. Invariant: `check_position_health` emits at most one alert per (instrument, alert_type) per invocation (see loop at [`app/services/position_monitor.py:123-171`](../../app/services/position_monitor.py) — each `if` branch pushes one alert per row, rows are per-instrument).

3. **Diff:**
   - `TO_OPEN = CURRENT.keys() - OPEN` — breaches without an open episode.
   - `TO_RESOLVE = OPEN - CURRENT.keys()` — open episodes whose breach has cleared.
   - Intersection → no-op.

4. **INSERT for each `(instrument_id, alert_type)` in `TO_OPEN`:**
   ```sql
   INSERT INTO position_alerts (instrument_id, alert_type, detail, current_bid)
   VALUES (%(instrument_id)s, %(alert_type)s, %(detail)s, %(current_bid)s)
   ON CONFLICT (instrument_id, alert_type) WHERE resolved_at IS NULL DO NOTHING;
   ```
   `detail` and `current_bid` come from the corresponding `MonitorAlert`. `opened_at` defaults to DB `now()` — not supplied by writer. `ON CONFLICT ... DO NOTHING` is belt-and-braces against concurrent writer races; primary correctness comes from the partial unique index.

5. **UPDATE for each `(instrument_id, alert_type)` in `TO_RESOLVE`:**
   ```sql
   UPDATE position_alerts
   SET resolved_at = now()
   WHERE instrument_id = %(instrument_id)s
     AND alert_type = %(alert_type)s
     AND resolved_at IS NULL;
   ```
   Scoped with `resolved_at IS NULL` guard so a row resolved between step 1 and step 5 isn't touched (prevention: `Single-row UPDATE silent no-op on missing row` — here the no-op is correct, not a bug).

6. **Commit** via the `conn.transaction()` context manager exiting cleanly. Counts accumulated into `PersistStats` before return.

**Scheduler integration** — [`app/workers/scheduler.py`](../../app/workers/scheduler.py) `monitor_positions_job`:

```python
with psycopg.connect(settings.database_url) as conn:
    result = check_position_health(conn)
    stats = persist_position_alerts(conn, result)

# existing stderr logging preserved; add one stats line:
logger.info(
    "monitor_positions: %d checked, episodes: +%d opened / -%d resolved / %d unchanged",
    result.positions_checked, stats.opened, stats.resolved, stats.unchanged,
)
```

`persist_position_alerts` must not raise on empty `result.alerts` (idempotent no-op when both `CURRENT` and `TO_RESOLVE` are empty AND `OPEN` is empty). Exception escalation: writer failure **must** keep `tracker.row_count = result.positions_checked` — the tracked-job surface already records the check count separately from alert persistence. An exception from the writer rolls back the transaction and propagates out; the existing `except Exception` in `monitor_positions_job` already handles this path (prevention: `Early return inside context-managed tracking without row_count`).

**Connection ownership** — `check_position_health` and `persist_position_alerts` share the same connection opened by the scheduler. `persist_position_alerts` must **not** call `conn.commit()` itself when the caller is managing the transaction (prevention: `Mid-transaction conn.commit() in service functions that accept a caller's connection`). The `with conn.transaction()` context inside `persist_position_alerts` creates a savepoint-like block that commits on exit *only if no outer transaction is active*; since `psycopg.connect(...)` enters autocommit-off and `check_position_health` runs a read with no BEGIN, the `conn.transaction()` block inside the writer IS the outer transaction. Document this in the function docstring so a future caller wrapping it in their own transaction doesn't double-commit.

---

## Read API

All three endpoints added to the existing [`app/api/alerts.py`](../../app/api/alerts.py) router under the same `/alerts` prefix. Auth and operator-resolution mirror the existing guard-rejection endpoints exactly (`require_session_or_service_token` + `sole_operator_id`, 503/501 mapping for `NoOperatorError` / `AmbiguousOperatorError`).

### `GET /alerts/position-alerts`

**Response:**

```python
class PositionAlert(BaseModel):
    alert_id: int
    alert_type: Literal["sl_breach", "tp_breach", "thesis_break"]
    instrument_id: int
    symbol: str
    opened_at: datetime
    resolved_at: datetime | None
    detail: str
    current_bid: Decimal | None

class PositionAlertsResponse(BaseModel):
    alerts_last_seen_position_alert_id: int | None
    unseen_count: int
    alerts: list[PositionAlert]
```

`instrument_id` and `symbol` are NOT nullable — `position_alerts.instrument_id` has a NOT NULL FK and is always resolvable. Divergence from the guard-rejection response where nullability is required. `alert_type` is a closed `Literal` union — prevention: `Loose 'string' on API response fields that mirror backend Literal types`.

**List query:**

```sql
SELECT
    pa.alert_id,
    pa.alert_type,
    pa.instrument_id,
    i.symbol,
    pa.opened_at,
    pa.resolved_at,
    pa.detail,
    pa.current_bid
FROM position_alerts pa
JOIN instruments i ON i.instrument_id = pa.instrument_id
WHERE pa.opened_at >= now() - INTERVAL '7 days'
ORDER BY pa.alert_id DESC
LIMIT 500;
```

- Inline `INTERVAL '7 days'` literal — prevention: `Interval construction via string concatenation in SQL`.
- Window filter on `opened_at`, not `resolved_at`. An episode that opened six days ago and resolved two hours ago is still in-window; an episode that opened nine days ago and is still unresolved falls OUT of window (edge case — accepted: if a breach has been open nine days, the strip isn't the right surface, the position page is).
- `JOIN instruments` is INNER because FK is NOT NULL — no LEFT JOIN indirection. Prevention: `JOIN fan-out inflates aggregates` N/A because `instruments.instrument_id` is unique.
- `ORDER BY alert_id DESC` is the stable PK sequence; the `rejections[0].alert_id === MAX(alert_id)` invariant the cursor depends on holds without clock-skew concerns (unlike `decision_time` in #394).
- `LIMIT 500` matches #394's cap. Prevention: `Unbounded API limit parameters`.

**`unseen_count` query** (separate; reflects true in-window unseen count regardless of the 500-row cap):

```sql
SELECT COUNT(*) AS unseen_count
FROM position_alerts
WHERE opened_at >= now() - INTERVAL '7 days'
  AND (%(last_id)s::BIGINT IS NULL OR alert_id > %(last_id)s::BIGINT);
```

Strict `>` — ties are structurally impossible because `alert_id` is a unique BIGSERIAL. Prevention: `Shared cursor across unrelated queries` — the two queries share a dict cursor but no `%(last_id)s` placeholder is reused across them with different semantics; each query gets its own `.execute()` with a fresh params dict.

### `POST /alerts/position-alerts/seen`

**Body:**

```python
class PositionAlertsMarkSeenRequest(BaseModel):
    seen_through_position_alert_id: int = Field(gt=0)
```

**Write:**

```sql
UPDATE operators AS op
SET alerts_last_seen_position_alert_id = GREATEST(
    COALESCE(op.alerts_last_seen_position_alert_id, 0),
    LEAST(%(seen_through_position_alert_id)s, m.max_id)
)
FROM (
    SELECT MAX(alert_id) AS max_id
    FROM position_alerts
    WHERE opened_at >= now() - INTERVAL '7 days'
) AS m
WHERE op.operator_id = %(op)s
  AND m.max_id IS NOT NULL;
```

`LEAST` clamp bounds ack to the current in-window MAX — prevents a buggy or malicious client from advancing the cursor past future un-arrived rows. `GREATEST` + `COALESCE(..., 0)` preserves monotonicity (never rewinds; handles NULL-initial). The `AND m.max_id IS NOT NULL` guard makes the UPDATE a no-op when the window is empty — without it, the subselect would return NULL → `LEAST(client, NULL) = NULL` OR (depending on COALESCE positioning) `GREATEST(0, 0) = 0` writes `0` as a fake cursor. Empty window + client POST should leave cursor unchanged (divergence from #394's `/alerts/seen` — that endpoint writes `0` on the same edge; tracked as a separate consistency fix, not in scope for this PR). Response: `204 No Content`.

### `POST /alerts/position-alerts/dismiss-all`

**Body:** empty.

**Write:**

```sql
UPDATE operators AS op
SET alerts_last_seen_position_alert_id = GREATEST(
    COALESCE(op.alerts_last_seen_position_alert_id, 0),
    m.max_id
)
FROM (
    SELECT MAX(alert_id) AS max_id
    FROM position_alerts
    WHERE opened_at >= now() - INTERVAL '7 days'
) AS m
WHERE op.operator_id = %(op)s
  AND m.max_id IS NOT NULL;
```

Empty-window no-op (when no rows in window, `m.max_id IS NULL` excludes the UPDATE target row → cursor unchanged). Atomic — a row inserted between the subselect and the UPDATE commit has a larger `alert_id` and stays unseen on next GET. Response: `204 No Content`.

### GET snapshot consistency (deferred)

The GET handler runs three separate statements (cursor read, count, list) against the same `psycopg.Connection` without wrapping them in a single REPEATABLE READ snapshot. Under `READ COMMITTED`, an insert committed between statements can make `unseen_count`, `alerts_last_seen_position_alert_id`, and the returned `alerts` list reflect different moments in time. The practical symptom is a one-refresh stale count or a "newer than the cursor but missing from the list" row — which self-heals on the next GET.

This is a known tech-debt shared with #394's `/alerts/guard-rejections` and is tracked against [#395](https://github.com/Luke-Bradford/eBull/issues/395) (tech-debt: dashboard reads — snapshot consistency on multi-query handlers). Deferred for consistency with the existing pattern; fixing it here without fixing #394's identical handler would leave the repo half-converted. When #395 is picked up, both handlers get the REPEATABLE READ wrapper in one pass.

### Error semantics

- Endpoint failures should not 500 the dashboard; the frontend renders null on error (#399 extends the existing silent-on-error policy to the new data source). Backend still returns real 500s on internal errors.
- Partial index + 7-day window keeps GET latency <200ms.

---

## Tests

### Unit: `persist_position_alerts` (`tests/test_position_monitor.py`, extending existing file)

Fixtures use the `ebull_test` test DB (prevention: `Tests must never wipe the dev DB`).

1. **Empty + empty** — no open episodes in DB, no current alerts → `PersistStats(0, 0, 0)`; zero rows after.
2. **New breach → opens episode** — no open episodes, `result.alerts = [sl_breach on instrument A]` → one INSERT; `PersistStats(1, 0, 0)`; table row has `resolved_at IS NULL`, `opened_at` set by DB, `detail` copied from alert.
3. **Still breaching → no-op** — one open episode for A/sl_breach, same alert in `result.alerts` → `PersistStats(0, 0, 1)`; row count unchanged; `opened_at` unchanged; no new row.
4. **Clearance → resolves episode** — one open episode for A/sl_breach, `result.alerts = []` → `PersistStats(0, 1, 0)`; row's `resolved_at` populated with DB `now()`; no new rows.
5. **Re-breach after clearance → new episode** — one resolved episode exists (prior `resolved_at` populated); `result.alerts = [sl_breach on A]` → `PersistStats(1, 0, 0)`; two rows in table, one with `resolved_at` set, one with `resolved_at IS NULL`.
6. **Mixed across alert types** — A has open `sl_breach`; `result.alerts = [tp_breach on A, thesis_break on B]` → `PersistStats(2, 1, 0)` (two opens, one resolve because A's sl_breach is no longer current).
7. **Mixed across instruments** — A has open sl_breach; B has open sl_breach; `result.alerts = [sl_breach on A, sl_breach on C]` → `PersistStats(1, 1, 1)`.
8. **ON CONFLICT guard** — simulate a racing second writer by pre-inserting an open row for A/sl_breach inside the same connection; call `persist_position_alerts` with the same alert → no duplicate row (ON CONFLICT DO NOTHING); `PersistStats.opened == 0`, `unchanged == 1`. Note: this test asserts the index/constraint safety net, not normal-path correctness.
9. **All three alert types for same instrument** — `result.alerts = [sl_breach, tp_breach, thesis_break all on A]`, no open episodes → three INSERTs; three open rows after (partial unique index is keyed on `(instrument_id, alert_type)` — three types are three distinct keys).
10. **`current_bid` NULL passes through** — `result.alerts = [thesis_break with current_bid=None]` (thesis-break path doesn't require a bid) → INSERT succeeds with NULL bid column.

### API: `tests/test_api_alerts.py` (extending existing file)

1. **Empty state** — no rows → `{ alerts: [], unseen_count: 0, alerts_last_seen_position_alert_id: null }`.
2. **Seven-day window inclusion** — insert rows at `now() - interval '6 days'` (opened_at) and `now() - interval '8 days'` (opened_at); only the 6-day row returned.
3. **500-row cap** — insert 510 open rows within window, cursor NULL → `alerts.length === 500`, `unseen_count === 510`.
4. **`unseen_count` anchor** — cursor set mid-window, verify strict `>` count.
5. **`unseen_count` NULL cursor** — all in-window rows counted.
6. **Resolved episodes included** — insert a row with `opened_at` at 6 days ago AND `resolved_at` at 2 hours ago; it appears in GET response with `resolved_at` populated.
7. **Out-of-window by opened_at** — insert row with `opened_at = now() - '9 days'` and `resolved_at = NULL` → excluded (window is keyed on `opened_at`, deliberate).
8. **Ordering** — insert rows with `alert_id` 50 and 51 where row 50 has `opened_at = now()` and row 51 has `opened_at = now() - '1 minute'` → `alerts[0].alert_id === 51` (ORDER BY `alert_id DESC`, not `opened_at`).
9. **POST /seen monotonic** — cursor = 1000, POST `seen_through_position_alert_id = 500` → unchanged.
10. **POST /seen first time** — cursor NULL, POST 1000 → set to 1000.
11. **POST /seen missing field → 422**.
12. **POST /seen non-integer → 422**.
13. **POST /seen clamped to in-window MAX** — window MAX = 200, cursor NULL, POST 99999 → cursor set to 200.
14. **POST /seen race safety** — insert row 101; POST `seen_through_position_alert_id = 100`; next GET shows `unseen_count === 1`.
15. **POST /dismiss-all advances to MAX** — rows 100/200/300 in window, cursor NULL → POST → cursor 300.
16. **POST /dismiss-all monotonic** — cursor = 500, MAX-in-window = 300 → unchanged at 500.
17. **POST /dismiss-all race safety** — cursor NULL, rows 100 and 200 present, POST dismiss-all, THEN insert row 201 → next GET `unseen_count === 1`.
18. **POST /dismiss-all empty window** — no rows, cursor NULL → unchanged at NULL.
19. **POST /dismiss-all empty window with cursor** — cursor = 500, no rows in window → unchanged at 500.
20. **`sole_operator_id` errors** — no operator → 503, multiple operators → 501, on all three endpoints.
21. **Alert-type round-trip** — each of the three literal values round-trips through the API without coercion.
22. **Operator cursor isolated from guard cursor** — POST /alerts/seen (guard path) does not touch `alerts_last_seen_position_alert_id`; POST /alerts/position-alerts/seen does not touch `alerts_last_seen_decision_id`. Pin this explicitly — two cursors on the same row are easy to cross-wire.

### Smoke

`tests/smoke/test_app_boots.py` already boots the full app via `TestClient` against dev DB. No change needed — new endpoints come up alongside the existing `/alerts/*` router, router registration is additive.

---

## Prevention entries consulted

| Entry | How applied |
|---|---|
| `datetime.now(UTC)` vs DB `now()` in freshness windows | All timestamps (`opened_at`, `resolved_at`, window filter, cursor MAX subselect) use DB `now()`. |
| Interval construction via string concatenation in SQL | `INTERVAL '7 days'` literal everywhere; no concatenation. |
| Shared column vocabulary mismatch across stages | `alert_type` values match `position_monitor.AlertType` Literal exactly; CHECK constraint pins the vocabulary in the DB. |
| Loose `string` on API response fields that mirror backend `Literal` | Pydantic model uses `Literal["sl_breach", "tp_breach", "thesis_break"]`. |
| New TEXT columns in migrations need CHECK constraints or Literal types | `alert_type` has CHECK; Pydantic Literal mirrors. |
| Unbounded API limit parameters | 500-row cap hardcoded server-side. |
| JOIN fan-out inflates aggregates | INNER JOIN on unique FK; no aggregate queries at risk. |
| `conn.transaction()` without `conn.commit()` silently rolls back in psycopg v3 | Writer uses `with conn.transaction()` ; outer scheduler connection is not in an existing transaction; the block IS the outer transaction and commits on clean exit. Documented in function docstring. |
| Mid-transaction `conn.commit()` in service functions that accept a caller's connection | Writer never calls `conn.commit()` directly; caller manages or the `with conn.transaction()` block handles it. |
| Read-then-write cap enforcement outside transaction | Diff read + insert/update in one `with conn.transaction()` block. |
| Shared cursor across unrelated queries | Each query gets its own `.execute()` with a fresh params dict. |
| Single-row UPDATE silent no-op on missing row | UPDATE on resolve path includes `resolved_at IS NULL` guard; silent no-op IS correct behaviour if row was resolved between read and UPDATE. |
| ON CONFLICT DO NOTHING counter overcount | Writer uses partial-index ON CONFLICT DO NOTHING + counts from the SQL RETURNING or `rowcount`; tests pin that conflict path yields `opened=0`. |
| ON DELETE CASCADE on `*_audit` / `*_log` tables destroys forensic history | `position_alerts.instrument_id` FK has no ON DELETE CASCADE. |
| Naive datetime in TIMESTAMPTZ query params | Cursor is BIGINT `alert_id`; no timestamp query params. |
| Early return inside context-managed tracking without `row_count` | `monitor_positions_job` integration keeps `tracker.row_count = result.positions_checked` on the error path and success path. |
| Unbounded enum filters accept nonsense values silently | GET takes no enum filter params (fixed window, no user-controlled filters). |
| Frontend async render-surface isolation | Out of scope (deferred to #399), but backend honours its side of the contract by returning proper error codes, not silent empty lists. |

## Settled decisions consulted

| Decision | How applied |
|---|---|
| Auditability — persist structured evidence where it matters | Breach episodes now have DB history, not just stderr. |
| Recommendation persistence is append-oriented, do not spam identical HOLD rows | Transition-only episode model is the parallel rule for alert persistence. |
| Product-visibility pivot — prioritise operator visibility over infra work | This spec is a direct descendant of the #315 strip work; position alerts are the single-biggest missing signal from the dashboard. |

---

## Definition of done

1. Migration `sql/045_position_alerts.sql` applied cleanly against `ebull` and `ebull_test`.
2. `persist_position_alerts` writer plus scheduler integration land in one PR with unit tests and API tests.
3. `GET /alerts/position-alerts`, `POST /alerts/position-alerts/seen`, `POST /alerts/position-alerts/dismiss-all` all return the shapes and semantics documented above.
4. `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`, `uv run pytest` all pass.
5. `tests/smoke/test_app_boots.py` passes (no frontend changes — no frontend test suite delta required).
6. Codex pre-spec review complete (this document); Codex pre-plan review before first task dispatch; Codex pre-push review before the first `git push`.
7. PR description follows `feedback_pr_description_brevity.md` — title `feat(#396): position-alert event persistence`; body: What / Why / Test plan bullets only.
8. On merge: close #396. #397 and #399 remain open; #399 now has one of its two preconditions met.
