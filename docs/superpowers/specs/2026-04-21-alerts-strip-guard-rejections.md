# Alerts strip — guard rejections (#315 Phase 3)

**Date:** 2026-04-21
**Issue:** #315 (P1-3 Dashboard as command center), final phase
**Plan:** [docs/superpowers/plans/2026-04-18-product-visibility-pivot.md](../plans/2026-04-18-product-visibility-pivot.md) line 49
**Prior phases merged:** PR #387 (Phase 1 — demote admin + Needs action), PR #388 (Phase 2 — rolling P&L pills)

---

## Context

Plan scope for #315 names three alert sources: **thesis breaches**, **filings-status drops from analysable**, **execution guard rejections since last visit**. Current state differs per source:

| Source | Current state | Persistence |
|---|---|---|
| Guard rejections | `decision_audit` written on every guard invocation ([app/services/execution_guard.py:587](../../app/services/execution_guard.py) `_write_audit`, rows since migration `sql/010_execution_guard.sql`) | Full history with `decision_time`, `pass_fail`, `explanation`, `evidence_json`, `recommendation_id` |
| Thesis breaches | `position_monitor.check_position_health` ([app/services/position_monitor.py:64](../../app/services/position_monitor.py)) runs hourly via `monitor_positions_job` ([app/workers/scheduler.py:2128](../../app/workers/scheduler.py)); results **only logged** | No DB rows |
| Filings-status drops | `coverage.filings_status` current-label column ([sql/036_coverage_filings_status.sql](../../sql/036_coverage_filings_status.sql)); `filings_audit_at` timestamp | No transition log |

Phase 3 ships **guard rejections only**. Thesis + filings require separate event-persistence work each smelling like its own ticket.

## Follow-ups (filed at merge)

- **#394** — position-alert event persistence: `position_alerts` table, `position_monitor` writer, dedupe policy, retention
- **#395** — coverage status transition log: `coverage_status_events` table, audit-path writer, retention
- **#396** — extend alerts strip to position + filings events (type-union addition on top of this PR's component)

Closing #315 is conditional on this PR landing. Follow-ups rejoin backlog under the product-visibility-pivot plan.

---

## Scope

**In:**
- Schema: `operators.alerts_last_seen_decision_id BIGINT NULL`; partial index on `decision_audit (decision_time DESC) WHERE pass_fail = 'FAIL' AND stage = 'execution_guard'`.
- Backend: `GET /alerts/guard-rejections` (7-day window, 500-row cap), `POST /alerts/seen` (normal-path ack via `seen_through_decision_id`), `POST /alerts/dismiss-all` (overflow-path ack, advances cursor to MAX in window).
- Frontend: `frontend/src/api/alerts.ts`, `frontend/src/components/dashboard/AlertsStrip.tsx`, wired into `DashboardPage.tsx` between `RollingPnlStrip` and Positions.
- Tests: backend API tests; frontend component tests.

**Out (explicit non-assertions):**
- Position alerts — deferred to #394.
- Filings-status drops — deferred to #395.
- Strip wiring for #394/#395 — deferred to #396.
- Dismissing individual rows (only "mark all read").
- Email / push / external notifications.
- Per-operator alert preferences or filters.
- Historical archive UI beyond the 7-day window.
- Changes to `decision_audit` schema or guard write path.

---

## Schema

New migration `sql/044_operators_alerts_seen.sql`:

```sql
-- Migration 044: operator-scoped alert acknowledgement + guard-rejection read index
--
-- 1. operators.alerts_last_seen_decision_id — NULL = never acknowledged (all in-window rows unseen).
--    Integer cursor keyed off decision_audit.decision_id (BIGSERIAL, unique).
--    Timestamp-based cursor was rejected: decision_time is TIMESTAMPTZ (microsecond resolution,
--    NOT unique under load), which leaves a tie-break race where a row inserted between GET
--    and POST at the same decision_time as rejections[0] would be silently acked. decision_id
--    is monotonic for the guard stage (single-threaded scheduler invocations; no concurrent
--    writers), so a strict > comparison fully closes the race.
-- 2. Partial index on decision_audit supports the dashboard GET scan.
--    Narrowed to stage='execution_guard' + pass_fail='FAIL' because
--    (a) the /alerts endpoint filters on both, (b) other stages write to
--    decision_audit (e.g. order_execution, deferred_retry) and must not
--    be indexed as guard rejections.

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_decision_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_decision_audit_guard_failed_recent
    ON decision_audit (decision_time DESC)
    WHERE pass_fail = 'FAIL' AND stage = 'execution_guard';
```

Existing `pass_fail` convention (see [app/services/execution_guard.py:87](../../app/services/execution_guard.py) `Verdict = Literal["PASS", "FAIL"]`): uppercase. `stage` written via the `STAGE: str = "execution_guard"` constant at [app/services/execution_guard.py:70](../../app/services/execution_guard.py). `decision_id` is `BIGSERIAL PRIMARY KEY` on `decision_audit` (see [sql/001_init.sql](../../sql/001_init.sql) L195).

No backfill. No data migration.

---

## Backend

### `GET /alerts/guard-rejections`

**Module:** new file `app/api/alerts.py`. Router registered in [app/main.py](../../app/main.py) alongside existing `/alerts` consumers (none today — path is free, verified via grep).

**Auth:** `dependencies=[Depends(require_session_or_service_token)]` + `sole_operator_id(conn)` — matches [app/api/watchlist.py:32](../../app/api/watchlist.py).

**Operator-resolution errors** (propagated from `sole_operator_id`):
- `NoOperatorError` → HTTP 503 (installation incomplete)
- `AmbiguousOperatorError` → HTTP 501 (unsupported multi-operator state)

Same shape as existing watchlist / portfolio routes; do not invent new codes.

**Query:**

```sql
SELECT
    da.decision_id,
    da.decision_time,
    da.instrument_id,
    i.symbol,
    tr.action,
    da.explanation
FROM decision_audit da
LEFT JOIN instruments i ON i.instrument_id = da.instrument_id
LEFT JOIN trade_recommendations tr ON tr.recommendation_id = da.recommendation_id
WHERE da.pass_fail = 'FAIL'
  AND da.stage = 'execution_guard'
  AND da.decision_time >= now() - INTERVAL '7 days'
ORDER BY da.decision_id DESC
LIMIT 500;
```

Ordering is by `decision_id DESC` (the unique BIGSERIAL PK), **not** `decision_time DESC`. Guard audit rows use an app-supplied timestamp from `_utcnow()` ([app/services/execution_guard.py:609](../../app/services/execution_guard.py)), not DB `now()` — so a clock-skewed host could insert a row with a later `decision_id` but an earlier `decision_time`. Ordering on `decision_time` would push that row below older-`decision_id` rows and break the `rejections[0].decision_id === MAX(decision_id)` invariant the ack cursor depends on. `decision_time` is retained as the 7-day window filter (still monotonic-enough across a multi-day window that occasional backskew doesn't matter for filtering) and as the row-display timestamp, but not as a sort key.

**Why LIMIT 500, not 50**: the button text is "Mark all read", and `seen_through_decision_id` clears every row with `decision_id <= seen_through_decision_id`. If `unseen_count` exceeds what the operator saw, clicking the button would silently acknowledge rows the client never rendered. 500 is large enough to cover any plausible 7-day guard-rejection count (≈70/day is extreme) while still being a bounded scan over the partial index. **The frontend renders every row in the payload** (scrollable container, no client-side slice) so `rejections.length` == "rows the operator actually saw". **Safety rule**: the "Mark all read" button is hidden when `unseen_count > rejections.length` — only true when total unseen exceeds 500, i.e., the true overflow case. See the overflow-exit path under the frontend section.

`pass_fail` / `stage` values match guard writer exactly (see Schema section — prevention: shared column vocabulary mismatch across stages). `now()` is DB-side (prevention: `datetime.now(UTC)` vs DB `now()` in freshness windows). Interval literal inlined — no string concatenation (prevention: interval construction via SQL string concatenation).

**`unseen_count` query** (separate; reflects true 7-day window count even when list is capped at 500):

```sql
SELECT COUNT(*) AS unseen_count
FROM decision_audit
WHERE pass_fail = 'FAIL'
  AND stage = 'execution_guard'
  AND decision_time >= now() - INTERVAL '7 days'
  AND (%(last_id)s IS NULL OR decision_id > %(last_id)s);
```

`%(last_id)s` is the operator's current `alerts_last_seen_decision_id`. `decision_id > …` is strict — a row at the same `decision_id` cannot exist (primary key is unique), so ties are structurally impossible, closing the race documented in the Schema rationale.

**Response envelope:**

```json
{
  "alerts_last_seen_decision_id": 1200,
  "unseen_count": 3,
  "rejections": [
    {
      "decision_id": 1234,
      "decision_time": "2026-04-21T09:30:00+00:00",
      "instrument_id": 42,
      "symbol": "AAPL",
      "action": "BUY",
      "explanation": "FAIL — cash_available: need £200, have £50"
    }
  ]
}
```

**Nullability:** `instrument_id`, `symbol`, `action` all nullable (audit rows can be written without linked recommendation per [sql/010_execution_guard.sql](../../sql/010_execution_guard.sql) schema note).

**`action` type:** `Literal["BUY", "ADD", "HOLD", "EXIT"] | None`. Canonical action set from [app/api/recommendations.py:43](../../app/api/recommendations.py) and [app/services/portfolio.py:73](../../app/services/portfolio.py). Mirror on the frontend `Literal` union (prevention: Loose `string` on API response fields mirroring backend `Literal`). `HOLD` rows are included if guard-evaluated — Phase 3 does not pre-filter by action; operator can see the full failure set.

### `POST /alerts/seen` (normal path)

**Body:** `{ "seen_through_decision_id": 1234 }` (integer; matches `decision_audit.decision_id`).

**Semantics:** the `max(decision_id)` of the rejections in the preceding GET response. Client sends `rejections[0].decision_id`. The GET query orders by `decision_id DESC` (primary BIGSERIAL sequence), not `decision_time DESC`, so index 0 is deterministically MAX regardless of any app-side clock skew that might have produced a smaller `decision_time` for a larger `decision_id`. A row inserted between GET and POST has `decision_id > rejections[0].decision_id`, so the strict `>` comparison in the unseen query leaves it unseen (race-safe).

The "Mark all read" button is only rendered when `unseen_count > 0 && unseen_count <= rejections.length` (see Frontend section); if there are zero rejections, the strip itself is hidden, so the frontend never POSTs an empty body.

**Server-side validation + clamp:**

- `seen_through_decision_id` is a required positive integer; missing → 422, non-integer/non-positive → 422.
- Clamp to current in-window guard MAX — a buggy or malicious client posting a huge `decision_id` must not permanently advance the cursor past unseen future rows. The `LEAST(...)` in the UPDATE bounds the ack to rows that actually exist in the current 7-day guard-failure window.

**Write:**

```sql
UPDATE operators
SET alerts_last_seen_decision_id = GREATEST(
    COALESCE(alerts_last_seen_decision_id, 0),
    LEAST(
        %(seen_through_decision_id)s,
        COALESCE((
            SELECT MAX(decision_id)
            FROM decision_audit
            WHERE pass_fail = 'FAIL'
              AND stage = 'execution_guard'
              AND decision_time >= now() - INTERVAL '7 days'
        ), 0)
    )
)
WHERE operator_id = %(op)s;
```

Monotonic — never rewinds (`COALESCE(..., 0)` ensures `GREATEST` works when current value is NULL). Clamped by `LEAST` so a client cannot advance the cursor past the real data. A client replaying an older ack cannot regress the cursor.

**Response:** 204 No Content.

### `POST /alerts/dismiss-all` (overflow path)

**Body:** empty (no parameters).

**Semantics:** explicit operator acknowledgement that they are dismissing every in-window guard rejection — including rows the frontend never received (only possible when `unseen_count > 500`). Server atomically advances the cursor to the current `MAX(decision_id)` for guard-stage rows in the 7-day window.

**Write:**

```sql
UPDATE operators AS op
SET alerts_last_seen_decision_id = GREATEST(
    COALESCE(op.alerts_last_seen_decision_id, 0),
    m.max_id
)
FROM (
    SELECT MAX(decision_id) AS max_id
    FROM decision_audit
    WHERE pass_fail = 'FAIL'
      AND stage = 'execution_guard'
      AND decision_time >= now() - INTERVAL '7 days'
) AS m
WHERE op.operator_id = %(op)s
  AND m.max_id IS NOT NULL;
```

No-op on empty window (no guard rows in 7-day window → `m.max_id IS NULL` → UPDATE matches zero rows → cursor unchanged). Atomic — a guard row inserted between the subselect and the UPDATE commit gets a larger `decision_id` and stays unseen on next GET, preserving race safety on the overflow path as well.

**Response:** 204 No Content.

### Error semantics

- Guard-rejections GET failing should **not** 500 the dashboard; the frontend renders null on error (silent-on-error policy for strips, prevention: Frontend async render-surface isolation). Backend still returns proper 500 on internal errors — silent-on-error is a **frontend** concession, not a backend contract.
- `GET /alerts/guard-rejections` must complete in <200ms on typical load. Partial index keeps the 7-day scan cheap.

---

## Frontend

### `frontend/src/api/alerts.ts`

```ts
export type GuardRejectionAction = "BUY" | "ADD" | "HOLD" | "EXIT";

export interface GuardRejection {
  decision_id: number;
  decision_time: string;
  instrument_id: number | null;
  symbol: string | null;
  action: GuardRejectionAction | null;
  explanation: string;
}

export interface GuardRejectionsResponse {
  alerts_last_seen_decision_id: number | null;
  unseen_count: number;
  rejections: GuardRejection[];
}

export async function fetchGuardRejections(): Promise<GuardRejectionsResponse>;
export async function markAlertsSeen(seenThroughDecisionId: number): Promise<void>;
export async function dismissAllAlerts(): Promise<void>;
```

Uses existing `apiFetch` helper — matches `frontend/src/api/portfolio.ts` shape.

### `frontend/src/components/dashboard/AlertsStrip.tsx`

**Render rules:**

1. `rejections.length === 0` → component returns `null`. Do not reserve dashboard space for zero signal.
2. Fetch error → component returns `null` (silent-on-error; matches `RollingPnlStrip` pattern from PR #388).
3. Header: `"Guard rejections"` + pill `"{unseen_count} new"` when `unseen_count > 0`. Right side: either "Mark all read" (normal path) or "Dismiss all ({unseen_count}) as acknowledged" (overflow path) — see below.
4. Body renders **every row in the payload** (up to 500) in a scrollable container (CSS `max-height: 24rem; overflow-y: auto;`). No client-side slice — `rejections.length` equals "rows the operator actually saw".
5. Row per rejection:
   - Amber left border if `decision_id > alerts_last_seen_decision_id` (or `alerts_last_seen_decision_id === null`); slate otherwise.
   - Columns: symbol · action · truncated `explanation` (title attribute = full) · relative time (`formatRelativeTime`).
   - Row wraps in a React Router Link to `/instruments/{instrument_id}` when `instrument_id != null`; plain row div otherwise.

**Normal path — "Mark all read":**

```ts
await markAlertsSeen(rejections[0].decision_id);  // newest decision_id in payload
refetch();
```

`decision_id` is a unique BIGSERIAL — the strict `>` comparison on the server closes the tie-break race that a timestamp-only cursor would leave.

**Overflow path — "Dismiss all ({unseen_count}) as acknowledged":**

Triggered when `unseen_count > rejections.length` (total unseen exceeds the 500-row payload). The normal "Mark all read" button would ack rows the operator never rendered; that's the bug the safety rule blocks. To avoid a dead-end (the overflow previously pointed at read-only `/recommendations`, leaving the badge stuck until rows aged out), the button swaps label and intent: operator explicitly acknowledges all in-window rejections, including the `unseen_count - rejections.length` that could not be rendered.

Behaviour:

- Button opens a confirm dialog: "Dismiss all {unseen_count} unseen rejections? {unseen_count - rejections.length} are not shown above. Review them at /recommendations before dismissing if they might matter."
- Confirm → `POST /alerts/dismiss-all` (no body); server atomically advances the cursor to the current MAX(decision_id) in the 7-day guard-failure window. No client-side timestamp is sent.
- Cancel → no-op, badge stays.
- Link to `/recommendations` shown beside the button for full triage ([frontend/src/pages/RecommendationsPage.tsx](../../frontend/src/pages/RecommendationsPage.tsx) route wired at [frontend/src/App.tsx](../../frontend/src/App.tsx)).

**Render rule for the header actions:**

```ts
const normalAck = unseenCount > 0 && unseenCount <= rejections.length;
const overflowAck = unseenCount > rejections.length;
// Exactly one of normalAck / overflowAck is ever true when unseenCount > 0.
```

### Wire into `DashboardPage.tsx`

New `useAsync(fetchGuardRejections, [])` hook. Rendered between `RollingPnlStrip` and the Positions section ([frontend/src/pages/DashboardPage.tsx](../../frontend/src/pages/DashboardPage.tsx) layout comment already documents the strip order).

No page-level error coupling — strip is self-isolating per rule 2 above.

---

## Tests

### Backend (`tests/test_api_alerts.py`)

Fixtures: minimum schema (operators, instruments, trade_recommendations, decision_audit). Test DB isolation via `ebull_test` (prevention: tests must never wipe dev DB).

1. **Empty state** — no rows → `{ rejections: [], unseen_count: 0, alerts_last_seen_decision_id: null }`.
2. **7-day window inclusion** — insert rows at `now() - interval '6 days'` and `now() - interval '8 days'`; only the 6-day row returned.
3. **500-row cap** — insert 510 failed rows within window, `alerts_last_seen_decision_id = NULL`; `rejections.length === 500`, `unseen_count === 510` (count query reflects true total, not the LIMIT). Frontend-side: this is the condition that triggers the overflow branch.
4. **pass_fail excludes `'PASS'`** — insert mixed; only `FAIL` rows returned.
5. **`unseen_count` anchor** — `alerts_last_seen_decision_id` set to mid-window value; rows with smaller `decision_id` → not counted, larger → counted.
6. **`unseen_count` NULL last-seen** — all in-window rows counted.
7. **POST /alerts/seen monotonic** — current = 1000, POST `seen_through_decision_id = 500` → column unchanged (GREATEST keeps 1000).
8. **POST /alerts/seen first time** — NULL current, POST `1000` → set to 1000.
9. **POST /alerts/seen missing field** — body `{}` → 422.
10. **POST /alerts/seen non-integer** — body `{"seen_through_decision_id": "abc"}` → 422.
11. **POST /alerts/seen clamped to MAX-in-window** — window MAX = 200; cursor NULL; POST `seen_through_decision_id = 99999` → column set to 200 (clamped by `LEAST`). Prevents buggy client from blinding operator to future alerts.
12. **POST /alerts/seen race safety** — insert row `R_new` with `decision_id = 101`; operator posts `seen_through_decision_id = 100`; after POST, `unseen_count === 1` (R_new `decision_id = 101 > 100` still unseen — strict `>` comparison).
13. **GET ordering by decision_id not decision_time** — insert two FAIL rows where `decision_id = 50` has `decision_time = now()` and `decision_id = 51` has `decision_time = now() - '1 minute'` (simulates app-side clock skew); `rejections[0].decision_id === 51` (ordering is `decision_id DESC`, which puts the later-PK row first even though its `decision_time` is earlier).
14. **POST /alerts/dismiss-all advances to MAX** — insert rows with `decision_id` 100, 200, 300 within window; cursor NULL; POST dismiss-all → column set to 300.
15. **POST /alerts/dismiss-all monotonic** — current = 500, MAX-in-window = 300; POST → column unchanged at 500 (GREATEST keeps 500; never rewinds).
16. **POST /alerts/dismiss-all race safety** — start with cursor NULL; insert guard rows 100 and 200; POST dismiss-all; then insert row 201 → next GET shows unseen_count === 1 (row 201 arrived after the atomic MAX subselect).
17. **POST /alerts/dismiss-all empty window** — no rows in window; cursor NULL; POST → column stays NULL (SQL UPDATE is no-op because `m.max_id IS NULL` excludes the row from matching).
18. **POST /alerts/dismiss-all empty window with existing cursor** — cursor = 500, no rows in window; POST → column unchanged at 500.
19. **Non-guard stage excluded** — insert FAIL row with `stage = 'order_execution'` → excluded from `/alerts/guard-rejections` and from `/alerts/dismiss-all` MAX scope.
20. **Missing instrument_id / recommendation_id** — row still renders with nulls for `symbol`, `action`.
21. **HOLD action round-trip** — row with `action = 'HOLD'` serialises through API without coercion.
22. **`sole_operator_id` errors** — no operator → 503; multiple operators → 501 (both endpoints).

### Frontend (`AlertsStrip.test.tsx`)

1. Zero rejections → component renders nothing (`expect(container).toBeEmptyDOMElement()`).
2. Fetch error → component renders nothing (silent-on-error).
3. Overflow render — when `unseen_count > rejections.length` → "Mark all read" hidden, "Dismiss all ({unseen_count}) as acknowledged" rendered with confirm dialog + `/recommendations` link shown beside it.
4. Overflow confirm behaviour — click "Dismiss all" → confirm dialog opens; confirm → `dismissAllAlerts` called (no body), then `refetch` triggered. Assert the POST actually fires, not just that the button exists.
5. Overflow cancel behaviour — click "Dismiss all" → confirm dialog opens; cancel → `dismissAllAlerts` NOT called, `refetch` NOT triggered, badge state unchanged.
6. When `rejections.length === 500` and `unseen_count === 500` (all unseen, none beyond payload) → "Mark all read" button stays visible (normal path). Pins the safety rule's positive branch at the cap.
7. Unseen rows: amber border class present; seen rows: slate class present.
8. "Mark all read" → `markAlertsSeen` called with `rejections[0].decision_id` (newest decision_id in payload); `refetch` triggered.
9. `instrument_id !== null` → row is a `<Link>`; `instrument_id === null` → plain row.
10. `unseen_count === 0` → no "Mark all read" button, no "N new" pill.
11. Explanation truncates with title attribute preserving full text.

Reuse existing `useAsync` test harness and `MemoryRouter` pattern from `frontend/src/components/dashboard/RollingPnlStrip.test.tsx`.

---

## Prevention entries consulted

| Entry | How applied |
|---|---|
| `datetime.now(UTC)` vs DB `now()` in freshness windows | Both endpoints use DB `now()` |
| Unbounded API limit parameters | 7-day window + 500-row hard cap hardcoded server-side; frontend renders every row in the payload (no client-side slice) so `rejections.length` == "rows operator saw" |
| Interval construction via string concatenation in SQL | `INTERVAL '7 days'` literal; no concatenation |
| Loose `string` on API response fields that mirror backend `Literal` | `action` typed as `Literal` union both sides |
| Frontend async render-surface isolation | Strip silent-on-error; does not couple to page banner |
| API response shapes invented at the type boundary | Envelope + row shapes defined in backend + frontend + tests |
| Naive datetime in TIMESTAMPTZ query params | N/A — cursor is BIGINT `decision_id`, not a timestamp, so no naive-datetime parse path exists |
| Unbounded enum filters accept nonsense values silently | No filter params accepted on GET (fixed window) |

## Settled decisions consulted

| Decision | How applied |
|---|---|
| Guard auditability — one decision_audit row per guard invocation | This is the read side of that contract |
| Product-visibility pivot — prioritise operator visibility over infra work | Closes final phase of #315 |

---

## Definition of done

1. Migration applied cleanly against dev DB.
2. Both endpoints return shapes documented above; tests pass.
3. Dashboard renders strip between `RollingPnlStrip` and Positions when rejections exist.
4. "Mark all read" refetches strip; `unseen_count` reflects only rejections written after the POST (races are honoured — rows that arrived between GET and POST remain unseen per race-safety contract).
5. Strip hidden when rejections are empty.
6. `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`, `uv run pytest` all pass.
7. `pnpm --dir frontend typecheck`, `pnpm --dir frontend test` all pass.
8. Codex pre-spec pass complete (this document); Codex pre-push pass before first `git push`.
9. PR description self-contained per `feedback_pr_description_brevity.md`.
10. On merge: #394, #395, #396 filed; #315 closed.
