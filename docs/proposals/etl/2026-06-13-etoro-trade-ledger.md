# eToro trade ledger — `trade_events` + Activity surface (#1593, folds #393)

Status: PROPOSAL v3 (Codex ckpt-1 rounds 1-2 folded) — awaiting operator sign-off.
Consumers: #1594 (portfolio value-over-time v2 `units_at_day`), reports epic (period activity).

## §0 Grep proof

> Generated 2026-06-13 against branch main @ b3a9cd89. Line numbers exact.

### Migration numbering

```text
$ ls sql/ | sort -t_ -k1 -n | tail -1
193_instrument_dimensional_facts.sql        → this spec takes sql/194
```

### Sink shape — broker_positions (read + archived by this spec)

```text
$ grep -n "PRIMARY KEY\|REFERENCES\|initial_units" sql/024_broker_positions.sql
15:    position_id              BIGINT PRIMARY KEY,                   -- eToro positionID
16:    instrument_id            BIGINT NOT NULL REFERENCES instruments(instrument_id),
19:    initial_units            NUMERIC(20, 8),                       -- detect partial closes (isPartiallyAltered)
```

Line 19's comment is load-bearing for §4: eToro's partial-close model is
same-id reduction — positionId persists, `units` shrinks, `initialUnits`
keeps the original, `isPartiallyAltered` flags it.

### Writers + sweep + callers

```text
$ grep -rn "INSERT INTO broker_positions" app/ --include="*.py"
app/api/orders.py:285
app/services/order_client.py:583
app/services/portfolio_sync.py:150        (inside _upsert_broker_positions, def at :121)

$ grep -n "DELETE FROM broker_positions" app/services/portfolio_sync.py
228:            DELETE FROM broker_positions          (disappeared-sweep; called from sync_portfolio at :688)

$ grep -n -- "-order_id" app/api/orders.py app/services/order_client.py
app/api/orders.py:282:        synthetic_position_id = -order_id     (#227 — negative synthetic namespace)
app/services/order_client.py:565 (docstring) + :607 ("pid": -order_id)

$ grep -n "sync_portfolio" app/services/etoro_websocket.py | grep -v "^#"
587: (docstring) _default_reconcile_runner runs ``sync_portfolio`` against a fresh DB connection
→ sync_portfolio has TWO callers: app/workers/scheduler.py:2903 (daily_portfolio_sync,
  the only scheduled site) and the WS reconcile runner.

$ grep -rn "trade_events" app/ sql/        (no hits — name free)
```

### Empirical probes (2026-06-13, demo creds; raw captures /tmp/etoro_probe/*.json → fixture in PR-1)

```text
GET /api/v1/trading/info/trade/demo/history?minDate=2020-01-01T00:00:00Z&page=1&pageSize=50
→ HTTP 200, list[1]: {positionId: 3308442654, instrumentId: 4077 (ILMN), isBuy: true,
   openTimestamp: 2025-08-12T16:47:12.643Z, openRate: 97.3,
   closeTimestamp: 2025-11-14T19:24:35.307Z, closeRate: 120.56,
   units: 82.135523, netProfit: 1910.47, fees: 0.0, investment: 7991.79,
   initialInvestment: 7991.79, leverage: 1, orderId: 272136682, socialTradeId: 0,
   parentPositionId: 0, stopLossRate: 0.0001, takeProfitRate: 200.0, trailingStopLoss: false}

minDate filter-field probe (ILMN open 2025-08-12, close 2025-11-14):
  minDate=2025-10-01 (between legs)  → list[1]   — open-date filtering REFUTED
  minDate=2025-12-01 (after close)   → list[0]
  ⇒ filter ≡ closeTimestamp >= minDate (an "either-leg" filter is mathematically
    equivalent since close > open). Watermark on max(close) is the correct field.

pageSize=500 → 200 (no cap error observed); minDate date-only form → 200.
Rate headers on every response: ratelimit-limit: 60, ratelimit-policy: 60;w=60.

GET /api/v1/balances/history → 403 {"errorCode":"InsufficientPermissions"};
GET /api/v1/balances → 403 (same) — current demo API key lacks balances scope.

OpenAPI v1.244.0 (fetched 2026-06-13): balances/history = EOD snapshots, 12-month
lookback, 365d max/request. trade/demo/history params = exactly
{x-request-id, x-api-key, x-user-key, minDate(required), page, pageSize};
no ETag/If-Modified-Since/304 on it (ETag exists only on aggregate-portfolio +
agent-portfolio endpoints).
```

## §1 Decisions

1. New append-only `trade_events` table (sql/194): one row per broker-observed position **open** or **close** event. Immutable — no UPDATE path, conflict = DO NOTHING.
2. **Demo trade history is ingestible**: `GET /api/v1/trading/info/trade/demo/history` works on the demo account (probed; §0). One row per closed slice carrying both legs. Backfill = first fetch with deep `minDate`; steady-state = same fetch every `portfolio_sync` tick with a watermark-derived `minDate` (filter field empirically validated, §0).
3. **Single writer service**: `sync_portfolio` itself is the only `trade_events` writer. Its signature gains an optional `trade_history: Sequence[ClosedTradeRow] | None = None` parameter — open events from the portfolio-payload position diff (always), open+close events from the supplied history rows (when provided). BOTH callers are updated explicitly in PR-1: `scheduler.py::daily_portfolio_sync` and the WS `_default_reconcile_runner` (§0) each fetch all history pages inside their own provider session and pass them in — no caller silently skips the ledger. Writes are idempotent under concurrent callers (atomic `ON CONFLICT DO NOTHING`; the mismatch WARNING is advisory logging, race-tolerant). Our own order/close path does NOT write the ledger directly (deviation from the issue sketch): its `broker_positions` rows use synthetic negative position ids (#227, both writers — §0) that are replaced by real ids at next sync, so order-path events would orphan. **NEW in PR-1**: after a successful broker call, the order path enqueues an immediate manual run of the registered job `daily_portfolio_sync` (manual-queue dispatch is by JOB NAME — `JOB_DAILY_PORTFOLIO_SYNC`, scheduler.py:307 — via the `publish_manual_job_request_with_conn` precedent); no such enqueue exists today (orders.py persists and stops); if the enqueue fails, freshness degrades to the next 5-min scheduled tick. Intent-side audit already lives in `orders`/`fills`/`decision_audit`.
4. Externally-closed positions are **archived, not just deleted**: the disappeared-DELETE sweep copies rows to `broker_positions_closed` before deleting. **Every ledger/archive read from `broker_positions` filters `position_id >= 0`** — synthetic negative rows are handoff artefacts: never archived, never diffed into open events (the open-event diff additionally keys on broker-payload ids, which are real by construction).
5. `cash_ledger` is NOT written by this path — cash truth stays with the existing broker_sync reconcile (no double-count).
6. #393 closes with: equity-over-time exists in the API (`/api/v1/balances/history`) but returns 403 InsufficientPermissions on our demo key; 12-month lookback cap anyway. Our own EOD persistence (#1594) is the equity-history source; revisit only if the operator regenerates keys with balances scope. (Findings posted on #393.)
7. **Units contract** (consumed by #1594): an open event's `units` = the position's ORIGINAL opened units; each close event's `units` = the closed slice delta. Invariant: Σ(close units) ≤ open units per position_id; net-open at time t = open − Σ(closes ≤ t), clamped ≥ 0 by consumers. See §4.
8. Operator sees: `GET /portfolio/activity` + Activity tab on PortfolioPage (PR-2) — when bought/sold, price, fees, holding period, realised P&L per closed trade.

## §2 Identifiers + identity-drift

- `position_id` (eToro positionID, int64) — event identity anchor. Stable for a position's lifetime including partial closes (same-id reduction model, §0/§4). Synthetic negative ids never enter `trade_events`: open-event diff keys on broker-payload position ids (real by construction); history rows carry real ids; the `position_id >= 0` CHECK is the backstop.
- `instrumentId` (eToro int) — numerically equal to our `instruments.instrument_id` (verified: 4077 = ILMN on dev). Historical closed trades may reference instruments no longer in the universe → two columns: `etoro_instrument_id BIGINT NOT NULL` (raw, always set) + `instrument_id BIGINT NULL REFERENCES instruments` (set when resolvable at ingest). No silent drops: unresolved rows still land, `instrument_id IS NULL`, surfaced in the Activity UI as `#<etoro_id>`. A runbook re-resolve exists for later universe additions (§17).
- `orderId`, `socialTradeId`, `parentPositionId` — carried as columns; `social_trade_id != 0` marks mirror-originated trades (FE default-filters them, consistent with value-history excluding mirrors). `orderId` provenance (open-order vs close-order id) is unverified — kept as data, NOT used in conflict keys (§21).
- Symbol changes are non-events (id-keyed). Instrument-id reuse by eToro: not observed; raw payload preserves the evidence if it ever happens.

## §3 Endpoint surface

| url | method | body_schema_version | sample_response_fixture_path |
|---|---|---|---|
| `/api/v1/trading/info/trade{/demo}/history?minDate=&page=&pageSize=` | GET | OpenAPI v1.244.0 | `tests/fixtures/etoro/trade_history_demo.json` (added in PR-1 from the probe capture) |

- Env segment placement: `/trade/demo/history` (demo) vs `/trade/history` (real) — differs from the `/info{/demo}/portfolio` convention; provider builds it explicitly.
- `minDate` REQUIRED; filters on `closeTimestamp >= minDate` (probed, §0). `page` (1-based) + `pageSize` optional; loop while `len(rows) == pageSize`, collect ALL pages before transform (per-position grouping needs the full batch, §4). Empty result = `[]` (HTTP 200).
- Response row schema: §0 probe. Money fields = USD account-currency floats; `units` float (6dp observed); timestamps ISO-8601 UTC with ms.
- Closed-slices only: every row has both legs. Open events for still-open positions come from the `/trading/info{/demo}/portfolio` payload already fetched by `sync_portfolio` — no extra endpoint.

## §4 Schema

```sql
-- sql/194_trade_events.sql
CREATE TABLE trade_events (
    event_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position_id         BIGINT NOT NULL CHECK (position_id >= 0),
    etoro_instrument_id BIGINT NOT NULL,
    instrument_id       BIGINT REFERENCES instruments(instrument_id),
    event_kind          TEXT NOT NULL CHECK (event_kind IN ('open', 'close')),
    side                TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    units               NUMERIC(20, 8) NOT NULL CHECK (units > 0),
    price               NUMERIC(20, 8) CHECK (price > 0),
    executed_at         TIMESTAMPTZ NOT NULL,
    fees_usd            NUMERIC(20, 4),
    realized_pnl_usd    NUMERIC(20, 4) CHECK (event_kind = 'close' OR realized_pnl_usd IS NULL),
    investment_usd      NUMERIC(20, 4),
    order_id            BIGINT,
    social_trade_id     BIGINT,
    parent_position_id  BIGINT,
    source              TEXT NOT NULL CHECK (source IN ('etoro_sync', 'etoro_history')),
    raw_payload         JSONB NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_trade_events_open
    ON trade_events (position_id) WHERE event_kind = 'open';
CREATE UNIQUE INDEX uq_trade_events_close
    ON trade_events (position_id, executed_at) WHERE event_kind = 'close';
CREATE INDEX ix_trade_events_instrument_time
    ON trade_events (instrument_id, executed_at);

-- archive for externally-closed broker rows (evidence; not a ledger consumer input)
CREATE TABLE broker_positions_closed (
    LIKE broker_positions INCLUDING DEFAULTS,
    closed_detected_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (position_id, closed_detected_at)
);
```

- Index Budget: `trade_events` = PK + 3 = 4, at cap, none to spare (declared). `broker_positions_closed` = PK only.
- **Units semantics under the partial-close model** (same-id reduction, §0 sql/024:19):
  - Open event from the portfolio diff: `units = initialUnits` from the payload (fall back to `units` when absent) — the ORIGINAL size, immune to later partial closes.
  - Open event synthesized from history (position fully closed before the ledger ever saw it): the transform groups the fetched batch by `positionId` and emits ONE open with `units = Σ slice units`, `executed_at = min(openTimestamp)`. Correctness: a never-observed position only enters via the deep-backfill window, which spans the account lifetime, so ALL its slices are in the batch. A position observed open during our watch gets its open from the portfolio diff (initialUnits) before any close arrives — first write wins, later history opens are expected duplicates.
  - Close events: one per history row, `units` = that slice's delta.
  - Monitored invariant: at ingest, if Σ(close units) would exceed the stored open's units for a position, count `conflict_anomaly` (§15) and WARN with both payloads — loud, never silent, no overwrite.
- `side` stamped at ingest: open → `buy` if `isBuy` else `sell`; close → the opposite. `isBuy` stays in `raw_payload` (no denormalised twin to drift).
- Encoding/precision: floats parsed `Decimal(str(x))` (prevention-log: never `Decimal(float)`); units/price NUMERIC(20,8), USD money NUMERIC(20,4) (precedent: sql/184).
- `price` nullable + `> 0` CHECK: eToro sentinel zeros (e.g. `stopLossRate: 0.0001`) must not become marks (prevention-log: non-positive price ≠ valid mark). Zero/absent rate → NULL price, row still lands, counted (§15).
- TIMESTAMPTZ everywhere; `executed_at` ALWAYS from the API payload (`openTimestamp`/`closeTimestamp`/`openDateTime`), never `now()` (prevention-log).
- No `ON DELETE CASCADE` anywhere (audit-grade table).

## §5 Fetch strategy + rate-limit composition

- `per_resource_http`, but the "resource" is the single account: 1 GET per sync tick (+pagination pages, expected 1 for years yet).
- Budget: eToro 60 GET/min per user key (header-confirmed). Each 5-min `orchestrator_high_frequency_sync` tick currently spends ~2 GETs (portfolio, fx); this adds 1 → ~3 per 5 min. No new lane: runs inside the existing `portfolio_sync` execution slot. WS-reconcile-triggered runs add at most a handful more — still two orders of magnitude under budget.

## §6 Conditional-GET semantics

N/A on this endpoint — OpenAPI v1.244.0 declares no ETag/If-Modified-Since/304 for trade history (§0; ETag exists only on aggregate-portfolio + agent-portfolio endpoints). `minDate` watermark is the delta mechanism.

## §7 Retry posture per error-class

| status | posture |
|---|---|
| 429 / 5xx / timeout | Skip history ingest this tick, log WARNING; watermark unmoved so next tick (≤5 min) re-fetches. Position upsert/diff is unaffected (history ingest runs as a separate step after the position transaction commits). |
| 403 | Loud ERROR (permission regression — observed on balances, §0; must not be silent on trade history). |
| 400 | Loud ERROR (request-shape bug — deterministic, fix code). |

No retry-storm risk: nothing re-fires faster than the sync cadence.

## §8 Multi-writer sink registry

`trade_events` is a NEW sink with ONE writer function (`sync_portfolio`, history rows supplied via its new optional parameter — §1.3), reached from two call sites (scheduled `daily_portfolio_sync`, WS `_default_reconcile_runner` — §0; both updated in PR-1 to fetch and pass history). Concurrency safety: `sync_portfolio` takes `pg_advisory_xact_lock` at entry so concurrent reconciles serialize — without it, two callers holding snapshots from different instants could interleave the archive/DELETE sweep against each other's upserts and archive a live position on stale evidence (Codex ckpt-2 HIGH). Ledger conflict keys = the two partial unique indexes (§4); conflict action = `ON CONFLICT DO NOTHING` (atomic, first observation wins). Disagreement detection (same key, different figures) is a post-insert advisory SELECT + WARNING — logging only.

`broker_positions_closed`: one writer (the archive step inside `_upsert_broker_positions`' delete sweep), filtered `position_id >= 0` (§1.4).

This spec adds NO writer to any existing multi-writer sink (`cash_ledger` untouched — §1.5).

## §9 Watermark + retry-budget

- `minDate = COALESCE(MAX(executed_at) FILTER (WHERE event_kind='close' AND source='etoro_history'), '2017-01-01') - interval '7 days'`. The endpoint filters on `closeTimestamp` (probed, §0), so max-close is the correct watermark field — a position opened years ago and closed yesterday has `closeTimestamp` inside any recent window and cannot be missed. Derived from the sink itself — no separate watermark row to drift. 7-day overlap absorbs skew; unique indexes make re-ingest idempotent.
- Empty ledger → deep backfill happens automatically on first sync after migration (bootstrap = steady-state with a wider window; satisfies the bulk-first-load rule with ~1-3 HTTP calls).
- Retry budget: none beyond §7 (cadence-bounded).

## §10 Encoding / precision / NULL / timezone

Covered in §4: `Decimal(str(...))` parsing, NUMERIC precisions, TIMESTAMPTZ UTC from payload, JSONB raw payload stored as received. SQL NULL (not JSON null) for absent optionals.

## §11 Backfill horizon + retention

- Horizon: account lifetime (`minDate` 2017-01-01 pre-dates the demo account; 2020 probe accepted). Volume: single account, long-horizon strategy → O(10²) rows/year. Storage negligible.
- Retention: forever. Append-only audit ledger; no sweep. `broker_positions_closed` likewise.

## §12 Partition strategy + extension deadline

N/A — O(hundreds) rows/year for one account; partitioning is pure overhead. No static window, no extension alarm.

## §13 Bootstrap vs steady-state mode

- Same code path both modes (§9): first run = deep `minDate`, steady-state = watermark. Expected HTTP per fire: 1 + extra pages; bootstrap ≤3 calls on any realistic account.
- Runs inside `portfolio_sync` (carve-out member `orchestrator_high_frequency_sync`, motivation b) — already creds-gated and `PREREQ_SKIP`-safe pre-universe. History ingest with an unresolvable instrument stores `instrument_id NULL` (§2), so a pre-universe fire cannot FK-fail.
- No new `ScheduledJob`, no new bootstrap stage, no gate questions.

## §14 Tombstones + soft-delete

N/A for `trade_events` — executed trades have no amendment/supersession concept; append-only, never deleted. `broker_positions` rows ARE hard-deleted by the existing sweep (pre-existing semantic), now with the archive copy (§1.4) preserving the evidence for real ids.

## §15 `rows_skipped` closed-set

Per ingest run, counters logged in the sync summary:

- `duplicate` — ON CONFLICT no-op (expected steady-state bulk).
- `unresolved_instrument` — row landed with `instrument_id NULL` (counted, not dropped).
- `null_price` — landed with NULL price after sentinel/absent rate (counted, not dropped).
- `conflict_anomaly` — close-slice sum would exceed the open's units, or a key-collision with differing figures (§4/§8). Landed/skipped per the DO-NOTHING outcome, WARN with payloads.
- `other` — anything else, with `partial_data_reason` free-text in the log line.

No row is ever silently dropped; "skip" means degraded-land, except `duplicate`.

## §16 Schema-evolution migration path

New table — no dual-parser window. If eToro adds/renames response fields, `raw_payload` already holds them; promoting a field = additive migration + one-shot backfill from raw_payload. No parser_version in v1 (single trivial mapping; revisit if a second transform version ever exists — §21).

## §17 Operator runbooks

- `app/runbooks/trade_events_reresolve_instruments.py` — re-resolve `instrument_id IS NULL` rows against the current universe (`--dry-run` default, `--apply` to write). PR-1 deliverable.
- Re-ingest after a bug: delete nothing; fix code — the watermark overlap + DO NOTHING re-walks recent rows. Deep re-walk: one-off runbook if ever needed; declared out of scope for v1.

## §18 Smoke matrix

Account-scoped source (not issuer-keyed) — panel = the dev demo account state:

1. ILMN (4077) closed trade → exactly 1 open + 1 close event, `realized_pnl_usd = 1910.47`, open units = close units = 82.135523, holding period 2025-08-12 → 2025-11-14.
2. The 7 currently-open broker positions → 7 open events (units = initialUnits), 0 close events.
3. Mirror positions → ZERO open events from the portfolio diff (mirrors parse into `copy_mirror_positions`, not `broker_positions`); any history row with `social_trade_id != 0` lands flagged.
4. Synthetic-id check: `SELECT count(*) FROM trade_events WHERE position_id < 0` = 0 and `broker_positions_closed` contains no negative ids after an eBull-originated order cycle.

PR description records the observed counts + the ILMN figures.

## §19 Cross-source verification

ILMN closed trade vs the eToro web UI portfolio→history view on the demo account: net profit $1,910.47, units 82.135523, open 2025-08-12 @ 97.30, close 2025-11-14 @ 120.56. Recorded in the PR description with operator confirmation.

## §20 Test placement

- Pure-logic (no DB): payload→event transform table tests (closed-slice grouping → one open + N closes; Σ-units open synthesis; sentinel rates → NULL price; mirror flag; side derivation; Decimal parsing; synthetic-id exclusion in the diff) — the bulk.
- ONE `db`-marked integration test: unique-index dedup (re-ingest same fixture twice → no new rows) — the one genuinely-new SQL mechanism.
- Order-path enqueue: unit test that a successful place/close publishes a manual request for job `daily_portfolio_sync` (by registered job name — §1.3 NEW work).
- Smoke: existing `tests/smoke/test_app_boots.py` covers migration 194; activity endpoint smoke in PR-2.
- Dev-verify (DoD clauses 8-11): run sync on dev, check §18 figures, hit `/portfolio/activity` after PR-2.

## §21 Rationale log

**Decision:** event-grained ledger (open/close rows), not closed-trade-grained.
**Rejected:** mirroring eToro's one-row-per-closed-slice shape — #1594 `units_at_day` needs a units timeline; closed-trade grain forces every consumer to re-split legs.

**Decision:** single writer service at the sync chokepoint; order path enqueues an immediate sync (new PR-1 work) instead of writing events.
**Rejected:** issue-sketch direct writes from `orders.py`/`order_client.py` — synthetic negative position ids (#227, both writers per §0) would orphan open events when the real id arrives at next sync; mutating ids breaks immutability. Intent audit already exists (`orders`, `fills`, `decision_audit`).

**Decision:** two instrument columns (`etoro_instrument_id` NOT NULL raw + nullable FK `instrument_id`).
**Rejected:** single FK NOT NULL column — delisted instruments in deep history would FK-fail and force dropping trades; single un-FK'd column — loses referential integrity for the resolvable majority.

**Decision:** `ON CONFLICT DO NOTHING` + advisory mismatch WARNING.
**Rejected:** DO UPDATE all financial columns (the usual prevention-log default) — this table is immutable evidence; first-observed wins and disagreement is a loud anomaly, not a merge.

**Decision:** close conflict key `(position_id, executed_at)` + `conflict_anomaly` monitoring.
**Rejected:** including `order_id` in the key — its provenance (open-order vs close-order id) is unverified (§2); a constant-per-position order_id would silently weaken the key. Same-ms multi-slice closes are pathological; if they ever occur the anomaly counter fires loudly and the key is relaxed by an additive migration.

**Decision:** open-event units = ORIGINAL units (initialUnits / Σ-slices), closes = deltas.
**Rejected:** current remaining units on the open event — wrong pre-partial-close history for `units_at_day`, and unfixable later under an immutable open row.

**Decision:** fetch history on EVERY sync tick.
**Rejected:** disappearance-triggered + daily sweep — needs fetch-state storage, misses nothing always-fetch misses, saves only ~288 trivially-cheap GETs/day against a 60/min budget.

**Decision:** archive table `broker_positions_closed` (real ids only) rather than a `closed_at` soft-close column.
**Rejected:** soft-close column — every existing reader of `broker_positions` assumes table = open positions; one missed filter creates phantom holdings in valuation. Separate table keeps the invariant by construction.

**Decision:** no `parser_version` in v1.
**Rejected:** carrying the SEC-manifest pattern — single trivial mapping with raw_payload fallback; rewash = re-read raw, no supersession machinery to version against.

## §22 Open questions

1. **Partial-close history row shape**: the same-id reduction model is established (sql/024:19 `isPartiallyAltered`), but whether each slice emits its own history row with the same positionId — and whether slice timestamps are guaranteed distinct — is unverified (the dev account has one single-slice close). The §4 units contract + §15 `conflict_anomaly` counter make either answer safe and loud. Optional: operator authorises a one-off demo partial-close experiment to settle it empirically.
2. **`pageSize` ceiling**: 500 accepted with a 1-row account; true cap unverifiable until more history exists. Loop-while-full-page is correct under any cap.
3. **Balances scope**: would regenerating the demo API key with wider scopes unlock `/api/v1/balances/history`? Operator action at eToro portal key management; if yes, #1594 gains a cross-check source (12-month window only).

## Implementation plan

- **PR-1 (backend):** sql/194 + provider `get_trade_history()` (thin adapter, explicit demo path segment) + `app/services/trade_events.py` ingest (batch-grouping transform + upserts + skip counters; exposes `compute_history_min_date(conn)` for callers) + `sync_portfolio(conn, portfolio, trade_history=None)` signature change with ingest inside (position-diff open events from broker-payload ids; history events when rows supplied) + BOTH callers updated: `scheduler.py::daily_portfolio_sync` (~2895: provider context closes before `sync_portfolio` — read watermark on a short-lived conn first, fetch portfolio + ALL history pages in the same provider session, pass both in) and WS `_default_reconcile_runner` (same fetch-and-pass inside its own provider session) + archive-before-delete (`position_id >= 0`) inside `_upsert_broker_positions`' sweep + **NEW** order-path post-trade enqueue of job `daily_portfolio_sync` (by job name, manual queue) + fixture + tests + runbook. Dev-verified per §18/§19.
- **PR-2 (API+FE):** `GET /portfolio/activity` (paged, joined to instruments, holding period + realised P&L) + PortfolioPage Activity tab (mirror filter default-on, unresolved-instrument fallback rendering). FE skills re-read at implementation time.
- #393 closed when PR-1 merges (findings already posted).
