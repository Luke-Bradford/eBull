# Portfolio value v2 — PR-A: per-day FX + EOD equity snapshots (#1594)

Scope: #1594 items **2 (per-day FX)** + **3 (EOD snapshot persistence)**. Lands first,
independent of the ledger-units reconstruction. PR-B (items 1+5: `units_at_day` from
`trade_events`, value-history reads-persisted/recomputes-tail, prominent surfacing)
follows on this base.

Parent epic: #1593 (trade ledger — PR-1 #1610 `46f609f8`, PR-2 #1611 `20b1e0a2`, both MERGED).

---

## §0 Grep proof

Every identifier below verified by grep/read this session, not memory.

### Migration numbering
- Highest `sql/NNN` = `195_raw_status_semantics_born_compacted.sql`. This PR takes **`sql/196`**.
- Runner: `app/db/migrations.py` — scans `sql/NNN_*.sql` lexicographically, tracks in
  `schema_migrations` (content_sha256 drift guard). New file auto-applies on next boot.
- No name clashes: `grep -rln 'fx_rates_daily|portfolio_eod|eod_snapshot' sql/ app/` = ∅.

### Current value-history chart (audited AS-IS post #1604/PR#1605)
- Endpoint: `app/api/portfolio.py:817` `get_value_history` (range 1m/3m/6m/1y/5y/max).
- FE: `frontend/src/components/dashboard/PortfolioValueChart.tsx` (lightweight-charts; dark-mode,
  buy/sell markers, hover tooltip — all #1604, **not** redone here).
- **#1604 already fixed part of gap 1** (units): instruments with `fills` rows replay the
  fills ledger exactly (BUY/ADD/SELL/EXIT) — `app/api/portfolio.py:907-939`. Broker-synced
  holdings still carry CURRENT units back-dated to `open_date` (`:957-977`), and closed broker
  positions still drop out — that residual is **PR-B** (`trade_events` reconstruction).
- **Gap 2 (FX) NOT fixed**: `fx_mode="live"` — `load_live_fx_rates_with_metadata` applies
  today's `live_fx_rates` snapshot to every historical day (`:858-859`, docstring `:849-852`).
  ← **this PR's item 2.**
- **Gap 3 (no persistence) NOT fixed**: series recomputed per request, nothing stored.
  ← **this PR's item 3.**

### FX plumbing (reused)
- `app/services/fx.py:27` `convert(amount, from, to, rates)` — **direct pair then inverse ONLY**
  (`:40-46`); NO USD cross-rate. EUR→GBP fails unless a direct/inverse pair exists. Dated store
  MUST therefore mirror the live USD-base convention for parity (see §1.D).
- `app/services/fx.py` `upsert_live_fx_rate` (def ~`:108`) writes `live_fx_rates` (current snapshot,
  distinct table). `:59` `load_live_fx_rates_with_metadata`.
- Frankfurter provider: `app/providers/implementations/frankfurter.py:48` `fetch_latest_rates`,
  `:90` `fetch_latest_rates_conditional` (ETag/304). Base URL `_BASE_URL='https://api.frankfurter.dev'`
  (`:28`). Rate semantics: 1 base = rate × target.
- Live job: `app/workers/scheduler.py:3860` `fx_rates_refresh` — fetches USD→{GBP,EUR},
  `quoted_at = ECB publication date` (prevention-log #216 lessons: commit savepoint, ecb_date).
- **Empirically verified time-series shape** (probe 2026-06-13):
  `GET https://api.frankfurter.dev/v1/{start}..{end}?base=USD&symbols=GBP,EUR` → 200,
  `{"base":"USD","start_date","end_date","rates":{ "YYYY-MM-DD": {"GBP":0.738,"EUR":0.875}, ... }}`.
  Weekends/ECB-holidays **omitted** (8-day span → 6 dates). ETag header present.

### Dated tax FX table (NOT reused — see §21 rationale R1)
- `sql/013_tax_disposal_matching.sql:5-11` `fx_rates (rate_date DATE, from_currency TEXT,
  to_currency TEXT, rate NUMERIC(18,10), PK(rate_date, from_currency, to_currency))`.
- Read by tax only: `app/services/tax_ledger.py:204` `_load_fx_rate` queries
  `from_currency=<native>, to_currency='GBP'` and **raises** `"Populate fx_rates before
  ingesting tax events"` when missing (`:231`). No writer in app/ — dormant/manual.
- `"fx_rates"` elsewhere = the LIVE-refresh **layer name** (`live_fx_rates` sink), unrelated.

### Snapshot source tables (read)
- `sql/024_broker_positions.sql:14` `broker_positions` — `units NUMERIC(20,8)`, `is_buy`,
  `open_date_time`, `position_id` (real ≥0; synthetic negative = `-order_id`, #227). Dev: 7 real
  rows, 5 instruments, all USD.
- `cash_ledger` (sql/001) — delta rows, `SUM(amount)` = running balance, `currency`. Dev: USD only.
- `price_daily` (sql/001) — `close` per `(instrument_id, price_date)`.
- `instruments.currency` — native ccy. `app/services/runtime_config.py:42`
  `SUPPORTED_CURRENCIES={GBP,USD,EUR}`; `display_currency` default `'GBP'` (`:177`).
- `trade_events` (sql/194) — read only for the FX-backfill earliest-date floor. Dev: 9 rows,
  `2025-08-12 .. 2025-11-14`.

### Job / lane plumbing
- `app/jobs/sources.py:62` `Lane = Literal[...]`; precedent single-job lanes `db_positions` (`:79`,
  #1527), `db_liveness`/`db_retry`/`db_cusip`/`db_ownership_obs` (#1526/#1527).
- `app/workers/scheduler.py` `ScheduledJob(name, display_name, source=<Lane>, description, cadence)`;
  `JOB_MONITOR_POSITIONS` is the own-lane template (`source="db_positions"`, own table, disjoint).
- Manual-trigger "triangle" (sec_rebuild shape): `_INVOKERS` (`app/jobs/runtime.py`) +
  `MANUAL_TRIGGER_JOB_SOURCES` (`app/jobs/sources.py`) + `MANUAL_TRIGGER_JOB_METADATA`
  (`app/services/processes/param_metadata.py`).
- Reads use `app/db/snapshot.py` `snapshot_read` for multi-statement consistency (prevention-log L1052).

### #393 reversal
- `grep -ni 'no nav snapshot|nav snapshot table' docs/settled-decisions.md` = ∅ — the
  "no NAV snapshot table" posture was **never a formal settled-decisions heading**. This PR records
  the EOD-snapshot decision as a fresh positive entry citing the 2026-06-12 roadmap approval +
  the `/api/v1/balances/history` 403 probe (#1593 step 1).

---

## §1 Decisions

**A. EOD persistence = our own table (operator-approved).** `/api/v1/balances/history` →
403 InsufficientPermissions on the demo key (#1593 step 1, OpenAPI v1.244.0). Cannot depend on it.
We persist daily equity ourselves. **Reverses #393's informal "no NAV snapshot table" posture**
(2026-06-12 roadmap). Recorded in `docs/settled-decisions.md` this PR.

**B. Forward-only snapshot, no reconstruction (decouples PR-A from PR-B).** The EOD job captures
the **portfolio as it stands at compute time** — current `broker_positions` marked to market at
the EOD close (FX-converted) + cash — and stamps it to the latest closed trading session (§10).
Per-position value uses the **canonical `/portfolio` MTM formula** (Codex ckpt-2 P2):
`amount + units·(close − open_rate)` for long, `amount + units·(open_rate − close)` for short —
NOT `close × units` (which is notional exposure and only equals equity for unleveraged long). This
makes the snapshot agree with the dashboard and stay correct if a leveraged/short row ever appears.
**PR-B's recompute-the-tail MUST use the same MTM formula** so persisted snapshot rows and the
recomputed era don't diverge. It needs no
`units_at_day` timeline, so PR-A ships without `trade_events`-units work; historical reconstruction
(days before the job first ran) is PR-B's recompute-the-tail concern.
- **Honesty bound (Codex ckpt-1 H1):** this is "captured forward", NOT "exact as-of close".
  `broker_positions` + `cash_ledger SUM` are as-of the compute instant; a position/cash sync
  between the close and the snapshot run is reflected in that day's row. The job fires shortly
  after US close to minimise drift; `computed_at` records the capture instant for audit. A same-day
  re-run overwrites (ON CONFLICT) with the latest state. This residual is documented, not hidden —
  it is strictly more honest than the current recompute-from-current-units chart.

**C. Per-day FX = new `fx_rates_daily` table, Frankfurter ECB time-series.** Dated USD-base rows,
bulk-backfilled once over the trade range, gap-filled forward. NOT the tax `fx_rates` table
(blast-radius on the safety-critical tax path — §21 R1). NOT #281 (live eToro-FX-instrument
conversions — different concern).

**D. Dated FX mirrors the live USD-base convention.** `convert()` does direct+inverse only, no
USD cross-rate. The live chart today supports USD↔{GBP,EUR} and **skips** EUR↔GBP. Storing
USD-base ECB rows per day gives byte-for-byte parity: PR-B's recompute and any future snapshot use
the identical conversion the live path uses — no silent "PR-A fixes EUR→GBP" divergence. Dev
portfolio is 100% USD so the live pair is USD→GBP throughout.

**E. Mirror/copy-portfolio equity EXCLUDED** from the snapshot — consistent with value-history
(`portfolio.py:841-843`); no history source for it.

**F. Bulk-on-first-load, not per-day drip** (feedback: backfills belong in bootstrap-tier). First
EOD-job run fetches the entire `min(trade_events.executed_at, cash_ledger.event_time) .. today`
range in one Frankfurter time-series call; steady-state runs gap-fill only the missing tail.

**G. Per-position breakdown = child table, not JSONB.** Auditability (CLAUDE.md "persist enough
structured evidence"; "every trade path auditable") — operator can query "AAPL value on 2026-03-01".

---

## §2 Identifiers + identity-drift
- `fx_rates_daily` keyed by `(rate_date, base_currency, quote_currency)` — currency codes are ISO
  TEXT, no instrument identity. ECB rates immutable per date → `ON CONFLICT DO NOTHING`.
- `portfolio_eod_snapshots` keyed by `snapshot_date` (single account, v1). Re-run same day =
  `ON CONFLICT (snapshot_date) DO UPDATE` (recompute overwrites; deterministic).
- `portfolio_eod_position_snapshots` keyed `(snapshot_date, position_id)` — **per broker
  position**, NOT per instrument (`broker_positions` carries multiple `position_id` per instrument;
  keying on instrument_id would collide/overwrite and break the per-position audit claim — Codex
  ckpt-1 B1). `instrument_id` is a column (FK → instruments) so the chart can still aggregate by
  instrument. Real ids only (`position_id >= 0`; synthetic negatives excluded, #227).

## §3 Endpoint surface
- **No new operator endpoint in PR-A.** Read endpoint changes (value-history reads persisted rows)
  are **PR-B**. PR-A is schema + ingest + job only.
- Manual triggers added (jobs router, existing `POST /jobs/{name}/run`):
  `portfolio_eod_snapshot` (also scheduled) + `fx_history_backfill` (triangle, manual-only).

## §4 Schema (`sql/196_portfolio_value_v2_fx_eod.sql`)

```sql
BEGIN;

-- A. Per-day dated FX (USD-base ECB reference rates; mirrors live_fx_rates convention).
CREATE TABLE IF NOT EXISTS fx_rates_daily (
    rate_date       DATE NOT NULL,
    base_currency   TEXT NOT NULL,
    quote_currency  TEXT NOT NULL,
    rate            NUMERIC(18, 10) NOT NULL CHECK (rate > 0),  -- 1 base = rate quote
    source          TEXT NOT NULL DEFAULT 'frankfurter.timeseries',
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);
CREATE INDEX IF NOT EXISTS idx_fx_rates_daily_pair_date
    ON fx_rates_daily (base_currency, quote_currency, rate_date DESC);

-- B. Daily portfolio equity snapshot (own-table; reverses #393 no-NAV posture).
CREATE TABLE IF NOT EXISTS portfolio_eod_snapshots (
    snapshot_date     DATE PRIMARY KEY,
    display_currency  TEXT NOT NULL,
    total_value       NUMERIC(20, 4) NOT NULL,   -- positions_value + cash_value
    positions_value   NUMERIC(20, 4) NOT NULL,
    cash_value        NUMERIC(20, 4) NOT NULL,
    -- which fx_rates_daily.rate_date actually priced this snapshot (carry-forward
    -- on weekends/holidays — operator sees the real rate date used, not snapshot_date).
    fx_rate_date      DATE,
    -- Closed-set skip counters (Codex ckpt-1 H3 — keep no-price vs no-FX distinct).
    positions_total       INTEGER NOT NULL DEFAULT 0,  -- real positions seen
    positions_priced      INTEGER NOT NULL DEFAULT 0,  -- contributed to positions_value
    positions_no_price    INTEGER NOT NULL DEFAULT 0,  -- no price_daily close on/before date
    positions_no_fx       INTEGER NOT NULL DEFAULT 0,  -- priced but native→display FX unavailable
    cash_no_fx_currencies INTEGER NOT NULL DEFAULT 0,  -- cash currencies dropped for missing FX
    computed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    -- invariant: positions_total = positions_priced + positions_no_price + positions_no_fx
);

-- C. Per-position breakdown — audit evidence (CLAUDE.md auditability).
CREATE TABLE IF NOT EXISTS portfolio_eod_position_snapshots (
    snapshot_date     DATE NOT NULL REFERENCES portfolio_eod_snapshots(snapshot_date) ON DELETE CASCADE,
    position_id       BIGINT NOT NULL,            -- broker positionID (real, >=0)
    instrument_id     BIGINT NOT NULL REFERENCES instruments(instrument_id),
    units             NUMERIC(20, 8) NOT NULL,
    close_price       NUMERIC(20, 8),             -- native-ccy close used (NULL if none on/before date)
    native_currency   TEXT,
    value_display     NUMERIC(20, 4),             -- MTM equity in display ccy (NULL if skipped)
    -- why this row did/didn't contribute (closed set, mirrors the header counters).
    price_status      TEXT NOT NULL DEFAULT 'priced'
                      CHECK (price_status IN ('priced', 'no_price', 'no_fx')),
    PRIMARY KEY (snapshot_date, position_id)
);
CREATE INDEX IF NOT EXISTS idx_eod_position_snap_instrument
    ON portfolio_eod_position_snapshots (instrument_id, snapshot_date);

COMMIT;
```

Note: `portfolio_eod_position_snapshots` FK-children the snapshot — add both to `_PLANNER_TABLES`
in `tests/fixtures/ebull_test_db.py`, child-to-parent order (prevention-log L953).
`ON DELETE CASCADE` is on a snapshot table, NOT an `_audit`/`_log` table — L350 does not apply.

## §5 Fetch strategy + rate-limit composition
- FX history: `batched_http` — ONE Frankfurter time-series GET per backfill range (range query,
  not per-day). Steady-state gap-fill = one short-range GET. No SEC bucket. Frankfurter is
  unauthenticated public ECB mirror; existing live job already polls it daily.
- Snapshot compute: `derive` — pure SQL reads, no HTTP.

## §6 Conditional-GET semantics
- Historical ranges are immutable (past ECB rates never change) → plain GET, no ETag needed; the
  PK `ON CONFLICT DO NOTHING` makes re-fetch idempotent. (Live `fx_rates_refresh` keeps its ETag
  path for the always-moving "latest" — untouched.)

## §7 Retry posture per error-class
- Frankfurter transport/5xx: log + leave gap; next job run re-attempts the missing range
  (gap-fill is self-healing). No row written on failure.
- A date Frankfurter omits (weekend/holiday) is NOT an error — carry-forward at read time.
- Snapshot compute failure (e.g. no `price_daily` close): per-position `price_status='no_price'`
  (or `'no_fx'`), value under-stated not invented (mirror value-history's skip-not-zero,
  `portfolio.py:1102-1103`).

## §8 Multi-writer sink registry
- `fx_rates_daily`: written by `fx_history.ensure_fx_history`, called from **two** triggers — the
  scheduled `portfolio_eod_snapshot` job and the manual `fx_history_backfill` job (Codex ckpt-1 H2).
  Both are bound to the **same `db_eod_snapshot` lane** so they serialise — no concurrent writers.
  The upsert is `ON CONFLICT (rate_date, base, quote) DO NOTHING` regardless (immutable ECB rows),
  so even a hypothetical concurrent write is safe; the shared lane is the primary guard.
- `portfolio_eod_snapshots` / `_position_snapshots`: single writer (`portfolio_eod` service), also
  on `db_eod_snapshot`.
- No existing job (any lane) writes any of these three tables. Tax `fx_rates` untouched.

## §9 Watermark + retry-budget
- FX backfill watermark is **derived from the sink**: `MAX(rate_date) FROM fx_rates_daily` per
  pair vs today → gap range. No separate watermark row (mirrors trade-ledger §9).
- Floor = `LEAST(MIN(trade_events.executed_at), MIN(cash_ledger.event_time))::date`, fallback
  `CURRENT_DATE` (empty ledger → single-day fetch).

## §10 Encoding / precision / NULL / timezone
- `rate NUMERIC(18,10)` matches tax `fx_rates` precision + ECB published precision.
- Money `NUMERIC(20,4)`; units `NUMERIC(20,8)` (matches `broker_positions`).
- `snapshot_date` is a DATE = the latest closed trading session, derived from data (see B2 rule
  below), NOT the wall-clock run date.
- `Decimal` throughout the compute (no float); `convert()` returns Decimal.
- **`snapshot_date` is data-anchored, NOT wall-clock (Codex ckpt-1 B2).** Stamp =
  `MAX(price_daily.price_date)` across the held instruments (the latest closed session we actually
  have closes for), fallback `CURRENT_DATE` when there are no holdings/prices. This makes the stamp
  deterministic and idempotent: a run after midnight UTC, a weekend run, or a manual retry all
  stamp the **same** date (the last session with prices) and ON-CONFLICT-overwrite it — never a
  spurious next-calendar-day row pricing `close <= date`. The FX date used (`fx_rate_date`) is the
  most-recent `fx_rates_daily.rate_date <= snapshot_date` (carry-forward).

## §11 Backfill horizon + retention
- FX: from earliest ledger activity → today. Tiny (≈ #trading-days × 2 pairs). No retention sweep
  (kept forever — auditability; bytes negligible).
- Snapshots: one row/day forever (single account). No retention sweep in v1.

## §12 Partition strategy + extension deadline
- None — single-account O(hundreds) rows/year, same rationale as `trade_events` (sql/194 comment).

## §13 Bootstrap vs steady-state mode + **lane decision**
- **Lane = own `db_eod_snapshot`** (new `Lane` Literal member). Rationale: daily cadence at any
  minute collides with `orchestrator_high_frequency_sync` (every_5min, `db` lane) on the
  non-blocking `pg_try_advisory_lock` race → would skip (the #1527/#1534 starvation class).
  **Write-disjointness PROVEN**: the job writes ONLY `fx_rates_daily` +
  `portfolio_eod_snapshots(+_position_snapshots)` — no other job writes these (§8). Reads
  (`broker_positions`/`cash_ledger`/`price_daily`/`instruments`) are MVCC-safe vs the orchestrator's
  portfolio write. So `db_eod_snapshot` runs concurrently with orchestrator ingest with no race —
  satisfies the prevention-log "lane extraction must not expose a write race the serialization
  masked" gate (#1534): there is no shared write target to race on.
- **Both** PR-A jobs (`portfolio_eod_snapshot` scheduled + `fx_history_backfill` manual) sit on
  `db_eod_snapshot` so they serialise with each other (the two `fx_rates_daily` writers, §8).
- Bootstrap: NOT a bootstrap stage. The first scheduled (or manual) run self-backfills the FX
  history in one call (§1.F) — bulk-on-first-load without a new bootstrap stage.

## §14 Tombstones + soft-delete
- None. Snapshots overwrite-in-place per date (recompute is authoritative for that date); FX rows
  immutable.

## §15 `rows_skipped` closed-set (snapshot job)
Per-position outcome is a closed set (Codex ckpt-1 H3 — no-price and no-FX kept distinct):
- `positions_priced` — contributed to `positions_value`.
- `positions_no_price` — no `price_daily` close on/before `snapshot_date` (under-stated, not zeroed).
- `positions_no_fx` — had a close but native→display FX unavailable for `fx_rate_date`.
- Invariant: `positions_total = positions_priced + positions_no_price + positions_no_fx`.
- Cash side: `cash_no_fx_currencies` — distinct cash currencies dropped for missing FX.
- Each per-position row carries `price_status ∈ {priced, no_price, no_fx}`.
- `job_runs.row_count` = `positions_priced`.

## §16 Schema-evolution migration path
- New tables, no ALTER of existing. `IF NOT EXISTS` idempotent. If `portfolio_eod_snapshots`
  pre-exists from a partial apply, the columns are all in the CREATE — no orphan-column trap
  (prevention-log L1086 N/A: not a new-column-on-existing-table case).

## §17 Operator runbooks
- **First load / re-backfill FX**: `POST /jobs/fx_history_backfill/run` → fetches full range into
  `fx_rates_daily`. Idempotent.
- **Force a snapshot**: `POST /jobs/portfolio_eod_snapshot/run` → ensures FX history then writes
  today's snapshot (ON CONFLICT overwrites).
- **Verify**: `SELECT * FROM portfolio_eod_snapshots ORDER BY snapshot_date DESC LIMIT 5;` and
  cross-check `total_value` against `GET /portfolio` AUM (ex-mirror) for today.

## §18 Smoke matrix (PORTFOLIO panel, not SEC)
- Dev account holdings (5 instruments, all USD) + the one closed ILMN trade.
- After `fx_history_backfill` + `portfolio_eod_snapshot`: assert today's `total_value` ≈
  `GET /portfolio` positions MV + cash (ex-mirror), within rounding.
- Assert `fx_rate_date` ≤ `snapshot_date` and a USD→GBP rate exists for it.
- Assert `portfolio_eod_position_snapshots` has 5 rows summing to `positions_value`.

## §19 Cross-source verification
- One FX figure: `fx_rates_daily` USD→GBP for a chosen date vs Frankfurter direct
  (`/v1/{date}?base=USD&symbols=GBP`) — exact match (same source, proves no transform bug).
- One equity figure — **exact**: independently recompute `total_value` from the SAME inputs
  (`broker_positions` × `price_daily.close` for `snapshot_date`, `cash_ledger SUM`, the persisted
  `fx_rate_date` rates) and assert it equals the stored row to the rounding quantum.
- One equity figure — **directional** (Codex ckpt-1 M2): compare against live `GET /portfolio` AUM
  (ex-mirror). NOT apples-to-apples — `/portfolio` marks to live `quotes` (with cost-basis
  fallback) while the snapshot uses EOD `price_daily.close`; a close-vs-live-quote gap is expected,
  so this is a sanity band (same order of magnitude), not an equality check.

## §20 Test placement
- **Pure-logic (fast tier)**: gap-range computation (`ensure_fx_history` missing-range math);
  carry-forward rate selection (`load_fx_rates_for_date` picks most-recent ≤ D); equity aggregation
  (positions + cash → total, with a skipped-FX and a skipped-price row). Extract these as pure
  functions table-tested — no DB.
- **DB tier (one test, the genuinely-new mechanism)**: snapshot upsert idempotency — run
  `compute_and_store_eod_snapshot` twice for the same date, assert one snapshot row + N position
  rows, second run overwrites (no duplicates).

## §21 Rationale log
- **R1 — why NOT reuse tax `fx_rates`.** Shape-identical and dormant, but `tax_ledger._load_fx_rate`
  (`:204`) RAISES on missing rows today; dropping ECB USD→GBP rows in would silently make USD tax
  disposals START succeeding on ECB noon rates — an unaudited change to the safety-critical tax
  path, and ECB ≠ HMRC's required basis. Tax correctness is out of #1594 scope. Separate table =
  zero blast radius. Future unification (tax onto the dense ECB source) is a deliberate follow-up,
  not a side effect here.
- **R2 — why USD-base mirror, not full cross-rates.** §1.D — parity with `convert()`'s
  direct+inverse-only behaviour; no silent divergence between persisted and live-recomputed values.
- **R3 — why forward-only snapshot.** §1.B — decouples PR-A from PR-B; exact capture beats
  reconstruction for the days we're actually present for.
- **R4 — why own lane.** §13 — avoids the every-5min `db`-lane race; write-disjoint so safe.
- **R5 — why child table not JSONB.** §1.G — queryable audit evidence.

## §22 Open questions
- Snapshot of a mixed-currency portfolio with an unsupported native ccy (not USD/EUR/GBP) → that
  position is FX-skipped (under-stated), same as value-history. Dev is all-USD so untested live;
  pure-logic test covers the skip branch. Acceptable for v1 (long-only US/EU/UK universe).
- **Supported-but-cross-rate-skipped (Codex ckpt-1 M1):** EUR↔GBP are BOTH in
  `SUPPORTED_CURRENCIES` yet skip under the USD-base mirror (no direct/inverse pair; §1.D). A
  GBP-native holding with display=EUR (or vice-versa) is FX-skipped → `positions_no_fx`. This
  matches the live chart's current behaviour exactly (no regression). Adding USD-cross derivation
  is a deliberate future enhancement, out of PR-A scope; the skip is surfaced, not silent.
- Backfill of EOD snapshots for PAST days (before the job existed) is **explicitly PR-B**
  (needs `units_at_day`); PR-A only captures forward. PR-B's value-history recompute fills the
  pre-snapshot era from `trade_events`.

---

## Implementation plan (PR-A)

1. `sql/196_portfolio_value_v2_fx_eod.sql` (§4). Add tables to `_PLANNER_TABLES`.
2. `app/providers/implementations/frankfurter.py`: add `fetch_timeseries_rates(base, targets,
   start, end) -> dict[date, dict[tuple[str,str], Decimal]]` (plain GET, parse the empirically-
   verified shape; skip None values like the existing parsers).
3. `app/services/fx_history.py`:
   - `ensure_fx_history(conn, *, base='USD', targets, since)` — compute gap range vs sink MAX,
     fetch, upsert `ON CONFLICT DO NOTHING`. Pure `_missing_range(...)` helper for the fast test.
   - `load_fx_rates_for_date(conn, rate_date) -> dict[tuple,Decimal]` — most-recent ≤ date per
     pair (carry-forward). Pure `_pick_carry_forward(...)` helper.
4. `app/services/portfolio_eod.py`:
   - pure `compute_eod_equity(positions, cash_balances, closes, rates, display_ccy) ->
     EodEquity` (total/positions/cash + the §15 closed-set counters + per-position `price_status`)
     — table-tested.
   - pure `resolve_snapshot_date(price_dates, fallback) -> date` = `max(price_dates)` (§10 B2).
   - `compute_and_store_eod_snapshot(conn)` — derives `snapshot_date` from
     `MAX(price_daily.price_date)` over held instruments under `snapshot_read`, calls
     `ensure_fx_history` + `load_fx_rates_for_date`, writes snapshot + per-`position_id` rows
     (ON CONFLICT upsert; delete-then-insert children or upsert keyed on `(snapshot_date,
     position_id)`).
5. `app/workers/scheduler.py`: `JOB_PORTFOLIO_EOD_SNAPSHOT` ScheduledJob (`source="db_eod_snapshot"`,
   daily after US close ~22:30 UTC, gated `_bootstrap_complete`) + `JOB_FX_HISTORY_BACKFILL` constant.
6. `app/jobs/sources.py`: add `db_eod_snapshot` to `Lane`; bind **both** job sources to it
   (`MANUAL_TRIGGER_JOB_SOURCES[fx_history_backfill]="db_eod_snapshot"` for the triangle job; the
   scheduled job's `source=` is on the `ScheduledJob`). Same lane = serialised `fx_rates_daily`
   writers (§8/H2).
7. `app/jobs/runtime.py` `_INVOKERS` + `app/services/processes/param_metadata.py`
   `MANUAL_TRIGGER_JOB_METADATA` for `fx_history_backfill` (zero params).
8. Tests (§20). Registry-shape test for the new ScheduledJob.
9. Dev-verify (§17-19), record in PR. Restart jobs proc onto branch is an operator step.

PR-B (separate): `trade_events` `units_at_day`; value-history reads `portfolio_eod_snapshots` +
recomputes tail from `fx_rates_daily`; prominent surfacing + period selectors.
