# Portfolio value v2 — PR-B: ledger-units history + persisted/recompute read (#1594)

Scope: #1594 items **1 (`units_at_day` from `trade_events`)** + **5 (prominent surfacing /
period selectors)**, plus the read-side rewire of `GET /portfolio/value-history` to **read
persisted `portfolio_eod_snapshots` and recompute only the pre-snapshot era + today's tail**.
Closes #1594; epic #1593 closes after this.

Builds on PR-A (#1621, `39e936ee`): `fx_rates_daily`, `portfolio_eod_snapshots`,
`portfolio_eod_position_snapshots`, the `portfolio_eod_snapshot` job, `fx_history` read helpers.

**No schema change.** PR-B is read-path + FE only.

---

## §0 Grep proof

Every identifier verified by grep/read/dev-probe this session, not memory.

### Current consumer (audited AS-IS, post #1604/#1605 + PR-A)
- Endpoint: `app/api/portfolio.py:817` `get_value_history(range)` → `ValueHistoryResponse`
  (`:794` `display_currency, range, days, fx_mode, fx_skipped, points, events`).
- Units basis TODAY (`:894-1001`): **hybrid** — `fills`-replay CTE (`fills_signed`/`fills_units`,
  `:907-939`) for instruments with our own order fills, UNION `position_units` (`:957-977`) which
  back-dates **CURRENT** `positions.current_units` to `open_date`. Closed broker positions drop out
  (the #1594 gap-1 residual). #1604 fixed the fills half only.
- FX TODAY (`:858-859`): `load_live_fx_rates_with_metadata` → today's `live_fx_rates` applied to
  **every** historical day. `fx_mode="live"` (`:806`). ← gap 2, PR-B fixes via `fx_rates_daily`.
- Persistence TODAY: none; recomputed per request (`:1163`). ← gap 3, PR-B reads persisted.
- Events (`:1032-1088`): buy/sell markers from `fills` ∪ `positions` opens. PR-B re-sources from
  `trade_events` so markers and the line share one basis.
- **No `snapshot_read`** wraps the 3 cursors today (`:867`, `:897`, `:1004`, `:1039`) — PR-B adds it
  (prevention-log L1053: multi-statement read GET whose results must agree).
- FE: `frontend/src/components/dashboard/PortfolioValueChart.tsx` (lightweight-charts; dark-mode,
  buy/sell markers, hover tooltip, `?value=` URL-sync, `RANGES` 1m/3m/6m/1y/5y/max `:36-43`). All
  #1604 — **not** redone. Rendered on the **dashboard** (`grep -rn PortfolioValueChart frontend/src`
  → `pages/DashboardPage.tsx` only; see §3 for surfacing).

### Ledger source (read)
- `sql/194_trade_events.sql` `trade_events`: `position_id≥0`, `etoro_instrument_id`,
  nullable `instrument_id`, `event_kind∈{open,close}`, `side∈{buy,sell}`, `units>0`, `price>0`,
  `executed_at`, `investment_usd`, `realized_pnl_usd` (close only), `social_trade_id`, `source`.
  Partial uniques: `uq_trade_events_open (position_id) WHERE open`;
  `uq_trade_events_close (position_id, executed_at) WHERE close`.
- **Units contract** (sql/194 header): `open.units` = ORIGINAL opened units; each `close.units` =
  that slice's delta; partial closes reduce the SAME `position_id` → one open + N closes.
- **Dev probe (2026-06-13)** — 9 rows: 7 sync opens (the current holdings) + ILMN(4077) hist
  open 2025-08-12 @97.30 / close 2025-11-14 @120.56 (full, `realized_pnl_usd`=+1910.47).

### Snapshot source (read; PR-A)
- `portfolio_eod_snapshots(snapshot_date PK, display_currency, total_value, positions_value,
  cash_value, fx_rate_date, positions_total/priced/no_price/no_fx, cash_no_fx_currencies,
  computed_at)`. Dev: one row `2026-06-12` total £51,433.7024 (positions £50,162.6998 + cash
  £1,271.0026), `display_currency='GBP'`, 7/7 priced.
- `app/services/fx_history.py`: `load_fx_rates_for_date(conn, date) -> (rates dict, fx_rate_date)`
  carry-forward (DISTINCT ON, most-recent ≤ date per `(base,quote)`); `ensure_fx_history`.
- `app/services/fx.py:27` `convert(amount, from, to, rates)` — direct pair then inverse ONLY, no
  USD cross-rate (`:40-46`); raises `FxRateNotFound`. `fx_rates_daily` is USD-base so USD↔{GBP,EUR}
  convert, EUR↔GBP skips (mirrors live, no regression).
- `price_daily(instrument_id, price_date, close)` — dev coverage for held + ILMN(4077) spans
  2022→2026-06-12 (1000+ rows each), so closed-trade history renders.
- `cash_ledger(event_time, event_type, amount, currency, note)` — delta rows, `SUM` = running
  balance. **Dev: ONE row** (2026-06-03 broker-sync delta, USD 1703.46). Cash history is sparse
  pre-June by data availability — same as the current endpoint, NOT a PR-B regression (§22).

### Empirical identity (the central-design decider — dev probe)
For **every** open: `trade_events.investment_usd` == `broker_positions.amount` **exactly**, and
`trade_events.price` == `broker_positions.open_rate` **exactly**; `broker.amount − units·open_rate`
= spread cents (≤ £0.01), `leverage=1`, `is_buy=true` for all. → the recompute can reconstruct
PR-A's MTM inputs faithfully and is **boundary-continuous** with persisted snapshots (§1.A).

---

## §1 Decisions

**A. Equity basis — recompute uses PR-A's MTM formula, reconstructed from the ledger (HARD
CONSTRAINT).** The persisted snapshot stores `amount + units·(close − open_rate)` (long) per
position using live `broker_positions.amount`/`open_rate`. The recompute MUST use the **same
formula**, never naked `close × units`, or persisted-vs-recomputed days step-discontinuity at the
snapshot boundary. From `trade_events` we reconstruct, per open position, at day D:
- `open_rate := open.price` (native ccy, immutable).
- `units_at_D := open.units − Σ close.units (executed_at::date ≤ D)`; drop the position when ≤ 0
  (fully closed → it **leaves** the open-positions series on its close date; see §1.H for what
  happens to the proceeds — they are NOT synthesised into cash, and why).
- `cost_per_unit := native_cost_basis(investment_usd, open_units, native_ccy, open_rate, fx@open)`
  — the per-unit invested capital in the instrument's **native** ccy: `investment_usd` converted to
  native at the **open-day** FX, ÷ `open_units` (fallback `open_rate` when investment/FX absent).
- `amount_at_D := units_at_D · cost_per_unit`; `equity_native(D) := amount_at_D + units_at_D·(close
  − open_rate)`.
- **Both Codex-P2 horns covered.** (i) *Currency*: the amount term is native, never account-ccy
  `investment_usd` mixed with a native price delta — so non-USD holdings convert cleanly. (ii)
  *Leverage*: `investment_usd` carries the margin, so a 2× position prices to equity not notional
  (`10u@100, $500 invested, close 120 → 500 + 10·20 = 700`, matching the snapshot — NOT 1200). On
  USD-native rows `native_cost_basis` returns `investment_usd / open_units`, so `amount_at_D` equals
  `broker_positions.amount × units_at_D/open_units` **exactly** → persisted and recomputed days do
  not step (dev-verified: 2026-06-12 recompute == persisted to the rounding quantum). `position_equity`
  is the unchanged canonical formula — no `close × units` shortcut.
- **Nullable-ledger guard (Codex ckpt-1 M2):** `trade_events.price` is nullable in sql/194. An open
  with NULL/≤0 `price` cannot be marked to market → that position is **skipped** for all days
  (under-states; same posture as a missing close). SQL filters opens on `price > 0` (prevention-log
  L112: a usable price is strictly positive). A NULL `investment_usd` (or missing open-day native
  FX) falls back to `open_rate` cost (unleveraged-long native cost), keeping the position visible.
- `equity_native(D) := amount_at_D + units_at_D·(close_daily(D) − open_rate)` (long; v1 is
  long-only/unleveraged so only the long branch is implemented — but it is the **same** canonical
  PR-A formula, so it does not collapse to `close × units` and stays consistent with the snapshot).
- `value_display(D) := convert(equity_native, native, display, fx_for_date(D))`.

**Boundary continuity PROVEN empirically**: at the snapshot boundary the recompute's
`amount_at_D` (= `investment_usd`) equals the snapshot's `broker.amount` to the cent, and
`open_rate`/`close`/FX are the same inputs → recompute(D) ≡ persisted(D) modulo rounding, no step.
This is **not** `close × units`: it numerically coincides only because `amount ≈ units·open_rate`
for unleveraged long; the formula stays MTM-correct off that special case.

**B. Read = persisted base-or-recompute, recompute is the always-present floor.** Build the full
recompute series for `[start, today]`. Then **overlay** persisted `portfolio_eod_snapshots`:
for each day D with a snapshot row **whose `display_currency` == the requested display ccy**,
override the recomputed point with `snapshot.total_value` (auditable, == what the dashboard showed
that day). Days without a matching snapshot (pre-job era, today's tail, or a display-ccy switch)
keep the recompute. This satisfies "reads persisted + recomputes the tail" while never leaving a
gap, and a display-ccy switch falls back to recompute rather than mislabelling a GBP snapshot as
USD (Codex-anticipated edge).

**Boundary-drift invariant (Codex ckpt-1 H1).** PR-A stamps `snapshot_date = MAX(price_daily.
price_date)` but reads `broker_positions` at compute time, so a persisted day can reflect a
position/cash change that landed *after* its price date but before the ~22:30 job ran (PR-A's
documented "captured forward, not exact as-of close" honesty bound). The ledger recompute, keyed on
`executed_at::date ≤ D`, dates that change to its true day. Consequence: where overlay shows a
persisted day, it shows **what we recorded** (auditable); recompute fills the rest. A step can only
appear at the seam between an overlaid day and an adjacent recomputed day, and only if a trade
executed inside that intra-day capture window — in which case the step is a **real** capital
movement dated one session apart, bounded by PR-A's `computed_at` audit field, never a phantom.
Enforced by a DB test: for a ledger with **no** trade between `snapshot_date` and the next session,
`recompute(snapshot_date) == persisted.total_value` to the rounding quantum (the dev case, §19).

**C. FX = `fx_rates_daily` carry-forward, per day (fixes gap 2).** Replace the live-snapshot FX
with dated rows. **Seed-row requirement (Codex ckpt-1 M1):** carry-forward needs the most-recent
rate on/before `start_date`, which may pre-date the window (weekend/holiday/arbitrary range start).
So the fetch is `SELECT base,quote,rate,rate_date FROM fx_rates_daily WHERE rate_date <= today
ORDER BY base,quote,rate_date` (the whole history up to today — ~hundreds of rows, cheap), and the
Python carry-forward walks each pair's rows, holding the latest `rate_date ≤ D` as it advances day
by day. A day earlier than a pair's first row has no rate for that pair → FX-skip (`fx_skipped`),
as today. `fx_mode` becomes `"historical"`.
`ensure_fx_history` is **NOT** called from the read path (no HTTP in a GET) — the scheduled job /
manual `fx_history_backfill` populate it; a day with no FX row on/before it FX-skips that pair
(`fx_skipped`), exactly as today.

**D. Units/markers single-sourced from `trade_events`.** Replaces the `fills`+`positions` hybrid
wholesale. `open` → BUY marker, `close` → SELL marker. Mirror rows (`COALESCE(social_trade_id,0)≠0`)
excluded, consistent with the snapshot and `/portfolio/activity`. This fixes gap 1 (closed
positions now in history) and keeps markers and the line on one basis.

**E. Cash unchanged.** Still `cash_ledger` `SUM(amount)` cumulative per currency per day, converted
via the new per-day FX. Cash history is as dense as `cash_ledger` (sparse on dev); out of scope to
backfill (§1.H, §22).

**F. No schema, no new job, no new lane.** Pure read-path + FE. PR-A's job already persists going
forward.

**G2. Closed-position proceeds are NOT synthesised into cash (Codex ckpt-1 H2).** An earlier draft
claimed a full close "transfers to cash" — **false and removed**. The series is precisely
**mark-to-market of OPEN positions + tracked `cash_ledger` cash**, nothing more. Why no synthesis:
every reconstruction of trade-driven cash from `trade_events` is provably wrong without the deposit
history we do not have —
(a) crediting close-proceeds *and* debiting open-investment double-removes value (the open position
already carries that value) and goes deeply negative without a modelled initial deposit;
(b) crediting close-proceeds only double-counts when proceeds are **reinvested** (dev: ILMN closes
2025-11-14 19:24 → BBBY opens 19:25 — the proceeds roll straight into a new position already counted);
(c) layering synthesised proceeds onto `cash_ledger` double-counts at its bootstrap reconcile row
(2026-06-03 records `delta = broker − local` assuming `local = 0`, i.e. a set-to-truth).
**Steady-state is correct without synthesis**: when a position closes after cash-tracking began, the
broker-sync reconcile writes the proceeds into `cash_ledger`, so the close becomes continuous **once
that cash event is dated** (position leaves, cash rises). If the reconcile lands on `D+1` rather than
the close day `D`, day `D` can show a one-session dip before the proceeds appear — bounded by the
reconcile's `cash_ledger.event_time`, a sync-cadence artefact, not a phantom; it self-heals on the
next session. The only gap is the **pre-cash-tracking backfill era**
(before `MIN(cash_ledger.event_time)`), where a closed position's proceeds are untracked — a
`cash_ledger` data-availability limit, surfaced via an FE caption (§FE) and the docstring, **not**
papered over with a wrong number. This is strictly more honest than the status quo, where closed
positions never appeared at all.

**G. Surfacing — keep on the dashboard, add to the Portfolio page header (§3).** Period selectors
already exist (`RANGES`); reuse the component, do not fork the range set.

---

## §2 Identifiers + identity-drift
- Recompute keys on `trade_events.position_id` (one open each), aggregates to `(day, native_ccy)`
  for FX then to `(day)` for the series. `instrument_id` may be NULL for deep-history rows — those
  cannot price (no `price_daily`/`instruments.currency`) → FX/price-skip, same as a missing close.
  Dev ILMN has `instrument_id=4077` (resolved), so it prices.
- No new persisted identifiers. Persisted overlay keys on `snapshot_date`.

## §3 Endpoint surface
- `GET /portfolio/value-history` — **same path**; response model gains ONE additive nullable field
  `cash_tracking_since: date | null` (for the §1.G2 caption). `fx_mode` value changes `"live"` →
  `"historical"`. `fx_skipped`, `points`, `events`, `days`, `range`, `display_currency` unchanged.
- No new endpoint. No new manual trigger.
- **Surfacing**: `PortfolioValueChart` already on `DashboardPage`. Add it to the Portfolio page
  (`frontend/src/pages/PortfolioPage.tsx`) header region above the Positions/Activity tabs, reusing
  the same component + `?value=` URL key (already namespaced distinct from `?chart=`).
- **FE captions (replace the `fx_mode==="live"` copy)**: (a) `fx_mode==="historical"` →
  "historical FX from ECB daily rates · excludes copy-portfolio equity" (drops the "today's FX"
  approximation wording, now fixed). (b) When the series reaches before cash-tracking began (the
  endpoint exposes the earliest tracked-cash date, or the FE infers from a flag), a muted note
  "cash history limited before <date>" so the closed-trade era reads honestly (§1.G2). The simplest
  surface: add a nullable `cash_tracking_since: date | null` to `ValueHistoryResponse` and caption
  when `points[0].date < cash_tracking_since`.

## §4 Schema
- **None.** No `sql/NNN`. (Confirms: `grep` shows highest = `sql/196` from PR-A; PR-B adds nothing.)

## §5 Fetch strategy + rate-limit composition
- **No external fetch on the read path.** FX rows are pre-populated by PR-A's job. `derive` only.

## §6 Conditional-GET semantics
- N/A (no HTTP in PR-B).

## §7 Retry posture per error-class
- Read endpoint: a missing `price_daily` close on/before D → that position skipped for D
  (under-states, never invents a zero — mirrors current `:1102`, and prevention-log L112: a usable
  close is STRICTLY `> 0`, guarded in SQL with `AND close > 0` / `NULLIF`).
- A missing FX pair for D → that `(from,to)` added to `fx_skipped`, row dropped (current behaviour).
- No persisted snapshot for D → recompute (the floor); never an error.

## §8 Multi-writer sink registry
- **No writes.** PR-B reads `trade_events`, `price_daily`, `cash_ledger`, `fx_rates_daily`,
  `portfolio_eod_snapshots`, `instruments`. Append-only / immutable sources.

## §9 Watermark + retry-budget
- N/A (read path). `range=max` start = `LEAST(MIN(trade_events.executed_at::date),
  MIN(cash_ledger.event_time::date))`, fallback `CURRENT_DATE` (replaces the fills/positions floor).

## §10 Encoding / precision / NULL / timezone
- `Decimal` throughout; `Decimal(str(x))` for every nullable/3rd-party numeric (prevention-log
  L972, L1199). `units NUMERIC(20,8)`, money `NUMERIC(20,4)`, price `NUMERIC(20,8)`.
- Day grain: `executed_at::date` and `price_date` are UTC trading days; `cash_ledger.event_time`
  truncated `(event_time AT TIME ZONE 'UTC')::date` to match PR-A's snapshot cash read.
- `float()` only at the response boundary (`ValueHistoryPoint.value`), as today.

## §11 Backfill horizon + retention
- N/A — read path. History depth bounded by `trade_events` + `price_daily` availability.

## §12 Partition strategy
- N/A.

## §13 Bootstrap vs steady-state + lane
- N/A — no job, no lane. (PR-A's `db_eod_snapshot` job unchanged.)

## §14 Tombstones + soft-delete
- N/A.

## §15 `rows_skipped` closed-set
- Read endpoint reuses the existing `fx_skipped` (distinct `(from,to)` pairs dropped). Position
  rows with no close on/before D are silently skipped for that day (under-statement, documented in
  the docstring), exactly as today. No new counter surface.

## §16 Schema-evolution migration path
- None (no schema).

## §17 Operator runbooks
- FX history / snapshots are populated by PR-A's jobs (`fx_history_backfill`,
  `portfolio_eod_snapshot`). PR-B adds no runbook; the chart simply reflects whatever those jobs
  have persisted plus the live recompute tail.
- **Verify**: `GET /portfolio/value-history?range=max` → assert ILMN appears as a BUY marker
  (2025-08-12) and SELL marker (2025-11-14) and contributes to `points` only within that window;
  assert the `2026-06-12` point equals the persisted snapshot total (£51,433.70).

## §18 Smoke matrix (PORTFOLIO panel)
- Dev holdings (VOO/QQQ/IEP×2/GME×2/BBBY, all USD) + the closed ILMN trade.
- `range=max`: ILMN priced Aug 13–Nov 13 2025, gone from Nov 14; 7 current holdings priced
  throughout their windows; `events` carries ILMN BUY+SELL markers; `fx_mode="historical"`.
- `2026-06-12` point == persisted snapshot `total_value` (overlay proven).
- A pre-snapshot day (e.g. `2026-03-01`) is recomputed (no snapshot row) and continuous with
  06-12 (no step).

## §19 Cross-source verification
- **Exact (overlay continuity)**: recompute `2026-06-12` independently (trade_events units × the
  same `price_daily` close × the same `fx_rates_daily` rate + cash) and assert it equals the
  persisted snapshot `total_value` to the rounding quantum — proves recompute and snapshot agree.
- **Independent (closed trade)**: ILMN holding-period 94.1093 d and realised +$1910.47 already
  cross-validated (#1611). Assert ILMN value on a mid-window day = `units(82.1355) × close(D)` in
  USD→GBP, spot-checked against `price_daily` directly.
- **Directional**: `range=1m` latest point within an order of magnitude of `GET /portfolio` AUM
  (ex-mirror) — not equality (EOD close vs live quote), a sanity band.

## §20 Test placement
- **Pure-logic (fast tier)** — extract the decision into pure functions, table-test:
  - `reconstruct_units_at_day(open_units, closes:[(date,units)], D) -> Decimal` (open day, partial
    close, **multiple partial closes**, full close → 0/drop, close-after-D ignored — Codex ckpt-1 L1).
  - `position_equity(amount_at_D, units_at_D, open_rate, close) -> Decimal` (the MTM identity;
    assert == `close×units` when `amount==units·open_rate` (the endpoint's native basis), and ≠ when
    a smaller/leveraged amount is passed — locks the formula against a naive `close×units` rewrite).
  - skip branch: NULL/≤0 `price` (M2) → position contributes nothing.
  - `carry_forward_rate_map(fx_rows, days) -> {day: rates}` (gap day carries prior; day before
    first rate → empty/skip).
  - `overlay_persisted(recomputed:{day:val}, snapshots:[(day,total,ccy)], display) -> {day:val}`
    (matching-ccy override; mismatched-ccy keeps recompute; tail untouched).
- **DB tier (one test)**: `get_value_history` against a seeded ledger with one open-then-closed
  position + one still-open + one snapshot row — assert closed position present pre-close / absent
  post-close, snapshot day overlaid, `events` has the open+close markers. ONE integration test for
  the genuinely-new read mechanism (CLAUDE.md test-tiering).
- **Frontend**: `PortfolioValueChart` already tested; add/adjust for `fx_mode==="historical"`
  caption and the Portfolio-page mount (`vitest`, `pnpm --dir frontend test:unit`).

## §21 Rationale log
- **R1 — why reconstruct `amount`/`open_rate` not use `close×units`.** The HARD CONSTRAINT: persisted
  snapshots use MTM `amount + units·(close−open_rate)`; a `close×units` recompute would step at the
  boundary for any leveraged/spread row. Reconstruction from `investment_usd`/`price` (== broker
  values to the cent, §0) keeps the formula identical and the boundary seamless.
- **R2 — why overlay persisted on a recompute floor (not persisted-only with recompute-gaps).** The
  recompute covers every day (pre-job era + tail + ccy-switch) with no holes; persisted refines the
  days it owns for auditability. Simpler and gap-free vs stitching sparse persisted rows.
- **R3 — why single-source units from `trade_events`, dropping the fills/positions hybrid.** Closed
  broker positions never had fills (broker-synced) and aren't in `positions` once closed → the only
  honest source for closed-trade history is the ledger. Keeping the hybrid would double-count a
  ledger open against a residual `positions` row.
- **R4 — why no `ensure_fx_history` on the GET.** No HTTP in a request path; the job owns
  population; missing FX degrades to `fx_skipped` exactly as today.
- **R5 — why `snapshot_read` now.** Recompute + persisted-overlay + events read the same logical
  state across 3+ statements; they must agree (prevention-log L1053). The current endpoint predates
  this rule; PR-B brings it into compliance.

## §22 Open questions / documented limitations
- **Cash history sparsity + closed-position proceeds (§1.G2).** `cash_ledger` has a single bootstrap
  delta on dev (2026-06-03); cash before then reads as 0 and a pre-tracking close's proceeds aren't
  booked. `cash_ledger` data-availability limit, **identical to the current endpoint**, NOT
  introduced by PR-B. Steady-state closes ARE captured (broker-sync reconcile). Open-position MTM
  history is fully correct. FE caption flags ranges reaching before `MIN(cash_ledger.event_time)`;
  endpoint docstring states the bound. A trade-driven cash reconstruction needs deposit history we
  lack and is provably uncorrectable here (§1.G2) → separate ticket.
- **Non-USD `amount` semantics.** `investment_usd` is USD; the MTM formula treats `amount` as native
  ccy (mirroring PR-A's `broker.amount`). Exact for USD-native instruments (all of dev + the v1
  US/EU/UK long-only universe is USD on dev). A non-USD-native holding inherits PR-A's same latent
  approximation (spec PR-A §22) — flagged, not silently wrong, and FX-skipped if its pair is
  cross-rate-only (EUR↔GBP).
- **Per-instrument vs per-position.** Recompute is per `position_id` then summed — strictly more
  correct than the current per-instrument aggregation (two GME positions priced on their own
  open_rates).

---

## Implementation plan (PR-B)

1. `app/services/portfolio_value_history.py` (new) — pure cores: `reconstruct_units_at_day`,
   `position_equity`, `carry_forward_rate_map`, `overlay_persisted` (+ dataclasses). Table-tested.
2. `app/api/portfolio.py` `get_value_history` rewrite:
   - `start_date` from `trade_events`/`cash_ledger` (§9).
   - **SQL** (under `snapshot_read`): per `(day, position)` native equity via `generate_series` ×
     `trade_events` opens, LATERAL close-sum (`executed_at::date ≤ d`), LATERAL most-recent
     `price_daily.close > 0` (`≤ d`) — emit `equity_native`, `native_ccy`, drop `units_at_D ≤ 0`
     and mirror rows. Cash query unchanged. Events from `trade_events` (open=BUY, close=SELL).
   - **Python**: build per-day FX carry-forward map from one `fx_rates_daily` range read; `convert`
     per `(day, ccy)`; sum to per-day; overlay persisted snapshots (matching display ccy).
     `fx_mode="historical"`.
3. FE: `PortfolioValueChart.tsx` caption `fx_mode==="historical"` wording; mount on
   `PortfolioPage.tsx` header. Adjust `frontend/src` tests.
4. Gates: ruff/format/pyright, `pytest -m "not db"` + smoke; scoped `-m db` for the new read test;
   `pnpm --dir frontend typecheck` + `test:unit`.
5. Codex ckpt-2 (`codex exec review --base main`) before first push.
6. Dev-verify §17-19 against the live `:8000` endpoint; record figures + commit SHA in the PR.
