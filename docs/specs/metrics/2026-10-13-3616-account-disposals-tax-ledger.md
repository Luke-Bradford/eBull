# The account's broker trades in the UK tax ledger (#3616 slice 3)

Source: #3616 (committee review, 2026-10-04): *"GBP after-tax P&L against the UCITS/ISA comparator."* Slice 2
(`docs/specs/metrics/2026-10-12-3616-gbp-isa-comparator.md`, "Why this slice computes no after-tax return") decided
that an after-tax figure must come from disposals matched under HMRC rules in `tax_ledger`, with each instrument's tax
character from its source, and that `tax_ledger` holds none of the engine's trades today. The prevention-log entry "A
tax rate applied to a TWR is not an after-tax return (#3616)" makes the same rule binding.

This slice puts the account's real trades into `tax_ledger`, correctly. It publishes the account's tax-year view on
the existing `/tax` surface. **It does not yet publish an engine-only after-tax return or compare one with the
comparator**; that is slice 4, and it needs this one (see "Not in this slice").

## Facts established first

All figures are from `scripts/measure_3616_slice3_population.py`, run on the dev DB on 2026-10-13; output
`docs/research/3616-slice3-population-2026-10-13.json`.

1. **`tax_ledger` has never priced a USD trade.** It reads only `fills` (`tax_ledger._ingest_fills`). The tax
   `fx_rates` table (sql/013) has no writer anywhere in `app/`, `scripts/` or `sql/` (grep `INSERT INTO fx_rates`), so
   `_load_fx_rate` raises for any USD fill. `tax_lots` holds 0 rows on dev.
2. **`fills` is the app's own order record, not the account's.** Its writers are `order_client._persist_fill` and
   `app/api/orders.py` (orders the app placed). The dev DB holds 1 fill: a demo stub, `broker_order_ref`
   `DEMO-1129-BUY` (WDC), with no counterpart in the broker record. Trades placed in the eToro app, and the engine's own
   executors, never write `fills`.
3. **The broker's record is `trade_events`.** 36 rows: 16 `etoro_sync/open`, 7 `etoro_history/open`, 13
   `etoro_history/close`. Every row carries the broker's order id (`orderID` on sync rows, `orderId` on history rows;
   0 rows without one).
4. **A partial close is booked under a new position id.** Order 383293149 (IUSA.L) bought 0.646048 units for $50.00
   (sync `initialUnits`, `initialAmountInDollars`). History then shows position 3602947861, with the same `orderId`
   and `openTimestamp`, `units` 0.323024, `investment` 25.0, `initialInvestment` 50.0, closed 13 s after the open; the
   original position 3602947846 kept 0.323024 and closed 2.5 minutes later. Both history rows carry
   `parentPositionId` 0, so the lineage is visible only through `orderId`. Order 383320185 (AAPL) has the same shape.
   `trade_events` therefore stores an `open` row for a position that was never a separate purchase; treating each
   `open` row as an acquisition would double the acquisition.
5. **Units and dollars reconcile per order.** For all 21 orders, acquired units (sync `initialUnits` where observed,
   otherwise the sum of the closed pieces) equal disposed plus held units. For every fully closed order, the pieces'
   `investment` sums to the order's `initialInvestment` / `initialAmountInDollars`.
6. **Proceeds identity.** On all 13 close pieces, `fees` is 0 and `investment + netProfit` equals the stored
   `investment_usd + realized_pnl_usd`.
7. **Settlement type is per position and only on the open-positions payload.** eToro documents `settlementTypeID` as
   "Position investment type. 0 - CFD, 1 - Real Asset, 2 - SWAP, 3 - Crypto MarginTrade, 4 - Future Contract"
   (`app/providers/broker.py:433`). Neither the trade-history endpoint nor the close-order endpoint returns it (live
   portal, 2026-10-13: `trading--demo/list-trading-history.md`,
   `trading--demo/get-close-order-information-and-closed-position-details.md`). We hold it for positions the sync
   observed open (`trade_events` sync payloads, `broker_positions`, `broker_positions_closed`).
   - Orders by character: **10 real, 6 non-real (all `settlementTypeID` 0, CFD), 5 unestablished** (history-only,
     never observed open: ILMN, AAPL, CSPX.L ×3).
   - Close pieces: 5 real, 2 CFD, 6 unestablished.
   - Instrument type does not predict it: IEP is eToro instrument type 5 (stock) and was a CFD; VOO, QQQ, SVXY and SVOL
     at x1 long were CFDs.
8. **Instrument currency is not reliable for money.** CSPX.L is stored with `instruments.currency` USD, yet its
   `units × openRate` (37.46) is not its $50 investment. The broker's own USD amounts are consistent (fact 5).
9. **Account and environment.** `broker_credentials` holds 2 rows, both `demo`. `trade_events` has no environment
   column.

## Source rules

- **Conversion to sterling.** HMRC CG78310: *"an amount of foreign currency must be converted into its sterling value
  at the time the amount is incurred/received"*, at *"the exchange rate in force at the date"*; *"HMRC does not
  prescribe what reference point should be used for the exchange rate"*, but *"a reasonable and consistent method"* is
  expected.
- **Consideration.** TCGA 1992 s38(1)(a): the allowable cost is *"the amount or value of the consideration, in money
  or money's worth, given by him … wholly and exclusively for the acquisition of the asset"* plus incidental costs.
  The consideration this account gives is the USD it pays (`initialAmountInDollars`); the disposal consideration is the
  USD it receives (`investment + netProfit`).
- **Identification.** Same-day (s105), 30-day (s106A) and the s104 pool, all already implemented by
  `tax_ledger._match_disposals_for_instrument` and unchanged here. They apply to "securities of the same class", so the
  pool key is the security, not the eToro listing (Design 4).
- **Non-reporting offshore funds.** SI 2009/3001 regs 17–18: a gain on disposal of an interest in a non-reporting fund
  is an offshore income gain, *"treated for all the purposes of the Tax Acts as income"*. Reg 42 (HMRC IFM13550): a
  loss gives a basic gain of nil; the loss *"can be relieved only as a capital loss"* and *"cannot be offset against
  offshore income gains of the same or of a later period."*
- **Reporting funds.** Disposals are chargeable gains (HMRC IFM12300). Excess reportable income is income of the
  holder and adds to base cost; this slice does not model it (Limits).
- **Fund status.** HMRC "Offshore funds: list of reporting funds" (`20261006_Master-Weblist.ods`, the file and lookups
  recorded on #3781): CSPX (IE00B5BMR087) and IUSA (IE0031442068) are reporting funds; SPY (US78462F1030) is not on
  the list.
- **CFDs.** HMRC CG56100 places retail CFDs within capital gains (TCGA s143) unless trading; CG56101's worked example
  covers dealing in one contract at a time, and financing payments enter the computation. That is a different
  computation from share identification, and CFD financing is not stored per position here. CFDs are out of this slice
  (Limits).

## Design

### 1. Tax FX: Bank of England daily spot

- **Source.** Bank of England series XUDLUSS, "Spot exchange rate, US $ into Sterling" (USD per GBP), from the public
  database CSV endpoint, no account (the URL is in the measurement script's docstring). It is the UK central bank's
  daily rate, which meets CG78310's "reasonable and consistent" test and is one method for every date.
- **Stored** in the tax `fx_rates` table as `(rate_date, 'USD', 'GBP', 1 / XUDLUSS)`, rounded half-even to the
  column's 10 decimal places, with a new `source` column (`'boe_xudluss'`, CHECKed). Insert-once per key; a re-fetch
  that disagrees with a stored row refuses and logs, never overwrites. `fx_rates_daily` (ECB) stays separate, per the
  settled decision "Own EOD NAV-snapshot table (#1594)".
- **Which rate a date takes** (`_load_fx_rate`, USD only):
  - a publication day takes its own rate;
  - a non-publication day takes the latest earlier publication, provided a later publication exists (so the day is
    known to be a non-publication day, not merely unfetched) and the earlier one is at most 4 calendar days before it.
    The bound is the series' widest gap since 1975, 5 days between publications (94 occurrences, latest 2026-04-02 →
    2026-04-07), less one;
  - otherwise the event is **deferred**: not ingested this run, counted, and retried next run. Deferral replaces the
    current raise for this case only; a date older than the series still raises.
- GBP needs no rate. Any other account currency refuses (the account currency is USD; fact 9 and
  `broker_account_session_close_snapshots.currency`).

### 2. Account events → tax lots

`_ingest_trade_events(conn)` replaces `_ingest_fills` as the only lot source. `fills` is no longer read by
`tax_ledger` (fact 2: it is a subset of the account's trades, and reading both would count app-placed trades twice).

- **One acquisition per broker order.** Group `trade_events` by order id.
  - Time: the order's `openTimestamp` (the earliest `executed_at` of its open rows; all agree on the measured
    population).
  - Units: the sync open row's `initialUnits` when observed; otherwise the sum of the order's close pieces' units, and
    then only when the order holds no open position.
  - Consideration (USD): `initialAmountInDollars` (sync) or `initialInvestment` (history). Where both exist they must
    be equal.
  - Completeness check: acquired units = Σ disposed units + Σ units held under the order's positions in
    `broker_positions`, and for a fully closed order Σ pieces' `investment` = consideration. A failing order refuses
    (all its lots and pieces are skipped and counted `order_incomplete`), never part-ingests.
- **One disposal per close piece** (`trade_events` `close` row): units = `units`; proceeds (USD) = `investment +
  netProfit`; time = `executed_at`. A piece with non-zero `fees` refuses (`fees_unmodelled`): the history endpoint
  documents `fees` only as "the fees of the trade", and fact 6 shows none.
- **Long only.** `isBuy` false refuses, `leverage` ≠ 1 refuses (a real asset is x1).
- **GBP amount** = USD amount × the event's UK-date rate (Design 1). `tax_lots.cost_or_proceeds` keeps the USD figure,
  `original_currency` 'USD', `fx_rate_to_gbp` the rate used.
- **Idempotency.** Migration `sql/484_tax_lots_broker_events.sql` adds `tax_lots.source` (`'fills'` | `'trade_events'`),
  `reference_order_id BIGINT` (unique among `trade_events` acquisitions) and `reference_trade_event_id BIGINT
  REFERENCES trade_events(event_id)` (unique among disposals), with a CHECK that a `trade_events` row has exactly the
  reference its direction needs. The existing `reference_fill_id` index is unchanged. Ingest is `ON CONFLICT DO
  NOTHING` on those indexes, as today.

### 3. Tax character

- **Settlement type, per order.** Union of every stored `settlementTypeID` for the order's positions (sync payloads,
  `broker_positions`, `broker_positions_closed`).
  - `{1}` → real asset: eligible for matching.
  - any other single value → `excluded_non_real` (CFD and the rest; Limits).
  - empty → `excluded_unestablished`.
  - more than one value → refuse (`settlement_conflict`).
- **Instrument character, for a real asset.** New table `tax_instrument_character (instrument_id PRIMARY KEY,
  character, source_ref, recorded_at)`, `character` ∈ `share`, `reporting_fund`, `non_reporting_fund`, seeded by the
  migration for the 7 real-asset instruments now in `trade_events`, each with its source:
  - `share`: AAPL (1001), GME (1699), ILMN (4077), NXH (1181), the company's own equity;
  - `reporting_fund`: CSPX.L (3434), IUSA.L (3075), the HMRC list rows on #3781;
  - `non_reporting_fund`: SPY.RTH (3417), absent from that list (#3781).
  An instrument with no row is `excluded_character_unknown`. Adding a row is a reviewed migration; automating it
  from HMRC's list is a follow-up.
- Excluded lots are still written to `tax_lots` with their exclusion reason (new nullable `exclusion_reason` column),
  so the record is complete and the count is auditable, but `run_disposal_matching` reads only lots whose
  `exclusion_reason` IS NULL.

### 4. Matching

Unchanged rules (`_match_disposals_for_instrument`). The group key becomes the security:
`COALESCE(instruments.canonical_instrument_id, instrument_id)`. SPY.RTH (3417) has `canonical_instrument_id` 3000
(SPY), whose only trades are CFDs, so the key changes no current result; it prevents two listings of one security
from forming two pools. `disposal_matches.instrument_id` and `s104_pool.instrument_id` store the key.

### 5. The tax-year view

`TaxYearSummary` gains, all computed in the engine (the `tax-ledger` skill's invariant):

- `offshore_income_gains_gbp`: the sum of positive per-disposal results on `non_reporting_fund` securities. Never in
  `net_gain_gbp`, never sheltered by the annual exempt amount (regs 17–18).
- A negative per-disposal result on a `non_reporting_fund` security is a capital loss and enters
  `total_losses_gbp` (reg 42).
  - "Per-disposal result" is the sum of that disposal lot's matches, so a disposal matched partly same-day and partly
    from the pool nets within itself before the sign is read. That is the reg 42 "basic gain" of one disposal.
- `excluded_disposals`: a count per exclusion reason for the tax year, and `deferred_fx` for the run, so a reader sees
  what the figure leaves out.
- `account_environment`: the environment of the account whose record was read. With credentials for more than one
  environment, the summary refuses (`account_environment_ambiguous`) until `trade_events` carries an environment.

The `estimated_cgt_*` scenarios keep their meaning and apply to `net_gain_gbp` only. No income-tax estimate is made
for offshore income gains; the operator's income band is unknown to the engine.

### 6. API and page

`GET /tax/summary` returns the new fields. `TaxPage` shows, under the existing gains block:
- "Offshore income gains (taxed as income, not CGT)" with the figure, shown only when non-zero;
- a line listing exclusions with their counts, for example "Not included: 2 CFD disposals, 6 with unknown product
  type";
- the account label, "Demo account — illustrative", while the environment is `demo`.

### 7. Job

`daily_tax_reconciliation` fetches the BoE rates it needs (the earliest un-ingested event's UK date to today), then
ingests and matches as today. It gains a daily schedule (07:20 UTC, after BoE's prior-day publication) on its existing
`db` lane, so no new lane or connection is added. A fetch failure leaves events deferred and the job `degraded`, never
failed silently.

## Local policy choices (no published rule; frozen here)

- BoE XUDLUSS as the reference (CG78310 leaves the choice open).
- The non-publication rule and its 4-day bound (Design 1).
- An order as the unit of acquisition (fact 4; eToro documents no partial-close lineage).
- Exclusion rather than inference when settlement type is unknown (fact 7).

## Not in this slice, and limits

- **Engine-only after-tax return and the comparator comparison (slice 4).** Tax is the person's: the pool and the
  30-day rule run across every holding of a security, manual and engine alike, so the engine's tax is not separable by
  filtering lots. Slice 4 must choose and justify an attribution rule (for example, the engine book computed as if it
  were the person's only holding) before any engine after-tax figure exists.
- **CFDs.** Out until per-position financing is stored and CG56101's computation is specified.
- **Dividends and distributions.** `_ingest_cash_events` stays a stub; IUSA.L distributes and SPY.RTH pays dividends,
  so the view omits dividend income.
- **Excess reportable income** on reporting funds is not added to base cost, which overstates gains on CSPX.L and
  IUSA.L disposals.
- **Holdings outside this eToro account** are invisible, and they share the person's pools.
- **History-only positions.** 5 of 21 orders cannot be classified from any documented source. Engine orders could
  record the settlement type they request (`etoro_broker.py`, `settlementType` on the open request); that is a
  follow-up, not this slice.
- **Demo.** Demo trades are not taxed. The view is a rehearsal of the computation on demo data and says so.

## Tests

- Pure: the order grouping on the IUSA.L and AAPL partial-close shapes (one acquisition, two disposals); the
  completeness refusals; settlement-type resolution including the conflict case; the FX date rule at a publication day,
  a weekend, a 5-day Easter gap, a gap longer than the bound, and an unfetched latest date; reg 42 routing for a
  non-reporting gain, a loss and a mixed same-day-plus-pool disposal.
- DB: one integration test of `_ingest_trade_events` against seeded `trade_events` and `fx_rates`, asserting the
  idempotency indexes (a second run inserts nothing) and the exclusion reasons.
- Existing `tests/test_tax_ledger.py` fill tests move to the new source or are removed with the `fills` path.

## Acceptance (dev DB)

After the build merges and `daily_tax_reconciliation` runs:
- `SELECT source, direction, exclusion_reason, count(*) FROM tax_lots GROUP BY 1,2,3` gives 21 acquisitions and 13
  disposals from `trade_events`; among disposals, 5 with no exclusion, 2 `excluded_non_real`, 6
  `excluded_unestablished`.
- `/tax/summary?tax_year=2026/27` shows the matched disposals (GME ×2, SPY.RTH, IUSA.L ×2), the exclusion counts and
  "Demo account — illustrative".
- One disposal cross-checked by hand: units, USD proceeds, BoE rate for its UK date, GBP figure.

## Full-population verification

The rule was run read-only over every `trade_events` row (facts 3–7), not a sample: 21 orders, 36 events, all units
reconciled and 0 rows without an order id.

## Security

Read-only additions to an authenticated read endpoint and an existing job. The BoE fetch is an unauthenticated GET to
a fixed URL with a parsed, range-checked numeric response. No order, capital or authorisation path changes.
