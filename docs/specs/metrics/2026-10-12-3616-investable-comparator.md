# Engine pot vs the investable S&P 500 comparator, and the active hurdle (#3616 slice 1)

Source: #3616 (committee review, 2026-10-04). *"The UK investable comparator is a UCITS S&P total-return product in an ISA (US ETFs are CFD-only on this account) … Beating SPY inside one sleeve ≠ the engine beating SPY … Publish the sleeve attribution and the required active excess."*

## Problem

The Strategies page shows the pot's time-weighted return (`/strategies/wealth-history`, #3334) but nothing to judge it against. `BENCHMARK_REFUSALS` (`app/api/strategies.py:886-934`) refuses an **S&P 500 total-return index** line for two reasons:
- *"We have reviewed no free S&P 500 total-return source and found it legally usable"*. This is a review status, not a finding about licence terms.
- Every S&P series we hold is a tracking ETF's price (SPY, IVV, VOO) or eToro's SPX500 price index.

That refusal stays exactly as it is. This slice adds something different: the return of an investable UCITS S&P 500 fund as our own broker quotes it. Whether eToro delivers this listing as the fund itself or as a CFD, and its ISA eligibility, are not established here; they belong to the GBP/ISA slice.

## Scope

Slice 1 adds three things over the pot's own TWR window:
1. the comparator's market-price return;
2. the engine's excess over it;
3. a counterfactual active hurdle at the current mandate's weights.

Out of scope (later #3616 slices): GBP, tax and ISA wrappers; realised sleeve attribution; per-strategy risk-adjusted metrics.

## Source rule

### Comparator identity

- **The fund.** iShares Core S&P 500 UCITS ETF, the **USD (Accumulating)** share class, ISIN IE00B5BMR087. The issuer's product page lists its ticker as CSPX (ishares.com/uk/individual/en/products/253743, read 2026-10-12).
- **The local series.** The eToro instrument with symbol `CSPX.L` has name "iShares Core S&P 500 UCITS ETF", currency USD and `exchange` '7' (LSE). It is mapped to that class by ticker, exchange and listing currency.
  - The issuer page names the fund's other classes: EUR Hedged Acc, GBP Hedged Dist and MXN Hedged Acc. None of them is the unhedged USD class that the CSPX ticker identifies.
  - Our `instruments` table holds no ISIN, so the mapping rests on ticker, exchange and currency. It is resolved at runtime by symbol, never by a fixed id (`instrument_id` 3434 on the dev DB).
  - The resolved row must have currency `USD` and `exchange` '7'. Otherwise the reason is `comparator_instrument_mismatch`; a missing row gives `comparator_instrument_missing`.
- **The return basis.**
  - The figure is the **market-price return of the listing in USD**: the ratio of `price_daily.close` at the window's end to its value at the window's start. `price_daily.close` is eToro's daily candle close, Bid-derived and not dividend-adjusted (`market-data` skill, "EXECUTION-VENUE view"). eToro does not document its cutoff time; the LSE session close is assumed.
  - It is not the fund's NAV performance.
  - An accumulating class pays no distributions (issuer, "Accumulating"), so the price ratio carries the reinvested income, net of the fund's costs and of the withholding tax the fund suffers.
  - **Splits** follow the existing documented rule. eToro back-adjusts candles for splits at fetch time, and the #2066 heal (`market_data.py:783-800`, `detect_adjustment_event`) re-fetches an instrument's full history when a split re-bases its overlap. The comparator relies on that upstream treatment and adds no heuristic of its own. Its limit is a residual assumption: the heal triggers only on an incremental fetch whose overlap ratio reaches `_ADJUSTMENT_RATIO_THRESHOLD` (1.2), so a split with a smaller ratio (e.g. 11:10) would leave a basis break. Dividends need no treatment, because an accumulating class pays none.
- **Diagnostics, not gates.**
  - Our close on 2026-10-09 is 841.59, against the issuer's NAV of 842.8552 that day, which is struck at the US close.
  - From 2024-10-31 to 2026-10-09, CSPX.L returned 18.498%/yr and SPY.RTH's price 17.680%/yr: 467 common dates, compounded over calendar years. The gap has the sign expected of reinvested income. We hold no SPY dividend history, so it is not an exact check.

### The window and its endpoints

- **The window is the pot's chain, gated on the TWR summary.** The comparison is computed only when `total_return_available` is true. The TWR summary describes the last complete point, and after a chain break it is unavailable (TWR spec, Rule 4), so an earlier comparison is never revived.
  - End E is that last complete point's date.
  - Start S is the `period_start` of the first point whose `cumulative_return` is non-null.
  - S is not `return_since`. When the chain starts from an unfunded base, the TWR reports `return_since` as the first funded valuation's date, but its first sub-period runs from the base date with the flow at its start (TWR spec, Rule 3). The comparator has to cover that sub-period too.
  - When `total_return_available` is false, the comparator return is NULL and S and E are NULL. The reason is `pot_return_unavailable` unless an instrument reason precedes it in the contract's order.
- **Endpoint resolution.** Each endpoint is the latest **valid** close dated on or before S or E, returned together with its `price_date`. A close is valid when it is finite and > 0, the `market-data` skill's rule. Invalid rows are skipped, and no row dated after E is read.
  - No valid close on or before the date → `comparator_close_missing`. That covers both a case with no rows and a case where every row is invalid.
- **Coverage.** Each resolved close must lie within 4 calendar days before its date. Otherwise → `comparator_stale`.
  - #1817's rule (`reporting.py::_benchmark_closes`) needs the end row's date to be ≥ the period start, so that some close falls inside the span. This guard is stricter and covers both ends: a window can still be stale at its start, or end weeks early, while passing #1817.
  - The 4 days is a **chosen tolerance**, not a calendar-derived rule. It is motivated by the longest LSE closure: from a Thursday close over Good Friday and Easter Monday, a date is at most 4 days after its latest close. The dev DB's longest gap between consecutive closes, 5 days, is consistent with that. A tolerance can also let a missing ordinary session through, which is the residual of choosing one.
  - A UK-only holiday on a US session resolves to the prior LSE close and stays valid.
- **Collapse.** S's row and E's row are the same row → `comparator_stale`. Distinct rows with equal closes are a genuine 0 return.
- **Alignment, a stated basis.** Pot points are dated by the US session (`portfolio_eod.resolve_snapshot_date`). The comparator's close dated the same day is assumed to be the LSE session close, earlier in the day by an amount that varies with daylight-saving and shortened sessions; eToro does not document the cutoff. Each endpoint can therefore differ by part of one session's move, or by up to 4 calendar days when it resolves to an earlier close. The UI states both.
- **Freshness.** `CSPX.L` joins `scheduler.BENCHMARK_SYMBOLS`, and a test binds the comparator constant to that set (#1818). Today the series is current through coverage tier only. It had a same-date close on 70 of 70 `portfolio_eod_snapshots` dates since 2025-10-31; that is an observation, not a guarantee.

### The hurdle (no source rule; fixed by construction)

- **Weights.** These come from the current enabled core mandate event and the current pool event. If either is missing, or the pool is `unconfigured` → `mandate_unconfigured`.
  - The active weight `w_active` = `active_risk_budget_pct` / 100. It is the cap on non-core committed capital as a share of `pool_base` (`strategy_paper_executor.py:1212-1214`), so it is a capital weight at full use, not a risk measure.
  - The core target `core_target_weight` = `core_target_pct` / 100 (`strategy_core_mandate`). It is **not** a share of the pot. The allocator measures it over `core_market_value + cash_balance` (`strategy_core_allocator.py`, `sleeve_value`), and that cash is the pot's sandbox headroom (`observe_core_sleeve`), which is net of everything committed, active included.
  - The core's pot weight `w_core` = `core_target_weight` × (1 − `w_active`): with the active sleeves at their cap, the core's sleeve is the remaining 1 − `w_active` of the pot.
  - Cash = 1 − `w_core` − `w_active` = (1 − `w_active`)(1 − `core_target_weight`).
  - *Corrected in the build (#3616, Codex checkpoint 2):* the merged draft set `w_core` = `core_target_pct` / 100, which mixes two denominators and gave 5/3 instead of 13/6 at the live 50/30.
  - The scenario must be allowed by the executor's cash reserve: total committed capital is capped at `pool_base` × (1 − `cash_reserve_pct` / 100) (`strategy_paper_executor.py:1202-1205`).
  - `core_target_weight` or `w_active` outside [0, 1], or `w_core` + `w_active` > 1 − `cash_reserve_pct` / 100 → `mandate_weights_invalid`. That is checked before `no_active_budget` (`w_active` = 0).
  - The live values are 35 + 30 = 65, within 90.
- **Scenario.** The scenario's starting capital equals `pool_base`, at the initial weights above. The executor's caps are fractions of `pool_base`, so the active weight is that cap at full use, and the core holds its target share of the rest. The pot is then bought and held over the window, with no leverage and no rebalancing. Over one buy-and-hold period, the pot's return is exactly the weighted sum of the sleeves' returns. Two assumptions are stated beside the figure:
  - the core earns the comparator's return;
  - cash earns 0. Uninvested principal adds no P&L to the pot NAV by construction (`strategy_wealth`).
- **Formulas.**
  - The pot matches the comparator when the active sleeves return `hurdle_return = r_cmp · (1 − w_core) / w_active`.
  - The multiplier is `(1 − w_core) / w_active`, which is 0.65 / 0.3 = 13/6 at the live 50/30.
  - `required_active_excess = hurdle_return − r_cmp`.
  - A `hurdle_return` below −1 cannot be matched exactly without leverage → `hurdle_infeasible`. Under these assumptions, even a total loss in the active sleeves would leave the pot ahead.
- **It is a counterfactual under the current mandate.** The response returns the weights and the mandate event ids used. A mandate edit changes the hurdle and nothing else.

## Contract change

`StrategyWealthHistoryResponse` gains one field, `comparator`. It is always present, never null. `benchmark_comparison_available` (False) and `benchmark_refusals` are unchanged: they report the S&P 500 TR index and CPIH, and the comparator is not that index. The UI labels it "UCITS comparator", not "benchmark". Its tooltip says that whether this account can hold the fund directly, rather than as a CFD, and in an ISA is not yet established.

```
comparator: {
  symbol: "CSPX.L", instrument_id: int | null,
  label: "iShares Core S&P 500 UCITS ETF (USD, Acc)",
  basis: "accumulating_ucits_market_price_return_usd",
  window_start: date | null, window_end: date | null,   # S and E
  start_close_date: date | null, end_close_date: date | null,
  return: Decimal | null,          # end_close / start_close − 1, cumulative fraction, unrounded
  unavailable_reason: "comparator_instrument_missing" | "comparator_instrument_mismatch"
      | "pot_return_unavailable" | "comparator_close_missing" | "comparator_stale"
      | null,                          # the first that applies, in this order
  engine_excess: Decimal | null,   # pot cumulative_return at E − return (fraction); NULL iff return is NULL
  hurdle: {
    core_target_weight: Decimal | null,     # core_target_pct / 100, a share of the core sleeve
    core_weight: Decimal | null,            # the core's share of the pot
    active_weight: Decimal | null,
    core_mandate_event_id: int | null, pool_event_id: int | null,
    multiplier: Decimal | null,              # available from the mandate alone
    hurdle_return: Decimal | null,           # NULL when return or multiplier is NULL, or infeasible
    required_active_excess: Decimal | null,
    unavailable_reason: "mandate_unconfigured" | "mandate_weights_invalid" | "no_active_budget"
      | "comparator_return_unavailable" | "hurdle_infeasible" | null,   # the first that applies, in this order
  },
}
```

**Null invariants.**
- `engine_excess` is NULL iff the comparator return is NULL. Hurdle infeasibility never affects it.
- `hurdle_return` and `required_active_excess` are NULL whenever any input to them is NULL, or the hurdle is infeasible.
- `multiplier` is NULL only under the first three hurdle reasons.
- Weights and event ids are populated whenever the mandate rows exist.
- `instrument_id` is populated whenever a row resolved, a mismatch included.
- The dates are populated as far as they resolved.

All returns are unrounded cumulative fractions, not annualised; rounding is UI-only. The `days` query parameter bounds the window exactly as it bounds the TWR.

**Worked example (dev DB, 2026-10-12):**
- S = 2026-09-17 and E = 2026-10-09; the pot's cumulative return is 0.010937. `return_since` is 2026-09-18: the chain starts from the unfunded 2026-09-17 base, and the first linked point's `period_start` is that base (Rule 3).
- CSPX.L moved from 822.86 to 841.59, a return of 0.022762.
- Engine excess: −0.011825.
- Multiplier 13/6, so the hurdle return is 0.049318 and the required active excess is 0.026556, both from the unrounded comparator return 0.0227620….
- *Corrected in the build (#3616):* the merged draft used S = 2026-09-18, which is `return_since`, contradicting the window rule above.

## UI

The comparison extends the existing Portfolio-tab return strip in `StrategyPortfolioLens`; there is no new panel, per the "simplify the existing page" rule.
- **Line 1:** "vs UCITS comparator iShares Core S&P 500 (USD, Acc), 18 Sep – 9 Oct: +2.27% · engine −1.17 pts". The dates are S and E. When S differs from the strip's existing "since" date (first funding), the tooltip says why. The tooltip also states the market-price basis, both resolved close dates, the assumed LSE cutoff and its timing against the US close, a resolution to an earlier close (up to 4 calendar days) when one applies, and the investability qualification above.
- **Line 2:** "Active sleeves would need +4.93% (+2.66 percentage points over the comparator) to match it". The hint is built from the returned weights: "at today's mandate: 30% active at its full cap, 35% core (its 50% target of the other 70%) assumed to track the comparator, 35% cash earning 0; held without rebalancing or leverage".
- Each field degrades on its own:
  - An unavailable hurdle replaces line 2 with its reason in plain words. `hurdle_infeasible` reads "Under these assumptions, even a total loss in the active sleeves would leave the pot ahead".
  - An unavailable comparator shows "—" with its reason, never a 0.
- The existing refusal notice for the S&P TR index stays.

## Tests

- **Pure (endpoints):**
  - a missing instrument, a mismatched currency and a mismatched exchange;
  - either or both endpoints missing, including histories where every row is invalid (0, negative, NaN, ±Infinity);
  - a stale start with a fresh end, and a fresh start with a stale end;
  - the 4-day boundary, at 4 days and at 5;
  - a UK holiday on a US session resolving to the prior close;
  - endpoints that collapse to one row;
  - distinct rows with equal closes giving a return of exactly 0;
  - an invalid close skipped to the prior valid one;
  - reason precedence, including a missing instrument while the pot return is unavailable.
- **Pure (window):**
  - S taken from the first linked `period_start` when the chain starts at an unfunded base, with `return_since` ≠ S;
  - a funded base;
  - trailing incomplete points (E is the last complete cumulative point);
  - a bridged no-flow gap;
  - a broken chain: `total_return_available` false gives `pot_return_unavailable`, and an earlier +10% is never revived;
  - no pot return → `pot_return_unavailable`;
  - changing `days` moves S, E and the comparator together.
- **Pure (hurdle):**
  - a multiplier of 13/6 at 50/30, with the core at 35% of the pot;
  - `no_active_budget`, `mandate_weights_invalid` (including a cash-reserve violation) and `mandate_unconfigured` (missing core mandate, missing pool, unconfigured pool), with the precedence and null invariants;
  - a mandate change moving only the hurdle outputs;
  - a zero comparator return and a negative one;
  - `hurdle_infeasible` below −1, with `engine_excess` still present; exactly −1 (comparator −62.5% at core 40 / active 50, multiplier 1.6) stays feasible;
  - a multiplier still present while the comparator return is NULL.
- **Binding:** `COMPARATOR_SYMBOL ∈ BENCHMARK_SYMBOLS`.
- **DB:** one test of the new SQL on a sparse fixture: the latest valid close on or before a date, returned with its `price_date`, and no future rows read.
- **API:** the field is always present; units are fractions; both close dates and the hurdle fields are populated; the existing refusals still appear beside an available comparator.
- **FE unit:** formatting in percentage points; "—" instead of 0; an unavailable hurdle removes the number but shows its reason while line 1 stays; the assumption text is built from the returned weights.
- **Dev stack:** the endpoint reproduces the worked example, and the strip is eyeballed in light and dark.

## Evidence queries

These are point-in-time observations (dev DB, 2026-10-12), not guarantees.

```sql
-- the instrument row the mapping rests on
SELECT instrument_id, symbol, company_name, currency, exchange FROM instruments WHERE symbol = 'CSPX.L';
-- issuer cross-check close and the worked example's endpoints
SELECT price_date, close FROM price_daily p JOIN instruments i USING (instrument_id)
WHERE i.symbol = 'CSPX.L' AND price_date IN ('2026-09-18', '2026-10-09');
-- longest gap between consecutive closes
SELECT max(gap) FROM (SELECT price_date - lag(price_date) OVER (ORDER BY price_date) gap
  FROM price_daily p JOIN instruments i USING (instrument_id) WHERE i.symbol = 'CSPX.L') x;
-- same-date coverage of pot snapshots since 2025-10-31 (70 / 70)
SELECT count(*), count(p.price_date) FROM portfolio_eod_snapshots s
LEFT JOIN price_daily p ON p.price_date = s.snapshot_date
  AND p.instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'CSPX.L')
WHERE s.snapshot_date >= '2025-10-31';
-- the mandate weights used (core event 35, pool event 5 on 2026-10-12)
SELECT core_mandate_event_id, core_target_pct FROM strategy_core_mandate_events ORDER BY 1 DESC LIMIT 1;
SELECT strategy_paper_pool_event_id, active_risk_budget_pct, cash_reserve_pct FROM strategy_paper_pool_events ORDER BY 1 DESC LIMIT 1;
```

The pot side of the worked example is `time_weighted_returns(load_strategy_wealth_history(conn, days=365))` from `app/services/strategy_wealth.py`. The CSPX.L/SPY.RTH diagnostic compounds `close` over calendar years between the first and last common dates.

## Security

A read-only addition to an authenticated read endpoint. No authorisation, order or capital path changes.
