---
name: market-data
description: eBull price/quote ingestion — the eToro-fed candle + quote refresh path (provider interface, price_daily/quotes tables, refresh_market_data service, daily_candle_refresh/fx_rates_refresh jobs) and the native-price-primary read contract.
---

# market-data

## When to use

Any change to `app/services/market_data.py`, the `price_daily` / `quotes`
tables, the `MarketDataProvider` interface (`app/providers/market_data.py`)
or its eToro implementation (`app/providers/implementations/etoro.py`), the
`daily_candle_refresh` / `fx_rates_refresh` jobs, or the read paths that serve
prices: `/instruments/{symbol}/candles`, `/intraday-candles`, `/risk-metrics`,
`/sse/quotes`, and the day-change on the instrument list/summary. Also read it
before touching how a mark or displayed price is derived from a quote.

## What it is

**Provider interface** — `MarketDataProvider` (ABC, `app/providers/market_data.py`):
`get_daily_candles`, `get_intraday_candles`, `get_quote`/`get_quotes`,
`get_tradable_instruments`, lookup catalogues. Frozen dataclasses `OHLCVBar`,
`IntradayBar`, `Quote` (`Quote.conversion_rate` = instrument→account FX mid).
v1 impl is `EtoroMarketDataProvider`; domain code imports the interface only.
Providers are thin adapters (settled: "Provider strategy → Provider design
rule") and persist raw responses before returning.

**Refresh service** — `refresh_market_data(provider, conn, instruments, ...)`
(`app/services/market_data.py`). Per instrument: fetch candles →
`_upsert_candles` into `price_daily` (idempotent `ON CONFLICT (instrument_id,
price_date) DO UPDATE … WHERE DISTINCT`) → `_compute_and_store_features`
(rolling returns `return_1w…1y`, `volatility_30d`, and the TA suite via
`technical_analysis.compute_indicators`). Then batch `get_quotes` →
`_upsert_quote` into `quotes` with `spread_pct` + `spread_flag`
(`DEFAULT_MAX_SPREAD_PCT = Decimal("1.0")`, 1%). Two-mode fetch (#271/#603):
1000-bar backfill for new/gapped instruments, else `_INCREMENTAL_FETCH_BARS = 3`.
Batch circuit-breaker (#1833): `_CANDLE_BATCH_ABORT_LIMIT = 10` consecutive
*systemic* failures raise `UpstreamUnreachableError` instead of grinding through
per-instrument 30s timeouts. `compute_day_change` / `load_day_changes` (#1924)
derive close-to-close change from the two most-recent strictly-positive closes.

**Tables** — `price_daily` (sql/001; PK `(instrument_id, price_date)`; OHLCV +
`volume`, return/volatility columns sql/002, TA columns sql/025). `quotes`
(sql/002; PK `instrument_id`, overwritten each refresh; `bid`/`ask`/`last`/
`spread_pct`/`spread_flag`; CHECK `quotes_last_positive` — `last IS NULL OR
last > 0`, sql/181). Price-derived risk stats live in
`instrument_risk_metrics_observations`/`_current` (sql/198, `risk_v1`).

**Jobs** — `daily_candle_refresh` (`app/workers/scheduler.py`, `etoro` lane)
calls `refresh_market_data(..., skip_quotes=True)`; **`quotes_refresh` (hourly
@ :23, `etoro` lane) owns quote freshness**, so the daily EOD sweep never
shadows fresher quotes and never stamps instruments with marks taken 70
minutes apart. `fx_rates_refresh` (`db` lane, daily 17:00 CET) owns ECB FX
rates ONLY — it gave up its batch-quote phase in #502 and owned nothing about
quotes for the ~3 months until #2271. All registered in
`app/jobs/runtime.py`; bootstrap stage `candle_refresh`
(`bootstrap_orchestrator.py`).

**Read paths** — `/instruments/{symbol}/candles` reads `price_daily` only, no
provider fallback (empty state, not 404). `/intraday-candles` fetches live from
the provider through `IntradayCandleCache` and is NEVER persisted. `/sse/quotes`
streams live ticks via `QuoteBus`; `/risk-metrics` serves the `risk_v1` layer.

## Invariants

- **eToro is the source of truth for quotes and candles in v1** (settled:
  "Provider strategy → Market / execution source of truth"). Providers stay
  thin: no DB lookups, no domain orchestration (settled: "Provider design rule").
- **Native price is primary (#1906, operator decision 2026-07-04, reverses
  #1845).** `current`/`currency` stay native — the tradable number never flips
  by which path (REST snapshot vs SSE tick) answers first. The FX-converted
  companion is `display_current`/`display_currency` (secondary, muted) and stays
  `None` when no FX rate exists or native == display — never a converted number
  under a wrong label. Enforced in `get_instrument_summary` and
  `sse_quotes._format_tick`.
- **`quotes.last` is a real trade price or NULL, never ≤0** (#1428/#1429). eToro
  emits `lastExecution=0` for un-freshly-traded instruments; a 0 mark reads as
  fake −100% P&L. Writers coerce at write time; the CHECK is the hard backstop;
  read-side derives a mark from bid/ask when NULL.
- **As-of is data-anchored, not wall-clock.** Day-change is stamped to the latest
  close's `price_date` (settled: "Own EOD NAV-snapshot table" — `MAX(price_daily.
  price_date)`, latest closed session), so a stale close reads honestly.
- `price_daily.volume` NULL means "not provided or zero" (#21). Candle upsert is
  idempotent; features recompute only when the newest complete OHLCV bar matches
  the row being written.
- **`price_daily` is the EXECUTION-VENUE view, not the research corpus (#2240).**
  It is eToro-sourced, Bid-derived, unadjusted (price not total return), and caps
  at ~4 years per instrument (1,000-bar API ceiling, #603). That is correct and
  wanted for marks, fills and the tradable universe. It is **not** the basis for
  backtesting: the universe holds only currently-listed instruments (5 of 12,691
  non-tradable have any history), so any replay of it is survivorship-biased.
  Historical strategy research runs on a separate public research corpus
  (#2282); the two are interchangeable for signal *shape* — measured return
  correlation 0.963–0.996, SMA-200 regime agreement 99.4–100% — with a consistent
  −0.15% level bias that is the Bid half-spread. ⚠ Never conclude from
  `price_daily` that a given history depth or instrument is unavailable; that is
  a property of our provider, not the market (prevention log: "A correct
  measurement of the WRONG POPULATION…").
- **`quotes` has exactly two writers and they share one row per instrument
  (#2271).** The WS subscriber (`etoro_websocket.upsert_quote`) is
  *visibility-driven* — #498, "Boots quiet … no Subscribe frame until an SSE
  stream lands" — so it writes **only** for instruments the operator has on
  screen. Everything headless is covered by the `quotes_refresh` job (hourly
  @ :23, held + benchmarks + T1/T2, `market_data.refresh_quotes`). Both
  upserts MUST stay monotonic in `quoted_at`
  (`WHERE quotes.quoted_at IS NULL OR EXCLUDED.quoted_at >= quotes.quoted_at`)
  or the hourly REST snapshot clobbers a live tick while the page is open.
  Do not "simplify" either guard away, and do not add a third writer without
  it. History: #502 dropped `fx_rates_refresh`'s batch-quote phase as
  "redundant for visibility-driven workflows", which left the table with no
  scheduled writer for ~3 months while scoring, portfolio, execution_guard,
  position_monitor, valuation, transaction_cost and coverage all read it
  headless. Measured 2026-08-04 before the fix: 107 rows for a ~12k universe,
  held-position marks 14-22h old.
- **No reader of `quotes` bounds `quoted_at`.** The settled "AUM basis" rule
  covers a *missing* quote ("fall back to cost basis"); a stale row is
  indistinguishable from a fresh one and never triggers it. If you need
  freshness, assert it at the read site — the table will not do it for you.
- **`refresh_market_data` REQUIRES an autocommit connection (#2269).** Its
  per-instrument `with conn.transaction()` is only a real commit boundary when no
  transaction is already open. Callers read first (`daily_candle_refresh` runs
  four scope SELECTs; the loop itself calls `_candles_are_fresh` every
  iteration), so under `autocommit=False` psycopg3 turns every block into a
  `SAVEPOINT` and the whole ~12k-instrument sweep commits once, at connection
  close. Four consecutive sweeps were reaped by a routine jobs-daemon restart and
  lost 100% of their work with every write reporting success. A pre-loop
  `conn.commit()` does NOT fix it — the next iteration's read re-opens the
  implicit tx. Both call sites (`app/workers/scheduler.py::daily_candle_refresh`,
  `scripts/rebackfill_candles_5y.py`) pass autocommit; the service warns if a
  caller does not. Durability here is only observable from a SECOND session —
  poll the target table, or watch `now() - xact_start` in `pg_stat_activity`
  (seconds = correct, monotonically growing = the bug).
- **v1 is long-only / no-leverage / deterministic-execution** — price/quote data
  feeds scoring + the mark-to-market marks the execution guard re-checks; every
  refresh must be reproducible and every displayed price auditable to its source.

## Risk-metrics conventions (#591, `app/services/risk_metrics.py`, `risk_v1`)

Price-derived risk stats are computed from the `price_daily.close` chain (instrument + SPY benchmark). Pinned conventions — keep identical across any future risk math:

- **Returns: SIMPLE** (`close[i]/close[i-1] - 1`) between consecutive *surviving* rows. A close is valid iff finite AND `> 0`; an invalid row **breaks the chain** (no gap-spanning synthetic return); the return is keyed to the later close's date.
- **Volatility: annualized realized** = sample std (n−1) of daily returns × `sqrt(TRADING_DAYS)`, `TRADING_DAYS = 252` (a fixed constant — never derived from observed row count). This realized vol is **NOT** the scorer's TA "volatility-regime" term (Bollinger/ATR) — distinct figure, distinct purpose.
- **CAGR: calendar-time** `(final/first)^(365/calendar_days) - 1`, NOT `(252/n_returns)` (the trading-day form silently inflates the figure when the survivor chain has gaps). Calmar's numerator uses the SAME annualizer.
- **Calmar** = `annualized_return / abs(max_drawdown)` (Calmar, NOT Sharpe — eBull has no risk-free-rate series, so an honest Sharpe is impossible; never label it Sharpe). `abs(max_dd)` near 0 → null.
- **Beta vs SPY: OLS on DATE-ALIGNED returns** — pair only dates where BOTH series have a return (intersection of date keys; NEVER positional-zip — holiday gaps differ between an instrument and SPY and a positional zip silently mis-pairs them). `beta = cov/var_m`, `r2 = corr²`; `var_m=0` → beta null; `var_i=0 or var_m=0` → r2 null. No `alpha` in v1.
- **var_5 (empirical VaR)** = type-7 linear-interpolation 5th percentile of daily returns, **signed** (a loss is negative — stored as-is, never abs'd). skew/excess-kurtosis use biased ÷n central moments (Fisher: `kurtosis − 3`); flagged low-sample below ~250 obs.
- **Float island:** skew / excess-kurtosis / percentile computed in float (numpy) then quantized to Decimal at the persistence boundary; everything else (returns/vol/drawdown/beta/CAGR/Calmar) end-to-end Decimal.
- **Windows** 1y/3y/full: standalone metrics use the instrument's valid history *within the window*; benchmark metrics (beta, excess) use the aligned-overlap window. Min-obs: vol/beta need ≥60 returns; annualized figures flag `partial_window` below ~252 returns.
- v1 is **price return, not total return** (dividend-adjusted TR is a filed follow-up). Persisted in the two-layer `instrument_risk_metrics_*` tables; served by `GET /instruments/{symbol}/risk-metrics`.

## Failure conditions

- Missing critical source data, stale timestamps beyond threshold, or
  contradictory evidence surface as **explicit signals** — never a neutral
  default. A non-positive `last`/`close` is a sentinel, not a price (→ NULL /
  omit, "—" on the surface); a stale close is stamped with its real `as_of`;
  when no FX rate exists the native price shows alone, not a mislabelled convert.
- **eToro market-data is UNREACHABLE from the autonomy-loop environment.** A
  steady-state flat SPX500 / empty live quotes there is the environment, NOT a
  code bug — do not re-investigate. Under a real outage the candle circuit-breaker
  (#1833) aborts the batch with a terminal `UpstreamUnreachableError` rather than
  grinding; quote reads degrade to "—", never to a fabricated mark.
