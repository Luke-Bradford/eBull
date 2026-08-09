# Evidence-backed signal engine — validated inputs and free-source boundary

Date: 2026-08-09
Status: Research correction and implementation boundary
Parent: #2437

Companion implementation/control-plane contract:
`2026-08-09-strategy-automation-control-plane.md`. It owns promotion, allocation,
manual-position isolation, monitoring/P&L, bounded signal retention and the
paper-to-live slice order; this document owns evidence, inputs and observation
storage.

## Decision

Build a **cost-aware probability-of-path engine** for daily-to-multi-day setups,
with intraday data used for causal confirmation and execution. Do not build an
equal-vote indicator checklist and do not describe it as market making.

A candidate becomes orderable only when all four independent gates pass:

```text
causal signal + positive net expectancy + executable quote + portfolio capacity
```

The output is not `BUY` or `SELL`. It is an auditable record containing the
feature snapshot, target/stop/timeout geometry, calibrated probabilities,
estimated costs, size, rejection reasons, and strategy/data versions.

## Relevance window and market-regime contract

The full 1962-2026 research corpus is an artefact/stress resource, not the
headline benchmark for a strategy intended to trade after 2026. Decimalisation,
electronic execution, Reg NMS, retail commission compression, broker product
rules, participation and settlement have changed the mechanism that generates
short-horizon returns and costs.

Use fixed, disclosed views rather than selecting the cutoff that makes a rule win:

```text
primary relevance window   2022-01-01 onward
recent sensitivity         rolling latest 24 and 36 months
year/regime stability      each calendar year and declared structural regime
older history              falsification and tail stress only
forward evidence           immutable observation after rule freeze
```

The 2022 boundary is a versioned project construction, not a published natural
law. It starts after the exceptional 2020-2021 zero-rate/meme stimulus interval
and supplies more than a single year of observations. It must not be moved after
results are opened. Report 2020+ beside it as a pandemic-era sensitivity arm,
never pool pre-2000 observations into the primary statistic, and never claim that
any historical window substitutes for post-freeze forward evidence.

Promotion requires the sign and economic conclusion to survive the primary
window and both recent sensitivities. Older data may kill a mechanism through a
known artefact or tail, but may not rescue a strategy that fails recently.

## Correction to the #2437 evidence

`scripts/verify_2437_confluence.py` previously grouped returns by the next-open
entry date and also used that date for market breadth and the 21-day market leg.
That leaked the entry session's market return into a signal stated to be known at
the prior close. The reported `breadth up day` marginal and every alignment-count
result containing conditions 5 or 11 are invalid until recomputed.

The script now separates `signal_date` from `entry_date`, fails closed when the
signal-date market context is absent, and has a regression test. No prior
confluence figure may be carried into a strategy specification.

Full-population rerun, 2,353,114 stock-days:

```text
10-day unconditional baseline       44.28 bps
10-day breadth-up condition          37.70 bps
alignment-count vs-base sequence    -35, -28, -39, -17, -10, 0,
                                     +5, +2, -6, +14, -26, +15 bps
```

The alignment sequence is not monotonic and its largest counts are sparse. Equal
counting supplies no validated confluence signal. Reproduce with:
`PYTHONPATH=. uv run python scripts/verify_2437_confluence.py`.

The separate short-continuation result does not use this confluence code and is
not invalidated by this defect. It remains exploratory because its five-bar
cohorts overlap, its ~100 searches are undeclared, and it has no executable
portfolio or forward sample.

## Published evidence: what is justified to test

These papers support **bounded hypotheses**, not implementation constants.

| hypothesis family | evidence-supported statement | eBull test boundary |
| --- | --- | --- |
| intraday market momentum | Gao, Han, Li and Zhou find that the US market/ETF first half-hour return predicts the last half-hour return, with stronger predictability on high-volatility and high-volume days | Test on very liquid ETFs first. Do not generalise the published ETF result to individual stocks without a separate arm. |
| short-term reversal / liquidity provision | Nagel finds reversal-strategy returns vary with VIX and rise in market turmoil, consistent with constrained liquidity provision | Test volatility regime as an interaction. It does not establish that buying every loser is profitable after eToro costs. |
| market dependence | Replication evidence outside the US is mixed; an Australian study reproduces the US method but finds no local effect | Exchange/region is part of strategy identity. Pooling markets is prohibited. |
| stops | Published stop-loss evidence is conditional on serial dependence and strategy family | Stops are risk-distribution parameters and require their own path backtest; they are never assumed to add edge. |

Primary pages:

- Gao et al., *Journal of Financial Economics*, 2018:
  https://doi.org/10.1016/j.jfineco.2018.05.009
- Nagel, *Review of Financial Studies*, 2012:
  https://doi.org/10.1093/rfs/hhs066
- Ho, Lv and Schultz, *Pacific-Basin Finance Journal*, 2021:
  https://doi.org/10.1016/j.pacfin.2021.101499

## The mathematical contract

For instrument `i` at completed signal bar `t`, construct only point-in-time
features:

```text
X(i,t) = price shock, trend, structure, volatility, volume,
         market/sector context, liquidity, catalyst, session state
```

Minimum price-state definitions:

```text
log_return       = ln(adj_close[t] / adj_close[t-1])
residual_return  = instrument_return - beta_market*market_return
                   - beta_sector*sector_return
shock_z          = residual_return / trailing_realised_volatility
close_location   = (2*close - high - low) / (high - low)
level_distance   = (price - causal_level) / ATR14
```

`beta_market` and `beta_sector` are trailing estimates ending at `t`; missing
benchmarks make the residual feature unavailable rather than zero. The first
implementation must also retain the raw return so the residual construction can
be falsified against it.

The label is a triple barrier using levels fixed from the signal snapshot:

```text
Y = target_first | stop_first | timeout
```

The decision model estimates calibrated `p_target`, `p_stop`, and `p_timeout`.
The execution gate consumes:

```text
EV_net = p_target * target_payoff
       - p_stop * stop_loss
       + p_timeout * expected_timeout_payoff
       - commission - spread - slippage - carry - borrow - FX
```

Promotion requires a positive **lower confidence bound** on `EV_net`, not a
positive point estimate or win rate. The first model must be an auditable
logistic/GAM-style model or a small fixed score. A tree model is a later trial;
an unrestricted subset search or neural network is out of scope for this corpus.

## Structure and ratcheting

The #2437 placebo result establishes only that the tested support proximity and
Fibonacci zones did not demonstrate unique unconditional directional value. It
does not falsify causal structure as a path or risk variable.

Allowed level sources in the first test:

- confirmed pivot high/low, exposed only after its right-hand confirmation bars;
- prior session/week/month high or low;
- trailing Donchian high or low;
- gap boundary known at the signal time.

Every level is stored with `level_type`, `level_price`, `known_at`, and rule-set
version. A break is defined in ATR units and must use a completed bar:

```text
long_break  = close > resistance + k*ATR14
short_break = close < support    - k*ATR14
```

`k` is an unvalidated strategy parameter and must be registered before it is
measured. A ratchet is tested as a separate strategy identity. It moves only in
the protective direction and cannot be used to claim that the entry has edge.

## Free and official source boundary

### Available and worth integrating

| source | verified capability | use | limitation |
| --- | --- | --- | --- |
| eToro REST + WebSocket | intraday OHLCV history plus live bid/ask/last already available in this repo | entry timing, realised volatility, observed live spread | REST has a 1,000-bar/no-anchor ceiling; stream has no depth or quote sizes |
| eToro trading preflight | current v2 demo/real eligibility and what-if schemas are documented and thin adapters are implemented | current orderability, direction/settlement constraints, limits and broker-estimated ticket costs | documentation is not population evidence; actual demo coverage and point-in-time short availability still require a bounded probe |
| SEC EDGAR | unauthenticated submissions and XBRL APIs; submissions typically update in under one second and XBRL in under one minute | causal filing/catalyst timestamp and structured fundamentals | filings are not a general news feed and many material events are unstructured |
| Nasdaq Trader halt RSS | free, US exchange-wide halt/pause feed updated once per minute, with date queries | fail-closed eligibility, halt attribution and gap-risk analysis | current/dated halt events, not order-book history |
| FINRA public API | consolidated short-interest datasets and the already-ingested short-interest source | slow-moving crowding/constraint context | short interest is not live borrow availability, fee, utilisation, or intraday short volume |
| FRED/ALFRED | official macro time series through a free API key | macro regime and point-in-time vintage studies | not an execution feed; licensing/attribution is series-specific |

Official documentation:

- SEC EDGAR APIs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
- Nasdaq halt RSS: https://www.nasdaqtrader.com/Trader.aspx?id=TradeHaltRSS
- FINRA Developer Center: https://developer.finra.org/docs
- FRED API: https://fred.stlouisfed.org/docs/api/fred/overview.html

### Not freely obtainable at the required quality

- consolidated historical bid/ask and trade tape for the full universe;
- point-in-time historical stock-loan availability and borrow fee;
- full L1 sizes, depth, queue position, aggressor side, and order-book events;
- historical broker-specific order acceptance, slippage, margin and forced-close behaviour;
- broad, reliably timestamped machine-readable news with redistribution rights;
- historical options surfaces across the full equity universe.

These fields must not be approximated with today's values or silently filled.
Before calling shortability or costs unavailable, probe eToro's current
eligibility and what-if endpoints. Any dimensions those endpoints omit remain
forward-observation gaps. Consequently eBull can
build a directional setup engine, but cannot honestly claim to implement
institutional market making or order-flow strategies.

## eToro capability audit — what actually needs changing

### Already present; reuse it

- Daily and intraday OHLCV, including 1/5/10/15/30-minute, 1-hour and 4-hour.
- Live bid/ask/last WebSocket state and current spread computation.
- Tradable-universe sync, exchange/type/industry catalogues and currency mapping.
- Portfolio sync, order placement/polling, private-channel reconciliation,
  fixed/trailing SL/TP API capability, execution guard and kill switch.
- Outcome ledger/resolver, strategy ledger/registry, walk-forward/embargo,
  block bootstrap, Deflated Sharpe and matched random-entry controls.

### eToro supplies it; eBull currently loses or does not expose it

1. The instrument/search payload documents `isOpen`, `isCurrentlyTradable`,
   `isBuyEnabled`, `isDelisted`, `currentRate` and `dailyPriceChange`. The
   provider normaliser currently discards them and sets `is_tradable=True` for
   every returned non-internal record. Persist observed eligibility fields with
   provider/receive timestamps; do not overload slower universe membership with
   intraday orderability.
2. Demo/real **instrument trading eligibility** has a thin non-persisting adapter.
   The documented response exposes direction/settlement leverage arms, minimum
   exposure, maximum units, open/close/partial-close and SL/TP constraints. Run
   the bounded authenticated demo population probe before choosing persistence
   columns; absence of a `SHORT` arm is a refusal, not a long default.
3. Demo/real **what-if trading cost breakdown** has a thin non-persisting adapter.
   The cost-name vocabulary remains provider-owned and open; the execution gate
   must sum every returned cost and refuse unknown currency rather than select a
   favourable subset. The first demo probe returned `value` while omitting the
   documented `amount`; its unit/scale is unproved, and one successful response
   carried a `lastUpdated` more than five months old. Preserve both fields and
   timestamp, and refuse execution until units, completeness and freshness are
   proved. Probe `buy` and `sellShort` across a broader liquid/illiquid cohort
   before treating it as borrow/carry coverage.
4. REST intraday history is passed through an in-memory chart cache and not
   harvested into a research store. A bounded, rate-budgeted harvester is needed,
   not a new data vendor.
5. WebSocket quote history is collapsed into the latest-only `quotes` row.
   Persist merged observations/bars plus subscription coverage; raw sparse frames
   are not the research series.
6. Trade history and detailed order/position reads exist but are not the primary
   post-trade calibration ledger. Reuse them to reconcile simulated and broker
   fills/costs.

### Still absent after the eToro audit

- depth/size/order-flow fields: eToro documents rate and private topics only;
- historical point-in-time eligibility/borrow/cost responses before recording;
- a consolidated trade print: measured `LastExecution` behaves as bid-side state;
- arbitrary-depth intraday history beyond the 1,000-bar endpoint ceiling.

## Storage budget and retention contract

The measured eToro stream must **not** be stored row-for-message. In the
universe capture it produced about 1,140 price-bearing updates/second during the
US session, with peaks above 6,000/second. At the mean, 6.5 hours x 252 sessions
would create about **6.7 billion rows/year**. This is rejected before schema
design regardless of compression.

Measured on the dev database 2026-08-09:

```text
database                         67,166,131,891 bytes
price_daily       6,723,014 rows    827,564,032 bytes  (~123 bytes/row incl indexes)
research_daily   25,920,971 rows  3,715,080,192 bytes  (~143 bytes/row incl indexes)
candidate quote tuple                                  66 bytes before heap/index overhead
candidate OHLCV tuple                                  80 bytes before heap/index overhead
```

Reproduce the relation figures with `pg_total_relation_size()` and `count(*)`
over `price_daily` / `research_price_daily`; tuple figures use
`pg_column_size(ROW(...))`. These are sizing anchors, not promises: the migration
must remeasure its actual table and indexes after a representative load.

### Required tiers

| tier | cohort and resolution | retention | upper-bound purpose |
| --- | --- | --- | --- |
| current quote | all instruments, one row each | latest only | existing operational read path |
| 30-minute context | at most 1,000 prequalified instruments | 24 months | broad intraday regime/context |
| 5-minute setup | at most 250 actively ranked instruments | 12 months | formation/confirmation research |
| 1-minute execution | at most 50 order-near instruments | rolling 30 days | spread, trigger and fill diagnosis |
| feature snapshot | candidate evaluations only | durable | auditable decision input, not a price replica |
| eligibility/cost | candidate preflights plus state changes | 24 months | broker constraint/cost calibration |
| raw WS payload | sampled diagnostics only | <=24 hours | parser/transport diagnosis, never research |

At the measured ~143-byte daily-table reference, the price-bar caps imply
approximately:

```text
30m: 1,000 * 13 * 252 = 3.28m rows/year  ~= 0.47 GB/year
 5m:   250 * 78 * 252 = 4.91m rows/year  ~= 0.70 GB/year
 1m:    50 * 390 * 30 = 0.59m retained   ~= 0.08 GB retained
```

Target steady-state growth for the complete observation subsystem is therefore
**<=1.5 GB/year**, including indexes and eligibility/feature rows. This is a
hard acceptance budget, not an estimate to hand-wave past. The existing daily
`pg_database_size` sampler and 7-day growth signal monitor the deployed effect.

This budget does not excuse the existing signal-detail ledger. Measured
2026-08-09, one successful scan wrote 34,698 rows and the 34,698-row relation
occupied 32 MB, of which 24 MB was indexes. Retaining the same detail/index
shape every session projects to 8.74 million rows and about 8.42 GB/year. Its
aggregate-and-partition retention fix is therefore required independently of
the bounded intraday tiers.

### Physical and query rules

- Aggregate merged WebSocket state in memory; write bars on close, not ticks.
- Store no derived indicator history. Compute indicators from bars and persist
  only the feature snapshot used for a candidate decision.
- Monthly range partitions for retained intraday observations; retention drops
  whole partitions rather than running large row deletes.
- Primary access is `(instrument_id, observed_at)`; add no index without an
  `EXPLAIN (ANALYZE, BUFFERS)` for an actual consumer query.
- Eligibility/cost rows are event/state-change driven. An unchanged heartbeat
  does not create a row.
- Coverage intervals are coalesced (`subscribed_from`, `subscribed_to`), not one
  coverage row per tick or minute.
- The writer has explicit per-tier active-instrument caps and backpressure. It
  degrades by refusing new low-priority subscriptions, never by growing an
  unbounded queue.
- Every job reports rows/bytes written and retention deletions. Post-deploy
  acceptance compares actual 7-day DB growth with the declared budget.

The existing empty `price_intraday` table is not automatically the target
schema: it assumes one-minute trade-style OHLCV and has neither quote semantics,
resolution identity, coverage nor partitioning. Because it contains zero rows,
the implementation may replace it cleanly after the endpoint/cost spike fixes
the required contract.

## Ticket/slice order

The work must land as independent, reviewable slices. Later slices are blocked
until the previous measurement is recorded:

1. **Capability spike — complete, cost use blocked:** typed demo eligibility +
   what-if-cost adapters are implemented and unit-tested. The versioned
   `etoro-preflight-v2` census selected four stocks and four ETFs at deterministic
   liquidity quantiles. All eight resolved; one locally tradable name was
   refused. Twenty bounded 1x/10x long-real and x1 short-CFD requests sampled
   seven permitted names: all returned USD-labelled `value`, none documented
   `amount`; 18/20 timestamps were about 41 hours old and 2/20 current. Scaling
   was not one equation across components. Consequently 0/20 responses are
   execution-usable. No migration or recurring writer is justified by this
   endpoint. Reproduce with `PYTHONPATH=. uv run python
   scripts/verify_2437_trading_preflight.py --apply --limit 8
   --max-cost-requests 20` in demo.
2. **Storage benchmark:** representative temporary-table load, actual bytes/row,
   insert throughput, index/query plan and retention-partition drop timing.
3. **Eligibility observations:** state-change-only writer and latest view.
4. **Bounded bar recorder:** 30m first; prove growth/queries before enabling 5m,
   then 1m. Each tier has a separate enable flag and cap.
5. **Feature snapshots/triple barriers:** consume the proven stores; no duplicate
   price history.
6. **Historical validation:** recent-regime contract, portfolio accounting and
   controls.
7. **Forward observation:** still no broker orders.
8. **Paper execution:** separate operator promotion after untouched evidence.

The parent #2437 documents the research thread. The ordered implementation
tickets now exist: #2447 evidence, #2448 bounded storage, #2454 governance and
ownership, #2451 crash-safe reconciliation, #2449 paper allocation/execution,
#2452 position management, #2453 monitoring/P&L and #2450 live gate.

### Validated paper-execution contract (#2449)

- Automated entry has one deliberately separate writer: eToro v2
  `POST /api/v2/trading/execution/demo/orders`. It refuses non-demo credentials,
  permits long `real` settlement at x1 only, requires fixed SL and TP, and sends
  the immutable pre-I/O UUID as `X-Request-Id`. The live flag cannot select a
  different endpoint.
- Available cash, total invested and equity come from the demo P&L endpoint and
  the formulas in eToro's current guides. `clientPortfolio.credit` alone is not
  available cash because pending orders must be deducted. Every position and
  pending order, manual or automated, consumes portfolio/instrument capacity;
  only exact strategy order/position provenance grants mutation authority.
- The current cost docs return USD `amount`, while the measured demo sample
  returned undocumented `value`. The executor accepts only documented `amount`,
  rejects `value`, stale/missing/negative/non-USD components and any positive
  recurring fee whose holding horizon is unmodelled. Net expectancy is the
  minimum pinned bootstrap expectancy CI less stressed current cost divided by
  the exact ticket amount.
- The free Nasdaq Trader RSS feed is the primary halt source. Its documented
  once-per-minute refresh guidance, provider `(symbol, halt_at)` identity,
  source publication time, halt code and trade-resumption time are retained.
  Missing/stale/malformed feed state or an unresolved halt refuses entry.
- Storage stays bounded: one policy current row plus revision events; one
  preflight/shadow row per durable fired signal; one rolling account high-water
  row; one halt-feed state row; and halt events retained for 90 days. Raw P&L,
  eligibility, cost, order and RSS payloads stay process-local.
- Every gate failure becomes the fired signal's sole unfunded shadow decision.
  Passing all gates commits funding, trade, order link and request UUID before
  broker I/O. A transport-uncertain submission is never retried under a new UUID;
  it enters the existing reconciliation backlog and blocks later entries if it
  breaches the configured SLO.

Primary contracts verified 2026-08-09: [eToro create order](https://api-portal.etoro.com/api-reference/trading--demo/create-an-order),
[eToro what-if costs](https://api-portal.etoro.com/api-reference/trading--demo/get-what-if-trading-cost-breakdown),
[eToro equity formula](https://api-portal.etoro.com/guides/calculate-equity), and
[Nasdaq Trader halt RSS](https://www.nasdaqtrader.com/Trader.aspx?id=TradeHaltRSS).

## Features to add before paper trading

Implementation update (2026-08-09): the #2437 child series now implements the
bounded observation store, causal feature/signal evidence, recent validation,
promotion/ownership, reconciliation, demo paper execution, exact-owned position
management, strategy P&L/monitoring and the fail-closed live gate. The numbered
list below is the original dependency analysis, retained for provenance; it is
not a current backlog. Live activation remains blocked by the measured eToro
cost contract and by zero promotable current strategy versions. See
`2026-08-09-strategy-automation-control-plane.md` and
`2026-08-09-strategy-live-promotion-runbook.md` for current operation.

1. **Eligibility/cost endpoint spike — measured:** endpoint schemas and thin
   calls retain raw payloads in memory. The deterministic bounded demo census
   records field presence, direction/settlement arms, cost vocabulary, canonical
   parsed-response bytes, exact scaling relationships and timestamp age. It
   proves current cost results are not safe to persist as normalised monetary
   costs or consume in execution; eligibility may later be stored as compact
   state changes, but recurring cost polling remains unjustified.
2. **Instrument eligibility observations** — preserve eToro's current open,
   tradable, buy-enabled and delisted fields with `observed_at`; add SELL-specific
   state only if the eligibility probe proves it exists.
3. **Quote observation ledger** — append-only bid/ask/last, provider timestamp,
   receive timestamp, instrument, session state and subscription coverage.
4. **Execution-observation ledger** — candidate, what-if cost estimate, short
   availability, requested order, broker response, fill, rejection and close.
5. **Trading-halt ingest** — Nasdaq RSS with provider-native event identity,
   source timestamps, halt code and resumption fields.
6. **Causal catalyst join** — SEC accession/acceptance timestamps and existing
   news events joined as optional context; absence remains absence.
7. **Feature snapshot ledger** — immutable `X(i,t)` with input observation ids,
   feature/rule version, `known_at`, and missing-reason codes.
8. **Triple-barrier labels** — target, stop and timeout fixed at signal time;
   ambiguous daily bars are not resolved optimistically.
9. **Calibrated model evaluation** — Brier score, log loss, calibration curve,
   expected calibration error, and decision-curve/net-EV results.
10. **Portfolio simulator** — immutable cohorts, concurrency/gross exposure caps,
   gap-CVaR sizing and calendar-block bootstrap.

## Validation gates

Historical promotion to forward observation requires all of:

1. Causal invariance test: recomputing at `t` from a truncated series equals the
   full-series feature at `t`.
2. Trial declared before measurement; constants and code/data contract hashed.
3. Purged folds and embargo around every label window.
4. Calendar-block bootstrap at least as long as the maximum holding period.
5. Matched random-entry control on the same dates, universe, exposure and costs.
6. Stratification by era, price, liquidity, exchange and catalyst availability.
7. Net result under base and stressed spread/slippage/borrow assumptions.
8. No single day, issuer, catalyst class, month or best 1% of trades dominates.
9. Portfolio drawdown and gap-CVaR within pre-registered limits.
10. No paper order is enabled yet: historical validation promotes only to a
    forward **observation** ledger. A later operator decision promotes observation
    to paper execution after enough untouched events accumulate.

## First bounded experiments

Run these in order; do not tune later experiments using an opened hold-out:

1. **Completed:** corrected long confluence retracts the invalid breadth result
   and finds no monotonic alignment gradient.
2. Put the frozen >=12% short-continuation rule through the existing portfolio,
   block-bootstrap, trial-register and random-entry machinery.
3. Test raw versus market/sector-residual shock with one fixed volatility
   normalisation and the same execution rule.
4. Test `shock_z x close_location x abnormal_volume` as one pre-registered
   interaction model; no subset search.
5. Test structure only as incremental information for `target_first` versus
   `stop_first`, against matched arbitrary-level placebos.
6. Test Gao et al.'s exact first/last-half-hour formulation on liquid US ETFs,
   separately by instrument and after observed eToro costs. Do not start with
   the broad single-stock universe.

Until those experiments pass, the correct operator-visible state is
`research_candidate`, never `validated` or `paper_ready`.
