# Opening-range / Stocks-in-Play replication contract

Date: 2026-08-10  
Status: delayed research measurement remains blocked on #2520; live execution
is refused after the no-subscription source audit in #2521
Issues: #2485, #2477, #2508, #2520, #2521

## Decision

Do not evaluate or expose an Opening Range Breakout (ORB) strategy over eBull's
eight-name intraday research panel. That would not reproduce the published
strategy and would give its result an invalid prior.

The valuable finding in Zarattini, Barbon and Aziz is not “price crossed the
first five-minute high/low”. It is the interaction between that rule and a
daily, point-in-time **Stocks in Play** selection made across the US common-stock
market. The selection must happen before a breakout is observed. Exchange,
security type, nominal price, normal liquidity, volatility and abnormal opening
volume are therefore inputs to the candidate, not post-hoc labels.

The final paper revision was verified from the
[authors' April 29, 2025 PDF](https://concretumgroup.com/wp-content/uploads/2026/02/A-Profitable-Day-Trading-Strategy-For-The-U.S.-Equity-Market.pdf)
and [SSRN record](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4729284).
Its evidence covers 2016–2023. It is prior evidence only; it is not evidence of
performance in 2026 or at eToro.

## Published rule, without adaptation

The paper's point-in-time universe contains NYSE- and Nasdaq-listed equities,
including later-delisted names. It does not publish its CRSP share-code
whitelist, so “equities” cannot yet be mapped exactly to eToro instrument types.
eBull's proposed executable arm predeclares common stocks only (excluding ETFs
and other types), records that as an adaptation, and keeps exact-replication
status refused until the paper's security-type boundary is confirmed. For each
session and instrument the paper applies:

1. opening price strictly above USD 5;
2. arithmetic average share volume over the previous 14 completed sessions at
   least 1,000,000 shares/day;
3. 14-session ATR strictly above USD 0.50;
4. first-five-minute relative volume at least 1.0, where
   `RVOL[t] = opening_5m_volume[t] / mean(opening_5m_volume[t-14:t-1])`;
5. retain at most the 20 eligible instruments with the highest RVOL that day;
6. bullish first five-minute candle: buy stop at its high; bearish candle: sell
   stop at its low; doji: no order;
7. after entry, stop distance is 10% of the 14-session ATR; if not stopped,
   close at the regular-session close;
8. size each position to at most 1% portfolio loss at the stop, subject in the
   paper to 4x maximum leverage.

The paper starts with USD 25,000 and charges USD 0.0035/share. eBull's trial is
predeclared **unlevered** and must use observed eToro bid/ask, spread and order
costs instead. The paper specifies no profit target and no ratcheting stop.
Adding either is a separate hypothesis and trial, not a faithful replication.

The paper describes ATR's true-range components but does not publish executable
code for this all-stock cross-sectional test. Its related single-instrument ORB
example uses a lagged 14-session arithmetic mean of true range, but that is not
proof that the portfolio paper used identical code. Until this is resolved from
an authoritative implementation, ATR calculation is a named reproducibility
ambiguity and the exact replication remains fail-closed.

## What eBull has, and what it does not

Already available:

- 6,083 currently classified NYSE/Nasdaq common stocks in the prospective eToro
  universe (3,618 Nasdaq and 2,465 NYSE at the 2026-08-10 census);
- transition-only security type and primary-listing history from 2026-08-10;
- daily OHLCV sufficient to calculate causal price, volume and ATR screens where
  14 complete as-traded observations exist;
- a bounded eight-name 5-minute panel and prospective eToro bid/ask observations;
- shared cohort verification across mechanism, type, listing, price, liquidity
  and their predeclared interactions.

Missing for an honest ORB test:

- a point-in-time, survivorship-safe historical NYSE/Nasdaq common-stock
  membership source; today's eToro universe cannot be projected backwards;
- the paper's CRSP share-code/security-type inclusion rule;
- first-five-minute volume for the **whole prefiltered cross-section** each day;
- post-09:35 intraday paths for the selected top 20, at sufficient resolution to
  resolve breakout/stop ordering and gap-through fills;
- historical decision-time eToro quotes, spreads, slippage and shortability;
- a confirmed all-stock ATR implementation and an as-traded corporate-action
  bridge for nominal price/ATR/volume comparability.

The current collector cannot close the cross-sectional gap. eToro's official
candle contract is one instrument per request. Its current documented
market-data quota is 120 requests per 60 seconds, shared by candles, rates,
instrument metadata and search. Scanning even the currently classified 6,083
NYSE/Nasdaq common stocks therefore has a theoretical lower bound of about 51
minutes before latency, other market-data calls, retries and validation, far
beyond a 09:35 decision. The rates endpoint batches up to 100 instruments but
provides bid, ask, last execution and margin/conversion fields, not volume. The
WebSocket instrument topic carries the same price family and no volume.

The separate #2521 audit found no documented bulk candle, opening-volume,
screener or market-mover endpoint. Personalized market recommendations return
only a list of instrument IDs and publish neither a complete cross-section nor
a reproducible volume ranking. They are not a Stocks-in-Play input. See
`2026-08-12-free-live-opening-volume-source-result.md` for the evidence and
refusal.

## Free-data feasibility and its boundary

Alpaca's [official market-data documentation](https://docs.alpaca.markets/us/v1.1/docs/about-market-data-api)
and [FAQ](https://docs.alpaca.markets/us/docs/market-data-faq) say a free account
can query historical SIP data when the request ends at least 15 minutes in the
past, with history since 2016 and a 200-request/minute limit. SIP combines the
CTA/NYSE and UTP/Nasdaq feeds; raw minute bars include OHLCV. This makes a
**delayed historical replication feed** worth a separate credentialed spike. It
does not solve live 09:35 discovery: free real-time equities are IEX-only, a
single-exchange and non-comparable slice of consolidated US volume, while
current SIP is delayed. Alpaca's current official pages are inconsistent about
the free historical entitlement, so #2520 must prove the exact account response
with credentials before any implementation assumes access.

Accordingly:

- Alpaca may be used to build/replay a recent historical candidate census;
- eToro remains the execution and executable-cost source;
- Alpaca volume must never be mixed with eToro volume under one source label;
- a delayed research result cannot be described as a real-time executable path;
- no subscription will be purchased or assumed.

No demo order may be emitted from this candidate until a later version proves a
complete decision-time discovery source. A positive delayed backtest would
justify continued prospective observation; it would not clear this execution
refusal.

## Bounded storage design

Do not retain a full-market tick tape or every derived indicator. The minimum
research shape is:

1. one immutable daily eligibility/opening summary per instrument/session:
   source/version, point-in-time identity, open, prior-volume mean, ATR,
   opening-five-minute OHLCV, RVOL, eligibility/refusal and rank;
2. raw one-minute RTH bars only for the selected top 20 plus declared controls,
   partitioned monthly and retained for the active validation horizon;
3. one immutable potential-fire decision with observed eToro quote status;
4. one terminal outcome with fill assumptions, stop/close exit and cost model.

At the paper's 7,000-name scale the summary ceiling is about 1.76 million rows
per year (`7,000 × 252`), rather than roughly 688 million one-minute full-market
rows. Selected paths are at most about 1.94 million rows/year
(`20 × 385 × 252`) before controls. Exact bytes and query plans must be measured
on a sample partition before enabling retention; whole expired partitions are
dropped. Rolling ATR/RVOL/indicator series are computed causally, not stored.

## Frozen analysis, once data exists

The primary trial is the paper's 5-minute, top-20 selection, unlevered. The
15/30/60-minute variants are separate trials. Report recent calendar months
individually, a longer context window, and an untouched later interval; never
select the best window after seeing outcomes.

Required output includes after-cost expectancy and clustered confidence
interval, profit factor, hit rate, drawdown and expected shortfall, turnover,
capacity/concurrency, long/short attribution, spread deciles and the shared
type/listing/price/dollar-volume interaction report. Compare against:

- matched random entry after the same Stocks-in-Play selection;
- selection-only same-direction exposure without breakout timing;
- the unfiltered ORB base rule;
- doubled costs and delayed/missed fills.

A subgroup can explain a result but cannot rescue a failed primary trial on the
same observations. A promising subgroup becomes a newly versioned hypothesis
tested on later untouched data. Promotion remains refused unless the lower
confidence bound on net expectancy is positive and the portfolio, broker,
freshness, shortability and prospective gates all pass.

## Implication for “AI trading systems”

Open-source systems such as Microsoft's
[Qlib](https://www.microsoft.com/en-us/research/publication/qlib-an-ai-oriented-quantitative-investment-platform/)
and [FinRL](https://arxiv.org/abs/2111.09395) are useful experiment engines:
they provide datasets, models, environments and evaluation machinery. They are
not portable proof that a supplied policy makes money after this broker's costs.
The reusable work is their discipline—point-in-time features, walk-forward
evaluation, costs, baselines and reproducibility—not a pretrained “winning”
model. Any learned model enters the same candidate ledger and must beat simple
rules on later data without leakage before it can receive demo capital.
