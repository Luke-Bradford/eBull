# Short-horizon strategy opportunity map

Date: 2026-08-11  
Status: research map; no strategy below is promoted or authorised for capital  
Parent: #2469; evidence gates: #2505; mechanism classification: #2507

Capital selection is governed by the later
`2026-08-11-autonomous-opportunity-engine.md` correction. Candidate families
nominate observations; they do not receive funds independently or in database
arrival order.

Candidate admission, trial budget and build order are governed by
`2026-08-11-portfolio-alpha-viability-plan.md`. This map is a taxonomy and
diagnostic backlog, not authority to test every row. In particular, the
sector-residual work remains reserved until it establishes a
mechanism independent of the rejected residual-confluence candidate and an
uncontaminated evaluation interval.

## Decision

eBull does **not** currently have four plausible alternatives for short-horizon
automated trading. It has four permanent backtest-harness controls whose time
horizons do not match the operator's intended minutes-to-two-weeks programme:

| control | actual decision and holding horizon | suitability for this programme |
| --- | --- | --- |
| S-1 time-series momentum | daily 50/200-day trend; no fixed maximum hold | multi-month control, not a short-horizon candidate |
| S-2 cross-sectional momentum | prior 2–12 month return; monthly rebalance | monthly/annual factor control, not day or swing trading |
| S-3 mean reversion in trend | daily RSI pullback; up to 10 sessions | the only current 1–2 week rule; causal evidence is near 50%, not the withdrawn 76.8% claim |
| S-4 volatility-compression breakout | daily breakout; up to 40 sessions | can hold roughly eight weeks; its compression rule was constructed locally and has no completed validating evidence |

All four have `purpose="harness_validation"`. None is a capital candidate. A
larger menu of similarly loose rules would create product noise, not trading
ability.

The programme will instead treat direction, setup, execution trigger, exit and
risk as separate contracts. It will test a small number of mechanism-backed
families at explicitly different horizons. A result at one horizon never
inherits evidence from another.

## The mathematical frame: remove what moves together first

Raw instrument movement combines common and idiosyncratic effects. At each
decision time, estimate causally:

```text
r(i,t) = beta_market(i,t) * r_market(t)
       + beta_sector(i,t) * r_sector(t)
       + residual(i,t)
```

The three quantities answer different questions:

- market component: is this mostly broad risk-on/risk-off movement?
- sector component: is capital rotating through this industry?
- residual: is this instrument moving differently from comparable instruments?

This is why “the stars align” cannot mean several transforms of the same close
price. RSI, a moving-average distance and a recent return are correlated views
of one path. Useful confluence must cover different causal dimensions:

1. **eligible instrument** — point-in-time common-stock/type, primary listing,
   as-traded price, age and liquidity;
2. **market regime** — broad return, breadth, volatility and correlation;
3. **sector regime** — sector return, breadth and relative strength;
4. **setup mechanism** — information continuation, liquidity dislocation,
   periodic flow or relative-value divergence;
5. **entry confirmation** — completed-bar price/volume or quote behaviour on
   the exact cadence the backtest uses;
6. **execution viability** — fresh quote, spread, halt, shortability, capacity
   and cost;
7. **predeclared exit/risk** — invalidation level, target/trailing policy,
   maximum hold and size from loss-at-stop, never from desired profit.

These are primarily AND-gates with named refusals. A fitted score may rank
fully eligible observations, but it may not compensate for missing catalyst,
stale quote, unknown shortability or an uneconomic bracket.

## Opportunity map by horizon

### Minutes to the close

| family | observable mechanism | exact data need | current decision |
| --- | --- | --- | --- |
| Stocks-in-Play opening-range breakout | abnormal opening participation followed by a first-range break | full-market opening 5-minute volume, selected intraday paths, quotes and shortability | exact contract frozen in #2485; eight-name imitation refused; #2521 found no qualifying free live discovery source, while delayed-history access remains unproved in #2520 |
| 30-minute residual reversal | provide liquidity against an extreme stock-specific move after removing intraday factors | broad, continuously covered bid/ask midpoints and factor cross-section | exact replication in #2484; collect prospectively, do not substitute candles |
| same-clock intraday continuation | a stock's return in one half-hour predicts the same half-hour on later days | broad half-hour cross-section with stable session clock and execution quotes | independent research family, but lower priority until broad intraday data exists |
| first-to-last-half-hour ETF momentum | overnight/first-half-hour ETF return predicts the final half-hour | exact ETF interval bars and executable costs | recent eToro test rejected; do not generalise it to stocks |

Heston, Korajczyk and Sadka report continuation at half-hour intervals exactly
one or more trading days apart, lasting up to 40 sessions. They also find that
sub-hour reversal is substantially temporary liquidity imbalance and bid/ask
bounce. This is evidence for two different mechanisms, not permission to trade
either from eight candles.

Order-book imbalance, queue position and millisecond scalping are out of scope.
Free eToro candles and retail order latency cannot reproduce a level-2/HFT
strategy; a backtest that ignores that mismatch would be fictional.

### One to three sessions

This horizon should begin with #2507's mechanism classifier, not a direction
rule. For an abnormal price/volume move:

```text
abnormal_return = stock_return
                - beta_market * market_return
                - beta_sector * sector_return
relative_volume = observed_completed_volume / causal_time_matched_volume
```

- known material catalyst + persistent abnormal volume: test **continuation**;
- no known catalyst + broad/sector stress + later stabilisation: test
  **liquidity reversal**;
- incomplete catalyst or quote coverage: `unknown`, no trade.

The existing broad residual-confluence candidate is not this test. It pooled
negative shocks and failed at its actual action boundary in conservative 2025
(−1.332% expectancy, profit factor 0.66). Reusing its winners to choose new
thresholds would be outcome mining. The next observation set must be a newly
versioned, prospective mechanism trial.

### Three to ten sessions

The highest-priority daily-data research family to source-check is
**sector/factor-neutral relative value**, because it directly models the common
movement the operator identified. The primary Avellaneda-Lee contract has now
been transcribed in
`2026-08-12-sector-etf-residual-replication-contract.md`. For stock `i`, it fits
the single assigned sector-ETF exposure on the 60 completed sessions ending at
the decision close, accumulates the residual, then models its equilibrium and
speed:

```text
X(i,t) = cumulative residual return
s(i,t) = (X(i,t) - equilibrium(i,t)) / equilibrium_sigma(i,t)
characteristic_time(i,t) = 1 / kappa(i,t)
```

The paper admits `kappa > 8.4` (characteristic reversion time below about 30
sessions), opens at `|s| > 1.25`, and exits on score reversion; it does not use
fixed 3/5/10-session exits. Market- or sector-neutral long/short is the
published economic form; fixed-horizon, long-only and unhedged versions are
adaptations and must not inherit its evidence. Point-in-time sector mapping,
survivorship-safe membership/market cap, causal next fills, shortability,
borrow/CFD carry and factor-leg execution are source blockers before outcomes
may be opened.

Avellaneda and Lee's sector-ETF/PCA work supplies a reproducible model, but its
1997–2007 performance decayed after 2002. It is therefore a formula source,
not current evidence. If the source gate passes, eBull must first test the
published score-exit rule as one frozen replication after costs. Fixed
3/5/10-session exits are separately motivated adaptations, not a horizon menu
from which to select a winner on the same outcomes.

Da, Liu and Schaumburg likewise find that the residual component of short-term
reversal—not raw losing returns—is the positive component in their historical
sample. Nagel links reversal returns to liquidity provision and finds strong
VIX conditioning. These support residualisation and a predeclared liquidity/
volatility interaction; they do not rescue eBull's already-failed broad shock
candidate.

### Ten to twenty sessions

Two families merit bounded spikes:

1. **sector-relative continuation after an information event** — earnings or a
   material filing known before entry, abnormal return/volume in the event
   direction, and sector/market decomposition. #2476 already freezes the
   point-in-time earnings-drift investigation. Classic PEAD is not assumed to
   survive: recent high-frequency work reports post-announcement trading
   consistent with efficient price formation after 2016.
2. **high-turnover continuation near a prior high** — a recent published study
   reports that turnover and distance to the 52-week high distinguish short-term
   momentum from reversal. Its exact executable formulation must be obtained
   and frozen before measurement; until then it is a literature lead, not a
   candidate.

S-2's 12-to-1-month momentum remains useful as a factor/control, but it does
not answer a two-week capital objective.

## Direction and context features eBull should evaluate

For every horizon, produce causal features at the market, sector and instrument
level; do not persist rolling copies when they can be recomputed:

| dimension | candidate measures | why it is distinct |
| --- | --- | --- |
| trend | 1/3/5/10-session market and sector returns; slope normalised by volatility | direction at the intended horizon |
| breadth | advance/decline share; percent above causal averages; new high/low share | whether a move is broad or carried by a few names |
| dispersion/correlation | cross-sectional residual dispersion; rolling common-factor share | whether stock selection or market beta dominates |
| volatility | realised market/sector/instrument volatility; VIX level/change/percentile | regime and risk, not entry direction by itself |
| participation | causal relative volume, dollar volume and opening/time-matched volume | distinguishes participation from a thin price print |
| relative movement | market/sector residual return and residual z-score | isolates the outlier from shared movement |
| liquidity | observed spread, zero-volume frequency, quote freshness and size/capacity | whether gross edge is executable |
| event state | point-in-time SEC event, halt/corporate action and coverage completeness | separates repricing from a possible dislocation |

The repository already snapshots point-in-time type, listing, price band,
share/dollar volume, relative volume, spread, realised volatility, gap,
market/sector residual z-score and VIX in `strategy_decision_context`. The
material additions are **market/sector breadth, dispersion/correlation,
multi-horizon returns, explicit event-mechanism state and exact intraday
coverage**. This is an enrichment of an existing context model, not a second
indicator warehouse.

## Validation contract

“Recent” is the primary relevance window, but it cannot mean repeatedly tuning
on the same recent months. Each family declares its cadence and fixed horizons
before outcomes are opened:

1. development on an earlier post-2020 interval;
2. anchored walk-forward validation with purge/embargo at least as long as the
   maximum holding period;
3. a terminal recent interval accessed once and logged;
4. prospective shadow decisions after the rule is frozen;
5. demo execution only after historical and prospective evidence plus broker
   safety gates pass.

Report every 1/3/5/10/20-session arm as a declared trial. Required metrics are
net expectancy and clustered confidence interval, profit factor, payoff ratio,
hit rate, drawdown, expected shortfall, turnover, capacity/concurrency, time to
outcome, regime/year stability, calibration and concentration excluding the
best observations. Compare with matched random timing, raw-return and
market/sector-only challengers.

A 75% hit rate is not the target. The objective is a positive lower confidence
bound on after-cost expectancy with acceptable tails. A 33% hit rate can be
profitable with winners larger than twice losers; the rejected ETF result was
bad because expectancy remained negative, not merely because 33% looked low.
Conversely, a tiny target and a large stop can manufacture a 75% hit rate while
losing money.

Machine learning may model declared interactions or calibrate the probability
of a predeclared outcome. It must beat a simple logistic/rule baseline on later
data and must not choose the setup, horizon, threshold and exit from one sample.
Open-source platforms such as Qlib and FinRL are experiment frameworks, not
pretrained profitable policies.

## Data and storage impact

No broad derived-feature history is required. Retain:

- bounded raw daily prices and compact market/sector regime series;
- bounded raw intraday bars/quotes only for a predeclared panel or selected
  candidates, with coverage provenance and partition retention;
- one current pre-trigger state per strategy/instrument/cadence by upsert;
- one immutable fired/refused decision and one terminal outcome;
- aggregate evidence and declared cohort counts.

Routine `not_fired` evaluations, indicator values and polling heartbeats remain
aggregates or short-lived telemetry. This preserves the storage contract in
#2465/#2485 and keeps the strategies page focused on money, positions and
promoted candidates.

## Ordered programme

1. Keep S-1..S-4 as controls and label their true horizons; do not tune them
   into short-horizon candidates.
2. Complete #2507's point-in-time mechanism state and finish #2523's production
   path. Broad snapshot breadth, prospective provider-industry identity and
   pure completed-session multi-horizon/dispersion/common-movement mathematics
   are now implemented; candidate-owned loading, compact persistence and exact
   intraday coverage remain.
3. Run #2522's source census against the exact sector-ETF residual contract.
   Do not open outcomes or substitute fixed 3/5/10-session exits until
   point-in-time mapping/membership and two-leg execution evidence exist.
4. Freeze #2582's exact initial-Schedule-13D trial after its successful source
   census. The already-open #2476 PEAD trial is inconclusive and closed; do not
   reopen its outcomes or describe it as pending work.
5. Continue prospective intraday collection; run #2484 and #2485 only when
   their exact input universes exist. Do not wait for them before learning from
   daily 1–20 session outcomes.
6. Add a candidate to the product only after #2505's attribution and evidence
   gates pass. Until then the honest catalogue remains empty.

## Primary research references

- Heston, Korajczyk and Sadka, [Intraday Patterns in the Cross-section of Stock
  Returns](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2010.01573.x)
- Da, Liu and Schaumburg, [Decomposing Short-Term Return
  Reversal](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1551025)
- Nagel, [Evaporating
  Liquidity](https://academic.oup.com/rfs/article-abstract/25/7/2005/1602153)
- Avellaneda and Lee, [Statistical Arbitrage in the U.S. Equities
  Market](https://math.nyu.edu/~avellane/AvellanedaLeeStatArb20090616.pdf)
- Liu and Stentoft, [Intraday Stock Predictability
  Everywhere](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4496917)
- Christensen, Timmermann and Veliyev, [Warp speed price moves: Jumps after
  earnings announcements](https://arxiv.org/abs/2601.08962)
- Hung and Yang, [Short-term momentum and reversals, turnover, and a stock's
  price-to-52-week-high
  ratio](https://www.sciencedirect.com/science/article/abs/pii/S0927539824000902)

## What “exhausted the research” can honestly mean

No team can exhaust quantitative finance, and proprietary profitable systems
do not publish their current edge. The defensible substitute is a maintained
hypothesis registry: search primary and recent replication evidence by
mechanism and horizon, record every investigated family and rejection, declare
every empirical trial before measurement, and require later data to confirm
anything discovered. That process prevents both tunnel vision and the endless
addition of plausible-sounding indicators.
