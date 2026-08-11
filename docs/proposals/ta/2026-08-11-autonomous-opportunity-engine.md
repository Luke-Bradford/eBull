# Autonomous portfolio and opportunity engine

Date: 2026-08-11
Status: architecture correction; required before any capital-backed paper run
Parent: #2469; implementation: #2525; candidate evidence: #2505; context: #2523

Research admission and build order are governed by
`docs/proposals/ta/2026-08-11-portfolio-alpha-viability-plan.md`. In particular,
the allocator must not turn the four harness controls or any survivor-only,
carry-unmodelled result into capital merely because this architecture exists.

## Decision

The capital-facing product must not be a picker of independent named
strategies. The operator selects a **portfolio mandate**, not a trading rule:
risk tolerance, liquidity need, permitted instruments/directions and maximum
acceptable loss. The system owns allocation within that mandate.

A hands-off investment system has two nested decisions:

1. how the whole pot should be divided among benchmark exposure, cash/defensive
   exposure, hedges and active alpha risk;
2. within the active budget, which current opportunities—if any—deserve capital.

The trading desk therefore answers:

> Of every executable opportunity observable now, which positions—if any—have
> the best conservative expected payoff after costs and portfolio risk?

The default active-trade answer is **none**. That does not imply the whole pot
is idle: the portfolio mandate may retain benchmark/core exposure. A strategy
rule may nominate an observation but cannot spend capital. Only the portfolio
decision can do that.

This changes the unit of automation from a strategy to:

```text
(decision time, instrument, direction, horizon, setup version, exit policy)
```

The engine separates six jobs:

```text
operator mandate and benchmark
  -> strategic/core allocation and risk budget
  -> market scanner
  -> mechanism + horizon forecasts
  -> executable opportunity distribution
  -> portfolio comparison and capital decision
  -> broker execution and lifecycle management
```

No model is allowed to collapse missing information into a favourable score.
Eligibility, data freshness, broker state and portfolio limits remain hard
refusals.

## The fund-level mandate

Pension funds, asset managers and hedge funds do not begin by selecting a chart
pattern. They begin with objectives and constraints: required return, acceptable
risk, cash needs, benchmark, horizon and permitted exposures. UK pension
guidance similarly separates return, risk and cash-flow objectives.

For eBull the minimum mandate record is:

- base currency and investable universe;
- benchmark and inflation index;
- risk profile expressed as target volatility, maximum drawdown/tail loss and
  maximum loss-at-stop—not a vague `low/medium/high` label alone;
- minimum liquidity/cash reserve;
- permitted long, short, hedge and eventually leverage authorities;
- maximum market beta, sector/factor concentration and tracking error;
- evaluation horizon and tax/cost assumptions.

Friendly risk labels can map to immutable, visible policy values, but the labels
are presentation. The risk budget is the contract.

### Benchmark and real-return accounting

If the objective is to beat the S&P 500, the comparator must be a total-return
series including dividends, converted causally into the account base currency.
Comparing against the price index would understate the hurdle. For a GBP
operator, report both:

```text
active_return = net_portfolio_return - S&P_500_total_return_GBP

real_return = (1 + net_portfolio_return) / (1 + UK_CPIH_inflation) - 1
```

CPIH is the ONS lead measure of UK consumer-price inflation. These are two
different tests: purchasing-power growth and value added over passive US equity
risk. Results are net of spreads, financing, FX, fees and realised trading
costs. Daily or annual outperformance cannot be guaranteed; mandate success is
reported over predeclared rolling and since-inception horizons with drawdown and
tracking error beside return.

S&P Dow Jones Indices reported that 79% of active US large-cap funds
underperformed the S&P 500 in 2025. That makes the index a demanding baseline,
not a minimum return the application can promise. A system that beats inflation
but trails a cheap benchmark has protected purchasing power but has not
demonstrated equity alpha.

### Core plus active overlays

A pot held entirely in cash until a short-horizon signal fires is a market-timing
portfolio and is likely to lag during persistent equity advances. The default
fund shape should therefore be evaluated as:

```text
portfolio = liquid benchmark/core exposure
          + active instrument/sector tilts
          + market-neutral or hedged alpha positions
          + defensive hedge/cash reserve
```

This is a research architecture, not an instruction to buy SPY immediately.
The core instrument, currency treatment, dividends, costs and rebalance policy
need their own validated implementation. The key point is attribution: passive
market return is not alpha, cash drag is visible, and the active layer must prove
that it improves the core portfolio net of costs for the selected risk level.

At each portfolio decision time, choose target weights to maximise conservative
net expected utility:

```text
max_w  E[net portfolio return]
     - gamma * portfolio variance
     - lambda_tail * expected shortfall
     - lambda_tracking * benchmark tracking error
     - transaction and financing costs
```

subject to the mandate, broker and concentration constraints. The risk-profile
choice primarily changes `gamma`, tail/drawdown limits, beta range and active
risk budget. It does not expose a strategy checklist.

Portfolio targets use no-trade bands: rebalance only when the expected benefit
of moving toward target exceeds costs and risk tolerance. Constant activity is
not evidence the money is being managed well.

### Where returns are intended to come from

Every pound of P&L is attributed to:

- benchmark beta and currency movement;
- sector/factor allocation;
- instrument selection;
- entry/exit timing;
- hedging and cash;
- spread, slippage, financing and other costs.

The pot grows when core exposure earns market return and the active/defensive
decisions add more value than their costs and mistakes. It does not grow because
the engine is forced to trade daily. If no proven active opportunity exists,
the mandate-defined core/cash allocation is preferable to inventing one.

## The original arrival-order gap and its bounded replacement

Before #2547, `strategy_paper_runtime.run_strategy_paper_cycle` selected
unfunded fired signals using:

```sql
ORDER BY s.signal_id DESC
LIMIT :signal_limit
```

and invoked `execute_fired_paper_signal` for each row. #2547 removes that
arrival-order path: the runtime now loads the complete current, calibrated,
positive-forecast set, ranks it by conservative after-cost expectancy with an
economic-key tie break, and only then applies its bounded execution limit.

That is a safety precursor, not the finished allocator. #2549 makes each
opportunity-bearing ranking set compact and immutable, records selected and
declined reasons, deduplicates unchanged polling cycles, and links execution
preflight to the exact selected member. It still does not produce a target
portfolio or jointly optimise correlation, factor exposure, capital duration,
core/cash competition and aggregate opportunity cost. Those remain blocking
requirements below.

This is not presently a capital defect because the strategy manifest contains
no `capital_candidate`. It becomes a blocking defect before the second candidate
can be paper-enabled, and arguably before the first: queue order is not an
investment policy.

The existing `residual_confluence_candidate` contains the right primitive for
one rejected experiment:

```text
EV_net = P(TP first) * TP payoff
       + P(SL first) * SL payoff
       + P(timeout) * mean timeout payoff
```

That primitive must become a generic, calibrated opportunity contract. Its
failed model is not reused.

## What constitutes an opportunity

A nominated observation progresses through four contracts.

### 1. Hard eligibility

All must be known at the decision time:

- point-in-time security type, primary market and tradability;
- as-traded price, minimum dollar volume and capacity;
- complete inputs for the exact model cadence;
- current quote/spread, session and halt state;
- direction-specific broker eligibility and shortability;
- event coverage sufficient to distinguish `unknown` from no known catalyst;
- portfolio and strategy risk controls available and current.

Failure does not reduce a score. It refuses the opportunity.

### 2. Multi-horizon path forecast

Each model produces an out-of-sample distribution for one frozen horizon and
exit policy, not a binary buy/sell label:

```text
P(target first)
P(stop first)
P(timeout)
E[net return | timeout]
expected sessions to resolution
prediction uncertainty / calibration bin
```

Candidate barriers are derived from a declared invalidation mechanism and
causal volatility/structure. They are not moved until a desired historical win
rate appears. A 3-session forecast and a 10-session forecast are distinct
trials and competing uses of capital.

The target/stop barrier percentages are frozen separately from their
conditional **net** payoffs. #2551 makes those exact barriers part of every new
forecast and uses them for loss-at-stop sizing and the submitted broker TP/SL.
A generic deployment default may cap risk but may not silently replace the
geometry whose probabilities authorised the order. Legacy forecasts without
barriers fail closed.

Where useful, prediction is staged:

1. market direction and volatility state;
2. sector direction and dispersion state;
3. stock-specific residual direction;
4. event/liquidity mechanism;
5. path-to-barrier and executable cost.

This is an ensemble only where the components provide distinct information.
Several transforms of the same price series are one feature family, not five
confirmations.

### 3. Conservative opportunity value

For opportunity `j`:

```text
EV_net(j) = sum_k P_j(k) * net_payoff_j(k)

conservative_EV(j) = lower confidence bound of EV_net(j)
                   - uncertainty haircut(j)

utility(j | portfolio) = conservative_EV(j)
                       - lambda_tail * marginal_expected_shortfall(j)
                       - lambda_corr * incremental_concentration(j)
                       - lambda_cost * cost_stress(j)
```

The lambdas and uncertainty haircut are frozen policy inputs, not fitted to
make a backtest pass. Opportunity ranking happens only after every candidate
clears a positive minimum conservative EV and its model demonstrates calibration.

Expected capital duration is reported and constrained. It must not be hidden by
annualising a tiny sample of very short trades. Comparison across horizons uses
portfolio simulation with cash occupation, overlapping opportunities and
turnover, not a naive `EV / days` ranking that would mechanically favour
scalping.

### 4. Portfolio allocation

The allocator observes the current core portfolio and all eligible
opportunities in one decision batch, then chooses target weights subject to:

- shared fixed or compounding capital authority;
- loss-at-stop risk budget;
- per-position and per-strategy caps;
- market, sector, instrument and correlated-model concentration;
- maximum concurrent positions and aggregate worst-case stop loss;
- cash, broker and order constraints;
- turnover/cost budget and current open-position exposures.

Position size begins with:

```text
units = permitted_loss_amount / abs(entry_price - stop_price)
```

then decreases for capacity, correlation, uncertainty and broker limits. It
never increases merely to reach a profit target.

Every eligible but unfunded opportunity receives a durable reason such as
`lower_ranked_than_funded_set`, `correlation_budget`, `capital_occupied` or
`uncertainty_too_high`. It still resolves in shadow so the selection policy can
be evaluated against opportunities it declined.

## Calibration and the requested high certainty

A model may say `P(TP first) = 0.78` only when later untouched observations in
that prediction band resolve near 78%, with sample size and uncertainty shown.
Required forecast diagnostics are:

- reliability/calibration curve;
- Brier score and multiclass log loss;
- calibration intercept and slope;
- count, effective count and outcome rate per probability bin;
- expected calibration error with a predeclared binning rule;
- calibration drift by recent month/regime;
- coverage: what share of observations survive a high-confidence threshold.

The automation may use an abstention policy such as `trade only if the lower
confidence bound on P(TP first) exceeds the policy threshold`, but the threshold
is validated as part of the policy. A nominal 75% prediction is not 75%
certainty. If no adequately calibrated observations meet it, the correct system
places no trades.

Win probability also remains subordinate to payoff. The funding boundary is:

```text
lower confidence bound on after-cost expectancy > 0
```

plus tail and portfolio gates. High hit rate with rare catastrophic losses is
not accepted.

## Research and model roles

Named strategy families become hypothesis and feature generators:

- opening participation / breakout;
- information-event continuation;
- liquidity-dislocation reversal;
- market/sector residual relative value;
- trend/volatility regime;
- intraday periodic flow.

The comparative model determines when a family is applicable and whether its
current opportunity clears the funding boundary. It must be challenged by:

- the setup alone without meta-selection;
- market/sector exposure alone;
- a simple regularised logistic or rules baseline;
- matched random opportunities;
- the same allocator with predicted ranks shuffled;
- doubled costs, delayed fills and removed best observations.

Machine learning is appropriate for modelling nonlinear interactions and
ranking once the economic hypotheses and labels are frozen. Research on
cross-sectional return prediction consistently identifies trend, liquidity and
volatility among important predictor families, but published model returns are
not portable to eToro. Combining many signals greatly raises overfitting risk;
the committed trial ledger and untouched/prospective evidence remain binding.

## Lifecycle and stop management

Entry, management and exit are one versioned policy:

- initial stop expresses the setup's invalidation point and maximum permitted
  loss;
- target expresses the declared payoff distribution, if the mechanism has one;
- time stop closes an opportunity whose forecast horizon expired;
- ratcheting is allowed only from predeclared, causally confirmed structure or
  volatility rules;
- a manual close records `operator_closed` and resolves the strategy-owned
  lifecycle without touching unrelated manual positions;
- model deterioration may block new entries but never removes protection from
  an existing position.

Changing a barrier, ratchet or timeout changes the opportunity-policy identity
and requires new evidence.

## Storage and runtime shape

At each scheduled cadence:

1. compute candidates in memory from bounded raw sources;
2. persist one compact row only for fully evaluated eligible/refused
   opportunities that cross the declared observation boundary;
3. rank one complete batch, never rows arriving in database order;
4. persist one immutable allocation decision containing the considered set,
   scores, constraints and chosen subset;
5. send chosen positions through the existing fail-closed executor;
6. retain one terminal outcome per observed opportunity.

Do not store every feature vector for every instrument and heartbeat. Store the
versioned decision inputs needed to reproduce funded and shadow decisions;
aggregate routine negatives and expire raw intraday partitions under their
source retention contracts.

## Product implication

The operator should primarily configure:

- capital and fixed-contribution or compounding mode;
- risk profile, expressed through its actual volatility/drawdown/tail limits;
- benchmark, base currency and evaluation horizon;
- maximum loss per position/day/portfolio and active-risk budget;
- allowed holding horizons and directions;
- maximum concurrency and concentration;
- minimum evidence/prediction policy;
- global pause.

The landing page shows total pot and net growth, real return, return versus the
total-return benchmark, equity/drawdown, core versus active allocation, capital
at risk, open positions and recent allocation decisions. Named research
families live in an evidence drill-through. The user does not need to decide
whether RSI or a breakout deserves the next pound; portfolio construction and
comparative selection are the system's purpose.

## Ordered implementation

1. Define the versioned mandate, total-return/inflation benchmarks and
   attribution model. Refuse performance claims while dividends, FX or costs do
   not reconcile.
2. Define a versioned `OpportunityForecast` and `OpportunityRefusal` contract,
   including horizon, side, barriers, probabilities, calibration and complete
   cost distribution.
3. Build a batch selector that accepts a whole decision-time opportunity set,
   applies hard refusals and ranks only positive conservative-EV candidates.
4. Integrate the mandate, core targets, current positions and `portfolio_risk`
   into subset selection, including factor, tracking-error, correlated-strategy
   and aggregate stop-loss constraints.
5. Replace newest-first funding in `strategy_paper_runtime` with one immutable
   target-portfolio allocation. Until then, permit at most zero capital
   candidates.
6. Shadow-resolve both funded and declined eligible opportunities and evaluate
   the allocator separately from each signal model.
7. Only then connect a promoted candidate to demo execution.

## Evidence references

- Gu, Kelly and Xiu, [Empirical Asset Pricing via Machine
  Learning](https://academic.oup.com/rfs/article/33/5/2223/5758276)
- Bailey et al., [The Effects of Backtest Overfitting on Out-of-Sample
  Performance](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2308659)
- Harvey and Liu,
  [Backtesting](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2345489)
- Novy-Marx, [Backtesting Strategies Based on Multiple
  Signals](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2629935)
- Liu and Stentoft, [Intraday Stock Predictability
  Everywhere](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4496917)
- Malamud and Pedersen, [Machine Learning and the Implementable Efficient
  Frontier](https://academic.oup.com/rfs/advance-article/doi/10.1093/rfs/hhag022/8524346)
- S&P Dow Jones Indices, [SPIVA U.S. Year-End
  2025](https://www.spglobal.com/spdji/en/spiva/article/spiva-us)
- UK Office for National Statistics, [Consumer Prices Indices Technical
  Guidance](https://www.ons.gov.uk/economy/inflationandpriceindices/methodologies/consumerpricesindicestechnicalguidance/pdf)

## Acceptance before hands-off demo trading

Implementation status (2026-08-11): the portfolio mandate and its pre-trade
risk ceilings are enforced. #2545 adds the compact immutable forecast contract
and makes a current, passed calibration plus positive conservative after-cost
expectancy mandatory before broker access. It does **not** manufacture a
forecast from the four research harnesses: no production candidate currently
has a calibrated forecast generator. The ranking batch is now auditable, but
target-portfolio optimisation and the prospective outcome monitor below remain
required. The safe current behaviour is therefore to place no autonomous
trades.

- [ ] Every funded order points to one reproducible forecast, policy and batch
  allocation decision.
- [ ] Total portfolio return reconciles into benchmark, active, hedge/cash, FX
  and cost attribution; S&P comparison uses total return in base currency.
- [x] Risk-profile labels resolve to immutable measurable mandate limits.
- [ ] All simultaneous opportunities are compared; database arrival order has
  no effect, proved by permutation tests.
- [ ] Predictions are calibrated on later data and monitored prospectively.
- [ ] Declined eligible opportunities resolve in shadow, allowing measurement
  of selection value and opportunity cost.
- [ ] The allocator adds after-cost value over setup-only, shuffled-rank and
  matched-random controls.
- [ ] Worst-case concurrent stops, correlation, drawdown, cost stress and cash
  occupation remain within operator limits.
- [ ] Unknown data, broker state or model calibration fails closed.
- [ ] No claim of daily profit or a guaranteed hit rate appears in the product.
