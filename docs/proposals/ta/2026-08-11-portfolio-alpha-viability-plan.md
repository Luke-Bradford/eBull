# Portfolio alpha viability plan

Status: **research gate; not implementation-ready**  
Parent: #2469  
Portfolio contract: #2525 and
`docs/proposals/ta/2026-08-11-autonomous-opportunity-engine.md`

This is the red-team plan for deciding whether eBull has anything fit to receive
capital. It replaces the idea that adding indicators, strategies or backtests is
progress by itself. The next implementation is authorised only where this document
names an evidence gate and the gate has passed.

The desired product remains simple for the operator: assign a pot, select a risk
mandate, and let the system choose whether to hold core exposure, cash or one or more
validated opportunities. The system is allowed to choose no active trade. It does not
promise a daily profit, a 75% win rate, or outperformance of the S&P 500.

## 1. What has been established

The following is a measured ledger, not a recollection of prior work. Database
measurements were taken on 2026-08-11.

| proposition | status | evidence and consequence |
| --- | --- | --- |
| The four visible strategies are investable candidates | **refuted** | S-1 through S-4 are all `purpose = harness_validation`. The current primary-window holdout gives S1 and S4 negative expectancy intervals; S3 has a negative point estimate with an interval crossing zero; and S2 is statistically uncertain, low-Sharpe and 43.68 points behind buy-and-hold. They test machinery; they must not be funded or presented as choices for capital. See `2026-08-12-s1-s4-primary-holdout-result.md`. |
| Current stored results can authorise capital | **refuted** | All 196 rows are `survivor_only` and `carry_unmodelled = true`. None is promotion evidence. Recent non-null S-2 Deflated Sharpe values are low (about 0.003-0.041) and are computed against the current, already incomplete trial register. |
| The declared multiple-testing denominator is complete | **refuted, fail-closed correction begun** | The register previously declared 12 trials. It now counts the documented 101-arm 2026-08-09 session as a traceable conservative search-family floor, making the current declared count 113, and promotion refuses results stamped against an older register. Later residual, event and intraday searches still require reconstruction; 113 is therefore a floor, not a claim of completeness. |
| The retained intraday and quote panel is ready for historical validation | **refuted** | `strategy_intraday_bars` has 9,013 rows over eight instruments; one-minute data covers two instruments. `strategy_quote_observations` has 376 observed samples from 2026-08-10 16:00-20:10 UTC. This retained panel is a collection/execution pilot, not a return sample. Separately, eToro REST can return the latest 1,000 OHLCV bars on demand: roughly two days at one minute, five days at five minutes, one month at 30 minutes, two months at one hour and eight months at four hours. That is usable bounded history, not a deep arbitrary-date corpus. |
| Historical universe membership is available | **refuted** | All 12,695 `instrument_universe_membership` rows start on 2026-08-10. They provide correct prospective membership, but cannot remove historical survivorship bias. |
| Storing bounded observations will bloat the database now | **refuted for the current panel** | The database is already 63 GB, but the current intraday partition tree is 1,216 kB of heap and 1,960 kB including indexes; quote observations are 192 kB. The storage contract is sound if the bounded panel, retention and no-derived-series rules remain enforced. Existing 3.5 GB `research_price_daily` and other historical relations—not this pilot—dominate. |
| eToro demo fills validate real trading costs | **refuted** | eToro states virtual trades carry no fees even though the virtual account mirrors features and live market conditions. Demo can test integration, timing, state and reconciliation; it cannot validate commissions, CFD spread charges, borrow, overnight financing or live slippage. Those require conservative modelling plus broker cost observations. |
| A long, unleveraged eToro stock is economically the same as a short | **refuted** | eToro states a non-leveraged long stock normally owns the underlying and receives dividends. Shorts and leveraged positions are CFDs. Current fees can include the market spread, a CFD opening/closing charge and overnight/borrow charges; product classification can vary and must be checked per proposed order. |
| The recent eToro benchmark is total return | **refuted** | The 7,693-series main equity corpus has populated split-and-dividend-adjusted `adj_close` through 2026-07-08; its `adjustment_basis = split_adjusted` describes OHLC, not `adj_close`. That does **not** solve the benchmark: the exact legacy market/sector comparator set with total-return-capable `adj_close` stops on 2024-09-27, while the recent eToro comparator snapshot has price candles and deliberately null `adj_close`. A 2026 benchmark claim cannot silently splice the comparator identities. |
| Daily price history alone should yield a high-certainty day-trade rule | **not supported** | Own tests show ordinary liquid-name ten-day drift of about 44 bps against an assumed 50 bps round trip. Published return-prediction work finds low individual-stock signal-to-noise and gains mainly from nonlinear interactions among a small set of trend, liquidity and volatility features, usually at portfolio scale—not deterministic calls. |
| ATR supplies the missing edge | **refuted** | ATR makes stop distance and risk comparable across instruments. It changes payoff shape; it does not create positive expectancy and can destroy it. |
| More confirming indicators necessarily create confidence | **refuted** | Correlated transformations of the same OHLCV path do not create independent evidence. The rejected residual-confluence candidate selected a less-bad population but failed at the action boundary: conservative 2025 expectancy was -1.332%, profit factor 0.66 and effective sample size 19. |
| The earlier overnight reversal is active alpha | **refuted** | After decision/fill alignment, the shared-print reversal was not executable. The surviving liquid-name effect was roughly 4-5 bps per day of overnight drift across deciles: the equity risk premium captured by holding, not by repeated trading. |
| A 75% win rate is an appropriate target | **refuted** | Win rate is not value. A 40% hit-rate system can be profitable with asymmetric payoffs; a 75% system can hide rare ruin. Admission is based on conservative after-cost expectancy, drawdown/tail, calibration and portfolio contribution. |

Primary broker references: [eToro fees](https://www.etoro.com/trading/fees/),
[eToro stocks and ownership](https://www.etoro.com/stocks/),
[eToro virtual account](https://www.etoro.com/trading/demo-account/), and
[eToro API index](https://api-portal.etoro.com/llms.txt). Verified against that live
index on 2026-08-11: market-data endpoints share 120 requests per 60 seconds;
ordinary/default reads use 60; order writes share 20; and the eligibility and cost
preflights each have a separate dedicated limit of 20. These are architecture
constraints, not alpha, and must be re-verified before implementation.

## 2. The bounded opportunity universe

“Find all opportunities” cannot mean trying an unlimited indicator catalogue. It
means closing the taxonomy over economic mechanisms and instruments that this
account could legally, economically and reproducibly trade. A new idea must map to
one family below or justify a new mechanism before it touches outcomes.

Scores are qualitative priors: `high`, `medium`, `low`, or `blocked`. They decide
what deserves a preregistered test; they are not backtest results.

| family / return source | mechanism and published prior | current free-data fit | eToro fit | decision |
| --- | --- | --- | --- | --- |
| **Broad equity beta / core allocation** | Compensation for bearing market risk; own corrected legacy comparator history measured about 6.3-6.6% annualised. This is the hurdle and no-alpha fallback, not a recent claim. | High for exact comparator total return to 2024; recent exact comparator is price return only. The main stock corpus separately has adjusted closes through 2026. | Conditional: the account-specific eligibility response must prove the proposed unleveraged instrument is the underlying product. | **Foundation, not an alpha trial.** Build correct total-return, FX, cash and dividend attribution before judging overlays. |
| **Low-turnover cross-sectional factors** | Value, profitability/quality, investment, momentum and low-risk have extensive published priors. They can diversify a core but may be risk premia rather than arbitrage. | Medium: daily adjusted prices and SEC fundamentals exist, but point-in-time availability, delisted membership and recent total-return gaps must be closed. | Medium/high for long legs; short legs inherit CFD/borrow problems. | **Admit one simple long-only quality-plus-momentum tilt only after the foundation gate.** Monthly/quarterly turnover; no parameter sweep. Not a day-trade claim. |
| **Opportunistic insider purchases** | A forced-information mechanism. Cohen, Malloy and Pomorski report routine trades as essentially uninformative and 82 bps/month abnormal return for opportunistic trades. | The causal 2019Q1–2026Q1 source and sealed 49-month reproduction are now retained in git. | Conditional for liquid unleveraged longs; naturally low turnover. | **Rejected under C-1.** The recent result was negative, the full-period interval crossed zero, placebo did better, drawdown/tail and single-name concentration failed. Preserve the family as evidence; do not retest neighbouring role/size/lag/weight/horizon/exit variants on the opened interval. |
| **Extreme price-shock continuation (short)** | A forced-flow/information-shock hypothesis. The searched event-day result was +49 bps after adverse assumed costs, but had an -87% worst gap and emerged from 101 searches. | Historical OHLCV can reproduce the event stream; historical shortability, exact carry and point-in-time sectors are absent. | **Blocked independently:** measured broker costs remain unusable and shorts are CFDs. | **Rejected under C-2.** All eight frozen capital-allocation arms lost 13.76%–47.72%, with 40.68%–69.05% drawdowns. The event-day mean vanished when finite capital was assigned across as many as 596 same-day signals. Preserve the family and do not rescue it with a threshold, cap, era or sector subset. |
| **Time-series / cross-sectional momentum** | Persistent prior across assets; crash risk rises after market declines amid high volatility and rebounds. Volatility scaling is evidence-based for this family. | High for daily research; recent total-return and PIT universe remain blockers. | Long implementation feasible; short implementation conditional. | Existing S-1/S-2 remain controls. A **new**, low-turnover factor tilt may be part of the factor trial above; do not tune the controls into candidates. |
| **Short-term reversal / statistical arbitrage** | Usually compensation for liquidity provision; published strength rises in market stress. Exact residual versions need midpoint/factor inputs and both long/short legs. | Medium for a bounded 30m-4h on-demand spike, low for deep walk-forward and historical spread/shortability. Current daily RSI and residual-confluence implementations failed or are controls. | Low: short/carry plus roughly 50 bps round-trip economics consume the daily edge. | **Defer from the first trial budget, not for lack of any intraday history.** A future fixed replication may bootstrap the latest 1,000 REST bars, but promotion still needs prospective quotes/costs and a deeper untouched interval. No new daily-OHLC reversal variants. |
| **Breakout / opening range / volatility compression** | May exploit delayed institutional flow in “stocks in play”; requires opening-range, relative-volume and gap selection together. ATR is risk scale, not alpha. | Medium for recent 30m-4h context but low for opening-range depth: 1m/5m REST reaches days and the prospective quote panel is tiny. | Low under present costs; published ORB assumptions are orders of magnitude cheaper than the repo’s broker estimate. | **Defer/rejection replication only.** On-demand REST can run a bounded recent falsification, but not a deep promotion backtest. Do not promote S-4 or a bare breakout; require sufficient spread observations and exact stock-in-play inputs. |
| **Earnings/filing continuation (PEAD)** | Delayed assimilation of genuinely surprising information. Classic PEAD has weakened in recent periods and analyst-expectation data is absent. | Medium for filings/XBRL, low for true surprise and announcement-time execution. The preregistered historical-SUE trial is already in the ledger. | Long/short economics differ. | **Low priority.** Keep the sealed result; do not search alternate surprise definitions. Filing text may justify a future independent trial after bodies and knowledge times exist. |
| **Pairs / market-neutral relative value** | Temporary divergence from a stable economic relation; portfolio construction, hedge error and borrow matter more than chart similarity. | Medium for daily prices, low for reliable PIT universe/corporate actions and execution costs. | Low because one leg is normally a CFD short. | **Defer** until shortability/carry history exists. Sector-relative context may improve other forecasts but is not itself an edge. |
| **Macro / sector rotation / defensive allocation** | Changes beta, duration or sector exposure in response to slow-moving public conditions. Useful primarily for risk and drawdown, while factor timing is difficult. | Medium/high: market/sector series exist; free ALFRED can preserve vintage macro observations, subject to licensing review per series. | Conditional on account-specific eligibility proving the selected ETF is underlying rather than CFD. | **Risk-context trial only after core accounting.** It must improve the implementable portfolio frontier after turnover, not merely predict a regime label. |
| **Options volatility/carry** | Variance and skew risk premia; materially different return source. | Blocked: no historical option chain or implied-volatility surface. | Blocked/region-dependent product support. | **Out of scope without a licensable free source and broker contract.** VIX alone cannot reconstruct an option strategy. |
| **Order-flow, market making and intraday liquidity** | Paid compensation for immediacy and inventory risk; requires trades, depth, side and latency. | Blocked: WebSocket/REST gives best prices but no reliable depth, aggressor side or historical book. | Retail API rate and latency are unsuitable for market making. | **Out of scope.** Do not proxy order-flow imbalance from OHLCV and call it the same signal. |
| **News, filing text and LLM features** | Convert new unstructured information into a timestamped structured event; an LLM is an extractor, not a price oracle. | Low today: `filing_documents` holds URLs, not bodies; free news is incomplete and licensing-sensitive. | Neutral once a validated low-turnover event exists. | **Future data spike, not a first-round return trial.** Any extracted feature competes with a simple event baseline. |
| **13F/crowding and FINRA short volume** | Slow ownership/crowding or forced-unwind context. 13F is delayed; daily short volume is flow, not short interest or borrow availability. | High for existing 13F and Reg SHO/FINRA-like data, but semantics limit actionability. | Low/medium as context; cannot prove an instrument can be borrowed. | **Context only.** No cloning strategy. Test only if a separately admitted mechanism requires it. |
| **Activist ownership (Schedule 13D)** | A new control stake can create a dated catalyst distinct from delayed 13F cloning; passive 13G is not the same hypothesis. | Medium: the outcome-free census found 895 modern initial 13Ds with basic 60-prior/20-later coverage, but historical security identity remains survivor-biased and filing times are mostly date-only. | Conditional long fit. | **C-4 historical falsification preregistered; no capital candidate.** One clean-chain, next-session-open, ten-session test is frozen. A pass only permits prospective shadowing; a failure is not tuned. |
| **Issuer capital events** | Buybacks, issuance/dilution, dividends and treasury changes alter supply or distribute cash; announcements, accounting realization and mechanical ex-date moves are different events. | Medium: XBRL buyback/treasury/share data and dividend events exist, but announcement knowledge time and completeness vary. | Conditional long fit; dividend capture must clear price adjustment and tax. | **Taxonomy only.** Split into one economically specific preregistration if a coverage census supports it; do not combine all capital events into a vote. |
| **Listing, index and calendar flows** | Index additions/deletions, Form 25/delistings, IPO lockups, month-end and tax-loss flows can force trading on known schedules. | Low/medium: prospective listing membership and Form 25 exist; historical index membership, lockup terms and a complete event history do not. | Product/side dependent. | **Reserved/data-gated.** Current Nasdaq daily files support prospective detection, not a survivorship-free historical claim. Seasonality requires an explicit mechanism and trial charge. |

The taxonomy intentionally excludes Fibonacci, generic support proximity and chart
pattern libraries: own placebo tests beat the named levels, so there is no basis for
spending another trial on them.

Published anchors include [Gu, Kelly and Xiu](https://academic.oup.com/rfs/article/33/5/2223/5758276)
on nonlinear interactions and the dominance of trend, liquidity and volatility
features; [Daniel and Moskowitz](https://www.nber.org/papers/w20439) on momentum
crashes; [Nagel](https://academic.oup.com/rfs/article-abstract/25/7/2005/1602153)
on reversal as liquidity provision; and
[Cohen, Malloy and Pomorski](https://www.nber.org/papers/w16454) on opportunistic
insiders. They supply priors and mechanisms, never eBull promotion evidence.

## 3. The initial research budget

The first programme originally admitted three alpha/risk-overlay hypotheses. C-1
and C-2 are now rejected. C-3 has now failed its point-in-time source gate, so
the first bounded candidate budget is closed with zero promotable alpha
candidates. See `2026-08-11-quality-momentum-feasibility-result.md`.
This is a budget, not a promise to produce a strategy.

### Foundation F-0 — core and no-trade comparator

This is not an alpha trial. Construct the portfolio result the operator would have
received from the eligible core ETF, cash and actual account currency over exactly
the same dates as an overlay. Include:

- dividends and distributions credited on the correct date;
- GBP/USD conversion known at each cash flow and valuation;
- instrument/product identity (underlying versus CFD);
- market spread, commission, tax, financing and withdrawal/account charges where
  applicable;
- cash yield if the broker actually pays it;
- total return, CPIH-relative real return, drawdown, expected shortfall, beta and
  exposure.

Until F-0 reconciles to broker statements, no claim to beat the S&P 500, inflation or
buy-and-hold is valid. Price-only SPY remains useful for market context but not for
the operator’s wealth comparison. This is not an unsourced vendor dependency:

- issue #2559 begins prospective reconciliation with one official aggregate
  account-equity observation per UTC day. The documented 12-month eToro balance
  history endpoint returned 403 for the configured demo account on 2026-08-11,
  so no historical broker NAV is invented; the working demo P&L endpoint seeds
  the ledger from the next portfolio sync;

- historical benchmark comparison through the frozen 2024 frontier uses the exact
  legacy comparator's adjusted close and keeps its identity separate; the main stock
  corpus's adjusted closes continue through 2026 but cannot be silently substituted
  for that comparator identity;
- prospective account return uses broker cash transactions, distributions, fills and
  valuations—the exact wealth actually received by this account;
- a recent external total-return benchmark remains a comparison gap. Until a legal
  free series passes review, eBull reports prospective account total return beside
  price-only market context and refuses an external recent excess-return claim.

Current implementation boundary, made explicit rather than inferred: the legacy
realised series remains available for audit, while the Strategies chart reads the
main portfolio's compact EOD position evidence and joins it to exact automated
ownership. It therefore shows daily realised plus historical open P&L without a
second quote, feature or per-position strategy store. Manual positions are excluded
structurally; missing marks/closes create a gap rather than zero. Pool-principal
changes are separately identified as external flows. This is still not total return:
distribution/cost reconciliation and the identity-safe recent benchmark remain open,
so both return and benchmark fields continue to refuse availability.

### Closed candidate C-1 — opportunistic insider purchase reproduction

The preregistered trial was opened once and rejected. Across 49 complete months, the
purchase-value-weighted long-opportunistic/short-routine spread was +1.192%/month,
but its 95% interval was [-1.452%, +4.215%]. The timing placebo did better; the
primary-minus-placebo estimate was -0.721%. Trailing 24 and 36 months were negative,
maximum drawdown was -54.84%, expected shortfall at 5% was -14.29%, equal weight was
negative, and disclosed-value weights reached 100% in one issuer.

Therefore `capital_candidate = false`. The causal source builder, sealed evaluator,
tests, preregistration and aggregate verdict are retained in git and the trial count.
No manifest row, strategy picker, bracket, resolver or allocation authority is
created. The opened interval may not be mined for a rescue variant. See
`2026-08-10-insider-purchase-result.md` and #2480.

### Closed candidate C-2 — frozen extreme-shock short

Do not tune the >=12% drop, five-session hold or 20% stop. The historical sample was
searched and the frozen portfolio test is now rejected.

The exact 8,049-trade stream reconciled to the earlier +156.81 bps event-day gross
mean and +49.41 bps after the adverse assumed cost. That statistic was not investable:
as many as 596 signals fired on one date and 1,210 overlapped. Across four declared
per-name caps with and without a 25% sector cap, capital-weighted trade return was
negative, total return ranged from -13.76% to -47.72%, and maximum drawdown ranged
from -40.68% to -69.05%. See `2026-08-11-extreme-shock-portfolio-result.md`.

The eight allocation arms are charged to the trial register, taking the C-2 family
floor to 109 and the repository-wide declared floor to 122. Missing broker cost,
shortability and point-in-time sector evidence can only weaken this result; it cannot
repair a negative capital-weighted outcome without selecting a new rule. Therefore
the planned prospective census is cancelled and no candidate/runtime state is
created.

The pre-result contract below remains as the audit record of what would have been
required to continue:

Before collecting outcomes:

1. Reconstruct and declare at least the roughly 100 known searches; mark the total as
   a conservative floor if exact recovery is impossible.
2. Run a portfolio simulation with concurrency, sector/factor concentration,
   gap-through stops, per-name caps and one-name -100% stress. A stop price is not a
   loss cap for a gapping short.
3. Prove the eToro API can report whether the short is currently available and return
   an order-specific opening/closing/overnight estimate. Missing or stale eligibility
   refuses the observation.
4. Record every firing prospectively, including rejected-unborrowable names, bid/ask,
   product type, financing schedule, event/halt state and the hypothetical result.
5. Judge calibration, capacity and net portfolio contribution only after a powered
   prospective sample. The searched +49 bps is forbidden as a planning effect size:
   use the independently declared minimum worthwhile net effect and prospective pilot
   variance. Apply the complete historical search count to selection-bias reporting
   and allocate error across every prospective validation in the new programme. Demo
   fills validate workflow but never replace the fee model.
6. After a fixed 60-session feasibility census, project time-to-power from eligible,
   independently clustered firings. Stop if it exceeds the 24-month detailed quote
   retention/relevance window; do not collect indefinitely.

Capital allocation removed the edge before broker feasibility could promote it, so
the family is closed. Do not search a neighbouring threshold to rescue it.

### Candidate C-3 — one low-turnover long-only factor tilt

**Disposition (2026-08-11): deferred before outcome access.** The #2537
full-population census found 7,709 archive series but only 5,269 identity maps,
zero archive CIKs, and point-in-time membership beginning only on 2026-08-10.
The operational SEC table's three-annual-accession retention leaves the
optimistic quality-input upper bound at 0.0%, 0.0%, 0.0%, 0.3%, 28.6%, 33.4%
and 31.3% of active archive series on the 2020–2026 June decision dates. The
free SEC bulk archive can repair retained accounting depth for identified
issuers; it cannot restore the absent dead-name price and historical membership
population. No factor return was inspected. Do not backtest C-3 on mapped
survivors or current membership and do not add it to the runtime catalogue.

The purpose is to ask whether a simple monthly or quarterly cross-sectional overlay
improves F-0, not to discover a daily entry trick. Freeze one specification from the
published families before outcome access:

- profitability/quality and 12-month momentum are the permitted independent inputs;
- size, price, dollar-volume, sector and volatility are eligibility/risk controls,
  not additional alpha votes;
- point-in-time SEC filing availability is mandatory;
- use value/liquidity-aware weights, volatility scaling, turnover and concentration
  constraints;
- compare the combination with each single-factor arm and the same core benchmark.
- regress net excess return and attribution against contemporaneous market, size,
  value, profitability, investment and momentum factors. The free
  [Kenneth French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library.html)
  is a candidate benchmark source subject to a licence/identity check. Loading a
  known factor premium is not strategy alpha.

This is one family trial with declared challenger arms, not a grid over lookbacks,
weights and cut-offs. If point-in-time fundamentals or historical membership cannot
support it, defer it rather than backfilling today’s universe into history. Its
admission rests on the independent published factor prior and low-turnover portfolio
role; the existing S-2 result is a contaminated harness observation and cannot select,
weight or power C-3.

### Preregistered falsification C-4 — initial Schedule 13D catalyst

The read-only #2582 census found 1,285 structured initial-13D accessions from
2024-12-18 through 2026-08-12, 1,160 mapped research-price series and 895 events
with 60 prior plus 20 later calendar days. All raw documents already contain Item 4;
no outcome was opened and no new raw store is required. This was enough to freeze
one exact historical falsification, not enough to call the candidate viable.

The family has an independent forced-participant mechanism: an active/control stake
becomes public. It also has an unusually strong falsifier. The SEC's 2023 rule
analysis found that, for one corporate-action subset, much of the reaction preceded
the filing. eBull may trade only after dissemination. Moreover, 1,162 of the 1,285
current rows are date-only, so their causal historical fill is no earlier than the
next regular-session open.

The frozen C-4 primary population is a clean first active chain with no earlier
active or passive filing and no same-timestamp ambiguity. It enters at the first
regular-session open strictly after the filing date, exits at the tenth session
close and charges 50 bps round trip. There is deliberately no historical TP/SL:
a capital bracket is a separately identified executable adaptation and cannot be
selected from the same opened outcomes. The contract also pins unfiltered 13D,
seeded same-instrument random-time and rule-stratified initial-13G challengers,
plus attribution fields that cannot gate the result. Exact matching buckets,
tie-breaking, challenger multiplicity and recent stability are part of the
machine contract. Item 4 is hashed but deliberately not classified in v1.

The 895-event source ceiling exists before chain and liquidity refusals. Against
the declared 1% minimum worthwhile effect and 10% planning standard deviation,
the independent-observation requirement is 785 before clustering. The result may
therefore be inconclusive even if its mean is positive; that is not permission to
relax the population. Historical data can only falsify or justify prospective
shadow collection and can never promote capital because its universe is
survivor-biased, its stock classification is current-only and it lacks
historical broker economics. Recent market context is pinned to the price-only
SPY series 7713 and is not a total-return benchmark. See
`2026-08-12-schedule-13d-source-census.md` and
`2026-08-12-schedule-13d-preregistration.md`.

The exact sector-residual replication in #2522 remains source-blocked. It does not
become C-4 by substituting current classifications, survivor projection or fixed
3/5/10-session exits for the published rule.

## 4. Research loops that cannot become parameter mining

Every candidate moves through the same loop. A loop can change a specification only
before its outcome is observed. After outcome access, a change creates a new trial,
new version and new untouched future interval.

Implementation note (2026-08-12, #2505): the result-to-historical-validation
transition now requires the compact immutable viability and same-path challenger
record defined in `2026-08-12-promotion-edge-evidence-contract.md`. A pinned
result ID or prose evidence reference alone can no longer cross that boundary.

| loop | question | output | failure action |
| --- | --- | --- | --- |
| 0. Mechanism | Who is forced, slow, constrained or compensated, and why should the payoff persist after discovery? | One-page hypothesis, published prior, falsifiers and family identity. | Reject ideas whose rationale is only a chart resemblance or indicator vote. |
| 1. Data | Was every input actually knowable at decision time, with adequate population, revisions, corporate actions and membership? | Coverage census, knowledge-time contract, lineage and storage estimate. | Repair a bounded source gap or defer. Never replace missing with zero/current membership. |
| 2. Economics | Can this account execute the side/product at the proposed time and survive adverse spread, slippage, borrow, financing, FX and taxes? | Broker eligibility/cost contract and break-even edge. | Reject for this broker when conservative cost exceeds plausible gross effect. |
| 3. Machinery | Does the evaluator recover known answers from synthetic paths and refuse ambiguity, stale data and unavailable fills? | Deterministic unit/property tests and synthetic controls. | Fix machinery without touching real outcomes. Reset contaminated results. |
| 4. Preregistration | Is the candidate fixed and is the complete search denominator declared? | Immutable hypothesis/config/data/holdout hashes and trial-ledger entry. | No measurement until complete. Unknown historical trials use a conservative floor, never 12 by convenience. |
| 5. Development | Does the mechanism show monotonic placement, economic magnitude and stability without relying on one date/name/regime? | Clustered diagnostics on development windows. | Reject the version. Diagnostics may motivate a new future trial but cannot edit this one. |
| 6. Sealed validation | Is conservative net expectancy and portfolio contribution positive on untouched recent data? | One signed result with favourable/adverse execution arms and all refusals. | Reject or remain inconclusive. Do not inspect another holdout for the same version. |
| 7. Prospective shadow | Do live frequency, availability, costs, calibration and outcomes match the validated distribution? | Durable firings and one terminal result each; compact daily aggregates for non-firings. | Disable/defer; diagnose data, execution, regime or decay before proposing a new model. |
| 8. Demo execution | Can the full allocation, risk, order, reconciliation, manual-position isolation and close lifecycle operate safely? | Broker-position reconciliation and implementation-shortfall report. | Fail closed. Demo success does not waive live cost evidence. |

This is the permitted refinement cycle. It allows us to learn while preventing a
failed backtest from being repeatedly sculpted into a winner.

## 5. Statistical contract

No universal trade count makes a study valid. Each preregistration computes a sample
requirement from an economic threshold fixed independently of the candidate's searched
point estimate and a variance estimate that matches its dependence and tail shape.

For candidate `j`:

```text
gross break-even_j = adverse spread + slippage + commission + carry + FX + tax
minimum net effect_j = return needed to improve F-0 after the mandate's risk penalty
planning SE target <= minimum net effect_j / (critical_value_j + z_(power))
```

- The expression is a planning approximation only for sufficiently regular,
  light-tailed outcomes. It is never the acceptance test. `critical_value_j` comes
  from the preregistered multiplicity and sampling model, not automatically a Normal
  quantile.
- Two ledgers have different jobs. The reconstructed historical register records the
  full discovery search and deflates claims carried from it. A separately frozen
  family-wise one-sided alpha budget covers every new sealed/prospective validation.
  Fresh data removes outcome reuse; it does not make the selected historical effect
  estimate unbiased. C-2's +49 bps is therefore never a prior or power input.
- Power is 80% unless the preregistration justifies higher. If the available recent
  independent dates cannot reach it inside the candidate's declared relevance and
  retention horizon, the answer is `data_infeasible`, not an unbounded “collect
  forward” or a relaxed interval.
- Errors cluster by decision date and, where appropriate, issuer. Overlapping holds
  are purged/embargoed or evaluated by a block method. Nominal trade count is never
  substituted for effective sample size.
- Report the full payoff distribution: net expectancy and confidence interval,
  profit factor, calibration/Brier score where probabilities are claimed, turnover,
  capacity, drawdown, expected shortfall, beta/factor/sector exposure, worst gap,
  losing streak and results by year/regime/liquidity tier.
- Nearby-parameter robustness is a falsification test after a fixed candidate, not a
  search for the best neighbour. A narrow isolated optimum rejects the premise.
- Random-entry, simpler single-input and core/no-trade challengers are mandatory.
- Admission uses a preregistered date-clustered/block or studentized bootstrap lower
  bound on the **portfolio** net return distribution, not a z interval on per-trade
  means. Heavy-tailed candidates additionally require resampled order/cost/missed-fill
  simulations and explicit jump scenarios. C-2 must survive one-name -100%, correlated
  gap and unavailable-borrow stresses within the mandate; an -87% historical stopped
  trade cannot be diluted into safety by its mean.
- The candidate passes only if the adverse-execution tail-aware lower confidence bound
  on net expectancy is positive **and** the mandate-level portfolio frontier improves.
  A positive mean alone cannot pass. A factor tilt must additionally show whether its
  return is explained by priced factor exposure; expected risk-premium exposure may be
  allocated deliberately but is never labelled alpha.

[Bailey et al.](https://papers.ssrn.com/sol3/Papers.cfm?abstract_id=2326253)
show why repeated selection over backtests overfits, and
[Novy-Marx](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2629935) shows
that multi-signal critical values can be several times conventional levels. The
repo’s experiment register must therefore include failed and abandoned searches.

## 6. Data changes, ordered by decision value

### Gate D-0 — audit before ingest

1. Reconstruct an experiment ledger from scripts, issues, commits and result
   identities. Replace the 12-trial promotion denominator with the conservative full
   count. Historical uncertainty stays visible. Preserve the immutable old rows, but
   make the data-layer promotion gate reject their superseded
   `trial_register_version`; an operator label is not invalidation.
2. Measure all current source frontiers and retention daily; current documentation
   still contains stale statements that intraday storage is empty.
3. Add a capability matrix per candidate: required field, source, knowledge time,
   first/last usable date, coverage, licence, retention and refusal code.

### Gate D-1 — broker economics and product identity

Capture only at a firing/preflight or changed eligibility state:

- underlying/CFD product type, long/short permission and available leverage;
- best bid/ask and quote age;
- broker estimated opening/closing cost;
- overnight/borrow schedule and hard-to-borrow/unavailable status;
- market session, halt and order-size constraints;
- submitted order, broker fill and reconciliation result.

The live portal verified on 2026-08-11 separates quotas: market data is a shared
120/minute lane, ordinary/default reads 60/minute, order writes a shared 20/minute
lane, and cost/eligibility each have a dedicated 20/minute limit. Eligibility accepts
a batch, so do not spend it one symbol at a time. A 6,700-name candle pull still has a
theoretical floor of about 56 minutes per interval and competes with other market-data
work; broad history is a scheduled harvest. The daily process narrows the universe
before high-frequency observation for cost, latency and storage reasons—not because
intraday REST is absent.

### Gate D-2 — return and ownership accounting

- Reconcile dividend/distribution cash into the strategy-owned portfolio and main
  portfolio without double counting.
- Preserve the current manual/automated ownership boundary; closing an automated
  trade manually creates a strategy-aware terminal reason, never a replacement
  position.
- Add causal FX and a recent total-return benchmark. Until a source passes licensing
  and identity review, calculate the account's own total return from broker cash/fill
  history, report recent eToro price return separately, and refuse external recent
  excess-return claims rather than splice.
- Preserve both fixed-principal and compounding mandates at allocation time; neither
  changes the strategy evidence.

### Gate D-3 — bounded evidence sources

1. **Do not expand Form 4 for rejected C-1.** Its 29-quarter transient source was
   sufficient to reject the frozen reproduction and was deliberately not loaded into
   application tables. A future Form 4 backfill requires a materially independent,
   preregistered mechanism and a new storage/coverage case; it cannot be justified as
   repair of C-1.
2. **Prospective universe membership** continues. Nasdaq’s current symbol directory
   and daily list can help detect listings, delistings and corporate actions, but
   current files must not be advertised as a historical constituent archive. See the
   [Nasdaq symbol directory](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs)
   and [daily-list description](https://nasdaqtrader.com/Trader.aspx?id=DailyListPD).
3. **On-demand intraday plus prospective quote/panels:** use the latest 1,000 REST
   bars for bounded preregistered spikes, acknowledging the interval-dependent reach
   and no date anchor. Retained panels keep the existing caps and build a deeper
   untouched corpus. They serve admitted candidates, not an all-ticker warehouse.
4. **Macro vintages** may use FRED/ALFRED only after per-series terms and revision
   semantics are recorded. The [official API](https://fred.stlouisfed.org/docs/api/fred/overview.html)
   supports FRED and archival ALFRED queries; it is context, not assumed alpha.
5. **FINRA short volume** requires no new ingest now: it is not short interest or
   borrow availability. The [official description](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data)
   is the semantic guard.

Do not ingest option chains, generic news firehoses, social sentiment, alternative
data or full order books until an admitted hypothesis proves those exact fields are
the sole blocker and a legal free source exists.

## 7. Storage and performance contract

The research platform keeps raw observations needed to reproduce decisions, not every
derived indicator or every “almost signal.”

| data | retention / shape |
| --- | --- |
| Daily adjusted price/fundamental/filing source | Existing durable corpus; add only source fields with a declared candidate consumer. |
| Intraday OHLCV | Existing bounded 30m/5m/1m partitions and panel caps; drop expired leaf partitions, no row deletes or per-indicator copies. |
| Quotes/cost/eligibility | Five-minute candidate-panel samples or candidate preflights; explicit missing rows; 24-month detail then compact coverage aggregates. |
| Fired/refused candidate | One immutable decision snapshot and one terminal outcome. Durable where it can affect evidence, allocation or audit. |
| Non-fired evaluations | Daily counts by reason; short bounded detail only for debugging. Never store every instrument × rule × heartbeat. |
| Backtest output | Aggregate result, folds, trial identity and compact diagnostics. Equity curves are generated from reproducible events unless a promoted portfolio needs a bounded display series. |
| Allocation | One decision batch with considered opportunities, current/target weights and reasons; no polling snapshots when nothing changes. |

Before any new high-volume relation, measure bytes per row and query plan, project
steady-state size including indexes/WAL, declare retention, and prove partition-drop
cleanup. The existing 1.5 GB retained intraday budget remains a hard cap unless a
separate storage review changes it; its source contract is
`docs/proposals/ta/2026-08-09-strategy-observation-storage.md` and the measured
retained-tier enforcement is recorded in
`docs/proposals/ta/2026-08-09-strategy-automation-control-plane.md`.

## 8. Build order and stop conditions

The order is deliberately different from “build more strategies.”

1. **Fail closed and repair research integrity:** keep the existing zero-candidate
   runtime gate; reject superseded trial-register versions in the data-layer promotion
   gate; keep all four controls non-promotable in API and UI; and remove
   arrival-order/newest-signal funding before any candidate can exist. Then reconstruct
   the historical trial ledger.
2. **F-0 portfolio truth:** total-return/FX/dividend/cost attribution and a reconciled
   core/cash baseline.
3. **Broker feasibility:** point-in-time product, shortability and cost preflight;
   prove demo/live semantic differences explicitly.
4. **C-1 closed:** retain its causal source/evaluator and failed aggregate verdict;
   no backfill, picker entry or neighbouring rescue trial.
5. **C-2 closed:** retain the frozen event extractor, portfolio simulator, trial
   charge and failed aggregate verdict; no prospective census or rescue search.
6. **C-3 deferred:** #2537 measured that point-in-time fundamentals, dead-name
   prices and historical membership do not make the factor trial honest. Retain
   prospective collection; do not preregister or backtest until the independent
   source contract in its result is met.
7. **Viability report:** compare every admitted candidate with F-0 and no trade.
   Select all independent passers; select none if none pass.
8. **Opportunity allocator:** implement #2525 only after at least one candidate passes
   or to allocate F-0/cash under a mandate. Arrival-order funding was already removed
   at step 1; prove permutation invariance here.
9. **Prospective shadow, then demo:** validate lifecycle and distribution. No elapsed
   time automatically promotes a stage.

Stop the programme and return to core/cash if any of these occurs:

- no candidate survives the declared first budget;
- required point-in-time or survivorship evidence cannot be obtained legally and for
  free;
- conservative broker costs exceed every plausible gross effect;
- live availability excludes the names producing historical performance;
- the portfolio improvement disappears under adverse execution or concentration
  constraints;
- prospective outcomes fall outside the preregistered calibration/decay bounds;
- the feasibility census projects time-to-power beyond the candidate's declared
  relevance/retention horizon (24 months for current quote evidence);
- broker/internal reconciliation, market data or ownership becomes stale.

Stopping active research is a valid successful outcome: it prevents a false edge from
consuming capital while preserving the core risk premium.

## 9. When the team can be comfortable

“Comfortable” cannot mean certain to make money. It means every surviving uncertainty
is named and bounded, and no engineering step relies on an untested return premise.
The plan is ready to proceed when:

- the opportunity taxonomy has an explicit disposition and no proposed candidate
  lives outside it;
- the trial ledger is conservatively complete enough that selection-bias corrections
  are not decorative;
- F-0 reconciles total return and account cash;
- each admitted candidate has a mechanism, exact data/broker contract, power
  calculation, immutable preregistration and stop rule;
- the first tests can return `reject` or `inconclusive` without triggering parameter
  search;
- storage projections and retention pass before ingestion;
- allocation cannot fund controls, missing inputs, negative conservative expectancy
  or arrival-order winners;
- the operator sees the pot, benchmark/real return, risk, open automated positions,
  TP/SL/timeout, attribution and material refusals—not every ticker evaluated.

The current state does **not** meet those conditions. It has enough evidence to close
C-1 and C-2 and defer C-3 before outcome access, but not enough to turn on autonomous
demo allocation. The first bounded candidate budget therefore ends at zero promotable
alpha candidates. The defensible sequence is core accounting, research-integrity and
prospective-source work while the mandate remains in F-0/cash; more technical-indicator
combinations are not the next opportunity.

## 10. External challenge incorporated

The independent prompt supplied by the operator correctly emphasises point-in-time
data, provider/broker abstraction, realistic fills, sealed validation, hard risk,
shadow mode, decay detection and permanent failed-research records. Those principles
are retained here.

It does not, by itself, identify an edge. Its ten-strategy minimum would create a
larger data-mining budget, and its generic provider/feature lists ignore this repo’s
measured broker costs, missing historical membership, incomplete recent total return,
tiny prospective intraday panel and existing failed tests. The useful contribution is
the research discipline and lifecycle architecture—not authorisation to implement
every listed family.

Finally, the base-rate hurdle is severe: S&P’s year-end 2025 scorecard reports that
79% of active US large-cap funds underperformed the S&P 500 in 2025. Fund fees,
mandates and closet indexing mean that statistic is **not** an eBull false-positive
rate. It is evidence that passive core is a serious competitor, not proof that eBull
cannot add value. See [SPIVA US](https://www.spglobal.com/spdji/en/spiva/article/spiva-us/).
