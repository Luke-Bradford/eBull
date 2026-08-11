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
| The four visible strategies are investable candidates | **refuted** | S-1 through S-4 are all `purpose = harness_validation`. The database has 196 result rows and zero `capital_candidate` rows. They test machinery; they must not be funded or presented as choices for capital. |
| Current stored results can authorise capital | **refuted** | All 196 rows are `survivor_only` and `carry_unmodelled = true`. None is promotion evidence. Recent S-2 rows show positive point estimates but a 0.036-0.041 Deflated Sharpe against the current, already incomplete trial register. |
| The declared multiple-testing denominator is complete | **refuted** | `trial_register.py` declares 12 trials. `2026-08-09-plan-of-attack.md` documents roughly 100 searched arms in one session alone, and later residual, event and intraday candidates add further searches. A DSR against 12 is an optimistic upper bound. It must not be used for promotion until the historical experiment ledger is reconstructed conservatively. |
| Recent intraday and quote evidence is ready for historical validation | **refuted** | `strategy_intraday_bars` has 9,013 rows over eight instruments; one-minute data covers two instruments. `strategy_quote_observations` has 376 observed samples from 2026-08-10 16:00-20:10 UTC. This is a collection/execution pilot, not a return sample. |
| Historical universe membership is available | **refuted** | All 12,695 `instrument_universe_membership` rows start on 2026-08-10. They provide correct prospective membership, but cannot remove historical survivorship bias. |
| Storing bounded observations will bloat the database now | **refuted for the current panel** | The database is already 63 GB, but all current intraday leaf partitions together are about 1.96 MB and quote observations are 192 KB. The storage contract is sound if the bounded panel, retention and no-derived-series rules remain enforced. Existing 3.5 GB `research_price_daily` and other historical relations—not this pilot—dominate. |
| eToro demo fills validate real trading costs | **refuted** | eToro states virtual trades carry no fees even though the virtual account mirrors features and live market conditions. Demo can test integration, timing, state and reconciliation; it cannot validate commissions, CFD spread charges, borrow, overnight financing or live slippage. Those require conservative modelling plus broker cost observations. |
| A long, unleveraged eToro stock is economically the same as a short | **refuted** | eToro states a non-leveraged long stock normally owns the underlying and receives dividends. Shorts and leveraged positions are CFDs. Current fees can include the market spread, a CFD opening/closing charge and overnight/borrow charges; product classification can vary and must be checked per proposed order. |
| The recent eToro benchmark is total return | **refuted** | The recent comparator snapshot contains price candles with no dividend-adjusted close. The older research corpus has split-and-dividend-adjusted `adj_close`, but stops in 2024. A 2026 portfolio claim cannot silently splice the two. |
| Daily price history alone should yield a high-certainty day-trade rule | **not supported** | Own tests show ordinary liquid-name ten-day drift of about 44 bps against an assumed 50 bps round trip. Published return-prediction work finds low individual-stock signal-to-noise and gains mainly from nonlinear interactions among a small set of trend, liquidity and volatility features, usually at portfolio scale—not deterministic calls. |
| ATR supplies the missing edge | **refuted** | ATR makes stop distance and risk comparable across instruments. It changes payoff shape; it does not create positive expectancy and can destroy it. |
| More confirming indicators necessarily create confidence | **refuted** | Correlated transformations of the same OHLCV path do not create independent evidence. The rejected residual-confluence candidate selected a less-bad population but failed at the action boundary: conservative 2025 expectancy was -1.332%, profit factor 0.66 and effective sample size 19. |
| The earlier overnight reversal is active alpha | **refuted** | After decision/fill alignment, the shared-print reversal was not executable. The surviving liquid-name effect was roughly 4-5 bps per day of overnight drift across deciles: the equity risk premium captured by holding, not by repeated trading. |
| A 75% win rate is an appropriate target | **refuted** | Win rate is not value. A 40% hit-rate system can be profitable with asymmetric payoffs; a 75% system can hide rare ruin. Admission is based on conservative after-cost expectancy, drawdown/tail, calibration and portfolio contribution. |

Primary broker references: [eToro fees](https://www.etoro.com/trading/fees/),
[eToro stocks and ownership](https://www.etoro.com/stocks/),
[eToro virtual account](https://www.etoro.com/trading/demo-account/), and
[eToro API rate limits](https://api-portal.etoro.com/getting-started/rate-limits).
The API currently documents 60 read requests and 20 execution/write requests per
minute. These are architecture constraints, not alpha.

## 2. The bounded opportunity universe

“Find all opportunities” cannot mean trying an unlimited indicator catalogue. It
means closing the taxonomy over economic mechanisms and instruments that this
account could legally, economically and reproducibly trade. A new idea must map to
one family below or justify a new mechanism before it touches outcomes.

Scores are qualitative priors: `high`, `medium`, `low`, or `blocked`. They decide
what deserves a preregistered test; they are not backtest results.

| family / return source | mechanism and published prior | current free-data fit | eToro fit | decision |
| --- | --- | --- | --- | --- |
| **Broad equity beta / core allocation** | Compensation for bearing market risk; own corrected long history measured about 6.3-6.6% annualised. This is the hurdle and no-alpha fallback. | High for historical total return to 2024; recent price return only. | High for unleveraged long stock/ETF, subject to exact product classification. | **Foundation, not an alpha trial.** Build correct total-return, FX, cash and dividend attribution before judging overlays. |
| **Low-turnover cross-sectional factors** | Value, profitability/quality, investment, momentum and low-risk have extensive published priors. They can diversify a core but may be risk premia rather than arbitrage. | Medium: daily adjusted prices and SEC fundamentals exist, but point-in-time availability, delisted membership and recent total-return gaps must be closed. | Medium/high for long legs; short legs inherit CFD/borrow problems. | **Admit one simple long-only quality-plus-momentum tilt only after the foundation gate.** Monthly/quarterly turnover; no parameter sweep. Not a day-trade claim. |
| **Opportunistic insider purchases** | A forced-information mechanism. Cohen, Malloy and Pomorski report routine trades as essentially uninformative and 82 bps/month abnormal return for opportunistic trades. | Medium/high: parser and 48,278 purchase rows exist, but 99% of usable history is 2023 onward and only 10% of purchasing insiders have three purchase-years. Filing acceptance time, not transaction date, must drive entry. | High for liquid unleveraged longs; naturally low turnover. | **Priority alpha investigation.** Backfill stable Form 4 XML to 2003, verify the exact published classification, then preregister one reproduction. Current positive own estimates are inconclusive and biased upward. |
| **Extreme price-shock continuation (short)** | A forced-flow/information-shock hypothesis. Own searched result for shorting a >=12% one-day drop for five bars with a 20% stop showed +49 bps/trade after a 30% annual borrow stress, `t=4.83`, but had an -87% worst gap and emerged from about 100 searches. | Medium for OHLCV and event context; historical shortability and exact carry are absent. | **Blocked:** shorts are CFDs; firing names are likely hard-to-borrow and may be unavailable. Demo has no fees. | **Promising lead, not evidence.** Freeze the discovered rule, reconstruct its trial count, simulate portfolio tails, capture point-in-time shortability/cost, and assess only on new prospective data. Never call 2020-2026 an untouched holdout. |
| **Time-series / cross-sectional momentum** | Persistent prior across assets; crash risk rises after market declines amid high volatility and rebounds. Volatility scaling is evidence-based for this family. | High for daily research; recent total-return and PIT universe remain blockers. | Long implementation feasible; short implementation conditional. | Existing S-1/S-2 remain controls. A **new**, low-turnover factor tilt may be part of the factor trial above; do not tune the controls into candidates. |
| **Short-term reversal / statistical arbitrage** | Usually compensation for liquidity provision; published strength rises in market stress. Exact residual versions need midpoint/factor inputs and both long/short legs. | Low for execution-grade history. Current daily RSI and residual-confluence implementations failed or are controls. | Low: short/carry plus roughly 50 bps round-trip economics consume the daily edge. | **Defer.** Prospective quote/intraday collection continues only as bounded evidence. No new daily-OHLC reversal variants in the first trial budget. |
| **Breakout / opening range / volatility compression** | May exploit delayed institutional flow in “stocks in play”; requires opening-range, relative-volume and gap selection together. ATR is risk scale, not alpha. | Low: prospective intraday panel is tiny; free REST history is shallow and rate-limited. | Low under present costs; published ORB assumptions are orders of magnitude cheaper than the repo’s broker estimate. | **Defer/rejection replication only.** Do not promote S-4 or a bare breakout. Revisit only after sufficient spread observations and exact stock-in-play inputs. |
| **Earnings/filing continuation (PEAD)** | Delayed assimilation of genuinely surprising information. Classic PEAD has weakened in recent periods and analyst-expectation data is absent. | Medium for filings/XBRL, low for true surprise and announcement-time execution. The preregistered historical-SUE trial is already in the ledger. | Long/short economics differ. | **Low priority.** Keep the sealed result; do not search alternate surprise definitions. Filing text may justify a future independent trial after bodies and knowledge times exist. |
| **Pairs / market-neutral relative value** | Temporary divergence from a stable economic relation; portfolio construction, hedge error and borrow matter more than chart similarity. | Medium for daily prices, low for reliable PIT universe/corporate actions and execution costs. | Low because one leg is normally a CFD short. | **Defer** until shortability/carry history exists. Sector-relative context may improve other forecasts but is not itself an edge. |
| **Macro / sector rotation / defensive allocation** | Changes beta, duration or sector exposure in response to slow-moving public conditions. Useful primarily for risk and drawdown, while factor timing is difficult. | Medium/high: market/sector series exist; free ALFRED can preserve vintage macro observations, subject to licensing review per series. | High through unleveraged ETFs where the account receives the underlying product. | **Risk-context trial only after core accounting.** It must improve the implementable portfolio frontier after turnover, not merely predict a regime label. |
| **Options volatility/carry** | Variance and skew risk premia; materially different return source. | Blocked: no historical option chain or implied-volatility surface. | Blocked/region-dependent product support. | **Out of scope without a licensable free source and broker contract.** VIX alone cannot reconstruct an option strategy. |
| **Order-flow, market making and intraday liquidity** | Paid compensation for immediacy and inventory risk; requires trades, depth, side and latency. | Blocked: WebSocket/REST gives best prices but no reliable depth, aggressor side or historical book. | Retail API rate and latency are unsuitable for market making. | **Out of scope.** Do not proxy order-flow imbalance from OHLCV and call it the same signal. |
| **News, filing text and LLM features** | Convert new unstructured information into a timestamped structured event; an LLM is an extractor, not a price oracle. | Low today: `filing_documents` holds URLs, not bodies; free news is incomplete and licensing-sensitive. | Neutral once a validated low-turnover event exists. | **Future data spike, not a first-round return trial.** Any extracted feature competes with a simple event baseline. |
| **13F/crowding and FINRA short volume** | Slow ownership/crowding or forced-unwind context. 13F is delayed; daily short volume is flow, not short interest or borrow availability. | High for existing 13F and Reg SHO/FINRA-like data, but semantics limit actionability. | Low/medium as context; cannot prove an instrument can be borrowed. | **Context only.** No cloning strategy. Test only if a separately admitted mechanism requires it. |

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

The first programme admits only three alpha/risk-overlay hypotheses. This is a
budget, not a promise to produce three strategies.

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
the operator’s wealth comparison.

### Candidate C-1 — opportunistic insider purchase reproduction

One preregistered rule, faithful to the published classification. Required before
outcomes:

1. Backfill Form 4 ownership XML to 2003 with filing acceptance timestamp, issuer,
   security class, insider identity, role, transaction code and amendment lineage.
2. Repair invalid transaction dates and measure parser/issuer coverage by year.
3. Verify from the paper whether insiders without the required historical pattern
   are opportunistic or unclassifiable; do not infer it.
4. Freeze code `P` purchases, role exclusions, aggregation, liquidity floor,
   next-executable fill, monthly holding/rebalance, benchmark adjustment, costs and
   missing-data policy.
5. Compare opportunistic purchases with routine purchases and a point-in-time
   matched random-entry control. Use 2022 onward as primary relevance, older periods
   as mechanism and stress evidence—not as a rescue for a recent failure.

This trial is rejected if the recent conservative lower confidence bound on net
expectancy is not positive, if the classification is not reproducible, or if results
are concentrated in illiquid/unavailable names.

### Candidate C-2 — frozen extreme-shock short, prospective confirmation

Do not tune the >=12% drop, five-session hold or 20% stop. The historical sample has
already been searched and is development evidence only.

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
   prospective sample. Demo fills validate workflow but never replace the fee model.

If shortability or adverse costs remove the edge, close the family for this broker.
Do not search a neighbouring threshold to rescue it.

### Candidate C-3 — one low-turnover long-only factor tilt

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

This is one family trial with declared challenger arms, not a grid over lookbacks,
weights and cut-offs. If point-in-time fundamentals or historical membership cannot
support it, defer it rather than backfilling today’s universe into history.

### Reserved—not yet admitted

The recent 3/5/10-session sector-residual work in #2522 is a diagnostic opportunity
map, not automatically C-4. It may be admitted only if it states a forced-participant
mechanism independent of the rejected daily residual-confluence model and can be
tested without reusing its contaminated 2024-2026 decisions. Otherwise the first
budget ends at C-3.

## 4. Research loops that cannot become parameter mining

Every candidate moves through the same loop. A loop can change a specification only
before its outcome is observed. After outcome access, a change creates a new trial,
new version and new untouched future interval.

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
requirement from its economic threshold and clustered variance.

For candidate `j`:

```text
gross break-even_j = adverse spread + slippage + commission + carry + FX + tax
minimum net effect_j = return needed to improve F-0 after the mandate's risk penalty
required clustered SE <= minimum net effect_j / (z_(1-alpha_j) + z_(power))
```

- Family-wise one-sided alpha is allocated across the admitted budget before
  measurement. The trial ledger, not the number of shipped strategies, determines
  the multiple-testing correction.
- Power is 80% unless the preregistration justifies higher. If the available recent
  independent dates cannot reach it, the answer is “inconclusive; collect forward,”
  not a relaxed confidence interval.
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
- The candidate passes only if the adverse-execution lower confidence bound on net
  expectancy is positive **and** the mandate-level portfolio frontier improves. A
  positive mean alone cannot pass.

[Bailey et al.](https://papers.ssrn.com/sol3/Papers.cfm?abstract_id=2326253)
show why repeated selection over backtests overfits, and
[Novy-Marx](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2629935) shows
that multi-signal critical values can be several times conventional levels. The
repo’s experiment register must therefore include failed and abandoned searches.

## 6. Data changes, ordered by decision value

### Gate D-0 — audit before ingest

1. Reconstruct an experiment ledger from scripts, issues, commits and result
   identities. Replace the 12-trial promotion denominator with the conservative full
   count. Historical uncertainty stays visible.
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

The eToro API’s 20-write/60-read per-minute limits mean a broad high-frequency scan
cannot depend on per-symbol polling at decision time. The daily process must narrow
the universe before intraday observation.

### Gate D-2 — return and ownership accounting

- Reconcile dividend/distribution cash into the strategy-owned portfolio and main
  portfolio without double counting.
- Preserve the current manual/automated ownership boundary; closing an automated
  trade manually creates a strategy-aware terminal reason, never a replacement
  position.
- Add causal FX and a recent total-return benchmark. Until a source passes licensing
  and identity review, report recent eToro price return separately rather than splice.
- Preserve both fixed-principal and compounding mandates at allocation time; neither
  changes the strategy evidence.

### Gate D-3 — bounded evidence sources

1. **SEC Form 4 backfill** is the first source expansion: official EDGAR submission
   history and stable XML are free, already parsed, and directly unblock C-1. The
   [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
   provide public submissions and XBRL endpoints.
2. **Prospective universe membership** continues. Nasdaq’s current symbol directory
   and daily list can help detect listings, delistings and corporate actions, but
   current files must not be advertised as a historical constituent archive. See the
   [Nasdaq symbol directory](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs)
   and [daily-list description](https://nasdaqtrader.com/Trader.aspx?id=DailyListPD).
3. **Prospective quote and intraday panels** retain the existing caps. They serve
   admitted candidates, not an all-ticker warehouse.
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
separate storage review changes it.

## 8. Build order and stop conditions

The order is deliberately different from “build more strategies.”

1. **Research-integrity repair:** reconstruct the trial ledger; mark current DSRs and
   all four controls non-promotable on the operator surface.
2. **F-0 portfolio truth:** total-return/FX/dividend/cost attribution and a reconciled
   core/cash baseline.
3. **Broker feasibility:** point-in-time product, shortability and cost preflight;
   prove demo/live semantic differences explicitly.
4. **C-1 data repair and preregistration:** Form 4 backfill, classification and one
   sealed test.
5. **C-2 prospective contract:** portfolio tail simulation plus prospective
   shortability/cost/outcome capture; no further historical threshold search.
6. **C-3 feasibility/preregistration:** only if point-in-time fundamentals and
   membership make the factor trial honest.
7. **Viability report:** compare every admitted candidate with F-0 and no trade.
   Select all independent passers; select none if none pass.
8. **Opportunity allocator:** implement #2525 only after at least one candidate passes
   or to allocate F-0/cash under a mandate. Replace newest-signal-first funding before
   any candidate can receive demo capital.
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

The current state does **not** meet those conditions. It has enough evidence to choose
the next bounded work, but not enough to turn on autonomous demo allocation. The most
defensible sequence is core accounting and research-integrity repair first,
opportunistic-insider reproduction second, and the frozen short-shock lead as a
prospective broker-feasibility experiment. More technical-indicator combinations are
not the next opportunity.

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
79% of active US large-cap funds underperformed the S&P 500 in 2025. That does not
prove eBull cannot add value; it makes passive core/no trade the required competitor
for every claim. See [SPIVA US](https://www.spglobal.com/spdji/en/spiva/article/spiva-us/).

