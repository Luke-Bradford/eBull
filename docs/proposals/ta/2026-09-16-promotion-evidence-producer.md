# Promotion-evidence producer (#3104)

Status: proposed 2026-09-16. #2505 shipped the refusal contract and the immutable
store; nothing produces a record. This settles #3104's clause 3, narrows clauses
1/2/4 to what is actually evidenced, scopes slice 1, and registers — rather than
hides — what each later slice must still settle.

⚠ Revised after Codex checkpoint 1 (37 findings). Three claims in the first draft
were false and are corrected in place below: where the ledger survives to, what
`PromotionEvidence`'s constructor enforces, and which candidate family s4 is.

## Source rule

The governing rule is **#2505's own contract document**,
`docs/proposals/ta/2026-08-12-promotion-edge-evidence-contract.md`, plus
`PromotionEvidence` / `evidence_refusals`
(`app/services/strategy_promotion_evidence.py`), which are its executable form.
Where that document fixes a quantity it is cited by sentence. Where it leaves a
sample estimator open, the published estimator is cited. Where neither exists,
the rule is fixed **by construction** and frozen in a version string.

⚠ Not an SEC/eToro data-source question. The skill that binds is
`.claude/skills/quant/strategy-evidence.md`, and it binds one thing here: the
decision metrics stay `expectancy_per_trade_pct` / `profit_factor` / deflated
Sharpe, which is what the contract already asks for.

## Clause 3 (attachment timing) — settled: at MEASURE time, carried to write time

Backtest per-trade returns are not retained. `strategy_results_store` is 98
columns of scalars and rule-version strings; dev's `information_schema` has no
per-trade backtest table (every `%trade%` table is the paper/live path). The
distribution the record summarises exists only inside `run_backtest`'s process.

⚠ **Correction to the first draft and to #3104's filing note: the ledger does
not survive to the row write.** `_measure_namespace`
(`app/services/backtest_run.py:1704`) consumes `_NamespaceBook` and returns an
aggregate `NamespaceMeasurement` (`:1801`); the book is discarded before
`_write_rows` ever runs. So "compute it at write time" is not implementable.

**The producer runs inside `_measure_namespace` and its output rides
`NamespaceMeasurement`**, exactly as `regime_cohorts`, `gross_returns` and
`termination_census` already do. That is the existing pattern for "a per-trade
fact that must outlive the book", and it needs no new plumbing concept.

Both gates are then fed from one object: the write-time gate reads the in-memory
`PromotionCandidate.promotion_evidence`, and the same record is persisted by
`store_promotion_evidence` in the result's own transaction for the
promotion-time gate to load.

## Clause 1 (measurement ownership)

The realised-trade ledger inside `_measure_namespace` is `_NamespaceBook`
(`backtest_run.py:1110`): `returns`, `entry_dates`, `exit_dates`, and
`regime_observations` — and `RegimeTradeObservation` carries `instrument_key`,
`signal_date`, `net_return_pct` and `regime`, one per realised trade, appended
in the same branch as the return (`:1495-1520`). Per-trade instrument identity
IS retained, contrary to #3104's filing note.

| field | source | slice |
| --- | --- | --- |
| `after_cost_expectancy_ci_low_pct`, `max_drawdown_pct` | already on `StrategyMetrics` | direct read |
| `expected_shortfall_5_pct`, `excluding_best_1_expectancy_pct` | `book.returns` | **1** |
| `outcome_count` / `profitable` / `losing` / `flat` | `book.returns` | **1** |
| `max_date_contribution_pct` | `book.entry_dates` | **1** |
| `max_name_contribution_pct` | `regime_observations.instrument_key` | **1** (was "no producer") |
| `max_concurrency` | realised intervals + open legs | **1** |
| `NamespaceMeasurement` field + assembly + `store_promotion_evidence` call | — | 2 |
| `max_sector_contribution_pct` | name key → sector | 3 |
| `target_first` / `stop_first` / `timeout` / `ambiguous_path_count` | the SELECTED close, see below | 3 |
| `recent_year_evidence`, `recent_year_stable` | year partition, see below | 4 |
| cost vector, `broker_eligible` | clause 1b | 5 |
| `risk_limits_*`, `probability_calibration_passed`, `path_diagnostics_complete` | verdicts | 6 |
| `capacity_usd` | ADV modelling | 7 |
| `challengers` (5 roles), `ev_buckets`, `outcome_contrasts` (8) | re-measurement harness | 8 |

### Three traps the later slices inherit, recorded now

- **The path counts are not `close_source` and not the resolver outcome
  either.** `CloseSource` is a mechanism vocabulary — `signal_pair | level |
  max_hold | calendar | ambiguous | series_termination`
  (`position_builder.py:79`) — and a `level` close may be a target or a stop.
  But the resolver's `ResolvedOutcome` does not describe the booked close
  either: position construction selects among competing close sources, and
  `_resolved_level_outcomes_for_arms` rewrites `ambiguous` into `tp_hit` /
  `sl_hit` before positions are built. Slice 3 must classify **the selected
  close**, on the realised population only, and must carry the PRE-projection
  ambiguity flag separately or `ambiguous_path_count` is unrecoverable.
- **The name key is not always an instrument id.** The survivorship-free path
  uses `-series_id` for a series admitted without a live link (#2721 step 3,
  `backtest_run.py:1540`), documented "IN-PASS ONLY; no negative key may reach a
  column typed as an instrument id". Name concentration is unaffected — identity
  is all it needs — but slice 3's sector join must state what it does with a
  negative key rather than silently dropping those names.
- **`promotion_evidence_missing` is a member of `STANDING_REFUSALS`**
  (`backtest_run.py:351`). `docs/review-prevention-log.md:4915` permits a
  standing member only where it is standing **by construction**. The moment
  slice 2 attaches evidence it stops being so, and `_expected_refusals` must
  predict it from the same input the gate reads — not from a second list.

### Clause 1b — the cost vector, and a circularity to declare away

`observed_cost_inputs` must equal `{spread, slippage, financing, fx,
broker_eligibility}` or `executable_cost_inputs_missing` refuses.

**Every slippage producer in this repo measures realised fills.** `rg -n
slippage app/ sql/` returns two shapes only: the live gate's
`(ee.average_price - s.fill_price) / s.fill_price` over executed orders filtered
to `d.mode='paper'` (`strategy_live_gate.py:403`, `app/api/strategies.py:3022`),
and the monitoring copy (`strategy_monitoring.py:244`). No quote-derived or
modelled slippage estimator exists.

Read strategy-scoped that is circular: slippage needs paper fills → paper needs
`paper_enabled` → needs `historical_validated` → needs slippage.

⚠ **Not settled here, and the first draft was wrong to settle it.** The draft
declared the vector a venue-level observation, citing the contract's *"their
source and observation/valid-through dates"*. That wording is equally true of a
realised-fill average, and the #2625 replay policy establishes **freshness**,
not aggregation scope — so the inference does not carry. What IS established:
the circularity is real, and slice 5 must break it by naming an actual cost
source rule. `app/services/cost_model.py` already documents session-filtered
spread calibration, the split-adjusted maximum-band rule and lane-specific
structural-zero financing/FX; that file is the starting point, and the answer is
partly a broker-source question rather than purely ours.

Slice 5 must also settle, and none of it is decided here: how one scalar vector
and one validity interval aggregate across many instruments and order sizes;
whether attaching an observed slippage requires **repricing** the backtest
(today's returns charge spread only, so a cost vector the returns were not
computed under describes a different strategy); and the clock conflict —
write-time `check_promotable` passes `result.identity.window_end` as `as_of`
while `promote_strategy` uses today, so one observation cannot satisfy both for
a historical window.

## Clause 2 (contract applicability) — narrowed

The v1 challenger vocabulary is declared for *"the current short-horizon
residual/shock candidate family"*, and a materially different mechanism *"must
declare an equivalently strong versioned challenger contract rather than fill
these roles with labels that do not apply"* (contract doc, line 76).

- The **core/cash sleeve** (#2833 S-A, #2834 ARM A) does not take this path:
  `strategy_core_executor.py:484` is a separate authority path. No contract
  version needed.
- ⚠ **Correction: the first draft claimed the surviving substrate is "s4/s8
  reversion" and that v1 therefore applies. Half of that is false.** The
  manifest ids are `s4-volatility-compression-breakout` and
  `s8-range-mean-reversion` — a breakout and a reversion. A breakout candidate
  has no obvious `raw_instrument_shock` or residual decomposition, which is
  exactly the "labels that do not apply" case the contract warns about.
- **So applicability is a per-candidate declaration made at registration, not a
  property of the producer.** The producer is mechanism-neutral for slices 1-7;
  slice 8 cannot be written until the first capital candidate is named, because
  the challenger set is a function of that candidate's mechanism.

## Clause 4 (acceptance) — the last slice, and what it must cover

The end-to-end test (registered `capital_candidate` → result write → evidence
attached → `historical_validated` → `forward_observation`) belongs to the final
slice; it cannot pass before every field has a producer. It must also exercise
the failure paths, not only the passing one: missing evidence, stale costs,
round-trip equality through the SHA-256 payload, the one-record-per-result
immutability trigger, and attachment to the exact arm.

⚠ Interim slices carry table tests over their own arithmetic and nothing else.
Do not invent a "partial evidence" mode. `PromotionEvidence` is frozen, and —
**correcting the first draft** — its constructor is *not* total: it permits
empty `challengers`, `ev_buckets`, `outcome_contrasts` and `recent_year_evidence`
and an incomplete `observed_cost_inputs`. Completeness is enforced by
`evidence_refusals`, which is a refusal surface and not a constructor. A partial
record is therefore constructible, which is precisely why one must not be built.

## Slice 1 — the ledger measurements (this PR)

A pure module. No DB, no gate change, no `PromotionCandidate` change, no caller
yet: slice 2 calls it from `_measure_namespace`. ⚠ `PromotionCandidate` is
deliberately untouched — a new field there requires a #2625 replay
classification and `tests/test_strategy_promotion_replay.py` fails on an
unclassified one.

### Inputs

Two populations, deliberately different sizes — the distinction `TradeReturns`
already documents (`strategy_statistics.py:84`, *"the trade-level metrics take
this object and the path metrics take the curve"*):

- **realised legs**, as parallel arrays: `net_return_pct`, `entry_fill_date`,
  `exit_bar_date`, `name_key`;
- **open legs** at the window end, as `(entry_fill_date, window_end)` intervals.

Validated at construction: equal lengths across the realised arrays, every
return finite, every `exit_bar_date >= entry_fill_date`. An empty realised
population is refused outright — every statistic below is undefined on it, and
an invented zero is the failure mode the contract's "missing is not zero" rule
names.

### Definitions, each with its rule

- **`expected_shortfall_5_pct`** — Acerbi & Tasche (2002), *On the coherence of
  expected shortfall*, JBF 26(7), §4's α-tail estimator **exactly**: with
  `k = floor(αn)` and the returns sorted ascending, the mean is
  `(sum of the k smallest + (αn - k) · x_(k+1)) / (αn)`. ⚠ `ceil(αn)` — the
  first draft's rule — is not that estimator and overstates the tail: at
  `n = 41` it averages three observations, i.e. 7.32% of the population, not 5%.
  For `n < 20`, `k = 0` and the formula reduces to the single worst return,
  which is the honest answer at that sample size.
- **`excluding_best_1_expectancy_pct`** — the mean of the returns at or below
  the 99th percentile, matching the only in-repo formulation of this exact
  statistic (`scripts/verify_2437_short_stops.py:163`,
  `arr[arr <= np.percentile(arr, 99)].mean()`). The contract's prose says
  *"excluding the best 1%"* twice, so the percentage reading governs the field
  name's ambiguous `_1`. ⚠ Material: the count reading and the percentage
  reading coincide only for `n <= 100`, and **576 of the 580 stored results
  carry `trade_count >= 100`** (`select count(*) filter (where trade_count >=
  100), count(*) from strategy_results_store` → 576, 580; min 59, max
  4,228,628). ⚠ A threshold trim is not a fixed-count trim: under ties it drops
  more or fewer than 1%, and the module reports how many it dropped so the
  difference is visible rather than assumed.
- **`max_date_contribution_pct` / `max_name_contribution_pct`** — the largest
  share of the realised outcome count falling on one entry-fill date, and on one
  name key. **Count share, not profit share**: the contract's worked example is
  *"36.1% of accepted 2025 trades enter on one date"*. ⚠ The example licenses the
  DATE reading directly; the name reading is the same construction applied to
  the other axis, declared here rather than inferred, and slice 6's risk policy
  must use these same denominators or its verdict describes a different
  quantity.
- **`max_concurrency`** — the largest number of positions held simultaneously,
  over realised **and** open legs. **Two ordering rules meet within a date and
  neither may swallow the other.** A leg closing on a date and a *different* leg
  opening on it are not concurrent: spec §3.5 rule 4, documented at
  `position_builder.py:661` — *"same-bar ordering is exit before entry"*. But a
  leg that opens and closes on ONE bar was held: `bars_held = 0` is legal
  because a tp/sl can be touched on the fill bar itself
  (`position_builder.Position.__post_init__`). ⚠ A single half-open rule — the
  first draft's — erases those legs, and an all-intraday population then reports
  `max_concurrency = 0`, which `PromotionEvidence` rejects outright. Caught at
  Codex checkpoint 2. The sweep therefore applies earlier-leg closes, then
  opens, then same-day closes. ⚠ Open legs are included because they are in
  exposure and on the equity curve; counting realised legs alone understates
  concurrency, which is the direction that flatters a candidate.
- **outcome counts** — `profitable` is `> 0`, `losing` is `< 0`, `flat` is
  exactly `0`; the three partition the realised population, as the constructor
  requires.

⚠ **A positive `expected_shortfall_5_pct` is reported as measured.** A
population whose worst 5% are profitable is a valid measurement, and this module
does not clamp it. `PromotionEvidence` then raises `ValueError` rather than
emitting a refusal — which is a contract gap, not a measurement one, and slice 2
must decide whether such a run aborts, stores no evidence, or the contract is
corrected. Recorded here so the decision is made deliberately.

### Deliberately NOT in slice 1: per-year evidence

The first draft put it here. It has the most unsettled semantics of any field
and none of it is measurement: the anchor date, whether partial and zero-trade
years count, what a cross-year hold belongs to (entry-year cohorts are not
yearly backtests — `strategy_recent_evidence.RECENT_EVIDENCE_WINDOWS` already
has a different convention), how a single-year window can ever satisfy
`recent_years_evaluated >= 2`, the per-year bootstrap seed, and the fact that a
percentile interval need not contain its own point estimate while
`RecentYearEvidence` refuses `ci_low > mean`. Slice 4, with its own source-rule
pass.

⚠ Related, and measured rather than assumed: `evidence_refusals` requires the
latest evidence year to be `>= as_of.year - 1`. On dev,
`max(window_end)` is **2026-07-08** for s1-s4 and **2024-09-27** for the other
seven strategies (`select strategy_id, max(window_end) from
strategy_results_store group by 1`). So the recency clause is satisfiable for
the four, and structurally unreachable at promotion-time `as_of` for the seven —
a fact about which rows can ever be pinned, not about this producer.

### Scale

`max(trade_count)` on the stored population is **4,228,628**. Slice 1's
statistics are one sort and two counting passes over float arrays, with the
concurrency sweep over `2n` boundary events held in arrays rather than per-trade
objects. ⚠ That bounds THIS module only — it says nothing about
`cluster_by_date` or the bootstrap, which allocate per trade and are slice 4's
problem.

## Open items registered against later slices

Not solved here, and listed so no slice inherits them silently: `worst_gap_pct`
has no producer and no definition anywhere (adverse-gap direction, reference
price and population all undefined — neither the worst return nor the
entry-to-exit return measures it); the eight outcome contrasts need per-trade
feature-score, execution-cost, entry-gap, liquidity and market-stress inputs the
book does not hold, so they must be aggregated before those inputs disappear;
`probability_calibration_passed` needs retained forecasts, realised labels, a
procedure and an acceptance criterion, and is not a policy boolean; the annual
risk verdict cannot be reconstructed from closed-trade year partitions; a
one-sided population (no winners or no losers) makes `OutcomeContrast`
unconstructible; the parent/evidence join (`outcome_count` against the row's
`trade_count`, the CI and drawdown against the row's own) is nowhere enforced;
and the measurement rule version is not carried in the stored payload, so the
estimator a stored record used is not identifiable from it.

## What this does not do

It does not promote anything, does not change a gate, and does not make any
current row promotable — all 580 stored results are
`purpose='harness_validation'` and are refused earlier
(`strategy_control_plane.py:591`). #3104's claim is a counterfactual and stays
one until a capital candidate is registered.

**Refs** #3104, #2505, #2500, #2437.
