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

## Slice 4 — the per-year evidence partition

Spec written 2026-09-16, after slices 1 and 2 shipped. ⚠ Revised after Codex
checkpoint 1 (29 findings). Four of the first draft's claims were false and are
corrected in place: where the year COUNT comes from, which date anchors it,
what an empty final year costs, and what `rolling-36m` spans.

### Source rule: the years partition THIS row's own legs

The choice was between an **entry-year partition of one run's ledger** and a
**cross-row join over the sibling `year-YYYY` result rows**, which are real
declared evaluations (`RECENT_EVIDENCE_WINDOWS`; 40 stored rows each for
`year-2022` / `year-2023` / `year-2024`).

Two things settle it, and neither is a preference:

1. **Attachment timing, already settled by slice 2.** The record is built inside
   `_measure_namespace`, in one run's process, in the result's own transaction.
   Sibling rows for other windows are not visible there and may not exist yet.
   A cross-row join is not implementable at the only point the evidence can be
   produced.
2. **`evidence_refusals` bounds the per-year populations by this row's own
   count** — `sum(item.observation_count ...) > evidence.outcome_count` refuses.
   ⚠ Codex is right that a cardinality bound does not by itself PROVE subset
   membership or disjointness; what it does establish is that the contract sizes
   the years against this record's population and not against an external one,
   which is the reading a cross-row join cannot satisfy except by accident.

⚠ The cross-row reading is not wrong about the repo, only about this field.
`strategy_operator_promotion.recent_evidence_refusals` already assembles across
the six declared windows and refuses a missing one. That is a different
denominator serving a different gate, and #2505's record does not replace it.
See the deadlock recorded at the end of this section, which is a consequence of
the two coexisting.

### How many years — FIVE, and the contract fixes it

⚠⚠ **The first draft read "three" off `RECENT_EVIDENCE_WINDOWS`' three
calendar-year entries and called it sourced. It was an invented extraction, and
it was also arithmetically wrong about its own corroboration:
`rolling-36m` is `2021-09-28 .. 2024-09-27`, which spans FOUR calendar years,
not three.** Caught at Codex checkpoint 1.

The executable contract fixes the number directly, in the place the first draft
failed to grep:

```python
if len(years) > 5:
    raise ValueError("recent year evidence is capped at five aggregate years")
```

(`strategy_promotion_evidence.py:289-290`; the floor is `evidence_refusals`'
`recent_years_evaluated < 2`.) **So the horizon is the trailing FIVE calendar
years** — the most the record can carry. Taking the cap rather than the floor is
also the conservative direction: `evidence_refusals` requires EVERY listed year
to have positive after-cost expectancy, so each extra year can only make the
gate harder to pass, never easier.

Measured first, because "every year in the window" was a live option until the
data ruled it out: window spans on the stored population run to **65 calendar
years** (`min(window_start)` 1962-01-02, `max(window_end)` 2026-07-08 — `select
strategy_id, min(window_start), max(window_end), max(extract(year from
window_end) - extract(year from window_start) + 1) from strategy_results_store
group by 1`). Sixty-five entries breach the cap by thirteen times over.

### What anchors the five — the row's own `window_end`, and NOT the date already plumbed

⚠⚠ **`_ledger_evidence` is currently handed `window_end=dates[-1]`, and that is
the wrong date for this purpose.** `dates[-1]` is the NAMESPACE METRIC AXIS end;
it is correct where it is used today (the mark bar for an open leg) and wrong as
a recency anchor, because `ResultIdentity.window_end` is the EVALUATION window
end. For `hold_out` they differ by trading-calendar slack; for `in_sample`,
`dates` is `corpus.in_sample_axis`, which ends at `HOLDOUT_BOUNDARY`
(2021-06-29) — years apart. Caught at Codex checkpoint 1.

The anchor is therefore passed explicitly as `corpus.window.end`, which IS
`ResultIdentity.window_end`: both result sites build the identity with
`evaluation_window=corpus.window` (`backtest_run.py:3789,3805`) and
`load_corpus` sets `evaluation_end=window.end` (`:1723`).

Anchoring on the row rather than on `date.today()` is what makes the record
self-consistent at the moment it is built: write-time `check_promotable` passes
`result.identity.window_end` as `as_of`, so
`recent_year_evidence[-1].year in {as_of.year - 1, as_of.year}` holds by
construction. ⚠ At promotion time `as_of` is `date.today()`, and a row whose
window ended 2024 fails that clause — already recorded above as a fact about
which rows can ever be pinned.

⚠ **Consequence for `in_sample`, stated rather than discovered later:** with an
anchor in 2026 the trailing five years are 2022-2026 and every in-sample leg
predates 2021-06-29, so the tuple is EMPTY. That is truthful — an in-sample row
has no recent evidence — and those rows already carry the hold-out refusals.

### The questions slice 1 parked, answered

- **Zero-trade years are unrepresentable, and that is the constructor's rule,
  not a choice.** `RecentYearEvidence.__post_init__` refuses
  `observation_count < 1`. So a year inside the five-year horizon with no legs
  contributes no entry.
  ⚠ **Correction to the first draft: this does NOT necessarily fail recency.**
  `evidence_refusals` refuses on `recent_year_evidence[-1].year < as_of.year - 1`,
  so an empty final year still passes when the year before it carries evidence.
  The first draft asserted the opposite. Caught at Codex checkpoint 1.
  ⚠ What the omission costs is a distinction, and the boundary is worth stating
  precisely. "Legs entered but nothing MEASURABLE" is kept, with null bootstrap
  fields — that case is handled below and is the common one. Omitted are a year
  with no legs at all AND a year whose legs are all still open at the window
  end; the second is rare, and "no realised outcome" is the honest reading of
  it, but it is an omission and not a null.
- **Partial years are included, as measured.** `evidence_refusals` admits
  `[-1].year == as_of.year`, which for a window ending mid-year (s1-s4 end
  2026-07-08) is only ever a partial year. ⚠ This is the contract permitting a
  partial year, not a coverage rule: nothing in it requires a minimum span, so
  two thin periods either side of New Year satisfy "two years evaluated".
  The producer therefore carries each year's **active entry-date count**
  (the bootstrap's own `cluster_count`) so thin coverage is visible in the
  record rather than inferred from a population count. Whether to refuse on it
  is a gate question and is registered, not decided here.
- **A cross-year hold belongs to its ENTRY year — by construction, with the
  reason.** ⚠ Codex is right that this does not FOLLOW from the clustering; it
  is a policy fixed here and frozen in the rule version. The reason is
  commensurability: `strategy_statistics` computes the parent row's own
  `after_cost_expectancy_ci_low_pct` from `cluster_by_date(net_returns,
  trades.entry_fill_date)`, and slice 1's date concentration uses the same key.
  A partition on the exit date would put the per-year interval on a different
  axis from the parent figure the contract prints beside it.
  ⚠⚠ **The property this buys and the one it costs, both stated:** an entry-year
  cohort is not calendar-year performance. A leg entered in 2024 and closed in
  2026 contributes its whole outcome to 2024, so a year's expectancy can be
  realised by later-year prices, cohorts overlap economically, and a window with
  2026 realisations can show no 2026 entries. Exit-year attribution reverses
  exactly these and breaks the commensurability above; entry-year is chosen and
  the cost is recorded.
  ⚠ Divergence recorded rather than silently inherited: `RegimeCohort` clusters
  on `signal_date`, one step earlier. Regimes are classified on the information
  the strategy consumed; this field is compared against a fill-date statistic.
- **Censoring is reported per year.** Legs open at the window end are absent from
  the realised population, and they concentrate in the most recent entry years —
  the ones this field is about. `RealisedLedger.open_legs` already carries their
  entry dates, so each year also carries its **open-leg count**. Without it the
  most censored year is the one the gate leans on hardest, invisibly.
- **Per-year bootstrap seed — the repo's existing derivation, reused verbatim.**
  `strategy_regime_evidence._seed` is
  `int.from_bytes(sha256(f"{root_seed}:{label}").digest()[:4], "big")`, and
  `strategy_cohort_report._seed_for` is the same construction. Used with the year
  as the label, off the same `BACKTEST_BOOTSTRAP_SEED` root `_measure_namespace`
  already passes to `build_regime_cohorts`. Nothing new is minted.
- **`ci_low > point estimate` is reported, not repaired.** A percentile interval
  need not contain its own statistic (Efron & Tibshirani ch. 13; the estimator is
  first-order accurate only, as `block_bootstrap`'s header states) and
  `RecentYearEvidence` refuses that pair. Slice 1's precedent governs: the
  producer emits a measurement object, NOT a `RecentYearEvidence`, so an
  unrepresentable year cannot abort a backtest run.
- **A single-year window can never reach `recent_years_evaluated >= 2`.**
  Confirmed on the corpus: the `year-2022` / `year-2023` / `year-2024` rows span
  one calendar year each (120 of 580 rows), so four of their five anchored years
  are empty. Those rows are permanently unpromotable on this clause.

### What slice 4 does NOT produce

- **`risk_limits_passed` (per year).** The contract's risk limits cover
  *"drawdown, tails and concentration"* and drawdown is a PATH statistic.
  `strategy_regime_evidence`'s own header settles why a closed-trade partition
  cannot supply it — *"Portfolio path statistics (especially drawdown) remain on
  the parent, because filtering closed trades cannot reconstruct overlapping
  marked paths."* ⚠ **The first draft implied the two inputs below would let
  slice 6 form the verdict. They do not** — annual drawdown, sector
  concentration, gaps and exposure are all still missing, and slice 6 must
  either take a per-year pass over the equity curve or the contract's annual
  risk boolean must be re-scoped to what a trade partition can certify.
  Registered, not solved.
  ⚠ Related and unresolved: a calendar-year drawdown includes positions carried
  in from an earlier year, while an entry-year expectancy includes trades
  closing in a later one. Nothing says which population the single annual
  boolean certifies.
- **`recent_year_stable`.** A declared boolean with no producer specified
  anywhere. `evidence_refusals` independently re-checks every year's expectancy,
  so the boolean adds a stability claim the per-year intervals do not imply.
  Slice 6, with the other verdicts.

What slice 4 DOES carry beyond the contract's five per-year fields is the two
risk inputs the partition yields for free — the year's α-tail mean and its
date/name concentration shares, using slice 1's own estimators. ⚠ Carried NOW
for the reason slice 2 exists: the book dies inside `_measure_namespace`, so a
per-year quantity not measured here is not measurable later at any price short
of re-running the backtest.

### Shape

`measure_ledger` gains two required keywords, `root_seed` and `anchor_year`, and
`LedgerMeasurements` gains `recent_years: tuple[RecentYearMeasurement, ...]`.
One call site (`_ledger_evidence`), one object, no new plumbing concept — the
same reuse argument that put slice 1's output on `NamespaceMeasurement`.
⚠ `LEDGER_MEASUREMENT_RULE_VERSION` bumps to `...-v2`: the module's output
changes shape and a stored record must stay attributable to the rule that
produced it.

`RecentYearMeasurement`, per kept year:

| field | always present |
| --- | --- |
| `year`, `observation_count`, `open_leg_count` | yes |
| `after_cost_expectancy_pct` | yes — pooled `sum/count` |
| `expected_shortfall_5_pct`, `max_date_contribution_pct`, `max_name_contribution_pct` | yes |
| `expectancy_ci_low_pct`, `expectancy_ci_high_pct`, `effective_sample_size`, `bootstrap_seed`, `bootstrap_block_length`, `bootstrap_cluster_count`, `bootstrap_design_effect` | all-or-none |

The last group mirrors `RegimeCohort`'s own all-present-or-all-absent rule.
`block_bootstrap_expectancy` returns `None` in three real states (one cluster
date, zero trade variance, zero bootstrap variance) and criterion 3 forbids
substituting a nominal figure for a measurement that could not be made.
⚠ `after_cost_expectancy_pct` is NOT in that group — the first draft had it
there, contradicting its own fallback. It is the bootstrap's `point_estimate_pct`
when the bootstrap ran (same ratio, taken from the object that computed the
interval so the pair cannot disagree by a rounding step) and the pooled mean
otherwise. `resamples` and the model id are module constants and are cited
rather than copied per year.

⚠⚠ **A year whose bootstrap returned `None` is KEPT, with nulls — never
dropped.** Dropping it would delete a year with adverse expectancy whose
interval merely could not be computed, and the remaining years would then read
as stable. That is the concealment the contract's "missing is not zero" rule
exists to prevent, and the census below shows it is the common case, not a
corner.

Invariants enforced on construction: `observation_count >= 1`; counts
non-negative; every reported percentage finite; `ci_low <= ci_high` when both
are present; `effective_sample_size > 0`; `design_effect > 0`; concentration
shares inside `(0, 100]`; the bootstrap group all-present-or-all-absent; and, on
the collection, years unique and strictly ascending with
`sum(observation_count) <= outcome_count`. ⚠ The year's own `>= 2000` bound is
NOT enforced here — it is `RecentYearEvidence`'s, and duplicating a contract
check in the producer is how the two drift apart. Unreachable on today's corpus
(every `window_end` is 2024 or later, so the earliest anchored year is 2020).

### Full-population check — on the failure modes, via the one population that has them

The producer's input is a per-run book, and no stored artefact retains one, so a
census over the 580 stored results is not computable without re-running 580
backtests. What IS computable is a census of the SAME estimator on the SAME
books over a different partition axis: `strategy_result_regime_cohorts`, 720 real
cohorts written by real runs.

```sql
select count(*),
       count(*) filter (where effective_sample_size is null) as bootstrap_null,
       count(*) filter (where expectancy_ci_low_pct is not null
                          and expectancy_ci_low_pct > expectancy_pct) as ci_above_mean
from strategy_result_regime_cohorts;
-- 720, 104, 0
```

- **104 of 720 cohorts (14.4%) produced no bootstrap at all.** The
  unrepresentable-year case is the common case, not a corner, which is why the
  year is kept with nulls rather than dropped. Banding by `trade_count`
  (`width_bucket(trade_count, array[2,10,100,1000,10000])`) puts every one of
  them below 1,000 trades: 8/8, 32/38, 32/86, 32/184, then 0/218 and 0/186.
- **Zero cohorts have `ci_low > expectancy`.** The inverted-pair case is real in
  theory and unobserved on this corpus. Measured, not assumed — and the producer
  still reports it rather than repairing it.

⚠ What this does not cover: per-year cohorts are thinner than per-regime ones
(five buckets against at most five regimes, but concentrated in the recent tail
of the window), so the 14.4% is a lower bound on how often a YEAR will be
unrepresentable, not an estimate of it.

### Scale — measured, not asserted

Five bootstraps per namespace where there were none, plus one pass over the
realised legs to bucket them by entry year. Measured at the stored population's
maximum, 4,228,628 realised legs spread over a 2019-2026 date axis with a
recency-weighted entry distribution:

| | wall-clock |
| --- | --- |
| `_recent_years` alone, five years measured | **1.70s** |
| `measure_ledger` end to end (slice 1 + slice 4) | **5.6s** |

The five years held 473k-839k legs each on 244-366 active entry dates, and every
one produced a bootstrap. ⚠ The year pass extracts entry years with
`np.fromiter` and selects with a boolean mask rather than building per-year
Python buckets; a bucket loop is what makes this shape quadratic in practice.
Peak RSS is dominated by the 4.2M-element input tuples the caller already holds,
not by anything this adds.

### ⛔ Recorded, not fixed here: the operator-promotion matrix and this clause are mutually unsatisfiable

`strategy_operator_promotion.recent_evidence_refusals` demands all six declared
windows, `year-2022` / `year-2023` / `year-2024` among them, and
`promote_strategy` runs `evidence_refusals` against EVERY pinned `result_id`
(`strategy_control_plane.py:724-735`). A single-calendar-year row can never carry
two recent years, so once promotion evidence is required on every pinned row,
that route refuses permanently and a passing `primary-2022-plus` row cannot
rescue it.

This is a collision between two existing contracts, not something slice 4
introduces — but slice 4 is what makes it reachable, so it is recorded here and
on #3104 rather than left for the slice that trips over it. It needs either a
per-window evidence rule or a pinning rule that does not require the single-year
rows to be independently promotable.

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
