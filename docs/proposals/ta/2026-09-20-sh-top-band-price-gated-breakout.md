# S-H arm 2 — the cheapest-cost-band price-gated compression breakout (#2840)

Refs #2840, #2832, #2437, #3238. Depends on #2829 (the declaration freezes after this
lands).

Contract version: `sh-cheapest-band-price-gate-2026-09-20-v2`. ⚠ That string is what
the frozen declaration will carry in `PreregDeclaration.contract_version`, and it is
how the readout rule in §"Readout and abort bar" becomes part of the declaration. The
digest hashes the STRING, not this file's bytes (`prereg_contract.digest_payload`), so
once the freeze runs this document is append-only: a correction is a new contract
version and a new trial.

⚠⚠ **`-v2` is the amendment recorded in §"Statistical contract".** The original string
attested a pass bar that no longer exists — two point-estimate inequalities, replaced
on 2026-09-20 by a preregistered paired date-clustered block-bootstrap test. Nothing is
frozen yet, so this is an edit and not a supersession; the version moves anyway,
because a digest over the old string would attest a rule this document no longer
contains.

Arm 2 of #2840, per the ticket's 2026-08-22 addendum. Arm 1
(`s11-volatile-regime-gated-breakout`) is terminal — it FAILED its frozen bar on
2026-08-23 under declaration 11.

## What this ships

The strategy module, the census that sized it, and their tests. **Not** the
declaration and not a run — see §"Sequencing".

## The rule

    entry(t) := s4_entry(t)  and  close(t) >= CHEAPEST_BAND.lower

S-4's rule, bracket, ATR multiples and 40-bar hold cap are IMPORTED unchanged: the
gate conditions ENTRY, never the exit. S-4 is left byte-identical so its stored
results stay interpretable as the control, exactly as for S-11.

### Source rule

Two decisions here are data-treatment decisions, and neither is invented:

**1. That a price gate needs an as-traded level.** Settled in this repo by #2508 /
#2400 and enforced in `sql/305_strategy_as_traded_price_provenance.sql`: a decision
context is complete only when `as_traded_price_basis IN ('observed_unadjusted',
'reconstructed_unadjusted')`, with the column comment stating *"split-adjusted
research levels are not eligible for price/liquidity cohort attribution"*. This
strategy is a price cohort in the entry rule rather than in a report, so the same
invariant binds it.

**2. Which band table.** The repo holds two price-band vocabularies and they are not
interchangeable:

| table | edges | purpose |
| --- | --- | --- |
| `strategy_decision_context.price_band_for` | 5 / 20 / 50 / 150 | REPORTING cohorts for decision contexts (#2508) |
| `cost_model.BANDS` | 5 / 20 / 100 | the CHARGED p75 round-trip spread (calibrated 2026-08-07) |

The hypothesis is *"trade only where the cost model charges least"*, so the gate must
read the table that decides the charge. Using the reporting vocabulary would gate on
an edge that no leg is ever charged at.

**3. The threshold is never typed as a literal.** It is
`min(BANDS, key=p75_spread_pct).lower` — the CHEAPEST charged band, mirroring
`cost_model.UNKNOWN_NOMINAL_PRICE_BAND`'s own `max(BANDS, key=...)` idiom.

⚠⚠ **NOT `BANDS[-1]`.** That is guaranteed to be the highest-PRICED band, not the
cheapest-CHARGED one. They are the same band today (`>=$100`, 0.322% round trip
against 0.509 / 0.571 / 1.450) and the module asserts at import that they still are,
raising if a recalibration ever reorders the table or makes the cheapest band a
bounded interval — because a `>=` gate would then no longer express the hypothesis.
`.claude/CLAUDE.md` requires a published formulation or a rule fixed **by
construction and frozen in a version hash**; there is no published formulation for a
price-band entry filter, and this is the construction.

### Why `close(t)` and not the fill's `open(t+1)` — and the proxy's measured cost

The charged band keys on the entry FILL, which `signal_ledger.resolve_fills` puts at
the next bar's open. `close(t)` is what is knowable when the decision is taken, so it
is the causal choice and it inherits S-4's own next-open execution contract unchanged.

It is therefore a **proxy in both directions**, and both are measured rather than
asserted (`scripts/census_2840_price_gate.py`, 33,025,695 in-sample decision/fill
pairs):

| direction | pairs | share |
| --- | ---: | --- |
| admitted by the gate, charged a dearer band (`close >= 100`, next `open < 100`) | 6,815 | 0.67% of admitted |
| rejected by the gate though the fill was cheap (`close < 100`, next `open >= 100`) | 7,579 | 0.02% of rejected |

⚠ The successor is taken over the **unfiltered** series and classified afterwards.
Filtering first would delete an unusable successor and slide a later bar into the
fill slot, scoring a fill the resolver never makes — it takes `signal_index + 1` and
records `unusable_fill_price` when that bar cannot be filled on. Measured, the
correction moves nothing: this corpus has **0** decision bars whose immediate
successor is unusable, so both figures are identical either way. The query now
reports that count instead of assuming it.

So the gate admits downward overnight crossers and rejects upward ones, which is a
selection effect on the gap distribution and not only a cost leak. It is small and
near-symmetric in absolute count. The readout reports the realised charged-band mix
so the leak is re-measured on the legs that actually trade.

⚠ **This does not make the historical experiment point-in-time.** The band table is a
2026 snapshot applied to 1962-2021 prices — the strategy's own threshold is therefore
not a quantity any 1990 trader could have read. That is one of several reasons the
declaration is `falsification_only`; it is a limit on what a pass could mean, and it
does not affect what a fail means.

## The price basis this gate needs, and the three places the guard sits

A `>= $100` gate is a nominal-price gate only if the prices are as traded.

- **Backtest.** `BACKTEST_UNIVERSE` is `survivorship_free`, pinned by
  `universe_selection.vendor_for` to `icyDenev/Intrader`, whose declared
  `adjustment_basis` is `unadjusted` — measured, not assumed (that archive's
  docstring records AAPL 2020-08-27 reading 500.04 pre-split against the HF archive's
  125.01). #3238 made this the live cost basis at all four charge sites, so the gate
  and the band selection now read the same kind of number.
- **Live scan.** `SCAN_UNIVERSE` is `survivor_only` over `price_daily`, whose history
  the provider **back-adjusts at fetch time** (`market_data.py:751`). ⚠ The defence
  *"the gate reads only the latest bar, so back-adjustment cannot reach it"* is **not
  available**: this module is a callable that gates every bar it is handed, and a
  scan catching up across a split hands it pre-split bars on the post-split scale.
  So the strategy refuses that universe outright rather than relying on the scan
  being up to date — guard 1 below. The scan-universe identity is in any case a
  SEPARATE `strategy_version` needing its own declaration, and arm 2 reads
  `BACKTEST_UNIVERSE` only.

**Three guards, because no one of them is enough.** The first is in the rule itself:
`AS_TRADED_UNIVERSES` declares the universes whose closes this rule may read as
nominal, and `s12_signals` refuses EVERY bar on any other with
`not_evaluable / missing_market_context` rather than judging a price on a scale it
never traded at. ⚠ That refusal exists because S-12 is the first strategy in the
package with an ABSOLUTE threshold: every other rule compares prices to prices, so a
uniformly re-based series gives it the same verdict and back-adjustment is invisible
to it. A live scan catching up across a split would hand this rule pre-split bars on
the post-split scale, and scan rows are terminal.

⚠ The set is DECLARED, not derived from `vendor_for(universe)`. `survivor_only` is
excluded for a different reason than a reader would guess: on the scan path it is
not served by that universe's archive at all, but by `price_daily`, whose history
the provider back-adjusts at fetch time and whose #2066 split-cliff guard HEALS a
mixed series onto the back-adjusted basis (`market_data.py:751`). Deriving the set
would reach today's answer by the wrong route.

The other two guards sit outside the rule, because a declaration cannot see a RUN. A configuration test alone would pass while
the run it is supposed to protect charged every leg the maximum band, since
`load_corpus` derives the basis from `_resolve_liquidity_policy` on the actually
selected series and `cost_price_basis(None)` fails closed to `split_adjusted`
(`backtest_run.py:2199`):

2. **Config, at test time.** `cost_price_basis(archive_policy_for(vendor_for(
   BACKTEST_UNIVERSE)).adjustment_basis) == "as_traded"`. Reds if the backtest corpus
   ever moves to a split-adjusted archive.
3. **Run, at measurement time.** The step-3 measurement script asserts the RUN's
   `cost_price_basis` resolved to `as_traded` and refuses to open the outcomes
   otherwise. A run whose provenance went missing is charged the maximum band on every
   leg, which would test the gate against precisely the distortion the gate exists to
   exploit — the same trap #3238 was filed for.

None of the three validates the PAYLOAD: a stored `unadjusted` label on rescaled or
mixed-basis data would satisfy both. That is the corpus's own invariant to hold
(`sql/249` / `sql/251`), not this strategy's, and it is named so the coverage is not
overstated.

## Identity

`S12_PARAMS` carries S-4's seven parameters written out key by key (not merged —
S-11's reasoning), plus:

- `price_band_label` and `price_band_lower`, read from the cheapest band;
- `s4_source_hash`, recomputed from S-4's module path. Load-bearing for S-11's reason:
  this module IMPORTS S-4's rule, so an edit to S-4 changes what S-12 does, and
  without the hash S-12's own `_source_hash()` would not move.

⚠ The band hashing is **belt-and-braces, not the primary protection**. `cost_model`'s
own module rule is that a change to what is charged is a new `COST_MODEL_ID`, and
`COST_MODEL_ID` is hashed into every `StrategyIdentity` (criterion 11) — so a
compliant recalibration already rotates every strategy version. What the params add is
cover for a spread-only edit that moved a p75 without moving a label, and a statement
in the identity itself of which threshold this rule used.

## Measured premise — the gate is runnable, and its failure surface is arm 1's inverted

`scripts/census_2840_price_gate.py`, read-only, over the 17,285 admitted
`survivorship_free` series, in-sample `1962-01-02 .. < 2021-06-29`
(`HOLDOUT_BOUNDARY`, exclusive). BARS only — no strategy evaluated, no outcome read,
the same class of pre-declaration fact as arm 1's regime-day priors.

| quantity | all | `close >= 100` | share |
| --- | ---: | ---: | ---: |
| usable bars | 33,040,536 | 1,025,067 | 3.10% |
| distinct series | 14,841 | 1,672 | 11.27% |
| distinct dates | 15,124 | 14,443 | **95.50%** |

⚠ The series denominator is 14,841, not 17,285: **2,444 admitted series hold no usable
in-sample bar at all** and are absent from the census rather than counted as ungated.
"Usable" is `close > 0 AND open > 0` and neither non-finite — both legs, because the
gate reads the close and the fill reads the next open.

By decade the gated share is 21.6% (1960s, 6 series), 5.4%, 1.3%, 1.0%, 1.1%, 4.1%,
7.9% (2020s, 912 series) — thin through the 1980s-2000s, thickening after 2010. The
gate is therefore **not era-neutral**, which is one of the mechanism-attribution
problems the readout has to separate.

⚠⚠ **This is why arm 2 is not arm 1 wearing a different gate.** Arm 1's regime gate is
MARKET-WIDE: one regime date fans out across hundreds of names, so its 6,616 trades
were 104 decision dates and it died of date starvation. This gate is PER-NAME: 95.50%
of in-sample dates carry at least one gated bar while only 11.27% of series ever do.
Name-sparse, date-dense.

⚠ **These are BAR counts, not S-4 SIGNAL counts, and the distinction is not
cosmetic.** How many S-4 entries sit above the edge is unmeasured and stays unmeasured
until the declaration is frozen. Bar supply is an upper bound on signal supply and
nothing more: S-4 refuses a bar for warm-up, masked OHLC or a segmentation break, and
its own docstring records that one masked bar can refuse the remaining tail. The
claim made here is only that the gate is **runnable** — the corpus contains the
population — not that any particular cohort will be populated.

## Readout and abort bar

Frozen under this contract version, before any look.

**Selection.** `namespace = 'in_sample'`, pinned to the single `run_id` the
exploration writes, with the strategy versions, `COST_MODEL_ID`, universe basis and
corpus version asserted against the declaration rather than assumed. ⚠ `result_scope`
is ALWAYS `'sleeve'` — a module constant at `backtest_run.py:312`; filtering on it for
the in-sample/hold-out split returns 0 rows silently, which is how arm 1's first
cohort read came back empty.

**Run set: every runnable strategy — S-12, S-4, S-8 and S-11. This is not a choice.**
`run_backtest`'s `strategy_id` parameter is SINGULAR (`backtest_run.py:3915`), so an
invocation runs the whole runnable set or exactly one strategy. There is no
`{S-12, S-4, S-8}` option. Naming S-4 as "the control that must be in the set" and S-11
as "kept in to clear `MIN_MEASURED_TRIALS`" would describe a lever that does not exist;
both are simply present. Passing `strategy_id` to isolate S-12 would leave ONE measured
trial against `MIN_MEASURED_TRIALS = 2` and write rows permanently refused with
`deflated_sharpe_not_computed`.

⚠⚠ **The deflated Sharpe is refused TODAY, and clearing `MIN_MEASURED_TRIALS` is not
what fixes it.** `deflate_group` (`backtest_run.py:3417`) computes
`set(measured) - TRIAL_REGISTER.trial_ids` and returns `(None, reason)` for the
WHOLE GROUP if that difference is non-empty — one undeclared strategy nulls the
deflation for every strategy measured beside it. On `main` at `61ef6e47` that
difference is already `['s11-volatile-regime-gated-breakout']`: arm 1's register
entry declares S-11 via `declared_for`, but `deflate_group` keys on the STRATEGY ID
and the ten original strategies each have a trial whose `trial_id` IS their
strategy id. S-11 has no such entry, so the group has been refused since arm 1
landed, and S-12 adds a second name to the same refusal rather than causing it.

Consequences, stated so the readout cannot promise a metric it will not have:

- Step 2's register entry must be keyed on the **strategy id**, not only on
  `declared_for`, or step 3 writes rows with `deflated_sharpe` NULL — immutably.
- **S-11's missing entry is a pre-existing defect and is NOT fixed here.** Adding
  trials moves `TRIAL_REGISTER_VERSION`, which changes `M` and therefore the bar
  every frozen declaration was measured against. That is a declaration decision, not
  a drive-by edit inside a strategy PR.
- If step 3 runs before either entry exists, the readout reports
  `deflated_sharpe_not_computed`. ⚠ **Corrected by the `-v2` amendment**: it does NOT
  then "decide on `expectancy_per_trade_pct` and `profit_factor` alone". The decision is
  the eight-conjunct test in §"Statistical contract", of which the expectancy axis is
  two conjuncts of four per arm; profit factor decides nothing and is reported. The
  earlier sentence described arm 1's position and was left standing here by oversight.

**Pass bar — the eight-conjunct intersection-union test in §"Statistical contract".**
Superseded text, kept visible because a reader arriving from an earlier session will
look for it: the original bar was `expectancy_per_trade_pct > 0` for S-12 in both
quarantine arms **and** S-12's figure above S-4's in the same cell. That was two
inequalities on point estimates — refused, twice, as the thing underneath two failed
floor derivations. §"Statistical contract" is the replacement and is the only bar.

Anything else is a FAIL. No cell, arm or regime is selectable after the look — a
result carried by one cell is a fail, as it was for arm 1.

**Reported, and NOT part of the pass bar** (they characterise, they do not decide):

- **Per-regime cohorts** from `strategy_result_regime_cohorts`, never pooled, with
  `decision_date_count` beside `trade_count` on every row. Arm 1's headline figures
  were a 4× arm-pooled double count and its abort bar was a fan-out count until it was
  corrected pre-freeze.
- **GROSS expectancy beside net.** Nominal price selects names, eras and price
  trajectories as well as spread band, so a net improvement alone does not establish a
  COST mechanism. Gross separates "the cheap names were better names" from "the same
  edge survived a smaller charge".
- **Realised charged-band mix of the legs that traded**, per the proxy table above.
- **Mechanism leg: trade count and position count**, S-12 against S-4 on the same run.
  ⚠ Reported as counts, which are unambiguous. The equity-curve `turnover` statistic
  is reported beside them but not compared to the ~50%/month viability bar: arm 1
  recorded 1.267 against S-4's 20.122 with the units undecoded, and that is still
  true. Fewer trades is not automatically less capital turned over.
- **Deflated Sharpe**, computed by the harness against the register. No threshold is
  set here; inventing one is the constant this instruction set forbids.
- **CAGR, Sharpe, Sortino and win rate are banned as decision metrics**
  (`.claude/skills/quant/cost-aware-viability.md`).

**Leakage.** No purge or embargo is specified here because the harness already
implements one and specifying a second would be a competing rule:
`strategy_result.namespace_for_signal` gives a signal whose bar is in-sample but whose
FILL is on or after the boundary a **third verdict** — purged, belonging to neither
side — and an open position is always `hold_out` whatever side its entry fell on. The
straddle is handled, at the signal level, by §5.2's own rule.

⚠ Do not compare against the 16 stored `survivorship_free` results: every leg of them
was charged the maximum band and they are known-stale since `61ef6e47`.

## Statistical contract

Frozen under `sh-cheapest-band-price-gate-2026-09-20-v2`, before any look. Implemented
as a pure evaluator in `app/services/strategy_s12_paired_trial.py`, for the reason
`strategy_mt1_trial`'s own header gives: *"Keeping the statistic here allows the
implementation to be reviewed before the trial register and sealed-outcome gate are
opened."* Where a constant or a scheme choice is justified, the justification is in that
module beside the constant and is not duplicated here.

⚠⚠ **This section has itself been through Codex checkpoint 1 and was refused (54
findings), and what follows is the corrected version — still NOT frozen.** The refusal
is not recorded as history: the corrections it forced are the section, and the blockers
it found that remain open are named in §"What this does not resolve". A reader should
assume nothing here is settled until a checkpoint pass accepts it.

### Why the old bar had to go, and what the two earlier refusals were about

The original pass bar was two inequalities on point estimates. Two successive
forward-shadow floor derivations were put through Codex checkpoint 1 and refused (22 and
30 findings), and the second refusal located the cause above both attempts: the bar
carried **no justified error control**, so `docs/proposals/ta/
2026-08-11-portfolio-alpha-viability-plan.md` §5's planning expression — which presumes
an acceptance rule with an error rate — had no `critical_value` to draw on that the
contract itself supplied.

⚠ Narrower than the earlier wording, which said the inequalities "are not a test": they
do have statistics and rejection regions. What they lacked is justified error control.

§5 also rejected the old bar on its own terms: admission needs *"a preregistered
date-clustered/block or studentized bootstrap lower bound on the portfolio net return
distribution, not a z interval on per-trade means"*, and *"A positive mean alone cannot
pass."* Leg 1 was a positive mean.

### The estimands — two axes, because two documents require different units

1. **Portfolio.** Daily after-cost sleeve returns, `equity[t]/equity[t-1] - 1`, on the
   run's shared in-sample panel axis. This is §5's literal unit. The pairing needs no
   alignment step: `backtest_run._measure` sets `dates = corpus.in_sample_axis` for
   every strategy in one invocation. ⚠ The estimand starts at the FIRST AXIS DATE'S
   CLOSE — `equity[0]` is that close, so any date-zero activity sits inside the base
   rather than in a return. Declared, because the evaluator cannot verify date zero was
   inert.
2. **Per-trade expectancy, clustered by entry fill date.** The decision metric
   `.claude/skills/quant/cost-aware-viability.md` mandates. Cluster key and its reason
   are `block_bootstrap.cluster_by_date`'s, unchanged.

Both are required. Neither substitutes for the other, and choosing between them after
the look is not available.

⚠ The expectancy legs resample the **union** of the two strategies' cluster dates, not
the intersection. Intersecting would restrict S-4 to the dates S-12 traded and so
silently re-scope the control, whose estimand is its own full population.

⚠⚠ **Axis 2 is NOT the unit `ForwardShadowFloor` counts, and the two must not be
equated.** The live gate counts one strategy's distinct resolved SIGNAL-BAR dates
(`strategy_live_gate.py:392-395`); this axis is two strategies' union of ENTRY-FILL
dates, after trade selection and position supersession. A count in one is not a count in
the other, and the floor cannot be read off this axis without a conversion nobody has
derived.

### The test

A **paired circular block bootstrap** over each axis: one set of resampled block indices
per replication, applied to BOTH strategies, with the difference formed inside the
replication. That is `strategy_mt1_trial`'s construction (*"All four arms use identical
resampled month indices in every replication"*) on this trial's axes.

⚠⚠ **This is what makes blocker 1 not bind on the acceptance rule**, and the checkpoint
pass confirmed it independently: *"Pairing correctly removes any need for pre-look
covariance estimation for acceptance."* The binding leg compares `mean₁₂ − mean₄`, whose
variance needs S-12's dispersion and the covariance, neither of which exists before S-12
runs. The pairing does not estimate the covariance in advance; it removes the need to,
because the sampling distribution of the difference is produced directly at the look.
Borrowing S-4's marginal standard error, which a refused draft did, bounds the
difference's SE in neither direction.

Decision quantity, per axis: the **lower end of the two-sided 95% percentile interval**
(`block_bootstrap.CONFIDENCE`, imported so the two constructions cannot drift). ⚠ That
endpoint is a **nominal one-sided α = 0.025**, not 0.05, and *nominal* is the operative
word: a percentile bootstrap's actual tail error is approximate, BCa is not computed,
and no calibration evidence is offered. The threshold is **zero**, which is §5's own
acceptance clause (*"the … lower confidence bound on net expectancy is positive"*) and
both repo precedents' — `strategy_mt1_trial` tests `primary_interval.low > 0.0`.

### Derived, and declared — the split stated rather than blurred

The checkpoint pass refused the first cut of this section for presenting two DECLARED
choices as derivations: a block length "fixed by construction" at `MAX_HOLD_BARS`, and
an axis floor of 170 obtained by inverting `optimal_block_length`'s `b_max` tuning cap.
Both were the defect that killed the two earlier drafts, wearing a new costume.
`.claude/CLAUDE.md` permits a choice where no published formulation exists, on condition
it is *said* to be one and frozen in a version hash.

**DERIVED — published formulations, used as published.**

- **Block length** — `block_bootstrap.optimal_block_length`, Politis & White (2004) with
  the Patton, Politis & White (2009) `4/3` circular correction, measured off the series'
  own autocovariance. ⚠⚠ **The first cut's reason for pinning it instead was wrong twice
  over.** Freezing a SELECTOR freezes the algorithm, not its output — the same standing
  the bootstrap's own covariance estimate already has in this contract. And a fixed
  block does not merely "contain" the dependence: an ordinary block scheme attenuates
  lag-`k` covariance by roughly `1 − k/b`, so pinning `b` at the dependence length
  recovers about two thirds of the long-run variance and reports an interval NARROWER
  than the truth — an error in the PASSING direction. The hold cap was also never a
  bound on the dependence: S-4's 100-bar compression window, recursive ATR, persistent
  regimes and repeated issuers all outlive it.
- **Circular blocks** — Politis & Romano (1992), for `block_bootstrap`'s own stated
  reason on this axis shape.
- **Percentile interval** — Efron & Tibshirani (1993) ch. 13.
- **The pairing** — `strategy_mt1_trial`'s identical-index construction.
- **The conjunction** — an intersection-union test, Berger (1982), *Multiparameter
  hypothesis testing and acceptance sampling*, Technometrics 24(4):295-300: rejecting
  only when every component rejects at level α gives the combination size at most α,
  under no assumption about dependence between components. ⚠ Conditional on each
  component being a VALID level-α test — the IUT confers nothing on a miscalibrated one.

**DECLARED — choices, frozen in `TRIAL_EVALUATOR_VERSION`, not derivations.**

- One block length for both axes, measured on the portfolio paired-difference series.
  ⚠ The cluster axis holds ACTIVE dates only, so `b` clusters span **at least** `b`
  panel dates and usually more. The direction is stated because it is the one a reader
  assumes wrongly; it is not a claim that over-spanning is safe, and longer blocks are
  not automatically conservative.
- 10,000 replications, the frozen seed, the union cluster axis, and the first-close
  estimand start. Each is argued beside its constant in the module.

**No sample-size floor certifying 2.5%-tail coverage is derivable here, and none is
invented.** The only published floor available is `block_bootstrap.MIN_CLUSTERS = 2`,
and it is used as such — as a precondition on EACH strategy's own supporting dates, not
on the union, because a six-trade treatment against a dense control satisfies a union
check and supports nothing.

### Pass bar — eight conjuncts, all required

Per quarantine arm (`masked`, `admitted`) of the `worst_case` ambiguity arm, and per
axis (portfolio, expectancy):

1. the lower bound on **S-12's own** mean is > 0; **and**
2. the lower bound on the **paired difference** (S-12 − S-4) is > 0.

2 arms × 2 axes × 2 legs = **8 inequalities, every one required**. Leg 1 is not
redundant: S-12 can beat S-4 while losing money. Leg 2 is not redundant either: a gated
subset that merely inherits S-4's edge has demonstrated nothing about the gate.

⚠ Both arms are checked BY NAME against the contract's declared pair, not merely
counted — reporting a pass from one arm, or from two arms under other names, would be
selecting an experiment after the look.

### Relevance horizon and planning effect — both DECLARED, neither derived

- **Relevance horizon: 24 months.** ⚠ An earlier draft of this section cited this to §5.
  That was wrong: the figure appears at line 504 under **§8, "Build order and stop
  conditions"** (*"the feasibility census projects time-to-power beyond the candidate's
  declared relevance/retention horizon (24 months for current quote evidence)"*).
  Adopting a current-quote-evidence horizon for historical strategy-return evidence is a
  **choice this contract declares** because the gate's threshold is quote-calibrated
  (`cost_model.BANDS`, a p75 round-trip spread calibrated 2026-08-07). It is not an
  automatic inheritance. ⚠ Its start date and its recalibration/reset semantics are
  **not** specified here, and that is an open gap.
- **Planning effect: a declared standardised paired effect `d = 0.5`,** power 0.8. Cohen
  (1988) describes 0.2/0.5/0.8 explicitly as **conventions**, not as economic
  thresholds, and `strategy_mt1_trial` declares 0.5 for its own paired trial. So this is
  a convention adopted by declaration. ⚠⚠ It is **not** §5's minimum net effect, which
  §5 defines as improvement over F-0 after the mandate's risk penalty — a quantity this
  `falsification_only` trial has no capital context to compute.

⚠⚠ **Three corrections to the first cut's reasoning about that effect, all of which
reversed a claim.**

1. **"A standardised effect sidesteps the unobtainable variance" is the wrong way
   round.** An ABSOLUTE economic effect can be declared with no variance at all; the
   variance is what converts either form into a sample size. Standardising removes the
   scale, not the need for the dependence adjustment, the cluster-weight behaviour, or
   the conversion into decision dates and weeks.
2. **`d`'s denominator is undefined here.** Daily paired-difference SD, long-run SD,
   per-trade SD and cluster influence-function SD are four different quantities, and a
   difference of ratios over unequal trade populations has no automatic "paired
   observation SD". Until the denominator is named, `n = ceil(((z + z)/d)²) = 32` is
   arithmetic without a unit.
3. **One effect does not specify eight alternatives.** Own-return and difference legs, on
   two axes and two arms, each need their own alternative for power. A positive
   improvement says nothing about how far own expectancy sits from zero.

⚠ The mechanism's own measured cost saving (≈0.29 pp of charged round trip per fire) is
**not** used as the effect, and could not be: it is a difference of charge COEFFICIENTS,
not of net expectancies, and the two coincide only if the strategies' gross expectancies
agree — which is part of what the experiment is for.

### §5 obligations: which are met, which are scoped out, and which are simply GAPS

⚠ An earlier draft called the last group "exemptions". Two of them are not.

**Legitimately out of scope.** *"the mandate-level portfolio frontier improves"* and the
F-0 comparison are admission clauses for a **capital candidate**. This declaration is
`falsification_only`, allocates nothing and has no frontier to move. ⚠ That is a scope
limitation on what a PASS could mean; it does not make the rest of the rule
"§5-compliant" generally, and it does not remove §5's economic planning inputs.

**Open gaps, named as gaps.**

- *"Random-entry, simpler single-input and core/no-trade challengers are mandatory."*
  §5 says mandatory and they are ABSENT. The run set's fixity (`run_backtest`'s singular
  `strategy_id`) explains why one convenient invocation cannot add them; it does not
  prevent constructing separate aligned challenger books, and S-4/S-8/S-11 do not stand
  in for a random-entry or core/no-trade control.
- *"Heavy-tailed candidates additionally require resampled order/cost/missed-fill
  simulations and explicit jump scenarios."* Not satisfied. S-8's stored skew of 36 and
  kurtosis of 1,976 establish that this family is a concern; they do not establish
  S-12's own tail law, and deferring the capital-stress clause does not validate the
  percentile bounds now.
- *"Nearby-parameter robustness"*, issuer-dependence, and §5's full reporting set
  (capacity, expected shortfall, drawdown, worst gap, losing streak, exposures, results
  by year and liquidity tier) have **no stated mapping** onto what this harness already
  produces. Some of it exists in `strategy_result`; the mapping is not written down.
- ⚠ `worst_case` ambiguity is **not** adverse-execution robustness. Choosing the
  unfavourable OHLC touch order says nothing about cost uncertainty, missed fills,
  market impact or jumps.

### What this resolves, and what it does not

**Resolved.** The acceptance rule now has a construction whose error control is argued
rather than assumed; the binding leg's variance is not needed before the look, confirmed
independently at checkpoint 1; the multiplicity model is declared with a published zero
correction; and no coefficient arithmetic appears in the bar, so the charge-coefficient
confusion is gone. The evaluator enforces the declared arm set, the shared axis, cluster
membership on it, per-strategy support, and the marginal/difference distinction on a
degenerate bootstrap.

**Not resolved, and not papered over.**

1. **No power calculation, for any conjunct or for the conjunction.** An IUT's power is
   bounded above by its weakest component's — ⚠ bounded ABOVE, which is not the same as
   "the conjunction needs strictly more than one component": perfectly dependent
   rejection events make the two equal. ⚠ Nor is it true that no rule exists: the union
   bound gives `P(all pass) ≥ 1 − Σβⱼ`, so marginal powers of 0.975 would suffice for
   0.8 jointly. What blocks the calculation is the missing assumptions above — the
   undefined `d` denominator and the eight unspecified alternatives — not the absence of
   a formulation.
2. **"Zero correction" covers these eight conjuncts only.** It says nothing about the
   earlier strategy selection, the successive arms of #2840, repeated looks, or future
   validations. §5 separately requires historical-search accounting (the trial register)
   and a prospective family-wise alpha budget, and neither is discharged here.
3. ⚠⚠ **The prospective experiment the floor would power is still undefined — and this
   is now the binding blocker.** The historical bar requires BOTH quarantine arms and
   BOTH strategies' books; production carries only `masked` (`price_masked_bars`), and
   this `strategy_version` cannot run in the scan universe at all. So there is no
   forward instrument that computes this bar, and a floor cannot be sized for an
   experiment that has not been specified. That is a strictly sharper statement than the
   previous session's *"arm 2 may not be preregisterable"*, and it names what would fix
   it: specify the forward instrument first.
4. **A FAIL is not calibrated falsification.** Missing data, a refusal and a failure to
   reject all collapse into FAIL operationally. Without power that is a stopping policy,
   not evidence of absence — and symmetrically, "a PASS is strong evidence" overstates
   what historically-selected data can carry.
5. **The frozen artefact does not bind the whole evaluator.** `PreregDeclaration` hashes
   a contract STRING, not this document's bytes or the module's. The module hashes only
   itself while importing live `CONFIDENCE`; no expected evaluator digest is checked, and
   the corpus-version assertion promised in §"Readout and abort bar" names a field
   `PreregDeclaration` does not carry.
6. **Evidence identity is unrepresentable in the evaluator's inputs.** `PairedBooks`
   carries no strategy version, run id, namespace, ambiguity arm, cost basis, sizing
   rule or corpus version, so gross books, a `best_case` arm or two curves from
   different runs would all pass its structural checks. The wiring step owns those
   checks and must be written to own them explicitly.
7. **A sparse-axis refusal is partly draw luck.** A replication that selects no trade for
   one strategy is refused; whether one occurs depends on the seed and the replication
   count. The deterministic guard is the per-strategy support precondition, and the
   absence of an empty draw establishes nothing on its own.
8. **Nothing computes this statistic inside a run yet.** The evaluator is pure and
   reviewed; wiring it to a run's two books per quarantine arm is step-3 work.

## Step 2's forward-shadow floor — still NOT derived; its blocker is now amended away

`ForwardShadowFloor` takes no default and `sql/333` CHECKs both numbers `> 0`, so step
2 cannot proceed without deriving them. A first derivation was written on 2026-09-20,
put through Codex checkpoint 1, and **refused on two structural grounds**. It is not
recorded here, because a refuted formula left in a design document is how it gets
re-adopted by whoever reads it next. What that pass established is below, followed by a
SECOND derivation — route 1, a fresh artefact rather than a re-check — which was also
refused, and by what that refusal found underneath both.

### Settled — the floor's unit, read from the consumer rather than from precedent

`strategy_live_gate.py:392-395` computes `forward_decision_dates` as
`count(DISTINCT s.signal_bar_date)` over forward signals **that resolved with a
`gross_return_pct`** — deliberately not the looser population, *"an unresolved signal
is not evidence, so it is not a decision date either"*. `:700-706` compares that to
`min_independent_decision_dates`, and `forward_days` to `7 × min_calendar_weeks`.

So the unit is **distinct dates on which this strategy fired a signal that later
resolved**, and the floor's function is that same-day fan-out cannot inflate it
(`prereg_contract.py:127-132`). Nothing about the unit is open to choice, and the
previous session's worry that *"the floor's unit may not be decision dates at all"* is
answered: it is, unconditionally.

### Settled — `masked` is the production arm, and `admitted` is not

The live scan reads `price_masked_bars`, which carries ONE arm on purpose: *"criterion
9's `admitted` arm is a sensitivity measurement that has no place in a scan"*
(`price_masked_bars.py` module header). Any claim about what a FORWARD window would
supply therefore reads the masked figures. ⚠ The refused derivation asserted the
opposite — that forward reads nothing masked — and built its conversion rate on it.
That premise would have been frozen into an immutable row.

⚠ Relatedly, and narrower than it is tempting to write: masking cannot ADD S-12 fires,
because a masked field propagates to `not_evaluable` (`indicator_series.py:467` — ATR
terminates at the first missing input). So `N_masked ≤ N_admitted` — *no larger*, not
*smaller*, and it is a fact about S-12's inputs rather than about masking in general.

### Settled — why both previously-named candidate readout units are rejected

1. **The in-sample `bear_volatile` date count.** The rule contains no regime condition
   (`s12_signals` reads close, ATR, compression rank and prior high — no regime input),
   and the per-regime cohorts are `Reported, and NOT part of the pass bar`. So that
   cohort supplies no declared sizing target. ⚠ Note the narrow form: "the rule has no
   regime condition" is a code fact; "S-12 fires in every regime" would be a
   measurement, and is not claimed.
2. **The gated-name supply per date.** ⚠ NOT rejected because "a name is not a date" —
   a count divided by a count-per-date is exactly a valid conversion, and the first
   draft's reason was wrong. It is rejected because gated BARS are not fired, filled or
   resolved entries: the spec's own §"Measured premise" says bar supply is an upper
   bound on signal supply and nothing more, and the order-of-magnitude claim made
   against it rested on no measured signal count at all.

### Settled — no power calculation is available, and what one would need

`docs/review-prevention-log.md`'s #2614 entry requires checking the candidate's own
contract first. Arm 2's pass bar is a pair of **inequalities on mean expectancy** (not,
as the first draft called it, a sign test — there is no significance level, power
target or multiplicity model attached to it), with no declared magnitude.

⚠ "No power calculation exists" is therefore true of arm 2 SPECIFICALLY, and the first
draft over-generalised it to the precedents: #2582 uses planning assumptions and
`freeze_2437_mt1` a *declared* standardised effect of 0.5, neither of which is an
observed estimate with measured dispersion. The repo also carries a general
formulation — `docs/proposals/ta/2026-08-11-portfolio-alpha-viability-plan.md` §5,
*"planning SE target <= minimum net effect / (critical_value + z_power)"*, power 0.8
unless justified, and `data_infeasible` as the declared answer when the available
independent dates cannot reach it. Instantiating it for arm 2 needs three things the
contract does not yet contain: a **minimum net effect**, a **critical value from a
declared multiplicity and sampling model**, and a variance estimate matching the
dependence and tail shape. None may be invented. Route 1 below attempted to
supply all three from measurements and published formulations, and was refused — the
reason is that this section's own opening sentence understates the problem: arm 2 has no
power calculation because it has **no test**.

### Route 1 was attempted and REFUSED — the blocker is the PASS BAR, not the floor

A second derivation was written on 2026-09-20 instantiating §5, put through Codex
checkpoint 1, and **refused — 30 findings, verdict "I would not freeze this
derivation"**. That is two refusals in a row on the same number, which
`.claude/CLAUDE.md` names as the signal to question the MODEL rather than try a third
key. Doing so found one root cause underneath both.

⚠ The refused arithmetic is deliberately not reproduced here, for the reason draft 1's
was not: a refuted formula left in a design document is how it gets re-adopted.

**The root cause: arm 2's pass bar is not a test, so there is nothing to power.**
§5's planning expression — *"planning SE target <= minimum net effect / (critical_value
+ z_power)"* — presumes an acceptance rule with an error rate. §"Readout and abort bar"
declares two point-estimate inequalities and no significance level, rejection region or
test statistic. Any `critical_value` therefore has to be imported from a test the
contract does not contain, which is the same defect that killed draft 1 in a different
costume: a design choice wearing a derivation's clothes.

⚠⚠ **And the pass bar is not §5-compliant on its own terms.** §5's acceptance clause
requires *"a preregistered date-clustered/block or studentized bootstrap lower bound on
the portfolio net return distribution, not a z interval on per-trade means"*, and adds
that *"A positive mean alone cannot pass."* The pass bar's leg 1 is a positive mean. So
the document that supplies the floor's formulation also rejects the rule the floor would
be sizing. Route 1 could not have succeeded against this contract.

**Three further blockers, each independent of the first:**

1. **The binding leg's variance is not obtainable before the look.** Leg 2 compares
   S-12 against S-4, so the quantity to power is `Var(mean₁₂ − mean₄)` — which needs
   S-12's dispersion and the covariance between the two. S-12 has no stored result and
   must not acquire one before the freeze. The draft borrowed S-4's *marginal* standard
   error and inflated it; a marginal SE bounds the difference's SE in neither direction,
   because the covariance is unsigned. ⚠ Nor is S-12's trade set a subset of S-4's:
   the gate can admit an entry on a date S-4 is already holding a superseded position,
   so the two trade populations differ in composition as well as in size.
2. **The effect was a difference of charge COEFFICIENTS, not of net expectancies.** The
   drag on a round trip is `2h/(1+h)` of the gross MULTIPLE, so in return points it is
   `(1+g)·2h/(1+h)`. The difference of two average coefficients equals the difference of
   two net expectancies only if the two strategies' gross expectancies agree — which is
   part of what the experiment is meant to find out.
3. **Only two of the pass bar's four required inequalities were powered**, and only the
   `masked` arm was sized at all. A conjunction's power is a property of the whole
   conjunction; S-12 can beat S-4 while staying negative, and the `admitted` arm has
   neither its own variance nor its own requirement.

⚠ Two claims the refused draft made are also recorded as WRONG so they are not reused.
The staleness adjustment was labelled an exact bound: a per-trade SD scales by exactly
`(1-h)/(1+h)` only under a COMMON multiplier, and re-costing assigns different
multipliers to different trades, which adds band variance and a covariance with gross
returns. And the `data_infeasible` verdict compared the requirement against the whole
58.9-year archive — the archive-keying that killed draft 1, since adding older dates
moves that comparison without changing prospective feasibility. §5 asks whether *recent*
evidence can reach the requirement inside a declared relevance horizon, and arm 2
declares no such horizon either.

**What this left, what was done about it, and where the blocker sits now.** The floor was
blocked on a contract amendment, not on a measurement. That amendment is
§"Statistical contract" above, landed 2026-09-20 and still unfrozen: the pass bar is a
preregistered paired date-clustered block-bootstrap intersection-union test, the
relevance horizon is DECLARED at 24 months, and the planning effect is a DECLARED
standardised `d = 0.5`. ⚠ Read against §5, the pass THRESHOLD is zero and the effect
would size a forward requirement — a narrowing of the handoff's own wording, argued
there.

⚠⚠ **Blocker 1 splits in two, and only one half survives.** For the ACCEPTANCE RULE it
is gone, confirmed at checkpoint 1: identical resampled indices produce the difference's
sampling distribution at the look, so no pre-run covariance is needed. For POWER it
stands — and the reason is NOT that no formulation exists (the union bound supplies one:
`P(all pass) ≥ 1 − Σβⱼ`). It stands because `d`'s denominator is undefined for a
difference of ratios over unequal trade populations, and because eight conjuncts need
eight alternatives, of which one declared effect specifies at most one.

⚠⚠ **And the binding blocker has moved again, to something sharper than "arm 2 may not
be preregisterable".** A floor sizes a FORWARD instrument, and there is no forward
instrument here to size:

1. The eight-conjunct bar requires BOTH quarantine arms; production carries only
   `masked`, on purpose (`price_masked_bars`).
2. It requires BOTH strategies' books; a forward shadow accumulates one strategy's
   signals.
3. This `strategy_version` cannot run in `SCAN_UNIVERSE` at all — `s12_signals` refuses
   it — which the section below already records.
4. The evaluator's cluster axis counts two strategies' union of ENTRY-FILL dates, while
   `strategy_live_gate` counts one strategy's resolved SIGNAL-BAR dates. **They are not
   the same quantity**, so the amended bar does NOT hand the floor a readout unit, and
   an earlier draft of this paragraph claiming it did was wrong.

**So the next step is not a third floor derivation. It is to SPECIFY the forward
instrument** — which strategy identity, which universe, which arms, which statistic —
and only then to size it. Both family-B precedents could skip that because their forward
instrument was the same rule they had just run; arm 2's is not, and cannot be.

⚠ Nothing about that sequencing is operator-gated.

### What the attempt DID establish, and is worth keeping

The measurements survive the refusal — they are charged-band and supply facts, taken
outcome-free, and any future derivation needs them. They are in
`scripts/census_2840_s12_signal_supply.py`; nothing in this repo now computes a floor
from them.

⚠ **The gate buys down roughly 0.29 percentage points of charged round trip per fire**
(masked, in-sample: S-4 0.6153% against S-12 0.3225%). That is a real, measured
property of the mechanism and it is the right order of magnitude to reason about. It is
NOT an effect size, for blocker 2 above, and it must not be re-adopted as one.

⚠ **S-12 does not pay the cheapest band on every trade.** 251 of 44,842 fires (0.56%)
fill in `$20-100` and 2 in `$5-20`, because the gate reads `close(t)` and the charge
reads `open(t+1)`. The spec's §"Why `close(t)`" measured that leak at the BAR level;
this is the same leak on the legs that actually fire.

⚠ **S-12's per-date dispersion is HIGHER than S-4's despite calmer names, and the
direction is the surprise.** Its decision bars have a trailing-return SD of 1.2553%
against S-4's 3.2420% — but it fires on 9.12 names per date against 89.83, so it
averages far less within a date. Under within-date independence the per-date figure is
`sd / sqrt(names)`, which reverses the comparison: 0.387 per name becomes 1.215 per
date. Any future variance argument has to carry both terms.

### Why route 2 was not taken

Both family-B precedents size their floor from a quantity their contract declared
BEFORE the floor existed: arm 1 from the `bear_volatile` date supply its pass leg rests
on, #2837 from *"the 3 worst drawdowns"* its §9 readout names. **Arm 2's contract
declares no such quantity** — it has a pass bar but, unlike arm 1, no abort bar on
cohort `n`.

The first draft filled that gap with "reproduce the whole in-sample supply", and that
is the second ground it was refused on: the pass bar states no such minimum, so the
requirement is a design choice wearing a derivation's clothes — and it is keyed to an
accident of corpus length. A longer archive would RAISE the floor and a shorter one
would LOWER it, neither of which follows from the hypothesis.

⚠⚠ **And the obvious repair does not work, which is worth recording so it is not
re-proposed.** The natural response is "key the floor on the DECLARED in-sample window
rather than on the archive" — arm 1 keyed its 14 on the pinned `primary-2022-plus`, so
the analogue would be arm 2's in-sample span. It is not an analogue:
`strategy_result.py:117-125` states that `EVALUATION_WINDOW_START` **is measured, not
chosen** — *"It is the first bar the corpus holds for any instrument in the validated
universe … the window opens where the data does"*. So arm 2's in-sample window IS the
archive's extent, and a floor keyed to it is archive-keyed however it is phrased. The
whole family of "reproduce the exploration window" derivations inherits the defect.

**The question that left open:** what required count does arm 2's contract declare that a
forward confirmation must reach — i.e. what is arm 2's abort bar on `n`, the quantity
playing the role of arm 1's 14 dates and #2837's 3 drawdowns?

Route 1 answers it by **adding the missing declaration** rather than by reverse-engineering
one: §5's three inputs are now stated above, in this document, before the freeze. That is
the repair the prevention entry asked for — *"if the contract declares no such quantity,
say so and fix the CONTRACT"* — and it is available precisely because a declaration is
still editable until it is frozen.

**Route 2 — declare an abort bar on `n` the way arm 1 declared its 508** — was not taken,
and would not have been better. Arm 1's 508 was *"the largest independent cohort the lead
itself rests on, i.e. an upper bound on available evidence, explicitly NOT a power
calculation"*, and it could be read off cohort figures that existed only because S-11 had
already been run. Arm 2 is pre-look by construction: its lead is the addendum's re-pricing
of S-4's measured trades, a sensitivity over ALL trades that never publishes the
cheapest-band subset. The nearest available figure — S-12's 44,842 fires over 4,916 dates —
is a SUPPLY count, and sizing a requirement from the supply is the archive-keyed defect
above wearing one more costume.

### The measurement that exists, and the bounds it does NOT establish

`scripts/census_2840_s12_signal_supply.py` evaluates the merged rule over the in-sample
`survivorship_free` corpus and reports, per strategy and per quarantine arm, the fired
count, the distinct signal-bar dates, the span and the same-day concentration. It loads
no bar on or after `HOLDOUT_BOUNDARY`, reads no return and writes no row.

Measured over the full in-sample population — 14,260 series evaluated, 1962-08-17 ..
2021-06-25, 21,497 calendar days (58.9 years):

| | fired | distinct fire-dates | median fires/date | max fires/date |
| --- | ---: | ---: | ---: | ---: |
| S-12, `masked` (production arm) | 44,842 | 4,916 | 2 | 168 |
| S-12, `admitted` | 48,714 | 4,925 | 2 | 185 |
| S-4, `masked` | 1,043,493 | 11,616 | 30 | 1,691 |
| S-4, `admitted` | 1,087,285 | 11,619 | 31 | 1,762 |

`N_masked <= N_admitted` holds on both axes, as predicted.

⚠⚠ **This REFUTES the "date-dense" half of this document's own §"Measured premise".**
That section says *"95.50% of in-sample dates carry at least one gated bar while only
11.27% of series ever do. Name-sparse, date-dense."* Both figures are correct AS BAR
COUNTS and the section labels them so. At the SIGNAL level the gate is far from
date-neutral: it keeps **4.30%** of S-4's fires (44,842 of 1,043,493) but only **42.32%**
of S-4's fire-dates (4,916 of 11,616). So S-12 is name-sparse and **date-thinning**, not
date-dense, and the bar-level 95.50% overstates the date supply available to the rule by
more than a factor of two. This is exactly what §"Measured premise" warned about when it
said bar supply is an upper bound on signal supply *"and nothing more"* — the warning was
right and the characterising sentence beside it was not.

⚠ Fan-out is much lower than S-4's — median 2 fires per date against 30, and 1,797 of
S-12's 4,916 dates (36.6%) carry exactly ONE fire. That is the quantity the floor's
date denomination exists to protect against, and on this rule it is mild.

⚠ **A fired count is an UPPER bound on a trade count**, in both arms: an unusable fill
open, a `superseded_open_position` collapse, and `namespace_for_signal`'s purge of a
pre-boundary signal whose FILL crosses the boundary all sit between a fire and a costed
trade. Any future derivation must carry that direction rather than call a fire a trade.

⚠ **Two bound directions the first draft claimed are NOT established** and are recorded
here so they are not re-asserted: that a dates floor built on fires is a lower bound
(fires bound trades from ABOVE, which pushes that component the other way), and that a
weeks floor built on the corpus's own arrival rate is a lower bound (a historical
average is neither a maximum future rate nor a guaranteed waiting time, and this gate
is measurably era-dependent — 1.0% of bars in the 1990s against 7.9% in the 2020s).
Both directions remain unestablished, and the refused route-1 draft did not need them
— which is worth recording, because its successor might.

**The charged-band mix, and the second weighting** (masked, in-sample). ⚠ The last
column's difference is the charge the gate buys down. It is a measured property of the
mechanism and it is NOT an effect size — see §"Route 1 was attempted and REFUSED",
blocker 2:

| | fires | fill-date clusters | `>=$100` share | mean round trip |
| --- | ---: | ---: | ---: | ---: |
| S-12, every fire | 44,842 | 4,916 | 99.44% | 0.3225% |
| S-12, max-hold collapse | 8,579 | 2,699 | 97.94% | 0.3253% |
| S-4, every fire | 1,043,493 | 11,620 | 4.30% | 0.6153% |
| S-4, max-hold collapse | 242,610 | 10,170 | 3.29% | 0.6640% |

⚠ **The collapse arm's direction is checked, not asserted.** It greedily refuses every
fire through `i + MAX_HOLD_BARS - 1` after an accepted one, so it must over-collapse — a
real position exits at or before the cap. S-4's collapsed fill-date count is **10,170**
against the **10,691** clusters its stored bootstrap actually used, i.e. 4.9% fewer,
which is the predicted side. ⚠ It remains a second WEIGHTING and not a bracket: greedy
sets under different quarantine lengths are not nested, so the true trade-weighted mix
is not arithmetically trapped between the two rows. What the pair buys is that the mix
barely moves (0.3225 → 0.3253 and 0.6153 → 0.6640), so the charge difference is not an
artefact of the weighting.

⚠ **The `- 1` is load-bearing and was wrong in the first cut** (Codex ckpt-1, finding
20). `position_builder` supersedes on `entry.fill_bar_date < open_until`, strictly
before, and records the rule beside it: *"A closed position whose close date equals a
later entry's fill bar does NOT suppress it — rule 4, exit before entry."* The
one-bar error suppressed 2,235 S-4 and 93 S-12 fires and 10 / 9 fill dates.

⚠ **0 fires in either strategy or either arm had an unusable fill open, and 0 had no
successor bar** — so the fire→trade gap here is the collapse and the boundary purge
alone, not a pricing gap.

**The clock convention is still undeclared, and the candidate is named so the next
attempt does not re-derive it.** S-12's own realised in-sample arrival rate is its
21,497-day span over `4,916 - 1` intervals = 4.3738 days per decision date, which is the
convention both #2616 floors used. ⚠ It is a convention and not a bound: it excludes
leading and trailing silence, warm-up and resolution delay, and it is not the consumer's
own clock — `forward_days` is `(paper_at or observed_at) - forward_at`, a
stage-entry-to-stage-exit elapsed time (`strategy_live_gate.py:551`). Whichever is used
has to be declared explicitly.

### ⚠⚠ And a framing problem the floor inherits either way

**This `strategy_version` cannot accumulate forward decision dates at all.** The scan
runs `SCAN_UNIVERSE` = `survivor_only`, and `s12_signals` refuses every bar on any
universe outside `AS_TRADED_UNIVERSES` = `{survivorship_free}`. A forward shadow of
this hypothesis is a DIFFERENT identity with its own declaration — which this document
already says under §"The price basis this gate needs".

That does not make the floor pointless and it is not new: arm 1's freeze script records
the same structure (*"the confirmatory instrument is forward shadow, which runs under
`SCAN_UNIVERSE` — a different `strategy_version`, hence its own declaration, made when
that evidence exists"*). The floor states what a forward confirmation of THIS hypothesis
would require. It should be frozen saying so, rather than implying this row will ever
clear it.

### Edge contracts any derivation must state before it is frozen

`sql/333`'s `> 0` CHECKs establish none of these — they only stop the most obvious one
reaching the table. The refused route-1 draft implemented refusals for the first seven
and they went with it; they are listed because the next attempt needs the same set, plus
the three the refusal added:

| edge | why it must be refused rather than handled |
| --- | --- |
| a census run with `--limit` | a timing slice is not a population figure |
| a corpus that failed closed to the maximum band | the recorded bands would be the `UNKNOWN_NOMINAL_PRICE_BAND` fallback, not a charged mix |
| a cell that charged no leg | every fire unfillable is a finding, not a zero |
| the gate buys down nothing, or costs more | squaring hides a sign: a negative effect otherwise yields a plausible finite floor |
| fewer than two decision dates | no arrival rate exists |
| no dispersion on a rule's own fires | the variance transfer would have no measured direction |
| more than one stored row for a borrowed cell | picking either is a silent choice between measurements |
| a zero, negative or non-finite cluster count, interval width or dispersion | each produces a plausible positive floor after squaring (Codex ckpt-1, finding 28) |
| a reversed interval (`ci_high < ci_low`) | the same, and `sql/265` only CHECKs it on the stored row |
| missing provenance on the census artefact | corpus version, strategy hashes, universe, boundary and `cost_model_id` are not checked today; an immutable judgement needs them pinned (finding 27) |

⚠ Three edges are not code refusals and are handled by construction. **Unequal arm
coverage** is reported rather than refused (`N_masked <= N_admitted` is expected).
**Corpus or vendor drift between the census and the run** is caught downstream — the
run's `corpus_version` is asserted against the declaration at readout, per §"Readout and
abort bar". **Same-day concentration** is reported on every cell, and it is mild here.

## Sequencing — why the declaration is NOT in this PR

`PreregDeclaration.digest_payload` includes `strategy_version`, which for a manifest
strategy IS the identity hash. A declaration frozen on this branch would be
invalidated by any review comment touching the module, and `sql/333` bars UPDATE and
DELETE — recovery would mean a new strategy version and a second charge on the shared
trial register.

1. **this PR** — the strategy exists, is registered, is tested, the census that sized it
   is reproducible, and (added by the `-v2` amendment) the declared statistic exists as a
   reviewed pure evaluator, `strategy_mt1_trial`'s own sequencing;
2. add the `DeclaredTrial` register entry, then freeze the declaration against the
   merged `strategy_version`, `falsification_only` — for arm 1's reason and one of its
   own: no stored corpus window can CONFIRM a hypothesis formed off stored cohorts,
   and the threshold is a 2026 calibration applied to historical prices. The run this
   authorises can only kill.

   ⚠ **ONE trial, keyed on the STRATEGY ID.** `sharpe_variance` and `deflate_group`
   both key by `trial_id` and are handed strategy ids, so a trial named after the
   contract (arm 1's `sh-volatile-regime-gate-2026-08-22`) is invisible to them — that
   is precisely why S-11 does not resolve. Minting a SECOND trial keyed on the strategy
   id would count one search twice in `M`. So arm 2's single entry takes
   `trial_id = "s12-cheapest-band-price-gated-breakout"` with
   `declared_for = ("s12-cheapest-band-price-gated-breakout", "<merged version>")`, and
   the declaration carries the contract string in `contract_version` — which does not
   have to equal a `trial_id`; arm 1's freeze script equates them only because it looks
   its trial up that way, and arm 2's script looks its own up by `declared_for`.

   ⚠ **Adding the trial IS the supersession** (`trial_register.py:118-129`): a new entry
   strands every stored deflation via `trial_register_superseded` whether or not
   `TRIAL_REGISTER_VERSION` moves, and *"declining to add one to protect old rows is the
   flattering direction this module is built against"*. Follow r8's and r9's own
   precedent — run the stranding query recorded beside that constant and write what it
   stranded into the bump comment, rather than arguing it.

   ⚠ **Freeze close to the run.** `strategy_version` hashes `COST_MODEL_ID`, so a cost
   model bump rotates it and strands the declaration — which is exactly what happened to
   arm 1: its entry pins S-11 at `strategy-registry-v1+d5f25fd08376` while the current
   version is `c00e00ce79bb`, moved by #3238's v3 → v4. S-12's merged version is
   `strategy-registry-v1+f6100a890599` under `COST_MODEL_ID` v4;
3. explore in-sample (pre-2021-06-29), with the run-time basis guard above;
4. forward-shadow confirmation, only if 3 passes. ⚠ Forward shadow is not
   self-evidently confirmatory either: `cost_model` records that its quote sample is
   concentrated in the closing hour with no volatile-regime observations, so that
   instrument's timing, provenance, horizon and stopping rule are fixed in ITS
   declaration, not assumed here.

## Arm 1's spec is corrected by APPENDIX, not by edit

`docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md` §"Arms 2 and 3"
says the corpus cannot assign a cost band, so arm 2 is forward-shadow only. That
premise was falsified on 2026-09-20 (#2840; census merged as `49f785df`):
`survivorship_free` is pinned to an `unadjusted` archive and bands fine.

That document is **append-only after its freeze**, by its own terms, and declaration
11 is frozen. So the section is left exactly as written and a clearly scoped appendix
is added pointing here. The appendix changes no term of arm 1's contract — arm 1's
terms are about arm 1, and the falsified sentence was a forward-looking remark about
a different arm.

## Tests

Pure tier, no DB.

*The rule*
- S-4 fires at `close >= edge` → S-12 fires; the same bar below the edge → `not_fired`.
- The edge is INCLUSIVE, matching `PriceBand.contains`.
- S-12 fires at least once and equals S-4 exactly when every close is above the edge —
  a subset assertion alone is satisfied by a strategy that never fires.
- S-12's fired set is a SUBSET of S-4's over a generated series.
- The float comparison agrees with `band_for(Decimal(repr(close)))` across a grid
  spanning all four bands. ⚠ What licenses a float compare at all is the corpus
  proximity census `backtest_run.py:1479` cites — 0 of 75,972,669 open/close values
  within 1e-9 of 5/20/100 without equalling one — and that is evidence about THESE
  edges on THIS corpus, not a general precision guarantee.

*Refusals*
- Masked OHLC on a gated bar → S-4's `masked_reason`, never `not_fired`.
- A bar inside S-4's warm-up → `insufficient_warmup`, whatever the price.
- A bar below the edge that S-4 also refuses stays `not_evaluable` — the gate must not
  convert a refusal into a decline.
- ⚠ Unlike S-11 there is no new unevaluable INPUT: the close is already one of S-4's
  four declared inputs, so a priced bar S-4 can evaluate is one S-12 can evaluate. The
  gate is a body condition, which is legal here precisely because it introduces no
  "there is no such input" state to collapse — the distinction S-11's docstring is
  built around and the bug S-6 shipped.

*Identity*
- `S12_PARAMS["s4_source_hash"]` equals S-4's live `_source_hash()`.
- Monkeypatching the S-4 hash changes `s12_identity(...).version`.
- Changing the band edge changes the version.
- Blank `cost_model_id` is rejected, as on S-4.
- The import-time assertion fires when the cheapest band is not the open-above band.

*Basis coupling*
- `BACKTEST_UNIVERSE`'s pinned archive earns `as_traded` (guard 1).

*The declared statistic* (`tests/test_2840_s12_paired_trial.py`)
- The block length is MEASURED, not pinned: two series with different serial structure
  get different block lengths, and the only sample floor asserted is
  `block_bootstrap.MIN_CLUSTERS`.
- A uniform per-observation advantage passes all four of an arm's bounds; beating the
  control while losing money fails leg 1; making money without beating the control fails
  leg 2; ⚠ **an expectancy conjunct alone can fail the arm** — without that case,
  dropping the expectancy legs from the decision would leave the suite green while the
  decision metric stopped deciding. One failing arm fails the trial.
- ⚠⚠ **The pairing is asserted against a built alternative, not against a width claim.**
  A treatment equal to the control plus a constant makes every replication's difference
  exactly that constant, so the paired interval collapses to a point — and the test
  CONSTRUCTS the unpaired version (two independent resamples, differenced afterwards)
  and shows it is wide there. An earlier version asserted a width ratio without ever
  building the alternative it was contrasting with.
- ⚠ **The expectancy estimator is trade-weighted**, shown on a deliberately unbalanced
  fixture (20 trades on one date against 1 on another). Every earlier fixture put one
  trade per date, under which a wrongly equal-weighted cluster mean is indistinguishable
  from the ratio estimator.
- The union axis keeps a control-only date: the control's point estimate equals its own
  full-population expectancy.
- Refusals: the arm set not being the contract's declared pair (no arm, one arm, invented
  names, a repeat); a non-ascending or repeating date axis; a cluster date absent from
  the shared axis; a book supported by too few decision dates, refused BEFORE any draw;
  a degenerate marginal (while a degenerate DIFFERENCE is allowed, which is the state the
  pairing test relies on); a ruined, non-finite or scalar equity path; two books on
  different-length axes.
- Same input → identical bounds and identical block length.

*Manifest wiring*
- Present, non-retired, `per_series`, entry-only, S-4's exit regime (40-bar cap,
  level-based), both `exit_levels` and `exit_levels_batch` registered.
- The manifest's signals adapter actually gates — a copy of `_s4_signals` would
  silently un-gate the strategy while every test of the module itself still passed.
  Asserted directly, as `test_2840_manifest_adapter_gates` does for S-11.
- Scalar and batch exit levels agree and equal S-4's for the same request.
- The #2845 `KEPT` set grows to include `s12-…`.

## What this is not

Not a new TA idea — the standing order forbids another daily-bar variant. This is a
declared conditioning of an EXISTING measured rule on the cost model's OWN band table,
which is #2840's addendum as filed and phase 2 of the R5b queue.
