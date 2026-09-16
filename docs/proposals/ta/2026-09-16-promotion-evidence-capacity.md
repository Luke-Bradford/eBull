# Per-leg entry liquidity — #3104 slice 7a, and why `capacity_usd` is not in it

Status: proposed 2026-09-16.

⚠⚠ **Two Codex checkpoint-1 passes (44 then 48 findings) each killed a different
`capacity_usd` formula. The field is not a slice; it is a research ticket with
four unsettled questions, recorded in "What still blocks `capacity_usd`" below.**

What this slice ships instead is the part of slice 7 that **cannot be added
later**, which the parent's own addendum already identified:

> ⛔ **But it does not reach the book.** `_NamespaceBook` carries returns,
> entry/exit dates and `RegimeTradeObservation`; no liquidity column. A
> per-trade ADV needs a new column threaded through `_absorb` — the same
> plumbing argument that made the name key available to slice 1, and the same
> reason it cannot be added later: the book dies inside `_measure_namespace`.
> — #3104, slice-7 scoping addendum

So: **thread the per-leg entry-liquidity observation onto the book and out
through `LedgerMeasurements`. Emit no capacity scalar and touch no gate.**

⚠ This removes the book-death constraint, not every constraint. Blocker 3 below
needs each leg's realised notional, which lives in the sizing kernel
(`equity_curve.py`) and is a separate carry. The claim is narrow and exact: the
part that dies with `_NamespaceBook` is landed here, so a later capacity rule
never has to re-open this function to get at the bars.

## Scope

- ✅ per-realised-leg **causal mean of `close × volume` at entry**, measured
  inside `_absorb`, carried on `_NamespaceBook`, and summarised onto
  `LedgerMeasurements`. ⚠ Summarised ALONGSIDE `RealisedLedger` and not through
  it: the observation is not positionally parallel to the realised columns — a
  leg whose window fails is counted rather than measured — and forcing it in
  would break that object's parallel-column invariant.
- ✅ the exclusion vocabulary, its frozen precedence, and its counts.
- ❌ no `capacity_usd`, no `PromotionEvidence` change, no `evidence_refusals`
  change, no gate change, no cost-model change, no schema migration.

## Source rule

### The statistic and the lookback — already frozen, reused not minted

`app/services/strategy_decision_context.py:39-41`:

```python
volume_lookback_sessions: int = 20
volume_capacity_statistic: str = "causal_mean"
```

`sql/304_strategy_volume_context_provenance.sql:1-3` (#2508):

> Mean ADV is the capacity convention; median volume is the robust typical-day
> baseline.

and the column comment fixes the window semantics as *"Completed causal sessions
used by both mean and median volume baselines"*. So the measurement is the
**causal mean of daily dollar volume over 20 completed sessions**, which is the
statistic `volume_capacity_statistic` names for the purpose `sql/304` names it
for.

⚠ **Reusing the statistic does NOT inherit an exclusion policy.**
`strategy_decision_context` permits a zero mean and records
`zero_volume_frequency` separately; this module excludes instead. That is a new
decision and it is declared as one below, not smuggled in under the citation.

### Dollar volume as `close × volume` — in-repo precedent, not a first principle

`price_quarantine.py:435-439` computes the T3 corroboration ratio as

```python
prev_volume, volume = _usable_volume(prev), _usable_volume(bar)
prev_close, close   = _usable_close(prev),  _usable_close(bar)
...
ratio = (close * volume) / (prev_close * prev_volume)
```

so `close × volume` is already the repo's turnover quantity inside a settled
quarantine rule, with `_usable_volume` (`:302-306` — NULL or `<= 0` → `None`) and
`_usable_close` as its usability rules.

⚠ The PREDICATE is reused, not the function: those helpers take a `Bar` and
return `Decimal | None`, while `_absorb` holds `OHLCVRow` mappings. Saying
"reused verbatim" would hide a type mismatch, so the module applies the same
predicate through a named adapter and says so.

### Provisional bars are excluded, and that rule is sourced too

`price_quarantine.py:486-489`:

> T3 reads volume, and a provisional bar's volume is a **part-session count**.
> DEFER the verdict — do not quarantine a genuine move because today's bar is
> half-formed.

The eligible archive marks its trailing capture-period bars provisional
(`research_corpus_ingest.py:174-176`). A part-session count in a 20-session mean
understates that session's turnover, so a window containing one excludes the leg.

### The causal boundary — the window ends AT the signal bar

`signal_ledger.py:9-11` carries §3.5: *"Signal on the close of bar t → fill at
the OPEN of bar t+1. No exceptions."* Bar `t` has closed when the signal is
formed, so its volume is known and is **included**. The window is the 20
completed sessions ending at and including the entry **signal** bar — never the
fill bar, which is `t+1` and unknowable.

### Adjustment basis — `sql/305` is a refusal and is honoured as one

`sql/305_strategy_as_traded_price_provenance.sql:1-6` (#2508 / #2400):

> Price and dollar-volume cohorts therefore require either a directly observed
> unadjusted level or a reconstruction backed by point-in-time adjustments.

Vendor → basis, full census of `research_price_series`:

| vendor | universe | `adjustment_basis` | series |
| --- | --- | --- | --- |
| `icyDenev/Intrader` | `survivorship_free` | **unadjusted** | 22,879 |
| `paperswithbacktest/Stocks-Daily-Price` | `survivor_only` | `split_adjusted` | 7,693 |
| `etoro/etoro-comparators-…` | — | `split_adjusted` | 18 |
| `cboe` | — | unadjusted | 1 |

The measurement is produced on `unadjusted` series and **withheld on every other
basis**, including an absent one. `reconstructed_unadjusted` is admitted by
`sql/305` but unreachable — `cost_model.py:451-453` records *"The corpus has no
such factors"* — so it is not implemented.

⚠ Stored results split 256 `survivorship_free` / 324 `survivor_only`, so the
eligible path is live rather than theoretical, and it is the survivorship-free
path (#2721) a capital candidate runs on.

### ⚠⚠ What this column is, and what it is NOT

It is **the stored close times the stored volume on a series declared
`unadjusted`**, summed over a causal window. It is **not** asserted to be
nominal USD turnover, and the difference is not pedantry:

- `sql/251:32` states `adjustment_basis` *"describes the OHLC columns"* — it says
  nothing about `volume`;
- the only cross-check available is the other archive, and
  `research_corpus_ingest.py:168-170` rules that both are Yahoo redistributions,
  *"so the two are ONE observation and agreement between them is circular, never
  corroborating"*;
- a measurement I ran and am NOT relying on: on 3,930,231 split-affected common
  bars (selected as `Intrader.close / HF.close > 1.5`, an overlap of 4,549
  instruments out of 22,879 eligible series) the ratio `k_vol / k_price` has
  median exactly 1.0000 with 84.0% inside ±5% — so 628,633 selected bars sit
  outside it, unclassified. ⚠⚠ **Even at 100% this would prove only that the
  two archives agree, which a shared upstream distortion produces equally
  well.** It is consistent with the adjusted archive scaling volume and price
  together; it establishes nothing about either product being nominal turnover,
  and nothing about the 18,330 series with no counterpart.

So the column carries the basis with it and claims only what it measures. Naming
it USD capacity is precisely the step this slice declines to take.

## The measurement

Per realised leg, computed in `_absorb` where the series is still in hand:

```
entry_close_volume_mean(leg) = MEAN over the 20 completed sessions ending at and
                               including the entry SIGNAL bar of (close_t × volume_t)
```

⚠ **The name is deliberately literal.** "Dollar volume", "turnover" and
"liquidity" all assert more than the quantity carries: the volume basis is
unverifiable (see above), and even a verified price × shares uses the CLOSING
price for the whole session rather than traded notional, and says nothing about
spread, depth or auction liquidity. The field is named for its arithmetic.

Rolled once per (series, quarantine arm) over a numpy array and indexed by
signal bar, never per leg. ⚠ Per arm, not per series: `_absorb` runs separately
for each ambiguity arm, and `MaskedSeries` differs between `masked` and
`admitted` (`research_price_structure_store._apply_arm`). The diagnostic
**follows the arm it is measured on**, exactly as every other book quantity
does; sharing one cache across arms would silently give the masked arm the
admitted arm's prices.

⚠ **Per-field masking is preserved.** `StructureBar` masks per field — a
`range_usable = False` verdict masks high/low and leaves `close` intact — so a
range-only quarantine does NOT make `close × volume` unusable. Only
`return_usable = False` (which masks `close`) does.

### Basis is resolved ONCE per run, and asserted rather than assumed

`adjustment_basis` is a property of a series, not of a namespace. But
`load_universe_selection` pins one vendor per universe
(`universe_selection.vendor_for`), and vendor determines basis, so a run is
single-basis by construction. The producer **asserts** that: it reads the
distinct `adjustment_basis` over the admitted series once, and if it is not
single-valued it withholds the diagnostic rather than picking one. An assumption
that is cheap to check is a check.

### Exclusions — one vocabulary, frozen precedence, every leg accounted for

⚠⚠ **"Exclude the leg" means exclude its LIQUIDITY OBSERVATION and nothing
else.** The leg stays in `returns`, `gross_returns`, `entry_dates`,
`regime_observations`, the equity curve and `book.excluded` untouched. This
slice changes no existing population; a diagnostic that moved the return ledger
would be a corpus change wearing a diagnostic's name.

A window fails for more than one reason routinely, so the reason counter records
the **first** hit in this frozen order, which keeps `measured + excluded ==
realised` exact:

| # | reason | rule |
| --- | --- | --- |
| 1 | `basis_ineligible` | `sql/305`; run-level, so it takes every leg at once |
| 2 | `window_short` | fewer than 20 stored sessions at or before the signal bar |
| 3 | `scale_break_spanned` | the window spans an unresolved `price_series_break` (linked series only — see below) |
| 4 | `provisional_bar` | any session at or after the archive's pinned provisional cutoff |
| 5 | `close_unusable` | any session's close fails `_usable_close` (NULL, `<= 0`, or `return_usable = False`) |
| 6 | `volume_unusable` | any session's volume fails `_usable_volume` (NULL or `<= 0`) |
| 7 | `non_finite` | the resulting mean is zero, negative or non-finite |

⚠ **A failing session excludes the LEG's observation, it does not drop a TERM.**
A mean over 17 of 20 sessions is a different estimator and
`volume_lookback_sessions` is a declared 20. ⚠ This diverges from
`strategy_decision_context`, which permits a zero mean and records
`zero_volume_frequency` alongside; that divergence is a **new decision declared
here**, not something inherited with the citation. `_usable_volume`'s own
rationale is a ratio denominator, which does not by itself invalidate a mean.

⚠ **Provisional is read from the archive's PINNED `quarantine_as_of`**
(`research_corpus_ingest`, per vendor) and `PROVISIONAL_WINDOW_DAYS`, never from
today's date or the run's end — a diagnostic whose value changes with the wall
clock is not reproducible. ⚠ It is a policy flag over a calendar tail, so it
excludes some genuinely complete sessions; that is the conservative direction
and it is counted.

⚠ **The usability helpers take `Bar`, and `_absorb` holds `OHLCVRow` mappings**,
so they are applied through a thin adapter with the same predicates rather than
"reused verbatim". Non-finite inputs are rejected **before** any Decimal
comparison, because a Decimal NaN comparison raises rather than returning False.

⚠⚠ **The scale-break check cannot run on an unlinked series, and the fix is a
CAVEAT rather than an exclusion — that changed after measuring it.**
`price_series_break.instrument_id REFERENCES instruments(instrument_id)`
(`sql/246:103`), so the table describes the LIVE corpus; the survivorship-free
path uses `-series_id` for a series admitted without a live link (#2721 step 3,
`backtest_run.py:1540`), and no row can exist for one.

A first draft excluded those legs as `scale_break_unknowable`, on the principle
that "we cannot check" must not read as "it passed". Measured, that principle
was being applied in the wrong place:

- on `survivorship_free` **17,707 of 22,879 series (77.4%) are unlinked**, so
  the rule would withhold three quarters of the one universe the diagnostic is
  eligible for;
- against a base rate of **412 unresolved breaks across 170 instruments** in the
  whole table;
- and — decisively — **`backtest_run` already passes `()` for a negative key at
  every other call site**, which is how those legs came to be OPENED at all. A
  diagnostic applying a stricter scale-break standard than the position
  construction it describes would characterise a population the run did not
  trade.

So those legs are **measured**, and the measurement carries
`unlinked_series_leg_count` — how many of its measured legs sit on a series the
break table cannot describe. ⚠ The original concern is preserved, not dropped:
"no break record" and "no break" remain different statements, and that count is
which one applies. ⚠ A present break record is itself only what the quarantine's
own detector found; complete coverage is not claimed either way.

### Output

`LedgerMeasurements` gains one field, and no scalar anybody could mistake for a
capacity claim:

```python
entry_liquidity: EntryLiquidityMeasurement
```

⚠ **Always present, never `None`** — withholding is expressed INSIDE it, so the
accounting survives. It carries the measured-leg count, the realised-leg count,
the exclusion counter by reason, the resolved basis (or the reason it could not
be resolved), the lookback, the rule version, and — **nullable together** — the
min / p05 / p50 of `entry_close_volume_mean` with the argmin leg's name key and
entry **signal** date. Where no leg was measured, the summary fields are `None`
and the counts still reconcile.

⚠ Withholding never raises. `_ledger_evidence`'s `ValueError` path aborts the
whole run (`backtest_run.py:1754`), which is right for a misaligned ledger and
wrong for a corpus whose basis does not support a diagnostic. ⚠ That is NOT a
blanket catch: misaligned arrays, a signal date after its own fill date, and a
reason counter that does not reconcile still raise, because those are structural
bugs rather than corpus properties.

⚠ Quantile method is frozen as `numpy.percentile`'s `linear` interpolation over
the measured population, with the measured/total counts travelling beside it, so
a p05 computed from a thin subset is visible as such. ⚠ Counts show
incompleteness; they do **not** show which way the excluded legs would have
moved the distribution, and the spec claims only the former. Argmin ties break
on the lowest `(name_key, signal_date, exit_date)`, fixed by construction so the
reported leg is deterministic.

⚠ **The deliverable is the per-leg ARRAY existing inside `_measure_namespace`,
not the summary.** The summary is what a human reads; the array is what a later
capacity rule consumes, in the same function, alongside the returns and
concurrency slices 1 and 4 already build there. It is deliberately not persisted
— 4.2M floats are what `LegBook`'s design exists to avoid — which is exactly why
the computation must live where the book does.

## What still blocks `capacity_usd` — four questions, each with its evidence

Recorded here because the next session must not re-derive them.

1. **There is no computable impact model.** `.claude/skills/quant/strategy-evidence.md`
   §2.11b: `cost_model.BANDS` is size-independent and *"has NOTHING to say about
   capacity"*. Square-root impact (the law §2.11b states) needs a calibration
   constant obtainable only from our own fills — slice 5's slippage circularity.
   Linear Amihud inversion needs no constant but contradicts the concavity the
   same source states and promotes a documented proxy to a calibrated
   coefficient.
2. **A count-based coverage floor cannot bound a MINIMUM.** Nineteen measurable
   legs at $1m plus one excluded $1 leg passes a 95% floor and reports $1m. Any
   min-aggregated capacity needs either 100% coverage or an aggregation that is
   not a minimum — and the participation constraint is hard, which is what makes
   it a minimum.
3. **The sizing rule does not allocate `A/n`.** `equity_curve.py:820` sizes an
   entry at `min(equity_ref / basket, cash)`, which can be **zero**, and equity
   compounds across the window. A capacity scalar consistent with the frozen
   `SIZING_RULE` needs each leg's realised notional relative to starting equity,
   which lives in the numba kernel, not in the book.
4. **The record has nowhere to put the provenance.** `PromotionEvidence` is
   frozen and has no field for a capacity rule version, coverage share, exclusion
   reasons or binding leg. A bare positive `Decimal` in an immutable sha256'd
   record is exactly the shape that later reads as measured.

None of the four is person-gated. (1) and (3) are research; (2) is a design
choice with a stated counterexample; (4) is a contract-version question that
belongs with slice 3's path-diagnostic versioning, which is already blocked on
naming the first capital candidate's mechanism.

## Full-population verification

- **Vendor → basis** is the full `research_price_series` census above (30,591
  series), not a sample.
- **Eligible results**: 256 of 580 (`survivorship_free`); the other 324 are
  withheld by basis.
- ⚠ **How many eligible legs actually survive the exclusions is NOT yet
  measured** — it cannot be, because no stored table holds a per-run book, and
  the producer's input exists only inside `run_backtest`. The PR measures it on
  a real run and records the exclusion histogram; this spec does not pre-claim
  it.
- ⚠ **A detector run and DISCARDED**, recorded so it is not re-run: selecting
  "exact" splits as a prior/current close ratio within 0.5% of an integer
  returns 886 bars with median dollar-volume ratio 0.518, which reads as
  mixed-basis and is an artefact — real splits move on the day (AAPL's 2020 4:1
  reads **3.869**, not 4.000), so the filter selects crash days landing near an
  integer by coincidence.

## Acceptance

Table tests over the arithmetic, per the producer proposal's rule that interim
slices carry no end-to-end test:

- the causal boundary: an extreme volume on the **signal** bar moves the mean; the
  same volume on the **fill** bar does not;
- a 19-session history excludes as `window_short`;
- one zero-volume session excludes the leg as `volume_unusable` rather than
  averaging 19 terms;
- a `return_usable = False` close excludes as `close_unusable`, while a
  `range_usable = False` bar with an intact close does **not** — per-field
  masking is preserved;
- a provisional trailing bar excludes as `provisional_bar`, and the cutoff comes
  from the pinned `quarantine_as_of` rather than the current date;
- a window spanning an unresolved break excludes as `scale_break_spanned`;
- a **negative name key** is MEASURED and counted in `unlinked_series_leg_count`
  rather than excluded, and `scale_break_unknowable` is asserted absent from the
  exclusion vocabulary so the two models cannot drift back together;
- a `split_adjusted` basis yields a measurement with zero measured legs, all
  realised legs counted under `basis_ineligible`, `None` summaries, and **no
  raise**;
- a mixed-basis admitted set withholds rather than picking a basis;
- **measured + excluded equals the realised-leg count, always** — including a
  window failing several rules at once, which must count exactly once under the
  frozen precedence;
- zero measured legs on an ELIGIBLE basis yields `None` summaries with counts
  intact (distinct from the ineligible case only by the reason counter);
- a misaligned array or a signal date after its fill date still raises;
- a book that recorded NO verdicts at all summarises to `None` (a hand-built
  harness ledger), while a book that recorded SOME but not one per realised leg
  still raises — a partial accounting read as a complete one is the failure the
  invariant exists to catch;
- the argmin tie-break is deterministic across two legs with equal means;
- the leg's presence in `returns` / `entry_dates` / `regime_observations` is
  unchanged by any exclusion — asserted directly, because "changes no existing
  population" is the claim a reviewer most needs proved.
