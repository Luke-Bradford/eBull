# #3104 slice 9 — the session-gap statistic, and the bound it CANNOT supply

**Status:** spec, v3 after two Codex checkpoint-1 passes (30 then 35 findings).
**Refs** #3104, #2505, #2500, #2437.

Slice 9 because `worst_gap_pct` belongs to **no** existing slice.
`docs/proposals/ta/2026-09-16-promotion-evidence-producer.md:583` registers it as
an open item — *"has no producer and no definition anywhere (adverse-gap
direction, reference price and population all undefined — neither the worst
return nor the entry-to-exit return measures it)"* — and the producer doc's field
table has no row for it. Numbering it 9 leaves slices 1-8 untouched.

It is a **required non-null field** of the frozen `PromotionEvidence`
(`strategy_promotion_evidence.py:170`), so #3104 cannot complete without it
whatever happens to the blocked slices 3 / 5 / 6 / 7b / 8.

## ⛔⛔ The finding: `worst_gap_pct` cannot do the job #2500 asks of it

v1 of this spec defined the field as the **exit-bar** gap and justified that from
its one in-repo consumer.
`strategy_decay_sequential_test.hoeffding_variance_proxy` (`:182-187`) says *"The
caller must therefore widen the strategy's TP/SL contract by the pinned
`worst_gap_pct` rather than pass the naive barriers"*, and
`docs/proposals/ta/2026-09-16-2500-sequential-decay-test.md:88` makes it concrete
— *"the declared lower bound is `-(stop_barrier + worst_gap)`, not
`-stop_barrier`"*.

**That is unsound, and the same docstring is where it says so.** Two lines above
the sentence it cites:

> ⚠⚠ AND THE BOUND MUST BE A TRUE ALMOST-SURE BOUND. **Policing it afterwards
> does not make it one.**

Any `worst_gap_pct` produced from a realised book is a **historical extremum** —
precisely the "policing afterwards" the module rejects. Hoeffding's lemma needs
`X ∈ [a, b]` **almost surely**, not `X ∈ [min observed, max observed]`. Three
independent defects, any one sufficient:

1. **A sample extremum is not a bound.** The next trade may gap worse, and ⚠ no
   finite sample can establish bounded support at all — so measuring more does
   not fix it, and neither does measuring both tails. (v2 claimed the two-tail
   measurement made the unboundedness "checkable". It does not; the tails are
   diagnostics.)
2. **Hoeffding needs BOTH bounds; the field is one-sided and non-positive.** A
   long can gap **through** its target: entry 100, target 120, prior close 119,
   an open at 150 is a 26.1% gap against a 30-point overshoot.
3. **The units do not line up.** A gap divides by the prior close; an overshoot
   divides by the entry; and the monitored stream is **net** of the half-spread
   `_absorb` charges both fills, which the gap does not contain.

⚠ **Stated precisely, because "no producer exists" is too strong.** What has no
producer is *a historical-extremum bound for the current, unrestricted return
stream*. A sound construction does exist and it **changes the estimand**: freeze
`Y = clip(X, a, b)` on declared `a, b` and monitor `Y` against **its own**
declared baseline. That is a different claim from #2500's expectancy one and must
be declared as such — it cannot be slipped in under the existing wording. ⚠ The
alternatives floated in v2 were wrong as stated: an ordinary broker stop does not
guarantee a bound, a guaranteed downside barrier still leaves the upside
unbounded, and a method needing no bounded support *replaces* eq (14) rather than
supplying its `[a, b]`.

**Two consequences, neither person-gated:**

- **This slice produces a DESCRIPTIVE realised-path tail statistic and says so.**
  Same standing as its neighbours in the record — `max_drawdown_pct` and
  `expected_shortfall_5_pct` are historical extrema too, and #2505 claims nothing
  more for them. ⚠ v2 argued from `max_drawdown_pct` to justify a realised-only
  population; that was wrong — drawdown is an equity-path statistic including
  marked open exposure. The population argument stands on its own below.
- **The unsafe instruction is disarmed where it lives, not only recorded
  elsewhere.** A blocker on a ticket does not stop a future assembly following a
  docstring, so `hoeffding_variance_proxy`'s docstring is amended in this change
  to name the defect and say *do not wire the widened bound*.

## Source rule

The contract gives three words — *"worst gap"*
(`docs/proposals/ta/2026-08-12-promotion-edge-evidence-contract.md:25`). No
window, no direction, no reference price. So this opens with a source-rule step,
per the *"am I about to pick a threshold, ratio or window"* trigger. What each
source licenses, and what it does not:

- **The reference-price pair is PUBLISHED.** Yang & Zhang (2000), *J. Business*
  73(3) decomposes daily variance into an **overnight** leg and an open-to-close
  leg, the overnight leg being the demeaned sample variance of `ln(O_t/C_{t-1})`
  — carried in `.claude/skills/market-technician/quant-methods.md` as the
  estimator *"that incorporates overnight gaps"*. ⚠ That licenses `O_t` against
  `C_{t-1}` as the session-gap reference pair **and nothing else**. It does not
  prescribe a simple-return form, an extremum, or any barrier property. The rest
  is constructed, and frozen in `EXIT_GAP_RULE_VERSION`.
- **The sign convention follows from LONG-ONLY, which is settled.**
  `position_costing.py:179-185`: *"LONG-ONLY BY ITS OWN ARITHMETIC … the entry is
  charged as `buy_price` and the exit as `sell_price`, so a short — whose entry
  is a sale — cannot pass through here meaningfully."* So down is adverse. ⚠ It
  does not make a one-sided statistic sufficient for anything two-sided.
- **The carry onto the return basis is an in-repo settled construction, reused
  not re-derived.** `synthetic_control_run.py:582-586` projects a bar's open onto
  the total-return basis with *"the same carry `_absorb` applies to a real leg …
  so the two are on one basis"*.
- **A hole is not a gap, and the repo already ruled on that.**
  `price_quarantine.evaluate_transitions`'s T2: *"calendar gap wider than the
  per-class hole threshold … A ratio spanning a series HOLE is not a same-scale
  comparison at all."* `hole_days` is imported from
  `params_for(research_corpus_ingest.ASSET_CLASS)` rather than restated.
- **A scale break is not a gap either.** `price_segments.py:1-6`:
  *"`price_series_break.break_date` is the first date at the new scale. Bars on
  either side remain usable inside their own segment; indicators and positions
  must not span the boundary."*

⚠ **A citation v1 made and this version withdraws.** v1 cited *"141 of 1,402
filled stops executed at an open beyond the stop level"* as empirical support.
That measurement (`docs/proposals/ta/2026-08-09-plan-of-attack.md:18`,
`scripts/verify_2437_short_stops.py:115-150`) is a **filtered short** scan
counting UPWARD stop breaches on a ≥12%-drop cohort. Not this population, not
this statistic, and it establishes neither coverage nor conservativeness here.

## Definition

Per **realised** leg, over every session boundary the leg held through: for a leg
filling on series bar `f` and closing on series bar `c`, the boundaries `(i-1, i)`
for `i ∈ [f+1, c]`.

```
gap%[i] = ((open[i] / raw_close[i]) * (wealth_close[i] / wealth_close[i-1]) - 1) * 100
```

The namespace statistic is `min_gap_pct` = the minimum over all measured
boundaries of all measured legs, and `max_gap_pct` = the maximum.

⚠ **Named for the arithmetic, not "worst" / "best".** A one-sided population
makes those words wrong: an all-adverse run has a *negative* maximum and an
all-favourable one a *positive* minimum. The `min(0, …)` the frozen field's sign
rule needs is an assembly step (below), not a clamp applied here — clamping here
would make "no adverse gap occurred" indistinguishable from "an adverse gap of
exactly zero".

### ⚠⚠ Two ratios, not one product — a correctness rule

The algebraically identical `(open × wealth / raw) / prior_wealth` **underflows**.
Codex reproduced it: with all four prices `1e-200` the product form returns
**−100% for a flat boundary**, and with all four `1e200` it returns `inf` and
withholds a boundary that is exactly flat. Each ratio in the form above is near 1
for a normal bar at any price level, so no intermediate leaves float range.
Finiteness is checked on the **final percentage**, not the ratio, because a finite
ratio can leave range when scaled.

### ⚠⚠ Two index domains

`raw_closes` and `wealth_closes` are **dense arrays over the instrument's PANEL
SPAN** (`_dense_price_history`, `backtest_run.py:1456-1481`: a
`[NaN] * (last - first + 1)` filled only at `axis_pos[when]`). `open` lives on the
**series**. Indexing both with one integer reads different bars wherever the panel
carries a date this instrument did not trade — the normal case on a 5,000-name
panel. So adjacency is taken on the series axis and **each** index is mapped
independently through `axis_pos[…] - first_axis_index`.

### ⚠ What the carry is, and is not

`wealth_close/raw_close` is **whatever separates the run's total-return close from
the stored close**, and that depends on the archive. `sql/251:32-47` (*"The OHLC
columns carry only the split adjustment … `adj_close` is NOT interchangeable with
`close`"*) describes **one** archive; `icyDenev/Intrader` — the eligible one —
stores `unadjusted` OHLC, so its factor carries splits **and** distributions. The
carry makes both cancel, which is why a split fixture (not only a dividend one) is
a required test.

⚠ **Under `LEGACY_RETURN_BASIS` there is no carry at all.**
`_dense_price_history` substitutes the raw close for the wealth close, so the
factor is exactly 1 and the statistic is a **price-only** gap rather than a
total-return one. Two runs reporting the same number mean different things unless
the basis says which, so `return_basis` is reported on the measurement.

⚠ **This is a GROSS price return across one boundary.** It excludes the
half-spread `_absorb` charges both fills, any execution slippage, and the
terminal-value haircut a `series_termination` close applies
(`backtest_run.py:1070`) — none of which is a price gap. A net-return bound is not
derivable from it, which is half of why the bound claim was withdrawn.

### Why the whole hold, and not the exit bar alone

v1 chose the exit bar because the barrier-overshoot argument made mid-hold
boundaries irrelevant. With that argument withdrawn there is nothing left to
justify a narrower population, and three cases show the narrower one understates:

- A **barrierless** strategy (`signal_pair`, `calendar`) can take its worst gap
  mid-hold and close on a flat boundary. ⚠ Close source cannot be used to infer
  barrier presence either — `max_hold` closes bracketed strategies too, and a
  stop-only bracket (S-7) has no finite target at all.
- A `series_termination` close is *last close × terminal-value fraction*, not a
  bar open, so its exit boundary can be flat while the leg lost most of its value.
- The denominators differ, so the two are not interchangeable where both exist.

**Cost.** ~`bars_held` boundary reads per leg against slice 4's measured 4.2M-leg
book. The per-bar array is rolled once per `_absorb` invocation — ⚠ i.e. per
(series, **quarantine arm, ambiguity arm**); `_absorb` runs separately for each
ambiguity arm, so "once per series" would be wrong. Same placement and the same
repetition as slice 7a's `causal_close_volume_means`. Measured and reported in the
PR.

## Population, exclusions and accounting

**Denominator is `len(book.returns)`** — realised legs that reached the curve,
counted after namespace routing and after `_absorb`'s own pricing exclusions
(`entry_bar_off_axis`, `uncosted_reason`, `total_return_price_missing`,
`close_bar_off_axis`). ⚠ Open legs are **not** the complement: a discarded
position is neither realised nor open. The recorder is called in **the same
branch that appends the return**, which is what makes the equality achievable
rather than merely asserted.

⚠ **Realised-only omits held exposure, and that is reported rather than implied.**
An open leg at the window end can hold the run's most extreme boundary.
`open_leg_count` travels with the measurement so the omission is visible.

Each realised leg gets **exactly one** verdict, so `measured + excluded ==
realised`. A leg is **measured** when at least one boundary is measurable, and
carries the min and max of those; otherwise it is excluded under the
lowest-ranked reason across its boundaries.

⚠ **"Earliest match" means LOWEST RANK in the frozen tuple, not chronologically
first.** A leg whose second boundary is `off_axis` and whose fifth is
`open_unusable` reports `off_axis`. Chronological order is not reproducible under
a re-ordered sweep.

⚠ **Leg-level reasons are evaluated before any boundary exists.** Otherwise a
`c == f` leg indexes `f-1` and, at `f == 0`, wraps to the last bar of the series.
The per-bar roll cannot pre-empt them either: `boundary_gaps` raises only on
misaligned inputs, never on data.

| # | reason | level | test |
| --- | --- | --- | --- |
| 1 | `not_instrumented` | leg | A realised return with no recorder call. ⚠ NOT inherited from slice 7a, which returns `None` when nothing was recorded and takes the accounting with it. On a hand-built harness book this is the truthful state; on a production run it is a bug, and naming it is what makes the difference visible. |
| 2 | `no_session_boundary` | leg | `c == f`. `sql/256` declares `bars_held = 0` legal; the entry fills at the fill bar's own open (`signal_ledger.resolve_fills`'s `unusable_fill_price` clause), so the gap into that bar happened before the position existed. |
| 3 | `provenance_unknown` | leg | `_resolve_liquidity_policy` returned `None` — an unknown vendor, a mismatched stored basis, or an admitted set spanning more than one. Without it there is no pinned `provisional_from`, so rule 6 cannot be evaluated and withholding is the honest verdict. |
| 4 | `off_axis` | boundary | either date absent from `axis_pos`, or its offset outside the dense span. |
| 5 | `session_hole_spanned` | boundary | `(dates[i] - dates[i-1]).days > hole_days`. T2's rule and threshold, imported. |
| 6 | `scale_break_spanned` | boundary | an unresolved break strictly after `dates[i-1]` and at or before `dates[i]`. |
| 7 | `provisional_bar` | boundary | either bar at or after the archive's pinned `provisional_from`. |
| 8 | `open_unusable` | boundary | `open[i]` NULL, non-finite or `<= 0` — `price_quarantine.rule_b1`'s open clause, and `synthetic_control_run.py:582`'s test verbatim. ⚠ The `Decimal → float` conversion is guarded, so a signalling NaN becomes this verdict rather than an exception out of a diagnostic. |
| 9 | `close_unusable` | boundary | any of `wealth_close[i-1]`, `wealth_close[i]`, `raw_close[i]` non-finite or `<= 0`. |
| 10 | `non_finite` | boundary | the final percentage is not finite. |

### ⚠⚠ `scale_break_spanned` exists because v2's claim that it could not happen is FALSE

v2 argued a realised leg cannot span an unresolved break, because positions are
built per segment. `segment_end_index` (`backtest_run.py:953-957`) guards the
**level-resolved** path only; a `signal_pair` / calendar / max-hold exit never
goes through the resolver. **Codex checkpoint 1 built one that crossed a supplied
break and booked a −50.72% return.** Left unexcluded, a 1:2 rescale prints a −50%
"gap" that never happened — and because the statistic is an extremum, one such
boundary does not bias the estimate, it *becomes* it.

⚠ Break coverage is partial in both directions, and neither is a safety argument.
`unresolved_breaks` arrives as `()` for a negative name key (`-series_id`, #2721
step 3) because `price_series_break.instrument_id REFERENCES instruments`
(`sql/246:103`) describes the live corpus — carried as
`unlinked_series_leg_count`, a caveat with no conservatism claim attached. And a
**positive** key is not evidence of full coverage either: the break table
describes live prices, while a research archive can carry historical scale defects
of its own.

### Not excluded: adjustment basis

⚠ Slice 7a gates on `adjustment_basis == 'unadjusted'` because `sql/305` requires
a directly observed unadjusted level for **price and dollar-volume attribution**.
This is a **return** on the basis the backtest already prices every leg on, and no
rule requires an unadjusted level for a return. Declared as a difference from 7a,
not inherited. ⚠ What the basis LABEL does not establish is that corporate-action
treatment is consistent across every series and boundary; it is reported beside
the number rather than inferred from.

### Two named limits of the hole and provisional rules

- **T2 excludes only LONG holes.** A single missing session inside ten calendar
  days still yields a multi-session return counted as one boundary, and the
  threshold cannot distinguish an archive hole from a genuine suspension.
  `boundary_calendar_days` travels with each binding observation, and the census
  reports the span distribution.
- **`provisional` means REVISABLE, not necessarily part-session.** v2's
  justification (*"its close is not the session close"*) was too strong —
  `PROVISIONAL_WINDOW_DAYS` covers completed sessions subject to correction too.
  Withholding still holds; the reason is revisability.
- ⚠ Masking does **not** remove bars. `_apply_arm` nulls selected fields, so the
  date axis and therefore the boundary set are identical across arms; compressing
  masked rows would invent different boundaries.

### Coverage, and the direction of its bias

`measured + excluded == realised` is an accounting identity and does **not**
establish coverage. Reported alongside:

- `measured_boundary_count` — the denominator, so `unmeasurable_boundary_count`
  reads as a rate rather than a bare number.
- `unmeasurable_boundary_count` — counted once per (leg, boundary); a boundary
  held by two overlapping legs counts for each.
- `partial_coverage_leg_count` — **measured** legs with at least one unmeasurable
  boundary. ⚠ A wholly excluded leg is not partially covered; conflating them lets
  "0 partial" coexist with "0 measured".

⚠⚠ **Missingness has a KNOWN DIRECTION here, unlike for a mean.** Restoring any
withheld boundary can only lower the minimum and raise the maximum. The measured
subset therefore systematically **understates tail magnitude or leaves it
unchanged** — it can never overstate it.

## Numeric policy

- `float` arithmetic; reported as `Decimal` at 4dp (a percentage, not a currency —
  7a's 2dp is a dollar-product rule). Quantisation runs in a local 800-digit
  context: `Decimal(repr(1e100)).quantize(Decimal("0.0001"))` raises
  `InvalidOperation` under the default 28-digit one.
- **Rounding is OUTWARD FROM THE INTERVAL** — the minimum floors, the maximum
  ceilings — so a rounded pair always contains the raw one. ⚠ The claim is about
  the **quantisation only**. Codex reproduced the limit: an exact
  `−0.1000000000000000000100%` is already `−0.09999999999998899` as a float before
  any rounding, and flooring gives `−0.1000%`, above the truth. Float error is not
  controlled and is not claimed to be.
- **Binding selection happens on the RAW floats**, with a declared tie-break
  (`name_key`, fill date, exit date, boundary date) — `argmin` alone returns
  corpus-sweep order.
- **Both extrema carry their own binding observation**, each with `name_key`,
  `fill_date`, `exit_date`, `prior_bar_date`, `bar_date`,
  `boundary_calendar_days` and `close_source`.
- **Nullable together.** With no measured leg, every summary field is `None`. No
  zero is substituted.

## Measurement identity

`EXIT_GAP_RULE_VERSION`, a literal bumped on a RULE change and never on a comment
(slice 7a's recorded reason: a source hash moves the stamp on a re-worded
docstring). It pins the arithmetic, the population, the exclusion precedence, the
conversion and rounding policy and the tie-break.

⚠⚠ **It pins this module's rules and NOT its inputs.** `HOLE_DAYS` and the
archive's `provisional_from` are imported, so a change in `price_quarantine` or in
an archive literal moves the result without moving the string. That is why
`hole_days`, `adjustment_basis` and `return_basis` are reported **values** on the
measurement rather than left implicit in the version.

## Placement, and what this slice does not do

Measured in `_absorb`, carried on `_NamespaceBook`, summarised onto
`LedgerMeasurements`, ridden out on `NamespaceMeasurement` — the producer doc's
clause 3 and slices 1/2/4/7a. The book dies inside `_measure_namespace` and
nothing stores a per-run book.

It does **not** touch the frozen `PromotionEvidence`, change a gate, add a
migration or a stored column, or make any current row promotable — all 580 stored
results are `purpose='harness_validation'` and are refused earlier
(`strategy_control_plane.py:591`).

⛔ **Registered for slice 8, not solved here: the assembly needs a COMPLETENESS
rule, not just a sign clamp.** `min(0, min_gap_pct)` fixes the sign and says
nothing about whether partial coverage may populate a mandatory field in an
immutable record that cannot carry the caveats. Slice 8 must decide whether to
withhold or to qualify, and `PromotionEvidence` has nowhere to put the
qualification — the same contract-version problem slices 3 and 7b already carry.

⚠ Also registered: `_ledger_evidence` returns `None` on an empty ledger, so a
namespace with no realised legs loses this measurement — and its zero counts and
version — with the enclosing record. Pre-existing for slice 7a too; not changed
here.

## Full-population census

`scripts/census_3104_exit_gap.py`, which **calls `boundary_gaps` itself** rather
than re-expressing its rules in SQL. ⚠ An earlier SQL-only census reported a
"measurable" population that was not the module's, because it had no hole rule, no
break rule and no provisional cutoff; a census that restates a closed vocabulary
is a second copy of it.

Run 2026-09-16, `vendor = 'icyDenev/Intrader'`, `provisional_from 2024-09-22`
(from the archive's pinned `quarantine_as_of 2024-09-27`), unresolved breaks on
170 linked instruments:

```
series               22,879
boundaries       50,134,060
  measurable     50,059,856  99.852%
  provisional_bar    50,519   0.101%
  no_session_boundary 22,879   0.046%   -- exactly one per series
  session_hole_spanned   786   0.002%
  scale_break_spanned     18   0.000%
  close_unusable           2   0.000%
  open_unusable            0   0.000%

adverse (< 0)    21,707,551  43.363% of measurable
minimum          -99.99947916666667   (series 20881, 2022-12-05)
maximum       11555444.444444446      (series 30452, 2018-05-31)

measured boundary calendar spans
  1 day            39,225,648
  2-3 days          9,482,309
  4-5 days          1,347,344
  6+ days               4,555
```

**It reconciles exactly with the SQL-only census this replaced**, which is the
check that the two differ only by the v2 rules: 50,111,179 − 50,059,856 = 51,323
= 50,519 provisional + 786 holes + 18 breaks.

⚠ The extremum is printed **unrounded**, and that matters: the SQL census
reported `-100.00` from a `round(…, 2)` and an earlier draft of this spec read
that as "exactly −100.0000%, a floor artefact". The true minimum is
**−99.99947916666667%** — still almost certainly a defect, but not a floor, and
the rounded figure could not have told the difference.

⚠⚠ **The maximum is `+11,555,444%`, and it is the argument for measuring both
tails.** A 115,000× up-gap is a data defect, not a market move; a one-sided
adverse statistic would report a clean number for that series while the defect
sat in it. ⚠ Noted, not filed — the census applies no quarantine masking (stated
below), and this is a corpus observation rather than a defect in this change.

⚠ **What it bounds and what it does not.** Per boundary over the whole archive.
The run's population is narrower (realised legs, one quarantine arm) and the
census applies no panel mapping — every series is censused on its own dense axis,
which is the most favourable case for `off_axis`, stated rather than hidden. Legs
are not uniformly distributed over bars, so nothing here predicts the per-leg
histogram.

## Acceptance

1. `measured + excluded == realised` against `len(book.returns)`, and a withheld
   boundary removes a leg's **gap observation and never the leg**.
2. **Changing** carry factors — a split (raw halves, wealth does not) and a
   dividend — both asserted to cancel. ⚠ A factor-of-one fixture cannot validate a
   carry.
3. Numeric extremes: `1e-200` and `1e200` flat boundaries both read 0.0 (the
   product-form regression).
4. Sparse-axis mapping: a series at dense offsets 0, 2, 5 reads its own bars.
5. Precedence: collisions within one boundary; **across** boundaries by rank not
   by chronology; leg-level before boundary-level; `f == 0` cannot wrap.
6. Populations kept distinct: none realised; all same-bar; all boundaries
   unusable; unresolved provenance; never instrumented; partial versus wholly
   excluded.
7. Both tails on one-sided populations, and deterministic ties for each.
8. Cross-break **non-level** leg excluded; a break on the prior bar does not
   exclude the following boundary.
9. Structural corruption raises rather than excluding: missing endpoint date,
   close before fill, short verdict array, mismatched raw/wealth lengths.
