# Cost basis from the pinned archive, not a literal (#3238)

Status: **researched, NOT ready to build.** Codex checkpoint 1 returned 30 findings and
three of them are structural — the fix is not the two-line change it looks like. This
document records what is established, what the fix must cover, and in what order.

Ticket: #3238. Blocks: #2840 arm 2. Refs #2437, #2721, #2400, #3104.

Reproduce every figure below with:

```
PYTHONPATH=. uv run python -m scripts.census_3238_cost_basis
```

## The defect

`app/services/backtest_run.py:2601` and `:2993` both call

```python
costed = list(cost_positions(positions, price_basis="split_adjusted"))
```

`cost_band_for` (`app/services/cost_model.py:513-527`) answers a `split_adjusted`
basis with `UNKNOWN_NOMINAL_PRICE_BAND` — the maximum band, `<$5` — unconditionally.

That is correct for `survivor_only` and wrong for `survivorship_free`:

| universe | `vendor_for` | stored `adjustment_basis` | admitted series |
|---|---|---|---|
| `survivor_only` | `paperswithbacktest/Stocks-Daily-Price` | `split_adjusted` (5,264 of 5,264) | 5,264 |
| `survivorship_free` | `icyDenev/Intrader` | **`unadjusted` (17,285 of 17,285)** | 17,285 |

The entry fill is the t+1 bar's `open` (`signal_ledger.resolve_fills`), read from the
admitted series. On `survivorship_free` that open is an as-traded nominal price, and
`position_costing` supports `price_basis="as_traded"` end to end. The run holds a
nominal price and tells the cost model it does not have one.

**Not a settled decision.** `docs/settled-decisions.md` and
`docs/review-prevention-log.md` carry nothing on `cost_band` / `price_basis` /
`UNKNOWN_NOMINAL` / `1.450`. Neither call site carries a justifying comment.

## Source rule

Not inferred. The governing rule is the cost model's own declared contract:

> `as_traded` may use the price thresholds. `split_adjusted` cannot: in the absence
> of a point-in-time split factor it receives the maximum frozen spread.

The rule is already written and already correct. What is wrong is the *input* — the
basis passed in must be the basis the corpus carries, which the engine already
resolves and asserts per-series (`backtest_run._resolve_liquidity_policy`).

⚠ No new constant, threshold or window is introduced anywhere in this design.

## Why it is not a two-line fix — the three structural findings

Verified in the code, not taken from the review:

1. **The buy-and-hold comparator stays at the maximum spread.** `_benchmark_book`
   (`backtest_run.py:1449`) hardcodes `UNKNOWN_NOMINAL_PRICE_BAND.half_spread` under
   the comment *"The corpus OHLC is split-adjusted"* — false for this universe.
2. **The synthetic control stays at the maximum spread.**
   `synthetic_control_run._HALF_SPREAD` (`:172`) is the same constant, used in both
   the scalar and batched paths.
3. **Both of those cost on TOTAL-RETURN-ADJUSTED prices**, not raw opens —
   `entry_wealth` in the comparator, `placement.adjusted_open` in the control. A
   nominal band cannot be selected from a total-return level even inside an
   unadjusted archive, because the dividend adjustment is still in it. Banding them
   naively recreates #2400.

Taken together: **fixing the strategy side alone makes the strategy cheaper while its
benchmark and its null stay at roughly twice the cost.** That flatters
`return_vs_buy_and_hold_pct` and `synthetic_control_passed` — it manufactures
promotions rather than measuring them, which is strictly worse than the uniform
overcharge in place today. A partial fix must not ship.

## Evidence

All full-population, from the census script. Bands and edges are read from
`cost_model.BANDS` by the script; nothing is retyped.

⚠ **`p75_spread_pct` is a ROUND TRIP.** One side is half of it
(`PriceBand.half_spread_pct`). The first draft of #3238 labelled a round-trip figure
"half-spread" and was out by 2x — corrected on the issue. The charged/banded RATIO is
unaffected, being a ratio of like quantities, which is exactly why the error survived
a read.

`survivorship_free`, usable `open` bars from `EVALUATION_WINDOW_START`
(38,738,790 bars, 1962-01-02 .. 2024-09-27) — these are nominal levels, so the bands
are meaningful:

| band | bars | share |
|---|---:|---:|
| `<$5` | 7,498,200 | 19.36% |
| `$5-20` | 13,619,656 | 35.16% |
| `$20-100` | 16,118,733 | 41.61% |
| `>=$100` | 1,502,201 | 3.88% |

Bar-weighted round trip if banded **0.7057%** against **1.450%** charged — **2.05x**.

⚠ **Bar-weighted, NOT per-trade.** Legs are not stored (`strategy_promotion_evidence`
is empty — #3104's missing producer), so no per-trade figure exists without a run.
2.05x must not be quoted as the effect on any verdict.

⚠ The census prints a 2.10x ratio for `survivor_only` too. That is **not** a defect
and its band shares are **not** meaningful — those prices are split-adjusted, so the
thresholds are not nominal bands. The script prints that caveat beside the number
rather than leaving a reader to supply it.

**Live footprint:** 16 stored results carry `universe_basis = 'survivorship_free'`.

### What the archive label rests on

The `unadjusted` label is stored per-series on all 17,285 admitted series and
asserted against the archive literal by `_resolve_liquidity_policy`. Independently
cross-checkable for **4,546 of 17,285** (26.3%) — the ones with a same-instrument
series under the other vendor. AAPL around its 4:1 split settling 2020-08-31, HF vs
Intrader close, ratio to 6 dp:

```
2020-08-27  125.010  500.04  3.999997
2020-08-28  124.808  499.23  3.999984
2020-08-31  129.040  129.04  1.000000
```

Consistent with `sql/251`'s recorded evidence. Post-split the two agree to the stored
precision, which is *consistent with* the HF archive carrying no dividend adjustment
on a dividend payer.

⚠ Two limits, both raised at checkpoint 1 and neither resolved here: (a) this is one
instrument around one event and does not establish opens across every admitted series
and epoch; (b) both archives are `yahoo_derivative`, which
`research_corpus_ingest.py:176` records as circular rather than corroborating. The
banding decision therefore rests on the **stored per-series basis**, which the
resolver asserts, and not on this comparison. The comparison is a sanity check.

## Sequenced design

Nothing here is built yet. Ordered so that no step can ship a flattering partial.

⚠ **Steps 1-4 are RESOLVED by "Resolved design" below (2026-09-20, later session) and
that section is what gets built.** The open fork in step 2 — *"carry the raw open for
band selection, or declare the comparator conservative"* — is settled: the first option
is not a new rule, it is what the strategy path has always done. Step 5 is unchanged and
still out of scope. Everything below is kept as the record of what was known before.

**Step 1 — one basis decision, three consumers.** A single helper keyed on the
already-resolved `_Corpus.liquidity_policy`:

```python
def _cost_price_basis(policy: ArchivePolicy | None) -> PriceBasis:
    if policy is not None and policy.adjustment_basis == "unadjusted":
        return "as_traded"
    return "split_adjusted"
```

Fail closed: a withheld policy (`None` — no declared provenance, a mixed admitted
set, or a stored basis disagreeing with the archive literal) keeps the maximum band.

⚠ Checkpoint 1 is right that "fail closed" here means only *"charge the maximum
calibrated band"*, not *"refuse"*. An unknown provenance still produces a result,
relabelled `split_adjusted`, and the reason it was unknown is lost. Whether that is
acceptable is a decision for step 1, not an implementation detail.

**Step 2 — the comparator and the control need a NOMINAL price they do not have.**
Both cost on total-return levels. Either carry the raw open alongside the wealth mark
purely for band selection, or state explicitly that the comparator and control keep
the maximum band and that every comparison is therefore conservative in the strategy's
disfavour. ⚠ This is the real design work and it gates steps 3-4.

**Step 3 — `COST_MODEL_ID` moves.** Checkpoint 1 sides with the counter-reading
flagged in the first draft, and it is right: the module rule is that *"a change to
what is charged is a new model"*, the #2833 no-bump precedent depended on nothing
being charged differently, and `backtest_run.py` is not hashed into strategy identity.
Without a bump, a stored row's `cost_model_id` no longer identifies what it was
charged, and `assert_no_existing_results` (`backtest_run.py:3790`) cannot tell an old
result from a corrected one.

**Step 4 — the A/B, redesigned.** The first draft's design was rejected at checkpoint
1 on two counts, both correct: it called `cost_position` directly (a component
comparison, not the shipped runner path — the control was reconstructed), and its
treatment arm hardcoded `as_traded`, so it never exercised the resolver the fix
depends on. The A/B must run the real runner twice under the two bases, compare
**keyed per-leg records** (series/name key, strategy, namespace, both arms — instrument
id alone misses unlinked dead series), and assert that fills, holds, gross returns and
exclusions are unchanged while only the charge moves.

⚠ Metrics must come from the production path. `CostedPosition.net_return_pct` uses raw
prices, while production applies total-return factors and namespace routing in
`_absorb`; reducing costed positions directly would read a split as an investment loss.

**Step 5 — re-run policy is NOT part of the fix.** The 16 stored `survivorship_free`
results become known-stale. Re-running a sealed preregistered trial is a #2829/#2599
declaration decision.

## Also surfaced, not in scope

Recorded so nobody re-derives them:

- `scripts/verify_2900_point_in_time.py:238` asserts the exact two literals this
  change removes, and several other verifiers hardcode `split_adjusted`; some select
  research series with no vendor pin, so the dual-archive multiplicity already
  invalidates their population assumptions.
- `scripts/measure_2827_gross_vs_net.py:240` still narrates "the corpus cannot assign
  bands" without scoping it by basis.
- `s2_cross_sectional_momentum.py:62` and the S-10 notes assume split-adjusted OHLC.
  Raw Intrader split jumps can contaminate signals even where final returns use wealth
  marks. Pre-existing — but cheaper costs must not make those signals look validated.
- `_NamespaceBook.half_spreads` (`backtest_run.py:1178`) documents single-valuedness as
  a property of the corpus. It becomes a set of size > 1 for `survivorship_free`.
  ⚠ Its cardinality is not a valid acceptance test: it records realised included legs
  only, and a correct run may legitimately hold one band or none.
- The band calibration itself remains a small, mostly closing-hour quote sample across
  nine summer dates, applied to next-open historical fills including dead names. A
  correctly selected band is not a validated historical execution cost.

---

# Resolved design (2026-09-20, later session) — steps 1-4

## Source rule

Unchanged from above and still not inferred: `cost_model.cost_band_for`'s own declared
contract — *"`as_traded` may use the price thresholds. `split_adjusted` cannot"*. No new
constant, threshold or window is introduced by anything below.

## The finding that settles step 2

Step 2 was recorded as an open fork because the comparator and the control cost on
total-return levels while the bands are nominal. **The strategy path already resolves
that fork and has since it shipped.** `_absorb` (`app/services/backtest_run.py:1875`):

```python
entry_price = float(row.entry_price_net) * entry_wealth_mark / entry_raw
exit_price  = float(row.exit_price_net)  * exit_wealth_mark  / exit_raw
```

`row.entry_price_net` is the **nominal** t+1 open times `(1 + h)`; the total-return
factor is applied **after**. So production **selects the band from a nominal price and
carries the charge onto a total-return level**, and that is arithmetically sound because
`h` enters multiplicatively — rescaling a price by `wealth/raw` leaves the fractional
charge unchanged.

Consequence: there is no "either/or" in step 2. The comparator and the synthetic control
are the two paths that are **out of step with the shipped semantic**, not the two paths
that need a novel rule. Both adopt what `_absorb` does: band on the nominal price, charge
the ratio, keep the mark on the total-return basis.

⚠ This does **not** make banding a total-return level acceptable anywhere. Neither
consumer will band `entry_wealth` or `adjusted_open`. Both already hold the raw price
they need, one line before they throw it away.

## Step 1 — one selector, four consumers

New in `app/services/cost_model.py`, beside `cost_band_for`:

```python
def cost_price_basis(adjustment_basis: str | None) -> PriceBasis:
    """The cost basis an archive's stored adjustment basis earns."""
    return "as_traded" if adjustment_basis == "unadjusted" else "split_adjusted"
```

Total, no raise, fail-closed by construction: anything that is not a declared
`unadjusted` archive — including `None` — keeps `split_adjusted`, i.e. the maximum band.

⚠ It takes a **plain string**, not an `ArchivePolicy`. `cost_model` must not import the
corpus-ingest layer, and `synthetic_control_run` cannot import `backtest_run` (the
dependency already runs the other way). A string keeps one selector reachable from all
three modules with no cycle.

`_Corpus` gains a resolved field, set once in `load_corpus` from the policy it already
resolves:

```python
cost_price_basis: PriceBasis = "split_adjusted"
```

⚠ **The default is the fail-closed value on purpose.** A construction site that forgets
the field gets the maximum band; reaching the cheaper basis requires an explicit act.
That is the invariant that makes a derived field safe to carry beside its source.

### What "fail closed" costs, stated rather than assumed

Checkpoint 1 was right that fail-closed here means *"charge the maximum calibrated
band"* and not *"refuse"*, and that the reason a run fell back is then lost — stored
columns cannot distinguish a withheld policy from a genuinely split-adjusted universe.

Not fixed by a stored column, and that is a decision rather than an omission:

- Raising instead would convert a data anomaly into the loss of a multi-hour run, and
  the maximum band **is** the cost model's declared treatment for an unknown basis.
- The withheld set is empty by construction today. Full-population on the dev corpus:
  `research_price_series` partitions cleanly by vendor — `icyDenev/Intrader` 22,879 rows
  all `unadjusted`, `paperswithbacktest/Stocks-Daily-Price` 7,693 all `split_adjusted`.
  `_resolve_liquidity_policy` withholds only on a basis disagreement, an undeclared
  vendor, or an empty admitted set.
- What IS added: `_resolve_liquidity_policy` currently warns *"entry-liquidity
  diagnostic withheld"* on one of its three withholding paths and is silent on the other
  two. All three now log, and the message names the cost consequence, because the
  consequence changed — a withheld policy is no longer only a missing diagnostic.

Storing the resolved basis per result row is a migration and is **out of scope**,
recorded here so nobody re-derives it.

## Step 2a — the comparator (`_benchmark_book`, `backtest_run.py:1449`)

The nominal price is already computed and already validated, and is then used for
nothing:

```python
entry_close  = float(window[entry_offset])        # RAW close — nominal on this archive
entry_wealth = float(wealth_window[entry_offset]) # total-return level
```

Change: `half = cost_band_for(Decimal(repr(entry_close)), price_basis=basis).half_spread`,
with `basis` threaded from `corpus.cost_price_basis`. The charge still lands on
`entry_wealth` / `exit_wealth`, unchanged. The comment asserting *"The corpus OHLC is
split-adjusted"* is deleted — it is false for this universe and is the sentence that hid
the defect.

⚠ **An asymmetry to state, not to fix.** A strategy leg bands on its entry **open** (the
t+1 fill); a comparator leg bands on its entry **close**, because the comparator's entry
*is* that close by construction. Banding must key on the price the leg transacts at, so
the two keying prices differ for the same reason the two entries differ. No change.

⚠ `Decimal(repr(...))` and not `Decimal(...)` on a float — the repo's existing idiom two
lines below (`Decimal(repr(entry_wealth))`), and the one that does not carry binary float
noise into a threshold comparison.

## Step 2b — the synthetic control (`synthetic_control_run.py`)

Same shape, same availability. `_eligible_fill_bars` reads the raw open and multiplies it
away on the next line:

```python
bar_open = series.rows[fill_index].get("open")   # RAW open — nominal on this archive
...
adjusted.append(float(bar_open) * wealth_close / raw_close)
```

Changes:

1. `_eligible_fill_bars` takes `price_basis` and returns a fourth parallel list — the
   per-bar `half_spread` selected from `bar_open`.
2. `SeriesPlacement` gains `half_spread: npt.NDArray[np.float64]`, parallel to
   `adjusted_open`; `__post_init__` asserts the parity it already asserts for `panel`.
3. `CohortCollector` takes the run's `price_basis` and passes it down.
4. `np.full(n, _HALF_SPREAD)` becomes `placement.half_spread[entries]` at all four sites
   (`_place_member` ×2, `_place_member_compact` ×2), and
   `SharedMarkLegBook(half_spread=np.full(size, _HALF_SPREAD))` becomes the concatenated
   per-leg array.
5. `_shared_member_inputs` / `_attach_shared_member_inputs` pack and attach a fifth
   array. It reuses `panel_start` / `panel_size` — the dataclass already asserts
   `panel.size == adjusted_open.size`, so the same slice is correct by that invariant.

⚠⚠ **Both sides key on `entries`, never on `exit_slot`.** §5.1 fixes the band on the
entry price and explicitly does not re-key mid-hold (`cost_model.half_spread_for`). The
exit line reads `placement.adjusted_open[exit_slot]` for the *price*, so writing
`placement.half_spread[exit_slot]` beside it is the one-character defect this change most
invites, it would pass every shape check, and it would silently re-key the band on the
outcome bar.

⚠ `SeriesPlacement`'s docstring — *"ONE `adjusted_open` ARRAY AND NOT TWO PRICE ARRAYS …
`h` constant over this corpus"* — records a premise that this change **ends**. It is
rewritten to say why the second array now exists rather than left contradicting the code.
Likewise `_HALF_SPREAD`'s comment (*"the half-spread every leg of this corpus carries"*)
and `_NamespaceBook.half_spreads`'s (*"on the research corpus it is [single-valued]"*).

⚠ Memory. `adjusted_open` is per **eligible fill bar**, not per corpus bar; `marks` (a
full per-series total-return span, ~200 MB over the corpus by its own comment) dominates
the placement footprint. One added `float64` parallel to `adjusted_open` is therefore
expected to be a small fraction — but that is a prediction, so it is **measured** via the
existing `shared_input_bytes` / `_placement_bytes`, reported in the PR, and not asserted.

## Step 3 — `COST_MODEL_ID` moves

From
`static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd`
to
`static-p75-insession-v4+archive-basis-band+carry-fx-structural-zero-long-x1-real-usd`.

The module's own rule is that a change to what is charged is a new model; the `#2833`
no-bump precedent turned on nothing being charged differently, which is not true here.
The `split-adjusted-max` token is additionally now a false description. Without the bump
`assert_no_existing_results` (`backtest_run.py:3790`) cannot tell an old row from a
corrected one.

## Step 4 — the full-population A/B

`scripts/ab_3238_cost_basis.py`. Both arms are the **real runner** over the **full**
admitted `survivorship_free` set — no `limit`, no reconstruction, no direct
`cost_position` call.

- **Treatment** — `load_corpus` as shipped. `cost_price_basis` comes from the resolver,
  so the arm exercises the thing under test rather than a hardcoded `as_traded`.
- **Control** — `dataclasses.replace(corpus, cost_price_basis="split_adjusted")`. This
  touches the cost basis and **nothing else**; in particular `liquidity_policy` is left
  alone, so the entry-liquidity and exit-gap diagnostics are identical across arms and
  cannot be confused with the effect being measured. `cost_price_basis(split_adjusted)`
  is exactly the literal being removed, so the control is the old behaviour reproduced by
  the shipped code path rather than simulated.

Comparison is **keyed per leg**, on `(name_key, strategy_id, namespace, entry_fill_date,
exit_date)` — `name_key` and not `instrument_id`, because an unlinked dead series carries
a negative in-pass key and matching on instrument id alone would drop exactly the
survivorship-relevant half of the corpus.

Asserted identical across arms — the gate is that **only the charge moves**:

- the leg population itself (added / removed must both be 0),
- entry and exit fill **bars**, and hold lengths,
- **gross** returns (`_NamespaceBook.gross_returns`),
- every `excluded` / `uncosted_reason` / `terminations` count.

Measured and expected to move:

- per-leg `half_spread` and the `half_spreads` census cardinality (1 → >1),
- net expectancy per trade and profit factor, per namespace **and per regime cohort**,
- the band mix over realised **legs** — the per-trade figure the census could not give,
  which is the number this ticket has owed since it was filed.

Metric: **distinct legs and distinct name keys**, never row counts. The **gain side is
inspected** — a sample of legs whose band moved off `<$5` is read against the archive's
raw open for that bar, to confirm the band was selected from a price that is actually
nominal and not from a wealth mark.

⚠ Metrics are read from the production path (`_measure_namespace` / the books), never by
reducing `CostedPosition.net_return_pct` — that field uses raw prices, so reducing it
directly would read a split as an investment loss.

⚠ Expected direction, declared before the run: net expectancy **rises** on the treatment
arm for the strategy, its benchmark **and** its null, because all three are overcharged
today. A run in which the strategy improves against an unchanged benchmark is a **failed**
A/B — it would mean a consumer was missed — and not a promising result.

## Step 5 — unchanged

The 16 stored `survivorship_free` results become known-stale. Re-running a sealed
preregistered trial is a #2829/#2599 declaration decision and is **not** part of this fix.

---

# Checkpoint 1 on the resolved design — 37 findings, and what each changed

Run against the "Resolved design" section above. Triaged rather than accepted
wholesale; the ones that changed the build are marked **CHANGED**.

## Corrections to the spec's own claims

**CHANGED — the central claim was overstated and is now narrowed.** The spec said
production *already selects a nominal band*. It does not: both call sites pass
`split_adjusted` today, so what `_absorb` proves is that the `wealth/raw` rescale is
CHARGE-PRESERVING, not that the band-selection policy is already shipped. Checkpoint 1
supplied the algebra that does settle the fork — with `Fₜ = wealth_closeₜ/raw_closeₜ`,
net entry is `openₑ·Fₑ·(1+h)` and net exit is `exitₓ·Fₓ·(1−h)`, so
`1+R_net = (1+R_gross)·(1−h)/(1+h)` **even when the two factors differ**. The charge is
therefore basis-invariant under the rescale and banding-on-nominal is arithmetically
coherent. That, not "production already does it", is the argument.

**Independently traced and confirmed:** Intrader ingest stores CSV open/close unchanged,
`_apply_arm` only masks, `_to_series` copies, `_dense_price_history` converts to float.
Nothing adjusts the comparator's or the control's inputs upstream, so "raw" is archive
raw. "Nominal" still rests on the stored per-series basis, as the spec already said.

**Citation corrected:** `archive_policy_for` lives in `strategy_entry_liquidity.py`, not
where the spec implied; `_placement_bytes` does not exist — the accounting is the inline
sum in `_shared_input_bytes`.

## Defects in the plan, fixed before any of it was written

**CHANGED — a fifth charge site was missing.** The spec listed the four price-array
sites and missed `_place_member`'s `book.add(half_spread=_HALF_SPREAD)`. `LegBook`'s
half-spread is charged AGAIN on every rebalance by `build_equity_curve`, so leaving it
scalar would have made the scalar and compact paths agree on trade returns and disagree
on portfolio metrics — the hardest class of bug to attribute. Now per leg, and
`test_the_scalar_and_compact_paths_agree_under_varying_bands` covers it.

**CHANGED — `SeriesPlacement` needed more than size parity.** It did not require
one-dimensional arrays, and nothing enforced `cost_model`'s `0 < h < 1` on the NumPy side
(the `Decimal` guards are upstream of the placement). `__post_init__` now checks size
parity against `panel`, dimensionality, and finite `h` in `(0, 1)`.

**CHANGED — `_shared_input_bytes` had to learn the fifth array**, or the pilot and canary
would under-report the memory they exist to bound.

**CHANGED — `_eligible_fill_bars` validated its inputs but not their product.** A finite
open over a tiny raw close can overflow the carried price; an infinite entry price would
have reached the curve rather than raised. The carried value is now `_usable`-checked.

## Full-population answers to two open questions

**`Decimal(repr(float))` at a band edge — measured, not argued.** The comparator bands
from a float array, so a stored value within a double-epsilon of 5/20/100 could round
across. Max scale on `research_price_daily` is **22 decimal places over 75,972,669
rows**, which looks alarming until the values are read: `32.54649353027344` is the exact
decimal expansion of a **float32**. These NUMERICs *originate* as floats, so the
`Decimal → float64` conversion is exact.

```sql
select count(*) filter (where open_at_risk), count(*) filter (where close_at_risk), count(*)
from (select (open <> 5 and abs(open - 5) < 1e-9) or (open <> 20 and abs(open - 20) < 1e-9)
           or (open <> 100 and abs(open - 100) < 1e-9) as open_at_risk, ... from research_price_daily) t
--  (0, 0, 75972669)
```

**0 of 75,972,669** open/close values sit within `1e-9` of a band edge without being
equal to it. ⚠ The synthetic control does not have this exposure at all — `OHLCVRow.open`
is a `Decimal` and `cost_band_for` receives it unconverted.

**The `COST_MODEL_ID` bump rotates every strategy identity** (`entry.identity(...,
cost_model_id=COST_MODEL_ID)`), which the spec had not costed. Measured with the existing
`scripts/census_3031_identity_rotation.py` — built for exactly this question, so it was
reused rather than rewritten:

| table | rows | on current identity (what a rotation detaches) |
|---|---:|---:|
| `strategy_signals` | 59,135 | **424** |
| `strategy_signals` (fired, unresolved) | 58,413 | **362** |
| `strategy_scan_watermark` | 35 | **3** |
| `strategy_results_store` | 580 | 0 |
| `strategy_preregistration_declarations` | 7 | 0 |
| `strategy_holdout_accesses` | 560 | 0 |
| `strategy_deployments` / `strategy_promotions` | 0 | 0 |

⚠ **The three registry-hashed frozen declarations are ALREADY detached** — declared
`mt1 …+32970feefa00`, `mt1-s8 …+b83c3e4fc997`, `s11 …+d5f25fd08376` against current
`+5d6f02df1b74` / `+bd7c7f003679` / `+0ab5cc7895a3`. The other four carry hand-named
versions with no registry hash and cannot move. So the bump's marginal cost to the
declaration register is **zero**; that they are already detached is a pre-existing
condition and not this change's doing.

⚠⚠ **It cannot pause the live demo sleeve.** `strategy_paper_runtime` selects paper
candidates by `entry.purpose == "capital_candidate"`, and after #2845 retired the dead
eight **every manifest strategy is `harness_validation`** — the filter yields an empty
version list today. `strategy_position_ownership` keys on `strategy_trade_id`, not
`strategy_version`, so the SPY.RTH position is untouched either way. The real marginal
cost is the 362 unresolved harness-validation fills that leave `run_outcome_resolution`'s
current-identity drain, and the 3 scan watermarks that restart.

## Accepted and applied to the A/B

Keyed on `(quarantine, ambiguity)` as well as namespace (identical name/date tuples recur
across arms); per-leg comparison by **element-wise `gross_returns` equality** over the
whole ordered tuple rather than a set difference, which also catches reordering and
duplicate-value swaps; exclusions compared via `position_count` (which counts uncosted
positions) plus `termination_census` and the label windows rather than a bare total;
`half_spreads` cardinality reported and **not** gated on, since a correct run may hold one
band or none; the runner driven at `evaluate_level_arms` / `evaluate_arm` — `run_backtest`
loads its own corpus and persists, so it is not an injection point; the `gross_returns`
equality doubles as the detector for bar data moving between arms.

## Accepted and recorded as a scope bound, not built

- **`evaluate_arm`'s call site is not exercised by any survivorship-free run.** A
  non-empty termination map routes every strategy through `evaluate_level_arms`, and
  `evaluate_arm` explicitly refuses such a corpus. The `:2601` fix is therefore
  correct-but-dormant: it is reachable only on `survivor_only`, which is split-adjusted
  and cannot change basis. The A/B prints `evaluate_arm_site_exercised: false` rather
  than implying coverage it does not have.
- **The same model id still cannot distinguish a resolved basis from a withheld one.**
  Storing the resolved basis per result row is a migration and stays out of scope; the
  three withholding paths now all log, which is the bounded half of the answer.
- **`band_crossings` is still never called in production.** Nominal banding makes that
  diagnostic meaningful for the first time. Noted, not wired.
- **The band calibration remains a small, mostly closing-hour quote sample** across nine
  summer dates. A correctly selected band is not a validated historical execution cost,
  and nothing here changes that.
- **Raw split jumps can still contaminate signals** on the unadjusted archive
  (`s2_cross_sectional_momentum.py:62` and the S-10 notes assume split-adjusted OHLC).
  Pre-existing; cheaper costs must not be read as validating those signals.

## Rejected

- *"Cardinality must move 1 → >1"* was in the first draft and is wrong as a gate; it is
  now a reported diagnostic. Checkpoint 1 is right that a benchmark can legitimately keep
  one band while the strategy's opens span several.
- *"Retain the original `Decimal` in the comparator"* — declined on the measurement above.
  `_dense_price_history` builds a compact `array("d")` deliberately, and carrying
  `Decimal`s through it to close a gap measured at zero occurrences would cost the memory
  that array exists to save.
