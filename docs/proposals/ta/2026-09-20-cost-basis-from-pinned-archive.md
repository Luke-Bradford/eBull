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
