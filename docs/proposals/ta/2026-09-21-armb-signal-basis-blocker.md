# ARM B blocker 3 — s2's momentum score is basis-sensitive on the survivorship-free corpus

Refs #2834. Part of #2832. Queue: #2437 phase 2.

**Verdict: ARM B's weighting prototype is BLOCKED on a third blocker. Unlike step
0's share coverage and step 1's kernel, this one is a live data-treatment defect.**

Reproduce: `PYTHONPATH=. uv run python -m scripts.measure_2834_armb_signal_basis`
(exits 1, by design — see §6).

## 1. What this session set out to do, and why it stopped

#2834's ARM B step-1 verdict (merged `2e76a42d`) named one unblock: a vectorized
monthly-return prototype comparing equal / dollar-volume / rank weights, to
decide whether per-name weighting in the kernel is worth building. Its own
requirement is that the prototype replicate **s2's signal exactly** — otherwise
it measures selection drift instead of weighting.

Checking that the signal is well-defined on the corpus ARM B runs against —
before writing the prototype, per working-order 3b — found that it is not.

## 2. The finding

`app/services/strategies/s2_cross_sectional_momentum.py:61-66` scores on `close`
and justifies it as a **source rule**:

> ⚠ PRICE RETURNS, NOT TOTAL RETURNS — §4 says so explicitly, and the corpus
> agrees by construction: `research_price_daily.close` is the SPLIT-adjusted
> close that is consistent with OHLC, while the dividend-adjusted series lives in
> `adj_close` (sql/251).

That sentence is **true for the vendor `sql/251` measured** — the HF archive
`paperswithbacktest/Stocks-Daily-Price` — and **false for the vendor s2 runs
on**. `BACKTEST_UNIVERSE = "survivorship_free"` pins `icyDenev/Intrader`
(`app/services/universe_selection.py:68`), and
`app/services/research_corpus_ingest.py:165-171` measured that archive as the
opposite:

> ⚠ `unadjusted` is MEASURED, and it is the opposite of what #2398 recorded for
> the same vendor … this archive's OHLC carry NEITHER the split nor the dividend
> adjustment, and its ninth CSV column — stored as `adj_close` — carries both.
> **Consumers computing returns must read `adj_close` (#2400)**; `close` here is
> the raw traded level.

Confirmed on the bars rather than by reconciling two docstrings — AAPL across
its 4:1 split of 2020-08-31:

| vendor | basis | bar_date | close | adj_close |
| --- | --- | --- | ---: | ---: |
| `icyDenev/Intrader` | `unadjusted` | 2020-08-27 | **500.04** | 122.169117 |
| `icyDenev/Intrader` | `unadjusted` | 2020-08-31 | **129.04** | 126.107534 |
| `paperswithbacktest/…` | `split_adjusted` | 2020-08-27 | 125.01 | 121.256416 |
| `paperswithbacktest/…` | `split_adjusted` | 2020-08-31 | 129.04 | 125.165390 |

A momentum score is a return, so on this vendor a corporate action falling inside
`t-252 .. t-21` **rescales the score's gross ratio by the adjustment factor**.

⚠ **The direction is not one-sided.** A first draft of this document claimed the
error "has a sign" because companies split after a run-up. That is wrong: a
forward split *divides* the raw gross return and a **reverse** split *multiplies*
it — a 1-for-4 reverse inflates an otherwise unchanged name's gross return by 4×,
which promotes a non-winner. So names are displaced in both directions, and
"winners are suppressed" is not established for this population.

Neither document is wrong on its own. `sql/251` verified its claim on the HF
archive; s2 cites it as a source rule; the backtest universe is the other vendor,
where ingest measured the opposite. The two facts have never met.

## 3. Blast radius — full population

`scripts/measure_2834_armb_signal_basis.py` scores s2's rule on both bases over
one shared cross-section and compares top-decile membership. Admission is the
settled `survivorship_free` rule (`universe_selection.load_universe_selection`),
**17,285 series** admitted. 1994-01-01 → 2021-06-29, **312 formations of 329
calendar months**, 121,601 decile slots:

| decade | formations | mean cross-section | decile slots | displaced | pct |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1990 | 71 | 1,872 | 13,268 | 2,753 | 20.75% |
| 2000 | 107 | 3,258 | 34,820 | 4,065 | 11.67% |
| 2010 | 116 | 5,497 | 63,709 | 9,381 | 14.72% |
| 2020 | 18 | 5,451 | 9,804 | 2,039 | 20.80% |

**Pooled: 15.00%** (18,238 of 121,601). Worst formation 1998-09-01 at 39.73%.
Arm asymmetry 0, i.e. the shared-cross-section invariant held.

⚠ 17 of 329 months produced no formation (no weekday bar from the admitted
corpus, or a cross-section below `MIN_CROSS_SECTION`). Stated rather than left
inside the phrase "full population".

⚠ Displacement is **slot-weighted** — `Σ displaced / Σ decile slots` — so a large
cross-section counts for more than a small one. Equal-per-month weighting answers
a different and equally legitimate question; the pooled figure is the one that
answers "how much of the decile is basis-dependent".

⚠ An earlier version of this measurement ran on every series of the vendor rather
than the settled admission, and reported 14.94%. The correction moved it to
15.00% — worth recording because the near-identity is itself evidence the result
is not an artefact of the universe cut.

## 4. What this does NOT claim

- **The cause is not decomposed.** An earlier draft attributed each displaced
  name to "split-scale" or "dividend-scale" by the change in its own
  `close/adj_close` factor, and reported ≈8.2% as split-attributable. **That
  figure is withdrawn.** The attribution is observational, not causal: a name's
  decile membership can change because a *competitor's* score moved, so an
  entirely unadjusted name displaced by a split-corrected rival was being
  labelled "dividend-scale". The threshold arithmetic was also wrong (for a cash
  distribution fraction `y` the factor gap is `1/(1−y)`, so a 1.5 gap is a 33%
  distribution, not 50%), and small stock dividends and small splits (5:4 → 1.25)
  both land inside the band assumed empty. Identifying the cause needs
  corporate-action evidence, not endpoint ratios.
- **Not that s2's stored results are wrong by any stated amount.** No stored
  result was recomputed. This measures *selection*, not return.
- **Not an attribution of s2's negative hold-out expectancy.** The pinned figures
  (#2834 step 1: −5.2% to −8.6% per trade, PF 0.33-0.66) are consistent with this
  contributing and equally consistent with it not.
- **Not a claim that `adj_close` is the correct basis.** It is not — it is a
  *total* return, which §4 explicitly rejects. The correct basis is a split-only
  adjustment, and **this vendor does not store one**. The two arms are a
  sensitivity, not a bracket: dividends can create *or* cancel a disagreement.
- **Not measured beyond s2.** Any strategy scoring a multi-month price ratio on
  `close` over `survivorship_free` has the same exposure. Not measured here, and
  deliberately not asserted.

### Declared fidelity limits

This is a basis-sensitivity comparison, not a backtest replication. Both arms
carry these deviations identically, so they do not affect the arm-vs-arm
difference, and none is claimed harmless in absolute terms:

1. **Weekends dropped before the positional lags.** s2's `rebalance_dates` drops
   them from the rebalance calendar (#2797) while its loader keeps them in bar
   history, so `lag(close, 21)` spans a different elapsed time there than here.
2. **No segment resets** — production evaluates through
   `strategy_segmented_evaluation.segmented_member`, which restarts warm-up at an
   unresolved price break and refuses segment-final bars.
3. **Postgres `numeric` arithmetic**, not Python float; the two can disagree at a
   tie.
4. **Reads are not window-bounded** — `n_bars` and the lags read each series'
   whole history. Only formation dates are bounded.
5. **Shared support is a restriction**: a name scoreable on raw close but not on
   the adjusted series is dropped from both arms rather than counted as a
   disagreement.

## 5. Why it blocks ARM B

ARM B's prototype must replicate s2's signal or it measures selection drift
rather than weighting. With 15% of the decile basis-dependent, "replicate s2's
signal" is not a well-defined instruction without also naming the price column,
and the two available columns give materially different portfolios.

⚠ **This is a scope limit, not an impossibility, and the distinction matters.** A
weighting comparison *conditional on the current signal* remains identifiable —
all three weight vectors would see the same contaminated selection. What it could
not support is the generalisation ARM B actually wants ("does weighting help
12-2 momentum"), because that claim must survive a change of signal basis. A
prototype run now would answer a question about `close`-scored s2 specifically.

⚠ The suspicion that the defect **interacts** with the weighting treatment —
because rescaled names may skew larger and more liquid, differentially exposing
the dollar-volume arm — is **asserted, not measured**. It is stated here as the
reason to settle the basis first, not as a finding.

## 6. The script and its tests

`scripts/measure_2834_armb_signal_basis.py`. Selection is s2's throughout, reused
from `scripts/verify_2240_s2_cross_sectional.py`'s `_RANKING_SQL` (already
equivalence-tested set-for-set against the module); admission is
`universe_selection`'s settled rule; the quarantine join pins `rule_set_version`
on both the coverage and the verdict table; the tie-break is the engine's own
`AdmittedSeries.name_key`.

Exit codes: `0` no material displacement, `1` displacement above
`_DISAGREEMENT_BAR_PCT` (1.0%, by construction and deliberately low — the arms
differ only by corporate actions, so any large disagreement *is* the result),
`2` nothing measurable.

Pure-logic tests: `tests/test_2834_armb_signal_basis.py` — the divide-by-zero
guards, the month denominator, the asymmetry invariant, and that the pooled
figure is slot-weighted rather than a mean of per-formation percentages (the
cross-section grows ~3× across the window, so the two disagree on real data).

## 7. Recommendation

**The next step is not the prototype.** It is to settle the signal basis for
`survivorship_free`, which is a bounded data question with three candidate
answers, all evidence-gated and none person-gated:

1. derive a split-only series for the Intrader vendor from corporate-action
   evidence (⚠ *not* from `close/adj_close` endpoint ratios — §4 explains why
   that cannot identify splits);
2. pin the survivor-only vendor, whose `close` genuinely is split-adjusted, and
   accept the survivorship cost with the label it already carries; or
3. record that §4's price-return rule cannot be honoured on this corpus and
   change the rule.

⚠ **The prototype's own design is NOT settled and must not be inherited as if it
were.** A spec was written this session and withdrawn after Codex checkpoint 1;
these findings remain unanswered and change the design rather than the prose:

1. **The estimand was wrong.** `excess_w − excess_equal` is algebraically
   `(R_decile,w − R_decile,equal) − (R_universe,w − R_universe,equal)`, so a
   weighting benefit common to both portfolios cancels to exactly zero, and an
   unchanged long leg can "separate" solely because its benchmark moved.
2. **Rank weighting over the full universe is undefined** — `N_sel + 1 − rank_i`
   outside the decile yields zero or negative weights, and a rank-weighted
   benchmark already contains the signal under test.
3. **Zero-weighting a delisted name does not conserve wealth.** The coherent form
   is `R = Σ w_initial × holding_return` with terminal proceeds carried as cash,
   and `app/services/series_termination.py::terminal_value_fraction` is the
   settled termination treatment (two-armed ambiguity classes that *raise* rather
   than silently pick a side).
4. **`MIN_CLUSTERS` is 2**, not an adequacy threshold; and
   `block_bootstrap_expectancy` returns `None` on zero variance, so a constant
   non-zero separation must not read as no separation.
5. **Dollar volume summed over "the formation month" is look-ahead** — the
   rebalance is the month's *first* bar.
6. **Non-rejection must not close ARM B** — the rule had no power requirement and
   no inconclusive state.
7. **Statistical significance is not materiality**, and sign-agreement across
   cohorts can veto a real regime-dependent effect.
