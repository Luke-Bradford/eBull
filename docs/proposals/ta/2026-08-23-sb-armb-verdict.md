# S-B ARM B (12-2 replication) — VERDICT: blocked as cost-unjustified, not killed. Cheap test path named.

Issue #2834. Part of #2832. Refs #2437 (R5b phase 2), #2827, #2845, #2426, #2840.

Step 0 (`d391df05`) closed with *"the replication itself … is the next step, dollar-volume
weighted"*. This document is the premise re-falsification working-order step 3c requires
before building on an inherited conclusion. It does not reach that step, but **it stops
short of killing the weighted replication** — an earlier draft did kill it, and Codex
checkpoint 1 was right that the evidence does not support that.

No new trial was charged to the register. Everything below is a read of stored rows and of
the engine source.

---

## 1. ARM B's rule is s2's rule. The "12-2 vs 12-1" difference is nominal.

`app/services/strategies/s2_cross_sectional_momentum.py` is *"cross-sectional momentum
(12-1)"*, ranking on `close(t-21) / close(t-252) - 1`, top decile, monthly rebalance, fill
at `open(t+1)`.

ARM B is specified as *"12-2, monthly formation, 1-month skip, top-decile long, monthly
rebalance"*. **"12-2" and s2's "12-1" name the same window**: both cumulate eleven months
and skip the most recent one. That is Fama-French's prior (2-12) momentum return, which
s2's docstring already cites to the Ken French library construction note and to Jegadeesh
& Titman (1993).

⚠ Stated explicitly because a reader seeing "12-2" and "12-1" in two ticket comments will
otherwise count two lookbacks searched where one was.

| ARM B delta | status |
| --- | --- |
| formation window 12-2 | **identical** to s2's 12-1 (same window, different convention) |
| survivorship-free universe | **already** — `backtest_run.py:295`, `BACKTEST_UNIVERSE: Universe = "survivorship_free"` |
| long span | **already** — stored rows span the corpus, `1962-01-02 → 2026-07-08` |
| **value / dollar-volume weighting** | **the only real delta — §2 and §4** |

## 2. The production engine cannot express per-name weights — but that blocks the BUILD, not the TEST

Step 0 measured the *data* blocker (PIT shares-outstanding: 7.7% pooled series coverage,
0/320 formations clear 80%, 2000-2008 exactly 0.0%) and concluded the dollar-volume
fallback binds. **The engine blocker is independent of that and was not measured:**

- `equity_curve.py:184` — `LegBook`, the columnar trade list. Columns are `entry_index,
  exit_index, entry_price, exit_price, half_spread, realised, mark_offset, marks`. **No
  weight, size or quantity column**, and `add()` raises on shapes it does not recognise.
- `equity_curve.py:391` — `_build_realised_shared_curve_kernel` allocates
  `target = equity_ref / basket`. **Equal weight is arithmetic inside the compiled
  kernel**, not policy above it.
- `build_capped_target_exposure_curve` is the apparent exception and is not: it rebalances
  *"uniformly to the new aggregate target"*, a scalar in `[0, 1]` for the whole sleeve.

So a value-, cap-, dollar-volume- or score-weighted portfolio needs a weight column, a
weight vector through the kernel, a fifth sizing-rule id, and the strategy →
signal-ledger → book path to carry a weight from formation. That is a cross-cutting change
to the most performance- and correctness-sensitive path in the harness — the one that
produced every stored result.

⚠ **What this does NOT justify** (Codex ckpt-1 corrected an earlier draft here): "the
production engine cannot express it" is not "it cannot be tested". A weighted 12-2
cross-sectional portfolio can be evaluated as **monthly portfolio returns in numpy**,
which needs formation-date ranks and a weight vector and never touches `LegBook`, the
kernel, fills, half-spreads or the trade ledger. That prototype is the cheap instrument,
and §5 makes it the recommendation.

## 3. The stored equal-weighted hold-out evidence — pinned, and it is one history, not nineteen

⚠⚠ **The pinning is load-bearing and an earlier draft got this wrong.**
`strategy_results_store` holds successive re-measurements of the same window under
evolving policy. Three rows exist for calendar-2023 alone, differing in
`trial_register_version` (`2026-08-07` / `-08-12-r5` / `-08-15-r7`), `return_basis`
(`raw-close-price-return-v1` vs `split-dividend-adjusted-wealth-v1`),
`ambiguity_rule_version` and `fx_unmodelled`. **An unpinned query pools them and reports
each as separate evidence.** The `return_basis` difference alone flips signs: the
superseded raw-price rows show positive expectancy on several windows, because the
strategy was measured on price returns while the benchmark accrued dividends.

Pinned to current policy — `benchmark_rule = 'equal_weight_buy_and_hold_v1'` (§6),
`trial_register_version = 'trial-register-2026-08-15-r7'`,
`return_basis = 'split-dividend-adjusted-wealth-v1'`, `worst_case` / `masked`:

| window | n | clusters | exp %/trade | 95% CI | PF | turnover | vs buy-and-hold |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 2021-09-28 → 2024-09-27 | 4,812 | 36 | −8.588 | [−11.13, −5.90] | 0.490 | 5.22 | −37.25 |
| 2022-01-01 → 2024-09-27 | 4,277 | 32 | −7.343 | [−10.39, −4.64] | 0.536 | 5.08 | −23.55 |
| 2022-09-28 → 2024-09-27 | 3,088 | 24 | −7.624 | [−11.76, −4.18] | 0.545 | 5.41 | −55.27 |
| 2023-01-01 → 2023-12-31 | 1,488 | 11 | −14.234 | [−16.14, −12.00] | 0.232 | 5.17 | −42.99 |
| 2024-01-01 → 2024-09-27 | 779 | 8 | −9.385 | [−16.53, −5.15] | 0.480 | 5.14 | −39.03 |

```sql
select namespace, window_start, window_end, trade_count, bootstrap_cluster_count,
       expectancy_per_trade_pct, expectancy_ci_low_pct, expectancy_ci_high_pct,
       profit_factor, turnover_annualised, return_vs_buy_and_hold_pct
  from strategy_results_store
 where strategy_id = 's2-cross-sectional-momentum'
   and benchmark_rule = 'equal_weight_buy_and_hold_v1'
   and trial_register_version = 'trial-register-2026-08-15-r7'
   and return_basis = 'split-dividend-adjusted-wealth-v1'
   and quarantine_arm = 'masked' and ambiguity_arm = 'worst_case'
 order by namespace, window_start, window_end;
```

**How to read the count honestly.** These are **five views of ONE hold-out history**,
2021-09-28 → 2024-09-27, about three years and ~36 bootstrap clusters. The three rolling
windows contain the two calendar years; only 2023 and 2024 are mutually disjoint. Any
phrasing like "N of N windows agree" is counting the same market months repeatedly and
must not be used — this document's first draft said "19 of 19" and that was wrong twice
over (unpinned, and overlapping).

What survives that correction is stronger than a count: **the bootstrap CI on
`expectancy_per_trade_pct` is wholly below zero on every view**, on the metric the
standing rules make decisive, at turnover 5.08–5.41. That is the same bar #2840 arm 1 was
killed on.

⚠ `HOLDOUT_BOUNDARY` is `2021-06-29`, but the earliest registered window starts
`2021-09-28`. The intervening quarter is covered by no registered window; that is a gap in
the window registry, not evidence either way.

⚠ `window_start` / `window_end` record the **corpus evaluation span for the full-corpus
rows, not the namespace's own span** — `hold_out` also carries a `1962-01-02` row, and
positions are routed to a namespace by entry date. An `in_sample` row spanning to 2026 is
not an in-sample window containing the hold-out.

## 4. What this does NOT establish — three inferences deliberately not drawn

Codex checkpoint 1 attacked the framing and was right on all three. Recorded so the next
session does not re-derive the stronger claim and act on it.

1. **It does not prove the family was overfit.** No evidence is offered here that s2's
   window or decile was *selected* on the 1962-2021 sample. The trial register does record
   uncounted pre-ledger parameter development that chose S-1..S-4's windows and thresholds
   (`trial_register.py`, the FLOOR note), so researcher degrees of freedom are documented
   rather than absent — but three years of poor hold-out is equally consistent with regime
   decay or a momentum crash, and this evidence cannot separate them.
2. **It does not kill the weighted arm by implication.** "Equal weighting is the flattering
   construction, so a fail there kills value weighting too" is **invalid as stated**.
   Hou/Xue/Zhang's 65-82% is an aggregate base rate, not a monotonicity theorem, and their
   design confounds NYSE breakpoints with value weighting. Momentum is a plausible
   exception. Equal weighting can be the *less* flattering construction when the small end
   carries wider spreads, staler prices, delisting losses and noisier ranks — and s2's
   universe floor admits exactly those names. Dollar-volume weighting could improve net
   returns by shifting toward tradeable names. **The direction is empirical and has not
   been measured.**
3. **It does not establish inferior risk-adjusted value.** `return_vs_buy_and_hold_pct` is
   a cumulative-return comparison between a partially invested long-only top-decile sleeve
   and a fully invested benchmark, with no beta, exposure or drawdown matching.

⚠ One asserted mechanism was cut from an earlier draft for lack of measurement: *"dollar
volume spikes on exactly the names the momentum signal selects"*. Plausible, unmeasured,
and the formation window skips the most recent month while the weighting measurement
window is unspecified — so it is not even well posed yet. If the §5 prototype runs, measure
it rather than assuming it.

## 5. Verdict and recommendation

**ARM B is BLOCKED as cost-unjustified in its production form. It is not killed.**

1. **Do not build per-name weighting into the production kernel for this ticket.** The
   change is cross-cutting on the hot path that produces every stored result, and it is
   being proposed to test a construction with no measurement behind it yet.
2. **The equal-weight implementation does not support promotion.** Pinned hold-out
   expectancy CI is wholly below zero on all five views (§3). s2 is already retired under
   #2845; nothing here argues for reinstating it.
3. **The weighted replication itself is UNTESTED, and the cheap instrument is a
   standalone vectorized monthly-return prototype** — formation ranks × weight vector,
   monthly rebalance, no `LegBook`, no kernel change, no fills. Compare equal / dollar-volume
   / rank weights against a **matched-weight** benchmark on one preregistered window. Only
   if a weighted arm separates materially from equal weight does the production kernel work
   become justified, and only then does it need a declaration.
4. If the prototype does not separate, ARM B closes on evidence rather than on the
   argument-by-implication this document declines to make.

**This closes nothing about ARM A**, which remains on the five-trading-day quote clock
restarted by `13e00540`.

## 6. ⚠⚠ Two stored rows for the same strategy, span and trade count disagree 7,255× on the benchmark

Found while reading §3; it inverted a draft conclusion before the correct row was
identified. `s2`, `in_sample`, same span, same `n = 19,916`, same expectancy:

| result_id | created_at | `benchmark_rule` | `buy_and_hold_return_pct` | `return_vs_buy_and_hold_pct` |
| ---: | --- | --- | ---: | ---: |
| 95 | 2026-08-08 11:23 | `equal_weight_concurrent_v1` | 29,751,408.21 | −27,175,064.49 |
| 123 | 2026-08-08 19:37 | `equal_weight_buy_and_hold_v1` | 4,100.95 | **+2,572,242.77** |

Row 95 is the **#2426 defect**, documented at the call site in `equity_curve.py`:
*"`equal_weight_concurrent_v1` re-imposes equal weight on every event date, and a
comparator that rebalances is not buy-and-hold. Measured on the full population, that
inheritance added 23.2 points of annual return and turned over 137,477,862× the pot."*
Row 123 (later, correct rule) is authoritative.

**The stale rows were never retracted, so the store holds both, and the sign of the
conclusion flips between them.** Any query against `strategy_results_store` that does not
filter `benchmark_rule` can silently read either. Recorded rather than ticketed, per the
loop's no-new-audit-ticket rule.

## 7. Reusable lessons

1. **`turnover_annualised` is pot-turnovers per year, not a percentage.**
   `strategy_statistics.py:487` — `traded / 2.0 / mean_equity / years`, halved so `1.0`
   means the pot turned over once. The Novy-Marx/Velikov ~50%/month bar is therefore
   `turnover_annualised ≈ 6.0`. s2 at 5.08–5.41 is **inside** it (~42-45%/month), so
   turnover is not what fails this family. ⚠ #2840 recorded *"the units are not established
   here"* and declined to compare s11's 1.267 against the bar; the field now carries them.
2. **A stored-result query without version pinning is not a measurement.** Pin
   `benchmark_rule`, `trial_register_version` and `return_basis` at minimum. Unpinned, the
   same window returns rows from three policy generations whose *signs disagree*, and the
   superseded `raw-close-price-return-v1` basis flatters the strategy against a
   dividend-accruing benchmark.
3. **Overlapping windows are not independent replications.** `primary-2022-plus`,
   `rolling-36m` and `rolling-24m` all contain the calendar years. Report the
   non-overlapping decomposition and the cluster count, never a "N of N" tally.
4. **"The engine cannot express X" is a build constraint, not a test constraint.** Check
   whether the question can be answered by a prototype outside the production path before
   costing the production change.
5. **Before building a capability for a strategy arm, check the engine can express the arm
   at all.** Step 0 measured the data blocker thoroughly; the engine blocker was one grep
   away throughout.

## 8. ⚠⚠ A claim in `strategy-evidence.md` and `.claude/CLAUDE.md` is falsified by §3

Both files carry, verbatim:

> *"Four of those independently predicted, ex ante, the ranking our own backtest produced
> — **s2 inside the turnover bar and the only one beating buy-and-hold**, s1 12× over it,
> s3 6.7× over."*

The turnover half is right and is now quantified (§7.1). **The "only one beating
buy-and-hold" half is false on the current corpus.** It was most likely written against
the pre-rebuild store in early August, when the reachable comparison was the in-sample
one and #2426's benchmark defect was still live.

It matters because that sentence is the standing reason a reader would treat s2's family
as the promising one — which is the premise ARM B inherited.

### ⚠⚠ 8.1 — this section said "all five hold-out views" and that is itself wrong (re-measured 2026-08-23)

Re-run under working-order 3c rather than inherited. Pinning every key of
`current_identity_pins()` (which fixes `namespace = 'hold_out'`) returns **six** distinct
windows for s2, not five — §3's table silently omits the 2022 calendar year, and that is
the one window where the sign flips:

| window | `return_vs_buy_and_hold_pct` best / worst case | `expectancy_per_trade_pct` best / worst |
| --- | ---: | ---: |
| 2021-09-28 → 2024-09-27 | −26.55 / −37.25 | −6.46 / −8.59 |
| 2022-01-01 → 2024-09-27 | −11.76 / −23.55 | −5.22 / −7.34 |
| **2022-01-01 → 2022-12-31** | **+13.35 / +9.59** | **−7.02 / −8.60** |
| 2022-09-28 → 2024-09-27 | −44.07 / −55.27 | −5.19 / −7.62 |
| 2023-01-01 → 2023-12-31 | −38.84 / −42.99 | −12.39 / −14.23 |
| 2024-01-01 → 2024-09-27 | −33.08 / −39.03 | −5.98 / −9.38 |

`quarantine_arm` (`masked` / `admitted`) does not move
`return_vs_buy_and_hold_pct` on any of the six; only `ambiguity_arm` does.

**The headline falsification survives — s2 is not "the only one beating buy-and-hold" —
but the repair sentence must not carry a second unmeasured quantifier.** This file's own
repo rule applies to itself: *"if a sentence contains 'most', 'usually', 'every',
'always' or 'rarely' about source data, it is a measurement. Run it or delete the
quantifier."*

⚠⚠ **And the 2022 row is the more useful finding, not a footnote.** It is the bear year;
s2 beat buy-and-hold there by **losing less**, while still posting −7.0% / −8.6% per
trade. `return_vs_buy_and_hold_pct` and `expectancy_per_trade_pct` **disagree in sign on
that window**. Relative outperformance in a falling market is not edge, which is exactly
why `cost-aware-viability.md` bans CAGR/Sharpe/Sortino as decision metrics — and
buy-and-hold comparison belongs on that banned list for the same reason. It is a
narrative metric that reads as a verdict.

### Correction to apply when a session can write to `.claude/**`

Strike *"and the only one beating buy-and-hold"*, and replace with:

> *"s2 inside the turnover bar. ⚠ Its buy-and-hold comparison is NOT a decision metric —
> negative on 5 of 6 pinned hold-out windows and positive on the 2022 calendar year
> (+13.35 / +9.59) where per-trade expectancy is nonetheless −7.02% / −8.60%. Beating a
> falling benchmark by losing less is not edge (#2834)."*

⚠ **Still not corrected in the skill: `.claude/**` remains unwritable headless** — sixth
occurrence. **The cause is now measured; see #2403** and the prevention-log entry
"`.claude/**` writes fail from a linked worktree in two distinct classes". Short form:
`.claude/CLAUDE.md` refuses as a *sensitive file* (a different, harder class), while
`.claude/skills/**` refuses as *ungranted* — and neither the worktree's bare
`Edit` / `Edit(//**)` nor `~/.claude/settings.json`'s `Edit(/.claude/skills/engineering/**)`
satisfies it from this checkout.

⚠ A shell `printf '' >> <file>` "writability test" does **not** detect either class and
reports success; the refusal is in the Edit/Write tool layer, not the filesystem. Test
with a real one-character edit, or assume refusal.
