# ARM B — the weighting-separation prototype

Refs #2834. Part of #2832. Queue: #2437 phase 2.

This is #2834's own named unblock, quoted from the ARM B step-1 verdict comment
(merged `2e76a42d`), item 3:

> The weighted replication is UNTESTED, and the cheap instrument is a vectorized
> monthly-return prototype — formation ranks × weight vector, monthly rebalance,
> matched-weight benchmark, no `LegBook`, no kernel, no fills, no half-spreads.
> Compare equal / dollar-volume / rank weights on one preregistered window.
> **Only if a weighted arm separates materially does the kernel work become
> justified — and only then does it need a declaration.**

## 1. The question, and what each answer costs

ARM B is blocked, not killed, because two independent blockers were measured and
both are about *implementation*, not about the idea:

- **step 0** — point-in-time shares outstanding reach 7.7% of the corpus, so
  cap weighting is unassemblable and the ticket's declared dollar-volume
  fallback binds;
- **step 1** — the production engine cannot express a per-name weight at any
  level. `LegBook` (`app/services/equity_curve.py:184`) has no weight column and
  `_build_realised_shared_curve_kernel` (`:391`) allocates
  `target = equity_ref / basket`, so equal weight is arithmetic *inside the
  compiled kernel*. The dollar-volume fallback is therefore equally
  unimplementable today.

So the only open question is whether removing that blocker would buy anything:

> **On a 12-2 momentum long leg, does dollar-volume or rank weighting produce a
> materially different result from equal weighting?**

- **Separates** → the kernel change is justified, and ARM B proceeds to a frozen
  declaration (#2829) as a real replication.
- **Does not separate** → ARM B **closes on evidence** rather than on the
  argument-by-implication step 1 declined to make.

## 2. Decision rule — FROZEN BEFORE ANY RESULT IS COMPUTED

For each weighted arm `w ∈ {dollar_volume, rank}`, and each regime cohort:

```
D_w(m) = excess_w(m) − excess_equal(m)        # paired by formation month m
```

**Material separation** = the 95% interval on `mean D_w` excludes zero, **and**
the sign of `mean D_w` agrees across the populated regime cohorts.

⚠ **No magnitude constant is invented.** An earlier draft of this rule carried a
bps threshold; there is no published formulation for "how much separation
justifies a kernel refactor", and the cost being weighed is fixed engineering
effort rather than a per-trade cost, so a bps bar would have been a number
chosen to be crossed. The rule above is sign-and-interval only.

⚠ **Agreement across cohorts is required, not pooled significance.** A pooled
interval over a 28-year window is the exact instrument `app/services/market_regime.py`
exists to replace ("one test from 20 years ago is unlikely to behave the same
today"). An arm that separates in one regime and reverses in another has not
established a weighting effect; it has established a regime interaction, which
is a different ticket.

### Cohorts, and the one that is structurally thin

Cohorts are `spy_chain_v1`'s four regimes via
`MarketRegimeProvider.load_research`, assigned by the regime on the formation
date. Measured on the loaded series: 8,192 classified days, **bull_quiet 5,977 ·
bear_quiet 1,910 · bear_volatile 155 · bull_volatile 150**.

⚠ The two volatile cohorts are thin **by construction and not by data gap**:
`market_regime.py`'s volatility leg is Bollinger's Squeeze/Bulge — BandWidth at
its lowest/highest in 126 trading days — which is a six-month *extreme*, so
"volatile" is rare by definition. At monthly granularity they will hold single-
digit formations. **"Populated" is therefore declared here as a cohort holding
at least `MIN_CLUSTERS` formations** (the existing floor in
`app/services/block_bootstrap.py`, below which that module already refuses to
compute an interval). Thin cohorts are printed with their counts and an explicit
`interval_not_computed`, never silently dropped and never pooled into a
neighbour.

## 3. Source rules

Every data-treatment decision below is taken from a rule this repo already
settled. Where none exists it is marked **BY CONSTRUCTION** and frozen.

### 3.1 The formation window — s2's, unchanged

`close(t-21) / close(t-252) − 1`, i.e. Fama-French's prior (2-12) per the Ken
French construction note and Jegadeesh & Titman (1993). Taken from
`app/services/strategies/s2_cross_sectional_momentum.py` (`LOOKBACK_BARS = 252`,
`SKIP_BARS = 21`), which cites it.

⚠ Step 1 established that **ARM B's rule is already implemented as s2** — "12-2"
and s2's "12-1" name the same window. The only genuine delta under test is
weighting, so the signal must be s2's *exactly*, or the prototype measures
selection drift instead.

### 3.2 Selection — s2's, unchanged

Rebalance on the first bar whose calendar month differs from the previous bar's;
eligibility ≥ 273 bars; price floor; last-bar refusal; thin-panel refusal at
`MIN_CROSS_SECTION`; top decile at `position <= n / 10` with the tie-break
"score descending, then **instrument** id ascending".

This is reused verbatim from `scripts/verify_2240_s2_cross_sectional.py`'s
`_RANKING_SQL`, which is already equivalence-tested set-for-set against the
module — including the two rebalance dates (1989-01-03, 1999-10-01) where the
decile cut lands on an exact tie and a `series_id` tie-break disagreed.

### 3.3 The score reads `close`; the return reads `adj_close`

Not an inconsistency — two settled rules that happen to touch different columns:

- the **score** is s2's, and `_close_input` reads `close`;
- the **return** is #2429's settled total-return basis, `adj_close` wealth
  (`docs/proposals/ta/2026-08-11-total-return-accounting-result.md`), labelled
  `split-dividend-adjusted-wealth-v1`. A price-only comparator understates the
  hurdle, and #2426's defect — two benchmark rules differing 7,255× with the
  sign of the conclusion flipping between them — is what reading `close` for
  returns here would re-create.

Monthly wealth relative is therefore `adj_close(t') / adj_close(t)`, with
non-positive endpoints excluded and counted, per #2429's
`total_return_price_missing`.

### 3.4 Bar hygiene — the settled quarantine, and nothing more

Join `research_bar_quarantine` on `rule_set_version` and honour
`return_usable`, exactly as `_RANKING_SQL` does. `B1`/`B4` set
`return_usable = false`; `B2`/`B3` are range-only, and `sql/247` records that
folding them into the return quarantine **over-rejected by 587 windows**.

⚠⚠ **No split filter is added, deliberately.** `app/services/price_quarantine.py`
records the settled finding that the transition rule `T3` *"rejects legitimate
data at every threshold — demonstrably-real level breaks outnumber split-like
ones ~10:1"*, and is justified as containment with its bias published. Inventing
a magnitude cut here would re-open a decision this repo already made against
itself. **Magnitude is a trigger, not a verdict.**

The residual is measured rather than assumed. Over 1990-2022 on the full corpus
(`select` in §7), the 2:1-split signature (`p(t)/p(t-1)` in `[0.45, 0.55]`)
appears on 5,300 `close` transitions and 2,884 `adj_close` transitions among the
22,880 `unadjusted`-basis series, against 1,697 / 1,686 among the 7,711
`split_adjusted` ones. So `adj_close` roughly halves it and does not eliminate
it, at a residual of ~0.007% of bars. **This is a named sensitivity of the
dollar-volume arm specifically** — DV weighting concentrates, so a single fake
−50% on a heavily weighted name moves that arm and not the equal arm — and §6
reports the arm's weight concentration so the reader can size it.

### 3.5 Universe — the survivorship-free corpus, and why not the validated one

All `research_price_series` in the window, not
`validated_universe.load_validated_universe`.

⚠ This is a deliberate departure from `_RANKING_SQL`'s `--ranking` arm, which
maps the *validated* universe to series and is therefore `survivor_only`
(`is_tradable` is today's listing state, and `validated_universe.py` says at
length that it is a look-ahead filter used anyway, with the bias disclosed by
label).

**Survivorship interacts with the treatment under test.** Dead names are small
and illiquid, so dollar-volume weighting underweights exactly the cohort a
survivor-only universe removes, while equal weighting overweights it. Running
the weighting comparison on a survivor-only universe would bias it in the
direction of the question being asked. ARM B's declared basis is
`survivorship_free` (step 1), and that is the basis used.

⚠ **The corpus carries no asset-class column**, so this universe includes ETFs,
funds and other non-common-stock series that s2's §4.0 cut would exclude. It is
declared rather than filtered, because the universe is a *shared control*: all
three arms and all three matched benchmarks see the identical cross-section, so
the comparison between arms is unaffected even though no single arm is a clean
US-common-stock replication. The survivor-only cut is reported as a sensitivity
arm, never as the decision.

### 3.6 Weight vectors

| arm | `w_i ∝` | provenance |
| --- | --- | --- |
| `equal` | `1` | s2's own construction (§4 ranks every eligible name equally) |
| `dollar_volume` | `Σ close × volume` over the formation month | #2834's **declared fallback** from step 0, "reported as such" |
| `rank` | `N_sel + 1 − rank_i` within the selected decile | **BY CONSTRUCTION** |

⚠ `dollar_volume` carries step 0's two published caveats unchanged and they are
reprinted with the output: summed monthly dollar volume is **turnover, not
size** (a high-turnover microcap outweighs a low-turnover mega-cap), and
`research_price_series` mixes adjustment bases (22,880 `unadjusted` / 7,711
`split_adjusted` — recompute rather than quoting, a re-harvest moves them), so
`close × volume` is only split-invariant where the vendor adjusted both legs
consistently.

⚠ `rank` has **no published formulation**. Linear-in-rank is the common form and
there is no citable rule fixing it, so it is frozen by construction and hashed
into the run stamp. It is included because it is the only one of the three that
is *implementable today without a share count* and still expresses conviction —
if neither it nor DV separates, the kernel question is settled for both the
available weightings and the unavailable one is moot.

## 4. Method

1. Build the eligible cross-section and decile per formation date from
   `_RANKING_SQL`'s logic, over the survivorship-free corpus.
2. Per formation date and arm, form `w` over the selected decile and, separately,
   over the **full eligible cross-section** (the matched benchmark).
3. Mark each name from formation close to the next formation close on the
   `adj_close` wealth scale. Names that stop printing between formations are
   marked to their last usable bar and then carry zero weight — a delisting is a
   realised outcome, not a dropped row.
4. `excess_w(m) = R_decile,w(m) − R_universe,w(m)`.
5. Per cohort, `mean D_w` and its interval via
   `block_bootstrap_expectancy` (Politis-White / Patton-Politis-White optimal
   block length), one cluster per formation date, seed frozen in the run stamp.
6. Apply §2's decision rule.

**No fills, no `LegBook`, no kernel, no half-spreads, no cost model** — item 3's
words. The prototype therefore cannot and does not produce a promotable result.

## 5. What this is NOT, stated so the output is not over-read

- **Not a preregistered trial.** It is exploration under #2437's spike rules, so
  it **does not touch `primary-2022-plus`** (2022-01-01 … 2024-09-27, per
  `scripts/check_strategy_viability.py:37`). Window is **1994-01-01 → 2021-12-31**,
  bounded at both ends: the lower bound is the first date `spy_chain_v1`
  classifies a regime (1993-11-11), the upper bound clears `primary-2022-plus`.
  Item 3 is explicit that a declaration is owed only *if* an arm separates.
- **Not a performance claim about s2.** s2 is retired under #2845 and its pinned
  hold-out expectancy is negative; nothing here argues for reinstating it. The
  comparison is arm-to-arm, and an arm can separate while every arm loses.
- **Not a cap-weighted replication.** Step 0 closed that: PIT share coverage is
  7.7%.
- **Not evidence for or against 12-2 as a signal.** The signal is held fixed.

## 6. Output

Per arm × cohort: formations, names per formation, `mean D_w`, its interval,
profit factor over monthly excesses, and the arm's **weight concentration**
(top-1 and top-5 share of `w`) — the last because §3.4's split residual and
§3.6's turnover-not-size caveat both scale with concentration, and a reader
cannot size either without it.

Pooled figures are printed **below** the cohort table and labelled context, never
as the decision.

Exit code is the verdict: `0` = no material separation (ARM B closes on
evidence), `1` = separation found (kernel work justified, declaration owed).

## 7. Reproducing the figures quoted above

```sql
-- §3.6 adjustment-basis mix
SELECT adjustment_basis, count(*) FROM research_price_series GROUP BY 1;

-- §3.4 split-signature residual, close vs adj_close, by basis
WITH r AS (
  SELECT s.adjustment_basis AS basis,
         d.close     / nullif(lag(d.close)     OVER w, 0) AS c_rel,
         d.adj_close / nullif(lag(d.adj_close) OVER w, 0) AS a_rel
  FROM research_price_daily d
  JOIN research_price_series s ON s.series_id = d.series_id
  WHERE d.bar_date >= DATE '1990-01-01' AND d.bar_date < DATE '2022-01-01'
    AND d.close > 0 AND d.adj_close > 0
  WINDOW w AS (PARTITION BY d.series_id ORDER BY d.bar_date)
)
SELECT basis, count(*) AS n,
       count(*) FILTER (WHERE c_rel BETWEEN 0.45 AND 0.55) AS close_halfish,
       count(*) FILTER (WHERE a_rel BETWEEN 0.45 AND 0.55) AS adj_halfish
FROM r WHERE c_rel IS NOT NULL AND a_rel IS NOT NULL GROUP BY 1;
```

§2's regime day counts:
`MarketRegimeProvider.load_research(conn)`, then count the non-`None` values.
