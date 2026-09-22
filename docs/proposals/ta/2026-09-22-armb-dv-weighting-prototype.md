# ARM B stage (i): the dollar-volume-weighted 12-2 replication

Refs #2834. Part of #2832. Queue: #2437 phase 2.

**Supersedes** `2026-09-21-armb-weighting-separation-prototype.md`, which was withdrawn
(`8518aabc`, never merged). Its seven Codex checkpoint-1 findings are in
`2026-09-21-armb-signal-basis-blocker.md` §7. This spec's own checkpoint-1 returned 45
findings; §8 lists how each was handled.

**This document is the stage-(i) declaration.** It is frozen by merging, and it merges
before the script that computes it exists. It does not declare stage (ii); see §1.

## 0. Premise: value weighting is unreachable, so the issuer-keying question does not bind here

The queue entry (#2834, 2026-09-22 21:00Z) reads: *"PIT shares outstanding must key by
the Intrader series' own issuer, or abstain on the 509 `linked_early_reuse_suspect`
series."* That question only matters if ARM B can value-weight. It cannot.

Step 0 was re-run at `ab5ddad9` with `PYTHONPATH=. uv run python -m
scripts.measure_2834_armb_weighting_basis`, which exits 1:

- **Every year 2000–2008 has 0.0% coverage on every arm the script reports.** That
  includes the series-count arm and the dollar-volume-weighted arm. When coverage is
  zero, the covered fraction is zero under any weighting. Cap weighting is therefore
  included, even though the script documents it as unevaluable as stated: cap weight is
  exactly the thing a missing share count leaves unknowable.
- Pooled across all years: 7.6% (fresh, series count) and 15.5% (fresh, dollar-volume
  weighted). On the series-count decision arm, 0 of 321 formations clear 80%.
- **Why the zero years stay zero:** the source #2834 names is *"stored SEC XBRL facts"*,
  and none exist for those years under any identity key. Issuer keying could change
  which post-2009 filings match. It cannot create pre-2009 ones.

Other sources for historical share counts (CRSP, pre-XBRL cover pages) are outside the
step-0 source that #2834 names, and none is stored. The ticket's declared fallback,
*"dollar-volume weighting, reported as such"*, therefore binds.

**Where identity can still reach this run:**
- `load_universe_selection` admits on `validated_ids`.
- `name_key` is the linked `instrument_id`, and `s2_select` breaks ties with it.

Two properties make that acceptable:
- **Uniqueness is enforced.** Admission refuses duplicate keys
  (`universe_selection.py:314`).
- **A key only orders ties.** No price, volume or return is read through it.

Two inputs are read without `instrument_id` at all:
- **Dollar volume** comes from the series' own bars.
- **Termination evidence** sits on the series row, behind the Form 25 identity gate
  (`series_termination.UnlinkedStratum.IDENTITY_UNVERIFIED_REUSE`).

The 509 suspects therefore need no treatment in this stage. Their count is printed in
the census line.

## 1. Question and decision, frozen before any result

**Question.** On the survivorship-free corpus, does canonical 12-2 momentum pass #2834's
bar (i) when weighted by dollar volume, as the ticket declares?

**Bar (i), verbatim:** *"long-decile minus market gross monthly premium positive with
year-clustered t ≥ 2."* This spec adds no materiality constant.

| verdict | condition | exit |
| --- | --- | --- |
| `PASS` | on **both** termination-ambiguity arms: mean premium > 0 **and** t ≥ 2 | 0 |
| `FAIL` | inference valid on both arms, and either arm misses the bar | 1 |
| `UNMEASURABLE` | on either arm: no formations, fewer than 2 year-clusters, clustered SE = 0 or non-finite, or any formation dropped under §2.4's zero-denominator rule | 2 |

Why each condition is there:

- **Both ambiguity arms must pass.** A worse terminal haircut lowers both the decile and
  the market, so `worst_case` is not a lower bound on the *premium*.
- **`UNMEASURABLE` is separate from `FAIL`.** An inference that cannot be computed is
  not a failed bar, and must not close the arm.
- **The two outcomes move ARM B differently.**
  - **`FAIL`** is #2834's own closing rule: *"direct-stock tilts are dead here; ETF arm
    is capped to whatever ARM A proved."*
  - **`PASS` is provisional.** It licenses two things: a separate #2829 declaration for
    stage (ii), *"net-of-cost hold-out return ≥ the equal-weight universe benchmark"*,
    and the engine weight column that running (ii) as declared requires. It is not a
    promotion.

⚠ **The window differs from the ticket's literal span.** Two operator rules conflict:

- #2834 (i) says *"2000–2026"*.
- The #2437 spike rule reads: *"Exploration NEVER touches `primary-2022-plus`."*
- #2834 (ii) makes `primary-2022-plus` the *one* confirmatory shot. It would be spent in
  advance if (i) read it.

The survivorship-free label also ends at `INTRADER_CAPTURE_DATE` = 2024-09-27, so 2026
is unreachable on this corpus. Stage (i) therefore ends before `HOLDOUT_BOUNDARY` (§2.1).
The verdict is scoped to that window and is printed with it.

⚠ `2 × SE_clustered` is printed as **"the premium the test needed to clear"**. It is not
a minimum detectable effect, because no power or alternative is declared.

## 2. Construction

### 2.1 Window and warm-up

- **Formations included:** every `t` that `rebalance_dates` returns with
  `t ≥ 2000-01-01`, whose next formation `t_next` satisfies
  `t_next < strategy_result.HOLDOUT_BOUNDARY` (2021-06-29).
  - The last included holding therefore closes in June 2021, before the boundary.
  - Nothing on or after 2021-06-29 is read.
- **Enforcement:** the bar query carries `bar_date < HOLDOUT_BOUNDARY`, and the run
  asserts the maximum loaded date.
- **Warm-up:** bars before 2000 are loaded without a lower bound, so the January 2000
  formation has its full 273-bar warm-up.

### 2.2 Universe

`load_universe_selection(conn, universe="survivorship_free", validated_ids=…)` is the
admission the engine reads (#3302). At `ab5ddad9` it admits 17,266 series. Two
limitations are declared rather than fixed:

1. **No asset-class cut.** The corpus has no asset-class column, and classifying through
   `instrument_id` would bring back the identity exposure §0 avoids. ETFs and funds are
   inside the ranked population and inside the market leg. This **changes the estimand**:
   it is 12-2 on the admitted population, not on US common stock. The fact that every
   arm shares it does not undo that.
2. **Residual selection.** Series alive at capture are admitted only when validated. The
   population is survivorship-*free* in the terminated tail, not free of listing
   selection.

### 2.3 Signal and selection

- **Selection:** `_RANKING_SQL` from `scripts/verify_2240_s2_cross_sectional.py` with
  `--universe survivorship_free`, the independent from-scratch SQL implementation.
  - This honours #2834's *"never reuse s2 machinery"* for the computation itself.
  - It scores `close(t−21) / close(t−252) − 1` on the split-corrected basis.
  - Top decile, `position ≤ n/10`, tie-break `name_key` ascending.
  - The oracle takes its `ln(round(split_factor, 30))` form from #3304.
- **Per-formation cross-check:** at every formation, the run also computes the module's
  selection (`momentum_series` on `load_ratio_basis`, then `s2_select`). It **refuses**
  on any set difference. #3304 reports 0/626 across all survivorship-free rebalance
  dates; this check makes that property hold for every formation this run actually
  uses, not just once.
- **Inherited parameters:** 252/21 bars, 273-bar eligibility, `MIN_CLOSE` = 1.0 on the
  as-traded bars, `MIN_CROSS_SECTION`, the floor-rounded decile, first-bar-of-month
  formation.
  - They are **inherited from s2's frozen identity, not re-derived**.
  - Step 1 (`2e76a42d`) established that ARM B's rule is s2's rule, and s2's docstring
    cites Jegadeesh-Titman (1993) and the Ken French construction note.
  - **Declared deviation:** these are *bar* offsets, not calendar-month returns, and the
    two differ for sparse histories.
  - The as-traded floor is available on this whole universe: the Intrader vendor stores
    `unadjusted` OHLC (`research_corpus_ingest.py:165-171`).

### 2.4 Weights: the only treatment

| arm | weight of decile member `i` at formation `t` | role |
| --- | --- | --- |
| `dollar_volume` | `DV_i / Σ_decile DV` | **decision** |
| `equal` | `1 / N_sel` | comparison only |

`DV_i` is the mean of raw `close × volume` over series `i`'s own **21 most recent bars
with `bar_date < t`**, counting only bars with `return_usable`, a positive close and a
non-null volume ≥ 0.

- **Bar selection:**
  - Missing sessions are simply absent; the 21 are the series' own observations.
  - No staleness limit is added. Every selected name already passed s2's evaluability
    at `t`.
- **Source rule:** #2834 declares the fallback but does not construct it. No published
  formulation fixes a dollar-volume weighting window. This one is set **by
  construction**:
  - 21 bars is the length of the skip month.
  - The window ends strictly before `t`.
  - It answers withdrawn-spec finding 5, where summing over "the formation month" was
    look-ahead.
  - The window length and the `< t` end are frozen in the run stamp.
- **Raw basis:** Intrader's close and volume are both as-traded. AAPL across 2020-08-31
  prints close 499.23 → 129.04 and volume 46,907,479 → 223,505,733
  (`2026-09-21-armb-split-adjustment-derivation.md` §4). Their product is therefore
  split-invariant on this single-vendor universe, and it involves no division.
- **Zero DV:** if a member has no qualifying bar, or all of its qualifying bars have zero
  volume, its DV weight is 0. It stays in `equal`.
  - The output prints how many members this affects, the weight they carry in `equal`,
    and the effective holding count of the DV arm.
  - Aggregate context, not a measure of selected names: 1,428,140 of 36,303,967
    Intrader bars in 2000–2021 are zero-volume (query in §7).
- **Zero denominator:** if `Σ DV` over the decile or over the market is 0, that
  formation is dropped from **both** arms and counted, and the verdict becomes
  `UNMEASURABLE` (§1). A dropped formation cannot be chosen after seeing the outcome.
- **Rank weighting is dropped.** It is not #2834's construction, it has no published
  form, and adding it would be another trial.

### 2.5 Holding return: buy-and-hold from `t` to `t_next`

`R_arm(t) = Σ_i w_i × h_i`.

- **Execution convention:** entry at `adj_close(t)`, using information through
  `close(t)`. This is the academic monthly-return convention. s2 itself fills at
  `open(t+1)`; the deviation is declared in §4.
- **Return basis:** `h_i` is computed on `adj_close`, #2429's settled
  `split-dividend-adjusted-wealth-v1` basis. Only bars with `return_usable` count.
- **Termination:** use the stored `AdmittedSeries.last_bar`, never the last date of the
  window-clipped load.
  - **`last_bar ≤ t_next`, so the series terminates inside the holding interval,
    endpoint included:**
    `h = adj_close(u) × terminal_value_fraction(class, arm) / adj_close(t) − 1`.
    - `u` is the last usable bar on or before `last_bar`.
    - The class is `classify_termination(series.termination)`. The admitted evidence
      is passed unchanged, `q_suffix` included.
    - Proceeds are held as cash at 0% until `t_next`. This is by construction and
      applies to both legs.
  - **`last_bar > t_next`, so the series is live:** `h = adj_close(u) / adj_close(t) − 1`.
    - `u` is the last usable bar on or before `t_next`.
    - A suspension or quarantined endpoint is therefore marked stale rather than
      excluded.
  - **`last_bar = t`:** cannot occur, because s2 refuses a segment-final bar at
    formation. The run asserts this.
- **Intra-month breaks:** the return is endpoint to endpoint. The settled quarantine is
  the only bar-hygiene rule, and no break filter is invented here.
  - ⚠ A break that survives quarantine distorts `h`.
  - DV concentration is printed so the reader can size how far one name can move the DV
    arm.

### 2.6 Market leg

`M(t)` is the **DV-weighted** return over **s2's full eligible, scored cross-section at
`t`**, meaning the population the decile was ranked from. It uses the same `DV`, `h`,
termination and zero-DV rules.

- #2834 names "market" without constructing it. Dollar volume is the ticket's declared
  proxy for value weight, so the market leg takes the same weighting.
- The equal-weighted `M` is printed as a sensitivity.
- One `M` serves both arms, so `(R_dv − M) − (R_equal − M) = R_dv − R_equal` exactly.
- The PASS statistic `R_dv − M` is, by the ticket's definition, net of anything the
  decile and the market share.

### 2.7 Inference

- **Premium:** `P(t) = R_dv(t) − M(t)`, one observation per formation.
- **Cluster:** each observation is assigned to the calendar year of `t`.
- **Mean and t:** the mean of `P`, and its t with CR1 year-clustered SE:
  `V = G/(G−1) × Σ_g (Σ_{t∈g} (P_t − P̄))² / n²`. Reference: Cameron & Miller (2015),
  *A Practitioner's Guide to Cluster-Robust Inference*, J. Human Resources 50(2).
  - With `G` about 22, the reference value is t ≥ 2, as the ticket states.
- **Validity:** judged on `SE`, not on the variance of `P`. A zero SE with a varying `P`
  is still `UNMEASURABLE`.
- **Profit factor:** `Σ max(P,0) / Σ max(−P,0)`. It prints `inf` when there is no losing
  month and `n/a` when `P` is empty or all zero. PF is context only; bar (i) decides.

### 2.8 Readouts: mandatory, and never a gate

Every figure below is printed for both ambiguity arms:

- **Per regime cohort.** `spy_chain_v1` regime on the formation date, from
  `MarketRegimeProvider.load_research`. Each cohort prints `n`, `G`, mean, and t.
  - t is printed only when the cohort has `G ≥ 2` and `SE > 0`.
  - These are descriptive cohorts on a retrospectively computed series. They do not
    veto, and they are never pooled into a neighbouring cohort.
- **The weighting effect:** `R_dv − R_equal`, with its CR1 t. This is the question the
  step-1 verdict left open. It is printed only and does not gate.
- **The premium on each basis:** equal-weighted `M` alongside the DV-weighted one.
- **DV concentration:** top-1 and top-5 weight share per formation, median and max.
- **Counts:** zero-DV members, terminations by class, dropped formations, and the 509
  reuse suspects.
- **The run stamp:**
  - the `main` SHA;
  - the quarantine `rule_set_version`;
  - `TERMINATION_RULE_VERSION`;
  - the regime version;
  - the admitted-series count;
  - the DV window;
  - the first and last formation.

## 3. Deliverable and trial accounting

The script is `scripts/measure_2834_armb_dv_prototype.py`. It is read-only and writes no
`strategy_results_store` row. Its exit code is the verdict code from §1.

**Trial accounting.** The run decides stage (i), so it is a trial.

- The implementing PR adds a `DeclaredTrial` to `app/services/trial_register.py`
  **before the first run**: `trial_id` `armb-12-2-dv-weighted-stage-i-2026-09-22`,
  exactness `EXACT`, `searches=1`. One decision arm; the equal arm and the sensitivities
  are readouts.
- That entry follows the pattern of the register's existing script-level first looks
  (`insider-purchase-forward-returns-first-look-2026-08-09`).

**Tests.** Pure-logic, with no DB, in `tests/test_2834_armb_dv_prototype.py`:

- the DV mean;
- the zero-DV and zero-denominator paths;
- the holding return on each `last_bar` branch;
- CR1 against a hand computation;
- `SE = 0` with a varying `P`;
- the verdict table from §1, all three outcomes and both-arm agreement.

The run checks four integration invariants at runtime, and refuses on any breach:

- the oracle agrees with the module at every formation;
- no bar is read on or after `HOLDOUT_BOUNDARY`;
- no selected name has `last_bar = t`;
- admission keys are unique (inherited).

## 4. Fidelity limits (declared)

1. Holdings run from close to close. s2 fills at `open(t+1)`.
2. The premium is gross, with no costs, spreads or fills. Bar (i) is gross; stage (ii)
   carries the banded cost model.
3. Formation uses bar offsets, not calendar months (§2.3).
4. The population includes non-common-stock series (§2.2).
5. Cash earns 0% after a termination.

## 5. What this is NOT

- **Not stage (ii).** No `primary-2022-plus` read, and no net-of-cost claim.
- **Not a cap-weighted replication** (§0).
- **Not a reinstatement case for s2**, which is retired under #2845.

## 6. Superseded-spec findings

| # | withdrawn-spec finding | here |
| --- | --- | --- |
| 1 | estimand cancels common benefit | one `M` serves both arms (§2.6) |
| 2 | rank weights undefined | dropped (§2.4) |
| 3 | delisting does not conserve wealth | `terminal_value_fraction`, proceeds held as cash (§2.5) |
| 4 | `MIN_CLUSTERS` is not adequacy; zero variance | CR1; `UNMEASURABLE` on SE (§1, §2.7) |
| 5 | DV look-ahead | window strictly `< t` (§2.4) |
| 6 | non-rejection closes; no inconclusive state | the ticket's bar; a separate `UNMEASURABLE` (§1) |
| 7 | significance ≠ materiality; cohort veto | no invented bar; cohorts do not veto (§1, §2.8) |

## 7. Reproduce

```sql
-- §2.4 zero-volume bar share (1,428,140 / 36,303,967 at ab5ddad9)
select count(*), count(*) filter (where d.volume = 0)
  from research_price_daily d
  join research_price_series s on s.series_id = d.series_id
 where s.vendor = 'icyDenev/Intrader'
   and d.bar_date >= date '2000-01-01' and d.bar_date < date '2022-01-01';
```

§0: `PYTHONPATH=. uv run python -m scripts.measure_2834_armb_weighting_basis`.

## 8. Checkpoint-1 on this spec (45 findings): disposition

Each finding is folded into the section named. Findings not listed as rebutted were
accepted:

- **Window:** 1, 22, 23 → §1, §2.1.
- **Premise:** 2, 3, 4 → §0.
- **Signal and selection:** 5, 6 → §2.3; the oracle is the computation, and the module
  is cross-checked at every formation.
- **Inherited strategy parameters:** 7, 8, 13 → §2.3, declared as inherited with the
  deviation named.
- **Universe:** 9, 10 → §2.2, declared limits.
- **Identity:** 11, 12 → §0.
- **DV construction:** 14–20 → §2.4.
- **Holding return and termination:** 21, 24–27, 29, 30 → §2.5, §4.
- **Worst-case logic:** 28 → §1; both arms must pass.
- **Market leg:** 31–33 → §2.6.
- **Verdict logic:** 34, 35, 36, 37 → §1, §2.7.
- **Cohort inference:** 38–40 → §2.8.
- **Profit factor:** 41 → §2.7.
- **Reproducibility:** 42 → the run stamp in §2.8.
- **Tests:** 43 → §3's runtime invariants.
- **Trial accounting:** 44 → §3; charged as a trial.
- **Stage (ii):** 45 → §1; `PASS` is provisional.

Partly rebutted:

- **15, the DV window is invented.** Accepted as by-construction, with the reason stated
  and the window frozen. No published formulation exists to substitute.
- **3, other share sources.** #2834's step 0 names XBRL, and no other source is stored.
  Building a new one is not stage (i).

## 9. Pre-run amendments (implementation, before any outcome was computed)

These were fixed while implementing, before any return existed. The only runs so far
were `--selection-only`, which derives no outcome mark.

1. **A missing mark drops the formation.** This covers the entry mark, the live exit
   mark and the terminal mark. A drop makes the verdict `UNMEASURABLE`, the same as
   §2.4's zero denominator. The alternative was to renormalise over the names that
   still have marks, and that would silently redefine the declared decile and market
   (Codex checkpoint 2, P1). Selection-only runs measured **0** missing entry marks
   over 240 formations.
2. **`t_next` is the next formation, per §2.1.** It is not the next rebalance date.
   Some in-range rebalance dates have no cross-section; they form nothing, and the
   book is held through them. The run prints the list.

   **Measured at selection-only:** 17 of 258 in-range rebalance dates form nothing,
   and every one is **1 January or Labor Day**:
   - 1 January in 2001–2004, 2006–2010, 2012 and 2013;
   - Labor Day in 2001–2003, 2007, 2008 and 2012.

   The cause is s2's `rebalance_dates`. It takes a stray holiday bar as the month's
   first bar, and no cross-section forms on that date. **This is an inherited s2
   calendar defect.** 17 of the 240 observations therefore hold for two months instead
   of one. The rule is kept, because changing it would change s2's identity, which §2.3
   inherits. It is declared here instead.
3. **Admission reads `validated_ids = load_validated_universe`** (US stocks ex-ETF,
   §4.0), as the engine does. Live series therefore do carry an asset-class cut. §2.2's
   "no asset-class cut" holds only for terminating series, which enter on their own
   evidence and have no class.
4. **The reuse-suspect count is printed at run time.** §0 quoted 509 from the
   2026-09-22 comment. The admission at this commit reports 521, and the printed figure
   is the authoritative one.
5. **The oracle cross-check covers the date sets as well as the members.** It refuses
   if the module and the SQL disagree on which in-range dates form at all.
