# #3621 — avoidance filters as standing long-book exclusions, measured on the #3609 panel

Status: **draft, before Codex checkpoint 1.** Nothing in this spec has read a filtered or unfiltered return on any
month. Programme: `docs/research/2026-10-04-strategy-research-sweep.md` §4 item 5. Standards:
`.claude/skills/quant/research-process.md`. Panel and book machinery: #3609 step 1 and step 2
(`docs/research/2026-10-04-3609-step1-factor-panel.md`, `docs/research/2026-10-06-3609-step2-factor-book.md`).

## Question

Does excluding the names a set of published avoidance filters flag leave an equal-weight long book of the #3609
panel no worse, net of our costs, than holding them, within a margin the planned data can test?

The filters are MAX/lottery (Bali, Cakici & Whitelaw 2011), price below $5, and fewer than 36 months of listing
history (Loughran & Ritter 1995). Short interest (Drechsler & Drechsler 2014) is slice 2, and retail attention
(Barber, Huang, Odean & Schwarz 2022) is not built (premise 4).

**What the answer can say.**
- **Pass in a segment:** the frozen filter set becomes the default exclusion for long books drawn from that segment
  of the panel population, and the MAX threshold below is the one every later long-book spec inherits (step 2's
  §"Exceptions to the skills" names this study as the owner of that threshold).
- **It does not say the flagged names underperform.** About ten years cannot power that claim at the published
  effect sizes (premise 3). The claim is non-inferiority: the exclusions cost no more than the frozen margin.
- **Fail in a segment:** "non-inferiority not demonstrated". The filter set is not adopted there by this study.
  The `market-segments.md` defaults for sub-$5 and young names rest on the published record and stay as written,
  but the failure is recorded beside them, and the next long-book spec for that segment must state how it treats
  it.
- **Never:** a strategy admission. This is a construction rule. It does not touch the 2026-10-08 demo screen,
  which applies to books.

**Estimand.** Step 1's restricted population (linked 10-K/10-Q filers our archive prices), on the formation grid
2014-09..2024-07. Nothing is generalised to CRSP or to today's eToro universe.

## Premises (measured)

**1. Where the filters bite.** Counts of admitted names flagged per stage-A formation (80 formations, 2014-09..
2021-04), by JKP NYSE size segment and for step 2's book universe (top 1,000 by ME). Counts only: no return was
read. Reproduce: `uv run python -m scripts.measure_3621_filter_premise` (stage-A artefact
`2026-10-07-0e8dba6e-stageA`, manifest sha256 `e5087210…`, as `measure_3609_step2_universe`).

| segment | admitted (median) | MAX top decile | price < $5 | seasoning < 36 m | any |
|---|---|---|---|---|---|
| micro | 1,344.5 | 234 | 528.5 | 277 | 756 |
| small | 784.5 | 48 | 17 | 123.5 | 174 |
| large | 595 | 17 | 1 | 53 | 67.5 |
| mega | 331.5 | 3.5 | 0 | 13 | 16 |
| top 1,000 | 1,000 | 23 | 2.5 | 74.5 | 96.5 |

Medians over the 80 formations; the script prints min and max and the per-formation table. Names with no
`rmax1_21d` value number at most 32 a month (micro), 3 (small) and 1 (large, mega, top 1,000). The premise
script applies the extreme-return screen only, not the ratio-move screen the build will share with `rvol_21d`, so
its MAX counts can differ slightly from the run's.

Reading: inside the top 1,000 the filters remove a median 96.5 names, mostly on seasoning, and MAX removes 23. In
micro caps they remove more than half. Whatever this study finds for the top 1,000 is a small-weight effect.

**2. Published support, after publication.** JKP's US monthly value-weighted capped factor returns
(`reference_snapshot_jkp_usa_monthly_vw_cap`, frozen in the stage-A artefact), signed by JKP Table 9 so a positive
return means the expected direction, months to 2014-09 only:

| JKP factor | sign | months | mean / month | sd / month | annualised IR |
|---|---|---|---|---|---|
| `rmax1_21d` (low MAX minus high) | −1 | 1926-02..2014-09 (1,064) | 0.00278 | 0.04668 | 0.206 |
| `rmax1_21d` | −1 | 2011-02..2014-09 (44, after BCW) | 0.00281 | 0.02953 | 0.329 |
| `age` (young minus old) | −1 | 1926-02..2014-09 (1,064) | −0.00087 | 0.03033 | −0.100 |
| `prc` (low price minus high) | −1 | 1926-01..2014-09 (1,065) | 0.00182 | 0.04402 | 0.144 |
| `prc` | −1 | 1990-01..2014-09 (297) | 0.00026 | 0.03682 | 0.024 |

The computation is in this spec's checkpoint log (round 0). Readings:
- **MAX:** the premium held after publication in the expected direction.
- **`age`:** JKP's sign is −1, so its negative mean says old firms beat young ones over the long history, the
  direction the seasoning filter assumes. JKP's `age` is firm age from first Compustat or CRSP appearance, not our
  archive seasoning (§"Source rules").
- **`prc`:** low-priced non-micro stocks earned slightly more, not less. The sub-$5 exclusion is therefore not a
  return-prediction rule. Its case is microstructure and cost: most anomalies are strongest in micro caps and fail
  value-weighted (Hou, Xue & Zhang 2020), and our cost band below $5 is the widest (`cost_model.py`). The net
  measurement here is what tests it.
- JKP's factors exclude micro caps (terciles on non-micro names), so none of them speaks to the micro segment.
  JKP's series in the artefact end at 2021-05.

**3. Power: what ten years can test.** A powered "flagged names underperform" claim is out of reach. At the
post-publication MAX IR of 0.33, one configuration needs `((1.645 + 0.842) / 0.33)²` ≈ 57 years. Track B's power
is about the non-inferiority margin instead (`research-process.md` §"Power, per track"). With two gated
configurations (§"Decision rule"), `power_check` gives `critical_t` 2.1646 and needs a margin of at least 0.955 IR
for 80% power over 9.92 years. The frozen margin is δ = 0.96: power 0.805, required years 9.81. Reproduce:
`PYTHONPATH=. uv run python -c "from app.services.trial_register import *; print(power_check(TrialDesign(EvidenceTrack.ADOPTION, 0.96, 'x', 9.92, 'x'), trials=2))"`.

The margin is in IR units of the monthly differential (filtered minus unfiltered, net). Its money size scales with
that differential's tracking error, which is unknown until the run and is printed beside the verdict.

**4. Data for the two filters not built here.**
- **Short interest.** FINRA bimonthly short interest covers exchange-listed names only from the 2021-07 settlement files
  (`data-sources/finra.md`, the pre-June-2021 note), so it lies wholly inside stage B. As stored it is not point-in-time:
  `filed_at` is the settlement date, and FINRA publishes days later (`quant/data-map.md`). It is not in the panel
  artefacts. **Slice 2** captures it with a publication-calendar lag and specifies its own registration; at about
  three years, its Track B margin would be about 1.4 IR, so its value is as a conditioning flag.
- **Retail attention.** Barber et al. 2022's return effect is measured on Robinhood user-count herding events
  (Robintrack, 2018-05..2020-08). We hold no such series. The usual price-and-volume attention proxy, abnormal
  volume (Barber & Odean 2008), measures who buys, not what the stock returns afterwards, and high abnormal volume
  is followed by higher returns, not lower (Gervais, Kaniel & Mingelgrin 2001). No proxy with a published negative
  return effect exists in our data, so no attention filter is built. MAX overlaps it: it is the lottery
  characteristic retail attention concentrates in (Bali et al. 2011).

## Source rules

- **MAX: `rmax1_21d`.** The largest daily total return over the 21 SPY sessions ending at s(M), with at least 15
  returns. That is JKP's `rmax1_21d` window and minimum, and the one-month window of Bali, Cakici & Whitelaw 2011
  (their MAX(1)). A daily return exists only between usable bars on adjacent sessions, and a window containing a
  return outside the panel's daily screen has no value, both exactly as step 1's `rvol_21d`
  (`app/services/factor_panel_prices.py`, `RVOL_SESSIONS`, `RVOL_MIN_RETURNS`, `SCREEN_RETURN_LOW/HIGH`, and the
  ratio-move screen). A name with no value is not flagged.
- **MAX threshold: the top decile,** breakpoints over all admitted names with a value at M. Bali et al. sort all
  stocks into deciles and the effect is in the top decile; JKP's terciles exist to build a long-short factor, not a
  filter. Ties at the breakpoint are flagged. This is the threshold `market-segments.md` lacks (step 2,
  §"Exceptions to the skills").
- **Price below $5:** the raw close at s(M) below `factor_book_path.PRICE_FLOOR`, never a split-adjusted price
  (`research-process.md` §"Returns").
- **Seasoning below 36 months:** `factor_book_path.archive_seasoned` is false: the series' first admitted bar is
  later than 36 months before s(M). It is archive seasoning, not an IPO date. A series that began trading before the
  archive's coverage counts as seasoned from its first archive bar, which can only under-flag. Loughran & Ritter's
  window is three years after the offering.
- **Size segments:** JKP's NYSE breakpoints at M (`nyse_p20`, `nyse_p50`, `nyse_p80`, frozen in the artefacts),
  the primary partition of `market-segments.md`.

## Samples and the hold-out

- **One path, 2014-09-30 to 2024-08,** stage A and stage B artefacts of #3609 (`2026-10-07-0e8dba6e-stageA` and
  `2026-10-08-7b3169b6-stageB-2039b95f…`), with step 2's timing, statuses, termination arms and cost bands.
- **No selection is made on outcomes,** so no development and confirmation split is needed: every definition above
  comes from its source rule, and premise 1 read counts only. The whole path is gated.
- **Stage B is reused validation** (`research-process.md` §"Hold-out"). Step 0 printed its baselines, and step 2
  printed the value book and its equal-weight top-1,000 universe there (#3609, 2026-10-08 16:50Z). Step 2's
  equal-weight universe is this study's unfiltered top-1,000 book with the same timing and costs, so that one
  comparator's stage-B return is already known (3.73% best case, 3.12% worst case). No filtered book has been
  computed on any month.
- **The stage-B access is logged** in `strategy_holdout_accesses` before the run reads the stage-B artefact.

## The books

For each segment S and formation M:
- **Unfiltered:** every admitted name in S at M, equal weight, rebalanced monthly. This is step 2's equal-weight
  universe reference (`factor_book_references.reference_decisions`) with `Formation.universe` set to S.
- **Filtered:** the same with every name flagged at M removed from `Formation.universe`. A held name flagged at M is
  sold at s(M) as a forced exit, and a name that stops being flagged re-enters at the next rebalance.
- Both are valued by `factor_book_path.value_path` under both termination arms at the base cost multiplier, with
  step 2's bands, statuses and path ends. An empty book holds cash.

**Gated segments** (two configurations):
1. **Top 1,000:** step 2's book universe.
2. **Rest:** admitted names ranked below 1,000 by ME at M.

**Printed segments:** micro, small, large and mega by NYSE breakpoints, with each single filter alone (MAX, sub-$5,
seasoning) beside the combined set.

## Decision rule (frozen before any outcome)

For each gated segment and each arm, D_t is the filtered book's net monthly return minus the unfiltered book's,
t = 2014-10..2024-08 (119 months). With m̂ its mean, ŝ its sample standard deviation and SE its Newey–West standard
error (lag 3, as step 2):

z = (m̂ + δ · ŝ / √12) / SE, with δ = 0.96.

**Pass for the segment** iff z > 2.1646 (the `power_check` critical t for two configurations) in **both** arms.
This is the one-sided non-inferiority test of H0: annualised IR(D) ≤ −0.96.

**Fidelity check, MAX only, printed and gating adoption of the threshold.** Our `rmax1_21d` long-short (JKP
terciles on non-micro names, capped value weights, sign −1: `factor_panel_fidelity.factor_month`) against JKP's
`rmax1_21d` over the stage-A months, with step 1's price-characteristic bars (`CORRELATION_BAR` 0.90, beta
0.7–1.3, `OFFSET_BAR` 0.03, lead-lag) through `compare_arm` and `characteristic_verdict`. A MAX fidelity failure
voids the MAX part of any pass: the segment's adopted set is then sub-$5 and seasoning only, and the report says
so. Sub-$5 and seasoning have no published series to match; their definitions are rules.

**The verdict line** names, per segment: PASS or FAIL, and the MAX fidelity verdict.

## Diagnostics (printed, never gated)

Per segment (gated and printed), per filter (each alone and combined), per arm, for the whole path, stage A,
stage B and each calendar year:
- filtered and unfiltered net and gross annualised return, volatility and maximum drawdown; the differential's
  mean, tracking error, annualised IR, Newey–West t and maximum drawdown;
- names flagged per formation (min, median, max) and the excluded weight at each formation;
- turnover (traded notional) and cost of each book, and the cost the exclusions saved or added;
- the flagged set's own equal-weight return (the names the filter removed), for reading only.

**Names excluded:** a gzipped JSON-lines file, one line per (M, `name_key`, symbol, segment, flags), with its
sha256 in the report.

## Registration

- One trial, `3621-avoidance-filters-v1`, Track B (`EvidenceTrack.ADOPTION`), declared in
  `app/services/trial_register.py` before the stage-B access. `TrialDesign`: effect_ir 0.96, effective_years 9.92,
  dependence "monthly rebalanced books, non-overlapping holding months; residual autocorrelation handled by
  Newey–West lag 3", trials 2.
- The declaration freezes this spec's sha256, the two artefact manifest digests, the filter definitions, δ, the
  critical t and the gated segments. The ledger follows step 2's (`docs/research/3609-ledger.jsonl` pattern),
  in `docs/research/3621-ledger.jsonl`.

## Slices

1. `rmax1_21d` and the flag function, pure, tested on fixtures, sharing step 1's screens.
2. Segment books and the differential statistics on `value_path`, tested on fixtures.
3. MAX fidelity on stage A, through step 1's fidelity functions.
4. Declaration, stage-B access, run, report, ledger.
5. Short interest (separate spec addendum, its own registration).

## Known limits

- Archive seasoning is not listing age, and the panel is retrospectively filtered (step 1 premise 4).
- Survivorship is unverified 2014-09..2018 (step 2 §"Known limits").
- Equal-weight segment books are references, not tradable strategies; eToro eligibility is not checked.
- The margin is in IR units, so its money size depends on the differential's tracking error.

## Checkpoint log

**Round 0 (author, 2026-10-08).** Premise 2's figures:
`gzcat <stage-A artefact>/inputs/reference_snapshot_jkp_usa_monthly_vw_cap.jsonl.gz` filtered to `rmax1_21d`,
`age`, `prc`; mean, sample sd and `mean / sd × √12` over the stated month ranges (inclusive of month-end dates).
