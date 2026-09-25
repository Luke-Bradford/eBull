# #3384 slice 1 — hunt admissibility census: the held price/dividend corpus

Build step 2 of `2026-09-25-pattern-hunt-programme.md`. Descriptive only: **no outcome return is computed**.
Slice 2 (FTD, Cboe, FRED/ALFRED, COT, MIDAS, Wikipedia: coverage, identity route and publication clock
measured from each source) follows in its own PR.

**Reproduce:** `PYTHONPATH=. uv run python -m scripts.census_3384_hunt_admissibility --out <path.json>`.
Committed output: `2026-09-25-3384-hunt-admissibility-census.json` (sha256
`91949a606faed6ad8519fbbdb7b30e966a216b1b4d8e66d9355ba1bca61ed0e9`, git head `a3aeb67a`). Every figure below
is read from that file; the script's docstring defines each field.

## Population
The survivorship-free universe exactly as the harness admits it (`universe_selection.load_universe_selection`
over `load_validated_universe`), vendor `icyDenev/Intrader`, capture 2024-09-27:

| | series |
| --- | ---: |
| harvested | 22,879 |
| admitted | 17,266 |
| — alive at capture (eToro-validated) | 4,630 |
| — terminating, `unknown_termination` (no Form 25 link, no `Q` suffix) | 11,383 |
| — terminating, `operation_of_law` / `exchange_failure` / `exchange_failure_a4` | 699 / 211 / 1 |
| — terminating, `q_suffix_otc_unverified` | 342 |
| not admitted: alive but not eToro-validated | 5,608 |
| not admitted: terminated exchange test issues | 5 |

⚠ The admission rule is asymmetric by design (#2721): a dead name is admitted on its own evidence, a live
name only if eToro lists it. 5,608 live-but-unlisted series are therefore outside every hunt arm.

## Finding 1 — no series in the archive ends before 2013-06-21
`earliest_last_bar` = **2013-06-21** over all 22,879 harvested series (exactly one admitted series ends in 2013;
`terminations_by_last_bar_year` runs 2014: 218 … 2023: 2,085). So **every name trading before mid-2013 is
conditioned on surviving to at least 2013-06-21**. The discovery window (1990–2008) and the first 4½ years
of validation are survivor-only, not "near survivor-only" as the programme doc words it.

Admitted series trading per year (the "later terminating" column = series whose own last bar is ≥ 2013):

| year | series | alive at capture | later terminating | with a dividend print | flat-bar % |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1970 | 28 | 25 | 3 | 5 | 0.0 |
| 1980 | 129 | 97 | 32 | 6 | 9.8 |
| 1986 | 431 | 296 | 135 | 72 | 8.1 |
| 1987 | 580 | 387 | 193 | 327 | 7.8 |
| 1990 | 988 | 609 | 379 | 654 | 15.0 |
| 1995 | 1,945 | 1,091 | 854 | 958 | 14.4 |
| 2000 | 2,926 | 1,540 | 1,386 | 1,360 | 12.1 |
| 2005 | 3,728 | 1,825 | 1,903 | 1,876 | 6.1 |
| 2008 | 4,564 | 2,039 | 2,525 | 2,266 | 6.4 |
| 2013 | 6,168 | 2,366 | 3,802 | 3,088 | 6.3 |
| 2018 | 7,871 | 3,127 | 4,744 | 3,549 | 9.0 |
| 2021 | 9,742 | 4,152 | 5,590 | 2,685 | 8.0 |
| 2024 | 5,845 | 4,630 | 1,215 | 1,698 | 5.6 |

## Finding 2 — dividends are near-absent before 1987
Series with at least one `dividend > 0` bar: 72 of 431 in 1986, 327 of 580 in 1987. Family 3's ex-date arm
has a usable dividend field from 1987, not 1990 or earlier.

## Finding 3 — flat bars, the overnight family's input
`flat_bars` = open NULL or open = high = low = close. Per window (admitted series, 2021 split out because it
straddles the boundary):

| window | alive: bars / flat % | later terminating: bars / flat % |
| --- | --- | --- |
| pre-discovery (≤ 1989) | 731,549 / 8.10 | 307,477 / 13.07 |
| discovery (1990–2008) | 6,430,924 / 8.72 | 5,893,476 / 10.78 |
| validation (2009–2020) | 7,825,283 / 2.56 | 10,906,404 / 10.96 |
| 2021 | 977,354 / 1.27 | 980,600 / 14.71 |
| holdout (2022–2024-09) | 3,022,402 / 1.80 | 1,617,827 / 32.78 |

A flat bar has no open-to-close move, so an overnight-vs-intraday split cannot use it. The share is highest
exactly where the family would look (dying names in their final years), so #3385's harness must count flat
bars as a refusal per arm, never drop them silently.

## Finding 4 — identity routes are dated, and none reaches discovery
- `instrument_id`: today's eToro mapping, undated. Present on all 4,630 alive admitted series; on 224 of the
  11,383 `unknown_termination` series.
- Form 25 register (`sec_form25_register`): filed 2013-01-02 … 2024-12-31, 11,367 rows.
- #3361 dated CIK linkage (`security_linkage.link_as_of`): `before_coverage` for any D with D − 730 days <
  2006-01-01, so the **first linkable date is 2008-01-01**. `research_price_series.cik` is empty for this
  vendor; the bundle is the route.
- 11,159 of the 11,383 `unknown_termination` series have no stored issuer link at all.

Families 1–3 run on series-level prices and need no issuer identity. Anything that joins an issuer fact
(FTD by CUSIP, short interest, MIDAS) inherits these floors.

## Finding 5 — contamination: every window has been evaluated before
Rolled up by **date overlap** (`contamination.per_window`). Labels are `family:namespace:universe` for
`strategy_results_store` rows, or the file-recorded study (`FILE_STUDIES` in the script, each citing its
document). Full rows, the holdout access log (561 accesses over 22 strategy versions) and the 8 frozen declarations are in the JSON.

| window | studies that already evaluated it |
| --- | --- |
| pre-discovery | s1, s2, s3 (survivor-only in-sample and hold-out rows stored over 1962 → 2026-07-08); s1, s4, s8, s11 survivorship-free in-sample (axis 1962-01-02 → 2021-06-28); §2.8 autocorrelation |
| discovery | the same eleven labels, plus §2.8b panic-state count |
| validation | the same, plus #2901's gate (2013-07 → 2024-08) |
| holdout | 22 labels: s1–s10 hold-out rows, the s1–s3 survivor-only in-sample rows, #2827, #2908, #2901, §2.8, §2.8b |

⚠ The survivor-only s1/s2/s3 rows carry no metric axis; their stored window (1962 → 2026-07-08) is recorded
as-is and counted as seen. `strategy_mt1_trial_results` is empty.

The overlap that matters is by **construction**, not only by date:
- §2.8 measured 1-day, 5-day and 1-month return autocorrelation on every year 1962–2024 and found short-horizon
  reversal in every price band. Family 2 (liquidity/volume shock → subsequent weeks) and family 1's reversal
  guard are looking at the same autocovariance; discovery is not a fresh look for either.
- s8 (range mean reversion) evaluated daily-bar reversal entries survivorship-free over 1962-01-02 →
  2021-06-28; s3 (mean reversion in trend) survivor-only over its stored 1962 → 2026-07-08 window.
- No study in this inventory evaluated overnight-vs-intraday splits, dividend ex-date windows, turn-of-month
  or pre-holiday effects. The inventory is closed over the two result tables and `FILE_STUDIES`; ad-hoc
  measurement scripts that printed without storing are not enumerable and are not claimed absent.

## What #3385 must declare from this
1. **The survivorship-free span is 2013-06-21 → 2024-09-27.** Pre-2013 results are survivor-conditioned; a
   family whose outcome correlates with later survival (post-drop reversal, liquidity shocks in small names)
   is biased upward there. Discovery remains a search space, not evidence, and its bias direction is
   declared per family.
2. **Validation is survivorship-free only from 2013-06-21 to 2021-06-28**, about eight years. Power
   statements use that span, not 2009 onward.
3. **The inherited trial count includes the TA in-sample runs and §2.8**, because they evaluated the
   discovery years with overlapping constructions (reversal).
4. **Flat bars and dividends are admissibility filters with counted refusals**: overnight arms exclude flat
   bars and report the count; the ex-date arm starts 1987.
5. **Any issuer-keyed join is floored** at 2008-01-01 (CIK) or 2013-01-02 (Form 25).
