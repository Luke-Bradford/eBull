# #3384 slice 1 — hunt admissibility census: the held price/dividend corpus

Build step 2 of `2026-09-25-pattern-hunt-programme.md`. Descriptive only: **no outcome return is computed**.
Slice 2 (FTD, Cboe, FRED/ALFRED, COT, MIDAS, Wikipedia: coverage, identity route and publication clock,
measured from each source) follows in its own PR.

**Reproduce:** `PYTHONPATH=. uv run python -m scripts.census_3384_hunt_admissibility --out <path.json>`.
Committed output: `2026-09-25-3384-hunt-admissibility-census.json` (`census_version`
`hunt-admissibility-census-v2`, git head `9376ee24`, `git_dirty` false, `validated_ids_sha256` `3b6448a1…`). Every figure below is
read from that file. The dev DB is not snapshotted: a re-run moves with `sync_universe` (the validated set) and
any corpus re-harvest, which is why the validated-id digest is recorded.

## Population
The survivorship-free universe exactly as the harness admits it (`universe_selection.load_universe_selection`
over `load_validated_universe`), vendor `icyDenev/Intrader`, capture 2024-09-27, 0 unharvested series:

| | series |
| --- | ---: |
| harvested | 22,879 |
| admitted | 17,266 |
| — last bar within 7 days of capture (`alive_at_capture`) | 4,630 |
| — terminating, `unknown_termination` (no Form 25 link, no `Q` suffix) | 11,383 |
| — terminating, `operation_of_law` / `exchange_failure` / `exchange_failure_a4` | 699 / 211 / 1 |
| — terminating, `q_suffix_otc_unverified` | 342 |
| not admitted: current, no link to a validated eToro instrument | 5,603 |
| not admitted: exchange test issues | 10 |

"Terminating" means the series stops more than 7 days before capture; it does not establish why. The
admission rule is asymmetric by design (#2721): a stopped series is admitted on its own evidence, a current
one only if it links to a validated eToro instrument. **So admission stays future-conditioned after 2013
too**: a current name enters only through today's eToro list. 521 admitted series are
`linked_early_reuse_suspect` (a current instrument's link on a series that stopped early); they are not
stratified further here.

## Finding 1 — no harvested series ends before 2013-06-21
`earliest_last_bar` = **2013-06-21** across all 22,879 harvested series; exactly one admitted series ends in
2013. The first year each termination class appears (`terminations_by_last_bar_year`): `unknown_termination`
2013, `q_suffix_otc_unverified` 2014, `operation_of_law` 2017, `exchange_failure` 2019, `exchange_failure_a4`
2022.

So **every series trading before 2013-06-21 continues to at least that date**. Everything before it is
survivor-conditioned, not "near survivor-only" as the programme doc words it. After that date exits are
observed, but their completeness against an exchange listing census is **not** measured here.

Admitted series trading per year ("later terminating" = in a terminating stratum; its own last bar is ≥
2013-06-21 by Finding 1):

| year | series | alive at capture | later terminating | with a dividend print (prints) | zero-range bar % |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1970 | 28 | 25 | 3 | 5 (19) | 0.0 |
| 1980 | 129 | 97 | 32 | 6 (21) | 9.8 |
| 1986 | 431 | 296 | 135 | 72 (213) | 8.1 |
| 1987 | 580 | 387 | 193 | 327 (507) | 7.8 |
| 1988 | 627 | 417 | 210 | 457 (1,697) | 11.0 |
| 1990 | 988 | 609 | 379 | 654 (2,531) | 15.0 |
| 1995 | 1,945 | 1,091 | 854 | 958 (3,496) | 14.4 |
| 2000 | 2,926 | 1,540 | 1,386 | 1,360 (5,108) | 12.1 |
| 2005 | 3,728 | 1,825 | 1,903 | 1,876 (7,769) | 6.1 |
| 2008 | 4,564 | 2,039 | 2,525 | 2,266 (9,522) | 6.4 |
| 2013 | 6,168 | 2,366 | 3,802 | 3,088 (14,157) | 6.3 |
| 2018 | 7,871 | 3,127 | 4,744 | 3,549 (13,938) | 9.0 |
| 2021 | 9,742 | 4,152 | 5,590 | 2,685 (10,251) | 8.0 |
| 2024 (to 09-27) | 5,845 | 4,630 | 1,215 | 1,698 (4,617) | 5.6 |

## Finding 2 — the dividend field
`dividend > 0` prints per year are in the table: 213 in 1986, 507 in 1987, 1,697 in 1988, 2,531 in 1990. The
field is populated from before discovery starts (1990). Not measured here: whether the bar date is the
ex-date, the dividend's type, or whether a zero is "no dividend" or "missing". Family 3's ex-date arm needs
that verified against a source before it is built.

## Finding 3 — opens, zero-range bars, volume
No bar has a NULL open (`open_null_bars` = 0 in every cell). Per window, admitted series (2021 split out
because it straddles the boundary; cells are calendar-year, so a 2021 cell belongs to both windows):

| window | alive: bars / zero-range % / no-volume % | later terminating: bars / zero-range % / no-volume % |
| --- | --- | --- |
| pre-discovery (1962–1989) | 731,549 / 8.10 / 3.14 | 307,477 / 13.07 / 7.42 |
| discovery (1990–2008) | 6,430,924 / 8.72 / 4.63 | 5,893,476 / 10.78 / 6.66 |
| validation (2009–2020) | 7,825,283 / 2.56 / 1.36 | 10,906,404 / 10.96 / 6.13 |
| 2021 | 977,354 / 1.27 / 0.42 | 980,600 / 14.71 / 5.54 |
| holdout (2022–2024-09) | 3,022,402 / 1.80 / 0.33 | 1,617,827 / 32.78 / 16.21 |

A zero-range bar (open = high = low = close) has a measurable overnight return and a zero intraday return;
whether the print is real or stale is not measurable from the bar. No-volume = volume NULL or ≤ 0.
`invalid_bars` (non-positive price, open/close outside [low, high]) is at most 0.124% of any cell group
above. Gap lengths, expected-session denominators and horizon-complete observations are not measured.

## Finding 4 — identity routes and their floors
- `instrument_id`: today's eToro mapping, undated. On all 4,630 `alive_at_capture` series; on 224 of 11,383
  `unknown_termination`.
- Form 25 register: filed 2013-01-02 … 2024-12-31 (11,367 rows). That is the register's span, not a link
  floor: the first Form-25-classified terminating series in the corpus is 2017 (`operation_of_law`).
- #3361 dated CIK linkage (`security_linkage.link_as_of`) answers `before_coverage` for any D with
  D − 730 days < 2006-01-01, so it **cannot link before 2008-01-01**, the last year of discovery. Link
  success after that floor is #3361's own census, not re-measured here. `research_price_series.cik` is empty
  for this vendor.
- No route in the corpus gives a dated CUSIP. FTD is keyed by CUSIP, so slice 2 must find one.
- 11,159 of the 11,383 `unknown_termination` series carry no stored link (no instrument, CIK or Form 25).

## Finding 5 — contamination: every window has been evaluated before
Rolled up by **date overlap** (`contamination.per_window`). A stored row counts its metric axis where
recorded, else its stored window. The stored window is an upper bound on what was evaluated, not a
measurement of it. Labels are `family:namespace:universe` for `strategy_results_store` rows, or the
file-recorded study. File-study windows are transcribed from the cited documents (`FILE_STUDIES`), not
re-measured.

| window | labels | what |
| --- | ---: | --- |
| pre-discovery | 11 | s1/s2/s3 survivor-only in-sample + hold-out rows (stored window 1962 → 2026-07-08, no metric axis); s1/s4/s8/s11 survivorship-free in-sample (axis 1962-01-02 → 2021-06-28); §2.8 autocorrelation |
| discovery | 12 | the same, plus the §2.8b panic-state count |
| validation | 13 | the same, plus #2901's gate (2013-07 → 2024-08) |
| holdout | 26 | s1–s10 hold-out rows, s1–s3 survivor-only in-sample rows, #2827, #2908, #2901, §2.8, §2.8b, and 4 holdout accesses with no stored holdout window (`…:window_unrecorded`: #2908, #2901, s11, se-ma-overlay) |

The holdout access log (561 accesses, 22 strategy versions) and 8 frozen declarations are in the JSON, each
declaration with `has_stored_result` / `has_holdout_access`. Declarations 5–9 have neither.
`strategy_mt1_trial_results` is empty. **Not reconciled here:** #2832's 38 candidates and #2840's arms,
which programme rule 2 names in the inherited count. Reconciling them is #3385's job, from the #2829
register.

By construction as well as by date:
- §2.8 measured 1-day, 5-day and 1-month return autocorrelation over 1962–2024 on the survivor-only sibling
  corpus (per `strategy-evidence.md` §2.8; transcribed). Family 2's shock → subsequent-weeks return and
  family 1's reversal guard are different estimands, but they overlap it.
- s8 evaluated daily-bar range-reversal entries survivorship-free over 1962-01-02 → 2021-06-28; s3
  (reversion in trend) survivor-only over its stored window.
- No label in this inventory names overnight-vs-intraday, dividend ex-date, turn-of-month or pre-holiday
  constructions. Stored rows carry no construction detail, and ad-hoc scripts that printed without storing
  are not enumerable, so this is **not** a claim that no such look ever happened.

## Inputs this gives #3385 (it declares; this census does not)
1. **Survivor conditioning before 2013-06-21 is measured, not assumed.** The bias direction depends on
   signal, side and matched control, so each family declares it rather than inheriting "upward".
2. **The unconditioned span is at most 2013-06-21 → 2021-06-28 in validation, plus the holdout.** Its exit
   completeness is unmeasured, and it has already been evaluated by the labels above, so "validation" is a
   previously examined window, not a first look. Power comes from eligible observations after purges,
   computed by #3385, not from a calendar span.
3. **The inherited trial count is derived by #3385** from the #2829 register plus this inventory. Grouped
   labels are not trial counts, and neither are access counts.
4. **Zero-range and no-volume bars are counted per arm.** The rule deciding eligibility must use only what
   is known before the session the arm trades, applied identically to its matched control.
5. **Issuer-keyed joins are floored** at 2008-01-01 (CIK route) and by each source's own coverage.
   Security-keyed joins (CUSIP) have no held route yet.
