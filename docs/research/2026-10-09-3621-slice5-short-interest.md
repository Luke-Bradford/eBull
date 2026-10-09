# #3621 slice 5 — short-interest filter, and the v2 re-run on the clipped panel (spec addendum)

Status: **draft; Codex checkpoint 1 rounds 1–6 applied (§"Checkpoint log").** No book, differential or flag-conditioned
return involving short interest has been computed. Base spec: `docs/research/2026-10-08-3621-avoidance-filters.md`
("the base spec"); everything it fixes applies here unless this addendum amends it by name. v1 results: register
r28, `docs/research/2026-10-09-3621-avoidance-filters-results.md`.

## Why a v2, and what it adds

One declaration covers two things.

1. **The short-interest filter**, the base spec's slice 5. #3621 lists it as Drechsler & Drechsler 2014; it is
   measured here through the short interest ratio because we hold no lending-fee history (§"Source rules").
2. **A clean re-run of v1's five sets.** v1's micro and rest verdicts measured the holding-return defect #3730 (257
   panel name-months above +300%). #3730 Amendment 3 clips holding returns to JKP's return cutoffs and republished
   stage A as `factor_panel_3609/2026-10-09-af7889cc-stageA` (manifest sha256
   `e0924564ef1639f9473586523679894eb5d1741a75a3a3b8ddce5858917cae6e`). Stage B under Amendment 3 does not exist yet;
   it is built inside v2's declared run (#3730 close-out, 2026-10-09 05:08Z).

v1 (r28) stays frozen and its record stands as computed. v2 is a new trial, `3621-avoidance-filters-v2`.

## Question

The base spec's question, on the Amendment-3 panel, for seven filter sets: v1's five, plus **SI** (high short
interest ratio) and **all four** (MAX + sub-$5 + young + SI).

## Premises (measured)

**Provenance.** Run at `af7889cc` plus this branch's scripts:
- `PYTHONPATH=. uv run python -m scripts.measure_3621_short_interest_premise` (23 s): premises 2–5. It reads the
  stage-B artefact bound by step 2's capture (`2026-10-08-7b3169b6-stageB-2039b95f…`, the one v1 read): admitted rows'
  identity and ME fields, the retrospective `terminating` field (a printed coverage diagnostic only; it enters no
  state, flag, calibration or verdict), the frozen daily volume, split stamps, SPY sessions and NYSE cutoffs. It reads
  no holding return and no post-s(M) price. It reads all 78 in-coverage calendar files (2021-06-15..2024-08-30): 76
  stored, and `shrt20210615` and `shrt20210630` fetched from the CDN into memory; 38 are used by covered formations
  and all 78 feed the revision check. It writes the payload manifest `docs/research/3621-si-premise-payloads.csv`
  (each settlement's origin, sha256 and row count; sha256
  `1a15dd98dbfe76c073f74eb9a714d56eb74e5300afeedaad5fd543648d96805c`) and the exact reproduction reference
  (§"Slices", 5c): `docs/research/3621-si-premise-counts.csv` (sha256
  `a12c79f09487cdee099280ce538f9339458860909c05cfcbd96de977c848727e`) and the per-name file (124,634 lines, uncompressed
  sha256 `c005ca8a83b3c8e2cb7c38afd17a7db55e778ced8711ef636b0fcc8af0d91b6d`).
- `PYTHONPATH=. uv run python -m scripts.build_3621_finra_si_calendar`: the calendar (premise 1 and §"Source rules").
- Premise 6's CDN probes, quoted with their command. Premise 1's inventory is printed by the premise script.

Admission, ME and the frozen inputs do not change under Amendment 3 (#3730 acceptance: 0 rows differ outside
`prices.holding`), so these counts are what v2's run must reproduce on its own stage B (§"The study").

**1. Where the data lives, and the survivorship trap.**
- FINRA's bimonthly files are kept whole in `filing_raw_documents` (accession `FINRA_SI_<YYYYMMDD>`, kind
  `finra_short_interest_csv`, a kept kind): 125 non-null payloads, `FINRA_SI_20210715` to `FINRA_SI_20260915`. The
  premise script compares the stored accessions with the calendar's in-coverage settlements and prints the ones not
  stored: 2021-06-15 and 2021-06-30 only. The CDN serves both (premise 6).
- `finra_short_interest_observations` is **not usable for a backtest.** Its ingest resolves symbols against
  `instruments WHERE is_tradable = TRUE` at ingest time and drops the rest (`app/services/finra_short_interest_ingest.py:146`,
  `:293-296`), and every stored row was ingested between 2026-06-04 and 2026-10-08 (the premise script prints
  `min/max(known_from)`). An unlinked
  panel name has no `instrument_id` and so can never have a row there. The study re-parses the raw files, every row.

**2. Coverage on stage B.** 38 of stage B's 39 formations carry a usable settlement, 2021-06..2024-07 (2021-05 does
not, §"Source rules"). Medians over the 38, and the min–max of the valid share:

| population | admitted | unmatched | identity fail | unverifiable | flagged (SIR decile) | valid / admitted (min–max) |
|---|---|---|---|---|---|---|
| micro | 1,307 | 6.5 | 12.5 | 1 | 89 | 97.2%–99.1% |
| small | 926.5 | 2 | 5 | 0 | 162.5 | 94.6%–99.8% |
| large | 704.5 | 0 | 1 | 0 | 72 | 97.9%–100% |
| mega | 396.5 | 0 | 0 | 0 | 2 | 98.9%–100% |
| top 1,000 | 1,000 | 0 | 1 | 0 | 58.5 | 99.2%–100% |
| rest | 2,296.5 | 9 | 16 | 2 | 265.5 | 96.6%–99.3% |
| all admitted | 3,296.5 | 9 | 17 | 2 | 327 | 97.4%–99.5% |

No symbol was ambiguous at any formation. The lowest valid share, small at formation 2022-04, comes from that
formation's 75 identity failures, the most at any formation. Over the 38 formations, valid name-months by link
status: linked 109,467 of 110,288 (99.3%); unlinked 13,902 of 14,346 (96.9%); names whose series later terminates
17,824 of 18,326 (97.3%).

**3. Identity calibration (identity data, full period).** Over the 124,077 checked matches with both volumes positive,
ln(FINRA `averageDailyVolumeQuantity` / our ADV on FINRA's window, §"Source rules"): p1 −0.0270, p5 −0.0141, median
0.0000, p95 +0.0331, p99 +0.1124 (ratios 0.973 to 1.119). Identity failures by tolerance: 1,911 at 1.1, 1,111 at 1.15,
789 at 1.2, 621 at 1.25, 367 at 1.5, 262 at 2, 211 at 3. Over the 124,158 name-months whose unshifted window is
complete, shifting both ends of the window back one or two calendar days leaves 13 and 20 of them `unverifiable` and
raises identity failures at 1.2 from 789 to 11,853 and 22,154; failures rise with each shift at every formation. The glossary window is the one FINRA's figures agree with. The audit list (§"Source rules", identity) prints
the first match under each FINRA `issueName` per series, with its state and ratio. Two series whose ticker belonged
to another issuer earlier in the window are on it: RBC (our series RBC Bearings) matches "Regal Beloit" at
2021-06-15 with ratio 2.774, `identity_fail`, and "RBC Bearings" from 2022-12-15 at 1.000; RDUS matches "Radius
Health" at 2021-06-15 with ratio 0.998, accepted, and "Schnitzer Steel" from 2023-09-15 at 1.004. RDUS's 0.998 is
consistent with our series carrying Radius Health's 2021 volume, a historical-series identity question; volume
agreement does not establish it, and this study does not resolve it (§"Known limits").

**4. Revisions.** FINRA defines the flag: "The 'revision flag' indicates that the previous short interest position in
the security was revised since the prior reporting cycle" (Regulatory Notice 21-19, note 10). Its data catalog adds:
"When those corrections are made, a Revision Flag will appear next to the revised item. Only the most recent data is
made available" (finra.org/finra-data/browse-catalog/equity-short-interest). A flag on settlement S's row is
therefore news about the PRIOR settlement's figure, published with S. Whether a stored file still holds its figures
as first published is not documented; the script measures what it can. Over the 77 consecutive pairs of the 78
files, for each symbol carried once in both, it compares `previousShortPositionQuantity` with the prior stored file's
`currentShortPositionQuantity`: flagged rows disagree in 4,629 of 4,650; unflagged rows agree in 1,480,671 of
1,480,698. 54,530 rows have no comparable prior row (a symbol new to the file; 26 of them flagged). No physical row
of the 78 files has a blank `previousShortPositionQuantity` (counted over every row, and never read as zero), so a
`previous` of 0 below is a reported zero. So the stored
prior file does not hold the revision that the next file announces. That is consistent with the stored files being
first-published, and does not prove it: a later, unannounced change to either figure would not show here. The
residue the script prints: 19 of the 21 flagged agreements are at the 2021-06-30 and 2021-07-15 settlements, the
first two after FINRA's June 2021 change, and 2 at 2024-07-31 (AVGO and USLM); 21 of the 27 unflagged disagreements
have `previous` = 0, 9 of them the BATR, FWON and LSXM tracking-stock lines at 2023-07-31 and 2023-08-15. Physical flagged rows in the 38 files covered formations use: 0 to 6 in 35, 19 in one, 1,663 in
`shrt20231115`, and 7,600 of `shrt20210615`'s 20,251 (revisions to the 2021-05-28 figures, which no formation uses).
No file carries a symbol twice.

**5. The SIR distribution.** The decile cutoff q ran from 0.0795 (2021-11) to 0.1075 (2024-07). Values above 1 number
0–5 per formation. Names whose short count was carried across a split between settlement and s(M): 0–9 per formation.
FINRA's files carry almost no zero-position rows (0 in 73 of the 78 files, 1 in three, 29 in `shrt20231115`, 160 in
`shrt20210615`). Under Asquith, Pathak & Ritter's rule that unreported short interest is zero (§"Source rules"),
unmatched names would enter N at 0: 38 flags change over the 38 formations, at most 3 in one.

**6. FINRA's pre-June-2021 files are not used, though they hold exchange-listed rows.** FINRA's file catalog says:
"Prior to June 2021, the data contains short interest positions in over-the-counter securities only and does not
reflect short interest data in exchange-listed securities" (finra.org/finra-data/browse-catalog/equity-short-interest/files).
Probes on 2026-10-09, `curl -s -o /dev/null -w '%{http_code}' https://cdn.finra.org/equity/otcmarket/biweekly/shrt<YYYYMMDD>.csv`:
403 for 20140115, 20140613, 20160129, 20160729, 20161230, 20170131, 20170731, 20170815, 20170831, 20170915, 20170929,
20171013, 20171016, 20171031, 20171115, 20171130 and 20171215; 200 for 20171229, 20190131, 20210528, 20210615 and
20210630. `shrt20190131` carries exchange-listed rows (`awk -F'|' 'NR>1{print $5}' | sort | uniq -c` on
`marketClassCode`: 7,355 OTC, 3,088 NYSE, 2,356 NNM, 1,488 ARCA; AAPL and GME present). What those rows represent
is contradicted by the publisher's own statement, so they are excluded until an independent historical series
reconciles them. From the earliest file these probes found, the 40 stage-A formations 2018-01..2021-04 could have
used them; whether earlier files exist is unknown (only the dates listed were probed). That is future work.

## Source rules

| item | governing source | rule as published | our implementation |
|---|---|---|---|
| measure | Asquith, Pathak & Ritter 2005 (JFE 78): "short interest ratios (shares sold short/shares outstanding)"; Hong, Li, Ni, Scheinkman & Yan, NBER WP 21166: SR, "shares shorted to the shares outstanding" | short interest ratio | SIR = `currentShortPositionQuantity` × split product / `me.shares` |
| threshold | `market-segments.md` flag list: "short-interest decile (FINRA, 2021+)"; Hong et al. sort SR into deciles at each month-end | top decile | top decile over all admitted names with a value at M (⌈0.9 N⌉-th smallest; ties flagged), the base spec's MAX convention |
| availability | FINRA Rule 4560: reports due "no later than the second business day after the reporting settlement date designated by FINRA"; FINRA's published schedule gives each settlement's publication date | a designated list, not a formula (`review-prevention-log.md`, #2234) | the latest settlement whose publication is strictly before s(M), from `docs/research/3621-finra-si-calendar.csv` |
| coverage | FINRA file catalog (premise 6) | exchange-listed short interest is in FINRA's files from June 2021 | a formation is covered when its settlement is on or after 2021-06-15 |
| revisions | Regulatory Notice 21-19, note 10 | the flag says the previous settlement's position was revised since the prior cycle | the settlement's row is used as stored, whatever its flag; that the stored file is first-published is a registered assumption premise 4 supports; the next file's `previousShortPositionQuantity` is never consumed |
| unreported | Asquith, Pathak & Ritter 2005, appendix: missing short interest is set to zero, an imputation they make although their source omitted some stocks that had positions | zero | departure, registered: an `unmatched` name has no value (below); premise 5 measures the difference |
| ADV | FINRA's short-interest glossary: "Avg Daily Volume: Total Volume or Adjusted Volume in case of splits / Total trade days between (previous settlement date + 1) to (current settlement date). The NULL values are translated as zero." | that window and split basis | the SPY sessions after the previous calendar settlement through the row's settlement; volumes carried to the settlement's split basis |
| split basis | step 1 §"Split basis" (`factor_panel.split_product`) | shares_s = shares × Π split_factor over Intrader stamps after the basis date, on or before s(M) | the same product applied to the short count from its settlement to s(M), and to each session's volume from that session to the settlement |
| denominator | step 1 §"Market equity" (Scope) and its alignment matrix | `me.shares` is an issuer-level `dei:EntityCommonStockSharesOutstanding` count, at most 15 months old; class scope "not measurable" | inherited unchanged: SIR's numerator is FINRA's per-issue count, so for an issuer with another class the ratio can understate; unmeasured, registered |
| identity | none for this panel (below) | — | by construction, registered as an unverified mapping exception |

Details:
- **Why the ratio and not the fee.** Drechsler & Drechsler sort on the 30-day value-weighted lending fee from Markit
  (NBER WP 20282, 2004–2012). We hold no fee history: the IBKR borrow-fee archiver (#3622) is forward-only from
  2026-10, and settled decision #532 rules out paid vendor fee data (`data-sources/finra.md` §7). They call the short
  interest ratio "a noisy proxy for shorting demand"; their own long-sample proxy, SIRIO, divides by institutional
  holdings, which needs 13F history we hold only from 2024. This study measures the short interest ratio as Asquith,
  Pathak & Ritter and Hong et al. publish it, and claims nothing about fees.
- **Not chosen: days to cover.** Hong et al. find DTC (SR / average daily turnover) a stronger predictor than SR. The
  house flag is the SI decile and one definition is registered, so DTC is not tested. It is the obvious alternative,
  not a tried-and-rejected configuration.
- **Timing.** Asquith, Pathak & Ritter form portfolios on the prior month's ratio; Hong et al. use the mid-month
  figure; OSAP's `ShortInterest` takes the mid-month observation "to make sure Data would be available in real time"
  (`Signals/LegacyStataCode/DataDownloads/G_CompustatShortInterest.do`, OpenSourceAP/CrossSection `8db89244`). At every
  covered formation (2021-06..2024-07) the calendar resolves to month M's mid-month settlement, published before s(M).
  Slice 5a asserts this for covered formations and refuses otherwise.
- **Why coverage starts at formation 2021-06.** No calendar settlement on or after the catalog's June 2021 start is
  published before formation 2021-05's s(M), so it is `no_settlement` and the SI flag excludes nothing there.
  Formation 2021-06 uses `shrt20210615`; its ADV window starts after 2021-05-28, a calendar date only.
- **Revisions and point in time: a registered assumption.** Every payload was retrieved in 2026, and FINRA documents
  no vintage policy for its files beyond "only the most recent data is made available". The study assumes a stored
  file holds its figures as first published. Premise 4 supports this and cannot prove it; the report labels the
  inputs a retrospectively retrieved archive and prints the revision check with its residue. A revision of the
  selected settlement's figure is announced in the next file, published after s(M); the implementation never
  consumes that file's `previousShortPositionQuantity`. That the selected figure was not changed retrospectively by
  any other route remains the assumption.
- **Identity: an unverified mapping exception, registered.** FINRA keys its file by symbol at the settlement date.
  The panel row's `symbol` is the series' vendor symbol, one per series and not point in time; `instrument_symbol_history`
  starts in 2026, and no effective-dated symbol or issuer mapping exists for the panel's full population (the linkage's
  CIK has no counterpart in FINRA's file). So the mapping is: exact normalised-symbol match, corroborated by FINRA's ADV
  agreeing with ours within a factor of 1.2 (|ln ratio| ≤ ln 1.2) on FINRA's own window. A wrong-issuer match can still
  pass when the two volumes agree, as RDUS's does (premise 3), and a ticker change leaves a name unmatched. The
  mapping accuracy is therefore not certified; the run prints the full-population audit list of series whose matched
  FINRA `issueName` changes over the window, and the report states the exception. Names are compared after
  uppercasing and deleting every character outside A–Z and 0–9; for each series the list shows the first settlement
  at which each normalised name appears, untruncated, with its state and ratio. In the premise run 163 of 4,223 matched series show
  more than one normalised name, 150 of them among accepted matches, RDUS included.
- **Calibration status.** The 1.2 tolerance and condition 5's 90% floor were chosen after seeing premises 2 and 3 over
  the full 2021–2024 period. Both read identity and coverage data only, never an outcome, but they are full-period
  exploratory calibrations, not chronologically held-out choices, and the report labels them so.
- **Our ADV, frozen to FINRA's formula.** The window is the SPY sessions after the previous calendar settlement through
  the row's settlement, and the denominator is their count, as FINRA divides total volume by total trade days. A
  session's volume counts when the series has a usable bar there (`usable` true in the frozen daily input, volume
  non-null, finite and ≥ 0); an observed zero counts as zero. FINRA translates its own NULLs to zero, but a missing
  vendor bar is not a FINRA NULL, so a window with any session lacking a usable bar makes the name `unverifiable`
  rather than renormalising. Our ADV is Σ volume × `split_product(splits, session, settlement)` over the window ÷ the
  session count. If exactly one of FINRA's ADV and ours is zero the identity fails; if both are zero it passes.
- **Missing values, in precedence order:** `ambiguous` (an empty normalised symbol, one shared by two admitted names
  at M, or one carried by two rows of the file used); `unmatched`; `unverifiable`; `identity_fail`. None has a value
  or a flag, and none enters N. For `unmatched` this departs from Asquith, Pathak & Ritter, who impute zero to missing
  short interest. Here a name may be absent because its vendor symbol was not its ticker at the settlement, and no
  effective-dated symbol history can tell that apart from a genuinely unreported position, so no value is imputed.
  Either way the name is unflagged; the rules differ only in N, and premise 5 measures that difference (38 flags
  over 38 formations). This is a deliberate policy
  difference from MAX: the base spec flags a screened MAX window, which holds an extreme move or a data defect, because
  excluding it is the construction under test; SI keeps a name whose ratio cannot be established, because a missing
  or unconfirmed FINRA row is no evidence of shorting. Each reason is counted separately.
- **Refusals (the run stops):** a selected payload missing from both the store and the CDN; a body `settlementDate`
  differing from the file's date; a negative short count or ADV, or a non-integer field; an admitted row with
  non-positive or non-finite `me.shares`; a covered formation with N = 0 valid ratios or q ≤ 0; a covered formation
  whose settlement is not month M's mid-month calendar settlement; a calendar that does not start at 2021-05-28, lacks
  exactly two settlements in any month 2021-06..2024-08, holds one outside that span, or has duplicate, unordered or
  non-increasing dates. Values above 1 are kept as reported.

## The study (amendments to the base spec)

- **Artefacts.** Stage A: `2026-10-09-af7889cc-stageA`. Stage B: built inside v2's declared run through step 2's
  capture path under v2's trial id, with Amendment 3; its manifest digest goes in the run's ledger rows.
- **Identity of the rebuilt stage B (refuses otherwise).** Against v1's stage B (manifest `3eee1005…`): every row,
  admitted or not, keyed by (`M`, `name_key`), with the same key set; each row recursively equal to its v1 row after
  deleting the `prices.holding` subtree from both, so any other added, missing or changed field refuses. Every frozen
  input v1's stage B lists is byte-equal in the new one, and the new one's only additional input is
  `inputs/reference_snapshot_jkp_return_cutoffs.jsonl.gz` (Amendment 3).
- **Filter sets: seven.** MAX; sub-$5; young; sub-$5 + young; MAX + sub-$5 + young; SI; MAX + sub-$5 + young + SI.
  No other combination is eligible from this study.
- **Populations, books, decision rule, diagnostics, windows and costs:** the base spec's, including the MAX fidelity
  check, which reruns on the clipped stage A against the unchanged JKP series.
- **Condition 1 on SI sets.** U and U_F of the SI set are identical through formation 2021-05. The base spec books a
  formation's trade costs in month M, so formation 2021-06's SI trades fall in month 2021-06: the SI set's whole-path
  ΔG is its 2021-06..2024-08 ΔG times 39/119 (39 of the path's 119 monthly returns), per arm and cost scenario. Condition 1 adds the
  stress-cost and both-arm checks but no earlier-regime evidence, and the SI verdict rests on 38 formations. For "all four",
  condition 1 evaluates the combined set; it does not establish SI's increment over MAX + sub-$5 + young, which the
  report prints as a diagnostic without a verdict: G("all four") − G("all three") for each population, on every
  window, arm and cost scenario of the base spec's §"Diagnostics", with its wealth-exhaustion rule.
- **Condition 5 (SI sets only), a coverage floor:** at every covered formation, names with an SI value are at least
  90% of U(P, M). It is a house regression guard on the identity and coverage pipeline, not validation of identity
  accuracy; premise 2's lowest observed share was 94.6% (small, 2022-04). The bound holds for U, the unfiltered
  target; in U_F the valueless names' share is higher, since the flagged names leave. The report prints, per formation
  and SI-containing book, the weight of names without an SI value. A population below the floor is NOT ELIGIBLE with
  condition 5 named.
- **No SI fidelity check.** JKP has no short-interest characteristic. OSAP publishes `ShortInterest`, but #3623's OSAP
  load has not run, and its portfolios are quintiles on Compustat short interest, not our decile. The report states
  that no factor-level fidelity exists for SI.
- **Cross-source check (`.claude/CLAUDE.md` §"Corpus changes"), fixed now.** The figure: `currentShortPositionQuantity`
  for AAPL and GME at settlement 2024-07-15, from the stored payload. The source: Nasdaq's short-interest history page
  for each symbol (`nasdaq.com/market-activity/stocks/<symbol>/short-interest`); when the live page no longer lists
  that settlement, the Wayback snapshot of it nearest after 2024-07-24 (the publication date) that lists it.
  Acceptance: both share counts are equal. An unequal count, or no source listing the settlement, fails the check and
  5c does not declare; only a reviewed addendum (checkpoint 1) that changes the source or registers the exception,
  made before the declaration, can replace this rule. An explanation alone does not pass it.
- **SI diagnostics:** per formation and population, the counts of `docs/research/3621-si-premise-counts.csv`'s
  columns (each state, `flagged`; `split_carried`: valid names with any split stamp in (settlement, s(M)], whatever
  the net product), q, values above 1, q and the flag changes under the unreported-is-zero rule, the revision-check
  table with its residue, the overlap of SI flags with each v1 filter, and the identity audit list.

## Inheritance contract (amended)

The base spec's §"Inheritance contract" applies with these numbers: v2 has **7 sets × 6 populations = 42 verdict
cells** and **86 searches**. A later book adopting any set lists **both** versions' candidate inventories and verdicts
(v1's 30 cells, v2's 42) in its design history; this study's cumulative register contribution is 62 + 86 = 148
searches. The base spec's verdict line (§"Decision rule") lists v2's 42 pairs.

## Samples and design history (additions)

- Seen before this addendum: everything the base spec lists; v1's run `2590008207b6` (all 30 verdicts, every ΔG, and
  the micro/rest defect reads 743 and 744); #3730's acceptance tables (holding-jump counts and micro worst-case growth
  1.692% → 0.988% on stage A, `scripts.measure_3730_holding_jumps`); and this addendum's premises.
- The v1 sets are re-run on data whose v1 outcomes are known: their v2 verdicts are retrospective twice over, and the
  reuse clause applies. The clip was motivated by micro and rest, where v1's defect concentrated (#3730); which v2
  cells it moves is a v2 result, reported per cell against v1.
- **Prior access, disclosed and not backdated:** the premise run read stage B's admitted rows (identity, ME and the
  `terminating` diagnostic), daily volume, split stamps, sessions and NYSE cutoffs; no holding return.
- **Execution access:** a `strategy_holdout_accesses` row is written after the declaration and before v2's first read
  of either artefact, including the reproduction of premise 2.

## Registration

- One trial, `3621-avoidance-filters-v2`, non-claiming, no `TrialDesign`, as v1.
- **Searches: 86** = 7 sets × 6 populations × 2 arms (84) + the MAX fidelity check's 2 arms. v1's 62 stay counted.
- **Evidence pins:** this addendum's and the base spec's sha256; stage A's manifest digest; v1's stage-B manifest
  digest (the identity the rebuilt stage B must match); the step 0 manifest; the calendar CSV's sha256; the sha256 of
  the payload manifest `docs/research/3621-si-premise-payloads.csv` (78 payloads); the construction hash; the JKP and OSAP code commits cited.
- **Ledger:** `docs/research/3621-ledger.jsonl`, v2 rows beside v1's.

## Slices

- **5a. Fixture-only.** The SI flag as pure functions: calendar validation and resolution, symbol identity, our ADV
  (window, completeness, zeros, split carry), split carry of the count, decile, the revision check. Fixtures: each
  missing state; each refusal, including a calendar missing its first month; formation 2021-05 resolving to
  `no_settlement` and 2021-06 to `shrt20210615`; a flagged row used as stored; a split between settlement and s(M);
  a split inside the ADV window; a ticker-reuse case that fails and one that passes the volume check. Capture `shrt20210630` into
  `filing_raw_documents` through the existing raw-store path (raw only, no observations row), and the same for
  `shrt20210615`; both must hash to the manifest's sha256.
- **5b. Fixture-only.** The v2 run script: the rebuilt-stage-B identity refusal, seven sets, condition 5, the SI
  diagnostics and the audit list.
- **5c.** Declaration (r29), execution access, then the reproduction (the run stops on any difference): the slice 5a
  implementation, run on v2's stage B, reads the 78 payloads of `docs/research/3621-si-premise-payloads.csv` and
  refuses any missing or with a different sha256, revision-check-only files included; it must write a counts table equal to
  `docs/research/3621-si-premise-counts.csv` cell for cell and a per-name file (`M`, `name_key`, state, settlement
  used, SIR, flagged) whose uncompressed sha256 equals the premise run's (§"Premises"). Its serialisation and SIR
  arithmetic are those the premise script's docstring fixes (order, `json.dumps` with compact separators, a trailing
  newline, Python float repr, SIR = `float(short × Decimal split product) / float(shares)`); slice 5a implements them
  and 5c's reproduction is the test. Then the run, report,
  ledger, and the verdict record beside the defaults in `market-segments.md` (posted on #2403 from a loop session).

## Known limits

- 38 formations of SI in one regime; a descriptive screen, never significance (base spec premise 3).
- The identity mapping is an unverified exception (above). RDUS's accepted 2021 match is consistent with a series
  whose early volume is another issuer's (premise 3); this study records it and does not resolve it.
- Point in time rests on the registered assumption that stored files are first-published, which premise 4 supports.
- SIR's denominator is step 1's issuer-level share count, up to 15 months old, class scope unmeasured (§"Source rules").
- FINRA's pre-June-2021 exchange-listed rows are unused (premise 6). `data-sources/finra.md` §2.10 says the
  pre-June-2021 archive is OTC-only; the files hold exchange-listed rows, and the publisher's statement is about what
  they reflect. That correction, and one to its §5 on the revision flag, are posted on #2403.
- Spread-only costs, as the base spec.

## Checkpoint log

**Round 1 (Codex, 2026-10-09): 14 findings, all applied.**
- **1:** identity is registered as an unverified mapping exception, the guarantees are removed, and the run prints a
  full-population audit list; the volume tolerance tightens from 3 to 1.2 after the audit found RDUS and RBC.
- **2:** our ADV uses FINRA's glossary window and split basis; the calendar gains the 2021-05-28 predecessor; the
  premises are remeasured.
- **3:** the volume denominator, zeros, missing bars and the 80% completeness floor are frozen.
- **4:** revised rows fall back to the previous settlement's unflagged row; the archive's retrieval date and the
  marking limit are disclosed.
- **5:** both thresholds are labelled full-period calibrations; the gate becomes a 90% regression guard.
- **6:** slices 5a and 5b are fixture-only; the reproduction moves after the declaration and access.
- **7:** the mid-month assertion covers covered formations only, with `no_settlement` fixtures.
- **8:** the inheritance numbers are amended.
- **9:** the refusals are listed.
- **10:** the rebuilt-stage-B comparison is defined field by field.
- **11:** provenance is split by command, and the fetch disclosure corrected.
- **12:** the `terminating` read is disclosed.
- **13:** the MAX contrast is reworded as a policy difference.
- **14:** the recoverable stage-A count is 40.

**Round 2 (Codex, 2026-10-09): 11 findings, all applied; premises 2–5 remeasured.**
- **1:** our ADV divides by every session of FINRA's window; a window session without a usable bar makes the name
  `unverifiable` instead of renormalising (this replaces round 1's 80% completeness floor).
- **2:** the premise script refuses a covered formation whose settlement is not M's mid-month settlement.
- **3:** the exact reproduction reference is `docs/research/3621-si-premise-counts.csv` (every state count per
  formation and population) plus the per-name file's sha256 (§"Slices", 5c).
- **4:** the rebuilt-stage-B comparison is recursive over every row, admitted or not, minus `prices.holding`.
- **5:** the window-shift and wrong-issuer ratio diagnostics, and the context file `shrt20210615`, are in the cited
  command; premises 3 and 4 quote its output.
- **6:** the availability bound is stated as "the earliest file these probes found".
- **7:** the usable-bar predicate is defined (`usable`, volume non-null, finite, ≥ 0).
- **8:** the calendar reader refuses duplicate or unordered settlements, non-increasing publications and any month
  without exactly two settlements; the fallback is the previous settlement by calendar index.
- **9:** `fallback` and `split_carried` are defined as counted (§"The study", SI diagnostics).
- **10:** file row and revision counts are physical rows; duplicated symbols are counted separately and are ambiguous.
- **11:** the SI-increment diagnostic is fixed to every window, arm and cost scenario of the base spec.

**Round 3 (Codex, 2026-10-09): 10 findings, all applied; premises 1–6 remeasured.**
- **1:** FINRA's note defines the flag as a revision of the PRIOR settlement's figure. The fallback is removed; the
  settlement's row is used as stored. The script measures, on all 77 file pairs, that stored files are the
  first-published vintage (premise 4). With no fallback, formation 2021-06 is covered: 38 formations.
- **2:** Asquith, Pathak & Ritter's unreported-is-zero rule is cited, the departure registered with its reason, and
  its effect on the flags measured (premise 5).
- **3:** condition 5's bound is stated for U only, and the valueless names' weight in each SI book is printed.
- **4:** the calendar reader fixes the span (start 2021-05-28; two settlements in every month 2021-06..2024-08; none
  outside).
- **5, 6:** the denominator is step 1's issuer-level, at most 15-month-old share count; its class scope is inherited
  as unmeasured, and the filing-period bound is removed.
- **7:** the per-name serialisation and SIR arithmetic are fixed in the premise script's docstring and cited by 5c.
- **8:** the fallback count is gone with the fallback.
- **9:** the inventory query, its calendar comparison and the CDN probes are quoted with their commands and full date
  list.
- **10:** the design history no longer predicts which v2 cells the clip moves.

**Round 4 (Codex, 2026-10-09): 8 findings, all applied.**
- **1:** first-published vintage is a registered assumption that premise 4 supports, not a measured fact; the check's
  uncompared rows and residue are printed and summarised; FINRA's catalog wording is quoted.
- **2:** condition 1's scaling is 39/119 from month 2021-06, where formation 2021-06's trade costs fall.
- **3:** Asquith, Pathak & Ritter's zero is described as their imputation; the departure's reason is "may", not a
  measured likelihood.
- **4:** the premise run writes a payload manifest (78 sha256s); slice 5a captures both June 2021 files; 5c refuses
  any payload missing from or differing with the manifest.
- **5:** the cross-source check's figure, source, vintage rule, acceptance and consequence are fixed.
- **6:** the inventory comparison and the `known_from` range are printed by the premise script.
- **7:** RDUS is "consistent with", not a diagnosis.
- **8:** the audit's name normalisation and first-occurrence rule are fixed in the spec.

**Round 5 (Codex, 2026-10-09): 5 findings, all applied.**
- **1:** the next file's `previousShortPositionQuantity` is never consumed, and the absence of other retrospective
  changes stays an assumption; the script docstring's timing is corrected (a flag in the selected file concerns the
  settlement before it).
- **2:** a blank `previousShortPositionQuantity` is kept as missing and counted apart; there are none in the 78 files.
- **3:** the cross-source check passes only on equality; a failure needs a reviewed pre-declaration addendum.
- **4:** the script prints the stored accession range.
- **5:** the audit prints normalised names untruncated.

**Round 6 (Codex, 2026-10-09): 2 findings, both applied.**
- **1:** blank `previousShortPositionQuantity` is counted over every physical row of all 78 files (0).
- **2:** the window-shift diagnostic separates `unverifiable` from `identity_fail` and names its cohort.
