# #3621 slice 5 — short-interest filter, and the v2 re-run on the clipped panel (spec addendum)

Status: **draft; Codex checkpoint 1 rounds 1 and 2 applied (§"Checkpoint log").** No book, differential or flag-conditioned
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

**Provenance.** Three commands, run at `af7889cc` plus this branch's scripts:
- `PYTHONPATH=. uv run python -m scripts.measure_3621_short_interest_premise`: premises 2–5. It reads the stage-B
  artefact bound by step 2's capture (`2026-10-08-7b3169b6-stageB-2039b95f…`, the one v1 read): admitted rows'
  identity and ME fields, the retrospective `terminating` field (a printed coverage diagnostic only; it enters no
  state, flag, calibration or verdict), the frozen daily volume, split stamps, SPY sessions and NYSE cutoffs. It reads
  no holding return and no post-s(M) price. It read 73 stored FINRA payloads and fetched two from the CDN into memory
  (`shrt20210630`, which formation 2021-07 uses, and `shrt20210615`, read for its revision count only), and prints each
  payload's sha256. It writes the exact reproduction reference (§"Slices", 5c): `docs/research/3621-si-premise-counts.csv`
  (sha256 `81b7a0e5f5c1b2368c3e0f3b37a7808507e47a6d456d93c755e083688243f23a`) and the per-name file (121,463 lines,
  uncompressed sha256 `35d60d0a5e58a1c9815d3bc38e53faef18059334342a516c6ef29271c635e42f`).
- `PYTHONPATH=. uv run python -m scripts.build_3621_finra_si_calendar`: the calendar (premise 1 and §"Source rules").
- The inventory queries and CDN probes in premise 1, quoted with it.

Admission, ME and the frozen inputs do not change under Amendment 3 (#3730 acceptance: 0 rows differ outside
`prices.holding`), so these counts are what v2's run must reproduce on its own stage B (§"The study").

**1. Where the data lives, and the survivorship trap.**
- FINRA's bimonthly files are kept whole in `filing_raw_documents` (accession `FINRA_SI_<YYYYMMDD>`, kind
  `finra_short_interest_csv`, a kept kind): 125 files, 2021-07-15 to 2026-09-15, every half-month present
  (`SELECT count(*), min(accession_number), max(accession_number) FROM filing_raw_documents WHERE document_kind =
  'finra_short_interest_csv'`; all 125 payloads non-null). The 2021-06 files are not stored; the CDN serves
  `shrt20210615` and `shrt20210630` (HTTP 200, probed 2026-10-09).
- `finra_short_interest_observations` is **not usable for a backtest.** Its ingest resolves symbols against
  `instruments WHERE is_tradable = TRUE` at ingest time and drops the rest (`app/services/finra_short_interest_ingest.py:146`,
  `:293-296`), and every stored row was ingested between 2026-06-04 and 2026-10-08 (`min/max(known_from)`). An unlinked
  panel name has no `instrument_id` and so can never have a row there. The study re-parses the raw files, every row.

**2. Coverage on stage B.** 37 formations carry a usable settlement, 2021-07..2024-07 (2021-05 and 2021-06 do not,
§"Source rules"). Medians over the 37, and the min–max of the valid share:

| population | admitted | unmatched | identity fail | unverifiable | flagged (SIR decile) | valid / admitted (min–max) |
|---|---|---|---|---|---|---|
| micro | 1,311 | 6 | 13 | 1 | 89 | 97.2%–99.1% |
| small | 924 | 2 | 5 | 0 | 162 | 94.6%–99.8% |
| large | 699 | 0 | 1 | 0 | 72 | 97.9%–100% |
| mega | 397 | 0 | 0 | 0 | 2 | 98.9%–100% |
| top 1,000 | 1,000 | 0 | 1 | 0 | 59 | 99.2%–100% |
| rest | 2,302 | 9 | 16 | 2 | 266 | 96.6%–99.3% |
| all admitted | 3,302 | 9 | 17 | 2 | 327 | 97.4%–99.5% |

No symbol was ambiguous and no name ended `revised` at any formation. The lowest valid share, small at formation
2022-04, comes from that formation's 75 identity failures, the most at any formation. Over the 37 formations, valid
name-months by link status: linked 106,908 of 107,701 (99.3%); unlinked 13,329 of 13,762 (96.9%); names whose series
later terminates 17,087 of 17,576 (97.2%).

**3. Identity calibration (identity data, full period).** Over the 120,930 checked matches with both volumes positive,
ln(FINRA `averageDailyVolumeQuantity` / our ADV on FINRA's window, §"Source rules"): p1 −0.0272, p5 −0.0142, median
0.0000, p95 +0.0337, p99 +0.1136 (ratios 0.973 to 1.120). Identity failures by tolerance: 1,884 at 1.1, 1,092 at 1.15,
769 at 1.2, 605 at 1.25, 353 at 1.5, 248 at 2, 199 at 3. Shifting both ends of the window back one or two calendar
days raises failures at 1.2, summed over the 37 covered formations, from 769 to 11,719 and 21,720; every formation
rises. The glossary window is the one FINRA's figures agree with. The audit list (§"Source rules", identity) prints
the first match under each FINRA `issueName` per series, with its state and ratio; two known wrong-issuer matches,
where a vendor symbol was another issuer's ticker earlier in the window, are on it: RDUS at 2021-07-15 (Radius
Health; our series is Radius Recycling, formerly SCHN), ratio 0.869, accepted; RBC at 2021-07-15 (Regal Beloit; our
series is RBC Bearings), ratio 6.395, `identity_fail`.

**4. Revisions.** FINRA revises published rows in place and marks them (`revisionFlag` non-blank). Physical flagged
rows per file, over the 74 files covered formations use: 0 to 21 in 67 files; 46, 57, 82, 509, 554, 1,554 and 1,663
in the other seven (the largest `shrt20231115`). The context file `shrt20210615` carries 7,600 of 20,251. No file
carries a symbol twice. The fallback rule (§"Source rules") resolves every flagged panel match at a covered formation,
342 of them at formation 2023-11, so no name ends `revised`.

**5. The SIR distribution.** The decile cutoff q ran from 0.0795 (2021-11) to 0.1075 (2024-07). Values above 1 number
0–5 per formation. Names whose short count was carried across a split between settlement and s(M): 0–9 per formation.

**6. FINRA's pre-June-2021 files are not used, though they hold exchange-listed rows.** FINRA's file catalog says:
"Prior to June 2021, the data contains short interest positions in over-the-counter securities only and does not
reflect short interest data in exchange-listed securities" (finra.org/finra-data/browse-catalog/equity-short-interest/files).
CDN probes on 2026-10-09: `shrt20171229` is served, and 2017-08-15, 08-31, 09-15, 09-29, 10-13, 10-16, 10-31, 11-15,
11-30 and 12-15 return 403; 2016-01-29, 2016-07-29, 2016-12-30, 2017-01-31, 2017-07-31 and 2014 dates also return
403. The served files carry NYSE and Nasdaq rows (`shrt20190131`: 3,088 NYSE, 2,356 NNM, with AAPL and GME present).
What those rows represent is contradicted by the publisher's own statement, so they are excluded until an independent
historical series reconciles them. From the earliest file these probes found, the 40 stage-A formations 2018-01..2021-04
could have used them; whether earlier files exist is unknown (only the dates listed were probed). That is future
work, not used here.

## Source rules

| item | governing source | rule as published | our implementation |
|---|---|---|---|
| measure | Asquith, Pathak & Ritter 2005 (JFE 78): "short interest ratios (shares sold short/shares outstanding)"; Hong, Li, Ni, Scheinkman & Yan, NBER WP 21166: SR, "shares shorted to the shares outstanding" | short interest ratio | SIR = `currentShortPositionQuantity` × split product / `me.shares` |
| threshold | `market-segments.md` flag list: "short-interest decile (FINRA, 2021+)"; Hong et al. sort SR into deciles at each month-end | top decile | top decile over all admitted names with a value at M (⌈0.9 N⌉-th smallest; ties flagged), the base spec's MAX convention |
| availability | FINRA Rule 4560: reports due "no later than the second business day after the reporting settlement date designated by FINRA"; FINRA's published schedule gives each settlement's publication date | a designated list, not a formula (`review-prevention-log.md`, #2234) | the latest settlement whose publication is strictly before s(M), from `docs/research/3621-finra-si-calendar.csv` |
| coverage | FINRA file catalog (premise 6) | exchange-listed short interest is in FINRA's files from June 2021 | settlements from 2021-06-15; a formation is covered only when its settlement and the one before it both qualify |
| revisions | FINRA marks revised rows (`revisionFlag`) | a flagged value was changed after first publication | a flagged row is replaced by the same symbol's unflagged row at the previous calendar settlement, or the name has no value |
| ADV | FINRA's short-interest glossary: "Avg Daily Volume: Total Volume or Adjusted Volume in case of splits / Total trade days between (previous settlement date + 1) to (current settlement date). The NULL values are translated as zero." | that window and split basis | the SPY sessions after the previous calendar settlement through the row's settlement; volumes carried to the settlement's split basis |
| split basis | step 1 §"Split basis" (`factor_panel.split_product`) | shares_s = shares × Π split_factor over Intrader stamps after the basis date, on or before s(M) | the same product applied to the short count from its settlement to s(M), and to each session's volume from that session to the settlement |
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
  covered formation (2021-07..2024-07) the calendar resolves to month M's mid-month settlement, published seven business days after it (FINRA Regulatory
  Notice 21-19) and before s(M). Slice 5a asserts this for covered formations and refuses otherwise.
- **Why coverage starts at formation 2021-07.** Formation 2021-06 would use `shrt20210615`, whose revision fallback,
  2021-05-28, predates the catalog's June 2021 start; 7,600 of that file's rows are flagged revised (premise 4).
  Formation 2021-05 has no settlement on or after 2021-06-15 published before its s(M). Both are `no_settlement`, so
  the SI flag excludes nothing there.
- **Revisions and point in time.** Every payload was retrieved in 2026 (the 2021-06-30 one on 2026-10-09). FINRA serves
  the current version, so the original published vintage is not held. A flagged row is not used; an unflagged row is
  taken to be the value as first published. A revision FINRA did not flag is undetectable here, and the report says
  the inputs are a retrospectively retrieved archive, point in time to FINRA's revision marking.
- **Identity: an unverified mapping exception, registered.** FINRA keys its file by symbol at the settlement date.
  The panel row's `symbol` is the series' vendor symbol, one per series and not point in time; `instrument_symbol_history`
  starts in 2026, and no effective-dated symbol or issuer mapping exists for the panel's full population (the linkage's
  CIK has no counterpart in FINRA's file). So the mapping is: exact normalised-symbol match, corroborated by FINRA's ADV
  agreeing with ours within a factor of 1.2 (|ln ratio| ≤ ln 1.2) on FINRA's own window. A wrong-issuer match can still
  pass when the two volumes happen to agree: RDUS at settlement 2021-07-15 (ratio 0.869) does, while RBC's at the same
  settlement (ratio 6.395) fails. A ticker change leaves a name unmatched. The mapping accuracy is
  therefore not certified; the run prints the full-population audit list of series whose accepted FINRA `issueName`
  changes over the window (in the premise run: 158 of 4,202 matched series show more than one name, 146 of them
  among accepted matches, RDUS included), and
  the report states the exception.
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
  at M, or one carried by two rows of the file used); `unmatched`; `revised` (flagged, and the fallback row absent,
  ambiguous or flagged); `unverifiable`; `identity_fail`. None has a value or a flag. This is a deliberate policy
  difference from MAX: the base spec flags a screened MAX window, which holds an extreme move or a data defect, because
  excluding it is the construction under test; SI keeps a name whose ratio cannot be established, because a missing
  or unconfirmed FINRA row is no evidence of shorting. Each reason is counted separately.
- **Refusals (the run stops):** a selected payload missing from both the store and the CDN; a body `settlementDate`
  differing from the file's date; a negative short count or ADV, or a non-integer field; an admitted row with
  non-positive or non-finite `me.shares`; a covered formation with N = 0 valid ratios; a covered formation whose
  settlement is not month M's mid-month calendar settlement. Values above 1 are kept as reported.

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
- **Condition 1 on SI sets.** U and U_F of the SI set are identical through formation 2021-06, so its whole-path ΔG is
  its 2021-07..2024-08 ΔG times 38/119 (38 of the path's 119 monthly returns). Condition 1 adds the stress-cost and
  both-arm checks but no earlier-regime evidence, and the SI verdict rests on 37 formations. For "all four",
  condition 1 evaluates the combined set; it does not establish SI's increment over MAX + sub-$5 + young, which the
  report prints as a diagnostic without a verdict: G("all four") − G("all three") for each population, on every
  window, arm and cost scenario of the base spec's §"Diagnostics", with its wealth-exhaustion rule.
- **Condition 5 (SI sets only), a coverage floor:** at every covered formation, names with an SI value are at least
  90% of U(P, M). It is a house regression guard on the identity and coverage pipeline, not validation of identity
  accuracy; premise 2's lowest observed share was 94.6% (small, 2022-04). At 90% the unflagged-for-want-of-data names are a tenth of
  the book at most. A population below it is NOT ELIGIBLE with condition 5 named.
- **No SI fidelity check.** JKP has no short-interest characteristic. OSAP publishes `ShortInterest`, but #3623's OSAP
  load has not run, and its portfolios are quintiles on Compustat short interest, not our decile. The build PR records
  a cross-source check of one FINRA figure against an independent source (`.claude/CLAUDE.md` §"Corpus changes"), and
  the report states that no factor-level fidelity exists for SI.
- **SI diagnostics:** per formation and population, the counts of `docs/research/3621-si-premise-counts.csv`'s
  columns (each state, `flagged`; `fallback`: valid names whose value came from the fallback row; `split_carried`:
  valid names with any split stamp in (settlement, s(M)], whatever the net product), q, values above 1, the overlap of
  SI flags with each v1 filter, and the identity audit list.

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
  reuse clause applies. The micro and rest cells are the ones the clip changes materially.
- **Prior access, disclosed and not backdated:** the premise run read stage B's admitted rows (identity, ME and the
  `terminating` diagnostic), daily volume, split stamps, sessions and NYSE cutoffs; no holding return.
- **Execution access:** a `strategy_holdout_accesses` row is written after the declaration and before v2's first read
  of either artefact, including the reproduction of premise 2.

## Registration

- One trial, `3621-avoidance-filters-v2`, non-claiming, no `TrialDesign`, as v1.
- **Searches: 86** = 7 sets × 6 populations × 2 arms (84) + the MAX fidelity check's 2 arms. v1's 62 stay counted.
- **Evidence pins:** this addendum's and the base spec's sha256; stage A's manifest digest; v1's stage-B manifest
  digest (the identity the rebuilt stage B must match); the step 0 manifest; the calendar CSV's sha256; the sha256 of
  each FINRA payload the run uses (74 in the premise run); the construction hash; the JKP and OSAP code commits cited.
- **Ledger:** `docs/research/3621-ledger.jsonl`, v2 rows beside v1's.

## Slices

- **5a. Fixture-only.** The SI flag as pure functions: calendar resolution with the coverage pair, revision fallback,
  symbol identity, our ADV (window, completeness, zeros, split carry), split carry of the count, decile. Fixtures:
  each missing state; each refusal; formations 2021-05 and 2021-06 resolving to `no_settlement`; a split between
  settlement and s(M); a split inside the ADV window; a revised row with a clean, a flagged and an absent fallback; a
  ticker-reuse case that fails and one that passes the volume check. Capture `shrt20210630` into
  `filing_raw_documents` through the existing raw-store path (raw only, no observations row).
- **5b. Fixture-only.** The v2 run script: the rebuilt-stage-B identity refusal, seven sets, condition 5, the SI
  diagnostics and the audit list.
- **5c.** Declaration (r29), execution access, then the reproduction (the run stops on any difference): the slice 5a
  implementation, run on v2's stage B with the same payloads (their sha256 pinned), must write a counts table equal to
  `docs/research/3621-si-premise-counts.csv` cell for cell and a per-name file (`M`, `name_key`, state, settlement
  used, SIR, flagged; canonical JSON lines) whose sha256 equals the premise run's (§"Premises"). Then the run, report,
  ledger, and the verdict record beside the defaults in `market-segments.md` (posted on #2403 from a loop session).

## Known limits

- 37 formations of SI in one regime; a descriptive screen, never significance (base spec premise 3).
- The identity mapping is an unverified exception (above); one wrong-issuer pass is known.
- Revisions are point in time only to FINRA's revision marking.
- SIR's denominator is the panel's XBRL share count, which can lag an issuance by up to a filing period.
- FINRA's pre-June-2021 exchange-listed rows are unused (premise 6). `data-sources/finra.md` §2.10 says the
  pre-June-2021 archive is OTC-only; the files hold exchange-listed rows, and the publisher's statement is about what
  they reflect. The skill correction is posted on #2403.
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
