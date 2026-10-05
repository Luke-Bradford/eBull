# #3609 step 1 — point-in-time factor panel and construction fidelity

Status: spec, revised after two rounds of Codex checkpoint 1 (54 + 56 findings, all applied; see §"Checkpoint log").
Nothing has been built and no factor return has been computed.
Programme: `docs/research/2026-10-04-strategy-research-sweep.md` §4 item 1. Prior step:
`docs/research/2026-10-04-3609-step0-baselines.md`, which this PR amends (§"Amendment 3" there).

## Question

Can we build, from our own point-in-time data, the published characteristics that step 2 will backtest, closely enough
that our long-short portfolios track the published series? Step 1 builds the panel and measures that tracking.
- It computes no strategy outcome.
- It never prints or stores our series' mean, alpha, t-statistic or cumulative return.

**What the answer can and cannot say.** #2901's rebuilt GP/A spread correlated +0.199 with global-q's
(`docs/proposals/ta/2026-09-25-2901-quality-result.md`), and the cause was never identified. Step 1 follows one
published construction, JKP, rule by rule. Every difference it cannot remove is in §"Alignment matrix", printed with
each result.
- A pass means our construction tracks JKP's on our restricted universe. It is not evidence of a premium.
- A failure is not by itself proof of a defect (`research-process.md` §Fidelity).

**Estimand.** Every result describes the **restricted population** of §"Universe": 10-K/10-Q filers that our archive
prices and our linkage identifies. It is not the CRSP universe, and nothing is generalised to the published universe
without separate evidence.

## Premise checks (full population, 2026-10-04)

**1. Survivorship has three regimes.**
- **Before 2014-09: survivor-conditioned.**
  - Of 22,879 `icyDenev/Intrader` series, one has `last_bar` before 2014-09 (2013-06-21).
  - `sec_form25_common_equity_delistings` alone holds 274 distinct issuers delisting in 2013.
  - PWB's earliest `last_bar` is 2022-03-02, and no other vendor is loaded.
- **2014-09 to 2018: terminations recorded, coverage unverified.** Terminations run at 33 in 2014-09, then between 45
  and 83 a month through 2016-06. No independent symbol-bearing exit list exists for these years.
  - Reproduce: `select to_char(last_bar,'YYYY-MM'), count(*) from research_price_series where vendor='icyDenev/Intrader' and last_bar < date '2016-07-01' group by 1 order by 1`.
- **2019 onward: coverage checked against Form 25 records that carry a cover-page symbol.** This is a selected
  reference population, not all exits.
  - Population: distinct (issuer CIK, `resolved_symbol`) with `filed_date` before 2024-09-01. That is 1,006 of the
    table's 1,102 symbol-bearing rows; the rest are later filings or duplicates.
  - Match: an Intrader series with that symbol, or with a trailing Q stripped.

  | year | records | with a series | series ends within ±30 days of suspension (else filing) date | ends elsewhere, share of matched |
  |---|---|---|---|---|
  | 2017–18 | 2 | 2 | 1 | — |
  | 2019 | 52 | 52 | 39 | 25% |
  | 2020 | 138 | 135 | 101 | 25% |
  | 2021 | 183 | 180 | 151 | 16% |
  | 2022 | 207 | 205 | 153 | 25% |
  | 2023 | 259 | 258 | 205 | 21% |
  | 2024 (to 08) | 165 | 164 | 123 | 25% |

  - A symbol match can be a reused ticker.
  - Why a matched series ends elsewhere (for example OTC prints after delisting) is not measured.
  - Slice 4's `--census-form25` reproduces this table, which was measured ad hoc for this spec.

**2. Fundamentals come from the #3360 bundle, joined through the #3361 linkage.**
- `financial_facts_raw`, `instrument_class_shares_outstanding` and `instrument_sec_profile` are keyed on eToro
  `instrument_id`. Only 463 of the 12,091 Intrader series that terminate before 2024-06 carry an instrument link, and
  `research_price_series.cik` is NULL on every row. Joining through instruments keeps survivors.
- The #3360 bundle (`app/services/pit_fundamentals.py`; `pit_fundamentals_3360/2026-09-24-227da140` under
  `~/Library/Application Support/eBull/research/`) is built from the full `companyfacts.zip`. It has 20,325 CIK
  shards, dead filers included, keyed on acceptance time.
- The #3361 linkage (`app/services/security_linkage.py`; `security_linkage_3361/2026-09-24-f6ae1edd`) maps a series
  to a CIK as of a decision date, from Form 3/4/5 evidence accepted before it. It is supported through 2024-09-27.

**3. Linkage coverage is partial and uneven.** The #3361 census at four sampled June decisions shows:
- 0.42 / 0.38 / 0.35 / 0.28 for series that terminate before capture;
- 0.57 / 0.55 / 0.51 / 0.47 for series that run to capture;
- 0.58–0.69 in the census's top three liquidity deciles, for both groups.

These rates have a mixed security-type denominator. Slice 4 replaces them with a census at every formation (§"Census").

**4. Two snapshot-wide integrity masks are not dated.**
- In the linkage: `security_linkage.py:399-401`, which covers accession conflicts, issuer integrity and collisions.
- In the bundle: CIK-level `CIK_INTEGRITY_EXCLUDED` (`pit_fundamentals.py:553,566-567`).

A mask built from later evidence can remove an earlier observation. **The panel is therefore retrospectively filtered
construction data, not a strictly point-in-time record.** That label is carried into step 2.
- Bundle exclusions are counted per name-month.
- Linkage masks act on evidence before a link decision. `LinkResult` does not attribute a changed decision to them,
  so their effect on links is **not measured**. The census reports the masked evidence rows only.

**5. No point-in-time exchange membership exists.** Size cutoffs come from JKP's published `nyse_cutoffs`. Those are
replication references, not information a strategy had at t (§"What step 2 inherits").

## Source rule: JKP

The comparator is Jensen, Kelly & Pedersen (2023), `jkp_usa_monthly_vw_cap` (US, loaded through 2025-12), with rules
from their Documentation.pdf (downloaded 2026-10-04; sha256 pinned in slice 1).

**Factor construction** (§"Factor Portfolio Construction"):
- monthly terciles. Non-micro stocks ("larger than NYSE 20th percentile") are sorted into "three groups of equal
  numbers of stocks", and micro stocks are placed on the same breakpoints;
- each tercile's return is "capped value weight", with ME winsorised at the NYSE 80th percentile;
- factor = high tercile − low tercile, long the tercile the original paper finds has the higher expected return;
- at least 5 stocks per leg;
- a 1-month holding period.

**Accounting timing** (§6.2–6.3):
- annual and quarterly characteristics are built separately, and the most recent of the two is used;
- data are assumed public 4 months after the period end;
- quarterly flows are summed over the last four quarters; balance-sheet items are period-end values.

**Variables** (Table 5, Compustat items):
- `sale*` = SALE, else REVT;
- `gp*` = GP, else `sale*` − COGS;
- `ope*` = `ebitda*` − XINT, which JKP notes "target[s] the same variable as the numerator of the profitability
  characteristic used to create the Robust-minus-weak factor" (Fama & French 2015);
- `ni*` = IB, else NI − `xido*`, else `pi*` − TXT − MII;
- `ocf*` = OANCF, else `ni*` − OACC, else `ni*` + DP − WCAPT;
- `at*` = AT, else a SEQ-based sum;
- `txditc*` = TXDITC, else TXDB + ITCB;
- `pstk*` = PSTKRV, else PSTKL, else PSTK;
- `seq*` = SEQ, else CEQ + `pstk*`, else AT − LT;
- `be*` = `seq*` + `txditc*` − `pstk*`, with `txditc*` and `pstk*` zero if missing.

**Characteristics** (Tables 6 and 8):
- `gp_at` = GP*/AT*;
- `be_me` = BE*/ME;
- `ope_be` = OPE*/BE*;
- `ni_me` = NI*/ME;
- `ocf_me` = OCF*/ME;
- `at_gr1` = AT*ₜ/AT*ₜ₋₁₂ − 1, "only … if the denominator is above zero";
- `ret_12_1` = Π₍ₙ₌₁..₁₁₎(1 + rₜ₋ₙ) − 1, which skips month t;
- `rvol_21d` = σ over 21 days of daily **excess** returns.

**Universe:** CRSP share codes 10, 11 and 12 (Table 2).

## Dates and stages

- **M** = the calendar month-end of formation month t, for example 2017-04-30.
- **s(M)** = the last session on or before M on which Intrader has an SPY bar, for example Friday 2017-04-28. Every
  price and ME is taken at s(M).
- **One evidence cutoff:** acceptance New York date strictly before s(M). It applies to accounting, shares, SIC and
  bundle reads. The linkage is read with `link_as_of(series_id, s(M))`, the same date.
- **The four-month lag uses M:** a period whose end E satisfies E + 4 calendar months ≤ M is lag-eligible at M.
- **Stage A (step 1).** Formations M = 2014-09-30 .. 2021-04-30; holding months 2014-10 .. 2021-05 (80 months). Every
  holding month ends before `HOLDOUT_BOUNDARY` (2021-06-29).
  - The builder's price queries are bounded at 2021-05-31.
  - Its bundle and SUB reads are bounded by the same cutoff.
  - No hold-out access is made.
- **Stage B (step 2, not built here).** Formations 2021-05 .. 2024-07. It is built only under step 2's declaration,
  which logs the access and labels the sample **reused validation**, since post-2021 outcomes have been inspected
  before (`research-process.md` §Hold-out). 2024-08 has no complete next month and is never a formation.
- Lookbacks may reach before 2014-09: a name alive at s(M) is in the archive whatever happened later (premise 1).

## Universe at M

The steps run in this order. The census counts each exclusion by count and ME.
1. **Priced at the decision.** The name is in `universe_selection`'s survivorship-free Intrader admission and has an
   admitted bar on s(M): a finite, positive `close` and `adj_close`, not in `research_bar_quarantine`.
   - A name not quoted on the decision session cannot be bought then, so it is not a holding.
   - This also excludes names that terminated before M.
2. **Linked.** `link_as_of(series_id, s(M)).reason == Reason.LINKED`, with `basis` recorded. Every other reason
   excludes and is counted separately, `NON_COMMON_SYMBOL_FORM` included.
3. **Filer.** The CIK has a 10-K-family (10-K, 10-K/A, 10-KT, 10-KT/A) or 10-Q-family (10-Q, 10-Q/A, 10-QT, 10-QT/A)
   accession accepted before s(M) and within 18 months of it. The 18 months is fixed by construction.
   - This restricts the estimand to 10-K/10-Q filers. 20-F and 40-F filers are out, although the bundle admits their
     forms: their accounting standards differ and are not mapped here.
   - JKP keeps share code 12, so foreign-incorporated 10-K filers are kept.
4. **Not a REIT.** SIC ≠ 6798, "Real Estate Investment Trusts" in the SEC SIC list. The SIC is SUB `sic` for the CIK's
   latest 10-K- or 10-Q-family accession accepted before s(M). The SEC defines it as "assigned by the Commission as
   of the filing date".
   - The join is on exact accession. An accession absent from the loaded SUB files is `sic_unloaded`, distinct from a
     filed NULL (`sic_null`). Both stay in and are counted.
   - Commodity pools, other funds and OTC quotation are **not separable** with our data. They are alignment-matrix
     rows.
5. **One listed security per firm at M.** No other series admitted under step 1 at M links to the same CIK at s(M).
   Firms with several linked securities at M are excluded and counted, because firm ME cannot be apportioned.
   - This is contemporaneous, so a ticker change over time is not a second class.
   - It does **not** prove single-class scope: a second listed class can be unlinked (§"Market equity").
6. **ME available** (§"Market equity"). Shares, price and ME must all be finite and positive, with a separate reason
   for each failure.

Holdings formed at M are kept through their holding month whatever happens later (§"Returns").

## Reading the bundle: state machines

**Prefix reads** (`public_events`):
- `OK` → proceed. Candidates are built from **both** `events` and `rejections`.
- `CIK_NOT_IN_BUNDLE` → the name is excluded at M (`no_fundamentals`, counted).
- `CIK_INTEGRITY_EXCLUDED` → excluded (`integrity_excluded`, counted).
- `AFTER_CAPTURE` → refuses the run. It cannot occur before 2026-09-23.
- `CONCEPT_NOT_IN_POLICY` → refuses the run (configuration error).

**Value reads** (`value_as_of` on the chosen key):

| status | action |
|---|---|
| `VALUE` | use, after checking that the returned accession's form is admitted for the period's role; otherwise the component is `form_mismatch` |
| `ABSENT` | the next branch of the hierarchy may be tried; components JKP sets "to zero if missing" become 0 |
| `AMBIGUOUS`, `BLOCKED_BY_REJECTION` | the characteristic is missing. No later branch and no zero; counted |
| anything else | refuses the run |

**Candidate precedence.** A key whose latest public item before the cutoff is a rejection is a candidate with that
status. It is ranked like any other candidate, so a newer blocked key is never skipped for an older clean key.

## Market equity

There is no daily share count in XBRL, so these rules are fixed by construction.

**Primary: the filing's own cover count.**
- Candidates are `dei:EntityCommonStockSharesOutstanding`, unit `shares`, instant, in 10-K- or 10-Q-family
  accessions accepted before s(M), whose context date is on or after that accession's period end (anchor below).
  That restricts them to the filing's own cover-page count, not a comparative.
- Rank by context date, then acceptance.
- **Age:** the context date must be within 15 months of s(M) (fixed by construction: an annual cover plus a quarter
  of slack).
- **Basis:** as of the context date.

**Fallback,** only when no age-eligible dei candidate exists, whether there was none at all or all were stale:
- `us-gaap:CommonStockSharesOutstanding`, the instant at the accession's own period end, ranked and age-limited the
  same way;
- basis: as of the accession's **acceptance** date. SEC SAB Topic 4C requires a split effective before the financial
  statements are issued to be reflected retroactively, so a balance-sheet count filed after a split is already
  post-split;
- flagged `shares_scope=balance_sheet`.

**No fallback when blocked.** If the best dei candidate's read is `AMBIGUOUS` or `BLOCKED_BY_REJECTION`, ME is
missing. The fallback is not tried.

**Split basis.** `shares_s = shares × Π split_factor` over Intrader `split_factor` stamps (`research_price_daily`, the
rows `total_return_reader.load_split_dates` reads) dated after the basis date and on or before s(M).
- A non-finite or non-positive stamp refuses the run.
- The stamp convention is pinned by fixtures: a forward split (AAPL 2020-08-31), a reverse split, and a basis date
  with the split falling between the basis date and s(M).
- **Population reconciliation:** for every stamp inside the stage-A window on a panel name,
  ME(after)/ME(before) ÷ (close_adj ratio) must lie in [0.8, 1.25]. The failures are listed.

**Value.** ME = `shares_s × close` at s(M), where `close` is Intrader's unadjusted print. Price and shares are on the
same date and basis.

**Discontinuity census.** Every month-over-month ME ratio that departs from the `adj_close` ratio by a factor outside
[0.8, 1.25] is flagged. The flagged name-months and their leg-weight exposure are printed.

**Scope.** This is the ME of the linked security computed with an issuer-level count. For a firm with another class,
listed and unlinked or unlisted, the dei total can overstate the security's ME. JKP's company ME sums listed
securities only. The exposure is **not measurable** with our data (companyfacts drops dimensional facts); it is an
alignment-matrix row.

**Amendment 2 (2026-10-05): share-count checks.** The 2026-10-05 proof artefact admitted share counts that are
filer errors, and ME then took them at face value:
- **Mis-scaled cover counts:** 89 name-months had ME above $3T, from 20 names, with cover counts tagged ×1,000 to
  ×10⁹. EEFT's count was 5.3×10¹⁶ shares; GRMN, WTW, AJG, YUM and AA are others.
- **Subsidiary and shell counts:** counts of 100 or 1,000 shares (DD, VTRS, WBA, FTI, DXC, ONEW).
- **Splits counted twice:** cover counts dated before a split but reported after it. NFLX's 2015-06-30 count is
  already the post-7:1 figure.

The census lists 34 split reconciliations that failed.

Under value weighting, one such name can carry most of a leg. These are examples, not an adjudication; slice 3d's
PR records the full adjudication (below).

**Checks.** They are evaluated in order, and the first failure is the row's ME-missing reason. Every check's own
outcome (`pass`, `fail` or `untested`) is also stored on the row as `me.checks`, so overlaps are counted. None of
the checks repairs a count, except the recovery in check 2.

1. **The applied stamp product is ambiguous or beyond tolerance → `shares_basis_ambiguous`.** Either condition
   fails the check:
   - **A split stamp in (context date, acceptance NY date] of a cover count.** Filers tag the cover count with a
     date before the split and report the post-split figure anyway (NFLX 2015-07, RAD 2019-04, SHW 2021-04). The
     context date then cannot say whether the split is already in the count.
   - **Any count whose applied split product lies outside [1/100, 100].** Intrader stamps a bankruptcy cancellation
     and re-issue as a split (BAS 2016-12, factor 0.0018), and a pre-event count multiplied through it is not the new
     equity.

   The balance-sheet fallback keeps its acceptance-date basis. That basis is an approximation of SAB Topic 4C's
   "financial statements issued" date by the SEC acceptance date, and this amendment labels it as one.
2. **Cover and balance-sheet counts differ more than 100× → `shares_scale_conflict`, unless recovered.** This is an
   adaptation of XBRL US **DQC_0095** ("Scale – Common Stock Outstanding", v30.0.4; effective 2020-09-01). That rule
   errors when `dei:EntityCommonStockSharesOutstanding` and `us-gaap:CommonStockSharesOutstanding` in one filing
   differ by more than 100 times.
   - **What is compared:**
     - for every accession the cover read returned (`FactUse.accns`), that accession's `CommonStockSharesOutstanding`
       at its own period anchor, read through the value state machine;
     - the anchor count multiplied by the stamps in (anchor, cover context date], since a split between the two
       dates is a real difference;
     - only positive `VALUE` reads. Anything else leaves the check `untested`.
   - **What differs from the rule as published:**
     - companyfacts drops dimensional facts, so only undimensioned facts are compared;
     - the published rule states no date condition, and the split adjustment is ours.
   - **Recovery.** DQC_0095 does not say which side is wrong, and the data errs in both directions: AMTX's cover
     count is right and its balance-sheet count is in thousands, while GRMN is the reverse. If check 4 holds a
     reference (below), the side within its 100× tolerance is used, unless both or neither are within it. A
     recovered row is flagged `shares_scope=dqc_recovered:<side>`; it is not verified, so it never becomes a
     reference. If there is no reference, ME is missing.
3. **Trailing dollar volume above 10 × ME → `shares_turnover_implausible`.**
   - **The metric:** the census liquidity measure (mean `close × volume` over admitted bars in the 126 sessions
     ending at s(M), at least 63 bars) divided by ME at s(M). It is a ratio of dollar volume to current ME, not a
     literal share turnover.
   - **Sensitivities:** a price collapse inside the window raises it, and screened bars stay in the numerator. The
     census counts how many rejections had a screened bar in the window.
   - **The bound is calibrated** on the proof artefact's admitted rows that pass checks 1–2, and frozen here.
     Calibration and the effect shown are the same data, so slice 3d reproduces the effect but does not validate it
     independently. The calibration:
     - Every row between 10 and 100 was inspected. ONEW is a 1,000-share balance-sheet count. ZNGA's balance-sheet
       count is in thousands. DCTH's count is dated before a reverse split, and the split's basis is unclear.
     - Above 100 there are 26 names, including DD, VTRS, WBA, FTI and DXC at 100 shares.
     - Between 1 and 10 there are real high-turnover names (NAKD, RIOT, MARA), so the bound sits at 10.
   - Names without the liquidity measure are `untested`.
4. **Count inconsistent with a verified reference → `shares_discontinuity`.**
   - **What is compared:** `shares_s(M)` against `shares_s(M′) × Π stamps in (s(M′), s(M)]`. A ratio above 100 or
     below 1/100 fails, which applies DQC_0095's tolerance by analogy across time. It is a heuristic, not an
     inherited validation.
   - **The reference M′.** It is the series' latest earlier formation whose ME was admitted and **verified**, which
     means checks 2 and 3 were both tested and passed. It must also be within 15 months of M (the share-age bound) and
     have the same linked CIK.
     - A count admitted while untested is used but never becomes a reference. This stops an unverified seed from
       rejecting later counts.
     - A linkage change resets identity: MRK was linked to another CIK from 2016-05 to 2018-04.
   - **Order and full grid.** The chain runs in formation order over every formation of the run. A publish and a
     replay always run the full grid. A `--formations` subset run stamps `chain_complete=false` in its census, and its
     rows are diagnostics only.
   - **Known gap.** A filer that mis-scales both counts identically, has dollar volume within the bound and has no
     verified reference within 15 months passes all four checks. PTP filed both counts ×1,000 from 2013-07 to
     2014-10, so its 2014-09 .. 2015-01 stage-A months are a known example. The total of such cases is not
     measurable.

**Discriminators tested and rejected** on the proof artefact:
- **A chain over raw filings, without verified references.** It rejected real counts that followed a 100-share shell,
  and it re-accepted a mis-scaled level after its 15-month reset. #2232 recorded the same failure for a history-only
  test.
- **A cross-sectional floor on dollar volume ÷ ME.** Large-cap ×1,000 errors land near 10⁻⁵ a day (WTW, PTP),
  alongside real illiquid names and SPAC units.

**Census and reconciliation.**
- A row whose ME a check removed keeps its unchecked value as `me.raw`, flagged as contaminated: it may be wrong by
  orders of magnitude.
- The split reconciliation and the discontinuity census are printed twice, on raw and on final ME. For every
  originally failing pair, they print its final status: reconciled, still failing, or unavailable, with the check
  that removed an endpoint.
- These are diagnostics, not invariants. A failure on final ME is expected for corporate events the `adj_close`
  ratio does not track, such as a spin-off (WIN 2015-04), a merger issuance after a stale count (JCI/Tyco 2016-09)
  and a re-issue (BCEI 2017-05). It is listed and classified in slice 3d's PR, and nothing is excluded on it.
- Per reason, the census prints:
  - row and name counts;
  - the share of the formation's total final ME held by the rows the reason removed. Rows removed contribute zero.
    The share computed on raw ME is printed too, and labelled contaminated.

## Accounting

**Period anchors.** A 10-K/10-Q-family accession's period end is the latest instant date among its `Assets` facts.
- An annual period is a 10-K-family accession's anchor.
- A quarterly period is a 10-Q-family anchor, or the fourth quarter implied by a 10-K anchor.
- An accession with no `Assets` instant has no anchor. Its facts are not used, and this is counted.

**Choosing the period.** For each characteristic, at M:
1. enumerate the CIK's anchored annual and quarterly periods;
2. keep those that are lag-eligible (E + 4 months ≤ M) and whose components' keys have a public item before s(M);
3. among those, the annual-based and quarterly-based values are each the one with the latest E;
4. use whichever of the two has the later E; on a tie, use the annual.

Nothing is chosen before eligibility is tested.

**Maximum age:** a value whose E is more than 18 months before M is missing. This is fixed by construction; JKP
states no limit.

**Component alignment, by type:**
- **contemporaneous components** (GP and AT; OPE and BE; the parts of BE): same E;
- **year-over-year** (`at_gr1`): AT at E, and AT at E′ where E′ is the CIK's anchored period of the same type with
  E′ within 12 months ± 14 days before E. AT at E′ must be > 0;
- **TTM constituents:** four consecutive quarters. Each quarter's start is within 7 days after the prior one's end,
  and the last quarter ends at E;
- **market denominators:** ME at s(M), never aligned to E (JKP updates ME monthly).

**Quarterly flows,** in precedence order:
1. a direct quarter fact (80–100 days) for that interval;
2. a year-to-date fact minus the shorter year-to-date fact with the same start;
3. Q4 = annual − (Q1 + Q2 + Q3), all with the same fiscal-year start.

The highest-precedence available value is used and the branch recorded. Values from several filings use each key's
own latest public item; a restated earlier quarter therefore enters with its restated value. A fiscal-year change
(a 10-KT, or anchors breaking the 12-month cadence) breaks TTM until four consistent quarters exist.

**Annual flows:** 350–380-day facts ending at an annual anchor.

**XBRL mapping of the JKP items.** "Dropped" means no XBRL tag of matching scope exists. Each dropped branch is in
the matrix.

| JKP item | XBRL, in order | dropped branches |
|---|---|---|
| SALE / REVT | `Revenues`; `RevenueFromContractWithCustomerExcludingAssessedTax`; `SalesRevenueNet` | "including assessed tax" variant (SALE is net of excise taxes) |
| COGS | `CostOfGoodsAndServicesSold`; `CostOfRevenue`; `CostOfGoodsSold` (flag `cogs_scope=goods_only`) | — |
| GP | `GrossProfit` | — |
| XSGA | `SellingGeneralAndAdministrativeExpense` | — |
| `ope*` numerator | `sale*` − COGS − XSGA − XINT. This is FF's OP numerator, which JKP states it targets. Labelled a **proxy**: filed COGS and SG&A may include D&A, which Compustat's exclude | EBITDA, OIBDP, XOPR |
| XINT | `InterestExpense` | — |
| IB | `IncomeLossFromContinuingOperations`, which the US-GAAP taxonomy defines as attributable to the parent | NI − XIDO (extraordinary items were eliminated by ASU 2015-01; discontinued-operations branch not built); PI − TXT − MII |
| OANCF | `NetCashProvidedByUsedInOperatingActivities` | NI − OACC; NI + DP − WCAPT |
| AT | `Assets` | SEQ-based sum |
| LT | `Liabilities` | — |
| SEQ | `StockholdersEquity` | CEQ + PSTK (no common-only tag) |
| TXDITC | **proxy:** `DeferredTaxLiabilitiesNoncurrent`, then `DeferredIncomeTaxLiabilitiesNet`. Different netting scopes; branch coverage reported | TXDB + ITCB |
| PSTKRV / PSTKL / PSTK | `PreferredStockRedemptionAmount`; `PreferredStockLiquidationPreferenceValue`; `PreferredStockValue` | — |

- **AT − LT** (the third `seq*` branch) includes noncontrolling interest, whereas `StockholdersEquity` is parent-only.
  It is used, as JKP uses it, and counted.

**Eligibility,** applying JKP's denominator-above-zero rule to every ratio and keeping negative numerators:
- `gp_at`: AT > 0;
- `at_gr1`: AT(E′) > 0;
- `be_me`, `ni_me`, `ocf_me`: ME > 0;
- `ope_be`: BE > 0.

**Concept-set extension** (slice 2). These are not in `CONCEPT_SET` (`pit_fundamentals.py:54`): `Liabilities`,
`SellingGeneralAndAdministrativeExpense`, `InterestExpense`, `IncomeLossFromContinuingOperations`, both deferred-tax
concepts and the three preferred concepts. These are exactly the additions; nothing else is added.
- **A/B:** superseded by Amendment 1 below, which keeps the same-pinned-inputs rule and makes the comparison row
  for row.
- Before editing, run `scripts.ai_trial_policy_guard`. On 2026-10-04, `pit_fundamentals.py` appeared in no
  `POLICY_MODULES` list.

**Amendment 1 (2026-10-04, found while starting slice 2; Codex checkpoint 1, 6 findings, all applied).**
**Coupling.** `security_linkage.POLICY_FILES` (`security_linkage.py:264-271`) hashes `pit_fundamentals.py`.
Editing `CONCEPT_SET` therefore changes the #3361 policy hash, and `load_security_linkage` refuses the 2026-09-24
linkage (`security_linkage.py:477-478`). Both linkage artefacts load under the current code on 2026-10-04; neither
would after slice 2. The panel needs the linkage (§"Universe at M", step 2). So slice 2 rebuilds it.

**A live rebuild is not an A/B.** `build_3361_security_linkage.py` re-reads the DB through `snapshot_db` (lines
77-108). Since 2026-09-24, `research_price_series` grew from 30,591 rows (the frozen `inputs/series_inventory.json`)
to 38,461, from #3639's separate PWB capture vendor, and `sec_form25_register` grows with new filings. A live
rebuild would mix the code change with input drift.

**#3360 bundle A/B (replaces the two bullets above).**
- **Arm A** is the 2026-09-24 bundle. It loads under the pre-change code: its manifest `policy` equals
  `policy_sha256()` on 2026-10-04, which hashes code, constants, Python and `tzdata` (`pit_fundamentals.py:490-502`).
  No pre-change worktree build is needed. **Arm B** is built by the new code from arm A's own `inputs/` ZIPs.
- **Compared per CIK, row for row, with payload and multiplicity:** the shard list and every shard's `accessions`;
  every `events` row (key, accession, acceptance, value, multiplicity) and every `rejections` row (key, accession,
  acceptance, reason, row indices). Arm B's rows for the pre-existing concepts must equal arm A's exactly; its
  only extra rows must be for the nine added concepts.
- **Manifest:** `snapshot_integrity_failures`, `supported_through` and every pre-existing ledger key must be equal.
  `policy` differs by construction. Any other difference refuses publication.
- **Additions accepted on their own evidence,** since equality of the old rows says nothing about the new ones:
  - every added concept has stored events (a zero refuses);
  - per concept, the ledger outcome counts are reported;
  - a cross-source check: the default panel's (AAPL, GME, MSFT, JPM, HD) latest 10-K value of `Liabilities` and
    `InterestExpense` read through `value_as_of` against the same figure in SEC EDGAR's filing.

  The admission rules for the new rows are the bundle's existing rules, unchanged; the concepts are taken as filed.

**Linkage replay.**
- Slice 2 adds a replay mode to the linkage builder. It takes a prior linkage bundle plus its pinned manifest
  digest, and checks the digest and schema but not the policy (the policy is what changed). Every input it reads
  (the four DB dumps, `form345/`, `submissions.zip`) comes from that bundle's `inputs/` and must match the recorded
  `input_sha256`.
- The #3360 side is the new bundle, passed as today with its manifest digest. Its `submissions.zip` must hash to the
  linkage's recorded `submissions` digest, and its `supported_through` must equal the 2026-09-24 #3360 value, which
  the linkage uses for coverage (`build_3361_security_linkage.py:220-228, 248-252`). Either difference refuses.
- It rebuilds `2026-09-24-f6ae1edd` (Form 25 mode). The `-without-form25` cross-check artefact is a #3361 diagnostic
  that step 1 does not read; it is not rebuilt.

**Linkage A/B.** Every series document and `ledger.json` must be byte-identical to the 2026-09-24 build, and the
loaded bundle must pass `verify_all()`. The manifest may differ only in `policy` and `input_sha256.pit_manifest`.
Anything else refuses publication.

**Other #3360 and #3361 readers** (`build_2901_quality_input.py`, `measure_2901_gpa_coverage.py`, the #3360/#3361
census and causal scripts) are closed research. They reproduce against the old artefacts at their recorded SHAs
**and** the same Python and `tzdata` versions, which both policy hashes include; slice 2's PR records those versions.

## Price characteristics and daily data

**Daily bars.**
- An admitted bar has a finite, positive `close` and `adj_close` and is not quarantined.
- A daily return exists only between admitted bars on **adjacent** SPY sessions: `adj_close[s] / adj_close[s−1] − 1`.
  A return spanning missing sessions is not a daily return and does not count.
- Excess return = daily return − the French daily RF for that session (`F-F_Research_Data_Factors_daily`, slice 1).
  A missing RF refuses the run.

**`ret_12_1`.**
- It needs all 11 monthly total returns for months t−11 .. t−1, from `total_return_reader`.
- JKP requires only "enough non-missing values" (Table 8, note 8), so needing all 11 is our stricter rule, fixed by
  construction.

**`rvol_21d`** is the sample standard deviation (n − 1) of excess daily returns over the 21 SPY sessions ending at
s(M), and it needs at least 15. JKP states 15 as its minimum for its 21-day residual estimates (2021-02-15 log
entry). It is applied here by analogy, as our choice.

**Daily screen.**
- A daily return outside [−0.9, +3.0], or an `adj_close/close` ratio that moves more than 50% between adjacent bars
  with no `split_factor` or `dividend` stamp, flags both endpoint bars.
  - "Moves more than 50%" means `|ln(ratio_q / ratio_p)| > ln 1.5` between consecutive admitted bars. Measured in
    the log, the test is symmetric: an unadjusted 1:2 reverse split (the ratio halves) is caught like a 2:1 split.
  - Any stamp dated in (p, q] excuses the move, whatever its size. The exemption is broad, so slice 3d's census
    counts the excused moves whose stamp does not explain the ratio change within the same tolerance (Amendment 2).
- A window containing a flagged pair's **later** bar leaves `rvol_21d` missing, counted. A pair whose later bar falls
  after s(M) is not observable at s(M): it is flagged in the census but does not screen the formation. Amendment 2
  records this reading, which slice 3c implemented.
- Flags are printed per year. Nothing is deleted.

**Daily–monthly reconciliation.** For every panel name-month, compounded admitted daily returns are compared with the
month's `total_return_reader` return. Disagreements beyond 1e-6 that are not explained by a non-adjacent gap are
listed.
- The independent total-return check is #3619's reconciliation. This step relies on it and does not repeat it.

## Returns and holdings

Weights are fixed at s(M). A holding's month-(t+1) return has one of three statuses:
- **`observed`:** the month's total return;
- **`terminal`:** the return from s(M) to the last observed bar (the reader's partial-month row up to its
  `end_bar`), then step 0's imputed realisation at `terminal_value_fraction(class, arm)`, held as cash at 0 for the
  rest of the month;
- **`coverage_exit`:** the same partial-month return to the last observed bar, then cash at 0. No haircut and no
  renormalisation.

Membership never depends on a later return being available.

**Arms.** `best_case` and `worst_case` are both computed and printed. A PASS needs both.

## Fidelity

**Our series.** Monthly, for each characteristic:
- **non-micro:** ME > JKP `nyse_cutoffs` 20th percentile for M. Units are normalised: JKP's cutoffs are USD millions,
  our ME is USD;
- sort the non-micro values. Ties are broken by `name_key`, and every value in a tied run goes to the group of the
  run's first member;
- the low group is the first ⌊n/3⌋ and the high group the last ⌊n/3⌋; the middle takes the remainder. This is our
  implementation of JKP's "equal numbers"; JKP's code was not compared;
- **breakpoints** are the low group's maximum and the high group's minimum. Micro names go low if ≤ the low
  breakpoint, high if ≥ the high breakpoint, otherwise middle;
- **weights** = min(ME, the NYSE 80th-percentile cutoff);
- **sign** follows JKP Table 9's direction, transcribed into a committed CSV in slice 1;
- **undersized:** fewer than 5 names in either leg makes the month missing.

Fixtures cover ties, n < 15, and all-micro months.

**Pairs:** `gp_at`, `be_me`, `ope_be`, `ni_me`, `ocf_me`, `at_gr1`, `ret_12_1` and `rvol_21d`, each against the
same-named JKP series as published.

**Comparison contract.** The series are indexed by calendar month over the 80-month stage-A grid.
- **Contemporaneous:** months where both are present.
- **Lag:** pairs (ours[m], published[m−1]) with both present and m−1 in the grid.
- **Lead:** pairs (ours[m], published[m+1]) with m ≤ 2021-04.
- Each mask needs at least 60 pairs, or the verdict is `INSUFFICIENT`. Zero variance on either side is also
  `INSUFFICIENT`.
- **Undersized-leg months** are counted on the full 80-month grid before masking.

**Printed:**
- months used;
- the missing months on each side;
- the undersized-leg count on the grid;
- the three correlations;
- the beta of ours on published;
- the tracking-error ratio, sd(ours − published) / sd(published);
- minimum and median leg counts;
- the offset boolean;
- the alignment matrix.

**Pass bars,** fixed by construction before any run. No published standard exists for rebuilding a factor on
independent accounting data. Every bar must hold under **both** arms:
- **correlation** ≥ 0.90 for `ret_12_1` and `rvol_21d`; ≥ 0.80 for the accounting characteristics;
- |contemporaneous correlation| ≥ |lag correlation| and ≥ |lead correlation|;
- **beta** in [0.7, 1.3], which catches a scale error that correlation cannot;
- **offset:** |12 × mean(ours − published)| ≤ 0.03 over the contemporaneous mask.
  - Only the boolean is printed or stored. A FAIL names this bar without its value.
  - The boolean does reveal something: together with JKP's public mean, it bounds ours to a ±3% band. That is the
    information exposed, and it is accepted.
- **undersized-leg months** ≤ 2 on the grid.

**Verdicts.**
- Per characteristic: `PASS` only if both arms pass every bar; otherwise `FAIL` (bars named) or `INSUFFICIENT`.
- Only `PASS` is eligible for step 2.
- **The unit is the characteristic.** A new version needs a named difference from the JKP rule, or an independently
  evidenced defect (a failing fixture, a mismatched input). It is never a change made because the correlation is low.
- A characteristic is **rejected** after 3 versions without a PASS. `INSUFFICIENT` counts as a version.
- The bars never change.

## Alignment matrix (printed with every result, with each row's measured exposure where measurable)

| difference from JKP | measured by |
|---|---|
| Universe: linked, 10-K/10-Q filers, single linked security, quoted on s(M); not CRSP 10/11/12 | universe census, by count and ME |
| Survivorship: unverified 2014-09..2018; checked against a selected Form 25 set from 2019 | premise 1 table |
| Retrospective integrity masks (bundle counted; linkage effect unmeasured) | census |
| Commodity pools, other funds and OTC-quoted filers not separable | not measurable |
| REIT exclusion by SIC 6798, not share code | count of 6798 exclusions; `sic_null`/`sic_unloaded` counts |
| XBRL as filed vs Compustat; dropped branches (mapping table); the `ope*` and TXDITC proxies; COGS goods-only flag | branch-use counts per characteristic |
| Acceptance required on top of the 4-month lag; 18-month maximum age | count of name-months delayed or aged out |
| Issuer-level shares for a security; unresolved class scope; 15-month share age; balance-sheet fallback | fallback count; discontinuity census; scope not measurable |
| Amendment 2 share checks (ME missing or DQC-recovered); identical mis-scaling of both counts passes undetected | per reason: row and name counts, final-ME share, raw-ME share labelled contaminated; recovered count; the undetected residual is not measurable |
| Quarterly items reconstructed (YTD differences, Q4 residuals) | branch counts |
| `ret_12_1` needs 11 of 11; `rvol_21d` minimum by analogy; daily screen | missing counts by reason |
| Tercile tie and quantile convention is ours | — |
| Holdings need a quote on s(M) | count of linked filers not quoted |
| Coverage exits and terminations imputed, not CRSP delisting returns | weight held in each status, per arm |

## Census (every formation; stored, and summarised in the report)

- **Exclusion funnel:** each universe step and each characteristic's missing reasons, by count and ME share (count
  only where ME is unknown).
- **Liquidity tercile:** the mean of `close × volume` over admitted bars in the prior 126 SPY sessions, needing at
  least 63 bars, else "unclassified". Nearest-rank terciles among classified archive names at M, ties by `name_key`.
- Linkage reasons, bundle integrity exclusions and masked-evidence rows.
- ME fallback use, the split reconciliation, the discontinuity flags, the daily-screen flags and the daily–monthly
  reconciliation.
- Termination status, **only** as a retrospective diagnostic column, never as a filter.

**Segmentation exception.** `research-process.md` asks for a predeclared partition. JKP's comparator series are
pooled across sizes, so fidelity is defined pooled. No per-cell fidelity is claimed. The liquidity tercile is a
census diagnostic only. Step 2's spec declares the partition for outcomes.

## Panel artefact, provenance and replay

- **Location:** `~/Library/Application Support/eBull/research/factor_panel_3609/<date>-<sha8>-stageA/`, under the
  #3360/#3361 publish protocol: an exclusive directory, the manifest written last, a dirty checkout refused.
- **Inputs frozen into the artefact's `inputs/` and hashed:**
  - the exact DB extracts read: price bars and stamps for every series read; the quarantine set; series metadata;
    termination evidence; the `universe_selection` admission;
  - the reference snapshots: JKP returns, `nyse_cutoffs`, the Table 9 CSV, French daily RF;
  - the FSDS SUB files.
- **Rows:** one per (M, name) for **every** name examined, admitted or excluded, with the exclusion reason. Admitted
  rows carry:
  - `M`, `s(M)` and `holding_month`;
  - series_id, cik, link reason and basis, SIC with its accession and status;
  - **ME provenance:** the full fact key, accession, acceptance, read status, basis date, split product and
    `shares_scope`;
  - **per characteristic:** its value and every contributing fact (key, coefficient, accession, acceptance, status,
    derivation branch);
  - `ret_12_1` and `rvol_21d` with their observation counts;
  - per arm, the return status and value.
- **Manifest:**
  - git SHA and this spec's full-file sha256;
  - the #3360 (new build) and #3361 manifest hashes;
  - every input hash;
  - `TOTAL_RETURN_SPLICE_VERSION`, `UNIVERSE_SELECTION_RULE_VERSION`, `TERMINATION_RULE_VERSION`;
  - a **construction-version hash per characteristic**, covering the spec hash plus the source hashes of the builder,
    the report and every module they import from `app/`.

## Registration, ledger and what step 2 inherits

**Registration.** This step's verdicts gate step 2, so it is a selection study and is counted.
- Before the first fidelity run, slice 4 adds a non-claiming `DeclaredTrial` (`declared_for=None`) for "#3609 step 1
  fidelity" to `app/services/trial_register.py`.
  - `searches` = 8 characteristics × 2 arms.
  - Every later version adds a row.
  - Its frozen inputs are this spec's sha256 and the construction-version hashes.
- The scripts are listed in `_NON_TRIAL_RESEARCH_READERS` only for the gate test's import check. The reason string
  cites the `DeclaredTrial`.

**Ledger.**
- Each run writes a `started` row to `var/research/3609_step1/ledger.jsonl` **before** evaluation: run id, spec hash,
  version hashes, command.
- It writes a `completed` or `failed` row before any result is printed.
- Every row, abandoned and failed runs included, is copied into the committed `docs/research/3609-ledger.jsonl` in the
  PR that posts the run, or in a ledger-only PR if a run is never posted.

**Step 2 inherits:**
- **Configurations tried:** the `DeclaredTrial` rows, which count into M.
- **Development data:** the 2014-10 .. 2021-05 window. Construction was decided on it, although no mean was seen.
  Step 2 claims on it need either a nested procedure that replays the fidelity selection inside each fold with
  fold-local data and all retained candidates, or a separately governed validation sample. Step 2's spec chooses one
  and specifies it.
- **Frozen families,** fixed before any result:
  - GP/A (`gp_at`);
  - value (`be_me`, `ni_me`, `ocf_me`);
  - profitability (`ope_be`);
  - investment (`at_gr1`);
  - 12-1 momentum (`ret_12_1`);
  - low volatility (`rvol_21d`).

  Step 2 may drop a characteristic that is not PASS. It may not add one without counting it.
- **Breakpoints.** JKP `nyse_cutoffs` are a 2026 reconstruction, not a vintage. Step 2's executable book needs a size
  rule computable from data available at s(M), declared in its spec.
- **Track.** Track B requires, per family, a stated adoption rationale and independent post-publication evidence, plus
  the net, deflated backtest against net SPY and the random-basket control (`research-process.md` §Track B).
  - The window from 2014-10 is about 10 years. #3610 measured that IR 0.5 at M = 505 needs about 123 years, so
    Track A is out of reach.
  - The declaration needs a `TrialDesign` that passes `power_check`.
- **Labels:** "retrospectively filtered" (premise 4), and the three survivorship regimes (premise 1).

## Slices

1. **Reference data.** Each source gets a parser test on a committed fixture, plus validation of units, calendar
   coverage and missing codes before publication:
   - JKP `nyse_cutoffs`;
   - JKP Documentation.pdf (sha256 pin), with the Table 9 direction CSV, which a test checks against the loaded
     factors;
   - French daily RF;
   - FSDS SUB (`adsh`, `cik`, `sic`, `form`, `accepted`) for 2012Q1 .. 2021Q2.

   JKP returns, cutoffs and documentation are pinned to one download date.
2. **Bundle extension and union A/B,** as above (corpus rung), plus the linkage replay rebuild and its
   byte-identity A/B (Amendment 1).
3. **Panel builder,** `scripts/build_3609_factor_panel.py` (stage A), with pure-function tests for:
   - the state machines;
   - period anchors and the choosing algorithm;
   - the timing examples:
     - FY end 2016-12-31 is lag-eligible at M = 2017-04-30. With a 10-K accepted 2017-04-27, it is used at s(M) =
       2017-04-28. If the 10-K was accepted 2017-05-02, it is first used at M = 2017-05-31;
     - FY end 2017-01-31 is first lag-eligible at M = 2017-05-31;
   - split fixtures, quarterly precedence and tercile ties;
   - ME, `gp_at` and `be_me` checked by hand for AAPL, GME, MSFT, JPM and HD at M = 2019-06-30.
   - **3d (Amendment 2): the share checks.**
     - **Pure tests:**
       - each check's `pass`, `fail` and `untested` outcomes;
       - NFLX 2015-07 and a stamp factor outside [1/100, 100];
       - GRMN and AMTX DQC recovery, both with and without a reference;
       - a chain that rejects a ×1,000 count and accepts the next clean one;
       - an untested count that never becomes a reference;
       - a reset on a CIK change;
       - prefix invariance: appending later formations leaves earlier rows unchanged.
     - **Full-population A/B against `prev3c-all`:**
       - The inputs are the same frozen inputs. Every row whose ME is unchanged is identical. The census aggregates are
         the only exception.
       - Every row whose ME changed is one of two kinds:
         - ME is now missing with an Amendment 2 reason; its admission and ME-denominated characteristics follow;
         - ME is DQC-recovered; its ME-denominated characteristics are recomputed and nothing else changes.
       - Any other difference refuses.
     - **The PR also records:**
       - the adjudication of every rejected and recovered name, with a cause per name;
       - each of the 34 original split-reconciliation failures with its final status;
       - the census counts per reason;
       - the `factor_panel_prices` module docstring, updated to cite the adopted readings.
4. **Fidelity report,** `scripts/report_3609_fidelity.py` (with `--census-form25`):
   - the `DeclaredTrial` row;
   - the declared run;
   - results and ledger posted on #3609.
5. **Step-0 Amendment 3:** the W1 split in `scripts/report_3609_baselines.py`, and a re-run (W2 stays reused
   validation, as before).

Slices 1–2 take the corpus/reference rung. Slices 3–5 take the behavioural/data rung, with Codex checkpoint 2 before
the first push.

## Known limits

- Restricted estimand. Linkage under-covers illiquid names, more so dead ones.
- Survivorship is unverified 2014-09..2018 and selectively checked from 2019.
- The panel is retrospectively filtered by undated integrity masks.
- No point-in-time exchange: OTC-quoted filers and some funds can enter.
- XBRL as filed is not Compustat; several JKP branches are dropped and two items are proxies.
- Class scope of issuer-level share counts is unmeasurable.

## Checkpoint log

**Round 1 (54 findings), all applied:**
- **1–3:** survivorship regimes, estimand, census;
- **4:** filer proxy limits;
- **5:** share code 12 kept;
- **6:** masks;
- **7:** linkage date;
- **8–11:** ME rules, ages, splits, discontinuity;
- **12–13:** fact selection and states;
- **14–16:** JKP annual and quarterly timing with examples;
- **17:** one cutoff;
- **18–20, 29–31, 34, 53:** French comparator dropped. JKP is the single source, with its cutoffs, sort conventions
  and pinned vintages;
- **21–24:** JKP Table 5 mapping;
- **25:** eligibility;
- **26–27:** momentum and volatility rules;
- **28:** daily screen;
- **32–33:** fixed holdings, statuses, both arms;
- **35:** breakpoint availability;
- **36:** alignment matrix;
- **37–39, 42:** selection counted, ledger, versions;
- **40, 54:** beta band and offset;
- **41:** comparison contract;
- **43, 51:** stages A and B;
- **44:** Track B conditions;
- **45:** FSDS join;
- **46:** OTC sensitivity dropped;
- **47–48:** union A/B;
- **49:** provenance;
- **50, 52:** segmentation deferred; scope of the extension cut.

**Round 2 (56 findings), all applied:**
- **1:** `DeclaredTrial` registration;
- **2:** contemporaneous one-security rule;
- **3–4:** "retrospectively filtered" label; mask effect unmeasured;
- **5, 38:** one decision session, s(M), for prices, linkage and evidence, with a quote required on s(M);
- **6:** `Reason.LINKED`;
- **7–8:** three survivorship regimes, Amendment 3 relabelled;
- **9:** SIC 6221 dropped; funds not separable;
- **10:** estimand restricted to 10-K/10-Q filers;
- **11:** SUB accession rule, `sic_unloaded` vs `sic_null`, load from 2012Q1;
- **12:** M versus s(M), with corrected examples;
- **13:** eligibility before choice;
- **14:** alignment by type;
- **15:** period anchors;
- **16:** rejected-only keys ranked;
- **17:** full state machines;
- **18:** form check on the returned accession;
- **19:** quarterly precedence and fiscal matching;
- **20:** 18-month maximum age;
- **21:** `at_gr1` matching;
- **22, 24:** `IncomeLossFromContinuingOperations` restored;
- **23:** `ope*` is FF's OP numerator, labelled a proxy;
- **25:** TXDITC labelled a proxy;
- **26:** every dropped branch listed;
- **27:** COGS scope flag;
- **28:** SAB 4C basis for the fallback; dei restricted to cover counts;
- **29:** shares and price on one date;
- **30:** fallback triggers;
- **31:** scope unmeasurable;
- **32:** positivity;
- **33:** split fixtures and population reconciliation;
- **34:** "− 1";
- **35–36:** adjacent sessions and bar admission;
- **37:** 15 labelled as by analogy;
- **39:** partial-month return before cash;
- **40:** tie and quantile convention stated as ours;
- **41:** calendar-indexed masks;
- **42:** undersized-leg months counted on the grid;
- **43:** verdict precedence and unit;
- **44:** offset exposure stated;
- **45:** full-spec hash and code hashes;
- **46:** ledger rows before evaluation;
- **47:** nested selection requirement handed to step 2;
- **48:** no lagged-reference size rule;
- **49:** stage B labelled reused validation;
- **50:** matrix completed;
- **51:** per-fact provenance;
- **52:** DB extracts frozen;
- **53:** daily–monthly reconciliation, with #3619 relied on for independence;
- **54:** segmentation exception stated;
- **55:** liquidity rule;
- **56:** Form 25 table population and denominators.

None rebutted.

**Amendment 2, checkpoint 1 (39 findings).**
- **Applied in this amendment (findings 15–39):**
  - 15: the acceptance-date basis is labelled an approximation of SAB 4C issuance;
  - 16: DQC_0095 is pinned to v30.0.4, with the adaptations listed;
  - 17: the comparison uses the accession(s) the cover read returned;
  - 18: anchor counts are split-adjusted to the cover date;
  - 19: DQC recovery against a verified reference;
  - 20, 22: the metric is renamed, and its bound labelled calibrated and frozen;
  - 21: the whole 10–100 band is adjudicated;
  - 23, 25, 26: only verified counts become references;
  - 24: a subset run is stamped `chain_complete=false`;
  - 27: a CIK change resets the chain, and stamps beyond [1/100, 100] fail check 1;
  - 28: inspected examples are no longer presented as complete, and the full adjudication moves to 3d's PR;
  - 29: the known gap is restated with its conditions;
  - 30–32: raw and final reconciliation, per-pair status, and the exposure denominators;
  - 33–34: the A/B criteria, check order and per-check outcomes;
  - 36: prefix-invariance test, and the window endpoint stated;
  - 37–38: screened-bar and stamp-excuse sensitivities counted;
  - 39: the docstring update is in 3d.
- **Not applicable:** 35 (the 3f criterion) left with the IB and OANCF branches.
- **Deferred to Amendment 2b on #3609:** 1–14 (the IB = NI − XIDO and OANCF continuing + discontinued branches,
  their concept-set extension and the tag measurement). The period-level measurement and the zero-imputation rules
  they need are not built yet.
