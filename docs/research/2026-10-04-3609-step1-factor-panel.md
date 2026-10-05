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
outcome (`pass`, `fail` or `untested`, and `recovered` for check 2) is also stored on the row as `me.checks`, so
overlaps are counted. None of
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
     - the anchor count on the comparator accession's acceptance date, which stands in for SAB Topic 4C's issuance
       date as it does for the fallback (Amendment 2.2, below);
     - only positive `VALUE` reads. Any public rejection of the comparator's key blocks it, whatever its date; this
       is conservative, and the check goes `untested` rather than reading around a rejection.
   - **Outcome:** `fail` if any comparator conflicts, `pass` if at least one agrees and none conflicts, and
     `untested` only when no usable comparator exists. A pass is agreement among the usable comparators, not
     coverage of every accession.
   - **Published DQC_0095 versus this adaptation.**
     - From the rule: the two concepts, one filing, and the 100× tolerance.
     - Ours:
       - only undimensioned facts, since companyfacts drops dimensional ones;
       - the comparator at the accession's own period anchor;
       - positive `VALUE` reads only;
       - rejection blocking;
       - aggregation over the accessions one read returns;
       - the acceptance basis;
       - recovery against a reference.
   - **Recovery.** DQC_0095 does not say which side is wrong, and the data errs in both directions: AMTX's cover
     count is right and its balance-sheet count is in thousands, while GRMN is the reverse. If check 4 holds a
     reference (below), the side within its 100× tolerance is used, unless both or neither are within it. Every
     usable comparator must conflict and all must carry one count (Amendment 2.2). A recovered row is flagged `shares_scope=dqc_recovered:<side>`; it is not verified, so it never becomes a
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
     means checks 2 and 3 were both tested and passed (widened by Amendment 2.1, below). It must also be within 15
     months of M (the share-age bound) and have the same linked CIK.
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
  - per formation, the share of the formation's total raw ME held by the rows the reason removed, labelled
    contaminated. A share of final ME would be zero by construction, since removed rows have none (Amendment 2.2).

**Slice 3d result (2026-10-05)** on the proof artefact's frozen inputs, built by
`PYTHONPATH=. uv run python -m scripts.ab_3609_share_checks`. Per-row evidence is in
`docs/research/3609-slice3d-share-check-changes.csv`.
- 1,379,598 rows are unchanged.
- 953 are DQC-recovered. Among admitted rows, 828 went to the cover count and 104 to the balance-sheet count. The
  other 21 are excluded at an earlier universe step.
- 6 more were recovered and then failed a later check. They are counted below.
- 729 lose ME, by reason:

  | reason | rows |
  |---|---|
  | `shares_basis_ambiguous` | 84 |
  | `shares_scale_conflict` | 387 |
  | `shares_turnover_implausible` | 191 |
  | `shares_discontinuity` | 67 |

- Of the 34 original split-reconciliation failures:
  - 11 are now unavailable because a check removed an endpoint (NFLX, RAD, SHW, DD and others).
  - 23 still fail on final ME. These are the corporate-event class, and they stay listed.

**The residual blocks the canonical publish.** ME above $1T still includes WTW (9 name-months), YUM (4), WWD (3)
and CCL (3), all ×1,000 cover counts, plus PTP's 5 months.

The amendment's simulated effect caught WTW, YUM, WWD and CCL with check 4. The rule as amended cannot: their
earlier counts had no same-filing balance-sheet comparator, so check 2 was `untested`. They were therefore never
verified and never became a reference.

Reference eligibility needs an Amendment 2.1 before the canonical publish. The handoff is on #3609.

**Amendment 2.1 (2026-10-05): reference eligibility by two-filing consensus.** Requiring check 2 to pass starves
check 4: a count whose filing has no usable undimensioned balance-sheet count at its anchor can never be a reference.
- **The rule.** An admitted count (ME present after all four checks) becomes check 4's reference when check 3
  passed and either:
  - check 2 passed (Amendment 2, unchanged); or
  - check 2 was `untested`, and the count agrees within 100× with its **consensus partner**, split-adjusted by the
    stamps in (s(M″), s(M)].
- **The consensus partner** is the count at M″, the series' latest earlier formation whose ME was admitted with
  check 3 `pass` and check 2 `pass` or `untested`. It must be within 15 months of M, on the same linked CIK and in
  the same link run (the same filters as the reference), and it need not itself be a reference. It must come from
  another filing: its accession set (`FactUse.accns` of the count used) is disjoint from the current count's, and
  its count date (the fact's context date) differs.
- **What stays ineligible.** A DQC-recovered count; a count with check 3 `untested`; and, with check 2 `untested`,
  a count with no partner, such as the first admitted count of a run.
- **Why these conditions.**
  - Consecutive formations usually read the same filing's cover count, and an amendment can repeat it with the same
    date. Without disjoint accessions and a different date, one mis-scaled fact would agree with itself.
  - A partner must have passed check 3 and not been recovered, so a count Amendment 2 already treats as
    insufficiently checked cannot vouch for another. A count first read with check 3 `untested` therefore does not
    displace an earlier qualifying partner.
  - `untested` covers every check-2 state without a usable comparator: none reported, a rejected or conflicting
    comparator, and the balance-sheet fallback (which has no cover count to compare). Consensus is a separate test of
    the count used, so all of them are admitted to it deliberately.
- **The 15-month window** is measured between formations, as for the reference. A count admitted at M″ can itself be
  up to 15 months old (the share-age bound), so the evidence behind a partner or reference can be older than 15
  months.
- **What it does not close.**
  - **A shared mis-scaling.** A filer that repeats one mis-scaling in two filings, with check 2 `untested` and
    dollar volume within the bound, forms a consensus. Its later clean counts then fail check 4, including a count
    whose own check 2 passed: a reference, once held, outranks the current filing's own agreement. The failure lasts
    until no erroneous count has refreshed the reference within 15 months.
  - **Drift.** Each consensus step allows 100×, so a sequence of filings can walk the reference by more than 100×.
  - **Real changes beyond 100×** that no split stamp records (an issuance or reorganisation) fail check 4, as under
    Amendment 2. More references expose more of them.

  The slice's A/B lists every newly rejected count, and the rule is adopted only if none of them is a clean count.
- **No published rule is cited.** DQC_0095 compares two counts within one filing. We know of no published rule that
  compares counts across filings, so this is a project heuristic, fixed by construction as check 4 is.
- **Choosing the rule.** Two candidates were first simulated on the slice-3d rows
  (`var/research/3609_step1/ab3d/rows.jsonl.gz`). The simulation froze checks 1–3 and the recovered side, and ignored
  link runs, so its counts were only a guide:
  - **consensus with the immediately previous admitted count** removed 27 more name-months: WTW (9), YUM (4), WWD (3),
    CCL (3), SGU (3), ARW (3) and EGY (2);
  - **the median of up to three previous admitted counts, read once per formation** removed 160, including 14-month
    runs of plausible counts (QRVO 148,468,717; ADSW 87,720,437). It was rejected on that.

  After checkpoint 1 tightened the partner, the exact rebuild measured the adopted rule (slice 3d-ii, below).
- **PTP** stays the stated known gap: its ×1,000 run starts before stage A, so no clean count precedes it.

**Slice 3d-ii result (2026-10-05).** The rebuild uses the proof artefact's frozen inputs and is compared with the
slice-3d rows by `PYTHONPATH=. uv run python -m scripts.ab_3609_share_checks --a <slice-3d rows> --b <3d-ii rows>
--out <changes.json> --csv docs/research/3609-slice3d-ii-reference-changes.csv`, which holds the per-row evidence.
- 1,381,216 rows are unchanged; none is unexplained.
- **27 lose ME to `shares_discontinuity`, all filer errors:**

  | name | months | count filed | evidence |
  |---|---|---|---|
  | WTW | 9 | 64,391,084,000 .. 64,702,012,000 | implies ME of $9.6T .. $10.4T |
  | YUM | 4 | 367,005,511,000,000 | ×10⁶ |
  | WWD | 3 | 62,383,699,000 | ×1,000 |
  | CCL | 3 | 932,485,510,000 | ×1,000 |
  | SGU | 3 | 55,887,832,000 | ×1,000 |
  | ARW | 3 | 87,973 | in thousands; dollar volume ÷ ME 4.8–5.0, under check 3's bound |
  | EGY | 2 | 58,554 | in thousands; dollar volume ÷ ME 7.6–8.2 |

  No clean count was rejected, so the rule is adopted. Every rejected count has check 2 `untested`.
- **37 are restored** from `shares_scale_conflict`: the newly eligible references let DQC recovery choose the cover
  count (BWS, CNNE, EYES, JOB, MASI, NG, PEG, ROP). Each cover count is within 100× of its reference and the
  balance-sheet count is not, for example ROP's cover count of 100,356,523 and PEG's of 504,999,536.
- **Admitted rows** go from 243,406 to 243,416.
- **Check 4 coverage:** `untested` falls from 57,644 to 11,021 rows (census `me_checks.outcomes`).
- **The residual is cleared.** Admitted ME above $1T is now AAPL, MSFT, AMZN and GOOG, which are real, and PTP's 5
  months, the stated gap.

**Amendment 2.2 (2026-10-05): check 2's comparator basis and recovery.** Codex checkpoint 1 on Amendment 2.1 found
these in Amendment 2's existing check 2 (findings 16–19 and 27). Amendment 2.2's own checkpoint 1 is logged below.
- **Comparator basis (16).** SAB Topic 4C requires a split effective before the financial statements are issued to
  be reflected retroactively. The acceptance date stands in for issuance, as it already does for the fallback (an
  approximation, labelled in check 1). Amendment 2 instead multiplied the comparator by the stamps in
  (anchor, cover date], which counted a split before the filing twice. A 10:1 split shrank a ×1,000 cover error to
  exactly 100× and passed it, and a 1:10 reverse split did the same to a count in thousands.
  - The comparator is now multiplied by the stamps in (acceptance, cover date]. That set is empty whenever the cover
    date is on or before the filing, which is what a cover count is, so the comparison is of raw counts, as DQC_0095
    publishes it.
  - A cover dated after its own filing is malformed. It is compared on consistent bases rather than refused, and the
    rebuild's A/B lists any row it changes.
  - A split between the cover date and the filing already fails check 1.
- **Recovery (17).** Amendment 2 recovered only with exactly one conflicting comparator. It now recovers when every
  usable comparator conflicts and they all carry one count. A comparator that agrees with the cover, or comparators
  that disagree with each other, leave the conflict unresolved.
  - One read returns several accessions only when they share an acceptance timestamp (`value_as_of` keeps the events
    at the latest acceptance), so these comparators form a co-filed set. The highest accession number supplies the
    recovered fact, for a deterministic provenance.
- **Recovered basis (18).** A recovered balance-sheet count takes its own accession's acceptance as its basis. That
  equals the cover read's acceptance by the same invariant, so this changes no row; it is a provenance assertion,
  not a behavioural change, and has no revert probe.
- **Partial coverage (19)** is settled in check 2's outcome text above.
- **Census (27).** The per-reason "share of final ME" was zero by construction. The census already printed only the
  raw-ME share, and the text above now says so.
- **Known limits.**
  - An amendment that repeats a pre-split balance-sheet count after a split is put on the new filing's date and is
    read as post-split. Whether a re-filed statement must be restated is not settled by SAB Topic 4C's text.
  - A later co-filed set displaces the original filing for check 2, since the read keeps only the latest acceptance.
    If the amendment has no usable comparator, the original's conflict is no longer tested, and the check is
    `untested`.

**Slice 3d-iii result (2026-10-05).** A rebuild on the proof artefact's frozen inputs is compared with the 3d-ii rows
by `scripts/ab_3609_share_checks.py`. Per-row evidence is in `docs/research/3609-slice3d-iii-check2-changes.csv`.
- 1,381,227 rows are unchanged; none is unexplained.
- Every changed row belongs to one of nine names, and each has a reverse-split stamp in the window: BLIN, CEI, DCTH,
  EGLE, INPX, PHIO, SONN, SRRA and TARA. Amendment 2 had applied the reverse split to balance-sheet counts already
  restated for it, which made false scale conflicts.

  | verdict | rows |
  |---|---|
  | restored from `shares_scale_conflict` | 27 |
  | restored from `shares_discontinuity` (CEI: a restored count became the reference) | 5 |
  | check 2 now passes, and a later check removes the row (`shares_scale_conflict` → `shares_discontinuity` 16, → `shares_turnover_implausible` 2) | 18 |
  | removed: `shares_discontinuity` | 3 |

- **The three removals are clean counts:** DCTH's 9,007,952 at 2019-02 .. 2019-04. DCTH's restored 2018-02 count
  (2,591,509) became the reference. Its genuine dilution before the 1:500 split of 2018-05-02 then put the later count
  1,738× above it, split-adjusted. This is check 4's stated limit on real changes beyond 100×, which a correct
  reference exposed; Amendment 2.2's comparison is not its cause. The rows were admitted before only because no
  reference was within 15 months. DCTH's Intrader close is a flat 0.0999 through the period.
- **Admitted rows** go from 243,416 to 243,445. Check 2 `fail` falls from 350 to 305.
- **Recovered rows** stay at 996. The co-filed recovery rules (17) change no row in this population.
- **Admitted ME above $1T** is unchanged: AAPL, MSFT, AMZN, GOOG and PTP.

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

**XBRL mapping of the JKP items.** "Dropped" means not built: either no XBRL tag of matching scope was found, or a
multi-item construction (such as PI − TXT − MII) was deliberately left unimplemented. Each dropped branch is in the
matrix.

| JKP item | XBRL, in order | dropped branches |
|---|---|---|
| SALE / REVT | `Revenues`; `RevenueFromContractWithCustomerExcludingAssessedTax`; `SalesRevenueNet` | "including assessed tax" variant (SALE is net of excise taxes) |
| COGS | `CostOfGoodsAndServicesSold`; `CostOfRevenue`; `CostOfGoodsSold` (flag `cogs_scope=goods_only`). **Superseded by Amendment 2c's COGS*:** excluding-DDA total; `CostOfGoodsAndServicesSold`; `CostOfRevenue`; else goods + services | — |
| GP | `GrossProfit` | — |
| XSGA | `SellingGeneralAndAdministrativeExpense`. **Superseded by Amendment 2c's XSGA*:** SG&A + RD*; else G&A + selling + RD*; sums under the `OperatingExpenses` bound | — |
| `ope*` numerator | `sale*` − COGS − XSGA − XINT. This is FF's OP numerator, which JKP states it targets. Labelled a **proxy**: filed COGS and SG&A may include D&A, which Compustat's exclude. **Amendment 2c:** `sale*` − COGS* − XSGA* − XINT*; else GP − XSGA* − XINT* | EBITDA, OIBDP, XOPR |
| XINT | `InterestExpense`. **Amendment 2c's XINT*:** else `InterestAndDebtExpense`, else `InterestExpenseDebt` (proxies), else zero under the witness veto | — |
| IB | `IncomeLossFromContinuingOperations`, which the US-GAAP taxonomy defines as attributable to the parent; else `NetIncomeLoss` − `xido*` (Amendment 2b) | PI − TXT − MII |
| OANCF | `NetCashProvidedByUsedInOperatingActivities`; else continuing + discontinued (Amendment 2b) | NI − OACC; NI + DP − WCAPT |
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

**Amendment 2b (2026-10-05): `ni*` = IB, else NI − `xido*`; `ocf*` = OANCF, else continuing + discontinued.**

**Premise, measured.** With IB read only from `IncomeLossFromContinuingOperations`, `ni_me` has a value on 40,475 of
the 243,445 admitted name-months in the 3d-iii rows. At M = 2019-06-30 it is missing for AAPL, MSFT, JPM and HD.
`ocf_me` has a value on 176,855.

**Source rule.** JKP Table 5 (the pinned Documentation.pdf), verbatim:
- `ni*`: "We prefer to use IB. If this is unavailable, we use NI-XIDO*. If this is unavailable, we prefer
  PI*-TXT-MII";
- `xido*`: "We prefer to use XIDO. If this is unavailable, we use XI+DO where we set DO to zero if missing";
- `ocf*`: "We prefer to use OANCF. If this is unavailable, we use NI*-OACC*. If this is unavailable, we use NI* +
  DP - WCAPT". JKP states no zero rule for OANCF.

No XBRL tag combines extraordinary items and discontinued operations, so `xido*` is always XI + DO here. ASC 205-20
and ASC 230 require the presentation, not a tag, so neither justifies a zero by itself.

**Rule.**
- **Per period.** The branch is chosen for the annual period, or for each quarter before the TTM chain. IB and
  NI − `xido*` are one Compustat item, so a TTM may mix quarters from both.
- **Blocking precedence.** Every period's parts go through `combine`, as everywhere in the panel: `ambiguous`,
  `blocked_by_rejection` and `form_mismatch` on any part (base or companion) block the period; only `absent` falls
  through.
- **`ni*`:** `IncomeLossFromContinuingOperations`; else `NetIncomeLoss` − XI − DO. Both base tags are attributable
  to the parent in the US-GAAP taxonomy.
  - **XI:** `ExtraordinaryItemNetOfTax`; if absent, zero (adaptation a).
  - **DO:** `IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity`; else
    `IncomeLossFromDiscontinuedOperationsNetOfTax` −
    `IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToNoncontrollingInterest`, which restores the parent
    scope; else the consolidated tag alone, a **proxy** that includes any noncontrolling share; else zero (JKP's rule,
    with adaptation b).
- **`ocf*`:** `NetCashProvidedByUsedInOperatingActivities`; else
  `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations` +
  `CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations`, the discontinued term zero if absent
  (adaptation b). This is **not** a JKP branch. It reconstructs OANCF on the **assumption** that Compustat's OANCF
  is total operating cash flow, continuing plus discontinued; Compustat's definition is not available to us. The
  zero is a second **assumption**: with no discontinued operations reported, continuing operating cash flow is the
  total.
- **Adaptations from JKP, each separate:**
  - **a. XI absent is zero.** ASU 2015-01 eliminated extraordinary items for fiscal years beginning after
    2015-12-15. An interval starting on or after 2015-12-16 (annual) or 2016-12-16 (quarter) lies in such a fiscal
    year whatever the year end. For any other interval this is an **assumption**.
  - **b. Witness veto.** An absent DO, or an absent discontinued operating cash flow, becomes zero only if no public
    fact on a witness concept overlaps the interval with a non-zero current value. "Current" means the value at
    that key's latest acceptance before s(M) (a correction to zero clears the veto). The prefix read is the
    acceptance-NY-date < s(M) slice (`PrefixCache.at`), so a later filing cannot veto an earlier formation. The
    veto is a conservative refusal, not a test of the target amount. An overlapping annual fact may belong to
    another quarter, and pretax or noncontrolling detail can be non-zero while the target is zero. So it also
    refuses some valid zeros, and those periods stay missing.
    - DO witnesses: both DO tags, the noncontrolling DO tag,
      `DiscontinuedOperationIncomeLossFromDiscontinuedOperationBeforeIncomeTax`,
      `DiscontinuedOperationGainLossOnDisposalOfDiscontinuedOperationNetOfTax` and
      `DiscontinuedOperationIncomeLossFromDiscontinuedOperationDuringPhaseOutPeriodNetOfTax`.
    - XI witness: `ExtraordinaryItemNetOfTax`.
    - Discontinued operating cash flow witnesses: the DO witnesses,
      `CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations` and
      `NetCashProvidedByUsedInDiscontinuedOperations`.
    - The witnesses were chosen from the USD duration tags the admitted CIKs file (`measure_3609_amendment_2b.py
      tags`, output in `docs/research/3609-amendment-2b-disc-tags.txt`; all 4,715 admitted CIKs have a companyfacts
      member).
  - **c.** The consolidated-DO proxy, above.
- **Interval match.** A companion whose interval differs from the base term's makes that period `absent`.
- **Still dropped:** PI* − TXT − MII; NI* − OACC*; NI* + DP − WCAPT.

**Measurement** (`scripts/measure_3609_amendment_2b.py measure`, summary in
`docs/research/3609-amendment-2b-measurement.json`).
- **Method.** It replays `factor_panel.characteristic` on every admitted name-month of the 3d-iii rows (sha256
  `c2b3eb69…f060d`). The scratch #3360 bundle is built from the pinned bundle's own `inputs/` with exactly the 10
  concepts below added (manifest sha256 `9e643922…6928b`; never published).
- **Replay check.** The replayed current `ni_me` and `ocf_me` equal the stored rows on value, missing reason, period
  end and kind for all 243,445 (0 mismatches). Facts and branches were not compared.
- **The oracle.** Its per-row output is the oracle for slice 3d-iv: `output_rows_sha256`, the sha256 of the decompressed JSONL, is
  `068afe2c…92dbe3`.

All counts are name-months:

| variant | values | values using the fallback | with an imputed DO / discontinued-OCF zero | with an imputed zero overlapped by a non-zero witness |
|---|---|---|---|---|
| `ni_me` today | 40,475 | — | — | — |
| NI − XI − DO, no imputed zero | 40,467 | 0 | 0 | — |
| NI − XI − DO, zero if absent | 219,917 | 192,245 | 187,373 | 3,379 |
| **NI − XI − DO, adopted** | **218,578** | **189,884** | **184,380** | **0 (the veto)** |
| `ocf_me` today | 176,855 | — | — | — |
| continuing + discontinued, no imputed zero | 180,688 | 4,397 | 0 | — |
| continuing + discontinued, zero if absent | 234,671 | 70,271 | 66,830 | 2,624 |
| **continuing + discontinued, adopted** | **233,252** | **68,244** | **64,423** | **0 (the veto)** |

- **Without imputation `ni*` is IB alone.** XI is filed by 15 admitted CIKs, so NI − XI − DO without an imputed XI
  zero yields no fallback value.
- **Adopted `ni*`, 189,884 fallback values:**
  - an imputed XI zero in every one, of which 91,374 are in an interval not certainly under ASU 2015-01;
  - an imputed DO zero in at least one period in 184,380; DO filed or derived in every fallback period in 5,504;
  - DO branches, by value (a TTM can use several): parent 1,738; consolidated minus noncontrolling 246; consolidated
    proxy 5,513;
  - 7,871 values mix IB and NI − XI − DO quarters.
- **Adopted `ocf*`, 68,244 fallback values,** partitioned disjointly by precedence:
  - an imputed zero in some period: 64,423;
  - otherwise a non-imputed zero (filed, or derived by a YTD difference or Q4 residual): 1,343;
  - otherwise all non-zero: 2,478.

  9,792 values mix total and continuing + discontinued quarters.
- **Witness evidence without the veto.** Of the 3,379 `ni*` values, 1,522 overlap a non-zero parent DO fact itself:
  the filer reports DO for an overlapping interval from which the matched interval could not be derived. This is
  overlapping evidence, not a proven wrong zero (see b).
- **Period changes,** where both readings have a value:
  - `ni_me`: the new period end is newer in all 11,414 changes, and 335 more keep the end but change kind;
  - `ocf_me`: newer in all 11,795.
- **Value to missing:** 32 (`ni_me`) and 52 (`ocf_me`), each choosing a newer period that is blocked. The choosing
  rule stops there, as it does for every characteristic.
- **Period evaluations** (not name-months), per variant: the adopted `ni*` veto refused a zero 30,140 times, and the
  adopted `ocf*` veto 29,538. Interval mismatches: 235 and 12.
- **Default panel at 2019-06-30.** AAPL, MSFT, JPM and HD gain `ni_me` from NI − XI − DO, with imputed XI and DO
  zeros. GME's is unchanged (IB). `ocf_me` is unchanged for all five. The rows are in the summary's
  `default_panel`.

**Concept-set extension: 10 concepts.** The four branch concepts (both DO tags, continuing OCF, discontinued OCF),
plus `ExtraordinaryItemNetOfTax`, the noncontrolling DO tag, the three DO detail tags and
`NetCashProvidedByUsedInDiscontinuedOperations`. It goes through Amendment 1's bundle A/B and linkage replay
unchanged.

**Known limits.**
- The veto is bounded by its concept list. A discontinued operation tagged only with a company extension, or with a
  standard tag outside the list, leaves a non-zero amount unwitnessed. That residual is not measurable.
- The consolidated-DO proxy, the pre-ASU XI assumption and the OANCF scope assumption stay in the alignment matrix.

**Amendment 2c (2026-10-05): `ope*` = `ebitda*` − XINT read as JKP and Compustat define its parts.**

**Premise, measured** (`scripts/measure_3609_amendment_2c.py components`, over the canonical stage-A rows
`2026-10-05-191d7b07-stageA`, decompressed sha256 `b9724baa…7805`). `ope_be` has a value on 46,086 of the 243,445
admitted name-months; 179,808 are `no_period`. At the latest lag-eligible annual period of those 179,808, the absent
components are: XSGA 134,101; COGS 102,179; XINT 68,869; `sale*` 58,688; BE 3,311 (a name-month can lack several).
XSGA alone is the only gap in 18,259 and XINT alone in 11,707. Of the 4,715 admitted CIKs, 2,016 file
`SellingGeneralAndAdministrativeExpense` in a 10-K/10-Q filed 2013-01-01 .. 2021-06-30; 2,194 file
`GeneralAndAdministrativeExpense`, 2,164 `ResearchAndDevelopmentExpense`, 3,698 `InterestExpense` and 547
`CostOfServices` (`… tags`, output in `docs/research/3609-amendment-2c-tags.txt`). The 16:41Z hypothesis was half
right: MSFT's gap is XSGA (it tags R&D, sales and marketing, and G&A separately); MSFT does file `InterestExpense`.

**Source rule.**
- JKP Table 5 (the pinned Documentation.pdf), verbatim:
  - `ope*`: "We use EBITDA*-XINT. Note that we target the same variable as the numerator of the profitability
    characteristic used to create the Robust-minus weak factor in the fama-French 5 factor model";
  - `ebitda*`: "We prefer to use EBITDA. If this is unavailable, we use OIBDP. If this is unavailable, we use
    SALE*-OPEX*. If this is unavailable, we use GP*-XSGA";
  - `opex*`: "We prefer to use XOPR. If this is unavailable, we use COGS+XSGA";
  - `gp*`: "We prefer to use GP. If this is unavailable we use sale*-COGS";
  - XSGA, COGS and XINT are each "Compustat item", with no missing rule.
- **Compustat `xsga` includes R&D.** "Compustat item xsga includes SG&A and R&D (after excluding rdip)", and where
  R&D sits in COGS "Compustat coded R&D expenses in item xrd, but did not include xrd in xsga" (Chiu, Jagannathan and
  Tseng, "Franchise Value, Intangibles, and Tobin's Q", NBER w30829, revised March 2023, the data section and its
  footnote on `xsga` < `xrd`; PDF sha256 `6850b463…6d6522`, downloaded 2026-10-05). This is a secondary source; the
  Compustat manual is not available to us.
- **The target variable's admission rule.** Ken French's 5-factor page (`f-f_5_factors_2x3.html`, fetched
  2026-10-05, sha256 `67c2150c…bccc2`) admits a firm to RMW with "non-missing revenues and at least one of the
  following: cost of goods sold, selling, general and administrative expenses, or interest expense"; the variable
  definitions page (sha256 `48a73927…98c4454`) defines OP as "annual revenues minus cost of goods sold, interest
  expense, and selling, general, and administrative expense divided by the sum of book equity and minority
  interest". Neither page states how a missing component enters the sum. Admitting a firm with two of the three
  missing implies they do not make OP missing; reading them as zero is our **inference**, used only where JKP is
  silent (JKP stays the single source for everything it states, round 1, 18–20).
- **US-GAAP definitions** of every tag the rule reads or uses as a witness are in
  `docs/research/3609-amendment-2c-taxonomy.txt`, from the FASB documentation linkbases (2021, sha256
  `b6c606d7…823f72`; 2017, sha256 `c276d7b3…a532b8`, for the tags the 2021 taxonomy no longer carries). The ones the
  rule turns on:
  - `OperatingExpenses`: "Generally recurring costs associated with normal operations except for the portion of
    these expenses which can be clearly related to production and included in cost of sales or services. Includes
    selling, general and administrative expense";
  - `ResearchAndDevelopmentExpense` includes "costs allocated in accounting for a business combination to in-process
    projects deemed to have no alternative future use"; `ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost`
    excludes acquired in-process R&D and "Excludes software research and development, which has a separate
    concept";
  - `CostOfGoodsSold` (2017): "Total costs related to goods produced and sold"; `CostOfServices` (2017): "Total costs
    related to services rendered"; the three excluding-DDA cost tags exclude "depreciation, depletion, and
    amortization";
  - `InterestAndDebtExpense`: "Interest and debt related expenses associated with nonoperating financing
    activities"; `InterestExpenseDebt`: "Amount of the cost of borrowed funds accounted for as interest expense for
    debt"; `CostsAndExpenses`: "Total costs of sales and operating expenses for the period".
- EBITDA, OIBDP and XOPR have no US-GAAP tag of matching scope (unchanged from the mapping table).

**Rule.** Every term of `ope*` and `gp*` is read per period: for the annual period, or for each quarter before the TTM
chain, as Amendment 2b reads `ni*`. So `gp*` = GP, else `sale*` − COGS*, is chosen per quarter too. Within a period every part must cover the interval of the chosen `ebitda*` branch's base
(`sale*`, or GP): a value over another interval is `absent`, as `factor_panel.companion` already treats companions.
`combine`'s blocking precedence holds for every part, and only `absent` falls through.
- **`ope*`** = `ebitda*` − XINT*.
- **`ebitda*`** = `sale*` − COGS* − XSGA*; else GP − XSGA* − XINT* with GP filed (JKP's last branch, added), each
  part aligned to GP's interval. EBITDA, OIBDP and XOPR stay dropped.
- **XSGA*** (Compustat's scope: SG&A plus R&D net of in-process R&D):
  - `SellingGeneralAndAdministrativeExpense` + RD*;
  - else, when `GeneralAndAdministrativeExpense` is filed: G&A + selling + RD*. Selling is
    `SellingAndMarketingExpense`, else `SellingExpense`; absent, it is zero unless a non-zero `SellingAndMarketingExpense`,
    `SellingExpense`, `MarketingExpense` or `MarketingAndAdvertisingExpense` fact overlaps (the Amendment 2b veto).
    With G&A absent the period is `absent`, unless RD* is blocked, which blocks it;
  - RD* = `ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost` + software R&D (zero unless a non-zero
    software R&D fact overlaps); else `ResearchAndDevelopmentExpense`, a **proxy** that can hold expensed acquired
    in-process R&D. Absent, RD* is zero unless a non-zero fact on any of the three overlaps.
- **The `OperatingExpenses` bound.** It tests only a sum the rule makes: SG&A + a non-zero RD*, and every G&A +
  selling + RD*. A filed SG&A alone is taken as filed and not tested. With `OperatingExpenses` read as a value over the
  base interval:
  - a sum above it by more than 0.5% of it cannot be right as a scope (the taxonomy defines it as every non-COGS
    operating cost, SG&A included). SG&A + RD* then falls back to SG&A alone, which is also Compustat's reading when
    R&D sits in COGS, labelled `rd_excluded_opex_bound`; G&A + selling + RD* makes the period `absent`, labelled
    `veto_opex_bound`;
  - at or below it the sum stands. The bound is one-sided: a duplicated part hidden by other operating lines (SG&A
    100 containing R&D 20, other lines 50: 120 against 150) passes. It is a refusal of contradicted sums, not a
    proof that a passing sum is right.

  A blocked `OperatingExpenses` blocks the sum (a guard in doubt is not a pass). An absent one, or one over another
  interval, leaves the sum unchecked, labelled and counted. The 0.5% tolerance is fixed by construction. Slice 3d-v
  records the `OperatingExpenses` key and acceptance with every tested sum, separately from the arithmetic facts.
- **XINT*** = `InterestExpense`; else `InterestAndDebtExpense`; else `InterestExpenseDebt`. Both fallbacks are
  **proxies**: Compustat's XINT definition is not available to us, and the first adds debt-related expense, the second
  covers debt only. Absent, XINT* is zero unless a non-zero fact overlaps on any of those, `InterestExpenseBorrowings`,
  `InterestExpenseLongTermDebt`, `InterestExpenseRelatedParty`, `InterestExpenseOther`, `InterestExpenseDeposits`,
  `InterestCostsIncurred`, `InterestPaidNet` or `InterestPaid` (chosen from the admitted CIKs' tags).
  - The zero is **adaptation d**, with three separate assumptions: FF's admission read as a zero (above); a missing
    Compustat item read as an absent XBRL tag; and an annual rule applied to each quarter.
  - The cash and incurred-interest witnesses are conservative, as in Amendment 2b. Interest paid can settle an
    earlier accrual, and incurred interest can be capitalised, so they also refuse some true zeros. Refusals are
    counted.
- **COGS*** (shared with `gp*`): Compustat's COGS excludes depreciation, so each excluding-DDA tag is read before its
  inclusive counterpart.
  - `CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization`; else `CostOfGoodsAndServicesSold`; else
    `CostOfRevenue`;
  - else goods + services: goods is the goods excluding-DDA tag, else `CostOfGoodsSold`; services is the services
    excluding-DDA tag, else `CostOfServices`. One of the two must be filed; the other is zero unless a non-zero fact on
    its own two tags overlaps. That zero is **adaptation e**: a cost of the other kind tagged with an industry or
    extension tag is not seen. Today's goods-only read (`cogs_scope=goods_only`) already reads goods alone with no
    services term, so adaptation e adds services where filed and imputes nothing the current rule does not already
    omit, except for services-only filers, whose goods zero is new and counted.
- **Rejected, each measured below:**
  - **COGS zero when no cost-of-sales tag is filed.** A cost-of-sales line carries industry tags with no bounded list
    (`BenefitsLossesAndExpenses`, `CostOfRealEstateRevenue`, `DirectCostsOfLeasedAndRentedPropertyOrEquipment` among
    the admitted CIKs' tags), so no witness list can veto a wrong zero, and the resulting `ope*` would be overstated for
    exactly the filers whose costs are tagged differently.
  - **`CostsAndExpenses` as XOPR** (in JKP's order, before COGS* + XSGA*). "Total costs of sales and operating
    expenses" is the whole of the operating cost lines, so it holds whatever operating lines a filer presents
    (depreciation, impairment, restructuring); JKP's fallback COGS + XSGA does not, and the tag carries no scope
    detail to separate them.

**Measurement** (`scripts/measure_3609_amendment_2c.py measure`; summaries `docs/research/3609-amendment-2c-m3.json`, every
variant, and `-m4.json`, the adopted variant with period-evaluation counts; m3 ran before the counters were
added, which change no reading: m4's adopted and `gp_at` rows equal m3's, 0 differences). m4's decompressed
output rows sha256 is `9d7c05ec…0cba`.
It replays `factor_panel.characteristic` on every admitted name-month of the canonical rows against a scratch #3360
bundle built from the pinned bundle's own `inputs/` with exactly the 24 concepts below added (manifest sha256
`f178b1e4…db53`; never published). The replayed current `ope_be` and `gp_at` equal the stored rows on value, missing
reason, period end and kind for all 243,445 (0 mismatches). Rows r0–r5 each add one change to the one
before; the rejected rows each add one branch to the adopted rule.

| `ope_be` | values | `no_period` | same-period value changes | period changes |
|---|---|---|---|---|
| stored | 46,086 | 179,808 | — | — |
| r0 per-period read of today's rule | 46,082 | 180,037 | 0 | 158 |
| r1 + GP − XSGA | 53,478 | 172,248 | 5 | 1,732 |
| r2 + RD* | 52,982 | 172,461 | 20,594 | 6,969 |
| r3 + G&A + selling + RD* | 77,964 | 139,474 | 20,449 | 7,218 |
| r4 + XINT* | 99,331 | 118,536 | 19,963 | 8,012 |
| r5 + COGS* | 102,071 | 115,365 | 20,711 | 8,251 |
| **adopted: r5 + the bound** | **101,328** | **115,873** | **20,458** | **8,247** |
| rejected: adopted + COGS zero | 124,461 | 89,509 | 20,183 | 8,692 |
| rejected: adopted + `CostsAndExpenses` as XOPR | 117,011 | 98,606 | 23,328 | 8,209 |

Changes are against the stored row, counted where both readings have a value.

- **Same-period value changes, adopted versus stored** (`ope_be` units): 20,458, of which 20,344 lower; p10 −0.630,
  median −0.108, p90 −0.018. Lower is the direction R&D and the XINT* fallbacks move it (more cost deducted). AAPL at
  2019-06-30 goes from 0.6727 to 0.5477.
- **Adopted branch counts** (name-months with a value; a TTM can use several): G&A + selling + RD* 34,494; GP −
  XSGA* 16,183; RD* filed 60,838 (inclusive tag 58,374, excluding-IPR&D 2,763), imputed RD* zero 40,000; R&D excluded
  by the bound 1,315; bound tested and passed 38,032, unchecked 34,799; selling zero 8,861; XINT* zero 14,821,
  `InterestAndDebtExpense` 3,428, `InterestExpenseDebt` 5,453; goods + services 22,715, with a services zero 17,478
  and a goods zero 2,508.
- **Refusals, as period evaluations** (m4, adopted only; a refused period leaves no label on a missing row):
  - the bound: passed 1,113,249; refused 65,018 (R&D excluded 21,497, a sum of parts made `absent` 43,521);
    unchecked 837,875; never blocked;
  - imputed zero and veto refusal, per companion: RD* 2,279,800 and 896,542; selling 734,681 and 120,689; XINT*
    348,666 and 1,384,332; services 438,314 and 2,850; goods 63,119 and 2,052; software R&D 65,917 and 5;
  - the XINT* witnesses refuse about four zeros for each one they admit. Those periods stay missing, which is the
    conservative direction; no cause is claimed here.
- **`gp_at`** (stored 140,668 values): today's rule read per period, 140,680, with 2,563 same-period changes
  (median −0.012) and 382 period changes, from quarters mixing GP and `sale*` − COGS; with COGS*, 145,283, with 4,903
  same-period changes (median −0.055, 4,220 lower) and 1,514 period changes. Goods + services in 15,253 values, a
  services zero in 9,247 and a goods zero in 3,141.
- **Descriptive only:** at each CIK's annual anchors read at its last formation, with every term a value over
  `OperatingExpenses`' interval, `OperatingExpenses` was within 0.5% of SG&A + RD* at 1,197 anchors, above
  it at 1,647 and below at 230; with SG&A absent, within 0.5% of G&A + selling + RD* at 2,880, above at 2,000 and below
  at 207. These motivated the bound; they do not validate it, which is the job of
  the slice's hand-computed tests and the per-name-month refusal counts above.
- **Default panel at 2019-06-30** (adopted): AAPL 0.5477 (was 0.6727); GME 0.2714 (unchanged); MSFT 0.3841 (was
  missing: G&A + sales and marketing + R&D); HD `nonpositive_denominator` and JPM `no_period` (unchanged). `gp_at` is
  unchanged for all five.

**Concept-set extension: 24 concepts.** `CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization`,
`CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization`, `CostOfServices`,
`CostOfServicesExcludingDepreciationDepletionAndAmortization`, `CostsAndExpenses` (for the rejected branch's
measurement only; not built), `GeneralAndAdministrativeExpense`, `InterestAndDebtExpense`, `InterestCostsIncurred`,
`InterestExpenseBorrowings`, `InterestExpenseDebt`, `InterestExpenseDeposits`, `InterestExpenseLongTermDebt`,
`InterestExpenseOther`, `InterestExpenseRelatedParty`, `InterestPaid`, `InterestPaidNet`,
`MarketingAndAdvertisingExpense`, `MarketingExpense`, `OperatingExpenses`, `ResearchAndDevelopmentExpense`,
`ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost`,
`ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost`, `SellingAndMarketingExpense`,
`SellingExpense`. The build adds the 23 without `CostsAndExpenses`.

**Known limits.**
- Filed SG&A, G&A and inclusive COGS may include depreciation, which Compustat's exclude; the `ope*` proxy label
  stands.
- Unmeasurable residuals: a duplicated part the bound cannot see (one-sided); a sum unchecked where
  `OperatingExpenses` is not filed; a part tagged only with a company extension (selling or R&D undercounted); R&D in
  COGS with room under the bound (counted twice); expensed acquired IPR&D under the inclusive R&D tag.
- Imputed selling, R&D, software, goods, services and interest zeros are bounded by their witness lists, as in
  Amendment 2b.
- Filers with neither COGS* nor GP (banks, insurers, many utilities) stay without `ope_be`.

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
| Amendment 2b: NI − XI − DO and continuing + discontinued OCF (OANCF scope assumed total); consolidated-DO proxy; imputed XI zeros outside ASU 2015-01; imputed DO and discontinued-OCF zeros under the witness veto; discontinued operations outside the witness list undetected | per branch: value counts, imputed-zero counts (XI by ASU certainty), DO-branch counts, veto refusals; the undetected residual is not measurable |
| Amendment 2c: R&D added to SG&A (Compustat scope, secondary source); inclusive-R&D and XINT fallback proxies; G&A + selling + RD* where SG&A is not filed; the one-sided `OperatingExpenses` bound, unchecked where not filed; adaptations d (XINT zero) and e (goods or services zero); other imputed zeros under the witness veto; filers with neither COGS* nor GP stay missing | per branch: value counts, imputed-zero counts, bound refusals and unchecked sums, veto refusals; the residuals listed under the amendment are not measurable |
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
   - **3d-ii (Amendment 2.1): reference eligibility.**
     - **Pure tests:**
       - an untested count agreeing with a partner from another filing becomes a reference, then rejects a ×1,000
         count;
       - it does not when the partner shares its accession or its count date, disagrees beyond 100×, or is absent,
         or when its own check 3 is untested;
       - a recovered count never becomes one;
       - a count first read with check 3 untested does not displace an earlier qualifying partner.
     - **Full-population A/B against the slice-3d rows,** same frozen inputs:
       - `me.checks`, `me.verified` and `me.raw` are diagnostics: a change confined to them (for example check 4
         going from `untested` to `pass`) leaves the row `unchanged`;
       - every other change is ME newly missing with an Amendment 2 reason, ME restored because the check that
         removed it no longer fails, or a changed DQC recovery. Each has its admission and ME-denominated
         characteristics following. Any other difference refuses;
       - every newly rejected count is listed and adjudicated as a filer error or a clean count. **One clean count
         rejected blocks adoption.**
     - `scripts/ab_3609_share_checks.py` gains a `--csv` writer, and the slice-3d evidence CSV is regenerated with it.
   - **3d-iii (Amendment 2.2): check 2's comparator basis and recovery.**
     - **Pure tests, each revert-probed:**
       - a ×1,000 cover count against a balance-sheet count already restated for a 10:1 split fails check 2;
       - a count in thousands against one restated for a 1:10 reverse split fails;
       - with a reference, co-filed comparators carrying one count recover to the highest accession number;
       - comparators that disagree with each other do not recover, and neither does a conflict when another
         comparator agrees with the cover.
     - The existing tests keep recovery to each side, no recovery without a reference and `untested` with no usable
       comparator. Finding 18 is a provenance assertion, not probed.
     - **Full-population A/B against the 3d-ii rows,** same frozen inputs and the same classes and refusal as 3d-ii.
       Every changed row is listed in an evidence CSV and adjudicated.
   - **3d-iv (Amendment 2b): the `ni*` and `ocf*` branches.** Two parts, in order.
     - **Bundle extension (corpus rung):**
       - the 10 concepts go through Amendment 1's bundle A/B (`ADDED_CONCEPTS` in
         `scripts/ab_3609_concept_extension.py` becomes this amendment's 10) and the linkage replay with its
         byte-identity A/B;
       - the new bundle's manifest must equal the scratch bundle's (`9e643922…6928b`) in every field except
         `policy`, which hashes the edited source. Both are built by the same concept set from the same inputs, and
         the manifest lists every shard's digest. A difference refuses until explained;
       - **cross-source check, a sample validation, not population evidence:** for each of the 10 concepts, one
         admitted filer's stored event against the same fact in its SEC EDGAR filing. Amount, start, end and
         accession must agree; any disagreement refuses. Population safety rests on the A/B below.
     - **Panel branches.** Pure tests, each revert-probed:
       - IB and OANCF win over their fallbacks in the same period, annual and quarterly;
       - companions read through each quarterly branch (direct, YTD difference, Q4 residual) and annually;
       - an absent XI is zero; a filed XI is subtracted, filed DO or not;
       - DO: the parent tag first; consolidated minus noncontrolling; the consolidated proxy flagged;
       - an absent DO or discontinued OCF with no witness is zero. A witness overlapping with a non-zero current
         value makes the period `absent`. A witness corrected to zero does not veto. A witness accepted on or after
         s(M) does not veto;
       - each blocking status on the base or on any companion blocks the period, including an absent base with a
         blocked companion;
       - an interval mismatch makes the period `absent`;
       - a TTM mixing primary and fallback quarters chains.
     - **Full-population A/B against the 3d-iii rows:**
       - **Allowed changes.** The inputs are the frozen 3d-iii inputs, except that the `BUNDLE` and `LINKAGE` pins
         move to the new artefacts. The linkage A/B makes every link identical. In each row, only `ni_me` and
         `ocf_me` may change, plus the manifest's provenance pins. Any other difference refuses.
       - **Oracle.** For every row, `ni_me` and `ocf_me` (value, missing reason, period end, kind) must equal
         `ni_adopted` and `ocf_adopted` in the measurement's output rows (decompressed sha256 `068afe2c…92dbe3`), exactly. Any
         difference refuses. Adopting a different oracle needs a new measurement run, with its digest and the
         reason, recorded on the PR.
       - **Oracle m6 (adopted for the census clause).** #3658 passed against m5 with five ALCO `ni_me` readings
         adjudicated, because m5's witness filter skipped facts filed without a start. m6 dates such a fact at its
         end, as the panel does, and replays the 3d-iv part 2 rows: decompressed sha256 `0de6e959…dbe0`, summary
         `docs/research/3609-amendment-2b-measurement-m6.json`. Against m5 it moves exactly those five readings;
         against the stored rows it moves none. `scripts/ab_3609_vetoes.py` checks it with no adjudication.
       - The branch, imputed-zero, DO-branch and veto counts go into the census.
   - **3d-v (Amendment 2c): the `ope*`, XSGA*, XINT* and COGS* reads.** Same two parts as 3d-iv.
     - **Bundle extension (corpus rung):** the 23 built concepts through Amendment 1's bundle A/B and the linkage
       replay; the new manifest equals a scratch bundle built from the same 23 in every field but `policy`; the
       cross-source sample check, one admitted filer's stored event per concept against its SEC EDGAR filing.
     - **Panel reads: pure tests with hand-computed expected values, each revert-probed.** These, not the oracle, are
       the independent check, since the oracle replays the measurement's own code:
       - the per-period read, through direct, YTD-difference and Q4-residual quarters, and a TTM mixing branches;
       - `gp*` − XSGA* only when `sale*` − COGS* − XSGA* is `absent`, with `sale*` present on another interval: XINT*
         and the quarter start follow GP;
       - a part over another interval than the base is `absent` (SG&A, G&A, COGS*, XINT*, `OperatingExpenses`);
       - RD*: excluding-IPR&D plus software; the inclusive fallback labelled; absent with and without a witness;
       - G&A + selling + RD* only without SG&A; selling absent, zero or vetoed by a marketing fact; G&A absent with a
         blocked RD* blocks;
       - the bound: R&D inside SG&A (sum above `OperatingExpenses`, SG&A alone kept); R&D in COGS with slack (passes:
         the one-sided residual, asserted as such); parts above it (`absent`); within tolerance; zero and negative
         `OperatingExpenses`; blocked (blocks); absent or another interval (unchecked); SG&A with RD* zero (not
         tested); the `OperatingExpenses` key and acceptance recorded;
       - XINT*: each branch; absent with no witness (zero); each witness kind vetoes, including an annual interest
         fact over a quarter; a witness corrected to zero or accepted on or after s(M) does not veto;
       - COGS*: excluding-DDA before inclusive; goods + services; each side's zero and veto; services-only;
       - blocking states on every part.
     - **Full-population A/B against the canonical stage-A rows:** only `ope_be` and `gp_at` may change, plus the
       provenance pins; each must equal the adopted variant of a measurement re-run against the built bundle,
       exactly. Branch, imputed-zero, bound and veto counts go into the census.
     - **Part 2 result (2026-10-05).** `factor_panel.py` holds the adopted rule (`Period`, `rd_read`, `bounded`,
       `xsga_term`, `cogs_term`, `xint_term`, `ope_unit`, `gp_unit`, `period_flow`); the measurement script now
       imports its tags from there. The bound's `OperatingExpenses` reads are stored as `guards` on the
       characteristic, apart from `facts`, over every tested period as `vetoes` are; the key is written only where a
       bound read something, so no other characteristic's row changes shape. When both `ope*` branches are
       `absent`, their refusal labels and guard reads stay on the row, except those of a branch whose base had no
       interval to test against. The census adds `veto_use` for `ope_be` and `gp_at`.
       - Oracle: the adopted variant re-run against bundle B from a clean `eeec97b2` worktree (`m5B`) reproduces
         m4's rows exactly (decompressed sha256 `9d7c05ec…0cba`), with 0 replay mismatches.
       - A/B (`scripts/ab_3609_ope_gp.py`): 1,381,280 rows, 243,445 admitted, 0 failures, none adjudicated;
         `ope_be` values 101,328 and `gp_at` 145,283, as measured.
       - Census refusals (name-month labels): RD* 896,542, selling 120,689 and XINT* 1,384,332 evaluations, equal
         to m4's period-evaluation counts. COGS goods 1,991, services 2,415 and the bound 41,366 are lower than
         m4's, because the census keeps only refusals that left a period `absent`. m4 also counted refusals the GP
         branch rescued, and bound refusals tested without a base interval.
       - Cross-source: every fact behind MSFT's and AAPL's `ope_be` and `gp_at` at 2019-06-30 equals SEC
         `companyconcept` on accession, start and end. The ratios recomputed from SEC's values equal the stored
         ones: MSFT 0.38406, AAPL 0.54774.
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
- XBRL as filed is not Compustat; several JKP branches are dropped and two items are proxies. Amendment 2c leaves
  filers with neither COGS* nor GP (banks, insurers, many utilities) without `ope_be`.
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
  their concept-set extension and the tag measurement). The checkpoint transcript was not kept. Amendment 2b
  answers them as summarised in the 2026-10-05 05:21Z handoff on #3609:
  - 1, 4: no zero rests on presentation rules. The DO zero is JKP's rule with the witness veto; XI and the OCF zero
    are labelled adaptations, with their measured uses;
  - 2, 3: the consolidated DO is a counted proxy, with consolidated minus noncontrolling preferred; the pre-ASU XI
    zero is a counted assumption;
  - 5: JKP's `ni*`, `xido*` and `ocf*` rules are quoted from the pinned PDF;
  - 6–9: period-level replay through the choosing rule. Transitions are counted directly; the imputed, non-imputed
    zero and non-zero partitions are disjoint;
  - 11: the measurement binds its input rows and the scratch bundle by sha256, and pins its output;
  - 12: a cross-source check for each added concept;
  - 13: the branch is chosen per period, before the TTM chain;
  - 14: only `absent` becomes zero;
  - 10: the handoff summary does not name it, so its disposition cannot be traced. Amendment 2b's own checkpoint 1,
    below, is the review of record for the whole branch design.

**Amendment 2b, checkpoint 1 (30 findings), all applied.** The measurement was rebuilt and re-run after it.
- **Rule:**
  - 1: XI is read and subtracted whether or not DO is filed;
  - 4: consolidated minus noncontrolling DO comes before the proxy;
  - 10: witnesses use each key's current value;
  - 12: every period's parts go through `combine`, so blocking precedence holds.
- **Measurement:**
  - 13: the replay compares period end and kind as well;
  - 14: transitions compare kind and missing reason;
  - 15, 16: a disjoint partition, with "non-imputed zero" covering derived zeros;
  - 17: the witness counts use each rule's own witness set;
  - 18: baseline totals are recorded;
  - 19: XI zeros are classified by fiscal-year start;
  - 20: DO-branch and mixed-TTM counts are recorded;
  - 21: the direction of period changes is recorded;
  - 22: the default panel rows are recorded;
  - 24: the scratch bundle holds exactly the 10 adopted concepts;
  - 30: evaluations are counted per variant.
- **Text:**
  - 2: the three adaptations are listed separately;
  - 3: XI is classified by interval start against the ASU 2015-01 date;
  - 5: the OANCF mapping is a labelled assumption;
  - 6: "dropped" is redefined;
  - 7–9: the veto is labelled a conservative detector, with its limits;
  - 11: point-in-time witnesses are stated and tested;
  - 23: the table's columns are labelled.
- **Slice 3d-iv:**
  - 25: a pinned row oracle with exact agreement;
  - 26: the pure-test list widened;
  - 27: the cross-source check is labelled a sample, with what must agree;
  - 28: the allowed artefact changes are listed;
  - 29: finding 10's disposition is stated as untraceable.

**Amendment 2.1, checkpoint 1 (27 findings).**
- **Applied to the rule:**
  - 5, 12: the partner is the last admitted count that passed check 3 and was not recovered;
  - 10, 11: disjoint accession sets and a different count date.
- **Applied to the text:**
  - 1–4, 21–23: the simulation is labelled a guide, and the adopted figures come from the exact rebuild;
  - 6–9: the shared mis-scaling, a reference outranking current agreement, refresh, drift and real >100× changes;
  - 13: every `untested` state is admitted to consensus, deliberately;
  - 14: the formation window is separated from the share-age bound;
  - 20: the rule is a project heuristic, with no published rule cited;
  - 24, 25: the A/B treats `me.checks`/`me.verified`/`me.raw` as diagnostics, and one clean rejection blocks
    adoption;
  - 26: check 2's `recovered` outcome is in the vocabulary.
- **15, answered:** `link_runs=None` is test-only. The build's only caller, `walk`, always passes it.
- **Deferred to Amendment 2.2 on #3609:** 16–19 and 27. They concern Amendment 2's existing check 2 and census, not
  reference eligibility:
  - 16: the comparator's split adjustment against SAB Topic 4C;
  - 17: the single-conflict condition on recovery;
  - 18: a recovered balance-sheet count takes the cover read's acceptance;
  - 19: partial comparator coverage;
  - 27: a removed row's share of final ME is zero by construction.

**Amendment 2.2, checkpoint 1 (12 findings).**
- **Applied to the rule:**
  - 3: no recovery when any comparator agrees with the cover;
  - 9: the co-filed tie-break is the highest accession number.
- **Applied to the text:**
  - 1: acceptance is labelled the stand-in for issuance;
  - 2, 5: the re-filed pre-split count and the co-filed displacement are known limits;
  - 4: rejection blocking is stated as conservative;
  - 6: a cover dated after its filing is stated;
  - 7: the published-rule versus adaptation inventory;
  - 8: the outcome definition;
  - 10–12: the acceptance cases.

**Amendment 2c, checkpoint 1 (26 findings), all applied.** The measurement was rebuilt and re-run after it (m3).
- **Measurement code:** 14, the GP branch aligns XINT* and the quarter start to GP; 15, every part aligned to the
  base interval; 16, a blocked RD* blocks with G&A absent; 17, a blocked `OperatingExpenses` blocks; 18, the bound
  tests only sums the rule makes, stated; 20, refusal, veto and fallback-tag counts and change quantiles; 21, a
  per-period `gp_at` baseline; 22, each rejected branch measured alone on the adopted rule, XOPR in JKP's order.
- **Rule:** 3, software R&D added to the excluding-IPR&D tag; 9, excluding-DDA cost tags read first.
- **Labels and limits:** 1, the XINT zero is adaptation d with its three assumptions; 2, the inclusive R&D fallback a
  proxy; 4, 11, 12, the bound is one-sided and its diagnostic descriptive; 5, 8, the reconstruction and goods or
  services zero (adaptation e) residuals; 6, both XINT fallbacks proxies; 7, conservative cash witnesses; 10, the
  rejection reasons rest on the cited definitions only; 24, the coverage limit names filers with neither COGS* nor GP.
- **Spec text:** 13, the adopted results filled from m3; 19, the bound's evidence recorded in slice 3d-v; 23, the
  mapping table; 25, every read or witness tag's definition in the taxonomy file; 26, hand-computed acceptance cases
  in slice 3d-v.
