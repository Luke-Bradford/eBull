# #3740 slice F — measurement spec: can an account-free route supply dividends at the ex-date?

Status: **draft v1, before Codex checkpoint 1 (round 7).** Nothing has been compared. Parent: step 3 spec
(`docs/research/2026-10-10-3740-step3-vw-book.md`, obligation 9 at lines 500–505; MAX at line 218; sealing invariant 1
at lines 402–413; returns window at lines 441–447). Route note: `docs/research/2026-10-10-3740-slice-f-forward-capture.md`
§"Route after the no-account rule". Round 6 findings: `docs/research/3740-slice-f-ckpt1-findings.md` §"Round 6".

## Question and what an answer authorises

**Question.** Does a frozen route built only on account-free sources (#3740, 2026-10-10 16:21Z) supply the cash
dividends that parent obligation 9 (holding-month total returns, dividends at the ex-date) and the MAX filter's daily
total returns need, correct and by each input's deadline, on the parent's kind of population?

**Scope.** Cash distributions only. Obligation 6 (raw close capture contract) and obligation 7 (dated split inventory)
are not measured here and keep their own evidence requirements (round 5, 22–24).

**What each outcome authorises** (round 6, 15–17):
- **pass** → a slice F v5 design that uses the passing route, with its own checkpoint 1. Nothing else: no source is
  accepted for the capture, no accrual starts, and the parent's dry-run and closure requirements (lines 469–505) are
  unchanged. Passing does not validate MAX inputs, splits, terminations or daily returns.
- **route-fail** → recorded on #3740 as "this frozen route did not demonstrate the required coverage". Whether to try
  another route or stop #3740 is then a supervisor **resource decision**, stated as such. It is not evidence that no
  account-free source exists (round 6, 16).
- **inconclusive** → one bounded extension (§"Branches"), then pass or route-fail.

## What changed since round 6: the reference exists

The route note left open whether Intrader's stamps are a dated distribution inventory with amounts (round 6, 1–2).
**They are.** Measured 2026-10-10 on the mirror and the dev DB:

- The mirror `var/research_corpus/mirrors/icyDenev_Intrader` is at commit `3dbfda5ca1ccecda443fd8979671fcfe47bc2a5c`
  (committed 2024-12-20). `Data/Day/*.csv`: 22,879 files, 50,134,060 rows, every row 9 comma-separated fields, no
  header: date, open, high, low, close, volume, **split coefficient**, **cash dividend per share**, adjusted close.
- Column semantics, checked on AAPL against its published actions: split coefficient 7 on 2014-06-09 and 4 on
  2020-08-31 (the two splits' ex-dates); dividend 3.29 on 2014-05-08, 0.82 on 2020-05-08, 0.25 on 2024-05-10 and
  2024-08-12 (ex-dates). Close is nominal (500.04 on 2020-08-27, before the 4:1), and the dividend amount is nominal
  per share on the ex-date's basis (3.29 before the 7:1, not 0.47). The adjusted close is separate.
- Across all files: 460,694 rows with a non-zero dividend, 9,354 with a split coefficient ≠ 1, 0 malformed rows.
  10,411 files end in 2024-09 (AAPL's last bar is 2024-09-27).
- The ingest already stores both fields: `research_price_daily.dividend` and `.split_factor`, for 22,879 series with
  `research_price_series.vendor = 'icyDenev/Intrader'`, `corporate_action_stamps = 'vendor_supplied'`,
  `adjustment_basis = 'unadjusted'`, `upstream_source = 'yahoo_derivative'`, `licence = 'other/unspecified'`.

So the reference is an explicit event dataset, not reverse engineering of an adjusted series (round 6, 1). It is
**historical only** (it ends 2024-09-27), so it serves the historical tests and cannot serve a prospective one.

**Reference contract** (round 6, 2). Artefact: the DB rows above, bound to the mirror commit by the measurement's
manifest (row identities and a digest of `(series_id, bar_date, dividend, split_factor, close)` for the cohort).
Provenance: a public GitHub repository with no licence and no documentation (README is the project name only); the
registry records it as a Yahoo derivative. Access: no account, no terms accepted. Fields: ex-date, nominal amount;
**no** record or payment date, currency, event type (regular/special) or distribution count per day. Known and
possible omissions, all scored rather than assumed: same-day distributions may be summed; non-USD amounts may be
converted; specials and return-of-capital distributions may be missing or merged. Independence: its upstream is a
market-data vendor, not issuer filings and not eToro, so it is independent of both candidates. **It is not an
authority:** every disagreement is adjudicated against the issuer's filing (§"Reconciliation").

## Event contract (round 6, 10)

- **Security.** The cohort's own series (one per `name_key`), linked to its CIK as of the cohort date. An event on
  another class of the same issuer is out of scope; an issuer filing that does not name the class is resolved to the
  cohort's class only when the issuer has one listed common class in EDGAR's submissions `tickers`/`exchanges`,
  otherwise `unresolved_class`.
- **Scope.** Cash distributions on the common stock: regular, special, and return-of-capital alike (all enter a gross
  total return). Out of scope, each recorded with its type: stock dividends and splits (obligation 7), rights,
  in-kind/spin-off distributions (`unresolved_inkind`; the parent's step 1 status rules apply to them, not this
  measurement).
- **Amount.** USD gross per share, before withholding (step 0's research convention), on the nominal basis of the
  ex-date. A distribution declared in another currency with no USD amount in the filing is `unresolved_currency`,
  never a match.
- **Key.** `(name_key, ex_date)`. Matching compares the **sum** of in-scope cash amounts per key, because the
  reference cannot separate same-day distributions; component amounts are kept, and a sum match whose components
  the candidate cannot reconcile is reported.
- **Equality.** Amounts normalised to 6 decimal places (the reference stores floats; issuer amounts carry ≤ 6
  decimals in practice, and a filing with more is kept at its own precision and compared at 6). Match = equal at 6
  dp. No other tolerance: a rounding difference is a conflict and goes to adjudication.

## Ex-date rule table (round 6, 7)

A filing that states the ex-date gives an **explicit** date (class E1). A filing with a record date only gives a
**derived** date (class E2) by this table, keyed by the listing venue (EDGAR submissions `exchanges`, as of the
filing) and record date. Explicit, derived and unresolved dates are reported separately.

| branch | rule | source |
|---|---|---|
| distribution < 25% of the security's value, record date ≥ 2024-05-29 | ex-date = record date (business day); the business day before it if the record date is a non-delivery date | FINRA Rule 11140(b)(1), effective 2024-05-28 (SR-FINRA-2023-017); Nasdaq notice ETA2024-29 |
| distribution < 25%, record date ≤ 2024-05-24 | ex-date = one business day before the record date | Nasdaq notice ETA2024-29 ("Previously … one business day before the record date") |
| record date 2024-05-28 | ex-date 2024-05-24 | Nasdaq notice ETA2024-29 transition table (no security went ex on 2024-05-28) |
| distribution ≥ 25% of value | ex-date = first business day after the payable date | FINRA Rule 11140(b)(2) |
| late definitive information | ex-date set by the exchange; **not derivable** → `unresolved_late` unless a filing or exchange notice states it | FINRA Rule 11140(c) |
| NYSE / NYSE American listings | the same branches **only after** the measurement script pins NYSE Rule 235's text; until then NYSE rule-derived dates are reported as `derived_unpinned`, not scored as E2 | NYSE Rule 235 (text not yet fetched; the rules site served navigation only on 2026-10-10) |

"Value" for the 25% branch is the cash amount over the close before the ex-date candidate; a distribution within
20–30% of value is `unresolved_size` unless explicit, because the exchange's own valuation is not observable.

## Deadlines (round 6, 8)

Each event is scored at the deadline of the input that uses it, from the parent:
- **D_RET(m)**: holding-month returns of month m use evidence available at the cutoff, the tenth session after m's
  last session (parent lines 441–442).
- **D_MAX(t)**: the MAX value at formation M uses the daily total returns of M's sessions up to s(M). A dividend
  with ex-date t < s(M) must be bound before the close of s(M); for t = s(M), within the post-close window ending
  before the next session's open (parent lines 402–409). So D_MAX(t) = close of s(M(t)), or the next open if t = s(M).

For every event the measurement records: the source's publication or acceptance time; first capture time (prospective
only); usable-by-D_MAX; usable-by-D_RET; and the final adjudicated value. EDGAR availability is the accession's
**acceptance time** (a knowledge timestamp; parent line 412), so a historical run reproduces what was knowable at each
deadline.

## Tests

### H1 — historical source-content feasibility (oracle-guided; diagnostic only)

Labelled as oracle-guided (round 6, 9): it uses the reference's ex-dates to choose where to look, so it measures
whether EDGAR **contains** the evidence, not whether a process finds it.

- **Cohort C_H (pilot, one formation; round 6, 4).** The stage-B panel artefact
  `factor_panel_3609/2026-10-09-25577cff-stageB-1c5de22cccda4937b55eb7e16e505b7f`, read through
  `read_verified_artefact` with the stage-B pins (as `scripts/capture_3609_step2.py::verify_capture` does), formation
  M = 2023-09-30, admitted rows, top 1,000 by ME descending, ties by `name_key` ascending (step 2's universe rule).
  The read uses only `M`, `exclusion`, `me`, `name_key`, `series_id` and `cik`; no return, holding status or
  characteristic value enters any figure (the `measure_3609_step2_universe` precedent). This is **not** the parent's
  population: eToro listing and tradability in 2023 are not stored point-in-time, and it is one formation, not a
  monthly population. Entrants, carried holdings and control draws are validated by test P and by v5's dry run.
- **Planning count** (an unverified read of the same rows, 2026-10-10, reproduced by the measurement under the
  verified read): all 1,000 series are Intrader; over ex-dates 2023-10-01..2024-08-31 the reference holds 2,070
  events on 612 payers, 77.22% of cohort ME; events per payer 1:18, 2:61, 3:222, 4:301, 5:7, 6:2, 13:1; no negative
  amount; per-event yield (amount / close on the ex-date) median 0.49%, 99th percentile 2.48%, max 25.4%; 970 series
  have bars into 2024-09.
- **Window W_H.** Ex-dates in 2023-10-02..2024-08-30. Eleven months: every quarterly payer appears at least three
  times and the window crosses the T+1 transition, so both rule-table branches are exercised; it ends four weeks
  before the reference's last bar so no in-window event is censored by the reference's end. Annual payers may fall
  outside it; that is a stated coverage limit, carried to test P.
- **Search.** For each reference event: every filing of the CIK accepted in [ex − 120 days, D_RET(month of ex)], all
  forms, all documents and exhibits (8-K of any Item, 10-Q, 10-K, 6-K, 20-F, 40-F, DEF 14A, 424B*), through the same
  frozen extractor that H2 and P use. Classes: E1 explicit ex-date and amount; E2 record date and amount, derived by
  the table; E3 amount only; E0 nothing. Reported per class, by count and by cohort ME weight, at D_MAX and D_RET.

### H2 — historical blind discovery (decides the EDGAR route)

- **Process (round 6, 9).** The frozen extractor runs over **every** filing of every C_H CIK accepted from
  2023-06-01 to D_RET(2024-08), without reading the reference. It emits in-scope cash distributions with their
  evidence class, ex-date, amount and acceptance time. Reference answers are joined only after the run's output is
  hashed into the manifest.
- **Extractor contract.** Frozen by version hash before the run: document text from EDGAR's archived accession files;
  amount, record, ex and payable dates parsed from the declaring sentence or table; issuer-level XBRL
  `CommonStockDividendsPerShareDeclared` used only as a **completeness detector** (a period with a positive value and
  no extracted event triggers a re-search of that period's filings, logged), never as an amount. Every rejected
  candidate sentence is kept with its reason (round 6, 19 by analogy).
- **Scoring.** Two-way reconciliation over the whole cohort (§"Reconciliation"), at D_MAX and D_RET.

### P — prospective common comparison (decides the eToro calendar's role and confirms H2 forward)

Every candidate is scored on one frozen population and window at the same deadlines (round 6, 5, 8).

- **Cohort C_P (pilot).** Frozen at the close of 2026-10-30: eToro instruments whose `instruments.instrument_type_id`
  is eToro's stocks type and whose `instruments.exchange` is a US exchange, with an `instrument_universe_membership`
  row open on that date (`effective_from` ≤ date, `effective_to` null or later) and `is_tradable`, linked to a CIK
  with a 10-K or 10-Q, top 1,000 by eToro close × the latest dei `EntityCommonStockSharesOutstanding` cover count
  accepted before 2026-10-30. This approximates the parent's
  ME (step 1 §"Market equity" needs slice F's own capture) and is labelled so; the membership file is written once
  and its digest recorded before the window opens.
- **Window W_P.** Ex-dates 2026-11-02..2027-01-29: one quarter, so every quarterly payer appears once. Observation
  tail: captures continue to D_RET(2027-01); adjudication closes 2027-03-31.
- **Candidates.**
  1. EDGAR: H2's frozen extractor, run daily over filings accepted since the last run.
  2. eToro's public dividend calendar (`etoro.com/investing/dividend-calendar/`), captured as below.
- **Combined policy (round 6, 12), frozen now.** EDGAR is the only amount and date source. The calendar is a
  **detector**: a calendar row with no matching EDGAR event by the deadline makes that holding's input incomplete
  (the parent's `INPUT_UNAVAILABLE` path, completed later within its nine-month limit) and triggers a logged re-search;
  it never supplies an amount or date. EDGAR revisions: the latest accession accepted before the deadline wins;
  an accession that withdraws a declaration removes it; two un-withdrawn declarations for one key with different
  amounts refuse (`unresolved_conflict`). An oracle union of the two candidates is reported as a diagnostic only.
- **Calendar capture (round 6, 18–19).** Two captures per US business day, 13:00 and 22:30 UTC, each with three
  retries ten minutes apart; raw bytes, headers, URL and fetch time stored. A capture succeeds only if the expected
  table is found **by its header names** (symbol, ex-date, payment date, amounts) and ≥ 1 row parses; every row's
  dates and amounts are validated and rejected rows are kept with reasons; a changed header set or a row count below
  half the previous successful capture's fails the capture (`completeness_unknown`). A day with no successful capture
  is an outage, recorded, never read as "no events". Calendar symbols join to eToro instruments through
  `instruments.symbol` and then to the cohort; an unmatched symbol is kept as `unresolved_identity` (round 6, 20).
- **Reference for P.** None independent exists forward. The scoring truth is built by the rules frozen here and
  frozen itself only after the adjudication deadline (round 6, 5): the union of both candidates' events, plus every
  key raised by two detectors (XBRL `dps_declared` for a period covering W_P with no event; and every C_P name that
  paid a regular dividend in each of its last four quarters in our stored history with none in W_P), each
  adjudicated against the issuer's filings and press releases on EDGAR.

## Reconciliation (round 6, 11)

Per key, after adjudication: `matched`, `amount_conflict`, `date_conflict` (same distribution, different ex-date),
`missed` (truth has it, candidate does not), `extra` (candidate has it, truth does not), `duplicate`, and the
`unresolved_*` statuses. Every non-matched key is adjudicated against the issuer's filing in P and in H2; in H2, if
more than 300 keys need it, a seeded random sample of 300 (seed and the full list frozen in the manifest) is
adjudicated and the rest are reported by their unadjudicated status, labelled. Adjudication can find the
**reference** wrong; reference errors are counted and the adjudicated value is the truth. Non-payers are in the
denominator, so a candidate's invented dividends on a non-payer score as `extra`.

## Error budget and branches (round 6, 13–15)

**Budget, in the book's terms.** For month m, the cap-weighted dividend error over the cohort:
E_m = Σ_i w_i · |d_i(route) − d_i(truth)| / P_i, where w_i is i's ME share of the cohort, P_i the close before the
ex-date, and the sum runs over every key whose route value at D_RET(m) differs from truth (a `missed` key counts its
full amount; an `extra` key its full amount; an unresolved key that the policy makes incomplete counts zero here and
is counted below). No source rule fixes this tolerance, so it is fixed **by construction** and frozen in the
measurement's version hash: Σ_m E_m over the window, annualised, **≤ 2 bp**. Scale: two orders below the 3.11 pp
G gap step 2 recorded on stage B (7.43% against 10.54%, #3609 2026-10-08), so ledger error at the bar cannot decide
condition 4. The book holds a subset of the cohort with larger weights than w_i; the v5 design must bound that
amplification, so the measurement also reports the largest single-key error as a yield (|Δd| / P), and the per-name
completeness and amount distribution (round 6, 13).

**Incompletes.** A key the policy leaves incomplete at D_RET is not an error but delays a month; it is permanently
fatal only if it is still unresolved at the parent's nine-month limit. Reported: count and cohort weight of incomplete
keys at D_RET, and of keys unresolved at D_RET + 9 months (historical) or at the adjudication deadline (P).

**Branches** (exact integer counts; no rounded percentages decide anything):

| branch | H2 (EDGAR route) | P (combined policy) |
|---|---|---|
| **inconclusive** | the verified artefact read refuses; the extractor run does not complete over every CIK; or more than 50 keys remain unadjudicated after the sample | fewer than 150 truth events in W_P; more than 5 outage days; adjudication unfinished at 2027-03-31; or the calendar page's structure changes so that more than 10 business days are `completeness_unknown` |
| **route-fail** | annualised Σ E_m > 2 bp; or any key with \|Δd\| / P ≥ 1% that is `missed`, `extra` or a conflict; or any key unresolved at D_RET + 9 months | the same three conditions on W_P at the policy's deadlines |
| **pass** | none of the above | none of the above |

The single extension: an inconclusive P extends W_P by one further quarter (ex-dates to 2027-04-30, adjudication to
2027-06-30) under the same frozen rules; an inconclusive H2 is re-run once after the stated cause is fixed, with the
fix recorded. A second inconclusive is a route-fail.

**Why these sizes** (round 6, 14). H2 is the decisive test because EDGAR's acceptance times make history
point-in-time: eleven months, 1,000 names, 2,070 reference events. P adds what history cannot: the calendar (no
archive exists), forward entrants and the current extractor against live filings. One quarter is a time-budgeted
pilot, not a population guarantee; 150 events is below the cohort's expected quarter (2,070 events over eleven
months on the historical cohort) so that a pass is not decided on a thin sample. Neither test removes v5's need for
ongoing reconciliation and refusal rules: the parent's dry run and the capture's own refusals stay required.

## Order and outcomes

1. This spec passes checkpoint 1; its frozen elements (cohort rules, windows, rule table, extractor contract,
   policy, budget, branches) are hashed into a version on the PR before any extraction runs.
2. The extractor is built and its version hash recorded. H1 and H2 run; the reference is joined after H2's output is
   hashed.
3. P's cohort freezes at the close of 2026-10-30 and its capture starts on 2026-11-02, whatever H2 shows, so that P
   is not chosen after seeing H2. **If 2026-10-30 has passed before step 1 closes**, P's dates move by whole months
   (cohort at the last session of the month before the capture starts, window three calendar months), recorded
   before the cohort is drawn.
4. Results: H2 on #3740 when it completes; P after its adjudication deadline. Pass on H2 alone authorises the v5
   design review to proceed on the EDGAR route **in parallel** with P, and v5's own dry run carries P's result as a
   precondition; a P route-fail after an H2 pass stops v5 at that precondition.

## Reproducibility (round 6, 23–24, 26)

The measurement script runs its DB reads in one `REPEATABLE READ READ ONLY` transaction, records the transaction
snapshot and timestamp, and writes a result artefact holding parameters, every contributing row identity and value
(cohort, reference events, extracted events, adjudications) and the digests of all fetched EDGAR documents and calendar
captures. The route note's account-free probe gets the same treatment when it is next run: the PWB README fetched at
the immutable revision (`/resolve/<sha>/README.md`) with its full access text saved, card claims kept apart from
observed download behaviour, and its labels corrected (the public-calendar GET; the fiscal predicate as printed).

## Round 6 dispositions

| finding | disposition |
|---|---|
| 1 adjusted series is not an inventory | APPLIED: the reference is Intrader's explicit dividend and split columns, verified on AAPL's dated actions (§"What changed since round 6") |
| 2 reference independence and access | APPLIED: §"Reference contract" freezes artefact, provenance, access, fields, omissions; independence argued by upstream; adjudication against filings |
| 3 frozen measurement | APPLIED: concrete cohort dates, windows, manifests; version hash before extraction (§"Order and outcomes") |
| 4 parent population | APPLIED as pilot cohorts, labelled, with the parent's listing and tradability in C_P; entrants, carried names and controls left to P and v5's dry run |
| 5 shared denominator | APPLIED: historical (H1, H2) and prospective (P) separated; P's truth-construction rules frozen now, its inventory frozen after adjudication |
| 6 EDGAR false negatives | APPLIED: all forms and exhibits; E1 explicit and E2 derived scored separately |
| 7 ex-date rules | APPLIED: venue/version table with sources; NYSE branch held as `derived_unpinned` until its text is pinned |
| 8 deadlines | APPLIED: D_MAX and D_RET from the parent; acceptance, capture, usable-by-deadline and final agreement reported separately |
| 9 oracle-guided vs discovery | APPLIED: H1 labelled oracle-guided and diagnostic; H2 and P are blind |
| 10 event contract | APPLIED: §"Event contract" |
| 11 two-way reconciliation | APPLIED: §"Reconciliation", non-payers in the denominator |
| 12 combined policy | APPLIED: EDGAR sole amount source, calendar as detector, revision and conflict rules |
| 13 threshold basis | APPLIED: budget in cap-weighted return terms, fixed by construction and stated as such, plus per-key bar and per-name reporting |
| 14 sample adequacy | APPLIED: sizes reasoned; P labelled a pilot; ongoing reconciliation kept for v5 |
| 15 branches | APPLIED: §"Branches" |
| 16 failure overgeneralised | APPLIED: route-fail wording and resource decision |
| 17 success scope | APPLIED: §"What each outcome authorises" |
| 18 capture spec | APPLIED for the calendar (schedule, retries, outages, tail) |
| 19 calendar parser | APPLIED: header-targeted, validated, rejects kept, completeness failure |
| 20 suffix-free classifier | APPLIED: identity join through eToro instruments; the route note's wording is already "suffix-free symbols" |
| 21 snapshot vs policy | ALREADY APPLIED in the route note ("in this snapshot") |
| 22 split wording | ALREADY APPLIED in the route note ("compatible with split adjustment") |
| 23 DB reproducibility | APPLIED as a requirement on the measurement script (§"Reproducibility") |
| 24 PWB reproduction | APPLIED as a requirement on the probe's next run; PWB is excluded by the rule, so nothing here depends on it |
| 25 checkpoint log overstated | APPLIED: the route note's checkpoint log points at this table for per-finding dispositions |
| 26 stale probe labels | carried into §"Reproducibility" |
| 27 round-5 totals | ALREADY APPLIED: the route note's checkpoint log reads 14 BLOCKING, 11 WARNING |
