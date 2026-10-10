# #3740 slice F — measurement spec: an EDGAR dividend screen against a historical reference

Status: **draft v2, before Codex checkpoint 1 round 8.** v1 (`da2c481b`) drew round 7 (55 findings: 35 BLOCKING,
19 WARNING, 1 NIT; verbatim in `docs/research/3740-slice-f-ckpt1-findings.md` §"Round 7"). v2 **narrows the scope**
to the one test whose truth can be established: a blind, chronological replay of EDGAR over a historical cohort,
scored against an independent reference with every disagreement adjudicated. v1's prospective test (P) is removed:
no independent forward inventory exists, so its "truth" could not certify recall (round 7, 13). Nothing has been
extracted or compared.

Parent: step 3 spec (`docs/research/2026-10-10-3740-step3-vw-book.md`; obligation 9 at lines 500–505; MAX at 218;
sealing invariant 1 at 402–413; returns window at 441–447). Route note:
`docs/research/2026-10-10-3740-slice-f-forward-capture.md` §"Route after the no-account rule".

## Question, and what an answer is

**Question.** Does a frozen extractor reading only EDGAR, in the order filings were accepted, supply each cash
distribution of a historical top-1,000 cohort, with a correct ex-date and amount, by the parent's MAX and
returns deadlines?

**This is a feasibility screen, not a safety bound** (round 7, 2–3). Its bars are fixed by construction (no source
rule sets a dividend-ledger tolerance) and make no claim about the parent book's error. Passing authorises **only**
a slice F v5 design that uses EDGAR as the dividend source, under its own checkpoint 1. v5 must still supply, and this
screen does not: a book-level error bound covering the book's weights, the control draws and both references; the
formation-input treatment of a dividend unavailable at D_MAX; the forward acquisition contract (run times, retries,
bootstrap lookback, completeness); the eToro calendar's role, if any; the parent's security classifier and ME rule
on a monthly, entrant-bearing population; and the parent's full dry run (lines 469–505).

**Outcomes.** `pass` as above. `route-fail` is recorded on #3740 as "this frozen EDGAR route did not demonstrate the
required coverage on this cohort"; whether to try another route or stop #3740 is then a supervisor **resource
decision**, not a fact the screen proves. `inconclusive` follows §"Branches".

**Scope.** Cash distributions on the cohort's common stock. Obligation 6 (raw close contract) and obligation 7
(split inventory) are not measured.

## Reference: Intrader's event columns

**Observed** (2026-10-10, from the mirror files): the mirror `var/research_corpus/mirrors/icyDenev_Intrader` is at
commit `3dbfda5ca1ccecda443fd8979671fcfe47bc2a5c` (2024-12-20). `Data/Day/<symbol>.csv`: 22,879 files, 50,134,060
rows, each 9 comma-separated fields, no header. 460,694 rows have field 8 ≠ 0; 9,354 have field 7 ≠ 1.

**Inferred semantics, from AAPL only** (round 7, 31): field 5 is the nominal close, field 7 a split coefficient on
the split's ex-date (7 on 2014-06-09, 4 on 2020-08-31), field 8 a nominal cash amount per share on the ex-date (3.29
on 2014-05-08, before the 7:1; 0.82 on 2020-05-08; 0.25 on 2024-05-10 and 2024-08-12), field 9 an adjusted close.
Currency, gross-versus-net, same-day aggregation and special-distribution coverage are **not** established. The
screen therefore treats the reference as a **list of candidate events**, not as truth: truth is the adjudicated
value (§"Adjudication"), and every reference event the adjudication cannot confirm is reported as a reference error.

**Lineage** (round 7, 32): no licence, no documentation; our registry records it as a Yahoo derivative
(`research_price_series.upstream_source`). Its independence from EDGAR is a matter of acquisition path (a market-data
scrape, not issuer filings); shared upstream errors are not ruled out, so the screen does not rely on independence:
both-sided misses are what adjudication against the issuer exists to catch, and the remaining blind spot (an event
neither the reference nor EDGAR carries) is stated as a limit.

**Binding** (round 7, 33): the screen reads the mirror's CSV files directly, checks that the mirror's `git rev-parse
HEAD` is the commit above, maps each cohort series to its file through `research_price_series.vendor_symbol`, and
stores each file's sha256. The ingested DB copy is not read.

**Coverage** (round 7, 34): per cohort name, the reference's sessions over the window are compared with
`app/services/market_calendar.py::us_market_status`. A name whose file lacks any session in the window (a series end
or an interior gap) has those sessions marked **uncovered**; reference silence there is not a non-event. Keys with an
ex-date in an uncovered interval are scored only if EDGAR or adjudication supplies them, and the uncovered
name-sessions are reported.

## Cohort and window

- **Cohort.** The stage-B panel artefact
  `factor_panel_3609/2026-10-09-25577cff-stageB-1c5de22cccda4937b55eb7e16e505b7f`, read through
  `scripts.build_3609_factor_panel.read_verified_artefact` with the stage-B pins, as
  `scripts/capture_3609_step2.py::verify_capture` reads it. Formation M = 2023-09-30, admitted rows (`exclusion` null),
  top 1,000 by `me` descending, ties by `name_key` ascending (step 2's universe rule). Fields used: `M`, `exclusion`,
  `me`, `name_key`, `series_id`, `cik`. No return, holding status or characteristic value is read into any figure.
  The cohort is a **pilot**: one formation, not the parent's monthly population, and eToro listing in 2023 is not held
  point-in-time.
- **Security identity** (round 7, 25, 38): one series per CIK in the admitted set (step 1's universe step 5 excludes
  CIKs that map to more than one linked, priced series; `scripts/build_3609_factor_panel.py::multiple_security_ciks`).
  A declaration that names a class other than the common stock (preferred, depositary shares, notes) is out of
  scope. A declaration that names no class is attributed to the cohort's series. A declaration naming two or more
  classes with different amounts is `ambiguous_class` and unresolved.
- **Window.** Ex-dates in sessions 2023-10-02 .. 2024-08-30. Filings replayed: every accession of a cohort CIK accepted
  2023-01-01 00:00 ET .. 2024-12-31 23:59 ET (nine months before the window, four after its end). A true key whose
  adjudicated declaration was accepted before 2023-01-01 counts as `missed` (conservative; round 7, 17).
- **Second cohort, reserved** (round 7, 11): formation M = 2022-09-30 from the same artefact, ex-dates 2022-10-03 ..
  2023-08-31, filings 2022-01-01 .. 2023-12-31. Unread. It is used only if a substantive rerun is needed
  (§"Branches").

## Exposure before freeze

Disclosed (round 7, 48–49): `scripts/probe_3740_slice_f_dividend_reference.py` is the exact read run on 2026-10-10,
an unverified read of the same artefact (no manifest check, no access-register entry) that used only the fields
above. It printed, for the 2023-09-30 cohort over ex-dates 2023-10-01..2024-08-31: 1,000 Intrader series; 2,070
reference events on 612 payers (77.22% of cohort ME); events per payer 1:18, 2:61, 3:222, 4:301, 5:7, 6:2, 13:1; no
negative amount; per-event yield median 0.4935%, 99th percentile 2.4784%, max 25.4127%; 970 series with bars in
2024-09. Those figures informed the window and the bars. **Not seen:** any EDGAR extraction, any match, any
adjudication, anything of the reserved cohort.

## The route under test: frozen extractor, chronological replay

**Replay** (round 7, 1, 18): accessions are processed one at a time in acceptance-time order. The extractor's state
at any instant is a function of the accessions accepted before it; nothing triggers a search of earlier filings, and
no XBRL or reference input is read. Each extracted declaration's **discovery time** is its accession's acceptance
time. The reference is read only after the run's output and its sha256 are written to the run manifest.

**Acquisition.** For each cohort CIK, EDGAR's submissions JSON (with its `files` continuation pages) lists the
accessions; every accession in the replay range is fetched from the EDGAR archive (`/Archives/edgar/data/<cik>/
<accession>/`), every document in its index. HTML and text are read; PDF, images and XBRL-only files are recorded as
`unreadable` with their names. Fetches follow SEC's fair-access limit (≤ 10 requests/s, declared User-Agent), retry
three times with backoff, and a CIK whose listing or any in-range accession is still unfetched after retries makes
the run **incomplete** (an inconclusive prerequisite, §"Branches"). All fetched bytes are retained.

**Extraction contract** (round 7, 19), frozen by the extractor's commit hash before the run:
- *Candidates*: every sentence or table row, in any document, that contains "dividend" or "distribution" and a money
  amount followed within the same sentence or row by "per share" or "per common share".
- *Fields*: amount (USD, as written, kept as a decimal string), and any of declaration, record, ex and payable dates
  in the same sentence or the next sentence of the same paragraph, recognised by their words ("record", "ex-dividend"
  or "ex-date", "payable" or "paid").
- *Excluded*: sentences that are historical (the past tense "paid" with no future payable date, prior-period tables),
  "merger consideration", per-share amounts of preferred or other named classes, stock dividends and splits, and
  negative statements ("suspend", "no dividend", "will not declare"), each kept with its reason.
- *Ambiguous*: two or more amounts or two or more record dates in one candidate that cannot be paired one-to-one →
  `ambiguous`, no declaration. A non-USD amount with no USD equivalent → `unresolved_currency`.
- *Declaration identity* (round 7, 26–27): (CIK, record date, payable date, amount). The same identity in several
  accessions is one declaration (first discovery time kept). A later accession with the same record and payable
  dates and a different amount supersedes the earlier one only if it says "amend", "correct" or "revise"; otherwise
  both are kept and the key is `conflict`. A declaration with no record date and no ex-date is evidence class E3
  (amount only) and yields no key.
- Output is immutable: one JSON line per candidate with accession, document, offset, text, fields, status. Human
  adjudication never edits it.

**Ex-date** of a declaration (round 7, 7, 21–24): an explicitly stated ex-date is used as stated (class **E1**).
Otherwise, from the record date (class **E2**), by FINRA Rule 11140 (the Uniform Practice Code; text fetched
2026-10-10), applied to every venue:

| record date | rule |
|---|---|
| ≤ 2024-05-24, a session | ex-date = the session before the record date (Nasdaq ETA2024-29: "Previously … one business day before the record date") |
| 2024-05-28 | ex-date = 2024-05-24 (ETA2024-29 transition table) |
| ≥ 2024-05-29, a session | ex-date = the record date (Rule 11140(b)(1), effective 2024-05-28) |
| not a session | `unresolved_derivation` (Rule 11140(b)(1)'s non-delivery-date branch is the Committee's designation) |
| amount ≥ 25% of the last close before the declaration's acceptance, or the declaration calls itself a special or liquidating distribution | ex-date = the session after the payable date (Rule 11140(b)(2)); if no payable date, `unresolved_derivation` |

The extractor applies the table mechanically. It does **not** know whether notice was timely (Rule 11140(c)) or how
NYSE designated a date under its own Rule 235, whose text is not pinned. Both are left to the truth, not to the
route: the adjudicated ex-date is the exchange-designated one where evidence of it exists, so a wrong mechanical
derivation scores against the route as a `date_conflict`. The table cannot make the route look better than it is.

**Amount basis** (round 7, 28): the key's amount is the declared amount. If the reference shows a split coefficient
≠ 1 for the name between the declaration's acceptance and the key's ex-date, the key is `basis_review` and its true
amount is set at adjudication.

**Key** (round 7, 27): (series, ex-date). Its route amount is the sum of the distinct declarations (by identity) with
that ex-date. Any key that contains a `conflict`, `ambiguous` or `unresolved_*` declaration has route status
`unresolved` and is never a match.

## Deadlines

Exact functions (round 7, 46), all in America/New_York, sessions and early closes from `us_market_status`:
- **D_MAX(t)** for ex-date t in month m, with s(m) the last session of m: if t < s(m), the close of s(m) (16:00, or
  the early-close time); if t = s(m), 09:30 on the next session. A declaration counts if its acceptance time is
  **strictly before** D_MAX(t). This follows the parent's two binding categories (lines 404–409); the screen
  measures availability only, and the parent's acquisition-versus-binding split is v5's.
- **D_RET(m)**: 23:59:59 on the tenth session after s(m) (parent line 441: "the tenth session after the month's last
  session"; the parent fixes the session, not a clock time, so the end of that day is used and stated as the
  screen's choice).
- **Eventual**: the end of the replay range, reported only.

EDGAR acceptance times are taken from each accession's `ACCEPTANCE-DATETIME` header (Eastern time).

## Adjudication

**Every** key on which route and reference disagree, and every `basis_review` and `unresolved` key, is adjudicated
(round 7, 8; no sampling). Disagreement: present in one and not the other, or amount or ex-date differs.

- **Evidence order** (round 7, 14), per field: the ex-date from an exchange designation where one is found (Nasdaq
  Trader or NYSE notices, issuer statements quoting the exchange), else the issuer's stated ex-date, else the rule
  table; the amount from the issuer's declaration. Sources: EDGAR, and the issuer's own investor-relations pages
  (public, no account). The adjudicator records URL, quote and decision for every key.
- **Outcomes per key**: `true` (with its amount and ex-date) or `not_a_distribution`. A key for which no issuer or
  exchange evidence is found **either way** is `source_absent` and counts against the route only if the reference
  has it (a real event EDGAR does not carry is a route miss; an event nobody documents is unresolved).
- Keys on which route and reference agree are `true` without adjudication; that agreement is not independent proof
  (shared upstream errors), and the limit is stated.
- **Amount equality** (round 7, 29): the reference float is converted with `Decimal(repr(value))`; both amounts are
  compared after quantising to the issuer's stated decimal places with ROUND_HALF_EVEN. The implied tolerance is half
  a unit in the issuer's last decimal, stated as such.

Adjudication is done by the session that runs the screen, after the run manifest is written, under this protocol; it
does not touch the extractor output.

## Scoring

Denominators (round 7, 51): **names** (1,000; payers = names with ≥ 1 true key); **true keys**; **months** (11). Per
key, at D_MAX and at D_RET: `correct` (route amount and ex-date equal truth), `missed` (true, route absent or
unresolved), `amount_conflict`, `date_conflict`, `extra` (route present, adjudicated `not_a_distribution`). Each
true key's **yield** is amount / reference close on the session before its ex-date (the reference's nominal close;
if absent, the last earlier close), computed in Decimal.

Reported, never decisive: evidence class mix (E1/E2/E3), per-name and per-month tables, uncovered sessions,
reference errors by kind, `source_absent` count, eventual availability.

## Branches

Evaluated in this order (round 7, 9, 55); counts are integers, shares exact Decimal, comparisons as written:

1. **Inconclusive** if any: the verified artefact read refuses; the mirror commit or a file hash check fails; the run
   is incomplete (§"Acquisition"); fewer than 1,500 true keys; more than 20 keys remain unadjudicated or
   `source_absent` with the reference having them and no evidence either way.
2. **Route-fail** if any, at D_MAX:
   - any `extra` key;
   - any true key with yield ≥ 1% that is not `correct`;
   - the not-`correct` true keys exceed 1% of true keys by count, or 1% of the summed yield of true keys.
3. **Pass** otherwise.

D_MAX is the binding deadline because it precedes D_RET for every key (round 7, 5); D_RET results are reported.

Why these bars, stated as construction, not derivation: zero `extra` because an invented dividend inflates a return
with no offsetting evidence; a 1% per-event yield line because above it a single error is larger than the window's
99th-percentile distribution; 1% of keys and of yield because the screen is meant to reject a route that needs a
material manual supplement. 1,500 keys is about three-quarters of the reference's 2,070, so a run that loses a
quarter of the reference to coverage or identity failures cannot pass on what remains.

**Reruns** (round 7, 11): an infrastructure retry (fetch failure, crash) re-runs the same frozen version on the same
cohort. A **substantive** change (extractor, rules, scoring) after any output is visible keeps the first result on
record and runs the new version on the reserved 2022-09-30 cohort, once, within 30 days of the first result. An
inconclusive first run gets one infrastructure retry; a second inconclusive is a route-fail.

## Reproducibility

The run writes `var/research/3740_slicef_dividend/<run_id>/` (round 7, 50): the run manifest (extractor commit,
spec sha256, cohort row identities from the verified read, mirror commit and file hashes, all fetched EDGAR bytes with
their URLs, the extractor's output and its hash), then, separately, the adjudication records (key, URL, quote,
decision) and the scored table. DB reads (series-to-file map only) run in one `REPEATABLE READ READ ONLY` transaction
and the rows read are written to the manifest.

## Round 6 and round 7 dispositions

Statuses: **APPLIED** (the text is in this spec), **MOVED TO v5** (removed from this screen's scope and listed above
as a v5 requirement), **LAPSED** (concerns text no longer present), **DEFERRED** (not done; where it is tracked).

Round 6: 1 APPLIED (explicit columns; semantics narrowed per round 7, 31) · 2 APPLIED (reference treated as
candidates; lineage qualified) · 3 APPLIED (§"Exposure before freeze"; extractor commit before run) · 4 APPLIED as a
labelled pilot; parent population MOVED TO v5 · 5 LAPSED (prospective test removed) · 6 APPLIED · 7 APPLIED (rule
table; NYSE and late notice left to truth) · 8 APPLIED (D_MAX, D_RET) · 9 APPLIED (blind chronological replay; the
oracle-guided test is removed) · 10 APPLIED (§"Key", identity, basis) · 11 APPLIED (two-way, all adjudicated) · 12
LAPSED (no combined policy) · 13 APPLIED as construction, no safety claim; book bound MOVED TO v5 · 14 APPLIED
(pilot, limits stated) · 15 APPLIED · 16 APPLIED · 17 APPLIED · 18–19 LAPSED (calendar removed) · 20 LAPSED · 21–22
apply to the route note and stand there · 23 APPLIED · 24, 26 DEFERRED to the route-note probe's next run (they do
not affect this screen) · 25 APPLIED (this table) · 27 APPLIED in the route note.

Round 7: 1 APPLIED · 2 APPLIED (performance calibration removed) · 3 APPLIED as screen; bound MOVED TO v5 · 4
APPLIED (§"Scoring"; no weighted budget) · 5 APPLIED (D_MAX binding); formation treatment MOVED TO v5 · 6 APPLIED
(unresolved is never correct) · 7 APPLIED (nine-month condition removed) · 8 APPLIED · 9 APPLIED · 10 LAPSED · 11
APPLIED · 12 LAPSED · 13 APPLIED (P removed) · 14 APPLIED · 15 LAPSED (detectors removed) · 16 LAPSED · 17 APPLIED
(nine-month lookback; earlier declarations count as missed) · 18 APPLIED · 19 APPLIED · 20 LAPSED (H1 removed) · 21
APPLIED (route applies one table; truth uses designations) · 22 APPLIED (threshold is the route's mechanical rule;
truth decides) · 23 APPLIED (left to truth) · 24 APPLIED · 25 APPLIED · 26 APPLIED · 27 APPLIED · 28 APPLIED
(`basis_review`) · 29 APPLIED · 30 APPLIED (merger consideration excluded; in-kind is not cash and is out of scope;
liquidating cash distributions in scope) · 31 APPLIED · 32 APPLIED · 33 APPLIED · 34 APPLIED · 35–37 MOVED TO v5 · 38
APPLIED · 39 APPLIED (sizes stated as expectations; minimum key count) · 40–44 LAPSED (calendar and prospective
acquisition removed; forward acquisition MOVED TO v5) · 45 MOVED TO v5 · 46 APPLIED · 47 APPLIED (operational
readiness MOVED TO v5) · 48 APPLIED · 49 APPLIED (probe committed and disclosed) · 50 APPLIED · 51 APPLIED · 52 LAPSED
(no sampling) · 53 DEFERRED (route-note probe) · 54 APPLIED · 55 APPLIED.
