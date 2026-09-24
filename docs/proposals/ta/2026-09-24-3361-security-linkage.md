# #3361 — dated series ↔ CIK linkage with an abstention census

Step 2 of `docs/proposals/ta/2026-09-24-selection-programme-v2.md` ("Security linkage"). Refs #2899, #2721,
#3360 (the fundamentals bundle this joins to), #3362 (termination; consumes the link end dates).

## Contract (deliberately narrow)
The artefact answers one question:

> For research price series S and decision session D: which SEC entity (CIK) was S's issuer, **judged only from
> evidence public strictly before D**, or a typed abstention.

It links a **series** (one vendor file = one security as the vendor keyed it) to an **entity** (a CIK). It does
not pick a primary share class, merge series, split a series at an identity change, or decide what a
succession means economically. Those are each arm's declared construction (#2901 first) or #3362's.

**Not here:** CUSIP/security master, share-class resolution beyond the vendor's own symbol grammar, ETFs and
funds (no Section 16 issuer), foreign private issuers (exempt from Section 16 by Rule 3a12-3(b), so no Form 3/4/5 evidence), and
fundamentals (#3360).

## Why causal, and why it matters here
A link at D that uses evidence filed after D conditions on the issuer still existing after D, i.e. on
survival. The programme forbids exactly that ("the universe never depends on eventual survival or future
mapping success"). So the reader is causal: every rule below uses only evidence with filing date < D, the
same public rule as #3360 rule 7. The price of this is abstention near an identity change, and the census
measures it.

## Source rules
- **Primary evidence: SEC Insider Transactions Data Sets** (`<YYYY>q<N>_form345.zip`,
  `app/services/sec_bulk_download.py`, first quarter `2006q1`), `SUBMISSION.tsv`: `ACCESSION_NUMBER`,
  `FILING_DATE`, `DOCUMENT_TYPE`, `ISSUERCIK`, `ISSUERTRADINGSYMBOL`. Exchange Act §16(a) requires Forms
  3/4/5 from the insiders of every issuer with an equity class registered under §12; Rule 3a12-3(b) (17 CFR
  240.3a12-3, read 2026-09-24 via LII) exempts foreign private issuers. The population is therefore US-registered
  §12 issuers. (Neither citation is in sec-edgar skill §2.3 yet; `.claude/**` is not writable from the loop
  worktree, so the skill text is parked on #2403.) It is independent of our
  broker list and of eventual survival. `issuerTradingSymbol` is **filer-entered and issuer-level** (the X0306
  Form 4 schema carries one symbol per filing, not per class; sec-edgar skill §2.3). Treat it as
  noisy: it is evidence, never a label.
- **Termination evidence: `sec_form25_register`** (sec-edgar skill §2.6), `provision_class =
  'equity_delisting'`: `issuer_cik`, `resolved_symbol` (pre-delisting cover-page `dei:TradingSymbol`, trap 4),
  `filed_date`, `security_class`. Span 2013-01-02 onward (#3360 census). Form 25 is per-security (trap 2), so it
  ends a link only when its `resolved_symbol` matches the series (rule 5).
- **Symbol grammar of the vendor series** (Intrader): a trailing `_<CLASS>` group marks non-common or
  class-specific lines (`_P…` preferred, `_WS`/`_W` warrants, `_U` units, `_R` rights, `_WD` when-distributed,
  `_CL` called, `_A`/`_B`… share classes). Measured below. This is the vendor's grammar, not an SEC rule;
  #2912 already excludes exchange test issues by identity.
- **Not evidence (deliberately):** `submissions.zip` `tickers` and `company_tickers*.json` (today's
  mapping, drops delisted names: sec-edgar trap 4), eToro `instrument_id` / `instrument_cik_history`
  (today's broker universe), and `sec_cover_12b_pairs` (a def14a-driven subset of about 1,000 covers, so
  population-dependent). The eToro mapping is used only as a **cross-check** in acceptance item 3.
- **Prior-art shape** (not a rule for our data): CRSP/Compustat's CCM link table uses dated links
  (`LINKDT`–`LINKENDDT`) with a link-type code instead of one static id. Same shape here, with the dates and
  reasons derived causally.

## Premise measurements
`PYTHONPATH=. uv run python scripts/measure_3361_symbol_evidence.py` (2026-09-24; 81 quarters `2006q1`–`2026q1`,
4,402,307 `SUBMISSION` rows, 27,389 distinct normalised symbols):
- 22,879 Intrader series; `research_price_series.cik` is NULL on all of them. Suffix grammar: 19,994 plain;
  1,324 `P`, 579 `WS`, 351 `U`, 288 `W`, 113 `CL`, 111 `R`, 17 `WD`, the rest single-letter class groups.
- Plain series vs Form 3/4/5 symbol evidence inside the series' own bar range: **8,170 one CIK**, **790 several
  CIKs**, 1,553 symbol seen only outside the range, 9,481 symbol never seen. Suffixed: 65 / 0 / 26 / 2,794.
- Of the 790 with several CIKs, **267 are a clean succession** (every observation of one CIK precedes every
  observation of the next: ticker reuse or successor, e.g. `ACET` CIK 2034 through 2019-02, CIK 1720580 from
  2020-09). **523 overlap.** Examples: `AB` (AllianceBernstein Holding and the unlisted LP both cite `AB`),
  and `ACM` (one stray 2023 filing under another CIK). Overlap is the noise class that rule 4 abstains on.
- "Symbol never seen" is not a failure count. It contains funds, ETFs and foreign private issuers, which have
  no Section 16 issuer by construction. The census splits it.

## Construction rules
1. **Inputs, snapshotted and hashed** (as #3360): the Form 3/4/5 archives (each quarter's zip), the sorted
   `sec_form25_register` equity-delisting rows read, and the sorted `research_price_series` rows read
   (`series_id, vendor, vendor_symbol, first_bar, last_bar`). The vendors are the Intrader corpus plus any
   other `yahoo_derivative` vendor present; each series is linked independently.
2. **Normalisation.** Symbol = upper-case, trimmed, the separators `. - / _ space` unified. An evidence symbol
   matches a series if equal after normalisation, or equal after stripping one trailing `Q` from the evidence
   symbol (bankruptcy OTC suffix: research-price-corpus skill §bankruptcy suffix). Empty, `NONE`, `N/A`, `NA`
   → no observation (counted). CIK zero-padded to 10 (sec-edgar trap 7).
3. **Series eligibility.** A series whose vendor symbol carries a non-common suffix class (`P`, `WS`, `W`,
   `U`, `R`, `WD`, `CL`) is **abstained whole** as `non_common_symbol_form:<class>`. A single-letter class group
   (`BF_B`) is eligible, but matches only evidence carrying the same class (`BF.B`, `BF-B`, `BFB` is **not** a
   match), never the base symbol. Exchange test issues are abstained as `vendor_test_symbol` via `universe_selection.EXCHANGE_TEST_ISSUE_SYMBOLS` (the #2912 rule, research-price-corpus skill §Exchange test issues).
4. **Observation** = (normalised symbol, CIK, filing date, accession, source ∈ {`form345`}). Public at D iff
   filing date < D. **Link at D** for series S, using S's matching observations with filing date in
   `[max(first_bar, D − 730 days), D)` (the #3360 census population window) — call this window W:
   - no observation in W → `no_recent_evidence` (sub-reason `never_seen` if the symbol has no observation
     at all before D);
   - one CIK in W → candidate that CIK;
   - several CIKs in W: if they form a **clean succession** (for CIKs ordered by first observation in W, every
     observation of each CIK precedes every observation of the next), candidate = the last one; otherwise
     **abstain `conflicting_evidence`**. No majority, count threshold or recency weighting: any interleaving
     abstains.
5. **Termination.** A candidate C is refused as `terminated_by_form25` if a `sec_form25_register`
   equity-delisting row with `issuer_cik = C` and `resolved_symbol` matching S (rule 2) was filed after C's
   latest observation in W and before D. A Form 25 for C with NULL `resolved_symbol` does not end the link.
   It is carried as the flag `form25_unattributed`, and the census counts it.
6. **Bar presence.** D outside `[first_bar, last_bar]` → `outside_series`. This is evaluated first; it is a
   property of the series, not an abstention.
7. **Result algebra** (exactly one): `outside_series`, `non_common_symbol_form:<class>`, `vendor_test_symbol`,
   `no_recent_evidence[:never_seen]`, `conflicting_evidence`, `terminated_by_form25`, `linked(cik, basis)`,
   where `basis` ∈ {`single_cik`, `succession`} and carries the supporting accessions (first and last in W) and
   the flags `form25_unattributed` and `shared_cik`. `shared_cik` = another eligible series is `linked` to the
   same CIK at the same D (classes, or a vendor duplicate). It is informational only; nothing is dropped.
8. **Reader** `link_as_of(series_id, D)` returns the rule-7 result. The artefact also provides **per-series
   interval compression** (maximal runs of identical results across trading days), so an arm can see the
   dates a link starts, switches and ends. Those intervals are derived from the reader, never the other way
   round.

## Artefact
Content-addressed like #3360 (`r6_pit_bundle.read_verified_document`, exclusive publish, policy-bound loader):
`<bundle>/inputs/` (snapshotted archives and row dumps), `<bundle>/observations/<normalised symbol>.json`
(sorted observations), `<bundle>/series.json`, `<bundle>/manifest.json` (policy sha over constants, builder,
reader, Python version; input digests; the ledger). The ledger reconciles `SUBMISSION` rows in = observations
stored + `empty_symbol` + `unparsed_date` + `integrity_excluded`. A duplicate accession with a conflicting
(issuer CIK, symbol) excludes that accession (`integrity_excluded`).

## Census (descriptive; before any strategy look)
- **Formations:** the #3360 grid (last NYSE session of each June, 2011–2024).
- **Population at D:** series with a bar **on D** (vendor-independent of linkage), grouped as eligible or
  abstained whole (rule 3).
- **Per formation:** result counts by reason (rule 7); by **size decile** on the series' own traded value
  (median close × volume over the 21 bars before D; census-only, stated as such; `size_unavailable` where
  volume is missing); and by **vendor outcome** (series still running at the vendor's capture end, or ending
  before it; with and without a `research_price_series.delisting_provision`). This is year × reason × size ×
  outcome, as the issue asks.
- **Join to #3360:** linked CIKs at D that are in the #3360 census population (submissions-derived) vs not;
  #3360 census members at D with no linked series (the other direction of the join).
- `shared_cik` count; `form25_unattributed` count; series whose link switches CIK at least once (succession)
  over their lifetime.

## Acceptance (this ticket)
1. **Pure fixtures:** single CIK; clean succession (reuse); interleaved stray filing → `conflicting_evidence`;
   evidence dated D is not public at D; the 730-day window drops stale evidence; `Q`-stripped evidence
   matches; class suffix matches only the same class; non-common suffix abstains whole; Form 25 with a
   matching symbol terminates, with NULL symbol flags only; `outside_series`; `shared_cik`; ledger
   reconciliation; policy mismatch and moved-manifest refusal; rebuild determinism.
2. **Causal reference** (full population, as #3360 item 2): for every series and every distinct evidence date
   d touching it (plus one day before the first), rebuild from inputs filtered to filing date < d+1, recompute
   rule 4–5 independently, and assert equality with `link_as_of(S, d+1)`.
3. **Cross-checks, reported, not gates:** (a) for series whose `instrument_id` has an eToro CIK
   (`instrument_cik_history`), agreement of the link at the series' last bar with that CIK; (b) for series
   linked to a Form 25 (`delisting_provision` set), agreement of the link on the Form 25 filing date with
   `issuer_cik`, **with Form 25 removed from the inputs**, so the check is not circular. Every disagreement is
   listed with its accessions.
4. Census evidence manifest written.
5. Registry (`app/services/research_point_in_time.py`): the `historical_population` cells that cite #3361
   stay **fail** until an arm declares how it consumes abstentions. This ticket records the linkage's
   `public_clock` (filing date; public from the next date) and `causal_transform` (item 2) as evidence only.
   **Nothing becomes admissible.**
