# SEC bulk archives — full catalogue

> Source-of-truth lens for every bulk archive SEC publishes, written 2026-05-25 to inform "bulk vs per-CIK HTTP" decisions during first-install bootstrap and steady-state refresh. Every claim is grounded in a live curl probe against `www.sec.gov` (User-Agent `eBull research luke.bradford@hotmail.co.uk`) or a peek inside the actual ZIP TSV headers from the 2026-Q1 / 2026-04 publications. Where this document says "we already use this" it cross-references `.claude/skills/data-engineer/etl-endpoint-coverage.md`. Where it says "we don't use this" it is a candidate for either bulk-replaces-HTTP optimisation or genuine new-source onboarding.

## Reading guide

- §1 — bulk archives **eBull already consumes** (verify the wiring matches this doc).
- §2 — bulk archives **published by SEC, eligible for eBull, NOT consumed**. Flag-red gap candidates.
- §3 — bulk archives **published by SEC, NOT eligible** (out of scope or out of asset class).
- §4 — reference JSON / index files (not "bulk" but bandwidth-cheap).
- §5 — per-form structured XML (single-filing HTTP only; no bulk path exists).
- §6 — **Gap summary**: ranked by impact on first-install wall clock.

Two non-negotiables apply throughout:

1. SEC's User-Agent + 10 req/s budget is per-IP. The bulk-archive lane (`sec_bulk_download`) and the per-CIK lane (`sec_rate`) share the same physical IP — they DO NOT have independent budgets. Bulk-archive HEAD/GET requests count against the same 10 r/s bucket; eBull tunes the bulk lane to single-stream so it doesn't starve per-CIK polls. See `app/providers/implementations/sec_edgar.py:72` (`_PROCESS_RATE_LIMIT_CLOCK`).
2. Bulk archives override `If-None-Match` / `If-Modified-Since` — they return `200 + full body` regardless of the request headers (empirical probe `sec-edgar.md` §4 "Bulk-archive reuse contract"). The right reuse contract is client-side HEAD → compare ETag against `<archive>.etag` sidecar (implemented at `app/services/sec_bulk_download.py::_preflight_etag_keyed_reuse`).

---

## 1. Bulk archives eBull already consumes

### 1.1 `submissions.zip`

**Canonical URL.** `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip`

**Refresh cadence.** Nightly rebuild ~03:00 ET. Live HEAD on 2026-05-25: `last-modified: Sat, 23 May 2026 04:36:48 GMT, etag: "0df720b19bf3703602790131605704ad-184", content-length: 1,541,582,444` (≈ 1.54 GB).

**Format.** ZIP of one `CIK<10-digit-padded>.json` per CIK. Plus overflow page files `CIK<padded>-submissions-<NNN>.json` for CIKs whose filing history exceeds the "recent" 1000-entry / ≥ 1-year cap. Empirical entry count from a central-directory probe: roughly 5,950 distinct `CIK*.json` filenames in the tail half-MB alone — full archive carries ≈ 1,000,000 CIK files spanning every filer that has ever filed via EDGAR (issuers, fund trusts, institutional advisers, blockholders, insider individuals, banking-act entities, agents — the whole universe).

**Inner JSON shape.** Each `CIK<padded>.json` is the EXACT same payload as `https://data.sec.gov/submissions/CIK<padded>.json`. Top-level keys: `cik, entityType, sic, sicDescription, name, tickers, exchanges, ein, lei, fiscalYearEnd, formerNames, addresses, filings`. `filings.recent` is a columnar block (parallel arrays) with keys `accessionNumber, filingDate, reportDate, acceptanceDateTime, act, form, fileNumber, filmNumber, items, core_type, size, isXBRL, isInlineXBRL, isXBRLNumeric, primaryDocument, primaryDocDescription`. `filings.files[]` carries pointers to overflow pages.

**Size.** ~1.54 GB compressed. Largest individual entry ~few MB for filer CIKs with deep history.

**Coverage.** Universal — every CIK that has filed anything via EDGAR is present, regardless of asset class, jurisdiction, or current registration status. Includes:
- Operating-issuer CIKs (Apple, Microsoft, …).
- Fund trust CIKs (Vanguard Trust, Fidelity Trust, …).
- Institutional adviser / 13F filer CIKs (BlackRock, Berkshire, …) — the population the operator's S16 cron currently per-CIK-walks despite the data already being in this archive.
- Blockholder / 13D/G filer CIKs (Carl Icahn, Pershing Square, …).
- Insider / Form 4 reporting persons (individual officers + directors).
- Filing-agent CIKs (Donnelley, Edgar Agents, RR Donnelley, …) — they HAVE submissions.json files too (their own filings, mostly admin).
- Tombstoned / deregistered CIKs.

**Primary keys + dedup.** Filename is unique by `(CIK)`. Inside the JSON, accession numbers within `filings.recent.accessionNumber[]` are unique per CIK. Amendments are surfaced as separate accession numbers with `form` carrying the `/A` suffix (e.g. `10-K/A`, `13F-HR/A`).

**Update semantics.** Full snapshot rewrite every night. There is no incremental shape. SEC retroactively backfills if a clerical fix changes a `filingDate`. Pre-#1010 amendments may flip a CIK from "active filer" → "tombstoned" by removing the rows; nothing in the archive tells you that, you only see the absence.

**Gotchas.**
- `filings.recent` is capped at "1000 most-recent OR ≥ 1 year". Older history sits in `filings.files[]` pointer array. Must recurse — pattern at `app/services/institutional_holdings.py:189-220`, `app/jobs/sec_rebuild.py:335`, `app/jobs/sec_first_install_drain.py:455`. The bulk archive INCLUDES the overflow pages (filenames like `CIK0001067983-submissions-001.json`), so first-install can drain deep history from the ZIP rather than per-CIK HTTP — but eBull's S8 ingester currently only reads the primary `CIK<padded>.json` file and skips the overflow pages, leaving them to Stage 13 `sec_submissions_files_walk` HTTP path.
- `tickers: []` for delisted / foreign-only / fund-family filers. Length-check before index.
- Same accession is in EVERY filer's submissions.json when the filing has multiple CIKs (e.g. 13F joint filings, 8-K co-issuer events). Naive iteration over every CIK + every accession will fetch the same primary document N times. The right shape is to dedupe accessions before fetching bodies.
- `acceptanceDateTime` is UTC despite SEC's published ET cutoff — convert via `zoneinfo.ZoneInfo("America/New_York")` if the day-boundary matters.
- The bulk ZIP's nightly rebuild can lag the per-CIK API by up to 24h. Operator-visible figure: a filing accepted at 23:00 UTC Tuesday is in `data.sec.gov/submissions/CIK*.json` by 23:01 UTC, but not in `submissions.zip` until ~07:00 UTC Wednesday.
- ZIP entry ordering not guaranteed — read by name, not by iteration position.

**License + rate limit.** Bulk HEAD and GET both count against the 10 req/s per-IP budget. A single GET of `submissions.zip` is one request — bandwidth is the real constraint, not request count. eBull's `sec_bulk_download` lane single-streams so it doesn't starve `sec_rate`. The on-host probe path (`app/services/sec_bulk_download.py::probe_bandwidth`) bypasses to the legacy per-CIK path if bandwidth is below threshold.

**eBull current use.**
- **Bootstrap Stage 7** (`sec_bulk_download` lane) — downloads the ZIP once at first-install. Reuse contract: HEAD → ETag-compare against `<archive>.zip.etag` sidecar. `bootstrap_runs` stamps `reuse_reason: "etag_match_sha256_verified"` if unchanged.
- **Bootstrap Stage 8** (`sec_submissions_ingest`) — ingests the ZIP into `sec_filing_manifest` for in-universe CIKs.
- **Daily refresh** (`sec_submissions_bulk_refresh`, daily 08:00 UTC) — HEAD-checks and re-pulls only if ETag changed.

**Known gap.** Stage 8's ingester walks the ZIP, but the operator's run-#8 receipt shows it skips submissions.json entries for **CIKs that have no `external_identifiers` row in the eBull universe** — which is correct for issuer-grain ingestion but means the ~12k institutional-filer CIKs are not read FROM THE BULK ZIP even though they're sitting in it. Stage 16 (`sec_first_install_drain` — institutional filer walk) then issues 11,205 fresh `data.sec.gov/submissions/CIK*.json` HTTP requests per first-install to read the EXACT same JSON the ZIP carries. **Bulk-replaces-HTTP candidate; estimated 30-45 min wall-clock save per first-install at 7 req/s sustained budget.**

---

### 1.2 `companyfacts.zip`

**Canonical URL.** `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip`

**Refresh cadence.** Nightly rebuild ~03:00 ET. Live HEAD on 2026-05-25: `last-modified: Sat, 23 May 2026 04:28:58 GMT, etag: "4c0025fb251d238a06d0706d3fff99a5-166", content-length: 1,384,523,212` (≈ 1.38 GB).

**Format.** ZIP of one `CIK<10-digit-padded>.json` per CIK. ~1 million entries (sample showed `CIK0001672619.json` etc. in central directory).

**Inner JSON shape.** Identical payload to `https://data.sec.gov/api/xbrl/companyfacts/CIK<padded>.json`. Top-level: `cik, entityName, facts`. `facts` is `{"dei": {...}, "us-gaap": {...}}`. Each concept (`facts["us-gaap"]["Assets"]`) carries `label, description, units`. `units` is keyed by unit-of-measure (`USD`, `shares`, `USD/shares`, `pure`, …); each unit's value is an array of fact rows: `{end, val, accn, fy, fp, form, filed, frame?, start?}`.

**Size.** ~1.38 GB compressed.

**Coverage.** Every CIK that has ever submitted an XBRL-tagged filing. Operating issuers (10-K / 10-Q post-2009-06-15), fund trusts (N-CSR iXBRL post-2022-07-25), some 8-K events with iXBRL attached. Pre-mandate filings have no facts. Foreign issuers without ADR registration won't have US-GAAP facts.

**Primary keys + dedup.** `(cik, concept, unit, accn, end)` — the same fact in an amended filing has a NEW accession number; the original stays. **Restatement chain**: a 10-K/A with the same `(concept, unit, end)` as the original gets a new row with a later `filed` date. eBull's `financial_periods` table picks the canonical by `(instrument_id, period_end_date, period_type)` with `superseded_at` tracking the override chain (sql/032).

**Update semantics.** Full snapshot rewrite nightly. Same lag-vs-realtime trade-off as `submissions.zip`. New filings accepted today don't land in tonight's ZIP if they cleared SEC after the rebuild cutover.

**Gotchas.**
- `units` can be empty or carry an unfamiliar unit key. Defensive iteration pattern at `sec-edgar.md` §7.5 — never `units["USD"][0]` blind.
- `period_end` outside `[1900, 2100)` is a real bug in raw XBRL — see `#1218` and `sec-edgar.md` §7.16. The parser guard at `app/providers/implementations/sec_fundamentals.py::_classify_period_rejection` rejects these at the chokepoint.
- Some concepts include a `frame` tag (e.g. `CY2024Q1`) that lets you reverse-lookup the same fact in the `frames` API. Not all concepts have frames.
- Restatements: same `(cik, concept, unit, end)` can have multiple rows differing only in `accn` + `filed`. Take latest by `filed` for current-value reads; keep historical rows for restatement audit.
- iXBRL inline filings (post-2019) and traditional XBRL (pre-2019) both normalise into the same `facts` structure — SEC handles the format compatibility server-side.

**License + rate limit.** Same single-stream constraint as `submissions.zip`.

**eBull current use.**
- **Bootstrap Stage 7** — downloads.
- **Bootstrap Stage 9** (`sec_companyfacts_ingest`) — ingests into `financial_facts_raw` for in-universe CIKs.
- **Bootstrap Stage 25** (`fundamentals_sync_bootstrap`, post-Stream-A PR-C2) — derives `financial_periods` + `financial_periods_ttm` from already-loaded `financial_facts_raw`. NO HTTP — the bulk-zip-loaded XBRL facts are the input.
- **Daily refresh** (`sec_companyfacts_bulk_refresh`, daily 08:30 UTC) — HEAD + ETag reuse.

**Known gap.** None. This is the cleanest end-to-end bulk path eBull has.

---

### 1.3 13F-HR quarterly dataset (`form13f`)

**Canonical URL.** `https://www.sec.gov/files/structureddata/data/form-13f-data-sets/{label}_form13f.zip`. **Label scheme changed mid-2024**: old `2013q2_form13f.zip` → `2024q1_form13f.zip` style stopped after 2023-Q4. New labels are 3-month windows by report period: `01dec2024-28feb2025_form13f.zip`, `01mar2025-31may2025_form13f.zip`, `01jun2025-31aug2025_form13f.zip`, `01sep2025-30nov2025_form13f.zip`, `01dec2025-28feb2026_form13f.zip`. eBull's label generator at `app/services/sec_bulk_download.py::last_n_13f_periods` produces both shapes — calendar-quarter fallback for pre-2024-Q1 archives.

**Refresh cadence.** Quarterly. Data filed after 5:30 PM ET on the last business day of a quarter rolls into the subsequent posting. Empirical: 2025-Q4 archive (`01dec2025-28feb2026_form13f.zip`) was posted shortly after the 45-day 13F filing window closed (≈ 14 Feb 2026).

**Format.** ZIP of TSVs. Members: `SUBMISSION.tsv, COVERPAGE.tsv, INFOTABLE.tsv, OTHERMANAGER.tsv, OTHERMANAGER2.tsv, SIGNATURE.tsv, SUMMARYPAGE.tsv, FORM13F_metadata.json, FORM13F_readme.htm`.

**Size.** ~80-90 MB per quarter (2025-Q3 = 82 MB).

**INFOTABLE.tsv field reference.**

| Field | Type | Meaning | PK contribution |
|---|---|---|---|
| ACCESSION_NUMBER | TEXT(25) | dashed accession, prefix=filer CIK | yes (with INFOTABLE_SK) |
| INFOTABLE_SK | BIGINT | row surrogate within accession | yes |
| NAMEOFISSUER | TEXT(200) | issuer name as filer wrote it | no |
| TITLEOFCLASS | TEXT(150) | `COM`, `COM CL A`, `PUT`, `CALL`, … | no |
| CUSIP | CHAR(9) | issuer-security identifier | resolution key |
| FIGI | TEXT | OpenFIGI ID — **new column 2024+** | future bridge |
| VALUE | NUMBER | **see VALUE-cutover 2023-01-03** (sec-edgar §7.1) | no |
| SSHPRNAMT | NUMBER | shares OR principal | conditional |
| SSHPRNAMTTYPE | TEXT(10) | `SH` (shares) or `PRN` (bond principal) | filter `'SH'` only |
| PUTCALL | TEXT(10) | `Put`, `Call`, or empty | branch |
| INVESTMENTDISCRETION | TEXT(10) | `SOLE`, `DFND`, `OTR` | no |
| OTHERMANAGER | TEXT(100) | comma-sep manager seq numbers | join to OTHERMANAGER.tsv |
| VOTING_AUTH_SOLE/SHARED/NONE | NUMBER | voting authority breakdown | no |

**COVERPAGE.tsv.** Filer identity: `FILINGMANAGER_NAME, FILINGMANAGER_STREET1/2/CITY/STATEORCOUNTRY/ZIPCODE, REPORTTYPE (13F-HR / 13F-NT / 13F-HR/A), FORM13FFILENUMBER, CRDNUMBER, SECFILENUMBER, ISAMENDMENT, AMENDMENTNO, AMENDMENTTYPE, REPORTCALENDARORQUARTER`.

**SUBMISSION.tsv.** `ACCESSION_NUMBER, FILING_DATE, SUBMISSIONTYPE, CIK, PERIODOFREPORT`.

**SUMMARYPAGE.tsv.** `OTHERINCLUDEDMANAGERSCOUNT, TABLEENTRYTOTAL, TABLEVALUETOTAL, ISCONFIDENTIALOMITTED`. The TABLEVALUETOTAL ALSO flips on the 2023-01-03 dollars-cutover.

**Coverage.** Every 13F-HR (holdings) and 13F-NT (notice) filing accepted in the 3-month report window, regardless of filer-CIK in-scope. ~3500-4000 13F-HR filings per quarter from US institutional advisers with > $100M discretionary AUM. **Reporting threshold**: securities listed on the SEC's Official 13(f) List (~24k CUSIPs) — bonds, futures, FX out of scope.

**Primary keys + dedup.** `(ACCESSION_NUMBER)` is unique per filing. `(ACCESSION_NUMBER, INFOTABLE_SK)` is unique per holding row. Same `(NAMEOFISSUER, CUSIP)` CAN appear multiple times per accession when the manager splits a position by share class or discretion bucket — must sum, not pick. Amendments (`13F-HR/A`) get NEW accession numbers; original stays in the archive.

**Update semantics.** Snapshot. The archive for the just-closed window includes corrections filed within that window, but a 13F-HR/A filed after the archive was generated lands in the NEXT quarterly archive, not retroactively in the current one.

**Gotchas.**
- **PRN rows** = bond principal in dollars, NOT shares. Filter `WHERE SSHPRNAMTTYPE = 'SH'` before share aggregation. Same issuer name (`APPLE INC`) appears for AAPL bonds vs AAPL stock — only the CUSIP distinguishes.
- **VALUE-cutover 2023-01-03**: pre-cutover VALUE is thousands of dollars (`230` = $230,000), post-cutover is dollars-to-the-nearest (`230` = $230). Branch on FILED_DATE not PERIODOFREPORT (some pre-cutover-period filings were re-filed post-cutover and use dollars).
- **13F-HR vs 13F-NT** (#1010): NT = "notice; another manager files our holdings". NT has ZERO infotable rows. eBull's `institutional_filers.last_13f_hr_at` is the HR-only recency signal that bounds the bootstrap-stage-21 cohort.
- **FIGI column added in 2024 sample archives** — older quarters do NOT have it. Parser must read columns by header, not by index.
- **Joint filers**: a single 13F can list multiple managers in OTHERMANAGER + OTHERMANAGER2; INFOTABLE rows point to manager seq via the `OTHERMANAGER` field. For per-manager aggregation, dedupe holdings the joint filer already counted.

**License + rate limit.** Bulk download via `sec_bulk_download` lane.

**eBull current use.**
- **Bootstrap Stage 7** — downloads last 4 quarters of `form13f` archives.
- **Bootstrap Stage 10** (`sec_13f_ingest_from_dataset`) — `app/services/sec_13f_dataset_ingest.py`. PRN drop, VALUE cutover, unresolved-CUSIP capture into `unresolved_13f_cusips` (`source='bulk_13f_dataset'`).
- **Steady-state Stage 21** (`sec_13f_quarterly_sweep`) — sweeps recent quarter via filer-directory walk + per-filing HTTP; cohort-bounded by `last_13f_hr_at`. Manifest worker drains via `sec_13f_hr.py` (EdgarTools wrapper, #931). Cron retired in #1164; manifest worker is sole steady-state writer.

**Known gap.** Cutover semantics applied service-side (`sec_13f_dataset_ingest.py:316-326`); parser-side raw value is preserved per #931 contract.

---

### 1.4 N-PORT-P quarterly dataset (`nport`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/form-n-port-data-sets/{YYYY}q{Q}_nport.zip`. Live HEAD 2026-05-25: `2026q1_nport.zip` exists.

**Refresh cadence.** Quarterly. SEC publishes N-PORT for the just-closed calendar quarter ~60 days after quarter end (filings due 60 days after each calendar-quarter end).

**Format.** ZIP of TSVs. **Largest in the catalogue** (≈ 442 MB for 2026Q1).

**Members (32 TSVs).** `SUBMISSION.tsv, REGISTRANT.tsv, FUND_REPORTED_INFO.tsv, INTEREST_RATE_RISK.tsv, BORROWER.tsv, BORROW_AGGREGATE.tsv, MONTHLY_TOTAL_RETURN.tsv, MONTHLY_RETURN_CAT_INSTRUMENT.tsv, FUND_VAR_INFO.tsv, FUND_REPORTED_HOLDING.tsv, IDENTIFIERS.tsv, DEBT_SECURITY.tsv, DEBT_SECURITY_REF_INSTRUMENT.tsv, CONVERTIBLE_SECURITY_CURRENCY.tsv, REPURCHASE_AGREEMENT.tsv, REPURCHASE_COUNTERPARTY.tsv, REPURCHASE_COLLATERAL.tsv, DERIVATIVE_COUNTERPARTY.tsv, SWAPTION_OPTION_WARNT_DERIV.tsv, DESC_REF_INDEX_BASKET.tsv, DESC_REF_INDEX_COMPONENT.tsv, DESC_REF_OTHER.tsv, FUT_FWD_NONFOREIGNCUR_CONTRACT.tsv, FWD_FOREIGNCUR_CONTRACT_SWAP.tsv, NONFOREIGN_EXCHANGE_SWAP.tsv, FLOATING_RATE_RESET_TENOR.tsv, OTHER_DERIV.tsv, OTHER_DERIV_NOTIONAL_AMOUNT.tsv, SECURITIES_LENDING.tsv, EXPLANATORY_NOTE.tsv` + readme + metadata.

**Headline schemas.**

```
SUBMISSION.tsv:
  ACCESSION_NUMBER  FILING_DATE  FILE_NUM  SUB_TYPE  REPORT_ENDING_PERIOD
  REPORT_DATE  IS_LAST_FILING

REGISTRANT.tsv:
  ACCESSION_NUMBER  CIK  REGISTRANT_NAME  FILE_NUM  LEI
  ADDRESS1  ADDRESS2  CITY  STATE  COUNTRY  ZIP  PHONE

FUND_REPORTED_INFO.tsv (one row per (accession, series)):
  ACCESSION_NUMBER  SERIES_NAME  SERIES_ID  SERIES_LEI
  TOTAL_ASSETS  TOTAL_LIABILITIES  NET_ASSETS  ASSETS_ATTRBT_TO_MISC_SECURITY
  ASSETS_INVESTED  BORROWING_PAY_WITHIN_1YR  …  IS_NON_CASH_COLLATERAL
  NET_REALIZE_GAIN_NONDERIV_MON{1,2,3}  NET_UNREALIZE_AP_NONDERIV_MON{1,2,3}

FUND_REPORTED_HOLDING.tsv (one row per holding):
  ACCESSION_NUMBER  HOLDING_ID  ISSUER_NAME  ISSUER_LEI  ISSUER_TITLE  ISSUER_CUSIP
  BALANCE  UNIT  OTHER_UNIT_DESC  CURRENCY_CODE  CURRENCY_VALUE  EXCHANGE_RATE
  PERCENTAGE  PAYOFF_PROFILE  ASSET_CAT  OTHER_ASSET  ISSUER_TYPE  OTHER_ISSUER
  INVESTMENT_COUNTRY  IS_RESTRICTED_SECURITY  FAIR_VALUE_LEVEL  DERIVATIVE_CAT

IDENTIFIERS.tsv (one+ row per HOLDING_ID):
  HOLDING_ID  IDENTIFIERS_ID  IDENTIFIER_ISIN  IDENTIFIER_TICKER
  OTHER_IDENTIFIER  OTHER_IDENTIFIER_DESC
```

**Size.** ~400-500 MB per quarter. 2026-Q1 archive uncompressed is several GB.

**Coverage.** Every N-PORT-P filing in the quarter — i.e. every open-end fund / ETF / closed-end fund / variable-annuity-sub-account that files monthly portfolio reports under rule 30b1-9. Trust-grain at the SUBMISSION level; series-grain at FUND_REPORTED_INFO; per-holding at FUND_REPORTED_HOLDING. NOT included: money-market funds (file N-MFP instead — see §2.3), unit investment trusts.

**Primary keys + dedup.** `ACCESSION_NUMBER` is unique per N-PORT-P filing. `HOLDING_ID` is unique within accession. `(accession, series_id)` is unique in FUND_REPORTED_INFO. Same issuer CUSIP appears once per holding row; share-class siblings each carry the same CUSIP at the security level. **Quarterly file carries 3 months of N-PORT-P filings** because filers file monthly but only the third-month-of-quarter filing is public — the prior 2 months are filed non-publicly with SEC. So one ZIP = roughly one filing per series.

**Update semantics.** Snapshot. Amendments (N-PORT/A) land in the next quarter's archive.

**Gotchas.**
- **UNIT column** can be `NS` (shares), `PA` (principal — bond), `NC` (notional contracts), `OU` (other). For equity-ownership aggregation, filter `UNIT='NS'` and `ASSET_CAT='EC'` (equity-common) and `PAYOFF_PROFILE='Long'`.
- **`PERCENTAGE` is decimal-percent** (`2.345` = 2.345%), not fraction.
- **Fund hierarchy**: filings are at the **trust** CIK level. Each trust has multiple **series** (`S000NNNNNN`). Each series has multiple **share classes** (`C000NNNNNN`). Aggregate by `(seriesId, issuerCusip)` — share classes share the portfolio, aggregating by classId double-counts.
- **HOLDING_ID is unique within accession, NOT globally** — the same HOLDING_ID appears in different N-PORT filings.
- **CUSIP can be NULL** for derivatives, swaps, foreign-only securities. Use IDENTIFIERS.tsv for ISIN / ticker fallback.
- **Per-series totals must sum across N rows of FUND_REPORTED_INFO when a trust has multiple series**, not just trust-level row count.
- ASSET_CAT enum: `EC` (equity-common), `EP` (equity-preferred), `DBT` (debt), `ABS` (asset-backed), `STIV` (short-term investment vehicle), `LON` (loan), `DCO` (derivatives — commodities), `DCR` (derivatives — credit), `DE` (derivatives — equity), `DFE` (derivatives — foreign-exchange), `DIR` (derivatives — interest-rate), `DOT` (derivatives — other), `REC` (real estate / commodities), `SN` (structured note), `RA` (repurchase agreement), `RFA` (reverse-repo), `MF` (mutual fund), `STN` (sub-trust), `MAC` (mortgage-backed), `COMM` (commodity), `OTH` (other).

**License + rate limit.** Bulk download; `sec_bulk_download` lane.

**eBull current use.**
- **Bootstrap Stage 7** — downloads last 4 quarters of nport archives.
- **Bootstrap Stage 12** (`sec_nport_ingest_from_dataset`) — `app/services/sec_nport_dataset_ingest.py`. Filters `ASSET_CAT='EC'`, `PAYOFF_PROFILE='Long'`, `UNIT='NS'`. Captures unresolved CUSIPs into `unresolved_13f_cusips` (`source='bulk_nport_dataset'`).
- **Bootstrap Stage 23** (`sec_n_port_ingest`) — current-quarter sweep via filer-directory + per-filing HTTP.
- **Manifest worker** — `manifest_parsers/sec_n_port.py` (#1133) for steady-state per-accession ingest.

**Known gap.** Heavy bandwidth — 442 MB per quarter × 4 quarters = 1.7 GB just for N-PORT. Bandwidth-probe-bounded.

---

### 1.5 Form 3/4/5 quarterly dataset (`form345`)

**Canonical URL.** `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{YYYY}q{Q}_form345.zip`. 2026-Q1 confirmed HEAD 200.

**Refresh cadence.** Quarterly. Filings accepted by 5:30 PM ET on the last business day of a quarter roll into the subsequent posting.

**Format.** ZIP of TSVs. ~13 MB compressed per quarter (smallest of the bulk datasets eBull consumes).

**Members.** `SUBMISSION.tsv, REPORTINGOWNER.tsv, OWNER_SIGNATURE.tsv, NONDERIV_TRANS.tsv, NONDERIV_HOLDING.tsv, DERIV_TRANS.tsv, DERIV_HOLDING.tsv, FOOTNOTES.tsv, FORM_345_metadata.json, FORM_345_readme.htm`.

**Headline schemas.**

```
SUBMISSION.tsv:
  ACCESSION_NUMBER  FILING_DATE  PERIOD_OF_REPORT  DATE_OF_ORIG_SUB
  NO_SECURITIES_OWNED  NOT_SUBJECT_SEC16  FORM3_HOLDINGS_REPORTED
  FORM4_TRANS_REPORTED  DOCUMENT_TYPE  ISSUERCIK  ISSUERNAME
  ISSUERTRADINGSYMBOL  REMARKS  AFF10B5ONE

REPORTINGOWNER.tsv:
  ACCESSION_NUMBER  RPTOWNERCIK  RPTOWNERNAME  RPTOWNER_RELATIONSHIP
  RPTOWNER_TITLE  RPTOWNER_TXT  RPTOWNER_STREET1  RPTOWNER_STREET2
  RPTOWNER_CITY  RPTOWNER_STATE  RPTOWNER_ZIPCODE  RPTOWNER_STATE_DESC
  FILE_NUMBER

NONDERIV_TRANS.tsv:
  ACCESSION_NUMBER  NONDERIV_TRANS_SK  SECURITY_TITLE  SECURITY_TITLE_FN
  TRANS_DATE  TRANS_DATE_FN  DEEMED_EXECUTION_DATE …  TRANS_FORM_TYPE
  TRANS_CODE  EQUITY_SWAP_INVOLVED  TRANS_SHARES  TRANS_SHARES_FN
  TRANS_PRICEPERSHARE  TRANS_PRICEPERSHARE_FN  TRANS_ACQUIRED_DISP_CD
  SHRS_OWND_FOLWNG_TRANS  VALU_OWND_FOLWNG_TRANS
  DIRECT_INDIRECT_OWNERSHIP  NATURE_OF_OWNERSHIP

DERIV_TRANS.tsv: + CONV_EXERCISE_PRICE, EXERCISE_DATE, EXPIRATION_DATE,
  UNDLYNG_SEC_TITLE, UNDLYNG_SEC_SHARES, UNDLYNG_SEC_VALUE, …
```

**Every value-field is paired with a `_FN` footnote-id sibling** — when the filer attached a footnote to a transaction (typical for net-settlement, gifts, swaps), the footnote text lives in FOOTNOTES.tsv keyed by `(ACCESSION_NUMBER, FOOTNOTE_ID)`.

**Size.** ~13-15 MB compressed per quarter; uncompressed ~50 MB.

**Coverage.** Every Form 3 (initial holdings), Form 4 (transactions), Form 5 (annual catch-up) filing in the quarter. ~150k filings per quarter at steady state. Includes officer/director/10%-holder filings for every US-listed issuer.

**Primary keys + dedup.** `ACCESSION_NUMBER` + `<TABLE>_SK` (transaction surrogate key per table). Same accession can have BOTH derivative and non-derivative transactions — same insider, same filing, different security types. Amendments (Form 4/A) have new accessions; original stays.

**Update semantics.** Snapshot. Amendments land in next quarter's archive.

**Gotchas.**
- **DIRECT_INDIRECT_OWNERSHIP**: `D` and `I` are SEPARATE position lines for the SAME insider — must aggregate BOTH for total Section 16 ownership (the #905 JPM rollup 1.29% → 6.16% bug). Direct = held in insider's name; indirect = held via trust, family member, LLC. Both count toward the insider's beneficial ownership.
- **TRANSACTION CODE reference** (also in `sec-edgar.md` §2.3): P (purchase), S (sale), A (award/grant), D (disposition back to company), F (net-settlement / tax withhold), M (option exercise), C (conversion), G (gift), K (swap), X (in/at-money exercise), O (out-of-money exercise), J (other), U (tender). For "open-market activity" signal, filter `TRANS_CODE IN ('P', 'S')`.
- **TRANS_PRICEPERSHARE** can be `0` for awards / grants / vesting (not market transactions). Filter for non-zero before using as a price signal.
- **Form 5 enum**: `DOCUMENT_TYPE='5'` is the annual catch-up; `4/A` is amended Form 4; `3/A` amended Form 3. eBull's insider-345 parser handles all three (`document_type='5'` rows land as `source='form4'` in observations because the source enum lacks `form5` — provenance preserved via `insider_filings.document_type` JOIN).
- **`<value>`-wrapping in raw XML** is not present in the TSV. The bulk dataset has flattened the per-leaf footnote wrapper that makes Form 4 XML parsing tricky (`sec-edgar.md` §2.3 / §7.10). If you only consume the TSV path, you skip the wrapper complexity.
- **DEEMED_EXECUTION_DATE** can be later than TRANS_DATE when the insider's broker reports late.

**License + rate limit.** Bulk download; `sec_bulk_download` lane.

**eBull current use.**
- **Bootstrap Stage 7** — downloads last 8 quarters of insider archives.
- **Bootstrap Stage 11** (`sec_insider_ingest_from_dataset`) — `app/services/sec_insider_dataset_ingest.py`.
- **Bootstrap Stage 18-20** — legacy backfill / Form 3 ingest / per-CIK walks.
- **Manifest worker** — `manifest_parsers/insider_345.py` (#1130, #1134).

**Known gap.** None — fully consumed end-to-end.

---

## 2. Bulk archives published, eligible, NOT consumed

### 2.1 N-CEN annual fund data (`ncen`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/form-n-cen-data-sets/{YYYY}q{Q}_ncen.zip`. 2026-Q1 confirmed.

**Refresh cadence.** Quarterly.

**Format.** ZIP of TSVs. ~16 MB compressed. **27 member TSVs** — by far the most relational structure of any SEC dataset.

**Members.** `SUBMISSION.tsv, REGISTRANT.tsv, REGISTRANT_WEBSITE.tsv, LOCATION_BOOKS_RECORD.tsv, TERMINATED_ORGANIZATION.tsv, DIRECTOR.tsv, DIRECTOR_FILE_NUMBER.tsv, CHIEF_COMPLIANCE_OFFICER.tsv, CCO_EMPLOYER.tsv, REGISTRANT_REPORTING_SERIES.tsv, RELEASE_NUMBER.tsv, PRINCIPAL_UNDERWRITER.tsv, PUBLIC_ACCOUNTANT.tsv, VALUATION_METHOD_CHANGE.tsv, VALUATION_METHOD_CHANGE_SERIES.tsv, FUND_REPORTED_INFO.tsv, SHARES_OUTSTANDING.tsv, FEEDER_FUNDS.tsv, MASTER_FUNDS.tsv, FOREIGN_INVESTMENT.tsv, SECURITY_LENDING.tsv, SEC_LENDING_IDEMNITY_PROVIDER.tsv, COLLATERAL_MANAGER.tsv, ADVISER.tsv, TRANSFER_AGENT.tsv` + readme + metadata.

**Headline schemas.**

```
SUBMISSION.tsv:
  ACCESSION_NUMBER  SUBMISSION_TYPE  CIK  FILING_DATE  REPORT_ENDING_PERIOD
  IS_REPORT_PERIOD_LT_12MONTH  FILE_NUM  REGISTRANT_SIGNED_NAME
  DATE_SIGNED  SIGNATURE  TITLE  IS_LEGAL_PROCEEDINGS  IS_PROVISION_FINANCIAL_SUPPORT
  IS_IPA_REPORT_INTERNAL_CONTROL  IS_CHANGE_ACC_PRINCIPLES  IS_INFO_REQUIRED_EO
  IS_OTHER_INFO_REQUIRED  IS_MATERIAL_AMENDMENTS  IS_INST_DEFINING_RIGHTS
  IS_NEW_OR_AMENDED_INV_ADV_CONT  IS_INFO_ITEM405  IS_CODE_OF_ETHICS

REGISTRANT.tsv:
  ACCESSION_NUMBER  REGISTRANT_NAME  FILE_NUM  CIK  LEI  ADDRESS…
  IS_FIRST_FILING  IS_LAST_FILING  IS_FAMILY_INVESTMENT_COMPANY
  FAMILY_INVESTMENT_COMPANY_NAME  INVESTMENT_COMPANY_TYPE  TOTAL_SERIES
  IS_REGISTERED_UNDER_ACT_1933  …  IS_NAV_ERROR_CORRECTED  ANY_DIVIDEND_PAYMENT

FUND_REPORTED_INFO.tsv (per-series):
  FUND_ID  ACCESSION_NUMBER  FUND_NAME  SERIES_ID  LEI  IS_FIRST_FILING
  AUTHORIZED_SHARES_CNT  ADDED_NEW_SHARES_CNT  TERMINATED_SHARES_CNT
  IS_ETF  IS_ETMF  IS_INDEX  IS_MULTI_INVERSE_INDEX  IS_INTERVAL
  IS_FUND_OF_FUND  IS_MASTER_FEEDER  IS_MONEY_MARKET  IS_TARGET_DATE
  IS_UNDERLYING_FUND  IS_INDEX_AFFILIATED  IS_INDEX_EXCLUSIVE
  RETURN_B4_FEES_AND_EXPENSES  RETURN_AFTR_FEES_AND_EXPENSES
  STDV_B4_FEES_AND_EXPENSES  STDV_AFTR_FEES_AND_EXPENSES
  IS_NON_DIVERSIFIED  IS_FOREIGN_SUBSIDIARY  IS_SEC_LENDING_AUTHORIZED
  DID_LEND_SECURITIES  IS_COLLATERAL_LIQUIDATED  …
  AVG_VALUE_SEC_LOAN  NET_INCOME_SEC_LENDING  IS_RELYON_RULE_*  …
  HAS_EXP_LIMIT  HAS_EXP_REDUCED_WAIVED  HAS_EXP_SUBJ_RECOUP  HAS_EXP_RECOUPED
  AGG_COMMISSION  AGG_PRINCIPAL  DID_PAY_BROKER_RESEARCH  MONTHLY_*

SHARES_OUTSTANDING.tsv:
  FUND_ID  CLASS_NAME  CLASS_ID  TICKER
```

**Size.** ~16 MB per quarter.

**Coverage.** Every N-CEN filing in the quarter. N-CEN is filed **annually** by registered investment companies (open-end funds, closed-end funds, UITs, ETFs). Within a year, every RIC files exactly one N-CEN within 75 days of fiscal year-end. The quarterly bulk archive carries roughly one quarter's worth of N-CEN filings — i.e. funds with March, June, September, December fiscal year ends.

**Primary keys + dedup.** `(ACCESSION_NUMBER)` unique per filing. `(ACCESSION_NUMBER, FUND_ID)` unique per series within filing. SHARES_OUTSTANDING is `(FUND_ID, CLASS_ID)` unique within filing. CLASS_ID maps directly to `company_tickers_mf.json` `classId` → `external_identifiers (provider='sec', identifier_type='class_id')`.

**Update semantics.** Snapshot per filing.

**Gotchas.**
- **IS_ETF / IS_ETMF / IS_INDEX / IS_INTERVAL / IS_MONEY_MARKET / IS_TARGET_DATE** are the closest thing SEC has to a structured fund-classification taxonomy. **eBull's `ncen_classifier` (`app/services/ncen_classifier.py`) currently HTTP-fetches per-filing to read these fields** — but the bulk dataset has them in TSV form. **Bulk-replaces-HTTP candidate.**
- N-CEN amendments (`N-CEN/A`) get new accessions; original stays.
- `IS_FAMILY_INVESTMENT_COMPANY` + `FAMILY_INVESTMENT_COMPANY_NAME` would let eBull build a fund-family parent-child graph — currently absent from the schema.
- `MONTHLY_*` columns at the end of FUND_REPORTED_INFO are placeholder / sparse — most are empty.

**License + rate limit.** Bulk download.

**eBull current use.** **NOT CONSUMED**. N-CEN data lands via `sec_n_csr` manifest parser (#1171) which reads iXBRL from N-CSR not the N-CEN bulk dataset. The `ncen_classifier` reads per-filing `/Archives/edgar/data/{cik}/{acc}/primary_doc.xml` via HTTP. **Bulk-replaces-HTTP candidate; ~16 MB/quarter for all ~5000 RIC classifications.**

---

### 2.2 Financial Statement Data Sets (`fsds`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/financial-statement-data-sets/{YYYY}q{Q}.zip`. 2026-Q1 HEAD 200; content-length ≈ 85 MB.

**Refresh cadence.** Quarterly. Documents filed after 5:30 PM ET on the last business day of a quarter roll into the subsequent posting.

**Format.** ZIP of 4 TSVs: `sub.tsv, tag.tsv, num.tsv, pre.tsv` (+ readme + metadata + `cal.tsv`).

**Schemas.**

```
sub.tsv (per-filing metadata):
  adsh  cik  name  countryba  stprba  cityba  zipba  bas1  bas2  baph
  countryma  stprma  cityma  zipma  mas1  mas2  countryinc  stprinc  ein
  former  changed  afs  wksi  fye  form  period  fy  fp  filed  fileNumber
  accepted  prevrpt  detail  instance  pubfloatusd  floatdate  inlineurl

num.tsv (numeric XBRL facts):
  adsh  tag  version  ddate  uom  segments  dimn  value  footnote  footlen

tag.tsv (taxonomy):
  tag  version  custom  abstract  datatype  iord  tlabel  doc

pre.tsv (presentation linkbase ordering):
  adsh  report  line  stmt  inpth  tag  version  prole  plabel  negating
```

**Size.** ~85 MB per quarter.

**Coverage.** Every 10-K and 10-Q filing in the quarter, structured per the XBRL taxonomy. Each filing's instance document split across `sub` (one row per filing) + `num` (one row per `(filing, tag, period_end, unit, segments)` numeric fact) + `tag` (taxonomy reference) + `pre` (presentation order).

**Primary keys + dedup.** `adsh` (accession-number-dash-stripped) is unique per filing in sub.tsv. `(adsh, tag, version, ddate, uom, segments)` is the natural composite key in num.tsv. tag.tsv `(tag, version)` is unique.

**Update semantics.** Snapshot.

**Gotchas.**
- **Largely redundant with `companyfacts.zip`** for eBull's purposes. The fsds quarterly archive carries the same facts as companyfacts but partitioned by quarter-filed instead of per-CIK. companyfacts wins for first-install bulk drain (cleaner partitioning by CIK matches eBull's `(instrument_id, …)` PK shape). fsds wins for "filings filed in 2025-Q3 only" cross-section queries.
- pre.tsv is the only presentation-order signal SEC publishes — useful for replicating a 10-K's actual line-item order in a UI. companyfacts does not carry this.
- `segments` column in num.tsv is the XBRL dimension serialisation — `us-gaap:StatementBusinessSegmentsAxis=us-gaap:SegmentXMember`. Dimension parsing is non-trivial.

**License + rate limit.** Bulk download.

**eBull current use.** **NOT CONSUMED**. eBull's fundamentals path uses `companyfacts.zip` (per-CIK partition) instead. fsds is more useful for cross-sectional analysis (e.g. "all companies that reported `Revenues` for 2025-Q3"). **Not a gap** — companyfacts covers the same data and is the right shape for our PK.

---

### 2.3 N-MFP Money Market Fund monthly dataset (`nmfp`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/{YYYYMMDD}-{YYYYMMDD}_nmfp.zip` (date range). Live HEAD 2026-05-25: `20260409-20260507_nmfp.zip` exists.

**Refresh cadence.** Monthly. N-MFP is filed by money-market funds within 5 business days of month-end; bulk archive aggregates ~1 month of filings.

**Format.** ZIP of TSVs (~12 MB compressed; 88 MB uncompressed). **23 member TSVs.**

**Members.** `NMFP_SUBMISSION.tsv, NMFP_SERIESLEVELINFO.tsv, NMFP_CLASSLEVELINFO.tsv, NMFP_SCHPORTFOLIOSECURITIES.tsv, NMFP_COLLATERALISSUERS.tsv, NMFP_NRSRO.tsv, NMFP_LIQUIDASSETSDETAILS.tsv, NMFP_DEMANDFEATURE.tsv, NMFP_GUARANTOR.tsv, NMFP_ENHANCEMENTPROVIDER.tsv, NMFP_ADVISER.tsv, NMFP_ADMINISTRATOR.tsv, NMFP_TRANSFERAGENT.tsv, NMFP_DLYNETASSETVALUEPERSHARC.tsv, NMFP_DLYNETASSETVALUEPERSHARS.tsv, NMFP_DLYSHAREHOLDERFLOWREPORT.tsv, NMFP_SEVENDAYGROSSYIELD.tsv, NMFP_SEVENDAYNETYIELD.tsv, NMFP_MASTERFEEDERFUND.tsv, NMFP_BENEFICIALRECORDOWNERCAT.tsv, NMFP_DISPOSITIONOFPORTFOLIOSE.tsv` + readme + metadata.

**Headline schemas.**

```
NMFP_SUBMISSION.tsv:
  ACCESSION_NUMBER  FILING_DATE  SUBMISSIONTYPE  CIK  REPORTDATE
  REGISTRANTFULLNAME  FILER_CIK  REGISTRANTLEIID  SERIES_NAME  NAMEOFSERIES
  LEIOFSERIES  SERIESID  TOTALSHARECLASSESINSERIES  …

NMFP_SERIESLEVELINFO.tsv (per series per filing):
  ACCESSION_NUMBER  SECURITIESACTFILENUMBER  INDPPUBACCTNAME  …
  FEEDERFUNDFLAG  MASTERFUNDFLAG  MONEYMARKETFUNDCATEGORY
  AVERAGEPORTFOLIOMATURITY  AVERAGELIFEMATURITY
  TOTDLYLIQUIDASSETFRIDAYWEEK{1..5}  TOTWLYLIQUIDASSETFRIDAYWEEK{1..5}
  PCTDLYLIQUIDASSETFRIDAYWEEK{1..5}  PCTWKLYLIQUIDASSETFRIDAYWEEK{1..5}
  CASH  TOTALVALUEPORTFOLIOSECURITIES  AMORTIZEDCOSTPORTFOLIOSECURITI
  TOTALVALUEOTHERASSETS  TOTALVALUELIABILITIES  NETASSETOFSERIES
  NUMBEROFSHARESOUTSTANDING  SEEKSTABLEPRICEPERSHARE  STABLEPRICEPERSHARE
  SEVENDAYGROSSYIELD  NETASSETVALUEFRIDAYWEEK{1..5}
  CASHMGMTVEHICLEAFFLIATEDFUNDF  LIQUIDITYFEEFUNDAPPLYFLAG

NMFP_CLASSLEVELINFO.tsv (per share class per filing):
  ACCESSION_NUMBER  CLASS_NAME  CLASSFULLNAME  CLASSESID  MININITIALINVESTMENT
  NETASSETSOFCLASS  NUMBEROFSHARESOUTSTANDING
  NETASSETPERSHAREFRIDAYWEEK{1..5}  GROSSSUBSCRIPTIONFRIDAYWEEK{1..5}
  GROSSREDEMPTIONFRIDAYWEEK{1..5}  TOTALGROSSSUBSCRIPTIONS  TOTALGROSSREDEMPTIONS
  NETASSETVALUEPERSHARE_L  NETSHAREHOLDERFLOWACTIVITYFO_L  SEVENDAYNETYIELD
  PCTSHAREHOLDERCOMP*  …

NMFP_SCHPORTFOLIOSECURITIES.tsv (per portfolio holding):
  ACCESSION_NUMBER  SECURITY_ID  NAMEOFISSUER  TITLEOFISSUER  COUPON
  CUSIP_NUMBER  LEI  ISIN  CIK  RSSDID  OTHERUNIQUEID
  INVESTMENTCATEGORY  BRIEFDESCRIPTION  …  YIELDOFTHESECURITYASOFREPORTIN
  PERCENTAGEOFMONEYMARKETFUNDNET  DAILYLIQUIDASSETSECURITYFLAG
  WEEKLYLIQUIDASSETSECURITYFLAG  ILLIQUIDSECURITYFLAG  RATING_L
  INVESTMENTOWNEDBALANCEPRINCI_L  AVAILABLEFORSALESECURITIESAM_L
```

**Coverage.** Every money-market fund's monthly portfolio report. ~400-500 MMF series per month, each with full portfolio detail (commercial paper, treasuries, repos, time deposits, etc.).

**Primary keys + dedup.** `ACCESSION_NUMBER` unique per filing. `(ACCESSION_NUMBER, SECURITY_ID)` in SCHPORTFOLIOSECURITIES.

**Update semantics.** Snapshot per month.

**Gotchas.**
- **MMFs file monthly but only the third-month-of-quarter is fully public** — same publication pattern as N-PORT.
- **No share-overlap with N-PORT**: N-PORT explicitly excludes money-market funds (they file N-MFP instead). For "ownership of issuer X by funds", both N-PORT (regular funds) and N-MFP (MMFs) carry holdings — though MMFs hold mostly debt, not equity.
- **SEVENDAYNETYIELD** is the closest thing to "interest rate the fund delivers" for retail.
- `CUSIP_NUMBER` and `ISIN` and `CIK` all present per holding — three identifiers without preference. Bridge to instrument via whichever resolves.

**License + rate limit.** Bulk download.

**eBull current use.** **NOT CONSUMED**. MMFs are out of eBull's tradable universe in v1 (settled-decisions: eToro-only broker, MMFs not offered). Wiring N-MFP would only add value if/when eBull adds MMF benchmarking or yield-curve construction. **Not a gap for v1.**

---

### 2.4 Mutual Fund Prospectus Risk/Return Summary (`rr1`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk/return-summary-data-sets/{YYYY}q{Q}_rr1.zip`. 2026-Q1 HEAD 200.

**Refresh cadence.** Quarterly.

**Format.** ZIP of TSVs (~50 MB compressed; 298 MB uncompressed). **Members:** `sub.tsv, tag.tsv, lab.tsv, num.tsv, txt.tsv, cal.tsv, readme.htm, rr1-metadata.json`.

**Schemas.**

```
sub.tsv:
  adsh  cik  name  …  fye  pdate  effdate  form  filed  accepted
  instance  nciks  aciks

num.tsv:
  adsh  tag  version  ddate  uom  series  class  measure  document
  otherdims  iprx  value  footnote  footlen  dimn  dcml

tag.tsv:
  tag  version  custom  abstract  datatype  iord  tlabel  doc
```

**Coverage.** Every 497 / 497K / N-1A / 485APOS / 485BPOS / N-CSR's Risk/Return Summary section, structured per XBRL. Per-(fund series, share class) expense ratio, NAV, total return, hypothetical fee example, etc.

**Primary keys + dedup.** `adsh` unique per filing. `(adsh, tag, ddate, series, class)` for num.

**Update semantics.** Snapshot.

**Gotchas.**
- **The `class` column carries `classId`** — direct bridge to `company_tickers_mf.json`.
- **txt.tsv is 242 MB uncompressed in 2026-Q1** — narrative risk/objective/strategy text. Huge for what it is.
- **Same data is broadly equivalent to what edgartools' `MutualFundObject` extracts from N-CSR iXBRL** — eBull's `sec_n_csr.py` parser reads it from N-CSR directly. So this archive is partially redundant.

**License + rate limit.** Bulk download.

**eBull current use.** **NOT CONSUMED**. The N-CSR parser (#1171) extracts per-(series, class) expense ratio + NAV + portfolio turnover + returns from iXBRL directly. The rr1 dataset would let eBull skip per-CSR iXBRL fetches by reading the consolidated quarterly rr1 archive instead — but the per-fund coverage is the same. **Marginal bulk-replaces-HTTP candidate; current N-CSR path already works via manifest worker.**

---

### 2.5 Variable Insurance Products (`vip`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/variable-insurance-product-data-sets/{YYYY-MM}_vip.zip`. **Monthly** (changed from annual in 2025). 2026-04 HEAD 200.

**Refresh cadence.** Monthly.

**Format.** ZIP of TSVs (~52 MB compressed; 265 MB uncompressed). Members: `sub.tsv, tag.tsv, num.tsv, txt.tsv, scn.tsv, vip_readme.htm, vip_metadata.json`.

**Schemas.**

```
sub.tsv:
  adsh  cik  name  …  form  filed  accepted  instance  docEndDate  docType

num.tsv:
  adsh  tag  version  ddate  uom  segments  value  footnote  footlen  dimn  contract

scn.tsv (contract / series / class):
  adsh  seriesId  seriesName  classId  className
```

**Coverage.** Filings by variable annuity and variable life insurance separate accounts. The `contract` column in num.tsv lets you resolve metrics to contract-level granularity.

**Primary keys + dedup.** `adsh` unique per filing.

**Update semantics.** Snapshot.

**eBull current use.** **NOT CONSUMED**. Variable annuities are not in eBull's tradable universe. **Not a gap for v1.**

---

### 2.6 Business Development Company (`bdc`)

**Canonical URL.** `https://www.sec.gov/files/structureddata/data/business-development-company-bdc-data-sets/{YYYY}_{MM}_bdc.zip`. **Monthly**. 2026-04 HEAD 200.

**Refresh cadence.** Monthly.

**Format.** ZIP of TSVs (~1 MB compressed; 11 MB uncompressed). Members: `datasets/sub.tsv, datasets/tag.tsv, datasets/cal.tsv, datasets/pre.tsv, datasets/num.tsv, datasets/txt.tsv, datasets/non.tsv, soi.tsv, readme.htm, bdc_metadata.json`.

**Schemas.**

```
sub.tsv:
  adsh  cik  name  …  form  period  fy  fp  filed  fileNumber  accepted

soi.tsv (Schedule of Investments):
  adsh  cik  name  ddate  form  filed  period  inlineurl  cstm
  Industry Sector Axis  Investment, Identifier Axis  Investment Type Axis
  Investment Interest Rate  Investment Maturity Date
  Investment Owned, Balance, Principal Amount  Investment Owned, Cost
  Investment Owned, Fair Value  Investment Owned, Net Assets, Percentage
  …  Investment shares  Investment, Acquisition Date  …
```

**Coverage.** Every Business Development Company's 10-K / 10-Q with Schedule of Investments parsed. ~120 BDCs in the US.

**Primary keys + dedup.** `adsh` unique per filing; `(adsh, Investment, Identifier Axis)` for individual SOI rows.

**Update semantics.** Snapshot.

**Gotchas.**
- **soi.tsv has unconventional column names with spaces and commas** — must quote-escape when joining.
- BDCs are operating companies (publicly traded equities) — they ARE in eBull's tradable universe (ARES, MAIN, OBDC, etc.). The SOI data would let eBull surface "what this BDC owns" portfolio detail — but this is far down the priority list.

**eBull current use.** **NOT CONSUMED**. **Low-priority gap** — useful for portfolio-transparency views of BDC equities, not a v1 deliverable.

---

### 2.7 Form D — Regulation D private offerings (`formd`)

**Canonical URL.** `https://www.sec.gov/files/structureddata/data/form-d-data-sets/{YYYY}q{Q}_d.zip`.

**Refresh cadence.** Quarterly.

**Format.** ZIP of TSVs (~3.5 MB compressed). Members: `FORMDSUBMISSION.tsv, ISSUERS.tsv, OFFERING.tsv, RECIPIENTS.tsv, SIGNATURES.tsv, RELATEDPERSONS.tsv` + readme + metadata.

**Schemas.**

```
FORMDSUBMISSION.tsv:
  ACCESSIONNUMBER  FILE_NUM  FILING_DATE  SIC_CODE  SCHEMAVERSION
  SUBMISSIONTYPE  TESTORLIVE  OVER100PERSONSFLAG  OVER100ISSUERFLAG

ISSUERS.tsv:
  ACCESSIONNUMBER  IS_PRIMARYISSUER_FLAG  ISSUER_SEQ_KEY  CIK
  ENTITYNAME  …  JURISDICTIONOFINC  ENTITYTYPE  …

OFFERING.tsv:
  ACCESSIONNUMBER  INDUSTRYGROUPTYPE  INVESTMENTFUNDTYPE  IS40ACT
  REVENUERANGE  AGGREGATENETASSETVALUERANGE  FEDERALEXEMPTIONS_ITEMS_LIST
  ISAMENDMENT  PREVIOUSACCESSIONNUMBER  SALE_DATE  YETTOOCCUR  MORETHANONEYEAR
  ISEQUITYTYPE  ISDEBTTYPE  …  TOTALOFFERINGAMOUNT  TOTALAMOUNTSOLD
  TOTALREMAINING  HASNONACCREDITEDINVESTORS  NUMBERNONACCREDITEDINVESTORS
  TOTALNUMBERALREADYINVESTED  SALESCOMM_DOLLARAMOUNT  FINDERSFEE_DOLLARAMOUNT
  GROSSPROCEEDSUSED_DOLLARAMOUNT  …

RECIPIENTS.tsv:
  ACCESSIONNUMBER  RECIPIENT_SEQ_KEY  RECIPIENTNAME  RECIPIENTCRDNUMBER
  ASSOCIATEDBDNAME  ASSOCIATEDBDCRDNUMBER  STREET1  …
```

**Coverage.** Every Form D (Reg D 506(b), 506(c), 504 private offerings).

**eBull current use.** **NOT CONSUMED**. Reg D offerings are private — irrelevant to eBull's public-equity tradable universe. **Not a gap.**

---

### 2.8 Regulation A — small public offerings (`rega`)

**Canonical URL.** `https://www.sec.gov/files/structureddata/data/regulation-a-data-sets/{YYYY}q{Q}_rega.zip`.

**Refresh cadence.** Quarterly.

**Format.** ZIP of TSVs (~130 KB compressed — tiny). Members: `REG_A_SUBMISSION.tsv, REG_A_ISSUER_INFO.tsv, REG_A_SECURITIES_OFFERED.tsv, REG_A_SECURITIES_ISSUED.tsv, REG_A_EQUITIES_DEBT.tsv, REG_A_EMPLOYEES_INFO.tsv, REG_A_JURISDICTION.tsv, REG_A_SUMMARY_INFO.tsv, REG_A_SUMMARY_INFO_KZ.tsv, REG_A_SIGNATURE_Z.tsv, REG_A_ITEM_INFO_K.tsv, REG_A_ITEM_KZ.tsv, REG_A_ITEM_SECURITIES_K.tsv, REG_A_CERT_SUSPENSION_Z.tsv, REG_A_SUMMARY_NAMES_KZ.tsv` + readme + metadata.

**Coverage.** Regulation A / A+ filings — small public offerings, capped at $75M/year. Most filers are tiny companies that never trade on major exchanges.

**eBull current use.** **NOT CONSUMED**. Out of scope. **Not a gap.**

---

### 2.9 Form C — Crowdfunding offerings (`cf`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/crowdfunding-offerings-data-sets/{YYYY}q{Q}_cf.zip`.

**Refresh cadence.** Quarterly.

**Format.** ZIP of TSVs (~260 KB compressed). Members: `FORM_C_SUBMISSION.tsv, FORM_C_ISSUER_INFORMATION.tsv, FORM_C_DISCLOSURE.tsv, FORM_C_ISSUER_JURISDICTIONS.tsv, FORM_C_ISSUER_SIGNATURE.tsv, FORM_C_COISSUER_INFORMATION.tsv, FORM_C_SIGNATURE.tsv` + readme + metadata.

**Coverage.** Regulation Crowdfunding offerings ($5M cap per year, retail investors). Mostly seed-stage.

**eBull current use.** **NOT CONSUMED**. Out of scope. **Not a gap.**

---

### 2.10 Transfer Agent (`ta`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/transfer-agent-data-sets/{YYYY}q{Q}_ta.zip`.

**Refresh cadence.** Quarterly.

**Format.** ZIP of TSVs (~533 KB compressed). Members: `TA_SUBMISSION.tsv, TA1_REGISTRANT.tsv, TA1_ADDRESS.tsv, TA1_CONTROL_ENTITIES.tsv, TA1_CORP_PARTNER_DATA.tsv, TA2_FILING.tsv, TA2_SECURITY_HOLDER_ACCOUNTS.tsv, TA2_DB_SEARCH.tsv, TA_DISCIPLINARY_HIST_DETAILS.tsv, TA_SERVICE_COMPANIES.tsv, TAW_ENTITY.tsv, TAW_FILING.tsv` + readme + metadata.

**Coverage.** Transfer-agent registrations (Form TA-1, TA-2). Operational metadata — how many shareholder accounts each TA services, broken down by security type (equity / debt / open-end / municipal).

**eBull current use.** **NOT CONSUMED**. Operational reference; no operator-visible figure depends on it. **Not a gap.**

---

### 2.11 Financial Statement and Notes Data Sets (`fsn`)

**Canonical URL.** `https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/{YYYY}_{MM}_notes.zip`.

**Refresh cadence.** **Monthly** (cadence changed from quarterly in 2017).

**Format.** ZIP of TSVs (~40-300 MB depending on month; 2026-01 was 41 MB). Members: `sub.tsv, tag.tsv, dim.tsv, ren.tsv, cal.tsv, pre.tsv, num.tsv, txt.tsv` + readme + metadata.

**Schemas.**

```
sub.tsv:
  adsh  cik  name  sic  …  form  period  fy  fp  filed  accepted  prevrpt
  detail  instance  nciks  aciks  pubfloatusd  floatdate  floataxis  floatmems

num.tsv (extends fsds num.tsv with dimensional fields):
  adsh  tag  version  ddate  qtrs  uom  dimh  iprx  value
  footnote  footlen  dimn  coreg  durp  datp  dcml

txt.tsv (narrative text blocks — footnotes):
  adsh  tag  version  ddate  qtrs  iprx  lang  dcml  durp  datp
  dimh  dimn  coreg  escaped  srclen  txtlen  footnote  footlen
  context  value

dim.tsv (dimension hash → segments):
  dimhash  segments  segt

ren.tsv (rendering / report metadata):
  adsh  report  rfile  menucat  shortname  longname  roleuri  parentroleuri
  parentreport  ultparentrpt
```

**Coverage.** All 10-K / 10-Q with full XBRL detail INCLUDING textual notes blocks (revenue recognition narrative, legal proceedings, segment descriptions, …). This is the **only** SEC bulk feed that carries text-block iXBRL facts.

**Primary keys + dedup.** `adsh` per filing; `(adsh, tag, ddate, dimh)` for num/txt.

**Update semantics.** Snapshot per month. Within-quarter restatements appear in the next month's archive.

**Gotchas.**
- **txt.tsv carries text-block facts** (e.g. `us-gaap:RevenueRecognitionPolicyTextBlock`, `us-gaap:SegmentReportingDisclosureTextBlock`) — useful for thesis evidence extraction.
- Has the `dim.tsv` table to decompose `dimh` (dimension hash) back into the actual axis-member combinations.
- More expensive than fsds because of txt.tsv.

**eBull current use.** **NOT CONSUMED**. Companyfacts.zip covers numeric facts but NOT text blocks. **Medium-priority gap** — fsn is the canonical bulk source for `us-gaap:*TextBlock` concepts (legal proceedings, revenue policy, segment narrative). The thesis engine would benefit from indexing these.

---

## 3. Bulk archives published, NOT eligible for eBull

### 3.1 EDGAR Log File Data Sets

**Canonical URL.** `https://www.sec.gov/data-research/sec-markets-data/edgar-log-file-data-sets`. Files at `https://www.sec.gov/dera/data/Public-EDGAR-log-file-data/` (paywalled / archived; coverage ended 2017-06).

**Refresh cadence.** **Discontinued**. Last public dataset covers through June 2017. Coverage notice: "EDGAR log file data sets — Dec. 23, 2015". The publicly downloadable HTML index at `/files/edgar_logfiledata_thru_jun2017.html` is the last one.

**Format.** CSV (zipped).

**Coverage.** Per-IP HTTP access logs for SEC's EDGAR system. Useful for academic research on filing-disclosure-to-trade correlation.

**eBull current use.** **NOT CONSUMED**. Discontinued + irrelevant to eBull's investment workflow. **Not a gap.**

---

## 4. Reference JSON files (bandwidth-cheap, "bulk-adjacent")

| File | URL | Refresh | Size | Used by |
|---|---|---|---|---|
| `company_tickers.json` | `https://www.sec.gov/files/company_tickers.json` | Daily nightly | ~600 KB | `daily_cik_refresh` (Stage 6) |
| `company_tickers_exchange.json` | `https://www.sec.gov/files/company_tickers_exchange.json` | Daily nightly | ~750 KB | `daily_cik_refresh` Stage 7 (G8) |
| `company_tickers_mf.json` | `https://www.sec.gov/files/company_tickers_mf.json` | Daily nightly | ~3 MB | `mf_directory_sync` (S26) |
| 13F Official List | `https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt` | Quarterly | ~600 KB | `cusip_universe_backfill` (Stage 3) |
| Fail-to-Deliver | `https://www.sec.gov/files/data/fails-deliver-data/cnsfails*.zip` | Bi-monthly | ~5 MB each | **NOT consumed** |
| MIDAS market-quality stats | `https://www.sec.gov/data-research/sec-markets-data/market-structure` | Various | varies | **NOT consumed** |

**Coverage verified 2026-05-25 (live HEAD).**

```json
// company_tickers.json — ticker-grain as of 2026-07-22 re-fetch (10,419 rows /
// 8,014 unique CIKs / 1,463 multi-ticker CIKs; the earlier "CIK-grain, primary
// symbol" shape no longer holds — #2108)
{"0":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA CORP"}, ...}

// company_tickers_exchange.json (ticker-grain, share-class siblings split;
// same row cohort as company_tickers.json as of 2026-07-22)
{"fields":["cik","name","ticker","exchange"],
 "data":[[1045810,"NVIDIA CORP","NVDA","Nasdaq"], ...]}

// company_tickers_mf.json (classId-grain — mutual funds disjoint from above)
{"fields":["cik","seriesId","classId","symbol"],
 "data":[[2110,"S000009184","C000024954","LACAX"], ...]}
```

**Fail-to-Deliver** is the one item flagged "NOT consumed" with operator visibility — bi-monthly CSV listing securities with failed-to-deliver positions ≥ 10,000 shares. Already-tracked at #915 (FINRA bimonthly short interest) is a different feed; FTD is SEC's own. **Low-medium-priority gap** — relevant to short-squeeze / market-quality lens (#915 universe).

---

## 5. Per-form HTTP-only (no bulk archive exists)

These SEC form types have NO bulk archive. Per-filing HTTP is the only path. Listed here so the operator knows what genuinely requires HTTP and what doesn't.

| Form | Why no bulk | eBull path |
|---|---|---|
| **Schedule 13D / 13G** | SEC does not publish a bulk 13D/G dataset. XML mandate effective 2024-12-18; pre-mandate filings are HTML-only. | Per-CIK `/Archives/edgar/data/.../primary_doc.xml`. Manifest parser `sec_13dg.py` (#1129). |
| **DEF 14A / DEFA14A / DEFR14A** | No bulk; proxy statements are narrative HTML. No structured-XBRL mandate for the beneficial-ownership table. | Per-filing HTML scraper `def14a.py` (#1128). |
| **8-K** | No bulk; item codes are in submissions JSON but body HTML is per-filing. | Manifest parser `eight_k.py` (#1126). Item codes come from submissions.json. |
| **10-K / 10-Q (narrative)** | XBRL facts ARE in `companyfacts.zip` + `financial-statement-data-sets`. Narrative HTML body is NOT in any bulk. | `companyfacts.zip` for facts. `manifest_parsers/sec_10k.py` (#1152) for Item 1 business summary HTML. `sec_10q.py` is synth no-op (no narrative consumer in v1). |
| **N-CSR / N-CSRS** | Bulk-adjacent via `rr1` (risk/return summary section only). Full N-CSR including holdings attestation is HTML-only. | Manifest parser `sec_n_csr.py` (#1171). Fund-metadata path (expense ratio, NAV, returns) from iXBRL. |
| **Schedule 13E (going-private)** | No bulk. | Not consumed. |
| **Form 144 (insider preliminary)** | No bulk. | Not consumed (open in build-priorities). |
| **Form ADV (investment adviser)** | **DOES have a bulk archive at IAPD** (`https://www.adviserinfo.sec.gov/IAPD/IAPDFirmList.aspx`) — but this is at adviserinfo.sec.gov, NOT www.sec.gov. Separate publisher (Investment Adviser Public Disclosure). Bulk shape: ZIP of `IA_FIRM_SEC_Feed_*.xml` per day. | Not consumed. **Gap candidate**: would let eBull build an adviser-fee structure / RIA reference graph cheaper than per-CIK polling. |
| **ATS-N (alternative trading systems)** | No bulk; per-firm only. | Not consumed. |
| **17-H (broker-dealer risk)** | Confidential; not public. | n/a. |
| **485APOS / 485BPOS (post-effective amendments)** | Bulk-adjacent via `rr1`. Narrative HTML body is per-filing. | Not consumed. |
| **Atom `getcurrent`** | Per-request, ISO-8859-1 encoded. Layer 1 fast-lane (`sec_atom_fast_lane`). | Wired post-#1155. |
| **Daily-index `master.YYYYMMDD.idx`** | Reconciliation layer; daily file per quarter. Layer 2. | Wired post-#1155. |
| **Per-CIK `submissions/CIK*.json` LIVE** | Real-time API. Layer 3 per-CIK poll. | Wired post-#1155. |

---

## 6. Gap summary

Ranked by "wall-clock save per first-install" impact.

### 6.1 HIGH — bulk data sitting in `submissions.zip` re-fetched via HTTP

**Finding.** First-install Stage 16 (`sec_first_install_drain`) issues 11,205 fresh `data.sec.gov/submissions/CIK*.json` GETs for institutional-filer CIKs that ARE in `submissions.zip`. Stage 8's ingester skips them because they have no universe instrument. Both jobs share the same SEC budget.

**Affected jobs.**
- Stage 16 `sec_first_install_drain` (institutional-filer cohort): 11,205 CIKs × 1 GET each = 11,205 GETs at 7 req/s sustainable ≈ 27 minutes.
- Stage 16 deep-history `filings.files[]` walk: same CIK cohort × variable overflow pages.
- Stage 13 `sec_submissions_files_walk`: per-CIK overflow page HTTP that the bulk ZIP already carries.

**Estimated save.** ~30-45 minutes per first-install.

**Bulk-replaces-HTTP path.** Stage 7 already downloads `submissions.zip`. Stage 8 currently filters to in-universe CIKs only — relax that filter for the institutional-filer cohort (read submissions.json directly from the ZIP for any CIK in `institutional_filers`, `sec_nport_filer_directory`, `blockholder_filers`). The ZIP has the JSON; the in-memory CIK→JSON dict is the same shape as the per-CIK HTTP response.

**Status.** GAP. Top candidate for Stream-B-or-equivalent.

---

### 6.2 MEDIUM — `ncen_classifier` per-filing HTTP duplicates `ncen.zip`

**Finding.** `app/services/ncen_classifier.py` reads `is_etf / is_etmf / is_money_market / is_index / is_target_date` from per-filing `primary_doc.xml` via HTTP. The N-CEN quarterly bulk dataset carries the same flags in `FUND_REPORTED_INFO.tsv` (boolean columns `IS_ETF, IS_ETMF, IS_INDEX, IS_INTERVAL, IS_MONEY_MARKET, IS_TARGET_DATE, IS_UNDERLYING_FUND, IS_NON_DIVERSIFIED`). One 16 MB ZIP / quarter replaces ~5000 per-filing HTTP requests.

**Estimated save.** ~5-10 minutes per quarterly fund-classification refresh; near-zero bandwidth.

**Bulk-replaces-HTTP path.** Add `ncen_quarterly_dataset` to `build_bulk_archive_inventory` + a new ingester `app/services/sec_ncen_dataset_ingest.py`. Operator-visible signal: fund-classification flags populate without per-filing HTTP.

**Status.** GAP. Medium priority. Open ticket worth filing.

---

### 6.3 MEDIUM — N-CSR fund-metadata partially in `rr1`

**Finding.** `manifest_parsers/sec_n_csr.py` (#1171) per-filing-fetches iXBRL N-CSR HTML to extract per-(series, class) expense ratio, NAV, returns, portfolio turnover. The Mutual Fund Prospectus Risk/Return Summary `rr1` quarterly archive carries the EXACT SAME risk/return data structured per `(cik, series, class)` per filing.

**Estimated save.** Per-CSR fetch is rate-limited; bulk one-shot ~50 MB/quarter would carry ~1500 fund filings = saving ~15-20 minutes per quarter.

**Bulk-replaces-HTTP path.** Add `rr1` as an alternative source for `sec_n_csr` fund-metadata. Manifest parser would consult the bulk first; fall back to per-CSR iXBRL only for filings absent from `rr1`.

**Status.** GAP. Marginal — current per-CSR path already works. Worth filing as tech-debt.

---

### 6.4 LOW — `financial-statement-and-notes-data-sets` carries `*TextBlock` concepts

**Finding.** Companyfacts.zip only carries NUMERIC XBRL facts. Text-block facts (e.g. `us-gaap:LegalProceedingsTextBlock`, `us-gaap:RevenueRecognitionPolicyTextBlock`) are in `txt.tsv` of `financial-statement-and-notes-data-sets` ONLY. No other bulk SEC feed has them.

**Estimated save.** No wall-clock save vs current state — eBull doesn't read text blocks today. **New capability**, not optimisation. Useful for thesis-engine evidence extraction.

**Status.** GAP. Low priority. File when thesis engine specifically needs text-block evidence.

---

### 6.5 LOW — Fail-to-Deliver bi-monthly

**Finding.** SEC publishes bi-monthly FTD reports at `https://www.sec.gov/files/data/fails-deliver-data/cnsfails*.zip`. Disjoint from #915 (FINRA bimonthly short interest). Securities with ≥ 10,000 shares failed-to-deliver get a row per settlement date.

**Estimated save.** N/A — new feed.

**Status.** GAP. Low priority. File when short-squeeze / market-quality lens is in scope.

---

### 6.6 LOW — Form ADV bulk at adviserinfo.sec.gov

**Finding.** Investment adviser registrations (Form ADV) have a bulk feed at `adviserinfo.sec.gov` — separate publisher from www.sec.gov but same data-governance regime. Would let eBull build an RIA reference graph (adviser → AUM → fee structure → custodian) cheaper than per-CIK polling.

**Estimated save.** N/A — new feed.

**Status.** GAP. Low priority. File only if eBull adds an advisor-fee or RIA-coverage lens.

---

### 6.7 NOT GAPS — out-of-scope feeds

- N-MFP (money-market funds): not in tradable universe.
- VIP (variable insurance products): not in tradable universe.
- Form D (private offerings): out of scope.
- Reg A (small public offerings): out of scope (most don't trade).
- Form C (crowdfunding): out of scope.
- Transfer agent operational data: no operator-visible figure depends on it.
- EDGAR log files: discontinued post-2017-06.

---

## 7. Wiring summary — bulk vs HTTP per source

| Source | Bulk archive? | eBull bulk path | eBull HTTP path | Gap? |
|---|---|---|---|---|
| Submissions JSON | ✅ `submissions.zip` | Stages 7, 8 | Stage 13 + Stage 16 + manifest worker | ⚠️ §6.1 (institutional-filer skip) |
| Companyfacts XBRL | ✅ `companyfacts.zip` | Stages 7, 9, 25 | — | ✅ clean |
| 13F-HR | ✅ `form13f` ZIP | Stages 7, 10, 21 | manifest worker per-filing | ✅ clean |
| N-PORT-P | ✅ `nport` ZIP | Stages 7, 12, 23 | manifest worker per-filing | ✅ clean |
| Form 3/4/5 | ✅ `form345` ZIP | Stages 7, 11, 18-20 | manifest worker per-filing | ✅ clean |
| N-CEN | ✅ `ncen` ZIP | — | `ncen_classifier` per-filing | ⚠️ §6.2 |
| N-CSR | partial (rr1 carries risk/return) | — | `sec_n_csr.py` per-filing iXBRL | ⚠️ §6.3 |
| 10-K narrative (Item 1) | ❌ (XBRL facts in companyfacts, narrative is HTML-only) | — | `sec_10k.py` per-filing | ✅ no bulk to use |
| 10-K text blocks | ✅ `fsn` ZIP txt.tsv | — | not consumed | ⚠️ §6.4 |
| 10-Q narrative | ❌ | — | synth no-op (#1168) | ✅ no consumer |
| 8-K | ❌ | — | `eight_k.py` per-filing | ✅ no bulk available |
| 13D/G | ❌ | — | `sec_13dg.py` per-filing | ✅ no bulk available |
| DEF 14A | ❌ | — | `def14a.py` per-filing | ✅ no bulk available |
| Form 144 | ❌ | — | not consumed | ✅ no bulk available |
| Form ADV | ✅ at adviserinfo.sec.gov | — | not consumed | ⚠️ §6.6 (low priority) |
| Daily/full-index | ✅ `.idx` files | Layer 2 + Layer 4 | — | ✅ wired post-#1155 / G12 |
| Atom getcurrent | ✅ (treat as bulk-adjacent) | Layer 1 | — | ✅ wired post-#1155 |
| company_tickers*.json | ✅ JSON | Stage 6 + Stage 7 (G8) + S26 | — | ✅ clean |
| 13F Official List | ✅ TXT | Stage 3 | — | ✅ clean |
| FTD bi-monthly | ✅ ZIP | — | not consumed | ⚠️ §6.5 (low priority) |
| MMF N-MFP | ✅ `nmfp` ZIP | — | not consumed | ✅ out of scope |
| VIP | ✅ `vip` ZIP | — | not consumed | ✅ out of scope |
| BDC | ✅ `bdc` ZIP | — | not consumed | ⚠️ low priority |
| Form D | ✅ `formd` ZIP | — | not consumed | ✅ out of scope |
| Reg A | ✅ `rega` ZIP | — | not consumed | ✅ out of scope |
| Crowdfunding | ✅ `cf` ZIP | — | not consumed | ✅ out of scope |
| Transfer Agent | ✅ `ta` ZIP | — | not consumed | ✅ out of scope |
| RR1 (mutual fund risk/return) | ✅ `rr1` ZIP | — | covered via N-CSR per-filing | ⚠️ §6.3 (marginal) |
| FSDS quarterly | ✅ `fsds` ZIP | — | redundant with companyfacts | ✅ not needed |
| FSN monthly | ✅ `fsn` ZIP | — | not consumed (text blocks only) | ⚠️ §6.4 (low priority) |
| EDGAR log files | ⚠️ discontinued | — | n/a | ✅ n/a |

---

## 8. Provenance

Every bulk-archive URL in this document was probed with `curl -A 'eBull research luke.bradford@hotmail.co.uk'` on 2026-05-25 and confirmed HTTP 200. Every inner-TSV header table was extracted directly from the actual ZIP downloaded from `www.sec.gov` on the same date. SEC publication-page text excerpts (cadence, scope, format) came from `https://www.sec.gov/data-research/sec-markets-data` and each per-dataset sub-page under that path.

Cross-references:
- `.claude/skills/data-sources/sec-edgar.md` — endpoint inventory authoritative source.
- `.claude/skills/data-engineer/etl-endpoint-coverage.md` — current per-source wiring.
- `.claude/skills/data-sources/edgartools.md` — library reference for per-filing structured parsers.
- `app/services/sec_bulk_download.py` — the bulk-download lane.
- `app/services/sec_bulk_refresh.py` — the daily HEAD-and-reuse contract.
- `docs/settled-decisions.md` — "Bulk archive reuse keyed on SEC ETag + SHA-256".
