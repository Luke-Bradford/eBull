# SEC EDGAR — source-of-truth reference

> Read this before adding any SEC ingest job, parser, or identifier resolver. eBull's data integrity depends on treating SEC formats as the source-of-truth, not guessing them. Every operator-visible figure traces back to a specific endpoint + format documented here.

## Executive cheat sheet

- **Two hostnames.** `data.sec.gov` serves JSON APIs (submissions, companyfacts, companyconcept, frames). `www.sec.gov` serves bulk archives, full-text indexes, primary documents under `/Archives/edgar/...`, JSON ticker reference files under `/files/...`, cgi-bin Atom feeds, daily/full-index `.idx` files. Don't confuse them.
- **Rate limit: 10 req/s per IP, regardless of machine count.** Source: <https://www.sec.gov/about/developer-resources>.
- **User-Agent required.** Format `<Name> <email>`. Missing or generic UA = immediate 403.
- **Bulk over per-filing whenever possible.** `submissions.zip` (~1.54 GB) and `companyfacts.zip` (~1.38 GB) rebuild nightly ~03:00 ET.
- **Conditional fetch supported.** Many endpoints emit `Last-Modified` / `ETag`. Always send `If-Modified-Since`.
- **Three-tier polling.** Hot (Atom getcurrent) / Warm (daily-index) / Cold (per-CIK submissions JSON).
- **Identifiers, never names.** TSLA = `TESLA INC` in SEC, `Tesla, Inc.` in broker. Fuzzy-name match is forbidden — use CIK / CUSIP bridges.

## 1. Endpoints

### Reference / canonical bridges

| Endpoint | URL | Refresh | Use For |
|---|---|---|---|
| Company tickers | `https://www.sec.gov/files/company_tickers.json` | Daily nightly | Ticker → CIK bridge (~10k operating-co rows) |
| Company tickers (exchange) | `https://www.sec.gov/files/company_tickers_exchange.json` | Daily nightly | + exchange (Nasdaq / NYSE / Cboe / OTC) |
| Mutual-fund tickers | `https://www.sec.gov/files/company_tickers_mf.json` | Daily nightly | ~28k rows; carries `seriesId` + `classId` |
| 13F Official List | `https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt` | Quarterly ~2 weeks post-quarter | CUSIP → issuer-name (~24k rows). Authoritative CUSIP/CIK bridge for institutional ownership. |

`company_tickers.json` shape:
```json
{"0": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"}}
```
`cik_str` is **integer** in JSON, **not zero-padded**. Always pad to 10 digits with `f"CIK{cik:010d}"` before constructing API URLs.

**Coverage gap**: `company_tickers.json` excludes pink-sheet/OTC, foreign-without-ADR, warrant-only, preferred-only. Layer `company_tickers_exchange.json` and `company_tickers_mf.json` to close gaps. eBull pattern lives in `daily_cik_refresh` (scheduled job, see `app/workers/scheduler.py`) calling `app/services/filings.py::upsert_cik_mapping`. The earlier `app/services/cik_discovery.py` helper was deleted in #1091.

### JSON APIs (`data.sec.gov`)

| Endpoint | URL | Refresh | Use For |
|---|---|---|---|
| Submissions per CIK | `https://data.sec.gov/submissions/CIK{padded}.json` | Real-time, <1s | Per-CIK 1000-most-recent filings + history pointers |
| Submissions overflow | `https://data.sec.gov/submissions/CIK{padded}-submissions-{NNN}.json` | Real-time | Older filings, paginated |
| Companyfacts | `https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json` | Real-time, <1min | All XBRL concepts for a CIK |
| Companyconcept | `https://data.sec.gov/api/xbrl/companyconcept/CIK{padded}/{taxonomy}/{tag}.json` | Real-time | One XBRL tag (smaller payload) |
| Frames | `https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json` | Real-time | Cross-sectional one-fact-per-filer |

**Submissions JSON top-level**: `cik, entityType, sic, name, tickers, exchanges, ein, lei, fiscalYearEnd, formerNames, addresses, filings`.

`filings.recent` is **columnar** — parallel arrays each capped at **1000 most-recent OR ≥ 1 year** (whichever yields more). Older history lives in `filings.files[]` pointer array. Always check `files` and recurse:

```python
def fetch_all_filings(cik_padded: str):
    primary = http_get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json")
    yield from _rows(primary["filings"]["recent"])
    for ptr in primary["filings"].get("files", []):
        page = http_get(f"https://data.sec.gov/submissions/{ptr['name']}")
        yield from _rows(page)
```

Pattern at [app/services/institutional_holdings.py:189-220](../../../app/services/institutional_holdings.py#L189-L220), rebuild at [app/jobs/sec_rebuild.py:335](../../../app/jobs/sec_rebuild.py#L335).

`recent` columnar keys: `accessionNumber, filingDate, reportDate, acceptanceDateTime, act, form, fileNumber, filmNumber, items, core_type, size, isXBRL, isInlineXBRL, isXBRLNumeric, primaryDocument, primaryDocDescription`. **All arrays must be same length, aligned by index** — pull row `i` by reading `recent[k][i]` for every `k`.

### Bulk archives

| Endpoint | Refresh | Use For |
|---|---|---|
| Submissions bulk ZIP | `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip` | Nightly ~03:00 ET | Initial-install drain (~1.54 GB) |
| Companyfacts bulk ZIP | `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` | Nightly ~03:00 ET | Initial fundamentals drain (~1.38 GB) |
| Form 13F dataset | `https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets` | Quarterly | All 13F holdings per quarter |
| N-PORT dataset | `https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets` | Quarterly | Mutual-fund/ETF holdings |
| Insider dataset | `https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets` | Quarterly | All Form 3/4/5 |
| Financial-statement dataset | `https://www.sec.gov/dera/data/financial-statement-data-sets.html` | Quarterly | XBRL extract (10-K / 10-Q) |

### Indexes + Atom feeds

| Endpoint | URL | Refresh | Use For |
|---|---|---|---|
| Full-index quarterly | `https://www.sec.gov/Archives/edgar/full-index/{YYYY}/QTR{n}/master.idx` | Weekly Sat (PAC rebuild) | Cross-quarter discovery |
| Daily-index | `https://www.sec.gov/Archives/edgar/daily-index/{YYYY}/QTR{n}/master.{YYYYMMDD}.idx` | Nightly ~22:00 ET | Yesterday's filings |
| Atom getcurrent | `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&output=atom` | Live | Hot polling for current-day filings |
| Atom getcompany | `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form}&output=atom` | Live | Per-CIK Atom alternative |
| Filing-folder manifest | `https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_dashes}/index.json` | Once at filing | Enumerate filing exhibits |

**Atom `getcurrent` is `ISO-8859-1`-encoded**, not UTF-8. Decode accordingly.

`master.idx` is pipe-delimited:
```
CIK|Company Name|Form Type|Date Filed|Filename
1000045|OLD MARKET CAPITAL Corp|15-12G/A|2026-01-02|edgar/data/1000045/0001437749-26-000015.txt
```

`form.idx` is fixed-width at offsets 0,12,74,86,98.

## 2. File formats

### 2.1 Form 13F-HR INFOTABLE schema

Source: <https://www.sec.gov/files/form_13f.pdf> §5.7.

| Field | Type | Notes |
|---|---|---|
| `ACCESSION_NUMBER` | VARCHAR2(25) | filer CIK + yy + seq |
| `INFOTABLE_SK` | NUMBER | row surrogate |
| `NAMEOFISSUER` | VARCHAR2(200) | |
| `TITLEOFCLASS` | VARCHAR2(150) | |
| `CUSIP` | CHAR(9) | |
| `VALUE` | NUMBER | **Unit-cutover gotcha — see §3.1** |
| `SSHPRNAMT` | NUMBER | shares OR principal |
| `SSHPRNAMTTYPE` | VARCHAR2(10) | `SH` or `PRN` (uppercase) |
| `PUTCALL` | VARCHAR2(10) | `Put` / `Call` (capitalised) or empty |
| `INVESTMENTDISCRETION` | VARCHAR2(10) | `SOLE` / `DFND` / `OTR` |
| `OTHERMANAGER` | VARCHAR2(100) | comma-sep seq numbers |
| `VOTING_AUTH_SOLE` / `_SHARED` / `_NONE` | NUMBER | voting authority |

**SSHPRNAMTTYPE = PRN** rows hold bond principal **in dollars**, not share counts. Filter `WHERE SSHPRNAMTTYPE = 'SH'` before any share aggregation. PRN belongs to a separate fixed-income rollup if surfaced at all. Pattern at [app/services/sec_13f_dataset_ingest.py:307-315](../../../app/services/sec_13f_dataset_ingest.py#L307-L315).

13F filed within **45 days after each calendar quarter end**.

### 2.2 N-PORT-P XML schema

Source: <https://www.sec.gov/info/edgar/specifications/form-n-port-xml-tech-specs.htm>.

```xml
<edgarSubmission xmlns="http://www.sec.gov/edgar/nport">
  <headerData><submissionType>NPORT-P</submissionType></headerData>
  <formData>
    <genInfo>
      <regCik>...</regCik><regLei>...</regLei>
      <seriesId>S000001234</seriesId><seriesName>...</seriesName>
      <repPdEnd>2026-03-31</repPdEnd><repPdDate>2026-03-31</repPdDate>
    </genInfo>
    <invstOrSecs>
      <invstOrSec>
        <name>APPLE INC</name><lei>HWUPKR0MPOU8FGXBT394</lei>
        <cusip>037833100</cusip>
        <balance>123456.000000</balance>
        <units>NS</units>          <!-- NS=shares; PA=principal; OU=other -->
        <curCd>USD</curCd>
        <valUSD>34567890.12</valUSD>
        <pctVal>2.345</pctVal>
        <payoffProfile>Long</payoffProfile>
        <assetCat>EC</assetCat>     <!-- equity-common, debt, etc. -->
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
    </invstOrSecs>
  </formData>
</edgarSubmission>
```

Critical fields: `cusip` (9 char), `lei` (20 char), `valUSD` (USD-converted regardless of `curCd`), `pctVal` (decimal — `2.345` = 2.345%), `balance` + `units` (same SH-vs-PRN trap as 13F: branch on `units='NS'`).

**Fund hierarchy**: filings are at the **trust** CIK level; each holding belongs to a **series** (`S000123456`); each series has multiple **share classes** (`C000234567`). For ownership rollup at operating-issuer level, aggregate `valUSD` across funds without double-counting fund-of-funds. Aggregate by `(seriesId, issuerCusip)`, NOT by classId — share classes share the same portfolio.

`<invstOrSec>` repeatability raised from 1000 → 500,000 — long lists are valid. eBull's parser is at [app/services/n_port_ingest.py](../../../app/services/n_port_ingest.py) (stdlib `xml.etree.ElementTree`, #917 closeout).

### 2.3 Form 3/4/5 — Section 16 insider transactions

XML root: `<ownershipDocument>`. **Element-wrapping idiom**: every leaf value lives inside a `<value>` child so SEC can attach a peer `<footnoteId>`. `findtext("transactionShares")` returns `None` — must descend to `transactionShares/value`.

```xml
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-04-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>...</rptOwnerCik><rptOwnerName>...</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>true</isOfficer><officerTitle>...</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionCoding>
        <transactionCode>M</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1717</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
```

**Transaction code reference** (Form 4 General Instructions):

| Code | Meaning |
|---|---|
| P | Open-market or private purchase |
| S | Open-market or private sale |
| A | Grant / award / RSU vest |
| D | Sale or transfer back to company |
| F | Net-settlement (tax / exercise withhold) |
| M | Exercise / conversion of derivative |
| C | Conversion of derivative |
| G | Bona fide gift |
| K | Equity swaps / hedging |
| X | Exercise of in/at-the-money derivative |
| O | Exercise of out-of-the-money derivative |
| J | Other (footnote required) |
| U | Disposition pursuant to tender offer |

`directOrIndirectOwnership`: `D` = Direct, `I` = Indirect. **Both surface separately** — Section 16 ownership totals must aggregate D + I separately because the FILER label "owns" both. They are NOT double-counts. This is what made JPM insider rollup go 1.29% → 6.16% post-#905 (`project_905_rollup_cutover_done.md`).

### 2.4 Schedule 13D / 13G — beneficial ownership

XML mandate **since 2024-12-18**. Current EDGAR XML technical spec revision is **2.2** (2026-03-16) — verify against `https://www.sec.gov/edgar/filer-information/current-edgar-technical-specifications` before relying on the schema. Pre-mandate filings are HTML/text — no `primary_doc.xml` exists; legacy coverage is lower-fidelity unless you write a parallel HTML extractor.

Sample reporting-person block:
```xml
<reportingPersonInfo>
  <reportingPersonCIK>0001161286</reportingPersonCIK>
  <reportingPersonName>...</reportingPersonName>
  <memberOfGroup>a</memberOfGroup>
  <citizenshipOrOrganization>FL</citizenshipOrOrganization>
  <soleVotingPower>2121212.00</soleVotingPower>
  <sharedVotingPower>2121212.00</sharedVotingPower>
  <soleDispositivePower>212121.00</soleDispositivePower>
  <sharedDispositivePower>212121.00</sharedDispositivePower>
  <aggregateAmountOwned>21222121.00</aggregateAmountOwned>
  <isAggregateExcludeShares>N</isAggregateExcludeShares>
  <percentOfClass>2.7</percentOfClass>
  <typeOfReportingPerson>BD</typeOfReportingPerson>
</reportingPersonInfo>
```

A single accession can carry up to 100 reporting persons (joint filings). 13G uses `classPercent` instead of `percentOfClass` and includes `classOwnership5PercentOrLess` flag (signals when filer dropped under 5%).

### 2.5 Date formats

| Format | Example | Where used |
|---|---|---|
| ISO 8601 `YYYY-MM-DD` | `2026-04-15` | submissions JSON (`filingDate`, `reportDate`), companyfacts (`start`, `end`, `filed`), Form 4 XML, 13D/G XML, Atom feed `<filing-date>`, full-index `master.idx` |
| ISO 8601 with timestamp | `2026-04-15T20:03:51.000Z` (UTC) | submissions JSON `acceptanceDateTime` |
| ISO 8601 with TZ offset | `2026-05-08T15:26:04-04:00` (ET) | Atom feed `<updated>` |
| `DD-MON-YYYY` (uppercase) | `14-NOV-2025` | **All bulk dataset TSV files** — 13F, NPORT, Insider, financial-statement |
| `DD-MMM-YYYY` (mixed case) | `14-Nov-2025` | Some bulk archives with locale-aware writers |
| `MM/DD/YYYY` | `06/07/2023` | 13D/G XML `<dateOfEvent>` |
| `YYYYMMDD` | `20061207` | Daily `Feed/` filenames |
| `MMDD` | `0926` | submissions JSON `fiscalYearEnd` (no year) |

**Always parse with try/except across formats relevant to the source.** Never "ISO 8601 only" — that is the #1 cause of silent ingest gaps. Pattern:

```python
def _parse_sec_date(s: str) -> date:
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unrecognised SEC date: {s!r}")
```

## 3. Identifiers

### 3.1 CIK (Central Index Key)

- 10-digit zero-padded number assigned by EDGAR when a filer registers.
- Identifies the **filer** (issuer / fund family / individual insider / institutional manager). **Not the security.**
- **Never recycled.** Renames preserve the CIK (AAPL = `APPLE COMPUTER INC`/`/ FA`/`APPLE INC`, all 0000320193).
- Padding: API + archive paths require 10-digit pad. JSON payloads carry it as integer (no padding) in some endpoints (`cik_str` in `company_tickers.json`) and as string in others. Normalise to int internally; pad to string at URL boundary.

### 3.2 CUSIP

- 9-character alphanumeric (8 + check digit). Identifies a **security**, not an issuer.
- Foreign issuers often have a **CINS** (CUSIP International Numbering System) — same shape, starts with letter (e.g. `G0R21F121` for Cayman Islands).
- Changes on corporate actions: stock splits, ticker changes, M&A, redomiciles. Issuer keeps CIK; security CUSIP moves.
- **Class disambiguation**: GOOGL = `02079K305` (Class A), GOOG = `02079K107` (Class C). Both CIK 1652044 (Alphabet). Aggregating without share-class CUSIP collapses two distinct holdings.
- Source of authoritative CUSIP→CIK mapping: 13F Official List (per-quarter): `https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt`. Format: fixed-width, columns `CUSIP NO.` (9 char) | `ISSUER NAME` | `ISSUER DESCRIPTION` (`SHS`, `CALL`, `PUT`, `UNIT`, `*W EXP`) | `STATUS`. Each issuer has multiple rows (common, warrant, unit, options-call, options-put). Filter for shares means matching description like `SHS`, `COMMON`, `COM`.

### 3.3 Accession number

Two interchangeable shapes:
- **With dashes**: `0001193125-26-214458` (20 chars). Used in submissions JSON, Atom, dataset ZIPs.
- **Without dashes**: `000119312526214458` (18 chars). Used in `/Archives/edgar/data/{cik_int}/{acc_no_dashes}/...` paths.

Conversion: `str.replace("-", "")`. **The first 10 digits of an accession number are the FILER CIK that submitted, NOT the issuer CIK.**

Archive-URL CIK varies by form type:
- **Form 4 / Form 3 / Form 5** — `/Archives/edgar/data/{ISSUER_CIK}/{acc_no_dashes}/...`. Insider Form 4 filings for AAPL are submitted by various filer-agent CIKs but stored under Apple's CIK 0000320193.
- **13F-HR / 13G/D / N-PORT-P** — `/Archives/edgar/data/{FILER_CIK}/{acc_no_dashes}/...`. The filing-manager / blockholder / fund-trust IS the filer; there is no separate "issuer" in those forms. eBull's 13F builder uses the filer CIK at [app/services/sec_13f_dataset_ingest.py:334](../../../app/services/sec_13f_dataset_ingest.py#L334).

### 3.4 LEI (Legal Entity Identifier)

20-character alphanumeric ISO 17442 code. Required on N-PORT, N-CSR, N-MFP, certain swap reports, and (since 2023-01-03) on Form 13F. Resolve via GLEIF API: `https://api.gleif.org/api/v1/lei-records/{lei}`.

### 3.5 Series ID / Class ID — fund hierarchy

- `seriesId`: `S` + 9 digits (e.g. `S000123456`) — one per fund within a trust.
- `classId`: `C` + 9 digits — share classes per series (Investor / Institutional / Retirement).
- Mutual-fund tickers map to **classId**, not seriesId. Two share classes share the same holdings → aggregate by seriesId, not classId.
- Source: `company_tickers_mf.json` carries ticker → seriesId → classId chain.

## 4. Rate limits + access discipline

### Official limit (verbatim from SEC)

> Current max request rate: **10 requests/second**. To ensure everyone has equitable access to SEC EDGAR content, please use efficient scripting. Download only what you need and please moderate requests to minimize server load.
>
> Current guidelines limit each user to a total of **no more than 10 requests per second, regardless of the number of machines used to submit requests**.

The "regardless of the number of machines" phrasing means horizontal scaling does not buy headroom — the budget is per-User-Agent identity. eBull treats 10 r/s as a global semaphore across every ingest job (#728). Empirical sustained ceiling is **5–7 r/s** to avoid transient 429/503; pattern at [app/services/sec_pipelined_fetcher.py:43](../../../app/services/sec_pipelined_fetcher.py#L43).

### User-Agent header

Required format: `<Name> <email>`. Email must be syntactically valid and routable. eBull config at [app/config.py:29](../../../app/config.py#L29) (`sec_user_agent`). **Default `eBull dev@example.com` is unacceptable for production** — every operator install must override.

### What gets you blocked

- Sustained > 10 r/s for any rolling 1-second window — soft block (403, recovers).
- > 10 r/s for several minutes — IP rate-limit page for 10–30 min.
- Repeated soft-blocks — IP ban until manual review.
- Missing/generic UA (`python-requests/2.x`, `curl/8.x`) — **immediate 403**.
- Crawling same URL hundreds of times — flagged as botnet even under 10 r/s.

### Strategies

1. **Bulk archives over per-filing scrapes.** `submissions.zip` + `companyfacts.zip` replace ~10k requests with 1.
2. **`If-Modified-Since` against `Last-Modified`** for any per-CIK or per-filing endpoint. Server returns 304 (no body) for unchanged. Pattern at [app/providers/implementations/sec_edgar.py:497-529](../../../app/providers/implementations/sec_edgar.py#L497-L529).
3. **`If-None-Match` against `ETag`** for archive files. Frankfurter pattern at [app/providers/implementations/frankfurter.py:98-144](../../../app/providers/implementations/frankfurter.py#L98-L144) is the cleanest in-repo template.
4. **Three-tier polling**:
   - **Hot**: Atom `getcurrent` for 8-K. Poll every 5–10 min during business hours.
   - **Warm**: `daily-index/master.{YYYYMMDD}.idx` early-morning batch.
   - **Cold**: per-CIK submissions JSON re-pull weekly or per-event.
5. **Cache fetched bytes** in `raw_filings` table so re-wash is local. [app/services/raw_filings.py:11](../../../app/services/raw_filings.py#L11).
6. **Backoff on 429 / 503**. Exponential, max 8 attempts. Many transient blocks clear within 60s.

## 5. Identity resolution + canonical mapping

### CIK → ticker bridge

**Source**: `company_tickers.json` primary, `company_tickers_exchange.json` for exchange context. Refresh daily.

Pattern in the canonical `daily_cik_refresh` scheduled job (`app/workers/scheduler.py`) calling `app/services/filings.py::upsert_cik_mapping`:
1. Pull both JSONs.
2. Build CIK-keyed dict; canonical wins on collision.
3. For each ticker store CIK + exchange + corporate name.
4. Persist with watermark via `external_identifiers`.

(The earlier `app/services/cik_discovery.py` helper was deleted in #1091; it had divergent ON CONFLICT semantics that flapped CIK ownership for share-class siblings — see #1094 / #1102 for the share-class fix.)

**Edge cases**:
- Multi-class issuers (GOOGL/GOOG): both share-class tickers map to same CIK. Store all rows.
- Renames: `formerNames` in submissions JSON gives historical timeline. For back-filling 13F holdings, resolve name-as-of-filing-date.
- Delisted: no longer in `company_tickers.json` but CIK persists. Use submissions JSON directly to detect "no current ticker".
- Foreign without ADR: CIK exists but `tickers` array empty. Look up by name in 13F Official List (CUSIP-keyed).

### CUSIP → CIK bridge

**Source**: SEC Official 13(f) List (quarterly).

Authoritative because:
- Enumerates every CUSIP that institutional managers may hold.
- Issuer-name column matches EDGAR-canonical name → derivable to CIK via `company_tickers.json`.
- Includes warrants / units / preferred / depositary receipts as separate CUSIPs under the same issuer name.

eBull imports into `sec_reference_documents` (sql/121); fuzzy threshold 0.92 on issuer name (PR #927). Coverage gate: 7.4% on dev as of #914 vs 80% target.

**Limitations**: pink-sheet/OTC absent. ADRs sometimes appear under both local-share CUSIP and depositary CUSIP. Quarterly cadence means new IPOs miss bridge for up to 90 days.

### Why fuzzy match is wrong

TSLA: broker = `Tesla, Inc.`; SEC = `TESLA INC`; historical = `Tesla Motors, Inc.`. Trigram fuzzy match might pull `TESLA SECURITIES TRUST` (totally separate filer). **Fuzzy match is a code-smell — replace with explicit CIK/CUSIP bridge.** When unavoidable, bound threshold ≥ 0.92 and gate on coverage.

### Tombstones / superseded filers

Mark CIK tombstoned when:
1. No longer in any current canonical list AND no filings ≥ 18 months.
2. Form 15 (notice of termination of registration) filed.
3. `company_tickers.json` drop after merger/acquisition overlap window.

**Do NOT delete historical observations** — they remain valid for as-of queries. Per-CIK refresh stops; observations stay.

## 6. Reference implementations

| Library | Use For | License | Maintenance |
|---|---|---|---|
| `edgartools` | Structured parsers (13F, 3/4/5, 13D/G, N-PORT, XBRL) | MIT | Very active (~2-3 patches/week) |
| `datamule` | Bulk-download throughput, AWS-mirrored archives | MIT | Active |
| `secedgar` | Light filing crawler, mostly superseded | Apache-2.0 | Last release 2025-05 |
| Direct `httpx` | Stable endpoints (companyfacts, master.idx, submissions JSON) | n/a | Always works |

See `.claude/skills/data-sources/edgartools.md` for full edgartools reference.

## 7. Gotchas

### 7.1 13F VALUE unit cutover (2023-01-03)

EDGAR Release 22.4.1 amended Form 13F to "the value to the nearest dollar" (was thousands). **The dataset PDF still says "(x$1000)" because SEC has not amended the codebook — trust the rule, not the codebook.**

Branch on **filing date** (not period end — some pre-cutover-period filings were re-filed after cutover and use dollars):

```python
_VALUE_DOLLARS_CUTOVER = date(2023, 1, 3)
if filed_at.date() >= _VALUE_DOLLARS_CUTOVER:
    value_dollars = value_raw  # already dollars
else:
    value_dollars = value_raw * 1000
```

Pattern at [app/services/sec_13f_dataset_ingest.py:316-326](../../../app/services/sec_13f_dataset_ingest.py#L316-L326). SUMMARYPAGE.TABLEVALUETOTAL **also** flips on this date.

### 7.2 13F PRN rows are bond principals

Filter `WHERE SSHPRNAMTTYPE = 'SH'` before any share aggregation. PRN rows hold dollar principal of bonds — issuer name might still be familiar (e.g. `APPLE INC` for AAPL bonds). PR #1054 found 20k PRN rows in 2026Q1 alone.

### 7.3 DD-MON-YYYY dates in bulk archives

`datetime.fromisoformat("15-JAN-2026")` raises `ValueError`. Pipeline silently drops 100% of rows. Workaround: `_parse_sec_date` pattern in §2.5.

### 7.4 Submissions JSON `recent` cap

Max 1000 entries OR ≥ 1 year. Always check `filings.files[]` and recurse. Pattern in §1 above.

### 7.5 Companyfacts can have null/empty units

`units["USD"][0]` may KeyError or IndexError on small/recently-listed issuers. Defensive iteration:

```python
for tag, concept in facts.get("us-gaap", {}).items():
    units = concept.get("units") or {}
    for unit_name, rows in units.items():
        for row in rows or []:
            ...
```

### 7.6 ZIP archive entry ordering not guaranteed

`for entry in zf.namelist()` may give wrong order. Always look up by name:
```python
with zipfile.ZipFile(path) as zf:
    submission = zf.read("SUBMISSION.tsv")
    infotable = zf.read("INFOTABLE.tsv")
```

### 7.7 13D/G coverage cliff at 2024-12-18

Pre-mandate filings are HTML/text — no `primary_doc.xml`. Filter ingest by filing date or accept lower-fidelity historical coverage.

### 7.8 `acceptanceDateTime` is UTC despite ET filing windows

SEC's "5:30 p.m. ET" cutoff = **22:30 UTC (EST, UTC-5)** or **21:30 UTC (EDT, UTC-4)**. Convert to ET via `zoneinfo.ZoneInfo("America/New_York")` before applying SEC's day boundary.

### 7.9 `getcurrent` Atom feed is `ISO-8859-1`

Aggressive UTF-8 decoding mojibakes filers with accented names. Use the encoding declared in the XML prolog or feed lxml the raw bytes.

### 7.10 `<value>`-wrapping in Form 4 / 13D/G

`findtext("transactionShares")` returns None — descend to `transactionShares/value` (the `<value>` child wrapper exists so SEC can attach a sibling `<footnoteId>`).

### 7.11 `data.sec.gov` does not support CORS

Front-end JavaScript cannot directly call `data.sec.gov` — must proxy through eBull's backend. Affects any "fetch SEC live in the browser" idea.

### 7.12 Tickers are uppercase but broker may return mixed case

Submissions JSON returns `["AAPL"]`. eToro historically surfaced `"Aapl"`. Always uppercase before equality-match.

### 7.13 Submissions JSON `tickers` array can be empty

For delisted / foreign-only / fund-family filers, `tickers: []`. Check length before indexing.

### 7.14 Mutual fund share classes share CUSIP at series level

Vanguard 500 Index has VFINX (Investor) + VFIAX (Admiral). Both share classes hold the same portfolio. Aggregating by classId double-counts. Aggregate by `(seriesId, issuerCusip)`.

### 7.15 Multiple INFOTABLE rows for same `(NAMEOFISSUER, CUSIP)` per accession

A manager can submit multiple rows per security (per share class / per discretion bucket / per managed sub-fund). Aggregating "manager X's AAPL position" requires summing across rows. PR #1054 caught this.

## 8. Operator checklist for new SEC integrations

Before writing code that hits SEC EDGAR:

1. ✅ Identify canonical endpoint (smallest payload, right refresh cadence).
2. ✅ Pin User-Agent to `{operator name} {operator email}`. Verify config flows through every HTTP call site.
3. ✅ Determine refresh tier (hot / warm / cold) and map to cron cadence.
4. ✅ Decide if bulk archive can replace per-filing fetches for initial backfill.
5. ✅ Implement conditional fetch (`If-Modified-Since` / `If-None-Match`) where headers are published.
6. ✅ Implement tolerant date parsing (ISO 8601 + `DD-MON-YYYY`) for any field touching a bulk archive.
7. ✅ Implement branch-on-type (INFOTABLE SH vs PRN, NPORT NS vs PA, Form 4 D vs I).
8. ✅ Implement VALUE unit cutover (date(2023,1,3) by filing date) for any 13F-derived figure.
9. ✅ Use explicit identifier bridges (`company_tickers.json`, 13F Official List, GLEIF). Never fuzzy-name match without bound + coverage gate.
10. ✅ Cache fetched bytes in `raw_filings` so re-wash is local.
11. ✅ Smoke-test against canonical 5-instrument panel: AAPL, GME, MSFT, JPM, HD. Verify operator-visible figure on live endpoint after backfill (CLAUDE.md ETL clauses 8-12).

## 9. eBull-internal entry points

Where this knowledge already lives in code:

| Concern | File |
|---|---|
| CIK / ticker discovery | `daily_cik_refresh` scheduled job in [app/workers/scheduler.py](../../../app/workers/scheduler.py) + [app/services/filings.py](../../../app/services/filings.py)::`upsert_cik_mapping` |
| 13F XML per-filing parse | [app/providers/implementations/sec_13f.py](../../../app/providers/implementations/sec_13f.py) (EdgarTools, #925) |
| 13F dataset bulk TSV | [app/services/sec_13f_dataset_ingest.py](../../../app/services/sec_13f_dataset_ingest.py) |
| 13F filer directory walk | [app/services/sec_13f_quarterly_sweep.py](../../../app/services/sec_13f_quarterly_sweep.py) |
| 13F Official List | [app/services/sec_13f_securities_list.py](../../../app/services/sec_13f_securities_list.py) |
| Submissions JSON walker | [app/providers/implementations/sec_submissions.py](../../../app/providers/implementations/sec_submissions.py) |
| Submissions overflow paging | [app/jobs/sec_rebuild.py:335](../../../app/jobs/sec_rebuild.py#L335) |
| Companyfacts ingest | [app/providers/implementations/sec_fundamentals.py](../../../app/providers/implementations/sec_fundamentals.py), [app/services/fundamentals.py](../../../app/services/fundamentals.py) |
| Conditional fetch | [app/providers/implementations/sec_edgar.py:497-529](../../../app/providers/implementations/sec_edgar.py#L497-L529) |
| Daily-index walker | [app/providers/implementations/sec_daily_index.py](../../../app/providers/implementations/sec_daily_index.py) |
| Atom getcurrent | [app/providers/implementations/sec_getcurrent.py](../../../app/providers/implementations/sec_getcurrent.py) |
| N-PORT XML parse + ingest | [app/services/n_port_ingest.py](../../../app/services/n_port_ingest.py) (stdlib `xml.etree.ElementTree`, #917) |
| N-PORT dataset bulk | [app/services/sec_nport_dataset_ingest.py](../../../app/services/sec_nport_dataset_ingest.py) |
| N-CEN classifier | [app/services/ncen_classifier.py](../../../app/services/ncen_classifier.py) |
| Form 4 insider | [app/services/insider_transactions.py](../../../app/services/insider_transactions.py), [app/services/sec_insider_dataset_ingest.py](../../../app/services/sec_insider_dataset_ingest.py) |
| 13D/G blockholders | [app/services/blockholders.py](../../../app/services/blockholders.py), [app/providers/implementations/sec_13dg.py](../../../app/providers/implementations/sec_13dg.py) |
| Filing-folder manifest | [app/services/filing_documents.py](../../../app/services/filing_documents.py) |
| Bulk download orchestrator | [app/services/sec_bulk_download.py](../../../app/services/sec_bulk_download.py) |
| Raw-filing cache | [app/services/raw_filings.py](../../../app/services/raw_filings.py) |
| Schedulers (conditional fetch) | [app/workers/scheduler.py:1460-1574](../../../app/workers/scheduler.py#L1460-L1574) |

## 10. Sources

- SEC accessing-edgar-data: <https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data>
- SEC EDGAR APIs: <https://www.sec.gov/search-filings/edgar-application-programming-interfaces>
- SEC developer resources: <https://www.sec.gov/about/developer-resources>
- Form 13F dataset PDF: <https://www.sec.gov/files/form_13f.pdf>
- Form 13F XML information-table guide: <https://www.sec.gov/files/13f-xml-information-table.pdf>
- Form 13F FAQ (rounding/units): <https://www.sec.gov/rules-regulations/staff-guidance/division-investment-management-frequently-asked-questions/frequently-asked-questions-about-form-13f>
- Form N-PORT XML tech specs (v1.7): <https://www.sec.gov/info/edgar/specifications/form-n-port-xml-tech-specs.htm>
- N-PORT dataset readme: <https://www.sec.gov/files/nport_readme.pdf>
- Schedule 13D/G XML tech specs (latest): <https://www.sec.gov/edgar/filer-information/current-edgar-technical-specifications> (current revision 2.2 as of 2026-03-16; pinned URL pattern is `https://www.sec.gov/file/schedule-13d-13g-tech-specs-{NN}`)
- Insider Forms 3/4/5 bulletin: <https://www.sec.gov/files/forms-3-4-5.pdf>
- EDGAR ownership XML spec: <https://www.sec.gov/info/edgar/ownershipxmlspec-v1-r1.doc>
- 2023-01-03 13F amendment context: <https://www.toppanmerrill.com/blog/sec-updates-edgar-on-jan-3-2023-for-form-13f-changes/>
