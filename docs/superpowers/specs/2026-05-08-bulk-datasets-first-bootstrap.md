# Bulk-datasets-first first-install bootstrap

Author: claude (autonomous, overnight session)
Date: 2026-05-08
Status: Draft (pre-Codex)

## Problem

The 2026-05-07 first-install bootstrap (`first-install-bootstrap.md`)
ships a working orchestrator but the wall-clock is dominated by
per-filing fetches throttled by SEC's 10 req/s ceiling:

- Smoke run 1 (universe-wide 13F sweep): ~174 minutes — mostly
  ancient `0001-99-...` accessions with no infotable. Fixed by
  PR #1009 (`min_period_of_report` recency bound).
- Smoke run 2 (recency-bounded 13F sweep): ~120 minutes — operator
  killed it. Even with the recency bound the per-CIK walk is
  bottlenecked by 10 req/s per-IP throttle, and parallelism inside
  one IP cannot help.
- Total observed end-to-end: 4.0–4.5 hours on a fresh DB.

**The structural waste is per-filing-fetching data the SEC publishes
in pre-aggregated bulk archives.** Submissions metadata, Company
Facts (XBRL fundamentals), Form 13F holdings, Form 3/4/5 insider
transactions, and Form N-PORT fund holdings all have free,
publicly-documented bulk-download archives that the current
bootstrap does not consume.

The remaining content (DEF 14A bodies, 10-K/8-K bodies, 10-K business
sections) is not in any bulk archive and must be fetched per-filing
— but is small in count and can be pipelined inside the rate budget.

## Goal

Reduce a fresh-install bootstrap to under ~90 minutes on a fast
connection while preserving 100% coverage (no top-N cohort cuts, no
"nightly will catch up" gap-filling). The operator clicks the
button once, and the system reaches a fully-backfilled state in one
sitting.

Specifically:

1. Pull what SEC publishes in bulk via bulk archives (no rate-limit
   cost beyond a handful of HTTP requests).
2. Pull what is NOT in bulk archives via per-filing fetches at
   ~7 req/s (5–7 is the real-world ceiling per datamule's
   "hidden cumulative rate limit" article; 10 is a documented hard
   cap that triggers UA bans on sustained use).
3. Cap end-to-end wall-clock dependence on bandwidth (slow internet
   path) by auto-detecting available bandwidth and falling back to
   per-filing pipelined for very slow connections.

Non-goals:

- Replacing the existing nightly cron schedules. Those continue
  unchanged after bootstrap completes.
- Replacing the per-CIK incremental ingest (#864 manifest worker).
  Bulk archives are the **first-install seed** only; daily updates
  remain per-CIK Atom + per-CIK submissions.json.
- Backfilling beyond what the bulk archives publish. The Form 13F
  Data Sets cover ~10 quarters historical (sufficient for
  ranking-engine inputs); we do not synthesise older data from
  per-filing fetches.

## Settled-decisions check

- **#719 (process topology):** Bulk-download stages run inside the
  jobs process, dispatched via the same `pending_job_requests`
  pattern the existing bootstrap orchestrator uses. The HTTP
  process never touches the bulk archives. ✓
- **Provider strategy (eToro source-of-truth, SEC for US filings):**
  Bulk archives are SEC-only. eToro lane unchanged. ✓
- **Free regulated-source-only fundamentals (#532):** Company Facts
  bulk archive is the same SEC XBRL Company Facts source the
  per-CIK ingest already uses. Same source, different transport. ✓
- **Do not add libraries casually:** `edgartools==5.30.2` is
  already a dependency (#925). Its `download_edgar_data()`
  function covers `submissions.zip` + `companyfacts.zip` only;
  the Data Sets archives are direct SEC URLs and pulled with
  the existing `httpx` client. No new dependency. ✓

## Prevention-log applicable entries

- **"Multiple ResilientClient instances sharing a rate limit must
  share throttle state" (#168):** Bulk-download stages do not
  share the per-filing token bucket — bulk archive endpoints are
  large file downloads, not per-CIK API calls, and SEC's published
  fair-use note treats them as a single transfer per file. Phase A
  uses a fresh `httpx.AsyncClient` configured with a 4-way
  connection-pool limit and the same `User-Agent` header. Phase C
  per-filing fetches reuse the existing `_PROCESS_RATE_LIMIT_CLOCK`.
- **"Naive datetime in TIMESTAMPTZ query params" (#278):** Bulk
  archive `period_of_report` and `filed_at` columns parse to
  `datetime` with explicit UTC tz; archive files use date-only or
  ISO 8601 strings.
- **"PR auto-close convention enforced by CI":** Each implementation
  PR body must `Closes #N` for the ticket it implements.
- **"Pydantic validation cliff" (2026-05-05):** `edgartools` parsers
  validate output through Pydantic models that reject some
  synthetic test fixtures. We sidestep this by using `edgartools`
  ONLY for `download_edgar_data()` (a pure download helper, no
  validation) and parsing bulk archive contents with stdlib
  `csv` / `xml.etree.ElementTree`.

## Facts (verified)

The architecture below depends on facts about SEC bulk archives.
Facts checked against (a) SEC.gov/developer documentation,
(b) `edgartools` source `EdgarTools-master/edgar/_filings.py` +
`edgar/storage.py`, and (c) datamule's published per-IP rate-limit
analysis.

| # | Fact | Source |
|---|---|---|
| F1 | SEC enforces 10 req/s per IP across all sec.gov endpoints, including /Archives. | data.sec.gov/developer fair-use page |
| F2 | Real-world sustained throughput is 5–7 req/s; 10 triggers UA bans. | datamule "hidden cumulative rate limit" article |
| F3 | `submissions.zip` (1.54 GB measured) is published nightly. URL: `/Archives/edgar/daily-index/bulkdata/submissions.zip`. Contains every CIK's submissions.json + recent block. | SEC Submissions endpoint docs + HEAD 2026-05-08 |
| F4 | `companyfacts.zip` (1.38 GB measured) is published nightly. URL: `/Archives/edgar/daily-index/xbrl/companyfacts.zip`. Contains Company Concepts + Company Facts XBRL JSON for every filer. | SEC XBRL Frames endpoint docs + HEAD 2026-05-08 |
| F5 | Form 13F Data Sets are published as rolling 3-month windows (e.g. `01dec2025-28feb2026_form13f.zip`, ~90 MB), with TSV files (SUBMISSION, COVERPAGE, INFOTABLE). Older datasets (pre-2024) use `<YYYY>q<N>_form13f.zip`. | SEC "Form 13F Data Sets" page + HTML scrape 2026-05-08 |
| F6 | Insider Transactions Data Sets are quarterly, named `<YYYY>q<N>_form345.zip` (~14 MB each), with TSV files (SUBMISSION, REPORTING_OWNER, NON_DERIV_TRANS, DERIV_TRANS). | SEC "Insider Transactions Data Sets" page + HEAD 2026-05-08 |
| F7 | Financial Statement Data Sets are quarterly (SUB, NUM, TAG, PRE), ~85 MB/quarter. **Not used by this spec** — duplicate of `companyfacts.zip` data. | SEC "Financial Statement Data Sets" page |
| F8 | Form N-PORT Data Sets are quarterly, named `<YYYY>q<N>_nport.zip` (~463 MB measured for 2026q1), under `/files/dera/data/form-n-port-data-sets/`. | SEC "Form N-PORT Data Sets" page + HEAD 2026-05-08 |
| F9 | EdgarTools `download_edgar_data()` covers ONLY `submissions.zip` + `companyfacts.zip`. It does NOT pull Data Sets archives or per-filing XBRL. | edgartools storage.py:download_edgar_data |
| F10 | The Daily Feed Public Data Set (full XBRL filings) is ~3 TB / 2 TB compressed. Too large for first-install. | SEC Filing Index Datasets page |
| F11 | `companyfacts.zip` contains one JSON per CIK — extracting and ingesting per-CIK avoids the per-CIK XBRL Company Facts API calls. | SEC XBRL Frames docs |
| F12 | DEF 14A bodies, 10-K business-section text, 8-K item bodies are NOT in any bulk archive. Must be per-filing-fetched. | SEC Submissions endpoint (filing index URLs only); manual verification of bulk-archive file inventories |
| F13 | Form 13F INFOTABLE TSV in Data Sets exposes CUSIP, value (USD), shares (split-adjusted), security type, voting authority — same fields the current per-filing parser extracts. | SEC Form 13F Data Sets schema doc |
| F14 | Insider Transactions Data Sets contain Form 3, 4, 5 transactions with shares, transaction code, transaction date. Same fields as the current per-filing Form 4 parser. | SEC Insider Transactions Data Sets schema doc |

## Architecture overview

The bootstrap is restructured into four phases:

```
Phase A (sequential init + parallel-after-A1)
  └── A1 universe_sync                        (existing; populates `instruments`)
        ├── A2 candle_refresh                 (existing eToro lane E1; runs after A1)
        ├── A3 sec_bulk_download              (NEW; downloads bulk archives — bandwidth-bound)
        └── B1–B4 (parallel inside SEC rate budget; share _PROCESS_RATE_LIMIT_CLOCK)
            ├── B1 cusip_universe_backfill              (existing, S1)
            ├── B2 sec_13f_filer_directory_sync         (existing, S2)
            ├── B3 sec_nport_filer_directory_sync       (existing, S3)
            └── B4 cik_refresh                          (existing, S4)

Phase C (DB-bound after A3 + B1–B4 join)
  ├── C1.a sec_submissions_ingest             (NEW; load submissions.zip → filing_events recent + instrument_sec_profile)
  ├── C2   sec_companyfacts_ingest            (NEW; load companyfacts.zip → financial_facts_raw)
  ├── C3   sec_13f_ingest_from_dataset        (NEW; load 13F TSV → ownership_institutions_observations)
  ├── C4   sec_insider_ingest_from_dataset    (NEW; load 3/4/5 TSV → ownership_insiders_observations)
  └── C5   sec_nport_ingest_from_dataset      (NEW; load NPORT XML → ownership_funds_observations)

Phase C' (rate-bound after C1.a populates issuer cohort)
  ├── C1.b sec_submissions_files_walk         (NEW; per-CIK filings.files[] secondary pages — preserves deep-history)
  └── C6   sec_first_install_drain            (existing; now drains from filing_events table after C1.a + C1.b)

Phase D (per-filing pipelined fetches — 7 req/s pipelined ×4 connection pool)
  ├── D1 sec_def14a_bootstrap                 (existing, S7)
  ├── D2 sec_business_summary_bootstrap       (existing, S8)
  └── D3 sec_8k_events_ingest                 (existing, S11)

Phase E (DB-only finalize)
  ├── E1 ownership_current_refresh            (NEW; refreshes ownership_*_current snapshots from observations)
  └── E2 fundamentals_derivation              (NEW; derives financial_periods from financial_facts_raw — no fetch)
```

### Phase-by-phase wall-clock estimate

Numbers below assume 100 Mbps connection (typical UK fibre), 8-core
modern laptop, and a 1.5 k-instrument universe. "Bandwidth bound"
stages scale with connection speed; "rate bound" stages scale with
SEC's 7 req/s cap; "DB bound" stages scale with local Postgres.

| Phase | Stages | Bound | ETA (100 Mbps) | ETA (25 Mbps) |
|---|---|---|---|---|
| A | A1; then A2 + A3 + B1–B4 (parallel after A1) | Mixed (A3 bandwidth) | ~10 min | ~32 min |
| C.a | C1.a + C2 (DB-bound) | DB | ~5 min | ~5 min |
| C.b | C3 + C4 + C5 (DB-bound) | DB | ~6 min | ~6 min |
| C.b' | C1.b + C6 (per-CIK secondary pages + drain) | Rate | ~3 min | ~3 min |
| D | D1–D3 (pipelined) | Rate | ~30 min | ~30 min |
| E | E1, E2 | DB | ~3 min | ~3 min |
| **Total** | | | **~57 min** | **~79 min** |

Slow-internet path (probe routes to legacy per-CIK; bulk-archive
fallback skipped): same as the existing 2026-05-07 spec ETA, ~210
min. The slow-path ETA in the bulk-archive table above (25 Mbps)
is the borderline-fast path where the bulk download still completes
inside the 60-min budget.

5.7 GB ÷ 25 Mbps = 32 min (Phase A bandwidth-bound). 5.7 GB ÷
100 Mbps = 8 min. Phase A wall-clock dominated by the largest
archive (NPORT 1.85 GB across 4 quarters; submissions 1.54 GB).

vs current 4.0–4.5 hours = **~76% reduction** on typical
broadband; **~67% reduction** for the borderline-fast path.

### Bulk archive inventory + sizes

| Archive | URL | Size (verified by HEAD 2026-05-08) | Cadence | Phase |
|---|---|---|---|---|
| `submissions.zip` | `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip` | 1.54 GB | nightly | A3 → C1, C6 |
| `companyfacts.zip` | `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` | 1.38 GB | nightly | A3 → C2 |
| Form 13F Data Sets (rolling 3-month) | `https://www.sec.gov/files/structureddata/data/form-13f-data-sets/<DDmonYYYY>-<DDmonYYYY>_form13f.zip` (e.g. `01dec2025-28feb2026_form13f.zip`) | ~90 MB × 4 | rolling 3-month | A3 → C3 |
| Insider Transactions | `https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/<YYYY>q<N>_form345.zip` | ~14 MB × 8 | quarterly | A3 → C4 |
| Form N-PORT Data Sets | `https://www.sec.gov/files/dera/data/form-n-port-data-sets/<YYYY>q<N>_nport.zip` | ~463 MB × 4 | quarterly | A3 → C5 |
| **Total Phase A download** | | **~5.7 GB** | | |

Verified URLs by direct HEAD against `www.sec.gov` on 2026-05-08
with the `User-Agent: ebull/dev (admin@example.com)` header.
Sizes are HTTP `Content-Length` from the latest fully-published
period in each archive; older periods are similar within ±20%.

URL-discovery note: the Form 13F filename pattern uses a calendar-
date *range* (e.g. `01dec2025-28feb2026`), NOT `<YYYY>q<N>`. Older
13F datasets (pre-2024) used `<YYYY>q<N>_form13f.zip`. The bootstrap
must list both naming conventions; PR1's URL builder probes both
and uses whichever returns 200. Identical pattern fork for
`/files/structureddata/` (Form 13F + Insider) vs `/files/dera/`
(Financial Statement + N-PORT) — these are SEC-internal CMS legacy
splits and the orchestrator must hard-code the correct prefix per
archive family.

Financial Statement Data Sets are deliberately omitted from Phase A:
their content (XBRL flattened SUB/NUM/TAG/PRE) is the same
fundamentals data that `companyfacts.zip` already provides in JSON
form, and Company Facts is the canonical source the existing
fundamentals path consumes. Adding finstmt would double-write the
same numerics.

### Phase A — bulk download (NEW)

A3 (`sec_bulk_download`) is a single new job invoker that downloads
all six archive families in parallel using `asyncio.gather` with a
shared `httpx.AsyncClient` (max 4 connections — well under the
10 req/s cap; each connection holds one large transfer). Every
download writes to `<settings.data_dir>/sec/bulk/<archive>` with
atomic move-on-complete (write to `.partial`, rename when fully
written + checksum verified).

#### Implementation

```python
# app/services/sec_bulk_download.py (NEW)

BULK_ARCHIVES: Sequence[BulkArchive] = [
    BulkArchive("submissions.zip",
                "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
                expected_min_bytes=int(1.2 * 1024**3)),  # 1.2 GB floor (1.54 GB observed)
    BulkArchive("companyfacts.zip",
                "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
                expected_min_bytes=int(1.0 * 1024**3)),  # 1.0 GB floor (1.38 GB observed)
    # 13F: last 4 rolling-3-month windows. Filenames are
    # ``<DDmonYYYY>-<DDmonYYYY>_form13f.zip`` for >=2024Q1, and
    # ``<YYYY>q<N>_form13f.zip`` for older. URL builder probes both.
    *(BulkArchive.from_13f_period(p, expected_min_bytes=50 * 1024**2)
      for p in last_n_13f_periods(4)),
    # Insider: last 8 quarters. Filename: ``<YYYY>q<N>_form345.zip``.
    *(BulkArchive(f"insider_{q.label}.zip",
                  f"https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{q.label}_form345.zip",
                  expected_min_bytes=8 * 1024**2)
      for q in last_n_quarters(8)),
    # N-PORT: last 4 quarters. Filename: ``<YYYY>q<N>_nport.zip``.
    # Note ``/files/dera/data/`` prefix (NOT ``/files/structureddata/``).
    *(BulkArchive(f"nport_{q.label}.zip",
                  f"https://www.sec.gov/files/dera/data/form-n-port-data-sets/{q.label}_nport.zip",
                  expected_min_bytes=300 * 1024**2)
      for q in last_n_quarters(4)),
]
```

Slow-connection guard: at startup the orchestrator runs a 5-second
HEAD-and-throughput probe against `submissions.zip` (range request
for first 4 MB to amortise TCP slow-start), measures effective
throughput, and computes ETA for the full ~5.7 GB. If estimated
download time exceeds 60 minutes (i.e. effective throughput below
~13 Mbps), the orchestrator skips A3 entirely and falls back to
the original per-CIK path. Operator sees a clear status line in the
admin UI: "Slow connection detected (~5 Mbps). Falling back to
per-filing fetch path. Estimated total: ~3 hours."

This guard preserves the demo-quality first-install for fast
connections without leaving slow-connection operators worse off
than today (they get the same ~4-hour path they have now, with
clear messaging).

#### Disk footprint

5.7 GB downloaded + ~10 GB unzipped + parsed = peak ~18 GB on disk
during Phase C. Cleaned to 0 GB at end of Phase C. Spec assumes
operator has 25 GB free. Pre-flight check at start of Phase A
queries `shutil.disk_usage(settings.data_dir)`; if <25 GB free,
skip A3 with a clear error in the admin UI.

Atomic-write integrity: each archive is written to `<name>.partial`
then renamed to `<name>` on completion. Integrity is asserted by
`Content-Length` match against the HEAD response — SEC does not
publish per-archive checksums, so we treat HTTP-level length match
+ a successful zipfile-list (`zipfile.ZipFile(path).namelist()` call)
as the integrity gate.

### Phase A ordering (corrected from Codex round 1)

A1 (`universe_sync`) MUST complete before A3 starts: A3's bulk
ingest needs the universe + CIK mapping populated for `cik_to_instrument`
lookups in Phase C. Also, B1 (`cusip_universe_backfill`) and
B4 (`cik_refresh`) read `instruments`, so they cannot start before A1.

The corrected ordering is:

```
A1 universe_sync (sequential, prerequisite)
   │
   ├── A2 candle_refresh (eToro lane; rate-budget separate from SEC)
   ├── A3 sec_bulk_download (parallel; bandwidth-bound, not rate-bound)
   └── B1 cusip_universe_backfill, B2 sec_13f_filer_directory_sync,
       B3 sec_nport_filer_directory_sync, B4 cik_refresh
       (parallel inside SEC rate budget, share _PROCESS_RATE_LIMIT_CLOCK)
```

This means A1 is the only true Phase-A1 stage in this spec; what
were "A2/A3" become parallel siblings of B1–B4 inside the SEC rate
budget *except* A3 which lives outside (bulk archives are large
file transfers — see "Bulk archives and rate budget" below).

### Phase B — directories (unchanged from existing bootstrap)

B1–B4 are existing stages from `2026-05-07-first-install-bootstrap.md`.
After A1 completes, they run in parallel with A2 + A3 — network-
independent of A3's bandwidth-heavy transfers (different SEC
endpoints, different transport pattern). B1/B2/B3/B4 share
`_PROCESS_RATE_LIMIT_CLOCK` per #168 and serialise within that
budget.

#### Bulk archives and rate budget

The bulk archive endpoints (`/Archives/edgar/daily-index/bulkdata/`,
`/Archives/edgar/daily-index/xbrl/`, `/files/structureddata/data/`,
`/files/dera/data/`) are served by SEC's CDN as multi-MB-to-GB file
transfers. SEC's published 10 req/s fair-use ceiling counts each
HTTP request, not each byte; downloading six archive files
end-to-end therefore consumes only ~18 HTTP requests over the full
~10 min of Phase A. We can safely run A3 *outside* the per-CIK
token bucket because it does not contend with B1–B4 for request
slots.

To stay belt-and-braces under #168, the A3 client opens a separate
`httpx.AsyncClient` instance with no shared throttle reference; it
rate-limits itself at `max_concurrency=4` simultaneous file
transfers (one TCP connection per archive family), serialised
within each family. If SEC ever moves bulk archives onto the
data.sec.gov endpoint (which IS rate-budgeted at the per-IP cap),
the implementation upgrades A3 to acquire from
`_PROCESS_RATE_LIMIT_CLOCK` — but per HEAD checks today the
bulk paths are CDN-served and not part of the token bucket.

### Phase C — DB-only ingest from bulk archives (NEW)

Phase C runs **after** A3 completes and B1–B4 are done. It is purely
DB-bound — every stage reads cached files from disk and writes to
Postgres. No network calls. No rate-limit cost.

#### C1: sec_submissions_ingest

Reads `submissions.zip` and seeds `filing_events` for every
CIK-mapped instrument. Replaces the per-CIK submissions walk that
S5 (`bootstrap_filings_history_seed`) currently does at 7 req/s
across the whole universe.

Schema: each entry in `submissions.zip` is one CIK's
`CIK<10-digit>.json` containing the `filings.recent` block. The
`recent` block holds the last ~12 months / up to 1000 most-recent
filings; older history is paginated under `filings.files[]`
(secondary URLs that are NOT in `submissions.zip` itself).

**Coverage caveat (deeper history).** The existing
`sec_first_install_drain` walks `filings.files[]` per-CIK
(`app/jobs/sec_first_install_drain.py:21`) to seed manifest +
freshness for every historical filing. C1 with bulk-only would
regress to ~12 months. To preserve parity with the 2026-05-07 spec,
C1 runs in two parts:

- **C1.a (bulk):** ingest `filings.recent` from `submissions.zip`
  for every CIK-mapped instrument. No network; zero rate-budget.
  Also populates `instrument_sec_profile` from the same
  `submissions.json` top-level fields (`sic`, `sicDescription`,
  `name`, `description`, `website`, `addresses`, etc) by chaining
  the existing `parse_entity_profile(...)` parser
  (`app/services/sec_entity_profile.py:163`) with
  `upsert_entity_profile(...)`
  (`app/services/sec_entity_profile.py:339`). The bulk archive
  payload exposes the exact same top-level shape as per-CIK
  `submissions.json`, so both helpers are reusable as-is.
- **C1.b (per-CIK secondary pages):** for each CIK whose recent
  block contained references to `filings.files[]`, walk those
  secondary pages at 7 req/s shared with the rest of the SEC
  bucket. Bounded by `_PROCESS_RATE_LIMIT_CLOCK`. Walked AFTER
  Phase B completes so the CUSIP universe + filer directories are
  in place.

  C1.b is also the input for C6's manifest + freshness drain over
  the deep-history rows. `sec_filing_manifest` and
  `data_freshness_index` parity with the existing 2026-05-07 spec
  is preserved by sequencing C6 to run AFTER both C1.a and C1.b
  have completed; the drain reads from the now-fully-populated
  `filing_events` table (which holds both recent + secondary-page
  rows) rather than re-fetching submissions over HTTP.

C1.b's request count scales with the count of universe-CIKs that
have files-block secondary pages — for a 1.5 k instrument universe
this is observed ~150 CIKs × ~3 secondary pages = 450 requests
≈ 65 s. Acceptable.

```python
# app/services/sec_submissions_ingest.py (NEW)

def ingest_submissions_archive(*, conn, archive_path, cik_to_instrument):
    with zipfile.ZipFile(archive_path) as zf:
        for name in zf.namelist():
            if not name.startswith("CIK") or not name.endswith(".json"):
                continue
            cik = int(name[3:13])  # CIK<10-digit>.json
            instrument_id = cik_to_instrument.get(cik)
            if instrument_id is None:
                continue  # Not in our universe.
            with zf.open(name) as f:
                payload = json.load(f)
            recent = payload.get("filings", {}).get("recent", {})
            files = payload.get("filings", {}).get("files", [])  # secondary pages
            _upsert_filing_events_from_recent(conn, instrument_id, recent)
            # Record secondary-page URLs for the C1.b sweep.
            _record_files_pages(conn, instrument_id, files)
```

#### C2: sec_companyfacts_ingest

Reads `companyfacts.zip` and seeds `financial_facts_raw` (XBRL
Company Facts JSON) for every CIK-mapped instrument. Replaces the
per-CIK Company Facts HTTP walk in S16 (`fundamentals_sync`).

Target table is `financial_facts_raw` (existing — see
`sql/048_financial_facts_raw_identity_constraint.sql`). The
companyfacts.zip JSON shape is identical to the per-CIK
`/api/xbrl/companyfacts/CIK<10>.json` response. The existing parser
chain converts that dict into a `list[XbrlFact]` before writing:

1. Per-CIK JSON entry → call
   `_extract_facts_from_section(gaap_section, taxonomy="us-gaap")`
   + `_extract_facts_from_section(dei_section, taxonomy="dei")`
   (`app/providers/implementations/sec_fundamentals.py:264`,
   currently module-private — PR2 promotes it to a public
   `extract_facts_from_companyfacts_payload()` wrapper).
2. Concatenated `list[XbrlFact]` → call
   `upsert_facts_for_instrument(conn, instrument_id=…, facts=…,
   ingestion_run_id=…)` (`app/services/fundamentals.py:474`).

Iterates the archive entries and resolves CIK → instrument_id from
the `cik_to_instrument` map produced by B4 (`cik_refresh`).

C2 does NOT populate `instrument_sec_profile`; that table is
populated by C1.a (see below) which has the necessary
`submissions.json` top-level fields. (Codex round-2 finding:
`instrument_sec_profile` is sourced from
`app/services/sec_entity_profile.py` which parses
`submissions.json` top-level fields like `sic`, `owner_org`,
`description`, `exchanges`, etc — none of which are in
`companyfacts.zip`.)

#### C3: sec_13f_ingest_from_dataset

Reads the 4 most-recent quarterly Form 13F Data Sets ZIPs.
Each ZIP contains:

- `SUBMISSION.tsv` — one row per filing (CIK, accession, period).
- `COVERPAGE.tsv` — one row per filing (filer name, total holdings value).
- `INFOTABLE.tsv` — one row per holding (CUSIP, value, shares, type, voting authority).

Joins SUBMISSION → COVERPAGE → INFOTABLE by accession; resolves
CUSIP → instrument via `external_identifiers`; writes
`ownership_institutions_observations` (existing partitioned table —
see `sql/114_ownership_institutions_observations.sql`).

This replaces S13's per-filing 13F sweep entirely. Universe-coverage
in the bulk archive is 100% of 13F filers — none of the
"top-N cohort" cuts.

**Required observation fields (per the 114 schema):**

| Column | Source in dataset | Notes |
|---|---|---|
| `instrument_id` | resolved from `INFOTABLE.CUSIP` | skip row if unresolved |
| `filer_cik` | `SUBMISSION.CIK` | NOT NULL |
| `filer_name` | `COVERPAGE.FILINGMANAGER_NAME` | NOT NULL |
| `filer_type` | derived: `'INV'` for typical 13F filer | nullable, CHECK enum |
| `ownership_nature` | `'economic'` (constant for 13F-HR) | CHECK enum |
| `source` | `'13f'` | CHECK enum |
| `source_document_id` | `SUBMISSION.ACCESSION_NUMBER` | NOT NULL |
| `source_accession` | same as `source_document_id` | |
| `source_url` | computed: `https://www.sec.gov/Archives/edgar/data/<cik>/<accession-without-dashes>/` | |
| `filed_at` | `SUBMISSION.FILING_DATE` | NOT NULL TIMESTAMPTZ |
| `period_end` | `COVERPAGE.REPORTCALENDARORQUARTER` | NOT NULL DATE — **partition key** |
| `ingest_run_id` | UUID generated per-archive | NOT NULL |
| `shares` | `INFOTABLE.SSHPRNAMT` | NUMERIC(24,4) via `Decimal(str(...))` |
| `market_value_usd` | `INFOTABLE.VALUE` × 1000 (SEC reports in $thousands) | NUMERIC(20,2) via `Decimal(str(...))` |
| `voting_authority` | `INFOTABLE.VOTING_AUTHORITY_SOLE/SHARED/NONE` → `'SOLE'/'SHARED'/'NONE'` | nullable enum |
| `exposure_kind` | `INFOTABLE.PUTCALL` → `'EQUITY'` (default) / `'PUT'` / `'CALL'` | NOT NULL DEFAULT 'EQUITY' |

```python
# app/services/sec_13f_dataset_ingest.py (NEW)

def ingest_13f_dataset_archive(*, conn, archive_path, ingest_run_id):
    with zipfile.ZipFile(archive_path) as zf:
        with zf.open("SUBMISSION.tsv") as f:
            subs_by_acc = {
                r["ACCESSION_NUMBER"]: r
                for r in csv.DictReader(io.TextIOWrapper(f), delimiter="\t")
            }
        with zf.open("COVERPAGE.tsv") as f:
            cover_by_acc = {
                r["ACCESSION_NUMBER"]: r
                for r in csv.DictReader(io.TextIOWrapper(f), delimiter="\t")
            }
        with zf.open("INFOTABLE.tsv") as f:
            for row in csv.DictReader(io.TextIOWrapper(f), delimiter="\t"):
                acc = row["ACCESSION_NUMBER"]
                sub = subs_by_acc.get(acc)
                cover = cover_by_acc.get(acc)
                if sub is None or cover is None:
                    continue  # Orphan row.
                cusip = row["CUSIP"]
                instrument_id = _resolve_cusip(conn, cusip)
                if instrument_id is None:
                    continue  # Universe gap; tracked separately.
                _record_institution_observation(
                    conn,
                    instrument_id=instrument_id,
                    filer_cik=sub["CIK"].zfill(10),
                    filer_name=cover["FILINGMANAGER_NAME"],
                    filer_type="INV",
                    ownership_nature="economic",
                    source="13f",
                    source_document_id=acc,
                    source_accession=acc,
                    filed_at=_parse_filing_date(sub["FILING_DATE"]),
                    period_end=_parse_date(cover["REPORTCALENDARORQUARTER"]),
                    ingest_run_id=ingest_run_id,
                    shares=Decimal(str(row["SSHPRNAMT"])) if row["SSHPRNAMT"] else None,
                    # SEC reports VALUE in $thousands; multiply for USD.
                    market_value_usd=Decimal(str(row["VALUE"])) * Decimal("1000")
                                      if row["VALUE"] else None,
                    voting_authority=_map_voting_authority(row),
                    exposure_kind=_map_putcall(row.get("PUTCALL")),
                )
```

Decimal handling: `value` and `shares` are TSV string columns;
parsed via `Decimal(str(row[...]))` (prevention-log
"Decimal(str()) for fixed-point math" 2026-05-05 — float coercion
loses precision on large institutional holdings).

Universe-CUSIP gap: 13F INFOTABLE rows whose CUSIP doesn't resolve
to an instrument are tracked in a counter (`rows_skipped_unresolved_cusip`)
not silently dropped. The CUSIP universe-backfill (#914 / B1) populates
the resolution table; expected gap rate is <30% for a 1.5 k universe
(13F holdings include the broad-market index components — only
those mapped to our instruments resolve).

#### C4: sec_insider_ingest_from_dataset

Reads the 8 most-recent quarterly Insider Transactions Data Sets
ZIPs (`<YYYY>q<N>_form345.zip`). Each ZIP contains:

- `SUBMISSION.tsv` — one row per Form 3/4/5 filing.
- `REPORTING_OWNER.tsv` — one row per insider per filing.
- `NON_DERIV_TRANS.tsv` — non-derivative transactions (stock).
- `NON_DERIV_HOLDING.tsv` — non-derivative holdings post-transaction.
- `DERIV_TRANS.tsv` — derivative transactions.
- `DERIV_HOLDING.tsv` — derivative holdings.

Replaces S9 (`sec_insider_transactions_backfill`) and S10
(`sec_form3_ingest`) entirely. Bulk archive covers Forms 3, 4, 5;
the per-filing path covered only Form 3 + Form 4. Form 5 (annual
catch-up) is a free upgrade.

**Required observation fields (per the 113 schema):**

| Column | Source in dataset | Notes |
|---|---|---|
| `instrument_id` | resolved from `SUBMISSION.ISSUERCIK` → CIK lookup | skip if unresolved |
| `holder_cik` | `REPORTING_OWNER.RPTOWNERCIK` | nullable; legacy NULL-CIK rows are OK |
| `holder_name` | `REPORTING_OWNER.RPTOWNERNAME` | NOT NULL |
| `holder_identity_key` | GENERATED column from holder_cik / lower(trim(holder_name)) | computed by Postgres, not by ingester |
| `ownership_nature` | `REPORTING_OWNER.RPTOWNER_RELATIONSHIP` mapped: officer/director → 'direct'; ten-percent owner → 'beneficial' | CHECK enum |
| `source` | `'form4'` for Form 4 / 4-A; `'form3'` for Form 3 / 3-A; `'form4'` (with note) for Form 5 acceptable per existing source-priority chain | CHECK enum |
| `source_document_id` | `SUBMISSION.ACCESSION_NUMBER` | NOT NULL |
| `filed_at` | `SUBMISSION.FILING_DATE` | NOT NULL |
| `period_end` | for Form 4/5: `NON_DERIV_TRANS.TRANS_DATE`; for Form 3: `SUBMISSION.PERIOD_OF_REPORT` (initial-statement date) | NOT NULL DATE |
| `ingest_run_id` | UUID per archive | NOT NULL |
| `shares` | `NON_DERIV_HOLDING.SHRS_OWND_FOLWNG_TRANS` (post-transaction shares-owned) | NUMERIC(24,4) via Decimal(str) |

Note: the existing per-filing Form 4 ingester writes the
**post-transaction shares-owned** value, not the per-transaction
delta. The dataset's `NON_DERIV_HOLDING` table carries that
identical figure; ingester joins HOLDING to TRANS by accession +
holder + line number to produce one observation per holding-state
post a filing. Implementation reuses the existing
`record_insider_observation()` helper which already handles the
holder_identity_key generation + source-priority dedup.

#### C5: sec_nport_ingest_from_dataset

Reads the 4 most-recent quarterly Form N-PORT Data Sets
(`<YYYY>q<N>_nport.zip` under `/files/dera/data/form-n-port-data-sets/`).
The archive layout per SEC docs is per-filing TSV/XML splits.

Replaces S14 (`sec_n_port_ingest`) entirely.

Stdlib `xml.etree.ElementTree` parses each NPORT-P element;
`ownership_funds_observations` is the write target (per #917 +
schema in `sql/123_ownership_funds.sql`).

**Required observation fields (per the 123 schema):**

| Column | Source in dataset | Notes |
|---|---|---|
| `instrument_id` | resolved from CUSIP / ISIN of holding | skip if unresolved |
| `fund_series_id` | `seriesId` element (S0000xxxxx pattern) | NOT NULL; CHECK validates `^S[0-9]{9}$` (S + 9 digits = 10 chars total) |
| `fund_series_name` | `seriesName` | NOT NULL |
| `fund_filer_cik` | filer CIK from filename / submission header | NOT NULL; for audit + per-filer rollup queries, NOT in PK |
| `ownership_nature` | `'economic'` (constant) | CHECK = 'economic' |
| `source` | `'nport'` | CHECK enum |
| `source_document_id` | filing accession | NOT NULL |
| `filed_at` | filing date | NOT NULL |
| `period_end` | `repPdEnd` | NOT NULL DATE |
| `ingest_run_id` | UUID | NOT NULL |
| `shares` | `balance` element | NOT NULL > 0; ingester filters non-positive |
| `asset_category` | `assetCat` | CHECK = 'EC'; ingester filters non-EC at write boundary |
| `payoff_profile` | `payoffProfile` | CHECK = 'Long'; ingester filters non-Long |

Holdings filtered at the write boundary so the schema CHECKs (which
are second-line guards) don't reject legitimate-shape rows from
debt / derivative / preferred / short positions in the same per-fund
holdings array.

#### C6: sec_first_install_drain (refactored)

Existing S6 stage. Currently issues per-CIK `submissions.json`
fetches at 7 req/s to seed `sec_filing_manifest` +
`data_freshness_index`. After C1.a + C1.b, the cumulative
filing-events history is in `filing_events` (recent + secondary
pages). Refactor C6 to seed manifest + freshness rows by walking
`filing_events` directly:

```python
def sec_first_install_drain(*, conn, prefer_filing_events=True):
    if prefer_filing_events and _has_filing_events_rows(conn):
        return _drain_from_filing_events(conn)  # No HTTP.
    return _drain_per_cik(conn)  # Existing fallback path.
```

The `_drain_from_filing_events` helper iterates the
`filing_events` rows by (instrument_id, accession_number) and
inserts the corresponding `sec_filing_manifest` +
`data_freshness_index` entries. This preserves parity with the
2026-05-07 spec (deep-history coverage from C1.b) and avoids
re-fetching submissions over HTTP.

The slow-connection fallback path (`prefer_filing_events=False`,
or `filing_events` empty because A3 was skipped) takes the legacy
per-CIK HTTP route — same behaviour as the 2026-05-07 spec.

### Phase D — per-filing pipelined fetches

The filings-content stages (DEF 14A bodies, 10-K business sections,
8-K item-text bodies) are NOT in any bulk archive (F12). They must
be fetched per-filing.

The current per-filing path issues fetches sequentially at 7 req/s.
Phase D switches to a pipelined fetcher that issues 4 concurrent
fetches at a time, sharing the rate budget — same total requests,
better wall-clock because PDF + HTML parsing CPU overlaps with
network I/O.

```python
# app/services/sec_pipelined_fetcher.py (NEW)

class PipelinedSecFetcher:
    def __init__(self, *, target_rps: float = 7.0, concurrency: int = 4):
        self._sem = asyncio.Semaphore(concurrency)
        self._rate_limiter = AsyncRateLimiter(target_rps)

    async def fetch_many(
        self, items: Sequence[FetchTask]
    ) -> AsyncIterator[FetchResult]:
        """Yields results in COMPLETION order (not request order).

        Each ``FetchTask`` carries an opaque ``key`` the caller uses
        to associate the result with the original request. Caller
        code must not rely on ordering.
        """
        async def _one(task: FetchTask) -> FetchResult:
            async with self._sem:
                await self._rate_limiter.acquire()
                resp = await self._client.get(task.url)
                return FetchResult(key=task.key, response=resp)
        async for result in _gather_as_completed(_one(t) for t in items):
            yield result
```

Same total request count, same rate ceiling, but ~30% faster
wall-clock when fetch latency is 200–500 ms (typical SEC EDGAR
response time for HTML/PDF bodies). Empirical measurement on the
2026-05-04 smoke run: average DEF 14A body fetch was 380 ms; 4-way
pipelining at 7 rps fully saturates the budget.

Ordering note: results yield in completion order, not request
order. Each existing per-stage caller (D1/D2/D3) is being updated
to associate results to filings via the `key` field rather than
positional index. This is a contract change and explicitly tested
in PR6.

#### D1: sec_def14a_bootstrap

Existing S7. Now reads `filing_events` rows (populated by C1)
filtered to DEF 14A; pipelined fetcher pulls bodies; writes to
`def14a_filings`.

#### D2: sec_business_summary_bootstrap

Existing S8. Same pattern — reads 10-K filings from C1's seeded
`filing_events`, pipelined fetcher pulls 10-K bodies + extracts
Item 1 business section, writes to `business_summaries`.

#### D3: sec_8k_events_ingest

Existing S11. Reads 8-K filings from C1, pipelined fetcher pulls
bodies, extracts items, writes to `eight_k_items`.

### Phase E — finalize

E1 (`ownership_current_refresh`) refreshes the materialised
`ownership_*_current` snapshots from the observation tables that
C3, C4, C5 populated. The existing `ownership_observations_backfill`
job does both the legacy → observations walk AND the
`_current` refresh; in this spec the legacy walk is skipped (C3/C4/C5
write observations directly), so E1 is the `_current`-refresh half
only. Implementation note: split the existing service helper so the
`_current` refresh can be invoked without a legacy walk.

E2 (`fundamentals_derivation`) consumes the `financial_facts_raw`
rows that C2 populated and writes derived period tables
(`financial_periods`, etc) — the same DB-only post-processing that
the existing `fundamentals_sync` runs after its HTTP fetch step.
E2 skips fetching (data already in `financial_facts_raw` from C2)
and runs the derivation step only. Wall-clock target ~3 min for a
1.5 k universe.

## Stages dropped from the existing bootstrap

| Stage | Why dropped | Replacement |
|---|---|---|
| S5 (`bootstrap_filings_history_seed`) | Per-CIK submissions walk @ 7 req/s | C1.a (bulk submissions.zip) + C1.b (per-CIK files[] secondary pages, preserves deep history) |
| S9 (`sec_insider_transactions_backfill`) — first-install only; nightly cron unchanged | Per-filing Form 4 fetch | C4 (Insider Transactions Data Sets) |
| S10 (`sec_form3_ingest`) — first-install only | Per-filing Form 3 fetch | C4 |
| S13 (`sec_13f_quarterly_sweep`) — first-install only | Per-filing 13F sweep @ 7 req/s | C3 (Form 13F Data Sets) |
| S14 (`sec_n_port_ingest`) — first-install only | Per-filing NPORT-P fetch | C5 (Form N-PORT Data Sets) |
| S15 (`ownership_observations_backfill`) — legacy walk half | C3/C4/C5 write observations directly | E1 (`_current`-refresh half only) |
| S16 (`fundamentals_sync`) — fetch half | C2 already wrote `financial_facts_raw` | E2 (derivation half only) |

The existing nightly cron versions of S9, S10, S13, S14 are
**unchanged**. They continue to handle delta updates after
bootstrap. Bulk archives are the **first-install seed** only.

## Slow-connection fallback

If A3's startup throughput probe shows <13 Mbps, the orchestrator
takes the legacy path: skip Phase A's A3, run the original S5/S6
per-CIK chains in Phase B (still in parallel with A1+A2), and run
all of S7–S16 sequentially in Phase C.

| Connection | Path | ETA |
|---|---|---|
| ≥13 Mbps | Bulk-archive (this spec) | ~57 min |
| <13 Mbps | Legacy per-CIK only (existing 2026-05-07 path) | ~210 min |

The 13 Mbps threshold is `5.7 GB × 8 ÷ 3600 s = 12.7 Mbps` (one-hour
download budget; rounded up). Below that, the bulk download takes
longer than the legacy per-CIK chain — the orchestrator skips A3
entirely and runs the existing 2026-05-07 stages unchanged.
Threshold is configurable via `BOOTSTRAP_BANDWIDTH_MIN_MBPS`.

A two-step probe (skip-some-archives partial path) was considered
and rejected: each archive family is ingested in one DB-bound stage
that depends on the file being on disk, so partial fallback would
require dual-codepaths in every Phase C ingester. Single
all-or-nothing threshold keeps the implementation tractable.

## Risks and disclaimers

1. **Bulk archive lag.** SEC publishes archives at ~03:00 ET nightly.
   A bootstrap run at 02:30 ET would download "yesterday's"
   archive. Mitigation: orchestrator records the
   `last-modified` header of `submissions.zip` and surfaces it in
   the admin UI: "Bulk archive dated 2026-05-08 03:14 ET — covers
   filings up to ~2026-05-07 EOD". After bootstrap, the nightly
   per-CIK incremental ingest catches anything filed since.
2. **Quarterly archive lag.** The Form 13F / NPORT / Insider Data
   Sets are published days-to-weeks after quarter end. The most
   recent completed quarter's data may not yet be in the archive
   when bootstrap runs. Mitigation: after Phase C, the orchestrator
   runs a single per-CIK delta sweep covering the date range
   `[archive_published_through, today]`. This stage replaces the
   "S13 universe sweep" with a much narrower, recency-bounded
   sweep against only filings actually missing from the bulk data.
3. **Disk usage spike.** ~25 GB peak during Phase C (downloads +
   unzipped). Pre-flight `shutil.disk_usage` check with a clear
   "insufficient disk space" admin error. Cleaned to 0 GB at the
   end of Phase C.
4. **SEC fair-use User-Agent.** All Phase A downloads send the same
   `User-Agent: ebull/<version> (admin@<host>)` header the existing
   per-filing fetches use. Phase A is a small number of HTTP
   transfers (each is multi-GB), well under the per-IP request-rate
   limit. Documented in `docs/settled-decisions.md` once spec is
   settled.
5. **Bulk archive parse errors.** Some filings fail strict-XBRL
   parsing in upstream tools. Mitigation: each Phase C ingest
   stage tracks `rows_processed` + `rows_skipped_parse_error` per
   archive; per-stage error count surfaces in admin UI. Hard-fail
   only if >5% of rows skip-with-parse-error per archive (signal
   archive corruption rather than expected stragglers).
6. **Multi-tenant data dir.** `settings.data_dir` is per-install,
   so concurrent installs do not collide. Operator setting one
   data dir per host is a documented pre-existing requirement.
7. **edgartools dependency boundary.** `edgartools` is used ONLY
   for `download_edgar_data()` (download helper). Bulk archive
   parsing uses stdlib `csv` / `xml.etree.ElementTree` /
   `json` — no Pydantic validation cliff. (Per 2026-05-05 prevention
   entry.)

## Acceptance criteria (Definition of Done)

The bulk-datasets-first bootstrap is **done** when:

1. Operator clicks "Run bootstrap" on a fresh DB and the system
   reaches `bootstrap_state.status='complete'` in <90 minutes on a
   100 Mbps connection.
2. `instruments`, `external_identifiers`, `filing_events`,
   `financial_facts_raw`, `instrument_sec_profile`,
   `ownership_institutions_observations`,
   `ownership_insiders_observations`, `ownership_funds_observations`,
   `def14a_filings`, `business_summaries`, `eight_k_items`,
   `sec_filing_manifest`, and `data_freshness_index` are all
   non-empty for AAPL, GME, MSFT, JPM, HD on dev DB.
3. Per-stage progress, ETA, error count, and bytes-downloaded are
   surfaced in the admin UI.
4. Slow-connection probe correctly routes to the per-CIK fallback
   when bandwidth <13 Mbps (verified by traffic-shaping a
   smoke run).
5. PR descriptions record the panel-of-5 verification + cross-source
   spot-check (per CLAUDE.md ETL clause 8–11).
6. `pytest tests/test_sec_bulk_*` covers (a) per-archive ingest unit
   tests against tiny golden fixtures, (b) end-to-end orchestration
   test using a mock bulk-archive HTTP server, (c) fallback test
   for slow-connection probe routing.
7. Each implementation PR has Codex pre-push review + APPROVE on
   the latest commit (per CLAUDE.md workflow).

## Implementation plan — 7 PRs

PRs are sized to land in 1–2 hours each so the orchestrator
landing PR can integrate them sequentially overnight.

### PR1: `sec_bulk_download` job invoker + slow-connection probe

- New file `app/services/sec_bulk_download.py` with
  `download_bulk_archives(*, target_dir, bandwidth_threshold_mbps)`.
- Throughput probe: HEAD `submissions.zip`, range-GET first 1 MB,
  measure throughput, return Mbps.
- Atomic write-then-rename pattern; resume-from-partial via
  HTTP range request.
- Disk space pre-flight via `shutil.disk_usage`.
- New job key `sec_bulk_download` registered in `_INVOKERS`.
- Tests: mock httpx server with Content-Length headers; assert
  partial-resume; assert atomic rename; assert disk-space refusal.
- Closes #N (umbrella)

### PR2: Phase C C1+C2 ingesters (submissions + companyfacts)

- New file `app/services/sec_submissions_ingest.py` —
  reads `submissions.zip` → `filing_events`.
- New file `app/services/sec_companyfacts_ingest.py` —
  reads `companyfacts.zip` → `financial_facts_raw` (per-instrument
  via `upsert_facts_for_instrument`).
- New job keys `sec_submissions_ingest` + `sec_companyfacts_ingest`.
- Test fixtures: tiny ZIPs with 3 CIKs each (AAPL, MSFT, GME)
  built deterministically in conftest.
- Verify: against the dev DB after run, AAPL `filing_events`
  count ≥100 and `financial_facts_raw` non-empty.
- Closes #N

### PR3: Phase C C3 13F dataset ingester

- New file `app/services/sec_13f_dataset_ingest.py`.
- Joins SUBMISSION.tsv + INFOTABLE.tsv; resolves CUSIP → instrument.
- Writes `ownership_institutions_observations`.
- Tests: tiny dataset ZIP fixture; assert AAPL/MSFT institutional
  rollups update.
- Cross-source spot-check: AAPL Q4-2025 institutional %-of-shares
  vs gurufocus.
- Closes #N

### PR4: Phase C C4 insider dataset ingester

- New file `app/services/sec_insider_dataset_ingest.py`.
- Forms 3, 4, 5 from Insider Transactions Data Sets.
- Writes `ownership_insiders_observations`.
- Tests: golden TSV fixture; assert insider transaction counts.
- Cross-source spot-check: AAPL CEO Q4-2025 transactions vs
  marketbeat.
- Closes #N

### PR5: Phase C C5 N-PORT dataset ingester

- New file `app/services/sec_nport_dataset_ingest.py`.
- Reads NPORT-P XML files; writes `ownership_funds_observations`.
- Reuses the stdlib-ElementTree parser from #917.
- Tests: golden NPORT-P XML fixture; assert fund holdings count.
- Closes #N

### PR6: Phase D pipelined fetcher + wire D1/D2/D3

- New file `app/services/sec_pipelined_fetcher.py`.
- Refactor existing `sec_def14a_bootstrap`,
  `sec_business_summary_bootstrap`, `sec_8k_events_ingest`
  invokers to accept an optional `fetcher` parameter; default
  remains sequential (preserves existing nightly behaviour).
- Bootstrap orchestrator passes the pipelined fetcher.
- Tests: assert concurrency cap; assert rate-limit honoured under
  load; assert each result carries the correct `key` (so callers
  can associate completion-ordered yields to original filings).
- Closes #N

### PR7: Bootstrap orchestrator integration + admin UI updates

- Refactor `app/services/bootstrap_orchestrator.py` to dispatch
  Phase A (A1 sequential first; then A2, A3, B1–B4 in parallel
  after A1 completes; bandwidth-probe gate inside A3), Phase C
  (C1.a + C2 in parallel after A3 join; then C3 + C4 + C5 in
  parallel after C2 completes; then C1.b sequential walk; then
  C6 sequential drain after C1.b completes), Phase D (pipelined
  D1–D3 after C completes), Phase E (E1, E2 after D).
- Refactor `frontend/src/components/admin/BootstrapPanel.tsx` to
  surface bytes-downloaded + per-archive progress for Phase A.
- Slow-connection banner: when probe routes to fallback, the panel
  shows "Slow connection (<13 Mbps detected). Using legacy path.
  ETA: ~3 hours."
- End-to-end test: mock SEC HTTP server with the full archive set;
  smoke run completes in <2 minutes against fixtures.
- Operator-visible verification on dev DB: panel-of-5 instruments
  rendered correctly post-bootstrap.
- Closes #N (umbrella)

## Open questions for Codex

1. Is the throughput probe accurate enough to discriminate 13 Mbps
   from typical jitter on a fast connection? Should it be a
   multi-probe smoothed measurement instead?
2. Does the bulk archive lag (T-1) violate any acceptance criterion
   given the post-Phase-C delta sweep covers it? Any edge cases
   where the delta sweep misses a filing?
3. Phase D pipelining: does sharing one rate budget across 4
   concurrent fetches actually maintain the documented 7 req/s
   floor under TCP retransmits or DNS hiccups? Recommend a more
   defensive concurrency cap (2 instead of 4)?
4. Is `xml.etree.ElementTree` strict enough for NPORT-P parsing?
   We deliberately picked stdlib over `lxml` to avoid a new
   dependency; #917 already uses ElementTree successfully.
5. Should Phase E E2 be removed entirely, or kept as a no-op
   that updates `data_freshness_index` for telemetry parity?
6. Is the disk-space pre-flight check at 30 GB sufficient given
   tmpfs / encrypted FS overhead, or should it be 50 GB?

## Settled-decisions deltas (if accepted)

This spec adds one new entry to `docs/settled-decisions.md`:

> **Bulk archives are the first-install seed only.** SEC publishes
> nightly bulk archives (submissions, companyfacts, 13F Data Sets,
> Insider Transactions Data Sets, N-PORT Data Sets). The bootstrap
> orchestrator pulls these in Phase A; per-filing fetches handle
> only the body content not in any archive (DEF 14A, 10-K, 8-K).
> Delta updates after bootstrap remain per-CIK per the existing
> nightly cadence — bulk archives are NOT a substitute for the
> per-CIK incremental ingest.
