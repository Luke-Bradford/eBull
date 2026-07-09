# FINRA — source-of-truth note

> Status: introduced 2026-05-18 alongside PR for #915 (Phase 6 PR 11).
> First non-SEC data provider family.
> Cross-reference: `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 (`finra_short_interest` row) + §7 G6.

## 1. What FINRA publishes (in-scope for eBull v1)

| Endpoint | Cadence | URL pattern | Status |
|---|---|---|---|
| Equity Short Interest (bimonthly) | 15th + last business day | `https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv` | ✅ WIRED 2026-05-18 (#915) |
| RegSHO Daily Short Volume | Daily, EOD ~6pm ET | `https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt` × 6 prefixes | ✅ WIRED 2026-05-18 (#916) |

Both endpoints are **anonymous CDN** — no OAuth, no API key, no developer-portal registration. The catalog page IS the contract; `cdn.finra.org/robots.txt` returns 403 but the public catalog page does not restrict programmatic anonymous download.

## 2. File format gotchas

### 2.1 `.csv` extension lies — files are pipe-delimited

Despite the `.csv` extension on bimonthly files, they are **pipe-delimited (`|`)**. Parse via `csv.DictReader(stream, delimiter='|')`. Empirically verified 2026-05-18 against `shrt20260430.csv` (~10k rows, 14 columns).

### 2.2 Bimonthly file 14-column header (exact)

```
accountingYearMonthNumber|symbolCode|issueName|issuerServicesGroupExchangeCode|marketClassCode|currentShortPositionQuantity|previousShortPositionQuantity|stockSplitFlag|averageDailyVolumeQuantity|daysToCoverQuantity|revisionFlag|changePercent|changePreviousNumber|settlementDate
```

Header mismatch / missing field = file-level fatal (`HeaderCorruptionError` in `app/services/finra_short_interest_ingest.py`).

### 2.3 RegSHO daily 6-column header (exact)

```
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
```

Six prefixes per day (`CNMSshvol{YYYYMMDD}.txt`, `FNQCshvol...`, `FNRAshvol...`, `FNSQshvol...`, `FNYXshvol...`, `FORFshvol...`) — each represents a separate reporting facility (Consolidated NMS, FINRA/NASDAQ TRF Chicago, ADF, FINRA/NASDAQ TRF Carteret, FINRA/NYSE TRF, ORF respectively). Operator UI typically aggregates `CNMS` as the canonical figure; the others are facility-attribution detail.

### 2.4 RegSHO daily — decimal volumes + comma-joined `Market` on CNMS (#916)

Empirically verified 2026-05-18 in spike §3.3/§3.4: `ShortVolume`, `ShortExemptVolume`, `TotalVolume` are **DECIMAL** (6 decimal places), not integer. FINRA reports per-symbol weighted aggregates across reporting facilities. Schema column is `NUMERIC(18, 6)`.

`Market` column shape:

- Non-CNMS prefixes: single-character facility code (e.g. `B`, `Q`, `N`, `O`).
- CNMS aggregate: **comma-joined union** of all facilities reporting volume for that `(symbol, trade_date)` (e.g. `B,Q,N`). PK on `finra_regsho_daily_observations` includes `market` so the CNMS aggregate row coexists with per-facility breakdown rows for the same `(instrument, trade_date)`.

### 2.5 RegSHO daily — footer is single-int row count (#916)

Every RegSHO daily file (CNMS + 5 per-facility prefixes) ends with a single line containing the integer count of body rows. Parser asserts `parsed_body_row_count == footer_int` and raises `HeaderCorruptionError` on mismatch (structural defect). Empty-body files (FNRA legitimate-empty shape) have header + footer `0` — success path with zero observations + manifest row still written.

### 2.6 RegSHO daily — body `Date` must match URL/caller date (#916)

For every body row, the parser asserts `row.Date == trade_date.strftime('%Y%m%d')`. A CDN path mistake or fixture seeded under the wrong date would silently write facts under the caller's `trade_date` while ignoring the body's date column. Mismatch raises `HeaderCorruptionError` mid-body — caller's txn rolls back atomically.

### 2.7 RegSHO daily — CDN returns 403 (not 404) for not-yet-published files (#916)

Empirically verified 2026-05-18 in live-smoke against `cdn.finra.org/equity/regsho/daily/`: requesting a file for a trade date BEFORE the EOD ~6 PM ET publication window returns **HTTP 403 Forbidden**, not 404. This is **different from the bimonthly CDN** (`/equity/otcmarket/biweekly/`) which returns 404 for missing files.

Provider `FinraRegShoProvider.fetch_regsho_daily_file` maps **both 403 + 404 → `FinraNotFound`** so the cron can safely run before EOD publication. The bimonthly provider only maps 404 (no observed 403 behaviour). Future FINRA endpoints should default to 403+404 = not-found UNLESS empirically verified otherwise — FINRA appears to use 403 as a "missing object" idiom on the RegSHO sub-host.

**Cross-source 403 idiom:** SEC's `efts.sec.gov` exhibits a similar "403 ≠ permanent" pattern on weekends — see `.claude/skills/data-sources/sec-edgar.md` §4 "Multi-host shared clock" for the SEC analogue. **Rule for new sources:** never default-treat 403 as "permanently unavailable" without an empirical probe against a known-good vs known-not-yet-published case for that specific CDN/host. The cost of mis-classifying transient 403 as permanent is silent gap; the cost of treating permanent 403 as transient is wasted budget. Probe both directions.

### 2.8 Encoding contract per file

| File | Encoding | Validation site |
| --- | --- | --- |
| Bimonthly `shrt{YYYYMMDD}.csv` (pipe-delimited) | UTF-8 | `finra_short_interest_ingest.py:189` (`raw_bytes.decode("utf-8")`) |
| RegSHO daily `{PREFIX}shvol{YYYYMMDD}.txt` (pipe-delimited) | UTF-8 | `finra_regsho_ingest.py:112` (`raw_bytes.decode("utf-8")`) |

Both files are pure ASCII in practice (ticker symbols + integers + dates + decimals — no diacritics or non-Latin chars). UTF-8 decode is a defensive contract; any decode error on a 200-response body indicates fixture corruption or a FINRA encoding-change incident — bubble to `HeaderCorruptionError`, do NOT silent-replace with `errors='replace'`. SEC's `getcurrent` Atom feed uses `ISO-8859-1` for filer names with diacritics; FINRA's universe is US-only and has no analogue.

### 2.9 Symbol form — alphanumeric only, NO separators

FINRA strips dot / hyphen / underscore from share-class siblings + preferreds:

- `ABRPRD` (FINRA) = Arbor Realty Trust **Preferred D** → vendor `ABR.PR.D` / `ABR-D`
- `ALLPRB` (FINRA) = Allstate Preferred B → vendor `ALL-PR-B`
- `BRKA` (FINRA) ≠ `BRK.A` (our `instruments.symbol` dotted form)

Resolution discipline at the service layer: strip non-alphanumerics + upper-case on BOTH sides before matching. The G6 PR uses `app.services.finra_short_interest_ingest.normalise_symbol` (public name — no leading underscore; `finra_short_interest_ingest.py:105`).

Collision handling: two instruments whose symbols collapse to the same normalised key (e.g. `ABR.PRD` + `ABRPRD` both → `ABRPRD`) get tracked in `ambiguous_keys` and the offending FINRA row is skipped with `skipped_ambiguous_symbol` incremented.

### 2.10 Cohort cliff — pre-June 2021 archive is OTC-only

Pre-June 2021 FINRA short interest files included **only OTC securities** (FINRA was the only SRO publishing for OTC). Post-June 2021 the file expanded to include exchange-listed securities. The GME 2021-Q1 squeeze settlement window is therefore **NOT in the bimonthly archive** as an exchange-listed cohort entry — only its OTC tape (which doesn't include exchange-listed `GME`). Smoke targets MUST land in the post-2021-06 cohort (e.g. 2021-07-15 first, 2026-04-30 most recent).

## 3. Rate-limit posture

FINRA publishes no explicit rate limit on the equity short interest catalog page. Default polite floor in eBull: **1 req/s** (`_FINRA_MIN_INTERVAL_S = 1.0` at `app/providers/implementations/finra_short_interest.py:50`, passed as `min_request_interval_s`).

`cdn.finra.org` is a **separate host** from SEC EDGAR (`data.sec.gov` / `www.sec.gov`). Rate budgets do NOT share. eBull's `Lane` Literal includes a dedicated `finra` lane (added 2026-05-18 alongside #915) so:

- Bumping FINRA fetch concurrency does not burn SEC budget.
- The module-global `_FINRA_RATE_LIMIT_CLOCK` + `_FINRA_RATE_LIMIT_LOCK` are shared across every `FinraShortInterestProvider` instance in-process (prevention-log #726 invariant).

## 4. Architecture in eBull — Option A (ScheduledJob owns writes)

FINRA bimonthly short interest:

- **ScheduledJob** `finra_short_interest_refresh` (daily 12:00 UTC) — owns discovery + fetch + parse + UPSERT into `finra_short_interest_observations` + `finra_short_interest_current`.
- **Manifest parser** `app/services/manifest_parsers/finra_short_interest.py` — **synth no-op** (sec_xbrl_facts G7 precedent). Manifest row dispatches only on `sec_rebuild --source=finra_short_interest`; the parser marks the row `parsed` without doing any real work.
- **Backfill** — REPL runbook against `run_finra_short_interest_refresh(conn, backfill_window_days=N)`. v1 manual-trigger surface is zero-param (POST `/jobs/finra_short_interest_refresh/run` runs the default 400-day window).

FINRA RegSHO daily short volume (#916):

- **ScheduledJob** `finra_regsho_daily_refresh` (daily 23:00 UTC) — owns discovery + fetch + parse + UPSERT into `finra_regsho_daily_observations` (no `_current` snapshot; daily file IS the snapshot). Iterates `(weekday trade_date, prefix)` pairs across the 6 prefixes per day.
- **Provider sibling** `app/providers/implementations/finra_regsho.py` — imports `_FINRA_RATE_LIMIT_CLOCK` + `_FINRA_RATE_LIMIT_LOCK` from the bimonthly module so combined bimonthly + daily fetch never exceeds 1 req/s in-process.
- **Manifest parser** `app/services/manifest_parsers/finra_regsho_daily.py` — synth no-op (same G7 / #915 shape). `parser_version='finra-regsho-daily-v1'`. Subject_id singleton `FINRA_REGSHO`.
- **Backfill** — REPL runbook against `run_finra_regsho_daily_refresh(conn, backfill_window_days=N)`. v1 manual-trigger surface is zero-param (default 30-day window). Extended backfill via REPL (~6 min per 90 days at 1 req/s × 6 prefixes × ~63 trading days).

Per spec §4 + plan §5: the ScheduledJob owns commit/rollback ownership. The service emits SQL only and is wrapped in the JOB's `with conn.transaction():`. Raw-payload-before-parse contract (#1168): `raw_filings.store_raw` + explicit `conn.commit()` happens BEFORE the per-file txn.

## 5. Revision-window discipline

FINRA publishes **in-place revisions** to bimonthly files (`revisionFlag='Y'`) within 1-2 cycles of the original publication. The ScheduledJob's `_compute_targets` re-fetches the two most-recent candidate dates ALWAYS, regardless of manifest-parsed status, so an in-place revision lands on the next cron fire after FINRA publishes the correction.

## 6. Operator runbooks

### 6.1 First-time bootstrap or extended-window backfill

```bash
docker exec -it ebull-postgres bash  # OR python -c ... locally
python -c "
from psycopg import connect
from app.config import settings
from app.jobs.finra_short_interest_refresh import run_finra_short_interest_refresh
with connect(settings.database_url) as c:
    print(run_finra_short_interest_refresh(c, backfill_window_days=730))
"
```

~48 settlement files at 1 req/s = ~48s wall-clock for a 2-year backfill.

### 6.2 Force-rebuild a specific settlement date

The synth no-op manifest parser doesn't re-ingest — it just re-marks `parsed`. To force re-ingest of one settlement date:

1. `DELETE FROM finra_short_interest_observations WHERE settlement_date = '<date>'` (and `_current` if needed).
2. `DELETE FROM filing_raw_documents WHERE accession_number = 'FINRA_SI_<YYYYMMDD>'` (so `store_raw` re-fetches the body).
3. `UPDATE sec_filing_manifest SET ingest_status='pending' WHERE accession_number = 'FINRA_SI_<YYYYMMDD>'` (cosmetic — gets re-set to `parsed` after the next ScheduledJob fire).
4. Re-fire the ScheduledJob: `POST /jobs/finra_short_interest_refresh/run` (the date is in the revision window if recent, OR within the 400-day window for older dates).

### 6.3 RegSHO daily — extended-window backfill (#916)

```bash
python -c "
from psycopg import connect
from app.config import settings
from app.jobs.finra_regsho_daily_refresh import run_finra_regsho_daily_refresh
with connect(settings.database_url) as c:
    print(run_finra_regsho_daily_refresh(c, backfill_window_days=90))
"
```

90 trading days × 6 prefixes ≈ 378 fetches at 1 req/s ≈ ~6 min wall-clock. Coexists with bimonthly throttle budget — the shared module-global clock serialises.

### 6.4 RegSHO daily partition extension (deadline: before 2035-Q2)

`sql/154` created quarterly partitions 2024-Q1 → 2030-Q1; `sql/174` extended the tail to 2035-Q1 (adds 2030-Q2 → 2035-Q1). Current window: **2024-Q1 → 2035-Q1 inclusive**. The daily cron hard-fails on the first 2035-Q2 trade date unless another migration extends the window before then. The bimonthly sibling `finra_short_interest_observations` shares the hazard — `sql/152` + `sql/176` cover 2021-Q3 → 2035-Q1 with NO DEFAULT partition — so extend both in the same migration. Operator runbook:

1. Author a new migration `sql/NNN_finra_regsho_daily_partition_extension.sql` that adds `2035-Q2` → later quarterly partitions to `finra_regsho_daily_observations` (follow the `sql/174` idempotent `CREATE TABLE IF NOT EXISTS … PARTITION OF … FOR VALUES FROM (…) TO (…)` loop).
2. Apply via the standard migration path; verify partition count via `SELECT count(*) FROM pg_inherits WHERE inhparent='finra_regsho_daily_observations'::regclass`.

### 6.5 Cross-source sanity check (one ticker)

After ingest, compare `current_short_interest` against:

- `marketbeat.com` short-interest page for the ticker
- `shortsqueeze.com` daily-update page
- `nasdaq.com/market-activity/stocks/{symbol}/short-interest`

±5% tolerance acknowledged (off-source reporting cadence drift; the FINRA value IS authoritative per regulatory definition).

## 7. What's intentionally NOT covered

- **Short borrow rate / utilisation** — vendor-paid data (S3, IHS Markit). Out of scope per settled-decisions #532 (free regulated-source-only).
- **Per-broker short interest disaggregation** — not published by FINRA.
- **ETF short rebate dynamics** — vendor-paid; out of scope.
- **Real-time / intraday short interest** — FINRA publishes EOD only.

## 8. Forward references

- ~~`#916` adds RegSHO daily ingest sibling — same `finra` Lane, separate cadence + schema.~~ ✅ Shipped 2026-05-18; see §4 daily entry + §6.3 runbook.
- Memo overlay UI surface (ownership card "Short interest X% of float, days-to-cover Y") is OBSERVATIONS-PRIMITIVE-deferred per spec §1 closure framing — re-open when ownership-card UI revisit lands.
