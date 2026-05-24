# finra_regsho_daily

**Class.** FINRA caller-owned.
**Form / endpoint.** Daily Short Sale Volume CDN — `https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt`.

## 1. Origin

Anonymous CDN. Pipe-delim TEXT with CRLF line terminators. URL pattern at `app/providers/implementations/finra_regsho.py:100`. Provider class `FinraRegShoProvider` at `app/providers/implementations/finra_regsho.py:59`. Six prefixes per trade-date: `CNMS` (aggregate across facilities) + `FNQC` (TRF Chicago) + `FNRA` (legacy ADF, often empty) + `FNSQ` (TRF Carteret) + `FNYX` (NYSE TRF) + `FORF` (ORF) — tuple pinned at `app/providers/implementations/finra_regsho.py:48-56`. Service module path is `app/services/finra_regsho_ingest.py` (note: file is `finra_regsho_ingest.py` NOT `finra_regsho_daily_ingest.py`).

## 2. Watermarking model

No conditional-GET. Manifest is the watermark. Per-`(trade_date, prefix)` file → one synthetic manifest accession. ScheduledJob skips manifest-parsed `(trade_date, prefix)` pairs EXCEPT the two most-recent trade dates × 6 prefixes (revision window — FINRA corrects daily files in-place within 1-2 cycles per scheduler description at `app/workers/scheduler.py:1177-1180`).

## 3. Retry posture

Two HTTP statuses → `FinraNotFound` (benign skip, ScheduledJob re-fires next cron): `404` (file purged) AND **`403` (not yet published — empirically verified 2026-05-18 live-smoke)**. See `app/providers/implementations/finra_regsho.py:114-119, 129-130`. FINRA's RegSHO CDN returns **403 Forbidden** (not 404) for not-yet-published trade dates BEFORE the EOD ~6 PM ET publication window. Both statuses mean "no file at this URL" in the RegSHO taxonomy so running the cron earlier in the trading day doesn't generate spurious failures. `5xx` raises `httpx.HTTPStatusError`; `4xx other than 403/404` raises (true rate-limit/auth defect).

## 4. Bootstrap path

NOT a bootstrap stage. The RegSHO daily job does not seed under `_BOOTSTRAP_STAGE_SPECS` at `app/services/bootstrap_orchestrator.py:1035-1193`. Default backfill window 30 days per scheduler description at `app/workers/scheduler.py:1186-1187` (`Default backfill window 30 days; extended-window backfill via REPL runbook`). Partition coverage: 25 partitions at `sql/154_finra_regsho_daily.sql:74-95` cover 2024-Q1 → 2030-Q1 exclusive; sql/174 extends 20 more covering 2030-Q2 → 2035-Q1 exclusive (per `sql/174_finra_regsho_daily_partitions_2035.sql:1-37`). Total 45 partitions through 2035-Q1.

## 5. Steady-state path

`ScheduledJob(name=JOB_FINRA_REGSHO_DAILY_REFRESH, source="finra", cadence=Cadence.daily(hour=23, minute=0))` at `app/workers/scheduler.py:1167-1194`. Job-name constant at `app/workers/scheduler.py:339`. Daily 23:00 UTC (post EOD ~6 PM ET FINRA publication window). Prerequisite `_bootstrap_complete`. Lane is `finra` per `app/jobs/sources.py:276` — same lane as `finra_short_interest_refresh`; the two sibling refreshers serialise behind a shared JobLock + share the 1 req/s rate budget via the module-global throttle imported from the bimonthly module at `app/providers/implementations/finra_regsho.py:37-42`. Six fetches per trading day (one per prefix).

## 6. Manifest insert

`sec_filing_manifest.source = 'finra_regsho_daily'`. Listed in `ManifestSource` Literal at `app/services/sec_manifest.py:121`. `subject_type='finra_universe'`, `subject_id='FINRA_REGSHO'` (or analogous synthetic per-prefix accession). Synth no-op manifest UPSERT pattern mirrors the bimonthly sibling — direct UPSERT inside the per-file caller-owned transaction, NOT via `record_manifest_entry` (would raise `parsed → parsed` on revision-window re-fetch).

## 7. Parser

Synth no-op at `app/services/manifest_parsers/finra_regsho_daily.py`. Registered with `requires_raw_payload=False` at `app/services/manifest_parsers/finra_regsho_daily.py:90-93`. Pattern: ScheduledJob is the sole writer; manifest-worker dispatch exists ONLY to satisfy the dispatch invariant on the `sec_rebuild --source=finra_regsho_daily` path. Architectural sibling: `finra_short_interest` (G6/#915) + `sec_xbrl_facts` (G7).

## 8. Observation insert

`finra_regsho_daily_observations` (partitioned by `trade_date` quarterly). Schema at `sql/154_finra_regsho_daily.sql:31-70`. PK `(instrument_id, trade_date, market, source_document_id)` at `sql/154_finra_regsho_daily.sql:69`. Composite PK lets the CNMS aggregate (`market='B,Q,N'` comma-joined union) coexist with per-facility rows (`market='B'` etc.) for the same `(instrument_id, trade_date)` — both are distinct facts (per `sql/154_finra_regsho_daily.sql:17-22`). `market TEXT NOT NULL` is comma-joined on CNMS; single-char on per-facility files (`sql/154_finra_regsho_daily.sql:36-40`). Volume columns `NUMERIC(18, 6)` per `sql/154_finra_regsho_daily.sql:48-51` — FINRA reports per-symbol weighted aggregates to 6 decimal places (e.g. AAPL ShortVolume 8714049.111124 confirmed in spike §3.3). `source TEXT CHECK (source = 'finra_regsho')` single-element CHECK at `sql/154_finra_regsho_daily.sql:53-58` (short-form column value mirrors the bimonthly's `'finra_si'`; manifest enum uses long-form `'finra_regsho_daily'`). `source_document_id = '{PREFIX}_{YYYYMMDD}'`. Body-Date validation per row + footer-row-count validation per file (per spec §7.4).

## 9. Current table refresh

**No `_current` snapshot.** Documented at `sql/154_finra_regsho_daily.sql:12-15` — the daily file IS the per-day snapshot. Per-instrument latest-trade-date queries land on the partitioned observations table directly via the `idx_finra_regsho_obs_instrument_trade (instrument_id, trade_date DESC)` index at `sql/154_finra_regsho_daily.sql:98-99`. NOT in the `_CATEGORIES` 7-tuple at `app/jobs/ownership_observations_repair.py:69` — outside ownership-decomposition repair sweep scope.

## 10. Operator-visible endpoint

**Not yet wired.** No `/instruments/<symbol>/regsho` route exists under `app/api/`. Operator access today: SQL against `finra_regsho_daily_observations` (latest trade date over a window via the instrument-DESC index). Endpoint scaffold tracked under operator-visibility backlog.

## 11. Verification queries

```sql
-- Most recent 7 trading days for GME (CNMS aggregate row).
SELECT trade_date, market, short_volume, total_volume,
       short_volume / NULLIF(total_volume, 0) AS short_ratio
  FROM finra_regsho_daily_observations
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'GME')
   AND market = 'B,Q,N'
 ORDER BY trade_date DESC LIMIT 7;

-- Per-facility split for a single trade date.
SELECT market, short_volume, total_volume
  FROM finra_regsho_daily_observations
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'GME')
   AND trade_date = '2026-05-15'
 ORDER BY market;
```

Cross-source confirm: spot-check the CNMS aggregate short_volume against FINRA's published `https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files` UI for the same trade date.

## 12. Smoke test

Path: `tests/smoke/test_etl_source_to_sink.py::test_finra_regsho_daily`. Asserts: provider importable; parser registered (`registered_parser_sources()` contains `'finra_regsho_daily'`); ScheduledJob `JOB_FINRA_REGSHO_DAILY_REFRESH` exists in `SCHEDULED_JOBS`; table `finra_regsho_daily_observations` present in schema; six `PREFIXES` tuple pinned membership.

## 13. Known gotchas

1. **Caller-owned transaction.** Same contract as bimonthly sibling — service emits SQL only into the caller's open `with conn.transaction():`. NEVER calls `conn.commit()` / `conn.rollback()` internally.
2. **403 ≡ not-yet-published.** Critical empirical finding from #916 live-smoke. The provider treats `(403, 404)` as the same `FinraNotFound` benign-skip semantic. Running the cron earlier in the trading day (before the EOD ~6 PM ET FINRA publication window) generates HTTP 403 responses that MUST NOT propagate as job-failures.
3. **CNMS `market` is comma-joined.** Schema column is `TEXT`, not enum. CNMS rows carry `market='B,Q,N'` (or whatever facility union for that trade-date); per-facility rows carry single-char codes. PK includes `market` so both shapes coexist for the same `(instrument_id, trade_date)`.
4. **DECIMAL volumes, not INTEGER.** `NUMERIC(18, 6)` — FINRA reports per-symbol weighted aggregates to 6 decimal places. A naive `BIGINT` schema would have lost precision on every row.
5. **Shared rate budget with bimonthly.** Imports `_FINRA_RATE_LIMIT_CLOCK` / `_FINRA_RATE_LIMIT_LOCK` from `finra_short_interest` module. Combined fetch budget 1 req/s total.
6. **FNRA prefix is often empty body.** Legacy ADF (alt display facility). Treat zero-row payload as valid published file.
7. **Body-Date validation per row + footer-row-count validation per file.** Defends against truncated/mangled CDN payloads per spec §7.4. File-level fatal on mismatch; caller-owned txn rolls back; raw payload stays durable.
8. **Partition cliff at 2035-Q1.** Without sql/174 extension, INSERTs would have failed starting 2030-04-01 (DE IMP2 finding from #1233 ETL sweep committee). Next extension required pre-2035-Q2.
