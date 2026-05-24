# etoro_candles

**Class.** broker REST (NOT `ManifestSource` — broker market data; section 6 = N/A).
**Form / endpoint.** eToro broker REST OHLCV candles — `/api/v1/market-data/instruments/{instrument_id}/history/candles/asc/{interval}/{count}`.

## 1. Origin

eToro broker REST. URL pattern at `app/providers/implementations/etoro.py:188` (daily candles) and `app/providers/implementations/etoro.py:213` (intraday). Provider class `EtoroProvider` — entrypoint `EtoroProvider.get_daily_candles` at `app/providers/implementations/etoro.py:181`; intraday carve-out `get_intraday_candles` at `app/providers/implementations/etoro.py:195-218` (used by `app/services/intraday_candles.py` TTL cache, per `app/providers/implementations/etoro.py:11-16`). Raw response normalised by `_normalise_candles` at `app/providers/implementations/etoro.py:357-358`. Sibling broker provider `EtoroBrokerProvider` at `app/providers/implementations/etoro_broker.py:96` covers order execution (separate concern). Out of SEC ETL scope but DOES land in the ETL pipeline (universe + market data layers).

## 2. Watermarking model

Per-instrument latest `price_date` in `price_daily`. The refresh helper checks `MAX(price_date) FROM price_daily WHERE instrument_id = ...` at `app/services/market_data.py:262` and only fetches the delta. Per-instrument freshness gate at `app/services/market_data.py:211` (`Return True if price_daily already has the most recent trading day's candle`). No `external_data_watermarks` row maintained by this path — `price_daily` IS the watermark.

## 3. Retry posture

Inherits the broker provider's HTTP error handling. eToro REST quota is enforced broker-side (per-account, not per-IP). 401 → token refresh path (see broker auth wiring); 429 → back-off; 5xx → retry budget. Per-instrument failure isolated — `daily_candle_refresh` continues across the remaining cohort on a single-symbol error.

## 4. Bootstrap path

**Stage 2 in `_BOOTSTRAP_STAGE_SPECS`.** `_spec("candle_refresh", 2, "etoro", "daily_candle_refresh")` at `app/services/bootstrap_orchestrator.py:1039`. Cap requirement `CapRequirement(all_of=("universe_seeded",))` at `app/services/bootstrap_orchestrator.py:529`. No cap provided (`candle_refresh` not in `_PROVIDES_CAPS` at `app/services/bootstrap_orchestrator.py:356-460` — comment at 356 notes some stages provide nothing). Lane `etoro` (`_LANE_MAX_CONCURRENCY["etoro"] = 1` at `app/services/bootstrap_orchestrator.py:239`). Per `app/services/bootstrap_orchestrator.py:13-14`: "(S2 ``candle_refresh``) runs alongside the SEC reference lane" — the eToro lane is disjoint from `sec_rate` so cross-lane parallelism is preserved during bootstrap.

## 5. Steady-state path

`daily_candle_refresh` runs daily via the **sync_orchestrator DAG** (NOT `SCHEDULED_JOBS`). DAG layer mapping `daily_candle_refresh: ("candles",)` at `app/services/sync_orchestrator/registry.py:235`. Adapter wiring at `app/services/sync_orchestrator/adapters.py:200` (`job_name="daily_candle_refresh"`). High-frequency-sync orchestrator handles dispatch + freshness check (audit fresh window 24h per `app/services/sync_orchestrator/freshness.py:123`). Function entrypoint `daily_candle_refresh` at `app/workers/scheduler.py:1739`. Skips on missing eToro credentials via `_record_prereq_skip` at `app/workers/scheduler.py:1761-1764`. Lane: `etoro` (`app/jobs/sources.py:81-83` — eToro REST budget; `execute_approved_orders` + `daily_candle_refresh` + `etoro_lookups_refresh` + `exchanges_metadata_refresh` serialise under one `JobLock`).

Cohort (per `app/workers/scheduler.py:1828-1830`): held + T1/T2 + T3 bootstrap unique instruments.

## 6. Manifest insert

**N/A.** Broker REST market data. No `sec_filing_manifest` row written. `etoro_candles` is not listed in the `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`.

## 7. Parser

In-line normaliser `_normalise_candles` at `app/providers/implementations/etoro.py:357-358` produces `OHLCVBar` instances. Intraday variant `_normalise_intraday_candles` (per `app/providers/implementations/etoro.py:208-209`) preserves the per-interval shape. Drops bars with missing OHLC fields. NOT a `manifest_parsers/` entry.

## 8. Observation insert

Destination: **`price_daily`** UPSERT at `app/services/market_data.py:284-310`. Idempotent — re-running with the same bars produces no change (UPSERT with `IS DISTINCT FROM` guards per `app/services/market_data.py:306-307`). PK is `(instrument_id, price_date)`. Bars stored oldest-first (per `app/api/instruments.py:725-727`). Per-instrument fetch + UPSERT loop in `refresh_market_data_for_instruments` at `app/services/market_data.py:62`.

## 9. Current table refresh

**N/A** — `price_daily` IS the current view (latest row per `(instrument_id, price_date)`). No separate `_current` snapshot. The chart endpoint reads `price_daily` directly. No MERGE writer.

## 10. Operator-visible endpoint

`GET /instruments/{symbol}/candles?range={1w|1m|3m|6m|ytd|1y|5y|max}` at `app/api/instruments.py:708-744`. Response model `InstrumentCandles` (oldest-first bars). Reads from `price_daily` only — no provider fallback per `app/api/instruments.py:716-720` (`if we don't have local bars, return an empty row list and let the chart render an empty state. A 404 is reserved for an unknown symbol`). Server-side range resolution maps token to day lookback (`_CANDLE_RANGE_DAYS` map).

## 11. Verification queries

```sql
-- AAPL last 30 trading days.
SELECT price_date, open, high, low, close, volume
  FROM price_daily
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'AAPL')
 ORDER BY price_date DESC LIMIT 30;

-- Coverage gap — instruments with stale latest candle (>3 days).
SELECT i.symbol, MAX(p.price_date) AS latest
  FROM instruments i
  LEFT JOIN price_daily p ON p.instrument_id = i.instrument_id
 WHERE i.is_tradable = TRUE
 GROUP BY i.symbol
HAVING MAX(p.price_date) < CURRENT_DATE - INTERVAL '3 days'
    OR MAX(p.price_date) IS NULL
 ORDER BY latest NULLS FIRST LIMIT 50;
```

Smoke: `curl 'http://localhost:8000/instruments/AAPL/candles?range=1m' | jq '.bars | length'` should return ≥ 20 (rough trading-day count over 1 month).

Cross-source confirm: spot-check AAPL daily close against `https://finance.yahoo.com/quote/AAPL/history` for the same date.

## 12. Smoke test

Path: `tests/smoke/test_etl_source_to_sink.py::test_etoro_candles`. Asserts: `EtoroProvider` importable; `daily_candle_refresh` registered against the sync_orchestrator DAG (`JOB_TO_LAYERS["daily_candle_refresh"] == ("candles",)`); bootstrap stage S2 `candle_refresh` present in `_BOOTSTRAP_STAGE_SPECS`; `price_daily` table exists; `GET /instruments/AAPL/candles?range=1m` returns ≥1 bar against the seeded dev DB.

## 13. Known gotchas

1. **NOT a `ManifestSource`.** Broker REST, not SEC manifest. Section 6 = N/A by design.
2. **`etoro` lane is fully disjoint from `sec_rate`.** `_LANE_MAX_CONCURRENCY["etoro"] = 1` at `app/services/bootstrap_orchestrator.py:239`. eToro per-account REST quota is broker-side enforced; we don't share an IP-bucket with SEC. Cross-lane parallelism preserved during bootstrap (S2 candles runs alongside S3-S6 SEC reference stages).
3. **Sync orchestrator DAG, not `SCHEDULED_JOBS`.** Unlike FINRA refreshes, `daily_candle_refresh` is dispatched by the high-frequency-sync orchestrator. The function body in `scheduler.py` exists for the `_INVOKERS` registry — `_tracked_job` still wraps the run for tracking. Cron-based ScheduledJob entry does NOT exist for this job.
4. **Endpoint never falls back to provider.** `GET /instruments/{symbol}/candles` reads `price_daily` only. Empty result = empty bars list; 404 reserved for unknown symbol (`app/api/instruments.py:716-720`). The refresh job is the sole writer.
5. **Idempotent UPSERT with `IS DISTINCT FROM` guard.** Re-running for the same `(instrument_id, price_date)` is a no-op when bars are unchanged — important because the refresh job runs on every sync-orchestrator tick.
6. **Credentials gate.** `daily_candle_refresh` skips entirely if eToro credentials are missing (`app/workers/scheduler.py:1761-1764`). Operator must rotate via the broker-credentials admin path; bootstrap S2 silently skips on prereq fail.
7. **Intraday carve-out is separate.** `get_intraday_candles` (`app/providers/implementations/etoro.py:195-218`) is gated by TTL cache in `app/services/intraday_candles.py` — does NOT write to `price_daily` and is NOT part of `daily_candle_refresh`. Documented at `app/providers/implementations/etoro.py:11-16`.
8. **`max` range token returns every stored bar.** Operator-facing — make sure `price_daily` is partitioned-or-pruned for instruments with deep history (multi-year coverage on the full universe is non-trivial storage).
