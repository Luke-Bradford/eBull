# cboe_vix

**Class.** Cboe public reference.
**Form / endpoint.** Official VIX daily OHLC history CSV — `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv`.

## 1. Origin

Cboe's [VIX historical-data page](https://www.cboe.com/tradable_products/vix/vix_historical_data) links the public `VIX_History.csv` download and describes it as daily closing history updated daily. `app/services/cboe_vix.py` fetches that CSV directly. This is the primary Cboe source, not the key-gated FRED derivative and not an eToro futures instrument. Cboe retains copyright: eBull stores it for attributed internal research and decision context and does not expose a raw-history download.

## 2. Watermarking model

`external_data_watermarks` key `(source='cboe.vix-history', key='VIX')` records `Last-Modified` (or the last bar date when the header is absent), parsed modification time when supplied, and SHA-256 response hash. A later request sends `If-Modified-Since`; HTTP 304 is a successful no-op. The data table remains the authoritative retained coverage, rather than inferring freshness from a global maximum date.

## 3. Retry posture

HTTP errors propagate and the tracked scheduled job records a failure. The normal scheduler retry/catch-up path revisits it; there is no aggressive in-process retry against this once-daily public file. HTTP 304 is a benign no-op. An unexpected header, ragged row, duplicate date, non-positive/non-finite number, invalid date, or impossible OHLC envelope rejects the complete response before any data mutation.

## 4. Bootstrap path

There is no bootstrap stage and no dependency on instrument-universe completion beyond the standard bootstrap-complete prerequisite. The first scheduled catch-up loads only bars dated 2021-01-01 onward into the existing research-series store. This is about 1,400 rows in 2026, not the source's full 1990-present history.

## 5. Steady-state path

`JOB_CBOE_VIX_REFRESH` runs daily at 02:12 UTC on the dedicated `cboe` lane, catches up on worker boot, and performs one conditional request. It reconciles a single bounded series. The time is after the preceding US session and does not imply that a same-date close was historically observable.

## 6. Manifest insert

None. This is a small job-owned reference series, not an accession/document stream, so it does not create `sec_filing_manifest` rows. Source provenance and conditional-fetch state live in `research_price_series` and `external_data_watermarks` respectively.

The raw CSV is intentionally dropped after complete normalization. Every source field in the retained contract lands as typed SQL OHLC/date data, while the response hash, URL/version and modification watermark preserve identity. This follows the explicit #470/#471 narrowing in `docs/review-prevention-log.md`: raw persistence is redundant once SQL normalization is complete. Keeping a revised full-history CSV each day would duplicate the same ~1,439 retained observations and conflict with the bounded-storage requirement.

## 7. Parser

`parse_vix_history` in `app/services/cboe_vix.py` requires the exact header `DATE,OPEN,HIGH,LOW,CLOSE`, parses dates as `%m/%d/%Y`, discards pre-2021 rows, then validates retained positive finite decimals and OHLC envelopes, rejects retained duplicates, and sorts ascending. Cboe's legacy file includes at least one published 1992 OHLC anomaly; it is explicitly outside the bounded contract and cannot block or widen the retained load. `SOURCE_VERSION='cboe-vix-daily-close-v1'` freezes this contract.

## 8. Observation insert

One `research_price_series` row uses `(vendor='cboe', vendor_symbol='VIX')`, no `instrument_id`, `upstream_source='other'`, and an explicit Cboe/internal-research licence label. `research_price_daily` stores one nullable-volume OHLC row per retained date. The natural `(series_id, bar_date)` conflict key makes refreshes idempotent; unchanged OHLC values are not rewritten. Dates removed from the bounded upstream response are reconciled within the retained range in the same transaction.

## 9. Current table refresh

There is no `_current` table and no stored rolling indicator. The series row carries compact first/last/count coverage. `load_vix_close_as_known` performs an indexed descending lookup and requires `bar_date < decision_at`'s New York date. Thus date D is usable only on a later New York date, conservatively preventing same-close lookahead even for after-close simulations.

## 10. Operator-visible endpoint

None. VIX is machine decision context, not a raw-data product. `load_decision_vix` resolves the exact prior-NYSE-session close into `DecisionVix`; eligible fired/refused context rows snapshot only `vix`, `vix_bar_date` and `vix_source_version`. Missing or stale coverage produces a typed refusal. Strategy evidence may expose those provenance fields and the refusal reason; it must not expose or redistribute the complete raw Cboe history.

## 11. Verification queries

```sql
SELECT vendor, vendor_symbol, licence, first_bar, last_bar, bar_count
FROM research_price_series
WHERE vendor = 'cboe' AND vendor_symbol = 'VIX';

SELECT d.bar_date, d.open, d.high, d.low, d.close
FROM research_price_series s
JOIN research_price_daily d USING (series_id)
WHERE s.vendor = 'cboe' AND s.vendor_symbol = 'VIX'
ORDER BY d.bar_date DESC
LIMIT 5;

SELECT source, key, watermark, watermark_at, response_hash, updated_at
FROM external_data_watermarks
WHERE source = 'cboe.vix-history' AND key = 'VIX';
```

Cross-check the latest row against Cboe's linked CSV. A decision on New York date D must resolve at most D-1 (or the preceding trading date), never D.

Live acceptance on 2026-08-12 loaded 1,439 rows from 2021-01-04 through 2026-08-11. The retained row payload measured 92,002 bytes; the indexed as-known lookup executed in 0.417 ms on the development database. Initial-load WAL was 4,587,384 bytes, while an immediate conditional repeat returned HTTP 304 and generated 3,744 bytes of watermark WAL. The as-known resolver returned 2026-08-10 for a 2026-08-11 New York decision and 2026-08-11 for a 2026-08-12 decision.

Decision-context acceptance on 2026-08-12 resolved close 15.28 / bar 2026-08-11 / source `cboe-vix-daily-close-v1` from the live store. The two added provenance scalars measure 28 bytes over an all-null composite fixture (52 versus 24 bytes), are written only on fired/refused decisions, add no index, and left the empty context table at 49,152 bytes. The follow-up constraint migration generated 12,096 WAL bytes; the live lookup measured 1.44 ms on that run.

## 12. Smoke test

`tests/test_cboe_vix.py` covers exact-schema rejection, numeric/OHLC validation, duplicate rejection, retention, same-date causal refusal, transactional idempotent storage, conditional requests, and the database as-known lookup. `tests/test_workers_scheduler_registry.py` covers cadence, lane, prerequisite, catch-up, and runtime invoker registration.

## 13. Known gotchas

1. VIX is a volatility-regime/risk context feature, not an entry signal and not a tradable eToro position.
2. The history file has dates but no per-row publication timestamps. The resolver therefore imposes the conservative next-New-York-date availability rule.
3. VIX levels are non-stationary. Strategy research should use causal transforms or frozen regime bands fitted only on training data, never tune thresholds over the complete evaluation period.
4. Cboe copyright and attribution remain applicable. Do not add a raw series export or UI history dump.
5. Keep retention bounded to 2021 onward unless a separate, evidenced research contract justifies older regimes and accounts for the storage/validation cost.
