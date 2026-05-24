# company_tickers_exchange

**Class.** SEC bulk reference (NOT `ManifestSource` — bulk reference; section 6 = N/A).
**Form / endpoint.** SEC ticker ↔ exchange directory (G8) — `https://www.sec.gov/files/company_tickers_exchange.json`.

## 1. Origin

Bulk JSON. URL constant `_EXCHANGE_URL` at `app/services/exchange_directory.py:64`. Fetcher `_fetch_directory` at `app/services/exchange_directory.py:67-73` uses the shared `SecFilingsProvider.fetch_document_text` (same `sec_rate` pool). Payload shape per `app/services/exchange_directory.py:18-25`:

```json
{
  "fields": ["cik", "name", "ticker", "exchange"],
  "data": [[1045810, "NVIDIA CORP", "NVDA", "Nasdaq"], ...]
}
```

CIKs arrive as integers; zero-padded to 10-digit TEXT per data-engineer I10 convention (`app/services/exchange_directory.py:27-28`). Service: `refresh_exchange_directory` at `app/services/exchange_directory.py:90`. Wired 2026-05-17 (G8 PR). Empirical row count: **10,353 rows / 7,996 unique CIKs / 1,446 multi-ticker CIKs** (BAC=17 variants, JPM=9). Ticker-grain — captures share-class siblings (GOOG / GOOGL), preferred-series tickers, ADR + OTC siblings (BABA / BABAF / BBAAY).

## 2. Watermarking model

**No conditional-GET in v1.** Documented at `app/services/exchange_directory.py:36-38`. ~1 MB daily fetch cost acceptable; ETag / Last-Modified plumbing deferred follow-up. Watermark surface: only `data_freshness_index` row(s). No `external_data_watermarks` row maintained by this module.

## 3. Retry posture

`RuntimeError` raised on empty body (`app/services/exchange_directory.py:70-71`). HTTP retries inherit the SEC provider's `ResilientClient` backoff. The bundled call from `daily_cik_refresh` runs inside a try/except that logs-but-does-not-raise on failure (`app/workers/scheduler.py:2047-2055`) so exchange directory refresh failure NEVER blocks equity-side CIK refresh. Returns early no-op on malformed `fields` / `data` shape per `app/services/exchange_directory.py:114-117`.

## 4. Bootstrap path

**Stage 6 sibling enrichment** — bundled inside `daily_cik_refresh` (`app/workers/scheduler.py:2044-2055`). Wired 2026-05-17 (G8 PR). NOT a separate `_BOOTSTRAP_STAGE_SPECS` stage — rides the same dispatch as the `cik_refresh` stage at S6. Lane `sec_rate`. Expected wall-clock <1 min (single ~1 MB JSON fetch + ~10k row UPSERT). Schema lives at `sql/150_cik_refresh_exchange_directory.sql` (created with G8 PR).

## 5. Steady-state path

Bundled into `daily_cik_refresh` as Stage 7 sibling enrichment (`app/workers/scheduler.py:2044-2055`) — fires on every daily CIK refresh cron run regardless of which equity branch was taken (G8 restructure made sibling enrichments fire unconditionally per `app/workers/scheduler.py:1893-1896`). Lane `sec_rate`.

## 6. Manifest insert

**N/A.** Bulk reference source. No `sec_filing_manifest` row written. `company_tickers_exchange` is not listed in the `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`.

## 7. Parser

In-line parser inside `refresh_exchange_directory` at `app/services/exchange_directory.py:90-130+`. Walks `payload["data"]` by `payload["fields"]` indices. Per-field tolerance per `app/services/exchange_directory.py:119-120` — absent required field → safe no-op. Per-cell text-coercion via `_coerce_text` at `app/services/exchange_directory.py:76-87` defends against SEC ever emitting a numeric in a TEXT-typed column (without `isinstance` guard, a numeric ticker would raise `AttributeError`). NOT a `manifest_parsers/` entry.

## 8. Observation insert

Destination: **`cik_refresh_exchange_directory`** keyed by `(cik, ticker)` per `app/services/exchange_directory.py:7-11`. Ticker-grain because a single CIK can produce multiple rows for share-class siblings, preferred-series tickers, and ADR + OTC siblings. NOT keyed by CIK alone — that would have collapsed BAC's 17 ticker variants into one row.

## 9. Current table refresh

**N/A.** `cik_refresh_exchange_directory` IS the current table — snapshot semantics, not append-only history. UPSERT advances `last_seen` on every observed row; rows SEC drops from the payload remain in the table with an older `last_seen` (per `app/services/exchange_directory.py:31-34`). No DELETE / mark-stale in v1 — matches the MF directory precedent. No MERGE writer.

## 10. Operator-visible endpoint

No dedicated endpoint. Coverage surfaces indirectly via:
- Exchange field on `GET /instruments/<symbol>` (where consumed by downstream resolvers).
- `GET /system/bootstrap-status` (S6 `cik_refresh` row state — covers the bundled exchange enrichment).
- `GET /admin/data-freshness` (panel reads `data_freshness_index`).

## 11. Verification queries

```sql
-- NVDA exchange resolution.
SELECT cik, ticker, exchange, name, last_seen
  FROM cik_refresh_exchange_directory
 WHERE ticker = 'NVDA';

-- BAC multi-ticker coverage (should be ~17 variants).
SELECT ticker, exchange, last_seen
  FROM cik_refresh_exchange_directory
 WHERE cik = '0000070858'  -- BAC
 ORDER BY ticker;

-- Directory size + multi-ticker CIK sanity.
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT cik) AS unique_ciks,
       SUM(CASE WHEN ticker_count > 1 THEN 1 ELSE 0 END) AS multi_ticker_ciks
  FROM (
    SELECT cik, COUNT(*) AS ticker_count
      FROM cik_refresh_exchange_directory
     GROUP BY cik
  ) t;
```

Cross-source confirm: spot-check NVDA → Nasdaq against `https://www.nasdaq.com/market-activity/stocks/nvda` directly.

## 12. Smoke test

Path: `tests/smoke/test_etl_source_to_sink.py::test_company_tickers_exchange`. Asserts: `refresh_exchange_directory` importable; bundled sibling enrichment present in `daily_cik_refresh` body; table `cik_refresh_exchange_directory` keyed `(cik, ticker)` present in schema; empirical-shape sanity (>5k rows; ≥1 multi-ticker CIK).

## 13. Known gotchas

1. **Bulk reference — NOT a `ManifestSource`.** Section 6 = N/A by design.
2. **Ticker-grain key, not CIK-grain.** PK is `(cik, ticker)` so BAC's 17 ticker variants coexist. CIK-keyed schema would have silently collapsed share-class siblings, preferred-series, and ADR + OTC siblings.
3. **No conditional-GET in v1.** Every daily run fetches the full ~1 MB JSON. Acceptable today.
4. **Snapshot semantics, not append-only.** Stale rows persist with older `last_seen`. Consumers needing freshness gate filter on `last_seen >= cutoff`. No history retention.
5. **Fail-soft from `daily_cik_refresh`.** Failure of `refresh_exchange_directory` logs-but-does-not-raise (`app/workers/scheduler.py:2054-2055`). G8 ticket; an exchange-refresh error MUST NOT block equity-side CIK refresh.
6. **Per-cell text coercion required.** `_coerce_text` at `app/services/exchange_directory.py:76-87` returns `None` for non-string inputs. Without the `isinstance` guard, a numeric ticker would raise `AttributeError: 'int' object has no attribute 'strip'`. Defends against future SEC payload type drift.
7. **CIKs arrive as integers, stored as zero-padded TEXT.** Without zero-pad, joins against canonical 10-digit TEXT shape silently miss.
8. **Raw bytes not retained.** Parsed-snapshot pattern, not raw-payload sink. The raw-payload prevention rule (`docs/review-prevention-log.md:1171`) targets per-filing ingest writers, not reference-directory aggregates. If exact-bytes retention becomes a future requirement, expand `cik_raw_documents.document_kind` enum (per `app/services/exchange_directory.py:40-45`).
