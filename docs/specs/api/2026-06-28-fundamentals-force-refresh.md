# Fundamentals targeted force-refresh API (#677 Part A)

Status: live spec (shipping). Part B (filing-poll watchlist / `expected_filings`) deferred to a follow-up ticket.

## Problem

SEC fundamentals refresh is gated by `plan_refresh()` (`app/services/fundamentals/__init__.py:2027`): only CIKs whose top-accession differs from the stored watermark get refreshed. So a `TRACKED_CONCEPTS` / allowlist extension does **not** backfill existing instruments until SEC ships them a fresh filing — the operator has no targeted way to say "re-extract IEP, ET, EPD now."

## Source rule

No SEC reg governs the *trigger* shape (it's an operator action, not a data-treatment decision). The *data treatment* is unchanged and governed by our settled "Fundamentals provider posture" (`docs/settled-decisions.md` — free regulated-source-only, #532): the endpoint reuses the exact `refresh_financial_facts` → `normalize_financial_periods` path the daily job uses, including the existing `TRACKED_CONCEPTS` whitelist and retention rules — it only bypasses the `plan_refresh` watermark gate. SEC fair-use: 10 req/s shared throttle (`_PROCESS_RATE_LIMIT_CLOCK` in `sec_edgar.py`) is enforced inside the provider — unchanged.

## What ships

### `POST /admin/fundamentals/refresh`

Body: `{ "symbols": ["IEP", "ET", ...] }`. Auth: `require_session_or_service_token` (matches `/jobs/{name}/run`, `/admin/business-summary-failures/*`).

Behaviour:
1. Validate `symbols` non-empty, `<= 50` (a work-bounding safety cap, not a latency guarantee — companyfacts latency + normalization volume vary). 400 on empty/oversize. Uppercase + dedup the requested list (case-insensitive) before resolving — duplicate symbols collapse to one entry in `results` and one SEC fetch.
2. Resolve each distinct symbol → `(symbol, instrument_id, primary_sec_cik)` in **one** query. Match `UPPER(i.symbol)` (operator input may be lowercase; mirrors the existing instrument-page resolvers) and pick the canonical primary listing via `DISTINCT ON` so a duplicate/retired listing row can't shadow it. This is a one-shot manual admin call, not a per-tick hot path, so the `UPPER()` scan over ~12k rows is acceptable (prevention-log #1186 targets per-tick resolvers):
   ```sql
   SELECT DISTINCT ON (UPPER(i.symbol))
          UPPER(i.symbol) AS symbol, i.instrument_id, ei.identifier_value AS cik
   FROM instruments i
   JOIN external_identifiers ei
     ON ei.instrument_id = i.instrument_id
    AND ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary = TRUE
   WHERE UPPER(i.symbol) = ANY(%(symbols)s)
   ORDER BY UPPER(i.symbol), i.is_primary_listing DESC, i.instrument_id ASC
   ```
   Symbols with no row (unknown instrument OR no primary SEC CIK) → reported per-symbol as `resolved=false`, not fetched. NOT a 404 for the whole call (partial-resolution is normal). `is_tradable` is **not** filtered — the operator explicitly named the symbol, so a retired-but-named instrument is still refreshable; the `DISTINCT ON` primary-listing winner already prevents shadow duplicates.
3. `refresh_financial_facts(provider, conn, triples)` — always re-fetches companyfacts, writes `financial_facts_raw`. Bypasses `plan_refresh`.
4. `normalize_financial_periods(conn, instrument_ids=[resolved ids])` — re-derive `financial_periods` from the now-current raw store. Normalization runs over **all** resolved ids regardless of per-fetch outcome: it is idempotent — an instrument whose fetch failed simply re-derives from its existing raw store (no corruption, no new data). `normalize_financial_periods` swallows per-instrument exceptions internally (logged server-side) and returns only success counts; the endpoint surfaces `periods_canonical_upserted` (actual successes) so a shortfall is visible, and `symbols_failed` (fetch/parse failures). A silently-failed normalize is logged, not counted — acceptable for a manual admin tool the operator can re-run; documented as a known limitation.
5. Return per-symbol results + a roll-up.

`results` carries one entry per **distinct requested symbol** (request order, resolved entries then unresolved). Two distinct tickers that map to the same `instrument_id` (e.g. `GOOG`/`GOOGL`) both appear as `resolved=true`, but the SEC fetch + normalization de-dups by `instrument_id` so the work runs once — the batch counts reflect the single fetch, the per-symbol `results` reflect every requested symbol.

### Concurrency

No advisory lock. Two concurrent force-refreshes (or an overlap with the daily job) of the same symbol are safe: `financial_facts_raw` writes are upserts (idempotent, last-writer-wins identical data) under per-instrument savepoints, and normalization is idempotent. Worst case is duplicated work, not corruption.

### Connection model

The handler is a **sync `def`** (FastAPI runs it in the threadpool — no event-loop block). It opens a dedicated `connect_job()` connection for the multi-second SEC fetch, NOT the pooled `get_conn` dependency — holding a pooled request connection across 5-60s of SEC I/O would starve the request pool. `refresh_financial_facts` commits its ledger row early and runs per-instrument savepoint transactions; we `conn.commit()` after `normalize_financial_periods` (psycopg3 savepoint≠commit — prevention-log). Connection closed in `finally`.

### Response shape

```jsonc
{
  "requested": 3,
  "resolved": 2,
  "facts_upserted": 410,
  "facts_skipped": 12,
  "symbols_failed": 0,                 // fetch/parse failures among resolved
  "periods_canonical_upserted": 88,
  "results": [
    { "symbol": "IEP", "resolved": true,  "instrument_id": 123, "cik": "0000813762" },
    { "symbol": "ET",  "resolved": true,  "instrument_id": 456, "cik": "0001276187" },
    { "symbol": "ZZZZ","resolved": false, "instrument_id": null, "cik": null }
  ]
}
```

`FactsRefreshSummary` is batch-level (no per-symbol upsert breakdown), so per-symbol counts are not surfaced — only `resolved` + the batch rollup. Documented as a known limitation; per-symbol counts would need a `refresh_financial_facts` signature change, out of scope.

## Tests (pure-logic-first)

- Pure: symbol-resolution stitching — given resolved DB rows + the requested symbols, produce the ordered `results` list with correct `resolved` flags and the `(symbol, id, cik)` triples passed to refresh. Extract a pure `build_refresh_plan(requested, resolved_rows)` helper (uppercases + dedups requested, joins against resolved rows) and table-test it: unknown symbol, no-CIK symbol, duplicate symbol (collapses to one), mixed-case input (`aapl`/`AAPL` → one), all-unknown.
- Validation: empty list → 400, 51 symbols → 400.
- One light integration test only if a new SQL mechanism is introduced (it is not — the resolve query is standard); rely on dev-verify for the live path.

## Definition-of-done (ETL clauses 8-12)

Touches the fundamentals write path (`financial_facts_raw` + `financial_periods`), so:
- Smoke `POST /admin/fundamentals/refresh {"symbols":["AAPL","GME","MSFT","JPM","HD"]}` on dev → record `facts_upserted` + `periods_canonical_upserted`.
- Cross-source: spot-check one instrument's resulting `/instruments/{symbol}/fundamentals` (or financial_periods rollup) figure vs SEC EDGAR companyfacts directly.
- Backfill: the endpoint IS the backfill trigger — exercising it on the panel satisfies the "run, don't queue" clause.
- Operator-visible figure: confirm `/instruments/AAPL/...` fundamentals figure renders post-refresh.
- PR records commit SHA + the figures.

## Out of scope (follow-up ticket)

Part B — `expected_filings` table + `expected_filings_poller` job (event-driven catch-up). Filed as a follow-up; needs schema migration + scheduler lane.
