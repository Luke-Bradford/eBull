# company_tickers

**Class.** SEC bulk reference (NOT `ManifestSource` — bulk reference; section 6 = N/A).
**Form / endpoint.** SEC operating-company ticker bridge — `https://www.sec.gov/files/company_tickers.json`.

## 1. Origin

Bulk JSON. URL constant `_TICKERS_URL` at `app/providers/implementations/sec_edgar.py:53`. Conditional-GET aware provider method `SecFilingsProvider.build_cik_mapping_conditional` at `app/providers/implementations/sec_edgar.py:593` (sends `If-Modified-Since: <prior Last-Modified>`; returns `None` on 304). Plain `build_cik_mapping` non-conditional helper at `app/providers/implementations/sec_edgar.py:580` (audit / one-shot use only — prefer conditional for scheduled fetches). Payload shape: `{"0": {"cik_str": int, "ticker": str, "title": str}, "1": {...}, ...}` covering ~10k operating-company rows. Parsed into `{TICKER: zero-padded-CIK}` per `app/providers/implementations/sec_edgar.py:808`. Service body: `daily_cik_refresh` at `app/workers/scheduler.py:1869-2063`.

## 2. Watermarking model

Two-layer watermark, both stored under `external_data_watermarks`:

1. **HTTP `Last-Modified`** sent as `If-Modified-Since` on next fetch. On 304 → equity upsert skipped entirely (`app/workers/scheduler.py:1936-1939`).
2. **`response_hash` (sha256 of body)** — defensive fallback when SEC serves 200 with identical bytes. Same-hash → equity upsert skipped, `Last-Modified` watermark advanced (`app/workers/scheduler.py:1940-1955`).

Watermark `source='sec.tickers'`, `key='global'` (`app/workers/scheduler.py:1883-1884`). Destination-empty override (#1056): if `external_identifiers (sec/cik)` was wiped, the conditional header is suppressed and a full unconditional fetch + upsert forced regardless of watermark / body hash (`app/workers/scheduler.py:1910-1969`).

## 3. Retry posture

`304 Not Modified` is the dominant branch (most days, equity-side is a zero-byte no-op). `5xx` raises per `SecFilingsProvider` retry budget. The G8 restructure (`app/workers/scheduler.py:1894-1897`) makes the Stage 6 (MF) + Stage 7 (exchange) sibling enrichments fire UNCONDITIONALLY — failure of one sibling logs-but-does-not-raise (`app/workers/scheduler.py:2041-2042, 2054-2055`) so equity-side never blocks downstream directory refreshes.

## 4. Bootstrap path

**Stage 6 in `_BOOTSTRAP_STAGE_SPECS`.** `_spec("cik_refresh", 6, "sec_rate", JOB_DAILY_CIK_REFRESH)` at `app/services/bootstrap_orchestrator.py:1044`. Cap requirement `CapRequirement(all_of=("universe_seeded",))` at `app/services/bootstrap_orchestrator.py:533`. Provides cap `cik_mapping_ready` at `app/services/bootstrap_orchestrator.py:361`. Lane `sec_rate` (SEC 10 req/s shared pool). Expected wall-clock <1 min (single 1-2 MB JSON fetch + ~10k row UPSERT). Sibling enrichments Stage 6 (MF) + Stage 7 (exchange) fire INSIDE the same `daily_cik_refresh` function body — they are NOT separate bootstrap stages, they ride the same dispatch. (Separately, `mf_directory_sync` IS a dedicated bootstrap stage at S26 — `_spec("mf_directory_sync", 26, "sec_rate", JOB_MF_DIRECTORY_SYNC)` at `app/services/bootstrap_orchestrator.py:1181` — it covers the trust-CIK walk + N-CSR drain, distinct from the bundled refresh during stage 6.)

## 5. Steady-state path

`daily_cik_refresh` runs daily via the sync_orchestrator DAG (NOT `SCHEDULED_JOBS`). Layer mapping at `app/services/sync_orchestrator/registry.py:232-235` confirms `daily_cik_refresh` is registered against the orchestrator. Function entrypoint `app/workers/scheduler.py:1869`. Lane: `sec_rate` (orchestrator dispatches via `_INVOKERS`). Idempotent — safe to re-run.

## 6. Manifest insert

**N/A.** Bulk reference source. No `sec_filing_manifest` row written. `company_tickers` is not listed in the `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`. The destination tables are `external_identifiers (provider='sec', identifier_type='cik')` (write-through for matched US-listed instruments per #475 scope at `app/workers/scheduler.py:1973-1978`) and indirectly the per-CIK fetch pool for every downstream SEC ingester.

## 7. Parser

In-line parser at `app/providers/implementations/sec_edgar.py:808` — `parse_company_tickers_json` (or equivalent helper used by `build_cik_mapping_conditional`). Drops rows where `cik_str` is missing or `ticker` is empty. Zero-pads CIK to 10-digit TEXT per data-engineer I10 invariant. NOT a `manifest_parsers/` entry (bulk reference; no manifest dispatch).

## 8. Observation insert

`external_identifiers` UPSERT. PK `(provider, identifier_type, instrument_id)` (or similar — see schema). `provider='sec'`, `identifier_type='cik'`, `identifier_value=<zero-padded CIK>`. Tombstone / `known_to` semantics inherit from the `external_identifiers` table contract. Scoped to US-listed exchanges only per #475 to avoid stamping unrelated US-company CIKs onto eToro crypto coins sharing tickers (e.g. `BTC` crypto vs Grayscale Bitcoin Mini Trust).

## 9. Current table refresh

**N/A.** `external_identifiers` IS the current table — no separate `_current` snapshot. No MERGE writer per #1255. Per-row UPSERT inside `daily_cik_refresh`.

## 10. Operator-visible endpoint

No dedicated endpoint. Coverage surfaces indirectly via:
- `GET /admin/data-freshness` (panel that reads `data_freshness_index`).
- `GET /instruments/<symbol>` (the `external_identifiers (sec/cik)` row is what links a symbol to every downstream SEC ingester).
- `GET /system/bootstrap-status` (S6 `cik_refresh` row state).

## 11. Verification queries

```sql
-- AAPL must resolve to CIK 0000320193.
SELECT identifier_value
  FROM external_identifiers
 WHERE provider = 'sec' AND identifier_type = 'cik'
   AND instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'AAPL');

-- Watermark health — should advance on every non-304 day.
SELECT source, key, watermark, response_hash, fetched_at
  FROM external_data_watermarks
 WHERE source = 'sec.tickers';

-- Total mapped instruments.
SELECT COUNT(*) FROM external_identifiers
 WHERE provider = 'sec' AND identifier_type = 'cik';
```

Cross-source confirm: `curl -s 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193' | head` should return AAPL filings (round-trips the bridge).

## 12. Smoke test

Import-time gate — `tests/smoke/test_etl_source_to_sink.py`: `test_source_has_spec_file[company_tickers]` + `test_source_spec_has_required_sections[company_tickers]`. (A BULK_REFERENCE source — NOT a `ManifestSource`, so the manifest-source parametrized cases do not apply to it.)

Not covered by the import-time gate (verified by the live-smoke runbooks under `app/runbooks/`, not pytest): provider importable, the `daily_cik_refresh` DAG registration, the AAPL `external_identifiers (sec, cik)` row, the `sec.tickers` / `global` watermark row, and bootstrap stage S6 in `_BOOTSTRAP_STAGE_SPECS`.

## 13. Known gotchas

1. **Bulk reference — NOT a `ManifestSource`.** Section 6 = N/A by design. The lint at `data-engineer/etl-source-to-sink-template.md` exempts bulk-reference sources from the `ManifestSource` parity check.
2. **Destination-empty override (#1056).** If the operator wipes `external_identifiers`, the 304/hash-skip branch would silently no-op forever. `_cik_destination_is_empty` check at `app/workers/scheduler.py:1910` forces a full unconditional fetch regardless. There is an explicit invariant assertion at `app/workers/scheduler.py:1924-1935` that RAISES if the provider returns 304 against an empty destination (the conditional header should have been suppressed; a future provider refactor that silently sends `If-Modified-Since` must not leave dest empty forever).
3. **G8 sibling enrichments fire unconditionally.** Prior to G8, an early `return` on 304 silently skipped Stage 6 (MF) and would have skipped Stage 7 (exchange) on every warm-watermark day. Post-G8 (`app/workers/scheduler.py:1894-1897`), `skip_equity_upsert` is a flag; sibling enrichments ALWAYS run regardless of which equity branch was taken.
4. **US-exchange scope filter (#475).** SEC `company_tickers.json` covers only US-registered companies. The mapper used to match every tradable instrument by symbol, stamping unrelated US-company CIKs onto eToro crypto coins sharing tickers. Scope is now restricted to US-listed exchanges only.
5. **Empty-string watermark is NOT a valid `If-Modified-Since`.** Explicit truthy check at `app/workers/scheduler.py:1918`. An empty-string watermark from a prior run where `Last-Modified` was absent must NOT be sent as `If-Modified-Since: ` (invalid HTTP date).
