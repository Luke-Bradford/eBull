# company_tickers_mf

**Class.** SEC bulk reference (NOT `ManifestSource` — bulk reference; section 6 = N/A).
**Form / endpoint.** SEC mutual-fund / ETF classId ↔ instrument bridge — `https://www.sec.gov/files/company_tickers_mf.json`.

## 1. Origin

Bulk JSON. URL constant `_MF_DIRECTORY_URL` at `app/services/mf_directory.py:44`. Fetcher `_fetch_directory` at `app/services/mf_directory.py:47-52` uses the shared `SecFilingsProvider.fetch_document_text` (same `sec_rate` pool as every other SEC fetch). Payload shape per `app/services/mf_directory.py:15-20`:

```json
{
  "fields": ["cik", "seriesId", "classId", "symbol"],
  "data": [[36405, "S000002839", "C000010048", "VFINX"], ...]
}
```

~28k mutual-fund / ETF rows. CIKs arrive as integers; zero-padded to 10-digit TEXT per data-engineer I10 convention (`app/services/mf_directory.py:22-23`). Service: `refresh_mf_directory` at `app/services/mf_directory.py:55`.

## 2. Watermarking model

**No conditional-GET in v1.** Documented at `app/services/mf_directory.py:25-27`. The ~1 MB daily fetch cost is acceptable; ETag / Last-Modified plumbing is deferred follow-up. Watermark surface: only `data_freshness_index` row(s) for the destination tables. No `external_data_watermarks` row maintained by this module.

## 3. Retry posture

`RuntimeError` raised on empty body (`app/services/mf_directory.py:50-51`). HTTP retries inherit the SEC provider's `ResilientClient` backoff. The bundled call from `daily_cik_refresh` runs inside a try/except that logs-but-does-not-raise on failure (`app/workers/scheduler.py:2033-2042`) so MF directory refresh failure NEVER blocks equity-side CIK refresh.

## 4. Bootstrap path

**Two-shot bootstrap.**

1. **Stage 6 sibling enrichment** — bundled inside `daily_cik_refresh` (`app/workers/scheduler.py:2028-2042`). Provides cap incidentally; not the primary signal.
2. **Stage 26 dedicated `mf_directory_sync`** — `_spec("mf_directory_sync", 26, "sec_rate", JOB_MF_DIRECTORY_SYNC)` at `app/services/bootstrap_orchestrator.py:1181`. Added in #1174 to give the N-CSR fund-metadata parser (S27) a hard cap dependency without coupling to S6's bundled refresh. Cap `CapRequirement(all_of=("universe_seeded",))` at `app/services/bootstrap_orchestrator.py:579`. Provides cap `class_id_mapping_ready` at `app/services/bootstrap_orchestrator.py:399`. Lane `sec_rate`. Required by S27 `sec_n_csr_bootstrap_drain` via `class_id_mapping_ready` per `app/services/bootstrap_orchestrator.py:299`.

Expected wall-clock <1 min (single ~1 MB JSON fetch + ~28k row UPSERT into `cik_refresh_mf_directory` + write-through filter to `external_identifiers`).

## 5. Steady-state path

Bundled into `daily_cik_refresh` as Stage 6 sibling enrichment (`app/workers/scheduler.py:2028-2042`) — fires on every daily CIK refresh cron run regardless of whether the equity branch upserted (G8 restructure made sibling enrichments fire unconditionally). Lane `sec_rate`.

## 6. Manifest insert

**N/A.** Bulk reference source. No `sec_filing_manifest` row written. `company_tickers_mf` is not listed in the `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`.

## 7. Parser

In-line parser inside `refresh_mf_directory` at `app/services/mf_directory.py:55`. Walks `payload["data"]` rows by `payload["fields"]` indices. Drops malformed rows defensively (empty fields → early return at `app/services/mf_directory.py:78-79`). NOT a `manifest_parsers/` entry.

## 8. Observation insert

Two destination tables:

1. **`cik_refresh_mf_directory`** — snapshot keyed by `classId` (per `app/services/mf_directory.py:5`). Schema at `sql/149_fund_metadata.sql:278`. All ~28k rows land here.
2. **`external_identifiers`** — write-through for symbols matching an existing instrument. `provider='sec'`, `identifier_type='class_id'` (per `app/services/mf_directory.py:6-7`). Subset only — rows whose `symbol` does NOT match an instrument are NOT written through (they live in `cik_refresh_mf_directory` only).

Returns counts dict `{fetched, directory_rows, external_identifier_rows}` per `app/services/mf_directory.py:63`.

## 9. Current table refresh

**N/A.** `cik_refresh_mf_directory` IS the current table — snapshot semantics, not append-only history. Rows SEC drops from the payload remain in the table with an older `last_seen` (mirrors `cik_refresh_exchange_directory` precedent at G8 / `app/services/exchange_directory.py:31-34`). No DELETE / mark-stale in v1. No MERGE writer.

## 10. Operator-visible endpoint

No dedicated endpoint. Coverage surfaces indirectly via:
- `GET /system/bootstrap-status` (S26 `mf_directory_sync` row state).
- Fund-metadata fields on `GET /instruments/<symbol>` for fund instruments (populated by the N-CSR parser at S27 which depends on this map).

## 11. Verification queries

```sql
-- Vanguard 500 Index Fund (VFINX) classId resolution.
SELECT class_id, series_id, trust_cik, symbol, last_seen
  FROM cik_refresh_mf_directory
 WHERE symbol = 'VFINX';

-- Write-through coverage for in-universe symbols.
SELECT COUNT(*) FROM external_identifiers
 WHERE provider = 'sec' AND identifier_type = 'class_id';

-- Directory size sanity (~28k expected).
SELECT COUNT(*) FROM cik_refresh_mf_directory;
```

Cross-source confirm: `curl -s 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=<series_cik>&type=N-CSR'` should return N-CSR filings for the series.

## 12. Smoke test

Import-time gate — `tests/smoke/test_etl_source_to_sink.py`: `test_source_has_spec_file[company_tickers_mf]` + `test_source_spec_has_required_sections[company_tickers_mf]`. (A BULK_REFERENCE source — NOT a `ManifestSource`, so the manifest-source parametrized cases do not apply to it.)

Not covered by the import-time gate (verified by the live-smoke runbooks under `app/runbooks/`, not pytest): `refresh_mf_directory` importable, bootstrap stage S26 `mf_directory_sync`, the `class_id_mapping_ready` cap, and the `cik_refresh_mf_directory` + `external_identifiers (sec, class_id)` write-through.

## 13. Known gotchas

1. **Bulk reference — NOT a `ManifestSource`.** Section 6 = N/A by design. Lint exempts bulk-reference sources.
2. **Two bootstrap entrypoints.** S6 bundled refresh (via `daily_cik_refresh` sibling enrichment) AND S26 dedicated `mf_directory_sync`. Both write to the same destination tables; both are idempotent. S26 exists to give S27 N-CSR drain a hard cap dependency without coupling to the equity-side refresh.
3. **No conditional-GET in v1.** Every daily run fetches the full ~1 MB JSON. Acceptable today; revisit if SEC adds bandwidth pressure.
4. **Snapshot semantics, not append-only.** Stale rows persist with older `last_seen`. Consumers needing a freshness gate filter on `last_seen >= cutoff`. No history retention.
5. **Write-through is conditional.** A classId row whose `symbol` does NOT match an existing instrument is stored in `cik_refresh_mf_directory` but is NOT written through to `external_identifiers`. This is by design — the universe controls who gets an external identifier.
6. **Fail-soft from `daily_cik_refresh`.** Failure of `refresh_mf_directory` logs-but-does-not-raise (`app/workers/scheduler.py:2041-2042`). A directory-refresh error MUST NOT block the equity-side CIK refresh.
7. **CIKs arrive as integers, stored as zero-padded TEXT.** Without the zero-pad, identity-resolution joins against the canonical 10-digit TEXT shape silently miss.
