# sec_form3

**Class.** SEC manifest.
**Form / endpoint.** Form 3 / 3/A — insider initial statement of beneficial ownership (Rule 16a-3). SEC archive: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{primary_doc}.xml`.

## 1. Origin
Single-attachment ownership XML. Issuer-scoped — `subject_type='issuer'`, `subject_id=<issuer_cik>`, `instrument_id` set. Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`. URL canonicalised by `_canonical_form_4_url` (`app/services/insider_transactions.py`) so XSL-rendered URLs strip back to raw XML. Form-mapping at `app/services/sec_manifest.py:862-863` (`"3"` + `"3/A"` → `"sec_form3"`).

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='issuer'` + `subject_id=<issuer_cik>` + `source='sec_form3'`. Driver column: `last_known_filed_at` — `predict_next_at(source, last_known_filed_at)` = `last_known_filed_at + cadence(source)` (`app/services/data_freshness.py:116-128`). Cadence ceiling **30d** (`data_freshness.py:74`). Atom feed (5-min) + daily-index reconcile are the primary discovery surfaces; the per-CIK reconcile poll is the safety-net. Manifest watermark column: `sec_filing_manifest.parser_version` — re-parse trips when `_FORM3_PARSER_VERSION` (`app/services/insider_form3_ingest.py:65`) bumps.

## 3. Retry posture
- Missing `instrument_id` or `primary_document_url` → `tombstoned` without fetch (`insider_345.py:365-384`).
- Fetch raise → `_failed_outcome` with **1h backoff** (`_FAILED_RETRY_DELAY` at `insider_345.py:89`).
- Empty / non-200 body → `_write_form_3_tombstone` + manifest `tombstoned`.
- `parse_form_3_xml` raises or returns `None` → `tombstoned` with `raw_status='stored'`.
- Upsert raise: `is_transient_upsert_error` (`app/services/manifest_parsers/_classify.py`) discriminates transient `psycopg.OperationalError` (1h retry) vs deterministic constraint violation (tombstone manifest row + tombstone `insider_filings`).
- No 3-year retention cap (unlike Form 4 / Form 5 — Form 3 is an initial statement; cap-floor would erase the baseline).

## 4. Bootstrap path
Stage 20 `sec_form3_ingest` (`app/services/bootstrap_orchestrator.py:1111`). Lane `sec_rate` (10 req/s shared). Dispatches `JOB_SEC_FORM3_INGEST = "sec_form3_ingest"` (`app/workers/scheduler.py:284`) via `_INVOKERS` — the SCHEDULED_JOBS entry has been retired post-#1155 (`scheduler.py:730-736`); function body + `_INVOKERS` registration kept for bootstrap + Admin "Run now". Expected wall-clock: small (the bulk-insider C-lane stage 11 has already populated `insider_initial_holdings` via `sec_insider_ingest_from_dataset` — this stage is a per-CIK top-up of new-baseline filings).

## 5. Steady-state path
**No SCHEDULED_JOBS cron.** Discovery flows via Layer 1/2/3 (Atom fast-lane + daily-index reconcile + per-CIK reconcile) → `sec_manifest_worker` → `manifest_parsers/insider_345.py::_parse_form3`. See `scheduler.py:730-736` for the retirement rationale (`sec_form3_ingest` retired post-#1155). `sec_form3_sweep` adapter remains on the Admin sweep UI for operator-triggered backfill.

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_form3'`. `subject_type='issuer'` (CHECK at `sec_manifest.py:223-227`) → `instrument_id` MUST be set; `subject_id = <issuer_cik_zero_padded>`. Option C `filed_at` gate enforced at `record_manifest_entry` (`sec_manifest.py:194-217`).

## 7. Parser
`app/services/manifest_parsers/insider_345.py::_parse_form3` (registered at `insider_345.py:781`). Parser version `f"form3-v{_FORM3_PARSER_VERSION}"` = `"form3-v1"` (`insider_345.py:92`, `_FORM3_PARSER_VERSION = 1` at `insider_form3_ingest.py:65`). Registered with `requires_raw_payload=True` (`insider_345.py:781`) — `store_raw` runs in its own savepoint BEFORE parse so the #938 invariant holds on parse-failure.

Extraction: `parse_form_3_xml(xml)` (`app/services/insider_transactions.py`) → parsed dataclass; `upsert_form_3_filing` writes `insider_filings` (document_type `'3'`) + `insider_initial_holdings` + fans out across share-class siblings via `siblings_for_issuer_cik` internally (`insider_345.py:48-51`). Observation write-through + per-instrument `refresh_insiders_current` happen inside the same savepoint.

## 8. Observation insert
`ownership_insiders_observations` via `record_insider_observation` (`app/services/ownership_observations.py:110-178`). PK on `(instrument_id, holder_identity_key, ownership_nature, source, source_document_id, period_end)`. ON CONFLICT DO UPDATE refreshes in place (idempotent re-ingest). Tombstones via `known_to` column (soft-delete; never hard-DELETE).

Source priority for Form 3 = `2` in the MERGE source CASE chain at `ownership_observations.py:235-248` (Form 4 beats Form 3 — current-position tracking).

## 9. Current table refresh
`refresh_insiders_current` (`app/services/ownership_observations.py:181-300`). PG17 MERGE writer per #1255 (`project_1233_pr12_ownership_merge_writer.md`). Watermark captured pre-MERGE; drift state in `ownership_refresh_state` (category `'insiders'`). Daily drift-repair via `_CATEGORIES[0]` at `app/jobs/ownership_observations_repair.py:81-86` — guarantees reconciliation within 24h regardless of writer path.

## 10. Operator-visible endpoint
`GET /instruments/{symbol}/insider_baseline` (`app/api/instruments.py:2053-2156`) + `/insider_baseline/drill` (`app/api/instruments.py:2157-2305`) + `/insider_baseline/export.csv` (`app/api/instruments.py:2306+`). Backing service: `app/services/insider_form3_ingest.py::list_baseline_only_insider_holdings`. Form 3 also contributes to `GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121`) `insiders` slice.

## 11. Verification queries
```sql
-- Form 3 baseline holdings for AAPL.
SELECT h.accession_number, h.holder_name, h.shares, f.filing_date
FROM insider_initial_holdings h
JOIN insider_filings f ON f.accession_number = h.accession_number
JOIN filing_events fe ON fe.provider_filing_id = h.accession_number
WHERE f.document_type LIKE '3%'
  AND f.is_tombstone = FALSE
  AND fe.provider = 'sec'
  AND fe.instrument_id = (SELECT id FROM instruments WHERE symbol = 'AAPL')
ORDER BY f.filing_date DESC LIMIT 20;
```
Smoke: `curl localhost:8000/instruments/AAPL/insider_baseline | jq '.holdings[:5]'`. Cross-source: spot-check a recent Form 3 against the SEC EDGAR per-CIK Atom feed for that issuer.

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_manifest_source_has_registered_parser[sec_form3]` + `test_source_has_spec_file[sec_form3]` + `test_source_spec_has_required_sections[sec_form3]`. Asserts parser is registered (`registered_parser_sources()` includes `sec_form3`) + this spec file exists with all 13 sections.

## 13. Known gotchas
1. **Shared XSL-render strip** (`insider_345.py:386`, `_canonical_form_4_url`). Forms 3/4/5 all share the EDGAR `/xslF345Xnn/` rendering wrapper. Atom discovery may emit the rendered URL; the parser MUST normalise to raw XML before fetch.
2. **Share-class fan-out is INSIDE `upsert_form_3_filing`** (`insider_345.py:48-51`). The manifest parser must NOT loop siblings itself — would double-insert.
3. **No retention cap** (unlike Form 4 §4.3 / Form 5 §4.4). Form 3 is the initial-statement baseline; capping would silently erase the floor under the cumulative cohort view.
4. **XML mandate since 2003-06-30** (`.claude/skills/data-sources/sec-edgar.md:288`) — pre-2003 filings are HTML-only and not parseable. In scope this is rare (decade-old officers); tombstone path covers it.
5. **`document_type` filter is `LIKE '3%'`** in operator queries (`app/api/instruments.py:2221, 2237, 2254`). Matches `'3'` + `'3/A'` (and any future `'3X'` variant) without joining a separate amendment table.
