# sec_form4

**Class.** SEC manifest.
**Form / endpoint.** Form 4 / 4/A — insider transactions (Rule 16a-3, 2-business-day filing window). SEC archive: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{primary_doc}.xml`.

## 1. Origin
Single-attachment ownership XML. Issuer-scoped — `subject_type='issuer'`, `subject_id=<issuer_cik>`, `instrument_id` set. Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`. URL canonicalised by `_canonical_form_4_url` (`app/services/insider_transactions.py`) strips the SEC XSL-rendering prefix. Form-mapping at `app/services/sec_manifest.py:864-865` (`"4"` + `"4/A"` → `"sec_form4"`).

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='issuer'` + `subject_id=<issuer_cik>` + `source='sec_form4'`. Driver column: `last_known_filed_at` — `predict_next_at` adds the per-source cadence (`app/services/data_freshness.py:116-128`). Cadence ceiling **30d** (`data_freshness.py:75`). Atom feed (5-min) + daily-index reconcile drive discovery; per-CIK reconcile poll is the safety-net. Manifest watermark column: `sec_filing_manifest.parser_version` (re-parse trips when `_PARSER_VERSION_FORM4` bumps; current = `"form4-v1"` at `insider_transactions.py:64`).

## 3. Retry posture
- Missing `instrument_id` / `primary_document_url` / `filed_at` → `tombstoned` without fetch (`insider_345.py:124-162`).
- **Pre-3y retention cap** (`insider_345.py:163-173`, PR4 #1233 §4.3) — `form4_within_retention(filed_at.date())` deterministic; tombstone (retry never changes the answer). Pre-fetch placement, no `filing_raw_documents` row written. Recovery on cap widening = `POST /jobs/sec_rebuild/run` (NOT parser-version rewash). `INSIDER_FORM4_RETENTION_YEARS = 3` at `insider_transactions.py:92`.
- Fetch raise → `_failed_outcome` with **1h backoff** (`insider_345.py:89`).
- Empty / non-200 body → `_write_tombstone` + manifest `tombstoned`.
- `parse_form_4_xml` raises or returns `None` → `tombstoned` with `raw_status='stored'`.
- Upsert raise: `is_transient_upsert_error` discriminates transient `OperationalError` (1h retry) vs deterministic constraint violation (tombstone manifest row + tombstone `insider_filings`); see `insider_345.py:284-337` for the PR #1131 branching.

## 4. Bootstrap path
**No dedicated Form 4 stage.** Two seeding stages run pre-discovery:
- Stage 11 `sec_insider_ingest_from_dataset` (`bootstrap_orchestrator.py:1054`, lane `db`) — bulk-loads the SEC insider dataset (decades of Form 4 history) into `insider_transactions` directly, NO HTTP cost. Drops ~427k rows per dev panel.
- Stage 19 `sec_insider_transactions_backfill` (`bootstrap_orchestrator.py:1110`, lane `sec_rate`) — round-robin per-CIK Form 4 backfill for deep-history filers still under the 3y cap.

Steady-state writes after that are 100% manifest-worker driven via the Layer 1/2/3 discovery → `_parse_form4` path. Wall-clock for stage 11: ~10 min cleanly per `project_1233_run2_measurement.md`.

## 5. Steady-state path
**No SCHEDULED_JOBS cron for Form 4 hourly ingest** — retired post-#1155 (`scheduler.py:665-671`). Discovery flows: Atom fast-lane (every 5 min) + daily-index reconcile (daily) → `sec_manifest_worker` → `manifest_parsers/insider_345.py::_parse_form4` (one write-path per accession).

Round-robin deep-tail backfill cron still scheduled: `ScheduledJob(name=JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL, source='sec_rate', cadence=hourly(minute=45))` at `scheduler.py:712-728`. Picks the 25 instruments with the largest pending backlog, clears up to 50 oldest-first per instrument per run. Prerequisite `_bootstrap_complete` (gated until first-install bootstrap finishes).

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_form4'`. `subject_type='issuer'` → `instrument_id` MUST be set; `subject_id = <issuer_cik_zero_padded>`. Option C `filed_at` gate enforced at `record_manifest_entry`. The 3y retention cap is applied parser-side (pre-fetch) NOT discovery-side, so the manifest carries every discovered accession; tombstoning happens at parse-time.

## 7. Parser
`app/services/manifest_parsers/insider_345.py::_parse_form4` (registered at `insider_345.py:780`). Parser version `_PARSER_VERSION_FORM4 = "form4-v1"` (`insider_transactions.py:64`). Registered with `requires_raw_payload=True` (`insider_345.py:780`).

Extraction: `parse_form_4_xml(xml)` → parsed dataclass; `upsert_filing` (`insider_transactions.py`) writes `insider_filings` (document_type `'4'`) + `insider_transactions` rows + fans out across share-class siblings via `siblings_for_issuer_cik` internally (`insider_345.py:48-51`). Observation write-through + per-instrument `refresh_insiders_current` happen inside the same `conn.transaction()` (`insider_345.py:285-292`).

## 8. Observation insert
`ownership_insiders_observations` via `record_insider_observation` (`app/services/ownership_observations.py:110-178`). PK on `(instrument_id, holder_identity_key, ownership_nature, source, source_document_id, period_end)`. ON CONFLICT DO UPDATE — idempotent re-ingest. Form 4 = top source-priority `1` in the MERGE source CASE chain (`ownership_observations.py:235-248`): supersedes Form 3 baseline + every other downstream source. Tombstones via `known_to`.

## 9. Current table refresh
`refresh_insiders_current` (`ownership_observations.py:181-300`). PG17 MERGE writer per #1255. Drift state in `ownership_refresh_state` (category `'insiders'`). Daily drift-repair via `_CATEGORIES[0]` (`app/jobs/ownership_observations_repair.py:81-86`).

## 10. Operator-visible endpoint
- `GET /instruments/{symbol}/insider_summary` (`app/api/instruments.py:1824`) — 90-day open-market + total-activity rollup.
- `GET /instruments/{symbol}/insider_transactions` (`app/api/instruments.py:1933`) — paginated transaction detail.
- `GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121`) — `insiders` slice contribution.

## 11. Verification queries
```sql
-- Latest 20 Form 4 transactions for AAPL.
-- NOTE: §11-author — `form4_retention_cutoff()` SQL function does not exist in dev DB;
-- replaced with explicit CURRENT_DATE - INTERVAL pending spec-author review of intended cutoff.
SELECT t.txn_date, t.filer_name, t.acquired_disposed_code,
       t.shares, t.price, t.accession_number
FROM insider_transactions t
JOIN filing_events fe ON fe.provider_filing_id = t.accession_number
WHERE fe.provider = 'sec'
  AND fe.instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'AAPL')
  AND t.txn_date >= CURRENT_DATE - INTERVAL '3 months'
ORDER BY t.txn_date DESC LIMIT 20;
```
Smoke: `curl localhost:8000/instruments/AAPL/insider_summary | jq '.summary'`. Cross-source: spot-check open-market P/S totals against `marketbeat.com` / `openinsider.com` 3-month windows.

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_manifest_source_has_registered_parser[sec_form4]` + `test_source_has_spec_file[sec_form4]` + `test_source_spec_has_required_sections[sec_form4]`.

## 13. Known gotchas
1. **3-year rolling retention floor** (`insider_345.py:163-173`, PR4 #1233 §4.3). Pre-fetch tombstone — every Form 4 writer chokepoint calls `form4_within_retention`. Cap widening recovers via `POST /jobs/sec_rebuild/run`, NOT parser-version rewash (no `filing_raw_documents` row to re-parse).
2. **2023-01-03 VALUE-cutover does NOT apply to Form 4** — that gotcha is 13F-HR (`sec_13f_hr.md` §13.1). Form 4 transaction shares + `price_per_share` are reported in raw share / dollar units throughout. The 2023-01-03 + PRN drop semantics are 13F-only.
3. **XSL-render strip is shared with Form 3 / Form 5** (`_canonical_form_4_url`). Atom discovery may emit the `/xslF345Xnn/`-prefixed URL.
4. **Share-class fan-out is INSIDE `upsert_filing`** (`insider_345.py:48-51`). The manifest parser must NOT loop siblings itself.
5. **XML mandate since 2003-06-30** (`.claude/skills/data-sources/sec-edgar.md:288`). Pre-2003 = HTML-only (rare; well outside the 3y cap anyway).
6. **Form 4 amendment (`4/A`) is the SAME source** — handled by the same parser path. Amendment chain tracked via `is_amendment_form` / `amends_accession` at the manifest layer.
7. **Manifest tombstone on parse-failure must match `insider_filings` state** — every parse-failure branch writes a tombstone (#1129 review PREVENTION).
