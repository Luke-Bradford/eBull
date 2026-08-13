# sec_form5

**Class.** SEC manifest.
**Form / endpoint.** Form 5 / 5/A — annual statement of changes in beneficial ownership (Rule 16a-3(f), 45 days after fiscal year-end). SEC archive: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{primary_doc}.xml`.

## 1. Origin
Single-attachment ownership XML — same EDGAR ownership XML schema as Form 4 (`<ownershipDocument>` namespace). Issuer-scoped — `subject_type='issuer'`, `subject_id=<issuer_cik>`, `instrument_id` set. Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`. URL canonicalised by `_canonical_form_4_url` (shared XSL `/xslF345Xnn/` prefix — strip back to raw XML). Form-mapping at `app/services/sec_manifest.py:866-867` (`"5"` + `"5/A"` → `"sec_form5"`).

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='issuer'` + `subject_id=<issuer_cik>` + `source='sec_form5'`. Driver column: `last_known_filed_at` — `predict_next_at` adds the per-source cadence (`data_freshness.py:116-128`). Cadence ceiling **365d** (`data_freshness.py:76`) — annual filing within 45 days of fiscal year-end. Atom feed (5-min) catches the individual event; per-CIK reconcile poll is the safety-net. Manifest watermark column: `sec_filing_manifest.parser_version` — re-parse trips when `_PARSER_VERSION_FORM5` bumps; current = `"form5-v1"` (`insider_transactions.py:70`).

## 3. Retry posture
- Missing `instrument_id` / `primary_document_url` / `filed_at` → `tombstoned` without fetch (`insider_345.py:567-602`).
- **18-month retention cap** (`insider_345.py:603-613`, PR10b #1233 §4.4) — `form5_within_retention(filed_at.date())`. Deterministic; tombstone (retry never changes the answer). Pre-fetch placement, no `filing_raw_documents` row written. Recovery on cap widening = `POST /jobs/sec_rebuild/run` (NOT parser-version rewash). `INSIDER_FORM5_RETENTION_MONTHS = 18` at `insider_transactions.py:145`.
- Fetch raise → `_failed_outcome` with **1h backoff** (`insider_345.py:89`).
- Empty / non-200 body → `_write_tombstone(..., document_type='5')` + manifest `tombstoned`.
- `parse_form_5_xml` raises or returns `None` → `tombstoned` with `raw_status='stored'`.
- Upsert raise: `is_transient_upsert_error` discriminates transient `OperationalError` (1h retry) vs deterministic constraint violation (tombstone manifest + `insider_filings` with `document_type='5'`).

## 4. Bootstrap path
**No dedicated Form 5 stage.** Form 5 piggybacks on stage 11 `sec_insider_ingest_from_dataset` (`bootstrap_orchestrator.py:1054`, lane `db`) which bulk-loads the SEC insider dataset (includes Forms 3/4/5). After that, the stage 19 `sec_insider_transactions_backfill` (`bootstrap_orchestrator.py:1110`, lane `sec_rate`) round-robin sweep + Layer 1/2/3 discovery handles Form 5 the same way it handles Form 4.

## 5. Steady-state path
**No SCHEDULED_JOBS cron specific to Form 5.** Discovery flows: Atom fast-lane + daily-index reconcile → `sec_manifest_worker` → `manifest_parsers/insider_345.py::_parse_form5`. The hourly `JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL` round-robin cron at `scheduler.py:712-728` walks Form 5 filings alongside Form 4 (deep-history tail).

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_form5'`. `subject_type='issuer'` → `instrument_id` MUST be set; `subject_id = <issuer_cik_zero_padded>`. Option C `filed_at` gate enforced at `record_manifest_entry`. The 18-month retention cap is applied parser-side (pre-fetch).

## 7. Parser
`app/services/manifest_parsers/insider_345.py::_parse_form5` (registered at `insider_345.py:782`). Parser version `_PARSER_VERSION_FORM5 = "form5-v1"` (`insider_transactions.py:70`). Registered with `requires_raw_payload=True` (`insider_345.py:782`).

Extraction: `parse_form_5_xml(xml)` → parsed dataclass (returns `None` for the optional year-end holdings reconciliation section — those filings tombstone via standard adapter path, see `insider_345.py:543-558` module docstring). `upsert_filing` (`insider_transactions.py`) writes `insider_filings` with `document_type='5'` + `insider_transactions` rows + fans out across share-class siblings.

**Observation write-through emits `source='form4'`** since the `OwnershipSource` enum on the observations CHECK constraint does NOT include `'form5'` (`insider_345.py:14-22`). Operator-visible provenance is preserved on `insider_filings.document_type='5'` via the JOIN. Raw payload stored under `document_kind='form5_xml'` (`insider_345.py:659`) — distinct from `form4_xml` so a future Form-5-only parser bump rewashes only Form 5 rows.

## 8. Observation insert
`ownership_insiders_observations` via `record_insider_observation` (`ownership_observations.py:110-178`). Source field carries `'form4'` per the enum-constraint workaround (see §7); MERGE source-priority = `1` (top, same as Form 4). PK + ON CONFLICT identical to §form4.

## 9. Current table refresh
`refresh_insiders_current` (`ownership_observations.py:181-300`). PG17 MERGE writer per #1255. Drift state in `ownership_refresh_state` (category `'insiders'`). Daily drift-repair via `_CATEGORIES[0]` (`app/jobs/ownership_observations_repair.py:81-86`). Read-side de-dupe via cumulative Form 4 rollup means Form 5 transactions are absorbed into the same per-(holder, nature) MERGE row as Form 4.

## 10. Operator-visible endpoint
Form 5 contributes to:
- `GET /instruments/{symbol}/insider_summary` (`app/api/instruments.py:1824`) — Form 5 transactions are counted alongside Form 4 in the 90-day rollup (both are non-derivative trades classified by `acquired_disposed_code`).
- `GET /instruments/{symbol}/insider_transactions` (`app/api/instruments.py:1933`) — paginated detail; Form 5 rows distinguishable by JOIN to `insider_filings.document_type = '5'`.
- `GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121`) — `insiders` slice contribution.

## 11. Verification queries
```sql
-- Recent Form 5 transactions for AAPL (within 18mo cap).
SELECT t.txn_date, t.filer_name, t.shares,
       t.acquired_disposed_code, t.accession_number, f.document_type
FROM insider_transactions t
JOIN insider_filings f ON f.accession_number = t.accession_number
JOIN filing_events fe ON fe.provider_filing_id = t.accession_number
WHERE fe.provider = 'sec'
  AND fe.instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'AAPL')
  AND f.document_type LIKE '5%'
  AND f.is_tombstone = FALSE
ORDER BY t.txn_date DESC LIMIT 20;
```
Smoke: `curl localhost:8000/instruments/AAPL/insider_transactions?limit=50 | jq '[.transactions[] | select(.document_type | startswith("5"))]'`. Cross-source: a Form 5 filed in Q1 should appear on `marketbeat.com` insider activity for that issuer.

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_manifest_source_has_registered_parser[sec_form5]` + `test_source_has_spec_file[sec_form5]` + `test_source_spec_has_required_sections[sec_form5]`.

## 13. Known gotchas
1. **18-month retention cap, not 12** (`insider_transactions.py:126-145` rationale). A 1y rolling cap would drop the latest Form 5 immediately on year-rollover, leaving weeks of zero-visibility per insider until the next annual lands. 18 months gives the annual-cadence buffer (Codex 1a MED on PR10b plan). Worst case: BOTH prior-year + current-year Form 5 admitted briefly — operator-visible impact zero (read-side de-dupes via cumulative rollup).
2. **Observation source enum says `'form4'`, not `'form5'`** (`insider_345.py:14-22`). The OwnershipSource CHECK constraint does not list `'form5'`; operator-visible provenance lives on `insider_filings.document_type='5'`. Don't search for `source='form5'` in `ownership_insiders_observations` — it won't exist.
3. **Cap widening recovery = source-reset, NOT rewash** (`insider_345.py:587-592`). Pre-fetch tombstone means no `filing_raw_documents` row for the parser to re-process; only `POST /jobs/sec_rebuild/run` re-enqueues.
4. **Holdings-only filings return `None`** from `parse_form_5_xml` (`insider_345.py:714`) — the optional year-end reconciliation section is out of scope. These tombstone via standard path.
5. **Tombstones carry `document_type='5'`** (`insider_345.py:640, 685, 702, 747`) so operator audits filtering by document_type see Form 5 failures distinctly from Form 4.
6. **Shared XSL prefix with Form 3 / Form 4** (`_canonical_form_4_url` matches `/xslF345Xnn/`).
7. **XML mandate since 2003-06-30** (`.claude/skills/data-sources/sec-edgar.md:288`). Well outside the 18-month cap.
