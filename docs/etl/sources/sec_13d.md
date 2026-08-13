# sec_13d

**Class.** SEC manifest.
**Form / endpoint.** Schedule 13D / SC 13D/A — beneficial owner ≥5% (active intent, Rule 13d-1). SEC archive: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/primary_doc.xml`.

## 1. Origin
Single-attachment XML (`primary_doc.xml`). **Filer-scoped** — `subject_type='blockholder_filer'`, `subject_id=<filer_cik>`, `instrument_id=NULL` (issuer linkage resolved at parse-time via CUSIP → `external_identifiers`). Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`. Canonical URL built from `cik + accession_number` via `_archive_file_url(filer_cik, accession, "primary_doc.xml")` (`app/services/blockholders.py`) — NOT `row.primary_document_url` (manifest URL may be the filing-index page or sibling attachment; only the archive URL guarantees XML — see `sec_13dg.py:36-42, 178-179`).

Form-mapping at `app/services/sec_manifest.py:872-877` — both `"SC 13D"` / `"SC 13D/A"` (legacy filer form) AND `"SCHEDULE 13D"` / `"SCHEDULE 13D/A"` (post-2024-12-18 BOM-normalised form) map to `"sec_13d"`. Both required because SEC submissions JSON uses both conventions (verified against Carl Icahn CIK 0000921669 — `blockholders.py:155-175`).

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='blockholder_filer'` + `subject_id=<filer_cik>` + `source='sec_13d'`. Driver column: `last_known_filed_at` (`data_freshness.py:116-128`). Cadence ceiling **90d** (`data_freshness.py:80`) — event-driven (10 days after threshold cross per Rule 13d-1); most amendments within a quarter; Atom feed is the primary discovery path. Manifest watermark column: `sec_filing_manifest.parser_version` — re-parse trips when `_PARSER_VERSION_13DG` bumps; current = `"13dg-primary-v1"` (`blockholders.py:69`).

## 3. Retry posture
- Empty / missing `row.cik` → `tombstoned` without fetch (`sec_13dg.py:107-116`).
- `row.cik` resolves to `KNOWN_FILING_AGENT_CIKS` → `tombstoned` immediately (`sec_13dg.py:117-149`). Defense-in-depth: archive walks under agent CIKs would 404; tombstoning surfaces the upstream discovery bug.
- **Retention floor at `max(today − 3y, 2024-12-18)`** (`sec_13dg.py:151-172`, PR11 chokepoint B). Pre-cap rows tombstone WITHOUT fetch — saves rate budget AND prevents operator-triggered `POST /jobs/sec_rebuild/run` from re-introducing un-parseable pre-mandate rows. `blockholders_within_retention(filed_at)` at `blockholders.py:120-131`.
- Fetch raise → `_failed_outcome` with **1h backoff** (`_FAILED_RETRY_DELAY` at `sec_13dg.py:81`).
- Empty / non-200 body → `_record_ingest_attempt(status='failed')` + manifest `tombstoned` (mirrors legacy semantics so the row doesn't spin retry; see `sec_13dg.py:193-217`).
- Parse raise (ET.ParseError / ValueError / unexpected) → `_record_ingest_attempt(status='failed')` + `_failed_outcome(raw_status='stored')`. Unexpected exceptions tagged distinctly so operators can distinguish schema-fail vs parser-crash (`sec_13dg.py:267-294`).
- Upsert raise: `is_transient_upsert_error` discriminates transient `OperationalError` (1h retry) vs deterministic constraint violation (manifest `tombstoned` + log `failed`).

## 4. Bootstrap path
**No dedicated 13D stage.** Stage 16 `sec_first_install_drain` (`bootstrap_orchestrator.py:1102-1107`, lane `sec_rate`) walks the manifest universe — once Layer 1/2/3 discovery has enqueued 13D rows (typically via daily-index reconcile + per-CIK submissions walk at stage 14 `sec_submissions_files_walk`), the manifest worker drains them through `_parse_13dg`. Dev DB carries ~575k discovered SC 13D/G manifest rows (`sec_13dg.py:331-333`).

## 5. Steady-state path
**No SCHEDULED_JOBS cron for 13D.** Discovery flows via Atom fast-lane (5 min) + daily-index reconcile (daily) + per-CIK submissions walk (at cadence 90d) → `sec_manifest_worker` → `manifest_parsers/sec_13dg.py::_parse_13dg`. The hourly `JOB_SEC_MANIFEST_WORKER` cron processes pending rows (rate-limited at 10 req/s shared with all SEC sources on the `sec_rate` lane).

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_13d'`. `subject_type='blockholder_filer'`, `subject_id = <filer_cik_zero_padded>`, `instrument_id = NULL` (per CHECK constraint at `sec_manifest.py:194-217` — non-issuer subject_types MUST have `instrument_id=None`). Option C `filed_at` gate enforced at `record_manifest_entry`. The 3y / mandate-floor retention cap is parser-side (pre-fetch) so the manifest carries every discovered accession.

## 7. Parser
`app/services/manifest_parsers/sec_13dg.py::_parse_13dg` (registered against BOTH `sec_13d` AND `sec_13g` at `sec_13dg.py:442-443` — one callable handles both schemas; downstream `submission_type` field disambiguates). Parser version `_PARSER_VERSION_13DG = "13dg-primary-v1"` (`blockholders.py:69`). Registered with `requires_raw_payload=True` (`sec_13dg.py:442-443`).

Extraction (PR11 #1233 §I10 — edgartools-backed): `Schedule13D.parse_xml(primary_xml)` from `edgar.beneficial_ownership.schedule13` (`sec_13dg.py:258-261`) → dict of frozen dataclasses → adapter `build_filing_from_edgartools_dict` (`app/services/manifest_parsers/_schedule13_adapter.py`) → `BlockholderFiling`. In-house `parse_primary_doc` retained at `app/providers/implementations/sec_13dg.py` for `rewash_filings._apply_blockholders` legacy consumer until migration.

Persistence (`sec_13dg.py:318-385`):
1. `_resolve_cusip_to_instrument_id(conn, filing.issuer_cusip)` (CUSIP-only resolution post-PR11 v8 empirical pivot 2026-05-21 — the `sec_13dg_discovery_issuer_hint` table + 5-case branch retired in sql/162 because `efts.sec.gov` doesn't index 13D/G by subject CIK).
2. `_upsert_filer(conn, cik, name)` → `blockholder_filers`.
3. Per `filing.reporting_persons`: `_upsert_filing_row` → `blockholder_filings` (with `instrument_id` NULL if CUSIP unresolved — audit trail preserved, rollup gated by #740).
4. If `instrument_id` resolved: `_record_13dg_observation_for_filing` + `refresh_blockholders_current(conn, instrument_id=instrument_id)`.
5. `_record_ingest_attempt(status='success' if resolved else 'partial')` → `blockholder_filings_ingest_log`.

## 8. Observation insert
`ownership_blockholders_observations` via `record_blockholder_observation` (`app/services/ownership_observations.py:561-643`). PK on `(instrument_id, reporter_cik, ownership_nature, source, source_document_id, period_end)`. **`reporter_cik` = PRIMARY filer** (`blockholder_filers.cik`), NOT per-row joint reporter — per #837 lesson (`ownership_observations.py:585-590`). Joint reporters on the same accession collapse to one observation row per SEC convention.

ON CONFLICT DO UPDATE — idempotent re-ingest. Carries `aggregate_amount_owned` + `percent_of_class` (the 5%+ block size). Tombstones via `known_to` column.

## 9. Current table refresh
`refresh_blockholders_current` (`ownership_observations.py:646-758`). PG17 MERGE writer per #1255. DISTINCT ON `(reporter_cik, ownership_nature)` ORDER BY `filed_at DESC, period_end DESC, source_document_id ASC` — latest filing per primary-filer wins. Drift state in `ownership_refresh_state` (category `'blockholders'`). Daily drift-repair via `_CATEGORIES[2]` (`app/jobs/ownership_observations_repair.py:93-98`).

## 10. Operator-visible endpoint
`GET /instruments/{symbol}/blockholders` (`app/api/instruments.py:3563`). Returns `BlockholdersResponse` — latest non-superseded filing per primary-filer per issuer with joint-filing reporters collapsed (one row per ≥5% block). Also contributes to `GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121`) `blockholders` slice.

## 11. Verification queries
```sql
-- Active 13D blockholders for AAPL.
SELECT bf.cik AS primary_filer_cik, bf.name AS primary_filer_name,
       bfl.submission_type, bfl.aggregate_amount_owned, bfl.percent_of_class,
       bfl.date_of_event, bfl.filed_at
FROM ownership_blockholders_current obc
JOIN blockholder_filings bfl ON bfl.accession_number = obc.source_accession
JOIN blockholder_filers bf ON bf.cik = obc.reporter_cik
WHERE obc.instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'AAPL')
  AND obc.source = '13d'
ORDER BY obc.filed_at DESC;
```
Smoke: `curl localhost:8000/instruments/AAPL/blockholders | jq '.blockholders[:5]'`. Cross-source: spot-check against the SEC EDGAR full-text search for `efts.sec.gov/LATEST/search-index?q=&forms=SC+13D&ciks={issuer_cik}` (note: post-2024-12-18 the index does NOT index by SUBJECT CIK, only by FILER CIK — use the issuer's submissions JSON for the latest blockholder set instead).

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_manifest_source_has_registered_parser[sec_13d]` + `test_source_has_spec_file[sec_13d]` + `test_source_spec_has_required_sections[sec_13d]`.

## 13. Known gotchas
1. **2024-12-18 XML mandate cutover** (`.claude/skills/data-sources/sec-edgar.md:288, 599`). Pre-mandate filings are HTML-only and not parseable by `edgartools.beneficial_ownership.schedule13` (skill G11) or any extant library here. PR11 chose the date-floor approach (`blockholders.py:79-117`) — every filing inside the window is guaranteed parseable.
2. **Filing-agent CIK guard** (`sec_13dg.py:117-149`). `row.cik` ∈ `KNOWN_FILING_AGENT_CIKS` = upstream discovery bug; tombstone immediately to surface (archive walks under agent CIKs 404).
3. **CUSIP-only resolution** (`sec_13dg.py:320-333`). `efts.sec.gov/LATEST/search-index` post-2024-12-18 does NOT index SC 13D/G by SUBJECT CIK — only by FILER CIK. The earlier 5-case CUSIP-vs-hint cross-validation branch + `sec_13dg_discovery_issuer_hint` side table are retired (sql/162). Legacy daily-index path remains the discovery mechanism.
4. **CUSIP-unresolved filings STILL write `blockholder_filings` rows** with `instrument_id=NULL` — audit trail preserved; rollup join gated by #740 CUSIP backfill coverage. `_record_ingest_attempt(status='partial', error='cusip_unresolved (cusip=...)')` records the reason.
5. **`reporter_cik` in observations = PRIMARY filer, NOT per-row joint reporter** (#837 lesson at `ownership_observations.py:585-590`). Joint filers claim the same beneficial ownership; one observation row per accession.
6. **Manifest URL may be wrong attachment** — always rebuild via `_archive_file_url(filer_cik, accession, "primary_doc.xml")` (`sec_13dg.py:174-179`).
7. **Composite-form mapping** (`sec_manifest.py:872-877`) — both `SC 13D` AND `SCHEDULE 13D` (post-BOM normalisation) map to `sec_13d`; both required.
