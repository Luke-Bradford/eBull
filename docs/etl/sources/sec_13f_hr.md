# sec_13f_hr

**Class.** SEC manifest.
**Form / endpoint.** Form 13F-HR / 13F-HR/A — quarterly institutional manager holdings. SEC archive: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{filename}`.

## 1. Origin
Two-attachment XML payload per accession: `primary_doc.xml` + `infotable.xml`. Filer-scoped (institutional manager CIK, discretionary AUM > $100M per 15 USC 78m(f)). Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`. Archive walk goes through `index.json` then per-attachment fetch (`app/services/manifest_parsers/sec_13f_hr.py:177-217`).

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='institutional_filer'`, `subject_id=<filer_cik>`. Cadence ceiling 120d (`app/services/data_freshness.py:87`) — quarter-end (~90d) + 45d filing window. Atom-feed fast-lane + daily-index reconcile drive discovery. Cohort gate `institutional_filers.last_13f_hr_at` (added per #1010, `project_1010_13f_cohort_bound.md`) bounds bootstrap sweep to filers with a 13F-HR in the last 380d — collapses 11.2k → 8.7k.

## 3. Retry posture
- Archive `index.json` 404 / empty body → tombstone + `_record_ingest_attempt(status='failed')`.
- Missing primary_doc or infotable attachment → tombstone (deterministic; re-fetch yields same gap).
- Fetch raise (transient) → `_failed_outcome` with 1h backoff (`_FAILED_RETRY_DELAY` at `sec_13f_hr.py:86`).
- Per-row upsert failure: `is_transient_upsert_error` (`app/services/manifest_parsers/_classify.py`) discriminates transient `psycopg.OperationalError` (1h retry) vs deterministic constraint violation (tombstone + ingest-log row).
- Defense-in-depth at `sec_13f_hr.py:141-170`: if `row.cik` is a `KNOWN_FILING_AGENT_CIKS` member, tombstone immediately (discovery bug surfaces loudly instead of every accession 404-tombstoning).

## 4. Bootstrap path
Stage 22 `sec_13f_recent_sweep` (`app/services/bootstrap_orchestrator.py:1126-1145`). Lane `sec_rate` (10 req/s shared). Dispatches `JOB_SEC_13F_QUARTERLY_SWEEP` (`sec_13f_quarterly_sweep`, `app/workers/scheduler.py:4490`) with two dynamic-resolved params + one static audit label:
- `min_period_of_report=_PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF` (today − 380d at UTC midnight)
- `min_last_13f_hr_at=_PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF` (today − 380d, cohort bound)
- `source_label="sec_edgar_13f_directory_bootstrap"` (audit-only).

Wall-clock band: 80→6.8 min post-PR-3 cohort bound (12×) per `project_1233_run2_measurement.md`.

## 5. Steady-state path
Atom feed + daily-index discovery seed the manifest. Per-CIK `data_freshness_index` reconcile re-polls submissions.json at cadence 120d. Bulk `sec_13f_quarterly_sweep` retired from `SCHEDULED_JOBS` post-#1155 (`app/workers/scheduler.py:924-932`); Admin "Run now" sweep-adapter `sec_13f_sweep` remains for operator-triggered backfill.

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_13f_hr'`. `subject_type='institutional_filer'`, `subject_id=<filer_cik_zero_padded>`, `instrument_id=NULL` (per-row issuer linkage by CUSIP at parse time, see §7). Option C `filed_at` gate at `record_manifest_entry` (`app/services/sec_manifest.py:194-217`) — pre-cap rows skipped at discovery, not parse time.

## 7. Parser
`app/services/manifest_parsers/sec_13f_hr.py::_parse_13f_hr`. Composite parser version `13f-hr-primary:{_PARSER_VERSION_13F_PRIMARY}+infotable:{_PARSER_VERSION_13F_INFOTABLE}` (`sec_13f_hr.py:90`). Registered with `requires_raw_payload=True` (`sec_13f_hr.py:599`) — both attachments saved to `filing_raw_documents` in savepoints BEFORE parse so re-wash never re-fetches.

Extraction:
1. Walk `index.json` → primary_doc + infotable filenames (`parse_archive_index`).
2. Fetch + `store_raw` primary_doc → `parse_primary_doc` → `info` (filer + period_of_report).
3. **Post-parse 8-quarter retention gate** (`sec_13f_hr.py:338` via `thirteen_f_within_retention`) — keys on `info.period_of_report` (intrinsic quarter), NOT `row.filed_at`. 13F-HR/A amendments restating pre-cap quarters tombstone correctly.
4. Fetch + `store_raw` infotable → `parse_infotable` → list[ThirteenFHolding].
5. Per holding: drop PRN (`sec_13f_hr.py:485-487` — `shares_or_principal_type != "SH"`); apply VALUE thousands-scaling if `filed_at < _VALUE_DOLLARS_CUTOVER (2023-01-03)` (`sec_13f_hr.py:104, 467, 490-491`); resolve CUSIP → `instrument_id` (unresolved CUSIPs recorded to `unresolved_13f_cusips`).
6. Per-row `_upsert_holding` + per-instrument `_record_13f_observations_for_filing` + `refresh_institutions_current` (`sec_13f_hr.py:518-527`).

## 8. Observation insert
`ownership_institutions_observations`. Per-(instrument, exposure-bucket) row with `exposure_key ∈ {EQUITY, PUT, CALL}` (`sec_13f_hr.py:513`). Tombstone semantics: superseded rows close `known_to=NOW()` via standard observation writer pattern.

## 9. Current table refresh
`refresh_institutions_current` (`app/services/ownership_observations.py`). MERGE writer per #1255 (`project_1233_pr12_ownership_merge_writer.md`). Category `institutions` in `_CATEGORIES` (`app/jobs/ownership_observations_repair.py:87-92`) — daily drift-repair sweep guarantees reconciliation within 24h regardless of writer path.

## 10. Operator-visible endpoint
`GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121-4178`). Returns `OwnershipRollup` with `institutions` slice. Backing service: `app/services/ownership_rollup.py::get_ownership_rollup`.

## 11. Verification queries
```sql
-- Latest 13F-HR observation for AAPL.
SELECT i.symbol, oio.filer_cik, ifr.name AS filer_name, oio.shares, oio.market_value_usd, oio.period_end
FROM ownership_institutions_observations oio
JOIN instruments i ON i.instrument_id = oio.instrument_id
JOIN institutional_filers ifr ON ifr.cik = oio.filer_cik
WHERE i.symbol = 'AAPL'
ORDER BY oio.filed_at DESC LIMIT 10;
```
Smoke: `curl localhost:8000/instruments/AAPL/ownership-rollup | jq '.institutions[:5]'`. Cross-source: spot-check top-10 against `whalewisdom.com` or `gurufocus.com` quarterly 13F page.

## 12. Smoke test
Import-time gate — `tests/smoke/test_etl_source_to_sink.py`, the per-source parametrized cases: `test_source_has_spec_file[sec_13f_hr]`, `test_source_spec_has_required_sections[sec_13f_hr]`, `test_manifest_source_has_registered_parser[sec_13f_hr]`, `test_manifest_source_form_mapping_present[sec_13f_hr]`, `test_manifest_source_has_freshness_cadence[sec_13f_hr]`, `test_manifest_source_has_sink_tables[sec_13f_hr-spec*]` (asserts the declared sinks `ownership_institutions_observations` / `ownership_institutions_current` exist).

Not covered by the import-time gate (verified by the live-smoke runbooks under `app/runbooks/`, not pytest): provider importable, bootstrap stage 22 in `_BOOTSTRAP_STAGE_SPECS`, and the operator-visible figure.

## 13. Known gotchas
1. **VALUE-cutover 2023-01-03** (`sec_13f_hr.py:92-104, 467, 490-491`). SEC EDGAR Release 22.4.1 switched Column 4 from $thousands to whole $dollars. Branch on `filed_at` NOT `period_of_report` — a 2022Q4 restatement filed March 2023 was entered in new-regime whole dollars.
2. **PRN drop** (`sec_13f_hr.py:480-487`). SSHPRNAMTTYPE='PRN' = bond principal in dollars; per-filing path historically failed to drop these, causing silent share-count inflation. Codex pre-push #1133 caught.
3. **Filing-agent CIK guard** (`sec_13f_hr.py:141-170`, #1249). `row.cik` in `KNOWN_FILING_AGENT_CIKS` = discovery bug; tombstone to surface.
4. **Composite parser version**. Bumping EITHER `_PARSER_VERSION_13F_PRIMARY` or `_PARSER_VERSION_13F_INFOTABLE` triggers re-wash via composite (`sec_13f_hr.py:90`).
5. **Cohort bound is HR-only** (`project_1010_13f_cohort_bound.md`). `last_13f_hr_at` tracks 13F-HR/A only — distinct from `last_filing_at` which includes 13F-NT.
6. **Spike memo**: `docs/_archive/2026-05/spike-13f-hr-edgartools.md` documents the per-filing-vs-bulk-dataset split.
