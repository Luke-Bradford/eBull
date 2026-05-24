# sec_13g

**Class.** SEC manifest.
**Form / endpoint.** Schedule 13G / SC 13G/A — beneficial owner ≥5% (passive intent / qualified institutional investor / exempt investor, Rule 13d-1(b)/(c)/(d)). SEC archive: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/primary_doc.xml`.

## 1. Origin
Single-attachment XML (`primary_doc.xml`). **Filer-scoped** — `subject_type='blockholder_filer'`, `subject_id=<filer_cik>`, `instrument_id=NULL` (issuer linkage resolved at parse-time via CUSIP → `external_identifiers`). Same provider, canonical-URL builder, and parser callable as `sec_13d` — `_parse_13dg` is registered against BOTH sources (`sec_13dg.py:442-443`); the parsed `submission_type` field on the cover-page XML disambiguates 13D vs 13G downstream.

Form-mapping at `app/services/sec_manifest.py:874-879` — both `"SC 13G"` / `"SC 13G/A"` (legacy filer form) AND `"SCHEDULE 13G"` / `"SCHEDULE 13G/A"` (post-2024-12-18 BOM-normalised form) map to `"sec_13g"`.

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='blockholder_filer'` + `subject_id=<filer_cik>` + `source='sec_13g'`. Driver column: `last_known_filed_at` (`data_freshness.py:116-128`). Cadence ceiling **90d** (`data_freshness.py:81`) — same posture as 13D (event-driven; Atom feed primary). Manifest watermark column: `sec_filing_manifest.parser_version` — re-parse trips when `_PARSER_VERSION_13DG` bumps; current = `"13dg-primary-v1"` (`blockholders.py:69`, shared with 13D).

## 3. Retry posture
Identical to 13D — same parser callable, same retention floor, same upsert path. See `sec_13d.md` §3 for the full list. Key branches at `sec_13dg.py`:
- Empty / missing `row.cik` → `tombstoned` (`sec_13dg.py:107-116`).
- `row.cik` ∈ `KNOWN_FILING_AGENT_CIKS` → `tombstoned` (`sec_13dg.py:117-149`).
- **Retention floor at `max(today − 3y, 2024-12-18)`** (`sec_13dg.py:151-172`) — pre-cap tombstone without fetch.
- Fetch raise → 1h `_failed_outcome` (`_FAILED_RETRY_DELAY` at `sec_13dg.py:81`).
- Empty / non-200 body → `_record_ingest_attempt(status='failed')` + manifest `tombstoned`.
- Parse raise → `tombstoned` (`raw_status='stored'`); unexpected exceptions tagged distinctly.
- Upsert raise: transient `OperationalError` 1h retry vs deterministic constraint violation tombstone.

## 4. Bootstrap path
**No dedicated 13G stage.** Same as 13D — stage 16 `sec_first_install_drain` (`bootstrap_orchestrator.py:1102-1107`, lane `sec_rate`) drains the manifest universe; Layer 1/2/3 discovery (Atom + daily-index + per-CIK submissions walk at stage 14) seeds the manifest. Dev DB carries ~575k discovered SC 13D/G manifest rows shared between 13D and 13G (`sec_13dg.py:331-333`).

## 5. Steady-state path
**No SCHEDULED_JOBS cron for 13G.** Discovery flows via Atom fast-lane + daily-index reconcile + per-CIK submissions walk (cadence 90d). Manifest-worker dispatch processes pending rows on the `sec_rate` lane (10 req/s shared with all SEC sources).

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_13g'`. `subject_type='blockholder_filer'`, `subject_id = <filer_cik_zero_padded>`, `instrument_id = NULL`. Option C `filed_at` gate enforced at `record_manifest_entry`. Retention cap is parser-side (pre-fetch).

## 7. Parser
`app/services/manifest_parsers/sec_13dg.py::_parse_13dg` (registered at `sec_13dg.py:443`). Parser version `_PARSER_VERSION_13DG = "13dg-primary-v1"` (`blockholders.py:69`). Registered with `requires_raw_payload=True`.

Source-discriminated dispatch: `if row.source == "sec_13d": Schedule13D.parse_xml(...)` else `Schedule13G.parse_xml(...)` (`sec_13dg.py:258-261`). 13G parsing routes through `edgar.beneficial_ownership.schedule13.Schedule13G.parse_xml` — adapter `build_filing_from_edgartools_dict` normalises to `BlockholderFiling`.

Persistence path identical to 13D — `_upsert_filer` → `_upsert_filing_row` (per reporting person) → conditional `_record_13dg_observation_for_filing` + `refresh_blockholders_current` → `_record_ingest_attempt`. See `sec_13d.md` §7 for the step-by-step (`sec_13dg.py:318-385`).

## 8. Observation insert
`ownership_blockholders_observations` via `record_blockholder_observation` (`ownership_observations.py:561-643`). Same PK + ON CONFLICT semantics as 13D. The `source` column distinguishes 13D vs 13G; the `submission_type` column carries the SEC form code (`"SC 13G"` / `"SCHEDULE 13G/A"` / etc).

13G filings cover the SAME 5%+ block category as 13D and use the same `ownership_blockholders_observations` table — the `_current` MERGE supersedes by `filed_at DESC` regardless of which schedule the latest filing carries (i.e. a 13D filed after a prior 13G/A by the same reporter wins; `app/api/instruments.py:3603-3622`).

## 9. Current table refresh
`refresh_blockholders_current` (`ownership_observations.py:646-758`) — shared with 13D. PG17 MERGE writer per #1255. DISTINCT ON `(reporter_cik, ownership_nature)` ORDER BY `filed_at DESC, period_end DESC, source_document_id ASC`. Daily drift-repair via `_CATEGORIES[2]` (`app/jobs/ownership_observations_repair.py:93-98`).

## 10. Operator-visible endpoint
`GET /instruments/{symbol}/blockholders` (`app/api/instruments.py:3563`) — 13G appears alongside 13D in the unified ≥5% blockholder list, distinguishable by the row's `submission_type` field. Also contributes to `GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121`) `blockholders` slice.

## 11. Verification queries
```sql
-- Active 13G blockholders for AAPL.
SELECT bf.cik AS primary_filer_cik, bf.name AS primary_filer_name,
       bfl.submission_type, bfl.aggregate_amount_owned, bfl.percent_of_class,
       bfl.date_of_event, bfl.filed_at
FROM ownership_blockholders_current obc
JOIN blockholder_filings bfl ON bfl.accession_number = obc.source_accession
JOIN blockholder_filers bf ON bf.cik = obc.reporter_cik
WHERE obc.instrument_id = (SELECT id FROM instruments WHERE symbol = 'AAPL')
  AND obc.source = '13g'
ORDER BY obc.filed_at DESC;
```
Smoke: `curl localhost:8000/instruments/AAPL/blockholders | jq '[.blockholders[] | select(.submission_type | startswith("SC 13G") or startswith("SCHEDULE 13G"))]'`. Cross-source: institutional 13G filers (Vanguard / BlackRock / State Street) on AAPL/MSFT — spot-check the latest 13G/A against `whalewisdom.com` or `gurufocus.com`.

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_manifest_source_has_registered_parser[sec_13g]` + `test_source_has_spec_file[sec_13g]` + `test_source_spec_has_required_sections[sec_13g]`.

## 13. Known gotchas
1. **2024-12-18 XML mandate cutover** (`.claude/skills/data-sources/sec-edgar.md:288, 599`). Pre-mandate filings HTML-only and not parseable (`edgartools` G11). PR11 date-floor approach at `blockholders.py:79-117`.
2. **One callable handles BOTH 13D and 13G** (`sec_13dg.py:442-443`). Bumping `_PARSER_VERSION_13DG` rewashes BOTH sources — they share `filing_raw_documents.document_kind='primary_doc_13dg'` (`sec_13dg.py:227`).
3. **13D vs 13G supersession** — the same `_current` row supersedes by `filed_at DESC` regardless of schedule (`ownership_observations.py:690-695`). A 13D filed after a prior 13G/A by the same reporter wins (status switch — active intent overrides passive); the `submission_type` column on the current row reflects which one is live.
4. **CUSIP-only resolution** (post-PR11 v8 empirical pivot; `sec_13dg.py:320-333`). `efts.sec.gov` doesn't index by SUBJECT CIK; rely on the legacy daily-index path + `_resolve_cusip_to_instrument_id`.
5. **CUSIP-unresolved 13G filings STILL write `blockholder_filings`** with `instrument_id=NULL` — audit-log row carries `partial` status + `cusip_unresolved (cusip=...)` error; rollup gated by #740.
6. **`reporter_cik` = PRIMARY filer** (joint reporters collapse to one observation row; #837 lesson).
7. **Filing-agent CIK guard** (`sec_13dg.py:117-149`) — same defensive tombstone as 13D.
8. **Manifest URL canonicalisation** (`sec_13dg.py:174-179`) — always rebuild via `_archive_file_url(filer_cik, accession, "primary_doc.xml")`; manifest URL may be the filing-index page.
9. **Composite-form mapping** (`sec_manifest.py:874-879`) — both `SC 13G` AND `SCHEDULE 13G` (post-BOM normalisation) map to `sec_13g`; both required.
