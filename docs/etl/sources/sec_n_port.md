# sec_n_port

**Class.** SEC manifest.
**Form / endpoint.** NPORT-P / NPORT-P/A — quarterly investment-company portfolio holdings. SEC archive primary_doc.xml. Only the public quarterly slice is in scope (the monthly NPORT-MFP filings stay confidential).

## 1. Origin
Single-attachment XML payload (`primary_doc.xml`) per accession. Fund-trust-scoped: manifest row carries trust CIK; per-holding rows fan out to individual portfolio issuers via CUSIP resolution. Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`. Parser is an EdgarTools wrapper (`edgar.funds.reports.FundReport.parse_fund_xml`) per #932 spike — see §13.

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='institutional_filer'` (filer-scoped) OR `subject_type='fund_series'` (when discovery routes the series). Cadence ceiling 90d (`app/services/data_freshness.py:92`) — 60d filing window + buffer. Atom + daily-index + per-trust submissions.json walk drive discovery.

## 3. Retry posture
- `primary_doc.xml` fetch raise (transient) → `_failed_outcome` 1h backoff (`app/services/manifest_parsers/sec_n_port.py:79, 82-92`).
- Empty body → tombstone + ingest-log row.
- `NPortMissingSeriesError` raise (filing lacks `<seriesId>`) → tombstone (deterministic; cannot synthesise collision-prone fallback identity — Codex pre-impl finding #2 at `app/services/n_port_ingest.py:36-40`).
- `NPortParseError` raise — every EdgarTools failure mode is converted (`n_port_ingest.py:28-31`); tombstone.
- Per-row upsert transient `psycopg.OperationalError` → 1h retry; deterministic constraint violation → tombstone + ingest-log row (`#1131` discrimination via `_classify.py`).
- Defense-in-depth filing-agent CIK guard at `sec_n_port.py:116-141` (#1250) — symmetric with sec_13f_hr.

## 4. Bootstrap path
Stage 23 `sec_n_port_ingest` (`app/services/bootstrap_orchestrator.py:1146-1163`). Lane `sec_rate`. Dispatches `JOB_SEC_N_PORT_INGEST = "sec_n_port_ingest"` (`app/workers/scheduler.py:295, 5305`) with one dynamic param:
- `min_last_seen_filed_at=_PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF` (today − 380d UTC midnight; `bootstrap_orchestrator.py:166, 198, 1161`).

PR7 #1233 §4.6 cohort bound (mirror of #1010 for 13F-HR). Collapses ~5k registered trusts to ~3-4k actively-filing. Wall-clock band: 46→9.25 min on Run #2 (5× speedup) per `project_1233_run2_measurement.md`. Bulk historical path is the dataset-quarterly stage 12 (`sec_nport_ingest_from_dataset`); this stage tops up post-bulk.

## 5. Steady-state path
`sec_n_port_ingest` retired from `SCHEDULED_JOBS` post-#1155 (`app/workers/scheduler.py:990`). Daily / Admin "Run now" dispatch `sec_n_port_ingest` with empty params → full cohort (safety-net for previously-inactive trusts re-emerging, `bootstrap_orchestrator.py:1156-1162`). Manifest worker drives atom-discovered freshness one accession at a time.

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_n_port'`. `subject_type='institutional_filer'` or `'fund_series'` depending on discovery path. `subject_id=<trust_cik_zero_padded>` or `<series_id>`. `instrument_id=NULL` (per-holding issuer linkage at parse time). Option C `filed_at` gate at `record_manifest_entry`.

## 7. Parser
`app/services/manifest_parsers/sec_n_port.py::_parse_n_port`. Version `_PARSER_VERSION_NPORT = "nport-v2-edgartools"` (`app/services/n_port_ingest.py:82`). Registered with `requires_raw_payload=True` (`sec_n_port.py:40-43`) — body persisted BEFORE parse so re-wash bumps never re-fetch.

Extraction (`n_port_ingest.py::parse_n_port_payload`, EdgarTools-backed):
1. Lazy-import EdgarTools FundReport (`_edgar_fund_report`, #925 drop-in pattern).
2. Parse XML → `NPortFiling` + list[`NPortHolding`].
3. Apply filter cascade (Codex pre-impl review):
   - equity-common only (`record_fund_observation` guard, also in seeder).
   - Long payoff only.
   - NS (shares) units only — no PRN.
   - Non-zero shares.
   - Resolved CUSIP (`_resolve_cusip_to_instrument_id`; unresolved → `unresolved_13f_cusips` with `source='bulk_nport_dataset'` for OpenFIGI sweep, see bootstrap stage 13).
4. Tier 1 retention gate: `n_port_within_retention(parsed.period_end)` (`n_port_ingest.py:1017`). 8-quarter sliding window via `NPORT_RETENTION_QUARTERS = 8` (`n_port_ingest.py:118-167`). Boundary on `period_of_report` NOT `filed_at` — amendments restating pre-cap periods correctly tombstone.
5. Per-touched-instrument: `record_fund_observation` + `refresh_funds_current`.
6. `_record_ingest_attempt` rolls up `success` / `partial` (some CUSIPs unresolved) / `failed`.

## 8. Observation insert
`ownership_funds_observations`. Per-(instrument, fund_series, period_end) row. `sec_fund_series` upserted via `upsert_sec_fund_series`. Refresh ordering: `filed_at DESC` so amendments win (Codex pre-impl #5).

## 9. Current table refresh
`refresh_funds_current(conn, instrument_id=...)` per touched instrument (`sec_n_port.py` ingest body). MERGE writer per #1255. Category `funds` in `_CATEGORIES` (`app/jobs/ownership_observations_repair.py:69`).

**Critical:** funds = NPORT-derived; this is the ONLY write-through path. There is no legacy mirror source for funds (per `app/jobs/ownership_observations_repair.py:74-78` — funds row in `_CATEGORIES` exists precisely because the daily drift-repair sweep is the SOLE daily reconciliation path).

## 10. Operator-visible endpoint
`GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121`) returns `OwnershipRollup` with `funds` slice.

## 11. Verification queries
```sql
-- Top fund holders for MSFT.
SELECT i.symbol, sfs.fund_series_name, ofo.shares, ofo.market_value_usd, ofo.period_end
FROM ownership_funds_observations ofo
JOIN instruments i ON i.instrument_id = ofo.instrument_id
JOIN sec_fund_series sfs ON sfs.fund_series_id = ofo.fund_series_id
WHERE i.symbol = 'MSFT' AND ofo.known_to IS NULL
ORDER BY ofo.market_value_usd DESC NULLS LAST LIMIT 20;
```
Smoke: `curl localhost:8000/instruments/MSFT/ownership-rollup | jq '.funds[:10]'`. Cross-source: spot-check top-10 holders against `fintel.io` mutual-fund holdings page or SEC EDGAR direct NPORT-P viewer.

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_sec_n_port_wired`. Asserts parser registered, stage 23 in `_BOOTSTRAP_STAGE_SPECS`, `ownership_funds_observations` + `ownership_funds_current` + `sec_fund_series` tables exist.

## 13. Known gotchas
1. **EdgarTools Pydantic validation cliff (#932)**. `pyproject.toml:21` pins `edgartools==5.30.2`. Pin-bump risk: internal `pydantic.ValidationError` on missing required Decimal fields is converted to `NPortParseError` (`n_port_ingest.py:28-31`) — a pin bump may silently broaden the failure surface. See `.claude/skills/data-sources/edgartools.md` for the decision tree.
2. **Both form spellings accepted**: `NPORT-P` / `NPORT-P/A` (current) AND `N-PORT` / `N-PORT/A` (legacy). Codex pre-impl #1 at `n_port_ingest.py:35-37`.
3. **Missing `<seriesId>` is fatal-deterministic**: tombstone, no synthesised fallback (Codex pre-impl #2).
4. **Funds have no legacy mirror.** The `_CATEGORIES` daily sweep is the SOLE daily reconciliation path — if `refresh_funds_current` is broken, the panel goes stale silently. Smoke test the funds slice on every NPORT-related deploy.
5. **8-quarter retention is month-end-anchored**, not period-of-report-anchored — every fund sees exactly 8 of its fiscal-Q snapshots regardless of fiscal-year alignment (`n_port_ingest.py:138-143`).
6. **Parser is pure XML-in / dataclass-out** (Codex pre-impl #6). No network calls during parse — test in `tests/services/test_n_port_ingest.py` proves the offline guarantee by raising on every HTTP client.
7. **Ingest log measures accessions, not row dimension** (Codex pre-impl #11 at `n_port_ingest.py:48`). Don't confuse "1 accession parsed" with "1 holding stored".
