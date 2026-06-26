# sec_def14a

**Class.** SEC manifest.
**Form / endpoint.** DEF 14A — definitive proxy statement (annual). SEC archive: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{primary_doc.html}`.

## 1. Origin
HTML primary document containing one or more beneficial-ownership tables (Item 12 / SCT-style layout). Issuer-scoped: manifest carries issuer CIK; parser fans out across share-class siblings via `siblings_for_issuer_cik` (`app/services/manifest_parsers/def14a.py:83, 111-148, 334-348`). Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`.

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='issuer'`, `subject_id=<issuer_cik>`. Cadence ceiling 365d (`app/services/data_freshness.py:90` — Codex v3 tightened from 395d to match spec annual exactly). Atom + daily-index drive discovery.

## 3. Retry posture
- Primary-doc fetch raise (transient) → `_failed_outcome` 1h backoff (`def14a.py:90, 102-108`).
- Empty body / 404 → tombstone + ingest-log row.
- Parser score-floor miss (no beneficial-ownership table identified) → tombstone with `best_score=<n>` audit detail (`def14a.py:327-332`).
- Upsert failure: `is_transient_upsert_error` discriminator (`def14a.py:402` via `_classify.py`) — transient retries 1h, deterministic tombstones with ingest-log `status='failed'`.

## 4. Bootstrap path
Stage 17 `sec_def14a_bootstrap` (`app/services/bootstrap_orchestrator.py:1108`). Lane `sec_rate`. Dispatches `JOB_SEC_DEF14A_BOOTSTRAP = "sec_def14a_bootstrap"` (`app/workers/scheduler.py:286, 4302`). Walks `institutional_filer_seeds` ∪ universe issuers; enqueues DEF 14A manifest rows. Per-filer cap `DEF14A_LATEST_PER_FILER_CAP = 2` (`app/services/def14a_ingest.py:107`) enforced parser-side via `def14a_within_cap` (`def14a_ingest.py:455-551`) — latest two filings per issuer kept, older accessions tombstone with cap audit detail.

## 5. Steady-state path
`sec_def14a_ingest` retired from `SCHEDULED_JOBS` post-#1155 (`app/workers/scheduler.py:737-742`). Atom-discovered freshness now drives the manifest worker. Weekly `sec_def14a_bootstrap` Sunday 02:30 UTC kept as safety-net (`app/workers/scheduler.py:779`). Admin "Run now" via `POST /jobs/sec_def14a_bootstrap/run` (`app/workers/scheduler.py:825-827`).

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_def14a'`. `subject_type='issuer'`, `subject_id=<issuer_cik_zero_padded>`, `instrument_id=<primary_share_class_iid>`. Option C `filed_at` gate at `record_manifest_entry`. Pre-cap accessions can still discover (no early gate); cap enforced post-parse so audit trail captures the supersession.

**Form scope (`_FORM_TO_SOURCE`).** `DEF 14A` (definitive) + `DEFA14A` (additional definitive) + `DEFM14A` (merger) + `DEFR14A` (revised definitive) map to `sec_def14a`. **`PRE 14A` is deliberately excluded (#1320)** — preliminary proxies are pre-finalisation drafts, classified metadata-only; the definitive DEF 14A that follows is what we ingest. Mapping PRE 14A routed 6k+ drafts into this manifest namespace which the parser tombstoned pre-fetch (§7) — wasted worker cycles + polluted `WHERE source='sec_def14a'` reads. It is now skipped at discovery (`map_form_to_source('PRE 14A') is None`). The parser's PRE-14A tombstone branch stays as defense-in-depth for any PRE row that reaches the worker via a legacy/manual seed. Existing mis-seeded rows purged by `sql/182_purge_pre14a_manifest_rows.sql`.

## 7. Parser
`app/services/manifest_parsers/def14a.py::_parse_def14a`. Version `_PARSER_VERSION_DEF14A` (`def14a_ingest.py:66`). Registered with `requires_raw_payload=True` — HTML body saved to `filing_raw_documents` BEFORE parse.

Extraction:
1. Resolve issuer CIK from `instrument_sec_profile`; `_CIK_MISSING_SENTINEL` triggers single-instrument-only path.
2. Fetch primary doc + `store_raw` inside committed savepoint.
3. `parse_beneficial_ownership_table` (`app/providers/implementations/sec_def14a.py`) returns `Def14ABeneficialOwnershipTable` with `rows` + `raw_table_score` + `as_of_date`.
4. Score below floor → tombstone (`def14a.py:327-332`).
5. Resolve share-class siblings (`_resolve_siblings`, `def14a.py:111-148`); for each sibling, batch-write `def14a_beneficial_holdings`, `ownership_def14a_observations` (via `_record_def14a_observations_for_filing`), `ownership_esop_observations` (via `_record_esop_observations_for_filing`).

## 8. Observation insert
Two destinations:
- **`ownership_def14a_observations`** — non-ESOP beneficial holders (`def14a.py:363-369`). Per-(instrument, holder) row.
- **`ownership_esop_observations`** — ESOP plans (`def14a.py:371-379`). Routed via name-pattern detection at parser AND in the legacy sync path (`app/services/ownership_observations_sync.py:706` — `is_esop_plan` + `extract_plan_name_and_trustee`) so pre-#843 rows backfill correctly.

Tombstone semantics: superseded rows close `known_to=NOW()` via standard observation writer.

**Treasury is NOT written by this parser.** Treasury observations come from a different source — `app/services/ownership_observations_sync.py::sync_treasury` (`:565-638`) mirrors `financial_periods.treasury_shares` (XBRL DEI / us-gaap `TreasuryStockShares`) into `ownership_treasury_observations` with `source='xbrl_dei'`. Listed here only because the brief associated DEF 14A with treasury — see #1313 for the architectural debate on whether DEF 14A's "Shares held by issuer in treasury" disclosure should also be parsed.

## 9. Current table refresh
- `refresh_def14a_current(conn, instrument_id=sibling_iid)` per sibling (`def14a.py:370`).
- `refresh_esop_current(conn, instrument_id=sibling_iid)` per sibling if ESOP rows written (`def14a.py:378-379`).

MERGE writer per #1255. Categories `def14a` + `esop` in `_CATEGORIES` (`app/jobs/ownership_observations_repair.py:69` + downstream entries) — daily drift-repair sweep covers BOTH.

## 10. Operator-visible endpoint
`GET /instruments/{symbol}/ownership-rollup` (`app/api/instruments.py:4121`). Returns `OwnershipRollup` with `def14a` + `esop` slices.

## 11. Verification queries
```sql
-- Latest DEF 14A holders for AAPL.
SELECT i.symbol, odo.holder_name, odo.holder_role, odo.shares, odo.percent_of_class, odo.period_end
FROM ownership_def14a_observations odo
JOIN instruments i ON i.instrument_id = odo.instrument_id
WHERE i.symbol = 'AAPL' AND odo.known_to IS NULL
ORDER BY odo.filed_at DESC, odo.shares DESC NULLS LAST LIMIT 20;

-- ESOP plans for HD.
SELECT i.symbol, oeo.plan_name, oeo.plan_trustee_name, oeo.shares, oeo.period_end
FROM ownership_esop_observations oeo
JOIN instruments i ON i.instrument_id = oeo.instrument_id
WHERE i.symbol = 'HD' AND oeo.known_to IS NULL
ORDER BY oeo.filed_at DESC LIMIT 10;
```
Smoke: `curl localhost:8000/instruments/AAPL/ownership-rollup | jq '.def14a, .esop'`. Cross-source: spot-check insider+5% holders against `proxymonitor.org` filings DB or SEC EDGAR full-text DEF 14A.

## 12. Smoke test
Import-time gate — `tests/smoke/test_etl_source_to_sink.py`, the per-source parametrized cases: `test_source_has_spec_file[sec_def14a]`, `test_source_spec_has_required_sections[sec_def14a]`, `test_manifest_source_has_registered_parser[sec_def14a]`, `test_manifest_source_form_mapping_present[sec_def14a]`, `test_manifest_source_has_freshness_cadence[sec_def14a]`, `test_manifest_source_has_sink_tables[sec_def14a-spec*]` (asserts the declared sinks `ownership_def14a_observations` / `_current` + `ownership_esop_observations` / `_current` exist).

Not covered by the import-time gate (verified by the live-smoke runbooks under `app/runbooks/`, not pytest): bootstrap stage 17 in `_BOOTSTRAP_STAGE_SPECS` and the operator-visible figure.

## 13. Known gotchas
1. **Two observation destinations from one parser.** `_record_def14a_observations_for_filing` + `_record_esop_observations_for_filing` MUST both run before `_record_ingest_attempt(status='success')` (`def14a.py:363-388`). Partial-fan-out is rolled back by the single outer `with conn.transaction()`.
2. **Share-class fan-out via siblings_for_issuer_cik** — issuer-level filing writes per-instrument rows so the per-class rollup is consistent (post-#1117 PR-B pattern).
3. **Per-filer cap = 2** (`DEF14A_LATEST_PER_FILER_CAP`). Older accessions tombstone with cap audit; rewash bumps cap → re-discover needed via `sec_rebuild`.
4. **HTML score-floor parser**. `Def14ABeneficialOwnershipTable.raw_table_score` is a fuzzy heuristic — score-floor tombstones are common (notice-only proxies, exotic layouts). Operator should not panic on per-filing tombstones; check the cohort metric instead.
5. **ESOP routing**: name-pattern detection (`is_esop_plan`) lives in BOTH the parser AND the legacy sync (`ownership_observations_sync.py:706`) — change ONE and both must change, or pre-#843 backfill diverges from manifest-handled rows.
6. **Treasury is NOT here** (see §8). Misattribution risk is high; `ownership_treasury_observations.source` is `'xbrl_dei'`, not `'def14a'`.
