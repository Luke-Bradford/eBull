# sec_xbrl_facts

**Class.** SEC manifest (synth no-op parser, G7).
**Form / endpoint.** XBRL company-facts data. Bulk source: `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` (cached during Phase A3 / Stage 7). Per-CIK source (deep-dive use only): `https://data.sec.gov/api/xbrl/companyfacts/CIK<10>.json`.

## 1. Origin

**The synth no-op parser does NOT call SEC.** The manifest row is the audit signal; XBRL facts have already landed via the Companyfacts bulk JSON path before the manifest row hits the worker.

Real ingest path:

- Bulk archive download: Stage 7 `sec_bulk_download` populates the local `companyfacts.zip` cache (per `app/services/bootstrap_orchestrator.py:1048`).
- Bulk ingest: Stage 9 `sec_companyfacts_ingest` (`app/services/bootstrap_orchestrator.py:1052`) reads the cached zip via `ingest_companyfacts_archive` (`app/services/sec_companyfacts_ingest.py:148`). Routes each `CIK<10>.json` payload through `extract_facts_from_companyfacts_payload` → `_extract_facts_from_section` → `upsert_facts_for_instrument`.
- Steady-state: `JOB_FUNDAMENTALS_SYNC` daily cron at **`app/workers/scheduler.py:616`** (cadence 02:30 UTC per `scheduler.py:631`). Phase 1 of `fundamentals_sync` (`scheduler.py:3193`) pulls per-CIK XBRL + normalizes.

Codex 1 caught earlier docs citing `scheduler.py:562` for `JOB_FUNDAMENTALS_SYNC`; live code has the `ScheduledJob` block at line 616. The constant declaration is at `scheduler.py:280`.

## 2. Watermarking model

- `data_freshness_index` row `(source='sec_xbrl_facts')`. Cadence ceiling **120d** (`app/services/data_freshness.py:99`) — piggybacks on 10-K/10-Q cadence.
- `sec_filing_manifest` row keyed `(source='sec_xbrl_facts', accession_number)`. `raw_status` transitions `pending → parsed` (no fetched/recorded — the synth no-op shortcuts both).
- `financial_facts_raw.ingestion_run_id` — Companyfacts bulk path's per-archive run identifier (`sec_companyfacts_ingest.py:65`). The real audit signal for whether facts landed.

No conditional-GET at the dispatch layer (synth no-op).

## 3. Retry posture

There is no retry posture at the manifest dispatch layer — the synth no-op cannot fail. Body of `_parse_sec_xbrl_facts` is `return ParseOutcome(status='parsed', parser_version='xbrl-facts-noop-v1')` (`app/services/manifest_parsers/sec_xbrl_facts.py:71-74`).

- No `tombstoned` branch — no failure mode requires permanent discard at dispatch.
- No `failed` branch — no DB write that can raise; no fetch that can raise.
- `requires_raw_payload=False` (`sec_xbrl_facts.py:87`) — synth source per sec-edgar §11.5.1.

Real retry posture lives on the Companyfacts bulk ingest path (`sec_companyfacts_ingest.py:_SkipEntry` sentinel) and on `fundamentals_sync` (`scheduler.py:3193`, phase-by-phase failure isolation).

SEC rate-limit pool: **unused by the manifest parser**. The bulk ingest path consumes the zip locally (zero HTTP) per `sec_companyfacts_ingest.py:6-8`. The steady-state per-CIK API path runs under `sec_rate` 10 req/s when needed.

## 4. Bootstrap path

Two stages contribute to XBRL coverage; neither is the synth no-op parser:

- **Stage 9 `sec_companyfacts_ingest`** on the `db` lane (`app/services/bootstrap_orchestrator.py:1052`). Caps required: `bulk_archives_ready` + `cik_mapping_ready` (line 537). Reads `companyfacts.zip`, writes `financial_facts_raw` per matched instrument (share-class fan-out at `sec_companyfacts_ingest.py:224`). Advertises capability `fundamentals_raw_seeded` (line 368).
- **Stage 25 `fundamentals_sync`** on the `db_fundamentals_raw` lane (`app/services/bootstrap_orchestrator.py:1175`). Dispatches the bootstrap-only invoker `fundamentals_sync_bootstrap` at `app/services/fundamentals/bootstrap.py:96` — derivation-only, **NO HTTP** (Stream A PR-C2 / #1310). 4-cap gate per PR-C1: `bulk_archives_ready` + `cik_mapping_ready` + `submissions_processed` + `fundamentals_raw_seeded`.

The bootstrap entrypoint runs phases 2 (`audit_all_instruments`) + 1 (`normalize_financial_periods`) only — phase 3 `review_coverage` fires on the first scheduled `fundamentals_sync` window post-bootstrap (`bootstrap.py:31-42`).

## 5. Steady-state path

`JOB_FUNDAMENTALS_SYNC` daily cron at `app/workers/scheduler.py:616` (cadence 02:30 UTC, `scheduler.py:631`). Runs four phases (`scheduler.py:3193`):

- Phase 0: CIK refresh.
- Phase 1: XBRL pull + normalization.
- Phase 1b: SEC snapshot.
- Phase 2: `audit_all_instruments`.
- Phase 3: `review_coverage` (tier promote / demote).

Manifest worker independently drains `sec_xbrl_facts` rows via the synth no-op as Layer 1/2/3 discovery populates them. Both paths coexist — the synth no-op is for accession-level audit / discovery tracking; `JOB_FUNDAMENTALS_SYNC` is for the actual facts.

## 6. Manifest insert

Row inserted at discovery via `record_manifest_entry`. Shape:

- `source = 'sec_xbrl_facts'` (per `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`).
- `subject_type = 'issuer'`, `subject_id = issuer CIK`.
- `accession_number` = SEC-assigned accession.
- `primary_document_url` populated at discovery; **synth no-op parser does not consume it**.

No Option C `filed_at` gate at the dispatch layer — there is no writer to gate. The real writer (`upsert_facts_for_instrument`) is keyed `(instrument_id, taxonomy, concept, period_end, ...)` with last-write-wins semantics.

## 7. Parser

Module `app/services/manifest_parsers/sec_xbrl_facts.py`. Function `_parse_sec_xbrl_facts` (line 53). Version `_PARSER_VERSION_XBRL_FACTS = "xbrl-facts-noop-v1"` (line 50). Registered at `register()` (line 77) with `requires_raw_payload=False`.

**Synth no-op pattern** per `.claude/skills/data-sources/sec-edgar.md §11.5.1`. Second adopter of the pattern (`sec_10q.py` is the canonical exemplar from #1168).

**Non-caller invariant** (`sec_xbrl_facts.py:29-35`): this module does NOT call `SecFilingsProvider.fetch_document_text`. If a future PR introduces an XBRL-facts manifest-dispatch consumer (e.g. structured-fact extraction beyond the Companyfacts bulk JSON path), that PR must add the fetcher + `tests/test_fetch_document_text_callers.py` allow-list update + SQL normalisation pathway in lockstep.

## 8. Observation insert

**None at the dispatch layer.** Real writes by `sec_companyfacts_ingest.py` go to:

- `financial_facts_raw` — XBRL fact storage, partitioned quarterly. Caps applied at extraction: `_ALL_TRACKED_TAGS` (us-gaap whitelist ~50 numeric concepts) + `_ALL_TRACKED_DEI_TAGS` (DEI shares-outstanding / public-float / employees) + `_default_retention_cutoff()` (today - 20y) per `sec_companyfacts_ingest.py:84-90`.
- `financial_periods_raw` + `financial_periods` — normalized periods, written by `normalize_financial_periods` (called from `fundamentals_sync` phase 1 and the bootstrap entrypoint).
- `ownership_treasury_observations` — treasury-shares observation write-through during normalization.

## 9. Current table refresh

`refresh_ownership_treasury_current` (one of the 7 categories in `_CATEGORIES` at `app/jobs/ownership_observations_repair.py:80`) — MERGE writer per PR #1255 (PG17 `MERGE … WHEN NOT MATCHED BY SOURCE`).

`financial_periods` IS the current canonical table for normalized financial-period values — no separate `_current` table.

The `_CATEGORIES` daily sweep at `ownership_observations_repair.py:69` covers `ownership_treasury_current` and the 6 other ownership categories — integrity floor per `data-engineer/SKILL.md §write-through`.

## 10. Operator-visible endpoint

- `GET /instruments/{symbol}/financials` (`app/api/instruments.py:608`) — sourced from `financial_periods`.
- `GET /instruments/{symbol}/dilution` (`app/api/instruments.py:1585`) — sourced from `ownership_treasury_current` + DEI shares-outstanding facts.
- `GET /instruments/{symbol}/employees` (`app/api/instruments.py:1081`) — sourced from DEI employee facts in `financial_facts_raw`.

XBRL **discovery** is visible via:

- `GET /coverage/manifest-parsers` — confirms `has_registered_parser=True` for `sec_xbrl_facts`.
- Manifest worker stats: `WorkerStats.skipped_no_parser_by_source['sec_xbrl_facts']` MUST stay at 0 per `sec_xbrl_facts.py:11-13`.

## 11. Verification queries

```sql
-- AAPL XBRL fact coverage by taxonomy
SELECT taxonomy, COUNT(*) FROM financial_facts_raw
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol='AAPL')
 GROUP BY taxonomy;

-- Most-recent ingestion run for AAPL
SELECT MAX(ingestion_run_id) FROM financial_facts_raw
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol='AAPL');

-- Period-end window guard check (#1218): nothing outside [1900, 2100)
SELECT period_end, COUNT(*) FROM financial_facts_raw
 WHERE period_end < DATE '1900-01-01' OR period_end >= DATE '2100-01-01'
 GROUP BY period_end;

-- Partition presence check (sql/156 + sql/175 — 2010-2040 quarterly)
SELECT relname FROM pg_class
 WHERE relname LIKE 'financial_facts_raw_%' ORDER BY relname;

-- Manifest drain audit
SELECT raw_status, COUNT(*) FROM sec_filing_manifest
 WHERE source='sec_xbrl_facts' AND subject_id='0000320193' GROUP BY raw_status;
```

Cross-source check: Companyfacts JSON `https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json` — spot-check one concept (e.g. `us-gaap:Revenues`) against `SELECT * FROM financial_facts_raw WHERE concept='Revenues'` for AAPL. Cross-check `financial_periods.revenue` against [gurufocus.com/stock/AAPL/financials](https://www.gurufocus.com/stock/AAPL/financials).

## 12. Smoke test

`tests/smoke/test_etl_source_to_sink.py` parametrized row for `sec_xbrl_facts`. Asserts: `sec_companyfacts_ingest` module importable; `registered_parser_sources()` contains `sec_xbrl_facts`; Stages 9 + 25 exist in `_BOOTSTRAP_STAGE_SPECS`; `JOB_FUNDAMENTALS_SYNC` exists in `SCHEDULED_JOBS` at `scheduler.py:616`; `financial_facts_raw` partitions present; financials endpoint responds for AAPL.

## 13. Known gotchas

- **The manifest row IS the audit signal.** "No observation table at dispatch" is by design. The real ingest path (Companyfacts bulk JSON / per-CIK API) writes `financial_facts_raw` long before / independently of the manifest row's `parsed` transition.
- **XBRL period_end window [1900, 2100)** per #1218 parser-side guard at `_extract_facts_from_section`. Pre-#1218 in-the-wild data: 1 bad row in dev caught by cleanup script (XBRL parser garbage — filings dated 2023-2024 claiming period_end years 2031+). Cleanup predicate `period_end > filed_date + INTERVAL '5 years'` (sql/175).
- **DEFAULT partition cleanup baked into sql/175** (PR #1314 / `e0f583d`). Pre-sql/175 the DEFAULT partition caught 2031+ rows; defeats partition pruning + retention sweep targets. New quarterly partitions 2031-2040 restore both. Partitions now cover 2010-2040 quarterly (post sql/156 + sql/175 — 124 quarterly + pre2010 + default).
- **Partition extension deadline.** 2040-Q4 is the current tail; tickets for extension to 2050 tracked under #1221 follow-up.
- **`JOB_FUNDAMENTALS_SYNC` at `scheduler.py:616`, NOT `:562`.** Earlier docs cited `:562` — Codex 1 caught the drift.
- **Stream A PR-C2 (#1310) split the bootstrap entrypoint.** S25 dispatches `fundamentals_sync_bootstrap` (derivation-only, NO HTTP) at `app/services/fundamentals/bootstrap.py`, NOT the steady-state `fundamentals_sync` job. The job_name divergence (stage_key=fundamentals_sync vs job_name=fundamentals_sync_bootstrap) is what lets PR-C2's lane reassignment to `db_fundamentals_raw` coexist with the steady-state `ScheduledJob`'s `source="db"` registration (`bootstrap_orchestrator.py:1165-1174`).
- **Lane disjointness is operationally sufficient but not absolute** per `bootstrap.py:44-58`. Manual-override `fundamentals_sync` invocations (mark_request_completed bypass) could theoretically race the bootstrap entrypoint; data-layer writes are last-write-wins UPSERTs so the race is benign for correctness, only telemetry-confusing.
- **Bulk ingest fan-out per share-class sibling** (`sec_companyfacts_ingest.py:224`). One JSON entry per CIK fans out to every CIK-mapped instrument; GOOG + GOOGL both get the same row.
- **Caps applied at bulk extraction, not per-CIK API extraction** (`sec_companyfacts_ingest.py:84-90`). Per spec `docs/superpowers/specs/2026-05-19-data-retention-rubric.md §4.1`, bulk ingest is the canonical "must apply caps" path; the per-CIK API extractor stays uncapped for ad-hoc deep-dive use.
