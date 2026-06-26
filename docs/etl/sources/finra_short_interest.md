# finra_short_interest

**Class.** FINRA caller-owned.
**Form / endpoint.** Bimonthly Equity Short Interest CDN — `https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv`.

## 1. Origin

Anonymous CDN. Pipe-delim TEXT despite `.csv` extension. URL pattern at `app/providers/implementations/finra_short_interest.py:99`. Provider class `FinraShortInterestProvider` at `app/providers/implementations/finra_short_interest.py:62`. Fourteen pipe-delim columns; expected header pinned at `app/services/finra_short_interest_ingest.py:64`. Header-corruption mismatch is file-level fatal per `app/services/finra_short_interest_ingest.py:84`.

## 2. Watermarking model

No conditional-GET. Manifest is the watermark. Per-settlement-date file → one synthetic manifest accession `FINRA_SI_{YYYYMMDD}` UPSERTed at `app/services/finra_short_interest_ingest.py:375-420`. ScheduledJob skips manifest-parsed settlement dates EXCEPT the two most recent (revision window — FINRA publishes in-place `revisionFlag='Y'` corrections within 1-2 cycles per scheduler description at `app/workers/scheduler.py:1149-1152`).

## 3. Retry posture

Two HTTP statuses → `FinraNotFound` (benign skip, ScheduledJob re-fires next cron): `404` (not yet published) AND `403` (also not-yet-published on FINRA CDN — empirical, confirmed during #916 live-smoke). See `app/providers/implementations/finra_short_interest.py:119-123`. `5xx` raises `httpx.HTTPStatusError` after retry budget. Per-file failure isolated — partial success durable.

## 4. Bootstrap path

NOT a bootstrap stage. The bimonthly job does not seed under `_BOOTSTRAP_STAGE_SPECS` at `app/services/bootstrap_orchestrator.py:1035-1193`. Bootstrap-time backfill is operator-driven via the REPL runbook documented in the ScheduledJob description at `app/workers/scheduler.py:1160-1161` (`extended-window backfill (>400 days) via REPL runbook`). The 23 quarterly partitions in `sql/152_finra_short_interest.sql:74-94` cover 2021-Q3 through 2027-Q1 (exchange-listed cohort begins post-June 2021 per spec §4.2; pre-June 2021 OTC-only out of scope).

## 5. Steady-state path

`ScheduledJob(name=JOB_FINRA_SHORT_INTEREST_REFRESH, source="finra", cadence=Cadence.daily(hour=12, minute=0))` at `app/workers/scheduler.py:1141-1166`. Job-name constant at `app/workers/scheduler.py:338`. Daily 12:00 UTC. Prerequisite `_bootstrap_complete`. Lane is `finra` per JobLock buckets at `app/jobs/sources.py:74` + `app/jobs/sources.py:271`. Disjoint from `sec_rate`. Rate-limit budget: 1 req/s polite floor, shared cluster-wide via module-global throttle clock + lock at `app/providers/implementations/finra_short_interest.py:48-50`. **Shared with `finra_regsho_daily`** — the RegSHO daily sibling imports the same `_FINRA_RATE_LIMIT_CLOCK` + `_FINRA_RATE_LIMIT_LOCK` symbols (`app/providers/implementations/finra_regsho.py:37-42`) so combined bimonthly + daily fetch never exceeds 1 req/s.

## 6. Manifest insert

`sec_filing_manifest.source = 'finra_short_interest'`. Listed in `ManifestSource` Literal at `app/services/sec_manifest.py:120`. `subject_type='finra_universe'`, `subject_id='FINRA_SI'`, `cik='FINRA_SI'`, `form='SHRT'`, `accession_number='FINRA_SI_{YYYYMMDD}'`, `instrument_id=NULL` (the file aggregates across the entire FINRA universe). Direct UPSERT at `app/services/finra_short_interest_ingest.py:375-420` — NOT via `record_manifest_entry` + `transition_status` (the revision-window re-fetch path would raise `parsed → parsed` per `_ALLOWED_TRANSITIONS`; manual UPSERT keeps idempotent semantics). Companion `seed_freshness_for_manifest_row` call at `app/services/finra_short_interest_ingest.py:428-439` replicates the freshness-index seeding `record_manifest_entry` would have done internally (Codex 2 r1 HIGH 1). Option C `filed_at` gate is N/A — the manifest UPSERT lands `ingest_status='parsed'` directly inside the same caller-owned `with conn.transaction():`.

## 7. Parser

Synth no-op at `app/services/manifest_parsers/finra_short_interest.py`. Registered with `requires_raw_payload=False` at `app/services/manifest_parsers/finra_short_interest.py:100-105`. Pattern: ScheduledJob is the sole writer; manifest-worker dispatch exists ONLY to satisfy the dispatch invariant on the rare `sec_rebuild --source=finra_short_interest` path. The synth no-op marks the row `parsed` without touching FINRA or the DB. Architectural sibling: `sec_xbrl_facts` (G7) per `app/services/manifest_parsers/finra_short_interest.py:21-22`. PARSER_VERSION constant `finra-si-bimonthly-v1` unified across both the writer (`app/services/finra_short_interest_ingest.py:81`) and the synth no-op parser per Codex 1b r2 MED 3.

## 8. Observation insert

`finra_short_interest_observations` (partitioned by `settlement_date` quarterly, 23 partitions). Schema at `sql/152_finra_short_interest.sql:22-94`. PK `(instrument_id, settlement_date, source_document_id)` at `sql/152_finra_short_interest.sql:66`. `source TEXT CHECK (source = 'finra_si')` single-element CHECK locks the table (the short-form column value mirrors the manifest's long-form `finra_short_interest`). UPSERT at `app/services/finra_short_interest_ingest.py:242-304`. Symbol resolution: `normalise_symbol` (strip non-alnum + upper) at `app/services/finra_short_interest_ingest.py:105-111`. Bidirectional collapse — `BRK.A` ↔ `BRKA` both → `BRKA`. Preloaded resolver at `app/services/finra_short_interest_ingest.py:114-157` materialises all `(instrument_id, symbol)` from `instruments WHERE is_tradable = TRUE` (filtered to avoid delisted bloat per #1233 §6.2). Ambiguity (collision into multi-instrument key) → `skipped_ambiguous_symbol`. Tombstones not used — `known_from` only.

## 9. Current table refresh

`finra_short_interest_current` keyed `PRIMARY KEY (instrument_id)` at `sql/152_finra_short_interest.sql:107-125`. Refreshed INLINE by the same UPSERT loop at `app/services/finra_short_interest_ingest.py:306-359`. NOT a `refresh_*_current` MERGE writer (per #1255). The compound predicate `EXCLUDED.settlement_date > current.settlement_date OR (== AND NOW() > refreshed_at)` at `app/services/finra_short_interest_ingest.py:336-341` enforces settlement-date-wins with same-date-revision tie-break per spec §5.4. NOT in the `_CATEGORIES` 7-tuple at `app/jobs/ownership_observations_repair.py:69` — outside the ownership-decomposition repair sweep scope (short interest is NOT an ownership category).

## 10. Operator-visible endpoint

**Not yet wired.** No `/instruments/<symbol>/short-interest` or `/system/finra-short-interest` route exists under `app/api/`. Operator access today: SQL against `finra_short_interest_current` (`SELECT * FROM finra_short_interest_current WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'GME')`). Endpoint scaffold tracked under the broader operator-visibility backlog.

## 11. Verification queries

```sql
-- Most recent settlement for GME — sanity-check days-to-cover band.
SELECT settlement_date, current_short_interest, days_to_cover, market_class_code
  FROM finra_short_interest_current
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'GME');

-- Manifest health for the most recent two settlement dates (revision window).
SELECT accession_number, filed_at, ingest_status, parser_version
  FROM sec_filing_manifest
 WHERE source = 'finra_short_interest'
 ORDER BY filed_at DESC LIMIT 4;
```

Cross-source confirm: spot-check the latest GME days-to-cover figure against FINRA's published `https://www.finra.org/finra-data/short-interest/short-interest-equity-search` reporting UI for the same settlement date.

## 12. Smoke test

Import-time gate — `tests/smoke/test_etl_source_to_sink.py`, the per-source parametrized cases: `test_source_has_spec_file[finra_short_interest]`, `test_source_spec_has_required_sections[finra_short_interest]`, `test_manifest_source_has_registered_parser[finra_short_interest]`, `test_manifest_source_has_freshness_cadence[finra_short_interest]`, `test_manifest_source_has_sink_tables[finra_short_interest-spec*]` (synth-noop: asserts no sink tables + `_SYNTH_NOOP=True` parity). `test_manifest_source_form_mapping_present[finra_short_interest]` SKIPS — FINRA is in `FORM_MAPPING_EXEMPT` (not discovered via SEC form type).

Not covered by the import-time gate (verified by the live-smoke runbooks under `app/runbooks/`, not pytest): provider importable, the `JOB_FINRA_SHORT_INTEREST_REFRESH` ScheduledJob, PARSER_VERSION parity across the writer + synth-no-op modules, the `finra_short_interest_observations` / `_current` tables, and the operator-visible figure.

## 13. Known gotchas

1. **Caller-owned transaction.** `ingest_settlement_file` NEVER calls `conn.commit()` / `conn.rollback()` and DOES NOT enter its own `with conn.transaction():`. Caller MUST wrap the call site. See module docstring at `app/services/finra_short_interest_ingest.py:12-17`. The SAVEPOINT-vs-TOPLEVEL ambiguity is avoided by construction — the SERVICE emits SQL only into the caller's open transaction.
2. **Raw-payload-before-parse contract (#1168).** Caller MUST run `raw_filings.store_raw(...)` + `conn.commit()` BEFORE calling `ingest_settlement_file`. See `app/services/finra_short_interest_ingest.py:26-28`.
3. **`csv.DictReader` truncated rows present as dict with `None` values, not absent keys.** Per-row defect check at `app/services/finra_short_interest_ingest.py:218-223` explicitly tests `symbolCode` + `currentShortPositionQuantity` + `settlementDate` for blank/None and increments `skipped_invalid_row`.
4. **Source value short/long-form split.** Manifest enum uses `'finra_short_interest'`; observations CHECK uses `'finra_si'`. Mirrored in RegSHO sibling (`'finra_regsho_daily'` vs `'finra_regsho'`).
5. **Revision window.** Two most-recent settlement dates always re-fetched. FINRA can publish `revisionFlag='Y'` corrections in-place within 1-2 cycles. Re-ingest is idempotent — UPSERT on `(instrument_id, settlement_date, source_document_id)`.
6. **Shared rate budget with RegSHO daily.** Both modules reach the same `_FINRA_RATE_LIMIT_CLOCK` / `_FINRA_RATE_LIMIT_LOCK` symbols at `app/providers/implementations/finra_short_interest.py:48-50` (RegSHO imports them at `finra_regsho.py:37-42`). Combined fetch budget is 1 req/s total, not 1 req/s per source. Preserves prevention-log #726.
