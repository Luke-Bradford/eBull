# eBull data engineer — what we own, how we store it, how we read it

> Read this when adding a new ingest path, schema migration, or operator-visible read endpoint. The non-negotiable directive from the operator: future agents must use this file to answer "where does X come from?" without guessing. Cross-reference: `docs/settled-decisions.md` for the decisions; `docs/review-prevention-log.md` for the recurring traps; `.claude/skills/data-sources/sec-edgar.md` for the source-of-truth knowledge.

## 0.0 Before-spec gate — for any ETL / schema / parser / identity work

If you are about to write a spec or implementation that touches schema, parsers, ingest paths, or identity resolution, complete this gate FIRST. Codex catching what this skill should have owned is a skill defect (operator-locked 2026-05-10).

1. **Grep the actual shape, never paraphrase.** For every table you touch in writes:
   - PK columns (column order matters for ON CONFLICT inference).
   - Every UNIQUE / UNIQUE INDEX (full column list, in order).
   - Every FK in (children pointing AT this table) and FK out.
   - Every CHECK constraint on enum-shaped columns (allowed values).
   - Generated columns / triggers.
   The reference matrix in §11 is the in-skill shortcut for the load-bearing tables. If your table is not listed, run `psql \d <table>` or grep `sql/*.sql` and add it before writing the spec.
2. **Identify the canonical-pick pattern for any "single canonical row" lookup.** CIK→canonical ext-identifier row, observation-priority winner, manifest source mapping, etc. Reference matrix §12. Re-using existing patterns verbatim avoids drift.
3. **Identify FK cascade impact for any PK / UNIQUE relaxation.** A child FK keyed on the parent's old shape blocks the relaxation. The reference matrix §11.B lists every accession-keyed parent + its children. Check before proposing PK changes.
4. **Identify the migration shape-check pattern.** `pg_constraint.contype + conkey` and `pg_index.indisunique + indkey` introspection are the canonical idempotency primitives — name-only checks miss partial-applies. Reference §12.D.
5. **Confirm the actual writer ON CONFLICT syntax.** Never propose changing a conflict target without grepping every site that writes the table. The grep command:

   ```bash
   grep -rn "ON CONFLICT.*<table_or_constraint_name>" --include="*.py" --include="*.sql"
   ```

6. **Confirm the actual reader filter pattern per site.** A read site that filters on a column that becomes per-instrument fan-out will silently multiply rows. The grep command:

   ```bash
   grep -rn "FROM <table>\|JOIN <table>" --include="*.py" -A3 | grep "instrument_id"
   ```

If any step's answer disagrees with what your spec assumed, **fix the spec first, then run Codex**. The spec author owns this gate. Codex is the second opinion, not the first.

## 0. Top-of-mind invariants — the don't-break list

| # | Rule | Enforced by |
|---|---|---|
| I1 | API authn ≠ authz; instrument-scoped endpoints must validate operator/instrument scope | per-endpoint code; e.g. `app/api/instruments.py:34-47` (`require_session_or_service_token`) |
| I2 | All SQL is parameterised — `%(name)s` / positional, never f-strings for values | `docs/review-prevention-log.md:334` |
| I3 | Every parsed filing carries `parser_version` | `sec_filing_manifest.parser_version` (`sql/118:97-102`); keyed on accession not row |
| I4 | Every rollup slice carries `denominator_basis ∈ {pie_wedge, institution_subset}` | `OwnershipSlice.denominator_basis` (`app/services/ownership_rollup.py:68, 125`) |
| I5 | Funds slice excluded from residual + concentration math (memo overlay) | `_compute_residual` + `_compute_concentration` filters (`app/services/ownership_rollup.py:923-955`) |
| I6 | Soft-delete via tombstones, never hard-delete observations | `ownership_*_observations.known_to`, `*_ingest_log` rows, manifest `tombstoned` state |
| I7 | Atomic versioning for `_current` writes: `pg_advisory_xact_lock` per instrument; PK is second-line guard | `refresh_*_current()` functions in `app/services/ownership_observations.py` |
| I8 | Every observation write triggers `_current` refresh (write-through). Plus weekly backfill catches legacy gaps | call-site pattern in ingesters; backfill `JOB_OWNERSHIP_OBSERVATIONS_BACKFILL` (`app/workers/scheduler.py:710-735`) |
| I9 | `sec_filing_manifest` is source of truth for "is filing X on file?". Bumping `parser_version` flips rows back to `pending` for rewash | manifest state machine + `sec_rebuild` job |
| I10 | Identity resolution: instruments are `BIGINT instrument_id`; filers are `TEXT cik` (10-digit zero-padded). They never collapse | manifest `subject_type` CHECK; observations split by category |
| I11 | Coverage is telemetry, not a gate: red/amber/green banner state never blocks an endpoint | `_compute_coverage` / `_banner_for_state` (`app/services/ownership_rollup.py:966-1093`); `OwnershipRollup.no_data` returns 200 with empty payload |
| I12 | eToro broker is sole execution boundary; all order writes go via `app/services/order_client.py` and audit `decision_audit` | settled-decisions Provider strategy; `app/api/orders.py` |

Hard-rule corollaries:
- `INSERT INTO instruments` fixtures must supply `is_tradable` (prev-log L615).
- `_PLANNER_TABLES` in `tests/fixtures/ebull_test_db.py` must list every new FK-child table.
- Never close positions without explicit user action — `close_position()` only via UI or EXIT recommendation.
- Never `ON DELETE CASCADE` on `*_audit` / `*_log` (prev-log L350).

## 1. Schema layer — by domain

### 1.1 Identity & instrument core

| Table | Sql | Purpose |
|---|---|---|
| `instruments` | sql/001 | Canonical entity row. `instrument_id BIGINT`, symbol, company_name, exchange, currency, sector, `is_tradable` |
| `external_identifiers` | sql/003 | `(provider, identifier_type, identifier_value) → instrument_id`. Globally unique. SEC: `provider='sec', identifier_type='cik' / 'cusip'` |
| `exchanges` | sql/067 | eToro `exchangeId` → semantic class. CHECK on `asset_class IN (us_equity, crypto, eu_equity, uk_equity, asia_equity, commodity, fx, index, unknown)` |
| `instrument_sec_profile` | sql/051 | 1:1 SEC submissions metadata (cik, sic, former_names, has_insider_issuer). ON DELETE CASCADE from instruments |
| `instrument_cik_history` | sql/102 | CIK chain per instrument with date ranges. `btree_gist` exclusion forbids overlapping ranges; partial UNIQUE WHERE `effective_to IS NULL` |
| `instrument_symbol_history` | sql/103 | Symbol chain (FB→META, BBBY→BBBYQ). Same temporal-overlap GiST exclude |
| `unresolved_13f_cusips` | sql/099 + sql/164 | Buffer for SEC-supplied CUSIPs not yet resolved to instruments. **Two-writer split** (#1233 PR-1a): legacy per-filing path (`source IS NULL`, partial UNIQUE `..._legacy_idx`) carries `name_of_issuer` + `last_accession_number`; bulk-dataset path (`source IN ('bulk_13f_dataset','bulk_nport_dataset')`, partial UNIQUE `..._bulk_idx`) carries `filer_cik` + `period_end` instead and leaves issuer name NULL until the OpenFIGI sweep (PR-1b) fills it |

**No dedicated `cusip_map` table.** CUSIP→instrument lives in `external_identifiers WHERE provider IN ('sec', 'openfigi') AND identifier_type='cusip'` (post #1233 PR-1b — provider widened). Promotion paths:

* **Legacy per-filing** (#781 / #836): 13F-HR ingest writes `(cusip, name_of_issuer, accession)` via `institutional_holdings._record_unresolved_cusip` (`source=NULL`) → CUSIP backfill (#914 weekly) writes `external_identifiers` (provider='sec') → `cusip_resolver.sweep_resolvable_unresolved_cusips` rewashes the source 13F (`app/services/cusip_resolver.py:425+`).
* **Bulk dataset** (#1233 PR-1a): `sec_13f_dataset_ingest` / `sec_nport_dataset_ingest` capture `(cusip, filer_cik, period_end, source)` via `cusip_resolver.record_unresolved_cusip_from_bulk` for every unresolved CUSIP they encounter. Written under per-row savepoints, flushed every 1000 rows + at archive boundary.
* **Phase D / S13 OpenFIGI sweep** (#1233 PR-1b): `cusip_resolver.sweep_unresolved_cusips_via_openfigi` reads bulk-source rows whose CUSIP has no existing `external_identifiers` row, calls `OpenFigiResolver` (host `api.openfigi.com`, own Lane), and on success writes `external_identifiers (provider='openfigi', identifier_type='cusip', is_primary=FALSE)`. The bulk rows are tombstoned with `resolution_status='resolved_via_openfigi'`. Lane: `openfigi` (cap=1; per-instance rate limiter at `app/services/openfigi_resolver.py`). Tier-dependent budget — unkeyed 250 mappings/min; keyed 25k mappings/min via `OPENFIGI_API_KEY`. SD-1 cross-reference (`docs/settled-decisions.md`) gates programmatic use.

**Provider precedence in `_load_cusip_map`** (sec_13f_dataset_ingest + sec_nport_dataset_ingest + bootstrap_preconditions.compute_cusip_coverage): the SELECT filters `provider IN ('sec', 'openfigi')` and ORDERs by `is_primary DESC, external_identifier_id ASC`. The OpenFIGI sweep deliberately writes `is_primary=FALSE`, so a SEC `is_primary=TRUE` row WINS over an OpenFIGI fallback row for the same CUSIP. ``compute_cusip_coverage`` uses `COUNT(DISTINCT instrument_id)` so a SEC + OpenFIGI pair for the same instrument doesn't double-count.

**Coverage floor (`bootstrap_runs.coverage_floor_met`)** (#1233 PR-1b): post-S13 sweep, the invoker stamps `bootstrap_runs.coverage_floor_met = (coverage_ratio >= 0.80)`. NULL = sweep didn't run; TRUE = floor met; FALSE = below floor (still completes — informational only, admin panel renders an amber chip).

**Why two writers, not one?** The bulk-dataset path doesn't have issuer name on the per-holding row (the SEC FORM 13F INFOTABLE / FUND_REPORTED_HOLDING tables carry CUSIP + filer + period only). The legacy path's `_record_unresolved_cusip` REQUIRES `name_of_issuer + accession_number` (both NOT NULL in pre-#1233 schema). Splitting the writers — instead of relaxing the legacy signature — keeps the per-filing path's invariants intact and lets the bulk path use a separate partial UNIQUE that includes `(filer_cik, period_end, source)` so the same CUSIP can have multiple bulk rows (one per filer × period) without colliding.

**Identity-graph cheat sheet**:
- Instrument = `BIGINT instrument_id` (eToro-derived).
- CIK = 10-digit zero-padded TEXT (SEC).
- Mapping: `external_identifiers.provider='sec', identifier_type='cik', is_primary=TRUE` is canonical resolver. Historical CIKs in `instrument_cik_history`.
- 13F filer CIKs ≠ issuer CIKs. Live in `institutional_filers.cik` (sql/090).
- N-PORT trust CIKs ≠ 13F manager CIKs. Live in `sec_nport_filer_directory.cik` (sql/126). Disjoint from `institutional_filers`. Codex finding on #919: walking `institutional_filers` for N-PORT was the root cause of empty fund holdings.
- 13D/G primary filers in `blockholder_filers.cik` (sql/095). Per-row `reporter_cik` nullable (natural persons / family trusts).

### 1.2 Ownership observations + current (sql/113 → sql/127)

**Two-layer model** (#840 Phase 1):
- **Layer 1 — observations**: immutable append-only fact log, partitioned `RANGE(period_end)` quarterly 2010-2030 + `_default` partition.
- **Layer 2 — `_current`**: mutable materialised dedup snapshot, rebuilt by `refresh_<category>_current(instrument_id)`.

**Five-axis category split** (six tables of each shape):

| Category | Subject | observations | _current | Source(s) | Identity (_current) |
|---|---|---|---|---|---|
| insiders | issuer | `ownership_insiders_observations` (sql/113) | `ownership_insiders_current` | form4, form3 | `(instrument_id, holder_identity_key, ownership_nature)` |
| institutions | issuer | `ownership_institutions_observations` (sql/114) | `ownership_institutions_current` | 13f | `(instrument_id, filer_cik, ownership_nature, exposure_kind)` |
| blockholders | issuer | `ownership_blockholders_observations` (sql/115) | `ownership_blockholders_current` | 13d, 13g | `(instrument_id, reporter_cik, ownership_nature)` |
| treasury | issuer | `ownership_treasury_observations` (sql/116) | `ownership_treasury_current` | xbrl_dei | `(instrument_id)` |
| def14a | issuer | `ownership_def14a_observations` (sql/116) | `ownership_def14a_current` | def14a | `(instrument_id, holder_name_key, ownership_nature)` |
| funds | issuer (via fund_series) | `ownership_funds_observations` (sql/123) | `ownership_funds_current` | nport (CHECK pinned) | `(instrument_id, fund_series_id)` |
| esop | issuer | `ownership_esop_observations` (sql/127) | `ownership_esop_current` | def14a (CHECK pinned) | `(instrument_id, plan_name)` |

**Provenance block** (uniform across every `ownership_*_observations`):

| Column | Type | Meaning |
|---|---|---|
| source | TEXT | CHECK list: `form4, form3, 13d, 13g, def14a, 13f, nport, ncsr, xbrl_dei, 10k_note, finra_si, derived` |
| source_document_id | TEXT | Per-row identifier inside accession (e.g. `accession#row_num`) |
| source_accession | TEXT | SEC accession |
| source_field | TEXT | Free-form provenance (XPath, JSON pointer) |
| source_url | TEXT | Direct link |
| filed_at | TIMESTAMPTZ | When SEC stamped acceptance |
| period_start / period_end | DATE | Period coverage; period_end is partition key |
| known_from / known_to | TIMESTAMPTZ | Valid-time. NULL `known_to` = currently valid; non-NULL = soft-delete |
| ingest_run_id | UUID | Correlation id |
| ingested_at | TIMESTAMPTZ | System-time. DEFAULT `clock_timestamp()` so each row in batch gets distinct stamp |

**Generated identity keys handle NULL CIKs**:
- `holder_identity_key` (sql/113:49-53) = `'CIK:'||trim(cik)` if non-NULL, else `'NAME:'||lower(trim(holder_name))`. Stops legacy NULL-CIK rows from collapsing.
- `holder_name_key` (sql/116:110) = `lower(trim(holder_name))` for DEF 14A.

**Cross-column CHECK invariants**:
- Blockholders: submission_type ↔ status_flag (`13D|13D/A → active`, `13G|13G/A → passive`).
- Funds: `payoff_profile='Long'`, `asset_category='EC'`, `shares > 0`.
- ESOP: `source='def14a'`, `ownership_nature='beneficial'`, `shares > 0`.
- Treasury: `ownership_nature` defaults `'economic'`.
- Funds & N-PORT: `fund_series_id ~ '^S[0-9]{9}$'` regex CHECK.

**Partitioning**:
- All parents `PARTITION BY RANGE (period_end)` quarterly 2010-2030 + `_default`.
- `_default` MUST stay empty post-backfill — pinned by `tests/test_ownership_observations.py::TestProvenanceBlockUniformity::test_default_partition_is_empty_post_backfill`.
- `ALTER TABLE` on parent propagates to existing partitions on Postgres 14+.

**Residual-exclusion semantics** (`denominator_basis`):
- `pie_wedge` slices (insiders, blockholders, institutions, etfs, def14a_unmatched) sum to ≤ `shares_outstanding` and contribute to residual + concentration.
- `institution_subset` slices (today: funds; future: ESOP / DRS / short-interest) render as memo overlays. Their shares are NOT subtracted from residual nor counted toward concentration. N-PORT rows are fund-level detail INSIDE the 13F-HR institutional aggregate — additive accounting would double-count.

### 1.3 SEC reference / discovery / coverage tables

| Table | Sql | Purpose |
|---|---|---|
| `sec_filing_manifest` | sql/118 | Single source of truth for "is accession X on file?" PK `accession_number`. State machine `pending/fetched/parsed/tombstoned/failed`. `parser_version` rewash trigger. Self-FK `amends_accession`. CHECK enforces `subject_type='issuer' ⇔ instrument_id IS NOT NULL` |
| `data_freshness_index` | sql/120 | "Should I poll subject Y for source Z?" Subject-polymorphic. PK `(subject_type, subject_id, source)`. State `unknown/current/expected_filing_overdue/never_filed/error`. Cadence map at `app/services/data_freshness.py:69-100` |
| `sec_reference_documents` | sql/121 | Per-quarterly raw SEC ref docs (e.g. 13F Securities List). PK `(document_kind, period_year, period_quarter)` |
| `sec_fund_series` | sql/124 | series_id → name + filer_cik. PK `fund_series_id` with `~ '^S[0-9]{9}$'` |
| `n_port_ingest_log` | sql/125 | Per-accession N-PORT ingest tombstone. PK `accession_number`. Re-record overwrites prior attempt |
| `sec_nport_filer_directory` | sql/126 | RIC trust CIKs. PK `cik`. Populated by `sec_nport_filer_directory_sync`. Sibling of `institutional_filers` (#912). Disjoint universes |
| `institutional_filers` | sql/090 | 13F-HR filer CIKs (managers). UNIQUE `cik`. CHECK `filer_type ∈ ETF/INV/INS/BD/OTHER` |
| `institutional_holdings` | sql/090 | Legacy per-(accession, instrument, is_put_call) 13F holdings. Partial UNIQUE on `(accession_number, instrument_id, COALESCE(is_put_call,'EQUITY'))` |
| `blockholder_filers` / `blockholder_filings` | sql/095 | 13D/G filer registry + per-reporter rows |
| `def14a_beneficial_holdings` / `def14a_ingest_log` | sql/097 | DEF 14A bene-table holdings + per-accession tombstone. UNIQUE `(accession_number, holder_name)` |
| `insider_transactions` | sql/056 | Form 4 per-transaction rows. UNIQUE `(accession_number, txn_row_num)` |
| `insider_initial_holdings` | sql/093 | Form 3 initial-holdings snapshots. UNIQUE `(accession_number, row_num)` |
| `filing_raw_documents` | sql/107 | Per-accession raw body store |
| `cik_raw_documents` | sql/109 | Per-CIK raw documents (e.g. submissions.json snapshots) |
| `financial_facts_raw` | sql/032 | Individual XBRL facts from companyfacts. UNIQUE `(instrument_id, concept, unit, COALESCE(period_start,'0001-01-01'), period_end, accession_number)` |
| `financial_periods_raw` / `financial_periods` | sql/032 | Wide period rows + canonical one-per-period. Canonical PK `(instrument_id, period_end_date, period_type)`. `superseded_at` for restatement chain |
| `data_ingestion_runs` | sql/032 | Audit trail per provider batch |

**Read-side views**:
- `instrument_share_count_latest` — DEI > us-gaap precedence over `financial_facts_raw` (sql/052). Drives ownership rollup denominator.
- `share_count_history`, `instrument_dilution_summary`, `dividend_history`, `instrument_dividend_summary` (sql/050 / sql/052).
- `financial_periods_ttm` (sql/032) — TTM = SUM last 4 quarters of flow + latest-quarter stock. Powers `instrument_valuation`.
- `instrument_valuation` (sql/032) — P/E, P/B, EV/EBITDA, dividend yield, FCF yield, debt/equity. `priced` CTE picks best price from `quotes`.

### 1.4 Market data / quotes / candles

| Table | Sql | Purpose |
|---|---|---|
| `price_daily` | sql/001 | OHLCV per `(instrument_id, price_date)` + TA columns |
| `quotes` | sql/002 | 1:1 current snapshot per instrument; overwritten each refresh; LEFT JOIN safe |
| `intraday_candles` | (not persisted) | Provider pass-through (eToro REST + TTL cache) |

### 1.5 Operator auth / broker secrets / runtime

| Table | Sql | Purpose |
|---|---|---|
| `operators` | sql/016 | One row in v1; identity anchor. UUID `operator_id` |
| `operator_sessions` | sql/016 | Cookie sessions for operator UI |
| `broker_credentials` | sql/018 | Encrypted broker secrets per operator+provider+label+environment. Partial UNIQUE `(operator_id, provider, label) WHERE NOT revoked`. Health-state added in sql/128 |
| `broker_credentials_audit` | sql/018 | Append-only audit. ON DELETE SET NULL on credential_id (forensic preservation) |
| `runtime_config` | sql/015 | DB-backed feature flags + kill_switch. Distinct from deployment config |
| `pending_job_requests` | sql/084 | Durable trigger queue. State `pending → claimed → dispatched → completed`. pg_notify is wakeup hint |
| `job_runs` | sql/014 | Per-run audit. Status `running/success/failure/skipped` |
| `bootstrap_state` | sql/129 | Singleton (id=1) — first-install gate. `pending/running/complete/partial_error` |
| `bootstrap_runs` | sql/129 | Per "Run bootstrap" click. Unique partial index forbids two concurrent `running` rows |
| `bootstrap_stages` | sql/129 | Per-stage detail; lane ∈ init/etoro/sec; status ∈ pending/running/success/error/skipped |
| `bootstrap_archive_results` | sql/130 | Per-(run, stage_key, archive_name) audit |
| `decision_audit` | sql/001 | One row per guard invocation; per-rule results in `evidence_json` |
| `tax_lots` | sql/001 | Tax lot ledger; matched fills via `reference_fill_id` |
| `cash_ledger` | sql/001 | `amount` sign: positive=inflow, negative=outflow |

### 1.6 Tombstone / soft-delete inventory

- `ownership_*_observations.known_to` — sets to NOW() on retraction.
- `ownership_*_observations` `_default` partition — itself a tombstone for unexpected `period_end`.
- `sec_filing_manifest.ingest_status='tombstoned'` (terminal under normal flow).
- `def14a_ingest_log`, `n_port_ingest_log`, `institutional_holdings_ingest_log`, `blockholder_filings_ingest_log` — per-accession attempted tombstones.
- `unresolved_13f_cusips.resolution_status` ∈ unresolvable/ambiguous/conflict/manual_review/resolved_via_extid.
- `broker_credentials.revoked_at`.
- `financial_periods.superseded_at` (restatement chain).

ON DELETE CASCADE used on: `instruments → instrument_sec_profile / sec_filing_manifest / data_freshness_index / instrument_*_history / ownership_esop_observations`. ON DELETE SET NULL used on `broker_credentials_audit.credential_id`.

## 2. Service layer

Service modules under `app/services/` (~96 modules). Canonical entry points by domain:

### 2.1 Ownership domain

| Module | Role | Entry points |
|---|---|---|
| [app/services/ownership_observations.py](../../../app/services/ownership_observations.py) | Two-layer write side: `record_*_observation()` + `refresh_*_current()` | record_insider:110, record_institution:292, record_blockholder:483, record_treasury:631, record_def14a:747, record_fund:913, record_esop:1104; refresh_*_current at 181/390/568/689/843/1040/1194 |
| [app/services/ownership_observations_sync.py](../../../app/services/ownership_observations_sync.py) | Legacy → observations backfill (`sync_all`); `JOB_OWNERSHIP_OBSERVATIONS_BACKFILL` weekly Sun 03:00 UTC | |
| [app/services/ownership_rollup.py](../../../app/services/ownership_rollup.py) | **Canonical Tier 0 read service**: `get_ownership_rollup(conn, symbol, instrument_id) -> OwnershipRollup` | line 1222 |
| [app/services/ownership_history.py](../../../app/services/ownership_history.py) | Time-series payload for chart | |
| [app/services/ownership_drillthrough.py](../../../app/services/ownership_drillthrough.py) | Per-slice drill detail | |
| [app/services/holder_name_resolver.py](../../../app/services/holder_name_resolver.py) | DEF 14A holder_name → filer_cik via `external_identifiers` | called from rollup `_enrich_and_union_def14a` |
| [app/services/cusip_resolver.py](../../../app/services/cusip_resolver.py) | Sweep `unresolved_13f_cusips` → promote to `external_identifiers` | `MATCH_THRESHOLD=0.92` |
| [app/services/rewash_filings.py](../../../app/services/rewash_filings.py) | Re-parse a previously-parsed filing under updated parser | called by manifest rebuild |
| [app/services/sec_manifest.py](../../../app/services/sec_manifest.py) | Manifest record + state-machine helpers | `record_manifest_entry`, `transition_status`, `iter_pending`, `iter_retryable` |
| [app/services/data_freshness.py](../../../app/services/data_freshness.py) | Scheduler API: `seed_scheduler_from_manifest`, `record_poll_outcome`, `subjects_due_for_poll` | |

### 2.2 Identity / discovery

`sec_13f_filer_directory.py`, `sec_nport_filer_directory.py`, `sec_13f_securities_list.py`, `sec_entity_profile.py`, `cusip_resolver.py`, `holder_name_resolver.py`, `instrument_history.py`, `cik_raw_filings.py`, `filer_seed_verification.py`. (CIK discovery now lives in the canonical `daily_cik_refresh` scheduled job + `app/services/filings.py::upsert_cik_mapping`; the legacy `cik_discovery.py` helper was deleted in #1091.)

### 2.3 SEC / filings ingest

`sec_companyfacts_ingest.py`, `sec_insider_dataset_ingest.py`, `sec_13f_dataset_ingest.py`, `sec_nport_dataset_ingest.py`, `sec_submissions_ingest.py`, `sec_submissions_files_walk.py`, `sec_pipelined_fetcher.py`, `sec_bulk_download.py`, `sec_bulk_orchestrator_jobs.py`, `sec_filing_items.py`, `raw_filings.py`, `raw_persistence.py`, `filing_documents.py`, `eight_k_events.py`, `business_summary.py`, `dilution.py`.

### 2.4 Market data / etoro

`market_data.py`, `intraday_candles.py`, `quote_stream.py`, `etoro_lookups.py`, `etoro_websocket.py`, `exchanges.py`, `fx.py`.

### 2.5 Portfolio / execution / reporting

`portfolio.py`, `portfolio_sync.py`, `position_monitor.py`, `order_client.py`, `execution_guard.py`, `return_attribution.py`, `reporting.py`, `budget.py`, `transaction_cost.py`, `tax_ledger.py`.

### 2.6 Coverage / fundamentals

`coverage.py`, `fundamentals.py`, `fundamentals_observability.py`, `dividends.py`, `dividend_calendar.py`, `scoring.py`, `technical_analysis.py`, `entry_timing.py`, `thesis.py`, `news.py`, `sentiment.py`, `xbrl_derived_stats.py`.

### 2.7 Operator + auth + ops

`operators.py`, `operator_setup.py`, `broker_credentials.py`, `credential_health.py`, `credential_health_cache.py`, `runtime_config.py`, `ops_monitor.py`, `bootstrap_orchestrator.py`, `bootstrap_preconditions.py`, `bootstrap_state.py`.

### 2.8 Sync orchestrator

`sync_orchestrator/{adapters,cascade,content_predicates,dispatcher,exception_classifier,executor,freshness,layer_failure_history,layer_state,layer_types,planner,progress,reaper,registry,row_count_spikes,types}.py` — DAG-driven sync replacing 12 legacy crons (#260; merged 2026-04-16/17). `dispatcher.publish_manual_job_request` is the API → jobs-process boundary.

### 2.9 Read paths — canonical example

Ownership rollup read flow:
```
GET /instruments/{symbol}/ownership-rollup
  └─ app/api/instruments.py:4069
       resolve symbol → instrument_id (instruments + is_primary_listing tiebreaker)
       with snapshot_read(conn):                    # REPEATABLE READ snapshot
         get_ownership_rollup(conn, symbol, instrument_id)
            ├─ _read_shares_outstanding             # instrument_share_count_latest VIEW
            ├─ historical_symbols_for(conn, instrument_id)
            ├─ _read_treasury_from_current
            ├─ _collect_canonical_holders_from_current
            │      ├─ ownership_insiders_current  (form4/form3)
            │      ├─ ownership_blockholders_current  (13d/13g)
            │      └─ ownership_institutions_current  (13f, exposure_kind='EQUITY')
            ├─ _read_def14a_unmatched_from_current
            ├─ _enrich_and_union_def14a (resolve_holder_to_filer)
            ├─ _collect_funds_from_current → ownership_funds_current
            ├─ partition into block-only vs other (separate-pool dedup, #837)
            ├─ _dedup_by_priority(other_candidates) (form4>form3>13d/g>def14a>13f>nport)
            ├─ _dedup_within_source(block_candidates) (latest amendment per CIK)
            ├─ _bucket_into_slices                  # insiders, blockholders, institutions, etfs, def14a_unmatched, funds(memo)
            ├─ _compute_residual                    # pie_wedge slices only
            ├─ _compute_concentration               # pie_wedge slices only
            ├─ _read_universe_estimates             # Tier 0: all NULL → banner=unknown_universe
            ├─ _compute_coverage / _banner_for_state
            └─ return OwnershipRollup
       _rollup_to_response(rollup) → Pydantic OwnershipRollupResponse
```

**No-data path returns 200 with empty slices + red banner — never 503** (`OwnershipRollup.no_data` at line 207).

### 2.10 Write paths — canonical examples

**13F-HR ingest (institutions)**:
```
sec_13f_quarterly_sweep job (Sat 02:00 UTC)
  → sec_13f_dataset_ingest.ingest_filer_13f
      → for each accession:
           parse XML
           upsert institutional_filers
           per holding:
             resolve CUSIP → instrument_id (external_identifiers)
                ├─ resolved → INSERT institutional_holdings (legacy)
                │             record_institution_observation (write-through)
                │             refresh_institutions_current(instrument_id)
                └─ unresolved → unresolved_13f_cusips upsert
           record manifest row (sec_filing_manifest, ingest_status=parsed)
```

**DEF 14A ingest**:
```
sec_def14a_ingest job (daily 04:35 UTC) | sec_def14a_bootstrap (weekly Sun 02:30)
  → def14a_ingest._record_def14a_observations_for_filing
      → record_def14a_observation (also routes ESOP names via record_esop_observation)
      → refresh_def14a_current(instrument_id) + refresh_esop_current(instrument_id)
      → def14a_ingest_log row
```

**N-PORT ingest**:
```
sec_n_port_ingest (monthly day 22 03:00 UTC)
  → n_port_ingest.ingest_n_port_for_filer
      → walk sec_nport_filer_directory CIKs
      → for each accession not in n_port_ingest_log:
           parse NPORT-P (stdlib ElementTree)
           equity-common-Long filter
           upsert sec_fund_series
           record_fund_observation (write-through)
           refresh_funds_current(instrument_id)
           n_port_ingest_log row
```

**Write-through is wired in ingester service modules. There is no trigger-based write-through** — Postgres TRIGGERs only touch `updated_at` on manifest + freshness. Any new ingest path **must explicitly call** both `record_<cat>_observation` AND `refresh_<cat>_current(instrument_id)` plus update manifest. Forgetting either leaves `_current` empty (prev-log L1162).

**Sync vs repair-sweep category asymmetry (Run-#8-readiness fixes / Architect lens REBUTTED by Codex)**:

The legacy-mirror dispatcher `sync_all` at `app/services/ownership_observations_sync.py:797` covers **5 categories** (insiders, institutions, blockholders, treasury, def14a). The daily 03:30 UTC drift-repair sweep at `app/jobs/ownership_observations_repair._CATEGORIES` covers **7 categories** (the 5 + funds + esop). This asymmetry is **by-design**, NOT a bug:

- **Funds** has no legacy mirror source — fund holdings land via NPORT manifest-worker write-through (`sec_n_port.py` parser → `refresh_funds_current`) and the bulk-dataset path. The daily sweep is funds' ONLY reconciliation path.
- **ESOP** rows ARE processed by `sync_all` transitively inside `sync_def14a` (lines 691-769) — DEF 14A bene-table rows flagged as ESOP route into `ownership_esop_observations`. ESOP has its own daily reconciliation entry in `_CATEGORIES` in addition to the transitive sync.

DO NOT "fix" the asymmetry by adding `sync_funds`/`sync_esop` — funds has no legacy source to read from; esop is already covered by `sync_def14a`. Both have explicit `tests/.../test_*sync_all_category_invariants.py` pinning the 5-vs-7 shape.

**What write-through does NOT update**:
- Legacy typed tables (`institutional_holdings`, `def14a_beneficial_holdings`, `insider_transactions`, `insider_initial_holdings`, `blockholder_filings`) — still written by ingesters, NOT read by rollup post-#905. Survive for chart history + drift detection.
- `data_freshness_index` — separate write-side via `record_poll_outcome`.
- `sec_filing_manifest.parser_version` and `raw_status` — bumped by manifest worker / parser, not by `record_*_observation`.

**Diff-aware MERGE replaces DELETE+INSERT in `refresh_*_current` helpers (#1233 PR12, SHIPPED 2026-05-22)**:

Every `refresh_X_current(conn, *, instrument_id)` helper (7 of them: insiders / institutions / blockholders / treasury / def14a / funds / esop) uses a single-statement PG17 `MERGE … WHEN NOT MATCHED BY SOURCE`. Template:

```
WITH watermark captured pre-MERGE in Python:
  cur.execute("SELECT MAX(ingested_at) FROM ownership_X_observations WHERE instrument_id = %s", (iid,))
  watermark = cur.fetchone()[0]

MERGE INTO ownership_X_current AS tgt
USING (SELECT DISTINCT ON (<pk_cols>) ... FROM ownership_X_observations WHERE instrument_id = %(iid)s AND known_to IS NULL ORDER BY ...) AS src
ON tgt.instrument_id = %(iid)s AND tgt.<pk> = src.<pk>   -- scope clamp on ON
WHEN MATCHED AND (business_cols) IS DISTINCT FROM (excluded.cols) THEN UPDATE SET ..., refreshed_at = now()
WHEN NOT MATCHED BY TARGET THEN INSERT (...) VALUES (...)   -- refreshed_at omitted; DEFAULT now() fires
WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE   -- scope clamp on DELETE

then UPSERT ownership_refresh_state(instrument_id, category, last_drained_observations_max_ingested_at, last_refresh_attempted_at) VALUES (iid, '<cat>', watermark, now())
```

Hard invariants pinned by `scripts/check_ownership_refresh_writer_pattern.sh` (93 clause-counts) + `tests/test_ownership_refresh_writer_merge.py` (52 parametrised cases):

- `refreshed_at` is NEVER in the `IS DISTINCT FROM` diff predicate (would always re-fire UPDATE → bloat returns). It lives only in the UPDATE SET path; INSERT lets DEFAULT now() fire.
- Scope clamp `tgt.instrument_id = %(iid)s` appears in BOTH the ON clause AND the NOT MATCHED BY SOURCE clause. **Exception: `ownership_treasury_current`** (single-col PK on instrument_id) — PG MERGE compiles NOT MATCHED BY SOURCE to FULL OUTER JOIN which requires an equi-join condition, so treasury's ON uses `tgt.instrument_id = src.instrument_id` and the const-clamp lives in the USING subquery's WHERE. The DELETE-clause clamp is preserved as defence-in-depth on all 7 helpers.
- Drift watermark for the repair-sweep lives in `ownership_refresh_state(instrument_id, category, last_drained_observations_max_ingested_at, last_refresh_attempted_at)` — separated from `_current.refreshed_at` so no-op MERGE calls do not freeze the watermark (would otherwise re-select the same instrument every sweep tick).
- Watermark captured PRE-MERGE in a Python variable (race-safe: prevents the post-MERGE UPSERT advancing past observations the MERGE did not see if new obs land between SELECT and MERGE).
- Repair sweep (`app/jobs/ownership_observations_repair.py`) uses an obs-anchored CTE aggregate:

```sql
WITH obs_max AS (SELECT instrument_id, MAX(ingested_at) AS m FROM ownership_X_observations GROUP BY instrument_id)
SELECT s.instrument_id FROM ownership_refresh_state s
LEFT JOIN obs_max ON obs_max.instrument_id = s.instrument_id
WHERE s.category = '<cat>'
  AND s.last_drained_observations_max_ingested_at IS DISTINCT FROM obs_max.m
```

`_CATEGORIES` is 7 entries (was 5 pre-PR12 — funds + esop added). PG ≥ 17 is asserted at lifespan startup (`app/system/postgres_version_guard.py`).

Spec: `docs/_archive/2026-05/2026-05-21-pr12-ownership-current-writer-merge.md`. Prevention-log entries: "MERGE WHEN NOT MATCHED BY SOURCE must carry the per-scope clamp on BOTH the ON clause AND the DELETE clause" + "Diff-aware writers must NOT include update-timestamp columns in the diff predicate".

**Lint contract (#1256)**: UPDATE SET / diff-tuple column shape is enforced by `scripts/_check_ownership_writer_columns.py` (5-axis invariant I a-e). One col per line in UPDATE SET; comma-separated `prefix.col` tokens per diff-tuple line; no inline comments in either span. The shell wrapper invokes the Python helper per-function for all 10 helpers (7 single + 3 batch).

**Batched form for post-ingest hot-paths (#1233 PR-4, SHIPPED 2026-05-23)**:

Three of the seven helpers also expose a batched form:

- `refresh_insiders_current_batch(conn, *, instrument_ids)`
- `refresh_institutions_current_batch(conn, *, instrument_ids)`
- `refresh_funds_current_batch(conn, *, instrument_ids)`

Used by `sec_bulk_orchestrator_jobs.py` after each bulk ingest. Collapses the per-instrument round-trip + lock + MERGE into one of each, scoped to the batch. Saves 5–10 min wall-clock per category at peak (~10k touched instruments × 50 ms each).

Hard invariants pinned by `tests/test_ownership_observations_refresh_batch.py` (21 cases = 7 × 3 helpers):

- **Deadlock-safe lock ordering**: the batched helper acquires ALL advisory locks for its batch in a single server-side query sorted by hashed lock key (`hashtextextended('refresh_X_current', 0) # iid::bigint`). NOT by raw `instrument_id` — the hash is not monotonic in `iid`, so two parallel callers seeing the same set in raw-int order could deadlock. Hash-key order makes both callers queue identically.
- **DISTINCT ON / ORDER BY lead column**: batched USING subquery prepends `instrument_id` to both the DISTINCT ON tuple and the ORDER BY list so per-instrument partitioning matches the single-instrument helper exactly.
- **ON clause**: drops the const clamp `tgt.instrument_id = %(iid)s` (no single iid in scope); uses `tgt.instrument_id = src.instrument_id` from MERGE's natural per-row matching.
- **NOT MATCHED BY SOURCE clamp**: `tgt.instrument_id = ANY(%(ids)s::bigint[])` replaces the const clamp — the load-bearing scope guard against cross-instrument cartesian DELETE.
- **Empty input**: no-op (returns 0, dispatches no SQL).
- **Idempotency**: identical to single-instrument helper — IS DISTINCT FROM diff predicate skips UPDATE on no-op; xmin stays stable; row count unchanged.
- **Caller normalisation**: `_normalise_instrument_ids` dedupes + sorts the input. A caller passing `[3, 1, 2, 1, 3]` behaves the same as `[1, 2, 3]`.
- **Watermark UPSERT**: one `ownership_refresh_state` row per instrument in the batch, written via `executemany` — same shape as the single-instrument helper's single INSERT.

Call-site pattern (in `sec_bulk_orchestrator_jobs.py`, mirrored across 13F / insider / N-PORT):

```python
_REFRESH_BATCH_CHUNK_SIZE: Final[int] = 200

for chunk_start in range(0, len(sorted_ids), _REFRESH_BATCH_CHUNK_SIZE):
    if bootstrap_cancel_requested():
        raise BootstrapStageCancelled(...)
    chunk = sorted_ids[chunk_start : chunk_start + _REFRESH_BATCH_CHUNK_SIZE]
    with conn.transaction():
        refresh_X_current_batch(conn, instrument_ids=chunk)
```

Cancel observation latency degrades from ~50 instruments (old serial loop) to ~`_REFRESH_BATCH_CHUNK_SIZE` (=200) instruments. Documented trade-off, accepted in spec §8.

The four other helpers (blockholders / treasury / def14a / esop) keep the single-instrument path only — nothing in the bulk orchestrator loops over them at scale, so the batched form would be code without a caller.
### 2.10b Bulk-archive ingest pattern — per-archive TEMP + COPY + ON CONFLICT INSERT (#1233 PR-3, SHIPPED 2026-05-22)

For multi-million-row SEC bulk dataset archives (13F INFOTABLE.tsv, N-PORT FUND_REPORTED_HOLDING.tsv, Insider NONDERIV_TRANS.tsv) the per-row `INSERT ... ON CONFLICT DO UPDATE` + `with conn.transaction()` SAVEPOINT pattern caps throughput at ~1500 rows/s. Bulk dataset ingesters (`app/services/sec_{13f,nport,insider}_dataset_ingest.py`) MUST use the per-archive lifecycle below. Lint guard: `scripts/check_bulk_ingest_copy_pattern.sh`.

```
# Per archive — orchestrator opens fresh conn, ingester runs, orchestrator commits.
with conn.cursor() as cur:
    cur.execute("""
        CREATE TEMP TABLE _stg_<category> (
            ...same columns as target observation table, sans GENERATED STORED cols...
        ) ON COMMIT DROP
    """)

# Stream archive → pre-validate → COPY into staging.
copy_sql = (
    "COPY _stg_<category> (col, col, ...) FROM STDIN "
    "WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)"
)
with conn.cursor() as cur, cur.copy(copy_sql) as copy:
    for row in _iter_tsv(zf, ...):
        # ...existing per-row Python gates: CUSIP map, retention, PRN-vs-SH, etc.
        copy.write_row((..., ...))

# Drain staging → target with DISTINCT ON dedupe + ON CONFLICT UPSERT.
cur.execute("""
    INSERT INTO ownership_<category>_observations (...)
    SELECT DISTINCT ON (<conflict_key_cols>)
        ...
    FROM _stg_<category>
    ORDER BY <conflict_key_cols>, ctid DESC
    ON CONFLICT (<conflict_key_cols>) DO UPDATE SET ...
""")

# Orchestrator commits → _stg_<category> drops via ON COMMIT DROP.
```

**Hard invariants** (pinned by `scripts/check_bulk_ingest_copy_pattern.sh`):

- **A. `cur.copy(` present** in every whitelisted ingester. Streams rows into staging via psycopg's COPY context.
- **B. `CREATE TEMP TABLE _stg_<category>` ON COMMIT DROP** — the TEMP table lifecycle MUST be bounded by the per-archive transaction commit. Hoisting `CREATE TEMP TABLE` outside the archive loop would make the first commit drop it and subsequent iterations fail with "relation does not exist".
- **C. NO `with conn.transaction()` AFTER the first `cur.copy(` call site** — the per-row SAVEPOINT pattern eliminated by PR-3. Per-archive series upserts (NPORT) belong in a pre-pass BEFORE the COPY context opens.
- **D. Drain via `INSERT INTO ownership_<category>_observations` + `ON CONFLICT` + `FROM _stg_<category>`** with DISTINCT ON to dedupe staged rows that share a conflict key (otherwise PG raises `cardinality_violation`).

**PG17 ON_ERROR ignore**: `COPY ... WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)` skips wire-level type-cast failures (NUMERIC overflow, bad timestamps) and emits NOTICEs — defence-in-depth against schema drift, NOT a substitute for Python pre-validation. Each skip increments `result.rows_skipped_bad_data` (derived from `copy_attempted - SELECT COUNT(*) FROM _stg`).

**DISTINCT ON dedupe**: two staging rows sharing a conflict key trigger `cardinality_violation: ON CONFLICT DO UPDATE command cannot affect row a second time`. The per-row INSERT path would have sequentially UPDATEd; the bulk path keeps that "last write wins" semantic via `ORDER BY <conflict_key>, ctid DESC` (the implicit physical row order — COPY appends so ctid grows monotonically).

**GENERATED STORED column handling**: `ownership_insiders_observations.holder_identity_key` is GENERATED ALWAYS — the COPY column list omits it, and the INSERT...SELECT into the target re-derives it on insert (PG materialises the generated value before consulting the unique index). The DISTINCT ON expression in the bulk drain materialises the same `CASE WHEN holder_cik IS NOT NULL ... END` formula the target uses.

**Cancel observation cost**: per-row INSERT used to checkpoint operator-cancel at sub-second latency. COPY drains atomically per archive (10-60s on multi-million-row archives). Operator cancel is observed at the archive boundary; the trade-off is documented in spec §7 and accepted (the throughput gain is ~30× for first-install bootstrap, which is the dominant cost driver).

**Companyfacts is NOT on this pattern** — `sec_companyfacts_ingest.py` reads XBRL JSON not TSV and already uses multi-row INSERT chunked at `_UPSERT_PAGE_SIZE=1000` via `upsert_facts_for_instrument`. The per-CIK savepoint there is intentional (one savepoint per CIK-payload, NOT per-row); it stays.

Spec: `docs/_archive/2026-05/superseded-bootstrap-etl-optimisation-v2.md` §7. Tests: `tests/test_pr3_copy_refactor.py` (throughput floor, ON_ERROR skip, commit-boundary preservation, idempotency, touched-instrument set).

### 2.11 Worker / job entry points

[app/workers/scheduler.py:453](../../../app/workers/scheduler.py#L453) — `SCHEDULED_JOBS` declaration. [app/jobs/](../../../app/jobs/) package owns runtime side:
- `__main__.py` boots singleton-fenced jobs process (advisory lock, `JOBS_PROCESS_LOCK_KEY`).
- `runtime.py` invokes.
- `listener.py` consumes `pending_job_requests` + pg_notify.

SEC-specific jobs:
- `sec_atom_fast_lane.py` — Atom feed (5 min cadence — fastest discovery layer).
- `sec_daily_index_reconcile.py` — daily-index reconciliation.
- `sec_per_cik_poll.py` — per-CIK submissions.json poller (cadence-driven from `data_freshness_index`).
- `sec_first_install_drain.py` — first-install drain (#871).
- `sec_manifest_worker.py` — pulls `pending` + `failed AND next_retry_at<=NOW()` from manifest, dispatches to parsers. **Fairness contract (#1179)**: the unscoped tick (`source=None`) allocates a per-source quota via `compute_quotas(sources, max_rows, tick_id)` (Phase A), then tops up residual budget against the global oldest tail (`iter_pending_topup` / `iter_retryable_topup` — Phase B). Both top-up queries are scoped to `registered_parser_sources()` only — `sec_xbrl_facts` / `finra_short_interest` rows never reach the dispatch loop via the unscoped path. `tick_id` advances by +1 per tick (module-global `itertools.count(0)` for production; tests inject explicitly) so rotation visits every source within `n - remainder + 1` ticks regardless of scheduler cadence. The per-source rebuild path (`source='sec_form4'` etc.) is unchanged — full `max_rows` budget consumed by the requested source.
- `sec_rebuild.py` — `POST /jobs/sec_rebuild/run` (operator-triggered targeted re-ingest).
- `ownership_observations_repair.py` — daily 03:30 UTC drift sweep.

**Process topology** (settled-decisions §"Process topology #719"):
- FastAPI process (`app.main`): HTTP only.
- Jobs process (`python -m app.jobs`): APScheduler + manual-trigger executor + reaper + queue dispatcher + heartbeat.
- IPC: Postgres only — `pending_job_requests` rows + `pg_notify('ebull_job_request', ...)`. No HTTP/Redis/shared memory.
- Both processes use `app/db/pool.open_pool` (`sql_pool` config). Never instantiate raw `ConnectionPool(...)`.

## 3. Operator-visible API surface

### 3.1 `/instruments/...` endpoints ([app/api/instruments.py](../../../app/api/instruments.py))

| Endpoint | Tables / services |
|---|---|
| `GET /instruments` | instruments + quotes + coverage + external_identifiers |
| `GET /instruments/{symbol}/financials` | financial_periods + financial_periods_ttm + instrument_valuation |
| `GET /instruments/{symbol}/candles` | price_daily |
| `GET /instruments/{symbol}/intraday-candles` | provider pass-through (eToro REST + TTL cache) |
| `GET /instruments/{symbol}/sec_profile` | instrument_sec_profile |
| `GET /instruments/{symbol}/employees` | financial_facts_raw (`dei:EntityNumberOfEmployees`) |
| `GET /instruments/{symbol}/eight_k_filings` | eight_k_filings + eight_k_items + eight_k_exhibits |
| `GET /instruments/{symbol}/business_sections` | instrument_business_summary_sections |
| `GET /instruments/{symbol}/filings/10-k/history` | filing_events + instrument_business_summary |
| `GET /instruments/{symbol}/dilution` | instrument_dilution_summary view |
| `GET /instruments/{symbol}/dividends` | dividend_history view + dividend_events |
| `GET /instruments/{symbol}/insider_summary` | insider_transactions aggregated |
| `GET /instruments/{symbol}/insider_transactions` | insider_transactions |
| `GET /instruments/{symbol}/insider_baseline` | insider_initial_holdings + insider_transactions cumulative |
| `GET /instruments/{symbol}/def14a_holdings/drill` | def14a_beneficial_holdings + drift detection |
| `GET /instruments/{symbol}/summary` | composite summary panel |
| `GET /instruments/{symbol}/institutional-holdings` | institutional_holdings + institutional_filers |
| `GET /instruments/{symbol}/blockholders` | blockholder_filings + chain aggregator |
| `GET /instruments/{symbol}/ownership-history` | ownership_history service (time series) |
| `GET /instruments/{symbol}/ownership-rollup` | **Tier 0 canonical read** — `ownership_rollup.get_ownership_rollup` |
| `GET /instruments/{symbol}/ownership-rollup/export.csv` | `build_rollup_csv` |

### 3.2 `/system/...` ([app/api/system.py](../../../app/api/system.py))

- `GET /system/status` — operator dashboard. Layer freshness via `check_all_layers`, job health via `check_job_health`, kill-switch via `get_kill_switch_status`. Returns `overall_status ∈ ok/degraded/down`. **503 on infra failure** (never 200).
- `GET /system/jobs` — declared schedule + computed `next_run_time` + most recent `job_runs` row.

Auth: router-level `require_session_or_service_token`. Reveals data-pipeline gaps so must not be public.

### 3.3 `/jobs/...` ([app/api/jobs.py](../../../app/api/jobs.py))

- `POST /jobs/{job_name}/run` — durable-queue manual trigger. INSERTs `pending_job_requests` + `pg_notify`. 202 on accept; 404 on unknown job_name.
- `GET /jobs/runs` — recent `job_runs`.
- `GET /jobs/requests` — recent `pending_job_requests`.

Notable triggers: `POST /jobs/sec_rebuild/run`, `POST /jobs/ownership_observations_backfill/run`, `POST /jobs/sec_def14a_bootstrap/run`, `POST /jobs/sec_business_summary_bootstrap/run`, `POST /jobs/cusip_universe_backfill/run`.

### 3.4 Other operator routers

`/sync` (sync_orchestrator), `/system/bootstrap`, `/auth/*`, `/broker-credentials/*`, `/operators/*`, `/coverage/*`, `/recommendations/*`, `/orders/*`, `/portfolio/*`, `/scores/*`, `/theses/*`, `/audit/*`, `/news/*`, `/filings/*`, `/watchlist/*`, `/budget/*`, `/copy_trading/*`, `/alerts/*`, `/reports/*`, `/attribution/*`, `/config/*`. Operator ingest ops: `/operator/ingest-status`, `/ingest-failures`, `/ingest-backfill-queue`, `/ingest-backfill`.

### 3.5 Pattern for adding a new operator-visible figure

1. **Schema**: migration (next `sql/NNN`) creating table or VIEW. If observation-shaped, mirror provenance block + `record_*_observation` + `refresh_*_current` + partition strategy. Add `_PLANNER_TABLES` entry. Add CHECK constraints + Literal types on app side.
2. **Write side**: ingester in `app/services/<source>_ingest.py`. Persist raw payload first (rule L1168). Write to typed table + write-through to observations + refresh `_current`. Manifest state-transition. `data_freshness_index` poll outcome.
3. **Read side**: dedicated service in `app/services/<feature>.py` exposing `get_<thing>(conn, ...)` reading from `_current` snapshot inside `snapshot_read(conn)`. Return frozen dataclasses. Tag every slice with `denominator_basis` if it touches the rollup.
4. **API**: endpoint in `app/api/instruments.py` or sibling. Call inside `with snapshot_read(conn):`. Return Pydantic mirror. Empty/no-data paths return 200 with empty payload + state flag, never 503.
5. **Frontend**: shape in `frontend/src/api/<feature>.ts` + page component. Match denominator_basis logic on FE so charts agree with rollup CSV invariants.
6. **Cron / job**: `ScheduledJob` entry in `app/workers/scheduler.py`. Gate behind `_bootstrap_complete`.
7. **Tests**: shape uniformity + ingest path + read path + frontend snapshot.
8. **Operator runbook**: backfill via `/jobs/sec_rebuild/run` for parser-version bumps.

## 4. "Where does X come from?" — common-questions FAQ

### Q1. AAPL institutional ownership %
- Subject: issuer.
- Tables: `ownership_institutions_current WHERE instrument_id=AAPL.id AND exposure_kind='EQUITY'` SUM(shares) / `instrument_share_count_latest.latest_shares`.
- Service: `get_ownership_rollup` → slice `category='institutions'`.
- Endpoint: `GET /instruments/AAPL/ownership-rollup`.
- Caveat: AAPL real institutional % ~62%; pre-#841 universe expansion the dev DB number is much lower because `institutional_filers` only contains the 11k-row form.idx universe and CUSIP coverage is still being expanded (#914 weekly job).

### Q2. Who holds the most TSLA?
- Same rollup endpoint. Holders sorted by shares within each slice.
- Per-filer drill: `GET /instruments/TSLA/institutional-holdings` (reads `institutional_holdings` directly with filer joins).
- 13D/G activists: `GET /instruments/TSLA/blockholders`.

### Q3. When was the last 10-K for MSFT?
- `sec_filing_manifest WHERE instrument_id=MSFT AND form='10-K' ORDER BY filed_at DESC LIMIT 1`. Or `filing_events` (legacy) joined to `instrument_business_summary`.
- Service: `app/services/filings.py`.
- Endpoint: `GET /filings/{instrument_id}` and `GET /instruments/MSFT/filings/10-k/history`.

### Q4. Why does GME insider % look low?
- Cohen Form 4 reports DIRECT shares ~38M (`ownership_nature='direct'`).
- Cohen 13D/A reports BENEFICIAL shares ~75M (`ownership_nature='beneficial'`).
- Pre-#840 / pre-#788: priority chain `form4 > 13d/g` collapsed into single 38M, losing beneficial. Two-axis model + parallel 13D/G dedup pool is what makes both render today (`app/services/ownership_rollup.py:1260-1283`).
- If only ~38M shows: legacy table read path still in effect (shouldn't be post-#905) OR `ownership_blockholders_current` empty. Rebuild: `POST /jobs/sec_rebuild/run` with `{"instrument_id": <id>, "source": "sec_13d"}`.
- denominator_basis: insiders + blockholders both `pie_wedge`, so chart shows them as separate wedges, not merged.

### Q5. How fresh is the filings data?
- "Is filing X on file?" → `sec_filing_manifest WHERE accession_number=X`.
- "Should we have polled subject Y?" → `data_freshness_index WHERE subject_type=... AND subject_id=... AND source=...`. State + `expected_next_at` show cadence health.
- Last bootstrap: `bootstrap_state` (singleton) or `GET /system/bootstrap/status`.
- Per-job freshness: `GET /system/status` aggregates `check_all_layers`.
- Per-job last run: `job_runs WHERE job_name=...` or `GET /system/jobs` / `GET /jobs/runs?job_name=...`.
- SEC ingest backlog: `sec_filing_manifest WHERE ingest_status IN ('pending','failed') AND (next_retry_at IS NULL OR next_retry_at <= NOW())`.

### Q6. Which CIK does ticker X map to?
- Current: `external_identifiers WHERE provider='sec' AND identifier_type='cik' AND is_primary=TRUE AND instrument_id=X.id` OR `instrument_sec_profile.cik`.
- Historical: `instrument_cik_history WHERE instrument_id=X.id ORDER BY effective_from`.
- Symbol history (FB → META): `instrument_symbol_history` analogous.
- Reverse "this CIK is whose?": `external_identifiers WHERE provider='sec' AND identifier_type='cik' AND identifier_value='0000xxxxxxx'`.
- For 13F/13D/N-PORT FILERS (not issuers): `institutional_filers.cik`, `blockholder_filers.cik`, `sec_nport_filer_directory.cik` — those are filer CIKs, NOT issuer CIKs.
- Unresolved: `unresolved_13f_cusips`. Promotion runs weekly via `cusip_universe_backfill` + `cusip_extid_sweep`.

### Q7. Does AAPL have an N-PORT mutual-fund slice?
- `ownership_funds_current WHERE instrument_id=AAPL.id`. Source always `nport`. denominator_basis always `institution_subset` (memo overlay). Series identity = `fund_series_id` matched against `sec_fund_series`.
- Fund manager (RIC trust) CIK in `fund_filer_cik`. To find which manager: `sec_nport_filer_directory WHERE cik=fund_filer_cik`. NOT in `institutional_filers` (that's 13F managers, disjoint).
- Slice does NOT contribute to residual or concentration; visualises fund-level detail of holdings already counted via 13F-HR.

### Q8. Why is this DEF 14A holder showing in `def14a_unmatched`?
- DEF 14A bene-table holders carry `holder_name` only — no CIK on proxy itself. Match-to-CIK happens at rollup-read time via `holder_name_resolver.resolve_holder_to_filer`. If resolver returns `matched=False`, candidate lands in `def14a_unmatched`.
- Common causes: name normalisation gap (try cleaning the holder_name spelling), or holder genuinely has no Form 4 / Form 3 / 13F filing in the DB (typical for non-officer 5%+ holders).

### Q9. Where do I look at raw SEC payloads?
- Per-accession bodies: `filing_raw_documents` (sql/107). `raw_status` on manifest tracks whether body is stored.
- Per-CIK documents (e.g. submissions.json snapshot): `cik_raw_documents` (sql/109).
- Per-quarter SEC reference docs (13F Securities List): `sec_reference_documents` (sql/121).
- Filesystem `data/raw/**` retention: `raw_data_retention_sweep` (daily 02:00 UTC). Compaction state in `raw_persistence_state` (sql/038).

### Q10. How are observations dedup'd into _current?
- `refresh_<category>_current(conn, instrument_id=X)` — DELETE rows for instrument X then INSERT one row per `(holder_identity_key, ownership_nature, ...)` group.
- Winner ordering: source priority (`form4 > form3 > 13d > 13g > def14a > 13f > nport > ncsr`) → `period_end DESC` → `filed_at DESC` (amendments win) → `source_document_id ASC`.
- Wrapped in `pg_advisory_xact_lock(<hash of instrument_id>)` so concurrent refreshes serialise; PK on `_current` is second-line guard.
- Cross-source dedup ONLY within compatible natures — Cohen direct + Cohen beneficial both survive.

### Q11. Why might a chart show non-zero figure but CSV export sum 0?
- CSV emits two memo rows: `__treasury__` and `__residual__` for additive reconciliation `treasury + residual + Σ pie_wedge = shares_outstanding`.
- Memo-overlay slices (today: funds) emitted with `__memo:funds__` category prefix AFTER residual row. A spreadsheet `SUM(shares)` over the whole file is inflated. Filter to non-`__memo:` categories before summing. See `build_rollup_csv` (`app/services/ownership_rollup.py:1326-1444`).

### Q12. How do I trigger a re-ingest after a parser bump?
- Bump `parser_version` constant in parser code → manifest queries detect rows on older version → operator runs `POST /jobs/sec_rebuild/run` with `{"source": "sec_form4"}` (or scoped `{"instrument_id": N, "source": "sec_13f_hr"}`).
- Job flips manifest rows back to `pending`, manifest worker drains at 10 r/s shared. Monitor via `GET /jobs/sec_manifest_worker/status` (or count of `manifest WHERE ingest_status='pending' AND source='...'`).

### Q13. Where does shares_outstanding come from?
- `instrument_share_count_latest` view (sql/052) → DEI > us-gaap precedence on `financial_facts_raw`.
- Producing accession + form_type enriched via re-query in `_read_shares_outstanding` (`app/services/ownership_rollup.py:1138-1200`).
- EDGAR archive URL computed backend-side (not frontend) to prevent the wrong endpoint shape (Claude-PR-800 caught a `filenum=`-using FE URL bug).

### Q14. Where is the kill switch?
- `runtime_config` row keyed by name. Read by `get_kill_switch_status` (`app/services/ops_monitor.py`).
- Toggle: `POST /system/config/kill-switch` (`app/api/config.py:233`).

### Q15. How do share-class siblings on a shared CIK (GOOG/GOOGL, BRK.A/BRK.B) flow through ETL?

The rule: **multiple `instruments` rows can share one SEC CIK**. Alphabet Class A (GOOG) and Class C (GOOGL) both carry CIK `0001652044`; Berkshire Class A (BRK.A) and Class B (BRK.B) both carry CIK `0001067983`. Schema acknowledges this at [sql/099_unresolved_13f_cusips.sql:60](../../../sql/099_unresolved_13f_cusips.sql#L60) + [sql/103_instrument_symbol_history.sql:8](../../../sql/103_instrument_symbol_history.sql#L8).

**Two rules apply** (sourced from `data-sources/sec-edgar.md` §3.6 fan-out table — re-derive any new code path from there):

1. **Issuer-scoped filings (10-K, 10-Q, 8-K, S-1, DEF 14A, Form 3/4/5, Companyfacts XBRL, submissions JSON, business-summary text)** = filer IS the issuer, no per-security data in the body. Writers MUST fan out per-instrument across every instrument sharing the issuer CIK via `siblings_for_issuer_cik(conn, cik)` ([app/services/sec_identity.py](../../../app/services/sec_identity.py)). Entity-level tables (`sec_filing_manifest`, `filing_events`, `filing_raw_documents`, `eight_k_filings`, `insider_filings`, `def14a_ingest_log`) stay PK on accession — one row per filing regardless of how many siblings the issuer has. The PER-INSTRUMENT tables (insider observations, def14a holdings, instrument_business_summary, instrument_sec_profile, financial_facts_raw → via `financial_periods.instrument_id`) get the fan-out.

2. **CUSIP-resolved filings (13F-HR, N-PORT, N-CSR holdings, SC 13D/G)** = body contains CUSIP-bearing structured data that disambiguates share class. SHAPE differs per form: 13F-HR / N-PORT carry per-holding rows (one CUSIP per row of `<infoTable>` or `<invstOrSec>`); SC 13D/G carries ONE issuer per accession (one `<securityInfo>/<cusip>` per primary_doc — the filing IS about one issuer's class). Either way, NO fan-out needed at write time — CUSIP maps 1:1 to a SECURITY-level instrument (GOOG.CUSIP `02079K107` ≠ GOOGL.CUSIP `02079K305`, even though both share Alphabet's CIK 1652044). The parser's CUSIP-resolution at parse-time picks the correct share-class sibling automatically. Aggregation across share classes happens at READ time in the rollup layer when desired.

**Audit checklist for new SEC parsers**:

- Does the body of the filing have per-security CUSIPs (or any per-security identifier)?
  - **Yes** → CUSIP-resolved write path; no fan-out; no per-CIK sibling concern.
  - **No** → issuer-scoped fan-out required; MUST call `siblings_for_issuer_cik` when writing to per-instrument tables.
- Does the parser write to entity-level tables (PK on accession) or per-instrument tables (PK includes instrument_id)?
  - Entity-level → single row per accession is correct; no fan-out.
  - Per-instrument → fan-out is mandatory if filing is issuer-scoped.

**Discovery-side concern**: when a per-issuer-CIK discovery walker (e.g. PR11's universe-CIK walker for SC 13D/G) joins `instruments` to `external_identifiers` on CIK, it MUST be prepared for N siblings per CIK. The discovery layer either (a) writes one manifest row per accession and lets CUSIP at parse-time disambiguate (preferred for CUSIP-bearing filings), or (b) writes one hint-table row per (accession, sibling) and cross-validates CUSIP-resolution against the hint set at parse-time (PR11 pattern for SC 13D/G).

**Common bug shape**: a `SELECT DISTINCT cik FROM instruments` that loses sibling instrument_ids; or a writer that calls `_resolve_cik_to_instrument_id` and gets back an arbitrary sibling instead of the share-class-correct one. The cure is: never write per-instrument observations from a CIK — always go through CUSIP at parse time, or fan out across `siblings_for_issuer_cik`.

PR11 (#1233) spec authoring surfaced this concern; Codex 1b BLOCKING #2 caught a draft that would have routed SC 13D against GOOG-A onto the GOOGL-C sibling. The fix landed as a `(accession_number, instrument_id)` PK multi-row hint table + CUSIP-cross-validated parser branch.

## 5. Quick reference — file/line index

**Schema**:
- Two-layer ownership: `sql/113` insiders, `sql/114` institutions, `sql/115` blockholders, `sql/116` treasury+def14a, `sql/119` ingested_at, `sql/123` funds, `sql/127` esop.
- SEC manifest + freshness: `sql/118`, `sql/120`, `sql/121`.
- Fund directory: `sql/124`, `sql/125`, `sql/126`.
- Identity: `sql/001`, `sql/003`, `sql/051`, `sql/067`, `sql/099`, `sql/102`, `sql/103`.
- Filings legacy: `sql/056`, `sql/090`, `sql/093`, `sql/095`, `sql/097`.
- Fundamentals: `sql/032`, `sql/050`, `sql/052`.
- Bootstrap: `sql/129`, `sql/130`, `sql/131`, `sql/132`.
- Ops + auth: `sql/014`, `sql/015`, `sql/016`, `sql/018`, `sql/084`, `sql/087`, `sql/128`.

**Services (canonical entry points)**:
- Read: `app/services/ownership_rollup.py:1222` (`get_ownership_rollup`).
- Write/refresh: `app/services/ownership_observations.py`.
- Manifest: `app/services/sec_manifest.py`.
- Freshness: `app/services/data_freshness.py`.
- CUSIP: `app/services/cusip_resolver.py`.
- Holder name: `app/services/holder_name_resolver.py`.

**Workers**:
- Schedule: `app/workers/scheduler.py:453` (`SCHEDULED_JOBS`).
- Jobs runtime: `app/jobs/__main__.py`, `app/jobs/runtime.py`, `app/jobs/listener.py`.
- Singleton fence: `app/jobs/locks.py` (`JOBS_PROCESS_LOCK_KEY`).
- DB pool: `app/db/pool.py:open_pool`.
- Snapshot read: `app/db/snapshot.py:snapshot_read`.

**API**:
- `app/api/instruments.py` — `/instruments`.
- `app/api/system.py` — `/system/status`, `/system/jobs`.
- `app/api/jobs.py` — `/jobs/{name}/run`, `/jobs/runs`, `/jobs/requests`.
- `app/api/bootstrap.py` — `/system/bootstrap/status`, `/system/bootstrap/run`, `/system/bootstrap/mark-complete` (router prefix `/system/bootstrap`).

**Specs**:
- `docs/proposals/etl/ownership-tier0-cik-history.md`
- `docs/proposals/etl/ownership-full-decomposition.md` (Phase 1 + 3)
- `docs/specs/etl/coverage-model.md` (manifest + freshness + 3-tier polling)
- `docs/proposals/etl/def14a-bene-table-extension.md` (#843 ESOP)
- `docs/specs/bootstrap/first-install.md` (#993)
- `docs/specs/bootstrap/orchestration.md`

**Endpoint coverage matrix**: `.claude/skills/data-engineer/etl-endpoint-coverage.md` — per-endpoint wiring across bootstrap + standard refresh + freshness + watermark + rate-limit + parser. Read this when answering "are we covered for source X?" or "why isn't endpoint Y firing on cadence?". Last audit 2026-05-13.

**Settled decisions**: `docs/settled-decisions.md`.
**Review prevention log**: `docs/review-prevention-log.md`.

## 6.5. Pipeline orchestration — invariants

> **State:** This section describes the **post-#1064 target state.** PR1 introduces source-level `JobLock` + `ParamMetadata` + `params_snapshot`; PR3 unifies the `bootstrap_state` gate across scheduled-fire and manual-trigger paths. Pre-PR1 reality: `JobLock` keys on `job_name`, scheduled jobs are zero-arg, and manual `/processes/{id}/trigger` bypasses prerequisites. The "Pre-PRn history" notes below mark each transition.
>
> Read before adding a new scheduled job, bootstrap stage, or operator-exposed parameter on an existing job. Cross-reference: [`docs/wiki/job-registry-audit.md`](../../../docs/wiki/job-registry-audit.md) for the per-job parameter surface; [`docs/proposals/ui/admin-control-hub-rewrite.md`](../../../docs/proposals/ui/admin-control-hub-rewrite.md) for the umbrella decisions.

### 6.5.1 Source-level concurrency (PR1 target state)

`JobLock` keys on `source` (the rate-bucket bound), NOT on `job_name`. Same-source jobs serialise under one lock; cross-source jobs run in parallel. Sources:

| Source | What it bounds | Lock contention |
|---|---|---|
| `init` | universe-sync only | One job total |
| `etoro` | eToro REST budget | execute_approved_orders + candle_refresh + lookups serialise |
| `sec_rate` | SEC 10 req/s shared per-IP bucket | Every per-CIK fetch + every per-accession fetch competes here |
| `sec_bulk_download` | Fixed-URL SEC archive downloads | Disjoint from `sec_rate` — large fixed downloads, no per-issuer iteration |
| `db` | DB-bound bulk ingest of pre-staged data | Same-source jobs serialise under the source lock |

The reason this matters: SEC's 10 req/s is per-IP, not per-job. Two SEC jobs running concurrently DO compete for the same bucket — serialising them at the lock layer is the right model. But SEC's bulk-download endpoints (`sec_bulk_download` source) have a separate budget — a `cusip_universe_backfill` (12k rows of CUSIP map data) does NOT compete with a `daily_cik_refresh` (per-CIK submissions polls), so they should run concurrently. Per-job locking would conflate them.

Within-source same-`job_name` semantics: triggering `sec_def14a_ingest` twice with different `params` **still serialises** under the source lock — the second invocation waits for the first to release. Per-param-set lock identity (one lock per `(job_name, params_hash)`) is deferred to v2.

**Bootstrap dispatcher concurrency vs JobLock — separate mechanisms.** The bootstrap orchestrator's `_LANE_MAX_CONCURRENCY` map ([`bootstrap_orchestrator.py:237`](../../../app/services/bootstrap_orchestrator.py#L237)) historically allowed up to 5 parallel `db`-lane stages WITHIN a single bootstrap run. That is a dispatcher knob, not a `JobLock` semantic. Under PR1 source-level locking, same-source jobs serialise across the entire process — the dispatcher's lane-concurrency map is either retired or reinterpreted as a "max queued before the lock kicks in" hint. The locked decision is unambiguous: same-source = serialised at the lock; the lane-concurrency map does not override this.

**Bootstrap-lane family split (#1141, post-#1136 Task E).** The pre-#1141 `db` lane was a single-stage chokepoint that added ~4 h to first-install wall-clock by serialising 5 unrelated db-bound stages (5 stages × 283 min serial vs ~110 min cross-lane parallel, measured `bootstrap_run_id=3`). The family split (`bootstrap_orchestrator.py:237-270`) carves `db` into **`db_filings` / `db_fundamentals_raw` / `db_ownership_inst` / `db_ownership_insider` / `db_ownership_funds`** — each cap=1, but cross-family parallel. `db` stays as the catch-all for Phase E derivations + scheduler `db`-source jobs. `openfigi` is its own cap=1 lane (the per-instance `_RateLimiter` in `openfigi_resolver.py` is the budget gate; cross-process isolation = per-worker, NOT shared — multi-worker deploy = N separate budgets). `finra` is a JobLock lane only (`app/jobs/sources.py:74`); the bootstrap dispatcher does not run FINRA stages so the lane is absent from `_LANE_MAX_CONCURRENCY`. When ADDING a new stage that writes to a different table family, add a new `db_<family>` lane rather than reusing `db` — the family split is the load-bearing parallelism mechanism.

**Caller-wraps-transaction discipline (#915 FINRA pattern + #1208 Phase 3 retention).** Service-layer ingesters MUST NOT enter their own `with conn.transaction():` block. The orchestrator owns the transaction boundary — see [`bootstrap_orchestrator.py:2172`](../../../app/services/bootstrap_orchestrator.py#L2172): *"The caller must commit the connection's transaction; the helper does not start its own `with conn.transaction():` because the orchestrator's prelude lives outside any open transaction."* Two coexisting patterns:

| Pattern | Owner of `with conn.transaction()` | Owner of `cur.copy(...)` | Example |
|---|---|---|---|
| **Per-archive bulk** (§2.10b) | bootstrap_orchestrator opens fresh conn per archive; per-archive transaction wraps the COPY drain | service `sec_13f_dataset_ingest`, `sec_nport_dataset_ingest`, `sec_insider_dataset_ingest` writes the COPY | `_phase_bulk_ingest` in `bootstrap_orchestrator.py` |
| **Caller-wraps (G7 / #915)** | `ScheduledJob` wrapper opens conn + transaction | service `finra_short_interest_ingest`, `finra_regsho_ingest` runs raw INSERTs inside the supplied tx | `finra_short_interest_refresh.py` |

`finra` adopted G7 because the fetch + write must be ONE tx (no half-applied bimonthly + half-applied row-count footer validation). The per-archive bulk pattern would split fetch + drain across two transactions which breaks the validation invariant. **Rule:** any new service-level ingester chooses pattern by asking "is fetch + write atomic per logical unit?". If yes (FINRA), G7. If no (multi-million-row SEC bulk archive), per-archive bulk. Never both.

**Multi-writer sink registry (target state — `docs/specs/etl/sinks/<table>.md`).** Several sink tables have ≥ 2 writers and conflict-key declarations drift between callers if managed per-spec:

| Sink table | Writers | Conflict key |
|---|---|---|
| `filing_events` | `sec_submissions_files_walk`, `sec_atom_fast_lane`, `sec_daily_index_reconcile`, `sec_per_cik_poll` | `uq_filing_events_provider_unique (provider, provider_filing_id)` |
| `sec_filing_manifest` | every parser + every discovery layer (~12 writers) | PK `accession_number` |
| `unresolved_13f_cusips` | `sec_13f_dataset_ingest`, `sec_nport_dataset_ingest`, `institutional_holdings._record_unresolved_cusip` | partial UNIQUE legacy/bulk split (sql/164) |
| `external_identifiers` (provider='openfigi') | `cusip_resolver.sweep_unresolved_cusips_via_openfigi`, `cusip_universe_backfill` | partial UNIQUE post-sql/143 (split by `provider='sec' AND identifier_type='cik'`) |
| `ownership_*_observations` | per-source `record_*_observation` + `sec_*_dataset_ingest` bulk COPY drain | per-table identity tuple (§1.2) |

When ADDING a new writer to any sink in this list, sync the conflict key + retention horizon + `parser_version` semantics with every existing writer. The lint guards at `scripts/check_*.sh` enforce the load-bearing pieces (PR12 writer pattern, COPY pattern, business-summary latest-only) but not cross-writer drift. The sink-registry doc — when it lands — is the authoritative cross-writer source.

**Dispatcher mental model (PR-2 #1233 — `as_completed` poll loop).** [`_phase_batched_dispatch`](../../../app/services/bootstrap_orchestrator.py) does NOT wait for a "ready batch" to drain before re-evaluating the runnable set. It uses a `wait(in_flight, return_when=FIRST_COMPLETED, timeout=1.0s)` poll:

1. On EVERY completion, immediately recompute `caps = _satisfied_capabilities(statuses, rows_processed, ...)`. The freshly-terminalised cap-provider may unblock siblings on the very next iteration — NOT after the slowest sibling in some heterogeneous batch finishes.
2. Per-lane in-flight is tracked via `lane_in_flight_count: dict[str, int]`. Submission to lane `L` is gated on `lane_in_flight_count[L] < _LANE_MAX_CONCURRENCY[L]`. At-cap stages stay `pending` and are reconsidered on the next poll iteration.
3. One persistent `ThreadPoolExecutor` per lane (lifetime = function entry → exit via `try/finally`). Same-lane submissions reuse threads; cross-lane submissions live in their own executors and run truly concurrently.
4. Cancel checkpoint fires at the TOP of every poll iteration (after every completion, not every batch). Pre-PR-2 latency was the duration of the longest in-flight batch (~5+ min on 13F sweeps); now it's bounded by ~ longest single stage + 1.0s cancel-poll timeout.
5. Deadlock detection: when no future is in-flight AND no cascade transition occurred AND pending stages remain, the dispatcher flips them to `blocked` with the "abandoned" reason. The cap-eval classifier (`_classify_requirement_unsatisfiable`) handles the common no-provider case directly via the "missing capability" reason — `abandoned` only fires when the graph is genuinely stuck (classifier returns None for an unsatisfiable requirement, which shouldn't happen with the current cap-map).

**Pre-PR-2 anti-pattern (do not reintroduce):** `wait([f for _, f in all_futures])` blocks until EVERY future in the current ready batch completes. On run_id=4 this left the `sec_rate` lane idle for 80+ min while the `db` lane churned. Tests at [`tests/test_bootstrap_dispatcher_cross_lane_parallelism.py`](../../../tests/test_bootstrap_dispatcher_cross_lane_parallelism.py) pin the per-completion contract — any future "optimisation" that reverts to batch-join semantics fails them. Specifically:

- A cross-lane test asserts five 100ms fast-lane stages all finish BEFORE a 2s slow-lane batch-mate. Batch-join semantics fail it because fast siblings serialise behind the join.
- A cap-recomputation test asserts a stage_B (cap-requirer) starts within 0.3s of stage_A (cap-provider) completing, even when a stage_pad sibling in the same submission round sleeps for 2.0s.

**`sec_rate` starvation risk.** A 1-hour `sec_def14a_bootstrap` drain holding the `sec_rate` lock will skip every hourly SEC ingest fired during that window (`sec_form4`, `sec_filing_documents_ingest`, `sec_8k_events_ingest`). Weekly bounded — acceptable. Repeated manual drains chain together to multiple-hour starvation — operator visibility required. Admin process table must surface "skipped — `sec_rate` source lock held by `sec_def14a_bootstrap`" with the holder identified, otherwise the operator sees mysterious skips during a drain and triggers it again, deepening the starvation. This goes in PR1's REASON_TOOLTIP map under `lock_held_by_other_source_member`.

### 6.5.2 ParamMetadata discipline

Every job in `SCHEDULED_JOBS` declares a tuple of `ParamMetadata` describing its operator-exposable parameter surface:

```python
@dataclass(frozen=True)
class ParamMetadata:
    name: str
    label: str
    help_text: str
    field_type: Literal["string", "int", "float", "date", "quarter", "ticker", "cik", "bool", "enum", "multi_enum"]
    default: Any | None
    advanced_group: bool
    enum_values: tuple[str, ...] | None = None
```

The Pydantic mirror in [`frontend/src/api/types.ts`](../../../frontend/src/api/types.ts) is the API contract. **Drift between the BE model and the FE types is a PREVENTION-log-grade risk** — the FE renders generic Advanced disclosure fields off this metadata; if the contract drifts, operators see the wrong inputs or no inputs at all. Round-trip tests cover one job (canonical) every PR; full-mirror coverage is bot-enforced via `frontend/src/api/types.ts` review.

Field types collapse to ~10 archetypes — see [`job-registry-audit.md` §6](../../../docs/wiki/job-registry-audit.md). Ticker / CIK fields render as typeaheads resolving to internal IDs operator-side; the BE receives the resolved `int` / `str`.

### 6.5.3 Bootstrap orchestration is parameter overrides — not bespoke wrappers

A bootstrap stage is `(stage_key, stage_order, lane, job_name, params dict)`. The orchestrator passes `params` to the registered callable; the callable validates against its `ParamMetadata`. There are NO separate bootstrap-only callables.

Pre-PR1 history: [`bootstrap_orchestrator.py`](../../../app/services/bootstrap_orchestrator.py) carried three bespoke wrappers (`bootstrap_filings_history_seed`, `sec_first_install_drain_job`, `bootstrap_sec_13f_recent_sweep_job`) that re-implemented the dispatch + `_tracked_job` wrapping just to override default params. PR1 lifts the shared workflow into the lower-level helpers (`refresh_filings`, `run_first_install_drain`, `ingest_all_active_filers`); bootstrap stages become data, not code; bespoke wrapper files disappear.

Practical consequence: any job invokable from bootstrap is ALSO invokable post-bootstrap from the admin process table with the same param surface. Operators never get stuck in a "this only runs during bootstrap" UX trap.

### 6.5.4 `bootstrap_state.status` as universal gate

Both scheduled-fire AND manual-trigger paths check `bootstrap_state.status='complete'` before running any gated job. `partial_error` is NOT-complete: scheduled fires reject, manual triggers reject by default. Manual remediation requires explicit `?override_bootstrap_gate=true` and writes a `decision_audit` row.

Pre-PR3 history: scheduled-fire honoured `_bootstrap_complete` prerequisite; manual `/processes/{id}/trigger` bypassed it. PR3 collapses both paths through `_check_bootstrap_state_gate(conn, *, allow_manual_remediation=...)`.

The 409 surfaces with reason `bootstrap_not_complete` and copy "Bootstrap is not complete. Finish first-install before triggering scheduled jobs." in [`frontend/src/components/admin/processStatus.ts::REASON_TOOLTIP`](../../../frontend/src/components/admin/processStatus.ts).

**Carve-outs (#1181):** registered jobs with `exempt_from_universal_bootstrap_gate=True` BYPASS the gate on all three dispatch paths — scheduled fire, catch-up, manual-queue. No `decision_audit` row is written for exempt fires (distinct from the operator-override path which does write audit). Current allow-list: `sec_daily_index_reconcile` only. Adding a new exempt job requires a spec + Codex review + update to `tests/test_universal_gate_carve_out.py::test_exempt_allowlist_is_explicit`. See settled-decisions §"Safety-net catch-up gate carve-out (#1181)" + §7.8 runbook below.

### 6.5.5 `job_runs.params_snapshot JSONB`

Every job_runs row records the params dict that produced it. Manual triggers write the operator-supplied dict; scheduled fires write the registry default + cadence-derived overrides. Operator history visibility — clicking a row in the admin process table reveals "this run used `chunk_limit=200, since=2026-01-01`" rather than just "ran for 47s".

Default `'{}'::jsonb` for jobs with no operator-exposable params.

This is the audit trail for "why did this run pull only 200 rows instead of the usual 500?" — answer is in `params_snapshot`. The pre-PR1 path lost this signal.

### 6.5.6 Prerequisite enforcement is unified

Every job in `SCHEDULED_JOBS` declares a `prerequisite: PrerequisiteFn | None`. Scheduled fire and manual trigger both call the same prereq through `_check_bootstrap_state_gate`. There is no manual-bypass shortcut. If the operator needs to fire a job whose prereq is unmet (e.g. `_has_actionable_recommendations` is False but they want to dry-run), the path is `?override_bootstrap_gate=true` (or future `?override_prerequisite=true`) with audit row, NOT a separate untracked code path.

### 6.5.7 Discipline summary — when adding a new scheduled job

1. **Pick a source.** Match the rate-bucket reality. Don't invent a new source unless the budget really is disjoint.
2. **Declare `ParamMetadata`.** Even if empty tuple. The decl forces the question "what should the operator be able to override?". Reject implementation-strategy knobs (`prefetch_urls`, `follow_pagination`, `use_bulk_zip`) and provenance labels (`source_label`, `match_threshold` outside narrow bounds) — those belong in code review, not Advanced disclosure. The audit `docs/wiki/job-registry-audit.md` §6 has the canonical exposable-vs-internal split.
3. **Mirror in `frontend/src/api/types.ts`.** Same PR. Drift = bug.
4. **Wire `prerequisite` if applicable.** `_bootstrap_complete` is the most common; compose with `_all_of(...)` for additional gates.
5. **Decide `catch_up_on_boot`.** Rule of thumb: TRUE for cheap idempotent reads (lookups, classification refreshes); FALSE for anything that hits a rate budget or holds DB workers.
6. **Audit row schema.** `_tracked_job(JOB_NAME)` writes the `job_runs` row; ensure the constant is in `__all__` so the registry-shape test catches drift.
7. **Test invariants.** Every job needs a registry-shape test (`tests/test_job_registry.py`) covering source non-NULL + params_metadata validates + prerequisite is callable.
8. **Queue/audit terminal-state correctness.** When the prelude (source lock acquisition, prereq check, `bootstrap_state` gate, fence-held opt-out) skips the body without invoking the underlying job, the corresponding `pending_job_requests` row MUST transition to `rejected` (not `completed`) with a structured `error_msg`. `mark_request_completed` after a skipped body produces an audit row that says "ran successfully" when the job never ran. PREVENTION-log-grade hazard — see PR #1072 BLOCKING and PR #1078 (orchestrator opt-out fence). The skipped/rejected vs completed distinction is the operator's only signal that a manual trigger didn't actually do what they asked. Grep `mark_request_completed` and verify each call site has the matching `mark_request_rejected` for the prelude-skip path.

### 6.5.8 Bootstrap manifest-reset prelude (#1233 PR-5a)

`run_bootstrap_orchestrator` runs a one-shot `reset_manifest_for_run` prelude AFTER the `running` snapshot validation and BEFORE the dispatcher loop. It UPDATEs `sec_filing_manifest`:

```
SET ingest_status='pending', next_retry_at=NULL, error=NULL, last_attempted_at=NULL
WHERE source = ANY(_BOOTSTRAP_MANIFEST_SOURCES::text[])
  AND ingest_status='failed'
  AND last_attempted_at IS NOT NULL
  AND last_attempted_at < bootstrap_runs.triggered_at
```

**Why:** a cancelled prior run can leave `failed` rows with `next_retry_at` in the future; without the reset, those rows refuse to drain in the new run even when parser_version bumped or the failure was transient. Run #4 inherited 1.18M `failed` rows from cancelled run #3 — the prelude prevents that recurrence.

**Source whitelist** = SEC subset of `ManifestSource` (every value except `finra_short_interest` / `finra_regsho_daily`). FINRA sources are owned by non-bootstrap drivers; the bootstrap orchestrator must not flip their failure state. The whitelist is computed at module load from `_MANIFEST_SOURCES_BY_STAGE` ([app/services/bootstrap_orchestrator.py](../../../app/services/bootstrap_orchestrator.py)).

**Time filter** = strict `<` against `bootstrap_runs.triggered_at`. A concurrent live cron writer landing a fresh `failed` row mid-reset has `last_attempted_at >= NOW() >= reset_started_at` and survives the predicate. Boundary case: equal timestamps are NOT reset (this is the operator's signal that the failure came from the running orchestrator itself, not the prior cancelled run).

The `last_attempted_at` stamp itself uses `clock_timestamp()` (statement-time wall-clock) rather than `NOW()` (= `transaction_timestamp()`, fixed at tx start). A worker tx that began BEFORE `bootstrap_runs.triggered_at` but commits AFTER would otherwise stamp the row with a tx-start time that survives the reset predicate and gets erroneously flipped — `clock_timestamp()` removes that race.

**Opt-out** = `bootstrap_runs.params['reset_failed_manifest']` (default TRUE; column added in sql/169 as `JSONB NOT NULL DEFAULT '{}'::jsonb` with `jsonb_typeof = 'object'` CHECK). Set FALSE via `POST /system/bootstrap/run` body `{"reset_failed_manifest": false}` when an operator deliberately wants to preserve stale `failed` rows from a prior run (e.g. debugging which accessions tripped a parser bug; letting routine backoff drive drainage). The orchestrator tests for exact `is False` so type-drift (string `"false"`, integer `0`) fail-closed against silent opt-out.

**Idempotency:** the reset is a single SQL UPDATE; a second invocation finds zero matching rows. Re-queuing the orchestrator after a worker crash is safe.

**Out of scope:** the prelude does NOT touch rows in any other state — `pending` rows stay pending, `parsed` stays parsed, `tombstoned` stays tombstoned. A stuck-`pending` row is the manifest-worker's problem (#1224); a `tombstoned` row needs an explicit `POST /jobs/sec_rebuild/run`.
### 6.5.9 Bootstrap orphan-stage reaper (#1233 PR-6)

When the jobs process crashes mid-stage, `bootstrap_stages.status` stays `'running'` forever. On restart, the dispatcher's `mark_stage_running(... AND status='pending')` silently no-ops against the stale row and the run sits stuck — operator must manually clear via Re-run failed.

The `reap_orphaned_running_stages` prelude (in [`bootstrap_orchestrator.py`](../../../app/services/bootstrap_orchestrator.py)) runs at the top of `run_bootstrap_orchestrator`, immediately after the `read_latest_run_with_stages` snapshot and before the dispatcher loop. Reset criteria — ALL THREE must hold:

1. `status = 'running'`.
2. `started_at < NOW() - INTERVAL '5 minutes'` (`_REAPER_GRACE_SECONDS = 300`). The grace window is longer than the slowest known stage's start-up so a worker that's alive-but-slow doesn't get its row pulled out from under it.
3. The corresponding `JobLock` advisory lock is NOT held in any Postgres session **in this database**. Probed via `SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objsubid=1 AND database=(current DB OID) AND classid=... AND objid=...` — **read-only, NEVER acquires**. The `database` predicate matters when multiple eBull instances share a Postgres cluster (dev + ebull_test on localhost:5432): the same advisory key in a sibling DB would otherwise spuriously suppress reset in this DB.

Lock key derivation MUST byte-for-byte match [`JobLock` at `app/jobs/locks.py:224`](../../../app/jobs/locks.py#L224): `hashtext('job_source:' || <source>)::int` (NOT `hashtextextended` — the JobLock key space is int4 by construction).

**`pg_locks` shape gotcha (empirically verified 2026-05-23).** `hashtext` returns a signed `int4`. When that's widened to the bigint key for `pg_try_advisory_lock(bigint)`, the high 32 bits depend on the sign:

- Positive hashtext (e.g. `hashtext('job_source:openfigi') = 1_447_707_902`): `pg_locks.classid = 0, objid = <hashtext>`.
- Negative hashtext (e.g. `hashtext('job_source:finra') = -685_386_401`): `pg_locks.classid = 4_294_967_295 (= 0xFFFFFFFF), objid = <hashtext & 0xFFFFFFFF> = 3_609_580_895`.

A naive probe of `classid = 0 AND objid = <hashtext>` would silently MISS every negative-hashtext key and reset stages whose workers are alive. The correct probe lets Postgres do the bigint split itself: `classid = ((K::bigint >> 32) & 4294967295)::oid AND objid = (K::bigint & 4294967295)::oid`. `objsubid = 1` is session-scope (the `JobLock` form); transaction-scoped advisory locks use `objsubid = 2` — we deliberately do NOT probe those.

**Reset shape.** On match, the row transitions `running` → `pending`: `started_at` / `completed_at` cleared, `last_error` APPENDED (not replaced) with `'reaper: reset from orphaned running (<NOW()>)'`. The append preserves forensic context from the previous crash so the operator can still investigate via `last_error`.

The UPDATE is guarded by `AND status = 'running'` so a stage that transitioned to `'success'` / `'error'` between the SELECT and the UPDATE is left alone (Codex pre-push W3 pattern).

**Caveats — accepted residual risk.**

* Cannot detect a hung-but-alive worker that holds the lock but makes no progress. The grace window catches the obvious case (worker crashed before its first commit); a deeper deadlock requires operator Re-run failed.
* Cannot detect the #1184 re-entrancy edge case where the outer-thread holds the lock but the stage-thread itself crashed — the outer holder will release on its own crash, and the next reaper pass after grace will reset.
* A stage whose `job_name` is not in the `app.jobs.sources.JOB_NAME_TO_SOURCE` registry is LEFT ALONE (logged as a warning). The reaper must never reset a stage whose lock-key shape it cannot derive — doing so would silently reset a stage whose worker IS alive (registry gap is an operator mistake, not a crash signal).

Acceptance criterion (`docs/proposals/etl/bootstrap-optimisation.md` §15): "process crash mid-stage: reaper resets to pending within 6 min (5 min grace + 1 min poll)".

### 6.5.10 Cap-ordering for concurrent writers (#1233 PR-1292)

PR-2's `as_completed` cross-lane parallelism exposed a row-lock contention bug: S8 `sec_submissions_ingest` (db_filings lane) and S15 `filings_history_seed` (sec_rate lane) both write to `filing_events` for the same `(instrument_id, …)` keys. Before PR-2 they were accidentally serialised by `wait(ALL_COMPLETED)`. After PR-2 they ran concurrently → PG transaction-lock contention left S8 stuck on the wait-graph for 17+ min.

Fix shape (`submissions_processed` capability):
- New capability provided by S8 on **success AND skip** (`_STAGE_PROVIDES_ON_SKIP`).
- S15 requires it via `CapRequirement(all_of=("cik_mapping_ready", "submissions_processed"))`.
- Effect: S15 waits for S8 to terminalise. Slow-connection fallback preserved because S8-on-skip still satisfies the cap.

**Audit pattern for adding any new stage that writes to a shared table:**
1. Grep `_STAGE_PROVIDES` + `_STAGE_REQUIRES_CAPS` for stages writing to the same target.
2. If cross-lane (the dispatcher would run them concurrent), introduce an ordering cap.
3. The cap should be provided ON SKIP if the downstream's slow-connection-fallback path requires the run to continue.

**Lock-contention audit COMPLETE (2026-05-23):** PR-1292 fixed S15↔S8. The 2026-05-23 audit identified 3 more pairs needing cap-gates:
- **S22 sec_13f_recent_sweep ↔ S10 sec_13f_ingest_from_dataset** — both write `ownership_institutions_observations`. Cap: `institutional_dataset_processed`.
- **S19 sec_insider_transactions_backfill ↔ S11 sec_insider_ingest_from_dataset** — both write `ownership_insiders_observations`. Cap: `insider_dataset_processed`.
- **S20 sec_form3_ingest ↔ S11** — same shared table as S19. Same cap.

All three cap-gates land in the same PR mirroring PR-1292's pattern: bulk ingester provides cap on success (`_STAGE_PROVIDES`) AND on skip (`_STAGE_PROVIDES_ON_SKIP`) for slow-connection-fallback parity; legacy stage requires it in `_STAGE_REQUIRES_CAPS`.

S17 sec_def14a_bootstrap / S18 sec_business_summary_bootstrap / S21 sec_8k_events_ingest — verified SAFE (disjoint writes, already chained behind `submissions_secondary_pages_walked` which transitively depends on S8).

**Ordering-only cap semantic:** these three caps (plus `submissions_processed`) are advertised whenever the upstream stage reaches ANY terminal status — not just success/skip but also blocked/error/cancelled. The cap's only meaning is "no concurrent writer remains"; once the upstream stage has terminalised, the legacy chain can write safely regardless of how the upstream ended. The `_ORDERING_ONLY_CAPS` frozenset is the dispatch hook in `_satisfied_capabilities` + `_capability_is_dead`. Without this concession, a cascade-blocked bulk stage would falsely gate its legacy counterpart from recovering (`test_partial_bulk_failure_legacy_recovers` regression sentinel).

### 6.5.11 NUMERIC precision gate for bulk ingest (#1233 PR-1291)

`ownership_funds_observations.shares` is NUMERIC(24, 4) NOT NULL with strict CHECK (shares > 0). PR-3's COPY-batched path replaced per-row INSERT + SAVEPOINT, which had silently absorbed CHECK violations as `rows_skipped_bad_data`. After PR-3 a single CHECK violation aborts the whole archive.

Trap: a fractional-share holding like `Decimal("0.00005")` passes Python's `balance > 0` predicate but quantises to `0.0000` on COPY into the NUMERIC(24, 4) staging — then trips the strict CHECK on the drain INSERT.

Fix shape (NPORT only — other ownership tables have no strict CHECK):
```python
_BALANCE_QUANTUM: Final = Decimal("0.0001")  # matches NUMERIC(24, 4) scale

balance_q = balance.quantize(_BALANCE_QUANTUM, rounding=ROUND_HALF_EVEN)
if balance_q <= 0:
    result.rows_skipped_non_positive_shares += 1
    continue
balance = balance_q  # reassign so write_row writes the value the gate validated
```

`ROUND_HALF_EVEN` matches Postgres NUMERIC coercion exactly. The same gate is required for any new bulk ingester that writes to a NUMERIC column with a strict positive CHECK.

### 6.5.12 Bulk-path unresolved-CUSIP capture (#1233 PR-1a)

Pre-PR-1a the bulk dataset ingesters (`sec_13f_dataset_ingest`, `sec_nport_dataset_ingest`) silently dropped rows whose CUSIP wasn't in the cusip_map — incremented `rows_skipped_unresolved_cusip` counter only. Run #6 demonstrated 2M+ such drops on a full bootstrap.

PR-1a captures them into `unresolved_13f_cusips` via the new helper `cusip_resolver.record_unresolved_cusip_from_bulk(conn, *, cusip, filer_cik, period_end, source)`. PR-1b's OpenFIGI sweep (S13 `cusip_resolver_post_bulk_sweep`) reads this buffer and promotes matches to `external_identifiers (provider='openfigi')`.

Schema split (sql/164 — see `unresolved_13f_cusips_bulk_columns.sql`):
- Original `(cusip)` PRIMARY KEY relaxed; new partial UNIQUE INDEX on `(cusip, filer_cik, period_end, source) WHERE source IS NOT NULL` for bulk writers.
- Legacy per-filing path still writes with `source=NULL`; partition-isolated via the resolver's `AND source IS NULL` clamp.

**Performance fix (#1295 — shipped)**: the per-row INSERT + SAVEPOINT loop is replaced by :func:`cusip_resolver.flush_unresolved_cusips_bulk`. The helper streams the buffer into a TEMP staging table ``_stg_unresolved_cusips_bulk`` (``ON COMMIT DROP``) via ``cur.copy()``, then drains via ``INSERT INTO unresolved_13f_cusips SELECT … FROM _stg ON CONFLICT … WHERE source IS NOT NULL DO NOTHING``. Same dedup semantics on the partial UNIQUE INDEX ``unresolved_13f_cusips_bulk_idx``. Single shared helper for both 13F + NPORT ingesters. Pre-fix ~1k rows/s; post-fix ~30-50k rows/s — saves 15-30 min Phase C wall-clock on a full bootstrap with a large unresolved backlog.

### 6.5.13 OpenFIGI reverse-resolver caveats (#1233 PR-0/PR-1b)

OpenFIGI v3 `/v3/mapping` accepts `idType=ID_CUSIP, idValue=<cusip>` and returns `{ticker, name, exchCode, securityType, …}`. **It does NOT return CUSIP in any response — CUSIP is input-only.** Approved use is CUSIP → ticker reverse resolution; the resolver matches the returned ticker against `instruments.symbol` and writes `external_identifiers (provider='openfigi', identifier_type='cusip', identifier_value=<cusip>, instrument_id=<matched>, is_primary=FALSE)`.

Critical defensive filter: AAPL's CUSIP returns 255 worldwide listings. Pick the entry matching `exchCode = 'US' AND securityType = 'Common Stock'`. Do NOT trust `data[0]`.

Rate limits (verified empirically, fixtures at `tests/fixtures/openfigi/`):
- Unkeyed: 25 req/min × 10 jobs/POST = 250 mappings/min
- Keyed: 25 req/6s × 100 jobs/POST = 25,000 mappings/min

429 response body is **plain text**, not JSON — branch on status BEFORE calling `.json()`.

Key loaded via `Settings.openfigi_api_key` (env or `.env` file). `OpenFigiResolver.from_env()` is the canonical entrypoint. See `app/services/openfigi_resolver.py` + `.claude/skills/data-sources/openfigi.md`.

### 6.5.14 Bootstrap stage declaration — `fetch_strategy` discipline

Every bootstrap stage MUST declare a `fetch_strategy` from a closed-set enum. The enum is the operator's contract for "what does this stage TOUCH?" + the dispatcher's load-bearing input for forbidden-HTTP linting in bootstrap mode.

Allowed values:

| Value | Meaning | Bootstrap-mode permission |
|---|---|---|
| `bulk_archive` | Fetch fixed-URL SEC bulk archive (zip/tsv). One HTTP per archive | YES |
| `per_resource_http` | Per-CIK or per-accession HTTP fetch | YES only if no bulk archive exists for the resource |
| `batched_http` | Multi-resource POST (OpenFIGI `/v3/mapping`, eToro batched lookups). N requests, M resources, N << M | YES if rate-budget bounded |
| `atom_feed` | RSS/Atom polling for discovery. Steady-state only | NO in bootstrap (use bulk_archive + daily-index seed) |
| `push` | Server-pushed connection (eToro WebSocket) | NO in bootstrap (steady-state only) |
| `cache` | Read from a producer stage's persisted artifact (e.g. S14 consuming S8's `submissions.zip` files[] sidecar) | YES |
| `derive` | Pure SQL — no HTTP at all (audit sweeps, write-throughs, refresh_*_current calls) | YES |

The bootstrap-mode rule (§6.5.15 below) gates stages whose `fetch_strategy ∈ {atom_feed, push}` from running during a bootstrap; `per_resource_http` is permitted only when no `bulk_archive` exists for the same source. The dispatcher logs `forbidden_http_in_bootstrap` when a bootstrap stage's runtime issues HTTP requests that exceed its declared `fetch_strategy` budget — see Codex finding #7 in the v3 committee review for the failure class this prevents.

When ADDING a new bootstrap stage, set `fetch_strategy` on its `StageSpec` (defaulting to a non-existent value MUST fail-closed in the catalogue-invariant test). The full StageSpec extension is documented in the discoverable `etl-stage-declaration` skill.

### 6.5.15 Bootstrap-mode = derivation + idempotent-sink only

**Rule:** bootstrap-mode entrypoints (any stage in `_BOOTSTRAP_STAGE_SPECS` running during a `bootstrap_state.status ∈ {pending,running,partial_error}` window) MUST be derivation-only. They draw from already-persisted SEC archives + DB state; they do NOT issue per-CIK / per-accession HTTP. The carve-outs are explicit and small:

| Carve-out | Why | Implementation |
|---|---|---|
| **S6 `cik_refresh`** | CIK directory has no bulk archive | `per_resource_http` against `data.sec.gov/submissions/CIK*.json` for tradable instruments only |
| **S16 institutional drain** | 13F filer registry has no bulk archive | `per_resource_http` bounded by `institutional_filers.last_13f_hr_at` cohort (#1010, post-#1222) |
| **S27 N-CSR** | NCEN / NCSR have no bulk archive | `per_resource_http` against `data.sec.gov/submissions/CIK*.json` for RIC trust CIKs |
| **S13 OpenFIGI sweep** | CUSIP→ticker reverse-lookup has no SEC equivalent | `batched_http` against `api.openfigi.com`, own `openfigi` lane |

Every other bootstrap stage MUST consume already-fetched bulk archives (`bulk_archive`), already-derived DB rows (`cache` / `derive`), or skip. The reason this rule is load-bearing: pre-#1233 runs measured each S25 fundamentals_sync at 101 min because Phase 1 issued 5,105 sequential per-CIK XBRL fetches; the bulk archive (`companyfacts.zip`, S9) had already loaded the same data in 15 min. The HTTP path was redundant AND quadratic-in-CIK-count.

**Coverage-floor pattern** (#1233 PR-1b): when a bootstrap-mode entrypoint DERIVES from a multi-source pool (S25 fundamentals from `financial_facts_raw` + `financial_periods`), validate per-CIK coverage BEFORE deriving. The PR-1b sweep stamps `bootstrap_runs.coverage_floor_met` with `coverage_ratio >= 0.80` post-S13; if FALSE, the derivation is allowed to complete (informational only) but the operator sees an amber chip on the admin panel. The pattern generalises: every bootstrap-mode entrypoint that reads from a sparse table SHOULD record a coverage telemetry signal so a Pyrrhic 25-min Run #8 (passes wall-clock, fails completeness) is immediately visible.

**Audit-during-bootstrap trap** (Codex v3 finding #8, CRITICAL): `audit_all_instruments` ([`coverage.py:1018`](../../../app/services/coverage.py#L1018)) classifies from `filing_events` aggregates. If it runs BEFORE S14/S15 populate filing history, it returns false `insufficient` verdicts that trigger Phase 2 backfills (`scheduler.py:3374-3399`), reintroducing the per-CIK HTTP the bootstrap mode was supposed to prevent. **Rule:** bootstrap-mode `fundamentals_sync_bootstrap` MUST call `audit_all_instruments` only AFTER S14 (sec_submissions_files_walk) AND S15 (filings_history_seed) have terminalised — gate via cap requirement, not stage_order.

### 6.5.16 Hallucinated-API class of defect (post-v3 committee, 2026-05-23)

The v3 spec round produced 4 invented APIs (`_STAGE_CATALOGUE_RENAME_MAP`, `master_key.is_bootstrapped()`, `coverage_audit()`, `financial_facts_raw.cik` column). Every one would have failed at first compile or first runtime. Class is structural: reviewer cited symbols not grepped.

**Defence:** when writing a spec that names a function / column / constant / module path, EVERY identifier MUST be `grep`-verified pre-Codex. The before-spec gate in §0.0 is the load-bearing check. Codex sees what the reviewer pre-checked; it does NOT re-grep every identifier. If the reviewer didn't, Codex misses it.

| Hallucinated reference | Real API |
|---|---|
| `_STAGE_CATALOGUE_RENAME_MAP` | Mechanism is automatic: `_BOOTSTRAP_STAGE_SPECS` lookup by `stage_key` ([`bootstrap_orchestrator.py:2586`](../../../app/services/bootstrap_orchestrator.py#L2586)) |
| `master_key.is_bootstrapped()` | `bootstrap(conn).state ∈ {"clean_install", "normal"}` — for "operator setup done?" use `SELECT 1 FROM operators` (`operators` row is the operator-setup landmark) |
| `coverage_audit()` from `app.services.coverage` | [`audit_all_instruments(conn)`](../../../app/services/coverage.py#L1018) |
| `SELECT COUNT(DISTINCT cik) FROM financial_facts_raw` | Table is keyed by `instrument_id`. Use `SELECT COUNT(DISTINCT instrument_id) FROM financial_facts_raw` or join through `external_identifiers WHERE provider='sec' AND identifier_type='cik'` |

## 6. Known live caveats / tech debt

- **Coverage = telemetry not gate**: per-category universe estimates still NULL for Tier 0 (`_read_universe_estimates` returns all-None). Banner reports `unknown_universe` on most instruments. Real estimates seeded in #790 / Batch 2.
- **AAPL institutional %**: under-reported on dev DB until universe-expansion sweep finishes. Operator audit 2026-05-04.
- **Funds slice coverage**: only 2020-CIK panel harvested before #963 directory walker. Sweep 2026-05-05 finished filling trust universe but per-CIK drain gated on monthly N-PORT job. For panel verification today, use `POST /jobs/sec_n_port_ingest/run`. **Workaround scripts at `.claude/*.py` are a tell that the standing job is broken — fix the job, don't extend the workaround.**
- **CI pytest job dropped (#928)**: pre-push hook is sole test gate.
- **AS-OF semantics**: `as_of_date` everywhere = period end, never fetch time. `ingested_at` is system-time watermark for repair sweep. `known_from`/`known_to` are valid-time. Don't mix.
- **N-PORT validation cliff (#932)**: EdgarTools' Pydantic `FundReport.parse_fund_xml` rejects synthetic test fixtures the bespoke parser tolerates. Bespoke stdlib-ElementTree parser remains shipped; rewrite parked.
- **Bootstrap reaches clean `complete` (redesigned pipeline, #1413/#1415/#1419)**: the bulk-only redesign runs end-to-end on dev to `bootstrap_state.status='complete'` in ~50 min, with `bootstrap_runs.validation_gate_status='warned'` (soft warnings, no hard-floor breach) and the 5-instrument ownership-rollup panel rendering SEC-sourced figures. The two non-pristine bits are **expected, not failures**: `coverage_floor_met=f` (dev CUSIP coverage is low → institutional totals lag; needs #841 universe/CUSIP expansion) and `stream_c_gate_status` only populated on the runbook trigger path, not the raw `POST /system/bootstrap/run` path. A `coverage_floor_met=t` + Stream-C-accepted run is gated on #841. Open follow-ups: P5 live timeline (#1409), P6 floor calibration. History in [`project_etl_readiness_audit.md`](../../../../../.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_readiness_audit.md) / [`project_bootstrap_etl_redesign_progress.md`](../../../../../.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_bootstrap_etl_redesign_progress.md).

## 7. Admin / ETL page — operator UX FAQ

This section is the answer key for "the admin processes page should behave like X — does it?" questions. Operator design intent (locked 2026-05-10):

### 7.1 Bootstrap-incomplete state — visibility

**Question:** "Bootstrap is in `partial_error` / `running` / not `complete`. Why am I seeing every other category disabled? They shouldn't even be on screen yet."

**Answer:** Operator decision — when `bootstrap_state.status != 'complete'`, the ProcessesTable hides every non-bootstrap category. Bootstrap is the only row visible, expanded to show its child stages. Other lanes (universe / candles / sec / ownership / fundamentals / ops / ai) are not just disabled — they are filtered out of the list entirely so the operator's only path forward is the bootstrap row.

Implementation surface: FE filter at `frontend/src/pages/AdminPage.tsx` or `useProcesses` consumer; BE `list_processes` continues returning all rows (BE stays mechanism-agnostic). The gate lives in the FE so bootstrap-as-overlay behaviour can change later without a schema migration.

### 7.2 Bootstrap stages — schedules

**Question:** "Why do bootstrap stages show a cadence? They're not scheduled."

**Answer:** They aren't. Bootstrap stages are a fixed sequence (init → etoro → sec_rate → sec_bulk_download → db lanes) declared in `_BOOTSTRAP_STAGE_SPECS`. They run when the orchestrator runs, period. Any cadence rendered next to a stage row is a FE bug. Stages render with a status indicator only ("pending" / "running" / "complete" / "failed" / "cancelled"); `cadence_human` / `cadence_cron` / `next_fire_at` must be omitted at render-time for `mechanism === 'bootstrap'`.

### 7.3 Bootstrap row — action verbs

**Question:** "The action buttons say 'Iterate' and 'Full-wash'. What do those mean for bootstrap?"

**Answer:** Wrong labels for bootstrap. Operator-locked verbs:

| Mechanism | Iterate label | Full-wash label |
|---|---|---|
| `scheduled_job` | "Iterate" (catch up since watermark) | "Full-wash" (reset watermark + re-fetch) |
| `bootstrap` | **"Re-run failed"** (resume incomplete + failed stages from where they stopped) | **"Re-run all"** (reset every stage to pending; full first-install replay) |
| `ingest_sweep` | (read-only — no buttons) | (read-only — no buttons) |

The underlying mechanics map: bootstrap "Re-run failed" = current `iterate` mode (`reset_failed_stages_for_retry` path); "Re-run all" = current `full_wash` mode (`start_run` flips bootstrap_state + every stage to pending). Only the labels change, not the fence-row + advisory-lock plumbing.

### 7.4 Cancel — bootstrap level wraps the running stages

**Question:** "Cancel today seems to be per-job. Bootstrap is N stages — I want one cancel that takes down whatever's running underneath."

**Answer:** Operator-locked: the bootstrap row's Cancel button is the canonical cancel for the entire run. Clicking it sets `bootstrap_runs.cancel_requested_at` (today already does this for the bootstrap process_id). The cancel signal must propagate to whichever stage's job is actively running — that propagation is the work in PR7 (#1064 follow-up sequence). Today the orchestrator checks `cancel_requested_at` between stages but does NOT signal a stage that's mid-flight. PR7 plumbs the signal through `_invoker_request_context` so the stage's checkpoint loop sees it and exits cooperatively.

UX expectation: when the bootstrap row says "running" and a stage is mid-flight, clicking Cancel on the bootstrap row should:
1. Mark `bootstrap_runs.cancel_requested_at`.
2. Signal the running stage (cooperative — no SIGKILL).
3. Stage exits at next checkpoint, marks itself `cancelled` (NOT `error` — see #1093).
4. Orchestrator sees the cancel between iterations, marks the run `cancelled`, every pending stage rolls forward as not-attempted.

`terminate` (SIGKILL) mode is hidden behind the More disclosure on the bootstrap row — same as scheduled jobs (#1092 fix wires the modal selection through to `cancel_mode`; today hardcoded `cooperative` at `bootstrap_state.py:742`).

### 7.5 Cancelled vs errored stages

**Question:** "After cancel, all the stages went red. They didn't error — I cancelled them. The Timeline doesn't tell me which is which."

**Answer:** Today `mark_run_cancelled` sweeps incomplete stages to `status='error'`. Wrong — they were never attempted (or were cooperatively interrupted). Operator-locked: cancelled stages need their own status value. Two paths considered:

1. **Tone-only synthesis (FE):** keep `status='error'` in DB but tone the row gray when the parent run is `cancelled`. Cheap. Loses the distinction in any direct DB query.
2. **New `status='cancelled'` on `bootstrap_stages`:** schema migration. Surfaces the distinction everywhere — FE, audit queries, runbook tooling. Costlier today.

Path 2 is the correct one. Filed as #1093. Tone synthesis is a stopgap if #1093 takes more than a sprint.

### 7.6 Why is everything on the page disabled when I just want to fix the bootstrap?

**Answer:** Two paths are conflated:

1. **Bootstrap-state gate** (`PR1b` / `PR1b-2`): when bootstrap != `complete`, scheduled jobs are gated — both scheduled fires AND manual triggers are rejected with `bootstrap_not_complete` reason. This is correct: you don't want `daily_candle_refresh` running while bootstrap is partway through `init`. Manual override exists (`?override_bootstrap_gate=true` + `decision_audit` audit row) for triage.

2. **FE rendering of disabled rows** (current bug): the page shows every category, all greyed out, with the "rejected" tooltip. Operator-correct UX hides them entirely (§7.1) — operator's sole path forward when bootstrap is broken should be the bootstrap row's "Re-run failed" / "Re-run all" buttons.

Today's UI shows you the locked door with a "no entry" sign on every category. The fix shows you only the bootstrap door. The gate logic doesn't change — only the rendering.

### 7.7 First-install bootstrap drains slowly — what's happening?

**Answer:** SEC bulk-download stages (12-15 in the sequence) fetch ~12,000 SEC per-CIK pages at a 10-req/s rate-limit. The bootstrap's `sec_first_install_drain` stage (#909 / PR1c) is the long pole — bounded by SEC's published rate limit, not eBull. Re-run all on a fresh DB is roughly 20+ minutes of bandwidth.

Operator-helpful surface (deferred, NOT scheduled): a pre-flight estimate "this will issue ~12,000 SEC requests over ~20 minutes" before kick-off. Tracked as a future operator-UX ticket; not blocking PR7.

### 7.8 Discovery-layer end-to-end smoke (Lane B — #1181)

**Use when:** verifying that Layer 1/2/3 discovery jobs are actually wired + firing + ingesting end-to-end. Authoritative smoke for "did the steady-state discovery path survive my change?"

**Pre-condition:** jobs process running. `bootstrap_state.status` may be incomplete (Layer 2 carve-out fires regardless; Layer 1/3 require `override_bootstrap_gate=true` when bootstrap is not complete).

For each of (`sec_atom_fast_lane`, `sec_daily_index_reconcile`, `sec_per_cik_poll`):

**Step 1 — Snapshot baseline.** Capture the last `run_id` (any status) so the next fire is provably ours, not a coincidental scheduled fire. COALESCE guards the zero-prior-rows case:

```sql
SELECT COALESCE(MAX(run_id), 0) FROM job_runs WHERE job_name = '<job_name>';
```

**Step 2 — Fire via manual queue.** Override is required when `bootstrap_state.status != 'complete'` AND the job is not exempt. Layer 2 fires without override regardless:

```text
POST /jobs/<job_name>/run
body: {"control": {"override_bootstrap_gate": true}}
```

**Step 3 — Confirm the fire succeeded.** Within 60s:

```sql
SELECT run_id, status, started_at, finished_at, row_count,
       params_snapshot, linked_request_id
FROM job_runs
WHERE job_name = '<job_name>' AND run_id > <baseline_run_id_from_step_1>
ORDER BY run_id DESC LIMIT 1;
```

Pass criteria: exactly one new row with `status='success'` AND populated `linked_request_id` matching the manual-queue request from step 2. `row_count` may be 0 (no new accessions to ingest is a valid success path for atom/daily-index in steady state); the proof is `status='success'` + `finished_at` populated.

**Step 4 — Discovery-attribution check (atom + daily-index only).** `sec_filing_manifest.source` records the parser-source enum NOT the discovery origin. Attribute via the run's started/finished window:

```sql
SELECT source, COUNT(*) AS new_rows
FROM sec_filing_manifest
WHERE created_at BETWEEN <run.started_at> AND <run.finished_at> + INTERVAL '5 seconds'
GROUP BY source;
```

Non-zero result during a known-active window proves discovery wrote rows. Zero result is INCONCLUSIVE for atom (no new accessions in 5 min is normal) — fall back to the jobs-process log line: `sec_atom_fast_lane: feed=X matched=Y upserted=Z ...`.

**Step 5 — Per-cik poll scheduler-write check (`sec_per_cik_poll` only).** Confirm the poll updated freshness scheduler state:

```sql
SELECT subject_type, source, last_polled_at, next_recheck_at
FROM data_freshness_index
WHERE last_polled_at BETWEEN <run.started_at> AND <run.finished_at> + INTERVAL '5 seconds'
LIMIT 5;
```

Expect non-zero rows when the poll had subjects in-budget.

**Step 6 — Confirm scheduled-fire registration.** Manual fire proves the invoker body works. Scheduled-fire registration is proved via:

```text
GET /system/jobs    (admin endpoint; reports next_run_time)
```

Expected `next_run_time`:

- `sec_atom_fast_lane`: within ~5 min.
- `sec_daily_index_reconcile`: next 04:00 UTC.
- `sec_per_cik_poll`: top of next hour.

**Step 7 — Wait for natural scheduled fire (atom + per-cik only).** For atom (5 min) and per-cik (hourly): wait one cadence, re-run step 3. A scheduled fire's row has `linked_request_id IS NULL` — that's the proof APScheduler is dispatching.

For daily-index (04:00 UTC): scheduled-fire confirmation is the registration check in step 6; waiting 24h is not viable. The full end-to-end is proved by (manual-fire success in step 3) + (registration populated in step 6); the carve-out (#1181) is what makes the next scheduled fire succeed regardless of `bootstrap_state`.

## 11. Integrity reference matrix — actual shapes per table

> When in doubt, this section is the source of truth, not memory. Source: grep of `sql/*.sql` 2026-05-10. If a table's actual shape diverges from this matrix, the matrix is stale — fix it before continuing.

### 11.A SEC filing-object tables — accession-keyed entity-level

| Table | PK | UNIQUE / UNIQUE INDEX | Children with FK pointing here | CHECK enum cols |
|---|---|---|---|---|
| `eight_k_filings` (sql/061) | `(accession_number)` | n/a | `eight_k_items.accession_number → eight_k_filings.accession_number ON DELETE CASCADE` (UNIQUE on `(accession_number, item_code)`); `eight_k_exhibits.accession_number → eight_k_filings.accession_number ON DELETE CASCADE` (UNIQUE on `(accession_number, exhibit_number)`) | n/a |
| `insider_filings` (sql/057) | `(accession_number)` | n/a | `insider_filers.accession_number → insider_filings.accession_number ON DELETE CASCADE` (UNIQUE on `(accession_number, filer_cik)`); `insider_transaction_footnotes.accession_number → insider_filings.accession_number ON DELETE CASCADE` (UNIQUE on `(accession_number, footnote_id)`); `insider_transactions.accession_number → insider_filings.accession_number` (PK on `(accession_number, txn_row_num)`); `insider_initial_holdings.accession_number → insider_filings.accession_number` (PK on `(accession_number, row_num)`) | n/a |
| `def14a_ingest_log` (sql/097) | `(accession_number)` | n/a | n/a | `status IN ('success','partial','failed')` |
| `n_port_ingest_log` (sql/125) | `(accession_number)` | n/a | n/a | (status enum — verify before write) |
| `sec_filing_manifest` (sql/118) | `(accession_number)` | n/a | self-FK `amends_accession → sec_filing_manifest.accession_number ON DELETE SET NULL` | `source IN ('sec_form3','sec_form4','sec_form5','sec_13d','sec_13g','sec_13f_hr','sec_def14a','sec_n_port','sec_n_csr','sec_10k','sec_10q','sec_8k','sec_xbrl_facts','finra_short_interest')`; `subject_type IN ('issuer','institutional_filer','blockholder_filer','fund_series','finra_universe')`; `ingest_status IN ('pending','fetched','parsed','tombstoned','failed')`; `raw_status IN ('absent','stored','compacted')`; CHECK `(subject_type='issuer' AND instrument_id IS NOT NULL) OR (subject_type<>'issuer' AND instrument_id IS NULL)` |
| `filing_raw_documents` (sql/107) | `(accession_number, document_kind)` | n/a | n/a | `document_kind` enum (extended in sql/122 to add `nport_xml` etc.) |
| `cik_raw_documents` (sql/109) | (verify before write) | (verify) | (verify) | (verify) |
| `sec_reference_documents` (sql/121) | `(document_kind, period_year, period_quarter)` | n/a | n/a | (verify) |

**Implication:** `eight_k_filings` and `insider_filings` PK relaxation to `(accession, instrument_id)` is BLOCKED by their child tables' accession-only FKs. If you need per-sibling visibility, route reads through `filing_events` (the per-instrument bridge — see §11.C). Do NOT propose PK changes here without rewriting every child FK.

### 11.B SEC bridge / per-instrument tables

| Table | PK | UNIQUE / UNIQUE INDEX | Children with FK | CHECK enum cols |
|---|---|---|---|---|
| `filing_events` (sql/001 + sql/004) | `(filing_event_id)` BIGSERIAL | `uq_filing_events_provider_unique` UNIQUE on `(provider, provider_filing_id)` | n/a | n/a |
| `external_identifiers` (sql/003) | `(external_identifier_id)` BIGSERIAL | (post-sql/143) partial UNIQUE INDEX `uq_external_identifiers_provider_value_non_cik` on `(provider, identifier_type, identifier_value) WHERE NOT (provider='sec' AND identifier_type='cik')` + partial UNIQUE INDEX `uq_external_identifiers_cik_per_instrument` on `(provider, identifier_type, identifier_value, instrument_id) WHERE provider='sec' AND identifier_type='cik'` | n/a | (verify identifier_type enum) |
| `def14a_beneficial_holdings` (sql/097) | `(holding_id)` BIGSERIAL | `uq_def14a_holdings_accession_holder` UNIQUE INDEX on `(accession_number, holder_name)` | n/a | n/a |
| `insider_transactions` (sql/056 + sql/057) | `(accession_number, txn_row_num)` | n/a | (FK to `insider_filings`) | n/a |
| `insider_initial_holdings` (sql/093) | `(accession_number, row_num)` | n/a | (FK to `insider_filings`) | n/a |
| `institutional_holdings` (sql/090) | `(holding_id)` BIGSERIAL | partial UNIQUE INDEX on `(accession_number, instrument_id, COALESCE(is_put_call,'EQUITY'))` | n/a | (verify) |
| `institutional_filers` (sql/090) | `(institutional_filer_id)` BIGSERIAL | UNIQUE `(cik)` | n/a | `filer_type IN ('ETF','INV','INS','BD','OTHER')` |
| `blockholder_filers` (sql/095) | `(blockholder_filer_id)` BIGSERIAL | UNIQUE `(cik)` | n/a | (verify) |
| `blockholder_filings` (sql/095) | (verify) | (verify) | (verify) | (verify status_flag enum) |
| `unresolved_13f_cusips` (sql/099) | (verify) | (verify) | n/a | `resolution_status IN ('unresolvable','ambiguous','conflict','manual_review','resolved_via_extid')` |
| `data_freshness_index` (sql/120) | `(subject_type, subject_id, source)` | n/a | n/a | (state enum: `unknown,current,expected_filing_overdue,never_filed,error`) |
| `instrument_cik_history` (sql/102) | (BIGSERIAL) | `btree_gist` exclusion forbids overlapping `(instrument_id, daterange(effective_from, effective_to))` ranges; partial UNIQUE INDEX on `(instrument_id) WHERE effective_to IS NULL` | n/a | n/a |
| `instrument_symbol_history` (sql/103) | same shape as above | same | n/a | n/a |
| `sec_fund_series` (sql/124) | `(fund_series_id)` | n/a | n/a | regex CHECK `fund_series_id ~ '^S[0-9]{9}$'` |
| `sec_nport_filer_directory` (sql/126) | `(cik)` | n/a | n/a | (verify) |

**Implication:** `def14a_beneficial_holdings` UNIQUE is `(accession_number, holder_name)`, NOT `(instrument_id, accession, holder_name)` as the spec author might assume. Per-instrument fan-out requires a UNIQUE shape change.

### 11.C Two-layer ownership tables

All `ownership_*_observations` partitioned `RANGE(period_end)` quarterly 2010-2030 + `_default` (must stay empty post-backfill).

| Table | Per-instrument | Identity (`_current`) | source CHECK |
|---|---|---|---|
| `ownership_insiders_observations` / `_current` (sql/113) | yes | `(instrument_id, holder_identity_key, ownership_nature)` | source ∈ canonical list |
| `ownership_institutions_observations` / `_current` (sql/114) | yes | `(instrument_id, filer_cik, ownership_nature, exposure_kind)` | `source='13f'` |
| `ownership_blockholders_observations` / `_current` (sql/115) | yes | `(instrument_id, reporter_cik, ownership_nature)` | `source IN ('13d','13g')` |
| `ownership_treasury_observations` / `_current` (sql/116) | yes | `(instrument_id)` | `source='xbrl_dei'` |
| `ownership_def14a_observations` / `_current` (sql/116) | yes | `(instrument_id, holder_name_key, ownership_nature)` | `source='def14a'` |
| `ownership_funds_observations` / `_current` (sql/123) | yes | `(instrument_id, fund_series_id)` | `source IN ('nport','ncsr')` |
| `ownership_esop_observations` / `_current` (sql/127) | yes | `(instrument_id, plan_name)` | `source='def14a'`, `ownership_nature='beneficial'`, `shares > 0` |

Canonical observations source enum (used in `record_*_observation` writers): `form4, form3, 13d, 13g, def14a, 13f, nport, ncsr, xbrl_dei, 10k_note, finra_si, derived`. Do not write a value not in this list — the per-table CHECK rejects.

### 11.D What "matrix is stale" looks like

Examples of integrity drift the matrix MUST catch before spec writing:

- "I'll change `ON CONFLICT (accession_number, holder_name)` to `(instrument_id, accession_number, holder_name)`." — Cross-check matrix §11.B before writing the spec; you'll see the actual current UNIQUE shape, not paraphrased.
- "I'll promote `eight_k_filings` PK to `(accession, instrument_id)`." — §11.A surfaces every accession-only child FK; you immediately see the cascade. Pivot to `filing_events`-bridge read pattern (§11.B) instead.
- "I'll write `source='sec_submissions'` on a manifest seeder INSERT." — §11.A enum list rejects `'sec_submissions'`; canonical mapping is `sec_form4`, `sec_def14a`, `sec_8k`, etc. via `app/services/sec_manifest.py::map_form_to_source(form)`.

If your spec proposal touches a table not yet in this matrix, ADD the row before writing the spec. The matrix grows; it does not shrink.

## 12. Canonical patterns — name, code, callers

### 12.A Canonical CIK pick (from external_identifiers)

When you need a single canonical `external_identifier_value` for an instrument:

```sql
SELECT identifier_value
  FROM external_identifiers
 WHERE provider = 'sec' AND identifier_type = 'cik'
   AND instrument_id = %s
 ORDER BY is_primary DESC, external_identifier_id ASC
 LIMIT 1
```

`is_primary DESC` prefers the primary mapping. `external_identifier_id ASC` is the deterministic tie-breaker — handles legacy multi-primary edge AND non-primary-only mappings. Used by [app/services/filings.py::upsert_cik_mapping](../../../app/services/filings.py) (and post-#1091 every site that needs a canonical CIK). Never `LIMIT 1` without the explicit ORDER BY.

### 12.B Canonical sibling-fan-out lookup (post-#1102)

Source-of-truth rule: [sec-edgar §3.6](../data-sources/sec-edgar.md). One-line summary: an issuer-scoped filing populating a per-instrument table fans out across every instrument sharing the issuer CIK; entity-level tables stay PK=accession.

When you need EVERY instrument sharing an issuer CIK (for per-instrument fan-out writes):

```python
def siblings_for_issuer_cik(conn, cik: str) -> list[int]:
    cik_padded = str(cik).strip().zfill(10)
    if not cik_padded.isdigit():
        raise ValueError(f"non-numeric CIK: {cik!r}")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id
            FROM external_identifiers
            WHERE provider = 'sec'
              AND identifier_type = 'cik'
              AND identifier_value = %s
            ORDER BY instrument_id
            """,
            (cik_padded,),
        )
        return [int(r[0]) for r in cur.fetchall()]
```

Lives in `app/services/sec_identity.py` (post-#1117). Ordering is deterministic, NOT semantically primary — caller fans out across the full list. For "single canonical sibling" use §12.A on instrument_id ASC.

### 12.C Manifest source mapping (form → source enum)

Lives in [`app/services/sec_manifest.py::map_form_to_source`](../../../app/services/sec_manifest.py). Returns one of the §11.A enum values OR `None` for unsupported forms (`S-1`, `424B5`, etc. — discovery paths must skip).

```python
from app.services.sec_manifest import map_form_to_source

source = map_form_to_source(filing_type)
if source is None:
    continue  # unsupported form — skip
```

NEVER hardcode `'sec_submissions'` or any other string not in the §11.A enum. The CHECK rejects.

### 12.D Migration shape-check (idempotent re-runnability)

Name-only constraint checks miss partial-applies. Use shape introspection:

```sql
-- For UNIQUE / PK constraints (pg_constraint):
DO $$
DECLARE
    has_correct_shape BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1 FROM pg_constraint c
        WHERE c.conname = '<name>'
          AND c.contype = 'u'  -- 'u' UNIQUE, 'p' PRIMARY KEY, 'c' CHECK, 'f' FK
          AND c.conrelid = '<table>'::regclass
          AND (
              SELECT array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum))
                FROM pg_attribute a
               WHERE a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
          ) = ARRAY['<col1>','<col2>',...]::name[]
    ) INTO has_correct_shape;

    IF NOT has_correct_shape THEN
        ALTER TABLE <table> DROP CONSTRAINT IF EXISTS <name>;
        ALTER TABLE <table> ADD CONSTRAINT <name> UNIQUE (<col1>, <col2>, ...);
    END IF;
END$$;

-- For CREATE UNIQUE INDEX (pg_index + pg_class):
DO $$
DECLARE
    has_correct_shape BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1
          FROM pg_index i
          JOIN pg_class c ON c.oid = i.indexrelid
         WHERE c.relname = '<index_name>'
           AND i.indrelid = '<table>'::regclass
           AND i.indisunique
           AND (
               SELECT array_agg(a.attname ORDER BY array_position(i.indkey::int[], a.attnum::int))
                 FROM pg_attribute a
                WHERE a.attrelid = i.indrelid
                  AND a.attnum = ANY(i.indkey::int[])
           ) = ARRAY['<col1>','<col2>',...]::name[]
    ) INTO has_correct_shape;

    IF NOT has_correct_shape THEN
        DROP INDEX IF EXISTS <wrong_name>;
        DROP INDEX IF EXISTS <index_name>;
        CREATE UNIQUE INDEX <index_name> ON <table> (<col1>, <col2>, ...);
    END IF;
END$$;
```

Why: a partial-apply leaves the constraint name pointing at a WRONG-shape constraint. Name-only checks falsely skip. Shape-introspection re-runs cleanly.

### 12.E Atomic _current refresh

```python
with conn.cursor() as cur:
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtext('<category>:%s' % instrument_id))"
    )
    cur.execute("DELETE FROM ownership_<cat>_current WHERE instrument_id = %s", (iid,))
    cur.execute(
        """
        INSERT INTO ownership_<cat>_current (...)
        SELECT DISTINCT ON (<identity-tuple>) ...
        FROM ownership_<cat>_observations
        WHERE instrument_id = %s AND known_to IS NULL
        ORDER BY <identity-tuple>, <winner-priority-clause>
        """,
        (iid,),
    )
```

Winner-priority order across categories: `source` priority (`form4 > form3 > 13d > 13g > def14a > 13f > nport > ncsr`) → `period_end DESC` → `filed_at DESC` (amendments win) → `source_document_id ASC`. Wrapped in `pg_advisory_xact_lock(<hash of instrument_id>)` so concurrent refreshes serialise; PK on `_current` is second-line guard.

### 12.F Forbidden patterns — these are PR review BLOCKING

- `ON CONFLICT (...) DO UPDATE SET <pk_col> = EXCLUDED.<pk_col>` — overwriting a column that's now in the conflict target is a smell. Hit means already correct; the SET drives nothing useful.
- `LIMIT 1` without `ORDER BY` — leaks nondeterminism; #1117 caught one in `rewash_filings`. Always pair LIMIT with explicit ORDER BY.
- `f-string interpolation into SQL` — parameterise. See §I2.
- `record_*_observation()` without paired `refresh_*_current(instrument_id)` — leaves `_current` empty (prev-log L1162). Both are mandatory.
- `ON DELETE CASCADE` on `*_audit` / `*_log` (prev-log L350).
- `SELECT DISTINCT` over a multi-column row when the dedup target is one column — use `DISTINCT ON (col)`.
- `dict[cik, instrument_id]` for any CIK→instrument lookup post-#1102 — must be `dict[cik, list[instrument_id]]` (the multimap pattern from #1117). Single-result collapse drops share-class siblings silently.

## 13. Per-source retention horizon

eBull doesn't keep every historical filing forever. Each source has a backfill horizon (what bootstrap drains) and a steady-state cadence (how the scheduled job refreshes). Steady-state only fetches NEW accessions discovered via atom / daily-index / per-CIK poll — no re-fetch of in-horizon data unless explicit rewash.

| Source | Backfill | Steady-state cadence | Storage budget per instrument |
|---|---|---|---|
| Form 4 / 3 / 5 | last 2 years | atom + daily-index | ~50-200 rows |
| 13F-HR | last 4 quarters | quarterly bulk sweep | ~4 quarterly rollups |
| N-PORT | last 4 quarters | monthly bulk + atom | ~4 quarterly rollups |
| 13D / 13G | last 2 years | atom + daily-index | ~5-20 rows |
| DEF 14A | last 2 years | atom + daily-index | ~1-2 proxy seasons |
| 8-K | last 2 years | atom | ~30-100 events |
| 10-K | last 3 annual | atom + daily-index | 3 filings |
| 10-Q | last 8 quarterly | atom + daily-index | 8 filings |
| FINRA short interest | last 2 years | bi-monthly per FINRA cadence | ~48 bi-monthly rows |
| Form 144 | rolling 90 days (effective ≤ 90d post-filing) | atom | ephemeral |
| SC 13E | last 2 years | atom + daily-index | rare event filings |

### 13.A Backfill horizon enforcement

The horizon is enforced at the **discovery layer**, not at the parser layer. Discovery sites (`check_freshness`, daily-index walker, per-CIK poll) filter by `filed_at >= cutoff` so older accessions never make it into the manifest. Once bootstrap completes, the steady-state poll's `last_known_filing_id` watermark naturally restricts to new accessions.

**Rule:** when adding a new source, the discovery layer MUST apply the cutoff before writing to `sec_filing_manifest`. Filtering at the parser layer (i.e. discovery writes everything; parser drops old) is forbidden because it leaves dead manifest rows that the `/coverage/manifest-parsers` audit counts as `pending`.

### 13.B Rewash retention exception

A parser-version bump triggers re-parse of in-horizon raw bodies (`filing_raw_documents`). Out-of-horizon bodies are NOT retained; if a rewash needs older accessions, the operator runs targeted `sec_rebuild` with `discover=True` to re-fetch from SEC. This pattern is documented in [docs/settled-decisions.md](../../../docs/settled-decisions.md).

### 13.C Adding a new source — derivable from this rule

`(filing size per instrument) × (rows per instrument over the horizon)` must fit the storage budget for the universe (~12k tradable instruments). A source with 100k filings per instrument over 2 years would blow the budget — pick a shorter horizon or a coarser steady-state cadence. The horizon values in the table above are derived from this constraint; do not change them without re-computing the budget for that source.

### 13.D Storage-side enforcement — `financial_facts_raw` retention sweep (#1208 Phase 3)

Discovery-layer enforcement (§13.A) keeps OLD accessions out of the manifest, but it does NOT bound the size of the parser-layer landing tables that already hold years of historical residue. For `financial_facts_raw` specifically, the 10-K = 3-annual + 10-Q = 8-quarterly horizons are also enforced on the storage side by `app/services/financial_facts_retention.py::sweep_retention_for_instrument`, registered as the daily `ScheduledJob` `financial_facts_retention_sweep` (02:45 UTC, gated on `_bootstrap_complete`).

Implementation details that any future storage-side retention sweep MUST mirror:

- **Family-level retention.** 10-K and 10-K/A share the ANNUAL family budget (3); 10-Q and 10-Q/A share the QUARTERLY family budget (8). XBRL amendments supersede the original — they consume the same slot.
- **DISTINCT-accession ranking, not per-row.** A 10-K filing emits hundreds of facts under one accession. Rank DISTINCT accessions then DELETE all facts of out-of-horizon accessions (NOT individual facts — ranking rows would evict facts 4..N of the latest filing and break the accession's atomicity). The Codex 1a BLOCKING #3 finding on the Phase 3 spec is the regression test for this rule.
- **Service-no-commit + autocommit orchestrator.** The per-instrument sweep is a service function that takes a conn and does NOT enter `with conn.transaction()`; the orchestrator opens `psycopg.connect(url, autocommit=True)` then iterates instruments with `with conn.transaction()` per instrument so each becomes a real top-level tx (Codex 1b BLOCKING #2 on the Phase 3 spec). Defeats the SAVEPOINT trap that Phase 1 prevention-log §"psycopg3 service-no-commit invariant" warns about.
- **Idempotent.** Second-pass deletes 0 rows. Verified in `tests/test_financial_facts_retention.py::test_idempotent_second_run_deletes_zero`.

When ADDING a new storage-side retention sweep for another mega-table (`filing_raw_documents`, `filing_events`, etc.) follow the same pattern: a `<table>_retention_sweep` service + a daily `ScheduledJob` wired through `_INVOKERS` + the §13 horizon table in this file picks up the new row.

### 13.E Operator-visible health surface (#1208 Phase 4)

`GET /system/postgres-health` is the live readout for the storage discipline this section enforces. Returns per-poll:

- `db_size_*`: `pg_database_size('ebull')` against the 10 GB warn threshold (matches the pre-push hook bloat warn in `.githooks/pre-push`).
- `leaked_test_db_*`: count + names of leaked `ebull_test_*_gw*` databases (Phase 2 sweep target — should be zero).
- `wal_dir_*` + `wal_since_checkpoint_*`: WAL retention + burst pressure against `max_wal_size=4 GB` (Phase 1 tuning gate).
- `last_checkpoint_at`: most recent PG checkpoint.
- `autovacuum_top10`: tables sorted by `n_dead_tup` (Phase 3 partition motivation — should stay <5% dead_fraction for active partitions).
- `financial_facts_raw_default_*`: row count against the 5000-row growth alarm (parser-junk early-warning).

Every metric is nullable + every breach flag is nullable; a failed probe (e.g. `pg_ls_waldir()` requires `pg_monitor` role on a non-superuser DB) returns `null` rather than `0` so a silent collection failure can't masquerade as "all clear". The `metric_errors` field lists which probes failed for ops triage.

When adding a new storage discipline rule (retention, sweep, partition retrofit), wire its operator-visible signal into this endpoint AND into `.githooks/pre-push` if it's a push-time concern. The two surfaces share a single threshold constant (`DB_SIZE_WARN_BYTES` is the working example) so an operator never sees one surface clean while the other is breached.

## 14. Postgres crash-recovery fsync tax — diagnosis runbook (#1444, 2026-06-02)

**Symptom:** dev PG stuck in `the database system is in recovery mode` for tens of minutes to HOURS, rejecting every connection. Blocks bootstrap, live verification, DB-backed tests.

**It is almost never WAL replay.** PG 17 after an unclean shutdown runs a **full-data-directory fsync** before accepting connections — `LOG: syncing data directory (pre-fsync), elapsed time: NNNN s, current path: ./base/<oid>/...`. This pass `fsync()`s **every file** in PGDATA. It is **file-count-bound, not data-size-bound**: 41 GB across 30M tiny files is far slower than 200 GB across 50k files. It IS progressing if `elapsed time` climbs across log lines; it is not deadlocked, just slow.

**The bloat source is almost always leaked test worker DBs, NOT `ebull`.** Each `kill -9`'d xdist worker / interrupted migration-replay can leave a multi-million-relation `ebull_test_*_gwN` / `ebull_mig*` database on disk (TRUNCATE-only per-test cleanup means a test that `CREATE`s relations without dropping accumulates them across the whole session; `kill -9` skips the teardown that would trip the 50k `_assert_worker_relations_under_ceiling`). Real `ebull` is ~10-15k files (instruments + bounded `RANGE(period_end)` partitions + indexes). A 6-10M-file base dir is a leaked test DB. **Confirmed 2026-06-02:** 4 leaked DBs (OIDs 33553986-89, consecutive = gw0-3) at 6-10M files each = ~30M files = multi-hour recovery; `ebull` (OID 58791433) was a healthy 10,889 files.

### Diagnosis commands (read-only — safe while PG is down)

```bash
# 1. Confirm it's the fsync pass, not WAL replay (look for "syncing data directory"):
docker logs ebull-postgres --tail 40 2>&1 | grep -i "syncing data directory\|redo\|recovery"

# 2. Per-DB file counts — find the bloated dir(s):
docker exec ebull-postgres bash -lc 'for d in /var/lib/postgresql/data/base/*/; do printf "%s " "$(basename $d)"; ls -f "$d" | wc -l; done' | sort -k2 -rn | head

# 3. Confirm bloat = permanent relations, not temp (sample filenames):
#    temp files start t<backend>_ ; permanent main forks are plain-numeric.
docker exec ebull-postgres bash -lc "ls -f /var/lib/postgresql/data/base/<oid> | head -300000 | awk '/^t[0-9]/{t++} /^[0-9]+$/{n++} END{print \"temp=\"t\" permanent=\"n}'"
```

`ebull`'s OID = the one with ~10-15k files (NOT one of the multi-million outliers). Disambiguates "production schema leak" (rare, serious) from "orphaned test DB" (common, the usual culprit).

### Remediation

1. **Do NOT `docker restart` / `docker compose up -d` to "fix" a slow recovery** — both recreate/restart the container and the official `postgres` entrypoint then runs `find /var/lib/postgresql/data ! -user postgres -exec chown postgres` over EVERY file (a D-state disk walk, minutes at high file count) BEFORE the postmaster even starts, then PG still does WAL redo on top. Confirmed #1444 (2026-06-02): a recreate to clear a wedged `datconnlimit=-2` corpse cost ~270s chown + ~8min WAL redo (the killed full-suite had written GBs of WAL). `syncfs` removes the per-file *fsync* walk but NOT the entrypoint chown and NOT WAL redo. Restart/recreate is the right move ONLY when PG is genuinely wedged (a corpse `WITH (FORCE)` won't drop, advisory lock stuck) — not to speed up a recovery that is already progressing. Let an in-progress recovery finish.
2. **After PG accepts connections:** `uv run python -m tests.fixtures.cleanup_test_dbs` — force-drops every `ebull_test_*` / `ebull_mig*` (WITH FORCE, clears `datconnlimit=-2` corpses too) except `ebull_test_template`. Removes the orphan files → next crash-recovery drops from hours to seconds.

### Reaper gaps that let this recur (`tests/fixtures/ebull_test_db.py`)

- `_assert_worker_relations_under_ceiling` (50k) runs in **teardown** — `kill -9` skips it. Not a kill-9-proof backstop.
- `_force_drop_invalid_test_dbs` drops only `datconnlimit=-2` corpses.
- `_drop_orphan_workers_older_than` uses **plain DROP** (no FORCE) → a held connection blocks it; and both reapers run **only at test-session start**, which can't happen while PG is in recovery (chicken-and-egg).

**The single biggest dead-time lever (Codex-validated against PG17 docs): `recovery_init_sync_method=syncfs`.** Default is `fsync`, which "recursively opens and synchronizes ALL files" under PGDATA — the per-file walk that costs hours at 30M files. `syncfs` issues one `syncfs()` per filesystem instead, skipping the per-file open. Set it on the dev Docker Postgres (`command: -c recovery_init_sync_method=syncfs` in `docker-compose.yml`, or `postgresql.conf`). Linux-only; acceptable for single-volume dev. This makes future crash recovery seconds regardless of file count — it does NOT remove the need to drop orphans (catalog bloat, planner cost) but removes the recovery stall.

Durable fix (#1444 — SHIPPED, see locations):
- ✅ **`recovery_init_sync_method=syncfs`** on dev PG — `docker-compose.yml` `command:` block (the direct fix for recovery dead-time; makes the next crash recovery seconds regardless of file count).
- ✅ **Canonical reaper** `app/db/dev_test_db_reaper.py` is the single source of truth for the safety rails (name regex + `NEVER_DROP`). The test fixture (`tests/fixtures/ebull_test_db.py`) imports the constants + delegates its sweep to it (`app` must not import `tests` — rails live under `app/`, fixture consumes them). **FORCE vs plain DROP is load-bearing** (a #1444 Codex BLOCKING-equivalent): live-capable orphans (`datconnlimit=-1`) use **plain `DROP`** in `sweep_orphan_test_databases` — the activity rail proves no backend only at snapshot time, so a sibling that connects in the snapshot→DROP gap raises `ObjectInUse` and is skipped, never evicted (#1208 invariant). `DROP ... WITH (FORCE)` is reserved for `datconnlimit=-2` **corpses** (`force_drop_invalid_test_dbs`), which refuse ALL connections (superuser included) so have no live sibling to kill. `REVOKE CONNECT` is NOT a guard in a `postgres`-user dev cluster (owner/superusers bypass `CONNECT`).
- ✅ **Run on a cadence + at jobs-process start:** `run_orphan_test_db_reap()` (dev-only via `app_env`; hard no-op in prod) called at jobs boot (`app/jobs/__main__.py` Step 10b) + daily `ScheduledJob` `orphan_test_db_reap` (`app/workers/scheduler.py`, 03:15 UTC). Breaks the chicken-and-egg where the test-session-start sweep can't run while PG is wedged in recovery over the very bloat it would clear.
- ✅ **Creation-time relation budget:** `_assert_worker_relations_under_ceiling` now runs at fixture **setup** too (`ebull_test_conn`), not only teardown — a `kill -9` skips teardown, so the first surviving test after a skipped teardown fails fast + names the worker DB, bounding per-session accumulation.
- ✅ **Bloat alarm on `/system/postgres-health`:** `leaked_test_db_total_bytes` / `_pretty` (`app/services/postgres_health.py::_q_leaked_test_db_bytes`, `statement_timeout='2s'`-guarded so a `datconnlimit=-2` corpse hang surfaces as a caught timeout, not a wedge). Size is the connection-free proxy — per-DB relation count would need connecting to each leaked DB (the #1393 hang). Joins the existing `leaked_test_db_count` / `_names`.
- ⏳ **Find the test(s) that `CREATE` relations without dropping** — still open (tracked separately). The now-doubled tripwire (setup + teardown) NAMES the culprit worker DB on the next occurrence; fixing requires that reproduction signal. Migration-replay tests (`ebull_mig*`) create separate DBs (reaper-handled), NOT the worker-DB relation leak. Suspect: partition-creating tests accumulating across the session.

Do NOT: manually `rm base/<oid>`, `pg_resetwal`, or `fsync=off` — those risk whole-cluster loss. The safe shortcut for THIS recovery is to let it finish; `syncfs` is for NEXT time.

### 14.1 The OTHER recovery failure mode — replay-memory OOM crash-loop (#1447, RCA 2026-06-03)

§14 above is about the **fsync file-walk** stall (slow, but it finishes). There is a second, worse mode that syncfs does NOT fix: **the recovery startup process gets OOM-killed during WAL *replay* and `restart: unless-stopped` loops it silently.** Full RCA: `docs/proposals/etl/2026-06-03-pg-recovery-oom-rca.md`.

**Signature** (distinguish from §14): `docker logs` shows `startup process … terminated by signal 9: Killed` repeating; `docker inspect --format '{{.RestartCount}}'` climbs; `pg_controldata -D /var/lib/postgresql/data` shows state `in crash recovery` with a **frozen** "Latest checkpoint's REDO location" across attempts. Each restart replays the same span and dies at the cgroup limit.

**Mechanism (multi-agent-verified, corrects a natural wrong guess):**
- The OOM driver is **NOT `shared_buffers`** (it only ever faults ~720 MB during recovery; lowering it frees ~0). It is **per-relation memory** (relcache / smgr / pending-sync + kernel dentry/inode slab) **proportional to the number of relations touched in the replayed WAL.** ~700k leaked test-DB relations ⇒ multi-GB replay memory ⇒ exceeds the 6 g cgroup.
- A **restartpoint** (which would flush + bound memory mid-replay) can only anchor on a **completed checkpoint record** in the WAL. A killed full-suite wrote ~20 GB WAL with **no completed checkpoint** in it ⇒ zero restartpoints ⇒ memory grows monotonically across the whole replay.
- `restart: unless-stopped` turns the OOM into an **invisible infinite loop** (observed 18 h, RestartCount 19): recovery never completes ⇒ no new checkpoint ⇒ redo pointer never advances ⇒ identical replay next attempt.

**Remediation when you see it:** there is NO knob under the 6 g cap that replays a 700k-relation WAL. Do not gamble on `shared_buffers=512MB` recover-in-place — it moves the kill point ~50 segments, not to the finish. **Wipe + re-bootstrap** is the deterministic path (the leaked relations live in `ebull`'s WAL, so dropping `base/<oid>` offline is unsafe; `pg_resetwal` is data-loss). A clean bootstrap is usually wanted anyway.

**Durable prevention (shipped #1447):**
- **C1 — the structural fix: the pytest suite runs on a SEPARATE cluster** (`docker-compose.yml` service `postgres-test`, **disk volume `pgdata_test`**, port 5433, profile `test`, `fsync`/`full_page_writes`/`synchronous_commit` off, `restart: on-failure:3`). Its WAL lives in its own `pg_wal` and **can never enter `ebull`'s crash recovery** — and if the test cluster itself wedges, it is isolated, wipeable (`docker compose rm -sfv postgres-test`), and surfaces via `on-failure:3` instead of looping. (DISK, not tmpfs: tmpfs charges every byte of test-DB data to the container memory cgroup, so parallel xdist worker-DB clones OOM a small `mem_limit` — verified during build, a tmpfs cluster OOM-killed at 2 g mid-suite and recursed into its own recovery OOM. Disk keeps DB data off RAM so `mem_limit` only bounds process/recovery memory.) The suite resolves its base URL via `tests/fixtures/ebull_test_db.py::_test_cluster_base_url()` (default = dev URL with port→5433; override `EBULL_TEST_DATABASE_URL`); `_assert_not_dev_cluster()` (loopback-alias-aware) fails loud if anything re-couples them; the orphan reaper is pinned to 5433 via its `admin_url` arg. The pre-push hook auto-starts it.
- **D2 — `restart: on-failure:5` (NOT `unless-stopped`) + a `pg_isready` healthcheck** (`start_period: 90m`, NOT wired to auto-restart). Converts a silent infinite loop into a capped, `STATUS=unhealthy`-visible failure.
- `shared_buffers` stays 2 GB (lowering it targets the wrong term — see above).
- Sibling guards from #1444 (reaper, 50k CREATE-time relation ceiling) remain but are NOT sufficient alone: the reaper can't run during recovery; the ceiling is skipped on `kill -9`. C1 is the one that closes it.
