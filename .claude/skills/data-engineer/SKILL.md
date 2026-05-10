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
| `unresolved_13f_cusips` | sql/099 | Buffer for 13F-supplied CUSIPs not yet resolved to instruments |

**No dedicated `cusip_map` table.** CUSIP→instrument lives in `external_identifiers WHERE provider='sec' AND identifier_type='cusip'`. Promotion path: 13F-HR ingest → unresolved → CUSIP backfill (#914 weekly) writes `external_identifiers` → `cusip_resolver.sweep_resolvable_unresolved_cusips` rewashes the source 13F (`app/services/cusip_resolver.py:425+`).

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

**What write-through does NOT update**:
- Legacy typed tables (`institutional_holdings`, `def14a_beneficial_holdings`, `insider_transactions`, `insider_initial_holdings`, `blockholder_filings`) — still written by ingesters, NOT read by rollup post-#905. Survive for chart history + drift detection.
- `data_freshness_index` — separate write-side via `record_poll_outcome`.
- `sec_filing_manifest.parser_version` and `raw_status` — bumped by manifest worker / parser, not by `record_*_observation`.

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
- `sec_manifest_worker.py` — pulls `pending` + `failed AND next_retry_at<=NOW()` from manifest, dispatches to parsers.
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
- `docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md`
- `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md` (Phase 1 + 3)
- `docs/superpowers/specs/2026-05-04-etl-coverage-model.md` (manifest + freshness + 3-tier polling)
- `docs/superpowers/specs/2026-05-06-def14a-bene-table-extension-design.md` (#843 ESOP)
- `docs/superpowers/specs/2026-05-07-first-install-bootstrap.md` (#993)
- `docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md`

**Settled decisions**: `docs/settled-decisions.md`.
**Review prevention log**: `docs/review-prevention-log.md`.

## 6.5. Pipeline orchestration — invariants

> **State:** This section describes the **post-#1064 target state.** PR1 introduces source-level `JobLock` + `ParamMetadata` + `params_snapshot`; PR3 unifies the `bootstrap_state` gate across scheduled-fire and manual-trigger paths. Pre-PR1 reality: `JobLock` keys on `job_name`, scheduled jobs are zero-arg, and manual `/processes/{id}/trigger` bypasses prerequisites. The "Pre-PRn history" notes below mark each transition.
>
> Read before adding a new scheduled job, bootstrap stage, or operator-exposed parameter on an existing job. Cross-reference: [`docs/wiki/job-registry-audit.md`](../../../docs/wiki/job-registry-audit.md) for the per-job parameter surface; [`docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md`](../../../docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md) for the umbrella decisions.

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

**Bootstrap dispatcher concurrency vs JobLock — separate mechanisms.** The bootstrap orchestrator's `_LANE_MAX_CONCURRENCY` map ([`bootstrap_orchestrator.py:98`](../../../app/services/bootstrap_orchestrator.py#L98)) historically allowed up to 5 parallel `db`-lane stages WITHIN a single bootstrap run. That is a dispatcher knob, not a `JobLock` semantic. Under PR1 source-level locking, same-source jobs serialise across the entire process — the dispatcher's lane-concurrency map is either retired or reinterpreted as a "max queued before the lock kicks in" hint. The locked decision is unambiguous: same-source = serialised at the lock; the lane-concurrency map does not override this.

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

## 6. Known live caveats / tech debt

- **Coverage = telemetry not gate**: per-category universe estimates still NULL for Tier 0 (`_read_universe_estimates` returns all-None). Banner reports `unknown_universe` on most instruments. Real estimates seeded in #790 / Batch 2.
- **AAPL institutional %**: under-reported on dev DB until universe-expansion sweep finishes. Operator audit 2026-05-04.
- **Funds slice coverage**: only 2020-CIK panel harvested before #963 directory walker. Sweep 2026-05-05 finished filling trust universe but per-CIK drain gated on monthly N-PORT job. For panel verification today, use `POST /jobs/sec_n_port_ingest/run`. **Workaround scripts at `.claude/*.py` are a tell that the standing job is broken — fix the job, don't extend the workaround.**
- **CI pytest job dropped (#928)**: pre-push hook is sole test gate.
- **AS-OF semantics**: `as_of_date` everywhere = period end, never fetch time. `ingested_at` is system-time watermark for repair sweep. `known_from`/`known_to` are valid-time. Don't mix.
- **N-PORT validation cliff (#932)**: EdgarTools' Pydantic `FundReport.parse_fund_xml` rejects synthetic test fixtures the bespoke parser tolerates. Bespoke stdlib-ElementTree parser remains shipped; rewrite parked.

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
