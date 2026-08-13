# ETL functional-spec rollout — v3 (executable plan)

**Status:** 2026-05-23. **Supersedes v1 + v2** (kept as drafting history). v3 is what we execute.

**Why v3:** v1 had 4 BLOCKING errors (wrong-arithmetic critical path, parallelise-fundamentals-already-parallel, advisory-lock-leak-not-a-leak, skill-shrink-sequencing). v2 had 5 BLOCKING errors (multi-endpoint contradiction, T1.2 phase naming, S14 cause incorrectly marked "unknown", one-job-per-file infrastructure false-positive, `bootstrap_entrypoint` field duplicating `job_name`). v3 fixes every BLOCKING + integrates every IMPORTANT finding. Pinned ambiguities; no gestures.

**Honest Run #8 projection**: max(sec_rate ≈ 20 min, db ≈ 22 min) ≈ **22-25 min wall-clock**. Under 60-min Tier 1 target with comfortable margin. Per `_STAGE_LANE_OVERRIDES` at `bootstrap_orchestrator.py:966` db family-split keeps S8-S12 cross-lane parallel; db wall-clock = max-of-stages.

---

## 1. Decisions pinned (replaces v2 "open questions")

| Decision | Answer |
|---------|--------|
| Multi-endpoint sources: one spec or many? | **One spec per (source, fetch_pattern) pair**, where `fetch_pattern ∈ {bulk_archive, per_resource_http, atom_feed, websocket}`. SEC submissions = 2 specs: `submissions-bulk.md` (zip path) + `submissions-files-walk.md` (per-CIK + secondary pages). NOT one spec with secondary sinks — that conflates sink plurality with endpoint plurality. |
| Derived sinks: separate template? | **Yes.** `docs/etl-specs/derived/TEMPLATE.md` covers internal-source sinks (`financial_periods`, `ownership_*_current`, `instrument_business_summary`, `report_snapshots`). Shape: upstream inputs → derivation rule → idempotency proof → rebuild SQL → lateness propagation → smoke matrix. |
| Enrichment (multi-provider): separate template? | **Yes.** `docs/etl-specs/enrichment/TEMPLATE.md` covers stages with two upstreams (OpenFIGI CUSIP resolver, ticker→CIK). Source template + provider matrix (per-provider rate limit + shared-budget calculation) + partial-failure semantics. |
| Spec-vs-code mapping format | **Spec §1 lists `Source files:`** as a YAML-style list embedded in markdown. Pre-push gate parses with simple grep. `docs/etl-specs/README.md` becomes the inverted index (auto-generated from §1 sections). |
| Smoke panel | **Per-source panel** — declared in spec §1 as `Smoke panel:` field. SEC issuers: AAPL/GME/MSFT/JPM/HD. 13F filers: BRK/Vanguard/BlackRock/Citadel/StateStreet. NPORT funds: VTSAX/VFIAX/SPY/AGG/VTI. FINRA: AAPL/GME/AMC/NIO/TSLA. Fixtures live in `tests/fixtures/etl_smoke_panel.py`. |
| Skill shrink timing | **After spec landed (merged), not drafted.** No mechanical gate; sequencing discipline in PR-author checklist. |
| Per-environment budgets | **No.** Template §12 declares one budget (dev DB, cold cache). Document cold/warm delta in §16 Rationale if non-trivial. |
| Bootstrap-mode mechanism | **Separate `_INVOKERS` entry via `job_name` swap**, NO new `bootstrap_entrypoint` field. v2's `bootstrap_entrypoint` is dropped — `StageSpec.job_name` IS the entrypoint. T1.2 changes `_BOOTSTRAP_STAGE_SPECS[25]` from `"fundamentals_sync"` to `"fundamentals_sync_bootstrap"`. |
| O(universe) test mechanism | **HTTP-count + DB-call-count assertion**, not wall-clock. Mock provider transport; assert call count constant across 5/50/500 fixture. Wall-clock as non-blocking telemetry. (Codex correction to v2's flaky wall-clock threshold.) |
| Naming convention | **`<daily_job_name>_bootstrap` suffix** for separate bootstrap entrypoints. Registered in `_INVOKERS` alongside daily. |
| Pre-push enforcement | **WARN-only** for "code without spec" — CI comment names changed files + missing spec paths; CODEOWNERS reviewer holds final gate. **HARD BLOCK only for `CREATE INDEX` → Index Budget update** (via `-- spec:` header comment in migration). |

---

## 2. The spec template — 18 sections (final)

Anchored at `docs/etl-specs/TEMPLATE.md`. Each spec MUST fill every section or explicitly "not applicable + reason." No skipping.

1. **Identity** — provider, endpoint URL, HTTP method, auth, rate limit, conditional-GET (ETag / If-Modified-Since). **Source files** (YAML list of `app/services/*.py`, `app/providers/implementations/*.py`, `app/jobs/*.py` files). **Stability contract** (stable-since date, last breaking change, where format drift would first manifest). **Smoke panel** (5 source-specific identifiers).
2. **Identifiers** — source-side PK; mapping to identity tables (CIK / instrument_id / accessionNumber / CUSIP / FIGI / symbol); FK targets; identity-resolution path.
3. **Schema (response)** — field-by-field table: name, JSON path, type, length, constraints, enum values, encoding, nullability, ordering, max array size.
4. **Change-detection** — split: (a) HTTP-level (Last-Modified / ETag / unconditional); (b) record-level (monotonic accession / sequence / `latest_filing_date`).
5. **Schema-drift detection** — declared `expected_field_set`; alarm rules covering: **missing field**, **new field**, **type change**, **enum expansion**, **array cardinality spike**, **parser quarantine** behavior. Alarm location (where the WARN appears in `bootstrap_archive_results.rows_skipped` or `job_runs.last_error`).
6. **Bootstrap behaviour** — `stage_key`; provides cap (cite `bootstrap_orchestrator.py:368`); requires cap; `fetch_strategy ∈ {bulk, per_resource_http, cache, derive}`; **O(complexity)** (`O(1)`, `O(archives)`, `O(quarters)`, `O(active_filers)` — never `O(universe)` without justification); separate-entrypoint? If yes, `<daily_job>_bootstrap` registration in `_INVOKERS`; max wall-clock + max HTTP count + expected rows.
7. **Watermark + late-arrival** — watermark column + type; source of truth; how far back amendments / supersessions can land.
8. **Update strategy + Tombstones** — incremental / bulk / both. Cadence. Revision window. **Tombstone table** (per scenario: amendment, supersession, no-tombstone). **Deletion semantics** split: source-delete vs retention-delete vs tombstone.
9. **Sink + PK + Conflict-key** — table(s); columns + types + lengths; PK; FKs. **Migration ref** (`sql/NNN_*.sql`). Conflict-key column tuple. **Idempotent UPSERT SQL pattern** (literal, not prose). **Secondary sinks** subsection if stage writes >1 table.
10. **Index Budget** — **hard cap N=4 per sink table** (PK, watermark, FK-into-ownership, one query-driven). Each index: name, columns, justification, write-amplification, last EXPLAIN check. Forbidden patterns. **Partition note**: if table is partitioned, cap counts parent indexes only (PG auto-inherits to partitions).
11. **Retention + Partition strategy** — retention rule; partition scheme; **extension deadline** for partitioned tables.
12. **Performance budget** — wall-clock (dev DB cold cache); HTTP count; DB roundtrips; memory ceiling; disk I/O; partition-write distribution. Assertion test path (`tests/integration/etl_contract/test_<source>_<endpoint>_budget.py`).
13. **Dynamic params** — every `_PARAM_DYNAMIC_*` sentinel; resolved formula; bootstrap-only vs shared-with-daily.
14. **Daily behaviour** — cron name; cadence; scope; polling pattern; discovery layer.
15. **Failure modes** — per-row recoverable; whole-job-fail; retry policy; backoff. **`rows_skipped` structure**: mandatory subsection with literal example JSON object showing expected keys + integer-counts contract.
16. **Cardinality envelope + Smoke matrix** — declared min/max rows per fetch / day / quarter. Smoke matrix per `(source, endpoint, sink)` triple: **3 mandatory** cases (empty / single / max-cardinality); **3 conditional** (late-arrival if §7 declares window; tombstone if §8 declares them; partition-boundary if §11 partitioned). Each row: input fixture, expected output, applicability flag.
17. **Backfill + Runbook** — manual backfill command (literal SQL/HTTP); wipe-and-rebuild; drift-detection query; freshness endpoint; rollback; **deprecation path** (`<endpoint>.deprecated.md` move; historical-row policy).
18. **Rationale log** — non-obvious choices captured at decision time. One line per choice: `<choice> — <reason> — <ticket ref>`. Prevents next engineer reverting silently.

Plus a **Gotchas (numbered)** preamble at the top.

---

## 3. Where things live

| Artifact | Path |
|---------|------|
| Plan (this) | `docs/superpowers/plans/2026-05-23-etl-functional-spec-rollout-v3.md` |
| Source-spec template | `docs/etl-specs/TEMPLATE.md` (18 sections) |
| Derived-sink template | `docs/etl-specs/derived/TEMPLATE.md` (12 sections — variant) |
| Enrichment template | `docs/etl-specs/enrichment/TEMPLATE.md` (template + provider matrix) |
| Index (auto-generated) | `docs/etl-specs/README.md` — `app/*.py → spec` mapping for CI |
| Per-source specs | `docs/etl-specs/<source>/<endpoint>.md` |
| Derived specs | `docs/etl-specs/derived/<sink>.md` |
| Enrichment specs | `docs/etl-specs/enrichment/<name>.md` |
| Smoke fixtures | `tests/fixtures/etl_smoke_panel.py` |
| Contract tests | `tests/integration/etl_contract/` |
| Smoke tests | `tests/integration/etl_smoke/` (network-marked) |

Skills (`.claude/skills/data-sources/*.md`) keep **source-wide rules** (rate limits, identity, cross-cutting gotchas list). Endpoint-specific schema moves to specs. No line-count cap; target = "no endpoint schema in skills."

---

## 4. Tier 1 ticket sequence (Run #8 path)

Order = wall-clock-binding-lane first. Time-boxed; failures defer per criteria below.

### T1.1 — S16 institutional_filers cohort bound *(easy, sec_rate)*
- Bound institutional_filer HTTP to `last_13f_hr_at >= (NOW() - INTERVAL '380 days')` — mirrors #1010 stage-22 pattern. Column already exists per `sql/157_institutional_filers_last_13f_hr_at.sql`.
- **Blockholder_filers has NO equivalent column.** Two options: (a) defer blockholder HTTP to scheduled job post-bootstrap (drop from S16 entirely); (b) keep full blockholder set (small enough — historically <2000 rows). **Decision: (a)** — drop blockholder from S16; add new scheduled job `blockholder_directory_sync_daily`. Spec: `docs/etl-specs/sec-edgar/blockholder-directory.md` documents the split.
- Projected: S16 65 min → ~8 min.
- Cohort projection: institutional 11,200 → ~8,000 (per #1010 measured shrinkage of 22%); blockholder 0 (deferred). Total HTTP fetches: 8,000 not 11,200.

### T1.2 — S25 separate bootstrap entrypoint *(largest single win, db)*
- Add `fundamentals_sync_bootstrap()` in `app/workers/scheduler.py` adjacent to `fundamentals_sync()`. Body:
  1. Coverage-floor check: `SELECT COUNT(DISTINCT cik) FROM financial_facts_raw` ≥ floor (default = 0.8 × `SELECT COUNT(*) FROM instruments WHERE primary_sec_cik IS NOT NULL`). If under, raise `BootstrapPreconditionError`.
  2. Call `normalize_financial_periods()` directly from `app.services.fundamentals` (post-T1.5 decomposition: from `app.services.fundamentals.normalize`).
  3. Call `coverage_audit()` from `app.services.coverage`.
  4. SKIP: Phase 0 CIK refresh, Phase 1 `daily_financial_facts()` HTTP, Phase 1b `refresh_fundamentals` snapshot, tier review.
- Register in `_INVOKERS` at `app/jobs/runtime.py:208`: `"fundamentals_sync_bootstrap": fundamentals_sync_bootstrap`.
- Change `_BOOTSTRAP_STAGE_SPECS` S25 from `_spec("fundamentals_sync", 25, "db", "fundamentals_sync")` to `_spec("fundamentals_sync", 25, "db", "fundamentals_sync_bootstrap")`. Stage key stays `"fundamentals_sync"` (preserves cap wiring); `job_name` swaps.
- Add `_STAGE_CATALOGUE_RENAME_MAP` entry: `"fundamentals_sync"` → `"fundamentals_sync_bootstrap"` (covers in-flight run resume after deploy; pattern from `bootstrap_orchestrator.py:2586`).
- Catalogue-invariant test asserts S25's `job_name == "fundamentals_sync_bootstrap"` AND `_INVOKERS["fundamentals_sync_bootstrap"]` is a different callable from `_INVOKERS["fundamentals_sync"]`.
- Projected: 101 min → 5 min.

### T1.3 — S14 use bulk archive's files[] *(sec_rate)*
- `sec_submissions_files_walk.py:109` fetches `provider.fetch_submissions(cik)` per CIK to get `files[]`. The bulk `submissions.zip` already contains `filings.files[]` for every CIK.
- Fix: `_list_cik_secondary_pages` reads `files[]` from cached/in-memory `submissions.zip` data (populated by S8 `sec_submissions_ingest`). Skip per-CIK primary HTTP entirely.
- Bulk data persistence: either (i) S8 caches `files[]` per CIK in a new `cik_secondary_pages_seed` table or `sec_filing_manifest.files_json` column, (ii) S14 re-reads the zip from disk.
- **Decision**: (i) — extend S8 to write `sec_filing_manifest.files_json` (per spec `sec-edgar/submissions-bulk.md`). Migration `sql/171_sec_filing_manifest_files_json.sql`. Backward compat: existing rows have NULL; S14 falls back to per-CIK HTTP for NULL rows during transition (one-time).
- Projected: S14 48 min → 5 min (only secondary-page HTTP fetches remain, ~150-300 of them).

### T1.4 — `fundamentals.py` decomposition *(blocks T1.2 imports)*
Required before T1.2 lands so `fundamentals_sync_bootstrap` imports cleanly.

**T1.4.0 — Symbol audit (1 day max)**
- Grep `tests/` + `app/` for every `from app.services.fundamentals import X` and `app.services.fundamentals.X` patch.
- Output: full list of public symbols + module-level constants that MUST re-export from `app/services/fundamentals/__init__.py`.
- Constants likely affected: `LOOKBACK_DAYS`, `FactRow`, `PeriodRow`, `RefreshPlan`, `RefreshOutcome`, `plan_refresh`, `execute_refresh`, `normalize_financial_periods`, `refresh_fundamentals`, `refresh_financial_facts`, `_canonical_merge_instrument`.

**T1.4.1 — Split (2-3 days)**
```
app/services/fundamentals/
  __init__.py            # re-export every symbol from audit
  _common.py             # shared types: FactRow, PeriodRow; shared constants: LOOKBACK_DAYS
  facts.py               # refresh_financial_facts + upsert_facts_for_instrument
  snapshot.py            # refresh_fundamentals + _upsert_snapshot
  normalize.py           # normalize_financial_periods, _derive_periods_from_facts,
                         # _upsert_period_raw, _canonical_merge_instrument,
                         # _record_treasury_observations_for_instrument
  sec_incremental.py     # plan_refresh, execute_refresh
  bootstrap.py           # NEW: fundamentals_sync_bootstrap (after T1.2 lands; or together)
```
Import DAG: `sec_incremental → {facts, snapshot, normalize}` → `_common`. No cycles.

**T1.4.2 — Smoke (1 day)**
- Run full pytest; every `from app.services.fundamentals import X` works.
- Run a 50-CIK bootstrap; S25 produces same output as pre-split.

**Time-box: 5 days total.** If T1.4.1 reveals genuine circular deps that need bigger refactor, defer to post-Run-#8 — but T1.2 then needs to live in the still-monolithic `fundamentals.py` (acceptable interim).

### T1.5 — `_canonical_merge_instrument` pre-dedupe *(correctness, db)*
- Source of CardinalityViolation × 43 in Run #7. Pre-dedupe input array on `(instrument_id, period_end_date, period_type)` before INSERT at `fundamentals.py:1426` (post-T1.4: `fundamentals/normalize.py`).
- Cheap, code-hygiene win. ~4 min wall-clock saving.

### T1.6 — Progress heartbeats on long stages *(observability)*
- Closes #1225.
- Every stage with `fetch_strategy ∈ {bulk, per_resource_http}` emits `last_progress_at` + `rows_processed` updates every N rows (default N=1000) or every 30s, whichever sooner.
- Implemented in the existing dispatcher's per-stage telemetry hook; per-stage spec §6 declares N.

### T1.7 — Move S13 cusip_resolver_post_bulk_sweep out of bootstrap *(cleanup)*
- No downstream cap depends on S13's output.
- Convert to scheduled job `cusip_resolver_sweep_daily` (cron @ 03:00 UTC, lane=openfigi).
- Drops S13 from bootstrap stage table.
- Spec: `docs/etl-specs/enrichment/cusip-resolver.md`.

### T1.8 — Fresh-DB boot guard *(operational safety)*
- Jobs process startup at `app/jobs/__main__.py` (or `app/services/processes/bootstrap_gate.py`):
  ```python
  if not master_key.is_bootstrapped():
      sys.exit(
          "ERROR: Operator setup not complete. "
          "Run /auth/setup at http://localhost:5173/setup before starting jobs."
      )
  ```
- Prevents the Run #1/#3/#6/#7 first-attempt failure pattern.
- Test: assert jobs process refuses to start on fresh DB without operator.

### Projected Run #8 wall-clock

After T1.1 + T1.2 + T1.3 + T1.5 + T1.6 + T1.7 + T1.8:
- sec_rate lane: S16 (8 min) + S14 (5 min) + small sec_rate stages (5 min cumulative max) ≈ **18 min**
- db lane (family-split parallel): max(S8=22, S9=15, S10=7, S11=1, S12=9, S25=5) = **22 min** (S8 unchanged is the bottleneck)
- Wall-clock = max(18, 22) ≈ **22-25 min**
- 60-min target met with 35-min headroom.

If S8 itself becomes the bottleneck post-fixes, Tier 2 explores chunking/parallel COPY there. Out of scope for Run #8.

---

## 5. Test gates

### 5.1 O(universe) work-detector (Codex's HTTP+DB-count, not wall-clock)
For each stage with `fetch_strategy ∈ {bulk, cache, derive}`:
- Synthetic fixture: 5 / 50 / 500 CIKs (from `tests/fixtures/scale_check_ciks.py`).
- Mock provider transport (intercepts HTTP, counts calls per endpoint).
- Spy on DB conn cursor (counts executes per statement family).
- **Assert: HTTP count(500) == HTTP count(5)** for `fetch_strategy=bulk` / `cache` / `derive`. (Constant, not "sub-linear.")
- **Assert: DB write count grows ≤ linearly with row count, NOT with CIK count** for bulk/derive stages.

### 5.2 Bootstrap-mode contract test
- Monkeypatch `SecFundamentalsProvider.extract_facts` + `extract_facts_and_catalog` to raise on `/api/xbrl/companyfacts/CIK`.
- Boot bootstrap fixture (5 CIKs); run S9 → S25.
- Assert no raises. Confirms S25 in bootstrap mode never hits per-CIK companyfacts endpoint.

### 5.3 Capability scheduling test
- Assert S25 cannot dispatch unless `fundamentals_raw_seeded` capability is held with min CIK-count floor (T1.2 raises floor from 1 to per-spec value).
- Lane caps enforced from `_LANE_MAX_CONCURRENCY`.

### 5.4 Conflict-key property test
- For every UPSERT writer with declared conflict key (per spec §9):
  - Generate input arrays with intentional duplicates on conflict key.
  - Assert UPSERT succeeds (no CardinalityViolation).

### 5.5 Catalogue-invariant test (extends existing `test_bootstrap_orchestrator_source_registry.py`)
- Assert every `_BOOTSTRAP_STAGE_SPECS` entry's `job_name` is present in `_INVOKERS`.
- Assert S25's `job_name == "fundamentals_sync_bootstrap"` (T1.2 acceptance).
- Assert the callable for `"fundamentals_sync_bootstrap"` is not `fundamentals_sync` itself.

### 5.6 Smoke matrix (per spec §16)
- 3 mandatory cases per `(source, endpoint, sink)`: empty / single / max-cardinality.
- 3 conditional cases activated by spec applicability flags.
- Marked `@pytest.mark.network` — opt-in.

### 5.7 Index Budget gate (CI hard block)
- Lint: every new `CREATE INDEX` migration must have `-- spec: docs/etl-specs/<source>/<endpoint>.md §10` as a header comment.
- Lint: the referenced spec's §10 must have been modified in the same PR.
- Drop-index migrations same rule (removing an index is a budget change).

### 5.8 Boot-guard test
- Wipe master_key state; start jobs process; assert exits with the configured error.

---

## 6. Enforcement (v3 — narrowed)

### Hard block (CI)
- **`CREATE INDEX` → Index Budget**: migration must reference spec §10 via header comment + spec §10 must be modified in same PR.
- **Boot guard**: jobs process refuses to start on un-bootstrapped DB.

### WARN-only (CI comment, CODEOWNERS gate)
- **Code-without-spec**: if a PR touches `app/services/sec_*`, `app/services/finra_*`, `app/services/fundamentals*`, `app/jobs/sec_*`, `app/providers/implementations/*` and no `docs/etl-specs/*.md` file changes, CI posts comment naming exact changed files + missing spec paths. CODEOWNERS review must explicitly acknowledge (label `etl-spec-omit` + reason in PR description).

### Dropped from v2
- ~~Per-service LOC cap (500)~~ — false-positives on package modules. Decomposition target stays for `fundamentals.py` (T1.4); not a general rule.
- ~~One-job-per-file~~ — `sec_bulk_orchestrator_jobs.py` intentionally has 5 jobs (Phase C family). No general rule; document multi-job intent in module docstring.

---

## 7. Rollout

| Phase | Deliverable | Time |
|------|-------------|------|
| 1 | This plan v3 final + index + 3 templates (`TEMPLATE.md`, `derived/TEMPLATE.md`, `enrichment/TEMPLATE.md`) | 1 PR, 1 day |
| 2 | 4 source specs + 1 derived spec: `submissions-bulk.md`, `submissions-files-walk.md`, `companyfacts.md`, `13f-bulk.md`, `derived/financial-periods.md` | 1 PR, 2 days |
| 3 | T1.4 `fundamentals.py` decomposition (T1.4.0 audit → T1.4.1 split → T1.4.2 smoke) | 1 PR, 5 days |
| 4 | T1.2 S25 separate entrypoint (depends on Phase 3) | 1 PR, 1 day |
| 5 | T1.1 S16 cohort bound + blockholder split | 1 PR, 1 day |
| 6 | T1.3 S14 use bulk archive `files[]` | 1 PR, 2 days |
| 7 | T1.5 + T1.6 + T1.7 + T1.8 (correctness + observability + cleanup + safety) | 1 PR each, 1 day each |
| 8 | **Run #8** verification — wipe DB; operator setup; bootstrap; assert ≤ 60 min | 1 day |
| 9 | Drill remaining ~10 sources + derived sinks (skill shrink follows spec land) | rolling |

**Total time to Run #8: ~3 weeks of focused effort.**

---

## 8. Acceptance criteria (Tier 1 done)

1. Every Phase-1-listed spec exists at its declared path and covers all 18 sections.
2. **Run #8 ≤ 60 min wall-clock** on fresh DB with operator setup completed before jobs process start.
3. Every bootstrap stage declares `fetch_strategy` in `StageSpec`; dispatcher enforces it via `BootstrapPreconditionError` when `fetch_strategy ∈ {bulk, derive}` and the invoker attempts HTTP.
4. O(universe) test green for every stage with `fetch_strategy ∈ {bulk, cache, derive}`.
5. Bootstrap-mode contract test green (S25 makes zero per-CIK companyfacts HTTP).
6. Catalogue-invariant test asserts S25's `job_name == "fundamentals_sync_bootstrap"`.
7. Conflict-key property test green for every UPSERT writer in scope.
8. Smoke matrix green per `(source, endpoint, sink)` triple (network tests opt-in but documented passing).
9. `bootstrap_archive_results` rows present for every archive stage; `rows_skipped` JSON matches per-spec declared schema.
10. Boot guard test green; manual verification of operator-setup-first protocol.
11. `fundamentals.py` decomposed; ≤ 500 LOC per resulting module OR justified in module docstring.
12. Pre-push WARN active; Index Budget hard block active; passes on baseline.

---

## 9. Risks

| Risk | Mitigation |
|------|------------|
| T1.4 decomposition reveals genuine circular deps | T1.4.0 audit identifies upfront. If unfixable, defer to post-Run-#8; T1.2 lives in monolithic file as interim (acceptable). |
| T1.2 bootstrap entrypoint doesn't drop S25 to 5 min | Coverage-floor check at top: if `financial_facts_raw` < floor, raise `BootstrapPreconditionError`. Forces caller to fix data, not silently produce incomplete derivation. |
| T1.3 S14 fix requires migration | Migration `sql/171_sec_filing_manifest_files_json.sql` ships in same PR; backward compat (NULL → fallback to per-CIK HTTP for one transition run). |
| S8 bulk ingest IS the new bottleneck post-fixes (22 min) | Document in v3 §4 as Tier 2 concern. Out of Run #8 scope. Spec `submissions-bulk.md` §12 declares the 22-min budget; if S8 regresses past 30 min, alarm fires. |
| Spec drift from code | WARN-only (not hard block) accepts this risk; CODEOWNERS sees diff; team owns. |
| Schema-drift detection alarms fire on legitimate SEC additions | Per-spec §5 declares WARN threshold; first false-positive triggers spec update + memory entry. |
| Run #8 STILL misses 60 min | We're at 22-25 min projected with 35-min margin. Margin must absorb 2× error before missing. If it misses anyway, profile per stage + iterate. |

---

## 10. What v3 explicitly does NOT do

- No Atom getcurrent / daily-index / per-CIK poll discovery layers (#1155 unblock) — separate concern, post-Tier-1.
- No discovery-layer firing assertion in steady state — bootstrap success doesn't require it.
- No `cusip_resolver_post_bulk_sweep` actually resolving 16M CUSIPs — that's moved to scheduled job per T1.7. CUSIP resolution becomes a steady-state property, not a bootstrap-completion property.
- No spec-versioning / source deprecation lifecycle in template — `<endpoint>.deprecated.md` rename rule covers it; full lifecycle deferred.

---

## 11. Definition of "nailed"

Same end-state as v1 §15 / v2 §14, with these refinements:

1. **Run #8 ≤ 60 min** on fresh DB. Single observable signal.
2. Every spec section non-empty.
3. Skills hold no endpoint schema.
4. Pre-push WARN + Index Budget hard block green.
5. O(universe) test green.
6. `fundamentals.py` decomposed.
7. Boot guard prevents the master-key-cache foot-gun forever.

That's the bar.
