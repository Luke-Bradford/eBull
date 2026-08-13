# ETL functional-spec rollout — v2 (post-review rewrite)

**Status:** draft v2 (2026-05-23). Supersedes v1 (`2026-05-23-etl-functional-spec-rollout.md`).
**Owner:** project owner; AI agent (me) running it end-to-end.
**Why v2:** v1 went through 4 parallel reviewers (Codex + adversarial code-reviewer + code-architect + data-engineer adversarial). 4 BLOCKING errors and ~20 IMPORTANT gaps surfaced. v2 is a clean rewrite; v1 is kept as historical record of how the diagnosis evolved.

## 0. Headline corrections from v1

| v1 claim | v2 correction | Source |
|----------|---------------|--------|
| S14 = "primary HTTP per CIK before checking `files[]`" | **WRONG.** `sec_submissions_files_walk.py` module docstring + code confirm it only walks secondary pages. 48-min cause is **unknown** — must profile before any fix. | Reviewer (BLOCKING) |
| S25 fix = "derive snapshot from `financial_facts_raw`" | Targets the wrong phase. **Real 85-min bottleneck = `daily_financial_facts()` per-CIK XBRL calls at `scheduler.py:3304`** (Phase 1), not `refresh_fundamentals` (Phase 1b which is feature-flag gated). Fix = separate bootstrap-only entrypoint that skips Phase 1 entirely. | Reviewer (BLOCKING) + Codex |
| "NEVER per-row HTTP in bootstrap mode" | Blocks legitimate S16 institutional/blockholder + S27 N-CSR drain (no bulk equivalents). Rule must qualify: "NEVER per-row HTTP for **bulk-covered** data; per-row HTTP allowed for sources with no bulk archive, bounded by cohort filter." | Reviewer (BLOCKING) |
| Skills shrink to pointers in Phase 1 | Skills shrink only AFTER spec landed (not drafted). Otherwise content gap. | Reviewer (BLOCKING) |
| New `SourcePlan` class | Extend existing `StageSpec` (`bootstrap_state.py:137`). Use existing `RefreshPlan / execute_refresh`, `ManifestSource + parser registry`, bootstrap capabilities. **No new framework.** | Architect + Codex |
| 14-section template | **17 sections** — adds Index Budget (hard cap), Retention, Schema-drift detection, Cardinality envelope, Backfill/replay commands, Encoding+precision table, Source files mapping, Stability contract, Dynamic params, Secondary sinks, Migration ref. | DE + Codex |
| Smoke panel = AAPL/GME/MSFT/JPM/HD per source | **Per `(source, endpoint, sink)` triple** with source-specific panels (issuer CIKs / institutional filer CIKs / fund series IDs / FINRA symbols). | DE + Codex |
| Bootstrap contract test = "captured-request mock" | Overengineered. Cheapest = monkeypatch `SecFundamentalsProvider.extract_facts*` to raise on `/api/xbrl/companyfacts/CIK`. | Codex |
| §8 test 1: "no per-CIK companyfacts HTTP" | Too narrow. Real invariant = **no O(universe) per-row work** of any kind. Test must scale-check (5 → 50 → 500 CIKs) for sub-linear wall-clock. | DE |
| `<100 lines` skill cap | Wrong cap. Real rule: "no endpoint-specific schema in skills." Sec-edgar skill stays ~300 lines correctly. | Architect |
| Pre-push hook hard-blocks "code without spec" | Brittle. Use WARN + PR-label override (existing `.githooks/pre-push:46` is already a brittle pile). | Codex |
| `fundamentals.py` not mentioned | **2953 LOC, 4 bounded contexts in one file.** Decomposition is Tier 1.5 (can't enforce "minimal copy" while this exists). | DE |
| `bootstrap_archive_results` not mentioned | The per-archive observability layer. Drives cap row resolution at `bootstrap_orchestrator.py:1232`. Plan must reference. | Codex |

## 1. Context (kept from v1, lightly revised)

We have ~14 wired ETL sources (SEC EDGAR, FINRA, eToro, OpenFIGI, Frankfurter ECB, MF directory, exchange directory) + several derived sinks (financial_periods, instrument_business_summary, ownership_*_current, ownership_*_observations, report_snapshots). v1's "44 pipelines" count was unverified — actual stage count is 27 bootstrap stages + 31 scheduled jobs (some shared, some non-ETL). The exact mapping is part of Phase 1 inventory work, not asserted upfront.

Across these we have:
1. **Three transaction-boundary patterns** coexisting (caller-wraps #915; implicit per-archive; hybrid). Same class of work behaves differently per author.
2. **Daily-mode code paths reused as bootstrap stages** — `fundamentals_sync` IS `daily_financial_facts` wrapped, doing audit + per-CIK HTTP even when bulk path wrote the data 5 min earlier.
3. **No canonical per-source functional spec** — content scattered across skills + impl-spec PRs.
4. **`fundamentals.py` is a 2953-line omnibus** combining snapshot + facts + normalization + sec_incremental contexts — explicit anti-example of "minimal copy."
5. **Bootstrap critical path = `sec_rate` lane ≈ 113 min** (S14+S16). `db` lane (S25 = 101 min) runs in parallel. v1 wrongly summed lanes.

## 2. Goal (kept from v1)

Land uniform functional spec per `(source, endpoint, sink)` triple so:
- Any engineer / agent reads ONE document to understand the contract.
- Skills point at specs (load-bearing rules stay in skills; endpoint-specific schema lives in specs).
- Each bootstrap stage declares: max wall-clock, max HTTP count, expected row count, smoke panel, O(complexity) bound.
- New sources fill the template or don't ship.

## 3. Where things live (v2 layering)

| Artifact | Path | Purpose |
|---------|------|--------|
| Plan v2 (this doc) | `docs/superpowers/plans/2026-05-23-etl-functional-spec-rollout-v2.md` | Rollout roadmap |
| Per-source spec template | `docs/etl-specs/TEMPLATE.md` | 17-section canonical shape |
| Per-source spec index | `docs/etl-specs/README.md` | Status table + `app/services/*.py → spec` mapping for pre-push gate |
| **Source specs** | `docs/etl-specs/<source>/<endpoint>.md` | One file per `(source, endpoint, sink)` triple |
| **Derived-sink specs** | `docs/etl-specs/derived/<sink>.md` | One file per derived sink (`financial_periods`, `ownership_*_current`, `instrument_business_summary`, `report_snapshots`) |
| **Cross-source enrichment specs** | `docs/etl-specs/enrichment/<name>.md` | OpenFIGI CUSIP resolver, ticker→CIK lookup, etc. — two-provider stages |
| Skills (existing) | `.claude/skills/data-sources/*.md` | **Source-wide rules stay** (rate limit, identity, gotchas list, format-derived cross-cutting rules). **Endpoint-specific schema MOVES OUT to specs.** Target: "no endpoint-specific schema in skills" — line count is consequence not target. |
| Coverage matrix | `.claude/skills/data-engineer/etl-endpoint-coverage.md` | Repurpose as INDEX + 5-layer-wiring dashboard. Each row links to its spec. |
| Pre-flight rule | `.claude/skills/data-engineer/SKILL.md` | Add: "Before changing a pipeline, read its spec OR write one. Specs are non-negotiable. CI WARN catches drift; PR-label override permitted with reason." |

## 4. Spec template — 17 sections (v2)

Sequence + content revised per DE adversarial pass. Every section has a hard "what goes here" definition. No "may be empty" — explicit "not applicable + reason" only.

1. **Identity** — provider, endpoint URL, HTTP method, auth, rate limit, **conditional-GET contract (ETag / If-Modified-Since)**. Plus **Source files**: explicit list of `app/services/`, `app/providers/implementations/`, `app/jobs/` files implementing this pipeline (used by pre-push gate to detect spec drift). Plus **Stability contract**: stable-since date, last breaking change, where format drift would first manifest.
2. **Identifiers** — source-side PK (CIK, accessionNumber, CUSIP, FIGI, symbol); mapping to our identity tables; FK targets in our schema. Identity resolution path explicit (CUSIP → CIK → instrument_id).
3. **Schema (response)** — field-by-field table: name, JSON path, type, length, constraints, enum values, encoding, nullability, ordering guarantees, max array size. **Encoding-precision subtable** for numeric fields (DD-MMM-YYYY, NUMERIC(18,6), Decimal precision, etc.).
4. **Change-detection contract** — **two sub-sections**: (a) HTTP-level (Last-Modified, ETag); (b) record-level (monotonic accessionNumber, sequence number, `latest_filing_date` field). These answer different operational questions and must be split.
5. **Schema-drift detection** — declared `expected_field_set`; WARN threshold if observed ⊂ expected by more than X%; alarm location.
6. **Watermark + late-arrival window** — watermark column + type; source of truth; **how far back into already-watermarked history can a new arrival land** (4 quarters for 13F-HR amendments; 60 days for NPORT; never for FINRA RegSHO; etc.).
7. **Update strategy + Tombstones** — incremental / bulk / both. Cadence. Revision window. **Tombstone table** with row per scenario: amendment (Form 3/5), supersession (NPORT, 13F within quarter), no-tombstone (RegSHO). Deletion semantics split: **source-delete vs retention-delete vs tombstone**.
8. **Sink + PK + Conflict-key** — Postgres table(s); columns + types + lengths; PK; FKs. **Migration ref**: `sql/{NNN}_{table_name}.sql` (one-line pointer; no history in spec). **Conflict-key column tuple** (the literal columns PG's `ON CONFLICT` uses) + idempotent upsert SQL pattern (literal SQL not prose). **Secondary sinks** subsection for stages writing multiple tables (e.g. `sec_submissions_ingest` writes `filing_events` AND `sec_filing_manifest`).
9. **Index Budget** — **hard cap N=4 indexes per sink table** (PK, watermark, one FK-into-ownership, one query-driven). Each index: name, columns, justification (which query uses it), write-amplification cost, last query-plan check date. Forbidden patterns (GIN on rarely-queried JSON; composite with leading low-cardinality; redundant covering of PK prefix). **Pre-push lint: new `CREATE INDEX` must update this section in same PR.**
10. **Retention + Partition strategy** — retention rule (13F: keep latest N quarters per filer; NPORT: 8 quarter history; etc.); **partition scheme** (range-partitioned by quarter / year / period_end); **extension deadline** for partitioned tables (FINRA RegSHO has hard 2030-Q1 cliff).
11. **Bootstrap behaviour** — `stage_key`; provides cap (links to existing `_STAGE_PROVIDES_CAPS` at `bootstrap_orchestrator.py:368`); requires cap (`_STAGE_REQUIRES_CAPS:526`); `fetch_strategy: Literal["bulk", "http", "cache", "derive"]` (new field on existing `StageSpec`); **bootstrap entrypoint** (separate function from daily entrypoint when applicable — Codex's "separate entrypoint" principle); **O(complexity) bound** (`O(1)`, `O(archives)`, `O(quarters)`, `O(active_filers)` — never `O(universe)` unless source has no bulk path); declared **max wall-clock**, **max HTTP count**, **expected rows**.
12. **Dynamic params** (per-stage) — list every `_PARAM_DYNAMIC_*` sentinel resolved at dispatch (`bootstrap_orchestrator.py:155-167`); resolved formula; bootstrap-only vs shared-with-daily. Operators re-triggering manually need to know what `min_last_13f_hr_at` resolves to.
13. **Daily behaviour** — cron name; cadence; scope; polling pattern (manifest worker / Atom / per-CIK poll / bulk-refresh). Discovery layer (1-4).
14. **Failure modes** — per-row recoverable; whole-job-fail; retry policy; backoff; what gets WARN'd in `bootstrap_archive_results.rows_skipped` JSON (the per-archive observability layer — `bootstrap_orchestrator.py:1232`).
15. **Cardinality envelope + Smoke matrix** — declared min/max rows per fetch / per day / per quarter. **Source-specific smoke panel** (NOT global AAPL/GME/MSFT — equity issuers for SEC filings, institutional filer CIKs for 13F, fund series IDs for NPORT, FINRA symbols/venues for FINRA). Per `(source, endpoint, sink)` triple: smoke matrix with rows for `empty / single / max-cardinality / late-arrival / tombstone / partition-boundary` cases.
16. **Performance budget + assertions** — wall-clock; HTTP count; DB roundtrips; **memory ceiling**; **disk I/O / archive size**; partition-write distribution. **Per-environment** (dev vs prod). Real `pytest` test asserting each budget on the smoke fixture. **O(universe) work-detector** test: scale-check 5 → 50 → 500 CIKs for sub-linear growth.
17. **Backfill + Operator runbook** — manual backfill command (literal SQL/HTTP, not prose); wipe-and-rebuild command; drift-detection query; freshness check endpoint; rollback procedure; **deprecation path** (when source retires, what happens to existing rows + spec moves to `<endpoint>.deprecated.md`).

Plus, embedded at the top of every spec: **"Gotchas (numbered)"** — specific traps (e.g. "13F-HR VALUE cutover 2023-01-03: pre-cutover values in thousands, post in dollars"; "SEC submissions.json overflows into `filings.files[]` once recent.filings has >1000 entries"). Numbered for cross-reference.

## 5. Tier 1 fix sequence (v2 — wall-clock-binding order, BLOCKING errors fixed)

Order = wall-clock impact on the binding lane first (sec_rate = 113 min). Each ticket lands its own PR + spec change in the same PR. **Diagnostic profiling is a pre-step for tickets with uncertain root cause.**

### Tier 1 (sec_rate lane — the binding lane)

**T1.0 — DIAGNOSTIC: profile S14 (sec_submissions_files_walk) before any fix**
- v1's diagnosis was wrong (module already only walks secondary pages). 48-min cause unknown.
- Add per-CIK + per-secondary-page timing logs; re-run small bootstrap (Run #7.5 — single-CIK smoke + 50-CIK ramp).
- Output: confirmed root cause. Could be lock contention with S25, secondary-page burst, or something else entirely.
- Spec authored from confirmed measurements: `docs/etl-specs/sec-edgar/submissions-files-walk.md`.

**T1.1 — S16 bulk-aware non-issuer HTTP cohort bound**
- Issuer fast-path already skips HTTP (verified — `sec_first_install_drain.py:310,318,340`).
- 65-min cause = institutional_filers + blockholder_filers per-CIK HTTP at 10 req/s shared.
- Fix: bound to `last_13f_hr_at >= cutoff` (mirror #1010 cohort-bound pattern); 11,200 → ~3,000 filers.
- Projected: 65 min → ~10 min.
- Spec: `docs/etl-specs/sec-edgar/13f-filer-directory.md` + `blockholder-directory.md`.

**T1.2 — S25 separate bootstrap-only entrypoint (decisive Codex finding)**
- Current `fundamentals_sync` = `daily_financial_facts()` + audit + tier review (omnibus).
- Phase 1 = per-CIK XBRL API calls at `scheduler.py:3304` = the 85-min bottleneck.
- Fix: new `fundamentals_sync_bootstrap()` entrypoint that runs:
  - Phase 1 SKIP (data already in `financial_facts_raw` via S9)
  - Phase 1b SKIP (audit not needed first-install)
  - Phase 2 normalize (derive `financial_periods` from `financial_facts_raw`)
  - Coverage audit
  - NO tier review
- The dispatcher (`bootstrap_orchestrator.py`) wires `fundamentals_sync_bootstrap` to stage S25; the cron retains the omnibus `fundamentals_sync` for daily mode.
- Projected: 101 min → ~5 min.
- Spec: `docs/etl-specs/sec-edgar/companyfacts.md` + `docs/etl-specs/derived/financial-periods.md`.

**T1.3 — Profiled S14 fix (whatever T1.0 reveals)**
- Could be: lock-contention serialisation with S25 (move S14 to after S25 done) — easiest.
- Could be: secondary-page concurrency cap too tight — bump concurrency within rate-limit budget.
- Could be: a per-CIK code path that's not visible in static reading.
- Real fix lands here once T1.0 confirms.
- Spec: `docs/etl-specs/sec-edgar/submissions-files-walk.md` populated with confirmed numbers.

### Tier 1.5 (cross-cutting, blocks principled fixes)

**T1.5 — `fundamentals.py` decomposition (DE adversarial finding)**
- 2953 LOC, 4 bounded contexts (snapshot / facts / normalization / sec_incremental).
- Split into `app/services/fundamentals/snapshot.py`, `facts.py`, `normalize.py`, `sec_incremental.py` — each ≤ 500 LOC.
- Public API preserved at `app/services/fundamentals/__init__.py` for backwards compat.
- Required before T1.2 lands so the new `fundamentals_sync_bootstrap` entrypoint has a clean home.

### Tier 1.6 (correctness, doesn't block but cheap)

**T1.6 — `_canonical_merge_instrument` pre-dedupe** at `fundamentals.py:1426`
- 43 CardinalityViolation retries × ~5s = 215s
- Pre-dedupe input array on `(instrument_id, period_end_date, period_type)` before INSERT.
- Code-hygiene win. Not Tier 1 wall-clock blocker.

### Tier 2 (db lane — non-binding, but bites once sec_rate is fixed)

**T2.1 — Chunked multi-row INSERT in `_upsert_period_raw`** (post-T1.5 decomposition)
- Phase 2 normalize: 125k-250k serial round-trips → batched.
- 12 min → ~2 min on db lane.

**T2.2 — Progress heartbeats on long stages** (closes #1225)
- Cross-cutting; documented as template §16 requirement; pre-push lint via O(universe) test.

**T2.3 — Move S13 cusip_resolver out of bootstrap critical path**
- No downstream cap depends on S13's output.
- Reschedule as post-bootstrap scheduled job.
- Cleaner observability; no Tier 1 wall-clock impact.

### Projected wall-clock after Tier 1 (T1.0-T1.3 + T1.5 + T1.6)

- sec_rate lane: S14 (TBD per T1.0) + S16 (10 min) + small stages (~5 min) = **20-30 min depending on T1.0 outcome**
- db lane: S25 (5 min) + Phase 2 (12 min if pre-T2.1, 2 min after) = **7-17 min**
- Wall-clock = `max(sec_rate, db) ≈ 20-30 min` — under 60-min Tier 1 target with headroom.

## 6. Architectural shape (v2 — built on existing patterns)

No new framework. Extend what exists:

### 6.1 `StageSpec` extension (single field add)

Add to `app/services/bootstrap_state.py:137` (existing `StageSpec`):
- `fetch_strategy: Literal["bulk", "http", "cache", "derive"]` — declared posture
- `row_budget: int | None` — expected row count (used by O(universe) test)
- `bootstrap_entrypoint: str` — name of the bootstrap-mode invoker (may differ from daily-mode); resolved through existing `_INVOKERS` registry at `app/jobs/runtime.py`

### 6.2 Capability layer integration (no duplication)

The plan **extends** existing `_STAGE_REQUIRES_CAPS` (`bootstrap_orchestrator.py:526`) / `_STAGE_PROVIDES_CAPS` (`:368`) / `_CAPABILITY_MIN_ROWS` (`:451`) — doesn't duplicate. Each spec's "Bootstrap behaviour" section names the cap(s) it requires + provides; values flow from existing maps.

### 6.3 `bootstrap_archive_results` observability layer

The per-archive ingest receipts table (rows drive cap row resolution at `bootstrap_orchestrator.py:1232`, written by `sec_bulk_orchestrator_jobs.py:253`) becomes the **canonical observability sink**. Every spec's §14 (Failure modes) names what goes into `rows_skipped` JSON for that source. Acceptance criterion: every archive stage writes at least one `bootstrap_archive_results` row.

### 6.4 Dispatcher enforcement (`BootstrapPreconditionError`)

The orchestrator's dispatch loop checks `stage.fetch_strategy`. If `bulk` or `derive`, attempts to make outbound HTTP (intercepted via test-injected provider transport patch in tests; via runtime assertion in dev) raise `BootstrapPreconditionError` (existing class at `bootstrap_preconditions.py:51`). No new exception types.

### 6.5 Separate bootstrap entrypoints (Codex's structural rule)

For stages with daily-mode omnibus wrappers (S25 today, possibly S16 once T1.1 lands), **physically separate bootstrap entrypoint** registered in `_INVOKERS`. The orchestrator routes S25 → `fundamentals_sync_bootstrap`; the apscheduler cron routes daily → `fundamentals_sync` (omnibus). Single function with `bootstrap_mode` param flag is rejected as too leaky — accidental Phase 1 reactivation is exactly the v1 mistake we're correcting.

## 7. Spec rollout sequence (v2)

| Phase | Output | Dependency |
|------|--------|-----------|
| 0 | This plan v2 reviewed + signed off | Now |
| 0.5 | Answer open Qs §10 #1, #4, #5, #6 (must answer before specs) | After v2 review |
| 1 | TEMPLATE.md (17 sections) + derived/TEMPLATE.md + enrichment/TEMPLATE.md + README.md | Phase 0.5 done |
| 2 | T1.0 diagnostic profiling of S14 — no spec, just measurements | Phase 1 done |
| 3 | 4 bootstrap-critical source specs + 1 derived spec (financial_periods) — drafted from real code + Run #7 receipts | Phase 1 done; can parallel with Phase 2 |
| 4 | Adversarial review of TEMPLATE + 5 specs (4 + 1 derived) | Phase 3 done |
| 5 | T1.5 `fundamentals.py` decomposition | Phase 4 done |
| 6 | T1.1 (S16 cohort bound) — separate PR; spec landed | Phase 5 done |
| 7 | T1.2 (S25 separate bootstrap entrypoint) — separate PR; spec landed | T1.5 done |
| 8 | T1.3 (profiled S14 fix, whatever T1.0 reveals) — separate PR; spec landed | T1.0 result clear |
| 9 | T1.6 + T2.1-T2.3 — cheap follow-up PRs | T1.1-T1.3 done |
| 10 | **Run #8** verification (60-min target) | T1.1-T1.6 + T1.5 + T2.1-T2.3 merged |
| 11 | Drill remaining ~10 sources + derived sinks | Post-Run-#8 |
| 12 | Skill shrinkage (Category B content moves to specs once specs landed; Category A+C content stays) | Phase 11 nearing complete |

## 8. Test gates (v2)

Live in `tests/integration/etl_contract/` + `tests/integration/etl_smoke/`.

### 8.1 O(universe) work-detector (replaces v1 narrow HTTP-only test)
For each bootstrap stage declaring `fetch_strategy ∈ {"bulk", "cache", "derive"}`:
- Synthetic fixture: 5 CIKs, 50 CIKs, 500 CIKs
- Run the stage in isolation
- Assert wall-clock grows **sub-linearly** (e.g. `t_500 / t_5 < 50` — i.e. less than 10× per 100× CIKs)
- Plus: HTTP count never increases between 5 and 500 (proves no per-CIK fetch)
- Catches per-row work of all kinds, not just HTTP.

### 8.2 Bootstrap contract test (cheap impl — Codex)
- Monkeypatch `SecFundamentalsProvider.extract_facts` + `extract_facts_and_catalog` to raise if endpoint contains `/api/xbrl/companyfacts/CIK`.
- Boot bootstrap fixture; run through S9 → S25.
- Assert: no raises occur (because S25 doesn't hit those endpoints in bootstrap mode).
- No captured-traffic infrastructure needed.

### 8.3 Capability scheduling test
- Assert S25 cannot dispatch unless `fundamentals_raw_seeded` capability is held.
- Assert lane caps (`_LANE_MAX_CONCURRENCY` at `:237`) enforced.
- Builds on existing capability layer — small additive test.

### 8.4 Conflict-key property test (DE finding)
- For every UPSERT writer (canonical merge, `_upsert_period_raw`, ownership observations, filing_events, etc.):
  - Generate random input arrays containing intentional duplicates on declared conflict key.
  - Assert upsert succeeds (no CardinalityViolation).
- Per spec §8 conflict-key declaration.

### 8.5 Index Budget enforcement
- CI lint: every new `CREATE INDEX` in `sql/*.sql` must have a matching update in the spec's §9 Index Budget section.
- Implementation: shell script comparing PR diff against `docs/etl-specs/<source>/<endpoint>.md`.

### 8.6 Per-source smoke matrix
- For each `(source, endpoint, sink)` triple:
  - `empty` case: stage runs without raising
  - `single` case: 1 row written
  - `max-cardinality` case: full history (e.g. BRK for fundamentals) completes within per-CIK budget
  - `late-arrival` case: amendment doesn't duplicate
  - `tombstone / supersession`: marks not deletes
  - `partition-boundary`: correct partition write
- Per source-specific panel (NOT global AAPL/MSFT for non-equity sources).
- Marked `@pytest.mark.network` — opt-in for live runs.

### 8.7 Performance budget assertion test
- Per spec §16: real pytest reading the spec's declared wall-clock + HTTP count budgets.
- Acknowledge wall-clock CI flakiness (#893 pytest pile-up + Postgres lock OOM precedent) — HTTP-count + DB-roundtrip-count are stable, wall-clock is best-effort with a slack factor.

## 9. Open questions — answer BEFORE Phase 1 (per Codex)

These MUST land before any spec is written:

1. **One spec or multiple for multi-endpoint sources?** (e.g. SEC submissions has primary JSON + secondary `files[]` + Atom getcurrent overlay) — proposed answer: **one spec per fetch+sink combination**. Submissions = 3 specs (`submissions.md` for primary; `submissions-files-walk.md` for secondary pages; `getcurrent-atom.md` for Atom — currently unwired). Driven by Phase 1 inventory work, finalized in template.
2. **Derived-sink spec format** — proposed: yes, separate `docs/etl-specs/derived/TEMPLATE.md` with sections: upstream inputs, derivation rule, idempotency proof, rebuild command, partition strategy, late-arrival behaviour. Phase 1 lands this.
3. **Deletion / Retention semantics in template** — answer: per Codex, splits into source-delete / retention-delete / tombstone in template §7. Mandatory.
4. **Per-environment performance budgets** — answer: yes, template §16 lists dev + prod. Single budget hides the cold-cache effect.
5. **Coverage-matrix ownership** — `etl-endpoint-coverage.md` becomes pure INDEX; per-source detail moves to specs. The G1-G13 gap register stays — that's the wiring-layer status board.

Defer:
- **GDPR / retention** beyond what each source's retention rule says — not relevant to bootstrap performance.
- **Upstream-provider SLA / known outages** — useful for retry posture but informational; can come post-Phase 1.

## 10. Acceptance criteria (v2)

Tier 1 is nailed when ALL the following:

1. Every wired source has a spec in `docs/etl-specs/<source>/<endpoint>.md` OR `docs/etl-specs/derived/<sink>.md` OR `docs/etl-specs/enrichment/<name>.md`.
2. Each spec covers all 17 template sections (or explicit "not applicable + reason").
3. Each spec declares a **bootstrap performance budget** (wall-clock + HTTP count + DB roundtrips + memory) AND passes its budget assertion test.
4. Each spec has a smoke matrix with per-source panel; smoke tests pass; cross-source verification figure documented and matches.
5. Skills hold no endpoint-specific schema (Category B content); they hold source-wide rules (Category A + C).
6. Pre-push: pipeline file change without spec change → WARN, requires PR label `etl-spec-omit` + reason. Hard block reserved for `CREATE INDEX` migrations (Index Budget gate).
7. **Run #8 completes in ≤ 60 min** on fresh dev DB wipe with operator setup completed beforehand (no master-key cache bug).
8. Bootstrap O(universe) work-detector test passes for every stage with `fetch_strategy ∈ {bulk, cache, derive}`.
9. Every archive stage writes a `bootstrap_archive_results` row; the per-archive ingest receipts visible to operator.
10. `fundamentals.py` ≤ 500 LOC per resulting module.
11. **S25 uses a separate `fundamentals_sync_bootstrap` entrypoint**, not a flag on the omnibus.
12. Operator runbook for "fresh DB → Run bootstrap" documented; includes jobs-process-restart-after-operator-setup step.

## 11. Enforcement (v2 — three gates added per DE)

1. **Per-service-module LOC cap**: 500 LOC hard cap on `app/services/*.py`. Pre-push lint. Permits explicit override per file (header comment `# loc-budget-override: <reason>`).
2. **One-job-per-file rule**: each `app/jobs/*.py` exposes exactly one `run_*` entrypoint. Pre-push lint via AST inspection.
3. **`CREATE INDEX` → Index Budget gate**: pre-push lint comparing PR diff for new `CREATE INDEX` statements against the relevant spec's §9 Index Budget section. Hard block. (Codex's "WARN + label override" applies to general pipeline-without-spec; Index Budget stays hard because over-indexing is irreversible without a write-tax-paying migration.)

Plus existing gates preserved (CLAUDE.md clauses 8-12 for ETL changes).

## 12. Risks + mitigations

| Risk | Mitigation |
|------|------------|
| 17 specs takes 4+ weeks | Land the 4 bootstrap-critical + 1 derived in Phase 3 (parallel with Phase 2 diagnostics). Drill rest after Run #8 passes. |
| Spec drift from code | Pre-push WARN + Index Budget hard block. Specs landed iteratively with implementation PRs, not before. |
| T1.0 diagnostic reveals S14 cause that requires bigger refactor | Buffer: if T1.0 takes a week, T1.2 (S25 entrypoint) still lands first because it's the bigger win and independent. T1.3 (S14 fix) can defer to post-Run-#8 if S14 is harder than projected. |
| `fundamentals.py` decomposition introduces regressions | T1.5 lands its own PR with full diff review + smoke tests across all 4 contexts before T1.2 builds on it. |
| Run #8 still misses 60-min target | A miss reveals a bottleneck v2 didn't catch (analogous to v1's mistakes). v2 has the O(universe) test as backstop; bottleneck shows up in stage timing budget violation, surfaced as a test failure, iterated. |
| Architect-vs-Codex disagreement on "params vs separate entrypoint" | v2 takes Codex's separate-entrypoint stance for S25 explicitly. If T1.2 implementation proves separate entrypoint is overkill, we revisit in T1.2's PR review. |
| Pre-push WARN gates ignored | WARN + PR label override forces explicit reason; CI WARN posts in PR; reviewers see it. |

## 13. What v2 does NOT promise

- v2 doesn't promise the 4 bootstrap-critical specs cover every gotcha — they're starting points, iterated under review.
- v2 doesn't promise discovery layers L1-L3 (Atom, daily-index, per-CIK poll — #1155 blocker) get fixed. That's a separate steady-state-freshness concern; bootstrap can succeed without them firing.
- v2 doesn't promise the next 10 sources after the 4 bootstrap-critical will all be the same shape — multi-endpoint, cross-source enrichment, and derived sinks each have their own templates.
- v2 doesn't promise no Tier 2 / Tier 3 work — under the new contract, future fixes still go through spec → test → PR sequence.

## 14. "This is nailed" — definition

Same as v1 §15 with these refinements:

1. **Run #8 ≤ 60 min** with the projected 20-30 min sec_rate-lane wall-clock + 7-17 min db lane.
2. **A new agent / engineer reads ONE spec file** and has full context to extend, fix, or audit a pipeline — no asking me, the user, or Codex.
3. **Every bootstrap stage is `fetch_strategy ∈ {bulk, cache, derive}` OR has a justified `http` posture with bounded cohort** (S16 non-issuer + S27 N-CSR; documented + tested).
4. **Skills hold no endpoint-specific schema**; they coordinate.
5. **`docs/etl-specs/README.md` index lists every wired source + derived sink as `landed`**; no `drafted` / `missing` / `deprecated-pending` rows.
6. **O(universe) work-detector test is green for every bootstrap stage.**

That's the end state. Bounded scope, observable signals.

## 15. Next adversarial review

Before any spec gets written, this v2 plan goes through a final pass:

1. **Codex** — does the wall-clock-binding lane ordering hold? Are there higher-value architectural shifts that v2 still misses?
2. **DE adversarial** — would a new data engineer joining tomorrow ship under v2 without asking me anything? If not, what's missing?
3. **Reviewer** — find places where v2 has the same "unverified diagnosis" problem v1 had (claims made without code citation).
4. **Architect** — does extending `StageSpec` + capability layer integration + separate bootstrap entrypoints actually work cleanly when implemented, or are there layering conflicts?

Each instructed: rip it apart again. If v2 passes adversarial review cleanly, we land Phase 0.5 (open Qs answers) + Phase 1 (template files) in a single PR.
