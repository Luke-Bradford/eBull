# US ETL completion — autonomous-execution plan

> **Goal:** Drive the US ETL endpoint coverage matrix to "ALL ✅" with bootstrap + steady-state daily updates both green. No new tech-debt tickets raised during execution — fix-in-scope as discovered.
>
> **Status:** DRAFT 2026-05-17. Operator-approved one-shot; subsequent sessions execute phases autonomously.
>
> **Reference:** `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 (per-source matrix) + §7 (gap register).

## 1. Autonomy contract — what runs without asking

Across all phases below, the executing session SHALL:

- Spec → Codex 1a → revise to CLEAN — no operator signoff between iterations.
- Plan → Codex 1b → revise to CLEAN — no operator signoff between iterations.
- Implement → local gates → Codex 2 pre-push → revise to CLEAN.
- Push → Monitor PR checks with proper terminal-exit conditions (no dead polling, no sleep loops, no ScheduleWakeup).
- Read bot review on landing → resolve every comment via `FIXED` / `DEFERRED` / `REBUTTED` contract → re-push → re-monitor → loop until APPROVE on the most recent commit + CI green.
- Merge (squash + delete branch).
- Update memory + handover summary.
- **No new tech-debt tickets.** If scope grows mid-PR — implement the fix in the SAME PR. The rule is: close everything we touch.

The executing session SHALL ASK the operator ONLY when:

- A genuine product / architectural trade-off requires judgement that Codex cannot resolve unilaterally (per `feedback_design_granularity.md`).
- A destructive operation outside the diff is required (Postgres restart, branch deletion of work-in-progress, etc.).
- A scope decision would touch outside the US-ETL coverage matrix (e.g. UI work, broker integration, anything in §F of the May 17 status report).

The operator handles in parallel (out-of-band):

- Bootstrap completion via admin UI — `/admin → Bootstrap → "Retry failed"`. The 5 remaining stages from #1187 retry land at ~67min/each over ~6h serial wall-clock. Operator does this whenever; not on the engineering critical path.

## 2. Phase plan (10 PRs)

Each phase = one autonomous session. Handover between phases.

### Phase 1 — Quick wins (parser hygiene + missing registry entries)

**PR 1: G7 — `sec_xbrl_facts` synth no-op parser**

- File: `app/services/manifest_parsers/sec_xbrl_facts.py` (NEW, follows sec_10q / sec_n_csr pattern).
- Body: `return ParseOutcome(status='parsed', parser_version='xbrl-facts-noop-v1')`. Document why (Company Facts API bulk path is the real ingest; manifest rows exist for accession tracking).
- Register in `app/services/manifest_parsers/__init__.py::register_all_parsers()`.
- Test: 1-line existence + behaviour test.
- Matrix update: §2 row + §7 G7 → CLOSED.
- Acceptance: manifest rows with `source='sec_xbrl_facts'` drain to `parsed` instead of `debug-skipped`.

**PR 2: G14 — `bootstrap_orchestrator` source-registry entry + manual-queue dispatch**

- Surfaced this session (PR #1188 T9-POST). `publish_manual_job_request(bootstrap_orchestrator)` → listener `_run_manual` → `JobLock(bootstrap_orchestrator)` → `source_for(...)` → KeyError.
- File: `app/jobs/sources.py::MANUAL_TRIGGER_JOB_SOURCES` add `"bootstrap_orchestrator": "init"` (pre-everything fence; serialises with universe_sync).
- Test: integration test that `publish_manual_job_request(bootstrap_orchestrator)` dispatches without KeyError.
- Acceptance: admin retry endpoint works without bypassing JobLock; operator-side retry no longer requires direct-Python-invocation workaround.

**PR 3: G13 — `subjects_due_for_recheck` reader verification**

- Per matrix §3 G13: Layer 3 `run_per_cik_poll` was updated in #1155 to drain both `subjects_due_for_poll` AND `subjects_due_for_recheck`. Verify the wiring actually fires under production cadence.
- AST audit + integration test that exercises both reader paths.
- If wiring exists per memory `[[us-source-coverage]]` "G13 recheck path added" — close G13 with test only.
- If wiring missing — fix in-scope.

### Phase 2 — Bridge tables (G8 + G9)

**PR 4: G8 — `company_tickers_exchange.json` consumer**

- Closes pink-sheet / OTC / foreign-without-ADR gap in CIK↔ticker bridge.
- Endpoint: `https://www.sec.gov/files/company_tickers_exchange.json`.
- Consumer: existing `daily_cik_refresh` shape; add as supplemental enrichment OR new dedicated ScheduledJob `daily_cik_exchange_refresh`.
- Persistence: extend `external_identifiers` with exchange metadata OR new `instrument_exchange` table per design.
- Matrix update: §4 row + §7 G8 → CLOSED.

**PR 5: G9 — `company_tickers_mf.json` consumer**

- ~28k mutual-fund rows with `seriesId` + `classId`. Already partially used by #1174 (S25 `mf_directory_sync` populates classId → instrument_id). Verify whether this PR is the canonical seed OR if #1174's path is sufficient. If sufficient — close G9 with documentation only.
- If not — wire dedicated consumer + persistence; bundle with PR 4 if scope aligns.

### Phase 3 — Cross-quarter discovery (G12)

**PR 6: G12 — `master.idx` quarterly cross-quarter walker**

- Endpoint: `https://www.sec.gov/Archives/edgar/full-index/YYYY/QTRn/master.idx`.
- Use case: cross-quarter discovery of accessions that the per-CIK polling missed (e.g. tombstoned CIKs, late-arriving amendments).
- Consumer: new ScheduledJob `sec_master_idx_quarterly_sweep` (cadence: weekly mid-quarter, monthly after).
- Persistence: writes to `sec_filing_manifest` for the manifest worker to drain.
- Matrix update: §4 row + §7 G12 → CLOSED.

### Phase 4 — API alternatives (G10 + G11)

**PR 7: G10 — `companyconcept` API consumer**

- Endpoint: `https://data.sec.gov/api/xbrl/companyconcept/CIK*/{taxonomy}/{tag}.json`.
- Use case: smaller-payload alternative to Companyfacts for known-tag pulls. Reduces SEC bandwidth + tightens fundamentals_sync inner loop.
- Implementation: extend `SecFundamentalsProvider` with a `fetch_concept(cik, taxonomy, tag)` method. fundamentals_sync opts into it for the Tier-1 metric set; full Companyfacts fetch remains the fallback.
- Performance audit: measure bandwidth + latency delta vs Companyfacts for a representative cohort.
- Matrix update: §4 row + §7 G10 → CLOSED.

**PR 8: G11 — `frames` API consumer**

- Endpoint: `https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/USD/{period}.json`.
- Use case: cross-sectional one-fact-per-filer for sector aggregates. v1 metrics surface currently doesn't expose sector heatmaps (the use case for this); evaluate whether the consumer is needed without a downstream UI. If no UI use case in v1, close G11 as DEFERRED-no-consumer via documentation; if there's a metrics ticket that benefits, wire the consumer.
- **Decision rule:** if `gh issue list --search "frames OR sector heatmap OR cross-sectional"` returns an open feature ticket, wire the consumer. Otherwise document G11 closure as "BY DESIGN — no v1 consumer; reopen when a sector-aggregate metric ticket lands."

### Phase 5 — Parser rewrites (EdgarTools drop-ins)

**PR 9: #925 — EdgarTools 13F-HR parser drop-in (follow-up to #913)**

- Current `sec_13f_hr.py` is hand-rolled. EdgarTools `Filing.obj()` returns a typed `ThirteenF` model that handles PRN/SH drop + 2023-01-03 VALUE cutover natively.
- Risk: per memory `[[edgartools]]` + #932, EdgarTools has a Pydantic validation cliff that can reject our fixtures. Pre-impl spike against the existing 13F-HR golden fixtures.
- If spike INFEASIBLE — close #925 with REBUTTED reasoning + freeze hand-rolled parser.
- If spike OK — drop-in + remove ~200 lines of hand-rolled parser code + extend tests to cover the EdgarTools path.

**PR 10: #932 — EdgarTools N-PORT FundReport parser drop-in (follow-up to #917)**

- Same shape as PR 9. Memory `[[edgartools]]` documents the Pydantic validation cliff. Pre-impl spike against existing N-PORT golden fixtures.
- If spike INFEASIBLE — close #932 with REBUTTED.
- If spike OK — drop-in + tests.

### Phase 6 — FINRA short-interest (#915 + #916)

**PR 11: #915 — FINRA bimonthly short interest ingest + schema**

- Parent **#796** + **#845** (closed). Headline real coverage gap.
- Endpoint: FINRA short interest bimonthly publication.
- Schema: `finra_short_interest_observations` (settle date, security symbol, short volume, days-to-cover). Partitioned by settle date if growth profile warrants.
- Consumer: new ScheduledJob `finra_short_interest_refresh` + writes to manifest.
- Parser: `app/services/manifest_parsers/finra_short_interest.py` registered.
- Matrix update: §2 row + §7 G6 → CLOSED (bimonthly portion).

**PR 12: #916 — FINRA RegSHO daily short volume ingest**

- Same shape as PR 11. RegSHO daily-volume files.
- Schema: `finra_regsho_daily_observations` (settle date, symbol, short volume, total volume, exchange).
- Bundles with PR 11 if cohesion is tight enough; otherwise sequential.
- Matrix update: §7 G6 → fully CLOSED.

### Phase 7 — Tech-debt hardening (#935)

**PR 13 + 14 — #935 ETL foundation contracts (split as needed)**

Per #935 issue body, six contract tasks:

1. Targeted rebuild full-history discovery (`sec_rebuild` follows `filings.files[]`).
2. First-install drain seeds scheduler rows.
3. Manifest worker enforces raw-payload persistence for payload-backed parsers.
4. Amendment detection uses `is_amendment_form` everywhere (replaces `endswith("/A")`).
5. No-parser manifest rows operator-visible (stats / log-level / job-result-detail).
6. Provider tag drift fix (capability tags ↔ implementation sources canonical mapping).

Bundle into 1-2 PRs. Sizing decision: 6 contracts is borderline for one PR; split if local gates show >2h per PR. The split point is the discovery vs invariant boundary — contracts 1+2 (rebuild + drain) in one PR; contracts 3+4+5+6 (invariants + observability + mapping) in a second.

### Phase 8 — Final validation + matrix sweep

**No PR — single audit pass + memory close-out.**

- Re-run `.claude/skills/data-engineer/etl-endpoint-coverage.md` matrix audit:
  - §2 — all 14 `ManifestSource` rows ✅.
  - §3 — all 3 discovery layers ✅.
  - §4 — all reference + bulk-archive endpoints ✅ OR formally documented BY DESIGN.
  - §7 — gap register: all `OPEN` rows → `CLOSED` OR `BY DESIGN`.
- Update `[[us-source-coverage]]` memory: replace open-gaps caveat with "ALL US ETL CLOSED — operator-side post-merge: bootstrap + daily steady-state validated end-to-end".
- Final smoke: trigger orchestrator FULL sync → confirm every db-lane + sec_rate-lane adapter target lands `job_runs.status='success'` (or legitimately `no_work` / `prereq_skip` for the right reason).
- Mark `[[legacy-cron-retirement]]` pre-condition fully MET.

## 3. Per-session handover template

At end of every session that closes a PR, append a handover block to this plan doc:

```markdown
## Handover — PR #<n> (merged <date>)

- Phase: <phase number>
- Gap / ticket closed: <ID>
- Merge SHA: <sha>
- Tests added: <list>
- Scope discoveries handled in-scope: <list, or "none">
- Matrix delta: <row + status>
- Next phase: <phase number + scope>
```

## 4. Anti-patterns to AVOID

Surfaced from the May 17 session (#1184 + #1187 retrospective):

- **No raising follow-up tickets for nits caught during PR review.** Fix in the same PR. Bot NITPICK / WARNING / PREVENTION — all resolved in the current PR, no `DEFERRED #N` escapes.
- **No long `sleep` in monitor scripts.** Use `until <terminal-condition>` loops or proper `Bash run_in_background` for one-shot waits. Per `feedback_no_sleepy_claude.md`.
- **No ScheduleWakeup mid-PR-cycle.** Drive each PR to merge in one session; if it can't fit, structure the next session to pick up at a clean handover point.
- **No "tested via mock — let CI catch real DB issues" punts.** Per `feedback_smoke_gate_swallowed_failures.md`. Every PR runs `tests/smoke/test_app_boots.py` locally.
- **No premature signoff asks.** Default to autonomous merge per §1 autonomy contract.
- **No closing-1-opening-many.** Every PR closes more tickets than it opens. The "in-scope discovery → fix-now" rule is non-negotiable. Track scope expansion in the PR body, not via new issues.

## 5. Estimated session count + sequencing

| Phase | PRs | Sessions (estimated) | Cumulative |
|---|---|---|---|
| 1 — Quick wins | 3 | 1 (PR 1+2 in one; PR 3 separate) | 1 |
| 2 — Bridge tables | 2 | 1-2 | 2-3 |
| 3 — Cross-quarter | 1 | 1 | 3-4 |
| 4 — API alternatives | 2 | 1-2 (G11 may be doc-only) | 4-6 |
| 5 — Parser rewrites | 2 | 2 (spike + impl per PR) | 6-8 |
| 6 — FINRA | 2 | 2 | 8-10 |
| 7 — #935 hardening | 1-2 | 1-2 | 10-12 |
| 8 — Final validation | 0 | 0.5 | ~12 |
| 9 — Backend stability + dev DB hygiene (#1208) | 1-3 | 2-3 | ~14-15 |

Operator runs bootstrap completion in parallel — independent track, no engineering bottleneck.

### Phase 9 — Backend stability + dev DB hygiene (#1208)

> Added 2026-05-18 after the two Postgres PANICs (09:05 + 12:54 UTC) on the dev DB. Not caused by Phase 6 work — root cause is wrong container defaults + leaked test DBs + unpartitioned `financial_facts_raw` (28 GB / 62 M rows). User directive: "Tests should be quick and mechanical, not storing data in mass. We have logic to test."

**Sub 1 — Postgres tuning** (one PR):
- New migration `sql/NNN_postgres_runtime_tuning.sql` applying ALTER SYSTEM for `max_wal_size=4GB`, `min_wal_size=512MB`, `wal_compression=on`, `checkpoint_completion_target=0.9`, `shared_buffers=2GB`, `maintenance_work_mem=512MB`, `effective_cache_size=4GB`, `work_mem=32MB`. Migration issues `pg_reload_conf()`; restart required for `shared_buffers`.
- `docker-compose.yml`: add `mem_limit: 4g` + `shm_size: 1g`.
- Document tuning rationale.

**Sub 2 — Test-fixture orphan sweep + slim test-data discipline** (one PR — the BIG one):
- Investigate why per-worker test DBs are ~150 MB each. Hypothesis: template carries seed data from migrations that should not own user data.
- Codify test-data discipline: minimal seeds (1-5 instruments per test), TRUNCATE-based teardown not whole-DB-rebuild. Tests test LOGIC, not data scale.
- Orphan sweep: `_drop_orphan_workers_older_than(min_age='1h')` in `build_template_if_stale`. Snipes `ebull_test_*_gw*` graveyards.
- Pre-push hook: warn if `pg_database_size('ebull') > 10 GB`.

**Sub 3 — `financial_facts_raw` partition + retention review** (one PR):
- Partition `financial_facts_raw` by `period_end` quarterly buckets. Autovacuum operates per-partition; WAL bursts shrink ~24×.
- Per-table retention enforcement (skill §13 horizons): 10-K last 3 annual, 10-Q last 8 quarterly, etc. Spot-check `filing_raw_documents` (2.9 GB) compaction is firing.

**Sub 4 — Observability** (in Sub 1 or Sub 3 PR):
- `/system/postgres-health` endpoint exposing `pg_database_size`, leaked-DB count, current WAL size, last checkpoint time, autovacuum lag per top-10 tables.

**Sub 5 — Prevention-log entry** (in any PR):
- Document the "Postgres on Docker Desktop macOS defaults blow up partition-heavy workloads" trap with the specific knob list + the leaked-DB hazard.

**Acceptance**:
1. Postgres survives a heavy ingest cycle without WAL PANIC.
2. Fresh pytest run leaves zero leaked test DBs.
3. Dev-DB total < 5 GB after retention sweep.
4. `/system/postgres-health` returns metrics + pre-push hook warns on bloat.
5. Prevention-log entry merged.

**Out of scope (yet)**: HA / replication, K8s migration, WAL archiving / PITR. eBull is demo-first; production-grade DB ops is a separate epic.

**Ticket**: #1208 — full sub-ticket breakdown + operator runbook.

## Handover — Phase 1 (PRs 1+2, session 1)

### Handover — PR #1190 (open 2026-05-17)

- Phase: 1
- Gap / ticket closed: **G7** (`sec_xbrl_facts` synth no-op manifest parser)
- Branch: `feat/g7-sec-xbrl-facts-synth-noop`
- Merge SHA: pending (awaiting Claude review bot + CI on most recent commit)
- Tests added:
  - `tests/test_manifest_parser_sec_xbrl_facts.py` — 4 tests (happy-path drain to `parsed`, form-agnostic seed (`10-K/A`), registry-wiring after `clear_registered_parsers` + `register_all_parsers`, durability gate proving the parser never calls `conn.execute` / `conn.cursor` / `conn.transaction` / `store_raw` / `fetch_document_text`).
- Scope discoveries handled in-scope:
  - `tests/test_fetch_document_text_callers.py` allow-list extension (Codex round-1 HIGH — the test would otherwise have flagged the parser/test pair as stale entries).
  - Module docstring updated to explicitly state the non-caller invariant (mirrors the sec_10q.py #1168 contract symmetry).
  - Cross-check table name corrected: `company_facts` → `financial_facts_raw` (the actual Companyfacts bulk-ingest target via `upsert_facts_for_instrument`).
- Matrix delta:
  - §2 row `sec_xbrl_facts` — `❌ by design` → `✅ sec_xbrl_facts.py (G7)`, status `WIRED`.
  - §7 G7 — `BY DESIGN` → `✅ CLOSED 2026-05-17`.

### Handover — PR #1191 (open 2026-05-17)

- Phase: 1
- Gap / ticket closed: **G14** (`bootstrap_orchestrator` source-registry entry)
- Branch: `feat/g14-bootstrap-orchestrator-source-registry`
- Merge SHA: pending (awaiting Claude review bot + CI on most recent commit)
- Tests added:
  - `tests/test_bootstrap_orchestrator_source_registry.py` — 5 tests (registry membership + value, `source_for` resolves without `KeyError`, `JobLock` constructs cleanly at the original KeyError site, `publish_manual_job_request` lands the queue row with no rejection, **disjointness invariant** walking `_BOOTSTRAP_STAGE_SPECS` directly + asserting no stage resolves to the `bootstrap` lane).
- Scope discoveries handled in-scope:
  - **Plan called for `init` source — that was wrong.** Codex round-1 BLOCKING surfaced the cross-thread `ContextVar` bug: bootstrap's `ThreadPoolExecutor` workers do NOT inherit `_HELD_SOURCES`, so the #1184 same-context re-entrancy bypass cannot fire from inside a stage worker. Any source shared with an inner stage (`init` with `nightly_universe_sync`; `db` with several Phase E stages; etc.) would have the worker thread hit `pg_try_advisory_lock` on a key the listener thread already holds → `JobAlreadyRunning` → stage fails.
  - **Pivoted to a fresh `bootstrap` lane** added to the `Lane` Literal in `app/jobs/sources.py`. Disjoint from every per-stage lane by construction; cross-thread inner acquisitions never contend with the outer lock. Multiple bootstrap triggers still serialise via the `bootstrap` advisory lock; `bootstrap_state.status='running'` remains the primary trigger-publish-time fence.
  - Added the disjointness invariant test that walks `_BOOTSTRAP_STAGE_SPECS` directly (Codex round-2 suggestion) so a future stage addition that uses `lane='bootstrap'` fails CI loudly.
- Matrix delta: G14 closes US-source-coverage memory hole flagged as PR #1188 T9-POST follow-up. No `.claude/skills/data-engineer/etl-endpoint-coverage.md` row touched (G14 is registry plumbing, not a discovery/parser entry).
- **Prevention-log candidate (extract on review)**: "ThreadPoolExecutor workers don't inherit `ContextVar`; pick a disjoint source for invokers that fan stages out to threads." Code/test comments capture this verbatim — extract to `docs/review-prevention-log.md` on first review round if the bot doesn't already cite a sibling entry.

### Next phase

- **Phase 1, PR 3 — G13 (`subjects_due_for_recheck` reader verification).** Plan §2 Phase 1 — AST audit + integration test verifying `subjects_due_for_recheck` reader path actually fires in Layer 3's `run_per_cik_poll`. If wiring exists per memory `[[us-source-coverage]]` "G13 recheck path added" — close G13 with test only. If wiring missing — fix in-scope.
- **Operator: post-merge follow-up.** Once PR #1190 + #1191 land, the admin "Retry failed" path can drop the direct-Python workaround. Confirm the remaining 5 bootstrap stages from #1187 retry land `bootstrap_state.status='complete'` end-to-end via the proper queue-listener path.

### Handover — PR #1193 (merged 2026-05-17)

- Phase: 1
- Gap / ticket closed: **G13** (`subjects_due_for_recheck` reader verification — sub-finding of #1155)
- Branch: `feat/g13-verify-recheck-reader-wiring`
- Merge SHA: `078a5e68bf9690a8b5ed6db2d9bd118a05895232`
- Tests added:
  - `tests/test_g13_recheck_reader_invariants.py` — 4 tests (imports invariant, consumed-call invariant, local-rebind invariant, return-annotation + `PerCikPollStats.recheck_*` fields invariant) under a single `_RunPerCikPollVisitor` traversal with symmetric nested-scope skips.
- Plan-defined acceptance: **closed-with-tests-only.** Wiring already exists at `app/jobs/sec_per_cik_poll.py:195-198` (added in #1155); integration coverage already at `tests/test_sec_per_cik_poll.py::TestG13RecheckPath`; hourly cadence + prereq + source already at `tests/test_layer_123_wiring.py::test_layer3_per_cik_poll_registered`. The PR added the static AST safety-net only.
- Scope discoveries handled in-scope:
  - Codex round 1 (medium): naive `ast.walk(fn)` recursed into nested scopes; reader-call result not checked as consumed. Both addressed by `_ConsumedReaderVisitor` with nested-scope skips + materialiser / iteration-shape check.
  - Codex round 2 (medium): local-rebind gap (a future stub `subjects_due_for_recheck = lambda: iter([])` would defeat the consumed-call invariant). Addressed by `test_reader_names_not_locally_rebound`.
  - Codex round 2 (note): `if False:` reachability — REBUTTED in PR body (full Python reachability is undecidable; the contrived form does not reflect a realistic regression; runtime reachability is owned by the integration suite).
  - Bot review round 1 (WARNING): rebind check used `ast.walk(stmt)` which recursed into nested scopes — asymmetric with the consumed-call visitor that skipped them. **FIXED** by merging both invariants into a single `_RunPerCikPollVisitor` (one traversal, one scope-skip rule). Module docstring records the scope-walk discipline so future intra-function AST checks reuse the unified visitor.
  - Bot review round 1 (PREVENTION): scope-walk asymmetry. **EXTRACTED** in-file (test module docstring "Scope-walk discipline" section). Not yet repo-wide-extracted to `docs/review-prevention-log.md` because this is currently a single-file pattern; if a second test file ever adds the same pattern, escalate.
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §3 sub-gap G13 narrative — "production never reaches it" → ✅ CLOSED narrative.
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §7 gap register G13 row — `OPEN` → `✅ CLOSED 2026-05-17`.
- ETL clauses #8-#12 — N/A (test-only PR, no parser / schema / data-path change; documented in PR body).

### Phase 1 close-out — all three PRs merged

PR 1 (G7 #1190) + PR 2 (G14 #1191) + PR 3 (G13 #1193) all merged. Phase 1 complete.

### Next phase (Phase 2 entry)

- **Phase 2, PR 5 — G9 (`company_tickers_mf.json` consumer).** ✅ CLOSED in-scope by PR #1194 (G8) — stale audit entry corrected. Consumer existed since #1171 (`refresh_mf_directory` bundled into `daily_cik_refresh` Stage 6) + #1174 (S25 `mf_directory_sync` dedicated bootstrap stage). Matrix §2 + §4 + §7 updated accordingly. No new code needed.

## Handover — Phase 3 (PR 6, session 3)

### Handover — PR #1196 (merged 2026-05-17)

- Phase: 3
- Gap / ticket closed: **G12** (`master.idx` quarterly cross-quarter walker)
- Branch: `feat/g12-master-idx-quarterly-walker` (deleted post-merge)
- Merge SHA: `e48eba3` (squash)
- Tests added:
  - `tests/test_sec_full_index_provider.py` — 11 unit tests (URL builder, quarter-start anchor, strict-vs-tolerant 404 contract, malformed-date fallback).
  - `tests/test_sec_master_idx_quarterly_sweep.py` — 24 integration + resolver-priority tests against `ebull_test_conn` (quarter-boundary helpers parametrised, happy path, unmapped-form skipping, asymmetric 404 contract — CQ-tolerant vs CQ-1-strict, per-quarter txn isolation for both `sec_filing_manifest` AND `data_freshness_index`, commit-before-next-quarter durability, explicit `quarters` kwarg path, ON CONFLICT preserves `ingest_status`, preloaded-resolver priority chain `issuer > institutional_filer > blockholder_filer`, unknown-CIK None, blockholder-only cohort).
  - `tests/test_sec_master_idx_scheduler_wiring.py` — 5 wiring invariants (constant value, ScheduledJob entry shape, `_INVOKERS.__wrapped__` identity, `source_for()` resolves).
  - `tests/test_universal_gate_carve_out.py` — added positive assertion that G12 is NOT in the exempt allow-list.
  - `tests/test_layer_123_wiring.py` — added Layer-4 row asserting full ScheduledJob shape.
- Scope discoveries handled in-scope:
  - **HIGH (Codex 1a r1)**: per-quarter txn cascade trap. Fixed via `conn.commit()` on success + `conn.rollback()` on failure inside the per-quarter try/except — preserves the per-quarter failure-isolation contract for BOTH `sec_filing_manifest` AND `data_freshness_index` writes (cross-table rollback proved by test 9). Tests 9 + 10 pin the contract.
  - **HIGH (Codex 1a r1)**: 404-ambiguity trap. `read_master_idx` strict-by-default; only the current calendar quarter passes `allow_404=True`. Previous-quarter 404 surfaces as `QuarterStats(failed=True)`. Test 7 pins the asymmetric contract.
  - **HIGH (Codex 1a r1)**: outage-window invariant ownership. >1-quarter recovery is an explicit Python REPL runbook against `run_master_idx_quarterly_sweep(conn, ..., quarters=[(YYYY,Q), ...])`. NO operator-facing `params_metadata` surface (avoids cross-cutting `multi_quarter` ParamFieldType extension).
  - **MED (Codex 1a r1)**: resolver hot path. `build_preloaded_subject_resolver(conn)` materialises a `dict[cik, ResolvedSubject]` once per fire (~17k entries / ~1.5 MB), returns O(1) closure. Replaces the per-row 3-table default. Priority chain `issuer > institutional_filer > blockholder_filer` via `setdefault`. Tests 14 + 16 pin both priority steps.
  - **MED (Codex 1a r1)**: cohort-correct smoke panel. AAPL ≠ 13F-HR (issuer-scoped CIK); 13F-HR is filer-scoped (Berkshire / BlackRock per `institutional_filers`). Spec acceptance §9 corrected.
  - **HIGH (Codex 1b r1)**: FK seed order. Test helper `_seed_issuer` inserts into `instruments(instrument_id, symbol, company_name)` BEFORE `instrument_sec_profile(instrument_id, cik)` — verified against `sql/001_init.sql:1-4` after r2 caught the wrong-PK-name version.
  - **HIGH (Codex 1b r1)**: blockholder cohort missing from tests. Added `_seed_blockholder_filer` helper + test 16 priority assertion + dedicated blockholder-only resolution test.
  - **LOW (Codex 1b r1)**: `_INVOKERS` identity pin via `.__wrapped__ is sec_master_idx_quarterly_sweep` (NOT comparing against a fresh `_adapt_zero_arg(...)` call which returns a new closure each time).
  - **HIGH (Codex 2 pre-push)**: partial-quarter failure was recorded as job success. Invoker now raises `RuntimeError` if `stats.failed_quarters > 0` so `_tracked_job` records `job_runs.status='failure'` with per-quarter detail. Successful quarters still commit before the raise — partial work is durable; the failure signal is for operator visibility only.
  - **LOW (Codex 2 r2)**: docstring said `status='error'`; actual contract is `status='failure'` per `record_job_finish` + SQL CHECK. Fixed.
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §3 (new Layer-4 row) + §4 (full-index quarterly row `❌ GAP` → `✅ WIRED 2026-05-17 (G12)`) + §7 G12 row (`OPEN (low)` → `✅ CLOSED 2026-05-17 — PR #1196 merge e48eba3`).
  - `.claude/skills/data-sources/sec-edgar.md` §1 (full-index quarterly row gets consumer annotation).
- Codex iteration counts:
  - 1a (spec): 3 rounds to CLEAN — round-1 3 HIGH + 3 MED + 1 LOW; round-2 1 residual stale text; round-3 minor wording residual.
  - 1b (plan): 3 rounds to CLEAN — round-1 2 HIGH + 2 MED + 2 LOW; round-2 schema-correct seed helper + fixture name precision; round-3 CLEAN.
  - 2 (pre-push): 2 rounds to CLEAN — round-1 1 BLOCKING (uncommitted) + 1 HIGH (partial-failure visibility); round-2 1 LOW (docstring `status='error'` → `'failure'`).
- ETL clauses #8-#12: NOT APPLICABLE end-to-end. G12 is a discovery primitive — UPSERTs manifest rows that downstream parsers ingest. No per-instrument figure change (clauses 9-11 N/A). Clause 8 smoke: G12 invoker can be triggered manually post-merge via the admin UI; assert manifest rows materialise for the most recent quarter. Documented in PR body.
- Spec: `docs/superpowers/specs/2026-05-17-g12-master-idx-quarterly-walker.md` (CLEAN v3).
- Plan: `docs/superpowers/plans/2026-05-17-g12-master-idx-quarterly-walker-plan.md`.

### Next phase (Phase 4 entry)

- **Phase 4, PR 7 — G10 (`companyconcept` API consumer).** ✅ MERGED 2026-05-18 PR #1198 `0ead989` — see handover block below.
- **Phase 4, PR 8 — G11 (`frames` API consumer).** Decision rule from plan: if `gh issue list --search "frames OR sector heatmap OR cross-sectional"` returns an open feature ticket, wire the consumer; otherwise close G11 as BY DESIGN with documentation.

## Handover — PR #1198 (merged 2026-05-18)

- Phase: 4
- Gap / ticket closed: **G10** (`companyconcept` API not consumed)
- Branch: `feat/g10-companyconcept-api-consumer` (deleted post-merge)
- Merge SHA: `0ead989` (squash)
- Closure framing: **PROVIDER PRIMITIVE** (not WIRED) — `fetch_concept` + `extract_concept_facts` exposed on `SecFundamentalsProvider`; **no production consumer in v1 by design.**
- Tests added:
  - `tests/test_sec_fundamentals_companyconcept.py` — 35 tests (URL builder + 404 → None + 5xx → raise [with `Request` attached so `raise_for_status` fires cleanly] + taxonomy validation [10 bad cases incl. trailing dash + leading dash + trailing newline + 6 legitimate cases] + tag validation [6 bad + 3 legitimate] + extractor reuse with integer USD revenue fixture + `Decimal(str(...))` boundary on float `USD/shares` EPS [pins prevention-log #1174] + empty on 404 + missing-units warning [#1204 close-out] + taxonomy-mismatch warning [Codex 2 r1 LOW-2] + rate-limit clock identity assertion + back-to-back throttle behaviour via `httpx.MockTransport`).
- Scope discoveries handled in-scope:
  - **Plan called for `fundamentals_sync` opt-in. That was wrong.** Spec §3.1 audit: under the 10 req/s shared SEC rate budget (`min_request_interval_s = 0.11` enforced via process-wide `_PROCESS_RATE_LIMIT_CLOCK`), companyconcept LOSES wall-clock to companyfacts for any consumer needing ≥2 tags per CIK. Snapshot path = 18 × 0.11 s ≈ 2.0 s vs companyfacts 1 × 0.11 s + ~0.5 s payload ≈ 0.5-1.0 s. Full extract path (`refresh_financial_facts`) is 81 tag variants × 0.11 s ≈ 9 s/CIK + would drop the post-#451 "every concept lands in `financial_facts_raw`" semantics. Conclusion: primitive lands; no production wire-up. Future single-tag consumer tickets (#435 dilution tracker; operator probes) re-open the wiring question.
  - **Closure framing `✅ WIRED` would overclaim** (Codex 1a r1 HIGH-1). Matrix §4 + §7 row carry `✅ PROVIDER PRIMITIVE 2026-05-17 (G10)` — distinct status from production-consumer-wired rows.
  - **Future-consumer raw-payload invariant codified** (Codex 1a r1 HIGH-2). Spec §3.3 binds any subsequent caller PR that wires this primitive into a DB writer to land raw-payload persistence per prevention-log #1168 IN THE SAME PR. Provider docstring cites the spec so a future consumer-PR's self-review surfaces the obligation.
  - **Taxonomy validation widened to SEC-syntax** (Codex 1a r1 MED-3). Original spec restricted to `{us-gaap, dei}`; widened to `^[a-z](?:[a-z0-9-]*[a-z0-9])?$` via `fullmatch` so the primitive accepts every published SEC taxonomy namespace (`srt`, `invest`, `country`, `ifrs-full`, …) — NOT bound to `TRACKED_CONCEPTS` / `DEI_TRACKED_CONCEPTS`. Those maps govern downstream normalisation, not arbitrary probe access. PR #1198 bot round-1 NITPICK on trailing-dash gap (`"us-gaap-"`) FIXED 504a070 by adding the trailing-alnum anchor.
  - **`fullmatch` discipline** (Codex 1b r1 MED-1). `re.match` + `^...$` admits a trailing `\n` because `$` matches before final newline. Both `_TAXONOMY_RE` and `_CONCEPT_TAG_RE` use `fullmatch`. Tests parametrise `"us-gaap\n"`, `"Revenues\n"`, etc.
  - **Test fixture realism** (Codex 1b r1 MED-2). Integer USD Revenues fixture stays integer; float-boundary exercise moved to dedicated EPS `USD/shares` test that pins prevention-log #1174.
  - **5xx `Request` attachment** (Codex 1b r1 MED-3). Bare `httpx.Response(500)` has no `Request`; `raise_for_status()` then raises wrong-fixture error. Test now constructs `httpx.Response(500, request=httpx.Request("GET", "..."))`.
  - **Rate-limit clock test cleanup** (Codex 1b r1 LOW-4). `_PROCESS_RATE_LIMIT_CLOCK[0]` reset to 0.0 in teardown so shared-clock mutation doesn't bleed into the rest of `uv run pytest`.
  - **`ResilientClient` retry override in test wrapper** (Codex 2 r1 LOW-3). `_rewire_transport` accepts `max_retries=0` (default) — 5xx test drops from ~7 s (default exponential backoff) to ~0.1 s.
  - **Tag-regex NCName overclaim narrowed** (Codex 2 r1 MED). Module comment now states the regex is "a deliberately tightened subset of legal XBRL NCName syntax — every SEC-observed concept name uses `[A-Za-z][A-Za-z0-9_]*`; widen + add regression test if SEC drift surfaces a legitimate NCName outside this subset" rather than claiming "every legal XBRL concept name."
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §4 row `data.sec.gov/api/xbrl/companyconcept/...` — `❌ GAP` → `✅ PROVIDER PRIMITIVE 2026-05-17 (G10)` with file:line + audit summary.
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §7 G10 row — `OPEN (low)` → `✅ CLOSED 2026-05-17 — G10 PR`.
  - `.claude/skills/data-sources/sec-edgar.md` §1.6 Companyconcept row — consumer annotation added.
- Codex iteration counts:
  - 1a (spec): 3 rounds to CLEAN — round-1 2 HIGH + 4 MED + 2 LOW; round-2 2 MED + 2 LOW; round-3 2 LOW (header v2-vs-v3 + MockTransport injection).
  - 1b (plan): 2 rounds to CLEAN — round-1 3 MED + 1 LOW + 3 LOW-confirmations; round-2 2 LOW (numbering drift + 5xx mechanics wording).
  - 2 (pre-push): 1 round — 1 MED (NCName overclaim) + 2 LOW (mismatch test + retry override) all addressed.
  - Bot review: round 1 APPROVE + 1 NITPICK (trailing-dash); round 2 APPROVE on format-fix; round 3 APPROVE on NITPICK-fix (504a070).
- ETL clauses #8-#12 — N/A end-to-end. G10 is a provider primitive; no schema / parser / observations / rollup change; no per-instrument figure touched. Documented in PR body. Architectural audit in spec §3.1 is the audit-of-record.
- Spec: `docs/superpowers/specs/2026-05-17-g10-companyconcept-api-consumer.md` (CLEAN v3 through Codex 1a r1+r2+r3 + Codex 2 r1).
- Plan: `docs/superpowers/plans/2026-05-17-g10-companyconcept-api-consumer-plan.md` (CLEAN v2 through Codex 1b r1+r2 + Codex 2 r1).
- **Operator follow-up:** none. The primitive has no DB / scheduler / lifespan touch. The smoke gate would test the same provider import path that the targeted test file already exercises.

### Next phase (Phase 4 PR 8)

- **Phase 4, PR 8 — G11 (`frames` API consumer).** ✅ MERGED 2026-05-18 PR #1200 `c954c50` — see handover block below.

## Handover — PR #1200 (merged 2026-05-18)

- Phase: 4
- Gap / ticket closed: **G11** (`frames` API not consumed)
- Branch: `feat/g11-frames-api-consumer` (deleted post-merge)
- Merge SHA: `c954c50` (squash)
- Closure framing: **PROVIDER PRIMITIVE** (not WIRED) — `fetch_frame` exposed on `SecFundamentalsProvider`; **no production consumer in v1 by design.**
- Decision-rule output: `gh issue list --search "frames OR sector heatmap OR cross-sectional"` returned #594 (peer-comparison radar + sector heatmap) — but #594 explicitly says "sector aggregates — needs sector median calculations server-side, OR client-side aggregation across the peer set." Frames is one option among several; #594 does NOT commit to it. Codex G11-scope review independently recommended primitive-only over full-pipeline wire. Refs #594.
- Tests added:
  - `tests/test_sec_fundamentals_frames.py` — 66 tests (9 logical with parametrise): URL builder + 404 → None + 5xx → raise (Request attached + `max_retries=0` drops test from ~7s to ~0.1s) + taxonomy validation (10 bad + 6 happy, G10 literals duplicated verbatim) + tag validation (6 bad + 3 happy) + unit validation (11 bad + 9 happy incl. `USD-per-shares`, `Y-per-shares`, lowercase `usd`) + period validation (10 bad incl. `CY####I` rejection + 5 happy) + payload-parsing fixture (Apple FY2024 Revenues `cik=320193`, `val=391035000000`) + rate-limit clock identity + back-to-back throttle smoke via `httpx.MockTransport`.
- Scope discoveries handled in-scope:
  - **`_UNIT_RE` grammar pivoted from char-class to `token(-per-token)?`** (Codex 1a r1 HIGH). Initial draft used `[A-Za-z][A-Za-z0-9-]*` which admitted `USD/shares` semantics under the wrong syntax — SEC frames URLs use `-per-` not `/`. Final grammar `[A-Za-z][A-Za-z0-9]*(?:-per-[A-Za-z][A-Za-z0-9]*)?` rejects double-dash (`USD--per-shares`), bare `-per` (`USD-per`), trailing dash (`USD-per-`), slash, and every URL-special character.
  - **`CY####I` annual-instantaneous rejection** (Codex 1a r1 OK). SEC docs define `Q#I` as instantaneous (balance-sheet); no annual-instantaneous frame exists. `_PERIOD_RE` pins the rejection.
  - **Lowercase `usd` admitted, not rejected** (Codex 1b r1 HIGH). Primitive is general SEC frames consumer, NOT bound to a known-unit allowlist. Moved to `_GOOD_UNIT`.
  - **Fixture realism — `Revenues` paired with `CY####` annual not `CY####Q#I`** (Codex 1b r1 MED). `Q#I` is balance-sheet only; `Revenues` is a flow. Test 8 fixture uses `CY2024` annual with Apple FY2024 figure; test 1 URL pivoted to balance-sheet concept `Assets` paired with `CY2024Q1I`.
  - **G10 test-literal counts pinned verbatim** (Codex 1b r1 LOW). Plan now says "duplicate G10 `_BAD_TAXONOMY` / `_GOOD_TAXONOMY` / `_BAD_TAG` / `_GOOD_TAG` literals verbatim" — exact-mirror prevents accidental coverage drift.
  - **Module-level comment block generalised** (Codex 1a r1 LOW-4). `_TAXONOMY_RE` + `_CONCEPT_TAG_RE` + new `_UNIT_RE` + `_PERIOD_RE` all live under one comment block citing both G10 + G11 specs.
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §4 row `data.sec.gov/api/xbrl/frames/...` — `❌ GAP` → `✅ PROVIDER PRIMITIVE 2026-05-18 (G11)` with audit summary.
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §7 G11 row — `OPEN (low)` → `✅ CLOSED 2026-05-18 — G11 PR`.
  - `.claude/skills/data-sources/sec-edgar.md` §1.6 Frames row — consumer annotation incl. `-per-` vs `/` syntax warning + period-shape reference.
- Codex iteration counts:
  - 1a (spec): 3 rounds to CLEAN — round-1 1 HIGH + 1 LOW + 2 OK; round-2 1 MED + 1 LOW; round-3 1 LOW (docstring sync).
  - 1b (plan): 2 rounds to CLEAN — round-1 1 HIGH + 2 MED + 1 LOW + 3 OK; round-2 1 MED + 1 LOW (stale prose).
  - 2 (pre-push): 1 round — NO FINDINGS, CLEAN.
  - Bot review: round 1 APPROVE on first push, no findings.
- ETL clauses #8-#12: N/A — provider primitive only.
- Spec: `docs/superpowers/specs/2026-05-18-g11-frames-api-consumer.md` (CLEAN v4 through Codex 1a r1+r2+r3).
- Plan: `docs/superpowers/plans/2026-05-18-g11-frames-api-consumer-plan.md` (CLEAN v2 through Codex 1b r1+r2).

### Phase 4 close-out — PRs 7 + 8 merged

PR 7 (G10 #1198 `0ead989`) + PR 8 (G11 #1200 `c954c50`) both merged 2026-05-18. Phase 4 complete. **All four `data.sec.gov/api/xbrl/*` endpoints (companyfacts, companyconcept, frames) now have provider surfaces; companyfacts is the only one with a production consumer in v1.**

### Phase 5 entry

- **Phase 5, PR 9 — #925 EdgarTools 13F-HR parser drop-in.** ✅ CLOSED 2026-05-18 PR #1203 merge `68e56c1` — pre-impl spike found #925 already shipped via PR #931 (2026-05-05, commit `0428dbf`); plan §2 PR 9 alternative scope REBUTTED-INFEASIBLE.
- **Phase 5, PR 10 — #932 EdgarTools N-PORT FundReport drop-in.** ✅ SHIPPED 2026-05-18 PR #1205 merge `7826109` — see handover block below.

## Handover — PR #1203 (merged 2026-05-18)

- Phase: 5
- Gap / ticket closed: **#925** (EdgarTools 13F-HR parser drop-in)
- Branch: `fix/925-13f-edgartools-spike-closeout` (deleted post-merge)
- Merge SHA: `68e56c1` (squash)
- Bot review: APPROVE on first push — "No findings. docs + matrix + comment-only change; no logic, no audit trail, no execution guard surface touched."
- CI: all 4 checks green on first push (lint, review, supply-chain, verify-issue-link).
- Closure framing: **DONE-AS-FRAMED-PRE-DATED** — PR #931 (`feat(#925): adopt EdgarTools as 13F-HR parser drop-in`, merged 2026-05-05, commit `0428dbf`) shipped #925's full scope verbatim (`pyproject.toml` pin, parser internals wrapped at `app/providers/implementations/sec_13f.py:20-54`, Berkshire 2024Q3 golden replay at `tests/test_sec_13f_parser.py:380-441`). The issue remained administratively OPEN only because PR #931 predated the auto-close CI enforcement gate (#942).
- Spike doc: `docs/superpowers/spikes/2026-05-18-13f-hr-edgartools-feasibility.md`. Same shape as `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` per plan §3 precedent.
- Tests added: **none.** The existing 20-test suite at `tests/test_sec_13f_parser.py` (incl. Berkshire 2024Q3 golden replay) already exercises the EdgarTools-wrapper path; `uv run pytest tests/test_sec_13f_parser.py -x -q` passes 20/0 against EdgarTools 5.30.2.
- Scope discoveries handled in-scope:
  - **Plan §2 PR 9 alternative scope REBUTTED on five binding grounds** (spike §7):
    1. Rate-limit pool bypass — `Filing.obj()` triggers SEC fetches via EdgarTools' `HTTP_MGR` which does NOT participate in `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK` (prevention-log:510-513 + `sec_edgar.py:54-80, 237-253`).
    2. VALUE cutover semantics divergence — EdgarTools' `report_period`-keyed cutoff at `2022-09-30` regresses eBull's correct `filed_at`-keyed handling of post-2023-01-03 amendments (3-OOM regression on amendment cohorts).
    3. No native PRN/SH drop — `parse_infotable_xml` preserves `Type='Principal'`; drop logic must remain in manifest adapter (confirmed at `edgar/thirteenf/parsers/infotable_xml.py:37, 102-103` + `models.py:458-462`).
    4. No raw-payload persistence hook — `requires_raw_payload=True` contract (#1168) cannot be satisfied through `Filing.obj()` alone.
    5. No transient-vs-deterministic error classification + ingest-log audit trail — eBull-specific service-layer concerns.
  - **Fix-in-scope (small, coupled, unblocked)**: `app/services/manifest_parsers/sec_13f_hr.py:91-102` comment was internally contradictory ("Pre-cutover amendments filed late still report thousands" conflicted with the example "a 2022Q4 restatement landed in March 2023 would carry dollars"). Code was correct; only the comment was muddled. Rewrote the comment to mirror the unambiguous wording at `sec_13f_dataset_ingest.py:316-322`. The cutover constant is now at `sec_13f_hr.py:103`; the scaling-branch flag at `:397`; the per-row scaling at `:419-421`; the PRN drop at `:415-417`.
  - **Matrix annotation stale**: `.claude/skills/data-engineer/etl-endpoint-coverage.md:43` said "PRN drop + 2023-01-03 VALUE cutover applied parser-side"; that's service-side (manifest-adapter), not parser-side. Same drift at `.claude/skills/data-sources/sec-edgar.md:616`. Both fixed in-scope with corrected file:line anchors + EdgarTools-wrapper provenance via #931.
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 `sec_13f_hr` row — annotated with EdgarTools-wrapper provenance (#931, 2026-05-05) + corrected service-side cutover/PRN-drop location + spike-doc citation.
  - `.claude/skills/data-sources/sec-edgar.md` §11 (error-handling table) `13F-HR` row — same fix.
- Codex iteration counts:
  - 1a (spec): 2 rounds to CLEAN — round-1 2 MED + 2 LOW; round-2 CLEAN.
  - 1b (plan / matrix delta): pending.
  - 2 (pre-push): pending.
- ETL clauses #8-#12 — N/A. This PR is docs + matrix + one comment-only code edit; no parser / schema / observations / rollup change. No per-instrument figure changes. The existing 20-test Berkshire 2024Q3 golden replay (`tests/test_sec_13f_parser.py`) re-exercises the EdgarTools-wrapper path through `uv run pytest`.
- Operator follow-up: **none.** The EdgarTools-wrapper path has been live in production since PR #931 (2026-05-05); this PR only updates the documentation surface + closes #925.

## Handover — PR #1205 (merged 2026-05-18)

- Phase: 5
- Gap / ticket closed: **#932** (EdgarTools N-PORT FundReport parser drop-in)
- Branch: `fix/932-nport-edgartools-spike` (deleted post-merge)
- Merge SHA: `7826109` (squash)
- Bot review: APPROVE on first push — zero findings; all 4 checks green (lint/review/supply-chain/verify-issue-link).
- CI: clean on first push.
- Closure framing: **drop-in shipped** — lazy-imported wrapper over `edgar.funds.reports.FundReport.parse_fund_xml` at `app/services/n_port_ingest.py::parse_n_port_payload`. Preserves `NPortFiling` / `NPortHolding` public dataclass surface. Parser-version bumped `nport-v1` → `nport-v2-edgartools`; auto-propagates to `app/services/manifest_parsers/sec_n_port.py` via direct symbol import.
- Spike doc: `docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md` (CLEAN through Codex 1a r3).
- Spec: `docs/superpowers/specs/2026-05-18-n-port-edgartools-dropin.md` (CLEAN through Codex 1a r4).
- Plan: `docs/superpowers/plans/2026-05-18-n-port-edgartools-dropin-plan.md` (CLEAN through Codex 1b r4).
- Tests added/modified:
  - `tests/test_n_port_ingest.py::TestParseNPortPayload` rewritten against real Vanguard Value Index Fund fixture; **new golden replay** test (`test_golden_replay_first_row_count_total`) locks JPM top holding + total $217.4B + 323 count; **new** `test_raises_on_empty_xml` (Codex 2 r1 XMLSyntaxError edge case).
  - `TestIngestFundNPort` 3-of-4 refactored to monkeypatch parser via `_make_filter_path_filing()` helper; 1 (missing-series) stays integration.
  - `tests/test_manifest_parser_sec_n_port.py` retargeted at JPM (real-fixture top holding) + `parser_version == "nport-v2-edgartools"` assertion.
- Scope discoveries handled in-scope:
  - **Risk-register fallback fired (T1)**: `S000002277` (Vanguard 500 Index Fund per synthetic-fixture claim) NOT under CIK 36405. Iterated 313 NPORT-P refs; selected most recent with ≥100 holdings + complete `valUSD` coverage = Vanguard **Value** Index Fund (S000002840). T4/T5/T10 assertions derived verbatim from T1-RESULTS table; series_id literal locked to `S000002840`.
  - **Codex 2 r1 MEDIUM (XMLSyntaxError)**: `FundReport.parse_fund_xml("")` raises `lxml.etree.XMLSyntaxError` AFTER recover=True fallback also fails. Added to wrapper catch list via `_lxml_syntax_error()` lazy-import factory. Regression test added.
  - **Codex 1a r1 HIGH (catch scope too narrow)**: try/except now wraps BOTH `parse_fund_xml` call AND post-parse normalisation; `NPortMissingSeriesError` + `NPortParseError` re-raised explicitly.
  - **Codex 1a r1 HIGH (fixture under-specified)**: missing-series fixture rewritten with all EdgarTools structural requirements (filerInfo + 9 fundInfo decimals + othMon1/2/3 + GeneralInfo non-Optional text fields) per `edgar/funds/reports.py:126-145` Pydantic model inspection.
  - **Codex 1a r1 MED (whitespace normalisation)**: wrapper strips `units` / `payoff_profile` / `asset_category` / `issuer_category` / `issuer_name` (defence against future EdgarTools whitespace-preservation changes; the ingester's exact-equality guards would otherwise mis-drop valid rows).
  - **Codex 1a r2 HIGH (GeneralInfo / ReturnInfo Pydantic-required fields)**: `regName` / `regCik` / `regFileNumber` / `regStreet1` non-Optional `str`; `<othMon1/2/3/>` non-Optional `RealizedChange`. Fixture extended.
  - **Skill update**: `.claude/skills/data-sources/edgartools.md` §G3 rewritten with structural-vs-Pydantic cliff distinction; the original §G3 "Pydantic validation cliff on `FundReport(**dict)`" framing was empirically wrong for edgartools 5.30.2 — `parse_fund_xml` returns a `Dict[str, Any]`, not a wrapped Pydantic model, and the observed crash on synthetic fixtures is structural (`<filerInfo>` block missing) not Pydantic-validation.
  - **Memory update**: `feedback_pydantic_validation_cliff.md` updated — Path A FEASIBLE shipped (NOT Path C lxml-rewrite as the stale memory recommended); two distinct cliff shapes documented (structural vs internal Pydantic).
- Matrix delta:
  - Live US source coverage matrix at `.claude/skills/data-engineer/etl-endpoint-coverage.md` — sec_n_port row already WIRED; no row status change. The change is parser-implementation, not coverage.
- Codex iteration counts:
  - 1a (spike): 3 rounds to CLEAN — round-1 5 findings; round-2 3 residuals; round-3 2 LOWs; round-4 CLEAN.
  - 1a (spec): 4 rounds to CLEAN — round-1 7 findings (2H + 4M + 2L); round-2 6 findings (2H + 1M + 3L); round-3 3 LOWs; round-4 CLEAN.
  - 1b (plan): 4 rounds to CLEAN — round-1 8 findings; round-2 2 residuals; round-3 2 LOWs; round-4 CLEAN.
  - 2 (pre-push): 2 rounds — round-1 1 MED (XMLSyntaxError escape); round-2 CLEAN.
  - Bot review: APPROVE on first push, zero findings.
- ETL clauses #8-#12:
  - #8 (smoke): N/A in operator-visible sense — N-PORT does not surface to standard smoke panel; rollup deferred to #919. Golden replay does exercise JPM (smoke-panel member) as canonical fixture's top holding.
  - #9 (cross-source): Independent raw XML spot-check against SEC EDGAR primary doc — JPM `value_usd = Decimal('7750046717.70000000')` matches SEC raw. True cross-source N/A until #919.
  - #10 (backfill): operator follow-up post-merge — `POST /jobs/sec_rebuild/run` body `{"source": "sec_n_port"}` resets manifest rows with `parser_version != 'nport-v2-edgartools'` to pending + redrains at 10 r/s shared budget.
  - #11 (operator-visible figure): N/A — no rollup endpoint surfaces N-PORT in v1; deferred to #919.
  - #12 (PR records verification + SHA): satisfied via PR body explicit N/A annotations + operator-trigger plan.
- Operator follow-up: **single endpoint call** — `POST /jobs/sec_rebuild/run` body `{"source": "sec_n_port"}` after merge to redrain N-PORT manifest rows with the new parser-version. Estimated ~200-1000 accessions across active fund-filer universe; ~20-100 second wall-clock at 10 r/s shared budget. Post-drain spot-check one Vanguard NPORT-P observation lands in `ownership_funds_observations` with `parser_version='nport-v2-edgartools'`.
- Prevention-log candidate: none — bot review found nothing; Codex 2 catch-list-scope finding was caught BEFORE push.

### Phase 5 close-out — both PRs merged

PR 9 (#925 closeout via #1203 `68e56c1`, 2026-05-18) + PR 10 (#932 drop-in via #1205 `7826109`, 2026-05-18) both merged. Phase 5 complete.

### Next phase (Phase 6 entry)

- **Phase 6, PR 11 — #915 FINRA bimonthly short interest ingest + schema.** Plan §2 Phase 6 — parent #796 + #845 (closed). Headline real coverage gap.
- **Phase 6, PR 12 — #916 FINRA RegSHO daily short volume ingest.** Same shape as PR 11. May bundle with PR 11 if cohesion tight.

## Handover — PR #1194 (merged 2026-05-17)

- Phase: 2
- Gap / ticket closed: **G8** (`company_tickers_exchange.json` consumer) + in-scope correction of stale G9 + matrix `company_tickers_mf.json` rows (consumer existed since #1171 / #1174 — audit entry stale)
- Branch: `feat/g8-company-tickers-exchange-directory` (deleted post-merge)
- Merge SHA: `30cd582347467670dc2690462af4e3662cf27faa` (squash)
- Tests added:
  - `tests/test_exchange_directory.py` — 12 service tests (happy-path / CIK zero-pad / multi-ticker CIK preserved / null exchange normalised / null ticker skipped / malformed row skipped / upsert idempotency / empty data / missing fields key / missing single field / field reordering / empty body raises).
  - `tests/test_daily_cik_refresh_sibling_enrichments.py` — 6 integration tests (sibling enrichments fire on 304 / hash-unchanged / full-upsert paths × Stage 6 fail-soft / Stage 7 fail-soft / both fail-soft).
  - `tests/test_daily_cik_refresh_scope.py` — added `_patch_sibling_enrichments` static method + applied to 2 non-raising tests (prevents live SEC fetch after T3 lands).
  - `tests/test_fetch_document_text_callers.py` — 2 new allow-list entries (`app/services/exchange_directory.py` + `tests/test_exchange_directory.py`) per #453 contract.
  - `tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` — added `cik_refresh_exchange_directory` for cross-test cleanup.
- Scope discoveries handled in-scope:
  - **Cohort observation (empirical 2026-05-17):** `company_tickers_exchange.json` shares the same row cohort COUNT as `company_tickers.json` (10,353) but is **ticker-grain not CIK-grain** — 7,996 unique CIKs / 1,446 multi-ticker CIKs. Plan's pre-cohort framing of "closes pink-sheet/OTC/foreign-without-ADR cohort gap" was empirically wrong; basic file already includes pink-sheet/OTC CIKs. The real value-add is the `(ticker, exchange)` mapping for preferred series (BAC=17 variants, JPM=9, MS=10), share-class siblings (GOOG/GOOGL), and ADR + OTC siblings (BABA/BABAF/BBAAY). Spec §1 / matrix §4 / sec-edgar.md §1 all corrected.
  - **MF Stage 6 latent skip fixed.** Pre-G8, Stage 6 MF refresh only fired on the full-upsert branch (304 / hash-unchanged early returns silently skipped it). The restructure makes Stage 6 + Stage 7 fire on every `daily_cik_refresh` invocation. Bootstrap-side authority remains `mf_directory_sync` (S25 #1174); this PR only fixes the daily-cron drift-heal path.
  - **PK granularity correction (Codex 1a round 2 HIGH 2).** Initial design was `PRIMARY KEY (cik)`; corrected to `(cik, ticker)` after re-counting the live payload showed 1,446 multi-ticker CIKs. A `(cik)`-only PK would have collapsed ~2,357 rows on every refresh.
  - **`fetch_document_text` allow-list update** added in-scope per #453 contract.
  - **Existing `test_daily_cik_refresh_scope.py` patched** to monkeypatch sibling refreshes — prevents live SEC fetch on the 2 non-raising tests that drive the real `daily_cik_refresh` after T3 lands (Codex 1b round-2 §3).
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 Stage 6 `cik_refresh` row — appended "+ Stage 7 exchange directory (G8, 2026-05-17)".
  - §4 reference endpoints row `www.sec.gov/files/company_tickers_exchange.json` — `❌ GAP` → `✅ WIRED 2026-05-17 (G8)` with full provenance.
  - §7 gap register G8 row — `OPEN (low)` → `✅ CLOSED 2026-05-17`.
  - `.claude/skills/data-sources/sec-edgar.md` §1 "Coverage gap" paragraph — rewritten with empirical ticker-grain correction + Stage 6/7 wiring map.
- Codex iteration counts:
  - Codex 1a (spec): 4 rounds to CLEAN — round-1 HIGH (early-return skip), HIGH (PK granularity), MED (txn ordering), MED (stale rows), MED (per-field tolerance), LOW (planner_tables), LOW (raw-payload-sink wording); round-2 HIGH (txn ordering wording precision); round-3 (allow-list update); round-4 CLEAN.
  - Codex 1b (plan): 3 rounds to CLEAN — round-1 HIGH (DAG dep T3→T2), HIGH (db_url monkeypatch), HIGH (FK seed), MED (stub-must-write), MED (non-string field guard), LOW (_PLANNER_TABLES sort); round-2 §1-3 (fixture names + seed helpers + scope-test live-fetch risk); round-3 CLEAN.
- ETL clauses #8-#12: Most N/A (reference-table snapshot, no per-instrument figure changes). Clause 9 spot-check: AAPL / GME / MSFT / JPM / HD against SEC live `company_tickers_exchange.json` — verification recorded in PR body.
- **Prevention-log candidate (extract on review):** "Re-count empirical cohort BEFORE finalising PK granularity for snapshot tables — same-count-row sets can still be many-to-one along the dimension you're keying on." Spec §1 documents the trap; if Codex / bot cites a sibling entry it ALREADY_COVERED, else EXTRACTED on first review round.
- Spec: `docs/superpowers/specs/2026-05-17-g8-company-tickers-exchange-directory.md` (v3 CLEAN).
- Plan: `docs/superpowers/plans/2026-05-17-g8-company-tickers-exchange-directory-plan.md` (v3 CLEAN).

## 6. Definition of done — for the whole plan

Plan is COMPLETE when:

1. All phases 1-8 merged.
2. `.claude/skills/data-engineer/etl-endpoint-coverage.md` matrix shows zero `OPEN` gaps in §7.
3. `[[us-source-coverage]]` memory caveat removed.
4. Operator confirms bootstrap reaches `status='complete'` AND next FULL orchestrator fire lands all db-lane + sec_rate-lane targets `success`.
5. `[[legacy-cron-retirement]]` pre-condition MET.

At that point: US ETL is "done, dusted, complete." Operator monitors daily steady-state via existing dashboards. Unexpected issues become anomalies, not known-bug churn.
