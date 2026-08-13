# Bootstrap ETL completion plan — done, complete, finito

**Status**: Proposal · 2026-05-25 · draft 5.2 (Codex v5.1 ledger normalised — all "51" refs → "55 GitHub + 1 internal")
**Owner**: TBD
**Target**: First-install bootstrap ≤ 60 min wall-clock + per-source documentation field-by-field complete + all 55 open ETL/Bootstrap/Admin/UI GitHub tickets + 1 internal work item resolved
**Baseline (Run #8, cancelled at S22 = 344min)**: 617 min total (10h17m)

**Operator framing** (verbatim, 2026-05-25): *"I want a done, completed process ... best in class, something others might want to adopt as a well defined model. No more danglers or thinking about punting things out of scope we know are an issue."*

Plan covers EVERY open ticket touching ETL / Bootstrap / Admin / UI. No deferred follow-ups. Phase 11 (completion verification) is the door-close gate.

---

## 1. The meta-issue — why we keep missing

Every failure mode in this session traced to dev-fixture-passes-prod-fails:

| Incident | Verification gap |
|---|---|
| #1255 MERGE attestation "7/7 EXPLAIN < 5s" | Dev fixtures empty `_current`; cost is `O(target_rows)` |
| Agent gap-memo: "S19/S20/S23 no-op post-bulk" | Agent didn't read invokers; live run proved each writes real rows |
| Original #1345: "JIT + partitions root cause" | Real cost is `WHEN NOT MATCHED BY SOURCE` seq-scan; empirical EXPLAIN caught it |
| Plan v1 §5 Phase 2 | Claimed `wait(all_futures)` serialises — code already uses `FIRST_COMPLETED`; Codex caught BEFORE code shipped |
| **Plan v4 §2 inventory** | Missed #1225 (open) + 18 ETL/UI tickets the gh API didn't return for Codex |

**The fix is process**: every ETL hot-path change must pass §4 verification protocol. Plus every plan revision passes Codex 1 sweep BEFORE code starts. THIS DOCUMENT had 4 BLOCKING rounds because the rule worked.

## 2. Complete open-ticket inventory (live-verified)

55 open GitHub tickets + 1 internal work item in scope (56 total). Each has ONE owning phase. Tickets referenced by other phases are noted as "(folded: see Phase N)".

### Performance / refactor (18)

| # | Title | Owning phase |
|---|---|---|
| #1225 | Bulk SEC ingesters land `bootstrap_stages.rows_processed=NULL` (precursor to #1273) | 0 |
| #1273 | Instrument long-pole bootstrap stages with target_count + processed_count | 0 |
| #1275 | Dispatcher `wait(all_futures)` — STALE per FIRST_COMPLETED at `bootstrap_orchestrator.py:2023`; re-investigation | 0.5 |
| #1346 | `SET LOCAL jit = off` × 10 ownership refresh helpers (verified 1.86×) | 1 |
| #1337 | Bulk-first epic (S8 cohort widening + S16 fast-path) | 2 |
| #1277 | S16 hardcodes `use_bulk_zip=False` — local zip parse | 2 |
| #1341 | S14 master.idx walk | 2 |
| #1340 | S23 NPORT trust bulk + `ManifestSubjectType` enum | 2 |
| #1305 | Bulk window depths short (13F=4q / NPORT=4q / insider=8q) | 2 |
| #1347 | S17 + S18 recency-bound cohort | 3 |
| #1343 | S18 + S21 lazy-on-click | 3 |
| #1342 | S17 cohort tightening (corrected — PipelinedSecFetcher already in place) | 3 |
| #1345 | S22 MERGE → DELETE+UPSERT (verified 11× for 1/7 helpers) | 4 |
| #1276 | Per-row INSERT + savepoint = 1500 rows/sec ceiling | 4 |
| #1274 | `ingest_all_active_filers` serial — 10× under-uses SEC budget | 4 |
| #482 | SEC ingesters block asyncio loop during long runs | 4 |
| #1350 | S22 cohort recency tighten 380d → ~270d (sub-60 stretch lever) | 4 |
| #1351 | S22 cohort universe-overlap trim (sub-60 stretch lever) | 4 |

### Retire / simplify (3)

| # | Title | Owning phase |
|---|---|---|
| #1338 | S19 Form 4 tail-cohort (retire vs widen-window) | 5 |
| #1339 | S20 Form 3 redundancy (retire) | 5 |
| #1348 | Retire S19+S20+S23 from bootstrap | 5 |

### Data integrity / cleanup (8)

| # | Title | Owning phase |
|---|---|---|
| #1349 | `unresolved_13f_cusips` 1.3GB / 6.7M rows (dedup broken) | 6 |
| #1302 | 13F bulk dataset: LEI column dropped silently | 6 |
| #1320 | Split PRE 14A from sec_def14a | 6 |
| #1333 | Migration content-drift runner guard | 6 |
| #1293 | candle_refresh S2 `rows_processed=0` — empty fetch no error | 6 |
| #1265 | Jobs process re-bootstrap master_key when broker_credentials added post-startup | 6 |
| #1325 | /system/* endpoints return 503 not 401 when PG down | 6 |
| #1270 | Exchanges table TRUNCATE wipe leaves it empty | 6 |

### Per-source documentation completion (8)

| # | Title | Owning phase |
|---|---|---|
| **INTERNAL**: §14 Schema field reference + §15 Bulk-vs-iterate decision tree × 21 specs | (internal work item) | 7 |
| #1323 | Retry-posture summary table in ETL sources README | 7 |
| #1326 | Per-source §13 — 304-as-stage-success gotcha | 7 |
| #1319 | Rewrite per-source §12 phantom test names | 7 |
| #1324 | Trim "pagination" / "multi-page" language from 13F + NPORT | 7 |
| #1321 | `_INTENTIONALLY_UNSUPPORTED_FORMS` frozenset doc | 7 |
| #1303 | N-CEN classifier unscheduled (annual cadence drift) — schedule OR retire | 7 |
| #1304 | Form 144 + SC 13E listed in sec-edgar skill but not wired — document OR wire | 7 |

### CI / hygiene / tooling (8)

| # | Title | Owning phase |
|---|---|---|
| #1322 | Manifest-source-has-observation-table smoke + categories-match-writers | 0 |
| #1256 | Strengthen check_ownership_refresh_writer_pattern.sh invariant I | 0 |
| #1327 | Raise DEFAULT_WAIT_FOR_JOBS_SEC to 1800 | 0 |
| #1330 | Per-source spec template version stamp + lint | 7 |
| #1329 | Mirror 14 pre-push lint scripts into ci.yml | 8 |
| #1257 | shellcheck CI gate for scripts/check_*.sh | 8 |
| #1331 | Multixact stub psycopg.Connection ABC parity | 8 |
| #1328 | Rotate var/runbooks/*.jsonl after 30 days | 8 |

### Admin / setup wizard / UX (11)

| # | Title | Owning phase |
|---|---|---|
| #1335 | First-install UX epic (progress bar, ETA, activity feed) | 9 |
| #1344 | OpenFIGI key UX pre-flight nudge | 9 |
| #1280 | Setup wizard: SEC user-agent email validator + OpenFIGI key | 9 |
| #1267 | "Open admin" banner link hide-when-on-/admin | 9 |
| #1264 | Hide "Re-run failed" on first_run_pending | 9 |
| #1229 | Admin processes "+N more" label expandable | 9 |
| #1271 | Bootstrap timeline page does not auto-refresh | 9 |
| #1266 | "Slow-connection fallback" skip should not render as error-colored | 9 |
| #1230 | "Trigger rejected" reason inline-visible, not hover-only | 9 |
| #1231 | Frontend kill-switch toggle | 9 |
| #449 | 10-K Item 1 business-summary: expand SQL schema | 9 (paired with #1343 lazy-on-click) |

**Total: 18 + 3 + 8 + 8 + 8 + 11 = 56 tickets across 6 categories.**

### Explicitly out-of-product-scope (operator sign-off required to close)

These touch ETL/Bootstrap nominally but are vision/research/architectural-rewrite-class — beyond done-state for THIS plan. Per Phase 11 step 3 they require explicit operator quote + sign-off to close without merging.

| # | Title | Rationale |
|---|---|---|
| #195 | Rethink filing ingestion — multi-region providers, storage, observability | Architectural rewrite class; not done-state for v1 |
| #208 | Alternative data layer — insider buys, 13F tracking, macro regime | New feature class; outside ETL completion |
| #198 | Vision: autonomous pocket hedge fund | Vision-level; not deliverable |
| #287 | etl/etoro: daily delistings thin pass | Adjacent ETL but not first-install scope |
| #281 | etl/fx: live conversions via eToro FX | Adjacent ETL but not first-install scope |
| #279 | etl/companies-house: switch UK filings to Streaming API | UK-specific; v1 scope is US/SEC |
| #414 | Fundamentals ingest redesign — lightweight incremental | Tracked separately; current S25 is acceptable |
| #430 | Expand TRACKED_CONCEPTS — DEI facts | New-source-concept work; not first-install gate |
| #94 | Server deployment + operations guide | Ops doc; orthogonal to ETL completion |

## 3. Sub-60 commitment — mandatory stretch scope

**Operator directive**: target was "under an hour". v3.x had this as "stretch / low confidence". Codex v4 flagged it as a punt. **v5 commits to ≤ 60 min wall-clock** by making the stretch levers MANDATORY scope, not optional.

### Run #8 sec_rate lane receipts

| # | Stage | Run #8 (min) | Status |
|---|---|---|---|
| 14 | sec_submissions_files_walk | 41 | success |
| 15 | filings_history_seed | 20 | success |
| 16 | sec_first_install_drain | 85 | success |
| 17 | sec_def14a_bootstrap | 60 | deadline-cut |
| 18 | sec_business_summary_bootstrap | 60 | deadline-cut |
| 19 | sec_insider_transactions_backfill | 3 | success |
| 20 | sec_form3_ingest | 1 | success |
| 21 | sec_8k_events_ingest | 0.5 | success |
| 22 | sec_13f_recent_sweep | 344 | **cancelled at 344 still running** |
| 23, 26, 27 | (pending) | — | cancelled |

S22 actual completion > 344min. Per Run-#7 receipts S22 post-cohort-bound is 75-180min HTTP-sweep + MERGE refresh scaled to `_current` size.

### Target wall-clock per phase boundary (absolute, mandatory)

| Phase boundary | sec_rate target | Confidence | Source |
|---|---|---|---|
| Baseline (Run #8 partial) | 617 min observed lower bound | — | Run #8 |
| **Post-Phase-0 incl. side-quest #1365 (extrapolated)** | **≤ 571 min** | **Medium — R2 + R3 to confirm** | **#1365 R1-observed S16 delta (62.7 → ~17 min); see [phase-0-close.md §6](./phase-0-close.md#6-master-plan-3-baseline-revision)** |
| After Phase 1 | ≤ 500 min | Medium | #1345 jit verified |
| After Phase 2 | ≤ 350 min | High | #1337/#1341 specs |
| After Phase 3 | ≤ 260 min | High | #1347/#1343 |
| After Phase 4 | ≤ 90 min; S22 ≤ 65 min | **Medium → upgraded to High via per-helper gate (§7)** | #1345 + sibling verification |
| After Phase 5 | ≤ 75 min | High | #1348 |
| **After Phase 5, assuming mandatory stretch levers landed in Phase 4** | **≤ 60 min** | Achieved IF #1274/#1276/#1350/#1351 + Phase 0.5 outcome all land | See "stretch lever ticketing" |

### Stretch lever ticketing (Codex v4 BLOCKING fold — no danglers)

Each stretch lever now has a concrete ticket scope, owner, acceptance gate. No "further cohort cuts" hand-waving.

| Lever | New ticket scope | Acceptance | Phase |
|---|---|---|---|
| S22 recency tighter | #1350 — "S22 cohort recency tighten: 380d → ~270d after dev-DB cohort-size verification" | Cohort drops > 30% AND smoke parity < 2% | 4 |
| S22 universe-overlap trim | #1351 — "S22 cohort universe-overlap filter: only institutional_filers whose 13F-HR mentions a CIK ∈ eToro universe issuers" — saves 30-50% cohort | Cohort drops > 30% AND smoke parity < 1% | 4 |
| #1274 SEC budget util | (existing) ingest_all_active_filers serial → 4-way parallel at 7 req/s | S22 HTTP-fetch ≤ 30 min | 4 |
| #1276 batched INSERT | (existing) 1500 rows/sec ceiling → batched COPY/INSERT | Bulk ingesters not throughput-limited | 4 |
| Phase 0.5 dispatcher result | If IDLE_TYPE_B sustained > 0, ticket the specific blocked-lane fix | Dispatcher idle reduced ≥ 20% | 0.5 follow-up |

**If Phases 1-5 land + all 5 stretch levers land, target hits ≤ 60 min.** If any stretch lever fails its acceptance gate, escalate to operator BEFORE Phase 11 closes — no silent slip to 75min.

## 4. Verification protocol (MANDATORY, CI-enforced)

### Pre-merge gate — every perf-claim PR

CI check `perf-claim-lint` (wired into `.github/workflows/ci.yml`, required on main) rejects PR if any artifact missing:

1. `var/perf_baselines/<ticket>-<sha>.txt` — EXPLAIN ANALYZE BUFFERS output
2. `var/perf_baselines/<ticket>-<sha>.json` — 3-trial wall-clock median + system fingerprint
3. `var/perf_baselines/<ticket>-<sha>.manifest.yaml` — fixture row counts (must meet §floors)
4. `## Sibling-shape audit` PR-description section
5. `## Rollback criteria` PR-description section: metric + threshold
6. `## Post-deploy SLO` PR-description section: 1-week metric

### Named row-count floors

Fixture state MUST meet floors for valid perf claim:

| Table | Min rows |
|---|---|
| `ownership_institutions_current` | ≥ 1,000,000 |
| `ownership_institutions_observations` | ≥ 2,000,000 |
| `ownership_insiders_observations` | ≥ 500,000 |
| `ownership_funds_observations` | ≥ 200,000 |
| `financial_facts_raw` | ≥ 10,000,000 |
| `sec_filing_manifest` | ≥ 1,000,000 |
| `filing_events` | ≥ 2,000,000 |

### Benchmark harness

`scripts/perf_bench/run_explain.sh <ticket_id>` — single command, all 3 artifacts produced.

### Implementation: `scripts/perf_bench/lint_pr_artifacts.py` (~150 LOC)

Logic: PR labelled `perf` OR has `## Performance impact` header → verify all artifacts + manifest fixture-row-counts ≥ §floors + EXPLAIN files start with `EXPLAIN (ANALYZE,` + trial JSON has ≥ 3 measurements. Non-zero exit + operator-readable message on failure.

`.github/workflows/ci.yml` adds:
```yaml
perf-claim-lint:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - run: python scripts/perf_bench/lint_pr_artifacts.py
```

Branch protection rule: required check on `main`.

## 5. Process rules (CTO-grade, regulated-desk bar)

Codified in `.claude/skills/engineering/etl-perf-claims.md` (discoverable skill; loaded by Claude before any perf-claim PR per the description-trigger). This section is the single source of truth for the rule list; the skill mirrors it. Drift between this section and the skill must be reconciled in the same PR that introduces it. Cross-linked back from `pre-flight-review.md` + `pre-pr-fresh-agent-review.md`.

1. **Immutable evidence bundle** — perf artifacts versioned in git: EXPLAIN + 3-trial JSON + manifest.yaml all committed in PR
2. **Reproducible benchmark harness** — `scripts/perf_bench/run_explain.sh` single-command; anyone (current author, future maintainer, regulator) reproduces measurement
3. **Data-quality invariants** — per-ticket: state row-count + distinct-count + aggregate-sum invariants that hold pre/post change. Post-deploy CI job verifies.
4. **Rollback decision criteria** — metric + threshold + operator-executed 24h-after-Codex-review SLA. No fake automation.
5. **Change-control ownership** — one named human accountable per phase (commit author, plan signoff, post-deploy verification). Real name in PR + plan doc.
6. **Audit trail of approvals** — PR template enforces:
   - Plan-approval comment with operator signature + date
   - Codex 1 plan-review transcript pasted
   - Codex 1 diff-review transcript pasted
   - Adversarial-reviewer transcript (high-stakes phases)
   - Operator sign-off comment with timestamp
7. **Post-deploy 1-week SLO** — defined metric + alert via ops-monitor; failure → automated alert → operator rollback decision
8. **Regulator-reconstructible** — every Codex 1 review answers: "could a regulator reconstruct what changed + why + when + by whom from git history alone?"
9. **Per-stage Phase 11 evidence package** — at completion, every phase must produce: commit SHAs, DB migration version applied, bootstrap run-id of verification, benchmark artifact filenames, production telemetry query IDs, rollback PR reference (or "not exercised")

## 6. Per-phase agent review protocol (operator-defined, hard-wired)

For EVERY phase, in this order. No phase ships without all 13 steps. No "I'll do that later".

```
1. Draft detailed sub-plan: docs/proposals/etl/phase-<N>-<name>.md
2. Self-review of the sub-plan against §1-§5
3. Multi-agent review:
   - data-engineer skill (always)
   - code-simplifier skill (always)
   - edgartools skill (any SEC-source-touching phase)
   - other skills if relevant: frontend-design / ui-ux-pro-max (Phase 9);
     metrics-analyst (Phase 0 telemetry); committee-review (high-stakes phases)
4. Codex 1 as CTO (mandatory final reviewer)
5. Iterate until plan APPROVED — NIT-only findings acceptable to proceed;
   BLOCKING/IMPORTANT must be resolved
6. **Operator sign-off recorded in plan doc** BEFORE any code (timestamp, signature)
7. Implement changes per the sub-plan
8. Pre-PR agent review (same agent set as step 3) — evidence bundle attached
9. Codex 1 on diff — full transcript pasted into PR
10. Push + CI/CD (pre-push hook + Claude bot + 4 CI checks)
11. Merge after APPROVE on latest commit (per CLAUDE.md merge gate)
12. Post-deploy 1-week SLO monitor
13. Move to next phase
```

**Evidence bundle attached to PR** (step 8 hard-wire):
- Plan doc reference (this file + phase sub-plan)
- All §3-step-7 review transcripts (paste full markdown into PR description)
- Perf artifacts under `var/perf_baselines/<ticket>-<sha>.*`
- Sibling-shape audit output
- Operator sign-off comment (with timestamp)

## 7. Phases

### Phase 0 — instrumentation + dispatcher kill/confirm gate (3-5 days)

**Why**: Every later phase needs measurement infrastructure. #1275 needs validation before downstream optimisation.

**What**:
- **#1225** — bulk SEC ingesters write `rows_processed=NULL` (precursor; covers operator-visible row-count display)
- **#1273** — long-pole stage instrumentation (`target_count` + `processed_count` writes from S22, S16, S14, S15, S17, S18, S25)
- **#1322** — manifest-source-has-observation-table + categories-match-writers smoke
- **#1256** — strengthen check_ownership_refresh_writer_pattern.sh invariant I (full set-equality)
- **#1327** — raise `DEFAULT_WAIT_FOR_JOBS_SEC` to 1800
- **NEW** — `scripts/perf_bench/run_explain.sh` + `lint_pr_artifacts.py` (§4 harness)
- **NEW skill** — `.claude/skills/engineering/etl-perf-claims.md` (§5 process rules)

**Sub-phase 0.5 — dispatcher residual-idle MEASUREMENT**:
- Add per-lane dispatcher logging (~50 LOC): per-lane `future_active`, `ready_stages`, `pending_uncapped`, `caps_blocked_on`
- IDLE_TYPE_A: `future_active=False AND ready_stages=0 AND pending_uncapped > 0` (dependency-natural, no fix)
- IDLE_TYPE_B: `future_active=False AND ready_stages > 0` (actionable bug — dispatcher had work but didn't submit)
- Outcome: RESULT_A (IDLE_TYPE_B sustained → dispatcher capacity bug → ticket follow-up); RESULT_B (IDLE_TYPE_B ≈ 0 → sec_rate lane is the floor, focus on sec_rate cuts only)

**Acceptance**: Run #N dashboard shows real-time per-stage `processed/target/percent + ETA`; perf-bench harness lints non-compliant PRs; dispatcher idle-type telemetry captured and reviewed.

**Sub-agents**: data-engineer + code-simplifier + Codex 1 + adversarial reviewer (challenge measurement methodology).

### Phase 1 — `jit = off` × 10 sites (1 day)

**Why**: Smallest perf win. Exercises §4 protocol on a 1-LOC-per-site change. Verified 1.86×.

**What**: #1346 across all 7 single-instrument + 3 batched ownership refresh helpers.

**Acceptance** (§4 protocol): EXPLAIN ANALYZE shows no JIT section; median 3-trial < 600ms on instrument 1004 (verified 496ms — repeat post-PR).

**Sub-agents**: data-engineer (psycopg3 SAVEPOINT-scope) + Codex 1.

### Phase 2 — bulk-first extraction (5-7 days)

**Why**: Stop re-fetching via HTTP what's in local bulk zips.

**What** (dependency-ordered):
1. **#1337 P1+P2** — S8 cohort widening + S16 fast-path
2. **#1277** — S16 local-zip parse (folds into #1337 P2)
3. **#1341** — S14 master.idx walk
4. **#1340** — S23 NPORT trust bulk + `ManifestSubjectType` enum
5. **#1337 §11** S15 master.idx 730d backfill
6. **#1305** — bulk window depths review (13F/NPORT/insider — widen if first-install gap demonstrated)

**Acceptance** (per ticket via §4):
- S16: 85 → < 5 min
- S14: 41 → < 10 min
- S15: 20 → < 10 min
- S23: < 10 min

**Sub-agents**: data-engineer + edgartools + code-simplifier + Codex 1.

### Phase 3 — cohort tightening + lazy-on-click (3-4 days)

**Why**: S17 + S18 deadline-cut Run #8. Recency-bound + lazy-load eliminates deadline-cut.

**What**:
1. **#1347** — recency-bound `discover_pending_def14a` + sibling at 13 months
2. **#1342** — S17 cohort tightening (subset of #1347)
3. **#1343** — S18 + S21 lazy-on-click: bootstrap populates metadata only; body fetched on user click (PAIRED with Phase 9 UX lazy-load affordance)

**Acceptance**: S17 < 25min; S18 < 5min (metadata only); S21 < 30s.

**Sub-agents**: data-engineer + edgartools + PM (lazy trade-off) + Codex 1.

### Phase 4 — S22 MERGE rewrite + concurrency wins + stretch levers (7-10 days)

**Why**: S22 dominates wall-clock. Biggest single perf phase. Stretch levers MANDATORY scope per §3 sub-60 commitment.

**What**:

1. **#1345** — rewrite all 7 `refresh_*_current` helpers MERGE → DELETE+UPSERT. **Per-helper gate**: each independently verified against §4 row-count floor. 11× verified for 1/7; 6 UNVERIFIED.
2. **#1276** — per-row INSERT + savepoint = 1500 rows/sec ceiling → batched COPY/INSERT in bulk-ingester hot paths
3. **#1274** — `ingest_all_active_filers` serial → bounded parallel HTTP (4-way at 7 req/s, mirror `PipelinedSecFetcher`)
4. **#482** — SEC ingesters block asyncio loop during long runs (related to #1274 concurrency)
5. **#1350** — S22 cohort recency tighten 380d → ~270d (post-measurement)
6. **#1351** — S22 cohort universe-overlap trim (only filers with holdings in eToro universe issuers)

**Per-helper acceptance gate** (every helper independently):
- Median 3-trial < 200ms on heaviest instrument
- EXPLAIN ANALYZE shows no `Seq Scan` over full `_current`
- Data-quality invariant: post-rewrite count(*) == pre-rewrite count(*) ± 1%

**Prevention-log mandatory PR entry**: dev-sized EXPLAIN ≠ prod-sized EXPLAIN.

**Acceptance**: S22 wall-clock 344min → < 65min (combined HTTP + refresh).

**Sub-agents**: data-engineer + code-simplifier + adversarial reviewer (high-stakes refactor) + Codex 1.

### Phase 5 — retire dead code (1-2 days)

**Why**: #1348 — S19/S20/S23 legacy chains do real work for ~1% incremental coverage post-bulk.

**What**:
- **#1348** retire S19/S20/S23 from `_BOOTSTRAP_STAGE_SPECS`
- **#1338** + **#1339** — confirmed retired-not-fixed via Run #8 coverage analysis
- Steady-state schedulers remain
- Zombie code audit: `ncen_classifier.classify_filers_via_ncen` zero callers (folded into Phase 7 #1303 decision)

**Acceptance**: Stage count 27 → 24. Smoke (AAPL/GME/MSFT/JPM/HD) < 2% row delta.

**Sub-agents**: data-engineer + adversarial reviewer + Codex 1.

### Phase 6 — data integrity cleanup (3-4 days)

**What**:
- **#1349** — `unresolved_13f_cusips` 1.3GB cleanup + writer-side dedup fix + VACUUM FULL
- **#1302** — 13F LEI column parser fix
- **#1320** — split PRE 14A from sec_def14a
- **#1333** — migration content-drift runner guard
- **#1293** — candle_refresh `rows_processed=0` empty-fetch
- **#1265** — re-bootstrap master_key after broker_credentials added
- **#1325** — /system/* 503 not 401 when PG down
- **#1270** — exchanges TRUNCATE wipe (seed re-population)

**Acceptance per ticket** + §4 perf protocol where applicable.

**Sub-agents**: data-engineer + Codex 1.

### Phase 7 — per-source documentation completion (5-7 days)

**Why**: Operator's repeated request for "field types, constraints, primary keys, freshness, bulk vs iterate" — currently `docs/etl/sources/*.md` have 14 sections covering operational behaviour but missing explicit DB-contract reference and explicit bulk-vs-iterate decision tree.

**What**: For each of the 21 per-source specs, add:

**§14 — Schema field reference (full DB contract — Codex v4 IMPORTANT fold)**:

Per-table inventory covering EVERY column the source writes to:
- Column name
- PostgreSQL type (with length / precision)
- NULL / NOT NULL
- PRIMARY KEY indicator
- FOREIGN KEY targets
- UNIQUE constraints
- CHECK constraints (with predicate)
- Indexes that enforce access patterns (with column order + included)
- Default values
- Generated columns (with expression)
- Enum / domain values (with full set)
- Partition key (if partitioned)
- Retention / cap policy (in days/quarters + enforcement helper)
- Watermark / freshness source (which table + column tracks staleness)
- Manifest coupling (which `sec_filing_manifest.source` values gate this column)
- Endpoint / consumer usage (which `/api/*` endpoint reads it)
- Plain-English meaning
- Source-of-truth field on SEC/source side
- Example value

**§15 — Bulk vs iterate decision tree**:

Per-source matrix covering:
- When to use bulk (first-install / large backfill / quarterly refresh / disaster recovery)
- When to use iterate (steady-state delta / per-CIK refresh / cancel-resume)
- Code references: which job + function + lane handles each path
- Cohort scoping: how cohort size is bounded (with code reference)
- **Freshness semantics (separate concern from bulk-vs-iterate)**:
  - Watermark table + column
  - Stale threshold (in time units)
  - 304 (Not Modified) handling
  - Retry horizon
  - Parser-version rewash trigger
  - How to detect "bulk cache fresh but observations stale" (cross-staleness)

**Plus**:
- **#1326** — per-source §13 gotcha: 304-as-stage-success
- **#1319** — rewrite §12 phantom test names (7 specs)
- **#1324** — trim "pagination" / "multi-page" language (13F + NPORT specs)
- **#1321** — `_INTENTIONALLY_UNSUPPORTED_FORMS` frozenset doc
- **#1303** — N-CEN classifier decision: schedule annually OR retire (folds with Phase 5)
- **#1304** — Form 144 + SC 13E: wire OR document explicitly unsupported
- **#1330** — per-source spec template version stamp + lint check (`scripts/check_etl_source_docs.sh` extension)
- **#1323** — `docs/etl/sources/README.md` cross-source retry-posture summary table

**Acceptance** (lint-verified):
- All 21 specs have 15 sections
- §14 covers every column in every table the source writes to (verify against `information_schema.columns`)
- §15 unambiguous bulk-vs-iterate rule + freshness contract
- All code references `file:line` resolve cleanly

**Sub-agents per source**: data-engineer + edgartools + Codex 1.

### Phase 8 — CI gates + scheduled-job hygiene (3-4 days)

**Why**: Codify the verification protocol in CI; install permanent gates so dev-fixture-passes-prod-fails can't recur.

**What**:
- **#1329** — mirror 14 pre-push lint scripts into ci.yml
- **#1257** — shellcheck CI gate for scripts/check_*.sh
- **#1331** — multixact stub psycopg.Connection ABC parity
- **#1328** — rotate `var/runbooks/*.jsonl` after 30 days
- **NEW** — `perf-claim-lint` workflow (§4) installed + required check on main
- **NEW** — `source-doc-lint` workflow (Phase 7 §14 + §15 enforcement) installed + required check
- Partition tail strategy alignment (`ownership_*_observations` to 2040 per sql/177; align siblings)
- `bootstrap_stages.expected_units` / `units_done` deprecation (sql/129 unused; sql/140 added the replacement)

**Acceptance**: All 4 CI checks (perf-claim-lint, source-doc-lint, shellcheck, mirrored pre-push) installed AND passing on main.

**Sub-agents**: data-engineer + code-simplifier + Codex 1.

### Phase 9 — UI improvements (10-15 days)

**Why**: First-install user is the riskiest UX moment. After perf target hit, UX work makes the wait FEEL good.

**What** (full ticket coverage):
- **#1335 first-install UX epic** — 7-phase sub-plan at `docs/proposals/admin/first-install-bootstrap-ux.md` (P1 telemetry already in Phase 0; P2-P7 here)
- **#1271** — bootstrap timeline auto-refresh
- **#1344** — OpenFIGI key UX pre-flight nudge
- **#1280** — setup wizard: SEC user-agent + OpenFIGI key surfacing
- **#1267** — "Open admin" banner link hide when on /admin
- **#1264** — hide "Re-run failed" on first_run_pending
- **#1229** — admin processes "+N more" expandable
- **#1230** — "Trigger rejected" reason inline-visible
- **#1266** — slow-connection fallback skip not error-colored
- **#1231** — frontend kill-switch toggle
- **#449** — 10-K Item 1 schema expand (paired with #1343 lazy-on-click backend in Phase 3)

**Measurement definition** (Codex v4 IMPORTANT fold):
- **Page-abandon rate** denominator = "first-install operator page-loads matching `/admin/process/bootstrap`"; event = "tab close OR navigate-away before bootstrap status transitions to `complete`"; sample window = first 30 operator first-install sessions post-deploy; privacy = local telemetry only, no external transmission, operator opt-in via setup-wizard checkbox
- **ETA accuracy** = `abs(predicted_total - actual_total) / actual_total`; sample = first 10 first-install runs post-deploy

**Acceptance**:
- Page-abandon rate < 5% on opted-in first-install sessions
- ETA accuracy ± 20%

**Sub-agents**: frontend-design + ui-ux-pro-max + PM/CX + data-engineer (telemetry contract) + Codex 1.

### Phase 10 — operator runbook + handoff doc (1-2 days)

**Why**: "Best in class" implies someone else can adopt + operate. Runbook is the artifact.

**What**:
- `docs/operator/runbooks/first-install.md` — exact paste-runnable steps for fresh install through completion
- `docs/operator/runbooks/troubleshooting-bootstrap.md` — per-stage failure mode + recovery
- `docs/operator/runbooks/perf-investigation.md` — how to use `scripts/perf_bench/` + interpret artifacts
- `docs/data-sources/sec-bulk-archives.md` already exists (7700w) — link as primary bulk-source reference
- `docs/etl/sources/README.md` already exists — link as primary per-source reference

**Sub-agents**: data-engineer + Codex 1.

### Phase 11 — completion verification + handoff (2-3 days)

**Why**: Door-close gate. Plan declares DONE.

**What**:
1. **End-to-end test**: clean dev DB → fresh `--apply` → measure full bootstrap wall-clock → confirm **≤ 60 min** (sub-60 commitment per §3)
2. **Per-source documentation audit**: lint script confirms all 21 specs have 15 sections; §14 covers every column; §15 freshness contract complete
3. **Open-ticket sweep**: every ticket in §2 (55 GitHub tickets + 1 internal work item) is `closed` (merged) — explicit out-of-product-scope tickets get operator-quoted sign-off comment in plan doc
4. **Smoke panel** (AAPL/GME/MSFT/JPM/HD): every operator-visible metric renders correctly
5. **Codex sweep**: full plan + every phase sub-plan re-reviewed by Codex 1 (CTO lens) — no remaining BLOCKING/IMPORTANT
6. **Operator demo**: walk through first-install on clean install, time it, confirm UX
7. **Memory write**: `MEMORY.md` index entry + closing memo recording done-state
8. **Phase-11 evidence package per phase** (Codex v4 IMPORTANT fold — regulator-reconstructible):
   - Commit SHAs per phase
   - DB migration version applied
   - Bootstrap run-id of verification
   - Benchmark artifact filenames
   - Production telemetry query IDs
   - Rollback PR reference (if exercised) OR "not exercised — SLO held"
9. **CI gate persistence check**: confirm `perf-claim-lint`, `source-doc-lint`, `shellcheck`, mirrored-pre-push all installed AND last 10 main-branch runs all green
10. **Rollback readiness check** (Codex v4 IMPORTANT fold): every perf phase has rollback PR pre-written (draft state in branch) OR rollback-decision doc recording why rollback impossible (e.g. data migration not reversible)
11. **Stale-ticket pass**: close #1275 with "fixed by PR-2 #1233 FIRST_COMPLETED"; close any other now-stale

## 8. Sequencing

```
Phase 0 + 0.5 (instrumentation + dispatcher gate)  — 3-5d  (precondition)
   ↓
Phase 1 (jit=off; exercise protocol)               — 1d
   ↓
Phase 4 (S22 rewrite + stretch levers)             — 7-10d  ┐
                                                              ├─ parallelisable
Phase 2 (bulk-first)                                — 5-7d   ┘
   ↓
Phase 3 (cohort + lazy)                             — 3-4d
   ↓
Phase 5 (retire)                                    — 1-2d
   ↓
Phase 6 (data integrity)                            — 3-4d   ┐
                                                              ├─ parallelisable
Phase 7 (per-source docs)                           — 5-7d   ┘
   ↓
Phase 8 (CI gates + hygiene)                        — 3-4d
   ↓
Phase 9 (UI)                                        — 10-15d
   ↓
Phase 10 (runbook + handoff)                        — 1-2d
   ↓
Phase 11 (completion verification)                  — 2-3d
```

**Total wall-clock**: 41-66 days serial; 25-35 days with Phase 4+2 parallelisation + Phase 6+7 parallelisation.

## 9. Done-state definition

Plan declares DONE when ALL of:

1. ✅ Clean-install bootstrap wall-clock **≤ 60 min** (sub-60 committed, no 75-min fallback)
2. ✅ All 21 per-source specs have 15 sections; §14 + §15 lint-verified
3. ✅ All 55 GitHub tickets + 1 internal work item in §2 closed (merged) OR operator-quoted out-of-product-scope sign-off in plan doc
4. ✅ Smoke cohort (AAPL/GME/MSFT/JPM/HD) renders all operator-visible metrics
5. ✅ First-install page-abandon rate < 5% (measurement per §Phase 9)
6. ✅ ETA accuracy ± 20% (measurement per §Phase 9)
7. ✅ CI gates permanently installed AND passing on main (`perf-claim-lint`, `source-doc-lint`, `shellcheck`, mirrored-pre-push)
8. ✅ Rollback readiness per perf phase (draft revert PR OR irreversibility doc)
9. ✅ Phase-11 evidence package per phase (commit SHAs + migration version + run-id + benchmark filenames + telemetry query IDs + rollback ref)
10. ✅ Codex sweep clean — no BLOCKING/IMPORTANT in plan or sub-plans
11. ✅ Operator demo + sign-off recorded
12. ✅ Memory index entry recording done-state

**No follow-ups.** Anything that surfaces post-completion = new product epic, not "Phase 11.5 punt".

## 10. References

- ETL simplification audit: `.scratch/etl_simplification_audit.md`
- S22 MERGE perf reviews: `.scratch/s22_merge_perf_code_review.md`, `.scratch/s22_merge_perf_data_engineer.md`
- Bulk archive catalogue: `docs/data-sources/sec-bulk-archives.md` (7700w, 16 archives)
- Bulk-first epic spec: `docs/proposals/etl/bulk-first-bootstrap.md`
- First-install UX spec: `docs/proposals/admin/first-install-bootstrap-ux.md`
- Verified MERGE perf (Run #8 dev DB): #1345 comment trail
- Per-source specs: `docs/etl/sources/*.md` (21 files, 80-150 lines each pre-Phase-7)
- Plan v1-v4 Codex 1 review transcripts: `/private/tmp/claude-501/-Users-lukebradford-Dev-eBull/.../tasks/b07042tgb.output` + subsequent re-pass logs
