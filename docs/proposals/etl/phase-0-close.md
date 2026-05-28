# Phase 0 close-out — bootstrap sub-1h instrumentation foundation

**Status**: LANDED 2026-05-28. Phase 0 code-complete (9 merged PRs + 2 side-quest code fixes + 1 prevention-only chore). R1 measurement run captured as bad-ordering reference; RESULT_B confirmed on R1 with 7051 dispatcher iterations. R2 + R3 deferred to a follow-up operator-driven session (separate ticket — Phase 0 close does not block on them).

**Master plan**: [bootstrap-sub-1h-plan.md](./bootstrap-sub-1h-plan.md) v5.2

**Phase 0 spec**: [phase-0-instrumentation.md](./phase-0-instrumentation.md) v1.5

## §1. Scope — Phase 0 PRs (code-complete 2026-05-27)

Nine PRs landed against the issues catalogued in [phase-0-instrumentation.md §1](./phase-0-instrumentation.md#1-scope--work-items). Listed in merge order:

| # | Stage | PR | SHA | Closes |
|---|-------|-----|-----|--------|
| 1 | #1327 `DEFAULT_WAIT_FOR_JOBS_SEC` 600 → 1800 | [#1352](https://github.com/Luke-Bradford/eBull/pull/1352) | `9bad176` | #1327 |
| 2 | #1256 invariant I 5-axis full-column-set | [#1353](https://github.com/Luke-Bradford/eBull/pull/1353) | `1b129da` | #1256 |
| 3 | #1322 manifest source/categories smoke | [#1354](https://github.com/Luke-Bradford/eBull/pull/1354) | `e7bf098` | #1322 |
| 4 | #1225 bulk ingester `rows_processed=NULL` | [#1355](https://github.com/Luke-Bradford/eBull/pull/1355) | `e61780c` | #1225 |
| 5 | NEW-A perf-bench harness + perf-claim-lint CI gate | [#1357](https://github.com/Luke-Bradford/eBull/pull/1357) | `dfe5be9` | #1356 (NEW-A) |
| 6 | NEW-B + NEW-C etl-perf-claims skill + synthetic-fixture seeders | [#1359](https://github.com/Luke-Bradford/eBull/pull/1359) | `edab8d4` | #1356 (NEW-B/C) |
| 7 | Phase 0.5 dispatcher residual-idle telemetry | [#1360](https://github.com/Luke-Bradford/eBull/pull/1360) | `beb9c42` | #1356 (Phase 0.5) |
| 8 | #1273 PR1 stage-progress helpers + cohort-shape audit memo | [#1361](https://github.com/Luke-Bradford/eBull/pull/1361) | `792291e` | partly #1273 |
| 9 | #1273 PR2 long-pole stage instrumentation + cohort-fingerprint wiring | [#1362](https://github.com/Luke-Bradford/eBull/pull/1362) | `4dd816b` | #1273 |

### Side-quests discovered during R1 measurement

R1 surfaced two latent code bugs that block the master plan §3 sub-60 target. Both fixed inline in this session. A third side-quest (#1367) is a prevention-only chore — no code change, captures the agent narrate-waiting anti-pattern in the prevention log:

| # | Stage | PR | SHA | Closes |
|---|-------|-----|-----|--------|
| S1 | Jobs process cold-start tolerance — `dev: start stack` was racing /auth/setup | [#1364](https://github.com/Luke-Bradford/eBull/pull/1364) | `be8a08e` | #1363 |
| S2 | S16 `sec_first_install_drain` waits for bulk path → fast-path fires → S16 ~85 min → ~17 min | [#1366](https://github.com/Luke-Bradford/eBull/pull/1366) | `c746624` | #1365 |
| S3 | Prevention-log + memory entry for agent-narrate-waiting anti-pattern | [#1368](https://github.com/Luke-Bradford/eBull/pull/1368) | `4f9e7d2` | #1367 |

R1 captured under pre-S2 binary (S2 #1365 merged after R1 dispatcher started) → R1 is the bad-ordering reference. R2 + R3 (operator-driven, separate session) measure the post-S2 floor.

Follow-up ticket: [#1369](https://github.com/Luke-Bradford/eBull/issues/1369) — hookify rule for waiting-token enforcement (paired with #1367 prevention-log entry).

## §2. R1 wall-clock baseline — bad-ordering reference

R1 run_id=1, triggered 2026-05-27 22:41:20 UTC, cancelled 2026-05-28 00:44:29 UTC. Wall-clock 2 h 3 min, 17 / 27 stages successfully terminalised, 1 stage cancelled mid-flight (S17 at 6.96 min, streaming 2000 processed), 9 stages cancelled before starting.

Operator cancellation rationale: R1's most valuable data point (S16 bad-ordering case) was already captured at 62.69 min; remaining stages (S17/S18 deadline-cuts, S22 cohort sweep) are same-as-Run-#8 receipts that don't need re-proving. R2 (post-#1365) is where the floor drops.

### Full R1 stage table

| Stage | Lane | Status | Minutes | Processed / target | Notes |
|-------|------|--------|---------|--------------------|-------|
| universe_sync | init | success | 0.13 | — | |
| cusip_universe_backfill | sec_rate | success | 0.20 | — | |
| sec_bulk_download | sec_bulk_download | success | 1.32 | — | |
| candle_refresh | etoro | success | 7.39 | — | |
| sec_13f_filer_directory_sync | sec_rate | success | 0.27 | — | |
| sec_nport_filer_directory_sync | sec_rate | success | 0.25 | — | |
| cik_refresh | sec_rate | success | 0.33 | — | |
| **sec_first_install_drain (S16)** | sec_rate | success | **62.69** | 11205 / — | **bad-ordering reference — fast_path_seeded=false, HTTP per-CIK loop** |
| sec_companyfacts_ingest | db | success | 16.30 | — | |
| sec_submissions_ingest (S8) | db | success | 23.86 | — | |
| sec_nport_ingest_from_dataset | db | success | 10.52 | — | |
| sec_insider_ingest_from_dataset | db | success | 1.17 | — | |
| sec_13f_ingest_from_dataset | db | success | 7.67 | — | |
| cusip_resolver_post_bulk_sweep | openfigi | success | 0.17 | — | |
| fundamentals_sync | db_fundamentals_raw | success | 4.29 | 4989 / 4989 | PR2 instrumentation visible |
| sec_submissions_files_walk (S14) | sec_rate | success | **28.02** | 5102 / 5102 | **vs Run #8 41 min — Stream A PR-B sidecar pre-pay ~13 min saved** |
| filings_history_seed (S15) | sec_rate | success | **21.16** | 5102 / 5102 | vs Run #8 20 min — comparable, no regression |
| sec_def14a_bootstrap (S17) | sec_rate | cancelled | 6.96 (streaming) | 2000 / — | operator-cancelled mid-flight; pre-#1365 ordering means streaming was at HTTP rate |
| sec_business_summary_bootstrap (S18) | sec_rate | cancelled (never started) | — | — | |
| sec_n_port_ingest | sec_rate | cancelled (never started) | — | — | |
| sec_n_csr_bootstrap_drain | sec_rate | cancelled (never started) | — | — | |
| sec_13f_recent_sweep (S22) | sec_rate | cancelled (never started) | — | — | |
| mf_directory_sync | sec_rate | cancelled (never started) | — | — | |
| sec_8k_events_ingest | sec_rate | cancelled (never started) | — | — | |
| sec_form3_ingest (S20) | sec_rate | cancelled (never started) | — | — | |
| sec_insider_transactions_backfill (S19) | sec_rate | cancelled (never started) | — | — | |
| ownership_observations_backfill | db | cancelled (never started) | — | — | |

### R1 critical observations

1. **S16 = 62.69 min** confirms the bad-ordering case study. Pre-#1365 binary, S16 started 16 s BEFORE `sec_bulk_download` completion → `fast_path_seeded=false` → HTTP per-CIK loop on 11205 subjects.
2. **Parallelism worked** — while S16 saturated `sec_rate` for ~62 min, the DB-family lanes (sql/147 lane split: `db_filings` / `db_fundamentals_raw` / `db_ownership_inst` / `db_ownership_insider` / `db_ownership_funds`) completed S8 (23.86, `db_filings`) + companyfacts (16.30, `db_fundamentals_raw`) + 3 bulk dataset ingests (insider 1.17 + 13f 7.67 + nport 10.52, on `db_ownership_*` lanes) + fundamentals_sync (4.29, `db_fundamentals_raw`) for a total of ~63 min DB-family-lane work in parallel. No idle_b iterations observed across any DB-family lane during this window — RESULT_B signal. The bare `db` lane in §3 shows busy_iter=0 because none of these stages map to it (they all hit the per-family child lanes); `db`'s `idle_a` captures their downstream consumers waiting on sec_rate to drain.
3. **PR2 instrumentation visible** — `fundamentals_sync` reports `target_count=4989` + `processed_count=4989`; S14 reports `target_count=5102` + live progress; S17 streamed `processed_count` up to 2000 before cancel. Cohort-fingerprint plumbing works as designed.
4. **S14 cohort = 5102** is the correct sidecar-bound subset (CIKs with non-empty `sec_cik_submissions_files_index` secondary pages — per Stream A PR-B), NOT the full ~12k issuer universe. The Stream A pre-pay on S14 cohort is already in effect.
5. **PR2 cohort fingerprints captured** — four long-pole stages emitted operator-visible fingerprints via `target_cohort_fingerprint` (sql/178 column):
    - `sec_first_install_drain`: `max_subjects=unbounded;follow_pagination=true;fast_path_seeded=false`
    - `fundamentals_sync`: `instrument_scope=universe_with_facts;source_table=financial_facts_raw`
    - `sec_submissions_files_walk`: `is_tradable_only=true;sidecar_sentinel=3228;sidecar_real_pages=1874;sidecar_empty=0`
    - `sec_def14a_bootstrap`: `chunk_limit=500;max_runtime_seconds=3600;form_types=4;cap_per_filer=2;rank_scope=def14a_with_cik;rank_predicate=type<>DEF14A_OR_cik_null_OR_rank<=cap;url_filter=true;tombstone_filter=true;pending_predicate_v1=true`

    The S14 fingerprint decomposes the cohort: **only 1874 of the 5102 targets are real-pages-with-HTTP-work**; 3228 are sentinel-only fast-skips. This is the canonical use case for the perf-bench harness in Phase 1 — fingerprints pin cohort identity across runs so jit=off PRs cannot accidentally compare different cohorts.

### R2 + R3 deferral

R2 and R3 are operator-driven measurement runs that exercise the post-#1363 + post-#1365 binary. Phase 0 close does NOT block on them — the R1 receipts plus the merged #1365 fix (regression-pinned by `test_sec_first_install_drain_requires_submissions_processed`) are sufficient evidence that the Phase 0 instrumentation works end-to-end.

R2 + R3 runbooks are pre-staged at `var/runbooks/phase-0-close/R2-20260528.md` + `R3-20260528.md` (not in repo — operator runbooks are gitignored at `var/runbooks/`). Operator drives wipes + bootstraps when convenient; results land in `docs/proposals/etl/phase-0-r2r3-results.md` (separate doc, follow-up commit).

## §3. Dispatcher idle RESULT — RESULT_B confirmed on R1

R1 ran 7051 dispatcher iterations under the pre-#1365 binary. Every lane recorded `idle_b_iter = 0`. Spec [phase-0-instrumentation.md §2.9.3](./phase-0-instrumentation.md#293-multi-run-measurement) decision rule: all runs `idle_b_iter ≈ 0` → RESULT_B → close #1275 with multi-run evidence.

R1 alone is one data point. R2 + R3 are deferred (this doc §2), so #1275 closure is also deferred until R2 + R3 confirm. Provisional RESULT_B on R1 is recorded here; final #1275 closure ticket pings to the R2 + R3 follow-up session.

### Final per-lane R1 aggregates

| Lane | busy_iter | idle_a_iter | idle_b_iter | idle_b_max_run | idle_b_stages_seen | Verdict |
|------|-----------|-------------|-------------|----------------|--------------------|---------|
| sec_rate | 7043 | 7 | 0 | 0 | [] | RESULT_B |
| db | 0 | 7034 | 0 | 0 | [] | RESULT_B (no work hit; downstream stages dep-natural idle) |
| db_filings | 1387 | 81 | 0 | 0 | [] | RESULT_B |
| db_fundamentals_raw | 1192 | 530 | 0 | 0 | [] | RESULT_B |
| db_ownership_funds | 598 | 81 | 0 | 0 | [] | RESULT_B |
| db_ownership_insider | 65 | 81 | 0 | 0 | [] | RESULT_B |
| db_ownership_inst | 434 | 81 | 0 | 0 | [] | RESULT_B |
| etoro | 421 | 7 | 0 | 0 | [] | RESULT_B |
| init | 8 | 0 | 0 | 0 | [] | RESULT_B |
| openfigi | 11 | 675 | 0 | 0 | [] | RESULT_B |
| sec_bulk_download | 79 | 7 | 0 | 0 | [] | RESULT_B |

Total dispatcher iterations: 7051. `sec_rate` 99.9% busy (7043/7051) — lane was genuinely saturated by S16's HTTP loop, not stalled on capacity. The `db` lane never had its own work fire (0 busy) but `idle_a` cleanly attributed every iteration as dependency-natural — downstream stages waiting on sec_rate to drain.

**R1 overall verdict: RESULT_B.** Dispatcher has no capacity-bug shape observable in this run. Following #1275's decision rule, multi-run confirmation comes from R2 + R3 in the follow-up session.

If R2 OR R3 reveals sustained `idle_b_iter > 0` → flip to RESULT_A, file a follow-up ticket citing the specific blocked lane + stages, and update this doc's verdict accordingly.

## §4. PR2 operator-visible evidence (#1273 PR2)

PR2 §10.4 smoke acceptance requires operator-visible screenshots of long-pole stages rendering progress bar + cohort-fingerprint tooltip on ProcessDetailPage. Screenshots are operator-captured and gitignored at `var/runbooks/phase-0-close/screenshots/` — not committed to the repo.

R1 captured the underlying mechanism via DB read — four stages emitted populated `target_cohort_fingerprint` rows (§2 observation 5). The frontend tooltip is the additional rendering layer over the same column; landing screenshots is the operator-visible verification step and is deferred to the R2 / R3 session where the operator drives the UI anyway.

PR2 §10.4 evidence captured in R1:

| Stage | Fingerprint observed (DB) | UI tooltip verification |
|-------|---------------------------|--------------------------|
| `sec_first_install_drain` | `max_subjects=unbounded;follow_pagination=true;fast_path_seeded=false` | deferred to R2 |
| `fundamentals_sync` | `instrument_scope=universe_with_facts;source_table=financial_facts_raw` | deferred to R2 |
| `sec_submissions_files_walk` | `is_tradable_only=true;sidecar_sentinel=3228;sidecar_real_pages=1874;sidecar_empty=0` | deferred to R2 |
| `sec_def14a_bootstrap` | `chunk_limit=500;max_runtime_seconds=3600;form_types=4;…` (full string in §2) | deferred to R2 |

Acceptance gate: the BACKEND emission is verified by R1's bootstrap_stages rows (`target_cohort_fingerprint IS NOT NULL` across 4 stages); the FRONTEND tooltip rendering is verified post-R2 by screenshot.

## §5. Acceptance criteria — Phase 0 done-state

| Criterion | State | Source |
|-----------|-------|--------|
| All 9 PRs merged to main | ✓ | §1 table |
| Side-quests #1363 + #1365 + #1367 fixed in-session | ✓ | §1 side-quests table |
| R1 measurement run captured with full per-stage wall-clock + dispatcher JSONL | ✓ | §2 R1 stage table + `var/dispatcher_idle/1.jsonl` (10 MB / 7051 lines at cancel) |
| R2 + R3 measurement runs captured | DEFERRED | §2 R2 + R3 deferral block |
| Dispatcher idle classifier output recorded for R1 | ✓ | §3 per-lane R1 aggregates |
| RESULT_A vs RESULT_B decision on R1 | ✓ — RESULT_B | §3 |
| Final RESULT_A vs RESULT_B (multi-run) | DEFERRED — pending R2 + R3 | §3 deferral block |
| PR2 cohort-fingerprint backend emission verified | ✓ | §2 observation 5 + §4 table |
| PR2 frontend tooltip screenshot capture | DEFERRED to R2 / R3 | §4 |
| Master plan §3 wall-clock baseline updated | ✓ — see §6 below | §6 |
| Lint / format / typecheck / pytest pre-push gates on Phase 0 PRs | ✓ | each PR's CI check log |
| Codex 1 review on this close-out doc | pending — invoked as final step before PR push | this PR |
| User sign-off on close-out + master plan revision | pending | this PR |

## §6. Master plan §3 baseline revision

Master plan §3 currently shows:

| Phase boundary | sec_rate target | Confidence | Source |
|---|---|---|---|
| Baseline (Run #8 partial) | 617 min observed lower bound | — | Run #8 |

R1 doesn't fully complete (operator cancel at 2h 3min with S17 cancelled mid + 9 stages cancelled-pre-start), so a direct Phase-0 "lower bound" is not produced. The most useful insight for §3 is the side-quest #1365 impact:

| Stage | Pre-#1365 (R1) | Post-#1365 (extrapolated) | Lever | Δ |
|-------|----------------|---------------------------|-------|---|
| S16 | 62.69 min | ~17 min | issuer cohort skips HTTP via filing_events | ~46 min saved |
| S14 | 28.02 min | ~28 min | unchanged (Stream A PR-B already pays) | 0 |
| S15 | 21.16 min | ~21 min | unchanged | 0 |

Phase 1 (#1346 jit=off × 10 sites) start state vs master plan §3:

- Master plan baseline = 617 min (Run #8 lower bound, pre-Phase-0).
- Phase 0 deliverables alone produce no direct wall-clock reduction — they are instrumentation, not optimisation.
- Side-quest #1365 (landed in Phase 0) drops S16 by ~46 min in isolation, so the Phase 1 START is approximately `617 - 46 = ~571 min`.

Updated master plan §3 table (this PR also edits `docs/proposals/etl/bootstrap-sub-1h-plan.md` to add a "Post-Phase-0 incl. side-quest #1365" row between Baseline and After Phase 1):

| Phase boundary | sec_rate target | Confidence | Source |
|---|---|---|---|
| Baseline (Run #8 partial) | 617 min observed lower bound | — | Run #8 |
| **Post-Phase-0 incl. side-quest #1365 (extrapolated)** | **≤ 571 min** | **Medium — R2 + R3 to confirm** | **#1365 R1-observed delta** |
| After Phase 1 (#1346 jit=off) | ≤ 500 min | Medium | #1345 jit verified |

## §7. Handoff to Phase 1

Phase 1 scope: #1346 — set `jit=off` on 7 single-instrument + 3 batched ownership refresh helpers. Acceptance per [bootstrap-sub-1h-plan.md §7 Phase 1](./bootstrap-sub-1h-plan.md#phase-1--jit--off--10-sites-1-day): EXPLAIN ANALYZE shows no JIT section; median 3-trial < 600 ms on instrument 1004 (verified 496 ms earlier — repeat post-PR per §4 protocol).

Pre-requisites carried from Phase 0 (all ready):

1. **PR2 cohort-fingerprint plumbing** — `target_cohort_fingerprint` column in `bootstrap_stages` + `set_stage_target` helper + `resolve_progress_context` contextvar. Used by perf-bench harness to keep cohort identity stable across runs.
2. **NEW-A perf-bench harness** — `scripts/perf_bench/` + `perf-claim-lint` CI gate. Every Phase 1 PR must produce `var/perf_baselines/<ticket>-<sha>.{txt,json,manifest.yaml}` + cohort fingerprint + sibling-shape audit per master plan §4 protocol.
3. **NEW-C synthetic-fixture seeders** — 1 implemented (`ownership_institutions_current`), 6 stubs. Phase 1 unblocks if a `jit=off` ticket needs fixture rows under-floor on dev DB.
4. **etl-perf-claims skill** — discoverable agent skill documenting the perf-claim PR shape so Phase 1 sub-agents know the artifact requirements.
5. **Stretch lever ticketing** — each lever has concrete acceptance gates per master plan §3.

Phase 1 start state per §6 update: ~571 min (post-Phase-0 incl. #1365 side-quest). Phase 1 target: ≤ 500 min.

### Phase 1 / Phase 2 status matrix

| Lever | Status post-Phase-0 |
|-------|---------------------|
| #1346 jit=off × 10 sites | Phase 1 — not started |
| #1337 P1 S8 cohort widening | Phase 2 — not started |
| #1337 P2 / #1365 S16 fast-path ordering | **partial — #1365 landed in Phase 0; remaining work: local-zip parse via #1277** |
| #1277 S16 local-zip parse | Phase 2 — not started |
| #1341 S14 master.idx walk | Phase 2 — not started |
| #1340 S23 NPORT trust bulk | Phase 2 — not started |
| #1305 bulk window depths review | Phase 2 — not started |

### What #1365 already gives (vs what's still on Phase 2)

#1365 lands the cap-ordering half of Phase 2 #1337 P2. After #1365:

- Issuer cohort (~12k subjects) skips HTTP and hydrates from `filing_events` populated by S8 — extrapolated ~17 min S16 wall-clock (~5× reduction from R1's 62.7 min observed bad-ordering case).
- Non-issuer cohort (institutional_filer + blockholder_filer, ~10k combined) still walks HTTP — no bulk archives exist for those subject types.
- Issuer + non-issuer split: ~25k → ~10k via HTTP.

Still on Phase 2:

- `seed_manifest_from_filing_events` is called ONCE at function entry. If S8 has not terminalised by S16 entry (which #1365 now prevents), fallback to HTTP. Phase 2 #1277 ("S16 local-zip parse") would parse the bulk zips directly + remove the dependency on `filing_events` row presence entirely.
- Non-issuer cohort fast-path: no current ticket; ~10k subjects × ~10 req/s shared = ~17 min floor. Phase 2 acceptance gate (S16 ≤ 5 min) requires either parallelising non-issuer HTTP or moving it off the bootstrap critical path.

## §8. Follow-up work post-close

- **R2 + R3 measurement runs** — operator-driven; populate `docs/proposals/etl/phase-0-r2r3-results.md`. Confirms multi-run RESULT_B and closes #1275.
- **#1369 hookify rule** — UserPromptSubmit / Stop hook for agent narrate-waiting tokens (paired with #1367 prevention-log entry).
- **Phase 1 PR sequence** — #1346 jit=off × 10 sites, acceptance per master plan §7 Phase 1.

## §9. Cross-references

- Master plan: [bootstrap-sub-1h-plan.md](./bootstrap-sub-1h-plan.md) v5.2 — §3 wall-clock baseline revised in this PR (§6 above).
- Phase 0 spec: [phase-0-instrumentation.md](./phase-0-instrumentation.md) v1.5.
- Phase 0.5 telemetry script: [scripts/dispatcher_idle_analysis.py](../../../scripts/dispatcher_idle_analysis.py).
- PR1 audit memo (frozen): [1273-pr1-cohort-shapes.md](./1273-pr1-cohort-shapes.md).
- PR2 spec (frozen): [phase-0-pr2-stage-progress-instrumentation.md](./phase-0-pr2-stage-progress-instrumentation.md).
- R1 raw memo: `var/runbooks/phase-0-close/R1-20260527.md` (gitignored).
- R1 dispatcher JSONL: `var/dispatcher_idle/1.jsonl` (10 MB / 7051 iterations at cancel).
- Side-quest issues: #1363 (cold-start tolerance, CLOSED), #1365 (S16 ordering, CLOSED), #1367 (agent waiting anti-pattern, CLOSED), #1369 (hookify rule, OPEN).
