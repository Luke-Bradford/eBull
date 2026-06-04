# ETL cleanup + closeout plan — clear prior-work debt, then pivot to product board

**Context (2026-06-04).** Post-bootstrap hardening backlog cleared (#1431/#1432/#1433/#1434/#1455 merged; #1428/#1429/#1430/#1435/#1424 closed). Data baseline validated SOUND and confirmed **not** under-ingested (#1438: us-equity-with-CIK = 98% facts; the rest is universe composition). Board = 149 open: ~56 ETL/bootstrap, 29 product `feat()`, 17 tech-debt/test, 12 FE/UX, 4 docs, 31 other.

**Goal.** Drain the ETL-era *correctness debt* + bugs so the bootstrap/ETL is "put down once and for all", then return to the old product board (#585 charts, #788 ownership, #437 expansion, vision epics). Perf is explicitly NOT a blocker — deferred, opportunistic.

**Per-batch rule.** Each batch = one focused session. Branch → TDD → gates → Codex ckpt-2 → PR → poll → merge. Group = one mental context load.

---

## BATCH 1 — ETL correctness bugs (code-only, high value, fast)
Real wrong/silent behaviour. Verify each on the healthy dev DB.
- **#1439** — demo synthetic-fill `last=0.00` in the **execution** path (sibling of the merged #1428 read-path floor; finishes the quote-floor story). *Matters before any execution-guard work.*
- **#1320** — `PRE 14A` mixed into `sec_def14a` — split or parser-gate on `form_type`.
- **#1302** — 13F bulk dataset silently drops the `LEI` column (added by SEC 2023-01-03) — capture it.
- **#1303** — N-CEN classifier exists but is **unscheduled** (annual cadence drift) — wire the schedule.
- **#1293** — `candle_refresh` S2 `rows_processed=0` empty fetch, no error — surface/handle the empty-fetch.
- **#1442** — layer-init guard `_layer_initialization_blocks` **fail-open** on a transient init-check DB error — fail-closed (settled-decision noted this was tracked separately).

## BATCH 2 — Bootstrap state-machine + JobLock papercuts (code-only)
Same lock/state family — load together.
- **#1224** — stage stuck `pending` after `finalize_run` (run_id=3 S16 exemplar).
- **#1226** — JobLock contention with standalone crons surfaces as stage errors (UX papercut).
- **#678** — sync orchestrator crashes + lock contention with legacy cron (~20% `sync_runs` failing). *Older, same root; high value — flaky syncs erode trust.*

## BATCH 3a — ETL tech-debt: trivial sweep (code-only, low risk)
Knock out together.
- **#1460** — manifest CIK/accession format guard, **SEC-source-scoped** (design already in the issue; FINRA-synthetic-accession aware + ~20 fixture-realism updates).
- **#1462** — tradable-aware `instruments` floor (`COUNT(*) WHERE is_tradable=TRUE`).
- **#1437** — stale bootstrap comments + dead columns (`bootstrap_stages.expected_units/units_done`) + duplicated CUSIP-map loaders (PR-D of the prior plan).
- **#1358** — `db_fundamentals_raw` family has 2 Phase C jobs (one-per-family invariant) — reconcile + fix the stale test.
- **#1406** — `EBULL_SECRETS_KEY` env-var name mismatch (Settings reads `SECRETS_KEY`; docs say `EBULL_SECRETS_KEY` → silently ignored). *Security-adjacent — fix the name + docs.*
- **#1333** — migration-runner content-drift detection. *Directly relevant: this session edited an already-applied migration (sql/159) — the runner has no checksum, so drift is invisible.*
- **#1221** — extend `financial_facts_raw` partitions past 2030 + refine the DEFAULT-partition alarm.
- **NEW** — file: `instruments.instrument_type`/`instrument_type_id` NULL for all rows (#503 cross-validation field unpopulated). Decide: wire from eToro feed or drop the columns.

## BATCH 3b — ETL tech-debt: rewash/dedup/concurrency correctness (needs care)
Data-correctness; not trivial — own session.
- **#954** — 13F rewash dedup mismatch (`_upsert_holding` keeps first dup, observations record last).
- **#953** — 13F rewash leaves stale `ownership_institutions_observations` rows on parser-fix CUSIP changes.
- **#899** — `filed_at` semantic mismatch in insider observations (legacy + write-through).
- **#817** — rewash cohort scan does not lock rows against concurrent live-ingest.
- **#815** — `cik_raw_documents` cache races on cold-cache concurrent reads.

## BATCH 4 — Dev-DB hygiene + infra/supply-chain (mixed; some ops)
- **#1417** — bump `vitest >=4.1.0` (critical dev-dep advisory GHSA-5xrq-8626-4rwp). *Quick, do first.*
- **#1349** — `unresolved_13f_cusips` = 1.3GB / 6.7M rows for 58k distinct CUSIPs (dedup broken) — dedup-key fix + cleanup.
- **#1219** — VACUUM FULL `financial_facts_raw` runbook + tooling (reclaim ~25GB dev-DB file).
- **#1412** — pytest gate blocked on bloated 13M-file dev-PG volume. *Verify: likely mitigated by the 5433 split (#1447) + #1455 corpse sweep — may be closeable.*
- **#859** — pytest CI duration ~13-20min — investigate xdist/per-test setup.

## BATCH 5 — ETL docs sweep (low effort, batch ALL in one pass)
- **#1319** (per-source §12 real test IDs ×7), **#1321** (`_INTENTIONALLY_UNSUPPORTED_FORMS` rationale), **#1323** (retry-posture table), **#1324** (trim pagination language), **#1326** (304-as-success path), **#1330** (per-source spec template version stamp + lint).

## BATCH 6 — Bootstrap progress/timings UX (PR-B; operator's explicit ask)
Spec: `docs/proposals/admin/first-install-bootstrap-ux.md §4.2`. FE + backend session.
- **#1271** (timeline auto-refresh), **#1225/#1005** (wire `processed_count/target_count/last_progress_at` in the 5 bulk ingesters + overall %), **#1335** (progress UX redesign epic), plus admin papercuts **#1229** (`+N more` expandable), **#1230** (trigger-rejected reason inline), **#1266** (slow-conn fallback not error-colored), **#1267** ('Open admin' banner deep-link).
- **#1264** — verify-then-close: hide 'Re-run failed' on first-run row is **likely already done** by the merged #1451 (`isFirstRun` hides the iterate button). Confirm + close, don't re-implement.

## DEFER — ETL perf (NOT blockers; opportunistic only if bootstrap wall-clock hurts)
#1276 (per-row INSERT ceiling), #1275 (dispatcher `wait(all)` serialises lanes), #1274 (serial filer ingest under-uses budget), #1436 (stream TSV loads — PR-C), #482 (asyncio-blocking ingesters), #761/#763 (companyfacts DB-bound), #1338/#1339 (bulk-eligible Form 3/4), #1350/#1351 (S22 cohort trims), #1337 (bulk-first epic), #1378 (S15 deferred). The 98%-CIK-coverage proof means speed, not correctness, is all that's left here.

---

## PIVOT — old product board (the destination, once Batches 1-3 are clean)
Resume in roughly this order:
1. **#437 meta** — SEC/official data expansion + richer instrument page + rankings (umbrella for much below).
2. **#585 epic** — instrument-page L2 drill redesign: #591 risk/returns, #592 filings analytics, #593 news analytics, #594 peer radar/heatmap, #608 live volume, #671 FCF trend.
3. **#788 epic** — ownership card production-trustworthy: #790/#806/#807 (seed/Form 3/13F filer expansion), #809/#828 (raw-store + historical-CIK routing), #813 (CIK long-tail: ETF map/ADR resolver), #844/#917/#920-923/#961/#966 (DRS/N-PORT/chart panes/ESOP). *Directly lifts the residual coverage levers #1438 surfaced.*
4. **Vision feats** — #208 alt-data layer, #206 MCP conversational layer, #204 charting, #201 social sentiment, #189 Track-2 discovery, #198 autonomous fund vision; #316/#317 terminal shell.
5. **Regional data research** — #508-#523 (UK/EU/Asia/MENA/crypto/fx/commodity/Canada source investigations) — unblocks non-US instrument-page parity (#486/#493).

---

## Suggested next-session pickups (concrete)
- **Session N:** BATCH 1 (correctness bugs) — highest trust-per-effort, all code-only.
- **Session N+1:** BATCH 3a (trivial debt) + BATCH 4 #1417 (vitest) + verify-close #1264/#1412 — fast cleanup, shrinks the board count visibly.
- **Session N+2:** BATCH 2 (state/lock) OR BATCH 6 (progress UX, operator-facing) depending on appetite.
- BATCH 3b + 5 slot in opportunistically. Then PIVOT.
