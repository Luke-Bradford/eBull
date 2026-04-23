# ADR 0004 — Fundamentals ingest shape (phase A)

Issue: #414 investigation phase A (root-cause path for site-freeze during `fundamentals_sync`).
Status: **Proposed** — measurement only. Implementation PR is separate.
Date: 2026-04-23

## Context

`fundamentals_sync` has been the lever that takes the operator UI down on every restart while seeding. Four stabiliser PRs shipped (#409, #411, #412, #413) without touching the underlying shape — #413 just disables `catch_up_on_boot` so restarts stop re-firing the heavy job.

Two signals motivated this investigation:

1. 4,582 of 5,134 covered US CIKs have no `sec.submissions` watermark. Steady-state ingest cannot progress past seeding.
2. While `fundamentals_sync` runs, operator HTTP latency spikes into tens of seconds.

Issue #414 enumerated four candidate architectures (E1 batched same-process, E2 `ProcessPoolExecutor`, E3 standalone worker, E4 out-of-process script) and explicitly gated architecture selection on first closing **investigation A: is the freeze GIL contention, transaction-lock contention, or something else?**

This ADR records what the DB-path benchmark *can* conclude and what it *cannot*, and picks a v1 shape accordingly.

## What the bench measures

`scripts/bench_fundamentals_upsert.py` runs against the isolated `ebull_test` Postgres and times three shapes of the per-CIK upsert against a synthetic 10,000-fact payload:

- **A — row-loop** (current prod shape at [app/services/fundamentals.py:300-374](../../app/services/fundamentals.py#L300-L374)).
- **B — `executemany(page_size=1000)`** — same SQL, batched round trips.
- **C — COPY STDIN into a TEMP staging table, then one `INSERT … SELECT … ON CONFLICT`**.

Three scenarios per shape:

- `seed` — empty index, all INSERTs hit new rows.
- `re-upsert no-op` — same payload re-upserted; the `WHERE IS DISTINCT FROM` filter short-circuits, so no row is rewritten. Models the watermark-unchanged steady-state path.
- `restatement` — same identity tuple, mutated `val`; the DO UPDATE path actually rewrites rows. Models a filing restatement.

## What the bench does **not** measure

All of these remain open after this ADR and must be checked again once the shape change ships:

- **Concurrent HTTP latency.** The bench is single-threaded. It cannot prove that a faster DB path eliminates the operator-UI freeze; it only proves the DB path gets cheaper. If residual freeze persists after shipping the shape change, parser-CPU / GIL contention becomes the suspect.
- **GIL pressure from XBRL parsing.** The bench uses pre-generated synthetic facts and does no parsing work.
- **Lock-wait behaviour.** `pg_stat_activity` / `pg_locks` is not sampled during the bench. It cannot say whether the current freeze is caused by a reader waiting on the ingest's write transaction or by round-trip-starved async handlers.
- **Absolute production wall-clock.** The bench starts from an empty `financial_facts_raw` table; production carries millions of rows (the repo's own comments on [sql/048_financial_facts_raw_identity_constraint.sql:38-39](../../sql/048_financial_facts_raw_identity_constraint.sql#L38-L39) reference a 10M-row working set). B-tree insert cost is O(log N), so absolute per-CIK durations on a large existing index will be higher than the numbers below. **Only the *ratio* between shapes should be taken as signal**; the absolute wall-clock on a cold test DB is not a production predictor.

## Numbers (10,000 facts, ebull_test, bench run 2026-04-23)

| Shape                              | seed (s) | no-op (s) | restatement (s) | facts/s (seed) |
|------------------------------------|---------:|----------:|----------------:|---------------:|
| A — row-loop (current prod)        |    5.08  |     4.64  |          7.36   |          1,967 |
| B — executemany(1000)              |    0.28  |     0.25  |          0.29   |         35,613 |
| C — COPY → temp → INSERT … SELECT  |    0.16  |     0.06  |          0.19   |         60,993 |

Ratios vs current prod (higher = faster):

| Shape | seed | no-op | restatement |
|-------|-----:|------:|------------:|
| B     | 18×  | 18×   | 25×         |
| C     | 31×  | 75×   | 39×         |

The ratio is remarkably consistent across scenarios: batching collapses round-trip overhead by ~18× regardless of whether the underlying work is INSERT-new, no-op skip, or actual rewrite.

## What the numbers do and don't justify

They **do** justify:

- A meaningful DB-path optimisation exists. Shape B shrinks the per-CIK transaction duration by ~18× on this bench. If production per-CIK duration tracks the same ratio, seed-time transactions drop from multi-second to sub-second, which directly reduces the window during which any operator request could block on lock contention against `financial_facts_raw`.
- Shape B is the smallest possible fix: same SQL, same ON CONFLICT clause, same WHERE filter — only the call shape changes.

They **do not** justify:

- Rejecting E2 (`ProcessPoolExecutor`) / E3 (standalone worker) / E4 (OS scheduler) outright. Those architectures exist to isolate GIL pressure, which this bench does not characterise. They remain on the table as follow-ups if B does not land the freeze.
- Claiming the freeze is "solved" once B ships. B is a necessary fix for the DB path regardless of what else is causing the freeze; it may not be sufficient.

## Decision

**v1: pick Shape B within E1 (same-process, batched upsert).**

Rationale:

- **Smallest diff that addresses the measured issue.** Same SQL, same identity index, same ON CONFLICT clause — one function body changes.
- **No new concurrency model.** Avoids the operational complexity jump of E2/E3/E4 until we can show they are actually needed.
- **Observable outcome.** After B ships, the implementation PR must add per-CIK timing at the `_run_cik_upsert` boundary (log-line or a new small column on `data_ingestion_runs`-like side table — `data_ingestion_runs` today is per provider batch, not per CIK). With that in place, the residual freeze question (GIL vs lock) becomes answerable in-prod by comparing per-CIK transaction wall-clock against operator-UI latency during an active seed.

Shape C is **deferred to v2** — the additional 4–8× lift from COPY + INSERT SELECT is worth considering once B is in, but the TEMP-staging indirection adds complexity and a pyright-visible schema drift risk. Re-evaluate after B has run in prod for a cycle.

E2 / E3 / E4 are **deferred, not rejected.** Each remains an option if B does not visibly reduce operator-UI latency during a seed run. The decision between them will need the in-prod probe that this bench deliberately does not attempt.

## Open questions carried into the implementation PR

The implementation PR for Shape B must answer:

1. **Does the production per-CIK transaction drop below 1 s in practice?** The current `data_ingestion_runs` table records per provider batch (not per CIK), so the implementation PR must add per-CIK timing instrumentation (log-line at the `_run_cik_upsert` boundary, or a small addition to the ingest-run observability surface). Without that, we cannot answer this question.
2. **Does operator-UI latency stop spiking during `fundamentals_sync`?** Compare p95 on `/health`, `/login`, and `/admin/jobs` while a seed is in flight — before and after.
3. **If (1) is yes and (2) is no** → GIL / parser is the suspect; revisit E2/E3. This is the piece the current bench deliberately does not attempt to settle.

## What this ADR does not change

This ADR is measurement only. Nothing in `app/` is touched. The only artefact is:

- `scripts/bench_fundamentals_upsert.py` — re-runnable benchmark against the isolated `ebull_test` DB (guarded — never touches the dev DB).

## Related

- Parent issue: #414.
- Unblocked follow-ups: #410 (submissions.json backfill), #414 B (cadence), #414 D (seed cap), #414 F (runtime toggles), #414 G (observability).
- Settled-decisions check — product-visibility pivot (2026-04-18): the current ingest shape is *hurting* operator visibility (freeze-induced UI outage), so the stabiliser track remains in scope.
