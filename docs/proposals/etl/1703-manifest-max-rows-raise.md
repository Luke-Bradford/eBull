# #1703 — Raise manifest worker `max_rows` 100 → 200 + keep 13F off the topup flood

Status: unshipped proposal. Closes the #1703 thread (PR1 de443459 drained the
out-of-retention 13F fetch-storm; PR2 535b2462 batched the per-holding
`refresh_institutions_current` write-amplification; this PR is the actual raise).

## Problem

`sec_manifest_worker_tick` drains `max_rows=100` rows/tick at a 5-min cadence
(`app/workers/scheduler.py:4841`). The form4 backlog alone is **443,055 pending**
(dev, 2026-06-22); the global drain is throughput-bound. #1703's goal: raise
`max_rows` — gated all along on "what is the per-tick wall, and is the raise
cadence-safe?".

## Source rule / governing constraint

`max_rows` is an engineering tunable, not a settled decision — no SEC reg fixes
the value. The governing constraint is **SEC EDGAR fair-access: 10 requests/second
maximum** (documented `sec_edgar.py:16`; enforced process-globally by the shared
rate gate `_PROCESS_RATE_LIMIT_LOCK` / `_PROCESS_RATE_LIMIT_CLOCK`,
`sec_edgar.py:58-85`, #726; reference: `.claude/skills/data-sources/sec-edgar.md`).
The raise must hold **0 HTTP-429s** at steady cadence under the shared budget.

### Full-population pending composition (dev, 2026-06-22)

`pending` = `ingest_status='pending'`. ("Retryable" rows — `ingest_status='failed'
AND (next_retry_at IS NULL OR next_retry_at <= NOW())`, `iter_retryable`,
`sec_manifest.py:667` — are a tiny, separate eligibility class; 457 `failed` rows
total exist, negligible vs the pending corpus below.)

| source | pending | oldest filed_at |
|---|---|---|
| sec_form4 | 443,055 | 2023-06-27 |
| **sec_13f_hr** | **66,785** | **2024-07-18 (all in-retention post-PR1)** |
| sec_10q | 50,743 | 2021-10-07 |
| sec_form3 | 45,454 | 2021-10-01 (global oldest) |
| sec_def14a | 28,559 | 2023-03-17 |
| sec_13g | 27,568 | 2025-04-25 |
| sec_13d | 2,621 | 2025-09-12 |
| sec_8k | 2 | 2026-06-22 |

## Measurements (daemon STOPPED + confirmed-down, single ticks, 0 429s each)

Path = `run_manifest_worker(conn, source=…, max_rows=…)` on this branch's parent
(batched-refresh code, 535b2462). Measurements, not acceptance evidence —
acceptance is the full-day cadence verify below.

| tick | elapsed | 13F rows | note |
|---|---|---|---|
| source=None, max_rows=200 | **54.4s** | 14 | current regime; form3 owns the global-oldest topup |
| source=sec_13f_hr, max_rows=200 | **878.5s** | 200 | forced heavy 13F; ≈**4.4s/filing** |
| source=sec_13f_hr, max_rows=30 | **94.0s** | 30 | oldest-30 in-retention ≈3.1s/filing |

**Finding:** PR2 removed the per-holding-MERGE wall; the wall is now the **serial
`infotable.xml` fetch+parse per 13F filing** (~3-4.4s each — a 13F holds thousands
of holdings; the infotable sits behind a post-primary retention gate and is NOT
prefetch-overlapped). 200 heavy 13F = 878s. So `max_rows` cannot be raised naked:
the only way 13F count/tick exceeds its fair quota is the **global-oldest Phase B
topup** shifting onto 13F (reached once form4/form3/10q pre-2024 drain below 13F's
2024-07 floor). Bound *that* path and the raise is safe.

## Design

1. **`max_rows` 100 → 200** (`scheduler.py:4841`). Doubles per-tick drain of the
   cheap single-doc sources (form4/form3/10q/def14a/13d/13g — ~0.2-0.3s/filing),
   the real backlog. Also doubles 13F's Phase-A quota (`~max_rows/n`, n≈14 → ≈14;
   matches the measured 14) — so 13F's normal slice rises 8→~14/tick (~2×), still
   light.

2. **Exclude `sec_13f_hr` from the Phase B global-oldest topup** (fairness path
   only). The Phase B topup (`iter_pending_topup` / `iter_retryable_topup`,
   `sec_manifest.py:721/760`) is the ONLY path that can admit a source *beyond* its
   fair quota — it picks global-oldest across `sources`. Removing 13F from the
   topup `sources` arg caps 13F at its **Phase R recent + Phase A quota share** —
   a *configuration-derived* bound `≈ max_rows/n` (n = registered sources ≈ 15), so
   at `max_rows=200` 13F ≈ 13-16/tick, independent of *how far the backlog has
   drained* (the topup-flood path — the only way 13F exceeds quota — is removed).
   NOT independent of `max_rows`: a future raise scales the quota share, so any
   later `max_rows` bump MUST re-check `quota_share × per-filing-cost` stays under
   cadence. At the proposed 200: worst 13F slice `≈ 16 × 4.4s ≈ 70s` ⇒ a
   13F-saturated tick is impossible; the worst tick is ~the measured 54s. The
   heavy-topup regime is eliminated by construction, not bounded by a sample.

   - **No underfill.** Both topup SQLs filter `source = ANY(%s::text[])` and
     short-circuit on empty (`sec_manifest.py:736/774`). With 13F removed, the
     one-shot `LIMIT remaining` simply selects the next-oldest *non-13F* rows
     (form4/form3 — 443k+ available) → the tick still fills to `max_rows`. The
     freed budget rolls forward via the SQL itself, not a post-trim (this is why a
     running-counter post-trim cap was rejected — it drops topup rows without
     re-querying other sources → underfills; Codex ckpt-1 BLOCKING).

   - **Fairness path only.** Applies to `source is None`. The `source is not None`
     per-source rebuild path (operator `sec_rebuild`) is untouched — an explicit
     single-source drain must not be throttled (and has no topup phase at all).

   - Configurable: module-level `_TOPUP_EXCLUDED_SOURCES: frozenset[ManifestSource]
     = {"sec_13f_hr"}` so a future heavy source can be added without re-plumbing.

### Why exclude-from-topup, not a numeric cap or a lower `max_rows`

- A blanket lower `max_rows` throttles the cheap 443k form4 backlog to protect
  against the rare heavy 13F slice — wrong trade.
- A numeric per-source counter cap (e.g. 13F≤30) needs the topup to re-query other
  sources when capped rows are trimmed, else it underfills (Codex BLOCKING). The
  exclude-from-topup approach gets the same bound (13F→quota) with zero new
  mechanism and zero underfill, because 13F's quota (~14-16) is already below any
  cap we'd pick. The numeric cap's only extra value is letting 13F drain *faster*
  than quota — unnecessary (see drain check).

### Drain check

13F normal-regime drain rises 8/tick → ~14/tick = ~4,032/day; backlog 66,785
clears in ~16 days plus easily absorbs quarterly inflow. Cheap sources double to
200/tick. Acceptable for a long-horizon engine; cadence-safety is the priority.

### Known limitation (far-future, documented not fixed)

If form4/form3/10q/def14a all drain and 13F becomes the *only* backlogged source,
the topup has nothing non-13F to pick → ticks underfill and 13F drains at its
quota (~14/tick) only. That endgame is a near-caught-up state (ticks ~70s, safe);
revisit 13F's allocation then. Irrelevant while form4 has 443k pending.

## Worst-case / cadence safety

- Worst fairness tick ≈ the measured 54s (13F structurally ≤ ~16). < 250s
  acceptance < 300s cadence ⇒ no `max_instances` skip.
- 0 429s observed at 200 rows/tick (one tick); the full-day verify is the
  acceptance gate for steady-cadence 429-freedom under concurrent SEC jobs.

## Tests (pure-logic, no DB)

`compute_quotas` expectations derived via `sorted(registered_parser_sources())`
(prevention-log #1344/#1345 — never a hand-ordered literal). New tests:
- (a) an excluded source is never present in the Phase B topup `sources` arg /
  never admitted beyond quota even when it dominates the global-oldest order;
- (b) with the excluded source removed, the tick still fills to `max_rows` from
  other sources (no underfill);
- (c) the per-source rebuild path (`source is not None`) ignores the exclusion;
- (d) a source absent from `_TOPUP_EXCLUDED_SOURCES` behaves exactly as today.

## Rollout / dev-verify (acceptance)

Restart daemon onto this branch; verify over a FULL DAY at normal cadence: worst
slice < ~250s, no skipped ticks, 0 429s under concurrent overnight SEC jobs. Do
NOT merge the value before the multi-day verify (the #1700 lesson cost 3 PRs).

## Process lesson (→ review-prevention-log.md in this PR)

Dev-verify a throughput-config change on the **normal daemon cadence** (one tick /
5 min, bounded 10 req/s), NOT a barrage of heavy ad-hoc worker ticks — the latter
over-drives SEC's shared throttle into a 429 storm and invalidates the timing. If
a heavy regime must be forced for a point measurement, do it with the daemon
STOPPED + confirmed-down (read the down-confirmation before launching) and a
SINGLE tick.
