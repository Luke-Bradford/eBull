# Admin Processes — self-healing jobs + single health verdict

Status: Proposal (2026-06-06). Evolves the shipped `admin-control-hub-rewrite.md`.
Builds on / supersedes-in-part: #1489 (dual-chip semantics), #649 (self-healing freshness), #1335 (bootstrap UX epic).

## Goal (operator's words)

> "I don't want anyone needing to even look at this page, just trust it is keeping everything up to date."

After bootstrap the system is current AND stays current with no manual job-kicking. The page shows a **clean bill of health** by default. Only a **genuine** issue surfaces, and it says plainly: what it is, whether the operator must act, or that it was transient (e.g. a timeout) and will be picked up later.

## Problem — why the page confuses today (root-caused)

The shipped model renders **two orthogonal axes** per row and the operator sees their product:

1. **`ProcessStatus` pill** (`ok|idle|failed|pending_first_run|running|pending_retry|cancelled|disabled`) — `app/services/processes/scheduled_adapter.py::_status_for`. `ok` = *last terminal run succeeded*, independent of staleness.
2. **`stale_reasons` chips** (`schedule_missed|watermark_gap|queue_stuck|mid_flight_stuck`) — `app/services/processes/stale_detection.py`, computed independently.

Contradictory combos result (#1489): "ok + schedule missed", "idle + schedule missed", "first run pending". `idle` = last run `skipped` (gated). `pending_first_run` = no terminal `job_runs` row.

Plus three behavioural gaps behind the noise:

- **No post-bootstrap kick.** `bootstrap_state.finalize_run` flips `status='complete'` and stops. Each gated job waits for its next *natural* cadence slot (daily ≤24h, weekly ≤7d, monthly ≤1mo, **yearly ≤12mo**). The boot catch-up (`runtime._catch_up`) runs **once** and is trapped by the universal gate: jobs evaluated before bootstrap finished wrote a `skipped` row and never re-evaluate until the next process restart (prevention-log 1339-1343). Yet bootstrap already populated most data via bulk stages — so most "first run pending" rows are **cosmetic**, while a few are **genuine gaps** (FINRA SI/RegSHO, def14a bodies, bulk-archive refreshes).
- **Failure is visible but not self-healing.** A throw → `job_runs.status='failure'` (`_tracked_job` → `record_job_finish`) → red pill + `/system/status` `overall_status='down'`. But there is **no job-level retry/backoff** — a failed daily job stays failed ~24h until its next slot; weekly a full week.
- **Stalls are invisible here.** The #1500 liveness watchdog (`job_liveness.find_stalled_jobs`) detects "stopped firing" but only via `GET /system/job-liveness` — **not wired into the Processes page nor `/system/status` overall_status**, and it only *reports*, never *acts*. No proactive push exists (only the PG crash detector).

## Target model (3 approved decisions)

1. **Single health verdict** per row (collapse the two axes).
2. **Seed + kick gaps** after bootstrap (don't re-run expensive drains).
3. **Self-heal in-system** now (watchdog acts + job-level retry/backoff + stall in rollups); proactive push deferred.

### 1. One verdict (computed layer, not an enum rewrite)

Add `health_verdict: Literal["current","working","attention"]` + `self_healing: bool` as a **computed field** on `ProcessRow`, derived from the existing `status` + `stale_reasons` + liveness + retry-state. Keep the underlying enums (reuse, non-breaking); the FE renders the verdict, not the raw axes.

| Verdict | Colour | Means | Derived from |
|---|---|---|---|
| **Current** | green | fresh / on-cadence / seeded by bootstrap | `ok` and no actionable stale-reason; or `pending_first_run` where source watermark is fresh |
| **Working** | blue | running / queued | `running` (no `mid_flight_stuck`), live `pending_job_requests` |
| **Self-healing** | amber, "no action needed" | failed/missed/stalled **and** auto-retry scheduled | `pending_retry`, or `failed`/stall with `next_retry_at` set, or re-enqueued by watchdog — shows "will retry HH:MM · reason" |
| **Needs attention** | red, rare | won't auto-recover / repeated failures / unfixable stall | `failed` with no retry in flight & not covered, exhausted retries, gate-blocked needing operator |

**Look-through:** a single gated `skipped` (latest terminal) must not mask an otherwise-current job once the gate is open and a run is imminent — verdict reads the relevant signal, not just `ORDER BY started_at DESC LIMIT 1`. Reasons render **inline** (folds in #1230), never hover-only. Remove dead `stale` literal.

### 2. Clean bill of health (reconcile with admin-triage "no banner")

admin-triage decided *"no 'No problems!' banner"* to avoid clutter. The operator now explicitly wants a positive all-clear. Reconciliation: a **quiet page header** ("All systems current · checked 14:03"), not a toast. When issues exist: "N need attention · M self-healing", Needs-attention surfaced, Self-healing collapsed by default. This is a deliberate, documented reversal scoped to the page header only.

### 3. Self-heal in-system

- **Job-level retry/backoff** — classify the failure: transient (timeout/network/429/lock-contention) → `next_retry_at` on short backoff; permanent (logic/validation) → straight to Needs-attention. Enables the "will retry HH:MM" verdict.
- **Watchdog acts** — `job_liveness` re-enqueues stalled jobs via the **audited manual-queue path** (`pending_job_requests`, terminal-state correct per prevention 1217), and wire stall into `/system/status::_derive_overall_status` (today only `failure` rolls up) + the row verdict. **CAVEAT (design constraint):** classify *why* stalled before acting — never re-enqueue a job stalled by its own rate-limit/gate into that same limit (relates #1484). Re-enqueue must be idempotent + bounded.
- **Post-bootstrap auto-current** — on `finalize_run` completion: (a) **seed `data_freshness_index` watermarks** for sources bootstrap already populated (13F/insider/funds/companyfacts/manifest) so those rows read **Current**, not "first run pending"; (b) **kick only genuine-gap jobs** (FINRA SI/RegSHO, def14a bodies, bulk refresh) via the audited manual-queue path; (c) re-evaluate the gate-skipped catch-up jobs now the gate is open (closes the catch-up trap). "Bootstrap-covered" = a job whose output table was written by a bootstrap stage this run (derive from `_BOOTSTRAP_STAGE_SPECS` → sink-table map).

## Settled-decision compliance

- **Universal gate (#1064/#1181):** the post-bootstrap kick uses the existing **manual-queue dispatch + `decision_audit`** path — it does NOT add a new `exempt_from_universal_bootstrap_gate` carve-out (that allow-list stays 2 jobs, test-pinned). Gate opens first (status=complete), then kick.
- **Two-axis (control-hub §A1):** superseded by the computed verdict; underlying enums retained so adapters/tests need minimal change.
- **catch-up trap (prevention 1339-1343):** the post-bootstrap re-evaluation is the documented fix; audited so a gate-skipped fire is re-queued, not silently lost.
- **None vs skipped (prevention 249):** `pending_first_run`/never-run stays a distinct input to the verdict — not folded into "attention".
- **skip ≠ completed (prevention 1217):** all re-enqueue/kick paths mark terminal state correctly.

## Decomposition

| Ticket | Scope | Builds on |
|---|---|---|
| **Epic** | clean bill of health + self-heal | #1489, #649, #1335 |
| T1 verdict (BE+FE) | computed `health_verdict`/`self_healing` over existing axes; inline reasons; remove dead `stale` | supersedes #1489; folds #1230 |
| T2 clean-bill header (FE) | quiet page-level summary; collapse non-actionable | reverses admin-triage banner decision |
| T3 retry/backoff (BE) | transient-vs-permanent classification + `next_retry_at` on job_runs | new; foundation for "will retry" |
| T4 watchdog-acts (BE) | re-enqueue stalls (cause-aware) + wire stall into overall_status + verdict | extends #1500; builds #649; caveat #1484 |
| T5 post-bootstrap auto-current (BE) | seed watermarks + kick gap-jobs + close catch-up trap | builds #649; relates #1438, #1224 |
| T6 proactive push (DEFERRED) | notify on Needs-attention | follow-on |
| T7 next_run honesty (tech-debt) | API `next_run` is cadence-math only ("declared") — reconcile with real scheduler state or label honestly | small |

**Build order:** T3 → T4 → T5 (foundation: make self-healing real) → T1 → T2 (surface it) → T7 ; T6 last.

Each ticket gets its own spec at pickup (repo spec-first + Codex checkpoints).

## Risks / open

- Re-enqueue-into-rate-limit (T4) — the load-bearing caveat; needs the stall-cause classifier before it can act.
- "Bootstrap-covered" derivation (T5) must track `_BOOTSTRAP_STAGE_SPECS` drift (lint/test).
- Verdict mapping must be exhaustive over the current enum × stale-reason matrix (table-test).
