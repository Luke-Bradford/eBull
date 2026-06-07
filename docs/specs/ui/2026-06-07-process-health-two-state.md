# Processes page: two-state health (act / don't-act)

**Status:** design · 2026-06-07 · umbrella #1508
**Supersedes parts of:** `docs/specs/ui/2026-06-06-process-health-verdict.md` (#1512 — the 4-state verdict)
**Builds on:** #1511 (source-level watermark), #1509 (retry/backoff), #1510 (liveness watchdog)

## Problem

The shipped page leaks three internal axes onto one row — the `ProcessStatus`
pill (*did the last run succeed?*), the `stale_reasons` chips (*overdue / behind
right now?*), and the rolled-up 4-state `health_verdict`
(`current`/`working`/`self_healing`/`attention`). The operator sees their
product and gets contradictory-reading rows: **`last run ok` + `attention` +
`schedule missed`**. Live dev examples (2026-06-07): `orchestrator_high_frequency_sync`
and `monitor_positions` both read `attention / schedule missed` despite a
successful last run.

The operator only ever needs one answer: **do I need to act, yes or no.**

## Root cause of the noise

`schedule_missed` fires on a **single** missed cadence slot
(`stale_detection.py:122-128`, `SCHEDULE_MISS_TOLERANCE_S = 60`) and is treated
as an actionable RED trigger (`health_verdict.py:51-52`). So a job that ran
fine a minute after its slot, or whose telemetry lags by one tick, paints RED
even though nothing is wrong. That is cadence-math answering "did it fire on its
clock?" when the real question is "is the data current / is anything stuck?"

## The rule (validated 2026-06-07 by operator-UX + adversarial lenses)

> **A row is GREEN unless something genuinely needs the operator. RED means act.**

The naive fix — *delete* `schedule_missed` + `watermark_gap` and lean on the
liveness watchdog — was rejected: both reviewers independently showed it makes
the page **lie green** (a job that succeeds doing nothing; long-cadence jobs
invisible for 3 cycles = weeks/months/years; the two orchestrator jobs excluded
from the watchdog entirely; a dead jobs-process → every row's last run is an old
success → all-green on a dead engine). The watchdog catches exactly one thing:
"a previously-firing, non-orchestrator job stopped firing for ≥3 cadence
cycles." It is not a sufficient backstop.

So the fix is **threshold-tuning, not deletion** — keep the signals, fire them
only when actionable:

### GREEN (calm — collapsed under the clean-bill header, no alarm)
- Latest terminal run **succeeded** (`ok` / benign `idle`-skip). A success
  **resets the overdue clock**, so it genuinely clears any earlier failure or
  missed slot — the operator's stated requirement.
- **Running now** → green with a quiet `updating…` hint.
- Latest run **failed but an auto-retry is in flight** (#1509 `retry_in_flight`)
  → green with `retrying HH:MM`. Nobody needs to look while it self-heals.
- A liveness-watchdog re-enqueue is recovering a stall (#1510) → green
  `re-enqueued, recovering`.

### RED (act — pinned, with the reason in plain words)
- Latest run **failed and retries are exhausted / not eligible** (no
  `retry_in_flight`) → `last run failed: <reason>`.
- **Genuinely overdue**: hasn't run in **more than a full cadence cycle** (not
  one late tick) → `hasn't run since <when>`.
- **Stuck mid-run** (`mid_flight_stuck`) — running but no heartbeat past the
  per-job threshold → `running but no progress`.
- **Queue stuck** (`queue_stuck`) — a dispatched request not picked up →
  `queued, not picked up`.
- **Source genuinely behind** (`watermark_gap`, source-level) → `source has new
  data not yet ingested`.
- **Jobs engine down** — see global banner below.

There is no amber and no third colour. `self_healing`/`working` fold into the
GREEN side as non-alarming text. Contradictory `ok + schedule_missed` rows
become impossible by construction.

## Changes

### C1 — `schedule_missed`: fire only when overdue by a whole cycle, anchored on `finished_at` (the core de-noise)
`app/services/processes/stale_detection.py` + `scheduled_adapter.py`.

Two coupled fixes (Codex ckpt-1 High #2):
1. **Anchor the overdue clock on the terminal run's `finished_at`, not
   `started_at`.** Today `expected_fire_at = compute_next_run(cadence,
   latest_terminal.started_at)` (`scheduled_adapter.py:803`). A healthy
   long-running job (starts :00, finishes :40 on a 15-min cadence) would
   already be "overdue" the instant it finishes. Anchor on
   `max(started_at, finished_at)` so a run that *just completed* resets the
   clock — this is what makes "a successful run clears the slate" literally
   true (the operator's requirement). For still-running rows the existing
   `status != "running"` guard already suppresses the rule.
2. **Threshold:** `overdue_at < now - max(cadence_period, FLOOR)` instead of
   `< now - 60s`, where `cadence_period` is the job's nominal interval (the
   adapter has the cadence already) and `FLOOR` (e.g. 5 min) protects
   every-5-min jobs from flapping. A job that ran within its last cycle is
   never `schedule_missed`; a job that skipped an *entire* cycle still
   surfaces — sooner than the watchdog's 3-cycle window, keeping long-cadence
   jobs honest (adversarial BLOCKING-1).

### C2 — `watermark_gap`: a CORRECT source-level "behind" predicate (not the inverse of fresh)
`app/services/processes/scheduled_adapter.py` (Codex ckpt-1 High #1 — the
biggest correction).

`_source_watermark_fresh` (#1511) is a **positive GREEN look-through** — it
returns `False` for no-rows / unknown-cadence / quiet source / fresh install.
`not source_watermark_fresh` is therefore NOT proof we're behind and would
false-RED quiet sources. Do **not** invert it.

**The signal is `data_freshness_index.state = 'error'`, source-level** (Codex
plan-review correction — `new_filings_since` is wrong: `data_freshness.py:363`
sets `state='current'` *with* `new_filings_since>0` right after a successful
ingest, so it is not a behind-signal; and `state='expected_filing_overdue'` is a
per-subject timing PREDICTION that fires routinely for event-driven forms when an
issuer simply doesn't file — that is the jitter we are removing). The state
machine sets `state='error'` only from a failed poll/ingest
(`data_freshness.py:367,388`), i.e. "we tried to keep this source current and
failed" — genuinely behind, actionable, and impossible to confuse with an issuer
choosing not to file.

`source_watermark_behind(source)` = `EXISTS (data_freshness_index WHERE source=X
AND state='error')` (LIMIT 1, source-level — any erroring subject means the
source's ingest is failing). Verdict reason: **"ingest failing"** (not "source
has new data" — the honest cause). This replaces the per-subject
`has_data_freshness_gap` (`expected_next_at < now`) timing probe entirely.

`source_watermark_fresh` (#1511) stays exactly as-is (GREEN promotion of
`pending_first_run`) — independent of this, not its negation. v1 must NOT paint
RED from `not source_watermark_fresh` (false-REDs quiet sources).

**Known residual (documented):** a job that polls successfully but silently
ingests nothing — without ever setting `state='error'` — is not caught by C2; it
relies on C1 overdue + the watchdog. A true upstream-vs-ingested diff would catch
it but the freshness index tracks our own polling view, not an independent
upstream truth, so it cannot be computed cheaply here. Tracked as a follow-up.

### C3 — collapse to two colours at the display layer (concrete FE changes)
`frontend/src/components/admin/processStatus.ts` + `ProcessesTable.tsx` +
`ProcessRow` (Codex ckpt-1 Medium — today `working` is pinned + blue and
`self_healing` is amber, so the two-state UI is NOT free):
- `VERDICT_VISUAL`: map `{current, working, self_healing}` → ONE calm green
  visual (dot + optional sub-label, no alarm colour); `attention` → red. Drop
  the blue `working` and amber `self_healing` palettes.
- `VERDICT_SORT_PRIORITY`: only `attention` sorts to the top/pinned region;
  `working`/`self_healing` sort with `current` (calm group).
- `ProcessesTable` pinning: pin ONLY `attention`. Remove the current
  `working`-is-pinned rule (`ProcessesTable.tsx:124`) — a running/recovering job
  is not something the operator must look at.
- Sub-label copy on the green row: `updating…` (running), `retrying HH:MM`
  (retry in flight), `re-enqueued, recovering` (liveness kick). Calm text, not a
  badge colour.
- History drill-down unchanged — past errors stay one click away, never
  alarming inline once a later run succeeded.

`compute_verdict` keeps its internal 4-value vocabulary (so the retry / kick /
wedge precedence from #1509/#1510 is preserved verbatim — the invariant "a
genuine wedge is never masked" still holds). Behaviour changes only via C1 (when
`schedule_missed` is emitted), C2 (what `watermark_gap` means), and the FE colour
mapping (C3). **No new verdict states.**

### C4 — global engine-down banner (close the dead-engine blind spot)
Correct surface (Codex ckpt-1 High #3): the scheduler/jobs-process heartbeat
aggregate lives on **`/system/jobs`** (`app/api/system.py:622`), NOT
`/system/status` — and `/system/status` `overall_status`
(`app/api/system.py:242`) currently folds only stalled-jobs / latest-job /
layers, **ignoring heartbeat state**. So the Processes page does not yet have the
fact it needs.

Requirement: the page must go hard RED globally — independent of every per-row
last-run — when the jobs-process heartbeat is stale. Either (a) fold heartbeat
staleness into `/system/status` `overall_status` and consume it in the header,
or (b) have the Processes header read the `/system/jobs` heartbeat aggregate
directly. Decide the minimal wiring at plan time; the invariant is that a dead
engine can never render an all-green page (operator-UX WARNING-4 / adversarial
BLOCKING-3).

### C5 — `orchestrator_full_sync` honest overdue via `sync_runs` (not just high-frequency)
`app/services/processes/scheduled_adapter.py` (Codex ckpt-1 Medium #5). Today
only `orchestrator_high_frequency_sync` is terminal-resolved from `sync_runs`
(`_ORCHESTRATOR_SYNC_SCOPE`, `scheduled_adapter.py:306`). `orchestrator_full_sync`
writes `sync_runs` too but is **excluded from the liveness watchdog**
(`job_liveness.py` exclusion set) AND not sync_runs-resolved here — so under C1 it
could lie GREEN (stale `job_runs` success) or false-RED. Add
`orchestrator_full_sync` → its `sync_runs` scope so its `finished_at`-anchored
overdue (C1) is honest, exactly like high-frequency. Both orchestrator jobs then
go overdue → RED when their sync genuinely stops.

## Explicitly NOT changing
- `queue_stuck` / `mid_flight_stuck` detection (genuine wedges — stay RED).
- The retry/backoff (#1509) and liveness-watchdog (#1510) mechanisms.
- `compute_verdict`'s precedence invariant (wedge never masked).
- The History / logs drill-down.

### C6 — never-run bound (concrete; Codex ckpt-1 High #4)

C1 cannot cover a job that has NEVER run (no terminal row → no
`expected_fire_at`), and the liveness watchdog explicitly excludes lifetime-zero
jobs (`job_liveness.py:24`). Today such a job reads `working "first run pending"`
forever. Bound it concretely:

- **Anchor:** `first_expected_fire` = first cadence occurrence strictly after a
  **persisted job-first-seen timestamp**, NOT the volatile jobs-process start
  (Codex re-review #2 — process-start resets the grace window on every restart,
  so a long-cadence never-run job would lie green indefinitely across restarts).
  Persist a `job_first_seen` row per job_name on first registry load (small
  schema add; if a registry/install timestamp already exists, reuse it). For
  `catch_up_on_boot=True` jobs a missing row past this anchor is a real
  "never started".
- **Threshold:** `now - first_expected_fire > max(cadence_period, FLOOR)` (same as C1).
- **Adapter field + verdict:** a new boolean (e.g. `never_started`) flips the
  `pending_first_run` branch in `compute_verdict` from `working "first run pending"`
  to `attention "never started"`.
- **Tests:** within-grace never-run → working; past-grace never-run → attention.

## Edge cases (from validation)

- **Succeeds-doing-nothing because ingest is ERRORING**: C2's
  `source_watermark_behind` (`state='error'`) is the backstop — a source whose
  poll/ingest is failing goes RED even while the job_runs row reads "success".
  (operator-UX BLOCKING-1.) The narrower "polls fine but silently drops
  everything, no error" case is the documented C2 residual (relies on C1 +
  watchdog).
- **Operator-initiated cancel** (made concrete — Codex re-review #3): today
  `compute_verdict` maps `cancelled` → `attention` forever (`health_verdict.py:171`).
  Wire it: adapter sets `cancel_was_operator_initiated` by matching the terminal
  run's `cancelled_at` to a `process_stop_requests` row for that job (the join
  already exists for the cancel path). `compute_verdict` precedence: an
  operator-initiated `cancelled` → green (benign) until the next fire; a
  system/crash `cancelled` (no matching stop-request) → `attention "last run
  cancelled"`. Tests: operator-cancel → green; crash-cancel → attention.
- **Flapping job** (fail/succeed/fail/succeed): reads steady green between
  flaps. Acceptable for v1; a "recent failures (N)" badge on green rows is a
  deferred enhancement, not v1. (operator-UX NITPICK-8 — DEFERRED)

## Test

- **C1** pure table-tests (`stale_detection.compute`): ran-within-cycle → no
  `schedule_missed`; skipped-full-cycle → `schedule_missed`; `finished_at`-anchor
  (a long run that finishes after a nominal slot does NOT immediately read overdue);
  FLOOR protects every-5-min.
- **C2** db tests: a `state='error'` row for the source → `source_watermark_behind`
  true → `watermark_gap` → red "ingest failing"; all-`current` source / no rows /
  fresh install → NO `watermark_gap` (the false-RED Codex flagged);
  `source_watermark_fresh` GREEN promotion unchanged.
- **Invariant (explicit — Codex ckpt-1 Medium #7):** `retry_in_flight` and
  `liveness_kick_in_flight` each suppress ONLY `schedule_missed` and NEVER
  `watermark_gap` / `queue_stuck` / `mid_flight_stuck` — a wedge co-present with a
  retry/kick still returns `attention`. Table-test all `{retry,kick} × {each wedge}`.
- **C6** never-started: within-grace → working; past-grace → attention.
- **FE** unit: `{current, working, self_healing}` → green visual + correct
  sub-label; `attention` → red + reason; only `attention` pins.
- **Dev-verify:** `orchestrator_high_frequency_sync` / `monitor_positions`
  false-red rows read green after C1; a genuinely-overdue daily job reads red; a
  source genuinely behind reads red via C2.

## Definition of done
Operator sees a page where green means green and red means act; no contradictory
rows; a dead engine / silently-behind source / stalled long-cadence job still
turns red; past errors remain in the drill-down.
