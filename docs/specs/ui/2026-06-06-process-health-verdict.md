# #1512 — single health verdict (computed layer over status + stale_reasons)

> **Status:** SPEC v1 2026-06-06. Child of epic #1508. Evolves `docs/proposals/ui/admin-processes-self-healing-health.md` §1. Subsumes #1489; folds #1230.

## 1. Problem

Each Processes row renders **two orthogonal axes** and the operator sees their product:

- `ProcessStatus` pill (`scheduled_adapter._status_for` etc.) — `ok` = *last terminal run succeeded*, independent of staleness.
- `stale_reasons` chips (`stale_detection.compute`) — *overdue / behind right now*.

Contradictory combos (verified dev 2026-06-06): `ok`+`schedule_missed` (monitor_positions, sec_per_cik_poll), `idle`+`schedule_missed` (ownership_observations_sync, cusip_extid_sweep), `ok`+`watermark_gap` (sec_filing_documents_ingest + 4 sweeps). To the operator: "which is it?"

## 2. Design — one computed verdict, non-breaking

Add a **computed** `health_verdict` + `self_healing` + `verdict_reason` to `ProcessRow`, derived from the existing `status` + `stale_reasons`. **Keep the underlying enums + adapters + tests** (reuse, non-breaking). The FE renders the verdict pill, not the two raw axes.

```
HealthVerdict = Literal["current", "working", "self_healing", "attention"]
```

| Verdict | Tone | Means | Operator action |
|---|---|---|---|
| **Current** | green | fresh / on-cadence / gated-benign | none |
| **Working** | blue | actively running, progressing | none |
| **Self-healing** | amber | failed/overdue **and** auto-recovery in flight | none — "will retry HH:MM" |
| **Needs attention** | red | won't auto-recover | act |

### 2.1 Pure function

New module `app/services/processes/health_verdict.py`:

```python
def compute_verdict(
    *,
    status: ProcessStatus,
    stale_reasons: tuple[StaleReason, ...],
    mechanism: ProcessMechanism,
) -> tuple[HealthVerdict, bool, str]:
    """Return (verdict, self_healing, verdict_reason). Pure; table-tested."""
```

Called once centrally in `app/api/processes.py::_convert_row` (the single choke point all three adapters flow through) so no adapter changes are needed for v1. Inputs available there today: `row.status`, `row.stale_reasons`, `row.mechanism`.

> **First cut scope.** T3 (retry/`next_retry_at`) / T4 (watchdog re-enqueue) / T5 (post-bootstrap seed+kick) are NOT yet landed. So v1 has no `next_retry_at` signal: the only `self_healing` source is the existing `pending_retry` status. When T3/T4/T5 land they extend `compute_verdict` (add `next_retry_at` / liveness inputs) to reclassify *covered* overdue rows from `attention` → `self_healing` with a "will retry HH:MM" reason. v1 is deliberately conservative: an overdue row with no recovery mechanism reads **attention** (honest — it IS currently stuck; that is the bug #1511 fixes).

### 2.2 Verdict mapping (precedence top→bottom, first match wins)

`ACTIONABLE_STALE = {schedule_missed, watermark_gap, queue_stuck, mid_flight_stuck}` (all four actionable in v1).

**Key invariant (Codex ckpt-1 fix):** an actionable stale reason must NEVER be masked by a status. So `disabled` (kill switch, global) is the only thing that beats stale; every other status is evaluated *after* the stale check. This closes the `running+queue_stuck → working` and `pending_retry+queue_stuck → self_healing` masking gaps Codex flagged.

| # | Condition | verdict | self_healing | reason |
|---|---|---|---|---|
| 1 | `status == disabled` | attention | F | "kill switch active" |
| 2 | any reason in `ACTIONABLE_STALE` | attention | F | *headline* (see below) |
| 3 | `status == running` | working | F | "" |
| 4 | `status == pending_retry` | self_healing | T | "retry scheduled" |
| 5 | `status == failed` | attention | F | "last run failed" |
| 6 | `status == cancelled` | attention | F | "last run cancelled" |
| 7 | `status == pending_first_run` | working | F | "first run pending" |
| 8 | `status == ok` | current | F | "" |
| 9 | `status == idle` | current | F | "" (gated — prerequisite not met) |
| — | fallback (unreachable) | attention | F | "unknown state" |

**Row 2 headline** (verdict is `attention` regardless; only the reason text differs, picked by usefulness):
- `status == failed` → "last run failed"
- `status == running` and `mid_flight_stuck` → "running but no progress"
- else → label of the first reason in fixed order (`schedule_missed` → `watermark_gap` → `queue_stuck` → `mid_flight_stuck`).

**Reachability (not all 9×16 combos occur — mapping is total/defensive anyway):** `stale_detection.compute` makes `schedule_missed`/`watermark_gap` impossible while `running`; `mid_flight_stuck` only while `running`; bootstrap emits only `queue_stuck`/`mid_flight_stuck`; ingest sweeps emit only `disabled/running/failed/ok` + `watermark_gap`. The table is keyed on status with the stale-set checked as a whole, so unreachable combos cost nothing and the fallback guards drift.

Notes on judgment cells (Codex ckpt-1 confirmed sound):

- **disabled (kill switch)** → `attention`. Global kill-switch is deliberate, but per-row it is the honest "this is not running and won't until you act" signal. #1513 header dedupes it into one banner; the per-row verdict stays honest. *(Alternative: a 5th neutral `paused` verdict — rejected to keep the agreed 4-bucket model.)*
- **cancelled** → `attention`. A cancelled run left no fresh data and nothing is re-running it; operator chose to cancel, so surfacing it is honest, not noise.
- **pending_first_run** → `working` (not `current`: it has no data yet; not `attention`: it is expected to fire at its first slot). T5 look-through will flip bootstrap-covered rows whose source watermark is fresh → `current`.
- **idle** (last terminal = `skipped`/gated) → `current` when no actionable stale. The contradictory `idle`+`schedule_missed` combo is resolved by row 6 (→ attention) *before* row 10 is reached. This is the catch-up-trap surface; #1511 fixes the underlying stuck-ness, after which the row stops being `schedule_missed`.

Contradiction-free **by construction**: every row matches exactly one precedence row → one verdict, one reason.

### 2.3 Remove dead `stale` literal

`ProcessStatus` includes `"stale"` which is **never set** by any adapter (verified: grep shows no `status="stale"` / `return "stale"` assignment). Remove from the `Literal` in `app/services/processes/__init__.py`, the API `ProcessStatus` mirror, FE `STATUS_VISUAL`/`STATUS_SORT_PRIORITY`, and the generated `@/api/types`. Guarded by a grep-based assertion in the table-test.

## 3. API

`app/api/processes.py`:
- `ProcessRowResponse` gains `health_verdict: HealthVerdict`, `self_healing: bool`, `verdict_reason: str`.
- `_convert_row` calls `compute_verdict(...)` and sets the three fields.
- `status` + `stale_reasons` stay on the payload (non-breaking; drill-in / tests still read them; FE main row stops rendering them as pills).

## 4. FE

`frontend/src/api/types.ts` (generated mirror — hand-edit to match BE):
- Add `HealthVerdict` type + `health_verdict`/`self_healing`/`verdict_reason` to `ProcessRowResponse`.
- Remove `"stale"` from `ProcessStatus`.

`frontend/src/components/admin/processStatus.ts`:
- Add `VERDICT_VISUAL: Record<HealthVerdict, StatusVisual>` (label + toneClass + pulse). `working`/`self_healing` pulse; `current`/`attention` static.
- Add `VERDICT_SORT_PRIORITY: Record<HealthVerdict, number>` (`attention` 0 → `self_healing` 1 → `working` 2 → `current` 3).
- Remove dead `stale` from `STATUS_VISUAL`; **delete `STATUS_SORT_PRIORITY`** (replaced by verdict sort — its only consumer is `compareRows`). Keep `STATUS_VISUAL` for the drill-in page if it still renders raw status; otherwise prune.

`frontend/src/components/admin/ProcessRow.tsx`:
- `StatusPill` renders `VERDICT_VISUAL[row.health_verdict]` instead of `STATUS_VISUAL[row.status]`.
- Replace the `StaleChips` cluster with **one inline `verdict_reason` line** when non-empty (folds #1230 — reason inline, not hover-only). Keep `triggerError`/`cancelError` "trigger rejected" lines but render the reason *category* inline too (#1230 scope).
- `pulseBorder` keyed off verdict (`self_healing`/`attention`→amber, `working`→sky, else transparent) instead of `stale_reasons.length`.
- `processRowSignature` serialises the whole row → picks up new fields automatically; keep the `mid_flight_stuck` elapsed term off `active_run`.

`frontend/src/components/admin/ProcessesTable.tsx` (Codex ckpt-1 pt 5 — sequencing):
- `compareRows` → sort by `VERDICT_SORT_PRIORITY[row.health_verdict]` (drops the `STATUS_SORT_PRIORITY.stale ?? status` two-step that depended on the removed `stale` literal). Tiebreak by `next_fire_at` then `display_name` unchanged.

`frontend/src/components/admin/StaleBanner.tsx` (Codex ckpt-1 pt 6):
- Rewire off `health_verdict` instead of `stale_reasons`: count `attention` (and, separately, `self_healing`) rows. Banner copy "N need attention · M self-healing", links to the first attention row. Keeps the existing render-nothing-when-clean behaviour. **#1513 expands this into the positive "All systems current" clean-bill header**; #1512 only makes it consistent with the single-axis model (no "stale" language that no longer matches the row pills).

## 5. Tests

- **BE table-test** `tests/services/processes/test_health_verdict.py`: exhaustive over `ProcessStatus × powerset-subset(StaleReason)` asserting (a) exactly one verdict, (b) the mapping table above, (c) no input yields a contradiction, (d) grep-guard that `"stale"` literal is gone.
- **FE unit** `ProcessRow.test.tsx`: verdict pill renders, inline reason shows, no double-axis pills, sort order.
- Adapter tests unchanged (enums retained).

## 6. Settled-decision compliance

- Two-axis (control-hub §A1): superseded by computed verdict; underlying enums retained → adapters/tests minimal change.
- None-vs-skipped (prevention 249): `pending_first_run` stays a distinct verdict input (row 8), not folded into attention.
- No new universal-gate carve-out (this ticket adds no dispatch path).

## 7. Out of scope

`next_retry_at`-driven self-healing reclassification (#1509/#1510), watermark look-through + post-bootstrap seed (#1511), clean-bill header (#1513). v1 is the contradiction-killing computed layer only.
