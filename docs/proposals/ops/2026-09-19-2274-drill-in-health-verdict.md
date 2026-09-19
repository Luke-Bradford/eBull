# #2274 — the process drill-in states the same health claim as the table it was reached from

**Status:** proposal · 2026-09-19 · branch `fix/2274-drill-in-health-verdict`
**Rev 2** after Codex ckpt-1 (26 findings; 11 changed this document — see §Ckpt-1).

## The defect

`frontend/src/pages/ProcessDetailPage.tsx:667` renders

```ts
const visual = STATUS_VISUAL[row.status];
```

as the drill-in's one and only "Status" pill. It never reads `health_verdict`,
`verdict_reason` or `stale_reasons`, all three of which are on the same
`ProcessRowResponse` it already holds.

The table renders a different field. `ProcessRow.tsx::StatusPill` uses
`VERDICT_VISUAL[row.health_verdict]`, with the comment *"#1512 — render the
single computed verdict, not the raw status."* So the operator's path is:

1. the Processes table pins a row red with `needs attention` plus a reason line;
2. they click it — that is what the red is for;
3. the drill-in says `idle`, grey, and gives no reason anywhere on the page.

`api/types.ts:2259` assigns `status` + `stale_reasons` to the drill-in:

> `#1512` — single computed health verdict + inline reason. The main row renders
> `health_verdict`; `status` + `stale_reasons` stay on the payload **for the
> drill-in**.

⚠ That comment assigns the *fields* to the drill-in. It does **not** say the
drill-in's headline must be the verdict — making the headline the verdict is a
design decision taken here, not a contract being enforced (ckpt-1 #1). What the
comment does establish is that `stale_reasons` was put on the wire for a
consumer that never rendered it: `STALE_REASON_LABEL` (`processStatus.ts:210`)
is exported, unit-tested at `processStatus.test.ts:100`, and has **no production
renderer anywhere in `frontend/src`**. (`stale_reasons` itself is read by
`ProcessRow::hasHeartbeatSuffix` — it is the *labels* that are unrendered, not
the field — ckpt-1 #2.)

## Measurement

**Procedure, stated so it can be re-run.** The verdict is not on `ProcessRow`;
it is computed by `verdict_for_row(row, now=...)`, which is the shared choke
point `app/api/processes.py::_convert_row:513` itself calls. So the script below
is the production path, not a reimplementation of `compute_verdict` with fewer
inputs (ckpt-1 #4):

```python
from app.api.processes import _gather_snapshot
from app.services.processes.health_verdict import verdict_for_row
snap = _gather_snapshot(conn)                       # 72 rows, partial=False
for r in snap.rows:
    verdict, self_healing, reason = verdict_for_row(r, now=now)
    # compare STATUS_VISUAL[r.status] (drill-in) vs VERDICT_VISUAL[verdict] (table)
```

Run against the dev DB 2026-09-19 ~05:30Z. **72 rows, `partial=False`.**

⚠ `partial=False` means no adapter raised, **not** that every job in the system
is represented — a registry the adapters do not cover contributes no row
(ckpt-1 #5). The denominator is "all 72 rows `/system/processes` returned",
which is the population the two surfaces actually render.

| disagreement | rows / 72 |
| --- | ---: |
| **pill LABEL differs** (`ok` vs `current`, `idle` vs `current`, …) | **72 (100%)** |
| pill TONE differs | 10 (13.9%) |
| tone differs **across the calm/alarm boundary** | 2 (2.8%) |

The label disagreement is total: there is no row on which the two surfaces
print the same word. The tone breakdown:

| `status` → `health_verdict` | rows | drill-in tone | table tone |
| --- | ---: | --- | --- |
| `ok` → `current` | 62 | `ok` | `ok` |
| `idle` → `current` | 8 | `neutral` | `ok` |
| `idle` → `attention` | 2 | `neutral` | **`risk`** |

The two cross-boundary rows are `core_rebalance_observation`
(`role='steady_state'`) and `sec_business_summary_bootstrap`
(`role='bootstrap'`), both `stale_reasons = ('schedule_missed',)`,
`verdict_reason = "schedule missed"`.

⚠ Only **one** of those is in the attention-pinned steady-state table.
`sec_business_summary_bootstrap` is `role='bootstrap'`, which
`processHealth.ts::isSteadyStateProcess` excludes and `ProcessesTable` folds
into the collapsed Bootstrap & backfill section (ckpt-1 #8). Both are reachable
at `/admin/processes/{id}` and both drill-ins are wrong; the *pinned-red-then-
grey* journey applies to one of them today.

⚠ A tone match is not semantic agreement and a tone mismatch is not proof of a
contradiction — `idle` and `current` describe different axes and can both be
true (ckpt-1 #9, #10). The claim here is narrower and is the one the fix rests
on: **two surfaces print different health words for the same row, and on 2 of 72
they print them in different alarm colours.** No claim is made about the
steady-state rate of that; this is one snapshot, and the adapters each call
`datetime.now()` independently so even "one instant" is approximate (ckpt-1 #7).

## Source rule

Not a data-treatment decision — no external reg governs it. The repo's own rules:

- `processStatus.ts:5` — *"Single source of truth for the FE — both ProcessRow
  and ProcessDetailPage import from here so the operator sees the same copy
  regardless of surface."* Both import the module and then import **different
  maps from it**, which that docstring does not cover.
- `processHealth.ts:1` (#1959) — two attention surfaces derived their answer
  independently and drifted, so the *predicate* was centralised. ⚠ #1959
  centralised selectors over a shared payload, not components; it is precedent
  for "do not let two surfaces derive it separately", not a mandate to extract a
  pill (ckpt-1 #3).
- `health_verdict.py::verdict_for_row` — the single producer, already shared by
  `/system/processes` and the legacy `/system/jobs`. Untouched here.

## Design

**1. The verdict pill moves to the page HEADER, beside `display_name`.**
Not inside `OverviewTab`. The route has six tabs and a health headline that
disappears when the operator opens History or Errors is a headline in name only;
`ProblemsPanel`, `StaleBanner` and the bootstrap timeline all deep-link here
(ckpt-1 #25). Header placement makes the claim tab-independent.

**2. Extract `ProcessRow`'s private `StatusPill` to
`components/admin/VerdictPill.tsx`; both surfaces render it.** The extraction
does not fix the bug — switching the field does (ckpt-1 #13). It is here so the
`self_healing` tooltip, the `aria-label="Health: …"` and the `data-verdict`
attribute exist once rather than twice.
⚠ `AdminPage.tsx::JobStatusCell` renders `VERDICT_VISUAL` with its own markup as
a third site and is **not** converted (ckpt-1 #24). Out of scope; naming it so
the extraction is not read as having deduplicated everything.
⚠ The extraction carries `PENDING_RETRY_TOOLTIP` (*"hiding prior errors during
retry…"*) to a second surface. That copy is already an overclaim — the adapter
suppresses prior errors for `running`/`pending_retry` only, while `self_healing`
can also arise from a watchdog re-enqueue with no prior error (ckpt-1 #20).
**Pre-existing and deliberately not corrected here**: changing it would be a copy
change riding a structural fix, and it is the same on both surfaces either way.

**3. `status` stays on the Overview tab, demoted and untoned.** It is real
information — the adapter-normalised process state the verdict was computed
*from*, including the kill switch's `disabled` masking; it is not a stored run
status (ckpt-1 #12). It renders as a `KeyValueRow` labelled `Process state`
carrying `STATUS_VISUAL[row.status].label` — the human copy, so that map keeps a
production consumer and the two surfaces still share their wording. No tone: a
second toned pill beside the verdict re-creates the two-cells-that-disagree
defect `RecentReaps`' docstring already names.

**4. `stale_reasons` get chips — MUTED, and framed as diagnostics, not alarms.**
`STALE_REASON_LABEL` finally gets its renderer, rendering **every** reason in
payload order.
⚠⚠ The chips must not re-alarm what the backend deliberately calmed
(ckpt-1 #17). `compute_verdict:217-219` returns neutral `paused` for a halted row
that still carries `schedule_missed` / `watermark_gap`, and `self_healing`
suppresses a reason whose retry is in flight. So the chips are slate/lowercase
with no `Badge` tone, under a caption naming them as reported-not-adjudicated.
The verdict pill remains the only toned health claim on the page.
⚠ Chip order is the payload's own array order. It is **not** derived from
`_WEDGE_HEADLINE_ORDER`, which applies only inside `compute_verdict`'s
`status == "disabled"` branch; the general headline pick uses `_REASON_ORDER`
(ckpt-1 #18). The point the chips serve stands either way: several reasons can
fire and `verdict_reason` shows one.
⚠ Chips are **static** — no elapsed suffix on any of them, including a
simultaneous `mid_flight_stuck` + `runtime_ceiling` (ckpt-1 #19). See below.

### Explicitly NOT in scope, with the reason

- **No elapsed-since-heartbeat suffix on this page.** `ProcessRow`'s
  `VerdictReason` computes `formatElapsedSince` **at render**; there is no timer
  in that component. It advances only because the table polls and
  `processRowSignature` folds the elapsed string in, forcing a repaint
  (ckpt-1 #15 — my rev-1 rationale, "a client clock ticking over frozen data",
  was wrong in mechanism). `ProcessDetailPage`'s envelope is
  `useAsync(() => fetchProcess(id), [id])` at line 131 with **no interval** — it
  refetches on actions, retry and route change, but never periodically. So the
  suffix would render once at fetch time and freeze, presenting a duration
  measured at page load as a live one. Omitting it is the honest option until
  the page polls.
  ⚠ `docs/proposals/ui/admin-control-hub-rewrite.md:90` prescribes a drill-in
  poll (1.5 s while running, 30 s otherwise) that was never implemented. If that
  lands, share `hasHeartbeatSuffix` the same way `StatusPill` is shared here.
  Named, not built.
- **No backend change.** `compute_verdict`, the adapters and the response model
  are untouched. A read-path display fix over a payload that already carries
  every field.
- **No change to the table.** `ProcessRow` renders `<VerdictPill row={row} />`
  before and after; only the import path moves.
- ⚠ **Shared rendering cannot guarantee the two surfaces ever show the same
  verdict simultaneously** (ckpt-1 #14). List and detail fetch independently and
  the detail does not poll, so a row can change between the click and the read.
  What this change guarantees is that **given the same payload they make the same
  claim** — which is the defect measured above. Stated rather than blurred.

## Acceptance

1. `health_verdict = 'attention'` renders `needs attention` (risk tone) in the
   drill-in header on **every** tab, not the raw status.
2. All six verdicts render their `VERDICT_VISUAL` label/tone in the header —
   table-driven, not one fixture (ckpt-1 #21).
3. Table and drill-in render the **same component**: one fixture row, both
   surfaces, equal label + equal `data-verdict` + equal `aria-label`.
4. A row whose `status` deliberately diverges from its `health_verdict`
   (`status='idle'`, `health_verdict='attention'`) shows the verdict in the
   header **and** `idle` as `Process state` — both, not one.
5. Every `stale_reason` renders a chip, in payload order, with no `Badge` tone;
   a row with `stale_reasons: []` renders no chip container.
6. A `paused` row carrying `schedule_missed` renders the neutral `paused` pill
   and the chip — the chip does not repaint the row as an alarm.
7. `verdict_reason` renders when non-empty, nothing when empty.
8. ⚠ Tests must set `health_verdict` / `verdict_reason` / `stale_reasons`
   **explicitly**. `__fixtures__/processes.ts:129` auto-derives the verdict via
   its own `deriveVerdict`, which is not the backend's function and already
   differs from it on disabled-row precedence (ckpt-1 #22).
9. `ProcessRow.test.tsx` stays green. ⚠ Green existing tests do not prove the
   extraction was behaviour-preserving (ckpt-1 #23) — hence 3, which compares
   the two surfaces directly.

## Risk

Frontend-only, one already-fetched payload, no new request, no new state. Blast
radius is the two files that render the pill plus the new component.

## Ckpt-1 (Codex, 26 findings)

Changed this document: #1 (contract overclaim → design decision, stated), #2
(`stale_reasons` has a consumer; the *labels* do not), #3 (#1959 analogy
softened), #4 (measurement procedure written out), #5 (denominator restated), #7
(`now()` per adapter), #8 (`role='bootstrap'` → 1 pinned row, not 2), #9/#10
(label metric added, 72/72; tone claim narrowed), #12 (`status` is
adapter-normalised, not stored), #13 (extraction is anti-drift, not the fix),
#14 (simultaneity explicitly not claimed), #15 (suffix rationale corrected —
no independent ticker), #17 (chips untoned so backend suppression survives),
#18 (`_REASON_ORDER`, not `_WEDGE_HEADLINE_ORDER`), #19 (chips static), #20
(tooltip overclaim named, not fixed), #21/#22/#23 (acceptance broadened), #24
(`JobStatusCell` named as unconverted), #25 (pill moved to the header), #26
(incidental finding re-measured — below).

Not adopted: #6 (asks for a committed measurement artefact — the procedure above
is reproducible in four lines against a live DB, and a checked-in snapshot of a
moving population is the hardcoded-statistic trap CLAUDE.md bars), #11 (agrees
with the design — status is kept), #16 (about a suffix this change does not add).

## Incidental finding (noted, NOT fixed here)

`core_rebalance_observation` — the job whose red row motivated this ticket — has
**never completed a run**. No window; the whole table:

```sql
SELECT status, count(*), min(started_at)::date, max(started_at)::date
  FROM job_runs WHERE job_name = 'core_rebalance_observation' GROUP BY 1;
```

→ **`skipped` 12** and **`failure` 6**, 2026-08-22 → 2026-09-18. **Zero
`success`, zero `partial`.** All 6 failures are
`orphaned: reaped at boot (owning worker thread died without a terminal status)`;
the last 4 skips are `lane_busy: lane stayed busy through the ~11.50s retry
window`, and the one before those is `prereq_missing: no core mandate
configured`.

That is a capital-path defect, not a display one. This change does not fix it and
does not surface its cause — it surfaces `schedule missed`, which is the verdict,
not the mechanism (ckpt-1 #26). Recorded on the PR.

Refs #2274. Refs #1512. Refs #1959. Refs #2437.
