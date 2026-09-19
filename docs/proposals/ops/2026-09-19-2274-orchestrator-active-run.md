# #2274 — the orchestrator wrappers get an active run, a status and a Cancel

Status: proposal (2026-09-19), rev 2 after Codex ckpt-1
Predecessors: `2026-09-19-2274-cancel-on-halted-wedge.md` (`5a3045b3`, which moved
`can_cancel` off `process_status` onto the active run), `2026-09-15-2274-sync-run-heartbeat.md`
(`run_liveness.py`), `#1474` Part 2 (`_resolve_terminal_row`).

This slice has now been specced twice and refuted twice at ckpt-1. Rev 1's design is in git
history and on #2274; **four of its decisions are reversed here** and §"What ckpt-1 killed"
says which, because two of them were reversed on measurements that contradict figures
already written into #2274.

## The defect

`scheduled_adapter._resolve_terminal_row` re-homes the **terminal** row of both
orchestrator wrapper jobs to `sync_runs` (`_ORCHESTRATOR_SYNC_SCOPE`, #1474 Part 2). Its
docstring records the **active** read as a deliberate deferral:

> *"an in-flight high-frequency sync writes a `sync_runs` 'running' row, not a `job_runs`
> one, so during the **(seconds-long, every-5-min)** active window the row shows the last
> terminal run instead of running/progress."*

Measured and correct — **for `high_frequency`**. It was written about one member of a
two-member dict and silently applies to the other, whose active window is p50 36.7 minutes
and max 4.6 hours.

So `orchestrator_full_sync` renders **no active run, no status and no Cancel for its entire
runtime**, while `_cancel_orchestrator_full_sync` sits behind it: a `sync_runs`-targeted
cooperative cancel the executor genuinely honours (`_check_cancel_signal`, checkpoints at
`executor.py:482` and `:608`, keyed on `sync_run_id` and therefore scope-agnostic) with a
passing endpoint test since PR6. It is one of only three consumers of `is_stop_requested` in
the repo. **The one scheduled process whose cancel actually works is the one the page cannot
offer it for.**

## Source rule / premise verification

No external source rule applies — this is our own two-table model, so the corpus is the
rule. Every figure is from the dev DB on 2026-09-19 with its query and window stated.

### The active read is structurally dead, not intermittently empty

Run with NO window, so the answer cannot depend on one:

```sql
SELECT job_name, status, count(*), min(started_at)::date, max(started_at)::date
  FROM job_runs
 WHERE job_name IN ('orchestrator_high_frequency_sync','orchestrator_full_sync')
 GROUP BY 1,2;
```

→ `orchestrator_high_frequency_sync` **1,578 `skipped`** (2026-06-03 → 2026-09-18),
`orchestrator_full_sync` **4 `skipped`** (2026-07-17 → 2026-08-09). **1,582 rows over the
whole corpus and every one is `skipped`.** No `running`, no `success`, no `failure`, ever.
So `_read_running_run` — `WHERE job_name = … AND status = 'running'` — cannot return a row
for either job, and `_status_for(has_running_row=False)` can never read `running` for them.

⚠ This corrects the previous session's framing twice. Its first note said "the wrapper
writes a zero-duration `job_runs` row"; its refutation corrected that to "suppression
records", measured as 490 of 492 on a 30-day window. That 2-row residue is **not
reproducible** — the same 30-day query today returns 488 rows, all `skipped` — and the
unwindowed query above makes the question moot. Check what a row RECORDS before reading it
as a run, and when a ratio is meant to say "never", drop the window rather than widen it.

### What the operator gains — and what they do NOT

⚠⚠ **Rev 1 claimed this buys a "Processed: N" ticker. That was false on both halves and
ckpt-1 caught it.** Measured:

```sql
SELECT scope, count(*), count(*) FILTER (WHERE processed_count > 0), max(processed_count)
  FROM sync_runs GROUP BY 1;
```

→ **0 of 23,603 rows across every scope have `processed_count > 0`; the max is 0.** The
column has no production writer (the executor's progress path updates `sync_layer_progress`
and advances only `last_progress_at` on the parent). `_build_active_run` maps
`processed > 0 ? processed : None`, so it would publish `None` for every sync run, forever.

And there is no renderer either: `rg` over `frontend/src` for `rows_processed_so_far`,
`progress_units_done`, `progress_units_total` and `is_cancelling` returns **nothing outside
`types.ts`, `__fixtures__/` and `.test.tsx`**. No production FE surface reads them.

So the honest gain list is exactly four things:

1. the status pill reads **Working** instead of the last terminal status, for a window whose
   p50 is 36.7 minutes;
2. a **Cancel button** appears on the one scheduled process whose cancel is genuinely
   honoured end-to-end;
3. `ProcessDetailPage:679` renders the live run's identity and start time (it already reads
   `row.active_run`);
4. `can_iterate` / `can_full_wash` correctly refuse while the run is in flight.

No progress bar, no ticker. Wiring `processed_count` is a different ticket with a different
prerequisite (a producer), and this spec does not pretend otherwise.

### Rule 4 exposure — measured on HEALTHY runs only

`stale_detection` rule 4 (`mid_flight_stuck`) fires when
`COALESCE(last_progress_at, started_at)` is older than `get_threshold(process_id)`, which
for both wrappers is the **300 s default** (`stale_thresholds._OVERRIDES` keys neither).

⚠ Rev 1 measured this over `finished_at IS NOT NULL`, which folds failed and cancelled runs
into a claim about false alarms on healthy ones. ckpt-1 was right; restricted to
`status='complete'`:

```sql
SELECT scope, status, count(*) n,
       count(*) FILTER (WHERE extract(epoch FROM (finished_at - coalesce(last_progress_at, started_at))) > 300) trip300
  FROM sync_runs WHERE finished_at IS NOT NULL GROUP BY 1,2;
```

| scope | completed runs | would have fired `mid_flight_stuck` |
| --- | ---: | ---: |
| `full` | 52 | **45 (86.5%)** |
| `behind` | 1,167 | 72 (6.2%) |
| `high_frequency` | 21,924 | **0** |

The conclusion survives the correction: feeding a `full` sync's active row into rule 4 at
the 300 s default red-flags **45 of 52 healthy full syncs**.

⚠ The measure is final-heartbeat-to-finish, not maximum inter-heartbeat silence — ckpt-1 is
right that those differ. They coincide here for the case that matters: `last_progress_at` is
non-null on only **4 of 63** full runs, so for 59 of them the heartbeat IS `started_at` and
rule 4 degenerates into a duration rule. That is why the number is this large.

### ⚠⚠ …and why the exemption is scoped to ONE job, not to the kind

Rev 1 exempted rule 4 for every `sync_run` active row. **ckpt-1 killed that, and the table
above is the reason the fix is narrower rather than the reason it is fine.** On
`high_frequency`, rule 4 costs **0 false alarms in 21,924 completed runs** — and it is
worth real signal, because once the HF row can read `running`, rule 1 (`schedule_missed`) is
suppressed for it. Today a stranded HF singleton surfaces as `schedule_missed`; a blanket
kind-based exemption would have replaced that with **nothing until rule 5's 24-hour
ceiling**. Keeping rule 4 live at 300 s is what covers the case instead, and ckpt-1
confirmed on the verdict path that a silent stranded HF run does still reach Attention.

⚠ **Not "strictly better" in every case, and the exception is worth naming.** Two
corrections from ckpt-1 rev 2:

- Rule 1's clock is the next scheduled fire *after the terminal anchor* plus
  `max(cadence, floor)` — for HF both are 300 s — not terminal age alone. Rule 4 fires
  after 300 s of SILENCE, not 300 s after start. So the two rules are close in timing but
  not identical, and neither is a strict refinement of the other.
- A run that is **slow but still heartbeating** clears rule 4 while rule 1 stays
  suppressed, where today's `schedule_missed` would eventually fire. That is a genuine
  narrowing on one case, and it is the correct one to accept: a ticking run is working,
  and rule 5's ceiling still bounds it. ⚠ It does mean this spec's "no heartbeat-staleness
  verdict is added for sync runs" is loose — rule 4 IS heartbeat-aware, and by leaving HF
  on the default we keep it that way for one of the two wrappers, deliberately.

Durations, whole corpus:

| scope | n | p50 | p95 | max |
| --- | ---: | ---: | ---: | ---: |
| `high_frequency` | 22,171 | 1 s | 6 s | 53 s |
| `behind` | 1,369 | 3 s | 1,520 s | 14,119 s |
| `full` | 63 | **2,204 s** | 14,751 s | **16,659 s** (4.6 h) |

300 s is 5.7× HF's all-time maximum, so the default is derivable there. For `full` it is
not: `run_liveness.py`'s docstring settled that no heartbeat threshold is derivable from
what is stored ("*the longest legitimate gap between two run heartbeats is at least the
longest non-ticking layer … #2274's own constraint is that 'a watchdog that fires on
legitimately-long corpus jobs is worse than none'*").

Which makes the fix the **existing** per-process mechanism rather than a new one.
`stale_thresholds._OVERRIDES`'s own docstring: *"Override only when the producer's natural
row-write cadence is slower than `DEFAULT_THRESHOLD_S`."* That is precisely this job. The
value is `RUNTIME_CEILING_S` — the one ceiling already justified on measured evidence and
already imported by `run_liveness` — so rule 4 declines to add a threshold rather than
inventing one.

Rule 5 (`runtime_ceiling`, 86,400 s) is untouched and still fires on these rows. Max
observed sync is 4.6 h against a 24 h ceiling.

⚠ Consequence, stated: for `orchestrator_full_sync` rules 4 and 5 now coincide on the 59 of
63 runs with no heartbeat, so a genuinely stranded full sync chips **both** reasons at 24 h.
Not duplication on the other 4 — there rule 4 measures silence-since-heartbeat, which is a
different and later instant.

## ⚠⚠ This ticket's OWN prior spec both predicted this fix and scoped it differently

`2026-09-15-2274-sync-run-heartbeat.md` §3 lists "Widening
`scheduled_adapter._read_running_run`" under what it is NOT doing, and §1.2 gives the
reason: of the 157 rows ever reaped `orchestrator_crash`, **148 are `boot_sweep`**, whose
`behind` scope is in neither map — *"a scope-keyed ProcessRow read structurally misses 94%
of the population this ticket is about."*

**That rejection stands, and it does not govern this change**, because the two are aimed at
different populations. §1.2 is about STRANDED runs, where the reaped population really is
94% `boot_sweep` and a scope-keyed read really is the wrong instrument. This spec is about
the display and cancel gap during a **healthy** full sync. It claims no coverage of the
stranded-walk population, and §"Known-and-not-fixed" says so.

§3 also anticipated the fix: *"If done later it must land WITH a `mid_flight_stuck`
threshold override for the two orchestrator process_ids, because `DEFAULT_THRESHOLD_S` is
300s and 28 runs exceed an hour."* This lands exactly that override — arrived at
independently, which is some evidence it is the right shape.

⚠ **But it lands on ONE process_id, not two, and the prior spec's own evidence is why.**
Runs exceeding an hour, whole corpus, by scope:

| scope | n | > 3,600 s | > 300 s |
| --- | ---: | ---: | ---: |
| `behind` | 1,369 | 15 | 127 |
| `full` | 63 | 14 | 55 |
| `high_frequency` | 22,179 | **0** | **0** |

The 29 long runs (28 when that spec was written) are `behind` and `full`. **Not one of
22,179 `high_frequency` runs has ever exceeded even 300 s**, so the sentence's justification
never covered the second process_id it prescribed. Overriding HF would have cost the one
alarm that still reaches a stranded HF singleton once rule 1 goes quiet for it.

A recommendation's scope can be wider than the evidence that justifies it, and inheriting
the scope without re-reading the evidence is how that spreads.

## The change

### 1. `RUNTIME_CEILING_S` moves to `stale_thresholds`

`stale_detection` already imports `get_threshold` from `stale_thresholds`, so the override
cannot import the constant back without a cycle. The constant (and its measured-evidence
comment block) moves into `stale_thresholds.py` — the module named for thresholds — and
`stale_detection` re-exports it, keeping `run_liveness`'s existing import path working
unchanged. No value changes.

### 2. One `_OVERRIDES` entry

`_OVERRIDES["orchestrator_full_sync"] = RUNTIME_CEILING_S`. That is the whole rule-4 change:
**no new parameter on `stale_detection.compute`, no kind-based branch, and therefore no
effect on the bootstrap caller** — which ckpt-1 correctly notes passes a real
`active_run_started_at` and for which rule 4 is the only hung-run detector, rule 5 excluding
bootstrap by design.

`tests/test_stale_thresholds.py` already grep-validates every override key against the live
registry, so the new key is covered by an existing gate.

### 3. The active row carries its kind, and the FE renders it

`ActiveRunSummary` gains `run_kind: RunKind`, where
`RunKind = Literal["job_run", "sync_run", "bootstrap_run"]` — the SAME vocabulary
`process_stop_requests.target_run_kind` is CHECK-constrained to (`sql/135:65`), and the same
shape the terminal row already carries as `terminal_kind` (#1508 Task 5).

Required, not defaulted: a new adapter that forgets it is a type error. Construction sites,
enumerated exhaustively (ckpt-1 flagged rev 1's list as production-only):

- `scheduled_adapter._build_active_run` → `job_run`, or `sync_run` for the wrappers;
- `bootstrap_adapter._build_active_run` → `bootstrap_run`;
- `app/api/processes.py::ActiveRunSummaryResponse` + `_convert_active_run`;
- `frontend/src/api/types.ts::ActiveRunSummaryResponse`;
- tests: `tests/test_processes_envelope.py` (incl. its exact-slot assertion),
  `frontend/src/components/admin/ProcessRow.test.tsx`,
  `frontend/src/components/admin/a11y.test.tsx`,
  `frontend/src/components/admin/__fixtures__/processes.ts`.

⚠ It gets a **reader in the same PR**: `ProcessDetailPage:682` renders `run #{run_id}`, which
for a wrapper is a `sync_run_id` sitting in the same visual slot as a `job_runs.run_id`. It
becomes `sync run #{run_id}` / `run #{run_id}` off `run_kind`. Shipping the field without the
reader would be the write-with-no-reader shape #3111 exists to stop.

### 4. The active read re-homes for the two wrappers only

New `_read_running_sync_run(conn, *, scope)` returning the dict shape `_read_running_run`
returns, from `sync_runs WHERE scope = %(scope)s AND status = 'running'`.
`_resolve_active_row(conn, *, job_name)` dispatches on `_ORCHESTRATOR_SYNC_SCOPE.get(job_name)`
exactly as `_resolve_terminal_row` does, and **both** `list_rows` and `get_row` call it, so
the list page and the detail page cannot disagree.

**Scope-filtered, deliberately.** Each wrapper row shows its OWN scope's run. A `behind` walk
has no ProcessRow of its own and so appears on neither — which is what happens today, and
rendering one as "the full sync is running" would be a lie.

### 5. Cancel routed by dict membership, with `process_id` passed through

`cancel_process` routes on `process_id in _ORCHESTRATOR_SYNC_SCOPE` instead of
`== JOB_ORCHESTRATOR_FULL_SYNC`, and `_cancel_orchestrator_full_sync` (renamed
`_cancel_orchestrator_sync`) takes `process_id` rather than hardcoding
`JOB_ORCHESTRATOR_FULL_SYNC` in its `request_stop` call. Without this, widening `can_cancel`
on the HF row offers a button whose POST falls through to the `job_runs` path and 409s — and
if it did not, the stop would be **audited against the full sync**.

The executor honours either scope already: `_check_cancel_signal` is keyed on `sync_run_id`.

### 6. ⚠⚠ The cancel target is pinned by the caller — this is a correctness prerequisite, not a nicety

`CancelRequest` gains `target_run_id: int | None = None`. Both scheduled resolvers
(`_resolve_active_sync_run`, `_resolve_active_job_run`) take it; when supplied and the locked
row's id does not match, the handler raises `409 run_changed` instead of cancelling. The FE
sends `row.active_run.run_id` from the row the operator was looking at.

**Why this rides this diff rather than a follow-up.** Rev 1 wrote the display/resolver
mismatch off as a render-to-click race bounded by the poll interval. ckpt-1 checked the FE
and it is not bounded by anything: `ProcessesTable` captures `cancelTarget` as a row object
and holds it for the entire life of the confirm dialog (`ProcessesTable.tsx:485` →
`handleCancelConfirmed(cancelTarget, mode)`), posting only `{mode}`. An operator who opens
the dialog and walks away cancels **whatever is running when they come back** — and with the
singleton that can be a different scope, a `behind`/`layer`/`job` manual walk, or a second
full sync. Today that is harmless because the button does not exist for these rows. This
spec creates the button, so it has to create the guard in the same change.

Optional, so the unscoped direct-POST escape path for a stranded non-wrapper walk is
preserved byte-for-byte: `_resolve_active_sync_run` stays **unscoped** when no id is pinned
(rev 1 refutation point 4 — a scope filter would leave a stranded `behind` walk with no
cancel route at all).

## Consequences — including the narrowing

Per CLAUDE.md's narrowing-gate rule, what this REJECTS:

0. ⚠ **A correction to this spec's own framing, from ckpt-1 rev 2.** "`has_running_row=
   False` can never yield `running`" is too strong: `_status_for` also returns `running`
   when a manual retry is in flight against a failed terminal. What is unreachable for
   these rows is the ACTIVE-RUN path specifically — the telemetry block, the heartbeat
   suffix and `can_cancel` — which is the defect either way.

1. **`can_iterate` / `can_full_wash` go False on a wrapper row while its own scope is
   running.** Both are gated on `process_status != "running"`, which these rows can now
   reach. ⚠ ckpt-1 is right that this is not strictly redundant with
   `idx_sync_runs_single_running` (`sql/033:34`): the API publishes a `pending_job_requests`
   row and admission happens later, so a request queued mid-sync could legitimately execute
   after it. The narrowing is nonetheless correct because it is the **same rule every other
   scheduled job already lives under** — no row in the table offers Iterate while it is
   running. It rejects exactly one thing: queueing an Iterate/Full-wash of a wrapper *during
   that wrapper's own run*, which the operator can do once it finishes.
2. **Rules 1 and 2 (`schedule_missed`, `watermark_gap`) are suppressed during the active
   window** — with the kill switch OFF. ⚠ Under the switch the status is `disabled`, so
   their `status != "running"` gates stay open and the verdict layer demotes the reasons
   instead. Both being gated on `status != "running"`, Correct — a job that is running is
   not missing its schedule — and the HF signal loss this would otherwise cause is exactly
   what §"scoped to ONE job" keeps rule 4 alive to cover.
3. **Under the kill switch the status reads `disabled`, not `running`, and `can_cancel`
   stays True.** `5a3045b3`'s behaviour, inherited unchanged.
4. **Full-wash on a wrapper stays a no-reset rerun.** `watermarks.py` resolves
   `watermark=None` for `orchestrator_full_sync`.

## Known-and-not-fixed, stated rather than left to be found

All five are pre-existing, none is made worse here, and each was surfaced by ckpt-1:

- **`is_cancelling` has no PROACTIVE renderer.** A second Cancel click does explain
  itself — both surfaces map `stop_already_pending` to "cancel already pending" — but
  nothing marks the row as cancelling BEFORE the operator clicks again. Pre-existing for
  every mechanism. (ckpt-1 rev 2 corrected this spec's first, stronger claim.)
- **Cancel latency is checkpoint-bounded.** Observation is between layers; a wedged layer
  keeps running. That is the settled cooperative-cancel contract (`settled-decisions.md`
  §"Cancel UX (#1064)"), not a gap.
- **The drill-in DAG tab reads the latest `sync_runs` row across every scope**, and the
  History tab still reads `job_runs` — which for these jobs contains suppression records
  (`skipped`), not sync history. So the newly-exposed active run has no matching history
  surface.
- **The detail page renders `STATUS_VISUAL[row.status]` raw**, ignoring `health_verdict`
  and `stale_reasons`. A wrapper past the ceiling therefore reads "running" THERE while the
  table shows Attention. Pre-existing for every row; worth naming because this change is
  what first puts a wrapper into `running` at all.
- **Planning is invisible**: `_start_sync_run` builds the execution plan before inserting the
  row, so a slow prelude still shows no active run. `run_liveness`'s docstring already
  records this.
- **`ProcessDetailPage` does not poll**, so a detail page left open across completion keeps
  stale action flags.

## What ckpt-1 killed (rev 1 → rev 2)

1. the "Processed: N ticker" gain — **no writer and no renderer**, both measured above;
2. the rule-4 exemption **by kind** — replaced by one `_OVERRIDES` entry, because a blanket
   exemption removes the stranded-HF alarm and the HF false-positive count is 0;
3. the "false alarms on healthy runs" figure — re-measured on `status='complete'` only
   (87.3% → **86.5%**, conclusion unchanged);
4. treating the display/resolver mismatch as an acceptable race — it is unbounded, so
   `target_run_id` pinning became part of this change.

Carried unchanged from rev 1's refutation, all still verified: `_has_active_job_run` is not
touched; the cancel resolver is not scope-filtered by default; no heartbeat-staleness verdict
is added for sync runs; `behind` gets no ProcessRow.

## Tests — as shipped

1. `tests/test_stale_thresholds.py` — `get_threshold("orchestrator_full_sync")` is
   `RUNTIME_CEILING_S` (pinned against the constant, never against `86_400`), and
   `orchestrator_high_frequency_sync` is **not** overridden. The second is the one that
   matters: it pins the asymmetry the measurement bought, against the obvious "exempt both,
   they read the same table" edit.
2. `tests/test_stale_detection.py` — a full sync ten default-thresholds old yields NO
   reasons; the same age past the ceiling yields both `runtime_ceiling` and
   `mid_flight_stuck` (the exemption is a raised threshold, not a removed rule); the HF
   wrapper still chips at the default; and **bootstrap still fires rule 4 at its own 1,800 s
   override** — the regression guard for the `active_run_kind` design this ticket did not
   ship, rule 4 being bootstrap's only hung-run detector.
3. `tests/test_scheduled_adapter_orchestrator.py` — `_resolve_active_row` dispatches to the
   sync reader for every member of the registry and to `_read_running_run` for a
   non-member, asserted against the registry rather than against name literals.
4. `tests/test_processes_envelope.py` — `run_kind` is in `ActiveRunSummary.__slots__`
   (the exact-slot assertion).
5. `tests/test_processes_endpoints.py` (db tier), four:
   - a running `scope='full'` row two hours old makes `orchestrator_full_sync` read
     `status='running'`, `can_cancel=True`, `can_iterate=False`, `can_full_wash=False`,
     `active_run.run_kind == "sync_run"`, and **no** `mid_flight_stuck`;
   - cancel on `orchestrator_high_frequency_sync` writes a stop row whose `process_id` is
     the HF job — pre-fix this POST fell through to the `job_runs` branch entirely;
   - a stale `target_run_id` returns `409 run_changed` and writes **no** stop row, leaving
     `cancel_requested_at` NULL;
   - omitting `target_run_id` still cancels the running row, including a `behind` walk that
     has no ProcessRow to read an id from.
6. `frontend/src/pages/ProcessDetailPage.test.tsx` — the cancel POST carries
   `target_run_id`, and a `sync_run` renders as `sync run #418` rather than `run #418`.

⚠ Not covered, and stated rather than implied: the cross-wrapper duplicate-stop case
(`stop_already_pending` reached through the other wrapper's endpoint), and the table's
cancel path, which shares `cancelProcess` with the detail page but has its own handler.
