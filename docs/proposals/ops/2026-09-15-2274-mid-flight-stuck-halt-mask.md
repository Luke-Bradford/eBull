# #2274 — `mid_flight_stuck` is masked by the kill switch, so #1689's hung-job answer has been dead for 79 days

Status: proposal, revision 2. Issue #2274 item 1 (the no-progress watchdog).
Predecessors: `2026-09-15-2274-job-runtime-ceiling.md` (rule 5),
`2026-09-15-2274-job-heartbeat-writer.md` (the producer).

⚠ **Revision 1 proposed dropping rule 4's `started_at` fallback. That is
refuted and is NOT in this spec** — see §7. Codex checkpoint 1 killed it on
three independent grounds, one of which is a settled-decision reversal.

## 1. The defect

`stale_detection.compute` rule 4 gates on `status == "running"`:

```python
if status == "running":
    threshold_s = get_threshold(process_id)
    heartbeat = last_progress_at or active_run_started_at
    if heartbeat is not None and heartbeat < now - _seconds(threshold_s):
        reasons.append("mid_flight_stuck")
```

`scheduled_adapter._status_for` returns `"disabled"` **first** when the kill
switch is active, before it examines `has_running_row`. The kill switch has
been active since **2026-06-28 01:32:30Z** (`activated_by='monitor'`, reason
*"autonomy loop unattended — block any order path (monitor boot)"*), so rule
4's gate has been unsatisfiable for 79 days.

Verified by CALLING the real functions on real stored inputs (`_status_for` +
`stale_detection.compute`, `process_id='thesis_refresh'`, a real
`active_run_started_at`, `now = started_at + 20min`):

```
kill_switch=True   -> status=disabled  stale_reasons=()
kill_switch=False  -> status=running   stale_reasons=('mid_flight_stuck',)
```

This is rule 5's own bug, in rule 5's sibling. Rule 5 (`runtime_ceiling`) is
gated on `active_run_started_at is not None`, and its comment records exactly
why:

> ⚠⚠ Gated on the ACTIVE RUN, not on ``status``, and that is deliberate.
> ``scheduled_adapter._status_for`` returns ``disabled`` FIRST when the kill
> switch is on […] so a ``status == "running"`` gate would silently never fire
> for a halted system, and the ``_WEDGE_STALE`` membership that exists to keep
> this reason red under the kill switch would be dead code (Codex ckpt-2).

The correction was applied to rule 5 and never propagated to rule 4.

## 2. What is actually lost — a settled decision's whole mechanism

This is not merely a dark display chip. `docs/specs/ops/2026-06-20-jobs-admin-display-honesty.md`
§Decision 4 ("hung-job") **rejected** a periodic reaper and chose this rule as
its entire answer:

> **running_too_long: no new code.** A wedged non-heartbeating `running` row is
> **already** caught — `stale_detection.py:166-172` keys `mid_flight_stuck` on
> `COALESCE(last_progress_at, started_at)` vs `get_threshold(process_id)`, so
> an old `running` row with no progress already reads `attention` (red). Add a
> **test** asserting it; do not add a redundant reason.

So #1689's hung-job coverage has not existed since the halt went on. The test
it asked for was written against `status="running"` and therefore passes while
the production path is dead — a verifier pinned one layer away from the defect.

`health_verdict`'s documented invariant is false for one of the two reasons it
names:

> Two exceptions still read ``attention`` so nothing genuine is hidden behind
> the switch: (a) a genuine WEDGE (``queue_stuck`` / ``mid_flight_stuck`` — a
> halt does not un-stick a wedged queue; ckpt-1 invariant)

`queue_stuck` is unaffected — rule 3 has no status gate. Only
`mid_flight_stuck` is masked, and its `_WEDGE_STALE` membership is dead code.

The adapters already build the inputs the fix needs while halted: both
`scheduled_adapter` and `bootstrap_adapter` compute
`active_run = _build_active_run(active_row) if active_row is not None else None`
independently of `process_status`, and pass both timestamps to `compute`.

## 3. Change

**One predicate.** Rule 4 gates on the active run, not on `status`:

```python
if active_run_started_at is not None:
```

Verbatim the predicate rule 5 uses, for the reason rule 5 documents. The
`COALESCE(last_progress_at, started_at)` fallback is **retained** (§7).
`_WEDGE_STALE` / `ACTIONABLE_STALE` are unchanged — the point is that their
`mid_flight_stuck` membership starts being reachable.

Two consequences of making the reason reachable under a halt, both found by
ckpt-1, both fixed here because the fix is what exposes them:

**3a — the `disabled` branch has no ceiling-first precedence.** The non-disabled
branch of `compute_verdict` deliberately headlines `runtime_ceiling` over
`mid_flight_stuck` (`elif status == "running" and "runtime_ceiling" in actionable`),
because the ceiling is the more informative claim. The `disabled` branch picks
`wedges[0]` in plain `_REASON_ORDER` order, where `mid_flight_stuck` comes
first. Combined with 3b's FE rule, a halted row past its ceiling would read a
bare *"no progress"* — hiding both the ceiling and the elapsed silence. Apply
the same precedence inside the `disabled` branch.

**3b — the FE attaches the heartbeat duration to another reason's headline.**
`hasHeartbeatSuffix` is true whenever `mid_flight_stuck` is present and
`runtime_ceiling` is not, regardless of which reason is the headline. So
`queue_stuck` + `mid_flight_stuck` renders *"queue stuck 7m"* where `7m` is the
heartbeat age, not the queue age. This is the exact error its own docstring
already reasons about for `runtime_ceiling` —

> appending an elapsed-since-heartbeat would attach an unrelated duration to
> the ceiling claim

— so the predicate must exclude `queue_stuck` for the same reason. Both
reasons are reachable together under the halt only after this fix.

## 4. Why the exposed false positives do not block the fix

Rule 4's fallback fires on any run longer than its threshold when the job has
no heartbeat. Tick coverage, derived from **code** rather than from the
heartbeat's 10 hours of history: `report_progress` is called from exactly three
places — `market_data.refresh_market_data`, `fundamentals.execute_refresh` /
`refresh_financial_facts`, and inside `thesis_refresh`. Tracing callers, the
tick-capable jobs are `daily_candle_refresh`, `daily_financial_facts` and
`thesis_refresh` — and the first two are **not in `SCHEDULED_JOBS`**, so they
have no ProcessRow and rule 4 never evaluates them. **Of the 63 jobs rule 4
does evaluate, exactly one (`thesis_refresh`) can tick.**

So the honest question is the operator's actual exposure, not a count of runs.
Measured over the full `job_runs` corpus (2026-06-03 → 2026-09-15, 8,984,533s,
130,489 completed runs), summing over-threshold running time for every
non-ticking scheduled job against its own `get_threshold()`, `status='success'`
only:

**Expected rows reading a false `mid_flight_stuck` at a random instant: 0.0133.**

That is a 1.33% chance that ONE row is wrongly red. Largest contributors:
`sec_manifest_worker` 37,636s over 141 runs (0.0042), `fundamentals_sync`
21,755s over 29 runs (0.0024), `sec_filing_documents_ingest` 20,288s over 6
(0.0023), `sec_business_summary_bootstrap` 18,980s over 10 (0.0021).

⚠ Revision 1 called this a "flood" off a run COUNT (≈1,000 instances over 3.5
months, 743 of them `thesis_refresh` — which ticks, so they are not false
positives at all). A count of run-instances is not an exposure. 1.33% of one
row is comfortably inside this ticket's constraint (*"must not train the
operator to ignore it"*), so **no threshold is changed here** — and the per-job
heartbeat-gap distribution that would justify changing one does not exist yet.

⚠ Status vocabulary, corrected from revision 1: `job_runs.status` is
`running|success|failure|skipped|cancelled|degraded` (sql/254). `partial` is
not a member, and `degraded` means *completed and made no progress* — not
healthy. Dev holds **zero** `degraded` and zero `partial` rows, so the figures
are unchanged; the claim was wrong, the number was not.

## 5. Tests

Pure-logic against `compute` / `compute_verdict` (no DB):

1. **Halt mask, regression (the defect).** `status="disabled"`, active run and
   heartbeat both older than the threshold → `mid_flight_stuck` present. Red
   before the fix.
2. **Halt mask on the fallback path.** `status="disabled"`,
   `last_progress_at=None`, `active_run_started_at` past the threshold →
   present. This is #1689's own case, under the halt. Red before the fix.
3. **Bootstrap under the halt.** `mechanism="bootstrap"`, `status="disabled"`,
   no heartbeat, active run past the 1800s override → present. Rule 5 excludes
   bootstrap, so this rule is bootstrap's only hung-run signal.
4. **No active run → no rule 4**, for every terminal status. Replaces
   `test_mid_flight_stuck_does_not_fire_on_terminal_status`, which expressed
   that intent via `status` while passing a non-None `active_run_started_at` —
   a combination `_status_for` cannot construct, and the same `status`-coupling
   that caused this bug. Rule 5 already fires on that input today.
5. **Fresh heartbeat still mutes** under `disabled` (10s quiet vs 300s
   threshold → absent; 400s → present).
6. **Per-process override still consulted** on the halted path
   (`sec_filing_documents_ingest`, 1800s: quiet 1000s absent, 2000s present).
7. **Verdict, 3a:** `disabled` + `mid_flight_stuck` + `runtime_ceiling` →
   headline is the ceiling, not "no progress". Red before the fix.
8. **Verdict, precedence preserved:** `disabled` + `queue_stuck` +
   `mid_flight_stuck` → headline `queue_stuck` (unchanged).
9. **FE, 3b:** `queue_stuck` + `mid_flight_stuck` → no heartbeat suffix;
   `mid_flight_stuck` alone → suffix present. Red before the fix.

Revert-probe items 1, 2, 7 and 9 at the layer each defect lives at (the
`compute` predicate, the verdict branch, the FE predicate) — not at the layer
the helper lives at, per the revert-probe entry in
`docs/review-prevention-log.md`.

⚠ Test 1/2 must use `status="disabled"`. A `running` fixture is green before
the fix and proves nothing about the gate — revision 1's test plan made exactly
that error (ckpt-1 finding 9).

## 6. Acceptance

No new script. The two census figures in §4 are reproduced by the queries
recorded on issue #2274; the spec quotes them with their population and span
rather than referencing a file.

Dev-verify after deploy, kill switch still on:

1. `scheduled_adapter.list_rows(conn)` — confirm no row reads
   `mid_flight_stuck` while nothing is wedged (the fix must not repaint a
   healthy halted board).
2. Confirm the same call surfaces a non-None `active_run.last_progress_at` for
   a running `daily_candle_refresh`, i.e. the input rule 4 now consults is
   really present under a halt.

⚠ A quiet-producer POSITIVE cannot be forced on the live stack without
stalling a real sweep, so the positive side is covered by tests 1/2/3/5/6 and
only the negative side is checked live. Stated rather than presented as a full
verification.

## 7. Refuted — revision 1's second edit (dropping the `started_at` fallback)

Killed by Codex checkpoint 1 on three independent grounds:

1. **It reverses a settled decision.** #1689 Decision 4 chose the fallback as
   its whole hung-job answer, over a periodic reaper, and asked for a
   regression test on it (§2). Deleting it is a settled-decision reversal,
   which is person-gated — not something to fold into a defect fix.
2. **Bootstrap loses hung-run detection entirely.** `bootstrap_adapter` calls
   `compute` with a real `active_run_started_at`, bootstrap's threshold is the
   1800s override, and rule 5 **excludes bootstrap by design** ("no cadence to
   bound a ceiling against and no measured duration distribution"). Its own
   adapter comment says *"Only queue_stuck + mid_flight_stuck are reachable"*.
   So "discharged by rule 5" was false for the one mechanism that has no
   alternative.
3. **Its justification did not measure what it claimed.** Run duration over
   threshold establishes fallback eligibility only when the heartbeat is
   absent; revision 1 attributed 743 `thesis_refresh` runs to the fallback
   when `thesis_refresh` ticks. And a `NULL` heartbeat does not identify an
   uninstrumented job — it also means instrumented-but-stalled-before-first-
   tick, or a failed telemetry write. The populations were conflated.

Also corrected: revision 1 claimed the pre-first-tick window is
`_MIN_WRITE_INTERVAL_S` (5s) wide. That constant bounds write FREQUENCY, not
first-tick latency; initialisation, slow first work or a failed heartbeat write
can leave `last_progress_at` NULL indefinitely.

## 8. Out of scope, recorded

**`can_cancel` is false for a halted wedge.** `scheduled_adapter` gates it on
`process_status == "running"`, so a row this fix newly paints red offers no
cancel affordance under the halt. The available recovery is the jobs-process
restart already in `docs/wiki/runbooks/runbook-stuck-process-triage.md`, or
lifting the switch. Making cancel reachable while halted is a behaviour change
on an operator control path and is not folded in here. The runbook's
"Action ladder" is updated to say so, since it currently implies Cancel is
always available.
