# #2274 — a job that keeps losing runs to orphan reaps reads `current`

Slice of #2274's "consecutive-orphan-reap verdict" remainder. **Surfacing half only** —
no acting half, nothing terminates a run, no threshold that ends anything.

Revised after Codex ckpt-1 (17 findings). Five of them changed the design; two were
refuted by measurement and are recorded here so the next reader does not re-litigate them.

## The defect, established on actual VERDICTS rather than on `status`

The first draft inferred invisibility from the latest terminal `status`. That is not the
same claim — a row can read `attention` for an unrelated stale reason. Re-measured by
calling `scheduled_adapter.list_rows` + `verdict_for_row` against the dev DB
(2026-09-19):

| job | reap events / 30d | `status` | **verdict** |
| --- | ---: | --- | --- |
| `sec_filing_documents_ingest` | 7 | `ok` | **`current`** (green) |
| `thesis_refresh` | 11 | `idle` | **`current`** (green) |
| `strategy_paper_cycle` | 2 | `ok` | **`current`** (green) |
| `core_rebalance_observation` | 6 | `idle` | `attention` — `schedule_missed`, unrelated |

Three of four read green while repeatedly losing runs. `strategy_paper_cycle` is the
engine's own cadence.

Two mechanisms, both verified in the tree:

1. `health_verdict.compute_verdict` is a function of the latest terminal `status` and the
   *current* `stale_reasons`. A recurrence is a property of a WINDOW of runs; the model
   has no axis for it.
2. `scheduled_adapter:979` sets `last_n_errors` from `terminal_row["error_classes"]` —
   the **latest terminal row only**, despite the plural name. One later success erases the
   history from the page.

## Two ckpt-1 findings REFUTED by measurement (do not re-raise)

- *"The reaper schedules an auto-retry, so a reaped row reads `self_healing` via
  `retry_in_flight`, not `attention`."* — `select count(next_retry_at) … where error_msg =
  <constant>` returns **0 of 502**. No retry is in flight from a reap, so that branch is
  not reached.
- *"Reaping writes `error_msg`, not `error_classes`, so the reap never reaches
  `last_n_errors` at all."* — `count(*) filter (where error_classes is not null)` returns
  **502 of 502**. It does reach it, while it is the latest terminal row; the erasure is
  caused by the later success, as stated above.

## The premise correction that decides the shape

The remainder is worded "consecutive". Measured, **"consecutive" mostly is not a streak of
incidents at all**: one boot reaps every orphan row in a single UPDATE, so
`thesis_refresh`'s apparent 25-run streak is **25 rows sharing one `finished_at` second** —
one incident. Across the corpus, 502 rows collapse to **467 events**;
`strategy_paper_cycle`'s 7 rows in 30 days are **2 events**.

So the unit is the **reap EVENT** (`job_name` + `date_trunc('second', finished_at)`), and a
row count would have reported one boot as 25 separate failures.

## Source rule

There is no published or vendor formulation for "how often may a background job lose a run
before that is worth showing". Recorded explicitly, per the same rule `RUNTIME_CEILING_S`
follows in `stale_detection.py`: **fixed BY CONSTRUCTION and frozen here** — not fitted,
and not given an invented citation.

**The discriminator is a convention, and is labelled as one.** `reap_orphaned_job_runs`
(`ops_monitor.py:776-792`) is the sole writer of the reap message — `rg` over `app/`
returns exactly one write site — and `error_category` is the shared `internal_error`, so
the message is the only reap-specific marker that exists today. It is exported as one
`Final[str]` and used at the write site, at the read site, and in
`scripts/measure_2946_quota_load.py`, which currently hand-rolls its own copy. The read
predicate is tightened to all three columns:

```sql
status = 'failure' AND error_category = 'internal_error' AND error_msg = <constant>
```

⚠ A shared constant prevents spelling drift between cooperating sites. It **cannot**
prevent a future writer storing identical text, and changing the constant would strand
502 historical rows. A reserved structured marker (a dedicated `error_category`, or a
`reaped` boolean) is the durable form and is deliberately NOT taken here — it is a
migration, and this slice is the surfacing half. Recorded as the follow-up.

## The rule

`RECENT_REAP_WINDOW_DAYS = 7` · `RECENT_REAP_CHIP_FLOOR = 3` (events, not rows)

`ProcessRow` carries `recent_reap_events` and `recent_reap_runs` — reap events, and the
runs those events wrote off, inside the trailing 7 days. The FE renders a muted
**historical** chip when `recent_reap_events >= 3`; the raw counts are always on the wire
so the drill-in can show them at any value.

**Window on `finished_at`, not `started_at`.** The reap event happens when the reaper runs,
not when the run started, and the lag is large: p50 **0.005 d**, p90 **0.281 d**, p99
**1.86 d**, max **4.375 d**. A `started_at` window would miss a ten-day-old run reaped
today and would drop a six-day-old one tomorrow. (This also invalidated the first draft's
historical fire-set, which had a look-ahead: it bucketed each reap on the day the run
*started*, before the reap had happened.)

**Why 7 days:** short enough that the count describes the situation now rather than a
regime that has ended — the trap this ticket has hit twice (90-day totals describing an
ended regime; an M1 post-fix window of 10 minutes describing nothing).

**Why a floor of 3 events, and what it is NOT.** It is **not** a causal claim. Deploys and
genuine faults are not separable from `job_runs` — several ordinary deploys in a session
produce several reaps, and a single reap can be an OOM. The floor is chosen to bound the
DISPLAY rate, measured on the real corpus (events, `finished_at`, no look-ahead, 30 days):

```
>=2 events / 7d : 17/30 days carry a firing job; max 10 concurrent
>=3 events / 7d : 17/30 days carry a firing job; max  5 concurrent   <- chosen
today at >=3: daily_candle_refresh(11), daily_portfolio_sync(4), sec_filing_documents_ingest(4)
```

Same day-coverage, half the worst-case row count. 3 chips on a 72-row page.

## ⚠ Coverage — measured, and it is the headline limitation

Two of today's three firing jobs are **not on the Processes page at all**. Verified by
building every adapter's rows and testing membership:

```
scheduled 65 · bootstrap 1 · ingest_sweep 6
daily_candle_refresh     ABSENT      (11 events / 7d — the worst offender)
daily_portfolio_sync     ABSENT      ( 4 events / 7d)
fx_rates_refresh         ABSENT
sec_filing_documents_ingest  IN PAGE ( 4 events / 7d)
```

They are orchestrator LAYER adapters, not `SCHEDULED_JOBS` entries — the same population
#2274's M1 measurement found ("the rest are orchestrator LAYER adapters that are not in
`SCHEDULED_JOBS` at all"). So this slice covers 65 scheduled rows and reaches **one** of
today's three firing jobs.

That is stated rather than worked around. It does not make the slice vacuous — it covers
`strategy_paper_cycle`, `thesis_refresh`, `sec_filing_documents_ingest` and 62 others —
but the remaining gap is #2274's already-open **"re-scoped ProcessRow coverage gap"** item,
which this measurement sharpens from an estimate to a named list.

## Why this is NOT a new `StaleReason` / verdict

Considered and rejected on measurement. A sixth reason in `ACTIONABLE_STALE` returns
`attention` on 17 of the last 30 days. Those are true positives, but they are chronic and
substantially caused by the operator's own deploys, and #2274's binding constraint is
*"must not train the operator to ignore it"*. #1831 is the measured precedent for painting
an expected chronic condition red: ~42 halted jobs went red and buried the real failures.

So the count is reported and the judgement stays with the operator.

⚠ Two consequences, stated rather than left to be found:

- A job losing runs every week shows a muted chip, not an alarm. If the operator wants an
  alarm, that is a decision made on this evidence rather than a default nobody chose.
- The chip must be labelled as **historical** ("3 reaps in 7d"), not as a health claim.
  A chip reading "unhealthy" beside a `current` verdict would re-introduce exactly the
  two-cells-that-disagree defect `health_verdict`'s docstring exists to prevent.

⚠ Promoting this to a verdict later is **not** a one-line change (first draft said it was,
wrongly): `compute_verdict` derives its actionable list from `_REASON_ORDER`, not from
`ACTIONABLE_STALE`, so promotion needs the `StaleReason` literal, `_REASON_ORDER`,
`_REASON_LABEL`, the wedge/kill-switch precedence decision, and a producer.

## What this does NOT claim

That the count alone evaluates #2274 M1's fix (`e358fa86`, deployed 2026-09-19 00:10:50Z).
It cannot: pre-fix events stay in the window for 7 days, window expiry alone mimics
improvement, and a delayed reap of an old orphan mimics a new failure. The post-fix window
at the time of writing is 10 minutes / 24 runs against a base rate of ~2-3 events per day.
The chip is a standing instrument, not a verdict on that fix.

`attempt` is not a substitute (ckpt-1 raised it): reaped rows carry `attempt` 1-5 with
**284 of 502 at attempt 1**, so it tracks the retry position of a scope, not a reap streak.

## Delivery contract

- `ProcessRow.recent_reap_events: int = 0`, `recent_reap_runs: int = 0` — defaults keep
  `bootstrap_adapter` / `ingest_sweep_adapter` unchanged (neither is backed by `job_runs`).
- ONE batched `GROUP BY job_name` read per snapshot, captured in `list_rows` the way
  `kill_switch_active` already is, so the window cannot drift row-to-row. `get_row` takes
  the same helper scoped to one job, so list and detail agree.
- A job absent from the result map is **0**, which is the honest value: no reap event in
  the window. There is no "unavailable" state — the query either succeeds for all rows or
  the adapter already fails the snapshot.
- Wire: `ProcessRowResponse` + `frontend/src/api/types.ts` + the chip in the processes
  table.

## Cost

One batched query per snapshot. Measured on dev: parallel seq scan over 139k `job_runs`,
**5.6 ms**, 9 groups. No index and no migration. ⚠ This is a scan, so it grows with
`job_runs`; at 10× the table it is still well inside a page load, and the read is shared
across all 65 rows rather than run per row.

## Security

No security surface — a read-only count on an authenticated admin page. No new input, no
user-controlled SQL, no change to any gate.
