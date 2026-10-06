# Monthly kill-switch drill with an estimated time-to-flat

#3614 scope item 4: *"A monthly recorded kill drill with time-to-flat."* Items 1–3 are merged
(#3671, #3672, #3678, #3679). This spec defines what the drill proves, how it runs without ever
committing a change to the kill switch, and what "time-to-flat" can honestly mean in an engine that
has no flatten-all path.

Rung: judgement artefact (spec) → Codex checkpoint 1 (log at the end). The build slices touch the
kill-switch read paths of three entry chokepoints, so they are behavioural and take checkpoint 2.

**Scope decision.** The item is met by a recorded drill plus an **estimated** time-to-flat for a
stated scenario. An observed flatten is outside this item. Flattening the book monthly would close
the core sleeve and re-open it, paying the spread twice a month and, on live capital, realising a
disposal per position. Those costs buy one latency observation the estimate's inputs can collect
without them (every real close adds a sample). This is an engineering call within the ticket, made
here and stated so it can be contested.

## What exists today

Measured on the dev DB at 2026-10-06 ~15:00Z, SELECT-only; the value column is the query's output
on that date. A schema grep (`CREATE TABLE … kill` in `sql/`) finds only `kill_switch` and
`strategy_kill_drill_events`:

| fact | value | query |
|---|---|---|
| per-strategy live-gate drill records | 0 rows in `strategy_kill_drill_events` | `SELECT count(*) FROM strategy_kill_drill_events` |
| live-gate policies | 0 | `SELECT count(*) FROM strategy_live_gate_policies` |
| global kill-switch toggles | 10 audit rows. Ids 11–16 on 2026-09-23 are the only drills (#2603 acceptance, attended, each on for 0.4–1.6 s). There is no table of global kill-switch drill results | `SELECT audit_id, changed_at, changed_by, reason, old_value, new_value FROM runtime_config_audit WHERE field = 'kill_switch' ORDER BY audit_id` |
| engine book | 4 active ownerships (ids 3–6), all SPY.RTH, all mapped to a `broker_positions` row with a stop, a target and `is_no_stop_loss = false` | `SELECT o.ownership_id, i.symbol, bp.position_id, bp.stop_loss_rate, bp.take_profit_rate, bp.is_no_stop_loss FROM strategy_position_ownership o JOIN strategy_trades t USING (strategy_trade_id) JOIN instruments i ON i.instrument_id = t.instrument_id LEFT JOIN broker_positions bp ON bp.position_id = o.broker_position_id WHERE o.status = 'active'` |
| closes ever recorded | 1: `close` / `core_rebalance`, created 2026-09-23 13:35:28Z, submitted 13:35:31Z, resolved 13:50:02Z | `SELECT operation_type, trigger_code, status, created_at, submitted_at, resolved_at FROM strategy_position_operations WHERE operation_type = 'close'` |
| `runtime_config` | `enable_auto_trading = true`, `enable_live_trading = false` | `SELECT enable_auto_trading, enable_live_trading FROM runtime_config` |

The existing `run_kill_drill` (`app/services/strategy_live_gate.py:853`) is a different control: it
toggles a synthetic `strategy_execution_blocks` row `drill:<kind>` for one of five health sources,
never the global `kill_switch`, and refuses without a live-gate policy (`:874-877`). It is unchanged
here and does not satisfy this item, nor this drill it.

**Evidence limits.** "No flatten-all" rests on a name grep (`flatten|close_all|liquidat` across
`app/`, `scripts/`, `frontend/src`) plus a read of every caller of the two broker close methods
(`close_position`, `close_demo_strategy_position`); neither loops over the book. "No frontend caller
of `operator_close`" rests on a grep of `frontend/src` for the route.

## What the kill switch does, path by path

**Entry chokepoints.** The broker entry methods are `place_order`, `place_demo_strategy_order` and
`place_demo_core_order` on `BrokerProvider`. Their callers outside tests
(`grep -rn "\.place_demo_strategy_order(\|\.place_demo_core_order(\|\.place_order(" app/ scripts/`):

| id | path | loader | decision | kill evidence |
|---|---|---|---|---|
| C1 | recommendation BUY/ADD: `order_client.execute_order` → `broker.place_order` (`order_client.py:2601`) | `execution_guard.load_kill_switch`, `get_runtime_config` | `execution_guard.decide_submission_controls` (`:370`) | the result with rule `kill_switch` has `passed = False` |
| C2 | strategy, ranking-pot and AI-trial entries → `broker.place_demo_strategy_order` (`strategy_paper_executor.py:1472`, `:1829`) | `get_runtime_config` + the `kill_switch` row, today inline in `_trading_enabled_refusal` (`:1530`) | the same function | returns `kill_switch_active_or_missing` |
| C3 | core `buy_core` → `broker.place_demo_core_order` (`strategy_core_executor.py:531`) | `strategy_core_preflight.preflight_core_submission` (`:451`), which requires the core submission and core mandate advisory locks held (`:486-497`) | `decide_core_preflight` (`:531`) | returns `core_kill_switch_active_or_missing` |

C2's check runs before any submission or uncertain-submission retry in all three executors
(`strategy_paper_executor.py:1600`, `ranking_pot_executor.py:284`, `ai_trial_executor.py:128`).
The manual `POST /portfolio/orders` route checks the kill switch (`app/api/orders.py:511`) but does
not reach a broker entry method, so it is not in the census.

The census is provisional by construction: it is a grep of three method names. Slice 1 turns it into
a test, so a fourth caller of an entry method fails CI until the drill covers it. It cannot detect an
entry that bypasses `BrokerProvider` altogether.

**Exit routes while the kill switch is on.**

| route | under kill | source |
|---|---|---|
| broker-side SL/TP on each position | executed by the broker | `etoro_broker.py:610-612`, `:680-681` |
| `manage_owned_position`: stop repair, timeout and deadline closes, pot exits, and the operator's `POST /strategies/positions/{trade}/{pos}/close` (`close_reason = operator_close`). Demo credentials only: the provider adapter refuses live ones (`strategy_position_manager.py:1642`) | open, deliberately | `strategy_position_manager.py:1640-1642`; `app/api/strategies.py:3645` |
| recommendation EXIT | refused, deliberately | `order_client.py:1455-1459`: the kill switch applies to every action; "EXIT is never blocked" governs thesis, coverage and spread |
| core `sell_core` (rebalance) | refused, deliberately | `strategy_position_manager.py:1644-1649`: a rebalance is not de-risking |

These semantics are recorded, not changed. The settled rule "EXIT never blocked"
(`docs/settled-decisions.md` § Execution guard semantics, and the 2026-08-22 entry) concerns stale
thesis, coverage and spread, and the code says so at both sites.

## Two modes, chosen by the kill switch's state

Every run holds a drill-specific session advisory lock for its whole life, on a dedicated
autocommit **lock connection**: `pg_try_advisory_lock(<drill key>)` first; if another run holds it,
the run exits without recording. The lock connection also carries the book snapshot and the
recording transaction (below), and its `finally` unlocks and closes it. Every other connection the
run opens is closed before the lock is released. The lock connection sets
`idle_session_timeout = '10min'`, so a suspended process loses its session, and with it the drill
lock, ten minutes after its last statement; a hung run therefore cannot block later runs beyond
that, and the overdue notice covers any occurrence it cost. Each run also generates a run token
(a UUID), stored on the event and carried in the sandbox activation's reason.

### Sandbox mode (the monthly run; the switch is off)

The drill turns the switch on **inside one transaction that it always rolls back**, and evaluates
the three chokepoints inside that transaction. No other session ever sees the switch on, no audit row
survives, and the drill has no code path that commits a kill-switch change. That removes, by
construction, the questions a committed toggle raises: overwriting an operator's activation,
clearing a kill the machine did not set, and paging a routine drill.

On a **sandbox connection** opened with `autocommit=False`. The run asserts `conn.autocommit is
False` and, after the explicit `BEGIN`, `transaction_status == INTRANS`, and refuses to continue
otherwise: on an autocommit connection the helper's `conn.transaction()` would be a top-level
transaction and would commit.

1. `BEGIN`; `SET LOCAL lock_timeout = '2s'`, `SET LOCAL statement_timeout = '5s'`,
   `SET LOCAL transaction_timeout = '30s'` (server-enforced on PostgreSQL 17; dev runs 17.9, so a
   stalled Python process cannot hold the transaction beyond 30 s).
2. Take `CORE_SUBMISSION_ADVISORY_LOCK` (transaction-scoped), then lock the kill row
   `FOR UPDATE`: the order `ops_monitor.activate_kill_switch` uses (`ops_monitor.py:1132-1134`).
   A missing row → run failure `kill_switch_row_missing`. If the switch is on → roll back and run
   observe mode.
3. Call the real `ops_monitor.activate_kill_switch(conn, reason, activated_by="kill-drill")`, with
   reason `kill drill sandbox <run token>, rolled back`. Its `conn.transaction()` is a savepoint inside the
   drill's transaction, and the advisory and row locks it takes are already held by this backend.
   The kill-switch trigger is statement-level `BEFORE` and takes the same advisory key
   (`sql/372_core_submission_safety_serialization.sql`), also already held. Measured with psycopg
   3.3.3 on the dev DB: a `conn.transaction()` block opened inside an open transaction is a
   savepoint, and the outer `ROLLBACK` discards its writes. The helper still logs
   `Kill switch ACTIVATED … reason=… sandbox, rolled back`; that line names the sandbox in its own
   reason field, but a log reader who ignores the reason can still misread it.
4. Load the current core mandate (`strategy_core_mandate_events`, latest revision) under
   `CORE_MANDATE_ADVISORY_LOCK` (transaction-scoped), so C3 probes the instrument the executor
   would, and C3's loader finds both locks held (`core_lock_held` reads granted advisory locks for
   this backend from `pg_locks`). The mandate event id, revision and instrument are stored. No
   enabled mandate → C3 outcome `not_applicable`: without one the core path places no order, so
   there is nothing for the kill switch to stop.
5. Evaluate C1–C3 on the sandbox connection (below).
6. `ROLLBACK` in `finally`, then close the sandbox connection.
7. **Verify**, in the same `finally` whenever step 3 was attempted, on the lock connection: does a
   `runtime_config_audit` row carry this run's token in its reason? Yes → a probe committed the
   sandbox activation: run failure `sandbox_committed`. The current kill state is recorded
   separately (`kill_active_at_verify`), because an operator may have toggled it since: a
   committed sandbox activation turned the switch on, never off, and the existing toggle notice
   pages that commit; whether it is still on is the recorded state, not an inference. The switch
   being on at verification without the token is an operator's activation, waiting on the drill's
   locks, and is not a drill failure.

**Which loaders must not commit.** `load_kill_switch` and `get_runtime_config` issue SELECTs with
no explicit commit, rollback or transaction block; inside the sandbox transaction their reads join it
(`execution_guard.py:160-170`; `app/services/runtime_config.py::get_runtime_config`).
`preflight_core_submission` is read-only by its docstring (`strategy_core_preflight.py:466`). C2's
loader is new in slice 1 and must not commit. Slice 1 tests each against a second connection that
watches the committed row during evaluation. Step 7 is the runtime check for the same property, and
its worst case is fail-closed.

**Who waits.** While the sandbox transaction is open, the core advisory key it holds is also taken
by the trigger on writes to `kill_switch`, `runtime_config`, `strategy_execution_blocks` and
`broker_credentials` (`sql/372…`), and by core submissions. Those writes and an operator activation
wait for at most the transaction's life, and the drill never overwrites them; a waiter whose own
lock, statement or request timeout is shorter than that can fail, and surfaces the failure as it
would any lock timeout. Reads of those tables do not wait. The
normal run lasts milliseconds; 30 s is the enforced ceiling, not a target.

### Observe mode (the switch is already on)

The drill writes nothing to the kill row. On a fresh `autocommit=False` connection with the same
`lock_timeout`, `statement_timeout` and `transaction_timeout`, it takes the core submission and
mandate advisory locks, **re-reads the kill row under those locks**, and records whose activation it
is (`activated_by`, `activated_at`). If the row is now off → `observe_outcome =
kill_changed_before_observe`, entry verdict `not_run`, no chokepoint rows. If it is missing → run failure `kill_switch_row_missing`. Otherwise it
evaluates C1–C3, rolls back and closes the connection.

This mode is how a **committed** activation is proved: the attended acceptance turns the switch on
through the normal endpoint, runs the drill, and reviews the result before anyone turns it off. The
machine never toggles the switch.

## Evaluating a chokepoint

Each chokepoint is evaluated by its own loader and decision function, never by the wrapper that
records refusals, so the drill writes no `decision_audit` or rejection rows:

- **C1:** `decide_submission_controls(load_kill_switch(conn), runtime, runtime_corrupt)`, with
  `RuntimeConfigCorrupt` mapped to `runtime=None, runtime_corrupt=True` as `evaluate_recommendation`
  does (`execution_guard.py:749-755`). Not `order_client._assert_submission_controls`, which writes
  and commits a `decision_audit` row before raising (`order_client.py:1461-1467`).
- **C2:** slice 1 splits `_trading_enabled_refusal` into a loader and a pure decision. The loader
  raises `RuntimeConfigCorrupt` exactly where the current code does, before the kill SELECT. The
  production wrapper keeps today's shape: on `RuntimeConfigCorrupt` it returns
  `runtime_config_corrupt` without committing; otherwise it commits and calls the decision. The drill
  catches `RuntimeConfigCorrupt` itself and calls the loader and decision without the commit.
- **C3:** `preflight_core_submission(conn, core_instrument_id=<from step 4>, action="buy_core", now)`.

### Outcome per chokepoint

Classified by whether the chokepoint's own precedence reached its kill check:

- `kill_refused` — the kill evidence in the census table is present. For C1 that is the
  `kill_switch` rule failing, whatever else fails; every failed rule name is stored.
- `other_refusal` — the chokepoint returned before its kill check, with one of the codes below.
  Only C2 and C3 have such a return: C2 returns `runtime_config_corrupt` or `auto_trading_disabled`
  ahead of its kill check (`strategy_paper_executor.py:1532-1541`); C3 returns
  `core_runtime_config_corrupt` or `core_auto_trading_disabled` before it reads the kill row
  (`strategy_core_preflight.py:503-508`), and `decide_core_preflight` checks the kill switch first
  after that (`:557`).
- `allowed` — the kill check was reached and did not refuse, whether or not another rule refused.
  For C1 this is the `kill_switch` rule passing while the row is on. A C1 run with auto-trading off
  and the kill rule removed is `allowed`, which is what the revert-probe must show.
- `error` — the loader or decision raised, or C1 returned `kill_switch_config_corrupt` while the
  drill can see the row (a loader that lost the row); the exception or rule is stored.
- `not_applicable` — C3 only, when no core mandate is enabled (step 4).

**What the kill evidence shows.** C2 and C3 return the same code for "on" and "row missing"
(`strategy_paper_executor.py:1540-1541`, `strategy_core_preflight.py:557-560`), so for them
`kill_refused` proves refusal on active-or-missing state, not that the loader saw the activation.
The drill stores its own read of the row beside it. A loader that lost the row would still refuse:
the missing-row reading is fail-closed in both.

### Entry verdict

- `not_run` — observe mode found the switch off (`kill_changed_before_observe`), or a run failure
  happened before any chokepoint was evaluated.
- `failed` — any chokepoint `allowed` or `error`, or a run failure after evaluation began.
- `incomplete` — none failed, but at least one `other_refusal`.
- `passed` — every applicable chokepoint `kill_refused`.

C1 has no early return, so every run that evaluates chokepoints tests a kill check on at least C1.
The codes that make a run `incomplete` are all "auto-trading off" or "runtime config corrupt", and
each refuses every entry on that path by itself.

### What this proves, and what it does not

It proves that, with the kill row on as each chokepoint's own loader reads it, each chokepoint's own
decision refuses. In sandbox mode the row read is the drill's own uncommitted write; that tests the
loaders' SQL and the decisions, not cross-session visibility of a committed row, which only observe
mode shows. It does not prove that every worker calls its chokepoint before submitting or obeys the
result: executor-path tests with a recording broker stub (slice 1) hold that, not the drill. C1 and
C2 also have a check-to-submit window: an entry that passed its check before an activation commits
can still submit after it. Only the core path serialises against activation, through the advisory
lock (`ops_monitor.py:1128-1131`). That window is existing behaviour, recorded here as a limit.

## The book snapshot

Taken after the chokepoint evaluation, on the lock connection, in one
`REPEATABLE READ READ ONLY` transaction, so every read sees one snapshot. Its first statement both
takes the snapshot and returns `clock_timestamp()`; t0 is that value, stored as `snapshot_at`. A run
failure before the snapshot sets book verdict `not_run` and estimate reason `snapshot_unavailable`. The estimate concerns this frozen exposure and assumes no entry
after it.

- **Exposure universe:** every `strategy_position_ownership` row with `status = 'active'`, plus
  outstanding engine authority: `strategy_trades` in `planned`, `submitted`, `closing` or
  `reconcile_required`, and `orders` with `execution_origin = 'strategy'` and `status = 'submitted'`,
  each stored by id and state. The operator's manual holdings and copy-mirrors are not engine
  positions and are excluded.
- **Per active ownership:** every `broker_positions` row it maps to (none, one or several), each
  row's `updated_at`, stop (`stop_loss_rate` not null and `is_no_stop_loss` false), target, and the
  order environment that opened it (`orders.broker_environment`). This checks cached state, not the
  broker's live view.
- **Book verdict:** `ok`, or `defects` with counts of unmapped ownerships, multiply mapped ownerships
  and positions without a stop. A defect does not change the entry verdict.

## Estimated time-to-flat

The drill records an **estimate**, named as one everywhere it is stored or shown, for one scenario:
the operator closes every engine position through `operator_close`, the operator route open under
kill. Broker SL/TP and the position manager's own exits run concurrently and are excluded from the
model: they can close positions sooner, and they can also use the shared write allowance.

**Timeline.** Closes are submitted in minute slots of 20, starting at the earliest scheduled
opportunity (below). Slot j starts j minutes after that opportunity; a slot that would start at or
after the session's close (16:00 ET, or 13:00 ET when `us_market_status` is `half_day`) moves to the
next session's open, and later slots follow it. Close k (0-based) is created at the start of slot
`floor(k / 20)`. Each close resolves R after it is created. The book is flat when the last close
resolves:

`estimated_time_to_flat = created_at(last close) + R − t0`

- **Opportunity.** On the New York civil date d of t0: if `session_rate_capture.session_open(t0)` →
  t0; else if `market_calendar.us_market_status(d)` is not `closed` and t0 is before d's 09:30 ET →
  that open; else `bar_capture_certificate.next_session_open_utc(d)`, whose `None` means
  `session_unknown`. A position whose instrument is not US-listed → `session_unknown`. This is the
  earliest scheduled opportunity, not a guarantee the broker accepts a close then (halts,
  instrument restrictions).
- **Queue.** eToro documents order writes at 20 per minute, shared
  (`.claude/skills/data-sources/etoro-api.md` § Stable facts, from the live portal 2026-08-11). The
  window's semantics are not documented, so the term assumes the full allowance is free at the
  opportunity and nothing else is writing: an optimistic assumption, labelled as one.
- **R.** The maximum of `resolved_at − created_at` over every `close` row in
  `strategy_position_operations` with `status = 'applied'`, read in the snapshot. Each sample's
  operation id, trigger and timestamps are stored, and rows not applied are counted beside it. R is
  an unadjusted historical duration: an observed maximum over n closes, not a bound or a percentile,
  and its samples may themselves contain session or queue waits the model also adds. Today n = 1
  (874 s, a `core_rebalance` close rather than an `operator_close`). With n = 0, R is NULL.
- **NULL estimate**, with the reason stored, when: R is NULL (`no_close_observed`); any opportunity
  is `session_unknown`; outstanding engine authority exists (`outstanding_authority`); any ownership
  is unmapped or multiply mapped (`mapping_defect`); or any position was not opened in the demo
  environment (`unsupported_route`, since `operator_close` is demo-only). With no active ownership and
  no outstanding authority the estimate is 0, stored as "no known engine exposure".

**Excluded:** operator reaction time and per-request operator work; broker outages; the broker's own
close-eligibility answer. **No threshold:** no repo rule or skill sets an acceptable time-to-flat
for this book (grepped `docs/`, `.claude/skills/` for "time-to-flat" and "time to flat": no hit), so
the figure is recorded and never gated.

The drill also records `operator_surface = 'api_only'` while `operator_close` has no frontend caller.

## Schema

One migration. Every table is append-only, enforced by trigger as in
`sql/467_ibkr_borrow_archive.sql:53-54`. Discrete CHECK columns, no JSON, because every field is read
by a fixed query.

- `kill_switch_drill_events`: `kill_switch_drill_event_id`, `started_at`, `finished_at`,
  `scheduled_for` (the occurrence a scheduled run serves; NULL for manual), `trigger`
  (`scheduled` | `manual`), `job_run_id` (nullable), `actor`, `run_token` (unique), `mode`
  (`sandbox` | `observe`), `kill_active_at_start`, `kill_active_at_verify` (sandbox only),
  `observed_activated_by`, `observed_activated_at`, `observe_outcome` (NULL |
  `kill_changed_before_observe`), `core_mandate_event_id`, `core_mandate_revision`,
  `core_instrument_id` (all NULL when no mandate is enabled), `entry_verdict` (`passed` |
  `incomplete` | `failed` | `not_run`), `book_verdict` (`ok` | `defects` | `not_run`),
  `run_failure` (NULL | `kill_switch_row_missing` | `sandbox_committed` | `exception`),
  `failure_detail`, `snapshot_at`, `unmapped_ownerships`, `multiply_mapped_ownerships`,
  `positions_without_stop`, `estimated_time_to_flat_s`, `estimate_null_reason`, `close_samples_n`,
  `close_samples_not_applied`, `close_resolution_max_s`, `operator_surface`, and the build-stamp
  columns order and decision rows carry (#3672).
- `kill_switch_drill_chokepoints`: primary key `(event_id, chokepoint)`, `chokepoint` in
  (`C1`, `C2`, `C3`), `outcome` (`kill_refused` | `other_refusal` | `allowed` | `error` |
  `not_applicable`), `refusal_code`, `failed_rules` (C1's failed rule names, as a text array),
  `drill_read_kill_active` (the drill's own read of the row), `error_detail`, `evaluated_at`. No rows
  when the entry verdict is `not_run`.
- `kill_switch_drill_positions`: surrogate primary key; `event_id`, `ownership_id`,
  `broker_position_id` (NULL for an unmapped ownership), `instrument_id`, `broker_row_updated_at`,
  `stop_present`, `target_present`, `broker_environment`, `opportunity_at`, `session_unknown`.
  Unique `(event_id, ownership_id, broker_position_id)` where the broker id is not NULL, and unique
  `(event_id, ownership_id)` where it is NULL.
- `kill_switch_drill_authority`: primary key `(event_id, kind, ref_id)`, `kind` in
  (`strategy_trade`, `order`), `state`.
- `kill_switch_drill_close_samples`: primary key `(event_id, position_operation_id)`, `trigger_code`,
  `status`, `created_at`, `resolved_at`.

**Failure recording.** Everything from the lock acquisition to the snapshot runs inside one
`try/except`. Any exception becomes `run_failure = 'exception'` with the class and message; a second
exception during cleanup is appended to `failure_detail`, never replacing the first. The event and
its child rows are written in one transaction on the lock connection. If recording itself fails, the
job run fails (`job_runs` keeps the error) and the manual script exits non-zero; the overdue notice
below covers a scheduled occurrence with no event.

## Running it

- **Manual (slice 1):** `scripts/run_kill_switch_drill.py --actor <name>`, trigger `manual`. Run from
  `~/Dev/eBull` by an attended session.
- **Scheduled (slice 3):** job `kill_switch_drill`, `Cadence.monthly(day=3, hour=6, minute=12)`
  (`scheduler.py:204`), trigger `scheduled`, actor `kill-drill`, `catch_up_on_boot=True`, storing
  `scheduled_for` and `job_run_id`. The day and time are a choice, not a rule: 06:12 UTC is outside
  the US session, before `execute_approved_orders` (06:30) and off the `:15`/`:30` dev connection
  peaks. Off-session is preferred, not required.
- Neither path places orders, so the clean-`origin/main` entry refusal (#3671) does not apply
  (Safety model). The build stamp makes a run from a dirty tree visible.

## Paging (`scripts/operator_alert_watch.py`)

Sandbox mode commits no kill-switch change, so the watch's kill-toggle notices
(`operator_alert_watch.py:142`) still fire only for committed toggles. Observe mode runs only after
someone else's committed toggle, which already pages. Slice 3 also corrects that notice's text,
"New entries are refused; exits still run" (`:152`): under kill, broker SL/TP and exact-owned exits
run, while recommendation EXIT and core rebalance sells are refused.

The new notices are evaluated per event and per occurrence over **every** drill event and
occurrence since slice 3 deployed, not over the watch's one-hour lookback (`:55-61`) and not only the
latest event, so a later run never hides an earlier finding. Each is keyed by event id or occurrence.
Drill keys never expire from the status file (about twelve a year), so a notice missed during a
watch outage is sent on the next pass and never sent twice. Each follows the #2843 contract (what
happened, evidence, what to check, safe default).

**Containment when the kill switch itself is suspect.** `enable_auto_trading = false` refuses all
three chokepoints independently of the kill row: C1's `auto_trading` rule
(`execution_guard.py:402`), C2's `auto_trading_disabled` (`strategy_paper_executor.py:1538-1539`),
and C3's `core_auto_trading_disabled` (`strategy_core_preflight.py:507-508`). That is the safe
default wherever the kill check is in doubt.

- **Entry verdict `failed`**, with the chokepoint outcomes and build stamp:
  - `allowed` or `error` on a chokepoint: that path may not stop under kill. Safe default: set
    `enable_auto_trading = false` until a re-run passes, and fix the path.
  - `kill_switch_row_missing`: the switch cannot be activated (`ops_monitor.py:1136-1137`), and
    every guard reads the missing row as active (`execution_guard.py:331-336`), so entries are
    refused. Safe default: leave it; restore the row before the next drill.
  - `sandbox_committed`: a probe published the drill's activation. The notice states the recorded
    `kill_active_at_verify`. Safe default: leave the switch as it is now, and fix the probe before
    the next drill.
  - `exception`: the drill could not complete, so no chokepoint is proven this month. Safe default:
    none needed for trading; re-run the drill manually after reading `failure_detail`.
  The drill never changes trading state on failure: a self-test that halts trading on its own bug
  would be an unexplained halt. The notice says what to do instead.
- **Book verdict `defects`**, with the counts and ownership ids. A missing stop breaks the
  operator's SL/TP rule. Safe default: none; the position manager's stop repair runs every 5-minute
  cycle (`strategy_position_manager.py:1798-1812`), and a defect that survives two drills is a bug to
  file.
- **Entry verdict `incomplete` with `runtime_config_corrupt` or `core_runtime_config_corrupt`**: a
  configuration-health verdict. Entries already refuse on that path; safe default: leave it and
  restore the `runtime_config` row. `auto_trading_disabled` does not page: nothing can enter.
- **Occurrence overdue:** occurrence o (each day-3 06:12 UTC from the first after slice 3 deploys)
  is satisfied by any event, scheduled or manual, with `started_at` in `[o, o + 24 h)`. An
  unsatisfied occurrence pages once at `o + 24 h`, keyed by o. Safe default: none for trading; run
  the drill manually.

The 24 h grace is a constructed choice: one day lets a catch-up fire after an overnight outage.

## Safety model

- The drill's only write to the kill row happens inside a non-autocommit transaction that is always
  rolled back, with a server-enforced 30 s ceiling. A crash, a timeout or a killed process aborts it.
  If any probe nevertheless commits, the committed result is the switch on, never off, and the run
  records and pages it.
- Under the locks it holds, writes that take the core advisory key and an operator activation wait
  at most the transaction's life, and are never overwritten by it. Its lock order matches
  `activate_kill_switch` and `deactivate_kill_switch`, the only writers of the row
  (`app/api/config.py:301-320` is their only caller).
- It commits only its own tables. It calls no broker method.
- `.claude/CLAUDE.md` § Objective and risk posture: *"The execution guard, kill switch and #2844
  sandbox stay fail-closed under either mode."* The drill cannot clear the switch. The autonomy
  loop's standing prompt (not in this repo) says *"do not touch the kill-switch"* for the loop
  session; the loop neither runs the drill nor its acceptance. The acceptance's committed toggle is
  a supervisor or operator act through the normal endpoint.
- The clean-`origin/main` refusal covers entries only (`app/security/unattended_guard.py:149-156`);
  the drill is not an entry.

## Slices

1. **Service, schema, manual script, tests.** Migration; `app/services/kill_switch_drill.py`; the C2
   loader/decision split; `scripts/run_kill_switch_drill.py`; the time-to-flat function. Tests:
   - each outcome per chokepoint, including revert-probes that remove each kill check with the other
     gates clear (→ `allowed`), and C1's with auto-trading off (still `allowed`, since its kill rule
     is reached); C2 and C3 with auto-trading off → `other_refusal`; C3 with no mandate →
     `not_applicable`;
   - sandbox leaves the committed row and `runtime_config_audit` unchanged, including when
     evaluation raises; an autocommit connection is refused before any write;
   - each loader leaves the committed row unchanged as seen from a second connection;
   - a concurrent activation on a second connection waits, then commits, is intact afterwards, and
     is not reported as `sandbox_committed`;
   - observe mode writes nothing to the kill row, and handles a switch turned off before it looks;
   - executor-path tests with a recording broker stub: with the switch committed on, C1–C3 place no
     order;
   - C2's wrapper keeps today's behaviour on the corrupt-config branch (no commit, same code);
   - time-to-flat fixtures: session open, pre-open same day, weekend, holiday, `session_unknown`,
     N = 0, N = 21, n = 0, outstanding authority, mapping defects, non-demo position;
   - a structural test that the callers of the three entry methods equal the census above.
   Codex checkpoint 2.
2. **Acceptance, attended (loop-ineligible).** On dev, from `~/Dev/eBull`: one sandbox run with
   `passed`; then an observe run inside an operator-made activation (switch on through
   `POST /config/kill-switch`, drill, review), also `passed`, with `observed_activated_by` matching
   the activation. The switch is turned off only after that review, and stays on if the observe run
   did not pass. Both event ids, outcomes and the estimate go on #3614.
3. **Schedule and watch.** Job registration and the notices above. Merged only after slice 2's
   evidence is on #3614, so the first scheduled fire follows an attended run.

## Known limits

- The drill covers the census; the structural test keeps the census honest only for callers of
  `BrokerProvider` entry methods.
- The estimate rests on one observed close of a different trigger until more accrue, and on an
  optimistic reading of the write cap.
- Recommendation EXIT and core `sell_core` stay blocked under kill. Operator-initiated flattening
  under kill means one `operator_close` request per position through the API, demo only. An operator
  flatten-all action with a UI surface is a separate follow-up ticket.
- The book check reads cached broker state. A live check would use the broker's close-eligibility
  endpoint (`etoro-api.md` § Stable facts); it is not in this item.
- Sandbox mode cannot observe cross-session visibility of a committed activation; observe mode during
  the attended acceptance does.

## Checkpoint log

**Round 1 (42 findings, `var/research/3614_item4/ckpt1_round1.txt`).** The first draft committed a
real toggle and restored it by compare-and-swap. Findings 1, 3–5, 8, 10, 31–33 and 36 were races
and gaps in that design (activation overwriting an operator's kill, a non-unique activation
identity, restore outcomes, audit-id availability, paging-suppression races and spoofing, crash
paging). They are resolved by the sandbox redesign: nothing commits, so there is nothing to restore,
suppress or reconcile. The rest were applied, and round 2 re-checked them.

**Round 2 (27 findings, `var/research/3614_item4/ckpt1_round2.txt`), all applied:**
- **1, 2:** non-autocommit sandbox connection asserted; loader transaction neutrality stated, tested
  and checked at runtime (step 7), with a fail-closed worst case.
- **3, 4:** `transaction_timeout` (PostgreSQL 17) and per-statement limits in both modes; the
  drill's session lock lives on the lock connection through recording, released in `finally`.
- **5:** the writes that wait are enumerated.
- **6:** observe mode re-reads the row under its locks; off and missing outcomes defined.
- **7, 8:** C1 classified over its full rule list; C3's auto-trading and runtime masking cited at
  `:503-508`.
- **9:** C2's corrupt-config branch keeps today's no-commit behaviour.
- **10:** C3's instrument is loaded under the mandate lock; no mandate is an outcome.
- **11:** run-level failure outcomes, error preservation, recording-failure coverage.
- **12, 18:** one `REPEATABLE READ` snapshot, `snapshot_at` as t0, authority identities stored.
- **13, 14:** demo-only route recorded; mapping defects and non-demo positions null the estimate;
  every mapped broker row stored.
- **15, 16:** R described as an unadjusted historical duration; concurrent exits excluded, not
  claimed monotone.
- **17:** the scope decision is stated at the top.
- **19:** the observe run must pass; the switch stays on if it does not.
- **20, 22:** notices read current state, are keyed and retained; occurrences have identities and
  half-open windows.
- **21:** safe defaults per failure kind.
- **23:** slice references fixed.
- **24:** governing excerpts cited; 24 h, 400 days, 30 s and the estimator labelled as constructed.
- **25:** the book query selects the symbol and left-joins broker rows.
- **26, 27:** wording fixed; the log line carries the sandbox in its own reason.

**Round 3 (19 findings, `var/research/3614_item4/ckpt1_round3.txt`), all applied:**
- **1, 2:** step 7 checks for this run's token in the audit, not the row's state; current state is
  recorded separately as `kill_active_at_verify`.
- **3:** surrogate key on positions, with separate uniqueness for mapped and unmapped rows.
- **4:** `not_run` verdicts, `snapshot_unavailable`, verification inside the `finally`.
- **5:** outcomes classified by whether the kill check was reached; C1's `allowed` and `error` cases
  defined.
- **6:** no mandate is `not_applicable`, not `other_refusal`.
- **7:** kill evidence for C2/C3 narrowed to active-or-missing; the drill's own read is stored.
- **8, 9:** notices evaluated per event and occurrence since deploy; keys never expire.
- **10:** a safe default per failure; `enable_auto_trading = false` named as the containment, with
  its three code sites.
- **11:** `idle_session_timeout` bounds a hung run's drill lock.
- **12, 16:** wording on waiters and on loader transaction behaviour.
- **13:** t0 is `clock_timestamp()` from the snapshot's first statement.
- **14:** closes are slotted inside the session; overflow moves to the next open.
- **15:** mandate event id and revision stored.
- **17:** the kill-toggle notice's "exits still run" is corrected in slice 3.
- **18, 19:** governing excerpts quoted; baseline values labelled as the dated output; schema grep
  stated.

Round-1 finding 34 (persistent incompleteness) is now handled differently: C1 tests the kill rule on
every completed run, and the only masking codes either refuse all entries (auto-trading off) or page
as a configuration-health verdict (corrupt runtime config).
