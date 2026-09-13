# Core/cash process-restart acceptance — #2949, round 1

**Tested commit:** `79720491969d1405c08193e23988c43ee3988d4c`
**Harness:** `tests/test_2949_core_restart_recovery_db.py`, `tests/fixtures/core_restart.py`,
`tests/fixtures/core_restart_child.py`
**Broker:** deterministic file-backed fake. **Database:** the per-worker disposable test DB.
**No broker mutation, no credentials, no order.** Every "order" in this document is a row in a
JSON file written by the fixture.

Reproduce:

```bash
docker compose --profile test up -d postgres-test
uv run pytest tests/test_2949_core_restart_recovery_db.py -v -o addopts=''
# expect: 9 passed
```

⚠ **Check the count, not the exit status.** `ebull_test_conn` *skips* when the test database is
unreachable, so a run with no Postgres exits `0` with nine skips and looks identical to a pass at
the shell. Verified here: 9 passed, and green on three consecutive runs under random ordering.

## What the harness is

A real process boundary, not a simulated one. A child process
(`tests/fixtures/core_restart_child.py`) runs `execute_core_rebalance` against the test
database and `SIGKILL`s itself at a declared fault point; the pytest parent is the restarted
engine. An in-process double cannot stand in for this — Python would unwind the stack, psycopg
would close the connection cleanly and `finally` blocks would run, and a crashed engine gets
none of that. The parent asserts the child died with `-SIGKILL` before reading any state, so a
scenario whose fault silently failed to fire cannot pass.

The broker's accepted orders live in a file, fsynced (data and directory entry) before the fault
can fire. #2949 required this and the reason is not tidiness: a response mock dies with the
engine, so every restart scenario would pass by amnesia instead of by recovery. Because the file
survives, *"did the broker see two economic orders?"* is answerable after our process is gone,
and that is what the exactly-once claim reduces to.

Its account snapshot is **derived from its own orders** rather than constant, so a recovered
position is visible to the next evaluation and the replay scenario is about the allocator rather
than about the fixture. (A constant snapshot would not have passed silently — it would fail the
ownership join or the no-growth assertion — but it would have made the scenario test nothing
about convergence.) Pending, unfilled amounts are netted from cash *and* added to invested, which
is what `etoro_broker.py:1506-1524` does; an earlier draft netted them from cash only, which
shrank equity on an unfilled order and would have let the drawdown observation fire on one.

### A prior gap this closed on the way past

**No test had ever run `execute_core_rebalance` against a database.**
`tests/test_2603_core_executor.py:43` is a `FakeConn` that matches SQL prefixes and returns
canned ids; `tests/test_2603_core_mandate_api.py` mocks the executor out of the route. So the
executor's durable-authority transaction (`app/services/strategy_core_executor.py:460-588`) had
never been accepted or rejected by the schema it writes to. It is accepted — that is the
baseline test — but two of this suite's assertions were wrong on first run in ways only a real
run could show: a CHECK constraint on the reconciliation payload hash, and two refusal-precedence
facts recorded under G-3 below.

`SELECTED_CORE_INSTRUMENT_ID` is `None` on `main` while #2833's five-trading-day verdict runs, so
the harness publishes a selection in its own process. That is a property of the declaration, not
of the recovery machinery, and the parent restores the constant via `monkeypatch`.

## Scenario outcomes

Classification uses #2949's three terms. **A safe stop is valid behaviour and is not called
automatic recovery.**

"Broker mutations" counts orders the fake broker **accepted and recorded**. On scenario 2 the
mutation method is entered and killed at its first statement, before anything is recorded — which
is what models a crash between the commit and the request reaching the broker. Zero accepted, not
zero calls.

| # | Fault | Broker mutations | Durable state after crash | Recovery outcome | Classification |
|---|---|---|---|---|---|
| 1 | Killed inside the authority transaction | 0 | nothing: 0 trades, 0 orders, 0 reconciliation rows | next cycle submits exactly 1 order | **automatically recovered** |
| 2 | Killed at the first statement of the broker call, before it accepts | 0 accepted | 1 trade `planned`, 1 order, no `broker_order_ref` | resume returns `core_order_reconciliation_not_found`; new entry refused `core_trade_in_flight`; **no path resolves it** | **safely stopped — then wedged (G-1)** |
| 3 | Broker accepted, response lost, engine died | 1 | 1 trade `planned`, order has no broker ref | exact-ID lookup on the request UUID resolves it: 1 order, 1 ownership claim, mutations still 1 | **automatically recovered, conditional on an attended caller (G-2)** |
| 4 | As 3, but the order is `Pending` | 1 | as 3 | first resume leaves it `pending` with 0 ownership; after the fill, 1 ownership, 1 order | **automatically recovered (same condition)** |
| 6a | Kill switch armed between acceptance and recovery | 1 | as 3 | reconciliation still reaches the broker (`lookup_calls == 1`) and resolves; ownership recorded | **automatically recovered** |
| 6b | Kill switch armed after a clean crash | 0 | nothing | new entry refused `core_kill_switch_active_or_missing`; 0 mutations, 0 orders | **safely stopped** |
| 8 | Replay a recovered cycle twice | 1 | 1 trade, 1 order, 1 ownership | both replays return `core_hold`; no growth in orders, trades, ownership or broker mutations | **automatically recovered** |

Scenario 2 is the one that matters. The broker genuinely never received the order, so the exact-ID
lookup misses; a lookup miss is correctly *not* treated as permission to resubmit
(`strategy_core_executor.py:263-268`), and the mutation counter stays at zero across three repeat
resumes. That half is exactly right. What is missing is any way out of the state it leaves.

## Gaps

### G-1 — a core authority the broker never received can never be resolved

Three facts, each asserted in
`test_scenario_2_stranded_authority_is_unreachable_by_any_unattended_caller`:

1. `reconcile_backlog` — the only unattended reconciliation caller, reached from
   `app/workers/scheduler.py::strategy_paper_cycle` — **excludes every core order**:
   `AND trade.core_rebalance_intent_id IS NULL`, `strategy_order_reconciliation.py:532`. Added
   without a comment or a spec line in `608dc879` (the #2603 execution PR).
   Measured **against a positive control**: with a non-core order in the same `unresolved` state
   in the same database, the identical call returns exactly that one and not the core order. The
   earlier draft asserted only that the call returned `()`, which a backlog broken to return
   nothing would have satisfied just as well.
2. The attended resume changes nothing on repetition — measured over three resumes with the
   broker's accepted-order count still zero. A terminal reconciliation state requires a
   *successful* lookup (`_apply_detail`), and the lookup cannot succeed for an order that was
   never submitted. `core_trade_in_flight` (`strategy_core_submission_gate.py:440`) then refuses
   every future core authority.
   ⚠ `_apply_detail` is not the only writer of a terminal state — `_submit_core_authority` writes
   `rejected` directly on a definite broker rejection (`strategy_core_executor.py:321`). That
   path belongs to a submission the process returns from, so it cannot rescue this authority, but
   the exhaustiveness claim needed correcting.
3. `enforce_reconciliation_slo` does **not** share the core exclusion, so the row nothing can
   resolve is the row it escalates, into
   `strategy_execution_blocks(source='order_reconciliation', active=true)`. That block is global
   across both arms. **It stops new strategy ENTRIES only** — reconciliation and owned-position
   management continue, existing positions stay closable, and clearing the block would not
   resolve the stranded authority.
   ⚠ **And the escalation is conditional.** `refresh_strategy_health` sets that same block to
   `active=false` whenever no *enabled paper deployment* supplies a policy
   (`strategy_paper_runtime.py:235-250`). The current configuration has none, so in a core-only
   world the scheduled path **clears** the block rather than raising it. The test drives
   `enforce_reconciliation_slo` directly, which is why it sees the block at all.

Net, stated at the strength the evidence supports: **a crash in the window between the authority
commit and the broker call leaves a core authority with no supported resolution path, and the
core arm then refuses every subsequent rebalance indefinitely.** In a core-only configuration it
does so *silently* — no block, no health signal, because the health gate that would report it is
switched off when no paper deployment is enabled. Once a paper deployment is enabled, the same
row additionally blocks new entries on the alpha arm.

The window is short but not theoretical: a deploy, an OOM kill or a `launchctl kickstart` landing
between two statements.

The fix is not "let it retry" — resubmission is the thing the design correctly refuses. It is a
bounded, audited way to *terminalise* an authority that the broker can be shown never to have
accepted, and it has to stay fail-closed: the discriminator must distinguish "the broker does not
have it" from "the broker has not answered yet", and eToro's `orders:lookup?referenceId=` is the
only evidence available. Scoped in #2961.

### G-2 — core recovery has no unattended caller

`resume_core_submission` has exactly one production caller, `app/api/strategies.py:3823`, inside
`rebalance_core_sleeve`. **Nothing schedules it.** Scenarios 3, 4 and 6a all recover correctly and
all three recover only because the harness called the service directly.

Two corrections to how this was first written, both of which matter for the fix:

- **"Requires a browser" is wrong.** The route requires an authenticated operator *session*, which
  a program can hold. The gap is the absence of a scheduled caller, not the presence of a human.
  Nor did this harness "play the part of that session" — it bypassed the route, its
  authentication, its credential decryption and its provenance check entirely, and calls the
  service functions directly. Whatever unattended caller is built will have to supply the parts
  the harness skipped.
- **It is not restart-specific.** The fault-free baseline also ends at reconciliation state
  `pending`. An ordinary, entirely successful core submission needs the same attended follow-up,
  so the core arm has no unattended reconciliation at all — a restart merely makes it visible.

Two candidate shapes — drop the `core_rebalance_intent_id IS NULL` exclusion, or add a core-scoped
scheduled caller that reuses the same services rather than duplicating the sizing/guard/
reconciliation logic. ⚠ Neither is free: the generic poller holds one broker without the core
arm's stored credential binding and does not take the core submission lock, so account provenance
and submission/reconciliation serialisation both need resolving first. Scoped in #2962.

### G-3 — the kill switch is outranked by two earlier refusals

`execute_core_rebalance` reads the kill switch inside `preflight_core_submission`
(`strategy_core_executor.py:525`), which sits behind the allocator's `hold` (`:493`) and the
submission gate's `core_trade_in_flight` (`:498`). Both earlier returns are correct — a hold
submits nothing, and an in-flight trade blocks entry regardless — so this is not a safety defect
and no exposure escapes.

It is an observability one, and it cost two wrong assertions in this suite before it was noticed.
⚠ Narrower than first written: the `rebalance_core_sleeve` route resumes an outstanding authority
*before* it ever calls the executor (`app/api/strategies.py:3822`), so an operator going through
the UI sees the reconciliation outcome, not `core_trade_in_flight`. The precedence is visible to a
caller that reaches `execute_core_rebalance` directly — which is exactly what an unattended caller
built for G-2 would do. Worth one line in the operator copy when that caller lands; not worth
reordering a precedence that is otherwise correct. Recorded here, no ticket.

## What was NOT run, and why

Declared before execution and listed here because a silently dropped scenario reads as a covered
one.

| Matrix item | Status | Reason |
|---|---|---|
| 5 — outage, then recovery with a backlog over the batch cap | **not run** | The core arm is excluded from `reconcile_backlog` (G-1.1), so it contributes nothing to the batch the cap applies to. The alpha-side behaviour this item describes is what #2948 fixed and tested. Re-run once G-2 lands and core orders are actually in the backlog. |
| 6 — credential rotation and mandate revocation between phases | **partial** | The kill-switch halves are run (6a, 6b). Credential provenance is guarded at `strategy_core_executor.py:514-523` and `app/api/strategies.py:3801` and has unit coverage; exercising a rotation across a process boundary needs a second credential pair in the seed. Mandate revocation needs no extra seeding and is a straightforward round-2 addition — it is deferred for time, not for difficulty. |
| 7 — rebalance sell | **not run, and cannot be** | `assess_core_broker_preflight` refuses `sell_core` as `core_close_side_cost_quote_unavailable` before any broker call (`strategy_core_broker_preflight.py:299`). There is no allocator-driven sell to fault-inject. It stays a named blocker, not a mocked pass — per #2949's own instruction. |
| 7 — position closure | **not run, but it exists** | ⚠ Correction to the first draft, which said closure could not be tested. `close_strategy_owned_position` (`app/api/strategies.py:3320`) → `manage_owned_position` → `_submit_close` is a working full-close path for an exact owned position. It is attended, it is a different lifecycle from the rebalance sell, and it is untested across a process boundary. Round 2. |
| Partial fills | **not run** | The fake broker's executions are whole. Ownership and reservation transitions under a partial fill *could* be tested without claiming any accounting tolerance; they are deferred for time. The accounting-tolerance half belongs to #2602. |

## What this does not prove

### Limits of the harness itself

Listed because they bound every classification in the table above, and a reader who takes
"automatically recovered" at face value should know what was and was not challenged.

- **The fault is an ENGINE death, not a host crash.** SIGKILL with Postgres and the broker still
  running. The OS page cache survives it, so the fsyncs in the broker fixture are belt-and-braces
  rather than load-bearing; the atomic `os.replace` is the part that matters.
- **The exit status proves a signal, not a location.** Each scenario asserts the resulting durable
  state, but nothing stamps the phase the child reached, so an unrelated early kill would satisfy
  scenario 1's empty-state assertions. The fault hooks are deterministic, which is why this is
  acceptable, not because it was measured.
- **Identity is barely challenged.** One order, one instrument, one position, no unrelated holding
  in the same instrument. Guards that resolve by exact id would not be distinguished here from
  guards that resolve by instrument.
- **Replay measures allocator convergence, not reconciliation idempotency.** Scenario 8 repeats
  the *evaluation* after one reconciliation; it never replays the reconciliation or the original
  submission authority. A reconciler that was not idempotent could pass it.
- **Assertions are on cardinality, not on accounting.** Order, trade and ownership counts, plus the
  broker's accepted-order count. Nothing asserts execution-ledger contents, reserved or released
  capital, or funding conservation. "Exactly once" here means exactly one accepted order and one
  ownership row, not a proved economic identity.
- **Fixture agreements by construction.** The double's filled-status vocabulary mirrors the
  reconciler's; the seeded eligibility verdict and policy versions are the production constants;
  the #2833 selection is force-published; executions are one unit, so the requested amount and the
  execution price coincide. These are controlled assumptions, not validated contracts.
- **Untested windows.** No kill during acceptance persistence, during reconciliation, during the
  ownership insert, or during recovery itself; no ambiguous COMMIT outcome; no concurrent worker;
  no delayed broker visibility; no second crash to show that recovered ownership is itself durable.
- **No adapter, scheduler or API coverage.** The real `EtoroBrokerProvider`, the scheduler's job
  wrapper and the FastAPI route are all out of the loop. Nothing here says the production startup
  path recovers; it says the *services* do, when something calls them.

### Beyond the harness

- **Nothing about profitability, sizing quality or instrument eligibility.** Every figure is
  synthetic and labelled plumbing-only; the trade amount comes from a fake cost quote applied to a
  seeded 1,000 pot.
- **Nothing about the real broker.** A deterministic double cannot show how eToro behaves on a
  lost response, a duplicate `referenceId`, or a lookup during an outage. #2949 keeps that as
  separate, account-specific demo evidence and this round does not touch it.
- **Nothing about #2602's accounting tolerance or #2844's consecutive-day acceptance.** Those
  remain the thresholds; this harness deliberately invents no readiness threshold of its own.
- **No green "ready" status.** G-1 alone is disqualifying for hands-off operation: an engine that
  can strand its own trading authority and has no way to clear it is not hands-off, it is
  unattended.

## Round 2 entry conditions

1. #2961 (G-1) landed, with the terminalisation discriminator fail-closed and audited.
2. #2962 (G-2) landed, so recovery has a caller that is not a browser.
3. Then re-run this suite plus matrix item 5, which only becomes meaningful once the core arm is
   in the backlog.

Round 2 is still fake-broker work. Account-specific demo evidence stays after it and stays
operator-attended, because its acceptance mutates broker state.
