# Core/cash process-restart acceptance — #2949

**Round 1 tested at** `79720491969d1405c08193e23988c43ee3988d4c`.
**Round 2's production base is** `ef93efcc3b7eeabd0d4984300ddea80dabb1571c` (`main`); its harness
is the branch this document lands on, so reproducing it needs both. Round 2 is
[a separate section below](#round-2--matrix-5-and-the-exit-lifecycle). Everything between here
and that section is round 1 as written, except where a line says otherwise.

**Harness:** `tests/test_2949_core_restart_recovery_db.py`,
`tests/test_2949_core_close_recovery_db.py` (round 2), `tests/fixtures/core_restart.py`,
`tests/fixtures/core_restart_child.py`
**Broker:** deterministic file-backed fake. **Database:** the per-worker disposable test DB.
**No broker mutation, no credentials, no order.** Every "order" in this document is a row in a
JSON file written by the fixture.

Reproduce:

```bash
docker compose --profile test up -d postgres-test
uv run pytest tests/test_2949_core_restart_recovery_db.py \
              tests/test_2949_core_close_recovery_db.py -v -o addopts=''
# expect: 32 passed  (23 restart + 9 close, at round 5)
```

⚠ **Check the count, not the exit status.** `ebull_test_conn` *skips* when the test database is
unreachable, so a run with no Postgres exits `0` with skips and looks identical to a pass at
the shell.

⚠ **The count is not the sum of what each round added.** Round 1 shipped 9 tests in the restart
file; #2962 and #2961's sessions added two more before round 2 started (the unattended
lost-acceptance recovery and the stranded read-surface flag); round 2 added one matrix-5 test
plus 3 in the close file (15); #2961's terminalisation fix then added 6 more to the restart file
and #2979's added 1 to the close file (22); round 3 adds the three matrix-6 tests (25), round 4
the two matrix-5 ones (27), and round 5 the five non-crash close failures (32). Round-1 figures
quoted below are left as they were written.

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

⚠ **Round 3 update to the paragraph above:** #2833's verdict opened on 2026-09-18 (`110b4981`:
`pass`, 3417 SPY.RTH), so the constant is no longer `None` and it now equals the harness's own
`CORE_INSTRUMENT_ID` by coincidence of the candidate set. The patch stays for the reason it was
written — the harness must measure the recovery machinery, not whatever the declaration currently
says — but it is no longer what makes these tests runnable at all.

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
reconciliation logic. Scoped in #2962.

> ⚠ **Amended 2026-09-13, same session.** This paragraph originally said the blocker was account
> provenance — that the generic poller holds a broker without the core arm's stored credential
> binding. **That is wrong**, and a Codex checkpoint-1 pass on #2962's spec killed the design built
> on it. `sql/373_core_unresolved_credential_guard.sql` is a DB trigger that refuses to revoke or
> delete either credential while any core order referencing the proof is non-terminal, so the pair
> an unresolved core authority names is guaranteed live and a provenance predicate would be dead
> code.
>
> **The real reason the exclusion exists is serialisation**, and it is recorded in #2948's own spec
> (`2026-09-13-reconciliation-backlog-fairness.md:236-239`): `_record_failure` can overwrite a
> terminal row under concurrency, accepted there on the grounds that production has exactly one
> scheduled reconciler and *"a second reconciler would be a new deployment decision"*. The core arm
> already has a second one — the attended route — which does not race the job only because the job
> excludes core orders. Removing the exclusion makes core the first order class with two
> reconcilers. **#2964** is the prerequisite; #2962 is blocked on it.
>
> Also corrected: the G-1 claim above that the SLO "still sees it, so the condition surfaces" holds
> only where an enabled paper deployment supplies a policy — which is the same conditional-escalation
> point already made in G-1.3, and which the first #2962 draft contradicted.

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

⚠ **Two rows of this table moved in round 2** — matrix 5 and matrix 7's position closure are now
run, with outcomes in [the round-2 section](#round-2--matrix-5-and-the-exit-lifecycle). The rest
of the table still stands.

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
2. **#2964 landed** — the reconciliation write path made safe for a second reconciler. Discovered
   after this report was first written, and it is the true head of the chain: #2962 is blocked on
   it, not the other way round.
3. #2962 (G-2) landed on top of it, so recovery has a scheduled caller.
4. Then re-run this suite plus matrix item 5, which only becomes meaningful once the core arm is
   in the backlog. ⚠ Partial fills have a known defect to reproduce first — **#2965**: a
   partially-filled core entry claims ownership while `pending`, and
   `load_engine_capital_authority` raises on that combination, taking down the core executor, the
   mandate writer and `core_rebalance_observation`. This suite's fake broker returns no executions
   while pending, which is why it did not surface here.

Round 2 is still fake-broker work. Account-specific demo evidence stays after it and stays
operator-attended, because its acceptance mutates broker state.

---

# Round 2 — matrix 5 and the exit lifecycle

**Production base:** `ef93efcc3b7eeabd0d4984300ddea80dabb1571c`
**Added:** `tests/test_2949_core_close_recovery_db.py` (3 scenarios) and one matrix-5 scenario in
the round-1 file. Same fake broker, same disposable database, still no broker mutation.

## What round 2 does and does not cover

Entry condition 1 above — **#2961 landed** — is NOT met, and is not met by anything here. It is
blocked on an attended demo order settling whether `orders:lookup?referenceId=` covers an order
the broker never accepted, and that acceptance mutates broker state, so it stays out of an
unattended run. Round 2 therefore took only the items that conditions 2 and 3 (#2964, landed as
`fb18d2d1`; #2962, landed as `d33b3124`) unblock:

| Round-2 item | Status |
|---|---|
| Matrix 5 — **over-cap half**: a core row behind the batch cap | **run** |
| Matrix 5 — **outage half**: a backlog accumulated while the broker was unreachable, then drained | **not run** — it exercises the cooldown arithmetic, not the rotation, and is a separate scenario |
| Matrix 7 — position closure across a process boundary | **run** (3 scenarios) |
| G-1 **terminalisation** acceptance | **not run** — blocked on #2961. ⚠ G-1's *regression* tests do still run every time, in the round-1 file; what is missing is acceptance of a fix that does not exist yet |
| Matrix 6 credential rotation, mandate revocation | **still not run** — unchanged from round 1 |
| Matrix 7 rebalance SELL | **still blocked** by `core_close_side_cost_quote_unavailable` |
| Partial fills | **still not run** — #2965 owns the defect to reproduce |

⚠ **One half of G-1 moved while round 2's base was being assembled, and this report should say so
rather than leave round 1's text standing.** G-1.1 recorded that the stranded core authority was
*silent* because `refresh_strategy_health` cleared the reconciliation block when no paper
deployment declared a policy. `ca7e4ee1` (#2961's visibility slice) changed that block to key on
the presence of unresolved order identity, so a core-only configuration no longer clears it. The
observability half of G-1 is therefore closed; the terminalisation half is not, and is still what
makes G-1 disqualifying.

## Matrix 5 — a core row behind the batch cap

`test_scenario_5_a_core_row_behind_the_batch_cap_is_reached_within_the_declared_bound`.

#2962's PR asserted only the weaker two-pass property on a backlog of one: a resolved core row
leaves the backlog. This is the over-cap case. Five alpha rows are seeded **first**, so the core
order is last in the queue, and the cap is two. `reconcile_backlog`'s docstring declares that a
due row is selected within `ceil(due_rows / limit)` completed cycles; with six due rows and a cap
of two that is three cycles, and the core row is selected in the third.

**Outcome: automatically recovered.** The declared bound holds for the core arm.

Two design points, because the test is worth nothing without either:

- **The competitors have to keep competing.** Each is registered on the broker double as an
  accepted-but-`Pending` order, so every poll leaves it `pending` — a progress state, which
  `reconcile_backlog` exempts from the exponential cooldown. Competitors that fell to `not_found`
  would earn the cooldown, drop out of the selection and let the core row through for a reason
  that has nothing to do with the rotation.
- **Non-vacuity is asserted, not assumed.** Cycle 1 must NOT contain the core order. Verified by
  revert-probe rather than by argument: replacing the `ORDER BY state.last_attempt_at ASC NULLS
  FIRST, ...` with the pre-#2948 `ORDER BY state.first_unresolved_at, state.order_id` fails the
  test with `assert 6 in [1, 2]` — the core row is never reached at all. The probe was reverted;
  the branch touches no production file.

## Matrix 7 — position closure across a process boundary

`tests/test_2949_core_close_recovery_db.py`. The lifecycle is `close_strategy_owned_position`
(`app/api/strategies.py:3323`) → `manage_owned_position` (`strategy_position_manager.py:844`) →
`_submit_close` (`:758`). The harness calls `manage_owned_position` directly, exactly as round 1
called the executor directly: the route's authentication, credential decryption and provenance
check are skipped, and that skip bounds the evidence.

**Two structural facts, read from source before the first run rather than discovered by it.**
Both shape every outcome below.

1. `_submit_close` writes the `orders` row with `execution_origin='strategy'` and links it
   `purpose='exit'`, but writes **no** `strategy_order_reconciliation_state` row.
   `reconcile_backlog` selects from that table, so an EXIT order is structurally invisible to the
   scheduled reconciler #2962 just extended to the core arm. Exit recovery rests entirely on
   `_resume_operation` (`:474`), reached from `strategy_paper_runtime.py:493`.
2. In `_resume_operation`, `landed` is hard-coded false for a close (`:520` —
   `operation["operation_type"] != "close"` is the first conjunct). ⚠ Its own comment gives the
   reason as "there is no edit/close lookup by request UUID", and that is established about **this
   implementation** — `get_demo_close_order` takes an `order_id` and nothing else
   (`app/providers/broker.py:700`). Whether eToro *could* offer one was not checked against the
   portal here, so treat it as a property of our adapter, not of the broker.

Fact 2 is why 7a and 7b produce the **same row on our side** and have opposite consequences.

| # | Fault | Broker | Our state after the scheduled pass | Classification |
|---|---|---|---|---|
| 7c | none — the child completes and stops at `submitted` | close accepted, 1 call | resumed by a process that never submitted it: `applied`, ownership `released` with `release_reason='operator_close'`, trade `closed`, exit order `filled` | **automatically recovered** |
| 7a | kill inside `close_demo_strategy_position` before anything is recorded | 0 closes **accepted** (the verb is entered and dies at its first statement), position untouched | `reconcile_required` / `crash_before_submission_identity`; ownership still active; allocator still `core_hold`; a fresh close request succeeds and places exactly one close | **safely stopped, recovered by re-request** |
| 7b | kill after the broker recorded the close, before `persist_response` | 1 close call, position gone | `reconcile_required` / `crash_before_submission_identity` — **identical to 7a** — ownership permanently active, `execute_core_rebalance` **raises** | **safely stopped at the broker, wedged in accounting — #2979** |

### 7c is the control, and it is not optional

Without it, a scenario that released no ownership would be indistinguishable from a lifecycle that
cannot release ownership at all. `close_calls == 1` is the load-bearing assertion in it: a resume
that re-closed would also end with the ownership released and the trade closed.

### 7a — the crash costs the request, not the position

The scheduled pass terminalises the orphaned intent without resubmitting, the position is
untouched, and the accounting join still resolves — so the core allocator keeps working and
returns `core_hold`. The exit stays reachable: an explicit re-close places exactly one close.

⚠ One observability cost, recorded and not ticketed: the trade is left at `reconcile_required`
even though nothing is actually unresolved. The state is conservative in the safe direction, and
distinguishing it requires exactly the discriminator fact 2 says does not exist.

### 7b — the finding, filed as #2979

The broker executed the close; the engine died before anything on our side recorded its identity.

What is safe: no second close reaches the broker, on the resume pass or on a fresh close request
(`close_calls` never leaves 1), and nothing invents a fill or releases ownership on an unverified
assumption.

What is wedged:

- `strategy_position_ownership` stays `active` naming a position the account no longer carries,
  and nothing terminalises it — the exit order is invisible to `reconcile_backlog` (fact 1), and
  every later `manage_owned_position` pass returns `owned_position_missing` while releasing
  nothing.
- `resolve_engine_capital_usage` refuses that join by design
  (`app/services/strategy_engine_capital.py:329`) and `strategy_core_executor.py:528` wraps the
  refusal into `StrategyCoreExecutionError`. The core allocator does not refuse politely, it
  **raises**, on this and every subsequent cycle, with no operator-visible reason code.

- **It is invisible to the reconciliation health surface as well.** Both `enforce_reconciliation_slo`
  and the no-policy block `ca7e4ee1` added count rows in `strategy_order_reconciliation_state`
  (`strategy_paper_runtime.py:287` reads an `unresolved` count over that table). Fact 1 says an
  EXIT order has no row there, so the wedge raises no health block either — it is silent in the
  same way G-1.1 used to be.

⚠ The fail-closed behaviour is correct in isolation. The defect is that no path exists to resolve
it at all — the same shape G-1 (#2961) records for the entry side, arrived at from the exit side.

⚠⚠ **"Permanent" is structural, not experimental**, and this is round 1's G-1 lesson restated. The
test repeats the scheduled pass three times and the state does not move, which shows the loop is
stable — not that it is eternal. What supports permanence is that the only reader which could
terminalise the ownership (`_resume_operation`) has already written a terminal operation row, and
the only other scheduled reconciler cannot see an EXIT order at all.

## What round 2 adds to "what this does not prove"

Round 1's limits all still bind. These are specific to this round.

**The close lifecycle:**

- **The close double has no partial close and no pending close.** `close_demo_strategy_position`
  always fills. A close that the broker accepts and then only partially executes is untested, and
  it is the shape #2965 is about on the entry side.
- **Only CRASH failures are exercised.** A definite rejection, an uncertain transport
  (`BrokerPositionMutationUncertain`, `strategy_position_manager.py:802-825`), a malformed
  acceptance, a lookup outage and a rejected close all take materially different branches and none
  of them is driven. ⚠ Note also that an unavailable portfolio blocks even a known-id close lookup,
  because `_exact_broker_position` runs first (`:515`).
- **Three crash windows inside the close are untested**: inside the close-intent transaction;
  between the two normalised acceptance updates; during the lookup-response persistence; and
  between the order-fill, ownership-release and trade-close writes in `_finish_close`. Splitting
  any of those into separate commits could pass this suite.
- **One of those windows is not merely untested, it is a candidate half of #2979.** The real
  adapter persists the raw acceptance *before* parsing its order id, so a crash just after that
  callback leaves durable close identity in `orders.raw_payload_json` which `_resume_operation`
  never reads. The fake's raw payload does not carry eToro's `orderForClose.orderID` at all, so
  this harness could not have found it — it is recorded on #2979 rather than claimed here.
- **`_resume_operation`'s edit branch is untouched.** Only `operation_type='close'` is exercised;
  the stop/take-profit resume path is exempt on the core arm by construction and was not driven on
  the signal arm.
- **The double does not validate close targets.** It records a filled close without checking that
  the position exists or that the instrument matches, and it reports `get_portfolio().available_cash`
  as zero while its risk snapshot derives cash properly. Those adapter and account contracts are
  unchallenged here.

**Matrix 5:**

- **Only the over-cap half of item 5 is run**, as the table above says. No outage, no accumulated
  `error` / `not_found` backlog, no elapsed retry delay, no restored service.
- **One core row and five competitors.** It establishes the declared bound at that shape, over two
  full rotations. It says nothing about contention: `StrategyReconciliationBusy` skips leave
  `last_attempt_at` alone, which `reconcile_backlog` notes degrades the bound to
  `ceil(due / (limit - b))` — that formula is in an implementation comment at the end of the
  function, not in the docstring, and it is undefined at `b == limit`, where an all-busy batch
  returns empty and is indistinguishable from an empty backlog. None of that is exercised.
- **Its accounting assertions are aggregate.** Global active-ownership count and per-order
  reconciliation state, not the exact trade-to-position mapping, the execution rows, or conserved
  commitment. Corrupted ownership with the right cardinality would pass.

## Round 3 entry conditions

1. **#2961 landed.** Unchanged, and still the disqualifying one: an engine that can strand its own
   trading authority and has no way to clear it is not hands-off.
2. **#2979 landed**, so a close the broker executed can be accounted for.
3. Then: matrix 6's credential rotation and mandate revocation, partial fills (#2965 first), and
   the contention shape of matrix 5.

**No green "ready" status.** Round 2 moved the exit lifecycle from untested to classified and
found one new wedge; it removed none.

## Round 3 — matrix 6, and the two entry conditions that closed on other PRs

Entry conditions 1 and 2 were both met before this round started, and **both were accepted on
the PRs that fixed them** rather than here:

| entry condition | landed | acceptance |
| --- | --- | --- |
| #2961 — terminalise an authority the broker never received | `971a7883` | `test_scenario_2c_…`, plus the after-marker, live-submitter, nested-resume, terminal-durability and marker-contract controls, all in this module |
| #2979 — account for a close the broker executed | `a555fe2b` | `test_scenario_7d_…` in the close module |

So round 3's own work is **matrix item 6**, which rounds 1 and 2 both recorded as not run.

### 6c — mandate revoked between broker acceptance and recovery

`test_scenario_6_mandate_revocation_between_phases_stops_entry_not_reconciliation`. The twin of
the kill-switch half, and the same invariant: **a stop must not strand an order it can no longer
authorise.** Revoked after the broker accepted, before recovery. Reconciliation still reaches the
broker (`lookup_calls == 1`), resolves, records exactly one ownership, and leaves broker
mutations at 1.

It holds **by construction**, not by a check — `resume_core_submission` never loads the mandate
(`strategy_core_executor.py:387-392`). Asserted anyway, because "by construction" is a property
of today's call graph: a mandate read added to the resume path would convert a revocation into
unaccounted broker exposure and nothing else in the suite would notice. The revert-probe confirms
it — inserting a mandate check into `resume_core_submission` fails this test.

### 6d — a revoked mandate refuses a clean re-entry, and the refusal is a RAISE

`test_scenario_6_mandate_revocation_refuses_a_clean_re_entry_after_a_crash`. No new exposure: 0
mutations, 0 orders, 0 intents.

⚠⚠ **The asymmetry is the finding.** The kill switch returns an audited
`refused` / `core_kill_switch_active_or_missing` verdict. A revoked mandate raises
`StrategyCoreExecutionError("an enabled core mandate is required")` at
`strategy_core_executor.py:596`, before any decision row is written. Both fail closed, so nothing
is unsafe — but only one leaves an operator-visible reason for why the sleeve stopped, and an
incident reader looking for the revocation they just made will not find it in the decision audit.
Recorded, not fixed: writing an audited refusal is a behaviour change to the executor and belongs
to #2603's surface, not to an acceptance round.

### ⚠⚠ 6d's first version could not tell the two halves apart, and only the probe said so

The gate is `mandate is None or not mandate.enabled or mandate.core_instrument_id is None` —
**three conditions behind one message.** The first `revoke_core_mandate` helper wrote
`enabled=FALSE` *and* `core_instrument_id=NULL`, so the refusal fired on either half and the test
could not distinguish them. A revert-probe that deleted `not mandate.enabled` from the gate
**still passed**.

It is also not what the writer does: `validate_core_mandate` refuses only `enabled AND instrument
IS NULL` and otherwise passes the caller's instrument through
(`strategy_core_mandate.py:239-242`), so "turn the sleeve off, keep the configuration" is the
likelier operator revocation. The helper now retains the instrument, and the same probe fails.

### 6e — credential rotation is held until the core order resolves

`test_scenario_6_credential_rotation_is_refused_until_the_core_order_resolves`.

Round 2 deferred this as needing "a second credential pair in the seed". That is true of
simulating a full rotation and **false of the invariant**, which is guarded in the database:
`prevent_unresolved_core_credential_removal` (`sql/373`) refuses to revoke either credential
named on the eligibility proof of a core order whose reconciliation state is not
`resolved`/`rejected`. A crash after broker acceptance is exactly what leaves one, so the process
boundary is what makes the guard reachable.

Three arms:

1. revoking the proof's `api_key` while unresolved → refused, `23503`, message *"credential is
   required by an unresolved core order"*;
2. an unreferenced credential → revoked freely in the same state. **The control is not optional**
   — without it a trigger that refused every revocation would pass identically, and that is a
   worse defect than the one being guarded;
3. after reconciliation resolves, the same revocation succeeds. The guard is a hold until
   terminal, not a permanent pin.

⚠ **The hold covers the whole rotation, not half of it.** `broker_credentials_unique_active`
(`sql/019`) is unique on `(operator_id, provider, label, environment) WHERE revoked_at IS NULL`,
so an operator cannot pre-insert the replacement and swap: revoke-then-insert is the only
available order and `sql/373` blocks its first step. That is also why arm 2's control has to
belong to a second operator — the harness operator cannot hold a second active demo `api_key` at
all. Confirmed by probe: disabling the trigger fails arm 1.

### What round 3 does NOT cover

| item | status |
| --- | --- |
| Matrix 6 — a full rotation (revoke, insert replacement, re-prove eligibility, resume) | **not run.** The invariant above says it cannot start while unresolved; what happens to an eligibility proof pointing at revoked credentials *after* resolution is a different question and belongs with #2603's revalidation slice |
| Matrix 5 — outage half (backlog accrued while the broker was unreachable, then drained) | **still not run** — cooldown arithmetic, a separate scenario from the rotation shape |
| Matrix 5 — contention | **still not run** |
| Matrix 7 — rebalance SELL | **still blocked** by `core_close_side_cost_quote_unavailable` |
| Partial fills | **still blocked** — #2965 needs an attended partial fill, which is `loop-ineligible` |
| Non-crash close failures | **still not run** — definite rejection, uncertain transport, malformed acceptance, lookup outage |

**Still no green "ready" status.** Round 3 closes matrix item 6 and records one auditability
asymmetry; it removes no blocker.

## Round 4 — matrix 5's two remaining halves

Both were deferred for time in earlier rounds, not blocked.

### 5b — the outage half

`test_scenario_5_an_outage_backlog_drains_after_the_cooldown_not_after_the_broker_returns`. The
broker double gains an `unreachable` switch whose `lookup_order` raises `BrokerOrderLookupError`
— the transport class, which `reconcile_strategy_order` maps to `error` / `broker_lookup_error`
(`:988-994`). Deliberately **not** `BrokerOrderNotFound`: that is a statement *about* an order and
drives a different state. An outage is the absence of an answer, not an answer.

The arc: three due rows (one core, two alpha) → outage → every row `error`, and the double's own
`lookup_calls` counter shows the reconciler did reach out → an immediate second cycle returns
**empty** because the cooldown bites → the broker comes back and the cycle is **still empty** →
`last_attempt_at` backdated past the cap, and the backlog drains.

⚠⚠ **The middle step is the finding, and it is easy to state backwards: recovery is gated on the
cooldown elapsing, not on the broker returning.** That is correct — nothing tells us the broker is
healthy except a successful poll, and polling to find out is the hammering the cooldown exists to
stop — but an operator watching a restored broker and an idle backlog needs it written down. At
the default constants the wait is up to `RECONCILIATION_RETRY_CAP_SECONDS` (3,450 s ≈ 57 min).

⚠ Each empty return is asserted against a live count of still-due rows. An empty
`reconcile_backlog` is the same value whether the backlog is empty or every row is cooling, so
without that count the scenario would pass against a backlog that had simply resolved everything.
Time is advanced by backdating `last_attempt_at`, not by shrinking `retry_base_seconds` — a
smaller base would exercise a parameter no caller uses and would not show the DEFAULT cooldown
ever releasing.

### 5c — the all-busy contention shape, and a named ambiguity

`test_scenario_5_an_all_busy_batch_returns_empty_and_keeps_every_rows_place`. Round 1's report
recorded this and nothing had driven it: with `b` busy rows the declared bound degrades to
`ceil(due / (limit - b))`, and **an all-busy batch returns empty, which is indistinguishable from
an empty backlog at the call site** — `skipped_busy` is logged and is not in the return value.
Both rows' locks are held on a second connection; the call returns `()` with two rows
demonstrably still due, and returns both the moment the locks are released.

The half with teeth is the second: `StrategyReconciliationBusy` is caught and `last_attempt_at` is
deliberately left **unchanged**, so a row skipped for somebody else's lock keeps its place at the
FRONT of the next rotation. Asserted on the stored timestamps — under
`ORDER BY last_attempt_at ASC NULLS FIRST` a contended row whose clock was advanced would silently
lose its turn to every row behind it, and nothing else in the suite would notice.

### Round 4's probes

| probe | prediction | result |
| --- | --- | --- |
| neutralise the cooldown clause in the selection SQL (`TRUE OR …`) | 5b fails | 5b FAILED, nothing else |
| advance `last_attempt_at` in the `StrategyReconciliationBusy` branch | 5c fails | 5c FAILED, nothing else |
| unmodified control | all pass | 23 passed |

### Still not covered after round 4

A full rotation past `sql/373`'s hold (belongs with #2603's revalidation slice, `59ee2fc6`) ·
non-crash close failures (definite rejection, uncertain transport, malformed acceptance, lookup
outage) · matrix 7's rebalance SELL, still blocked by `core_close_side_cost_quote_unavailable` ·
partial fills, still blocked on #2965's attended partial fill.

**Still no green "ready" status.** Matrix items 5 and 6 are now complete; item 7 is not, and the
two open blockers are unchanged.

## Round 5 — the non-crash close failures

A different axis from every round before it: **no `SIGKILL`, no restart, one live process
throughout.** What varies is how the broker ANSWERS. These are what #2603's sell leg will meet far
more often than a crash, and `manage_owned_position` already distinguishes four classes — nothing
had driven any of them.

| class | our state | position |
| --- | --- | --- |
| definite rejection | operation `rejected` / `broker_close_rejected`, order `rejected`, trade back to **`open`** | owned, immediately re-closable |
| uncertain transport | operation `reconcile_required` / `broker_close_uncertain`, order `submitted`, trade `reconcile_required` | owned; whether the broker acted is undecidable from our records |
| malformed acceptance (filled, wrong position id) | `reconcile_required` / `close_order_did_not_affect_exact_position`, order `rejected` | **not released** |
| lookup outage | `pending` / `close_lookup_unavailable`, **nothing terminal written** | owned; finishes on a later pass |

### ⚠⚠ The uncertain class is not milder than 7b — in one arm it IS 7b

I wrote the uncertain scenario to assert that the capital reader keeps working, and **it does
not — the test was wrong, not the code.** It is now run in BOTH arms of the ambiguity:

- **taken** (double records the close, then raises): the broker's snapshot no longer carries the
  position while our ownership row does, so `resolve_engine_capital_usage` raises
  `engine_capital_ownership_unwitnessed` — 7b's wedge exactly.
- **not taken** (raises before recording): the position is there, the witness join resolves, the
  allocator holds.

**Our side is identical across the two** — same operation status, same error code, same ownership,
same order status. That is the whole justification for `reconcile_required`: the state is
undecidable from our records alone, so nothing may be auto-released and nothing may be
auto-retried. The pair is worth more than either test alone, and a single arm would have read as a
statement about our state when it is a statement about what our state cannot tell.

⚠ The exit-side #2965 question is answered in passing: it is `resolve_engine_capital_usage` that
refuses on a witness mismatch, while `load_engine_capital_authority` returns normally in both arms.
The entry-side defect — a pending claim making the AUTHORITY load raise — has no twin here.

### Round 5's probes

| probe | prediction | result |
| --- | --- | --- |
| force `uncertain = True` in the close failure branch | the rejection test fails | that one FAILED, nothing else |
| force `uncertain = False` | both uncertain tests fail | exactly those two FAILED |
| drop the `position_ids == (owned.broker_position_id,)` comparison | the malformed-acceptance test fails | that one FAILED, nothing else |
| terminalise the operation on a lookup outage | the outage test fails | that one FAILED, nothing else |
| unmodified control | all pass | 9 passed |

### Still not covered after round 5

A full rotation past `sql/373`'s hold (belongs with #2603's revalidation slice, `59ee2fc6`) ·
matrix 7's rebalance SELL, still blocked by `core_close_side_cost_quote_unavailable` · partial
fills, still blocked on #2965's attended partial fill · the EDIT (SL/TP) path's twin failure
classes, which share `_terminal` with the close path and are not exercised here.

**Still no green "ready" status.** Rounds 3-5 closed matrix items 5 and 6 and classified the
non-crash exit failures. The two standing blockers are unchanged, and both need an attended
session.
