"""#2949 — process-boundary fault recovery for the core/cash sleeve.

⚠ THIS IS THE FIRST TEST THAT RUNS ``execute_core_rebalance`` AGAINST A REAL
DATABASE.  ``tests/test_2603_core_executor.py`` drives it through a ``FakeConn``
that matches SQL prefixes and returns canned ids, and
``tests/test_2603_core_mandate_api.py`` mocks the executor out of the route
entirely, so nothing had ever established that the executor's 130-line durable
authority transaction is even accepted by the schema it writes to.  It is —
see :func:`test_the_executor_writes_one_durable_authority_against_the_real_schema`
— and the first run of this harness found the answer only because the whole
thing was exercised, not simulated.

**Why a subprocess.**  #2949's bounded experiment requires a real process
restart, and an in-process double cannot produce one: Python would unwind the
stack, psycopg would close the connection cleanly and ``finally`` blocks would
run.  A crashed engine gets none of that.  The child therefore SIGKILLs itself at
a declared fault point (``tests/fixtures/core_restart_child.py``) and the parent
is the restarted engine.

**Why the broker's state is a file.**  A stateful broker is the other half of the
requirement: a response mock dies with the engine, so every restart scenario
would pass by amnesia rather than by recovery.  The file records each accepted
order BEFORE the fault can fire, so "did the broker see two economic orders?" is
answerable after our process is gone.

Scenario numbering follows #2949's frozen matrix.  Round 2 added item 5 (a
backlog genuinely over the batch cap, which #2962 made non-vacuous for the core
arm); item 7's position-closure half lives in
``tests/test_2949_core_close_recovery_db.py`` because the EXIT lifecycle is a
different transaction, a different recovery reader and a different broker verb.
What is still NOT run — item 6's credential rotation and mandate revocation,
item 7's rebalance SELL (blocked by ``core_close_side_cost_quote_unavailable``),
and partial fills — is recorded in
``docs/proposals/execution/2026-09-13-core-restart-acceptance.md``; a silently
dropped scenario reads as a covered one.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import psycopg
import pytest

from app.providers.broker import BrokerProvider
from app.services.strategy_core_executor import (
    core_authority_is_stranded,
    execute_core_rebalance,
    load_core_resume_authority,
    resume_core_submission,
)
from app.services.strategy_order_reconciliation import (
    enforce_reconciliation_slo,
    reconcile_backlog,
)
from tests.fixtures.core_restart import (
    API_CREDENTIAL_ID,
    CLOCK,
    OPERATOR_ID,
    SIGKILL_RETURNCODE,
    USER_CREDENTIAL_ID,
    FileBackedFakeBroker,
    core_state_report,
    run_engine_until_fault,
    seed_core_execution_world,
    seed_non_core_strategy_order,
    select_core_instrument,
)
from tests.fixtures.ebull_test_db import test_database_url


@pytest.fixture
def core_world(
    ebull_test_conn: psycopg.Connection[Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Seed and COMMIT the world both processes read, and publish the selection.

    Committed rather than left in the fixture's transaction because the child is
    a different process: anything uncommitted here is invisible to it.

    ``SELECTED_CORE_INSTRUMENT_ID`` is ``None`` on ``main`` while #2833's
    five-trading-day verdict runs, so the executor refuses every call without
    this.  That is a property of the DECLARATION, not of the recovery machinery
    under test.  The parent patches it through ``monkeypatch`` so the module
    constant is restored; the child sets it in its own process and dies.
    """
    from app.services import strategy_core_selection

    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_INSTRUMENT_ID", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_EVIDENCE_REF", None, raising=False)
    select_core_instrument()
    seed_core_execution_world(ebull_test_conn)
    return tmp_path


def _restarted_engine_broker(workdir: Path, *, fill_status: str = "Filled") -> FileBackedFakeBroker:
    """The same broker, reached by a process that did not submit the order."""
    return FileBackedFakeBroker(workdir / "broker.json", fill_status=fill_status)


def _provider(broker: FileBackedFakeBroker) -> BrokerProvider:
    """Present the double as the interface, once, in a named place.

    ``BrokerProvider`` is an ABC with dozens of abstract methods and the double
    implements the four these paths call.  Subclassing to satisfy pyright would
    make the fixture larger than the thing it stands in for; a cast at one
    chokepoint is honest about the same gap and keeps every call site readable.
    """
    return cast(BrokerProvider, broker)


def _attempt_counts(conn: psycopg.Connection[Any]) -> dict[int, int]:
    rows = conn.execute("SELECT order_id, attempt_count FROM strategy_order_reconciliation_state").fetchall()
    conn.commit()
    return {int(order_id): int(count) for order_id, count in rows}


def _execute(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker) -> Any:
    return execute_core_rebalance(
        conn,
        broker=_provider(broker),
        operator_id=OPERATOR_ID,
        api_key_credential_id=API_CREDENTIAL_ID,
        user_key_credential_id=USER_CREDENTIAL_ID,
        recorded_by="core-restart-harness",
        clock=lambda: CLOCK,
    )


# ---------------------------------------------------------------------------
# Baseline — the fault-free arc, which no prior test established
# ---------------------------------------------------------------------------


def test_the_executor_writes_one_durable_authority_against_the_real_schema(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """One clean cycle: one intent, one trade, one order, one broker mutation.

    The control arm for every fault below.  Without it a scenario that produced
    nothing would be indistinguishable from a scenario whose fault fired early.
    """
    process = run_engine_until_fault(database_url=test_database_url(), workdir=core_world, fault="none")

    assert process.returncode == 0, process.stderr
    result = json.loads((core_world / "result.json").read_text())
    assert result["state"] == "submitted"
    assert result["reason_code"] == "broker_accepted_pending_reconciliation"

    broker_state = json.loads((core_world / "broker.json").read_text())
    assert broker_state["mutation_calls"] == 1

    report = core_state_report(ebull_test_conn)
    assert report["core_trades"] == 1
    assert report["strategy_orders"] == 1
    assert report["orders_with_broker_ref"] == 1
    assert report["trade_statuses"] == ["submitted"]
    assert report["reconciliation_states"] == ["pending"]
    assert report["requested_amount_total"] == Decimal(result["amount"])


# ---------------------------------------------------------------------------
# Scenario 1 — restart before durable authority commit
# ---------------------------------------------------------------------------


def test_scenario_1_restart_before_commit_leaves_no_authority_and_no_mutation(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Killed inside the authority transaction: nothing durable, nothing claimed.

    The classification #2949 asks for is AUTOMATICALLY RECOVERED — a later cycle
    starts clean and submits exactly one order, with no orphan funding claim from
    the crashed attempt.
    """
    process = run_engine_until_fault(
        database_url=test_database_url(), workdir=core_world, fault="before_authority_commit"
    )
    assert process.returncode == SIGKILL_RETURNCODE

    crashed = core_state_report(ebull_test_conn)
    assert crashed["core_trades"] == 0
    assert crashed["strategy_orders"] == 0
    assert crashed["reconciliation_rows"] == 0
    # ⚠ The INTENT too. The other three counts would all be zero for an
    # implementation that committed an orphan rebalance intent before opening the
    # authority transaction, and "nothing durable" has to mean nothing.
    assert crashed["intents"] == 0
    assert json.loads((core_world / "broker.json").read_text())["mutation_calls"] == 0

    broker = _restarted_engine_broker(core_world)
    assert load_core_resume_authority(ebull_test_conn) is None
    result = _execute(ebull_test_conn, broker)

    assert result.state == "submitted"
    assert broker.read()["mutation_calls"] == 1
    recovered = core_state_report(ebull_test_conn)
    assert recovered["core_trades"] == 1
    assert recovered["strategy_orders"] == 1


# ---------------------------------------------------------------------------
# Scenario 2 — restart after commit, before submission
# ---------------------------------------------------------------------------


def test_scenario_2_restart_after_commit_never_resubmits_and_blocks_new_authority(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """The authority survives; the broker never heard of it; nothing retries it.

    Classification: SAFELY STOPPED.  The two halves that make it safe are both
    asserted — the resume path reconciles rather than resubmitting (broker
    mutations stay at zero), and a fresh evaluation is refused
    ``core_trade_in_flight`` rather than issuing a second economic order.
    """
    process = run_engine_until_fault(
        database_url=test_database_url(), workdir=core_world, fault="after_commit_before_submit"
    )
    assert process.returncode == SIGKILL_RETURNCODE
    assert json.loads((core_world / "broker.json").read_text())["mutation_calls"] == 0

    crashed = core_state_report(ebull_test_conn)
    assert crashed["core_trades"] == 1
    assert crashed["orders_with_broker_ref"] == 0
    assert crashed["reconciliation_states"] == ["unresolved"]

    broker = _restarted_engine_broker(core_world)
    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None
    assert authority.broker_order_ref is None

    resumed = resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)
    assert resumed.state == "submission_uncertain"
    assert resumed.reason_code == "core_order_reconciliation_not_found"
    # THE invariant of this scenario: a lookup miss is not permission to retry.
    assert broker.read()["mutation_calls"] == 0

    refused = _execute(ebull_test_conn, broker)
    assert refused.state == "refused"
    assert refused.reason_code == "core_trade_in_flight"
    assert broker.read()["mutation_calls"] == 0
    assert core_state_report(ebull_test_conn)["strategy_orders"] == 1


def test_scenario_2_stranded_authority_is_reached_unattended_and_still_cannot_resolve(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """The stop is safe, is now REACHED unattended, and still cannot resolve.

    ⚠ This test asserted the opposite until #2962, and the inversion is the
    point rather than a rewrite.  ``reconcile_backlog`` carried
    ``AND trade.core_rebalance_intent_id IS NULL`` from ``608dc879``, so the one
    scheduled reconciliation caller could not see any core order — the finding
    this harness was written to produce (G-2).  The predicate is gone; what did
    NOT change is everything below it.

    Three facts, each asserted rather than argued:

    1. ``reconcile_backlog`` — reached from ``scheduler.strategy_paper_cycle`` —
       now returns this order.  ⚠ Still asserted against a POSITIVE CONTROL, a
       non-core order in the same unresolved state in the same database, because
       the discrimination it buys only changed direction: with both present, a
       selection broken to return everything and one correctly covering both arms
       are still told apart by the control's presence, and the core order's
       ``not_found`` outcome below is what proves the batch actually looked it up
       rather than merely listing it.
    2. The unattended pass changes nothing about resolvability, exactly as the
       attended resume did not.  The broker genuinely never accepted the order,
       so the exact-ID lookup misses and the reconciliation row cannot reach a
       terminal state: ``_apply_detail`` needs a successful lookup, and the
       executor's own ``rejected`` write (``strategy_core_executor.py:321``)
       belongs to the submission path a dead process never returns to.  ⚠ The
       mutation count is the invariant: covering the core arm unattended must not
       have made anything place an order.
    3. ``enforce_reconciliation_slo`` never shared the core exclusion, so the row
       nothing can resolve is still the row it escalates — into a global
       ``strategy_execution_blocks`` row that stops new strategy ENTRIES on both
       arms.  ⚠ It does not stop reconciliation or owned-position management, and
       clearing it would not resolve the authority.

    ⚠ Repeated attempts cannot establish permanence as a matter of experiment;
    what establishes it is that no code path writes a terminal state without a
    successful lookup, and the lookup cannot succeed for an order that was never
    submitted.  Recorded as gap G-1 (#2961), which #2962 does NOT close.
    """
    process = run_engine_until_fault(
        database_url=test_database_url(), workdir=core_world, fault="after_commit_before_submit"
    )
    assert process.returncode == SIGKILL_RETURNCODE
    broker = _restarted_engine_broker(core_world)

    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None

    control_order_id, _ = seed_non_core_strategy_order(ebull_test_conn)
    picked_up = reconcile_backlog(ebull_test_conn, broker=_provider(broker), limit=20)
    assert sorted(result.order_id for result in picked_up) == sorted([authority.order_id, control_order_id])

    # A second attended resume on top adds nothing and, critically, still places
    # nothing -- the two reconcilers reaching the same order is now ordinary.
    again = resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)
    assert again.state == "submission_uncertain"
    assert broker.read()["mutation_calls"] == 0
    core_row = ebull_test_conn.execute(
        "SELECT state FROM strategy_order_reconciliation_state WHERE order_id=%s",
        (authority.order_id,),
    ).fetchone()
    ebull_test_conn.commit()
    assert core_row is not None and core_row[0] == "not_found"

    # Age the row rather than sleeping for it. `first_unresolved_at` is
    # deliberately never written by production code -- that is what makes the
    # SLO's measurement untamperable -- so a test that needs an aged row has to
    # set it, and saying so here is the honest form. A `sleep` would make the
    # same point non-deterministically and slowly.
    #
    # Scoped to the core order: the control order must not contribute to the
    # overdue count, or the assertion below would not be about the core arm.
    ebull_test_conn.execute(
        "UPDATE strategy_order_reconciliation_state "
        "SET first_unresolved_at = now() - interval '1 hour' WHERE order_id=%s",
        (authority.order_id,),
    )
    ebull_test_conn.commit()
    health = enforce_reconciliation_slo(ebull_test_conn, max_unresolved_seconds=60)
    ebull_test_conn.commit()
    assert health.active_block is True
    assert health.overdue_count == 1
    block = ebull_test_conn.execute(
        "SELECT active FROM strategy_execution_blocks WHERE source='order_reconciliation'"
    ).fetchone()
    ebull_test_conn.commit()
    assert block is not None and block[0] is True


def test_the_stranded_shape_is_distinguishable_on_the_read_surface(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """#2961: the operator page must not promise a resume will resolve this.

    ``core_authority_is_stranded`` is what lets ``GET /core-sleeve`` say
    something true about this row instead of the generic "the next attended
    action resumes or reconciles that exact order", which for this shape is a
    promise that can never be kept.

    Both conjuncts are asserted to be load-bearing rather than assumed: the
    stored ``broker_order_ref`` and the ``not_found`` state each flip the answer
    on their own. A test asserting only ``True`` on the stranded row would pass
    against a function that returned ``True`` unconditionally.

    ⚠ This is a READ-surface flag and deliberately not a verdict. A lookup miss
    is not proof the broker never received the order — ``orders:lookup``'s
    ``referenceId`` coverage is undocumented — which is why nothing here
    terminalises anything and resubmission stays refused.
    """
    process = run_engine_until_fault(
        database_url=test_database_url(), workdir=core_world, fault="after_commit_before_submit"
    )
    assert process.returncode == SIGKILL_RETURNCODE
    broker = _restarted_engine_broker(core_world)

    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None

    # Before any lookup has run the row is `unresolved`, not `not_found`: nothing
    # has yet asked the broker, so nothing may be said about what it holds.
    assert core_authority_is_stranded(ebull_test_conn, order_id=authority.order_id) is False

    resumed = resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)
    assert resumed.state == "submission_uncertain"
    assert broker.read()["mutation_calls"] == 0
    assert core_authority_is_stranded(ebull_test_conn, order_id=authority.order_id) is True

    # The second conjunct: had acceptance ever been persisted, a position may
    # exist and this shape is no longer the never-submitted one.
    ebull_test_conn.execute(
        "UPDATE orders SET broker_order_ref='90210' WHERE order_id=%s",
        (authority.order_id,),
    )
    ebull_test_conn.commit()
    assert core_authority_is_stranded(ebull_test_conn, order_id=authority.order_id) is False


# ---------------------------------------------------------------------------
# Scenario 3 — broker accepts, the response is lost, the engine restarts
# ---------------------------------------------------------------------------


def test_scenario_3_lost_acceptance_reconciles_to_exactly_one_owned_position(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """The only fault that can strand real money, and the one it recovers from.

    Classification: AUTOMATICALLY RECOVERED.  What is proved here is the
    economics of the ATTENDED path: the exact-ID lookup finds the order the dead
    process submitted, one ownership claim is made, and the broker's mutation
    count never moves past one.  The unattended path is the same recovery through
    a different caller and is proved separately, immediately below.
    """
    process = run_engine_until_fault(database_url=test_database_url(), workdir=core_world, fault="after_broker_accept")
    assert process.returncode == SIGKILL_RETURNCODE

    accepted = json.loads((core_world / "broker.json").read_text())
    assert accepted["mutation_calls"] == 1
    assert len(accepted["orders"]) == 1

    crashed = core_state_report(ebull_test_conn)
    # The engine died before persisting acceptance, so it does not know the
    # broker's id for its own order. Only the request UUID connects them.
    assert crashed["orders_with_broker_ref"] == 0
    assert crashed["reconciliation_states"] == ["unresolved"]

    broker = _restarted_engine_broker(core_world)
    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None
    assert str(authority.request_id) == accepted["orders"][0]["reference_id"]

    resumed = resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)
    assert resumed.state == "held"
    assert resumed.reason_code == "core_order_reconciled"

    report = core_state_report(ebull_test_conn)
    assert report["strategy_orders"] == 1
    assert report["active_ownership"] == 1
    assert report["reconciliation_states"] == ["resolved"]
    assert report["trade_statuses"] == ["open"]
    assert broker.read()["mutation_calls"] == 1


def test_scenario_3_lost_acceptance_is_recovered_with_no_session_at_all(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """#2962's acceptance: the same recovery, reached by the scheduled caller.

    Identical fault to the test above, and deliberately NOT a variation of it —
    the difference is the caller and nothing else.  ``reconcile_backlog`` is what
    ``scheduler.strategy_paper_cycle`` runs every five minutes, so this is the
    whole claim "a core order left non-terminal by a process restart reaches
    ``resolved`` with one ownership claim, with no browser session involved".

    Nothing here holds an operator session, loads a credential by
    ``session.operator_id`` or touches ``rebalance_core_sleeve``.  ⚠ The account
    identity that makes that safe is held outside this code: ``sql/373`` refuses
    to revoke or delete either credential the order's eligibility proof names
    while it is non-terminal, and ``broker_credentials`` admits one live row per
    ``(operator, provider, label, environment)``.  A provenance predicate in the
    selection would be unreachable, which is why there is not one.

    ⚠ ``mutation_calls == 1`` is the load-bearing assertion, not
    ``active_ownership == 1``.  An unattended reconciler that resubmitted would
    also end at one ownership row.
    """
    process = run_engine_until_fault(database_url=test_database_url(), workdir=core_world, fault="after_broker_accept")
    assert process.returncode == SIGKILL_RETURNCODE
    assert json.loads((core_world / "broker.json").read_text())["mutation_calls"] == 1

    crashed = core_state_report(ebull_test_conn)
    assert crashed["orders_with_broker_ref"] == 0
    assert crashed["reconciliation_states"] == ["unresolved"]

    broker = _restarted_engine_broker(core_world)
    # A READ, for the assertion only -- the recovery below is given nothing but a
    # connection and a broker, which is all the scheduled cycle has.
    stranded = load_core_resume_authority(ebull_test_conn)
    assert stranded is not None

    reconciled = reconcile_backlog(ebull_test_conn, broker=_provider(broker), limit=20)
    assert [(result.order_id, result.state) for result in reconciled] == [(stranded.order_id, "resolved")]

    report = core_state_report(ebull_test_conn)
    assert report["strategy_orders"] == 1
    assert report["active_ownership"] == 1
    assert report["reconciliation_states"] == ["resolved"]
    assert report["trade_statuses"] == ["open"]
    assert broker.read()["mutation_calls"] == 1

    # Matrix item 5 (backlog over the batch cap) stops being vacuous for the core
    # arm here: the arm now HAS a batch. A second pass must select nothing, since
    # the row is terminal.
    assert reconcile_backlog(ebull_test_conn, broker=_provider(broker), limit=20) == ()
    assert broker.read()["lookup_calls"] == 1


def test_scenario_8_repeating_a_recovered_cycle_creates_no_further_order(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Matrix item 8: replay must not grow orders, reservations or ledger rows.

    The fake broker's account snapshot is DERIVED from its own filled orders, so
    the recovered position is visible to the next evaluation.  A constant
    snapshot would make this vacuous — the engine would keep seeing an empty
    sleeve and keep buying, and the assertion would be measuring the fixture.
    """
    process = run_engine_until_fault(database_url=test_database_url(), workdir=core_world, fault="after_broker_accept")
    assert process.returncode == SIGKILL_RETURNCODE

    broker = _restarted_engine_broker(core_world)
    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None
    resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)

    for _ in range(2):
        replayed = _execute(ebull_test_conn, broker)
        assert replayed.state == "held"
        assert replayed.reason_code == "core_hold"

    report = core_state_report(ebull_test_conn)
    assert report["strategy_orders"] == 1
    assert report["core_trades"] == 1
    assert report["active_ownership"] == 1
    assert broker.read()["mutation_calls"] == 1


# ---------------------------------------------------------------------------
# Scenario 5 — a backlog genuinely over the batch cap, with a core row in it
# ---------------------------------------------------------------------------


def test_scenario_5_a_core_row_behind_the_batch_cap_is_reached_within_the_declared_bound(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Matrix item 5's OVER-CAP half, which #2962 made meaningful.

    ⚠ Item 5 reads "outage, then recovery with a backlog over the batch cap" and
    only the second clause is run here.  The outage half — a backlog full of
    ``error`` / ``not_found`` rows accumulated while the broker was unreachable,
    then drained once it returns — exercises the cooldown arithmetic rather than
    the rotation, and is a separate scenario.  Claiming item 5 whole would be the
    silently-dropped-scenario defect this matrix exists to avoid.

    #2962 asserted the weaker two-pass property — a resolved core row leaves the
    backlog — with a backlog of one.  This is the over-cap case: five alpha rows
    are seeded FIRST so the core row is last in the queue and behind a cap of
    two, and ``reconcile_backlog`` declares that a due row is selected within
    ``ceil(due_rows / limit)`` completed cycles.

    ⚠ THE COMPETITORS HAVE TO KEEP COMPETING.  Each is registered on the broker
    double as an accepted-but-``Pending`` order, so every poll leaves it
    ``pending`` — a progress state, which ``reconcile_backlog`` exempts from the
    exponential cooldown.  Competitors that fell to ``not_found`` would earn the
    cooldown, drop out of the selection, and let the core row through for a
    reason that has nothing to do with the rotation — the test would then pass
    against the very absorbing state #2948 fixed.

    ⚠ NON-VACUITY IS ASSERTED, not assumed: cycle 1 must NOT contain the core
    order.  Without that, a backlog that happened to select everything would
    satisfy the bound trivially.  Under the pre-#2948 sort
    (``first_unresolved_at, order_id``, both immutable for a non-terminal row)
    the first two competitors would be re-selected forever and the core row at
    position six would never be reached at all.
    """
    competitors = 5
    limit = 2

    broker = _restarted_engine_broker(core_world)
    control_order_ids: list[int] = []
    for ordinal in range(competitors):
        order_id, request_id = seed_non_core_strategy_order(ebull_test_conn, ordinal=ordinal, amount="1")
        # Same amount on both sides. They are different ledgers -- ours reserves
        # against the 1,000 pot, the broker's nets against account cash -- and a
        # competitor whose two sides disagreed would be seeding an accounting
        # inconsistency into a test that is not about accounting.
        broker.seed_accepted_order(reference_id=str(request_id), amount="1", status="Pending")
        control_order_ids.append(order_id)

    process = run_engine_until_fault(database_url=test_database_url(), workdir=core_world, fault="none")
    assert process.returncode == 0, process.stderr
    core_order_id = json.loads((core_world / "result.json").read_text())["order_id"]

    due = ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_order_reconciliation_state WHERE state NOT IN ('resolved','rejected')"
    ).fetchone()
    ebull_test_conn.commit()
    assert due is not None and int(due[0]) == competitors + 1

    # ceil(6 / 2) == 3. Stated as the arithmetic rather than as a literal, so a
    # change to either constant cannot leave the bound silently wrong.
    bound = -(-(competitors + 1) // limit)
    selected_per_cycle: list[list[int]] = []
    for _ in range(bound):
        results = reconcile_backlog(ebull_test_conn, broker=_provider(broker), limit=limit)
        selected_per_cycle.append([result.order_id for result in results])

    assert [len(cycle) for cycle in selected_per_cycle] == [limit] * bound
    assert core_order_id not in selected_per_cycle[0]
    assert core_order_id in selected_per_cycle[bound - 1]

    # The core order resolved on the one poll it got; the competitors are still
    # pending, which is what kept them in the rotation.
    states = dict(
        ebull_test_conn.execute(
            "SELECT order_id, state FROM strategy_order_reconciliation_state ORDER BY order_id"
        ).fetchall()
    )
    ebull_test_conn.commit()
    assert states[core_order_id] == "resolved"
    assert {states[order_id] for order_id in control_order_ids} == {"pending"}
    core_attempts_when_resolved = _attempt_counts(ebull_test_conn)[core_order_id]

    # SUSTAINED rotation, not just the opening sweep.  The three cycles above
    # start from five never-attempted competitors and one already-attempted core
    # row, which a selector that merely preferred `last_attempt_at IS NULL` would
    # also satisfy.  Running the rotation a second time round asserts the
    # property that actually matters: every still-pending competitor is revisited
    # once the never-attempted ones are gone.
    for _ in range(bound):
        reconcile_backlog(ebull_test_conn, broker=_provider(broker), limit=limit)
    attempts = _attempt_counts(ebull_test_conn)
    assert min(attempts[order_id] for order_id in control_order_ids) >= 2
    # The resolved core row is terminal and must NOT be re-polled, whatever the
    # rotation does -- it is excluded by STATE, not by having been seen.
    # Asserted as "unchanged", not as a literal: the row already carries two
    # attempts by the time it resolves, one written by the submission path and
    # one by the poll that resolved it, and pinning that number here would make
    # this test fail on an unrelated change to either.
    assert attempts[core_order_id] == core_attempts_when_resolved

    report = core_state_report(ebull_test_conn)
    assert report["active_ownership"] == 1
    assert broker.read()["mutation_calls"] == 1


# ---------------------------------------------------------------------------
# Scenario 4 — pending across the restart, then filled
# ---------------------------------------------------------------------------


def test_scenario_4_pending_then_filled_claims_ownership_exactly_once(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """A pending order stays pending, owns nothing, and does not re-reserve.

    The first resume must NOT invent ownership for an unfilled order, and the
    second must not create a second one when the same order fills.  Both are
    cardinality claims, so both are asserted on counts rather than on states.
    """
    process = run_engine_until_fault(
        database_url=test_database_url(),
        workdir=core_world,
        fault="after_broker_accept",
        fill_status="Pending",
    )
    assert process.returncode == SIGKILL_RETURNCODE

    broker = _restarted_engine_broker(core_world, fill_status="Pending")
    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None

    pending = resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)
    assert pending.state == "submission_uncertain"
    assert pending.reason_code == "core_order_reconciliation_pending"
    interim = core_state_report(ebull_test_conn)
    assert interim["active_ownership"] == 0
    assert interim["reconciliation_states"] == ["pending"]

    broker.set_order_status(reference_id=str(authority.request_id), status="Filled")
    filled = resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)
    assert filled.state == "held"
    assert filled.reason_code == "core_order_reconciled"

    report = core_state_report(ebull_test_conn)
    assert report["active_ownership"] == 1
    assert report["strategy_orders"] == 1
    assert broker.read()["mutation_calls"] == 1


# ---------------------------------------------------------------------------
# Scenario 6 (partial) — the kill switch moves between phases
# ---------------------------------------------------------------------------


def test_scenario_6_kill_switch_between_phases_stops_entry_not_reconciliation(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """A stop must not strand an order it cannot then account for.

    The kill switch is armed AFTER the broker accepted and BEFORE recovery.
    Reconciliation is a read, so it must still reach the broker and resolve the
    outstanding order — otherwise the switch creates exactly the unaccounted
    exposure it exists to prevent.

    The ENTRY half of item 6 is a separate test below, because on this fault the
    post-recovery sleeve is inside its band and ``hold`` short-circuits before
    the kill switch is read.  Only these two parts of item 6 are run; credential
    rotation and mandate revocation are recorded as not-run in the acceptance
    report.
    """
    process = run_engine_until_fault(database_url=test_database_url(), workdir=core_world, fault="after_broker_accept")
    assert process.returncode == SIGKILL_RETURNCODE

    ebull_test_conn.execute("UPDATE kill_switch SET is_active=TRUE WHERE id=TRUE")
    ebull_test_conn.commit()

    broker = _restarted_engine_broker(core_world)
    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None
    resumed = resume_core_submission(ebull_test_conn, broker=_provider(broker), authority=authority)
    # Asserted on the broker's own call counter, not inferred from the verdict:
    # the point is that the lookup HAPPENED under an active kill switch.
    assert broker.read()["lookup_calls"] == 1
    assert resumed.state == "held"
    assert resumed.reason_code == "core_order_reconciled"
    assert core_state_report(ebull_test_conn)["active_ownership"] == 1
    assert broker.read()["mutation_calls"] == 1


def test_scenario_6_kill_switch_refuses_a_clean_re_entry_after_a_crash(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """No new exposure while the switch is on, and the refusal names the switch.

    ⚠ Run on ``before_authority_commit`` deliberately.  ``execute_core_rebalance``
    reads the kill switch inside ``preflight_core_submission``, which sits
    BEHIND two earlier returns: the allocator's ``hold``
    (``strategy_core_executor.py:493``) and the submission gate's
    ``core_trade_in_flight`` (``:498``).  Every other fault in this matrix leaves
    one of those two binding, so on any of them this assertion would have been
    green without the kill switch ever being consulted.  Only a crash that left
    nothing durable gives a clean, actionable state in which the switch is the
    refusal that fires.

    That precedence is correct — all three refuse — but it is worth recording:
    an operator reading a core refusal during an incident sees the in-flight
    code, not the switch they just pulled.
    """
    process = run_engine_until_fault(
        database_url=test_database_url(), workdir=core_world, fault="before_authority_commit"
    )
    assert process.returncode == SIGKILL_RETURNCODE
    assert core_state_report(ebull_test_conn)["core_trades"] == 0

    ebull_test_conn.execute("UPDATE kill_switch SET is_active=TRUE WHERE id=TRUE")
    ebull_test_conn.commit()

    broker = _restarted_engine_broker(core_world)
    refused = _execute(ebull_test_conn, broker)
    assert refused.state == "refused"
    assert refused.reason_code == "core_kill_switch_active_or_missing"
    assert broker.read()["mutation_calls"] == 0
    assert core_state_report(ebull_test_conn)["strategy_orders"] == 0
