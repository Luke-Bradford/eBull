"""#2949 round 2, matrix item 7 — process-boundary faults on the EXIT lifecycle.

Round 1 established the ENTRY arc (``execute_core_rebalance``) and deferred
position closure with the reason recorded rather than mocked: it is a different
transaction, a different recovery reader and a different broker verb, so nothing
round 1 proved carries over.  This file is that scenario family.

**The lifecycle under test.**  ``close_strategy_owned_position``
(``app/api/strategies.py:3323``) → ``manage_owned_position``
(``strategy_position_manager.py:844``) → ``_submit_close`` (``:758``).  The
harness calls ``manage_owned_position`` directly, exactly as round 1 called the
executor directly: the route's authentication, credential decryption and
provenance check are skipped, and that skip is a limit of the evidence, not a
claim about it.

**Two structural facts shape every scenario below, and both were read from
source before the first run rather than discovered by it.**

1. ``_submit_close`` writes the ``orders`` row with ``execution_origin='strategy'``
   and links it ``purpose='exit'``, but writes **no**
   ``strategy_order_reconciliation_state`` row.  ``reconcile_backlog`` selects
   from that table, so an EXIT order is structurally invisible to the scheduled
   reconciler that #2962 just extended to the core arm.  Exit recovery rests
   entirely on ``_resume_operation`` (``:474``), reached from the paper cycle at
   ``strategy_paper_runtime.py:493``.
2. In ``_resume_operation``, ``landed`` is hard-coded false for a close
   (``operation["operation_type"] != "close"`` is the first conjunct).  So a crash
   straddling the broker call still terminalises to ``reconcile_required`` /
   ``crash_before_submission_identity``, whatever the broker actually did.  ⚠ Its
   own comment gives the reason as "there is no edit/close lookup by request UUID",
   and that is established about THIS ADAPTER — ``get_demo_close_order`` takes an
   ``order_id`` and nothing else (``app/providers/broker.py:700``).  Whether eToro
   could offer one was not checked against the portal, so it is not a claim about
   the broker.

Fact 2 is why scenarios 7a and 7b produce the SAME operation row and yet have
opposite consequences: the difference is entirely in what the broker did, which
our side cannot observe.  Separating them is the point of this file.

**Round 3 (#2979) narrowed fact 2 without removing it.** ``_submit_close`` commits
``mark_close_submitting`` between the close intent and the broker call, so
``intent_persisted`` now PROVES the verb was never entered and ``submitting`` proves
only that it was.  That carves out scenario 7d — the genuinely unambiguous crash,
which recovers cleanly — and leaves 7a and 7b reading the same ``submitting`` status
as each other, because no observation available to us distinguishes those two.
Resolving ``submitting`` is #2979's remaining half, blocked on the same
``orders:lookup?referenceId=`` coverage question as #2961, #2965 and #2942 half 2.

⚠ No broker mutation.  The double is file-backed, the database is a disposable
per-worker one, no credential is decrypted and no eToro adapter is imported.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import psycopg
import pytest

from app.providers.broker import BrokerProvider
from app.services.strategy_core_executor import StrategyCoreExecutionError, execute_core_rebalance
from app.services.strategy_engine_capital import EngineCapitalObservationError
from app.services.strategy_order_reconciliation import reconcile_backlog
from app.services.strategy_position_manager import manage_owned_position
from tests.fixtures.core_restart import (
    API_CREDENTIAL_ID,
    CLOCK,
    OPERATOR_ID,
    SIGKILL_RETURNCODE,
    USER_CREDENTIAL_ID,
    FileBackedFakeBroker,
    close_state_report,
    core_ownership_coordinates,
    core_state_report,
    run_engine_until_fault,
    seed_core_execution_world,
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

    Identical to round 1's fixture and deliberately duplicated rather than moved
    to a shared conftest: it is four lines, and a reader of either file can see
    the whole world its scenarios run against without leaving the file.
    """
    from app.services import strategy_core_selection

    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_INSTRUMENT_ID", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_EVIDENCE_REF", None, raising=False)
    select_core_instrument()
    seed_core_execution_world(ebull_test_conn)
    return tmp_path


def _provider(broker: FileBackedFakeBroker) -> BrokerProvider:
    return cast(BrokerProvider, broker)


def _restarted_engine_broker(workdir: Path) -> FileBackedFakeBroker:
    """The same broker, reached by a process that did not submit anything."""
    return FileBackedFakeBroker(workdir / "broker.json")


def _own_one_core_position(
    conn: psycopg.Connection[Any],
    workdir: Path,
) -> tuple[int, int]:
    """Setup, not a scenario: get to one active core ownership, fault-free.

    Runs the entry child to completion and resolves it through the scheduled
    reconciler.  Asserted at every step because a close matrix built on a
    mis-established position would produce confident, meaningless outcomes — and
    the failure would look like a close bug.
    """
    process = run_engine_until_fault(database_url=test_database_url(), workdir=workdir, fault="none")
    assert process.returncode == 0, process.stderr

    broker = _restarted_engine_broker(workdir)
    reconciled = reconcile_backlog(conn, broker=_provider(broker), limit=20)
    assert [result.state for result in reconciled] == ["resolved"]

    entry = core_state_report(conn)
    assert entry["active_ownership"] == 1
    assert entry["trade_statuses"] == ["open"]
    assert broker.read()["mutation_calls"] == 1
    assert broker.read().get("close_calls", 0) == 0
    return core_ownership_coordinates(conn)


def _run_close_child(workdir: Path, coordinates: tuple[int, int], *, fault: str) -> Any:
    strategy_trade_id, broker_position_id = coordinates
    return run_engine_until_fault(
        database_url=test_database_url(),
        workdir=workdir,
        fault=fault,  # type: ignore[arg-type]
        mode="close",
        strategy_trade_id=strategy_trade_id,
        broker_position_id=broker_position_id,
    )


def _manage(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker, coordinates: tuple[int, int]) -> Any:
    """One scheduled-cycle pass over the owned position.

    No ``close_reason``: this is what ``run_strategy_paper_cycle`` does
    (``strategy_paper_runtime.py:493``), which is the whole reason an outstanding
    close operation is reachable without anyone asking for a close again.
    """
    strategy_trade_id, broker_position_id = coordinates
    return manage_owned_position(
        conn,
        broker=_provider(broker),
        strategy_trade_id=strategy_trade_id,
        broker_position_id=broker_position_id,
        now=CLOCK,
    )


def _assert_exit_order_is_invisible_to_the_backlog(
    conn: psycopg.Connection[Any],
    broker: FileBackedFakeBroker,
) -> None:
    """Fact 1 of the module docstring, asserted rather than argued.

    Without this, a change that enrolled EXIT orders in the generic reconciler —
    which is one plausible shape of the #2979 fix — would leave every scenario
    below still passing while their stated reasoning had silently become wrong.
    Two checks because either alone is weak: the row's absence, and an actual
    ``reconcile_backlog`` pass returning nothing for it.
    """
    rows = conn.execute(
        """
        SELECT count(*)
        FROM strategy_order_reconciliation_state state
        JOIN orders o ON o.order_id = state.order_id
        WHERE o.action = 'EXIT'
        """
    ).fetchone()
    conn.commit()
    assert rows is not None and int(rows[0]) == 0

    exit_order_ids = {int(row[0]) for row in conn.execute("SELECT order_id FROM orders WHERE action='EXIT'").fetchall()}
    conn.commit()
    assert exit_order_ids, "the scenario must have created an exit order before this is meaningful"
    selected = {result.order_id for result in reconcile_backlog(conn, broker=_provider(broker), limit=20)}
    assert not (selected & exit_order_ids)


def _execute_core(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker) -> Any:
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
# 7c — the control: a close that reached the broker, resolved by a later process
# ---------------------------------------------------------------------------


def test_scenario_7c_an_accepted_close_is_finished_by_a_process_that_did_not_submit_it(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Classification: AUTOMATICALLY RECOVERED.

    The control arm for 7a and 7b, and it is not optional: without it, a
    scenario that released no ownership would be indistinguishable from a
    lifecycle that cannot release ownership at all.

    The close child runs to completion and stops at ``submitted`` — that is the
    normal end of ``_submit_close``, not a fault, because the broker's 202 is
    acceptance and not settlement.  The position is then finished by the parent,
    which never submitted anything and holds no session: it reads the stored
    ``broker_order_ref``, looks the close order up, matches the exact position id
    and the request UUID, and only then releases ownership.

    ⚠ ``close_calls == 1`` is the load-bearing assertion.  A resume that
    re-closed would also end with the ownership released and the trade closed.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)

    process = _run_close_child(core_world, coordinates, fault="none")
    assert process.returncode == 0, process.stderr
    submitted = json.loads((core_world / "result.json").read_text())
    assert submitted["state"] == "submitted"
    assert submitted["reason_code"] == "broker_close_accepted"

    handed_over = close_state_report(ebull_test_conn)
    assert handed_over["close_statuses"] == ["submitted"]
    assert handed_over["active_ownership"] == 1
    assert handed_over["trade_statuses"] == ["closing"]

    broker = _restarted_engine_broker(core_world)
    assert broker.read()["close_calls"] == 1
    resumed = _manage(ebull_test_conn, broker, coordinates)
    assert resumed.state == "applied"
    # The RESUME's reason names what was verified, not why the close was asked
    # for; `operator_close` survives as the ownership row's `release_reason`,
    # asserted below on the released row rather than on this verdict.
    assert resumed.reason_code == "exact_position_closed"

    report = close_state_report(ebull_test_conn)
    assert report["close_statuses"] == ["applied"]
    assert report["active_ownership"] == 0
    assert report["released_ownership"] == 1
    # The audit trail carries WHY, which the resume verdict does not.
    assert report["release_reasons"] == ["operator_close"]
    assert report["trade_statuses"] == ["closed"]
    assert report["exit_order_statuses"] == ["filled"]
    assert broker.read()["close_calls"] == 1
    # ⚠ The ENTRY counter too. `close_calls` alone would not notice a stray BUY
    # placed anywhere in the exit lifecycle or its recovery.
    assert broker.read()["mutation_calls"] == 1


# ---------------------------------------------------------------------------
# 7a — the broker never heard of the close
# ---------------------------------------------------------------------------


def test_scenario_7a_close_intent_without_submission_costs_the_request_not_the_position(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Classification: SAFELY STOPPED, then RECOVERED BY RE-REQUEST.

    The engine dies holding a durable close intent the broker never received.
    Three properties, each asserted rather than argued:

    1. The position is untouched and the broker recorded no close.  ⚠ The precise
       claim is "nothing accepted", not "never called": the fault fires INSIDE
       ``close_demo_strategy_position``, so the verb was entered and died before
       incrementing anything.  That is the same distinction round 1 drew for the
       entry side, and it is the one that matters — what the broker believes.
    2. The scheduled pass terminalises the orphaned intent — to
       ``reconcile_required``, because for a close ``_resume_operation`` cannot
       tell "never sent" from "sent and lost" and refuses to guess.  It does NOT
       resubmit, which is what makes the stop safe.
    3. Accounting is intact: the ownership row still names a position the broker
       still carries, so the core allocator keeps working.  This is the whole
       difference from 7b, where the same operation row sits on top of a
       position that is gone.

    ⚠ The trade is left at ``reconcile_required`` even though nothing is
    actually unresolved — the position is present, exactly owned, and worth what
    it was.  Recorded as an observability cost of fact 2 rather than a defect on
    its own, because the state is conservative in the safe direction.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)

    process = _run_close_child(core_world, coordinates, fault="after_close_intent_before_submit")
    assert process.returncode == SIGKILL_RETURNCODE
    assert not (core_world / "result.json").exists()

    broker_state = json.loads((core_world / "broker.json").read_text())
    assert broker_state["close_calls"] == 0
    assert broker_state["closes"] == []
    assert all(not record.get("closed") for record in broker_state["orders"])

    crashed = close_state_report(ebull_test_conn)
    # ⚠ `submitting`, not `intent_persisted`, since #2979: this fault fires INSIDE
    # `close_demo_strategy_position`, so `mark_close_submitting` has already
    # committed.  The status is therefore honest about what we can prove — the verb
    # WAS entered — and deliberately does not claim the stronger "never submitted"
    # that 7c establishes.
    assert crashed["close_statuses"] == ["submitting"]
    assert crashed["exit_orders"] == 1
    assert crashed["trade_statuses"] == ["closing"]

    broker = _restarted_engine_broker(core_world)
    resumed = _manage(ebull_test_conn, broker, coordinates)
    assert resumed.state == "reconcile_required"
    assert resumed.reason_code == "crash_before_submission_identity"
    assert broker.read()["close_calls"] == 0

    stalled = close_state_report(ebull_test_conn)
    assert stalled["close_statuses"] == ["reconcile_required"]
    assert stalled["close_error_codes"] == ["crash_before_submission_identity"]
    assert stalled["active_ownership"] == 1
    assert stalled["trade_statuses"] == ["reconcile_required"]
    _assert_exit_order_is_invisible_to_the_backlog(ebull_test_conn, broker)

    # Accounting intact: the allocator still resolves capital against a snapshot
    # that carries the owned position, and holds rather than raising.
    held = _execute_core(ebull_test_conn, broker)
    assert held.state == "held"
    assert held.reason_code == "core_hold"

    # And the exit is still reachable: a fresh request closes it, once.
    strategy_trade_id, broker_position_id = coordinates
    reclosed = manage_owned_position(
        ebull_test_conn,
        broker=_provider(broker),
        strategy_trade_id=strategy_trade_id,
        broker_position_id=broker_position_id,
        close_reason="operator_close",
        now=CLOCK,
    )
    assert reclosed.state == "submitted"
    assert reclosed.reason_code == "broker_close_accepted"
    assert broker.read()["close_calls"] == 1
    reopened = close_state_report(ebull_test_conn)
    assert reopened["close_operations"] == 2
    assert reopened["close_statuses"] == ["reconcile_required", "submitted"]
    assert broker.read()["mutation_calls"] == 1


# ---------------------------------------------------------------------------
# 7b — the broker closed the position and the identity never reached us
# ---------------------------------------------------------------------------


def test_scenario_7b_lost_close_acceptance_wedges_the_core_capital_reader(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Classification: SAFELY STOPPED AT THE BROKER, WEDGED IN ACCOUNTING.

    The exit-side twin of #2961, and the more consequential half of matrix 7.
    The broker executed the close; the engine died before anything on our side
    recorded its identity.  ``_resume_operation`` produces the SAME row as 7a —
    that is fact 2 of the module docstring, and it is why these two scenarios
    cannot be merged — but here it sits on top of a position that no longer
    exists.

    What is safe:

    * no second close reaches the broker, on the resume pass or on a fresh
      close request (``close_calls`` never leaves 1);
    * nothing invents a fill or releases ownership on an unverified assumption.

    What is wedged, and this is the finding:

    * ``strategy_position_ownership`` stays ``active`` naming a position the
      account no longer carries, and nothing terminalises it — the exit order is
      invisible to ``reconcile_backlog`` (fact 1), and every later
      ``manage_owned_position`` pass returns ``owned_position_missing`` without
      releasing anything;
    * ``resolve_engine_capital_usage`` refuses that join by design
      (``strategy_engine_capital.py:329``), and ``execute_core_rebalance`` wraps
      the refusal into ``StrategyCoreExecutionError``
      (``strategy_core_executor.py:527``).  So the core allocator does not refuse
      politely, it RAISES, on this and every subsequent cycle.

    ⚠ The fail-closed behaviour is correct in isolation: an ownership row the
    account cannot witness must not be silently marked good, and #2602 owns the
    question of what may release it.  The defect is that no path exists to
    resolve it at all, which is the same shape #2961 records for the entry side.

    ⚠⚠ "Permanent" is NOT established by the three repeats below, and repeating
    more would not establish it either — that is round 1's lesson about G-1,
    restated.  What establishes it is structural: the only reader that could
    terminalise the ownership is ``_resume_operation``, which has already run and
    written a terminal operation row, and the only other scheduled reconciler
    cannot see an EXIT order at all (fact 1).  The repeats show the loop is
    stable, not that it is eternal.  Recorded as #2979.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)

    process = _run_close_child(core_world, coordinates, fault="after_close_accept")
    assert process.returncode == SIGKILL_RETURNCODE
    assert not (core_world / "result.json").exists()

    broker_state = json.loads((core_world / "broker.json").read_text())
    assert broker_state["close_calls"] == 1
    assert len(broker_state["closes"]) == 1
    assert all(record.get("closed") for record in broker_state["orders"])

    crashed = close_state_report(ebull_test_conn)
    # Still indistinguishable from 7a, and that remains the point: both faults land
    # AFTER `mark_close_submitting`, so both read `submitting`.  #2979's marker
    # separates 7c from this pair; it does not separate 7a from 7b, because nothing
    # on our side can.
    assert crashed["close_statuses"] == ["submitting"]
    assert crashed["trade_statuses"] == ["closing"]

    broker = _restarted_engine_broker(core_world)
    resumed = _manage(ebull_test_conn, broker, coordinates)
    assert resumed.state == "reconcile_required"
    assert resumed.reason_code == "crash_before_submission_identity"
    assert broker.read()["close_calls"] == 1

    stranded = close_state_report(ebull_test_conn)
    assert stranded["close_statuses"] == ["reconcile_required"]
    assert stranded["active_ownership"] == 1
    assert stranded["released_ownership"] == 0
    # Fact 1: the one scheduled reconciler that exists cannot see this order.
    _assert_exit_order_is_invisible_to_the_backlog(ebull_test_conn, broker)

    # The wedge. Not a refusal verdict -- an exception out of the allocator.
    #
    # ⚠ The CAUSE is asserted, not just the type. `strategy_core_executor.py:528`
    # raises the same `StrategyCoreExecutionError` for every failure of the
    # snapshot block, so a broken account read would satisfy a type-only
    # assertion and this test would be claiming something it had not shown. The
    # chained `EngineCapitalObservationError` names the exact position id, which
    # is what ties the raise to the ownership row this scenario stranded.
    with pytest.raises(StrategyCoreExecutionError) as raised:
        _execute_core(ebull_test_conn, broker)
    cause = raised.value.__cause__
    assert isinstance(cause, EngineCapitalObservationError)
    assert str(cause) == f"active core position {coordinates[1]} is absent from broker snapshot"
    ebull_test_conn.rollback()

    # A later scheduled pass reaches it, reports the truth, and still cannot
    # release it -- the entry-side #2961 shape, on the exit side.
    for _ in range(2):
        later = _manage(ebull_test_conn, broker, coordinates)
        assert later.state == "reconcile_required"
        assert later.reason_code == "owned_position_missing"
    assert close_state_report(ebull_test_conn)["active_ownership"] == 1

    # An explicit re-close does not place a second close either: the position is
    # gone, so the manager refuses before reaching the broker.
    strategy_trade_id, broker_position_id = coordinates
    refused = manage_owned_position(
        ebull_test_conn,
        broker=_provider(broker),
        strategy_trade_id=strategy_trade_id,
        broker_position_id=broker_position_id,
        close_reason="operator_close",
        now=CLOCK,
    )
    assert refused.state == "reconcile_required"
    assert refused.reason_code == "owned_position_missing"
    assert broker.read()["close_calls"] == 1
    assert broker.read()["mutation_calls"] == 1
    assert close_state_report(ebull_test_conn)["close_operations"] == 1


# ---------------------------------------------------------------------------
# 7d — the close request died before the broker verb was ever entered (#2979)
# ---------------------------------------------------------------------------


def test_scenario_7d_close_intent_before_the_marker_is_provably_unsubmitted(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Classification: SAFELY STOPPED, and PROVABLY SO — recovered with no cost.

    The case #2979's marker exists to carve out of 7a/7b.  ``_submit_close`` now
    commits ``mark_close_submitting`` between the close intent and the broker call,
    so a crash in the gap between those two commits leaves ``intent_persisted`` —
    and that is a fact about OUR write ordering, not an inference about the broker.

    ⚠ The distinction this file could not draw before is exactly here.  7a and 7b
    both land AFTER the marker and both read ``submitting``; nothing on our side
    separates them, and #2979's remaining half is still open for that.  This
    scenario is the distinct third case, and it is the one where the engine can say "the
    broker never saw it" and be right by construction rather than by assumption.

    So the recovery is clean rather than conservative: the request is abandoned,
    the POSITION is untouched, and the trade returns to ``open`` — not to
    ``reconcile_required``, which is what 7a pays and which the 7a docstring
    already records as an observability cost.

    ⚠ No second close is placed by the recovery.  Abandoning the request leaves a
    fresh close to the scheduled cycle's own judgement; placing one from inside a
    crash-recovery path would let a crash trigger a broker mutation.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)

    process = _run_close_child(core_world, coordinates, fault="after_close_intent_before_marker")
    assert process.returncode == SIGKILL_RETURNCODE
    assert not (core_world / "result.json").exists()

    # The broker was never reached, and the intent IS durable -- both halves
    # matter, because a rolled-back intent would make the scenario vacuous.
    broker_state = json.loads((core_world / "broker.json").read_text())
    assert broker_state["close_calls"] == 0
    assert broker_state["closes"] == []
    assert all(not record.get("closed") for record in broker_state["orders"])

    crashed = close_state_report(ebull_test_conn)
    assert crashed["close_statuses"] == ["intent_persisted"]
    assert crashed["exit_orders"] == 1
    assert crashed["trade_statuses"] == ["closing"]

    broker = _restarted_engine_broker(core_world)
    resumed = _manage(ebull_test_conn, broker, coordinates)
    assert resumed.state == "rejected"
    assert resumed.reason_code == "close_never_submitted"
    assert broker.read()["close_calls"] == 0

    resolved = close_state_report(ebull_test_conn)
    assert resolved["close_statuses"] == ["rejected"]
    assert resolved["close_error_codes"] == ["close_never_submitted"]
    assert resolved["exit_order_statuses"] == ["rejected"]
    # The position is still owned and the trade is back to a manageable state --
    # this is the difference from 7a, and it is the whole point of the marker.
    assert resolved["active_ownership"] == 1
    assert resolved["released_ownership"] == 0
    assert resolved["trade_statuses"] == ["open"]
    _assert_exit_order_is_invisible_to_the_backlog(ebull_test_conn, broker)

    # Accounting is intact: the allocator resolves capital against a snapshot that
    # still carries the owned position, and holds rather than raising.
    held = _execute_core(ebull_test_conn, broker)
    assert held.state == "held"
    assert held.reason_code == "core_hold"

    # And the exit is reachable on the next request, exactly once.
    strategy_trade_id, broker_position_id = coordinates
    reclosed = manage_owned_position(
        ebull_test_conn,
        broker=_provider(broker),
        strategy_trade_id=strategy_trade_id,
        broker_position_id=broker_position_id,
        close_reason="operator_close",
        now=CLOCK,
    )
    assert reclosed.state == "submitted"
    assert reclosed.reason_code == "broker_close_accepted"
    assert broker.read()["close_calls"] == 1
    assert broker.read()["mutation_calls"] == 1
    reopened = close_state_report(ebull_test_conn)
    assert reopened["close_operations"] == 2
    assert reopened["close_statuses"] == ["rejected", "submitted"]
