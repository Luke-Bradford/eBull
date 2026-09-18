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
from app.services.strategy_core_executor import execute_core_rebalance
from app.services.strategy_engine_capital import (
    EngineCapitalObservationError,
    load_engine_capital_authority,
    resolve_engine_capital_usage,
)
from app.services.strategy_order_reconciliation import reconcile_backlog
from app.services.strategy_position_manager import manage_owned_position
from tests.fixtures.core_restart import (
    API_CREDENTIAL_ID,
    CLOCK,
    CORE_INSTRUMENT_ID,
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

    # ⚠ All THREE constants must be registered for restoration. `select_core_instrument`
    # sets the outcome too (#3037); leaving it out leaks `SELECTED_CORE_OUTCOME="pass"`
    # into every later test in the process, as a declaration with no instrument id.
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_OUTCOME", None, raising=False)
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
      (``strategy_engine_capital.py``), on this and every subsequent cycle.

    ⚠ **What #2979 half b changed, and what it did not.**  The allocator no longer
    RAISES on that refusal: it returns ``refused`` /
    ``engine_capital_ownership_unwitnessed``, so the core arm degrades legibly instead
    of surfacing a 409 whose only text is the outer sentence.  The wedge itself is
    untouched — ownership is still ``active``, the join still refuses, and nothing
    terminalises the row — which is why #2979 stays open on its other half.  The caller
    that reached this refusal on every UNATTENDED tick was the paper cycle, which used
    to abort entirely; it now rejects the signal (``strategy_paper_executor``).

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

    # The allocator now REFUSES rather than raising (#2979 half b). The wedge itself
    # is unchanged -- ownership is still `active` and the join still refuses -- but the
    # arm degrades with a reason code instead of disappearing behind a 409 whose only
    # text is the outer sentence.
    #
    # ⚠ The IDENTITY evidence is kept, and deliberately not left to the code alone:
    # `engine_capital_ownership_unwitnessed` is a bucket, and a broken account read or
    # a snapshot of the wrong account would produce the same bucket. The reader is
    # therefore called directly below, so the position id this scenario stranded is
    # still what ties the refusal to the ownership row.
    refusal = _execute_core(ebull_test_conn, broker)
    assert refusal.state == "refused"
    assert refusal.reason_code == "engine_capital_ownership_unwitnessed"
    assert refusal.intent_id is None
    assert refusal.trade_id is None
    assert refusal.order_id is None
    ebull_test_conn.rollback()

    authority = load_engine_capital_authority(ebull_test_conn)
    assert authority is not None
    assert coordinates[1] in authority.core_active_position_ids
    with pytest.raises(EngineCapitalObservationError) as raised:
        resolve_engine_capital_usage(
            authority,
            _provider(broker).get_account_risk_snapshot(),
            core_instrument_id=CORE_INSTRUMENT_ID,
        )
    assert str(raised.value) == f"active core position {coordinates[1]} is absent from broker snapshot"
    assert raised.value.reason_code == "engine_capital_ownership_unwitnessed"
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


# ---------------------------------------------------------------------------
# Round 5 — the NON-CRASH close failures
#
# A different axis from every scenario above: no SIGKILL, no restart, one live
# process the whole way through.  What varies is how the broker ANSWERS, and the
# four classes below are the ones `manage_owned_position` actually distinguishes.
# They matter because they are what #2603's sell leg will meet in production far
# more often than a crash.
# ---------------------------------------------------------------------------


def _close(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker, coordinates: tuple[int, int]) -> Any:
    """Ask for a close in-process, the way the attended route does."""
    strategy_trade_id, broker_position_id = coordinates
    return manage_owned_position(
        conn,
        broker=_provider(broker),
        strategy_trade_id=strategy_trade_id,
        broker_position_id=broker_position_id,
        close_reason="operator_close",
        now=CLOCK,
    )


def test_a_definitely_rejected_close_costs_the_request_and_leaves_the_position_owned(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Classification: SAFELY STOPPED, and immediately re-requestable.

    A definite rejection is the one close failure where we know the broker did
    NOT act, so the safe state is the one we started in: position owned, trade
    back to ``open``, and the next request free to try again. Anything that left
    the trade in a reconcile state would make a routine refusal — insufficient
    margin, a closed venue — look like a lost close and pull it into the same
    manual queue as 7b.

    ⚠ The double raises BEFORE recording the close, which is what "definite"
    means. The counter assertions are the evidence: ``close_calls == 0``.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)
    broker = _restarted_engine_broker(core_world)
    broker.close_failure = "rejected"

    rejected = _close(ebull_test_conn, broker, coordinates)
    assert rejected.state == "rejected"
    assert rejected.reason_code == "broker_close_rejected"
    assert broker.read()["close_calls"] == 0
    assert broker.read()["closes"] == []

    report = close_state_report(ebull_test_conn)
    assert report["close_statuses"] == ["rejected"]
    assert report["close_error_codes"] == ["broker_close_rejected"]
    assert report["active_ownership"] == 1
    assert report["released_ownership"] == 0
    # Back to `open`, NOT `reconcile_required` -- the difference from every
    # uncertain-or-lost case in this module.
    assert report["trade_statuses"] == ["open"]
    assert report["exit_order_statuses"] == ["rejected"]

    # Accounting is intact and the allocator still runs.
    held = _execute_core(ebull_test_conn, broker)
    assert held.state == "held"
    assert held.reason_code == "core_hold"

    # And the next request goes through, once.
    broker.close_failure = None
    accepted = _close(ebull_test_conn, broker, coordinates)
    assert accepted.state == "submitted"
    assert accepted.reason_code == "broker_close_accepted"
    assert broker.read()["close_calls"] == 1
    assert close_state_report(ebull_test_conn)["close_statuses"] == ["rejected", "submitted"]


def test_an_uncertain_close_holds_the_position_and_does_not_break_the_capital_reader(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Classification: SAFELY STOPPED, awaiting reconciliation.

    The dangerous class: the broker MAY have closed the position and our side
    does not know.

    ⚠⚠ **I set out to assert that the capital reader keeps working here, and it
    does not — the test was wrong, not the code.** The scenario is run in BOTH
    arms of the ambiguity, which is what makes the real property visible:

    - **taken** — the double records the close, then raises. The broker's
      snapshot no longer carries the position while our ownership row does, so
      ``resolve_engine_capital_usage`` raises
      ``engine_capital_ownership_unwitnessed``. **That is 7b's wedge exactly.**
      `broker_close_uncertain` is therefore NOT a milder state than a lost
      acceptance: when the ambiguity resolves "the broker did act", it IS one.
    - **not taken** — the double raises before recording. The position is still
      there, the reader works, and the allocator holds.

    OUR side is byte-identical across the two — same operation status, same error
    code, same ownership, same order status — and that is the whole justification
    for `reconcile_required`: the state is genuinely undecidable from our records
    alone, so nothing may be auto-released and nothing may be auto-retried.

    ⚠ The exit-side #2965 question is answered in passing and separately: it is
    ``resolve_engine_capital_usage`` that refuses, on a witness mismatch, and
    ``load_engine_capital_authority`` returns normally in both arms. The
    entry-side defect (a pending claim making the AUTHORITY load raise) has no
    twin here.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)
    _, broker_position_id = coordinates
    broker = _restarted_engine_broker(core_world)
    broker.close_failure = "uncertain"

    uncertain = _close(ebull_test_conn, broker, coordinates)
    assert uncertain.state == "reconcile_required"
    assert uncertain.reason_code == "broker_close_uncertain"
    # The broker DID take it -- that is what makes this uncertain rather than
    # rejected, and a double that discarded it would make the scenario vacuous.
    assert broker.read()["close_calls"] == 1

    taken_report = close_state_report(ebull_test_conn)
    assert taken_report["close_statuses"] == ["reconcile_required"]
    assert taken_report["close_error_codes"] == ["broker_close_uncertain"]
    assert taken_report["active_ownership"] == 1
    assert taken_report["released_ownership"] == 0
    assert taken_report["trade_statuses"] == ["reconcile_required"]
    # `submitted`, not `rejected`: the order may exist at the broker.
    assert taken_report["exit_order_statuses"] == ["submitted"]

    # The authority load is fine; it is the WITNESS join that refuses.
    authority = load_engine_capital_authority(ebull_test_conn)
    assert authority is not None
    assert broker_position_id in authority.core_active_position_ids
    with pytest.raises(EngineCapitalObservationError) as raised:
        resolve_engine_capital_usage(
            authority,
            _provider(broker).get_account_risk_snapshot(),
            core_instrument_id=CORE_INSTRUMENT_ID,
        )
    assert raised.value.reason_code == "engine_capital_ownership_unwitnessed"
    ebull_test_conn.rollback()


def test_an_uncertain_close_the_broker_never_took_leaves_the_sleeve_fully_working(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """The other arm of the same ambiguity — see the test above for why it exists.

    The broker did NOT take the close. Our records say exactly what they say in
    the taken arm, and the sleeve is entirely healthy underneath: the position is
    there, the witness join resolves, and the allocator holds.

    Two identical states, two different worlds. That is the argument for
    ``reconcile_required`` rather than an automatic retry or an automatic
    release, and it is why the pair is worth more than either test alone.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)
    broker = _restarted_engine_broker(core_world)
    broker.close_failure = "uncertain_not_taken"

    uncertain = _close(ebull_test_conn, broker, coordinates)
    assert uncertain.state == "reconcile_required"
    assert uncertain.reason_code == "broker_close_uncertain"
    assert broker.read()["close_calls"] == 0, "this arm is the one where the broker did not act"

    report = close_state_report(ebull_test_conn)
    assert report["close_statuses"] == ["reconcile_required"]
    assert report["close_error_codes"] == ["broker_close_uncertain"]
    assert report["active_ownership"] == 1
    assert report["released_ownership"] == 0
    assert report["trade_statuses"] == ["reconcile_required"]
    assert report["exit_order_statuses"] == ["submitted"]

    # ... and here the witness join resolves, which is the whole difference.
    authority = load_engine_capital_authority(ebull_test_conn)
    assert authority is not None
    usage = resolve_engine_capital_usage(
        authority,
        _provider(broker).get_account_risk_snapshot(),
        core_instrument_id=CORE_INSTRUMENT_ID,
    )
    assert usage is not None
    # The authority read opened a transaction; the executor requires an idle
    # connection, as every other caller in this module does.
    ebull_test_conn.commit()
    held = _execute_core(ebull_test_conn, broker)
    assert held.state == "held"


def test_a_close_that_names_another_position_never_releases_our_ownership(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """Malformed acceptance: ``filled``, and against the wrong position.

    ``manage_owned_position`` compares the reported ``position_ids`` against the
    exact owned id (`strategy_position_manager.py:604-605`), so a close that
    looks entirely successful but names something else must NOT release
    ownership. This is the assertion that stops a broker-side id mix-up from
    silently deleting our record of a position we still hold.

    ⚠ Driven through the RESUME path, not the submission path, because the
    comparison lives there: the close is accepted normally, and the lookup on the
    next scheduled pass is what reports the wrong id.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)
    _, broker_position_id = coordinates
    broker = _restarted_engine_broker(core_world)

    accepted = _close(ebull_test_conn, broker, coordinates)
    assert accepted.state == "submitted"
    assert accepted.reason_code == "broker_close_accepted"

    broker.close_reports_position_id = broker_position_id + 99_000
    resumed = _manage(ebull_test_conn, broker, coordinates)
    assert resumed.state == "reconcile_required"
    assert resumed.reason_code == "close_order_did_not_affect_exact_position"

    report = close_state_report(ebull_test_conn)
    assert report["active_ownership"] == 1, "a close naming another position must not release ours"
    assert report["released_ownership"] == 0
    assert report["release_reasons"] == []
    assert report["trade_statuses"] == ["reconcile_required"]
    assert report["exit_order_statuses"] == ["rejected"]


def test_a_close_lookup_outage_is_a_delay_and_not_a_terminal_state(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
) -> None:
    """The exit-side twin of matrix 5b, and the property is the same one.

    An accepted close whose lookup cannot be reached must stay ``pending`` and
    write nothing terminal — a resume that guessed here would either release a
    position that is still open or strand one that is closed. When the lookup
    comes back, the same scheduled pass finishes the close with no second broker
    mutation.

    ⚠ Asserted on the double's own ``close_lookup_calls`` counter: an outage is a
    call that got no answer, and a scenario where the lookup was never attempted
    would prove nothing about outages.
    """
    coordinates = _own_one_core_position(ebull_test_conn, core_world)
    broker = _restarted_engine_broker(core_world)

    accepted = _close(ebull_test_conn, broker, coordinates)
    assert accepted.state == "submitted"

    broker.close_lookup_unreachable = True
    lookups_before = broker.read().get("close_lookup_calls", 0)
    stalled = _manage(ebull_test_conn, broker, coordinates)
    assert stalled.state == "pending"
    assert stalled.reason_code == "close_lookup_unavailable"
    assert broker.read()["close_lookup_calls"] == lookups_before + 1

    during = close_state_report(ebull_test_conn)
    assert during["close_statuses"] == ["submitted"], "an outage must not terminalise the operation"
    assert during["active_ownership"] == 1
    assert during["released_ownership"] == 0

    # The lookup returns; the same pass finishes the close, and the broker sees
    # no second close.
    broker.close_lookup_unreachable = False
    finished = _manage(ebull_test_conn, broker, coordinates)
    # `applied`, the operation's terminal state, as in 7c -- the trade is what
    # becomes `closed`, and the report below is where that is asserted.
    assert finished.state == "applied"
    after = close_state_report(ebull_test_conn)
    assert after["active_ownership"] == 0
    assert after["released_ownership"] == 1
    assert after["release_reasons"] == ["operator_close"]
    assert broker.read()["close_calls"] == 1
