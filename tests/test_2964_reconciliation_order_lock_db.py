"""#2964 items 2-4 — the per-order reconciliation lock.

The write path was only safe while exactly one reconciler existed per order.
Item 1 stopped a late failure demoting a terminal row; this is the other half —
making two reconcilers, or a reconciler and a submitter, safe against each other.

⚠ **No start-``Barrier`` races here.** A barrier guarantees a common start, not
an overlap, and a barrier placed inside a broker stub hangs when only one caller
reaches it. Every case instead has a SECOND connection hold the lock outright and
then runs the subject on the first, so the interleave under test is the one
asserted on every run. Each case that can be satisfied vacuously also carries its
control arm — the same call with the lock free — or it could not fail.

⚠ Prevention-log rule "Architectural-invariant claims about Postgres advisory
locks must be empirically tested before they ship" (#1233): every PG semantic the
design leans on is asserted here against a real server, not quoted from docs.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a
test source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

import threading
import time
import zlib
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import psycopg.errors
import psycopg.rows
import pytest

from app.providers.broker import BrokerOrderDetail, BrokerPositionExecution, BrokerProvider
from app.services import strategy_order_reconciliation as recon
from app.services.strategy_control_plane import (
    create_strategy_trade,
    decide_funding,
    link_strategy_order,
)
from app.services.strategy_order_reconciliation import (
    StrategyReconciliationBusy,
    StrategyReconciliationError,
    ensure_strategy_request_id,
    reconcile_backlog,
    reconcile_strategy_order,
    reconciliation_order_lock,
    try_reconciliation_order_lock,
)
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_strategy_order_reconciliation import _seed_deployment

pytestmark = pytest.mark.integration


# --------------------------------------------------------------------------
# Seeding
# --------------------------------------------------------------------------


def _seed_order(conn: psycopg.Connection[Any], *, symbol: str, deployment_id: int) -> tuple[int, int]:
    """A funded strategy trade and its submitted order. Returns (trade, order).

    ⚠ The funding chain is not ceremony: ``strategy_trades_exactly_one_authorisation``
    requires either a funding decision or a core rebalance intent, so a bare
    ``INSERT INTO strategy_trades`` is rejected by the schema. Reuses the
    signal -> funding -> trade shape from ``test_strategy_order_reconciliation``.

    ⚠ ``instruments.instrument_id`` is the BROKER's id, not a sequence, so every
    seed supplies one. ``zlib.crc32`` and not ``hash()``: Python randomises str
    hashing per interpreter, so ``hash()`` is stable only within one run and a
    future failure would be irreproducible.
    """
    instrument_id = 2_964_500 + (zlib.crc32(symbol.encode()) % 400_000)
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,%s,'Order Lock Test',TRUE) ON CONFLICT DO NOTHING",
        (instrument_id, symbol),
    )
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe,
            input_rule_set_versions
        ) VALUES ('S-REC', 'v1', %s, '2026-08-06', 'entry', 'fired',
                  '2026-08-07', 100, 'survivor_only',
                  '{"indicator_series":"rules-v1"}'::jsonb)
        RETURNING signal_id
        """,
        (instrument_id,),
    ).fetchone()
    assert signal is not None
    decision_id = decide_funding(
        conn,
        signal_id=int(signal[0]),
        verdict="allocated",
        deployment_id=deployment_id,
        amount=Decimal("100"),
        reason_code="test",
    )
    trade_id = create_strategy_trade(conn, decision_id)
    order = conn.execute(
        """
        INSERT INTO orders (instrument_id, action, order_type, requested_amount, status, execution_origin)
        VALUES (%s, 'BUY', 'MARKET', 100, 'submitted', 'strategy')
        RETURNING order_id
        """,
        (instrument_id,),
    ).fetchone()
    assert order is not None
    order_id = int(order[0])
    link_strategy_order(conn, strategy_trade_id=trade_id, order_id=order_id, purpose="entry")
    ensure_strategy_request_id(conn, order_id=order_id)
    conn.commit()
    return trade_id, order_id


@pytest.fixture
def deployment_id(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> int:
    """One paper deployment per test; the promotion rows may only be inserted once."""
    return _seed_deployment(ebull_test_conn)


def _detail(*, instrument_id: int, broker_status: str = "Filled") -> BrokerOrderDetail:
    execution = BrokerPositionExecution(
        position_id=964_000_001,
        state="open",
        remaining_units=Decimal("1"),
        opening_units=Decimal("1"),
        average_price=Decimal("100"),
        execution_time=datetime(2026, 9, 13, 9, 0, tzinfo=UTC),
        fees=Decimal("0.1"),
        raw_payload={"positionId": 964_000_001},
    )
    return BrokerOrderDetail(
        broker_order_ref="96400001",
        reference_id=None,
        status="filled",
        broker_status=broker_status,
        instrument_id=instrument_id,
        position_executions=(execution,),
        last_update=datetime(2026, 9, 13, 9, 1, tzinfo=UTC),
        raw_payload={"orderId": 96400001, "status": {"name": broker_status}},
    )


# ⚠ Both readers COMMIT. Under `autocommit=False` a bare SELECT leaves the
# connection INTRANS, and every subject under test here refuses a non-idle
# connection — so a helper that reads without committing fails the next call for
# a reason that has nothing to do with what is being asserted.
def _instrument_of(conn: psycopg.Connection[Any], order_id: int) -> int:
    row = conn.execute("SELECT instrument_id FROM orders WHERE order_id=%s", (order_id,)).fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def _state(conn: psycopg.Connection[Any], order_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM strategy_order_reconciliation_state WHERE order_id=%s", (order_id,))
        row = cur.fetchone()
    conn.commit()
    assert row is not None
    return dict(row)


@pytest.fixture
def holder(ebull_test_conn: psycopg.Connection[Any]) -> Any:
    """A second real connection, used to HOLD a lock against the subject.

    ⚠ Depends on ``ebull_test_conn`` purely for its availability check: that
    fixture ``pytest.skip``s when no test DB is reachable, and a bare
    ``psycopg.connect`` here would instead ERROR on a box without Postgres --
    the one failure shape that is NOT a clean skip.
    """
    conn = psycopg.connect(test_database_url())
    try:
        yield conn
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 1. Postgres semantics the design leans on — measured, never quoted
# --------------------------------------------------------------------------


def test_the_lock_key_is_equal_across_sessions_and_survives_commit(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
) -> None:
    """The three properties a row lock does NOT have, which is why this is advisory.

    ⚠ The middle one is the whole reason ``FOR UPDATE SKIP LOCKED`` — what the
    ticket proposed — cannot work here: ``reconcile_backlog`` MUST commit after
    selecting, because broker I/O may not run inside a DB transaction.
    """
    params = recon._order_lock_params(4242)
    mine = ebull_test_conn.execute(f"SELECT {recon._ORDER_LOCK_KEY_SQL}", params).fetchone()
    theirs = holder.execute(f"SELECT {recon._ORDER_LOCK_KEY_SQL}", params).fetchone()
    ebull_test_conn.commit()
    holder.commit()
    assert mine == theirs, "two sessions must compute the same key or the lock serialises nothing"

    with try_reconciliation_order_lock(ebull_test_conn, 4242):
        # Survives the helper's own commit AND a caller commit.
        ebull_test_conn.commit()
        assert holder.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params).fetchone() == (
            False,
        )
        holder.commit()
    assert holder.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params).fetchone() == (True,)
    holder.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", params)
    holder.commit()


def test_a_dead_connection_cannot_wedge_an_order(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """Crash release is why no lease column and no stale-claim reaper exist.

    ⚠ Takes ``ebull_test_conn`` it does not otherwise use, for its skip-when-no-DB
    guard: two bare ``psycopg.connect`` calls would ERROR rather than skip on a
    box without Postgres.
    """
    params = recon._order_lock_params(4243)
    doomed = psycopg.connect(test_database_url())
    doomed.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params)
    doomed.commit()
    doomed.close()

    survivor = psycopg.connect(test_database_url())
    try:
        assert survivor.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params).fetchone() == (
            True,
        )
        survivor.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", params)
        survivor.commit()
    finally:
        survivor.close()


def test_a_bigint_order_id_beyond_int4_keys_correctly(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
) -> None:
    """``orders.order_id`` is BIGINT, and the prevention log bans an int4 cast.

    A two-``int4`` key (``core_submission_lock``'s shape) would raise or wrap for
    this id. ``hashtextextended`` over a namespaced identity does not.
    """
    huge = 9_223_372_036_854_775_000
    with try_reconciliation_order_lock(ebull_test_conn, huge):
        params = recon._order_lock_params(huge)
        assert holder.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params).fetchone() == (
            False,
        )
        holder.commit()


def test_the_lock_key_sql_is_spelled_in_exactly_one_module() -> None:
    """Prevention log: two writers sharing an advisory lock need ONE key SQL.

    Satisfied structurally rather than by a grep tripwire — but only while the
    template really does live in one place, which is what this asserts.
    """
    from pathlib import Path

    hits = [path for path in Path("app").rglob("*.py") if "strategy-order-reconciliation:" in path.read_text()]
    assert hits == [Path("app/services/strategy_order_reconciliation.py")], hits


# --------------------------------------------------------------------------
# 2. Transaction hygiene — the helper owns its transaction
# --------------------------------------------------------------------------


def test_the_connection_is_idle_after_acquire_refusal_and_exception(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
) -> None:
    """Under ``autocommit=False`` the acquire SELECT itself opens a transaction.

    Without the helper's own commits the caller's next statement runs inside a
    stray transaction and ``reconcile_strategy_order``'s idle check fails — so a
    contention refusal would poison the NEXT order in the backlog loop, which is
    a worse bug than the one being fixed.
    """
    conn = ebull_test_conn
    idle = psycopg.pq.TransactionStatus.IDLE

    with try_reconciliation_order_lock(conn, 4244):
        assert conn.info.transaction_status == idle, "acquire must commit"
    assert conn.info.transaction_status == idle, "release must commit"

    params = recon._order_lock_params(4245)
    holder.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params)
    holder.commit()
    with pytest.raises(StrategyReconciliationBusy):
        with try_reconciliation_order_lock(conn, 4245):
            pytest.fail("unreachable")
    assert conn.info.transaction_status == idle, "the CONTENTION path must commit too"

    with pytest.raises(RuntimeError, match="boom"):
        with try_reconciliation_order_lock(conn, 4246):
            conn.execute("SELECT 1")  # leaves an open transaction behind
            raise RuntimeError("boom")
    assert conn.info.transaction_status == idle
    # ...and the lock was genuinely released, not merely reported released.
    assert conn.execute(
        f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", recon._order_lock_params(4246)
    ).fetchone() == (True,)
    conn.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", recon._order_lock_params(4246))
    conn.commit()


def test_a_non_idle_connection_is_refused(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    conn.execute("SELECT 1")
    with pytest.raises(StrategyReconciliationError, match="idle connection"):
        with try_reconciliation_order_lock(conn, 4247):
            pytest.fail("unreachable")
    conn.rollback()


def test_a_nested_acquire_raises_rather_than_silently_double_counting(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Measured against the server: a nested acquire returns TRUE, and the first
    unlock returns TRUE while leaving the lock HELD. An inner context manager
    would therefore report success and release nothing. No path nests today; this
    makes a future one fail loudly instead of quietly losing mutual exclusion.
    """
    conn = ebull_test_conn
    with try_reconciliation_order_lock(conn, 4248):
        with pytest.raises(StrategyReconciliationError, match="already held"):
            with try_reconciliation_order_lock(conn, 4248):
                pytest.fail("unreachable")
    # A DIFFERENT order in the same context is fine — the guard is per order.
    with try_reconciliation_order_lock(conn, 4249):
        pass


def test_two_connections_in_one_context_may_hold_different_orders(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
) -> None:
    """The nesting guard is per (connection, order), not per order.

    Review nitpick on PR #2975. The hazard it exists for is ONE session acquiring
    twice; two different connections in the same call context is the ordinary
    contention this module resolves at the server. Keying on the order alone would
    have invented a conflict Postgres does not have — and, worse, a SECOND caller
    on its own connection would have been refused before ever asking the server,
    so a legitimate wait would have surfaced as a programming error.
    """
    with try_reconciliation_order_lock(ebull_test_conn, 4251):
        with try_reconciliation_order_lock(holder, 4252):
            pass
    # Same ORDER on the second connection is a real conflict, resolved by the
    # server (Busy), not by the local guard.
    with try_reconciliation_order_lock(ebull_test_conn, 4253):
        with pytest.raises(StrategyReconciliationBusy):
            with try_reconciliation_order_lock(holder, 4253):
                pytest.fail("unreachable")


def test_the_bodys_exception_survives_a_lost_lock_at_release(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A release failure must not replace the caller's real failure.

    Review WARNING on PR #2975. Raising the lost-ownership error from the exit
    path while another exception is propagating would surface a broker or
    persistence failure as a lock-ownership message — less informative, and
    pointing at the wrong subsystem entirely. It is logged instead.

    ⚠ The lock is stolen mid-body by unlocking it from the holder connection
    (advisory locks are not owner-checked across sessions for this purpose here:
    the release simply finds nothing to release and returns FALSE), which is the
    only way to reach the lost-ownership branch deterministically.
    """
    conn = ebull_test_conn
    params = recon._order_lock_params(4254)
    with caplog.at_level("ERROR"):
        with pytest.raises(ValueError, match="the real failure"):
            with try_reconciliation_order_lock(conn, 4254):
                # Release it out from under the context manager, so its own
                # unlock returns FALSE.
                conn.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", params)
                conn.commit()
                raise ValueError("the real failure")
    assert any("ownership for 4254 was lost" in record.getMessage() for record in caplog.records)

    # Control arm: with NO exception in flight, the same lost lock DOES raise —
    # otherwise the assertion above would pass for a helper that never checks.
    with pytest.raises(StrategyReconciliationError, match="ownership for 4255 was lost"):
        with try_reconciliation_order_lock(conn, 4255):
            conn.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", recon._order_lock_params(4255))
            conn.commit()
    assert holder is not None  # the fixture supplies this module's skip-when-no-DB guard


# --------------------------------------------------------------------------
# 3. Item 2 — the reconciler claims, and the backlog skips what it cannot claim
# --------------------------------------------------------------------------


def test_a_second_reconciler_is_refused_and_makes_no_broker_call(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
    deployment_id: int,
) -> None:
    conn = ebull_test_conn
    _, order_id = _seed_order(conn, symbol="LOCK.ONE", deployment_id=deployment_id)
    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.return_value = _detail(instrument_id=_instrument_of(conn, order_id))

    params = recon._order_lock_params(order_id)
    holder.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params)
    holder.commit()
    with pytest.raises(StrategyReconciliationBusy):
        reconcile_strategy_order(conn, broker=broker, order_id=order_id)
    broker.lookup_order.assert_not_called()
    assert _state(conn, order_id)["state"] == "unresolved", "a refusal must write nothing"

    # Control arm: with the lock free the identical call proceeds, so the
    # assertion above is not vacuous.
    holder.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", params)
    holder.commit()
    assert reconcile_strategy_order(conn, broker=broker, order_id=order_id).state == "resolved"


def test_the_backlog_skips_a_held_row_without_inventing_a_failure(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
    deployment_id: int,
) -> None:
    """A busy row must not be recorded as an error, and must not lose its place.

    ⚠ ``last_attempt_at`` is deliberately NOT advanced: advancing it would push
    the row to the back of #2948's least-recently-attempted rotation for somebody
    else's work, and ``_record_failure`` would additionally invent an error code
    that no broker ever returned.
    """
    conn = ebull_test_conn
    _, held_id = _seed_order(conn, symbol="LOCK.HELD", deployment_id=deployment_id)
    _, free_id = _seed_order(conn, symbol="LOCK.FREE", deployment_id=deployment_id)
    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.return_value = _detail(instrument_id=_instrument_of(conn, free_id))

    before = _state(conn, held_id)
    params = recon._order_lock_params(held_id)
    holder.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params)
    holder.commit()
    results = reconcile_backlog(conn, broker=broker, limit=50)

    assert [result.order_id for result in results] == [free_id], "the free row is still worked"
    after = _state(conn, held_id)
    assert (after["state"], after["attempt_count"], after["last_attempt_at"], after["last_error_code"]) == (
        before["state"],
        before["attempt_count"],
        before["last_attempt_at"],
        before["last_error_code"],
    )

    holder.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", params)
    holder.commit()
    # Control arm: once free, the same batch reaches it — so the skip above is a
    # skip and not an ordinary selection miss.
    assert held_id in {result.order_id for result in reconcile_backlog(conn, broker=broker, limit=50)}


# --------------------------------------------------------------------------
# 4. Submitters BLOCK — a refusal would abandon a durable authority
# --------------------------------------------------------------------------


def test_a_submitter_waits_for_the_lock_instead_of_refusing(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
) -> None:
    """The asymmetry between the two entry points, asserted.

    A submitter's ``orders`` and reconciliation rows are already committed by the
    time it calls the broker, and ``resume_*`` is deliberately forbidden to
    resubmit — so a ``Busy`` refusal here would strand a durable authority and
    wedge the arm. It must wait.
    """
    conn = ebull_test_conn
    params = recon._order_lock_params(4250)
    holder.execute(f"SELECT pg_try_advisory_lock({recon._ORDER_LOCK_KEY_SQL})", params)
    holder.commit()

    def _release_after_delay() -> None:
        time.sleep(0.4)
        holder.execute(f"SELECT pg_advisory_unlock({recon._ORDER_LOCK_KEY_SQL})", params)
        holder.commit()

    releaser = threading.Thread(target=_release_after_delay)
    started = time.monotonic()
    releaser.start()
    with reconciliation_order_lock(conn, 4250):
        waited = time.monotonic() - started
    releaser.join()
    assert waited >= 0.3, f"the submitter returned in {waited:.3f}s, so it did not wait for the holder"


# --------------------------------------------------------------------------
# 5. Item 4 — the submission/reconciliation CHECK violation the lock closes
# --------------------------------------------------------------------------


def test_persisting_acceptance_over_a_resolved_row_violates_the_check(
    ebull_test_conn: psycopg.Connection[Any],
    deployment_id: int,
) -> None:
    """The control arm for item 4: the hazard is real, and it is not hypothetical.

    ``_persist_core_acceptance`` writes ``state='pending'`` without clearing
    ``reconciled_at``. If a reconciler resolves the order inside the submission
    window, that statement leaves a terminal ``reconciled_at`` beside a
    non-terminal state, which ``strategy_order_reconciliation_resolved_shape``
    refuses — failing the ATTENDED request, not the background one.

    ⚠ This is why item 4 is closed by the lock rather than by a terminal-preserving
    ``CASE``: preserving ``resolved`` here is not the right outcome either, because
    the submitter genuinely did just get an acceptance. The only sound answer is
    that the interleave cannot happen, which the lock provides and this asserts the
    need for.
    """
    conn = ebull_test_conn
    _, order_id = _seed_order(conn, symbol="LOCK.ITEM4", deployment_id=deployment_id)
    conn.execute(
        "UPDATE strategy_order_reconciliation_state SET state='resolved', reconciled_at=now() WHERE order_id=%s",
        (order_id,),
    )
    conn.commit()
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            """
            UPDATE strategy_order_reconciliation_state
            SET state='pending', broker_status='accepted', last_attempt_at=now(),
                attempt_count=attempt_count+1, last_error_code=NULL, updated_at=now()
            WHERE order_id=%s
            """,
            (order_id,),
        )
    conn.rollback()
