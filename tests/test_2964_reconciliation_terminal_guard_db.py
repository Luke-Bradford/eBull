"""#2964 item 1 — ``_record_failure`` must not demote a terminal row.

A failure arriving for an already-``resolved``/``rejected`` order is an
OBSERVATION, not a state transition. Before the guard it was an
``ON CONFLICT DO UPDATE SET state = EXCLUDED.state, reconciled_at = NULL`` with
neither a terminal check nor a pre-lock, so a delayed 404, a transport error or
an unsafe-detail result could erase a settled outcome.

⚠ ``strategy_order_reconciliation_resolved_shape`` (sql/285) cannot catch it: it
requires ``reconciled_at IS NOT NULL`` exactly when the state is terminal, and the
demotion moved BOTH together — every step produced a valid ROW. The constraint
describes a valid row, not a valid TRANSITION.

⚠ The helper's contract is testable SINGLE-THREADED, and an earlier draft of the
spec wrongly claimed otherwise. Only the end-to-end schedule through
``reconcile_strategy_order`` needs two connections, because its public path
short-circuits on a terminal ``prior_state`` before any broker call.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from typing import Any

import psycopg
import psycopg.rows
import pytest

from app.services.strategy_order_reconciliation import (
    _TERMINAL_RECONCILIATION_STATES,
    _record_failure,
)

pytestmark = pytest.mark.integration

FAILURE_STATES = ("not_found", "ambiguous", "error")
NON_TERMINAL_STATES = ("unresolved", "pending", "not_found", "ambiguous", "error")


def _seed_order(conn: psycopg.Connection[Any], *, symbol: str) -> int:
    """A strategy-origin order with no reconciliation row yet.

    Deliberately minimal: this module tests ONE function's write semantics, so it
    seeds the FK targets and nothing else. The full deployment/trade fixtures live
    in ``test_strategy_order_reconciliation``.
    """
    # ⚠ `instruments.instrument_id` is the BROKER's id, not a sequence — it is
    # assigned, so every seed must supply one. Derived from the symbol so the id
    # is stable per test and cannot collide between parametrised cases; based
    # well above any real eToro instrument id.
    instrument_id = 2_964_000 + (abs(hash(symbol)) % 900_000)
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,%s,'Terminal Guard Test',TRUE) ON CONFLICT DO NOTHING",
        (instrument_id, symbol),
    )
    order = conn.execute(
        """
        INSERT INTO orders (instrument_id, action, order_type, requested_units, status, execution_origin)
        VALUES (%s, 'buy', 'market', 1, 'submitted', 'strategy')
        RETURNING order_id
        """,
        (instrument_id,),
    ).fetchone()
    assert order is not None
    return int(order[0])


def _seed_state(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    state: str,
    broker_status: str | None,
    error_code: str | None,
) -> None:
    terminal = state in _TERMINAL_RECONCILIATION_STATES
    conn.execute(
        """
        INSERT INTO strategy_order_reconciliation_state (
            order_id, state, reconciled_at, last_attempt_at, attempt_count,
            broker_status, last_error_code, last_payload_sha256, position_count
        ) VALUES (
            %s, %s, CASE WHEN %s THEN now() END, now() - interval '1 hour', 3,
            %s, %s, %s, 2
        )
        """,
        (order_id, state, terminal, broker_status, error_code, "c" * 64),
    )


def _read(conn: psycopg.Connection[Any], order_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM strategy_order_reconciliation_state WHERE order_id=%s", (order_id,))
        row = cur.fetchone()
    assert row is not None
    return dict(row)


# --------------------------------------------------------------------------
# The guard: 2 terminal states x 3 failure states
# --------------------------------------------------------------------------


@pytest.mark.parametrize("terminal_state", sorted(_TERMINAL_RECONCILIATION_STATES))
@pytest.mark.parametrize("failure_state", FAILURE_STATES)
def test_a_terminal_row_keeps_its_outcome_and_its_evidence(
    ebull_test_conn: psycopg.Connection[Any],
    terminal_state: str,
    failure_state: str,
) -> None:
    """Every field that carries the OUTCOME or its EVIDENCE survives.

    ⚠ ``broker_status`` and ``last_error_code`` are in that set and preserving them
    is not tidiness. Every call site passes ``broker_status=None``, so assigning it
    would replace a terminal ``Filled`` with NULL; and a retained
    ``last_error_code`` makes the API add a ``*_reconciliation_error`` to a RESOLVED
    trade's lifecycle ``incomplete_reasons``, which nothing would ever clear.
    """
    conn = ebull_test_conn
    order_id = _seed_order(conn, symbol=f"TG.{terminal_state[:3]}.{failure_state[:3]}")
    _seed_state(
        conn,
        order_id=order_id,
        state=terminal_state,
        broker_status="Filled" if terminal_state == "resolved" else "Rejected",
        error_code=None,
    )
    before = _read(conn, order_id)

    result = _record_failure(
        conn,
        order_id=order_id,
        state=failure_state,  # type: ignore[arg-type]
        error_code="broker_order_not_found",
    )

    after = _read(conn, order_id)
    assert after["state"] == terminal_state
    assert after["reconciled_at"] == before["reconciled_at"]
    assert after["broker_status"] == before["broker_status"]
    assert after["last_error_code"] is None
    # The observation IS recorded — exactly one attempt, and the clock moved.
    assert after["attempt_count"] == before["attempt_count"] + 1
    assert after["last_attempt_at"] > before["last_attempt_at"]
    # The returned result describes the STORED row, not the attempted write.
    assert result.state == terminal_state
    assert result.broker_status == before["broker_status"]
    assert result.error_code is None
    conn.rollback()


@pytest.mark.parametrize("terminal_state", sorted(_TERMINAL_RECONCILIATION_STATES))
def test_a_terminal_rows_prior_evidence_fields_are_immutable(
    ebull_test_conn: psycopg.Connection[Any],
    terminal_state: str,
) -> None:
    """``first_unresolved_at``, the payload hash and the position count belong to
    the resolve and must not be touched by a later failure."""
    conn = ebull_test_conn
    order_id = _seed_order(conn, symbol=f"TG.IMM.{terminal_state[:3]}")
    _seed_state(conn, order_id=order_id, state=terminal_state, broker_status="Filled", error_code=None)
    before = _read(conn, order_id)

    _record_failure(conn, order_id=order_id, state="error", error_code="broker_lookup_error")

    after = _read(conn, order_id)
    assert after["first_unresolved_at"] == before["first_unresolved_at"]
    assert after["last_payload_sha256"] == before["last_payload_sha256"]
    assert after["position_count"] == before["position_count"]
    conn.rollback()


# --------------------------------------------------------------------------
# The control: a non-terminal row must still be written
# --------------------------------------------------------------------------


@pytest.mark.parametrize("prior_state", NON_TERMINAL_STATES)
def test_a_non_terminal_row_still_takes_the_failure(
    ebull_test_conn: psycopg.Connection[Any],
    prior_state: str,
) -> None:
    """Without this, every assertion above could hold because nothing writes."""
    conn = ebull_test_conn
    order_id = _seed_order(conn, symbol=f"TG.NT.{prior_state[:4]}")
    _seed_state(conn, order_id=order_id, state=prior_state, broker_status="Placed", error_code="older_code")
    before = _read(conn, order_id)

    result = _record_failure(
        conn,
        order_id=order_id,
        state="ambiguous",
        error_code="unsafe_broker_detail",
        broker_status="Received",
    )

    after = _read(conn, order_id)
    assert after["state"] == "ambiguous"
    assert after["reconciled_at"] is None
    assert after["broker_status"] == "Received"
    assert after["last_error_code"] == "unsafe_broker_detail"
    assert after["attempt_count"] == before["attempt_count"] + 1
    assert result.state == "ambiguous"
    assert result.error_code == "unsafe_broker_detail"
    conn.rollback()


def test_an_absent_row_is_inserted_unchanged(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The INSERT arm is untouched by the guard."""
    conn = ebull_test_conn
    order_id = _seed_order(conn, symbol="TG.NEW")

    result = _record_failure(conn, order_id=order_id, state="not_found", error_code="broker_order_not_found")

    after = _read(conn, order_id)
    assert after["state"] == "not_found"
    assert after["reconciled_at"] is None
    assert after["attempt_count"] == 1
    assert after["last_error_code"] == "broker_order_not_found"
    assert result.state == "not_found"
    conn.rollback()


# --------------------------------------------------------------------------
# Revert-probe, in the suite rather than only in the PR
# --------------------------------------------------------------------------


def test_the_pre_guard_statement_really_did_demote_a_terminal_row(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The guarded assertions above cannot pass for the wrong reason.

    This runs the ORIGINAL statement verbatim against the same fixture and
    asserts the corruption — including that the CHECK does not stop it, which is
    the whole reason a schema constraint was not the fix.
    """
    conn = ebull_test_conn
    order_id = _seed_order(conn, symbol="TG.PROBE")
    _seed_state(conn, order_id=order_id, state="resolved", broker_status="Filled", error_code=None)

    conn.execute(
        """
        INSERT INTO strategy_order_reconciliation_state (
            order_id, state, last_attempt_at, attempt_count, broker_status,
            last_error_code, updated_at
        ) VALUES (%s, %s, now(), 1, %s, %s, now())
        ON CONFLICT (order_id) DO UPDATE SET
            state = EXCLUDED.state,
            last_attempt_at = now(),
            reconciled_at = NULL,
            attempt_count = strategy_order_reconciliation_state.attempt_count + 1,
            broker_status = EXCLUDED.broker_status,
            last_error_code = EXCLUDED.last_error_code,
            updated_at = now()
        """,
        (order_id, "not_found", None, "broker_order_not_found"),
    )

    after = _read(conn, order_id)
    assert after["state"] == "not_found", "the pre-guard statement demoted a resolved row"
    assert after["reconciled_at"] is None
    assert after["broker_status"] is None, "and erased the terminal broker status"
    conn.rollback()


# --------------------------------------------------------------------------
# The trade gate lives in `test_strategy_order_reconciliation`
# --------------------------------------------------------------------------
#
# `_record_failure`'s second write -- `strategy_trades.status =
# 'reconcile_required'` -- is gated on the STORED state by the same change, and
# both directions plus the pre-existing `NOT IN ('closed','failed')` exclusion are
# asserted in `tests/test_strategy_order_reconciliation.py`.
#
# They live there deliberately rather than here: a `strategy_trades` row is
# created through `create_strategy_trade(conn, decision_id)` off a funding
# decision, and that module already owns `_seed_deployment` / `_seed_order`.
# Hand-rolling a second trade seeder in this file would be a second definition of
# how a strategy trade comes into existence -- and the first draft's attempt
# invented a `strategy_id`/`mode` column pair the table does not have.
