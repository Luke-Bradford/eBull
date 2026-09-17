"""#2942 half 2 — which stranded claims may be released, against real rows.

The decision is a WHERE clause plus an advisory lock, and neither is answerable
by a mocked cursor: a mock can prove the SQL text contains the words, not that a
``broker_verb_entered`` row survives the UPDATE, nor that a key held by a second
backend actually blocks. The ordering proof (claim commit → marker commit →
broker call) is pure and lives in ``tests/test_order_client.py``.

Own module because the ``db`` marker is module-scoped: one DB test inside
``tests/test_order_client.py`` would evict that whole file from the
``-m "not db"`` push gate.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services.order_client import (
    RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS,
    ConcurrentSubmissionInFlightError,
    terminalise_unsubmitted_recommendation_attempt,
)
from tests.fixtures.ebull_test_db import test_database_url

INSTRUMENT_ID = 990_942
_NOW = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)


def _seed_instrument(conn: psycopg.Connection[Any]) -> None:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'MARK.2942','Submission Marker Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )


def _seed_recommendation(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(
        "INSERT INTO trade_recommendations (instrument_id, action, rationale, status) "
        "VALUES (%s,'BUY','marker test','approved') RETURNING recommendation_id",
        (INSTRUMENT_ID,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_attempt(
    conn: psycopg.Connection[Any],
    *,
    recommendation_id: int,
    status: str = "submitted",
    phase: str | None = "claim_committed",
) -> int:
    row = conn.execute(
        """
        INSERT INTO orders
            (instrument_id, recommendation_id, action, order_type, status,
             raw_payload_json, created_at, recommendation_request_id,
             recommendation_submission_phase)
        VALUES
            (%(iid)s, %(rid)s, 'BUY', 'market', %(status)s,
             '{}'::jsonb, %(now)s, %(req)s, %(phase)s)
        RETURNING order_id
        """,
        {
            "iid": INSTRUMENT_ID,
            "rid": recommendation_id,
            "status": status,
            "now": _NOW,
            "req": uuid4(),
            "phase": phase,
        },
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def _status_of(conn: psycopg.Connection[Any], order_id: int) -> str:
    row = conn.execute("SELECT status FROM orders WHERE order_id=%s", (order_id,)).fetchone()
    assert row is not None
    return str(row[0])


def test_a_claim_committed_attempt_is_released(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The window this slice exists for: the claim is on disk, the marker never
    moved, so the broker verb was never entered and no order can exist."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec)

    released = terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW)

    assert released == order_id
    assert _status_of(ebull_test_conn, order_id) == "refused"


def test_the_released_claim_actually_lifts(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Releasing is only useful if ``idx_orders_recommendation_open_attempt``
    stops holding — that index, not the application, is what refuses. Assert on
    a second INSERT rather than on the status string."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_attempt(ebull_test_conn, recommendation_id=rec)

    terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW)

    # A fresh claim for the same recommendation now succeeds. Before the
    # release this raises UniqueViolation.
    second = _seed_attempt(ebull_test_conn, recommendation_id=rec)
    assert _status_of(ebull_test_conn, second) == "submitted"


def test_a_broker_verb_entered_attempt_is_NOT_released(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The dangerous direction. The marker committed, so the verb may have been
    entered and an order may exist at the broker; releasing the claim here is
    the duplicate-order bug this ticket was filed about."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec, phase="broker_verb_entered")

    assert terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW) is None
    assert _status_of(ebull_test_conn, order_id) == "submitted"


def test_an_uncertain_attempt_that_entered_the_verb_is_NOT_released(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``uncertain`` is in the claim predicate, so it has to be shown that the
    MARKER and not the status is what decides."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec, status="uncertain", phase="broker_verb_entered")

    assert terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW) is None
    assert _status_of(ebull_test_conn, order_id) == "uncertain"


def test_a_null_marker_row_is_NOT_released(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """NULL is not "no marker yet" — it is "not written by this path, or
    predates the column". Every pre-existing row is NULL and none of them may be
    reinterpreted as provably-unsubmitted."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec, phase=None)

    assert terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW) is None
    assert _status_of(ebull_test_conn, order_id) == "submitted"


def test_a_terminal_row_is_not_re_released(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A row already outside the claim predicate holds nothing, so it is not a
    candidate however its marker reads — otherwise a settled order would collect
    a fresh refusal audit on every pass."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec, status="refused")

    assert terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW) is None
    assert _status_of(ebull_test_conn, order_id) == "refused"


def test_another_session_holding_the_key_blocks_the_release(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The second condition, and the reason the marker alone is unsafe.

    A live submitter between its claim commit and its marker commit leaves a row
    reading exactly ``claim_committed``. Only the advisory lock tells the two
    apart, and it has to be a genuinely different backend — this is precisely
    what a mocked connection cannot express.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec)

    with psycopg.connect(test_database_url()) as other:
        held = other.execute(
            "SELECT pg_try_advisory_lock(%s, %s)",
            (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, rec),
        ).fetchone()
        other.commit()
        assert held == (True,)
        with pytest.raises(ConcurrentSubmissionInFlightError):
            terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW)

    # Untouched: a refusal to decide must not leave a half-resolved row.
    assert _status_of(ebull_test_conn, order_id) == "submitted"


def test_a_key_held_for_a_DIFFERENT_recommendation_does_not_block(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Why the key is per-recommendation. A global evidence key would let one
    unrelated in-flight submission starve every other recommendation (#2961's
    blast-radius finding)."""
    _seed_instrument(ebull_test_conn)
    mine = _seed_recommendation(ebull_test_conn)
    other_rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=mine)

    with psycopg.connect(test_database_url()) as other:
        other.execute(
            "SELECT pg_try_advisory_lock(%s, %s)",
            (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, other_rec),
        ).fetchone()
        other.commit()
        assert (
            terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=mine, now=_NOW)
            == order_id
        )


def test_the_reentrant_caller_still_owns_the_key_afterwards(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``execute_order`` holds this key across the whole span and then calls the
    terminaliser, whose own nested try succeeds because advisory locks are
    reentrant. The release decrements a reference count rather than freeing the
    key, so the outer holder must still own it — assert that, not just that the
    call worked."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec)

    outer = ebull_test_conn.execute(
        "SELECT pg_try_advisory_lock(%s, %s)",
        (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, rec),
    ).fetchone()
    ebull_test_conn.commit()
    assert outer == (True,)

    assert terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW) == order_id

    held = ebull_test_conn.execute(
        """
        SELECT count(*) FROM pg_locks
        WHERE locktype='advisory' AND classid=%s AND objid=%s
          AND pid = pg_backend_pid() AND granted
        """,
        (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, rec),
    ).fetchone()
    ebull_test_conn.commit()
    assert held == (1,), "the nested release freed the outer holder's key"

    ebull_test_conn.execute(
        "SELECT pg_advisory_unlock(%s, %s)",
        (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, rec),
    ).fetchone()
    ebull_test_conn.commit()


def test_the_key_is_released_when_nothing_was_terminalised(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ordinary path returns early. A session-scoped key leaked there would
    outlive the request on a pooled connection and wedge that recommendation for
    every later pass."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_attempt(ebull_test_conn, recommendation_id=rec, phase="broker_verb_entered")

    terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW)

    held = ebull_test_conn.execute(
        """
        SELECT count(*) FROM pg_locks
        WHERE locktype='advisory' AND classid=%s AND objid=%s
          AND pid = pg_backend_pid() AND granted
        """,
        (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, rec),
    ).fetchone()
    ebull_test_conn.commit()
    assert held == (0,)


def test_a_release_writes_its_own_refusal_audit(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The attempt happened and belongs in the audit trail — the row is resolved,
    never deleted."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_attempt(ebull_test_conn, recommendation_id=rec)

    terminalise_unsubmitted_recommendation_attempt(ebull_test_conn, recommendation_id=rec, now=_NOW)

    row = ebull_test_conn.execute(
        """
        SELECT evidence_json->>'refusal', pass_fail
        FROM decision_audit
        WHERE recommendation_id=%s AND evidence_json->>'refusal'='never_submitted'
        """,
        (rec,),
    ).fetchone()
    ebull_test_conn.commit()
    assert row == ("never_submitted", "FAIL")

    payload = ebull_test_conn.execute(
        "SELECT raw_payload_json->>'refusal' FROM orders WHERE order_id=%s",
        (order_id,),
    ).fetchone()
    assert payload == ("never_submitted",)


def test_the_marker_check_constraint_refuses_an_invented_phase(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The discriminator is a closed vocabulary. A third value would read as
    neither provably-unsubmitted nor possibly-submitted, and the UPDATE's
    predicate would silently treat it as the safe one — safe here, but by
    accident. The constraint makes it impossible rather than lucky."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)

    with pytest.raises(psycopg.errors.CheckViolation):
        _seed_attempt(ebull_test_conn, recommendation_id=rec, phase="submitting")
    ebull_test_conn.rollback()
