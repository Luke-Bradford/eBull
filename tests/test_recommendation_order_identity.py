"""#2942 — the two guarantees only Postgres can prove.

The claim and the immutability of the request identity are enforced by a
partial unique index and a trigger, not by application code, because the
failure mode is a restarted or concurrent second submission that no
in-process check can exclude. Mocked-cursor tests cannot exercise either, so
these two live here against a real database.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg
import pytest

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 9, 13, 12, 0, 0, tzinfo=UTC)
_INSTRUMENT_ID = 2942001


def _seed(conn: psycopg.Connection[Any]) -> int:
    """Create one approved recommendation and return its id."""
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, %s, %s, true) ON CONFLICT (instrument_id) DO NOTHING",
        (_INSTRUMENT_ID, f"C{_INSTRUMENT_ID}", "Claim test"),
    )
    row = conn.execute(
        """
        INSERT INTO trade_recommendations
            (instrument_id, action, status, model_version, rationale, created_at)
        VALUES (%s, 'BUY', 'approved', 'v1-test', '#2942 claim test', %s)
        RETURNING recommendation_id
        """,
        (_INSTRUMENT_ID, _NOW),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert_intent(
    conn: psycopg.Connection[Any],
    *,
    recommendation_id: int,
    status: str = "submitted",
) -> int:
    row = conn.execute(
        """
        INSERT INTO orders
            (instrument_id, recommendation_id, action, order_type, status,
             created_at, recommendation_request_id)
        VALUES (%s, %s, 'BUY', 'market', %s, %s, %s)
        RETURNING order_id
        """,
        (_INSTRUMENT_ID, recommendation_id, status, _NOW, uuid4()),
    ).fetchone()
    assert row is not None
    return int(row[0])


@pytest.mark.parametrize("held_status", ["submitted", "pending", "uncertain"])
def test_one_unresolved_attempt_per_recommendation(
    ebull_test_conn: psycopg.Connection[Any],
    held_status: str,
) -> None:
    """The claim index refuses the second economic order (#2942).

    All three unresolved statuses hold the claim: `submitted` (committed but
    the broker never answered), `pending` (asynchronously accepted) and
    `uncertain` (transport/5xx — it may already exist at the broker).
    """
    conn = ebull_test_conn
    recommendation_id = _seed(conn)
    _insert_intent(conn, recommendation_id=recommendation_id, status=held_status)

    with pytest.raises(psycopg.errors.UniqueViolation) as excinfo:
        _insert_intent(conn, recommendation_id=recommendation_id)

    assert excinfo.value.diag.constraint_name == "idx_orders_recommendation_open_attempt"
    conn.rollback()


def test_a_resolved_attempt_releases_the_claim(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A terminal outcome must not park the recommendation forever.

    `failed` here is a real broker rejection — the broker answered and said no
    — which is exactly the case that may legitimately be retried. Transport
    failures never reach this status; they land on `uncertain` above.
    """
    conn = ebull_test_conn
    recommendation_id = _seed(conn)
    _insert_intent(conn, recommendation_id=recommendation_id, status="failed")

    second = _insert_intent(conn, recommendation_id=recommendation_id)

    assert second > 0
    conn.rollback()


def test_the_claim_does_not_bind_unrelated_recommendations(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A partial unique index on a nullable column must not collapse NULLs.

    Two manual orders (recommendation_id IS NULL) would collide if the index
    predicate were wrong, which would break the operator's manual order path.
    """
    conn = ebull_test_conn
    first = _seed(conn)
    second = _seed(conn)
    _insert_intent(conn, recommendation_id=first)
    _insert_intent(conn, recommendation_id=second)

    for _ in range(2):
        conn.execute(
            """
            INSERT INTO orders (instrument_id, action, order_type, status, created_at)
            VALUES (%s, 'BUY', 'market', 'submitted', %s)
            """,
            (_INSTRUMENT_ID, _NOW),
        )

    conn.rollback()


def test_request_id_is_immutable_once_assigned(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Rotating the UUID would orphan the submission it identifies (#2942)."""
    conn = ebull_test_conn
    recommendation_id = _seed(conn)
    order_id = _insert_intent(conn, recommendation_id=recommendation_id)

    with pytest.raises(psycopg.errors.RaiseException, match="immutable once assigned"):
        conn.execute(
            "UPDATE orders SET recommendation_request_id = %s WHERE order_id = %s",
            (uuid4(), order_id),
        )
    conn.rollback()


def test_request_id_belongs_only_to_recommendation_orders(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The column must not drift onto manual or strategy rows."""
    conn = ebull_test_conn

    with pytest.raises(psycopg.errors.CheckViolation) as excinfo:
        conn.execute(
            """
            INSERT INTO orders
                (instrument_id, action, order_type, status, created_at,
                 recommendation_request_id)
            VALUES (%s, 'BUY', 'market', 'submitted', %s, %s)
            """,
            (_INSTRUMENT_ID, _NOW, uuid4()),
        )

    assert excinfo.value.diag.constraint_name == "orders_recommendation_request_origin_check"
    conn.rollback()
