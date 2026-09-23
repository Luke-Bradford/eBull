"""#3007 part 2 — booking a confirmed late EXIT fill, against real rows.

Spec: ``docs/proposals/execution/2026-09-23-late-exit-fill-booking.md`` (acceptance,
"DB" and "End-to-end"). The broker is the REAL ``EtoroBrokerProvider`` with only its
HTTP client faked, answering with the verbatim ``statusID 3`` body of the attended
whole close ``383339190`` (IUSA.L, GBX) — so every test runs provider decode →
checks → transaction, not a hand-built detail.

The pure checks 1-6 are table-tested in ``tests/test_late_exit_booking.py``. Own
module because the ``db`` marker is module-scoped.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, LiteralString
from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import psycopg
import psycopg.rows
import pytest

from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.services import order_client
from app.services.order_client import (
    _persist_fill,
    _poll_one_pending_order,
    reconcile_pending_recommendation_orders,
)
from tests.fixtures.ebull_test_db import test_database_url

_FIXTURE = Path(__file__).parent / "fixtures" / "etoro" / "attended_2026-09-23-02_partial_close_gbx.jsonl"
_REF = "383339190"
_INSTRUMENT_ID = 3075  # IUSA.L; the body's own `instrumentID`
_LOT_ID = 3_602_947_846
_LOT_UNITS = Decimal("0.323024")
_AVG_COST = Decimal("5817.710000")
# (5817.29 − 5817.71) × 0.323024, quantised HALF_UP to 6 dp.
_DELTA = Decimal("-0.135670")
_OCCURRED = datetime(2026, 9, 23, 8, 1, 53, 830000, tzinfo=UTC)
_CREATED = datetime(2026, 9, 23, 8, 1, 53, tzinfo=UTC)
_NOW = datetime(2026, 9, 23, 9, 0, tzinfo=UTC)
_ENV = "demo"


def _whole_close_body() -> dict[str, Any]:
    for line in _FIXTURE.read_text().splitlines():
        record = json.loads(line)
        if record["step"] == f"close_order_RAW_{_REF}":
            return record["payload"]
    raise AssertionError("whole-close body missing from the attended fixture")


@contextmanager
def _broker(
    body: dict[str, Any] | None = None, *, on_get: Callable[[], None] | None = None
) -> Iterator[EtoroBrokerProvider]:
    """The real adapter, answering ``GET close-orders/{id}`` with ``body`` verbatim."""
    payload = _whole_close_body() if body is None else body

    def _get(url: str, **_: Any) -> httpx.Response:
        if on_get is not None:
            on_get()
        return httpx.Response(200, content=json.dumps(payload).encode(), request=httpx.Request("GET", url))

    with EtoroBrokerProvider(api_key="k", user_key="u", env=_ENV) as broker:
        broker._http_read = MagicMock()
        broker._http_read.get.side_effect = _get
        yield broker


def _seed(
    conn: psycopg.Connection[Any],
    *,
    prior_pnl: Decimal = Decimal("0"),
    pool_avg: Decimal = _AVG_COST,
    rec_status: str = "execution_pending",
    parked: str | None = None,
) -> tuple[int, int]:
    """One live-claimed EXIT, after the sync already applied the close (units zeroed)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'IUSA.L','iShares Core S&P 500',TRUE) ON CONFLICT DO NOTHING",
        (_INSTRUMENT_ID,),
    )
    conn.execute(
        """
        INSERT INTO positions (instrument_id, open_date, avg_cost, current_units, cost_basis,
                               realized_pnl, unrealized_pnl, source, updated_at)
        VALUES (%s, '2026-09-23', %s, 0, 0, %s, 1.25, 'ebull', %s)
        """,
        (_INSTRUMENT_ID, pool_avg, prior_pnl, _CREATED),
    )
    conn.execute(
        "INSERT INTO cash_ledger (event_time, event_type, amount, currency, note) "
        "VALUES (%s, 'broker_sync', 24.99, 'USD', 'the sync applied the close')",
        (_CREATED,),
    )
    rec = conn.execute(
        "INSERT INTO trade_recommendations (instrument_id, action, rationale, status) "
        "VALUES (%s,'EXIT','late booking test',%s) RETURNING recommendation_id",
        (_INSTRUMENT_ID, rec_status),
    ).fetchone()
    assert rec is not None
    order = _seed_exit_order(conn, recommendation_id=int(rec[0]), status="pending", parked=parked)
    conn.commit()
    return int(rec[0]), order


def _seed_exit_order(
    conn: psycopg.Connection[Any], *, recommendation_id: int, status: str, parked: str | None = None
) -> int:
    row = conn.execute(
        """
        INSERT INTO orders
            (instrument_id, recommendation_id, action, order_type, status, broker_order_ref,
             raw_payload_json, created_at, execution_origin, recommendation_request_id,
             recommendation_submission_phase, broker_environment, recommendation_exit_position_id,
             recommendation_exit_units, recommendation_submission_context, recommendation_exit_avg_cost,
             recommendation_poll_parked_reason)
        VALUES
            (%(iid)s, %(rid)s, 'EXIT', 'market', %(status)s, %(ref)s, '{}'::jsonb, %(created)s, 'manual',
             %(req)s, 'broker_verb_entered', %(env)s, %(lot)s, %(units)s, '{"order_params": null}'::jsonb,
             %(cost)s, %(parked)s)
        RETURNING order_id
        """,
        {
            "iid": _INSTRUMENT_ID,
            "rid": recommendation_id,
            "status": status,
            "ref": _REF,
            "created": _CREATED,
            "req": uuid4(),
            "env": _ENV,
            "lot": _LOT_ID,
            "units": _LOT_UNITS,
            "cost": _AVG_COST,
            "parked": parked,
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _reconcile(conn: psycopg.Connection[Any], broker: EtoroBrokerProvider) -> list[str]:
    return [r.verdict for r in reconcile_pending_recommendation_orders(conn, broker=broker, env=_ENV, now=_NOW)]


def _one(conn: psycopg.Connection[Any], query: LiteralString, *params: object) -> Any:
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return row[0]


def _ledger(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT current_units, cost_basis, unrealized_pnl, realized_pnl, avg_cost, "
            "(SELECT sum(amount) FROM cash_ledger) AS cash FROM positions WHERE instrument_id=%s",
            (_INSTRUMENT_ID,),
        )
        row = cur.fetchone()
    # The poller refuses a connection inside a transaction; a test read must not leave one.
    conn.commit()
    assert row is not None
    return row


def _order(conn: psycopg.Connection[Any], order_id: int) -> tuple[str, str | None]:
    row = conn.execute(
        "SELECT status, recommendation_poll_parked_reason FROM orders WHERE order_id=%s", (order_id,)
    ).fetchone()
    assert row is not None
    return str(row[0]), row[1]


def _fill_count(conn: psycopg.Connection[Any], order_id: int) -> int:
    return int(_one(conn, "SELECT count(*) FROM fills WHERE order_id=%s", order_id))


def _park_reason(conn: psycopg.Connection[Any], order_id: int) -> str | None:
    row = conn.execute(
        "SELECT evidence_json->>'reason' FROM decision_audit "
        "WHERE evidence_json->>'refusal' = 'pending_order_filled_not_booked' "
        "AND (evidence_json->>'order_id')::bigint = %s",
        (order_id,),
    ).fetchone()
    return None if row is None else row[0]


def _assert_untouched(conn: psycopg.Connection[Any], order_id: int, rec: int, before: dict[str, Any]) -> None:
    assert _fill_count(conn, order_id) == 0
    assert _ledger(conn) == before
    assert _one(conn, "SELECT status FROM trade_recommendations WHERE recommendation_id=%s", rec) == (
        "execution_pending"
    )


@pytest.mark.parametrize("prior_pnl", [Decimal("0"), Decimal("12.345678")])
def test_the_verbatim_whole_close_is_booked_end_to_end(
    ebull_test_conn: psycopg.Connection[tuple], prior_pnl: Decimal
) -> None:
    """Provider decode → checks → transaction → ``filled_booked``, writing ONLY what
    the sync cannot: the fill, the realized P&L and the terminal states."""
    rec, order_id = _seed(ebull_test_conn, prior_pnl=prior_pnl)
    before = _ledger(ebull_test_conn)

    with _broker() as broker:
        assert _reconcile(ebull_test_conn, broker) == ["filled_booked"]

    after = _ledger(ebull_test_conn)
    for column in ("current_units", "cost_basis", "unrealized_pnl", "avg_cost", "cash"):
        assert after[column] == before[column], column
    assert after["realized_pnl"] == prior_pnl + _DELTA
    with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT filled_at, price, units, gross_amount, fees FROM fills WHERE order_id=%s", (order_id,))
        fills = cur.fetchall()
    assert fills == [
        {
            "filled_at": _OCCURRED,
            "price": Decimal("5817.290000"),
            "units": Decimal("0.323024"),
            "gross_amount": Decimal("1879.124285"),
            "fees": Decimal("0.000000"),
        }
    ]
    assert _order(ebull_test_conn, order_id) == ("filled", None)
    assert _one(ebull_test_conn, "SELECT status FROM trade_recommendations WHERE recommendation_id=%s", rec) == (
        "executed"
    )
    audit = _one(
        ebull_test_conn,
        "SELECT evidence_json FROM decision_audit WHERE recommendation_id=%s AND pass_fail='PASS'",
        rec,
    )
    assert audit["late_exit_booking"]["realized_pnl_delta"] == str(_DELTA)
    assert audit["fees_source"] == "not_reported"
    assert audit["conversion_rate"] == 0.013303 and audit["proceeds"] == 24.99
    assert audit["exit_completion"]["position_fully_closed"] is None
    # The claim lifted: `filled` is outside idx_orders_recommendation_open_attempt.
    _seed_exit_order(ebull_test_conn, recommendation_id=rec, status="pending")
    ebull_test_conn.rollback()


def test_a_moved_pool_average_parks(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Check 8: a booked acquisition since the submission recomputed ``avg_cost``."""
    rec, order_id = _seed(ebull_test_conn, pool_avg=Decimal("5900"))
    before = _ledger(ebull_test_conn)

    with _broker() as broker:
        assert _reconcile(ebull_test_conn, broker) == ["filled_not_booked"]

    _assert_untouched(ebull_test_conn, order_id, rec, before)
    assert _order(ebull_test_conn, order_id) == ("pending", "filled_unbooked")
    assert _park_reason(ebull_test_conn, order_id) == "avg_cost_moved"


def test_a_recommendation_cas_miss_rolls_back_the_fill_and_the_pnl_then_parks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    rec, order_id = _seed(ebull_test_conn, rec_status="approved")
    before = _ledger(ebull_test_conn)

    with _broker() as broker:
        assert _reconcile(ebull_test_conn, broker) == ["filled_not_booked"]

    assert _fill_count(ebull_test_conn, order_id) == 0
    assert _ledger(ebull_test_conn) == before
    assert _order(ebull_test_conn, order_id) == ("pending", "filled_unbooked")
    assert _park_reason(ebull_test_conn, order_id) == "recommendation_cas_miss"


def test_a_lot_already_disposed_of_parks(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Check 9: a prior EXIT fill on the same broker position, by another order."""
    rec, order_id = _seed(ebull_test_conn)
    other = _one(
        ebull_test_conn,
        "INSERT INTO trade_recommendations (instrument_id, action, rationale, status) "
        "VALUES (%s,'EXIT','earlier',%s) RETURNING recommendation_id",
        _INSTRUMENT_ID,
        "executed",
    )
    earlier = _seed_exit_order(ebull_test_conn, recommendation_id=int(other), status="filled")
    _persist_fill(ebull_test_conn, earlier, Decimal("5817.29"), _LOT_UNITS, Decimal("0"), _CREATED)
    ebull_test_conn.commit()
    before = _ledger(ebull_test_conn)

    with _broker() as broker:
        assert _reconcile(ebull_test_conn, broker) == ["filled_not_booked"]

    _assert_untouched(ebull_test_conn, order_id, rec, before)
    assert _park_reason(ebull_test_conn, order_id) == "lot_already_disposed"


def test_a_refused_check_parks_with_its_reason(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Check 2 through the provider: the PARTIAL close's new slice id is not the lot."""
    rec, order_id = _seed(ebull_test_conn)
    body = _whole_close_body()
    body["positions"][0]["positionID"] = 3_602_947_861
    before = _ledger(ebull_test_conn)

    with _broker(body) as broker:
        assert _reconcile(ebull_test_conn, broker) == ["filled_not_booked"]

    _assert_untouched(ebull_test_conn, order_id, rec, before)
    assert _park_reason(ebull_test_conn, order_id) == "not_the_lot"


@pytest.mark.parametrize("parked", ["filled_unbooked", None])
def test_a_row_parked_or_filled_since_selection_is_not_looked_up(
    ebull_test_conn: psycopg.Connection[tuple], parked: str | None
) -> None:
    """The re-read under the lock sees the park / the terminal status the batch did not."""
    rec, order_id = _seed(ebull_test_conn, parked=parked)
    if parked is None:
        ebull_test_conn.execute("UPDATE orders SET status='filled' WHERE order_id=%s", (order_id,))
        ebull_test_conn.commit()
    broker = MagicMock()
    stale = {"order_id": order_id, "recommendation_id": rec, "instrument_id": _INSTRUMENT_ID}

    result = _poll_one_pending_order(ebull_test_conn, broker=broker, env=_ENV, row=stale, now=_NOW)

    assert result.verdict == "no_longer_pending"
    broker.get_close_order.assert_not_called()
    assert _fill_count(ebull_test_conn, order_id) == 0


def test_a_row_reassigned_since_selection_is_not_looked_up(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    rec, order_id = _seed(ebull_test_conn)
    broker = MagicMock()
    stale = {"order_id": order_id, "recommendation_id": rec + 1, "instrument_id": _INSTRUMENT_ID}

    result = _poll_one_pending_order(ebull_test_conn, broker=broker, env=_ENV, row=stale, now=_NOW)

    assert result.verdict == "no_longer_pending"
    broker.get_close_order.assert_not_called()


@pytest.mark.parametrize(
    ("concurrent_write", "pool_avg", "refused_on"),
    [
        # Resolved concurrently, on the refusal path (check 8 refuses, the park misses).
        ("UPDATE orders SET status='filled' WHERE order_id=%s", Decimal("5900"), "avg_cost_moved"),
        # Identity changed, on the booking path (the order CAS misses, then the park).
        ("UPDATE orders SET recommendation_exit_avg_cost=5800 WHERE order_id=%s", _AVG_COST, "order_cas_miss"),
    ],
)
def test_the_conditional_park_never_overwrites_a_concurrent_change(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
    concurrent_write: str,
    pool_avg: Decimal,
    refused_on: str,
) -> None:
    """The write lands during the broker round-trip, which runs outside any transaction."""
    rec, order_id = _seed(ebull_test_conn, pool_avg=pool_avg)
    before = _ledger(ebull_test_conn)

    def _race() -> None:
        with psycopg.connect(test_database_url(), autocommit=True) as other:
            other.execute(concurrent_write, (order_id,))  # type: ignore[arg-type]

    with _broker(on_get=_race) as broker:
        assert _reconcile(ebull_test_conn, broker) == ["no_longer_pending"]

    assert _fill_count(ebull_test_conn, order_id) == 0
    assert _ledger(ebull_test_conn) == before
    assert _order(ebull_test_conn, order_id)[1] is None
    assert _park_reason(ebull_test_conn, order_id) is None
    # Nothing is audited on a miss, so the log is the only witness of which step refused.
    assert f"not parked ({refused_on})" in caplog.text


def test_a_transient_database_error_never_parks(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``LockNotAvailable`` inside the transaction, AFTER every write ran: all of it
    rolls back, and the row stays pending and unparked for the next poll."""
    rec, order_id = _seed(ebull_test_conn)
    before = _ledger(ebull_test_conn)
    locked = order_client._book_late_exit_fill_locked

    def _then_lock_timeout(*args: Any, **kwargs: Any) -> None:
        locked(*args, **kwargs)
        raise psycopg.errors.LockNotAvailable("canceling statement due to lock timeout")

    monkeypatch.setattr(order_client, "_book_late_exit_fill_locked", _then_lock_timeout)
    with _broker() as broker:
        assert _reconcile(ebull_test_conn, broker) == ["poll_error"]

    _assert_untouched(ebull_test_conn, order_id, rec, before)
    assert _order(ebull_test_conn, order_id) == ("pending", None)
    assert _park_reason(ebull_test_conn, order_id) is None


def test_a_failed_park_leaves_the_row_pending_and_unparked(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    rec, order_id = _seed(ebull_test_conn, pool_avg=Decimal("5900"))

    def _park_fails(*_: Any, **__: Any) -> None:
        raise RuntimeError("park failed")

    monkeypatch.setattr(order_client, "_park_unbooked_fill", _park_fails)
    with _broker() as broker:
        assert _reconcile(ebull_test_conn, broker) == ["poll_error"]

    assert _order(ebull_test_conn, order_id) == ("pending", None)
    assert _fill_count(ebull_test_conn, order_id) == 0


def test_persist_fill_defaults_are_the_synchronous_behaviour(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The synchronous callers pass neither new parameter: ``price × units`` at ``now``."""
    rec, order_id = _seed(ebull_test_conn)
    fill_id = _persist_fill(ebull_test_conn, order_id, Decimal("10.5"), Decimal("2"), Decimal("0.1"), _NOW)

    row = ebull_test_conn.execute("SELECT filled_at, gross_amount FROM fills WHERE fill_id=%s", (fill_id,)).fetchone()
    assert row == (_NOW, Decimal("21.000000"))
    ebull_test_conn.rollback()
