"""The trade ledger's provenance (#3616 slice 3c, sql/485; spec Design 1, 2 and 8).

The scope check and fetch row of ``record_trade_events``, the observations ``ingest_trade_events`` retains when a
payload meets a stored row, and the migration's guards. Auto-marked ``db``.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerClosedTrade, BrokerPosition
from app.services.trade_events import (
    HISTORY_EPOCH,
    TradeHistoryFetch,
    compute_history_min_date,
    record_trade_events,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401  (fixture)

_CID = 4242
_FETCHED = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
_OPEN_AT = datetime(2026, 9, 16, 14, 30, tzinfo=UTC)
_CLOSE_AT = datetime(2026, 10, 1, 15, 0, tzinfo=UTC)
_MIGRATION = Path(__file__).parent.parent / "sql" / "485_tax_broker_ledger_provenance.sql"


def _position(position_id: int = 501, **payload: Any) -> BrokerPosition:
    raw: dict[str, Any] = {
        "CID": _CID,
        "positionID": position_id,
        "orderID": 77,
        "instrumentID": 4077,
        "isBuy": True,
        "isPartiallyAltered": False,
        "initialUnits": 10,
        "initialAmountInDollars": 1000,
        "totalFees": 0,
        "openRate": 100,
        "openDateTime": "2026-09-16T14:30:00Z",
        "unrealizedPnL": 1.5,
    }
    raw.update(payload)
    return BrokerPosition(
        instrument_id=4077,
        units=Decimal("10"),
        open_price=Decimal("100"),
        current_price=Decimal("101"),
        raw_payload={k: v for k, v in raw.items() if v is not None},
        position_id=position_id,
        initial_units=Decimal("10"),
        initial_amount_in_dollars=Decimal("1000"),
        open_date_time=_OPEN_AT,
    )


def _close(position_id: int = 601, **payload: Any) -> BrokerClosedTrade:
    raw: dict[str, Any] = {
        "positionId": position_id,
        "orderId": 88,
        "instrumentId": 4077,
        "isBuy": True,
        "units": 5,
        "investment": 500,
        "initialInvestment": 500,
        "netProfit": 10,
        "fees": 0,
        "openRate": 100,
        "closeRate": 102,
        "openTimestamp": "2026-09-16T14:30:00Z",
        "closeTimestamp": "2026-10-01T15:00:00Z",
    }
    raw.update(payload)
    return BrokerClosedTrade(
        position_id=position_id,
        instrument_id=4077,
        is_buy=True,
        units=Decimal("5"),
        open_rate=Decimal("100"),
        open_timestamp=_OPEN_AT,
        close_rate=Decimal("102"),
        close_timestamp=_CLOSE_AT,
        net_profit=Decimal("10"),
        fees=Decimal("0"),
        investment=Decimal("500"),
        initial_investment=Decimal("500"),
        leverage=1,
        order_id=88,
        social_trade_id=0,
        parent_position_id=0,
        raw_payload={k: v for k, v in raw.items() if v is not None},
    )


def _history(rows: list[BrokerClosedTrade] | None) -> TradeHistoryFetch:
    return TradeHistoryFetch(min_date=HISTORY_EPOCH, started_at=_FETCHED, finished_at=_FETCHED, rows=rows)


def _record(
    conn: psycopg.Connection[Any],
    positions: list[BrokerPosition],
    rows: list[BrokerClosedTrade] | None = None,
    environment: Any = "demo",
) -> str:
    counters = record_trade_events(
        conn, positions, _history(rows), environment=environment, portfolio_fetched_at=_FETCHED
    )
    return counters.fetch_outcome


def _all(conn: psycopg.Connection[Any], sql: str) -> list[tuple[Any, ...]]:
    return conn.execute(sql).fetchall()  # type: ignore[arg-type]


def _observations(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    return _all(conn, "SELECT kind, source, event_kind, position_id FROM trade_event_observations ORDER BY 1, 2, 3, 4")


@pytest.fixture
def conn(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:  # noqa: F811
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (4077, 'ILMN', 'ILMN Inc', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """
    )
    return ebull_test_conn


def test_first_batch_with_an_account_establishes_the_scope(conn: psycopg.Connection[Any]) -> None:
    assert _record(conn, [_position()], [_close()]) == "ok"
    assert _all(conn, "SELECT environment, account_id FROM trade_ledger_scope") == [("demo", _CID)]
    assert _all(conn, "SELECT DISTINCT environment FROM trade_events") == [("demo",)]
    assert _all(conn, "SELECT count(*) FROM trade_events") == [(3,)]
    assert _all(
        conn,
        "SELECT environment, account_id, portfolio_fetched_at, portfolio_real_positions, min_date, outcome, row_count "
        "FROM trade_history_fetches",
    ) == [("demo", _CID, _FETCHED, 1, HISTORY_EPOCH, "ok", 1)]
    assert _observations(conn) == []


def test_a_scoped_payload_that_forms_no_open_is_retained(conn: psycopg.Connection[Any]) -> None:
    undated = replace(_position(502, totalFees=3), open_date_time=None)
    assert _record(conn, [_position(501), undated], []) == "ok"
    assert _all(conn, "SELECT position_id FROM trade_events") == [(501,)]
    assert _all(conn, "SELECT kind, position_id, observed_normalised->>'totalfees' FROM trade_event_observations") == [
        ("unscoped", 502, "3")
    ]


def test_failed_fetch_writes_a_failed_row_and_still_ingests_opens(conn: psycopg.Connection[Any]) -> None:
    assert _record(conn, [_position()], None) == "failed"
    assert _all(conn, "SELECT min_date, portfolio_fetched_at, outcome, row_count FROM trade_history_fetches") == [
        (HISTORY_EPOCH, _FETCHED, "failed", None)
    ]
    assert _all(conn, "SELECT count(*) FROM trade_events") == [(1,)]


def test_a_payload_without_cid_refuses_the_batch_and_keeps_it_unscoped(conn: psycopg.Connection[Any]) -> None:
    batch = [_position(501), _position(502, CID=None)]
    assert _record(conn, batch, [_close()]) == "scope_refused"
    assert _all(conn, "SELECT count(*) FROM trade_events") == [(0,)]
    assert _all(conn, "SELECT count(*) FROM trade_ledger_scope") == [(0,)]
    assert _all(conn, "SELECT account_id, outcome FROM trade_history_fetches") == [(None, "scope_refused")]
    expected = [
        ("unscoped", "etoro_history", "close", 601),
        ("unscoped", "etoro_history", "open", 601),
        ("unscoped", "etoro_sync", "open", 501),
        ("unscoped", "etoro_sync", "open", 502),
    ]
    assert _observations(conn) == expected
    # An identical repeat (only the live valuation moved) is not written again.
    batch = [_position(501, unrealizedPnL=-2.0), _position(502, CID=None)]
    assert _record(conn, batch, [_close()]) == "scope_refused"
    assert _observations(conn) == expected


@pytest.mark.parametrize(("environment", "cid"), [("real", _CID), ("demo", _CID + 1)])
def test_a_batch_outside_the_scope_writes_no_ledger_row(
    conn: psycopg.Connection[Any], environment: str, cid: int
) -> None:
    assert _record(conn, [_position()], []) == "ok"
    before = _all(conn, "SELECT count(*) FROM trade_events")
    assert _record(conn, [_position(503, CID=cid)], [_close()], environment=environment) == "scope_refused"
    assert _all(conn, "SELECT count(*) FROM trade_events") == before
    assert _all(conn, "SELECT outcome FROM trade_history_fetches ORDER BY fetch_id DESC LIMIT 1") == [
        ("scope_refused",)
    ]


def test_a_flat_account_history_waits_for_a_batch_with_the_account(conn: psycopg.Connection[Any]) -> None:
    assert _record(conn, [], [_close()]) == "scope_unverified"
    assert _all(conn, "SELECT count(*) FROM trade_events") == [(0,)]
    assert compute_history_min_date(conn) == HISTORY_EPOCH  # the watermark did not move
    assert _all(conn, "SELECT portfolio_real_positions, outcome FROM trade_history_fetches") == [
        (0, "scope_unverified")
    ]
    assert _record(conn, [_position()], [_close()]) == "ok"
    assert _all(conn, "SELECT count(*) FROM trade_events WHERE event_kind = 'close'") == [(1,)]


def test_history_close_amendments(conn: psycopg.Connection[Any]) -> None:
    assert _record(conn, [_position()], [_close()]) == "ok"
    _record(conn, [_position()], [_close()])
    assert _observations(conn) == []
    _record(conn, [_position()], [_close(investment=499)])
    _record(conn, [_position()], [_close(investment=499)])  # the same correction again: nothing
    _record(conn, [_position()], [_close(fees=None)])  # a dropped field
    assert _all(
        conn,
        "SELECT kind, observed_normalised->>'investment', observed_normalised ? 'fees' FROM trade_event_observations "
        "WHERE event_kind = 'close' ORDER BY observation_id",
    ) == [("amendment", "499", True), ("amendment", "500", False)]
    # The stored row never changes (sql/194's contract).
    assert _all(conn, "SELECT investment_usd FROM trade_events WHERE event_kind = 'close'") == [(Decimal("500.0000"),)]


def test_sync_supplement_to_a_history_first_open(conn: psycopg.Connection[Any]) -> None:
    # History first: the position is closed (not held) when first seen, then the sync reports it.
    assert _record(conn, [_position(501)], [_close(601)]) == "ok"
    sync_601 = [_position(501), _position(601, orderID=88)]
    _record(conn, sync_601, [])
    _record(conn, sync_601, [])  # repeat: nothing
    revised = [_position(501), _position(601, orderID=88, totalFees=5)]
    _record(conn, revised, [])
    _record(conn, revised, [])  # same revision: nothing
    _record(conn, sync_601, [])  # reversion: a second amendment
    assert _all(
        conn,
        "SELECT kind, observed_normalised->>'totalfees' FROM trade_event_observations "
        "WHERE position_id = 601 AND source = 'etoro_sync' ORDER BY observation_id",
    ) == [("supplemental", "0"), ("amendment", "5"), ("amendment", "0")]


def test_sync_first_open_amendments_use_the_stored_row_as_baseline(conn: psycopg.Connection[Any]) -> None:
    _record(conn, [_position(501)])
    _record(conn, [_position(501, totalFees=5)])
    _record(conn, [_position(501, totalFees=5)])
    _record(conn, [_position(501)])
    _record(conn, [_position(501, isPartiallyAltered=True)])
    assert _all(
        conn,
        "SELECT kind, observed_normalised->>'totalfees', stored_normalised->>'totalfees' FROM trade_event_observations "
        "ORDER BY observation_id",
    ) == [("amendment", "5", "0"), ("amendment", "0", "0"), ("lifecycle", "0", "0")]


def test_history_open_beside_a_sync_open_is_supplemental_once(conn: psycopg.Connection[Any]) -> None:
    # One batch: the sync open of an unseen position and a contradictory history open of it.
    held = _position(501)
    close = replace(_close(501), raw_payload={**_close(501).raw_payload, "orderId": 99})
    assert _record(conn, [held], [close]) == "ok"
    assert _all(conn, "SELECT source FROM trade_events WHERE event_kind = 'open'") == [("etoro_sync",)]
    assert _observations(conn) == [("supplemental", "etoro_history", "open", 501)]
    _record(conn, [held], [close])
    assert _observations(conn) == [("supplemental", "etoro_history", "open", 501)]


def test_tax_lots_refuses_every_insert(conn: psycopg.Connection[Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="tax_lots is retired"):
        conn.execute(
            "INSERT INTO tax_lots (instrument_id, event_time, event_type, quantity, cost_or_proceeds, tax_year) "
            "VALUES (4077, now(), 'BUY', 1, 1, '2026/27')"
        )


def _rerun_migration(conn: psycopg.Connection[Any]) -> None:
    body = _MIGRATION.read_text().replace("BEGIN;", "").replace("COMMIT;", "")
    conn.execute(body)  # type: ignore[arg-type]


def _seed_ledger_row(conn: psycopg.Connection[Any], position_id: int, source: str, payload: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trade_events (position_id, etoro_instrument_id, event_kind, side, units, executed_at, source,
                                  raw_payload, environment, recorded_at)
        VALUES (%s, 4077, 'open', 'buy', 1, now(), %s, %s, 'demo', '2026-06-13T00:00Z')
        """,
        (position_id, source, Jsonb(payload)),
    )


@pytest.mark.parametrize(
    ("seed", "message"),
    [
        ("two_cids", "distinct CID"),
        ("null_cid", "invalid CID"),
        ("history_only", "no sync row"),
        ("tax_lots", "tax_lots holds"),
        ("real_credential", "non-demo broker credential"),
        ("empty_access_log", "access log is empty"),
        ("held_fee", "total_fees NULL or non-zero"),
    ],
)
def test_migration_guards_refuse(conn: psycopg.Connection[Any], seed: str, message: str) -> None:
    if seed == "two_cids":
        _seed_ledger_row(conn, 1, "etoro_sync", {"CID": 1})
        _seed_ledger_row(conn, 2, "etoro_sync", {"CID": 2})
    elif seed == "null_cid":
        _seed_ledger_row(conn, 1, "etoro_sync", {"CID": None})
    elif seed == "history_only":
        _seed_ledger_row(conn, 1, "etoro_history", {})
    elif seed in ("real_credential", "empty_access_log"):
        _seed_ledger_row(conn, 1, "etoro_sync", {"CID": 1})
    elif seed == "held_fee":
        # An archived position not yet in the ledger, with a non-zero fee archived before a later zero.
        _seed_archived_fee(conn, 9001, Decimal("0.5"), "2026-10-01T00:00Z")
        _seed_archived_fee(conn, 9001, Decimal("0"), "2026-10-02T00:00Z")
    else:
        conn.execute("ALTER TABLE tax_lots DISABLE TRIGGER tax_lots_refuse_insert")
        conn.execute(
            "INSERT INTO tax_lots (instrument_id, event_time, event_type, quantity, cost_or_proceeds, tax_year) "
            "VALUES (4077, now(), 'BUY', 1, 1, '2026/27')"
        )
        conn.execute("ALTER TABLE tax_lots ENABLE TRIGGER tax_lots_refuse_insert")
    if seed == "real_credential":
        _seed_access_log(conn, environment="real")
    elif seed == "empty_access_log":
        _seed_access_log(conn, logged=False)
    elif seed != "tax_lots":
        # The credential guard passes: one demo credential read before the ledger began.
        _seed_access_log(conn)
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        _rerun_migration(conn)
    conn.rollback()


def _seed_archived_fee(conn: psycopg.Connection[Any], position_id: int, fees: Decimal, detected_at: str) -> None:
    columns = conn.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'broker_positions_closed' AND is_nullable = 'NO' AND column_default IS NULL
        """
    ).fetchall()
    neutral = {"boolean": False, "text": "x", "timestamp with time zone": detected_at, "jsonb": Jsonb({})}
    values: dict[str, Any] = {"position_id": position_id, "total_fees": fees, "closed_detected_at": detected_at}
    # Every other required column gets a neutral value of its type.
    for name, data_type in columns:
        values.setdefault(name, neutral.get(data_type, 0))
    names = ", ".join(values)
    placeholders = ", ".join(f"%({k})s" for k in values)
    conn.execute(f"INSERT INTO broker_positions_closed ({names}) VALUES ({placeholders})", values)  # type: ignore[arg-type]


def _seed_access_log(conn: psycopg.Connection[Any], environment: str = "demo", logged: bool = True) -> None:
    """One credential, read once before the ledger's first ``recorded_at`` (2026-06-13)."""
    operator_id = uuid4()
    conn.execute(
        "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s, %s, 'x')",
        (operator_id, f"op_{operator_id.hex[:8]}"),
    )
    row = conn.execute(
        """
        INSERT INTO broker_credentials (operator_id, provider, label, environment, ciphertext, last_four, key_version)
        VALUES (%s, 'etoro', 'api_key', %s, '\\x00'::bytea, '0000', 1)
        RETURNING id
        """,
        (operator_id, environment),
    ).fetchone()
    assert row is not None
    if logged:
        conn.execute(
            """
            INSERT INTO broker_credential_access_log (credential_id, operator_id, accessed_at, caller, success)
            VALUES (%s, %s, '2026-06-03T00:00Z', 'test', TRUE)
            """,
            (row[0], operator_id),
        )


def test_a_write_without_an_environment_fails(conn: psycopg.Connection[Any]) -> None:
    # sql/487 (#3796): no default, so a writer that bypasses ingest cannot be labelled demo under any scope.
    with pytest.raises(psycopg.errors.NotNullViolation, match="environment"):
        conn.execute(
            "INSERT INTO trade_events (position_id, etoro_instrument_id, event_kind, side, units, executed_at, "
            "source, raw_payload) VALUES (1, 4077, 'open', 'buy', 1, now(), 'etoro_sync', '{}')"
        )
    conn.rollback()
