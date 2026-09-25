"""#3381 slice 3: the perishables recorder's write path against a real database."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.providers.broker import BrokerWhatIfOrder
from app.providers.implementations.etoro_perishables import RawResponse
from app.providers.implementations.etoro_social import SocialResponse
from app.services.investor_cohort_recorder import record_investor_snapshot
from app.services.perishables_recorder import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_PARTIAL,
    PerishableSnapshotPartial,
    PerishableSnapshotRefused,
    record_perishables_snapshot,
)

T0 = datetime(2026, 9, 25, 19, 7, tzinfo=UTC)
PARAMS = {"environment": "demo"}
LONG_X1 = {"settlementType": "real", "direction": "long", "leverageValues": [1], "isPotential": False}
SHORT_X1 = {"settlementType": "cfd", "direction": "short", "leverageValues": [1, 2], "isPotential": False}


class _Investors:
    """One cohort member holding 1001 long and 1002 short."""

    def get_rankings_page(self, params: dict[str, str | int | bool], page: int) -> SocialResponse:
        items = [{"cid": 1, "username": "u1", "copiers": 10}]
        envelope = {"page": 1, "pageSize": 100, "totalItems": 1, "hasNext": False}
        return SocialResponse(200, {"results": items, "pagination": envelope}, T0)

    def get_live_portfolio(self, username: str) -> SocialResponse:
        positions = [
            {"positionId": 1, "instrumentId": 1001, "isBuy": True},
            {"positionId": 2, "instrumentId": 1002, "isBuy": False},
        ]
        return SocialResponse(200, {"positions": positions, "socialTrades": []}, T0)


class _Source:
    def __init__(self, eligibility_status: int = 200) -> None:
        self.eligibility_status = eligibility_status
        self.orders: list[BrokerWhatIfOrder] = []

    def post_eligibility(self, instrument_ids: list[int]) -> RawResponse:
        found = [
            {"instrumentId": i, "allowOpenPosition": True, "leverageConfigs": [LONG_X1, SHORT_X1]}
            for i in instrument_ids
            if i in (1001, 1002)
        ]
        missing = [i for i in instrument_ids if i not in (1001, 1002)]
        body = {"currency": "USD", "eligibilities": found, "notFoundInstrumentIds": missing, "notFoundSymbols": []}
        return RawResponse(self.eligibility_status, body, T0)

    def post_what_if(self, order: BrokerWhatIfOrder) -> RawResponse:
        self.orders.append(order)
        costs = [{"costType": "marketSpread", "currency": "USD", "value": 0.09}]
        return RawResponse(
            200, {"instrumentId": order.instrument_id, "costs": costs, "lastUpdated": "2026-09-25T19:08:00Z"}, T0
        )

    def get_rates(self, instrument_ids: list[int]) -> RawResponse:
        rates = [{"instrumentID": i, "bid": 10, "ask": 10.1, "date": "2026-09-25T19:06:59Z"} for i in instrument_ids]
        return RawResponse(200, {"rates": rates}, T0)


def _seed(conn: psycopg.Connection[Any]) -> None:
    for instrument_id, tradable in ((1001, True), (1002, True), (1003, False)):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', %s)",
            (instrument_id, f"S{instrument_id}", f"Co {instrument_id}", tradable),
        )
    conn.commit()
    record_investor_snapshot(conn, _Investors(), request_params={}, clock=lambda: T0)


def _header(conn: psycopg.Connection[Any]) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT status, universe_size, whatif_panel_added, eligibility_expected, eligibility_ok, eligibility_errored, "
        "whatif_expected, whatif_ok, rates_expected, rates_ok, rates_errored FROM etoro_perishable_snapshots "
        "ORDER BY snapshot_id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    return tuple(row)


def test_complete_snapshot_and_the_universe_is_sticky(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn)
    source = _Source()
    result = record_perishables_snapshot(conn, source, request_params=PARAMS, clock=lambda: T0)

    assert _header(conn) == (STATUS_COMPLETE, 2, 2, 1, 1, 0, 4, 4, 1, 1, 0)
    assert result.row_count == 2 + 2 + 4
    assert conn.execute(
        "SELECT instrument_id, long_holders, short_holders FROM etoro_whatif_panel ORDER BY 1"
    ).fetchall() == [(1001, 1, None), (1002, None, 1)]
    assert conn.execute(
        "SELECT instrument_id, answer, short_x1, long_x1_settlement, max_short_leverage "
        "FROM etoro_eligibility_observations ORDER BY 1"
    ).fetchall() == [(1001, "found", "available", "real", 2), (1002, "found", "available", "real", 2)]
    arms = conn.execute(
        "SELECT w.instrument_id, w.arm, w.outcome, w.transaction, w.settlement_type, r.phase, e.phase "
        "FROM etoro_whatif_observations w JOIN etoro_perishable_requests r ON r.request_id = w.request_id "
        "JOIN etoro_perishable_requests e ON e.request_id = w.eligibility_request_id ORDER BY 1, 2"
    ).fetchall()
    assert arms == [
        (1001, "long", "ok", "buy", "real", "whatif", "eligibility"),
        (1001, "short", "ok", "sellShort", "cfd", "whatif", "eligibility"),
        (1002, "long", "ok", "buy", "real", "whatif", "eligibility"),
        (1002, "short", "ok", "sellShort", "cfd", "whatif", "eligibility"),
    ]
    # The raw body is kept whole on the request row.
    raw = conn.execute(
        "SELECT raw -> 'costs' -> 0 ->> 'value' FROM etoro_perishable_requests WHERE phase = 'whatif' LIMIT 1"
    )
    assert raw.fetchone() == ("0.09",)

    # 1002 leaves the tradable universe: it is still asked about (sticky), and eToro's answer is recorded.
    conn.execute("UPDATE instruments SET is_tradable = FALSE WHERE instrument_id = 1002")
    conn.commit()
    record_perishables_snapshot(conn, _Source(), request_params=PARAMS, clock=lambda: T0)
    assert _header(conn)[1] == 2
    assert conn.execute("SELECT count(*) FROM etoro_perishable_universe").fetchone() == (2,)

    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE etoro_perishable_snapshots SET status = 'failed'")
    conn.rollback()


def test_an_errored_request_commits_partial_then_raises(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn)
    with pytest.raises(PerishableSnapshotPartial):
        record_perishables_snapshot(conn, _PartialSource(), request_params=PARAMS, clock=lambda: T0)
    assert _header(conn)[0] == STATUS_PARTIAL
    assert conn.execute(
        "SELECT phase, outcome, http_status FROM etoro_perishable_requests WHERE outcome = 'error'"
    ).fetchall() == [("whatif", "error", 400)]
    assert conn.execute(
        "SELECT instrument_id, arm, outcome FROM etoro_whatif_observations WHERE outcome <> 'ok'"
    ).fetchall() == [(1002, "short", "error")]


class _PartialSource(_Source):
    def post_what_if(self, order: BrokerWhatIfOrder) -> RawResponse:
        if order.instrument_id == 1002 and order.transaction == "sellShort":
            return RawResponse(400, {"errorCode": "BelowMinimum"}, T0)
        return super().post_what_if(order)


def test_a_credential_refusal_fails_the_run_and_keeps_what_it_collected(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    with pytest.raises(PerishableSnapshotRefused, match="401"):
        record_perishables_snapshot(conn, _Source(eligibility_status=401), request_params=PARAMS, clock=lambda: T0)
    assert _header(conn) == (STATUS_FAILED, 2, 2, 1, 0, 1, None, None, None, None, None)
    assert conn.execute("SELECT phase, outcome, http_status FROM etoro_perishable_requests").fetchall() == [
        ("eligibility", "error", 401)
    ]
    # The sticky ledgers commit with the failed header.
    assert conn.execute("SELECT count(*) FROM etoro_perishable_universe").fetchone() == (2,)
    assert conn.execute("SELECT count(*) FROM etoro_whatif_panel").fetchone() == (2,)
