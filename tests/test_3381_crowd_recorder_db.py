"""#3381 slice 1: the crowd recorder's append-only write path against a real database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.market_data import BroadMarketSnapshot, MarketSnapshotInstrument
from app.services.crowd_recorder import (
    SNAPSHOT_COMPLETE,
    SNAPSHOT_FAILED,
    record_crowd_snapshot,
)

T0 = datetime(2026, 9, 25, 21, 52, tzinfo=UTC)
PARAMS = {"path": "/api/v1/market-data/search", "pageSize": 10_000}


def _row(instrument_id: int, *, buy: str | None, observed_at: datetime | None = T0) -> MarketSnapshotInstrument:
    raw: dict[str, object] = {"instrumentId": instrument_id, "buyHoldingPct": None if buy is None else float(buy)}
    return MarketSnapshotInstrument(
        instrument_id=instrument_id,
        current_rate=None,
        daily_price_change_pct=None,
        weekly_price_change_pct=None,
        monthly_price_change_pct=None,
        is_currently_tradable=None,
        is_exchange_open=None,
        is_active_in_platform=None,
        is_buy_enabled=None,
        industry_id=None,
        sector_id=None,
        popularity_uniques_7d=Decimal("10"),
        traders_7d_change=Decimal("-1.5"),
        buy_holding_pct=None if buy is None else Decimal(buy),
        sell_holding_pct=None if buy is None else Decimal(100) - Decimal(buy),
        holding_pct=Decimal("0.01"),
        traders_30d_change=Decimal("3"),
        observed_at=observed_at,
        raw=raw,
    )


def _snapshot(*rows: MarketSnapshotInstrument) -> BroadMarketSnapshot:
    return BroadMarketSnapshot(T0, T0 + timedelta(seconds=6), len(rows) + 1, 1, rows, pages=1)


def _clock() -> datetime:
    return T0


def test_complete_snapshot_appends_header_and_rows(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    result = record_crowd_snapshot(
        conn,
        lambda: _snapshot(_row(1001, buy="91.5"), _row(999_999_999, buy=None)),
        request_params=PARAMS,
        clock=_clock,
    )
    assert (result.recorded_items, result.buy_sell_covered) == (2, 1)

    header = conn.execute(
        "SELECT status, request_params, pages, reported_total_items, discarded_items, recorded_items, error "
        "FROM etoro_crowd_snapshots WHERE snapshot_id = %s",
        (result.snapshot_id,),
    ).fetchone()
    assert header == (SNAPSHOT_COMPLETE, PARAMS, 1, 3, 1, 2, None)

    rows = conn.execute(
        "SELECT instrument_id, observed_at, buy_holding_pct, sell_holding_pct, holding_pct, popularity_uniques_14d, "
        "traders_change_7d, traders_change_30d, raw FROM etoro_crowd_observations WHERE snapshot_id = %s "
        "ORDER BY instrument_id",
        (result.snapshot_id,),
    ).fetchall()
    assert rows[0] == (
        1001,
        T0,
        Decimal("91.5"),
        Decimal("8.5"),
        Decimal("0.01"),
        None,
        Decimal("-1.5"),
        Decimal("3"),
        {"instrumentId": 1001, "buyHoldingPct": 91.5},
    )
    # An id we may not hold is recorded, and absent values stay NULL (never 0).
    assert rows[1][0] == 999_999_999 and rows[1][2] is None and rows[1][8]["buyHoldingPct"] is None

    # A second run is a second snapshot, never an upsert.
    again = record_crowd_snapshot(conn, lambda: _snapshot(_row(1001, buy="90")), request_params=PARAMS, clock=_clock)
    assert again.snapshot_id != result.snapshot_id
    count = conn.execute("SELECT count(*) FROM etoro_crowd_observations WHERE instrument_id = 1001").fetchone()
    assert count == (2,)


def _boom() -> BroadMarketSnapshot:
    raise ValueError("eToro search pagination incomplete: received 1 of 3 rows")


@pytest.mark.parametrize(
    "fetch",
    [_boom, lambda: _snapshot(_row(1001, buy="50", observed_at=None))],
    ids=["fetch-raises", "row-without-page-time"],
)
def test_failed_collection_is_recorded_and_reraises_the_original(
    ebull_test_conn: psycopg.Connection[Any], fetch: Any
) -> None:
    conn = ebull_test_conn
    # The ORIGINAL exception propagates, so the job's failure classifier sees its type.
    with pytest.raises(ValueError):
        record_crowd_snapshot(conn, fetch, request_params=PARAMS, clock=_clock)
    header = conn.execute(
        "SELECT snapshot_id, status, request_params, pages, recorded_items, error FROM etoro_crowd_snapshots"
    ).fetchall()
    assert len(header) == 1
    snapshot_id, *fields, error = header[0]
    assert fields == [SNAPSHOT_FAILED, PARAMS, None, 0]
    assert error.startswith("ValueError: ")
    rows = conn.execute("SELECT count(*) FROM etoro_crowd_observations WHERE snapshot_id = %s", (snapshot_id,))
    assert rows.fetchone() == (0,)


def test_an_http_error_keeps_its_type_for_the_classifier(ebull_test_conn: psycopg.Connection[Any]) -> None:
    import httpx

    from app.services.sync_orchestrator.exception_classifier import classify_exception
    from app.services.sync_orchestrator.layer_types import FailureCategory

    request = httpx.Request("GET", "https://public-api.etoro.com/api/v1/market-data/search")
    response = httpx.Response(401, request=request)

    def unauthorised() -> BroadMarketSnapshot:
        raise httpx.HTTPStatusError("401 Unauthorized", request=request, response=response)

    with pytest.raises(httpx.HTTPStatusError) as raised:
        record_crowd_snapshot(ebull_test_conn, unauthorised, request_params=PARAMS, clock=_clock)
    assert classify_exception(raised.value) == FailureCategory.AUTH_EXPIRED


def test_tables_refuse_update(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    result = record_crowd_snapshot(conn, lambda: _snapshot(_row(1001, buy="91.5")), request_params=PARAMS, clock=_clock)
    for statement in (
        "UPDATE etoro_crowd_observations SET buy_holding_pct = 0 WHERE snapshot_id = %s",
        "UPDATE etoro_crowd_snapshots SET recorded_items = 0 WHERE snapshot_id = %s",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            with conn.transaction():
                conn.execute(statement, (result.snapshot_id,))
