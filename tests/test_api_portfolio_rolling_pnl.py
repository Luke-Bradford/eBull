"""Tests for GET /portfolio/rolling-pnl (#315 Phase 2)."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _make_conn(fetchall_per_cursor: list[list[dict[str, object]]]) -> MagicMock:
    """Conn that returns a fresh cursor per call, each pre-loaded with
    `fetchall_per_cursor[i]` as the rows for that cursor. The
    rolling-pnl endpoint opens one cursor per period and uses
    `fetchall()` (one row per open position)."""
    cursors = iter(fetchall_per_cursor)

    def _next(*_a: object, **_k: object) -> MagicMock:
        cur = MagicMock()
        cur.__enter__.return_value = cur
        cur.__exit__.return_value = None
        cur.fetchall.return_value = next(cursors)
        return cur

    conn = MagicMock()
    conn.cursor.side_effect = _next
    return conn


def _runtime_stub(currency: str = "GBP") -> MagicMock:
    """get_runtime_config returns an object with display_currency attr."""
    rt = MagicMock()
    rt.display_currency = currency
    return rt


_NEXT_IID = [1]


def _position_row(
    *,
    curr: Decimal,
    prior: Decimal | None,
    units: Decimal,
    currency: str = "GBP",
    instrument_id: int | None = None,
) -> dict[str, object]:
    if instrument_id is None:
        instrument_id = _NEXT_IID[0]
        _NEXT_IID[0] += 1
    return {
        "instrument_id": instrument_id,
        "native_currency": currency,
        "current_units": units,
        "curr_close": curr,
        "curr_date": None,  # not asserted in tests; endpoint uses LATERAL anchor
        "prior_close": prior,
    }


def test_rolling_pnl_returns_three_periods_in_order(client: TestClient) -> None:
    """Happy path: one GBP position × three periods. 1d = +1GBP, 1w = +5GBP, 1m = +12GBP."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                # 1d: curr=101, prior=100, units=100 → pnl=100, cost=10000
                [_position_row(curr=Decimal("101"), prior=Decimal("100"), units=Decimal("100"))],
                # 1w: curr=101, prior=96 → pnl=500, cost=9600
                [_position_row(curr=Decimal("101"), prior=Decimal("96"), units=Decimal("100"))],
                # 1m: curr=101, prior=89 → pnl=1200, cost=8900
                [_position_row(curr=Decimal("101"), prior=Decimal("89"), units=Decimal("100"))],
            ]
        )

    app.dependency_overrides[get_conn] = _conn
    try:
        with (
            patch(
                "app.api.portfolio.get_runtime_config",
                return_value=_runtime_stub("GBP"),
            ),
            patch(
                "app.api.portfolio.load_live_fx_rates_with_metadata",
                return_value={},
            ),
        ):
            resp = client.get("/portfolio/rolling-pnl")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["display_currency"] == "GBP"
    assert [p["period"] for p in body["periods"]] == ["1d", "1w", "1m"]
    assert body["periods"][0]["pnl"] == pytest.approx(100.0)
    assert body["periods"][0]["pnl_pct"] == pytest.approx(0.01)
    assert body["periods"][2]["pnl"] == pytest.approx(1200.0)


def test_rolling_pnl_positions_without_prior_close_skip(client: TestClient) -> None:
    """Positions with no prior close at the lookback contribute nothing
    to pnl / cost and don't count toward coverage. Empty cost → null pct."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                [_position_row(curr=Decimal("100"), prior=None, units=Decimal("10"))],
                [_position_row(curr=Decimal("100"), prior=None, units=Decimal("10"))],
                [_position_row(curr=Decimal("100"), prior=None, units=Decimal("10"))],
            ]
        )

    app.dependency_overrides[get_conn] = _conn
    try:
        with (
            patch(
                "app.api.portfolio.get_runtime_config",
                return_value=_runtime_stub(),
            ),
            patch(
                "app.api.portfolio.load_live_fx_rates_with_metadata",
                return_value={},
            ),
        ):
            resp = client.get("/portfolio/rolling-pnl")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200
    for period in resp.json()["periods"]:
        assert period["pnl"] == pytest.approx(0.0)
        assert period["pnl_pct"] is None
        assert period["coverage"] == 0


def test_rolling_pnl_fx_converts_usd_positions_to_display_gbp(
    client: TestClient,
) -> None:
    """Each position's native-currency delta is FX-converted to display
    currency before summing. USD position with $100 delta should land
    as ~80 GBP at a 0.80 USD→GBP rate (prevents the native-sum bug
    Codex flagged on round-1)."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                [_position_row(curr=Decimal("110"), prior=Decimal("100"), units=Decimal("10"), currency="USD")],
                [_position_row(curr=Decimal("110"), prior=Decimal("100"), units=Decimal("10"), currency="USD")],
                [_position_row(curr=Decimal("110"), prior=Decimal("100"), units=Decimal("10"), currency="USD")],
            ]
        )

    app.dependency_overrides[get_conn] = _conn
    try:
        with (
            patch(
                "app.api.portfolio.get_runtime_config",
                return_value=_runtime_stub("GBP"),
            ),
            patch(
                "app.api.portfolio.load_live_fx_rates_with_metadata",
                return_value={
                    ("USD", "GBP"): {
                        "rate": Decimal("0.80"),
                        "quoted_at": "2026-04-21T00:00:00Z",
                    }
                },
            ),
        ):
            resp = client.get("/portfolio/rolling-pnl")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    body = resp.json()
    # Delta native = (110-100)*10 = 100 USD. Converted to GBP @ 0.80 = 80.
    for period in body["periods"]:
        assert period["pnl"] == pytest.approx(80.0)


def test_rolling_pnl_coverage_counts_positions_with_prior_close(
    client: TestClient,
) -> None:
    """coverage = positions that contributed. Mix of with/without prior."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        # 1d period: 2 of 3 positions have prior closes → coverage 2.
        yield _make_conn(
            [
                [
                    _position_row(curr=Decimal("110"), prior=Decimal("100"), units=Decimal("1")),
                    _position_row(curr=Decimal("50"), prior=Decimal("45"), units=Decimal("1")),
                    _position_row(curr=Decimal("200"), prior=None, units=Decimal("1")),
                ],
                # 1w and 1m reuse same shape for brevity.
                [
                    _position_row(curr=Decimal("110"), prior=Decimal("100"), units=Decimal("1")),
                    _position_row(curr=Decimal("50"), prior=Decimal("40"), units=Decimal("1")),
                    _position_row(curr=Decimal("200"), prior=Decimal("180"), units=Decimal("1")),
                ],
                [
                    _position_row(curr=Decimal("110"), prior=Decimal("80"), units=Decimal("1")),
                    _position_row(curr=Decimal("50"), prior=Decimal("30"), units=Decimal("1")),
                    _position_row(curr=Decimal("200"), prior=Decimal("150"), units=Decimal("1")),
                ],
            ]
        )

    app.dependency_overrides[get_conn] = _conn
    try:
        with (
            patch(
                "app.api.portfolio.get_runtime_config",
                return_value=_runtime_stub(),
            ),
            patch(
                "app.api.portfolio.load_live_fx_rates_with_metadata",
                return_value={},
            ),
        ):
            resp = client.get("/portfolio/rolling-pnl")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    body = resp.json()
    assert [p["coverage"] for p in body["periods"]] == [2, 3, 3]
