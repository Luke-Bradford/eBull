"""Tests for GET /portfolio/value-history (#204)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


_DEFAULT_START = {"start_date": date(2020, 1, 1)}


def _make_conn(
    fetchall_per_cursor: list[list[dict[str, object]]],
    *,
    start_date_row: dict[str, object] | None = None,
) -> MagicMock:
    """Conn with one cursor per `conn.cursor()` call.

    The value-history endpoint opens three cursors in sequence:
        1. start-date resolution (`cur.fetchone()` returns a dict
           with `start_date`)
        2. positions query (`cur.fetchall()` returns position rows)
        3. cash query (`cur.fetchall()` returns cash rows)

    `fetchall_per_cursor` holds rows for cursors 2 and 3; the first
    cursor's fetchone uses `start_date_row` or a sane default that
    predates every fixture in this file."""
    cursor_1 = {"fetchone": start_date_row or _DEFAULT_START}
    fetchalls = iter(fetchall_per_cursor)
    cursor_idx = [0]

    def _next(*_a: object, **_k: object) -> MagicMock:
        cur = MagicMock()
        cur.__enter__.return_value = cur
        cur.__exit__.return_value = None
        if cursor_idx[0] == 0:
            cur.fetchone.return_value = cursor_1["fetchone"]
        else:
            cur.fetchall.return_value = next(fetchalls)
        cursor_idx[0] += 1
        return cur

    conn = MagicMock()
    conn.cursor.side_effect = _next
    return conn


def _runtime_stub(currency: str = "GBP") -> MagicMock:
    rt = MagicMock()
    rt.display_currency = currency
    return rt


def test_value_history_sums_positions_and_cash_per_day(client: TestClient) -> None:
    """Two dates × (one GBP position + one cash event) — values sum
    per date in display currency."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                # positions: units=10 @ close=100 on d1, 10 @ 110 on d2
                [
                    {
                        "point_date": date(2026, 4, 19),
                        "instrument_id": 1,
                        "native_currency": "GBP",
                        "units_at_date": Decimal("10"),
                        "close_at_date": Decimal("100"),
                    },
                    {
                        "point_date": date(2026, 4, 20),
                        "instrument_id": 1,
                        "native_currency": "GBP",
                        "units_at_date": Decimal("10"),
                        "close_at_date": Decimal("110"),
                    },
                ],
                # cash: £500 cumulative on both dates
                [
                    {"point_date": date(2026, 4, 19), "currency": "GBP", "balance": Decimal("500")},
                    {"point_date": date(2026, 4, 20), "currency": "GBP", "balance": Decimal("500")},
                ],
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
            resp = client.get("/portfolio/value-history?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["display_currency"] == "GBP"
    assert body["range"] == "1m"
    assert body["fx_mode"] == "live"
    # Day 1: 10×100 + 500 = 1500.  Day 2: 10×110 + 500 = 1600.
    assert body["points"] == [
        {"date": "2026-04-19", "value": 1500.0},
        {"date": "2026-04-20", "value": 1600.0},
    ]


def test_value_history_fx_converts_native_positions(client: TestClient) -> None:
    """USD position → display currency GBP at the live rate. Covers the
    FX path for both positions and cash."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                # 100 USD worth of position on a single day
                [
                    {
                        "point_date": date(2026, 4, 20),
                        "instrument_id": 1,
                        "native_currency": "USD",
                        "units_at_date": Decimal("1"),
                        "close_at_date": Decimal("100"),
                    },
                ],
                # $200 cash on the same day
                [
                    {"point_date": date(2026, 4, 20), "currency": "USD", "balance": Decimal("200")},
                ],
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
            resp = client.get("/portfolio/value-history?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    body = resp.json()
    # 100 USD × 0.80 = 80 GBP; 200 USD × 0.80 = 160 GBP; sum = 240.
    assert body["points"] == [{"date": "2026-04-20", "value": 240.0}]


def test_value_history_skips_position_days_without_prior_close(client: TestClient) -> None:
    """If `close_at_date` is NULL (no price on or before), that position
    contributes nothing rather than being treated as worth zero."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                # Day 1 has no close_at_date; day 2 has one
                [
                    {
                        "point_date": date(2026, 4, 19),
                        "instrument_id": 1,
                        "native_currency": "GBP",
                        "units_at_date": Decimal("10"),
                        "close_at_date": None,
                    },
                    {
                        "point_date": date(2026, 4, 20),
                        "instrument_id": 1,
                        "native_currency": "GBP",
                        "units_at_date": Decimal("10"),
                        "close_at_date": Decimal("100"),
                    },
                ],
                # No cash rows
                [],
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
            resp = client.get("/portfolio/value-history?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    body = resp.json()
    # Day 1 skipped entirely because no close — not an artifact zero,
    # not present in the series.
    assert body["points"] == [{"date": "2026-04-20", "value": 1000.0}]


def test_value_history_rejects_unknown_range(client: TestClient) -> None:
    """Unknown range → FastAPI 422 from the `Literal` param validator.
    Pins that we don't silently default to '1y' on unexpected input.
    Overrides get_conn with a noop since dep resolution runs alongside
    param validation in FastAPI."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_conn] = _conn
    try:
        resp = client.get("/portfolio/value-history?range=2y")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 422


def test_value_history_fx_skipped_counts_distinct_pairs_not_rows(
    client: TestClient,
) -> None:
    """A single missing pair dropping many rows must surface as
    fx_skipped=1, not len(rows) — keeps the operator-facing count
    interpretable when a gap spans an entire series."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                # 3 days × same USD instrument → 3 rows, all dropping on
                # the same USD→GBP pair.
                [
                    {
                        "point_date": date(2026, 4, 18),
                        "instrument_id": 1,
                        "native_currency": "USD",
                        "units_at_date": Decimal("1"),
                        "close_at_date": Decimal("100"),
                    },
                    {
                        "point_date": date(2026, 4, 19),
                        "instrument_id": 1,
                        "native_currency": "USD",
                        "units_at_date": Decimal("1"),
                        "close_at_date": Decimal("100"),
                    },
                    {
                        "point_date": date(2026, 4, 20),
                        "instrument_id": 1,
                        "native_currency": "USD",
                        "units_at_date": Decimal("1"),
                        "close_at_date": Decimal("100"),
                    },
                ],
                # No cash rows
                [],
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
            resp = client.get("/portfolio/value-history?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    body = resp.json()
    assert body["fx_skipped"] == 1  # one pair (USD→GBP), not three rows


def test_value_history_max_empty_ledger_returns_days_zero(
    client: TestClient,
) -> None:
    """range=max on an empty ledger → start_date defaults to today,
    generate_series produces a single row, nothing to price → empty
    points + days=0. Pins the contract so FE can trust days=0 ==
    'no history', not 'couldn't compute'."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        # Custom start_date_row: the COALESCE in the start-date query
        # produces CURRENT_DATE when both ledgers are empty.
        yield _make_conn(
            [[], []],
            start_date_row={"start_date": date(2026, 4, 21)},
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
            resp = client.get("/portfolio/value-history?range=max")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    body = resp.json()
    assert body["points"] == []
    assert body["days"] == 0


def test_value_history_max_reports_effective_days_from_point_span(
    client: TestClient,
) -> None:
    """range=max sends NULL days to SQL (letting the query start from
    the earliest ledger row); the endpoint should report the actual
    span from first to last point as `days`, not a hard-coded ceiling.
    Pins that `max` is no longer a silent duplicate of `5y`."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                # Three dates roughly ~2 years apart — exercises the
                # span-computation branch.
                [
                    {
                        "point_date": date(2024, 4, 20),
                        "instrument_id": 1,
                        "native_currency": "GBP",
                        "units_at_date": Decimal("10"),
                        "close_at_date": Decimal("100"),
                    },
                    {
                        "point_date": date(2026, 4, 20),
                        "instrument_id": 1,
                        "native_currency": "GBP",
                        "units_at_date": Decimal("10"),
                        "close_at_date": Decimal("110"),
                    },
                ],
                [],
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
            resp = client.get("/portfolio/value-history?range=max")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    body = resp.json()
    assert body["range"] == "max"
    # Exact span from first to last point — kept computed, not hard-
    # coded, so a future schema/semantics change surfaces here.
    assert body["days"] == (date(2026, 4, 20) - date(2024, 4, 20)).days
    assert [p["date"] for p in body["points"]] == ["2024-04-20", "2026-04-20"]


def test_value_history_fx_missing_skips_cash_event(client: TestClient) -> None:
    """Cash in a currency with no live FX rate is logged-and-skipped,
    not a 500. Keeps the endpoint resilient to partial FX coverage."""
    from app.db import get_conn

    def _conn() -> Iterator[MagicMock]:
        yield _make_conn(
            [
                # No positions
                [],
                # Cash in EUR; no EUR→GBP rate loaded
                [
                    {"point_date": date(2026, 4, 20), "currency": "EUR", "balance": Decimal("100")},
                ],
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
            resp = client.get("/portfolio/value-history?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    # Request succeeds; EUR cash skipped; no points emitted. fx_skipped
    # counts distinct pairs (one: EUR→GBP), not the number of rows
    # dropped — a 365-day gap on one pair must read as "1 pair missing"
    # not "365 rows missing" for the FE copy to stay meaningful.
    assert resp.status_code == 200
    body = resp.json()
    assert body["points"] == []
    assert body["fx_skipped"] == 1
