"""Regression test for #244 — thesis accuracy must use thesis active at
position entry, not a hindsight thesis written later on the same day.

The previous query used
    created_at < (ra.hold_start::timestamptz + interval '1 day')
where ``hold_start`` is a DATE, so any thesis created later on the same
calendar day (including AFTER the entry fill) would match. The fix
anchors the cut-off to the entry fill's timestamp via
``return_attribution.entry_fill_id`` (or
``trade_recommendations.created_at`` as fallback).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.reporting import _thesis_accuracy
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Test"),
    )


def _seed_thesis(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    version: int,
    created_at: datetime,
    base_value: Decimal,
    bull_value: Decimal,
    bear_value: Decimal,
    stance: str = "bullish",
) -> None:
    conn.execute(
        """
        INSERT INTO theses (
            instrument_id, thesis_version, created_at, thesis_type,
            stance, base_value, bull_value, bear_value, memo_markdown,
            confidence_score
        ) VALUES (
            %s, %s, %s, 'standard', %s, %s, %s, %s, 'test', 0.6
        )
        """,
        (instrument_id, version, created_at, stance, base_value, bull_value, bear_value),
    )


def _seed_order_and_fill(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    action: str,
    filled_at: datetime,
    price: Decimal,
    units: Decimal,
) -> int:
    """Insert one order + one fill for that order. Returns fill_id."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO orders (instrument_id, action, order_type, status, requested_units)
            VALUES (%s, %s, 'market', 'filled', %s)
            RETURNING order_id
            """,
            (instrument_id, action, units),
        )
        order_id = cur.fetchone()["order_id"]  # type: ignore[index]

        cur.execute(
            """
            INSERT INTO fills (order_id, filled_at, price, units, fees, gross_amount)
            VALUES (%s, %s, %s, %s, 0, %s)
            RETURNING fill_id
            """,
            (order_id, filled_at, price, units, price * units),
        )
        return int(cur.fetchone()["fill_id"])  # type: ignore[index]


def _seed_attribution(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    hold_start: date,
    hold_end: date,
    entry_fill_id: int | None,
    exit_fill_id: int,
    recommendation_id: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO return_attribution (
            instrument_id, hold_start, hold_end, hold_days,
            gross_return_pct, market_return_pct, sector_return_pct,
            model_alpha_pct, timing_alpha_pct, cost_drag_pct, residual_pct,
            entry_fill_id, exit_fill_id, recommendation_id
        ) VALUES (
            %s, %s, %s, %s,
            0.05, 0.02, 0.01,
            0.02, 0, 0, 0,
            %s, %s, %s
        )
        """,
        (
            instrument_id,
            hold_start,
            hold_end,
            (hold_end - hold_start).days,
            entry_fill_id,
            exit_fill_id,
            recommendation_id,
        ),
    )


class TestThesisAccuracyEntryAnchor:
    def test_picks_before_entry_thesis_when_two_on_same_day(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The bug: ``created_at < hold_start::timestamptz + interval '1 day'``
        admitted any thesis on the same calendar day, including a
        hindsight thesis written AFTER the entry fill. The fix anchors
        on ``fills.filled_at`` so a same-day post-entry thesis is
        correctly excluded.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=100, symbol="ACME")

        # Entry fill at 09:00 UTC
        entry_at = datetime(2026, 4, 6, 9, 0, 0, tzinfo=UTC)
        entry_fill_id = _seed_order_and_fill(
            conn,
            instrument_id=100,
            action="BUY",
            filled_at=entry_at,
            price=Decimal("100"),
            units=Decimal("10"),
        )
        # Exit fill 30 days later at $110
        exit_at = datetime(2026, 5, 6, 9, 0, 0, tzinfo=UTC)
        exit_fill_id = _seed_order_and_fill(
            conn,
            instrument_id=100,
            action="EXIT",
            filled_at=exit_at,
            price=Decimal("110"),
            units=Decimal("10"),
        )

        # Two theses on the entry's calendar day:
        #   v1 — 06:00 (BEFORE entry) — base=110 bull=120 bear=90
        #   v2 — 14:00 (AFTER entry) — base=130 bull=150 bear=80 (hindsight!)
        _seed_thesis(
            conn,
            instrument_id=100,
            version=1,
            created_at=datetime(2026, 4, 6, 6, 0, 0, tzinfo=UTC),
            base_value=Decimal("110"),
            bull_value=Decimal("120"),
            bear_value=Decimal("90"),
        )
        _seed_thesis(
            conn,
            instrument_id=100,
            version=2,
            created_at=datetime(2026, 4, 6, 14, 0, 0, tzinfo=UTC),
            base_value=Decimal("130"),
            bull_value=Decimal("150"),
            bear_value=Decimal("80"),
        )

        _seed_attribution(
            conn,
            instrument_id=100,
            hold_start=date(2026, 4, 6),
            hold_end=date(2026, 5, 6),
            entry_fill_id=entry_fill_id,
            exit_fill_id=exit_fill_id,
        )
        conn.commit()

        rows = _thesis_accuracy(conn, period_start=date(2026, 5, 1), period_end=date(2026, 5, 31))

        assert len(rows) == 1
        # Must be the BEFORE-entry thesis (v1), not the hindsight v2.
        assert Decimal(rows[0]["base_value"]) == Decimal("110")
        assert Decimal(rows[0]["bull_value"]) == Decimal("120")
        assert Decimal(rows[0]["bear_value"]) == Decimal("90")
        # Exit price 110 >= base 110 (and < bull 120) → "base".
        assert rows[0]["target_hit"] == "base"

    def test_falls_back_to_recommendation_when_entry_fill_id_null(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """When ``return_attribution.entry_fill_id`` is NULL (legacy row
        from before the column was populated), fall back to
        ``trade_recommendations.created_at`` via
        ``return_attribution.recommendation_id``.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=101, symbol="DELL")

        # Recommendation created at 09:00 UTC
        rec_at = datetime(2026, 4, 7, 9, 0, 0, tzinfo=UTC)
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                INSERT INTO trade_recommendations (instrument_id, created_at, action, status, rationale)
                VALUES (%s, %s, 'BUY', 'approved', 'test rationale')
                RETURNING recommendation_id
                """,
                (101, rec_at),
            )
            rec_id = int(cur.fetchone()["recommendation_id"])  # type: ignore[index]

        exit_fill_id = _seed_order_and_fill(
            conn,
            instrument_id=101,
            action="EXIT",
            filled_at=datetime(2026, 5, 7, 9, 0, 0, tzinfo=UTC),
            price=Decimal("105"),
            units=Decimal("10"),
        )

        # Theses: v1 BEFORE rec timestamp, v2 AFTER (same day, hindsight).
        _seed_thesis(
            conn,
            instrument_id=101,
            version=1,
            created_at=datetime(2026, 4, 7, 6, 0, 0, tzinfo=UTC),
            base_value=Decimal("100"),
            bull_value=Decimal("110"),
            bear_value=Decimal("85"),
        )
        _seed_thesis(
            conn,
            instrument_id=101,
            version=2,
            created_at=datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC),
            base_value=Decimal("120"),
            bull_value=Decimal("130"),
            bear_value=Decimal("80"),
        )

        _seed_attribution(
            conn,
            instrument_id=101,
            hold_start=date(2026, 4, 7),
            hold_end=date(2026, 5, 7),
            entry_fill_id=None,  # fall back to recommendation
            exit_fill_id=exit_fill_id,
            recommendation_id=rec_id,
        )
        conn.commit()

        rows = _thesis_accuracy(conn, period_start=date(2026, 5, 1), period_end=date(2026, 5, 31))

        assert len(rows) == 1
        assert Decimal(rows[0]["base_value"]) == Decimal("100")
        assert Decimal(rows[0]["bull_value"]) == Decimal("110")

    def test_no_anchor_returns_null_target_hit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """When neither entry_fill_id nor recommendation_id is set,
        the LATERAL join cannot anchor a cutoff. The thesis fields
        should be NULL and ``target_hit`` should be None — not silently
        pick the latest thesis (which would be a hindsight read).
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=102, symbol="WIDG")

        exit_fill_id = _seed_order_and_fill(
            conn,
            instrument_id=102,
            action="EXIT",
            filled_at=datetime(2026, 5, 8, 9, 0, 0, tzinfo=UTC),
            price=Decimal("99"),
            units=Decimal("5"),
        )

        # Theses exist, but no anchor.
        _seed_thesis(
            conn,
            instrument_id=102,
            version=1,
            created_at=datetime(2026, 4, 1, 9, 0, 0, tzinfo=UTC),
            base_value=Decimal("100"),
            bull_value=Decimal("110"),
            bear_value=Decimal("90"),
        )

        _seed_attribution(
            conn,
            instrument_id=102,
            hold_start=date(2026, 4, 1),
            hold_end=date(2026, 5, 8),
            entry_fill_id=None,
            exit_fill_id=exit_fill_id,
            recommendation_id=None,
        )
        conn.commit()

        rows = _thesis_accuracy(conn, period_start=date(2026, 5, 1), period_end=date(2026, 5, 31))

        assert len(rows) == 1
        assert rows[0]["base_value"] is None
        assert rows[0]["bull_value"] is None
        assert rows[0]["target_hit"] is None
