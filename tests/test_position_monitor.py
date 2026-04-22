"""Tests for app.services.position_monitor — intraday SL/TP/thesis-break detection.

Structure:
  - TestCheckPositionHealth — full check_position_health with mocked DB
  - TestPersistPositionAlerts — writer diff-logic tests against real ebull_test DB
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services.position_monitor import (
    EXIT_RED_FLAG_THRESHOLD,
    MonitorAlert,
    MonitorResult,
    PersistStats,
    check_position_health,
    persist_position_alerts,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    """Build a mock cursor that returns dict rows from fetchall."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    """Build a mock connection whose cursor() calls consume cursors in order."""
    conn = MagicMock()
    cursor_iter = iter(cursors)
    conn.cursor.side_effect = lambda **kwargs: next(cursor_iter)
    conn.execute.return_value = MagicMock()
    return conn


def _position_row(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    stop_loss_rate: float | None = 140.0,
    take_profit_rate: float | None = 200.0,
    bid: float | None = 160.0,
    red_flag_score: float | None = 0.20,
) -> dict[str, Any]:
    """Build a synthetic position row matching the query projection."""
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "stop_loss_rate": stop_loss_rate,
        "take_profit_rate": take_profit_rate,
        "bid": bid,
        "red_flag_score": red_flag_score,
    }


# ---------------------------------------------------------------------------
# TestCheckPositionHealth
# ---------------------------------------------------------------------------


class TestCheckPositionHealth:
    """Full check_position_health with mocked DB."""

    def test_no_open_positions_returns_empty(self) -> None:
        """Empty fetchall → 0 checked, no alerts."""
        conn = _make_conn([_make_cursor([])])
        result = check_position_health(conn)
        assert result.positions_checked == 0
        assert result.alerts == ()

    def test_position_below_stop_loss_generates_alert(self) -> None:
        """bid < sl → sl_breach alert."""
        row = _position_row(bid=130.0, stop_loss_rate=140.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert result.positions_checked == 1
        sl_alerts = [a for a in result.alerts if a.alert_type == "sl_breach"]
        assert len(sl_alerts) == 1
        alert = sl_alerts[0]
        assert alert.instrument_id == 1
        assert alert.symbol == "AAPL"
        assert alert.current_bid == Decimal("130.0")
        assert "stop_loss" in alert.detail

    def test_position_above_take_profit_generates_alert(self) -> None:
        """bid >= tp → tp_breach alert."""
        row = _position_row(bid=200.0, take_profit_rate=200.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tp_alerts = [a for a in result.alerts if a.alert_type == "tp_breach"]
        assert len(tp_alerts) == 1
        alert = tp_alerts[0]
        assert alert.current_bid == Decimal("200.0")
        assert "take_profit" in alert.detail

    def test_position_strictly_above_take_profit_generates_alert(self) -> None:
        """bid > tp (strict) also triggers tp_breach."""
        row = _position_row(bid=210.0, take_profit_rate=200.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tp_alerts = [a for a in result.alerts if a.alert_type == "tp_breach"]
        assert len(tp_alerts) == 1

    def test_position_with_high_red_flag_generates_thesis_break_alert(self) -> None:
        """red_flag >= 0.80 → thesis_break alert."""
        row = _position_row(red_flag_score=0.85)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 1
        alert = tb_alerts[0]
        assert "red_flag" in alert.detail
        assert "threshold" in alert.detail

    def test_red_flag_exactly_at_threshold_generates_alert(self) -> None:
        """red_flag == EXIT_RED_FLAG_THRESHOLD (0.80) → thesis_break (inclusive boundary)."""
        row = _position_row(red_flag_score=float(EXIT_RED_FLAG_THRESHOLD))
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 1

    def test_red_flag_just_below_threshold_no_alert(self) -> None:
        """red_flag < EXIT_RED_FLAG_THRESHOLD → no thesis_break alert."""
        row = _position_row(red_flag_score=0.79)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 0

    def test_healthy_position_generates_no_alert(self) -> None:
        """Price in range, low red flag → no alerts."""
        row = _position_row(
            bid=160.0,
            stop_loss_rate=140.0,
            take_profit_rate=200.0,
            red_flag_score=0.20,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert result.positions_checked == 1
        assert result.alerts == ()

    def test_null_sl_tp_does_not_crash(self) -> None:
        """NULL SL/TP/red_flag → no alerts, no crash."""
        row = _position_row(
            stop_loss_rate=None,
            take_profit_rate=None,
            red_flag_score=None,
            bid=160.0,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert result.positions_checked == 1
        assert result.alerts == ()

    def test_null_bid_skips_sl_and_tp_checks(self) -> None:
        """NULL bid → sl_breach and tp_breach checks skipped; no alerts."""
        row = _position_row(
            bid=None,
            stop_loss_rate=140.0,
            take_profit_rate=200.0,
            red_flag_score=0.10,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        # red_flag is 0.10 < threshold so no thesis_break either
        assert result.alerts == ()

    def test_multiple_positions_counted_correctly(self) -> None:
        """Multiple positions → positions_checked equals the row count."""
        rows = [
            _position_row(instrument_id=1, symbol="AAPL"),
            _position_row(instrument_id=2, symbol="TSLA"),
            _position_row(instrument_id=3, symbol="MSFT"),
        ]
        conn = _make_conn([_make_cursor(rows)])
        result = check_position_health(conn)
        assert result.positions_checked == 3

    def test_position_can_trigger_multiple_alert_types(self) -> None:
        """A single position can generate both sl_breach and thesis_break simultaneously."""
        row = _position_row(
            bid=130.0,
            stop_loss_rate=140.0,
            take_profit_rate=200.0,
            red_flag_score=0.90,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        alert_types = {a.alert_type for a in result.alerts}
        assert "sl_breach" in alert_types
        assert "thesis_break" in alert_types
        # bid=130 < tp=200 so no tp_breach
        assert "tp_breach" not in alert_types

    def test_alert_carries_correct_instrument_id_and_symbol(self) -> None:
        """Alert fields match the source position row."""
        row = _position_row(instrument_id=42, symbol="NVDA", bid=130.0, stop_loss_rate=140.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert len(result.alerts) == 1
        alert = result.alerts[0]
        assert alert.instrument_id == 42
        assert alert.symbol == "NVDA"

    def test_result_is_frozen_dataclass(self) -> None:
        """MonitorResult and MonitorAlert are frozen (immutable)."""
        conn = _make_conn([_make_cursor([])])
        result = check_position_health(conn)
        assert dataclasses.is_dataclass(result)
        # Frozen: assigning to a field raises FrozenInstanceError
        try:
            result.positions_checked = 999  # type: ignore[misc]
            assert False, "Expected FrozenInstanceError"
        except Exception as e:
            assert "FrozenInstanceError" in type(e).__name__ or "cannot assign" in str(e).lower()

    def test_alert_current_bid_is_none_when_bid_null(self) -> None:
        """When bid is NULL but red_flag triggers thesis_break, current_bid on alert is None."""
        row = _position_row(bid=None, red_flag_score=0.90)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 1
        assert tb_alerts[0].current_bid is None


# ---------------------------------------------------------------------------
# TestPersistPositionAlerts
# ---------------------------------------------------------------------------

_next_instrument_id = 0


def _seed_instrument(conn: psycopg.Connection[tuple], *, symbol: str = "AAPL") -> int:
    """Insert a tradable instrument, return instrument_id.

    ``instruments.instrument_id`` is a BIGINT PRIMARY KEY with NO default
    (sql/001_init.sql:2), so the caller supplies the id. ``symbol`` and
    ``company_name`` are NOT NULL (sql/001_init.sql:3-4) — no fixture-neutral
    defaults. Prevention: ``INSERT INTO instruments fixtures must supply
    is_tradable``; we supply it explicitly even though it has a default.
    """
    global _next_instrument_id
    _next_instrument_id += 1
    iid = _next_instrument_id
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
    conn.commit()
    return iid


@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestPersistPositionAlerts:
    """Writer diff-logic tests against real ebull_test DB."""

    def test_empty_and_empty_is_noop(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        result = MonitorResult(positions_checked=0, alerts=())
        stats = persist_position_alerts(ebull_test_conn, result)
        assert stats == PersistStats(opened=0, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM position_alerts")
            assert cur.fetchone() == (0,)

    def test_new_breach_opens_episode(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        stats = persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,)))
        assert stats == PersistStats(opened=1, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_type, detail, current_bid, resolved_at FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "sl_breach"
        assert rows[0][1] == "bid=130 < sl=140"
        assert rows[0][2] == Decimal("130")
        assert rows[0][3] is None

    def test_still_breaching_is_noop(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,)))
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_id, opened_at FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            first = cur.fetchone()
        assert first is not None

        stats = persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,)))
        assert stats == PersistStats(opened=0, resolved=0, unchanged=1)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_id, opened_at FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == first[0]
        assert rows[0][1] == first[1]

    def test_clearance_resolves_episode(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,)))
        stats = persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=()))
        assert stats == PersistStats(opened=0, resolved=1, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolved_at FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] is not None

    def test_re_breach_after_clearance_opens_new_episode(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,)))
        persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=()))
        stats = persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,)))
        assert stats == PersistStats(opened=1, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolved_at FROM position_alerts WHERE instrument_id = %s ORDER BY alert_id",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0][0] is not None
        assert rows[1][0] is None

    def test_mixed_across_alert_types(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        sl = MonitorAlert(
            instrument_id=iid, symbol="AAPL", alert_type="sl_breach", detail="sl", current_bid=Decimal("100")
        )
        tp = MonitorAlert(
            instrument_id=iid, symbol="AAPL", alert_type="tp_breach", detail="tp", current_bid=Decimal("250")
        )
        thesis = MonitorAlert(
            instrument_id=iid, symbol="AAPL", alert_type="thesis_break", detail="red=0.9", current_bid=None
        )
        persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(sl,)))
        stats = persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(tp, thesis)))
        assert stats == PersistStats(opened=2, resolved=1, unchanged=0)

    def test_mixed_across_instruments(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid_a = _seed_instrument(ebull_test_conn, symbol="AAPL")
        iid_b = _seed_instrument(ebull_test_conn, symbol="MSFT")
        iid_c = _seed_instrument(ebull_test_conn, symbol="GOOG")
        sl_a = MonitorAlert(iid_a, "AAPL", "sl_breach", "a", Decimal("100"))
        sl_b = MonitorAlert(iid_b, "MSFT", "sl_breach", "b", Decimal("100"))
        sl_c = MonitorAlert(iid_c, "GOOG", "sl_breach", "c", Decimal("100"))
        persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=2, alerts=(sl_a, sl_b)))
        stats = persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=2, alerts=(sl_a, sl_c)))
        assert stats == PersistStats(opened=1, resolved=1, unchanged=1)

    def test_partial_unique_index_blocks_duplicate_open_pair(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Direct DB-level test: two open rows for same (instrument, type) fail.

        Wraps both INSERTs in a ``conn.transaction()`` savepoint so the
        UniqueViolation is absorbed cleanly — without it, the connection's
        implicit transaction remains in an aborted state after the second
        INSERT fails, which would leak into any subsequent statement on
        the same connection (brittle under future test-code additions).
        """
        iid = _seed_instrument(ebull_test_conn)
        with pytest.raises(psycopg.errors.UniqueViolation):
            with ebull_test_conn.transaction():
                with ebull_test_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO position_alerts "
                        "(instrument_id, alert_type, detail) "
                        "VALUES (%s, 'sl_breach', 'first')",
                        (iid,),
                    )
                    cur.execute(
                        "INSERT INTO position_alerts "
                        "(instrument_id, alert_type, detail) "
                        "VALUES (%s, 'sl_breach', 'second')",
                        (iid,),
                    )

    def test_partial_unique_index_allows_reopen_after_resolve(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Partial index WHERE resolved_at IS NULL — a resolved row does not
        block a new open row for the same (instrument, type)."""
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO position_alerts "
                "(instrument_id, alert_type, detail, resolved_at) "
                "VALUES (%s, 'sl_breach', 'first', now())",
                (iid,),
            )
            cur.execute(
                "INSERT INTO position_alerts "
                "(instrument_id, alert_type, detail) "
                "VALUES (%s, 'sl_breach', 'second-open')",
                (iid,),
            )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            assert cur.fetchone() == (2,)

    def test_all_three_alert_types_for_same_instrument(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        sl = MonitorAlert(iid, "AAPL", "sl_breach", "s", Decimal("100"))
        tp = MonitorAlert(iid, "AAPL", "tp_breach", "t", Decimal("250"))
        th = MonitorAlert(iid, "AAPL", "thesis_break", "r", None)
        stats = persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(sl, tp, th)))
        assert stats == PersistStats(opened=3, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_type FROM position_alerts WHERE instrument_id = %s ORDER BY alert_type",
                (iid,),
            )
            types = [row[0] for row in cur.fetchall()]
        assert types == ["sl_breach", "thesis_break", "tp_breach"]

    def test_current_bid_null_passes_through(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(iid, "AAPL", "thesis_break", "red=0.9", None)
        persist_position_alerts(ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,)))
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT current_bid FROM position_alerts WHERE instrument_id = %s", (iid,))
            row = cur.fetchone()
        assert row == (None,)
