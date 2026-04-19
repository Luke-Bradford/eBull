from unittest.mock import MagicMock

from app.services.sync_orchestrator.content_predicates import (
    candles_content_ok,
    fundamentals_content_ok,
)


def test_candles_content_ok_when_no_missing_rows() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (0,)
    ok, detail = candles_content_ok(conn)
    assert ok is True
    assert "current" in detail.lower() or "ok" in detail.lower()


def test_candles_content_missing_reports_count() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (17,)
    ok, detail = candles_content_ok(conn)
    assert ok is False
    assert "17" in detail


def test_fundamentals_content_ok_when_no_missing() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (0,)
    ok, _ = fundamentals_content_ok(conn)
    assert ok is True


def test_fundamentals_content_missing_reports_count() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (5,)
    ok, detail = fundamentals_content_ok(conn)
    assert ok is False
    assert "5" in detail
