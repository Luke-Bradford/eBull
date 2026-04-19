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


def test_candles_content_query_filters_tradable_instruments() -> None:
    # Pin the is_tradable filter so a future refactor cannot silently
    # drop it — a delisted instrument with tier 1/2 coverage would
    # otherwise make the layer permanently fail content freshness.
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (0,)
    candles_content_ok(conn)
    sql = conn.execute.call_args.args[0]
    assert "is_tradable = TRUE" in sql or "is_tradable=TRUE" in sql
    assert "coverage_tier IN (1, 2)" in sql or "coverage_tier IN(1, 2)" in sql


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
