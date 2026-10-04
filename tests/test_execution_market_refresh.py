"""#3578 — the execute jobs' pre-batch hook: halts, then the due entries' quotes; a failed fetch aborts the fire."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.market_data import QuoteRefreshSummary
from app.workers import scheduler


def _wire(monkeypatch: pytest.MonkeyPatch, summary: QuoteRefreshSummary) -> list[Any]:
    calls: list[Any] = []
    conn = MagicMock()

    @contextmanager
    def _connect(**_: object) -> Any:
        yield conn

    monkeypatch.setattr(scheduler, "_refresh_strategy_halt_feed", lambda: calls.append("halts"))
    monkeypatch.setattr(scheduler, "EtoroMarketDataProvider", MagicMock())
    monkeypatch.setattr(scheduler, "connect_job", _connect)

    def _refresh(_market: object, c: object, ids: list[int]) -> QuoteRefreshSummary:
        assert c is conn
        calls.append(("quotes", ids))
        return summary

    monkeypatch.setattr(scheduler, "refresh_signal_quotes", _refresh)
    return calls


def _due(_conn: object, *, today: date) -> list[int]:
    assert isinstance(today, date)
    return [7, 9]


def test_the_hook_refreshes_halts_then_the_due_entries_quotes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _wire(monkeypatch, QuoteRefreshSummary(2, 2, 0, 0))
    scheduler._execution_market_refresh("job", ("k", "u"), _due)()
    assert calls == ["halts", ("quotes", [7, 9])]


def test_a_failed_quote_fetch_aborts_the_fire(monkeypatch: pytest.MonkeyPatch) -> None:
    boom = RuntimeError("eToro down")
    _wire(monkeypatch, QuoteRefreshSummary(2, 0, 2, 0, batch_failed=True, batch_error=boom))
    with pytest.raises(RuntimeError, match="eToro down"):
        scheduler._execution_market_refresh("job", ("k", "u"), _due)()


def test_a_partial_refresh_proceeds_and_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    calls = _wire(monkeypatch, QuoteRefreshSummary(2, 1, 1, 0))
    with caplog.at_level("WARNING", logger=scheduler.logger.name):
        scheduler._execution_market_refresh("job", ("k", "u"), _due)()
    assert calls == ["halts", ("quotes", [7, 9])]
    assert "refreshed for 1 of 2" in caplog.text


@pytest.mark.parametrize("summary", [QuoteRefreshSummary(2, 0, 2, 0), QuoteRefreshSummary(2, 0, 0, 0)])
def test_no_quote_stored_for_any_due_instrument_aborts_the_fire(
    monkeypatch: pytest.MonkeyPatch, summary: QuoteRefreshSummary
) -> None:
    """Every name omitted, or every upsert failed: a total failure, not a partial one."""
    _wire(monkeypatch, summary)
    with pytest.raises(RuntimeError, match="no execution-time quote stored for any of 2"):
        scheduler._execution_market_refresh("job", ("k", "u"), _due)()


def test_nothing_due_on_the_re_query_is_not_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _wire(monkeypatch, QuoteRefreshSummary(0, 0, 0, 0))
    scheduler._execution_market_refresh("job", ("k", "u"), _due)()


def test_a_quote_whose_commit_fails_is_not_counted_as_updated(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.providers.market_data import Quote
    from app.services import market_data

    @contextmanager
    def _failing_commit() -> Any:
        yield
        raise RuntimeError("commit failed")

    conn = MagicMock()
    conn.transaction.side_effect = _failing_commit
    upserted: list[int] = []
    monkeypatch.setattr(market_data, "_upsert_quote", lambda _c, iid, *_: upserted.append(iid) or True)
    provider = MagicMock()
    provider.get_quotes.return_value = [
        Quote(instrument_id=7, timestamp=MagicMock(), bid=Decimal(1), ask=Decimal(1), last=None)
    ]
    summary = market_data.refresh_quotes(provider, conn, [(7, "X")])
    assert upserted == [7]
    assert (summary.quotes_updated, summary.spread_flags_set) == (0, 0)
