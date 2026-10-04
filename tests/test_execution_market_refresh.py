"""#3578 — the execute jobs' pre-batch hook: halts, then the due entries' quotes; a failed fetch aborts the fire."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
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
