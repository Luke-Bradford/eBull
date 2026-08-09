from datetime import date

import pytest

from scripts.verify_2437_confluence import _market_context


def test_market_context_uses_signal_close_not_next_open_session() -> None:
    lookback = date(2026, 7, 8)
    signal = date(2026, 8, 6)
    entry = date(2026, 8, 7)
    market_returns = {signal: -0.01, entry: 0.08}
    market_cumulative = {lookback: 1.0, signal: 0.95, entry: 1.026}

    context = _market_context(signal, lookback, market_returns, market_cumulative)

    assert context is not None
    assert context[0] == pytest.approx(-0.05)
    assert context[1] is False


def test_market_context_fails_closed_when_signal_day_is_missing() -> None:
    signal = date(2026, 8, 6)
    lookback = date(2026, 7, 8)

    assert _market_context(signal, lookback, {}, {lookback: 1.0}) is None
