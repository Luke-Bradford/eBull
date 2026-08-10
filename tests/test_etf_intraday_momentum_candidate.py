from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.services.etf_intraday_momentum_candidate import (
    CANDIDATE_VERSION,
    DEFINITION,
    CandidateRefusal,
    definition_json,
    opening_signal,
    resolve_gross_feasibility,
)
from app.services.strategy_observation_storage import IntradayBar


def _bar(
    stamp: str,
    *,
    open_: str = "100",
    close: str = "101",
    timeframe: str = "30m",
    instrument_id: int = 1,
    source: str = "etoro/ETORO-RTH-V1/nyse_rth",
) -> IntradayBar:
    opening = Decimal(open_)
    closing = Decimal(close)
    return IntradayBar(
        timeframe=timeframe,  # type: ignore[arg-type]
        bar_time=datetime.fromisoformat(stamp).astimezone(UTC),
        instrument_id=instrument_id,
        open=opening,
        high=max(opening, closing),
        low=min(opening, closing),
        close=closing,
        volume=Decimal("1000000"),
        source=source,
    )


def test_definition_is_stable_and_contains_no_measured_result() -> None:
    payload = definition_json()
    assert CANDIDATE_VERSION.startswith("etf-intraday-momentum-v1+")
    assert DEFINITION.primary_symbol == "SPY"
    assert DEFINITION.robustness_symbols == ("QQQ", "IWM")
    assert "expectancy_pct" not in payload
    assert "win_rate" not in payload


def test_positive_opening_return_fires_long_and_resolves_only_gross_proxy() -> None:
    signal = opening_signal(
        symbol="spy",
        prior_close_bar=_bar("2026-08-07T19:30:00+00:00", close="100"),
        opening_bar=_bar("2026-08-10T13:30:00+00:00", close="102"),
    )
    assert signal.opening_return == Decimal("0.02")
    assert signal.published_side == "long"
    assert signal.adaptation_verdict == "fired_long"
    assert signal.known_at.isoformat() == "2026-08-10T10:00:00-04:00"

    outcome = resolve_gross_feasibility(
        signal,
        last_half_hour_bar=_bar("2026-08-10T19:30:00+00:00", open_="200", close="202"),
    )
    assert outcome.gross_return == Decimal("0.01")
    assert "historical_entry_exit_quotes_unavailable" in outcome.promotion_refusals


def test_nonpositive_opening_return_preserves_published_short_but_long_only_abstains() -> None:
    signal = opening_signal(
        symbol="SPY",
        prior_close_bar=_bar("2026-08-07T19:30:00+00:00", close="100"),
        opening_bar=_bar("2026-08-10T13:30:00+00:00", close="99"),
    )
    assert signal.published_side == "short"
    assert signal.adaptation_verdict == "not_fired"
    outcome = resolve_gross_feasibility(
        signal,
        last_half_hour_bar=_bar("2026-08-10T19:30:00+00:00", open_="100", close="90"),
    )
    assert outcome.gross_return == 0


@pytest.mark.parametrize(
    ("symbol", "bar", "message"),
    [
        ("AAPL", _bar("2026-08-10T13:30:00+00:00"), "outside the preregistered ETF set"),
        ("SPY", _bar("2026-08-10T14:00:00+00:00"), "must start at 09:30:00"),
        ("SPY", _bar("2026-08-10T13:30:00+00:00", timeframe="5m"), "requires a 30m bar"),
        ("SPY", _bar("2026-11-27T14:30:00+00:00"), "excludes closed and half-day"),
    ],
)
def test_wrong_universe_interval_or_session_refuses(symbol: str, bar: IntradayBar, message: str) -> None:
    with pytest.raises(CandidateRefusal, match=message):
        opening_signal(
            symbol=symbol,
            prior_close_bar=_bar("2026-08-07T19:30:00+00:00", close="100"),
            opening_bar=bar,
        )


def test_last_bar_must_be_same_session_and_exact_interval() -> None:
    signal = opening_signal(
        symbol="SPY",
        prior_close_bar=_bar("2026-08-07T19:30:00+00:00", close="100"),
        opening_bar=_bar("2026-08-10T13:30:00+00:00"),
    )
    with pytest.raises(CandidateRefusal, match="must start at 15:30:00"):
        resolve_gross_feasibility(
            signal,
            last_half_hour_bar=_bar("2026-08-10T19:00:00+00:00"),
        )
    with pytest.raises(CandidateRefusal, match="must share a session"):
        resolve_gross_feasibility(
            signal,
            last_half_hour_bar=_bar("2026-08-11T19:30:00+00:00"),
        )


def test_source_instrument_and_immediate_prior_session_are_structural() -> None:
    opening = _bar("2026-08-10T13:30:00+00:00")
    with pytest.raises(CandidateRefusal, match="frozen namespaced etoro RTH source"):
        opening_signal(
            symbol="SPY",
            prior_close_bar=_bar("2026-08-07T19:30:00+00:00", source="other"),
            opening_bar=opening,
        )
    with pytest.raises(CandidateRefusal, match="share an instrument"):
        opening_signal(
            symbol="SPY",
            prior_close_bar=_bar("2026-08-07T19:30:00+00:00", instrument_id=2),
            opening_bar=opening,
        )
    with pytest.raises(CandidateRefusal, match="immediately preceding trading session"):
        opening_signal(
            symbol="SPY",
            prior_close_bar=_bar("2026-08-06T19:30:00+00:00"),
            opening_bar=opening,
        )
