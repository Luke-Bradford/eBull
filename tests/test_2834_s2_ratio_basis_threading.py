"""#2834 §7 item 2 — S-2's split-corrected ratio basis reaches the manifest path.

The refusal this replaces compared carrier VALUES against archive BASES and never
fired, so every S-2 backtest on the as-traded Intrader pin scored the split. These
tests pin the replacement: the caller supplies the basis, ``None`` refuses, the
adapter scores the basis it was given, and ``segmented_member`` cuts it with the
series.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services import backtest_run
from app.services.indicator_series import BarSeries
from app.services.market_regime import unconstrained_regime
from app.services.strategies.s2_cross_sectional_momentum import rebalance_dates, s2_member
from app.services.strategy_manifest import RATIO_BASIS_CONSUMERS, STRATEGY_MANIFEST
from app.services.strategy_price_basis import from_undeclared_source
from app.services.strategy_segmented_evaluation import segmented_member
from app.services.technical_analysis import OHLCVRow

S2 = "s2-cross-sectional-momentum"
S10 = "s10-relative-strength-leader"
UNIVERSE = "survivorship_free"
REASON = "quarantined_bar"
N_BARS = 400
SPLIT_AT = 300


def _bars(closes: Sequence[float], *, start: date = date(2020, 1, 1)) -> BarSeries:
    rows: list[OHLCVRow] = [
        {
            "open": Decimal(str(c)),
            "high": Decimal(str(c)),
            "low": Decimal(str(c)),
            "close": Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c in closes
    ]
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _as_traded_and_corrected() -> tuple[BarSeries, BarSeries]:
    """A steadily rising name with a 2-for-1 split at bar ``SPLIT_AT``.

    As traded, the close halves at the split; corrected, the pre-split bars are
    divided by two so the series is continuous. A 12-1 ratio spanning the split
    is ~0.5x the true return on the first and the true return on the second.
    """
    true = [20.0 + 0.1 * i for i in range(N_BARS)]
    as_traded = _bars([c if i >= SPLIT_AT else 2 * c for i, c in enumerate(true)])
    corrected = BarSeries(dates=as_traded.dates, rows=_bars(true).rows)
    return as_traded, corrected


def _call_member(series: BarSeries, ratio_basis: BarSeries | None) -> Any:
    entry = STRATEGY_MANIFEST[S2]
    assert entry.member is not None
    dates = entry.decision_calendar(series.dates)
    assert dates is not None
    return entry.member(
        series,
        panel_decision_dates=dates,
        universe=UNIVERSE,
        masked_reason=REASON,
        regime=unconstrained_regime(len(series)),
        price_basis=from_undeclared_source(series=series),
        ratio_basis=ratio_basis,
    )


def test_s2_refuses_a_path_that_built_no_ratio_basis() -> None:
    as_traded, _ = _as_traded_and_corrected()
    with pytest.raises(ValueError, match="split-consistent ratio basis"):
        _call_member(as_traded, None)


def test_s2_scores_the_supplied_basis_not_the_as_traded_bars() -> None:
    as_traded, corrected = _as_traded_and_corrected()
    via_manifest = _call_member(as_traded, corrected)
    dates = rebalance_dates(as_traded.dates)
    expected = s2_member(
        as_traded, ratio_basis=corrected, panel_rebalance_dates=dates, universe=UNIVERSE, close_reason=REASON
    )
    contaminated = s2_member(
        as_traded, ratio_basis=as_traded, panel_rebalance_dates=dates, universe=UNIVERSE, close_reason=REASON
    )
    assert via_manifest.score.values == expected.score.values
    # ⚠ The fixture must actually discriminate, or the equality above proves nothing.
    assert via_manifest.score.values != contaminated.score.values


def _segmented(series: BarSeries, ratio_basis: BarSeries | None, *, breaks: tuple[date, ...] = ()) -> Any:
    entry = STRATEGY_MANIFEST[S2]
    return segmented_member(
        entry,
        series,
        panel_decision_dates=frozenset(series.dates),
        universe=UNIVERSE,
        masked_reason=REASON,
        unresolved_breaks=breaks,
        regime=unconstrained_regime(len(series)),
        price_basis=from_undeclared_source(series=series),
        ratio_basis=ratio_basis,
    )


def test_segmented_member_cuts_the_ratio_basis_with_the_series() -> None:
    as_traded, corrected = _as_traded_and_corrected()
    breaks = (as_traded.dates[100],)
    staged = _segmented(as_traded, corrected, breaks=breaks)
    after = BarSeries(dates=as_traded.dates[100:], rows=as_traded.rows[100:])
    after_basis = BarSeries(dates=corrected.dates[100:], rows=corrected.rows[100:])
    direct = s2_member(
        after,
        ratio_basis=after_basis,
        panel_rebalance_dates=frozenset(after.dates),
        universe=UNIVERSE,
        close_reason=REASON,
    )
    expected = {
        when: value
        for when, value in zip(direct.dates, direct.score.values, strict=True)
        if value is not None and when in staged.scores
    }
    assert expected, "no scored bar after the break — the fixture is too short to test slicing"
    assert {when: staged.scores[when] for when in expected} == expected


def test_segmented_member_refuses_a_ratio_basis_on_other_dates() -> None:
    as_traded, corrected = _as_traded_and_corrected()
    shifted = BarSeries(dates=tuple(d + timedelta(days=1) for d in corrected.dates), rows=corrected.rows)
    with pytest.raises(ValueError, match="same bars on another basis"):
        _segmented(as_traded, shifted)


def test_s2_is_the_only_ratio_basis_consumer() -> None:
    assert RATIO_BASIS_CONSUMERS == frozenset({S2})


def test_ratio_basis_for_a_non_consumer_touches_no_connection() -> None:
    as_traded, _ = _as_traded_and_corrected()
    got = backtest_run._ratio_basis_for(
        object(),  # type: ignore[arg-type]  # any attribute access would raise
        STRATEGY_MANIFEST[S10],
        as_traded,
        series_id=1,
        through_date=as_traded.dates[-1],
    )
    assert got is None


def test_ratio_basis_for_s2_returns_the_loaded_correction(monkeypatch: pytest.MonkeyPatch) -> None:
    as_traded, corrected = _as_traded_and_corrected()
    seen: dict[str, Any] = {}

    class _Corrected:
        ratio_basis = corrected

    def _fake_load(conn: Any, series_id: int, series: BarSeries, *, through_date: date | None = None) -> Any:
        seen.update(conn=conn, series_id=series_id, series=series, through_date=through_date)
        return _Corrected(), "split_corrected"

    monkeypatch.setattr(backtest_run, "load_ratio_basis", _fake_load)
    conn = object()
    got = backtest_run._ratio_basis_for(
        conn,  # type: ignore[arg-type]
        STRATEGY_MANIFEST[S2],
        as_traded,
        series_id=42,
        through_date=as_traded.dates[-1],
    )
    assert got is corrected
    assert seen == {"conn": conn, "series_id": 42, "series": as_traded, "through_date": as_traded.dates[-1]}
