from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.strategy_regime_context import (
    REGIME_VERSION,
    CompletedSessionPanel,
    ReferenceSessionCoverage,
    RegimeMember,
    decompose_return,
    measure_completed_session_regime,
    select_completed_session_dates,
)


def _dates(count: int = 21) -> tuple[date, ...]:
    first = date(2026, 7, 1)
    return tuple(first + timedelta(days=index) for index in range(count))


def _member(
    instrument_id: int,
    industry_id: int,
    closes: Sequence[str | None],
) -> RegimeMember:
    return RegimeMember(
        instrument_id=instrument_id,
        provider_industry_id=industry_id,
        closes=tuple(None if value is None else Decimal(value) for value in closes),
        return_links=(False,) + (True,) * (len(closes) - 1),
    )


def _panel(*members: RegimeMember, dates: tuple[date, ...] | None = None) -> CompletedSessionPanel:
    return CompletedSessionPanel(
        session_dates=_dates() if dates is None else dates,
        members=tuple(members),
        cohort_version="liquid-us-common-v1",
        source_version="fixture-v1",
        price_basis="quarantine_joinable_vendor_close",
    )


def _rising(start: int, step: int = 1) -> list[str]:
    return [str(start + step * index) for index in range(21)]


def test_exact_horizon_returns_breadth_dispersion_and_prior_trend() -> None:
    result = measure_completed_session_regime(
        _panel(_member(1, 10, _rising(100)), _member(2, 10, _rising(200, -1))),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )

    one = result.market_horizons[0]
    expected_a = Decimal(120) / Decimal(119) - 1
    expected_b = Decimal(180) / Decimal(181) - 1
    assert one.verdict == "usable"
    assert one.equal_weight_return == (expected_a + expected_b) / 2
    assert one.advance_share == Decimal("0.5")
    assert one.return_dispersion is not None and one.return_dispersion > 0
    assert result.prior_trend.share == Decimal("0.5")
    assert result.prior_trend.coverage == Decimal(1)
    assert result.version == REGIME_VERSION


def test_horizons_are_exactly_1_3_5_10_20_completed_sessions() -> None:
    result = measure_completed_session_regime(
        _panel(_member(1, 10, _rising(100)), _member(2, 10, _rising(100))),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )
    assert [item.horizon_sessions for item in result.market_horizons] == [1, 3, 5, 10, 20]
    assert result.market_horizons[-1].equal_weight_return == Decimal("0.2")


def test_missing_endpoint_is_not_a_zero_return_and_can_refuse_coverage() -> None:
    incomplete = _rising(100)
    incomplete[-1] = None  # type: ignore[assignment]
    result = measure_completed_session_regime(
        _panel(
            _member(1, 10, _rising(100)),
            _member(2, 10, incomplete),
            _member(3, 10, _rising(300)),
        ),
        minimum_coverage=Decimal("0.8"),
        minimum_sector_members=2,
    )
    one = result.market_horizons[0]
    assert one.verdict == "refused"
    assert one.observed_count == 2
    assert one.coverage == Decimal(2) / Decimal(3)
    assert one.equal_weight_return is None
    assert one.refusal_reason == "coverage:2/3<0.8"


def test_missing_in_middle_only_affects_measures_that_consume_it() -> None:
    values = _rising(100)
    values[8] = None  # type: ignore[assignment]
    result = measure_completed_session_regime(
        _panel(_member(1, 10, values), _member(2, 10, _rising(200))),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )
    assert all(item.verdict == "usable" for item in result.market_horizons)
    assert result.prior_trend.verdict == "refused"
    assert result.prior_trend.share is None
    assert result.prior_trend.observed_count == 1
    assert result.prior_trend.refusal_reason == "coverage:1/2<1"
    assert result.common_movement.verdict == "refused"
    assert result.common_movement.balanced_count == 1


def test_sector_uses_only_its_point_in_time_members_and_reports_relative_component() -> None:
    result = measure_completed_session_regime(
        _panel(
            _member(1, 10, _rising(100, 2)),
            _member(2, 10, _rising(200, 2)),
            _member(3, 20, _rising(300, -1)),
            _member(4, 20, _rising(400, -1)),
        ),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )
    market = result.market_horizons[0].equal_weight_return
    sector = result.sectors[0].horizons[0].equal_weight_return
    assert market is not None and sector is not None and sector > market
    instrument = Decimal(140) / Decimal(138) - 1
    components = decompose_return(instrument_return=instrument, market_return=market, sector_return=sector)
    assert sum(components, Decimal(0)) == instrument


def test_tiny_sector_refuses_instead_of_publishing_one_name_regime() -> None:
    result = measure_completed_session_regime(
        _panel(
            _member(1, 10, _rising(100)),
            _member(2, 10, _rising(200)),
            _member(3, 20, _rising(300)),
        ),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )
    tiny = next(sector for sector in result.sectors if sector.provider_industry_id == 20)
    assert all(item.verdict == "refused" for item in tiny.horizons)
    assert all(item.refusal_reason == "sector_members:1<2" for item in tiny.horizons)
    assert tiny.prior_trend.share is None
    assert tiny.prior_trend.refusal_reason == "sector_members:1<2"
    assert tiny.common_movement.verdict == "refused"


def test_common_movement_is_one_for_identical_paths() -> None:
    result = measure_completed_session_regime(
        _panel(_member(1, 10, _rising(100)), _member(2, 10, _rising(100))),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )
    assert result.common_movement.verdict == "usable"
    assert result.common_movement.variance_share == Decimal(1)


def test_common_movement_cancels_for_opposing_equal_return_paths() -> None:
    # These are generated from alternating +/-1% returns so the equal-weight
    # daily return is exactly zero while each constituent varies.
    first = [Decimal(100)]
    second = [Decimal(100)]
    for index in range(20):
        move = Decimal("0.01") if index % 2 == 0 else Decimal("-0.01")
        first.append(first[-1] * (1 + move))
        second.append(second[-1] * (1 - move))
    result = measure_completed_session_regime(
        _panel(
            RegimeMember(1, 10, tuple(first), (False,) + (True,) * 20),
            RegimeMember(2, 10, tuple(second), (False,) + (True,) * 20),
        ),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )
    assert result.common_movement.verdict == "usable"
    assert result.common_movement.variance_share == Decimal(0)


def test_unresolved_unit_break_invalidates_every_window_that_crosses_it() -> None:
    links = [False] + [True] * 20
    links[-2] = False
    result = measure_completed_session_regime(
        _panel(
            RegimeMember(1, 10, tuple(Decimal(value) for value in _rising(100)), tuple(links)),
            _member(2, 10, _rising(200)),
        ),
        minimum_coverage=Decimal("1"),
        minimum_sector_members=2,
    )
    assert result.market_horizons[0].verdict == "usable"
    assert result.market_horizons[1].verdict == "refused"
    assert result.market_horizons[1].observed_count == 1
    assert result.prior_trend.verdict == "refused"
    assert result.common_movement.verdict == "refused"


@pytest.mark.parametrize(
    ("panel", "message"),
    [
        (_panel(_member(1, 10, _rising(100)), dates=_dates(20)), "at least 21"),
        (
            _panel(
                _member(1, 10, _rising(100)),
                _member(1, 10, _rising(200)),
            ),
            "unique and positive",
        ),
        (
            _panel(RegimeMember(1, 10, tuple(Decimal(1) for _ in range(20)), (False,) + (True,) * 19)),
            "not aligned",
        ),
    ],
)
def test_invalid_panels_fail_closed(panel: CompletedSessionPanel, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        measure_completed_session_regime(
            panel,
            minimum_coverage=Decimal("1"),
            minimum_sector_members=2,
        )


def test_unsorted_or_duplicate_sessions_are_rejected() -> None:
    sessions = list(_dates())
    sessions[-1] = sessions[-2]
    with pytest.raises(ValueError, match="strictly increasing"):
        measure_completed_session_regime(
            _panel(_member(1, 10, _rising(100)), dates=tuple(sessions)),
            minimum_coverage=Decimal("1"),
            minimum_sector_members=2,
        )


def test_session_anchor_ignores_a_partial_latest_reference_date() -> None:
    observations = tuple(
        ReferenceSessionCoverage(session_date, 90 if index < 21 else 12)
        for index, session_date in enumerate(_dates(22))
    )
    selected = select_completed_session_dates(
        observations,
        expected_count=100,
        minimum_anchor_coverage=Decimal("0.8"),
        required_sessions=21,
    )
    assert selected == _dates(21)


def test_session_anchor_keeps_a_sparse_middle_reference_date() -> None:
    observations = tuple(
        ReferenceSessionCoverage(session_date, 10 if index == 8 else 90)
        for index, session_date in enumerate(_dates(21))
    )
    selected = select_completed_session_dates(
        observations,
        expected_count=100,
        minimum_anchor_coverage=Decimal("0.8"),
        required_sessions=21,
    )
    assert selected == _dates(21)
    assert selected[8] == observations[8].session_date


def test_session_anchor_refuses_without_coverage_or_history() -> None:
    with pytest.raises(ValueError, match="no reference session"):
        select_completed_session_dates(
            tuple(ReferenceSessionCoverage(day, 7) for day in _dates(21)),
            expected_count=10,
            minimum_anchor_coverage=Decimal("0.8"),
            required_sessions=21,
        )
    with pytest.raises(ValueError, match="fewer than 22"):
        select_completed_session_dates(
            tuple(ReferenceSessionCoverage(day, 10) for day in _dates(21)),
            expected_count=10,
            minimum_anchor_coverage=Decimal("0.8"),
            required_sessions=22,
        )


@pytest.mark.parametrize("threshold", [Decimal(0), Decimal("1.01")])
def test_invalid_coverage_threshold_is_rejected(threshold: Decimal) -> None:
    with pytest.raises(ValueError, match="inside"):
        measure_completed_session_regime(
            _panel(_member(1, 10, _rising(100))),
            minimum_coverage=threshold,
            minimum_sector_members=2,
        )
