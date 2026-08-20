"""Pure, compact completed-session market/sector context for #2523.

These aggregates describe the environment in which a separately declared
candidate fired.  They are not entry signals and this module does not choose a
coverage threshold, universe, horizon, sector mapping, or trend rule after
seeing outcomes.

The caller supplies an aligned point-in-time cohort.  ``None`` is missing; it
never becomes a zero return.  Only the aggregate result belongs in an immutable
decision context.  The member panel is bounded source data and is not persisted
as another indicator history.
"""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from typing import Final, Literal

RegimeVerdict = Literal["usable", "refused"]
PriceBasis = Literal["quarantine_joinable_vendor_close"]


@dataclass(frozen=True)
class RegimeDefinition:
    horizons: tuple[int, ...] = (1, 3, 5, 10, 20)
    return_formula: str = "simple_close_to_close:close_t/close_t_minus_h-1"
    market_weighting: str = "equal_weight_valid_point_in_time_members"
    sector_weighting: str = "equal_weight_valid_point_in_time_members"
    dispersion_formula: str = "sample_standard_deviation_of_constituent_simple_returns"
    trend_lookback_sessions: int = 20
    trend_formula: str = "close_t>arithmetic_mean(close_t_minus_20...close_t_minus_1)"
    common_movement_lookback_sessions: int = 20
    common_movement_formula: str = "variance(equal_weight_daily_return)/mean(constituent_daily_return_variance)"
    missing_semantics: str = "excluded_from_measure_included_in_declared_cohort_coverage"
    price_basis: str = "quarantine_joinable_vendor_close"
    unit_regime_rule: str = "every_transition_inside_return_or_trend_window_must_be_joinable"
    classification_semantics: str = "caller_supplied_point_in_time_provider_industry"


DEFINITION: Final = RegimeDefinition()
# Decimal arithmetic can leave an economically meaningless excursion this far
# outside [0, 1] after ratios of sample variances. Anything larger is a real
# contract violation and is refused rather than clipped.
_VARIANCE_SHARE_ROUNDING_EPSILON: Final = Decimal("1e-28")


@dataclass(frozen=True)
class RegimeMember:
    instrument_id: int
    provider_industry_id: int
    # Aligned one-for-one with CompletedSessionPanel.session_dates.
    closes: tuple[Decimal | None, ...]
    # At index j: whether close[j-1] and close[j] share a resolved unit regime.
    # Index zero has no predecessor and must be False.
    return_links: tuple[bool, ...]


@dataclass(frozen=True)
class CompletedSessionPanel:
    session_dates: tuple[date, ...]
    members: tuple[RegimeMember, ...]
    cohort_version: str
    source_version: str
    price_basis: PriceBasis


@dataclass(frozen=True)
class ReferenceSessionCoverage:
    session_date: date
    observed_count: int


@dataclass(frozen=True)
class HorizonAggregate:
    horizon_sessions: int
    verdict: RegimeVerdict
    refusal_reason: str | None
    expected_count: int
    observed_count: int
    coverage: Decimal
    equal_weight_return: Decimal | None
    return_dispersion: Decimal | None
    advance_share: Decimal | None


@dataclass(frozen=True)
class CommonMovement:
    verdict: RegimeVerdict
    refusal_reason: str | None
    expected_count: int
    balanced_count: int
    coverage: Decimal
    variance_share: Decimal | None


@dataclass(frozen=True)
class ParticipationAggregate:
    verdict: RegimeVerdict
    refusal_reason: str | None
    expected_count: int
    observed_count: int
    coverage: Decimal
    share: Decimal | None


@dataclass(frozen=True)
class SectorRegime:
    provider_industry_id: int
    expected_count: int
    horizons: tuple[HorizonAggregate, ...]
    prior_trend: ParticipationAggregate
    common_movement: CommonMovement


@dataclass(frozen=True)
class CompletedSessionRegime:
    version: str
    cohort_version: str
    source_version: str
    latest_completed_session: date
    minimum_coverage: Decimal
    minimum_sector_members: int
    expected_count: int
    market_horizons: tuple[HorizonAggregate, ...]
    prior_trend: ParticipationAggregate
    common_movement: CommonMovement
    sectors: tuple[SectorRegime, ...]


def _mean(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _sample_variance(values: list[Decimal]) -> Decimal | None:
    if len(values) < 2:
        return None
    mean = _mean(values)
    return sum(((value - mean) ** 2 for value in values), Decimal(0)) / Decimal(len(values) - 1)


def _sample_stddev(values: list[Decimal]) -> Decimal | None:
    variance = _sample_variance(values)
    if variance is None:
        return None
    with localcontext() as context:
        context.prec = 34
        return variance.sqrt()


def _valid_close(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def _simple_return(member: RegimeMember, horizon: int) -> Decimal | None:
    current = member.closes[-1]
    prior = member.closes[-1 - horizon]
    if not _valid_close(current) or not _valid_close(prior) or not all(member.return_links[-horizon:]):
        return None
    assert current is not None and prior is not None
    return current / prior - 1


def _coverage(observed: int, expected: int) -> Decimal:
    return Decimal(observed) / Decimal(expected)


def select_completed_session_dates(
    observations: tuple[ReferenceSessionCoverage, ...],
    *,
    expected_count: int,
    minimum_anchor_coverage: Decimal,
    required_sessions: int,
) -> tuple[date, ...]:
    """Anchor on the latest broadly populated reference-market session.

    A provider can publish a reference instrument before the wider universe is
    complete. The latest date alone is therefore not a completed-session
    contract. Once the anchor is chosen, every prior reference date is retained
    even when its coverage is poor; skipping a sparse middle session would
    silently change every horizon instead of letting downstream coverage fail.
    """
    if expected_count <= 0:
        raise ValueError("expected_count must be positive")
    if not Decimal(0) < minimum_anchor_coverage <= Decimal(1):
        raise ValueError("minimum_anchor_coverage must be inside (0, 1]")
    if required_sessions <= 0:
        raise ValueError("required_sessions must be positive")
    dates = tuple(item.session_date for item in observations)
    if dates != tuple(sorted(dates)) or len(dates) != len(set(dates)):
        raise ValueError("reference session observations must be strictly increasing and unique")
    for item in observations:
        if not 0 <= item.observed_count <= expected_count:
            raise ValueError("reference session observed_count must be inside 0..expected_count")
    anchors = [
        index
        for index, item in enumerate(observations)
        if _coverage(item.observed_count, expected_count) >= minimum_anchor_coverage
    ]
    if not anchors:
        raise ValueError("no reference session reaches minimum anchor coverage")
    anchor_index = anchors[-1]
    first_index = anchor_index - required_sessions + 1
    if first_index < 0:
        raise ValueError(f"fewer than {required_sessions} reference sessions exist through the covered anchor")
    return dates[first_index : anchor_index + 1]


def _horizon_aggregate(
    members: tuple[RegimeMember, ...], *, horizon: int, minimum_coverage: Decimal
) -> HorizonAggregate:
    returns = [value for member in members if (value := _simple_return(member, horizon)) is not None]
    coverage = _coverage(len(returns), len(members))
    usable = coverage >= minimum_coverage and len(returns) >= 2
    reason = None
    if coverage < minimum_coverage:
        reason = f"coverage:{len(returns)}/{len(members)}<{minimum_coverage.normalize()}"
    elif len(returns) < 2:
        reason = "dispersion_requires_two_members"
    return HorizonAggregate(
        horizon_sessions=horizon,
        verdict="usable" if usable else "refused",
        refusal_reason=reason,
        expected_count=len(members),
        observed_count=len(returns),
        coverage=coverage,
        equal_weight_return=_mean(returns) if usable else None,
        return_dispersion=_sample_stddev(returns) if usable else None,
        advance_share=(Decimal(sum(value > 0 for value in returns)) / Decimal(len(returns))) if usable else None,
    )


def _trend_share(members: tuple[RegimeMember, ...], *, minimum_coverage: Decimal) -> ParticipationAggregate:
    outcomes: list[bool] = []
    lookback = DEFINITION.trend_lookback_sessions
    for member in members:
        current = member.closes[-1]
        history = member.closes[-1 - lookback : -1]
        if (
            not _valid_close(current)
            or len(history) != lookback
            or any(not _valid_close(value) for value in history)
            or not all(member.return_links[-lookback:])
        ):
            continue
        assert current is not None
        prior = [value for value in history if value is not None]
        outcomes.append(current > _mean(prior))
    coverage = _coverage(len(outcomes), len(members))
    if coverage < minimum_coverage:
        return ParticipationAggregate(
            verdict="refused",
            refusal_reason=f"coverage:{len(outcomes)}/{len(members)}<{minimum_coverage.normalize()}",
            expected_count=len(members),
            observed_count=len(outcomes),
            coverage=coverage,
            share=None,
        )
    return ParticipationAggregate(
        verdict="usable",
        refusal_reason=None,
        expected_count=len(members),
        observed_count=len(outcomes),
        coverage=coverage,
        share=Decimal(sum(outcomes)) / Decimal(len(outcomes)),
    )


def _common_movement(members: tuple[RegimeMember, ...], *, minimum_coverage: Decimal) -> CommonMovement:
    lookback = DEFINITION.common_movement_lookback_sessions
    balanced: list[list[Decimal]] = []
    for member in members:
        closes = member.closes[-1 - lookback :]
        if (
            len(closes) != lookback + 1
            or any(not _valid_close(value) for value in closes)
            or not all(member.return_links[-lookback:])
        ):
            continue
        concrete = [value for value in closes if value is not None]
        balanced.append([concrete[index] / concrete[index - 1] - 1 for index in range(1, len(concrete))])

    coverage = _coverage(len(balanced), len(members))
    reason: str | None = None
    if coverage < minimum_coverage:
        reason = f"coverage:{len(balanced)}/{len(members)}<{minimum_coverage.normalize()}"
    elif len(balanced) < 2:
        reason = "common_movement_requires_two_members"
    if reason is not None:
        return CommonMovement("refused", reason, len(members), len(balanced), coverage, None)

    market_returns = [_mean([returns[index] for returns in balanced]) for index in range(lookback)]
    market_variance = _sample_variance(market_returns)
    constituent_variances = [value for returns in balanced if (value := _sample_variance(returns)) is not None]
    denominator = _mean(constituent_variances) if constituent_variances else Decimal(0)
    if market_variance is None or denominator <= 0:
        return CommonMovement(
            "refused", "zero_or_undefined_constituent_variance", len(members), len(balanced), coverage, None
        )
    share = market_variance / denominator
    # Tiny Decimal rounding excursions are not economic information.
    if share < 0 and share > -_VARIANCE_SHARE_ROUNDING_EPSILON:
        share = Decimal(0)
    if share > 1 and share < 1 + _VARIANCE_SHARE_ROUNDING_EPSILON:
        share = Decimal(1)
    if not Decimal(0) <= share <= Decimal(1):
        return CommonMovement(
            "refused",
            "variance_share_outside_unit_interval",
            len(members),
            len(balanced),
            coverage,
            None,
        )
    return CommonMovement("usable", None, len(members), len(balanced), coverage, share)


def decompose_return(
    *, instrument_return: Decimal, market_return: Decimal, sector_return: Decimal
) -> tuple[Decimal, Decimal, Decimal]:
    """Return market, sector-relative, and instrument-residual components."""
    return market_return, sector_return - market_return, instrument_return - sector_return


def measure_completed_session_regime(
    panel: CompletedSessionPanel,
    *,
    minimum_coverage: Decimal,
    minimum_sector_members: int,
) -> CompletedSessionRegime:
    """Measure declared context through the panel's final completed session."""
    if not Decimal(0) < minimum_coverage <= Decimal(1):
        raise ValueError("minimum_coverage must be inside (0, 1]")
    if minimum_sector_members < 2:
        raise ValueError("minimum_sector_members must be at least two")
    if not panel.cohort_version or not panel.source_version:
        raise ValueError("cohort_version and source_version must be non-empty")
    if panel.price_basis != DEFINITION.price_basis:
        raise ValueError(f"unsupported price basis {panel.price_basis!r}")
    required_sessions = max(DEFINITION.horizons) + 1
    if len(panel.session_dates) < required_sessions:
        raise ValueError(f"at least {required_sessions} completed sessions are required")
    if tuple(sorted(panel.session_dates)) != panel.session_dates or len(set(panel.session_dates)) != len(
        panel.session_dates
    ):
        raise ValueError("session dates must be strictly increasing and unique")
    if not panel.members:
        raise ValueError("point-in-time cohort must not be empty")
    ids = [member.instrument_id for member in panel.members]
    if any(instrument_id <= 0 for instrument_id in ids) or len(ids) != len(set(ids)):
        raise ValueError("member instrument IDs must be unique and positive")
    for member in panel.members:
        if member.provider_industry_id <= 0:
            raise ValueError("provider industry IDs must be positive")
        if len(member.closes) != len(panel.session_dates):
            raise ValueError(f"instrument {member.instrument_id} is not aligned to session dates")
        if len(member.return_links) != len(panel.session_dates) or member.return_links[0]:
            raise ValueError(f"instrument {member.instrument_id} has invalid unit-regime link alignment")

    market_horizons = tuple(
        _horizon_aggregate(panel.members, horizon=horizon, minimum_coverage=minimum_coverage)
        for horizon in DEFINITION.horizons
    )
    prior_trend = _trend_share(panel.members, minimum_coverage=minimum_coverage)
    grouped: dict[int, list[RegimeMember]] = {}
    for member in panel.members:
        grouped.setdefault(member.provider_industry_id, []).append(member)
    sectors: list[SectorRegime] = []
    for industry_id, raw_members in sorted(grouped.items()):
        members = tuple(raw_members)
        sector_threshold = minimum_coverage if len(members) >= minimum_sector_members else Decimal(1)
        sector_horizons = tuple(
            _horizon_aggregate(members, horizon=horizon, minimum_coverage=sector_threshold)
            for horizon in DEFINITION.horizons
        )
        if len(members) < minimum_sector_members:
            sector_horizons = tuple(
                HorizonAggregate(
                    horizon_sessions=item.horizon_sessions,
                    verdict="refused",
                    refusal_reason=f"sector_members:{len(members)}<{minimum_sector_members}",
                    expected_count=item.expected_count,
                    observed_count=item.observed_count,
                    coverage=item.coverage,
                    equal_weight_return=None,
                    return_dispersion=None,
                    advance_share=None,
                )
                for item in sector_horizons
            )
        sector_trend = _trend_share(members, minimum_coverage=sector_threshold)
        common = _common_movement(members, minimum_coverage=sector_threshold)
        if len(members) < minimum_sector_members:
            sector_trend = ParticipationAggregate(
                "refused",
                f"sector_members:{len(members)}<{minimum_sector_members}",
                sector_trend.expected_count,
                sector_trend.observed_count,
                sector_trend.coverage,
                None,
            )
            common = CommonMovement(
                "refused",
                f"sector_members:{len(members)}<{minimum_sector_members}",
                common.expected_count,
                common.balanced_count,
                common.coverage,
                None,
            )
        sectors.append(
            SectorRegime(
                provider_industry_id=industry_id,
                expected_count=len(members),
                horizons=sector_horizons,
                prior_trend=sector_trend,
                common_movement=common,
            )
        )
    return CompletedSessionRegime(
        version=REGIME_VERSION,
        cohort_version=panel.cohort_version,
        source_version=panel.source_version,
        latest_completed_session=panel.session_dates[-1],
        minimum_coverage=minimum_coverage,
        minimum_sector_members=minimum_sector_members,
        expected_count=len(panel.members),
        market_horizons=market_horizons,
        prior_trend=prior_trend,
        common_movement=_common_movement(panel.members, minimum_coverage=minimum_coverage),
        sectors=tuple(sectors),
    )


def _version() -> str:
    semantic_functions = (
        _mean,
        _sample_variance,
        _sample_stddev,
        _valid_close,
        _simple_return,
        _coverage,
        select_completed_session_dates,
        _horizon_aggregate,
        _trend_share,
        _common_movement,
        decompose_return,
        measure_completed_session_regime,
    )
    payload = repr(DEFINITION) + "".join(inspect.getsource(function) for function in semantic_functions)
    return "completed-session-regime-v1:" + hashlib.sha256(payload.encode()).hexdigest()[:16]


REGIME_VERSION: Final = _version()


__all__ = [
    "CompletedSessionPanel",
    "CompletedSessionRegime",
    "HorizonAggregate",
    "ParticipationAggregate",
    "REGIME_VERSION",
    "ReferenceSessionCoverage",
    "RegimeMember",
    "decompose_return",
    "measure_completed_session_regime",
    "select_completed_session_dates",
]
