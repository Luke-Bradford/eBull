"""Fail-closed breadth from an ephemeral broad-market snapshot (#2523).

The provider catalogue is never a quote or a complete security master.  This
module measures it only against a caller-supplied, predeclared point-in-time
cohort and exposes the missing denominator.  It does not persist routine scan
rows; a candidate may copy the resulting compact aggregate into its immutable
decision context.
"""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Literal

from app.providers.market_data import BroadMarketSnapshot

BreadthVerdict = Literal["usable", "refused"]


@dataclass(frozen=True)
class BreadthDefinition:
    provider_change_unit: str = "percentage_points"
    sign_rule: str = "positive_advance_negative_decline_exact_zero_unchanged"
    share_denominator: str = "known_change_rows"
    coverage_denominator: str = "caller_supplied_point_in_time_cohort"
    impossible_lower_change_pct: str = "-100"
    missing_semantics: str = "excluded_from_breadth_share_but_included_in_coverage_denominator"


DEFINITION: Final = BreadthDefinition()


@dataclass(frozen=True)
class SnapshotBreadth:
    version: str
    verdict: BreadthVerdict
    refusal_reason: str | None
    expected_count: int
    observed_count: int
    coverage: Decimal
    advance_count: int
    decline_count: int
    unchanged_count: int
    advance_share: Decimal | None
    decline_share: Decimal | None
    unchanged_share: Decimal | None


def measure_daily_breadth(
    snapshot: BroadMarketSnapshot,
    *,
    expected_instrument_ids: tuple[int, ...],
    minimum_coverage: Decimal,
) -> SnapshotBreadth:
    """Measure daily direction only when a frozen cohort is sufficiently seen.

    ``minimum_coverage`` is deliberately supplied by the candidate contract;
    this shared function does not choose a threshold after seeing outcomes.
    Values below -100% are impossible one-session returns and count as missing.
    Positive extremes are not winsorised: changing them cannot affect the sign
    statistic, and an arbitrary outcome-dependent trim would add a hidden rule.
    """
    if not Decimal("0") < minimum_coverage <= Decimal("1"):
        raise ValueError("minimum_coverage must be inside (0, 1]")
    if not expected_instrument_ids:
        raise ValueError("expected_instrument_ids must not be empty")
    if any(instrument_id <= 0 for instrument_id in expected_instrument_ids):
        raise ValueError("expected instrument IDs must be positive")
    expected = set(expected_instrument_ids)
    if len(expected) != len(expected_instrument_ids):
        raise ValueError("expected instrument IDs must be unique")

    changes_by_id: dict[int, Decimal] = {}
    seen_ids: set[int] = set()
    for row in snapshot.instruments:
        if row.instrument_id not in expected:
            continue
        if row.instrument_id in seen_ids:
            raise ValueError(f"snapshot contains duplicate instrument ID {row.instrument_id}")
        seen_ids.add(row.instrument_id)
        if row.daily_price_change_pct is not None and row.daily_price_change_pct >= Decimal(
            DEFINITION.impossible_lower_change_pct
        ):
            changes_by_id[row.instrument_id] = row.daily_price_change_pct
    changes = list(changes_by_id.values())
    observed = len(changes)
    coverage = Decimal(observed) / Decimal(len(expected))
    advances = sum(value > 0 for value in changes)
    declines = sum(value < 0 for value in changes)
    unchanged = observed - advances - declines
    denominator = Decimal(observed) if observed else None
    verdict: BreadthVerdict = "usable" if coverage >= minimum_coverage else "refused"
    refusal_reason = (
        None if verdict == "usable" else f"coverage:{observed}/{len(expected)}<{minimum_coverage.normalize()}"
    )
    return SnapshotBreadth(
        version=BREADTH_VERSION,
        verdict=verdict,
        refusal_reason=refusal_reason,
        expected_count=len(expected),
        observed_count=observed,
        coverage=coverage,
        advance_count=advances,
        decline_count=declines,
        unchanged_count=unchanged,
        advance_share=None if denominator is None else Decimal(advances) / denominator,
        decline_share=None if denominator is None else Decimal(declines) / denominator,
        unchanged_share=None if denominator is None else Decimal(unchanged) / denominator,
    )


def _version() -> str:
    payload = repr(DEFINITION) + inspect.getsource(measure_daily_breadth)
    return "snapshot-breadth-v1:" + hashlib.sha256(payload.encode()).hexdigest()[:16]


BREADTH_VERSION: Final = _version()


__all__ = ["BREADTH_VERSION", "SnapshotBreadth", "measure_daily_breadth"]
