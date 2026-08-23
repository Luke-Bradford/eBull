"""The two zero-turnover R6 operational rules from #2914.

Neither object authorises a trade.  The calendar helper exposes a preference
window over sessions supplied by the caller's venue calendar.  The valuation
record preserves context on an already-declared arm and refuses factor returns
masquerading as a valuation spread.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from datetime import date
from decimal import Decimal
from typing import Final, Literal

TURN_OF_MONTH_RULE_VERSION: Final = "r6-2914-turn-of-month-preference-v1"
FACTOR_VALUATION_RULE_VERSION: Final = "r6-2914-factor-valuation-record-v1"
TURN_OF_MONTH_OFFSETS: Final = (-3, -2, -1, 0, 1, 2, 3)
REFERENCE_RETURN_UNITS: Final = frozenset({"decimal_return", "percent_per_annum", "binary_indicator"})

FactorValuationStatus = Literal["recorded", "unavailable"]
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def turn_of_month_preference_window(
    sessions: tuple[date, ...],
    *,
    target_year: int,
    target_month: int,
) -> tuple[date, ...]:
    """Return session offsets -3..+3 around the target month's last session.

    ``sessions`` belongs to the instrument's actual venue and must already be a
    complete ordered calendar.  This function deliberately has no order or
    position input, so its output cannot create execution authority or turnover.
    """
    if not 1 <= target_month <= 12:
        raise ValueError("target_month must be in 1..12")
    if len(sessions) < len(TURN_OF_MONTH_OFFSETS):
        raise ValueError("session calendar cannot provide the complete -3..+3 window")
    if any(left >= right for left, right in zip(sessions, sessions[1:])):
        raise ValueError("session calendar must be strictly increasing and duplicate-free")

    target_indices = [
        index for index, session in enumerate(sessions) if session.year == target_year and session.month == target_month
    ]
    if not target_indices:
        raise ValueError("session calendar contains no target-month anchor")
    anchor_index = target_indices[-1]
    start = anchor_index - 3
    stop = anchor_index + 4
    if start < 0 or stop > len(sessions):
        raise ValueError("session calendar cannot provide the complete -3..+3 window")
    window = sessions[start:stop]
    if len(window) != len(TURN_OF_MONTH_OFFSETS) or window[3] != sessions[anchor_index]:
        raise RuntimeError("turn-of-month window construction violated its fixed offsets")
    return window


@dataclass(frozen=True)
class FactorValuationRecord:
    """Declaration-time factor valuation context; never a launch decision."""

    factor_id: str
    status: FactorValuationStatus
    reason: str
    spread_measure: str | None = None
    spread_value: Decimal | None = None
    spread_unit: str | None = None
    observation_date: date | None = None
    history_start: date | None = None
    history_end: date | None = None
    historical_percentile: Decimal | None = None
    source: str | None = None
    dataset_key: str | None = None
    series_key: str | None = None
    source_snapshot_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.factor_id.strip() or not self.reason.strip():
            raise ValueError("factor_id and reason must be non-empty")
        value_fields = tuple(
            field.name for field in fields(self) if field.name not in {"factor_id", "status", "reason"}
        )
        if self.status == "unavailable":
            populated = [name for name in value_fields if getattr(self, name) is not None]
            if populated:
                raise ValueError(f"unavailable valuation record cannot carry values: {', '.join(populated)}")
            return
        if self.status != "recorded":
            raise ValueError(f"unknown factor valuation status: {self.status!r}")

        missing = [name for name in value_fields if getattr(self, name) is None]
        if missing:
            raise ValueError(f"recorded valuation record is missing: {', '.join(missing)}")
        assert self.spread_value is not None
        assert self.spread_unit is not None
        assert self.observation_date is not None
        assert self.history_start is not None
        assert self.history_end is not None
        assert self.historical_percentile is not None
        assert self.source_snapshot_sha256 is not None
        if not self.spread_value.is_finite():
            raise ValueError("spread_value must be finite")
        canonical_unit = self.spread_unit.strip()
        if not canonical_unit or canonical_unit in REFERENCE_RETURN_UNITS:
            raise ValueError("factor return/context units cannot be recorded as a valuation spread")
        if self.spread_unit != canonical_unit:
            raise ValueError("spread_unit must not contain surrounding whitespace")
        if not Decimal(0) <= self.historical_percentile <= Decimal(1):
            raise ValueError("historical_percentile must be in [0, 1]")
        if not self.history_start <= self.history_end <= self.observation_date:
            raise ValueError("valuation history must end no later than the observation date")
        if not _SHA256.fullmatch(self.source_snapshot_sha256):
            raise ValueError("source_snapshot_sha256 must be lowercase SHA-256")
        for field_name in ("spread_measure", "source", "dataset_key", "series_key"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be non-empty")


__all__ = [
    "FACTOR_VALUATION_RULE_VERSION",
    "REFERENCE_RETURN_UNITS",
    "TURN_OF_MONTH_OFFSETS",
    "TURN_OF_MONTH_RULE_VERSION",
    "FactorValuationRecord",
    "turn_of_month_preference_window",
]
