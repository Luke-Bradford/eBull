"""#3471 spec v6 §16.3 — a structure plan's derived figures and its level checks (pure).

The model (or the §16.5 library rule) names an invalidation level id and a target level id;
everything numeric is derived here from the pack's own ``levels`` and §6 ATR measurement.
Every value is an exact rational (a stored float enters as ``Decimal(repr(x))``), and a
recorded quotient is quantized with ``qs`` — §6's ``q`` on the magnitude, sign kept — because a
refused plan can carry a negative figure (r2-11).

This module owns orders 8–12 of the §16.3 per-decision precedence. Orders 1–7 and 13 need the
response, the shortlist or the library and live with the validator (slice v6-3). The §16.5
library script plans with ``library_plan`` so that the base rates are measured on the same
derivation the guard applies.

Provenance of every constant is the spec's §16.1 "Every other v6 constant" table.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Final, Literal

from app.services.ai_trial_decision import (
    STOP_ATR_MULTIPLE_MAX,
    STOP_PCT_MAX,
    STOP_PCT_MIN,
    TARGET_PCT_MAX,
    TARGET_PCT_MIN,
    AtrMeasurement,
    exact,
    quantize,
)
from app.services.ai_trial_levels import SUPPORT_LEVEL_IDS, TARGET_LEVEL_IDS, Level

#: Stop offset beyond the invalidation level, in ATR14 (supervisor 10:40Z; by construction).
STOP_OFFSET_ATR: Final = Fraction(1, 4)
#: Stop floor in ATR14 per horizon: the random-entry MAE p50 rounded UP to 0.5, as defined by
#: ``scripts/ai_trial_exit_base_rates.py`` (its checked-in output is test-pinned to these).
HORIZON_STOP_FLOOR_ATR: Final[Mapping[int, Fraction]] = {5: Fraction(1), 10: Fraction(3, 2), 20: Fraction(2)}
#: R floor (supervisor 10:40Z, up from v5's 1.5).
PLAN_R_MIN: Final = Fraction(2)

PlanRefusal = Literal[
    "level_unavailable",
    "stop_below_horizon_floor",
    "stop_outside_atr_band",
    "reward_risk_below_min",
    "plan_outside_bounds",
]


def qs(value: Fraction) -> Decimal:
    """§16.2 ``qs(x) = sign(x) × q(|x|)``: §6's half-up 4-decimal quantum on the magnitude."""
    magnitude = quantize(abs(value))
    return -magnitude if value < 0 and magnitude != 0 else magnitude


@dataclass(frozen=True)
class PlanFigures:
    """The §16.3 recorded figures. Each is ``None`` when an input is missing or its
    denominator is ≤ 0; ``stop_price`` is exact and never quantized."""

    stop_price: Fraction | None
    stop_atr_multiple: Decimal | None
    stop_pct: Decimal | None
    target_pct: Decimal | None
    r_multiple: Decimal | None


@dataclass(frozen=True)
class PlanVerdict:
    figures: PlanFigures
    refusal: PlanRefusal | None

    @property
    def feasible(self) -> bool:
        return self.refusal is None


def _valid(atr: AtrMeasurement | None) -> bool:
    """§6 validity, re-checked here so a hand-built measurement cannot divide by zero."""
    return atr is not None and atr.atr14 > 0 and atr.close > 0 and atr.atr14_pct > 0


def plan_figures(atr: AtrMeasurement | None, invalidation: Level | None, target: Level | None) -> PlanFigures:
    """§16.3 derivation, computed whatever check later fails (§6: figures are recorded
    independently of the refusal). An invalid measurement gives all-``None`` figures."""
    if atr is None or not _valid(atr):
        return PlanFigures(None, None, None, None, None)
    atr14, close = exact(atr.atr14), exact(atr.close)
    stop_price = None if invalidation is None else invalidation.price - STOP_OFFSET_ATR * atr14
    risk = None if stop_price is None else close - stop_price
    reward = None if target is None else target.price - close
    return PlanFigures(
        stop_price=stop_price,
        stop_atr_multiple=None if risk is None else qs(risk / atr14),
        stop_pct=None if risk is None else qs(100 * risk / close),
        target_pct=None if reward is None else qs(100 * reward / close),
        r_multiple=None if risk is None or reward is None or risk <= 0 else qs(reward / risk),
    )


def derive_plan(
    atr: AtrMeasurement | None, invalidation: Level | None, target: Level | None, *, horizon_days: int
) -> PlanVerdict:
    """Orders 8–12 of the §16.3 precedence, first failure named. Never clamps or repairs."""
    floor = HORIZON_STOP_FLOOR_ATR[horizon_days]
    figures = plan_figures(atr, invalidation, target)
    if (
        atr is None
        or not _valid(atr)
        or invalidation is None
        or target is None
        or figures.stop_price is None
        or figures.stop_price <= 0
        or invalidation.price >= exact(atr.close)
        or target.price <= exact(atr.close)
    ):
        return PlanVerdict(figures, "level_unavailable")
    # Order 8 passed, so risk and reward are > 0 and every figure is defined; an explicit
    # check rather than an ``assert`` (stripped under ``python -O``) keeps that fail-closed.
    stop_atr, stop_pct, target_pct, r_multiple = (
        figures.stop_atr_multiple,
        figures.stop_pct,
        figures.target_pct,
        figures.r_multiple,
    )
    if stop_atr is None or stop_pct is None or target_pct is None or r_multiple is None:
        return PlanVerdict(figures, "level_unavailable")
    if Fraction(stop_atr) < floor:
        return PlanVerdict(figures, "stop_below_horizon_floor")
    if Fraction(stop_atr) > STOP_ATR_MULTIPLE_MAX:
        return PlanVerdict(figures, "stop_outside_atr_band")
    # r2-14: R on prices AND the exact ratio of the quantized percentages execution uses.
    if Fraction(r_multiple) < PLAN_R_MIN or stop_pct <= 0 or Fraction(target_pct) / Fraction(stop_pct) < PLAN_R_MIN:
        return PlanVerdict(figures, "reward_risk_below_min")
    if not (exact(STOP_PCT_MIN) <= Fraction(stop_pct) <= exact(STOP_PCT_MAX)) or not (
        exact(TARGET_PCT_MIN) <= Fraction(target_pct) <= exact(TARGET_PCT_MAX)
    ):
        return PlanVerdict(figures, "plan_outside_bounds")
    return PlanVerdict(figures, None)


@dataclass(frozen=True)
class LibraryPlan:
    invalidation_level_id: str
    target_level_id: str
    figures: PlanFigures


def library_plan(
    levels: Mapping[str, Level | None], atr: AtrMeasurement | None, *, horizon_days: int
) -> LibraryPlan | None:
    """§16.5's deterministic plan: the highest-priced support id below close that passes
    orders 8–12 with some target, then the lowest-priced target id above close that does.
    Price ties break by the §16.3 enum order. ``None`` is the library's ``no_valid_plan``."""
    if atr is None:
        return None
    close = exact(atr.close)
    supports = sorted(
        ((lvl.price, rank, lid) for rank, lid in enumerate(SUPPORT_LEVEL_IDS) if (lvl := levels[lid]) is not None),
        key=lambda s: (-s[0], s[1]),
    )
    targets = sorted(
        ((lvl.price, rank, lid) for rank, lid in enumerate(TARGET_LEVEL_IDS) if (lvl := levels[lid]) is not None),
        key=lambda s: (s[0], s[1]),
    )
    for support_price, _, support_id in supports:
        if support_price >= close:
            continue
        for target_price, _, target_id in targets:
            if target_price <= close:
                continue
            verdict = derive_plan(atr, levels[support_id], levels[target_id], horizon_days=horizon_days)
            if verdict.feasible:
                return LibraryPlan(support_id, target_id, verdict.figures)
    return None


__all__ = [
    "HORIZON_STOP_FLOOR_ATR",
    "PLAN_R_MIN",
    "STOP_OFFSET_ATR",
    "LibraryPlan",
    "PlanFigures",
    "PlanRefusal",
    "PlanVerdict",
    "derive_plan",
    "library_plan",
    "plan_figures",
    "qs",
]
