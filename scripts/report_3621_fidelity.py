"""#3621 slice 3: MAX fidelity, our ``rmax1_21d`` long-short against JKP's published factor.

Spec: ``docs/research/2026-10-08-3621-avoidance-filters.md`` §"Decision rule", "MAX fidelity" (PR #3724). Stage A,
holding months 2014-10..2021-05 (JKP's series ends at 2021-05).

* **Construction:** step 1's :func:`~app.services.factor_panel_fidelity.factor_month` (JKP terciles on non-micro
  names, value weights capped at the NYSE p80, sign x (high - low)) with JKP Table 9's sign for ``rmax1_21d``. Only
  names with a numeric slice 1 value enter; screened, short and zero-heavy names are left out, never ranked.
* **Comparison:** :func:`~app.services.factor_panel_fidelity.compare_arm` and ``characteristic_verdict``, with step
  1's price-characteristic correlation bar passed explicitly; its beta, offset, lead-lag, ``MIN_LEG``,
  ``MIN_PAIRS`` and ``MAX_UNDERSIZED`` rules apply unchanged.
* **Registry:** step 1's ``CHARACTERISTICS``, ``CORRELATION_BAR`` and ``FIDELITY_SEARCHES`` are not changed, and
  ``factor_panel_fidelity.py`` is not edited; this configuration lives here.
* **Scope:** the check shows the factor-return series tracks JKP's. It shows nothing about name-level ranking
  agreement, the top-decile threshold or micro caps.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from app.services.avoidance_filters import MaxReading
from app.services.factor_panel_fidelity import (
    ARMS,
    CORRELATION_BAR,
    PRICE_CHARACTERISTICS,
    ArmResult,
    FactorMonth,
    Holding,
    SeriesAccumulator,
    Verdict,
    characteristic_verdict,
    compare_arm,
    factor_month,
    month_key,
    shift_month,
)
from scripts.report_3609_step2 import PanelName

MAX_CHARACTERISTIC: Final = "rmax1_21d"
#: Step 1's price-characteristic correlation bar (one value for both of its price characteristics).
MAX_CORRELATION_BAR: Final = CORRELATION_BAR[PRICE_CHARACTERISTICS[0]]
assert all(CORRELATION_BAR[c] == MAX_CORRELATION_BAR for c in PRICE_CHARACTERISTICS)


def max_holdings(readings: Mapping[int, MaxReading], admitted: Mapping[int, PanelName]) -> list[Holding]:
    """Step 1's holdings for one formation: every admitted name with a numeric ``rmax1_21d`` value."""
    missing = sorted(admitted.keys() - readings.keys())
    if missing:
        raise ValueError(f"{len(missing)} admitted names have no MAX reading: {missing[:5]}")
    return [
        Holding(name, value, panel.me, str(panel.holding.status), {str(a): r for a, r in panel.holding.by_arm.items()})
        for name, panel in sorted(admitted.items())
        if (value := readings[name].value) is not None
    ]


@dataclass(frozen=True)
class FidelityFormation:
    formation: date
    holdings: Sequence[Holding]
    #: JKP's NYSE p20 (the micro cutoff) and p80 (the weight cap) at M, in USD.
    micro_cutoff_usd: float
    cap_usd: float


@dataclass(frozen=True)
class MaxFidelity:
    verdict: Verdict
    arms: Mapping[str, ArmResult]
    undersized: int

    @property
    def passed(self) -> bool:
        return self.verdict is Verdict.PASS


def max_fidelity(formations: Sequence[FidelityFormation], published: Mapping[str, float], sign: int) -> MaxFidelity:
    """The fidelity verdict over the formations' holding months; ``published`` is JKP's series by ``YYYY-MM``."""
    grid = [shift_month(month_key(f.formation), 1) for f in formations]
    accumulator = SeriesAccumulator(grid)
    for f, month in zip(formations, grid, strict=True):
        got = (
            factor_month(f.holdings, f.micro_cutoff_usd, f.cap_usd, sign)
            if f.holdings
            else FactorMonth(0, 0, None, None)
        )
        accumulator.add(month, got)
    # As ``SeriesAccumulator.result``: a grid month never added is undersized too; one leg count is kept per add.
    undersized = accumulator.undersized + len(grid) - len(accumulator.leg_counts["low"])
    arms = {arm: compare_arm(accumulator.ours[arm], published, grid, undersized, MAX_CORRELATION_BAR) for arm in ARMS}
    return MaxFidelity(characteristic_verdict(arms), arms, undersized)
