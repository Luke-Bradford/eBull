"""#3621 slice 3: MAX fidelity through step 1's construction and comparison, on synthetic inputs only."""

from __future__ import annotations

import random
from datetime import date

import pytest

from app.services.avoidance_filters import MaxMissing, MaxReading
from app.services.factor_book_path import HoldingReturn
from app.services.factor_panel import add_months
from app.services.factor_panel_fidelity import Bar, Holding, Verdict, factor_month, month_key, shift_month
from app.services.factor_panel_prices import HoldingStatus
from app.services.factor_panel_reference import load_table9_signs
from scripts.report_3609_step2 import PanelName
from scripts.report_3621_fidelity import (
    MAX_CHARACTERISTIC,
    MAX_CORRELATION_BAR,
    FidelityFormation,
    max_fidelity,
    max_holdings,
)

P20, P80 = 100.0, 1000.0
FORMATIONS = [add_months(date(2014, 9, 30), i) for i in range(80)]  # holding months 2014-10..2021-05


def test_configuration_is_step_1s_price_bar_and_table_9s_sign() -> None:
    assert MAX_CORRELATION_BAR == 0.90
    assert load_table9_signs()[MAX_CHARACTERISTIC] == -1


def _panel(me: float, status: HoldingStatus = HoldingStatus.OBSERVED) -> PanelName:
    return PanelName(1, me, "OTHER", {}, HoldingReturn(status, {"best_case": 0.01, "worst_case": -0.02}), None)


def test_holdings_take_numeric_values_only_with_the_panel_me_status_and_arms() -> None:
    readings = {
        1: MaxReading(0.05, 20, 0, None),
        2: MaxReading(None, 20, 0, MaxMissing.SCREENED),
        3: MaxReading(None, 3, 0, MaxMissing.SHORT),
        4: MaxReading(None, 20, 12, MaxMissing.ZERO_HEAVY),
    }
    admitted = {n: _panel(500.0, HoldingStatus.TERMINAL if n == 1 else HoldingStatus.OBSERVED) for n in readings}
    got = max_holdings(readings, admitted)
    assert got == [Holding(1, 0.05, 500.0, "terminal", {"best_case": 0.01, "worst_case": -0.02})]
    with pytest.raises(ValueError, match="no MAX reading"):
        max_holdings({}, admitted)


def _formations(rng: random.Random, *, empty: frozenset[int] = frozenset()) -> list[FidelityFormation]:
    out = []
    for i, formation in enumerate(FORMATIONS):
        holdings = []
        if i not in empty:
            for n in range(40):
                r = rng.gauss(0.0, 0.05)
                holdings.append(
                    Holding(
                        n,
                        rng.random(),
                        rng.uniform(150.0, 3000.0),
                        "observed",
                        {"best_case": r, "worst_case": r - 0.001},
                    )
                )
        out.append(FidelityFormation(formation, holdings, P20, P80))
    return out


def _ours(formations: list[FidelityFormation]) -> dict[str, float]:
    """Our best-case factor series by holding month, computed independently of ``max_fidelity``."""
    out = {}
    for f in formations:
        if f.holdings:
            got = factor_month(f.holdings, P20, P80, -1).returns
            assert got is not None
            out[shift_month(month_key(f.formation), 1)] = got["best_case"]
    return out


def test_a_published_series_equal_to_ours_passes_and_noise_fails_correlation() -> None:
    formations = _formations(random.Random(3621))
    ours = _ours(formations)
    assert min(ours) == "2014-10" and max(ours) == "2021-05" and len(ours) == 80
    got = max_fidelity(formations, ours, -1)
    assert got.passed and got.undersized == 0
    noise = random.Random(1)
    failed = max_fidelity(formations, {m: noise.gauss(0, 0.05) for m in ours}, -1)
    assert failed.verdict is Verdict.FAIL
    assert Bar.CORRELATION in failed.arms["best_case"].failed_bars


def test_empty_formations_count_as_undersized_and_short_coverage_is_insufficient() -> None:
    formations = _formations(random.Random(7), empty=frozenset({3, 4, 5}))
    ours = _ours(formations)
    got = max_fidelity(formations, ours, -1)
    assert got.undersized == 3
    assert got.verdict is Verdict.FAIL and Bar.UNDERSIZED in got.arms["worst_case"].failed_bars
    short = max_fidelity(_formations(random.Random(7)), dict(list(ours.items())[:50]), -1)
    assert short.verdict is Verdict.INSUFFICIENT and not short.passed
