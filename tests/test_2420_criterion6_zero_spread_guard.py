"""#2420 — P11's zero-spread guard, driven through ``_criterion6`` itself.

⚠ THIS EXISTS BECAUSE THE SCRIPT'S OWN ACCEPTANCE RUN CANNOT REACH THE BRANCH.
``scripts/verify_2240_statistics.py --curve`` refuses on corpus state
(*"4546 instruments carry more than one research series"*) long before criterion
6 runs — verified identical on ``origin/main``, so it is pre-existing and
unrelated, and filed separately. A fix whose only evidence is a command that
exits 1 for another reason has no evidence at all.

The numpy behaviour these guards exist for is pinned in
``tests/test_backtest_run.py::test_a_constant_return_series_refuses_and_is_not_read_as_uncorrelated``;
this file covers the second copy of the guard rather than re-pinning it.
"""

from __future__ import annotations

from array import array
from datetime import date
from types import SimpleNamespace

import pytest

from app.services.deflated_sharpe import TradeMoments
from scripts.verify_2240_statistics import _criterion6, _Sleeve

_DATES = [date(2010, 1, 4), date(2010, 1, 5), date(2010, 1, 6)]


def _moments(sharpe: float) -> TradeMoments:
    return TradeMoments(sharpe=sharpe, skewness=0.0, kurtosis=3.0, trade_count=64)


def _sleeve(label: str, returns: list[float], *, sharpe: float) -> _Sleeve:
    """One trial with a declared per-entry-date return series.

    One trade per date, so ``daily_trade_returns``' mean is that date's value —
    the shape the correlation is measured on.
    """
    sleeve = _Sleeve(label)
    sleeve.returns = array("d", returns)
    sleeve.entry_dates = list(_DATES)
    sleeve.moments = _moments(sharpe)
    return sleeve


def _run(s1: list[float], s3: list[float], capsys: pytest.CaptureFixture[str]) -> tuple[list[str], str]:
    """Drive ``_criterion6`` on two synthetic trials and return (problems, output).

    ``measured`` is read for exactly one attribute — ``effective_sample_size`` —
    so a namespace carries it. Deliberately NOT asserting ``problems == []`` in
    here: a refusal path returns early and legitimately reports none, while the
    varying path runs the whole DSR contrast on invented moments where a P9
    finding would say nothing about this fix.
    """
    sleeves = {
        "S-1": _sleeve("S-1", s1, sharpe=0.05),
        "S-3": _sleeve("S-3", s3, sharpe=0.09),
    }
    measured = {label: SimpleNamespace(effective_sample_size=48.0) for label in sleeves}
    problems = _criterion6(sleeves, measured)  # type: ignore[arg-type]
    return problems, capsys.readouterr().out


def test_a_constant_trial_is_refused_by_name_rather_than_read_as_uncorrelated(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """⚠⚠ The defect: ``np.corrcoef`` gives a constant row a finite **0.0**.

    On numpy 2.4.4 the old ``isfinite`` guard never fired, so this trial reached
    ``average_trial_correlation`` as rho = 0 and ``implied_independent_trials``
    returned N_hat ≈ M — a number nobody measured, leaning safe, which is exactly
    why it survived unnoticed.

    Asserts the reason NAMES the trial, per the ticket's acceptance.
    """
    problems, out = _run([0.1, 0.1, 0.1], [0.1, -0.2, 0.3], capsys)
    # A refusal is a reported state, not a property violation.
    assert problems == []
    assert "refused" in out
    assert "constant return series" in out
    assert "'S-1'" in out
    # The old branch's wording must not be what fired.
    assert "correlation matrix is not finite" not in out
    # And it must refuse BEFORE reporting a correlation it cannot have measured.
    assert "avg trial correlation" not in out


def test_the_std_predicate_would_not_have_fired_on_this_series() -> None:
    """⚠ Why ``ptp`` and not ``std == 0.0`` — the first repair attempt's bug.

    The mean of three binary ``0.1``s is ``0.10000000000000002``, so the
    deviations are ~1e-17 and the standard deviation is a denormal rather than a
    zero. Pinned here because the two predicates look interchangeable and only
    one of them is exact.
    """
    import numpy as np

    row = np.array([0.1, 0.1, 0.1])
    assert float(np.std(row)) != 0.0
    assert float(np.ptp(row)) == 0.0


def test_both_trials_constant_names_both(capsys: pytest.CaptureFixture[str]) -> None:
    """The reason lists every degenerate trial, not the first one found.

    ⚠ Asserts the refusal and the absence of a correlation as well as the two
    names — checking names alone would pass on any output that happened to
    mention both trials, which several of this function's other prints do.
    """
    problems, out = _run([0.1, 0.1, 0.1], [0.2, 0.2, 0.2], capsys)
    assert problems == []
    assert "refused" in out
    assert "constant return series" in out
    assert "'S-1'" in out and "'S-3'" in out
    assert "avg trial correlation" not in out


def test_varying_trials_still_measure_a_correlation(capsys: pytest.CaptureFixture[str]) -> None:
    """The live path is unchanged — the guard must not swallow real series.

    S-1 and S-3 both vary on the real corpus, which is why this defect never
    changed a shipped number; a fix that also refused these would be worse than
    the bug.
    """
    _, out = _run([0.1, -0.2, 0.3], [0.2, -0.1, 0.4], capsys)
    assert "constant return series" not in out
    assert "avg trial correlation" in out
