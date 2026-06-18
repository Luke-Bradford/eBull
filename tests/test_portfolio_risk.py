"""Pure tests for position-vs-portfolio risk math (#1636). No DB."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from app.services.portfolio_risk import (
    build_portfolio_returns,
    marginal_risk_contribution,
    relative_risk_metrics,
)

D = Decimal
_BASE = date(2026, 1, 1)


def _d(n: int) -> date:
    return _BASE + timedelta(days=n)


class TestBuildPortfolioReturns:
    def test_market_value_weighted_sum_on_common_dates(self) -> None:
        # 75/25 value split → r_p = 0.75*a + 0.25*b on shared dates.
        a = {_d(1): D("0.10"), _d(2): D("0.20")}
        b = {_d(1): D("0.00"), _d(2): D("0.40")}
        out = dict(build_portfolio_returns([(D("75"), a), (D("25"), b)]))
        assert out[_d(1)] == D("0.075")
        assert out[_d(2)] == D("0.25")  # 0.75*0.2 + 0.25*0.4

    def test_intersection_only(self) -> None:
        # b is missing d2 → portfolio series has only the common date d1.
        a = {_d(1): D("0.1"), _d(2): D("0.2")}
        b = {_d(1): D("0.1")}
        out = build_portfolio_returns([(D("1"), a), (D("1"), b)])
        assert [d for d, _ in out] == [_d(1)]

    def test_empty_and_zero_weight(self) -> None:
        assert build_portfolio_returns([]) == []
        assert build_portfolio_returns([(D("0"), {_d(1): D("0.1")})]) == []

    def test_disjoint_histories_yield_empty(self) -> None:
        a = {_d(1): D("0.1")}
        b = {_d(2): D("0.1")}
        assert build_portfolio_returns([(D("1"), a), (D("1"), b)]) == []

    def test_holding_with_no_returns_forces_empty(self) -> None:
        # A holding with an empty return map empties the common intersection —
        # the book can't be fully constructed (→ book_history_unavailable), NOT
        # a renormalized subset of the holdings that do have history (Codex P1).
        a = {_d(1): D("0.1"), _d(2): D("0.2")}
        assert build_portfolio_returns([(D("90"), a), (D("10"), {})]) == []


class TestRelativeRiskMetrics:
    def test_perfect_positive_gives_beta_and_corr(self) -> None:
        # candidate == 2× portfolio → β=2, corr=+1.
        p = [(_d(i), D(str(0.01 * (i % 5 - 2)))) for i in range(1, 40)]
        c = [(d, r * 2) for d, r in p]
        m = relative_risk_metrics(c, p)
        assert m.n_obs == len(p)
        assert m.beta is not None and abs(m.beta - D("2")) < D("0.0001")
        assert m.correlation is not None and abs(m.correlation - D("1")) < D("0.0001")
        assert m.last_date == p[-1][0]

    def test_negative_relationship_gives_negative_corr(self) -> None:
        p = [(_d(i), D(str(0.01 * (i % 5 - 2)))) for i in range(1, 40)]
        c = [(d, -r) for d, r in p]
        m = relative_risk_metrics(c, p)
        assert m.beta is not None and m.beta < 0
        assert m.correlation is not None and m.correlation < 0

    def test_window_is_the_intersection_not_full_series(self) -> None:
        # portfolio has an extra early date the candidate lacks; σ_p must be
        # computed over the SHARED window only (Codex ckpt-1 consistency).
        p = [(_d(i), D("0.01")) for i in range(1, 10)]
        c = [(_d(i), D("0.02")) for i in range(4, 10)]  # starts at d4
        m = relative_risk_metrics(c, p)
        assert m.n_obs == 6  # d4..d9, not 9
        assert m.last_date == _d(9)

    def test_under_two_obs_is_none(self) -> None:
        m = relative_risk_metrics([(_d(1), D("0.1"))], [(_d(1), D("0.1"))])
        assert m.beta is None and m.correlation is None
        assert m.candidate_vol is None and m.portfolio_vol is None
        assert m.n_obs == 1


class TestMarginalRiskContribution:
    def test_beta_times_portfolio_vol(self) -> None:
        assert marginal_risk_contribution(D("1.5"), D("0.20")) == D("0.30")

    def test_none_inputs_stay_none(self) -> None:
        assert marginal_risk_contribution(None, D("0.2")) is None
        assert marginal_risk_contribution(D("1.5"), None) is None
