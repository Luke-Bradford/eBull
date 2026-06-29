"""Pure-logic tests for the IAR evidence signals (#1823). No DB — every signal
is a pure function over fact dicts / populations."""

from __future__ import annotations

from app.services.instrument_analytics import (
    altman_z2,
    compute_peer_grades,
    hybrid_grade,
    insider_signal,
    inst_13f_signal,
    percentile_rank,
    piotroski_f,
    short_interest_signal,
)

# A clean two-FY pair that earns all 9 Piotroski points.
_CURR = {
    "NetIncomeLoss": 100.0,
    "Assets": 1000.0,
    "NetCashProvidedByUsedInOperatingActivities": 150.0,
    "LongTermDebt": 200.0,
    "AssetsCurrent": 500.0,
    "LiabilitiesCurrent": 250.0,
    "WeightedAverageNumberOfDilutedSharesOutstanding": 1000.0,
    "GrossProfit": 400.0,
    "Revenues": 1000.0,
    "Liabilities": 400.0,
    "RetainedEarningsAccumulatedDeficit": 300.0,
    "OperatingIncomeLoss": 120.0,
    "StockholdersEquity": 600.0,
}
_PRIOR = {
    "NetIncomeLoss": 80.0,
    "Assets": 1000.0,
    "LongTermDebt": 250.0,
    "AssetsCurrent": 400.0,
    "LiabilitiesCurrent": 250.0,
    "WeightedAverageNumberOfDilutedSharesOutstanding": 1000.0,
    "GrossProfit": 350.0,
    "Revenues": 900.0,
}


class TestPiotroski:
    def test_full_nine(self) -> None:
        r = piotroski_f(_CURR, _PRIOR)
        assert r.components_available == 9
        assert r.score == 9
        assert r.band == "strong"
        assert all(r.components.values())

    def test_no_prior_only_profitability(self) -> None:
        # Without a prior year only the 3 current-only profitability points
        # (roa/cfo/accrual) are evaluable — never imputed.
        r = piotroski_f(_CURR, None)
        assert r.components_available == 3
        assert set(r.components) == {"roa_positive", "cfo_positive", "accrual_cfo_gt_ni"}
        assert r.score == 3

    def test_weak_band(self) -> None:
        bad = {"NetIncomeLoss": -50.0, "Assets": 1000.0, "NetCashProvidedByUsedInOperatingActivities": -100.0}
        r = piotroski_f(bad, None)
        # roa<0, cfo<0, and cfo(-100) < ni(-50) -> all three profitability points fail.
        assert r.score == 0
        assert r.band == "weak"

    def test_revenue_fallback_chain(self) -> None:
        # No 'Revenues'/'GrossProfit' but ASC-606 revenue + CostOfRevenue present.
        curr = dict(_CURR)
        del curr["Revenues"], curr["GrossProfit"]
        curr["RevenueFromContractWithCustomerExcludingAssessedTax"] = 1000.0
        curr["CostOfRevenue"] = 600.0
        prior = dict(_PRIOR)
        del prior["Revenues"], prior["GrossProfit"]
        prior["RevenueFromContractWithCustomerExcludingAssessedTax"] = 900.0
        prior["CostOfRevenue"] = 560.0
        r = piotroski_f(curr, prior)
        # gross-margin + asset-turnover are still evaluable via the fallback.
        assert "dgross_margin_up" in r.components
        assert "dasset_turnover_up" in r.components

    def test_no_inputs(self) -> None:
        r = piotroski_f({}, None)
        assert r.score is None
        assert r.components_available == 0
        assert r.reason == "no_inputs"


class TestAltman:
    def test_safe_band(self) -> None:
        r = altman_z2(_CURR)
        assert r.z is not None and r.z > 2.60
        assert r.band == "safe"

    def test_distress_band(self) -> None:
        distressed = {
            "Assets": 1000.0,
            "Liabilities": 1200.0,
            "AssetsCurrent": 100.0,
            "LiabilitiesCurrent": 800.0,
            "RetainedEarningsAccumulatedDeficit": -500.0,
            "OperatingIncomeLoss": -100.0,
            "StockholdersEquity": -200.0,
        }
        r = altman_z2(distressed)
        assert r.z is not None and r.z < 1.10
        assert r.band == "distress"

    def test_missing_input_null(self) -> None:
        partial = dict(_CURR)
        del partial["RetainedEarningsAccumulatedDeficit"]
        r = altman_z2(partial)
        assert r.z is None
        assert r.reason == "missing_input"

    def test_no_total_assets(self) -> None:
        r = altman_z2({"Liabilities": 100.0})
        assert r.z is None
        assert r.reason == "no_total_assets"


class TestPositioning:
    def test_insider_buy_above_neutral(self) -> None:
        s = insider_signal(net_shares=1_000_000, shares_outstanding=1_000_000_000)
        assert s["signal"] is not None and s["signal"] > 0.5

    def test_insider_sell_floored(self) -> None:
        s = insider_signal(net_shares=-50_000_000, shares_outstanding=1_000_000_000)
        assert s["signal"] == 0.40  # heavy net sell floored, never below 0.40

    def test_insider_neutral_zero_net(self) -> None:
        s = insider_signal(net_shares=0, shares_outstanding=1_000_000_000)
        assert s["signal"] == 0.5

    def test_insider_missing(self) -> None:
        assert insider_signal(None, 1_000.0)["signal"] is None
        assert insider_signal(100.0, None)["signal"] is None
        assert insider_signal(100.0, 0)["signal"] is None

    def test_13f_accumulation(self) -> None:
        assert inst_13f_signal(0.10)["signal"] == 1.0  # +10% QoQ saturates high
        assert inst_13f_signal(-0.10)["signal"] == 0.0  # -10% saturates low
        assert inst_13f_signal(0.0)["signal"] == 0.5
        assert inst_13f_signal(None)["signal"] is None

    def test_short_interest(self) -> None:
        assert short_interest_signal(0.02, False)["signal"] == 1.0  # ~no shorting
        assert short_interest_signal(0.30, False)["signal"] == 0.0  # very heavily shorted
        falling = short_interest_signal(0.30, True)["signal"]
        assert falling == 0.1  # 0.0 + 0.1 falling bonus
        assert short_interest_signal(None, None)["signal"] is None


class TestPeerGrade:
    def test_hybrid_and_percentile(self) -> None:
        assert hybrid_grade(0.8, 1.0) == round(0.7 * 0.8 + 0.3 * 1.0, 4)
        assert percentile_rank(0.5, []) == 0.5  # empty -> neutral
        assert percentile_rank(10.0, [0.0, 5.0, 10.0]) == (2 + 0.5) / 3  # mid-rank tie

    def _items(self, n: int, sector: str | None) -> list:
        return [
            (
                i,
                sector,
                {
                    "quality": i / n,
                    "value": 0.5,
                    "turnaround": 0.5,
                    "momentum": 0.5,
                    "sentiment": 0.5,
                    "confidence": 0.5,
                },
            )
            for i in range(1, n + 1)
        ]

    def test_sector_cohort(self) -> None:
        grades = compute_peer_grades(self._items(10, "4"))
        g = grades[10]
        assert g["basis"] == "run_eligible_sector"
        assert g["peer_n"] == 10
        # top quality (i=10) -> high percentile -> hybrid > absolute
        q = g["families"]["quality"]
        assert q["percentile"] > 0.8
        assert q["hybrid"] == hybrid_grade(q["absolute"], q["percentile"])

    def test_universe_fallback(self) -> None:
        # 6 items in one sector: <8 sector peers but >=5 universe -> universe basis.
        grades = compute_peer_grades(self._items(6, "9"))
        assert grades[6]["basis"] == "run_eligible_universe"

    def test_thin_peer_set(self) -> None:
        grades = compute_peer_grades(self._items(3, "2"))
        g = grades[3]
        assert g["basis"] == "peer_set_thin"
        q = g["families"]["quality"]
        assert q["percentile"] is None
        assert q["hybrid"] == q["absolute"]  # absolute-only when thin
