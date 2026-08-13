"""Pure-logic tests for the IAR evidence signals (#1823). No DB — every signal
is a pure function over fact dicts / populations."""

from __future__ import annotations

from datetime import date, timedelta

from app.services.instrument_analytics import (
    _FINRA_SETTLEMENT_MAX_AGE,
    _SHARE_COUNT_FILED_MAX_AGE,
    _build_analytics_block,
    _short_interest_from_row,
    altman_z2,
    compute_peer_grades,
    hybrid_grade,
    insider_signal,
    inst_13f_signal,
    percentile_rank,
    piotroski_f,
    short_interest_signal,
)
from app.services.thesis_break import FRESHNESS_BOUNDS

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


#: SPEC literal, deliberately NOT imported from the code under test — the max
#: age of a FINRA bimonthly settlement date that may still be read as live
#: positioning (#2336). One bridge test below ties it to the shared constant.
_SPEC_FINRA_MAX_AGE_DAYS = 45
#: SPEC literal, same posture — the max age of the FILING that stated the share
#: count in the ratio's denominator (#2411). Bridge test below.
_SPEC_SHARE_COUNT_MAX_AGE_DAYS = 183
_TODAY = date(2026, 8, 8)
#: A denominator filing well inside its bound, so the numerator tests isolate the
#: numerator. Must NOT be _TODAY — a bound tested only at age 0 is not tested.
_FRESH_FILED = _TODAY - timedelta(days=30)


class TestShortInterestFromRow:
    """Shared pure row→signal helper (#2127 Phase 2) — must match the early-return
    contract of the per-instrument reader exactly."""

    def test_none_row(self) -> None:
        assert _short_interest_from_row(
            None, 1000.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        ) == short_interest_signal(None, None)

    def test_current_none(self) -> None:
        assert _short_interest_from_row(
            (None, 5.0, 3.0, date(2026, 8, 1)), 1000.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        ) == short_interest_signal(None, None)

    def test_shares_missing_or_nonpositive(self) -> None:
        row = (100.0, 90.0, 2.0, date(2026, 8, 1))
        assert _short_interest_from_row(
            row, None, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        ) == short_interest_signal(None, None)
        assert _short_interest_from_row(
            row, 0.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        ) == short_interest_signal(None, None)

    def test_valid_row_falling_with_days_and_asof(self) -> None:
        # current < previous → falling; short_pct = 100/1000 = 0.10.
        out = _short_interest_from_row(
            (100.0, 120.0, 2.5, date(2026, 7, 15)), 1000.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        )
        expected = short_interest_signal(0.10, True)
        assert out["signal"] == expected["signal"]
        assert out["falling"] is True
        assert out["days_to_cover"] == 2.5
        assert out["asof"] == "2026-07-15"

    def test_valid_row_rising_no_days_to_cover(self) -> None:
        # current > previous → not falling; days_to_cover absent → no key.
        out = _short_interest_from_row(
            (200.0, 100.0, None, date(2026, 7, 15)), 1000.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        )
        assert out["falling"] is False
        assert "days_to_cover" not in out
        assert out["asof"] == "2026-07-15"


class TestShortInterestStaleness:
    """#2336 — ``finra_short_interest_current`` is latest-WINS, not current-CYCLE,
    so a row can be arbitrarily old (backfilled file, or an instrument that stopped
    appearing in FINRA's file). Beyond the bound the signal is suppressed, not read."""

    _ROW = (100.0, 120.0, 2.5)

    def _at_age(self, days: int) -> dict:
        settlement = _TODAY - timedelta(days=days)
        return _short_interest_from_row(
            (*self._ROW, settlement), 1000.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        )

    def test_boundary_age_is_still_live(self) -> None:
        out = self._at_age(_SPEC_FINRA_MAX_AGE_DAYS)
        assert out["signal"] is not None
        assert "reason" not in out

    def test_one_day_past_boundary_is_suppressed(self) -> None:
        out = self._at_age(_SPEC_FINRA_MAX_AGE_DAYS + 1)
        assert out["signal"] is None
        assert out["reason"] == "stale_settlement"
        assert out["max_age_days"] == _SPEC_FINRA_MAX_AGE_DAYS
        # asof retained: the operator sees WHICH settlement date was rejected.
        assert out["asof"] == (_TODAY - timedelta(days=_SPEC_FINRA_MAX_AGE_DAYS + 1)).isoformat()
        # No positioning numbers leak out of a suppressed signal.
        assert "short_pct" not in out
        assert "days_to_cover" not in out

    def test_backfilled_two_year_old_row_is_suppressed(self) -> None:
        # The #2234 backfill shape: a 2024 settlement date seeded into _current.
        out = _short_interest_from_row(
            (*self._ROW, date(2024, 2, 15)), 1000.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED
        )
        assert out["signal"] is None
        assert out["reason"] == "stale_settlement"

    def test_null_settlement_fails_closed(self) -> None:
        # Column is NOT NULL (sql/152); fail closed anyway, as thesis_break.observe does.
        out = _short_interest_from_row((*self._ROW, None), 1000.0, today=_TODAY, shares_outstanding_filed=_FRESH_FILED)
        assert out["signal"] is None
        assert out["reason"] == "stale_settlement"
        assert "asof" not in out

    def test_bound_is_the_shared_finra_freshness_bound(self) -> None:
        """Bridge: the IAR reader and thesis_break must not drift apart (#1955 class)."""
        assert _FINRA_SETTLEMENT_MAX_AGE == timedelta(days=_SPEC_FINRA_MAX_AGE_DAYS)
        assert _FINRA_SETTLEMENT_MAX_AGE == FRESHNESS_BOUNDS["short_interest_days_to_cover"]["finra_settlement"]
        assert _FINRA_SETTLEMENT_MAX_AGE == FRESHNESS_BOUNDS["short_interest_pct_shares_out"]["finra_settlement"]


class TestShareCountStaleness:
    """#2411 — the DENOMINATOR of the same ratio. A ratio is only as fresh as its
    stalest input, and until #2411 this reader could divide a current short interest
    by a share count filed in 2012 (dev: ``DXR``, 5,246 days)."""

    _ROW = (100.0, 120.0, 2.5, _TODAY - timedelta(days=10))

    def _at_age(self, days: int) -> dict:
        return _short_interest_from_row(
            self._ROW, 1000.0, today=_TODAY, shares_outstanding_filed=_TODAY - timedelta(days=days)
        )

    def test_boundary_age_is_still_live(self) -> None:
        out = self._at_age(_SPEC_SHARE_COUNT_MAX_AGE_DAYS)
        assert out["signal"] is not None
        assert "reason" not in out

    def test_one_day_past_boundary_is_suppressed(self) -> None:
        out = self._at_age(_SPEC_SHARE_COUNT_MAX_AGE_DAYS + 1)
        assert out["signal"] is None
        assert out["reason"] == "stale_share_count"
        assert out["max_age_days"] == _SPEC_SHARE_COUNT_MAX_AGE_DAYS
        # The rejected filing date stays visible, as the numerator gate keeps `asof`.
        assert (
            out["shares_outstanding_asof"] == (_TODAY - timedelta(days=_SPEC_SHARE_COUNT_MAX_AGE_DAYS + 1)).isoformat()
        )
        assert "short_pct" not in out

    def test_delinquent_filer_shape_is_suppressed(self) -> None:
        # The dev shape this bound exists for: a live FINRA row over a 2012 filing.
        out = _short_interest_from_row(self._ROW, 4_216_643.0, today=_TODAY, shares_outstanding_filed=date(2012, 3, 28))
        assert out["signal"] is None
        assert out["reason"] == "stale_share_count"

    def test_null_filed_date_fails_closed(self) -> None:
        out = _short_interest_from_row(self._ROW, 1000.0, today=_TODAY, shares_outstanding_filed=None)
        assert out["signal"] is None
        assert out["reason"] == "stale_share_count"
        assert "shares_outstanding_asof" not in out

    def test_stale_on_both_reports_the_settlement(self) -> None:
        """Order is contractual: FINRA republishes fortnightly, so the numerator's age
        is the one an operator can act on. A delinquent filer's share count will not
        move whatever we report."""
        out = _short_interest_from_row(
            (100.0, 120.0, 2.5, date(2024, 2, 15)),
            1000.0,
            today=_TODAY,
            shares_outstanding_filed=date(2012, 3, 28),
        )
        assert out["reason"] == "stale_settlement"

    def test_bound_is_the_shared_share_count_freshness_bound(self) -> None:
        """Bridge: same #1955 guard as the numerator's. `share_count_filed` is assigned
        to exactly one metric in the vocabulary — if a second metric ever claims it, this
        reader has to decide whether it applies there too rather than inherit silently."""
        assert _SHARE_COUNT_FILED_MAX_AGE == timedelta(days=_SPEC_SHARE_COUNT_MAX_AGE_DAYS)
        assert _SHARE_COUNT_FILED_MAX_AGE == FRESHNESS_BOUNDS["short_interest_pct_shares_out"]["share_count_filed"]
        assert [m for m, inputs in FRESHNESS_BOUNDS.items() if "share_count_filed" in inputs] == [
            "short_interest_pct_shares_out"
        ]


class TestBuildAnalyticsBlock:
    """Shared pure block builder (#2127 Phase 2). Guarantees the per-instrument and
    bulk paths emit byte-identical blocks from the same resolved inputs."""

    def _block(self, **kw: object) -> dict:
        base: dict = dict(
            gics_sector=None,
            shares_outstanding=1000.0,
            curr=None,
            prior=None,
            insider_net=None,
            insider_asof=None,
            delta_pct=None,
            inst_asof=None,
            short_interest=short_interest_signal(None, None),
        )
        base.update(kw)
        return _build_analytics_block(**base)  # type: ignore[arg-type]

    def test_financials_suppressed(self) -> None:
        b = self._block(gics_sector="Financials", curr=_CURR, prior=_PRIOR)
        assert b["piotroski"] == {"score": None, "suppressed": True, "reason": "quality_signal_na_financials"}
        assert b["altman_z"] == {"z": None, "suppressed": True, "reason": "quality_signal_na_financials"}

    def test_no_annual_facts(self) -> None:
        b = self._block(curr=None)
        assert b["piotroski"]["reason"] == "no_annual_facts"
        assert b["piotroski"]["suppressed"] is False
        assert b["altman_z"]["reason"] == "no_annual_facts"

    def test_computed_quality(self) -> None:
        b = self._block(curr=_CURR, prior=_PRIOR)
        assert b["piotroski"]["score"] == piotroski_f(_CURR, _PRIOR).score
        assert b["piotroski"]["suppressed"] is False
        assert b["altman_z"]["z"] == altman_z2(_CURR).z
        assert b["altman_z"]["suppressed"] is False

    def test_positioning_signals_and_asof(self) -> None:
        b = self._block(
            insider_net=5000.0,
            insider_asof=date(2026, 2, 1),
            delta_pct=0.05,
            inst_asof=date(2026, 3, 1),
        )
        pos = b["positioning"]
        assert pos["insider_net_90d"] == {**insider_signal(5000.0, 1000.0), "asof": "2026-02-01"}
        assert pos["inst_13f_qoq"] == {**inst_13f_signal(0.05), "asof": "2026-03-01"}

    def test_insider_none_degrades_not_zero(self) -> None:
        # insider_net None (missing-schema degrade) → signal None, NOT a 0.0-computed signal.
        b = self._block(insider_net=None)
        assert b["positioning"]["insider_net_90d"] == insider_signal(None, 1000.0)

    def test_short_interest_passthrough_and_default_peer_grade(self) -> None:
        si = short_interest_signal(0.2, False)
        b = self._block(short_interest=si)
        assert b["positioning"]["short_interest"] == si
        assert b["schema"] == "iar_v1"
        assert b["peer_grade"] == {"basis": "absolute_only", "reason": "no_run_context", "families": {}}
