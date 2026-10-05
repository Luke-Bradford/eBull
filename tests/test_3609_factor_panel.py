"""#3609 step 1 slice 3: pure fixtures for the factor panel's accounting and ME core.

Each test builds a one-CIK #3360 shard in memory, validates it with the real shard validator and reads it
through the real ``PitFundamentalsBundle``, so the bundle's own read rules are exercised, not re-implemented.
Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from app.services import pit_fundamentals as pf
from app.services.factor_panel import (
    Check,
    CikView,
    Exclusion,
    Group,
    Kind,
    MeMissing,
    Missing,
    PanelError,
    PrefixCache,
    ShareReference,
    SicStatus,
    SortInput,
    SplitStamp,
    TermStatus,
    add_months,
    characteristic,
    decision_session,
    fallback_flow,
    flow,
    formation_months,
    is_filer,
    lag_eligible,
    market_equity,
    prefix_exclusion,
    sic_as_of,
    split_product,
    tercile_groups,
    usable_reference,
)
from app.services.pit_fundamentals import PitFundamentalsBundle, ReadStatus

CIK = "0000000001"
ME = Decimal(1000)


def _acc(day: str) -> str:
    # 20:00Z is 15:00 or 16:00 in New York, so the NY date is ``day``.
    return f"{day}T20:00:00.000Z"


class Shard:
    def __init__(self) -> None:
        self.accessions: dict[str, tuple[str, str]] = {}
        self.events: list[dict[str, Any]] = []
        self.rejections: list[dict[str, Any]] = []

    def filing(self, accn: str, accepted: str, form: str) -> Shard:
        self.accessions[accn] = (_acc(accepted), form)
        return self

    def fact(
        self,
        concept: str,
        value: object,
        accn: str,
        end: str,
        start: str | None = None,
        *,
        taxonomy: str = "us-gaap",
        unit: str = "USD",
    ) -> Shard:
        self.events.append(
            {
                "taxonomy": taxonomy,
                "concept": concept,
                "unit": unit,
                "start": start,
                "end": end,
                "accn": accn,
                "acceptance": self.accessions[accn][0],
                "value": pf.canonical_decimal(Decimal(str(value))),
                "multiplicity": 1,
            }
        )
        return self

    def reject(
        self,
        concept: str,
        accn: str,
        end: str,
        start: str | None = None,
        *,
        taxonomy: str = "us-gaap",
        unit: str = "USD",
    ) -> Shard:
        self.rejections.append(
            {
                "taxonomy": taxonomy,
                "concept": concept,
                "unit": unit,
                "start": start,
                "end": end,
                "accn": accn,
                "acceptance": self.accessions[accn][0],
                "reason": "chokepoint_reject",
                "rows": [0],
            }
        )
        return self

    def bundle(self) -> PitFundamentalsBundle:
        manifest = {"supported_through": "2026-09-23", "snapshot_integrity_failures": [], "shards": [{"cik": CIK}]}
        bundle = PitFundamentalsBundle(Path("/nonexistent"), manifest, "test")
        shard = {
            "schema": pf.SHARD_SCHEMA,
            "cik": CIK,
            "events": sorted(self.events, key=pf._event_order),
            "rejections": sorted(self.rejections, key=pf._rejection_order),
            "accessions": sorted(
                ({"accn": a, "acceptance": t, "form": f} for a, (t, f) in self.accessions.items()),
                key=pf._accession_order,
            ),
        }
        bundle._cache[CIK] = pf.validate_shard(shard, cik10=CIK, expected_events=len(self.events))
        return bundle

    def view(self, decision: date) -> CikView:
        return CikView(self.bundle(), CIK, decision)


def _balance(shard: Shard, accn: str, end: str, *, assets: object = 100, seq: object = 40) -> Shard:
    return shard.fact("Assets", assets, accn, end).fact("StockholdersEquity", seq, accn, end)


# --------------------------------------------------------------------------- dates


def test_calendar_and_stage_a_grid() -> None:
    assert add_months(date(2016, 12, 31), 4) == date(2017, 4, 30)
    assert add_months(date(2017, 1, 31), 4) == date(2017, 5, 31)
    grid = formation_months()
    assert (len(grid), grid[0], grid[-1]) == (80, date(2014, 9, 30), date(2021, 4, 30))
    sessions = [date(2017, 4, 27), date(2017, 4, 28), date(2017, 5, 1)]
    assert decision_session(date(2017, 4, 30), sessions) == date(2017, 4, 28)
    with pytest.raises(PanelError):
        decision_session(date(2017, 4, 26), sessions)


def test_lag_eligibility_examples() -> None:
    assert lag_eligible(date(2016, 12, 31), date(2017, 4, 30))
    assert not lag_eligible(date(2017, 1, 31), date(2017, 4, 30))
    assert lag_eligible(date(2017, 1, 31), date(2017, 5, 31))


def _timing_shard(k_accepted: str) -> Shard:
    shard = Shard().filing("q3", "2016-11-01", "10-Q").filing("k", k_accepted, "10-K")
    _balance(shard, "q3", "2016-09-30", seq=30)
    return _balance(shard, "k", "2016-12-31", seq=40)


def test_timing_10k_accepted_before_the_decision_session_is_used() -> None:
    view = _timing_shard("2017-04-27").view(date(2017, 4, 28))
    got = characteristic("be_me", view, date(2017, 4, 30), ME)
    assert (got.period_end, got.kind, got.value) == (date(2016, 12, 31), Kind.ANNUAL, 0.04)


def test_timing_10k_accepted_after_the_decision_session_waits_a_month() -> None:
    shard = _timing_shard("2017-05-02")
    april = characteristic("be_me", shard.view(date(2017, 4, 28)), date(2017, 4, 30), ME)
    assert (april.period_end, april.kind, april.value) == (date(2016, 9, 30), Kind.QUARTERLY, 0.03)
    may = characteristic("be_me", shard.view(date(2017, 5, 31)), date(2017, 5, 31), ME)
    assert may.period_end == date(2016, 12, 31)


def test_timing_january_fiscal_year_first_eligible_in_may() -> None:
    shard = _balance(Shard().filing("k", "2017-03-20", "10-K"), "k", "2017-01-31")
    april = characteristic("be_me", shard.view(date(2017, 4, 28)), date(2017, 4, 30), ME)
    assert april.missing is Missing.NO_PERIOD
    may = characteristic("be_me", shard.view(date(2017, 5, 31)), date(2017, 5, 31), ME)
    assert may.period_end == date(2017, 1, 31)


def test_prefix_cache_reproduces_the_bundle_prefix_at_every_earlier_decision() -> None:
    shard = _timing_shard("2017-05-02")
    bundle = shard.bundle()
    cache = PrefixCache(bundle, CIK, date(2017, 5, 31))
    for decision in (date(2016, 11, 1), date(2016, 11, 2), date(2017, 4, 28), date(2017, 5, 2), date(2017, 5, 31)):
        cached = CikView(bundle, CIK, decision, prefixes=cache)
        direct = CikView(bundle, CIK, decision)
        assert cached.prefix("us-gaap", "StockholdersEquity") == direct.prefix("us-gaap", "StockholdersEquity")
        assert (cached.anchors, cached.filings) == (direct.anchors, direct.filings)
    with pytest.raises(PanelError):
        cache.at("us-gaap", "Assets", date(2017, 6, 1))


def test_value_older_than_18_months_is_aged_out() -> None:
    shard = _balance(Shard().filing("k", "2015-02-20", "10-K"), "k", "2014-12-31")
    got = characteristic("be_me", shard.view(date(2016, 7, 29)), date(2016, 7, 31), ME)
    assert (got.missing, got.period_end) == (Missing.AGED_OUT, date(2014, 12, 31))


# --------------------------------------------------------------------------- state machines


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (ReadStatus.OK, None),
        (ReadStatus.CIK_NOT_IN_BUNDLE, Exclusion.NO_FUNDAMENTALS),
        (ReadStatus.CIK_INTEGRITY_EXCLUDED, Exclusion.INTEGRITY_EXCLUDED),
    ],
)
def test_prefix_state_machine(status: ReadStatus, expected: Exclusion | None) -> None:
    assert prefix_exclusion(status) is expected


@pytest.mark.parametrize("status", [ReadStatus.AFTER_CAPTURE, ReadStatus.CONCEPT_NOT_IN_POLICY])
def test_prefix_states_that_refuse_the_run(status: ReadStatus) -> None:
    with pytest.raises(PanelError):
        prefix_exclusion(status)


def test_cik_outside_the_bundle_is_excluded() -> None:
    bundle = Shard().bundle()
    assert CikView(bundle, "0000000002", date(2017, 4, 28)).exclusion is Exclusion.NO_FUNDAMENTALS


def test_newer_blocked_key_is_never_skipped_for_an_older_clean_one() -> None:
    shard = Shard().filing("q3", "2016-11-01", "10-Q").filing("k", "2017-02-20", "10-K")
    _balance(shard, "q3", "2016-09-30")
    shard.fact("Assets", 100, "k", "2016-12-31").reject("StockholdersEquity", "k", "2016-12-31")
    got = characteristic("be_me", shard.view(date(2017, 4, 28)), date(2017, 4, 30), ME)
    assert (got.missing, got.period_end) == (Missing.BLOCKED_BY_REJECTION, date(2016, 12, 31))


def test_ambiguous_value_is_missing_with_no_fallback_branch() -> None:
    shard = Shard().filing("k", "2017-02-20", "10-K").fact("Assets", 100, "k", "2016-12-31")
    shard.fact("StockholdersEquity", 40, "k", "2016-12-31").fact("StockholdersEquity", 41, "k", "2016-12-31")
    shard.fact("Liabilities", 60, "k", "2016-12-31")  # AT - LT must not be tried
    got = characteristic("be_me", shard.view(date(2017, 4, 28)), date(2017, 4, 30), ME)
    assert got.missing is Missing.AMBIGUOUS


def test_value_from_an_unanchored_accession_is_a_form_mismatch() -> None:
    shard = Shard().filing("k", "2017-02-20", "10-K").filing("ka", "2017-03-10", "10-K/A")
    _balance(shard, "k", "2016-12-31")
    shard.fact("StockholdersEquity", 45, "ka", "2016-12-31")  # the amendment reports no Assets: no anchor
    view = shard.view(date(2017, 4, 28))
    assert view.unanchored == ["ka"]
    assert characteristic("be_me", view, date(2017, 4, 30), ME).missing is Missing.FORM_MISMATCH


def test_twenty_f_accession_has_no_anchor() -> None:
    shard = _balance(Shard().filing("f", "2017-03-10", "20-F"), "f", "2016-12-31")
    view = shard.view(date(2017, 4, 28))
    assert view.anchors == {} and characteristic("be_me", view, date(2017, 4, 30), ME).missing is Missing.NO_PERIOD


# --------------------------------------------------------------------------- items


def test_book_equity_branches_and_zero_if_missing_components() -> None:
    shard = Shard().filing("k", "2017-02-20", "10-K").fact("Assets", 100, "k", "2016-12-31")
    shard.fact("Liabilities", 70, "k", "2016-12-31").fact("DeferredIncomeTaxLiabilitiesNet", 5, "k", "2016-12-31")
    shard.fact("PreferredStockValue", 2, "k", "2016-12-31")
    got = characteristic("be_me", shard.view(date(2017, 4, 28)), date(2017, 4, 30), ME)
    assert got.value == pytest.approx(0.033)  # (100 - 70) + 5 - 2
    assert "AT-LT" in got.branches and "TXDITC" in got.branches
    assert {f.key.concept: f.value for f in got.facts}["Liabilities"] == "70"
    signs = {f.key.concept: f.coefficient for f in got.facts}
    assert signs == {
        "Assets": 1,
        "Liabilities": -1,
        "DeferredIncomeTaxLiabilitiesNet": 1,
        "PreferredStockValue": -1,
    }


def _quarters(shard: Shard) -> Shard:
    """FY2016 (calendar) quarters for a 10-Q/10-K filer; GP reported directly for Q1 only."""
    for accn, accepted, form, end in (
        ("q1", "2016-05-01", "10-Q", "2016-03-31"),
        ("q2", "2016-08-01", "10-Q", "2016-06-30"),
        ("q3", "2016-11-01", "10-Q", "2016-09-30"),
        ("k", "2017-02-20", "10-K", "2016-12-31"),
    ):
        shard.filing(accn, accepted, form).fact("Assets", 100, accn, end)
    return shard


def test_quarterly_precedence_direct_then_ytd_then_q4_residual() -> None:
    shard = _quarters(Shard())
    shard.fact("GrossProfit", 10, "q1", "2016-03-31", "2016-01-01")  # direct
    shard.fact("GrossProfit", 25, "q2", "2016-06-30", "2016-01-01")  # H1 YTD -> Q2 = 15
    shard.fact("GrossProfit", 12, "q3", "2016-09-30", "2016-07-01")  # direct
    shard.fact("GrossProfit", 50, "k", "2016-12-31", "2016-01-01")  # annual -> Q4 = 50 - (10 + 15 + 12) = 13
    view = shard.view(date(2017, 4, 28))
    ttm = flow(view, ("GrossProfit",), date(2016, 12, 31), Kind.QUARTERLY)
    assert ttm.value == Decimal(50)
    assert {"direct", "ytd_difference", "q4_residual"} <= set(ttm.branches)


def test_ttm_breaks_when_quarters_are_not_consecutive() -> None:
    shard = _quarters(Shard())
    for start, end, accn in (
        ("2016-01-01", "2016-03-31", "q1"),
        ("2016-04-01", "2016-06-30", "q2"),
        ("2016-07-20", "2016-09-30", "q3"),  # starts 20 days after Q2 ends
        ("2016-10-01", "2016-12-31", "k"),
    ):
        shard.fact("GrossProfit", 10, accn, end, start)
    view = shard.view(date(2017, 4, 28))
    assert flow(view, ("GrossProfit",), date(2016, 12, 31), Kind.QUARTERLY).value is None


def test_synonym_concepts_fall_back_per_quarter() -> None:
    shard = _quarters(Shard())
    shard.fact("SalesRevenueNet", 30, "q1", "2016-03-31", "2016-01-01")
    for start, end, accn in (
        ("2016-04-01", "2016-06-30", "q2"),
        ("2016-07-01", "2016-09-30", "q3"),
        ("2016-10-01", "2016-12-31", "k"),
    ):
        shard.fact("Revenues", 30, accn, end, start)
    ttm = flow(shard.view(date(2017, 4, 28)), ("Revenues", "SalesRevenueNet"), date(2016, 12, 31), Kind.QUARTERLY)
    assert ttm.value == Decimal(120)


def test_later_quarterly_period_beats_an_older_annual_one_and_ties_go_annual() -> None:
    shard = _quarters(Shard())
    shard.filing("q1b", "2017-05-01", "10-Q").fact("Assets", 100, "q1b", "2017-03-31")
    for accn, start, end, value in (
        ("k", "2016-01-01", "2016-12-31", 40),
        ("q1", "2016-01-01", "2016-03-31", 10),
        ("q2", "2016-04-01", "2016-06-30", 10),
        ("q3", "2016-07-01", "2016-09-30", 10),
        ("k", "2016-10-01", "2016-12-31", 10),
        ("q1b", "2017-01-01", "2017-03-31", 20),
    ):
        shard.fact("GrossProfit", value, accn, end, start)
    may = characteristic("gp_at", shard.view(date(2017, 4, 28)), date(2017, 4, 30), None)
    assert (may.period_end, may.kind, may.value) == (date(2016, 12, 31), Kind.ANNUAL, 0.4)
    august = characteristic("gp_at", shard.view(date(2017, 7, 31)), date(2017, 7, 31), None)
    assert (august.period_end, august.kind, august.value) == (date(2017, 3, 31), Kind.QUARTERLY, 0.5)


def test_asset_growth_matches_the_prior_year_period_and_needs_a_positive_base() -> None:
    shard = Shard().filing("k15", "2016-02-20", "10-K").filing("k16", "2017-02-20", "10-K")
    shard.fact("Assets", 80, "k15", "2015-12-31").fact("Assets", 100, "k16", "2016-12-31")
    got = characteristic("at_gr1", shard.view(date(2017, 4, 28)), date(2017, 4, 30), None)
    assert got.value == pytest.approx(0.25)
    zero = Shard().filing("k15", "2016-02-20", "10-K").filing("k16", "2017-02-20", "10-K")
    zero.fact("Assets", 0, "k15", "2015-12-31").fact("Assets", 100, "k16", "2016-12-31")
    got = characteristic("at_gr1", zero.view(date(2017, 4, 28)), date(2017, 4, 30), None)
    assert got.missing is Missing.NONPOSITIVE_DENOMINATOR


# --------------------------------------------------------------------------- Amendment 2b: ni* and ocf*

FY_START, FY_END, FY = "2016-01-01", "2016-12-31", date(2016, 12, 31)
DECIDED = date(2017, 4, 28)
DO_PARENT = "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity"
DO_CONSOLIDATED = "IncomeLossFromDiscontinuedOperationsNetOfTax"
DO_NCI = "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToNoncontrollingInterest"
DO_BEFORE_TAX = "DiscontinuedOperationIncomeLossFromDiscontinuedOperationBeforeIncomeTax"
XI = "ExtraordinaryItemNetOfTax"
OCF_CONTINUING = "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
OCF_DISCONTINUED = "CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations"


def _annual(**facts: object) -> Shard:
    """A FY2016 10-K reporting each ``concept=value`` over the fiscal year."""
    shard = Shard().filing("k", "2017-02-20", "10-K").fact("Assets", 100, "k", FY_END)
    for concept, value in facts.items():
        shard.fact(concept, value, "k", FY_END, FY_START)
    return shard


def _ni(shard: Shard, kind: Kind = Kind.ANNUAL) -> Any:
    return fallback_flow(shard.view(DECIDED), "ni_me", FY, kind)


def _ocf(shard: Shard, kind: Kind = Kind.ANNUAL) -> Any:
    return fallback_flow(shard.view(DECIDED), "ocf_me", FY, kind)


def _each_quarter(shard: Shard, concept: str, value: object) -> Shard:
    for accn, start, end in (
        ("q1", "2016-01-01", "2016-03-31"),
        ("q2", "2016-04-01", "2016-06-30"),
        ("q3", "2016-07-01", "2016-09-30"),
        ("k", "2016-10-01", "2016-12-31"),
    ):
        shard.fact(concept, value, accn, end, start)
    return shard


def test_ib_and_oancf_win_over_their_fallbacks_annual_and_quarterly() -> None:
    annual = _annual(IncomeLossFromContinuingOperations=10, NetIncomeLoss=50)
    annual.fact("NetCashProvidedByUsedInOperatingActivities", 7, "k", FY_END, FY_START)
    annual.fact(OCF_CONTINUING, 3, "k", FY_END, FY_START)
    assert (_ni(annual).value, _ni(annual).branches[0]) == (Decimal(10), "ib")
    assert (_ocf(annual).value, _ocf(annual).branches[0]) == (Decimal(7), "oancf")
    quarterly = _each_quarter(_quarters(Shard()), "IncomeLossFromContinuingOperations", 10)
    _each_quarter(quarterly, "NetIncomeLoss", 50)
    _each_quarter(quarterly, "NetCashProvidedByUsedInOperatingActivities", 7)
    _each_quarter(quarterly, OCF_CONTINUING, 3)
    ni, ocf = _ni(quarterly, Kind.QUARTERLY), _ocf(quarterly, Kind.QUARTERLY)
    assert (ni.value, "ni_minus_xido" in ni.branches) == (Decimal(40), False)
    assert (ocf.value, "continuing_plus_discontinued" in ocf.branches) == (Decimal(28), False)


def test_companions_read_through_every_quarterly_branch_and_annually() -> None:
    shard = _quarters(Shard())
    for concept, q1, h1, q3, year in (("NetIncomeLoss", 10, 25, 12, 50), (DO_PARENT, 1, 3, 1, 5)):
        shard.fact(concept, q1, "q1", "2016-03-31", "2016-01-01")  # direct
        shard.fact(concept, h1, "q2", "2016-06-30", "2016-01-01")  # YTD difference
        shard.fact(concept, q3, "q3", "2016-09-30", "2016-07-01")  # direct
        shard.fact(concept, year, "k", FY_END, FY_START)  # Q4 residual, and the annual period
    ttm = _ni(shard, Kind.QUARTERLY)
    assert ttm.value == Decimal(45)  # NI 50 - DO 5, XI absent and unwitnessed
    assert {"direct", "ytd_difference", "q4_residual", "do_parent", "do_filed"} <= set(ttm.branches)
    assert _ni(shard).value == Decimal(45)


def test_absent_xi_is_zero_and_filed_xi_is_subtracted_with_or_without_do() -> None:
    assert _ni(_annual(NetIncomeLoss=50)).value == Decimal(50)
    assert _ni(_annual(NetIncomeLoss=50, **{XI: 3})).value == Decimal(47)
    both = _ni(_annual(NetIncomeLoss=50, **{XI: 3, DO_PARENT: 5}))
    assert both.value == Decimal(42) and {"xi_filed", "do_filed"} <= set(both.branches)


def test_do_reads_parent_then_consolidated_minus_nci_then_the_flagged_proxy() -> None:
    parent = _ni(_annual(NetIncomeLoss=50, **{DO_PARENT: 5, DO_CONSOLIDATED: 8, DO_NCI: 1}))
    assert parent.value == Decimal(45) and "do_parent" in parent.branches
    minus = _ni(_annual(NetIncomeLoss=50, **{DO_CONSOLIDATED: 8, DO_NCI: 1}))
    assert minus.value == Decimal(43) and "do_consolidated_minus_nci" in minus.branches
    proxy = _ni(_annual(NetIncomeLoss=50, **{DO_CONSOLIDATED: 8}))
    assert proxy.value == Decimal(42) and "do_consolidated_proxy" in proxy.branches


def test_an_absent_companion_is_zero_unless_a_public_non_zero_witness_overlaps() -> None:
    assert _ni(_annual(NetIncomeLoss=50)).value == Decimal(50)
    assert _ocf(_annual(**{OCF_CONTINUING: 30})).value == Decimal(30)
    witnessed = _annual(NetIncomeLoss=50, **{DO_BEFORE_TAX: 4})
    assert _ni(witnessed).status is TermStatus.ABSENT
    assert _ocf(_annual(**{OCF_CONTINUING: 30, DO_BEFORE_TAX: 4})).status is TermStatus.ABSENT
    # The witness's interval need only overlap the period.
    overlap = _annual(NetIncomeLoss=50).fact(DO_BEFORE_TAX, 4, "k", "2016-06-30", "2016-04-01")
    assert _ni(overlap).status is TermStatus.ABSENT
    # A fact filed without a start is dated at its end: inside the interval it vetoes, outside it does not.
    dated = _annual(NetIncomeLoss=50).fact(DO_BEFORE_TAX, 4, "k", "2016-05-15")
    assert _ni(dated).status is TermStatus.ABSENT
    outside = _annual(NetIncomeLoss=50).fact(DO_BEFORE_TAX, 4, "k", "2017-01-15")
    assert _ni(outside).value == Decimal(50)
    # A witness whose current value is zero does not veto.
    corrected = _annual(NetIncomeLoss=50, **{DO_BEFORE_TAX: 4}).filing("ka", "2017-03-10", "10-K/A")
    corrected.fact(DO_BEFORE_TAX, 0, "ka", FY_END, FY_START)
    assert _ni(corrected).value == Decimal(50)
    # A witness accepted on or after s(M) is not public at s(M).
    late = _annual(NetIncomeLoss=50).filing("ka", DECIDED.isoformat(), "10-K/A")
    late.fact(DO_BEFORE_TAX, 4, "ka", FY_END, FY_START)
    assert _ni(late).value == Decimal(50)


def test_a_blocked_base_or_companion_blocks_the_period() -> None:
    base = _annual(**{XI: 3}).reject("NetIncomeLoss", "k", FY_END, FY_START)
    assert _ni(base).status is TermStatus.BLOCKED_BY_REJECTION
    for concept in (XI, DO_PARENT):
        blocked = _annual(NetIncomeLoss=50).reject(concept, "k", FY_END, FY_START)
        assert _ni(blocked).status is TermStatus.BLOCKED_BY_REJECTION
    absent_base = _annual().reject(XI, "k", FY_END, FY_START)
    assert _ni(absent_base).status is TermStatus.BLOCKED_BY_REJECTION
    ocf = _annual(**{OCF_CONTINUING: 30}).reject(OCF_DISCONTINUED, "k", FY_END, FY_START)
    assert _ocf(ocf).status is TermStatus.BLOCKED_BY_REJECTION


def test_a_companion_over_a_different_interval_leaves_the_period_absent() -> None:
    shard = _annual(NetIncomeLoss=50).fact(XI, 3, "k", FY_END, "2016-01-02")
    assert _ni(shard).status is TermStatus.ABSENT
    ocf = _annual(**{OCF_CONTINUING: 30}).fact(OCF_DISCONTINUED, 2, "k", FY_END, "2016-01-02")
    assert _ocf(ocf).status is TermStatus.ABSENT


def test_fallback_provenance_carries_each_facts_sign() -> None:
    got = _ni(_annual(NetIncomeLoss=50, **{XI: 3, DO_CONSOLIDATED: 8, DO_NCI: 1}))
    assert got.value == Decimal(40)  # 50 - 3 - (8 - 1)
    signs = {f.key.concept: f.coefficient for f in got.facts}
    assert signs == {"NetIncomeLoss": 1, XI: -1, DO_CONSOLIDATED: -1, DO_NCI: 1}


def test_quarterly_nci_over_a_different_interval_is_not_subtracted() -> None:
    shard = _each_quarter(_quarters(Shard()), "NetIncomeLoss", 10)
    _each_quarter(shard, DO_CONSOLIDATED, 2)
    _each_quarter(shard, DO_NCI, 1)
    assert _ni(shard, Kind.QUARTERLY).value == Decimal(36)  # 40 - 4 x (2 - 1)
    mismatched = _each_quarter(_quarters(Shard()), "NetIncomeLoss", 10)
    _each_quarter(mismatched, DO_CONSOLIDATED, 2)
    for accn, start, end in (
        ("q1", "2016-01-01", "2016-03-31"),
        ("q2", "2016-04-01", "2016-06-30"),
        ("q3", "2016-07-01", "2016-09-30"),
        ("k", "2016-10-03", "2016-12-31"),  # a quarter, but not the consolidated DO's
    ):
        mismatched.fact(DO_NCI, 1, accn, end, start)
    # Q4's DO is absent, and the non-zero consolidated DO witnesses against a zero: Q4 and the TTM are absent.
    assert _ni(mismatched, Kind.QUARTERLY).status is TermStatus.ABSENT


def test_a_ttm_mixing_primary_and_fallback_quarters_chains() -> None:
    shard = _quarters(Shard())
    shard.fact("IncomeLossFromContinuingOperations", 10, "q1", "2016-03-31", "2016-01-01")
    shard.fact("IncomeLossFromContinuingOperations", 10, "q2", "2016-06-30", "2016-04-01")
    shard.fact("NetIncomeLoss", 20, "q3", "2016-09-30", "2016-07-01")
    shard.fact("NetIncomeLoss", 20, "k", FY_END, "2016-10-01")
    ttm = _ni(shard, Kind.QUARTERLY)
    assert ttm.value == Decimal(60) and {"ib", "ni_minus_xido"} <= set(ttm.branches)
    got = characteristic("ni_me", shard.view(DECIDED), date(2017, 4, 30), ME)
    assert (got.value, got.kind) == (pytest.approx(0.06), Kind.QUARTERLY)


# --------------------------------------------------------------------------- market equity


def _cover(shard: Shard, accn: str, context: str, shares: object) -> Shard:
    return shard.fact("EntityCommonStockSharesOutstanding", shares, accn, context, taxonomy="dei", unit="shares")


def test_me_uses_the_cover_count_moved_through_a_forward_split() -> None:
    # AAPL's 4-for-1: stamp 4 on the 2020-08-31 ex-date, between the cover date and s(M).
    shard = _balance(Shard().filing("q", "2020-07-31", "10-Q"), "q", "2020-06-27")
    _cover(shard, "q", "2020-07-17", 4_275_634_000)
    me = market_equity(shard.view(date(2020, 9, 30)), Decimal("115.81"), [SplitStamp(date(2020, 8, 31), Decimal(4))])
    assert (me.shares_scope, me.basis, me.split_product) == ("cover", date(2020, 7, 17), Decimal(4))
    assert me.value == Decimal(4_275_634_000) * 4 * Decimal("115.81")


def test_split_product_conventions() -> None:
    reverse = [SplitStamp(date(2021, 8, 2), Decimal("0.125"))]
    assert split_product(reverse, date(2021, 7, 1), date(2021, 8, 31)) == Decimal("0.125")
    on_basis = [SplitStamp(date(2020, 8, 31), Decimal(4))]
    assert split_product(on_basis, date(2020, 8, 31), date(2020, 9, 30)) == 1
    after_decision = [SplitStamp(date(2020, 10, 1), Decimal(4))]
    assert split_product(after_decision, date(2020, 7, 17), date(2020, 9, 30)) == 1
    with pytest.raises(PanelError):
        split_product([SplitStamp(date(2020, 8, 31), Decimal(0))], date(2020, 7, 17), date(2020, 9, 30))


def test_me_falls_back_to_the_balance_sheet_count_with_acceptance_basis() -> None:
    shard = _balance(Shard().filing("k", "2017-02-20", "10-K"), "k", "2016-12-31")
    shard.fact("CommonStockSharesOutstanding", 50, "k", "2016-12-31", unit="shares")
    _cover(shard, "k", "2015-01-30", 999)  # a comparative before the period end: not a cover count
    me = market_equity(shard.view(date(2017, 4, 28)), Decimal(2), [SplitStamp(date(2017, 3, 1), Decimal(2))])
    assert (me.shares_scope, me.basis, me.value) == ("balance_sheet", date(2017, 2, 20), Decimal(200))


def test_rejected_share_context_after_the_decision_never_outranks_a_valid_count() -> None:
    shard = _balance(Shard().filing("k", "2017-02-20", "10-K"), "k", "2016-12-31")
    _cover(shard, "k", "2017-02-10", 50)
    shard.reject("EntityCommonStockSharesOutstanding", "k", "2207-01-01", taxonomy="dei", unit="shares")
    me = market_equity(shard.view(date(2017, 4, 28)), Decimal(2), [])
    assert (me.missing, me.value) == (None, Decimal(100))


def test_me_takes_no_fallback_when_the_cover_count_is_blocked() -> None:
    shard = _balance(Shard().filing("k", "2017-02-20", "10-K"), "k", "2016-12-31")
    shard.fact("CommonStockSharesOutstanding", 50, "k", "2016-12-31", unit="shares")
    _cover(shard, "k", "2017-02-10", 50)
    _cover(shard, "k", "2017-02-10", 51)
    me = market_equity(shard.view(date(2017, 4, 28)), Decimal(2), [])
    assert (me.missing, me.value) == (MeMissing.SHARES_AMBIGUOUS, None)


def test_stale_cover_count_falls_back_and_missing_price_is_its_own_reason() -> None:
    shard = _balance(Shard().filing("k", "2015-02-20", "10-K"), "k", "2014-12-31")
    _cover(shard, "k", "2015-02-10", 50)
    assert market_equity(shard.view(date(2016, 7, 29)), Decimal(2), []).missing is MeMissing.NO_SHARES
    fresh = market_equity(shard.view(date(2015, 3, 31)), None, [])
    assert fresh.missing is MeMissing.NONPOSITIVE_PRICE


# --------------------------------------------------------------------------- Amendment 2 share checks


def _q(
    shard: Shard, accn: str, accepted: str, end: str, *, cover: tuple[str, object] | None, sheet: object = None
) -> Shard:
    _balance(shard.filing(accn, accepted, "10-Q"), accn, end)
    if cover is not None:
        _cover(shard, accn, cover[0], cover[1])
    if sheet is not None:
        shard.fact("CommonStockSharesOutstanding", sheet, accn, end, unit="shares")
    return shard


def _ref(shares: object, *, formation: date = date(2017, 3, 31), cik: str = CIK) -> ShareReference:
    return ShareReference(formation, formation, Decimal(str(shares)), cik)


def test_a_split_between_the_cover_date_and_the_filing_is_ambiguous() -> None:
    # NFLX 2015-07: cover dated 2015-06-30, the 7:1 split on 2015-07-15, filed 2015-07-17.
    shard = _q(Shard(), "q", "2015-07-17", "2015-06-30", cover=("2015-06-30", 425_889_000))
    me = market_equity(shard.view(date(2015, 7, 31)), Decimal(114), [SplitStamp(date(2015, 7, 15), Decimal(7))])
    assert (me.missing, me.value, me.checks["basis"]) == (MeMissing.SHARES_BASIS_AMBIGUOUS, None, Check.FAIL)
    assert me.raw_value == Decimal(425_889_000) * 7 * 114


def test_a_stamp_product_beyond_tolerance_is_ambiguous_for_either_scope() -> None:
    # A bankruptcy re-issue stamped as a split (BAS 2016-12, 0.0018): the pre-event count is not the new equity.
    shard = _q(Shard(), "q", "2016-11-09", "2016-09-30", cover=("2016-11-01", 42_000_000))
    me = market_equity(shard.view(date(2016, 12, 30)), Decimal(35), [SplitStamp(date(2016, 12, 23), Decimal("0.0018"))])
    assert me.missing is MeMissing.SHARES_BASIS_AMBIGUOUS


def test_dqc_0095_conflict_without_a_reference_is_missing() -> None:
    # GRMN: the cover count tagged x1,000, the same filing's balance sheet right.
    shard = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    me = market_equity(shard.view(date(2017, 5, 31)), Decimal(50), [])
    assert (me.missing, me.checks["scale"], me.verified) == (MeMissing.SHARES_SCALE_CONFLICT, Check.FAIL, False)


def test_dqc_0095_conflict_recovers_the_side_the_reference_supports() -> None:
    grmn = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    me = market_equity(grmn.view(date(2017, 5, 31)), Decimal(50), [], reference=_ref(189_000_000))
    assert (me.missing, me.shares_scope, me.shares) == (None, "dqc_recovered:balance_sheet", Decimal(188_000_000))
    assert (me.checks["scale"], me.verified) == (Check.RECOVERED, False)
    # AMTX: the cover count right, the balance sheet in thousands.
    amtx = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 20_432_827), sheet=20_428)
    me = market_equity(amtx.view(date(2017, 5, 31)), Decimal(5), [], reference=_ref(20_400_000))
    assert (me.shares_scope, me.shares) == ("dqc_recovered:cover", Decimal(20_432_827))


def test_a_split_before_filing_is_already_in_the_balance_sheet_count() -> None:
    # Amendment 2.2, SAB Topic 4C: the 10:1 split on 2017-04-10 precedes the filing, so the 2017-03-31 balance-sheet
    # count of 4,000 is post-split. The cover count is x1,000. Adjusting the sheet count by the split again would
    # make the gap exactly 100x and pass it.
    split = [SplitStamp(date(2017, 4, 10), Decimal(10))]
    shard = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 4_000_000), sheet=4_000)
    me = market_equity(shard.view(date(2017, 5, 31)), Decimal(5), split)
    assert (me.missing, me.checks["scale"]) == (MeMissing.SHARES_SCALE_CONFLICT, Check.FAIL)
    agree = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 4_000), sheet=4_000)
    assert market_equity(agree.view(date(2017, 5, 31)), Decimal(5), split).checks["scale"] is Check.PASS
    # A 1:10 reverse split the other way: a cover count in thousands against the restated sheet was 100x before.
    reverse = [SplitStamp(date(2017, 4, 10), Decimal("0.1"))]
    thousands = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 4_000), sheet=4_000_000)
    me = market_equity(thousands.view(date(2017, 5, 31)), Decimal(5), reverse)
    assert (me.missing, me.checks["scale"]) == (MeMissing.SHARES_SCALE_CONFLICT, Check.FAIL)


def test_recovery_takes_comparators_that_agree_with_each_other() -> None:
    # One read returns several accessions only when they share an acceptance timestamp (``value_as_of``): here a
    # filing and a co-filed amendment, each with the same balance-sheet count against the x1,000 cover count.
    shard = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    _q(shard, "qa", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    me = market_equity(shard.view(date(2017, 5, 31)), Decimal(50), [], reference=_ref(189_000_000))
    assert me.facts[-1].accns == ("q", "qa")
    assert (me.shares_scope, me.basis, me.shares) == (
        "dqc_recovered:balance_sheet",
        date(2017, 5, 1),
        Decimal(188_000_000),
    )
    assert me.facts[0].accns == ("qa",)  # the highest accession number of the co-filed set
    # Comparators that disagree with each other leave nothing to recover.
    _q(shard, "qb", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000)
    me = market_equity(shard.view(date(2017, 5, 31)), Decimal(50), [], reference=_ref(189_000_000))
    assert (me.missing, me.checks["scale"]) == (MeMissing.SHARES_SCALE_CONFLICT, Check.FAIL)


def test_recovery_refuses_comparators_that_break_the_co_filed_invariant() -> None:
    # ``value_as_of`` returns only accessions at one acceptance; a read that broke that would mis-basis recovery.
    from app.services.factor_panel import _checked

    shard = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    _q(shard, "qa", "2017-05-08", "2017-03-31", cover=None, sheet=188_000_000)
    view = shard.view(date(2017, 5, 31))
    common: dict[str, Any] = {
        "shares_scope": "cover",
        "shares": Decimal(188_000_000_000),
        "basis": date(2017, 4, 25),
        "split_product": Decimal(1),
        "facts": (),
    }
    with pytest.raises(PanelError, match="co-filed"):
        _checked(
            view,
            Decimal(50),
            [],
            common,
            Decimal(188_000_000_000),
            ("q", "qa"),
            date(2017, 5, 1),
            None,
            _ref(189_000_000),
            None,
        )


def test_a_comparator_agreeing_with_the_cover_blocks_recovery_to_another() -> None:
    # Cover 188B; one co-filed comparator agrees with it, the other conflicts and matches the reference.
    shard = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    _q(shard, "qa", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=187_000_000_000)
    me = market_equity(shard.view(date(2017, 5, 31)), Decimal(50), [], reference=_ref(189_000_000))
    assert (me.missing, me.checks["scale"]) == (MeMissing.SHARES_SCALE_CONFLICT, Check.FAIL)


def test_a_rejected_comparator_leaves_check_2_untested_and_recovery_records_the_count_used() -> None:
    blocked = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    blocked.reject("CommonStockSharesOutstanding", "q", "2017-03-31", unit="shares")
    assert market_equity(blocked.view(date(2017, 5, 31)), Decimal(50), []).checks["scale"] is Check.UNTESTED
    grmn = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    me = market_equity(grmn.view(date(2017, 5, 31)), Decimal(50), [], reference=_ref(189_000_000))
    used, cover = me.facts
    assert (used.key.concept, used.value, used.branch, used.accns) == (
        "CommonStockSharesOutstanding",
        "188000000",
        "dqc_comparator",
        ("q",),
    )
    assert cover.key.concept == "EntityCommonStockSharesOutstanding"


def test_counts_that_agree_pass_and_a_count_without_a_comparator_is_untested() -> None:
    agree = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 101), sheet=100)
    me = market_equity(agree.view(date(2017, 5, 31)), Decimal(2), [], dollar_volume=20.0)
    assert (me.checks["scale"], me.checks["turnover"], me.checks["discontinuity"], me.verified) == (
        Check.PASS,
        Check.PASS,
        Check.UNTESTED,
        True,
    )
    alone = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 101))
    me = market_equity(alone.view(date(2017, 5, 31)), Decimal(2), [], dollar_volume=20.0)
    assert (me.missing, me.checks["scale"], me.verified) == (None, Check.UNTESTED, False)


def test_dollar_volume_above_ten_times_me_is_implausible() -> None:
    shell = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 100), sheet=100)
    me = market_equity(shell.view(date(2017, 5, 31)), Decimal(20), [], dollar_volume=20_001.0)  # ME 2,000
    assert (me.missing, me.checks["turnover"]) == (MeMissing.SHARES_TURNOVER_IMPLAUSIBLE, Check.FAIL)
    assert market_equity(shell.view(date(2017, 5, 31)), Decimal(20), [], dollar_volume=20_000.0).missing is None


def test_the_chain_rejects_a_x1000_count_then_accepts_the_next_clean_one() -> None:
    shard = _q(Shard(), "q1", "2017-05-01", "2017-03-31", cover=("2017-04-25", 30_000_000_000))
    _q(shard, "q2", "2017-08-01", "2017-06-30", cover=("2017-07-25", 29_900_000))
    reference = _ref(30_000_000)
    bad = market_equity(shard.view(date(2017, 5, 31)), Decimal(60), [], reference=reference)
    assert (bad.missing, bad.checks["discontinuity"]) == (MeMissing.SHARES_DISCONTINUITY, Check.FAIL)
    clean = market_equity(shard.view(date(2017, 8, 31)), Decimal(60), [], reference=reference)
    assert (clean.missing, clean.checks["discontinuity"]) == (None, Check.PASS)
    # The reference moves through a real split: 30M x 4 against 120M passes.
    split = [SplitStamp(date(2017, 8, 15), Decimal(4))]
    moved = market_equity(shard.view(date(2017, 8, 31)), Decimal(15), split, reference=reference)
    assert moved.checks["discontinuity"] is Check.PASS


def test_a_reference_applies_only_within_15_months_on_the_same_cik() -> None:
    reference = _ref(1, formation=date(2017, 3, 31))
    assert usable_reference(reference, date(2018, 6, 30), CIK) is reference
    assert usable_reference(reference, date(2018, 7, 31), CIK) is None
    assert usable_reference(reference, date(2017, 4, 30), "0000000002") is None
    assert usable_reference(None, date(2017, 4, 30), CIK) is None


def _partner(shares: object, accn: str, counted: str = "2017-04-25") -> ShareReference:
    return ShareReference(date(2017, 3, 31), date(2017, 3, 31), Decimal(str(shares)), CIK, (accn,), counted)


def test_an_untested_count_is_verified_only_by_agreement_with_another_filing() -> None:
    # Amendment 2.1: no same-filing comparator, so check 2 is untested; the run's previous admitted count decides.
    shard = _q(Shard(), "q2", "2017-08-01", "2017-06-30", cover=("2017-07-25", 30_100_000))
    view = shard.view(date(2017, 8, 31))

    def verified(partner: ShareReference | None, dollar_volume: float | None = 1e9) -> bool:
        me = market_equity(view, Decimal(60), [], dollar_volume=dollar_volume, partner=partner)
        assert (me.missing, me.checks["scale"]) == (None, Check.UNTESTED)  # never a check: admission is unchanged
        return me.verified

    assert verified(_partner(30_000_000, "q1"))
    assert not verified(_partner(30_000_000, "q2"))  # the same filing's fact, read again, does not agree with itself
    assert not verified(_partner(30_000_000, "q1", counted="2017-07-25"))  # the same count date: an amendment's repeat
    assert not verified(_partner(30_000, "q1"))  # beyond 100x
    assert not verified(_partner(30_000_000, "q1"), dollar_volume=None)  # check 3 untested
    assert not verified(None)  # the first admitted count of a run
    # A DQC-recovered count stays ineligible, whatever the previous count says.
    grmn = _q(Shard(), "q", "2017-05-01", "2017-03-31", cover=("2017-04-25", 188_000_000_000), sheet=188_000_000)
    me = market_equity(
        grmn.view(date(2017, 5, 31)), Decimal(50), [], reference=_ref(189_000_000), partner=_partner(188_000_000, "p")
    )
    assert (me.checks["scale"], me.verified) == (Check.RECOVERED, False)


# --------------------------------------------------------------------------- universe helpers


def test_filer_window_and_sic_by_exact_accession() -> None:
    filings = {"k": (_acc("2015-02-20"), "10-K"), "q": (_acc("2016-11-01"), "10-Q"), "f": (_acc("2017-01-05"), "20-F")}
    decision = date(2017, 4, 28)
    assert is_filer(filings, decision)
    assert not is_filer({"k": filings["k"], "f": filings["f"]}, decision)
    assert sic_as_of(filings, {"q": 6798}, decision).sic == 6798
    assert sic_as_of(filings, {"q": None}, decision).status is SicStatus.SIC_NULL
    assert sic_as_of(filings, {"k": 1311}, decision).status is SicStatus.SIC_UNLOADED


# --------------------------------------------------------------------------- terciles


def _sort(values: Sequence[float], *, micro: Sequence[float] = ()) -> dict[int, Group]:
    names = [SortInput(i, v, 1e9) for i, v in enumerate(values)]
    names += [SortInput(100 + i, v, 1.0) for i, v in enumerate(micro)]
    return dict(tercile_groups(names, micro_cutoff_usd=1e6).groups)


def test_terciles_equal_counts_and_micro_names_on_the_breakpoints() -> None:
    groups = _sort([1, 2, 3, 4, 5, 6, 7], micro=[0.5, 2, 4, 6, 9])
    assert [groups[i] for i in range(7)] == [Group.LOW] * 2 + [Group.MIDDLE] * 3 + [Group.HIGH] * 2
    assert [groups[100 + i] for i in range(5)] == [Group.LOW, Group.LOW, Group.MIDDLE, Group.HIGH, Group.HIGH]


def test_tied_run_takes_its_first_members_group() -> None:
    groups = _sort([1, 2, 2, 2, 5, 6])  # n=6, thirds of 2: index 1 is low, so the whole run of 2s is low
    assert [groups[i] for i in range(6)] == [Group.LOW] * 4 + [Group.HIGH] * 2


def test_small_and_all_micro_months() -> None:
    assert set(_sort([1, 2]).values()) == {Group.MIDDLE}
    assert set(_sort([], micro=[1, 2, 3]).values()) == {Group.MIDDLE}
