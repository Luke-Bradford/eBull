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
    CikView,
    Exclusion,
    Group,
    Kind,
    MeMissing,
    Missing,
    PanelError,
    SicStatus,
    SortInput,
    SplitStamp,
    add_months,
    characteristic,
    decision_session,
    flow,
    formation_months,
    is_filer,
    lag_eligible,
    market_equity,
    prefix_exclusion,
    sic_as_of,
    split_product,
    tercile_groups,
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

    def reject(self, concept: str, accn: str, end: str, start: str | None = None, *, unit: str = "USD") -> Shard:
        self.rejections.append(
            {
                "taxonomy": "us-gaap",
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


# --------------------------------------------------------------------------- universe helpers


def test_filer_window_and_sic_by_exact_accession() -> None:
    forms = {"k": "10-K", "q": "10-Q", "f": "20-F"}
    accessions = {"k": _acc("2015-02-20"), "q": _acc("2016-11-01"), "f": _acc("2017-01-05")}
    assert is_filer(accessions, forms, date(2017, 4, 28))
    assert not is_filer({"k": accessions["k"]}, forms, date(2017, 4, 28))
    assert sic_as_of(accessions, forms, {"q": 6798}, date(2017, 4, 28)).sic == 6798
    assert sic_as_of(accessions, forms, {"q": None}, date(2017, 4, 28)).status is SicStatus.SIC_NULL
    assert sic_as_of(accessions, forms, {"k": 1311}, date(2017, 4, 28)).status is SicStatus.SIC_UNLOADED


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
