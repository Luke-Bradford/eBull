"""#3360 acceptance item 3: the census's pure pieces (``scripts/census_3360_pit_fundamentals.py``)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from app.services import pit_fundamentals as pf
from scripts import census_3360_pit_fundamentals as census
from tests.test_3360_pit_fundamentals import CIK, T1, T2, A, R, _one, _row


def test_formation_is_the_last_nyse_session_of_june() -> None:
    # 2024-06-30 is a Sunday; 2019-06-30 a Sunday; 2022-06-30 a Thursday.
    assert census.formation_dates((2019, 2022, 2024)) == (date(2019, 6, 28), date(2022, 6, 30), date(2024, 6, 28))
    assert len(census.formation_dates()) == 14


def test_membership_window_is_half_open() -> None:
    d = date(2020, 6, 30)
    assert census.membership_mask([date(2018, 7, 1)], [d]) == 1  # D - 730 inclusive
    assert census.membership_mask([date(2018, 6, 30)], [d]) == 0
    assert census.membership_mask([d], [d]) == 0  # accepted ON D is not before D
    assert census.membership_mask([date(2020, 6, 29)], [d, date(2021, 6, 30), date(2023, 6, 30)]) == 0b011


def test_size_deciles_rank_on_value_then_cik() -> None:
    assert census.size_buckets({}) == {}
    assets = {f"{i:010d}": Decimal(i) for i in range(20)}
    buckets = census.size_buckets(assets)
    assert buckets["0000000000"] == 0 and buckets["0000000019"] == 9
    assert sorted(buckets.values()) == [d for d in range(10) for _ in range(2)]
    tie = census.size_buckets({"0000000002": Decimal(5), "0000000001": Decimal(5)})
    assert tie == {"0000000001": 0, "0000000002": 5}


SPAN = (date(2013, 1, 2), date(2024, 12, 31))


@pytest.mark.parametrize(
    ("filings", "decision", "expected"),
    [
        ([], date(2015, 6, 30), census.Outcome(census.NO_FORM25_OBSERVED)),
        ([], date(2012, 6, 29), census.Outcome(census.UNOBSERVED_HORIZON)),  # D before the span
        ([], date(2023, 6, 30), census.Outcome(census.UNOBSERVED_HORIZON)),  # D + 730 after it
        ([(date(2015, 6, 30), "(b)")], date(2015, 6, 30), census.Outcome(census.NO_FORM25_OBSERVED)),  # ON D: out
        (
            [
                (date(2017, 6, 29), "(b)"),
                (date(2016, 1, 5), None),
                (date(2016, 1, 5), "(a)(3)"),
                (date(2016, 2, 1), "x"),
            ],
            date(2015, 6, 30),
            census.Outcome(census.FORM25_OBSERVED, frozenset({"(b)", "NULL", "(a)(3)", "other"}), "(a)(3)+NULL"),
        ),
        # An observed filing is reported even when the rest of the horizon is unobserved.
        (
            [(date(2024, 1, 2), "(a)(4)")],
            date(2023, 6, 30),
            census.Outcome(census.FORM25_OBSERVED, frozenset({"(a)(4)"}), "(a)(4)"),
        ),
    ],
)
def test_outcome_labels_are_raw_and_windowed(
    filings: list[tuple[date, str | None]], decision: date, expected: census.Outcome
) -> None:
    assert census.outcome_at(filings, decision, SPAN) == expected


def test_member_facts_reads_through_the_bundle(tmp_path: Path) -> None:
    rows = {
        A: [_row(100, "a1"), _row(90, "a1", end="2018-12-31"), _row(5, "a1", end="2016-12-31")],
        R: [_row(7, "a1", start="2019-01-01"), _row(8, "a2", start="2019-01-01"), _row(9, "a2", start="2019-01-01")],
    }
    bundle, _ = _one(tmp_path, rows, [("a1", T1, "10-K"), ("a2", T2, "10-K/A")])
    before = census.member_facts(bundle, CIK, date(2020, 2, 10))
    assert before == census.MemberFacts(False, {f"{t}/{c}": frozenset() for t, c in pf.CONCEPT_SET}, None)

    after_t1 = census.member_facts(bundle, CIK, date(2020, 2, 11))
    assert after_t1.has_public_event and after_t1.assets == Decimal(100)  # latest instant end, not largest value
    assert after_t1.coverage["us-gaap/Assets"] == {"value"}
    assert after_t1.coverage["us-gaap/Revenues"] == {"value"}

    after_t2 = census.member_facts(bundle, CIK, date(2020, 6, 2))
    assert after_t2.coverage["us-gaap/Revenues"] == {"ambiguous"}  # two values on one accession
    # 2021-07-02 - 548 days = 2020-01-01: no key is recent any more, but size still reads the
    # latest instant Assets (the spec puts no recency bound on size).
    late = census.member_facts(bundle, CIK, date(2021, 7, 2))
    assert late.coverage["us-gaap/Assets"] == frozenset() and late.assets == Decimal(100)


def test_tabulate_keeps_the_two_axes_and_size_buckets_separate() -> None:
    d = date(2015, 6, 30)
    empty = {f"{t}/{c}": frozenset[str]() for t, c in pf.CONCEPT_SET}
    facts = {
        "0000000001": census.MemberFacts(True, {**empty, "us-gaap/Assets": frozenset({"value"})}, Decimal(10)),
        "0000000002": census.MemberFacts(False, empty, None),
    }
    members = {
        "0000000001": census.IN_BUNDLE,
        "0000000002": census.IN_BUNDLE,
        "0000000003": census.NO_COMPANYFACTS_ENTRY,
    }
    outcomes = {
        "0000000001": census.Outcome(census.FORM25_OBSERVED, frozenset({"(b)"}), "(b)"),
        "0000000002": census.Outcome(census.NO_FORM25_OBSERVED),
        "0000000003": census.Outcome(census.NO_FORM25_OBSERVED),
    }
    row = census.tabulate_formation(d, members, facts, outcomes)
    assert row["members"] == 3
    assert row["status"] == {
        "in_bundle/has_public_event": 1,
        "in_bundle/no_public_event": 1,
        "no_companyfacts_entry/no_public_event": 1,
    }
    assert row["concepts"]["us-gaap/Assets"] == {"value": 1}
    assert row["assets_available"] == 1
    assert row["by_size"]["decile_0"] == {
        "members": 1,
        "form25_observed": 1,
        "label:(b)": 1,
        "first_date:(b)": 1,
        "value:us-gaap/Assets": 1,
    }
    assert row["by_size"][census.ASSETS_UNAVAILABLE] == {"members": 2, "no_form25_observed": 2}
    assert row["outcomes_by_snapshot"]["no_companyfacts_entry"] == {"no_form25_observed": 1}


def test_ledger_reconciles_per_concept_and_refuses_a_gap() -> None:
    rows = {
        "us-gaap/Assets/USD": {"raw": 3, "stored": 2, "integrity_excluded": 1},
        "us-gaap/Assets/EUR": {"raw": 1, "unit_outside_policy": 1},
    }
    assert census.reconcile_ledger(rows) == {
        "us-gaap/Assets": {"integrity_excluded": 1, "raw": 4, "stored": 2, "unit_outside_policy": 1}
    }
    with pytest.raises(RuntimeError, match="does not reconcile"):
        census.reconcile_ledger({"us-gaap/Assets/USD": {"raw": 3, "stored": 2}})
