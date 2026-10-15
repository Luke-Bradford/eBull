"""Slice 3d-3c-3 part A: non-reporting fund disposals, pure (spec Tests 1-8).

Spec: docs/specs/metrics/2026-10-15-3616-slice3d3c3-fund-disposals.md. Every figure is computed by hand in the test.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from app.services.tax_character import SecurityEvidence
from app.services.tax_fund_determination import Decision, Outcome, RunState, parse_determination
from app.services.tax_ledger import _compute_tax_year
from app.services.tax_matching import (
    DISPOSAL_FORMULA_ASSUMED,
    HISTORY_COVERAGE_UNESTABLISHED,
    NON_REPORTING_STATUS_ASSUMED,
    SPREAD_INCLUSION_ASSUMED,
    THIRTY_DAY_PERIOD_ASSUMED,
    WINDOW_OPEN,
    FundIdentified,
    FundInputs,
    FundLot,
    FundRefused,
    FundStatus,
    InstrumentRow,
    LotRow,
    MatchingInputs,
    SubjectRow,
    _check_fund_invariants,  # pyright: ignore[reportPrivateUsage]
    compute_matching,
    fund_digest_entry,
    fund_status,
    fund_statuses,
    identify_fund,
    inputs_digest,
    inputs_document,
)

D = Decimal
HCU, NRS, F2, WO = HISTORY_COVERAGE_UNESTABLISHED, NON_REPORTING_STATUS_ASSUMED, THIRTY_DAY_PERIOD_ASSUMED, WINDOW_OPEN
BASE = {HCU, NRS}
#: A fetch long after every window in these tests: no ``window_open`` unless a test says otherwise.
LATE = datetime(2030, 1, 1, tzinfo=UTC)
SPY_FILE = Path(__file__).resolve().parents[1] / "app/services/tax_fund_determinations/0000884394_v1.json"
SPY_ID = "fund_determination_v1:0000884394:1"
SPY_ASSUMPTIONS = (
    "A1_trustee_cmc",
    "A2_no_uk_branch",
    "A3_indenture",
    "A4_no_other_exclusion",
    "A5_part3_not_applying",
    "A6_law_current",
    "A7_identity_continuity",
    "A8_inventory_complete",
    "A9_terms_continuity",
)


def _at(day: date, hour: int = 12, minute: int = 0) -> datetime:
    """An instant on ``day`` in UTC; at the hours used here the UK date is ``day`` all year."""
    return datetime.combine(day, time(hour, minute), tzinfo=UTC)


def _lot(
    key: str,
    direction: str,
    at: datetime,
    units: str,
    gbp: str | None,
    reasons: frozenset[str] | None = None,
    subject: str | None = None,
) -> FundLot:
    uk_date = at.date()
    acquisition = direction == "a"
    return FundLot(
        lot_key=key,
        direction="acquisition" if acquisition else "disposal",
        subject=subject or f"order:{key}",
        executed_at=at,
        uk_date=uk_date,
        tax_year=_compute_tax_year(uk_date),
        units=D(units),
        amount_gbp=None if gbp is None else D(gbp),
        reasons=reasons
        if reasons is not None
        else frozenset({SPREAD_INCLUSION_ASSUMED if acquisition else DISPOSAL_FORMULA_ASSUMED}),
    )


def _identified(lots: list[FundLot], fetch: datetime | None = LATE, transparent: str = "unresolved") -> FundIdentified:
    result = identify_fund(lots, fetch, transparent)
    assert isinstance(result, FundIdentified), result
    return result


def _rows(result: FundIdentified) -> list[tuple[str, str, str, Decimal, Decimal | None]]:
    return [
        (x.lot_key, i.rule, i.acquisition_lot_key, i.units, i.cost_gbp)
        for x in result.disposals
        for i in x.identifications
    ]


D1 = date(2026, 6, 1)


def _day(n: int) -> date:
    return D1 + timedelta(days=n)


# --- 1. order --------------------------------------------------------------------------------------------------------


class TestOrder:
    def test_lifo_across_days(self) -> None:
        r = _identified(
            [
                _lot("a1", "a", _at(_day(0)), "10", "100"),
                _lot("a2", "a", _at(_day(1)), "10", "300"),
                _lot("s", "d", _at(_day(2)), "15", "500"),
            ]
        )
        assert _rows(r) == [("s", "lifo", "a2", D("10"), D("300")), ("s", "lifo", "a1", D("5"), D("50"))]
        assert r.disposals[0].computed_gain_gbp == D("150")
        assert [(b.lot_key, b.units_left, b.cost_left_gbp) for b in r.balances] == [("a1", D("5"), D("50"))]

    def test_thirty_day_before_lifo(self) -> None:
        r = _identified(
            [
                _lot("a1", "a", _at(_day(0)), "10", "100"),
                _lot("s", "d", _at(_day(1)), "5", "80"),
                _lot("a2", "a", _at(_day(9)), "10", "200"),
            ]
        )
        assert _rows(r) == [("s", "thirty_day", "a2", D("5"), D("100"))]
        assert [(b.lot_key, b.units_left) for b in r.balances] == [("a1", D("10")), ("a2", D("5"))]

    def test_thirty_day_fifo_across_two_later_acquisitions(self) -> None:
        r = _identified(
            [
                _lot("s", "d", _at(_day(0)), "15", "400"),
                _lot("a2", "a", _at(_day(5)), "10", "200"),
                _lot("a1", "a", _at(_day(4)), "10", "100"),
            ]
        )
        assert _rows(r) == [("s", "thirty_day", "a1", D("10"), D("100")), ("s", "thirty_day", "a2", D("5"), D("100"))]

    def test_two_disposals_on_one_date_in_time_order(self) -> None:
        r = _identified(
            [
                _lot("a1", "a", _at(_day(0)), "10", "100"),
                _lot("a2", "a", _at(_day(1)), "10", "200"),
                _lot("s2", "d", _at(_day(2), 11), "10", "150"),
                _lot("s1", "d", _at(_day(2), 10), "10", "150"),
            ]
        )
        # The earlier disposal takes the later acquisition under LIFO first.
        assert _rows(r) == [("s1", "lifo", "a2", D("10"), D("200")), ("s2", "lifo", "a1", D("10"), D("100"))]
        assert [x.seq for x in r.disposals] == [1, 2]

    def test_earlier_disposals_thirty_day_claim_before_later_disposals_lifo(self) -> None:
        r = _identified(
            [
                _lot("a0", "a", _at(_day(0)), "10", "100"),
                _lot("s1", "d", _at(_day(1)), "10", "150"),
                _lot("a2", "a", _at(_day(5)), "10", "300"),
                _lot("s2", "d", _at(_day(10)), "10", "150"),
            ]
        )
        assert _rows(r) == [("s1", "thirty_day", "a2", D("10"), D("300")), ("s2", "lifo", "a0", D("10"), D("100"))]


# --- 2. the period (F2) ----------------------------------------------------------------------------------------------


class TestThePeriod:
    def test_a_later_same_day_acquisition_is_never_identified_with_the_disposal(self) -> None:
        d = _day(10)
        r = _identified(
            [
                _lot("a0", "a", _at(_day(5)), "10", "100"),
                _lot("s", "d", _at(d, 10), "5", "80"),
                _lot("a1", "a", _at(d, 14), "5", "60"),
            ]
        )
        assert _rows(r) == [("s", "lifo", "a0", D("5"), D("50"))]
        assert [(b.lot_key, b.units_left) for b in r.balances] == [("a0", D("5")), ("a1", D("5"))]

    def test_only_a_later_same_day_acquisition_is_incomplete(self) -> None:
        d = _day(10)
        result = identify_fund(
            [_lot("s", "d", _at(d, 10), "5", "80"), _lot("a1", "a", _at(d, 14), "5", "60")], LATE, "unresolved"
        )
        assert isinstance(result, FundRefused)
        assert result.reason == "identification_incomplete"

    def test_d_plus_30_is_in_the_period_d_plus_31_is_not(self) -> None:
        d = _day(0)
        r = _identified([_lot("s", "d", _at(d), "5", "80"), _lot("a30", "a", _at(d + timedelta(days=30)), "5", "50")])
        assert _rows(r) == [("s", "thirty_day", "a30", D("5"), D("50"))]
        r = _identified(
            [
                _lot("a0", "a", _at(d - timedelta(days=1)), "5", "40"),
                _lot("s", "d", _at(d), "5", "80"),
                _lot("a31", "a", _at(d + timedelta(days=31)), "5", "50"),
            ]
        )
        assert _rows(r) == [("s", "lifo", "a0", D("5"), D("40"))]

    def test_an_earlier_same_day_acquisition_is_lifo(self) -> None:
        d = _day(0)
        r = _identified([_lot("a", "a", _at(d, 9), "5", "50"), _lot("s", "d", _at(d, 10), "5", "80")])
        assert _rows(r) == [("s", "lifo", "a", D("5"), D("50"))]
        assert F2 not in r.disposals[0].reasons

    def test_period_across_5_april_keeps_the_disposals_tax_year(self) -> None:
        r = _identified(
            [_lot("s", "d", _at(date(2027, 3, 20)), "5", "80"), _lot("a", "a", _at(date(2027, 4, 10)), "5", "50")]
        )
        assert _rows(r) == [("s", "thirty_day", "a", D("5"), D("50"))]
        assert r.disposals[0].tax_year == "2026/27"

    def test_spy_evidence_run_7_date_reading(self) -> None:
        """Spec fact 4: SPY's eight lots of evidence run 7, with its history fetch start."""

        def at(text: str) -> datetime:
            return datetime.fromisoformat(text).replace(tzinfo=UTC)

        spread, formula = frozenset({SPREAD_INCLUSION_ASSUMED}), frozenset({DISPOSAL_FORMULA_ASSUMED})
        lots = [
            _lot("acq:demo:382257232", "a", at("2026-09-18 15:36:45.083"), "0.29615500", "168.43050669140000", spread),
            _lot(
                "dis:demo:3601264304", "d", at("2026-09-23 13:35:31.583"), "0.29615500", "172.44628722262500", formula
            ),
            _lot("acq:demo:383458210", "a", at("2026-09-23 14:02:07.123"), "0.16227400", "94.27063701503500", spread),
            _lot("acq:demo:383458257", "a", at("2026-09-23 14:10:19.957"), "0.12993500", "75.03203920117100", spread),
            _lot(
                "acq:demo:385158730", "a", at("2026-09-30 15:37:50.750"), "20.62672000", "11933.68460630607000", spread
            ),
            _lot("acq:demo:385501993", "a", at("2026-10-01 15:37:26.687"), "0.13442600", "77.30850742070600", spread),
            _lot("acq:demo:387752115", "a", at("2026-10-09 15:37:35.540"), "0.61384700", "360.56710775200500", spread),
            _lot("acq:demo:388523915", "a", at("2026-10-14 15:38:12.083"), "0.09032700", None, spread),
        ]
        r = _identified(lots, at("2026-10-15 05:04:16"))
        (x,) = r.disposals
        # 0.29615500 × 11933.68460630607000 / 20.62672000, half-even at 1E-14.
        assert _rows(r) == [
            ("dis:demo:3601264304", "thirty_day", "acq:demo:385158730", D("0.29615500"), D("171.34185001689916"))
        ]
        assert (x.status, x.computed_gain_gbp, x.basic_gain_gbp) == (
            "computed",
            D("1.10443720572584"),
            D("1.10443720572584"),
        )
        assert (x.gain_character, x.offshore_income_gain_gbp, x.chargeable_gain_gbp) == (
            "oig_or_capital_unresolved",
            None,
            None,
        )
        reasons = (DISPOSAL_FORMULA_ASSUMED, HCU, NRS, SPREAD_INCLUSION_ASSUMED, F2, WO)
        assert x.reasons == reasons
        assert len(r.balances) == 7
        assert r.pool.units == D("21.75752900")
        balance = next(b for b in r.balances if b.lot_key == "acq:demo:385158730")
        assert (balance.units_left, balance.cost_left_gbp) == (D("20.33056500"), D("11762.34275628917084"))
        assert (r.pool.cost_gbp, r.pool.missing_lot_keys) == (None, ("acq:demo:388523915",))
        # The holding is its balances' reasons plus the carried ones: no disposal's consideration reason.
        assert (r.pool.window_open_since, r.pool.reasons) == (date(2026, 9, 23), reasons[1:])


# --- 3. refusals -----------------------------------------------------------------------------------------------------


class TestRefusals:
    def test_two_lots_at_one_instant(self) -> None:
        at = _at(_day(0))
        result = identify_fund([_lot("a", "a", at, "5", "50"), _lot("s", "d", at, "5", "60")], LATE, "unresolved")
        assert isinstance(result, FundRefused) and result.reason == "time_order_unestablished"

    def test_two_closes_of_one_order_at_different_instants(self) -> None:
        lots = [
            _lot("a", "a", _at(_day(0)), "10", "100"),
            _lot("s1", "d", _at(_day(1), 10), "5", "60", subject="order:1"),
            _lot("s2", "d", _at(_day(1), 11), "5", "60", subject="order:1"),
        ]
        result = identify_fund(lots, LATE, "unresolved")
        assert isinstance(result, FundRefused) and result.reason == "close_fragments_unestablished"

    def test_two_closes_of_different_orders_are_separate_disposals(self) -> None:
        r = _identified(
            [
                _lot("a", "a", _at(_day(0)), "10", "100"),
                _lot("s1", "d", _at(_day(1), 10), "5", "60", subject="order:1"),
                _lot("s2", "d", _at(_day(1), 11), "5", "60", subject="order:2"),
            ]
        )
        assert [x.computed_gain_gbp for x in r.disposals] == [D("10"), D("10")]

    def test_pre_2009_rights(self) -> None:
        result = identify_fund(
            [_lot("a", "a", _at(date(2009, 11, 30)), "5", "50"), _lot("s", "d", _at(date(2020, 1, 2)), "5", "60")],
            LATE,
            "unresolved",
        )
        assert isinstance(result, FundRefused) and result.reason == "pre_2009_rights"
        _identified(
            [_lot("a", "a", _at(date(2009, 12, 1)), "5", "50"), _lot("s", "d", _at(date(2020, 1, 2)), "5", "60")]
        )


# --- 4. gain and character ---------------------------------------------------------------------------------------


def _one(proceeds: str, transparent: str) -> tuple[Any, ...]:
    r = _identified(
        [_lot("a", "a", _at(_day(0)), "10", "100"), _lot("s", "d", _at(_day(1)), "10", proceeds)],
        transparent=transparent,
    )
    (x,) = r.disposals
    return (
        x.gain_character,
        x.computed_gain_gbp,
        x.basic_gain_gbp,
        x.offshore_income_gain_gbp,
        x.chargeable_gain_gbp,
    )


class TestGainAndCharacter:
    def test_a_loss_is_capital(self) -> None:
        for transparent in ("opaque", "transparent", "unresolved"):
            assert _one("80", transparent) == ("capital_loss", D("-20"), D("0"), D("0"), D("-20"))

    def test_zero_is_nil(self) -> None:
        assert _one("100", "opaque") == ("nil", D("0"), D("0"), D("0"), D("0"))

    def test_an_opaque_funds_gain_is_an_offshore_income_gain(self) -> None:
        assert _one("130", "opaque") == ("offshore_income_gain", D("30"), D("30"), D("30"), D("0"))

    @pytest.mark.parametrize("transparent", ["transparent", "unresolved"])
    def test_otherwise_the_split_is_unresolved(self, transparent: str) -> None:
        assert _one("130", transparent) == ("oig_or_capital_unresolved", D("30"), D("30"), None, None)

    def test_a_gain_and_a_loss_on_one_day_stay_two_disposals(self) -> None:
        r = _identified(
            [
                _lot("a1", "a", _at(_day(0)), "10", "100"),
                _lot("a2", "a", _at(_day(1)), "10", "300"),
                _lot("s1", "d", _at(_day(2), 10), "10", "250"),
                _lot("s2", "d", _at(_day(2), 11), "10", "150"),
            ],
            transparent="opaque",
        )
        assert [(x.gain_character, x.computed_gain_gbp, x.basic_gain_gbp) for x in r.disposals] == [
            ("capital_loss", D("-50"), D("0")),
            ("offshore_income_gain", D("50"), D("50")),
        ]


# --- 5. withdrawals --------------------------------------------------------------------------------------------------


class TestWithdrawals:
    def test_three_parts_the_last_taking_the_exact_remainder(self) -> None:
        r = _identified(
            [
                _lot("a", "a", _at(_day(0)), "3", "100"),
                _lot("s1", "d", _at(_day(40)), "1", "40"),
                _lot("s2", "d", _at(_day(41)), "1", "40"),
                _lot("s3", "d", _at(_day(42)), "1", "40"),
            ]
        )
        # 1 × 100 / 3 = 33.333…; then 1 × 66.66666666666667 / 2 = 33.333333333333335, a tie rounded to even (…34);
        # the last takes what is left.
        costs = [x.cost_gbp for x in r.disposals]
        assert costs == [D("33.33333333333333"), D("33.33333333333334"), D("33.33333333333333")]
        assert sum(c for c in costs if c is not None) == D("100")
        assert r.balances == ()

    def test_pending_proceeds(self) -> None:
        r = _identified([_lot("a", "a", _at(_day(0)), "10", "100"), _lot("s", "d", _at(_day(1)), "10", None)])
        (x,) = r.disposals
        assert (x.status, x.proceeds_gbp, x.cost_gbp, x.missing_lot_keys) == ("amount_missing", None, D("100"), ("s",))
        assert (x.computed_gain_gbp, x.basic_gain_gbp, x.gain_character) == (None, None, None)
        assert (x.offshore_income_gain_gbp, x.chargeable_gain_gbp) == (None, None)

    def test_a_pending_acquisition_partly_taken(self) -> None:
        r = _identified([_lot("a", "a", _at(_day(0)), "10", None), _lot("s", "d", _at(_day(1)), "4", "50")])
        (x,) = r.disposals
        (i,) = x.identifications
        (b,) = r.balances
        assert (x.status, x.proceeds_gbp, x.cost_gbp, x.missing_lot_keys) == ("amount_missing", D("50"), None, ("a",))
        assert (i.cost_gbp, i.missing_lot_keys) == (None, ("a",))
        assert (b.units_left, b.cost_left_gbp, b.missing_lot_keys) == (D("6"), None, ("a",))
        assert (r.pool.cost_gbp, r.pool.missing_lot_keys) == (None, ("a",))

    def test_a_pending_acquisition_untouched(self) -> None:
        r = _identified(
            [
                _lot("a1", "a", _at(_day(0)), "10", "100"),
                _lot("s", "d", _at(_day(1)), "5", "60"),
                _lot("a2", "a", _at(_day(40)), "10", None),
            ]
        )
        (x,) = r.disposals
        assert (x.status, x.computed_gain_gbp) == ("computed", D("10"))
        assert (r.pool.units, r.pool.cost_gbp, r.pool.missing_lot_keys) == (D("15"), None, ("a2",))

    def test_a_holding_emptied_with_an_open_window(self) -> None:
        r = _identified(
            [_lot("a", "a", _at(_day(0)), "10", "100"), _lot("s", "d", _at(_day(1)), "10", "120")], fetch=_at(_day(2))
        )
        assert r.balances == ()
        assert (r.pool.units, r.pool.cost_gbp, r.pool.missing_lot_keys) == (D("0"), D("0"), ())
        assert (r.pool.reasons, r.pool.window_open_since) == (tuple(sorted(BASE | {WO})), _day(1))


# --- 6. reasons ------------------------------------------------------------------------------------------------------

AR, SR = frozenset({"acq_reason"}), frozenset({"disposal_reason"})


class TestReasons:
    def test_the_matrix(self) -> None:
        r = _identified(
            [
                _lot("a1", "a", _at(_day(0)), "10", "100", frozenset({"a1_reason"})),
                _lot("a2", "a", _at(_day(1)), "10", "100", AR),
                _lot("s", "d", _at(_day(2)), "5", "60", SR),
            ]
        )
        (x,) = r.disposals
        (i,) = x.identifications
        assert x.reasons == tuple(sorted(BASE | SR | AR))
        assert i.reasons == tuple(sorted(BASE | AR))
        assert {b.lot_key: b.reasons for b in r.balances} == {
            "a1": tuple(sorted(BASE | {"a1_reason"})),
            "a2": tuple(sorted(BASE | AR)),
        }
        assert r.pool.reasons == tuple(sorted(BASE | AR | {"a1_reason"}))

    def test_an_open_window_on_the_first_disposal_marks_everything_after(self) -> None:
        lots = [
            _lot("a1", "a", _at(_day(0)), "10", "100", AR),
            _lot("s1", "d", _at(_day(1)), "2", "30", SR),
            _lot("s2", "d", _at(_day(2)), "2", "30", SR),
        ]
        r = _identified(lots, fetch=_at(_day(3)))
        assert all(WO in x.reasons and all(WO in i.reasons for i in x.identifications) for x in r.disposals)
        assert all(WO in b.reasons for b in r.balances) and WO in r.pool.reasons
        assert r.pool.window_open_since == _day(1)

    def test_a_window_open_only_on_the_second_disposal_marks_from_it_onwards(self) -> None:
        lots = [
            _lot("a1", "a", _at(_day(0)), "10", "100", AR),
            _lot("s1", "d", _at(_day(1)), "2", "30", SR),
            _lot("s2", "d", _at(_day(60)), "2", "30", SR),
        ]
        r = _identified(lots, fetch=_at(_day(61)))
        first, second = r.disposals
        assert WO not in first.reasons and WO not in first.identifications[0].reasons
        assert WO in second.reasons and WO in second.identifications[0].reasons
        assert WO in r.balances[0].reasons and r.pool.window_open_since == _day(60)

    @pytest.mark.parametrize(
        ("day", "open_until"),
        [
            (date(2026, 9, 23), datetime(2026, 10, 23, 23, 0, tzinfo=UTC)),  # BST: London midnight is 23:00Z
            (date(2026, 10, 10), datetime(2026, 11, 10, 0, 0, tzinfo=UTC)),  # across the clock change: 00:00Z
        ],
    )
    def test_the_window_clears_at_elapse(self, day: date, open_until: datetime) -> None:
        lots = [_lot("a", "a", _at(day - timedelta(days=1)), "10", "100"), _lot("s", "d", _at(day), "5", "60")]
        assert WO in _identified(lots, fetch=open_until - timedelta(seconds=1)).disposals[0].reasons
        elapsed = _identified(lots, fetch=open_until)
        assert WO not in elapsed.disposals[0].reasons and elapsed.pool.window_open_since is None

    def test_the_period_reason_from_the_first_disposal_with_a_later_same_day_acquisition(self) -> None:
        d = _day(5)
        r = _identified(
            [
                _lot("a0", "a", _at(_day(0)), "10", "100"),
                _lot("s1", "d", _at(_day(1)), "2", "30"),
                _lot("s2", "d", _at(d, 10), "2", "30"),
                _lot("a1", "a", _at(d, 14), "2", "20"),
            ]
        )
        first, second = r.disposals
        assert F2 not in first.reasons and F2 not in first.identifications[0].reasons
        assert F2 in second.reasons and F2 in second.identifications[0].reasons
        assert all(F2 in b.reasons for b in r.balances) and F2 in r.pool.reasons

    def test_two_securities_each_with_their_own_reasons(self) -> None:
        d = _day(5)
        with_f2 = _identified(
            [
                _lot("a0", "a", _at(_day(0)), "10", "100"),
                _lot("s", "d", _at(d, 10), "2", "30"),
                _lot("a1", "a", _at(d, 14), "2", "20"),
            ]
        )
        without = _identified([_lot("b0", "a", _at(_day(0)), "10", "100"), _lot("t", "d", _at(d, 10), "2", "30")])
        assert F2 in with_f2.disposals[0].reasons and F2 in with_f2.pool.reasons
        assert F2 not in without.disposals[0].reasons and F2 not in without.pool.reasons


# --- 7. the status rule ----------------------------------------------------------------------------------------------

CUTOFF = datetime(2026, 10, 15, 5, 4, 43, tzinfo=UTC)
AS_OF = datetime(2026, 10, 15, 6, 0, tzinfo=UTC)
LOT_DATES = [date(2026, 9, 18), date(2026, 9, 23), date(2026, 9, 30)]


def _spy() -> dict[str, Any]:
    det, errors = parse_determination(SPY_FILE.read_bytes())
    assert det is not None, errors
    return det


def _with_answer(det: dict[str, Any], question: str, answer: str, basis: str) -> dict[str, Any]:
    out = json.loads(json.dumps(det))
    for f in out["findings"]:
        if f["question"] == question:
            f["answer"], f["basis"] = answer, basis
    return out


def _fund(
    *,
    run: RunState | None = RunState("succeeded", CUTOFF),
    outcome: str | None = "determined",
    det: dict[str, Any] | None = None,
    determination_id: str = SPY_ID,
    file_sha256: str = "a" * 64,
    search: str | None = "not_found",
) -> FundInputs:
    if outcome is None:
        return FundInputs(run, None, None, None, search)
    has_det = outcome not in ("fund_observation_unavailable", "not_candidate", "no_determination", "binding_mismatch")
    return FundInputs(
        run,
        Decision(cast(Outcome, outcome), None, determination_id if has_det else None),
        (det or _spy()) if has_det else None,
        file_sha256 if has_det else None,
        search,
    )


class TestStatusRule:
    def test_non_reporting_assumed_at_every_date(self) -> None:
        assert fund_status(_fund(), LOT_DATES, AS_OF) == FundStatus(
            "non_reporting_assumed",
            determination_id=SPY_ID,
            file_sha256="a" * 64,
            transparent_fund="unresolved",
            transparent_basis="unresolved",
            assumptions=SPY_ASSUMPTIONS,
        )

    @pytest.mark.parametrize(
        ("fund", "expected"),
        [
            (_fund(run=None), FundStatus("no_run")),
            (_fund(run=RunState("running", None)), FundStatus("run_failed")),
            (_fund(run=RunState("failed", None)), FundStatus("run_failed")),
            (_fund(outcome=None), FundStatus("not_observed")),
            (_fund(outcome="not_candidate"), FundStatus("not_candidate")),
            (_fund(outcome="stale"), FundStatus("stale")),
            (_fund(search="listed"), FundStatus("reporting_status_unestablished", "list_match")),
            (_fund(search="unresolved"), FundStatus("reporting_status_unestablished", "list_match")),
            (_fund(search=None), FundStatus("reporting_status_unestablished", "search_unavailable")),
            (_fund(det=_with_answer(_spy(), "s355_limb", "none", "established")), FundStatus("not_offshore_fund")),
            (_fund(determination_id="fund_determination_v1:0000000001:1"), FundStatus("exceptions_unreviewed")),
        ],
    )
    def test_each_failing_status_has_no_date(self, fund: FundInputs, expected: FundStatus) -> None:
        assert fund_status(fund, LOT_DATES, AS_OF) == expected

    def test_a_lot_before_valid_from_is_outside_the_interval_with_its_date(self) -> None:
        early = date(2026, 1, 25)
        assert fund_status(_fund(), [*LOT_DATES, early], AS_OF) == FundStatus("outside_interval", None, early)

    def test_the_holding_date_alone(self) -> None:
        """A run whose cutoff precedes ``valid_from`` fails at the holding date, with no lot dates at all."""
        cutoff = datetime(2026, 1, 20, tzinfo=UTC)
        assert fund_status(_fund(run=RunState("succeeded", cutoff)), [], AS_OF) == FundStatus(
            "outside_interval", None, date(2026, 1, 20)
        )

    def test_a_security_with_no_lots_is_evaluated_at_the_holding_date(self) -> None:
        assert fund_status(_fund(), [], AS_OF).status == "non_reporting_assumed"

    def test_review_out_of_date_against_the_matching_runs_as_of(self) -> None:
        assert fund_status(_fund(), LOT_DATES, datetime(2027, 10, 14, 23, tzinfo=UTC)).status == "non_reporting_assumed"
        assert fund_status(_fund(), LOT_DATES, datetime(2027, 10, 15, tzinfo=UTC)) == FundStatus("review_out_of_date")


# --- 8. the digest ---------------------------------------------------------------------------------------------------


def _inputs() -> MatchingInputs:
    return MatchingInputs(
        evidence_run_id=7,
        evidence_policy_id="broker_evidence_v3",
        history_fetch_started_at=None,
        subjects=(),
        lots=(),
        instruments={},
        as_of=AS_OF,
        characters={},
    )


def _document(*statuses: tuple[int, FundStatus]) -> str:
    return inputs_document(_inputs(), dict(statuses))


class TestDigest:
    def test_with_no_fund_the_document_is_v4_with_an_empty_list(self) -> None:
        document = json.loads(inputs_document(_inputs(), {}))
        assert document["version"] == "matching_inputs_v4" and document["funds"] == []

    def test_equivalent_determination_runs_give_one_document(self) -> None:
        later = RunState("succeeded", CUTOFF + timedelta(days=3))
        assert _document((3000, fund_status(_fund(), LOT_DATES, AS_OF))) == _document(
            (3000, fund_status(_fund(run=later), LOT_DATES, AS_OF))
        )
        early = [date(2026, 1, 25)]
        assert _document((3000, fund_status(_fund(), early, AS_OF))) == _document(
            (3000, fund_status(_fund(run=later), early, AS_OF))
        )
        assert _document((3001, fund_status(_fund(outcome="not_candidate"), [], AS_OF))) == _document(
            (3001, fund_status(_fund(run=later, outcome="not_candidate"), [], AS_OF))
        )

    def test_the_assumption_union_is_one_array_in_any_order(self) -> None:
        det = _spy()
        reordered = json.loads(json.dumps(det))
        reordered["findings"].reverse()
        a = fund_status(_fund(det=det), LOT_DATES, AS_OF)
        b = fund_status(_fund(det=reordered), list(reversed(LOT_DATES)), AS_OF)
        assert _document((3000, a)) == _document((3000, b))

    def test_a_status_file_hash_or_transparency_change_changes_it(self) -> None:
        base = _document((3000, fund_status(_fund(), LOT_DATES, AS_OF)))
        changed = [
            fund_status(_fund(outcome="stale"), LOT_DATES, AS_OF),
            fund_status(_fund(file_sha256="b" * 64), LOT_DATES, AS_OF),
            fund_status(_fund(det=_with_answer(_spy(), "transparent_fund", "opaque", "assumed")), LOT_DATES, AS_OF),
        ]
        assert all(_document((3000, s)) != base for s in changed)

    def test_pinned_fixture(self) -> None:
        document = _document(
            (3000, fund_status(_fund(), LOT_DATES, AS_OF)),
            (3001, FundStatus("not_candidate")),
            (3002, FundStatus("outside_interval", None, date(2026, 1, 25))),
        )
        assumptions = ",".join(f'"{a}"' for a in SPY_ASSUMPTIONS)
        assert document == (
            '{"character":[],"evidence":[],"funds":['
            f'[3000,["non_reporting_assumed",null,null,"{SPY_ID}","{"a" * 64}","unresolved","unresolved",'
            f"[{assumptions}]]],"
            '[3001,["not_candidate",null,null,null,null,null,null,null]],'
            '[3002,["outside_interval",null,"2026-01-25",null,null,null,null,null]]],'
            '"instruments":[],"subjects":[],"version":"matching_inputs_v4"}'
        )
        assert inputs_digest(document) == hashlib.sha256(document.encode("ascii")).hexdigest()
        assert inputs_digest(document) == "dd78c971033300e633f640c62bfc918e4932103d3a6cb6c2a3e5e63fb747e026"


# --- part B: the run's wiring (compute_matching, fund_statuses; Designs 1, 5 and 6) ---------------------------------


def _etf(*ciks: str) -> SecurityEvidence:
    return SecurityEvidence(
        types=frozenset({6}),
        ciks=frozenset(ciks),
        symbol="SPY",
        symbols_ever=frozenset({"SPY"}),
        profiles=(),
        filings=(),
        first_acquisition=None,
        covers={},
    )


def _row(key: str, subject: str, direction: str, at: datetime, units: str, gbp: str) -> LotRow:
    return LotRow(
        lot_key=key,
        subject=subject,
        instrument_id=3000,
        direction=direction,
        uk_date=at.date(),
        tax_year=_compute_tax_year(at.date()),
        units=D(units),
        amount_gbp=D(gbp),
        execution_evidence="lookup",
        fingerprint="f" * 64,
        executed_at=at,
    )


def _subject_row(subject: str) -> SubjectRow:
    return SubjectRow(subject, "admitted", 3000, "lookup", (), (), frozenset())


#: Bought 10 for £100 on 09-18, sold 4 for £60 on 09-23: LIFO, £40 cost, a £20 gain.
FUND_LOTS = (
    _row("acq:1", "order:1", "acquisition", _at(date(2026, 9, 18)), "10", "100"),
    _row("dis:1", "order:1", "disposal", _at(date(2026, 9, 23)), "4", "60"),
)


def _fund_inputs(fund: FundInputs | None, lots: tuple[LotRow, ...] = FUND_LOTS, ev: SecurityEvidence | None = None):
    return MatchingInputs(
        evidence_run_id=7,
        evidence_policy_id="broker_evidence_v3",
        history_fetch_started_at=LATE,
        subjects=tuple(_subject_row(s) for s in sorted({lot.subject for lot in lots})),
        lots=lots,
        instruments={3000: InstrumentRow(3000, "SPY", None)},
        as_of=AS_OF,
        characters={3000: ev or _etf("0000884394")},
        funds={} if fund is None else {3000: replace(fund, run_id=11)},
    )


class TestWiring:
    def test_a_non_reporting_fund_is_identified_as_relevant_securities(self) -> None:
        inputs = _fund_inputs(_fund())
        result = compute_matching(inputs)
        (s,) = result.securities
        assert (s.status, s.holding_basis, s.character.character, s.character.detail) == (
            "computed",
            "relevant_securities",
            "non_reporting_fund",
            None,
        )
        assert s.character.fund == fund_status(_fund(), [date(2026, 9, 18), date(2026, 9, 23)], AS_OF)
        assert s.character.fund_run_id == 11 and s.disposals == ()
        assert s.fund is not None
        (x,) = s.fund.disposals
        assert [(i.rule, i.acquisition_lot_key, i.units, i.cost_gbp) for i in x.identifications] == [
            ("lifo", "acq:1", D("4"), D("40"))
        ]
        assert (x.computed_gain_gbp, x.gain_character) == (D("20"), "oig_or_capital_unresolved")
        assert s.pool is not None and s.pool == s.fund.pool
        assert (s.pool.units, s.pool.cost_gbp) == (D("6"), D("60"))
        assert result.dependencies == ()
        counts = result.counts()
        assert (counts["fund_disposals"], counts["fund_identifications"], counts["fund_balances"]) == (1, 1, 1)
        assert (counts["statutory_disposals"], counts["identifications"]) == (0, 0)
        assert s.character.fund is not None
        assert json.loads(inputs_document(inputs, fund_statuses(inputs)))["funds"] == [
            [3000, fund_digest_entry(s.character.fund)]
        ]

    @pytest.mark.parametrize("fund", [None, _fund(outcome="not_candidate"), _fund(search="listed")])
    def test_any_other_status_withholds_at_the_etf_line(self, fund: FundInputs | None) -> None:
        result = compute_matching(_fund_inputs(fund))
        (s,) = result.securities
        expected = FundStatus("no_run") if fund is None else fund_status(fund, [date(2026, 9, 18)], AS_OF)
        assert (s.status, s.holding_basis, s.character.character, s.character.detail) == (
            "withheld",
            None,
            "unestablished",
            "exchange_traded_fund",
        )
        assert s.character.fund is not None and s.character.fund.status == expected.status
        assert s.character.fund_run_id is None and s.fund is None
        assert [(d.reason, d.detail) for d in result.dependencies] == [
            ("character_unestablished", "exchange_traded_fund")
        ]

    def test_a_refusal_withholds_with_its_reason(self) -> None:
        lots = (*FUND_LOTS, _row("dis:2", "order:1", "disposal", _at(date(2026, 9, 24)), "1", "15"))
        result = compute_matching(_fund_inputs(_fund(), lots))
        (s,) = result.securities
        assert (s.status, s.character.character) == ("withheld", "non_reporting_fund")
        assert [(d.reason, d.detail) for d in result.dependencies] == [
            ("close_fragments_unestablished", "two disposal lots of order:1")
        ]

    def test_two_primary_ciks_keep_member_conflict_and_are_never_a_fund(self) -> None:
        """Test 7's remaining clause: an all-type-6 security with two primary CIKs."""
        inputs = _fund_inputs(_fund(), ev=_etf("0000884394", "0000000001"))
        assert fund_statuses(inputs) == {}
        (s,) = compute_matching(inputs).securities
        assert (s.character.character, s.character.detail, s.character.fund) == (
            "unestablished",
            "member_conflict",
            None,
        )
        assert json.loads(inputs_document(inputs, fund_statuses(inputs)))["funds"] == []

    def test_a_disposal_cost_differing_from_its_identifications_is_refused_before_insert(self) -> None:
        lots = [_lot("a", "a", _at(date(2026, 9, 18)), "10", "100"), _lot("d", "d", _at(date(2026, 9, 23)), "4", "60")]
        good = _identified(lots, LATE)
        (x,) = good.disposals
        bad = replace(
            good, disposals=(replace(x, cost_gbp=D("41"), computed_gain_gbp=D("19"), basic_gain_gbp=D("19")),)
        )
        with pytest.raises(RuntimeError, match="identifications' sum"):
            _check_fund_invariants(lots, bad, "unresolved")
