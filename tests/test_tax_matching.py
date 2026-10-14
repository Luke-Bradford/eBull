"""Statutory matching, pure (#3616 slice 3d-2; spec docs/specs/metrics/2026-10-14-3616-slice3d2-statutory-matching.md,
"Tests" 1-13 and the pure half of 16). Every expected figure is computed by hand in the test."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.tax_broker_evidence import EvidenceRunReport
from app.services.tax_character import (
    ISSUER_BINDING_ASSUMED,
    OFFSHORE_FUND_EXCLUSION_ASSUMED,
    Cover,
    CoverFiling,
    Profile,
    SecurityEvidence,
)
from app.services.tax_ledger import _compute_tax_year
from app.services.tax_matching import (
    DISPOSAL_FORMULA_ASSUMED,
    DISTRIBUTION_CHARACTER_ASSUMED,
    EXECUTION_ASSUMED,
    HISTORY_COVERAGE_UNESTABLISHED,
    LOOKUP_EVIDENCE_UNOBSERVED,
    SPREAD_INCLUSION_ASSUMED,
    SYNC_CHARGES_UNOBSERVED,
    WINDOW_OPEN,
    CoverRefreshReport,
    Identified,
    InstrumentRow,
    LotRow,
    MatchingInputs,
    MatchingRunReport,
    MatchLot,
    StepFiveReached,
    SubjectRow,
    apportion,
    classify_lot,
    compute_matching,
    fits,
    identify_security,
    inputs_digest,
    inputs_document,
    security_members,
    share_quanta,
    window_elapses_at,
)

D = Decimal
HCU = HISTORY_COVERAGE_UNESTABLISHED
IBA, OFE = ISSUER_BINDING_ASSUMED, OFFSHORE_FUND_EXCLUSION_ASSUMED
#: A fetch long after every window in these tests: no ``window_open`` unless a test says otherwise.
LATE = datetime(2030, 1, 1, tzinfo=UTC)


def _lot(key: str, direction: str, day: date, units: str, gbp: str | None, reasons: Iterable[str] = ()) -> MatchLot:
    return MatchLot(
        lot_key=key,
        direction="acquisition" if direction == "a" else "disposal",
        uk_date=day,
        tax_year=_compute_tax_year(day),
        units=D(units),
        amount_gbp=None if gbp is None else D(gbp),
        reasons=frozenset(reasons),
    )


def _identified(lots: list[MatchLot], fetch: datetime | None = LATE) -> Identified:
    result = identify_security(lots, fetch)
    assert isinstance(result, Identified)
    return result


def _rows(result: Identified) -> list[tuple[date, str, date | None, Decimal, Decimal | None]]:
    return [
        (x.uk_date, i.rule, i.acquisition_uk_date, i.units, i.cost_gbp)
        for x in result.disposals
        for i in x.identifications
    ]


# --- 1. the three 3a defects, and the 3a matcher's other cases at day level -------------------------------------------


class TestThe3aDefectsStayFixed:
    def test_same_day_acquisitions_are_averaged(self) -> None:
        d = date(2026, 9, 23)
        r = _identified(
            [_lot("a1", "a", d, "10", "100"), _lot("a2", "a", d, "10", "300"), _lot("s", "d", d, "10", "250")]
        )
        assert _rows(r) == [(d, "same_day", d, D("10"), D("200"))]
        assert r.disposals[0].gain_gbp == D("50")
        assert (r.pool.units, r.pool.cost_gbp) == (D("10"), D("200"))

    def test_a_days_own_disposal_takes_its_acquisition_before_an_earlier_30_day_claim(self) -> None:
        d0, d5, d10 = date(2026, 1, 1), date(2026, 1, 6), date(2026, 1, 11)
        r = _identified(
            [
                _lot("a0", "a", d0, "10", "100"),
                _lot("s5", "d", d5, "10", "150"),
                _lot("a10", "a", d10, "10", "200"),
                _lot("s10", "d", d10, "10", "210"),
            ]
        )
        assert _rows(r) == [(d5, "s104_pool", None, D("10"), D("100")), (d10, "same_day", d10, D("10"), D("200"))]
        assert (r.pool.units, r.pool.cost_gbp) == (D("0"), D("0"))

    def test_two_same_day_disposals_of_one_purchase_leave_no_pool(self) -> None:
        d = date(2026, 3, 2)
        r = _identified(
            [_lot("a", "a", d, "20", "400"), _lot("s1", "d", d, "10", "210"), _lot("s2", "d", d, "10", "190")]
        )
        assert _rows(r) == [(d, "same_day", d, D("20"), D("400"))]
        assert r.disposals[0].lot_keys == ("s1", "s2")
        assert (r.pool.units, r.pool.cost_gbp) == (D("0"), D("0"))

    def test_partial_same_day_leaves_the_rest_in_the_pool(self) -> None:
        d = date(2026, 6, 15)
        r = _identified([_lot("a", "a", d, "100", "800"), _lot("s", "d", d, "40", "400")])
        assert _rows(r) == [(d, "same_day", d, D("40"), D("320"))]
        assert (r.pool.units, r.pool.cost_gbp) == (D("60"), D("480"))

    def test_an_acquisition_within_30_days_after_is_identified_before_the_pool(self) -> None:
        d0, d10, d20 = date(2026, 6, 1), date(2026, 6, 11), date(2026, 6, 21)
        r = _identified(
            [_lot("a0", "a", d0, "100", "1000"), _lot("s", "d", d10, "50", "600"), _lot("a20", "a", d20, "50", "550")]
        )
        assert _rows(r) == [(d10, "bed_and_breakfast", d20, D("50"), D("550"))]
        assert r.disposals[0].gain_gbp == D("50")
        assert (r.pool.units, r.pool.cost_gbp) == (D("100"), D("1000"))

    def test_earliest_acquisitions_in_the_window_first(self) -> None:
        d, a5, a10 = date(2026, 6, 1), date(2026, 6, 6), date(2026, 6, 11)
        r = _identified(
            [_lot("s", "d", d, "100", "1500"), _lot("b1", "a", a5, "60", "600"), _lot("b2", "a", a10, "60", "900")]
        )
        assert _rows(r) == [
            (d, "bed_and_breakfast", a5, D("60"), D("600")),
            (d, "bed_and_breakfast", a10, D("40"), D("600")),
        ]
        assert (r.pool.units, r.pool.cost_gbp) == (D("20"), D("300"))

    def test_the_pool_averages_its_acquisitions(self) -> None:
        r = _identified(
            [
                _lot("a1", "a", date(2026, 1, 5), "100", "1000"),
                _lot("a2", "a", date(2026, 2, 14), "100", "3000"),
                _lot("s", "d", date(2026, 4, 1), "50", "1200"),
            ]
        )
        assert _rows(r) == [(date(2026, 4, 1), "s104_pool", None, D("50"), D("1000"))]
        assert (r.pool.units, r.pool.cost_gbp) == (D("150"), D("3000"))

    def test_the_pool_carries_across_tax_years(self) -> None:
        r = _identified([_lot("a", "a", date(2025, 6, 1), "10", "100"), _lot("s", "d", date(2026, 6, 1), "4", "60")])
        assert r.disposals[0].tax_year == "2026/27"
        assert (r.disposals[0].cost_gbp, r.pool.units, r.pool.cost_gbp) == (D("40"), D("6"), D("60"))


# --- 2-3. one statutory disposal per day; competing windows -----------------------------------------------------------


def test_pieces_of_one_day_net_inside_one_disposal() -> None:
    d = date(2026, 3, 1)
    r = _identified(
        [_lot("a", "a", date(2026, 1, 1), "10", "100"), _lot("p1", "d", d, "5", "80"), _lot("p2", "d", d, "5", "40")]
    )
    (x,) = r.disposals
    assert (x.lot_keys, x.units, x.proceeds_gbp, x.cost_gbp, x.gain_gbp) == (
        ("p1", "p2"),
        D("10"),
        D("120"),
        D("100"),
        D("20"),
    )


def test_the_earlier_disposal_takes_an_acquisition_inside_both_windows() -> None:
    d1, d2, a = date(2026, 2, 10), date(2026, 2, 20), date(2026, 2, 25)
    r = _identified(
        [
            _lot("a0", "a", date(2026, 1, 1), "20", "200"),
            _lot("s1", "d", d1, "10", "120"),
            _lot("s2", "d", d2, "10", "120"),
            _lot("a1", "a", a, "10", "150"),
        ]
    )
    assert _rows(r) == [(d1, "bed_and_breakfast", a, D("10"), D("150")), (d2, "s104_pool", None, D("10"), D("100"))]
    assert (r.pool.units, r.pool.cost_gbp) == (D("10"), D("100"))


# --- 4-7. the window --------------------------------------------------------------------------------------------------


class TestWindow:
    def test_day_30_is_inside_and_day_31_is_not(self) -> None:
        opening, d = _lot("a0", "a", date(2026, 1, 1), "10", "100"), date(2026, 3, 1)
        inside = _identified([opening, _lot("s", "d", d, "5", "60"), _lot("a", "a", date(2026, 3, 31), "5", "70")])
        assert _rows(inside) == [(d, "bed_and_breakfast", date(2026, 3, 31), D("5"), D("70"))]
        outside = _identified([opening, _lot("s", "d", d, "5", "60"), _lot("a", "a", date(2026, 4, 1), "5", "70")])
        assert _rows(outside) == [(d, "s104_pool", None, D("5"), D("50"))]
        assert (outside.pool.units, outside.pool.cost_gbp) == (D("10"), D("120"))

    def test_elapse_instants_across_clock_changes(self) -> None:
        assert window_elapses_at(date(2026, 9, 23)) == datetime(2026, 10, 23, 23, tzinfo=UTC)  # BST
        assert window_elapses_at(date(2026, 11, 20)) == datetime(2026, 12, 21, tzinfo=UTC)  # GMT
        assert window_elapses_at(date(2026, 10, 10)) == datetime(2026, 11, 10, tzinfo=UTC)  # spans 2026-10-25

    def test_window_open_until_the_fetch_starts_at_elapse(self) -> None:
        lots = [_lot("a0", "a", date(2026, 9, 1), "10", "100"), _lot("s", "d", date(2026, 9, 23), "5", "60")]
        for fetch, is_open in (
            (datetime(2026, 10, 23, 22, 59, 59, tzinfo=UTC), True),
            (datetime(2026, 10, 23, 23, tzinfo=UTC), False),
            (None, True),
        ):
            r = _identified(lots, fetch)
            assert (WINDOW_OPEN in r.disposals[0].reasons) is is_open
            assert (r.pool.window_open_since == date(2026, 9, 23)) is is_open
            assert (WINDOW_OPEN in r.pool.reasons) is is_open

    def test_a_fully_same_day_disposal_has_no_window(self) -> None:
        d = date(2026, 9, 23)
        r = _identified([_lot("a", "a", d, "10", "100"), _lot("s", "d", d, "10", "90")], None)
        assert WINDOW_OPEN not in r.disposals[0].reasons
        assert (r.pool.window_open_since, WINDOW_OPEN in r.pool.reasons) == (None, False)

    def test_the_pool_keeps_the_window_through_depletion_and_replenishment(self) -> None:
        lots = [
            _lot("a0", "a", date(2026, 9, 1), "10", "100", {EXECUTION_ASSUMED}),
            _lot("s", "d", date(2026, 9, 23), "10", "120"),
            _lot("a1", "a", date(2026, 10, 24), "5", "60"),
        ]
        before = _identified(lots, datetime(2026, 10, 23, 22, tzinfo=UTC))
        assert (before.pool.units, before.pool.cost_gbp, before.pool.window_open_since) == (
            D("5"),
            D("60"),
            date(2026, 9, 23),
        )
        # The depleted contributor's reason left at zero; the window did not.
        assert before.pool.reasons == (HCU, IBA, OFE, WINDOW_OPEN)
        after = _identified(lots, datetime(2026, 10, 23, 23, tzinfo=UTC))
        assert (after.pool.window_open_since, after.pool.reasons) == (None, (HCU, IBA, OFE))

    def test_a_30_day_acquisition_across_5_april_keeps_the_disposals_tax_year(self) -> None:
        r = _identified(
            [
                _lot("a0", "a", date(2027, 1, 10), "10", "100"),
                _lot("s", "d", date(2027, 3, 20), "5", "90"),
                _lot("a1", "a", date(2027, 4, 10), "5", "80"),
            ]
        )
        assert _rows(r) == [(date(2027, 3, 20), "bed_and_breakfast", date(2027, 4, 10), D("5"), D("80"))]
        assert r.disposals[0].tax_year == "2026/27"

    def test_late_evidence_changes_the_earlier_figure(self) -> None:
        n = [_lot("a0", "a", date(2027, 1, 10), "10", "100"), _lot("s", "d", date(2027, 3, 20), "5", "90")]
        assert _identified(n).disposals[0].gain_gbp == D("40")  # from the pool at £10
        n1 = _identified([*n, _lot("a1", "a", date(2027, 4, 10), "5", "80")])
        assert n1.disposals[0].gain_gbp == D("10")  # 30-day at £16


# --- 8. missing amounts -----------------------------------------------------------------------------------------------


class TestMissingAmounts:
    def test_pending_proceeds(self) -> None:
        r = _identified([_lot("a", "a", date(2026, 1, 10), "10", "100"), _lot("s", "d", date(2026, 2, 20), "5", None)])
        (x,) = r.disposals
        assert (x.status, x.proceeds_gbp, x.cost_gbp, x.gain_gbp, x.missing_lot_keys) == (
            "amount_missing",
            None,
            D("50"),
            None,
            ("s",),
        )

    def test_a_pending_acquisition_inside_the_window(self) -> None:
        r = _identified(
            [
                _lot("a0", "a", date(2026, 1, 10), "10", "100"),
                _lot("s", "d", date(2026, 2, 20), "5", "60"),
                _lot("b", "a", date(2026, 3, 1), "5", None),
            ]
        )
        (x,) = r.disposals
        (i,) = x.identifications
        assert (i.rule, i.cost_gbp, i.missing_lot_keys) == ("bed_and_breakfast", None, ("b",))
        assert (x.status, x.missing_lot_keys) == ("amount_missing", ("b",))
        assert (r.pool.units, r.pool.cost_gbp, r.pool.missing_lot_keys) == (D("10"), D("100"), ())

    def test_mixed_defined_and_pending_lots_on_one_day(self) -> None:
        d = date(2026, 1, 10)
        r = _identified(
            [_lot("a1", "a", d, "5", "50"), _lot("a2", "a", d, "5", None), _lot("s", "d", date(2026, 3, 1), "4", "60")]
        )
        assert (r.disposals[0].cost_gbp, r.disposals[0].missing_lot_keys) == (None, ("a2",))
        assert (r.pool.units, r.pool.cost_gbp, r.pool.missing_lot_keys) == (D("6"), None, ("a2",))

    def test_a_pending_day_fully_consumed_same_day_leaves_the_pool_defined(self) -> None:
        d = date(2026, 1, 10)
        r = _identified(
            [_lot("a0", "a", date(2026, 1, 5), "10", "100"), _lot("b", "a", d, "5", None), _lot("s", "d", d, "5", "70")]
        )
        assert (r.disposals[0].status, r.disposals[0].missing_lot_keys) == ("amount_missing", ("b",))
        assert (r.pool.units, r.pool.cost_gbp, r.pool.missing_lot_keys) == (D("10"), D("100"), ())

    def test_a_pending_remainder_in_the_pool_until_depletion(self) -> None:
        r = _identified(
            [
                _lot("b", "a", date(2026, 1, 10), "10", None),
                _lot("s1", "d", date(2026, 2, 20), "4", "60"),
                _lot("s2", "d", date(2026, 3, 25), "6", "90"),
                _lot("c", "a", date(2026, 4, 30), "5", "50"),
            ]
        )
        assert [(x.cost_gbp, x.missing_lot_keys) for x in r.disposals] == [(None, ("b",)), (None, ("b",))]
        assert (r.pool.units, r.pool.cost_gbp, r.pool.missing_lot_keys) == (D("5"), D("50"), ())


# --- 9. reasons -------------------------------------------------------------------------------------------------------


class TestReasons:
    def test_a_partial_disposal_keeps_every_contributors_reason_until_zero(self) -> None:
        lots = [
            _lot("x", "a", date(2026, 1, 10), "10", "100", {EXECUTION_ASSUMED, SPREAD_INCLUSION_ASSUMED}),
            _lot("y", "a", date(2026, 1, 11), "10", "100", {SPREAD_INCLUSION_ASSUMED}),
            _lot("s1", "d", date(2026, 3, 1), "10", "120"),
        ]
        r = _identified(lots)
        assert r.pool.reasons == (EXECUTION_ASSUMED, HCU, IBA, OFE, SPREAD_INCLUSION_ASSUMED)
        assert r.disposals[0].identifications[0].reasons == (EXECUTION_ASSUMED, HCU, SPREAD_INCLUSION_ASSUMED)
        cleared = _identified([*lots, _lot("s2", "d", date(2026, 4, 15), "10", "130")])
        assert (cleared.pool.units, cleared.pool.reasons) == (D("0"), (HCU, IBA, OFE))

    def test_a_mixed_day_carries_its_union_into_every_use(self) -> None:
        d1, d = date(2026, 3, 1), date(2026, 3, 10)
        r = _identified(
            [
                _lot("x", "a", d, "6", "60", {EXECUTION_ASSUMED}),
                _lot("y", "a", d, "6", "60", {SYNC_CHARGES_UNOBSERVED}),
                _lot("s1", "d", d1, "4", "50"),
                _lot("s", "d", d, "3", "36"),
            ]
        )
        assert _rows(r) == [(d1, "bed_and_breakfast", d, D("4"), D("40")), (d, "same_day", d, D("3"), D("30"))]
        for x in r.disposals:
            assert {EXECUTION_ASSUMED, SYNC_CHARGES_UNOBSERVED} <= set(x.identifications[0].reasons)
        assert (r.pool.units, r.pool.cost_gbp) == (D("5"), D("50"))
        assert {EXECUTION_ASSUMED, SYNC_CHARGES_UNOBSERVED} <= set(r.pool.reasons)

    def test_a_disposal_takes_its_lots_its_identifications_and_the_slice_reasons(self) -> None:
        d = date(2026, 3, 1)
        r = _identified(
            [
                _lot("a", "a", d, "1", "10", {SPREAD_INCLUSION_ASSUMED}),
                _lot("s", "d", d, "1", "11", {DISPOSAL_FORMULA_ASSUMED}),
            ]
        )
        assert r.disposals[0].reasons == (DISPOSAL_FORMULA_ASSUMED, HCU, IBA, OFE, SPREAD_INCLUSION_ASSUMED)


# --- 10. classes ------------------------------------------------------------------------------------------------------


def _lot_row(
    key: str = "L1", direction: str = "acquisition", evidence: str | None = "lookup", instrument: int = 1, **kw: object
) -> LotRow:
    base: dict[str, object] = {
        "lot_key": key,
        "subject": "order:1",
        "instrument_id": instrument,
        "direction": direction,
        "uk_date": date(2026, 9, 23),
        "tax_year": "2026/27",
        "units": D("1"),
        "amount_gbp": D("10"),
        "execution_evidence": evidence,
        "fingerprint": "f" * 64,
    }
    base.update(kw)
    return LotRow(**base)  # type: ignore[arg-type]


def _subject(
    subject: str = "order:1",
    outcome: str = "admitted",
    instrument: int | None = 1,
    evidence: str | None = "lookup",
    unobserved: tuple[int, ...] | None = (),
    nonzero: tuple[int | None, ...] | None = (),
    positions: frozenset[int] = frozenset(),
) -> SubjectRow:
    return SubjectRow(subject, outcome, instrument, evidence, unobserved, nonzero, positions)


class TestClasses:
    @pytest.mark.parametrize(
        ("lot", "subject", "expected"),
        [
            (_lot_row(), _subject(), ("provisional", (SPREAD_INCLUSION_ASSUMED,))),
            (
                _lot_row(evidence="sync_only"),
                _subject(evidence="sync_only"),
                ("provisional", (EXECUTION_ASSUMED, LOOKUP_EVIDENCE_UNOBSERVED, SPREAD_INCLUSION_ASSUMED)),
            ),
            (_lot_row(direction="disposal"), _subject(), ("provisional", (DISPOSAL_FORMULA_ASSUMED,))),
            (
                _lot_row(direction="disposal"),
                _subject(unobserved=(5, 6)),
                ("provisional", (DISPOSAL_FORMULA_ASSUMED, SYNC_CHARGES_UNOBSERVED)),
            ),
            (_lot_row(), _subject(evidence="sync_only"), ("unavailable", ("execution_evidence_mismatch",))),
            (_lot_row(), _subject(unobserved=None), ("unavailable", ("total_fees_unobserved_null",))),
            (_lot_row(direction="dividend"), _subject(), ("unavailable", ("direction_invalid",))),
            (
                _lot_row(direction="dividend", evidence=None),
                _subject(unobserved=None),
                ("unavailable", ("direction_invalid", "execution_evidence_mismatch", "total_fees_unobserved_null")),
            ),
        ],
    )
    def test_the_3d1_table(self, lot: LotRow, subject: SubjectRow, expected: tuple[str, tuple[str, ...]]) -> None:
        c = classify_lot(lot, subject)
        assert (c.lot_class, c.reasons) == expected

    def test_a_subjects_unobserved_positions_change_the_class_and_the_digest_not_the_fingerprint(self) -> None:
        lot = _lot_row()
        before, after = _subject(), _subject(unobserved=(7,))
        assert classify_lot(lot, before).reasons != classify_lot(lot, after).reasons
        docs = [inputs_document(_inputs([lot], [s], {1: InstrumentRow(1, "SPY", None)})) for s in (before, after)]
        assert docs[0] != docs[1]

    def test_an_unavailable_lot_withholds_its_security_naming_the_field(self) -> None:
        result = compute_matching(
            _inputs([_lot_row()], [_subject(unobserved=None)], {1: InstrumentRow(1, "SPY", None)})
        )
        (security,) = result.securities
        assert (security.status, security.pool) == ("withheld", None)
        (dep,) = result.dependencies
        assert (dep.tax_security_id, dep.reason, dep.lot_key, dep.detail) == (
            1,
            "lot_unavailable",
            "L1",
            "total_fees_unobserved_null",
        )


# --- 11. the tax security ---------------------------------------------------------------------------------------------


AS_OF = datetime(2026, 10, 14, 12, tzinfo=UTC)
_CIK = "0000000001"
_COVER = "0000000001-26-000001"


def _share(symbol: str) -> SecurityEvidence:
    """Character inputs ``classify`` decides are a share's (3d-3a): one Stocks member, a current operating profile, and
    a newest cover listing the symbol as common stock; no acquisition, so no start cover is read."""
    return SecurityEvidence(
        types=frozenset({5}),
        ciks=frozenset({_CIK}),
        symbol=symbol,
        symbols_ever=frozenset({symbol}),
        profiles=(Profile("operating", "3571", AS_OF),),
        filings=(CoverFiling(_COVER, date(2020, 1, 2), "https://www.sec.gov/a.htm"),),
        first_acquisition=None,
        covers={_COVER: Cover(frozenset({_CIK}), frozenset({("Common Stock", symbol)}), frozenset())},
    )


def _inputs(
    lots: list[LotRow],
    subjects: list[SubjectRow],
    instruments: dict[int, InstrumentRow],
    fetch: datetime | None = LATE,
    characters: dict[int, SecurityEvidence] | None = None,
) -> MatchingInputs:
    """By default every security's character inputs are a share's, so these tests exercise identification."""
    bare = MatchingInputs(1, "broker_evidence_v3", fetch, tuple(subjects), tuple(lots), instruments, AS_OF, {})
    if characters is None:
        characters = {key: _share(instruments[key].symbol.upper()) for key in security_members(bare)}
    return MatchingInputs(1, "broker_evidence_v3", fetch, tuple(subjects), tuple(lots), instruments, AS_OF, characters)


def _rows_of(*rows: tuple[int, str, int | None]) -> dict[int, InstrumentRow]:
    return {i: InstrumentRow(i, s, c) for i, s, c in rows}


class TestMapping:
    def test_an_established_component_pools_a_bound_variant_with_its_base(self) -> None:
        lots = [
            _lot_row(
                "L1", instrument=1, units=D("10"), amount_gbp=D("100"), uk_date=date(2026, 1, 5), tax_year="2025/26"
            ),
            _lot_row("L2", "disposal", instrument=2, units=D("4"), amount_gbp=D("60")),
        ]
        result = compute_matching(_inputs(lots, [_subject()], _rows_of((1, "SPY", None), (2, "SPY.RTH", 1))))
        (s,) = result.securities
        assert (s.tax_security_id, s.instrument_ids, s.status) == (1, (1, 2), "computed")
        assert s.pool is not None and (s.pool.units, s.pool.cost_gbp) == (D("6"), D("60"))

    def test_a_lower_case_bound_variant_is_established(self) -> None:
        result = compute_matching(
            _inputs([_lot_row(instrument=41)], [_subject()], _rows_of((40, "SPY", None), (41, "spy.rth", 40)))
        )
        assert [(s.tax_security_id, s.status) for s in result.securities] == [(40, "computed")]

    @pytest.mark.parametrize(
        "rows",
        [
            ((1, "AAA", None), (2, "AAA.RTH", None)),  # an unbound variant beside its base
            ((1, "X", None), (2, "X.RTH", 99)),  # a pointer to a missing row
            ((1, "Y", None), (2, "Z.RTH", 1)),  # a symbol mismatch
        ],
    )
    def test_an_unestablished_component_withholds_each_member_with_evidence(
        self, rows: tuple[tuple[int, str, int | None], ...]
    ) -> None:
        result = compute_matching(_inputs([_lot_row(instrument=1)], [_subject()], _rows_of(*rows)))
        assert [(s.tax_security_id, s.instrument_ids, s.status) for s in result.securities] == [(1, (1,), "withheld")]
        assert [(d.reason, d.instrument_id, d.detail) for d in result.dependencies] == [
            ("identity_unestablished", 1, "component [1, 2]"),
            ("character_unestablished", None, "identity_unestablished"),
        ]
        assert [(s.character.character, s.character.detail) for s in result.securities] == [
            ("unestablished", "identity_unestablished")
        ]
        assert [p.tax_security_id for p in result.placements] == [None, None]

    def test_a_chain_withholds_everything_reachable(self) -> None:
        rows = _rows_of((10, "A", 11), (11, "B", 12), (12, "C", None))
        result = compute_matching(
            _inputs(
                [_lot_row(instrument=12)], [_subject(instrument=12), _subject("order:2", "unresolved", 10, None)], rows
            )
        )
        assert [(s.tax_security_id, s.status) for s in result.securities] == [(10, "withheld"), (12, "withheld")]
        assert {d.reason for d in result.dependencies} == {
            "identity_unestablished",
            "unresolved_subject",
            "character_unestablished",
        }
        assert sorted(p.instrument_id for p in result.placements) == [10, 11, 12]

    def test_a_lot_on_an_id_with_no_row_withholds_every_security(self) -> None:
        result = compute_matching(
            _inputs(
                [_lot_row("L1", instrument=1), _lot_row("L2", instrument=7)], [_subject()], _rows_of((1, "SPY", None))
            )
        )
        assert result.withheld_all
        assert [(s.tax_security_id, s.status) for s in result.securities] == [(1, "withheld")]
        assert [(d.tax_security_id, d.reason, d.lot_key) for d in result.dependencies] == [(None, "unplaceable", "L2")]


# --- 12. step 5 -------------------------------------------------------------------------------------------------------


def test_a_disposal_with_no_prior_acquisition_reaches_step_5_and_is_withheld() -> None:
    assert identify_security([_lot("s", "d", date(2026, 1, 1), "1", "10")], LATE) == StepFiveReached(
        date(2026, 1, 1), D("1")
    )
    result = compute_matching(_inputs([_lot_row(direction="disposal")], [_subject()], _rows_of((1, "SPY", None))))
    assert [(s.status, s.pool) for s in result.securities] == [("withheld", None)]
    (dep,) = result.dependencies
    assert (dep.reason, dep.detail) == ("subsequent_acquisition_rule", "disposal 2026-09-23: 1 units unidentified")


# --- 13. rounding and capacity ----------------------------------------------------------------------------------------


class TestRounding:
    def test_a_pool_drawn_in_thirds_conserves_cost_exactly(self) -> None:
        r = _identified(
            [
                _lot("a", "a", date(2026, 1, 1), "3", "10"),
                _lot("s1", "d", date(2026, 3, 1), "1", "4"),
                _lot("s2", "d", date(2026, 4, 1), "1", "4"),
                _lot("s3", "d", date(2026, 5, 1), "1", "4"),
            ]
        )
        # 10/3 rounds down; 6.66666666666667/2 = 3.333333333333335 is an exact tie and rounds to even (up, from 3);
        # the last draw takes the exact remainder.
        costs = [x.cost_gbp for x in r.disposals]
        assert costs == [D("3.33333333333333"), D("3.33333333333334"), D("3.33333333333333")]
        assert sum(c for c in costs if c is not None) == D("10")

    def test_half_even_on_exact_ties(self) -> None:
        quantum = D("1E-14")
        assert apportion(D("1"), quantum, D("2")) == D("0E-14")  # 0.5 quanta -> 0
        assert apportion(D("1"), 3 * quantum, D("2")) == D("2E-14")  # 1.5 quanta -> 2
        assert apportion(D("1"), 5 * quantum, D("2")) == D("2E-14")  # 2.5 quanta -> 2

    def test_the_near_tie_does_not_double_round(self) -> None:
        u = 10**24 - 1
        n = (u - 1) // 2
        c = (10**20 - 1) * u + 1
        assert share_quanta(n, c, u) == (10**20 - 1) * n

    def test_capacity(self) -> None:
        assert fits(D("9999999999999999.99999999"), 24, 8)
        assert not fits(D("10000000000000000"), 24, 8)
        assert not fits(D("1.000000001"), 24, 8)
        big = [
            _lot_row("L1", units=D("9000000000000000"), amount_gbp=D("1")),
            _lot_row("L2", units=D("9000000000000000"), amount_gbp=D("1")),
        ]
        with pytest.raises(ValueError, match="pool units"):
            compute_matching(_inputs(big, [_subject()], _rows_of((1, "SPY", None))))


# --- the inputs document (test 16, pure half) -------------------------------------------------------------------------


def test_the_inputs_document_is_pinned() -> None:
    """A missing instrument (7) and a subject with no lot (order:2) fix the document's bytes and SHA-256."""
    inputs = _inputs(
        [_lot_row("L1", instrument=1), _lot_row("L2", instrument=7)],
        [
            _subject(unobserved=(9, 3), nonzero=(3,), positions=frozenset({9, 3})),
            _subject("order:2", "unresolved", None, None),
        ],
        _rows_of((1, "SPY", None), (2, "SPY.RTH", 1), (5, "OTHER", None)),
    )
    document = inputs_document(inputs)
    assert document == (
        '{"character":[[1,[5],["0000000001"],"SPY",["SPY"],["operating","3571"],'
        '["0000000001-26-000001",[["0000000001"],[["Common Stock","SPY"]],[]]],null]],'
        '"evidence":[["L1","' + "f" * 64 + '"],["L2","' + "f" * 64 + '"]],'
        '"instruments":[[1,"SPY",null],[2,"SPY.RTH",1],[7,null,null]],'
        '"subjects":[["order:1","admitted",1,"lookup",[3,9],[3],[3,9]],'
        '["order:2","unresolved",null,null,[],[],[]]],'
        '"version":"matching_inputs_v3"}'
    )
    assert inputs_digest(document) == "8e263b6fdbf459f3e3ab2c291cc11c30897eea6cb1d191c48367c324d0acd65a"


# --- run 2, from the spec's own day totals (Facts 3; Acceptance) ------------------------------------------------------


def test_run_2_spy_replays_the_acceptance_figures() -> None:
    s = {SPREAD_INCLUSION_ASSUMED}
    r = _identified(
        [
            _lot("a0918", "a", date(2026, 9, 18), "0.29615500", "168.43050669140000", s),
            _lot("a0923", "a", date(2026, 9, 23), "0.29220900", "169.30267621620600", s),
            _lot("s0923", "d", date(2026, 9, 23), "0.29615500", "172.44628722262500", {DISPOSAL_FORMULA_ASSUMED}),
            _lot("a0930", "a", date(2026, 9, 30), "20.62672000", "11933.68460630607000", s),
            _lot("a1001", "a", date(2026, 10, 1), "0.13442600", "77.30850742070600", s),
            _lot("a1009", "a", date(2026, 10, 9), "0.61384700", "360.56710775200500", s),
        ],
        datetime(2026, 10, 14, 2, 14, 52, tzinfo=UTC),
    )
    assert _rows(r) == [
        (date(2026, 9, 23), "same_day", date(2026, 9, 23), D("0.29220900"), D("169.30267621620600")),
        (date(2026, 9, 23), "bed_and_breakfast", date(2026, 9, 30), D("0.00394600"), D("2.28297661753705")),
    ]
    (x,) = r.disposals
    assert (x.cost_gbp, x.gain_gbp) == (D("171.58565283374305"), D("0.86063438888195"))
    assert x.reasons == (DISPOSAL_FORMULA_ASSUMED, HCU, IBA, OFE, SPREAD_INCLUSION_ASSUMED, WINDOW_OPEN)
    assert (r.pool.units, r.pool.cost_gbp) == (D("21.66720200"), D("12537.70775155264395"))
    assert (r.pool.reasons, r.pool.window_open_since) == (
        (HCU, IBA, OFE, SPREAD_INCLUSION_ASSUMED, WINDOW_OPEN),
        date(2026, 9, 23),
    )


# --- 20. the jobs ----------------------------------------------------------------------------------------------------


_COVERS_OK = CoverRefreshReport(2, 1, 1, 0, ())


def _stub_jobs(
    monkeypatch: pytest.MonkeyPatch, evidence: Any, matching: MatchingRunReport, covers: CoverRefreshReport = _COVERS_OK
) -> list[str]:
    from app.workers import scheduler

    calls: list[str] = []

    @contextmanager
    def tracked(_job_name: str) -> Iterator[SimpleNamespace]:
        yield SimpleNamespace(row_count=None, note=None)

    @contextmanager
    def connected(**_kwargs: object) -> Iterator[MagicMock]:
        yield MagicMock()

    class _Broker:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> _Broker:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    def run_matching(_conn: object) -> MatchingRunReport:
        calls.append("matching")
        return matching

    def refresh_covers(_conn: object) -> CoverRefreshReport:
        calls.append("covers")
        return covers

    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", connected)
    monkeypatch.setattr(scheduler, "_load_etoro_credentials", lambda _name: ("api", "user"))
    monkeypatch.setattr("app.providers.implementations.etoro_broker.EtoroBrokerProvider", _Broker)
    monkeypatch.setattr("app.services.tax_broker_evidence.run_tax_broker_evidence", evidence)
    monkeypatch.setattr("app.services.tax_matching.run_tax_matching", run_matching)
    monkeypatch.setattr("app.services.tax_matching.refresh_cover_instances", refresh_covers)
    return calls


def _evidence(state: str, reason: str | None = None) -> Any:
    return lambda *_args: EvidenceRunReport(state=state, run_id=5, reason=reason)  # type: ignore[arg-type]


class TestJobs:
    def test_an_evidence_refusal_still_runs_matching_and_fails_the_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.workers import scheduler

        calls = _stub_jobs(
            monkeypatch, _evidence("refused", "scope_mismatch"), MatchingRunReport("already_published", 3, 4)
        )
        with pytest.raises(RuntimeError, match=r"refused: scope_mismatch.*matching=already_published"):
            scheduler.tax_broker_evidence_refresh()
        assert calls == ["covers", "matching"]

    def test_an_evidence_exception_still_runs_matching_and_fails_the_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.workers import scheduler

        def boom(*_args: object) -> None:
            raise OSError("broker down")

        calls = _stub_jobs(monkeypatch, boom, MatchingRunReport("published", 3, 4))
        with pytest.raises(RuntimeError, match=r"evidence raised OSError: broker down.*matching=published"):
            scheduler.tax_broker_evidence_refresh()
        assert calls == ["covers", "matching"]

    def test_a_matching_refusal_fails_the_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.workers import scheduler

        refused = MatchingRunReport("refused", 3, 4, "evidence_policy_unsupported")
        _stub_jobs(monkeypatch, _evidence("published"), refused)
        with pytest.raises(RuntimeError, match="matching=refused"):
            scheduler.tax_broker_evidence_refresh()
        with pytest.raises(RuntimeError, match="evidence_policy_unsupported"):
            scheduler.daily_tax_reconciliation()

    def test_both_steps_succeeding_pass(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.workers import scheduler

        calls = _stub_jobs(monkeypatch, _evidence("published"), MatchingRunReport("published", 3, 4))
        scheduler.tax_broker_evidence_refresh()
        _stub_jobs(monkeypatch, _evidence("skipped", "another run holds the lock"), MatchingRunReport("no_generation"))
        scheduler.tax_broker_evidence_refresh()
        scheduler.daily_tax_reconciliation()
        assert calls == ["covers", "matching"]

    def test_a_failed_cover_still_runs_matching_and_fails_both_jobs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.workers import scheduler

        failed = CoverRefreshReport(2, 0, 1, 0, ("0000000001-26-000002: http 503",))
        calls = _stub_jobs(monkeypatch, _evidence("published"), MatchingRunReport("published", 3, 4), failed)
        with pytest.raises(RuntimeError, match=r"failures=\['0000000001-26-000002: http 503'\].*matching=published"):
            scheduler.tax_broker_evidence_refresh()
        with pytest.raises(RuntimeError, match=r"http 503.*matching=published"):
            scheduler.daily_tax_reconciliation()
        assert calls == ["covers", "matching", "covers", "matching"]


# --- slice 3d-3b: A8 (distribution_character_assumed) -----------------------------------------------------------------


def _a8_lot(key: str, subject: str, direction: str, day: date, units: str, gbp: str) -> LotRow:
    year = "2025/26" if day < date(2026, 4, 6) else "2026/27"
    return _lot_row(key, direction, subject=subject, uk_date=day, tax_year=year, units=D(units), amount_gbp=D(gbp))


class TestDistributionCharacter:
    def test_a8_on_acquisitions_with_income_only(self) -> None:
        income = _subject(nonzero=(21,), positions=frozenset({21}))
        assert DISTRIBUTION_CHARACTER_ASSUMED in classify_lot(_lot_row(), income).reasons
        assert DISTRIBUTION_CHARACTER_ASSUMED not in classify_lot(_lot_row(direction="disposal"), income).reasons
        assert (
            DISTRIBUTION_CHARACTER_ASSUMED not in classify_lot(_lot_row(), _subject(positions=frozenset({21}))).reasons
        )

    @pytest.mark.parametrize(
        "nonzero",
        [None, (None,), (0,), (-1,), (22, 21), (21, 21), (99,)],
    )
    def test_an_invalid_array_makes_the_lot_unavailable(self, nonzero: tuple[int | None, ...] | None) -> None:
        subject = _subject(nonzero=nonzero, positions=frozenset({21, 22}))
        c = classify_lot(_lot_row(), subject)
        assert (c.lot_class, c.reasons) == ("unavailable", ("total_fees_nonzero_invalid",))
        result = compute_matching(_inputs([_lot_row()], [subject], {1: InstrumentRow(1, "SPY", None)}))
        assert result.securities[0].status == "withheld"

    def test_a8_reaches_a_pooled_gain_of_another_order_and_stays_on_the_pool(self) -> None:
        lots = [
            _a8_lot("Y1", "order:Y", "acquisition", date(2026, 1, 5), "10", "100"),
            _a8_lot("X1", "order:X", "acquisition", date(2026, 3, 2), "10", "120"),
            _a8_lot("X2", "order:X", "disposal", date(2026, 6, 1), "5", "70"),
        ]
        subjects = [
            _subject("order:Y", nonzero=(31,), positions=frozenset({31})),
            _subject("order:X", positions=frozenset({41})),
        ]
        result = compute_matching(_inputs(lots, subjects, {1: InstrumentRow(1, "SPY", None)}))
        (security,) = result.securities
        assert security.status == "computed"
        (disposal,) = security.disposals
        assert DISTRIBUTION_CHARACTER_ASSUMED in disposal.reasons
        assert security.pool is not None and DISTRIBUTION_CHARACTER_ASSUMED in security.pool.reasons

    def test_a_release_without_propagation(self) -> None:
        lots = [
            _a8_lot("X1", "order:X", "acquisition", date(2026, 1, 5), "10", "100"),
            _a8_lot("X2", "order:X", "disposal", date(2026, 1, 5), "10", "101"),
            _a8_lot("Y1", "order:Y", "acquisition", date(2026, 6, 1), "10", "130"),
        ]
        subjects = [
            _subject("order:X", positions=frozenset({41})),
            _subject("order:Y", nonzero=(31,), positions=frozenset({31})),
        ]
        result = compute_matching(_inputs(lots, subjects, {1: InstrumentRow(1, "SPY", None)}))
        (security,) = result.securities
        (disposal,) = security.disposals
        assert security.status == "computed"
        assert DISTRIBUTION_CHARACTER_ASSUMED not in disposal.reasons

    def test_a_release_blocked_by_another_unresolved_subject(self) -> None:
        lots = [_a8_lot("Y1", "order:Y", "acquisition", date(2026, 6, 1), "10", "130")]
        subjects = [
            _subject("order:Y", nonzero=(31,), positions=frozenset({31})),
            _subject("order:Z", "unresolved", 1, None),
        ]
        result = compute_matching(_inputs(lots, subjects, {1: InstrumentRow(1, "SPY", None)}))
        assert result.securities[0].status == "withheld"
