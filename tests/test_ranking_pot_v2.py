"""Ranking-pot-v2 pure core (#3592 slice 2): spec §2, §5, §6 and Appendix A's slice-2 table tests."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from app.services import ranking_pot as pot
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_v2 as v2

AS_OF = datetime(2026, 10, 5, 13, 50, tzinfo=UTC)
TARGET = date(2026, 10, 5)  # window: 2026-04-01 .. 2026-10-01
FILER, ISSUER = "0000000001", "0000000100"
FULL_HISTORY = date(2020, 1, 1)  # no month hidden for the years these tests use

_next_id = iter(range(1, 1_000_000))


def _row(
    *,
    txn_date: date,
    filed_at: datetime | None = None,
    code: str | None = "P",
    ad: str | None = "A",
    iid: int = 7,
    filer: str | None = FILER,
    issuer: str | None = ISSUER,
    derivative: bool = False,
    invalid: bool = False,
) -> v2.InsiderRow:
    n = next(_next_id)
    return v2.InsiderRow(
        txn_id=n,
        accession=f"0000000000-26-{n:06d}",
        instrument_id=iid,
        filer_cik=filer,
        issuer_cik=issuer,
        txn_date=txn_date,
        txn_code=code,
        acquired_disposed_code=ad,
        is_derivative=derivative,
        txn_date_invalid=invalid,
        filed_at=filed_at or datetime(txn_date.year, txn_date.month, txn_date.day, 20, tzinfo=UTC),
    )


def _history(months_by_year: dict[int, list[int]], **kw: object) -> list[v2.InsiderRow]:
    """Open-market sales in the given (year → months), each filed the evening of its trade."""
    return [
        _row(txn_date=date(y, m, 10), code="S", ad="D", **kw)  # type: ignore[arg-type]
        for y, months in months_by_year.items()
        for m in months
    ]


def _read(purchases: list[v2.InsiderRow], history: list[v2.InsiderRow], **kw: object) -> v2.InsiderRead:
    args: dict[str, object] = {
        "s0_ids": {7, 8, 9},
        "target_session": TARGET,
        "as_of": AS_OF,
        "history_floor": FULL_HISTORY,
    } | kw
    return v2.insider_read(purchases, history, **args)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Look-back window
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (date(2026, 10, 5), (date(2026, 4, 1), date(2026, 10, 1))),
        (date(2026, 1, 2), (date(2025, 7, 1), date(2026, 1, 1))),  # January: the window is all last year
        (date(2026, 7, 1), (date(2026, 1, 1), date(2026, 7, 1))),
        (date(2026, 2, 3), (date(2025, 8, 1), date(2026, 2, 1))),  # straddles December/January
    ],
)
def test_lookback_window(target: date, expected: tuple[date, date]) -> None:
    assert v2.lookback_window(target) == expected


# ---------------------------------------------------------------------------
# CMP classification
# ---------------------------------------------------------------------------
def _classify(cells: list[tuple[int, int]], floor: date = FULL_HISTORY) -> v2.Classification:
    return v2.classify(cells, 2026, history_floor=floor)


def test_classify_cases() -> None:
    assert _classify([(2023, 3), (2024, 3), (2025, 3)]) == "routine"
    assert _classify([(2023, 3), (2024, 4), (2025, 5)]) == "opportunistic"  # three years, no common month
    assert _classify([(2023, 3), (2025, 3)]) == "unclassifiable"  # 2024 missing
    assert _classify([(2022, 3), (2023, 3), (2024, 3), (2025, 3)]) == "routine"  # 2022 is outside the window
    assert _classify([(2023, 3), (2024, 3), (2026, 3)]) == "unclassifiable"  # the purchase year never counts
    assert _classify([(2023, 1), (2023, 3), (2024, 3), (2024, 7), (2025, 3)]) == "routine"


TRUNCATED = date(2023, 6, 1)  # 2023-01..05 unobservable


@pytest.mark.parametrize(
    ("cells", "expected"),
    [
        # 2024 and 2025 share March, hidden in 2023: routine on some filling, opportunistic on another.
        ([(2023, 8), (2024, 3), (2025, 3)], "indeterminate"),
        # A shared month observed in all three years is routine whatever is hidden.
        ([(2023, 8), (2024, 8), (2025, 8)], "routine"),
        # The later years share only an OBSERVABLE month absent in 2023: opportunistic on every filling.
        ([(2023, 9), (2024, 8), (2025, 8)], "opportunistic"),
        # The later years share no month at all: opportunistic on every filling.
        ([(2023, 9), (2024, 2), (2025, 4)], "opportunistic"),
        # No observable 2023 trade: classified only if a hidden month had one — undecidable.
        ([(2024, 2), (2025, 4)], "indeterminate"),
        # A later year with no trade is unclassifiable on every filling.
        ([(2023, 9), (2025, 4)], "unclassifiable"),
    ],
)
def test_classify_under_a_truncated_corpus(cells: list[tuple[int, int]], expected: str) -> None:
    assert _classify(cells, TRUNCATED) == expected


def test_classify_truncation_edges() -> None:
    # A trade observed BEFORE the floor is fixed evidence: March 2023 makes this pair routine.
    assert _classify([(2023, 3), (2024, 3), (2025, 3)], TRUNCATED) == "routine"
    # A floor after every history year hides all of them: nothing is decidable unless some year is provably empty.
    assert _classify([(2024, 3)], date(2026, 1, 1)) == "indeterminate"
    # An empty, fully observable later year is unclassifiable whatever the hidden first year holds.
    assert _classify([(2024, 3)], date(2023, 6, 1)) == "unclassifiable"
    with pytest.raises(ValueError, match="month out of range"):
        _classify([(2023, 13), (2024, 13), (2025, 13)])


def _months(start: date, counts: list[int]) -> dict[date, int]:
    out: dict[date, int] = {}
    y, m = start.year, start.month
    for n in counts:
        out[date(y, m, 1)] = n
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def test_history_floor_is_the_month_after_the_runs_first_month() -> None:
    # 2022-01..2023-05 sparse tail, 2023-06 partial, then dense to 2026-09; freeze month 2026-10.
    counts = _months(date(2022, 1, 1), [20] * 17 + [4000] + [8000] * 39)
    assert max(counts) == date(2026, 9, 1)
    assert v2.history_floor(counts, freeze_month=date(2026, 10, 1)) == date(2023, 7, 1)
    # A gap month inside the reference window breaks the run: the floor moves after it.
    gapped = {**counts, date(2025, 2, 1): 10}
    assert v2.history_floor(gapped, freeze_month=date(2026, 10, 1)) == date(2025, 4, 1)


def test_history_floor_refusals() -> None:
    assert v2.history_floor({}, freeze_month=date(2026, 10, 1)) == "history_floor_unavailable"
    stale = _months(date(2023, 1, 1), [8000] * 40 + [0])  # last complete month below threshold
    assert v2.history_floor(stale, freeze_month=date(2026, 6, 1)) == "history_floor_unavailable"
    with pytest.raises(ValueError, match="month starts"):
        v2.history_floor({date(2026, 1, 2): 1}, freeze_month=date(2026, 10, 1))


def test_guarded_months_are_the_live_history_windows_from_the_floor() -> None:
    frozen = _months(date(2023, 1, 1), [100] * 45)  # 2023-01 .. 2026-09
    floor = date(2023, 7, 1)
    # Target 2026-10-05: window 2026-04..09, purchase year 2026 → history 2023–2025, from the floor.
    months = v2.guarded_months(frozen, history_floor=floor, target_session=date(2026, 10, 5))
    assert (months[0], months[-1], len(months)) == (date(2023, 7, 1), date(2025, 12, 1), 30)
    # Target 2027-01-05: window 2026-07..12, still purchase year 2026 only.
    assert v2.guarded_months(frozen, history_floor=floor, target_session=date(2027, 1, 5)) == months
    # Target 2027-03-02: window 2026-09..2027-02 spans 2026 and 2027 → history 2023–2026.
    spanning = v2.guarded_months(frozen, history_floor=floor, target_session=date(2027, 3, 2))
    assert (spanning[0], spanning[-1]) == (date(2023, 7, 1), date(2026, 9, 1))
    # Target 2027-08-02: window 2027-02..07, purchase year 2027 → 2024–2026: 2023 has aged out.
    aged = v2.guarded_months(frozen, history_floor=floor, target_session=date(2027, 8, 2))
    assert aged[0] == date(2024, 1, 1)


def test_history_floor_moved_names_months_below_95_percent() -> None:
    frozen = {date(2024, 1, 1): 100, date(2024, 2, 1): 100, date(2024, 3, 1): 100, date(2023, 6, 1): 100}
    live = {date(2024, 1, 1): 95, date(2024, 2, 1): 94, date(2023, 6, 1): 0}
    moved = v2.history_floor_moved(frozen, live, history_floor=date(2023, 7, 1), target_session=date(2026, 10, 5))
    # 95 is not below 95%; an absent month counts 0; a month before the floor is never checked.
    assert moved == (date(2024, 2, 1), date(2024, 3, 1))
    assert (
        v2.history_floor_moved(frozen, frozen, history_floor=date(2023, 7, 1), target_session=date(2026, 10, 5)) == ()
    )


def test_floor_at_the_first_years_start_is_cmp_exactly() -> None:
    cells = [(2023, 8), (2024, 3), (2025, 3)]
    assert _classify(cells, date(2023, 1, 1)) == _classify(cells) == "opportunistic"
    with pytest.raises(ValueError, match="month start"):
        _classify(cells, date(2023, 6, 2))


def test_indeterminate_pair_is_excluded_and_counted() -> None:
    hist = _history({2023: [8], 2024: [3], 2025: [3]})
    read = _read([_row(txn_date=date(2026, 5, 4))], hist, history_floor=TRUNCATED)
    assert read.buyers == frozenset()
    assert (read.indeterminate_pairs, read.indeterminate_names, read.unclassifiable_pairs) == (1, 1, 0)


def test_routine_pair_is_not_a_buyer() -> None:
    hist = _history({2023: [3], 2024: [3], 2025: [3]})
    read = _read([_row(txn_date=date(2026, 5, 4))], hist)
    assert read.buyers == frozenset()
    assert set(read.pair_class.values()) == {"routine"}


def test_opportunistic_pair_is_a_buyer_with_its_accession_and_cells() -> None:
    hist = _history({2023: [3], 2024: [4], 2025: [5]})
    buy = _row(txn_date=date(2026, 5, 4))
    read = _read([buy], hist)
    assert read.buyers == frozenset({7})
    assert read.qualifying == {7: (buy.accession,)}
    assert read.pair_cells[(FILER, ISSUER, 2026)] == ((2023, 3), (2024, 4), (2025, 5))


def test_unclassifiable_pair_is_excluded_and_counted() -> None:
    hist = _history({2023: [3], 2025: [5]})
    read = _read([_row(txn_date=date(2026, 5, 4))], hist)
    assert read.buyers == frozenset()
    assert (read.unclassifiable_pairs, read.unclassifiable_names) == (1, 1)


def test_routine_and_opportunistic_pairs_on_one_issuer_make_a_buyer() -> None:
    other = "0000000002"
    hist = _history({2023: [3], 2024: [3], 2025: [3]}) + _history({2023: [1], 2024: [6], 2025: [9]}, filer=other)
    read = _read([_row(txn_date=date(2026, 5, 4)), _row(txn_date=date(2026, 6, 4), filer=other)], hist)
    assert read.buyers == frozenset({7})
    assert read.pair_class == {(FILER, ISSUER, 2026): "routine", (other, ISSUER, 2026): "opportunistic"}


def test_issuer_is_part_of_the_key() -> None:
    """The owner's routine history at ANOTHER issuer does not classify this pair."""
    hist = _history({2023: [3], 2024: [3], 2025: [3]}, issuer="0000000999")
    read = _read([_row(txn_date=date(2026, 5, 4))], hist)
    assert read.pair_class == {(FILER, ISSUER, 2026): "unclassifiable"}


def test_purchase_filed_after_as_of_is_ignored() -> None:
    hist = _history({2023: [3], 2024: [4], 2025: [5]})
    late = _row(txn_date=date(2026, 9, 30), filed_at=datetime(2026, 10, 5, 14, tzinfo=UTC))
    read = _read([late], hist)
    assert read.buyers == frozenset()
    assert read.purchases_in_window == 0


def test_purchase_outside_the_window_or_outside_s0_is_ignored() -> None:
    hist = _history({2023: [3], 2024: [4], 2025: [5]})
    rows = [
        _row(txn_date=date(2026, 3, 31)),  # before the window
        _row(txn_date=date(2026, 10, 1), filed_at=datetime(2026, 10, 2, tzinfo=UTC)),  # target month
        _row(txn_date=date(2026, 5, 4), iid=99),  # not in S₀
    ]
    assert _read(rows, hist).purchases_in_window == 0


def test_history_filing_cut_is_strictly_before_1_january_utc() -> None:
    """A 2025 trade filed at 2026-01-01T00:00Z was not public at the classification date; one filed a second earlier
    was."""
    base = _history({2023: [3], 2024: [4]})
    on_cut = _row(txn_date=date(2025, 12, 30), code="S", ad="D", filed_at=datetime(2026, 1, 1, tzinfo=UTC))
    before = replace(on_cut, txn_id=on_cut.txn_id + 500_000, filed_at=datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC))
    buy = _row(txn_date=date(2026, 5, 4))
    assert _read([buy], [*base, on_cut]).pair_class[(FILER, ISSUER, 2026)] == "unclassifiable"
    assert _read([buy], [*base, before]).pair_class[(FILER, ISSUER, 2026)] == "opportunistic"


def test_late_filed_history_cannot_make_a_pair_routine() -> None:
    hist = _history({2023: [3], 2024: [3], 2025: [4]})
    late = _row(txn_date=date(2025, 3, 10), code="S", ad="D", filed_at=datetime(2026, 2, 1, tzinfo=UTC))
    read = _read([_row(txn_date=date(2026, 5, 4))], [*hist, late])
    assert read.pair_class[(FILER, ISSUER, 2026)] == "opportunistic"


def test_multiple_purchase_years_are_classified_separately() -> None:
    """Target 2026-02: a December 2025 buy is classified on 2022-24, a January 2026 buy on 2023-25."""
    hist = _history({2022: [12], 2023: [12], 2024: [12], 2025: [1]})
    dec, jan = _row(txn_date=date(2025, 12, 3)), _row(txn_date=date(2026, 1, 6))
    read = _read([dec, jan], hist, target_session=date(2026, 2, 3), as_of=datetime(2026, 2, 3, 15, tzinfo=UTC))
    assert read.pair_class == {(FILER, ISSUER, 2025): "routine", (FILER, ISSUER, 2026): "opportunistic"}
    assert read.qualifying == {7: (jan.accession,)}


def test_null_direction_flags_never_qualify() -> None:
    hist = _history({2023: [3], 2024: [4], 2025: [5]})
    assert _read([_row(txn_date=date(2026, 5, 4), ad=None)], hist).purchases_in_window == 0
    assert _read([_row(txn_date=date(2026, 5, 4), code=None)], hist).purchases_in_window == 0
    # A history sale with no direction flag is not an open-market trade: 2025 goes missing.
    flagless = [*_history({2023: [3], 2024: [4]}), _row(txn_date=date(2025, 5, 10), code="S", ad=None)]
    assert _read([_row(txn_date=date(2026, 5, 4))], flagless).pair_class[(FILER, ISSUER, 2026)] == "unclassifiable"


def test_derivative_and_invalid_dated_rows_never_count() -> None:
    hist = _history({2023: [3], 2024: [4], 2025: [5]})
    assert _read([_row(txn_date=date(2026, 5, 4), derivative=True)], hist).purchases_in_window == 0
    assert _read([_row(txn_date=date(2026, 5, 4), invalid=True)], hist).purchases_in_window == 0
    gap = [*_history({2023: [3], 2024: [4]}), _row(txn_date=date(2025, 5, 10), code="S", ad="D", invalid=True)]
    assert _read([_row(txn_date=date(2026, 5, 4))], gap).pair_class[(FILER, ISSUER, 2026)] == "unclassifiable"


def test_null_cik_purchase_is_excluded_and_counted() -> None:
    read = _read([_row(txn_date=date(2026, 5, 4), filer=None), _row(txn_date=date(2026, 5, 5), issuer=None)], [])
    assert (read.purchases_in_window, read.excluded_null_cik, read.buyers) == (2, 2, frozenset())


def test_amendment_rows_are_additive() -> None:
    """A 4/A repeating the original's purchase adds a second accession, never a second indicator unit."""
    hist = _history({2023: [3], 2024: [4], 2025: [5]})
    original = _row(txn_date=date(2026, 5, 4))
    amendment = _row(txn_date=date(2026, 5, 4), filed_at=datetime(2026, 6, 1, tzinfo=UTC))
    read = _read([original, amendment], hist)
    assert read.buyers == frozenset({7})
    assert read.qualifying[7] == tuple(sorted((original.accession, amendment.accession)))


# ---------------------------------------------------------------------------
# Days-to-cover selection and gates
# ---------------------------------------------------------------------------
def _dtc(
    iid: int, settle: date, value: str | None, known: datetime, src: str = "FINRA_SI_X", adv: int | None = 1000
) -> v2.DtcRow:
    return v2.DtcRow(iid, settle, None if value is None else Decimal(value), known, src, adv)


S1, S2 = date(2026, 9, 15), date(2026, 9, 30)
K1, K2 = datetime(2026, 9, 25, 12, tzinfo=UTC), datetime(2026, 10, 3, 12, tzinfo=UTC)


def test_dtc_takes_the_latest_settlement_observed_by_as_of() -> None:
    rows = [_dtc(7, S1, "3.5", K1), _dtc(7, S2, "4", K2), _dtc(8, S1, "2", K1)]
    read = v2.dtc_read(rows, s0_ids={7, 8, 9}, as_of=AS_OF)
    assert read.settlement_date == S2
    assert read.values == {7: Fraction(4)}
    assert read.missing == {8: "no_row", 9: "no_row"}  # 8's older settlement is never used


def test_dtc_ignores_unobserved_and_future_rows() -> None:
    rows = [
        _dtc(7, S1, "3.5", K1),
        _dtc(7, S2, "4", datetime(2026, 10, 5, 14, tzinfo=UTC)),  # observed after as_of
        _dtc(8, date(2026, 10, 15), "1", K1),  # settlement after as_of's date
    ]
    read = v2.dtc_read(rows, s0_ids={7, 8}, as_of=AS_OF)
    assert (read.settlement_date, read.values) == (S1, {7: Fraction(7, 2)})


def test_dtc_ignores_names_outside_s0_when_choosing_s_star() -> None:
    read = v2.dtc_read([_dtc(7, S1, "2", K1), _dtc(99, S2, "2", K2)], s0_ids={7}, as_of=AS_OF)
    assert read.settlement_date == S1


def test_dtc_latest_revision_wins_and_an_unusable_one_is_not_resurrected() -> None:
    later = datetime(2026, 10, 4, 12, tzinfo=UTC)
    rows = [
        _dtc(7, S2, "4", K2),
        _dtc(7, S2, None, later, "FINRA_SI_Y"),
        _dtc(8, S2, "5", K2),
        _dtc(8, S2, "-1", later, "FINRA_SI_Y"),
        _dtc(9, S2, "NaN", K2),
    ]
    read = v2.dtc_read(rows, s0_ids={7, 8, 9}, as_of=AS_OF)
    assert read.values == {}
    assert read.missing == {7: "null", 8: "negative", 9: "non_finite"}


def test_dtc_equal_known_from_breaks_by_source_document_id() -> None:
    rows = [_dtc(7, S2, "4", K2, "FINRA_SI_B"), _dtc(7, S2, "9", K2, "FINRA_SI_A")]
    read = v2.dtc_read(rows, s0_ids={7}, as_of=AS_OF)
    assert read.values == {7: Fraction(4)}
    assert read.chosen[7].source_document_id == "FINRA_SI_B"


def test_dtc_finra_not_available_and_default_zero_are_missing() -> None:
    rows = [
        _dtc(7, S2, "999.99", K2, adv=0),  # FINRA N/A, encoded 999.99
        _dtc(8, S2, "999.99", K2, adv=None),  # NULL ADV reads as zero
        _dtc(9, S2, "0", K2),  # never a formula value
        _dtc(10, S2, "999.99", K2, adv=3),  # a capped finite ratio stays usable
        _dtc(11, S2, "0", K2, adv=0),  # zero ADV takes precedence
    ]
    read = v2.dtc_read(rows, s0_ids={7, 8, 9, 10, 11}, as_of=AS_OF)
    assert read.values == {10: Fraction(99999, 100)}
    assert read.missing == {7: "not_available", 8: "not_available", 9: "default_zero", 11: "not_available"}


def test_dtc_no_rows() -> None:
    read = v2.dtc_read([], s0_ids={7}, as_of=AS_OF)
    assert (read.settlement_date, read.missing) == (None, {7: "no_row"})
    assert v2.dtc_gate(read, as_of=AS_OF, baseline=1) == "dtc_unavailable"


def test_dtc_gates() -> None:
    fresh = v2.dtc_read([_dtc(i, S2, "2", K2) for i in range(95)], s0_ids=range(100), as_of=AS_OF)
    assert v2.dtc_gate(fresh, as_of=AS_OF, baseline=100) is None  # 95 of 100 is exactly the floor
    assert v2.dtc_gate(fresh, as_of=AS_OF, baseline=101) == "dtc_incomplete"
    stale = v2.dtc_read([_dtc(7, date(2026, 9, 3), "2", K1)], s0_ids={7}, as_of=AS_OF)
    assert v2.dtc_gate(stale, as_of=AS_OF, baseline=1) == "dtc_unavailable"  # 32 days old
    edge = v2.dtc_read([_dtc(7, date(2026, 9, 4), "2", K1)], s0_ids={7}, as_of=AS_OF)
    assert v2.dtc_gate(edge, as_of=AS_OF, baseline=1) is None  # 31 days old


# ---------------------------------------------------------------------------
# Percentiles, composite, orders
# ---------------------------------------------------------------------------
def test_midrank_ties_and_missing() -> None:
    p = v2.midrank_percentiles({1: Fraction(1), 2: Fraction(2), 3: Fraction(2), 4: Fraction(5)}, {1, 2, 3, 4, 5})
    assert p == {1: Fraction(1, 8), 2: Fraction(1, 2), 3: Fraction(1, 2), 4: Fraction(7, 8), 5: Fraction(1, 2)}


def test_midrank_all_missing_and_all_equal_are_one_half() -> None:
    assert set(v2.midrank_percentiles({}, {1, 2}).values()) == {Fraction(1, 2)}
    assert set(v2.midrank_percentiles({1: Fraction(3), 2: Fraction(3)}, {1, 2}).values()) == {Fraction(1, 2)}


def _universes(scores: dict[int, str]) -> pot.Universes:
    r = {i: Decimal(s) for i, s in scores.items()}
    return pot.Universes(
        breakpoint=Decimal(1),
        max_cut=Fraction(1, 10),
        r_ids=frozenset(r),
        f_ids=frozenset(r),
        hold_failure={},
        entry_failure={},
        own_score=r,
        max_return={},
        atr={},
        max_population=0,
        nyse_cap_population=0,
    )


def test_composite_values_and_buyer_bonus() -> None:
    u = _universes({1: "0.9", 2: "0.5", 3: "0.1"})
    c = v2.composite_scores(u.r_ids, u.own_score, {1: Fraction(10), 2: Fraction(1)}, frozenset({3}))
    # u_score 5/6, 1/2, 1/6; u_dtc (−DTC) 1/4, 3/4, 1/2 (3 missing); u_ins 1/3, 1/3, 5/6
    assert c == {
        1: (Fraction(5, 6) + Fraction(1, 4) + Fraction(1, 3)) / 3,
        2: (Fraction(1, 2) + Fraction(3, 4) + Fraction(1, 3)) / 3,
        3: (Fraction(1, 6) + Fraction(1, 2) + Fraction(5, 6)) / 3,
    }
    assert v2.real_order(u, {1: Fraction(10), 2: Fraction(1)}, frozenset({3})) == (2, 3, 1)


def test_buyer_bonus_is_one_sixth_whatever_the_prevalence() -> None:
    u = _universes({i: "0.5" for i in range(1, 11)})
    for buyers in (frozenset({1}), frozenset(range(1, 10))):
        c = v2.composite_scores(u.r_ids, u.own_score, {}, buyers)
        assert c[min(buyers)] - c[10] == Fraction(1, 6)


def test_exact_composite_ties_break_by_instrument_id() -> None:
    u = _universes({4: "0.9", 2: "0.1"})
    # 4: (3/4 + 1/4 + 1/2)/3 ; 2: (1/4 + 3/4 + 1/2)/3 — equal exactly
    assert v2.real_order(u, {4: Fraction(9), 2: Fraction(1)}, frozenset()) == (2, 4)


def test_both_components_constant_reproduce_v1_order_and_one_does_not() -> None:
    u = _universes({1: "0.2", 2: "0.9", 3: "0.9", 4: "0.5"})
    assert v2.real_order(u, {}, frozenset()) == pot.real_order(u)
    assert v2.real_order(u, {i: Fraction(2) for i in u.r_ids}, frozenset(u.r_ids)) == pot.real_order(u)
    # DTC constant, insider not: the order is no longer v1's.
    assert v2.real_order(u, {}, frozenset({1})) != pot.real_order(u)


def test_score_must_be_present_for_every_r_member() -> None:
    with pytest.raises(ValueError, match="finite score"):
        v2.composite_scores({1, 2}, {1: Decimal(1)}, {}, frozenset())


def test_identity_permutation_reproduces_the_real_order() -> None:
    u = _universes({1: "0.9", 2: "0.5", 3: "0.1", 4: "0.3"})
    dtc, buyers = {1: Fraction(9), 3: Fraction(1)}, frozenset({4})
    identity = {i: i for i in [*u.r_ids, 5]}
    assert v2.control_order(u, identity, dtc, buyers) == v2.real_order(u, dtc, buyers)


def test_control_donates_the_pair_and_keeps_the_recipients_score() -> None:
    u = _universes({1: "0.9", 2: "0.5", 3: "0.1"})
    dtc = {2: Fraction(1), 5: Fraction(3)}  # 5 is an S₀ donor outside R
    buyers = frozenset({5})
    donor_of = {1: 5, 2: 3, 3: 2, 5: 1}
    dtc_k, buyers_k = v2.donated(u, donor_of, dtc, buyers)
    assert dtc_k == {1: Fraction(3), 3: Fraction(1)}  # 2's donor (3) has no DTC
    assert buyers_k == frozenset({1})
    assert v2.missing_donor_dtc(u, donor_of, dtc) == 1
    expected = v2.order_of(v2.composite_scores(u.r_ids, u.own_score, dtc_k, buyers_k))
    assert v2.control_order(u, donor_of, dtc, buyers) == expected


def test_order_diagnostics() -> None:
    d = v2.order_diagnostics({1, 2, 3}, {1: Fraction(2), 2: Fraction(2), 9: Fraction(5)}, frozenset({9}))
    assert d == v2.OrderDiagnostics(r_buyers=0, r_dtc_coverage=2, dtc_constant=True, ins_constant=True)
    d = v2.order_diagnostics({1, 2}, {1: Fraction(2), 2: Fraction(3)}, frozenset({1}))
    assert (d.dtc_constant, d.ins_constant) == (False, False)


# ---------------------------------------------------------------------------
# Strata and the stratified draw
# ---------------------------------------------------------------------------
def test_score_strata_deciles_ties_low_and_unscored() -> None:
    scores: dict[int, Decimal | None] = {i: Decimal(i) for i in range(1, 21)}
    scores[21] = None
    strata = v2.score_strata(range(1, 23), scores)  # 22 has no score row at all
    # cut points (nearest rank over 20 values): 2, 4, ..., 18; a value at a cut takes the lower decile
    assert [strata[i] for i in range(1, 21)] == [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9]
    assert strata[21] == strata[22] == v2.UNSCORED_STRATUM
    assert v2.strata_refusal(strata) is None


def test_strata_refusal_on_a_singleton() -> None:
    assert v2.strata_refusal({1: 0, 2: 0, 3: 1}) == "stratum_too_small"
    assert v2.strata_refusal({1: 0, 2: 0, 3: 1, 4: 1}) is None


def test_stratum_seed_golden() -> None:
    base = sim.control_seed("11" * 32, "22" * 32)
    assert v2.stratum_seed(base, 0).hex() == "8d31db95549ab7ee57c47cdbcf93046a5eb4e652db77dfa2188c0df56903226c"
    assert v2.stratum_seed(base, 10).hex() == "e51c719539e73c1b5bd861f804ef0a28b8ddd499f8d145caa918775bf5d41ae1"
    assert v2.stratum_seed(base, 10) == hashlib.sha256(base + b"\x00\x00\x00\x0a").digest()
    with pytest.raises(ValueError):
        v2.stratum_seed(base, 11)


def test_draw_stratified_is_a_within_stratum_bijection_from_v1s_draw() -> None:
    strata = {i: i % 3 for i in range(1, 31)}
    base = sim.control_seed("11" * 32, "22" * 32)
    pi = v2.draw_stratified(strata, base_seed=base, k=7)
    assert sorted(pi) == sorted(pi.values()) == list(range(1, 31))
    assert all(strata[d] == strata[r] for r, d in pi.items())
    stratum1 = sorted(i for i in strata if strata[i] == 1)
    assert {r: pi[r] for r in stratum1} == sim.draw_bijection(stratum1, seed=v2.stratum_seed(base, 1), k=7)
    assert pi != v2.draw_stratified(strata, base_seed=base, k=8)


# ---------------------------------------------------------------------------
# The v2 block
# ---------------------------------------------------------------------------
def _inputs() -> v2.V2Inputs:
    return v2.V2Inputs(
        purchases=(_row(txn_date=date(2026, 5, 4)), _row(txn_date=date(2026, 6, 4), filer=None)),
        history=tuple(_history({2023: [3], 2024: [4], 2025: [5]})),
        dtc=(_dtc(8, S2, "999.99", K2), _dtc(7, S2, None, K2)),
        reader_counts={"form4_unparsed": 3, "form4_tombstoned": 0},
    )


def test_block_round_trip_is_exact_and_canonical() -> None:
    inputs = _inputs()
    block = v2.encode_v2_block(inputs)
    assert block["schema"] == v2.V2_BLOCK_SCHEMA
    assert [r["instrument_id"] for r in block["dtc"]] == [7, 8]
    decoded = v2.decode_v2_block(block)
    assert v2.encode_v2_block(decoded) == block
    assert set(decoded.purchases) == set(inputs.purchases)
    assert set(decoded.dtc) == set(inputs.dtc)


def test_snapshot_attach_and_read() -> None:
    snap = v2.with_v2_block({"kind": "ranking-pot-snapshot"}, _inputs())
    assert v2.v2_block_of(snap) == v2.decode_v2_block(snap["v2"])
    with pytest.raises(ValueError, match="no ranking-pot-v2"):
        v2.v2_block_of({"kind": "ranking-pot-snapshot"})
    with pytest.raises(ValueError, match="already"):
        v2.with_v2_block(snap, _inputs())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b.update(schema="ranking-pot-v2-snapshot-2"),
        lambda b: b.update(extra=1),
        lambda b: b.pop("reader_counts"),
        lambda b: b["purchases"][0].update(is_derivative=0),
        lambda b: b["purchases"][0].update(txn_id=True),
        lambda b: b["purchases"][0].update(filed_at="2026-05-04T20:00:00"),
        lambda b: b["purchases"][0].pop("txn_date_invalid"),
        lambda b: b["dtc"][0].update(days_to_cover="abc"),
        lambda b: b["dtc"].reverse(),
        lambda b: b["reader_counts"].update(form4_unparsed="3"),
    ],
)
def test_block_decode_fails_closed(mutate: Callable[[dict[str, Any]], object]) -> None:
    block = v2.encode_v2_block(_inputs())
    mutate(block)
    with pytest.raises(ValueError):
        v2.decode_v2_block(block)


def test_encode_refuses_duplicates_and_naive_timestamps() -> None:
    inputs = _inputs()
    with pytest.raises(ValueError, match="duplicate txn_id"):
        v2.encode_v2_block(replace(inputs, purchases=inputs.purchases + inputs.purchases[:1]))
    with pytest.raises(ValueError, match="duplicate DTC"):
        v2.encode_v2_block(replace(inputs, dtc=inputs.dtc + inputs.dtc[:1]))
    naive = replace(inputs.purchases[0], filed_at=datetime(2026, 5, 4, 20))
    with pytest.raises(ValueError, match="timezone-aware"):
        v2.encode_v2_block(replace(inputs, purchases=(naive,)))
