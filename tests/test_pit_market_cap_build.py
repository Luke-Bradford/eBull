"""#3841 slice 2: the build's pure readers-side rules (``app/services/pit_market_cap_build.py``)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

from app.services.pit_fundamentals import Filing, SubmissionsIndex
from app.services.pit_market_cap import Concept, FloatFact, Trade
from app.services.pit_market_cap_build import (
    BALANCE_SHEET,
    COVER,
    PUBLIC_FLOAT,
    CikRefusal,
    FactRow,
    InsiderRow,
    nyse_sessions,
    public_floats,
    resolve_amendments,
    timed_share_facts,
)


def _index(*filings: tuple[str, str, str | None]) -> SubmissionsIndex:
    return SubmissionsIndex({accn: Filing(accn, form, acceptance) for accn, form, acceptance in filings})


def _fact(concept: tuple[str, str], accession: str, context: date, value: str) -> FactRow:
    return FactRow(concept[0], concept[1], accession, context, Decimal(value))


def test_share_facts_take_form_and_acceptance_from_submissions() -> None:
    index = _index(("A-1", "10-Q", "2025-08-01T20:30:00.000Z"))
    facts, refused = timed_share_facts(
        [_fact(COVER, "A-1", date(2025, 7, 25), "100"), _fact(BALANCE_SHEET, "A-1", date(2025, 6, 30), "99")], index
    )
    assert refused is None
    assert [(f.concept, f.form, f.accepted.isoformat()) for f in facts] == [
        (Concept.COVER, "10-Q", "2025-08-01"),
        (Concept.BALANCE_SHEET, "10-Q", "2025-08-01"),
    ]


def test_acceptance_after_new_york_midnight_utc_is_the_previous_new_york_day() -> None:
    # 02:30Z on 2 Aug is 22:30 EDT on 1 Aug: the count is public from the first session after 1 Aug.
    index = _index(("A-1", "10-Q", "2025-08-02T02:30:00.000Z"))
    facts, _ = timed_share_facts([_fact(COVER, "A-1", date(2025, 7, 25), "100")], index)
    assert facts[0].accepted == date(2025, 8, 1)


def test_an_untimed_share_count_accession_refuses_the_cik_whole() -> None:
    index = _index(("A-1", "10-Q", "2025-08-01T20:30:00.000Z"), ("A-2", "10-Q", None))
    facts, refused = timed_share_facts(
        [_fact(COVER, "A-1", date(2025, 7, 25), "100"), _fact(COVER, "A-2", date(2025, 10, 25), "100")], index
    )
    assert (facts, refused) == ((), CikRefusal.UNTIMED_ACCESSION)
    # An accession absent from the index refuses alike.
    _, refused = timed_share_facts([_fact(BALANCE_SHEET, "A-9", date(2025, 6, 30), "1")], index)
    assert refused is CikRefusal.UNTIMED_ACCESSION


def test_public_float_rows_do_not_count_toward_share_timing() -> None:
    # A float in an accession the index lacks does not refuse the CIK's share counts.
    index = _index(("A-1", "10-K", "2025-02-01T20:30:00.000Z"))
    facts, refused = timed_share_facts(
        [_fact(COVER, "A-1", date(2025, 1, 25), "100"), _fact(PUBLIC_FLOAT, "A-9", date(2024, 6, 28), "5")], index
    )
    assert refused is None and len(facts) == 1


def test_public_float_takes_the_latest_accepted_accession_per_context() -> None:
    index = _index(
        ("K-1", "10-K", "2025-02-01T20:30:00.000Z"),
        ("K-2", "10-K/A", "2025-04-01T20:30:00.000Z"),
        ("F-1", "20-F", "2025-05-01T20:30:00.000Z"),
    )
    context = date(2024, 6, 28)
    rows = [
        _fact(PUBLIC_FLOAT, "K-1", context, "1000"),
        _fact(PUBLIC_FLOAT, "K-2", context, "1200"),
        _fact(PUBLIC_FLOAT, "F-1", context, "9999"),  # out-of-scope form: never the latest
    ]
    assert public_floats(rows, index) == (FloatFact(context=context, value=1200.0),)


def test_public_float_with_two_values_in_its_latest_accession_is_skipped() -> None:
    index = _index(("K-1", "10-K", "2025-02-01T20:30:00.000Z"))
    context = date(2024, 6, 28)
    rows = [_fact(PUBLIC_FLOAT, "K-1", context, "1000"), _fact(PUBLIC_FLOAT, "K-1", context, "1100")]
    assert public_floats(rows, index) == ()


FIRST = date(2025, 1, 1)


def _insider(accession: str, document_type: str, day: date, price: float, *, signed: date) -> InsiderRow:
    return InsiderRow(accession, 1, document_type, "0001", date(2025, 3, 3), signed, day, price)


def test_an_amendment_replaces_its_original_report() -> None:
    rows = [
        _insider("O-1", "4", date(2025, 3, 3), 10.0, signed=date(2025, 3, 4)),
        _insider("A-1", "4/A", date(2025, 3, 3), 11.0, signed=date(2025, 3, 10)),
    ]
    assert resolve_amendments(rows, FIRST) == (Trade(day=date(2025, 3, 3), price=11.0),)


def test_the_latest_of_two_amendments_wins_and_originals_without_one_stay() -> None:
    rows = [
        _insider("O-1", "4", date(2025, 3, 3), 10.0, signed=date(2025, 3, 4)),
        _insider("A-1", "4/A", date(2025, 3, 3), 11.0, signed=date(2025, 3, 10)),
        _insider("A-2", "4/A", date(2025, 3, 3), 12.0, signed=date(2025, 3, 20)),
        InsiderRow("O-2", 1, "4", "0002", date(2025, 3, 3), date(2025, 3, 4), date(2025, 3, 3), 13.0),
        InsiderRow("O-3", 2, "4", "0002", date(2025, 3, 3), date(2025, 3, 5), date(2025, 3, 4), 14.0),
    ]
    assert resolve_amendments(rows, FIRST) == (
        Trade(day=date(2025, 3, 3), price=12.0),
        Trade(day=date(2025, 3, 3), price=13.0),
        Trade(day=date(2025, 3, 4), price=14.0),
    )


def test_an_amendment_that_recodes_a_row_away_from_p_or_s_still_displaces_the_original() -> None:
    original = _insider("O-1", "4", date(2025, 3, 3), 10.0, signed=date(2025, 3, 4))
    recoded = InsiderRow("A-1", 1, "4/A", "0001", date(2025, 3, 3), date(2025, 3, 10), date(2025, 3, 3), 10.0, "M")
    assert resolve_amendments([original, recoded], FIRST) == ()


def test_rows_with_no_report_period_are_never_pooled_into_one_report() -> None:
    day = date(2025, 3, 3)
    original = InsiderRow("O-1", 1, "4", "0001", None, None, day, 10.0)
    other = InsiderRow("O-2", 1, "4", "0002", None, None, day, 11.0)
    unrelated_amendment = InsiderRow("A-9", 1, "4/A", "0003", None, date(2025, 3, 9), day, 12.0)
    assert resolve_amendments([original, other, unrelated_amendment], FIRST) == (
        Trade(day=day, price=10.0),
        Trade(day=day, price=11.0),
        Trade(day=day, price=12.0),
    )


def test_rows_qualify_only_as_gate_1_input() -> None:
    day = date(2025, 3, 3)
    base = InsiderRow("O-1", 1, "4", "0001", day, None, day, 10.0)
    rows = [
        base,
        replace(base, row=2, price=0.0),
        replace(base, row=3, title="Series A Preferred"),
        replace(base, row=4, derivative=True),
        replace(base, row=5, date_invalid=True),
        replace(base, row=6, day=date(2024, 12, 31)),
        replace(base, row=7, price=float("nan")),
    ]
    assert resolve_amendments(rows, FIRST) == (Trade(day=day, price=10.0),)


def test_sessions_follow_the_nyse_calendar() -> None:
    # 2025-07-04 is Independence Day (closed); 2025-07-03 is a half day, which is a session.
    assert nyse_sessions(date(2025, 7, 2), date(2025, 7, 8)) == (
        date(2025, 7, 2),
        date(2025, 7, 3),
        date(2025, 7, 7),
        date(2025, 7, 8),
    )
