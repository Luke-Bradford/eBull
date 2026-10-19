"""#3902: the build's submissions index under #3841 spec Amendment 1 (``app/services/pit_market_cap_build.py``)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from app.services.pit_fundamentals import SubmissionsIndex
from app.services.pit_market_cap import CikRefusal, rule_version
from app.services.pit_market_cap_build import (
    COVER,
    PUBLIC_FLOAT,
    FactRow,
    InstrumentIndex,
    deconflict_submissions,
    merge_indexes,
    timed_share_facts,
)

CIK = "0000000001"
T1 = "2025-08-01T20:30:00.000Z"
T1_ET_AS_Z = "2025-08-01T16:30:00.000Z"  # the measured shape: the same instant's New York time labelled Z
T1_NEXT_DAY = "2025-08-02T14:30:00.000Z"  # a different New York date: a real clock conflict


def _block(*rows: tuple[Any, Any, Any, Any]) -> dict[str, list[Any]]:
    columns = ("accessionNumber", "acceptanceDateTime", "form", "items")
    return {name: [row[i] for row in rows] for i, name in enumerate(columns)}


def _cik(recent: dict[str, Any], pages: dict[str, Any] | None = None, cik: str = CIK) -> tuple[Any, Any]:
    pages = pages or {}
    main = {"cik": int(cik), "filings": {"recent": recent, "files": [{"name": name} for name in pages]}}
    return main, pages.get


def _index(recent: dict[str, Any], pages: dict[str, Any] | None = None, cik: str = CIK) -> InstrumentIndex | str:
    index, listed, conflicted = deconflict_submissions(cik, *_cik(recent, pages, cik))
    if isinstance(index, str):
        return index
    return merge_indexes(index, listed, conflicted, ())


def _cover(accession: str) -> FactRow:
    return FactRow(COVER[0], COVER[1], accession, date(2025, 7, 25), Decimal(100))


def test_copies_on_one_new_york_date_agree_and_keep_the_later_acceptance() -> None:
    # AMZN's shape: a year-2000 424B3 listed on two overflow pages, 5 hours apart, both on 18 Jan in New York.
    index = _index(
        _block(("A-1", T1, "10-Q", "")),
        {"page-002.json": _block(("OLD-1", "2000-01-18T05:00:00.000Z", "424B3", ""))}
        | {"page-003.json": _block(("OLD-1", "2000-01-18T10:00:00.000Z", "424B3", ""))},
    )
    assert isinstance(index, InstrumentIndex) and not index.conflicted
    assert index.filings["OLD-1"].acceptance == "2000-01-18T10:00:00.000Z"
    index = _index(_block(("A-1", T1_ET_AS_Z, "10-Q", ""), ("A-1", T1, "10-Q", "")))
    assert isinstance(index, InstrumentIndex) and index.filings["A-1"].acceptance == T1


def test_a_conflict_on_an_uncited_accession_no_longer_refuses_the_cik() -> None:
    index = _index(
        _block(("A-1", T1, "10-Q", "")),
        {"page-002.json": _block(("OLD-1", T1, "424B3", ""))} | {"page-003.json": _block(("OLD-1", T1, "S-3", ""))},
    )
    assert isinstance(index, InstrumentIndex)
    assert "OLD-1" not in index.filings and index.conflicted == {"OLD-1"}
    facts, refused = timed_share_facts([_cover("A-1")], index)
    assert refused is None and [f.accession for f in facts] == ["A-1"]


def test_any_two_of_three_copies_disagreeing_drop_the_accession() -> None:
    index = _index(_block(("A-1", T1, "10-Q", ""), ("A-1", T1, "10-Q", ""), ("A-1", T1_NEXT_DAY, "10-Q", "")))
    assert isinstance(index, InstrumentIndex) and "A-1" not in index.filings


def test_copies_differing_only_in_items_or_spelling_are_one_record() -> None:
    index = _index(_block(("A-1", T1, "10-Q", ""), ("A-1", "2025-08-01T20:30:00Z", "10-Q", "2.02")))
    assert isinstance(index, InstrumentIndex)
    assert index.filings["A-1"].acceptance == T1 and not index.conflicted


def test_a_conflict_on_a_cited_share_or_float_accession_refuses() -> None:
    index = _index(_block(("A-1", T1, "10-Q", ""), ("A-1", T1_NEXT_DAY, "10-Q", ""), ("K-1", T1, "10-K", "")))
    assert isinstance(index, InstrumentIndex)
    assert timed_share_facts([_cover("A-1")], index) == ((), CikRefusal.UNTIMED_ACCESSION)
    float_on_conflict = FactRow(PUBLIC_FLOAT[0], PUBLIC_FLOAT[1], "A-1", date(2024, 6, 28), Decimal(5))
    assert timed_share_facts([_cover("K-1"), float_on_conflict], index) == ((), CikRefusal.UNTIMED_ACCESSION)


def test_a_malformed_row_inside_a_conflicting_block_still_fails_integrity() -> None:
    recent = _block(("A-1", T1, "10-Q", ""), ("A-1", T1_NEXT_DAY, "10-Q", ""), ("A-2", T1, None, ""))
    assert _index(recent) == "submissions_malformed"


def test_unequal_columns_and_missing_pages_still_fail_integrity() -> None:
    recent = _block(("A-1", T1, "10-Q", ""), ("A-1", T1_NEXT_DAY, "10-Q", ""))
    recent["items"].append("")
    assert _index(recent) == "submissions_column_length_mismatch"
    main, _ = _cik(_block(("A-1", T1, "10-Q", "")), {"page-002.json": _block()})
    assert deconflict_submissions(CIK, main, lambda name: None)[0] == "submissions_page_missing"


def _merged(primary: dict[str, Any], *others: dict[str, Any] | str) -> InstrumentIndex:
    index, listed, conflicted = deconflict_submissions(CIK, *_cik(primary))
    assert isinstance(index, SubmissionsIndex)
    other_indexes: list[SubmissionsIndex | str] = []
    for n, other in enumerate(others, start=2):
        if isinstance(other, str):
            other_indexes.append(other)
            continue
        cik = f"{n:010d}"
        other_indexes.append(deconflict_submissions(cik, *_cik(other, cik=cik))[0])
    return merge_indexes(index, listed, conflicted, other_indexes)


def test_a_predecessor_only_accession_is_excluded_not_refused() -> None:
    # XOM's shape: the predecessor's 10-Q is listed only by the instrument's non-primary CIK.
    index = _merged(_block(("NEW-1", T1, "10-Q", "")), _block(("OLD-1", "2025-05-01T20:30:00.000Z", "10-Q", "")))
    assert index.predecessor == {"OLD-1"}
    facts, refused = timed_share_facts([_cover("OLD-1"), _cover("NEW-1")], index)
    assert refused is None and [f.accession for f in facts] == ["NEW-1"]
    # With no current-registrant count, the instrument has no facts and no refusal.
    assert timed_share_facts([_cover("OLD-1")], index) == ((), None)


def test_a_predecessor_accession_the_primary_also_lists_is_timed_by_the_primary() -> None:
    index = _merged(_block(("J-1", T1, "10-Q", "")), _block(("J-1", T1, "10-Q", "")))
    assert not index.predecessor and index.filings["J-1"].acceptance == T1


def test_a_cross_cik_listing_on_the_same_new_york_date_keeps_the_later_acceptance() -> None:
    # XOM's shape: its latest 10-Q is listed by the holding company and the predecessor, offset by the ET offset.
    index = _merged(_block(("J-1", T1_ET_AS_Z, "10-Q", "")), _block(("J-1", T1, "10-Q", "")))
    assert not index.conflicted and index.filings["J-1"].acceptance == T1


def test_a_cross_cik_disagreement_drops_the_accession() -> None:
    index = _merged(_block(("J-1", T1, "10-Q", "")), _block(("J-1", T1_NEXT_DAY, "10-Q", "")))
    assert "J-1" not in index.filings and "J-1" in index.conflicted
    assert timed_share_facts([_cover("J-1")], index) == ((), CikRefusal.UNTIMED_ACCESSION)


def test_a_primary_conflict_a_non_primary_also_lists_stays_untimed() -> None:
    primary = _block(("A-1", T1, "10-Q", ""), ("A-1", T1_NEXT_DAY, "10-Q", ""))
    index = _merged(primary, _block(("A-1", T1, "10-Q", "")))
    assert not index.predecessor
    assert timed_share_facts([_cover("A-1")], index) == ((), CikRefusal.UNTIMED_ACCESSION)


def test_a_failed_non_primary_index_lists_nothing() -> None:
    index = _merged(_block(("NEW-1", T1, "10-Q", "")), "submissions_page_missing")
    assert not index.predecessor
    assert timed_share_facts([_cover("OLD-1")], index) == ((), CikRefusal.UNTIMED_ACCESSION)


def test_the_rule_version_carries_the_cik_refusals() -> None:
    assert {str(r) for r in CikRefusal} == {"untimed_accession", "fpi_adr"}
    assert len(rule_version()) == 64
