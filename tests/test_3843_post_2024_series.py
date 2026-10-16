"""#3843 — per-source delisting verdicts and event coverage for the post-2024-09 research series."""

from __future__ import annotations

from datetime import date

import pytest

from scripts.research_3843.post_2024_series import (
    ETORO,
    PWB_VENDOR,
    SURVIVOR_ONLY_VENDOR,
    WINDOW_START,
    Event,
    Found,
    classify_source,
    event_status,
    resolve_etoro_instrument,
)

CAPTURE = date(2026, 9, 9)


def _event(filed: date, *, latest: date | None = None, provisions: int = 1) -> Event:
    return Event(
        issuer_cik="0000000001",
        symbol="XYZ",
        issuer_name="XYZ Corp",
        earliest_filed=filed,
        latest_filed=latest or filed,
        provision_variants=provisions,
        provision="(b)",
        suspension_variants=0,
        suspension_date=None,
    )


def _found(first: date, last: date) -> Found:
    return Found(ref=1, first_bar=first, last_bar=last, last_close=1.0)


def test_window_starts_the_month_after_the_intrader_capture() -> None:
    assert WINDOW_START == date(2024, 10, 1)


@pytest.mark.parametrize(
    ("last_bar", "expected"),
    [
        # 17 CFR 240.12d2-2(d)(1): effective 10 days after filing. The boundary day ends with the listing.
        (date(2025, 3, 11), "terminates"),
        (date(2025, 3, 12), "continues"),
        # A (b) filing trails the suspension by months; no lower bound on the gap.
        (date(2024, 11, 4), "terminates"),
        (date(2024, 9, 30), "dead_before_window"),
    ],
)
def test_last_bar_against_the_effective_date(last_bar: date, expected: str) -> None:
    ev = _event(date(2025, 3, 1))
    assert classify_source(ev, _found(date(2010, 1, 4), last_bar), CAPTURE) == expected


def test_effective_date_after_the_capture_cannot_show_termination() -> None:
    ev = _event(date(2026, 9, 1))  # effective 2026-09-11, capture 2026-09-09
    assert classify_source(ev, _found(date(2010, 1, 4), date(2026, 9, 9)), CAPTURE) == "after_capture"


def test_refusals_come_from_the_corpus_linkage_classifier() -> None:
    filed = date(2025, 3, 1)
    # Series starts after every filing: not the removed security's history.
    assert classify_source(_event(filed), _found(date(2025, 6, 2), date(2026, 9, 9)), CAPTURE) == "identity_unverified"
    # Filings disagree on the provision.
    assert classify_source(_event(filed, provisions=2), _found(date(2010, 1, 4), filed), CAPTURE) == "conflict"
    # Two events resolved onto one series.
    assert classify_source(_event(filed), _found(date(2010, 1, 4), filed), CAPTURE, shared=True) == "conflict"
    assert classify_source(_event(filed), None, CAPTURE) == "absent"


def test_coverage_takes_the_first_terminating_source_in_precedence_order() -> None:
    verdicts = {PWB_VENDOR: "continues", SURVIVOR_ONLY_VENDOR: "terminates", ETORO: "terminates"}
    assert event_status(verdicts) == ("covered", SURVIVOR_ONLY_VENDOR)


def test_dead_before_window_is_not_applicable_unless_a_source_shows_it_alive() -> None:
    assert event_status({PWB_VENDOR: "absent", SURVIVOR_ONLY_VENDOR: "absent", ETORO: "dead_before_window"}) == (
        "not_applicable",
        None,
    )
    assert event_status({PWB_VENDOR: "continues", SURVIVOR_ONLY_VENDOR: "absent", ETORO: "dead_before_window"}) == (
        "uncovered",
        None,
    )
    assert event_status({PWB_VENDOR: "absent", SURVIVOR_ONLY_VENDOR: "absent", ETORO: "absent"}) == ("uncovered", None)


def test_etoro_resolution_needs_the_security_symbol_not_just_the_issuer() -> None:

    by_symbol = {"ABC": [1], "SHR": [2, 3], "BBBY": [4]}
    # A CIK alone never resolves: it names the issuer, and the removed class may not be the listed one.
    assert resolve_etoro_instrument(None, {1}, by_symbol) is None
    assert resolve_etoro_instrument("ABC", set(), by_symbol) == 1
    # A Chapter 11 Q symbol falls back to the pre-bankruptcy spelling.
    assert resolve_etoro_instrument("BBBYQ", set(), by_symbol) == 4
    # A shared symbol resolves only when the issuer's CIK narrows it to one.
    assert resolve_etoro_instrument("SHR", {3}, by_symbol) == 3
    assert resolve_etoro_instrument("SHR", set(), by_symbol) is None
