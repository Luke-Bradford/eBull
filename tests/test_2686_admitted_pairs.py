"""#2686 — the series-selection rule `verify_2240_statistics` admits on.

Pure-logic. What is pinned is that the harness takes its population from
``universe_selection`` (one vendor-pinned, versioned rule) rather than from a
uniqueness assertion over every linked series — and that the refusal, if it ever
fires, counts the thing it names.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.universe_selection import AdmittedSeries, UniverseSelection
from scripts.verify_2240_statistics import admitted_pairs


def _selection(*admitted: AdmittedSeries, vendor: str = "vendorA") -> UniverseSelection:
    return UniverseSelection(
        universe="survivor_only",
        vendor=vendor,
        capture_date=None,
        admitted=admitted,
        unlinked_alive_excluded=0,
        linked_early_reuse_suspect=0,
        exchange_test_issues_excluded=0,
        unharvested_excluded=0,
        vendor_series_total=len(admitted),
    )


def _series(series_id: int, *, name_key: int, instrument_id: int | None = None) -> AdmittedSeries:
    return AdmittedSeries(
        series_id=series_id,
        name_key=name_key,
        instrument_id=instrument_id if instrument_id is not None else (name_key if name_key > 0 else None),
        termination=None,
        last_bar=date(2026, 7, 8),
    )


def test_pairs_are_name_key_and_series_id_ascending() -> None:
    selection = _selection(_series(70, name_key=9), _series(50, name_key=4))
    assert admitted_pairs(selection) == ((4, 50), (9, 70))


def test_an_unlinked_series_keys_on_its_negative_series_id() -> None:
    """The engine's in-pass key. Nothing in this harness is persisted, which is
    what makes a negative key safe here."""
    selection = _selection(_series(88, name_key=-88, instrument_id=None))
    assert admitted_pairs(selection) == ((-88, 88),)


def test_an_empty_admitted_set_is_empty_not_an_error() -> None:
    assert admitted_pairs(_selection()) == ()


class TestTheRefusal:
    def test_two_series_under_one_name_key_refuses(self) -> None:
        """This is #2686's whole defect — one name acquiring a second series on a
        different adjustment basis. It cannot happen under a vendor pin, so it is
        asserted rather than assumed."""
        selection = _selection(_series(1, name_key=7), _series(2, name_key=7))
        with pytest.raises(RuntimeError, match="vendor pin is not holding"):
            admitted_pairs(selection)

    def test_the_message_counts_the_thing_it_names(self) -> None:
        """⚠ The replaced message printed ``len(pairs) - len(instruments)`` — the
        number of EXCESS SERIES — while saying "instruments" (4,546 against
        4,550). Two names here, one with three series and one with two: a
        message counting excess series would say 3."""
        selection = _selection(
            _series(1, name_key=7),
            _series(2, name_key=7),
            _series(3, name_key=7),
            _series(4, name_key=8),
            _series(5, name_key=8),
            _series(6, name_key=9),
        )
        with pytest.raises(RuntimeError) as caught:
            admitted_pairs(selection)
        assert "2 name keys carry more than one admitted series (worst: 3)" in str(caught.value)
