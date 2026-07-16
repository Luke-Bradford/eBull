"""Pure-logic tests for the #2002 calibration-ledger capture predicates.

No DB: anchor parsing, data-anchored maturity, and the immature-pair
split (data_current / series_stalled / series_dead) are plain functions
over values — the house lean-test rule."""

from __future__ import annotations

from datetime import date

import pytest

from app.services.thesis_outcomes import (
    HORIZONS,
    anchor_date_from_summary,
    classify_immature,
    is_mature,
)

_TODAY = date(2026, 7, 16)


def _summary(available: object = True, as_of: object = "2026-07-01") -> dict:
    return {"blocks": {"price_anchor": {"available": available, "as_of": as_of}}}


@pytest.mark.parametrize(
    ("summary", "expected"),
    [
        (_summary(), date(2026, 7, 1)),
        # ISO timestamp: only the date prefix is read (same as the DQ audit).
        (_summary(as_of="2026-07-01T12:00:00Z"), date(2026, 7, 1)),
        (_summary(available=False), None),
        (_summary(available=None), None),
        (_summary(as_of=None), None),
        (_summary(as_of="not-a-date"), None),
        ({"blocks": {}}, None),
        ({}, None),
        (None, None),
        ("price_anchor", None),
    ],
)
def test_anchor_date_from_summary(summary: object, expected: date | None) -> None:
    assert anchor_date_from_summary(summary) == expected


@pytest.mark.parametrize(
    ("anchor", "horizon", "max_price", "expected"),
    [
        # Maturity is data-anchored: the series must have printed AT or
        # PAST the due date; wall-clock never participates.
        (date(2026, 1, 5), 30, date(2026, 2, 4), True),
        (date(2026, 1, 5), 30, date(2026, 2, 5), True),
        (date(2026, 1, 5), 30, date(2026, 2, 3), False),
        (date(2026, 1, 5), 30, None, False),
        (date(2026, 1, 5), 365, date(2027, 1, 4), False),
        (date(2026, 1, 5), 365, date(2027, 1, 5), True),
    ],
)
def test_is_mature(anchor: date, horizon: int, max_price: date | None, expected: bool) -> None:
    assert is_mature(anchor, horizon, max_price) is expected


@pytest.mark.parametrize(
    ("tradable", "max_price", "due", "expected"),
    [
        # Live series, pair simply not due yet — the normal young-thesis case.
        (True, _TODAY, date(2027, 7, 1), "immature_data_current"),
        (True, date(2026, 7, 15), date(2026, 8, 8), "immature_data_current"),
        # Series stopped recently (delisted 5d ago), due within grace of the
        # last print — not yet provably dead.
        (False, date(2026, 7, 11), date(2026, 7, 18), "immature_series_stalled"),
        # Tradable but series stale just past grace, due still near last
        # print — stalled, recovers to data_current if ingest resumes.
        (True, date(2026, 6, 14), date(2026, 6, 20), "immature_series_stalled"),
        # Series ended far before the due print — dead; the print this pair
        # needs will never come.
        (False, date(2026, 4, 1), date(2026, 7, 1), "series_dead"),
        (True, date(2026, 6, 1), date(2027, 6, 1), "series_dead"),
        # No price series at all: absent data is a terminal verdict only
        # when the instrument is also untradable (Codex ckpt-2 Medium).
        (True, None, date(2026, 8, 1), "immature_series_stalled"),
        (False, None, date(2026, 8, 1), "series_dead"),
    ],
)
def test_classify_immature(tradable: bool, max_price: date | None, due: date, expected: str) -> None:
    assert classify_immature(is_tradable=tradable, max_price_date=max_price, due_date=due, today=_TODAY) == expected


def test_horizons_are_the_spec_set() -> None:
    """Schema CHECK pins (30, 90, 365); the constant must match."""
    assert HORIZONS == (30, 90, 365)
