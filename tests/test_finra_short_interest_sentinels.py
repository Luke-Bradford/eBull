"""Pure-logic tests for the FINRA short-interest health sentinels (#2337).

No DB — ``evaluate_file_sentinels`` and ``required_row_fields`` are pure, so
the arms are table-tested here and the fast gate covers them. The DB-backed
half (the ``_previous_stored_resolved`` LATERAL, and the job-level assertion
that a normal partial-universe fire stays quiet) lives in
``tests/test_finra_short_interest_refresh.py``.

The numbers the arms are calibrated against come from
``scripts/audit_2337_finra_match_rate.py`` over every stored payload — 34
files / 715,915 rows, match rate 25.47%-26.58%, ``invalid_row`` 0, consecutive
retention min 0.99929.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from app.jobs.finra_short_interest_refresh import (
    _RESOLVED_RETENTION_FLOOR,
    evaluate_file_sentinels,
)
from app.services.finra_short_interest_ingest import (
    SettlementIngestStats,
    required_row_fields,
)

_TODAY = date(2026, 7, 31)
_PRIOR = date(2026, 7, 15)


def _stats(**kw: object) -> SettlementIngestStats:
    base: dict[str, object] = {
        "settlement_date": _TODAY,
        "rows_parsed": 22341,
        "rows_resolved": 5700,
        "rows_upserted": 5700,
        "skipped_no_instrument_match": 16618,
        "skipped_ambiguous_symbol": 23,
        "skipped_invalid_row": 0,
    }
    base.update(kw)
    return SettlementIngestStats(**base)  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# The #2337 regression: the healthy operating point must be SILENT
# ----------------------------------------------------------------------


def test_measured_healthy_operating_point_is_silent() -> None:
    """25.51% match — the real 2026-07-31 figure — raises nothing.

    This is the defect #2337 exists to fix: the old arm warned below an
    absolute 0.50 match rate, and the census found 34 of 34 stored files below
    it. A sentinel that fires on every fire carries no information.
    """
    stats = _stats()
    assert stats.rows_resolved / stats.rows_parsed < 0.50
    assert evaluate_file_sentinels(stats, (_PRIOR, 5716)) == []


@pytest.mark.parametrize("resolved", [5699, 5700, 5729])
def test_corpus_consecutive_retention_is_silent(resolved: int) -> None:
    """Every consecutive pair in the corpus stays above the floor."""
    assert evaluate_file_sentinels(_stats(rows_resolved=resolved), (_PRIOR, 5716)) == []


# ----------------------------------------------------------------------
# Arm 1 — row shape
# ----------------------------------------------------------------------


def test_any_invalid_row_warns() -> None:
    findings = evaluate_file_sentinels(_stats(skipped_invalid_row=1), (_PRIOR, 5716))
    assert [f.kind for f in findings] == ["row_shape"]
    assert "1 of 22341" in findings[0].detail


# ----------------------------------------------------------------------
# Arm 2 — total resolution failure
# ----------------------------------------------------------------------


def test_zero_resolution_warns_once_not_twice() -> None:
    """``no_resolution`` suppresses ``universe_drift`` — one fault, one alarm."""
    findings = evaluate_file_sentinels(
        _stats(rows_resolved=0, rows_upserted=0, skipped_no_instrument_match=22341, skipped_ambiguous_symbol=0),
        (_PRIOR, 5716),
    )
    assert [f.kind for f in findings] == ["no_resolution"]


def test_zero_resolution_still_reports_row_shape() -> None:
    findings = evaluate_file_sentinels(
        _stats(rows_resolved=0, rows_upserted=0, skipped_invalid_row=4),
        (_PRIOR, 5716),
    )
    assert [f.kind for f in findings] == ["row_shape", "no_resolution"]


# ----------------------------------------------------------------------
# Arm 3 — retention against the previous stored settlement date
# ----------------------------------------------------------------------


def test_retention_below_floor_warns() -> None:
    findings = evaluate_file_sentinels(_stats(rows_resolved=4000), (_PRIOR, 5716))
    assert [f.kind for f in findings] == ["universe_drift"]
    assert "2026-07-15" in findings[0].detail


def test_retention_exactly_at_floor_is_silent() -> None:
    """The floor is a strict ``<``, so the boundary value does not fire."""
    at_floor = math.ceil(5716 * _RESOLVED_RETENTION_FLOOR)
    assert evaluate_file_sentinels(_stats(rows_resolved=at_floor), (_PRIOR, 5716)) == []
    assert evaluate_file_sentinels(_stats(rows_resolved=at_floor - 1), (_PRIOR, 5716))


def test_no_baseline_skips_retention() -> None:
    """The oldest settlement date held has nothing to compare against."""
    assert evaluate_file_sentinels(_stats(rows_resolved=1), None) == []


def test_zero_baseline_does_not_divide_by_zero() -> None:
    assert evaluate_file_sentinels(_stats(rows_resolved=1), (_PRIOR, 0)) == []


def test_growth_is_silent() -> None:
    """The floor is one-sided — a bigger universe is not a fault."""
    assert evaluate_file_sentinels(_stats(rows_resolved=99999), (_PRIOR, 5716)) == []


# ----------------------------------------------------------------------
# Files that are already surfaced elsewhere yield nothing
# ----------------------------------------------------------------------


def test_failed_file_yields_nothing() -> None:
    """Per-file failures raise ``RuntimeError`` at the end of the run."""
    assert evaluate_file_sentinels(_stats(failed=True, error_detail="fetch: 503"), (_PRIOR, 5716)) == []


def test_empty_file_yields_nothing() -> None:
    assert (
        evaluate_file_sentinels(
            _stats(rows_parsed=0, rows_resolved=0, rows_upserted=0, skipped_no_instrument_match=0),
            (_PRIOR, 5716),
        )
        == []
    )


# ----------------------------------------------------------------------
# The shared row-shape gate the census and ingest both call
# ----------------------------------------------------------------------


def test_required_row_fields_accepts_a_well_formed_row() -> None:
    row = {"symbolCode": " AAPL ", "currentShortPositionQuantity": "123", "settlementDate": "2026-07-31"}
    assert required_row_fields(row) == ("AAPL", 123, "2026-07-31")


@pytest.mark.parametrize(
    "row",
    [
        {"symbolCode": "", "currentShortPositionQuantity": "1", "settlementDate": "2026-07-31"},
        {"symbolCode": None, "currentShortPositionQuantity": "1", "settlementDate": "2026-07-31"},
        {"symbolCode": "AAPL", "currentShortPositionQuantity": "", "settlementDate": "2026-07-31"},
        {"symbolCode": "AAPL", "currentShortPositionQuantity": None, "settlementDate": "2026-07-31"},
        {"symbolCode": "AAPL", "currentShortPositionQuantity": "1", "settlementDate": ""},
        {"symbolCode": "AAPL", "currentShortPositionQuantity": "1", "settlementDate": None},
        {"symbolCode": "AAPL", "currentShortPositionQuantity": "not-a-number", "settlementDate": "2026-07-31"},
        {},
    ],
)
def test_required_row_fields_rejects_shape_defects(row: dict[str, object]) -> None:
    assert required_row_fields(row) is None
