"""Pure-logic tests for the FSDS per-class shares ingest (#788). No DB.

The read-path swap is exercised DB-side in tests/test_ownership_rollup.py; here we
table-test the parse/select/no-demotion decisions and pin the curated map against
the SEC-verified per-class CUSIPs (the curated map IS the correctness guarantee —
Codex ckpt-1 §11.2)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.fsds_class_shares import (
    _CLASS_MEMBER_TO_CUSIP,
    _ClassRow,
    _parse_fsds_date,
    _supersedes,
    parse_class_member,
)
from app.services.ownership_rollup import _humanize_class_member, _should_use_class_denominator

# --- parse_class_member -----------------------------------------------------


@pytest.mark.parametrize(
    ("segments", "expected"),
    [
        ("ClassOfStock=CommonClassA;", "CommonClassA"),
        ("ClassOfStock=CapitalClassC;", "CapitalClassC"),
        ("ClassOfStock=HeicoCommonStock;", "HeicoCommonStock"),
        # Multi-axis → rejected (restatement / scenario / consolidated sub-slice).
        ("ClassOfStock=CommonClassA;EquityComponents=CommonStock;", None),
        ("ClassOfStock=CommonClassB;Restatement=ScenarioPreviouslyReported;", None),
        # Combined / single-class fingerprint → not a ClassOfStock segment.
        ("", None),
        ("EquityComponents=CommonStock;", None),
        ("ClassOfStock=;", None),  # empty member
    ],
)
def test_parse_class_member(segments: str, expected: str | None) -> None:
    assert parse_class_member(segments) == expected


# --- _parse_fsds_date -------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("20241231", date(2024, 12, 31)),
        ("20250131", date(2025, 1, 31)),
        ("2024123", None),  # too short
        ("2024-12-31", None),  # wrong format
        ("00000000", None),  # invalid date
        ("", None),
        (None, None),
    ],
)
def test_parse_fsds_date(raw: str | None, expected: date | None) -> None:
    assert _parse_fsds_date(raw) == expected


# --- _supersedes (restatement no-demotion) ----------------------------------


def _row(*, filed: date, adsh: str) -> _ClassRow:
    return _ClassRow(
        instrument_id=1,
        period_end=date(2024, 12, 31),
        shares=Decimal(100),
        class_member="CommonClassA",
        source_cik="0001652044",
        source_adsh=adsh,
        source_form_type="10-K",
        source_filed_at=filed,
    )


def test_supersedes_later_filed_wins() -> None:
    incumbent = _row(filed=date(2025, 1, 1), adsh="a-1")
    candidate = _row(filed=date(2025, 2, 1), adsh="a-1")
    assert _supersedes(candidate, incumbent) is True
    assert _supersedes(incumbent, candidate) is False


def test_supersedes_equal_filed_breaks_on_adsh() -> None:
    incumbent = _row(filed=date(2025, 1, 1), adsh="aaa")
    candidate = _row(filed=date(2025, 1, 1), adsh="zzz")
    assert _supersedes(candidate, incumbent) is True
    assert _supersedes(incumbent, candidate) is False


# --- curated map fixture (SEC-verified, FSDS 2025q1) ------------------------


def test_curated_map_matches_sec_verified_cusips() -> None:
    """Every curated (cik, member) → the SEC-verified per-class CUSIP. Catches a
    typo / A↔C swap at authoring (not via a runtime magnitude heuristic)."""
    expected = {
        ("0001652044", "CommonClassA"): "02079K305",  # GOOGL (Class A)
        ("0001652044", "CapitalClassC"): "02079K107",  # GOOG (Class C)
        ("0000046619", "CommonClassA"): "422806208",  # HEI.A
        ("0000046619", "HeicoCommonStock"): "422806109",  # HEI
        ("0001687187", "CommonClassA"): "75134P600",  # METC
        ("0001687187", "CommonClassB"): "75134P501",  # METCB
    }
    assert _CLASS_MEMBER_TO_CUSIP == expected


def test_curated_map_cusips_are_distinct_per_cik() -> None:
    """No two classes of one issuer map to the same CUSIP (would mean a swap or
    a duplicate)."""
    by_cik: dict[str, set[str]] = {}
    for (cik, _member), cusip in _CLASS_MEMBER_TO_CUSIP.items():
        by_cik.setdefault(cik, set()).add(cusip)
    for cik, cusips in by_cik.items():
        members = [m for (c, m) in _CLASS_MEMBER_TO_CUSIP if c == cik]
        assert len(cusips) == len(members), f"{cik} has colliding CUSIPs"


# --- _humanize_class_member (user-facing copy, not the XBRL localname) -------


@pytest.mark.parametrize(
    ("member", "expected"),
    [
        ("CommonClassA", "Class A"),
        ("CapitalClassC", "Class C"),
        ("CommonClassB", "Class B"),
        ("PreferredClassD", "Class D"),
        # Issuer-specific localname → space-separated fallback, never verbatim.
        ("HeicoCommonStock", "Heico Common Stock"),
        ("CommonStockNonExchangeable", "Common Stock Non Exchangeable"),
    ],
)
def test_humanize_class_member(member: str, expected: str) -> None:
    assert _humanize_class_member(member) == expected


# --- _should_use_class_denominator (fail-closed guard) ----------------------

_GOOGL_CLASS = Decimal("5835000000")
_COMBINED = Decimal("12211000000")
_PERIOD = date(2024, 12, 31)
_TODAY = date(2025, 6, 1)  # ~152 days after _PERIOD → within the 548-day freshness bound


def test_guard_passes_for_alphabet_shape() -> None:
    # Class A 5,835M < combined 12,211M; largest holder ≤ class; class period fresh.
    assert _should_use_class_denominator(
        class_shares=_GOOGL_CLASS,
        class_period_end=_PERIOD,
        combined_shares=_COMBINED,
        today=_TODAY,
        max_pie_holder_shares=Decimal("2541000000"),
    )


def test_guard_rejects_stale_class_period() -> None:
    # Class period > 548 days before today → too stale → fall back to the #1646 caveat.
    assert not _should_use_class_denominator(
        class_shares=_GOOGL_CLASS,
        class_period_end=_PERIOD,
        combined_shares=_COMBINED,
        today=date(2026, 12, 31),  # ~730 days after _PERIOD
        max_pie_holder_shares=Decimal("2541000000"),
    )


def test_guard_rejects_class_ge_combined() -> None:
    assert not _should_use_class_denominator(
        class_shares=_COMBINED,  # not a strict subset
        class_period_end=_PERIOD,
        combined_shares=_COMBINED,
        today=_TODAY,
        max_pie_holder_shares=Decimal(0),
    )


def test_guard_rejects_non_positive_class() -> None:
    assert not _should_use_class_denominator(
        class_shares=Decimal(0),
        class_period_end=_PERIOD,
        combined_shares=_COMBINED,
        today=_TODAY,
        max_pie_holder_shares=Decimal(0),
    )


def test_guard_rejects_holder_exceeds_class() -> None:
    # A holder owning more than the (mis-mapped, too-small) class exists →
    # %-inflating direction → fall back to the #1646 caveat.
    assert not _should_use_class_denominator(
        class_shares=_GOOGL_CLASS,
        class_period_end=_PERIOD,
        combined_shares=_COMBINED,
        today=_TODAY,
        max_pie_holder_shares=_GOOGL_CLASS + Decimal(1),
    )
