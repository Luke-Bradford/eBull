"""#3227 tier (IM) capture: pure tests.

Spec: docs/specs/ownership/2026-10-22-3227-13f-included-managers.md § Change 1-2.

DB behaviour (upsert rule, bulk SQL aggregation, replay orders, backfill) is in ``test_thirteen_f_capture_db.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from app.providers.implementations.sec_13f import (
    ThirteenFHolding,
    ThirteenFOtherManager,
    parse_infotable_with_controls,
    parse_primary_doc,
)
from app.services.thirteen_f_capture import (
    canonical_shares,
    capture_status,
    parse_column7,
    parse_sequence_number,
    segments_payload,
)
from app.services.thirteen_f_normalise import merge_resolved_by_instrument, normalise_13f_holdings

_FIXTURES = Path(__file__).parent / "fixtures" / "sec" / "13f"
_FILED = datetime(2026, 8, 14, tzinfo=UTC)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", (1,)),
        ("1,2", (1, 2)),
        ("1, 19, 4", (1, 4, 19)),
        ("1 3 4", (1, 3, 4)),
        ("01", (1,)),
        ("2,1,2", (1, 2)),
        ("", ()),
        (None, ()),
        ("0", None),
        ("NONE", None),
        ("Pershing Square Capital Management", None),
        ("1,0", None),
        ("١", None),  # ARABIC-INDIC DIGIT ONE: str.isdigit() is True, the ASCII grammar refuses it
        ("1234567890", None),  # 10 digits
        ("+1", None),
        ("1_0", None),
    ],
)
def test_column7_grammar(raw: str | None, expected: tuple[int, ...] | None) -> None:
    assert parse_column7(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("1", 1), ("01", 1), ("999999999", 999999999), ("0", None), (" 1", None), ("١", None), (None, None)],
)
def test_sequence_number_grammar(raw: str | None, expected: int | None) -> None:
    assert parse_sequence_number(raw) == expected


def _entry(seq: str | None, cik: str | None = "0000924020") -> ThirteenFOtherManager:
    return ThirteenFOtherManager(sequence_number_raw=seq, cik=cik, name="M", form13f_file_number=None)


def _status(entries: list[ThirteenFOtherManager], **overrides: object) -> str:
    kwargs: dict[str, object] = {
        "expected_list_count": len(entries),
        "line_count": 10,
        "expected_line_count": 10,
        "value_total": Decimal(1000),
        "expected_value_total": Decimal(1000),
    }
    kwargs.update(overrides)
    return capture_status(entries, **kwargs)  # type: ignore[arg-type]


class TestCaptureStatus:
    def test_complete(self) -> None:
        assert _status([_entry("1"), _entry("2")]) == "complete"

    def test_empty_list_with_zero_count_is_complete(self) -> None:
        # Special Instruction 6.a: "If none, enter the number zero"; 7.a's NONE.
        assert _status([], expected_list_count=0) == "complete"

    def test_list_count_differs(self) -> None:
        assert _status([_entry("1")], expected_list_count=2) == "incomplete"

    def test_line_count_differs(self) -> None:
        assert _status([_entry("1")], expected_line_count=11) == "incomplete"

    @pytest.mark.parametrize("control", ["expected_list_count", "expected_line_count", "expected_value_total"])
    def test_absent_control_is_incomplete(self, control: str) -> None:
        assert _status([_entry("1")], **{control: None}) == "incomplete"

    def test_value_total_within_half_unit_per_line(self) -> None:
        assert _status([], expected_list_count=0, value_total=Decimal(1005)) == "complete"
        assert _status([], expected_list_count=0, value_total=Decimal("1005.01")) == "incomplete"

    def test_duplicate_sequence_number_is_invalid(self) -> None:
        assert _status([_entry("1"), _entry("01")]) == "invalid"

    def test_sequence_number_outside_grammar_is_invalid(self) -> None:
        assert _status([_entry("1"), _entry("A")]) == "invalid"
        assert _status([_entry(None)]) == "invalid"


def _holding(*, cusip: str = "037833100", shares: int, discretion: str | None, column7: str | None) -> ThirteenFHolding:
    return ThirteenFHolding(
        cusip=cusip,
        name_of_issuer="X",
        title_of_class="COM",
        value_usd=Decimal(shares),
        shares_or_principal=Decimal(shares),
        shares_or_principal_type="SH",
        put_call=None,
        investment_discretion=discretion,
        voting_sole=Decimal(0),
        voting_shared=Decimal(0),
        voting_none=Decimal(0),
        other_managers_raw=column7,
    )


class TestSegmentsThroughNormalisation:
    def test_mixed_discretion_survives_both_aggregation_stages(self) -> None:
        # Two CUSIPs of one instrument, with mixed Column 6/7: investment_discretion collapses to None, the segments
        # keep every (Column 6, Column 7) group.
        holdings = [
            _holding(cusip="AAA", shares=100, discretion="DFND", column7="1,2"),
            _holding(cusip="AAA", shares=50, discretion="DFND", column7="2 1"),
            _holding(cusip="AAA", shares=7, discretion="SOLE", column7=""),
            _holding(cusip="BBB", shares=3, discretion="OTR", column7="NONE"),
        ]
        normalised = normalise_13f_holdings(holdings, filed_at=_FILED)
        merged = merge_resolved_by_instrument([(1, h) for h in normalised])
        assert len(merged) == 1
        _, holding = merged[0]
        assert holding.investment_discretion is None
        assert holding.shares_or_principal == Decimal(160)
        assert holding.cusips == ("AAA", "BBB")
        assert holding.segments == (
            ("DFND", (1, 2), Decimal(150)),
            ("OTR", None, Decimal(3)),
            ("SOLE", (), Decimal(7)),
        )

    def test_prn_lines_are_not_segments(self) -> None:
        prn = _holding(shares=999, discretion="DFND", column7="1")
        prn = ThirteenFHolding(**{**prn.__dict__, "shares_or_principal_type": "PRN"})
        normalised = normalise_13f_holdings([prn, _holding(shares=5, discretion="DFND", column7="1")], filed_at=_FILED)
        assert normalised[0].segments == (("DFND", (1,), Decimal(5)),)

    def test_payload_shape_and_order(self) -> None:
        capture_id = uuid4()
        payload = segments_payload(
            capture_id,
            ["BBB", "AAA", "AAA"],
            [
                ("DFND", None, Decimal(1)),
                ("DFND", (1, 10), Decimal(2)),
                ("DFND", (1, 2), Decimal("3.0000")),
                ("DFND", (), Decimal(4)),
                (None, (1,), Decimal(5)),
            ],
        )
        assert payload == {
            "capture_id": str(capture_id),
            "cusips": ["AAA", "BBB"],
            "lines": [
                {"discretion": None, "other_managers": [1], "shares": "5"},
                {"discretion": "DFND", "other_managers": [], "shares": "4"},
                {"discretion": "DFND", "other_managers": [1, 10], "shares": "2"},
                {"discretion": "DFND", "other_managers": [1, 2], "shares": "3"},
                {"discretion": "DFND", "other_managers": None, "shares": "1"},
            ],
        }

    @pytest.mark.parametrize(("value", "text"), [("100", "100"), ("100.0000", "100"), ("12.5000", "12.5"), ("0", "0")])
    def test_canonical_shares_matches_trim_scale(self, value: str, text: str) -> None:
        assert canonical_shares(Decimal(value)) == text


class TestProviderCapture:
    def test_berkshire_golden_capture_is_complete(self) -> None:
        # Real 2024Q3 filing: 14 included managers, 121 information-table lines.
        info = parse_primary_doc((_FIXTURES / "berkshire_2024q3_primary_doc.xml").read_text())
        holdings, controls = parse_infotable_with_controls((_FIXTURES / "berkshire_2024q3_infotable.xml").read_text())
        page = info.summary_page
        assert page is not None
        assert page.other_included_managers_count == 14
        assert page.table_entry_total == 121
        assert page.table_value_total == Decimal(266378900503)
        assert len(page.other_managers) == 14
        assert all(parse_sequence_number(e.sequence_number_raw) is not None for e in page.other_managers)
        assert controls.line_count == 121
        status = capture_status(
            page.other_managers,
            expected_list_count=page.other_included_managers_count,
            line_count=controls.line_count,
            expected_line_count=page.table_entry_total,
            value_total=controls.value_total,
            expected_value_total=page.table_value_total,
        )
        assert status == "complete"
        assert any(h.other_managers_raw for h in holdings)
        assert all(parse_column7(h.other_managers_raw) is not None for h in holdings)

    def test_summary_page_entries_as_filed(self) -> None:
        xml = (_FIXTURES / "berkshire_2024q3_primary_doc.xml").read_text()
        # An entry without an otherManager child is kept (EdgarTools drops it), and the sequence number stays raw.
        xml = xml.replace("<sequenceNumber>1</sequenceNumber>", "<sequenceNumber>01</sequenceNumber>", 1)
        page = parse_primary_doc(xml).summary_page
        assert page is not None
        assert page.other_managers[0].sequence_number_raw == "01"

    def test_absent_controls_are_none(self) -> None:
        xml = (_FIXTURES / "berkshire_2024q3_primary_doc.xml").read_text()
        start = xml.index("<otherIncludedManagersCount>")
        end = xml.index("</otherIncludedManagersCount>") + len("</otherIncludedManagersCount>")
        page = parse_primary_doc(xml[:start] + xml[end:]).summary_page
        assert page is not None
        assert page.other_included_managers_count is None
        assert page.table_entry_total == 121
