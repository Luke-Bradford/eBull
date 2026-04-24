"""Unit tests for ``app.services.eight_k_events.parse_8k_filing`` (#450)."""

from __future__ import annotations

from datetime import date

from app.services.eight_k_events import (
    Parsed8KFiling,
    Parsed8KItem,
    parse_8k_filing,
)

# Minimal but realistic 8-K shape: cover page + a material agreement
# item + a 9.01 exhibits block + a signature.
_BASIC_8K = """
<html><body>
<p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>
<p>Washington, D.C. 20549</p>
<p>FORM 8-K</p>
<p>Date of Report (Date of earliest event reported): March 15, 2026</p>
<p>(Exact name of registrant as specified in its charter)</p>
<p>APEX INDUSTRIES INC.</p>
<p>(State of Incorporation: Delaware) Commission File Number 001-12345</p>
<p>Item 1.01. Entry into a Material Definitive Agreement.</p>
<p>On March 14, 2026, the Company entered into a credit agreement
   with Acme Bank for a $500 million revolving credit facility.</p>
<p>Item 9.01. Financial Statements and Exhibits.</p>
<p>(d) Exhibits.</p>
<p>99.1 Press Release dated March 15, 2026 announcing the credit facility</p>
<p>10.1 Credit Agreement dated March 14, 2026</p>
<p>SIGNATURE</p>
<p>By: /s/ Jane Smith</p>
<p>Title: Chief Financial Officer</p>
<p>Date: March 16, 2026</p>
</body></html>
"""

# 8-K/A amendment.
_AMENDMENT_8K = """
<html><body>
<p>FORM 8-K/A (Amendment No. 1)</p>
<p>Date of Report: April 1, 2026</p>
<p>(Exact name of registrant) APEX INDUSTRIES INC.</p>
<p>Item 5.02. Departure of Directors.</p>
<p>On April 1, 2026, the Company announced the departure of its CFO.</p>
<p>SIGNATURE</p>
<p>By: /s/ John Doe</p>
</body></html>
"""


class TestParse8KFiling:
    def test_basic_8k_extracts_header_items_exhibits(self) -> None:
        parsed = parse_8k_filing(
            _BASIC_8K,
            known_items=("1.01", "9.01"),
            item_labels={
                "1.01": ("Entry into a Material Definitive Agreement", "material"),
                "9.01": ("Financial Statements and Exhibits", "informational"),
            },
        )
        assert parsed is not None
        assert parsed.document_type == "8-K"
        assert parsed.is_amendment is False
        assert parsed.date_of_report == date(2026, 3, 15)
        assert parsed.reporting_party is not None
        assert "APEX INDUSTRIES" in parsed.reporting_party
        item_codes = {it.item_code for it in parsed.items}
        assert item_codes == {"1.01", "9.01"}
        # Item 1.01 body should contain the material-agreement text.
        item_101 = next(it for it in parsed.items if it.item_code == "1.01")
        assert "credit agreement" in item_101.body
        # Exhibits list captured.
        ex_numbers = {e.exhibit_number for e in parsed.exhibits}
        assert "99.1" in ex_numbers
        assert "10.1" in ex_numbers
        # Signature block.
        assert parsed.signature_name is not None
        assert "Jane Smith" in parsed.signature_name
        assert parsed.signature_title is not None
        assert "Chief Financial Officer" in parsed.signature_title

    def test_amendment_detected(self) -> None:
        parsed = parse_8k_filing(_AMENDMENT_8K, known_items=("5.02",))
        assert parsed is not None
        assert parsed.document_type == "8-K/A"
        assert parsed.is_amendment is True

    def test_missing_item_from_known_list_synthesised_as_empty(self) -> None:
        """If the HTML heading regex misses an item that ``known_items``
        declares, a row with an empty body is still produced so the
        declared item doesn't silently disappear."""
        html = """
        <html><body>
        <p>FORM 8-K</p>
        <p>Date of Report: April 1, 2026</p>
        <p>(Exact name of registrant) APEX INDUSTRIES INC.</p>
        <p>SIGNATURE By: /s/ X</p>
        </body></html>
        """
        parsed = parse_8k_filing(html, known_items=("2.02",))
        assert parsed is not None
        codes = {it.item_code for it in parsed.items}
        assert "2.02" in codes
        synthesised = next(it for it in parsed.items if it.item_code == "2.02")
        assert synthesised.body == ""

    def test_non_8k_document_returns_none(self) -> None:
        assert parse_8k_filing("<html>nothing relevant here</html>") is None

    def test_empty_input_returns_none(self) -> None:
        assert parse_8k_filing("") is None

    def test_parsed_shape(self) -> None:
        parsed = parse_8k_filing(_BASIC_8K, known_items=("1.01",))
        assert isinstance(parsed, Parsed8KFiling)
        assert parsed.items and isinstance(parsed.items[0], Parsed8KItem)
