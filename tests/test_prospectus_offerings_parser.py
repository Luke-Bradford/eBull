"""Pure-logic tests for the 424B cover offering extractor (#1816).

No DB. Five real fixtures (FPS 424B4 row-major mixed primary+resale, MLCI
424B1 row-major issuer-only, TD 424B3 column-major structured note, JEF 424B5
percent-of-principal note, ADT 424B7 resale shelf with no pricing table) ground
the three observed physical cover layouts; synthetic snippets cover the
spec-mandated edge cases (resale-only cover, range price, non-USD currency,
total-absent per-unit-only, unrecognizable body).
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from app.services.prospectus_offerings import parse_prospectus_offering

_FIXTURES = Path(__file__).parent / "fixtures" / "prospectus"


def _fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8", errors="replace")


# --- Real fixtures — the three physical cover layouts ---------------------


def test_fps_424b4_row_major_mixed_primary_and_resale() -> None:
    """IPO pricing cover: two-column rows, issuer AND selling-holder proceeds.

    Cover values hand-verified against the SEC document
    (accession 0001193125-26-294982).
    """
    o = parse_prospectus_offering(_fixture("fps_424b4.htm"), "424B4")
    assert o is not None
    assert o.price_per_unit == Decimal("49.000")
    assert o.unit_label == "Per Share"
    assert o.aggregate_offering_amount == Decimal("2138850000")
    assert o.underwriting_discount == Decimal("53471250")
    assert o.net_proceeds_to_issuer == Decimal("695409317")
    assert o.proceeds_to_selling_holders == Decimal("1389969433")
    # Mixed primary+resale: the flag is cover-derived (issuer row present ⇒
    # True) — NOT subtype-derived.
    assert o.is_issuer_offering is True
    assert o.currency == "USD"
    assert o.security_type == "Common Stock"


def test_mlci_424b1_row_major_issuer_only_with_footnotes() -> None:
    """Row-major cover with footnote markers inside the labels
    (accession 0001628280-26-002468)."""
    o = parse_prospectus_offering(_fixture("mlci_424b1.htm"), "424B1")
    assert o is not None
    assert o.price_per_unit == Decimal("25.00")
    assert o.unit_label == "Per Note"
    assert o.aggregate_offering_amount == Decimal("40000000")
    assert o.underwriting_discount == Decimal("1250000")
    assert o.net_proceeds_to_issuer == Decimal("38750000")
    assert o.proceeds_to_selling_holders is None
    assert o.is_issuer_offering is True
    assert o.security_type == "Notes"


def test_td_424b3_column_major() -> None:
    """Column-major cover: labels first, then ``Per Note $a $b $c`` and
    ``Total $A $B $C`` value rows (accession 0001140361-26-027409). Also
    exercises the prose-price-mention tightening (the table label is preceded
    by "less than the public offering price of the Notes")."""
    o = parse_prospectus_offering(_fixture("td_424b3.htm"), "424B3")
    assert o is not None
    assert o.price_per_unit == Decimal("1000.00")
    assert o.unit_label == "Per Note"
    assert o.aggregate_offering_amount == Decimal("1720000.00")
    assert o.underwriting_discount == Decimal("6880.00")
    assert o.net_proceeds_to_issuer == Decimal("1713120.00")
    assert o.is_issuer_offering is True


def test_jef_424b5_percent_of_principal_yields_nulls() -> None:
    """Structured-note cover pricing as ``100.00%`` with EMPTY ``$`` cells
    (accession 0001140361-26-027261): the trailing ``$ 1`` footnote marker
    must NOT be read as money — all money fields NULL, never fabricated. The
    issuer flag still resolves (a "Proceeds to Jefferies..." row is present)."""
    o = parse_prospectus_offering(_fixture("jef_424b5.htm"), "424B5")
    assert o is not None
    assert o.price_per_unit is None
    assert o.aggregate_offering_amount is None
    assert o.underwriting_discount is None
    assert o.net_proceeds_to_issuer is None
    assert o.unit_label == "Per Note"
    assert o.is_issuer_offering is True
    assert o.security_type == "Notes"


def test_adt_424b7_no_pricing_table_stores_null_row() -> None:
    """Resale shelf with NO Item 501(b)(3) presentation (accession
    0001703056-26-000092): a recognizable prospectus without a resolvable
    cover is a VALID null-money row ("an offering happened"), NOT a tombstone.
    The issuer flag stays NULL — never guessed from the B7 subtype."""
    o = parse_prospectus_offering(_fixture("adt_424b7.htm"), "424B7")
    assert o is not None
    assert o.is_issuer_offering is None
    assert o.price_per_unit is None
    assert o.aggregate_offering_amount is None
    assert o.net_proceeds_to_issuer is None
    assert o.proceeds_to_selling_holders is None
    assert o.security_type == "Common Stock"


# --- Synthetic edge cases --------------------------------------------------


def _cover(rows: str, head: str = "PROSPECTUS 5,000,000 Shares of Common Stock") -> str:
    return f"<html><body><p>{head}</p><table>{rows}</table></body></html>"


def test_resale_only_cover_is_issuer_false() -> None:
    """Only a selling-shareholders proceeds row ⇒ ``is_issuer_offering=False``
    with issuer proceeds NULL and holder proceeds populated (spec: the flag is
    derived from the cover proceeds rows)."""
    body = _cover(
        "Per Share Total "
        "Price to Public $ 10.00 $ 1,000,000 "
        "Underwriting Discounts and Commissions $ 0.50 $ 50,000 "
        "Proceeds to Selling Stockholders $ 9.50 $ 950,000 "
    )
    o = parse_prospectus_offering(body, "424B7")
    assert o is not None
    assert o.is_issuer_offering is False
    assert o.net_proceeds_to_issuer is None
    assert o.proceeds_to_selling_holders == Decimal("950000")
    assert o.aggregate_offering_amount == Decimal("1000000")


def test_resale_only_spaced_selling_security_holders() -> None:
    """The spaced "Selling Security Holders" rendering must classify as a
    resale row, not issuer proceeds (Codex ckpt-2)."""
    body = _cover(
        "Per Share Total "
        "Price to Public $ 10.00 $ 1,000,000 "
        "Underwriting Discounts and Commissions $ 0.50 $ 50,000 "
        "Proceeds to the Selling Security Holders $ 9.50 $ 950,000 "
    )
    o = parse_prospectus_offering(body, "424B7")
    assert o is not None
    assert o.is_issuer_offering is False
    assert o.net_proceeds_to_issuer is None
    assert o.proceeds_to_selling_holders == Decimal("950000")


def test_range_price_yields_null_price() -> None:
    """A price range ("$8.00 to $10.00") is not a priced cover — per-unit and
    aggregate stay NULL rather than reading the range bounds as two cells."""
    body = _cover(
        "Per Share Total "
        "Price to Public $ 8.00 to $ 10.00 "
        "Underwriting Discounts and Commissions $ 0.50 $ 50,000 "
        "Proceeds to us $ 9.00 $ 900,000 "
    )
    o = parse_prospectus_offering(body, "424B4")
    assert o is not None
    assert o.price_per_unit is None
    assert o.aggregate_offering_amount is None
    # The other rows still resolve.
    assert o.underwriting_discount == Decimal("50000")
    assert o.net_proceeds_to_issuer == Decimal("900000")


def test_euro_cover_detects_currency() -> None:
    body = _cover(
        "Per Share Total "
        "Price to Public € 10.00 € 1,000,000 "
        "Underwriting Discounts € 0.50 € 50,000 "
        "Proceeds to us € 9.50 € 950,000 "
    )
    o = parse_prospectus_offering(body, "424B4")
    assert o is not None
    assert o.currency == "EUR"
    # € values are not ``$``-prefixed — money fields stay NULL rather than
    # mixing currencies into a USD-shaped extraction.
    assert o.aggregate_offering_amount is None


def test_total_only_cover_without_per_unit_header() -> None:
    """A total-only presentation (no "Per X" column) fills the totals and
    leaves per-unit NULL — never divides/multiplies to fabricate."""
    body = _cover(
        "Total Price to Public $ 1,000,000 Underwriting Discounts and Commissions $ 50,000 Proceeds to us $ 950,000 "
    )
    o = parse_prospectus_offering(body, "424B5")
    assert o is not None
    assert o.price_per_unit is None
    assert o.unit_label is None
    assert o.aggregate_offering_amount == Decimal("1000000")
    assert o.underwriting_discount == Decimal("50000")
    assert o.net_proceeds_to_issuer == Decimal("950000")


def test_single_value_with_per_unit_header_is_ambiguous() -> None:
    """One ``$`` value under a two-column header can be either column ⇒ both
    NULL (never guess which cell was populated)."""
    body = _cover(
        "Per Share Total "
        "Price to Public $ 10.00 "
        "Underwriting Discounts and Commissions $ 0.50 $ 50,000 "
        "Proceeds to us $ 9.50 $ 950,000 "
    )
    o = parse_prospectus_offering(body, "424B4")
    assert o is not None
    assert o.price_per_unit is None
    assert o.aggregate_offering_amount is None


def test_unrecognizable_body_tombstones() -> None:
    assert parse_prospectus_offering("<html><body>hello world</body></html>", "424B4") is None


def test_out_of_scope_subtype_raises() -> None:
    with pytest.raises(ValueError, match="subtype"):
        parse_prospectus_offering("<html>prospectus</html>", "424B2")
