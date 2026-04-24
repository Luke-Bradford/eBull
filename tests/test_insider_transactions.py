"""Unit tests for ``app.services.insider_transactions`` (#429).

Form 4 XML is well-structured — the SEC publishes a schema for it.
Fixtures here model the real shape: one ownershipDocument with a
reportingOwner block and two transaction tables (non-derivative +
derivative), each containing zero or more transactions.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.insider_transactions import (
    ParsedFiling,
    ParsedTransaction,
    parse_form_4_xml,
)

_FORM_4_BASIC = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerName>Jane Smith</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-04-15</value></transactionDate>
      <transactionCoding>
        <transactionCode>P</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>250</value></transactionShares>
        <transactionPricePerShare><value>185.42</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


class TestParseForm4Xml:
    def test_basic_non_derivative_buy(self) -> None:
        """Canonical open-market purchase (transactionCode=P)."""
        parsed = parse_form_4_xml(_FORM_4_BASIC)
        assert parsed is not None
        assert parsed.filer_name == "Jane Smith"
        assert parsed.filer_role == "director|officer:Chief Financial Officer"
        assert len(parsed.transactions) == 1
        txn = parsed.transactions[0]
        assert txn.txn_date == date(2024, 4, 15)
        assert txn.txn_code == "P"
        assert txn.shares == Decimal("250")
        assert txn.price == Decimal("185.42")
        assert txn.direct_indirect == "D"
        assert txn.is_derivative is False

    def test_multi_transaction_filing(self) -> None:
        """A single Form 4 reporting two sales on different days."""
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>John Doe</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-05-01</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>200.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-05-02</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>500</value></transactionShares>
        <transactionPricePerShare><value>202.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert len(parsed.transactions) == 2
        # Row indices are deterministic source-order.
        assert parsed.transactions[0].txn_row_num == 0
        assert parsed.transactions[1].txn_row_num == 1
        assert parsed.transactions[0].txn_date == date(2024, 5, 1)
        assert parsed.transactions[1].txn_date == date(2024, 5, 2)

    def test_derivative_table_tagged(self) -> None:
        """Options / RSU transactions live under <derivativeTable> and
        must be tagged ``is_derivative=True``. Important because the
        sentiment signal weights non-derivative trades more heavily."""
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Alice Insider</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>VP</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <derivativeTable>
    <derivativeTransaction>
      <transactionDate><value>2024-06-10</value></transactionDate>
      <transactionCoding><transactionCode>A</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5000</value></transactionShares>
        <transactionPricePerShare><value>0</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert len(parsed.transactions) == 1
        assert parsed.transactions[0].is_derivative is True
        assert parsed.transactions[0].txn_code == "A"

    def test_ten_percent_owner_role(self) -> None:
        """A holder with isTenPercentOwner=1 gets the right role label."""
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Fund LLC</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isTenPercentOwner>1</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-01</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10000</value></transactionShares>
        <transactionPricePerShare><value>50.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.filer_role == "ten_percent_owner"
        assert parsed.transactions[0].direct_indirect == "I"

    def test_missing_price_is_none(self) -> None:
        """Grants (transactionCode=A) often have price=0 or missing.
        Parser must not crash — price is nullable."""
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Bob Exec</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-07-01</value></transactionDate>
      <transactionCoding><transactionCode>A</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.transactions[0].price is None
        assert parsed.transactions[0].shares == Decimal("1000")

    def test_malformed_xml_returns_none(self) -> None:
        """Garbage input must return None, not raise."""
        assert parse_form_4_xml("<not actually xml") is None
        assert parse_form_4_xml("") is None

    def test_non_form_4_root_returns_none(self) -> None:
        """A document whose root isn't ownershipDocument is not a
        Form 4 — returning None stops the ingester from upserting
        garbage when SEC's URL rot points us at the wrong document."""
        xml = "<?xml version='1.0'?><somethingElse></somethingElse>"
        assert parse_form_4_xml(xml) is None

    def test_empty_transactions_returns_none(self) -> None:
        """A Form 4 with a reporting owner but zero transactions (the
        SEC occasionally has these for ownership-only amendments) is
        treated as None so we don't insert a filer-only row."""
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Empty Filer</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
</ownershipDocument>
"""
        assert parse_form_4_xml(xml) is None

    @pytest.mark.parametrize(
        "direct_val,expected",
        [("D", "D"), ("I", "I"), ("", None), ("X", None)],
    )
    def test_direct_indirect_validation(self, direct_val: str, expected: str | None) -> None:
        """direct_indirect must be 'D' or 'I' per SEC; anything else
        (typo, empty) normalises to None so the column stays clean."""
        xml = f"""<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>X</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isOfficer>1</isOfficer></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-07-01</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1</value></transactionShares>
        <transactionPricePerShare><value>1</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>{direct_val}</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.transactions[0].direct_indirect == expected

    def test_parsed_filing_has_correct_shape(self) -> None:
        """Sanity-check the public dataclasses carry exactly the
        fields the service expects."""
        parsed = parse_form_4_xml(_FORM_4_BASIC)
        assert isinstance(parsed, ParsedFiling)
        assert isinstance(parsed.transactions[0], ParsedTransaction)
