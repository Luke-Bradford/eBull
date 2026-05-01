"""Unit tests for ``app.services.insider_transactions`` (#429).

Form 4 XML is well-structured — the SEC publishes a schema for it.
These fixtures model the real 057-era shape: ``<documentType>`` +
``<issuer>`` + ``<reportingOwner>`` (with CIK) + transaction tables +
optional ``<footnotes>`` + ``<remarks>`` + ``<ownerSignature>``.

Every Form 4 XML element surfaced by the parser gets a unit test
covering both the happy and the ``None`` / missing paths.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.insider_transactions import (
    ParsedFiler,
    ParsedFiling,
    ParsedTransaction,
    _canonical_form_4_url,
    filer_role_string,
    parse_form_4_xml,
)


def _wrap(inner_body: str) -> str:
    """Wrap a transaction-body snippet with a minimal but complete
    header so individual tests can focus on the row under test.
    """
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2024-04-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Jane Smith</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  {inner_body}
</ownershipDocument>
"""


_FORM_4_BASIC = _wrap(
    """
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
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
"""
)


class TestParseForm4Xml:
    def test_basic_non_derivative_buy(self) -> None:
        parsed = parse_form_4_xml(_FORM_4_BASIC)
        assert parsed is not None
        assert len(parsed.filers) == 1
        filer = parsed.filers[0]
        assert filer.filer_cik == "0001000001"
        assert filer.filer_name == "Jane Smith"
        assert filer_role_string(filer) == "director|officer:Chief Financial Officer"
        assert len(parsed.transactions) == 1
        txn = parsed.transactions[0]
        assert txn.filer_cik == "0001000001"
        assert txn.security_title == "Common Stock"
        assert txn.txn_date == date(2024, 4, 15)
        assert txn.txn_code == "P"
        assert txn.shares == Decimal("250")
        assert txn.price == Decimal("185.42")
        assert txn.acquired_disposed_code == "A"
        assert txn.direct_indirect == "D"
        assert txn.is_derivative is False

    def test_multi_transaction_filing(self) -> None:
        xml = _wrap(
            """
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
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert len(parsed.transactions) == 2
        assert parsed.transactions[0].txn_row_num == 0
        assert parsed.transactions[1].txn_row_num == 1

    def test_derivative_table_populates_derivative_fields(self) -> None:
        """Options / RSU transactions live under <derivativeTable> and
        every derivative-specific field must land."""
        xml = _wrap(
            """
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Employee Stock Option (Right to Buy)</value></securityTitle>
      <conversionOrExercisePrice><value>185.00</value></conversionOrExercisePrice>
      <transactionDate><value>2024-06-10</value></transactionDate>
      <transactionCoding><transactionCode>A</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5000</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <exerciseDate><value>2025-06-10</value></exerciseDate>
      <expirationDate><value>2034-06-10</value></expirationDate>
      <underlyingSecurity>
        <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
        <underlyingSecurityShares><value>5000</value></underlyingSecurityShares>
      </underlyingSecurity>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </derivativeTransaction>
  </derivativeTable>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert len(parsed.transactions) == 1
        txn = parsed.transactions[0]
        assert txn.is_derivative is True
        assert txn.txn_code == "A"
        assert txn.conversion_exercise_price == Decimal("185.00")
        assert txn.exercise_date == date(2025, 6, 10)
        assert txn.expiration_date == date(2034, 6, 10)
        assert txn.underlying_security_title == "Common Stock"
        assert txn.underlying_shares == Decimal("5000")

    def test_post_transaction_shares_captured(self) -> None:
        xml = _wrap(
            """
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-15</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>500</value></transactionShares>
        <transactionPricePerShare><value>150.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>3200500</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.transactions[0].post_transaction_shares == Decimal("3200500")

    def test_10b5_1_plan_and_late_timeliness_preserved(self) -> None:
        xml = _wrap(
            """
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-15</value></transactionDate>
      <deemedExecutionDate><value>2023-11-01</value></deemedExecutionDate>
      <transactionCoding>
        <transactionCode>S</transactionCode>
        <equitySwapInvolved>0</equitySwapInvolved>
      </transactionCoding>
      <transactionTimeliness><value>L</value></transactionTimeliness>
      <transactionAmounts>
        <transactionShares><value>200</value></transactionShares>
        <transactionPricePerShare><value>200.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        txn = parsed.transactions[0]
        assert txn.deemed_execution_date == date(2023, 11, 1)
        assert txn.transaction_timeliness == "L"
        assert txn.equity_swap_involved is False

    def test_nature_of_ownership_preserved(self) -> None:
        xml = _wrap(
            """
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-15</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>1</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
        <natureOfOwnership><value>By Trust dated 2020-01-01</value></natureOfOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.transactions[0].direct_indirect == "I"
        assert parsed.transactions[0].nature_of_ownership == "By Trust dated 2020-01-01"

    def test_joint_filing_captures_both_filers(self) -> None:
        """Two ``<reportingOwner>`` blocks: both land in ``filers``,
        transactions default-attribute to the first owner."""
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000002</rptOwnerCik>
      <rptOwnerName>Smith John (Trustee)</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>
  </reportingOwner>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000003</rptOwnerCik>
      <rptOwnerName>Smith Family Trust</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOther>1</isOther><otherText>Indirect via family trust</otherText>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-20</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>160</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert [f.filer_cik for f in parsed.filers] == ["0001000002", "0001000003"]
        assert parsed.transactions[0].filer_cik == "0001000002"
        # Second filer carries the other-text detail
        assert parsed.filers[1].is_other is True
        assert parsed.filers[1].other_text == "Indirect via family trust"

    def test_footnotes_and_refs_captured(self) -> None:
        xml = _wrap(
            """
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-15</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>500</value><footnoteId id="F1"/></transactionShares>
        <transactionPricePerShare><value>150</value><footnoteId id="F2"/></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <footnotes>
    <footnote id="F1">Weighted average price across a range of $149.80-$150.20.</footnote>
    <footnote id="F2">Trade executed under 10b5-1 plan adopted 2023-11-01.</footnote>
  </footnotes>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert {fn.footnote_id for fn in parsed.footnotes} == {"F1", "F2"}
        refs = {(r.footnote_id, r.field) for r in parsed.transactions[0].footnote_refs}
        assert ("F1", "transactionShares") in refs
        assert ("F2", "transactionPricePerShare") in refs

    def test_remarks_and_signature_captured(self) -> None:
        xml = _wrap(
            """
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-15</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1</value></transactionShares>
        <transactionPricePerShare><value>1</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <remarks>Pre-arranged under Rule 10b5-1 plan.</remarks>
  <ownerSignature>
    <signatureName>/s/ Jane Q. Lawyer</signatureName>
    <signatureDate>2024-06-17</signatureDate>
  </ownerSignature>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.remarks == "Pre-arranged under Rule 10b5-1 plan."
        assert parsed.signature_name == "/s/ Jane Q. Lawyer"
        assert parsed.signature_date == date(2024, 6, 17)

    def test_amendment_carries_document_type_and_original_date(self) -> None:
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4/A</documentType>
  <periodOfReport>2024-06-01</periodOfReport>
  <dateOfOriginalSubmission>2024-06-02</dateOfOriginalSubmission>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Cook Timothy D.</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CEO</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-01</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
        <transactionPricePerShare><value>1</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.document_type == "4/A"
        assert parsed.date_of_original_submission == date(2024, 6, 2)

    def test_missing_price_is_none(self) -> None:
        xml = _wrap(
            """
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
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.transactions[0].price is None
        assert parsed.transactions[0].shares == Decimal("1000")

    def test_malformed_xml_returns_none(self) -> None:
        assert parse_form_4_xml("<not actually xml") is None
        assert parse_form_4_xml("") is None

    def test_non_form_4_root_returns_none(self) -> None:
        assert parse_form_4_xml("<?xml version='1.0'?><somethingElse></somethingElse>") is None

    def test_empty_transactions_returns_none(self) -> None:
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <issuer><issuerCik>X</issuerCik><issuerName>X</issuerName></issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001</rptOwnerCik><rptOwnerName>Empty</rptOwnerName>
    </reportingOwnerId>
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
        xml = _wrap(
            f"""
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
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.transactions[0].direct_indirect == expected

    def test_default_namespace_stripped(self) -> None:
        """Codex #429 M4 regression — a Form 4 with an inline
        ``xmlns=...`` default namespace on the root must still parse."""
        xml = """<?xml version="1.0"?>
<ownershipDocument xmlns="http://www.sec.gov/edgar/common">
  <documentType>4</documentType>
  <issuer><issuerCik>X</issuerCik><issuerName>X</issuerName></issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001</rptOwnerCik><rptOwnerName>NS Insider</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CEO</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-08-10</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>50.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.filers[0].filer_name == "NS Insider"
        assert len(parsed.transactions) == 1

    def test_row_num_stable_when_earlier_row_malformed(self) -> None:
        """Codex #429 H2 regression — malformed leading row must not
        shift later rows' row_num."""
        xml = _wrap(
            """
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <!-- missing transactionDate — today's parser skips this row -->
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-08-10</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>20</value></transactionShares>
        <transactionPricePerShare><value>50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert len(parsed.transactions) == 1
        assert parsed.transactions[0].txn_row_num == 1

    @pytest.mark.parametrize("bad", ["-100", "NaN", "Infinity", "1e20"])
    def test_decimal_validation_rejects_invalid(self, bad: str) -> None:
        xml = _wrap(
            f"""
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-08-10</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>{bad}</value></transactionShares>
        <transactionPricePerShare><value>50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
"""
        )
        parsed = parse_form_4_xml(xml)
        assert parsed is not None
        assert parsed.transactions[0].shares is None

    def test_parsed_filing_shape(self) -> None:
        parsed = parse_form_4_xml(_FORM_4_BASIC)
        assert isinstance(parsed, ParsedFiling)
        assert isinstance(parsed.filers[0], ParsedFiler)
        assert isinstance(parsed.transactions[0], ParsedTransaction)


class TestCanonicalForm4Url:
    """#454 regression — XSL-rendered HTML paths must be normalised to
    the canonical raw-XML URL before the ingester fetches them."""

    def test_strips_xslf345x06_segment(self) -> None:
        raw = "https://www.sec.gov/Archives/edgar/data/320193/000114036126015421/xslF345X06/form4.xml"
        assert _canonical_form_4_url(raw) == (
            "https://www.sec.gov/Archives/edgar/data/320193/000114036126015421/form4.xml"
        )

    def test_strips_xslf345x05_segment(self) -> None:
        raw = "https://www.sec.gov/Archives/edgar/data/320193/abc/xslF345X05/form4.xml"
        assert _canonical_form_4_url(raw) == ("https://www.sec.gov/Archives/edgar/data/320193/abc/form4.xml")

    def test_strips_xslf345_segment(self) -> None:
        raw = "https://www.sec.gov/Archives/edgar/data/320193/abc/xslF345/ownership.xml"
        assert _canonical_form_4_url(raw) == ("https://www.sec.gov/Archives/edgar/data/320193/abc/ownership.xml")

    def test_canonical_url_passes_through_unchanged(self) -> None:
        raw = "https://www.sec.gov/Archives/edgar/data/320193/abc/form4.xml"
        assert _canonical_form_4_url(raw) == raw

    def test_unrelated_path_segment_not_touched(self) -> None:
        """Defensive: ``xslF345X06`` only matches as a path segment,
        not as a substring inside another segment."""
        raw = "https://www.sec.gov/Archives/edgar/data/xslF345X06extra/form4.xml"
        assert _canonical_form_4_url(raw) == raw

    def test_strips_older_xslf345x02_segment(self) -> None:
        """Operator surfaced 2026-05-01: backfill against pre-2018
        Form 4 filings hit a 98% parse_miss rate because the
        ``xslF345X02`` / ``xslF345X03`` variants weren't matched by
        the regex. The ingester fetched the XSL-rendered HTML and
        the XML parser failed on every filing."""
        raw = "https://www.sec.gov/Archives/edgar/data/871763/000120919108030833/xslF345X02/doc4.xml"
        assert _canonical_form_4_url(raw) == (
            "https://www.sec.gov/Archives/edgar/data/871763/000120919108030833/doc4.xml"
        )

    def test_strips_older_xslf345x03_segment(self) -> None:
        raw = "https://www.sec.gov/Archives/edgar/data/871763/000120919108047089/xslF345X03/doc4.xml"
        assert _canonical_form_4_url(raw) == (
            "https://www.sec.gov/Archives/edgar/data/871763/000120919108047089/doc4.xml"
        )

    def test_strips_intermediate_xslf345x04_segment(self) -> None:
        """Future-proofing: the ``[A-Z0-9]*`` suffix matches any new
        SEC XSL variant without another regex patch."""
        raw = "https://www.sec.gov/Archives/edgar/data/320193/abc/xslF345X04/form4.xml"
        assert _canonical_form_4_url(raw) == ("https://www.sec.gov/Archives/edgar/data/320193/abc/form4.xml")


class TestFilerRoleString:
    def _filer(self, **overrides: object) -> ParsedFiler:
        base: dict[str, object] = {
            "filer_cik": "X",
            "filer_name": "X",
            "street1": None,
            "street2": None,
            "city": None,
            "state": None,
            "zip_code": None,
            "state_description": None,
            "is_director": None,
            "is_officer": None,
            "officer_title": None,
            "is_ten_percent_owner": None,
            "is_other": None,
            "other_text": None,
        }
        base.update(overrides)
        return ParsedFiler(**base)  # type: ignore[arg-type]

    def test_ten_percent_owner_only(self) -> None:
        assert filer_role_string(self._filer(is_ten_percent_owner=True)) == "ten_percent_owner"

    def test_officer_with_title(self) -> None:
        assert filer_role_string(self._filer(is_officer=True, officer_title="CFO")) == "officer:CFO"

    def test_combined_roles(self) -> None:
        result = filer_role_string(self._filer(is_director=True, is_officer=True, officer_title="CEO"))
        assert result == "director|officer:CEO"

    def test_no_flags_returns_none(self) -> None:
        assert filer_role_string(self._filer()) is None
