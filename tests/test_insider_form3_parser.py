"""Unit tests for ``parse_form_3_xml`` (#768).

Form 3 records the *initial* holdings snapshot when an insider becomes
subject to Section 16 reporting. Tests cover the holding-row parser
shape, the row-num interleave between non-derivative + derivative
tables, and the gating returns (``None`` for malformed / wrong
documentType / no reporting owners).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.insider_transactions import (
    ParsedHolding,
    parse_form_3_xml,
)


def _wrap(inner_body: str, *, document_type: str = "3") -> str:
    """Minimal Form 3 envelope with one reporting owner — tests inject
    the holdings tables via ``inner_body``."""
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>{document_type}</documentType>
  <periodOfReport>2026-01-15</periodOfReport>
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
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  {inner_body}
  <ownerSignature>
    <signatureName>Jane Smith</signatureName>
    <signatureDate>2026-01-16</signatureDate>
  </ownerSignature>
</ownershipDocument>
"""


_NON_DERIVATIVE_HOLDING = """
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>50000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
"""


_DERIVATIVE_HOLDING = """
  <derivativeTable>
    <derivativeHolding>
      <securityTitle><value>Stock Option (Right to Buy)</value></securityTitle>
      <conversionOrExercisePrice><value>120.00</value></conversionOrExercisePrice>
      <exerciseDate><value>2025-01-01</value></exerciseDate>
      <expirationDate><value>2030-01-01</value></expirationDate>
      <underlyingSecurity>
        <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
        <underlyingSecurityShares><value>10000</value></underlyingSecurityShares>
      </underlyingSecurity>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>10000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </derivativeHolding>
  </derivativeTable>
"""


class TestGatingReturns:
    def test_empty_input_returns_none(self) -> None:
        assert parse_form_3_xml("") is None

    def test_malformed_xml_returns_none(self) -> None:
        assert parse_form_3_xml("<not-xml") is None

    def test_wrong_root_tag_returns_none(self) -> None:
        assert parse_form_3_xml("<otherDoc/>") is None

    def test_form_4_document_type_is_rejected(self) -> None:
        # Caller mis-routed a Form 4 to the Form 3 parser. Drop rather
        # than try to coerce.
        assert parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING, document_type="4")) is None

    def test_no_reporting_owners_returns_none(self) -> None:
        # Strip the reportingOwner block.
        xml = _wrap(_NON_DERIVATIVE_HOLDING).replace("<reportingOwner>", "<!--").replace("</reportingOwner>", "-->")
        assert parse_form_3_xml(xml) is None


class TestHeader:
    def test_period_of_report_and_issuer_extracted(self) -> None:
        result = parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING))
        assert result is not None
        assert result.document_type == "3"
        assert result.period_of_report is not None
        assert result.period_of_report.isoformat() == "2026-01-15"
        assert result.issuer_cik == "0000320193"
        assert result.issuer_name == "Apple Inc."
        assert result.issuer_trading_symbol == "AAPL"

    def test_amendment_document_type_3a_accepted(self) -> None:
        result = parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING, document_type="3/A"))
        assert result is not None
        assert result.document_type == "3/A"

    def test_signature_extracted(self) -> None:
        result = parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING))
        assert result is not None
        assert result.signature_name == "Jane Smith"
        assert result.signature_date is not None
        assert result.signature_date.isoformat() == "2026-01-16"

    def test_filer_extracted_from_reporting_owner(self) -> None:
        result = parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING))
        assert result is not None
        assert len(result.filers) == 1
        filer = result.filers[0]
        assert filer.filer_cik == "0001000001"
        assert filer.filer_name == "Jane Smith"
        assert filer.is_officer is True
        assert filer.officer_title == "Chief Financial Officer"


class TestNonDerivativeHolding:
    def test_single_non_derivative_row_parsed(self) -> None:
        result = parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING))
        assert result is not None
        assert len(result.holdings) == 1
        holding = result.holdings[0]
        assert holding.row_num == 0
        assert holding.is_derivative is False
        assert holding.security_title == "Common Stock"
        assert holding.shares == Decimal("50000")
        assert holding.direct_indirect == "D"
        assert holding.filer_cik == "0001000001"

    def test_share_count_above_cap_resolves_to_none(self) -> None:
        # 1e10 share count is the parser cap (matches Form 4 sanitiser).
        # A Form 3 reporting an obviously corrupt value should drop the
        # share count rather than persist a bogus number.
        body = _NON_DERIVATIVE_HOLDING.replace("50000", "99999999999")
        result = parse_form_3_xml(_wrap(body))
        assert result is not None
        assert result.holdings[0].shares is None


class TestDerivativeHolding:
    def test_single_derivative_row_parsed_with_underlying(self) -> None:
        result = parse_form_3_xml(_wrap(_DERIVATIVE_HOLDING))
        assert result is not None
        assert len(result.holdings) == 1
        holding = result.holdings[0]
        assert holding.row_num == 0
        assert holding.is_derivative is True
        assert holding.security_title == "Stock Option (Right to Buy)"
        assert holding.shares == Decimal("10000")
        assert holding.conversion_exercise_price == Decimal("120.00")
        assert holding.exercise_date is not None and holding.exercise_date.isoformat() == "2025-01-01"
        assert holding.expiration_date is not None and holding.expiration_date.isoformat() == "2030-01-01"
        assert holding.underlying_security_title == "Common Stock"
        assert holding.underlying_shares == Decimal("10000")

    def test_non_derivative_fields_are_none_on_derivative_row(self) -> None:
        result = parse_form_3_xml(_wrap(_DERIVATIVE_HOLDING))
        # The derivative-row contract: non-derivative-only fields stay
        # populated (security_title, shares, direct_indirect) — the
        # derivative-only block adds exercise / expiry / underlying.
        # Sanity-pin nothing leaks the *other* direction.
        assert result is not None
        # There is no "non-derivative-only" field on ParsedHolding
        # today; the row is fully described by the same dataclass for
        # both. Lock the row's is_derivative flag so a future schema
        # split (separate dataclasses) doesn't silently mark a
        # derivative as non-derivative.
        assert result.holdings[0].is_derivative is True


class TestRowNumInterleave:
    def test_non_derivative_first_then_derivative_with_continuous_row_num(self) -> None:
        # Non-derivative + derivative tables in the same Form 3 must
        # produce a single contiguous ``row_num`` sequence so the
        # ``(accession, row_num)`` UNIQUE key in
        # insider_initial_holdings doesn't collide between the two
        # tables.
        result = parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING + _DERIVATIVE_HOLDING))
        assert result is not None
        assert len(result.holdings) == 2
        assert result.holdings[0].row_num == 0
        assert result.holdings[0].is_derivative is False
        assert result.holdings[1].row_num == 1
        assert result.holdings[1].is_derivative is True

    def test_multiple_rows_within_one_table_increment_row_num(self) -> None:
        body = """
        <nonDerivativeTable>
          <nonDerivativeHolding>
            <securityTitle><value>Common Stock</value></securityTitle>
            <postTransactionAmounts><sharesOwnedFollowingTransaction><value>100</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
            <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
          </nonDerivativeHolding>
          <nonDerivativeHolding>
            <securityTitle><value>Common Stock</value></securityTitle>
            <postTransactionAmounts><sharesOwnedFollowingTransaction><value>200</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
            <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
          </nonDerivativeHolding>
        </nonDerivativeTable>
        """
        result = parse_form_3_xml(_wrap(body))
        assert result is not None
        assert [h.row_num for h in result.holdings] == [0, 1]
        assert [h.shares for h in result.holdings] == [Decimal("100"), Decimal("200")]
        assert [h.direct_indirect for h in result.holdings] == ["D", "I"]


class TestEmptyHoldingsAccepted:
    def test_filer_only_form_3_returns_parsed_with_no_holdings(self) -> None:
        # Form 3 declaring no positions is legal (newly appointed
        # director who holds nothing yet). Parser returns the parsed
        # filing with empty ``holdings`` so the ingester can record
        # the accession and skip it on re-runs.
        result = parse_form_3_xml(_wrap(""))
        assert result is not None
        assert result.holdings == ()


class TestNamespaceHandling:
    def test_default_xmlns_is_stripped_so_findall_succeeds(self) -> None:
        # Real Form 3 XMLs from EDGAR carry a default xmlns on the
        # root. The parser pre-strips it so namespace-blind findall
        # works — pin that behaviour here so a regression in the
        # _XMLNS_RE pre-strip is caught.
        body = _NON_DERIVATIVE_HOLDING
        wrapped = _wrap(body).replace(
            "<ownershipDocument>",
            '<ownershipDocument xmlns="http://www.sec.gov/edgar/ownership">',
        )
        result = parse_form_3_xml(wrapped)
        assert result is not None
        assert len(result.holdings) == 1
        assert result.holdings[0].shares == Decimal("50000")


@pytest.mark.parametrize(
    "raw,expected_shares",
    [
        ("0", Decimal("0")),
        ("0.0001", Decimal("0.0001")),
        ("1234567.5", Decimal("1234567.5")),
    ],
)
def test_share_count_parses_decimal_shapes(raw: str, expected_shares: Decimal) -> None:
    body = _NON_DERIVATIVE_HOLDING.replace("50000", raw)
    result = parse_form_3_xml(_wrap(body))
    assert result is not None
    assert result.holdings[0].shares == expected_shares


def test_parsed_holding_is_immutable() -> None:
    # Dataclass is frozen — pin so a future refactor doesn't silently
    # make holdings mutable (matters for hashability + the ParsedFiling
    # frozen contract elsewhere in the module).
    result = parse_form_3_xml(_wrap(_NON_DERIVATIVE_HOLDING))
    assert result is not None
    holding = result.holdings[0]
    with pytest.raises(Exception):
        holding.shares = Decimal("999")  # type: ignore[misc]
    # And it really is a ParsedHolding (not ParsedTransaction):
    assert isinstance(holding, ParsedHolding)


# ---------------------------------------------------------------------
# Codex review (#768 PR1) — fixture cases for real-world Form 3 shapes
# ---------------------------------------------------------------------


_FOOTNOTE_HOLDING = """
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>12000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership>
          <value>I</value>
          <footnoteId id="F1"/>
        </directOrIndirectOwnership>
        <natureOfOwnership>
          <value>By Trust</value>
          <footnoteId id="F1"/>
        </natureOfOwnership>
      </ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
  <footnotes>
    <footnote id="F1">Held by family trust of which the reporting person is trustee.</footnote>
  </footnotes>
"""


def test_footnote_refs_and_bodies_extracted_from_holding() -> None:
    # Codex (#768 PR1 review) — Form 3 footnotes are common on
    # indirect-ownership rows; silently dropping them violates the
    # migration-057 "every structured field surfaced" precedent.
    result = parse_form_3_xml(_wrap(_FOOTNOTE_HOLDING))
    assert result is not None
    assert len(result.footnotes) == 1
    assert result.footnotes[0].footnote_id == "F1"
    assert "family trust" in result.footnotes[0].footnote_text
    holding = result.holdings[0]
    assert len(holding.footnote_refs) >= 1
    assert any(ref.footnote_id == "F1" for ref in holding.footnote_refs)


def test_no_securities_owned_header_flag_extracted() -> None:
    # A "blank" Form 3 (newly-appointed director with no positions
    # yet) carries the noSecuritiesOwned header flag. The flag lets
    # downstream distinguish "we know they hold nothing" from "data
    # not on file" — both states have an empty holdings list, but
    # only the former is positively confirmed.
    body = "<noSecuritiesOwned>1</noSecuritiesOwned>"
    xml = _wrap(body).replace("<periodOfReport>", body + "<periodOfReport>", 1)
    # Above replacement injects the flag right before periodOfReport;
    # parser reads from root so order doesn't matter.
    result = parse_form_3_xml(xml)
    assert result is not None
    assert result.no_securities_owned is True
    assert result.holdings == ()


def test_amendment_carries_date_of_original_submission() -> None:
    # 3/A amendments link back to the original filing's date via
    # dateOfOriginalSubmission. Useful for chaining + drift detection
    # between the corrected snapshot and what was originally reported.
    body = "<dateOfOriginalSubmission>2026-01-10</dateOfOriginalSubmission>"
    xml = _wrap(_NON_DERIVATIVE_HOLDING, document_type="3/A").replace("<periodOfReport>", body + "<periodOfReport>", 1)
    result = parse_form_3_xml(xml)
    assert result is not None
    assert result.document_type == "3/A"
    assert result.date_of_original_submission is not None
    assert result.date_of_original_submission.isoformat() == "2026-01-10"


def test_value_owned_branch_populated_when_shares_branch_absent() -> None:
    # postTransactionAmounts can carry valueOwnedFollowingTransaction
    # *instead of* sharesOwnedFollowingTransaction — typical for
    # fractional-undivided-interest securities. Both columns must
    # surface or the value-branch holdings drop silently.
    body = """
      <nonDerivativeTable>
        <nonDerivativeHolding>
          <securityTitle><value>Series A Units</value></securityTitle>
          <postTransactionAmounts>
            <valueOwnedFollowingTransaction><value>250000</value></valueOwnedFollowingTransaction>
          </postTransactionAmounts>
          <ownershipNature>
            <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
          </ownershipNature>
        </nonDerivativeHolding>
      </nonDerivativeTable>
    """
    result = parse_form_3_xml(_wrap(body))
    assert result is not None
    assert len(result.holdings) == 1
    holding = result.holdings[0]
    assert holding.shares is None
    assert holding.value_owned == Decimal("250000")


def test_underlying_security_value_extracted_on_derivative_row() -> None:
    # Performance / dollar-denominated derivative grants express the
    # underlying as a value, not a share count. Mirrors the Form 4
    # underlying_value extraction added in migration 057.
    body = """
      <derivativeTable>
        <derivativeHolding>
          <securityTitle><value>Performance Stock Unit</value></securityTitle>
          <conversionOrExercisePrice><value>0.00</value></conversionOrExercisePrice>
          <exerciseDate><value>2025-01-01</value></exerciseDate>
          <expirationDate><value>2030-01-01</value></expirationDate>
          <underlyingSecurity>
            <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
            <underlyingSecurityValue><value>1500000</value></underlyingSecurityValue>
          </underlyingSecurity>
          <postTransactionAmounts>
            <sharesOwnedFollowingTransaction><value>0</value></sharesOwnedFollowingTransaction>
          </postTransactionAmounts>
          <ownershipNature>
            <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
          </ownershipNature>
        </derivativeHolding>
      </derivativeTable>
    """
    result = parse_form_3_xml(_wrap(body))
    assert result is not None
    holding = result.holdings[0]
    assert holding.underlying_security_title == "Common Stock"
    assert holding.underlying_shares is None
    assert holding.underlying_value == Decimal("1500000")


def test_invalid_direct_indirect_value_sanitised_to_none() -> None:
    # SEC enumerates only "D" and "I"; any other value is malformed.
    # Persisting it would mislead read-side filters that branch on
    # this column. Mirrors the Form 4 sanitiser.
    body = _NON_DERIVATIVE_HOLDING.replace(
        "<directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>",
        "<directOrIndirectOwnership><value>X</value></directOrIndirectOwnership>",
    )
    result = parse_form_3_xml(_wrap(body))
    assert result is not None
    assert result.holdings[0].direct_indirect is None


_TWO_OWNER_HEADER = """
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000001</rptOwnerCik>
      <rptOwnerName>Jane Smith</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CFO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001000002</rptOwnerCik>
      <rptOwnerName>Smith Family Trust</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOther>1</isOther>
      <otherText>Trust controlled by reporting officer</otherText>
    </reportingOwnerRelationship>
  </reportingOwner>
"""


def test_joint_filing_extracts_every_reporting_owner() -> None:
    # Joint-filing convention: all listed owners report jointly. PR1
    # attributes every holding row to the first listed owner (matches
    # Form 4 default_filer_cik); the full filer list still surfaces so
    # PR2's ingester can persist all owners under their own dim rows.
    xml = f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>3</documentType>
  <periodOfReport>2026-01-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  {_TWO_OWNER_HEADER}
  {_NON_DERIVATIVE_HOLDING}
</ownershipDocument>
"""
    result = parse_form_3_xml(xml)
    assert result is not None
    assert len(result.filers) == 2
    assert {f.filer_cik for f in result.filers} == {"0001000001", "0001000002"}
    # First-listed owner is the holding-row attribution.
    assert result.holdings[0].filer_cik == "0001000001"


def test_pure_ten_percent_holder_with_no_officer_director_flags() -> None:
    # 10% holders are reporting persons under Section 16 even without
    # an officer / director appointment. Pin that the parser surfaces
    # the bare role without coercing to officer / director.
    xml = f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>3</documentType>
  <periodOfReport>2026-01-15</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0009999999</rptOwnerCik>
      <rptOwnerName>Activist Capital LP</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isTenPercentOwner>1</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  {_NON_DERIVATIVE_HOLDING}
</ownershipDocument>
"""
    result = parse_form_3_xml(xml)
    assert result is not None
    assert len(result.filers) == 1
    filer = result.filers[0]
    assert filer.is_ten_percent_owner is True
    assert filer.is_officer is None or filer.is_officer is False
    assert filer.is_director is None or filer.is_director is False
