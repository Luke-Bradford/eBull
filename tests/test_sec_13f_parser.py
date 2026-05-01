"""Unit tests for the SEC 13F-HR XML parser (#730 PR 1).

Fixture XML is hand-written to mirror the namespace + element shape
of real SEC 13F-HR filings without pulling production payloads into
the repo. Each scenario pins a single behaviour:

  * Header parsing — primary_doc.xml namespace, signature date,
    summary-page total, malformed inputs.
  * Holdings parsing — infotable.xml namespace, multi-row tables,
    voting-authority sub-amounts, put/call exposure, malformed
    rows being skipped.
  * Helper ``dominant_voting_authority`` — picks the right label
    on ties, on all-zero, and on each authority winning.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from app.providers.implementations.sec_13f import (
    ThirteenFHolding,
    dominant_voting_authority,
    parse_infotable,
    parse_primary_doc,
)

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


_PRIMARY_DOC_NS = "http://www.sec.gov/edgar/thirteenffiler"
_INFOTABLE_NS = "http://www.sec.gov/edgar/document/thirteenf/informationtable"


def _primary_doc(
    *,
    cik: str = "0001067983",
    name: str = "BERKSHIRE HATHAWAY INC",
    period: str = "09-30-2024",
    signature: str | None = "11-14-2024",
    table_total: str | None = "266380000000",
) -> str:
    """Hand-crafted primary_doc.xml mirroring SEC's namespace + shape."""
    sig_block = f"<signatureDate>{signature}</signatureDate>" if signature else ""
    summary = (
        f"<summaryPage><tableValueTotal>{table_total}</tableValueTotal></summaryPage>"
        if table_total is not None
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_PRIMARY_DOC_NS}">
  <headerData>
    <filerInfo>
      <filer>
        <credentials>
          <cik>{cik}</cik>
        </credentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>{period}</reportCalendarOrQuarter>
      <filingManager>
        <name>{name}</name>
      </filingManager>
    </coverPage>
    {summary}
    <signatureBlock>{sig_block}</signatureBlock>
  </formData>
</edgarSubmission>
"""


def _infotable_row(
    *,
    cusip: str,
    name: str = "APPLE INC",
    title: str = "COM",
    value: str = "69900000",
    shares: str = "300000000",
    shares_type: str = "SH",
    put_call: str | None = None,
    discretion: str = "SOLE",
    sole: str = "300000000",
    shared: str = "0",
    none: str = "0",
) -> str:
    put_call_xml = f"<putCall>{put_call}</putCall>" if put_call is not None else ""
    return f"""<infoTable>
  <nameOfIssuer>{name}</nameOfIssuer>
  <titleOfClass>{title}</titleOfClass>
  <cusip>{cusip}</cusip>
  <value>{value}</value>
  <shrsOrPrnAmt>
    <sshPrnamt>{shares}</sshPrnamt>
    <sshPrnamtType>{shares_type}</sshPrnamtType>
  </shrsOrPrnAmt>
  {put_call_xml}
  <investmentDiscretion>{discretion}</investmentDiscretion>
  <votingAuthority>
    <Sole>{sole}</Sole>
    <Shared>{shared}</Shared>
    <None>{none}</None>
  </votingAuthority>
</infoTable>"""


def _infotable(rows: list[str]) -> str:
    body = "\n  ".join(rows)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="{_INFOTABLE_NS}">
  {body}
</informationTable>
"""


# ---------------------------------------------------------------------------
# parse_primary_doc
# ---------------------------------------------------------------------------


class TestParsePrimaryDoc:
    def test_round_trip_typical_filing(self) -> None:
        info = parse_primary_doc(_primary_doc())
        assert info.cik == "0001067983"
        assert info.name == "BERKSHIRE HATHAWAY INC"
        assert info.period_of_report == date(2024, 9, 30)
        assert info.filed_at == datetime(2024, 11, 14)
        assert info.table_value_total_usd == Decimal("266380000000")

    def test_missing_signature_block_returns_none_filed_at(self) -> None:
        info = parse_primary_doc(_primary_doc(signature=None))
        assert info.filed_at is None
        # Other fields still populate.
        assert info.cik == "0001067983"
        assert info.period_of_report == date(2024, 9, 30)

    def test_missing_table_value_total_is_optional(self) -> None:
        info = parse_primary_doc(_primary_doc(table_total=None))
        assert info.table_value_total_usd is None

    def test_zero_pads_short_cik(self) -> None:
        """SEC CIKs are 10-digit; some filings carry the unpadded form."""
        info = parse_primary_doc(_primary_doc(cik="1067983"))
        assert info.cik == "0001067983"

    def test_strips_non_numeric_cik_chars(self) -> None:
        """Defensive: a future filer / agent might prefix CIK with 'CIK'."""
        info = parse_primary_doc(_primary_doc(cik="CIK0001067983"))
        assert info.cik == "0001067983"

    def test_missing_cik_raises_value_error(self) -> None:
        broken = _primary_doc().replace("<cik>0001067983</cik>", "")
        with pytest.raises(ValueError, match="missing a <cik>"):
            parse_primary_doc(broken)

    def test_missing_filing_manager_name_raises_value_error(self) -> None:
        broken = _primary_doc().replace("<name>BERKSHIRE HATHAWAY INC</name>", "")
        with pytest.raises(ValueError, match="missing the filingManager"):
            parse_primary_doc(broken)

    def test_missing_period_raises_value_error(self) -> None:
        broken = _primary_doc().replace("<reportCalendarOrQuarter>09-30-2024</reportCalendarOrQuarter>", "")
        with pytest.raises(ValueError, match="reportCalendarOrQuarter"):
            parse_primary_doc(broken)

    def test_iso_date_period_is_accepted(self) -> None:
        """A future SEC schema change to ISO is parseable."""
        info = parse_primary_doc(_primary_doc(period="2024-09-30"))
        assert info.period_of_report == date(2024, 9, 30)

    def test_signature_block_name_does_not_shadow_filing_manager_name(self) -> None:
        """Codex pre-push regression. Real 13F-HR XML carries a
        ``<name>`` on the signer in ``<signatureBlock>`` AND the filing
        manager's ``<name>`` on the cover page. The parser must scope
        its lookup to the filingManager subtree so the wrong value
        cannot be silently picked up. A document-wide first-match
        lookup would return the signer's name on a malformed filing
        with the elements reordered (or simply on any payload where
        signatureBlock precedes the cover page in document order).
        """
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <filer>
        <credentials>
          <cik>0001067983</cik>
        </credentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <signatureBlock>
      <name>WARREN E. BUFFETT</name>
      <title>CHAIRMAN</title>
      <signatureDate>11-14-2024</signatureDate>
    </signatureBlock>
    <coverPage>
      <reportCalendarOrQuarter>09-30-2024</reportCalendarOrQuarter>
      <filingManager>
        <name>BERKSHIRE HATHAWAY INC</name>
      </filingManager>
    </coverPage>
  </formData>
</edgarSubmission>
"""
        info = parse_primary_doc(xml)
        assert info.name == "BERKSHIRE HATHAWAY INC"
        assert info.filed_at == datetime(2024, 11, 14)


# ---------------------------------------------------------------------------
# parse_infotable
# ---------------------------------------------------------------------------


class TestParseInfotable:
    def test_single_holding_round_trip(self) -> None:
        xml = _infotable([_infotable_row(cusip="037833100")])
        holdings = parse_infotable(xml)
        assert len(holdings) == 1
        h = holdings[0]
        assert h.cusip == "037833100"
        assert h.name_of_issuer == "APPLE INC"
        assert h.title_of_class == "COM"
        assert h.value_usd == Decimal("69900000")
        assert h.shares_or_principal == Decimal("300000000")
        assert h.shares_or_principal_type == "SH"
        assert h.put_call is None
        assert h.investment_discretion == "SOLE"
        assert h.voting_sole == Decimal("300000000")
        assert h.voting_shared == Decimal(0)
        assert h.voting_none == Decimal(0)

    def test_multiple_holdings(self) -> None:
        xml = _infotable(
            [
                _infotable_row(cusip="037833100", name="APPLE INC"),
                _infotable_row(cusip="594918104", name="MICROSOFT CORP", value="42000000"),
                _infotable_row(cusip="023135106", name="AMAZON COM INC", value="36000000"),
            ]
        )
        holdings = parse_infotable(xml)
        assert len(holdings) == 3
        cusips = [h.cusip for h in holdings]
        assert cusips == ["037833100", "594918104", "023135106"]

    def test_put_call_normalises_to_uppercase(self) -> None:
        xml = _infotable(
            [
                _infotable_row(cusip="037833100", put_call="Put"),
                _infotable_row(cusip="037833101", put_call="CALL"),
                _infotable_row(cusip="037833102", put_call="put"),
            ]
        )
        holdings = parse_infotable(xml)
        assert holdings[0].put_call == "PUT"
        assert holdings[1].put_call == "CALL"
        assert holdings[2].put_call == "PUT"

    def test_unknown_put_call_falls_back_to_none(self) -> None:
        """A future schema field smuggled into putCall must not corrupt
        the constrained Literal — fall back to None and warn."""
        xml = _infotable([_infotable_row(cusip="037833100", put_call="Straddle")])
        holdings = parse_infotable(xml)
        assert holdings[0].put_call is None

    def test_voting_authority_breakdown_preserved(self) -> None:
        xml = _infotable(
            [
                _infotable_row(
                    cusip="037833100",
                    sole="200000000",
                    shared="50000000",
                    none="50000000",
                )
            ]
        )
        h = parse_infotable(xml)[0]
        assert h.voting_sole == Decimal("200000000")
        assert h.voting_shared == Decimal("50000000")
        assert h.voting_none == Decimal("50000000")

    def test_principal_amount_holding(self) -> None:
        """Bond holdings report PRN, not SH, in sshPrnamtType. Parser
        preserves the type — the service layer chooses how to render
        it (the ownership card consumes only SH rows)."""
        xml = _infotable([_infotable_row(cusip="912828YT0", shares="100000", shares_type="PRN")])
        h = parse_infotable(xml)[0]
        assert h.shares_or_principal == Decimal("100000")
        assert h.shares_or_principal_type == "PRN"

    def test_malformed_row_with_missing_value_is_skipped(self) -> None:
        """A row missing <value> is unparseable. The parser keeps the
        well-formed siblings rather than aborting the whole infotable."""
        xml = _infotable(
            [
                _infotable_row(cusip="037833100"),
                # Hand-crafted broken row: no <value> element.
                """<infoTable>
                    <nameOfIssuer>UNKNOWN</nameOfIssuer>
                    <cusip>UNKNOWN111</cusip>
                    <shrsOrPrnAmt>
                      <sshPrnamt>1000</sshPrnamt>
                      <sshPrnamtType>SH</sshPrnamtType>
                    </shrsOrPrnAmt>
                </infoTable>""",
                _infotable_row(cusip="594918104"),
            ]
        )
        holdings = parse_infotable(xml)
        cusips = [h.cusip for h in holdings]
        assert cusips == ["037833100", "594918104"]

    def test_malformed_row_with_non_numeric_value_is_skipped(self) -> None:
        xml = _infotable(
            [
                _infotable_row(cusip="037833100", value="N/A"),
                _infotable_row(cusip="594918104"),
            ]
        )
        holdings = parse_infotable(xml)
        assert len(holdings) == 1
        assert holdings[0].cusip == "594918104"

    def test_value_with_thousands_comma_separator(self) -> None:
        """Pre-2018 filings sometimes carry comma-separated thousands."""
        xml = _infotable([_infotable_row(cusip="037833100", value="69,900,000")])
        h = parse_infotable(xml)[0]
        assert h.value_usd == Decimal("69900000")

    def test_empty_infotable_returns_empty_list(self) -> None:
        xml = _infotable([])
        assert parse_infotable(xml) == []


# ---------------------------------------------------------------------------
# dominant_voting_authority
# ---------------------------------------------------------------------------


class TestDominantVotingAuthority:
    def _holding(self, *, sole: int, shared: int, none: int) -> ThirteenFHolding:
        return ThirteenFHolding(
            cusip="037833100",
            name_of_issuer="APPLE INC",
            title_of_class="COM",
            value_usd=Decimal("69900000"),
            shares_or_principal=Decimal("300000000"),
            shares_or_principal_type="SH",
            put_call=None,
            investment_discretion="SOLE",
            voting_sole=Decimal(sole),
            voting_shared=Decimal(shared),
            voting_none=Decimal(none),
        )

    def test_sole_dominant(self) -> None:
        h = self._holding(sole=300, shared=10, none=0)
        assert dominant_voting_authority(h) == "SOLE"

    def test_shared_dominant(self) -> None:
        h = self._holding(sole=10, shared=300, none=20)
        assert dominant_voting_authority(h) == "SHARED"

    def test_none_dominant(self) -> None:
        h = self._holding(sole=0, shared=0, none=500)
        assert dominant_voting_authority(h) == "NONE"

    def test_all_zero_returns_none_label(self) -> None:
        """All-zero is rare but legal in the SEC schema (filing
        manager has no voting authority over the position). Maps to
        NULL on insert via the canonical column."""
        h = self._holding(sole=0, shared=0, none=0)
        assert dominant_voting_authority(h) is None

    def test_tie_prefers_sole(self) -> None:
        """When sole == shared, sole wins. Documented preference for
        ownership-card reporting consistency."""
        h = self._holding(sole=100, shared=100, none=0)
        assert dominant_voting_authority(h) == "SOLE"
