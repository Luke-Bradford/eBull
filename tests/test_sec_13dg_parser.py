"""Unit tests for the SEC Schedule 13D / 13G XML parser (#766 PR 1).

Fixture XML is hand-built to mirror the namespace + element shape of
real SEC primary_doc.xml payloads (sampled from accessions
0001140361-25-040863 — Estee Lauder family 13D — and
0000950103-25-014355 — Aura Minerals 13G — and
0001193125-25-270277 — Carmart 13G with 3 joint reporters).

Each scenario pins a single behaviour:

  * Status enum — 13D / 13D/A → active, 13G / 13G/A → passive.
  * Submission-type validation — unrecognised values raise.
  * Multi-reporter joint filings — one ``BlockholderReportingPerson``
    per cover-page block.
  * 13D vs 13G schema divergence — different field names, different
    nesting, different namespaces.
  * No-CIK reporters — natural persons / family trusts persisted via
    name only with ``no_cik = True``.
  * Issuer cover-page parsing — CIK / CUSIP / class title / event
    date.
  * Signature block — UTC tz-aware datetime, ``None`` when missing.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from app.providers.implementations.sec_13dg import (
    BlockholderFiling,
    parse_primary_doc,
)

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


_NS_13D = "http://www.sec.gov/edgar/schedule13D"
_NS_13G = "http://www.sec.gov/edgar/schedule13g"


def _13d_xml(
    *,
    submission_type: str = "SCHEDULE 13D",
    primary_filer_cik: str = "0002093607",
    issuer_cik: str = "0001001250",
    issuer_cusip: str = "518439104",
    issuer_name: str = "The Estee Lauder Companies Inc.",
    securities_class_title: str = "Class A Common Stock, par value $.01 per share",
    date_of_event: str | None = "11/03/2025",
    signature_date: str | None = "11/06/2025",
    reporters_xml: str = """
        <reportingPersonInfo>
          <reportingPersonCIK>0002093607</reportingPersonCIK>
          <reportingPersonNoCIK>N</reportingPersonNoCIK>
          <reportingPersonName>Roaring Fork Trust Company, Inc.</reportingPersonName>
          <memberOfGroup>b</memberOfGroup>
          <citizenshipOrOrganization>SD</citizenshipOrOrganization>
          <soleVotingPower>0</soleVotingPower>
          <sharedVotingPower>0</sharedVotingPower>
          <soleDispositivePower>0</soleDispositivePower>
          <sharedDispositivePower>0</sharedDispositivePower>
          <aggregateAmountOwned>0</aggregateAmountOwned>
          <percentOfClass>0</percentOfClass>
          <typeOfReportingPerson>CO</typeOfReportingPerson>
        </reportingPersonInfo>
        <reportingPersonInfo>
          <reportingPersonNoCIK>Y</reportingPersonNoCIK>
          <reportingPersonName>The LAL 2015 ELF Trust</reportingPersonName>
          <memberOfGroup>b</memberOfGroup>
          <citizenshipOrOrganization>NY</citizenshipOrOrganization>
          <soleVotingPower>1500000</soleVotingPower>
          <sharedVotingPower>0</sharedVotingPower>
          <soleDispositivePower>1500000</soleDispositivePower>
          <sharedDispositivePower>0</sharedDispositivePower>
          <aggregateAmountOwned>1500000</aggregateAmountOwned>
          <percentOfClass>2.5</percentOfClass>
          <typeOfReportingPerson>OO</typeOfReportingPerson>
        </reportingPersonInfo>
    """,
) -> str:
    event_block = f"<dateOfEvent>{date_of_event}</dateOfEvent>" if date_of_event else ""
    sig_block = (
        f"""
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>{signature_date}</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>"""
        if signature_date
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13D}">
  <headerData>
    <submissionType>{submission_type}</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>{primary_filer_cik}</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>{securities_class_title}</securitiesClassTitle>
      {event_block}
      <issuerInfo>
        <issuerCIK>{issuer_cik}</issuerCIK>
        <issuerCUSIP>{issuer_cusip}</issuerCUSIP>
        <issuerName>{issuer_name}</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      {reporters_xml}
    </reportingPersons>{sig_block}
  </formData>
</edgarSubmission>
"""


def _13g_xml(
    *,
    submission_type: str = "SCHEDULE 13G",
    primary_filer_cik: str = "0002083532",
    issuer_cik: str = "0001468642",
    issuer_cusip: str = "G06973112",
    issuer_name: str = "Aura Minerals Inc.",
    securities_class_title: str = "Common Shares, no par value",
    date_of_event: str | None = "09/30/2025",
    signature_date: str | None = "10/15/2025",
    reporter_blocks: str = """
        <coverPageHeaderReportingPersonDetails>
          <reportingPersonName>De Brito Paulo Carlos</reportingPersonName>
          <citizenshipOrOrganization>D5</citizenshipOrOrganization>
          <reportingPersonBeneficiallyOwnedNumberOfShares>
            <soleVotingPower>39838685.00</soleVotingPower>
            <sharedVotingPower>0.00</sharedVotingPower>
            <soleDispositivePower>39838685.00</soleDispositivePower>
            <sharedDispositivePower>0.00</sharedDispositivePower>
          </reportingPersonBeneficiallyOwnedNumberOfShares>
          <reportingPersonBeneficiallyOwnedAggregateNumberOfShares>39838685.00</reportingPersonBeneficiallyOwnedAggregateNumberOfShares>
          <classPercent>47.6843</classPercent>
          <typeOfReportingPerson>IN</typeOfReportingPerson>
        </coverPageHeaderReportingPersonDetails>
    """,
) -> str:
    event_block = (
        f"<eventDateRequiresFilingThisStatement>{date_of_event}</eventDateRequiresFilingThisStatement>"
        if date_of_event
        else ""
    )
    sig_block = (
        f"""
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>{signature_date}</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>"""
        if signature_date
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13G}">
  <headerData>
    <submissionType>{submission_type}</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>{primary_filer_cik}</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>{securities_class_title}</securitiesClassTitle>
      {event_block}
      <issuerInfo>
        <issuerCik>{issuer_cik}</issuerCik>
        <issuerName>{issuer_name}</issuerName>
        <issuerCusip>{issuer_cusip}</issuerCusip>
      </issuerInfo>
    </coverPageHeader>
    {reporter_blocks}{sig_block}
  </formData>
</edgarSubmission>
"""


# ---------------------------------------------------------------------------
# 13D parsing
# ---------------------------------------------------------------------------


def test_parse_13d_returns_active_status_and_zero_padded_ciks() -> None:
    parsed = parse_primary_doc(_13d_xml())

    assert parsed.submission_type == "SCHEDULE 13D"
    assert parsed.status == "active"
    assert parsed.primary_filer_cik == "0002093607"
    assert parsed.issuer_cik == "0001001250"
    assert parsed.issuer_cusip == "518439104"
    assert parsed.issuer_name == "The Estee Lauder Companies Inc."
    assert parsed.securities_class_title is not None
    assert parsed.securities_class_title.startswith("Class A Common Stock")
    assert parsed.date_of_event == date(2025, 11, 3)
    assert parsed.filed_at == datetime(2025, 11, 6, tzinfo=UTC)


def test_parse_13d_amendment_classifies_as_active() -> None:
    parsed = parse_primary_doc(_13d_xml(submission_type="SCHEDULE 13D/A"))
    assert parsed.submission_type == "SCHEDULE 13D/A"
    assert parsed.status == "active"


def test_parse_13d_multi_reporter_yields_one_row_each() -> None:
    parsed = parse_primary_doc(_13d_xml())

    assert len(parsed.reporting_persons) == 2

    rftc, elf = parsed.reporting_persons
    assert rftc.cik == "0002093607"
    assert rftc.no_cik is False
    assert rftc.name == "Roaring Fork Trust Company, Inc."
    assert rftc.percent_of_class == Decimal("0")
    assert rftc.type_of_reporting_person == "CO"

    assert elf.cik is None
    assert elf.no_cik is True
    assert elf.name == "The LAL 2015 ELF Trust"
    assert elf.aggregate_amount_owned == Decimal("1500000")
    assert elf.percent_of_class == Decimal("2.5")


def test_parse_13d_pads_short_cik() -> None:
    parsed = parse_primary_doc(
        _13d_xml(primary_filer_cik="12345", issuer_cik="67890"),
    )
    assert parsed.primary_filer_cik == "0000012345"
    assert parsed.issuer_cik == "0000067890"


def test_parse_13d_missing_signature_returns_none_filed_at() -> None:
    parsed = parse_primary_doc(_13d_xml(signature_date=None))
    assert parsed.filed_at is None


def test_parse_13d_missing_event_date_returns_none() -> None:
    parsed = parse_primary_doc(_13d_xml(date_of_event=None))
    assert parsed.date_of_event is None


def test_parse_13d_no_reporters_raises() -> None:
    with pytest.raises(ValueError, match="no <reportingPersonInfo>"):
        parse_primary_doc(_13d_xml(reporters_xml=""))


def test_parse_13d_missing_issuer_cusip_raises() -> None:
    xml = _13d_xml().replace("<issuerCUSIP>518439104</issuerCUSIP>", "")
    with pytest.raises(ValueError, match="<issuerCUSIP>"):
        parse_primary_doc(xml)


def test_parse_13d_missing_reporter_name_raises() -> None:
    bad_reporter = """
        <reportingPersonInfo>
          <reportingPersonCIK>0001234567</reportingPersonCIK>
          <reportingPersonNoCIK>N</reportingPersonNoCIK>
          <memberOfGroup>b</memberOfGroup>
        </reportingPersonInfo>
    """
    with pytest.raises(ValueError, match="reportingPersonName"):
        parse_primary_doc(_13d_xml(reporters_xml=bad_reporter))


# ---------------------------------------------------------------------------
# 13G parsing
# ---------------------------------------------------------------------------


def test_parse_13g_returns_passive_status() -> None:
    parsed = parse_primary_doc(_13g_xml())

    assert parsed.submission_type == "SCHEDULE 13G"
    assert parsed.status == "passive"
    assert parsed.primary_filer_cik == "0002083532"
    assert parsed.issuer_cik == "0001468642"
    assert parsed.issuer_cusip == "G06973112"
    assert parsed.issuer_name == "Aura Minerals Inc."
    assert parsed.date_of_event == date(2025, 9, 30)


def test_parse_13g_amendment_classifies_as_passive() -> None:
    parsed = parse_primary_doc(_13g_xml(submission_type="SCHEDULE 13G/A"))
    assert parsed.submission_type == "SCHEDULE 13G/A"
    assert parsed.status == "passive"


def test_parse_13g_extracts_nested_voting_powers_and_class_percent() -> None:
    parsed = parse_primary_doc(_13g_xml())

    assert len(parsed.reporting_persons) == 1
    person = parsed.reporting_persons[0]
    assert person.name == "De Brito Paulo Carlos"
    assert person.cik is None  # no CIK element on the cover-page block
    assert person.no_cik is True
    assert person.sole_voting_power == Decimal("39838685.00")
    assert person.aggregate_amount_owned == Decimal("39838685.00")
    assert person.percent_of_class == Decimal("47.6843")
    assert person.type_of_reporting_person == "IN"


def test_parse_13g_with_explicit_reporter_cik_zero_pads_and_strips_no_cik() -> None:
    """Some 13G filings carry an explicit ``<reportingPersonCik>`` even
    though the SEC schema makes it optional; the parser must zero-pad
    it like every other CIK in the codebase and report ``no_cik=False``
    when the element is present and non-empty. Both the camelCase
    (``Cik``) and the all-caps (``CIK``) tag spellings are valid in
    practice and the parser tolerates either."""
    blocks_camel = """
        <coverPageHeaderReportingPersonDetails>
          <reportingPersonCik>12345</reportingPersonCik>
          <reportingPersonName>Some Registered Holder LLC</reportingPersonName>
          <classPercent>9.9</classPercent>
        </coverPageHeaderReportingPersonDetails>
    """
    parsed = parse_primary_doc(_13g_xml(reporter_blocks=blocks_camel))
    assert len(parsed.reporting_persons) == 1
    person = parsed.reporting_persons[0]
    assert person.cik == "0000012345"
    assert person.no_cik is False
    assert person.name == "Some Registered Holder LLC"
    assert person.percent_of_class == Decimal("9.9")

    blocks_caps = blocks_camel.replace(
        "<reportingPersonCik>12345</reportingPersonCik>",
        "<reportingPersonCIK>67890</reportingPersonCIK>",
    )
    parsed_caps = parse_primary_doc(_13g_xml(reporter_blocks=blocks_caps))
    assert parsed_caps.reporting_persons[0].cik == "0000067890"
    assert parsed_caps.reporting_persons[0].no_cik is False


def test_parse_13g_multi_reporter_joint_filing() -> None:
    """Carmart-style joint filing — three reporters (firm + 2 principals)."""
    blocks = """
        <coverPageHeaderReportingPersonDetails>
          <reportingPersonName>Silver Point Capital, L.P.</reportingPersonName>
          <citizenshipOrOrganization>DE</citizenshipOrOrganization>
          <reportingPersonBeneficiallyOwnedNumberOfShares>
            <soleVotingPower>0</soleVotingPower>
            <sharedVotingPower>800000</sharedVotingPower>
            <soleDispositivePower>0</soleDispositivePower>
            <sharedDispositivePower>800000</sharedDispositivePower>
          </reportingPersonBeneficiallyOwnedNumberOfShares>
          <reportingPersonBeneficiallyOwnedAggregateNumberOfShares>800000</reportingPersonBeneficiallyOwnedAggregateNumberOfShares>
          <classPercent>15.3</classPercent>
          <typeOfReportingPerson>IA</typeOfReportingPerson>
        </coverPageHeaderReportingPersonDetails>
        <coverPageHeaderReportingPersonDetails>
          <reportingPersonName>Edward A. Mule</reportingPersonName>
          <citizenshipOrOrganization>US</citizenshipOrOrganization>
          <classPercent>15.3</classPercent>
          <typeOfReportingPerson>IN</typeOfReportingPerson>
        </coverPageHeaderReportingPersonDetails>
        <coverPageHeaderReportingPersonDetails>
          <reportingPersonName>Robert J. O'Shea</reportingPersonName>
          <citizenshipOrOrganization>US</citizenshipOrOrganization>
          <classPercent>15.3</classPercent>
          <typeOfReportingPerson>IN</typeOfReportingPerson>
        </coverPageHeaderReportingPersonDetails>
    """
    parsed = parse_primary_doc(_13g_xml(reporter_blocks=blocks))

    names = [p.name for p in parsed.reporting_persons]
    assert names == [
        "Silver Point Capital, L.P.",
        "Edward A. Mule",
        "Robert J. O'Shea",
    ]
    # All three claim the same percent — joint-filing semantics.
    percents = {p.percent_of_class for p in parsed.reporting_persons}
    assert percents == {Decimal("15.3")}
    # Only the firm has voting/dispositive numbers; the individuals
    # defer to the firm's cover page.
    assert parsed.reporting_persons[0].shared_voting_power == Decimal("800000")
    assert parsed.reporting_persons[1].shared_voting_power is None
    assert parsed.reporting_persons[2].shared_voting_power is None


def test_parse_13g_no_reporters_raises() -> None:
    with pytest.raises(ValueError, match="coverPageHeaderReportingPersonDetails"):
        parse_primary_doc(_13g_xml(reporter_blocks=""))


def test_parse_13g_missing_issuer_cik_raises() -> None:
    xml = _13g_xml().replace("<issuerCik>0001468642</issuerCik>", "")
    with pytest.raises(ValueError, match="<issuerCik>"):
        parse_primary_doc(xml)


# ---------------------------------------------------------------------------
# Submission-type validation
# ---------------------------------------------------------------------------


def test_unsupported_submission_type_raises() -> None:
    xml = _13d_xml(submission_type="SCHEDULE 13F")
    with pytest.raises(ValueError, match="unsupported submission type"):
        parse_primary_doc(xml)


def test_missing_submission_type_raises() -> None:
    xml = _13d_xml().replace("<submissionType>SCHEDULE 13D</submissionType>", "")
    with pytest.raises(ValueError, match="<submissionType>"):
        parse_primary_doc(xml)


def test_missing_primary_filer_cik_raises() -> None:
    xml = _13d_xml().replace("<cik>0002093607</cik>", "")
    # The header CIK is the first <cik> element; removing it makes
    # the parser miss the primary filer credentials block.
    with pytest.raises(ValueError, match="primary filer"):
        parse_primary_doc(xml)


# ---------------------------------------------------------------------------
# Sanity: dataclass identity
# ---------------------------------------------------------------------------


def test_returned_dataclass_is_blockholder_filing() -> None:
    parsed = parse_primary_doc(_13d_xml())
    assert isinstance(parsed, BlockholderFiling)
