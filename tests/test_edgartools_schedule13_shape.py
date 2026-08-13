"""Contract test pinning the edgartools ``Schedule13D / Schedule13G``
``parse_xml`` shape so a library upgrade that renames either the
top-level dict keys or the nested-dataclass attribute names breaks CI
immediately (#1233 PR11, edgartools skill G15).

The manifest-worker adapter at
``app/services/manifest_parsers/_schedule13_adapter.py`` reads:

* top-level dict keys: ``"issuer_info"``, ``"security_info"``,
  ``"reporting_persons"``, ``"date_of_event"`` (13D) /
  ``"event_date"`` (13G), ``"signatures"``;
* nested attribute access on the dataclasses:
  ``IssuerInfo.cik`` / ``.name`` / ``.cusip``,
  ``SecurityInfo.cusip`` / ``.title``,
  ``ReportingPerson.aggregate_amount`` (NOT
  ``aggregate_amount_owned``) / ``.percent_of_class`` / ``.no_cik`` /
  ``.sole_voting_power`` etc.

Constructing a ``Schedule13D`` Pydantic instance from the dict requires
7 positional args including a ``filing`` ref the worker does not have
— the adapter intentionally uses the dict + dataclass attrs without
constructing the Pydantic object. This contract test pins both
contracts via ``inspect.signature``.

If edgartools 5.31+ renames any of these, the test fails loudly and
the adapter is updated in the same PR rather than silently miscarrying
field values into ``blockholder_filings``.
"""

from __future__ import annotations

import inspect

from edgar.beneficial_ownership.models import (
    IssuerInfo,
    ReportingPerson,
    SecurityInfo,
    Signature,
)
from edgar.beneficial_ownership.schedule13 import Schedule13D, Schedule13G

# Minimal valid SC 13D XML using the post-mandate ``<coverPageHeader>``
# element (NOT the in-house parser's ``<coverPage>``; edgartools requires
# the mandated element name per the parse_xml source at
# .venv/lib/.../schedule13.py:163-166 — "Invalid XML: missing
# <coverPageHeader>"). Date format is ISO so the adapter's
# ``date.fromisoformat`` path is exercised; the production fixture
# tolerates MM/DD/YYYY by catching ``ValueError`` upstream.
_FIXTURE_13D_XML = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D">
  <headerData>
    <submissionType>SCHEDULE 13D</submissionType>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Class A Common Stock, par value $.01 per share</securitiesClassTitle>
      <dateOfEvent>2025-11-03</dateOfEvent>
      <issuerInfo>
        <issuerCIK>0001001250</issuerCIK>
        <issuerCUSIP>518439104</issuerCUSIP>
        <issuerName>The Estee Lauder Companies Inc.</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo>
        <reportingPersonCIK>0002093607</reportingPersonCIK>
        <reportingPersonNoCIK>N</reportingPersonNoCIK>
        <reportingPersonName>Roaring Fork Trust Company, Inc.</reportingPersonName>
        <memberOfGroup>b</memberOfGroup>
        <citizenshipOrOrganization>SD</citizenshipOrOrganization>
        <soleVotingPower>1500000</soleVotingPower>
        <sharedVotingPower>0</sharedVotingPower>
        <soleDispositivePower>1500000</soleDispositivePower>
        <sharedDispositivePower>0</sharedDispositivePower>
        <aggregateAmountOwned>1500000</aggregateAmountOwned>
        <percentOfClass>5.5</percentOfClass>
        <typeOfReportingPerson>CO</typeOfReportingPerson>
      </reportingPersonInfo>
    </reportingPersons>
    <signatureInfo>
      <signaturePerson>
        <signatureReportingPerson>Roaring Fork Trust Company, Inc.</signatureReportingPerson>
        <signatureDetails>
          <signature>/s/ Roaring Fork</signature>
          <title>Authorized Signatory</title>
          <date>2025-11-06</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
"""


# Minimal valid SC 13G XML — note the schedule13G shape differs:
# event date is ``<eventDateRequiresFilingThisStatement>`` (NOT
# ``<dateOfEvent>``), reporting persons live in
# ``<coverPageHeaderReportingPersonDetails>`` (NOT
# ``<reportingPersons>/<reportingPersonInfo>``), and CIK is NOT
# present in the cover-page block. The adapter dispatches the date-key
# read on ``source`` (``date_of_event`` vs ``event_date``).
_FIXTURE_13G_XML = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13g">
  <headerData>
    <submissionType>SCHEDULE 13G</submissionType>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Common Shares, no par value</securitiesClassTitle>
      <eventDateRequiresFilingThisStatement>2025-09-30</eventDateRequiresFilingThisStatement>
      <issuerInfo>
        <issuerCik>0001468642</issuerCik>
        <issuerName>Aura Minerals Inc.</issuerName>
        <issuerCusip>G06973112</issuerCusip>
      </issuerInfo>
    </coverPageHeader>
    <coverPageHeaderReportingPersonDetails>
      <reportingPersonName>De Brito Paulo Carlos</reportingPersonName>
      <citizenshipOrOrganization>D5</citizenshipOrOrganization>
      <reportingPersonBeneficiallyOwnedNumberOfShares>
        <soleVotingPower>39838685</soleVotingPower>
        <sharedVotingPower>0</sharedVotingPower>
        <soleDispositivePower>39838685</soleDispositivePower>
        <sharedDispositivePower>0</sharedDispositivePower>
      </reportingPersonBeneficiallyOwnedNumberOfShares>
      <reportingPersonBeneficiallyOwnedAggregateNumberOfShares>39838685</reportingPersonBeneficiallyOwnedAggregateNumberOfShares>
      <classPercent>47.6843</classPercent>
      <typeOfReportingPerson>IN</typeOfReportingPerson>
    </coverPageHeaderReportingPersonDetails>
  </formData>
</edgarSubmission>
"""


# ---------------------------------------------------------------------------
# Top-level dict contract
# ---------------------------------------------------------------------------


def test_schedule13d_parse_xml_returns_dict_with_expected_top_level_keys() -> None:
    """Adapter relies on dict-key access for the outer layer; pin it."""
    parsed = Schedule13D.parse_xml(_FIXTURE_13D_XML)

    assert isinstance(parsed, dict), "edgartools Schedule13D.parse_xml must return dict (G15)"
    # Adapter reads these specific keys; absence breaks _parse_13dg.
    assert "issuer_info" in parsed
    assert "security_info" in parsed
    assert "reporting_persons" in parsed
    assert "date_of_event" in parsed
    assert "signatures" in parsed


def test_schedule13g_parse_xml_returns_dict_with_event_date_key() -> None:
    """13G uses ``event_date`` (NOT ``date_of_event``) — adapter
    dispatches on source so this contract pins the divergence."""
    parsed = Schedule13G.parse_xml(_FIXTURE_13G_XML)

    assert isinstance(parsed, dict)
    assert "issuer_info" in parsed
    assert "security_info" in parsed
    assert "reporting_persons" in parsed
    # 13G calls it event_date; 13D calls it date_of_event. Adapter
    # dispatches on the manifest source param.
    assert "event_date" in parsed
    assert "date_of_event" not in parsed


# ---------------------------------------------------------------------------
# Nested dataclass attribute-access contract
# ---------------------------------------------------------------------------


def test_issuer_info_is_dataclass_with_cik_name_cusip_attrs() -> None:
    parsed = Schedule13D.parse_xml(_FIXTURE_13D_XML)
    issuer = parsed["issuer_info"]

    assert isinstance(issuer, IssuerInfo)
    # Attribute access (NOT dict-key) — pin so a future Pydantic
    # migration doesn't silently flip to dict semantics.
    assert issuer.cik == "0001001250"
    assert issuer.name == "The Estee Lauder Companies Inc."
    assert issuer.cusip == "518439104"


def test_security_info_is_dataclass_with_cusip_and_title() -> None:
    parsed = Schedule13D.parse_xml(_FIXTURE_13D_XML)
    sec_info = parsed["security_info"]

    assert isinstance(sec_info, SecurityInfo)
    # Adapter uses SecurityInfo.cusip (share-class CUSIP) for the
    # observation-layer instrument disambiguation, NOT IssuerInfo.cusip.
    assert sec_info.cusip == "518439104"
    assert sec_info.title == "Class A Common Stock, par value $.01 per share"


def test_reporting_person_is_dataclass_with_aggregate_amount_attr() -> None:
    """``ReportingPerson.aggregate_amount`` (NOT
    ``aggregate_amount_owned``) — Codex 1b HIGH on the PR11 spec."""
    parsed = Schedule13D.parse_xml(_FIXTURE_13D_XML)
    persons = parsed["reporting_persons"]

    assert isinstance(persons, list)
    assert len(persons) == 1
    person = persons[0]

    assert isinstance(person, ReportingPerson)
    # Attribute names the adapter relies on — every one of these is
    # load-bearing for the BlockholderReportingPerson mapping.
    assert person.cik == "0002093607"
    assert person.name == "Roaring Fork Trust Company, Inc."
    assert person.citizenship == "SD"
    assert person.sole_voting_power == 1500000
    assert person.shared_voting_power == 0
    assert person.sole_dispositive_power == 1500000
    assert person.shared_dispositive_power == 0
    assert person.aggregate_amount == 1500000  # NOT aggregate_amount_owned
    assert person.percent_of_class == 5.5
    assert person.type_of_reporting_person == "CO"
    assert person.member_of_group == "b"
    assert person.no_cik is False  # bool, not str — adapter relies on this


def test_signatures_are_dataclasses() -> None:
    parsed = Schedule13D.parse_xml(_FIXTURE_13D_XML)
    sigs = parsed["signatures"]

    assert isinstance(sigs, list)
    assert len(sigs) >= 1
    assert isinstance(sigs[0], Signature)


# ---------------------------------------------------------------------------
# Pydantic constructor positional-arg contract
# ---------------------------------------------------------------------------


def test_schedule13d_init_requires_seven_positional_args_including_filing() -> None:
    """Pin the constructor shape so a future edgartools change that drops
    the ``filing`` requirement doesn't silently invalidate the adapter's
    "don't construct the Pydantic object" assumption (G15)."""
    sig = inspect.signature(Schedule13D.__init__)
    # Drop self
    params = [p for name, p in sig.parameters.items() if name != "self"]

    # The adapter's contract assumes >= 7 positional-or-keyword params
    # (filing, issuer_info, security_info, reporting_persons, items,
    # signatures, date_of_event, [previously_filed, amendment_number]).
    # If this count drops, re-evaluate whether constructing the object
    # is now feasible without the filing ref.
    assert len(params) >= 7
    # ``filing`` must be present as a required param — that's the one
    # the worker doesn't have, motivating the dict-only adapter.
    filing_param = next((p for p in params if p.name == "filing"), None)
    assert filing_param is not None
    assert filing_param.default is inspect.Parameter.empty
