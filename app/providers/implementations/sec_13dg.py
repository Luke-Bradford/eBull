"""SEC Schedule 13D / 13G blockholder ownership parser.

Schedule 13D is filed by any entity that becomes the beneficial owner
of more than 5% of a registered class of voting equity and intends to
influence the issuer (Section 13(d) of the Exchange Act). Schedule
13G covers the same threshold but is reserved for passive holders —
qualified institutional investors, exempt investors, and passive
investors who explicitly disclaim an intent to influence (Rule
13d-1(b), (c), (d)).

Both forms became structured-XML submissions under the SEC's
Beneficial Ownership Modernization rule (effective 2024-12-19). Each
filing's ``primary_doc.xml`` is the canonical source. Pre-rule HTML
text filings are out of scope for this parser — those accessions are
older than the data window the ownership card cares about and the
ingester (PR 2) will simply skip them.

The two forms share enough cover-page structure that one parser can
serve both, but field names and namespaces diverge:

  * 13D root namespace:  ``http://www.sec.gov/edgar/schedule13D``
  * 13G root namespace:  ``http://www.sec.gov/edgar/schedule13g``

  * 13D issuer block:    ``<issuerCIK>`` / ``<issuerCUSIP>``
  * 13G issuer block:    ``<issuerCik>`` / ``<issuerCusip>`` (camelCase)

  * 13D event date:      ``<dateOfEvent>`` (MM/DD/YYYY)
  * 13G event date:      ``<eventDateRequiresFilingThisStatement>``

  * 13D reporter array:  ``<reportingPersons>/<reportingPersonInfo>+``
  * 13G reporter array:  ``<coverPageHeaderReportingPersonDetails>+``
                         (repeats directly under ``<formData>``;
                         no wrapper element)

  * 13D ownership block: flat under ``<reportingPersonInfo>``
                         (``<soleVotingPower>``, ``<percentOfClass>``)
  * 13G ownership block: nested inside
                         ``<reportingPersonBeneficiallyOwnedNumberOfShares>``
                         and ``<classPercent>``

This module is a pure parser: XML strings in, typed dataclasses out.
HTTP fetch + DB resolution stay in the service layer — providers
remain thin per the settled provider-design rule.

#766 PR 1 of 3. Subsequent PRs add the SEC walker + ingester
(PR 2) and the reader endpoint + 5th sunburst category (PR 3 — the
ownership card #729 follow-on).
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET  # noqa: S405 — SEC EDGAR is the trusted source for 13D/G XML.
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final, Literal

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


SubmissionType = Literal[
    "SCHEDULE 13D",
    "SCHEDULE 13D/A",
    "SCHEDULE 13G",
    "SCHEDULE 13G/A",
]
Status = Literal["active", "passive"]


@dataclass(frozen=True)
class BlockholderReportingPerson:
    """One reporting-person record from a 13D / 13G primary_doc.xml.

    A single accession can carry multiple reporting persons (joint
    filings — e.g. a hedge fund + its general partner + its principal
    all reporting on the same issuer). Each becomes a separate row in
    ``blockholder_filings`` so the PR 2 aggregator can detect joint
    filings via ``member_of_group`` and avoid double-counting shares.

    Field semantics:

      * ``cik`` — zero-padded 10-digit CIK string when the reporter
        is an EDGAR-registered entity. ``None`` for natural persons,
        family trusts, or foreign holdcos that have no CIK
        (``reportingPersonNoCIK = Y`` in the source).
      * ``no_cik`` — explicit boolean form of the above for callers
        that want the binary signal without a None-vs-empty check.
      * ``name`` — the reporting person's name as it appears on the
        cover page. Stripped of leading/trailing whitespace.
      * ``member_of_group`` — Item 2(a) checkbox: ``'a'`` if the
        reporter is a member of a Rule 13d-1(b)(1)(ii)(J) group,
        ``'b'`` if not. Source field is free-text on some legacy
        filings — pass-through with no validation.
      * ``type_of_reporting_person`` — SEC code (``IN``, ``CO``,
        ``OO``, ``HC``, etc.). Pass-through.
      * ``citizenship`` — state/country code (e.g. ``DE``, ``NY``,
        ``D5`` for foreign).
      * ``sole_voting_power`` / ``shared_voting_power`` /
        ``sole_dispositive_power`` / ``shared_dispositive_power`` —
        Items 5–8 of the cover page. May be ``None`` when the filing
        defers to the prior cover page rather than restating numbers
        (legal on amendments but yields a non-canonical row).
      * ``aggregate_amount_owned`` — Item 9 / 13G total beneficially
        owned. ``None`` under the same defer-to-prior-cover rule.
      * ``percent_of_class`` — Item 11 / 13G ``classPercent``. NUMERIC
        because SEC allows up to 4 decimals (e.g. ``47.6843``).
    """

    cik: str | None
    no_cik: bool
    name: str
    member_of_group: str | None
    type_of_reporting_person: str | None
    citizenship: str | None
    sole_voting_power: Decimal | None
    shared_voting_power: Decimal | None
    sole_dispositive_power: Decimal | None
    shared_dispositive_power: Decimal | None
    aggregate_amount_owned: Decimal | None
    percent_of_class: Decimal | None


@dataclass(frozen=True)
class BlockholderFiling:
    """The full parsed payload from one 13D / 13G primary_doc.xml.

    Field semantics:

      * ``submission_type`` — verbatim ``<submissionType>`` from the
        XML header. Constrained to the four legal values; the parser
        raises ``ValueError`` on any other value to make a future
        SEC schema change visible rather than silently miscategorised.
      * ``status`` — derived enum: ``13D|13D/A → active``,
        ``13G|13G/A → passive``. Set by the parser, not the ingester.
      * ``primary_filer_cik`` — zero-padded CIK of the entity that
        actually submitted the filing on EDGAR
        (``headerData/filerInfo/filer/filerCredentials/cik``). This
        is what the ``blockholder_filer_seeds`` table keys on. Note
        that this is sometimes the same as the first reporting
        person, sometimes a service company (e.g. a transfer-agent
        filing on behalf of a family trust).
      * ``issuer_cik`` — the issuing company's CIK. Joins to the
        eBull ``instruments`` table via
        ``instrument_sec_profile.cik``.
      * ``issuer_cusip`` — issuer + share-class identifier. The
        ingester (PR 2) resolves this to ``instrument_id`` via
        ``external_identifiers``. Issuer-level dedupe across share
        classes is the ingester's responsibility, not the parser's.
      * ``issuer_name`` — informational; pass-through from cover.
      * ``securities_class_title`` — the share class on the cover
        (e.g. "Class A Common Stock, par value $.01 per share").
        Pass-through; not normalised.
      * ``date_of_event`` — Item 4 / 13G "event requiring filing"
        date. ``None`` if missing or unparseable.
      * ``filed_at`` — first signature-block date in the filing,
        coerced to UTC midnight. ``None`` if no signature is found
        (rare; typically a malformed filing). The signature date is
        the closest the SEC schema gets to a filing timestamp — the
        accession-date filed-at on EDGAR's web UI carries a clock
        but is not in the primary_doc.xml itself.
      * ``reporting_persons`` — 1..N. Empty list is treated as a
        parse error and raised; an accession with zero reporting
        persons is malformed by definition.
    """

    submission_type: SubmissionType
    status: Status
    primary_filer_cik: str
    issuer_cik: str
    issuer_cusip: str
    issuer_name: str
    securities_class_title: str | None
    date_of_event: date | None
    filed_at: datetime | None
    reporting_persons: list[BlockholderReportingPerson]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


_SUBMISSION_TYPES: Final[frozenset[SubmissionType]] = frozenset(
    ("SCHEDULE 13D", "SCHEDULE 13D/A", "SCHEDULE 13G", "SCHEDULE 13G/A")
)


def _strip_ns(tag: str) -> str:
    """``{http://...}foo`` -> ``foo``. Idempotent on un-namespaced tags."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_descendant(root: ET.Element, name: str) -> ET.Element | None:
    """Find the first descendant element whose stripped name matches."""
    for el in root.iter():
        if _strip_ns(el.tag) == name:
            return el
    return None


def _find_descendants(root: ET.Element, name: str) -> list[ET.Element]:
    """Find every descendant element whose stripped name matches."""
    return [el for el in root.iter() if _strip_ns(el.tag) == name]


def _child_text(parent: ET.Element | None, name: str) -> str | None:
    """Find the first descendant of ``parent`` whose stripped name matches.

    Scoped to ``parent``'s subtree so an ambiguously-named element
    (``<name>``, ``<cik>``) under one branch can't be picked from a
    sibling branch — important on the 13D primary doc, where cover-
    page issuer info, reporting persons, and the signature block
    each have their own nested ``<name>`` elements.
    """
    if parent is None:
        return None
    for el in parent.iter():
        if _strip_ns(el.tag) == name and el.text is not None:
            text = el.text.strip()
            if text:
                return text
    return None


def _decimal_or_none(text: str | None) -> Decimal | None:
    if text is None:
        return None
    try:
        return Decimal(text.replace(",", ""))
    except InvalidOperation:
        return None


def _parse_date_loose(text: str | None) -> date | None:
    """Both 13D ``dateOfEvent`` and 13G
    ``eventDateRequiresFilingThisStatement`` ship as MM/DD/YYYY in
    the wild. Accept the SEC-form dash variant and ISO too so a
    future SEC formatting change doesn't silently null the column.
    """
    if text is None:
        return None
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_signature_datetime(text: str | None) -> datetime | None:
    """Coerce a date-only signature value to UTC midnight so the
    persisted ``filed_at TIMESTAMPTZ`` does not drift across local
    timezones (Codex #730 PR 1 review caught the same bug there)."""
    parsed = _parse_date_loose(text)
    if parsed is None:
        return None
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)


def _zero_pad_cik(text: str) -> str:
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        raise ValueError(f"primary_doc.xml carried a non-numeric CIK: {text!r}")
    return digits.zfill(10)


def _classify(submission_type: str) -> tuple[SubmissionType, Status]:
    """Validate the submission-type string against the SEC enum and
    derive the active/passive status. Raises ``ValueError`` on any
    value outside the four-form set so a schema change fails loudly
    rather than landing wrong rows."""
    if submission_type not in _SUBMISSION_TYPES:
        raise ValueError(f"unsupported submission type: {submission_type!r}")
    typed: SubmissionType = submission_type  # type: ignore[assignment]
    status: Status = "active" if typed.startswith("SCHEDULE 13D") else "passive"
    return typed, status


# ---------------------------------------------------------------------------
# 13D-specific extractors
# ---------------------------------------------------------------------------


def _parse_13d_reporting_person(node: ET.Element) -> BlockholderReportingPerson:
    """Extract one ``<reportingPersonInfo>`` block from a 13D filing.

    The 13D schema lays the ownership block flat under the reporter
    element — ``<soleVotingPower>``, ``<percentOfClass>`` etc. are
    direct children of ``<reportingPersonInfo>``. A ``<noCIK>`` flag
    marks reporters that have no EDGAR CIK (natural persons, family
    trusts).
    """
    cik_text = _child_text(node, "reportingPersonCIK")
    no_cik_text = _child_text(node, "reportingPersonNoCIK")
    no_cik = (no_cik_text or "").upper() == "Y"
    cik = _zero_pad_cik(cik_text) if (cik_text and not no_cik) else None

    name = _child_text(node, "reportingPersonName") or ""
    if not name:
        raise ValueError("13D reportingPersonInfo is missing <reportingPersonName>")

    return BlockholderReportingPerson(
        cik=cik,
        no_cik=no_cik,
        name=name,
        member_of_group=_child_text(node, "memberOfGroup"),
        type_of_reporting_person=_child_text(node, "typeOfReportingPerson"),
        citizenship=_child_text(node, "citizenshipOrOrganization"),
        sole_voting_power=_decimal_or_none(_child_text(node, "soleVotingPower")),
        shared_voting_power=_decimal_or_none(_child_text(node, "sharedVotingPower")),
        sole_dispositive_power=_decimal_or_none(_child_text(node, "soleDispositivePower")),
        shared_dispositive_power=_decimal_or_none(_child_text(node, "sharedDispositivePower")),
        aggregate_amount_owned=_decimal_or_none(_child_text(node, "aggregateAmountOwned")),
        percent_of_class=_decimal_or_none(_child_text(node, "percentOfClass")),
    )


def _extract_13d(root: ET.Element) -> tuple[str, str, str, str | None, date | None, list[BlockholderReportingPerson]]:
    """Extract the 13D-shaped subset of cover-page + reporter data.

    Returns ``(issuer_cik, issuer_cusip, issuer_name,
    securities_class_title, date_of_event, reporting_persons)``.
    """
    issuer_info = _find_descendant(root, "issuerInfo")
    issuer_cik_text = _child_text(issuer_info, "issuerCIK")
    issuer_cusip = _child_text(issuer_info, "issuerCUSIP")
    issuer_name = _child_text(issuer_info, "issuerName")

    if issuer_cik_text is None:
        raise ValueError("13D primary_doc.xml is missing <issuerCIK>")
    if issuer_cusip is None:
        raise ValueError("13D primary_doc.xml is missing <issuerCUSIP>")
    if issuer_name is None:
        raise ValueError("13D primary_doc.xml is missing <issuerName>")

    cover = _find_descendant(root, "coverPageHeader")
    securities_class_title = _child_text(cover, "securitiesClassTitle")
    date_of_event = _parse_date_loose(_child_text(cover, "dateOfEvent"))

    reporters_root = _find_descendant(root, "reportingPersons")
    if reporters_root is None:
        raise ValueError("13D primary_doc.xml is missing <reportingPersons>")
    reporter_nodes = [el for el in reporters_root if _strip_ns(el.tag) == "reportingPersonInfo"]
    if not reporter_nodes:
        raise ValueError("13D primary_doc.xml has no <reportingPersonInfo> children")

    reporters = [_parse_13d_reporting_person(n) for n in reporter_nodes]

    return (
        _zero_pad_cik(issuer_cik_text),
        issuer_cusip,
        issuer_name,
        securities_class_title,
        date_of_event,
        reporters,
    )


# ---------------------------------------------------------------------------
# 13G-specific extractors
# ---------------------------------------------------------------------------


def _parse_13g_reporting_person(node: ET.Element) -> BlockholderReportingPerson:
    """Extract one ``<coverPageHeaderReportingPersonDetails>`` block.

    13G nests the ownership numbers inside
    ``<reportingPersonBeneficiallyOwnedNumberOfShares>`` and uses
    ``<classPercent>`` instead of ``<percentOfClass>``. The reporter
    CIK is not reliably present on the 13G cover-page subtree (the
    SEC schema makes it optional); when missing, fall back to
    ``no_cik = True`` and persist the name only.
    """
    name = _child_text(node, "reportingPersonName") or ""
    if not name:
        raise ValueError("13G reporting person details missing <reportingPersonName>")

    cik_text = _child_text(node, "reportingPersonCik") or _child_text(node, "reportingPersonCIK")
    no_cik_text = _child_text(node, "reportingPersonNoCIK") or _child_text(node, "reportingPersonNoCik")
    no_cik = (no_cik_text or "").upper() == "Y" or (cik_text is None)
    cik = _zero_pad_cik(cik_text) if (cik_text and not no_cik) else None

    powers = _find_descendant(node, "reportingPersonBeneficiallyOwnedNumberOfShares")
    sole_voting = _decimal_or_none(_child_text(powers, "soleVotingPower"))
    shared_voting = _decimal_or_none(_child_text(powers, "sharedVotingPower"))
    sole_disp = _decimal_or_none(_child_text(powers, "soleDispositivePower"))
    shared_disp = _decimal_or_none(_child_text(powers, "sharedDispositivePower"))

    aggregate = _decimal_or_none(_child_text(node, "reportingPersonBeneficiallyOwnedAggregateNumberOfShares"))
    percent = _decimal_or_none(_child_text(node, "classPercent"))

    return BlockholderReportingPerson(
        cik=cik,
        no_cik=no_cik,
        name=name,
        member_of_group=_child_text(node, "memberOfGroup"),
        type_of_reporting_person=_child_text(node, "typeOfReportingPerson"),
        citizenship=_child_text(node, "citizenshipOrOrganization"),
        sole_voting_power=sole_voting,
        shared_voting_power=shared_voting,
        sole_dispositive_power=sole_disp,
        shared_dispositive_power=shared_disp,
        aggregate_amount_owned=aggregate,
        percent_of_class=percent,
    )


def _extract_13g(root: ET.Element) -> tuple[str, str, str, str | None, date | None, list[BlockholderReportingPerson]]:
    """Extract the 13G-shaped subset of cover-page + reporter data."""
    issuer_info = _find_descendant(root, "issuerInfo")
    issuer_cik_text = _child_text(issuer_info, "issuerCik") or _child_text(issuer_info, "issuerCIK")
    issuer_cusip = _child_text(issuer_info, "issuerCusip") or _child_text(issuer_info, "issuerCUSIP")
    issuer_name = _child_text(issuer_info, "issuerName")

    if issuer_cik_text is None:
        raise ValueError("13G primary_doc.xml is missing <issuerCik>")
    if issuer_cusip is None:
        raise ValueError("13G primary_doc.xml is missing <issuerCusip>")
    if issuer_name is None:
        raise ValueError("13G primary_doc.xml is missing <issuerName>")

    cover = _find_descendant(root, "coverPageHeader")
    securities_class_title = _child_text(cover, "securitiesClassTitle")
    date_of_event = _parse_date_loose(_child_text(cover, "eventDateRequiresFilingThisStatement"))

    reporter_nodes = _find_descendants(root, "coverPageHeaderReportingPersonDetails")
    if not reporter_nodes:
        raise ValueError("13G primary_doc.xml has no <coverPageHeaderReportingPersonDetails>")

    reporters = [_parse_13g_reporting_person(n) for n in reporter_nodes]

    return (
        _zero_pad_cik(issuer_cik_text),
        issuer_cusip,
        issuer_name,
        securities_class_title,
        date_of_event,
        reporters,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def parse_primary_doc(xml: str) -> BlockholderFiling:
    """Parse a 13D or 13G ``primary_doc.xml`` payload.

    Branches on ``<submissionType>`` to pick the correct cover-page
    extractor — the two schemas share enough structure that one
    return type fits, but field names and nesting differ.

    Raises ``ValueError`` on any of:

      * missing or unrecognised ``<submissionType>``
      * missing primary filer CIK
      * missing issuer CIK / CUSIP / name
      * empty reporting-persons array
      * empty / missing reporter name on any reporter row

    The ingester (PR 2) decides whether a ``ValueError`` becomes a
    ``failed`` tombstone or a hard log-and-continue — that policy
    does not belong in the parser.
    """
    root = ET.fromstring(xml)  # noqa: S314 — SEC EDGAR is the trusted source.

    submission_type_text = _child_text(_find_descendant(root, "headerData"), "submissionType")
    if submission_type_text is None:
        raise ValueError("primary_doc.xml is missing <submissionType>")
    submission_type, status = _classify(submission_type_text)

    primary_filer_cik_text = _child_text(_find_descendant(root, "filerCredentials"), "cik")
    if primary_filer_cik_text is None:
        raise ValueError("primary_doc.xml is missing the primary filer <cik>")
    primary_filer_cik = _zero_pad_cik(primary_filer_cik_text)

    if submission_type.startswith("SCHEDULE 13D"):
        issuer_cik, issuer_cusip, issuer_name, class_title, date_of_event, reporters = _extract_13d(root)
    else:
        issuer_cik, issuer_cusip, issuer_name, class_title, date_of_event, reporters = _extract_13g(root)

    signature_info = _find_descendant(root, "signatureInfo")
    signature_date = _child_text(signature_info, "date") if signature_info is not None else None
    filed_at = _parse_signature_datetime(signature_date)

    return BlockholderFiling(
        submission_type=submission_type,
        status=status,
        primary_filer_cik=primary_filer_cik,
        issuer_cik=issuer_cik,
        issuer_cusip=issuer_cusip,
        issuer_name=issuer_name,
        securities_class_title=class_title,
        date_of_event=date_of_event,
        filed_at=filed_at,
        reporting_persons=reporters,
    )
