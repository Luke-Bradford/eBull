"""SEC 13F-HR institutional holdings parser.

13F-HR is filed quarterly by every institutional manager with
discretionary AUM exceeding $100M (15 USC 78m(f)). Each filing has
two XML attachments:

  * ``primary_doc.xml`` — header, filer metadata, cover page (period
    of report, filing manager name), summary page (total table value
    and table-entry count).
  * ``infotable.xml`` (sometimes named with the accession's date
    suffix) — the holdings list. One ``<infoTable>`` element per
    issuer + share class, with CUSIP, value, share count or
    principal amount, voting authority breakdown, and put/call
    indicator for option exposure.

This module is a pure parser: XML strings in, typed dataclasses out.
HTTP fetch + DB resolution stay in the service layer — providers
remain thin per the settled provider-design rule.

XML namespaces:

  * primary_doc.xml uses ``http://www.sec.gov/edgar/thirteenffiler``
    for cover-page elements.
  * infotable.xml uses
    ``http://www.sec.gov/edgar/document/thirteenf/informationtable``.

Both are stripped at parse time via :func:`_strip_ns` so callers
work with bare element names (``infoTable``, not the namespaced form).

#730 PR 1 of 4. Subsequent PRs add the ingester (PR 2),
filer-type classifier (PR 3), and reader API + frontend wiring
(PR 4 — the ownership card #729 follow-on).
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET  # noqa: S405 — SEC EDGAR is the trusted source for 13F-HR XML.
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Final, Literal

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ThirteenFFilerInfo:
    """Header + cover-page metadata extracted from primary_doc.xml.

    Field semantics:
      * ``cik`` — zero-padded 10-digit CIK string. Filers are sometimes
        called Reporting Managers in the 13F instructions; the CIK
        joins to the rest of the SEC ingest in this repo.
      * ``name`` — filing manager's name as it appears on the cover.
      * ``period_of_report`` — fiscal-quarter end date that the
        holdings reflect. Note: 13F-HR is a quarterly snapshot, not
        a continuous record.
      * ``filed_at`` — header signature date. NULL when the parser
        cannot find a signature block (rare; typically a malformed
        filing).
      * ``table_value_total_usd`` — summary page total in USD. SEC
        instructions changed in 2022 from "thousands" to "whole
        dollars" — this value is whatever the filer entered. Unit
        normalisation lives in the service layer.
    """

    cik: str
    name: str
    period_of_report: date
    filed_at: datetime | None
    table_value_total_usd: Decimal | None


@dataclass(frozen=True)
class ThirteenFHolding:
    """One ``<infoTable>`` row from an infotable.xml attachment.

    Field semantics:
      * ``cusip`` — issuer + share-class identifier (9 chars). Joined
        in the service layer via ``external_identifiers`` to map to
        the eBull instrument_id.
      * ``name_of_issuer`` / ``title_of_class`` — filer-supplied
        labels; informational only, not used for resolution.
      * ``value_usd`` — Column 4 of the 13F-HR. Whole dollars
        post-2022; thousands pre-2022. The parser does NOT normalise
        — service layer applies any conversion based on
        ``period_of_report``.
      * ``shares_or_principal`` — Column 5 quantity. Type is in
        ``shares_or_principal_type`` (``SH`` for shares, ``PRN``
        for principal-amount-of-bonds-style holdings).
      * ``put_call`` — option exposure indicator. Underlying-equity
        rows leave this NULL.
      * ``investment_discretion`` — ``SOLE`` / ``DEFINED`` / ``OTR``;
        retained as a free-text label for audit, not used for the
        ownership-card slice computation.
      * ``voting_sole`` / ``voting_shared`` / ``voting_none`` — three
        sub-amounts on the votingAuthority breakdown. Ownership-card
        consumers want the dominant authority; the service layer
        derives that label and writes the canonical
        ``voting_authority`` column.
    """

    cusip: str
    name_of_issuer: str
    title_of_class: str
    value_usd: Decimal
    shares_or_principal: Decimal
    shares_or_principal_type: str
    put_call: Literal["PUT", "CALL"] | None
    investment_discretion: str | None
    voting_sole: Decimal
    voting_shared: Decimal
    voting_none: Decimal


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


_NUMERIC_FIELDS: Final[tuple[str, ...]] = (
    "value",
    "sshPrnamt",
    "Sole",
    "Shared",
    "None",
)


def _strip_ns(tag: str) -> str:
    """``{http://...}foo`` -> ``foo``. Idempotent on un-namespaced tags."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _walk_text(root: ET.Element, name: str) -> str | None:
    """Find the first descendant element whose stripped name matches."""
    for el in root.iter():
        if _strip_ns(el.tag) == name and el.text is not None:
            text = el.text.strip()
            if text:
                return text
    return None


def _find_descendant(root: ET.Element, name: str) -> ET.Element | None:
    """Find the first descendant element whose stripped name matches.

    Returns the element so callers can scope further sub-element
    lookups to it — ``_walk_text`` searches the entire tree, which
    is unsafe for ambiguous element names like ``<name>`` that can
    appear in multiple unrelated subtrees (filingManager, signature
    block, internalUseFile, etc.).
    """
    for el in root.iter():
        if _strip_ns(el.tag) == name:
            return el
    return None


def _child_text(parent: ET.Element, name: str) -> str | None:
    """Like ``_walk_text`` but scoped to ``parent``'s descendants."""
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


def _parse_date_mmddyyyy(text: str | None) -> date | None:
    """13F cover-page reportCalendarOrQuarter ships as MM-DD-YYYY."""
    if text is None:
        return None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_signature_date(text: str | None) -> datetime | None:
    """Signature block carries a date but no time. Coerce to midnight
    UTC and tag the timezone explicitly so the value lands in
    ``filed_at TIMESTAMPTZ`` without psycopg falling back to the
    server's local zone (which differs from UTC on non-UTC dev hosts
    and would cause cross-tz drift in the persisted timestamp)."""
    parsed = _parse_date_mmddyyyy(text)
    if parsed is None:
        return None
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)


def _zero_pad_cik(text: str) -> str:
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        raise ValueError(f"primary_doc.xml carried a non-numeric CIK: {text!r}")
    return digits.zfill(10)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def parse_primary_doc(xml: str) -> ThirteenFFilerInfo:
    """Parse a 13F-HR ``primary_doc.xml`` payload.

    Raises ``ValueError`` if the document is missing the CIK, the
    filing manager name, or the period of report — those three are
    the minimum viable record for an ingester to write rows against.
    """
    root = ET.fromstring(xml)  # noqa: S314 — SEC EDGAR is the trusted source.

    cik_text = _walk_text(root, "cik")
    if cik_text is None:
        raise ValueError("primary_doc.xml is missing a <cik> element")

    # Scope the name lookup to the ``<filingManager>`` subtree so a
    # ``<name>`` that appears under ``<signatureBlock>`` (signer's
    # name) or any other unrelated branch can never be picked by
    # mistake. Codex pre-push review caught this on PR review.
    filing_manager = _find_descendant(root, "filingManager")
    name = _child_text(filing_manager, "name") if filing_manager is not None else None
    if name is None:
        raise ValueError("primary_doc.xml is missing the filingManager <name>")

    period_text = _walk_text(root, "reportCalendarOrQuarter")
    period_of_report = _parse_date_mmddyyyy(period_text)
    if period_of_report is None:
        raise ValueError(f"primary_doc.xml has no parseable <reportCalendarOrQuarter>; got {period_text!r}")

    signature_date = _walk_text(root, "signatureDate")
    filed_at = _parse_signature_date(signature_date)

    table_value_total = _decimal_or_none(_walk_text(root, "tableValueTotal"))

    return ThirteenFFilerInfo(
        cik=_zero_pad_cik(cik_text),
        name=name.strip(),
        period_of_report=period_of_report,
        filed_at=filed_at,
        table_value_total_usd=table_value_total,
    )


def parse_infotable(xml: str) -> list[ThirteenFHolding]:
    """Parse a 13F-HR ``infotable.xml`` payload.

    Returns one :class:`ThirteenFHolding` per ``<infoTable>`` element.
    Skips rows where the CUSIP, value, or share count cannot be
    resolved — those are malformed entries that no consumer of the
    ingest can act on.
    """
    root = ET.fromstring(xml)  # noqa: S314 — SEC EDGAR is the trusted source.

    holdings: list[ThirteenFHolding] = []
    for entry in root.iter():
        if _strip_ns(entry.tag) != "infoTable":
            continue

        fields: dict[str, str] = {}
        for child in entry.iter():
            tag = _strip_ns(child.tag)
            if child.text is not None:
                text = child.text.strip()
                if text:
                    fields[tag] = text

        cusip = fields.get("cusip")
        value_text = fields.get("value")
        shares_text = fields.get("sshPrnamt")
        if cusip is None or value_text is None or shares_text is None:
            logger.debug(
                "13F infoTable row dropped — missing required field; have keys=%s",
                sorted(fields),
            )
            continue

        value_usd = _decimal_or_none(value_text)
        shares_or_principal = _decimal_or_none(shares_text)
        if value_usd is None or shares_or_principal is None:
            logger.debug(
                "13F infoTable row dropped — non-numeric value/shares; cusip=%s",
                cusip,
            )
            continue

        put_call_raw = fields.get("putCall")
        put_call: Literal["PUT", "CALL"] | None
        if put_call_raw is None:
            put_call = None
        elif put_call_raw.upper() == "PUT":
            put_call = "PUT"
        elif put_call_raw.upper() == "CALL":
            put_call = "CALL"
        else:
            logger.warning("13F infoTable row had unknown putCall=%r; treating as None", put_call_raw)
            put_call = None

        holdings.append(
            ThirteenFHolding(
                cusip=cusip,
                name_of_issuer=fields.get("nameOfIssuer", ""),
                title_of_class=fields.get("titleOfClass", ""),
                value_usd=value_usd,
                shares_or_principal=shares_or_principal,
                shares_or_principal_type=fields.get("sshPrnamtType", "SH"),
                put_call=put_call,
                investment_discretion=fields.get("investmentDiscretion"),
                voting_sole=_decimal_or_none(fields.get("Sole")) or Decimal(0),
                voting_shared=_decimal_or_none(fields.get("Shared")) or Decimal(0),
                voting_none=_decimal_or_none(fields.get("None")) or Decimal(0),
            )
        )

    return holdings


def dominant_voting_authority(holding: ThirteenFHolding) -> Literal["SOLE", "SHARED", "NONE"] | None:
    """Pick the largest of the three voting-authority sub-amounts.

    Helper exposed for the service layer (PR 2) so the canonical
    ``voting_authority`` column gets the correct constrained value.
    Returns ``None`` only when all three sub-amounts are zero — that
    case maps to NULL on insert (the schema CHECK allows it).
    """
    sole = holding.voting_sole
    shared = holding.voting_shared
    none = holding.voting_none
    if sole == 0 and shared == 0 and none == 0:
        return None
    if sole >= shared and sole >= none:
        return "SOLE"
    if shared >= none:
        return "SHARED"
    return "NONE"
