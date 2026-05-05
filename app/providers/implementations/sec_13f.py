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

#925 — internals are now a thin wrapper over EdgarTools'
``edgar.thirteenf.parsers.primary_xml.parse_primary_document_xml`` and
``edgar.thirteenf.parsers.infotable_xml.parse_infotable_xml``. EdgarTools is pinned tight
(``edgartools==5.30.2``, ceiling ``<5.31.0``) per the license/maintenance
review at ``.claude/codex-913-license.txt``. The wrapper preserves
the existing :class:`ThirteenFFilerInfo` / :class:`ThirteenFHolding`
public surface so downstream callers (institutional_holdings,
rewash_filings) need no change.

What we still do ourselves:

  * **CIK extraction** — EdgarTools' ``PrimaryDocument13F`` does not
    surface the filer CIK; we read it from
    ``headerData/filerInfo/filer/credentials/cik`` directly and
    zero-pad to 10 digits.
  * **Signature-date timezone** — EdgarTools returns the raw string
    ``MM-DD-YYYY``; we coerce to a ``datetime`` at midnight UTC so
    the value lands in TIMESTAMPTZ without psycopg falling back to
    the server's local zone.
  * **PutCall normalisation** — EdgarTools returns the raw string
    (``Put``, ``CALL``, etc.); we collapse to the constrained
    ``Literal["PUT", "CALL"]`` and warn on unknown values.
  * **Type-code passthrough** — EdgarTools relabels ``SH``/``PRN``
    to ``Shares``/``Principal``; we map back to the raw two-letter
    SEC codes that the rest of the pipeline expects.
  * **Empty-CUSIP drop** — EdgarTools tolerates rows with empty
    CUSIPs (or other missing required fields) by filling defaults;
    we drop those rows so an unresolvable holding does not silently
    appear with a blank identifier downstream.

Offline guarantee: EdgarTools' static parsers (and the bundled
parquet ticker mapping it consults) operate purely on the input XML
plus an in-package data file — no SEC fetches occur during parse.
The golden-file replay test pins this contract.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET  # noqa: S405 — SEC EDGAR is the trusted source for 13F-HR XML.
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

logger = logging.getLogger(__name__)


# EdgarTools is imported lazily inside the public entry points. Importing
# ``edgar`` (or any of its submodules) at module-load time triggers the
# package's filesystem cache initialiser (``HTTP_MGR``), which mkdirs
# ``~/.edgar/_tcache`` as a side effect. That violates the pure-parser
# contract — the parser must be safe to import on read-only or
# sandboxed homes (CI runners, Docker images with non-writable $HOME).
# Lazy-importing keeps the side effect deferred until the first parse
# call, where the operator already accepts that EdgarTools is in play.
def _edgar_parsers() -> tuple[Any, Any]:
    from edgar.thirteenf.parsers.infotable_xml import (
        parse_infotable_xml as parse_infotable_xml,
    )
    from edgar.thirteenf.parsers.primary_xml import (
        parse_primary_document_xml as parse_primary_document_xml,
    )

    return parse_primary_document_xml, parse_infotable_xml


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


def _zero_pad_cik(text: str) -> str:
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        raise ValueError(f"primary_doc.xml carried a non-numeric CIK: {text!r}")
    return digits.zfill(10)


def _parse_signature_date(text: str | None) -> datetime | None:
    """Signature block carries a date but no time. Coerce to midnight
    UTC and tag the timezone explicitly so the value lands in
    ``filed_at TIMESTAMPTZ`` without psycopg falling back to the
    server's local zone (which differs from UTC on non-UTC dev hosts
    and would cause cross-tz drift in the persisted timestamp)."""
    if text is None:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)
    return None


# Type-code map: EdgarTools rewrites the SEC two-letter codes; we want the
# raw SEC codes back so downstream consumers (institutional_holdings, the
# ownership-card slice) keep their existing contract.
_TYPE_CODE_FROM_LABEL: dict[str, str] = {
    "Shares": "SH",
    "Principal": "PRN",
}


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def parse_primary_doc(xml: str) -> ThirteenFFilerInfo:
    """Parse a 13F-HR ``primary_doc.xml`` payload.

    Raises ``ValueError`` if the document is missing the CIK, the
    filing manager name, or the period of report — those three are
    the minimum viable record for an ingester to write rows against.
    """
    # CIK lives under headerData/filerInfo/filer/credentials/cik in real
    # SEC primary_doc.xml. EdgarTools' parser does not surface it, so we
    # read it ourselves before delegating.
    root = ET.fromstring(xml)  # noqa: S314 — SEC EDGAR is the trusted source.
    cik_text = _walk_text(root, "cik")
    if cik_text is None:
        raise ValueError("primary_doc.xml is missing a <cik> element")
    cik = _zero_pad_cik(cik_text)

    edgar_parse_primary, _ = _edgar_parsers()
    parsed = edgar_parse_primary(xml)

    name = (parsed.cover_page.filing_manager.name or "").strip()
    if not name:
        raise ValueError("primary_doc.xml is missing the filingManager <name>")

    period_dt = parsed.report_period
    period_of_report = period_dt.date() if isinstance(period_dt, datetime) else period_dt
    if not isinstance(period_of_report, date):  # defensive
        raise ValueError("primary_doc.xml has no parseable <periodOfReport>")

    filed_at = _parse_signature_date(parsed.signature.date)

    table_value_raw = parsed.summary_page.total_value
    table_value_total_usd: Decimal | None
    if table_value_raw in (None, 0, Decimal(0)):
        table_value_total_usd = None
    else:
        table_value_total_usd = Decimal(table_value_raw)

    return ThirteenFFilerInfo(
        cik=cik,
        name=name,
        period_of_report=period_of_report,
        filed_at=filed_at,
        table_value_total_usd=table_value_total_usd,
    )


def _normalise_put_call(raw: Any) -> Literal["PUT", "CALL"] | None:
    """Map EdgarTools' raw ``putCall`` text to the constrained Literal."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    upper = text.upper()
    if upper == "PUT":
        return "PUT"
    if upper == "CALL":
        return "CALL"
    logger.warning("13F infoTable row had unknown putCall=%r; treating as None", text)
    return None


def parse_infotable(xml: str) -> list[ThirteenFHolding]:
    """Parse a 13F-HR ``infotable.xml`` payload.

    Returns one :class:`ThirteenFHolding` per ``<infoTable>`` element
    that EdgarTools' parser surfaces. Drops rows where:

      * the CUSIP is empty — unresolvable downstream and the join
        column for ``external_identifiers``;
      * both the dollar value and the share count are zero —
        EdgarTools' XML parser falls back to ``0`` on missing
        ``<value>`` / ``<sshPrnamt>`` rather than raising. Genuine
        13F-HR rows always carry positive values for at least one of
        those two columns, so a both-zero row is a malformed entry
        no consumer of the ingest can act on. The bespoke parser this
        wrapper replaces dropped these via ``_decimal_or_none``
        returning ``None``; the explicit-zero check keeps that
        contract.
    """
    _, edgar_parse_infotable = _edgar_parsers()
    df = edgar_parse_infotable(xml)

    holdings: list[ThirteenFHolding] = []
    if len(df) == 0:
        return holdings

    for record in df.to_dict(orient="records"):
        cusip = str(record.get("Cusip") or "").strip()
        if not cusip:
            logger.debug("13F infoTable row dropped — empty CUSIP")
            continue

        value_int = int(record.get("Value") or 0)
        shares_int = int(record.get("SharesPrnAmount") or 0)
        if value_int == 0 and shares_int == 0:
            logger.debug(
                "13F infoTable row dropped — both value and shares are 0; cusip=%s",
                cusip,
            )
            continue

        type_label = str(record.get("Type") or "").strip()
        type_code = _TYPE_CODE_FROM_LABEL.get(type_label, "SH")

        holdings.append(
            ThirteenFHolding(
                cusip=cusip,
                name_of_issuer=str(record.get("Issuer") or ""),
                title_of_class=str(record.get("Class") or ""),
                value_usd=Decimal(value_int),
                shares_or_principal=Decimal(shares_int),
                shares_or_principal_type=type_code,
                put_call=_normalise_put_call(record.get("PutCall")),
                investment_discretion=(str(record.get("InvestmentDiscretion") or "").strip() or None),
                voting_sole=Decimal(int(record.get("SoleVoting") or 0)),
                voting_shared=Decimal(int(record.get("SharedVoting") or 0)),
                voting_none=Decimal(int(record.get("NonVoting") or 0)),
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
