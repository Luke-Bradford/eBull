"""edgartools → eBull ``BlockholderFiling`` adapter for the SC 13D /
13G manifest worker (#1233 PR11).

Background — adopting edgartools' ``Schedule13D.parse_xml`` /
``Schedule13G.parse_xml`` lets the parser track upstream SEC schema
changes for free; ``parse_xml`` is a static method returning a dict
(NOT a Pydantic instance, because constructing one requires a
``filing`` reference the worker doesn't have — see skill_edgartools.md
G15). The dict's TOP-LEVEL is dict-keyed but the NESTED VALUES are
frozen dataclasses (``IssuerInfo``, ``SecurityInfo``,
``ReportingPerson``, ``Signature``) accessed via attribute syntax.

This adapter converts an edgartools ``parse_xml`` result into the
repo-internal ``BlockholderFiling`` dataclass shape, so downstream
consumers (``_upsert_filing_row``,
``_record_13dg_observation_for_filing``) need no changes.

Key invariants:

* ``primary_filer_cik`` MUST come from the caller's
  ``manifest_filer_cik`` arg (the canonical filer-of-record identity
  per ``app/providers/implementations/sec_13dg.py:141-147``).
  edgartools does NOT expose
  ``headerData/filerInfo/filer/filerCredentials/cik``; reading
  ``reporting_persons[0].cik`` would silently mis-key the
  ``blockholder_filers`` PK and the ``blockholder_filings_ingest_log``
  filer identity for joint / agent-submitted filings. Codex 1c HIGH
  on the spec.
* ``submission_type`` is derived from the caller's ``manifest_form``
  via a closed mapping (KeyError on unknown form — loud, not silent;
  Codex 1b HIGH).
* ``date_of_event`` (13D) / ``event_date`` (13G) is dispatched on
  source. The dict value is a string; we attempt ISO parse and fall
  back to ``None`` on ``ValueError`` (legacy fixtures use
  ``MM/DD/YYYY`` which the in-house parser tolerated).
* Share-power / aggregate / percent fields are coerced to ``Decimal``
  for NUMERIC fidelity (NUMERIC schema columns; edgartools returns
  int / float).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Final, Literal

from app.providers.implementations.sec_13dg import (
    BlockholderFiling,
    BlockholderReportingPerson,
    Status,
    SubmissionType,
)
from app.services.blockholders import _zero_pad_cik

# ---------------------------------------------------------------------------
# Source / form mapping tables
# ---------------------------------------------------------------------------


_STATUS_FOR_SOURCE: Final[dict[Literal["sec_13d", "sec_13g"], Status]] = {
    "sec_13d": "active",
    "sec_13g": "passive",
}


# Form-name aliasing (#1233 PR11 v8 empirical pivot): legacy
# pre-mandate filings index under ``SC 13D/A`` style; post-2024-12-18
# XML mandate filings index under ``SCHEDULE 13D/A`` style
# (see ``sec_manifest.py:855-866`` form-type mapping which carries
# both spellings). Live smoke 2026-05-21 against SCHEDULE 13D/A
# manifest rows surfaced the prior single-spelling KeyError. Both
# spellings normalise to the same SubmissionType.
_SUBMISSION_TYPE_FOR_FORM: Final[dict[str, SubmissionType]] = {
    # Short ``SC`` spelling (pre-mandate index)
    "SC 13D": "SCHEDULE 13D",
    "SC 13D/A": "SCHEDULE 13D/A",
    "SC 13G": "SCHEDULE 13G",
    "SC 13G/A": "SCHEDULE 13G/A",
    # Full ``SCHEDULE`` spelling (post-mandate index)
    "SCHEDULE 13D": "SCHEDULE 13D",
    "SCHEDULE 13D/A": "SCHEDULE 13D/A",
    "SCHEDULE 13G": "SCHEDULE 13G",
    "SCHEDULE 13G/A": "SCHEDULE 13G/A",
}


# Schedule13D writes ``date_of_event`` into parsed; Schedule13G writes
# ``event_date``. We dispatch on the manifest source so the adapter
# does not need to introspect either dict.
_DATE_KEY_FOR_SOURCE: Final[dict[Literal["sec_13d", "sec_13g"], str]] = {
    "sec_13d": "date_of_event",
    "sec_13g": "event_date",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_decimal(value: Any) -> Decimal | None:
    """Coerce edgartools int / float to ``Decimal`` via ``str`` to
    preserve precision. ``None`` passes through as ``None`` so the
    schema-side NULL-able columns stay NULL when the source field is
    absent."""
    if value is None:
        return None
    return Decimal(str(value))


def _parse_date_or_none(value: Any) -> date | None:
    """Parse an ISO date string into ``date``; return ``None`` for
    empty / malformed input. Legacy fixtures use ``MM/DD/YYYY`` which
    ``date.fromisoformat`` rejects with ``ValueError`` — we catch and
    return ``None`` rather than crash. The in-house parser handled
    MM/DD/YYYY; the cutover to edgartools-backed parsing accepts the
    behaviour change because every retention-window filing is
    post-2024-12-18 (XML-mandate floor) and SEC's structured XML uses
    ISO dates in modern filings."""
    if not value:
        return None
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Public adapter
# ---------------------------------------------------------------------------


def build_filing_from_edgartools_dict(
    parsed: dict,
    *,
    source: Literal["sec_13d", "sec_13g"],
    manifest_form: str,
    manifest_filer_cik: str,
) -> BlockholderFiling:
    """Convert an edgartools ``parse_xml`` dict into a repo-internal
    ``BlockholderFiling``.

    Args:
        parsed: result of ``Schedule13D.parse_xml(xml)`` or
            ``Schedule13G.parse_xml(xml)``. Top-level dict; nested
            values are frozen dataclasses (see skill_edgartools.md
            G15).
        source: manifest source key (``'sec_13d'`` or ``'sec_13g'``).
            Drives both the ``status`` enum and the date-key dispatch.
        manifest_form: the SEC form label from the manifest row
            (``'SC 13D'``, ``'SC 13D/A'``, ``'SC 13G'``,
            ``'SC 13G/A'``). Drives the ``submission_type`` mapping;
            KeyError on any unknown form (loud, not silent).
        manifest_filer_cik: caller's ``row.cik`` — the canonical
            filer-of-record CIK / archive-owner CIK. Becomes
            ``BlockholderFiling.primary_filer_cik`` after zero-pad.
            Codex 1c HIGH: this MUST NOT be derived from any
            reporting-person CIK or the joint-filing /
            agent-submitted accession identity gets silently
            scrambled.

    Returns:
        ``BlockholderFiling`` dataclass for ``_upsert_filing_row`` to
        consume unchanged.

    Raises:
        KeyError: on an unknown ``manifest_form``.
    """
    submission_type = _SUBMISSION_TYPE_FOR_FORM[manifest_form]
    status = _STATUS_FOR_SOURCE[source]

    issuer_info = parsed["issuer_info"]
    security_info = parsed["security_info"]
    reporting_persons_raw = parsed["reporting_persons"]

    date_key = _DATE_KEY_FOR_SOURCE[source]
    date_of_event = _parse_date_or_none(parsed.get(date_key))

    reporting_persons: list[BlockholderReportingPerson] = []
    for person in reporting_persons_raw:
        no_cik = bool(getattr(person, "no_cik", False))
        # edgartools returns empty string when CIK is absent (e.g. on
        # the 13G cover); coerce to None for the schema-nullable
        # reporter_cik column. ``no_cik=True`` also yields None.
        raw_cik = getattr(person, "cik", "") or ""
        cik: str | None = None if no_cik else (raw_cik or None)
        reporting_persons.append(
            BlockholderReportingPerson(
                cik=cik,
                no_cik=no_cik,
                name=person.name,
                member_of_group=person.member_of_group,
                type_of_reporting_person=person.type_of_reporting_person or None,
                citizenship=person.citizenship or None,
                sole_voting_power=_to_decimal(person.sole_voting_power),
                shared_voting_power=_to_decimal(person.shared_voting_power),
                sole_dispositive_power=_to_decimal(person.sole_dispositive_power),
                shared_dispositive_power=_to_decimal(person.shared_dispositive_power),
                aggregate_amount_owned=_to_decimal(person.aggregate_amount),
                percent_of_class=_to_decimal(person.percent_of_class),
            )
        )

    return BlockholderFiling(
        submission_type=submission_type,
        status=status,
        primary_filer_cik=_zero_pad_cik(manifest_filer_cik),
        issuer_cik=issuer_info.cik,
        issuer_cusip=security_info.cusip,
        issuer_name=issuer_info.name,
        securities_class_title=security_info.title or None,
        date_of_event=date_of_event,
        # Manifest layer fills this from row.filed_at; adapter leaves
        # it None so an upstream change of policy is unambiguous.
        filed_at=None,
        reporting_persons=reporting_persons,
    )
