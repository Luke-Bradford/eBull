"""Form 4 insider-transactions parser + ingester (#429, #888).

SEC Form 4 is filed by directors, officers, and ≥10% holders within
two business days of any trade in their company's stock. Free,
timely, strong sentiment signal. The submissions.json feed flags
``insiderTransactionForIssuerExists: 1`` on issuers that have ever
filed a Form 4; the transaction rows themselves live in each Form 4
XML primary document.

Scope (migration 057 expansion): every structured field in the Form 4
XML lands in SQL, no raw-file persistence and no dropped elements.
The normalised shape spans four tables:

  insider_filings                — one row per filing accession
    └── insider_filers           — one row per (accession, filer_cik)
    └── insider_transaction_footnotes
                                 — one row per (accession, footnote_id)
    └── insider_transactions     — one row per (accession, txn_row_num)
        │                          carries filer_cik + footnote_refs JSONB
        │                          pointing back at the owner + footnote
        │                          bodies on the filing.

Pure/impure split mirrors :mod:`app.services.business_summary`:

- :func:`parse_form_4_xml` is a pure function over raw XML.
- :func:`ingest_insider_transactions` is the DB path — queries
  filing_events for Form 4 filings lacking matching insider rows,
  fetches the XML, parses, upserts across all four tables.

Tombstones (fetch 404 / parse failure) now live on the filing row
(``insider_filings.is_tombstone``) rather than a synthetic
``txn_row_num = -1`` sentinel. The hourly ingester continues to skip
accessions with an existing insider_filings row; the reader excludes
tombstoned filings via JOIN filter.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import re
import unicodedata
import xml.etree.ElementTree as ET  # noqa: S405 — Form 4 source is SEC EDGAR, trusted.
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any, Final, Literal, Protocol
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from app.providers.concurrent_fetch import FetchOutcome, fetch_document_texts_classified
from app.services import raw_filings
from app.services.filings import reconcile_mislinked_ownership_filing_events
from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
    tombstone_non_sibling_insider_observations,
)
from app.services.sec_identity import resolve_insider_writer_routing
from app.services.upsert_classify import (
    format_upsert_error,
    is_transient_upsert_error,
)

_PARSER_VERSION_FORM4 = "form4-v1"
# Form 5 (annual statement of changes in beneficial ownership) reuses the
# Form 4 ownership XML schema. parse_form_5_xml returns the same
# ParsedFiling shape; the only delta is the documentType gate. Separate
# manifest parser_version string lets the worker route Form 5 backlog
# through ``rewash`` independently from Form 4.
_PARSER_VERSION_FORM5 = "form5-v1"

logger = logging.getLogger(__name__)

# Bump whenever parse_form_4_xml shape changes so the ingester can
# re-parse older filings under the new parser without having to
# re-fetch the universe of Form 4 XML from SEC.
# 3 (#3227): holding lines parsed and stored (``insider_form4_holdings``); a holdings-only Form 4
# is a filing, not a tombstone. The raw-store watermark (``_PARSER_VERSION_FORM4``) is not bumped:
# that would mark every stored Form 4 body stale for ``scripts/rewash.py``, and only bodies with a
# holding line parse differently — ``scripts/backfill_3227_form4_holding_rows.py`` replays those.
_PARSER_VERSION = 3

# Form 4 retention cap (#1233 PR4, spec §4.3). Tightened from 5y to
# 3y after the 2026-05-19 T9-POST drive surfaced that historical Form
# 4 ingest costs days of SEC bandwidth for signal that decays sharply:
# 90d is the alert window, 12mo the thesis window, 3y the
# cluster-buy / persistence-of-trend window. Pre-3y trades cost
# ingest budget at no operator value.
#
# Ingest-side cap only — existing rows are untouched until the
# operator-driven pre-wipe + clean re-run (spec §6.3). The cutoff is
# computed in Python (calendar-year arithmetic — Codex PR3 lesson:
# `365 * N` days drifts past leap days) and passed as a `date` param
# to every SQL chokepoint, NOT as `NOW() - make_interval(...)` which
# carries DB session-timezone ambiguity.
INSIDER_FORM4_RETENTION_YEARS: int = 3


def form4_retention_cutoff(now: datetime | None = None) -> date:
    """Earliest ``filing_date`` accepted for Form 4 / 4-A ingest.

    Calendar-year subtraction so the boundary stays stable across leap
    days. Feb 29 in a non-leap target year anchors to Feb 28 of that
    year — the conservative choice (one day earlier, so a filing
    exactly at Feb 28 in the target year remains inside the window).

    ``now`` is injectable so tests can pin a stable reference instant
    without monkeypatching ``datetime.now``.
    """
    if now is None:
        now = datetime.now(tz=UTC)
    today = now.date()
    target_year = today.year - INSIDER_FORM4_RETENTION_YEARS
    try:
        return today.replace(year=target_year)
    except ValueError:
        return date(target_year, 2, 28)


def form4_within_retention(filing_date: date, now: datetime | None = None) -> bool:
    """Boundary check used by every Form 4 writer chokepoint.

    Returns True iff ``filing_date >= form4_retention_cutoff(now)``.
    Boundary is inclusive — a filing exactly at the cutoff date is
    retained (no off-by-one drop on the rolling boundary).
    """
    return filing_date >= form4_retention_cutoff(now)


# Form 5 retention cap (#1233 PR10b, spec §4.4). Form 5 is the annual
# catch-up of any missed Form 4s — cadence is one filing per insider
# per fiscal year, due ~45 days after fiscal-year-end. Spec §4.4 says
# "latest only matters"; the closest annual-cadence approximation
# without a schema overhaul is an 18-month ``filed_at`` window.
#
# Why 18 months, not 12: a 1y rolling cap drops the only/latest Form 5
# for a pair the moment it crosses the year boundary, leaving zero
# Form 5 visibility for weeks until the next annual lands. 18 months
# gives the annual-cadence buffer (Codex 1a MED on the PR10b plan).
# Worst case it admits BOTH the prior-year Form 5 and the current-year
# Form 5 briefly — operator-visible impact is zero because the read
# side de-dupes via cumulative Form 4 rollup
# (``refresh_insiders_current`` aggregates observations + Form 3
# baseline; Form 5 transactions land in ``insider_transactions`` and
# flow through the same cumulative path).
#
# Ingest-side cap only — existing rows are untouched until the
# operator-driven pre-wipe + clean re-run (spec §6.3).
INSIDER_FORM5_RETENTION_MONTHS: int = 18


def form5_retention_cutoff(now: datetime | None = None) -> date:
    """Earliest ``filed_at`` (as ``date``) accepted for Form 5 / 5-A ingest.

    Calendar-month subtraction so the boundary stays stable across
    leap days. ``now`` is injectable so tests can pin a stable
    reference instant without monkeypatching ``datetime.now``.
    """
    if now is None:
        now = datetime.now(tz=UTC)
    today = now.date()
    months_back = INSIDER_FORM5_RETENTION_MONTHS
    # Subtract whole months. Year/month arithmetic without dateutil:
    # convert today's (year, month) to a monotonic month index, step
    # backward, convert back. Day-of-month is clamped to the target
    # month's max (Feb 29 → Feb 28 in a non-leap year, Jan 31 → Feb 28
    # in any year).
    month_index = today.year * 12 + (today.month - 1) - months_back
    target_year = month_index // 12
    target_month = (month_index % 12) + 1
    target_day = today.day
    while target_day > 1:
        try:
            return date(target_year, target_month, target_day)
        except ValueError:
            target_day -= 1
    return date(target_year, target_month, 1)


def form5_within_retention(filing_date: date, now: datetime | None = None) -> bool:
    """Boundary check used by every Form 5 writer chokepoint.

    Returns True iff ``filing_date >= form5_retention_cutoff(now)``.
    Boundary is inclusive.
    """
    return filing_date >= form5_retention_cutoff(now)


# ---------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedFiler:
    """One reporting-owner block from ``<reportingOwner>``."""

    filer_cik: str
    filer_name: str
    street1: str | None
    street2: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    state_description: str | None
    is_director: bool | None
    is_officer: bool | None
    officer_title: str | None
    is_ten_percent_owner: bool | None
    is_other: bool | None
    other_text: str | None


@dataclass(frozen=True)
class ParsedFootnote:
    """One ``<footnote id="...">...</footnote>`` body."""

    footnote_id: str
    footnote_text: str


@dataclass(frozen=True)
class ParsedFootnoteRef:
    """Pointer from a transaction row to a footnote it relies on."""

    footnote_id: str
    # XML element that carried the ``footnoteId`` attribute, e.g.
    # ``transactionShares``, ``transactionPricePerShare``,
    # ``transactionDate``, ``ownershipNature``. Lets the UI render
    # the footnote next to the specific field it qualifies.
    field: str


@dataclass(frozen=True)
class ParsedTransaction:
    """One row extracted from a Form 4 transaction table (non-
    derivative or derivative). Every structured field in the
    ``(nonDerivative|derivative)Transaction`` element is surfaced —
    no silent drops."""

    txn_row_num: int
    is_derivative: bool
    # Filer linkage — CIK of the reporting owner this row belongs to.
    # When a filing has multiple owners we attribute each transaction
    # to the first listed owner (joint-filing convention: all listed
    # owners report the same set of transactions jointly).
    filer_cik: str | None
    security_title: str | None
    txn_date: date
    deemed_execution_date: date | None
    txn_code: str
    equity_swap_involved: bool | None
    # ``transactionCoding/transactionFormType`` — "4" or "5", mandatory on a
    # Form 4 submission (EDGAR Ownership XML Tech Spec §4.3.8.2). Carried for
    # the date-validity decision only (#2790) and deliberately NOT persisted: a
    # new stored column is a parse-shape change, which ``sql/057`` says bumps
    # ``parser_version`` and therefore costs a full Form 4 rewash.
    txn_form_type: str | None
    transaction_timeliness: str | None
    shares: Decimal | None
    price: Decimal | None
    acquired_disposed_code: str | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    # Derivative-only fields. None on non-derivative rows.
    conversion_exercise_price: Decimal | None
    exercise_date: date | None
    expiration_date: date | None
    underlying_security_title: str | None
    underlying_shares: Decimal | None
    underlying_value: Decimal | None
    footnote_refs: tuple[ParsedFootnoteRef, ...] = ()
    # #1687 — TRUE when txn_date postdates the SEC filing date, which is
    # impossible per Rule 16a-3(a) for a non-early filing (a filer source
    # typo). Set in :func:`upsert_filing` once filed_at is resolved; the raw
    # txn_date is retained for audit and operator-visible readers exclude
    # flagged rows.
    txn_date_invalid: bool = False


# #2441 — Securities Exchange Act § 16 was enacted "June 6, 1934, ch. 404,
# title I, § 16, 48 Stat. 896" (15 U.S.C. § 78p). § 16(a)(1) attaches the
# reporting duty to an insider of a class registered under § 12, and a Form
# 3/4/5 reports events in that capacity — so no reported § 16 date can precede
# the statute that creates the duty, the capacity and the registration alike.
# The enactment date is the deliberately WEAKEST anchor: it cannot reject a
# date that a later (§ 12 registration) anchor would allow.
SECTION_16_ENACTED: Final = date(1934, 6, 6)


def predates_section_16(value: date | None) -> bool:
    """True when ``value`` is a date § 16 could not have governed (#2441).

    The floor is form-agnostic, unlike the Rule 16a-3 upper bound: § 16(a)(2)
    sets only LATEST bounds for a Form 3 (so a Form 3 ahead of its filing is
    correct — #2790), but nothing makes a pre-enactment date correct on any
    form.
    """
    return value is not None and value < SECTION_16_ENACTED


#: Ownership submission types whose transaction lines §4.3.8.2 governs — the only
#: submissions on which a ``transactionFormType`` of "5" means "volunteered early".
_FORM4_SUBMISSION_TYPES: Final = frozenset({"4", "4/A"})


def is_early_form5_line(
    submission_form_type: str | None,
    txn_form_type: str | None,
    transaction_timeliness: str | None,
) -> bool:
    """True when Rule 16a-3(g)'s upper bound does not govern this line (#2790).

    SOURCE RULE. EDGAR Ownership XML Technical Specification v5.1 §4.3.8.2, for
    submission types "4" and "4/A": *"The ``<transactionFormType>`` is mandatory
    and must be '4' or '5.' … By definition, a '4' transaction is on time.
    Provide no value for this case. By definition, a '5' transaction is early.
    You do not have to provide a value of 'E,' but you can if you wish. A value
    other than 'E' will cause a SUSPENSE error."*

    A form-type-**5** line on a Form 4 is a Form-5-eligible transaction the filer
    volunteered ahead of its own deadline, and that deadline is Rule 16a-3(f)'s —
    45 days after the fiscal year end, on a LATER filing. So this filing's date
    does not bound the event, and a future date is legitimate. Worked case:
    ``0001127602-24-015987`` (RANGE RESOURCES / FUNK JAMES M), filed 2024-05-20,
    two form-type-5 lines dated 2024-06-03, footnote F2 — *"exempt from
    reporting, however the reporting person is voluntarily disclosing … a
    scheduled deferred compensation plan distribution with a distribution date of
    June 3, 2024."*

    Three things this deliberately is NOT:

    * **not the timeliness letter alone.** ``E`` is OPTIONAL on a form-type-5
      line (§4.3.8.2), so keying on it misses a quarter of them — full
      population, ``scripts/correct_2790_insider_future_period.py``: 22 of the 76
      exempt rows carry a BLANK timeliness (54 carry ``E``). It survives
      here only as a fallback for an UNKNOWN line form type, because EDGAR
      permits ``E`` nowhere else ("a value other than 'E' will cause a SUSPENSE
      error"). An explicit ``4`` + ``E`` is malformed and is NOT exempted — the
      alternative would exempt a row #2790's correction deletes.
    * **not the line form type alone.** On a Form **5** submission the same
      form-type-5 line is bound by 16a-3(f) directly: that filing reports
      transactions *during the fiscal year already ended*, so an event after it
      was not in that year. Measured: 50 breaching rows have this shape,
      including ``0001415889-23-002362`` (ARCH CAPITAL, Form 5 for FY2022 filed
      2023-02-13), which carries the SAME gift at ``15-NOV-2022`` and
      ``15-NOV-2023`` — a year typo visible inside one filing.
    * **not a claim that the date is right.** It is a claim that no published
      rule makes it impossible. Nothing bounds how far ahead a Form-5-eligible
      event may be volunteered, and #2441 refused inventing such a window.
    """
    submission = (submission_form_type or "").strip().upper()
    line = (txn_form_type or "").strip()
    timeliness = (transaction_timeliness or "").strip().upper()
    submission_ok = not submission or submission in _FORM4_SUBMISSION_TYPES
    line_is_form5 = line == "5" or (not line and timeliness == "E")
    return submission_ok and line_is_form5


def evaluate_insider_date_validity(
    txn_date: date,
    deemed_execution_date: date | None,
    filed_at: datetime | None,
    transaction_timeliness: str | None,
    *,
    submission_form_type: str | None = None,
    txn_form_type: str | None = None,
) -> tuple[bool, date | None]:
    """Apply the Rule 16a-3(a) execution-date invariant to one transaction.

    A Form 4 is due before the end of the 2nd business day *following* the
    reportable transaction (Securities Exchange Act §16(a) / Rule 16a-3(a)),
    so the execution date precedes or equals the filing date. A ``txn_date``
    / ``deemed_execution_date`` that postdates ``filed_at`` is therefore
    impossible — a filer source typo (#1687).

    Returns ``(txn_date_invalid, deemed_execution_date)``:

    - ``txn_date_invalid`` is True when ``txn_date > filed_at`` (the row is
      flagged; the raw value is kept since ``txn_date`` is NOT NULL).
    - ``deemed_execution_date`` is dropped to ``None`` when it postdates
      ``filed_at`` (nullable ⇒ quarantine, never invent a value).

    Exemptions — they apply to the UPPER bound ONLY (see below):

    - ``filed_at is None`` — no authoritative anchor (mirrors the
      ``upsert_filing`` filed_at fallback semantics).
    - :func:`is_early_form5_line` — a Form-5-eligible line volunteered early on
      a Form 4 is bound by Rule 16a-3(f), not 16a-3(g), so it may legitimately
      report a transaction dated after this filing. #2790 re-keyed this from the
      bare ``transaction_timeliness == 'E'`` test onto (submission type × line
      form type); see that function for the spec citation and both worked
      filings. ``sql/057``'s ``:264`` comment ("filed early (before the event)")
      was the wrong half of its own contradiction — ``:338`` ("before the
      deadline") is the reading the spec supports.

    #2441 adds a statutory FLOOR (:func:`predates_section_16`), and neither
    exemption is inherited by it: the floor needs no filing anchor, and no form
    type makes a pre-1934 date possible.

    The two dates are assessed INDEPENDENTLY: a floor hit on ``txn_date``
    does not skip the ``deemed_execution_date`` assessment, and a bad deemed
    date never flags an otherwise-valid transaction.
    """
    # #2441 — statutory floor, evaluated first and unconditionally.
    txn_date_invalid = predates_section_16(txn_date)
    deemed_out = None if predates_section_16(deemed_execution_date) else deemed_execution_date
    if filed_at is None or is_early_form5_line(submission_form_type, txn_form_type, transaction_timeliness):
        return (txn_date_invalid, deemed_out)
    # Anchor on the UTC calendar date (the SEC filing date is a UTC-stamped
    # acceptance date) so the boundary is session-timezone independent and
    # matches the migration's ``(filed_at AT TIME ZONE 'UTC')::date``.
    filed_date = filed_at.astimezone(UTC).date()
    txn_date_invalid = txn_date_invalid or txn_date > filed_date
    if deemed_out is not None and deemed_out > filed_date:
        deemed_out = None
    return (txn_date_invalid, deemed_out)


@dataclass(frozen=True)
class ParsedFiling:
    """Structured extraction of one Form 4 XML primary document.

    All header-level / issuer / signature fields land here. Filers,
    footnote bodies, and transactions are carried on related lists so
    the upsert path can populate all four tables in one transaction.
    """

    document_type: str
    period_of_report: date | None
    date_of_original_submission: date | None
    not_subject_to_section_16: bool | None
    form3_holdings_reported: bool | None
    form4_transactions_reported: bool | None
    issuer_cik: str | None
    issuer_name: str | None
    issuer_trading_symbol: str | None
    remarks: str | None
    signature_name: str | None
    signature_date: date | None
    filers: tuple[ParsedFiler, ...]
    footnotes: tuple[ParsedFootnote, ...]
    transactions: tuple[ParsedTransaction, ...]
    # #3227 — the Table I / II holding lines of a Form 4 (``parse_form_5_xml`` leaves this empty:
    # Form 5 holdings stay out of scope).
    holdings: tuple[ParsedHolding, ...] = ()


# ---------------------------------------------------------------------
# XML parser
# ---------------------------------------------------------------------


# Form 4 XMLs frequently declare a default namespace on
# ``<ownershipDocument xmlns="...">`` that makes every subsequent
# namespace-blind ``.find()`` miss. Regex pre-strip keeps the rest
# of the parser simple without threading ``{ns}`` braces everywhere.
_XMLNS_RE = re.compile(r'\sxmlns="[^"]*"')


def _unwrap_sgml_submission(text: str) -> str:
    """Slice the embedded ``<ownershipDocument>`` XML out of a full
    SGML submission (``<SEC-DOCUMENT>`` wrapper).

    EDGAR serves every filing at TWO URLs: the per-document primary
    doc (clean XML) and the ``.txt`` full submission (SGML container
    embedding every document verbatim). Master/daily-index discovery
    only knows the ``.txt`` URL, so manifest rows and legacy
    owner-stream fetches store the SGML shape — 14,279 form3/4/5 raw
    rows on dev as of 2026-07-22 (#2110 full-pop scan; 11,331 of them
    tombstoned because the parsers rejected the wrapper). Unwrapping
    at the parser chokepoint fixes every consumer at once: live
    ingest, the manifest parser's stored-body reuse (#1591), and
    rewash — without mutating stored payloads (raw store keeps the
    fetched bytes verbatim).

    First-close slice, not rindex: a submission carrying two ownership
    documents must not produce a multi-root slice (Codex ckpt-2 on
    #828 PR-2, where this exact slice repaired the 758-cohort's 13
    wrapped rows). Non-SGML input is returned unchanged; a wrapper
    with no embedded ownership doc falls through to the normal
    ``ET.fromstring`` failure → ``None``.
    """
    if not text.lstrip().startswith("<SEC-DOCUMENT>"):
        return text
    start = text.find("<ownershipDocument")
    if start == -1:
        return text
    end = text.find("</ownershipDocument>", start)
    if end == -1:
        return text
    return text[start : end + len("</ownershipDocument>")]


def parse_form_4_xml(raw_xml: str) -> ParsedFiling | None:
    """Extract everything structural from a Form 4 primary document.

    Returns ``None`` when:

    - Input is empty or fails to parse as XML.
    - Document root is not ``ownershipDocument`` (URL rot — operator
      pointed us at a cover page or a wrong document).
    - Zero transactions AND zero holdings across both tables. A
      filer-only filing is not a useful observation; drop it rather than
      insert a phantom record. A holdings-only Form 4 is a filing (#3227):
      its holding lines are stated balances (General Instruction 4(c)(i)).

    Otherwise returns a :class:`ParsedFiling` with:

    - The full filing header (document type, period of report,
      amendment linkage date, remarks, signature).
    - The issuer block (CIK, name, trading symbol).
    - Every listed reporting owner (``filers``) with full name,
      address, and relationship flags.
    - Every footnote body (``footnotes``), each with its SEC-assigned
      identifier ("F1" etc.).
    - Every transaction row (``transactions``) across non-derivative +
      derivative, with source-order ``txn_row_num`` for stable
      ``(accession, row_num)`` keys. Each transaction carries its
      footnote refs so the UI can render the footnote text next to
      the specific field it qualifies.
    - Every holding row (``holdings``), non-derivative then derivative, with
      a shared ``row_num`` and attributed to the first listed owner like the
      transactions.
    """
    if not raw_xml:
        return None
    cleaned_xml = _XMLNS_RE.sub("", _unwrap_sgml_submission(raw_xml))
    try:
        root = ET.fromstring(cleaned_xml)
    except ET.ParseError:
        return None

    if _localname(root.tag) != "ownershipDocument":
        return None

    document_type = _text(root.find("./documentType")) or "4"
    if document_type not in ("4", "4/A"):
        return None

    period_of_report = _date(_text(root.find("./periodOfReport")))
    date_of_original_submission = _date(_text(root.find("./dateOfOriginalSubmission")))
    not_subject_to_section_16 = _flag(_text(root.find("./notSubjectToSection16")))
    form3_holdings_reported = _flag(_text(root.find("./form3HoldingsReported")))
    form4_transactions_reported = _flag(_text(root.find("./form4TransactionsReported")))

    issuer_el = root.find("./issuer")
    issuer_cik = _text(issuer_el.find("./issuerCik")) if issuer_el is not None else None
    issuer_name = _text(issuer_el.find("./issuerName")) if issuer_el is not None else None
    issuer_trading_symbol = _text(issuer_el.find("./issuerTradingSymbol")) if issuer_el is not None else None

    filers = _extract_filers(root)
    if not filers:
        return None

    footnotes = _extract_footnotes(root)
    # Default filer_cik for transactions = first listed reporting
    # owner. Joint filings list all owners at filing-scope but don't
    # attribute individual transactions in the XML.
    default_filer_cik = filers[0].filer_cik

    transactions = _extract_transactions(root, default_filer_cik=default_filer_cik)
    holdings = _extract_holdings(root, default_filer_cik=default_filer_cik)

    if not transactions and not holdings:
        return None

    remarks = _text(root.find("./remarks"))
    owner_sig = root.find("./ownerSignature")
    signature_name = _text(owner_sig.find("./signatureName")) if owner_sig is not None else None
    signature_date = _date(_text(owner_sig.find("./signatureDate"))) if owner_sig is not None else None

    return ParsedFiling(
        document_type=document_type,
        period_of_report=period_of_report,
        date_of_original_submission=date_of_original_submission,
        not_subject_to_section_16=not_subject_to_section_16,
        form3_holdings_reported=form3_holdings_reported,
        form4_transactions_reported=form4_transactions_reported,
        issuer_cik=issuer_cik,
        issuer_name=issuer_name,
        issuer_trading_symbol=issuer_trading_symbol,
        remarks=remarks,
        signature_name=signature_name,
        signature_date=signature_date,
        filers=filers,
        footnotes=footnotes,
        transactions=transactions,
        holdings=holdings,
    )


def parse_form_5_xml(raw_xml: str) -> ParsedFiling | None:
    """Extract the structured transaction list from a Form 5 primary
    document.

    Form 5 is the annual statement of changes in beneficial ownership
    (Rule 16a-3(f)) and uses the SAME EDGAR ownership XML schema as
    Form 4. The parser shape mirrors :func:`parse_form_4_xml` byte-for-
    byte except for the ``documentType`` gate. ``ParsedFiling`` is
    intentionally the same dataclass — Form 5 transactions persist into
    the same ``insider_filings`` + ``insider_transactions`` tables with
    ``document_type='5'`` recording the provenance.

    Form 5 holdings (the optional year-end balance reconciliation
    section) are out of scope for this PR. The annual sections most
    operators use are the late-Form-4 transactions in
    ``<nonDerivativeTable>/<nonDerivativeTransaction>`` /
    ``<derivativeTable>/<derivativeTransaction>``, which this parser
    handles. A filing carrying ONLY holdings sections returns
    ``None`` — the manifest adapter then writes a tombstone so the
    worker stops re-fetching the same payload every tick.

    Returns ``None`` when:

    - Input is empty or fails to parse as XML.
    - Document root is not ``ownershipDocument``.
    - ``documentType`` is not ``5`` or ``5/A``.
    - Zero reporting owners.
    - Zero transactions across both the non-derivative and derivative
      tables.

    Observations: Form 5 transactions write through with
    ``source='form4'``. The shared ``OwnershipSource`` enum on
    ``ownership_*_observations`` does NOT include a ``form5`` value
    (CHECK constraint, see sql/113-116); the operator-visible
    provenance is preserved on ``insider_filings.document_type='5'``
    via the JOIN at read time. Treating Form 5 transactions as Form 4
    in the priority chain is correct semantically — they ARE late
    Form 4 trades surfaced annually.
    """
    if not raw_xml:
        return None
    cleaned_xml = _XMLNS_RE.sub("", _unwrap_sgml_submission(raw_xml))
    try:
        root = ET.fromstring(cleaned_xml)
    except ET.ParseError:
        return None

    if _localname(root.tag) != "ownershipDocument":
        return None

    document_type = _text(root.find("./documentType")) or "5"
    if document_type not in ("5", "5/A"):
        return None

    period_of_report = _date(_text(root.find("./periodOfReport")))
    date_of_original_submission = _date(_text(root.find("./dateOfOriginalSubmission")))
    not_subject_to_section_16 = _flag(_text(root.find("./notSubjectToSection16")))
    form3_holdings_reported = _flag(_text(root.find("./form3HoldingsReported")))
    form4_transactions_reported = _flag(_text(root.find("./form4TransactionsReported")))

    issuer_el = root.find("./issuer")
    issuer_cik = _text(issuer_el.find("./issuerCik")) if issuer_el is not None else None
    issuer_name = _text(issuer_el.find("./issuerName")) if issuer_el is not None else None
    issuer_trading_symbol = _text(issuer_el.find("./issuerTradingSymbol")) if issuer_el is not None else None

    filers = _extract_filers(root)
    if not filers:
        return None

    footnotes = _extract_footnotes(root)
    default_filer_cik = filers[0].filer_cik

    transactions = _extract_transactions(root, default_filer_cik=default_filer_cik)

    if not transactions:
        return None

    remarks = _text(root.find("./remarks"))
    owner_sig = root.find("./ownerSignature")
    signature_name = _text(owner_sig.find("./signatureName")) if owner_sig is not None else None
    signature_date = _date(_text(owner_sig.find("./signatureDate"))) if owner_sig is not None else None

    return ParsedFiling(
        document_type=document_type,
        period_of_report=period_of_report,
        date_of_original_submission=date_of_original_submission,
        not_subject_to_section_16=not_subject_to_section_16,
        form3_holdings_reported=form3_holdings_reported,
        form4_transactions_reported=form4_transactions_reported,
        issuer_cik=issuer_cik,
        issuer_name=issuer_name,
        issuer_trading_symbol=issuer_trading_symbol,
        remarks=remarks,
        signature_name=signature_name,
        signature_date=signature_date,
        filers=filers,
        footnotes=footnotes,
        transactions=transactions,
    )


# ---------------------------------------------------------------------
# Element helpers
# ---------------------------------------------------------------------


def _localname(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _text(el: ET.Element | None) -> str | None:
    if el is None or el.text is None:
        return None
    stripped = el.text.strip()
    return stripped or None


def _child_text(el: ET.Element | None, path: str) -> str | None:
    """Convenience for the common ``X/value`` shape where a leaf
    wraps its value in a ``<value>`` child."""
    if el is None:
        return None
    return _text(el.find(path))


def _flag(raw: str | None) -> bool | None:
    """Parse Form 4's boolean flag shape. SEC uses ``1`` / ``0`` or
    ``true`` / ``false``. Returns ``None`` when the field was absent
    (so downstream can distinguish "not set" from "explicitly false")."""
    if raw is None:
        return None
    low = raw.strip().lower()
    if low in ("1", "true"):
        return True
    if low in ("0", "false"):
        return False
    return None


def _date(raw: str | None) -> date | None:
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


# Shares/price caps — see migration 056 for the rationale. Real Form 4
# values never approach these; values above the cap usually indicate a
# malformed filing (decimal-separator bug, scientific-notation overflow).
_MAX_SHARES = Decimal("1e10")
_MAX_PRICE = Decimal("1e9")


def _safe_decimal(raw: str | None, *, max_value: Decimal) -> Decimal | None:
    """Parse a numeric value with strict Form 4 semantics. Returns
    ``None`` for malformed strings, NaN / Infinity, negative values,
    and values exceeding ``max_value``."""
    if raw is None:
        return None
    try:
        parsed = Decimal(raw)
    except InvalidOperation, ValueError:
        return None
    if not parsed.is_finite():
        return None
    if parsed < Decimal(0):
        return None
    if parsed > max_value:
        return None
    return parsed


# ---------------------------------------------------------------------
# Sub-extractors
# ---------------------------------------------------------------------


def _extract_filers(root: ET.Element) -> tuple[ParsedFiler, ...]:
    """Walk every ``<reportingOwner>`` at the document root."""
    owners: list[ParsedFiler] = []
    for ro in root.findall("./reportingOwner"):
        owner_id = ro.find("./reportingOwnerId")
        if owner_id is None:
            continue
        filer_cik = _text(owner_id.find("./rptOwnerCik"))
        filer_name = _text(owner_id.find("./rptOwnerName"))
        # Require both name and CIK — CIK is the stable dedup key we
        # rely on; a nameless row is useless.
        if filer_cik is None or filer_name is None:
            continue

        addr = ro.find("./reportingOwnerAddress")
        rel = ro.find("./reportingOwnerRelationship")

        owners.append(
            ParsedFiler(
                filer_cik=filer_cik,
                filer_name=filer_name,
                street1=_text(addr.find("./rptOwnerStreet1")) if addr is not None else None,
                street2=_text(addr.find("./rptOwnerStreet2")) if addr is not None else None,
                city=_text(addr.find("./rptOwnerCity")) if addr is not None else None,
                state=_text(addr.find("./rptOwnerState")) if addr is not None else None,
                zip_code=_text(addr.find("./rptOwnerZipCode")) if addr is not None else None,
                state_description=(_text(addr.find("./rptOwnerStateDescription")) if addr is not None else None),
                is_director=_flag(_text(rel.find("./isDirector"))) if rel is not None else None,
                is_officer=_flag(_text(rel.find("./isOfficer"))) if rel is not None else None,
                officer_title=_text(rel.find("./officerTitle")) if rel is not None else None,
                is_ten_percent_owner=(_flag(_text(rel.find("./isTenPercentOwner"))) if rel is not None else None),
                is_other=_flag(_text(rel.find("./isOther"))) if rel is not None else None,
                other_text=_text(rel.find("./otherText")) if rel is not None else None,
            )
        )
    return tuple(owners)


def _extract_footnotes(root: ET.Element) -> tuple[ParsedFootnote, ...]:
    container = root.find("./footnotes")
    if container is None:
        return ()
    notes: list[ParsedFootnote] = []
    for fn in container.findall("./footnote"):
        fn_id = fn.get("id")
        body = (fn.text or "").strip()
        if not fn_id or not body:
            continue
        notes.append(ParsedFootnote(footnote_id=fn_id, footnote_text=body))
    return tuple(notes)


# Element names that may carry ``footnoteId`` children. Used by
# _collect_footnote_refs to walk the transaction element and harvest
# every footnote pointer alongside the field it qualifies.
_FOOTNOTE_CARRIER_FIELDS = (
    "securityTitle",
    "transactionDate",
    "deemedExecutionDate",
    "transactionCoding",
    "transactionTimeliness",
    "transactionShares",
    "transactionPricePerShare",
    "transactionAcquiredDisposedCode",
    "sharesOwnedFollowingTransaction",
    "valueOwnedFollowingTransaction",
    "directOrIndirectOwnership",
    "natureOfOwnership",
    "conversionOrExercisePrice",
    "exerciseDate",
    "expirationDate",
    "underlyingSecurityTitle",
    "underlyingSecurityShares",
    "underlyingSecurityValue",
)


def _collect_footnote_refs(txn: ET.Element) -> tuple[ParsedFootnoteRef, ...]:
    refs: list[ParsedFootnoteRef] = []
    for field_name in _FOOTNOTE_CARRIER_FIELDS:
        for el in txn.iter(field_name):
            for footnote_el in el.findall("./footnoteId"):
                footnote_id = footnote_el.get("id")
                if footnote_id:
                    refs.append(ParsedFootnoteRef(footnote_id=footnote_id, field=field_name))
    return tuple(refs)


def _extract_transactions(root: ET.Element, *, default_filer_cik: str) -> tuple[ParsedTransaction, ...]:
    """Walk non-derivative + derivative tables in source order.

    ``txn_row_num`` is the SOURCE POSITION across both tables (non-
    derivative first, then derivative) so the ``(accession, row_num)``
    UNIQUE key is stable across parser revisions. Row 0 that fails to
    parse today and succeeds after a regex fix tomorrow still lands at
    row 0; parseable siblings keep their indices. Increment the
    counter for every source element, not only the ones that parse.
    """
    rows: list[ParsedTransaction] = []
    row_num = 0
    for table_path, is_deriv in (
        ("./nonDerivativeTable/nonDerivativeTransaction", False),
        ("./derivativeTable/derivativeTransaction", True),
    ):
        for txn_el in root.findall(table_path):
            parsed = _parse_one_transaction(
                txn_el,
                row_num=row_num,
                is_derivative=is_deriv,
                filer_cik=default_filer_cik,
            )
            row_num += 1  # advance even on parse-miss so indices stay stable
            if parsed is not None:
                rows.append(parsed)
    return tuple(rows)


_VALID_DIRECT_INDIRECT = {"D", "I"}
_VALID_ACQUIRED_DISPOSED = {"A", "D"}
_VALID_TIMELINESS = {"E", "L"}
# EDGAR Ownership XML Tech Spec §4.3.8.2/§4.3.8.3 — the only values the element
# may carry ("3" appears only on a late Form 3 holding reported via a Form 5).
_VALID_TXN_FORM_TYPES = {"3", "4", "5"}


def _parse_one_transaction(
    txn: ET.Element,
    *,
    row_num: int,
    is_derivative: bool,
    filer_cik: str,
) -> ParsedTransaction | None:
    """Parse one ``(nonD|D)erivativeTransaction`` element.

    Returns ``None`` when the date or code can't be extracted — these
    are the two NOT NULL columns on ``insider_transactions``. Every
    other field is nullable and passes through as None on absent /
    malformed input."""
    txn_date_s = _child_text(txn, "./transactionDate/value")
    txn_code = _child_text(txn, "./transactionCoding/transactionCode")
    if not txn_date_s or not txn_code:
        return None
    txn_date = _date(txn_date_s)
    if txn_date is None:
        return None

    security_title = _child_text(txn, "./securityTitle/value")
    deemed_execution_date = _date(_child_text(txn, "./deemedExecutionDate/value"))
    equity_swap_involved = _flag(_child_text(txn, "./transactionCoding/equitySwapInvolved"))

    timeliness_raw = _child_text(txn, "./transactionTimeliness/value")
    transaction_timeliness = timeliness_raw if timeliness_raw in _VALID_TIMELINESS else None

    # #2790 — mandatory on a Form 4 submission per EDGAR Ownership XML Tech Spec
    # §4.3.8.2 and the field that decides whether Rule 16a-3(g)'s upper bound
    # governs the line. Unrecognised values collapse to None (⇒ unknown), which
    # lets the ``E`` fallback in :func:`is_early_form5_line` speak instead.
    form_type_raw = _child_text(txn, "./transactionCoding/transactionFormType")
    txn_form_type = form_type_raw if form_type_raw in _VALID_TXN_FORM_TYPES else None

    shares = _safe_decimal(
        _child_text(txn, "./transactionAmounts/transactionShares/value"),
        max_value=_MAX_SHARES,
    )
    price = _safe_decimal(
        _child_text(txn, "./transactionAmounts/transactionPricePerShare/value"),
        max_value=_MAX_PRICE,
    )
    # Grants often lack a price or carry "0"; normalise "0" to None
    # so the column semantically means "unpriced" rather than "priced
    # at zero".
    if price is not None and price == Decimal(0):
        price = None

    ad_raw = _child_text(txn, "./transactionAmounts/transactionAcquiredDisposedCode/value")
    acquired_disposed_code = ad_raw if ad_raw in _VALID_ACQUIRED_DISPOSED else None

    post_transaction_shares = _safe_decimal(
        _child_text(txn, "./postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
        max_value=_MAX_SHARES,
    )

    ownership = txn.find("./ownershipNature")
    direct_raw = _child_text(ownership, "./directOrIndirectOwnership/value") if ownership is not None else None
    direct_indirect = direct_raw if direct_raw in _VALID_DIRECT_INDIRECT else None
    nature_of_ownership = _child_text(ownership, "./natureOfOwnership/value") if ownership is not None else None

    # Derivative-only fields
    conversion_exercise_price: Decimal | None = None
    exercise_date: date | None = None
    expiration_date: date | None = None
    underlying_security_title: str | None = None
    underlying_shares: Decimal | None = None
    underlying_value: Decimal | None = None
    if is_derivative:
        conversion_exercise_price = _safe_decimal(
            _child_text(txn, "./conversionOrExercisePrice/value"),
            max_value=_MAX_PRICE,
        )
        exercise_date = _date(_child_text(txn, "./exerciseDate/value"))
        expiration_date = _date(_child_text(txn, "./expirationDate/value"))
        underlying = txn.find("./underlyingSecurity")
        if underlying is not None:
            underlying_security_title = _child_text(underlying, "./underlyingSecurityTitle/value")
            underlying_shares = _safe_decimal(
                _child_text(underlying, "./underlyingSecurityShares/value"),
                max_value=_MAX_SHARES,
            )
            underlying_value = _safe_decimal(
                _child_text(underlying, "./underlyingSecurityValue/value"),
                max_value=_MAX_PRICE,
            )

    return ParsedTransaction(
        txn_row_num=row_num,
        is_derivative=is_derivative,
        filer_cik=filer_cik,
        security_title=security_title,
        txn_date=txn_date,
        deemed_execution_date=deemed_execution_date,
        txn_code=txn_code,
        equity_swap_involved=equity_swap_involved,
        txn_form_type=txn_form_type,
        transaction_timeliness=transaction_timeliness,
        shares=shares,
        price=price,
        acquired_disposed_code=acquired_disposed_code,
        post_transaction_shares=post_transaction_shares,
        direct_indirect=direct_indirect,
        nature_of_ownership=nature_of_ownership,
        conversion_exercise_price=conversion_exercise_price,
        exercise_date=exercise_date,
        expiration_date=expiration_date,
        underlying_security_title=underlying_security_title,
        underlying_shares=underlying_shares,
        underlying_value=underlying_value,
        footnote_refs=_collect_footnote_refs(txn),
    )


# ---------------------------------------------------------------------
# Form 3 parser (#768) — initial holdings snapshot
# ---------------------------------------------------------------------
#
# Form 3 is filed once when an insider becomes subject to Section 16
# reporting (officer / director / 10% holder appointment). It carries
# the *snapshot* of their positions on that date — no transactions, no
# acquired/disposed codes. Cumulative balance at any later point =
# Form 3 baseline + signed sum of Form 4 deltas since.
#
# XML structure mirrors Form 4 but uses ``nonDerivativeHolding`` /
# ``derivativeHolding`` (not ``...Transaction``) under
# ``nonDerivativeTable`` / ``derivativeTable``. Each holding carries
# ``postTransactionAmounts/sharesOwnedFollowingTransaction/value`` for
# the share count (the only "transaction" semantics on a Form 3 are
# the synthetic "you now hold this many shares as of periodOfReport").


@dataclass(frozen=True)
class ParsedHolding:
    """One row from a Form 3 (non-derivative or derivative) holding
    table. Mirrors :class:`ParsedTransaction` but drops the
    transaction-state fields (``txn_code``, ``acquired_disposed_code``,
    ``shares`` of the transaction itself) since a holding row records
    the snapshot, not a delta.

    Share-vs-value semantics: SEC ``postTransactionAmounts`` allows
    *either* ``sharesOwnedFollowingTransaction`` *or*
    ``valueOwnedFollowingTransaction`` (typically used for
    fractional-undivided-interest securities). Both surface here so
    the ingester / reader can pick the populated branch without a
    silent drop. Same shape as ``ParsedTransaction`` for Form 4 (post-
    migration 057): every structured field surfaced, no silent drops.
    """

    row_num: int
    is_derivative: bool
    # Filer linkage — same convention as Form 4 transactions: when the
    # filing has multiple owners, attribute each row to the first
    # listed owner (joint-filing convention).
    filer_cik: str | None
    security_title: str | None
    shares: Decimal | None
    value_owned: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    # Derivative-only fields. ``None`` on non-derivative rows.
    conversion_exercise_price: Decimal | None
    exercise_date: date | None
    expiration_date: date | None
    underlying_security_title: str | None
    underlying_shares: Decimal | None
    # Value-branch alternative for derivative underlyings — parallels
    # ParsedTransaction.underlying_value added in migration 057.
    underlying_value: Decimal | None
    # Pointers from this row to footnote bodies on the filing. Mirrors
    # ParsedTransaction.footnote_refs — operator-facing UX must surface
    # the footnote next to the field it qualifies. Form 3 footnotes
    # commonly explain indirect-ownership chains (e.g. "Held by family
    # trust of which the reporting person is trustee"). Dropping them
    # silently violates the migration-057 "every structured field
    # surfaced" precedent.
    footnote_refs: tuple[ParsedFootnoteRef, ...] = ()


@dataclass(frozen=True)
class ParsedForm3:
    """Structured extraction of one Form 3 XML primary document.

    Holding rows for both tables are flattened into a single
    ``holdings`` tuple with stable ``row_num`` so the
    ``(accession, row_num)`` UNIQUE key in
    :data:`insider_initial_holdings` doesn't collide between
    non-derivative and derivative rows from the same filing.

    ``footnotes`` carries the body of every ``<footnote id="...">``
    on the filing; ``ParsedHolding.footnote_refs`` points at them by
    id so the reader can render the body next to the field it
    qualifies (matches the Form 4 model post-057).

    ``no_securities_owned`` is the Form 3 header flag asserting the
    filer holds nothing. Set to ``True`` on a "blank" Form 3 (newly-
    appointed director with no positions yet); the holdings list will
    also be empty in that case but the explicit flag lets the operator
    distinguish "we know they hold nothing" from "data not on file".

    ``date_of_original_submission`` is non-null on ``3/A`` amendments
    and points back at the date of the original Form 3 being amended
    — useful for chaining + drift detection.
    """

    document_type: str
    period_of_report: date | None
    date_of_original_submission: date | None
    no_securities_owned: bool | None
    issuer_cik: str | None
    issuer_name: str | None
    issuer_trading_symbol: str | None
    remarks: str | None
    signature_name: str | None
    signature_date: date | None
    filers: tuple[ParsedFiler, ...]
    footnotes: tuple[ParsedFootnote, ...]
    holdings: tuple[ParsedHolding, ...]


def parse_form_3_xml(raw_xml: str) -> ParsedForm3 | None:
    """Extract the structured holdings snapshot from a Form 3 primary
    document.

    Returns ``None`` when:

    - Input is empty or fails to parse as XML.
    - Document root is not ``ownershipDocument`` (URL rot — caller
      pointed us at a cover page or a wrong document).
    - ``documentType`` is not ``3`` or ``3/A`` (caller mis-routed a
      Form 4 / 5 / amendment).
    - Zero reporting owners (the file is unattributable — drop rather
      than persist a phantom).

    Empty holdings tables are *not* an error: a filer can submit a
    Form 3 declaring "no holdings" (e.g. a newly-appointed director
    who holds nothing yet). The caller writes a row with no holdings
    so re-runs skip it; the absence of a row in
    ``insider_initial_holdings`` is then meaningful (vs "we never
    looked").
    """
    if not raw_xml:
        return None
    cleaned_xml = _XMLNS_RE.sub("", _unwrap_sgml_submission(raw_xml))
    try:
        root = ET.fromstring(cleaned_xml)  # noqa: S314 — same trust posture as Form 4 parser.
    except ET.ParseError:
        return None

    if _localname(root.tag) != "ownershipDocument":
        return None

    document_type = _text(root.find("./documentType")) or "3"
    if document_type not in ("3", "3/A"):
        return None

    period_of_report = _date(_text(root.find("./periodOfReport")))
    date_of_original_submission = _date(_text(root.find("./dateOfOriginalSubmission")))
    no_securities_owned = _flag(_text(root.find("./noSecuritiesOwned")))

    issuer_el = root.find("./issuer")
    issuer_cik = _text(issuer_el.find("./issuerCik")) if issuer_el is not None else None
    issuer_name = _text(issuer_el.find("./issuerName")) if issuer_el is not None else None
    issuer_trading_symbol = _text(issuer_el.find("./issuerTradingSymbol")) if issuer_el is not None else None

    filers = _extract_filers(root)
    if not filers:
        return None
    default_filer_cik = filers[0].filer_cik

    footnotes = _extract_footnotes(root)
    holdings = _extract_holdings(root, default_filer_cik=default_filer_cik)

    remarks = _text(root.find("./remarks"))
    owner_sig = root.find("./ownerSignature")
    signature_name = _text(owner_sig.find("./signatureName")) if owner_sig is not None else None
    signature_date = _date(_text(owner_sig.find("./signatureDate"))) if owner_sig is not None else None

    return ParsedForm3(
        document_type=document_type,
        period_of_report=period_of_report,
        date_of_original_submission=date_of_original_submission,
        no_securities_owned=no_securities_owned,
        issuer_cik=issuer_cik,
        issuer_name=issuer_name,
        issuer_trading_symbol=issuer_trading_symbol,
        remarks=remarks,
        signature_name=signature_name,
        signature_date=signature_date,
        filers=filers,
        footnotes=footnotes,
        holdings=holdings,
    )


def _extract_holdings(
    root: ET.Element,
    *,
    default_filer_cik: str,
) -> tuple[ParsedHolding, ...]:
    """Walk ``nonDerivativeTable/nonDerivativeHolding`` then
    ``derivativeTable/derivativeHolding`` in document order. Row index
    is shared across both tables so ``(accession, row_num)`` is a
    stable dedup key for the upsert path."""
    holdings: list[ParsedHolding] = []
    row_num = 0
    for non_der in root.findall("./nonDerivativeTable/nonDerivativeHolding"):
        holdings.append(
            _parse_one_holding(non_der, row_num=row_num, is_derivative=False, default_filer_cik=default_filer_cik)
        )
        row_num += 1
    for der in root.findall("./derivativeTable/derivativeHolding"):
        holdings.append(
            _parse_one_holding(der, row_num=row_num, is_derivative=True, default_filer_cik=default_filer_cik)
        )
        row_num += 1
    return tuple(holdings)


def _parse_one_holding(
    el: ET.Element,
    *,
    row_num: int,
    is_derivative: bool,
    default_filer_cik: str,
) -> ParsedHolding:
    security_title = _child_text(el.find("./securityTitle"), "./value")
    # SEC postTransactionAmounts allows EITHER a share count OR a value
    # (fractional-undivided-interest securities use the value branch).
    # Surface both — the ingester picks whichever is populated.
    shares = _safe_decimal(
        _child_text(
            el.find("./postTransactionAmounts/sharesOwnedFollowingTransaction"),
            "./value",
        ),
        max_value=_MAX_SHARES,
    )
    value_owned = _safe_decimal(
        _child_text(
            el.find("./postTransactionAmounts/valueOwnedFollowingTransaction"),
            "./value",
        ),
        max_value=_MAX_PRICE,
    )
    own_nature = el.find("./ownershipNature")
    direct_indirect = _child_text(
        own_nature.find("./directOrIndirectOwnership") if own_nature is not None else None,
        "./value",
    )
    # Sanitise direct/indirect — SEC enumerates only "D" and "I"; any
    # other value is malformed and persisting it would mislead the
    # ownership-card filter that branches on this column. Mirrors the
    # Form 4 sanitiser at _parse_one_transaction.
    if direct_indirect not in ("D", "I"):
        direct_indirect = None
    nature_of_ownership = _child_text(
        own_nature.find("./natureOfOwnership") if own_nature is not None else None,
        "./value",
    )

    conversion_exercise_price: Decimal | None = None
    exercise_date: date | None = None
    expiration_date: date | None = None
    underlying_security_title: str | None = None
    underlying_shares: Decimal | None = None
    underlying_value: Decimal | None = None
    if is_derivative:
        conversion_exercise_price = _safe_decimal(
            _child_text(el.find("./conversionOrExercisePrice"), "./value"),
            max_value=_MAX_PRICE,
        )
        exercise_date = _date(_child_text(el.find("./exerciseDate"), "./value"))
        expiration_date = _date(_child_text(el.find("./expirationDate"), "./value"))
        underlying = el.find("./underlyingSecurity")
        if underlying is not None:
            underlying_security_title = _child_text(underlying.find("./underlyingSecurityTitle"), "./value")
            underlying_shares = _safe_decimal(
                _child_text(underlying.find("./underlyingSecurityShares"), "./value"),
                max_value=_MAX_SHARES,
            )
            # Value-branch alternative (matches ParsedTransaction
            # .underlying_value added in migration 057). Some
            # derivative grants — particularly performance / dollar-
            # denominated awards — express the underlying as a value
            # rather than a share count.
            underlying_value = _safe_decimal(
                _child_text(underlying.find("./underlyingSecurityValue"), "./value"),
                max_value=_MAX_PRICE,
            )

    return ParsedHolding(
        row_num=row_num,
        is_derivative=is_derivative,
        filer_cik=default_filer_cik,
        security_title=security_title,
        shares=shares,
        value_owned=value_owned,
        direct_indirect=direct_indirect,
        nature_of_ownership=nature_of_ownership,
        conversion_exercise_price=conversion_exercise_price,
        exercise_date=exercise_date,
        expiration_date=expiration_date,
        underlying_security_title=underlying_security_title,
        underlying_shares=underlying_shares,
        underlying_value=underlying_value,
        footnote_refs=_collect_footnote_refs(el),
    )


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


def lookup_sec_filed_at(conn: psycopg.Connection[Any], accession_number: str) -> datetime | None:
    """Resolve an accession's TRUE SEC filing timestamp (#899).

    Priority: ``sec_filing_manifest.filed_at`` (canonical, #1233) →
    ``filing_events.filing_date`` (legacy cohort with no manifest row) →
    ``None`` (caller falls back to its event-date derivation, logged).
    ``filing_events`` can carry one row per share-class sibling for the
    same accession; the filing_date is identical across them, the ORDER
    BY just pins determinism."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT filed_at FROM sec_filing_manifest WHERE accession_number = %s",
            (accession_number,),
        )
        row = cur.fetchone()
        if row is not None and row[0] is not None:
            return row[0]
        cur.execute(
            """
            SELECT filing_date::timestamp AT TIME ZONE 'UTC'
            FROM filing_events
            WHERE provider = 'sec' AND provider_filing_id = %s
            ORDER BY filing_date DESC
            LIMIT 1
            """,
            (accession_number,),
        )
        row = cur.fetchone()
        return row[0] if row is not None else None


def upsert_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: ParsedFiling,
    is_rewash: bool = False,
    filed_at: datetime | None = None,
    refresh_current: bool = True,
) -> None:
    """Insert/refresh the filing header + filer dim + footnote bodies +
    transaction rows for one accession.

    ``refresh_current=False`` skips the per-sibling ``_current`` refresh after the observation
    write-through — for a bulk replay that batch-refreshes once at the end (#3227's backfill: the
    per-instrument refresh is ~97% of a replayed accession's time).

    Idempotency: every child table keys on ``(accession, …)`` with
    ON CONFLICT DO UPDATE so re-running the ingester on the same
    accession (e.g. after a parser bump) refreshes every field in
    place. The prior 056-era ``ON CONFLICT DO NOTHING`` policy would
    have frozen pre-expansion rows forever; the new parser version
    deliberately overwrites them.

    ``is_rewash``: when True, the conflict branch preserves the
    original ``fetched_at`` (re-parsing a stored body isn't a fresh
    SEC fetch and shouldn't claim to be — operator audit / recency
    logic using ``fetched_at`` would otherwise read every re-walked
    filing as "fetched today").

    ``filed_at`` (#899): the SEC filing timestamp stamped onto the
    observation write-through. Manifest-driven callers pass
    ``row.filed_at``; when ``None`` (legacy per-CIK ingester, rewash)
    it is resolved at this chokepoint via :func:`lookup_sec_filed_at`
    so every caller gets true-filing-time semantics without per-site
    changes. Final fallback (no manifest row, no filing_event) is the
    historical ``txn_date @ midnight UTC``, logged."""
    if filed_at is None:
        filed_at = lookup_sec_filed_at(conn, accession_number)

    # #1687 — apply the Rule 16a-3(a) execution-date guard once filed_at is
    # resolved. Sanitise the parsed transactions BEFORE both the DB insert
    # loop and the ownership write-through below: a future-dated typo must
    # neither surface in the insider list nor win the latest-per-group
    # ownership observation.
    sanitised: list[ParsedTransaction] = []
    for txn in parsed.transactions:
        invalid, deemed = evaluate_insider_date_validity(
            txn.txn_date,
            txn.deemed_execution_date,
            filed_at,
            txn.transaction_timeliness,
            # #2790 — the submission type comes from the document header and the
            # line type from the row; parsing either without passing BOTH here
            # is inert, which is why a test asserts this wiring.
            submission_form_type=parsed.document_type,
            txn_form_type=txn.txn_form_type,
        )
        if invalid != txn.txn_date_invalid or deemed != txn.deemed_execution_date:
            txn = replace(txn, txn_date_invalid=invalid, deemed_execution_date=deemed)
        sanitised.append(txn)
    parsed = replace(parsed, transactions=tuple(sanitised))

    # #828 PR-1 — route entity-level writes by the PARSED issuer CIK, not the
    # discovery-time instrument. A filing discovered via the reporting OWNER's
    # EDGAR stream (BAC Form 4 in Berkshire's feed) must bind to the issuer's
    # siblings; the owner's filing_events binding is deleted below. Fail-open:
    # unknown/unroutable issuer keeps the discovery linkage unchanged.
    routing = resolve_insider_writer_routing(conn, discovery_instrument_id=instrument_id, issuer_cik=parsed.issuer_cik)
    entity_instrument_id = routing.entity_instrument_id
    if routing.is_mislink:
        logger.warning(
            "form4/5 writer routing (#828): accession=%s discovered under "
            "instrument %s but issuer CIK %s maps to siblings %s — entity rows "
            "routed to instrument %s",
            accession_number,
            instrument_id,
            parsed.issuer_cik,
            routing.sibling_instrument_ids,
            entity_instrument_id,
        )

    # #817 — the single once-per-accession chokepoint for Form 4/5 typed-table
    # writes (live manifest drain, rewash, legacy ingest all funnel here).
    # Serialise concurrent writers of this accession's rows before the first
    # mutation (no SEC fetch happens in this function — safe to hold). Every
    # caller commits per accession now: rewash + legacy commit per accession, and
    # the manifest worker commits per row (#1735, ``_dispatch_rows``), so the xact
    # lock auto-releases at each accession's row boundary on all paths.
    raw_filings.acquire_filing_accession_write_lock(conn, accession_number)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO insider_filings (
                accession_number, instrument_id, document_type,
                period_of_report, date_of_original_submission,
                not_subject_to_section_16,
                form3_holdings_reported, form4_transactions_reported,
                issuer_cik, issuer_name, issuer_trading_symbol,
                remarks, signature_name, signature_date,
                primary_document_url, parser_version, is_tombstone
            ) VALUES (
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, FALSE
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                instrument_id                = EXCLUDED.instrument_id,
                document_type                = EXCLUDED.document_type,
                period_of_report             = EXCLUDED.period_of_report,
                date_of_original_submission  = EXCLUDED.date_of_original_submission,
                not_subject_to_section_16    = EXCLUDED.not_subject_to_section_16,
                form3_holdings_reported      = EXCLUDED.form3_holdings_reported,
                form4_transactions_reported  = EXCLUDED.form4_transactions_reported,
                issuer_cik                   = EXCLUDED.issuer_cik,
                issuer_name                  = EXCLUDED.issuer_name,
                issuer_trading_symbol        = EXCLUDED.issuer_trading_symbol,
                remarks                      = EXCLUDED.remarks,
                signature_name               = EXCLUDED.signature_name,
                signature_date               = EXCLUDED.signature_date,
                primary_document_url         = EXCLUDED.primary_document_url,
                parser_version               = EXCLUDED.parser_version,
                is_tombstone                 = FALSE,
                fetched_at                   = CASE WHEN %s
                                                    THEN insider_filings.fetched_at
                                                    ELSE NOW() END
            """,
            (
                accession_number,
                entity_instrument_id,
                parsed.document_type,
                parsed.period_of_report,
                parsed.date_of_original_submission,
                parsed.not_subject_to_section_16,
                parsed.form3_holdings_reported,
                parsed.form4_transactions_reported,
                parsed.issuer_cik,
                parsed.issuer_name,
                parsed.issuer_trading_symbol,
                parsed.remarks,
                parsed.signature_name,
                parsed.signature_date,
                primary_document_url,
                _PARSER_VERSION,
                is_rewash,
            ),
        )

        for filer in parsed.filers:
            cur.execute(
                """
                INSERT INTO insider_filers (
                    accession_number, filer_cik, filer_name,
                    street1, street2, city, state, zip_code,
                    state_description,
                    is_director, is_officer, officer_title,
                    is_ten_percent_owner, is_other, other_text
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (accession_number, filer_cik) DO UPDATE SET
                    filer_name           = EXCLUDED.filer_name,
                    street1              = EXCLUDED.street1,
                    street2              = EXCLUDED.street2,
                    city                 = EXCLUDED.city,
                    state                = EXCLUDED.state,
                    zip_code             = EXCLUDED.zip_code,
                    state_description    = EXCLUDED.state_description,
                    is_director          = EXCLUDED.is_director,
                    is_officer           = EXCLUDED.is_officer,
                    officer_title        = EXCLUDED.officer_title,
                    is_ten_percent_owner = EXCLUDED.is_ten_percent_owner,
                    is_other             = EXCLUDED.is_other,
                    other_text           = EXCLUDED.other_text
                """,
                (
                    accession_number,
                    filer.filer_cik,
                    filer.filer_name,
                    filer.street1,
                    filer.street2,
                    filer.city,
                    filer.state,
                    filer.zip_code,
                    filer.state_description,
                    filer.is_director,
                    filer.is_officer,
                    filer.officer_title,
                    filer.is_ten_percent_owner,
                    filer.is_other,
                    filer.other_text,
                ),
            )

        for note in parsed.footnotes:
            cur.execute(
                """
                INSERT INTO insider_transaction_footnotes
                    (accession_number, footnote_id, footnote_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (accession_number, footnote_id) DO UPDATE SET
                    footnote_text = EXCLUDED.footnote_text
                """,
                (accession_number, note.footnote_id, note.footnote_text),
            )

        for txn in parsed.transactions:
            footnote_refs_json = Jsonb(
                [{"footnote_id": ref.footnote_id, "field": ref.field} for ref in txn.footnote_refs]
            )
            cur.execute(
                """
                INSERT INTO insider_transactions (
                    instrument_id, accession_number, txn_row_num,
                    filer_cik, filer_name, filer_role,
                    security_title,
                    txn_date, deemed_execution_date,
                    txn_code, acquired_disposed_code,
                    equity_swap_involved, transaction_timeliness,
                    shares, price, post_transaction_shares,
                    direct_indirect, nature_of_ownership,
                    is_derivative,
                    conversion_exercise_price,
                    exercise_date, expiration_date,
                    underlying_security_title,
                    underlying_shares, underlying_value,
                    footnote_refs, txn_date_invalid
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s,
                    %s,
                    %s, %s,
                    %s,
                    %s, %s,
                    %s, %s
                )
                ON CONFLICT (accession_number, txn_row_num) DO UPDATE SET
                    instrument_id             = EXCLUDED.instrument_id,
                    filer_cik                 = EXCLUDED.filer_cik,
                    filer_name                = EXCLUDED.filer_name,
                    filer_role                = EXCLUDED.filer_role,
                    security_title            = EXCLUDED.security_title,
                    txn_date                  = EXCLUDED.txn_date,
                    deemed_execution_date     = EXCLUDED.deemed_execution_date,
                    txn_code                  = EXCLUDED.txn_code,
                    acquired_disposed_code    = EXCLUDED.acquired_disposed_code,
                    equity_swap_involved      = EXCLUDED.equity_swap_involved,
                    transaction_timeliness    = EXCLUDED.transaction_timeliness,
                    shares                    = EXCLUDED.shares,
                    price                     = EXCLUDED.price,
                    post_transaction_shares   = EXCLUDED.post_transaction_shares,
                    direct_indirect           = EXCLUDED.direct_indirect,
                    nature_of_ownership       = EXCLUDED.nature_of_ownership,
                    is_derivative             = EXCLUDED.is_derivative,
                    conversion_exercise_price = EXCLUDED.conversion_exercise_price,
                    exercise_date             = EXCLUDED.exercise_date,
                    expiration_date           = EXCLUDED.expiration_date,
                    underlying_security_title = EXCLUDED.underlying_security_title,
                    underlying_shares         = EXCLUDED.underlying_shares,
                    underlying_value          = EXCLUDED.underlying_value,
                    footnote_refs             = EXCLUDED.footnote_refs,
                    txn_date_invalid          = EXCLUDED.txn_date_invalid
                """,
                (
                    entity_instrument_id,
                    accession_number,
                    txn.txn_row_num,
                    txn.filer_cik,
                    _primary_filer_name(parsed, txn.filer_cik),
                    _primary_filer_role(parsed, txn.filer_cik),
                    txn.security_title,
                    txn.txn_date,
                    txn.deemed_execution_date,
                    txn.txn_code,
                    txn.acquired_disposed_code,
                    txn.equity_swap_involved,
                    txn.transaction_timeliness,
                    txn.shares,
                    txn.price,
                    txn.post_transaction_shares,
                    txn.direct_indirect,
                    txn.nature_of_ownership,
                    txn.is_derivative,
                    txn.conversion_exercise_price,
                    txn.exercise_date,
                    txn.expiration_date,
                    txn.underlying_security_title,
                    txn.underlying_shares,
                    txn.underlying_value,
                    footnote_refs_json,
                    txn.txn_date_invalid,
                ),
            )

        # #3227 — the holding lines, keyed like the transactions (sql/503).
        for holding in parsed.holdings:
            cur.execute(
                """
                INSERT INTO insider_form4_holdings (
                    instrument_id, accession_number, row_num,
                    filer_cik, filer_name, filer_role,
                    security_title, shares, value_owned, is_derivative,
                    direct_indirect, nature_of_ownership,
                    conversion_exercise_price, exercise_date, expiration_date,
                    underlying_security_title, underlying_shares, underlying_value,
                    footnote_refs
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s
                )
                ON CONFLICT (accession_number, row_num) DO UPDATE SET
                    instrument_id             = EXCLUDED.instrument_id,
                    filer_cik                 = EXCLUDED.filer_cik,
                    filer_name                = EXCLUDED.filer_name,
                    filer_role                = EXCLUDED.filer_role,
                    security_title            = EXCLUDED.security_title,
                    shares                    = EXCLUDED.shares,
                    value_owned               = EXCLUDED.value_owned,
                    is_derivative             = EXCLUDED.is_derivative,
                    direct_indirect           = EXCLUDED.direct_indirect,
                    nature_of_ownership       = EXCLUDED.nature_of_ownership,
                    conversion_exercise_price = EXCLUDED.conversion_exercise_price,
                    exercise_date             = EXCLUDED.exercise_date,
                    expiration_date           = EXCLUDED.expiration_date,
                    underlying_security_title = EXCLUDED.underlying_security_title,
                    underlying_shares         = EXCLUDED.underlying_shares,
                    underlying_value          = EXCLUDED.underlying_value,
                    footnote_refs             = EXCLUDED.footnote_refs
                """,
                (
                    entity_instrument_id,
                    accession_number,
                    holding.row_num,
                    holding.filer_cik,
                    _primary_filer_name(parsed, holding.filer_cik),
                    _primary_filer_role(parsed, holding.filer_cik),
                    holding.security_title,
                    holding.shares,
                    holding.value_owned,
                    holding.is_derivative,
                    holding.direct_indirect,
                    holding.nature_of_ownership,
                    holding.conversion_exercise_price,
                    holding.exercise_date,
                    holding.expiration_date,
                    holding.underlying_security_title,
                    holding.underlying_shares,
                    holding.underlying_value,
                    Jsonb([{"footnote_id": ref.footnote_id, "field": ref.field} for ref in holding.footnote_refs]),
                ),
            )

    # Write-through observations + refresh _current (#888 / spec §"Eliminate
    # periodic re-scan jobs"). Replaces the legacy nightly
    # ownership_observations_sync read-from-typed-tables path with an
    # inline call so the operator-visible rollup reflects the new filing
    # without waiting for the next sync cycle.
    #
    # Per-share-class fan-out (#1117 PR-B): same Form 4 describes the
    # same issuer; each share-class sibling carries its own
    # ownership_insiders_observations rows. Entity-level tables
    # (insider_filings, insider_transactions, insider_filers,
    # insider_transaction_footnotes) stay PK=accession — siblings see
    # them via filing_events bridge in read paths.
    siblings = routing.sibling_instrument_ids or [entity_instrument_id]
    for sibling_iid in siblings:
        _record_form4_observations_for_filing(
            conn,
            instrument_id=sibling_iid,
            accession_number=accession_number,
            primary_document_url=primary_document_url,
            parsed=parsed,
            filed_at=filed_at,
            entity_instrument_id=entity_instrument_id,
        )
        if refresh_current:
            refresh_insiders_current(conn, instrument_id=sibling_iid)

    # #828 PR-1 — a mislinked filing's owner-instrument filing_events rows ARE
    # the L2/tombstone-count pollution (the per-instrument read bridges key on
    # fe.instrument_id). Delete every non-sibling binding and upsert one row
    # per issuer sibling so the bridge is correct immediately, without waiting
    # for the issuer's own discovery stream to walk the accession. Stale
    # non-sibling observations (legacy pre-#1117 fan-out, or a rewash of a
    # historically-mislinked accession) are soft-deleted and the affected
    # instruments' _current refreshed so the owner's rollup drops them too
    # (Codex ckpt-2 finding 2).
    if routing.is_mislink:
        reconcile_mislinked_ownership_filing_events(
            conn,
            accession_number=accession_number,
            sibling_instrument_ids=routing.sibling_instrument_ids,
            filing_type=parsed.document_type or "",
            filed_at=filed_at,
            period_of_report=parsed.period_of_report,
            primary_document_url=primary_document_url,
            symbol=parsed.issuer_trading_symbol,
        )
        for stale_iid in tombstone_non_sibling_insider_observations(
            conn,
            source_accession=accession_number,
            sibling_instrument_ids=routing.sibling_instrument_ids,
        ):
            refresh_insiders_current(conn, instrument_id=stale_iid)


def _record_form4_observations_for_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: ParsedFiling,
    filed_at: datetime | None = None,
    entity_instrument_id: int | None = None,
) -> None:
    """Derive one ``ownership_insiders_observations`` row per
    ``(filer_cik, direct_indirect)`` from the parsed transactions and
    record it.

    Mirrors the existing batch-sync rule in
    ``ownership_observations_sync.sync_insiders``:

      - Filter: ``post_transaction_shares IS NOT NULL`` AND
        ``is_derivative = FALSE`` (derivative-side rows aren't a
        share-balance signal).
      - Group key: ``(filer_cik, direct_indirect)``. Latest txn per
        group wins (``txn_date DESC, txn_row_num DESC``) — except an
        ``I`` group whose vehicles' balances sum under
        :func:`form4_observation_shares`, and a group whose holding
        lines fold in under :func:`form4_fold_shares` (#3227).
      - ``ownership_nature``: ``'indirect'`` when direct_indirect='I',
        else ``'direct'``.

    The two-axis identity is preserved at the observations table — a
    filer's direct + indirect splits produce SEPARATE observation
    rows.
    """
    # Build the latest-per-group map. Key: (filer_cik, direct_indirect).
    latest: dict[tuple[str | None, str | None], ParsedTransaction] = {}
    for txn in parsed.transactions:
        if txn.is_derivative:
            continue
        if txn.post_transaction_shares is None:
            continue
        if txn.txn_date is None:
            continue
        # #1687 — a future-dated typo must not win the latest-per-group
        # holding (it would write a future period_end and the wrong share
        # balance into ownership_insiders_current).
        if txn.txn_date_invalid:
            continue
        key = (txn.filer_cik, txn.direct_indirect)
        prior = latest.get(key)
        if prior is None:
            latest[key] = txn
            continue
        # Pick later txn_date; tie-break on txn_row_num
        if (txn.txn_date, txn.txn_row_num) > ((prior.txn_date or date.min), prior.txn_row_num):
            latest[key] = txn

    run_id = uuid4()
    owner_ciks = [f.filer_cik for f in parsed.filers]
    if parsed.holdings:
        _record_form4_holding_observations(
            conn,
            instrument_id=instrument_id,
            accession_number=accession_number,
            primary_document_url=primary_document_url,
            parsed=parsed,
            run_id=run_id,
        )

    if not latest:
        return

    # #899 — filed_at carries the SEC filing timestamp. The trade date
    # is preserved as period_end (the observation natural key). Fallback
    # to the historical txn_date derivation only when no manifest /
    # filing_events timestamp exists for the accession.
    if filed_at is None:
        logger.info(
            "form4 observations: no SEC filed_at for accession=%s; falling back to txn_date",
            accession_number,
        )

    for (filer_cik, direct_indirect), txn in latest.items():
        if txn.txn_date is None or txn.post_transaction_shares is None:
            # Belt-and-braces — the filter above already excludes these.
            continue
        holder_name = _primary_filer_name(parsed, filer_cik)
        if not holder_name and not filer_cik:
            # Skip rows with no identity at all.
            continue
        shares = Decimal(txn.post_transaction_shares)
        # ``insider_transactions`` holds the lines on the entity instrument, so the cross-filing
        # check reads there, whichever sibling this is.
        unit_cik = (filer_cik or "").strip() or None
        unit_txns = [t for t in parsed.transactions if ((t.filer_cik or "").strip() or None) == unit_cik]
        earlier = partial(
            form4_earlier_balances,
            conn,
            instrument_id=entity_instrument_id if entity_instrument_id is not None else instrument_id,
            accession_number=accession_number,
            filer_cik=unit_cik,
        )
        if direct_indirect == "I":
            # #3227 — the sum of the filer's vehicles' balances where the filing's arithmetic
            # allows it, else this latest line.
            verdict, vehicle_sum = form4_observation_shares(
                unit_txns,
                document_type=parsed.document_type,
                filer_cik=unit_cik,
                owner_ciks=owner_ciks,
                earlier_balances=earlier,
            )
            if verdict == FORM4_VEHICLE_SUMS and vehicle_sum is not None:
                shares = vehicle_sum
        if direct_indirect in ("D", "I"):
            # #3227 — the filer's holding lines of this D/I fold in where the filing's arithmetic
            # allows it; every holding line is attributed to the first owner, as the parser does.
            fold_verdict, folded = form4_fold_shares(
                unit_txns,
                [h for h in parsed.holdings if ((h.filer_cik or "").strip() or None) == unit_cik],
                direct_indirect,
                document_type=parsed.document_type,
                filer_cik=unit_cik,
                owner_ciks=owner_ciks,
                earlier_balances=earlier,
            )
            if fold_verdict == FORM4_VEHICLE_SUMS and folded is not None:
                shares = folded
        nature = "indirect" if direct_indirect == "I" else "direct"
        record_insider_observation(
            conn,
            instrument_id=instrument_id,
            holder_cik=filer_cik,
            holder_name=holder_name or (filer_cik or "UNKNOWN"),
            ownership_nature=nature,  # type: ignore[arg-type]
            source="form4",
            source_document_id=accession_number,
            source_accession=accession_number,
            source_field=None,
            source_url=primary_document_url or None,
            filed_at=filed_at
            if filed_at is not None
            else datetime.combine(txn.txn_date, datetime.min.time(), tzinfo=UTC),
            period_start=None,
            period_end=txn.txn_date,
            ingest_run_id=run_id,
            shares=shares,
        )


def _record_form4_holding_observations(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: ParsedFiling,
    run_id: UUID,
) -> None:
    """The live writer's half of :func:`form4_holding_observations` (#3227): one observation per
    written holding-only group of the first listed owner (every line is attributed to them)."""
    with conn.cursor() as cur:
        cur.execute("SELECT filed_at FROM sec_filing_manifest WHERE accession_number = %s", (accession_number,))
        row = cur.fetchone()
    manifest_filed_at = row[0] if row is not None else None
    filer_cik = parsed.filers[0].filer_cik
    groups = form4_holding_observations(
        [h for h in parsed.holdings if h.filer_cik == filer_cik],
        [t for t in parsed.transactions if t.filer_cik == filer_cik],
        document_type=parsed.document_type,
        owner_ciks=[f.filer_cik for f in parsed.filers],
        signature_date=parsed.signature_date,
        period_of_report=parsed.period_of_report,
        manifest_filed_at=manifest_filed_at,
    )
    holder_name = _primary_filer_name(parsed, filer_cik).strip()
    for g in groups:
        if g.shares is None or g.period_end is None or manifest_filed_at is None:
            continue
        record_insider_observation(
            conn,
            instrument_id=instrument_id,
            holder_cik=filer_cik,
            holder_name=holder_name or filer_cik,
            ownership_nature=FORM4_HOLDING_NATURE[g.direct_indirect],
            source="form4",
            source_document_id=accession_number,
            source_accession=accession_number,
            source_field=None,
            source_url=primary_document_url or None,
            filed_at=manifest_filed_at,
            period_start=None,
            period_end=g.period_end,
            ingest_run_id=run_id,
            shares=g.shares,
        )


# ---------------------------------------------------------------------
# #3227 — a Form 4's Table I holding lines reach the insider observation
# ---------------------------------------------------------------------

FORM4_HOLDING_NATURE: Final[dict[str, Literal["direct", "indirect"]]] = {"D": "direct", "I": "indirect"}
FORM4_HOLDING_WRITES: Final = frozenset({"one_line", "summed"})


class Form4HoldingLine(Protocol):
    """The holding fields :func:`form4_holding_observations` reads. ``ParsedHolding`` satisfies it,
    and so does a stored ``insider_form4_holdings`` row (``derive_form4_observations``)."""

    @property
    def is_derivative(self) -> bool: ...
    @property
    def security_title(self) -> str | None: ...
    @property
    def shares(self) -> Decimal | None: ...
    @property
    def direct_indirect(self) -> str | None: ...


class Form4BalanceLine(Protocol):
    """The transaction fields :func:`form4_holding_observations` reads."""

    @property
    def is_derivative(self) -> bool: ...
    @property
    def security_title(self) -> str | None: ...
    @property
    def post_transaction_shares(self) -> Decimal | None: ...
    @property
    def direct_indirect(self) -> str | None: ...


@dataclass(frozen=True)
class Form4HoldingGroup:
    """One holding-only group's verdict. ``shares`` is set only when the verdict is in
    :data:`FORM4_HOLDING_WRITES` and the group has a ``period_end``."""

    direct_indirect: str
    verdict: str
    shares: Decimal | None
    period_end: date | None


def form4_holding_as_of(signature_date: date | None, period_of_report: date | None, filed: date) -> date | None:
    """A holding line's ``period_end``. The note to Instruction 4(a)(i) dates the balance "by the
    date of the Form", and the XML carries no per-line date, so it is read as the signature date —
    an interpretation fixed by construction (spec, Source rule): the signature date when
    ``period_of_report <= signature <= filed``, else ``period_of_report`` when it is ``<= filed``,
    else none. The bounds refuse a typo in either date that would dominate ``period_end DESC``
    (the #1687 hazard). ``filed`` is the UTC date of ``sec_filing_manifest.filed_at``."""
    if signature_date is not None and period_of_report is not None and period_of_report <= signature_date <= filed:
        return signature_date
    if period_of_report is not None and period_of_report <= filed:
        return period_of_report
    return None


def form4_holding_observations(
    holdings: Iterable[Form4HoldingLine],
    transactions: Iterable[Form4BalanceLine],
    *,
    document_type: str | None,
    owner_ciks: Iterable[str | None],
    signature_date: date | None,
    period_of_report: date | None,
    manifest_filed_at: datetime | None,
) -> list[Form4HoldingGroup]:
    """ONE filer's holding-only groups in ONE Form 4/4A, each with its verdict.

    Spec: ``docs/specs/ownership/2026-10-18-3227-form4-holding-rows.md``, Change 3; both Form 4
    observation writers call this, so the weekly re-derivation cannot revert the live one (#3874).
    ``holdings`` and ``transactions`` are the filer's lines in the accession, derivative ones
    included (they are ignored). Returns nothing outside Form 4/4A, without a manifest
    ``filed_at``, or outside :func:`form4_within_retention` — no fallback date applies.

    A group is the filer's non-derivative ``D`` (or ``I``) holding lines, when the accession has no
    non-derivative transaction line of that D/I — any line, used by the transaction writers or not,
    and one with no D/I counts as ``D`` because both writers label it ``direct``. Where a
    transaction line exists the transaction rule governs and the holding lines are left alone (the
    mixed-group residual). A holding line with no D/I joins no group.

    Verdicts (the labels ``scripts.census_3227_form4_holding_rows`` prints):
      * ``refused:no_share_count_line`` — a line has no share count (the SEC value branch, or an
        unreadable amount), so the group's share total is unknown;
      * ``one_line`` — its shares;
      * ``summed`` — several lines, summed when ``form3_observation_shares``'s conditions hold (one
        owner, every line >= 0, one non-blank title or several naming one class
        (:func:`form4_title_match`, which then decides "that title" below), no two equal amounts
        among the filer's holding lines of that title, D and I together) and no amount of the group equals a
        column-5 balance of a transaction line of that title — a Form 4 carries balances on
        transaction rows, and a holding equal to one may be the same block restated (Instruction
        4(b)(iv));
      * ``refused:<condition>`` otherwise. A refused group writes nothing: never a partial sum,
        never the last line (UMH's last ``D`` line is a preferred class).
    A group with no :func:`form4_holding_as_of` date writes nothing either.
    """
    if document_type not in FORM4_VEHICLE_SUM_DOCUMENT_TYPES or manifest_filed_at is None:
        return []
    filed = manifest_filed_at.astimezone(UTC).date()
    if not form4_within_retention(filed):
        return []
    lines = [h for h in holdings if not h.is_derivative]
    txns = [t for t in transactions if not t.is_derivative]
    covered = {"I" if t.direct_indirect == "I" else "D" for t in txns}
    holding_amounts = [((h.security_title or "").strip(), h.shares) for h in lines if h.shares is not None]
    txn_balances = [
        ((t.security_title or "").strip(), t.post_transaction_shares)
        for t in txns
        if t.post_transaction_shares is not None
    ]
    owner_count = len({(c or "").strip() for c in owner_ciks})
    period_end = form4_holding_as_of(signature_date, period_of_report, filed)

    out: list[Form4HoldingGroup] = []
    for di in ("D", "I"):
        if di in covered:
            continue
        group = [h for h in lines if h.direct_indirect == di]
        if not group:
            continue
        verdict = _form4_holding_verdict(group, holding_amounts, txn_balances, owner_count)
        shares: Decimal | None = None
        if verdict in FORM4_HOLDING_WRITES and period_end is not None:
            shares = sum((h.shares for h in group if h.shares is not None), Decimal(0))
        out.append(Form4HoldingGroup(direct_indirect=di, verdict=verdict, shares=shares, period_end=period_end))
    return out


def _form4_holding_verdict(
    group: list[Form4HoldingLine],
    holding_amounts: list[tuple[str, Decimal]],
    txn_balances: list[tuple[str, Decimal]],
    owner_count: int,
) -> str:
    if any(h.shares is None for h in group):
        return "refused:no_share_count_line"
    if len(group) == 1:
        return "one_line"
    if owner_count != 1:
        return "refused:joint_filing"
    amounts = [h.shares for h in group if h.shares is not None]
    if any(s < 0 for s in amounts):
        return "refused:negative"
    titles = {(h.security_title or "").strip() for h in group}
    same_title = form4_title_match(titles)
    if same_title is None:
        return "refused:multi_title"
    if titles == {""}:
        return "refused:blank_title"
    same = [a for t, a in holding_amounts if same_title(t)]
    if len(set(same)) != len(same):
        return "refused:equal_amounts"
    if set(amounts) & {b for t, b in txn_balances if same_title(t)}:
        return "refused:equal_to_transaction_balance"
    return "summed"


# ---------------------------------------------------------------------
# #3227 — a Form 4's indirect observation sums its vehicles' balances
# ---------------------------------------------------------------------

FORM4_VEHICLE_SUM_DOCUMENT_TYPES: Final = frozenset({"4", "4/A"})
FORM4_VEHICLE_SUMS: Final = "sums"


class Form4Line(Protocol):
    """The Table I fields :func:`form4_observation_shares` reads. ``ParsedTransaction`` satisfies
    it, and so does a stored ``insider_transactions`` row (``sync_insiders``)."""

    @property
    def txn_row_num(self) -> int: ...
    @property
    def is_derivative(self) -> bool: ...
    @property
    def security_title(self) -> str | None: ...
    @property
    def txn_date(self) -> date | None: ...
    @property
    def txn_date_invalid(self) -> bool: ...
    @property
    def shares(self) -> Decimal | None: ...
    @property
    def acquired_disposed_code(self) -> str | None: ...
    @property
    def post_transaction_shares(self) -> Decimal | None: ...
    @property
    def direct_indirect(self) -> str | None: ...
    @property
    def nature_of_ownership(self) -> str | None: ...


def form4_form_key(direct_indirect: str | None, nature_of_ownership: str | None) -> tuple[str | None, str]:
    """A line's form of ownership: ``('I', nature)`` with the nature reduced to lower-case ASCII
    alphanumerics ('' is blank), and ``(code, '')`` for every other line, so the ``D`` lines are
    one series whatever their nature text."""
    if direct_indirect != "I":
        return (direct_indirect, "")
    return ("I", re.sub(r"[^a-zA-Z0-9]", "", nature_of_ownership or "").lower())


# #3227 multi-title — a Table I security title's class, a rule fixed by construction (spec
# ``docs/specs/ownership/2026-10-18-3227-form4-multi-title.md``). Form 4 General Instruction 3(b):
# "The title of the security should clearly identify the class". ``<securityTitle>`` is free text
# and a line carries no CUSIP or class code, so the class is the title's own words, minus only the
# words below, which say where or how a share of the class is held, never which class it is. Any
# other word stays in the key, so a title this list does not explain never matches another.
_TITLE_DROPPED_WORDS: Final = frozenset(
    {
        # the generic noun of an equity title ("Common Shares" is "Common Stock")
        *("stock", "stocks", "share", "shares"),
        # vesting state of shares of the class ("units" stays; "restricted" stays beside "voting")
        *("restricted", "unvested", "vested"),
        # plan, account and custody wrappers named in the title
        *("plan:401", "ira", "roth", "sep", "esop", "espp", "sip", "psp", "drp", "drip", "savings", "retirement"),
        *("plan", "book", "entry", "street", "name", "direct", "indirect", "account"),
        # ownership wording ("Held by Spouse", "Jointly Owned", the defined term "the")
        *("by", "held", "in", "with", "of", "the", "and", "spouse", "joint", "jointly", "owned"),
    }
)
_TITLE_BASE_WORDS: Final = frozenset({"common", "ordinary", "capital"})
_TITLE_EQUITY_NOUNS: Final = frozenset({"stock", "stocks", "share", "shares"})
# A par-value phrase as a whole, its amount included: "$.10 par value", "par value $0.01 per share",
# "no par value". Numbers and "no"/"without" elsewhere in the title stay.
_TITLE_PAR: Final = re.compile(
    r"(?:\$\s*[\d.,/-]*\d[\d.,/-]*\s*)?(?:(?:no|without)\s+)?par\s+valu?e?\b(?:\s+of)?"
    r"(?:\s*\$\s*[\d.,/-]*\d[\d.,/-]*)?(?:\s+per\s+(?:share|shares|shre))?"
)
# "401(k)", "401k", "401-K", "(401)k", "403(b)": a plan, so its letter is not read as a class. Never
# after "class"/"series" (an identifier) or inside an extracted ``class:`` word.
_TITLE_PLAN_ID: Final = re.compile(r"(?<!:)(?<!class )(?<!series )\b40[13][\s()\-]*[kb]\b")
_TITLE_PROFIT_SHARING_PLAN: Final = re.compile(r"\bprofit\W+sharing\W+plan\b")
_TITLE_CLASS_ID: Final = re.compile(r"\bclass\s+(\w+(?:[-.–/]\w+)*)")
# A word, with any digits joined to it by "-", ".", "/" or a dash kept in it ("a-1", "1/2", "6.6").
_TITLE_TOKEN: Final = re.compile(r"(?:class|plan):[\w.–/-]+|\w+(?:[-.–/]\d+)*")


def form4_title_class(title: str | None) -> tuple[str, ...] | None:
    """The class a Table I ``security_title`` is read as, as its ordered words, or ``None``.

    NFC-normalised and lower-cased; a par-value phrase is removed whole (its trailing amount only
    with a "$"); "401(k)"-style plan names become one dropped
    word and "class X-1" the one word ``class:x-1`` (punctuation kept, so ``x-1`` is not ``x1``);
    "profit sharing plan" is a plan; then the title's words in order (a base word once, any other
    repeat kept), without :data:`_TITLE_DROPPED_WORDS` ("restricted"
    is kept when the title says "voting": Canadian restricted voting shares are a class). ``None``
    when no equity noun or base word (common, ordinary, capital) appears. A title whose only base
    is its equity noun ("Restricted Stock") reads as ``common``. "Common Stock, $.10 par value (SIP)"
    and "Restricted Shares of Common Stock" are ``("common",)``; "Class A Common Stock - Retirement
    Plan" is ``("class:a", "common")``; "Common Stock (SUA)", "Prfd Stock", "Commom Stock" and
    "2024 Annual Restricted Stock Units" each keep a word ``("common",)`` lacks.
    """
    text = _TITLE_PAR.sub(" ", unicodedata.normalize("NFC", title or "").lower())
    text = _TITLE_CLASS_ID.sub(lambda m: " class:" + m.group(1) + " ", text)
    text = _TITLE_PLAN_ID.sub(" plan:401 ", text)
    text = _TITLE_PROFIT_SHARING_PLAN.sub(" plan ", text)
    tokens = _TITLE_TOKEN.findall(text)
    if not any(t in _TITLE_EQUITY_NOUNS or t in _TITLE_BASE_WORDS for t in tokens):
        return None
    keep_restricted = "voting" in tokens
    kept = [t for t in tokens if t not in _TITLE_DROPPED_WORDS or (keep_restricted and t == "restricted")]
    if not set(kept) & _TITLE_BASE_WORDS:
        kept.append("common")
    # A base word once ("Common Stock, par value $.01 (the "Common Stock")"); any other repeat stays.
    first_base = [t not in _TITLE_BASE_WORDS or kept.index(t) == i for i, t in enumerate(kept)]
    return tuple(t for t, keep in zip(kept, first_base, strict=True) if keep)


Form4TitleMatch = Callable[[str | None], bool]


def form4_title_match(titles: Iterable[str | None]) -> Form4TitleMatch | None:
    """Condition 3's "one security title" test for a unit's titles, or ``None`` when it refuses.

    One distinct title: the test is equality with it — exactly the rule before this function, so
    a single-title unit's verdict cannot change. Several: each must have a :func:`form4_title_class`
    and all the same one; the test is then class equality, so "same title" in the unit's other
    conditions and in 5c's earlier balances means "same class"."""
    distinct = set(titles)
    if len(distinct) == 1:
        only = next(iter(distinct))
        return lambda t: t == only
    classes = {form4_title_class(t) for t in distinct}
    if len(classes) != 1 or None in classes:
        return None
    key = next(iter(classes))
    return lambda t: t is not None and form4_title_class(t) == key


# Freezes the rule: every word list and pattern, and the source of both functions. A test pins it.
FORM4_TITLE_CLASS_VERSION: Final = hashlib.sha256(
    repr(
        (
            sorted(_TITLE_DROPPED_WORDS),
            sorted(_TITLE_BASE_WORDS),
            sorted(_TITLE_EQUITY_NOUNS),
            *((r.pattern, r.flags) for r in (_TITLE_PAR, _TITLE_PLAN_ID, _TITLE_PROFIT_SHARING_PLAN)),
            *((r.pattern, r.flags) for r in (_TITLE_CLASS_ID, _TITLE_TOKEN)),
            inspect.getsource(form4_title_class),
            inspect.getsource(form4_title_match),
        )
    ).encode()
).hexdigest()[:12]


def _form4_opening(line: Form4Line) -> Decimal | None:
    """Column 5 before the line's own transaction: ``post - signed shares`` (``A`` adds)."""
    post, shares = line.post_transaction_shares, line.shares
    if post is None or shares is None:
        return None
    if line.acquired_disposed_code == "A":
        return post - shares
    if line.acquired_disposed_code == "D":
        return post + shares
    return None


def _form4_line_usable(line: Form4Line) -> bool:
    post, shares, opening = line.post_transaction_shares, line.shares, _form4_opening(line)
    return (
        post is not None
        and post >= 0
        and shares is not None
        and shares >= 0
        and opening is not None
        and opening >= 0
        and line.txn_date is not None
        and not line.txn_date_invalid
        and line.direct_indirect in ("D", "I")
        and bool((line.security_title or "").strip())
        and (line.direct_indirect == "D" or form4_form_key("I", line.nature_of_ownership)[1] != "")
    )


def form4_observation_shares(
    lines: Iterable[Form4Line],
    *,
    document_type: str | None,
    filer_cik: str | None,
    owner_ciks: Iterable[str | None],
    earlier_balances: Callable[[Form4TitleMatch], Iterable[tuple[str | None, str | None, Decimal]] | None],
) -> tuple[str, Decimal | None]:
    """Whether ONE filer's ``I`` observation of ONE Form 4 is the sum of its vehicles' balances.

    Spec: ``docs/specs/ownership/2026-10-17-3227-form4-vehicle-balance.md``. ``lines`` are the
    filer's lines in the accession (derivative ones are ignored), D and I, including those the
    writers skip. Returns ``(verdict, sum)``; the sum is set only when the verdict is
    :data:`FORM4_VEHICLE_SUMS`, and the caller otherwise keeps its own latest-line value, never a
    partial sum. Other verdicts name the first failed condition, in the spec's order, with the
    labels ``scripts.census_3227_form4_vehicle_balance`` prints.

    Form 4 Instruction 4(b)(iii) puts each form of indirect ownership on its own line, and 4(a)(i)
    requires the class total after the transactions, but not how that total is laid out across the
    lines' column 5. Filers do it two ways: each line carries its own form's balance (MNSO, WOOF),
    or a running total across forms (Abdiel ``0000950170-23-052960``). So the sum is taken only
    where the filing's arithmetic does not contradict the per-form reading — a policy fixed by
    construction, not a reading of the instructions:

    1. more than one ``I`` form (a blank nature counts, so it refuses at 2);
    2. every line usable: balance, shares and opening (``post - signed shares``) non-null and
       >= 0, an ``A``/``D`` code, a valid date, ``D``/``I``, a non-blank title and, for ``I``, a
       non-blank nature;
    3. one security title, or several naming one class (:func:`form4_title_match`) — classes
       never sum;
    4. the filing's reporting owners are exactly this filer — the parser gives every line to one
       owner, so a joint filing could credit one co-filer with the others' vehicles;
    5. (a) within each form, each line (by ``txn_row_num``) opens at the previous line's balance;
       (b) no line opens at the non-zero balance of a line of another form (``D`` included), in
       either row order; (c) no ``I`` form's first line opens at a non-zero balance that matches
       another form's balance in an earlier filing and none of its own form's
       (``earlier_balances``; ``None`` there means the filing has no manifest time, which refuses);
    6. no two forms' final balances are equal and non-zero — one block reported under an entity
       and again under its owner (Instruction 4(b)(iv)) cannot be told from two equal vehicles.

    Every term is >= 0 and the latest ``I`` line's balance is one of them, so a sum is never below
    the latest-line value.
    """
    if document_type not in FORM4_VEHICLE_SUM_DOCUMENT_TYPES or filer_cik is None:
        return "out_of_scope", None
    unit = sorted((ln for ln in lines if not ln.is_derivative), key=lambda ln: ln.txn_row_num)
    forms = [form4_form_key(ln.direct_indirect, ln.nature_of_ownership) for ln in unit]
    if len({f for f in forms if f[0] == "I"}) < 2:
        return "1 one_form", None
    if not all(_form4_line_usable(ln) for ln in unit):
        return "2 incomplete", None
    same_title = form4_title_match(ln.security_title for ln in unit)
    if same_title is None:
        return "3 multi_title", None
    if {(c or "").strip() for c in owner_ciks} != {filer_cik}:
        return "4 joint_or_owner_mismatch", None

    # Usable lines: every balance, opening and date below is non-null.
    by_form: dict[tuple[str | None, str], list[Form4Line]] = {}
    for ln, form in zip(unit, forms, strict=True):
        by_form.setdefault(form, []).append(ln)
    for form_lines in by_form.values():
        for prev, cur in zip(form_lines, form_lines[1:], strict=False):
            if _form4_opening(cur) != prev.post_transaction_shares:
                return "5a form_breaks_chain", None
    forms_at_balance: dict[Decimal, set[tuple[str | None, str]]] = {}
    for ln, form in zip(unit, forms, strict=True):
        post = ln.post_transaction_shares
        if post is not None and post != 0:
            forms_at_balance.setdefault(post, set()).add(form)
    for ln, form in zip(unit, forms, strict=True):
        opening = _form4_opening(ln)
        if opening is not None and forms_at_balance.get(opening, set()) - {form}:
            return "5b running_total", None

    firsts = {form: _form4_opening(fl[0]) for form, fl in by_form.items() if form[0] == "I"}
    if any(o != 0 for o in firsts.values()):
        history = earlier_balances(same_title)
        if history is None:
            return "5c running_total_across_filings", None
        seen = {(form4_form_key(di, nature), bal) for di, nature, bal in history}
        for form, opening in firsts.items():
            if opening == 0:
                continue
            same = (form, opening) in seen
            other = any(bal == opening and f != form for f, bal in seen)
            if other and not same:
                return "5c running_total_across_filings", None

    finals = {
        form: max(fl, key=lambda ln: (ln.txn_date or date.min, ln.txn_row_num)).post_transaction_shares or Decimal(0)
        for form, fl in by_form.items()
    }
    nonzero = [v for v in finals.values() if v != 0]
    if len(set(nonzero)) != len(nonzero):
        return "6 equal_finals", None
    return FORM4_VEHICLE_SUMS, sum((v for form, v in finals.items() if form[0] == "I"), Decimal(0))


# ---------------------------------------------------------------------
# #3227 — a Form 4's holding lines fold into the same filer's transaction observation
# ---------------------------------------------------------------------

FORM4_FOLD_NOT_MIXED: Final = "not_mixed"


class Form4FoldHolding(Form4HoldingLine, Protocol):
    """The holding fields :func:`form4_fold_shares` reads: :class:`Form4HoldingLine` plus the
    nature, which condition 2 requires of an ``I`` line."""

    @property
    def nature_of_ownership(self) -> str | None: ...


def _form4_holding_usable(line: Form4FoldHolding) -> bool:
    shares = line.shares
    return (
        shares is not None
        and shares >= 0
        and line.direct_indirect in ("D", "I")
        and bool((line.security_title or "").strip())
        and (line.direct_indirect == "D" or form4_form_key("I", line.nature_of_ownership)[1] != "")
    )


def form4_fold_shares(
    transactions: Iterable[Form4Line],
    holdings: Iterable[Form4FoldHolding],
    direct_indirect: str,
    *,
    document_type: str | None,
    filer_cik: str | None,
    owner_ciks: Iterable[str | None],
    earlier_balances: Callable[[Form4TitleMatch], Iterable[tuple[str | None, str | None, Decimal]] | None],
) -> tuple[str, Decimal | None]:
    """Whether ONE filer's ``direct_indirect`` observation of ONE Form 4 folds in its holding lines.

    Spec: ``docs/specs/ownership/2026-10-18-3227-form4-mixed-groups.md``, Change 1; both Form 4
    observation writers call it, so the weekly re-derivation cannot revert the live one (#3874).
    ``transactions`` and ``holdings`` are the filer's lines in the accession, derivative ones
    included (they are ignored), D and I. Returns ``(fold verdict, value)``; the value is set only
    on :data:`FORM4_VEHICLE_SUMS`, and on any other verdict the caller keeps that key's value as
    it computes it without this helper — never a partial sum.

    ``out_of_scope`` outside Form 4/4A or without a CIK; :data:`FORM4_FOLD_NOT_MIXED` unless the
    unit has a non-derivative holding line AND a non-derivative transaction line of
    ``direct_indirect`` (a transaction with no D/I counts as ``D``). Otherwise each holding line is
    a one-line form of its own, whatever its nature, and :func:`form4_observation_shares`'s
    conditions 2–6 run over every line of the unit, transactions and holdings, D and I — with 5b
    counting a holding as another form, 5b′ refusing a non-zero holding equal to any transaction
    balance (Instruction 4(b)(iv)), 5c over the summed key's transaction series only, and 6 over
    the series' finals plus every holding. Zeros are ignored in 5b, 5b′ and 6. The other labels
    are those ``scripts.census_3227_form4_mixed_groups`` prints.

    The value is the key's transaction series' finals plus its holdings. Every term is >= 0, and
    the writers' value without the fold (the latest line, or the ``I`` vehicle sum, whose
    conditions are a subset of these) is one of the terms or their sum, so a fold never falls.
    """
    if document_type not in FORM4_VEHICLE_SUM_DOCUMENT_TYPES or filer_cik is None:
        return "out_of_scope", None
    txns = sorted((t for t in transactions if not t.is_derivative), key=lambda t: t.txn_row_num)
    held = [h for h in holdings if not h.is_derivative]
    if not any(("I" if t.direct_indirect == "I" else "D") == direct_indirect for t in txns) or not any(
        h.direct_indirect == direct_indirect for h in held
    ):
        return FORM4_FOLD_NOT_MIXED, None
    if not all(_form4_line_usable(t) for t in txns) or not all(_form4_holding_usable(h) for h in held):
        return "2 incomplete", None
    same_title = form4_title_match([*(t.security_title for t in txns), *(h.security_title for h in held)])
    if same_title is None:
        return "3 multi_title", None
    if {(c or "").strip() for c in owner_ciks} != {filer_cik}:
        return "4 joint_or_owner_mismatch", None

    # Usable lines: every balance, opening and date below is non-null. A holding's form is
    # ``("H", <its index>)``, distinct from every transaction form and every other holding.
    by_form: dict[tuple[str | None, str], list[Form4Line]] = {}
    for t in txns:
        by_form.setdefault(form4_form_key(t.direct_indirect, t.nature_of_ownership), []).append(t)
    for form_lines in by_form.values():
        for prev, cur in zip(form_lines, form_lines[1:], strict=False):
            if _form4_opening(cur) != prev.post_transaction_shares:
                return "5a form_breaks_chain", None
    forms_at_balance: dict[Decimal, set[tuple[str | None, str]]] = {}
    for form, form_lines in by_form.items():
        for t in form_lines:
            if t.post_transaction_shares:
                forms_at_balance.setdefault(t.post_transaction_shares, set()).add(form)
    for i, h in enumerate(held):
        if h.shares:
            forms_at_balance.setdefault(h.shares, set()).add(("H", str(i)))
    for form, form_lines in by_form.items():
        for t in form_lines:
            opening = _form4_opening(t)
            if opening is not None and forms_at_balance.get(opening, set()) - {form}:
                return "5b running_total", None
    txn_balances = {t.post_transaction_shares for t in txns if t.post_transaction_shares}
    if any(h.shares in txn_balances for h in held):
        return "5b holding_equals_transaction_balance", None

    summed = [f for f in by_form if (f[0] == "I") == (direct_indirect == "I")]
    firsts = {f: _form4_opening(by_form[f][0]) for f in summed}
    if any(o != 0 for o in firsts.values()):
        history = earlier_balances(same_title)
        if history is None:
            return "5c running_total_across_filings", None
        seen = {(form4_form_key(di, nature), bal) for di, nature, bal in history}
        for form, opening in firsts.items():
            if opening == 0:
                continue
            if any(bal == opening and f != form for f, bal in seen) and (form, opening) not in seen:
                return "5c running_total_across_filings", None

    finals = {
        form: max(fl, key=lambda t: (t.txn_date or date.min, t.txn_row_num)).post_transaction_shares or Decimal(0)
        for form, fl in by_form.items()
    }
    every_final = [*finals.values(), *(h.shares or Decimal(0) for h in held)]
    nonzero = [v for v in every_final if v != 0]
    if len(set(nonzero)) != len(nonzero):
        return "6 equal_finals", None
    value = sum((finals[f] for f in summed), Decimal(0))
    value += sum((h.shares or Decimal(0) for h in held if h.direct_indirect == direct_indirect), Decimal(0))
    return FORM4_VEHICLE_SUMS, value


# Condition 5c's sources (spec): the filer's balances on the instrument, of the unit's title, in
# filings with a manifest time strictly earlier than this accession's — non-derivative,
# non-tombstoned Form 4/4A transaction lines and Form 3/3A holding lines, D or I.
# The title is matched in Python (``form4_title_match``), so the query returns every title.
_FORM4_EARLIER_BALANCES_SQL: Final = """
SELECT t.security_title, t.direct_indirect, t.nature_of_ownership, t.post_transaction_shares
  FROM insider_transactions t
  JOIN insider_filings f ON f.accession_number = t.accession_number
  JOIN sec_filing_manifest m ON m.accession_number = t.accession_number
 WHERE t.instrument_id = %(iid)s AND t.filer_cik = %(cik)s
   AND NOT t.is_derivative AND f.document_type IN ('4', '4/A') AND NOT f.is_tombstone
   AND t.direct_indirect IN ('D', 'I') AND t.post_transaction_shares IS NOT NULL
   AND m.filed_at < %(filed_at)s
UNION ALL
SELECT h.security_title, h.direct_indirect, h.nature_of_ownership, h.shares
  FROM insider_initial_holdings h
  JOIN insider_filings f ON f.accession_number = h.accession_number
  JOIN sec_filing_manifest m ON m.accession_number = h.accession_number
 WHERE h.instrument_id = %(iid)s AND h.filer_cik = %(cik)s
   AND NOT h.is_derivative AND f.document_type IN ('3', '3/A') AND NOT f.is_tombstone
   AND h.direct_indirect IN ('D', 'I') AND h.shares IS NOT NULL
   AND m.filed_at < %(filed_at)s
"""


def form4_earlier_balances(
    conn: psycopg.Connection[Any],
    title: str | Form4TitleMatch,
    *,
    instrument_id: int,
    accession_number: str,
    filer_cik: str | None,
) -> list[tuple[str | None, str | None, Decimal]] | None:
    """``(direct_indirect, nature, balance)`` for condition 5c of :func:`form4_observation_shares`,
    or ``None`` when the accession has no ``sec_filing_manifest.filed_at`` to compare against.
    ``title`` is the unit's :func:`form4_title_match` test, or one exact title.

    The CIK matches the stored column exactly, so the ``(instrument_id, filer_cik)`` indexes serve
    it; the parser writes CIKs stripped (``_text``)."""
    with conn.cursor() as cur:
        cur.execute("SELECT filed_at FROM sec_filing_manifest WHERE accession_number = %s", (accession_number,))
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        cur.execute(
            _FORM4_EARLIER_BALANCES_SQL,
            {"iid": instrument_id, "cik": filer_cik, "filed_at": row[0]},
        )
        match = title if callable(title) else (lambda t: t == title)
        return [(r[1], r[2], Decimal(r[3])) for r in cur.fetchall() if match(r[0])]


def filer_role_string(filer: ParsedFiler) -> str | None:
    """Render a ``ParsedFiler``'s relationship flags as the pipe-joined
    role string kept on ``insider_transactions.filer_role`` (a
    denormalised convenience for the widget).

    Order: director, officer:<title>, ten_percent_owner, other:<text>.
    ``None`` when none of the flags are true.
    """
    parts: list[str] = []
    if filer.is_director:
        parts.append("director")
    if filer.is_officer:
        parts.append(f"officer:{filer.officer_title}" if filer.officer_title else "officer")
    if filer.is_ten_percent_owner:
        parts.append("ten_percent_owner")
    if filer.is_other:
        parts.append(f"other:{filer.other_text}" if filer.other_text else "other")
    return "|".join(parts) if parts else None


def _primary_filer_name(parsed: ParsedFiling, cik: str | None) -> str:
    """Resolve filer display name from the parsed filers.

    Kept on the transaction row as a denormalised convenience so the
    instrument-page widget doesn't have to JOIN ``insider_filers`` for
    every read. The source of truth is ``insider_filers`` — this column
    can drift if a later filing corrects the spelling; reader paths
    that need the canonical name JOIN explicitly.
    """
    if cik is None:
        return parsed.filers[0].filer_name if parsed.filers else ""
    for f in parsed.filers:
        if f.filer_cik == cik:
            return f.filer_name
    return parsed.filers[0].filer_name if parsed.filers else ""


def _primary_filer_role(parsed: ParsedFiling, cik: str | None) -> str | None:
    """Pipe-joined relationship flags, same shape as the 056 parser so
    existing consumers keep working. Sourced from the authoritative
    boolean columns on ``insider_filers``."""
    target: ParsedFiler | None = None
    if cik is not None:
        target = next((f for f in parsed.filers if f.filer_cik == cik), None)
    if target is None and parsed.filers:
        target = parsed.filers[0]
    if target is None:
        return None
    return filer_role_string(target)


# ---------------------------------------------------------------------
# Tombstoning — filing-level now, not transaction-level
# ---------------------------------------------------------------------


_TOMBSTONE_DOC_TYPE = "4"  # default keeps Form 4 callers untouched


def _write_tombstone(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    document_type: str = _TOMBSTONE_DOC_TYPE,
) -> None:
    """Mark an accession as unfetchable / unparseable at the filing
    level so the next ingester pass skips it.

    Tombstones carry no ``insider_filers`` / ``insider_transactions``
    children. The reader excludes them via ``is_tombstone = FALSE``
    in the summary query. A re-parse that succeeds for the same
    accession flips the row back to live via the ON CONFLICT DO
    UPDATE branch of :func:`upsert_filing`.

    ``document_type`` is plumbed so Form 5 tombstones land with
    ``document_type='5'`` (vs the legacy Form 4 default). Operator
    audits filtering by ``document_type`` shouldn't see Form 5
    failures misattributed to Form 4."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO insider_filings (
                accession_number, instrument_id, document_type,
                primary_document_url, parser_version, is_tombstone
            ) VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (accession_number) DO NOTHING
            """,
            (
                accession_number,
                instrument_id,
                document_type,
                primary_document_url,
                _PARSER_VERSION,
            ),
        )


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class _DocFetcher(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


# SEC submissions.json ``primaryDocument`` for ownership filings
# (Forms 3/4/5) commonly points at an XSL-rendered HTML view rather
# than the raw XML. The rendered path carries an ``xslF345`` segment
# (with a versioned suffix that has rotated over the years —
# ``xslF345X02``, ``xslF345X03``, ``xslF345X04``, ``xslF345X05``,
# ``xslF345X06`` are all observed in production filings) before the
# filename; the same file without that segment is the canonical XML.
#
#   XSL:  /Archives/edgar/data/320193/000114036126015421/xslF345X06/form4.xml  → text/html
#   Raw:  /Archives/edgar/data/320193/000114036126015421/form4.xml             → text/xml
#
# Pre-fix the regex matched only ``xslF345``, ``xslF345X05`` and
# ``xslF345X06``. Older filings (pre-2018) carry ``X02`` / ``X03`` /
# ``X04`` suffixes — those passed through unchanged, the ingester
# fetched HTML, and the parser failed with a 98%+ parse_miss rate
# on backfill ticks against historical Form 4s.
#
# Match any ``xslF345`` followed by an optional alphanumeric suffix
# so future SEC variants are absorbed without another patch.
_XSL_FORM345_PREFIX_RE = re.compile(r"/xslF345[A-Z0-9]*/")


def _canonical_form_4_url(url: str) -> str:
    """Strip the XSL-rendering segment from a SEC Form-3/4/5 URL so
    the fetch returns raw XML, not XSL-transformed HTML. Idempotent —
    already-canonical URLs pass through unchanged."""
    return _XSL_FORM345_PREFIX_RE.sub("/", url, count=1)


@dataclass(frozen=True)
class IngestResult:
    filings_scanned: int
    filings_parsed: int
    rows_inserted: int
    fetch_errors: int
    parse_misses: int
    # #1698 — transient upsert errors (serialization / deadlock / conn
    # drop) that rolled back + skipped WITHOUT tombstoning. Surfaced so a
    # DB connection storm is visible in the job summary, not silent.
    transient_upsert_errors: int = 0


def ingest_insider_transactions_for_instrument(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    instrument_id: int,
    limit: int = 500,
) -> IngestResult:
    """Targeted backfill for a single instrument.

    The universe-wide :func:`ingest_insider_transactions` selector runs
    newest-first across every Form 4 filing in ``filing_events`` and
    is bounded at 500 per tick — which is fine for keeping up with
    incoming flow but leaves an instrument with hundreds of historical
    filings starved for days while newer filings on other tickers
    cycle through. This variant scopes the scan to one instrument so
    an operator (or a round-robin backfill job) can drain a specific
    ticker's backlog in one pass.

    Same contract as the universe-wide function:

    - Candidate = ``provider='sec'`` + ``filing_type IN ('4', '4/A')``
      + ``primary_document_url IS NOT NULL`` + no existing
      ``insider_filings`` row + ``filing_date >= form4_retention_cutoff()``
      (#1233 PR4 — 3y cap honoured at every chokepoint, including this
      inner SELECT which pre-PR4 lacked the floor predicate).
    - Tombstone on fetch error / parse miss.
    - Caller's fetch URL runs through :func:`_canonical_form_4_url` to
      strip XSL-rendering segments.
    """
    conn.commit()
    retention_cutoff = form4_retention_cutoff()
    candidates: list[tuple[int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.instrument_id,
                   fe.provider_filing_id,
                   fe.primary_document_url
            FROM filing_events fe
            LEFT JOIN insider_filings fil ON fil.accession_number = fe.provider_filing_id
            WHERE fe.provider = 'sec'
              AND fe.filing_type IN ('4', '4/A')
              AND fe.primary_document_url IS NOT NULL
              AND fe.instrument_id = %(iid)s
              AND fil.accession_number IS NULL
              AND fe.filing_date >= %(retention_cutoff)s
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %(limit)s
            """,
            {"iid": instrument_id, "limit": limit, "retention_cutoff": retention_cutoff},
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1]), _canonical_form_4_url(str(row[2]))))
    conn.commit()

    return _process_candidates(conn, fetcher, candidates)


def ingest_insider_transactions(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    limit: int = 500,
) -> IngestResult:
    """Scan Form 4 filings, fetch XML, parse, upsert across all four
    tables.

    Candidate selector:

    1. ``fe.filing_type IN ('4', '4/A')`` — amendments carry the same
       shape and are authoritative replacements.
    2. ``fe.primary_document_url IS NOT NULL``.
    3. No existing ``insider_filings`` row for the accession. A filing
       is ingested exactly once per accession — tombstones live in the
       same table so a failed fetch writes a tombstone row and the
       hourly ingester never re-fetches the same dead URL.
    4. ``fe.filing_date >= form4_retention_cutoff()`` — see
       :data:`INSIDER_FORM4_RETENTION_YEARS`. Historical Form 4s outside
       the cap are skipped permanently; the operator can widen the cap
       if archaeology becomes a need. Python-computed cutoff (not
       ``NOW() - make_interval(...)``) so the Feb 29 logic + UTC anchor
       are shared with the helper and not subject to DB session TZ.
    5. Ordered by filing_date DESC so fresh filings always get budget.

    Bounded per run (``limit=500``) to match expected daily Form 4
    volume across the universe.
    """
    conn.commit()

    # Per-#1117 PR-B: universe-wide selector requires inner-CTE
    # DISTINCT ON dedup so N siblings sharing a CIK do not consume N
    # slots of the LIMIT budget for the same accession.
    retention_cutoff = form4_retention_cutoff()
    candidates: list[tuple[int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH per_accession AS (
                SELECT DISTINCT ON (fe.provider_filing_id)
                    fe.instrument_id, fe.provider_filing_id,
                    fe.primary_document_url, fe.filing_date, fe.filing_event_id
                FROM filing_events fe
                LEFT JOIN insider_filings fil ON fil.accession_number = fe.provider_filing_id
                WHERE fe.provider = 'sec'
                  AND fe.filing_type IN ('4', '4/A')
                  AND fe.primary_document_url IS NOT NULL
                  AND fil.accession_number IS NULL
                  AND fe.filing_date >= %(retention_cutoff)s
                ORDER BY fe.provider_filing_id, fe.instrument_id
            )
            SELECT instrument_id, provider_filing_id, primary_document_url
            FROM per_accession
            ORDER BY filing_date DESC, filing_event_id DESC
            LIMIT %(limit)s
            """,
            {"limit": limit, "retention_cutoff": retention_cutoff},
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1]), _canonical_form_4_url(str(row[2]))))
    conn.commit()

    return _process_candidates(conn, fetcher, candidates)


def _process_candidates(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    candidates: list[tuple[int, str, str]],
) -> IngestResult:
    """Shared fetch-parse-upsert loop for every ingest entry point.

    Each candidate is ``(instrument_id, accession_number,
    canonical_url)``. Fetch errors / None bodies / parse misses all
    write a filing-level tombstone so the ingester never re-fetches
    a dead accession. Successful parses flow through
    :func:`upsert_filing` which refreshes every column on conflict.

    #726: the network leg fans out to a thread pool so SEC's
    ~800 ms per-request response time overlaps across workers.
    Aggregate request rate stays bounded by the shared rate-limit
    clock (``_PROCESS_RATE_LIMIT_CLOCK``) + lock
    (``_PROCESS_RATE_LIMIT_LOCK``) inside ``ResilientClient``.
    Parse + DB upsert stay serial on the caller's connection (no
    concurrent psycopg writes — a future change could parallelise
    that too with a per-thread connection but that's out of scope).
    """
    filings_parsed = 0
    rows_inserted = 0
    fetch_errors = 0
    parse_misses = 0
    transient_upsert_errors = 0

    # Pre-fetch every URL concurrently. Each maps to a discriminated
    # ``(FetchOutcome, body)`` — #1698: a transient 429 (caught -> None
    # through the lossy ``fetch_document_texts``) must NOT be tombstoned
    # like a 404, or a recoverable SEC throttle permanently drops a real
    # filing (411 filings lost in one 429 burst, dev 2026-06-21).
    outcomes = fetch_document_texts_classified(fetcher, (url for _, _, url in candidates))

    for instrument_id, accession, url in candidates:
        # Absent key (URL de-duped away / missing) defaults to TRANSIENT —
        # never tombstone on an unclassified result (Codex ckpt-1 MED).
        outcome, xml = outcomes.get(url, (FetchOutcome.TRANSIENT, None))
        if outcome is FetchOutcome.TRANSIENT:
            # 429-after-retries / 5xx / network / unexpected raise. Leave
            # the accession selectable so the next hourly run retries it —
            # tombstoning here is the #1698 data-loss bug. The classifier
            # already logged the exception class.
            fetch_errors += 1
            continue
        if outcome is not FetchOutcome.OK:
            # MISSING (404 / 410) or EMPTY (a 200 with an empty body).
            # Both PERMANENT: ``store_raw`` rejects an empty payload, so an
            # empty body that slipped through would raise an uncaught
            # ValueError and re-poison every run (eBay + universe insider
            # data frozen ~3 months, #1683). Tombstone + skip stops the
            # selector re-fetching a dead/empty URL.
            logger.warning(
                "ingest_insider_transactions: permanent fetch outcome=%s accession=%s url=%s",
                outcome.value,
                accession,
                url,
            )
            fetch_errors += 1
            _write_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
            )
            conn.commit()
            continue

        # outcome is FetchOutcome.OK — the classifier only emits OK with a
        # non-empty body (EMPTY / MISSING / TRANSIENT all returned above).
        assert xml is not None

        # Persist raw body BEFORE parsing — re-wash workflows depend
        # on this row even if parsing fails. Operator audit
        # 2026-05-03 + PR #808 contract.
        raw_filings.store_raw(
            conn,
            accession_number=accession,
            document_kind="form4_xml",
            payload=xml,
            parser_version=_PARSER_VERSION_FORM4,
            source_url=url,
        )
        conn.commit()

        parsed = parse_form_4_xml(xml)
        if parsed is None:
            parse_misses += 1
            _write_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
            )
            conn.commit()
            continue

        try:
            upsert_filing(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
                parsed=parsed,
            )
            conn.commit()
        except Exception as exc:
            # #1131: pre-#1131 this branch just rolled back + continued.
            # Candidate selector keys on ``insider_filings.accession_number
            # IS NULL`` (see _process_candidates query above) so a
            # deterministic constraint violation re-picked the same
            # accession on every scheduler tick, re-hammering SEC for
            # the same dead XML every hour. Discriminate by psycopg
            # exception class: transient (OperationalError) keeps the
            # legacy retry-loop behaviour (rollback + continue —
            # scheduler picks it back up next tick); deterministic
            # constraint violations write a tombstone in a fresh tx so
            # the next selector run skips the row.
            conn.rollback()
            logger.warning(
                "ingest_insider_transactions: upsert failed accession=%s (%s)",
                accession,
                type(exc).__name__,
                exc_info=True,
            )
            if not is_transient_upsert_error(exc):
                try:
                    _write_tombstone(
                        conn,
                        instrument_id=instrument_id,
                        accession_number=accession,
                        primary_document_url=url,
                    )
                    conn.commit()
                    logger.info(
                        "ingest_insider_transactions: tombstoned accession=%s after deterministic upsert error: %s",
                        accession,
                        format_upsert_error(exc),
                    )
                except Exception:
                    conn.rollback()
                    logger.exception(
                        "ingest_insider_transactions: tombstone INSERT failed after deterministic upsert"
                        " error accession=%s",
                        accession,
                    )
            else:
                # Transient DB error — rolled back, NOT tombstoned, will
                # retry next run. Count it so a connection storm is visible
                # in the job summary instead of vanishing (#1698 review).
                transient_upsert_errors += 1
            continue

        filings_parsed += 1
        rows_inserted += len(parsed.transactions)

    return IngestResult(
        filings_scanned=len(candidates),
        filings_parsed=filings_parsed,
        rows_inserted=rows_inserted,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
        transient_upsert_errors=transient_upsert_errors,
    )


def ingest_insider_transactions_backfill(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    instruments_per_tick: int = 25,
    per_instrument_limit: int = 50,
) -> dict[str, int]:
    """Round-robin backfill for instruments with un-ingested Form 4
    filings. Complements :func:`ingest_insider_transactions` (which
    runs newest-first universe-wide and can starve deep per-ticker
    backlogs).

    Strategy: pick the ``instruments_per_tick`` instruments with the
    most un-ingested candidates, drain up to ``per_instrument_limit``
    oldest candidates for each. "Oldest-first" because the newest-first
    universe job already covers the recent end; this job clears the
    historical tail.

    Returns per-bucket counters so scheduler observability can pick
    out how many instruments made progress.
    """
    conn.commit()

    retention_cutoff = form4_retention_cutoff()

    # Per-#1117 PR-B: dedup by accession before GROUP BY so a single
    # share-class accession is not double-counted across siblings.
    # The canonical-sibling parser fan-out (see upsert_filing) covers
    # all siblings on parse, so picking the canonical sibling for the
    # backfill drain is sufficient — the per-instrument fan-out
    # reaches the others without scheduling them as separate
    # candidates.
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH per_accession AS (
                SELECT DISTINCT ON (fe.provider_filing_id)
                    fe.instrument_id, fe.provider_filing_id
                FROM filing_events fe
                LEFT JOIN insider_filings fil ON fil.accession_number = fe.provider_filing_id
                WHERE fe.provider = 'sec'
                  AND fe.filing_type IN ('4', '4/A')
                  AND fe.primary_document_url IS NOT NULL
                  AND fil.accession_number IS NULL
                  AND fe.filing_date >= %(retention_cutoff)s
                ORDER BY fe.provider_filing_id, fe.instrument_id
            )
            SELECT instrument_id, COUNT(*) AS unfetched
            FROM per_accession
            GROUP BY instrument_id
            ORDER BY unfetched DESC
            LIMIT %(limit)s
            """,
            {"limit": instruments_per_tick, "retention_cutoff": retention_cutoff},
        )
        targets = [(int(r[0]), int(r[1])) for r in cur.fetchall()]
    conn.commit()

    totals = {
        "instruments_processed": 0,
        "filings_parsed": 0,
        "rows_inserted": 0,
        "fetch_errors": 0,
        "parse_misses": 0,
        "transient_upsert_errors": 0,
    }

    for instrument_id, _unfetched in targets:
        candidates: list[tuple[int, str, str]] = []
        with conn.cursor() as cur:
            # PR4 gap fix: inner per-instrument SELECT must honour the
            # retention cutoff too. Pre-PR4 the outer aggregate picked
            # an instrument by count of inside-floor un-ingested
            # candidates, but the inner SELECT was floor-free — so an
            # instrument with one fresh filing + 99 pre-floor filings
            # would consume per_instrument_limit slots on pre-floor
            # work. The cap is honoured uniformly now.
            cur.execute(
                """
                SELECT fe.instrument_id,
                       fe.provider_filing_id,
                       fe.primary_document_url
                FROM filing_events fe
                LEFT JOIN insider_filings fil
                    ON fil.accession_number = fe.provider_filing_id
                WHERE fe.provider = 'sec'
                  AND fe.filing_type IN ('4', '4/A')
                  AND fe.primary_document_url IS NOT NULL
                  AND fe.instrument_id = %(iid)s
                  AND fil.accession_number IS NULL
                  AND fe.filing_date >= %(retention_cutoff)s
                ORDER BY fe.filing_date ASC, fe.filing_event_id ASC
                LIMIT %(limit)s
                """,
                {
                    "iid": instrument_id,
                    "limit": per_instrument_limit,
                    "retention_cutoff": retention_cutoff,
                },
            )
            for row in cur.fetchall():
                candidates.append((int(row[0]), str(row[1]), _canonical_form_4_url(str(row[2]))))
        conn.commit()

        if not candidates:
            continue
        result = _process_candidates(conn, fetcher, candidates)
        totals["instruments_processed"] += 1
        totals["filings_parsed"] += result.filings_parsed
        totals["rows_inserted"] += result.rows_inserted
        totals["fetch_errors"] += result.fetch_errors
        totals["parse_misses"] += result.parse_misses
        totals["transient_upsert_errors"] += result.transient_upsert_errors

    return totals


# ---------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class InsiderSummary:
    """Aggregate view of recent insider activity for an instrument.

    Two lenses, both scoped to the last 90 days of non-derivative
    transactions:

    - **Open-market** (``open_market_*``) — only ``txn_code='P'``
      (open-market buy) and ``txn_code='S'`` (open-market sale). This
      is the discretionary-sentiment signal: the insider chose to
      trade in public markets. Grants, exercises, tax-withholding
      sells, and same-day sell-to-cover lots are excluded because
      they are mechanical consequences of compensation, not
      directional bets.
    - **Total acquired / disposed** (``total_acquired_*`` /
      ``total_disposed_*``) — every non-derivative transaction, split
      by ``acquired_disposed_code`` when present and by ``txn_code``
      otherwise. This is the full insider-holdings-change view, so a
      panel that only shows open-market numbers doesn't accidentally
      imply insiders are net sellers during an RSU-vest month when
      they actually accumulated shares on balance.

    Operators need both. A large grant (A) followed by a small
    sell-to-cover (S) reads very differently from an operator
    deliberately dumping stock into the market.
    """

    # Open-market (discretionary-sentiment) view.
    open_market_net_shares_90d: Decimal
    open_market_buy_count_90d: int
    open_market_sell_count_90d: int
    # Total-acquired / total-disposed (full-activity) view.
    total_acquired_shares_90d: Decimal
    total_disposed_shares_90d: Decimal
    acquisition_count_90d: int
    disposition_count_90d: int
    # Cross-lens metadata.
    unique_filers_90d: int
    latest_txn_date: date | None

    # Back-compat aliases so existing callers that rely on the old
    # ``net_shares_90d`` / ``buy_count_90d`` / ``sell_count_90d``
    # names keep working while the API + frontend migrate.
    @property
    def net_shares_90d(self) -> Decimal:
        return self.open_market_net_shares_90d

    @property
    def buy_count_90d(self) -> int:
        return self.open_market_buy_count_90d

    @property
    def sell_count_90d(self) -> int:
        return self.open_market_sell_count_90d


def get_insider_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> InsiderSummary:
    """Return the 90-day insider summary.

    Tombstoned filings are excluded via an INNER JOIN to
    ``insider_filings``. ``unique_filers_90d`` dedups by ``filer_cik``
    (stable SEC identifier) not ``filer_name``, so two insiders
    sharing a name aren't conflated.

    Always returns a summary object (zero-counters when no activity)
    so callers don't have to branch on None.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                -- Open-market (discretionary) view: only explicit
                -- P / S codes. This is the sentiment signal.
                COALESCE(SUM(
                    CASE
                        WHEN it.txn_code = 'P' THEN it.shares
                        WHEN it.txn_code = 'S' THEN -it.shares
                        ELSE 0
                    END
                ), 0)                                                 AS open_market_net,
                COUNT(*) FILTER (WHERE it.txn_code = 'P')             AS open_market_buys,
                COUNT(*) FILTER (WHERE it.txn_code = 'S')             AS open_market_sells,
                -- Total-activity view: classify by acquired_disposed_code
                -- when SEC gave us one (authoritative), else fall back
                -- to txn_code (P/A/M/X = acquired; S/D/F/G = disposed).
                COALESCE(SUM(
                    CASE
                        WHEN COALESCE(it.acquired_disposed_code,
                                      CASE WHEN it.txn_code IN ('P','A','M','X','C','V','J') THEN 'A'
                                           WHEN it.txn_code IN ('S','D','F','G') THEN 'D'
                                           ELSE NULL END) = 'A'
                        THEN it.shares
                        ELSE 0
                    END
                ), 0)                                                 AS total_acquired,
                COALESCE(SUM(
                    CASE
                        WHEN COALESCE(it.acquired_disposed_code,
                                      CASE WHEN it.txn_code IN ('P','A','M','X','C','V','J') THEN 'A'
                                           WHEN it.txn_code IN ('S','D','F','G') THEN 'D'
                                           ELSE NULL END) = 'D'
                        THEN it.shares
                        ELSE 0
                    END
                ), 0)                                                 AS total_disposed,
                COUNT(*) FILTER (
                    WHERE COALESCE(it.acquired_disposed_code,
                                   CASE WHEN it.txn_code IN ('P','A','M','X','C','V','J') THEN 'A'
                                        WHEN it.txn_code IN ('S','D','F','G') THEN 'D'
                                        ELSE NULL END) = 'A'
                )                                                     AS acquisition_count,
                COUNT(*) FILTER (
                    WHERE COALESCE(it.acquired_disposed_code,
                                   CASE WHEN it.txn_code IN ('P','A','M','X','C','V','J') THEN 'A'
                                        WHEN it.txn_code IN ('S','D','F','G') THEN 'D'
                                        ELSE NULL END) = 'D'
                )                                                     AS disposition_count,
                COUNT(DISTINCT COALESCE(it.filer_cik, it.filer_name)) AS filers,
                MAX(it.txn_date)                                      AS latest
            FROM insider_transactions it
            INNER JOIN insider_filings f
                ON f.accession_number = it.accession_number
               AND f.is_tombstone = FALSE
            WHERE it.instrument_id = %s
              AND it.is_derivative = FALSE
              AND NOT it.txn_date_invalid  -- #1687 — exclude future-dated typos
              AND it.txn_date >= CURRENT_DATE - INTERVAL '90 days'
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None:
        return InsiderSummary(
            open_market_net_shares_90d=Decimal(0),
            open_market_buy_count_90d=0,
            open_market_sell_count_90d=0,
            total_acquired_shares_90d=Decimal(0),
            total_disposed_shares_90d=Decimal(0),
            acquisition_count_90d=0,
            disposition_count_90d=0,
            unique_filers_90d=0,
            latest_txn_date=None,
        )
    return InsiderSummary(
        open_market_net_shares_90d=Decimal(row[0]) if row[0] is not None else Decimal(0),
        open_market_buy_count_90d=int(row[1] or 0),
        open_market_sell_count_90d=int(row[2] or 0),
        total_acquired_shares_90d=Decimal(row[3]) if row[3] is not None else Decimal(0),
        total_disposed_shares_90d=Decimal(row[4]) if row[4] is not None else Decimal(0),
        acquisition_count_90d=int(row[5] or 0),
        disposition_count_90d=int(row[6] or 0),
        unique_filers_90d=int(row[7] or 0),
        latest_txn_date=row[8],
    )


# ---------------------------------------------------------------------
# Rich transaction listing (instrument-page detail widget)
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class InsiderTransactionDetail:
    """Wide-shape row for the instrument-page insider-activity table.

    Every structured field from Form 4 that is useful to an operator
    reading the instrument page is surfaced. Footnote bodies are
    resolved from ``insider_transaction_footnotes`` and attached to
    the row as ``footnotes`` — one entry per (footnote_id, field)
    pair carried on ``footnote_refs``.
    """

    accession_number: str
    txn_row_num: int
    document_type: str
    txn_date: date
    deemed_execution_date: date | None
    filer_cik: str | None
    filer_name: str
    filer_role: str | None
    security_title: str | None
    txn_code: str
    acquired_disposed_code: str | None
    shares: Decimal | None
    price: Decimal | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    is_derivative: bool
    equity_swap_involved: bool | None
    transaction_timeliness: str | None
    # Derivative-only
    conversion_exercise_price: Decimal | None
    exercise_date: date | None
    expiration_date: date | None
    underlying_security_title: str | None
    underlying_shares: Decimal | None
    underlying_value: Decimal | None
    # Footnote bodies relevant to this row, keyed by the field they
    # qualify: {"transactionShares": "Weighted avg price …", ...}
    footnotes: dict[str, str] = field(default_factory=dict)


def list_insider_transactions(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    limit: int = 100,
) -> list[InsiderTransactionDetail]:
    """Return recent insider transactions for an instrument, most
    recent first. Tombstoned filings excluded. Footnote bodies are
    attached per-row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                it.accession_number,
                it.txn_row_num,
                f.document_type,
                it.txn_date,
                it.deemed_execution_date,
                it.filer_cik,
                it.filer_name,
                it.filer_role,
                it.security_title,
                it.txn_code,
                it.acquired_disposed_code,
                it.shares,
                it.price,
                it.post_transaction_shares,
                it.direct_indirect,
                it.nature_of_ownership,
                it.is_derivative,
                it.equity_swap_involved,
                it.transaction_timeliness,
                it.conversion_exercise_price,
                it.exercise_date,
                it.expiration_date,
                it.underlying_security_title,
                it.underlying_shares,
                it.underlying_value,
                it.footnote_refs
            FROM insider_transactions it
            INNER JOIN insider_filings f
                ON f.accession_number = it.accession_number
               AND f.is_tombstone = FALSE
            WHERE it.instrument_id = %s
              AND NOT it.txn_date_invalid  -- #1687 — exclude future-dated typos
            ORDER BY it.txn_date DESC, it.id DESC
            LIMIT %s
            """,
            (instrument_id, limit),
        )
        raw_rows = cur.fetchall()

        # Bulk-load footnote bodies for the accessions we touched.
        accession_numbers = {str(r[0]) for r in raw_rows}
        footnote_bodies: dict[tuple[str, str], str] = {}
        if accession_numbers:
            cur.execute(
                """
                SELECT accession_number, footnote_id, footnote_text
                FROM insider_transaction_footnotes
                WHERE accession_number = ANY(%s)
                """,
                (list(accession_numbers),),
            )
            for acc, fn_id, fn_text in cur.fetchall():
                footnote_bodies[(str(acc), str(fn_id))] = str(fn_text)

    rows: list[InsiderTransactionDetail] = []
    for r in raw_rows:
        acc = str(r[0])
        refs_raw = r[25] or []
        # psycopg returns JSONB as a Python list/dict already.
        refs = refs_raw if isinstance(refs_raw, list) else json.loads(refs_raw)
        footnotes: dict[str, str] = {}
        for ref in refs:
            fn_id = str(ref.get("footnote_id"))
            field_name = str(ref.get("field"))
            body = footnote_bodies.get((acc, fn_id))
            if body is not None:
                footnotes[field_name] = body
        rows.append(
            InsiderTransactionDetail(
                accession_number=acc,
                txn_row_num=int(r[1]),
                document_type=str(r[2]),
                txn_date=r[3],
                deemed_execution_date=r[4],
                filer_cik=r[5],
                filer_name=str(r[6]),
                filer_role=r[7],
                security_title=r[8],
                txn_code=str(r[9]),
                acquired_disposed_code=r[10],
                shares=r[11],
                price=r[12],
                post_transaction_shares=r[13],
                direct_indirect=r[14],
                nature_of_ownership=r[15],
                is_derivative=bool(r[16]),
                equity_swap_involved=r[17],
                transaction_timeliness=r[18],
                conversion_exercise_price=r[19],
                exercise_date=r[20],
                expiration_date=r[21],
                underlying_security_title=r[22],
                underlying_shares=r[23],
                underlying_value=r[24],
                footnotes=footnotes,
            )
        )
    return rows
