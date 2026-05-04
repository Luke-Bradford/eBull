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

import json
import logging
import re
import xml.etree.ElementTree as ET  # noqa: S405 — Form 4 source is SEC EDGAR, trusted.
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from app.providers.concurrent_fetch import fetch_document_texts
from app.services import raw_filings
from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
)

_PARSER_VERSION_FORM4 = "form4-v1"

logger = logging.getLogger(__name__)

# Bump whenever parse_form_4_xml shape changes so the ingester can
# re-parse older filings under the new parser without having to
# re-fetch the universe of Form 4 XML from SEC.
_PARSER_VERSION = 2

# Backfill horizon for Form 4 ingestion. eBull's posture is long-
# horizon (months-to-years holding periods), and the signal carried
# by insider Form 4 trades decays sharply with age — the 2026-05
# backlog audit found 1.02M un-ingested filings older than 2y vs
# 221k under 2y. Draining the historical tail at 500/hr would burn
# ~6 weeks of SEC bandwidth on filings that never reach the live
# trading model. A 5-year floor keeps recent insider activity, the
# COVID era, and the rate-cycle pivot, while skipping pre-2021
# noise. Bump this if the operator decides to widen the window.
INSIDER_FORM4_BACKFILL_FLOOR_YEARS: int = 5


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


# ---------------------------------------------------------------------
# XML parser
# ---------------------------------------------------------------------


# Form 4 XMLs frequently declare a default namespace on
# ``<ownershipDocument xmlns="...">`` that makes every subsequent
# namespace-blind ``.find()`` miss. Regex pre-strip keeps the rest
# of the parser simple without threading ``{ns}`` braces everywhere.
_XMLNS_RE = re.compile(r'\sxmlns="[^"]*"')


def parse_form_4_xml(raw_xml: str) -> ParsedFiling | None:
    """Extract everything structural from a Form 4 primary document.

    Returns ``None`` when:

    - Input is empty or fails to parse as XML.
    - Document root is not ``ownershipDocument`` (URL rot — operator
      pointed us at a cover page or a wrong document).
    - Zero transactions across both the non-derivative and derivative
      tables. A filer-only filing is not a useful observation; drop it
      rather than insert a phantom record.

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
    """
    if not raw_xml:
        return None
    cleaned_xml = _XMLNS_RE.sub("", raw_xml)
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
    cleaned_xml = _XMLNS_RE.sub("", raw_xml)
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


def upsert_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: ParsedFiling,
    is_rewash: bool = False,
) -> None:
    """Insert/refresh the filing header + filer dim + footnote bodies +
    transaction rows for one accession.

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
    filing as "fetched today")."""
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
                instrument_id,
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
                    footnote_refs
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
                    %s
                )
                ON CONFLICT (accession_number, txn_row_num) DO UPDATE SET
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
                    footnote_refs             = EXCLUDED.footnote_refs
                """,
                (
                    instrument_id,
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
                ),
            )

    # Write-through observations + refresh _current (#888 / spec §"Eliminate
    # periodic re-scan jobs"). Replaces the legacy nightly
    # ownership_observations_sync read-from-typed-tables path with an
    # inline call so the operator-visible rollup reflects the new filing
    # without waiting for the next sync cycle.
    _record_form4_observations_for_filing(
        conn,
        instrument_id=instrument_id,
        accession_number=accession_number,
        primary_document_url=primary_document_url,
        parsed=parsed,
    )
    refresh_insiders_current(conn, instrument_id=instrument_id)


def _record_form4_observations_for_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: ParsedFiling,
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
        group wins (``txn_date DESC, txn_row_num DESC``).
      - ``ownership_nature``: ``'indirect'`` when direct_indirect='I',
        else ``'direct'``.

    The two-axis identity is preserved at the observations table — a
    filer's direct + indirect splits produce SEPARATE observation
    rows.
    """
    # Build the latest-per-group map
    LatestKey = tuple[str | None, str | None]  # (filer_cik, direct_indirect)
    latest: dict[LatestKey, ParsedTransaction] = {}
    for txn in parsed.transactions:
        if txn.is_derivative:
            continue
        if txn.post_transaction_shares is None:
            continue
        if txn.txn_date is None:
            continue
        key: LatestKey = (txn.filer_cik, txn.direct_indirect)
        prior = latest.get(key)
        if prior is None:
            latest[key] = txn
            continue
        # Pick later txn_date; tie-break on txn_row_num
        if (txn.txn_date, txn.txn_row_num) > ((prior.txn_date or date.min), prior.txn_row_num):
            latest[key] = txn

    if not latest:
        return

    run_id = uuid4()
    for (filer_cik, direct_indirect), txn in latest.items():
        if txn.txn_date is None or txn.post_transaction_shares is None:
            # Belt-and-braces — the filter above already excludes these.
            continue
        holder_name = _primary_filer_name(parsed, filer_cik)
        if not holder_name and not filer_cik:
            # Skip rows with no identity at all.
            continue
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
            filed_at=datetime.combine(txn.txn_date, datetime.min.time(), tzinfo=UTC),
            period_start=None,
            period_end=txn.txn_date,
            ingest_run_id=run_id,
            shares=Decimal(txn.post_transaction_shares),
        )


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


_TOMBSTONE_DOC_TYPE = "4"  # keep shape legal even for tombstones


def _write_tombstone(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
) -> None:
    """Mark an accession as unfetchable / unparseable at the filing
    level so the next ingester pass skips it.

    Tombstones carry no ``insider_filers`` / ``insider_transactions``
    children. The reader excludes them via ``is_tombstone = FALSE``
    in the summary query. A re-parse that succeeds for the same
    accession flips the row back to live via the ON CONFLICT DO
    UPDATE branch of :func:`upsert_filing`."""
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
                _TOMBSTONE_DOC_TYPE,
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
      ``insider_filings`` row.
    - Tombstone on fetch error / parse miss.
    - Caller's fetch URL runs through :func:`_canonical_form_4_url` to
      strip XSL-rendering segments.
    """
    conn.commit()
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
              AND fe.instrument_id = %s
              AND fil.accession_number IS NULL
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %s
            """,
            (instrument_id, limit),
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
    4. ``fe.filing_date >= NOW() - INTERVAL '<floor> years'`` — see
       :data:`INSIDER_FORM4_BACKFILL_FLOOR_YEARS`. Historical Form 4s
       outside the floor are skipped permanently; the operator can
       widen the floor if archaeology becomes a need.
    5. Ordered by filing_date DESC so fresh filings always get budget.

    Bounded per run (``limit=500``) to match expected daily Form 4
    volume across the universe.
    """
    conn.commit()

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
              AND fil.accession_number IS NULL
              AND fe.filing_date >= NOW() - make_interval(years => %(floor_years)s)
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %(limit)s
            """,
            {"limit": limit, "floor_years": INSIDER_FORM4_BACKFILL_FLOOR_YEARS},
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

    # Pre-fetch every URL concurrently. ``bodies[url]`` is the XML
    # text on success, ``None`` on 404 / fetch error / per-future
    # exception (the helper traps + logs each).
    bodies = fetch_document_texts(fetcher, (url for _, _, url in candidates))

    for instrument_id, accession, url in candidates:
        xml = bodies.get(url)
        if xml is None:
            # Codex pre-flight: per-future exceptions logged
            # generically by the helper. Re-emit at this scope with
            # ``accession`` so operators can grep failures by
            # filing identifier (matches the pre-#726 log shape).
            logger.warning(
                "ingest_insider_transactions: fetch failed accession=%s url=%s",
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
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_insider_transactions: upsert failed accession=%s",
                accession,
                exc_info=True,
            )
            continue

        filings_parsed += 1
        rows_inserted += len(parsed.transactions)

    return IngestResult(
        filings_scanned=len(candidates),
        filings_parsed=filings_parsed,
        rows_inserted=rows_inserted,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
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

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.instrument_id, COUNT(*) AS unfetched
            FROM filing_events fe
            LEFT JOIN insider_filings fil ON fil.accession_number = fe.provider_filing_id
            WHERE fe.provider = 'sec'
              AND fe.filing_type IN ('4', '4/A')
              AND fe.primary_document_url IS NOT NULL
              AND fil.accession_number IS NULL
              AND fe.filing_date >= NOW() - make_interval(years => %(floor_years)s)
            GROUP BY fe.instrument_id
            ORDER BY unfetched DESC
            LIMIT %(limit)s
            """,
            {"limit": instruments_per_tick, "floor_years": INSIDER_FORM4_BACKFILL_FLOOR_YEARS},
        )
        targets = [(int(r[0]), int(r[1])) for r in cur.fetchall()]
    conn.commit()

    totals = {
        "instruments_processed": 0,
        "filings_parsed": 0,
        "rows_inserted": 0,
        "fetch_errors": 0,
        "parse_misses": 0,
    }

    for instrument_id, _unfetched in targets:
        candidates: list[tuple[int, str, str]] = []
        with conn.cursor() as cur:
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
                  AND fe.instrument_id = %s
                  AND fil.accession_number IS NULL
                ORDER BY fe.filing_date ASC, fe.filing_event_id ASC
                LIMIT %s
                """,
                (instrument_id, per_instrument_limit),
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
