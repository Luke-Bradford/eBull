"""SEC N-PORT mutual-fund holdings ingester (#917 — Phase 3 PR1).

Walks each fund-filer CIK's submissions index, ingests every pending
NPORT-P / NPORT-P/A accession into ``ownership_funds_observations``,
and refreshes ``ownership_funds_current`` per touched issuer.

This is the public-quarterly slice of N-PORT (Form NPORT-P): SEC
publishes one quarterly snapshot per fund series, 60 days after the
period_end. The monthly-internal NPORT-MFP filings remain
confidential and are not in scope.

Spec:
docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md
(Phase 3, §"Source matrix" + §"Per-category natural keys").

## Parser posture — EdgarTools FundReport wrapper (#932)

Wraps ``edgar.funds.reports.FundReport.parse_fund_xml`` (lazy-imported
via :func:`_edgar_fund_report` per the #925 13F-HR drop-in pattern).
Preserves the public :class:`NPortFiling` / :class:`NPortHolding`
dataclass surface so the ingester body is unchanged.

The previous stdlib-``xml.etree.ElementTree`` parser was replaced by
this wrapper after the #932 spike (see
``docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md``)
confirmed FEASIBLE for the probed real-SEC NPORT-P shape. The wrapper
catches every EdgarTools failure mode (structural-block dereferences,
internal ``pydantic.ValidationError`` on missing required Decimal
fields, dict-shape drift on pin bump) and converts to
:class:`NPortParseError` so accession-level tombstones fire via
:func:`_ingest_single_accession`.

Codex pre-impl review (2026-05-05) findings folded in:

* #1 — both ``NPORT-P`` / ``NPORT-P/A`` (current SEC spelling) and
  ``N-PORT`` / ``N-PORT/A`` (legacy spelling) accepted by the
  submissions-index walker.
* #2 — filings missing a ``seriesId`` raise ``NPortMissingSeriesError``
  and tombstone as ``failed`` instead of synthesising a collision-prone
  fallback identity.
* #3 + #4 — equity-common + Long-payoff guards live in
  ``record_fund_observation`` (not just here) so test seeders inherit
  the guards.
* #5 — refresh ordering by ``filed_at DESC`` so amendments win.
* #6 — parser is pure XML-in / dataclass-out; no network calls
  during parse. Test in ``tests/services/test_n_port_ingest.py``
  proves the offline guarantee by raising on every HTTP client.
* #11 — ingest log measures parsed accessions, not row dimension.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol
from uuid import uuid4

import psycopg

from app.services.bootstrap_state import (
    resolve_progress_context,
    set_stage_processed,
    set_stage_target,
)
from app.services.fundamentals import finish_ingestion_run, start_ingestion_run
from app.services.ownership_observations import (
    record_fund_observation,
    refresh_funds_current,
    upsert_sec_fund_series,
)

logger = logging.getLogger(__name__)


# Bumped when ``parse_n_port_payload`` semantics change in a way that
# affects what lands in ``ownership_funds_observations``. Re-wash
# workflows compare against this constant and reset
# ``filing_raw_documents.parser_version`` mismatches back to ``pending``
# in the manifest worker (#869). The bump propagates to the manifest
# adapter (`app/services/manifest_parsers/sec_n_port.py`) via direct
# symbol import.
_PARSER_VERSION_NPORT = "nport-v2-edgartools"


# Both spellings appear in the SEC submissions API. The current
# canonical spelling is ``NPORT-P`` (post-2018); ``N-PORT`` is the
# pre-2018 legacy spelling that still surfaces on some amendments.
# Codex pre-impl review #1.
_NPORT_FORM_TYPES: frozenset[str] = frozenset(
    {
        "NPORT-P",
        "NPORT-P/A",
        "N-PORT",
        "N-PORT/A",
    }
)


# ---------------------------------------------------------------------------
# N-PORT 8-quarter retention cap (#1233 PR7, spec §4.6)
# ---------------------------------------------------------------------------

# NPORT-P period_of_report is a calendar MONTH end (END of the third
# month of the fund's fiscal quarter; funds have their own fiscal
# calendars so the month can be any of Jan-Dec). Spec §4.6 caps depth
# at 8 fiscal-quarter snapshots per fund. Anchoring to month
# boundaries (not a floating ``today - 760d`` window, not the
# calendar-quarter anchor used by 13F-HR §4.5) admits 24 consecutive
# completed month-ends, which by the mod-3 congruence-class argument
# contains exactly 8 month-ends for every fiscal-Q choice.
#
# Ingest-side cap only — existing rows are untouched until the
# operator-driven pre-wipe + clean re-run (spec §6.3). Cutoff is
# computed in Python and passed as a ``date`` everywhere, NOT as
# ``NOW() - make_interval(...)`` which carries DB session-timezone
# ambiguity. UTC anchor — #1010 Codex 2 lesson: ``date.today()``
# returns local TZ, drifts the cutoff by ±1 day on non-UTC dev hosts.
NPORT_RETENTION_QUARTERS: int = 8


def _last_day_of_month(year: int, month: int) -> date:
    """Return the canonical month-end ``date`` for (year, month).

    Local helper — month-end arithmetic is a one-off here; importing
    dateutil for ``relativedelta`` would be casual dependency creep
    (CLAUDE.md non-negotiables).
    """
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def n_port_retention_cutoff(now: datetime | None = None) -> date:
    """Earliest ``period_of_report`` accepted for NPORT-P / NPORT-P/A.

    Returns the calendar month-end exactly ``NPORT_RETENTION_QUARTERS
    * 3`` = 24 months before today (i.e. the month-end of the month
    24 months ago). Boundary inclusive → the admitted set is the 24
    consecutive completed month-ends ending at ``today.month - 1``,
    which by the mod-3 congruence-class argument contains exactly 8
    month-ends for every fiscal-Q congruence class. So every fund
    sees exactly 8 of its fiscal-Q snapshots regardless of its
    fiscal-year alignment.

    ``now`` must be a tz-aware datetime; the helper normalises to UTC
    before taking ``.date()`` so the cutoff doesn't drift on non-UTC
    callers (#1010 / PR6 Codex lesson).
    """
    if now is None:
        now = datetime.now(tz=UTC)
    if now.tzinfo is None:
        raise ValueError(
            "n_port_retention_cutoff: ``now`` must be a tz-aware datetime; "
            "naive datetimes would honour the caller's local TZ and drift the cutoff."
        )
    today = now.astimezone(UTC).date()
    # Target month = (today.year, today.month - 24) with year-wrap.
    # ``today.month - 24`` is the calendar month exactly 24 months
    # before today's month; its month-end is the inclusive lower bound
    # of the admitted window.
    months_back = NPORT_RETENTION_QUARTERS * 3  # 24
    target_y = today.year
    target_m = today.month - months_back
    while target_m <= 0:
        target_m += 12
        target_y -= 1
    return _last_day_of_month(target_y, target_m)


def n_port_within_retention(
    period_of_report: date | None,
    now: datetime | None = None,
) -> bool:
    """Boundary check used by every N-PORT writer chokepoint.

    Returns True iff ``period_of_report >= n_port_retention_cutoff(now)``.
    A None ``period_of_report`` returns False — defensive: an
    accession we couldn't tag with a month end is unsafe to admit.
    """
    if period_of_report is None:
        return False
    return period_of_report >= n_port_retention_cutoff(now)


# ---------------------------------------------------------------------------
# Provider contract
# ---------------------------------------------------------------------------


class SecArchiveFetcher(Protocol):
    """Subset of the SEC EDGAR provider this ingester relies on.

    Same shape as :class:`app.services.institutional_holdings.SecArchiveFetcher`;
    duplicated here to keep the N-PORT service decoupled from the 13F
    service. Production binding is :class:`app.providers.implementations.
    sec_edgar.SecFilingsProvider`.
    """

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AccessionRef:
    """One discovered NPORT-P accession to ingest."""

    accession_number: str
    filing_type: str
    period_of_report: date | None
    filed_at: datetime | None


@dataclass(frozen=True)
class NPortHolding:
    """One per-issuer holding row from an NPORT-P invstOrSec entry.

    Captures both the canonical share-position fields (``cusip``,
    ``shares``, ``value_usd``) and the SEC categorisation fields
    (``payoff_profile``, ``asset_category``, ``issuer_category``,
    ``units``) needed by the equity-common-Long write-side guard.

    ``units`` (Codex pre-push review #3): ``'NS'`` = number of shares,
    ``'PA'`` = principal amount (debt). The ingester rejects any
    non-``'NS'`` units even when ``asset_category='EC'`` — a Long EC
    convertible-bond holding reports a balance in PA and would be
    silently treated as shares without this guard.
    """

    cusip: str
    issuer_name: str
    shares: Decimal
    value_usd: Decimal | None
    payoff_profile: str  # raw enum: 'Long' | 'Short' | other
    asset_category: str  # raw assetCat: 'EC' | 'EP' | 'DBT' | 'DV' | etc.
    issuer_category: str  # raw issuerCat: informational only
    units: str  # raw units: 'NS' | 'PA' | 'OU' | etc.


@dataclass(frozen=True)
class NPortFiling:
    """Header + holdings extracted from one NPORT-P primary doc.

    ``filed_at`` is ``None`` when the primary doc lacks an explicit
    header timestamp — the ingester falls back to the submissions-index
    ``filingDate`` for that accession. Codex pre-push review (2026-05-05)
    finding #1: never silently default to ``period_end`` midnight inside
    the parser, because two filings with the same period (NPORT-P +
    NPORT-P/A) would then carry identical ``filed_at`` values and the
    ``_current`` refresh tie-break would pick the wrong one.
    """

    filer_cik: str
    series_id: str
    series_name: str
    period_end: date
    filed_at: datetime | None
    holdings: tuple[NPortHolding, ...]


@dataclass(frozen=True)
class IngestSummary:
    """Per-CIK rollup of one ingest pass."""

    filer_cik: str
    accessions_seen: int
    accessions_ingested: int
    accessions_failed: int
    holdings_inserted: int
    holdings_skipped_no_cusip: int
    holdings_skipped_non_equity: int
    holdings_skipped_short: int
    holdings_skipped_non_share_units: int
    holdings_skipped_zero_shares: int
    first_error: str | None = None


@dataclass(frozen=True)
class _AccessionOutcome:
    """Internal: per-accession ingest outcome."""

    status: str
    holdings_inserted: int
    holdings_skipped_no_cusip: int
    holdings_skipped_non_equity: int
    holdings_skipped_short: int
    holdings_skipped_non_share_units: int
    holdings_skipped_zero_shares: int
    error: str | None
    series_id: str | None
    period_of_report: date | None

    @property
    def ingested(self) -> bool:
        return self.status in ("success", "partial")


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class NPortParseError(ValueError):
    """Malformed N-PORT XML; accession tombstoned as ``failed``."""


class NPortMissingSeriesError(NPortParseError):
    """N-PORT primary doc had no ``seriesId``. Codex pre-impl review #2:
    we refuse to synthesise a fallback identity — colliding rows would
    produce silent over-counting in the rollup."""


# ---------------------------------------------------------------------------
# URL builders (mirror institutional_holdings.py)
# ---------------------------------------------------------------------------


_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{filename}"


def _zero_pad_cik(cik: str | int) -> str:
    return str(int(str(cik).strip())).zfill(10)


def _accession_no_dashes(accession_number: str) -> str:
    return accession_number.replace("-", "")


def _submissions_url(cik: str) -> str:
    return _SUBMISSIONS_URL.format(cik=_zero_pad_cik(cik))


def _archive_file_url(cik: str, accession_number: str, filename: str) -> str:
    return _ARCHIVE_URL.format(
        cik_int=int(_zero_pad_cik(cik)),
        accn_no_dashes=_accession_no_dashes(accession_number),
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Submissions index walker
# ---------------------------------------------------------------------------


def parse_submissions_index(
    payload: str,
    *,
    min_period_of_report: date | None = None,
) -> list[AccessionRef]:
    """Walk ``data.sec.gov/submissions/CIK{cik}.json`` and emit one
    :class:`AccessionRef` per NPORT-P / NPORT-P/A row.

    Older-history shards via ``filings.files`` are out of scope here —
    the ``recent`` array holds the most recent ~1,000 filings per CIK
    which covers ≥ 12 quarters of monthly N-PORT for any actively
    filing fund family.

    PR7 #1233 §4.6 — applies the intrinsic 24-month cap as the
    effective floor; any caller-provided ``min_period_of_report`` can
    RAISE the floor but never lower it. Caller passing ``None`` still
    gets the cap.
    """
    try:
        data: dict[str, Any] = json.loads(payload)
    except json.JSONDecodeError:
        logger.warning("n_port submissions index payload is not valid JSON")
        return []

    filings = data.get("filings", {})
    recent = filings.get("recent", {})
    accessions = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])

    intrinsic_floor = n_port_retention_cutoff()
    if min_period_of_report is None:
        effective_floor = intrinsic_floor
    else:
        effective_floor = max(min_period_of_report, intrinsic_floor)

    out: list[AccessionRef] = []
    for i, accession in enumerate(accessions):
        if i >= len(forms):
            break
        form = str(forms[i])
        if form not in _NPORT_FORM_TYPES:
            continue
        filed_at = _safe_iso_datetime(filing_dates[i] if i < len(filing_dates) else "")
        period = _safe_iso_date(report_dates[i] if i < len(report_dates) else "")
        if period is not None and period < effective_floor:
            continue
        out.append(
            AccessionRef(
                accession_number=str(accession),
                filing_type=form,
                period_of_report=period,
                filed_at=filed_at,
            )
        )
    return out


def _safe_iso_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _safe_iso_datetime(text: str | None) -> datetime | None:
    parsed = _safe_iso_date(text)
    if parsed is None:
        return None
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)


# ---------------------------------------------------------------------------
# N-PORT XML parser — EdgarTools FundReport wrapper (#932)
# ---------------------------------------------------------------------------


def parse_n_port_payload(xml: str) -> NPortFiling:
    """Parse an NPORT-P primary doc XML into an :class:`NPortFiling`.

    Wraps ``edgar.funds.reports.FundReport.parse_fund_xml`` (#932,
    superseding the stdlib-ET parser from #917). EdgarTools is lazy-
    imported at first call so module import stays side-effect-free
    (#925 pattern; mirrors ``app/providers/implementations/sec_13f.py``
    L76-80).

    Catches every EdgarTools failure mode (structural-block dereferences,
    internal Pydantic ``ValidationError`` on missing required Decimal
    fields, dict-shape drift) and converts to :class:`NPortParseError`
    so ``_ingest_single_accession``'s tombstone path fires.

    Preserves all six #917 Codex pre-impl invariants:
      * ``period_end`` mandatory → :class:`NPortParseError` if missing.
      * ``series_id`` mandatory → :class:`NPortMissingSeriesError`
        (distinct from generic :class:`NPortParseError`).
      * ``cik`` mandatory, with ``regCik`` → header
        ``issuer_credentials.cik`` fallback.
      * ``filed_at`` returned as ``None`` from the parser — the ingester
        layers in submissions-index ``filingDate`` before any midnight
        fallback. EdgarTools' ``parse_fund_xml`` does not surface a
        header-level filedAt field.
      * ``units`` passthrough (strip-only; the ingester rejects non-NS).
      * ``balance`` ``None`` → drop row.

    EdgarTools field-name trap: ``general_info.fiscal_year_end`` stores
    the ``repPdEnd`` text (period end), NOT the fund's fiscal-year-end.
    Documented at the golden-replay test.

    Pure XML-in / dataclass-out. No network calls, no DB access.
    """
    fund_report = _edgar_fund_report()
    pydantic_validation_error = _pydantic_validation_error()
    xml_syntax_error = _lxml_syntax_error()

    # Catch scope covers BOTH the EdgarTools parser call AND the
    # post-parse normalisation. Any KeyError (dict-shape drift on pin
    # bump), AttributeError (missing typed object attribute), or
    # InvalidOperation / ValueError that fires during normalisation
    # must surface as NPortParseError so the tombstone path fires.
    # lxml.etree.XMLSyntaxError covers the edge case where both the
    # primary lxml parse AND EdgarTools' recover=True fallback fail
    # (e.g. empty payload, null-byte body): EdgarTools re-raises the
    # raw XMLSyntaxError without classifying it. Codex 2 round 1.
    # NPortMissingSeriesError / NPortParseError are re-raised
    # explicitly to preserve their dedicated tombstone paths.
    try:
        parsed = fund_report.parse_fund_xml(xml)

        general_info = parsed["general_info"]
        cik_text = (general_info.cik or "").strip() or _header_issuer_cik(parsed.get("header"))
        series_id_raw = (general_info.series_id or "").strip()
        period_end_text = (general_info.fiscal_year_end or "").strip() or None
        series_name = (general_info.series_name or "").strip()

        if not cik_text:
            raise NPortParseError("NPORT-P: missing regCik / header issuer_credentials.cik")
        if not series_id_raw:
            # Codex #917 pre-impl review #2 — refuse to synthesise.
            raise NPortMissingSeriesError(
                "NPORT-P: missing seriesId in genInfo header; refusing to "
                "synthesise an identity. Filing tombstoned for operator review."
            )
        if not period_end_text:
            raise NPortParseError("NPORT-P: missing repPdEnd in genInfo header")
        period_end = _safe_iso_date(period_end_text)
        if period_end is None:
            raise NPortParseError(f"NPORT-P: malformed repPdEnd={period_end_text!r}")

        # Codex #917 pre-push review #1: do NOT default filed_at to
        # period_end midnight inside the parser. Two filings sharing a
        # period (NPORT-P + NPORT-P/A) would then carry identical
        # filed_at values and the _current refresh tie-break would pick
        # the wrong one. The ingester layers in the submissions-index
        # filingDate before any midnight fallback.
        filed_at: datetime | None = None

        holdings: list[NPortHolding] = []
        for investment in parsed["investments"]:
            balance = investment.balance
            if balance is None:
                # No balance = unparseable holding; parser-level drop.
                continue
            # Strip + upper categorical fields. The downstream ingester
            # guards (units != "NS", payoff_profile != "Long",
            # asset_category != "EC") are exact-equality so a stray
            # leading/trailing whitespace from a future EdgarTools
            # whitespace-preservation change would mis-drop valid rows.
            holdings.append(
                NPortHolding(
                    cusip=(investment.cusip or "").strip().upper(),
                    issuer_name=(investment.name or "").strip(),
                    shares=balance,
                    value_usd=investment.value_usd,
                    payoff_profile=(investment.payoff_profile or "").strip(),
                    asset_category=(investment.asset_category or "").strip(),
                    issuer_category=(investment.issuer_category or "").strip(),
                    units=(investment.units or "").strip(),
                )
            )

        return NPortFiling(
            filer_cik=_zero_pad_cik(cik_text),
            series_id=series_id_raw,
            series_name=series_name,
            period_end=period_end,
            filed_at=filed_at,
            holdings=tuple(holdings),
        )
    except NPortMissingSeriesError:
        # Distinct from generic NPortParseError tombstone path.
        raise
    except NPortParseError:
        # Already-classified parser error; preserve verbatim.
        raise
    except (
        AttributeError,
        KeyError,
        InvalidOperation,
        ValueError,
        TypeError,
        pydantic_validation_error,
        xml_syntax_error,
    ) as exc:
        raise NPortParseError(f"NPORT-P EdgarTools parse failed: {exc}") from exc


def _edgar_fund_report() -> Any:
    """Lazy import: defer EdgarTools' filesystem-cache mkdir
    (``~/.edgar/_tcache``) until first parse call. Mirrors the lazy
    factory at ``app/providers/implementations/sec_13f.py``:76-80."""
    from edgar.funds.reports import FundReport

    return FundReport


def _pydantic_validation_error() -> type[Exception]:
    """Lazy import: ``pydantic.ValidationError`` for the wrapper's
    catch list. Pydantic comes in transitively via edgartools but we
    hold our own reference to its ValidationError type to keep the
    catch clause explicit. Lazy to keep module import side-effect-free.
    """
    from pydantic import ValidationError

    return ValidationError


def _lxml_syntax_error() -> type[Exception]:
    """Lazy import: ``lxml.etree.XMLSyntaxError`` for the wrapper's
    catch list. EdgarTools' ``parse_fund_xml`` raw-parses with
    ``etree.fromstring`` and falls back to ``recover=True`` on
    ``XMLSyntaxError``; if the recover-mode parse ALSO fails (empty
    body, null-byte body, etc.), the raw exception escapes without
    classification. Lazy to keep module import side-effect-free.
    """
    from lxml.etree import XMLSyntaxError

    return XMLSyntaxError


def _header_issuer_cik(header: Any) -> str | None:
    """Best-effort dereference of
    ``parsed['header'].filer_info.issuer_credentials.cik``. ``None`` if
    any intermediate is ``None`` or missing.

    Mirrors the ``regCik`` → header ``cik`` fallback that the pre-#932
    stdlib parser performed via
    ``_find_text(root, local_name='cik')``.
    """
    if header is None:
        return None
    try:
        cik = header.filer_info.issuer_credentials.cik
    except AttributeError:
        return None
    if not cik:
        return None
    return cik.strip() or None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _existing_accessions_for_fund_filer(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
) -> set[str]:
    """Return every accession previously attempted for this CIK.

    Reads from ``n_port_ingest_log`` regardless of status — same
    contract as ``_existing_accessions_for_filer`` in the 13F path."""
    cur = conn.execute(
        "SELECT accession_number FROM n_port_ingest_log WHERE filer_cik = %(cik)s",
        {"cik": filer_cik},
    )
    return {row[0] for row in cur.fetchall()}


def _record_ingest_attempt(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
    accession_number: str,
    fund_series_id: str | None,
    period_of_report: date | None,
    status: str,
    holdings_inserted: int = 0,
    holdings_skipped: int = 0,
    error: str | None = None,
) -> None:
    """Idempotent upsert into ``n_port_ingest_log``. Re-record on
    follow-up runs so a 'failed' or 'partial' can promote to
    'success' once the underlying issue clears (parser bump, CUSIP
    backfill landing, etc.)."""
    conn.execute(
        """
        INSERT INTO n_port_ingest_log (
            accession_number, filer_cik, fund_series_id, period_of_report,
            status, holdings_inserted, holdings_skipped, error
        ) VALUES (
            %(accession)s, %(cik)s, %(series)s, %(period)s,
            %(status)s, %(inserted)s, %(skipped)s, %(error)s
        )
        ON CONFLICT (accession_number) DO UPDATE SET
            fund_series_id = EXCLUDED.fund_series_id,
            period_of_report = EXCLUDED.period_of_report,
            status = EXCLUDED.status,
            holdings_inserted = EXCLUDED.holdings_inserted,
            holdings_skipped = EXCLUDED.holdings_skipped,
            error = EXCLUDED.error,
            fetched_at = NOW()
        """,
        {
            "accession": accession_number,
            "cik": filer_cik,
            "series": fund_series_id,
            "period": period_of_report,
            "status": status,
            "inserted": holdings_inserted,
            "skipped": holdings_skipped,
            "error": error,
        },
    )


def _resolve_cusip_to_instrument_id(
    conn: psycopg.Connection[tuple],
    cusip: str,
) -> int | None:
    """Resolve CUSIP via ``external_identifiers``. Same contract as
    the 13F path."""
    if not cusip or len(cusip.strip()) != 9:
        return None
    cur = conn.execute(
        """
        SELECT instrument_id
        FROM external_identifiers
        WHERE provider = 'sec'
          AND identifier_type = 'cusip'
          AND identifier_value = %(cusip)s
        ORDER BY is_primary DESC, external_identifier_id ASC
        LIMIT 1
        """,
        {"cusip": cusip.strip().upper()},
    )
    row = cur.fetchone()
    return int(row[0]) if row is not None else None


# ---------------------------------------------------------------------------
# Public ingest entry points
# ---------------------------------------------------------------------------


def ingest_fund_n_port(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    filer_cik: str,
    min_period_of_report: date | None = None,
) -> IngestSummary:
    """Fetch + parse + upsert every pending NPORT-P for one fund-filer
    CIK. Per-accession failures are isolated (logged + tombstoned) so
    a single malformed accession does not abort the filer batch.

    PR7 #1233 §4.6 — ``min_period_of_report`` plumbs through to
    ``parse_submissions_index``; ``None`` (default) yields the
    intrinsic 24-month cap. A caller can tighten the floor but never
    loosen it.
    """
    cik = _zero_pad_cik(filer_cik)
    summary = _MutableSummary(cik=cik)

    submissions_payload = sec.fetch_document_text(_submissions_url(cik))
    if submissions_payload is None:
        logger.info("n_port ingest: submissions JSON 404/error for cik=%s", cik)
        return summary.to_immutable()

    pending = parse_submissions_index(
        submissions_payload,
        min_period_of_report=min_period_of_report,
    )
    summary.accessions_seen = len(pending)
    if not pending:
        return summary.to_immutable()

    already = _existing_accessions_for_fund_filer(conn, filer_cik=cik)

    for ref in pending:
        if ref.accession_number in already:
            continue
        outcome = _ingest_single_accession(conn, sec, filer_cik=cik, ref=ref)
        _record_ingest_attempt(
            conn,
            filer_cik=cik,
            accession_number=ref.accession_number,
            fund_series_id=outcome.series_id,
            period_of_report=outcome.period_of_report or ref.period_of_report,
            status=outcome.status,
            holdings_inserted=outcome.holdings_inserted,
            holdings_skipped=outcome.holdings_skipped_no_cusip
            + outcome.holdings_skipped_non_equity
            + outcome.holdings_skipped_short
            + outcome.holdings_skipped_non_share_units
            + outcome.holdings_skipped_zero_shares,
            error=outcome.error,
        )
        if outcome.ingested:
            summary.accessions_ingested += 1
        else:
            summary.accessions_failed += 1
            if outcome.error and summary.first_error is None:
                summary.first_error = f"{ref.accession_number}: {outcome.error}"
        summary.holdings_inserted += outcome.holdings_inserted
        summary.holdings_skipped_no_cusip += outcome.holdings_skipped_no_cusip
        summary.holdings_skipped_non_equity += outcome.holdings_skipped_non_equity
        summary.holdings_skipped_short += outcome.holdings_skipped_short
        summary.holdings_skipped_non_share_units += outcome.holdings_skipped_non_share_units
        summary.holdings_skipped_zero_shares += outcome.holdings_skipped_zero_shares

    return summary.to_immutable()


def ingest_all_fund_filers(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    ciks: list[str],
    deadline_seconds: float | None = None,
    source_label: str = "sec_n_port_ingest",
    min_period_of_report: date | None = None,
) -> list[IngestSummary]:
    """Walk a list of fund-filer CIKs and ingest each one's pending
    NPORT-Ps. Per-filer crashes isolated; a soft deadline allows the
    sweep to be interrupted cleanly with the next run resuming the
    tail (already-attempted accessions tombstoned in
    ``n_port_ingest_log``)."""
    if not ciks:
        logger.info("n_port ingest: no fund-filer CIKs to ingest; nothing to do")
        return []

    deadline_ts: float | None
    if deadline_seconds is None:
        deadline_ts = None
    else:
        deadline_ts = time.monotonic() + deadline_seconds

    run_id = start_ingestion_run(
        conn,
        source=source_label,
        endpoint="/Archives/edgar/data/{cik}/{accession}/",
        instrument_count=len(ciks),
    )
    conn.commit()

    rows_upserted = 0
    rows_skipped = 0
    summaries: list[IngestSummary] = []
    crash_error: str | None = None
    accession_failures = 0
    first_accession_error: str | None = None
    deadline_hit = False
    filers_attempted = 0

    # #1273 PR2 — long-pole stage instrumentation (S23). Same shape as
    # S22's ingest_all_active_filers; manual-fire / scheduled / test
    # paths get progress_ctx=None and skip every helper call.
    progress_ctx = resolve_progress_context()
    if progress_ctx is not None:
        fingerprint = (
            f"min_period_of_report="
            f"{min_period_of_report.isoformat() if min_period_of_report else 'none'};"
            f"deadline_seconds={deadline_seconds if deadline_seconds is not None else 'none'};"
            f"directory=sec_nport_filer_directory"
        )
        set_stage_target(
            run_id=progress_ctx.run_id,
            stage_key=progress_ctx.stage_key,
            target_count=len(ciks),
            cohort_fingerprint=fingerprint,
        )
    _emit_every_n = max(1, len(ciks) // 100)
    _last_progress_emit = time.monotonic()

    try:
        for cik in ciks:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                deadline_hit = True
                logger.info(
                    "n_port ingest: deadline reached after %d/%d filers",
                    filers_attempted,
                    len(ciks),
                )
                break
            filers_attempted += 1
            try:
                summary = ingest_fund_n_port(
                    conn,
                    sec,
                    filer_cik=cik,
                    min_period_of_report=min_period_of_report,
                )
            except Exception as exc:  # noqa: BLE001 — per-filer crash isolation
                logger.exception("n_port ingest: filer %s raised; continuing", cik)
                crash_error = f"{cik}: {exc}"
                conn.rollback()
                continue
            conn.commit()
            summaries.append(summary)
            rows_upserted += summary.holdings_inserted
            rows_skipped += (
                summary.holdings_skipped_no_cusip
                + summary.holdings_skipped_non_equity
                + summary.holdings_skipped_short
                + summary.holdings_skipped_non_share_units
                + summary.holdings_skipped_zero_shares
            )
            accession_failures += summary.accessions_failed
            if summary.first_error and first_accession_error is None:
                first_accession_error = f"{cik} {summary.first_error}"
            # #1273 PR2 — cadenced operator-progress emit.
            if progress_ctx is not None:
                _now = time.monotonic()
                if filers_attempted % _emit_every_n == 0 or _now - _last_progress_emit > 30:
                    set_stage_processed(
                        run_id=progress_ctx.run_id,
                        stage_key=progress_ctx.stage_key,
                        processed_count=filers_attempted,
                    )
                    _last_progress_emit = _now
    finally:
        # Status precedence mirrors institutional_holdings.ingest_all_active_filers:
        #   deadline beats crash-only; crash-only with summaries beats failed.
        if deadline_hit:
            status = "partial"
        elif crash_error and not summaries:
            status = "failed"
        elif crash_error or accession_failures > 0 or rows_skipped > 0:
            status = "partial"
        else:
            status = "success"
        error_parts: list[str] = []
        if crash_error:
            error_parts.append(f"crash: {crash_error}")
        if first_accession_error:
            error_parts.append(f"accession: {first_accession_error}")
        if rows_skipped > 0 and not error_parts:
            error_parts.append(f"{rows_skipped} holdings skipped (non-equity/short/non-share-units/zero-shares/cusip)")
        if deadline_hit:
            error_parts.append(f"deadline reached after {filers_attempted}/{len(ciks)} filers")
        finish_ingestion_run(
            conn,
            run_id=run_id,
            status=status,
            rows_upserted=rows_upserted,
            rows_skipped=rows_skipped,
            error="; ".join(error_parts) or None,
        )
        conn.commit()
        # #1273 PR2 — final operator-progress emit on every exit branch.
        if progress_ctx is not None:
            set_stage_processed(
                run_id=progress_ctx.run_id,
                stage_key=progress_ctx.stage_key,
                processed_count=filers_attempted,
            )

    return summaries


# ---------------------------------------------------------------------------
# Internal driver
# ---------------------------------------------------------------------------


@dataclass
class _MutableSummary:
    cik: str
    accessions_seen: int = 0
    accessions_ingested: int = 0
    accessions_failed: int = 0
    holdings_inserted: int = 0
    holdings_skipped_no_cusip: int = 0
    holdings_skipped_non_equity: int = 0
    holdings_skipped_short: int = 0
    holdings_skipped_non_share_units: int = 0
    holdings_skipped_zero_shares: int = 0
    first_error: str | None = None

    def to_immutable(self) -> IngestSummary:
        return IngestSummary(
            filer_cik=self.cik,
            accessions_seen=self.accessions_seen,
            accessions_ingested=self.accessions_ingested,
            accessions_failed=self.accessions_failed,
            holdings_inserted=self.holdings_inserted,
            holdings_skipped_no_cusip=self.holdings_skipped_no_cusip,
            holdings_skipped_non_equity=self.holdings_skipped_non_equity,
            holdings_skipped_short=self.holdings_skipped_short,
            holdings_skipped_non_share_units=self.holdings_skipped_non_share_units,
            holdings_skipped_zero_shares=self.holdings_skipped_zero_shares,
            first_error=self.first_error,
        )


def _ingest_single_accession(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    filer_cik: str,
    ref: AccessionRef,
) -> _AccessionOutcome:
    """Per-accession driver. Never raises; every fetch / parse failure
    resolves to an outcome with status='failed'."""
    # NPORT-P primary doc lives at the accession's primary_doc.xml.
    # SEC EDGAR convention: NPORT-P archives also expose an
    # ``primary_doc.xml`` even though the underlying XBRL document
    # may be named differently in the inline-XBRL container.
    primary_url = _archive_file_url(filer_cik, ref.accession_number, "primary_doc.xml")

    # Fetch + persist raw BEFORE parse — non-negotiable per the
    # prevention-log entry "Raw API payload must be persisted before
    # any parse / normalise step" (#1168).
    primary_xml = sec.fetch_document_text(primary_url)
    if primary_xml is None:
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            holdings_skipped_non_equity=0,
            holdings_skipped_short=0,
            holdings_skipped_non_share_units=0,
            holdings_skipped_zero_shares=0,
            error="primary_doc.xml fetch failed",
            series_id=None,
            period_of_report=ref.period_of_report,
        )

    from app.services import raw_filings

    raw_filings.store_raw(
        conn,
        accession_number=ref.accession_number,
        document_kind="nport_xml",
        payload=primary_xml,
        parser_version=_PARSER_VERSION_NPORT,
        source_url=primary_url,
    )
    # Commit the raw row before parse so a later parse failure that
    # propagates rollback can't drop the body. Same pattern as
    # institutional_holdings._ingest_single_accession (Codex review
    # #913 lesson).
    conn.commit()

    try:
        parsed = parse_n_port_payload(primary_xml)
    except NPortMissingSeriesError as exc:
        logger.warning(
            "n_port ingest: missing seriesId for cik=%s accession=%s — tombstoning",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            holdings_skipped_non_equity=0,
            holdings_skipped_short=0,
            holdings_skipped_non_share_units=0,
            holdings_skipped_zero_shares=0,
            error=str(exc),
            series_id=None,
            period_of_report=ref.period_of_report,
        )
    except NPortParseError as exc:
        logger.exception(
            "n_port ingest: parse failed for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            holdings_skipped_non_equity=0,
            holdings_skipped_short=0,
            holdings_skipped_non_share_units=0,
            holdings_skipped_zero_shares=0,
            error=f"parse failed: {exc}",
            series_id=None,
            period_of_report=ref.period_of_report,
        )

    # PR7 #1233 §4.6 — defensive post-parse gate. ``parse_submissions_index``
    # already skips accessions with a known pre-cap ``period_of_report``,
    # but submissions JSON may carry a NULL / malformed ``reportDate``
    # → leaks past the index-level gate and reaches here. Re-check
    # against ``parsed.period_end`` and short-circuit BEFORE the first
    # observation write so pre-cap accessions never touch
    # ``ownership_funds_observations`` / ``ownership_funds_current``.
    if not n_port_within_retention(parsed.period_end):
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            holdings_skipped_non_equity=0,
            holdings_skipped_short=0,
            holdings_skipped_non_share_units=0,
            holdings_skipped_zero_shares=0,
            error="retention floor",
            series_id=parsed.series_id,
            period_of_report=parsed.period_end,
        )

    # Update series reference table — UPSERT on every ingest so a
    # series rename or new filer assignment propagates. Done before
    # observation writes so refresh_funds_current can JOIN against
    # a populated row even if the observations refresh fails midway.
    upsert_sec_fund_series(
        conn,
        fund_series_id=parsed.series_id,
        fund_series_name=parsed.series_name,
        fund_filer_cik=parsed.filer_cik,
        last_seen_period_end=parsed.period_end,
    )

    inserted = 0
    skipped_no_cusip = 0
    skipped_non_equity = 0
    skipped_short = 0
    skipped_non_share_units = 0
    skipped_zero_shares = 0
    touched_instruments: set[int] = set()
    run_id = uuid4()
    # Codex pre-push review #1: prefer header ``filed_at`` → submissions
    # index ``filingDate`` → period_end midnight. The submissions index
    # is always present in the ``recent`` array, so amendments will
    # carry distinct ``filed_at`` values from the original even when
    # both lack header timestamps.
    if parsed.filed_at is not None:
        filed_at = parsed.filed_at
    elif ref.filed_at is not None:
        filed_at = ref.filed_at
    else:
        filed_at = datetime(parsed.period_end.year, parsed.period_end.month, parsed.period_end.day, tzinfo=UTC)

    for holding in parsed.holdings:
        # Apply the equity-common-Long write-side guards as drop
        # filters BEFORE the helper-side validation. The helper
        # ``record_fund_observation`` re-applies the same guards as
        # value-error raises; doing the drop counts here surfaces the
        # operator-visible counters per category. Order matters —
        # check non-equity / short / units before CUSIP / zero-shares
        # so the most specific category wins (a Short DBT row counts
        # as non_equity, not short).
        if holding.asset_category != "EC":
            skipped_non_equity += 1
            continue
        if holding.payoff_profile != "Long":
            skipped_short += 1
            continue
        # Codex pre-push review #3: guard against units other than
        # 'NS' (number of shares). A Long EC convertible-bond holding
        # reports balance in 'PA' (principal amount) and would
        # silently land as shares without this guard.
        if holding.units != "NS":
            skipped_non_share_units += 1
            continue
        # Codex pre-push review #4: explicit zero-shares guard before
        # any DB work. Helper raises ValueError if shares <= 0; doing
        # the check here keeps the counter accurate (otherwise the
        # except branch below would lump zero-shares with helper-guard
        # bypasses).
        if holding.shares is None or holding.shares <= 0:
            skipped_zero_shares += 1
            continue
        if not holding.cusip or len(holding.cusip) != 9:
            skipped_no_cusip += 1
            continue

        instrument_id = _resolve_cusip_to_instrument_id(conn, holding.cusip)
        if instrument_id is None:
            skipped_no_cusip += 1
            # PR1 deliberately does NOT track unresolved N-PORT CUSIPs in
            # ``unresolved_13f_cusips``. The 13F CUSIP universe drains via
            # #913 + #914; once #913's quarterly sweep populates more
            # external_identifiers, the N-PORT path will resolve more
            # holdings without bespoke telemetry. Follow-up: extend
            # the table with a source column. Codex pre-impl review #9.
            continue

        try:
            record_fund_observation(
                conn,
                instrument_id=instrument_id,
                fund_series_id=parsed.series_id,
                fund_series_name=parsed.series_name,
                fund_filer_cik=parsed.filer_cik,
                source_document_id=ref.accession_number,
                source_accession=ref.accession_number,
                source_field=None,
                source_url=primary_url,
                filed_at=filed_at,
                period_start=None,
                period_end=parsed.period_end,
                ingest_run_id=run_id,
                shares=holding.shares,
                market_value_usd=holding.value_usd,
                payoff_profile=holding.payoff_profile,
                asset_category=holding.asset_category,
            )
            inserted += 1
            touched_instruments.add(instrument_id)
        except ValueError:
            # Helper-side guard caught what the loop guard didn't —
            # log loudly and count as a parser-shape failure
            # (``skipped_non_equity`` is the closest existing bucket
            # but the operator sees the exception in logs to disambiguate).
            # The loop guards above mirror the helper exactly; reaching
            # this branch indicates a parser-helper contract drift.
            logger.exception(
                "n_port ingest: helper-side guard rejected holding cik=%s accession=%s cusip=%s "
                "(parser-helper contract drift — investigate)",
                filer_cik,
                ref.accession_number,
                holding.cusip,
            )
            skipped_non_equity += 1

    # Refresh ``ownership_funds_current`` once per touched instrument.
    # A single NPORT-P can carry 100s-1000s of holdings; refreshing
    # per-row would be O(N²). Set collapses to distinct issuers held.
    for instrument_id in touched_instruments:
        refresh_funds_current(conn, instrument_id=instrument_id)

    if not parsed.holdings:
        # Legal-empty NPORT-P (small fund holding only cash mid-quarter).
        # Recorded as success so re-runs skip it.
        return _AccessionOutcome(
            status="success",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            holdings_skipped_non_equity=0,
            holdings_skipped_short=0,
            holdings_skipped_non_share_units=0,
            holdings_skipped_zero_shares=0,
            error=None,
            series_id=parsed.series_id,
            period_of_report=parsed.period_end,
        )

    total_skipped = (
        skipped_no_cusip + skipped_non_equity + skipped_short + skipped_non_share_units + skipped_zero_shares
    )
    skip_breakdown = (
        f"non-equity={skipped_non_equity}, short={skipped_short}, "
        f"non-share-units={skipped_non_share_units}, zero-shares={skipped_zero_shares}, "
        f"no-cusip={skipped_no_cusip}"
    )
    if inserted == 0 and total_skipped > 0:
        # Every parsed holding was filtered out — fund holds only
        # non-equity / short / non-share-units / zero-shares /
        # unresolved-CUSIP positions. Report 'partial' so the operator
        # distinguishes from 'success' with zero holdings (legal-empty
        # NPORT-P).
        status = "partial"
        error = f"{total_skipped} holdings skipped ({skip_breakdown}); 0 written"
    elif total_skipped > 0:
        status = "partial"
        error = f"{total_skipped} holdings skipped ({skip_breakdown})"
    else:
        status = "success"
        error = None

    return _AccessionOutcome(
        status=status,
        holdings_inserted=inserted,
        holdings_skipped_no_cusip=skipped_no_cusip,
        holdings_skipped_non_equity=skipped_non_equity,
        holdings_skipped_short=skipped_short,
        holdings_skipped_non_share_units=skipped_non_share_units,
        holdings_skipped_zero_shares=skipped_zero_shares,
        error=error,
        series_id=parsed.series_id,
        period_of_report=parsed.period_end,
    )


# ---------------------------------------------------------------------------
# Iterators (exposed for ad-hoc reporting / debug)
# ---------------------------------------------------------------------------


def iter_fund_observations(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    limit: int = 1000,
) -> Iterator[dict[str, Any]]:
    """Yield the most recent observations for one instrument.
    Used by the rollup endpoint (in #919) and tests."""
    import psycopg.rows

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                   source_document_id, source_accession, source_url,
                   filed_at, period_start, period_end,
                   shares, market_value_usd, payoff_profile, asset_category
            FROM ownership_funds_observations
            WHERE instrument_id = %(iid)s
            ORDER BY period_end DESC, filed_at DESC
            LIMIT %(limit)s
            """,
            {"iid": instrument_id, "limit": limit},
        )
        for row in cur.fetchall():
            yield dict(row)
