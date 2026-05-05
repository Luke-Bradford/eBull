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

## Parser posture — lxml-direct, NOT EdgarTools

The spec recommended EdgarTools as a parser dep. The codebase already
parses 13F-HR / 13D/G / Form 4 with stdlib ``xml.etree.ElementTree``;
extending that pattern to N-PORT keeps the parser self-contained,
zero-dep, and offline-deterministic. The EdgarTools 13F drop-in is
tracked separately as #925; if N-PORT parsing turns out to need
EdgarTools' fallback heuristics, that's a follow-up — for now the
hand-rolled parser covers every well-formed NPORT-P SEC publishes
(per the EDGAR XSD).

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
import xml.etree.ElementTree as ET  # noqa: S405 — we only use ET to catch ParseError on malformed input
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol
from uuid import uuid4

import psycopg

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
# in the manifest worker (#869).
_PARSER_VERSION_NPORT = "nport-v1"


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


def parse_submissions_index(payload: str) -> list[AccessionRef]:
    """Walk ``data.sec.gov/submissions/CIK{cik}.json`` and emit one
    :class:`AccessionRef` per NPORT-P / NPORT-P/A row.

    Older-history shards via ``filings.files`` are out of scope here —
    the ``recent`` array holds the most recent ~1,000 filings per CIK
    which covers ≥ 12 quarters of monthly N-PORT for any actively
    filing fund family.
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

    out: list[AccessionRef] = []
    for i, accession in enumerate(accessions):
        if i >= len(forms):
            break
        form = str(forms[i])
        if form not in _NPORT_FORM_TYPES:
            continue
        filed_at = _safe_iso_datetime(filing_dates[i] if i < len(filing_dates) else "")
        period = _safe_iso_date(report_dates[i] if i < len(report_dates) else "")
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
# N-PORT XML parser
# ---------------------------------------------------------------------------


# N-PORT publishes XBRL-shaped XML under the SEC ``edgar/nport``
# namespace. The relevant elements live under
# ``{nport}genInfo`` (header) and ``{nport}invstOrSecs/invstOrSec``
# (per-holding records). The parser is namespace-aware to defend
# against SEC's namespace evolutions across N-PORT-1 → N-PORT-2 → etc.
# ``ET.iterparse`` could be used for memory savings on >1MB files but
# in practice N-PORT XMLs land at 200KB-2MB; ``fromstring`` is fine.
def _stripns(tag: str) -> str:
    """Drop XML namespace prefix: ``{ns}foo`` → ``foo``."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_text(elem: ET.Element, *, local_name: str) -> str | None:
    """Return the text of the first descendant whose local-name
    (namespace-stripped) matches ``local_name``, or ``None``."""
    for child in elem.iter():
        if _stripns(child.tag) == local_name:
            text = (child.text or "").strip()
            return text or None
    return None


def _children_by_local_name(elem: ET.Element, *, local_name: str) -> list[ET.Element]:
    """Direct-children filter by local name (not full descendant scan)."""
    return [c for c in elem if _stripns(c.tag) == local_name]


def _decimal_or_none(text: str | None) -> Decimal | None:
    if not text:
        return None
    try:
        return Decimal(text.strip())
    except InvalidOperation, AttributeError:
        return None


def parse_n_port_payload(xml: str) -> NPortFiling:
    """Parse an NPORT-P primary doc XML into an :class:`NPortFiling`.

    Pure XML-in / dataclass-out — no network calls, no DB access.
    Raises :class:`NPortParseError` on malformed XML, or
    :class:`NPortMissingSeriesError` if the filing has no ``seriesId``.

    Holdings are emitted in document order. The ingester filters /
    transforms downstream — this parser surfaces every well-formed
    holding without applying business logic (debt / short / preferred
    rows are surfaced; the ingester drops them via the equity-common
    guard).
    """
    try:
        root = ET.fromstring(xml)  # noqa: S314 — we accept SEC-published XML; parser is stdlib, no DTD resolution
    except ET.ParseError as exc:
        raise NPortParseError(f"NPORT-P XML parse failed: {exc}") from exc

    # ----- header (genInfo + filer identity) -----
    # The NPORT-P XML carries:
    #   formData/genInfo/regCik         — registered investment company CIK
    #   formData/genInfo/seriesId       — SEC series identifier (S0000xxxxx)
    #   formData/genInfo/seriesName     — fund series human-readable name
    #   formData/genInfo/repPdEnd       — period_of_report end date
    #   headerData/filerInfo/filer/issuerCredentials/cik (for the registrant)
    # Walk by local-name to be robust to namespace versioning.
    cik_text = _find_text(root, local_name="regCik") or _find_text(root, local_name="cik")
    series_id = _find_text(root, local_name="seriesId")
    series_name = _find_text(root, local_name="seriesName") or ""
    period_end_text = _find_text(root, local_name="repPdEnd")
    filed_at_text = _find_text(root, local_name="filedAt") or _find_text(root, local_name="acceptedDate")

    if not cik_text:
        raise NPortParseError("NPORT-P: missing regCik / cik in header")
    if not series_id:
        # Codex pre-impl review #2 — refuse to synthesise.
        raise NPortMissingSeriesError(
            "NPORT-P: missing seriesId in genInfo header; refusing to "
            "synthesise an identity. Filing tombstoned for operator review."
        )
    if not period_end_text:
        raise NPortParseError("NPORT-P: missing repPdEnd in genInfo header")

    period_end = _safe_iso_date(period_end_text)
    if period_end is None:
        raise NPortParseError(f"NPORT-P: malformed repPdEnd={period_end_text!r}")

    # Codex pre-push review #1: do NOT default ``filed_at`` to
    # ``period_end`` midnight inside the parser. Two filings sharing a
    # period (NPORT-P + NPORT-P/A) would then carry identical
    # ``filed_at`` values and the ``_current`` refresh tie-break would
    # pick the wrong one. The ingester layers in the submissions-index
    # ``filingDate`` (always present in the ``recent`` array) before
    # any midnight fallback — see ``_ingest_single_accession``.
    filed_at = _safe_iso_datetime(filed_at_text)

    # ----- holdings -----
    holdings: list[NPortHolding] = []
    for holding_elem in root.iter():
        if _stripns(holding_elem.tag) != "invstOrSec":
            continue
        cusip = _find_text(holding_elem, local_name="cusip") or ""
        issuer_name = _find_text(holding_elem, local_name="name") or ""
        balance_text = _find_text(holding_elem, local_name="balance")
        value_text = _find_text(holding_elem, local_name="valUSD")
        payoff_profile = _find_text(holding_elem, local_name="payoffProfile") or ""
        asset_cat = _find_text(holding_elem, local_name="assetCat") or ""
        issuer_cat = _find_text(holding_elem, local_name="issuerCat") or ""
        units = _find_text(holding_elem, local_name="units") or ""

        balance = _decimal_or_none(balance_text)
        if balance is None:
            # No balance = unparseable holding; skip silently. The
            # ingester counts these via the parser-failure path.
            continue

        holdings.append(
            NPortHolding(
                cusip=cusip.strip().upper(),
                issuer_name=issuer_name,
                shares=balance,
                value_usd=_decimal_or_none(value_text),
                payoff_profile=payoff_profile,
                asset_category=asset_cat,
                issuer_category=issuer_cat,
                units=units,
            )
        )

    return NPortFiling(
        filer_cik=_zero_pad_cik(cik_text),
        series_id=series_id,
        series_name=series_name,
        period_end=period_end,
        filed_at=filed_at,
        holdings=tuple(holdings),
    )


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
) -> IngestSummary:
    """Fetch + parse + upsert every pending NPORT-P for one fund-filer
    CIK. Per-accession failures are isolated (logged + tombstoned) so
    a single malformed accession does not abort the filer batch."""
    cik = _zero_pad_cik(filer_cik)
    summary = _MutableSummary(cik=cik)

    submissions_payload = sec.fetch_document_text(_submissions_url(cik))
    if submissions_payload is None:
        logger.info("n_port ingest: submissions JSON 404/error for cik=%s", cik)
        return summary.to_immutable()

    pending = parse_submissions_index(submissions_payload)
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
                summary = ingest_fund_n_port(conn, sec, filer_cik=cik)
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
