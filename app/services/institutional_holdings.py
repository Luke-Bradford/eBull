"""SEC 13F-HR institutional holdings ingester (#730 PR 2 of 4).

Walks the operator-curated ``institutional_filer_seeds`` list and, for
each active filer:

  1. Fetches ``data.sec.gov/submissions/CIK{cik}.json`` to discover
     13F-HR / 13F-HR/A accessions filed by that CIK.
  2. For each accession not yet present in ``institutional_holdings``,
     fetches the per-filing ``index.json`` to locate the
     ``primary_doc.xml`` + infotable XML attachments.
  3. Parses both via :mod:`app.providers.implementations.sec_13f`.
  4. Resolves each holding's CUSIP to an ``instrument_id`` via
     ``external_identifiers``. Holdings whose CUSIP is unknown are
     dropped with a counter — the gap is tracked in #740 (CUSIP
     backfill via SEC company-facts XBRL + 13F securities list).
  5. Upserts the filer + every resolved holding inside one
     transaction. Idempotent re-ingest of the same accession is
     guaranteed by the partial UNIQUE INDEX from migration 090.

The ingester is the only DB-touching half of the pipeline; the
parser stays pure (XML in, dataclasses out). The HTTP fetch routes
through the bounded-concurrency client added in #728 so concurrent
filer ingests share the SEC fair-use rate budget.

Tombstones: a filing whose primary_doc.xml or infotable.xml fetch
404s is recorded in ``data_ingestion_runs`` with status='partial'
plus the accession number in ``error``. The next run sees the
accession is still missing and retries — short-lived 404s heal
naturally; persistent failures show up in the ops monitor (#13).
"""

from __future__ import annotations

import json
import logging
import time
import xml.etree.ElementTree as ET  # noqa: S405 — only used to catch ET.ParseError on parse failure
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import uuid4

import psycopg
import psycopg.rows

from app.providers.implementations.sec_13f import (
    ThirteenFFilerInfo,
    ThirteenFHolding,
    dominant_voting_authority,
    parse_infotable,
    parse_primary_doc,
)
from app.services import raw_filings
from app.services.fundamentals import finish_ingestion_run, start_ingestion_run
from app.services.ownership_observations import (
    record_institution_observation,
    refresh_institutions_current,
    resolve_filer_cik_or_raise,
)

# Parser-version tags written alongside the raw bodies. Re-wash
# workflows compare against these constants and skip rows already
# on the latest parser. Bump when ``parse_primary_doc`` /
# ``parse_infotable`` semantics change in a way that affects what
# lands in the typed tables.
_PARSER_VERSION_13F_PRIMARY = "13f-primary-v1"
_PARSER_VERSION_13F_INFOTABLE = "13f-infotable-v1"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider contract
# ---------------------------------------------------------------------------


class SecArchiveFetcher(Protocol):
    """Subset of the SEC EDGAR provider this ingester relies on.

    Decoupled to keep the service unit-testable with an in-memory
    fake. The production binding is :class:`app.providers.
    implementations.sec_edgar.SecEdgarProvider`, which already has
    the ``fetch_document_text`` method shape.
    """

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AccessionRef:
    """One discovered 13F-HR accession to ingest."""

    accession_number: str
    filing_type: str  # "13F-HR" | "13F-HR/A"
    period_of_report: date | None  # may be NULL when SEC submissions index lacks it
    filed_at: datetime | None


@dataclass(frozen=True)
class IngestSummary:
    """Per-filer rollup of one ingest pass."""

    filer_cik: str
    accessions_seen: int
    accessions_ingested: int
    accessions_failed: int
    holdings_inserted: int
    holdings_skipped_no_cusip: int
    # First per-accession failure reason for this filer, if any.
    # Surfaces into ``data_ingestion_runs.error`` via the batch
    # wrapper. Only the first reason is captured to keep the audit
    # column under the row size budget; full per-accession failure
    # detail lives in ``institutional_holdings_ingest_log.error``.
    first_error: str | None = None


@dataclass(frozen=True)
class _AccessionOutcome:
    """Internal: per-accession ingest outcome.

    ``status`` is one of:
      * ``'success'`` — every step completed and at least one
        canonical row was attempted (zero-row legal-empty 13F also
        lands here so the next run skips it).
      * ``'partial'`` — every step completed but some holdings were
        dropped (unresolved CUSIPs); next-run retry would surface
        the same gap until #740 closes.
      * ``'failed'`` — fetch / parse failure that prevents writing
        a canonical row. The accession is logged so re-runs skip
        it; the operator can clear the log row to force a retry.
    """

    status: str
    holdings_inserted: int
    holdings_skipped_no_cusip: int
    error: str | None

    @property
    def ingested(self) -> bool:
        return self.status in ("success", "partial")


# ---------------------------------------------------------------------------
# URL builders
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
    :class:`AccessionRef` per 13F-HR / 13F-HR/A row.

    SEC's submissions JSON shape:

    .. code:: json

        {
          "filings": {
            "recent": {
              "accessionNumber": ["0001067983-25-000001", ...],
              "filingDate":      ["2025-02-14", ...],
              "form":            ["13F-HR", "10-Q", ...],
              "reportDate":      ["2024-12-31", "", ...],
              ...
            },
            "files": [{"name": "CIK{cik}-submissions-001.json", ...}]
          }
        }

    Older-history shards are referenced by ``files`` and need a
    second fetch. Out of scope here — the ingester walks the
    ``recent`` array, which holds the most recent ~1,000 filings
    per filer.

    For active filers (banks, asset managers) the recent array can
    cover 60+ years of filings since most issuers file far fewer
    than 1,000 reports per decade. ``min_period_of_report`` (#1008)
    bounds historical depth: when set, accessions whose
    ``period_of_report`` is older than the cut-off are skipped.
    Pre-2013 13F filings additionally have no machine-readable
    primary_doc / infotable structure, so iterating them costs an
    SEC fetch + parse and yields zero rows. First-install bootstrap
    passes a recent cut-off (last 4 quarters); the standalone
    weekly cron leaves it ``None`` for full historical coverage.
    """
    try:
        data: dict[str, Any] = json.loads(payload)
    except json.JSONDecodeError:
        logger.warning("submissions index payload is not valid JSON")
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
        form = forms[i]
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        filed_at = _safe_iso_datetime(filing_dates[i] if i < len(filing_dates) else "")
        period = _safe_iso_date(report_dates[i] if i < len(report_dates) else "")
        if min_period_of_report is not None and period is not None and period < min_period_of_report:
            # #1008 — first-install bootstrap caller passes a recent
            # cut-off so we don't iterate pre-2013 filings whose
            # primary_doc/infotable XML structure didn't exist yet.
            continue
        out.append(
            AccessionRef(
                accession_number=str(accession),
                filing_type=str(form),
                period_of_report=period,
                filed_at=filed_at,
            )
        )
    return out


def parse_archive_index(payload: str) -> tuple[str | None, str | None]:
    """Walk a per-accession ``index.json`` and return
    ``(primary_doc_filename, infotable_filename)``.

    SEC archive listing shape (verified against #723 fix at
    ``app/services/filing_documents.py``):

    .. code:: json

        {
          "directory": {
            "item": [
              {"name": "primary_doc.xml", ...},
              {"name": "infotable.xml", ...},
              ...
            ]
          }
        }

    The infotable file name varies. Common patterns:
      * ``infotable.xml`` (most issuers).
      * ``form13fInfoTable.xml`` (some larger filers — pre-2018).
      * ``{accession_no_dashes}_infotable.xml`` (rare, agent-built).
      * ``13F_{cik}_{period_end}.xml`` (Vanguard, BlackRock, and
        other large filers using the SEC EDGAR Online filing
        client — caught when the curated seed list pulled live
        data and Vanguard's infotable was named
        ``13F_0000102909_20251231.xml``).

    Heuristic: prefer any ``.xml`` whose lowercase basename
    contains ``infotable``, ``information_table``, or starts with
    ``13f`` / ``form13f``. As a last-resort fallback, when there's
    exactly one non-primary_doc XML in the listing, treat it as
    the infotable — every 13F-HR submission has at most two XML
    attachments by SEC convention.
    """
    try:
        data: dict[str, Any] = json.loads(payload)
    except json.JSONDecodeError:
        return None, None

    directory = data.get("directory", {})
    items = directory.get("item", [])

    primary: str | None = None
    infotable: str | None = None
    other_xmls: list[str] = []
    for item in items:
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        lower = name.lower()
        if lower == "primary_doc.xml":
            primary = name
            continue
        if not lower.endswith(".xml"):
            continue
        canonical = lower.replace("-", "").replace("_", "").replace(" ", "")
        if (
            "infotable" in canonical
            or "informationtable" in canonical
            or canonical.startswith("13f")
            or canonical.startswith("form13f")
        ):
            infotable = name
        else:
            other_xmls.append(name)

    # Fallback: if no name-pattern match fired but exactly one
    # non-primary_doc XML exists, treat it as the infotable. SEC
    # 13F-HR submissions have at most two XML attachments
    # (primary_doc + infotable); ambiguity is impossible at the
    # one-extra-XML scale.
    if infotable is None and len(other_xmls) == 1:
        infotable = other_xmls[0]

    return primary, infotable


def _safe_iso_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _safe_iso_datetime(text: str | None) -> datetime | None:
    """Coerce a ``YYYY-MM-DD`` to a tz-aware UTC ``datetime``.

    ``filed_at`` is ``TIMESTAMPTZ`` — passing a naive datetime would
    have psycopg fall back to the server's local zone, drifting the
    persisted timestamp. Always tag UTC explicitly. (Same shape as
    the parser's ``_parse_signature_date`` in #739.)
    """
    parsed = _safe_iso_date(text)
    if parsed is None:
        return None
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _list_active_filer_seeds(conn: psycopg.Connection[tuple]) -> list[str]:
    cur = conn.execute("SELECT cik FROM institutional_filer_seeds WHERE active = TRUE ORDER BY cik")
    return [_zero_pad_cik(row[0]) for row in cur.fetchall()]


def list_directory_filer_ciks(conn: psycopg.Connection[tuple]) -> list[str]:
    """Walk every CIK in ``institutional_filers`` (the SEC 13F filer
    directory populated by ``sec_13f_filer_directory_sync`` #912).

    Used by :func:`ingest_directory_filers` (#913) — the universe
    expansion entrypoint. Returns CIKs ordered by ``last_filing_at``
    DESC so the most recently active filers are ingested first; a
    deadline-budget interruption then leaves the long-tail
    (low-activity filers) for the next sweep without losing the
    operator-relevant household names.
    """
    cur = conn.execute(
        """
        SELECT cik
        FROM institutional_filers
        ORDER BY last_filing_at DESC NULLS LAST, cik
        """
    )
    return [_zero_pad_cik(row[0]) for row in cur.fetchall()]


def _existing_accessions_for_filer(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
) -> set[str]:
    """Return every accession_number this filer has already had an
    ingest attempt for — success OR partial OR failed.

    Reads from ``institutional_holdings_ingest_log`` (the
    per-accession tombstone) rather than the holdings table itself
    because:
      * An empty 13F-HR (legal — filer reported exempt-list) writes
        no holding row.
      * An accession where every CUSIP is unresolved (#740 backfill
        gap) also writes no holding row.
      * A persistent 404 on the archive index never writes a
        holding row.
    All three cases must be tracked so the ingester does not
    re-fetch them every run. The log is the source of truth for
    "have we attempted this accession?".

    A row stamped ``status='failed'`` is also treated as already
    attempted, so re-runs do not tight-loop against a persistent
    404. To retry a specific accession the operator deletes the
    log row for it and the next run re-fetches; bulk retry is a
    follow-up tool.
    """
    cur = conn.execute(
        """
        SELECT accession_number
        FROM institutional_holdings_ingest_log
        WHERE filer_cik = %(cik)s
        """,
        {"cik": filer_cik},
    )
    return {row[0] for row in cur.fetchall()}


def _record_ingest_attempt(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
    accession_number: str,
    period_of_report: date | None,
    status: str,
    holdings_inserted: int = 0,
    holdings_skipped: int = 0,
    error: str | None = None,
) -> None:
    """Idempotent upsert into institutional_holdings_ingest_log.

    Status is one of 'success' / 'partial' / 'failed'. The row is
    keyed on accession_number (globally unique per SEC), so
    re-recording overwrites the prior attempt — this lets a
    follow-up run that succeeds promote a 'partial' or 'failed'
    accession to 'success'.
    """
    conn.execute(
        """
        INSERT INTO institutional_holdings_ingest_log (
            accession_number, filer_cik, period_of_report,
            status, holdings_inserted, holdings_skipped, error
        ) VALUES (
            %(accession_number)s, %(filer_cik)s, %(period_of_report)s,
            %(status)s, %(holdings_inserted)s, %(holdings_skipped)s, %(error)s
        )
        ON CONFLICT (accession_number) DO UPDATE SET
            status = EXCLUDED.status,
            holdings_inserted = EXCLUDED.holdings_inserted,
            holdings_skipped = EXCLUDED.holdings_skipped,
            error = EXCLUDED.error,
            fetched_at = NOW()
        """,
        {
            "accession_number": accession_number,
            "filer_cik": filer_cik,
            "period_of_report": period_of_report,
            "status": status,
            "holdings_inserted": holdings_inserted,
            "holdings_skipped": holdings_skipped,
            "error": error,
        },
    )


def _record_unresolved_cusip(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    name_of_issuer: str,
    accession_number: str,
) -> None:
    """Track a 13F-HR holding whose CUSIP didn't resolve to an
    ``instruments`` row at ingest time. Idempotent — re-encountering
    the same CUSIP bumps ``observation_count`` + refreshes the
    latest filer-supplied issuer name. Resolver service (#781)
    consumes this table.

    ``resolution_status`` is intentionally NOT reset on conflict.
    A tombstoned CUSIP (``unresolvable`` / ``ambiguous`` /
    ``conflict``) that re-appears in a new filing gets its
    observation_count and last-seen name updated for audit, but
    the resolver still skips it on the next pass. The operator
    clears ``resolution_status`` to force a retry once the
    underlying issue (missing instrument seed, share-class
    disambiguation, mapping correction) is fixed. Bot review of
    #781 caught the absence of this note.
    """
    conn.execute(
        """
        INSERT INTO unresolved_13f_cusips (
            cusip, name_of_issuer, last_accession_number
        ) VALUES (%(cusip)s, %(name)s, %(accession)s)
        ON CONFLICT (cusip) DO UPDATE SET
            name_of_issuer = EXCLUDED.name_of_issuer,
            last_accession_number = EXCLUDED.last_accession_number,
            observation_count = unresolved_13f_cusips.observation_count + 1,
            last_observed_at = NOW()
        """,
        {
            "cusip": cusip.strip().upper(),
            "name": name_of_issuer,
            "accession": accession_number,
        },
    )


def _resolve_cusip_to_instrument_id(
    conn: psycopg.Connection[tuple],
    cusip: str,
) -> int | None:
    """Look up the instrument_id mapped to a CUSIP via
    external_identifiers. CUSIPs use ``provider='sec'``,
    ``identifier_type='cusip'``. The backfill that populates these
    rows is tracked in #740."""
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


def classify_filer_type(
    conn: psycopg.Connection[tuple],
    cik: str,
) -> str:
    """Map a filer CIK to one of the constrained filer_type labels.

    Delegates to :func:`app.services.ncen_classifier.compose_filer_type`,
    which composes the priority chain:

      1. Curated ETF seed list (``etf_filer_cik_seeds``) -> ``ETF``.
      2. N-CEN-derived classification (``ncen_filer_classifications``,
         #782) -> ``INS`` / ``INV`` / ``OTHER`` per the
         investment-company-type mapping.
      3. Default -> ``INV``.

    Indirection here keeps the 13F-HR ingester unaware of the
    individual classification sources — adding a future Form ADV /
    FOCUS ingest for ``BD`` classification only requires
    ``compose_filer_type`` to grow another priority tier; this
    function (and every call site that invokes it) stays unchanged.

    Broker-dealer (``BD``) is reachable from this function's enum
    but no source today populates it — see #782 out-of-scope notes.
    """
    # Local import to avoid a circular dependency: ncen_classifier
    # imports from the institutional namespace for shared helpers
    # in a future enhancement, and the test fixtures import this
    # module before the classifier service is initialised.
    from app.services.ncen_classifier import compose_filer_type

    return compose_filer_type(conn, cik)


def seed_etf_filer(
    conn: psycopg.Connection[tuple],
    *,
    cik: str | int,
    label: str,
    notes: str | None = None,
    active: bool = True,
) -> None:
    """Idempotent helper for adding a CIK to the curated ETF list.
    Mirrors :func:`seed_filer` for the institutional-filer seed
    table; both are exposed for tests + admin scripts."""
    conn.execute(
        """
        INSERT INTO etf_filer_cik_seeds (cik, label, active, notes)
        VALUES (%(cik)s, %(label)s, %(active)s, %(notes)s)
        ON CONFLICT (cik) DO UPDATE SET
            label = EXCLUDED.label,
            active = EXCLUDED.active,
            notes = COALESCE(EXCLUDED.notes, etf_filer_cik_seeds.notes)
        """,
        {
            "cik": _zero_pad_cik(cik),
            "label": label,
            "active": active,
            "notes": notes,
        },
    )


def _upsert_filer(
    conn: psycopg.Connection[tuple],
    info: ThirteenFFilerInfo,
) -> int:
    """Insert / update an institutional_filers row. Returns filer_id.

    ``filer_type`` is derived from the curated ETF list (#730 PR 3)
    via :func:`classify_filer_type` on every write. The classifier
    is cheap (single index lookup) and idempotent, so re-running
    after a seed-list update propagates the new label on the next
    ingest cycle without a backfill migration.

    ``aum_usd`` is not set here — the aggregator (#730 PR 4) sums
    the latest-quarter holdings per filer on read.
    """
    filer_type = classify_filer_type(conn, info.cik)
    cur = conn.execute(
        """
        INSERT INTO institutional_filers (cik, name, filer_type, last_filing_at)
        VALUES (%(cik)s, %(name)s, %(filer_type)s, %(last_filing_at)s)
        ON CONFLICT (cik) DO UPDATE SET
            name = EXCLUDED.name,
            filer_type = EXCLUDED.filer_type,
            last_filing_at = GREATEST(
                COALESCE(institutional_filers.last_filing_at, '-infinity'),
                COALESCE(EXCLUDED.last_filing_at, '-infinity')
            ),
            fetched_at = NOW()
        RETURNING filer_id
        """,
        {
            "cik": info.cik,
            "name": info.name,
            "filer_type": filer_type,
            "last_filing_at": info.filed_at,
        },
    )
    row = cur.fetchone()
    assert row is not None, "filer upsert RETURNING produced no row"
    return int(row[0])


def _record_13f_observations_for_filing(
    conn: psycopg.Connection[Any],
    *,
    filer_id: int,
    accession_number: str,
    period_of_report: date,
    filed_at: datetime,
    resolved_holdings: list[tuple[int, ThirteenFHolding]],
) -> None:
    """Record one ``ownership_institutions_observations`` row per
    (instrument, exposure_kind) within a single 13F accession.

    Mirrors the legacy batch-sync rule in
    ``ownership_observations_sync.sync_institutions``:

      - Identity per holding: ``(instrument_id, filer_cik, period_end,
        source_document_id, exposure_kind)``. PUT/CALL options on the
        same security as the equity position produce SEPARATE rows.
      - ``ownership_nature``: pinned to ``'economic'`` (13F-HR is a
        full-position report).
      - ``filer_cik`` resolved via ``resolve_filer_cik_or_raise`` so
        an orphan filer_id surfaces loudly rather than silently
        dropping observations (Codex plan-review finding #2).

    Refresh of ``ownership_institutions_current`` is the caller's
    responsibility — keeps this function pure-write.
    """
    cik, filer_name, filer_type = resolve_filer_cik_or_raise(conn, filer_id=filer_id)
    run_id = uuid4()
    for instrument_id, holding in resolved_holdings:
        exposure: Any = "EQUITY"
        if holding.put_call in ("PUT", "CALL"):
            exposure = holding.put_call
        record_institution_observation(
            conn,
            instrument_id=instrument_id,
            filer_cik=cik,
            filer_name=filer_name,
            filer_type=filer_type,
            ownership_nature="economic",
            source="13f",
            source_document_id=accession_number,
            source_accession=accession_number,
            source_field=None,
            source_url=None,
            filed_at=filed_at,
            period_start=None,
            period_end=period_of_report,
            ingest_run_id=run_id,
            shares=Decimal(holding.shares_or_principal),
            market_value_usd=Decimal(holding.value_usd) if holding.value_usd is not None else None,
            voting_authority=dominant_voting_authority(holding),
            exposure_kind=exposure,
        )


def _upsert_holding(
    conn: psycopg.Connection[tuple],
    *,
    filer_id: int,
    instrument_id: int,
    accession_number: str,
    period_of_report: date,
    filed_at: datetime,
    holding: ThirteenFHolding,
) -> bool:
    """Idempotent per-row upsert. Returns True on insert, False on
    re-ingest of the same (accession, instrument, is_put_call)
    tuple — the partial UNIQUE INDEX from migration 090 backstops
    re-runs.

    Voting-authority labelling: derive the dominant authority via
    :func:`dominant_voting_authority`. NULL maps when all three
    sub-amounts are zero (legal but rare; the schema CHECK allows
    NULL).
    """
    voting = dominant_voting_authority(holding)
    # ``ON CONFLICT DO NOTHING`` (no explicit conflict_target)
    # matches any unique violation, including the partial
    # expression-based UNIQUE INDEX from migration 090
    # ``uq_holdings_accession_instrument_putcall`` on
    # ``(accession_number, instrument_id, COALESCE(is_put_call, 'EQUITY'))``.
    # An explicit inference clause cannot reference an expression
    # column without repeating the COALESCE expression verbatim;
    # using DO NOTHING with no target is the cleanest safe path
    # here because the synthetic PK on holding_id never collides
    # (BIGSERIAL) so any conflict that fires is the expression
    # index by construction.
    cur = conn.execute(
        """
        INSERT INTO institutional_holdings (
            filer_id, instrument_id, accession_number, period_of_report,
            shares, market_value_usd, voting_authority, is_put_call, filed_at
        ) VALUES (
            %(filer_id)s, %(instrument_id)s, %(accession_number)s, %(period_of_report)s,
            %(shares)s, %(market_value_usd)s, %(voting_authority)s, %(is_put_call)s, %(filed_at)s
        )
        ON CONFLICT DO NOTHING
        """,
        {
            "filer_id": filer_id,
            "instrument_id": instrument_id,
            "accession_number": accession_number,
            "period_of_report": period_of_report,
            "shares": holding.shares_or_principal,
            "market_value_usd": holding.value_usd,
            "voting_authority": voting,
            "is_put_call": holding.put_call,
            "filed_at": filed_at,
        },
    )
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Public seeding helper (exposed for tests + admin scripts)
# ---------------------------------------------------------------------------


def seed_filer(
    conn: psycopg.Connection[tuple],
    *,
    cik: str | int,
    label: str,
    expected_name: str | None = None,
    notes: str | None = None,
    active: bool = True,
) -> None:
    """Idempotent helper for adding a filer to the curated list.

    Used by tests + an operator-side script. The admin UI in PR 4
    will call the same helper via an API endpoint.

    ``expected_name`` records the operator-recorded SEC entity name
    so the verification sweep
    (``app.services.filer_seed_verification``) can flag drift
    between the recorded name and SEC's live submissions.json. When
    omitted, ``label`` is used — labels with disambiguation suffixes
    (e.g. ``"FMR LLC (Fidelity)"``) will trip the verification
    sweep until the operator fills in a clean ``expected_name``.
    """
    conn.execute(
        """
        INSERT INTO institutional_filer_seeds (
            cik, label, expected_name, active, notes
        )
        VALUES (
            %(cik)s, %(label)s, COALESCE(%(expected_name)s, %(label)s),
            %(active)s, %(notes)s
        )
        ON CONFLICT (cik) DO UPDATE SET
            label = EXCLUDED.label,
            -- Use the RAW parameter (not EXCLUDED) so that a caller
            -- omitting expected_name on an update preserves the
            -- operator's prior value. EXCLUDED.expected_name is
            -- always non-null because the VALUES clause coalesces
            -- it to label — using EXCLUDED would silently clobber
            -- prior operator-set values with display text. Codex
            -- pre-push review caught this.
            expected_name = COALESCE(%(expected_name)s, institutional_filer_seeds.expected_name),
            active = EXCLUDED.active,
            notes = COALESCE(EXCLUDED.notes, institutional_filer_seeds.notes)
        """,
        {
            "cik": _zero_pad_cik(cik),
            "label": label,
            "expected_name": expected_name,
            "active": active,
            "notes": notes,
        },
    )


# ---------------------------------------------------------------------------
# Public ingest entry points
# ---------------------------------------------------------------------------


def ingest_filer_13f(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    filer_cik: str,
    ingestion_run_id: int | None = None,
    min_period_of_report: date | None = None,
) -> IngestSummary:
    """Fetch + parse + upsert every pending 13F-HR for one filer.

    ``filer_cik`` is normalised to 10-digit padded form on entry.
    ``ingestion_run_id`` is optional — when provided, per-row counts
    flow into the existing data_ingestion_runs row owned by the
    caller; when absent, no audit row is touched. The batch-mode
    entry point :func:`ingest_all_active_filers` always supplies one.
    ``min_period_of_report`` (#1008) bounds historical depth — see
    :func:`parse_submissions_index`.
    """
    cik = _zero_pad_cik(filer_cik)
    summary = _MutableSummary(cik=cik)

    submissions_payload = sec.fetch_document_text(_submissions_url(cik))
    if submissions_payload is None:
        logger.warning("13F ingest: submissions JSON 404/error for cik=%s", cik)
        return summary.to_immutable()

    pending_accessions = parse_submissions_index(
        submissions_payload,
        min_period_of_report=min_period_of_report,
    )
    summary.accessions_seen = len(pending_accessions)

    already_ingested = _existing_accessions_for_filer(conn, filer_cik=cik)

    for ref in pending_accessions:
        if ref.accession_number in already_ingested:
            continue
        outcome = _ingest_single_accession(conn, sec, filer_cik=cik, ref=ref)
        # Always log the attempt so the next run skips this
        # accession regardless of how it ended (zero-row legal
        # empty, all-CUSIPs-unresolved, persistent 404). Without
        # this row the ingester re-fetches the same archive on
        # every cadence run, burning SEC bandwidth and producing
        # duplicate ``holdings_skipped_no_cusip`` counts forever.
        _record_ingest_attempt(
            conn,
            filer_cik=cik,
            accession_number=ref.accession_number,
            period_of_report=ref.period_of_report,
            status=outcome.status,
            holdings_inserted=outcome.holdings_inserted,
            holdings_skipped=outcome.holdings_skipped_no_cusip,
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

    return summary.to_immutable()


def ingest_all_active_filers(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    ciks: list[str] | None = None,
    deadline_seconds: float | None = None,
    source_label: str = "sec_edgar_13f",
    min_period_of_report: date | None = None,
) -> list[IngestSummary]:
    """Walk a list of filer CIKs and ingest each filer's pending 13F-HRs.

    ``ciks`` selects the universe:
      * ``None`` (default) — walks ``institutional_filer_seeds`` for
        operator-curated runs (legacy behaviour, kept for backward
        compat with the existing scheduled trigger).
      * supplied list — walks exactly those CIKs in order. The new
        ``sec_13f_quarterly_sweep`` job (#913) passes the universe
        from ``institutional_filers`` so every filer in the SEC
        directory gets ingested.

    ``deadline_seconds`` is a soft budget — when exceeded between
    per-filer iterations, the loop stops cleanly and the partial
    work commits. Already-ingested accessions are tombstoned in
    ``institutional_holdings_ingest_log``, so the next sweep
    resumes against the unprocessed tail rather than redoing work.

    ``source_label`` distinguishes audit trails — the seed-curated
    run keeps ``sec_edgar_13f`` so existing dashboards aren't
    perturbed; the universe sweep passes ``sec_edgar_13f_directory``
    so an operator can grep ``data_ingestion_runs`` to separate the
    two paths.

    ``min_period_of_report`` (#1008) bounds historical depth so
    first-install bootstrap doesn't iterate decades of pre-2013
    13F filings whose primary_doc/infotable XML structure didn't
    exist yet. ``None`` keeps the previous full-history behaviour
    for the standalone weekly cron.
    """
    if ciks is None:
        ciks = _list_active_filer_seeds(conn)
    if not ciks:
        logger.info("13F ingest: no filer CIKs to ingest; nothing to do")
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
    cancelled_by_operator = False
    filers_attempted = 0
    # PR3d #1064 follow-up — poll the bootstrap cancel signal between
    # filers. The 13F-HR universe is ~11k filers @ ~30s each; a
    # cooperative cancel from the operator's modal lands within one
    # filer-iteration (~30s) instead of waiting on the soft 6h deadline.
    # Outside a bootstrap dispatch the contextvar is unset and the
    # helper short-circuits to False, so the standalone weekly sweep
    # and operator manual-trigger paths are unaffected.
    from app.services.processes.bootstrap_cancel_signal import bootstrap_cancel_requested

    try:
        for cik in ciks:
            # Cancel ranks above deadline: the operator-cancel branch
            # raises BootstrapStageCancelled to flip the bootstrap_stage
            # to ``cancelled``, while deadline returns normally
            # (status='success' on bootstrap_stage, partial in
            # data_ingestion_runs). If both signals fire on the same
            # iteration, taking the deadline branch first would silently
            # treat the cancel as "success — incremental progress" and
            # the bootstrap row would never reach ``cancelled``. Codex
            # pre-push round 1.
            if bootstrap_cancel_requested():
                cancelled_by_operator = True
                logger.info(
                    "13F ingest: cancel signal observed after %d/%d filers; bookkeeping then raising",
                    filers_attempted,
                    len(ciks),
                )
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                deadline_hit = True
                logger.info(
                    "13F ingest: deadline reached after %d/%d filers; remaining will be picked up by the next sweep",
                    filers_attempted,
                    len(ciks),
                )
                break
            filers_attempted += 1
            try:
                summary = ingest_filer_13f(
                    conn,
                    sec,
                    filer_cik=cik,
                    ingestion_run_id=run_id,
                    min_period_of_report=min_period_of_report,
                )
            except Exception as exc:  # noqa: BLE001 — per-filer crash must not abort the batch
                logger.exception("13F ingest: filer %s raised; continuing batch", cik)
                crash_error = f"{cik}: {exc}"
                conn.rollback()
                continue
            conn.commit()
            summaries.append(summary)
            rows_upserted += summary.holdings_inserted
            rows_skipped += summary.holdings_skipped_no_cusip
            accession_failures += summary.accessions_failed
            if summary.first_error and first_accession_error is None:
                first_accession_error = f"{cik} {summary.first_error}"
    finally:
        # Status precedence:
        #   * deadline hit (work was interrupted, partial by definition)
        #     -> partial, even if every attempted filer crashed
        #   * any per-filer crash + zero successful summaries -> failed
        #   * any per-filer crash with at least one summary    -> partial
        #   * any per-accession failure across the batch       -> partial
        #   * any per-accession unresolved-CUSIP skip          -> partial
        #     (rows_skipped > 0 indicates partial coverage)
        #   * else                                              -> success
        # Codex pre-push review #913: deadline_hit must beat the
        # crash-only `failed` branch so an interrupted sweep with
        # incidental per-filer crashes is correctly classified as
        # resumable partial work.
        # Cancel + deadline both produce resumable-partial state; cancel
        # ranks above deadline so the audit trail names the operator
        # action rather than a clock that never actually expired.
        if cancelled_by_operator:
            status = "partial"
        elif deadline_hit:
            status = "partial"
        elif crash_error and not summaries:
            status = "failed"
        elif crash_error or accession_failures > 0 or rows_skipped > 0:
            status = "partial"
        else:
            status = "success"
        # Combine the surfaced error so data_ingestion_runs.error
        # carries a useful audit trail without exceeding the column.
        # Per-accession detail lives in
        # institutional_holdings_ingest_log; this is the executive
        # summary.
        error_parts: list[str] = []
        if crash_error:
            error_parts.append(f"crash: {crash_error}")
        if first_accession_error:
            error_parts.append(f"accession: {first_accession_error}")
        if rows_skipped > 0 and not error_parts:
            # Pure-coverage gap (no crashes, no failed accessions —
            # just unresolved CUSIPs). Surface the count as the only
            # signal so the operator can correlate with #740.
            error_parts.append(f"{rows_skipped} holdings skipped — CUSIPs unresolved (#740)")
        if deadline_hit:
            # Surface the deadline interruption explicitly so the
            # operator knows the next sweep should resume the tail.
            error_parts.append(f"deadline reached after {filers_attempted}/{len(ciks)} filers")
        if cancelled_by_operator:
            error_parts.append(f"cancelled by operator after {filers_attempted}/{len(ciks)} filers")
        finish_ingestion_run(
            conn,
            run_id=run_id,
            status=status,
            rows_upserted=rows_upserted,
            rows_skipped=rows_skipped,
            error="; ".join(error_parts) or None,
        )
        conn.commit()

    if cancelled_by_operator:
        # Raise after bookkeeping so data_ingestion_runs records the
        # partial state. The orchestrator's _run_one_stage catches
        # BootstrapStageCancelled and writes a ``cancelled`` row to
        # bootstrap_stages (PR3c #1093 status); outside bootstrap the
        # contextvar guard means we never enter this branch.
        from app.services.bootstrap_state import BootstrapStageCancelled

        raise BootstrapStageCancelled(
            f"13F quarterly sweep cancelled by operator after {filers_attempted}/{len(ciks)} filers",
            stage_key="sec_13f_quarterly_sweep",
        )

    return summaries


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@dataclass
class _MutableSummary:
    cik: str
    accessions_seen: int = 0
    accessions_ingested: int = 0
    accessions_failed: int = 0
    holdings_inserted: int = 0
    holdings_skipped_no_cusip: int = 0
    first_error: str | None = None

    def to_immutable(self) -> IngestSummary:
        return IngestSummary(
            filer_cik=self.cik,
            accessions_seen=self.accessions_seen,
            accessions_ingested=self.accessions_ingested,
            accessions_failed=self.accessions_failed,
            holdings_inserted=self.holdings_inserted,
            holdings_skipped_no_cusip=self.holdings_skipped_no_cusip,
            first_error=self.first_error,
        )


def _ingest_single_accession(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    filer_cik: str,
    ref: AccessionRef,
) -> _AccessionOutcome:
    """Per-accession driver. Never raises — every fetch / parse
    failure resolves to a ``_AccessionOutcome`` with status='failed'
    so a single malformed accession does not abort the filer batch.
    """
    base_url = _archive_file_url(filer_cik, ref.accession_number, "")  # …/{accn}/
    index_url = base_url + "index.json"

    index_payload = sec.fetch_document_text(index_url)
    if index_payload is None:
        logger.info(
            "13F ingest: index.json 404/error for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            error="archive index.json fetch failed",
        )

    primary_name, infotable_name = parse_archive_index(index_payload)
    if primary_name is None or infotable_name is None:
        logger.warning(
            "13F ingest: archive index missing primary_doc / infotable for cik=%s accession=%s "
            "(primary=%s, infotable=%s)",
            filer_cik,
            ref.accession_number,
            primary_name,
            infotable_name,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            error=(f"archive index missing files (primary={primary_name!r}, infotable={infotable_name!r})"),
        )

    primary_url = _archive_file_url(filer_cik, ref.accession_number, primary_name)
    infotable_url = _archive_file_url(filer_cik, ref.accession_number, infotable_name)

    # Raw-payload retention (operator audit 2026-05-03 + PR #808):
    # both fetched bodies are persisted to ``filing_raw_documents``
    # IMMEDIATELY after successful fetch, BEFORE parsing. That way:
    #
    #   * A parser bug discovered later can re-wash from the stored
    #     body without re-fetching SEC at 10 req/sec.
    #   * If parsing raises, we still have the body for diagnostic.
    #   * Re-fetches (amended filings) overwrite via ON CONFLICT —
    #     the new body is always authoritative.
    #
    # Prior contract was "every structured field from the upstream
    # document lands in SQL" via the upserts below — that holds, but
    # is no longer the only re-wash path.
    primary_xml = sec.fetch_document_text(primary_url)
    if primary_xml is None:
        logger.warning(
            "13F ingest: primary_doc.xml 404/error for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            error="primary_doc.xml fetch failed",
        )
    raw_filings.store_raw(
        conn,
        accession_number=ref.accession_number,
        document_kind="primary_doc",
        payload=primary_xml,
        parser_version=_PARSER_VERSION_13F_PRIMARY,
        source_url=primary_url,
    )
    # Commit the raw row immediately so a later parse failure that
    # propagates up to the outer filer loop's rollback can't take
    # the just-stored body down with it. Codex pre-push review caught
    # this — without the commit, the rollback at the per-filer
    # exception handler discards the raw row exactly when we need it
    # most (parse failed → re-wash needs the body).
    conn.commit()

    try:
        info = parse_primary_doc(primary_xml)
    except (ValueError, ET.ParseError) as exc:
        logger.exception(
            "13F ingest: primary_doc.xml parse failed for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            error=f"primary_doc.xml parse failed: {exc}",
        )

    infotable_xml = sec.fetch_document_text(infotable_url)
    if infotable_xml is None:
        logger.warning(
            "13F ingest: infotable.xml 404/error for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            error="infotable.xml fetch failed",
        )
    raw_filings.store_raw(
        conn,
        accession_number=ref.accession_number,
        document_kind="infotable_13f",
        payload=infotable_xml,
        parser_version=_PARSER_VERSION_13F_INFOTABLE,
        source_url=infotable_url,
    )
    conn.commit()

    try:
        holdings = parse_infotable(infotable_xml)
    except (ValueError, ET.ParseError) as exc:
        logger.exception(
            "13F ingest: infotable.xml parse failed for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            error=f"infotable.xml parse failed: {exc}",
        )
    filer_id = _upsert_filer(conn, info)

    if not holdings:
        # Empty 13F-HR is legal (filer reported exempt-list /
        # cancellation). Recorded as success so re-runs skip it.
        logger.info(
            "13F ingest: empty infotable for cik=%s accession=%s — filer recorded, no holdings",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="success",
            holdings_inserted=0,
            holdings_skipped_no_cusip=0,
            error=None,
        )

    inserted = 0
    skipped_no_cusip = 0
    period = info.period_of_report
    filed_at = info.filed_at
    if filed_at is None:
        # Fall back to submissions-index filing date when
        # primary_doc.xml had no signature block. Tag UTC
        # explicitly — filed_at is TIMESTAMPTZ and a naive
        # datetime would drift to the server's local zone on
        # write.
        filed_at = ref.filed_at or datetime(period.year, period.month, period.day, tzinfo=UTC)

    # Codex review (#889): dedupe by (instrument_id, exposure) to match
    # the DB unique-key collapse behavior. ``_upsert_holding`` does
    # ON CONFLICT DO NOTHING on (accession, instrument, COALESCE(is_put_call,
    # 'EQUITY')); duplicate XML rows after the first are silently dropped
    # at the DB. Mirror that here so observations record only the first
    # parsed row per (instrument, exposure) — the row that actually lives
    # in the typed table.
    resolved_by_key: dict[tuple[int, str], tuple[int, ThirteenFHolding]] = {}
    for holding in holdings:
        instrument_id = _resolve_cusip_to_instrument_id(conn, holding.cusip)
        if instrument_id is None:
            skipped_no_cusip += 1
            # Track the unresolved CUSIP so the resolver service
            # (#781) can fuzzy-match against ``instruments.
            # company_name`` later without re-fetching the SEC
            # archive. Idempotent — re-encountering the same CUSIP
            # increments ``observation_count`` and refreshes the
            # latest filer-supplied issuer name.
            _record_unresolved_cusip(
                conn,
                cusip=holding.cusip,
                name_of_issuer=holding.name_of_issuer,
                accession_number=ref.accession_number,
            )
            continue
        if _upsert_holding(
            conn,
            filer_id=filer_id,
            instrument_id=instrument_id,
            accession_number=ref.accession_number,
            period_of_report=period,
            filed_at=filed_at,
            holding=holding,
        ):
            inserted += 1
        # Dedupe key matches the DB unique key:
        # (instrument_id, COALESCE(is_put_call, 'EQUITY')).
        # Bot review: the ``instrument_id is None`` branch above always
        # ``continue``s, so by this point instrument_id is non-None.
        # Assert for static-analysis clarity + belt-and-braces against
        # a future control-flow refactor that drops the continue.
        assert instrument_id is not None  # noqa: S101
        exposure_key = holding.put_call if holding.put_call in ("PUT", "CALL") else "EQUITY"
        resolved_by_key.setdefault((instrument_id, exposure_key), (instrument_id, holding))

    resolved_holdings: list[tuple[int, ThirteenFHolding]] = list(resolved_by_key.values())

    # Write-through observations + refresh _current (#889 / spec
    # §"Eliminate periodic re-scan jobs"). Replaces the legacy nightly
    # ownership_observations_sync.sync_institutions read-from-typed-
    # tables path. record_institution_observation is itself UPSERT so
    # re-ingest of the same accession (parser bump, manifest rebuild)
    # refreshes existing rows in place — no need to gate on
    # ``inserted``.
    if resolved_holdings:
        _record_13f_observations_for_filing(
            conn,
            filer_id=filer_id,
            accession_number=ref.accession_number,
            period_of_report=period,
            filed_at=filed_at,
            resolved_holdings=resolved_holdings,
        )
        # Dedupe touched instruments → one refresh per unique
        # instrument. A single 13F can carry 1000+ holdings; refreshing
        # per-row would be O(N²). The set collapses to the count of
        # distinct issuers held.
        for unique_instrument_id in {iid for iid, _ in resolved_holdings}:
            refresh_institutions_current(conn, instrument_id=unique_instrument_id)

    # Promote to 'partial' when at least one holding was dropped due
    # to an unresolved CUSIP. The accession itself is recorded so
    # re-runs skip it; once the #740 backfill closes the CUSIP gap,
    # the operator can clear the log row to force a re-ingest of
    # those accessions.
    status = "partial" if skipped_no_cusip > 0 else "success"
    error = f"{skipped_no_cusip} unresolved CUSIPs (gated by #740 backfill)" if skipped_no_cusip > 0 else None
    return _AccessionOutcome(
        status=status,
        holdings_inserted=inserted,
        holdings_skipped_no_cusip=skipped_no_cusip,
        error=error,
    )


# ---------------------------------------------------------------------------
# Iterators (exposed for ad-hoc reporting / debug)
# ---------------------------------------------------------------------------


def iter_filer_holdings(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
    limit: int = 1000,
) -> Iterator[dict[str, Any]]:
    """Yield the most recent holdings for one filer. Used by the
    PR 4 reader API + the admin CLI; exposed here for symmetry with
    ingest path."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT h.accession_number, h.period_of_report, h.shares,
                   h.market_value_usd, h.voting_authority, h.is_put_call,
                   h.filed_at, i.symbol, i.company_name
            FROM institutional_holdings h
            JOIN institutional_filers f USING (filer_id)
            JOIN instruments i ON i.instrument_id = h.instrument_id
            WHERE f.cik = %(cik)s
            ORDER BY h.period_of_report DESC, h.market_value_usd DESC NULLS LAST
            LIMIT %(limit)s
            """,
            {"cik": _zero_pad_cik(filer_cik), "limit": limit},
        )
        for row in cur.fetchall():
            yield dict(row)
