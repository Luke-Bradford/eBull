"""SEC DEF 14A ingester (#769 PR 2 of N).

Walks ``filing_events`` for DEF 14A accessions, fetches each
filing's primary document, parses the beneficial-ownership table
via :mod:`app.providers.implementations.sec_def14a`, and persists
each holder row to ``def14a_beneficial_holdings``. Idempotent
re-ingest is guaranteed by the
``(accession_number, holder_name)`` UNIQUE INDEX from migration
097; ``ON CONFLICT DO UPDATE`` lets a re-parse with improved role
inference promote the existing row.

The drift-detector job (PR 3) is separate — this module only owns
the parse-and-persist path. The drift detector reads
``def14a_beneficial_holdings`` and ``insider_transactions`` and
writes flags to the ops monitor.

Tombstone semantics mirror the institutional / blockholder
ingesters: every accession we *attempt* — success, partial (no
recognisable table), failed (404 / parse error) — writes a row to
``def14a_ingest_log``. The next run skips already-attempted
accessions; the operator clears log rows to force retry.

Discovery selector reads from ``filing_events`` rather than walking
SEC archive indexes directly because the SEC ingest pipeline (#262)
already populates DEF 14A rows with ``primary_document_url`` and
``instrument_id`` — re-walking the archive would duplicate work and
risk drift between the two indexers.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET  # noqa: S405 — only used to catch ET.ParseError; no untrusted input parsed here.
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import uuid4

import psycopg
import psycopg.rows

from app.providers.implementations.sec_def14a import (
    Def14ABeneficialHolder,
    Def14ABeneficialOwnershipTable,
    extract_plan_name_and_trustee,
    parse_beneficial_ownership_table,
)
from app.services import raw_filings
from app.services.fundamentals import finish_ingestion_run, start_ingestion_run
from app.services.ownership_observations import (
    record_def14a_observation,
    record_esop_observation,
    refresh_def14a_current,
    refresh_esop_current,
)

_PARSER_VERSION_DEF14A = "def14a-v1"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider contract
# ---------------------------------------------------------------------------


class SecDocFetcher(Protocol):
    """Subset of the SEC EDGAR provider this ingester relies on.

    Matches the contract used by :mod:`app.services.business_summary`
    and :mod:`app.services.blockholders` so the production binding
    (:class:`app.providers.implementations.sec_edgar.SecEdgarProvider`)
    drops in without an adapter.
    """

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


# DEF 14A and DEFA14A both list beneficial ownership; DEFM14A
# (merger proxies) typically don't but a few large-cap mergers
# include the table for the surviving entity. ``DEFR14A`` (revised
# definitive proxy, #939) is the amendment-style proxy that the
# manifest now classifies as ``sec_def14a`` — keep this set in lock-
# step with ``_FORM_TO_SOURCE`` in ``app.services.sec_manifest`` so
# the ingester sees every accession the manifest enqueues. The parser
# tombstones any accession whose body has no recognisable table.
_DEF14A_FORM_TYPES: frozenset[str] = frozenset(("DEF 14A", "DEFA14A", "DEFM14A", "DEFR14A"))


@dataclass(frozen=True)
class AccessionRef:
    """One DEF 14A accession to ingest. Sourced from
    ``filing_events`` (provider='sec', filing_type IN _DEF14A_FORM_TYPES).
    """

    accession_number: str
    instrument_id: int
    filing_date: date
    primary_document_url: str | None


@dataclass(frozen=True)
class IngestSummary:
    """Per-batch rollup of one ingest pass.

    ``rows_inserted`` counts holder-row INSERTs; ``rows_updated``
    counts ON-CONFLICT promotions of existing rows (re-parse with
    improved role inference). Both contribute to the ops monitor's
    "rows touched" gauge but only inserts move the operator-facing
    coverage chip.

    ``accessions_partial`` is tracked separately from
    ``accessions_succeeded`` so a run consisting entirely of
    notice-only / no-table tombstones downgrades the
    ``data_ingestion_runs.status`` to ``partial`` rather than
    silently reporting ``success``. Codex pre-push review caught
    this on PR review.
    """

    accessions_seen: int
    accessions_succeeded: int
    accessions_partial: int
    accessions_failed: int
    rows_inserted: int
    rows_updated: int
    first_error: str | None = None

    @property
    def accessions_ingested(self) -> int:
        """Backwards-compatible counter — the legacy "ingested"
        bucket sums everything that reached the persistence layer
        (``success`` + ``partial``). Most call sites should prefer
        the explicit ``accessions_succeeded`` /
        ``accessions_partial`` fields; ``accessions_ingested`` is
        retained for the API surface that tests assert against."""
        return self.accessions_succeeded + self.accessions_partial


@dataclass(frozen=True)
class _AccessionOutcome:
    status: str  # 'success' | 'partial' | 'failed'
    rows_inserted: int
    rows_updated: int
    error: str | None
    # Resolved issuer CIK (or _CIK_MISSING_SENTINEL when no
    # ``instrument_sec_profile`` row exists). Threaded out of the
    # per-accession driver so the outer loop's tombstone log write
    # does not re-issue the same lookup. Bot review of the first
    # PR draft caught the double DB round-trip on every success.
    issuer_cik: str


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def discover_pending_def14a(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int | None = None,
    limit: int = 100,
) -> list[AccessionRef]:
    """Return DEF 14A accessions in ``filing_events`` that have not
    yet been attempted (no row in ``def14a_ingest_log``).

    Filters on ``filing_type IN _DEF14A_FORM_TYPES`` and
    ``primary_document_url IS NOT NULL`` — accessions without a
    fetchable URL are skipped (the SEC ingest pipeline backfills
    those separately and we don't want to tombstone a row that may
    still get a URL on the next sync).

    Ordered ``filing_date DESC`` so the most recent proxies parse
    first — operators care most about current-year ownership
    snapshots.

    ``instrument_id`` filter scopes the discovery to a single
    issuer (used by ad-hoc re-ingest scripts and the per-instrument
    backfill in PR 3); ``None`` returns the full pending set.
    """
    where_iid = "AND fe.instrument_id = %(iid)s" if instrument_id is not None else ""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT fe.provider_filing_id, fe.instrument_id, fe.filing_date,
                   fe.primary_document_url
            FROM filing_events fe
            LEFT JOIN def14a_ingest_log log
                ON log.accession_number = fe.provider_filing_id
            WHERE fe.provider = 'sec'
              AND fe.filing_type = ANY(%(forms)s)
              AND fe.primary_document_url IS NOT NULL
              AND log.accession_number IS NULL
              {where_iid}
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %(limit)s
            """,
            {
                "forms": list(_DEF14A_FORM_TYPES),
                "iid": instrument_id,
                "limit": limit,
            },
        )
        rows = cur.fetchall()

    return [
        AccessionRef(
            accession_number=str(r["provider_filing_id"]),  # type: ignore[arg-type]
            instrument_id=int(r["instrument_id"]),  # type: ignore[arg-type]
            filing_date=r["filing_date"],  # type: ignore[arg-type]
            primary_document_url=str(r["primary_document_url"]) if r["primary_document_url"] is not None else None,  # type: ignore[arg-type]
        )
        for r in rows
    ]


def _resolve_issuer_cik(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
) -> str | None:
    """Look up the issuer's CIK via ``instrument_sec_profile``.

    Returns ``None`` when no profile row exists. The ingester still
    persists the holder rows in that case (with ``issuer_cik`` set
    to a sentinel ``"CIK-MISSING"`` so the schema's NOT NULL
    constraint is satisfied) — the audit value isn't strictly
    needed for downstream reads but the column is required by the
    schema. PR 3's drift detector ignores rows with the sentinel.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cik FROM instrument_sec_profile WHERE instrument_id = %s LIMIT 1",
            (instrument_id,),
        )
        row = cur.fetchone()
    return str(row[0]) if row is not None else None


def _record_ingest_attempt(
    conn: psycopg.Connection[tuple],
    *,
    accession_number: str,
    issuer_cik: str,
    status: str,
    rows_inserted: int = 0,
    rows_skipped: int = 0,
    error: str | None = None,
) -> None:
    """Idempotent upsert into ``def14a_ingest_log``."""
    conn.execute(
        """
        INSERT INTO def14a_ingest_log (
            accession_number, issuer_cik, status,
            rows_inserted, rows_skipped, error
        ) VALUES (
            %(accession)s, %(cik)s, %(status)s,
            %(inserted)s, %(skipped)s, %(error)s
        )
        ON CONFLICT (accession_number) DO UPDATE SET
            status = EXCLUDED.status,
            rows_inserted = EXCLUDED.rows_inserted,
            rows_skipped = EXCLUDED.rows_skipped,
            error = EXCLUDED.error,
            fetched_at = NOW()
        """,
        {
            "accession": accession_number,
            "cik": issuer_cik,
            "status": status,
            "inserted": rows_inserted,
            "skipped": rows_skipped,
            "error": error,
        },
    )


def _upsert_holding(
    conn: psycopg.Connection[tuple],
    *,
    accession_number: str,
    issuer_cik: str,
    instrument_id: int,
    as_of_date: date | None,
    holder: Def14ABeneficialHolder,
) -> str:
    """UPSERT one ``def14a_beneficial_holdings`` row.

    Returns ``'inserted'`` when the row was new, ``'updated'`` when
    it promoted an existing row (e.g. re-parse with improved role
    inference). The schema's UNIQUE INDEX is keyed on
    ``(accession_number, holder_name)`` and excludes role on
    purpose (#769 PR 1 review fix), so role updates flow through
    the conflict path cleanly.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                instrument_id, accession_number, issuer_cik,
                holder_name, holder_role, shares, percent_of_class,
                as_of_date
            ) VALUES (
                %(iid)s, %(accession)s, %(cik)s,
                %(name)s, %(role)s, %(shares)s, %(pct)s,
                %(as_of)s
            )
            ON CONFLICT (accession_number, holder_name) DO UPDATE SET
                instrument_id = EXCLUDED.instrument_id,
                issuer_cik = EXCLUDED.issuer_cik,
                holder_role = EXCLUDED.holder_role,
                shares = EXCLUDED.shares,
                percent_of_class = EXCLUDED.percent_of_class,
                as_of_date = EXCLUDED.as_of_date,
                fetched_at = NOW()
            RETURNING (xmax = 0) AS inserted
            """,
            {
                "iid": instrument_id,
                "accession": accession_number,
                "cik": issuer_cik,
                "name": holder.holder_name,
                "role": holder.holder_role,
                "shares": holder.shares,
                "pct": holder.percent_of_class,
                "as_of": as_of_date,
            },
        )
        row = cur.fetchone()
    # ``xmax = 0`` is true on a fresh INSERT, false when an UPDATE
    # path fires under ON CONFLICT. Standard psycopg recipe for
    # disambiguating insert vs update on UPSERT.
    assert row is not None
    return "inserted" if row["inserted"] else "updated"


# ---------------------------------------------------------------------------
# Per-accession driver
# ---------------------------------------------------------------------------


_CIK_MISSING_SENTINEL = "CIK-MISSING"


def _ingest_single_accession(
    conn: psycopg.Connection[tuple],
    fetcher: SecDocFetcher,
    *,
    ref: AccessionRef,
) -> _AccessionOutcome:
    """Per-accession driver. Never raises — every fetch / parse
    failure resolves to an ``_AccessionOutcome`` with status='failed'
    so a single malformed accession does not abort the batch.

    Catches ``ET.ParseError`` alongside ``ValueError`` (the parser
    itself doesn't raise but the underlying tag-walker can on truly
    malformed input). Same defensive shape as
    :mod:`app.services.blockholders`.
    """
    # Resolve the issuer CIK once, up-front. Every outcome path
    # (success, partial, failed) carries it on the returned
    # ``_AccessionOutcome`` so the outer loop's tombstone log write
    # never has to re-issue the lookup. Bot review caught the
    # double round-trip on the success path.
    issuer_cik = _resolve_issuer_cik(conn, instrument_id=ref.instrument_id) or _CIK_MISSING_SENTINEL

    if ref.primary_document_url is None:
        # Should be filtered out by the discovery query, but defensive
        # in case a caller passes an ad-hoc ref with no URL.
        return _AccessionOutcome(
            status="failed",
            rows_inserted=0,
            rows_updated=0,
            error="primary_document_url is NULL",
            issuer_cik=issuer_cik,
        )

    body = fetcher.fetch_document_text(ref.primary_document_url)
    if body is None:
        logger.info(
            "DEF 14A ingest: primary doc 404/error for accession=%s url=%s",
            ref.accession_number,
            ref.primary_document_url,
        )
        return _AccessionOutcome(
            status="failed",
            rows_inserted=0,
            rows_updated=0,
            error="primary doc fetch failed",
            issuer_cik=issuer_cik,
        )
    # Persist raw body BEFORE parsing — re-wash workflows depend on
    # this row even if parsing fails. Operator audit 2026-05-03 +
    # PR #808 contract. Commit immediately so a later per-accession
    # exception that triggers the outer ``conn.rollback()`` cannot
    # take this row down with it (Codex pre-push review).
    raw_filings.store_raw(
        conn,
        accession_number=ref.accession_number,
        document_kind="def14a_body",
        payload=body,
        parser_version=_PARSER_VERSION_DEF14A,
        source_url=ref.primary_document_url,
    )
    conn.commit()

    try:
        parsed: Def14ABeneficialOwnershipTable = parse_beneficial_ownership_table(body)
    except (ValueError, ET.ParseError) as exc:
        logger.exception(
            "DEF 14A ingest: parse failed for accession=%s",
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            rows_inserted=0,
            rows_updated=0,
            error=f"parse failed: {exc}",
            issuer_cik=issuer_cik,
        )

    if not parsed.rows:
        # Parser couldn't confidently identify the beneficial-ownership
        # table. Tombstone with status=partial so the next run skips
        # this accession but the operator can clear the log row to
        # force a retry once the parser is improved. Surface the
        # diagnostic score so the ops monitor can correlate.
        return _AccessionOutcome(
            status="partial",
            rows_inserted=0,
            rows_updated=0,
            error=f"no beneficial-ownership table identified (best_score={parsed.raw_table_score})",
            issuer_cik=issuer_cik,
        )

    inserted = 0
    updated = 0

    for holder in parsed.rows:
        outcome = _upsert_holding(
            conn,
            accession_number=ref.accession_number,
            issuer_cik=issuer_cik,
            instrument_id=ref.instrument_id,
            as_of_date=parsed.as_of_date,
            holder=holder,
        )
        if outcome == "inserted":
            inserted += 1
        else:
            updated += 1

    # Write-through observations + refresh _current (#891 / spec
    # §"Eliminate periodic re-scan jobs"). Replaces nightly
    # ownership_observations_sync.sync_def14a read-from-typed-tables
    # path. record_def14a_observation is itself UPSERT so re-ingest
    # of the same accession (parser bump) refreshes existing rows
    # in place.
    if parsed.rows:
        _record_def14a_observations_for_filing(
            conn,
            instrument_id=ref.instrument_id,
            accession_number=ref.accession_number,
            as_of_date=parsed.as_of_date,
            holders=parsed.rows,
        )
        refresh_def14a_current(conn, instrument_id=ref.instrument_id)
        # ESOP write-through (#843). Mirrors the def14a write-through
        # above but lands rows in ``ownership_esop_observations`` for
        # the dedicated funds-slice overlay path (#961). Same accession
        # / as_of semantics so the two slices reconcile against one
        # provenance.
        esop_rows_written = _record_esop_observations_for_filing(
            conn,
            instrument_id=ref.instrument_id,
            accession_number=ref.accession_number,
            as_of_date=parsed.as_of_date,
            holders=parsed.rows,
        )
        if esop_rows_written > 0:
            refresh_esop_current(conn, instrument_id=ref.instrument_id)

    return _AccessionOutcome(
        status="success",
        rows_inserted=inserted,
        rows_updated=updated,
        error=None,
        issuer_cik=issuer_cik,
    )


def _record_def14a_observations_for_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    as_of_date: date | None,
    holders: list[Def14ABeneficialHolder],
) -> None:
    """Record one ``ownership_def14a_observations`` row per holder
    on this DEF 14A accession.

    Mirrors the legacy batch-sync rule in
    ``ownership_observations_sync.sync_def14a``:

      - Filter: ``shares IS NOT NULL`` (the typed table requires it
        and the legacy batch query enforces).
      - ``ownership_nature``: pinned to ``'beneficial'`` (DEF 14A's
        canonical table reports beneficial ownership per Rule 13d-3).
      - ``period_end``: ``as_of_date`` when present, else falls back
        to ``fetched_at.date()`` — matches the legacy
        ``sync_def14a`` rule (``as_of_date OR fetched_at.date()``).
        Codex pre-push review flagged this divergence — period_end
        is part of the DEF 14A observation conflict key, so any
        difference between legacy + inline produces a different
        observation identity.
      - ``filed_at``: ``fetched_at`` from the row we just wrote (the
        column default is ``NOW()`` so this is current-transaction
        wall clock). Same value the legacy batch would have read.
      - Identity: ``holder_name`` (normalised by the observations
        layer via ``holder_name_key`` GENERATED column). DEF 14A
        rows don't carry holder CIK; CIK match happens at rollup-read
        time.
    """
    fetched_at = datetime.now(tz=UTC)
    # Match legacy sync_def14a:
    #   filed_at = row.fetched_at OR (as_of midnight UTC fallback)
    #   period_end = as_of_date OR row.fetched_at.date()
    # Inline path: ``fetched_at`` is current wall-clock (the typed
    # row we just wrote has the same value via column default).
    period_end: date = as_of_date or fetched_at.date()
    filed_at = fetched_at
    run_id = uuid4()
    for holder in holders:
        if holder.shares is None:
            continue
        if not holder.holder_name or not holder.holder_name.strip():
            continue
        # ESOP-role rows write through to ownership_esop_observations
        # via _record_esop_observations_for_filing INSTEAD of the
        # general def14a observations path. Routing them through both
        # would double-count in the rollup: the insider/blockholder
        # def14a slice would surface the plan AND the dedicated
        # funds-slice ESOP overlay (#961) would tag the matching
        # fund row. Codex pre-push review (#843) caught this.
        if holder.holder_role == "esop":
            continue
        record_def14a_observation(
            conn,
            instrument_id=instrument_id,
            holder_name=holder.holder_name,
            holder_role=holder.holder_role,
            ownership_nature="beneficial",
            source="def14a",
            source_document_id=accession_number,
            source_accession=accession_number,
            source_field=None,
            source_url=None,
            filed_at=filed_at,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal(holder.shares),
            percent_of_class=Decimal(holder.percent_of_class) if holder.percent_of_class is not None else None,
        )


def _record_esop_observations_for_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    as_of_date: date | None,
    holders: list[Def14ABeneficialHolder],
) -> int:
    """Record one ``ownership_esop_observations`` row per
    ``holder_role='esop'`` row from this DEF 14A accession (#843).

    Returns the number of ESOP rows written so the caller can decide
    whether to call ``refresh_esop_current`` (skip the refresh + its
    advisory lock when the filing has zero ESOP rows — the common case
    for large-cap issuers whose plans don't cross the 5% threshold).

    ``plan_trustee_cik`` is left NULL — DEF 14A's trustee name (e.g.
    ``"Vanguard Fiduciary Trust Company"``) is a SEPARATE corporate
    entity from the fund-trust CIKs in ``sec_nport_filer_directory``
    (e.g. ``"VANGUARD INDEX FUNDS"``). Resolving trustee→CIK requires
    a fuzzy name match or a curated alias table; #961 (the funds-slice
    ESOP overlay consumer) is the right layer to build that — this
    layer just persists the trustee_name string for downstream lookup.

    Mirrors ``_record_def14a_observations_for_filing`` for
    period_end / filed_at semantics so the two slices reconcile
    against the same provenance.
    """
    fetched_at = datetime.now(tz=UTC)
    period_end: date = as_of_date or fetched_at.date()
    filed_at = fetched_at
    run_id = uuid4()
    written = 0
    for holder in holders:
        if holder.holder_role != "esop":
            continue
        if holder.shares is None or holder.shares <= 0:
            continue
        plan_name, trustee_name = extract_plan_name_and_trustee(holder.holder_name)
        if not plan_name:
            continue
        record_esop_observation(
            conn,
            instrument_id=instrument_id,
            plan_name=plan_name,
            plan_trustee_name=trustee_name,
            plan_trustee_cik=None,
            source_document_id=accession_number,
            source_accession=accession_number,
            source_field=None,
            source_url=None,
            filed_at=filed_at,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal(holder.shares),
            percent_of_class=Decimal(holder.percent_of_class) if holder.percent_of_class is not None else None,
        )
        written += 1
    return written


# ---------------------------------------------------------------------------
# Public batch entry point
# ---------------------------------------------------------------------------


def ingest_def14a(
    conn: psycopg.Connection[tuple],
    fetcher: SecDocFetcher,
    *,
    instrument_id: int | None = None,
    limit: int = 100,
    prefetch_urls: bool = False,
    prefetch_user_agent: str | None = None,
) -> IngestSummary:
    """Discover pending DEF 14A accessions and ingest each.

    ``instrument_id=None`` ingests across all instruments;
    otherwise scopes to one. ``limit`` caps the number of
    accessions per call so a long-tail backfill cannot run forever
    against the SEC fair-use rate budget.

    Commits per-accession so a mid-batch crash leaves a partial
    persistent state (rows for the accessions already attempted +
    matching log entries). Mirrors the institutional-holdings
    ingester's commit cadence.
    """
    pending = discover_pending_def14a(conn, instrument_id=instrument_id, limit=limit)
    if not pending:
        return IngestSummary(
            accessions_seen=0,
            accessions_succeeded=0,
            accessions_partial=0,
            accessions_failed=0,
            rows_inserted=0,
            rows_updated=0,
        )

    # #1045 fast path: prefetch the cohort's primary documents via the
    # pipelined fetcher (4-way concurrent at the shared 7 req/s ceiling).
    # Wraps the sync fetcher so per-filing fetch_document_text reads
    # from cache when available; cache misses fall back to the
    # underlying fetcher transparently.
    if prefetch_urls:
        from app.services.sec_pipelined_fetcher import _CachedDocFetcher, prefetch_document_texts

        urls = [ref.primary_document_url for ref in pending if ref.primary_document_url]
        if urls:
            ua = prefetch_user_agent or "eBull research/1.0"
            cache = prefetch_document_texts(urls, user_agent=ua)
            fetcher = _CachedDocFetcher(fetcher, cache)  # type: ignore[assignment]

    run_id = start_ingestion_run(
        conn,
        source="sec_edgar_def14a",
        endpoint="filing_events / def14a primary doc",
        instrument_count=len(pending),
    )
    conn.commit()

    accessions_seen = len(pending)
    accessions_succeeded = 0
    accessions_partial = 0
    accessions_failed = 0
    rows_inserted = 0
    rows_updated = 0
    first_error: str | None = None
    crash_error: str | None = None

    try:
        for ref in pending:
            # Per-accession crash isolation wraps the FULL block —
            # parse, log write, commit. A DB error during the
            # tombstone or the commit must not abort the rest of the
            # batch. Codex pre-push review caught the prior version
            # which had only ``_ingest_single_accession`` inside the
            # try.
            try:
                outcome = _ingest_single_accession(conn, fetcher, ref=ref)
                # Issuer CIK travels on the outcome — it was
                # resolved once inside the per-accession driver
                # so the log write does not re-issue the lookup.
                _record_ingest_attempt(
                    conn,
                    accession_number=ref.accession_number,
                    issuer_cik=outcome.issuer_cik,
                    status=outcome.status,
                    rows_inserted=outcome.rows_inserted,
                    rows_skipped=0,
                    error=outcome.error,
                )
                conn.commit()
            except Exception as exc:  # noqa: BLE001 — per-accession crash must not abort batch
                logger.exception("DEF 14A ingest: accession %s raised; continuing batch", ref.accession_number)
                crash_error = f"{ref.accession_number}: {exc}"
                conn.rollback()
                # Tombstone the accession in a fresh transaction so
                # the bootstrap drain doesn't rediscover and re-crash
                # on the same row every chunk for the entire deadline.
                # Codex pre-push review for #839 caught the prior gap:
                # rolled-back accessions stayed PENDING (no log row),
                # so the next discovery query returned them again. In
                # bootstrap mode that wasted SEC calls + clock for
                # nothing. Use 'failed' status so an operator can clear
                # the row to retry once the underlying bug is fixed.
                # Always count the failure FIRST so the
                # ``seen == succeeded + partial + failed`` invariant
                # holds even on double-fault (crash + tombstone write
                # also fails). Bot review for #839 PR #850 caught the
                # prior version which only counted on successful
                # tombstone — a double-fault silently dropped the
                # accession from accounting and the bootstrap audit
                # trail couldn't be reconciled.
                accessions_failed += 1
                if first_error is None:
                    first_error = f"{ref.accession_number} (crash): {exc}"
                try:
                    _record_ingest_attempt(
                        conn,
                        accession_number=ref.accession_number,
                        issuer_cik="CIK-CRASH",  # canonical sentinel — no CIK lookup possible after rollback
                        status="failed",
                        rows_inserted=0,
                        rows_skipped=0,
                        error=f"crash: {type(exc).__name__}: {exc}",
                    )
                    conn.commit()
                except Exception:  # noqa: BLE001 — tombstone failure shouldn't abort batch
                    logger.exception(
                        "DEF 14A ingest: failed to tombstone crash for %s; row stays pending",
                        ref.accession_number,
                    )
                    conn.rollback()
                    # Still counted as failed above; the audit log
                    # just doesn't carry the row. Operator drains via
                    # log inspection + manual re-trigger.
                continue

            if outcome.status == "success":
                accessions_succeeded += 1
            elif outcome.status == "partial":
                accessions_partial += 1
                if outcome.error and first_error is None:
                    first_error = f"{ref.accession_number}: {outcome.error}"
            else:  # 'failed'
                accessions_failed += 1
                if outcome.error and first_error is None:
                    first_error = f"{ref.accession_number}: {outcome.error}"
            rows_inserted += outcome.rows_inserted
            rows_updated += outcome.rows_updated
    finally:
        # Status precedence:
        #   * any per-accession crash with zero persisted progress
        #     (no succeeded AND no partial — every attempt rolled
        #     back) -> failed
        #   * any crash / failure / partial -> partial
        #     (partial counts because a run that only tombstones
        #     no-table proxies is degraded, not success — the
        #     operator-facing run audit must surface that)
        #   * else -> success
        # A partial accession committed its tombstone row before any
        # later crash, so it represents persisted progress and the
        # batch should NOT report ``failed`` on its account. Codex
        # pre-push review caught the prior gate that ignored partial.
        if crash_error and accessions_succeeded == 0 and accessions_partial == 0:
            status = "failed"
        elif crash_error or accessions_failed > 0 or accessions_partial > 0:
            status = "partial"
        else:
            status = "success"
        error_parts: list[str] = []
        if crash_error:
            error_parts.append(f"crash: {crash_error}")
        if accessions_partial > 0:
            error_parts.append(f"{accessions_partial} accession(s) tombstoned partial (no recognisable table)")
        if first_error:
            error_parts.append(f"first: {first_error}")
        finish_ingestion_run(
            conn,
            run_id=run_id,
            status=status,
            rows_upserted=rows_inserted + rows_updated,
            rows_skipped=0,
            error="; ".join(error_parts) or None,
        )
        conn.commit()

    return IngestSummary(
        accessions_seen=accessions_seen,
        accessions_succeeded=accessions_succeeded,
        accessions_partial=accessions_partial,
        accessions_failed=accessions_failed,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        first_error=first_error,
    )


# ---------------------------------------------------------------------------
# Bootstrap drain (#839 — operator audit found def14a_beneficial_holdings empty)
# ---------------------------------------------------------------------------


def bootstrap_def14a(
    conn: psycopg.Connection[tuple],
    fetcher: SecDocFetcher,
    *,
    chunk_limit: int = 500,
    max_runtime_seconds: int = 3600,
    prefetch_urls: bool = False,
    prefetch_user_agent: str | None = None,
) -> IngestSummary:
    """One-shot drain of the entire DEF 14A candidate set.

    Calls :func:`ingest_def14a` repeatedly with a chunked limit until
    either the candidate query returns zero rows or the runtime
    deadline elapses. Mirrors the
    :func:`app.services.business_summary.bootstrap_business_summaries`
    pattern: idempotent — safe to re-run; subsequent invocations
    no-op fast once every accession has a row in ``def14a_ingest_log``.

    Designed for first-time backfill of the SEC DEF 14A universe
    (#839). Operator audit 2026-05-03 found
    ``def14a_beneficial_holdings`` empty across the dev DB despite
    44k+ DEF 14A filings on file in ``filing_events`` — the daily
    cron's ``limit=100`` is too slow to drain the historical backlog.
    This bootstrap processes the entire backlog in one bounded
    session under the SEC fair-use rate-limit budget.

    Returns aggregate :class:`IngestSummary` summing every chunk's
    counts. Tombstoned accessions stay tombstoned (the standard
    discovery filter excludes anything already in
    ``def14a_ingest_log``); operator clears log rows to force retry.
    """
    import time

    deadline = time.monotonic() + max_runtime_seconds
    total_seen = 0
    total_succeeded = 0
    total_partial = 0
    total_failed = 0
    total_inserted = 0
    total_updated = 0
    first_error: str | None = None

    while time.monotonic() < deadline:
        chunk = ingest_def14a(
            conn,
            fetcher,
            limit=chunk_limit,
            prefetch_urls=prefetch_urls,
            prefetch_user_agent=prefetch_user_agent,
        )
        total_seen += chunk.accessions_seen
        total_succeeded += chunk.accessions_succeeded
        total_partial += chunk.accessions_partial
        total_failed += chunk.accessions_failed
        total_inserted += chunk.rows_inserted
        total_updated += chunk.rows_updated
        if first_error is None and chunk.first_error is not None:
            first_error = chunk.first_error
        if chunk.accessions_seen == 0:
            break

    # Bot review for #839 PR #850: enforce the accounting invariant
    # so a future regression that drops accessions from one of the
    # outcome buckets trips here rather than silently undercounting.
    # Soft-assert via logger.warning rather than raise — a partial
    # accounting result is still useful operator output, but we want
    # the discrepancy to be visible in the run audit.
    accounted = total_succeeded + total_partial + total_failed
    if accounted != total_seen:
        logger.warning(
            "bootstrap_def14a accounting drift: seen=%d != succeeded(%d)+partial(%d)+failed(%d)=%d",
            total_seen,
            total_succeeded,
            total_partial,
            total_failed,
            accounted,
        )

    logger.info(
        "bootstrap_def14a complete: seen=%d succeeded=%d partial=%d failed=%d inserted=%d updated=%d",
        total_seen,
        total_succeeded,
        total_partial,
        total_failed,
        total_inserted,
        total_updated,
    )

    return IngestSummary(
        accessions_seen=total_seen,
        accessions_succeeded=total_succeeded,
        accessions_partial=total_partial,
        accessions_failed=total_failed,
        rows_inserted=total_inserted,
        rows_updated=total_updated,
        first_error=first_error,
    )


# ---------------------------------------------------------------------------
# Iterator (exposed for ad-hoc reporting / debug)
# ---------------------------------------------------------------------------


def iter_holdings_for_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    limit: int = 1000,
) -> Iterator[dict[str, Any]]:
    """Yield the most recent holdings for one instrument."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, holder_name, holder_role,
                   shares, percent_of_class, as_of_date, fetched_at
            FROM def14a_beneficial_holdings
            WHERE instrument_id = %(iid)s
            ORDER BY as_of_date DESC NULLS LAST,
                     accession_number DESC,
                     shares DESC NULLS LAST
            LIMIT %(limit)s
            """,
            {"iid": instrument_id, "limit": limit},
        )
        for row in cur.fetchall():
            yield dict(row)
