"""Re-wash workflow — re-parse stored raw bodies under the current
parser version.

The contract: ``filing_raw_documents`` retains the source XML / HTML
body of every ownership filing the app ingests (PR #808 + #810 +
#811 wired this for all five kinds). When a parser bug ships, the
fix is to re-walk every row whose ``parser_version`` is below the
current one and re-apply the typed-table upsert against the stored
body — no SEC re-fetch required.

Operator audit 2026-05-03 motivated this: a parser bug discovery
under the prior architecture forced a full re-fetch from SEC at
10 req/s. With the raw store in place, re-wash is local I/O.

Architecture: a per-kind ``ParserSpec`` registry binds each
``DocumentKind`` to the parse + apply pair already shipped in the
ingester. Re-wash dispatches by kind. ``run_rewash(conn, kind=...)``
walks every row whose ``parser_version`` doesn't match the spec's
``current_version`` and re-applies the parser. The raw row's
``parser_version`` is bumped on success so a second pass is a
no-op.

Scope of this PR: framework + Form 4 wiring (the most common
ownership filing kind, ~440k rows). Other kinds (13F, 13D/G, Form
3, DEF 14A) wire in follow-up PRs once the framework + first kind
have shipped and proven the contract — same rollout shape as the
reconciliation framework.

Operator runs:

    uv run python scripts/rewash.py --kind form4_xml
    uv run python scripts/rewash.py --kind form4_xml --since 2024-01-01
    uv run python scripts/rewash.py --kind form4_xml --dry-run

Re-wash is idempotent. Re-running after a successful pass is a
no-op because every row's ``parser_version`` already matches the
current spec.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import psycopg
import psycopg.rows

from app.services import raw_filings
from app.services.raw_filings import DocumentKind, RawFilingDocument

logger = logging.getLogger(__name__)


class RewashParseError(Exception):
    """A parser returned ``None`` (or otherwise rejected) a body
    that a prior parser version produced typed-table output for.

    Distinguishes a parser REGRESSION (must surface in
    ``rows_failed``) from a legitimate "no typed row to update"
    skip (``apply_fn`` returns ``False`` → ``rows_skipped``)."""


@dataclass(frozen=True)
class ParserSpec:
    """Per-kind parser binding for re-wash.

    ``apply_fn`` does the typed-table upsert. It receives the
    connection and the raw document; the rest of the context
    (instrument_id, accession_number, etc.) is its responsibility
    to derive — typically by reading the existing typed-table row
    keyed on ``raw_doc.accession_number``.

    ``apply_fn`` returns ``True`` when the upsert ran (a typed
    row existed and was refreshed under the current parser),
    ``False`` when the row should be skipped (e.g., no typed
    row exists yet — re-wash is not a first-time ingester).
    """

    document_kind: DocumentKind
    current_version: str
    apply_fn: Callable[[psycopg.Connection[Any], RawFilingDocument], bool]


@dataclass(frozen=True)
class RewashResult:
    document_kind: DocumentKind
    rows_scanned: int
    rows_reparsed: int
    rows_skipped: int
    rows_failed: int


_REGISTRY: dict[DocumentKind, ParserSpec] = {}


def register_parser(spec: ParserSpec) -> None:
    """Register a parser binding. Idempotent — re-registering the
    same kind overwrites the prior spec. Used by the per-ingester
    modules at import time."""
    _REGISTRY[spec.document_kind] = spec


def registered_specs() -> dict[DocumentKind, ParserSpec]:
    """Snapshot for tests + introspection."""
    return dict(_REGISTRY)


def run_rewash(
    conn: psycopg.Connection[Any],
    *,
    document_kind: DocumentKind,
    since: date | None = None,
    dry_run: bool = False,
    batch_size: int = 100,
) -> RewashResult:
    """Walk every raw row of ``document_kind`` whose parser_version
    is not the registered spec's current_version and re-apply the
    parser.

    ``since`` filters by ``fetched_at`` to scope sweeps to recent
    filings — useful when an operator only wants to re-wash the
    cohort affected by a recent bug.

    ``dry_run=True`` walks the rows and counts what WOULD be
    re-parsed but writes nothing — the typed-table upsert and the
    raw-row parser_version bump are both skipped.

    Returns counts for operator triage.
    """
    spec = _REGISTRY.get(document_kind)
    if spec is None:
        raise ValueError(f"No parser registered for document_kind={document_kind!r}. Available: {sorted(_REGISTRY)}")

    scanned = 0
    reparsed = 0
    skipped = 0
    failed = 0

    # Eager-fetch the cohort: accession_number + fetched_at only,
    # NOT the body. The bodies can be hundreds of KB each — loading
    # all of them up front would balloon memory. Per-accession
    # ``read_raw`` later in the loop fetches the body for one row
    # at a time.
    #
    # A server-side cursor + commit/rollback in the loop is NOT an
    # option: PostgreSQL closes the cursor on every commit, so once
    # the first buffered batch is exhausted the next fetch raises
    # ``InvalidCursorName``. Eager-fetching the small identifier set
    # sidesteps the issue entirely.
    cohort = _fetch_cohort(
        conn,
        document_kind=document_kind,
        current_version=spec.current_version,
        batch_size=batch_size,
    )

    for accession, fetched_at in cohort:
        scanned += 1
        if since is not None and fetched_at.date() < since:
            skipped += 1
            continue

        if dry_run:
            reparsed += 1
            continue

        # Read the body NOW — separate per-accession round-trip but
        # avoids the cursor-commit interaction.
        raw_doc = raw_filings.read_raw(
            conn,
            accession_number=accession,
            document_kind=document_kind,
        )
        if raw_doc is None:
            # Row vanished between cohort scan and read (rare but
            # possible if a parallel process truncated). Treat as
            # skipped; the next sweep will see the new state.
            skipped += 1
            continue

        try:
            applied = spec.apply_fn(conn, raw_doc)
        except Exception:  # noqa: BLE001 — single-row failure must not abort
            logger.exception(
                "rewash: apply_fn raised on accession=%s kind=%s",
                accession,
                document_kind,
            )
            conn.rollback()
            failed += 1
            continue

        if not applied:
            skipped += 1
            conn.commit()
            continue

        # Bump the raw row's parser_version so a re-run is a no-op.
        # Done in the same transaction as the typed-table upsert
        # so a crash between the two leaves both unchanged — the
        # row will be picked up again on the next sweep.
        try:
            _bump_parser_version(
                conn,
                accession_number=accession,
                document_kind=document_kind,
                new_version=spec.current_version,
            )
            conn.commit()
        except Exception:
            logger.exception(
                "rewash: parser_version bump failed for accession=%s",
                accession,
            )
            conn.rollback()
            failed += 1
            continue

        reparsed += 1

    return RewashResult(
        document_kind=document_kind,
        rows_scanned=scanned,
        rows_reparsed=reparsed,
        rows_skipped=skipped,
        rows_failed=failed,
    )


def _fetch_cohort(
    conn: psycopg.Connection[Any],
    *,
    document_kind: DocumentKind,
    current_version: str,
    batch_size: int,  # noqa: ARG001 — kept for API symmetry with iter_raw; eager fetch ignores
) -> list[tuple[str, datetime]]:
    """Return ``(accession_number, fetched_at)`` for every row of
    ``document_kind`` whose parser_version is NOT ``current_version``.
    Body intentionally not fetched here — keeps the cohort scan
    cheap (~25 bytes per row × ~440k Form 4 rows = ~11 MB)."""
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT accession_number, fetched_at
            FROM filing_raw_documents
            WHERE document_kind = %s
              AND (parser_version IS NULL OR parser_version <> %s)
            ORDER BY accession_number
            """,
            (document_kind, current_version),
        )
        return list(cur.fetchall())


def _bump_parser_version(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
    new_version: str,
) -> None:
    """Update the ``parser_version`` on a single raw row WITHOUT
    rewriting the body. Avoid ``store_raw`` (which would refresh
    ``fetched_at``) so the operator-visible "when did SEC last
    publish this?" timestamp is preserved."""
    conn.execute(
        """
        UPDATE filing_raw_documents
        SET parser_version = %s
        WHERE accession_number = %s AND document_kind = %s
        """,
        (new_version, accession_number, document_kind),
    )


# ---------------------------------------------------------------------------
# Form 4 wiring — first kind to land
# ---------------------------------------------------------------------------


def _apply_form4(
    conn: psycopg.Connection[Any],
    raw_doc: RawFilingDocument,
) -> bool:
    """Re-parse the Form 4 XML body and re-apply the typed-table
    upsert. Returns ``False`` when no existing ``insider_filings``
    row is found (re-wash is not a first-time ingester — the
    instrument resolution / filer seeding has to have happened on
    the original ingest pass)."""
    from app.services.insider_transactions import parse_form_4_xml, upsert_filing

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, primary_document_url
            FROM insider_filings
            WHERE accession_number = %s
            """,
            (raw_doc.accession_number,),
        )
        row = cur.fetchone()
    if row is None:
        return False
    instrument_id, primary_document_url = row

    parsed = parse_form_4_xml(raw_doc.payload)
    if parsed is None:
        # Parser regression — the previous parser presumably
        # produced a typed-table row for this body, but the current
        # parser returns None. RAISE rather than return False:
        # ``apply_fn`` returning False means "no typed row to
        # update, legitimately skip", which is operator-invisible
        # in the failure counter. A parser regression is a real
        # failure that must surface in ``rows_failed``.
        raise RewashParseError(
            f"parse_form_4_xml returned None for accession={raw_doc.accession_number} body_size={len(raw_doc.payload)}"
        )

    upsert_filing(
        conn,
        instrument_id=int(instrument_id),
        accession_number=raw_doc.accession_number,
        primary_document_url=str(primary_document_url) if primary_document_url else "",
        parsed=parsed,
        is_rewash=True,  # preserve original fetched_at — re-wash isn't a fresh SEC fetch
    )
    return True


# Registered eagerly so the registry is populated at import time —
# matches the pattern in app.services.reconciliation. The version
# strings mirror the constants in each ingester; if either changes,
# both must change so re-wash actually re-walks.
register_parser(
    ParserSpec(
        document_kind="form4_xml",
        current_version="form4-v1",
        apply_fn=_apply_form4,
    )
)


# ---------------------------------------------------------------------------
# Form 3 wiring
# ---------------------------------------------------------------------------


def _apply_form3(
    conn: psycopg.Connection[Any],
    raw_doc: RawFilingDocument,
) -> bool:
    """Re-parse the Form 3 XML body and re-apply the typed-table
    upsert. Same shape as Form 4 — Form 3 is the
    initial-holdings-baseline cousin of Form 4's transactions.

    Returns ``False`` when no existing ``insider_filings`` row is
    found (re-wash is not a first-time ingester). Raises
    ``RewashParseError`` on parser regression so the failure
    surfaces in ``rows_failed``, not silently in ``rows_skipped``."""
    from app.services.insider_form3_ingest import upsert_form_3_filing
    from app.services.insider_transactions import parse_form_3_xml

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, primary_document_url
            FROM insider_filings
            WHERE accession_number = %s
            """,
            (raw_doc.accession_number,),
        )
        row = cur.fetchone()
    if row is None:
        return False
    instrument_id, primary_document_url = row

    parsed = parse_form_3_xml(raw_doc.payload)
    if parsed is None:
        raise RewashParseError(
            f"parse_form_3_xml returned None for accession={raw_doc.accession_number} body_size={len(raw_doc.payload)}"
        )

    upsert_form_3_filing(
        conn,
        instrument_id=int(instrument_id),
        accession_number=raw_doc.accession_number,
        primary_document_url=str(primary_document_url) if primary_document_url else "",
        parsed=parsed,
        is_rewash=True,
    )
    return True


# Form 3 parser version is "form3-v{N}" — see _FORM3_PARSER_VERSION
# in insider_form3_ingest.py. Bump both constants in lockstep when
# the parser semantics change in a way that affects what lands in
# typed tables.
register_parser(
    ParserSpec(
        document_kind="form3_xml",
        current_version="form3-v1",
        apply_fn=_apply_form3,
    )
)


# ---------------------------------------------------------------------------
# DEF 14A proxy beneficial-ownership table wiring
# ---------------------------------------------------------------------------


def _apply_def14a(
    conn: psycopg.Connection[Any],
    raw_doc: RawFilingDocument,
) -> bool:
    """Re-parse the DEF 14A HTML body and re-apply the beneficial-
    ownership-holdings upsert.

    Replace-then-insert: clear all rows for the accession, then
    INSERT each parsed holder. The unique key on the typed table
    is ``(accession_number, holder_name)`` — without a clear, a
    new parser version that DROPS a stale holder would leave the
    old row pinned forever.

    Returns ``False`` when no existing typed row is found (re-wash
    isn't a first-time ingester). Raises ``RewashParseError`` on
    parser regression so the failure surfaces in ``rows_failed``.
    Note: a no-table-found (parsed.rows empty) is also a regression
    here — under the existing ingester it would tombstone status=
    partial, but in re-wash context it means the new parser is
    weaker than the prior one against the same body."""
    from app.providers.implementations.sec_def14a import parse_beneficial_ownership_table
    from app.services.def14a_ingest import _upsert_holding

    # Resolution priority:
    #   1. Existing typed rows in def14a_beneficial_holdings —
    #      happy path (first ingest produced rows, parser bump
    #      now updating them).
    #   2. Fallback to def14a_ingest_log + filing_events when no
    #      typed rows exist. Covers the rescue cohort: original
    #      ingest tombstoned status='failed' or 'partial' with
    #      zero typed rows; the new parser now wants to fill them
    #      in. Codex pre-push review caught this gap — without
    #      the fallback, the cohort rewash should rescue stays on
    #      the old parser_version forever.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT issuer_cik, instrument_id
            FROM def14a_beneficial_holdings
            WHERE accession_number = %s
            LIMIT 1
            """,
            (raw_doc.accession_number,),
        )
        row = cur.fetchone()
    had_existing_rows = row is not None
    if row is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT log.issuer_cik, fe.instrument_id
                FROM def14a_ingest_log log
                JOIN filing_events fe
                  ON fe.provider_filing_id = log.accession_number
                 AND fe.provider = 'sec'
                WHERE log.accession_number = %s
                LIMIT 1
                """,
                (raw_doc.accession_number,),
            )
            row = cur.fetchone()
    if row is None:
        return False
    issuer_cik, instrument_id = row

    try:
        parsed = parse_beneficial_ownership_table(raw_doc.payload)
    except Exception as exc:
        raise RewashParseError(
            f"parse_beneficial_ownership_table failed for accession={raw_doc.accession_number}: {exc}"
        ) from exc

    if not parsed.rows:
        if had_existing_rows:
            # Parser regression on a populated accession — raise
            # so the operator sees the gap in rows_failed rather
            # than silently zeroing out typed rows.
            raise RewashParseError(
                f"DEF 14A re-parse produced zero holders for accession="
                f"{raw_doc.accession_number} (best_score={parsed.raw_table_score}); "
                f"previous parser found rows"
            )
        # Rescue cohort with still-empty parse. Don't raise —
        # parser hasn't improved enough yet. Skip without bumping
        # parser_version so a future sweep with a better parser
        # re-tries.
        return False

    # Replace-then-insert: clear all existing holders for the
    # accession so a holder dropped by the new parser cannot
    # linger.
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM def14a_beneficial_holdings WHERE accession_number = %s",
            (raw_doc.accession_number,),
        )

    for holder in parsed.rows:
        _upsert_holding(
            conn,
            accession_number=raw_doc.accession_number,
            issuer_cik=str(issuer_cik),
            instrument_id=int(instrument_id),
            as_of_date=parsed.as_of_date,
            holder=holder,
        )

    # Write-through to ownership_def14a_observations + refresh
    # ownership_def14a_current. Mirrors the first-ingest path at
    # app/services/def14a_ingest.py:463-471. Without this, the rewash
    # writes typed rows but leaves the rollup (#905 read-path) stale
    # against the new parser output (#945 same-pattern as 13F).
    from app.services.def14a_ingest import _record_def14a_observations_for_filing
    from app.services.ownership_observations import refresh_def14a_current

    _record_def14a_observations_for_filing(
        conn,
        instrument_id=int(instrument_id),
        accession_number=raw_doc.accession_number,
        as_of_date=parsed.as_of_date,
        holders=parsed.rows,
    )
    refresh_def14a_current(conn, instrument_id=int(instrument_id))
    return True


register_parser(
    ParserSpec(
        document_kind="def14a_body",
        current_version="def14a-v1",
        apply_fn=_apply_def14a,
    )
)


# ---------------------------------------------------------------------------
# 13D/G blockholder primary_doc.xml wiring
# ---------------------------------------------------------------------------


def _apply_blockholders(
    conn: psycopg.Connection[Any],
    raw_doc: RawFilingDocument,
) -> bool:
    """Re-parse the 13D/G primary_doc.xml body and re-apply the
    typed-table upsert.

    Unlike Form 3 / Form 4, the existing ingester's per-reporter
    upsert uses ``ON CONFLICT DO NOTHING`` (one accession × one
    reporter == one row, immutable on first ingest). For re-wash
    we DELETE all rows for the accession then re-INSERT under the
    new parser — equivalent to the "replace-then-insert" pattern
    Codex caught in the Form 3 / Form 4 ingesters when a new
    parser version stops emitting a stale joint-filer.

    Returns ``False`` when no existing row is found (re-wash isn't
    a first-time ingester). Raises ``RewashParseError`` on parser
    regression so failures surface in ``rows_failed``."""
    from app.providers.implementations.sec_13dg import parse_primary_doc
    from app.services.blockholders import (
        _resolve_cusip_to_instrument_id,
        _upsert_filer,
        _upsert_filing_row,
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT filed_at
            FROM blockholder_filings
            WHERE accession_number = %s
            LIMIT 1
            """,
            (raw_doc.accession_number,),
        )
        row = cur.fetchone()
    if row is None:
        return False
    (filed_at,) = row

    try:
        filing = parse_primary_doc(raw_doc.payload)
    except Exception as exc:
        raise RewashParseError(
            f"parse_primary_doc(13dg) failed for accession={raw_doc.accession_number}: {exc}"
        ) from exc

    # Empty reporting_persons after re-parse means the new parser
    # rejected every reporter on a previously-populated filing.
    # Raise so the regression surfaces in rows_failed — without
    # this guard, the DELETE below would silently destroy every
    # existing reporter row with no error. Codex pre-push review
    # caught this.
    if not filing.reporting_persons:
        raise RewashParseError(
            f"13D/G re-parse produced zero reporting_persons for "
            f"accession={raw_doc.accession_number}; previous parser "
            f"found rows"
        )

    # CRITICAL: re-resolve instrument_id from the FRESH parsed
    # CUSIP, not the stale value from the old typed row. The point
    # of rewash is to pick up parser-corrected fields; re-using
    # the prior instrument_id while the new parser emits a
    # different issuer_cusip produces an internally-inconsistent
    # row that silently joins to the wrong instrument. Codex
    # pre-push review caught the prior reuse-of-stale-value bug.
    instrument_id = _resolve_cusip_to_instrument_id(conn, filing.issuer_cusip)

    # Resolve canonical filer name + filer_id (preserved across
    # re-wash via ON CONFLICT (cik) DO UPDATE in _upsert_filer).
    # ``reporting_persons`` is non-empty here — the empty-list guard
    # above raises before this point — so the ``next()`` default
    # falls back to the first reporter's name without needing the
    # ``CIK {primary_filer_cik}`` branch the prior version had.
    # Claude PR #825 review (round 2) caught the unreachable else.
    filer_name = next(
        (p.name for p in filing.reporting_persons if p.cik == filing.primary_filer_cik),
        filing.reporting_persons[0].name,
    )
    filer_id = _upsert_filer(conn, cik=filing.primary_filer_cik, name=filer_name)

    # Replace-then-insert: clear all reporter rows for the
    # accession so the new parser's set is the only one on file.
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM blockholder_filings WHERE accession_number = %s",
            (raw_doc.accession_number,),
        )

    for person in filing.reporting_persons:
        _upsert_filing_row(
            conn,
            filer_id=filer_id,
            accession_number=raw_doc.accession_number,
            submission_type=filing.submission_type,
            status=filing.status,
            instrument_id=instrument_id,
            issuer_cik=filing.issuer_cik,
            issuer_cusip=filing.issuer_cusip,
            securities_class_title=filing.securities_class_title,
            date_of_event=filing.date_of_event,
            filed_at=filing.filed_at or filed_at,
            person=person,
        )

    # Write-through to ownership_blockholders_observations + refresh
    # ownership_blockholders_current. Mirrors first-ingest path at
    # app/services/blockholders.py:707-724. Same #945 pattern as 13F:
    # rewash writes typed rows but leaves the rollup stale without
    # this hook. ``_record_13dg_observation_for_filing`` requires an
    # ``AccessionRef`` for the ``filed_at`` fallback — synthesise one
    # from the rewash's known values (the filing's own ``filed_at``
    # takes priority when present).
    if instrument_id is not None:
        from uuid import uuid4

        from app.services.blockholders import (
            AccessionRef,
            _record_13dg_observation_for_filing,
        )
        from app.services.ownership_observations import refresh_blockholders_current

        ref = AccessionRef(
            accession_number=raw_doc.accession_number,
            filing_type=filing.submission_type,
            filed_at=filed_at,
        )
        _record_13dg_observation_for_filing(
            conn,
            instrument_id=int(instrument_id),
            accession_number=raw_doc.accession_number,
            primary_document_url="",
            filing=filing,
            filer_name=filer_name,
            ref=ref,
            run_id=uuid4(),
        )
        refresh_blockholders_current(conn, instrument_id=int(instrument_id))
    return True


register_parser(
    ParserSpec(
        document_kind="primary_doc_13dg",
        current_version="13dg-primary-v1",
        apply_fn=_apply_blockholders,
    )
)

# ---------------------------------------------------------------------------
# 13F-HR infotable.xml wiring
# ---------------------------------------------------------------------------


def _apply_13f_infotable(
    conn: psycopg.Connection[Any],
    raw_doc: RawFilingDocument,
) -> bool:
    """Re-parse the 13F-HR infotable.xml body and re-apply the
    holdings upsert.

    Replace-then-insert pattern (same as 13D/G + DEF 14A):
    existing per-holding upsert uses ON CONFLICT DO NOTHING via
    the partial UNIQUE INDEX, so re-wash needs to DELETE all
    holdings for the accession before INSERT.

    Each holding's instrument_id is RE-RESOLVED from the parsed
    CUSIP via _resolve_cusip_to_instrument_id — same path the
    first-time ingester uses. A new parser fix that emits
    different CUSIPs gets the right instrument linkage on rewash.

    Returns ``False`` when no existing institutional_holdings row
    is found (re-wash isn't a first-time ingester). Raises
    ``RewashParseError`` on parser failure."""
    from app.providers.implementations.sec_13f import parse_infotable
    from app.services.institutional_holdings import (
        _resolve_cusip_to_instrument_id,
        _upsert_holding,
    )

    # Resolution priority:
    #   1. Existing typed rows in institutional_holdings — happy path
    #      (first ingest produced rows for at least some holdings).
    #   2. Fallback to institutional_holdings_ingest_log JOIN
    #      institutional_filers — covers the rescue cohort: legal-
    #      empty 13F-HRs and all-CUSIPs-unresolved accessions write
    #      zero holdings rows but DO record a row in the ingest log.
    #      Codex pre-push review caught the gap.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT filer_id, period_of_report, filed_at
            FROM institutional_holdings
            WHERE accession_number = %s
            LIMIT 1
            """,
            (raw_doc.accession_number,),
        )
        row = cur.fetchone()
    had_existing_holdings = row is not None
    if row is None:
        # Rescue cohort. ``filed_at`` is sourced from
        # ``filing_events.filing_date`` so the typed-table row gets
        # the SEC-canonical filing date — NOT ``log.fetched_at``,
        # which is the moment the ingest worker scanned the row and
        # is days/weeks later than the actual filing date. Claude
        # PR #827 round 2 review caught this as WARNING:
        # ingest-time leaking into ``institutional_holdings.filed_at``
        # poisons every downstream "as of" calculation that joins
        # on it (rollup tie-breaks, freshness chips, etc.). LEFT
        # JOIN with COALESCE to ``log.fetched_at`` so the rescue
        # still works on the rare path where filing_events has no
        # row for the accession (legacy cohort).
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.filer_id,
                       log.period_of_report,
                       COALESCE(fe.filing_date::timestamptz, log.fetched_at) AS filed_at
                FROM institutional_holdings_ingest_log log
                JOIN institutional_filers f ON f.cik = log.filer_cik
                LEFT JOIN filing_events fe
                  ON fe.provider_filing_id = log.accession_number
                 AND fe.provider = 'sec'
                WHERE log.accession_number = %s
                LIMIT 1
                """,
                (raw_doc.accession_number,),
            )
            row = cur.fetchone()
    if row is None:
        return False
    filer_id, period_of_report, filed_at = row

    try:
        holdings = parse_infotable(raw_doc.payload)
    except Exception as exc:
        raise RewashParseError(f"parse_infotable failed for accession={raw_doc.accession_number}: {exc}") from exc

    if not holdings:
        if had_existing_holdings:
            # Populated accession lost all holdings on re-parse —
            # parser regression. Raise so it surfaces in
            # rows_failed instead of silently zeroing out the
            # typed table.
            raise RewashParseError(
                f"13F infotable re-parse produced zero holdings for "
                f"accession={raw_doc.accession_number}; parser regression"
            )
        # Rescue cohort with empty parse — could be a legal-empty
        # 13F-HR (filer reported "exempt list" or cancellation) or
        # all-CUSIPs-unresolved that the new parser also can't
        # solve. Record success to ingest_log + return True so
        # parser_version bumps; the accession is on file as a
        # zero-holdings filing. ``ON CONFLICT DO UPDATE`` updates
        # the existing log row in place.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO institutional_holdings_ingest_log (
                    accession_number, filer_cik, period_of_report,
                    status, holdings_inserted, holdings_skipped, error
                )
                SELECT %s, f.cik, %s, 'success', 0, 0, NULL
                FROM institutional_filers f WHERE f.filer_id = %s
                ON CONFLICT (accession_number) DO UPDATE SET
                    status = EXCLUDED.status,
                    holdings_inserted = EXCLUDED.holdings_inserted,
                    holdings_skipped = EXCLUDED.holdings_skipped,
                    error = EXCLUDED.error,
                    fetched_at = NOW()
                """,
                (raw_doc.accession_number, period_of_report, int(filer_id)),
            )
        return True

    # Resolve every CUSIP BEFORE the DELETE so we never destroy
    # existing holdings without confirmed replacements. Codex
    # pre-push review caught the prior version which DELETEd
    # first, then iterated; if every CUSIP turned out unresolvable
    # the existing rows were silently destroyed with no replacement
    # and no path to repair (return False prevented the bump but
    # the typed table was already empty).
    resolved: list[tuple[int, Any]] = []  # (instrument_id, holding)
    skipped_no_cusip = 0
    for holding in holdings:
        instrument_id = _resolve_cusip_to_instrument_id(conn, holding.cusip)
        if instrument_id is None:
            skipped_no_cusip += 1
            continue
        resolved.append((instrument_id, holding))

    # ANY unresolved CUSIP defers the rewash — neither full replace
    # nor partial replace is safe:
    #
    #   * Full replace + partial set: original holdings whose new
    #     CUSIPs no longer resolve are silently destroyed. Next sweep
    #     repeats the same delete/insert cycle; the lost holdings
    #     never come back. Claude PR #827 review caught this as
    #     BLOCKING — the prior version went down this path when
    #     ``resolved`` was non-empty AND ``skipped_no_cusip > 0``.
    #   * Skip the rewash entirely + return False: typed table stays
    #     intact, parser_version doesn't bump, the accession stays
    #     eligible for the next sweep. Once #740 backfill closes the
    #     CUSIP gap, all holdings resolve on a follow-up pass and the
    #     full replace runs cleanly.
    #
    # The all-unresolved case (resolved is empty) and the partial
    # case (resolved is non-empty + skipped > 0) collapse to the
    # same branch: log partial, leave typed table alone, return
    # False.
    if skipped_no_cusip > 0:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO institutional_holdings_ingest_log (
                    accession_number, filer_cik, period_of_report,
                    status, holdings_inserted, holdings_skipped, error
                )
                SELECT %s, f.cik, %s, 'partial', 0, %s, %s
                FROM institutional_filers f WHERE f.filer_id = %s
                ON CONFLICT (accession_number) DO UPDATE SET
                    period_of_report = EXCLUDED.period_of_report,
                    status = EXCLUDED.status,
                    holdings_inserted = EXCLUDED.holdings_inserted,
                    holdings_skipped = EXCLUDED.holdings_skipped,
                    error = EXCLUDED.error,
                    fetched_at = NOW()
                """,
                (
                    raw_doc.accession_number,
                    period_of_report,
                    skipped_no_cusip,
                    f"{skipped_no_cusip} unresolved CUSIPs (gated by #740 backfill)",
                    int(filer_id),
                ),
            )
        return False

    # All CUSIPs resolved — safe to replace-then-insert.
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM institutional_holdings WHERE accession_number = %s",
            (raw_doc.accession_number,),
        )

    inserted = 0
    for instrument_id, holding in resolved:
        _upsert_holding(
            conn,
            filer_id=int(filer_id),
            instrument_id=instrument_id,
            accession_number=raw_doc.accession_number,
            period_of_report=period_of_report,
            filed_at=filed_at,
            holding=holding,
        )
        inserted += 1

    # Write-through to observations + refresh ownership_institutions_current
    # so the rollup (#905 read-path cutover) reflects the recovered
    # holdings on the same transaction. Mirrors the first-ingest path
    # in app/services/institutional_holdings.py:1260-1274. Without this,
    # ``cusip_resolver.sweep_resolvable_unresolved_cusips`` would happily
    # log "rewashed accession=..." while leaving every ownership rollup
    # query showing zero institutional shares (#945).
    if resolved:
        from app.services.institutional_holdings import _record_13f_observations_for_filing
        from app.services.ownership_observations import refresh_institutions_current

        # ``filed_at`` from the SELECT above is a tuple-row Decimal/None
        # type (psycopg returned timestamptz) — record_institution_observation
        # expects a datetime. The first-ingest path threads the same
        # value through the same helper, so the type is already what
        # the helper accepts.
        _record_13f_observations_for_filing(
            conn,
            filer_id=int(filer_id),
            accession_number=raw_doc.accession_number,
            period_of_report=period_of_report,
            filed_at=filed_at,
            resolved_holdings=resolved,
        )
        for unique_instrument_id in {iid for iid, _ in resolved}:
            refresh_institutions_current(conn, instrument_id=unique_instrument_id)

    # Log full success.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO institutional_holdings_ingest_log (
                accession_number, filer_cik, period_of_report,
                status, holdings_inserted, holdings_skipped, error
            )
            SELECT %s, f.cik, %s, 'success', %s, 0, NULL
            FROM institutional_filers f WHERE f.filer_id = %s
            ON CONFLICT (accession_number) DO UPDATE SET
                period_of_report = EXCLUDED.period_of_report,
                status = EXCLUDED.status,
                holdings_inserted = EXCLUDED.holdings_inserted,
                holdings_skipped = EXCLUDED.holdings_skipped,
                error = EXCLUDED.error,
                fetched_at = NOW()
            """,
            (
                raw_doc.accession_number,
                period_of_report,
                inserted,
                int(filer_id),
            ),
        )
    return True


register_parser(
    ParserSpec(
        document_kind="infotable_13f",
        current_version="13f-infotable-v1",
        apply_fn=_apply_13f_infotable,
    )
)


def _rewash_13f_accession(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
) -> bool:
    """Re-apply the registered 13F-HR infotable parser to a single
    accession's stored raw body.

    Used by the CUSIP extid sweep (#836): when an ``unresolved_13f_cusips``
    row's CUSIP turns out to already exist in ``external_identifiers``,
    the sweep needs to re-process the original filing so the now-resolvable
    holdings land in ``institutional_holdings``. ``run_rewash`` is the
    bulk API and walks every parser_version-stale row of the kind; this
    helper is the single-accession variant the sweep loops over.

    Returns the underlying ``apply_fn`` outcome:
      * ``True`` — typed-table upsert ran (or rescue-cohort log entry
        was recorded). Caller may bump parser_version separately if it
        cares; the sweep does not, since the rewash trigger is incidental
        to the extid promotion.
      * ``False`` — raw body absent, no existing typed row / ingest log
        row found, OR the rewash deferred (any-CUSIP-still-unresolved
        partial path in ``_apply_13f_infotable``). The caller treats
        ``False`` as "rewash deferred — extid promotion remains valid;
        the next bulk ``run_rewash`` will pick this accession up once
        every CUSIP in the filing resolves".

    Raises :class:`RewashParseError` on parser regression, mirroring
    ``run_rewash`` semantics."""
    spec = _REGISTRY.get("infotable_13f")
    if spec is None:
        # Defensive: the spec is registered eagerly above; only an
        # import-order accident could leave it unregistered. Surface
        # that as a hard error rather than silently succeeding.
        raise RuntimeError("13F-HR infotable parser not registered in rewash_filings")

    raw_doc = raw_filings.read_raw(
        conn,
        accession_number=accession_number,
        document_kind="infotable_13f",
    )
    if raw_doc is None:
        return False

    return spec.apply_fn(conn, raw_doc)
