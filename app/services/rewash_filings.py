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
    return True


register_parser(
    ParserSpec(
        document_kind="def14a_body",
        current_version="def14a-v1",
        apply_fn=_apply_def14a,
    )
)
