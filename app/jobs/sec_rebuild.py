"""Targeted manifest rebuild (#872).

Issue #872 / spec §"Mode 2 — Targeted rebuild".

Operator-triggered clean sweep. Resolves a scope to a set of
``(subject_type, subject_id, source)`` triples, then:

  1. Resets the matching ``data_freshness_index`` rows to
     ``state='unknown'``, ``expected_next_at=NOW()``,
     ``last_known_filing_id=NULL`` (Codex review v2 finding 6:
     explicit reset so the rebuild scope drains immediately rather
     than sitting in the future-poll queue).
  2. Sets the matching ``sec_filing_manifest`` rows to
     ``ingest_status='pending'`` (NOT delete — preserves accession
     history and lets the worker pick them up cleanly).
  3. Returns scope statistics; the worker (#869) and per-CIK poll
     (#870) drain naturally afterwards.

Scope payloads:

    { "instrument_id": int }
        # all issuer-scoped sources for that instrument
    { "filer_cik": str, "source": str }
        # all filings under that filer's CIK for the source
    { "source": str }
        # universe-wide for that source
    { "instrument_id": int, "source": str }
        # narrow

Codex review v3 finding 2: this PR also runs a per-CIK history scan
for the rebuild scope BEFORE handing work to the manifest worker, so
the rebuild can repair manifest gaps where the original discovery
missed an accession.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import sql

from app.providers.implementations.sec_submissions import (
    HttpGet,
    check_freshness,
    parse_submissions_page,
)
from app.services.sec_manifest import ManifestSource, record_manifest_entry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RebuildScope:
    instrument_id: int | None = None
    filer_cik: str | None = None
    source: ManifestSource | None = None


@dataclass(frozen=True)
class RebuildStats:
    scope_triples: int
    manifest_rows_reset: int
    scheduler_rows_reset: int
    discovery_new_manifest_rows: int


def _resolve_scope(conn: psycopg.Connection[Any], scope: RebuildScope) -> list[tuple[str, str, ManifestSource]]:
    """Resolve a scope payload to (subject_type, subject_id, source) triples.

    Reads ``data_freshness_index`` to find every triple matching the
    scope. ``data_freshness_index`` is the canonical scheduler — if a
    triple has never been polled, it isn't in scope (rebuild is for
    repairing existing tracking, not discovering new subjects;
    first-install drain handles new discovery).
    """
    if scope.instrument_id is None and scope.filer_cik is None and scope.source is None:
        raise ValueError("RebuildScope: at least one of instrument_id / filer_cik / source must be set")

    where_clauses: list[sql.Composable] = []
    params: list[Any] = []
    if scope.instrument_id is not None:
        where_clauses.append(sql.SQL("instrument_id = %s"))
        params.append(scope.instrument_id)
    if scope.filer_cik is not None:
        where_clauses.append(
            sql.SQL("(subject_type IN ('institutional_filer', 'blockholder_filer') AND subject_id = %s)")
        )
        params.append(scope.filer_cik)
    if scope.source is not None:
        where_clauses.append(sql.SQL("source = %s"))
        params.append(scope.source)

    query = sql.SQL(
        "SELECT subject_type, subject_id, source FROM data_freshness_index"
        " WHERE {where} ORDER BY subject_type, subject_id, source"
    ).format(where=sql.SQL(" AND ").join(where_clauses))

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [(str(t), str(s), src) for t, s, src in cur.fetchall()]  # type: ignore[misc]


def _reset_scheduler_rows(conn: psycopg.Connection[Any], triples: list[tuple[str, str, ManifestSource]]) -> int:
    """Reset state='unknown' + expected_next_at=NOW() for each triple."""
    if not triples:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            UPDATE data_freshness_index
            SET state = 'unknown',
                expected_next_at = NOW(),
                last_known_filing_id = NULL,
                last_known_filed_at = NULL,
                last_polled_outcome = 'never',
                state_reason = 'rebuild',
                next_recheck_at = NULL
            WHERE subject_type = %s AND subject_id = %s AND source = %s
            """,
            triples,
        )
        return cur.rowcount


def _reset_manifest_rows(conn: psycopg.Connection[Any], triples: list[tuple[str, str, ManifestSource]]) -> int:
    """Set ingest_status='pending' for every manifest row in scope.

    Preserves parser_version (so the rewash detector can compare).
    Clears retry state. The worker picks these up via iter_pending.
    """
    if not triples:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            UPDATE sec_filing_manifest
            SET ingest_status = 'pending',
                next_retry_at = NULL,
                error = NULL
            WHERE subject_type = %s AND subject_id = %s AND source = %s
              AND ingest_status NOT IN ('pending')
            """,
            triples,
        )
        return cur.rowcount


def run_sec_rebuild(
    conn: psycopg.Connection[Any],
    scope: RebuildScope,
    *,
    http_get: HttpGet | None = None,
    discover: bool = True,
) -> RebuildStats:
    """Reset + (optionally) discover for a scope.

    Steps:
      1. Resolve scope → (subject_type, subject_id, source) triples
         from data_freshness_index.
      2. Reset scheduler rows.
      3. Reset manifest rows to pending.
      4. (optional, default on) Run a per-CIK history scan via
         check_freshness so any missing-accession gaps in the manifest
         are filled. ``discover=False`` skips this — useful when the
         caller only wants to flip already-known accessions back to
         pending.

    Returns RebuildStats. The worker (#869) drains the resulting
    pending rows.
    """
    triples = _resolve_scope(conn, scope)
    if not triples:
        logger.info("sec rebuild: scope resolved to 0 triples — no-op")
        return RebuildStats(
            scope_triples=0,
            manifest_rows_reset=0,
            scheduler_rows_reset=0,
            discovery_new_manifest_rows=0,
        )

    sched_reset = _reset_scheduler_rows(conn, triples)
    manifest_reset = _reset_manifest_rows(conn, triples)

    discovery_new = 0
    if discover and http_get is not None:
        discovery_new = _discovery_pass(conn, triples=triples, http_get=http_get)

    logger.info(
        "sec rebuild: triples=%d scheduler_reset=%d manifest_reset=%d new_manifest=%d",
        len(triples),
        sched_reset,
        manifest_reset,
        discovery_new,
    )
    return RebuildStats(
        scope_triples=len(triples),
        manifest_rows_reset=manifest_reset,
        scheduler_rows_reset=sched_reset,
        discovery_new_manifest_rows=discovery_new,
    )


def _discovery_pass(
    conn: psycopg.Connection[Any],
    *,
    triples: list[tuple[str, str, ManifestSource]],
    http_get: HttpGet,
) -> int:
    """Per-CIK history scan for the rebuild scope.

    Spec v3 finding #2: rebuild needs to RUN A FULL DISCOVERY before
    handing work to the worker. Without this, ``manifest worker``
    only re-parses accessions we already knew about; an accession
    missing from the original ingest would never be repaired.
    """
    new_rows = 0

    # Group triples by (subject_type, subject_id) so we issue one
    # HTTP fetch per CIK, then UPSERT manifest rows for every
    # in-scope source matching that subject.
    by_subject: dict[tuple[str, str], list[ManifestSource]] = {}
    for stype, sid, src in triples:
        by_subject.setdefault((stype, sid), []).append(src)

    # Resolve each subject's CIK + instrument_id once
    for (stype, sid), sources in by_subject.items():
        cik, instrument_id = _resolve_subject_cik(conn, stype, sid)
        if cik is None:
            continue

        try:
            delta = check_freshness(
                http_get,
                cik=cik,
                last_known_filing_id=None,  # full history
                sources=set(sources),
            )
        except Exception as exc:
            logger.warning("sec rebuild discovery: check_freshness raised cik=%s: %s", cik, exc)
            continue

        sources_set = set(sources)
        for row in delta.new_filings:
            if row.source is None or row.source not in sources_set:
                continue
            try:
                record_manifest_entry(
                    conn,
                    row.accession_number,
                    cik=row.cik,
                    form=row.form,
                    source=row.source,
                    subject_type=stype,  # type: ignore[arg-type]
                    subject_id=sid,
                    instrument_id=instrument_id,
                    filed_at=row.filed_at,
                    accepted_at=row.accepted_at,
                    primary_document_url=row.primary_document_url,
                    is_amendment=row.is_amendment,
                )
                new_rows += 1
            except ValueError as exc:
                logger.warning("sec rebuild: rejected %s: %s", row.accession_number, exc)

        # #936: ``check_freshness`` reads only the ``recent[]`` array
        # (~1000 most-recent accessions). Older filings live in the
        # secondary ``filings.files[]`` pages. Targeted rebuild claims
        # full-history discovery, so it MUST follow those pages too —
        # otherwise an aged-out accession can never be repaired.
        # Atom / per-CIK polling intentionally stay on ``recent`` (cheap
        # path); only rebuild pays the secondary-page cost.
        if delta.has_more_in_files:
            if delta.files_pages:
                new_rows += _walk_secondary_pages(
                    conn,
                    http_get=http_get,
                    cik=cik,
                    subject_type=stype,
                    subject_id=sid,
                    instrument_id=instrument_id,
                    sources_set=sources_set,
                    files_pages=delta.files_pages,
                )
            else:
                # Guard against silent skip: ``check_freshness`` already
                # logs a warning when extraction fails, but log here too
                # so the rebuild's own log stream surfaces the gap. Bot
                # review BLOCKING on PR #958.
                logger.warning(
                    "sec rebuild discovery: cik=%s has_more_in_files=True but "
                    "files_pages empty — secondary walk skipped",
                    cik,
                )

    return new_rows


def _walk_secondary_pages(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    cik: str,
    subject_type: str,
    subject_id: str,
    instrument_id: int | None,
    sources_set: set[ManifestSource],
    files_pages: list[str],
) -> int:
    """Walk every ``filings.files[]`` secondary page for one CIK (#936).

    ``files_pages`` is sourced from the ``FreshnessDelta`` returned by
    ``check_freshness`` — the primary CIK JSON has already been
    fetched + parsed by the time this helper runs, so we re-use the
    page list rather than re-fetching the primary body.

    Mirrors the per-page UPSERT logic in
    ``app/jobs/sec_first_install_drain.py:_drain_secondary_pages``,
    but scoped to the rebuild's source set. Filters in-page rows to
    ``sources_set`` so a rebuild scoped to ``sec_def14a`` does not
    UPSERT every 8-K it happens to encounter on the same secondary
    page.
    """
    cik_padded = cik.zfill(10)
    headers = {
        "User-Agent": "eBull research/1.0 contact@example.com",
        "Accept-Encoding": "gzip, deflate",
    }
    new_rows = 0
    for name in files_pages:
        if not name:
            continue
        page_url = f"https://data.sec.gov/submissions/{name}"
        # Codex pre-push: ``http_get`` and ``parse_submissions_page``
        # can raise (transport errors, malformed body, unexpected JSON
        # shape). Isolate per-page failures so one bad secondary page
        # does not abort the whole rebuild — the rest of the scope
        # still drains.
        try:
            status, body = http_get(page_url, headers)
        except Exception as exc:  # noqa: BLE001 — per-page isolation
            logger.warning("sec rebuild (secondary): fetch raised page=%s: %s", name, exc)
            continue
        if status != 200:
            continue
        try:
            rows, _ = parse_submissions_page(body, cik=cik_padded)
        except Exception as exc:  # noqa: BLE001 — per-page isolation
            logger.warning("sec rebuild (secondary): parse raised page=%s: %s", name, exc)
            continue
        for row in rows:
            if row.source is None or row.source not in sources_set:
                continue
            try:
                record_manifest_entry(
                    conn,
                    row.accession_number,
                    cik=row.cik,
                    form=row.form,
                    source=row.source,
                    subject_type=subject_type,  # type: ignore[arg-type]
                    subject_id=subject_id,
                    instrument_id=instrument_id,
                    filed_at=row.filed_at,
                    accepted_at=row.accepted_at,
                    primary_document_url=row.primary_document_url,
                    is_amendment=row.is_amendment,
                )
                new_rows += 1
            except ValueError as exc:
                logger.warning("sec rebuild (secondary): rejected %s: %s", row.accession_number, exc)
    return new_rows


def _resolve_subject_cik(
    conn: psycopg.Connection[Any], subject_type: str, subject_id: str
) -> tuple[str | None, int | None]:
    """Resolve (subject_type, subject_id) → (cik, instrument_id)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cik, instrument_id FROM data_freshness_index WHERE subject_type = %s AND subject_id = %s LIMIT 1",
            (subject_type, subject_id),
        )
        row = cur.fetchone()
    if row is None:
        return None, None
    return (row[0], int(row[1]) if row[1] is not None else None)
