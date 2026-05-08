"""C1.b — per-CIK ``filings.files[]`` secondary-pages walker (#1027).

The bulk ``submissions.zip`` archive published by SEC contains every
CIK's ``filings.recent`` block (last ~12 months / 1000 most-recent
filings). For deeper history, SEC paginates older filings under
``filings.files[]`` — secondary JSON URLs the bulk archive does NOT
include.

C1.a (bulk submissions ingester, #1022) reads the bulk archive's
``recent`` block. C1.b walks each CIK's secondary pages — bounded
by the per-CIK rate budget — to seed ``filing_events`` with the
deeper history that the existing ``sec_first_install_drain`` would
otherwise have walked one CIK at a time at 7 req/s.

This is a ~150–300 secondary-page-fetch job for a typical 1.5 k
universe (most CIKs have <12 months of history; only the
deepest-history filers cross into the secondary-page region).
Walked AFTER C1.a + Phase B so the CUSIP universe + filer
directories are in place; rate-limited via the existing process-wide
SEC clock.

Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import (
    SecFilingsProvider,
    _normalise_submissions_block,
)
from app.services.filings import _upsert_filing

logger = logging.getLogger(__name__)


@dataclass
class FilesWalkResult:
    """Per-CIK walk telemetry."""

    ciks_visited: int = 0
    secondary_pages_fetched: int = 0
    filings_upserted: int = 0
    parse_errors: int = 0


def _list_cik_secondary_pages(
    conn: psycopg.Connection[Any],
) -> list[tuple[int, str, str]]:
    """Return ``[(instrument_id, cik_padded, symbol)]`` for every
    CIK-mapped universe instrument.

    Joins ``external_identifiers`` to ``instruments`` so the writer
    below threads the canonical ticker through to
    ``_normalise_submissions_block`` + ``_upsert_filing``. Passing
    ``str(instrument_id)`` as the symbol corrupts every
    ``filing_events.raw_payload_json`` row — Codex review BLOCKING
    for PR #1035, parity with the same fix in PR #1030 (C1.a).
    """
    out: list[tuple[int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ei.instrument_id, ei.identifier_value, i.symbol
            FROM external_identifiers ei
            JOIN instruments i ON i.instrument_id = ei.instrument_id
            WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik'
            """,
        )
        for row in cur.fetchall():
            instrument_id, identifier, symbol = row
            cik = str(identifier).zfill(10)
            out.append((int(instrument_id), cik, str(symbol or "")))
    return out


def walk_files_pages(
    *,
    conn: psycopg.Connection[Any],
) -> FilesWalkResult:
    """Walk ``filings.files[]`` secondary pages for every CIK-mapped
    universe instrument and append discovered filings to
    ``filing_events``.

    The walker reuses the existing ``SecFilingsProvider`` HTTP client
    (which already shares the process-wide rate-limit clock) so this
    pass coexists with concurrent SEC ingest jobs without bursting
    the per-IP budget.
    """
    result = FilesWalkResult()
    targets = _list_cik_secondary_pages(conn)

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        for instrument_id, cik, symbol in targets:
            result.ciks_visited += 1
            try:
                # Fetch primary submissions.json (rate-limited).
                # The provider exposes ``fetch_submissions(cik)`` which
                # returns the parsed dict for the given CIK; the dict's
                # ``filings.files[]`` array names secondary pages.
                primary = provider.fetch_submissions(cik)
            except Exception as exc:  # noqa: BLE001
                logger.debug("files walk: primary fetch failed for CIK %s: %s", cik, exc)
                result.parse_errors += 1
                continue

            if not isinstance(primary, dict):
                continue
            filings_block = primary.get("filings")
            files: list[Any] = []
            if isinstance(filings_block, dict):
                raw_files = filings_block.get("files")
                if isinstance(raw_files, list):
                    files = raw_files

            for entry in files:
                if not isinstance(entry, dict):
                    continue
                page_name = entry.get("name")
                if not page_name:
                    continue
                try:
                    page = provider.fetch_submissions_page(page_name)
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "files walk: secondary fetch failed for CIK %s/%s: %s",
                        cik,
                        page_name,
                        exc,
                    )
                    result.parse_errors += 1
                    continue
                if page is None:
                    continue

                result.secondary_pages_fetched += 1
                try:
                    # ``symbol`` is the canonical ticker (e.g. "AAPL"),
                    # NOT a stringified instrument_id. Threaded from
                    # the universe lookup at the top of the walk so
                    # ``filing_events.raw_payload_json`` carries the
                    # right value. Codex review BLOCKING for PR #1035.
                    filings = _normalise_submissions_block(
                        page,
                        cik_padded=cik,
                        symbol=symbol or cik,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug("files walk: normalise failed for %s: %s", page_name, exc)
                    result.parse_errors += 1
                    continue

                for filing in filings:
                    try:
                        with conn.transaction():
                            _upsert_filing(conn, str(instrument_id), "sec", filing)
                            result.filings_upserted += 1
                    except Exception as exc:  # noqa: BLE001
                        logger.debug(
                            "files walk: upsert failed for %s/%s: %s",
                            cik,
                            filing.provider_filing_id,
                            exc,
                        )
                        result.parse_errors += 1
    return result


# Job invoker registered in app/jobs/runtime.py:_INVOKERS.
JOB_SEC_SUBMISSIONS_FILES_WALK = "sec_submissions_files_walk"


def sec_submissions_files_walk_job() -> None:
    """Zero-arg job invoker for the runtime registry.

    Within an orchestrated bootstrap run, validates C1.a's
    rows_written > 0 before walking. Records its own per-run
    archive_result row so D-stages can verify provenance.
    """
    from app.services.bootstrap_preconditions import (
        assert_c1b_preconditions,
        record_archive_result,
    )

    # Find current bootstrap_run.
    run_id: int | None = None
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM bootstrap_runs WHERE status='running' ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            run_id = int(row[0]) if row else None
        if run_id is not None:
            assert_c1b_preconditions(conn, bootstrap_run_id=run_id)

    with psycopg.connect(settings.database_url) as conn:
        result = walk_files_pages(conn=conn)
        conn.commit()
    logger.info(
        "sec_submissions_files_walk: ciks=%d pages=%d filings=%d parse_errors=%d",
        result.ciks_visited,
        result.secondary_pages_fetched,
        result.filings_upserted,
        result.parse_errors,
    )
    if run_id is not None:
        with psycopg.connect(settings.database_url) as conn:
            record_archive_result(
                conn,
                bootstrap_run_id=run_id,
                stage_key="sec_submissions_files_walk",
                archive_name="__job__",
                rows_written=result.filings_upserted,
                rows_skipped={"parse_errors": result.parse_errors},
            )
            conn.commit()
