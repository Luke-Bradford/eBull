"""Zero-arg job invokers for the bulk-archive Phase C ingesters (#1027).

Each invoker is a thin wrapper the runtime registry can dispatch with
no arguments. The wrappers:

  * Open a fresh ``psycopg`` connection against ``settings.database_url``.
  * Resolve the cached archive path under
    ``resolve_data_dir() / "sec" / "bulk" / <name>``.
  * Skip with a clear log line if the archive is missing (slow-connection
    fallback path skipped Phase A3).
  * Call the matching service-layer ingester.
  * Commit + log.

Each wrapper is registered in ``app/jobs/runtime.py:_INVOKERS`` so the
orchestrator + admin UI can dispatch them.

Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Final

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.sec_13f_dataset_ingest import ingest_13f_dataset_archive
from app.services.sec_companyfacts_ingest import ingest_companyfacts_archive
from app.services.sec_insider_dataset_ingest import ingest_insider_dataset_archive
from app.services.sec_nport_dataset_ingest import ingest_nport_dataset_archive
from app.services.sec_submissions_ingest import ingest_submissions_archive

logger = logging.getLogger(__name__)


JOB_SEC_SUBMISSIONS_INGEST: Final[str] = "sec_submissions_ingest"
JOB_SEC_COMPANYFACTS_INGEST: Final[str] = "sec_companyfacts_ingest"
JOB_SEC_13F_INGEST_FROM_DATASET: Final[str] = "sec_13f_ingest_from_dataset"
JOB_SEC_INSIDER_INGEST_FROM_DATASET: Final[str] = "sec_insider_ingest_from_dataset"
JOB_SEC_NPORT_INGEST_FROM_DATASET: Final[str] = "sec_nport_ingest_from_dataset"


def _bulk_dir() -> Path:
    return resolve_data_dir() / "sec" / "bulk"


def _archive_path(name: str) -> Path:
    return _bulk_dir() / name


def _list_archives_matching(prefix: str) -> list[Path]:
    """Return every archive in the cache whose filename starts with ``prefix``.

    Used for the multi-quarter Phase C ingesters (13F, insider, N-PORT)
    which iterate every quarterly ZIP in the cache.
    """
    base = _bulk_dir()
    if not base.exists():
        return []
    return sorted(p for p in base.iterdir() if p.is_file() and p.name.startswith(prefix))


def _run_with_conn(fn: Callable[[psycopg.Connection[tuple]], object]) -> None:
    """Open a fresh psycopg connection, run ``fn``, commit."""
    with psycopg.connect(settings.database_url) as conn:
        fn(conn)
        conn.commit()


def _delete_archive_after_success(archive: Path) -> None:
    """Delete a successfully-ingested archive from the bulk cache.

    Disk-hygiene policy (#1020 follow-up): once an archive's contents
    are durably committed to Postgres, the multi-GB ZIP on disk is
    dead weight — leaving it around bloats ``<data_dir>/sec/bulk/``
    by ~5–6 GB per bootstrap and stales every subsequent download
    (the next run's pre-flight then has to re-download anyway, but
    cleanly via ``download_bulk_archives``).

    Failures elsewhere in the pipeline are unaffected — only the
    successful-ingest path deletes. ``OSError`` is logged and
    swallowed so a permission glitch does not unwind the commit.
    """
    try:
        archive.unlink(missing_ok=True)
        logger.info("disk hygiene: deleted ingested archive %s", archive)
    except OSError as exc:
        logger.warning("disk hygiene: failed to delete %s: %s", archive, exc)


# ---------------------------------------------------------------------------
# C1.a — submissions.zip ingester
# ---------------------------------------------------------------------------


def sec_submissions_ingest_job() -> None:
    archive = _archive_path("submissions.zip")
    if not archive.exists():
        logger.info("sec_submissions_ingest: archive %s not present, skipping", archive)
        return

    def _do(conn: psycopg.Connection[tuple]) -> None:
        result = ingest_submissions_archive(conn=conn, archive_path=archive)
        logger.info(
            "sec_submissions_ingest: matched=%d filings_upserted=%d profiles=%d parse_errors=%d",
            result.instruments_matched,
            result.filings_upserted,
            result.profiles_upserted,
            result.parse_errors,
        )

    _run_with_conn(_do)
    _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C2 — companyfacts.zip ingester
# ---------------------------------------------------------------------------


def sec_companyfacts_ingest_job() -> None:
    archive = _archive_path("companyfacts.zip")
    if not archive.exists():
        logger.info("sec_companyfacts_ingest: archive %s not present, skipping", archive)
        return

    def _do(conn: psycopg.Connection[tuple]) -> None:
        result = ingest_companyfacts_archive(conn=conn, archive_path=archive)
        logger.info(
            "sec_companyfacts_ingest: matched=%d facts_upserted=%d parse_errors=%d",
            result.instruments_matched,
            result.facts_upserted,
            result.parse_errors,
        )

    _run_with_conn(_do)
    _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C3 — Form 13F dataset ingester (one job, walks all cached quarters)
# ---------------------------------------------------------------------------


def sec_13f_ingest_from_dataset_job() -> None:
    archives = _list_archives_matching("form13f_")
    if not archives:
        logger.info("sec_13f_ingest_from_dataset: no form13f_*.zip cached, skipping")
        return

    # Per-archive commit so an exception on archive N does not roll
    # back archives 1..N-1's writes (Codex review WARNING for PR #1035).
    total_written = 0
    total_skipped = 0
    for archive in archives:
        with psycopg.connect(settings.database_url) as conn:
            try:
                result = ingest_13f_dataset_archive(conn=conn, archive_path=archive)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("sec_13f_ingest_from_dataset: archive=%s failed", archive.name)
                continue
        _delete_archive_after_success(archive)
        total_written += result.rows_written
        total_skipped += result.rows_skipped_unresolved_cusip
        logger.info(
            "sec_13f_ingest_from_dataset: archive=%s rows_written=%d unresolved_cusip=%d",
            archive.name,
            result.rows_written,
            result.rows_skipped_unresolved_cusip,
        )
    logger.info(
        "sec_13f_ingest_from_dataset: total_rows_written=%d total_unresolved=%d",
        total_written,
        total_skipped,
    )


# ---------------------------------------------------------------------------
# C4 — Insider Transactions dataset ingester
# ---------------------------------------------------------------------------


def sec_insider_ingest_from_dataset_job() -> None:
    archives = _list_archives_matching("insider_")
    if not archives:
        logger.info("sec_insider_ingest_from_dataset: no insider_*.zip cached, skipping")
        return

    # Per-archive commit per Codex review WARNING for PR #1035.
    total_written = 0
    for archive in archives:
        with psycopg.connect(settings.database_url) as conn:
            try:
                result = ingest_insider_dataset_archive(conn=conn, archive_path=archive)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("sec_insider_ingest_from_dataset: archive=%s failed", archive.name)
                continue
        _delete_archive_after_success(archive)
        total_written += result.rows_written
        logger.info(
            "sec_insider_ingest_from_dataset: archive=%s rows_written=%d unresolved_cik=%d",
            archive.name,
            result.rows_written,
            result.rows_skipped_unresolved_cik,
        )
    logger.info(
        "sec_insider_ingest_from_dataset: total_rows_written=%d",
        total_written,
    )


# ---------------------------------------------------------------------------
# C5 — Form N-PORT dataset ingester
# ---------------------------------------------------------------------------


def sec_nport_ingest_from_dataset_job() -> None:
    archives = _list_archives_matching("nport_")
    if not archives:
        logger.info("sec_nport_ingest_from_dataset: no nport_*.zip cached, skipping")
        return

    # Per-archive commit per Codex review WARNING for PR #1035.
    total_written = 0
    for archive in archives:
        with psycopg.connect(settings.database_url) as conn:
            try:
                result = ingest_nport_dataset_archive(conn=conn, archive_path=archive)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("sec_nport_ingest_from_dataset: archive=%s failed", archive.name)
                continue
        _delete_archive_after_success(archive)
        total_written += result.rows_written
        logger.info(
            "sec_nport_ingest_from_dataset: archive=%s rows_written=%d unresolved_cusip=%d non_equity=%d",
            archive.name,
            result.rows_written,
            result.rows_skipped_unresolved_cusip,
            result.rows_skipped_non_equity,
        )
    logger.info(
        "sec_nport_ingest_from_dataset: total_rows_written=%d",
        total_written,
    )
