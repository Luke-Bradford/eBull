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
from app.services.sec_bulk_download import build_bulk_archive_inventory
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


def _current_running_bootstrap_run_id() -> int | None:
    """Read the currently-running bootstrap_run id, if any.

    Returns ``None`` only when there is genuinely no running run —
    the operator may be invoking a Phase C job standalone. A DB
    error here is RAISED, not swallowed, because returning None on
    a connection hiccup would make C-stages skip every precondition
    and silently no-op (the very failure mode #1020 fixes).
    PR review WARNING (bot, PR #1038).
    """
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM bootstrap_runs
                WHERE status = 'running'
                ORDER BY id DESC LIMIT 1
                """,
            )
            row = cur.fetchone()
            return int(row[0]) if row else None


def _record_archive_result(
    *,
    bootstrap_run_id: int,
    stage_key: str,
    archive_name: str,
    rows_written: int,
    rows_skipped: dict[str, int] | None = None,
) -> None:
    """Write an audit-trail row in ``bootstrap_archive_results``."""
    from app.services.bootstrap_preconditions import record_archive_result

    with psycopg.connect(settings.database_url) as conn:
        record_archive_result(
            conn,
            bootstrap_run_id=bootstrap_run_id,
            stage_key=stage_key,
            archive_name=archive_name,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
        )
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
    from app.services.bootstrap_preconditions import assert_c1a_preconditions

    run_id = _current_running_bootstrap_run_id()
    archive = _archive_path("submissions.zip")

    # Within an orchestrated run: preconditions must pass before any
    # write. Standalone manual invocation (no run): fall back to the
    # legacy "ingest if archive exists" path.
    if run_id is not None:
        with psycopg.connect(settings.database_url) as conn:
            assert_c1a_preconditions(conn, bootstrap_run_id=run_id, bulk_dir=_bulk_dir())
        if not archive.exists():
            from app.services.sec_bulk_download import BootstrapPartialDownloadError

            raise BootstrapPartialDownloadError(
                f"sec_submissions_ingest: archive {archive} not present; "
                f"upstream sec_bulk_download did not land submissions.zip."
            )
    elif not archive.exists():
        logger.info("sec_submissions_ingest: archive %s not present, skipping (no run)", archive)
        return

    captured: dict[str, int] = {}

    def _do(conn: psycopg.Connection[tuple]) -> None:
        result = ingest_submissions_archive(conn=conn, archive_path=archive)
        captured["filings_upserted"] = result.filings_upserted
        captured["profiles_upserted"] = result.profiles_upserted
        captured["parse_errors"] = result.parse_errors
        logger.info(
            "sec_submissions_ingest: matched=%d filings_upserted=%d profiles=%d parse_errors=%d",
            result.instruments_matched,
            result.filings_upserted,
            result.profiles_upserted,
            result.parse_errors,
        )

    _run_with_conn(_do)
    if run_id is not None:
        _record_archive_result(
            bootstrap_run_id=run_id,
            stage_key="sec_submissions_ingest",
            archive_name="submissions.zip",
            rows_written=captured.get("filings_upserted", 0),
            rows_skipped={"parse_errors": captured.get("parse_errors", 0)},
        )
    _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C2 — companyfacts.zip ingester
# ---------------------------------------------------------------------------


def sec_companyfacts_ingest_job() -> None:
    from app.services.bootstrap_preconditions import assert_c2_preconditions

    run_id = _current_running_bootstrap_run_id()
    archive = _archive_path("companyfacts.zip")

    if run_id is not None:
        with psycopg.connect(settings.database_url) as conn:
            assert_c2_preconditions(conn, bootstrap_run_id=run_id, bulk_dir=_bulk_dir())
        if not archive.exists():
            from app.services.sec_bulk_download import BootstrapPartialDownloadError

            raise BootstrapPartialDownloadError(
                f"sec_companyfacts_ingest: archive {archive} not present; "
                f"upstream sec_bulk_download did not land companyfacts.zip."
            )
    elif not archive.exists():
        logger.info("sec_companyfacts_ingest: archive %s not present, skipping (no run)", archive)
        return

    captured: dict[str, int] = {}

    def _do(conn: psycopg.Connection[tuple]) -> None:
        result = ingest_companyfacts_archive(conn=conn, archive_path=archive)
        captured["facts_upserted"] = result.facts_upserted
        captured["parse_errors"] = result.parse_errors
        logger.info(
            "sec_companyfacts_ingest: matched=%d facts_upserted=%d parse_errors=%d",
            result.instruments_matched,
            result.facts_upserted,
            result.parse_errors,
        )

    _run_with_conn(_do)
    if run_id is not None:
        _record_archive_result(
            bootstrap_run_id=run_id,
            stage_key="sec_companyfacts_ingest",
            archive_name="companyfacts.zip",
            rows_written=captured.get("facts_upserted", 0),
            rows_skipped={"parse_errors": captured.get("parse_errors", 0)},
        )
    _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C3 — Form 13F dataset ingester (one job, walks all cached quarters)
# ---------------------------------------------------------------------------


def sec_13f_ingest_from_dataset_job() -> None:
    from app.services.bootstrap_preconditions import assert_c3_preconditions
    from app.services.sec_bulk_download import BootstrapPartialDownloadError

    run_id = _current_running_bootstrap_run_id()
    archives = _list_archives_matching("form13f_")

    expected: list[str] | None = None
    if run_id is not None:
        expected = [a.name for a in build_bulk_archive_inventory() if a.name.startswith("form13f_")]
        with psycopg.connect(settings.database_url) as conn:
            assert_c3_preconditions(
                conn,
                bootstrap_run_id=run_id,
                bulk_dir=_bulk_dir(),
                expected_archive_names=expected,
            )
        # Restrict loop to the expected manifest-backed names so
        # stale archives left on disk from a prior run are NOT
        # ingested under the current run.
        archives = [a for a in archives if a.name in set(expected)]
        # All expected files MUST be on disk after manifest provenance
        # passes; missing physical file is a partial-download failure.
        present_names = {a.name for a in archives}
        missing = [n for n in expected if n not in present_names]
        if missing:
            from app.services.sec_bulk_download import BootstrapPartialDownloadError

            raise BootstrapPartialDownloadError(
                f"expected archives missing on disk after preconditions passed: {missing}"
            )
        if not archives:
            raise BootstrapPartialDownloadError(
                "sec_13f_ingest_from_dataset: no form13f_*.zip cached; "
                "upstream sec_bulk_download did not land 13F archives."
            )
    elif not archives:
        logger.info("sec_13f_ingest_from_dataset: no form13f_*.zip cached, skipping (no run)")
        return

    # Per-archive commit so an exception on archive N does not roll
    # back archives 1..N-1's writes. Defer archive deletion until
    # the WHOLE stage has succeeded — if a later archive fails,
    # retry needs all manifest archives on disk (Codex sweep
    # BLOCKING).
    failed_archives: list[str] = []
    total_written = 0
    total_skipped = 0
    succeeded: list[Path] = []
    touched_ids: set[int] = set()
    for archive in archives:
        with psycopg.connect(settings.database_url) as conn:
            try:
                result = ingest_13f_dataset_archive(conn=conn, archive_path=archive)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                logger.exception("sec_13f_ingest_from_dataset: archive=%s failed", archive.name)
                failed_archives.append(f"{archive.name}: {exc}")
                continue
        succeeded.append(archive)
        total_written += result.rows_written
        total_skipped += result.rows_skipped_unresolved_cusip
        touched_ids |= result.touched_instrument_ids
        if run_id is not None:
            _record_archive_result(
                bootstrap_run_id=run_id,
                stage_key="sec_13f_ingest_from_dataset",
                archive_name=archive.name,
                rows_written=result.rows_written,
                rows_skipped={"unresolved_cusip": result.rows_skipped_unresolved_cusip},
            )
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
    if failed_archives:
        raise RuntimeError(
            f"sec_13f_ingest_from_dataset: {len(failed_archives)} archives failed: " + "; ".join(failed_archives)
        )
    if run_id is not None and total_written == 0:
        raise RuntimeError(
            f"sec_13f_ingest_from_dataset: aggregate rows_written=0 across {len(archives)} archives; "
            f"unresolved_cusip={total_skipped}. Check CUSIP coverage."
        )
    # Refresh ownership_institutions_current for every instrument
    # whose observations changed. Bulk ingest writes only to the
    # _observations table; the rollup endpoint reads _current. Without
    # this refresh AAPL/MSFT etc. show 0 institutional ownership even
    # after a successful ingest. Codex sweep BLOCKING for #1020.
    #
    # Refresh failures MUST propagate before disk cleanup — otherwise
    # observations land but _current is stale AND archives are deleted,
    # leaving no retry input. Codex pre-push BLOCKING for #1020.
    if touched_ids:
        from app.services.ownership_observations import refresh_institutions_current

        refresh_failures: list[str] = []
        with psycopg.connect(settings.database_url) as conn:
            for instrument_id in sorted(touched_ids):
                # Per-iteration savepoint — refresh_institutions_current
                # owns its own ``with conn.transaction()`` (sql/.py:404),
                # so this is defence-in-depth against a future refactor
                # of the helper that drops its internal txn wrap. PR
                # review BLOCKING for #1047.
                try:
                    with conn.transaction():
                        refresh_institutions_current(conn, instrument_id=instrument_id)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "sec_13f_ingest_from_dataset: refresh_institutions_current failed for instrument=%d",
                        instrument_id,
                    )
                    refresh_failures.append(f"instrument={instrument_id}: {exc}")
        if refresh_failures:
            raise RuntimeError(
                f"sec_13f_ingest_from_dataset: {len(refresh_failures)}/{len(touched_ids)} "
                f"_current refreshes failed; archives retained for retry: " + "; ".join(refresh_failures[:5])
            )
        logger.info(
            "sec_13f_ingest_from_dataset: refreshed ownership_institutions_current for %d instruments",
            len(touched_ids),
        )
    # Stage succeeded — now safe to free disk.
    for archive in succeeded:
        _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C4 — Insider Transactions dataset ingester
# ---------------------------------------------------------------------------


def sec_insider_ingest_from_dataset_job() -> None:
    from app.services.bootstrap_preconditions import assert_c4_preconditions
    from app.services.sec_bulk_download import BootstrapPartialDownloadError

    run_id = _current_running_bootstrap_run_id()
    archives = _list_archives_matching("insider_")

    expected: list[str] | None = None
    if run_id is not None:
        expected = [a.name for a in build_bulk_archive_inventory() if a.name.startswith("insider_")]
        with psycopg.connect(settings.database_url) as conn:
            assert_c4_preconditions(
                conn,
                bootstrap_run_id=run_id,
                bulk_dir=_bulk_dir(),
                expected_archive_names=expected,
            )
        archives = [a for a in archives if a.name in set(expected)]
        # All expected files MUST be on disk after manifest provenance
        # passes; missing physical file is a partial-download failure.
        present_names = {a.name for a in archives}
        missing = [n for n in expected if n not in present_names]
        if missing:
            from app.services.sec_bulk_download import BootstrapPartialDownloadError

            raise BootstrapPartialDownloadError(
                f"expected archives missing on disk after preconditions passed: {missing}"
            )
        if not archives:
            raise BootstrapPartialDownloadError(
                "sec_insider_ingest_from_dataset: no insider_*.zip cached; "
                "upstream sec_bulk_download did not land insider archives."
            )
    elif not archives:
        logger.info("sec_insider_ingest_from_dataset: no insider_*.zip cached, skipping (no run)")
        return

    failed_archives: list[str] = []
    total_written = 0
    succeeded: list[Path] = []
    touched_ids: set[int] = set()
    for archive in archives:
        with psycopg.connect(settings.database_url) as conn:
            try:
                result = ingest_insider_dataset_archive(conn=conn, archive_path=archive)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                logger.exception("sec_insider_ingest_from_dataset: archive=%s failed", archive.name)
                failed_archives.append(f"{archive.name}: {exc}")
                continue
        succeeded.append(archive)
        total_written += result.rows_written
        touched_ids |= result.touched_instrument_ids
        if run_id is not None:
            _record_archive_result(
                bootstrap_run_id=run_id,
                stage_key="sec_insider_ingest_from_dataset",
                archive_name=archive.name,
                rows_written=result.rows_written,
                rows_skipped={"unresolved_cik": result.rows_skipped_unresolved_cik},
            )
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
    if failed_archives:
        raise RuntimeError(
            f"sec_insider_ingest_from_dataset: {len(failed_archives)} archives failed: " + "; ".join(failed_archives)
        )
    if run_id is not None and total_written == 0:
        raise RuntimeError(
            f"sec_insider_ingest_from_dataset: aggregate rows_written=0 across {len(archives)} archives. "
            "Check CIK coverage."
        )
    # Refresh ownership_insiders_current for every instrument whose
    # observations moved. See sec_13f_ingest_from_dataset_job rationale
    # — refresh failures propagate before disk cleanup so the archives
    # remain available for retry. Codex pre-push BLOCKING for #1020.
    if touched_ids:
        from app.services.ownership_observations import refresh_insiders_current

        refresh_failures: list[str] = []
        with psycopg.connect(settings.database_url) as conn:
            for instrument_id in sorted(touched_ids):
                try:
                    with conn.transaction():
                        refresh_insiders_current(conn, instrument_id=instrument_id)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "sec_insider_ingest_from_dataset: refresh_insiders_current failed for instrument=%d",
                        instrument_id,
                    )
                    refresh_failures.append(f"instrument={instrument_id}: {exc}")
        if refresh_failures:
            raise RuntimeError(
                f"sec_insider_ingest_from_dataset: {len(refresh_failures)}/{len(touched_ids)} "
                f"_current refreshes failed; archives retained for retry: " + "; ".join(refresh_failures[:5])
            )
        logger.info(
            "sec_insider_ingest_from_dataset: refreshed ownership_insiders_current for %d instruments",
            len(touched_ids),
        )
    for archive in succeeded:
        _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C5 — Form N-PORT dataset ingester
# ---------------------------------------------------------------------------


def sec_nport_ingest_from_dataset_job() -> None:
    from app.services.bootstrap_preconditions import assert_c5_preconditions
    from app.services.sec_bulk_download import BootstrapPartialDownloadError

    run_id = _current_running_bootstrap_run_id()
    archives = _list_archives_matching("nport_")

    expected: list[str] | None = None
    if run_id is not None:
        expected = [a.name for a in build_bulk_archive_inventory() if a.name.startswith("nport_")]
        with psycopg.connect(settings.database_url) as conn:
            assert_c5_preconditions(
                conn,
                bootstrap_run_id=run_id,
                bulk_dir=_bulk_dir(),
                expected_archive_names=expected,
            )
        archives = [a for a in archives if a.name in set(expected)]
        # All expected files MUST be on disk after manifest provenance
        # passes; missing physical file is a partial-download failure.
        present_names = {a.name for a in archives}
        missing = [n for n in expected if n not in present_names]
        if missing:
            from app.services.sec_bulk_download import BootstrapPartialDownloadError

            raise BootstrapPartialDownloadError(
                f"expected archives missing on disk after preconditions passed: {missing}"
            )
        if not archives:
            raise BootstrapPartialDownloadError(
                "sec_nport_ingest_from_dataset: no nport_*.zip cached; "
                "upstream sec_bulk_download did not land NPORT archives."
            )
    elif not archives:
        logger.info("sec_nport_ingest_from_dataset: no nport_*.zip cached, skipping (no run)")
        return

    failed_archives: list[str] = []
    total_written = 0
    succeeded: list[Path] = []
    touched_ids: set[int] = set()
    for archive in archives:
        with psycopg.connect(settings.database_url) as conn:
            try:
                result = ingest_nport_dataset_archive(conn=conn, archive_path=archive)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                logger.exception("sec_nport_ingest_from_dataset: archive=%s failed", archive.name)
                failed_archives.append(f"{archive.name}: {exc}")
                continue
        succeeded.append(archive)
        total_written += result.rows_written
        touched_ids |= result.touched_instrument_ids
        if run_id is not None:
            _record_archive_result(
                bootstrap_run_id=run_id,
                stage_key="sec_nport_ingest_from_dataset",
                archive_name=archive.name,
                rows_written=result.rows_written,
                rows_skipped={
                    "unresolved_cusip": result.rows_skipped_unresolved_cusip,
                    "non_equity": result.rows_skipped_non_equity,
                },
            )
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
    if failed_archives:
        raise RuntimeError(
            f"sec_nport_ingest_from_dataset: {len(failed_archives)} archives failed: " + "; ".join(failed_archives)
        )
    if run_id is not None and total_written == 0:
        raise RuntimeError(
            f"sec_nport_ingest_from_dataset: aggregate rows_written=0 across {len(archives)} archives. "
            "Check CUSIP coverage."
        )
    # Refresh ownership_funds_current for every instrument whose
    # fund-holdings observations moved. See C3 job rationale — refresh
    # failures propagate before disk cleanup so archives stay
    # retryable. Codex pre-push BLOCKING for #1020.
    if touched_ids:
        from app.services.ownership_observations import refresh_funds_current

        refresh_failures: list[str] = []
        with psycopg.connect(settings.database_url) as conn:
            for instrument_id in sorted(touched_ids):
                try:
                    with conn.transaction():
                        refresh_funds_current(conn, instrument_id=instrument_id)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "sec_nport_ingest_from_dataset: refresh_funds_current failed for instrument=%d",
                        instrument_id,
                    )
                    refresh_failures.append(f"instrument={instrument_id}: {exc}")
        if refresh_failures:
            raise RuntimeError(
                f"sec_nport_ingest_from_dataset: {len(refresh_failures)}/{len(touched_ids)} "
                f"_current refreshes failed; archives retained for retry: " + "; ".join(refresh_failures[:5])
            )
        logger.info(
            "sec_nport_ingest_from_dataset: refreshed ownership_funds_current for %d instruments",
            len(touched_ids),
        )
    for archive in succeeded:
        _delete_archive_after_success(archive)
