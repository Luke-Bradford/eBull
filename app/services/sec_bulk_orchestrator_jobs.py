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
from app.services.fsds_class_shares import ingest_fsds_class_shares_archive
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
JOB_SEC_FSDS_CLASS_SHARES_INGEST: Final[str] = "sec_fsds_class_shares_ingest"
JOB_SEC_FSDS_DIMENSIONAL_INGEST: Final[str] = "sec_fsds_dimensional_ingest"


# Post-ingest batched _current refresh chunk size (PR-4 spec §8).
#
# Each post-ingest job (13F / insider / NPORT) calls the matching
# ``refresh_<category>_current_batch`` helper in chunks so the
# cancel-poll cadence remains interactive. The single-instrument loop
# polled every 50 ids; the batch path round-trips ONE MERGE per chunk,
# so cancel observation latency degrades from ~50 instruments to
# ~_REFRESH_BATCH_CHUNK_SIZE instruments. 200 is the documented
# trade-off — large enough to dominate the per-batch fixed cost
# (server-side hash-sort + lock acquire), small enough that an
# operator cancel still lands within ~5 s of the next chunk boundary
# even on a slow disk.
_REFRESH_BATCH_CHUNK_SIZE: Final[int] = 200


def _bulk_dir() -> Path:
    return resolve_data_dir() / "sec" / "bulk"


def _archive_path(name: str) -> Path:
    return _bulk_dir() / name


def _list_archives_matching(prefix: str) -> list[Path]:
    """Return every ``.zip`` archive in the cache whose filename starts with ``prefix``.

    Used for the multi-quarter Phase C ingesters (13F, insider, N-PORT, FSDS)
    which iterate every quarterly ZIP in the cache.

    The ``.zip`` suffix filter is load-bearing (#1576): the downloader writes
    ``<archive>.zip.etag`` / ``.zip.sha256`` sidecars next to each zip, and those
    also match ``startswith(prefix)``. In orchestrated bootstrap mode the run-manifest
    name filter hides them, but in STANDALONE mode (run_id None — manual trigger /
    direct invocation) each sidecar reached ``zipfile.ZipFile`` and logged a
    ``BadZipFile`` ERROR per sidecar per run (caught per-archive, so pure noise +
    misleading "failed" lines). All callers only ever want the zips.
    """
    base = _bulk_dir()
    if not base.exists():
        return []
    return sorted(p for p in base.iterdir() if p.is_file() and p.name.startswith(prefix) and p.name.endswith(".zip"))


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
        # Stream A PR-B T1.3 (#1233): per-archive sidecar telemetry
        # captured here so the audit row below persists it. Without
        # this, the Stream-C C7 gate has no auditable per-run figure
        # for sidecar coverage (DE MEDIUM pre-push review).
        captured["ciks_sidecared"] = result.ciks_sidecared
        captured["sidecar_pages_indexed"] = result.sidecar_pages_indexed
        logger.info(
            "sec_submissions_ingest: matched=%d filings_upserted=%d profiles=%d parse_errors=%d "
            "ciks_sidecared=%d sidecar_pages_indexed=%d",
            result.instruments_matched,
            result.filings_upserted,
            result.profiles_upserted,
            result.parse_errors,
            result.ciks_sidecared,
            result.sidecar_pages_indexed,
        )

    _run_with_conn(_do)
    if run_id is not None:
        _record_archive_result(
            bootstrap_run_id=run_id,
            stage_key="sec_submissions_ingest",
            archive_name="submissions.zip",
            rows_written=captured.get("filings_upserted", 0),
            rows_skipped={
                "parse_errors": captured.get("parse_errors", 0),
                "ciks_sidecared": captured.get("ciks_sidecared", 0),
                "sidecar_pages_indexed": captured.get("sidecar_pages_indexed", 0),
            },
        )
    # #1277 — submissions.zip deletion deferred to S16
    # sec_first_install_drain so the hybrid HttpGet can read PRIMARY
    # CIK<10>.json entries from disk for the non-issuer cohort. S16's
    # ``_cleanup_submissions_zip_after_drain`` is called unconditionally
    # on the drain SUCCESS path (zip OR HTTP fallback) — disk hygiene
    # preserved end-to-end. Other bulk archives (companyfacts, etc.)
    # keep their existing post-ingest deletion in sibling jobs.


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
        captured["facts_skipped"] = result.facts_skipped
        captured["parse_errors"] = result.parse_errors
        logger.info(
            "sec_companyfacts_ingest: matched=%d facts_upserted=%d facts_skipped=%d parse_errors=%d",
            result.instruments_matched,
            result.facts_upserted,
            result.facts_skipped,
            result.parse_errors,
        )

    _run_with_conn(_do)
    if run_id is not None:
        # #1294: ``rows_written`` semantics drive the strict-gate
        # ``fundamentals_raw_seeded`` cap (rows_processed >= 1). Using
        # ``facts_upserted`` alone undercounts on re-runs where every
        # value is unchanged: psycopg's ``ON CONFLICT DO UPDATE WHERE
        # IS DISTINCT FROM`` filter returns rowcount=0 for idempotent
        # re-upserts. The cap then false-blocks S25 fundamentals_sync
        # even though the financial_facts_raw partition is fully
        # populated.
        # Fix: account ``facts_upserted + facts_skipped`` as
        # "rows the upsert path saw". This matches the cap's intended
        # semantic ("data passed through the upsert layer") rather
        # than the narrower "rows that mutated" semantic.
        facts_seen = captured.get("facts_upserted", 0) + captured.get("facts_skipped", 0)
        _record_archive_result(
            bootstrap_run_id=run_id,
            stage_key="sec_companyfacts_ingest",
            archive_name="companyfacts.zip",
            rows_written=facts_seen,
            rows_skipped={
                "parse_errors": captured.get("parse_errors", 0),
                "facts_unchanged": captured.get("facts_skipped", 0),
            },
        )
    _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C3 — Form 13F dataset ingester (one job, walks all cached quarters)
# ---------------------------------------------------------------------------


def sec_13f_ingest_from_dataset_job() -> None:
    from app.services.bootstrap_preconditions import assert_c3_preconditions
    from app.services.sec_bulk_download import BootstrapPartialDownloadError, read_run_manifest

    run_id = _current_running_bootstrap_run_id()
    archives = _list_archives_matching("form13f_")

    if run_id is not None:
        # #1423 — the newest 13F rolling window is optional (SEC publishes
        # it weeks after the quarter closes). Only the non-optional windows
        # gate the stage; the optional one may legitimately be absent.
        inventory_13f = [a for a in build_bulk_archive_inventory() if a.name.startswith("form13f_")]
        required = [a.name for a in inventory_13f if not a.optional]
        with psycopg.connect(settings.database_url) as conn:
            assert_c3_preconditions(
                conn,
                bootstrap_run_id=run_id,
                bulk_dir=_bulk_dir(),
                expected_archive_names=required,
            )
        # Restrict the loop to archives recorded in THIS run's manifest.
        # The manifest only lists archives that landed (or were ETag-reused)
        # under the current bootstrap_run_id, so this is the provenance gate:
        # it admits a successfully-downloaded optional window while excluding
        # a stale same-name zip left on disk from a prior run (Codex ckpt-2).
        manifest = read_run_manifest(_bulk_dir())
        manifest_names = {entry["name"] for entry in manifest.get("archives", [])} if manifest else set()
        archives = [a for a in archives if a.name in manifest_names]
        # Every REQUIRED file MUST be on disk after manifest provenance
        # passes; missing physical file is a partial-download failure. An
        # optional window may be absent (not yet published) — not fatal.
        present_names = {a.name for a in archives}
        missing = [n for n in required if n not in present_names]
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
    # PR3d #1064 follow-up — poll the bootstrap cancel signal between
    # archives. Each archive ingests in 5-20 min; without a poll the
    # operator's cancel is observed only at the next bootstrap-stage
    # boundary, well after the C-stage commits a long-running archive.
    # Outside ``active_bootstrap_run`` (manual trigger / no-run path)
    # the helper short-circuits to False, so non-bootstrap callers are
    # unaffected. Per-archive commits already drop a clean rollback
    # boundary; raising on observed cancel preserves whatever ran
    # before the signal landed.
    from app.services.bootstrap_state import BootstrapStageCancelled
    from app.services.processes.bootstrap_cancel_signal import (
        active_bootstrap_stage_key,
        bootstrap_cancel_requested,
    )

    failed_archives: list[str] = []
    total_written = 0
    total_skipped = 0
    total_retention_skipped = 0
    succeeded: list[Path] = []
    touched_ids: set[int] = set()
    for archive in archives:
        if bootstrap_cancel_requested():
            # #1114: stage_key sourced from contextvar.
            raise BootstrapStageCancelled(
                f"sec_13f_ingest_from_dataset cancelled by operator after {len(succeeded)}/{len(archives)} archives",
                stage_key=active_bootstrap_stage_key() or "",
            )
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
        total_retention_skipped += result.rows_skipped_retention
        touched_ids |= result.touched_instrument_ids
        if run_id is not None:
            _record_archive_result(
                bootstrap_run_id=run_id,
                stage_key="sec_13f_ingest_from_dataset",
                archive_name=archive.name,
                rows_written=result.rows_written,
                rows_skipped={
                    "unresolved_cusip": result.rows_skipped_unresolved_cusip,
                    "retention": result.rows_skipped_retention,
                },
            )
        logger.info(
            "sec_13f_ingest_from_dataset: archive=%s rows_written=%d unresolved_cusip=%d retention_skipped=%d",
            archive.name,
            result.rows_written,
            result.rows_skipped_unresolved_cusip,
            result.rows_skipped_retention,
        )
    logger.info(
        "sec_13f_ingest_from_dataset: total_rows_written=%d total_unresolved=%d total_retention_skipped=%d",
        total_written,
        total_skipped,
        total_retention_skipped,
    )
    if failed_archives:
        raise RuntimeError(
            f"sec_13f_ingest_from_dataset: {len(failed_archives)} archives failed: " + "; ".join(failed_archives)
        )
    if run_id is not None and total_written == 0:
        # Codex 2 MED — distinguish all-retention-skipped from CUSIP-coverage failure.
        # An all-pre-cap drain is a no-op by design (#1233 §4.5); a zero-CUSIP-resolve
        # is still a coverage failure. Only the latter should raise.
        if total_retention_skipped > 0 and total_skipped == 0:
            logger.info(
                "sec_13f_ingest_from_dataset: aggregate rows_written=0 across %d archives; "
                "all %d rows skipped by 8q retention cap (#1233 §4.5). Not an error.",
                len(archives),
                total_retention_skipped,
            )
        else:
            raise RuntimeError(
                f"sec_13f_ingest_from_dataset: aggregate rows_written=0 across {len(archives)} archives; "
                f"unresolved_cusip={total_skipped} retention_skipped={total_retention_skipped}. "
                f"Check CUSIP coverage."
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
        # Codex pre-push round 1: cancel must outrank the post-loop
        # refresh too, otherwise a signal that lands between the last
        # archive's poll and the refresh loop entry would let the
        # stage finish + delete archives + return success while the
        # dispatcher waits for the next stage boundary.
        if bootstrap_cancel_requested():
            # #1114: stage_key sourced from contextvar.
            raise BootstrapStageCancelled(
                f"sec_13f_ingest_from_dataset cancelled by operator before refresh of {len(touched_ids)} instruments",
                stage_key=active_bootstrap_stage_key() or "",
            )

        from app.services.ownership_observations import refresh_institutions_current_batch

        refresh_failures: list[str] = []
        sorted_ids = sorted(touched_ids)
        with psycopg.connect(settings.database_url) as conn:
            # Chunk batched refresh so cancel observation stays interactive.
            # Single-instrument loop polled every 50 ids; batched call
            # round-trips one MERGE per CHUNK_SIZE ids, so we poll between
            # chunks instead. Documented trade-off: cancel latency degrades
            # from ~50 instruments to ~200 instruments. PR-4 spec §8.
            for chunk_start in range(0, len(sorted_ids), _REFRESH_BATCH_CHUNK_SIZE):
                if bootstrap_cancel_requested():
                    raise BootstrapStageCancelled(
                        f"sec_13f_ingest_from_dataset cancelled by operator after "
                        f"refreshing {chunk_start}/{len(sorted_ids)} instruments",
                        stage_key=active_bootstrap_stage_key() or "",
                    )
                chunk = sorted_ids[chunk_start : chunk_start + _REFRESH_BATCH_CHUNK_SIZE]
                # Per-chunk savepoint — refresh_institutions_current_batch
                # owns its own ``with conn.transaction()``; this is
                # defence-in-depth so one bad chunk does not abort the
                # ambient connection's prior chunk commits. Mirrors the
                # single-instrument loop's defence per PR #1047.
                try:
                    with conn.transaction():
                        refresh_institutions_current_batch(conn, instrument_ids=chunk)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "sec_13f_ingest_from_dataset: refresh_institutions_current_batch failed for "
                        "chunk start=%d size=%d",
                        chunk_start,
                        len(chunk),
                    )
                    # Surface the first 5 instrument_ids in the chunk so
                    # operator-visible error still names concrete ids.
                    refresh_failures.append(
                        f"chunk start={chunk_start} ids={chunk[:5]}{'...' if len(chunk) > 5 else ''}: {exc}"
                    )
        if refresh_failures:
            raise RuntimeError(
                f"sec_13f_ingest_from_dataset: {len(refresh_failures)} chunk(s) of {len(sorted_ids)} "
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

    # PR3d #1064 follow-up — see sec_13f_ingest_from_dataset_job for
    # the cancel-poll rationale; same pattern applies per-archive.
    from app.services.bootstrap_state import BootstrapStageCancelled
    from app.services.processes.bootstrap_cancel_signal import (
        active_bootstrap_stage_key,
        bootstrap_cancel_requested,
    )

    failed_archives: list[str] = []
    total_written = 0
    succeeded: list[Path] = []
    touched_ids: set[int] = set()
    for archive in archives:
        if bootstrap_cancel_requested():
            # #1114: stage_key sourced from contextvar.
            raise BootstrapStageCancelled(
                f"sec_insider_ingest_from_dataset cancelled by operator after "
                f"{len(succeeded)}/{len(archives)} archives",
                stage_key=active_bootstrap_stage_key() or "",
            )
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
                rows_skipped={
                    "unresolved_cik": result.rows_skipped_unresolved_cik,
                    "retention": result.rows_skipped_retention,
                },
            )
        logger.info(
            "sec_insider_ingest_from_dataset: archive=%s rows_written=%d unresolved_cik=%d retention_skipped=%d",
            archive.name,
            result.rows_written,
            result.rows_skipped_unresolved_cik,
            result.rows_skipped_retention,
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
        # Codex round 1 — cancel poll before + during refresh loop.
        if bootstrap_cancel_requested():
            # #1114: stage_key sourced from contextvar.
            raise BootstrapStageCancelled(
                f"sec_insider_ingest_from_dataset cancelled by operator before "
                f"refresh of {len(touched_ids)} instruments",
                stage_key=active_bootstrap_stage_key() or "",
            )

        from app.services.ownership_observations import refresh_insiders_current_batch

        refresh_failures: list[str] = []
        sorted_ids = sorted(touched_ids)
        with psycopg.connect(settings.database_url) as conn:
            # See sec_13f_ingest_from_dataset_job for the chunked-batch
            # cancel-poll rationale (PR-4 spec §8).
            for chunk_start in range(0, len(sorted_ids), _REFRESH_BATCH_CHUNK_SIZE):
                if bootstrap_cancel_requested():
                    raise BootstrapStageCancelled(
                        f"sec_insider_ingest_from_dataset cancelled by operator after "
                        f"refreshing {chunk_start}/{len(sorted_ids)} instruments",
                        stage_key=active_bootstrap_stage_key() or "",
                    )
                chunk = sorted_ids[chunk_start : chunk_start + _REFRESH_BATCH_CHUNK_SIZE]
                try:
                    with conn.transaction():
                        refresh_insiders_current_batch(conn, instrument_ids=chunk)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "sec_insider_ingest_from_dataset: refresh_insiders_current_batch failed for "
                        "chunk start=%d size=%d",
                        chunk_start,
                        len(chunk),
                    )
                    refresh_failures.append(
                        f"chunk start={chunk_start} ids={chunk[:5]}{'...' if len(chunk) > 5 else ''}: {exc}"
                    )
        if refresh_failures:
            raise RuntimeError(
                f"sec_insider_ingest_from_dataset: {len(refresh_failures)} chunk(s) of {len(sorted_ids)} "
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

    # PR3d #1064 follow-up — see sec_13f_ingest_from_dataset_job for
    # the cancel-poll rationale; same pattern applies per-archive.
    from app.services.bootstrap_state import BootstrapStageCancelled
    from app.services.processes.bootstrap_cancel_signal import (
        active_bootstrap_stage_key,
        bootstrap_cancel_requested,
    )

    failed_archives: list[str] = []
    total_written = 0
    total_holdings_seen = 0
    total_skipped = 0
    total_retention_skipped = 0
    succeeded: list[Path] = []
    touched_ids: set[int] = set()
    for archive in archives:
        if bootstrap_cancel_requested():
            # #1114: stage_key sourced from contextvar.
            raise BootstrapStageCancelled(
                f"sec_nport_ingest_from_dataset cancelled by operator after {len(succeeded)}/{len(archives)} archives",
                stage_key=active_bootstrap_stage_key() or "",
            )
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
        total_holdings_seen += result.holdings_seen
        total_skipped += result.rows_skipped_unresolved_cusip
        total_retention_skipped += result.rows_skipped_retention
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
                    "retention": result.rows_skipped_retention,
                },
            )
        logger.info(
            "sec_nport_ingest_from_dataset: archive=%s rows_written=%d unresolved_cusip=%d "
            "non_equity=%d retention_skipped=%d",
            archive.name,
            result.rows_written,
            result.rows_skipped_unresolved_cusip,
            result.rows_skipped_non_equity,
            result.rows_skipped_retention,
        )
    logger.info(
        "sec_nport_ingest_from_dataset: total_rows_written=%d total_unresolved=%d total_retention_skipped=%d",
        total_written,
        total_skipped,
        total_retention_skipped,
    )
    if failed_archives:
        raise RuntimeError(
            f"sec_nport_ingest_from_dataset: {len(failed_archives)} archives failed: " + "; ".join(failed_archives)
        )
    if run_id is not None and total_written == 0:
        # PR7 #1233 §4.6 — distinguish all-retention-skipped (no error,
        # by design) from CUSIP-coverage / shape failures. Tighter than
        # PR6's 13F split because ``NPortIngestResult`` carries many
        # more skip buckets (orphan / non_equity / non_long /
        # non_share_units / non_positive_shares / missing_series /
        # bad_data / parse_errors); a loose ``retention_skipped > 0 &&
        # unresolved_cusip == 0`` check would silently suppress
        # RuntimeError when those buckets also contributed to
        # rows_written=0. Only treat as no-op when EVERY observed
        # holding row landed in the retention bucket (Codex 2 WARN on
        # PR7).
        if total_holdings_seen > 0 and total_retention_skipped == total_holdings_seen:
            logger.info(
                "sec_nport_ingest_from_dataset: aggregate rows_written=0 across %d archives; "
                "all %d holdings skipped by 8q retention cap (#1233 §4.6). Not an error.",
                len(archives),
                total_retention_skipped,
            )
        else:
            raise RuntimeError(
                f"sec_nport_ingest_from_dataset: aggregate rows_written=0 across {len(archives)} archives; "
                f"holdings_seen={total_holdings_seen} unresolved_cusip={total_skipped} "
                f"retention_skipped={total_retention_skipped}. Check CUSIP coverage / archive shape."
            )
    # Refresh ownership_funds_current for every instrument whose
    # fund-holdings observations moved. See C3 job rationale — refresh
    # failures propagate before disk cleanup so archives stay
    # retryable. Codex pre-push BLOCKING for #1020.
    if touched_ids:
        # Codex round 1 — cancel poll before + during refresh loop.
        if bootstrap_cancel_requested():
            # #1114: stage_key sourced from contextvar.
            raise BootstrapStageCancelled(
                f"sec_nport_ingest_from_dataset cancelled by operator before refresh of {len(touched_ids)} instruments",
                stage_key=active_bootstrap_stage_key() or "",
            )

        from app.services.ownership_observations import refresh_funds_current_batch

        refresh_failures: list[str] = []
        sorted_ids = sorted(touched_ids)
        with psycopg.connect(settings.database_url) as conn:
            # See sec_13f_ingest_from_dataset_job for the chunked-batch
            # cancel-poll rationale (PR-4 spec §8).
            for chunk_start in range(0, len(sorted_ids), _REFRESH_BATCH_CHUNK_SIZE):
                if bootstrap_cancel_requested():
                    raise BootstrapStageCancelled(
                        f"sec_nport_ingest_from_dataset cancelled by operator after "
                        f"refreshing {chunk_start}/{len(sorted_ids)} instruments",
                        stage_key=active_bootstrap_stage_key() or "",
                    )
                chunk = sorted_ids[chunk_start : chunk_start + _REFRESH_BATCH_CHUNK_SIZE]
                try:
                    with conn.transaction():
                        refresh_funds_current_batch(conn, instrument_ids=chunk)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "sec_nport_ingest_from_dataset: refresh_funds_current_batch failed for chunk start=%d size=%d",
                        chunk_start,
                        len(chunk),
                    )
                    refresh_failures.append(
                        f"chunk start={chunk_start} ids={chunk[:5]}{'...' if len(chunk) > 5 else ''}: {exc}"
                    )
        if refresh_failures:
            raise RuntimeError(
                f"sec_nport_ingest_from_dataset: {len(refresh_failures)} chunk(s) of {len(sorted_ids)} "
                f"_current refreshes failed; archives retained for retry: " + "; ".join(refresh_failures[:5])
            )
        logger.info(
            "sec_nport_ingest_from_dataset: refreshed ownership_funds_current for %d instruments",
            len(touched_ids),
        )
    for archive in succeeded:
        _delete_archive_after_success(archive)


# ---------------------------------------------------------------------------
# C6 — DERA FSDS per-class shares-outstanding ingester (#788)
# ---------------------------------------------------------------------------


def sec_fsds_class_shares_ingest_job() -> None:
    """Stream the cached ``fsds_*.zip`` archives and upsert per-class
    shares-outstanding rows into ``instrument_class_shares_outstanding`` (sql/200).

    The per-class denominator (GOOGL ÷ Class-A, not ÷ combined) supersedes the
    #1646 caveat in the ownership rollup. Tiny output (hundreds of rows over the
    curated dual-class set); fail-closed per-row (unmapped member / ambiguous CUSIP
    / non-current-period → skipped, never written). Standalone-safe: with no
    running bootstrap run it ingests whatever ``fsds_*.zip`` is cached.
    """
    from app.services.bootstrap_preconditions import assert_not_fallback_mode
    from app.services.bootstrap_state import BootstrapStageCancelled
    from app.services.processes.bootstrap_cancel_signal import (
        active_bootstrap_stage_key,
        bootstrap_cancel_requested,
    )
    from app.services.sec_bulk_download import BootstrapPartialDownloadError, read_run_manifest

    run_id = _current_running_bootstrap_run_id()
    archives = _list_archives_matching("fsds_")

    if run_id is not None:
        with psycopg.connect(settings.database_url) as conn:
            assert_not_fallback_mode(_bulk_dir(), bootstrap_run_id=run_id)
        # Manifest provenance gate (Codex ckpt-2): the per-run manifest lists only
        # archives that landed (or were ETag-reused) under THIS bootstrap_run_id,
        # so restricting to manifest names admits a freshly-downloaded optional
        # quarter while excluding a stale same-name ``fsds_*.zip`` left on disk
        # from a prior run (incl. an optional quarter this run 404'd). The
        # ``bootstrap_run_id`` check rejects a STALE manifest from a prior run whose
        # names happen to match (Codex ckpt-2b): a mismatched manifest is treated as
        # absent so the no-archives guard below raises rather than ingesting stale
        # zips. Mirrors ``assert_archives_in_manifest`` / the 13F ingester.
        manifest = read_run_manifest(_bulk_dir())
        if manifest is not None and int(manifest.get("bootstrap_run_id", -1)) == run_id:
            manifest_names = {entry["name"] for entry in manifest.get("archives", [])}
        else:
            manifest_names = set()
        archives = [a for a in archives if a.name in manifest_names]
        if not archives:
            raise BootstrapPartialDownloadError(
                "sec_fsds_class_shares_ingest: no fsds_*.zip in this run's manifest; "
                "upstream sec_bulk_download did not land FSDS archives."
            )
    elif not archives:
        logger.info("sec_fsds_class_shares_ingest: no fsds_*.zip cached, skipping (no run)")
        return

    failed_archives: list[str] = []
    succeeded: list[Path] = []
    total_written = 0
    no_row_pairs: set[str] = set()
    for archive in archives:
        if bootstrap_cancel_requested():
            raise BootstrapStageCancelled(
                f"sec_fsds_class_shares_ingest cancelled by operator after {len(succeeded)}/{len(archives)} archives",
                stage_key=active_bootstrap_stage_key() or "",
            )
        fsds_qtr = archive.name.removeprefix("fsds_").removesuffix(".zip")
        with psycopg.connect(settings.database_url) as conn:
            try:
                result = ingest_fsds_class_shares_archive(conn=conn, archive_path=archive, fsds_qtr=fsds_qtr)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                logger.exception("sec_fsds_class_shares_ingest: archive=%s failed", archive.name)
                failed_archives.append(f"{archive.name}: {exc}")
                continue
        succeeded.append(archive)
        total_written += result.rows_written
        no_row_pairs.update(result.curated_pairs_without_row)
        if run_id is not None:
            _record_archive_result(
                bootstrap_run_id=run_id,
                stage_key="sec_fsds_class_shares_ingest",
                archive_name=archive.name,
                rows_written=result.rows_written,
                rows_skipped={
                    "not_current_period": result.skipped_not_current_period,
                    "cusip_unresolved": result.skipped_cusip_unresolved,
                    "cusip_ambiguous": result.skipped_cusip_ambiguous,
                    "bad_value": result.skipped_bad_value,
                },
            )
    logger.info(
        "sec_fsds_class_shares_ingest: total_rows_written=%d archives=%d",
        total_written,
        len(succeeded),
    )
    if failed_archives:
        raise RuntimeError(
            f"sec_fsds_class_shares_ingest: {len(failed_archives)} archives failed: " + "; ".join(failed_archives)
        )
    # NB (#1590): do NOT delete the fsds_*.zip here. The dimensional-facts stage
    # (sec_fsds_dimensional_ingest) consumes the SAME archives and bootstrap stage
    # ORDER is not scheduling order, so a delete here could remove a zip that stage
    # still needs. Both FSDS consumers leave the archives in the bulk cache —
    # deterministic fsds_{q}.zip names mean a re-download overwrites rather than
    # accumulates, exactly as companyfacts.zip / nport_*.zip already persist.


def sec_fsds_dimensional_ingest_job() -> None:
    """Stream the cached ``fsds_*.zip`` archives and bulk-load dimensional XBRL facts
    (segment / product-or-service / geographic revenue, operating income, assets) into
    ``instrument_dimensional_facts`` (sql/193) — the quick-and-dirty bootstrap tier of
    #1590. The precise #554 per-filing path converges on top (it replaces a bulk
    accession's rows when its manifest worker re-parses that filing).

    Mirrors ``sec_fsds_class_shares_ingest_job``: same per-run manifest-provenance gate
    (admit only archives that landed under THIS bootstrap_run_id; a stale same-name zip
    is excluded), same per-archive commit, same standalone-safe behaviour. Does NOT
    delete the archives (the class-shares stage shares them — see the NB above).
    """
    from app.services.bootstrap_preconditions import assert_not_fallback_mode
    from app.services.bootstrap_state import BootstrapStageCancelled
    from app.services.fsds_dimensional_facts import ingest_fsds_dimensional_archive
    from app.services.processes.bootstrap_cancel_signal import (
        active_bootstrap_stage_key,
        bootstrap_cancel_requested,
    )
    from app.services.sec_bulk_download import BootstrapPartialDownloadError, read_run_manifest

    run_id = _current_running_bootstrap_run_id()
    archives = _list_archives_matching("fsds_")

    if run_id is not None:
        with psycopg.connect(settings.database_url) as conn:
            assert_not_fallback_mode(_bulk_dir(), bootstrap_run_id=run_id)
        manifest = read_run_manifest(_bulk_dir())
        if manifest is not None and int(manifest.get("bootstrap_run_id", -1)) == run_id:
            manifest_names = {entry["name"] for entry in manifest.get("archives", [])}
        else:
            manifest_names = set()
        archives = [a for a in archives if a.name in manifest_names]
        if not archives:
            raise BootstrapPartialDownloadError(
                "sec_fsds_dimensional_ingest: no fsds_*.zip in this run's manifest; "
                "upstream sec_bulk_download did not land FSDS archives."
            )
    elif not archives:
        logger.info("sec_fsds_dimensional_ingest: no fsds_*.zip cached, skipping (no run)")
        return

    failed_archives: list[str] = []
    succeeded: list[Path] = []
    total_written = 0
    for archive in archives:
        if bootstrap_cancel_requested():
            raise BootstrapStageCancelled(
                f"sec_fsds_dimensional_ingest cancelled by operator after {len(succeeded)}/{len(archives)} archives",
                stage_key=active_bootstrap_stage_key() or "",
            )
        fsds_qtr = archive.name.removeprefix("fsds_").removesuffix(".zip")
        with psycopg.connect(settings.database_url) as conn:
            try:
                # The ingester commits per accession (advisory-lock hygiene); no outer txn.
                result = ingest_fsds_dimensional_archive(conn=conn, archive_path=archive, fsds_qtr=fsds_qtr)
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                logger.exception("sec_fsds_dimensional_ingest: archive=%s failed", archive.name)
                failed_archives.append(f"{archive.name}: {exc}")
                continue
        succeeded.append(archive)
        total_written += result.rows_written
        if run_id is not None:
            _record_archive_result(
                bootstrap_run_id=run_id,
                stage_key="sec_fsds_dimensional_ingest",
                archive_name=archive.name,
                rows_written=result.rows_written,
                rows_skipped={
                    "accessions_skipped_existing": result.accessions_skipped_existing,
                    "accessions_no_instrument": result.accessions_no_instrument,
                },
            )
    logger.info(
        "sec_fsds_dimensional_ingest: total_rows_written=%d archives=%d",
        total_written,
        len(succeeded),
    )
    if failed_archives:
        raise RuntimeError(
            f"sec_fsds_dimensional_ingest: {len(failed_archives)} archives failed: " + "; ".join(failed_archives)
        )
