"""Disk-hygiene regression tests for the bulk orchestrator jobs (#1020)."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import patch

from app.services import sec_bulk_orchestrator_jobs as jobs
from app.services.sec_bulk_orchestrator_jobs import _delete_archive_after_success


class TestDeleteArchiveAfterSuccess:
    def test_deletes_existing_file(self, tmp_path: Path) -> None:
        archive = tmp_path / "submissions.zip"
        archive.write_bytes(b"x" * 100)
        _delete_archive_after_success(archive)
        assert not archive.exists()

    def test_missing_file_no_op(self, tmp_path: Path) -> None:
        archive = tmp_path / "missing.zip"
        # Must not raise — missing_ok=True.
        _delete_archive_after_success(archive)


def _build_minimal_archive(path: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("CIK0000000000.json", '{"filings":{"recent":{},"files":[]}}')
    path.write_bytes(buf.getvalue())


class TestSubmissionsJobPreservesArchive:
    """#1277 T10 — S8 ``sec_submissions_ingest_job`` no longer deletes
    ``submissions.zip`` post-ingest. Deletion deferred to S16
    ``sec_first_install_drain`` (``_cleanup_submissions_zip_after_drain``)
    so the hybrid HttpGet path can read PRIMARY ``CIK<10>.json`` entries
    from disk for the non-issuer cohort.

    The other bulk ingester jobs (companyfacts, etc.) keep their
    existing post-ingest deletion via sibling call sites — see
    ``app/services/sec_bulk_orchestrator_jobs.py:290, 501, 661, 849``.

    Regression sentinel: re-adding ``_delete_archive_after_success`` at
    the original site silently orphans S16's zip-path expectations.
    """

    def test_archive_preserved_after_ingest_success(self, tmp_path: Path) -> None:
        archive = tmp_path / "sec" / "bulk" / "submissions.zip"
        archive.parent.mkdir(parents=True)
        _build_minimal_archive(archive)

        # Mock _current_running_bootstrap_run_id → None so the standalone
        # path executes (skips the run_id-gated _record_archive_result +
        # preconditions). Without this mock _current_running_bootstrap_run_id
        # tries to open a real DB connection and raises when PG is down.
        with (
            patch.object(jobs, "_bulk_dir", return_value=archive.parent),
            patch.object(jobs, "_current_running_bootstrap_run_id", return_value=None),
            patch.object(jobs, "_run_with_conn") as run_with_conn,
        ):
            run_with_conn.return_value = None
            jobs.sec_submissions_ingest_job()
        assert archive.exists(), (
            "S8 sec_submissions_ingest_job must preserve submissions.zip on disk "
            "post-#1277 — deletion is deferred to S16 _cleanup_submissions_zip_after_drain"
        )


class TestS16CleanupSubmissionsZip:
    """#1277 T11 / T11b / T12 — S16 owns the lifecycle of submissions.zip
    post-ingest. Cleanup is UNCONDITIONAL on the drain SUCCESS path so
    the ``use_bulk_zip=False`` rollback path stays disk-clean."""

    def test_helper_deletes_existing_archive(self, tmp_path: Path) -> None:
        from app.workers.scheduler import _cleanup_submissions_zip_after_drain

        archive = tmp_path / "submissions.zip"
        archive.write_bytes(b"x" * 100)
        _cleanup_submissions_zip_after_drain(archive)
        assert not archive.exists()

    def test_helper_missing_archive_no_op(self, tmp_path: Path) -> None:
        from app.workers.scheduler import _cleanup_submissions_zip_after_drain

        archive = tmp_path / "missing.zip"
        # Must not raise — missing_ok=True semantics mirror
        # _delete_archive_after_success.
        _cleanup_submissions_zip_after_drain(archive)
        assert not archive.exists()
