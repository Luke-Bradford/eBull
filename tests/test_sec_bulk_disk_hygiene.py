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


class TestSubmissionsJobDeletesAfterSuccess:
    def test_archive_removed_after_ingest_success(self, tmp_path: Path) -> None:
        archive = tmp_path / "sec" / "bulk" / "submissions.zip"
        archive.parent.mkdir(parents=True)
        _build_minimal_archive(archive)

        # Patch resolve_data_dir + _run_with_conn so the test does not
        # need a live Postgres.
        with (
            patch.object(jobs, "_bulk_dir", return_value=archive.parent),
            patch.object(jobs, "_run_with_conn") as run_with_conn,
        ):
            run_with_conn.return_value = None
            jobs.sec_submissions_ingest_job()
        assert not archive.exists(), "archive must be deleted after successful ingest"
