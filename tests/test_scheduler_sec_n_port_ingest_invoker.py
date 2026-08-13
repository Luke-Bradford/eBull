"""Scheduler-invoker tests for ``sec_n_port_ingest`` local-zip path (#1340).

Covers (no live PG):
  * Strict-bool coercion of ``use_bulk_zip``.
  * use_bulk_zip=True + provenanced archive → ingest gets a
    ``ZipBackedArchiveFetcher``; archive deleted on success (S23 owns cleanup).
  * Missing archive / outside dispatch / provenance mismatch → HTTP fallback
    (the raw ``SecFilingsProvider`` is passed, not the zip wrapper).
  * Cleanup is unconditional on the success path (mirrors S16's old invariant,
    now owned by S23).
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.services.bootstrap_state import BootstrapProgressContext
from app.services.sec_submissions_zip import ZipBackedArchiveFetcher
from app.workers import scheduler


def _patch_invoker_dependencies(
    archive_path: Path,
    *,
    progress_context: BootstrapProgressContext | None,
    assert_provenance_raises: Exception | None = None,
):
    fake_conn_ctx = MagicMock()
    fake_conn_ctx.__enter__.return_value = MagicMock()
    fake_conn_ctx.__exit__.return_value = False

    sec_obj = MagicMock(name="SecFilingsProvider_instance")
    fake_sec_ctx = MagicMock()
    fake_sec_ctx.__enter__.return_value = sec_obj
    fake_sec_ctx.__exit__.return_value = False

    fake_tracker_ctx = MagicMock()
    fake_tracker_ctx.__enter__.return_value = MagicMock()
    fake_tracker_ctx.__exit__.return_value = False

    def _assert_provenance(*args, **kwargs):
        if assert_provenance_raises is not None:
            raise assert_provenance_raises

    ingest_spy = MagicMock(return_value=[])

    patches = (
        patch.object(scheduler, "SecFilingsProvider", return_value=fake_sec_ctx),
        patch("psycopg.connect", return_value=fake_conn_ctx),
        patch.object(scheduler, "_tracked_job", return_value=fake_tracker_ctx),
        patch("app.services.n_port_ingest.ingest_all_fund_filers", ingest_spy),
        patch(
            "app.services.sec_nport_filer_directory.list_nport_filer_ciks",
            return_value=["0000320193"],
        ),
        patch(
            "app.security.master_key.resolve_data_dir",
            return_value=archive_path.parents[2],
        ),
        patch(
            "app.services.bootstrap_state.resolve_progress_context",
            return_value=progress_context,
        ),
        patch("app.services.bootstrap_state.set_stage_target", MagicMock()),
        patch(
            "app.services.sec_bulk_download.assert_archive_belongs_to_run",
            side_effect=_assert_provenance,
        ),
    )
    return patches, ingest_spy, sec_obj


def _stack(patches):
    import contextlib

    cm = contextlib.ExitStack()
    for p in patches:
        cm.enter_context(p)
    return cm


def _valid_bulk_archive_at(tmp_path: Path) -> Path:
    """Write a REAL (openable) submissions.zip at <data>/sec/bulk/.

    S23 opens the ZipFile in the invoker (unlike S16, which delegates to the
    mocked drain), so the bytes must be a valid zip.
    """
    archive = tmp_path / "sec" / "bulk" / "submissions.zip"
    archive.parent.mkdir(parents=True)
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("CIK0000320193.json", b'{"cik":"320193"}')
    return archive


class TestSecNPortIngestInvoker:
    def test_strict_bool_non_bool_downgrades(self, tmp_path: Path) -> None:
        archive = _valid_bulk_archive_at(tmp_path)
        patches, ingest_spy, sec_obj = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=7, stage_key="sec_n_port_ingest"),
        )
        with _stack(patches):
            scheduler.sec_n_port_ingest({"use_bulk_zip": "true"})
        # String "true" is not a bool → HTTP fallback: raw provider passed.
        assert ingest_spy.call_args.args[1] is sec_obj
        # Cleanup unconditional on success.
        assert not archive.exists()

    def test_outside_bootstrap_dispatch_downgrades_and_preserves_archive(self, tmp_path: Path) -> None:
        # No progress context = not a bootstrap dispatch (monthly / Admin run).
        # Zip path downgraded AND the archive is preserved — outside bootstrap
        # S23 does not own the submissions.zip lifecycle (#1340 Codex 2).
        archive = _valid_bulk_archive_at(tmp_path)
        patches, ingest_spy, sec_obj = _patch_invoker_dependencies(
            archive,
            progress_context=None,
        )
        with _stack(patches):
            scheduler.sec_n_port_ingest({"use_bulk_zip": True})
        assert ingest_spy.call_args.args[1] is sec_obj
        assert archive.exists()

    def test_corrupt_archive_downgrades_to_http(self, tmp_path: Path) -> None:
        # Archive passes exists() + provenance but is not a valid zip → the
        # ZipFile open raises BadZipFile → S23 must fall back to HTTP, not
        # crash (#1340 Codex 2 BLOCKING: zip is a pure accelerator).
        archive = tmp_path / "sec" / "bulk" / "submissions.zip"
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b"not a zip at all")
        patches, ingest_spy, sec_obj = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=7, stage_key="sec_n_port_ingest"),
        )
        with _stack(patches):
            scheduler.sec_n_port_ingest({"use_bulk_zip": True})
        # Fell back to the raw provider (no wrapper) and did not raise.
        assert ingest_spy.call_args.args[1] is sec_obj
        # In bootstrap → cleanup still fires.
        assert not archive.exists()

    def test_missing_archive_downgrades(self, tmp_path: Path) -> None:
        archive = tmp_path / "sec" / "bulk" / "submissions.zip"
        archive.parent.mkdir(parents=True)  # do NOT create the file
        patches, ingest_spy, sec_obj = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=7, stage_key="sec_n_port_ingest"),
        )
        with _stack(patches):
            scheduler.sec_n_port_ingest({"use_bulk_zip": True})
        assert ingest_spy.call_args.args[1] is sec_obj
        assert not archive.exists()

    def test_provenance_mismatch_downgrades(self, tmp_path: Path) -> None:
        archive = _valid_bulk_archive_at(tmp_path)
        patches, ingest_spy, sec_obj = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=7, stage_key="sec_n_port_ingest"),
            assert_provenance_raises=RuntimeError("run_id mismatch"),
        )
        with _stack(patches):
            scheduler.sec_n_port_ingest({"use_bulk_zip": True})
        # Provenance failure → fallback to raw provider, but archive STILL
        # deleted post-drain (unconditional success-path cleanup).
        assert ingest_spy.call_args.args[1] is sec_obj
        assert not archive.exists()

    def test_use_bulk_zip_true_wraps_fetcher_and_cleans(self, tmp_path: Path) -> None:
        archive = _valid_bulk_archive_at(tmp_path)
        patches, ingest_spy, _sec_obj = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=7, stage_key="sec_n_port_ingest"),
        )
        with _stack(patches):
            scheduler.sec_n_port_ingest({"use_bulk_zip": True})
        # The ingester received the zip-backed wrapper, not the raw provider.
        assert isinstance(ingest_spy.call_args.args[1], ZipBackedArchiveFetcher)
        # S23 owns the deletion now.
        assert not archive.exists()

    def test_default_no_zip_path(self, tmp_path: Path) -> None:
        archive = _valid_bulk_archive_at(tmp_path)
        patches, ingest_spy, sec_obj = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=7, stage_key="sec_n_port_ingest"),
        )
        with _stack(patches):
            scheduler.sec_n_port_ingest({})
        assert ingest_spy.call_args.args[1] is sec_obj
        assert not archive.exists()
