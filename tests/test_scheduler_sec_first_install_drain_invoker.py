"""Scheduler-invoker tests for ``sec_first_install_drain`` (#1277).

Covers T6 / T6b / T6c (spec §4) without requiring a live PG:

  * Strict-bool coercion at the job-param boundary.
  * Archive provenance check fires when use_bulk_zip=True.
  * Archive provenance mismatch downgrades to HTTP.
  * Outside-bootstrap-dispatch downgrades to HTTP.
  * Missing archive downgrades to HTTP.

#1413 UPDATE: S23 ``sec_n_port_ingest`` was dropped from the bootstrap
stage set (bulk-only collapse), so S16 is again the LAST bootstrap
consumer of ``submissions.zip`` and owns the post-drain delete on its
success path (#1340 had moved it to S23). These tests assert the archive
is DELETED after a successful drain on every path.

These complement the integration tests in
``tests/test_sec_first_install_drain.py`` (T2 / T3) which exercise the
underlying drain function against ``ebull_test``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from app.jobs.sec_first_install_drain import DrainStats
from app.services.bootstrap_state import BootstrapProgressContext
from app.workers import scheduler


def _patch_invoker_dependencies(
    archive_path: Path,
    *,
    progress_context: BootstrapProgressContext | None,
    assert_provenance_raises: Exception | None = None,
):
    """Build the dependency-mock context for ``sec_first_install_drain``.

    Returns a ContextManager that patches:
      * ``SecFilingsProvider`` (avoid SEC HTTP setup)
      * ``psycopg.connect`` (no DB)
      * ``_make_sec_http_get`` (no transport)
      * ``_tracked_job`` (no job_runtime row writes)
      * ``run_first_install_drain`` (returns a fixed DrainStats; spy)
      * ``resolve_data_dir`` (point at the test archive's parent)
      * ``resolve_progress_context``
      * ``assert_archive_belongs_to_run`` (raise or no-op)
    """
    stats = DrainStats(
        ciks_processed=1,
        ciks_skipped=0,
        secondary_pages_fetched=0,
        manifest_rows_upserted=2,
        errors=0,
    )

    fake_conn_ctx = MagicMock()
    fake_conn_ctx.__enter__.return_value = MagicMock()
    fake_conn_ctx.__exit__.return_value = False

    fake_sec_ctx = MagicMock()
    fake_sec_ctx.__enter__.return_value = MagicMock()
    fake_sec_ctx.__exit__.return_value = False

    fake_tracker_ctx = MagicMock()
    fake_tracker_ctx.__enter__.return_value = MagicMock()
    fake_tracker_ctx.__exit__.return_value = False

    def _assert_provenance(*args, **kwargs):
        if assert_provenance_raises is not None:
            raise assert_provenance_raises

    drain_spy = MagicMock(return_value=stats)

    patches = (
        patch.object(scheduler, "SecFilingsProvider", return_value=fake_sec_ctx),
        patch("psycopg.connect", return_value=fake_conn_ctx),
        patch.object(scheduler, "_make_sec_http_get", return_value=lambda url, h: (404, b"")),
        patch.object(scheduler, "_tracked_job", return_value=fake_tracker_ctx),
        patch(
            "app.jobs.sec_first_install_drain.run_first_install_drain",
            drain_spy,
        ),
        patch(
            "app.security.master_key.resolve_data_dir",
            # data_dir / "sec" / "bulk" / "submissions.zip" == archive_path,
            # so data_dir == archive_path.parents[2].
            return_value=archive_path.parents[2],
        ),
        patch(
            "app.services.bootstrap_state.resolve_progress_context",
            return_value=progress_context,
        ),
        patch(
            "app.services.sec_bulk_download.assert_archive_belongs_to_run",
            side_effect=_assert_provenance,
        ),
    )
    return patches, drain_spy


def _stack(patches):
    """Open the patches as a single context-manager stack."""
    import contextlib

    cm = contextlib.ExitStack()
    for p in patches:
        cm.enter_context(p)
    return cm


def _bulk_archive_at(tmp_path: Path) -> Path:
    """Create a tmp <data_dir>/sec/bulk/submissions.zip-shaped path."""
    archive = tmp_path / "sec" / "bulk" / "submissions.zip"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"PK\x03\x04dummy")  # well-formed-ish bytes for exists()
    return archive


class TestSchedulerInvoker:
    """#1277 T6 / T6b / T6c."""

    def test_strict_bool_non_bool_downgrades(self, tmp_path: Path) -> None:
        # T6c — JSON-style "true" string is NOT bool — must downgrade.
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({"use_bulk_zip": "true"})
        # Drain was called with use_bulk_zip=False (string coerced to False).
        call = drain_spy.call_args
        assert call.kwargs["use_bulk_zip"] is False
        assert call.kwargs["archive_path"] is None
        # #1413 — S16 deletes submissions.zip on success (S23 dropped).
        assert not archive.exists()

    def test_outside_bootstrap_dispatch_downgrades(self, tmp_path: Path) -> None:
        # T6 — use_bulk_zip=True but no progress context → downgrade.
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=None,
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({"use_bulk_zip": True})
        call = drain_spy.call_args
        assert call.kwargs["use_bulk_zip"] is False
        assert call.kwargs["archive_path"] is None
        # #1413 — S16 deletes submissions.zip on success (S23 dropped).
        assert not archive.exists()

    def test_missing_archive_downgrades(self, tmp_path: Path) -> None:
        # T6 — use_bulk_zip=True but archive missing → downgrade.
        archive = tmp_path / "sec" / "bulk" / "submissions.zip"
        # Do NOT create the file.
        archive.parent.mkdir(parents=True)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({"use_bulk_zip": True})
        call = drain_spy.call_args
        assert call.kwargs["use_bulk_zip"] is False
        assert call.kwargs["archive_path"] is None
        # File was never created; S16 doesn't create it.
        assert not archive.exists()

    def test_provenance_mismatch_downgrades(self, tmp_path: Path) -> None:
        # T6b — assert_archive_belongs_to_run raises → downgrade.
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
            assert_provenance_raises=RuntimeError(
                "PRECONDITION: bulk manifest run_id=41 != current bootstrap_run_id=42"
            ),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({"use_bulk_zip": True})
        call = drain_spy.call_args
        assert call.kwargs["use_bulk_zip"] is False
        assert call.kwargs["archive_path"] is None
        # #1413 — S16 deletes submissions.zip on success on every path
        # (provenance-mismatch downgraded to HTTP, but the drain still
        # succeeded, so the stale archive is cleaned up).
        assert not archive.exists()

    def test_follow_pagination_defaults_true(self, tmp_path: Path) -> None:
        # #1413 Step 2.3 — no param → steady-state safety-net default
        # follow_pagination=True (secondary deep-history pages walked).
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({})
        assert drain_spy.call_args.kwargs["follow_pagination"] is True

    def test_follow_pagination_false_passed_through(self, tmp_path: Path) -> None:
        # #1413 Step 2.3 — bootstrap dispatch passes follow_pagination=False
        # → ZERO secondary-page HTTP. The invoker must thread it through,
        # not hardcode True.
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({"follow_pagination": False})
        assert drain_spy.call_args.kwargs["follow_pagination"] is False

    def test_follow_pagination_strict_bool_non_bool_defaults_true(self, tmp_path: Path) -> None:
        # #1413 Step 2.3 — strict-bool boundary (mirror use_bulk_zip): a
        # non-bool (e.g. JSON "false" string) must NOT silently disable
        # pagination; it falls back to the safe default True.
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({"follow_pagination": "false"})
        assert drain_spy.call_args.kwargs["follow_pagination"] is True

    def test_use_bulk_zip_true_with_provenanced_archive(self, tmp_path: Path) -> None:
        # T6 — use_bulk_zip=True + archive present + provenance passes →
        # drain is called with use_bulk_zip=True + archive_path set.
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({"use_bulk_zip": True})
        call = drain_spy.call_args
        assert call.kwargs["use_bulk_zip"] is True
        assert call.kwargs["archive_path"] == archive
        # #1413 — S16 deletes submissions.zip on success (S23 dropped).
        assert not archive.exists()

    def test_use_bulk_zip_false_default_no_zip_path(self, tmp_path: Path) -> None:
        # Default-path sanity: use_bulk_zip omitted → drain called with
        # use_bulk_zip=False + archive_path=None.
        archive = _bulk_archive_at(tmp_path)
        patches, drain_spy = _patch_invoker_dependencies(
            archive,
            progress_context=BootstrapProgressContext(run_id=42, stage_key="sec_first_install_drain"),
        )
        with _stack(patches):
            scheduler.sec_first_install_drain({})
        call = drain_spy.call_args
        assert call.kwargs["use_bulk_zip"] is False
        assert call.kwargs["archive_path"] is None
        # #1413 — S16 deletes submissions.zip on success on the HTTP-default path too.
        assert not archive.exists()
