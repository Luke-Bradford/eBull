"""Cancel-signal adoption tests for the bulk-archive C-stage ingesters.

Issue #1064 PR3d follow-up. The C3/C4/C5 ingesters
(``sec_13f_ingest_from_dataset_job``, ``sec_insider_ingest_from_dataset_job``,
``sec_nport_ingest_from_dataset_job``) walk per-archive loops where each
archive ingest takes 5-20 minutes. Polling between archives lets the
operator's cancel signal land at an archive boundary instead of waiting
for the next bootstrap-stage checkpoint.

Outside ``active_bootstrap_run`` the contextvar is unset and
``bootstrap_cancel_requested`` short-circuits to False, so the
no-run / standalone-trigger paths are unaffected. These tests stub the
helper directly with ``monkeypatch`` so the per-job loop runs without a
live bootstrap_runs row.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from app.services import sec_bulk_orchestrator_jobs as jobs
from app.services.bootstrap_state import BootstrapStageCancelled


def _make_form13f_archive(path: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("README.txt", "stub")
    path.write_bytes(buf.getvalue())


@pytest.mark.parametrize(
    ("job_name", "archive_prefix", "stage_key"),
    [
        ("sec_13f_ingest_from_dataset_job", "form13f_", "sec_13f_ingest_from_dataset"),
        ("sec_insider_ingest_from_dataset_job", "insider_", "sec_insider_ingest_from_dataset"),
        ("sec_nport_ingest_from_dataset_job", "nport_", "sec_nport_ingest_from_dataset"),
    ],
)
def test_cancel_signal_aborts_before_first_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_name: str,
    archive_prefix: str,
    stage_key: str,
) -> None:
    """When ``bootstrap_cancel_requested`` returns True on the first
    iteration, the per-archive loop raises ``BootstrapStageCancelled``
    with the matching ``stage_key`` and the per-archive ingester is
    never invoked.

    The no-run path skips the precondition + manifest checks; the
    cancel poll fires for any caller that runs the loop body, so the
    standalone path is the cleanest test surface.
    """
    bulk_dir = tmp_path / "sec" / "bulk"
    bulk_dir.mkdir(parents=True)
    # Two archives so the loop has work — the cancel must block both.
    for i in range(2):
        _make_form13f_archive(bulk_dir / f"{archive_prefix}2024q{i + 1}.zip")

    monkeypatch.setattr(
        "app.services.processes.bootstrap_cancel_signal.bootstrap_cancel_requested",
        lambda: True,
    )

    # Track whether the per-archive ingester was invoked. Patching the
    # symbol on the module means a False positive on cancel-aborts
    # (i.e. cancel poll didn't fire) would call the patched ingester
    # and the test would observe the call.
    ingest_calls: list[Path] = []

    def _record(*args, **kwargs):
        ingest_calls.append(kwargs.get("archive_path") or args[1])
        return None

    job = getattr(jobs, job_name)

    with (
        patch.object(jobs, "_bulk_dir", return_value=bulk_dir),
        patch.object(jobs, "_current_running_bootstrap_run_id", return_value=None),
        patch.object(jobs, "ingest_13f_dataset_archive", side_effect=_record),
        patch.object(jobs, "ingest_insider_dataset_archive", side_effect=_record),
        patch.object(jobs, "ingest_nport_dataset_archive", side_effect=_record),
    ):
        with pytest.raises(BootstrapStageCancelled) as exc_info:
            job()

    # Pin the stage_key attribute, not just substring-in-message —
    # message-substring would pass even on a wrong-stage_key bug.
    # Codex pre-push round 1.
    assert exc_info.value.stage_key == stage_key
    assert "cancelled by operator" in str(exc_info.value)
    # Critical: the per-archive ingester was NEVER called.
    assert ingest_calls == [], f"ingest helper called despite cancel: {ingest_calls}"


@pytest.mark.parametrize(
    ("job_name", "archive_prefix", "expected_ingester", "other_ingesters"),
    [
        (
            "sec_13f_ingest_from_dataset_job",
            "form13f_",
            "ingest_13f_dataset_archive",
            ("ingest_insider_dataset_archive", "ingest_nport_dataset_archive"),
        ),
        (
            "sec_insider_ingest_from_dataset_job",
            "insider_",
            "ingest_insider_dataset_archive",
            ("ingest_13f_dataset_archive", "ingest_nport_dataset_archive"),
        ),
        (
            "sec_nport_ingest_from_dataset_job",
            "nport_",
            "ingest_nport_dataset_archive",
            ("ingest_13f_dataset_archive", "ingest_insider_dataset_archive"),
        ),
    ],
)
def test_each_job_calls_only_its_own_ingester(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_name: str,
    archive_prefix: str,
    expected_ingester: str,
    other_ingesters: tuple[str, ...],
) -> None:
    """Without a cancel signal, the no-run path runs the loop once and
    the EXPECTED per-job ingester is called. The two non-matching
    ingesters must NOT be invoked — guards against a copy-paste regression
    binding the wrong helper. Codex pre-push round 1.
    """
    bulk_dir = tmp_path / "sec" / "bulk"
    bulk_dir.mkdir(parents=True)
    _make_form13f_archive(bulk_dir / f"{archive_prefix}2024q1.zip")

    monkeypatch.setattr(
        "app.services.processes.bootstrap_cancel_signal.bootstrap_cancel_requested",
        lambda: False,
    )

    expected_calls: list[Path] = []
    foreign_calls: dict[str, list[Path]] = {name: [] for name in other_ingesters}

    def _make_recorder(sink: list[Path]):
        def _record(*args, **kwargs):
            sink.append(kwargs.get("archive_path") or args[1])
            from types import SimpleNamespace

            return SimpleNamespace(
                rows_written=1,
                rows_skipped_unresolved_cusip=0,
                rows_skipped_unresolved_cik=0,
                rows_skipped_non_equity=0,
                touched_instrument_ids=set(),
            )

        return _record

    job = getattr(jobs, job_name)

    patches = [
        patch.object(jobs, "_bulk_dir", return_value=bulk_dir),
        patch.object(jobs, "_current_running_bootstrap_run_id", return_value=None),
        patch.object(jobs, expected_ingester, side_effect=_make_recorder(expected_calls)),
    ]
    for name in other_ingesters:
        patches.append(patch.object(jobs, name, side_effect=_make_recorder(foreign_calls[name])))

    # Apply patches in a stack.
    from contextlib import ExitStack

    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        # _delete_archive_after_success is benign on the temp file but
        # patch it to a no-op so a slow filesystem doesn't matter.
        stack.enter_context(patch.object(jobs, "_delete_archive_after_success"))
        job()

    assert len(expected_calls) == 1
    for name, calls in foreign_calls.items():
        assert calls == [], f"{job_name} called {name} but should only call {expected_ingester}"


@pytest.mark.parametrize(
    "job_name",
    [
        "sec_13f_ingest_from_dataset_job",
        "sec_insider_ingest_from_dataset_job",
        "sec_nport_ingest_from_dataset_job",
    ],
)
def test_no_archives_skips_loop_without_cancel_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_name: str,
) -> None:
    """When there are no archives on disk and no bootstrap run is
    active, the early-return path runs and the cancel poll is never
    reached. Pinned to guard against regression where a misplaced poll
    would raise on standalone triggers with empty disk state.
    """
    bulk_dir = tmp_path / "sec" / "bulk"
    bulk_dir.mkdir(parents=True)

    poll_calls = [0]

    def _poll() -> bool:
        poll_calls[0] += 1
        return True

    monkeypatch.setattr(
        "app.services.processes.bootstrap_cancel_signal.bootstrap_cancel_requested",
        _poll,
    )

    job = getattr(jobs, job_name)

    with (
        patch.object(jobs, "_bulk_dir", return_value=bulk_dir),
        patch.object(jobs, "_current_running_bootstrap_run_id", return_value=None),
    ):
        # Returns cleanly with no archives present, no exception.
        job()

    # Cancel poll never fired because the loop had nothing to iterate.
    assert poll_calls == [0]
