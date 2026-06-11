"""#819 — populate_canonical_redirects job-runs telemetry wiring.

The job's first-ever successful manual dispatch (2026-06-11) orphaned
its prelude-written ``running`` job_runs row: the bare service body
never adopted the row via ``_tracked_job``. These tests pin the fix:

* ``_INVOKERS`` registers the SCHEDULER tracked wrapper, not the bare
  service function.
* The wrapper runs the body inside ``_tracked_job`` and stamps
  ``row_count = redirects_set``.

Pure-logic tier — the service body and the tracking context manager
are both stubbed; no DB.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from app.jobs.runtime import _INVOKERS
from app.services.canonical_instrument_redirects import (
    JOB_POPULATE_CANONICAL_REDIRECTS,
    RedirectPopulationStats,
)
from app.workers import scheduler as scheduler_module


class TestInvokerRegistration:
    def test_invoker_wraps_scheduler_tracked_wrapper(self) -> None:
        """The registered invoker must be the scheduler-side wrapper.

        Registering the bare service function reintroduces the
        orphaned-``running``-row defect — the prelude row would have
        no finaliser.
        """
        invoker = _INVOKERS[JOB_POPULATE_CANONICAL_REDIRECTS]
        wrapped = getattr(invoker, "__wrapped__", None)
        assert wrapped is scheduler_module.populate_canonical_redirects


class TestTrackedWrapper:
    def test_wrapper_tracks_and_stamps_row_count(self, monkeypatch: Any) -> None:
        stats = RedirectPopulationStats(
            variants_scanned=561,
            redirects_set=540,
            redirects_already_correct=0,
            redirects_skipped_no_base=21,
            redirects_skipped_ambiguous=0,
        )

        class _Tracker:
            row_count: int | None = None

        tracker = _Tracker()
        entered: list[str] = []

        @contextmanager
        def _fake_tracked_job(job_name: str):
            entered.append(job_name)
            yield tracker

        monkeypatch.setattr(scheduler_module, "_tracked_job", _fake_tracked_job)
        monkeypatch.setattr(
            "app.services.canonical_instrument_redirects.populate_canonical_redirects_job",
            lambda: stats,
        )

        scheduler_module.populate_canonical_redirects()

        assert entered == [JOB_POPULATE_CANONICAL_REDIRECTS]
        assert tracker.row_count == 540

    def test_body_exception_propagates_inside_tracking(self, monkeypatch: Any) -> None:
        """A body failure must surface INSIDE the tracking scope so
        ``_tracked_job`` records the failed run (not an orphaned row)."""
        entered: list[str] = []
        exited: list[bool] = []

        @contextmanager
        def _fake_tracked_job(job_name: str):
            entered.append(job_name)
            try:
                yield type("T", (), {"row_count": None})()
            finally:
                exited.append(True)

        def _boom() -> RedirectPopulationStats:
            raise RuntimeError("populate failed")

        monkeypatch.setattr(scheduler_module, "_tracked_job", _fake_tracked_job)
        monkeypatch.setattr(
            "app.services.canonical_instrument_redirects.populate_canonical_redirects_job",
            _boom,
        )

        try:
            scheduler_module.populate_canonical_redirects()
        except RuntimeError:
            pass
        else:  # pragma: no cover - assertion guard
            raise AssertionError("body exception was swallowed")

        assert entered == [JOB_POPULATE_CANONICAL_REDIRECTS]
        assert exited == [True]
