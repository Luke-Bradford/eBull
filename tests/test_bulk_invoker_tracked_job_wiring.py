"""#1573 — bulk-dataset invoker job-runs telemetry wiring.

Manual dispatch ("Run now" / POST /jobs/{name}/run) of a bare bulk-dataset
invoker reaches ``run_with_prelude``, which writes a ``running`` ``job_runs``
row and stashes its ``run_id`` in a ContextVar. A bare body never adopts it
(it never calls ``consume_prelude_run_id``), so the row orphans ``running``
forever — only the #1510 boot reaper later marks it ``failure``. Reproduced
on dev (2026-06-23): a no-op manual ``sec_submissions_ingest`` left
``job_runs`` stuck ``running`` while the queue row showed ``completed``.

These tests pin the ``_tracked_zero_arg`` fix: the 9 affected invokers run
their body inside ``_tracked_job`` so the prelude row finalises.

Pure-logic tier — ``_tracked_job`` and the bodies are stubbed; no DB.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from app.jobs.runtime import _INVOKERS, _tracked_zero_arg
from app.workers import scheduler as scheduler_module

# The 9 bulk-dataset invokers wrapped by #1573 (issue's 7 + 2 FSDS siblings).
BULK_JOBS = (
    "sec_submissions_ingest",
    "sec_companyfacts_ingest",
    "sec_13f_ingest_from_dataset",
    "sec_insider_ingest_from_dataset",
    "sec_nport_ingest_from_dataset",
    "sec_fsds_class_shares_ingest",
    "sec_fsds_dimensional_ingest",
    "sec_submissions_files_walk",
    "sec_bulk_download",
)


class TestTrackedZeroArgHelper:
    def test_runs_body_inside_tracked_job(self, monkeypatch: Any) -> None:
        entered: list[str] = []
        order: list[str] = []

        @contextmanager
        def _fake_tracked_job(job_name: str):
            entered.append(job_name)
            order.append("enter")
            yield type("T", (), {"row_count": None})()
            order.append("exit")

        monkeypatch.setattr(scheduler_module, "_tracked_job", _fake_tracked_job)

        def _body() -> None:
            order.append("body")

        invoker = _tracked_zero_arg("job_x", _body)
        invoker({})

        assert entered == ["job_x"]
        # Body must run strictly between enter and exit (inside the tracking
        # scope) so _tracked_job owns the success/failure of the row.
        assert order == ["enter", "body", "exit"]

    def test_body_exception_propagates_inside_tracking(self, monkeypatch: Any) -> None:
        """A body failure surfaces INSIDE the tracking scope so ``_tracked_job``
        records the failed run rather than leaving an orphaned ``running`` row."""
        exited: list[bool] = []

        @contextmanager
        def _fake_tracked_job(job_name: str):
            try:
                yield type("T", (), {"row_count": None})()
            finally:
                exited.append(True)

        monkeypatch.setattr(scheduler_module, "_tracked_job", _fake_tracked_job)

        def _boom() -> None:
            raise RuntimeError("ingest failed")

        invoker = _tracked_zero_arg("job_x", _boom)

        with pytest.raises(RuntimeError, match="ingest failed"):
            invoker({})
        assert exited == [True]

    def test_marker_and_wrapped_set(self) -> None:
        def _body() -> None: ...

        invoker = _tracked_zero_arg("job_x", _body)
        # Explicit finalisation marker (inspect.getsource follows __wrapped__
        # to the bare body, so the regression-guard invariant reads the marker).
        assert getattr(invoker, "__finalises_prelude_row__", False) is True
        assert getattr(invoker, "__wrapped__", None) is _body

    def test_params_discarded(self, monkeypatch: Any) -> None:
        """The bulk bodies are zero-arg; the params dict must be discarded."""
        seen: list[int] = []

        @contextmanager
        def _fake_tracked_job(job_name: str):
            yield type("T", (), {"row_count": None})()

        monkeypatch.setattr(scheduler_module, "_tracked_job", _fake_tracked_job)
        invoker = _tracked_zero_arg("job_x", lambda: seen.append(1))
        invoker({"ignored": "value"})
        assert seen == [1]


class TestBulkInvokerRegistration:
    """Every #1573 bulk invoker in ``_INVOKERS`` must enter ``_tracked_job``
    with its own job_name when dispatched."""

    @pytest.mark.parametrize("job_name", BULK_JOBS)
    def test_invoker_enters_tracked_job(self, job_name: str, monkeypatch: Any) -> None:
        entered: list[str] = []

        class _Sentinel(Exception):
            pass

        @contextmanager
        def _fake_tracked_job(name: str):
            entered.append(name)
            # Raise on entry so the real body never runs (bodies do real
            # DB/SEC work). The recorded name proves the wrapper fired.
            raise _Sentinel
            yield  # pragma: no cover - unreachable, keeps this a generator

        monkeypatch.setattr(scheduler_module, "_tracked_job", _fake_tracked_job)

        with pytest.raises(_Sentinel):
            _INVOKERS[job_name]({})
        assert entered == [job_name]
