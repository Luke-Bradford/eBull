"""#1703 — fast-tier (no DB) guard for the Phase B top-up source exclusion.

The manifest worker raises ``max_rows`` 100 → 200 (``scheduler.py``). The one
heavy source (``sec_13f_hr`` — serial ``infotable.xml`` fetch+parse, ~3-4.4s/
filing) is kept OFF the Phase B global-oldest top-up so a drained-backlog regime
cannot flood a tick with heavy 13F and overrun the 5-min cadence. 13F is then
drained by its Phase R + Phase A quota only.

This test stubs the DB ``iter_*`` selectors + the dispatch tail so the pure
selection wiring runs with no Postgres — it proves both top-up calls receive a
``sources`` list with the excluded source removed and the non-excluded sources
retained. The DB-backed end-to-end proof (13F held to quota, residual rolls to
other sources, no under-fill) lives in
``tests/test_sec_manifest_worker.py::TestFairness`` (run with ``-m db``).
"""

from __future__ import annotations

from typing import Any, cast

import psycopg
import pytest

import app.jobs.sec_manifest_worker as worker
from app.jobs.sec_manifest_worker import (
    _TOPUP_EXCLUDED_SOURCES,
    ParseOutcome,
    WorkerStats,
    clear_registered_parsers,
    register_parser,
    run_manifest_worker,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    clear_registered_parsers()
    yield
    clear_registered_parsers()


def _fake_parser(conn: Any, row: Any) -> ParseOutcome:
    return ParseOutcome(status="parsed", parser_version="fake-v1")


def test_excluded_source_dropped_from_both_topup_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    # Register the heavy excluded source alongside cheap ones so
    # registered_parser_sources() is production-shape.
    for source in ("sec_form4", "sec_form3", "sec_13f_hr", "sec_def14a"):
        register_parser(source, _fake_parser)

    # The excluded set is exactly what the worker module declares.
    assert "sec_13f_hr" in _TOPUP_EXCLUDED_SOURCES

    captured: dict[str, list[str]] = {}

    # Per-source phases (R / A) return nothing → rows stay empty → the full
    # max_rows budget falls through to Phase B, firing BOTH top-up calls.
    monkeypatch.setattr(worker, "iter_pending_recent", lambda *a, **k: iter(()))
    monkeypatch.setattr(worker, "iter_pending", lambda *a, **k: iter(()))
    monkeypatch.setattr(worker, "iter_retryable", lambda *a, **k: iter(()))

    def _cap_pending(conn: Any, *, sources: Any, exclude_accessions: Any, limit: int) -> Any:
        captured["pending"] = list(sources)
        return iter(())

    def _cap_retryable(conn: Any, *, sources: Any, exclude_accessions: Any, limit: int) -> Any:
        captured["retryable"] = list(sources)
        return iter(())

    monkeypatch.setattr(worker, "iter_pending_topup", _cap_pending)
    monkeypatch.setattr(worker, "iter_retryable_topup", _cap_retryable)
    monkeypatch.setattr(
        worker,
        "_prefetch_then_dispatch",
        lambda conn, rows, *, now: WorkerStats(rows_processed=0, parsed=0, tombstoned=0, failed=0, skipped_no_parser=0),
    )

    # Conn is never touched: every DB selector + the dispatch tail are stubbed.
    dummy_conn = cast("psycopg.Connection[Any]", object())
    run_manifest_worker(dummy_conn, source=None, max_rows=200, tick_id=0)

    # Both top-up calls fired and both dropped the excluded source.
    assert "pending" in captured and "retryable" in captured
    for phase in ("pending", "retryable"):
        assert "sec_13f_hr" not in captured[phase], phase
        # Non-excluded registered sources are retained (residual rolls to them).
        assert "sec_form4" in captured[phase], phase
        assert "sec_form3" in captured[phase], phase
        assert "sec_def14a" in captured[phase], phase
