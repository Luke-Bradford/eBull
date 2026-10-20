"""#2327 — ``cusip_universe_backfill`` runs the 13D/G bridge only when enabled, and a bridge failure does not cost
the fuzzy pass: the pass runs, then the failure is re-raised so the run records as failed."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from app.services import cusip_bridge_13dg, sec_13f_securities_list
from app.workers import scheduler


class _Conn:
    def __init__(self) -> None:
        self.rolled_back = 0

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        self.rolled_back += 1

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *_: object) -> None:
        pass


def _result() -> Any:
    sweep = SimpleNamespace(promoted=0, rewashed=0)
    return SimpleNamespace(
        inserted=3,
        unpublished_quarter=None,
        list_rows=0,
        instruments_seen=0,
        skipped_already_mapped=0,
        tombstoned_unresolvable=0,
        tombstoned_ambiguous=0,
        tombstoned_conflict=0,
        tombstoned_option_pseudo_cusip=0,
        sweep=sweep,
    )


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    h = SimpleNamespace(conn=_Conn(), tracker=SimpleNamespace(run_id=7, row_count=None, note=None), calls=[])

    @contextmanager
    def tracked(_: str) -> Iterator[Any]:
        yield h.tracker

    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", lambda: h.conn)
    monkeypatch.setattr(
        sec_13f_securities_list, "backfill_cusip_coverage", lambda conn: h.calls.append("fuzzy") or _result()
    )
    return h


def test_disabled_bridge_is_not_called(harness: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler.settings, "cusip_13dg_bridge_enabled", False)
    monkeypatch.setattr(sec_13f_securities_list, "ensure_window_lists", lambda conn: harness.calls.append("lists"))

    scheduler.cusip_universe_backfill()

    assert harness.calls == ["fuzzy"]
    assert harness.tracker.note is None


def test_bridge_failure_runs_the_fuzzy_pass_then_fails_the_run(
    harness: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scheduler.settings, "cusip_13dg_bridge_enabled", True)

    def boom(conn: Any) -> Any:
        raise RuntimeError("SEC 503")

    monkeypatch.setattr(sec_13f_securities_list, "ensure_window_lists", boom)

    with pytest.raises(RuntimeError, match="SEC 503"):
        scheduler.cusip_universe_backfill()

    assert harness.calls == ["fuzzy"]
    assert harness.conn.rolled_back == 1
    assert harness.tracker.row_count == 3


def test_enabled_bridge_runs_before_the_fuzzy_pass(harness: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler.settings, "cusip_13dg_bridge_enabled", True)
    monkeypatch.setattr(
        sec_13f_securities_list, "ensure_window_lists", lambda conn: harness.calls.append("lists") or []
    )
    bridge = cusip_bridge_13dg.BridgeResult(latest_quarter="2026q3", gaps=())
    bridge.inserted["bind"] = 2

    def bind(conn: Any, *, lists: Any, job_run_id: int) -> Any:
        harness.calls.append(("bind", job_run_id))
        return bridge

    monkeypatch.setattr(cusip_bridge_13dg, "bind_13dg_cusips", bind)

    scheduler.cusip_universe_backfill()

    assert harness.calls == ["lists", ("bind", 7), "fuzzy"]
    assert harness.tracker.row_count == 5
    assert harness.tracker.note is not None and "13dg-bridge-v1" in harness.tracker.note
