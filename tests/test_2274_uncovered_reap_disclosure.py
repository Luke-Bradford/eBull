"""#2274 — the Processes table discloses the jobs it does not carry.

PURE-LOGIC, no DB, per the repo's lean-test default. The SQL mechanism this
builds on (the event-vs-row collapse, the ``finished_at`` window, the
three-column reap predicate) is already pinned against real rows in
``tests/test_2274_recent_reap_counts.py``; re-running it here would buy nothing
and would evict this whole module from the fast tier, since the ``db`` marker is
per-MODULE.

What IS new and is tested here: the residual filter, the display floor, the
ordering contract, and the ``partial`` suppression — none of which is SQL.

Spec: ``docs/proposals/ops/2026-09-19-2274-uncovered-reap-disclosure.md``.
"""

from __future__ import annotations

import contextlib
from typing import Any, cast

import psycopg
import pytest

from app.api import processes as processes_api
from app.services.processes import ProcessSnapshot, UncoveredReap, scheduled_adapter
from app.services.processes.scheduled_adapter import (
    RECENT_REAP_CHIP_FLOOR,
    uncovered_reap_counts,
)

_CONN = cast(psycopg.Connection[Any], object())


def _counts(monkeypatch: pytest.MonkeyPatch, table: dict[str, tuple[int, int]]) -> None:
    """Stand in for the batched aggregate with an exact, readable table."""
    monkeypatch.setattr(scheduled_adapter, "_recent_reap_counts", lambda conn, **_: table)


def test_only_the_residual_survives(monkeypatch: pytest.MonkeyPatch) -> None:
    """A job WITH a row is absent however loudly it reaps; one without is present.

    This is the whole point: the per-row chip already answers for the covered
    side, and repeating it here would double-report the same incident on one
    page.
    """
    _counts(
        monkeypatch,
        {
            "sec_filing_documents_ingest": (9, 9),  # covered — the chip has it
            "daily_candle_refresh": (11, 14),  # uncovered — nothing shows it
        },
    )

    out = uncovered_reap_counts(_CONN, covered={"sec_filing_documents_ingest"})

    assert out == (UncoveredReap(job_name="daily_candle_refresh", events=11, runs=14),)


def test_events_and_runs_are_not_transposed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both numbers survive, in the right slots.

    ⚠ The fixture uses ``events != runs`` deliberately. On the real corpus the
    two are often equal (one reaped row per event), so a fixture copied from
    production data could not tell a transposition from a correct read.
    """
    _counts(monkeypatch, {"daily_candle_refresh": (4, 25)})

    (entry,) = uncovered_reap_counts(_CONN, covered=set())

    assert entry.events == 4
    assert entry.runs == 25


def test_below_the_chip_floor_is_not_disclosed(monkeypatch: pytest.MonkeyPatch) -> None:
    """The floor is the SHIPPED constant, so the page cannot show two thresholds.

    A job the per-row chip would stay silent about must not become loud merely
    by lacking a row — that would make the disclosure a stricter signal than the
    thing it is disclosing the absence of.
    """
    _counts(
        monkeypatch,
        {
            "just_under": (RECENT_REAP_CHIP_FLOOR - 1, 99),
            "exactly_at": (RECENT_REAP_CHIP_FLOOR, 1),
        },
    )

    out = uncovered_reap_counts(_CONN, covered=set())

    assert [e.job_name for e in out] == ["exactly_at"]


def test_order_is_events_desc_then_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """``(-events, job_name)`` — and the fixture defeats the two wrong answers.

    ``zz_loudest`` sorts LAST alphabetically but has the most events, so a
    name-only sort fails. The two equal-event jobs are supplied in reverse
    alphabetical order, so returning input order fails.
    """
    _counts(
        monkeypatch,
        {
            "b_quiet": (3, 3),
            "a_quiet": (3, 3),
            "zz_loudest": (11, 11),
        },
    )

    out = uncovered_reap_counts(_CONN, covered=set())

    assert [e.job_name for e in out] == ["zz_loudest", "a_quiet", "b_quiet"]


# ---------------------------------------------------------------------------
# Snapshot composition
# ---------------------------------------------------------------------------


class _StubAdapter:
    def __init__(self, rows: list[Any], *, raises: bool = False) -> None:
        self._rows = rows
        self._raises = raises

    def list_rows(self, conn: Any) -> list[Any]:
        if self._raises:
            raise RuntimeError("adapter down")
        return self._rows


@contextlib.contextmanager
def _no_snapshot(conn: Any):
    """``snapshot_read`` needs a real connection (it commits + sets isolation)."""
    yield


def _stub_adapters(
    monkeypatch: pytest.MonkeyPatch,
    *,
    scheduled_raises: bool = False,
) -> None:
    monkeypatch.setattr(processes_api, "snapshot_read", _no_snapshot)
    monkeypatch.setattr(processes_api, "bootstrap_adapter", _StubAdapter([]))
    monkeypatch.setattr(
        processes_api,
        "scheduled_adapter",
        _StubAdapter([], raises=scheduled_raises),
    )
    monkeypatch.setattr(processes_api, "ingest_sweep_adapter", _StubAdapter([]))


def test_partial_snapshot_discloses_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A raising adapter must not INVENT a coverage gap.

    ``covered`` is built from the rows actually collected, so a missing
    mechanism's jobs would be named as uncovered when they are nothing of the
    sort. The disclosure is skipped entirely, and ``partial`` — already rendered
    as "this snapshot is incomplete" — is what the operator reads instead.
    """
    _stub_adapters(monkeypatch, scheduled_raises=True)
    called: list[int] = []
    monkeypatch.setattr(
        processes_api.scheduled_adapter,
        "uncovered_reap_counts",
        lambda conn, **_: called.append(1) or (),
        raising=False,
    )

    snapshot = processes_api._gather_snapshot(_CONN)

    assert snapshot.partial is True
    assert snapshot.uncovered_reaps == ()
    assert called == [], "the residual must not even be READ on a partial snapshot"


def test_a_failing_residual_read_flags_partial_rather_than_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Returning () silently would claim "none"; the envelope says "incomplete".

    Same contract the adapter loop already has — one failing read does not take
    the whole page down, but it also does not get to pass itself off as an
    all-clear.
    """
    _stub_adapters(monkeypatch)

    def _boom(conn: Any, **_: Any) -> tuple[UncoveredReap, ...]:
        raise RuntimeError("read down")

    monkeypatch.setattr(processes_api.scheduled_adapter, "uncovered_reap_counts", _boom, raising=False)

    snapshot = processes_api._gather_snapshot(_CONN)

    assert snapshot.partial is True
    assert snapshot.uncovered_reaps == ()


def test_the_field_reaches_the_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """A helper-only test passes while the field never reaches the browser.

    This is the step that would have caught PR #3211's shape of defect — backend
    correct, operator still cannot see it — one layer earlier.
    """
    monkeypatch.setattr(
        processes_api,
        "_gather_snapshot",
        lambda conn: ProcessSnapshot(
            rows=(),
            partial=False,
            uncovered_reaps=(UncoveredReap(job_name="daily_candle_refresh", events=11, runs=14),),
        ),
    )

    body = processes_api.list_processes(conn=_CONN)

    assert [u.model_dump() for u in body.uncovered_reaps] == [
        {"job_name": "daily_candle_refresh", "events": 11, "runs": 14}
    ]


def test_the_field_defaults_empty_for_existing_callers() -> None:
    """``ProcessSnapshot`` predates this field; older constructions still work."""
    assert ProcessSnapshot(rows=(), partial=False).uncovered_reaps == ()
