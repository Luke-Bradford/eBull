"""Pure-logic tests for the #1735 per-row commit contract in ``_dispatch_rows``.

No DB: a counting fake connection + monkeypatched ``transition_status`` let us
assert the commit/rollback cadence directly. The state-machine semantics of
``transition_status`` itself (the ``parsed -> parsed`` / ``tombstoned ->
tombstoned`` no-ops + the illegal-transition raise) are covered separately in
``tests/test_sec_manifest.py``; here we only pin that ``_dispatch_rows`` commits
once before the loop + once per dispatched row, and that a row-level failure
rolls back and continues instead of aborting the tick.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

import app.jobs.sec_manifest_worker as worker
from app.jobs.sec_manifest_worker import (
    ParseOutcome,
    _dispatch_rows,
    clear_registered_parsers,
    register_parser,
)
from app.services.sec_manifest import ManifestRow

_NOW = datetime(2026, 6, 26, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _isolate_registry() -> Any:
    """Clear the module-global parser registry before each test and RESTORE the
    real registry after, so fake registrations can't leak into an xdist-colocated
    test (mirrors ``test_manifest_worker_prefetch``)."""
    clear_registered_parsers()
    yield
    clear_registered_parsers()
    from app.services.manifest_parsers import register_all_parsers

    register_all_parsers()


class _CountingConn:
    """Minimal stand-in for ``psycopg.Connection`` — counts commit/rollback.

    ``_dispatch_rows`` only touches ``commit`` / ``rollback`` directly (the
    parser fakes ignore the conn and ``transition_status`` is monkeypatched), so
    nothing else is needed."""

    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _row(accession: str, *, source: str = "sec_form4", raw_status: str = "stored") -> ManifestRow:
    return ManifestRow(
        accession_number=accession,
        cik="0000000001",
        form="4",
        source=source,  # type: ignore[arg-type]
        subject_type="issuer",
        subject_id="1",
        instrument_id=1,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        accepted_at=None,
        primary_document_url=None,
        is_amendment=False,
        amends_accession=None,
        ingest_status="pending",
        parser_version=None,
        raw_status=raw_status,  # type: ignore[arg-type]
        last_attempted_at=None,
        next_retry_at=None,
        error=None,
    )


def _stub_transition(monkeypatch: pytest.MonkeyPatch, fn: Any) -> None:
    """Replace the module-level ``transition_status`` reference the worker
    imported (``app/jobs/sec_manifest_worker.py``) so no DB is touched."""
    monkeypatch.setattr(worker, "transition_status", fn)


def test_commits_once_before_loop_plus_once_per_dispatched_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_parser("sec_form4", lambda _c, _r: ParseOutcome(status="parsed", raw_status="stored"))
    _stub_transition(monkeypatch, lambda *_a, **_k: None)
    conn = _CountingConn()
    rows = [_row(f"0000000001-26-00000{i}") for i in range(1, 4)]

    stats = _dispatch_rows(conn, rows, now=_NOW)  # type: ignore[arg-type]

    # 1 pre-loop read-tx close + 1 per dispatched row.
    assert conn.commits == 1 + len(rows)
    assert conn.rollbacks == 0
    assert stats.parsed == 3


def test_skipped_rows_do_not_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    # No parser registered for the row's source → skipped before any transition.
    _stub_transition(monkeypatch, lambda *_a, **_k: None)
    conn = _CountingConn()
    rows = [_row("0000000001-26-000001", source="sec_form4")]

    stats = _dispatch_rows(conn, rows, now=_NOW)  # type: ignore[arg-type]

    # Only the pre-loop commit fires; a skipped row takes no lock, commits nothing.
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert stats.skipped_no_parser == 1


def test_parser_exception_commits_the_failed_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(_c: Any, _r: ManifestRow) -> ParseOutcome:
        raise RuntimeError("parser blew up")

    register_parser("sec_form4", _boom)
    _stub_transition(monkeypatch, lambda *_a, **_k: None)
    conn = _CountingConn()
    rows = [_row("0000000001-26-000001")]

    stats = _dispatch_rows(conn, rows, now=_NOW)  # type: ignore[arg-type]

    # pre-loop + the failed-transition commit (so the row's locks release at its
    # boundary even on the failure path).
    assert conn.commits == 1 + 1
    assert conn.rollbacks == 0
    assert stats.failed == 1


def test_raw_payload_violation_commits_the_failed_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Payload-backed parser returns parsed but raw_status='absent' → #938 turns
    # it into a failed transition, which must still commit per-row (#1735).
    register_parser(
        "sec_form4",
        lambda _c, _r: ParseOutcome(status="parsed", raw_status="absent"),
        requires_raw_payload=True,
    )
    _stub_transition(monkeypatch, lambda *_a, **_k: None)
    conn = _CountingConn()
    rows = [_row("0000000001-26-000001", raw_status="absent")]

    stats = _dispatch_rows(conn, rows, now=_NOW)  # type: ignore[arg-type]

    assert conn.commits == 1 + 1
    assert stats.failed == 1
    assert stats.raw_payload_violations == 1


def test_transition_failure_rolls_back_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A transition raising on ONE row (e.g. illegal transition / deadlock victim)
    # must roll back only that row and continue the tick, not abort it.
    poison = "0000000001-26-000002"

    def _transition(_conn: Any, accession: str, **_k: Any) -> None:
        if accession == poison:
            raise ValueError("illegal transition")

    register_parser("sec_form4", lambda _c, _r: ParseOutcome(status="parsed", raw_status="stored"))
    _stub_transition(monkeypatch, _transition)
    conn = _CountingConn()
    rows = [_row(f"0000000001-26-00000{i}") for i in range(1, 4)]

    stats = _dispatch_rows(conn, rows, now=_NOW)  # type: ignore[arg-type]

    # rows 1 + 3 commit (pre-loop + 2); row 2 rolls back; tick completes.
    assert conn.commits == 1 + 2
    assert conn.rollbacks == 1
    assert stats.parsed == 2


# --- #3111 slice 1: every row lands in exactly one outcome bucket ---


def test_a_failed_transition_is_counted_rather_than_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The outer handler now increments a counter instead of only logging.

    Before #3111 slice 1 this row was counted in ``rows_processed`` and in NO
    outcome bucket, so a tick that lost every row still returned a summary that
    read as healthy. The row itself is unchanged and re-drains next tick — that
    is what makes the loss silent rather than loud.
    """
    register_parser("sec_form4", lambda _c, _r: ParseOutcome(status="parsed", raw_status="stored"))

    def _boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("illegal state transition")

    _stub_transition(monkeypatch, _boom)
    conn = _CountingConn()
    rows = [_row("0000000001-26-000001")]

    stats = _dispatch_rows(conn, rows, now=_NOW)  # type: ignore[arg-type]

    assert stats.dispatch_errors == 1
    assert conn.rollbacks == 1
    # Not any other bucket: a transition failure is NOT a parser failure, which
    # is caught one level in and stamps a retry.
    assert (stats.parsed, stats.tombstoned, stats.failed, stats.skipped_no_parser) == (0, 0, 0, 0)
    assert stats.outcome_total() == stats.rows_processed == 1


def test_the_tick_identity_holds_across_every_outcome_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``rows_processed == parsed + tombstoned + failed + skipped + dispatch_errors``.

    One batch reaching all five buckets plus a #938 raw-payload violation, so the
    identity is exercised against the one term that is NOT part of it:
    ``raw_payload_violations`` is a SUBSET of ``failed``, written alongside it,
    and adding it would double-count. A per-bucket test cannot catch that; only
    the sum can.
    """

    def _parse(_c: Any, row: ManifestRow) -> ParseOutcome:
        if row.accession_number.endswith("2"):
            return ParseOutcome(status="tombstoned", raw_status="stored")
        if row.accession_number.endswith("3"):
            raise RuntimeError("parser blew up")
        if row.accession_number.endswith("5"):
            # Payload-backed contract violated -> #938 failed + raw_violation.
            return ParseOutcome(status="parsed", raw_status="absent")
        return ParseOutcome(status="parsed", raw_status="stored")

    register_parser("sec_form4", _parse, requires_raw_payload=True)

    def _transition(_c: Any, accession: str, **_k: Any) -> None:
        if accession.endswith("6"):
            raise RuntimeError("deadlock victim")

    _stub_transition(monkeypatch, _transition)
    conn = _CountingConn()
    rows = [
        _row("0000000001-26-000001"),  # parsed
        _row("0000000001-26-000002"),  # tombstoned
        _row("0000000001-26-000003"),  # parser raised -> failed
        _row("0000000001-26-000004", source="finra_regsho_daily"),  # no parser -> skipped
        _row("0000000001-26-000005"),  # raw-payload violation -> failed (+ violation)
        _row("0000000001-26-000006"),  # transition raised -> dispatch_errors
    ]

    stats = _dispatch_rows(conn, rows, now=_NOW)  # type: ignore[arg-type]

    assert (stats.parsed, stats.tombstoned, stats.failed) == (1, 1, 2)
    assert (stats.skipped_no_parser, stats.dispatch_errors) == (1, 1)
    assert stats.raw_payload_violations == 1
    assert stats.rows_processed == len(rows)
    assert stats.outcome_total() == stats.rows_processed
    # The #1179 invariant still holds alongside the new one: a dispatch error
    # happens AFTER the row reached the parser entry point, so it stays in
    # `processed_by_source`.
    assert sum(stats.processed_by_source.values()) == stats.rows_processed - stats.skipped_no_parser
