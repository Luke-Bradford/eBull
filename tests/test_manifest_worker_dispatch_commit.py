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


# --- #3111 slice 2: what the tick reports to the operator ---


def test_telemetry_records_only_rows_whose_handling_did_not_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same all-buckets batch, now asserted on the operator-facing aggregate.

    The axis is "did the worker FINISH with this row", not "is the outcome
    good". A ``tombstoned`` row got a terminal decision and moved — and 69.5%
    of worker-written tombstones are deliberate policy (retention floor, 424B2
    volume cap, latest-N cap; census in the #3111 spec §5a), so counting them
    as errors would report policy as failure. A ``failed`` row is coming back.
    """
    from app.services.job_telemetry import JobTelemetryAggregator

    def _parse(_c: Any, row: ManifestRow) -> ParseOutcome:
        if row.accession_number.endswith("2"):
            return ParseOutcome(status="tombstoned", raw_status="stored")
        if row.accession_number.endswith("3"):
            raise RuntimeError("parser blew up")
        if row.accession_number.endswith("5"):
            return ParseOutcome(status="parsed", raw_status="absent")
        if row.accession_number.endswith("7"):
            return ParseOutcome(status="failed", error="iXBRL fetch timed out")
        return ParseOutcome(status="parsed", raw_status="stored")

    register_parser("sec_form4", _parse, requires_raw_payload=True)

    def _transition(_c: Any, accession: str, **_k: Any) -> None:
        if accession.endswith("6"):
            raise RuntimeError("deadlock victim")

    _stub_transition(monkeypatch, _transition)
    conn = _CountingConn()
    agg = JobTelemetryAggregator()
    rows = [
        _row("0000000001-26-000001"),  # parsed          -> not recorded
        _row("0000000001-26-000002"),  # tombstoned      -> not recorded
        _row("0000000001-26-000003"),  # parser raised   -> error
        _row("0000000001-26-000004", source="finra_regsho_daily"),  # no parser -> skip
        _row("0000000001-26-000005"),  # raw violation   -> error
        _row("0000000001-26-000006"),  # transition raised -> error
        _row("0000000001-26-000007"),  # parser returned failed -> error
    ]

    stats = _dispatch_rows(conn, rows, now=_NOW, telemetry=agg)  # type: ignore[arg-type]

    # One error per row whose handling did not complete, and no others: the
    # aggregate reconciles exactly with slice 1's counters.
    assert agg.rows_errored == stats.failed + stats.dispatch_errors == 4
    assert set(agg.to_error_classes_jsonb()) == {
        "ParserRaised:RuntimeError",
        "RawPayloadMissing",
        "DispatchFailed:RuntimeError",
        "ParserReportedFailure:sec_form4",
    }
    # The parser's own reason text is the sample an operator reads.
    assert "timed out" in agg.to_error_classes_jsonb()["ParserReportedFailure:sec_form4"]["sample_message"]
    # Subject carries source + accession so the failure is locatable.
    assert (
        agg.to_error_classes_jsonb()["DispatchFailed:RuntimeError"]["last_subject"] == "sec_form4 0000000001-26-000006"
    )
    assert agg.to_skips_jsonb() == {"no_parser_registered": 1}
    # ⚠ Progress is NOT this producer's: ``report_progress`` owns that surface
    # for this job, and a second writer would race the #2274 heartbeat.
    assert agg.processed_count == 0
    assert not agg.has_processed_state


def test_a_parser_raise_whose_transition_also_raises_records_exactly_one_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recording at the counter site, not where the exception was caught.

    The parser raise is handled by calling ``transition_status``; if THAT
    raises, the row lands in ``dispatch_errors`` and never reaches ``failed``.
    Recording the parser error where it was caught would emit two errors for
    one row and break ``rows_errored == failed + dispatch_errors``.
    """
    from app.services.job_telemetry import JobTelemetryAggregator

    def _boom_parser(_c: Any, _r: ManifestRow) -> ParseOutcome:
        raise ValueError("parser blew up")

    def _boom_transition(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("illegal state transition")

    register_parser("sec_form4", _boom_parser)
    _stub_transition(monkeypatch, _boom_transition)
    conn = _CountingConn()
    agg = JobTelemetryAggregator()

    stats = _dispatch_rows(conn, [_row("0000000001-26-000001")], now=_NOW, telemetry=agg)  # type: ignore[arg-type]

    assert (stats.failed, stats.dispatch_errors) == (0, 1)
    assert agg.rows_errored == 1
    assert list(agg.to_error_classes_jsonb()) == ["DispatchFailed:RuntimeError"]


def test_a_failed_outcome_with_no_error_text_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ParseOutcome.error`` is legally ``None`` and ``record_error`` slices it."""
    from app.services.job_telemetry import JobTelemetryAggregator

    register_parser("sec_form4", lambda _c, _r: ParseOutcome(status="failed"))
    _stub_transition(monkeypatch, lambda *_a, **_k: None)
    conn = _CountingConn()
    agg = JobTelemetryAggregator()

    stats = _dispatch_rows(conn, [_row("0000000001-26-000001")], now=_NOW, telemetry=agg)  # type: ignore[arg-type]

    assert stats.failed == 1
    assert agg.to_error_classes_jsonb()["ParserReportedFailure:sec_form4"]["sample_message"]


def test_telemetry_is_optional_and_absent_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every ``scripts/backfill_*.py`` caller passes no aggregator."""
    register_parser("sec_form4", lambda _c, _r: ParseOutcome(status="failed", error="x"))
    _stub_transition(monkeypatch, lambda *_a, **_k: None)
    conn = _CountingConn()

    stats = _dispatch_rows(conn, [_row("0000000001-26-000001")], now=_NOW)  # type: ignore[arg-type]

    assert stats.failed == 1


def test_a_failed_outcome_with_an_EMPTY_error_string_also_gets_the_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The review bot's NITPICK, pinned as behaviour rather than argued once.

    ``or`` catches ``""`` as well as ``None``, and that is deliberate: an empty
    string is not a message. Rendering an empty ``sample_message`` in the
    Processes Errors tab is strictly worse for an operator than the fallback
    sentence, and the class + subject still identify the row either way. The
    parser's original value reaches the manifest untouched — only the telemetry
    SAMPLE is substituted.
    """
    from app.services.job_telemetry import JobTelemetryAggregator

    register_parser("sec_form4", lambda _c, _r: ParseOutcome(status="failed", error=""))
    _stub_transition(monkeypatch, lambda *_a, **_k: None)
    conn = _CountingConn()
    agg = JobTelemetryAggregator()

    _dispatch_rows(conn, [_row("0000000001-26-000001")], now=_NOW, telemetry=agg)  # type: ignore[arg-type]

    sample = agg.to_error_classes_jsonb()["ParserReportedFailure:sec_form4"]["sample_message"]
    assert sample == "parser reported failure with no error text"
