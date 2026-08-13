"""Tests for executor `_record_layer_failed` forensics capture (#645).

Verifies the new error_message + error_traceback + error_fingerprint
columns get populated, the message length is capped, and the
fingerprint groups repeats of the same exception class + frame.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest

from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test Postgres not reachable",
)


@pytest.fixture(autouse=True)
def _redirect_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point `settings.database_url` at ebull_test for every test in
    this module. `_record_layer_failed` opens its own connection via
    `settings.database_url`; without this redirect it would write to
    the dev DB and the test seed/assert in ebull_test would never
    observe its mutation."""
    from app.config import settings

    monkeypatch.setattr(settings, "database_url", _test_database_url())


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[object]]:
    c: psycopg.Connection[object] = psycopg.connect(_test_database_url(), autocommit=True)
    try:
        yield c
    finally:
        c.close()


def _seed_running_layer(conn: psycopg.Connection[object], layer: str) -> int:
    """Seed a sync_layer_progress row in 'running' state.

    The parent sync_run is inserted as 'complete' (NOT 'running') so
    the partial unique index `idx_sync_runs_single_running` does not
    block parallel test cases — what the test exercises is the layer
    row's status transition, not the parent sync_run gate.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_runs
                (scope, trigger, started_at, finished_at, status, layers_planned)
            VALUES ('full', 'manual', now() - interval '1 min', now(), 'complete', 1)
            RETURNING sync_run_id
            """,
        )
        row = cur.fetchone()
        assert row is not None
        sid = int(row[0])  # type: ignore[index]
        cur.execute(
            """
            INSERT INTO sync_layer_progress
                (sync_run_id, layer_name, status, started_at)
            VALUES (%s, %s, 'running', now())
            """,
            (sid, layer),
        )
    return sid


def _read_forensics(
    conn: psycopg.Connection[object],
    sync_run_id: int,
    layer: str,
) -> tuple[str, str | None, str | None, str | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, error_message, error_traceback, error_fingerprint
            FROM sync_layer_progress
            WHERE sync_run_id = %s AND layer_name = %s
            """,
            (sync_run_id, layer),
        )
        row = cur.fetchone()
        assert row is not None
        return (
            str(row[0]),  # type: ignore[index]
            None if row[1] is None else str(row[1]),  # type: ignore[index]
            None if row[2] is None else str(row[2]),  # type: ignore[index]
            None if row[3] is None else str(row[3]),  # type: ignore[index]
        )


def _raise_keyerror() -> None:
    """Helper that always raises from the same source line so the
    fingerprint test can compare two captures of the same failure."""
    d: dict[str, int] = {}
    _ = d["nope"]  # KeyError: 'nope'


def _raise_runtimeerror_with_long_message() -> None:
    raise RuntimeError("X" * 5000)


class TestRecordLayerFailedCapturesForensics:
    def test_captures_message_traceback_fingerprint(self, conn: psycopg.Connection[object]) -> None:
        from app.services.sync_orchestrator.executor import _record_layer_failed

        layer = f"test-forensics-{uuid4()}"
        sid = _seed_running_layer(conn, layer)

        try:
            _raise_keyerror()
        except KeyError as exc:
            _record_layer_failed(sid, layer, exc)

        status, msg, tb, fp = _read_forensics(conn, sid, layer)
        assert status == "failed"
        assert msg is not None
        assert "KeyError" in msg
        assert "'nope'" in msg
        assert tb is not None
        assert "_raise_keyerror" in tb
        assert fp is not None
        # SHA1 hex = 40 chars
        assert len(fp) == 40

    def test_message_length_capped(self, conn: psycopg.Connection[object]) -> None:
        from app.services.sync_orchestrator.executor import _record_layer_failed

        layer = f"test-msg-cap-{uuid4()}"
        sid = _seed_running_layer(conn, layer)

        try:
            _raise_runtimeerror_with_long_message()
        except RuntimeError as exc:
            _record_layer_failed(sid, layer, exc)

        _status, msg, _tb, _fp = _read_forensics(conn, sid, layer)
        assert msg is not None
        # repr() adds wrapping ('RuntimeError(...)') so the cap is on
        # the repr, not on the inner string.
        assert len(msg) <= 1000

    def test_traceback_length_capped(self, conn: psycopg.Connection[object]) -> None:
        from app.services.sync_orchestrator.executor import _record_layer_failed

        layer = f"test-tb-cap-{uuid4()}"
        sid = _seed_running_layer(conn, layer)

        # Force a deep stack so format_exc could exceed the cap on a
        # debug build with verbose locals. Cap is 8000.
        def deep(n: int) -> None:
            if n == 0:
                _raise_keyerror()
                return
            deep(n - 1)

        try:
            deep(10)
        except KeyError as exc:
            _record_layer_failed(sid, layer, exc)

        _status, _msg, tb, _fp = _read_forensics(conn, sid, layer)
        assert tb is not None
        assert len(tb) <= 8000

    def test_fingerprint_groups_repeats_of_same_failure(self, conn: psycopg.Connection[object]) -> None:
        # Two captures of the SAME source-line KeyError should produce
        # the same fingerprint, even though tracebacks contain
        # different absolute paths or line numbers across runs.
        from app.services.sync_orchestrator.executor import _record_layer_failed

        layer_a = f"test-fp-a-{uuid4()}"
        layer_b = f"test-fp-b-{uuid4()}"
        sid_a = _seed_running_layer(conn, layer_a)
        sid_b = _seed_running_layer(conn, layer_b)

        try:
            _raise_keyerror()
        except KeyError as exc:
            _record_layer_failed(sid_a, layer_a, exc)
        try:
            _raise_keyerror()
        except KeyError as exc:
            _record_layer_failed(sid_b, layer_b, exc)

        _, _, _, fp_a = _read_forensics(conn, sid_a, layer_a)
        _, _, _, fp_b = _read_forensics(conn, sid_b, layer_b)
        assert fp_a is not None and fp_b is not None
        assert fp_a == fp_b

    def test_contract_guard_fingerprint_is_deterministic_across_restarts(self) -> None:
        # The contract-guard path in `_run_layers_loop` formats the
        # expected/got sequences into the exception message. If those
        # were rendered as `set(...)` reprs the fingerprint would be
        # hash-seed-dependent and the same contract violation would
        # group differently on every worker restart. Sorting both
        # sides keeps the message + fingerprint stable.
        from app.services.sync_orchestrator.executor import _build_forensics

        # Reproduce the executor's contract-guard message verbatim
        # (the actual call site sorts both sequences).
        emits = ["scoring", "recommendations"]
        returned = ["recommendations", "scoring"]
        msg = f"refresh contract violation: expected {sorted(emits)}, got {sorted(returned)}"
        # Same logical violation reported by another worker that may
        # have shuffled the inputs.
        msg_alt = f"refresh contract violation: expected {sorted(reversed(emits))}, got {sorted(reversed(returned))}"
        _, _, fp_a = _build_forensics(RuntimeError(msg))
        _, _, fp_b = _build_forensics(RuntimeError(msg_alt))
        assert fp_a == fp_b

    def test_records_traceback_when_called_outside_active_except(self, conn: psycopg.Connection[object]) -> None:
        # The contract-guard path in `_run_layers_loop` builds a
        # RuntimeError without raising it, then calls
        # `_record_layer_failed(error=that_runtime_error)`. If
        # `_build_forensics` used `traceback.format_exc()` the
        # traceback column would record the literal stub
        # `"NoneType: None\n"` and the fingerprint would collapse to
        # the hash of that stub for every contract violation.
        from app.services.sync_orchestrator.executor import _record_layer_failed

        layer = f"test-no-active-except-{uuid4()}"
        sid = _seed_running_layer(conn, layer)

        # Construct an error WITHOUT raising it — mirrors the contract
        # guard path exactly (see executor.py:269 `contract_exc = ...`).
        contract_exc = RuntimeError("refresh contract violation: expected {a}, got [b]")
        _record_layer_failed(sid, layer, contract_exc)

        _status, msg, tb, fp = _read_forensics(conn, sid, layer)
        assert msg is not None and "refresh contract violation" in msg
        assert tb is not None
        assert "NoneType: None" not in tb
        assert "RuntimeError" in tb
        assert fp is not None and len(fp) == 40

    def test_fingerprint_differs_for_different_exception_class(self, conn: psycopg.Connection[object]) -> None:
        from app.services.sync_orchestrator.executor import _record_layer_failed

        layer_a = f"test-fp-diff-a-{uuid4()}"
        layer_b = f"test-fp-diff-b-{uuid4()}"
        sid_a = _seed_running_layer(conn, layer_a)
        sid_b = _seed_running_layer(conn, layer_b)

        try:
            _raise_keyerror()
        except KeyError as exc:
            _record_layer_failed(sid_a, layer_a, exc)
        try:
            raise ValueError("not the same shape")
        except ValueError as exc:
            _record_layer_failed(sid_b, layer_b, exc)

        _, _, _, fp_a = _read_forensics(conn, sid_a, layer_a)
        _, _, _, fp_b = _read_forensics(conn, sid_b, layer_b)
        assert fp_a is not None and fp_b is not None
        assert fp_a != fp_b
