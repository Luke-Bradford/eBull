"""#3614 item 4 — the kill-switch drill against a real database.

What only a database proves: the sandbox activation never commits (also when evaluation
raises, and on an autocommit connection, which is refused first), each chokepoint
loader stays inside the drill's transaction, an operator activation queued behind the
drill commits intact and is not misread as a sandbox commit, and observe mode writes
nothing. The pure classification and estimate are in ``test_3614_kill_switch_drill.py``.

Executor-path refusal with the switch committed on is already held by existing tests,
each asserting the broker entry method is never reached: C1
``test_order_client.py::test_kill_switch_activated_after_approval_refuses``, C2
``test_ai_trial_executor_db.py::test_the_kill_switch_refuses_and_an_uncertain_leg_resumes_on_its_committed_uuid``
(the shared ``_trading_enabled_refusal``), C3
``test_2949_core_restart_recovery_db.py::test_scenario_6_kill_switch_refuses_a_clean_re_entry_after_a_crash``.
"""

from __future__ import annotations

import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.pq import TransactionStatus

import app.services.execution_guard as execution_guard
import app.services.kill_switch_drill as drill
import app.services.strategy_core_preflight as core_preflight
from app.services.execution_guard import RuleResult, load_kill_switch
from app.services.ops_monitor import activate_kill_switch
from app.services.runtime_config import get_runtime_config
from app.services.strategy_core_mandate import CoreMandate
from app.services.strategy_core_preflight import preflight_core_submission
from app.services.strategy_paper_executor import _trading_enabled_refusal, load_trading_enabled_state
from tests.fixtures.ebull_test_db import test_database_url

Conn = psycopg.Connection[Any]
CORE_INSTRUMENT_ID = 920_651  # nothing seeds it: C3 refuses on the kill switch before the instrument


def _connect() -> Conn:
    return psycopg.connect(test_database_url())


def _set_state(conn: Conn, *, auto: bool = True, kill: bool = False, by: str | None = None) -> None:
    conn.execute("UPDATE runtime_config SET enable_auto_trading = %s WHERE id = TRUE", (auto,))
    conn.execute(
        "UPDATE kill_switch SET is_active = %s, activated_by = %s, activated_at = CASE WHEN %s THEN now() END "
        "WHERE id = TRUE",
        (kill, by, kill),
    )
    conn.commit()


def _kill_row(conn: Conn) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT is_active, activated_at, activated_by, reason FROM kill_switch WHERE id = TRUE"
    ).fetchone()
    conn.commit()
    assert row is not None
    return tuple(row)


def _audit_count(conn: Conn) -> int:
    row = conn.execute("SELECT count(*) FROM runtime_config_audit WHERE field = 'kill_switch'").fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def _with_mandate(monkeypatch: pytest.MonkeyPatch) -> None:
    mandate = CoreMandate(
        event_id=7,
        revision=3,
        enabled=True,
        base_currency="USD",
        core_instrument_id=CORE_INSTRUMENT_ID,
        core_target_pct=Decimal("50"),
        liquidity_reserve_pct=Decimal("5"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("10"),
        policy_version="test",
    )
    monkeypatch.setattr(drill, "load_core_mandate", lambda _conn: mandate)


def _run() -> drill.DrillRun:
    run = drill.run_kill_switch_drill(_connect, trigger="manual", actor="test")
    assert run is not None
    return run


def _outcomes(run: drill.DrillRun) -> dict[str, str]:
    return {c.chokepoint: c.outcome for c in run.chokepoints}


# --------------------------------------------------------------------------
# Sandbox mode
# --------------------------------------------------------------------------


def test_sandbox_passes_and_commits_nothing_to_the_kill_row(ebull_test_conn: Conn) -> None:
    _set_state(ebull_test_conn)
    before, audits = _kill_row(ebull_test_conn), _audit_count(ebull_test_conn)

    run = _run()

    assert (run.mode, run.entry_verdict, run.run_failure) == ("sandbox", "passed", None)
    assert _outcomes(run) == {"C1": "kill_refused", "C2": "kill_refused", "C3": "not_applicable"}
    assert all(c.drill_read_kill_active for c in run.chokepoints), "the drill saw its own activation"
    assert run.kill_active_at_verify is False
    assert _kill_row(ebull_test_conn) == before
    assert _audit_count(ebull_test_conn) == audits

    event = ebull_test_conn.execute(
        "SELECT entry_verdict, book_verdict, estimated_time_to_flat_s, estimate_null_reason, mode, run_token, "
        "positions_without_stop, positions_without_target "
        "FROM kill_switch_drill_events WHERE kill_switch_drill_event_id = %s",
        (run.event_id,),
    ).fetchone()
    # The defect counts are written, not left at their NULL default (sql/477's column included).
    assert event == ("passed", "ok", Decimal("0"), None, "sandbox", run.run_token, 0, 0)
    rows = ebull_test_conn.execute(
        "SELECT chokepoint, outcome FROM kill_switch_drill_chokepoints WHERE event_id = %s ORDER BY 1",
        (run.event_id,),
    ).fetchall()
    assert rows == [("C1", "kill_refused"), ("C2", "kill_refused"), ("C3", "not_applicable")]


def test_c3_with_a_mandate_refuses_on_the_kill_switch(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_state(ebull_test_conn)
    _with_mandate(monkeypatch)
    run = _run()
    assert _outcomes(run)["C3"] == "kill_refused"
    assert (run.core_mandate_event_id, run.core_mandate_revision, run.core_instrument_id) == (7, 3, CORE_INSTRUMENT_ID)
    assert run.entry_verdict == "passed"


def test_auto_trading_off_masks_c2_and_c3_but_not_c1(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_state(ebull_test_conn, auto=False)
    _with_mandate(monkeypatch)
    run = _run()
    assert _outcomes(run) == {"C1": "kill_refused", "C2": "other_refusal", "C3": "other_refusal"}
    c1 = next(c for c in run.chokepoints if c.chokepoint == "C1")
    assert set(c1.failed_rules) == {"kill_switch", "auto_trading"}
    assert run.entry_verdict == "incomplete"


# --------------------------------------------------------------------------
# Revert-probes: remove each kill check and the drill must fail
# --------------------------------------------------------------------------


def test_c1_without_its_kill_check_is_allowed(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_state(ebull_test_conn, auto=False)  # kill rule still reached with auto-trading off
    monkeypatch.setattr(execution_guard, "_check_kill_switch", lambda _row: RuleResult(rule="kill_switch", passed=True))
    run = _run()
    assert _outcomes(run)["C1"] == "allowed"
    assert run.entry_verdict == "failed"


def test_c2_without_its_kill_check_is_allowed(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_state(ebull_test_conn)
    monkeypatch.setattr(
        drill, "decide_trading_enabled", lambda s: None if s.enable_auto_trading else "auto_trading_disabled"
    )
    run = _run()
    assert _outcomes(run)["C2"] == "allowed"
    assert run.entry_verdict == "failed"


def test_c3_without_its_kill_check_is_allowed(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_state(ebull_test_conn)
    _with_mandate(monkeypatch)
    original = core_preflight.decide_core_preflight
    monkeypatch.setattr(
        core_preflight,
        "decide_core_preflight",
        lambda obs, **kw: original(replace(obs, kill_switch_active=False), **kw),
    )
    run = _run()
    c3 = next(c for c in run.chokepoints if c.chokepoint == "C3")
    assert c3.outcome == "allowed"
    assert c3.refusal_code == "core_instrument_missing"  # the next check down refused instead
    assert run.entry_verdict == "failed"


# --------------------------------------------------------------------------
# Never commits
# --------------------------------------------------------------------------


def test_an_evaluation_that_raises_still_commits_nothing(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_state(ebull_test_conn)
    before, audits = _kill_row(ebull_test_conn), _audit_count(ebull_test_conn)

    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("probe exploded")

    monkeypatch.setattr(drill, "evaluate_chokepoints", boom)
    run = _run()
    assert (run.run_failure, run.entry_verdict, run.mode) == ("exception", "not_run", "sandbox")
    assert run.failure_detail is not None and "probe exploded" in run.failure_detail
    assert _kill_row(ebull_test_conn) == before
    assert _audit_count(ebull_test_conn) == audits
    assert ebull_test_conn.execute(
        "SELECT book_verdict, estimate_null_reason FROM kill_switch_drill_events WHERE kill_switch_drill_event_id = %s",
        (run.event_id,),
    ).fetchone() == ("not_run", "snapshot_unavailable")


def test_an_autocommit_connection_is_refused_before_any_write(ebull_test_conn: Conn) -> None:
    _set_state(ebull_test_conn)
    before, audits = _kill_row(ebull_test_conn), _audit_count(ebull_test_conn)

    def autocommit_connect() -> Conn:
        conn = _connect()
        conn.autocommit = True
        return conn

    run = drill.run_kill_switch_drill(autocommit_connect, trigger="manual", actor="test")
    assert run is not None
    assert run.run_failure == "exception"
    assert run.failure_detail is not None and "DrillRefused" in run.failure_detail
    assert run.mode is None and run.chokepoints == []
    assert _kill_row(ebull_test_conn) == before
    assert _audit_count(ebull_test_conn) == audits


def test_each_loader_stays_inside_the_drill_transaction(ebull_test_conn: Conn) -> None:
    """A loader that committed would publish the sandbox activation to every session."""
    _set_state(ebull_test_conn)
    with _connect() as watcher:
        watcher.autocommit = True
        sandbox = _connect()
        try:
            drill._begin_bounded(sandbox)  # pyright: ignore[reportPrivateUsage]
            drill._lock_core_keys(sandbox)  # pyright: ignore[reportPrivateUsage]
            activate_kill_switch(sandbox, "loader neutrality test", activated_by="test")
            loaders = [
                lambda: load_kill_switch(sandbox),
                lambda: get_runtime_config(sandbox),
                lambda: load_trading_enabled_state(sandbox),
                lambda: preflight_core_submission(
                    sandbox, core_instrument_id=CORE_INSTRUMENT_ID, action="buy_core", now=datetime.now(UTC)
                ),
            ]
            for load in loaders:
                load()
                assert sandbox.info.transaction_status == TransactionStatus.INTRANS
                row = watcher.execute("SELECT is_active FROM kill_switch WHERE id = TRUE").fetchone()
                assert row == (False,)
        finally:
            sandbox.rollback()
            sandbox.close()
    assert _kill_row(ebull_test_conn)[0] is False


def test_c2_wrapper_keeps_its_no_commit_corrupt_branch(ebull_test_conn: Conn) -> None:
    ebull_test_conn.execute("DELETE FROM runtime_config WHERE id = TRUE")
    assert _trading_enabled_refusal(ebull_test_conn) == "runtime_config_corrupt"
    assert ebull_test_conn.info.transaction_status == TransactionStatus.INTRANS, "the corrupt branch must not commit"
    ebull_test_conn.rollback()
    assert get_runtime_config(ebull_test_conn) is not None
    ebull_test_conn.rollback()


def test_an_operator_activation_behind_the_drill_commits_intact(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_state(ebull_test_conn)
    original = drill.evaluate_chokepoints
    errors: list[BaseException] = []

    def operator() -> None:
        try:
            with _connect() as conn:
                activate_kill_switch(conn, "operator halt during drill", activated_by="operator-test")
        except BaseException as exc:  # pragma: no cover - surfaced by the assertion below
            errors.append(exc)

    thread = threading.Thread(target=operator)

    def evaluate_with_waiter(conn: Conn, **kw: Any) -> list[drill.ChokepointResult]:
        thread.start()
        time.sleep(0.5)  # the operator is now queued on the core advisory key the drill holds
        assert thread.is_alive(), "the activation must wait for the drill's transaction"
        return original(conn, **kw)

    monkeypatch.setattr(drill, "evaluate_chokepoints", evaluate_with_waiter)
    run = _run()
    thread.join(timeout=10)
    assert not errors

    assert run.run_failure is None and run.entry_verdict == "passed"
    is_active, _, by, reason = _kill_row(ebull_test_conn)
    assert (is_active, by, reason) == (True, "operator-test", "operator halt during drill")
    reasons = [
        r[0]
        for r in ebull_test_conn.execute(
            "SELECT reason FROM runtime_config_audit WHERE field = 'kill_switch' ORDER BY audit_id"
        ).fetchall()
    ]
    assert reasons[-1] == "operator halt during drill"
    assert not any(str(run.run_token) in r for r in reasons)


# --------------------------------------------------------------------------
# Observe mode
# --------------------------------------------------------------------------


def test_observe_mode_reads_the_committed_activation_and_writes_nothing(ebull_test_conn: Conn) -> None:
    _set_state(ebull_test_conn, kill=True, by="operator-test")
    before, audits = _kill_row(ebull_test_conn), _audit_count(ebull_test_conn)

    run = _run()

    assert (run.mode, run.entry_verdict, run.observed_activated_by) == ("observe", "passed", "operator-test")
    assert run.kill_active_at_start is True and run.kill_active_at_verify is None
    assert _kill_row(ebull_test_conn) == before
    assert _audit_count(ebull_test_conn) == audits


def test_observe_mode_records_a_switch_turned_off_before_it_looked(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_state(ebull_test_conn, kill=True, by="operator-test")
    original = drill._lock_core_keys  # pyright: ignore[reportPrivateUsage]
    calls = 0

    def lock_then_operator_clears(conn: Conn) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:  # observe mode, after the sandbox found the switch on
            with _connect() as other:
                other.execute("UPDATE kill_switch SET is_active = FALSE WHERE id = TRUE")
        original(conn)

    monkeypatch.setattr(drill, "_lock_core_keys", lock_then_operator_clears)
    run = _run()
    assert (run.mode, run.observe_outcome, run.entry_verdict, run.chokepoints) == (
        "observe",
        "kill_changed_before_observe",
        "not_run",
        [],
    )
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM kill_switch_drill_chokepoints WHERE event_id = %s", (run.event_id,)
    ).fetchone() == (0,)


# --------------------------------------------------------------------------
# One run at a time
# --------------------------------------------------------------------------


def test_a_second_run_while_one_holds_the_drill_lock_records_nothing(ebull_test_conn: Conn) -> None:
    _set_state(ebull_test_conn)
    with _connect() as holder:
        holder.autocommit = True
        holder.execute("SELECT pg_advisory_lock(%s, %s)", drill.DRILL_ADVISORY_LOCK)
        assert drill.run_kill_switch_drill(_connect, trigger="manual", actor="test") is None
    assert ebull_test_conn.execute("SELECT count(*) FROM kill_switch_drill_events").fetchone() == (0,)


def test_drill_evidence_refuses_update_and_delete(ebull_test_conn: Conn) -> None:
    _set_state(ebull_test_conn)
    run = _run()
    for stmt in (
        "UPDATE kill_switch_drill_events SET actor = 'x' WHERE kill_switch_drill_event_id = %s",
        "DELETE FROM kill_switch_drill_chokepoints WHERE event_id = %s",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            ebull_test_conn.execute(stmt, (run.event_id,))
        ebull_test_conn.rollback()
    # A sandbox run leaves some tables empty, and a row trigger never fires on zero
    # rows, so the other tables are held by the catalog: every table, both statements.
    tables = (
        "kill_switch_drill_events",
        "kill_switch_drill_chokepoints",
        "kill_switch_drill_positions",
        "kill_switch_drill_authority",
        "kill_switch_drill_close_samples",
    )
    # pg_trigger, not information_schema: the view keeps disabled, WHEN-conditioned and
    # UPDATE OF column triggers, and renders the function call as version-dependent text.
    # tgtype bits (catalog/pg_trigger.h): ROW 1, BEFORE 2, DELETE 8, UPDATE 16.
    rows = ebull_test_conn.execute(
        """
        SELECT c.relname, t.tgtype & 27
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
         WHERE c.relnamespace = current_schema()::regnamespace
           AND c.relname = ANY(%s)
           AND t.tgfoid = 'kill_switch_drill_append_only'::regproc
           AND t.tgenabled IN ('O', 'A')
           AND t.tgqual IS NULL
           AND cardinality(t.tgattr::int2[]) = 0
        """,
        (list(tables),),
    ).fetchall()
    assert sorted(rows) == sorted((t, 27) for t in tables)
