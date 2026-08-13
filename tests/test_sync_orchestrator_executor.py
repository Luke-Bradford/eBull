"""Tests for sync orchestrator executor.

DB-backed paths (_start_sync_run, _safe_run_and_finalize end-to-end)
use settings.database_url — the test DB. Pure-logic paths use mocks.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from unittest.mock import MagicMock, patch

import pytest

from app.services.sync_orchestrator import executor
from app.services.sync_orchestrator.types import (
    ExecutionPlan,
    LayerOutcome,
    LayerPlan,
)
from tests.fixtures.ebull_test_db import test_database_url


@pytest.fixture
def settings_use_test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Point ``settings.database_url`` at the per-worker test DB so the
    PR4a real-conn tests below never touch the operator's dev DB
    (test-DB isolation — see ``tests/fixtures/ebull_test_db``)."""
    from app.config import settings

    url = test_database_url()
    monkeypatch.setattr(settings, "database_url", url)
    yield url


def _lp(
    name: str,
    emits: tuple[str, ...],
    deps: tuple[str, ...] = (),
    is_blocking: bool = True,
) -> LayerPlan:
    return LayerPlan(
        name=name,
        emits=emits,
        reason="stale",
        dependencies=deps,
        is_blocking=is_blocking,
        estimated_items=0,
    )


class TestBlockingDependencyFailed:
    def test_failed_blocking_dep_returns_skip_reason(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        upstream: Mapping[str, LayerOutcome] = {"universe": LayerOutcome.FAILED}
        # Patch LAYERS to mark universe blocking.
        with patch.object(
            executor,
            "_blocking_dependency_failed",
            wraps=executor._blocking_dependency_failed,
        ):
            reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is not None
        assert "universe" in reason
        assert "failed" in reason

    def test_dep_skipped_on_blocking_dep_returns_reason(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        upstream: Mapping[str, LayerOutcome] = {"universe": LayerOutcome.DEP_SKIPPED}
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is not None
        assert "dep_skipped" in reason

    def test_prereq_skip_on_blocking_dep_returns_reason(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        upstream: Mapping[str, LayerOutcome] = {"universe": LayerOutcome.PREREQ_SKIP}
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is not None
        assert "prerequisite" in reason

    def test_partial_on_blocking_dep_does_not_block(self) -> None:
        """PARTIAL is explicitly 'some items worked' — downstream runs."""
        plan = _lp("scoring", ("scoring",), deps=("thesis",))
        upstream: Mapping[str, LayerOutcome] = {"thesis": LayerOutcome.PARTIAL}
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is None

    def test_failed_non_blocking_dep_does_not_block(self) -> None:
        """A FAILED non-blocking dep must not block downstream. Post-Phase 1.2
        no scheduled layer declares a non-blocking dep naturally, but the
        contract is still enforced — exercise it by fabricating a plan with
        fx_rates (is_blocking=False) as a dep."""
        plan = _lp(
            "synthetic_downstream",
            ("synthetic_downstream",),
            deps=("fx_rates",),
        )
        upstream: Mapping[str, LayerOutcome] = {"fx_rates": LayerOutcome.FAILED}
        reason = executor._blocking_dependency_failed(plan, upstream)
        assert reason is None


class TestBuildUpstreamOutcomes:
    def test_in_run_deps_use_outcomes_map(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        outcomes: dict[str, LayerOutcome] = {"universe": LayerOutcome.SUCCESS}
        resolved = executor._build_upstream_outcomes(plan, outcomes)
        assert resolved["universe"] is LayerOutcome.SUCCESS

    def test_unplanned_dep_resolved_from_job_runs(self) -> None:
        plan = _lp("candle_refresh", ("candles",), deps=("universe",))
        outcomes: dict[str, LayerOutcome] = {}
        with patch.object(
            executor,
            "_last_counting_outcome_from_job_runs",
            return_value=LayerOutcome.SUCCESS,
        ) as m:
            resolved = executor._build_upstream_outcomes(plan, outcomes)
        # PR4a (#1472): standalone caller threads gate_conn=None (no run-scoped
        # conn) → _last_counting_outcome_from_job_runs opens its own conn.
        m.assert_called_once_with("universe", gate_conn=None)
        assert resolved["universe"] is LayerOutcome.SUCCESS


class TestRunLayersLoopContract:
    """_run_layers_loop adapter-contract guards."""

    def test_adapter_returning_empty_list_marks_all_emits_failed(self, monkeypatch) -> None:
        # morning_candidate_review is the surviving composite job emitting
        # (scoring, recommendations) — same contract as the retired
        # daily_financial_facts composite.
        plan_item = _lp(
            "morning_candidate_review",
            emits=("scoring", "recommendations"),
            deps=(),
        )
        exec_plan = ExecutionPlan(
            layers_to_refresh=(plan_item,),
            layers_skipped=(),
            estimated_duration=None,
        )
        outcomes: dict[str, LayerOutcome] = {}

        # Adapter that returns empty list — contract violation.
        def bad_adapter(**kwargs):
            return []

        from dataclasses import replace

        from app.services.sync_orchestrator import registry

        monkeypatch.setitem(
            registry.LAYERS,
            "scoring",
            replace(registry.LAYERS["scoring"], refresh=bad_adapter),
        )

        # Patch audit writers to no-op.
        for writer in (
            "_record_layer_started",
            "_record_layer_failed",
            "_record_layer_skipped",
            "_record_layer_result",
        ):
            monkeypatch.setattr(executor, writer, MagicMock())
        monkeypatch.setattr(
            executor,
            "_make_progress_callback",
            lambda *a, **kw: lambda *args, **kwargs: None,
        )

        executor._run_layers_loop(sync_run_id=1, plan=exec_plan, outcomes=outcomes)

        assert outcomes == {
            "scoring": LayerOutcome.FAILED,
            "recommendations": LayerOutcome.FAILED,
        }

    def test_adapter_raising_marks_all_emits_failed(self, monkeypatch) -> None:
        plan_item = _lp(
            "morning_candidate_review",
            emits=("scoring", "recommendations"),
            deps=(),
        )
        exec_plan = ExecutionPlan(
            layers_to_refresh=(plan_item,),
            layers_skipped=(),
            estimated_duration=None,
        )
        outcomes: dict[str, LayerOutcome] = {}

        def raising_adapter(**kwargs):
            raise RuntimeError("boom")

        from dataclasses import replace

        from app.services.sync_orchestrator import registry

        monkeypatch.setitem(
            registry.LAYERS,
            "scoring",
            replace(registry.LAYERS["scoring"], refresh=raising_adapter),
        )
        for writer in (
            "_record_layer_started",
            "_record_layer_failed",
            "_record_layer_skipped",
            "_record_layer_result",
        ):
            monkeypatch.setattr(executor, writer, MagicMock())
        monkeypatch.setattr(
            executor,
            "_make_progress_callback",
            lambda *a, **kw: lambda *args, **kwargs: None,
        )

        executor._run_layers_loop(sync_run_id=1, plan=exec_plan, outcomes=outcomes)

        assert outcomes == {
            "scoring": LayerOutcome.FAILED,
            "recommendations": LayerOutcome.FAILED,
        }


class TestCategorizeError:
    # _categorize_error replaced by classify_exception from exception_classifier.
    # Tests updated to FailureCategory values (behaviour change notes below):
    # - "db_constraint" → FailureCategory.DB_CONSTRAINT (same semantics)
    # - "unknown" → FailureCategory.INTERNAL_ERROR (KeyError was previously
    #   "unknown"; now bucketed as INTERNAL_ERROR — retriable, same effect)
    def test_integrity_error(self) -> None:
        import psycopg

        from app.services.sync_orchestrator.exception_classifier import classify_exception
        from app.services.sync_orchestrator.layer_types import FailureCategory

        exc = psycopg.errors.IntegrityError("fk violation")
        assert classify_exception(exc) is FailureCategory.DB_CONSTRAINT

    def test_unknown_fallback(self) -> None:
        from app.services.sync_orchestrator.exception_classifier import classify_exception
        from app.services.sync_orchestrator.layer_types import FailureCategory

        exc = KeyError("nope")
        assert classify_exception(exc) is FailureCategory.INTERNAL_ERROR

    def test_master_key_not_loaded_maps_to_master_key_missing(self) -> None:
        # #643 — distinct category so the operator-actionable banner
        # ("restart the backend / open /recover") fires instead of the
        # opaque "Unclassified error" the path used to hit.
        from app.security.secrets_crypto import MasterKeyNotLoadedError
        from app.services.sync_orchestrator.exception_classifier import classify_exception
        from app.services.sync_orchestrator.layer_types import FailureCategory

        exc = MasterKeyNotLoadedError("broker-encryption key is not loaded")
        assert classify_exception(exc) is FailureCategory.MASTER_KEY_MISSING

    def test_master_key_missing_remedy_present(self) -> None:
        # The classifier mapping is useless without the REMEDIES entry.
        # Pin the wiring so a future contributor can't drop one without
        # the other.
        from app.services.sync_orchestrator.layer_types import REMEDIES, FailureCategory

        assert FailureCategory.MASTER_KEY_MISSING in REMEDIES
        remedy = REMEDIES[FailureCategory.MASTER_KEY_MISSING]
        # operator_fix is required — this category exists specifically
        # because there's an operator action to take.
        assert remedy.operator_fix is not None
        # NOT self_heal — backoff retry won't recover; the key has to
        # come back via either the persisted root secret or the
        # operator-driven recovery flow.
        assert remedy.self_heal is False


# `set_executor` and `submit_sync` were deleted in #719 — the API
# publishes via dispatcher.publish_sync_request and the jobs-process
# listener invokes `run_sync` on its own executor. The
# `TestSetExecutor` class that lived here previously tested the
# in-process executor wiring; obsoleted by the cross-process design.


class TestPR4aGateCheckConnReuse:
    """#1472 PR4a — `_run_layers_loop` reuses ONE run-scoped autocommit
    connection for every per-layer read gate-check instead of opening a
    fresh raw ``psycopg.connect`` per check per layer (the cadence-boundary
    raw-connect herd that was the #1472 RCA)."""

    class _FakeConn:
        """Minimal psycopg-conn stand-in for the no-DB holder tests."""

        def __init__(self) -> None:
            self.closed = False
            self.broken = False
            self.autocommit = True
            self.close_calls = 0

        def close(self) -> None:
            self.closed = True
            self.close_calls += 1

        def __enter__(self) -> TestPR4aGateCheckConnReuse._FakeConn:
            return self

        def __exit__(self, *exc: object) -> bool:
            self.close()
            return False

    def _patch_connect(self, monkeypatch) -> list[TestPR4aGateCheckConnReuse._FakeConn]:
        created: list[TestPR4aGateCheckConnReuse._FakeConn] = []

        def fake_connect(*_a: object, **_k: object) -> TestPR4aGateCheckConnReuse._FakeConn:
            conn = TestPR4aGateCheckConnReuse._FakeConn()
            created.append(conn)
            return conn

        monkeypatch.setattr(executor.psycopg, "connect", fake_connect)
        return created

    # -- holder unit behaviour -------------------------------------------

    def test_holder_single_connect_when_healthy(self, monkeypatch) -> None:
        created = self._patch_connect(monkeypatch)
        holder = executor._GateCheckConnection()
        a, b, c = holder.get(), holder.get(), holder.get()
        assert a is b is c
        assert len(created) == 1  # reused, not reopened
        holder.close()
        assert created[0].close_calls == 1
        assert holder._conn is None

    def test_holder_reconnects_when_closed(self, monkeypatch) -> None:
        created = self._patch_connect(monkeypatch)
        holder = executor._GateCheckConnection()
        first = holder.get()
        created[0].closed = True  # _FakeConn attr (assignable, unlike the real property)
        second = holder.get()
        assert second is not first
        assert len(created) == 2  # reopened after the conn went closed
        holder.close()

    def test_holder_reconnects_when_broken(self, monkeypatch) -> None:
        created = self._patch_connect(monkeypatch)
        holder = executor._GateCheckConnection()
        first = holder.get()
        created[0].broken = True  # mid-walk EOF / PG restart
        second = holder.get()
        assert second is not first
        assert len(created) == 2  # → reconnect on next gate-check
        holder.close()

    def test_gate_check_conn_none_opens_fresh_each_call(self, monkeypatch) -> None:
        created = self._patch_connect(monkeypatch)
        with executor._gate_check_conn(None) as c1:
            assert c1.autocommit is True
        with executor._gate_check_conn(None) as c2:
            pass
        assert c1 is not c2
        assert len(created) == 2
        # owned conns are closed on context exit (standalone / test callers).
        assert created[0].close_calls == 1
        assert created[1].close_calls == 1

    def test_gate_check_conn_holder_reused_not_closed(self, monkeypatch) -> None:
        created = self._patch_connect(monkeypatch)
        holder = executor._GateCheckConnection()
        with executor._gate_check_conn(holder) as c1:
            pass
        with executor._gate_check_conn(holder) as c2:
            pass
        assert c1 is c2  # same physical conn across both borrows
        assert created[0].close_calls == 0  # borrow does NOT close; the walk owns it
        holder.close()
        assert created[0].close_calls == 1

    # -- loop threads ONE shared holder to every gate-check --------------

    def test_run_layers_loop_threads_one_shared_gate_conn(self, monkeypatch) -> None:
        captured: dict[str, list[object]] = {"cancel": [], "upstream": [], "cred": [], "init": []}

        def cap_cancel(_sid: object, *, gate_conn: object = None) -> None:
            captured["cancel"].append(gate_conn)

        def cap_upstream(_lp: object, _oc: object, *, gate_conn: object = None) -> dict[str, object]:
            captured["upstream"].append(gate_conn)
            return {}

        def cap_cred(_lp: object, *, gate_conn: object = None) -> None:
            captured["cred"].append(gate_conn)
            return None

        def cap_init(_lp: object, *, gate_conn: object = None) -> str:
            captured["init"].append(gate_conn)
            return "init-skip: test"  # short-circuit before the adapter runs

        monkeypatch.setattr(executor, "_check_cancel_signal", cap_cancel)
        monkeypatch.setattr(executor, "_build_upstream_outcomes", cap_upstream)
        monkeypatch.setattr(executor, "_credential_health_blocks", cap_cred)
        monkeypatch.setattr(executor, "_layer_initialization_blocks", cap_init)
        monkeypatch.setattr(executor, "_record_layer_skipped", MagicMock())

        closes: list[object] = []
        real_close = executor._GateCheckConnection.close

        def spy_close(self: object) -> None:
            closes.append(self)
            real_close(self)  # type: ignore[arg-type]

        monkeypatch.setattr(executor._GateCheckConnection, "close", spy_close)

        plan = ExecutionPlan(
            layers_to_refresh=(_lp("a", ("a",)), _lp("b", ("b",))),
            layers_skipped=(),
            estimated_duration=None,
        )
        executor._run_layers_loop(sync_run_id=1, plan=plan, outcomes={})

        all_conns = captured["cancel"] + captured["upstream"] + captured["cred"] + captured["init"]
        assert all_conns, "gate-checks should have received the run-scoped gate_conn"
        holder = all_conns[0]
        assert isinstance(holder, executor._GateCheckConnection)
        # EVERY gate-check on BOTH layers got the SAME holder instance.
        assert all(c is holder for c in all_conns)
        # 2 layers + 1 post-loop cancel checkpoint.
        assert len(captured["cancel"]) == 3
        assert len(captured["cred"]) == 2
        assert len(captured["init"]) == 2
        # Closed exactly once, in the finally.
        assert closes == [holder]

    # -- real-DB: autocommit shared conn is no-poison + reused -----------

    def test_holder_reuses_one_real_connection(self, settings_use_test_db: str) -> None:
        holder = executor._GateCheckConnection()
        try:
            with executor._gate_check_conn(holder) as c1:
                assert c1.execute("SELECT 1").fetchone() == (1,)
            with executor._gate_check_conn(holder) as c2:
                assert c2.execute("SELECT 1").fetchone() == (1,)
            assert c1 is c2  # one physical connection reused across borrows
            assert c1.autocommit is True
        finally:
            holder.close()

    def test_holder_survives_swallowed_sql_error(self, settings_use_test_db: str) -> None:
        """A gate-check that swallows a SQL error must not poison the next
        check on the shared autocommit conn (prevention-log §'conn.rollback()
        needed after caught exception on a shared connection' is moot under
        autocommit — verified empirically)."""
        holder = executor._GateCheckConnection()
        try:
            with executor._gate_check_conn(holder) as c:
                try:
                    c.execute("SELECT * FROM _pr4a_definitely_no_such_table")
                except Exception:
                    pass  # gate-check swallows infra errors (fail-open / fail-closed)
            with executor._gate_check_conn(holder) as c2:
                assert c2.execute("SELECT 1").fetchone() == (1,)  # not poisoned
            assert c is c2  # same conn, still usable
        finally:
            holder.close()
