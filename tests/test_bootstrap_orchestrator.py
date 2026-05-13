"""Tests for app.services.bootstrap_orchestrator.

Covers:

* Stage catalogue cardinality + lane composition.
* Phase A → Phase B → Phase C transitions on the happy path.
* Mid-SEC-lane stage failure does not abort other stages.
* A1 (init) failure prevents Phase B and finalises ``partial_error``.
* Retry-failed pre-check skips ``success`` stages.
* Unknown ``job_name`` recorded as stage error rather than crashing.

The lane runners spawn `threading.Thread`. Tests substitute the
``_INVOKERS`` map with deterministic in-process fakes via
``monkeypatch`` so the orchestrator runs end-to-end without hitting
real provider stacks.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

import psycopg
import pytest

from app.services.bootstrap_orchestrator import (
    _BOOTSTRAP_STAGE_SPECS,
    _CAPABILITY_PROVIDERS,
    _STAGE_PROVIDES,
    _STAGE_REQUIRES_CAPS,
    JOB_BOOTSTRAP_ORCHESTRATOR,
    JOB_DAILY_CIK_REFRESH,
    JOB_DAILY_FINANCIAL_FACTS,
    _run_one_stage,
    _satisfied_capabilities,
    _should_run,
    get_bootstrap_stage_specs,
    run_bootstrap_orchestrator,
)
from app.services.bootstrap_state import (
    read_latest_run_with_stages,
    read_state,
    start_run,
)
from app.workers.scheduler import (
    JOB_FILINGS_HISTORY_SEED,
    JOB_SEC_FIRST_INSTALL_DRAIN,
)


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    """Bring bootstrap_state back to the canonical 'pending' state."""
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status            = 'pending',
               last_run_id       = NULL,
               last_completed_at = NULL
         WHERE id = 1
        """
    )
    conn.commit()


def _bind_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> str:
    """Point ``app.config.settings.database_url`` at the worker's
    private test DB so the orchestrator's ``psycopg.connect`` calls
    use the same DB the fixture truncates.

    Without this, the orchestrator would connect to the dev DB and
    write live state — the test_db_isolation feedback memory
    explicitly forbids that.
    """
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", url)
    return url


def _register_synthetic_jobs(monkeypatch: pytest.MonkeyPatch, mapping: dict[str, str]) -> None:
    """Add synthetic ``job_name -> Lane`` entries to the source-lock
    registry so ``JobLock(database_url, job_name)`` resolves without
    raising ``KeyError`` for fixture-only job names.

    PR1a #1064 made ``JobLock`` eagerly resolve job_name -> source via
    ``app.jobs.sources.source_for``; tests that construct synthetic
    stage specs (e.g. ``alpha_job``, ``bravo_job``) must therefore
    register the name. The registry is a process-wide dict cached
    behind ``get_job_name_to_source``; monkeypatch.setitem reverses
    each insert at teardown.
    """
    from app.jobs.sources import get_job_name_to_source

    registry = get_job_name_to_source()
    for name, lane in mapping.items():
        monkeypatch.setitem(registry, name, lane)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Catalogue invariants
# ---------------------------------------------------------------------------


def test_stage_catalogue_has_twenty_four_stages() -> None:
    """Catalogue size pinned to surface adds/removes in code review.

    24 = 1 init + 1 etoro + 4 sec_rate (B-stages) + 1 sec_bulk_download
    + 5 db (Phase C ingesters) + 1 sec_rate (C1.b) + 7 sec_rate (legacy
    chain) + 2 sec_rate (legacy 13F/N-PORT recent sweeps) + 2 db (E-stages).
    """
    specs = get_bootstrap_stage_specs()
    assert len(specs) == 24


def test_stage_catalogue_lane_composition() -> None:
    specs = get_bootstrap_stage_specs()
    by_lane: dict[str, int] = {}
    for spec in specs:
        by_lane[spec.lane] = by_lane.get(spec.lane, 0) + 1
    # 1 + 1 + (4 + 1 + 7 + 2) + 1 + (5 + 2) = 24
    assert by_lane == {
        "init": 1,
        "etoro": 1,
        "sec_rate": 14,
        "sec_bulk_download": 1,
        "db": 7,
    }


def test_stage_orders_are_unique_and_contiguous() -> None:
    specs = get_bootstrap_stage_specs()
    orders = sorted(spec.stage_order for spec in specs)
    assert orders == list(range(1, len(specs) + 1))


def test_critical_constants_exposed() -> None:
    # Tests + frontend will import these; keep them stable.
    assert JOB_BOOTSTRAP_ORCHESTRATOR == "bootstrap_orchestrator"
    # PR1c #1064 — bespoke wrapper job names retired; the promoted
    # scheduler-side constants now own these strings.
    assert JOB_FILINGS_HISTORY_SEED == "filings_history_seed"
    assert JOB_SEC_FIRST_INSTALL_DRAIN == "sec_first_install_drain"
    assert JOB_DAILY_CIK_REFRESH == "daily_cik_refresh"
    assert JOB_DAILY_FINANCIAL_FACTS == "daily_financial_facts"


# ---------------------------------------------------------------------------
# Pre-check semantics
# ---------------------------------------------------------------------------


def test_should_run_skips_success() -> None:
    assert _should_run("pending") is True
    assert _should_run("running") is True
    assert _should_run("error") is True
    assert _should_run("skipped") is True
    assert _should_run("success") is False


# ---------------------------------------------------------------------------
# _run_one_stage — direct test
# ---------------------------------------------------------------------------


def test_run_one_stage_records_success(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)
    _register_synthetic_jobs(monkeypatch, {"alpha_job": "init"})
    from app.services.bootstrap_state import StageSpec

    specs = (StageSpec(stage_key="alpha", stage_order=1, lane="init", job_name="alpha_job"),)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    calls: list[str] = []

    def alpha_invoker(_params: object = None) -> None:
        calls.append("alpha")

    outcome = _run_one_stage(
        run_id=run_id,
        stage_key="alpha",
        job_name="alpha_job",
        invoker=alpha_invoker,
        database_url=test_db_url,
    )
    assert outcome.success is True
    assert calls == ["alpha"]

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.stages[0].status == "success"


def test_run_one_stage_records_error_on_invoker_exception(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)
    _register_synthetic_jobs(monkeypatch, {"bravo_job": "init"})
    from app.services.bootstrap_state import StageSpec

    specs = (StageSpec(stage_key="bravo", stage_order=1, lane="init", job_name="bravo_job"),)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    def bravo_invoker(_params: object = None) -> None:
        raise RuntimeError("kaboom")

    outcome = _run_one_stage(
        run_id=run_id,
        stage_key="bravo",
        job_name="bravo_job",
        invoker=bravo_invoker,
        database_url=test_db_url,
    )
    assert outcome.success is False
    assert outcome.error is not None and "kaboom" in outcome.error

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.stages[0].status == "error"
    assert snap.stages[0].last_error is not None
    assert "kaboom" in snap.stages[0].last_error


# ---------------------------------------------------------------------------
# End-to-end run_bootstrap_orchestrator with stubbed invokers
# ---------------------------------------------------------------------------


def _patch_invokers_with_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    failing_jobs: set[str] | None = None,
) -> dict[str, list[str]]:
    """Replace every _INVOKERS entry the orchestrator might dispatch
    with a deterministic in-process fake. Returns a calls dict so
    tests can assert which invokers fired.

    The fakes do not touch any real provider or DB beyond what the
    orchestrator service itself does (mark stage running / success /
    error). This keeps the test runtime well under one second per
    case.
    """
    calls: dict[str, list[str]] = {"order": []}
    failing = failing_jobs or set()

    def _make_fake(name: str) -> Callable[..., None]:
        # PR1b-2 (#1064) widened JobInvoker to ``(Mapping) -> None``;
        # bootstrap dispatch now calls invoker({}). Accept-and-ignore
        # the params kwarg so this fake satisfies both the legacy
        # zero-arg and the post-PR1b-2 signature without test churn.
        def _fake(_params: object = None) -> None:
            calls["order"].append(name)
            if name in failing:
                raise RuntimeError(f"forced {name} failure")

        return _fake

    from app.jobs import runtime as runtime_module

    fake_invokers = {spec.job_name: _make_fake(spec.job_name) for spec in get_bootstrap_stage_specs()}
    monkeypatch.setattr(runtime_module, "_INVOKERS", fake_invokers)
    return calls


def test_orchestrator_happy_path_completes(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    calls = _patch_invokers_with_fakes(monkeypatch)

    run_id = start_run(
        ebull_test_conn,
        operator_id=None,
        stage_specs=get_bootstrap_stage_specs(),
    )
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.run_id == run_id
    statuses = {stage.stage_key: stage.status for stage in snap.stages}
    assert all(s == "success" for s in statuses.values()), statuses

    state = read_state(ebull_test_conn)
    assert state.status == "complete"

    # All 24 invokers called.
    assert len(calls["order"]) == 24
    # Phase A's universe sync was first.
    assert calls["order"][0] == "nightly_universe_sync"


def test_orchestrator_init_failure_skips_phase_b(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    calls = _patch_invokers_with_fakes(monkeypatch, failing_jobs={"nightly_universe_sync"})

    start_run(ebull_test_conn, operator_id=None, stage_specs=get_bootstrap_stage_specs())
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    statuses = {stage.stage_key: stage.status for stage in snap.stages}
    assert statuses["universe_sync"] == "error"
    # New dispatcher (#1020): downstream stages with `requires` on
    # the failed stage propagate to `blocked` instead of staying
    # pending. Distinguishes upstream-failure from "operator hasn't
    # triggered yet".
    for key, status in statuses.items():
        if key == "universe_sync":
            continue
        assert status == "blocked", (key, status)

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"
    assert calls["order"] == ["nightly_universe_sync"]


def test_orchestrator_mid_sec_lane_failure_continues_lane_and_etoro(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    calls = _patch_invokers_with_fakes(
        monkeypatch,
        failing_jobs={"sec_def14a_bootstrap"},  # mid-SEC-lane (S9)
    )

    start_run(ebull_test_conn, operator_id=None, stage_specs=get_bootstrap_stage_specs())
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    statuses = {stage.stage_key: stage.status for stage in snap.stages}

    # eToro lane completed regardless.
    assert statuses["candle_refresh"] == "success"
    # Init succeeded.
    assert statuses["universe_sync"] == "success"
    # Failed stage marked error.
    assert statuses["sec_def14a_bootstrap"] == "error"
    # Subsequent SEC-lane stages still ran (continue past errors per spec §Goal 4).
    assert statuses["fundamentals_sync"] == "success"

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"

    # The failed invoker still appears in the call log because the
    # invoker raised; we record that as one call.
    assert "sec_def14a_bootstrap" in calls["order"]


def test_orchestrator_skips_stages_already_success(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry-failed semantics: stages already in 'success' must not
    be re-dispatched. Simulate by manually marking some stages
    success before invoking the orchestrator.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    calls = _patch_invokers_with_fakes(monkeypatch)

    run_id = start_run(
        ebull_test_conn,
        operator_id=None,
        stage_specs=get_bootstrap_stage_specs(),
    )
    ebull_test_conn.commit()

    # Pre-mark a few SEC stages as success — orchestrator must skip them.
    skip_keys = {
        "cusip_universe_backfill",
        "sec_13f_filer_directory_sync",
        "sec_nport_filer_directory_sync",
    }
    for key in skip_keys:
        ebull_test_conn.execute(
            """
            UPDATE bootstrap_stages
               SET status = 'success', completed_at = now()
             WHERE bootstrap_run_id = %s AND stage_key = %s
            """,
            (run_id, key),
        )
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    # The pre-marked stages were not re-invoked.
    for key in skip_keys:
        assert key not in calls["order"], f"{key} should have been skipped"
    # A1 + the rest of SEC-lane stages still ran.
    assert "nightly_universe_sync" in calls["order"]
    assert "fundamentals_sync" in calls["order"]


def test_orchestrator_unknown_job_name_recorded_as_error(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If a stage's job_name is missing from _INVOKERS, the orchestrator
    must mark that stage error rather than crash.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)

    from app.jobs import runtime as runtime_module
    from app.services.bootstrap_state import StageSpec

    # Empty registry on purpose — nothing is invokable.
    monkeypatch.setattr(runtime_module, "_INVOKERS", {})

    specs = (StageSpec(stage_key="orphan", stage_order=1, lane="init", job_name="nonexistent_job"),)
    start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    # Should not raise.
    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.stages[0].status == "error"
    assert snap.stages[0].last_error is not None
    assert "unknown job_name" in snap.stages[0].last_error

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"


# ---------------------------------------------------------------------------
# Concurrency probe — orchestrator threading does not deadlock
# ---------------------------------------------------------------------------


def test_orchestrator_returns_within_reasonable_time(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Joins both lane threads; if either thread hangs the test would
    deadlock pytest. Bound the run with a simple timer.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    _patch_invokers_with_fakes(monkeypatch)

    start_run(ebull_test_conn, operator_id=None, stage_specs=get_bootstrap_stage_specs())
    ebull_test_conn.commit()

    done = threading.Event()

    def _run() -> None:
        run_bootstrap_orchestrator()
        done.set()

    thread = threading.Thread(target=_run)
    thread.start()
    thread.join(timeout=30.0)
    assert done.is_set(), "run_bootstrap_orchestrator() did not return within 30s"


# ---------------------------------------------------------------------------
# Capability layer (#1138 Task A) — fallback shapes + catalogue invariants
# ---------------------------------------------------------------------------


def test_every_required_capability_has_a_provider() -> None:
    """Every cap referenced in `_STAGE_REQUIRES_CAPS` must be provided
    by at least one stage. Catches typo-style drift in the requires
    table before the dispatcher tries to evaluate a never-satisfiable
    requirement at runtime.
    """
    referenced: set[str] = set()
    for req in _STAGE_REQUIRES_CAPS.values():
        for cap in req.all_of:
            referenced.add(cap)
        for group in req.any_of:
            for cap in group:
                referenced.add(cap)
    missing = [c for c in referenced if not _CAPABILITY_PROVIDERS.get(c)]  # type: ignore[arg-type]
    assert not missing, f"capabilities with no provider: {missing}"


def test_every_stage_appears_in_requires_caps() -> None:
    """Every stage in `_BOOTSTRAP_STAGE_SPECS` must have a
    `_STAGE_REQUIRES_CAPS` entry (even if `CapRequirement()`). Catches
    missing entries — a stage absent from the requires map would fall
    back to the no-deps default, silently bypassing intended gates.
    """
    spec_keys = {spec.stage_key for spec in _BOOTSTRAP_STAGE_SPECS}
    requires_keys = set(_STAGE_REQUIRES_CAPS.keys())
    missing = spec_keys - requires_keys
    assert not missing, f"stages without _STAGE_REQUIRES_CAPS entry: {missing}"


def test_partial_bulk_failure_legacy_recovers(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1138 §8 test 1 — `sec_bulk_download` errors (NOT skips). Every
    legacy ownership stage succeeds. Per-family ownership caps are
    satisfied by their legacy providers, so
    `ownership_observations_backfill` reaches `success`. The 5 Phase C
    bulk ingesters cascade to `blocked` (error-classified) because
    `bulk_archives_ready` is error-dead.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    calls = _patch_invokers_with_fakes(
        monkeypatch,
        failing_jobs={"sec_bulk_download"},
    )

    start_run(ebull_test_conn, operator_id=None, stage_specs=get_bootstrap_stage_specs())
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    statuses = {stage.stage_key: stage.status for stage in snap.stages}

    # S7 error.
    assert statuses["sec_bulk_download"] == "error"
    # 5 Phase C bulk ingesters cascade-blocked (error-dead bulk_archives_ready).
    phase_c_bulk = {
        "sec_submissions_ingest",
        "sec_companyfacts_ingest",
        "sec_13f_ingest_from_dataset",
        "sec_insider_ingest_from_dataset",
        "sec_nport_ingest_from_dataset",
    }
    for key in phase_c_bulk:
        assert statuses[key] == "blocked", f"{key} expected blocked, got {statuses[key]}"
    # Phase C invokers NOT called (cascade-block transitions directly).
    for key in phase_c_bulk:
        assert key not in calls["order"], f"{key} should not have been invoked"
    # Legacy ownership stages succeeded → per-family caps satisfied.
    assert statuses["ownership_observations_backfill"] == "success"
    # Fundamentals also blocks because S9 (its sole provider) is blocked.
    assert statuses["fundamentals_sync"] == "blocked"

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"


def test_intentional_slow_connection_skip_cascade(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1138 §8 test 2 — `sec_bulk_download` raises
    `BootstrapPhaseSkipped` (the new fallback path). Phase C cascades
    to `skipped` per §6.3; legacy chain succeeds; downstream reaches
    `success` via legacy per-family caps. Walker S13 runs to success
    because legacy drain S15 provides `filing_events_seeded`.
    """
    from app.services.bootstrap_preconditions import BootstrapPhaseSkipped

    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)

    calls: dict[str, list[str]] = {"order": []}

    def _make_fake(name: str) -> Callable[..., None]:
        def _fake(_params: object = None) -> None:
            calls["order"].append(name)
            if name == "sec_bulk_download":
                raise BootstrapPhaseSkipped("slow connection; fallback path")

        return _fake

    from app.jobs import runtime as runtime_module

    fake_invokers = {spec.job_name: _make_fake(spec.job_name) for spec in get_bootstrap_stage_specs()}
    monkeypatch.setattr(runtime_module, "_INVOKERS", fake_invokers)

    start_run(ebull_test_conn, operator_id=None, stage_specs=get_bootstrap_stage_specs())
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    statuses = {stage.stage_key: stage.status for stage in snap.stages}

    # S7 transitioned to `skipped` via BootstrapPhaseSkipped.
    assert statuses["sec_bulk_download"] == "skipped"

    # 5 Phase C bulk ingesters cascade to skipped without invocation.
    phase_c_bulk = {
        "sec_submissions_ingest",
        "sec_companyfacts_ingest",
        "sec_13f_ingest_from_dataset",
        "sec_insider_ingest_from_dataset",
        "sec_nport_ingest_from_dataset",
    }
    for key in phase_c_bulk:
        assert statuses[key] == "skipped", f"{key} expected skipped, got {statuses[key]}"
        assert key not in calls["order"], f"{key} should not have been invoked under cascade"

    # S24 fundamentals_sync cascades skipped (sole provider S9 skipped).
    assert statuses["fundamentals_sync"] == "skipped"

    # Legacy chain runs.
    assert statuses["filings_history_seed"] == "success"
    assert statuses["sec_first_install_drain"] == "success"
    # Walker runs to success via filing_events_seeded from legacy drain.
    assert statuses["sec_submissions_files_walk"] == "success"
    # Typed parsers run via submissions_secondary_pages_walked from drain.
    assert statuses["sec_def14a_bootstrap"] == "success"
    assert statuses["sec_business_summary_bootstrap"] == "success"
    assert statuses["sec_8k_events_ingest"] == "success"
    # Ownership backfill reaches success via legacy per-family providers.
    assert statuses["ownership_observations_backfill"] == "success"

    # Caps invariant — skipped S7 does NOT advertise bulk_archives_ready.
    caps = _satisfied_capabilities(statuses)
    assert "bulk_archives_ready" not in caps

    state = read_state(ebull_test_conn)
    # All-success-or-skip → complete.
    assert state.status == "complete"


def test_both_ownership_paths_fail_blocks_final_stage(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1138 §8 test 3 — bulk AND legacy ownership stages all error,
    so every per-family ownership cap is error-dead.
    `ownership_observations_backfill` transitions to `blocked` with
    a structured "missing capability" reason naming at least one
    per-family cap.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)

    # Fail S7 AND every legacy ownership stage. Bulk Phase C is then
    # error-blocked from S7 (error-dead bulk_archives_ready); legacy
    # ownership stages fail directly.
    failing = {
        "sec_bulk_download",
        "sec_insider_transactions_backfill",
        "sec_form3_ingest",
        "sec_13f_quarterly_sweep",  # underlying job_name for S21
        "sec_n_port_ingest",
    }
    _patch_invokers_with_fakes(monkeypatch, failing_jobs=failing)

    start_run(ebull_test_conn, operator_id=None, stage_specs=get_bootstrap_stage_specs())
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    statuses = {stage.stage_key: stage.status for stage in snap.stages}
    last_errors = {stage.stage_key: stage.last_error for stage in snap.stages}

    assert statuses["ownership_observations_backfill"] == "blocked"
    reason = last_errors["ownership_observations_backfill"] or ""
    assert "missing capability" in reason
    # The reason should name at least one per-family ownership cap.
    family_caps = (
        "insider_inputs_seeded",
        "form3_inputs_seeded",
        "institutional_inputs_seeded",
        "nport_inputs_seeded",
    )
    assert any(c in reason for c in family_caps), f"expected per-family cap in reason, got: {reason!r}"

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"


def test_phase_c_provides_are_per_family() -> None:
    """Sanity: bulk insider ingester provides BOTH insider+form3 caps;
    the per-family split keeps bulk-vs-legacy alternatives expressible
    at the provider side (no consumer-side any_of needed).
    """
    bulk_insider = _STAGE_PROVIDES["sec_insider_ingest_from_dataset"]
    assert "insider_inputs_seeded" in bulk_insider
    assert "form3_inputs_seeded" in bulk_insider
    # Legacy insider txns covers Form 4 only.
    legacy_insider = _STAGE_PROVIDES["sec_insider_transactions_backfill"]
    assert legacy_insider == ("insider_inputs_seeded",)
    # Legacy Form 3 covers Form 3 only.
    legacy_form3 = _STAGE_PROVIDES["sec_form3_ingest"]
    assert legacy_form3 == ("form3_inputs_seeded",)


def test_cascade_recompute_on_non_topological_pending_order(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex pre-push WARNING regression — when a downstream stage is
    evaluated earlier in pending_keys than its upstream provider, and
    the upstream then cascade-skips later in the same inner loop, the
    dispatcher must recompute caps on the next outer iteration rather
    than dropping the downstream into the deadlock "abandoned" branch.

    Builds a synthetic 3-stage scenario via ``_phase_batched_dispatch``
    directly with reverse-topological ``runnable`` order: downstream
    first, upstream last. Upstream raises ``BootstrapPhaseSkipped``;
    downstream must end in ``skipped`` (cascade), not ``blocked``
    ("abandoned").
    """
    from app.services.bootstrap_orchestrator import (
        CapRequirement,
        _phase_batched_dispatch,
        _RunnableStage,
    )
    from app.services.bootstrap_preconditions import BootstrapPhaseSkipped
    from app.services.bootstrap_state import StageSpec

    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)
    _register_synthetic_jobs(
        monkeypatch,
        {"alpha_job": "init", "bravo_job": "init"},
    )

    specs = (
        # Downstream first in stage_order — non-topological w.r.t.
        # the cap dependency below.
        StageSpec(stage_key="downstream", stage_order=1, lane="init", job_name="bravo_job"),
        StageSpec(stage_key="upstream", stage_order=2, lane="init", job_name="alpha_job"),
    )
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    # Upstream raises BootstrapPhaseSkipped → cascades to skipped.
    # Downstream requires the cap upstream would have provided.
    def upstream_invoker(_params: object = None) -> None:
        raise BootstrapPhaseSkipped("simulated bypass")

    def downstream_invoker(_params: object = None) -> None:  # pragma: no cover
        raise AssertionError("downstream must not invoke when upstream skips")

    runnable = [
        # Order matters for the regression: downstream BEFORE upstream
        # in the runnable list → pending_keys iteration sees downstream
        # first.
        _RunnableStage(
            stage_key="downstream",
            job_name="bravo_job",
            lane="init",
            invoker=downstream_invoker,
            requires=CapRequirement(all_of=("synthetic_upstream_done",)),  # type: ignore[arg-type]
        ),
        _RunnableStage(
            stage_key="upstream",
            job_name="alpha_job",
            lane="init",
            invoker=upstream_invoker,
            requires=CapRequirement(),
        ),
    ]

    statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
        provides_map={"upstream": ("synthetic_upstream_done",)},  # type: ignore[dict-item]
    )

    assert cancelled is False
    # Upstream completed via BootstrapPhaseSkipped → skipped.
    assert statuses["upstream"] == "skipped"
    # Downstream cascaded to skipped (the bug would have left it
    # blocked/abandoned). Cap is skip-only-dead because upstream
    # skipped without an explicit provides_on_skip entry.
    assert statuses["downstream"] == "skipped"

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    downstream_row = next(s for s in snap.stages if s.stage_key == "downstream")
    assert downstream_row.last_error is not None
    assert "cascaded skip" in downstream_row.last_error
