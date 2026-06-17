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
    _STAGE_LANE_OVERRIDES,
    _STAGE_PROVIDES,
    _STAGE_PROVIDES_ON_SKIP,
    _STAGE_REQUIRES_CAPS,
    JOB_BOOTSTRAP_ORCHESTRATOR,
    JOB_DAILY_CIK_REFRESH,
    JOB_DAILY_FINANCIAL_FACTS,
    _run_one_stage,
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


def test_stage_catalogue_has_twenty_one_stages() -> None:
    """Catalogue size pinned to surface adds/removes in code review.

    #1413 (bulk-only bootstrap) dropped 8 per-CIK HTTP stages
    (S14 submissions_files_walk, S15 filings_history_seed, S17 def14a,
    S19 insider_transactions_backfill, S20 form3, S22 13f_recent_sweep,
    S23 n_port_ingest, S27 sec_n_csr_bootstrap_drain) → 27 - 8 = 19.
    #1415 (P3) added the S15-slot master.idx recent-window gap-close
    (filing-metadata-scoped, with the per-source watermark guard) → 20.
    #1419 (P4) added the terminal bootstrap_validation stage → 21.

    21 = 1 init + 1 etoro + 9 sec_rate + 1 sec_bulk_download + 7 db
    + 1 db_fundamentals_raw + 1 openfigi.
    """
    specs = get_bootstrap_stage_specs()
    assert len(specs) == 22


def test_stage_catalogue_lane_composition() -> None:
    specs = get_bootstrap_stage_specs()
    by_lane: dict[str, int] = {}
    for spec in specs:
        by_lane[spec.lane] = by_lane.get(spec.lane, 0) + 1
    # #1413 — sec_rate: 16 − 8 per-CIK HTTP stages removed (incl. S27 N-CSR
    # drain) = 8; #1415 + 1 master.idx gap-close = 9. #1419 (P4) + 1 db stage
    # (bootstrap_validation) → db = 7. #788 + 1 db stage (sec_fsds_class_shares_ingest)
    # → db = 8. Total 1 + 1 + 9 + 1 + 8 + 1 + 1 = 22.
    # ``db`` = 8 (5 bulk ingesters + sec_fsds_class_shares_ingest +
    # ownership_observations_backfill + bootstrap_validation);
    # ``db_fundamentals_raw`` = 1 (S25 fundamentals_sync);
    # ``openfigi`` = 1 (S13 cusip_resolver_post_bulk_sweep).
    assert by_lane == {
        "init": 1,
        "etoro": 1,
        "sec_rate": 9,
        "sec_bulk_download": 1,
        "db": 8,
        "db_fundamentals_raw": 1,
        "openfigi": 1,
    }


def test_stage_orders_are_unique_and_ascending() -> None:
    # #1413 — stage_orders are no longer contiguous: dropping the 7 per-CIK
    # stages leaves gaps (…13, 16, 18, 21, 24…). Gaps are intentional — they
    # preserve operator traceability (stage 22 stays "the 13F sweep") and
    # visibly signal removed stages. The invariant is unique + strictly
    # ascending (catches dup / mis-ordered specs), not contiguity.
    specs = get_bootstrap_stage_specs()
    orders = [spec.stage_order for spec in specs]
    assert orders == sorted(orders), "stage_order must be ascending in catalogue order"
    assert len(set(orders)) == len(orders), "stage_order values must be unique"


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
    phase_skip_jobs: set[str] | None = None,
    rows_by_job: dict[str, int] | None = None,
) -> dict[str, list[str]]:
    """Replace every _INVOKERS entry the orchestrator might dispatch
    with a deterministic in-process fake. Returns a calls dict so
    tests can assert which invokers fired.

    The fakes do not touch any real provider or DB beyond what the
    orchestrator service itself does (mark stage running / success /
    error). This keeps the test runtime well under one second per
    case.

    #1140 Task C — each fake also inserts a ``job_runs`` row with
    ``row_count = rows_by_job.get(job_name, 1)`` so the orchestrator's
    ``_resolve_stage_rows`` source-3 fallback resolves to a real
    number. Without this every stage's ``rows_processed`` would be
    NULL and the strict-gate caps (per-family ownership +
    ``fundamentals_raw_seeded``) would block downstream consumers in
    every existing test. Tests that want to simulate "ran but wrote
    zero" pass ``rows_by_job={"some_job_name": 0}``.
    """
    import psycopg as _psycopg

    from app.config import settings as _app_settings
    from app.services.bootstrap_preconditions import BootstrapPhaseSkipped

    calls: dict[str, list[str]] = {"order": []}
    failing = failing_jobs or set()
    phase_skipping = phase_skip_jobs or set()
    rows = rows_by_job or {}

    def _make_fake(name: str) -> Callable[..., None]:
        # PR1b-2 (#1064) widened JobInvoker to ``(Mapping) -> None``;
        # bootstrap dispatch now calls invoker({}). Accept-and-ignore
        # the params kwarg so this fake satisfies both the legacy
        # zero-arg and the post-PR1b-2 signature without test churn.
        def _fake(_params: object = None) -> None:
            calls["order"].append(name)
            if name in failing:
                raise RuntimeError(f"forced {name} failure")
            if name in phase_skipping:
                raise BootstrapPhaseSkipped(f"forced {name} phase skip")
            # #1140 Task C — mirror _tracked_job's job_runs write so
            # _resolve_stage_rows source 3 finds a real row_count for
            # this stage. Capture started_at/finished_at as now() so
            # the row's run_id falls inside the JobLock window.
            row_count = rows.get(name, 1)
            with _psycopg.connect(_app_settings.database_url) as conn:
                conn.execute(
                    """
                    INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count)
                    VALUES (%s, now(), now(), 'success', %s)
                    """,
                    (name, row_count),
                )
                conn.commit()

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

    # #1413 dropped 8 per-CIK HTTP stages + #1415 added the master.idx
    # gap-close + #1419 (P4) added the terminal bootstrap_validation stage
    # → 21 invokers fire on the happy path.
    assert len(calls["order"]) == 22
    # Phase A's universe sync was first.
    assert calls["order"][0] == "nightly_universe_sync"
    # #1419 (P4) — validation is genuinely TERMINAL: it requires a cap from
    # every data/derivation stage (S24 ownership_current_refreshed, S25
    # fundamentals_synced, S26 class_id_mapping_ready), so it must run after all
    # three. stage_order does not order execution — the caps do.
    val_idx = calls["order"].index("bootstrap_validation")
    for prior_job in ("ownership_observations_backfill", "fundamentals_sync_bootstrap", "mf_directory_sync"):
        assert val_idx > calls["order"].index(prior_job), prior_job


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
        # #1413 — sec_def14a_bootstrap (old S17) dropped under bulk-only;
        # fail a surviving mid-sec_rate-lane stage instead.
        failing_jobs={"sec_business_summary_bootstrap"},  # mid-SEC-lane (S18)
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
    assert statuses["sec_business_summary_bootstrap"] == "error"
    # Subsequent SEC-lane stages still ran (continue past errors per spec §Goal 4).
    assert statuses["fundamentals_sync"] == "success"

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"

    # The failed invoker still appears in the call log because the
    # invoker raised; we record that as one call.
    assert "sec_business_summary_bootstrap" in calls["order"]


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
    # A1 + the rest of SEC-lane stages still ran. ``calls["order"]``
    # records JOB/invoker names, not stage keys — the S25 stage
    # ``fundamentals_sync`` dispatches the ``fundamentals_sync_bootstrap``
    # invoker (job_name divergence introduced by #1400 / #1397).
    assert "nightly_universe_sync" in calls["order"]
    assert "fundamentals_sync_bootstrap" in calls["order"]


def test_orchestrator_unknown_job_name_recorded_as_error(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registry-side gap: catalogue stage_key resolves to a job_name
    that is not in _INVOKERS. Orchestrator must mark error, not crash.

    #1136 Phase A.3 — uses a catalogue stage_key (``universe_sync``)
    so the dispatch hardening's "stage_key not in catalogue" branch
    is skipped; the registry-side guard is what fires when _INVOKERS
    is forced empty.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)

    from app.jobs import runtime as runtime_module
    from app.services.bootstrap_state import StageSpec

    # Empty registry on purpose — every catalogue lookup misses.
    monkeypatch.setattr(runtime_module, "_INVOKERS", {})

    # Catalogue stage_key + ``init`` lane so no upstream blocks
    # cascade-block this; isolated single-row scenario.
    specs = (StageSpec(stage_key="universe_sync", stage_order=1, lane="init", job_name="nightly_universe_sync"),)
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


def test_dispatch_resolves_job_name_from_spec_by_stage_key(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1136 Phase A.3 — dispatcher uses spec.job_name, not stage.job_name.

    Reproduces the run_id=3 S21 regression class. PR1c #1064 renamed
    the canonical 13F-sweep wrapper from ``bootstrap_sec_13f_recent_sweep``
    to ``sec_13f_quarterly_sweep`` but bootstrap_stages.job_name for
    an in-flight run was never updated. Pre-#1136 the dispatcher
    looked up ``_INVOKERS.get(stage.job_name)`` and errored
    "unknown job_name 'bootstrap_sec_13f_recent_sweep'". Post-#1136
    it resolves by stage_key from ``_BOOTSTRAP_STAGE_SPECS`` and
    dispatches the canonical name.

    The test asserts the effective name reaches both the invoker
    lookup AND param validation (Codex 1b §5) — the seeded stage
    carries the bootstrap-only ``source_label`` internal-key param
    which is allow-listed under the canonical job's metadata but
    has no entry under the stale name.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    calls = _patch_invokers_with_fakes(monkeypatch)

    # Seed a normal run.
    run_id = start_run(
        ebull_test_conn,
        operator_id=None,
        stage_specs=get_bootstrap_stage_specs(),
    )
    # #1413 — re-pointed from the dropped S22 ``sec_13f_recent_sweep`` to
    # the surviving stage_key≠job_name divergence: S25 stage_key
    # ``fundamentals_sync`` dispatches job_name ``fundamentals_sync_bootstrap``.
    # Rewrite its DB row to a stale wrapper name — what an in-flight retry
    # of a pre-rename run looks like.
    ebull_test_conn.execute(
        """
        UPDATE bootstrap_stages
           SET job_name = 'bootstrap_fundamentals_sync_stale'
         WHERE bootstrap_run_id = %s
           AND stage_key = 'fundamentals_sync'
        """,
        (run_id,),
    )
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    fs = next(s for s in snap.stages if s.stage_key == "fundamentals_sync")
    # Post-fix: stage runs to success under the canonical name even
    # though the DB row still carries the stale string.
    assert fs.status == "success", fs.last_error
    # The canonical invoker — not the stale wrapper — appears in the call log.
    assert "fundamentals_sync_bootstrap" in calls["order"]
    assert "bootstrap_fundamentals_sync_stale" not in calls["order"]
    # DB column stays as the audit snapshot — never silently rewritten.
    assert fs.job_name == "bootstrap_fundamentals_sync_stale"


def test_dispatch_unknown_stage_key_fails_closed(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1136 Phase A.3 — catalogue-trimmed stage_key fails closed.

    A pre-#719 install might still carry rows for a stage_key that
    has been removed from ``_BOOTSTRAP_STAGE_SPECS`` (e.g. legacy
    ``dividend_calendar``). Silently dispatching the DB row's
    ``stage.job_name`` would lose canonical params / CapRequirement /
    lane semantics. The dispatcher must mark error instead — and
    the error must actually land (Codex 1b §1: ``mark_stage_error``
    no-ops against pending rows; the path must run pending → running
    → error).
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    _patch_invokers_with_fakes(monkeypatch)

    from app.services.bootstrap_state import StageSpec

    # ``dividend_calendar`` is not in _BOOTSTRAP_STAGE_SPECS today (#260
    # retired the standalone cron). The stage_key existing in a DB row
    # is the exact pre-#719-install survivor case.
    specs = (StageSpec(stage_key="dividend_calendar", stage_order=1, lane="init", job_name="dividend_calendar_job"),)
    start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert len(snap.stages) == 1
    row = snap.stages[0]
    # Critical: row did NOT survive in pending. That's the silent-
    # no-op pitfall Codex 1b §1 flagged.
    assert row.status == "error", (
        f"unknown stage_key must reach terminal error; "
        f"got status={row.status!r} (silent no-op? mark_stage_running missing?)"
    )
    assert row.last_error is not None
    assert "dividend_calendar" in row.last_error
    assert "not in current bootstrap catalogue" in row.last_error

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


def test_stage_keyed_dicts_reference_only_real_stages() -> None:
    """Every stage_key used as a key in the four stage-keyed catalogue
    dicts MUST be a stage present in ``_BOOTSTRAP_STAGE_SPECS``.

    Codex checkpoint-1 (HIGH) on the bulk-only-bootstrap redesign:
    ``_CAPABILITY_PROVIDERS`` is built from ``_STAGE_PROVIDES`` WITHOUT
    filtering to the live stage set. So dropping a stage from
    ``_BOOTSTRAP_STAGE_SPECS`` while leaving its ``_STAGE_PROVIDES`` /
    ``_STAGE_PROVIDES_ON_SKIP`` / ``_STAGE_REQUIRES_CAPS`` /
    ``_STAGE_LANE_OVERRIDES`` entry behind would let the catalogue
    tests pass while runtime sees no status for that stage — the cap is
    silently mis-attributed to a stage that never runs. This invariant
    fails at test time the moment a dropped stage leaves a stale entry,
    so the dropping PR must clean all four dicts in lockstep.
    """
    spec_keys = {spec.stage_key for spec in _BOOTSTRAP_STAGE_SPECS}
    for name, mapping in (
        ("_STAGE_PROVIDES", _STAGE_PROVIDES),
        ("_STAGE_PROVIDES_ON_SKIP", _STAGE_PROVIDES_ON_SKIP),
        ("_STAGE_REQUIRES_CAPS", _STAGE_REQUIRES_CAPS),
        ("_STAGE_LANE_OVERRIDES", _STAGE_LANE_OVERRIDES),
    ):
        stale = set(mapping.keys()) - spec_keys
        assert not stale, f"{name} references stages not in _BOOTSTRAP_STAGE_SPECS: {stale}"


# #1407 — every bootstrap stage whose job WALKS ``filing_events`` for its
# source accessions MUST directly require ``filing_events_seeded`` so the
# DAG never lets it run against an unpopulated table (across parallel
# lanes, an ``*_dataset_processed`` ordering cap does NOT imply
# filing_events is seeded — that bit S20 ``sec_form3_ingest`` on run_id=1).
#
# Manually maintained: when a new stage that reads ``filing_events`` is
# added, list it here. Provider stages that SEED filing_events
# (``sec_submissions_ingest`` S8, ``sec_first_install_drain`` S16) are
# deliberately excluded — they may read it but must not require the cap
# they themselves provide. ``ownership_observations_backfill`` (S24)
# reads filing_events as a bridge but is gated transitively via the
# ownership input caps, so it is excluded from the DIRECT-require
# assertion below.
#
# #1413 — S14 submissions_files_walk, S17 def14a, S19 insider_backfill,
# S20 form3 DROPPED (bulk-only). The surviving filing_events readers that
# seed typed metadata are S18 (business summary) + S21 (8-K); both still
# require ``filing_events_seeded`` directly.
_FILING_EVENTS_READER_STAGES: frozenset[str] = frozenset(
    {
        "sec_business_summary_bootstrap",  # S18
        "sec_8k_events_ingest",  # S21
    }
)


@pytest.mark.parametrize("stage_key", sorted(_FILING_EVENTS_READER_STAGES))
def test_filing_events_reader_stages_require_filing_events_seeded(stage_key: str) -> None:
    """#1407 regression sentinel: a stage that walks ``filing_events``
    must require ``filing_events_seeded`` directly, or it can fire across
    a parallel lane before S8/S15/S16 populate the table — producing a
    silent 0-row pass (the failure that blocked S24 on run_id=1).
    """
    req = _STAGE_REQUIRES_CAPS[stage_key]
    assert req.all_of and "filing_events_seeded" in req.all_of, (
        f"{stage_key} walks filing_events but does not require "
        "filing_events_seeded (#1407 read-before-seed ordering invariant)"
    )


def test_sec_first_install_drain_dispatches_use_bulk_zip_true() -> None:
    """#1277 T7 — S16 StageSpec dispatches ``use_bulk_zip=True`` on the
    bootstrap path so the drain routes PRIMARY ``CIK<10>.json`` reads
    through the local ``submissions.zip`` S7 landed.

    Regression sentinel — a future spec edit dropping this flag would
    silently re-route ~11k non-issuer primary fetches back through HTTP
    and re-inflate S16 wall-clock by 5-10×. Sibling to the #1366
    ``submissions_processed`` cap test above — both pin the perf
    invariant from different angles.

    Companion: ``JOB_INTERNAL_KEYS["sec_first_install_drain"]`` must
    include ``use_bulk_zip`` so the bootstrap-dispatched param survives
    validation (see ``tests/test_job_registry.py``).
    """
    spec = next(
        (s for s in _BOOTSTRAP_STAGE_SPECS if s.stage_key == "sec_first_install_drain"),
        None,
    )
    assert spec is not None, "sec_first_install_drain missing from _BOOTSTRAP_STAGE_SPECS"
    assert spec.params.get("use_bulk_zip") is True, (
        "S16 must dispatch use_bulk_zip=True (#1277 — local-zip primary-page path)"
    )


def test_sec_first_install_drain_dispatches_follow_pagination_false() -> None:
    """#1413 Step 2.3 — S16 StageSpec dispatches ``follow_pagination=False``
    on the bootstrap path so the drain NEVER fetches secondary
    ``CIK<10>-submissions-<NNN>.json`` pages (the last per-CIK HTTP
    source in the bootstrap lane). Secondary-page (deep-history) coverage
    is deferred to steady-state Layer 2/3.

    The steady-state safety-net invoker keeps ``follow_pagination=True``
    by default; only the bootstrap dispatch flips it via the StageSpec
    params + the ``JOB_INTERNAL_KEYS`` allow-list. Regression sentinel —
    dropping this flag re-introduces ~per-CIK secondary-page HTTP and
    re-inflates the bootstrap wall-clock.
    """
    spec = next(
        (s for s in _BOOTSTRAP_STAGE_SPECS if s.stage_key == "sec_first_install_drain"),
        None,
    )
    assert spec is not None, "sec_first_install_drain missing from _BOOTSTRAP_STAGE_SPECS"
    assert spec.params.get("follow_pagination") is False, (
        "S16 must dispatch follow_pagination=False (#1413 — zero secondary-page HTTP in bootstrap)"
    )


def test_s16_no_longer_provides_secondary_pages_walked() -> None:
    """#1413 Step 2.3 — with ``follow_pagination=False`` S16 no longer
    walks secondary pages, so it must NOT advertise
    ``submissions_secondary_pages_walked``. The cap is removed end-to-end
    (Capability Literal + provides + every consumer).
    """
    assert "submissions_secondary_pages_walked" not in _STAGE_PROVIDES.get("sec_first_install_drain", ())


def test_master_idx_gap_close_stage_present() -> None:
    """#1415 (P3) — the recent-window gap-close occupies the freed S15 slot
    (order 15), runs on ``sec_rate``, and dispatches the bootstrap-only
    ``sec_master_idx_gap_close`` invoker (NOT the unscoped weekly G12
    ``sec_master_idx_quarterly_sweep``). The dedicated invoker is what carries
    the filing-metadata ``source_allowlist`` so the gap-close never advances
    an ownership-source watermark (the per-source guard). It gates on
    ``cik_mapping_ready`` + ``submissions_processed`` and provides NO
    capability (pure filing-metadata discovery — must never advertise an
    ownership cap, P3 invariant).
    """
    spec = next(
        (s for s in _BOOTSTRAP_STAGE_SPECS if s.stage_key == "sec_master_idx_gap_close"),
        None,
    )
    assert spec is not None, "sec_master_idx_gap_close missing from _BOOTSTRAP_STAGE_SPECS"
    assert spec.stage_order == 15
    assert spec.lane == "sec_rate"
    assert spec.job_name == "sec_master_idx_gap_close", (
        "must dispatch the source-allowlist-scoped bootstrap invoker, not the unscoped G12 sweep"
    )
    req = _STAGE_REQUIRES_CAPS["sec_master_idx_gap_close"]
    assert set(req.all_of) == {"cik_mapping_ready", "submissions_processed"}
    assert req.any_of == ()
    assert "sec_master_idx_gap_close" not in _STAGE_PROVIDES
    assert "sec_master_idx_gap_close" not in _STAGE_PROVIDES_ON_SKIP


@pytest.mark.parametrize("stage_key", ["sec_business_summary_bootstrap", "sec_8k_events_ingest"])
def test_typed_metadata_stages_do_not_require_secondary_pages_walked(stage_key: str) -> None:
    """#1413 Step 2.3 — S18 (business summary) + S21 (8-K) typed-metadata
    seed stages gate ONLY on ``filing_events_seeded`` now; the
    ``submissions_secondary_pages_walked`` requirement is dropped (S16 no
    longer provides it, and recent deep-history bodies are lazy-on-view
    per #1343). Leaving the requirement would make these stages
    permanently unsatisfiable once the cap is removed.
    """
    req = _STAGE_REQUIRES_CAPS[stage_key]
    assert "submissions_secondary_pages_walked" not in req.all_of
    assert "filing_events_seeded" in req.all_of


def test_sec_first_install_drain_requires_submissions_processed() -> None:
    """Issue #1365 — S16 ``sec_first_install_drain`` MUST require
    ``submissions_processed`` so it waits for the bulk path
    (``sec_submissions_ingest``) to terminalise before starting.

    Background: S16's fast-path at
    ``app/jobs/sec_first_install_drain.py::seed_manifest_from_filing_events``
    runs ONCE at function entry. If ``filing_events`` has no SEC rows
    yet, S16 falls through to a per-CIK HTTP loop covering ~25k
    subjects (~85 min observed on Run #8). The fast-path was supposed
    to fire when the bulk path had already populated ``filing_events``,
    but pre-#1365 S16 required only ``cik_mapping_ready`` and raced
    ahead of ``sec_submissions_ingest``.

    Adding ``submissions_processed`` (provided on SUCCESS or SKIP by
    S8) makes S16 wait for the bulk path to terminalise: success →
    filing_events populated → fast-path fires; skip → cascade-skip
    parity preserves the slow-connection fallback.

    Regression sentinel — a future spec edit that drops the requires
    line would re-introduce the HTTP-instead-of-files fallback
    silently and re-inflate S16 wall-clock by 5-10×.
    """
    req = _STAGE_REQUIRES_CAPS["sec_first_install_drain"]
    assert "submissions_processed" in req.all_of, (
        "sec_first_install_drain must require submissions_processed "
        "(#1365 fast-path ordering invariant — wait for bulk path before draining)"
    )


def test_submissions_processed_provided_by_s8_on_success_and_skip() -> None:
    """Companion invariant to the above. ``submissions_processed`` is
    provided by ``sec_submissions_ingest`` (S8) on BOTH success AND
    skip — the SKIP entry preserves the slow-connection fallback
    where S7 → S8 cascade-skip and S15 still proceeds as the legacy
    chain's filing_events seeder. Drop either provides entry and the
    slow-connection path either deadlocks (no SKIP entry: S15 cascade-
    skips, no provider) or reverts to the lock contention bug (no
    SUCCESS entry: nothing waits on S8).
    """
    from app.services.bootstrap_orchestrator import (
        _STAGE_PROVIDES,
        _STAGE_PROVIDES_ON_SKIP,
    )

    assert "submissions_processed" in _STAGE_PROVIDES["sec_submissions_ingest"]
    assert "submissions_processed" in _STAGE_PROVIDES_ON_SKIP["sec_submissions_ingest"]


# ---------------------------------------------------------------------------
# #1233 extended lock-contention cap-gates — PR-1292 pattern. #1437: the
# three ``*_dataset_processed`` ordering caps were removed (their legacy
# consumers were dropped by #1413), so only ``submissions_processed``
# remains; its provider/skip invariants are pinned below.
# ---------------------------------------------------------------------------


def test_ordering_only_caps_disjoint_from_strict_gate_caps() -> None:
    """Codex 2 LOW on #1233 cap-gates: pin the ``_ORDERING_ONLY_CAPS``
    allowlist as an explicit invariant. Members must NOT also be
    strict-gate caps (``_CAPABILITY_MIN_ROWS``) — ordering caps fire
    on any terminal status, strict-gate caps require a row floor on
    ``success``; conflating the two would let a strict cap leak into
    the terminal-failure escape hatch and falsely satisfy a content
    requirement.
    """
    from app.services.bootstrap_orchestrator import (
        _CAPABILITY_MIN_ROWS,
        _ORDERING_ONLY_CAPS,
    )

    assert _ORDERING_ONLY_CAPS == frozenset(
        {
            "submissions_processed",
        }
    ), (
        "_ORDERING_ONLY_CAPS membership changed without updating the test. "
        "New ordering caps must be added consciously: they advertise on "
        "ANY terminal status, including error/blocked/cancelled — a content "
        "cap added here would silently bypass the strict-gate row floor."
    )
    leaks = _ORDERING_ONLY_CAPS & _CAPABILITY_MIN_ROWS.keys()
    assert not leaks, (
        f"ordering-only caps overlap strict-gate caps: {leaks}. "
        "An ordering cap fires on terminal failure regardless of row "
        "count; a strict-gate cap requires rows on success. Combining "
        "both would let a zero-row failure satisfy a content cap."
    )


def test_ordering_caps_satisfied_on_terminal_failure_but_content_caps_are_not() -> None:
    """Codex 2 LOW on #1233 cap-gates: prove the terminal-failure
    escape hatch in ``_satisfied_capabilities`` works as documented.
    The ordering cap (``submissions_processed``) MUST be satisfied
    whenever its provider reaches blocked / error / cancelled; content
    caps advertised by terminal-failed stages MUST NOT — they stay dead
    so downstream content consumers correctly block.

    (#1437 — the ``*_dataset_processed`` legs were removed with the
    caps; the content-cap-stays-dead half is preserved on the bulk
    ingesters below.)
    """
    from app.services.bootstrap_orchestrator import _satisfied_capabilities

    # Content caps stay dead on terminal failure of the bulk ingesters.
    for terminal_status in ("blocked", "error", "cancelled"):
        caps = _satisfied_capabilities(
            statuses={
                "sec_insider_ingest_from_dataset": terminal_status,
                "sec_13f_ingest_from_dataset": terminal_status,
            },
            rows_processed={
                "sec_insider_ingest_from_dataset": None,
                "sec_13f_ingest_from_dataset": None,
            },
        )
        assert "insider_inputs_seeded" not in caps, (
            f"content cap leaked on status={terminal_status!r} — downstream consumer would falsely proceed"
        )
        assert "institutional_inputs_seeded" not in caps

    # Bot WARNING on PR #1299: ``submissions_processed`` is also a
    # member of ``_ORDERING_ONLY_CAPS`` and thus gains the
    # terminal-failure escape hatch. Pre-#1299 the cap fired on
    # success OR skip only; the extension is intentional — the
    # cap's semantic is "S8 is done writing filing_events", which
    # holds whenever S8 terminalises. Without this assertion the
    # silent extension would be unpinned.
    for terminal_status in ("blocked", "error", "cancelled"):
        caps = _satisfied_capabilities(
            statuses={"sec_submissions_ingest": terminal_status},
            rows_processed={"sec_submissions_ingest": None},
        )
        assert "submissions_processed" in caps, (
            f"submissions_processed missing on status={terminal_status!r} — "
            "legacy filings_history_seed would falsely block (PR-1292 + #1299)"
        )
        # Content cap MUST stay dead on terminal failure.
        assert "filing_events_seeded" not in caps, (
            f"filing_events_seeded leaked on status={terminal_status!r} — "
            "content cap escaped through the ordering escape hatch"
        )


def test_both_ownership_paths_fail_blocks_final_stage(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1138 §8 test 3 (reworked for #1413 bulk-only bootstrap) — the
    legacy per-CIK ownership stages (S19/S20/S22/S23) are dropped, so
    the bulk ingesters S10/S11/S12 are the SOLE providers of the
    per-family ownership caps. Failing S7 ``sec_bulk_download``
    error-deads ``bulk_archives_ready`` → S10/S11/S12 cascade-block →
    every per-family cap is error-dead → ``ownership_observations_backfill``
    transitions to ``blocked`` with a structured "missing capability"
    reason naming at least one per-family cap.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)

    # Fail S7 only. Under bulk-only bootstrap there is no longer a
    # legacy ownership path — the bulk Phase C ingesters are the only
    # per-family-cap providers, and they cascade-block from the
    # error-dead ``bulk_archives_ready`` S7 would have provided.
    #
    # Resolve job_name from stage_key via the catalogue so a future
    # rename doesn't silently no-op the failing set (resolving through
    # ``get_bootstrap_stage_specs()`` raises on a typo).
    _job_by_stage = {spec.stage_key: spec.job_name for spec in get_bootstrap_stage_specs()}
    failing_stage_keys = {"sec_bulk_download"}
    failing = {_job_by_stage[key] for key in failing_stage_keys}
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
        "institutional_inputs_seeded",
        "nport_inputs_seeded",
    )
    assert any(c in reason for c in family_caps), f"expected per-family cap in reason, got: {reason!r}"

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"


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


# ---------------------------------------------------------------------------
# #1140 Task C — strict-gate row-count cap-eval widening
# ---------------------------------------------------------------------------


def test_strict_cap_blocks_consumer_on_zero_rows(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single-provider strict cap with the provider succeeding at
    rows_processed=0 transitions the consumer to ``blocked`` with a
    structured "no surviving provider met rows floor" reason. Run
    finalises ``partial_error``.

    Exercises the real dispatcher end-to-end: the bulk
    ``sec_companyfacts_ingest`` lands ``success`` with ``rows_processed=0``
    (via the fake invoker's rows_by_job override); its sole cap
    ``fundamentals_raw_seeded`` is strict-gated at min_rows=1; the
    downstream ``fundamentals_sync`` blocks.
    """
    _reset_state(ebull_test_conn)
    _bind_settings_to_test_db(monkeypatch)
    _patch_invokers_with_fakes(
        monkeypatch,
        rows_by_job={"sec_companyfacts_ingest": 0},
    )

    start_run(ebull_test_conn, operator_id=None, stage_specs=get_bootstrap_stage_specs())
    ebull_test_conn.commit()

    run_bootstrap_orchestrator()

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    statuses = {stage.stage_key: stage.status for stage in snap.stages}

    assert statuses["sec_companyfacts_ingest"] == "success"
    assert statuses["fundamentals_sync"] == "blocked"

    fundamentals_row = next(s for s in snap.stages if s.stage_key == "fundamentals_sync")
    assert fundamentals_row.last_error is not None
    assert "fundamentals_raw_seeded" in fundamentals_row.last_error
    assert "rows floor 1" in fundamentals_row.last_error
    assert "rows_processed=0" in fundamentals_row.last_error

    state = read_state(ebull_test_conn)
    assert state.status == "partial_error"


def test_strict_cap_dead_on_zero_rows_classifies_error_not_skip() -> None:
    """Unit test for the cap-eval helpers — confirms a strict cap
    where the only provider is ``success`` with under-floor rows is
    classified as error-dead (so consumer blocks), not skip-only-dead
    (which would cascade-skip).
    """
    from app.services.bootstrap_orchestrator import (
        _capability_is_dead,
        _classify_dead_cap,
    )

    statuses = {"sec_companyfacts_ingest": "success"}
    rows = {"sec_companyfacts_ingest": 0}
    assert _capability_is_dead("fundamentals_raw_seeded", statuses, rows) is True
    assert _classify_dead_cap("fundamentals_raw_seeded", statuses, rows) == "error"


def test_non_strict_cap_unchanged_by_zero_rows() -> None:
    """A cap NOT in ``_CAPABILITY_MIN_ROWS`` is satisfied by a
    ``success`` provider regardless of ``rows_processed``. Legacy
    Task A behaviour preserved — confirms the new strict-gate rule
    doesn't widen to caps it shouldn't touch.

    ``universe_seeded`` is not in the strict set; a ``universe_sync``
    success with rows=0 still satisfies it.
    """
    from app.services.bootstrap_orchestrator import _satisfied_capabilities

    caps = _satisfied_capabilities(
        {"universe_sync": "success"},
        {"universe_sync": 0},
    )
    assert "universe_seeded" in caps


def test_strict_caps_have_at_least_one_provider() -> None:
    """Every cap in ``_CAPABILITY_MIN_ROWS`` must have at least one
    registered provider in ``_CAPABILITY_PROVIDERS``. Catches a stale
    entry that names a removed cap before the dispatcher tries to
    evaluate a never-satisfiable strict-gate requirement at runtime.
    """
    from app.services.bootstrap_orchestrator import _CAPABILITY_MIN_ROWS

    missing = [c for c in _CAPABILITY_MIN_ROWS if not _CAPABILITY_PROVIDERS.get(c)]  # type: ignore[arg-type]
    assert not missing, f"strict-gate caps with no provider: {missing}"


# ---------------------------------------------------------------------------
# Stream A PR-C1 T1.2 (#1233): fundamentals_sync cap-strengthen + lane delta.
# ---------------------------------------------------------------------------


def test_fundamentals_sync_requires_four_caps_after_pr_c1() -> None:
    """Stream A PR-C1 T1.2 (#1233): S25 ``fundamentals_sync`` is
    strengthened from a 1-cap requirement (`fundamentals_raw_seeded`
    only) to the 4-cap tuple needed by the bootstrap entrypoint's
    audit-during-bootstrap defence (which lands in PR-C2).

    All four caps are verified real (spec §0.1 grep proof). Terminal-
    status safety: ``submissions_processed`` is in
    ``_ORDERING_ONLY_CAPS`` so the dispatcher satisfies the cap on
    ``blocked|error|cancelled`` as well as ``success|skipped`` — no
    stuck-S25 failure mode when S8 errors.

    Regression sentinel — relaxing this requirement back to a subset
    would re-expose the audit-during-bootstrap trap that Codex v3
    finding #8 flagged.
    """
    req = _STAGE_REQUIRES_CAPS["fundamentals_sync"]
    assert set(req.all_of) == {
        "bulk_archives_ready",
        "cik_mapping_ready",
        "submissions_processed",
        "fundamentals_raw_seeded",
    }, "fundamentals_sync must require the 4-cap PR-C1 tuple (#1233 §13)"


def test_fundamentals_sync_runs_on_db_fundamentals_raw_lane_after_pr_c2() -> None:
    """Stream A PR-C2 T1.2 (#1233): S25 dispatches the bootstrap-only
    ``fundamentals_sync_bootstrap`` invoker (NOT the steady-state
    ``fundamentals_sync`` job) on the dedicated ``db_fundamentals_raw``
    lane. The job_name divergence is what lets PR-C2's lane
    reassignment coexist with the steady-state ScheduledJob's
    ``source="db"`` registration — see ``app/jobs/sources.py:
    _build_job_name_to_source`` Pass 1/2 separation.

    Flipped from the PR-C1 deferral test that pinned `lane=="db"`
    pending PR-C2's bootstrap-only invoker.
    """
    fundamentals_spec = next(
        (s for s in _BOOTSTRAP_STAGE_SPECS if s.stage_key == "fundamentals_sync"),
        None,
    )
    assert fundamentals_spec is not None, "fundamentals_sync stage missing from _BOOTSTRAP_STAGE_SPECS"
    assert fundamentals_spec.stage_order == 25
    assert fundamentals_spec.lane == "db_fundamentals_raw", (
        "fundamentals_sync (stage_key) runs on db_fundamentals_raw lane post-PR-C2 (#1233 §5)"
    )
    assert fundamentals_spec.job_name == "fundamentals_sync_bootstrap", (
        "fundamentals_sync stage dispatches fundamentals_sync_bootstrap invoker post-PR-C2"
    )
