"""Cross-lane parallelism + per-completion cap recomputation tests for
``_phase_batched_dispatch`` (#1233 PR-2).

Spec: docs/superpowers/specs/2026-05-22-bootstrap-etl-optimisation-v2.md §6.

The pre-PR-2 dispatcher waits for an entire ready batch to drain
(``wait([f for _, f in all_futures])``) before re-evaluating
capabilities and the cancel signal. That blocks idle lanes for the
duration of the slowest sibling — measured at 80+ min sec_rate lane
idle on run_id=4. PR-2 replaces the batch-join with an
``as_completed``-style first-completion poll loop and recomputes caps
+ cancel checkpoint between every individual completion.

These tests are the load-bearing regression net: they pin the
contract pre-implementation so a future "optimisation" cannot
silently revert to batch-join semantics.

The fixtures build entirely synthetic stages — they don't go through
``get_bootstrap_stage_specs``. Each stage's invoker is a deterministic
sleep + bookkeeping closure. ``_phase_batched_dispatch`` is exercised
directly with ``provides_map`` overrides so synthetic
``stage_key``/``capability`` pairs participate in cap-eval.

Lane caps are mutated via monkeypatching ``_LANE_MAX_CONCURRENCY`` so
the per-lane in-flight respect can be exercised on a single lane (the
production cap=1 setting elides intra-lane parallelism).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import psycopg
import pytest

from app.services.bootstrap_orchestrator import (
    CapRequirement,
    _phase_batched_dispatch,
    _RunnableStage,
)
from app.services.bootstrap_state import (
    StageSpec,
    cancel_run,
    read_latest_run_with_stages,
    start_run,
)

# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
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
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    url = test_database_url()
    monkeypatch.setattr(app_settings, "database_url", url)
    return url


def _register_synthetic_jobs(monkeypatch: pytest.MonkeyPatch, mapping: dict[str, str]) -> None:
    """Add synthetic ``job_name -> Lane`` entries to the source-lock
    registry so ``JobLock`` resolves without ``KeyError`` for
    fixture-only job names.
    """
    from app.jobs.sources import get_job_name_to_source

    registry = get_job_name_to_source()
    for name, lane in mapping.items():
        monkeypatch.setitem(registry, name, lane)  # type: ignore[arg-type]


@dataclass(frozen=True)
class _Span:
    """One stage's recorded wall-clock invocation span."""

    name: str
    started_at: float
    ended_at: float


def _sleep_invoker(name: str, sleep_seconds: float, calls: list[_Span]) -> Callable[..., None]:
    """Record start/end wall-clock for the named stage. The dispatcher's
    parallelism contract is then asserted against these spans.

    Returns a closure suitable for ``_RunnableStage.invoker``. The
    invoker accepts (and ignores) the per-stage params kwarg per the
    PR1b-2 ``JobInvoker`` widening.
    """

    def _invoker(_params: object = None) -> None:
        started = time.monotonic()
        time.sleep(sleep_seconds)
        ended = time.monotonic()
        calls.append(_Span(name=name, started_at=started, ended_at=ended))

    return _invoker


def _orchestrator_specs_for(stage_keys: tuple[str, ...]) -> tuple[StageSpec, ...]:
    """Build minimal ``StageSpec`` tuples for ``start_run`` so the
    dispatcher's DB-side bookkeeping (mark_stage_running etc.) has
    rows to update. Lane + job_name on the spec are decorative for
    the dispatcher's runnable list (which is supplied explicitly).
    """
    return tuple(
        StageSpec(
            stage_key=key,
            stage_order=index + 1,
            lane="init",
            job_name=f"synth_job_{key}",
        )
        for index, key in enumerate(stage_keys)
    )


# ---------------------------------------------------------------------------
# 1. Cross-lane parallelism: fast lane drains before slow lane finishes.
# ---------------------------------------------------------------------------


def test_cross_lane_parallelism_fast_lane_drains_before_slow_lane(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two lanes: slow lane has one stage that sleeps 2.0s; fast lane
    has five stages that sleep 0.10s each. Production lane caps stay
    at 1 — the parallelism win is **cross-lane**, not intra-lane.

    Pre-PR-2: the dispatcher's ``wait([all_futures])`` blocks until
    EVERY future in the ready batch (slow + first-fast) completes;
    the next fast-lane sibling only gets submitted after that join
    returns. Five 100 ms fast-lane stages serialised behind a 2 s
    slow-lane batch-mate take ~2.4 s to drain.

    Post-PR-2: each fast-lane completion is picked up by the poll
    loop and immediately submits the next fast-lane stage. Five fast
    stages drain in ~0.5 s wall-clock — well before the 2.0 s slow
    stage finishes.

    Assertion: all five fast-lane stages have ``ended_at`` BEFORE the
    slow lane's ``ended_at``. This single check separates batch-join
    semantics from per-completion semantics — the pre-PR-2 dispatcher
    cannot satisfy it.
    """
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)

    fast_keys = tuple(f"fast_{i}" for i in range(5))
    all_keys = ("slow_0", *fast_keys)
    _register_synthetic_jobs(
        monkeypatch,
        {f"synth_job_{k}": ("init" if k == "slow_0" else "etoro") for k in all_keys},
    )

    specs = _orchestrator_specs_for(all_keys)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    calls: list[_Span] = []
    runnable = [
        _RunnableStage(
            stage_key="slow_0",
            job_name="synth_job_slow_0",
            lane="init",
            invoker=_sleep_invoker("slow_0", 2.0, calls),
            requires=CapRequirement(),
        ),
        *[
            _RunnableStage(
                stage_key=k,
                job_name=f"synth_job_{k}",
                lane="etoro",
                invoker=_sleep_invoker(k, 0.10, calls),
                requires=CapRequirement(),
            )
            for k in fast_keys
        ],
    ]

    statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
    )

    assert cancelled is False
    assert all(statuses[k] == "success" for k in all_keys), statuses

    by_name = {c.name: c for c in calls}
    slow_end = by_name["slow_0"].ended_at
    for k in fast_keys:
        assert by_name[k].ended_at < slow_end, (
            f"{k} finished at {by_name[k].ended_at:.3f}s but slow lane finished at "
            f"{slow_end:.3f}s — fast-lane stage was serialised behind slow-lane batch-mate "
            "(batch-join semantics regression)."
        )


# ---------------------------------------------------------------------------
# 2. Per-completion cap recomputation: B starts shortly after A completes,
#    NOT after the slowest batch-mate completes.
# ---------------------------------------------------------------------------


def test_per_completion_cap_recomputation_immediately_unblocks_consumer(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three stages in one batch:

      * stage_a (cap-provider) — sleeps 0.5 s, provides capability
        ``synth_cap_alpha``.
      * stage_b (cap-requirer) — requires ``synth_cap_alpha``. Sleeps 0.1 s.
      * stage_pad — independent slow stage, sleeps 2.0 s. Blocks the
        batch-join in the pre-PR-2 dispatcher.

    Pre-PR-2: stage_b cannot dispatch until the entire ready batch
    drains via ``wait([all_futures])``. The first batch contains only
    stage_a + stage_pad (B is not yet ready). The dispatcher waits
    ~2.0 s for stage_pad to finish before re-evaluating caps and
    dispatching B.

    Post-PR-2: stage_a's completion at t≈0.5 s immediately triggers
    cap recomputation; stage_b is dispatched on its lane within the
    poll iteration. Stage_b starts within ~0.2 s of stage_a's end —
    NOT 1.5 s later when stage_pad finishes.

    Assertion: ``stage_b.started_at - stage_a.ended_at < 0.3``. The
    tolerance accommodates DB round-trips for ``mark_stage_running``
    + cap re-eval; the pre-PR-2 dispatcher's ~1.5 s gap would fail
    this margin by a factor of 5.
    """
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)

    _register_synthetic_jobs(
        monkeypatch,
        {
            "synth_job_stage_a": "init",
            "synth_job_stage_b": "etoro",
            "synth_job_stage_pad": "sec_rate",
        },
    )

    specs = _orchestrator_specs_for(("stage_a", "stage_b", "stage_pad"))
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    calls: list[_Span] = []
    runnable = [
        _RunnableStage(
            stage_key="stage_a",
            job_name="synth_job_stage_a",
            lane="init",
            invoker=_sleep_invoker("stage_a", 0.5, calls),
            requires=CapRequirement(),
        ),
        _RunnableStage(
            stage_key="stage_b",
            job_name="synth_job_stage_b",
            lane="etoro",
            invoker=_sleep_invoker("stage_b", 0.1, calls),
            requires=CapRequirement(all_of=("synth_cap_alpha",)),  # type: ignore[arg-type]
        ),
        _RunnableStage(
            stage_key="stage_pad",
            job_name="synth_job_stage_pad",
            lane="sec_rate",
            invoker=_sleep_invoker("stage_pad", 2.0, calls),
            requires=CapRequirement(),
        ),
    ]

    statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
        provides_map={"stage_a": ("synth_cap_alpha",)},  # type: ignore[dict-item]
    )

    assert cancelled is False
    assert statuses == {"stage_a": "success", "stage_b": "success", "stage_pad": "success"}, statuses

    by_name = {c.name: c for c in calls}
    gap = by_name["stage_b"].started_at - by_name["stage_a"].ended_at
    assert gap < 0.3, (
        f"stage_b started {gap:.3f}s after stage_a completed; expected < 0.3s. "
        "Pre-PR-2 batch-join would gate stage_b on stage_pad's 2.0s sleep — confirms "
        "the dispatcher recomputes caps per completion, not per batch."
    )


# ---------------------------------------------------------------------------
# 3. Cancel observation latency: cancel after A completes ⇒ B never starts.
# ---------------------------------------------------------------------------


def test_cancel_after_first_completion_aborts_followup_stage(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """While stage_a (cap-provider) runs, a separate thread inserts a
    process_stop row. Stage_a's completion triggers cap recomputation
    AND the cancel checkpoint at the top of the next poll iteration.
    Stage_b must NEVER start.

    Pre-PR-2: the cancel checkpoint only fires between BATCHES. If
    stage_a + stage_pad both completed before stage_b could be
    submitted, the next iteration would observe the cancel. But if
    stage_b were in the same ready batch as stage_a, batch-join would
    submit stage_b before checkpointing — and stage_b would run to
    completion.

    Post-PR-2: every completion is followed by a cancel checkpoint.
    Stage_b's submission gate sees the cancel and skips submission.

    Assertion: stage_b's invoker is never called. ``cancelled=True``
    propagates back to the caller.
    """
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)

    _register_synthetic_jobs(
        monkeypatch,
        {
            "synth_job_stage_a": "init",
            "synth_job_stage_b": "etoro",
        },
    )

    specs = _orchestrator_specs_for(("stage_a", "stage_b"))
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    calls: list[_Span] = []
    b_started = threading.Event()

    def a_invoker(_params: object = None) -> None:
        started = time.monotonic()
        time.sleep(0.3)
        # Insert the cancel row while still inside stage_a so the next
        # checkpoint sees it.
        with psycopg.connect(test_db_url) as conn:
            cancel_run(conn, requested_by_operator_id=None)
            conn.commit()
        ended = time.monotonic()
        calls.append(_Span(name="stage_a", started_at=started, ended_at=ended))

    def b_invoker(_params: object = None) -> None:  # pragma: no cover - must not run
        b_started.set()
        calls.append(_Span(name="stage_b", started_at=time.monotonic(), ended_at=time.monotonic()))

    runnable = [
        _RunnableStage(
            stage_key="stage_a",
            job_name="synth_job_stage_a",
            lane="init",
            invoker=a_invoker,
            requires=CapRequirement(),
        ),
        _RunnableStage(
            stage_key="stage_b",
            job_name="synth_job_stage_b",
            lane="etoro",
            invoker=b_invoker,
            requires=CapRequirement(all_of=("synth_cap_alpha",)),  # type: ignore[arg-type]
        ),
    ]

    _statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
        provides_map={"stage_a": ("synth_cap_alpha",)},  # type: ignore[dict-item]
    )

    assert cancelled is True
    assert not b_started.is_set(), "stage_b started despite cancel checkpoint after stage_a completion"
    assert [c.name for c in calls] == ["stage_a"]

    # The run terminalised cancelled — confirm DB-side state matches.
    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    assert snap.run_status == "cancelled"


# ---------------------------------------------------------------------------
# 4. Per-lane cap respect: 3 stages on a cap=1 lane → strictly serial.
# ---------------------------------------------------------------------------


def test_lane_cap_respected_strictly_serial_at_cap_one(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three stages on a single lane (``etoro``, production cap=1).
    The dispatcher must NEVER submit more than one of them to the
    lane executor at the same time.

    Production lane caps are all 1 because ``JobLock`` (per-source
    advisory lock in PG) serialises stages with the same lane / source
    anyway — submitting two at once would only earn one of them a
    ``JobAlreadyRunning`` error. The pre-PR-2 dispatcher demonstrably
    over-submitted: when the in-batch slice cap of one was exceeded
    by combining multiple iterations the second stage hit a
    ``JobLock`` collision (observed in this test file's own initial
    failure run). The new dispatcher uses an in-flight counter
    decremented at completion + a re-evaluation gate at submit, so
    over-submission is structurally impossible.

    Assertion: pairwise spans NEVER overlap. ``peak_concurrent == 1``
    across all three intervals.
    """
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)
    # Production lane cap is 1; leave as-is.

    _register_synthetic_jobs(
        monkeypatch,
        {f"synth_job_stage_{i}": "etoro" for i in range(3)},
    )

    keys = tuple(f"stage_{i}" for i in range(3))
    specs = _orchestrator_specs_for(keys)
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    calls: list[_Span] = []
    runnable = [
        _RunnableStage(
            stage_key=k,
            job_name=f"synth_job_{k}",
            lane="etoro",
            invoker=_sleep_invoker(k, 0.2, calls),
            requires=CapRequirement(),
        )
        for k in keys
    ]

    statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
    )

    assert cancelled is False
    assert all(statuses[k] == "success" for k in keys), statuses
    assert len(calls) == 3

    # Pairwise interval overlap — strict serial on cap=1 lane.
    sorted_calls = sorted(calls, key=lambda c: c.started_at)

    def _max_concurrent(spans: list[_Span]) -> int:
        events: list[tuple[float, int]] = []
        for span in spans:
            events.append((span.started_at, +1))
            events.append((span.ended_at, -1))
        # Sort with end-events ahead of start-events at the same
        # timestamp so adjacent serial spans don't appear to overlap.
        events.sort(key=lambda e: (e[0], -e[1]))
        cur = peak = 0
        for _, delta in events:
            cur += delta
            peak = max(peak, cur)
        return peak

    peak = _max_concurrent(sorted_calls)
    assert peak == 1, (
        f"observed peak concurrency {peak} on cap=1 lane (expected exactly 1). "
        "Pre-PR-2 over-submitted siblings → JobLock collisions."
    )


# ---------------------------------------------------------------------------
# 5. Deadlock detection: a pending stage with no possible provider is
#    still classified as blocked / abandoned per existing logic.
# ---------------------------------------------------------------------------


def test_pending_stage_with_no_provider_is_blocked(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stage whose ``CapRequirement`` references a capability with
    no producer in the runnable set + no preexisting status terminalises
    cleanly as ``blocked`` with a structured "missing capability"
    reason. The cap-eval classifier owns this path; the new poll loop
    must not regress it (pre-PR-2 produced the same outcome via the
    same classifier).

    Distinct from the "abandoned" reason at the dispatcher's deadlock
    branch — that branch fires only when the classifier returns None
    (requirement still potentially satisfiable) AND there are no
    in-flight futures, which the cap-eval layer makes structurally
    rare. This test pins the common no-provider semantic.
    """
    _reset_state(ebull_test_conn)
    test_db_url = _bind_settings_to_test_db(monkeypatch)

    _register_synthetic_jobs(monkeypatch, {"synth_job_lonely": "init"})
    specs = _orchestrator_specs_for(("lonely",))
    run_id = start_run(ebull_test_conn, operator_id=None, stage_specs=specs)
    ebull_test_conn.commit()

    def lonely_invoker(_params: object = None) -> None:  # pragma: no cover - must not run
        raise AssertionError("lonely must not invoke without its provider")

    runnable = [
        _RunnableStage(
            stage_key="lonely",
            job_name="synth_job_lonely",
            lane="init",
            invoker=lonely_invoker,
            # Cap with no producer anywhere — neither in runnable nor in
            # preexisting_statuses. Classifier flags as error-dead.
            requires=CapRequirement(all_of=("synth_cap_phantom",)),  # type: ignore[arg-type]
        ),
    ]

    statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=test_db_url,
    )

    assert cancelled is False
    assert statuses == {"lonely": "blocked"}

    snap = read_latest_run_with_stages(ebull_test_conn)
    assert snap is not None
    by_key = {s.stage_key: s for s in snap.stages}
    assert by_key["lonely"].status == "blocked"
    assert by_key["lonely"].last_error is not None
    # Structured cap-eval reason — distinct from the dispatcher's
    # deadlock "abandoned" fallback.
    assert "missing capability" in by_key["lonely"].last_error.lower()
    assert "synth_cap_phantom" in by_key["lonely"].last_error
