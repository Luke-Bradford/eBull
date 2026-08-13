"""Unit tests for ``_emit_dispatcher_telemetry`` + ``dispatcher_idle_analysis``.

Spec: docs/proposals/etl/phase-0-instrumentation.md §2.9.

The helper runs in the dispatcher's hot poll loop so the tests pin:

* per-lane ``idle_type`` classification (A / B / null) under synthetic
  ``statuses`` / ``in_flight`` / ``pending_keys``;
* JSONL writes are append-only to ``<output_dir>/<run_id>.jsonl``;
* I/O failures NEVER raise into the dispatcher;
* aggregation matches the spec's per-lane definitions, including
  ``idle_b_max_run_iters`` consecutive-run detection.

No DB, no orchestrator boot — direct helper exercise. The integration
contract (emission fires on both wait branches every iteration) is
covered by the cross-lane parallelism tests reading the same JSONL
file at run end if a future test wants it; the unit tests below pin
the helper itself so a refactor cannot silently change the contract.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path

import pytest

from app.services.bootstrap_orchestrator import (
    CapRequirement,
    _emit_dispatcher_telemetry,
    _RunnableStage,
)


def _stage(stage_key: str, lane: str, *, requires: CapRequirement | None = None) -> _RunnableStage:
    return _RunnableStage(
        stage_key=stage_key,
        job_name=f"synth_job_{stage_key}",
        lane=lane,
        invoker=lambda _params=None: None,
        requires=requires or CapRequirement(),
    )


def _read_single_record(path: Path) -> dict[str, object]:
    lines = path.read_text().splitlines()
    assert len(lines) == 1, f"expected 1 JSONL line, got {len(lines)}: {lines!r}"
    return json.loads(lines[0])


# ---------------------------------------------------------------------------
# Helper: writes JSONL with the expected envelope shape.
# ---------------------------------------------------------------------------


def test_emit_writes_jsonl_to_run_file(tmp_path: Path) -> None:
    by_key = {"alpha": _stage("alpha", "init")}
    _emit_dispatcher_telemetry(
        run_id=42,
        iteration=1,
        statuses={"alpha": "pending"},
        in_flight_keys=set(),
        lane_in_flight_count={"init": 0},
        caps=set(),
        by_key=by_key,
        wait_returned_empty=True,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "42.jsonl")
    assert record["run_id"] == 42
    assert record["iteration"] == 1
    assert record["wait_returned_empty"] is True
    assert "ts" in record and isinstance(record["ts"], str)
    assert "lanes" in record


def test_emit_appends_one_line_per_invocation(tmp_path: Path) -> None:
    by_key = {"alpha": _stage("alpha", "init")}
    for i in range(3):
        _emit_dispatcher_telemetry(
            run_id=7,
            iteration=i + 1,
            statuses={"alpha": "pending"},
            in_flight_keys=set(),
            lane_in_flight_count={"init": 0},
            caps=set(),
            by_key=by_key,
            wait_returned_empty=True,
            output_dir=tmp_path,
        )

    lines = (tmp_path / "7.jsonl").read_text().splitlines()
    assert len(lines) == 3
    iters = [json.loads(line)["iteration"] for line in lines]
    assert iters == [1, 2, 3]


# ---------------------------------------------------------------------------
# Classifier: idle_type A / B / null.
# ---------------------------------------------------------------------------


def test_classifies_idle_type_a_when_pending_blocked_only(tmp_path: Path) -> None:
    """Blocked pending + no ready + wait_returned_empty + lane idle → A."""
    by_key = {
        "blocked_stage": _stage(
            "blocked_stage",
            "sec_rate",
            requires=CapRequirement(all_of=("universe_seeded",)),
        ),
    }
    _emit_dispatcher_telemetry(
        run_id=1,
        iteration=1,
        statuses={"blocked_stage": "pending"},
        in_flight_keys=set(),
        lane_in_flight_count={"sec_rate": 0},
        caps=set(),  # universe_seeded NOT present
        by_key=by_key,
        wait_returned_empty=True,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "1.jsonl")
    lane = record["lanes"]["sec_rate"]  # type: ignore[index]
    assert lane["idle_type"] == "A"
    assert lane["pending_ready"] == []
    assert lane["pending_blocked"] == ["blocked_stage"]


def test_classifies_idle_type_b_when_pending_ready_exists(tmp_path: Path) -> None:
    """Ready pending + lane idle + wait_returned_empty → B (actionable bug)."""
    by_key = {"ready_stage": _stage("ready_stage", "sec_rate")}  # no requires
    _emit_dispatcher_telemetry(
        run_id=1,
        iteration=1,
        statuses={"ready_stage": "pending"},
        in_flight_keys=set(),
        lane_in_flight_count={"sec_rate": 0},
        caps=set(),
        by_key=by_key,
        wait_returned_empty=True,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "1.jsonl")
    lane = record["lanes"]["sec_rate"]  # type: ignore[index]
    assert lane["idle_type"] == "B"
    assert lane["pending_ready"] == ["ready_stage"]
    assert lane["pending_blocked"] == []


def test_busy_lane_idle_type_is_null(tmp_path: Path) -> None:
    """``in_flight > 0`` always classifies as null (busy)."""
    by_key = {"in_flight_stage": _stage("in_flight_stage", "sec_rate")}
    _emit_dispatcher_telemetry(
        run_id=1,
        iteration=1,
        statuses={"in_flight_stage": "pending"},  # still pending until completion applies
        in_flight_keys={"in_flight_stage"},
        lane_in_flight_count={"sec_rate": 1},
        caps=set(),
        by_key=by_key,
        wait_returned_empty=True,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "1.jsonl")
    assert record["lanes"]["sec_rate"]["idle_type"] is None  # type: ignore[index]


def test_completion_iteration_idle_type_is_null(tmp_path: Path) -> None:
    """``wait_returned_empty=False`` → no classification fires (None)."""
    by_key = {"ready_stage": _stage("ready_stage", "sec_rate")}
    _emit_dispatcher_telemetry(
        run_id=1,
        iteration=1,
        statuses={"ready_stage": "pending"},
        in_flight_keys=set(),
        lane_in_flight_count={"sec_rate": 0},
        caps=set(),
        by_key=by_key,
        wait_returned_empty=False,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "1.jsonl")
    # Lane appears (pending_ready non-empty) but idle_type is null
    # because a completion fired this iteration on some other lane.
    assert record["lanes"]["sec_rate"]["idle_type"] is None  # type: ignore[index]


def test_drained_lane_idle_type_is_null(tmp_path: Path) -> None:
    """Lane has nothing pending and nothing in flight → null (drained)."""
    _emit_dispatcher_telemetry(
        run_id=1,
        iteration=1,
        statuses={},
        in_flight_keys=set(),
        lane_in_flight_count={"sec_rate": 0},
        caps=set(),
        by_key={},
        wait_returned_empty=True,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "1.jsonl")
    assert record["lanes"]["sec_rate"]["idle_type"] is None  # type: ignore[index]


def test_per_lane_classification_isolated(tmp_path: Path) -> None:
    """Two lanes, different states — classified independently."""
    by_key = {
        "busy_one": _stage("busy_one", "init"),
        "ready_one": _stage("ready_one", "sec_rate"),
    }
    _emit_dispatcher_telemetry(
        run_id=1,
        iteration=1,
        statuses={"busy_one": "pending", "ready_one": "pending"},
        in_flight_keys={"busy_one"},
        lane_in_flight_count={"init": 1, "sec_rate": 0},
        caps=set(),
        by_key=by_key,
        wait_returned_empty=True,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "1.jsonl")
    assert record["lanes"]["init"]["idle_type"] is None  # type: ignore[index]
    assert record["lanes"]["sec_rate"]["idle_type"] == "B"  # type: ignore[index]


def test_ignores_pending_keys_outside_by_key(tmp_path: Path) -> None:
    """preexisting_statuses keys (not in ``by_key``) MUST be skipped —
    they belong to a prior dispatch and have no lane on this run.
    """
    by_key = {"alpha": _stage("alpha", "init")}
    _emit_dispatcher_telemetry(
        run_id=1,
        iteration=1,
        statuses={"alpha": "pending", "preexisting_orphan": "pending"},
        in_flight_keys=set(),
        lane_in_flight_count={"init": 0},
        caps=set(),
        by_key=by_key,
        wait_returned_empty=True,
        output_dir=tmp_path,
    )

    record = _read_single_record(tmp_path / "1.jsonl")
    assert "preexisting_orphan" not in record["lanes"]["init"]["pending_ready"]  # type: ignore[index]
    assert "preexisting_orphan" not in record["lanes"]["init"]["pending_blocked"]  # type: ignore[index]


# ---------------------------------------------------------------------------
# Safety: I/O failure MUST NOT raise into the dispatcher.
# ---------------------------------------------------------------------------


def test_emit_swallows_io_failure(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Pointing at a file path (not a directory) makes mkdir fail.
    The helper must log + return cleanly — the dispatcher must NEVER
    crash on telemetry I/O.
    """
    not_a_dir = tmp_path / "actually_a_file"
    not_a_dir.write_text("blocking the directory creation")

    by_key = {"alpha": _stage("alpha", "init")}
    with caplog.at_level(logging.WARNING):
        _emit_dispatcher_telemetry(
            run_id=99,
            iteration=1,
            statuses={"alpha": "pending"},
            in_flight_keys=set(),
            lane_in_flight_count={"init": 0},
            caps=set(),
            by_key=by_key,
            wait_returned_empty=True,
            output_dir=not_a_dir,
        )

    assert any("dispatcher telemetry write failed" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Aggregator (scripts/dispatcher_idle_analysis.py).
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")


def _make_record(iteration: int, lanes: dict[str, dict[str, object]]) -> dict[str, object]:
    return {
        "ts": f"2026-05-27T00:00:{iteration:02d}+00:00",
        "run_id": 17,
        "iteration": iteration,
        "wait_returned_empty": True,
        "lanes": lanes,
    }


def _lane_state(
    *,
    in_flight: int = 0,
    pending_ready: list[str] | None = None,
    pending_blocked: list[str] | None = None,
    idle_type: str | None = None,
) -> dict[str, object]:
    return {
        "in_flight": in_flight,
        "pending_ready": pending_ready or [],
        "pending_blocked": pending_blocked or [],
        "idle_type": idle_type,
    }


def test_aggregator_counts_busy_idle_a_idle_b(tmp_path: Path) -> None:
    from scripts.dispatcher_idle_analysis import aggregate

    records = [
        _make_record(1, {"sec_rate": _lane_state(in_flight=1)}),
        _make_record(2, {"sec_rate": _lane_state(pending_blocked=["x"], idle_type="A")}),
        _make_record(3, {"sec_rate": _lane_state(pending_ready=["y"], idle_type="B")}),
        _make_record(4, {"sec_rate": _lane_state(pending_ready=["y"], idle_type="B")}),
    ]
    result = aggregate(records)
    lane = result["lanes"]["sec_rate"]
    assert lane["busy_iter"] == 1
    assert lane["idle_a_iter"] == 1
    assert lane["idle_b_iter"] == 2
    assert lane["idle_b_max_run_iters"] == 2
    assert lane["idle_b_stages_seen"] == ["y"]
    assert result["run_id"] == 17
    assert result["total_iterations"] == 4


def test_aggregator_resets_b_run_on_busy_or_a(tmp_path: Path) -> None:
    """Consecutive run counter must reset whenever the lane stops
    classifying as ``"B"`` (busy, ``"A"``, or drained gap).
    """
    from scripts.dispatcher_idle_analysis import aggregate

    records = [
        _make_record(1, {"l": _lane_state(pending_ready=["s"], idle_type="B")}),
        _make_record(2, {"l": _lane_state(pending_ready=["s"], idle_type="B")}),
        _make_record(3, {"l": _lane_state(in_flight=1)}),
        _make_record(4, {"l": _lane_state(pending_ready=["s"], idle_type="B")}),
    ]
    result = aggregate(records)
    assert result["lanes"]["l"]["idle_b_max_run_iters"] == 2  # 2-run, then 1-run; max = 2


def test_aggregator_cli_round_trip(tmp_path: Path) -> None:
    """Invoke the CLI end-to-end with a temp JSONL file."""
    jsonl_path = tmp_path / "17.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            _make_record(1, {"db": _lane_state(in_flight=1)}),
            _make_record(2, {"db": _lane_state(pending_ready=["s2"], idle_type="B")}),
        ],
    )
    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "scripts/dispatcher_idle_analysis.py", str(jsonl_path)],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["lanes"]["db"]["busy_iter"] == 1
    assert payload["lanes"]["db"]["idle_b_iter"] == 1
    assert payload["lanes"]["db"]["idle_b_stages_seen"] == ["s2"]


def test_aggregator_rejects_malformed_jsonl(tmp_path: Path) -> None:
    from scripts.dispatcher_idle_analysis import _BadInput, _iter_records

    with pytest.raises(_BadInput, match="malformed JSONL"):
        list(_iter_records(["{not json"]))


def test_aggregator_cli_exits_2_on_missing_input(tmp_path: Path) -> None:
    """Documented exit code 2 for bad input (vs the default code-1 from
    a bare ``SystemExit(str)``). Codex 2 fold.
    """
    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "scripts/dispatcher_idle_analysis.py", str(tmp_path / "does_not_exist.jsonl")],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    assert "input not a file" in proc.stderr
