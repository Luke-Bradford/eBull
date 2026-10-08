"""#3609 step 2's report run: the binding, the freeze re-checks and ledger steps 5 and 6 around ``evaluate``.

Fixtures only: nothing here reads stage-B data or the database.
"""

from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import gzip
import hashlib
import json
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pytest

import scripts.capture_3609_step2 as capture_module
import scripts.report_3609_step2_inputs as inputs
import scripts.report_3609_step2_run as run_module
from app.services import factor_book
from app.services.factor_book import BookRefusal
from app.services.factor_book_declaration import TRIAL_ID, CodeHashes, DeclarationError, payload_sha256
from app.services.factor_book_ledger import (
    CAPTURE_AMBIGUOUS,
    COMMITTED_LEDGER_PATH,
    DATA_FROZEN_EVENT,
    DECLARATION_MISMATCH,
    LEDGER_MISMATCH,
    Binding,
    check_report_gate,
)
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.trial_register import DeclaredTrial, TrialExactness, TrialRegister
from scripts.build_3609_factor_panel import VerifiedArtefact
from scripts.report_3609_baselines import COSTS
from scripts.report_3609_step2 import CONSUMED_INPUTS, STAGE_A_MANIFEST_SHA256, TABLE9_SIGNS, PanelMonth, ReportError
from scripts.report_3609_step2_assembly import LABELS, SURVIVORSHIP, Report
from scripts.report_3609_step2_inputs import Step0
from scripts.report_3609_step2_run import REFUSED, run_report, stage_cutoffs
from scripts.report_3609_step2_universe import NYSE_CUTOFFS
from tests.test_3609_step2_assembly import UNIVERSE, _evaluate, _panel
from tests.test_3609_step2_report_inputs import _evidence as _inputs_evidence

CAPTURE = "a" * 32
REUSE = "b" * 32
FROZEN = "f" * 64
HEAD = "c" * 40
HASHES = CodeHashes(spec_sha256="1" * 64, construction_sha256="2" * 64, register_policy_sha256="3" * 64, python="3.14")
TRIAL = DeclaredTrial(
    trial_id=TRIAL_ID,
    description="#3609 step 2 factor book",
    evidence="; ".join(f"{label}={value}" for label, value in HASHES.by_label().items()),
    exactness=TrialExactness.EXACT,
)
REGISTER = TrialRegister("r", (TRIAL,))
DECLARED = {"event": "declared", "trial_id": TRIAL_ID, "payload_sha256": payload_sha256(TRIAL)}


def _started(run: str, hashes: CodeHashes = HASHES) -> dict[str, Any]:
    return {"run_id": run, "event": "started", **hashes.by_label()}


def _access(run: str) -> dict[str, Any]:
    return {"run_id": run, "event": "access_recorded", "access_id": 7}


def _frozen(run: str = CAPTURE, digest: str = FROZEN, **row: Any) -> dict[str, Any]:
    return {"run_id": run, "event": DATA_FROZEN_EVENT, "trial_id": TRIAL_ID, "manifest_sha256": digest, **row}


def _capture(run: str = CAPTURE) -> list[dict[str, Any]]:
    """A capturing attempt's rows through ``data_frozen``, as merged on ``main`` before any report."""
    return [
        _started(run),
        _access(run),
        {"run_id": run, "event": "sub_published", "manifest_sha256": "4" * 64},
        {"run_id": run, "event": "stage_b_published", "manifest_sha256": "5" * 64},
        _frozen(run),
    ]


def _reuse(run: str = REUSE, capturing: str = CAPTURE, digest: str = FROZEN) -> list[dict[str, Any]]:
    return [
        _started(run),
        _access(run),
        {"run_id": run, "event": "capture_reused", "capturing_run_id": capturing, "manifest_sha256": digest},
    ]


COMMITTED = [DECLARED, *_capture()]


def _gate(
    committed: list[dict[str, Any]], local: Iterable[dict[str, Any]] = (), run: str = CAPTURE, **kwargs: Any
) -> tuple[DeclaredTrial, Binding]:
    return check_report_gate(
        kwargs.get("register", REGISTER), committed, [*committed, *local], run, kwargs.get("current", HASHES)
    )


# --------------------------------------------------------------------------- the gate


def test_the_capturing_attempt_and_a_reusing_attempt_bind_the_one_committed_capture() -> None:
    assert _gate(COMMITTED) == (TRIAL, Binding(CAPTURE, FROZEN))
    assert _gate(COMMITTED, _reuse(), run=REUSE) == (TRIAL, Binding(CAPTURE, FROZEN))
    # Another trial's capture is not this one's.
    assert _gate([*COMMITTED, _frozen("d" * 32, trial_id="other-v1")])[1] == Binding(CAPTURE, FROZEN)


@pytest.mark.parametrize(
    ("committed", "local"),
    [
        pytest.param([DECLARED, *_capture()[:-1]], [_frozen()], id="capture-only-in-the-local-ledger"),
        pytest.param([*COMMITTED, _frozen("d" * 32, "e" * 64)], [], id="a-second-committed-capture"),
        pytest.param([*COMMITTED, {k: v for k, v in _frozen().items() if k != "trial_id"}], [], id="an-unnamed-row"),
        pytest.param([*COMMITTED, _frozen("d" * 32, trial_id=None)], [], id="a-null-trial"),
        pytest.param([*COMMITTED, _frozen("d" * 32, trial_id="")], [], id="an-empty-trial"),
        pytest.param([DECLARED, *_capture()[:-1], _frozen(digest="")], [], id="a-row-without-its-sha256"),
    ],
)
def test_anything_but_exactly_one_committed_capture_for_the_trial_is_ambiguous(
    committed: list[dict[str, Any]], local: list[dict[str, Any]]
) -> None:
    with pytest.raises(BookRefusal, match=CAPTURE_AMBIGUOUS):
        _gate(committed, local)


@pytest.mark.parametrize(
    ("local", "run", "message"),
    [
        pytest.param([], REUSE, "no single leading 'started'", id="no-started-row"),
        pytest.param([{"run_id": CAPTURE, "event": "report_started"}], CAPTURE, "already written", id="reported"),
        pytest.param([{"run_id": CAPTURE, "event": "failed"}], CAPTURE, "already ended", id="ended"),
        pytest.param(_reuse(CAPTURE)[2:], CAPTURE, "also wrote", id="capturer-also-reused"),
        pytest.param(_reuse()[:2], REUSE, "found 0", id="reuse-without-its-row"),
        pytest.param([*_reuse(), _frozen(REUSE, "e" * 64)], REUSE, "no capture rows", id="reuse-with-own-capture"),
        pytest.param(_reuse(digest="e" * 64), REUSE, "not the binding", id="reuse-of-another-sha256"),
        pytest.param(_reuse(capturing="d" * 32), REUSE, "not the binding", id="reuse-of-another-run"),
    ],
)
def test_an_attempt_whose_rows_do_not_tie_it_to_the_binding_refuses(
    local: list[dict[str, Any]], run: str, message: str
) -> None:
    with pytest.raises(BookRefusal, match=f"{LEDGER_MISMATCH}.*{message}"):
        _gate(COMMITTED, local, run=run)


def test_the_freeze_is_rechecked_at_head_against_the_row_and_the_attempts_started_row() -> None:
    edited = dataclasses.replace(TRIAL, description="edited after the freeze")
    with pytest.raises(BookRefusal, match=f"{DECLARATION_MISMATCH}.*payload"):
        _gate(COMMITTED, register=TrialRegister("r", (edited,)))
    with pytest.raises(BookRefusal, match=f"{DECLARATION_MISMATCH}.*construction_sha256"):
        _gate(COMMITTED, current=dataclasses.replace(HASHES, construction_sha256="9" * 64))
    # The checkout still matches the row, but the attempt began under other code: it cannot report under that id.
    for label in HASHES.by_label():
        began = dataclasses.replace(HASHES, **{label: "9"})
        committed = [DECLARED, _started(CAPTURE, began), *_capture()[1:]]
        with pytest.raises(BookRefusal, match=f"{DECLARATION_MISMATCH}.*{label}"):
            _gate(committed)


def test_the_declared_pin_is_read_from_the_committed_ledger_only() -> None:
    with pytest.raises(BookRefusal, match=f"{DECLARATION_MISMATCH}.*0 'declared'"):
        _gate(_capture(), [DECLARED])


def test_the_committed_ledger_never_holds_two_captures_or_an_unnamed_one() -> None:
    """§"Slices", capture lifecycle: a second ``data_frozen`` row fails at its own push or CI, never at the report."""
    frozen = [row for row in read_ledger(COMMITTED_LEDGER_PATH) if row.get("event") == DATA_FROZEN_EVENT]
    assert all("trial_id" in row for row in frozen)
    trials = [row["trial_id"] for row in frozen]
    assert len(trials) == len(set(trials)), f"more than one {DATA_FROZEN_EVENT!r} row for a trial: {trials}"


# --------------------------------------------------------------------------- the run


@dataclasses.dataclass(frozen=True)
class _Verdict:
    status: str
    reason: str | None


@dataclasses.dataclass(frozen=True)
class _Report:
    verdict: _Verdict


class _Ledgers:
    def __init__(self, root: Path, committed: list[dict[str, Any]], local: Iterable[dict[str, Any]] = ()) -> None:
        self.root = root
        self.committed = root / "committed.jsonl"
        self.local = root / "local.jsonl"
        self.out = root / "out"
        for row in committed:
            append_ledger(self.committed, row)
        for row in local:
            append_ledger(self.local, row)

    def run(
        self,
        evaluate_run: Callable[[DeclaredTrial, Binding], Any],
        run: str = CAPTURE,
        current: Callable[[], CodeHashes] = lambda: HASHES,
    ) -> run_module.ReportOutcome:
        return run_report(
            run,
            head=HEAD,
            register=REGISTER,
            evaluate_run=evaluate_run,
            out_dir=self.out,
            ledger=self.local,
            committed_ledger=self.committed,
            current=current,
        )

    def events(self, run: str = CAPTURE) -> list[Mapping[str, Any]]:
        return [row for row in read_ledger(self.local) if row.get("run_id") == run]


@pytest.fixture
def stub_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_module, "payload", lambda report: {"verdict": {"status": report.verdict.status}})


@pytest.mark.usefixtures("stub_payload")
def test_a_run_writes_report_started_then_the_output_then_completed_naming_its_sha256(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED, _reuse())
    seen: list[list[str]] = []

    def evaluate_run(trial: DeclaredTrial, binding: Binding) -> _Report:
        assert (trial, binding) == (TRIAL, Binding(CAPTURE, FROZEN))
        seen.append([row["event"] for row in ledgers.events(REUSE)])  # what the ledger held when inputs were read
        return _Report(_Verdict("FAIL", "G1"))

    outcome = ledgers.run(evaluate_run, run=REUSE)
    assert seen == [["started", "access_recorded", "capture_reused", "report_started"]]
    started, completed = ledgers.events(REUSE)[-2:]
    assert (started["event"], started["git_sha"], started["capturing_run_id"], started["manifest_sha256"]) == (
        "report_started",
        HEAD,
        CAPTURE,
        FROZEN,
    )
    document = outcome.path.read_bytes()
    assert hashlib.sha256(document).hexdigest() == outcome.sha256 == completed["output_sha256"]
    assert (completed["event"], completed["status"], completed["reason"], completed["output"]) == (
        "completed",
        "FAIL",
        "G1",
        str(outcome.path),
    )
    body = json.loads(document)
    assert body["run"] == {
        "run_id": REUSE,
        "trial_id": TRIAL_ID,
        "git_sha": HEAD,
        "capturing_run_id": CAPTURE,
        "data_frozen_sha256": FROZEN,
        **HASHES.by_label(),
    }
    # Canonical JSON (§"Registration"): sorted keys, no whitespace.
    assert document == json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def test_a_refusal_from_the_evaluation_completes_refused_with_the_refusal_payload_only(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)

    def evaluate_run(*_: object) -> Report:
        raise BookRefusal("WEALTH_NONPOSITIVE", "formation 2022-03-31 draw 4")

    outcome = ledgers.run(evaluate_run)
    assert (outcome.status, outcome.reason) == (REFUSED, "WEALTH_NONPOSITIVE")
    assert ledgers.events()[-1]["status"] == REFUSED
    body = json.loads(outcome.path.read_bytes())
    assert set(body) == {"run", "verdict", "labels"}
    assert body["labels"] == dict(LABELS)
    assert body["verdict"]["line"] == f"REFUSED: WEALTH_NONPOSITIVE. {SURVIVORSHIP}."
    assert "draw 4" in body["verdict"]["detail"]


def test_a_gate_refusal_writes_no_row_and_reads_nothing(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, [DECLARED, *_capture()[:-1]])
    with pytest.raises(BookRefusal, match=CAPTURE_AMBIGUOUS):
        ledgers.run(lambda *_: pytest.fail("evaluated past a refused gate"))
    assert not ledgers.local.exists() and not ledgers.out.exists()


def test_a_failure_past_report_started_ends_the_run_failed_with_no_output(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)

    def evaluate_run(*_: object) -> Report:
        raise ZeroDivisionError("a bug, not a refusal")

    with pytest.raises(ZeroDivisionError):
        ledgers.run(evaluate_run)
    assert [row["event"] for row in ledgers.events()] == ["report_started", "failed"]
    assert ledgers.events()[-1]["step"] == "report"
    assert not ledgers.out.exists()
    # The run has ended: a second report under its id refuses at the gate.
    with pytest.raises(BookRefusal, match=LEDGER_MISMATCH):
        ledgers.run(lambda *_: pytest.fail("evaluated an ended run"))


@pytest.mark.usefixtures("stub_payload")
def test_an_output_the_ledger_cannot_name_is_deleted_and_the_run_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)

    def append(path: Path, row: Mapping[str, Any]) -> None:
        if row["event"] == "completed":
            raise OSError("disk full")
        append_ledger(path, row)

    monkeypatch.setattr(run_module, "append_ledger", append)
    with pytest.raises(OSError, match="disk full"):
        ledgers.run(lambda *_: _Report(_Verdict("PASS", None)))
    assert [row["event"] for row in ledgers.events()] == ["report_started", "failed"]
    assert list(ledgers.out.iterdir()) == []


def test_a_payload_holding_its_own_run_block_fails_rather_than_being_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)
    monkeypatch.setattr(run_module, "payload", lambda _: {"run": {"run_id": "forged"}})
    with pytest.raises(ValueError, match="'run' block"):
        ledgers.run(lambda *_: _Report(_Verdict("PASS", None)))
    assert ledgers.events()[-1]["event"] == "failed" and not ledgers.out.exists()


@pytest.mark.usefixtures("stub_payload")
def test_code_that_moves_during_the_evaluation_ends_the_run_failed(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)
    taken = iter([HASHES, dataclasses.replace(HASHES, construction_sha256="9" * 64)])
    with pytest.raises(DeclarationError, match="moved during the run"):
        ledgers.run(lambda *_: _Report(_Verdict("PASS", None)), current=lambda: next(taken))
    assert [row["event"] for row in ledgers.events()] == ["report_started", "failed"]
    assert not ledgers.out.exists()


@pytest.mark.usefixtures("stub_payload")
def test_an_existing_output_file_is_never_overwritten(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)
    ledgers.out.mkdir()
    (ledgers.out / f"{CAPTURE}-report.json").write_text("an earlier file")
    with pytest.raises(FileExistsError):
        ledgers.run(lambda *_: _Report(_Verdict("PASS", None)))
    assert (ledgers.out / f"{CAPTURE}-report.json").read_text() == "an earlier file"
    assert ledgers.events()[-1]["event"] == "failed"


def test_a_real_report_round_trips_through_the_output_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(factor_book, "UNIVERSE_SIZE", UNIVERSE)
    report = _evaluate(_panel())
    outcome = _Ledgers(tmp_path, COMMITTED).run(lambda *_: report)
    body = json.loads(outcome.path.read_bytes())
    assert (outcome.status, body["verdict"]["status"]) == (report.verdict.status, report.verdict.status)
    assert {"path", "operations", "construction", "labels"} <= set(body)


@pytest.mark.usefixtures("stub_payload")
def test_the_gate_and_report_started_run_under_one_exclusive_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two concurrent reports of one run cannot both pass the gate: the claim is held from the read to the append."""
    ledgers = _Ledgers(tmp_path, COMMITTED)
    held: list[bool] = []
    real_gate = run_module.check_report_gate

    def gate(*args: Any) -> Any:
        with (tmp_path / "local.jsonl.report.lock").open("a") as other:
            try:
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                held.append(True)
            else:
                fcntl.flock(other, fcntl.LOCK_UN)
                held.append(False)
        return real_gate(*args)

    monkeypatch.setattr(run_module, "check_report_gate", gate)
    ledgers.run(lambda *_: _Report(_Verdict("PASS", None)))
    assert held == [True]


@pytest.mark.usefixtures("stub_payload")
def test_an_output_whose_write_fails_after_creation_is_removed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)
    real_open = os.open

    def open_(path: Any, flags: int, *args: Any) -> int:
        if Path(path) == ledgers.out:  # the directory fsync, after the file was created and written
            raise OSError("directory fsync failed")
        return real_open(path, flags, *args)

    monkeypatch.setattr(capture_module.os, "open", open_)
    with pytest.raises(OSError, match="directory fsync"):
        ledgers.run(lambda *_: _Report(_Verdict("PASS", None)))
    assert list(ledgers.out.iterdir()) == []
    assert [row["event"] for row in ledgers.events()] == ["report_started", "failed"]


# --------------------------------------------------------------------------- the evaluation's inputs

A_FIRST, A_LAST, B_FIRST = date(2014, 9, 30), date(2021, 4, 30), date(2021, 5, 31)
B1_CLOSE = "197.02"
PINNED = _inputs_evidence(**{inputs.B1_CLOSE_LABEL: B1_CLOSE})


def _month(formation: date, session: date | None = None) -> PanelMonth:
    return PanelMonth(formation, session or formation, {}, {}, {})


def _cutoff_bytes(rows: Mapping[date, float]) -> bytes:
    lines = [json.dumps(["nyse_p50", day.isoformat(), str(value), "usd_millions"]) for day, value in rows.items()]
    lines.append(json.dumps(["nyse_p20", A_FIRST.isoformat(), "1", "usd_millions"]))  # another series: ignored
    return gzip.compress("\n".join(lines).encode())


def _verified(stage: str, cutoffs: Mapping[date, float], table9: str = "9" * 64) -> VerifiedArtefact:
    return VerifiedArtefact({"stage": stage, "inputs": {TABLE9_SIGNS: table9}}, {NYSE_CUTOFFS: _cutoff_bytes(cutoffs)})


class _Inputs:
    """Stands in for every reader past the pins; records each call, in order."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, stage_b_table9: str = "9" * 64) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.panels = {stage: [_month(m) for m in grid] for stage, grid in zip("AB", run_module.STAGE_GRIDS)}
        stage_a = _verified("A", {A_FIRST: 2000.0, A_LAST: 3000.0, B_FIRST: 3100.0})
        stage_b = _verified("B", {A_LAST: 3000.0, B_FIRST: 3100.0}, stage_b_table9)
        self.step0 = Step0(manifest={}, b1_saved={"months": []}, factor_snapshots={"five": 39, "mom": 40})
        self.factors = {"Mkt-RF": {(2014, 10): 0.01}}

        def read_artefact(path: Path, digest: str, *, keep: Iterable[str]) -> VerifiedArtefact:
            self.calls.append(("stage_a", (path, digest, tuple(keep))))
            return stage_a

        def verify(path: Path, digest: str, keep: Iterable[str]) -> Any:
            self.calls.append(("capture", (path, digest, tuple(keep))))
            return dataclasses.make_dataclass("C", ["stage_b"])(stage_b)

        def panel(verified: VerifiedArtefact, ff12: object) -> list[PanelMonth]:
            self.calls.append(("panel", verified.manifest["stage"]))
            return self.panels[verified.manifest["stage"]]

        def step0(digest: str, run: Path) -> Step0:
            self.calls.append(("step0", (digest, run)))
            return self.step0

        def factors(conn: object, ids: Mapping[str, int], pins: Any) -> Any:
            self.calls.append(("factors", (conn, ids, pins)))
            return self.factors

        def evaluate(panel: list[PanelMonth], **kw: Any) -> str:
            self.calls.append(("evaluate", (panel, kw)))
            return "report"

        for name, fake in [
            ("read_verified_artefact", read_artefact),
            ("verify_capture", verify),
            ("read_panel", panel),
            ("read_step0", step0),
            ("read_factors", factors),
            ("evaluate", evaluate),
            ("load_ff12", lambda: "ff12"),
        ]:
            monkeypatch.setattr(run_module, name, fake)

    def run(self, tmp_path: Path, evidence: str = PINNED) -> Any:
        @contextlib.contextmanager
        def connect() -> Iterator[str]:
            self.calls.append(("connect", None))
            yield "conn"

        return run_module.evaluate_run(
            dataclasses.replace(TRIAL, evidence=evidence),
            Binding(CAPTURE, FROZEN),
            connect=connect,  # type: ignore[arg-type]
            stage_a=tmp_path / "stage_a",
            capture_root=tmp_path / "captures",
            step0_run=tmp_path / "step0",
        )


def test_evaluate_run_reads_every_input_against_its_pin_and_evaluates_both_stages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes = _Inputs(monkeypatch)
    assert fakes.run(tmp_path) == "report"
    keep = (*CONSUMED_INPUTS, NYSE_CUTOFFS)
    pins = inputs.declared_pins(PINNED)
    names = [name for name, _ in fakes.calls]
    assert names == ["stage_a", "capture", "panel", "panel", "step0", "connect", "factors", "evaluate"]
    calls = dict(fakes.calls)
    assert calls["stage_a"] == (tmp_path / "stage_a", STAGE_A_MANIFEST_SHA256, keep)
    assert calls["capture"] == (tmp_path / "captures" / f"{CAPTURE}.json", FROZEN, keep)
    assert calls["step0"] == ("0" * 64, tmp_path / "step0")
    assert calls["factors"] == ("conn", fakes.step0.factor_snapshots, pins.snapshots)
    panel, kw = calls["evaluate"]
    assert panel == [*fakes.panels["A"], *fakes.panels["B"]]
    assert kw == {
        "factors": fakes.factors,
        # Each formation's cutoff from its own stage's snapshot.
        "cutoffs": {A_FIRST: 2000e6, A_LAST: 3000e6, B_FIRST: 3100e6},
        "b1_saved": fakes.step0.b1_saved,
        "b1_close": 197.02,
        "costs": COSTS,
    }


@pytest.mark.parametrize(
    "evidence",
    [
        pytest.param(_inputs_evidence(), id="no-b1-close"),
        pytest.param(_inputs_evidence(**{inputs.B1_CLOSE_LABEL: "NaN"}), id="a-non-finite-b1-close"),
        pytest.param(_inputs_evidence(**{inputs.STAGE_A_LABEL: "e" * 64, inputs.B1_CLOSE_LABEL: B1_CLOSE}), id="a-pin"),
    ],
)
def test_a_declaration_without_its_pins_refuses_before_any_input_is_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, evidence: str
) -> None:
    fakes = _Inputs(monkeypatch)
    with pytest.raises((DeclarationError, ReportError)):
        fakes.run(tmp_path, evidence)
    assert fakes.calls == []


def test_a_stage_b_artefact_whose_table9_is_not_the_declared_one_refuses_before_any_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes = _Inputs(monkeypatch, stage_b_table9="8" * 64)
    with pytest.raises(ReportError, match="Table 9"):
        fakes.run(tmp_path)
    assert [name for name, _ in fakes.calls] == ["stage_a", "capture"]


def test_a_stage_a_not_starting_at_the_b1_close_session_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes = _Inputs(monkeypatch)
    fakes.panels["A"][0] = _month(A_FIRST, session=date(2014, 9, 29))
    with pytest.raises(ReportError, match="B1's declared close"):
        fakes.run(tmp_path)
    assert "step0" not in dict(fakes.calls)


def test_stage_cutoffs_take_each_formation_from_its_own_stage() -> None:
    a, b = [_month(A_FIRST), _month(A_LAST)], [_month(B_FIRST)]
    # Stage B's snapshot publishing an earlier date stage A lacks does not reach a stage-A formation.
    assert stage_cutoffs([(a, {A_LAST: 3.0}), (b, {A_FIRST: 2.0, B_FIRST: 4.0})]) == {A_LAST: 3.0, B_FIRST: 4.0}


def test_stage_cutoffs_that_disagree_on_a_shared_date_refuse() -> None:
    a, b = [_month(A_LAST)], [_month(B_FIRST)]
    with pytest.raises(BookRefusal, match="CUTOFF_INVALID"):
        stage_cutoffs([(a, {A_LAST: 3.0, B_FIRST: 4.0}), (b, {B_FIRST: 4.5})])


@pytest.mark.parametrize(
    ("stage", "change"),
    [
        pytest.param("B", lambda panel: panel.clear(), id="an-empty-stage-b"),
        pytest.param("B", lambda panel: panel.pop(), id="a-stage-b-short-of-2024-07"),
        pytest.param("A", lambda panel: panel.pop(3), id="a-stage-a-missing-a-month"),
    ],
)
def test_a_stage_whose_formations_are_not_its_grid_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str, change: Callable[[list[PanelMonth]], object]
) -> None:
    fakes = _Inputs(monkeypatch)
    change(fakes.panels[stage])
    with pytest.raises(ReportError, match=f"stage {stage}'s formations"):
        fakes.run(tmp_path)
    assert "step0" not in dict(fakes.calls)


# --------------------------------------------------------------------------- the command line


class _Git:
    def __init__(self, *, status: str = "", head: str = HEAD, merged: str = HEAD) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.answers = {"status": status, "HEAD": head, "origin/main": merged}

    def __call__(self, *args: str) -> str:
        self.calls.append(args)
        return self.answers.get(args[-1], self.answers.get(args[0], ""))


def test_report_head_fetches_then_requires_a_clean_checkout_at_origin_main() -> None:
    git = _Git()
    assert run_module.report_head(git) == HEAD
    assert git.calls[0] == ("fetch", "--quiet", "origin", "main")


@pytest.mark.parametrize(
    ("git", "match"),
    [
        pytest.param(_Git(status="?? stray.txt"), "untracked", id="dirty"),
        pytest.param(_Git(merged="d" * 40), "is not origin/main", id="behind-or-ahead"),
    ],
)
def test_report_head_refuses_a_dirty_checkout_or_one_not_at_origin_main(git: _Git, match: str) -> None:
    with pytest.raises(ReportError, match=match):
        run_module.report_head(git)


def _outcome(tmp_path: Path, document: bytes, digest: str | None = None) -> run_module.ReportOutcome:
    path = tmp_path / "report.json"
    path.write_bytes(document)
    return run_module.ReportOutcome("FAIL", "G1", path, digest or hashlib.sha256(document).hexdigest())


def test_main_prints_only_after_the_run_has_ended(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    outcome = _outcome(tmp_path, json.dumps({"verdict": {"line": "FAIL: G1. survivorship"}}).encode())
    seen: dict[str, Any] = {}

    def run_report_(run_id: str, **kw: Any) -> run_module.ReportOutcome:
        seen.update(kw, run_id=run_id, printed=capsys.readouterr().out)
        return outcome

    monkeypatch.setattr(run_module, "report_head", lambda: HEAD)
    monkeypatch.setattr(run_module, "run_report", run_report_)
    assert run_module.main(["--run-id", REUSE]) == 0
    assert seen["printed"] == ""
    assert (seen["run_id"], seen["head"], seen["evaluate_run"]) == (REUSE, HEAD, run_module.evaluate_run)
    # The gate reads the run ledger and committed ledger at their defaults only.
    assert not {"ledger", "committed_ledger"} & set(seen)
    line, summary = capsys.readouterr().out.splitlines()
    assert line == "FAIL: G1. survivorship"
    assert json.loads(summary) == {
        "run_id": REUSE,
        "status": "FAIL",
        "reason": "G1",
        "output": str(outcome.path),
        "output_sha256": outcome.sha256,
    }


def test_main_prints_a_gate_refusal_and_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def refuse(*_: Any, **__: Any) -> Any:
        raise BookRefusal(CAPTURE_AMBIGUOUS, "0 committed rows")

    monkeypatch.setattr(run_module, "report_head", lambda: HEAD)
    monkeypatch.setattr(run_module, "run_report", refuse)
    assert run_module.main(["--run-id", REUSE]) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == CAPTURE_AMBIGUOUS


def test_main_refuses_an_output_that_is_not_the_file_the_ledger_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    outcome = _outcome(tmp_path, b'{"verdict":{"line":"PASS"}}', digest="0" * 64)
    monkeypatch.setattr(run_module, "report_head", lambda: HEAD)
    monkeypatch.setattr(run_module, "run_report", lambda *_, **__: outcome)
    with pytest.raises(ReportError, match="completed row names"):
        run_module.main(["--run-id", REUSE])
    assert capsys.readouterr().out == ""


def test_main_reads_no_ledger_from_a_checkout_that_is_not_origin_main(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse() -> str:
        raise ReportError("HEAD is not origin/main")

    monkeypatch.setattr(run_module, "report_head", refuse)
    monkeypatch.setattr(run_module, "run_report", lambda *_, **__: pytest.fail("ran from a stale checkout"))
    with pytest.raises(ReportError):
        run_module.main(["--run-id", REUSE])


def test_main_offers_no_flag_that_redirects_a_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ledger path outside the checkout ``report_head`` vouched for could hold rows the checkout never had."""
    monkeypatch.setattr(run_module, "report_head", lambda: pytest.fail("parsed past an unknown flag"))
    with pytest.raises(SystemExit):
        run_module.main(["--run-id", REUSE, "--ledger", "/tmp/other.jsonl"])


@pytest.mark.parametrize("document", [b"not json", b"[]", b'{"verdict": {}}'])
def test_main_refuses_a_named_output_without_a_verdict_line_as_a_report_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], document: bytes
) -> None:
    outcome = _outcome(tmp_path, document)
    monkeypatch.setattr(run_module, "report_head", lambda: HEAD)
    monkeypatch.setattr(run_module, "run_report", lambda *_, **__: outcome)
    with pytest.raises(ReportError, match="no verdict line"):
        run_module.main(["--run-id", REUSE])
    assert capsys.readouterr().out == ""
