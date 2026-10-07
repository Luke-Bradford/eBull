"""#3609 step 2's report run: the binding, the freeze re-checks and ledger steps 5 and 6 around ``evaluate``.

Fixtures only: nothing here reads stage-B data or the database.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest

import scripts.report_3609_step2_run as run_module
from app.services import factor_book
from app.services.factor_book import BookRefusal
from app.services.factor_book_declaration import TRIAL_ID, CodeHashes, payload_sha256
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
from scripts.report_3609_step2_assembly import LABELS, SURVIVORSHIP, Report
from scripts.report_3609_step2_run import REFUSED, run_report
from tests.test_3609_step2_assembly import UNIVERSE, _evaluate, _panel

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

    def run(self, evaluate_run: Callable[[Binding], Any], run: str = CAPTURE) -> run_module.ReportOutcome:
        return run_report(
            run,
            head=HEAD,
            register=REGISTER,
            evaluate_run=evaluate_run,
            out_dir=self.out,
            ledger=self.local,
            committed_ledger=self.committed,
            current=lambda: HASHES,
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

    def evaluate_run(binding: Binding) -> _Report:
        assert binding == Binding(CAPTURE, FROZEN)
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

    def evaluate_run(_: Binding) -> Report:
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
        ledgers.run(lambda _: pytest.fail("evaluated past a refused gate"))
    assert not ledgers.local.exists() and not ledgers.out.exists()


def test_a_failure_past_report_started_ends_the_run_failed_with_no_output(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)

    def evaluate_run(_: Binding) -> Report:
        raise ZeroDivisionError("a bug, not a refusal")

    with pytest.raises(ZeroDivisionError):
        ledgers.run(evaluate_run)
    assert [row["event"] for row in ledgers.events()] == ["report_started", "failed"]
    assert ledgers.events()[-1]["step"] == "report"
    assert not ledgers.out.exists()
    # The run has ended: a second report under its id refuses at the gate.
    with pytest.raises(BookRefusal, match=LEDGER_MISMATCH):
        ledgers.run(lambda _: pytest.fail("evaluated an ended run"))


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
        ledgers.run(lambda _: _Report(_Verdict("PASS", None)))
    assert [row["event"] for row in ledgers.events()] == ["report_started", "failed"]
    assert list(ledgers.out.iterdir()) == []


@pytest.mark.usefixtures("stub_payload")
def test_an_existing_output_file_is_never_overwritten(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path, COMMITTED)
    ledgers.out.mkdir()
    (ledgers.out / f"{CAPTURE}-report.json").write_text("an earlier file")
    with pytest.raises(FileExistsError):
        ledgers.run(lambda _: _Report(_Verdict("PASS", None)))
    assert (ledgers.out / f"{CAPTURE}-report.json").read_text() == "an earlier file"
    assert ledgers.events()[-1]["event"] == "failed"


def test_a_real_report_round_trips_through_the_output_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(factor_book, "UNIVERSE_SIZE", UNIVERSE)
    report = _evaluate(_panel())
    outcome = _Ledgers(tmp_path, COMMITTED).run(lambda _: report)
    body = json.loads(outcome.path.read_bytes())
    assert (outcome.status, body["verdict"]["status"]) == (report.verdict.status, report.verdict.status)
    assert {"path", "operations", "construction", "labels"} <= set(body)
