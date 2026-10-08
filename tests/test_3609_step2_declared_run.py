"""#3609 step 2 slice 5: the declared run's ``started`` and ``access_recorded`` rows and its step sequence.

Fixtures only: nothing here reads stage-B data or the database.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import app.services.factor_book_ledger as factor_book_ledger
import scripts.run_3609_step2 as declared_run
from app.services.factor_book import BookRefusal
from app.services.factor_book_declaration import TRIAL_ID, DeclarationError, payload_sha256
from app.services.factor_book_ledger import (
    CAPTURE_AMBIGUOUS,
    DATA_FROZEN_EVENT,
    STRATEGY_ID,
    STRATEGY_VERSION,
    StageBAccessError,
    access_purpose,
    check_report_gate,
    recorded_access_id,
)
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.result_ledger import HoldoutAccess
from app.services.trial_register import TRIAL_REGISTER_VERSION, TrialRegister
from tests.test_3609_step2_stage_b_builder import HASHES, RUN, _declared, _trial

HEAD = "c" * 40
COMMAND = ["python", "-m", "scripts.run_3609_step2", "--accessed-by", "loop"]


class _Ledgers:
    def __init__(self, tmp_path: Path) -> None:
        self.trial = _trial()
        self.register = TrialRegister("r", (self.trial,))
        self.ledger = tmp_path / "ledger.jsonl"
        self.committed = tmp_path / "committed.jsonl"
        self.accesses: list[HoldoutAccess] = []
        append_ledger(self.committed, _declared(self.trial))

    def record(self, access: HoldoutAccess) -> int:
        self.accesses.append(access)
        return 41

    def start(
        self,
        run_id: str = RUN,
        *,
        record: Callable[[HoldoutAccess], int] | None = None,
        current: Callable[[], Any] = lambda: HASHES,
        reuse: bool = False,
    ) -> int:
        return declared_run.start_run(
            run_id,
            head=HEAD,
            command=COMMAND,
            accessed_by="loop",
            reuse=reuse,
            record_access=record or self.record,
            register=self.register,
            ledger=self.ledger,
            committed_ledger=self.committed,
            current=current,
        )

    def events(self, run_id: str = RUN) -> list[str]:
        return [row["event"] for row in read_ledger(self.ledger) if row.get("run_id") == run_id]


# --------------------------------------------------------------------------- start


def test_start_writes_started_then_the_committed_evaluate_access(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    assert ledgers.start() == 41
    started, recorded = read_ledger(ledgers.ledger)
    assert {k: started[k] for k in started if k != "at"} == {
        "run_id": RUN,
        "event": "started",
        "git_sha": HEAD,
        **HASHES.by_label(),
        "payload_sha256": payload_sha256(ledgers.trial),
        "register_version": TRIAL_REGISTER_VERSION,
        "command": COMMAND,
    }
    assert {k: recorded[k] for k in recorded if k != "at"} == {
        "run_id": RUN,
        "event": "access_recorded",
        "access_id": 41,
    }
    assert ledgers.accesses == [
        HoldoutAccess(
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            access_kind="evaluate",
            accessed_by="loop",
            purpose=access_purpose(RUN),
            result_version=RUN,
        )
    ]
    # The rows are the ones every stage-B gate reads: the SUB publisher's goes through.
    rows = read_ledger(ledgers.committed, ledgers.ledger)
    assert recorded_access_id(rows, RUN, before="sub_published") == 41


def test_the_started_row_satisfies_the_reports_comparison(tmp_path: Path) -> None:
    """``check_report_gate`` compares this checkout's four values with the ``started`` row's under the same labels."""
    ledgers = _Ledgers(tmp_path)
    ledgers.start()
    append_ledger(
        ledgers.ledger,
        {"run_id": RUN, "event": DATA_FROZEN_EVENT, "trial_id": TRIAL_ID, "manifest_sha256": "f" * 64},
    )
    for row in read_ledger(ledgers.ledger):  # the run's rows through ``data_frozen``, merged on ``main``
        append_ledger(ledgers.committed, row)
    rows = read_ledger(ledgers.committed, ledgers.ledger)
    committed = read_ledger(ledgers.committed)
    trial, binding = check_report_gate(ledgers.register, committed, rows, RUN, HASHES)
    assert trial is ledgers.trial and binding.capturing_run_id == RUN


def test_a_checkout_that_does_not_match_the_declaration_refuses_before_any_row_or_access(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    moved = dataclasses.replace(HASHES, construction_sha256="9" * 64)
    with pytest.raises(DeclarationError):
        ledgers.start(current=lambda: moved)
    assert not ledgers.ledger.exists() and ledgers.accesses == []


def test_no_declared_row_refuses_before_any_row(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    ledgers.committed.write_text("")
    with pytest.raises(DeclarationError):
        ledgers.start()
    assert not ledgers.ledger.exists() and ledgers.accesses == []


def test_a_run_id_already_in_either_ledger_refuses(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    append_ledger(ledgers.committed, {"run_id": RUN, "event": "failed"})
    with pytest.raises(StageBAccessError, match="used once"):
        ledgers.start()
    assert not ledgers.ledger.exists() and ledgers.accesses == []


def test_an_access_that_fails_ends_the_run_failed(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)

    def refuse(_access: HoldoutAccess) -> int:
        raise RuntimeError("database down")

    with pytest.raises(RuntimeError, match="database down"):
        ledgers.start(record=refuse)
    assert ledgers.events() == ["started", "failed"]
    assert read_ledger(ledgers.ledger)[-1]["step"] == "access_recorded"


def _frozen(run_id: str = "d" * 32, **row: Any) -> dict[str, Any]:
    return {"run_id": run_id, "event": DATA_FROZEN_EVENT, "trial_id": TRIAL_ID, "manifest_sha256": "f" * 64, **row}


@pytest.mark.parametrize("trial_id", [TRIAL_ID, None, ""])
def test_a_fresh_capture_refuses_while_a_committed_capture_could_bind_the_trial(
    tmp_path: Path, trial_id: str | None
) -> None:
    """Refused before ``started``: ``freeze_capture`` would refuse it only after the SUB and stage-B builds."""
    ledgers = _Ledgers(tmp_path)
    append_ledger(ledgers.committed, _frozen(trial_id=trial_id))
    with pytest.raises(StageBAccessError, match="--reuse"):
        ledgers.start()
    assert not ledgers.ledger.exists() and ledgers.accesses == []


def test_another_trials_capture_does_not_block_a_fresh_one(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    append_ledger(ledgers.committed, _frozen(trial_id="another-trial"))
    assert ledgers.start() == 41


def test_a_reuse_needs_the_one_committed_binding(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    with pytest.raises(BookRefusal) as refused:
        ledgers.start(reuse=True)
    assert refused.value.code == CAPTURE_AMBIGUOUS
    assert not ledgers.ledger.exists() and ledgers.accesses == []
    append_ledger(ledgers.committed, _frozen())
    assert ledgers.start(reuse=True) == 41


def test_a_started_append_that_raises_after_its_row_is_durable_ends_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledgers = _Ledgers(tmp_path)

    def durable_then_raise(path: Path, row: Any) -> None:
        append_ledger(path, row)
        if row["event"] == "started":
            raise OSError("fsync")

    monkeypatch.setattr(declared_run, "append_ledger", durable_then_raise)
    with pytest.raises(OSError, match="fsync"):
        ledgers.start()
    assert ledgers.events() == ["started", "failed"] and ledgers.accesses == []
    assert read_ledger(ledgers.ledger)[-1]["step"] == "started"


def test_a_started_append_that_wrote_nothing_leaves_no_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledgers = _Ledgers(tmp_path)

    def refuse(_path: Path, _row: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(declared_run, "append_ledger", refuse)
    with pytest.raises(OSError, match="disk full"):
        ledgers.start()
    assert read_ledger(ledgers.ledger) == [] and ledgers.accesses == []


def test_a_failed_row_that_cannot_be_written_never_masks_the_original_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``end_run_failed`` catches its own append error and notes it on the original (``factor_book_ledger.py``), so
    ``_end_if_open`` re-raises the failure that ended the run, not the ledger's."""
    ledgers = _Ledgers(tmp_path)

    def refuse(_access: HoldoutAccess) -> int:
        raise RuntimeError("database down")

    def ledger_gone(_path: Path, _row: Any) -> None:
        raise OSError("ledger gone")

    monkeypatch.setattr(factor_book_ledger, "append_ledger", ledger_gone)
    with pytest.raises(RuntimeError, match="database down") as raised:
        ledgers.start(record=refuse)
    assert any("'failed' ledger row was not written" in note for note in raised.value.__notes__)
    assert ledgers.events() == ["started"]


# --------------------------------------------------------------------------- the steps


def test_steps_run_in_order_on_the_run_id_and_return_the_last_value(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    seen: list[tuple[str, str]] = []

    def step(name: str) -> Callable[[str], str]:
        def run(run_id: str) -> str:
            seen.append((name, run_id))
            return f"{name}-digest"

        return run

    steps = [(name, step(name)) for name in ("sub_published", "stage_b_published", DATA_FROZEN_EVENT)]
    result = declared_run.run_steps(RUN, steps, ledger=ledgers.ledger, committed_ledger=ledgers.committed)
    assert result == "data_frozen-digest"
    assert seen == [("sub_published", RUN), ("stage_b_published", RUN), (DATA_FROZEN_EVENT, RUN)]


def test_a_step_that_leaves_the_run_open_ends_it_failed_and_stops(tmp_path: Path) -> None:
    """A gate refusal past the access writes no row of its own; the run is ended here, and no later step runs."""
    ledgers = _Ledgers(tmp_path)
    ledgers.start()
    later: list[str] = []

    def refuse(_run_id: str) -> str:
        raise StageBAccessError("gate")

    steps = [("sub_published", refuse), ("stage_b_published", lambda run_id: later.append(run_id))]
    with pytest.raises(StageBAccessError, match="gate"):
        declared_run.run_steps(RUN, steps, ledger=ledgers.ledger, committed_ledger=ledgers.committed)
    assert ledgers.events() == ["started", "access_recorded", "failed"] and later == []
    assert read_ledger(ledgers.ledger)[-1]["step"] == "sub_published"


def test_a_step_that_ended_the_run_itself_is_not_ended_twice(tmp_path: Path) -> None:
    ledgers = _Ledgers(tmp_path)
    ledgers.start()

    def fail(run_id: str) -> str:
        append_ledger(ledgers.ledger, {"run_id": run_id, "event": "failed", "step": "sub_published"})
        raise RuntimeError("download")

    with pytest.raises(RuntimeError, match="download"):
        declared_run.run_steps(
            RUN, [("sub_published", fail)], ledger=ledgers.ledger, committed_ledger=ledgers.committed
        )
    assert ledgers.events() == ["started", "access_recorded", "failed"]


def test_capture_steps_name_the_ledger_events_in_order() -> None:
    assert [name for name, _ in declared_run.capture_steps(reuse=False)] == [
        "sub_published",
        "stage_b_published",
        DATA_FROZEN_EVENT,
    ]
    assert [name for name, _ in declared_run.capture_steps(reuse=True)] == ["capture_reused"]
