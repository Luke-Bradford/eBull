"""#3609 step 2 slice 5: the data-capture manifest, its ``data_frozen`` and ``capture_reused`` rows and verifier.

Fixtures only: nothing here reads stage-B data or the database.
"""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import scripts.build_3609_factor_panel as builder
import scripts.capture_3609_step2 as capture
from app.services.factor_book import BookRefusal
from app.services.factor_book_declaration import TRIAL_ID, DeclarationError, canonical_json
from app.services.factor_book_ledger import CAPTURE_AMBIGUOUS, StageBAccessError, capture_binding, check_report_gate
from app.services.factor_panel import PanelError
from app.services.factor_panel_artefact import gz_content_sha256, sha256_file, write_gz_lines
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.trial_register import TrialRegister
from tests.test_3609_step2_stage_b_builder import HASHES, RUN, _declared, _sub_artefact, _trial

REUSE = "b" * 32
SESSIONS = f"inputs/{builder.Frozen.SESSIONS}"


def _stage_b(root: Path, sub_sha256: str, run_id: str = RUN) -> tuple[Path, str]:
    """A stage-B artefact as ``publish_stage_b`` lays it out: its own pins naming the SUB, and the run's id."""
    out = root / "stageB"
    (out / "inputs").mkdir(parents=True)
    write_gz_lines(out / SESSIONS, ["2021-06-01"])
    write_gz_lines(out / builder.ROWS_FILE, [{"M": "2021-05-31"}])
    (out / builder.CENSUS_FILE).write_text("{}\n")
    manifest = {
        "schema": builder.MANIFEST_SCHEMA,
        "stage": "B",
        "run_id": run_id,
        "access_id": 7,
        "pinned_manifests": builder.stage_b_pins(sub_sha256),
        "inputs": {SESSIONS: sha256_file(out / SESSIONS)},
        "rows": {
            "path": builder.ROWS_FILE,
            "count": 1,
            "sha256": sha256_file(out / builder.ROWS_FILE),
            "content_sha256": gz_content_sha256(out / builder.ROWS_FILE),
        },
        "census": {"path": builder.CENSUS_FILE, "sha256": sha256_file(out / builder.CENSUS_FILE)},
    }
    (out / builder.MANIFEST_FILE).write_text(json.dumps(manifest))
    return out, sha256_file(out / builder.MANIFEST_FILE)


class _Run:
    """One capturing run's ledgers and artefacts, through ``stage_b_published``, under a matching declaration."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, declared: bool = True) -> None:
        self.sub = _sub_artefact(tmp_path, monkeypatch)
        self.stage_b = _stage_b(tmp_path, self.sub[1])
        self.ledger = tmp_path / "ledger.jsonl"
        self.committed = tmp_path / "committed.jsonl"
        self.root = tmp_path / "captures"
        self.confirmed: list[tuple[str, int]] = []
        trial = _trial()
        monkeypatch.setattr(capture, "TRIAL_REGISTER", TrialRegister("r", (trial,) if declared else ()))
        monkeypatch.setattr(capture.CodeHashes, "current", classmethod(lambda cls: HASHES))
        append_ledger(self.committed, _declared(trial))

    def open(self, run_id: str = RUN, *, published: bool = True) -> None:
        append_ledger(self.ledger, {"run_id": run_id, "event": "started"})
        append_ledger(self.ledger, {"run_id": run_id, "event": "access_recorded", "access_id": 7})
        if published:
            for event, (path, digest) in (("sub_published", self.sub), (builder.STAGE_B_EVENT, self.stage_b)):
                append_ledger(
                    self.ledger, {"run_id": run_id, "event": event, "artefact": str(path), "manifest_sha256": digest}
                )

    def _confirm(self, run_id: str, access_id: int) -> None:
        self.confirmed.append((run_id, access_id))

    def freeze(self, confirm: Callable[[str, int], None] | None = None) -> str:
        return capture.freeze_capture(
            RUN,
            confirm_access=confirm or self._confirm,
            ledger=self.ledger,
            committed_ledger=self.committed,
            root=self.root,
        )

    def reuse(self) -> str:
        return capture.reuse_capture(
            REUSE, confirm_access=self._confirm, ledger=self.ledger, committed_ledger=self.committed, root=self.root
        )

    def merge(self) -> None:
        """The capture's rows through ``data_frozen`` merged on ``main``, as the report requires."""
        for row in read_ledger(self.ledger):
            append_ledger(self.committed, row)

    def events(self, run_id: str = RUN) -> list[str]:
        return [row["event"] for row in read_ledger(self.ledger) if row.get("run_id") == run_id]


def _rewrite(path: Path, edit: Callable[[dict[str, Any]], None]) -> str:
    manifest = json.loads(path.read_bytes())
    edit(manifest)
    path.write_text(json.dumps(manifest))
    return sha256_file(path)


# --------------------------------------------------------------------------- freeze


def test_freeze_writes_the_manifest_then_data_frozen_and_the_report_binds_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _Run(tmp_path, monkeypatch)
    run.open()
    digest = run.freeze()

    path = capture.capture_path(RUN, run.root)
    document = path.read_bytes()
    assert hashlib.sha256(document).hexdigest() == digest and document == canonical_json(json.loads(document))
    manifest = json.loads(document)
    assert manifest["schema"] == capture.CAPTURE_SCHEMA and manifest["trial_id"] == TRIAL_ID
    assert (manifest["run_id"], manifest["access_id"]) == (RUN, 7)
    assert manifest["sub"]["manifest_sha256"] == run.sub[1] and manifest["sub"]["files"]
    assert manifest["stage_b"]["manifest_sha256"] == run.stage_b[1]
    assert manifest["stage_b"]["pins"] == builder.stage_b_pins(run.sub[1])
    assert manifest["stage_b"]["inputs"] == {SESSIONS: sha256_file(run.stage_b[0] / SESSIONS)}
    assert run.confirmed == [(RUN, 7)] and run.events()[-1] == "data_frozen"
    assert read_ledger(run.ledger)[-1]["trial_id"] == TRIAL_ID

    run.merge()
    binding = capture_binding(read_ledger(run.committed))
    assert (binding.capturing_run_id, binding.manifest_sha256) == (RUN, digest)
    verified = capture.verify_capture(path, binding.manifest_sha256, keep=[SESSIONS])
    assert verified.manifest == manifest and verified.sub["run_id"] == RUN
    assert gzip.decompress(verified.stage_b.files[SESSIONS]) == b'"2021-06-01"\n'


@pytest.mark.parametrize(
    ("declared", "setup", "error", "message"),
    [
        pytest.param(False, "open", DeclarationError, "0 register rows", id="no declaration"),
        pytest.param(True, "unpublished", StageBAccessError, "0 'sub_published'", id="no artefacts"),
        pytest.param(True, "frozen", StageBAccessError, "already written", id="already frozen"),
    ],
)
def test_the_gate_refuses_before_any_read_and_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    declared: bool,
    setup: str,
    error: type[Exception],
    message: str,
) -> None:
    run = _Run(tmp_path, monkeypatch, declared=declared)
    run.open(published=setup != "unpublished")
    if setup == "frozen":
        append_ledger(run.ledger, {"run_id": RUN, "event": "data_frozen"})
    with pytest.raises(error, match=message):
        run.freeze()
    assert run.confirmed == [] and not run.root.exists() and "failed" not in run.events()


def test_an_uncommitted_access_refuses_before_any_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = _Run(tmp_path, monkeypatch)
    run.open()

    def uncommitted(run_id: str, access_id: int) -> None:
        raise StageBAccessError("no committed evaluate access")

    with pytest.raises(StageBAccessError, match="no committed"):
        run.freeze(uncommitted)
    assert not run.root.exists() and "failed" not in run.events()


@pytest.mark.parametrize(
    ("tamper", "message"),
    [
        pytest.param("stage-b-run", "published by run", id="another run's stage B"),
        pytest.param("sub-file", "SUB file digest moved", id="an altered SUB file"),
        pytest.param("sub-extra", "does not match its manifest's file list", id="an unlisted SUB file"),
        pytest.param("stage-b-input", "frozen input digest moved", id="an altered stage-B input"),
        pytest.param("sub-no-inputs", "does not match its manifest's file list", id="a SUB without inputs/"),
    ],
)
def test_a_failure_past_the_gate_ends_the_run_and_leaves_no_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper: str, message: str
) -> None:
    run = _Run(tmp_path, monkeypatch)
    if tamper == "stage-b-run":
        run.stage_b = _stage_b(tmp_path / "other", run.sub[1], run_id=REUSE)
    elif tamper == "sub-file":
        next((run.sub[0] / "inputs").rglob("*.txt")).write_text("altered\n")
    elif tamper == "sub-no-inputs":
        shutil.rmtree(run.sub[0] / "inputs")
    elif tamper == "sub-extra":
        (run.sub[0] / "inputs" / "extra.txt").write_text("unlisted\n")
    else:
        (run.stage_b[0] / SESSIONS).write_bytes(b"altered")
    run.open()
    with pytest.raises(PanelError, match=message):
        run.freeze()
    assert run.events()[-1] == "failed" and list(run.root.glob("*")) == []


# --------------------------------------------------------------------------- the verifier


@pytest.fixture
def frozen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[_Run, Path, str]:
    run = _Run(tmp_path, monkeypatch)
    run.open()
    digest = run.freeze()
    return run, capture.capture_path(RUN, run.root), digest


def test_the_verifier_refuses_a_moved_manifest_digest(frozen: tuple[_Run, Path, str]) -> None:
    _, path, _ = frozen
    with pytest.raises(PanelError, match="data-capture manifest digest moved"):
        capture.verify_capture(path, "0" * 64)


def test_the_verifier_refuses_an_artefact_replaced_after_the_capture(frozen: tuple[_Run, Path, str]) -> None:
    """A re-hashed stage-B manifest beside the capture: the capture still names the original's sha256."""
    run, path, digest = frozen
    _rewrite(run.stage_b[0] / builder.MANIFEST_FILE, lambda m: m.update(access_id=8))
    with pytest.raises(PanelError, match="artefact manifest digest moved"):
        capture.verify_capture(path, digest)


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        pytest.param(lambda m: m["stage_b"]["inputs"].clear(), "no longer state", id="a restated input map"),
        pytest.param(lambda m: m.update(access_id=8), "published by run and access", id="another access"),
        pytest.param(lambda m: m.update(trial_id="other-v1"), "is not a", id="another trial"),
        pytest.param(lambda m: m.update(schema="v0"), "is not a", id="another schema"),
    ],
)
def test_the_verifier_refuses_a_capture_whose_record_differs_from_the_artefacts(
    frozen: tuple[_Run, Path, str], edit: Callable[[dict[str, Any]], None], message: str
) -> None:
    _, path, _ = frozen
    with pytest.raises(PanelError, match=message):
        capture.verify_capture(path, _rewrite(path, edit))


def test_the_verifier_refuses_a_stage_b_artefact_pinned_to_another_sub(frozen: tuple[_Run, Path, str]) -> None:
    run, path, _ = frozen
    other = _stage_b(run.root.parent / "substituted", "t" * 64)

    def substitute(m: dict[str, Any]) -> None:
        m["stage_b"].update(artefact=str(other[0]), manifest_sha256=other[1])

    with pytest.raises(PanelError, match="this stage's pins"):
        capture.verify_capture(path, _rewrite(path, substitute))


# --------------------------------------------------------------------------- reuse


def test_a_later_attempt_reuses_the_committed_capture_and_the_report_gate_accepts_it(
    frozen: tuple[_Run, Path, str],
) -> None:
    run, _, digest = frozen
    run.merge()
    run.open(REUSE, published=False)
    assert run.reuse() == digest
    row = read_ledger(run.ledger)[-1]
    assert (row["event"], row["capturing_run_id"], row["manifest_sha256"]) == ("capture_reused", RUN, digest)

    rows = [
        {**r, **HASHES.by_label()} if r["event"] == "started" else r for r in read_ledger(run.committed, run.ledger)
    ]
    committed = read_ledger(run.committed)
    _, binding = check_report_gate(TrialRegister("r", (_trial(),)), committed, rows, REUSE, HASHES)
    assert (binding.capturing_run_id, binding.manifest_sha256) == (RUN, digest)


def test_reuse_needs_exactly_one_committed_capture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = _Run(tmp_path, monkeypatch)
    run.open(REUSE, published=False)
    with pytest.raises(BookRefusal, match=CAPTURE_AMBIGUOUS):
        run.reuse()
    assert run.confirmed == [] and "failed" not in run.events(REUSE)


def test_an_attempt_with_capture_rows_of_its_own_cannot_reuse(frozen: tuple[_Run, Path, str]) -> None:
    run, _, _ = frozen
    run.merge()
    run.open(REUSE)
    with pytest.raises(StageBAccessError, match="cannot reuse"):
        run.reuse()
    assert "failed" not in run.events(REUSE)


def test_a_reuse_whose_capture_no_longer_verifies_ends_the_run(frozen: tuple[_Run, Path, str]) -> None:
    run, _, _ = frozen
    run.merge()
    (run.stage_b[0] / SESSIONS).write_bytes(b"altered")
    run.open(REUSE, published=False)
    with pytest.raises(PanelError, match="frozen input digest moved"):
        run.reuse()
    assert run.events(REUSE)[-1] == "failed"


# --------------------------------------------------------------------------- one capture per trial, one row per step


def test_a_fresh_freeze_refuses_once_the_committed_ledger_binds_a_capture(frozen: tuple[_Run, Path, str]) -> None:
    """Codex checkpoint 2: a second ``data_frozen`` row would make every report refuse ``CAPTURE_AMBIGUOUS``."""
    run, _, _ = frozen
    run.merge()
    other = "d" * 32
    run.open(other)
    with pytest.raises(StageBAccessError, match="reuse it"):
        capture.freeze_capture(
            other,
            confirm_access=lambda *_: None,
            ledger=run.ledger,
            committed_ledger=run.committed,
            root=run.root,
        )
    assert "failed" not in run.events(other) and not capture.capture_path(other, run.root).exists()


@pytest.mark.parametrize("step", ["freeze", "reuse"])
def test_each_capture_step_holds_its_claim_from_the_gate_through_its_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, step: str
) -> None:
    """Codex checkpoint 2: two concurrent invocations for one run cannot both pass the gate and append."""
    run = _Run(tmp_path, monkeypatch)
    run.open()
    if step == "reuse":
        run.freeze()
        run.merge()
        run.open(REUSE, published=False)
    held: list[bool] = []
    real_append = capture.append_ledger

    def append(path: Path, row: Any) -> None:
        with (path.parent / f"{path.name}.{capture.CAPTURE_CLAIM}.lock").open("a") as other:
            try:
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                held.append(True)
            else:
                fcntl.flock(other, fcntl.LOCK_UN)
                held.append(False)
        real_append(path, row)

    monkeypatch.setattr(capture, "append_ledger", append)
    run.freeze() if step == "freeze" else run.reuse()
    assert held == [True]


def test_a_committed_row_naming_no_trial_also_refuses_a_fresh_freeze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``capture_binding`` counts it as this trial's (every report refuses ``CAPTURE_AMBIGUOUS``), so does the
    freeze; another trial's row does not."""
    run = _Run(tmp_path, monkeypatch)
    append_ledger(run.committed, {"run_id": "d" * 32, "event": "data_frozen", "trial_id": "other-v1"})
    run.open()
    for trial in (None, "", 123):  # what ``capture_binding`` counts as naming no trial
        committed = run.committed.read_bytes()
        append_ledger(run.committed, {"run_id": "e" * 32, "event": "data_frozen", "trial_id": trial})
        with pytest.raises(StageBAccessError, match="reuse it"):
            run.freeze()
        run.committed.write_bytes(committed)
    assert run.confirmed == [] and "failed" not in run.events()


def test_a_retry_under_an_id_whose_manifest_exists_without_its_row_refuses_before_any_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex checkpoint 3: a crash between the manifest and both rows leaves the run open with the file on disk."""
    run = _Run(tmp_path, monkeypatch)
    run.open()
    capture.capture_path(RUN, run.root).parent.mkdir(parents=True)
    capture.capture_path(RUN, run.root).write_bytes(b"{}")
    shutil.rmtree(run.sub[0])  # any artefact read would raise a file error instead of the refusal
    with pytest.raises(StageBAccessError, match="abandoned"):
        run.freeze()
    assert run.confirmed == [] and "failed" not in run.events()


def test_a_failed_data_frozen_append_ends_the_run_and_keeps_the_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The append may have made the row durable before it raised, so the file it names stays and still verifies."""
    run = _Run(tmp_path, monkeypatch)
    run.open()
    real_append = capture.append_ledger

    def append(path: Path, row: Any) -> None:
        real_append(path, row)
        raise OSError("fsync failed after the write")

    monkeypatch.setattr(capture, "append_ledger", append)
    with pytest.raises(OSError, match="fsync failed"):
        run.freeze()
    frozen_row = read_ledger(run.ledger)[-2]
    assert run.events()[-2:] == ["data_frozen", "failed"]
    capture.verify_capture(capture.capture_path(RUN, run.root), frozen_row["manifest_sha256"])
