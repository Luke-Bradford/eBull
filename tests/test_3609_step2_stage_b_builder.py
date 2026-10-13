"""#3609 step 2 slice 2: the stage-B builder path, its declaration and access gates, and per-stage pin maps.

Fixtures only: nothing here reads stage-B data or the database.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest

import scripts.build_3609_factor_panel as builder
from app.services.factor_book_declaration import (
    CONSTRUCTION_LABEL,
    POLICY_LABEL,
    PYTHON_LABEL,
    SPEC_LABEL,
    TRIAL_ID,
    CodeHashes,
    DeclarationError,
    check_declaration,
    evidence_value,
    payload_sha256,
    register_policy_sha256,
)
from app.services.factor_book_ledger import COMMITTED_LEDGER_PATH, StageBAccessError
from app.services.factor_book_reference import STAGE_B_FIRST_FORMATION, STAGE_B_LAST_FORMATION, STAGE_B_PRICE_BOUND
from app.services.factor_panel import PanelError, formation_months
from app.services.factor_panel_artefact import gz_content_sha256, sha256_file, write_gz_lines
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.trial_register import DeclaredTrial, TrialExactness, TrialRegister
from scripts import publish_3609_step2_sub as sub_publisher
from tests.test_publish_3609_step2_sub import _Sec

RUN = "a" * 32
HASHES = CodeHashes(spec_sha256="1" * 64, construction_sha256="2" * 64, register_policy_sha256="3" * 64, python="3.14")
STAGE_B = formation_months(STAGE_B_FIRST_FORMATION, STAGE_B_LAST_FORMATION)


# --------------------------------------------------------------------------- stages


def test_stage_b_is_39_formations_bounded_at_its_last_holding_month() -> None:
    assert len(STAGE_B) == 39 and STAGE_B[0].isoformat() == "2021-05-31" and STAGE_B[-1].isoformat() == "2024-07-31"
    assert builder.price_bound(STAGE_B) == STAGE_B_PRICE_BOUND
    assert builder.price_bound(formation_months()) == builder.PRICE_BOUND


@pytest.mark.parametrize(
    "formations", [(*formation_months()[-1:], STAGE_B[0]), (*STAGE_B, STAGE_B[-1].replace(month=8))]
)
def test_a_formation_set_outside_one_stage_refuses(formations: tuple[Any, ...]) -> None:
    with pytest.raises(PanelError, match="not within one stage"):
        builder.price_bound(formations)


def test_only_price_bound_names_a_stage_bound() -> None:
    """Every bounded read takes the stage's bound as an argument: a function naming a stage constant directly would
    read stage A's window inside a stage-B build (Codex checkpoint 2: ``split_stamps`` did)."""
    import ast

    tree = ast.parse(Path(builder.__file__).read_text())
    constants = {"PRICE_BOUND", "STAGE_B_PRICE_BOUND"}
    naming = sorted(
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(isinstance(n, ast.Name) and n.id in constants for n in ast.walk(node))
    )
    assert naming == ["price_bound"]


def test_a_full_stage_grid_is_chain_complete_and_a_subset_is_not() -> None:
    assert builder.chain_complete(STAGE_B) and builder.chain_complete(list(formation_months()))
    assert not builder.chain_complete(STAGE_B[:-1]) and not builder.chain_complete(formation_months()[1:])


def test_read_rf_takes_the_stage_bound(tmp_path: Path) -> None:
    write_gz_lines(
        tmp_path / builder.Frozen.snapshot(builder.RF_DATASET),
        [["RF", "2021-05-28", "0.1", "decimal_return"], ["RF", "2021-06-01", "0.2", "decimal_return"]],
    )
    first = STAGE_B_FIRST_FORMATION.replace(day=1)
    assert list(builder.read_rf(tmp_path, first, builder.PRICE_BOUND)) == [STAGE_B_FIRST_FORMATION.replace(day=28)]
    assert len(builder.read_rf(tmp_path, first, STAGE_B_PRICE_BOUND)) == 2


# --------------------------------------------------------------------------- per-stage pin maps


def _artefact(tmp_path: Path, pins: dict[str, str]) -> tuple[Path, str]:
    out = tmp_path / "artefact"
    (out / "inputs").mkdir(parents=True)
    write_gz_lines(out / "inputs" / builder.Frozen.SESSIONS, ["2021-06-01"])
    write_gz_lines(out / builder.ROWS_FILE, [{"M": "2021-05-31"}])
    (out / builder.CENSUS_FILE).write_text("{}\n")
    manifest = {
        "schema": builder.MANIFEST_SCHEMA,
        "pinned_manifests": pins,
        "inputs": {f"inputs/{builder.Frozen.SESSIONS}": sha256_file(out / "inputs" / builder.Frozen.SESSIONS)},
        "rows": {
            "path": builder.ROWS_FILE,
            "sha256": sha256_file(out / builder.ROWS_FILE),
            "content_sha256": gz_content_sha256(out / builder.ROWS_FILE),
        },
        "census": {"path": builder.CENSUS_FILE, "sha256": sha256_file(out / builder.CENSUS_FILE)},
    }
    (out / builder.MANIFEST_FILE).write_text(json.dumps(manifest))
    return out, sha256_file(out / builder.MANIFEST_FILE)


def test_each_stage_verifies_under_its_own_pin_map(tmp_path: Path) -> None:
    stage_a, digest_a = _artefact(tmp_path / "a", dict(builder.STAGE_A_PINS))
    stage_b, digest_b = _artefact(tmp_path / "b", builder.stage_b_pins("s" * 64))
    assert builder.verify_artefact(stage_a, digest_a)["pinned_manifests"] == builder.STAGE_A_PINS
    assert builder.verify_artefact(stage_b, digest_b, builder.stage_b_pins("s" * 64))


@pytest.mark.parametrize(
    ("artefact_pins", "expected"),
    [
        pytest.param(dict(builder.STAGE_A_PINS), builder.stage_b_pins("s" * 64), id="stage A under stage B's map"),
        pytest.param(builder.stage_b_pins("s" * 64), dict(builder.STAGE_A_PINS), id="stage B under stage A's map"),
        pytest.param(builder.stage_b_pins("t" * 64), builder.stage_b_pins("s" * 64), id="substituted SUB"),
    ],
)
def test_an_artefact_under_another_stages_pins_refuses(
    tmp_path: Path, artefact_pins: dict[str, str], expected: dict[str, str]
) -> None:
    out, digest = _artefact(tmp_path, artefact_pins)
    with pytest.raises(PanelError, match="this stage's pins"):
        builder.verify_artefact(out, digest, expected)


def test_replay_is_stage_a_only(tmp_path: Path) -> None:
    out, digest = _artefact(tmp_path, builder.stage_b_pins("s" * 64))
    with pytest.raises(PanelError, match="this stage's pins"):
        builder.replay(out, digest, tmp_path / "replay")
    assert not (tmp_path / "replay.jsonl.gz").exists()


class _Conn:
    """A stand-in connection recording the mode the dump runs under."""

    def __init__(self) -> None:
        self.isolation_level: Any = None
        self.read_only = False
        self.in_transaction = False

    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        self.in_transaction = True
        yield
        self.in_transaction = False


def test_a_publish_dumps_inside_one_read_only_repeatable_read_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _Conn()
    connects: list[dict[str, Any]] = []
    seen: list[tuple[Any, bool, bool]] = []

    def connect(*args: Any, **kwargs: Any) -> _Conn:
        connects.append(kwargs)
        return conn

    def dump(c: _Conn, inputs: Path, **kwargs: Any) -> None:
        seen.append((c.isolation_level, c.read_only, c.in_transaction))
        inputs.mkdir(parents=True)

    def build(inputs: Path, formations: Any, rows: Path, census: Path, sub: str | None) -> dict[str, Any]:
        write_gz_lines(rows, [{"M": "2019-01-31"}])
        census.write_text("{}\n")
        return {"rows": 1}

    monkeypatch.setattr(builder.psycopg, "connect", connect)
    monkeypatch.setattr(builder, "dump_inputs", dump)
    monkeypatch.setattr(builder, "build", build)
    out = tmp_path / "artefact"
    out.mkdir()
    builder._publish_artefact(
        out, formation_months(), head="f" * 40, stage="A", pins=builder.STAGE_A_PINS, versions=lambda: {}
    )
    assert connects == [{}]  # not autocommit
    assert seen == [(psycopg.IsolationLevel.REPEATABLE_READ, True, True)]
    assert json.loads((out / builder.MANIFEST_FILE).read_text())["pinned_manifests"] == builder.STAGE_A_PINS


# --------------------------------------------------------------------------- the extended SUB


def _sub_artefact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    monkeypatch.setattr(sub_publisher, "_clean_head", lambda: "f" * 40)
    monkeypatch.setattr(sub_publisher, "_check_declared", lambda rows: None)
    ledger = tmp_path / "sub-ledger.jsonl"
    append_ledger(ledger, {"run_id": RUN, "event": "started"})
    append_ledger(ledger, {"run_id": RUN, "event": "access_recorded", "access_id": 7})
    with httpx.Client(transport=httpx.MockTransport(_Sec())) as client:
        digest = sub_publisher.publish(
            tmp_path / "sub",
            RUN,
            ledger=ledger,
            client=client,
            confirm_access=lambda run_id, access_id: None,
            committed_ledger=tmp_path / "none.jsonl",
        )
    return tmp_path / "sub", digest


def test_the_extended_sub_merges_into_step_1s_and_refuses_a_conflicting_sic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, digest = _sub_artefact(tmp_path, monkeypatch)
    alone = builder.load_sub_sic(root, digest)
    assert alone["0000320193-21-000003"] == 3571 and alone["0000950170-21-000003"] is None
    merged = builder.load_sub_sic(root, digest, into={"0000000000-20-000001": 1000, "0000320193-21-000003": 3571})
    assert merged["0000000000-20-000001"] == 1000 and len(merged) == len(alone) + 1
    with pytest.raises(PanelError, match="two SIC codes"):
        builder.load_sub_sic(root, digest, into={"0000320193-21-000003": 3572})
    with pytest.raises(PanelError, match="manifest digest moved"):
        builder.load_sub_sic(root, "0" * 64)


# --------------------------------------------------------------------------- the declaration


def _trial(evidence: str | None = None, **changes: Any) -> DeclaredTrial:
    if evidence is None:
        evidence = "; ".join(f"{label}={value}" for label, value in HASHES.by_label().items())
    return DeclaredTrial(
        trial_id=TRIAL_ID,
        description="#3609 step 2 factor book",
        evidence=evidence,
        exactness=changes.pop("exactness", TrialExactness.EXACT),
        **changes,
    )


def _declared(trial: DeclaredTrial) -> dict[str, Any]:
    return {"event": "declared", "trial_id": TRIAL_ID, "payload_sha256": payload_sha256(trial)}


def test_a_matching_declaration_passes() -> None:
    trial = _trial()
    assert check_declaration(TrialRegister("r", (trial,)), [_declared(trial)], HASHES) is trial


@pytest.mark.parametrize("label", [SPEC_LABEL, CONSTRUCTION_LABEL, POLICY_LABEL, PYTHON_LABEL])
def test_a_checkout_that_differs_from_any_declared_value_refuses(label: str) -> None:
    trial = _trial()
    moved = dataclasses.replace(HASHES, **{label: "9"})  # each label is its field's name
    with pytest.raises(DeclarationError, match=label):
        check_declaration(TrialRegister("r", (trial,)), [_declared(trial)], moved)


@pytest.mark.parametrize(
    ("trial", "message"),
    [
        (_trial(searches=2), "non-claiming exact one-search"),
        (_trial(exactness=TrialExactness.FLOOR), "non-claiming exact one-search"),
        (_trial(declared_for=("3609-step2-book", "v1")), "non-claiming exact one-search"),
        (_trial(evidence=f"{SPEC_LABEL}=1; {SPEC_LABEL}=1"), "exactly once"),
    ],
)
def test_a_row_of_the_wrong_shape_refuses(trial: DeclaredTrial, message: str) -> None:
    with pytest.raises(DeclarationError, match=message):
        check_declaration(TrialRegister("r", (trial,)), [_declared(trial)], HASHES)


def test_a_missing_row_or_an_unpinned_or_edited_payload_refuses() -> None:
    trial = _trial()
    with pytest.raises(DeclarationError, match="0 register rows"):
        check_declaration(TrialRegister("r", ()), [_declared(trial)], HASHES)
    with pytest.raises(DeclarationError, match="0 'declared'"):
        check_declaration(TrialRegister("r", (trial,)), [], HASHES)
    with pytest.raises(DeclarationError, match="2 'declared'"):
        check_declaration(TrialRegister("r", (trial,)), [_declared(trial)] * 2, HASHES)
    edited = dataclasses.replace(trial, description="edited after the freeze")
    with pytest.raises(DeclarationError, match="payload"):
        check_declaration(TrialRegister("r", (edited,)), [_declared(trial)], HASHES)


def test_evidence_labels_match_whole_labels_only() -> None:
    assert evidence_value(f"x {SPEC_LABEL}=ab; other_{SPEC_LABEL}=cd", SPEC_LABEL) == "ab"
    with pytest.raises(DeclarationError, match="0 times"):
        evidence_value(f"other_{SPEC_LABEL}=cd", SPEC_LABEL)


def test_the_register_policy_hash_ignores_the_register_data_and_nothing_else() -> None:
    source = (Path(builder.REPO_ROOT) / "app/services/trial_register.py").read_text()
    base = register_policy_sha256(source.encode())
    version_line = next(line for line in source.splitlines() if line.startswith("TRIAL_REGISTER_VERSION"))
    bumped = source.replace(version_line, 'TRIAL_REGISTER_VERSION: Final = "trial-register-bumped"')
    assert register_policy_sha256(bumped.encode()) == base
    assert register_policy_sha256((source + "\nPOLICY_EDIT = 1\n").encode()) != base
    with pytest.raises(DeclarationError, match="register data assignments"):
        register_policy_sha256(source.replace(version_line, "").encode())


# --------------------------------------------------------------------------- the stage-B publish gate


class _Run:
    """``publish_stage_b`` with the checkout, the declaration and the build replaced by recorders."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, declared: bool = True) -> None:
        self.root = tmp_path / "panels"
        self.ledger = tmp_path / "ledger.jsonl"
        self.committed = tmp_path / "committed.jsonl"
        self.confirmed: list[tuple[str, int]] = []
        self.built: list[dict[str, Any]] = []
        self.build_error: BaseException | None = None
        trial = _trial()
        register = TrialRegister("r", (trial,) if declared else ())
        append_ledger(self.committed, _declared(trial))
        monkeypatch.setattr(builder, "_clean_head", lambda: "f" * 40)
        monkeypatch.setattr(builder, "TRIAL_REGISTER", register)
        monkeypatch.setattr(builder.CodeHashes, "current", classmethod(lambda cls: HASHES))
        monkeypatch.setattr(builder, "_publish_artefact", self._build)

    def _build(self, out: Path, formations: Any, **kwargs: Any) -> None:
        self.built.append({"formations": tuple(formations), **kwargs})
        if self.build_error is not None:
            raise self.build_error
        (out / builder.MANIFEST_FILE).write_text("{}")

    def publish(self, confirm: Callable[[str, int], None] | None = None) -> tuple[Path, str]:
        def recording(run_id: str, access_id: int) -> None:
            self.confirmed.append((run_id, access_id))

        return builder.publish_stage_b(
            self.root,
            RUN,
            confirm_access=confirm or recording,
            ledger=self.ledger,
            committed_ledger=self.committed,
        )

    def events(self) -> list[str]:
        return [row["event"] for row in read_ledger(self.ledger) if row.get("run_id") == RUN]


def _open_run(run: _Run, sub: tuple[Path, str]) -> None:
    append_ledger(run.ledger, {"run_id": RUN, "event": "started"})
    append_ledger(run.ledger, {"run_id": RUN, "event": "access_recorded", "access_id": 7})
    append_ledger(
        run.ledger, {"run_id": RUN, "event": "sub_published", "artefact": str(sub[0]), "manifest_sha256": sub[1]}
    )


def test_stage_b_publishes_under_its_own_pins_and_writes_its_ledger_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub = _sub_artefact(tmp_path, monkeypatch)
    run = _Run(tmp_path, monkeypatch)
    _open_run(run, sub)
    out, digest = run.publish()

    assert run.confirmed == [(RUN, 7)]
    (built,) = run.built
    assert built["formations"] == STAGE_B and built["stage"] == "B"
    assert built["pins"] == builder.stage_b_pins(sub[1]) and built["step2_sub"] == sub
    assert built["provenance"] == {"run_id": RUN, "access_id": 7}
    assert run.events()[-1] == builder.STAGE_B_EVENT
    assert read_ledger(run.ledger)[-1]["manifest_sha256"] == digest == sha256_file(out / builder.MANIFEST_FILE)


@pytest.mark.parametrize(
    ("declared", "rows", "error", "message"),
    [
        pytest.param(False, "open", DeclarationError, "0 register rows", id="no declaration"),
        pytest.param(True, "no access", StageBAccessError, "0 'access_recorded'", id="no access"),
        pytest.param(True, "no sub", StageBAccessError, "0 'sub_published'", id="no extended SUB"),
        pytest.param(True, "published", StageBAccessError, "already written", id="already published"),
    ],
)
def test_the_gate_refuses_before_any_stage_b_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, declared: bool, rows: str, error: type[Exception], message: str
) -> None:
    sub = _sub_artefact(tmp_path, monkeypatch)
    run = _Run(tmp_path, monkeypatch, declared=declared)
    append_ledger(run.ledger, {"run_id": RUN, "event": "started"})
    if rows != "no access":
        append_ledger(run.ledger, {"run_id": RUN, "event": "access_recorded", "access_id": 7})
    if rows in ("open", "published"):
        append_ledger(
            run.ledger, {"run_id": RUN, "event": "sub_published", "artefact": str(sub[0]), "manifest_sha256": sub[1]}
        )
    if rows == "published":
        append_ledger(run.ledger, {"run_id": RUN, "event": builder.STAGE_B_EVENT})
    with pytest.raises(error, match=message):
        run.publish()
    assert run.confirmed == [] and run.built == [] and not run.root.exists()
    assert "failed" not in run.events()


def test_an_uncommitted_access_refuses_before_any_stage_b_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sub = _sub_artefact(tmp_path, monkeypatch)
    run = _Run(tmp_path, monkeypatch)
    _open_run(run, sub)

    def uncommitted(run_id: str, access_id: int) -> None:
        raise StageBAccessError("no committed evaluate access")

    # Gate before read: with the ledger-named SUB artefact gone, any read of it before the access check would
    # raise a file error instead of the refusal.
    shutil.rmtree(sub[0])

    with pytest.raises(StageBAccessError, match="no committed"):
        run.publish(uncommitted)
    assert run.built == [] and not run.root.exists() and "failed" not in run.events()


def test_a_failure_after_the_gate_ends_the_run_and_removes_the_artefact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub = _sub_artefact(tmp_path, monkeypatch)
    run = _Run(tmp_path, monkeypatch)
    _open_run(run, sub)
    run.build_error = PanelError("the build failed")
    with pytest.raises(PanelError, match="the build failed"):
        run.publish()
    assert run.events()[-1] == "failed" and list(run.root.iterdir()) == []


def test_another_runs_sub_artefact_ends_the_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sub = _sub_artefact(tmp_path, monkeypatch)
    manifest = json.loads((sub[0] / "manifest.json").read_text())
    manifest["run_id"] = "b" * 32
    (sub[0] / "manifest.json").write_text(json.dumps(manifest))
    run = _Run(tmp_path, monkeypatch)
    _open_run(run, (sub[0], sha256_file(sub[0] / "manifest.json")))
    with pytest.raises(PanelError, match="not published by run"):
        run.publish()
    assert run.built == [] and run.events()[-1] == "failed"


def test_a_gate_pinning_a_sub_binds_another_runs_sub_without_a_sub_published_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#3621 v2: a rebuild whose inputs must equal step 2's binds step 2's SUB, which names step 2's run."""
    sub = _sub_artefact(tmp_path, monkeypatch)
    run = _Run(tmp_path, monkeypatch, declared=False)  # step 2's declaration is not the gate's
    checked: list[int] = []
    gate = builder.StageBGate(lambda rows: checked.append(len(rows)), lambda: {"spec_sha256": "9" * 64}, sub=sub)
    other = "c" * 32
    append_ledger(run.ledger, {"run_id": other, "event": "started"})
    append_ledger(run.ledger, {"run_id": other, "event": "access_recorded", "access_id": 9})
    out, digest = builder.publish_stage_b(
        run.root,
        other,
        confirm_access=lambda run_id, access_id: run.confirmed.append((run_id, access_id)),
        ledger=run.ledger,
        committed_ledger=run.committed,
        gate=gate,
    )
    assert checked and run.confirmed == [(other, 9)]
    (built,) = run.built
    assert built["step2_sub"] == sub and built["pins"] == builder.stage_b_pins(sub[1])
    assert built["versions"] is gate.versions and built["provenance"] == {"run_id": other, "access_id": 9}
    assert read_ledger(run.ledger)[-1]["event"] == builder.STAGE_B_EVENT
    assert digest == sha256_file(out / builder.MANIFEST_FILE)


def test_a_gate_refusal_reads_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sub = _sub_artefact(tmp_path, monkeypatch)
    run = _Run(tmp_path, monkeypatch)
    _open_run(run, sub)

    def refuse(rows: object) -> object:
        raise DeclarationError("not v2's declaration")

    gate = builder.StageBGate(refuse, lambda: {}, sub=sub)
    with pytest.raises(DeclarationError, match="v2"):
        builder.publish_stage_b(
            run.root, RUN, confirm_access=lambda *_: None, ledger=run.ledger, committed_ledger=run.committed, gate=gate
        )
    assert run.built == [] and not run.root.exists() and "failed" not in run.events()


# --------------------------------------------------------------------------- the capture binding


def test_the_committed_ledger_binds_at_most_one_capture() -> None:
    """Spec §"Slices", capture lifecycle: a second ``data_frozen`` row can never merge, so the first capture to
    merge is the trial's binding. Only step 2's declared run writes this event to the shared ledger."""
    frozen = [row for row in read_ledger(COMMITTED_LEDGER_PATH) if row.get("event") == "data_frozen"]
    assert len(frozen) <= 1, frozen
