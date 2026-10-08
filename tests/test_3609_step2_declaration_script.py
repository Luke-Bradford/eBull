"""#3609 step 2 slice 4: the declaration script's evidence, ``declared`` row, access order and B1 close.

Fixtures only: no database, no stage-B data, and the script is never run against the dev DB here.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pytest

import scripts.declare_3609_step2 as declare_mod
from app.services.factor_book_declaration import (
    TRIAL_ID,
    CodeHashes,
    DeclarationError,
    check_declaration,
    evidence_value,
)
from app.services.factor_panel_fidelity import read_ledger
from app.services.result_ledger import HoldoutAccess
from app.services.total_return_reader import MonthEnd
from app.services.trial_register import TrialExactness, TrialRegister
from scripts.report_3609_step2 import STAGE_A_MANIFEST_SHA256, TABLE9_SIGNS
from scripts.report_3609_step2_inputs import SnapshotDigests, declared_b1_close, declared_pins

FIVE, MOMENTUM = "french_five_factor_monthly", "french_momentum_monthly"
HASHES = CodeHashes(spec_sha256="1" * 64, construction_sha256="2" * 64, register_policy_sha256="3" * 64, python="3.14")
SNAPSHOT_IDS = {FIVE: 39, MOMENTUM: 40}
DIGESTS = {FIVE: SnapshotDigests("a" * 64, "b" * 64), MOMENTUM: SnapshotDigests("c" * 64, "d" * 64)}
STEP0 = "0" * 64
TABLE9 = "9" * 64
B1 = 197.020004


def _declare(
    calls: list[str] | None = None, digests: Any = None, **override: Any
) -> tuple[int, Any, dict[str, Any], list[HoldoutAccess]]:
    calls = [] if calls is None else calls
    accesses: list[HoldoutAccess] = []

    def record(access: HoldoutAccess) -> int:
        calls.append("access")
        accesses.append(access)
        return 41

    def read(snapshot_ids: Mapping[str, int]) -> Mapping[str, SnapshotDigests]:
        calls.append("digests")
        assert snapshot_ids == SNAPSHOT_IDS
        return DIGESTS

    kwargs: dict[str, Any] = {
        "step0_manifest_sha256": STEP0,
        "table9_sha256": TABLE9,
        "snapshot_ids": SNAPSHOT_IDS,
        "b1": B1,
        "accessed_by": "autonomy loop",
        "record_access": record,
        "digests": digests or read,
        **override,
    }
    access_id, trial, row = declare_mod.declare(HASHES, **kwargs)
    return access_id, trial, row, accesses


def test_the_declared_row_passes_the_runs_own_gates() -> None:
    access_id, trial, row, _ = _declare()
    assert access_id == 41
    assert (trial.trial_id, trial.declared_for, trial.exactness, trial.searches) == (
        TRIAL_ID,
        None,
        TrialExactness.EXACT,
        1,
    )
    assert check_declaration(TrialRegister("r", (trial,)), [row], HASHES) is trial
    assert row["event"] == "declared" and set(row) == {"event", "trial_id", "payload_sha256", "at"}


def test_the_evidence_names_every_pin_once_and_reads_back() -> None:
    _, trial, _, _ = _declare()
    pins = declared_pins(trial.evidence)
    assert (pins.step0_manifest_sha256, pins.table9_sha256, dict(pins.snapshots)) == (STEP0, TABLE9, DIGESTS)
    assert declared_b1_close(trial.evidence) == B1
    for label, value in HASHES.by_label().items():
        assert evidence_value(trial.evidence, label) == value
    assert evidence_value(trial.evidence, declare_mod.HOLDOUT_STRATEGY_LABEL) == "3609-step2-book"
    assert evidence_value(trial.evidence, declare_mod.HOLDOUT_VERSION_LABEL) == "v1"
    assert trial.evidence.startswith(declare_mod.SPEC_REFERENCE + "; ")


def test_a_payload_edited_after_the_declared_row_refuses() -> None:
    _, trial, row, _ = _declare()
    edited = declare_mod.declared_trial(trial.evidence.replace(f"={B1!r}", "=197.03"))
    with pytest.raises(DeclarationError, match="differs from the payload"):
        check_declaration(TrialRegister("r", (edited,)), [row], HASHES)


def test_the_read_access_is_recorded_before_the_digests_and_names_the_spec_identity() -> None:
    calls: list[str] = []
    _, _, _, accesses = _declare(calls)
    assert calls == ["access", "digests"]
    (access,) = accesses
    assert (access.strategy_id, access.strategy_version, access.access_kind, access.result_version) == (
        "3609-step2-book",
        "v1",
        "read",
        None,
    )
    assert access.accessed_by == "autonomy loop"
    assert access.purpose == (
        "#3609 step 2 declaration: integrity digests of factor snapshots 39 and 40, no values read out"
    )


def test_a_failed_read_after_the_access_propagates_with_the_access_kept() -> None:
    calls: list[str] = []

    def broken(_ids: Mapping[str, int]) -> Mapping[str, SnapshotDigests]:
        calls.append("digests")
        raise RuntimeError("snapshot payload moved")

    with pytest.raises(RuntimeError, match="payload moved"):
        _declare(calls, digests=broken)
    assert calls == ["access", "digests"]


def test_a_trial_already_in_the_register_or_the_ledger_refuses() -> None:
    _, trial, row, _ = _declare()
    declare_mod.require_undeclared(TrialRegister("r", ()), [{"event": "declared", "trial_id": "other"}])
    with pytest.raises(DeclarationError, match="already declared: 1 register rows, 0 ledger rows"):
        declare_mod.require_undeclared(TrialRegister("r", (trial,)), [])
    with pytest.raises(DeclarationError, match="already declared: 0 register rows, 1 ledger rows"):
        declare_mod.require_undeclared(TrialRegister("r", ()), [row])


def test_stage_a_table9_checks_the_manifest_pin_then_its_frozen_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = json.dumps({"inputs": {TABLE9_SIGNS: TABLE9}}).encode()
    with pytest.raises(DeclarationError, match=STAGE_A_MANIFEST_SHA256):
        declare_mod.stage_a_table9(manifest, TABLE9)
    monkeypatch.setattr(declare_mod, "STAGE_A_MANIFEST_SHA256", hashlib.sha256(manifest).hexdigest())
    declare_mod.stage_a_table9(manifest, TABLE9)
    with pytest.raises(DeclarationError, match="froze Table 9 CSV"):
        declare_mod.stage_a_table9(manifest, "8" * 64)


def _month_end(day: date, close: float = B1) -> MonthEnd:
    return MonthEnd(bar_date=day, adj_close=163.47, close=close)


def test_b1_close_is_spys_one_intrader_bar_at_the_first_session() -> None:
    assert declare_mod.b1_close([7694], {(7694, (2014, 9)): _month_end(date(2014, 9, 30))}) == B1


@pytest.mark.parametrize(
    ("ids", "ends", "match"),
    [
        ([], {}, "exactly one"),
        ([1, 2], {(1, (2014, 9)): _month_end(date(2014, 9, 30))}, "exactly one"),
        ([1], {}, "not 2014-09-30"),
        ([1], {(1, (2014, 9)): _month_end(date(2014, 9, 29))}, "not 2014-09-30"),
        ([1], {(1, (2014, 9)): _month_end(date(2014, 9, 30), float("nan"))}, "raw close"),
        ([1], {(1, (2014, 9)): _month_end(date(2014, 9, 30), 0.0)}, "raw close"),
    ],
)
def test_b1_close_refuses_anything_but_one_valid_bar_on_the_session(
    ids: list[int], ends: dict[Any, MonthEnd], match: str
) -> None:
    with pytest.raises(DeclarationError, match=match):
        declare_mod.b1_close(ids, ends)


def test_main_checks_the_file_pins_before_any_database_access(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stage A manifest that does not verify refuses before the B1 read and the access row."""
    touched: list[str] = []
    monkeypatch.setattr(declare_mod, "TRIAL_REGISTER", TrialRegister("r", ()))
    monkeypatch.setattr(declare_mod.CodeHashes, "current", classmethod(lambda cls: HASHES))

    def moved(_manifest: bytes, _table9: str) -> None:
        raise DeclarationError("moved")

    monkeypatch.setattr(declare_mod, "stage_a_table9", moved)
    monkeypatch.setattr(declare_mod.psycopg, "connect", lambda *_a, **_k: touched.append("connect"))
    monkeypatch.setattr(declare_mod, "_record_access", lambda _a: touched.append("access"))
    with pytest.raises(DeclarationError, match="moved"):
        declare_mod.main(["--accessed-by", "autonomy loop"])
    assert touched == []


def test_main_appends_one_declared_row_and_prints_the_access_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ledger = tmp_path / "3609-ledger.jsonl"
    monkeypatch.setattr(declare_mod, "COMMITTED_LEDGER_PATH", ledger)
    monkeypatch.setattr(declare_mod, "TRIAL_REGISTER", TrialRegister("r", ()))
    monkeypatch.setattr(declare_mod.CodeHashes, "current", classmethod(lambda cls: HASHES))
    monkeypatch.setattr(declare_mod, "sha256_file", lambda path: STEP0 if path.name == "manifest.json" else TABLE9)
    monkeypatch.setattr(declare_mod, "STAGE_A_ARTEFACT", tmp_path)
    (tmp_path / "manifest.json").write_bytes(b"{}")
    monkeypatch.setattr(declare_mod, "stage_a_table9", lambda manifest, table9: None)

    class Step0:
        factor_snapshots = SNAPSHOT_IDS

    monkeypatch.setattr(declare_mod, "read_step0", lambda digest: Step0 if digest == STEP0 else None)

    class Conn:
        read_only = False

        def __enter__(self) -> Conn:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    monkeypatch.setattr(declare_mod.psycopg, "connect", lambda *_a, **_k: Conn())
    monkeypatch.setattr(declare_mod, "read_b1_close", lambda conn: B1 if conn.read_only else 0.0)
    monkeypatch.setattr(declare_mod, "_record_access", lambda _access: 41)
    monkeypatch.setattr(declare_mod, "_digests", lambda _ids: DIGESTS)

    assert declare_mod.main(["--accessed-by", "autonomy loop"]) == 0
    printed = json.loads(capsys.readouterr().out)
    (row,) = read_ledger(ledger)
    assert printed["access_id"] == 41 and printed["payload_sha256"] == row["payload_sha256"]
    trial = declare_mod.declared_trial(printed["evidence"])
    assert check_declaration(TrialRegister("r", (trial,)), [row], HASHES) is trial
    with pytest.raises(DeclarationError, match="already declared"):
        declare_mod.main(["--accessed-by", "autonomy loop"])


def test_the_b1_query_is_step_0s_month_end_query_bounded_before_distinct_on() -> None:
    sql = declare_mod.bounded_month_end_sql()
    assert sql.count("d.bar_date BETWEEN %(first)s AND %(last)s") == 1
    assert sql.index("d.bar_date BETWEEN %(first)s") < sql.index("ORDER BY")
    assert sql.replace("WHERE d.bar_date BETWEEN %(first)s AND %(last)s\n  AND d.series_id", "WHERE d.series_id") == (
        declare_mod._MONTH_END_SQL
    )
    with pytest.raises(DeclarationError, match="expected one"):
        declare_mod.bounded_month_end_sql("SELECT 1")
