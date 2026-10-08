"""#3609 step 2 slice 5b: the declaration's pins, step 0's B1 path and the observation digest.

Fixtures only: nothing here reads stage-B data or the database (the snapshot reads are in the ``_db`` sibling).
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

import scripts.report_3609_step2_inputs as inputs
from app.services.factor_book_declaration import DeclarationError, canonical_json
from app.services.factor_book_reference import QMJ_PDF_SHA256, SICCODES12_SHA256
from scripts.build_3609_factor_panel import VerifiedArtefact
from scripts.report_3609_step2 import STAGE_A_MANIFEST_SHA256, TABLE9_SIGNS, ReportError

FIVE, MOMENTUM = "french_five_factor_monthly", "french_momentum_monthly"


def _evidence(**override: str) -> str:
    labels = {
        inputs.STAGE_A_LABEL: STAGE_A_MANIFEST_SHA256,
        inputs.FF12_LABEL: SICCODES12_SHA256,
        inputs.QMJ_LABEL: QMJ_PDF_SHA256,
        inputs.STEP0_LABEL: "0" * 64,
        inputs.TABLE9_LABEL: "9" * 64,
        inputs.response_label(FIVE): "a" * 64,
        inputs.observations_label(FIVE): "b" * 64,
        inputs.response_label(MOMENTUM): "c" * 64,
        inputs.observations_label(MOMENTUM): "d" * 64,
        **override,
    }
    return "; ".join(f"{label}={value}" for label, value in labels.items())


def test_declared_pins_reads_every_label() -> None:
    pins = inputs.declared_pins(_evidence())
    assert pins.step0_manifest_sha256 == "0" * 64
    assert pins.table9_sha256 == "9" * 64
    assert pins.snapshots == {
        FIVE: inputs.SnapshotDigests("a" * 64, "b" * 64),
        MOMENTUM: inputs.SnapshotDigests("c" * 64, "d" * 64),
    }


@pytest.mark.parametrize("label", [inputs.STAGE_A_LABEL, inputs.FF12_LABEL, inputs.QMJ_LABEL])
def test_a_pin_that_is_also_a_code_constant_must_equal_it(label: str) -> None:
    with pytest.raises(ReportError, match=label):
        inputs.declared_pins(_evidence(**{label: "e" * 64}))


def test_a_missing_or_repeated_label_refuses() -> None:
    with pytest.raises(DeclarationError, match="exactly once"):
        inputs.declared_pins(_evidence().replace(f"{inputs.STEP0_LABEL}=", "other="))
    with pytest.raises(DeclarationError, match="exactly once"):
        inputs.declared_pins(_evidence() + f"; {inputs.TABLE9_LABEL}={'9' * 64}")


@pytest.mark.parametrize("value", ["F" * 64, "f" * 63, "not-a-digest"])
def test_a_pin_that_is_not_a_lowercase_hex_sha256_refuses(value: str) -> None:
    with pytest.raises(ReportError, match="not a lowercase hex sha256"):
        inputs.declared_pins(_evidence(**{inputs.STEP0_LABEL: value}))


def test_the_declared_b1_close_is_a_decimal_named_for_stage_as_first_session() -> None:
    assert inputs.B1_CLOSE_LABEL == "b1_close_2014_09_30"
    assert inputs.declared_b1_close(_evidence(**{inputs.B1_CLOSE_LABEL: "197.02"})) == 197.02
    with pytest.raises(DeclarationError, match="exactly once"):
        inputs.declared_b1_close(_evidence())


@pytest.mark.parametrize("value", ["abc", "NaN", "Infinity", "1e400", "0", "-197.02"])
def test_a_b1_close_that_is_not_a_finite_positive_decimal_refuses(value: str) -> None:
    with pytest.raises(ReportError, match=inputs.B1_CLOSE_LABEL):
        inputs.declared_b1_close(_evidence(**{inputs.B1_CLOSE_LABEL: value}))


def test_require_table9_compares_the_artefact_manifest_entry() -> None:
    verified = VerifiedArtefact(manifest={"inputs": {TABLE9_SIGNS: "9" * 64}}, files={})
    inputs.require_table9(verified, "9" * 64)
    with pytest.raises(ReportError, match="Table 9"):
        inputs.require_table9(verified, "8" * 64)
    with pytest.raises(ReportError, match="Table 9"):
        inputs.require_table9(VerifiedArtefact(manifest={"inputs": {}}, files={}), "9" * 64)


# --------------------------------------------------------------------------- step 0

B1 = {"months": ["2014-10"], "continuing": [0.01], "rebalance_cost": [0.0], "liquidation_by_month": [0.001]}


def _step0(tmp_path: Path, snapshots: object = None, b1: object = B1, drop: str | None = None) -> str:
    paths = json.dumps({inputs.B1_KEY: b1, "B2a 60/40": {}}).encode()
    (tmp_path / inputs.STEP0_PATHS).write_bytes(paths)
    manifest = {
        "sha256": {"paths": hashlib.sha256(paths).hexdigest()},
        "factor_snapshots": {FIVE: 39, MOMENTUM: 40} if snapshots is None else snapshots,
    }
    if drop is not None:
        del manifest[drop]
    document = json.dumps(manifest).encode()
    (tmp_path / inputs.STEP0_MANIFEST).write_bytes(document)
    return hashlib.sha256(document).hexdigest()


def test_read_step0_returns_b1_and_the_snapshot_ids(tmp_path: Path) -> None:
    step0 = inputs.read_step0(_step0(tmp_path), tmp_path)
    assert step0.b1_saved == B1
    assert step0.factor_snapshots == {FIVE: 39, MOMENTUM: 40}


def test_read_step0_refuses_a_moved_manifest_or_paths_file(tmp_path: Path) -> None:
    digest = _step0(tmp_path)
    with pytest.raises(ReportError, match="step 0 manifest"):
        inputs.read_step0("f" * 64, tmp_path)
    (tmp_path / inputs.STEP0_PATHS).write_bytes(b"{}")
    with pytest.raises(ReportError, match="paths.json"):
        inputs.read_step0(digest, tmp_path)


@pytest.mark.parametrize("snapshots", [{FIVE: 39}, {FIVE: 39, MOMENTUM: 40, "other": 41}, {FIVE: 39, MOMENTUM: "40"}])
def test_read_step0_refuses_other_factor_snapshots(tmp_path: Path, snapshots: object) -> None:
    with pytest.raises(ReportError, match="factor snapshot"):
        inputs.read_step0(_step0(tmp_path, snapshots), tmp_path)


@pytest.mark.parametrize("drop", ["sha256", "factor_snapshots"])
def test_a_verified_manifest_missing_a_field_refuses_as_a_report_error(tmp_path: Path, drop: str) -> None:
    with pytest.raises(ReportError, match="does not parse"):
        inputs.read_step0(_step0(tmp_path, drop=drop), tmp_path)


@pytest.mark.parametrize("document", [b"not json", b"[1, 2]", b'{"sha256": {"paths": 7}, "factor_snapshots": {}}'])
def test_a_verified_manifest_that_does_not_parse_refuses_as_a_report_error(tmp_path: Path, document: bytes) -> None:
    (tmp_path / inputs.STEP0_MANIFEST).write_bytes(document)
    with pytest.raises(ReportError, match="does not parse"):
        inputs.read_step0(hashlib.sha256(document).hexdigest(), tmp_path)


def test_a_paths_file_without_an_object_b1_refuses(tmp_path: Path) -> None:
    with pytest.raises(ReportError, match="not an object"):
        inputs.read_step0(_step0(tmp_path, b1=[1, 2]), tmp_path)


# --------------------------------------------------------------------------- observation digest


def test_observation_digest_is_the_canonical_json_of_sorted_rows() -> None:
    rows = [
        ("SMB", date(2020, 2, 29), Decimal("0.0100"), "decimal_return"),
        ("HML", date(2020, 1, 31), Decimal("-0.0115"), "decimal_return"),
        ("SMB", date(2020, 1, 31), Decimal("0.0015"), "decimal_return"),
    ]
    expected = [
        ["HML", "2020-01-31", "-0.0115", "decimal_return"],
        ["SMB", "2020-01-31", "0.0015", "decimal_return"],
        ["SMB", "2020-02-29", "0.0100", "decimal_return"],
    ]
    assert inputs.observations_sha256(rows) == hashlib.sha256(canonical_json(expected)).hexdigest()
    assert inputs.observations_sha256(list(reversed(rows))) == inputs.observations_sha256(rows)
    # The stored scale is part of the digest: str(Decimal) keeps trailing zeros.
    changed = [rows[0][:2] + (Decimal("0.01"), rows[0][3]), *rows[1:]]
    assert inputs.observations_sha256(changed) != inputs.observations_sha256(rows)
