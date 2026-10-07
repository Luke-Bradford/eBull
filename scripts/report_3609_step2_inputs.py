"""#3609 step 2's report inputs outside the panel artefacts: the declaration's pins, step 0's B1 path and the factor
snapshots (slice 5b).

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"Registration": the declaration's
``evidence`` list, and "Every consumed file is verified immediately before use" (step 0's ``paths.json``, factor and
RF data, each reference artefact).

* **Pins.** The declaration's ``evidence`` names each pin once as ``label=value`` (:func:`declared_pins`). The
  stage-A manifest, FF-12 map and QMJ PDF sha256s are also code constants inside the construction hash, so a
  declaration naming other values refuses. The Table 9 CSV is read from each panel artefact's frozen copy, so its
  pin is checked against the artefact manifest's entry (:func:`require_table9`).
* **Step 0** (:func:`read_step0`): its ``manifest.json`` is read once and checked against the declared sha256, then
  ``paths.json`` once against that manifest's ``sha256`` map. B1 is the ``"B1 SPY"`` entry; the factor snapshot ids
  are the manifest's ``factor_snapshots``.
* **Factor and RF data** (:func:`read_factors`): for each of step 0's snapshots, in one read-only repeatable-read
  transaction, the row must be ``accepted`` and of its dataset, the sha256 of its stored ``payload`` must equal its
  ``response_sha256`` and the declared value, and the observation digest (:func:`observations_sha256`) must equal
  the declared one. The returned values come only from the rows that digest covered.
* **The integrity-only read** the declaration script makes (slice 4) is :func:`snapshot_digests`: the same checks
  without a pin, returning the two digests and no value.

A mismatch raises :class:`~scripts.report_3609_step2.ReportError`, as the artefact verifiers raise ``PanelError``: a
run past ``report_started`` then ends ``failed``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import psycopg

from app.services.factor_book_declaration import canonical_json, evidence_value
from app.services.factor_book_path import Month
from app.services.factor_book_reference import QMJ_PDF_SHA256, SICCODES12_SHA256
from scripts.build_3609_factor_panel import VerifiedArtefact
from scripts.report_3609_baselines import FACTOR_DATASETS, FACTOR_UNIT
from scripts.report_3609_step2 import STAGE_A_MANIFEST_SHA256, TABLE9_SIGNS, ReportError

_REPO_ROOT: Final = Path(__file__).resolve().parents[1]
#: Step 0's declared run (spec §"References and the control", B1).
STEP0_RUN: Final = _REPO_ROOT / "var" / "research" / "3609_step0" / "20261004T214439Z"
STEP0_MANIFEST: Final = "manifest.json"
STEP0_PATHS: Final = "paths.json"
B1_KEY: Final = "B1 SPY"

#: The ``evidence`` labels of the declaration's pins, each named exactly once.
STAGE_A_LABEL: Final = "stage_a_manifest_sha256"
STEP0_LABEL: Final = "step0_manifest_sha256"
FF12_LABEL: Final = "ff12_sha256"
QMJ_LABEL: Final = "qmj_pdf_sha256"
TABLE9_LABEL: Final = "table9_sha256"


def response_label(dataset: str) -> str:
    return f"{dataset}_response_sha256"


def observations_label(dataset: str) -> str:
    return f"{dataset}_observations_sha256"


@dataclass(frozen=True)
class SnapshotDigests:
    """A factor snapshot's content: its stored payload's sha256 and its observation digest."""

    response_sha256: str
    observations_sha256: str


@dataclass(frozen=True)
class DeclaredPins:
    stage_a_manifest_sha256: str
    step0_manifest_sha256: str
    table9_sha256: str
    #: Per step 0 factor dataset.
    snapshots: Mapping[str, SnapshotDigests]


def declared_pins(evidence: str) -> DeclaredPins:
    """The pins the declaration's ``evidence`` names; the three that are also code constants must equal them."""
    for label, constant in (
        (STAGE_A_LABEL, STAGE_A_MANIFEST_SHA256),
        (FF12_LABEL, SICCODES12_SHA256),
        (QMJ_LABEL, QMJ_PDF_SHA256),
    ):
        if evidence_value(evidence, label) != constant:
            raise ReportError(f"the declaration's {label} is not this checkout's pinned {constant}")
    return DeclaredPins(
        stage_a_manifest_sha256=STAGE_A_MANIFEST_SHA256,
        step0_manifest_sha256=evidence_value(evidence, STEP0_LABEL),
        table9_sha256=evidence_value(evidence, TABLE9_LABEL),
        snapshots={
            dataset: SnapshotDigests(
                evidence_value(evidence, response_label(dataset)), evidence_value(evidence, observations_label(dataset))
            )
            for dataset, _ in FACTOR_DATASETS
        },
    )


def require_table9(verified: VerifiedArtefact, pinned: str) -> None:
    """The artefact's frozen Table 9 CSV is the declared one; the artefact's verifier has checked its bytes."""
    found = verified.manifest["inputs"].get(TABLE9_SIGNS)
    if found != pinned:
        raise ReportError(f"the artefact's Table 9 CSV is {found!r}, the declaration pins {pinned}")


# --------------------------------------------------------------------------- step 0


def _read_once(path: Path, digest: str, what: str) -> bytes:
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ReportError(f"{what} sha256 differs from its pin: {path}")
    return payload


@dataclass(frozen=True)
class Step0:
    manifest: Mapping[str, Any]
    #: ``paths.json``'s ``"B1 SPY"`` entry, ``b1_path``'s ``saved``.
    b1_saved: Mapping[str, Sequence[object]]
    #: Dataset key to ``reference_data_snapshots`` id.
    factor_snapshots: Mapping[str, int]


def read_step0(manifest_sha256: str, run: Path = STEP0_RUN) -> Step0:
    """Step 0's manifest against the declared sha256, then its ``paths.json`` against the manifest; each read once."""
    manifest = json.loads(_read_once(run / STEP0_MANIFEST, manifest_sha256, "step 0 manifest"))
    paths = json.loads(_read_once(run / STEP0_PATHS, manifest["sha256"]["paths"], "step 0 paths.json"))
    snapshots = manifest["factor_snapshots"]
    wanted = sorted(dataset for dataset, _ in FACTOR_DATASETS)
    if sorted(snapshots) != wanted:
        raise ReportError(f"step 0 pins factor snapshots for {sorted(snapshots)}, expected {wanted}")
    ids = {dataset: snapshots[dataset] for dataset in wanted}
    if any(type(i) is not int or i <= 0 for i in ids.values()):
        raise ReportError(f"step 0's factor snapshot ids {ids} are not positive integers")
    return Step0(manifest=manifest, b1_saved=paths[B1_KEY], factor_snapshots=ids)


# --------------------------------------------------------------------------- factor and RF data

_SNAPSHOT_SQL: Final = """
SELECT dataset_key, parse_status, payload, response_sha256 FROM reference_data_snapshots
WHERE snapshot_id = %(snapshot_id)s
"""
_OBSERVATIONS_SQL: Final = """
SELECT series_key, observation_date, value, unit FROM reference_data_observations
WHERE snapshot_id = %(snapshot_id)s
ORDER BY series_key, observation_date
"""

Observation = tuple[str, Any, Decimal, str]


def observations_sha256(rows: Sequence[Observation]) -> str:
    """sha256 of the canonical JSON of ``[series_key, observation_date, str(value), unit]`` rows, sorted by series key
    then date (spec §"Registration", factor and RF data). ``value`` is the stored ``NUMERIC`` as a ``Decimal``."""
    ordered = sorted(([key, day, str(value), unit] for key, day, value, unit in rows), key=lambda r: (r[0], r[1]))
    return hashlib.sha256(canonical_json(ordered)).hexdigest()


def _read_snapshot(
    conn: psycopg.Connection[Any], dataset: str, snapshot_id: int
) -> tuple[SnapshotDigests, list[Observation]]:
    row = conn.execute(_SNAPSHOT_SQL, {"snapshot_id": snapshot_id}).fetchone()
    if row is None:
        raise ReportError(f"factor snapshot {snapshot_id} does not exist")
    dataset_key, status, payload, stored = row
    if dataset_key != dataset or status != "accepted":
        raise ReportError(f"factor snapshot {snapshot_id} is {status!r} {dataset_key!r}, not accepted {dataset!r}")
    response = hashlib.sha256(bytes(payload)).hexdigest()
    if response != stored:
        raise ReportError(f"factor snapshot {snapshot_id}'s payload sha256 {response} is not its stored {stored}")
    observations: list[Observation] = conn.execute(_OBSERVATIONS_SQL, {"snapshot_id": snapshot_id}).fetchall()
    return SnapshotDigests(response, observations_sha256(observations)), observations


def _snapshot_transaction(conn: psycopg.Connection[Any]) -> None:
    """Make ``conn``'s next transaction read-only repeatable read, so every snapshot is read from one database state;
    refused on a connection already in a transaction, whose isolation could not change."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise ReportError("the factor read needs a connection with no open transaction")
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    conn.read_only = True


def snapshot_digests(conn: psycopg.Connection[Any], snapshot_ids: Mapping[str, int]) -> dict[str, SnapshotDigests]:
    """The integrity-only read (slice 4): each snapshot's two digests, after the row checks; no value is returned."""
    _snapshot_transaction(conn)
    with conn.transaction():
        return {dataset: _read_snapshot(conn, dataset, i)[0] for dataset, i in sorted(snapshot_ids.items())}


def read_factors(
    conn: psycopg.Connection[Any], snapshot_ids: Mapping[str, int], pins: Mapping[str, SnapshotDigests]
) -> dict[str, dict[Month, float]]:
    """G1's series (step 0's ``FACTOR_DATASETS``: FF5, RF and momentum), by series key then month, from snapshots
    whose digests equal the declared ones. Every observation of a used series must be in ``FACTOR_UNIT`` and appear
    once per calendar month."""
    _snapshot_transaction(conn)
    factors: dict[str, dict[Month, float]] = {}
    with conn.transaction():
        for dataset, series_keys in FACTOR_DATASETS:
            digests, observations = _read_snapshot(conn, dataset, snapshot_ids[dataset])
            if digests != pins[dataset]:
                raise ReportError(f"factor snapshot {snapshot_ids[dataset]} ({dataset}) differs from its pins")
            for key in series_keys:
                rows = [(day, value, unit) for k, day, value, unit in observations if k == key]
                if not rows or any(unit != FACTOR_UNIT for _, _, unit in rows):
                    raise ReportError(f"{dataset}/{key}: no rows, or a unit other than {FACTOR_UNIT}")
                values = {(day.year, day.month): float(value) for day, value, _ in rows}
                if len(values) != len(rows) or key in factors:
                    raise ReportError(f"{dataset}/{key} repeats a calendar month or another dataset's series")
                factors[key] = values
    return factors


__all__ = [
    "B1_KEY",
    "FF12_LABEL",
    "QMJ_LABEL",
    "STAGE_A_LABEL",
    "STEP0_LABEL",
    "STEP0_MANIFEST",
    "STEP0_PATHS",
    "STEP0_RUN",
    "TABLE9_LABEL",
    "DeclaredPins",
    "SnapshotDigests",
    "Step0",
    "declared_pins",
    "observations_label",
    "observations_sha256",
    "read_factors",
    "read_step0",
    "require_table9",
    "response_label",
    "snapshot_digests",
]
