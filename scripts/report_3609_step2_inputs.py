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

import contextlib
import hashlib
import json
import re
from collections.abc import Iterator, Mapping, Sequence
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


_SHA256: Final = re.compile(r"[0-9a-f]{64}")


def _pin(evidence: str, label: str) -> str:
    value = evidence_value(evidence, label)
    if not _SHA256.fullmatch(value):
        raise ReportError(f"the declaration's {label} {value!r} is not a lowercase hex sha256")
    return value


def declared_pins(evidence: str) -> DeclaredPins:
    """The pins the declaration's ``evidence`` names, each a lowercase hex sha256; the three that are also code
    constants must equal them."""
    for label, constant in (
        (STAGE_A_LABEL, STAGE_A_MANIFEST_SHA256),
        (FF12_LABEL, SICCODES12_SHA256),
        (QMJ_LABEL, QMJ_PDF_SHA256),
    ):
        if _pin(evidence, label) != constant:
            raise ReportError(f"the declaration's {label} is not this checkout's pinned {constant}")
    return DeclaredPins(
        stage_a_manifest_sha256=STAGE_A_MANIFEST_SHA256,
        step0_manifest_sha256=_pin(evidence, STEP0_LABEL),
        table9_sha256=_pin(evidence, TABLE9_LABEL),
        snapshots={
            dataset: SnapshotDigests(
                _pin(evidence, response_label(dataset)), _pin(evidence, observations_label(dataset))
            )
            for dataset in _DATASETS
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
    """Step 0's manifest against the declared sha256, then its ``paths.json`` against the manifest; each read once.
    A file that verifies but lacks a field the report reads refuses as a mismatch does."""
    manifest = json.loads(_read_once(run / STEP0_MANIFEST, manifest_sha256, "step 0 manifest"))
    try:
        paths = json.loads(_read_once(run / STEP0_PATHS, manifest["sha256"]["paths"], "step 0 paths.json"))
        snapshots, b1 = dict(manifest["factor_snapshots"]), paths[B1_KEY]
    except (KeyError, TypeError, ValueError) as exc:
        raise ReportError(f"step 0's manifest or paths.json lacks a field the report reads: {exc!r}") from exc
    if sorted(snapshots) != list(_DATASETS):
        raise ReportError(f"step 0 pins factor snapshots for {sorted(snapshots)}, expected {list(_DATASETS)}")
    if any(type(i) is not int or i <= 0 for i in snapshots.values()):
        raise ReportError(f"step 0's factor snapshot ids {snapshots} are not positive integers")
    if not isinstance(b1, dict):
        raise ReportError(f"step 0's {B1_KEY!r} path is a {type(b1).__name__}, not an object")
    return Step0(manifest=manifest, b1_saved=b1, factor_snapshots=snapshots)


# --------------------------------------------------------------------------- factor and RF data

_DATASETS: Final = tuple(sorted(dataset for dataset, _ in FACTOR_DATASETS))
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


@contextlib.contextmanager
def _snapshot_transaction(conn: psycopg.Connection[Any], snapshot_ids: Mapping[str, int]) -> Iterator[None]:
    """One read-only repeatable-read transaction on ``conn``, so every snapshot is read from one database state; the
    connection's isolation level and read-only flag are restored after it. Refused on a connection already in a
    transaction, whose isolation could not change, or for datasets other than step 0's two."""
    if sorted(snapshot_ids) != list(_DATASETS):
        raise ReportError(f"factor snapshots for {sorted(snapshot_ids)}, expected {list(_DATASETS)}")
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise ReportError("the factor read needs a connection with no open transaction")
    isolation, read_only = conn.isolation_level, conn.read_only
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    conn.read_only = True
    try:
        with conn.transaction():
            yield
    finally:
        conn.isolation_level, conn.read_only = isolation, read_only


def snapshot_digests(conn: psycopg.Connection[Any], snapshot_ids: Mapping[str, int]) -> dict[str, SnapshotDigests]:
    """The integrity-only read (slice 4): each snapshot's two digests, after the row checks; no value is returned."""
    with _snapshot_transaction(conn, snapshot_ids):
        return {dataset: _read_snapshot(conn, dataset, snapshot_ids[dataset])[0] for dataset in _DATASETS}


def read_factors(
    conn: psycopg.Connection[Any], snapshot_ids: Mapping[str, int], pins: Mapping[str, SnapshotDigests]
) -> dict[str, dict[Month, float]]:
    """G1's series (step 0's ``FACTOR_DATASETS``: FF5, RF and momentum), by series key then month, from snapshots
    whose digests equal the declared ones. Every observation of a used series must be finite, in ``FACTOR_UNIT``, and
    appear once per calendar month."""
    if sorted(pins) != list(_DATASETS):
        raise ReportError(f"factor pins for {sorted(pins)}, expected {list(_DATASETS)}")
    factors: dict[str, dict[Month, float]] = {}
    with _snapshot_transaction(conn, snapshot_ids):
        for dataset, series_keys in FACTOR_DATASETS:
            digests, observations = _read_snapshot(conn, dataset, snapshot_ids[dataset])
            if digests != pins[dataset]:
                raise ReportError(f"factor snapshot {snapshot_ids[dataset]} ({dataset}) differs from its pins")
            for key in series_keys:
                rows = [(day, value, unit) for k, day, value, unit in observations if k == key]
                if not rows or any(unit != FACTOR_UNIT or not value.is_finite() for _, value, unit in rows):
                    raise ReportError(
                        f"{dataset}/{key}: no rows, a non-finite value or a unit other than {FACTOR_UNIT}"
                    )
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
