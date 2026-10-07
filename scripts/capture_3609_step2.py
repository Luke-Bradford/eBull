"""#3609 step 2's data-capture manifest: the second freeze stage, written after stage B and before any report.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"Registration" ("The data-capture
manifest", ledger step 4, "Every consumed file is verified immediately before use") and §"Slices" (slice 5, capture
lifecycle).

**Format** (``CAPTURE_SCHEMA``), canonical JSON, one file per capturing run at :func:`capture_path`, created
exclusively. The run id names the file and the ``data_frozen`` row's sha256 pins its bytes, so the committed row is
the whole binding:

* ``schema``, ``trial_id``, ``run_id`` and ``access_id`` (the run's hold-out access);
* ``sub``: the extended SUB artefact's directory, its manifest sha256 and every file it lists (path to sha256);
* ``stage_b``: the stage-B artefact's directory, its manifest sha256, its pin map, every frozen input (path to
  sha256), and its published rows (``sha256`` and ``content_sha256``) and census sha256.

The input hashes repeat what each artefact's manifest already pins; :func:`verify_capture` requires them equal, so
the capture states what was frozen without opening either artefact.

**Writers.** :func:`freeze_capture` is the capturing run's ``data_frozen`` step: its gate is the declaration at this
checkout and the run's ``started``, ``access_recorded``, ``sub_published`` and ``stage_b_published`` rows with no
``data_frozen`` or terminal row. :func:`reuse_capture` is a later attempt's ``capture_reused`` row, in place of
those three capture rows: it binds the one committed ``data_frozen`` row for the trial (``CAPTURE_AMBIGUOUS``
otherwise) and never publishes or rebuilds. Both verify every file before writing their row. A gate refusal writes
nothing; any failure past it ends the run ``failed`` and removes a manifest the ledger does not name.

**The verifier** (:func:`verify_capture`) reads the capture manifest once, checks its sha256 against the binding,
then verifies the SUB artefact and the stage-B artefact (under stage B's own pin map, the SUB's sha256 included)
reading each file once; the report parses stage B from the bytes it returns.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.factor_book_declaration import TRIAL_ID, CodeHashes, canonical_json, check_declaration
from app.services.factor_book_ledger import (
    CAPTURE_REUSED_EVENT,
    COMMITTED_LEDGER_PATH,
    DATA_FROZEN_EVENT,
    LEDGER_PATH,
    Binding,
    StageBAccessError,
    capture_binding,
    end_run_failed,
    recorded_access_id,
    require_committed_access,
    run_event,
)
from app.services.factor_panel import PanelError
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.trial_register import TRIAL_REGISTER
from scripts import publish_3609_step2_sub as sub_publisher
from scripts.build_3609_factor_panel import (
    RESEARCH_ROOT,
    STAGE_B_EVENT,
    VerifiedArtefact,
    read_verified_artefact,
    stage_b_pins,
)

CAPTURE_SCHEMA: Final = "factor-book-3609-step2-capture-v1"
CAPTURE_ROOT: Final = RESEARCH_ROOT / "factor_book_3609_step2_capture"
#: The claim ``freeze_capture`` and ``reuse_capture`` hold (:func:`ledger_claim`).
CAPTURE_CLAIM: Final = "capture"
_SUB_EVENT: Final = sub_publisher.EVENT
_CAPTURE_EVENTS: Final = (_SUB_EVENT, STAGE_B_EVENT, DATA_FROZEN_EVENT)


def capture_path(run_id: str, root: Path = CAPTURE_ROOT) -> Path:
    """The capturing run's manifest: named by its run id, pinned by the ``data_frozen`` row's sha256."""
    return root / f"{run_id}.json"


def write_exclusive(path: Path, document: bytes) -> None:
    """Create ``path`` (an existing file is refused, never overwritten), write and fsync it and its directory entry,
    so a durable ledger row never names a missing file. A file this call created is removed if any later step
    fails."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("xb")
    try:
        with handle:
            handle.write(document)
            handle.flush()
            os.fsync(handle.fileno())
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


@contextlib.contextmanager
def ledger_claim(ledger: Path, step: str) -> Iterator[None]:
    """An exclusive lock beside the run ledger, held by a step from its gate's ledger read through its row's append,
    so two concurrent invocations cannot both pass the gate. ``append_ledger`` locks the ledger file itself, so this
    lock is a separate file, one per step (``<ledger>.<step>.lock``)."""
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with (ledger.parent / f"{ledger.name}.{step}.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _read_once(path: Path, digest: str, what: str) -> bytes:
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != digest:
        raise PanelError(f"{what} digest moved: {path}")
    return payload


def verify_sub(artefact: Path, manifest_sha256: str) -> dict[str, Any]:
    """The extended SUB artefact's manifest, after its digest, schema, file list and every file's sha256 are checked,
    each file read once. Stage B consumed these files; the report re-verifies them and parses none."""
    manifest = json.loads(_read_once(artefact / sub_publisher.MANIFEST_FILE, manifest_sha256, "SUB manifest"))
    if manifest.get("schema") != sub_publisher.MANIFEST_SCHEMA:
        raise PanelError(f"SUB schema {manifest.get('schema')!r}, expected {sub_publisher.MANIFEST_SCHEMA!r}")
    files = _sub_files(manifest)
    listed = {p.relative_to(artefact).as_posix() for p in (artefact / "inputs").rglob("*") if p.is_file()}
    if listed != set(files):
        raise PanelError("the SUB artefact's inputs/ does not match its manifest's file list")
    for relative, digest in files.items():
        _read_once(artefact / relative, digest, "SUB file")
    return manifest


def _sub_files(manifest: Mapping[str, Any]) -> dict[str, str]:
    files = {entry["path"]: entry["sha256"] for entry in manifest["fsds_sub"]}
    if len(files) != len(manifest["fsds_sub"]):
        raise PanelError("the SUB manifest lists a path twice")
    return files


def _describe(
    run_id: str,
    access_id: int,
    sub: tuple[Path, str],
    stage_b: tuple[Path, str],
    sub_manifest: Mapping[str, Any],
    stage_b_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """The data-capture manifest of two artefacts already verified; both must carry this run's id and access."""
    for what, manifest in (("SUB", sub_manifest), ("stage-B", stage_b_manifest)):
        published = (manifest.get("run_id"), manifest.get("access_id"))
        if published != (run_id, access_id):
            raise PanelError(f"the {what} artefact was published by run and access {published}, not {run_id}")
    return {
        "schema": CAPTURE_SCHEMA,
        "trial_id": TRIAL_ID,
        "run_id": run_id,
        "access_id": access_id,
        "sub": {"artefact": str(sub[0]), "manifest_sha256": sub[1], "files": _sub_files(sub_manifest)},
        "stage_b": {
            "artefact": str(stage_b[0]),
            "manifest_sha256": stage_b[1],
            "pins": stage_b_manifest["pinned_manifests"],
            "inputs": stage_b_manifest["inputs"],
            "rows": {k: stage_b_manifest["rows"][k] for k in ("sha256", "content_sha256")},
            "census_sha256": stage_b_manifest["census"]["sha256"],
        },
    }


def capture_manifest(run_id: str, access_id: int, sub: tuple[Path, str], stage_b: tuple[Path, str]) -> dict[str, Any]:
    """The data-capture manifest of a run's two published artefacts, each verified as it is recorded: the SUB
    first, then stage B under the pin map naming that SUB."""
    sub_manifest = verify_sub(*sub)
    stage_b_manifest = read_verified_artefact(*stage_b, pins=stage_b_pins(sub[1])).manifest
    return _describe(run_id, access_id, sub, stage_b, sub_manifest, stage_b_manifest)


@dataclass(frozen=True)
class Capture:
    """A verified capture: its manifest, the SUB's manifest, and stage B with the bytes of every kept input."""

    manifest: Mapping[str, Any]
    sub: Mapping[str, Any]
    stage_b: VerifiedArtefact


def verify_capture(path: Path, manifest_sha256: str, keep: Iterable[str] = ()) -> Capture:
    """Verify a capture manifest against its pinned sha256, then both artefacts against it, each file read once.

    Refuses (:class:`PanelError`) on a moved digest, another schema or trial, or an artefact whose manifest no
    longer states what the capture recorded. ``keep`` is :func:`read_verified_artefact`'s, for stage B."""
    recorded = json.loads(_read_once(path, manifest_sha256, "data-capture manifest"))
    if recorded.get("schema") != CAPTURE_SCHEMA or recorded.get("trial_id") != TRIAL_ID:
        raise PanelError(f"{path} is not a {CAPTURE_SCHEMA} capture of {TRIAL_ID}")
    sub = (Path(recorded["sub"]["artefact"]), recorded["sub"]["manifest_sha256"])
    stage_b = (Path(recorded["stage_b"]["artefact"]), recorded["stage_b"]["manifest_sha256"])
    sub_manifest = verify_sub(*sub)
    verified = read_verified_artefact(*stage_b, pins=stage_b_pins(sub[1]), keep=keep)
    restated = _describe(recorded["run_id"], recorded["access_id"], sub, stage_b, sub_manifest, verified.manifest)
    if restated != recorded:
        raise PanelError(f"the artefacts no longer state what {path} recorded")
    return Capture(manifest=recorded, sub=sub_manifest, stage_b=verified)


def _artefact(rows: list[dict[str, Any]], run_id: str, event: str) -> tuple[Path, str]:
    row = run_event(rows, run_id, event)
    return Path(row["artefact"]), str(row["manifest_sha256"])


def freeze_capture(
    run_id: str,
    *,
    confirm_access: Callable[[str, int], None],
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
    root: Path = CAPTURE_ROOT,
) -> str:
    """Ledger step 4: write the run's data-capture manifest, then its ``data_frozen`` row; returns the sha256.

    Refuses while the committed ledger already binds a capture for the trial: a later attempt reuses it."""
    with ledger_claim(ledger, CAPTURE_CLAIM):
        rows = read_ledger(committed_ledger, ledger)
        check_declaration(TRIAL_REGISTER, rows, CodeHashes.current())
        access_id = recorded_access_id(rows, run_id, before=DATA_FROZEN_EVENT)
        bound = [row for row in read_ledger(committed_ledger) if row.get("event") == DATA_FROZEN_EVENT]
        if any(row.get("trial_id") == TRIAL_ID or not row.get("trial_id") for row in bound):
            raise StageBAccessError(f"the committed ledger already holds a {DATA_FROZEN_EVENT!r} row; reuse it")
        sub, stage_b = _artefact(rows, run_id, _SUB_EVENT), _artefact(rows, run_id, STAGE_B_EVENT)
        confirm_access(run_id, access_id)
        return _freeze(run_id, access_id, sub, stage_b, ledger=ledger, path=capture_path(run_id, root))


def _freeze(
    run_id: str, access_id: int, sub: tuple[Path, str], stage_b: tuple[Path, str], *, ledger: Path, path: Path
) -> str:
    written = False
    try:
        document = canonical_json(capture_manifest(run_id, access_id, sub, stage_b))
        write_exclusive(path, document)
        written = True
        digest = hashlib.sha256(document).hexdigest()
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": DATA_FROZEN_EVENT,
                "at": datetime.now(UTC).isoformat(),
                "trial_id": TRIAL_ID,
                "manifest_sha256": digest,
            },
        )
        return digest
    except BaseException as exc:
        try:
            if written:
                path.unlink(missing_ok=True)
        finally:
            end_run_failed(ledger, run_id, DATA_FROZEN_EVENT, exc)
        raise


def reuse_capture(
    run_id: str,
    *,
    confirm_access: Callable[[str, int], None],
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
    root: Path = CAPTURE_ROOT,
) -> str:
    """A later attempt's ``capture_reused`` row naming the trial's one committed capture, after verifying it."""
    with ledger_claim(ledger, CAPTURE_CLAIM):
        committed = read_ledger(committed_ledger)
        rows = read_ledger(committed_ledger, ledger)
        check_declaration(TRIAL_REGISTER, rows, CodeHashes.current())
        access_id = recorded_access_id(rows, run_id, before=CAPTURE_REUSED_EVENT)
        own = [row["event"] for row in rows if row.get("run_id") == run_id and row["event"] in _CAPTURE_EVENTS]
        if own:
            raise StageBAccessError(f"run {run_id} has written capture rows {own}; it cannot reuse another's")
        binding = capture_binding(committed)
        if binding.capturing_run_id == run_id:
            raise StageBAccessError(f"run {run_id} is the binding's own capture")
        confirm_access(run_id, access_id)
        return _reuse(run_id, binding, ledger=ledger, root=root)


def _reuse(run_id: str, binding: Binding, *, ledger: Path, root: Path) -> str:
    try:
        verify_capture(capture_path(binding.capturing_run_id, root), binding.manifest_sha256)
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": CAPTURE_REUSED_EVENT,
                "at": datetime.now(UTC).isoformat(),
                "capturing_run_id": binding.capturing_run_id,
                "manifest_sha256": binding.manifest_sha256,
            },
        )
        return binding.manifest_sha256
    except BaseException as exc:
        end_run_failed(ledger, run_id, CAPTURE_REUSED_EVENT, exc)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--reuse", action="store_true", help="bind the trial's committed capture instead")
    parser.add_argument("--ledger", type=Path, default=LEDGER_PATH)
    args = parser.parse_args()

    def confirm(run_id: str, access_id: int) -> None:
        # A fresh connection: it sees the access row only if the run committed it.
        with psycopg.connect(settings.database_url) as conn:
            require_committed_access(conn, run_id, access_id)

    step = reuse_capture if args.reuse else freeze_capture
    digest = step(args.run_id, confirm_access=confirm, ledger=args.ledger)
    print(json.dumps({"run_id": args.run_id, "manifest_sha256": digest}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
