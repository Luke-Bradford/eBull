"""#3609 step 2's declared report run: the ledger around :func:`~scripts.report_3609_step2_assembly.evaluate`.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"Registration" (ledger steps 5 and 6,
canonical JSON, labels) and §"Slices" (capture lifecycle, "The report's ledger revision"). In order:

1. **The gate** (:func:`~app.services.factor_book_ledger.check_report_gate`), before any evaluation input is read:
   the binding, the attempt's own rows and every freeze re-check. A refusal here writes no row, as the builder's and
   the SUB publisher's gates write none: nothing has been read.
2. **``report_started``**, with the report's HEAD and the binding.
3. **The evaluation** of the bound capture. Loading and pin-checking its inputs against the data-capture manifest is
   slice 5's, which defines that manifest's format, so the caller passes it as ``evaluate_run``.
4. **The output file** (canonical JSON, created exclusively), then the terminal row naming its sha256, and only then
   is anything printed. A ``REFUSED`` verdict (a :class:`~app.services.factor_book.BookRefusal` from the
   evaluation) is a decision-rule outcome, so it ends ``completed`` with its reason code and the refusal payload.
   Any other failure past step 2 ends the run ``failed``, and an output file the ledger does not name is deleted.
   The code hashes are taken again after the evaluation; a checkout that moved during it ends the run ``failed``.

This module is the report's entry point, so it is a construction-hash root
(:data:`~app.services.factor_book_declaration.CONSTRUCTION_ROOTS`).
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from app.services.factor_book import BookRefusal
from app.services.factor_book_declaration import TRIAL_ID, CodeHashes, DeclarationError, canonical_json
from app.services.factor_book_ledger import (
    COMMITTED_LEDGER_PATH,
    LEDGER_PATH,
    Binding,
    check_report_gate,
    end_run_failed,
    report_started_row,
)
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.trial_register import TrialRegister
from scripts.report_3609_step2_assembly import LABELS, SURVIVORSHIP, Report, payload

REFUSED: Final = "REFUSED"
OUTPUT_ROOT: Final = LEDGER_PATH.parent
REPORT_STEP: Final = "report"


@dataclass(frozen=True)
class ReportOutcome:
    """What the terminal ``completed`` row recorded: the status, its reason and the output file with its sha256."""

    status: str
    reason: str | None
    path: Path
    sha256: str


def refused_payload(refusal: BookRefusal) -> dict[str, Any]:
    """Verdict order step 1: the status and reason code, the refusal's detail (the offending formation, name and
    draw, and the counts, as the raising check states them) and the labels; no path, gate or diagnostic."""
    return {
        "verdict": {
            "line": f"{REFUSED}: {refusal.code}. {SURVIVORSHIP}.",
            "status": REFUSED,
            "reason": refusal.code,
            "detail": str(refusal),
        },
        "labels": dict(LABELS),
    }


def _write_exclusive(path: Path, document: bytes) -> None:
    """Create ``path`` (an existing file is refused, never overwritten), write and fsync it and its directory entry,
    so a durable ``completed`` row never names a missing file. A file this call created is removed if any later
    step fails."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("xb")  # one report per run id
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
def _report_claim(ledger: Path) -> Iterator[None]:
    """An exclusive lock beside the run ledger, held from the ledger read through the ``report_started`` append, so
    two concurrent reports of one run cannot both pass the gate. ``append_ledger`` locks the ledger file itself, so
    this lock is a separate file."""
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with (ledger.parent / f"{ledger.name}.report.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_report(
    run_id: str,
    *,
    head: str,
    register: TrialRegister,
    evaluate_run: Callable[[Binding], Report],
    out_dir: Path = OUTPUT_ROOT,
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
    current: Callable[[], CodeHashes] = CodeHashes.current,
) -> ReportOutcome:
    """Ledger steps 5 and 6 around one evaluation of the bound capture; see the module docstring for the order.

    ``head`` is the report's HEAD, from a clean checkout equal to ``origin/main`` after a fetch (the caller's
    check); the committed ledger is read from that checkout."""
    hashes = current()
    with _report_claim(ledger):
        committed = read_ledger(committed_ledger)
        rows = read_ledger(committed_ledger, ledger)
        _, binding = check_report_gate(register, committed, rows, run_id, hashes)
        append_ledger(ledger, report_started_row(run_id, head, binding))
    out = out_dir / f"{run_id}-report.json"
    written = False
    try:
        try:
            report = evaluate_run(binding)
        except BookRefusal as refusal:
            body, status, reason = refused_payload(refusal), REFUSED, refusal.code
        else:
            body, status, reason = payload(report), report.verdict.status, report.verdict.reason
        run_block = {
            "run_id": run_id,
            "trial_id": TRIAL_ID,
            "git_sha": head,
            "capturing_run_id": binding.capturing_run_id,
            "data_frozen_sha256": binding.manifest_sha256,
            **hashes.by_label(),
        }
        if "run" in body:
            raise ValueError("the report payload already holds a 'run' block")
        # The gate checked the hashes before evaluation; code edited on disk during it ends the run ``failed``. Not
        # a ``BookRefusal``, which a caller reads as a ``REFUSED`` verdict.
        if current() != hashes:
            raise DeclarationError(f"run {run_id}: the checkout's code hashes moved during the run")
        document = canonical_json({**body, "run": run_block})
        digest = hashlib.sha256(document).hexdigest()
        _write_exclusive(out, document)
        written = True
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": "completed",
                "at": datetime.now(UTC).isoformat(),
                "trial_id": TRIAL_ID,
                "status": status,
                "reason": reason,
                "output": str(out),
                "output_sha256": digest,
            },
        )
    except BaseException as exc:
        try:
            if written:
                out.unlink(missing_ok=True)
        finally:
            end_run_failed(ledger, run_id, REPORT_STEP, exc)
        raise
    return ReportOutcome(status=status, reason=reason, path=out, sha256=digest)


__all__ = ["OUTPUT_ROOT", "REFUSED", "REPORT_STEP", "ReportOutcome", "refused_payload", "run_report"]
