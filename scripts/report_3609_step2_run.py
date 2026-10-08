"""#3609 step 2's declared report run: the ledger around :func:`~scripts.report_3609_step2_assembly.evaluate`.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"Registration" (ledger steps 5 and 6,
canonical JSON, labels) and §"Slices" (capture lifecycle, "The report's ledger revision"). In order:

1. **The gate** (:func:`~app.services.factor_book_ledger.check_report_gate`), before any evaluation input is read:
   the binding, the attempt's own rows and every freeze re-check. A refusal here writes no row, as the builder's and
   the SUB publisher's gates write none: nothing has been read.
2. **``report_started``**, with the report's HEAD and the binding.
3. **The evaluation** of the bound capture under the gate's declared trial (:func:`evaluate_run`, passed in so
   tests can stand in for it): every input verified against the declaration's pins and the binding, then
   :func:`~scripts.report_3609_step2_assembly.evaluate`.
4. **The output file** (canonical JSON, created exclusively), then the terminal row naming its sha256, and only then
   is anything printed. A ``REFUSED`` verdict (a :class:`~app.services.factor_book.BookRefusal` from the
   evaluation) is a decision-rule outcome, so it ends ``completed`` with its reason code and the refusal payload.
   Any other failure past step 2 ends the run ``failed``, and an output file the ledger does not name is deleted.
   The code hashes are taken again after the evaluation; a checkout that moved during it ends the run ``failed``.

The command line (:func:`main`) first requires a clean checkout at ``origin/main`` after a fetch
(:func:`report_head`).

This module is the report's entry point, so it is a construction-hash root
(:data:`~app.services.factor_book_declaration.CONSTRUCTION_ROOTS`).
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
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
from app.services.factor_book_reference import STAGE_B_FIRST_FORMATION, STAGE_B_LAST_FORMATION, load_ff12
from app.services.factor_panel import formation_months
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.trial_register import TRIAL_REGISTER, DeclaredTrial, TrialRegister
from scripts.build_3609_factor_panel import read_verified_artefact
from scripts.capture_3609_step2 import CAPTURE_ROOT, capture_path, ledger_claim, verify_capture, write_exclusive
from scripts.report_3609_baselines import COSTS
from scripts.report_3609_step2 import CONSUMED_INPUTS, STAGE_A_ARTEFACT, PanelMonth, ReportError, read_panel
from scripts.report_3609_step2_assembly import LABELS, SURVIVORSHIP, Report, evaluate, payload
from scripts.report_3609_step2_inputs import (
    B1_CLOSE_SESSION,
    STEP0_RUN,
    declared_b1_close,
    declared_pins,
    read_factors,
    read_step0,
    require_table9,
)
from scripts.report_3609_step2_universe import NYSE_CUTOFFS, read_cutoffs

REFUSED: Final = "REFUSED"
_REPO_ROOT: Final = Path(__file__).resolve().parents[1]
OUTPUT_ROOT: Final = LEDGER_PATH.parent
REPORT_STEP: Final = "report"
#: Each stage's formations (§"The book"): stage A 2014-09 .. 2021-04, stage B 2021-05 .. 2024-07.
STAGE_GRIDS: Final = (formation_months(), formation_months(STAGE_B_FIRST_FORMATION, STAGE_B_LAST_FORMATION))


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


def stage_cutoffs(stages: Sequence[tuple[Sequence[PanelMonth], Mapping[date, float]]]) -> dict[date, float]:
    """Each formation's NYSE cutoff from its own stage's frozen snapshot (#3666 item 21), keyed by formation as
    :func:`evaluate` reads it. A date both snapshots publish must agree (``CUTOFF_INVALID``). A formation its stage's
    snapshot does not publish is left out: §"Diagnostics", Universe, prints an unpublished month ``unavailable``."""
    for (_, first), (_, second) in itertools.combinations(stages, 2):
        differ = sorted(day for day in first.keys() & second.keys() if first[day] != second[day])
        if differ:
            raise BookRefusal("CUTOFF_INVALID", f"the stages' frozen cutoffs differ at {differ[:3]}")
    return {m.formation: cutoffs[m.formation] for panel, cutoffs in stages for m in panel if m.formation in cutoffs}


def _connect() -> psycopg.Connection[Any]:
    return psycopg.connect(settings.database_url)


def evaluate_run(
    trial: DeclaredTrial,
    binding: Binding,
    *,
    connect: Callable[[], psycopg.Connection[Any]] = _connect,
    stage_a: Path = STAGE_A_ARTEFACT,
    capture_root: Path = CAPTURE_ROOT,
    step0_run: Path = STEP0_RUN,
) -> Report:
    """Step 3 of the module docstring: every evaluation input, verified against the declaration's pins and the
    binding immediately before use and parsed from the bytes hashed, then :func:`evaluate`.

    Stage A is the pinned artefact, stage B the bound capture's (each panel artefact's Table 9 the declared one);
    step 0's B1 path and factor snapshot ids come from its declared manifest, and the factor and RF data from those
    snapshots against their declared digests. A pin mismatch raises (a ``PanelError`` or ``ReportError``), so the run
    ends ``failed``; a data refusal is a :class:`BookRefusal`."""
    pins = declared_pins(trial.evidence)
    b1_close = declared_b1_close(trial.evidence)
    keep = (*CONSUMED_INPUTS, NYSE_CUTOFFS)
    stages = (
        read_verified_artefact(stage_a, pins.stage_a_manifest_sha256, keep=keep),
        verify_capture(capture_path(binding.capturing_run_id, capture_root), binding.manifest_sha256, keep).stage_b,
    )
    for verified in stages:
        require_table9(verified, pins.table9_sha256)
    ff12 = load_ff12()
    panels = [read_panel(verified, ff12) for verified in stages]
    for stage, panel, grid in zip("AB", panels, STAGE_GRIDS, strict=True):
        if tuple(month.formation for month in panel) != grid:
            raise ReportError(f"stage {stage}'s formations are not its declared {grid[0]} .. {grid[-1]} grid")
    first = panels[0][0].session
    if first != B1_CLOSE_SESSION:
        raise ReportError(f"stage A's first session is {first}, B1's declared close is at {B1_CLOSE_SESSION}")
    cutoffs = stage_cutoffs(
        [(panel, read_cutoffs(v.files[NYSE_CUTOFFS])) for panel, v in zip(panels, stages, strict=True)]
    )
    step0 = read_step0(pins.step0_manifest_sha256, step0_run)
    with connect() as conn:
        factors = read_factors(conn, step0.factor_snapshots, pins.snapshots)
    return evaluate(
        [month for panel in panels for month in panel],
        factors=factors,
        cutoffs=cutoffs,
        b1_saved=step0.b1_saved,
        b1_close=b1_close,
        costs=COSTS,
    )


def run_report(
    run_id: str,
    *,
    head: str,
    register: TrialRegister,
    evaluate_run: Callable[[DeclaredTrial, Binding], Report],
    out_dir: Path = OUTPUT_ROOT,
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
    current: Callable[[], CodeHashes] = CodeHashes.current,
) -> ReportOutcome:
    """Ledger steps 5 and 6 around one evaluation of the bound capture; see the module docstring for the order.

    ``head`` is the report's HEAD, from a clean checkout equal to ``origin/main`` after a fetch (the caller's
    check); the committed ledger is read from that checkout.

    Raises :class:`BookRefusal` only from the gate: the caller prints ``REFUSED``, and no row was written. Any
    exception after ``report_started`` (a :class:`DeclarationError` for code that moved during the run, an I/O
    error, a defect) means the run has ended ``failed``. A ``REFUSED`` verdict from the evaluation is returned as
    the outcome, never raised."""
    hashes = current()
    with ledger_claim(ledger, REPORT_STEP):
        committed = read_ledger(committed_ledger)
        rows = read_ledger(committed_ledger, ledger)
        trial, binding = check_report_gate(register, committed, rows, run_id, hashes)
        append_ledger(ledger, report_started_row(run_id, head, binding))
    out = out_dir / f"{run_id}-report.json"
    written = False
    try:
        try:
            report = evaluate_run(trial, binding)
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
        write_exclusive(out, document)
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


# --------------------------------------------------------------------------- the command line


def _git(*args: str) -> str:
    """``git`` in this checkout, ``GIT_*`` scrubbed: a hook's ``GIT_DIR`` would point it at another repository."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    done = subprocess.run(
        ["git", *args], cwd=_REPO_ROOT, env=env, check=True, capture_output=True, text=True, timeout=300
    )
    return done.stdout.strip()


def report_head(git: Callable[..., str] = _git) -> str:
    """§"Slices", "The report's ledger revision": the HEAD of a clean checkout equal to ``origin/main`` after a
    fetch, so the committed ledger it reads is the merged one. Refuses before the gate, writing no row."""
    git("fetch", "--quiet", "origin", "main")
    if git("status", "--porcelain"):
        raise ReportError("refusing to report from a checkout with uncommitted or untracked files")
    head, merged = git("rev-parse", "HEAD"), git("rev-parse", "origin/main")
    if head != merged:
        raise ReportError(f"HEAD {head} is not origin/main {merged} after a fetch")
    return head


def main(argv: Sequence[str] | None = None) -> int:
    """Ledger steps 5 and 6 for one attempt. Prints nothing until the run has ended: a gate refusal (no row written,
    exit 1), or the terminal ``completed`` row, after which the output file is read back against the sha256 that row
    names and its verdict line printed (exit 0, ``REFUSED`` verdicts included)."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ledger", type=Path, default=LEDGER_PATH)
    args = parser.parse_args(argv)
    head = report_head()
    try:
        outcome = run_report(
            args.run_id, head=head, register=TRIAL_REGISTER, evaluate_run=evaluate_run, ledger=args.ledger
        )
    except BookRefusal as refusal:
        print(json.dumps({"run_id": args.run_id, "status": REFUSED, "reason": refusal.code, "detail": str(refusal)}))
        return 1
    document = outcome.path.read_bytes()
    if hashlib.sha256(document).hexdigest() != outcome.sha256:
        raise ReportError(f"{outcome.path} is not the file the ledger's completed row names")
    print(json.loads(document)["verdict"]["line"])
    print(
        json.dumps(
            {
                "run_id": args.run_id,
                "status": outcome.status,
                "reason": outcome.reason,
                "output": str(outcome.path),
                "output_sha256": outcome.sha256,
            }
        )
    )
    return 0


__all__ = [
    "OUTPUT_ROOT",
    "REFUSED",
    "REPORT_STEP",
    "STAGE_GRIDS",
    "ReportOutcome",
    "evaluate_run",
    "main",
    "refused_payload",
    "report_head",
    "run_report",
    "stage_cutoffs",
]


if __name__ == "__main__":
    raise SystemExit(main())
