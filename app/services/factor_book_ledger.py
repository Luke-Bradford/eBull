"""The #3609 step 2 declared run's hold-out identity and its stage-B access gate.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Registration" (PR #3666). Every stage-B read, the
extended SUB files first, happens only after the run's ``started`` ledger row and a committed ``access_recorded``
row. ``record_holdout_access`` does not enforce a declaration for this trial (it runs with
``require_declaration=False``), so the run checks its own ledger and the access log before reading.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import psycopg

from app.services.factor_book import BookRefusal
from app.services.factor_book_declaration import TRIAL_ID, CodeHashes, DeclarationError, check_declaration
from app.services.factor_panel_fidelity import append_ledger
from app.services.trial_register import DeclaredTrial, TrialRegister

STRATEGY_ID: Final = "3609-step2-book"
STRATEGY_VERSION: Final = "v1"

_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
LEDGER_PATH: Final = _REPO_ROOT / "var" / "research" / "3609_step2" / "ledger.jsonl"
COMMITTED_LEDGER_PATH: Final = _REPO_ROOT / "docs" / "research" / "3609-ledger.jsonl"

_TERMINAL_EVENTS: Final = frozenset({"completed", "failed"})

#: Ledger steps 4 and 5 and the reuse row (§"Slices", capture lifecycle). Slice 5 writes the first two:
#: ``data_frozen`` carries ``trial_id`` and the data-capture manifest's ``manifest_sha256``; ``capture_reused``
#: carries ``capturing_run_id`` and that capture's ``manifest_sha256``.
DATA_FROZEN_EVENT: Final = "data_frozen"
CAPTURE_REUSED_EVENT: Final = "capture_reused"
REPORT_STARTED_EVENT: Final = "report_started"
#: The capture rows a reusing attempt writes ``capture_reused`` in place of.
_CAPTURE_EVENTS: Final = ("sub_published", "stage_b_published", DATA_FROZEN_EVENT)
CAPTURE_AMBIGUOUS: Final = "CAPTURE_AMBIGUOUS"
LEDGER_MISMATCH: Final = "LEDGER_MISMATCH"
DECLARATION_MISMATCH: Final = "DECLARATION_MISMATCH"


def access_purpose(run_id: str) -> str:
    """The ``purpose`` the run's ``access_recorded`` row is declared with (spec §"Registration", ledger step 2)."""
    return f"#3609 step 2 declared run {run_id}"


_COMMITTED_ACCESS: Final = """
    SELECT 1 FROM strategy_holdout_accesses
    WHERE access_id = %(access_id)s
      AND strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND result_version = %(run_id)s
      AND access_kind = 'evaluate'
      AND purpose = %(purpose)s
"""


class StageBAccessError(RuntimeError):
    """A stage-B read was attempted before the run's hold-out access was recorded and committed."""


def recorded_access_id(rows: Sequence[Mapping[str, Any]], run_id: str, *, before: str) -> int:
    """The run's ``access_id``, once its ledger holds ``started`` then ``access_recorded`` and nothing that ends it.

    ``before`` is the event about to be written; a run that already wrote it is refused, so each stage-B step
    runs once per run id.
    """
    events = [row["event"] for row in rows if row.get("run_id") == run_id]
    if not events or events[0] != "started" or events.count("started") != 1:
        raise StageBAccessError(f"run {run_id}: the ledger has no single leading 'started' row")
    recorded = [row for row in rows if row.get("run_id") == run_id and row["event"] == "access_recorded"]
    if len(recorded) != 1:
        raise StageBAccessError(f"run {run_id}: {len(recorded)} 'access_recorded' rows; exactly one is required")
    if _TERMINAL_EVENTS & set(events):
        raise StageBAccessError(f"run {run_id} has already ended")
    if before in events:
        raise StageBAccessError(f"run {run_id} has already written {before!r}")
    access_id = recorded[0].get("access_id")
    if not isinstance(access_id, int) or isinstance(access_id, bool) or access_id <= 0:
        raise StageBAccessError(f"run {run_id}: access_id {access_id!r} is not a positive integer")
    return access_id


def require_committed_access(
    conn: psycopg.Connection[Any],
    run_id: str,
    access_id: int,
    *,
    strategy: tuple[str, str] = (STRATEGY_ID, STRATEGY_VERSION),
    purpose: str | None = None,
) -> None:
    """Refuse unless the access log holds this run's ``evaluate`` row, read on a connection that did not write it,
    so only a committed row is visible. ``strategy`` and ``purpose`` default to step 2's; another trial that builds
    stage B (#3621 v2) passes its own."""
    row = conn.execute(
        _COMMITTED_ACCESS,
        {
            "access_id": access_id,
            "strategy_id": strategy[0],
            "strategy_version": strategy[1],
            "run_id": run_id,
            "purpose": access_purpose(run_id) if purpose is None else purpose,
        },
    ).fetchone()
    if row is None:
        raise StageBAccessError(
            f"run {run_id}: no committed evaluate access {access_id} for {strategy[0]} {strategy[1]}"
        )


def run_event(rows: Sequence[Mapping[str, Any]], run_id: str, event: str) -> Mapping[str, Any]:
    """The run's single ``event`` row; refuses on none or several."""
    found = [row for row in rows if row.get("run_id") == run_id and row.get("event") == event]
    if len(found) != 1:
        raise StageBAccessError(f"run {run_id}: {len(found)} {event!r} rows; exactly one is required")
    return found[0]


def end_run_failed(ledger: Path, run_id: str, step: str, exc: BaseException) -> None:
    """Write the run's ``failed`` row after a stage-B step failed past its gate.

    Stage-B data may already have been read, so the run ends whatever went wrong: a retry needs a fresh run id and
    its own access row, never a second look under this one. A failure to write the row is attached to ``exc``
    rather than replacing it.
    """
    try:
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": "failed",
                "at": datetime.now(UTC).isoformat(),
                "step": step,
                "error": repr(exc),
            },
        )
    except Exception as ledger_error:
        exc.add_note(f"the 'failed' ledger row was not written ({ledger_error!r}); end run {run_id} by hand")


# --------------------------------------------------------------------------- the report's gate


@dataclass(frozen=True)
class Binding:
    """The trial's one committed capture: the run that wrote ``data_frozen`` and the data-capture manifest's sha256."""

    capturing_run_id: str
    manifest_sha256: str


def may_bind_trial(row: Mapping[str, Any]) -> bool:
    """A ``data_frozen`` row that names this trial, or names none (missing, null, empty or not a string), which
    could be this trial's: :func:`capture_binding` and a fresh capture both count it."""
    trial = row.get("trial_id")
    return not isinstance(trial, str) or trial in ("", TRIAL_ID)


def capture_binding(committed: Sequence[Mapping[str, Any]]) -> Binding:
    """The single ``data_frozen`` row for the trial in the committed ledger at HEAD; ``CAPTURE_AMBIGUOUS`` otherwise.

    A ``data_frozen`` row that names no trial (none, null, empty or not a string) could belong to this one, so it
    refuses too."""
    frozen = [row for row in committed if row.get("event") == DATA_FROZEN_EVENT]
    candidates = [row for row in frozen if may_bind_trial(row)]
    ours = [row for row in candidates if row.get("trial_id") == TRIAL_ID]
    unnamed = len(candidates) - len(ours)
    if unnamed or len(ours) != 1:
        raise BookRefusal(
            CAPTURE_AMBIGUOUS,
            f"{len(ours)} committed {DATA_FROZEN_EVENT!r} rows for {TRIAL_ID} and {unnamed} naming no trial; "
            "exactly one for the trial is required",
        )
    run_id, digest = ours[0].get("run_id"), ours[0].get("manifest_sha256")
    if not isinstance(run_id, str) or not run_id or not isinstance(digest, str) or not digest:
        raise BookRefusal(CAPTURE_AMBIGUOUS, f"the committed {DATA_FROZEN_EVENT!r} row lacks its run id or sha256")
    return Binding(capturing_run_id=run_id, manifest_sha256=digest)


def check_report_gate(
    register: TrialRegister,
    committed: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    run_id: str,
    current: CodeHashes,
) -> tuple[DeclaredTrial, Binding]:
    """§"Slices", "The report's ledger revision": every check the report makes before it reads an evaluation input.

    ``committed`` is the committed ledger at HEAD; ``rows`` are those plus the local ledger. Refuses (a
    :class:`BookRefusal`, so the run prints ``REFUSED``) unless:

    * the trial has exactly one committed ``data_frozen`` row (``CAPTURE_AMBIGUOUS``);
    * the attempt holds a single leading ``started`` row, one ``access_recorded`` row, and no ``report_started`` or
      terminal row; it is the binding's capturing run with no ``capture_reused`` row, or it holds one
      ``capture_reused`` row naming the binding and no capture rows of its own (``LEDGER_MISMATCH``);
    * the freeze holds at HEAD: the ``declared`` payload pin and this checkout's four hashes against the row's
      ``evidence`` (:func:`check_declaration`, on the committed ledger), and the same four against the values the
      attempt's ``started`` row recorded under the same labels (``CodeHashes.by_label``), so code changed between
      the capture and the report cannot evaluate under the capture's run id (``DECLARATION_MISMATCH``).
    """
    binding = capture_binding(committed)
    try:
        recorded_access_id(rows, run_id, before=REPORT_STARTED_EVENT)
    except StageBAccessError as exc:
        raise BookRefusal(LEDGER_MISMATCH, str(exc)) from exc
    events = [row.get("event") for row in rows if row.get("run_id") == run_id]
    reused = [row for row in rows if row.get("run_id") == run_id and row.get("event") == CAPTURE_REUSED_EVENT]
    if binding.capturing_run_id == run_id:
        if reused:
            raise BookRefusal(LEDGER_MISMATCH, f"run {run_id} captured the binding and also wrote {reused!r}")
    else:
        own = [event for event in events if event in _CAPTURE_EVENTS]
        if own or len(reused) != 1:
            raise BookRefusal(
                LEDGER_MISMATCH,
                f"run {run_id} is not the binding's capture ({binding.capturing_run_id}): it needs exactly one "
                f"{CAPTURE_REUSED_EVENT!r} row and no capture rows, found {len(reused)} and {own}",
            )
        named = (reused[0].get("capturing_run_id"), reused[0].get("manifest_sha256"))
        if named != (binding.capturing_run_id, binding.manifest_sha256):
            raise BookRefusal(LEDGER_MISMATCH, f"run {run_id} reused {named}, not the binding {binding}")
    try:
        trial = check_declaration(register, committed, current)
    except DeclarationError as exc:
        raise BookRefusal(DECLARATION_MISMATCH, str(exc)) from exc
    started = run_event(rows, run_id, "started")
    moved = sorted(label for label, value in current.by_label().items() if started.get(label) != value)
    if moved:
        raise BookRefusal(DECLARATION_MISMATCH, f"run {run_id}: this checkout's {moved} differ from its 'started' row")
    return trial, binding


def report_started_row(run_id: str, head: str, binding: Binding) -> dict[str, Any]:
    """Ledger step 5: the report's HEAD and the capture it evaluates; ``started`` keeps the attempt's own HEAD."""
    return {
        "run_id": run_id,
        "event": REPORT_STARTED_EVENT,
        "at": datetime.now(UTC).isoformat(),
        "git_sha": head,
        "capturing_run_id": binding.capturing_run_id,
        "manifest_sha256": binding.manifest_sha256,
    }


__all__ = [
    "CAPTURE_AMBIGUOUS",
    "CAPTURE_REUSED_EVENT",
    "COMMITTED_LEDGER_PATH",
    "DATA_FROZEN_EVENT",
    "DECLARATION_MISMATCH",
    "LEDGER_MISMATCH",
    "LEDGER_PATH",
    "REPORT_STARTED_EVENT",
    "STRATEGY_ID",
    "STRATEGY_VERSION",
    "Binding",
    "StageBAccessError",
    "access_purpose",
    "capture_binding",
    "check_report_gate",
    "end_run_failed",
    "may_bind_trial",
    "recorded_access_id",
    "report_started_row",
    "require_committed_access",
    "run_event",
]
