"""#3609 step 2 slice 5: the declared run, ledger steps 1 to 4, from a clean checkout at ``origin/main``.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"Registration" (the run's own code check,
ledger steps 1 to 4) and §"Slices" item 5 (capture lifecycle). In order:

1. **``started``** (:func:`start_run`): the declaration must match this checkout (``check_declaration``: the
   ``declared`` payload pin and the spec, construction and register-policy hashes and Python version against the
   row's ``evidence``). The row records the attempt's HEAD, those four values under their ``evidence`` labels (the
   report compares them at ``report_started``), the payload sha256 it checked, ``TRIAL_REGISTER_VERSION`` and the
   command. A fresh capture refuses while the committed ledger already binds one, and ``--reuse`` refuses without
   the one binding, so neither mode reaches a stage-B read it would then refuse. A refusal here writes no row.
2. **``access_recorded``**: the run's ``evaluate`` hold-out access, committed in its own transaction, then the row
   naming its ``access_id``.
3. **The capture**: the extended SUB artefact (``sub_published``), the stage-B artefact (``stage_b_published``), then
   the data-capture manifest (``data_frozen``); or, with ``--reuse``, one ``capture_reused`` row binding the trial's
   committed capture. Each step runs its own gate.

Any failure after ``started`` ends the run ``failed`` unless the failing step already ended it (:func:`run_steps`):
stage-B data may have been read, so a retry is a new run id with its own access.

The run then stops. Its ledger rows go to ``docs/research/3609-ledger.jsonl`` on ``main`` (a capture must be merged
before any report binds it), and the report runs as ``python -m scripts.report_3609_step2_run --run-id <run id>``.
Usage::

    PYTHONPATH=. uv run python -m scripts.run_3609_step2 --accessed-by "<operator or loop identity>" [--reuse]

This module executes inside the declared run, so it is a construction-hash root
(:data:`~app.services.factor_book_declaration.CONSTRUCTION_ROOTS`).
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import httpx
import psycopg

from app.config import settings
from app.services.factor_book_declaration import CodeHashes, check_declaration, payload_sha256
from app.services.factor_book_ledger import (
    CAPTURE_REUSED_EVENT,
    COMMITTED_LEDGER_PATH,
    DATA_FROZEN_EVENT,
    LEDGER_PATH,
    STRATEGY_ID,
    STRATEGY_VERSION,
    StageBAccessError,
    access_purpose,
    capture_binding,
    end_run_failed,
    may_bind_trial,
    require_committed_access,
)
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.result_ledger import HoldoutAccess, record_holdout_access
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION, TrialRegister
from scripts import publish_3609_step2_sub as sub_publisher
from scripts.build_3609_factor_panel import PUBLISH_ROOT, RESEARCH_ROOT, STAGE_B_EVENT, publish_stage_b
from scripts.capture_3609_step2 import freeze_capture, ledger_claim, reuse_capture
from scripts.report_3609_step2_run import report_head

STARTED_EVENT: Final = "started"
ACCESS_EVENT: Final = "access_recorded"
#: The claim :func:`start_run` holds (``capture_3609_step2.ledger_claim``).
START_CLAIM: Final = "start"
SUB_ROOT: Final = RESEARCH_ROOT / "factor_book_3609_step2_sub"
_TERMINAL_EVENTS: Final = frozenset({"completed", "failed"})


def started_row(run_id: str, *, head: str, hashes: CodeHashes, payload: str, command: Sequence[str]) -> dict[str, Any]:
    """Ledger step 1. The four ``evidence`` values sit under their labels (``CodeHashes.by_label``), the keys
    ``check_report_gate`` compares; the rest is provenance."""
    return {
        "run_id": run_id,
        "event": STARTED_EVENT,
        "at": datetime.now(UTC).isoformat(),
        "git_sha": head,
        **hashes.by_label(),
        "payload_sha256": payload,
        "register_version": TRIAL_REGISTER_VERSION,
        "command": list(command),
    }


def _end_if_open(run_id: str, step: str, exc: BaseException, *, ledger: Path, committed_ledger: Path) -> None:
    """End the run ``failed`` unless its ledger shows no row for it (nothing durable to end) or a terminal row. A
    failed append may still have made its row durable, so the ledger is re-read; if it cannot be, the row is
    written regardless."""
    try:
        events = {row.get("event") for row in read_ledger(committed_ledger, ledger) if row.get("run_id") == run_id}
    except Exception as read_error:
        exc.add_note(f"the ledger was not re-read ({read_error!r}); a 'failed' row is written regardless")
        events = {STARTED_EVENT}
    if events and not events & _TERMINAL_EVENTS:
        end_run_failed(ledger, run_id, step, exc)


def require_capture_mode(committed: Sequence[Mapping[str, Any]], *, reuse: bool) -> None:
    """Before ``started``: a fresh capture refuses while the committed ledger already holds a ``data_frozen`` row
    that could bind the trial (``freeze_capture`` would refuse it only after the stage-B build), and a reuse needs
    the one binding (``capture_binding``, ``CAPTURE_AMBIGUOUS`` otherwise)."""
    if reuse:
        capture_binding(committed)
    elif any(row.get("event") == DATA_FROZEN_EVENT and may_bind_trial(row) for row in committed):
        raise StageBAccessError(f"the committed ledger already holds a {DATA_FROZEN_EVENT!r} row; run with --reuse")


def start_run(
    run_id: str,
    *,
    head: str,
    command: Sequence[str],
    accessed_by: str,
    reuse: bool,
    record_access: Callable[[HoldoutAccess], int],
    register: TrialRegister = TRIAL_REGISTER,
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
    current: Callable[[], CodeHashes] = CodeHashes.current,
) -> int:
    """Ledger steps 1 and 2; returns the access id. A refusal writes no row and records no access.

    ``record_access`` must commit before it returns, so the ``access_recorded`` row never names an uncommitted
    access. A failure once ``started`` may be durable ends the run ``failed``."""
    hashes = current()
    with ledger_claim(ledger, START_CLAIM):
        rows = read_ledger(committed_ledger, ledger)
        trial = check_declaration(register, rows, hashes)
        if any(row.get("run_id") == run_id for row in rows):
            raise StageBAccessError(f"run {run_id} already has ledger rows; a run id is used once")
        require_capture_mode(read_ledger(committed_ledger), reuse=reuse)
        step = STARTED_EVENT
        try:
            append_ledger(
                ledger, started_row(run_id, head=head, hashes=hashes, payload=payload_sha256(trial), command=command)
            )
            step = ACCESS_EVENT
            access_id = record_access(
                HoldoutAccess(
                    strategy_id=STRATEGY_ID,
                    strategy_version=STRATEGY_VERSION,
                    access_kind="evaluate",
                    accessed_by=accessed_by,
                    purpose=access_purpose(run_id),
                    result_version=run_id,
                )
            )
            append_ledger(
                ledger,
                {"run_id": run_id, "event": ACCESS_EVENT, "at": datetime.now(UTC).isoformat(), "access_id": access_id},
            )
        except BaseException as exc:
            _end_if_open(run_id, step, exc, ledger=ledger, committed_ledger=committed_ledger)
            raise
    return access_id


def run_steps(
    run_id: str,
    steps: Sequence[tuple[str, Callable[[str], Any]]],
    *,
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
) -> Any:
    """Ledger steps 3 and 4 in order; returns the last step's value.

    A step's own gate refuses without a row, and a step that fails past its gate ends the run itself. Either way the
    access is recorded, so a step that raised and left the run open is ended ``failed`` here under its name."""
    result: Any = None
    for name, step in steps:
        try:
            result = step(run_id)
        except BaseException as exc:
            _end_if_open(run_id, name, exc, ledger=ledger, committed_ledger=committed_ledger)
            raise
    return result


def _record_access(access: HoldoutAccess) -> int:
    with psycopg.connect(settings.database_url) as conn:
        access_id = record_holdout_access(conn, access)
        conn.commit()
    return access_id


def _confirm(run_id: str, access_id: int) -> None:
    # A fresh connection: it sees the access row only if the run committed it.
    with psycopg.connect(settings.database_url) as conn:
        require_committed_access(conn, run_id, access_id)


def _publish_sub(run_id: str) -> str:
    with httpx.Client(timeout=600, headers={"User-Agent": settings.sec_user_agent}) as client:
        return sub_publisher.publish(
            SUB_ROOT / run_id, run_id, ledger=LEDGER_PATH, client=client, confirm_access=_confirm
        )


def capture_steps(reuse: bool) -> list[tuple[str, Callable[[str], Any]]]:
    """Ledger steps 3 and 4 as the run's own gates name them; ``reuse`` binds the committed capture instead."""
    if reuse:
        return [(CAPTURE_REUSED_EVENT, lambda run_id: reuse_capture(run_id, confirm_access=_confirm))]
    return [
        (sub_publisher.EVENT, _publish_sub),
        (STAGE_B_EVENT, lambda run_id: publish_stage_b(PUBLISH_ROOT, run_id, confirm_access=_confirm)[1]),
        (DATA_FROZEN_EVENT, lambda run_id: freeze_capture(run_id, confirm_access=_confirm)),
    ]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--accessed-by", required=True, help="the operator or loop identity running the attempt")
    parser.add_argument("--reuse", action="store_true", help="bind the trial's committed capture instead")
    args = parser.parse_args(argv)
    head = report_head()
    run_id = uuid.uuid4().hex  # a fresh id for every attempt, so a retry is a new run
    command = [sys.executable, "-m", "scripts.run_3609_step2", *(sys.argv[1:] if argv is None else argv)]
    access_id = start_run(
        run_id, head=head, command=command, accessed_by=args.accessed_by, reuse=args.reuse, record_access=_record_access
    )
    digest = run_steps(run_id, capture_steps(args.reuse))
    print(
        json.dumps(
            {
                "run_id": run_id,
                "access_id": access_id,
                "event": CAPTURE_REUSED_EVENT if args.reuse else DATA_FROZEN_EVENT,
                "manifest_sha256": digest,
            }
        )
    )
    return 0


__all__ = [
    "ACCESS_EVENT",
    "STARTED_EVENT",
    "START_CLAIM",
    "SUB_ROOT",
    "capture_steps",
    "main",
    "require_capture_mode",
    "run_steps",
    "start_run",
    "started_row",
]


if __name__ == "__main__":
    raise SystemExit(main())
