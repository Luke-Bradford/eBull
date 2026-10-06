"""The #3609 step 2 declared run's hold-out identity and its stage-B access gate.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Registration" (PR #3666). Every stage-B read, the
extended SUB files first, happens only after the run's ``started`` ledger row and a committed ``access_recorded``
row. ``record_holdout_access`` does not enforce a declaration for this trial (it runs with
``require_declaration=False``), so the run checks its own ledger and the access log before reading.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import psycopg

STRATEGY_ID: Final = "3609-step2-book"
STRATEGY_VERSION: Final = "v1"

_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
LEDGER_PATH: Final = _REPO_ROOT / "var" / "research" / "3609_step2" / "ledger.jsonl"
COMMITTED_LEDGER_PATH: Final = _REPO_ROOT / "docs" / "research" / "3609-ledger.jsonl"

_TERMINAL_EVENTS: Final = frozenset({"completed", "failed"})

_COMMITTED_ACCESS: Final = """
    SELECT 1 FROM strategy_holdout_accesses
    WHERE access_id = %(access_id)s
      AND strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND result_version = %(run_id)s
      AND access_kind = 'evaluate'
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


def require_committed_access(conn: psycopg.Connection[Any], run_id: str, access_id: int) -> None:
    """Refuse unless the access log holds this run's ``evaluate`` row, read on a connection that did not write it,
    so only a committed row is visible."""
    row = conn.execute(
        _COMMITTED_ACCESS,
        {
            "access_id": access_id,
            "strategy_id": STRATEGY_ID,
            "strategy_version": STRATEGY_VERSION,
            "run_id": run_id,
        },
    ).fetchone()
    if row is None:
        raise StageBAccessError(
            f"run {run_id}: no committed evaluate access {access_id} for {STRATEGY_ID} {STRATEGY_VERSION}"
        )


__all__ = [
    "COMMITTED_LEDGER_PATH",
    "LEDGER_PATH",
    "STRATEGY_ID",
    "STRATEGY_VERSION",
    "StageBAccessError",
    "recorded_access_id",
    "require_committed_access",
]
