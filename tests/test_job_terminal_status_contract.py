"""#2218 — the ``job_runs`` status vocabulary, derived rather than trusted.

WHY THIS FILE EXISTS
--------------------
The vocabulary was spelled by hand in **eight** places: the CHECK constraint
(rewritten across sql/014, sql/020, sql/137, sql/254), five SQL ``IN (...)``
filters in four modules, and two Python ``Literal``s. pyright cannot see inside
a SQL string, so nothing connected them, and the drift was already real before
this ticket:

* ``ops_monitor.JobStatus`` was missing ``cancelled``, which sql/137 added.
* The first draft of sql/254 was copied from sql/020 — which predates sql/137 —
  and silently DROPPED ``cancelled`` from the constraint. It applied clean on
  this dev DB because there happen to be no cancelled rows, and would have
  rejected the next one. Codex caught it at checkpoint 2; no gate did.

So the guard reads the migration as text and asserts the Python constants
agree. It is deliberately a SOURCE-derived test, not a DB test: it fails in the
fast tier, on a machine with no Postgres, at the moment the sixth member is
added.
"""

from __future__ import annotations

import re
from pathlib import Path

from app.services.ops_monitor import JOB_TERMINAL_STATUSES, TERMINAL_STATUS_SQL

_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "254_job_runs_degraded_progress.sql"

#: ``running`` is a status but NOT a terminal one — a row still in flight has
#: not finished, and every "latest terminal run" query must exclude it or the
#: admin verdict would read an in-progress run as the last outcome.
_NON_TERMINAL = frozenset({"running"})


def _constraint_statuses() -> set[str]:
    """Statuses the shipped CHECK constraint admits, parsed from the migration."""
    sql = _MIGRATION.read_text()
    match = re.search(
        r"ADD CONSTRAINT job_runs_status_check\s*\n?\s*CHECK \(status IN \(([^)]*)\)\)",
        sql,
    )
    assert match is not None, "could not find the job_runs_status_check definition in sql/254"
    return {value.strip().strip("'") for value in match.group(1).split(",")}


def test_terminal_statuses_are_the_constraint_minus_running() -> None:
    """The load-bearing assertion.

    Adding a status to the migration without adding it here fails; adding it
    here without the migration fails. Either direction is the drift that let a
    degraded row be written and never read.
    """
    assert set(JOB_TERMINAL_STATUSES) == _constraint_statuses() - _NON_TERMINAL


def test_the_sql_fragment_and_the_tuple_cannot_disagree() -> None:
    """``JOB_TERMINAL_STATUSES`` is parsed from ``TERMINAL_STATUS_SQL``, so this
    pins the derivation rather than a second hand-written copy."""
    assert TERMINAL_STATUS_SQL.startswith("(") and TERMINAL_STATUS_SQL.endswith(")")
    assert set(JOB_TERMINAL_STATUSES) == {
        part.strip().strip("'") for part in TERMINAL_STATUS_SQL.strip("()").split(",")
    }


def test_cancelled_survived_the_migration_rewrite() -> None:
    """Named explicitly because losing it is the specific mistake that was made.

    sql/137 added ``cancelled``; a migration copied from sql/020 drops it, and
    the loss is invisible on any database that has no cancelled rows to
    validate against.
    """
    assert "cancelled" in _constraint_statuses()


def test_degraded_is_terminal() -> None:
    # If it were excluded from the terminal set, the row would be written and
    # every "latest terminal run" query would skip past it to an older,
    # greener run — the exact way to ship this ticket and change nothing.
    assert "degraded" in JOB_TERMINAL_STATUSES


def test_no_module_spells_the_terminal_list_by_hand() -> None:
    """Grep guard for the pattern that caused the drift.

    ⚠ Matches the SHAPE, not an exact string, because the five sites differed
    only in whitespace and an equality check would have read each as "not the
    same literal".

    Two precision fixes, both found by running it:

    * ``'failure'`` is required. ``bootstrap_adapter`` filters
      ``IN ('success', 'error', 'skipped', 'blocked', 'cancelled')`` — a
      DIFFERENT vocabulary on a different table, and flagging it would make
      this guard a nuisance that gets deleted.
    * Python comment lines are stripped first. Otherwise the guard flags the
      comment in ``ops_monitor`` that quotes the anti-pattern in order to warn
      about it.
    """
    root = Path(__file__).resolve().parents[1] / "app"
    pattern = re.compile(r"IN\s*\(\s*'success'[^)]*'failure'[^)]*'skipped'[^)]*\)", re.IGNORECASE)

    def _code(path: Path) -> str:
        return "\n".join(line for line in path.read_text().splitlines() if not line.lstrip().startswith("#"))

    offenders = [str(path.relative_to(root)) for path in root.rglob("*.py") if pattern.search(_code(path))]
    assert offenders == [], (
        f"hand-spelled terminal-status list in {offenders} — "
        "import TERMINAL_STATUS_SQL from app.services.ops_monitor instead"
    )
