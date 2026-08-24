"""#2603 item 3 step 3a — the gate's constants must stay bound to their SOURCES.

Pure-logic: no database.  Everything here asserts that a constant in the module
still matches the migration or the encoding it was copied from, because each of
these is a value that reads as arbitrary once separated from where it came from —
and a reader who cannot see the source will eventually "tidy" it.

⚠ This is the same device as
``test_the_migration_reason_codes_match_the_allocator_vocabulary``: a constant and
its source in two files with nothing binding them is two constants.
"""

from __future__ import annotations

import re
from pathlib import Path

from app.services.strategy_control_plane import PAPER_ALLOCATOR_ADVISORY_LOCK
from app.services.strategy_core_mandate import CORE_MANDATE_ADVISORY_LOCK
from app.services.strategy_core_submission_gate import (
    _ADMISSION_SQL,
    _RECONCILIATION_TERMINAL_STATES,
    _TERMINAL_TRADE_STATUSES,
    CORE_SUBMISSION_ADVISORY_LOCK,
    CoreSubmissionRefusal,
)

_SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def _migration(prefix: str) -> str:
    matches = sorted(_SQL_DIR.glob(f"{prefix}_*.sql"))
    assert len(matches) == 1, f"expected exactly one sql/{prefix}_* migration, found {matches}"
    return matches[0].read_text()


def test_the_reconciliation_terminal_set_matches_the_backlog_index_it_was_copied_from() -> None:
    """``sql/285`` defines "the broker effect is known", and this must be it.

    The gate's whole in-flight rule is "block unless the reconciliation state is
    terminal".  If the migration ever adds an eighth state, or promotes one to
    terminal, a stale copy here would either block forever or admit a duplicate
    order — and neither would fail any other test.
    """
    text = _migration("285")
    predicate = re.search(r"WHERE state NOT IN \(([^)]*)\)", text)
    assert predicate is not None, "sql/285's backlog index predicate has moved; re-source this constant"
    sourced = tuple(sorted(value.strip().strip("'") for value in predicate.group(1).split(",")))
    assert sourced == tuple(sorted(_RECONCILIATION_TERMINAL_STATES))


def test_every_terminal_trade_status_is_one_the_schema_admits() -> None:
    """A typo here would silently stop blocking nothing, which looks like working.

    ``t.status <> ALL (...)`` against a value the CHECK cannot produce is a
    no-op — the in-flight arm would then treat closed and failed trades as
    blockers and deny every later rebalance.
    """
    text = _migration("281")
    check = re.search(r"CHECK \(status IN \(([^)]*)\)\)", text, re.DOTALL)
    assert check is not None, "sql/281's strategy_trades status CHECK has moved; re-source this constant"
    admitted = {value.strip().strip("'") for value in check.group(1).split(",")}
    assert set(_TERMINAL_TRADE_STATUSES) <= admitted


def test_the_lock_assertion_pins_the_two_int4_advisory_encoding() -> None:
    """``objsubid = 2`` is the difference between a control and a coincidence.

    Postgres records a two-``int4`` advisory key as ``classid``/``objid`` with
    ``objsubid = 2``, and a one-``int8`` key as the halves of the bigint with
    ``objsubid = 1``.  Drop the predicate and an unrelated
    ``pg_advisory_lock(bigint)`` whose halves collide satisfies the assertion —
    i.e. the gate would confirm a critical section that is open somewhere else.
    """
    source = Path(__file__).resolve().parents[1] / "app/services/strategy_core_submission_gate.py"
    text = source.read_text()
    assert "objsubid=2" in text.replace(" ", "")
    assert "pid=pg_backend_pid()" in text.replace(" ", "")
    assert "granted" in text


def test_all_advisory_keys_are_positive_and_distinct_because_the_assertion_compares_unsigned() -> None:
    """``pg_locks.classid``/``objid`` are OID-width; the lock functions take signed int4.

    A negative key component would be stored as its uint32 image and would not
    compare equal to the signed literal, so ``core_lock_held`` would return False for
    a lock that IS held — and the gate would raise on every call.  Cheap to pin,
    invisible if it ever changes.
    """
    keys = (PAPER_ALLOCATOR_ADVISORY_LOCK, CORE_MANDATE_ADVISORY_LOCK, CORE_SUBMISSION_ADVISORY_LOCK)
    for key in keys:
        assert all(component > 0 for component in key), key
    assert len(set(keys)) == len(keys)


def test_the_admission_query_is_one_statement() -> None:
    """One snapshot is the correctness property, not a style choice.

    Under READ COMMITTED a second statement gets a second snapshot, so "this is
    the newest intent" and "no trade cites it yet" could each be true of a
    different instant and false together.
    """
    assert _ADMISSION_SQL.count(";") == 0
    assert _ADMISSION_SQL.strip().upper().startswith("SELECT")


def test_the_refusal_vocabulary_is_closed_and_has_no_duplicates() -> None:
    """A `Literal` is the enforcement: pyright checks every return site against it."""
    codes = CoreSubmissionRefusal.__args__  # type: ignore[attr-defined]
    assert len(set(codes)) == len(codes)
    # The four inherited scope items plus the two this slice's falsification added.
    assert {"core_intent_already_submitted", "core_trade_in_flight"} <= set(codes)
    assert {"core_intent_superseded", "core_mandate_revision_stale"} <= set(codes)
    assert {"core_eligibility_unproved", "core_partial_close_unproved"} <= set(codes)
