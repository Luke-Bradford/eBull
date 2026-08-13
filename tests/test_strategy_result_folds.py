"""Phase 5e-5c — the per-fold walk-forward rows, against a real database.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.3 (purge and
embargo), §8 (stage 5e-5c), acceptance C5. DDL: ``sql/269``. Writer:
``app/services/result_ledger.py``. Shape and its invariants:
``app/services/walk_forward.WalkForwardFolds`` (pure tier —
``tests/test_walk_forward.py``).

⚠⚠ WHAT IS DB-TIER HERE AND WHY NOTHING ELSE IS. The shape checks — four folds,
contiguous, one population — are properties of a frozen dataclass and are tested
without Postgres. What needs a real database is the part that is a property of a
RELATION and that a mocked cursor would pass against:

- the FK and its ``ON DELETE CASCADE`` — an orphan split cannot survive;
- the primary key — one result cannot carry two splits;
- ``sql/269``'s trigger — a fold row cannot hang off a ``hold_out`` result, by
  anyone, on INSERT or on UPDATE;
- the ``NUMERIC``-free but still lossy-able round trip: ``BIGINT`` counts and
  ``DATE`` bounds come back as what went in, in the column order the statement
  and the unpacking share.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.result_ledger import (
    read_walk_forward_folds,
    store_holdout_result,
    store_in_sample_result,
    store_walk_forward_folds,
)
from app.services.walk_forward import WalkForwardFolds
from tests.test_result_ledger import build_result
from tests.test_walk_forward import build_fold_record, build_split

_ACTOR = "tests/test_strategy_result_folds.py"
_PURPOSE = "stage 5e-5c acceptance"

#: ⚠ A fixed statement, never interpolated — psycopg types ``query`` as
#: ``LiteralString`` and the chokepoint lint refuses the f-string form. This is
#: the only way to write a row ``result_ledger`` would refuse, which is what the
#: table-level CHECKs have to be tested against.
_RAW_FOLD_INSERT = """
    INSERT INTO strategy_result_folds (
        result_id, fold_index, walk_forward_model_id, fold_count, first_index, last_index,
        first_date, last_date, bar_count, embargo_bars, test_count, train_count, purged_count, embargoed_count
    ) VALUES (
        %(result_id)s, %(fold_index)s, %(model_id)s, 4, 0, 9,
        '2000-01-01', '2000-01-10', 1000, 7, 5, 20, 3, 2
    )
"""


def _in_sample_result_id(conn: psycopg.Connection[tuple], **overrides: object) -> int:
    return store_in_sample_result(conn, build_result(namespace="in_sample", **overrides))


def test_a_split_round_trips_through_the_table(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ Equality on the WHOLE object, not field by field.

    A per-field sweep is written once and then never extended, and the field
    most likely to be missing from it is the newest one. ``WalkForwardFolds`` is
    frozen, so ``==`` compares every fold, every census bucket and both date
    bounds — and it re-runs the type's own construction checks on the way back,
    which is what makes this a check rather than a copy.
    """
    split = build_split()
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-RT")
        assert store_walk_forward_folds(ebull_test_conn, result_id, split) == 4
        assert read_walk_forward_folds(ebull_test_conn, result_id) == split


def test_a_result_with_no_split_reads_as_none(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ None, not an empty split — the type cannot express one."""
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-NONE")
        assert read_walk_forward_folds(ebull_test_conn, result_id) is None


def test_folds_are_refused_on_a_hold_out_result(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠⚠ The trigger. Folds are cut INSIDE the in-sample side (§5.3).

    A fold row on a hold-out result claims a cross-validation of the withheld
    data that nobody ran, which is the class of claim criterion 5 exists to stop.
    """
    with ebull_test_conn.transaction():
        result_id = store_holdout_result(
            ebull_test_conn,
            build_result(strategy_id="S-FOLD-HO", namespace="hold_out"),
            accessed_by=_ACTOR,
            purpose=_PURPOSE,
        )
    with pytest.raises(psycopg.errors.IntegrityError, match="IN-SAMPLE"), ebull_test_conn.transaction():
        store_walk_forward_folds(ebull_test_conn, result_id, build_split())


def test_a_fold_cannot_be_moved_onto_a_hold_out_result(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ INSERT OR UPDATE. Insert against the in-sample row, then re-point it."""
    with ebull_test_conn.transaction():
        in_sample_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-MOVE")
        hold_out_id = store_holdout_result(
            ebull_test_conn,
            build_result(strategy_id="S-FOLD-MOVE", namespace="hold_out"),
            accessed_by=_ACTOR,
            purpose=_PURPOSE,
        )
        store_walk_forward_folds(ebull_test_conn, in_sample_id, build_split())
    with pytest.raises(psycopg.errors.IntegrityError, match="IN-SAMPLE"), ebull_test_conn.transaction():
        ebull_test_conn.execute(
            "UPDATE strategy_result_folds SET result_id = %(to)s WHERE result_id = %(from)s",
            {"to": hold_out_id, "from": in_sample_id},
        )


def test_deleting_the_result_deletes_its_split(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ An orphan split is a stored cross-validation of nothing."""
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-CASCADE")
        store_walk_forward_folds(ebull_test_conn, result_id, build_split())
        ebull_test_conn.execute("DELETE FROM strategy_results_store WHERE result_id = %(id)s", {"id": result_id})
        remaining = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_result_folds WHERE result_id = %(id)s", {"id": result_id}
        ).fetchone()
    assert remaining is not None
    assert remaining[0] == 0


def test_one_result_cannot_carry_two_splits(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """The primary key. A second split would silently double every count."""
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-TWICE")
        store_walk_forward_folds(ebull_test_conn, result_id, build_split())
    with pytest.raises(psycopg.errors.UniqueViolation), ebull_test_conn.transaction():
        store_walk_forward_folds(ebull_test_conn, result_id, build_split())


def test_the_writer_refuses_a_construction_it_did_not_implement(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ A write happens under TODAY's construction; a read returns the stored one."""
    with pytest.raises(ValueError, match="this module implements"), ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-MODEL")
        store_walk_forward_folds(ebull_test_conn, result_id, build_split(model_id="c5-purged-walk-forward-v2"))


def test_a_split_whose_rows_declare_two_constructions_is_refused_on_read(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ Reachable only by writing around the writer, which is why it is checked.

    One split is one construction. A mixed set is two runs whose rows landed on
    one result, and averaging them is not a thing a reader could notice.
    """
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-MIXED")
        store_walk_forward_folds(ebull_test_conn, result_id, build_split())
        ebull_test_conn.execute(
            """
            UPDATE strategy_result_folds
            SET walk_forward_model_id = 'c5-purged-walk-forward-v2'
            WHERE result_id = %(id)s AND fold_index = 2
            """,
            {"id": result_id},
        )
        with pytest.raises(ValueError, match="carries folds from"):
            read_walk_forward_folds(ebull_test_conn, result_id)


def test_a_blank_construction_is_refused_by_the_table(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ The #2286 shape: NOT NULL admits a PRESENT-but-empty value.

    A blank model id is a split whose construction is undeclared while looking
    declared, and no Python guard is reached by a writer that bypasses the
    module.
    """
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-BLANK")
    with pytest.raises(psycopg.errors.CheckViolation), ebull_test_conn.transaction():
        ebull_test_conn.execute(_RAW_FOLD_INSERT, {"result_id": result_id, "fold_index": 0, "model_id": ""})


def test_a_stored_split_still_satisfies_its_own_shape_checks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ The read reconstructs through ``WalkForwardFolds``, checks and all.

    A row set that reached the table by some other path — three folds, a gap, a
    fold counting a different population — is refused on the way OUT rather than
    returned as a split. The table cannot express the population check (it is a
    property of the set, not of a row), so the type is where it lives.
    """
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-PARTIAL")
        store_walk_forward_folds(ebull_test_conn, result_id, build_split())
        ebull_test_conn.execute(
            "DELETE FROM strategy_result_folds WHERE result_id = %(id)s AND fold_index = 3", {"id": result_id}
        )
        with pytest.raises(ValueError, match="carries 4 folds"):
            read_walk_forward_folds(ebull_test_conn, result_id)


def test_the_last_fold_measures_no_embargo_and_stores_it(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """⚠ 0 is a measurement, not a skipped step — nothing follows the last fold."""
    folds = (
        build_fold_record(0, 0, 9),
        build_fold_record(1, 10, 19),
        build_fold_record(2, 20, 29),
        build_fold_record(3, 30, 39, embargo_bars=0),
    )
    split = WalkForwardFolds(model_id=build_split().model_id, folds=folds)
    with ebull_test_conn.transaction():
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-ZERO")
        store_walk_forward_folds(ebull_test_conn, result_id, split)
        read_back = read_walk_forward_folds(ebull_test_conn, result_id)
    assert read_back is not None
    assert read_back.folds[3].embargo_bars == 0


def test_the_split_writer_is_atomic_on_an_autocommit_connection(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ A partial split must be unreachable however the caller CONNECTED.

    Every other test here runs inside an explicit transaction, where a refused
    row aborts the batch for free — so none of them exercise an autocommit
    caller, and this repo opens several (``app/main.py``'s lifespan guards, the
    runbooks). Fold 2 is made to fail by planting a row on its primary key
    first; folds 0 and 1 must not survive it.

    ⚠ THIS PINS THE PROPERTY, NOT OUR GUARD, and the distinction is measured
    rather than assumed: on psycopg 3.3.3 ``executemany`` already runs its batch
    in a transaction, so this test passes with or without the writer's own
    ``conn.transaction()`` (established by the revert probe reporting NOT
    CAUGHT — see ``scripts/probe_2240_result_ledger.py``'s header). It is kept
    because the property is what matters to a reader of the table, and because a
    driver that stopped providing it would fail here.
    """
    split = build_split()
    ebull_test_conn.rollback()
    ebull_test_conn.autocommit = True
    try:
        result_id = _in_sample_result_id(ebull_test_conn, strategy_id="S-FOLD-ATOMIC")
        ebull_test_conn.execute(
            _RAW_FOLD_INSERT,
            {"result_id": result_id, "fold_index": 2, "model_id": "c5-purged-walk-forward-v1"},
        )
        with pytest.raises(psycopg.errors.UniqueViolation):
            store_walk_forward_folds(ebull_test_conn, result_id, split)
        landed = ebull_test_conn.execute(
            "SELECT count(*) FROM strategy_result_folds WHERE result_id = %(id)s", {"id": result_id}
        ).fetchone()
        assert landed == (1,), "folds written before the refused one survived"
    finally:
        # ⚠ Autocommit means nothing unwinds itself; the FK cascade takes the
        # planted fold row with the carrier result.
        ebull_test_conn.execute("DELETE FROM strategy_results_store WHERE strategy_id = 'S-FOLD-ATOMIC'")
        ebull_test_conn.autocommit = False
