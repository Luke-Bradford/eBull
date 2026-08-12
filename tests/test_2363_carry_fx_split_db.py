"""#2363 — what ``sql/335`` actually did to the schema.

⚠ THE MIGRATION'S EFFECTS, NOT THE PYTHON RULE. The rule is covered by pure
tests in ``test_prereg_declaration_gate`` and ``test_strategy_result``; these
assert the three things a Python test cannot see and that a careless later
migration would silently undo: the view still EXPOSES the new column, the view
still CARRIES its cascaded check option, and the promotion index's predicate
still matches the gate's two cost clauses.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a
test source db-marks the WHOLE module at collection, so mixing these with pure
tests would drag the fast tier onto Postgres.
"""

from typing import Any

import psycopg
import pytest


@pytest.mark.parametrize(
    "table,column",
    [
        ("strategy_results_store", "fx_unmodelled"),
        ("strategy_preregistration_declarations", "declared_fx_unmodelled"),
    ],
)
def test_the_stamp_is_not_null_and_defaults_to_the_fail_closed_value(
    ebull_test_conn: psycopg.Connection[Any], table: str, column: str
) -> None:
    """NOT NULL, and defaulting to TRUE — the refused direction.

    ⚠ The default is asserted rather than merely tolerated. It is what makes the
    migration rolling-safe against a writer that predates the column
    (``strategy_backtest_run`` is a live job and holds a transaction for hours),
    and TRUE is the only value that is safe to inherit: it means "unmodelled",
    which refuses. A future migration dropping the default would reintroduce the
    NOT NULL violation this was chosen to avoid, so it fails here.
    """
    row = ebull_test_conn.execute(
        """
        SELECT is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %(table)s AND column_name = %(column)s
        """,
        {"table": table, "column": column},
    ).fetchone()
    assert row is not None, f"{table}.{column} does not exist — sql/335 did not apply"
    is_nullable, column_default = row
    assert is_nullable == "NO"
    assert column_default == "true"


def test_the_in_sample_view_exposes_the_new_stamp(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """``strategy_results`` is a VIEW, and ``SELECT *`` is expanded at creation.

    So a column added to the store is invisible through the view until the view
    is recreated. A reader that gates on the stamp through the view would then
    be gating on a column that is not there.
    """
    row = ebull_test_conn.execute(
        """
        SELECT count(*) FROM information_schema.columns
        WHERE table_name = 'strategy_results' AND column_name = 'fx_unmodelled'
        """
    ).fetchone()
    assert row is not None and row[0] == 1


def test_the_view_keeps_its_cascaded_check_option(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠⚠ ``CREATE OR REPLACE VIEW`` DROPS the check option silently.

    ``sql/264`` gave this view a cascaded check option so the in-sample insert
    statement is physically incapable of writing a hold-out row — the guard is
    the relation, not the caller. Recreating the view to expose a new column
    removes that guard unless it is restored, and nothing else in the suite
    would notice.
    """
    row = ebull_test_conn.execute(
        "SELECT check_option FROM information_schema.views WHERE table_name = 'strategy_results'"
    ).fetchone()
    assert row is not None
    assert row[0] == "CASCADED"


def test_the_promotion_index_predicate_matches_both_cost_clauses(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The candidate prefilter may not be WEAKER than the structural gate.

    ⚠ It is a coarse filter and deliberately omits most refusals — that is not
    what this asserts. It asserts only that the two clauses it DOES encode are
    both there: an index that still said `NOT carry_unmodelled` alone would
    offer rows that `structural_promotion_refusals` now refuses on FX.
    """
    row = ebull_test_conn.execute(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_strategy_results_promotable_basis'"
    ).fetchone()
    assert row is not None, "the promotion prefilter index is missing"
    indexdef = str(row[0])
    assert "NOT carry_unmodelled" in indexdef
    assert "NOT fx_unmodelled" in indexdef
