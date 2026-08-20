"""Pure tests for the per-test cleanup delete ordering (#1568).

``_topological_delete_order`` decides the sequence in which the DB fixture
empties tables. ``DELETE`` has no ``CASCADE``, so a wrong order raises
``ForeignKeyViolation`` and every DB test fails at teardown. IO-free — no
Postgres needed, so this stays in the fast tier.

``referencing`` maps a table to the tables holding an FK pointing AT it; those
must be emptied first.
"""

from __future__ import annotations

from tests.fixtures.ebull_test_db import _topological_delete_order


def test_children_come_before_parents() -> None:
    order = _topological_delete_order(
        {"instruments", "price_daily"},
        {"instruments": {"price_daily"}},
    )
    assert order.index("price_daily") < order.index("instruments")


def test_chain_is_ordered_leaf_first() -> None:
    order = _topological_delete_order({"a", "b", "c"}, {"a": {"b"}, "b": {"c"}})
    assert order == ("c", "b", "a")


def test_diamond_covers_every_table_exactly_once() -> None:
    tables = {"a", "b", "c", "d"}
    order = _topological_delete_order(tables, {"a": {"b", "c"}, "b": {"d"}, "c": {"d"}})
    assert set(order) == tables
    assert len(order) == len(tables)
    assert order.index("d") < order.index("b") < order.index("a")
    assert order.index("d") < order.index("c") < order.index("a")


def test_self_reference_does_not_block_a_table() -> None:
    # A row referencing another row in its own table (a parent-CIK link, say)
    # is removed by the same DELETE, so a self-edge must not stall the table.
    order = _topological_delete_order({"cik_map"}, {"cik_map": {"cik_map"}})
    assert order == ("cik_map",)


def test_referencing_tables_outside_the_wipe_set_are_ignored() -> None:
    # Tables outside the wipe set are never emptied, so waiting on them would
    # deadlock the ordering of the tables that are.
    order = _topological_delete_order({"instruments"}, {"instruments": {"other"}})
    assert order == ("instruments",)


def test_fk_cycle_terminates_instead_of_spinning() -> None:
    # No such cycle exists in the schema today, but a future migration could
    # add one. The caller's TRUNCATE fallback covers the unordered result if a
    # DELETE among them then violates a constraint.
    order = _topological_delete_order({"a", "b"}, {"a": {"b"}, "b": {"a"}})
    assert set(order) == {"a", "b"}
