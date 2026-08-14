"""#2603 item 3 step 2 — the two core-arm predicates must stay DIFFERENT.

Pure-logic: these assert on the composed SQL text, no database.

⚠ The regression this guards is a TIDY-UP, not a bug. There are two predicates
that look near-identical and a reader will reasonably want to collapse them into
one. Collapsing in either direction is a defect, and neither direction fails any
existing test:

* authorised → present on an ACT path would let a ``refused`` intent or a
  non-paper mandate authorise the manager to trade.
* present → authorised on a REPORT path would HIDE a core position from the
  operator -- which is the precise failure the whole slice exists to remove. On
  a read path, failing closed means SHOWING the row, not hiding it.

The DB module (``test_2603_core_trade_arc_db``) proves what the act path admits
and refuses against real rows. This one pins the distinction itself, which is a
property of the source and not of any row.
"""

from __future__ import annotations

from app.services.strategy_core_arc_sql import (
    core_arm_authorised,
    core_arm_joins,
    core_arm_present,
)


def test_the_authorised_predicate_witnesses_every_link() -> None:
    """Each conjunct proves a DIFFERENT thing; any one missing fails open.

    The arc column alone proves only that the row is a core trade -- not that its
    verdict was actionable (sql/348 stores holds and refusals as evidence), and
    not that the governing mandate declared paper.
    """
    predicate = core_arm_authorised("t")
    assert "t.core_rebalance_intent_id IS NOT NULL" in predicate
    assert "core_intent.action IN ('buy_core','sell_core')" in predicate
    assert "core_mandate.core_mandate_event_id IS NOT NULL" in predicate


def test_the_report_predicate_requires_neither_verdict_nor_mandate() -> None:
    """⚠ The half that is easy to 'fix' into a bug.

    A report asks only "is this a core trade". If this ever starts requiring an
    actionable verdict, a core position that has been HELD -- the normal steady
    state of a core sleeve -- vanishes from the operator's positions list.
    """
    predicate = core_arm_present("trade")
    assert predicate == "trade.core_rebalance_intent_id IS NOT NULL"
    assert "action" not in predicate
    assert "core_mandate" not in predicate


def test_the_two_predicates_are_not_the_same_string() -> None:
    assert core_arm_authorised("t") != core_arm_present("t")


def test_the_alias_reaches_every_reference_in_the_joins() -> None:
    """A half-substituted alias would silently correlate the subquery to the
    wrong table rather than fail -- Postgres resolves an unqualified outer name.
    """
    joins = core_arm_joins("trade")
    assert "{t}" not in joins
    assert "trade.core_rebalance_intent_id" in joins
    # The mandate is bound through the intent, never straight off the trade.
    assert "core_mandate.core_mandate_event_id=core_intent.core_mandate_event_id" in joins
    assert "core_mandate.mode='paper'" in joins


def test_the_paper_filter_sits_in_the_join_not_the_caller() -> None:
    """``mode='paper'`` must live in the ON clause so a LEFT JOIN keeps it a
    filter on this arm only; the caller then witnesses it with an IS NOT NULL.
    If it migrated to a WHERE clause it would delete the signal arm's rows.
    """
    joins = core_arm_joins("t")
    paper_line_index = joins.index("core_mandate.mode='paper'")
    assert "WHERE" not in joins[:paper_line_index]
    assert "LEFT JOIN strategy_core_mandate_events core_mandate" in joins[:paper_line_index]
