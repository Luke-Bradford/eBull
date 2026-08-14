"""#2603 item 3 step 1 — the pure half of the rebalance-intent writer.

Two things are covered here and nowhere else: ``_storable_or_none``'s asymmetry
(round on scale, NULL on magnitude/finiteness), and the claim in the module and
migration headers that this table AUTHORISES NOTHING.

⚠ The second is a test rather than a docstring on purpose. "Nothing reads it" is
the only thing making a durable ``buy_core`` record safe to ship before an
executor exists, and a promise in prose cannot fail when someone adds a reader.
Same shape as the existing check that no file under ``scripts/`` reaches a
mutating broker method.
"""

from __future__ import annotations

import ast
import re
from decimal import Decimal
from pathlib import Path
from typing import get_args

import pytest

from app.services.strategy_core_allocator import CoreRebalanceReasonCode
from app.services.strategy_core_rebalance_intent import (
    _INSERT_INTENT,
    _INTENT_COLUMNS,
    _storable_or_none,
)

_WRITER = "app/services/strategy_core_rebalance_intent.py"
_TABLE = "strategy_core_rebalance_intents"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # Exact at six places: unchanged.
        (Decimal("1234.567890"), Decimal("1234.567890")),
        (Decimal("0"), Decimal("0.000000")),
        # More scale than the column holds: ROUNDED, not refused. `_state_refusal`
        # bounds finiteness and magnitude but says nothing about decimal places,
        # so a nine-place broker valuation reaches a perfectly valid `buy_core` —
        # refusing it would make a SUCCESSFUL verdict unstorable.
        (Decimal("1234.567890123"), Decimal("1234.567890")),
        # ROUND_DOWN, matching the allocator's own direction — toward zero on
        # both signs, so neither is quietly enlarged.
        (Decimal("-1.9999999"), Decimal("-1.999999")),
        # Negative is storable and IS the evidence: `_state_refusal` refuses it,
        # so it can only reach a refusal row, where recording it is the point.
        (Decimal("-500"), Decimal("-500.000000")),
        # One quantum below the magnitude bound.
        (Decimal("999999999999.999999"), Decimal("999999999999.999999")),
    ],
)
def test_storable_values_round_rather_than_vanish(value: Decimal, expected: Decimal) -> None:
    assert _storable_or_none(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        # The refusals these produce (`sleeve_valuation_invalid`, and the
        # currency/instrument mismatches checked BEFORE it) are exactly the rows
        # whose evidence would otherwise be unwritable: NUMERIC(18,6) raises
        # `numeric field overflow` and the control cannot express the state.
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("1000000000000"),  # the bound itself is exclusive
        Decimal("1e30"),
        Decimal("-1e30"),
    ],
)
def test_unrepresentable_values_become_null_rather_than_raising(value: Decimal) -> None:
    assert _storable_or_none(value) is None


def test_finiteness_is_checked_before_magnitude() -> None:
    """Ordering, not style: ``abs(Decimal("NaN")) >= x`` raises InvalidOperation.

    Swapping the two guards in the writer turns this from None into an
    uncaught ``decimal.InvalidOperation`` at the first NaN valuation.
    """
    assert _storable_or_none(Decimal("NaN")) is None
    assert _storable_or_none(Decimal("sNaN")) is None


def test_the_insert_column_list_and_placeholder_block_cannot_disagree() -> None:
    """Both are generated from ``_INTENT_COLUMNS``, so this pins the generation.

    #2623 shipped a value into the wrong block by maintaining the column list,
    the ``%(name)s`` block and a reader tuple separately — psycopg binds by NAME,
    which makes the order feel irrelevant, but the order that matters is the
    placeholder block against the column list.
    """
    columns = re.search(rf"INSERT INTO {_TABLE} \((.*?)\) VALUES", _INSERT_INTENT, re.S)
    placeholders = re.search(r"VALUES \((.*?)\) RETURNING", _INSERT_INTENT, re.S)
    assert columns is not None and placeholders is not None
    assert [c.strip() for c in columns.group(1).split(",")] == list(_INTENT_COLUMNS)
    assert [p.strip() for p in placeholders.group(1).split(",")] == [f"%({column})s" for column in _INTENT_COLUMNS]


def _mentions_table_in_code(path: Path) -> bool:
    """True when the table name appears in a string the module EXECUTES.

    A plain substring scan also fires on a comment or a docstring that merely
    names the table — which the next slice's manager change will certainly do
    while still not reading it. ``ast`` drops ``#`` comments for free; docstrings
    survive as ``Constant`` nodes, so they are subtracted explicitly by identity.

    A SQL string and a docstring are the same node TYPE and differ only by
    position, which is why this is positional rather than heuristic.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    return any(
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and _TABLE in node.value
        and id(node) not in docstrings
        for node in ast.walk(tree)
    )


#: Every module allowed to name this table in EXECUTED SQL, and why.
#:
#: ⚠ Step 1 asserted this list was EMPTY apart from the writer -- that was the
#: enforceable half of "authorises nothing", and step 2 (sql/349) deliberately
#: ends it: the arc gives a row here a way to become a trade. The guard is kept
#: rather than deleted, converted from "nobody reads it" to "exactly these do",
#: because the hazard it was really tracking never went away -- an UNREVIEWED
#: reader is what turns a stored verdict into an action nobody agreed to.
_SANCTIONED_READERS = {
    # The writer.
    "app/services/strategy_core_rebalance_intent.py",
    # The shared load fragments. Every act-path consumer composes its predicate
    # from here rather than naming the table, so this stays a one-line list even
    # as consumers multiply -- and the requirements (actionable verdict, paper
    # mandate) cannot drift between them.
    "app/services/strategy_core_arc_sql.py",
}


def test_only_sanctioned_modules_read_the_intents_table() -> None:
    """A new reader must be added here deliberately, in the PR that adds it."""
    offenders = sorted(
        str(path)
        for path in Path("app").rglob("*.py")
        if str(path) not in _SANCTIONED_READERS and _mentions_table_in_code(path)
    )
    assert offenders == [], (
        f"{_TABLE} gained an unsanctioned reader: {offenders}. "
        "Add it to _SANCTIONED_READERS with its reason, or route it through "
        "strategy_core_arc_sql so the load predicate stays single-sourced."
    )


def test_only_the_trade_arc_references_the_intents_table() -> None:
    """An FK is what lets a row here become a row somewhere real.

    ⚠ Step 1 asserted NO table referenced it. ``sql/349`` adds exactly one, from
    ``strategy_trades``, which is the arc. Pinned to that single migration so a
    SECOND referencing table -- a sibling design this slice explicitly rejected,
    because it would need every arm-agnostic consumer dual-written -- cannot land
    without this failing.
    """
    referencing = sorted(
        path.name
        for path in Path("sql").glob("*.sql")
        if re.search(rf"REFERENCES\s+{_TABLE}\b", path.read_text(encoding="utf-8"))
    )
    assert referencing == ["349_core_trade_arc.sql"], (
        f"the set of tables referencing {_TABLE} changed: {referencing}. "
        "The arc is deliberately the only one — see docs/proposals/ta/2026-08-14-core-trade-arc.md."
    )


def test_the_migration_reason_codes_match_the_allocator_vocabulary() -> None:
    """The single source the ``reason_code`` CHECK cannot import.

    SQL has no way to reference ``CoreRebalanceReasonCode``, so the migration
    restates it — and a restatement drifts. This binds the two in BOTH
    directions, which is what makes it enforcement rather than documentation:

    * a code the allocator can return but the column refuses is an **unwritable
      verdict**, the exact failure class this table was shaped around;
    * a code the column admits but the allocator cannot produce is dead
      vocabulary that a later reader will treat as reachable.

    Not a tautology: the ``Literal`` is checked by pyright against every
    ``return "..."`` site in the allocator, and the CHECK list is parsed from the
    shipped migration file rather than re-typed here.
    """
    sql = Path("sql/348_core_rebalance_intents.sql").read_text(encoding="utf-8")
    block = re.search(r"reason_code\s+TEXT CHECK \(reason_code IN \((.*?)\)\)", sql, re.S)
    assert block is not None, "sql/348 no longer declares a reason_code IN (...) CHECK"
    in_migration = set(re.findall(r"'([a-z_]+)'", block.group(1)))
    in_allocator = set(get_args(CoreRebalanceReasonCode))

    assert in_allocator - in_migration == set(), (
        "the allocator can return codes sql/348 refuses, so those verdicts cannot "
        f"be stored: {sorted(in_allocator - in_migration)}"
    )
    assert in_migration - in_allocator == set(), (
        f"sql/348 admits codes the allocator cannot produce: {sorted(in_migration - in_allocator)}"
    )
