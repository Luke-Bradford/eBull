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

import re
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.strategy_core_rebalance_intent import (
    _INTENT_COLUMNS,
    _INSERT_INTENT,
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
    assert [p.strip() for p in placeholders.group(1).split(",")] == [
        f"%({column})s" for column in _INTENT_COLUMNS
    ]


def test_no_module_outside_the_writer_reads_the_intents_table() -> None:
    """The enforceable half of "authorises nothing".

    A durable ``buy_core`` row is only safe to ship ahead of an executor because
    nothing can act on it. When the executor lands it will read this table, and
    this test is what forces that change to arrive alongside the trade linkage
    and the position-manager change rather than on its own.
    """
    offenders = [
        str(path)
        for path in Path("app").rglob("*.py")
        if str(path) != _WRITER and _TABLE in path.read_text(encoding="utf-8")
    ]
    assert offenders == [], (
        f"{_TABLE} gained a reader outside {_WRITER}: {offenders}. "
        "It authorises nothing only while nothing reads it — see #2603 item 3."
    )


def test_no_table_references_the_intents_table() -> None:
    """The other half: an FK would let a row here become a row somewhere real.

    The next slice adds exactly that FK, from ``strategy_trades``, together with
    the manager change. Until then a reference is the whole hazard.
    """
    referencing = [
        path.name
        for path in Path("sql").glob("*.sql")
        if re.search(rf"REFERENCES\s+{_TABLE}\b", path.read_text(encoding="utf-8"))
    ]
    assert referencing == [], f"a table now references {_TABLE}: {referencing}"
