"""#2603 item 1 — what ``sql/336`` enforces, and the writer's revision behaviour.

⚠ THE MIGRATION'S BACKSTOP, NOT THE PYTHON RULE. The arithmetic is covered by
pure tests in ``test_2603_core_mandate``; these INSERT **raw SQL that bypasses
the validator**, because a CHECK that only ever sees pre-validated values is not
demonstrably a backstop at all. A later migration relaxing one of these would
otherwise be invisible.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection, so mixing these with pure tests
would drag the fast tier onto Postgres.
"""

from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.strategy_core_mandate import (
    CoreMandateError,
    configure_core_mandate,
    load_core_mandate,
)

_INSERT = """
INSERT INTO strategy_core_mandate_events (
    revision,enabled,base_currency,core_instrument_id,core_target_pct,
    liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
    policy_version,changed_by,reason
) VALUES (
    1,%(enabled)s,%(base_currency)s,%(core_instrument_id)s,%(core_target_pct)s,
    %(liquidity_reserve_pct)s,%(rebalance_band_pct)s,%(min_rebalance_amount)s,
    'core-mandate-v1','test','test'
)
"""

_VALID_ROW: dict[str, Any] = {
    "enabled": False,
    "base_currency": "USD",
    "core_instrument_id": None,
    "core_target_pct": Decimal("60"),
    "liquidity_reserve_pct": Decimal("20"),
    "rebalance_band_pct": Decimal("5"),
    "min_rebalance_amount": Decimal("25"),
}


def _seed_instrument(conn: psycopg.Connection[Any]) -> int:
    """A minimal FK target. The fixture's per-test wipe removes it.

    ``is_tradable`` is listed explicitly per #1233 §6.2 — the chokepoint lint
    refuses an instruments INSERT that leaves it to the column default.
    """
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (920603,'CORE.TEST','Core Mandate Test',TRUE) ON CONFLICT DO NOTHING"
    )
    return 920603


def test_the_reference_row_inserts(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The control. Without it a CHECK test could pass for the wrong reason."""
    ebull_test_conn.execute(_INSERT, _VALID_ROW)
    assert load_core_mandate(ebull_test_conn) is not None


@pytest.mark.parametrize(
    "overrides,constraint",
    [
        # core 60 + band 25 leaves 15 cash against a 20 reserve.
        (
            {"rebalance_band_pct": Decimal("25")},
            "strategy_core_mandate_band_respects_reserve",
        ),
        # A band wider than the target puts the lower trigger below zero.
        (
            {
                "core_target_pct": Decimal("4"),
                "rebalance_band_pct": Decimal("5"),
                "liquidity_reserve_pct": Decimal("0"),
            },
            "strategy_core_mandate_band_within_range",
        ),
        # Enabled with no instrument to hold.
        ({"enabled": True}, "strategy_core_mandate_enabled_has_instrument"),
    ],
)
def test_the_named_checks_reject_raw_inserts(
    ebull_test_conn: psycopg.Connection[Any], overrides: dict[str, Any], constraint: str
) -> None:
    with pytest.raises(psycopg.errors.CheckViolation) as caught:
        ebull_test_conn.execute(_INSERT, {**_VALID_ROW, **overrides})
    assert caught.value.diag.constraint_name == constraint
    ebull_test_conn.rollback()


def test_non_usd_is_refused_by_the_column_check_too(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """#2603 item 4's deferral survives a writer that skips the service."""
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(_INSERT, {**_VALID_ROW, "base_currency": "GBP"})
    ebull_test_conn.rollback()


@pytest.mark.parametrize(
    "column",
    [
        "core_target_pct",
        "liquidity_reserve_pct",
        "rebalance_band_pct",
        "min_rebalance_amount",
        "base_currency",
        "enabled",
        "revision",
        "policy_version",
    ],
)
def test_the_arithmetic_columns_are_not_null(ebull_test_conn: psycopg.Connection[Any], column: str) -> None:
    """A CHECK is satisfied by NULL, so nullability is what makes it binding.

    Every arithmetic invariant above evaluates to UNKNOWN — and therefore passes —
    if any operand is NULL. NOT NULL is the reason the CHECKs cannot be bypassed
    by omission.
    """
    row = ebull_test_conn.execute(
        """
        SELECT is_nullable FROM information_schema.columns
        WHERE table_name = 'strategy_core_mandate_events' AND column_name = %(column)s
        """,
        {"column": column},
    ).fetchone()
    assert row is not None, f"{column} does not exist — sql/336 did not apply"
    assert row[0] == "NO"


def test_the_instrument_fk_restricts_deletes(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """An authority row must not lose its subject to an upstream delete.

    Asserted from the catalog rather than by deleting an instrument, which would
    be a destructive probe against a shared FK target.
    """
    row = ebull_test_conn.execute(
        """
        SELECT rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.referential_constraints rc
          ON rc.constraint_name = tc.constraint_name
        WHERE tc.table_name = 'strategy_core_mandate_events'
          AND tc.constraint_type = 'FOREIGN KEY'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "RESTRICT"


def test_revision_is_unique(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The backstop behind the advisory lock, not the mechanism."""
    ebull_test_conn.execute(_INSERT, _VALID_ROW)
    with pytest.raises(psycopg.errors.UniqueViolation):
        ebull_test_conn.execute(_INSERT, {**_VALID_ROW, "core_target_pct": Decimal("50")})
    ebull_test_conn.rollback()


def test_the_writer_appends_revisions_and_refuses_a_no_op(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """One integration pass over the writer: empty → 1 → 2, then a no-op refused.

    ``load_core_mandate`` returning None on an empty table is asserted first: no
    mandate configured is a state, not an implied default allocation.
    """
    assert load_core_mandate(ebull_test_conn) is None
    instrument_id = _seed_instrument(ebull_test_conn)

    first = configure_core_mandate(
        ebull_test_conn,
        enabled=True,
        core_instrument_id=instrument_id,
        core_target_pct=Decimal("60"),
        liquidity_reserve_pct=Decimal("20"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("25"),
        changed_by="test",
        reason="initial core/cash mandate",
    )
    assert first.revision == 1
    assert first.cash_target_pct == Decimal("40")

    second = configure_core_mandate(
        ebull_test_conn,
        enabled=True,
        core_instrument_id=instrument_id,
        core_target_pct=Decimal("70"),
        liquidity_reserve_pct=Decimal("20"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("25"),
        changed_by="test",
        reason="raise the core weight",
    )
    assert second.revision == 2
    assert load_core_mandate(ebull_test_conn) == second

    with pytest.raises(CoreMandateError, match="must alter at least one mandate value"):
        configure_core_mandate(
            ebull_test_conn,
            enabled=True,
            core_instrument_id=instrument_id,
            core_target_pct=Decimal("70"),
            liquidity_reserve_pct=Decimal("20"),
            rebalance_band_pct=Decimal("5"),
            min_rebalance_amount=Decimal("25"),
            changed_by="test",
            reason="no material change",
        )
    ebull_test_conn.rollback()
