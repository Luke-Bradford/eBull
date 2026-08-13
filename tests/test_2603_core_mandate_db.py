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

import re
from decimal import Decimal
from pathlib import Path
from typing import Any, LiteralString, cast
from uuid import uuid4

import psycopg
import pytest

from app.services.strategy_core_mandate import (
    CORE_MANDATE_POLICY_VERSION,
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
    %(policy_version)s,'test','test'
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
    # Parameterised rather than inlined so the #2670 bump is exercised as a
    # CHECK and not merely assumed: a literal here would make a stale version
    # untestable at the only layer that can still be handed one.
    "policy_version": CORE_MANDATE_POLICY_VERSION,
}


def _migration_precondition_block() -> LiteralString:
    """sql/344's superseded-version guard, read from the SHIPPED file.

    Read rather than re-typed on purpose: a test that restates the SQL it is
    checking passes when the migration and the copy drift apart, which is the
    tautology the prevention log already names for constants.

    The ``cast`` is the one thing a file read costs: psycopg's ``execute`` takes
    a ``LiteralString`` so caller-controlled SQL cannot reach it. Sound here and
    nowhere near a request path — the source is a repo-controlled migration this
    process already executes verbatim at boot, and nothing interpolates into it.
    """
    sql = Path("sql/344_core_mandate_trigger_reachability.sql").read_text(encoding="utf-8")
    match = re.search(r"^DO \$\$.*?^END \$\$;$", sql, re.S | re.M)
    assert match is not None, "sql/344 no longer contains a DO-block precondition"
    return cast("LiteralString", match.group(0))


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


def _seed_proved_account(conn: psycopg.Connection[Any], instrument_id: int) -> dict[str, Any]:
    """An operator with a live credential pair and a passing eligibility proof.

    #2603 item 2 gates an ENABLED mandate on one; the gate itself is covered in
    ``test_2603_core_eligibility_db``, so this is only the setup the writer's own
    revision behaviour now needs.
    """
    operator_id = uuid4()
    conn.execute(
        "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s,%s,'x')",
        (operator_id, f"op_{operator_id.hex[:8]}"),
    )
    credential_ids: list[Any] = []
    for label in ("api_key", "user_key"):
        row = conn.execute(
            """
            INSERT INTO broker_credentials
                (operator_id, provider, label, environment, ciphertext, last_four, key_version)
            VALUES (%s,'etoro',%s,'demo','\\x00'::bytea,'0000',1)
            RETURNING id
            """,
            (operator_id, label),
        ).fetchone()
        assert row is not None
        credential_ids.append(row[0])
    conn.execute(
        """
        INSERT INTO strategy_core_eligibility_proofs (
            instrument_id, operator_id, provider, environment,
            api_key_credential_id, user_key_credential_id,
            verdict, requested_currency, response_currency,
            settlement_type, direction, leverage_values, qualifying_arm_count,
            allow_open_position, response_digest, policy_version, recorded_by
        ) VALUES (
            %s,%s,'etoro','demo',%s,%s,'underlying','USD','usd','real','long',
            ARRAY[1],1,TRUE,%s,'core-eligibility-v1','test'
        )
        """,
        (instrument_id, operator_id, credential_ids[0], credential_ids[1], "a" * 64),
    )
    return {"operator_id": operator_id, "provider": "etoro", "environment": "demo"}


def test_the_reference_row_inserts(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The control. Without it a CHECK test could pass for the wrong reason."""
    ebull_test_conn.execute(_INSERT, _VALID_ROW)
    assert load_core_mandate(ebull_test_conn) is not None


@pytest.mark.parametrize(
    "overrides",
    [
        # #2670 tightened two bounds; these pin that it did not tighten a third.
        # Worst-case cash exactly equal to a POSITIVE reserve stays storable —
        # band_respects_reserve is deliberately still `>=`, and the upper trigger
        # is live at 90 < 100.
        {
            "core_target_pct": Decimal("60"),
            "rebalance_band_pct": Decimal("30"),
            "liquidity_reserve_pct": Decimal("10"),
        },
        # One NUMERIC(8,4) quantum inside each dead point.
        {
            "core_target_pct": Decimal("20"),
            "rebalance_band_pct": Decimal("19.9999"),
            "liquidity_reserve_pct": Decimal("0"),
        },
        {
            "core_target_pct": Decimal("60"),
            "rebalance_band_pct": Decimal("39.9999"),
            "liquidity_reserve_pct": Decimal("0"),
        },
    ],
)
def test_mandates_one_quantum_inside_the_dead_points_still_insert(
    ebull_test_conn: psycopg.Connection[Any], overrides: dict[str, Any]
) -> None:
    """A tightening is only correct if it stops exactly where it says it does.

    Raw SQL on purpose, per this module's contract: these bound the MIGRATION,
    so a later one that over-corrected to `>=` on the reserve — or moved either
    strict bound by more than a quantum — fails here rather than silently
    shrinking what an operator can declare.
    """
    ebull_test_conn.execute(_INSERT, {**_VALID_ROW, **overrides})
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
        # #2670, lower dead point: band == target gives lower == 0, and
        # `core_pct < 0` is unreachable. sql/336 stored this; sql/344 refuses it.
        (
            {
                "core_target_pct": Decimal("20"),
                "rebalance_band_pct": Decimal("20"),
                "liquidity_reserve_pct": Decimal("0"),
            },
            "strategy_core_mandate_band_within_range",
        ),
        # #2670, upper dead point: target + band == 100 at reserve 0 gives
        # upper == 100, and `core_pct > 100` is unreachable. Note it clears
        # band_respects_reserve at equality, which is why it needs its own name.
        (
            {
                "core_target_pct": Decimal("60"),
                "rebalance_band_pct": Decimal("40"),
                "liquidity_reserve_pct": Decimal("0"),
            },
            "strategy_core_mandate_band_upper_reachable",
        ),
        # The same dead point at NUMERIC(8,4) granularity.
        (
            {
                "core_target_pct": Decimal("99.9999"),
                "rebalance_band_pct": Decimal("0.0001"),
                "liquidity_reserve_pct": Decimal("0"),
            },
            "strategy_core_mandate_band_upper_reachable",
        ),
        # #2670's version bump, enforced at rest: a row written under the looser
        # v1 arithmetic is no longer storable.
        (
            {"policy_version": "core-mandate-v1"},
            "strategy_core_mandate_events_policy_version_check",
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


def test_the_migration_precondition_is_a_no_op_on_a_clean_database(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The control: without it, the test below could pass on a broken block."""
    ebull_test_conn.execute(_migration_precondition_block())


def test_the_migration_precondition_names_superseded_rows_rather_than_aborting_opaquely(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2670, raised at Codex checkpoint 2.

    sql/344's `policy_version = 'core-mandate-v2'` CHECK aborts the migration —
    and boot — on any database holding a v1 revision, even one satisfying the new
    band bounds. That abort is intended, so the guard exists to make it a named,
    actionable failure instead of a `CheckViolation` on a bookkeeping stamp.

    Exercised by dropping the CHECK inside this transaction, which is the only
    way to create the state the guard is for now that the CHECK forbids it. DDL
    is transactional in Postgres, so the rollback restores it.
    """
    ebull_test_conn.execute(
        "ALTER TABLE strategy_core_mandate_events DROP CONSTRAINT strategy_core_mandate_events_policy_version_check"
    )
    ebull_test_conn.execute(_INSERT, {**_VALID_ROW, "policy_version": "core-mandate-v1"})

    with pytest.raises(psycopg.errors.RaiseException) as caught:
        ebull_test_conn.execute(_migration_precondition_block())

    assert "superseded policy_version" in str(caught.value)
    # The count, so the operator learns the size of the problem from the message.
    assert "1 core mandate revision(s)" in str(caught.value)
    # And the remedy, including the one Codex proposed and the reason it is wrong.
    assert "storable and unusable" in (caught.value.diag.message_hint or "")
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
    account = _seed_proved_account(ebull_test_conn, instrument_id)

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
        **account,
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
        **account,
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
            **account,
        )
    ebull_test_conn.rollback()
