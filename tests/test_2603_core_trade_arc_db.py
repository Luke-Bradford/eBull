"""#2603 item 3 step 2 — what ``sql/349`` enforces, and which queries SELECT a core trade.

Two things are under test and they fail differently, so they are kept apart.

**The migration's backstop.** Raw INSERTs, bypassing every writer, because a CHECK
that only ever sees writer-produced values is not demonstrably a backstop
(``docs/review-prevention-log.md``: #2679; same posture as the step-1 module).

**The dispatcher.** ⚠⚠ The defect this slice exists to prevent is NOT "the manager
mishandles a core position" — it is "nothing ever hands the manager one".
``manage_owned_position`` is never called for a trade the paper cycle's batch does
not return, so a widened worker under an unwidened dispatcher looks complete at
every level a diff review inspects. The batch query is therefore exercised
directly, as a query, against rows that exist.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
import pytest

from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION
from app.services.strategy_paper_runtime import _OWNED_BATCH_SQL
from app.services.strategy_position_manager import _LOAD_OWNED_SQL

_INSTRUMENT_ID = 920605
_OTHER_INSTRUMENT_ID = 920606
_PAST = datetime(2026, 1, 2, 9, 0, tzinfo=UTC)


def _seed_instruments(conn: psycopg.Connection[Any]) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'CORE.ARC','Core Arc Test',TRUE),(%s,'CORE.OTHER','Core Arc Other',TRUE) "
        "ON CONFLICT DO NOTHING",
        (_INSTRUMENT_ID, _OTHER_INSTRUMENT_ID),
    )


def _seed_mandate(conn: psycopg.Connection[Any], *, mode: str = CORE_MANDATE_MODE) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason,mode
        ) VALUES (1,TRUE,'USD',%s,60,20,5,25,%s,'test','test',%s)
        RETURNING core_mandate_event_id
        """,
        (_INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION, mode),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_intent(
    conn: psycopg.Connection[Any],
    *,
    event_id: int,
    action: str = "buy_core",
    instrument_id: int = _INSTRUMENT_ID,
) -> int:
    """One actionable intent by default; ``action`` is what the callers vary."""
    refused = action == "refused"
    row = conn.execute(
        """
        INSERT INTO strategy_core_rebalance_intents (
            core_mandate_event_id, allocator_policy_version, recorded_by,
            core_instrument_id, currency, core_market_value, cash_balance,
            state_as_of, action, reason_code, amount, core_pct, target_pct,
            lower_pct, upper_pct, effective_floor, floor_source,
            reserve_breached, reserve_margin_pct
        ) VALUES (
            %(event_id)s, %(policy)s, 'test', %(instrument_id)s, 'USD', 600, 400,
            %(state_as_of)s, %(action)s, %(reason_code)s, %(amount)s,
            %(core_pct)s, %(target_pct)s, %(lower_pct)s, %(upper_pct)s,
            %(floor)s, %(floor_source)s, %(breached)s, %(margin)s
        )
        RETURNING core_rebalance_intent_id
        """,
        {
            "event_id": event_id,
            "policy": CORE_MANDATE_POLICY_VERSION,
            "instrument_id": instrument_id,
            "state_as_of": _PAST,
            "action": action,
            # sql/348 requires a refusal to CITE a code, and separately requires
            # 'core_mandate_absent' to be the one code with no event row. This
            # seed has an event, so the code must be any other one.
            "reason_code": "core_sleeve_empty" if refused else None,
            "amount": Decimal("0") if action in ("hold", "refused") else Decimal("50"),
            # sql/348's shape CHECK requires every derived field NULL on a
            # refusal and non-NULL otherwise, so they move together with action.
            "core_pct": None if refused else Decimal("60"),
            "target_pct": None if refused else Decimal("60"),
            "lower_pct": None if refused else Decimal("55"),
            "upper_pct": None if refused else Decimal("65"),
            "floor": None if refused else Decimal("25"),
            "floor_source": None if refused else "mandate",
            "breached": None if refused else False,
            "margin": None if refused else Decimal("10"),
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_core_position(
    conn: psycopg.Connection[Any],
    *,
    intent_id: int,
    instrument_id: int = _INSTRUMENT_ID,
) -> tuple[int, int]:
    """An open core trade with active ownership. Returns (trade_id, position_id)."""
    trade = conn.execute(
        "INSERT INTO strategy_trades (core_rebalance_intent_id,instrument_id,status) "
        "VALUES (%s,%s,'open') RETURNING strategy_trade_id",
        (intent_id, instrument_id),
    ).fetchone()
    assert trade is not None
    trade_id = int(trade[0])
    position_id = 7_700_000 + trade_id
    conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id,broker_position_id,status,claimed_at) "
        "VALUES (%s,%s,'active',now())",
        (trade_id, position_id),
    )
    return trade_id, position_id


# --------------------------------------------------------------------------
# The migration's backstop.
# --------------------------------------------------------------------------


def test_a_core_trade_stores_with_no_funding_decision(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The whole point of the arc: before sql/349 this row could not exist."""
    _seed_instruments(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    trade_id, _ = _seed_core_position(ebull_test_conn, intent_id=intent_id)
    row = ebull_test_conn.execute(
        "SELECT funding_decision_id,core_rebalance_intent_id FROM strategy_trades WHERE strategy_trade_id=%s",
        (trade_id,),
    ).fetchone()
    assert row == (None, intent_id)


def test_neither_authorisation_is_rejected(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠ The half a plain nullable column would have silently allowed.

    Dropping NOT NULL on funding_decision_id without the exactly-one CHECK would
    make an UNAUTHORISED trade storable -- a strictly worse state than the
    pre-migration one it was meant to relax.
    """
    _seed_instruments(ebull_test_conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "INSERT INTO strategy_trades (instrument_id,status) VALUES (%s,'open')",
            (_INSTRUMENT_ID,),
        )


def test_both_authorisations_are_rejected(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed_instruments(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    deployment_id, funding_id = _seed_signal_funding(ebull_test_conn)
    assert deployment_id
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "INSERT INTO strategy_trades (funding_decision_id,core_rebalance_intent_id,instrument_id,status) "
            "VALUES (%s,%s,%s,'open')",
            (funding_id, intent_id, _INSTRUMENT_ID),
        )


def test_one_trade_per_intent(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """Each arm keeps one-trade-per-authorisation; a repeat rebalance must not
    open a second position against a verdict already acted on."""
    _seed_instruments(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    _seed_core_position(ebull_test_conn, intent_id=intent_id)
    with pytest.raises(psycopg.errors.UniqueViolation):
        ebull_test_conn.execute(
            "INSERT INTO strategy_trades (core_rebalance_intent_id,instrument_id,status) VALUES (%s,%s,'open')",
            (intent_id, _INSTRUMENT_ID),
        )


def test_a_mandate_requires_an_explicit_mode(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠ NOT DEFAULTED, on purpose: a writer that forgets must fail rather than
    inherit safety it never asked for."""
    _seed_instruments(ebull_test_conn)
    with pytest.raises(psycopg.errors.NotNullViolation):
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_core_mandate_events (
                revision,enabled,base_currency,core_instrument_id,core_target_pct,
                liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
                policy_version,changed_by,reason
            ) VALUES (1,TRUE,'USD',%s,60,20,5,25,%s,'test','test')
            """,
            (_INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION),
        )


# --------------------------------------------------------------------------
# The dispatcher: which rows the paper cycle's batch actually SELECTS.
# --------------------------------------------------------------------------


def _batch_trade_ids(conn: psycopg.Connection[Any]) -> set[int]:
    return {int(row[0]) for row in conn.execute(_OWNED_BATCH_SQL, (0, 50)).fetchall()}


def test_the_owned_batch_selects_a_core_position(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠⚠ THE DECISIVE ASSERTION OF THIS SLICE.

    If this fails, ``manage_owned_position`` is never invoked for a core holding
    and every other change here is dead code -- which is exactly the state the
    first two drafts of the plan would have shipped.
    """
    _seed_instruments(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    trade_id, _ = _seed_core_position(ebull_test_conn, intent_id=intent_id)
    assert trade_id in _batch_trade_ids(ebull_test_conn)


@pytest.mark.parametrize("action", ["hold", "refused"])
def test_the_owned_batch_refuses_a_non_actionable_intent(ebull_test_conn: psycopg.Connection[Any], action: str) -> None:
    """sql/348 stores holds and refusals as evidence, so the FK alone would let a
    refusal back a trade. The load predicate, not convention, is what forbids it."""
    _seed_instruments(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn), action=action)
    trade_id, _ = _seed_core_position(ebull_test_conn, intent_id=intent_id)
    assert trade_id not in _batch_trade_ids(ebull_test_conn)


def test_the_loader_refuses_a_trade_whose_instrument_is_not_the_intents(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Without this the manager would be authorised to close a DIFFERENT
    instrument's position. The two columns live in separate tables, so this is a
    load-time predicate rather than a CHECK."""
    _seed_instruments(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    trade_id, position_id = _seed_core_position(
        ebull_test_conn, intent_id=intent_id, instrument_id=_OTHER_INSTRUMENT_ID
    )
    loaded = ebull_test_conn.execute(_LOAD_OWNED_SQL, (trade_id, position_id)).fetchone()
    assert loaded is None


def test_the_loader_returns_a_core_position_with_null_signal_policy(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The four fields that became nullable, asserted as NULL together -- they are
    what the manager's core exemption then has to avoid dereferencing."""
    _seed_instruments(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    trade_id, position_id = _seed_core_position(ebull_test_conn, intent_id=intent_id)
    with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_LOAD_OWNED_SQL, (trade_id, position_id))
        row = cur.fetchone()
    assert row is not None
    assert row["core_rebalance_intent_id"] == intent_id
    assert row["deployment_id"] is None
    assert row["entry_stop"] is None
    assert row["entry_take_profit"] is None
    assert row["max_quote_age_seconds"] is None


# --------------------------------------------------------------------------
# The signal arm must be untouched by all of the above.
# --------------------------------------------------------------------------


def _seed_signal_funding(conn: psycopg.Connection[Any]) -> tuple[int, int]:
    """A paper deployment with one allocated funding decision. Returns (deployment, funding)."""
    _seed_instruments(conn)
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,
            input_rule_set_versions
        )
        VALUES ('arc_test','arc-v1',%s,DATE '2026-01-02','entry','fired',
                DATE '2026-01-05',10,'survivor_only','{"arc":"v1"}'::jsonb)
        RETURNING signal_id
        """,
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert signal is not None
    deployment = conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id,strategy_version,mode,capital_limit,currency,enabled,updated_by,reason
        )
        VALUES ('arc_test','arc-v1','paper',1000,'USD',TRUE,'test','test')
        RETURNING deployment_id
        """
    ).fetchone()
    assert deployment is not None
    funding = conn.execute(
        """
        INSERT INTO strategy_funding_decisions (deployment_id,signal_id,verdict,amount,reason_code)
        VALUES (%s,%s,'allocated',100,'test')
        RETURNING funding_decision_id
        """,
        (int(deployment[0]), int(signal[0])),
    ).fetchone()
    assert funding is not None
    return int(deployment[0]), int(funding[0])


def test_the_owned_batch_still_selects_a_signal_position(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ The regression the LEFT-JOIN rewrite could silently cause.

    Converting an INNER JOIN that also FILTERS into a LEFT JOIN moves that filter
    into the WHERE clause or deletes it. This asserts the signal arm still loads
    at all; the paired test below asserts the filter was moved, not deleted.
    """
    _, funding_id = _seed_signal_funding(ebull_test_conn)
    trade = ebull_test_conn.execute(
        "INSERT INTO strategy_trades (funding_decision_id,instrument_id,status) "
        "VALUES (%s,%s,'open') RETURNING strategy_trade_id",
        (funding_id, _INSTRUMENT_ID),
    ).fetchone()
    assert trade is not None
    trade_id = int(trade[0])
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id,broker_position_id,status,claimed_at) "
        "VALUES (%s,%s,'active',now())",
        (trade_id, 7_800_000 + trade_id),
    )
    assert trade_id in _batch_trade_ids(ebull_test_conn)


def test_the_owned_batch_still_excludes_a_live_deployment(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠⚠ The paper gate, asserted on the INPUT rather than assumed from the shape.

    ``d.mode='paper'`` moved from the WHERE clause into a LEFT JOIN's ON clause.
    That is only safe because ``d.deployment_id IS NOT NULL`` witnesses it -- and
    if the witness were ever dropped, every previous test here would still pass
    while a LIVE position started loading.
    """
    _seed_instruments(ebull_test_conn)
    signal = ebull_test_conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,
            input_rule_set_versions
        )
        VALUES ('arc_live','arc-v1',%s,DATE '2026-01-02','entry','fired',
                DATE '2026-01-05',10,'survivor_only','{"arc":"v1"}'::jsonb)
        RETURNING signal_id
        """,
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert signal is not None
    deployment = ebull_test_conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id,strategy_version,mode,capital_limit,currency,enabled,updated_by,reason
        )
        VALUES ('arc_live','arc-v1','live',1000,'USD',TRUE,'test','test')
        RETURNING deployment_id
        """
    ).fetchone()
    assert deployment is not None
    funding = ebull_test_conn.execute(
        """
        INSERT INTO strategy_funding_decisions (deployment_id,signal_id,verdict,amount,reason_code)
        VALUES (%s,%s,'allocated',100,'test')
        RETURNING funding_decision_id
        """,
        (int(deployment[0]), int(signal[0])),
    ).fetchone()
    assert funding is not None
    trade = ebull_test_conn.execute(
        "INSERT INTO strategy_trades (funding_decision_id,instrument_id,status) "
        "VALUES (%s,%s,'open') RETURNING strategy_trade_id",
        (int(funding[0]), _INSTRUMENT_ID),
    ).fetchone()
    assert trade is not None
    trade_id = int(trade[0])
    ebull_test_conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id,broker_position_id,status,claimed_at) "
        "VALUES (%s,%s,'active',now())",
        (trade_id, 7_900_000 + trade_id),
    )
    assert trade_id not in _batch_trade_ids(ebull_test_conn)
