"""#3284 item 4a — the streak UPSERTs, against real rows.

One integration test per genuinely-new SQL mechanism (repo test-quality rule). The
mechanism here is three ``ON CONFLICT`` branches over one primary key plus a shape CHECK,
and none of it is provable against a mocked cursor: whether a second refusal INCREMENTS
rather than resetting to 1, whether ``first_refused_at`` survives the update that changes
the reason, and whether the CHECK admits a live streak with no reason are all properties
of Postgres executing the statement.

⚠ ``_db`` file, separate from ``test_3284_repair_refusal_streak.py``, deliberately: the
``db`` marker is applied per MODULE, so one DB test in the pure file would evict every
classification test from the fast tier.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest

from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION
from app.services.strategy_position_repair_streak import RepairStreakError, record_repair_visit

INSTRUMENT_ID = 990_284
_NOW = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
#: ``core_rebalance_intent_state_not_after_evaluation`` compares ``state_as_of`` against
#: ``evaluated_at``, which defaults to ``now()`` — so the fixture stamp must be past.
_PAST = datetime(2026, 1, 2, 9, 0, tzinfo=UTC)


def _ownership(conn: psycopg.Connection[Any]) -> int:
    """One active core-arm ownership row, returning its ``ownership_id``.

    ⚠ ``strategy_trades_exactly_one_authorisation`` is
    ``num_nonnulls(funding_decision_id, core_rebalance_intent_id) = 1``, so there is no
    cheaper "just a trade row" — the mandate event → intent → trade chain is the minimum.
    Core arm because the core sleeve is the position this ticket protects; the streak
    itself is arm-agnostic.
    """
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'RPR.STREAK','Repair Streak Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    event = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason,mode
        ) VALUES (1,TRUE,'USD',%s,60,20,5,25,%s,'test','test',%s)
        RETURNING core_mandate_event_id
        """,
        (INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION, CORE_MANDATE_MODE),
    ).fetchone()
    assert event is not None
    intent = conn.execute(
        """
        INSERT INTO strategy_core_rebalance_intents (
            core_mandate_event_id, allocator_policy_version, recorded_by,
            core_instrument_id, currency, core_market_value, cash_balance,
            state_as_of, action, amount, core_pct, target_pct,
            lower_pct, upper_pct, effective_floor, floor_source,
            reserve_breached, reserve_margin_pct
        ) VALUES (%s,%s,'test',%s,'USD',600,400,%s,'buy_core',50,60,60,55,65,25,'mandate',FALSE,10)
        RETURNING core_rebalance_intent_id
        """,
        (event[0], CORE_MANDATE_POLICY_VERSION, INSTRUMENT_ID, _PAST),
    ).fetchone()
    assert intent is not None
    trade = conn.execute(
        "INSERT INTO strategy_trades (core_rebalance_intent_id,instrument_id,status) "
        "VALUES (%s,%s,'open') RETURNING strategy_trade_id",
        (intent[0], INSTRUMENT_ID),
    ).fetchone()
    assert trade is not None
    own = conn.execute(
        "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id, status) "
        "VALUES (%s, 7_284_001, 'active') RETURNING ownership_id",
        (trade[0],),
    ).fetchone()
    assert own is not None
    conn.commit()
    return int(own[0])


def _streak(conn: psycopg.Connection[Any], ownership_id: int) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT consecutive_refusals, first_refused_at, last_refusal_reason, last_checked_at "
        "FROM strategy_position_repair_streaks WHERE ownership_id=%s",
        (ownership_id,),
    ).fetchone()
    assert row is not None
    return row


def test_the_streak_accrues_then_resets_and_dates_the_episode(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The whole mechanism in one arc, because the states only mean anything in sequence.

    A streak of one is indistinguishable from a transient; what item 4b needs is that a
    SECOND consecutive refusal reads as two, that the episode keeps its ORIGINAL start
    even as the reason changes, and that a protected visit ends it — so that a later
    failure is a new episode rather than a resumed total.
    """
    conn = ebull_test_conn
    ownership_id = _ownership(conn)

    # Nothing recorded until the arm actually evaluates the position.
    assert conn.execute(
        "SELECT count(*) FROM strategy_position_repair_streaks WHERE ownership_id=%s",
        (ownership_id,),
    ).fetchone() == (0,)

    assert (
        record_repair_visit(
            conn,
            ownership_id=ownership_id,
            state="rejected",
            reason_code="fixed_exit_quote_unsafe",
            observed_at=_NOW,
        )
        == "refused"
    )
    assert _streak(conn, ownership_id) == (1, _NOW, "fixed_exit_quote_unsafe", _NOW)

    # A second refusal five minutes later — one owned-batch rotation.
    later = _NOW + timedelta(minutes=5)
    record_repair_visit(
        conn,
        ownership_id=ownership_id,
        state="rejected",
        reason_code="broker_fixed_exit_edit_not_allowed",
        observed_at=later,
    )
    # ⚠ `first_refused_at` stays at the FIRST refusal: it dates the episode, so 4b can
    # report how long the position has been refusing. The reason is the latest one.
    assert _streak(conn, ownership_id) == (2, _NOW, "broker_fixed_exit_edit_not_allowed", later)

    # An undecided visit stamps the clock and leaves the live streak alone. Counting it
    # would alert about a stop that may already be set; clearing it would hide a real one.
    undecided = later + timedelta(minutes=5)
    assert (
        record_repair_visit(
            conn,
            ownership_id=ownership_id,
            state="reconcile_required",
            reason_code="broker_edit_uncertain",
            observed_at=undecided,
        )
        == "unknown"
    )
    assert _streak(conn, ownership_id) == (2, _NOW, "broker_fixed_exit_edit_not_allowed", undecided)

    # The position comes back protected: the episode is over, detail cleared.
    protected = undecided + timedelta(minutes=5)
    assert (
        record_repair_visit(
            conn,
            ownership_id=ownership_id,
            state="no_change",
            reason_code="position_protected",
            observed_at=protected,
        )
        == "cleared"
    )
    assert _streak(conn, ownership_id) == (0, None, None, protected)

    # A later failure is a NEW episode, dated from itself rather than from _NOW.
    again = protected + timedelta(hours=2)
    record_repair_visit(
        conn,
        ownership_id=ownership_id,
        state="rejected",
        reason_code="fixed_exit_quote_unsafe",
        observed_at=again,
    )
    assert _streak(conn, ownership_id) == (1, again, "fixed_exit_quote_unsafe", again)


def test_a_live_streak_cannot_exist_without_a_reason(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The shape CHECK, asserted at the DB rather than trusted from the DDL.

    ``strategy_position_ownership_release_shape`` — the precedent this constraint was
    written from — uses a bare ``release_reason <> ''``, which passes on NULL and so
    admits the very row it forbids. This one names ``IS NOT NULL`` explicitly, and this
    test is what proves the difference is real rather than cosmetic.
    """
    conn = ebull_test_conn
    ownership_id = _ownership(conn)
    with pytest.raises(psycopg.errors.CheckViolation, match="strategy_position_repair_streak_shape"):
        conn.execute(
            "INSERT INTO strategy_position_repair_streaks "
            "(ownership_id, consecutive_refusals, first_refused_at, last_refusal_reason, last_checked_at) "
            "VALUES (%s, 3, %s, NULL, %s)",
            (ownership_id, _NOW, _NOW),
        )
    conn.rollback()

    # And the service layer refuses before it can even try.
    with pytest.raises(RepairStreakError, match="must carry a reason code"):
        record_repair_visit(
            conn,
            ownership_id=ownership_id,
            state="rejected",
            reason_code="",
            observed_at=_NOW,
        )
