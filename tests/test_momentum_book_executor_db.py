from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services import momentum_book as mb
from app.services import momentum_book_executor as mx
from app.services import momentum_book_intent as mi
from app.services.momentum_book_job import form_book
from app.services.strategy_control_plane import configure_deployment, configure_execution_policy, configure_paper_pool
from app.services.strategy_paper_executor import StrategyPaperExecutionError
from tests.fixtures.entry_ticket import assert_entry_ticket
from tests.test_ai_trial_executor_db import _broker, _enable_trading
from tests.test_momentum_book_job_db import SESSION, _seed

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("marked_daily_loss_neutral")]

Conn = psycopg.Connection[Any]
NOW = datetime(2026, 10, 15, 15, 30, tzinfo=UTC)


def _market(conn: Conn, book: tuple[int, ...]) -> None:
    conn.execute("UPDATE instruments SET currency = 'USD' WHERE instrument_id = ANY(%s)", (list(book),))
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag) "
        "SELECT i, %s, 99.9, 100, 99.95, 0.1, false FROM unnest(%s::bigint[]) AS i "
        "ON CONFLICT (instrument_id) DO UPDATE SET quoted_at = EXCLUDED.quoted_at",
        (NOW, list(book)),
    )
    conn.execute(
        "INSERT INTO strategy_halt_feed_state (source, fetched_at, source_pub_at, item_count, payload_sha256) "
        "VALUES ('nasdaq_trader_rss', %s, %s, 0, %s) "
        "ON CONFLICT (source) DO UPDATE SET fetched_at = EXCLUDED.fetched_at",
        (NOW, NOW, "0" * 64),
    )
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("20000"),
        risk_profile="balanced",
        approval_mode="manual",
        max_concurrent_positions_override=None,
        changed_by="test",
        reason="#3822 executor fixture",
    )
    deployment = configure_deployment(
        conn,
        strategy_id=mb.STRATEGY_ID,
        strategy_version=mb.STRATEGY_VERSION,
        mode="paper",
        capital_limit=Decimal("10000"),
        enabled=True,
        changed_by="test",
        reason="#3822 executor fixture",
    )
    configure_execution_policy(
        conn,
        deployment_id=deployment.deployment_id,
        ticket_sizing_mode="fixed",
        ticket_fraction=None,
        fixed_ticket_amount=Decimal("40"),
        max_ticket_amount=Decimal("40"),
        stop_loss_pct=Decimal("25"),
        take_profit_pct=Decimal("100"),
        max_quote_age_seconds=600,
        max_scan_age_seconds=86400,
        max_halt_feed_age_seconds=600,
        max_cost_age_seconds=600,
        max_reconciliation_age_seconds=3600,
        max_instrument_exposure_pct=Decimal("10"),
        max_portfolio_exposure_pct=Decimal("100"),
        max_drawdown_pct=Decimal("25"),
        min_net_expectancy_pct=Decimal("0"),
        cost_stress_multiplier=Decimal("1"),
        changed_by="test",
        reason="#3822 executor fixture",
    )
    _enable_trading(conn)
    conn.commit()


def _stub_broker(instrument_id: int) -> MagicMock:
    broker = _broker(instrument_id, spread_value="0.1", equity="20000", available_cash="15000")
    broker.get_account_risk_snapshot.return_value = replace(
        broker.get_account_risk_snapshot.return_value, observed_at=NOW
    )
    broker.get_what_if_costs.return_value = replace(broker.get_what_if_costs.return_value, last_updated=NOW)
    return broker


def test_a_formed_entry_submits_with_its_levels_and_ticket(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)

    signal_id = formed.entry_signal_ids[0]
    broker = _stub_broker(formed.book[0])
    result = mx.execute_momentum_signal(conn, broker=broker, signal_id=signal_id, now=NOW)
    assert result.verdict == "submitted", result
    assert result.amount == Decimal("40")
    sent = broker.place_demo_strategy_order.call_args.args[0]
    assert (sent.stop_loss_rate, sent.take_profit_rate) == (Decimal("75.00"), Decimal("200.00"))
    assert result.order_id is not None
    assert_entry_ticket(
        conn,
        signal_id,
        rationale_class="signal",
        evidence_kind="momentum_book_formation",
        evidence_id=formed.formation_id,
    )

    conn.commit()
    # Decided once: a second call returns the stored result and sends nothing.
    broker.place_demo_strategy_order.reset_mock()
    again = mx.execute_momentum_signal(conn, broker=broker, signal_id=signal_id, now=NOW)
    assert again.verdict == "submitted" and again.order_id == result.order_id
    broker.place_demo_strategy_order.assert_not_called()


def test_an_entry_past_its_session_is_refused_expired(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    later = datetime(2026, 10, 16, 15, 30, tzinfo=UTC)
    broker = _stub_broker(formed.book[0])
    result = mx.execute_momentum_signal(conn, broker=broker, signal_id=formed.entry_signal_ids[0], now=later)
    assert (result.verdict, result.reason_code) == ("rejected", "decision_expired")
    broker.place_demo_strategy_order.assert_not_called()


def test_another_strategys_signal_is_raised_not_persisted(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    other = conn.execute(
        """
        INSERT INTO strategy_signals (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind,
            verdict, fill_bar_date, fill_price, universe, input_rule_set_versions)
        SELECT 'ranking-pot-v1', 'v1', min(instrument_id), '2026-10-14', 'entry', 'fired', '2026-10-15', 50,
               'survivor_only', '{"x": "y"}'
        FROM instruments
        RETURNING signal_id
        """
    ).fetchone()
    assert other is not None
    conn.commit()
    with pytest.raises(StrategyPaperExecutionError, match="not a momentum-book signal"):
        mx.execute_momentum_signal(conn, broker=_stub_broker(1), signal_id=int(other[0]), now=NOW)


def test_null_joins_refuse_in_order_without_raising(ebull_test_conn: Conn) -> None:
    """No deployment, policy, pool, quote or halt feed: every LEFT JOIN is NULL, and the loader returns the first
    ordered refusal rather than raising (review WARNING on #3825)."""
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    conn.commit()
    intent, reason, halt_ran = mi.load_intent(conn, signal_id=formed.entry_signal_ids[0], now=NOW)
    assert (intent, reason, halt_ran) == (None, "paper_deployment_missing", True)
