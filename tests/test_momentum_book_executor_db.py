from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.market_data import IntradayBar
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


def _quotes(conn: Conn, book: tuple[int, ...]) -> None:
    conn.execute("UPDATE instruments SET currency = 'USD' WHERE instrument_id = ANY(%s)", (list(book),))
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag) "
        "SELECT i, %s, 99.9, 100, 99.95, 0.1, false FROM unnest(%s::bigint[]) AS i "
        "ON CONFLICT (instrument_id) DO UPDATE SET quoted_at = EXCLUDED.quoted_at",
        (NOW, list(book)),
    )


def _halt_feed(conn: Conn) -> None:
    conn.execute(
        "INSERT INTO strategy_halt_feed_state (source, fetched_at, source_pub_at, item_count, payload_sha256) "
        "VALUES ('nasdaq_trader_rss', %s, %s, 0, %s) "
        "ON CONFLICT (source) DO UPDATE SET fetched_at = EXCLUDED.fetched_at",
        (NOW, NOW, "0" * 64),
    )


def _pool(conn: Conn) -> None:
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


def _deployment(conn: Conn) -> int:
    return configure_deployment(
        conn,
        strategy_id=mb.STRATEGY_ID,
        strategy_version=mb.STRATEGY_VERSION,
        mode="paper",
        capital_limit=Decimal("10000"),
        enabled=True,
        changed_by="test",
        reason="#3822 executor fixture",
    ).deployment_id


def _policy(conn: Conn, deployment_id: int, *, fixed_ticket: Decimal | None = Decimal("40")) -> None:
    """``fixed_ticket=None`` configures percent sizing, which the momentum book refuses."""
    configure_execution_policy(
        conn,
        deployment_id=deployment_id,
        ticket_sizing_mode="fixed" if fixed_ticket is not None else "percent",
        ticket_fraction=None if fixed_ticket is not None else Decimal("0.01"),
        fixed_ticket_amount=fixed_ticket,
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


def _market(conn: Conn, book: tuple[int, ...]) -> None:
    _quotes(conn, book)
    _halt_feed(conn)
    _pool(conn)
    _policy(conn, _deployment(conn))
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
    result = mx.execute_momentum_signal(conn, broker=broker, signal_id=signal_id, clock=lambda: NOW)
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
    again = mx.execute_momentum_signal(conn, broker=broker, signal_id=signal_id, clock=lambda: NOW)
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
    result = mx.execute_momentum_signal(conn, broker=broker, signal_id=formed.entry_signal_ids[0], clock=lambda: later)
    assert (result.verdict, result.reason_code) == ("rejected", "decision_expired")
    broker.place_demo_strategy_order.assert_not_called()


def test_costs_are_judged_when_they_arrive_not_when_evaluation_began(ebull_test_conn: Conn) -> None:
    """eToro's what-if ``lastUpdated`` is the server time of the call, about 8 s after evaluation starts. Judged
    against the start it is 'in the future' and was refused ``costs_stale_or_mismatched`` (#3822, 16 of 30 on
    2026-10-15)."""
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    arrived = NOW + timedelta(seconds=8)
    broker = _stub_broker(formed.book[0])
    broker.get_what_if_costs.return_value = replace(broker.get_what_if_costs.return_value, last_updated=arrived)
    reads = iter([NOW, arrived])
    result = mx.execute_momentum_signal(
        conn, broker=broker, signal_id=formed.entry_signal_ids[0], clock=lambda: next(reads, arrived)
    )
    assert result.verdict == "submitted", result


def test_a_stale_quote_defers_without_deciding_and_a_later_fire_submits(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    signal_id = formed.entry_signal_ids[0]
    conn.execute(
        "UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (NOW - timedelta(hours=1), formed.book[0])
    )
    conn.commit()
    broker = _stub_broker(formed.book[0])

    deferred = mx.execute_momentum_signal(conn, broker=broker, signal_id=signal_id, clock=lambda: NOW)
    assert (deferred.verdict, deferred.reason_code) == ("deferred", "quote_stale")
    written = conn.execute(
        "SELECT (SELECT count(*) FROM strategy_funding_decisions WHERE signal_id = %(s)s),"
        "       (SELECT count(*) FROM strategy_entry_preflights WHERE signal_id = %(s)s)",
        {"s": signal_id},
    ).fetchone()
    conn.commit()
    assert written == (0, 0)

    conn.execute("UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (NOW, formed.book[0]))
    conn.commit()
    result = mx.execute_momentum_signal(conn, broker=broker, signal_id=signal_id, clock=lambda: NOW)
    assert result.verdict == "submitted", result


def _bar(at: datetime, close: str) -> IntradayBar:
    c = Decimal(close)
    return IntradayBar(timestamp=at, open=c, high=c, low=c, close=c, volume=None)


def test_a_delayed_quote_is_priced_from_the_latest_candle_close(ebull_test_conn: Conn) -> None:
    """eToro serves some names a quote 1–2 h late while their OneMinute candles stay real-time (#3833)."""
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    signal_id = formed.entry_signal_ids[0]
    conn.execute(
        "UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (NOW - timedelta(hours=1), formed.book[0])
    )
    conn.commit()
    broker = _stub_broker(formed.book[0])
    asked: list[int] = []

    def latest_bar(instrument_id: int) -> IntradayBar:
        asked.append(instrument_id)
        return _bar(NOW - timedelta(seconds=30), "120")

    result = mx.execute_momentum_signal(
        conn, broker=broker, signal_id=signal_id, clock=lambda: NOW, latest_bar=latest_bar
    )
    assert result.verdict == "submitted", result
    assert asked == [formed.book[0]]
    sent = broker.place_demo_strategy_order.call_args.args[0]
    assert (sent.stop_loss_rate, sent.take_profit_rate) == (Decimal("90.00"), Decimal("240.00"))
    row = conn.execute(
        "SELECT p.price_reference, p.quote_at, p.quote_ask, tk.why_now FROM strategy_entry_preflights p, "
        "strategy_entry_tickets tk WHERE p.signal_id = %s AND tk.order_id = %s",
        (signal_id, result.order_id),
    ).fetchone()
    conn.commit()
    assert row is not None
    assert row[:3] == ("candle_close", NOW - timedelta(seconds=30), Decimal("120"))
    assert "#3833" in row[3]


@pytest.mark.parametrize(
    "latest_bar",
    [
        pytest.param(lambda _i: _bar(NOW - timedelta(hours=1), "120"), id="candle-also-stale"),
        pytest.param(lambda _i: None, id="no-candle"),
        pytest.param(lambda _i: (_ for _ in ()).throw(RuntimeError("candles down")), id="candle-read-raised"),
    ],
)
def test_a_delayed_quote_without_a_fresh_candle_still_defers(ebull_test_conn: Conn, latest_bar: mx.LatestBar) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    signal_id = formed.entry_signal_ids[0]
    conn.execute(
        "UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (NOW - timedelta(hours=1), formed.book[0])
    )
    conn.commit()
    broker = _stub_broker(formed.book[0])
    result = mx.execute_momentum_signal(
        conn, broker=broker, signal_id=signal_id, clock=lambda: NOW, latest_bar=latest_bar
    )
    assert (result.verdict, result.reason_code) == ("deferred", "quote_stale")
    broker.place_demo_strategy_order.assert_not_called()


def test_a_candle_priced_entry_refuses_a_wide_live_spread_and_records_its_reference(ebull_test_conn: Conn) -> None:
    """The candle has no bid/ask, so the live what-if spread is the spread gate; the rejection keeps provenance."""
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    signal_id = formed.entry_signal_ids[0]
    conn.execute(
        "UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (NOW - timedelta(hours=1), formed.book[0])
    )
    conn.commit()
    broker = _stub_broker(formed.book[0])
    costs = broker.get_what_if_costs.return_value
    # 0.5 on a 40 ticket = 1.25%, over the 1% bound.
    broker.get_what_if_costs.return_value = replace(costs, costs=(replace(costs.costs[0], value=Decimal("0.5")),))
    result = mx.execute_momentum_signal(
        conn,
        broker=broker,
        signal_id=signal_id,
        clock=lambda: NOW,
        latest_bar=lambda _i: _bar(NOW - timedelta(seconds=30), "120"),
    )
    assert (result.verdict, result.reason_code) == ("rejected", "live_spread_wide")
    broker.place_demo_strategy_order.assert_not_called()
    row = conn.execute(
        "SELECT price_reference, quote_ask FROM strategy_entry_preflights WHERE signal_id = %s", (signal_id,)
    ).fetchone()
    conn.commit()
    assert row == ("candle_close", Decimal("120"))


def test_a_candle_that_opens_after_evaluation_began_is_judged_on_arrival(ebull_test_conn: Conn) -> None:
    """The candle request can cross a minute boundary; its new bar must not read as 'in the future'."""
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    conn.execute(
        "UPDATE quotes SET quoted_at = %s WHERE instrument_id = %s", (NOW - timedelta(hours=1), formed.book[0])
    )
    conn.commit()
    arrived = NOW + timedelta(seconds=70)
    broker = _stub_broker(formed.book[0])
    broker.get_what_if_costs.return_value = replace(broker.get_what_if_costs.return_value, last_updated=arrived)
    reads = iter([NOW])
    result = mx.execute_momentum_signal(
        conn,
        broker=broker,
        signal_id=formed.entry_signal_ids[0],
        clock=lambda: next(reads, arrived),
        latest_bar=lambda _i: _bar(NOW + timedelta(seconds=60), "120"),
    )
    assert result.verdict == "submitted", result


def test_a_fresh_quote_is_used_and_no_candle_is_read(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    _market(conn, formed.book)
    signal_id = formed.entry_signal_ids[0]
    latest_bar = MagicMock()
    result = mx.execute_momentum_signal(
        conn, broker=_stub_broker(formed.book[0]), signal_id=signal_id, clock=lambda: NOW, latest_bar=latest_bar
    )
    assert result.verdict == "submitted", result
    latest_bar.assert_not_called()
    row = conn.execute(
        "SELECT price_reference FROM strategy_entry_preflights WHERE signal_id = %s", (signal_id,)
    ).fetchone()
    conn.commit()
    assert row == ("quote",)


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
        mx.execute_momentum_signal(conn, broker=_stub_broker(1), signal_id=int(other[0]), clock=lambda: NOW)


def test_null_joins_refuse_in_order_without_raising(ebull_test_conn: Conn) -> None:
    """No deployment, policy, pool, quote or halt feed: every LEFT JOIN is NULL, and the loader returns the first
    ordered refusal rather than raising (review WARNING on #3825). Supplying each join in turn moves the refusal to
    the next one in `load_intent`'s chain, until the intent authorises (#3827)."""
    conn = ebull_test_conn
    _seed(conn, held=False)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    conn.commit()
    signal_id = formed.entry_signal_ids[0]
    deployment: list[int] = []
    steps: list[tuple[str, Any]] = [
        ("paper_deployment_missing", lambda: deployment.append(_deployment(conn))),
        ("paper_pool_unconfigured", lambda: _pool(conn)),
        ("execution_policy_missing", lambda: _policy(conn, deployment[0], fixed_ticket=None)),
        ("momentum_ticket_unconfigured", lambda: _policy(conn, deployment[0])),
        ("quote_missing", lambda: _quotes(conn, formed.book)),
        ("halt_feed_missing", lambda: _halt_feed(conn)),
    ]
    for expected, supply in steps:
        intent, reason, halt_ran = mi.load_intent(conn, signal_id=signal_id, now=NOW)
        assert (intent, reason, halt_ran) == (None, expected, True)
        supply()
        conn.commit()
    intent, reason, _ = mi.load_intent(conn, signal_id=signal_id, now=NOW)
    assert reason is None and intent is not None and intent.signal_id == signal_id
