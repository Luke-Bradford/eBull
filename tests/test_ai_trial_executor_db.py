"""#3471 slice 2b-ii — the trial executor against real Postgres (spec §8 "Sizing", "Cost cap",
"Protective levels", "Demo boundary").

One published pair (``test_ai_trial_intent_db._published_pair``), a stub broker, and the trial
leg sized, priced, protected and submitted through the paper path's durable machinery.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import psycopg
import pytest

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerCostComponent,
    BrokerEligibilityResponse,
    BrokerInstrumentEligibility,
    BrokerLeverageConfig,
    BrokerOrderSubmission,
    BrokerOrderSubmissionUncertain,
    BrokerProvider,
    BrokerWhatIfCostResponse,
)
from app.services import ai_trial_executor
from app.services.ai_trial_executor import TRIAL_ALLOCATED_REASON, _open_leg_trades, execute_trial_signal
from app.services.ai_trial_intent import TRIAL_CAPITAL_MODE, load_trial_intent
from app.services.strategy_control_plane import configure_paper_pool
from app.services.strategy_paper_executor import COST_BASIS_BROKER_PREFLIGHT_VALUE, StrategyPaperExecutionError
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_ai_trial_intent_db import ARM_INSTRUMENT, NOW, _published_pair, _signal

Conn = psycopg.Connection[Any]
_REQUEST_ID = UUID("5a4f0e7e-3471-4b2b-9d00-2b2b2b2b2b2b")


def _enable_trading(conn: Conn, *, kill: bool = False) -> None:
    conn.execute(
        """
        INSERT INTO runtime_config (id, enable_auto_trading, enable_live_trading, updated_by, reason)
        VALUES (true, true, false, 'test', '#3471 trial executor fixture')
        ON CONFLICT (id) DO UPDATE SET enable_auto_trading=true, updated_at=now()
        """
    )
    conn.execute(
        "INSERT INTO kill_switch (id, is_active) VALUES (true, %s) "
        "ON CONFLICT (id) DO UPDATE SET is_active=EXCLUDED.is_active",
        (kill,),
    )
    conn.commit()


def _broker(instrument_id: int, *, equity: str = "1000", spread_value: str = "0.5") -> MagicMock:
    broker = MagicMock(spec=BrokerProvider)
    broker.get_account_risk_snapshot.return_value = BrokerAccountRiskSnapshot(
        available_cash=Decimal("600"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal(equity),
        instrument_investments=(),
        observed_at=NOW,
        account_currency_id=1,
        raw_payload={},
    )
    broker.check_instrument_eligibility.return_value = BrokerEligibilityResponse(
        currency="USD",
        eligibilities=(
            BrokerInstrumentEligibility(
                instrument_id=instrument_id,
                symbol=f"AIT{instrument_id}",
                min_position_exposure=Decimal("10"),
                max_units_per_order=None,
                allow_open_position=True,
                allow_close_position=True,
                allow_partial_close_position=True,
                allow_trailing_stop_loss=False,
                leverage_configs=(
                    BrokerLeverageConfig(
                        settlement_type="real",
                        direction="LONG",
                        leverage_values=(1,),
                        min_position_amount=Decimal("10"),
                        allow_edit_stop_loss=True,
                        allow_edit_take_profit=True,
                        allow_stop_loss_take_profit=True,
                        raw_payload={},
                    ),
                ),
                raw_payload={},
            ),
        ),
        not_found_instrument_ids=(),
        not_found_symbols=(),
        raw_payload={},
    )
    broker.get_what_if_costs.return_value = BrokerWhatIfCostResponse(
        instrument_id=instrument_id,
        symbol=f"AIT{instrument_id}",
        costs=(
            BrokerCostComponent(
                cost_type="marketSpread", amount=None, value=Decimal(spread_value), currency="USD", raw_payload={}
            ),
        ),
        last_updated=NOW,
        raw_payload={},
    )
    broker.place_demo_strategy_order.return_value = BrokerOrderSubmission(
        broker_order_ref="3471001",
        reference_id=_REQUEST_ID,
        token=UUID("066faaee-e1e9-49d2-a568-c6e1cc336ad8"),
    )
    return broker


def _control_instrument(conn: Conn, signal_id: int) -> int:
    row = conn.execute("SELECT instrument_id FROM strategy_signals WHERE signal_id=%s", (signal_id,)).fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def test_the_arm_leg_submits_its_ticket_with_its_own_levels_and_no_evidence(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, signals = _published_pair(conn)
    _enable_trading(conn)
    broker = _broker(ARM_INSTRUMENT)
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)

    result = execute_trial_signal(conn, broker=broker, signal_id=signals["arm"], now=NOW)

    assert (result.verdict, result.reason_code, result.amount) == ("submitted", "broker_accepted", Decimal("125"))
    body = broker.place_demo_strategy_order.call_args.args[0]
    # The decision's 8% stop / 16% target off the 100 ask; unlevered underlying stock.
    assert (body.amount, body.stop_loss_rate, body.take_profit_rate, body.settlement_type) == (
        Decimal("125"),
        Decimal("92.000000"),
        Decimal("116.000000"),
        "real",
    )
    assert broker.place_demo_strategy_order.call_args.kwargs["request_id"] == _REQUEST_ID
    preflight = conn.execute(
        "SELECT verdict, reason_code, allocated_amount, forecast_id, ranking_member_id, scan_at, "
        "gross_expectancy_ci_low_pct, net_expectancy_pct, stressed_cost_amount, cost_basis, quote_ask "
        "FROM strategy_entry_preflights WHERE signal_id=%s",
        (signals["arm"],),
    ).fetchone()
    assert preflight == (
        "allocated",
        TRIAL_ALLOCATED_REASON,
        Decimal("125.000000"),
        None,
        None,
        None,
        None,
        None,
        Decimal("1.000000"),  # 0.5 spread x the policy's 2x stress
        COST_BASIS_BROKER_PREFLIGHT_VALUE,
        Decimal("100.000000"),  # the ask the 92/116 levels were derived from (sql/438)
    )
    link = conn.execute(
        "SELECT l.leg, l.requested_amount, fd.amount FROM ai_trial_trade_links l "
        "JOIN strategy_trades t ON t.strategy_trade_id = l.strategy_trade_id "
        "JOIN strategy_funding_decisions fd ON fd.funding_decision_id = t.funding_decision_id "
        "WHERE l.strategy_trade_id = %s",
        (result.strategy_trade_id,),
    ).fetchone()
    assert link == ("arm", Decimal("125.000000"), Decimal("125.000000"))
    assert _open_leg_trades(conn, _deployment_of(conn, signals["arm"])) == 1

    # A retry is read-only: one funding decision per signal, one broker write.
    again = execute_trial_signal(conn, broker=broker, signal_id=signals["arm"], now=NOW)
    assert again.order_id == result.order_id
    broker.place_demo_strategy_order.assert_called_once()


def _deployment_of(conn: Conn, signal_id: int) -> int:
    row = conn.execute(
        "SELECT d.deployment_id FROM strategy_signals s JOIN strategy_deployments d "
        "ON d.strategy_id = s.strategy_id AND d.strategy_version = s.strategy_version AND d.mode = 'paper' "
        "WHERE s.signal_id = %s",
        (signal_id,),
    ).fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def test_a_capacity_reduced_control_leg_records_requested_and_actual(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _, signals = _published_pair(conn)
    _enable_trading(conn)
    control_instrument = _control_instrument(conn, signals["control"])
    # The 30% instrument-exposure cap of $200 equity = $60 of capacity: below the $125 ticket,
    # above the broker's $10 open minimum, so the leg is accepted REDUCED (§8 "Sizing").
    broker = _broker(control_instrument, equity="200", spread_value="0.2")

    result = execute_trial_signal(conn, broker=broker, signal_id=signals["control"], now=NOW)

    assert (result.verdict, result.amount) == ("submitted", Decimal("60.00"))
    body = broker.place_demo_strategy_order.call_args.args[0]
    # The control's own ATR-derived 4% / 8% levels, not the arm's 8% / 16%.
    assert (body.stop_loss_rate, body.take_profit_rate) == (Decimal("96.000000"), Decimal("108.000000"))
    assert conn.execute(
        "SELECT leg, requested_amount FROM ai_trial_trade_links WHERE strategy_trade_id = %s",
        (result.strategy_trade_id,),
    ).fetchone() == ("control", Decimal("125.000000"))


def test_cost_cap_slot_cap_and_kill_switch_refuse_before_any_broker_write(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, signals = _published_pair(conn)
    _enable_trading(conn)
    # 0.63 x 2 stress = 1.26 > 1.0% of $125.
    expensive = _broker(ARM_INSTRUMENT, spread_value="0.63")
    refused = execute_trial_signal(conn, broker=expensive, signal_id=signals["arm"], now=NOW)
    assert (refused.verdict, refused.reason_code) == ("rejected", "trial_cost_cap")
    expensive.place_demo_strategy_order.assert_not_called()
    assert conn.execute(
        "SELECT verdict, reason_code, forecast_id, gross_expectancy_ci_low_pct, quote_ask "
        "FROM strategy_entry_preflights WHERE signal_id = %s",
        (signals["arm"],),
    ).fetchone() == ("rejected", "trial_cost_cap", None, None, Decimal("100.000000"))
    conn.commit()

    control_instrument = _control_instrument(conn, signals["control"])
    monkeypatch.setattr(ai_trial_executor, "TRIAL_MAX_CONCURRENT_PER_LEG", 0)
    full = _broker(control_instrument)
    slots = execute_trial_signal(conn, broker=full, signal_id=signals["control"], now=NOW)
    assert slots.reason_code == "trial_leg_slots_full"
    full.get_account_risk_snapshot.assert_not_called()


def test_the_kill_switch_refuses_and_an_uncertain_leg_resumes_on_its_committed_uuid(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, signals = _published_pair(conn)
    _enable_trading(conn, kill=True)
    killed = _broker(ARM_INSTRUMENT)
    assert execute_trial_signal(conn, broker=killed, signal_id=signals["arm"], now=NOW).reason_code == (
        "kill_switch_active_or_missing"
    )
    killed.get_account_risk_snapshot.assert_not_called()

    _enable_trading(conn)
    control_instrument = _control_instrument(conn, signals["control"])
    broker = _broker(control_instrument)
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", lambda: _REQUEST_ID)
    broker.place_demo_strategy_order.side_effect = BrokerOrderSubmissionUncertain("timeout")
    first = execute_trial_signal(conn, broker=broker, signal_id=signals["control"], now=NOW)
    assert first.verdict == "submission_uncertain"

    broker.place_demo_strategy_order.side_effect = None
    resumed = execute_trial_signal(conn, broker=broker, signal_id=signals["control"], now=NOW)
    assert (resumed.verdict, resumed.order_id) == ("submitted", first.order_id)
    retry = broker.place_demo_strategy_order.call_args
    assert retry.kwargs["request_id"] == _REQUEST_ID
    assert (retry.args[0].stop_loss_rate, retry.args[0].take_profit_rate) == (
        Decimal("96.000000"),
        Decimal("108.000000"),
    )


def test_a_non_trial_signal_is_raised_not_persisted(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _published_pair(conn)
    _enable_trading(conn)
    other = _signal(conn, "S-NOT-A-TRIAL", "v1", ARM_INSTRUMENT)
    conn.commit()
    with pytest.raises(StrategyPaperExecutionError, match="not a demo_trial signal"):
        execute_trial_signal(conn, broker=_broker(ARM_INSTRUMENT), signal_id=other, now=NOW)
    # A funding decision is one per signal: a refusal written here would pre-empt its owner.
    assert conn.execute(
        "SELECT count(*) FROM strategy_funding_decisions WHERE signal_id = %s", (other,)
    ).fetchone() == (0,)


def test_a_halt_landing_during_the_broker_round_trips_refuses_before_authority(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    declaration_id, signals = _published_pair(conn)
    _enable_trading(conn)
    broker = _broker(ARM_INSTRUMENT)
    priced = broker.get_what_if_costs.return_value

    def halt_then_price(_order: object) -> BrokerWhatIfCostResponse:
        # The loader already saw `active`; the operator halts while the broker is priced.
        with psycopg.connect(test_database_url()) as other:
            other.execute(
                "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
                "VALUES (%s, 'active', 'halted_operator', 'test', 'operator')",
                (declaration_id,),
            )
        return priced

    broker.get_what_if_costs.side_effect = halt_then_price
    result = execute_trial_signal(conn, broker=broker, signal_id=signals["arm"], now=NOW)

    assert (result.verdict, result.reason_code) == ("rejected", "trial_not_active")
    broker.place_demo_strategy_order.assert_not_called()
    assert conn.execute("SELECT count(*) FROM ai_trial_trade_links").fetchone() == (0,)


def test_legs_with_different_effective_policies_refuse(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _, signals = _published_pair(conn)
    _enable_trading(conn)
    # O9: only the control's policy moves; the arm leg may not trade against a different pair
    # of terms. The revision and audit columns are bookkeeping and do not count.
    conn.execute(
        "UPDATE strategy_execution_policies p SET cost_stress_multiplier = 3 FROM strategy_deployments d "
        "WHERE d.deployment_id = p.deployment_id AND d.strategy_id = 'ai-discretionary-v1-control'"
    )
    conn.commit()
    broker = _broker(ARM_INSTRUMENT, spread_value="0.1")
    result = execute_trial_signal(conn, broker=broker, signal_id=signals["arm"], now=NOW)
    assert result.reason_code == "trial_policy_parity"
    broker.place_demo_strategy_order.assert_not_called()


def test_a_compounding_pool_does_not_make_a_leg_compound(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _, signals = _published_pair(conn)
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("2000"),
        capital_mode="compound",
        risk_profile="balanced",
        approval_mode="manual",
        changed_by="test",
        reason="#3471 compound pool",
    )
    conn.commit()
    intent, reason, _ = load_trial_intent(conn, signal_id=signals["arm"], now=NOW)
    conn.commit()
    assert reason is None and intent is not None
    assert intent.capital_mode == TRIAL_CAPITAL_MODE == "fixed"
