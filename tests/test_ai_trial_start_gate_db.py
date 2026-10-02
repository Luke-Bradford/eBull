"""#3471 slice 2c-i — the step-0 preview's read-only DB assembler against real Postgres."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services import ai_trial_start_gate
from app.services.ai_trial_start_gate import TRIAL_POOL_MAX_CONCURRENT, preview_trial_capacity
from app.services.strategy_control_plane import configure_paper_pool
from app.services.strategy_engine_capital import EngineCapitalObservationError
from tests.test_ai_trial_intent_db import NOW, _published_pair

Conn = psycopg.Connection[Any]

RISK = BrokerAccountRiskSnapshot(
    available_cash=Decimal("5000"),
    total_invested=Decimal("0"),
    unrealized_pnl=Decimal("0"),
    equity=Decimal("100000"),
    instrument_investments=(),
    observed_at=NOW,
    account_currency_id=1,
    raw_payload={},
)


def test_the_preview_reads_both_legs_and_writes_nothing(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    declaration_id, _ = _published_pair(conn)
    # The fixture pool: $2,000 balanced, concurrency cap 8 < TRIAL_POOL_MAX_CONCURRENT.
    below_design = "trial_capacity_unavailable:trial_pool_concurrency_below_design"
    assert preview_trial_capacity(conn, declaration_id=declaration_id, risk=RISK, now=NOW) == below_design

    def growth(override: int | None) -> None:
        configure_paper_pool(
            conn,
            enabled=True,
            capital_limit=Decimal("10000"),
            risk_profile="growth",
            approval_mode="manual",
            max_concurrent_positions_override=override,
            changed_by="test",
            reason="#3471 step-0 preview",
        )
        conn.commit()

    # `growth` alone caps at 12; the demo pool's override (sql/440) is what the preview reads.
    growth(None)
    assert preview_trial_capacity(conn, declaration_id=declaration_id, risk=RISK, now=NOW) == below_design
    growth(TRIAL_POOL_MAX_CONCURRENT)
    assert preview_trial_capacity(conn, declaration_id=declaration_id, risk=RISK, now=NOW) is None
    # Read-only: the engine-pot high-water state `_observe_local_mandate_risk` advances is untouched.
    assert conn.execute("SELECT count(*) FROM strategy_engine_pot_risk_state").fetchone() == (0,)
    conn.commit()
    # A declaration with no deployed legs refuses rather than previewing half a pair.
    assert (
        preview_trial_capacity(conn, declaration_id=declaration_id + 999, risk=RISK, now=NOW)
        == "trial_capacity_unavailable:paper_deployment_missing"
    )


def test_the_preview_refuses_a_busy_connection_and_leaves_it_idle(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    declaration_id, _ = _published_pair(conn)
    conn.execute("SELECT 1")  # a caller's open transaction is never the preview's to end
    with pytest.raises(ValueError, match="idle connection"):
        preview_trial_capacity(conn, declaration_id=declaration_id, risk=RISK, now=NOW)
    conn.rollback()
    preview_trial_capacity(conn, declaration_id=declaration_id, risk=RISK, now=NOW)
    assert conn.info.transaction_status == TransactionStatus.IDLE


def test_a_refusing_capital_observation_is_a_named_refusal(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    declaration_id, _ = _published_pair(conn)

    def refuse(_conn: Conn) -> None:
        raise EngineCapitalObservationError("test", "engine_capital_population_incomplete")

    monkeypatch.setattr(ai_trial_start_gate, "load_engine_capital_authority", refuse)
    assert (
        preview_trial_capacity(conn, declaration_id=declaration_id, risk=RISK, now=NOW)
        == "trial_capacity_unavailable:engine_capital_population_incomplete"
    )
