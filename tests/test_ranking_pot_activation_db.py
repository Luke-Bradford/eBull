"""#2842 slice 5c-ii-b — activation and resumption against real Postgres (spec §7.3, ``sql/452``)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services import ranking_pot_activation as act
from app.services.strategy_control_plane import configure_paper_pool
from app.services.strategy_position_manager import configure_position_manager
from tests.test_ranking_pot_schema_db import POT, _frozen, _move

# #3610: freezes a claim with no TrialDesign while testing something else (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("assume_trial_powered")

Conn = psycopg.Connection[Any]

NOW = datetime(2026, 10, 2, 15, 30, tzinfo=UTC)


def _risk(cash: str = "5100") -> BrokerAccountRiskSnapshot:
    return BrokerAccountRiskSnapshot(
        available_cash=Decimal(cash),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("40000"),
        instrument_investments=(),
        observed_at=NOW,
        account_currency_id=1,
        raw_payload={},
    )


def _setup(conn: Conn, minima: dict[int, Decimal | None] | None = None) -> int:
    decl = _frozen(conn)
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("40000"),
        risk_profile="growth",
        approval_mode="manual",
        max_concurrent_positions_override=30,
        changed_by="test",
        reason="#2842 activation fixture",
    )
    snapshot = conn.execute(
        "INSERT INTO etoro_perishable_snapshots (started_at, finished_at, status, recorder_version, request_params) "
        "VALUES (%s, %s, 'complete', 't', '{}') RETURNING snapshot_id",
        (NOW, NOW),
    ).fetchone()
    assert snapshot is not None
    request = conn.execute(
        "INSERT INTO etoro_perishable_requests (snapshot_id, phase, seq, instrument_ids, request_body, observed_at, "
        "outcome, http_status, raw) VALUES (%s, 'eligibility', 0, ARRAY[2842, 2843, 2844], '{}', %s, 'ok', 200, '{}') "
        "RETURNING request_id",
        (snapshot[0], NOW),
    ).fetchone()
    assert request is not None
    for iid, minimum in (minima or {2842: Decimal("10"), 2843: Decimal("50"), 2844: None}).items():
        found = minimum is not None
        conn.execute(
            "INSERT INTO etoro_eligibility_observations (snapshot_id, instrument_id, request_id, observed_at, answer, "
            "allow_open_position, min_position_exposure) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (snapshot[0], iid, request[0], NOW, "found" if found else "not_found", True if found else None, minimum),
        )
    conn.commit()
    return decl


def _activate(conn: Conn, capital: str | None, *, apply: bool = True, cash: str = "5100") -> act.ActivationReport:
    conn.commit()  # the helpers' reads leave a transaction open; activation needs an idle connection
    return act.activate_pot(
        conn,
        fetch_risk=lambda: _risk(cash),
        pot_capital=None if capital is None else Decimal(capital),
        declared_by="supervisor-test",
        apply=apply,
        now=lambda: NOW,
    )


def _state(conn: Conn, decl: int) -> str:
    row = conn.execute(
        "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (decl,),
    ).fetchone()
    assert row is not None
    return str(row[0])


def _deployments(conn: Conn) -> int:
    row = conn.execute("SELECT count(*) FROM strategy_deployments WHERE strategy_id = %s", (POT,)).fetchone()
    assert row is not None
    return int(row[0])


def test_activation_previews_refuses_then_writes_everything(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _setup(conn)

    # Dry run without a capital: the preview is printed (cash binds: 5,100 → 5,000), nothing is written.
    dry = _activate(conn, None, apply=False)
    assert dry.refusals == ("pot_capital_not_entered",)
    assert dry.preview is not None and (dry.preview.binding_term, dry.preview.preview) == ("available_cash", 5000)
    assert dry.minimum == Decimal("50")  # the not_found name is skipped
    assert (_state(conn, decl), _deployments(conn)) == ("shadow_only", 0)

    assert _activate(conn, "4500").refusals == ("pot_capital_not_preview",)
    assert _activate(conn, "5500").refusals == ("pot_capital_unavailable:available_cash",)
    clean_dry = _activate(conn, "5000", apply=False)
    assert (clean_dry.refusals, clean_dry.applied) == ((), False)
    assert (_state(conn, decl), _deployments(conn)) == ("shadow_only", 0)

    applied = _activate(conn, "5000")
    assert (applied.refusals, applied.applied, applied.mode) == ((), True, "activation")
    assert _state(conn, decl) == "executing"
    row = conn.execute(
        "SELECT a.pot_capital, a.detail ->> 'binding_term', d.capital_limit, d.enabled, p.stop_loss_pct, "
        "p.max_ticket_amount, p.fixed_ticket_amount, m.max_position_age_seconds "
        "FROM ranking_pot_activations a, strategy_deployments d "
        "JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id "
        "JOIN strategy_position_manager_policies m ON m.deployment_id = d.deployment_id "
        "WHERE a.declaration_id = %s AND d.strategy_id = %s",
        (decl, POT),
    ).fetchone()
    assert row is not None
    assert (Decimal(row[0]), row[1], Decimal(row[2]), row[3]) == (
        Decimal("5000"),
        "available_cash",
        Decimal("5000"),
        True,
    )
    assert (Decimal(row[4]), Decimal(row[5]), Decimal(row[6]), row[7]) == (
        Decimal("25"),
        Decimal("5000"),
        Decimal("200"),
        None,
    )

    # A retry after an unknown commit reports, and writes nothing.
    again = _activate(conn, "5000")
    assert (again.mode, again.refusals, again.applied) == ("already_active", (), False)


def test_ticket_below_the_largest_open_minimum_refuses(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _setup(conn, {2842: Decimal("10"), 2843: Decimal("500"), 2844: Decimal("10")})
    assert "pot_ticket_below_minimum" in _activate(conn, "5000").refusals  # 5,000 / 25 = 200 < 500


def test_a_missing_eligibility_row_fails_closed(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _setup(conn, {2842: Decimal("10"), 2843: Decimal("10")})
    assert act.MINIMUM_UNOBSERVED in _activate(conn, "5000").refusals


def test_resumption_checks_configuration_and_writes_only_the_event(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _setup(conn)
    assert _activate(conn, "5000").applied
    _move(conn, decl, "executing", "halted_operator", "operator")

    assert _activate(conn, "4500").refusals == ("pot_capital_fixed",)
    resumed = _activate(conn, None, cash="1")  # no preview on resumption: cash does not matter
    assert (resumed.mode, resumed.refusals, resumed.applied) == ("resumption", (), True)
    assert _state(conn, decl) == "executing"

    _move(conn, decl, "executing", "halted_operator", "operator")
    deployment = conn.execute(
        "SELECT deployment_id FROM strategy_deployments WHERE strategy_id = %s", (POT,)
    ).fetchone()
    assert deployment is not None
    configure_position_manager(
        conn,
        deployment_id=int(deployment[0]),
        max_position_age_seconds=60,
        ratchet_variant_id=None,
        updated_by="test",
        reason="drift",
    )
    conn.commit()
    assert _activate(conn, None).refusals == ("pot_configuration_drift",)
    assert _state(conn, decl) == "halted_operator"


def test_executing_without_an_activation_row_is_refused_by_the_database(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)
    with pytest.raises(psycopg.errors.RaiseException, match="not_activated"):
        conn.execute(
            "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
            "VALUES (%s, 'shadow_only', 'executing', 't', 'supervisor')",
            (decl,),
        )
    conn.rollback()
