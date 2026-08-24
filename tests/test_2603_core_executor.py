"""#2603 attended core executor orchestration; broker and DB are deterministic doubles."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerCoreOrderSubmission,
    BrokerOrderNotFound,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
)
from app.services.broker_credentials import CredentialInUse, revoke_credential
from app.services.strategy_core_executor import (
    CoreExecutionResult,
    CoreResumeAuthority,
    _observe_core_portfolio_drawdown,
    execute_core_rebalance,
    resume_core_submission,
)

OPERATOR = UUID("73d8ad78-3062-4ef5-8f0a-7428865e23d7")
API_CREDENTIAL = UUID("ba39f751-d4bd-4553-ab25-d9acbb73fbe8")
USER_CREDENTIAL = UUID("f7306e0b-9494-415e-85fd-97874510cc83")


class FakeResult:
    def __init__(self, row: tuple[int] | None = None) -> None:
        self._row = row

    def fetchone(self) -> tuple[int] | None:
        return self._row


class FakeConn:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)

    def transaction(self) -> nullcontext[None]:
        return nullcontext()

    def commit(self) -> None:
        return None

    def execute(self, sql: str, _params: object = None) -> FakeResult:
        normalized = " ".join(sql.split())
        if normalized.startswith("INSERT INTO strategy_trades"):
            self.events.append("persist_trade")
            return FakeResult((21,))
        if normalized.startswith("INSERT INTO orders"):
            self.events.append("persist_order")
            return FakeResult((31,))
        if normalized.startswith("INSERT INTO strategy_order_reconciliation_state"):
            self.events.append("persist_reconciliation")
        if normalized.startswith("UPDATE orders SET broker_order_ref"):
            self.events.append("persist_acceptance")
        if "state='rejected'" in normalized:
            self.events.append("persist_rejection")
        if "state='error'" in normalized:
            self.events.append("persist_uncertainty")
        return FakeResult()


class FakeBroker:
    def __init__(self, events: list[str], submission: object) -> None:
        self.events = events
        self.submission = submission

    def get_account_risk_snapshot(self) -> object:
        self.events.append("read_snapshot")
        return SimpleNamespace(
            available_cash=Decimal("500"),
            equity=Decimal("1000"),
            observed_at=datetime.now(UTC),
        )

    def place_demo_core_order(self, _order: object, *, request_id: UUID) -> BrokerCoreOrderSubmission:
        assert request_id is not None
        assert "persist_order" in self.events
        assert "persist_reconciliation" in self.events
        self.events.append("broker_submit")
        if isinstance(self.submission, Exception):
            raise self.submission
        assert isinstance(self.submission, BrokerCoreOrderSubmission)
        return self.submission


@contextmanager
def _tracking_lock(events: list[str]):
    events.append("lock_enter")
    try:
        yield
    finally:
        events.append("lock_exit")


def _decision(action: str = "buy_core") -> SimpleNamespace:
    return SimpleNamespace(action=action, reason_code="within_band" if action == "hold" else None)


def _run(
    submission: object,
    *,
    action: str = "buy_core",
    snapshot_observed_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
    binding_proof_id: int = 7,
    drawdown_refusal: str | None = None,
) -> tuple[CoreExecutionResult, list[str]]:
    events: list[str] = []
    conn = FakeConn(events)
    mandate = SimpleNamespace(event_id=1, enabled=True, core_instrument_id=3417)
    proof = SimpleNamespace(
        proof_id=7,
        api_key_credential_id=API_CREDENTIAL,
        user_key_credential_id=USER_CREDENTIAL,
        response_currency="USD",
        min_position_exposure=Decimal("10"),
        min_position_amount=Decimal("10"),
    )
    binding_proof = SimpleNamespace(**{**proof.__dict__, "proof_id": binding_proof_id})
    decision = _decision(action)
    intent = SimpleNamespace(core_rebalance_intent_id=11, decision=decision)
    admission = SimpleNamespace(admitted=True, eligibility_proof_id=7, reason_code=None)
    db_preflight = SimpleNamespace(admitted=True, reason_code=None)
    broker_preflight = SimpleNamespace(
        admitted=True,
        reason_code=None,
        amount=Decimal("49.9"),
        snapshot_observed_at=snapshot_observed_at or datetime.now(UTC),
        max_account_risk_age_seconds=30,
        account_equity=Decimal("1000"),
    )
    capital_authority = SimpleNamespace(enabled=True, pool_event_id=5)
    paper_pool = SimpleNamespace(
        event_id=5,
        mandate=SimpleNamespace(max_portfolio_drawdown_pct=Decimal("15")),
    )
    capital_usage = SimpleNamespace(
        core_market_value=Decimal("500"),
        headroom=SimpleNamespace(within_bound=True, remaining=Decimal("500")),
    )

    def observe_drawdown(*_args: object, **_kwargs: object) -> str | None:
        events.append("drawdown_observation")
        return drawdown_refusal

    with (
        patch("app.services.strategy_core_executor.load_core_mandate", return_value=mandate),
        patch("app.services.strategy_core_executor.load_engine_capital_authority", return_value=capital_authority),
        patch("app.services.strategy_core_executor.load_paper_pool", return_value=paper_pool),
        patch("app.services.strategy_core_executor.resolve_engine_capital_usage", return_value=capital_usage),
        patch("app.services.strategy_core_executor.require_selected_core_instrument"),
        patch("app.services.strategy_core_executor.require_core_eligibility", side_effect=[proof, binding_proof]),
        patch("app.services.strategy_core_executor.observe_core_sleeve", return_value=object()),
        patch("app.services.strategy_core_executor.evaluate_core_rebalance", return_value=decision),
        patch("app.services.strategy_core_executor.assess_core_broker_preflight", return_value=broker_preflight),
        patch("app.services.strategy_core_executor.core_submission_lock", return_value=_tracking_lock(events)),
        patch("app.services.strategy_core_executor.record_core_rebalance_intent", return_value=intent),
        patch("app.services.strategy_core_executor.admit_core_rebalance_intent", return_value=admission),
        patch("app.services.strategy_core_executor.preflight_core_submission", return_value=db_preflight),
        patch(
            "app.services.strategy_core_executor._observe_core_portfolio_drawdown",
            side_effect=observe_drawdown,
        ),
        patch(
            "app.services.strategy_core_executor.link_strategy_order",
            side_effect=lambda *_a, **_k: events.append("link"),
        ),
    ):
        kwargs = {} if clock is None else {"clock": clock}
        result = execute_core_rebalance(
            conn,  # type: ignore[arg-type]
            broker=FakeBroker(events, submission),  # type: ignore[arg-type]
            operator_id=OPERATOR,
            api_key_credential_id=API_CREDENTIAL,
            user_key_credential_id=USER_CREDENTIAL,
            recorded_by="operator",
            **kwargs,  # type: ignore[arg-type]
        )
    return result, events


def test_core_drawdown_observation_refuses_at_the_portfolio_limit() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (
        Decimal("1000"),
        datetime(2026, 8, 24, 11, 59, tzinfo=UTC),
    )

    refusal = _observe_core_portfolio_drawdown(
        conn,
        equity=Decimal("850"),
        observed_at=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
        max_drawdown_pct=Decimal("15"),
    )

    assert refusal == "portfolio_drawdown_limit"
    assert any("INSERT INTO strategy_paper_account_risk_state" in call.args[0] for call in conn.execute.call_args_list)


def test_core_drawdown_observation_refuses_an_older_broker_snapshot() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (
        Decimal("1000"),
        datetime(2026, 8, 24, 12, 1, tzinfo=UTC),
    )

    refusal = _observe_core_portfolio_drawdown(
        conn,
        equity=Decimal("999"),
        observed_at=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
        max_drawdown_pct=Decimal("15"),
    )

    assert refusal == "core_account_risk_stale"
    assert conn.execute.call_count == 1


def test_core_drawdown_refusal_precedes_durable_order_authority() -> None:
    result, events = _run(
        AssertionError("must not submit"),
        drawdown_refusal="portfolio_drawdown_limit",
    )

    assert result.state == "refused"
    assert result.reason_code == "portfolio_drawdown_limit"
    assert "persist_order" not in events
    assert "broker_submit" not in events


def test_hold_advances_the_shared_drawdown_high_water() -> None:
    result, events = _run(AssertionError("must not submit"), action="hold")

    assert result.state == "held"
    assert events.count("drawdown_observation") == 1
    assert "persist_order" not in events


def test_acceptance_identity_is_persisted_after_authority_commits() -> None:
    result, events = _run(
        BrokerCoreOrderSubmission(
            broker_order_ref="9001",
            reference_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
            response_digest="a" * 64,
        )
    )

    assert result.state == "submitted"
    assert result.reason_code == "broker_accepted_pending_reconciliation"
    assert events.index("persist_order") < events.index("broker_submit") < events.index("persist_acceptance")
    assert events.index("lock_enter") < events.index("broker_submit") < events.index("lock_exit")


@pytest.mark.parametrize(
    ("error", "state", "evidence_event"),
    [
        (BrokerOrderSubmissionError("rejected"), "refused", "persist_rejection"),
        (BrokerOrderSubmissionUncertain("timeout"), "submission_uncertain", "persist_uncertainty"),
    ],
)
def test_rejection_and_uncertainty_have_distinct_durable_outcomes(
    error: Exception,
    state: str,
    evidence_event: str,
) -> None:
    result, events = _run(error)

    assert result.state == state
    assert evidence_event in events


def test_hold_records_an_intent_without_creating_or_submitting_an_order() -> None:
    result, events = _run(AssertionError("must not submit"), action="hold")

    assert result.state == "held"
    assert result.intent_id == 11
    assert "persist_order" not in events
    assert "broker_submit" not in events


def test_snapshot_that_expires_while_waiting_for_the_lock_is_not_submitted() -> None:
    observed = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    result, events = _run(
        AssertionError("must not submit"),
        snapshot_observed_at=observed,
        clock=lambda: datetime(2026, 8, 24, 12, 1, tzinfo=UTC),
    )

    assert result.state == "refused"
    assert result.reason_code == "core_account_risk_stale"
    assert "broker_submit" not in events


def test_eligibility_refresh_during_broker_preflight_is_not_submitted() -> None:
    result, events = _run(AssertionError("must not submit"), binding_proof_id=8)

    assert result.state == "refused"
    assert result.reason_code == "core_credential_provenance_changed"
    assert "broker_submit" not in events


def test_credential_revocation_takes_the_core_mutation_lock() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = (False,)
    cursor.rowcount = 1
    conn.cursor.return_value.__enter__.return_value = cursor

    revoke_credential(conn, credential_id=API_CREDENTIAL, operator_id=OPERATOR)

    lock_call = conn.execute.call_args_list[0]
    assert "pg_advisory_xact_lock" in lock_call.args[0]
    assert lock_call.args[1] == (2603, 3)


def test_credential_rotation_is_refused_while_core_reconciliation_needs_it() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = (True,)
    conn.cursor.return_value.__enter__.return_value = cursor

    with pytest.raises(CredentialInUse, match="unresolved core order"):
        revoke_credential(conn, credential_id=API_CREDENTIAL, operator_id=OPERATOR)

    assert all("UPDATE broker_credentials" not in call.args[0] for call in cursor.execute.call_args_list)


def test_resume_keeps_the_committed_authority_unresolved_when_not_found() -> None:
    events: list[str] = []
    conn = FakeConn(events)
    authority = CoreResumeAuthority(
        intent_id=11,
        trade_id=21,
        order_id=31,
        instrument_id=3417,
        amount=Decimal("49.9"),
        request_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
        broker_order_ref=None,
        eligibility_proof_id=7,
        operator_id=OPERATOR,
        api_key_credential_id=API_CREDENTIAL,
        user_key_credential_id=USER_CREDENTIAL,
    )
    broker = MagicMock()
    broker.lookup_order.side_effect = BrokerOrderNotFound("not found")
    broker.place_demo_core_order.return_value = BrokerCoreOrderSubmission(
        broker_order_ref="9001",
        reference_id=authority.request_id,
        response_digest="a" * 64,
    )
    with (
        patch("app.services.strategy_core_executor.core_submission_lock", return_value=nullcontext()),
        patch("app.services.strategy_core_executor.load_core_resume_authority", return_value=authority),
        patch(
            "app.services.strategy_core_executor.reconcile_strategy_order",
            return_value=SimpleNamespace(state="not_found"),
        ),
        patch(
            "app.services.strategy_core_executor.load_core_mandate",
            return_value=SimpleNamespace(enabled=True, core_instrument_id=3417),
        ),
        patch("app.services.strategy_core_executor.require_selected_core_instrument"),
        patch(
            "app.services.strategy_core_executor.require_core_eligibility",
            return_value=SimpleNamespace(
                proof_id=7,
                api_key_credential_id=API_CREDENTIAL,
                user_key_credential_id=USER_CREDENTIAL,
                response_currency="USD",
                min_position_exposure=Decimal("10"),
                min_position_amount=Decimal("10"),
            ),
        ),
        patch("app.services.strategy_core_executor.observe_core_sleeve", return_value=object()),
        patch(
            "app.services.strategy_core_executor.evaluate_core_rebalance",
            return_value=SimpleNamespace(action="buy_core"),
        ),
        patch(
            "app.services.strategy_core_executor.assess_core_broker_preflight",
            return_value=SimpleNamespace(admitted=True, reason_code=None, amount=authority.amount),
        ),
        patch(
            "app.services.strategy_core_executor.preflight_core_submission",
            return_value=SimpleNamespace(admitted=True, reason_code=None),
        ),
    ):
        result = resume_core_submission(conn, broker=broker, authority=authority)  # type: ignore[arg-type]

    assert result.state == "submission_uncertain"
    assert result.reason_code == "core_order_reconciliation_not_found"
    broker.place_demo_core_order.assert_not_called()


def test_resume_reconciles_a_found_order_without_resubmitting() -> None:
    events: list[str] = []
    conn = FakeConn(events)
    authority = CoreResumeAuthority(
        intent_id=11,
        trade_id=21,
        order_id=31,
        instrument_id=3417,
        amount=Decimal("49.9"),
        request_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
        broker_order_ref=None,
        eligibility_proof_id=7,
        operator_id=OPERATOR,
        api_key_credential_id=API_CREDENTIAL,
        user_key_credential_id=USER_CREDENTIAL,
    )
    broker = MagicMock()
    reconcile = MagicMock(return_value=SimpleNamespace(state="resolved"))
    with (
        patch("app.services.strategy_core_executor.core_submission_lock", return_value=nullcontext()),
        patch("app.services.strategy_core_executor.load_core_resume_authority", return_value=authority),
        patch("app.services.strategy_core_executor.reconcile_strategy_order", reconcile),
    ):
        result = resume_core_submission(conn, broker=broker, authority=authority)  # type: ignore[arg-type]

    assert result.reason_code == "core_order_reconciled"
    reconcile.assert_called_once_with(conn, broker=broker, order_id=31)
    broker.place_demo_core_order.assert_not_called()


def test_resume_lookup_miss_never_reaches_fresh_safety_or_resubmission() -> None:
    events: list[str] = []
    conn = FakeConn(events)
    authority = CoreResumeAuthority(
        intent_id=11,
        trade_id=21,
        order_id=31,
        instrument_id=3417,
        amount=Decimal("49.9"),
        request_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
        broker_order_ref=None,
        eligibility_proof_id=7,
        operator_id=OPERATOR,
        api_key_credential_id=API_CREDENTIAL,
        user_key_credential_id=USER_CREDENTIAL,
    )
    broker = MagicMock()
    broker.lookup_order.side_effect = BrokerOrderNotFound("not found")
    with (
        patch("app.services.strategy_core_executor.core_submission_lock", return_value=nullcontext()),
        patch("app.services.strategy_core_executor.load_core_resume_authority", return_value=authority),
        patch(
            "app.services.strategy_core_executor.reconcile_strategy_order",
            return_value=SimpleNamespace(state="not_found"),
        ),
        patch(
            "app.services.strategy_core_executor.load_core_mandate",
            return_value=SimpleNamespace(enabled=True, core_instrument_id=3417),
        ),
        patch("app.services.strategy_core_executor.require_selected_core_instrument"),
        patch(
            "app.services.strategy_core_executor.require_core_eligibility",
            return_value=SimpleNamespace(
                proof_id=7,
                api_key_credential_id=API_CREDENTIAL,
                user_key_credential_id=USER_CREDENTIAL,
                response_currency="USD",
                min_position_exposure=Decimal("10"),
                min_position_amount=Decimal("10"),
            ),
        ),
        patch("app.services.strategy_core_executor.observe_core_sleeve", return_value=object()),
        patch(
            "app.services.strategy_core_executor.evaluate_core_rebalance",
            return_value=SimpleNamespace(action="buy_core"),
        ),
        patch(
            "app.services.strategy_core_executor.assess_core_broker_preflight",
            return_value=SimpleNamespace(admitted=True, reason_code=None, amount=authority.amount),
        ),
        patch(
            "app.services.strategy_core_executor.preflight_core_submission",
            return_value=SimpleNamespace(admitted=False, reason_code="core_kill_switch_active"),
        ),
    ):
        result = resume_core_submission(conn, broker=broker, authority=authority)  # type: ignore[arg-type]

    assert result.state == "submission_uncertain"
    assert result.reason_code == "core_order_reconciliation_not_found"
    broker.place_demo_core_order.assert_not_called()
