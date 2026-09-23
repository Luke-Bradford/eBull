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
    StrategyCoreExecutionError,
    _observe_core_portfolio_drawdown,
    core_order_shape_for,
    execute_core_rebalance,
    resume_core_submission,
)
from app.services.strategy_core_sleeve import CoreSleeveObservationError
from app.services.strategy_engine_capital import EngineCapitalObservationError, EngineCapitalRefusal

OPERATOR = UUID("73d8ad78-3062-4ef5-8f0a-7428865e23d7")
API_CREDENTIAL = UUID("ba39f751-d4bd-4553-ab25-d9acbb73fbe8")
USER_CREDENTIAL = UUID("f7306e0b-9494-415e-85fd-97874510cc83")


class FakeResult:
    def __init__(self, row: tuple[object, ...] | None = None, *, rowcount: int = 0) -> None:
        self._row = row
        # #2961's marker gates the submission on affecting EXACTLY ONE row, so the
        # double has to answer the count. Defaulting to 0 rather than 1 is
        # deliberate: an unmatched statement then LOOKS like a no-op, so a future
        # writer that also counts rows fails loudly here instead of being handed a
        # silent success by the fixture.
        self.rowcount = rowcount

    def fetchone(self) -> tuple[object, ...] | None:
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
        # #2964 item 2. The submitter holds the per-order reconciliation lock
        # across broker I/O and persistence. Both statements MUST answer
        # ``(True,)``: the helper treats anything else as lost ownership and
        # raises, which is the behaviour that matters and is asserted below.
        if "pg_advisory_lock(" in normalized:
            self.events.append("order_lock_acquired")
            return FakeResult((True,))
        if "pg_advisory_unlock(" in normalized:
            self.events.append("order_lock_released")
            return FakeResult((True,))
        if normalized.startswith("INSERT INTO strategy_trades"):
            self.events.append("persist_trade")
            return FakeResult((21,))
        if normalized.startswith("INSERT INTO orders"):
            self.events.append("persist_order")
            # #3003. The side is a PARAMETER now, not a literal in the statement, so the
            # assertion has to read the bound value -- matching on the SQL text would
            # pass for a sell persisted as a buy.
            assert isinstance(_params, tuple)
            self.events.append(f"order_action={_params[1]}")
            return FakeResult((31,))
        if normalized.startswith("INSERT INTO strategy_core_entry_exit_levels"):
            # #3284 item 1. Asserted on the BOUND parameters, for #3003's reason: the
            # statement text carries no rates, so matching on it would pass for a body
            # submitted with the wrong pair.
            self.events.append("persist_exit_levels")
            assert isinstance(_params, tuple)
            self.events.append(f"exit_levels={_params[3]}/{_params[4]}")
            return FakeResult()
        if normalized.startswith("INSERT INTO strategy_order_reconciliation_state"):
            self.events.append("persist_reconciliation")
            # The authority transaction DECLARES the phase; nothing else may.
            assert "'authority_committed'" in normalized
        if "submission_phase='broker_verb_entered'" in normalized:
            self.events.append("mark_submitting")
            return FakeResult(rowcount=1)
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
        self.last_order: object | None = None

    def get_account_risk_snapshot(self) -> object:
        self.events.append("read_snapshot")
        return SimpleNamespace(
            available_cash=Decimal("500"),
            equity=Decimal("1000"),
            observed_at=datetime.now(UTC),
        )

    def place_demo_core_order(self, _order: object, *, request_id: UUID) -> BrokerCoreOrderSubmission:
        assert request_id is not None
        self.last_order = _order
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
    authority_error: Exception | None = None,
    usage_error: Exception | None = None,
    preflight_price: Decimal | None = Decimal("759.86"),
    preflight_quoted_at: datetime | None = None,
    broker_sink: list[object] | None = None,
    within_bound: bool = True,
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
    # #3284 item 1. The exit anchor is the preflight's own ask -- the double carries it
    # because the executor reads it, not because the test needs a number.
    db_preflight = SimpleNamespace(
        admitted=True,
        reason_code=None,
        price=preflight_price,
        quoted_at=preflight_quoted_at or datetime.now(UTC),
    )
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
        headroom=SimpleNamespace(within_bound=within_bound, remaining=Decimal("500" if within_bound else "-80")),
    )

    def observe_drawdown(*_args: object, **_kwargs: object) -> str | None:
        events.append("drawdown_observation")
        return drawdown_refusal

    with (
        # #2603 sell leg step 0 has its own DB test; here there is never a close in flight.
        patch("app.services.strategy_core_executor._resolve_outstanding_core_rebalance_close", return_value=None),
        patch(
            "app.services.strategy_core_executor._execute_core_sell",
            side_effect=lambda *_a, **_k: (
                events.append("sell_branch")
                or CoreExecutionResult("submitted", "core_rebalance_close_submitted", 11, None, None, Decimal("0"))
            ),
        ),
        patch("app.services.strategy_core_executor.load_core_mandate", return_value=mandate),
        # `side_effect=None` is the default, so one patch covers both the healthy and
        # the refusing case without a conditional kwargs dict.
        patch(
            "app.services.strategy_core_executor.load_engine_capital_authority",
            return_value=capital_authority,
            side_effect=authority_error,
        ),
        patch("app.services.strategy_core_executor.load_paper_pool", return_value=paper_pool),
        patch(
            "app.services.strategy_core_executor.resolve_engine_capital_usage",
            return_value=capital_usage,
            side_effect=usage_error,
        ),
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
            side_effect=lambda *_a, **_k: events.extend(["link", f"link_purpose={_k['purpose']}"]),
        ),
    ):
        kwargs = {} if clock is None else {"clock": clock}
        broker = FakeBroker(events, submission)
        if broker_sink is not None:
            broker_sink.append(broker)
        result = execute_core_rebalance(
            conn,  # type: ignore[arg-type]
            broker=broker,  # type: ignore[arg-type]
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


def test_only_a_buy_has_a_built_order_shape() -> None:
    assert core_order_shape_for("buy_core") == ("BUY", "entry")
    # `sell_core` is the one that matters; the rest guard against a caller reaching the
    # map with an allocator verdict that is not a trade at all, or with the order side
    # already resolved (which would mask a double-mapping).
    for unbuilt in ("sell_core", "hold", "refused", "BUY", ""):
        assert core_order_shape_for(unbuilt) is None


def test_a_sell_branches_to_the_whole_close_and_never_reaches_the_entry_write() -> None:
    # #3003 then #2603 sell leg. A sell used to be refused at the write by the buy-only
    # order-shape map; it now branches to `_execute_core_sell` after the common gates, and
    # must still never persist a trade/order or reach the entry verb.
    result, events = _run(AssertionError("must not submit"), action="sell_core")

    assert (result.state, result.reason_code) == ("submitted", "core_rebalance_close_submitted")
    assert events.count("sell_branch") == 1
    assert "persist_trade" not in events
    assert "persist_order" not in events
    assert "broker_submit" not in events


@pytest.mark.parametrize(
    ("action", "expected"),
    [("buy_core", ("refused", "sandbox_exceeded")), ("sell_core", ("submitted", "core_rebalance_close_submitted"))],
)
def test_over_the_sandbox_bound_only_a_buy_is_refused(action: str, expected: tuple[str, str]) -> None:
    """A close reduces exposure; refusing it over the bound would pin the sleeve there."""
    result, events = _run(AssertionError("must not submit"), action=action, within_bound=False)

    assert (result.state, result.reason_code) == expected
    assert "persist_order" not in events


def test_a_drawdown_refusal_does_not_block_a_sell_but_is_still_observed() -> None:
    result, events = _run(
        AssertionError("must not submit"), action="sell_core", drawdown_refusal="portfolio_drawdown_limit"
    )

    assert events.count("drawdown_observation") == 1
    assert "sell_branch" in events
    assert result.reason_code == "core_rebalance_close_submitted"


def test_a_core_entry_reaches_the_broker_carrying_its_stop_and_target() -> None:
    """#3284 item 1 -- no window exists in which a core position is naked.

    The three assertions are one claim each and all three are load-bearing: the levels
    are COMMITTED (so a crash mid-submit resumes with the same body), they are committed
    BEFORE the broker call (so they are never a fact the broker knew first), and the
    submitted order carries the SAME pair that was stored (so the durable record is the
    body, not a parallel opinion about it).
    """
    sink: list[object] = []
    result, events = _run(
        BrokerCoreOrderSubmission(
            broker_order_ref="9001",
            reference_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
            response_digest="a" * 64,
        ),
        broker_sink=sink,
    )

    assert result.state == "submitted"
    # 759.86 is the live core entry; -50% / +200% under `core-exit-v2`.
    assert "exit_levels=379.93/2279.58" in events
    assert events.index("persist_exit_levels") < events.index("broker_submit")
    order = sink[0].last_order  # type: ignore[attr-defined]
    assert (order.stop_loss_rate, order.take_profit_rate) == (Decimal("379.93"), Decimal("2279.58"))


@pytest.mark.parametrize("price", [None, Decimal("0"), Decimal("-1")])
def test_an_unusable_exit_anchor_refuses_before_durable_order_authority(price: Decimal | None) -> None:
    """A core order that cannot derive its stop must not become an order at all.

    Refusing AFTER the `orders` INSERT would leave a durable authority that
    `load_core_resume_authority` then has to raise on forever, so the refusal has to
    precede the write -- which is what the absent `persist_order` asserts.
    """
    result, events = _run(
        BrokerCoreOrderSubmission(
            broker_order_ref="9001",
            reference_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
            response_digest="a" * 64,
        ),
        preflight_price=price,
    )

    assert (result.state, result.reason_code) == ("refused", "core_exit_anchor_unavailable")
    assert "persist_order" not in events
    assert "broker_submit" not in events


def test_an_anchor_too_small_to_quantize_a_stop_refuses_rather_than_raising() -> None:
    """Under two cents the stop quantizes DOWN to 0.00 and `core_exit_levels` raises.

    Unreachable for SPY, reachable for a configurable mandate instrument — and an
    executor that raises where it could refuse turns a bad candidate into a failed
    attended request rather than a reported one.
    """
    result, events = _run(
        BrokerCoreOrderSubmission(
            broker_order_ref="9001",
            reference_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
            response_digest="a" * 64,
        ),
        preflight_price=Decimal("0.01"),
    )

    assert (result.state, result.reason_code) == ("refused", "core_exit_levels_underivable")
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
    # #3003. Both are DERIVED from the allocator action now. Asserted on the bound
    # parameter and the passed kwarg, because the statement text no longer carries them.
    assert "order_action=BUY" in events
    assert "link_purpose=entry" in events
    assert events.index("persist_order") < events.index("broker_submit") < events.index("persist_acceptance")
    assert events.index("lock_enter") < events.index("broker_submit") < events.index("lock_exit")
    # #2964 item 4. The per-order reconciliation lock spans the broker call AND
    # the acceptance write. Without that span a reconciler can resolve the order
    # between them, and `_persist_core_acceptance`'s `state='pending'` then leaves
    # a terminal `reconciled_at` beside a non-terminal state, which
    # `strategy_order_reconciliation_resolved_shape` refuses -- failing the
    # ATTENDED request. Asserting only "the lock was taken" would not catch a
    # release moved back above the persist, which is why both bounds are here.
    assert (
        events.index("order_lock_acquired")
        < events.index("broker_submit")
        < events.index("persist_acceptance")
        < events.index("order_lock_released")
    )
    # #2961. The marker is committed AFTER the authority and BEFORE the broker
    # verb, and both bounds are load-bearing in opposite directions: moved above
    # `persist_reconciliation` there is no separate commit to observe and every
    # authority reads as "may have reached the broker"; moved below
    # `broker_submit` a crash inside the call leaves a row that claims the verb
    # was never entered, which is the direction that costs real money.
    assert events.index("persist_reconciliation") < events.index("mark_submitting") < events.index("broker_submit")


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
    # The failure branches write reconciliation state too, so they are inside the
    # lock for the same reason the acceptance branch is.
    assert events.index("order_lock_acquired") < events.index(evidence_event) < events.index("order_lock_released")


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

    with pytest.raises(CredentialInUse, match="unresolved core or recommendation order"):
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
        stop_loss_rate=Decimal("379.93"),
        take_profit_rate=Decimal("2279.58"),
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
        stop_loss_rate=Decimal("379.93"),
        take_profit_rate=Decimal("2279.58"),
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
        stop_loss_rate=Decimal("379.93"),
        take_profit_rate=Decimal("2279.58"),
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


# --- #2979 half b: the capital reader's refusal is a verdict, not an exception ---


def _capital_refusal(
    code: EngineCapitalRefusal = "engine_capital_ownership_unwitnessed",
) -> EngineCapitalObservationError:
    return EngineCapitalObservationError("active core position 4242 is absent from broker snapshot", code)


@pytest.mark.parametrize("stage", ["authority", "usage"])
def test_a_capital_observation_refusal_returns_a_verdict_rather_than_raising(stage: str) -> None:
    """#2979 half b, at both steady-state sites.

    An inconsistent shared population is true on this cycle and every later one, so the
    caller gets a code it can act on instead of a 409 whose only text is the outer
    sentence.  Still fail-closed: no intent, no order, no broker call.
    """
    error = _capital_refusal()
    kwargs = {"authority_error": error} if stage == "authority" else {"usage_error": error}
    result, events = _run(AssertionError("must not submit"), **kwargs)  # type: ignore[arg-type]

    assert result.state == "refused"
    assert result.reason_code == "engine_capital_ownership_unwitnessed"
    assert result.intent_id is None
    assert result.trade_id is None
    assert result.order_id is None
    # Nothing durable was created and nothing reached the broker.
    assert "broker_submit" not in events
    assert "persist_order" not in events
    assert "persist_reconciliation" not in events
    # The lock is entered and released either way.
    assert events[0] == "lock_enter"
    assert events[-1] == "lock_exit"


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("the account read failed"),
        CoreSleeveObservationError("snapshot observed_at must be timezone-aware"),
    ],
    ids=["broker_read", "sleeve_drift"],
)
def test_the_snapshot_block_still_raises_for_everything_else(error: Exception) -> None:
    """The new arm must PRECEDE the blanket one without absorbing it.

    A failed broker read is an execution fault, and ``CoreSleeveObservationError`` is
    input drift in one payload -- its own docstring puts that on the raising side.  If
    the new arm were written as a bare ``except Exception`` both would silently become
    refusals, which is the failure this test exists to catch.
    """
    with pytest.raises(StrategyCoreExecutionError, match="could not describe the core sleeve"):
        _run(AssertionError("must not submit"), usage_error=error)
