"""The scheduled core rebalance execution job (#3359).

``execute_core_rebalance`` had one caller -- the attended endpoint. These tests pin
the scheduled caller's decisions: what it refuses before a secret or a request, that
it dispatches exactly as the endpoint does (resume an unresolved order, else
rebalance), that it is demo-pinned, and that ``reconcile_required`` is a failed run
rather than a quiet success.

No live database or network calls -- every dependency is patched.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest

from app.services.broker_credentials import LoadedCredential
from app.services.market_session_support import venue_session_is_open
from app.services.strategy_core_executor import CoreExecutionResult
from app.services.strategy_core_mandate import CoreMandate
from app.workers.scheduler import (
    JOB_CORE_REBALANCE_EXECUTION,
    core_rebalance_execution,
    core_venue_skip_reason,
)

_UNSET: Any = object()
_API = LoadedCredential(id=UUID(int=1), plaintext="key")
_USER = LoadedCredential(id=UUID(int=2), plaintext="ukey")
_OPERATOR = uuid4()


def _mandate(*, enabled: bool = True, core_instrument_id: int | None = 3417) -> CoreMandate:
    return CoreMandate(
        event_id=7,
        revision=1,
        enabled=enabled,
        base_currency="USD",
        core_instrument_id=core_instrument_id,
        core_target_pct=Decimal("60"),
        liquidity_reserve_pct=Decimal("5"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("50"),
        policy_version="core-mandate-v1",
    )


def _result(state: str = "held", reason: str = "core_within_band", **kw: Any) -> CoreExecutionResult:
    return CoreExecutionResult(
        state=state,  # type: ignore[arg-type]
        reason_code=reason,
        intent_id=kw.get("intent_id", 11),
        trade_id=kw.get("trade_id"),
        order_id=kw.get("order_id"),
        amount=kw.get("amount", Decimal("0")),
    )


class _Harness:
    def __init__(self) -> None:
        self.broker = MagicMock()
        self.broker.__enter__ = MagicMock(return_value=self.broker)
        self.broker.__exit__ = MagicMock(return_value=False)
        self.tracker = MagicMock()
        self.tracker.__enter__ = MagicMock(return_value=self.tracker)
        self.tracker.__exit__ = MagicMock(return_value=False)
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchone.return_value = ("us_equity",)
        self.conn = conn

    def run(
        self,
        *,
        env: str = "demo",
        mandate: CoreMandate | None | Any = _UNSET,
        venue_skip: str | None = None,
        creds: tuple[LoadedCredential, LoadedCredential] | None = (_API, _USER),
        resume: Any = None,
        result: CoreExecutionResult | None = None,
    ) -> dict[str, MagicMock]:
        if mandate is _UNSET:
            mandate = _mandate()
        with (
            patch("app.workers.scheduler.settings.etoro_env", env),
            patch("app.workers.scheduler._record_prereq_skip") as skip,
            patch("app.workers.scheduler._tracked_job", return_value=self.tracker),
            patch("app.workers.scheduler.connect_job", return_value=self.conn),
            patch("app.workers.scheduler.sole_operator_id", return_value=_OPERATOR),
            patch("app.workers.scheduler.core_venue_skip_reason", return_value=venue_skip),
            patch("app.workers.scheduler._load_etoro_credentials_with_ids", return_value=creds) as load_creds,
            patch("app.providers.implementations.etoro_broker.EtoroBrokerProvider", return_value=self.broker) as prov,
            patch("app.services.strategy_core_mandate.load_core_mandate", return_value=mandate),
            patch("app.services.strategy_core_executor.load_core_resume_authority", return_value=resume),
            patch(
                "app.services.strategy_core_executor.resume_core_submission",
                return_value=result or _result(),
            ) as resume_fn,
            patch(
                "app.services.strategy_core_executor.execute_core_rebalance",
                return_value=result or _result(),
            ) as execute,
        ):
            core_rebalance_execution()
        return {"skip": skip, "provider": prov, "load_creds": load_creds, "resume": resume_fn, "execute": execute}


class TestRefusalsBeforeAnySecretOrRequest:
    """Each must skip before the credential decrypt and before a provider exists."""

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"env": "real"}, "demo"),
            ({"mandate": None}, "no core mandate"),
            ({"mandate": _mandate(core_instrument_id=None)}, "no core instrument"),
            ({"mandate": _mandate(enabled=False)}, "disabled"),
            ({"venue_skip": "core venue session is closed"}, "closed"),
        ],
    )
    def test_skips_without_credentials_or_broker(self, kwargs: dict[str, Any], expected: str) -> None:
        calls = _Harness().run(**kwargs)
        calls["load_creds"].assert_not_called()
        calls["provider"].assert_not_called()
        calls["execute"].assert_not_called()
        assert calls["skip"].call_args[0][0] == JOB_CORE_REBALANCE_EXECUTION
        assert expected in calls["skip"].call_args[0][1]

    def test_missing_credentials_skip_without_touching_the_broker(self) -> None:
        calls = _Harness().run(creds=None)
        calls["provider"].assert_not_called()
        assert "credentials" in calls["skip"].call_args[0][1]


class TestDispatchMirrorsTheEndpoint:
    def test_no_unresolved_order_runs_the_rebalance_with_the_loaded_account(self) -> None:
        calls = _Harness().run()
        calls["resume"].assert_not_called()
        kwargs = calls["execute"].call_args.kwargs
        assert kwargs["operator_id"] == _OPERATOR
        assert (kwargs["api_key_credential_id"], kwargs["user_key_credential_id"]) == (_API.id, _USER.id)
        assert kwargs["recorded_by"] == JOB_CORE_REBALANCE_EXECUTION

    def test_an_unresolved_order_is_reconciled_and_nothing_new_is_evaluated(self) -> None:
        authority = SimpleNamespace(order_id=42, api_key_credential_id=_API.id, user_key_credential_id=_USER.id)
        calls = _Harness().run(resume=authority)
        calls["execute"].assert_not_called()
        assert calls["resume"].call_args.kwargs["authority"] is authority

    def test_an_unresolved_order_owned_by_other_credentials_refuses_before_the_broker(self) -> None:
        authority = SimpleNamespace(order_id=42, api_key_credential_id=uuid4(), user_key_credential_id=_USER.id)
        harness = _Harness()
        with pytest.raises(RuntimeError, match="different account"):
            harness.run(resume=authority)
        harness.broker.__enter__.assert_not_called()

    def test_the_provider_is_pinned_to_the_demo_literal(self) -> None:
        """Check/use gap: the setting is flipped AFTER the guard, inside the one
        call that runs between the guard and construction."""
        from app.workers import scheduler

        harness = _Harness()

        def flip_env_then_answer(_job: str) -> tuple[LoadedCredential, LoadedCredential]:
            scheduler.settings.etoro_env = "real"  # type: ignore[misc]
            return (_API, _USER)

        with (
            patch("app.workers.scheduler.settings.etoro_env", "demo"),
            patch("app.workers.scheduler._record_prereq_skip"),
            patch("app.workers.scheduler._tracked_job", return_value=harness.tracker),
            patch("app.workers.scheduler.connect_job", return_value=harness.conn),
            patch("app.workers.scheduler.sole_operator_id", return_value=_OPERATOR),
            patch("app.workers.scheduler.core_venue_skip_reason", return_value=None),
            patch("app.workers.scheduler._load_etoro_credentials_with_ids", side_effect=flip_env_then_answer),
            patch("app.providers.implementations.etoro_broker.EtoroBrokerProvider", return_value=harness.broker) as p,
            patch("app.services.strategy_core_mandate.load_core_mandate", return_value=_mandate()),
            patch("app.services.strategy_core_executor.load_core_resume_authority", return_value=None),
            patch("app.services.strategy_core_executor.execute_core_rebalance", return_value=_result()),
        ):
            core_rebalance_execution()
        assert p.call_args.kwargs["env"] == "demo"


class TestOutcomes:
    @pytest.mark.parametrize(
        "result",
        [
            _result("held", "core_within_band"),
            _result("refused", "core_auto_trading_disabled"),
            _result("submitted", "core_submitted", order_id=9, amount=Decimal("120")),
            _result("closed", "core_rebalance_closed", order_id=9),
        ],
    )
    def test_normal_outcomes_finish_as_success_with_the_reason_in_the_note(self, result: CoreExecutionResult) -> None:
        harness = _Harness()
        harness.run(result=result)
        assert f"state={result.state}" in harness.tracker.note
        assert f"reason={result.reason_code}" in harness.tracker.note

    def test_reconcile_required_is_a_failed_run(self) -> None:
        harness = _Harness()
        with pytest.raises(RuntimeError, match="reconcil"):
            harness.run(result=_result("reconcile_required", "core_rebalance_close_unresolved", order_id=9))
        assert "reconcile_required" in harness.tracker.note


def _utc_at(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=UTC)


class TestTheFireTimeIsInsideEverySession:
    """15:37 UTC must be open on exactly the days the NYSE trades -- both DST
    regimes, half days included. Compared against 11:00 ET, which is inside every
    regular and half-day session, so this pins the constant, not the calendar."""

    def test_every_day_2025_through_2027(self) -> None:
        from app.workers.scheduler import SCHEDULED_JOBS

        entry = next(j for j in SCHEDULED_JOBS if j.name == JOB_CORE_REBALANCE_EXECUTION)
        et = ZoneInfo("America/New_York")
        day = date(2025, 1, 1)
        mismatches = []
        while day <= date(2027, 12, 31):
            fire = _utc_at(day, entry.cadence.hour, entry.cadence.minute)
            mid_morning = datetime.combine(day, time(11, 0), tzinfo=et)
            if venue_session_is_open("us_equity", fire) != venue_session_is_open("us_equity", mid_morning):
                mismatches.append(day)
            day += timedelta(days=1)
        assert mismatches == []

    @pytest.mark.parametrize(
        ("asset_class", "when", "expected"),
        [
            ("us_equity", _utc_at(date(2026, 9, 24), 15, 37), None),  # Thu, EDT
            ("us_equity", _utc_at(date(2026, 1, 15), 15, 37), None),  # Thu, EST
            ("us_equity", _utc_at(date(2026, 9, 26), 15, 37), "closed"),  # Sat
            ("us_equity", _utc_at(date(2026, 12, 25), 15, 37), "closed"),  # Christmas
            (None, _utc_at(date(2026, 9, 24), 15, 37), "not session-supported"),
        ],
    )
    def test_skip_reason(self, asset_class: str | None, when: datetime, expected: str | None) -> None:
        reason = core_venue_skip_reason(asset_class, when)
        assert reason == expected if expected is None else (reason is not None and expected in reason)


def test_registration() -> None:
    """Registered, invocable, never a boot catch-up or a re-armed late fire (it
    submits orders), on the core lane and still on the general execution lane."""
    from app.jobs.runtime import _INVOKERS, EXECUTION_LANE_GENERAL, execution_lane_for
    from app.jobs.sources import source_for
    from app.workers.scheduler import SCHEDULED_JOBS

    entry = next(j for j in SCHEDULED_JOBS if j.name == JOB_CORE_REBALANCE_EXECUTION)
    assert JOB_CORE_REBALANCE_EXECUTION in _INVOKERS
    assert entry.catch_up_on_boot is False
    assert entry.rearm_on_lost_fire is False
    assert source_for(JOB_CORE_REBALANCE_EXECUTION) == "etoro_core_rebalance"
    assert execution_lane_for(JOB_CORE_REBALANCE_EXECUTION) == EXECUTION_LANE_GENERAL
