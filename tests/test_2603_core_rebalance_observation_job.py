"""The core rebalance observation job (#2603 step 3b-3).

The chain ``get_account_risk_snapshot -> observe_core_sleeve ->
record_core_rebalance_intent`` was complete and had no caller. These tests pin the
caller's decisions: what it refuses without touching the broker, what it deliberately
does NOT refuse, and that a failure is a failed job rather than a quiet success.

No live database or network calls — every dependency is patched.

Spec: ``docs/proposals/ta/2026-08-22-core-rebalance-observation-job.md``
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.strategy_core_allocator import CoreSleeveState
from app.services.strategy_core_mandate import CoreMandate
from app.services.strategy_core_sleeve import CoreSleeveObservationError
from app.workers.scheduler import JOB_CORE_REBALANCE_OBSERVATION, core_rebalance_observation

_UNSET: Any = object()


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


def _state(core_instrument_id: int = 3417) -> CoreSleeveState:
    return CoreSleeveState(
        core_instrument_id=core_instrument_id,
        core_market_value=Decimal("6000"),
        cash_balance=Decimal("4000"),
        currency="USD",
        as_of=datetime(2026, 8, 22, 22, 45, tzinfo=UTC),
    )


class _Harness:
    """One dispatch of the job body with every collaborator patched."""

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
        self.conn = conn

        self.intent = MagicMock()
        self.intent.core_rebalance_intent_id = 11
        self.intent.core_mandate_event_id = 7
        self.intent.decision.action = "hold"
        self.intent.decision.reason_code = None

    def run(
        self,
        *,
        env: str = "demo",
        creds: tuple[str, str] | None = ("key", "ukey"),
        mandate: CoreMandate | None | Any = _UNSET,
        mandate_after: CoreMandate | None | Any = _UNSET,
        observe: Any = None,
        snapshot_error: Exception | None = None,
    ) -> dict[str, MagicMock]:
        # ⚠ A sentinel, not ``None``: "no mandate configured" is one of the cases
        # under test, so the default cannot be spelled the same way as it.
        if mandate is _UNSET:
            mandate = _mandate()
        if snapshot_error is not None:
            self.broker.get_account_risk_snapshot.side_effect = snapshot_error

        # The job loads the mandate twice — once before the HTTP call to learn
        # the instrument, once under the advisory lock to confirm it has not
        # moved. ``mandate_after`` is the second answer.
        load_kwargs: dict[str, Any] = (
            {"return_value": mandate} if mandate_after is _UNSET else {"side_effect": [mandate, mandate_after]}
        )

        observe_kwargs: dict[str, Any] = (
            {"side_effect": observe} if isinstance(observe, Exception) else {"return_value": observe or _state()}
        )

        with (
            patch("app.workers.scheduler.settings.etoro_env", env),
            patch("app.workers.scheduler._load_etoro_credentials", return_value=creds),
            patch("app.workers.scheduler._record_prereq_skip") as skip,
            patch("app.workers.scheduler._tracked_job", return_value=self.tracker),
            patch("app.workers.scheduler.connect_job", return_value=self.conn),
            patch("app.providers.implementations.etoro_broker.EtoroBrokerProvider", return_value=self.broker) as prov,
            patch("app.services.strategy_core_mandate.load_core_mandate", **load_kwargs) as load,
            patch("app.services.strategy_core_sleeve.observe_core_sleeve", **observe_kwargs) as obs,
            patch(
                "app.services.strategy_core_rebalance_intent.record_core_rebalance_intent",
                return_value=self.intent,
            ) as record,
        ):
            core_rebalance_observation()

        return {"skip": skip, "provider": prov, "load": load, "observe": obs, "record": record}


class TestRefusalsThatCostNoBrokerCall:
    """Each of these must skip BEFORE the provider is constructed.

    A refusal that still spends a request is not free, and the reason these are
    checked on the provider rather than on the response is that a MagicMock broker
    answers happily either way.
    """

    def test_a_real_environment_skips_without_touching_the_broker(self) -> None:
        calls = _Harness().run(env="real")
        calls["provider"].assert_not_called()
        assert calls["skip"].call_args[0][0] == JOB_CORE_REBALANCE_OBSERVATION
        assert "demo" in calls["skip"].call_args[0][1]

    def test_missing_credentials_skip_without_touching_the_broker(self) -> None:
        calls = _Harness().run(creds=None)
        calls["provider"].assert_not_called()
        assert "credentials" in calls["skip"].call_args[0][1]

    def test_no_mandate_skips_without_touching_the_broker(self) -> None:
        """Spending a request to learn a fact about our OWN configuration table.

        ``core_mandate_absent`` is fully derivable from
        ``strategy_core_mandate_events``, and reaching it here would need a
        fabricated CoreSleeveState to hang the code on.
        """
        calls = _Harness().run(mandate=None)
        calls["provider"].assert_not_called()
        calls["record"].assert_not_called()
        assert "no core mandate" in calls["skip"].call_args[0][1]

    def test_a_mandate_without_an_instrument_skips_without_touching_the_broker(self) -> None:
        calls = _Harness().run(mandate=_mandate(core_instrument_id=None))
        calls["provider"].assert_not_called()
        calls["record"].assert_not_called()
        assert "no core instrument" in calls["skip"].call_args[0][1]


class TestTheDeliberateNonRefusal:
    def test_a_disabled_mandate_is_still_observed_and_recorded(self) -> None:
        """⚠ The one place the cheap choice was not taken, and the test that
        defends it.

        Skipping on ``enabled = False`` would save one request a day and make the
        disabled window a hole indistinguishable from "the job was down" — and it
        would leave ``core_mandate_disabled`` producible by the allocator and
        reachable from no producer, which is the shape step 3b-2 item 1 deleted.
        """
        harness = _Harness()
        calls = harness.run(mandate=_mandate(enabled=False))
        harness.broker.get_account_risk_snapshot.assert_called_once()
        calls["record"].assert_called_once()
        calls["skip"].assert_not_called()


class TestTheMandateRevisionRace:
    """A revision landing during the broker round-trip.

    ⚠ ``sleeve_instrument_mismatch`` catches only the instrument-changing case.
    A revision that moves the target, band, reserve or floor while KEEPING the
    instrument produces a verdict that looks entirely normal and describes a
    sleeve nobody observed under it — which is why the event id is re-checked
    under the same advisory lock ``configure_core_mandate`` takes.
    """

    def test_a_same_instrument_reconfiguration_drops_the_tick_rather_than_misattributing_it(self) -> None:
        harness = _Harness()
        moved = CoreMandate(
            event_id=8,
            revision=2,
            enabled=True,
            base_currency="USD",
            core_instrument_id=3417,  # unchanged — the case the allocator cannot see
            core_target_pct=Decimal("80"),
            liquidity_reserve_pct=Decimal("5"),
            rebalance_band_pct=Decimal("5"),
            min_rebalance_amount=Decimal("50"),
            policy_version="core-mandate-v1",
        )
        calls = harness.run(mandate_after=moved)
        calls["record"].assert_not_called()
        harness.conn.rollback.assert_called_once()
        assert "skipped" in harness.tracker.note
        assert harness.tracker.row_count == 0

    def test_a_mandate_deleted_during_the_round_trip_drops_the_tick(self) -> None:
        harness = _Harness()
        calls = harness.run(mandate_after=None)
        calls["record"].assert_not_called()
        harness.conn.rollback.assert_called_once()

    def test_the_recheck_holds_the_same_advisory_lock_configure_takes(self) -> None:
        """Re-reading without the lock is a check-then-write window, not a fix:
        READ COMMITTED gives each statement a fresh snapshot, so a writer can
        still commit between the re-read and the INSERT."""
        from app.services.strategy_core_mandate import CORE_MANDATE_ADVISORY_LOCK

        harness = _Harness()
        harness.run()
        locks = [c for c in harness.conn.execute.call_args_list if "pg_advisory_xact_lock" in str(c.args[0])]
        assert len(locks) == 1
        assert locks[0].args[1] == CORE_MANDATE_ADVISORY_LOCK


class TestTheHappyPath:
    def test_one_dispatch_records_exactly_one_intent_stamped_with_the_job_name(self) -> None:
        harness = _Harness()
        calls = harness.run()
        calls["record"].assert_called_once()
        kwargs = calls["record"].call_args.kwargs
        assert kwargs["recorded_by"] == JOB_CORE_REBALANCE_OBSERVATION
        assert kwargs["state"].core_instrument_id == 3417
        harness.conn.commit.assert_called_once()

    def test_the_mandate_instrument_is_what_gets_observed(self) -> None:
        """The sleeve is observed for the mandate's instrument, not the one the
        allocator happens to find later — the pairing is the whole point of the
        pre-read."""
        harness = _Harness()
        calls = harness.run(mandate=_mandate(core_instrument_id=3434), observe=_state(3434))
        assert calls["observe"].call_args.kwargs["core_instrument_id"] == 3434

    def test_the_provider_is_pinned_to_demo_rather_than_re_reading_the_setting(self) -> None:
        """Check/use gap: the guard reads ``settings.etoro_env`` and the
        construction must not read it a second time, or a mutation between the two
        opens a real account under a demo verdict.

        ⚠ The obvious form of this test — run the harness, assert
        ``env == "demo"`` — passes under BOTH spellings, because the harness has
        already set ``settings.etoro_env`` to ``"demo"`` and the two are then
        equal. Revert-probed: swapping the literal for ``settings.etoro_env``
        left it green. So the setting is mutated *between* the guard and the
        construction, in the one place that runs between them, and the assertion
        is on the value that survives that mutation. (Prevention-log shape: a
        test written against the one-member set cannot tell the two apart.)
        """
        from app.workers import scheduler

        harness = _Harness()

        def flip_env_then_answer(_job_name: str) -> tuple[str, str]:
            scheduler.settings.etoro_env = "real"  # type: ignore[misc]
            return ("key", "ukey")

        with (
            patch("app.workers.scheduler.settings.etoro_env", "demo"),
            patch("app.workers.scheduler._load_etoro_credentials", side_effect=flip_env_then_answer),
            patch("app.workers.scheduler._record_prereq_skip"),
            patch("app.workers.scheduler._tracked_job", return_value=harness.tracker),
            patch("app.workers.scheduler.connect_job", return_value=harness.conn),
            patch(
                "app.providers.implementations.etoro_broker.EtoroBrokerProvider",
                return_value=harness.broker,
            ) as provider,
            patch("app.services.strategy_core_mandate.load_core_mandate", return_value=_mandate()),
            patch("app.services.strategy_core_sleeve.observe_core_sleeve", return_value=_state()),
            patch(
                "app.services.strategy_core_rebalance_intent.record_core_rebalance_intent",
                return_value=harness.intent,
            ),
        ):
            core_rebalance_observation()

        assert provider.call_args.kwargs["env"] == "demo"

    def test_no_db_connection_is_held_across_the_http_call(self) -> None:
        """#1593 — the mandate read and the intent write are separate short-lived
        connections either side of the provider session."""
        harness = _Harness()
        order: list[str] = []
        harness.conn.__exit__ = MagicMock(side_effect=lambda *_: order.append("conn_closed") or False)
        harness.broker.get_account_risk_snapshot.side_effect = lambda: order.append("http") or MagicMock()

        harness.run()

        # closed, http, closed — the HTTP call sits between two closed connections.
        assert order == ["conn_closed", "http", "conn_closed"]


class TestFailuresAreFailures:
    """⚠ Not caught-and-noted. A job that no-ops and reports success is invisible
    to every automated check this repo has, and there is no primary work here to
    protect by swallowing — the observation IS the work."""

    def test_an_unavailable_broker_propagates_and_records_nothing(self) -> None:
        harness = _Harness()
        with pytest.raises(RuntimeError, match="etoro down"):
            harness.run(snapshot_error=RuntimeError("etoro down"))
        # The tracked block is still open when it raises, so the job_runs row is
        # finalised as a failure by ``_tracked_job``'s own __exit__.
        harness.tracker.__exit__.assert_called_once()

    def test_an_unobservable_sleeve_propagates_and_records_nothing(self) -> None:
        harness = _Harness()
        with pytest.raises(CoreSleeveObservationError):
            harness.run(observe=CoreSleeveObservationError("direct short on the core instrument"))


def test_the_job_is_registered_and_invocable() -> None:
    """A body with no registry entry never fires; one with no invoker cannot be
    triggered from the admin UI. Both are silent."""
    from app.jobs.runtime import _INVOKERS
    from app.workers.scheduler import SCHEDULED_JOBS

    entry = next((j for j in SCHEDULED_JOBS if j.name == JOB_CORE_REBALANCE_OBSERVATION), None)
    assert entry is not None, "core_rebalance_observation missing from SCHEDULED_JOBS"
    assert JOB_CORE_REBALANCE_OBSERVATION in _INVOKERS


def test_the_lane_is_etoro_and_not_strategy_execution() -> None:
    """``strategy_execution`` is held by strategy_paper_cycle every five minutes,
    so a daily job on that lane is a daily job that skips. The only external call
    here is an eToro read, and ``etoro`` is the lane that owns that budget."""
    from app.workers.scheduler import SCHEDULED_JOBS

    entry = next(j for j in SCHEDULED_JOBS if j.name == JOB_CORE_REBALANCE_OBSERVATION)
    assert entry.source == "etoro"


def test_the_mandate_mode_check_still_pins_paper() -> None:
    """The job's demo-only guard rests on a value it never reads.

    ``load_core_mandate`` does not SELECT ``mode``, so nothing in the job
    observes that the mandate it is evaluating is a PAPER one — it relies on
    ``sql/349``'s CHECK making every row paper by construction. That reliance is
    invisible at the call site, which is exactly why it is bound here: if ``mode``
    ever widens, this fails and the assumption surfaces before the widening
    merges, rather than after a live book's drift has been attributed to a paper
    policy.
    """
    from pathlib import Path

    sql = (Path(__file__).resolve().parents[1] / "sql" / "349_core_trade_arc.sql").read_text()
    assert "CHECK (mode = 'paper')" in sql


def test_the_job_does_not_catch_up_on_boot() -> None:
    """``catch_up_on_boot`` defaults to True in ``ScheduledJob``. Left at the
    default, a restart appends an off-schedule observation whose marks are
    whatever the broker happens to hold at boot."""
    from app.workers.scheduler import SCHEDULED_JOBS

    entry = next(j for j in SCHEDULED_JOBS if j.name == JOB_CORE_REBALANCE_OBSERVATION)
    assert entry.catch_up_on_boot is False
