"""#2949 — the shared half of the core/cash process-restart acceptance harness.

Imported by BOTH sides of the fault boundary: the pytest parent
(``tests/test_2949_core_restart_recovery_db.py``) and the child process it kills
(``tests/fixtures/core_restart_child.py``).  Everything here therefore has to be
importable without pytest and without the repo conftest.

Two things in it are load-bearing rather than convenience.

**The broker keeps its state in a FILE.** #2949's bounded experiment says so
explicitly, and the reason is not tidiness: a mock whose accepted orders live in
the engine's own memory dies with the engine, so every restart scenario would
"pass" by amnesia.  The one fact a crash-recovery proof needs is that the BROKER
still believes an order exists after our process is gone, and only out-of-process
state can carry it.

**The seed is committed, not a fixture rollback.** The child is a separate
process with its own connection; anything the parent leaves in an open
transaction is invisible to it.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import psycopg

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerCoreOrder,
    BrokerCoreOrderSubmission,
    BrokerCostComponent,
    BrokerDirectPositionInvestment,
    BrokerInstrumentInvestment,
    BrokerOrderDetail,
    BrokerOrderLookupError,
    BrokerOrderNotFound,
    BrokerPositionExecution,
    BrokerWhatIfCostResponse,
    BrokerWhatIfOrder,
)
from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_PASS_VERDICT,
    CORE_ELIGIBILITY_POLICY_VERSION,
)
from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION

#: #2833's declared candidate set.  All three must exist in ``instruments`` or
#: ``load_core_selection`` reports ``missing_candidate_ids`` and refuses.
CANDIDATE_IDS = (3417, 3434, 3075)
CORE_INSTRUMENT_ID = 3417
EXCHANGE_ID = "CORE2949"

OPERATOR_ID = UUID("73d8ad78-3062-4ef5-8f0a-7428865e23d7")
API_CREDENTIAL_ID = UUID("ba39f751-d4bd-4553-ab25-d9acbb73fbe8")
USER_CREDENTIAL_ID = UUID("f7306e0b-9494-415e-85fd-97874510cc83")

#: A fixed instant inside a US regular session (Friday 2026-09-11, 10:30 ET).
#: Injected as the executor's ``clock`` so ``core_market_session_closed`` does not
#: make the whole harness a function of what time the suite happens to run.  Quote
#: and halt-feed freshness are measured against THIS instant, so their seeded
#: stamps are relative to it; eligibility age is measured against SQL ``now()``
#: and is seeded from the real clock instead.
CLOCK = datetime(2026, 9, 11, 14, 30, tzinfo=UTC)

#: Empty core sleeve, all cash.  Keeps ``core_active_position_ids`` empty so the
#: capital authority needs no pre-existing exact ownership to join against.
#: A 60% target with a 5% band on a 1,000 sleeve buys to the 55 near edge, plus
#: whatever the cost quote grosses up -- deliberately NOT written down here, so
#: the assertions compare the persisted amount against what the executor returned
#: rather than against a number a later sizing change would leave stale.
ACCOUNT_CASH = Decimal("1000")

FaultPoint = Literal[
    "none",
    "before_authority_commit",
    "after_commit_before_submit",
    "after_broker_accept",
]

#: The exit status a SIGKILLed child reports through ``subprocess``.
SIGKILL_RETURNCODE = -signal.SIGKILL

#: Wall-clock bound on one engine process.  Dominated by the child's import of
#: ``app``; a run that exceeds it is a hang, not a slow machine.
ENGINE_PROCESS_TIMEOUT_SECONDS = 120

#: Mirrors ``strategy_order_reconciliation._KNOWN_FILLED_BROKER_STATES``.  Copied
#: rather than imported: this is the BROKER's vocabulary in the double, and
#: importing the reconciler's private set would make the fixture agree with the
#: code under test by construction.
_FILLED_BROKER_STATES = frozenset({"Filled", "Executed"})


def _kill_self() -> None:
    """Stop this process with no unwind, no ``finally`` and no commit.

    ``SIGKILL`` rather than ``sys.exit``: the latter raises ``SystemExit``, which
    unwinds the stack, runs ``finally`` blocks and lets psycopg close the
    connection, so the server would see a clean disconnect and any open
    transaction would end deliberately rather than by abandonment.

    ⚠ Not equivalent to a host power cut and not claimed to be.  ``os._exit``
    would be near-identical to this for our purposes; the OS page cache, and
    therefore anything already written by this process, survives either.  What
    this models is an ENGINE death — SIGKILL, OOM, a `launchctl kickstart` — with
    the database and the broker still running.  A machine-level crash is a
    different fault and is not in this matrix.
    """
    os.kill(os.getpid(), signal.SIGKILL)


# ---------------------------------------------------------------------------
# The stateful, file-backed fake broker
# ---------------------------------------------------------------------------


class FileBackedFakeBroker:
    """A broker whose accepted orders outlive the process that submitted them.

    Only ``place_demo_core_order`` mutates.  Every call to it is counted in the
    file BEFORE the fault can fire, so "did the broker see two economic orders?"
    is answerable from the file alone after the engine is dead — which is the
    question the exactly-once invariant reduces to.
    """

    def __init__(
        self,
        state_path: Path | str,
        *,
        fault: FaultPoint = "none",
        fill_status: str = "Filled",
    ) -> None:
        self.state_path = Path(state_path)
        self.fault = fault
        self.fill_status = fill_status
        if not self.state_path.exists():
            self._write({"orders": [], "mutation_calls": 0, "lookup_calls": 0})

    # -- state file -------------------------------------------------------
    def read(self) -> dict[str, Any]:
        return json.loads(self.state_path.read_text())

    def _write(self, state: dict[str, Any]) -> None:
        """Atomically replace the state file, then fsync it.

        The atomic ``os.replace`` IS load-bearing: a SIGKILL during a plain
        truncate-and-write would leave a half-written file and the parent would
        read a corrupt broker rather than a crashed engine.

        The fsyncs are NOT load-bearing for the fault this harness injects — the
        OS page cache outlives the killed process, so the parent would see the
        write either way.  They are here so the fixture stays correct if the
        matrix is ever extended to a machine-level crash, and saying which of the
        two is doing the work matters more than having both.
        """
        directory = self.state_path.parent
        handle = tempfile.NamedTemporaryFile("w", dir=directory, delete=False, encoding="utf-8")
        try:
            json.dump(state, handle)
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            handle.close()
        os.replace(handle.name, self.state_path)
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    # -- reads ------------------------------------------------------------
    def get_account_risk_snapshot(self) -> BrokerAccountRiskSnapshot:
        """Derive the account from this broker's OWN accepted orders.

        A constant snapshot would make the repeat-cycle scenario vacuous: the
        engine would keep seeing an empty sleeve and keep buying, and the harness
        would be measuring the fixture rather than the allocator.  Deriving it
        also exercises the real fail-closed join — ``resolve_engine_capital_usage``
        refuses when an ownership row names a position the snapshot does not
        carry, which is precisely the accounting invariant #2949 asks about.
        """
        state = self.read()
        invested = Decimal("0")
        pending = Decimal("0")
        positions: list[BrokerDirectPositionInvestment] = []
        for record in state["orders"]:
            amount = Decimal(record["amount"])
            if record.get("status", self.fill_status) in _FILLED_BROKER_STATES:
                invested += amount
                positions.append(
                    BrokerDirectPositionInvestment(
                        position_id=int(record["position_id"]),
                        instrument_id=int(record["instrument_id"]),
                        is_buy=True,
                        amount=amount,
                        unrealized_pnl=Decimal("0"),
                        market_value=amount,
                        is_partially_altered=False,
                    )
                )
            else:
                pending += amount
        # eToro reports `available_cash` NET of unfilled commitments and ADDS the
        # same amount to `total_invested` and to the per-instrument investment --
        # `etoro_broker.py:1506-1524` does both, and equity is therefore unchanged
        # by merely placing an order. An earlier draft of this double subtracted
        # pending from cash without adding it back to invested, which quietly
        # shrank equity and would have made the drawdown observation fire on a
        # pending order alone.
        cash = ACCOUNT_CASH - invested - pending
        invested_including_pending = invested + pending
        return BrokerAccountRiskSnapshot(
            available_cash=cash,
            total_invested=invested_including_pending,
            unrealized_pnl=Decimal("0"),
            equity=cash + invested_including_pending,
            instrument_investments=(
                BrokerInstrumentInvestment(
                    CORE_INSTRUMENT_ID,
                    invested_including_pending,
                    invested_including_pending,
                    len(positions),
                    0,
                ),
            ),
            direct_positions=tuple(positions),
            observed_at=CLOCK,
            raw_payload={},
            account_currency_id=1,
            pending_order_amount=pending,
        )

    def set_order_status(self, *, reference_id: str, status: str) -> None:
        """Advance one accepted order's broker status (pending -> filled)."""
        state = self.read()
        for record in state["orders"]:
            if record["reference_id"] == reference_id:
                record["status"] = status
                self._write(state)
                return
        raise AssertionError(f"no accepted order for reference_id={reference_id!r}")

    def get_what_if_costs(self, order: BrokerWhatIfOrder) -> BrokerWhatIfCostResponse:
        return BrokerWhatIfCostResponse(
            instrument_id=order.instrument_id,
            symbol="CORE2949",
            costs=(
                BrokerCostComponent(
                    cost_type="marketSpread",
                    amount=None,
                    value=Decimal("0.10"),
                    currency="USD",
                    raw_payload={},
                ),
            ),
            last_updated=CLOCK,
            raw_payload={},
        )

    def lookup_order(
        self,
        *,
        order_id: str | None = None,
        reference_id: str | None = None,
    ) -> BrokerOrderDetail:
        if order_id is None and reference_id is None:
            # The real adapter refuses an identifier-free lookup
            # (``etoro_broker.py:823``). Without this the double would happily
            # return its FIRST order, so a reconciler mutated to drop both
            # arguments -- i.e. to stop resolving by exact identity at all --
            # would still pass every scenario here.
            raise BrokerOrderLookupError("lookup_order requires an order id or a reference id")
        state = self.read()
        state["lookup_calls"] = int(state.get("lookup_calls", 0)) + 1
        self._write(state)
        for record in state["orders"]:
            if order_id is not None and record["broker_order_ref"] != order_id:
                continue
            if reference_id is not None and record["reference_id"] != reference_id:
                continue
            return self._detail(record)
        raise BrokerOrderNotFound(f"no order for order_id={order_id!r} reference_id={reference_id!r}")

    def _detail(self, record: dict[str, Any]) -> BrokerOrderDetail:
        executions: tuple[BrokerPositionExecution, ...] = ()
        status = str(record.get("status", self.fill_status))
        if status in _FILLED_BROKER_STATES:
            executions = (
                BrokerPositionExecution(
                    position_id=int(record["position_id"]),
                    state="Open",
                    remaining_units=Decimal("1"),
                    opening_units=Decimal("1"),
                    average_price=Decimal(record["amount"]),
                    execution_time=CLOCK,
                    fees=Decimal("0"),
                    raw_payload={},
                ),
            )
        return BrokerOrderDetail(
            broker_order_ref=str(record["broker_order_ref"]),
            reference_id=str(record["reference_id"]),
            status="filled" if executions else "pending",
            broker_status=status,
            instrument_id=int(record["instrument_id"]),
            position_executions=executions,
            last_update=CLOCK,
            raw_payload={},
        )

    # -- the one mutation -------------------------------------------------
    def place_demo_core_order(
        self,
        order: BrokerCoreOrder,
        *,
        request_id: UUID,
    ) -> BrokerCoreOrderSubmission:
        if self.fault == "after_commit_before_submit":
            # BEFORE any state is recorded: the engine dies holding durable
            # authority the broker has never heard of.
            _kill_self()
        state = self.read()
        sequence = len(state["orders"]) + 1
        state["mutation_calls"] = int(state["mutation_calls"]) + 1
        state["orders"].append(
            {
                "reference_id": str(request_id),
                "broker_order_ref": str(900000 + sequence),
                "position_id": 950000 + sequence,
                "instrument_id": order.instrument_id,
                "amount": str(order.amount),
                "status": self.fill_status,
            }
        )
        self._write(state)
        if self.fault == "after_broker_accept":
            # The acceptance is durable at the broker and the response never
            # reaches us. This is scenario 3 and it is the only fault that can
            # strand real money.
            _kill_self()
        record = state["orders"][-1]
        return BrokerCoreOrderSubmission(
            broker_order_ref=str(record["broker_order_ref"]),
            reference_id=request_id,
            # 64 lowercase hex: `strategy_order_reconciliation_hash_check` is a
            # regex on the stored column, so a readable placeholder is rejected.
            response_digest=hashlib.sha256(str(record["broker_order_ref"]).encode()).hexdigest(),
        )


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def seed_core_execution_world(conn: psycopg.Connection[Any]) -> None:
    """Write every row ``execute_core_rebalance`` re-proves, and COMMIT.

    Deliberately raw INSERTs rather than the service writers: the harness is
    about what survives a crash, so the setup must not itself depend on the code
    under test being able to run to completion.
    """
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, 'core restart harness', 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO UPDATE SET asset_class='us_equity'
        """,
        (EXCHANGE_ID,),
    )
    for instrument_id in CANDIDATE_IDS:
        conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, exchange)
            VALUES (%s, %s, 'Core Restart Harness', TRUE, %s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET is_tradable=TRUE, exchange=EXCLUDED.exchange
            """,
            (instrument_id, f"CORE2949.{instrument_id}", EXCHANGE_ID),
        )
    conn.execute(
        """
        INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag)
        VALUES (%s, %s, 99.95, 100.05, 100.00, 0.1, FALSE)
        ON CONFLICT (instrument_id) DO UPDATE
          SET quoted_at=EXCLUDED.quoted_at, bid=EXCLUDED.bid, ask=EXCLUDED.ask,
              spread_flag=FALSE
        """,
        (CORE_INSTRUMENT_ID, CLOCK - timedelta(seconds=30)),
    )
    conn.execute(
        """
        INSERT INTO kill_switch (id, is_active) VALUES (TRUE, FALSE)
        ON CONFLICT (id) DO UPDATE SET is_active=FALSE
        """
    )
    conn.execute(
        """
        INSERT INTO strategy_halt_feed_state (source, fetched_at, source_pub_at, item_count, payload_sha256)
        VALUES ('nasdaq_trader_rss', %s, %s, 0, repeat('0', 64))
        ON CONFLICT (source) DO UPDATE SET fetched_at=EXCLUDED.fetched_at
        """,
        (CLOCK - timedelta(seconds=30), CLOCK - timedelta(seconds=30)),
    )
    conn.execute(
        """
        INSERT INTO runtime_config (id, enable_auto_trading, enable_live_trading, updated_by, reason)
        VALUES (TRUE, TRUE, FALSE, 'core-restart-harness', '#2949 acceptance harness')
        ON CONFLICT (id) DO UPDATE
          SET enable_auto_trading=TRUE, enable_live_trading=FALSE
        """
    )
    conn.execute(
        """
        INSERT INTO operators (operator_id, username, password_hash)
        VALUES (%s, 'core-restart-harness', 'x')
        ON CONFLICT (operator_id) DO NOTHING
        """,
        (OPERATOR_ID,),
    )
    for credential_id, label in ((API_CREDENTIAL_ID, "api_key"), (USER_CREDENTIAL_ID, "user_key")):
        conn.execute(
            """
            INSERT INTO broker_credentials (id, operator_id, provider, label, environment, ciphertext, last_four)
            VALUES (%s, %s, 'etoro', %s, 'demo', '\\x00'::bytea, '0000')
            ON CONFLICT (id) DO NOTHING
            """,
            (credential_id, OPERATOR_ID, label),
        )
    conn.execute(
        """
        INSERT INTO strategy_core_eligibility_proofs (
            instrument_id, operator_id, provider, environment,
            api_key_credential_id, user_key_credential_id, observed_at, verdict,
            requested_currency, response_currency, settlement_type, direction,
            leverage_values, qualifying_arm_count,
            allow_open_position, allow_close_position, allow_partial_close_position,
            min_position_amount, min_position_exposure,
            response_digest, policy_version, recorded_by
        ) VALUES (
            %s, %s, 'etoro', 'demo', %s, %s, now(), %s,
            'USD', 'USD', 'real', 'long', ARRAY[1], 1, TRUE, TRUE, TRUE, 10, 10,
            repeat('a', 64), %s, 'core-restart-harness'
        )
        """,
        (
            CORE_INSTRUMENT_ID,
            OPERATOR_ID,
            API_CREDENTIAL_ID,
            USER_CREDENTIAL_ID,
            CORE_ELIGIBILITY_PASS_VERDICT,
            CORE_ELIGIBILITY_POLICY_VERSION,
        ),
    )
    conn.execute(
        """
        INSERT INTO strategy_paper_pool_events (
            enabled, capital_limit, currency, changed_by, reason, capital_mode,
            mandate_policy_version, risk_profile, max_portfolio_drawdown_pct,
            shorts_allowed, leverage_allowed, approval_mode
        ) VALUES (
            TRUE, 1000, 'USD', 'core-restart-harness', '#2949 acceptance harness',
            'fixed', 'portfolio-mandate-v1', 'balanced', 50, FALSE, FALSE, 'manual'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision, enabled, base_currency, core_instrument_id, core_target_pct,
            liquidity_reserve_pct, rebalance_band_pct, min_rebalance_amount,
            policy_version, changed_by, reason, mode
        ) VALUES (1, TRUE, 'USD', %s, 60, 5, 5, 25, %s, 'core-restart-harness',
                  '#2949 acceptance harness', %s)
        """,
        (CORE_INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION, CORE_MANDATE_MODE),
    )
    conn.commit()


def select_core_instrument() -> None:
    """Publish #2833's selection for the duration of this process.

    ``SELECTED_CORE_INSTRUMENT_ID`` is ``None`` on ``main`` — the five-trading-day
    cost verdict is still running — so ``require_selected_core_instrument`` refuses
    every call and the executor cannot be exercised at all without this.  That is a
    property of the DECLARATION, not of the machinery this harness measures, so it
    is patched rather than worked around.
    """
    from app.services import strategy_core_selection

    strategy_core_selection.SELECTED_CORE_INSTRUMENT_ID = CORE_INSTRUMENT_ID  # type: ignore[misc]
    strategy_core_selection.SELECTED_CORE_EVIDENCE_REF = "#2949 acceptance harness"  # type: ignore[misc]


def run_engine_until_fault(
    *,
    database_url: str,
    workdir: Path,
    fault: FaultPoint,
    fill_status: str = "Filled",
) -> subprocess.CompletedProcess[str]:
    """Run one engine process against ``database_url`` and let it hit ``fault``.

    Returns the completed process so the caller can assert on the exit status:
    ``SIGKILL_RETURNCODE`` means the fault fired, ``0`` means it ran to the end.

    ⚠ Bounded.  The child takes an advisory submission lock and can block on it;
    without a timeout a lock-release regression would hang the suite rather than
    fail it, which is the failure mode an unattended gate is least able to
    report.  The bound is generous — the child's own import of ``app`` dominates
    it — so it cannot fire on a slow machine.
    """
    config_path = workdir / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "database_url": database_url,
                "fault": fault,
                "fill_status": fill_status,
                "broker_state_path": str(workdir / "broker.json"),
                "result_path": str(workdir / "result.json"),
            }
        )
    )
    return subprocess.run(
        [sys.executable, "-m", "tests.fixtures.core_restart_child", str(config_path)],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[2]),
        check=False,
        timeout=ENGINE_PROCESS_TIMEOUT_SECONDS,
    )


def seed_non_core_strategy_order(conn: psycopg.Connection[Any]) -> int:
    """Seed one ALPHA-arm strategy order in the same unresolved shape, and COMMIT.

    The positive control for the core-exclusion claim.  Asserting only that
    ``reconcile_backlog`` returns nothing for a core order proves nothing on its
    own: a backlog broken to return ``()`` unconditionally would pass identically.
    With a non-core order present in the same database and the same
    ``unresolved`` state, the call has to return exactly that one, which
    discriminates "excludes the core arm" from "is broken".

    Raw INSERTs down the whole authorisation chain, because
    ``strategy_trades_exactly_one_authorisation`` requires a funding decision for
    a non-core trade and the point is the SHAPE, not how it was produced.
    """
    signal = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, universe, input_rule_set_versions
        ) VALUES ('s-harness', 'v1', %s, DATE '2026-09-10', 'entry', 'not_fired',
                  'survivorship_free', '{"harness": "2949"}'::jsonb)
        RETURNING signal_id
        """,
        (CORE_INSTRUMENT_ID,),
    ).fetchone()
    assert signal is not None
    deployment = conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id, strategy_version, mode, capital_limit, enabled, updated_by, reason
        ) VALUES ('s-harness', 'v1', 'paper', 100, FALSE, 'core-restart-harness',
                  '#2949 positive control')
        RETURNING deployment_id
        """
    ).fetchone()
    assert deployment is not None
    funding = conn.execute(
        """
        INSERT INTO strategy_funding_decisions (signal_id, deployment_id, verdict, amount, reason_code)
        VALUES (%s, %s, 'allocated', 100, 'harness')
        RETURNING funding_decision_id
        """,
        (signal[0], deployment[0]),
    ).fetchone()
    assert funding is not None
    trade = conn.execute(
        """
        INSERT INTO strategy_trades (funding_decision_id, instrument_id, status)
        VALUES (%s, %s, 'submitted')
        RETURNING strategy_trade_id
        """,
        (funding[0], CORE_INSTRUMENT_ID),
    ).fetchone()
    assert trade is not None
    order = conn.execute(
        """
        INSERT INTO orders (
            instrument_id, action, order_type, requested_amount, status,
            execution_origin, strategy_request_id
        ) VALUES (%s, 'BUY', 'MARKET', 100, 'submitted', 'strategy', gen_random_uuid())
        RETURNING order_id
        """,
        (CORE_INSTRUMENT_ID,),
    ).fetchone()
    assert order is not None
    conn.execute(
        "INSERT INTO strategy_trade_orders (strategy_trade_id, order_id, purpose) VALUES (%s,%s,'entry')",
        (trade[0], order[0]),
    )
    conn.execute(
        "INSERT INTO strategy_order_reconciliation_state (order_id) VALUES (%s)",
        (order[0],),
    )
    conn.commit()
    return int(order[0])


def core_state_report(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """The persisted transitions #2949 asks every scenario to record."""
    row = conn.execute(
        """
        SELECT
          (SELECT count(*) FROM strategy_trades WHERE core_rebalance_intent_id IS NOT NULL),
          (SELECT count(*) FROM orders WHERE execution_origin='strategy'),
          (SELECT count(*) FROM strategy_core_rebalance_intents),
          (SELECT count(*) FROM strategy_order_reconciliation_state),
          (SELECT count(*) FROM strategy_position_ownership WHERE status='active'),
          (SELECT array_agg(DISTINCT status ORDER BY status) FROM strategy_trades),
          (SELECT array_agg(DISTINCT state ORDER BY state) FROM strategy_order_reconciliation_state),
          (SELECT count(*) FROM orders WHERE execution_origin='strategy' AND broker_order_ref IS NOT NULL),
          (SELECT coalesce(sum(requested_amount),0) FROM orders WHERE execution_origin='strategy')
        """
    ).fetchone()
    conn.commit()
    assert row is not None
    return {
        "core_trades": int(row[0]),
        "strategy_orders": int(row[1]),
        "intents": int(row[2]),
        "reconciliation_rows": int(row[3]),
        "active_ownership": int(row[4]),
        "trade_statuses": list(row[5] or []),
        "reconciliation_states": list(row[6] or []),
        "orders_with_broker_ref": int(row[7]),
        "requested_amount_total": Decimal(str(row[8])),
    }


__all__ = [
    "ACCOUNT_CASH",
    "API_CREDENTIAL_ID",
    "CANDIDATE_IDS",
    "CLOCK",
    "CORE_INSTRUMENT_ID",
    "FaultPoint",
    "FileBackedFakeBroker",
    "OPERATOR_ID",
    "SIGKILL_RETURNCODE",
    "USER_CREDENTIAL_ID",
    "core_state_report",
    "run_engine_until_fault",
    "seed_core_execution_world",
    "seed_non_core_strategy_order",
    "select_core_instrument",
]
