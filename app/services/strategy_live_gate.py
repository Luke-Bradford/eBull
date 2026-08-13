"""Fail-closed paper-to-live strategy promotion controls (#2450).

There are deliberately no default evidence thresholds.  One immutable policy
must be registered while the strategy is still in forward observation, before
paper results exist.  Assessment is read-only; only an explicit operator
promotion attempt appends a compact evidence snapshot.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus

from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration, declaration_refusals
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import load_preregistration
from app.services.strategy_control_plane import (
    StrategyControlError,
    current_stage,
    lock_strategy_control,
    registered_strategy_purpose,
)
from app.services.strategy_monitoring import load_entry_block_state, load_owned_pnl

DrillKind = Literal[
    "quote_lag",
    "scan_lag",
    "broker_outage",
    "reconciliation_backlog",
    "drawdown",
]

LIVE_GATE_VERSION = "strategy-live-gate-v1"
REQUIRED_KILL_DRILLS: tuple[DrillKind, ...] = (
    "quote_lag",
    "scan_lag",
    "broker_outage",
    "reconciliation_backlog",
    "drawdown",
)


@dataclass(frozen=True)
class LiveGatePolicy:
    live_gate_policy_id: int
    strategy_id: str
    strategy_version: str
    min_forward_resolved_signals: int
    min_forward_days: int
    min_paper_closed_trades: int
    min_paper_days: int
    max_reconciliation_age_seconds: int
    min_shadow_alpha_pct: Decimal
    max_cost_drift_pct: Decimal
    max_average_slippage_pct: Decimal
    max_drawdown_pct: Decimal
    max_scan_age_seconds: int
    max_quote_age_seconds: int
    max_broker_health_age_seconds: int
    max_live_capital: Decimal
    currency: str
    leverage: int
    registered_at: datetime
    #: #2599's frozen preregistration declaration, whose forward-shadow floor
    #: binds this policy. ⚠ NULLABLE, and NULL is the fail-closed default: a
    #: policy registered before #2599 carries no floor, and the gate refuses
    #: with `forward_shadow_floor_missing` rather than treating "unset" as
    #: "unbounded". Making the column NOT NULL would have needed a backfill
    #: whose safety rested on one dev database being empty on one day.
    declaration_id: int | None


@dataclass(frozen=True)
class LiveGateFacts:
    stage: str | None
    forward_resolved_signals: int
    #: ⚠ DISTINCT DECISION DATES, NOT SIGNALS. Twenty signals fired on one day
    #: are one decision date; `forward_resolved_signals` cannot tell that from
    #: twenty days of evidence. The narrow claim is that a distinct-date count
    #: cannot be inflated by same-day fan-out — NOT that the dates are
    #: statistically independent, which they are not, and which is what stage
    #: 5e-2's block bootstrap exists to handle.
    forward_decision_dates: int
    forward_days: int
    paper_closed_trades: int
    paper_days: int
    #: #2612 — how many times this version ARRIVED at each window anchor.
    #: Both windows are anchored on `max(promoted_at)` for their stage, which is
    #: only the true window start because a version reaches each stage AT MOST
    #: ONCE. That is not an assumption this module makes; it is enforced twice
    #: over, in `strategy_control_plane._NEXT_STAGE` (a DAG whose only edge into
    #: `forward_observation` leaves `historical_validated`, with no back-edge —
    #: `paused` goes only to `retired`) and by the partial UNIQUE index
    #: `idx_strategy_promotions_one_successor` on
    #: `(strategy_id, strategy_version, from_stage)`.
    #: ⚠ CARRIED AS A FACT, NOT ASSERTED, because if either enforcement is ever
    #: relaxed the failure is SILENT: `max` would quietly re-anchor on the second
    #: arrival, discarding the whole first observation period and restarting
    #: `forward_days` — the exact quantities #2599's contract-frozen floor reads.
    forward_observation_entries: int
    paper_enabled_entries: int
    funded_shadow_average_return_pct: Decimal | None
    unfunded_shadow_average_return_pct: Decimal | None
    shadow_alpha_pct: Decimal | None
    average_slippage_pct: Decimal | None
    cost_drift_pct: Decimal | None
    max_observed_drawdown_pct: Decimal | None
    reconciliation_order_count: int
    reconciliation_breach_count: int
    scan_age_seconds: Decimal | None
    active_owned_instrument_count: int
    oldest_owned_quote_age_seconds: Decimal | None
    halt_feed_age_seconds: Decimal | None
    broker_health_age_seconds: Decimal | None
    broker_health_active_block: bool | None
    paper_pnl_complete: bool
    completed_kill_drills: tuple[str, ...]
    auto_trading_enabled: bool
    live_trading_enabled: bool
    global_kill_active: bool
    active_execution_block_count: int


@dataclass(frozen=True)
class LiveGateReport:
    strategy_id: str
    strategy_version: str
    policy: LiveGatePolicy | None
    requested_capital: Decimal
    facts: LiveGateFacts
    refusal_codes: tuple[str, ...]
    forward_shadow_floor: ForwardShadowFloor | None = None

    @property
    def passed(self) -> bool:
        return not self.refusal_codes


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise StrategyControlError(f"{field} must be non-empty")


def _decimal(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _policy_from_row(row: dict[str, Any]) -> LiveGatePolicy:
    return LiveGatePolicy(
        live_gate_policy_id=int(row["live_gate_policy_id"]),
        strategy_id=str(row["strategy_id"]),
        strategy_version=str(row["strategy_version"]),
        min_forward_resolved_signals=int(row["min_forward_resolved_signals"]),
        min_forward_days=int(row["min_forward_days"]),
        min_paper_closed_trades=int(row["min_paper_closed_trades"]),
        min_paper_days=int(row["min_paper_days"]),
        max_reconciliation_age_seconds=int(row["max_reconciliation_age_seconds"]),
        min_shadow_alpha_pct=Decimal(str(row["min_shadow_alpha_pct"])),
        max_cost_drift_pct=Decimal(str(row["max_cost_drift_pct"])),
        max_average_slippage_pct=Decimal(str(row["max_average_slippage_pct"])),
        max_drawdown_pct=Decimal(str(row["max_drawdown_pct"])),
        max_scan_age_seconds=int(row["max_scan_age_seconds"]),
        max_quote_age_seconds=int(row["max_quote_age_seconds"]),
        max_broker_health_age_seconds=int(row["max_broker_health_age_seconds"]),
        max_live_capital=Decimal(str(row["max_live_capital"])),
        currency=str(row["currency"]),
        leverage=int(row["leverage"]),
        registered_at=row["registered_at"],
        declaration_id=None if row.get("declaration_id") is None else int(row["declaration_id"]),
    )


def load_live_gate_policy(
    conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str
) -> LiveGatePolicy | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT * FROM strategy_live_gate_policies WHERE strategy_id=%s AND strategy_version=%s",
            (strategy_id, strategy_version),
        )
        row = cur.fetchone()
    return None if row is None else _policy_from_row(row)


def register_live_gate_policy(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    min_forward_resolved_signals: int,
    min_forward_days: int,
    min_paper_closed_trades: int,
    min_paper_days: int,
    max_reconciliation_age_seconds: int,
    min_shadow_alpha_pct: Decimal,
    max_cost_drift_pct: Decimal,
    max_average_slippage_pct: Decimal,
    max_drawdown_pct: Decimal,
    max_scan_age_seconds: int,
    max_quote_age_seconds: int,
    max_broker_health_age_seconds: int,
    max_live_capital: Decimal,
    registered_by: str,
    reason: str,
) -> LiveGatePolicy:
    """Preregister immutable live thresholds before paper observation begins.

    ⚠ #2599 — THE FORWARD-SHADOW FLOOR IS NOT A PARAMETER HERE. It is read off
    the trial's frozen preregistration declaration and stored by reference, so
    there is no number to type differently. An operator who wants a different
    floor has to freeze a different declaration, which is a new strategy_version
    and a visible act.

    Three registration refusals, all fail-closed:

    - nothing frozen for this trial → there is no floor to bind to;
    - the frozen declaration is incoherent → it authorises nothing;
    - it declares `falsification_only` → a trial that said it cannot promote
      capital has no live gate to register, and registering one would be the
      declaration being quietly walked back.
    """
    frozen = load_preregistration(conn, strategy_id, strategy_version)
    if frozen is None:
        raise StrategyControlError(
            "no frozen preregistration declaration for this strategy version; live thresholds cannot bind a "
            "forward-shadow floor that was never declared (#2599)"
        )
    # ⚠ THE DIGEST, NOT ONLY THE COHERENCE. A declaration edited around the
    # immutability trigger (a superuser can disable one) can still satisfy every
    # coherence rule while carrying a different floor from the one that was
    # frozen. `record_holdout_access` already refuses on this; the asymmetry was
    # the live-gate path silently accepting what the research path rejects.
    if not frozen.digest_intact:
        raise StrategyControlError(
            "the frozen preregistration declaration no longer matches its own digest; it has been rewritten (#2599)"
        )
    declaration_problems = declaration_refusals(frozen.declaration)
    if declaration_problems:
        raise StrategyControlError(
            f"frozen preregistration declaration is not coherent: {', '.join(declaration_problems)}"
        )
    if frozen.declaration.prereg_purpose != "capital_candidate":
        raise StrategyControlError(
            "this trial declared itself falsification_only; it has no live gate to register (#2599)"
        )
    for value, field in (
        (strategy_id, "strategy_id"),
        (strategy_version, "strategy_version"),
        (registered_by, "registered_by"),
        (reason, "reason"),
    ):
        _require_text(value, field)
    positives = (
        min_forward_resolved_signals,
        min_forward_days,
        min_paper_closed_trades,
        min_paper_days,
        max_reconciliation_age_seconds,
        max_scan_age_seconds,
        max_quote_age_seconds,
        max_broker_health_age_seconds,
    )
    if any(value <= 0 for value in positives):
        raise StrategyControlError("live sample, duration and freshness limits must be positive")
    if max_cost_drift_pct < 0 or max_average_slippage_pct < 0:
        raise StrategyControlError("live drift limits must be non-negative")
    if not (Decimal("0") < max_drawdown_pct < Decimal("100")):
        raise StrategyControlError("max_drawdown_pct must be in (0, 100)")
    if max_live_capital <= 0:
        raise StrategyControlError("max_live_capital must be positive")

    lock_strategy_control(conn, strategy_id, strategy_version)
    if current_stage(conn, strategy_id, strategy_version) != "forward_observation":
        raise StrategyControlError("live thresholds must be preregistered during forward_observation")
    existing = conn.execute(
        "SELECT 1 FROM strategy_live_gate_policies WHERE strategy_id=%s AND strategy_version=%s",
        (strategy_id, strategy_version),
    ).fetchone()
    if existing is not None:
        raise StrategyControlError("live thresholds are immutable for a strategy version")
    paper = conn.execute(
        "SELECT 1 FROM strategy_promotions WHERE strategy_id=%s AND strategy_version=%s AND to_stage='paper_enabled'",
        (strategy_id, strategy_version),
    ).fetchone()
    if paper is not None:
        raise StrategyControlError("live thresholds cannot be changed after paper observation begins")
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO strategy_live_gate_policies (
                strategy_id, strategy_version, min_forward_resolved_signals,
                min_forward_days, min_paper_closed_trades, min_paper_days,
                max_reconciliation_age_seconds, min_shadow_alpha_pct,
                max_cost_drift_pct, max_average_slippage_pct, max_drawdown_pct,
                max_scan_age_seconds, max_quote_age_seconds,
                max_broker_health_age_seconds, max_live_capital, currency,
                leverage, registered_by, reason, declaration_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'USD',1,%s,%s,%s)
            RETURNING *
            """,
            (
                strategy_id,
                strategy_version,
                min_forward_resolved_signals,
                min_forward_days,
                min_paper_closed_trades,
                min_paper_days,
                max_reconciliation_age_seconds,
                min_shadow_alpha_pct,
                max_cost_drift_pct,
                max_average_slippage_pct,
                max_drawdown_pct,
                max_scan_age_seconds,
                max_quote_age_seconds,
                max_broker_health_age_seconds,
                max_live_capital,
                registered_by,
                reason,
                frozen.declaration_id,
            ),
        )
        row = cur.fetchone()
    assert row is not None
    return _policy_from_row(row)


def _age_seconds(now: datetime, value: datetime | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(max((now - value).total_seconds(), 0)))


def assess_live_gate(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    requested_capital: Decimal,
    now: datetime | None = None,
) -> LiveGateReport:
    """Derive one write-free promotion report from compact existing ledgers."""
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    policy = load_live_gate_policy(conn, strategy_id, strategy_version)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
              max(promoted_at) FILTER (WHERE to_stage='forward_observation') AS forward_at,
              max(promoted_at) FILTER (WHERE to_stage='paper_enabled') AS paper_at,
              -- #2612 — the arrival counts these two anchors rest on. Same scan,
              -- so measuring the assumption costs nothing.
              count(*) FILTER (WHERE to_stage='forward_observation') AS forward_entries,
              count(*) FILTER (WHERE to_stage='paper_enabled') AS paper_entries
            FROM strategy_promotions
            WHERE strategy_id=%s AND strategy_version=%s
            """,
            (strategy_id, strategy_version),
        )
        stages = cur.fetchone()
        assert stages is not None
        forward_at = stages["forward_at"]
        paper_at = stages["paper_at"]

        cur.execute(
            """
            SELECT
              count(*) FILTER (WHERE o.gross_return_pct IS NOT NULL
                                AND s.created_at >= %(forward_at)s
                                AND s.created_at < %(paper_at)s
                                AND s.signal_bar_date >= %(forward_date)s) AS forward_resolved,
              -- #2599 — the SAME predicate, counted by distinct decision date.
              -- ⚠ Deliberately not a looser population: an unresolved signal is
              -- not evidence, so it is not a decision date either, and counting
              -- one would let a strategy clear a forward floor on looks that
              -- never produced an outcome.
              count(DISTINCT s.signal_bar_date) FILTER (WHERE o.gross_return_pct IS NOT NULL
                                AND s.created_at >= %(forward_at)s
                                AND s.created_at < %(paper_at)s
                                AND s.signal_bar_date >= %(forward_date)s) AS forward_decision_dates,
              count(*) FILTER (WHERE d.mode='paper' AND t.status='closed'
                                AND s.created_at >= %(paper_at)s) AS paper_closed,
              avg(o.gross_return_pct) FILTER (WHERE d.mode='paper' AND fd.verdict='allocated'
                                AND s.created_at >= %(paper_at)s) AS funded_avg,
              avg(o.gross_return_pct) FILTER (WHERE s.created_at >= %(paper_at)s
                                AND fd.verdict IS DISTINCT FROM 'allocated') AS unfunded_avg,
              avg(((exec.average_price-s.fill_price)/NULLIF(s.fill_price,0))*100)
                    FILTER (WHERE d.mode='paper' AND s.created_at >= %(paper_at)s) AS average_slippage,
              sum(pf.stressed_cost_amount) FILTER (WHERE d.mode='paper' AND t.status='closed'
                                AND s.created_at >= %(paper_at)s) AS expected_cost,
              sum(coalesce(fee.actual_fees,0)) FILTER (WHERE d.mode='paper' AND t.status='closed'
                                AND s.created_at >= %(paper_at)s) AS actual_cost,
              bool_and(fee.fees_complete) FILTER (WHERE d.mode='paper' AND t.status='closed'
                                AND s.created_at >= %(paper_at)s) AS fees_complete,
              sum(fd.amount) FILTER (WHERE d.mode='paper' AND t.status='closed'
                                AND s.created_at >= %(paper_at)s) AS closed_capital
            FROM strategy_signals s
            LEFT JOIN strategy_outcomes o ON o.signal_id=s.signal_id
              AND o.rule_set_version=%(outcome_version)s
              AND o.input_rule_set_version=%(input_version)s
            LEFT JOIN strategy_funding_decisions fd ON fd.signal_id=s.signal_id
            LEFT JOIN strategy_deployments d ON d.deployment_id=fd.deployment_id
            LEFT JOIN strategy_trades t ON t.funding_decision_id=fd.funding_decision_id
            LEFT JOIN strategy_entry_preflights pf ON pf.signal_id=s.signal_id
            LEFT JOIN LATERAL (
                SELECT sum(e.opening_units*e.average_price)/NULLIF(sum(e.opening_units),0) AS average_price
                FROM strategy_trade_orders sto
                JOIN strategy_order_position_executions e ON e.order_id=sto.order_id
                WHERE sto.strategy_trade_id=t.strategy_trade_id AND sto.purpose='entry'
                  AND e.opening_units>0 AND e.average_price>0
            ) exec ON true
            LEFT JOIN LATERAL (
                SELECT sum(te.fees_usd) AS actual_fees,
                       bool_and(te.fees_usd IS NOT NULL) AND count(*)>0 AS fees_complete
                FROM strategy_position_ownership own
                JOIN trade_events te ON te.position_id=own.broker_position_id
                                      AND te.event_kind='close'
                WHERE own.strategy_trade_id=t.strategy_trade_id
            ) fee ON true
            WHERE s.strategy_id=%(strategy_id)s AND s.strategy_version=%(strategy_version)s
              AND s.signal_kind='entry' AND s.verdict='fired'
            """,
            {
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "forward_at": forward_at or observed_at,
                "paper_at": paper_at or observed_at,
                "forward_date": (forward_at or observed_at).date(),
                "outcome_version": OUTCOME_RULE_SET_VERSION,
                "input_version": QUARANTINE_RULE_SET_VERSION,
            },
        )
        evidence = cur.fetchone()
        assert evidence is not None

        cur.execute(
            """
            SELECT rs.max_drawdown_pct
            FROM strategy_deployments d
            JOIN strategy_paper_deployment_risk_state rs ON rs.deployment_id=d.deployment_id
            WHERE d.strategy_id=%s AND d.strategy_version=%s AND d.mode='paper'
            """,
            (strategy_id, strategy_version),
        )
        risk_state = cur.fetchone()

        cur.execute(
            """
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE rs.order_id IS NULL
                     OR rs.state NOT IN ('resolved','rejected')
                     OR rs.first_unresolved_at + make_interval(secs => %(max_age)s) <
                        coalesce(rs.reconciled_at, %(now)s)) AS breaches
            FROM strategy_deployments d
            JOIN strategy_funding_decisions fd ON fd.deployment_id=d.deployment_id AND fd.verdict='allocated'
            JOIN strategy_trades t ON t.funding_decision_id=fd.funding_decision_id
            JOIN strategy_trade_orders sto ON sto.strategy_trade_id=t.strategy_trade_id
            LEFT JOIN strategy_order_reconciliation_state rs ON rs.order_id=sto.order_id
            WHERE d.strategy_id=%(strategy_id)s AND d.strategy_version=%(strategy_version)s AND d.mode='paper'
            """,
            {
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "max_age": policy.max_reconciliation_age_seconds if policy else 1,
                "now": observed_at,
            },
        )
        reconciliation = cur.fetchone()
        assert reconciliation is not None

        cur.execute(
            """
            SELECT max(w.updated_at) AS scan_at,
                   max(h.fetched_at) AS halt_at,
                   count(owned_instruments.instrument_id) AS active_owned_instruments,
                   min(q.quoted_at) AS oldest_quote_at
            FROM strategy_scan_watermark w
            LEFT JOIN strategy_halt_feed_state h ON h.source='nasdaq_trader_rss'
            LEFT JOIN (
                SELECT DISTINCT t.instrument_id
                FROM strategy_deployments d
                JOIN strategy_funding_decisions fd ON fd.deployment_id=d.deployment_id
                JOIN strategy_trades t ON t.funding_decision_id=fd.funding_decision_id
                JOIN strategy_position_ownership own
                  ON own.strategy_trade_id=t.strategy_trade_id AND own.status='active'
                WHERE d.strategy_id=%s AND d.strategy_version=%s AND d.mode='paper'
            ) owned_instruments ON true
            LEFT JOIN quotes q ON q.instrument_id=owned_instruments.instrument_id
            WHERE w.strategy_id=%s AND w.strategy_version=%s
            """,
            (strategy_id, strategy_version, strategy_id, strategy_version),
        )
        freshness = cur.fetchone()
        assert freshness is not None

        cur.execute("SELECT active, updated_at FROM strategy_execution_blocks WHERE source='broker_availability'")
        broker_health = cur.fetchone()
        cur.execute(
            """
            SELECT DISTINCT ON (drill_kind) drill_kind, passed
            FROM strategy_kill_drill_events
            WHERE live_gate_policy_id=%s
            ORDER BY drill_kind, run_at DESC, kill_drill_event_id DESC
            """,
            (policy.live_gate_policy_id if policy else -1,),
        )
        drill_rows = cur.fetchall()
        completed_drills = tuple(sorted(str(row["drill_kind"]) for row in drill_rows if row["passed"]))
        cur.execute("SELECT count(*) AS count FROM strategy_execution_blocks WHERE active")
        active_block_row = cur.fetchone()
        assert active_block_row is not None
        active_blocks = int(active_block_row["count"])

    entry_block = load_entry_block_state(conn)
    pnl = load_owned_pnl(conn, versions=[strategy_version]).get((strategy_id, strategy_version))
    funded_avg = _decimal(evidence["funded_avg"])
    unfunded_avg = _decimal(evidence["unfunded_avg"])
    shadow_alpha = funded_avg - unfunded_avg if funded_avg is not None and unfunded_avg is not None else None
    expected_cost = _decimal(evidence["expected_cost"])
    actual_cost = _decimal(evidence["actual_cost"])
    closed_capital = _decimal(evidence["closed_capital"])
    cost_drift = None
    if (
        evidence["fees_complete"] is True
        and expected_cost is not None
        and actual_cost is not None
        and closed_capital is not None
        and closed_capital > 0
    ):
        cost_drift = ((actual_cost - expected_cost) / closed_capital) * Decimal("100")

    facts = LiveGateFacts(
        stage=current_stage(conn, strategy_id, strategy_version),
        forward_resolved_signals=int(evidence["forward_resolved"] or 0),
        forward_decision_dates=int(evidence["forward_decision_dates"] or 0),
        forward_days=max(((paper_at or observed_at) - forward_at).days, 0) if forward_at else 0,
        paper_closed_trades=int(evidence["paper_closed"] or 0),
        paper_days=max((observed_at - paper_at).days, 0) if paper_at else 0,
        forward_observation_entries=int(stages["forward_entries"]),
        paper_enabled_entries=int(stages["paper_entries"]),
        funded_shadow_average_return_pct=funded_avg,
        unfunded_shadow_average_return_pct=unfunded_avg,
        shadow_alpha_pct=shadow_alpha,
        average_slippage_pct=_decimal(evidence["average_slippage"]),
        cost_drift_pct=cost_drift,
        max_observed_drawdown_pct=_decimal(risk_state["max_drawdown_pct"] if risk_state else None),
        reconciliation_order_count=int(reconciliation["total"]),
        reconciliation_breach_count=int(reconciliation["breaches"]),
        scan_age_seconds=_age_seconds(observed_at, freshness["scan_at"]),
        active_owned_instrument_count=int(freshness["active_owned_instruments"]),
        oldest_owned_quote_age_seconds=_age_seconds(observed_at, freshness["oldest_quote_at"]),
        halt_feed_age_seconds=_age_seconds(observed_at, freshness["halt_at"]),
        broker_health_age_seconds=_age_seconds(observed_at, broker_health["updated_at"] if broker_health else None),
        broker_health_active_block=bool(broker_health["active"]) if broker_health else None,
        paper_pnl_complete=bool(pnl and pnl.complete),
        completed_kill_drills=completed_drills,
        auto_trading_enabled=entry_block.auto_trading_enabled,
        live_trading_enabled=entry_block.live_trading_enabled,
        global_kill_active=entry_block.global_kill_active,
        active_execution_block_count=active_blocks,
    )

    # ⚠ THE FLOOR IS READ OFF THE DECLARATION THE *POLICY* POINTS AT, never off
    # whatever declaration currently exists for the trial. A policy registered
    # against one declaration must keep answering to that one — otherwise
    # freezing a second, laxer declaration would retro-loosen a policy that is
    # supposed to be immutable.
    #
    # ⚠⚠ #2634 WIDENED THAT TO THE POLICY'S DECLARATION *CHAIN*, and the reason
    # is that the narrow form re-created the wedge one level up. A supersession
    # inserts a new revision, so `policy.declaration_id` stops equalling the
    # current one — and because the policy is itself immutable and cannot be
    # re-registered, the trial would sit at `forward_shadow_floor_missing`
    # forever. Honouring an ancestor cannot loosen anything: a supersession may
    # not change the purpose, the stamps or either floor (`sql/337`,
    # `prereg_contract.SUPERSESSION_MUTABLE_FIELDS`), so every revision in a
    # chain carries identical terms. A genuinely laxer declaration cannot be a
    # supersession at all — it needs a new `strategy_version`, which is a
    # different trial and a different policy. The digest and coherence checks
    # below still run against the CURRENT revision.
    frozen = load_preregistration(conn, strategy_id, strategy_version)
    declaration: PreregDeclaration | None = None
    digest_intact = True
    coherent = True
    if frozen is not None and policy is not None and policy.declaration_id in frozen.chain_declaration_ids:
        declaration = frozen.declaration
        digest_intact = frozen.digest_intact
        # ⚠ RE-CHECKED ON EVERY ASSESSMENT, not only at registration. The policy
        # is immutable but the structural-refusal POLICY VERSION is not, and a
        # declaration frozen under a superseded one stops authorising anything
        # the moment it moves — which is exactly what `record_holdout_access`
        # does on the research side. Checking only the digest here left the two
        # enforcement points disagreeing on the one case the "no
        # re-interpretation" rule exists for.
        coherent = not declaration_refusals(declaration)

    return LiveGateReport(
        strategy_id,
        strategy_version,
        policy,
        requested_capital,
        facts,
        live_gate_refusals(
            purpose=registered_strategy_purpose(strategy_id),
            policy=policy,
            declaration=declaration,
            declaration_digest_intact=digest_intact,
            declaration_coherent=coherent,
            facts=facts,
            requested_capital=requested_capital,
        ),
        None if declaration is None or not digest_intact or not coherent else declaration.forward_shadow,
    )


def live_gate_refusals(
    *,
    purpose: str | None,
    policy: LiveGatePolicy | None,
    declaration: PreregDeclaration | None,
    facts: LiveGateFacts,
    requested_capital: Decimal,
    declaration_digest_intact: bool = True,
    declaration_coherent: bool = True,
) -> tuple[str, ...]:
    """Every reason this strategy may not take live capital. Empty means none.

    Pure — reads no database — so the whole table is exercisable without
    Postgres. Extracted from ``assess_live_gate`` by #2599, which needed the
    three forward-shadow codes covered by pure-logic tests and found the
    assembly welded to five cursors.

    ⚠ ORDER IS PRESERVED FROM THE ORIGINAL and the de-duplication at the end is
    kept: `assess_live_gate` returned `dict.fromkeys(refusals)`, and a caller
    reading `refusal_codes[0]` as "the main reason" would see a different code
    if this reordered.
    """
    refusals: list[str] = []
    if purpose == "harness_validation":
        refusals.append("harness_validation_only")
    elif purpose != "capital_candidate":
        refusals.append("strategy_not_capital_candidate")
    if policy is None:
        refusals.append("live_gate_policy_missing")
    if facts.stage != "paper_enabled":
        refusals.append("paper_stage_required")
    # #2612 — TWO CODES, NOT ONE: a second `forward_observation` arrival corrupts
    # `forward_days` + `forward_decision_dates` (what #2599's frozen floor reads),
    # a second `paper_enabled` arrival corrupts every paper metric below. They are
    # different emergencies and collapsing them would hide one, per this
    # function's own three-codes convention above.
    #
    # ⚠ These do not breach the ORDER contract in the docstring. Both anchors are
    # single-entry by construction (see `LiveGateFacts`), so on every REACHABLE
    # input these counts are 0 or 1 and neither code is emitted — the position
    # cannot move `refusal_codes[0]` for any input the stage machine can produce.
    # They sit here, rather than appended at the end, because a corrupt window
    # invalidates the forward and paper clauses that follow: the floor checks
    # below can PASS spuriously on a spliced window, so the reason this strategy
    # is refused must not be buried beneath them.
    if facts.forward_observation_entries > 1:
        refusals.append("forward_window_ambiguous")
    if facts.paper_enabled_entries > 1:
        refusals.append("paper_window_ambiguous")
    if requested_capital <= 0:
        refusals.append("live_capital_must_be_positive")
    # #2599's forward-shadow floor. ⚠ DUAL ENFORCEMENT, NOT REPLACEMENT: the
    # operator-registered `min_forward_*` below still bind, and these two are
    # the contract-frozen floor from the candidate's own power calculation.
    # Both must pass.
    if declaration is None:
        refusals.append("forward_shadow_floor_missing")
    elif not declaration_digest_intact:
        # ⚠ THREE DISTINCT CODES, not one. "No floor was frozen", "the frozen
        # floor has been rewritten" and "the frozen floor was computed under a
        # policy that has since moved" are three different operator
        # emergencies, and collapsing them would hide two of them.
        refusals.append("declaration_digest_mismatch")
    elif not declaration_coherent:
        refusals.append("declaration_no_longer_coherent")
    else:
        floor = declaration.forward_shadow
        if facts.forward_decision_dates < floor.min_independent_decision_dates:
            refusals.append("forward_decision_dates_insufficient")
        # ⚠ `forward_days` comes from `timedelta.days`, which TRUNCATES, so this
        # requires 7M fully elapsed days. Truncation makes the bound stricter,
        # which is the fail-closed direction.
        if facts.forward_days < 7 * floor.min_calendar_weeks:
            refusals.append("forward_calendar_weeks_insufficient")
    if policy is not None:
        checks = (
            (facts.forward_resolved_signals >= policy.min_forward_resolved_signals, "forward_sample_insufficient"),
            (facts.forward_days >= policy.min_forward_days, "forward_duration_insufficient"),
            (facts.paper_closed_trades >= policy.min_paper_closed_trades, "paper_sample_insufficient"),
            (facts.paper_days >= policy.min_paper_days, "paper_duration_insufficient"),
            (
                facts.shadow_alpha_pct is not None and facts.shadow_alpha_pct >= policy.min_shadow_alpha_pct,
                "shadow_alpha_below_policy",
            ),
            (
                facts.cost_drift_pct is not None and facts.cost_drift_pct <= policy.max_cost_drift_pct,
                "cost_drift_unavailable_or_high",
            ),
            (
                facts.average_slippage_pct is not None
                and abs(facts.average_slippage_pct) <= policy.max_average_slippage_pct,
                "slippage_unavailable_or_high",
            ),
            (
                facts.max_observed_drawdown_pct is not None
                and facts.max_observed_drawdown_pct <= policy.max_drawdown_pct,
                "drawdown_unavailable_or_high",
            ),
            (
                facts.reconciliation_order_count > 0 and facts.reconciliation_breach_count == 0,
                "reconciliation_slo_failed",
            ),
            (
                facts.scan_age_seconds is not None and facts.scan_age_seconds <= policy.max_scan_age_seconds,
                "scan_health_stale",
            ),
            (
                facts.active_owned_instrument_count == 0
                or (
                    facts.oldest_owned_quote_age_seconds is not None
                    and facts.oldest_owned_quote_age_seconds <= policy.max_quote_age_seconds
                ),
                "quote_health_stale",
            ),
            (
                facts.halt_feed_age_seconds is not None and facts.halt_feed_age_seconds <= policy.max_scan_age_seconds,
                "halt_feed_health_stale",
            ),
            (
                facts.broker_health_active_block is False
                and facts.broker_health_age_seconds is not None
                and facts.broker_health_age_seconds <= policy.max_broker_health_age_seconds,
                "broker_health_unavailable_or_stale",
            ),
            (requested_capital <= policy.max_live_capital, "live_capital_exceeds_preregistered_cap"),
            (set(REQUIRED_KILL_DRILLS).issubset(facts.completed_kill_drills), "kill_drills_incomplete"),
        )
        refusals.extend(code for passed, code in checks if not passed)
    if not facts.paper_pnl_complete:
        refusals.append("paper_pnl_incomplete")
    if not facts.auto_trading_enabled:
        refusals.append("automatic_trading_disabled")
    if not facts.live_trading_enabled:
        refusals.append("global_live_trading_disabled")
    if facts.global_kill_active:
        refusals.append("global_kill_active")
    if facts.active_execution_block_count:
        refusals.append("strategy_execution_block_active")
    # The measured eToro cost contract still supplies undocumented/stale values
    # and the only strategy writer intentionally refuses real credentials.
    # Keep this explicit: every evidence gate can turn green without silently
    # making real broker I/O reachable.
    refusals.append("live_strategy_broker_contract_not_validated")
    return tuple(dict.fromkeys(refusals))


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


def record_live_promotion_attempt(
    conn: psycopg.Connection[Any],
    *,
    report: LiveGateReport,
    assessed_by: str,
    reason: str,
) -> int:
    """Append a refused operator attempt; passing is intentionally unavailable.

    The live broker contract has not passed its measured prerequisite, so this
    function currently cannot create a promotion or live deployment.  Once the
    contract is validated, the same report hash becomes the pinned authority
    for an atomic promotion implementation.
    """
    _require_text(assessed_by, "assessed_by")
    _require_text(reason, "reason")
    if report.passed:
        raise StrategyControlError("live promotion writer is unavailable until the broker contract is validated")
    payload = _jsonable({"facts": asdict(report.facts), "gate_version": LIVE_GATE_VERSION})
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    row = conn.execute(
        """
        INSERT INTO strategy_live_gate_assessments (
            live_gate_policy_id, strategy_id, strategy_version,
            requested_capital, passed, refusal_codes,
            evidence_sha256, evidence_json, assessed_by, reason
        ) VALUES (%s,%s,%s,%s,false,%s,%s,%s::jsonb,%s,%s)
        RETURNING live_gate_assessment_id
        """,
        (
            report.policy.live_gate_policy_id if report.policy is not None else None,
            report.strategy_id,
            report.strategy_version,
            report.requested_capital,
            list(report.refusal_codes),
            digest,
            canonical,
            assessed_by,
            reason,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def run_kill_drill(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    drill_kind: DrillKind,
    run_by: str,
    reason: str,
) -> int:
    """Make one simulated health breach visible, prove entry blocking, restore.

    The active block is committed before the assertion, so other workers see
    it.  Owned-position risk reduction has no dependency on execution blocks;
    that structural contract is integration-tested with the manager itself.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyControlError("kill drills require an idle connection")
    if drill_kind not in REQUIRED_KILL_DRILLS:
        raise StrategyControlError(f"unknown kill drill {drill_kind!r}")
    _require_text(run_by, "run_by")
    _require_text(reason, "reason")
    policy = load_live_gate_policy(conn, strategy_id, strategy_version)
    if policy is None:
        conn.rollback()
        raise StrategyControlError("kill drill requires a preregistered live gate policy")
    source = f"drill:{drill_kind}"
    drill_reason = f"kill drill {drill_kind}: {reason}"
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM strategy_execution_blocks WHERE source=%s", (source,))
        prior = cur.fetchone()
    conn.rollback()
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO strategy_execution_blocks (source,active,reason,blocked_at,cleared_at,updated_at)
            VALUES (%s,true,%s,now(),NULL,now())
            ON CONFLICT (source) DO UPDATE SET active=true, reason=EXCLUDED.reason,
              blocked_at=now(), cleared_at=NULL, updated_at=now()
            """,
            (source, drill_reason),
        )
    entry_block_observed = False
    state_restored = False
    try:
        active_drill_row = conn.execute(
            "SELECT active FROM strategy_execution_blocks WHERE source=%s", (source,)
        ).fetchone()
        entry_block_state = load_entry_block_state(conn)
        entry_block_observed = (
            bool(active_drill_row and active_drill_row[0])
            and entry_block_state.new_entries_blocked
            and drill_reason in entry_block_state.execution_block_reasons
        )
    finally:
        # A drill must not strand its synthetic source even when its assertion
        # path fails. Roll back the read transaction, then restore the exact
        # prior row in a separate committed transaction.
        conn.rollback()
        with conn.transaction():
            if prior is None:
                conn.execute("DELETE FROM strategy_execution_blocks WHERE source=%s", (source,))
            else:
                conn.execute(
                    """
                    UPDATE strategy_execution_blocks
                    SET active=%s, reason=%s, blocked_at=%s, cleared_at=%s, updated_at=%s
                    WHERE source=%s
                    """,
                    (
                        prior["active"],
                        prior["reason"],
                        prior["blocked_at"],
                        prior["cleared_at"],
                        prior["updated_at"],
                        source,
                    ),
                )
            restored = conn.execute(
                "SELECT active,reason,blocked_at,cleared_at,updated_at FROM strategy_execution_blocks WHERE source=%s",
                (source,),
            ).fetchone()
            if prior is None:
                state_restored = restored is None
            else:
                assert restored is not None
                state_restored = tuple(restored) == (
                    prior["active"],
                    prior["reason"],
                    prior["blocked_at"],
                    prior["cleared_at"],
                    prior["updated_at"],
                )
    with conn.transaction():
        event = conn.execute(
            """
            INSERT INTO strategy_kill_drill_events (
                live_gate_policy_id,drill_kind,entry_block_observed,state_restored,run_by,reason
            ) VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING kill_drill_event_id
            """,
            (
                policy.live_gate_policy_id,
                drill_kind,
                entry_block_observed,
                state_restored,
                run_by,
                reason,
            ),
        ).fetchone()
    assert event is not None
    return int(event[0])


__all__ = [
    "LIVE_GATE_VERSION",
    "REQUIRED_KILL_DRILLS",
    "LiveGateFacts",
    "LiveGatePolicy",
    "LiveGateReport",
    "assess_live_gate",
    "load_live_gate_policy",
    "record_live_promotion_attempt",
    "live_gate_refusals",
    "register_live_gate_policy",
    "run_kill_drill",
]
