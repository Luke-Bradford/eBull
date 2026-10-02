"""Bounded recurring paper-strategy operating loop (#2450).

The cycle reconciles uncertain orders, refreshes five current health blocks,
repairs/manages exact owned positions, then evaluates the strongest bounded
slice of the complete current positive-forecast set. Health rows are updated in
place and unchanged position polls add no rows.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerProvider
from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import COST_MODEL_ID
from app.services.engine_pot_risk import (
    advance_deployment_drawdown,
    advance_pot_drawdown,
    load_stored_pot_drawdown,
    observe_deployment_nav,
    observe_pot_nav,
    record_deployment_refusal,
)
from app.services.price_masked_bars import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_core_arc_sql import core_arm_authorised, core_arm_joins
from app.services.strategy_engine_capital import EngineCapitalObservationError
from app.services.strategy_forecast_outcome_resolution import RESOLVER_VERSION as FORECAST_OUTCOME_RESOLVER_VERSION
from app.services.strategy_manifest import DEMO_TRIAL_STRATEGY_IDS, STRATEGY_MANIFEST
from app.services.strategy_opportunity_forecast import FORECAST_POLICY_VERSION
from app.services.strategy_opportunity_ranker import (
    RankableOpportunity,
    persist_ranking_batch,
    rank_positive_opportunities,
)
from app.services.strategy_order_reconciliation import (
    count_unresolved_order_identity,
    enforce_reconciliation_slo,
    reconcile_backlog,
)
from app.services.strategy_paper_executor import execute_fired_paper_signal
from app.services.strategy_position_manager import manage_owned_position

logger = logging.getLogger(__name__)

# The batch that SELECTS the positions to manage.  ⚠⚠ This query, not
# ``manage_owned_position``, is what decides whether a core position is managed
# at all: the manager is never called for a trade this does not return.  Widening
# the worker and not the dispatcher looks complete at every level a diff review
# inspects, and is the defect this slice exists to avoid (#2603, and #2437's R4
# pattern one level up).
#
# The core arm uses the AUTHORISED predicate -- this path acts on what it loads,
# so a non-actionable intent or a non-paper mandate must not load, exactly as a
# non-paper deployment does not load on the signal arm.
#
# Signal-arm behaviour is unchanged.  The former INNER ``fd``/``d`` pair filtered
# by ``d.mode='paper'``; the LEFT pair witnessed by ``d.deployment_id IS NOT
# NULL`` admits precisely the same rows, because the FK makes ``fd`` total for a
# non-null ``funding_decision_id`` and ``mode`` now filters inside the ON clause.
_OWNED_BATCH_SQL = f"""
        WITH active AS (
          SELECT own.strategy_trade_id,own.broker_position_id,
                 row_number() OVER (ORDER BY own.ownership_id) AS ordinal,
                 count(*) OVER () AS total
          FROM strategy_position_ownership own
          JOIN strategy_trades t ON t.strategy_trade_id=own.strategy_trade_id
          LEFT JOIN strategy_funding_decisions fd ON fd.funding_decision_id=t.funding_decision_id
          LEFT JOIN strategy_deployments d ON d.deployment_id=fd.deployment_id AND d.mode='paper'
{core_arm_joins("t")}
          WHERE own.status='active'
            AND (
              d.deployment_id IS NOT NULL
              OR {core_arm_authorised("t")}
            )
        )
        SELECT strategy_trade_id,broker_position_id
        FROM active
        ORDER BY mod(ordinal-1-%s+total,total)
        LIMIT %s
"""


@dataclass(frozen=True)
class StrategyPaperCycleResult:
    reconciled_orders: int
    managed_positions: int
    evaluated_signals: int
    # ``None`` when the health refresh itself failed (#3546): the blocks were not re-read.
    active_health_blocks: int | None
    # ``PositionManagerResult.state`` -> count: what management DID, where
    # ``managed_positions`` counts visits only.
    management: Mapping[str, int] = field(default_factory=dict)
    # Stage -> items whose unmodelled failure was contained (#3546 gap B).
    errors: Mapping[str, int] = field(default_factory=dict)
    # Reason -> entries deferred with nothing written (#3546 gap F, ``PAPER_ENTRY_DEFERRALS``);
    # each is also counted in ``evaluated_signals`` and is re-selected next cycle.
    deferred: Mapping[str, int] = field(default_factory=dict)


def _load_ranked_opportunities(
    conn: psycopg.Connection[Any],
    *,
    strategy_versions: Sequence[str],
    observed_at: datetime,
) -> list[RankableOpportunity]:
    """Load the complete current set, then rank without surrogate identities."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT s.signal_id,f.forecast_id,s.strategy_id,s.strategy_version,s.instrument_id,
                   s.signal_bar_date,f.side,f.horizon_market_days,f.setup_version,
                   f.exit_policy_version,f.decided_at,
                   f.conservative_net_expectancy_pct
            FROM strategy_signals s
            JOIN strategy_deployments d
              ON d.strategy_id=s.strategy_id AND d.strategy_version=s.strategy_version
             AND d.mode='paper' AND d.enabled
            JOIN strategy_execution_policies p ON p.deployment_id=d.deployment_id
            JOIN strategy_opportunity_forecasts f ON f.signal_id=s.signal_id
            JOIN strategy_forecast_calibrations c ON c.calibration_id=f.calibration_id
            JOIN LATERAL (
              SELECT policy_id,max_assessment_age_days
              FROM strategy_forecast_assessment_policies
              WHERE effective_from <= %(observed_at)s
              ORDER BY effective_from DESC LIMIT 1
            ) assessment_policy ON true
            JOIN strategy_forecast_assessment_current current_assessment
             ON current_assessment.policy_id=assessment_policy.policy_id
             AND current_assessment.strategy_id=s.strategy_id
             AND current_assessment.strategy_version=s.strategy_version
             AND current_assessment.forecast_policy_version=f.forecast_policy_version
             AND current_assessment.model_version=c.model_version
             AND current_assessment.calibration_id=c.calibration_id
             AND current_assessment.setup_version=f.setup_version
             AND current_assessment.exit_policy_version=f.exit_policy_version
             AND current_assessment.resolver_version=%(outcome_resolver)s
             AND current_assessment.input_rule_set_version=%(input_rule_set)s
             AND current_assessment.checked_at >= %(observed_at)s
                 - assessment_policy.max_assessment_age_days * interval '1 day'
             -- Tolerate only scheduler/allocator clock skew inside one cycle;
             -- this is not permission to consume future assessment evidence.
             AND current_assessment.checked_at <= %(observed_at)s + interval '5 seconds'
            JOIN strategy_forecast_assessments prospective_assessment
             ON prospective_assessment.assessment_id=current_assessment.assessment_id
             AND prospective_assessment.policy_id=current_assessment.policy_id
             AND prospective_assessment.strategy_id=current_assessment.strategy_id
             AND prospective_assessment.strategy_version=current_assessment.strategy_version
             AND prospective_assessment.forecast_policy_version=current_assessment.forecast_policy_version
             AND prospective_assessment.model_version=current_assessment.model_version
             AND prospective_assessment.calibration_id=current_assessment.calibration_id
             AND prospective_assessment.setup_version=current_assessment.setup_version
             AND prospective_assessment.exit_policy_version=current_assessment.exit_policy_version
             AND prospective_assessment.resolver_version=current_assessment.resolver_version
             AND prospective_assessment.input_rule_set_version=current_assessment.input_rule_set_version
             AND prospective_assessment.passed
            JOIN LATERAL (
              SELECT max(sp.promoted_at) AS paper_at
              FROM strategy_promotions sp
              WHERE sp.strategy_id=s.strategy_id
                AND sp.strategy_version=s.strategy_version
                AND sp.to_stage='paper_enabled'
            ) promotion ON s.created_at >= promotion.paper_at
            LEFT JOIN strategy_funding_decisions fd ON fd.signal_id=s.signal_id
            WHERE s.signal_kind='entry' AND s.verdict='fired'
              AND s.strategy_version=ANY(%(versions)s) AND fd.signal_id IS NULL
              AND f.forecast_policy_version=%(forecast_policy)s
              AND f.cost_model_id=%(cost_model)s
              AND c.passed
              AND c.holdout_end < f.decided_at::date
              AND f.decided_at <= %(observed_at)s
              AND f.valid_through >= %(observed_at)s
              AND f.conservative_net_expectancy_pct > 0
            """,
            {
                "versions": list(strategy_versions),
                "forecast_policy": FORECAST_POLICY_VERSION,
                "cost_model": COST_MODEL_ID,
                "observed_at": observed_at,
                "outcome_resolver": FORECAST_OUTCOME_RESOLVER_VERSION,
                "input_rule_set": QUARANTINE_RULE_SET_VERSION,
            },
        )
        rows = cur.fetchall()
    # End the read transaction before pure validation/ranking can fail. A
    # duplicate economic identity must fail closed without leaving this pooled
    # connection in a transaction or touching already-committed health state.
    conn.commit()
    return rank_positive_opportunities(
        [
            RankableOpportunity(
                signal_id=int(row["signal_id"]),
                forecast_id=int(row["forecast_id"]),
                strategy_id=str(row["strategy_id"]),
                strategy_version=str(row["strategy_version"]),
                instrument_id=int(row["instrument_id"]),
                signal_bar_date=row["signal_bar_date"],
                side=str(row["side"]),
                horizon_market_days=int(row["horizon_market_days"]),
                setup_version=str(row["setup_version"]),
                exit_policy_version=str(row["exit_policy_version"]),
                decided_at=row["decided_at"],
                conservative_net_expectancy_pct=Decimal(str(row["conservative_net_expectancy_pct"])),
            )
            for row in rows
        ]
    )


def _set_block(conn: psycopg.Connection[Any], *, source: str, active: bool, reason: str) -> None:
    conn.execute(
        """
        INSERT INTO strategy_execution_blocks (source,active,reason,blocked_at,cleared_at,updated_at)
        VALUES (%s,%s,%s,CASE WHEN %s THEN now() ELSE NULL END,
                CASE WHEN %s THEN NULL ELSE now() END,now())
        ON CONFLICT (source) DO UPDATE SET
          active=EXCLUDED.active, reason=EXCLUDED.reason,
          blocked_at=CASE
            WHEN EXCLUDED.active AND NOT strategy_execution_blocks.active THEN now()
            WHEN EXCLUDED.active THEN strategy_execution_blocks.blocked_at
            ELSE NULL END,
          cleared_at=CASE WHEN EXCLUDED.active THEN NULL ELSE now() END,
          updated_at=now()
        """,
        (source, active, reason, active, active),
    )


def _pot_drawdown_block(
    conn: psycopg.Connection[Any], *, risk: BrokerAccountRiskSnapshot | None, limit: Decimal
) -> tuple[bool, str]:
    """The ``drawdown`` block's verdict from the engine pot (#3541). Fails closed.

    ``risk`` is ``None`` when the probe failed or is stale. A probe older than the stored
    state defers to the stored figure: it is newer evidence, and this tick decides nothing
    on its own snapshot.
    """
    if risk is None:
        return True, "engine pot drawdown unavailable because broker risk is unavailable or stale"
    try:
        with conn.transaction():
            drawdown = advance_pot_drawdown(conn, observe_pot_nav(conn, risk))
    except EngineCapitalObservationError as exc:
        return True, f"engine pot drawdown unobservable: {exc.reason_code} ({exc})"
    if drawdown == "engine_pot_risk_stale":
        stored = load_stored_pot_drawdown(conn)
        if stored is None:  # pragma: no cover - stale implies a stored row
            return True, "engine pot drawdown unavailable"
        drawdown = stored
    elif isinstance(drawdown, str):
        return True, f"engine pot drawdown refused: {drawdown}"
    if drawdown >= limit:
        return True, f"engine pot drawdown {drawdown:.4f}% reached the configured paper limit {limit}%"
    return False, "engine pot drawdown is within the configured paper limit"


def _advance_deployment_risk(
    conn: psycopg.Connection[Any], *, risk: BrokerAccountRiskSnapshot, deployment_id: int
) -> None:
    """Link one deployment's own-book drawdown (#3541 slice 3); a failure marks only its row.

    Its own savepoint, so one deployment's defect neither rolls back another's advance nor the
    pot's ``drawdown`` block. The refusal is written after the savepoint rolled back.
    """
    try:
        with conn.transaction():
            advance_deployment_drawdown(conn, deployment_id, observe_deployment_nav(conn, risk, deployment_id))
        return
    except EngineCapitalObservationError as exc:
        logger.warning("paper deployment %s drawdown unobservable: %s", deployment_id, exc)
        refusal = f"{exc.reason_code}: {exc}"
    except Exception as exc:
        # Any other failure (a DB error, a violated CHECK) rolled back only this savepoint; it
        # must not abort the other deployments or the freshness blocks written after the loop.
        logger.exception("paper deployment %s drawdown advance failed", deployment_id)
        refusal = f"unexpected {type(exc).__name__}"
    # The refusal write is isolated the same way: if it fails, the row keeps its last state and
    # the live gate's age bound withdraws it once the probe stops advancing it.
    try:
        with conn.transaction():
            record_deployment_refusal(conn, deployment_id, refusal)
    except Exception:
        logger.exception("paper deployment %s drawdown refusal could not be recorded", deployment_id)


def refresh_strategy_health(
    conn: psycopg.Connection[Any], *, broker: BrokerProvider, now: datetime | None = None
) -> int:
    """Refresh bounded current health state for enabled paper deployments.

    ⚠⚠ FOUR of the five gates are scoped to enabled paper deployments and clear
    when none exists.  ``order_reconciliation`` is NOT, and the difference is
    #2961: unresolved order identity is a property of the ORDERS, and the core
    arm has orders without ever having a ``strategy_deployments`` row.  In
    today's core-only configuration (measured 2026-09-13: zero enabled paper
    deployments) the blanket clear wrote ``active=false`` / *"no enabled paper
    strategy deployment requires this health gate"* over a database that could
    hold a core order nothing can resolve — a sentence true about deployments
    and false about the system.

    ⚠ This is an OBSERVABILITY fix and deliberately not described as a safety
    one.  A new core authority is already refused by ``core_trade_in_flight``
    (#2949 scenario 2), and with no enabled deployment there are no alpha entries
    to block, so nothing was ever admitted by the false clear.  What was lost was
    the operator's only signal that the core arm is stuck.

    ⚠⚠ The core-only branch keys on PRESENCE, not on an age, and that is forced
    rather than chosen: ``strategy_core_mandate_events`` carries no
    reconciliation-age column (checked), so there is no declared threshold for
    this arm and none is invented here.  Presence is STRICTER than the policy
    branch's age rule — it fires the moment a row is non-terminal — which is the
    fail-closed direction and costs nothing, because the block gates entries that
    this configuration cannot make anyway.  When a paper deployment IS enabled the
    policy branch runs and the declared age governs, unchanged.
    """
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT min(p.max_reconciliation_age_seconds) AS reconciliation_age,
                   min(p.max_scan_age_seconds) AS scan_age,
                   min(p.max_quote_age_seconds) AS quote_age,
                   min(p.max_drawdown_pct) AS drawdown_limit,
                   array_agg(d.deployment_id ORDER BY d.deployment_id) AS deployment_ids
            FROM strategy_deployments d
            JOIN strategy_execution_policies p ON p.deployment_id=d.deployment_id
            WHERE d.mode='paper' AND d.enabled
            """
        )
        policy = cur.fetchone()
    assert policy is not None
    if policy["reconciliation_age"] is None:
        # ⚠ No `conn.commit()` here, and the absence is load-bearing rather than a
        # saving: the policy SELECT above leaves the connection INTRANS, and
        # `conn.transaction()` on a non-idle connection opens a SAVEPOINT instead
        # of a transaction — silently, which is the trap. The commit that used to
        # sit here is now `count_unresolved_order_identity`'s stated postcondition.
        # A second one is not defence in depth, it is two commits in a row
        # (review nitpick, round 1).
        unresolved = count_unresolved_order_identity(conn)
        with conn.transaction():
            for source in (
                "scan_freshness",
                "quote_freshness",
                "broker_availability",
                "drawdown",
            ):
                _set_block(
                    conn,
                    source=source,
                    active=False,
                    reason="no enabled paper strategy deployment requires this health gate",
                )
            _set_block(
                conn,
                source="order_reconciliation",
                active=unresolved > 0,
                reason=(
                    f"{unresolved} strategy order(s) have unresolved broker identity and no enabled "
                    "paper deployment declares a reconciliation age, so no age threshold applies"
                    if unresolved
                    else "no strategy order has unresolved broker identity"
                ),
            )
        return 1 if unresolved else 0

    reconciliation = enforce_reconciliation_slo(conn, max_unresolved_seconds=int(policy["reconciliation_age"]))
    conn.commit()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (
                     WHERE w.updated_at IS NULL OR w.updated_at < %(cutoff)s
                   ) AS stale,
                   count(*) AS total
            FROM strategy_deployments d
            LEFT JOIN strategy_scan_watermark w
              ON w.strategy_id=d.strategy_id AND w.strategy_version=d.strategy_version
            WHERE d.mode='paper' AND d.enabled
              -- #3471 §8: a demo-trial leg has no scan-watermark producer; its freshness
              -- bound is the decision's own target session, checked by its loader. Counted
              -- here, its absent watermark would hold scan_freshness active for everyone.
              AND NOT (d.strategy_id = ANY(%(demo_trial_ids)s::text[]))
            """,
            {
                "cutoff": observed_at - timedelta(seconds=int(policy["scan_age"])),
                "demo_trial_ids": sorted(DEMO_TRIAL_STRATEGY_IDS),
            },
        )
        scan = cur.fetchone()
        assert scan is not None
        # Core arm admitted by PRESENCE, not by the authorised witness chain
        # (app/services/strategy_core_arc_sql.py).  This population feeds an
        # execution BLOCK, so counting a position makes the block more likely to
        # trip -- presence is the fail-closed choice here, and the authorised
        # predicate would be the fail-open one.  The signal arm's own predicate
        # is unchanged: the INNER fd/d pair under `d.mode='paper' AND d.enabled`
        # is exactly the LEFT pair under `d.deployment_id IS NOT NULL`.
        cur.execute(
            """
            SELECT count(*) FILTER (
                     WHERE q.quoted_at IS NULL OR q.quoted_at < %(cutoff)s
                   ) AS stale,
                   count(*) AS total
            FROM strategy_position_ownership own
            JOIN strategy_trades t ON t.strategy_trade_id=own.strategy_trade_id
            LEFT JOIN strategy_funding_decisions fd ON fd.funding_decision_id=t.funding_decision_id
            LEFT JOIN strategy_deployments d ON d.deployment_id=fd.deployment_id
             AND d.mode='paper' AND d.enabled
            LEFT JOIN quotes q ON q.instrument_id=t.instrument_id
            WHERE own.status='active'
              AND (d.deployment_id IS NOT NULL OR t.core_rebalance_intent_id IS NOT NULL)
            """,
            {"cutoff": observed_at - timedelta(seconds=int(policy["quote_age"]))},
        )
        quotes = cur.fetchone()
        assert quotes is not None

    # End the read transaction before the broker network round-trip. Risk state
    # is read afresh afterwards in the transaction that persists the decision.
    conn.rollback()

    risk = None
    try:
        risk = broker.get_account_risk_snapshot()
        broker_active = risk.observed_at < observed_at - timedelta(seconds=int(policy["quote_age"]))
        broker_reason = "broker account-risk snapshot stale" if broker_active else "broker account-risk probe healthy"
    except Exception:
        logger.warning("strategy broker account-risk probe unavailable", exc_info=True)
        broker_active = True
        broker_reason = "broker account-risk probe unavailable"

    scan_active = int(scan["stale"]) > 0
    quote_active = int(quotes["stale"]) > 0
    with conn.transaction():
        # #3541: the ENGINE POT's drawdown, never the account's -- the account also holds
        # the operator's own positions. Advanced only from a fresh probe, as before.
        drawdown_active, drawdown_reason = _pot_drawdown_block(
            conn,
            risk=None if broker_active else risk,
            limit=Decimal(str(policy["drawdown_limit"])),
        )
        if risk is not None and not broker_active:
            for deployment_id in policy["deployment_ids"]:
                _advance_deployment_risk(conn, risk=risk, deployment_id=int(deployment_id))
        _set_block(
            conn,
            source="scan_freshness",
            active=scan_active,
            reason=(
                f"{scan['stale']} of {scan['total']} enabled strategy scans are stale"
                if scan_active
                else "enabled strategy scans are current"
            ),
        )
        _set_block(
            conn,
            source="quote_freshness",
            active=quote_active,
            reason=(
                f"{quotes['stale']} of {quotes['total']} owned-position quotes are stale"
                if quote_active
                else "owned-position quotes are current"
            ),
        )
        _set_block(conn, source="broker_availability", active=broker_active, reason=broker_reason)
        _set_block(conn, source="drawdown", active=drawdown_active, reason=drawdown_reason)
    return sum((reconciliation.active_block, scan_active, quote_active, broker_active, drawdown_active))


def run_strategy_paper_cycle(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    signal_limit: int = 5,
    reconciliation_limit: int = 20,
    position_limit: int = 5,
    now: datetime | None = None,
    strategy_versions: Sequence[str] | None = None,
    entries: bool = True,
) -> StrategyPaperCycleResult:
    """Run one bounded demo-only strategy lifecycle cycle.

    ``entries=False`` runs reconciliation, health and position management only -- the
    risk-reducing half -- and evaluates no new signal (#3546: the caller withholds entries
    when an entry-only input, the halt feed, could not be refreshed).

    #3546 gap B: every stage and every item is contained. One failure used to raise out of
    the cycle and skip everything after it -- a poison position stopped the protection of
    every later one. A failed reconciliation or health refresh also withholds entries, since
    both are entry inputs; management never waits on either. Failures are counted on
    ``errors`` so the caller degrades the run instead of recording it clean.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("strategy paper cycle requires an idle connection")
    if signal_limit <= 0 or reconciliation_limit <= 0 or position_limit <= 0:
        raise ValueError("strategy paper cycle limits must be positive")
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    # A fresh instant per item unless the caller pinned one: the executor refuses an account
    # snapshot stamped more than 5 s after its `now`, so the cycle's start instant would
    # turn a slow cycle's later fresh snapshots into stale refusals (as `ai_trial_jobs`).
    clock: Callable[[], datetime] = (lambda: observed_at) if now is not None else (lambda: datetime.now(UTC))
    errors: Counter[str] = Counter()

    reconciled = 0
    try:
        reconciled = len(reconcile_backlog(conn, broker=broker, limit=reconciliation_limit))
    except Exception:
        logger.exception("strategy paper cycle: reconciliation failed; entries withheld, management continues")
        _reset(conn)
        errors["reconcile"] += 1
    active_blocks: int | None = None
    try:
        active_blocks = refresh_strategy_health(conn, broker=broker, now=observed_at)
        conn.commit()
    except Exception:
        logger.exception("strategy paper cycle: health refresh failed; entries withheld, management continues")
        _reset(conn)
        errors["health"] += 1

    # Rotate the bounded ownership batch by five-minute slot. A fixed
    # ``ORDER BY ... LIMIT`` would protect the same oldest positions forever
    # and starve later positions once the sleeve grows past the cap.
    position_offset = (int(observed_at.timestamp()) // 300) * position_limit
    management: Counter[str] = Counter()
    try:
        owned = conn.execute(_OWNED_BATCH_SQL, (position_offset, position_limit)).fetchall()
        conn.commit()
    except Exception:
        logger.exception("strategy paper cycle: owned-position batch read failed")
        _reset(conn)
        errors["manage"] += 1
        owned = []
    for trade_id, position_id in owned:
        try:
            outcome = manage_owned_position(
                conn,
                broker=broker,
                strategy_trade_id=int(trade_id),
                broker_position_id=int(position_id),
                now=clock(),
            )
        except Exception:
            logger.exception("strategy paper cycle: managing trade %s raised; continuing with the batch", trade_id)
            _reset(conn)
            errors["manage"] += 1
            continue
        management[outcome.state] += 1
    managed = sum(management.values())

    if not entries or errors["reconcile"] or errors["health"]:
        return StrategyPaperCycleResult(reconciled, managed, 0, active_blocks, dict(management), dict(errors))
    versions = (
        list(strategy_versions)
        if strategy_versions is not None
        else [
            entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
            for entry in STRATEGY_MANIFEST.values()
            if entry.purpose == "capital_candidate"
        ]
    )
    try:
        candidates = _load_ranked_opportunities(conn, strategy_versions=versions, observed_at=observed_at)
        members = persist_ranking_batch(
            conn,
            opportunities=candidates,
            selection_limit=signal_limit,
            decided_at=observed_at,
        )
    except Exception:
        # Fails closed: no ranking, no entry (a duplicate economic identity raises here).
        logger.exception("strategy paper cycle: ranking failed; no entries this cycle")
        _reset(conn)
        errors["ranking"] += 1
        members = []
    evaluated = 0
    deferred: Counter[str] = Counter()
    for member in members:
        # A signal that raises has no funding decision, so the next cycle re-selects it;
        # containing it here keeps it from also skipping the rest of the batch.
        try:
            outcome = execute_fired_paper_signal(
                conn,
                broker=broker,
                signal_id=member.opportunity.signal_id,
                ranking_member_id=member.ranking_member_id,
                now=clock(),
            )
        except Exception:
            logger.exception(
                "strategy paper cycle: signal %s raised; continuing with the batch", member.opportunity.signal_id
            )
            _reset(conn)
            errors["execute"] += 1
            continue
        evaluated += 1
        if outcome.verdict == "deferred":
            deferred[outcome.reason_code] += 1
    return StrategyPaperCycleResult(
        reconciled, managed, evaluated, active_blocks, dict(management), dict(errors), dict(deferred)
    )


def _reset(conn: psycopg.Connection[Any]) -> None:
    """Leave the connection usable for the next item after a contained failure."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()


__all__ = ["StrategyPaperCycleResult", "refresh_strategy_health", "run_strategy_paper_cycle"]
