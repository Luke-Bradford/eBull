"""Operator-governed strategy promotion, allocation, and exact ownership.

This module is intentionally broker-free.  It creates the durable authority a
later executor must prove, but cannot place, patch, or close an order itself.
Manual order paths remain in :mod:`app.services.order_client` and never acquire
strategy ownership by instrument coincidence.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal, cast

import psycopg
import psycopg.rows

from app.services.result_ledger import holdout_access_counts, stored_result_promotion_refusals_for
from app.services.strategy_base_currency import (
    DEPLOYMENT_CURRENCY,
    DEPLOYMENT_CURRENCY_UNSUPPORTED,
    canonical_currency_code,
    normalise_deployment_currency,
)
from app.services.strategy_capital_sandbox import sandbox_bound
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyPurpose
from app.services.strategy_promotion_evidence import evidence_refusals
from app.services.strategy_promotion_evidence_store import load_promotion_evidences
from app.services.strategy_result import holdout_count_promotion_refusals, structural_promotion_refusals
from app.services.strategy_result_ambiguity import load_promotion_ambiguity_refusals
from app.services.strategy_result_universe import load_result_universes, universe_promotion_refusals

Stage = Literal[
    "research_candidate",
    "historical_validated",
    "forward_observation",
    "paper_enabled",
    "live_enabled",
    "paused",
    "retired",
]
Mode = Literal["paper", "live"]
CapitalMode = Literal["fixed", "compound"]
RiskProfile = Literal["unconfigured", "cautious", "balanced", "growth"]
TicketSizingMode = Literal["percent", "fixed"]

GOVERNANCE_GATE_VERSION = "strategy-governance-v2+edge-evidence"
MANDATE_POLICY_VERSION = "portfolio-mandate-v1"
UNCONFIGURED_MANDATE_POLICY_VERSION = "portfolio-mandate-unconfigured"

# Two-key Postgres advisory-lock namespace.  2454 is this issue's stable,
# documented namespace; hashtext(strategy identity) supplies the per-version
# key.  Do not reuse 2454 for an unrelated advisory lock.
_ADVISORY_LOCK_NAMESPACE = 2454
PAPER_ALLOCATOR_ADVISORY_LOCK = (2449, 1)

_NEXT_STAGE: dict[Stage | None, frozenset[Stage]] = {
    None: frozenset({"research_candidate"}),
    "research_candidate": frozenset({"historical_validated", "paused"}),
    "historical_validated": frozenset({"forward_observation", "paused"}),
    "forward_observation": frozenset({"paper_enabled", "paused"}),
    "paper_enabled": frozenset({"live_enabled", "paused"}),
    "live_enabled": frozenset({"paused"}),
    "paused": frozenset({"retired"}),
    "retired": frozenset(),
}

_RESULT_EVIDENCE_STAGES = frozenset({"historical_validated", "forward_observation"})
_EXTERNAL_EVIDENCE_STAGES = frozenset({"historical_validated", "forward_observation", "paper_enabled", "live_enabled"})


class StrategyControlError(ValueError):
    """A fail-closed control-plane refusal."""


class StrategyOwnershipError(StrategyControlError):
    """The exact broker position is not actively owned by this trade."""


def registered_strategy_purpose(strategy_id: str) -> StrategyPurpose | None:
    entry = STRATEGY_MANIFEST.get(strategy_id)
    return None if entry is None else entry.purpose


@dataclass(frozen=True)
class Promotion:
    promotion_id: int
    strategy_id: str
    strategy_version: str
    from_stage: Stage | None
    to_stage: Stage


@dataclass(frozen=True)
class Deployment:
    deployment_id: int
    strategy_id: str
    strategy_version: str
    mode: Mode
    capital_limit: Decimal
    enabled: bool
    revision: int
    # The currency actually PERSISTED, post-normalisation. Callers echoed their own
    # pre-call value before this existed, so a response could disagree with the row.
    currency: str = DEPLOYMENT_CURRENCY


@dataclass(frozen=True)
class PortfolioMandate:
    policy_version: str
    risk_profile: RiskProfile
    target_volatility_pct: Decimal | None
    max_portfolio_drawdown_pct: Decimal | None
    max_loss_per_position_pct: Decimal | None
    max_daily_loss_pct: Decimal | None
    active_risk_budget_pct: Decimal | None
    cash_reserve_pct: Decimal | None
    max_concurrent_positions: int | None
    shorts_allowed: bool = False
    leverage_allowed: bool = False

    @property
    def configured(self) -> bool:
        return self.risk_profile != "unconfigured"


UNCONFIGURED_MANDATE = PortfolioMandate(
    policy_version=UNCONFIGURED_MANDATE_POLICY_VERSION,
    risk_profile="unconfigured",
    target_volatility_pct=None,
    max_portfolio_drawdown_pct=None,
    max_loss_per_position_pct=None,
    max_daily_loss_pct=None,
    active_risk_budget_pct=None,
    cash_reserve_pct=None,
    max_concurrent_positions=None,
)

_MANDATE_PROFILES: dict[RiskProfile, PortfolioMandate] = {
    "unconfigured": UNCONFIGURED_MANDATE,
    "cautious": PortfolioMandate(
        MANDATE_POLICY_VERSION,
        "cautious",
        Decimal("8"),
        Decimal("10"),
        Decimal("0.5"),
        Decimal("1"),
        Decimal("10"),
        Decimal("25"),
        4,
    ),
    "balanced": PortfolioMandate(
        MANDATE_POLICY_VERSION,
        "balanced",
        Decimal("12"),
        Decimal("15"),
        Decimal("0.75"),
        Decimal("1.5"),
        Decimal("20"),
        Decimal("15"),
        8,
    ),
    "growth": PortfolioMandate(
        MANDATE_POLICY_VERSION,
        "growth",
        Decimal("18"),
        Decimal("25"),
        Decimal("1"),
        Decimal("2.5"),
        Decimal("30"),
        Decimal("10"),
        12,
    ),
}


def mandate_for_profile(risk_profile: RiskProfile) -> PortfolioMandate:
    """Resolve a presentation label to the exact immutable v1 limits.

    These are operator risk ceilings, not estimated returns or fitted strategy
    parameters.  A later policy changes ``MANDATE_POLICY_VERSION`` and writes
    its exact limits; it never mutates the meaning of an existing event.
    """
    try:
        return _MANDATE_PROFILES[risk_profile]
    except KeyError as exc:  # pragma: no cover - guarded by Literal/Pydantic at typed boundaries
        raise StrategyControlError(f"unknown risk profile: {risk_profile}") from exc


@dataclass(frozen=True)
class PaperPool:
    event_id: int | None
    enabled: bool
    capital_limit: Decimal
    currency: str = "USD"
    capital_mode: CapitalMode = "fixed"
    mandate: PortfolioMandate = UNCONFIGURED_MANDATE


def load_paper_pool(conn: psycopg.Connection[Any]) -> PaperPool:
    row = conn.execute(
        """
        SELECT strategy_paper_pool_event_id,enabled,capital_limit,currency,capital_mode,
               mandate_policy_version,risk_profile,target_volatility_pct,
               max_portfolio_drawdown_pct,max_loss_per_position_pct,max_daily_loss_pct,
               active_risk_budget_pct,cash_reserve_pct,max_concurrent_positions,
               shorts_allowed,leverage_allowed
        FROM strategy_paper_pool_events
        ORDER BY strategy_paper_pool_event_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return PaperPool(None, False, Decimal("0"))
    mandate = PortfolioMandate(
        policy_version=str(row[5]),
        risk_profile=cast(RiskProfile, row[6]),
        target_volatility_pct=None if row[7] is None else Decimal(str(row[7])),
        max_portfolio_drawdown_pct=None if row[8] is None else Decimal(str(row[8])),
        max_loss_per_position_pct=None if row[9] is None else Decimal(str(row[9])),
        max_daily_loss_pct=None if row[10] is None else Decimal(str(row[10])),
        active_risk_budget_pct=None if row[11] is None else Decimal(str(row[11])),
        cash_reserve_pct=None if row[12] is None else Decimal(str(row[12])),
        max_concurrent_positions=None if row[13] is None else int(row[13]),
        shorts_allowed=bool(row[14]),
        leverage_allowed=bool(row[15]),
    )
    return PaperPool(
        int(row[0]),
        bool(row[1]),
        Decimal(str(row[2])),
        str(row[3]),
        cast(CapitalMode, row[4]),
        mandate,
    )


def configure_paper_pool(
    conn: psycopg.Connection[Any],
    *,
    enabled: bool,
    capital_limit: Decimal,
    capital_mode: CapitalMode = "fixed",
    risk_profile: RiskProfile,
    changed_by: str,
    reason: str,
) -> PaperPool:
    """Append one material shared paper-capital authority revision."""
    _require_text(changed_by, "changed_by")
    _require_text(reason, "reason")
    if not capital_limit.is_finite() or capital_limit < 0 or (enabled and capital_limit <= 0):
        raise StrategyControlError("enabled paper pool requires a positive finite USD capital limit")
    if capital_mode not in {"fixed", "compound"}:
        raise StrategyControlError("capital_mode must be fixed or compound")
    mandate = mandate_for_profile(risk_profile)
    if enabled and not mandate.configured:
        raise StrategyControlError("enabled paper pool requires a configured portfolio risk mandate")
    # Conflict with the executor's session lock so a pause/lower cannot race an
    # already-sized order between its authority read and demo broker submit.
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK)
    current = load_paper_pool(conn)
    if (
        current.enabled == enabled
        and current.capital_limit == capital_limit
        and current.capital_mode == capital_mode
        and current.mandate == mandate
    ):
        raise StrategyControlError(
            "paper pool change must alter enabled state, capital limit, capital mode, or mandate"
        )
    if current.event_id is not None and capital_limit < current.capital_limit:
        # A lower principal is an external withdrawal from the virtual sleeve,
        # not merely a cosmetic limit edit.  It cannot remove money already
        # committed to an allocated lifecycle.  Realised losses reduce the
        # withdrawable balance; known profits count only in compound mode.
        committed_row = conn.execute(
            """
            SELECT COALESCE(SUM(decision.amount), 0)
            FROM strategy_funding_decisions decision
            WHERE decision.verdict='allocated'
              AND EXISTS (
                  SELECT 1 FROM strategy_deployments deployment
                  WHERE deployment.deployment_id=decision.deployment_id
                    AND deployment.mode='paper'
              )
              AND (
                  NOT EXISTS (
                      SELECT 1 FROM strategy_trades trade
                      WHERE trade.funding_decision_id=decision.funding_decision_id
                  )
                  OR EXISTS (
                      SELECT 1 FROM strategy_trades trade
                      WHERE trade.funding_decision_id=decision.funding_decision_id
                        AND trade.status NOT IN ('closed', 'failed')
                  )
              )
            """
        ).fetchone()
        assert committed_row is not None
        committed = Decimal(str(committed_row[0]))
        if committed:
            # Local import keeps the governance module independent at import
            # time while reusing the exact-owned, fail-closed reconciliation.
            from app.services.strategy_monitoring import load_paper_realised_pnl

            realised = load_paper_realised_pnl(conn)
            if realised is None:
                raise StrategyControlError("paper principal cannot be withdrawn while realised P&L is incomplete")
            # One arithmetic with the executor and the /strategies card (#2844) —
            # this check decides whether the operator may withdraw below what the
            # executor has already committed, so a divergence here would authorise a
            # withdrawal the executor's own bound has already spent against.
            effective_after = sandbox_bound(
                capital_limit=capital_limit,
                capital_mode=capital_mode,
                realised_delta=sum(realised.values(), Decimal("0")),
            )
            if effective_after < committed:
                raise StrategyControlError("paper principal cannot be withdrawn below committed strategy capital")
    row = conn.execute(
        """
        INSERT INTO strategy_paper_pool_events (
            enabled,capital_limit,currency,capital_mode,changed_by,reason,
            mandate_policy_version,risk_profile,target_volatility_pct,
            max_portfolio_drawdown_pct,max_loss_per_position_pct,max_daily_loss_pct,
            active_risk_budget_pct,cash_reserve_pct,max_concurrent_positions,
            shorts_allowed,leverage_allowed
        )
        VALUES (%s,%s,'USD',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING strategy_paper_pool_event_id
        """,
        (
            enabled,
            capital_limit,
            capital_mode,
            changed_by,
            reason,
            mandate.policy_version,
            mandate.risk_profile,
            mandate.target_volatility_pct,
            mandate.max_portfolio_drawdown_pct,
            mandate.max_loss_per_position_pct,
            mandate.max_daily_loss_pct,
            mandate.active_risk_budget_pct,
            mandate.cash_reserve_pct,
            mandate.max_concurrent_positions,
            mandate.shorts_allowed,
            mandate.leverage_allowed,
        ),
    ).fetchone()
    assert row is not None
    return PaperPool(int(row[0]), enabled, capital_limit, "USD", capital_mode, mandate)


@dataclass(frozen=True)
class ExecutionPolicy:
    deployment_id: int
    revision: int
    ticket_sizing_mode: TicketSizingMode
    ticket_fraction: Decimal | None
    fixed_ticket_amount: Decimal | None
    max_ticket_amount: Decimal
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    max_quote_age_seconds: int
    max_scan_age_seconds: int
    max_halt_feed_age_seconds: int
    max_cost_age_seconds: int
    max_reconciliation_age_seconds: int
    max_instrument_exposure_pct: Decimal
    max_portfolio_exposure_pct: Decimal
    max_drawdown_pct: Decimal
    min_net_expectancy_pct: Decimal
    cost_stress_multiplier: Decimal


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise StrategyControlError(f"{field} must be non-empty")


def _lock_strategy(conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str) -> None:
    conn.execute(
        "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
        (_ADVISORY_LOCK_NAMESPACE, f"{strategy_id}\x1f{strategy_version}"),
    )


def lock_strategy_control(conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str) -> None:
    """Serialize a compound governance read/write with promotion changes.

    Callers that must evaluate evidence before invoking a control-plane
    mutation acquire this transaction lock first.  The internal mutation lock
    is re-entrant within the same transaction.
    """
    _lock_strategy(conn, strategy_id, strategy_version)


def is_risk_reducing_deployment_change(
    *,
    current_capital_limit: Decimal,
    current_enabled: bool,
    current_currency: str,
    capital_limit: Decimal,
    enabled: bool,
    currency: str,
) -> bool:
    """Return whether a deployment change cannot add capital authority."""
    return capital_limit <= current_capital_limit and (not enabled or current_enabled) and currency == current_currency


def current_stage(conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str) -> Stage | None:
    row = conn.execute(
        """
        SELECT to_stage
        FROM strategy_promotions
        WHERE strategy_id = %s AND strategy_version = %s
        ORDER BY promotion_id DESC
        LIMIT 1
        """,
        (strategy_id, strategy_version),
    ).fetchone()
    return cast(Stage, row[0]) if row is not None else None


def promote_strategy(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    to_stage: Stage,
    promoted_by: str,
    reason: str,
    evidence_ref: str | None = None,
    result_ids: Sequence[int] = (),
    gate_version: str = GOVERNANCE_GATE_VERSION,
) -> Promotion:
    """Append one explicit, ordered promotion event.

    Evidence is pinned, never inferred from a current metric.  Result ids must
    belong to this exact strategy version.  Global auto/live switches are not
    read here and therefore cannot create or advance a promotion.

    Evidence stages replay, per pinned result: the #2505 edge-evidence record,
    the #2621 frozen-universe record (``evaluated ⊆ validated`` as the run
    loaded it — never today's ``load_validated_universe``, whose ``is_tradable``
    filter would let a later delisting retroactively invalidate a passing
    result), the #2625 frozen §3.4 ambiguity record, and the row's own
    structural stamps through the shared ``structural_promotion_refusals``. A
    result stored without any required record refuses; this is the fail-closed
    replacement for trusting that the sole writer ran ``check_promotable`` at
    write time.

    #2639 closed the remainder: criterion 5's two counts (against TODAY's
    ledger, because a frozen pair is blind to a later unrecorded look),
    criterion 9's arm pair (re-derived from the identity hash, not from a
    recorded boolean), and the row's own purpose / deflation /
    effective-sample-size / §9 clauses. ``unenforced_candidate_fields()`` is now
    empty.

    ⚠ WHAT THIS STILL DOES NOT DO. The counts are read once and are not atomic
    with the INSERT below — the hold-out writers do not take this function's
    advisory lock, so a hold-out row may commit between the read and the
    promotion. And a corrupt stored row RAISES out of
    ``stored_result_promotion_refusals`` rather than refusing, which aborts
    before the remaining refusals are gathered and therefore masks them.

    Every input's temporal rule and whether the transition applies it are
    declared in ``app.services.strategy_promotion_replay``. Do not add an input
    here without classifying it there; a test enforces that.
    """
    for value, field in (
        (strategy_id, "strategy_id"),
        (strategy_version, "strategy_version"),
        (promoted_by, "promoted_by"),
        (reason, "reason"),
        (gate_version, "gate_version"),
    ):
        _require_text(value, field)
    if to_stage == "live_enabled":
        raise StrategyControlError("live_enabled requires the dedicated measured live-promotion gate")
    purpose = registered_strategy_purpose(strategy_id)
    if to_stage in _EXTERNAL_EVIDENCE_STAGES and purpose == "harness_validation":
        raise StrategyControlError("harness-validation strategies are permanent controls and cannot be promoted")
    if to_stage in _EXTERNAL_EVIDENCE_STAGES and purpose != "capital_candidate":
        raise StrategyControlError("unregistered strategies cannot advance to evidence-backed deployment stages")
    if evidence_ref is not None:
        _require_text(evidence_ref, "evidence_ref")
    if len(set(result_ids)) != len(result_ids):
        raise StrategyControlError("result_ids must be unique")

    _lock_strategy(conn, strategy_id, strategy_version)
    from_stage = current_stage(conn, strategy_id, strategy_version)
    if to_stage not in _NEXT_STAGE[from_stage]:
        raise StrategyControlError(f"invalid promotion transition: {from_stage!r} -> {to_stage!r}")

    if to_stage in _EXTERNAL_EVIDENCE_STAGES and evidence_ref is None:
        raise StrategyControlError(f"{to_stage} requires an immutable evidence_ref")
    if to_stage in _RESULT_EVIDENCE_STAGES and not result_ids:
        raise StrategyControlError(f"{to_stage} requires at least one pinned result_id")

    if result_ids:
        rows = conn.execute(
            """
            SELECT result_id, profit_factor, evaluated_instrument_count,
                   universe_basis, carry_unmodelled, fx_unmodelled, opportunity_set_digest
            FROM strategy_results_store
            WHERE strategy_id = %s AND strategy_version = %s
              AND result_id = ANY(%s)
            """,
            (strategy_id, strategy_version, list(result_ids)),
        ).fetchall()
        found = {int(row[0]) for row in rows}
        missing = set(result_ids) - found
        if missing:
            raise StrategyControlError(
                f"result_ids do not belong to {strategy_id}@{strategy_version}: {sorted(missing)}"
            )
        if to_stage in _RESULT_EVIDENCE_STAGES:
            # #2639 — criterion 5's two counts, read ONCE for the whole
            # transition. They are scoped to (strategy_id, strategy_version),
            # which is exactly what this call promotes, so they are a property
            # of the promotion rather than of any one pinned result — and every
            # result's refusal list carries them so that the raise names them.
            #
            # ⚠ AGAINST TODAY'S COUNTS, NOT A FROZEN PAIR. Freezing defeats the
            # criterion: a pair frozen at result time is blind to a later
            # unrecorded look at the same version's hold-out, which is the leak
            # criterion 5 exists to catch. `holdout_access_counts` records
            # nothing, so asking is safe. Reasoning in
            # `app.services.strategy_promotion_replay`.
            counts = holdout_access_counts(conn, strategy_id, strategy_version)
            holdout_refusals = holdout_count_promotion_refusals(
                holdout_evaluations=counts.holdout_evaluations,
                recorded_accesses=counts.recorded_accesses,
            )
            profit_factor_by_result = {int(row[0]): None if row[1] is None else Decimal(str(row[1])) for row in rows}
            evaluated_count_by_result = {int(row[0]): int(row[2]) for row in rows}
            opportunity_digest_by_result = {int(row[0]): None if row[6] is None else str(row[6]) for row in rows}
            # ⚠ A NULL COST STAMP READS AS *UNMODELLED*, NEVER AS MODELLED.
            # `bool(None)` is False, and False means "carry is modelled" — so the
            # obvious coercion turns an unset stamp into a PASS on the clause it
            # exists to enforce, which is fail-open on the Tier 1 refusals. Both
            # columns are NOT NULL today (verified against dev, 2026-08-13), so
            # this is defence in depth rather than a live bug; it is written this
            # way because the failure direction is silent and the schema is one
            # migration away from changing. `universe_basis` needs no such care —
            # it preserves None, which `structural_promotion_refusals` already
            # refuses as `universe_basis_absent`.
            stamps_by_result = {
                int(row[0]): (
                    None if row[3] is None else str(row[3]),
                    True if row[4] is None else bool(row[4]),
                    True if row[5] is None else bool(row[5]),
                )
                for row in rows
            }
            # #2641 — every per-result record type is read for the WHOLE batch
            # before the loop, one statement each, where the loop previously
            # issued five round trips per pinned result. ⚠ The reordering this
            # buys is named on `stored_result_promotion_refusals_for`: a corrupt
            # record anywhere in the batch now raises before ANY result's
            # refusals are gathered. Within a result nothing moves.
            universes = load_result_universes(conn, result_ids)
            ambiguity_refusals = load_promotion_ambiguity_refusals(conn, result_ids)
            stored_refusals = stored_result_promotion_refusals_for(conn, result_ids)
            evidences = load_promotion_evidences(conn, result_ids)
            for result_id in result_ids:
                # #2621 — the transition REPLAYS the universe check from the
                # frozen record instead of trusting the write-time refusal that
                # died with ``WrittenRow``. Frozen at result time, deliberately:
                # today's date enters this gate only where a validity window was
                # declared (the cost staleness clause inside
                # ``evidence_refusals``); the universe declares none, and the
                # order-time rule against the CURRENT universe is the execution
                # guard's, not promotion's. Both refusal lists are gathered
                # before raising so one missing input cannot mask the other.
                refusals = list(
                    universe_promotion_refusals(
                        universes.get(result_id),
                        evaluated_instrument_count=evaluated_count_by_result[result_id],
                        expected_opportunity_digest=opportunity_digest_by_result[result_id],
                    )
                )
                # #2625 — the §3.4 ambiguity comparison, re-derived from the
                # frozen record rather than trusted. Same shape and the same
                # argument as the universe replay above; the record stores the
                # comparison's INPUTS so the verdict can be disagreed with.
                refusals.extend(ambiguity_refusals[result_id])
                # #2625 — the row's own STRUCTURAL stamps. ⚠ These were
                # persisted and never replayed: before this, a result stamped
                # `survivor_only` / `carry_unmodelled` / `fx_unmodelled` — which
                # is all 324 rows in dev — could be pinned to a promotion
                # without the transition ever looking. Routed through the SHARED
                # `structural_promotion_refusals`, the single copy #2599's
                # preregistration freeze also calls, so the freeze's expectation
                # and the transition's verdict cannot drift apart.
                universe_basis, carry_unmodelled, fx_unmodelled = stamps_by_result[result_id]
                refusals.extend(
                    structural_promotion_refusals(
                        universe_basis=universe_basis,
                        carry_unmodelled=carry_unmodelled,
                        fx_unmodelled=fx_unmodelled,
                    )
                )
                # #2639 — the row's OWN remaining clauses: its stamped purpose,
                # criteria 6 and 3, criterion 9's arm pair and §9's acceptance.
                # Every one is a column on the row already pinned above, and
                # every one was trusting the write-time verdict that died with
                # `WrittenRow`. ⚠ Deliberately does NOT return the structural
                # stamps — see `stored_result_promotion_refusals` for why the
                # read directly above is kept rather than sourced from the
                # rebuilt object.
                refusals.extend(stored_refusals[result_id])
                refusals.extend(holdout_refusals)
                evidence = evidences.get(result_id)
                if evidence is None:
                    refusals.append("promotion_evidence_missing")
                else:
                    refusals.extend(
                        evidence_refusals(
                            evidence,
                            profit_factor=profit_factor_by_result[result_id],
                            as_of=date.today(),
                        )
                    )
                if refusals:
                    raise StrategyControlError(f"result {result_id} fails promotion evidence: {', '.join(refusals)}")

    row = conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING promotion_id
        """,
        (
            strategy_id,
            strategy_version,
            from_stage,
            to_stage,
            gate_version,
            evidence_ref,
            promoted_by,
            reason,
        ),
    ).fetchone()
    assert row is not None
    promotion_id = int(row[0])
    if result_ids:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO strategy_promotion_results (promotion_id, result_id)
                VALUES (%s, %s)
                """,
                [(promotion_id, result_id) for result_id in result_ids],
            )
    return Promotion(promotion_id, strategy_id, strategy_version, from_stage, to_stage)


def configure_deployment(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    mode: Mode,
    capital_limit: Decimal,
    enabled: bool,
    changed_by: str,
    reason: str,
    currency: str = DEPLOYMENT_CURRENCY,
) -> Deployment:
    """Create/update one operator capital ceiling and append its audit event."""
    for value, field in (
        (strategy_id, "strategy_id"),
        (strategy_version, "strategy_version"),
        (changed_by, "changed_by"),
        (reason, "reason"),
        (currency, "currency"),
    ):
        _require_text(value, field)
    if capital_limit < 0:
        raise StrategyControlError("capital_limit must be non-negative")
    # Normalise ONCE, by rebinding, so every downstream use reads the canonical code:
    # the is_risk_reducing_deployment_change comparison below, both INSERTs and the
    # UPDATE. Four comparisons kept in step by hand is how the #2634 wedge happened.
    #
    # No risk-reducing exemption, and none is needed: sql/338 forces the stored
    # currency to be supported, so `existing[4]` is always canonical and a DISABLE
    # (which echoes the stored value back -- app/api/strategies.py:2162) can never be
    # the call that trips this. A guard that could block an operator from reducing
    # exposure would fail open; this one provably cannot reach that state.
    supported = normalise_deployment_currency(currency)
    if supported is None:
        raise StrategyControlError(
            f"{DEPLOYMENT_CURRENCY_UNSUPPORTED}: {canonical_currency_code(currency)!r} is not a "
            f"supported deployment currency (FX is unmodelled -- #2363)"
        )
    currency = supported

    _lock_strategy(conn, strategy_id, strategy_version)
    stage = current_stage(conn, strategy_id, strategy_version)
    existing = conn.execute(
        """
        SELECT deployment_id, revision, capital_limit, enabled, currency
        FROM strategy_deployments
        WHERE strategy_id = %s AND strategy_version = %s AND mode = %s
        FOR UPDATE
        """,
        (strategy_id, strategy_version, mode),
    ).fetchone()
    risk_reducing = existing is not None and is_risk_reducing_deployment_change(
        current_capital_limit=Decimal(str(existing[2])),
        current_enabled=bool(existing[3]),
        current_currency=str(existing[4]),
        capital_limit=capital_limit,
        enabled=enabled,
        currency=currency,
    )
    purpose = registered_strategy_purpose(strategy_id)
    if purpose == "harness_validation" and not risk_reducing and (enabled or capital_limit > 0):
        raise StrategyControlError("harness-validation strategies cannot receive capital authority")
    if purpose != "capital_candidate" and not risk_reducing and (enabled or capital_limit > 0):
        raise StrategyControlError("unregistered strategies cannot receive capital authority")
    eligible: dict[Mode, frozenset[Stage]] = {
        "paper": frozenset({"paper_enabled", "live_enabled"}),
        "live": frozenset({"live_enabled"}),
    }
    if enabled and stage not in eligible[mode] and not risk_reducing:
        raise StrategyControlError(f"{mode} deployment cannot be enabled at stage {stage!r}")
    if existing is None:
        revision = 1
        row = conn.execute(
            """
            INSERT INTO strategy_deployments (
                strategy_id, strategy_version, mode, capital_limit, currency,
                enabled, revision, updated_by, reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING deployment_id
            """,
            (
                strategy_id,
                strategy_version,
                mode,
                capital_limit,
                currency,
                enabled,
                revision,
                changed_by,
                reason,
            ),
        ).fetchone()
        assert row is not None
        deployment_id = int(row[0])
    else:
        deployment_id = int(existing[0])
        revision = int(existing[1]) + 1
        conn.execute(
            """
            UPDATE strategy_deployments
            SET capital_limit = %s, currency = %s, enabled = %s,
                revision = %s, updated_by = %s, reason = %s, updated_at = now()
            WHERE deployment_id = %s
            """,
            (capital_limit, currency, enabled, revision, changed_by, reason, deployment_id),
        )

    conn.execute(
        """
        INSERT INTO strategy_deployment_events (
            deployment_id, revision, capital_limit, currency, enabled,
            changed_by, reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (deployment_id, revision, capital_limit, currency, enabled, changed_by, reason),
    )
    return Deployment(
        deployment_id,
        strategy_id,
        strategy_version,
        mode,
        capital_limit,
        enabled,
        revision,
        currency,
    )


def configure_execution_policy(
    conn: psycopg.Connection[Any],
    *,
    deployment_id: int,
    ticket_fraction: Decimal | None,
    max_ticket_amount: Decimal,
    ticket_sizing_mode: TicketSizingMode = "percent",
    fixed_ticket_amount: Decimal | None = None,
    stop_loss_pct: Decimal,
    take_profit_pct: Decimal,
    max_quote_age_seconds: int,
    max_scan_age_seconds: int,
    max_halt_feed_age_seconds: int,
    max_cost_age_seconds: int,
    max_reconciliation_age_seconds: int,
    max_instrument_exposure_pct: Decimal,
    max_portfolio_exposure_pct: Decimal,
    max_drawdown_pct: Decimal,
    min_net_expectancy_pct: Decimal,
    cost_stress_multiplier: Decimal,
    changed_by: str,
    reason: str,
) -> ExecutionPolicy:
    """Set explicit paper-execution limits and append the complete revision.

    There are intentionally no policy defaults: every number can authorise or
    refuse capital and must therefore be an operator decision visible in the
    audit stream.
    """
    for value, field in ((changed_by, "changed_by"), (reason, "reason")):
        _require_text(value, field)
    if ticket_sizing_mode == "percent":
        if ticket_fraction is None or not (Decimal("0") < ticket_fraction <= Decimal("1")):
            raise StrategyControlError("percent ticket_fraction must be in (0, 1]")
        if fixed_ticket_amount is not None:
            raise StrategyControlError("percent sizing cannot carry fixed_ticket_amount")
    elif ticket_sizing_mode == "fixed":
        if ticket_fraction is not None:
            raise StrategyControlError("fixed sizing cannot carry ticket_fraction")
        if fixed_ticket_amount is None or not fixed_ticket_amount.is_finite() or fixed_ticket_amount <= 0:
            raise StrategyControlError("fixed_ticket_amount must be positive and finite")
    else:
        raise StrategyControlError("ticket_sizing_mode must be percent or fixed")
    if max_ticket_amount <= 0:
        raise StrategyControlError("max_ticket_amount must be positive")
    if not (Decimal("0") < stop_loss_pct < Decimal("100")) or take_profit_pct <= 0:
        raise StrategyControlError("stop/take-profit percentages must be positive and stop_loss_pct < 100")
    ages = (
        max_quote_age_seconds,
        max_scan_age_seconds,
        max_halt_feed_age_seconds,
        max_cost_age_seconds,
        max_reconciliation_age_seconds,
    )
    if any(value <= 0 for value in ages):
        raise StrategyControlError("freshness and reconciliation ages must be positive")
    if not (Decimal("0") < max_instrument_exposure_pct <= Decimal("100")):
        raise StrategyControlError("max_instrument_exposure_pct must be in (0, 100]")
    if not (Decimal("0") < max_portfolio_exposure_pct <= Decimal("100")):
        raise StrategyControlError("max_portfolio_exposure_pct must be in (0, 100]")
    if not (Decimal("0") < max_drawdown_pct < Decimal("100")):
        raise StrategyControlError("max_drawdown_pct must be in (0, 100)")
    if cost_stress_multiplier < 1:
        raise StrategyControlError("cost_stress_multiplier must be at least 1")

    deployment = conn.execute(
        "SELECT mode FROM strategy_deployments WHERE deployment_id = %s FOR UPDATE",
        (deployment_id,),
    ).fetchone()
    if deployment is None:
        raise StrategyControlError("deployment does not exist")
    if deployment[0] != "paper":
        raise StrategyControlError("the MVP execution policy is paper-only")
    current = conn.execute(
        "SELECT revision FROM strategy_execution_policies WHERE deployment_id = %s",
        (deployment_id,),
    ).fetchone()
    revision = 1 if current is None else int(current[0]) + 1
    values = (
        deployment_id,
        revision,
        ticket_sizing_mode,
        ticket_fraction,
        fixed_ticket_amount,
        max_ticket_amount,
        stop_loss_pct,
        take_profit_pct,
        max_quote_age_seconds,
        max_scan_age_seconds,
        max_halt_feed_age_seconds,
        max_cost_age_seconds,
        max_reconciliation_age_seconds,
        max_instrument_exposure_pct,
        max_portfolio_exposure_pct,
        max_drawdown_pct,
        min_net_expectancy_pct,
        cost_stress_multiplier,
        changed_by,
        reason,
    )
    conn.execute(
        """
        INSERT INTO strategy_execution_policies (
            deployment_id, revision, ticket_sizing_mode, ticket_fraction, fixed_ticket_amount, max_ticket_amount,
            stop_loss_pct, take_profit_pct, max_quote_age_seconds,
            max_scan_age_seconds, max_halt_feed_age_seconds,
            max_cost_age_seconds, max_reconciliation_age_seconds,
            max_instrument_exposure_pct, max_portfolio_exposure_pct,
            max_drawdown_pct, min_net_expectancy_pct,
            cost_stress_multiplier, updated_by, reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (deployment_id) DO UPDATE SET
            revision = EXCLUDED.revision,
            ticket_sizing_mode = EXCLUDED.ticket_sizing_mode,
            ticket_fraction = EXCLUDED.ticket_fraction,
            fixed_ticket_amount = EXCLUDED.fixed_ticket_amount,
            max_ticket_amount = EXCLUDED.max_ticket_amount,
            stop_loss_pct = EXCLUDED.stop_loss_pct,
            take_profit_pct = EXCLUDED.take_profit_pct,
            max_quote_age_seconds = EXCLUDED.max_quote_age_seconds,
            max_scan_age_seconds = EXCLUDED.max_scan_age_seconds,
            max_halt_feed_age_seconds = EXCLUDED.max_halt_feed_age_seconds,
            max_cost_age_seconds = EXCLUDED.max_cost_age_seconds,
            max_reconciliation_age_seconds = EXCLUDED.max_reconciliation_age_seconds,
            max_instrument_exposure_pct = EXCLUDED.max_instrument_exposure_pct,
            max_portfolio_exposure_pct = EXCLUDED.max_portfolio_exposure_pct,
            max_drawdown_pct = EXCLUDED.max_drawdown_pct,
            min_net_expectancy_pct = EXCLUDED.min_net_expectancy_pct,
            cost_stress_multiplier = EXCLUDED.cost_stress_multiplier,
            updated_by = EXCLUDED.updated_by, reason = EXCLUDED.reason,
            updated_at = now()
        """,
        values,
    )
    conn.execute(
        """
        INSERT INTO strategy_execution_policy_events (
            deployment_id, revision, ticket_sizing_mode, ticket_fraction, fixed_ticket_amount, max_ticket_amount,
            stop_loss_pct, take_profit_pct, max_quote_age_seconds,
            max_scan_age_seconds, max_halt_feed_age_seconds,
            max_cost_age_seconds, max_reconciliation_age_seconds,
            max_instrument_exposure_pct, max_portfolio_exposure_pct,
            max_drawdown_pct, min_net_expectancy_pct,
            cost_stress_multiplier, changed_by, reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        values,
    )
    return ExecutionPolicy(*values[:18])


def decide_funding(
    conn: psycopg.Connection[Any],
    *,
    signal_id: int,
    verdict: Literal["allocated", "rejected"],
    reason_code: str,
    deployment_id: int | None = None,
    amount: Decimal | None = None,
    detail: str | None = None,
) -> int:
    """Persist the sole funding verdict for a durable fired signal."""
    _require_text(reason_code, "reason_code")
    if detail is not None:
        _require_text(detail, "detail")
    signal = conn.execute(
        """
        SELECT strategy_id, strategy_version, signal_kind, verdict
        FROM strategy_signals WHERE signal_id = %s
        FOR UPDATE
        """,
        (signal_id,),
    ).fetchone()
    if signal is None or signal[3] != "fired":
        raise StrategyControlError("funding decisions require a fired durable signal")

    if verdict == "allocated":
        if signal[2] != "entry":
            raise StrategyControlError("capital may only be allocated to an entry signal")
        if deployment_id is None or amount is None or amount <= 0:
            raise StrategyControlError("allocated verdict requires deployment_id and positive amount")
        _lock_strategy(conn, str(signal[0]), str(signal[1]))
        deployment = conn.execute(
            """
            SELECT strategy_id, strategy_version, mode, capital_limit, enabled
            FROM strategy_deployments WHERE deployment_id = %s
            FOR UPDATE
            """,
            (deployment_id,),
        ).fetchone()
        if deployment is None or not bool(deployment[4]):
            raise StrategyControlError("allocation requires an enabled deployment")
        if (deployment[0], deployment[1]) != (signal[0], signal[1]):
            raise StrategyControlError("signal and deployment strategy versions do not match")
        stage = current_stage(conn, str(signal[0]), str(signal[1]))
        mode = cast(Mode, deployment[2])
        eligible: dict[Mode, frozenset[Stage]] = {
            "paper": frozenset({"paper_enabled", "live_enabled"}),
            "live": frozenset({"live_enabled"}),
        }
        if stage not in eligible[mode]:
            raise StrategyControlError(f"{mode} funding cannot be allocated at strategy stage {stage!r}")

        # The deployment lock serialises the reservation read with concurrent
        # decisions. Decisions with no trade are pending; reconciliations stay
        # reserved. Closed/failed trades release their allocation capacity.
        reserved_row = conn.execute(
            """
            SELECT COALESCE(SUM(d.amount), 0)
            FROM strategy_funding_decisions d
            LEFT JOIN strategy_trades t
              ON t.funding_decision_id = d.funding_decision_id
            WHERE d.deployment_id = %s AND d.verdict = 'allocated'
              AND (t.strategy_trade_id IS NULL OR t.status NOT IN ('closed', 'failed'))
            """,
            (deployment_id,),
        ).fetchone()
        assert reserved_row is not None
        reserved = Decimal(str(reserved_row[0]))
        if reserved + amount > Decimal(str(deployment[3])):
            raise StrategyControlError("allocation exceeds the deployment capital_limit")
    elif deployment_id is not None or amount is not None:
        raise StrategyControlError("rejected verdict cannot reserve deployment capital")

    row = conn.execute(
        """
        INSERT INTO strategy_funding_decisions (
            signal_id, deployment_id, verdict, amount, reason_code, detail
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING funding_decision_id
        """,
        (signal_id, deployment_id, verdict, amount, reason_code, detail),
    ).fetchone()
    assert row is not None
    return int(row[0])


def create_strategy_trade(conn: psycopg.Connection[Any], funding_decision_id: int) -> int:
    """Create a planned trade from one allocated decision; derive its instrument."""
    row = conn.execute(
        """
        SELECT d.verdict, s.instrument_id
        FROM strategy_funding_decisions d
        JOIN strategy_signals s ON s.signal_id = d.signal_id
        WHERE d.funding_decision_id = %s
        """,
        (funding_decision_id,),
    ).fetchone()
    if row is None or row[0] != "allocated":
        raise StrategyControlError("a strategy trade requires an allocated funding decision")
    created = conn.execute(
        """
        INSERT INTO strategy_trades (funding_decision_id, instrument_id)
        VALUES (%s, %s)
        RETURNING strategy_trade_id
        """,
        (funding_decision_id, row[1]),
    ).fetchone()
    assert created is not None
    return int(created[0])


def link_strategy_order(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    order_id: int,
    purpose: Literal["entry", "exit", "stop_loss", "take_profit", "stop_ratchet", "reconcile"],
) -> int:
    """Link an explicitly strategy-origin order to a same-instrument trade."""
    row = conn.execute(
        """
        SELECT t.instrument_id, o.instrument_id, o.execution_origin
        FROM strategy_trades t CROSS JOIN orders o
        WHERE t.strategy_trade_id = %s AND o.order_id = %s
        """,
        (strategy_trade_id, order_id),
    ).fetchone()
    if row is None:
        raise StrategyControlError("strategy trade or order does not exist")
    if row[2] != "strategy":
        raise StrategyControlError("manual orders cannot be linked to strategy trades")
    if row[0] != row[1]:
        raise StrategyControlError("strategy trade and order instruments do not match")
    linked = conn.execute(
        """
        INSERT INTO strategy_trade_orders (strategy_trade_id, order_id, purpose)
        VALUES (%s, %s, %s)
        RETURNING strategy_trade_order_id
        """,
        (strategy_trade_id, order_id, purpose),
    ).fetchone()
    assert linked is not None
    return int(linked[0])


def claim_exact_position(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    entry_order_id: int,
    broker_position_id: int,
) -> int:
    """Claim the exact position returned for this trade's strategy entry order."""
    row = conn.execute(
        """
        SELECT 1
        FROM strategy_trades t
        JOIN strategy_trade_orders sto
          ON sto.strategy_trade_id = t.strategy_trade_id
         AND sto.order_id = %s AND sto.purpose = 'entry'
        JOIN orders o ON o.order_id = sto.order_id AND o.execution_origin = 'strategy'
        JOIN strategy_order_position_executions execution
          ON execution.order_id = o.order_id
         AND execution.broker_position_id = %s
        WHERE t.strategy_trade_id = %s
        """,
        (entry_order_id, broker_position_id, strategy_trade_id),
    ).fetchone()
    if row is None:
        raise StrategyOwnershipError("position claim requires the exact strategy entry order and broker position id")
    claimed = conn.execute(
        """
        INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id)
        VALUES (%s, %s)
        RETURNING ownership_id
        """,
        (strategy_trade_id, broker_position_id),
    ).fetchone()
    assert claimed is not None
    return int(claimed[0])


def record_order_position_execution(conn: psycopg.Connection[Any], *, order_id: int, broker_position_id: int) -> None:
    """Persist one exact position id returned by detailed strategy-order lookup.

    The future reconciler owns the broker call. This broker-free control-plane
    method records its result and refuses manual-origin orders.
    """
    if broker_position_id <= 0:
        raise StrategyOwnershipError("broker_position_id must be positive")
    row = conn.execute(
        "SELECT execution_origin FROM orders WHERE order_id = %s",
        (order_id,),
    ).fetchone()
    if row is None or row[0] != "strategy":
        raise StrategyOwnershipError("position executions may be recorded only for a strategy-origin order")
    conn.execute(
        """
        INSERT INTO strategy_order_position_executions (order_id, broker_position_id)
        VALUES (%s, %s)
        """,
        (order_id, broker_position_id),
    )


def assert_exact_position_owned(
    conn: psycopg.Connection[Any], *, strategy_trade_id: int, broker_position_id: int
) -> None:
    """Fail unless this exact trade/id pair has active strategy ownership."""
    row = conn.execute(
        """
        SELECT 1 FROM strategy_position_ownership
        WHERE strategy_trade_id = %s AND broker_position_id = %s
          AND status = 'active'
        """,
        (strategy_trade_id, broker_position_id),
    ).fetchone()
    if row is None:
        raise StrategyOwnershipError(
            f"broker position {broker_position_id} is not actively owned by strategy trade {strategy_trade_id}"
        )


def release_exact_position(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    broker_position_id: int,
    reason: str,
) -> None:
    """Release only the exact active ownership pair, preserving its history."""
    _require_text(reason, "reason")
    row = conn.execute(
        """
        UPDATE strategy_position_ownership
        SET status = 'released', released_at = now(), release_reason = %s
        WHERE strategy_trade_id = %s AND broker_position_id = %s
          AND status = 'active'
        RETURNING ownership_id
        """,
        (reason, strategy_trade_id, broker_position_id),
    ).fetchone()
    if row is None:
        raise StrategyOwnershipError(
            f"broker position {broker_position_id} is not actively owned by strategy trade {strategy_trade_id}"
        )


__all__ = [
    "Deployment",
    "ExecutionPolicy",
    "GOVERNANCE_GATE_VERSION",
    "PaperPool",
    "PAPER_ALLOCATOR_ADVISORY_LOCK",
    "Promotion",
    "StrategyControlError",
    "StrategyOwnershipError",
    "assert_exact_position_owned",
    "claim_exact_position",
    "configure_deployment",
    "configure_execution_policy",
    "configure_paper_pool",
    "create_strategy_trade",
    "current_stage",
    "decide_funding",
    "is_risk_reducing_deployment_change",
    "link_strategy_order",
    "load_paper_pool",
    "lock_strategy_control",
    "promote_strategy",
    "registered_strategy_purpose",
    "record_order_position_execution",
    "release_exact_position",
]
