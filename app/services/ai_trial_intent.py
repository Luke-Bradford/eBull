"""#3471 slice 2b-i — the ``demo_trial`` intent loader (spec §8 "Loader authorisation").

The paper path's ``_load_intent`` refuses without promotion evidence, a calibrated forecast,
a passed prospective assessment and a selected ranking member. The trial has none of them,
so it gets its OWN intent type and loader rather than a flag on that one (§8, O7): nothing
here populates a forecast or ranking field, and nothing is fabricated to satisfy one.

What the loader authorises, in one query at submission time:

* the signal links, through ``ai_trial_leg_links`` (§8's "``ai_trial_executions``" — the
  link table ``sql/432`` built for exactly this), to an ``accepted`` decision of a
  ``decided`` run;
* signal, decision, instrument, leg, strategy id/version and deployment all agree;
* the strategy's purpose is ``demo_trial``;
* the run's declaration is the trial's digest-intact root declaration;
* the latest ``ai_trial_state_events`` state is ``active`` — a halt stops queued decisions;
* ``session_date`` is the current NYSE session (``decision_expired`` replaces the paper
  path's scan-watermark gate: the watermark has no producer here).

Every NON-evidence gate of ``_load_intent`` is kept, with the same reason code, and
``PAPER_GATE_MAP`` records the disposition of every one of its codes; a test fails when
``_load_intent`` gains a code this map does not classify. The sizing, cost-cap and
submission half is slice 2b-ii; nothing here touches the broker or writes a row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final, Literal, cast

import psycopg
import psycopg.rows

from app.services.ai_trial_pack import NonCanonicalValue, canonical_sha256
from app.services.strategy_base_currency import (
    DEPLOYMENT_CURRENCY_UNSUPPORTED,
    SUPPORTED_DEPLOYMENT_CURRENCIES,
)
from app.services.strategy_control_plane import registered_strategy_purpose
from app.services.strategy_halt_identity import INSTRUMENT_HALT_SYMBOL_SQL
from app.services.strategy_paper_executor import _NY, _age_ok, _session_is_open

#: The #2599 row's ``contract_version`` prefix; ``sql/432``'s bind trigger requires
#: ``ai-trial-declaration-v1:<doc_sha256>``.
DECLARATION_CONTRACT_PREFIX: Final = "ai-trial-declaration-v1:"

#: §8 trial caps (frozen by construction): the ``size_tier``'s requested ticket, USD only (O8).
#: The inherited capacity arithmetic may reduce it; nothing may raise it.
TRIAL_TICKET_USD: Final[dict[str, Decimal]] = {"full": Decimal("250"), "half": Decimal("125")}
TRIAL_CURRENCY: Final = "USD"

Leg = Literal["arm", "control"]

#: Every reason code the paper loader ``_load_intent`` can return, and what the trial does
#: with it. ``kept`` = the trial loader applies the same gate under the same code;
#: ``replaced:<code>`` = the evidence field is replaced by a trial source (§8 table), refused
#: under the trial's ``<code>``; ``dropped`` = an evidence gate with no trial counterpart
#: (§8: "Evidence fields replaced, never fabricated"). ``tests/test_ai_trial_intent.py``
#: parses ``_load_intent``'s source and fails on an unclassified code, so a new paper gate
#: cannot silently skip the trial; it also fails when a kept or replacing code is not one
#: ``load_trial_intent`` can return.
PAPER_GATE_MAP: Final[dict[str, str]] = {
    "signal_not_fired_entry": "kept",
    "instrument_not_tradable": "kept",
    "unsupported_market_session": "kept",
    "paper_deployment_missing": "kept",
    "paper_pool_unconfigured": "kept",
    "paper_pool_disabled": "kept",
    "portfolio_mandate_unconfigured": "kept",
    "portfolio_mandate_incomplete": "kept",
    "paper_deployment_disabled": "kept",
    DEPLOYMENT_CURRENCY_UNSUPPORTED: "kept",
    "execution_policy_missing": "kept",
    "execution_block_active": "kept",
    "quote_missing": "kept",
    "quote_ask_invalid": "kept",
    "quote_spread_flagged": "kept",
    "halt_feed_missing": "kept",
    "instrument_halted": "kept",
    "quote_stale": "kept",
    "halt_feed_stale": "kept",
    "market_session_closed": "kept",
    # §8: the scan watermark has no producer on this path; the decision's own target
    # session is the freshness bound.
    "scan_watermark_missing": "replaced:decision_expired",
    "scan_stale": "replaced:decision_expired",
    # §8: forecast barriers -> the validated decision's stop/target, still bounded by the
    # policy stop_loss_pct under the trial's own code.
    "opportunity_forecast_stop_exceeds_policy": "replaced:decision_stop_exceeds_policy",
    "opportunity_forecast_target_barrier_missing": "replaced:decision_stop_exceeds_policy",
    "opportunity_forecast_stop_barrier_missing": "replaced:decision_stop_exceeds_policy",
    # Evidence gates with no trial counterpart (§8 table: forecast_id / ranking_member_id /
    # expectancy -> "none"). The ranking-to-pool binding has nothing to go stale: the
    # trial reads the CURRENT pool event at submission, so a mandate change applies to
    # already-queued decisions (§8 table).
    "opportunity_ranking_mandate_stale": "dropped",
    "opportunity_forecast_missing": "dropped",
    "opportunity_forecast_policy_stale": "dropped",
    "opportunity_calibration_not_passed": "dropped",
    "opportunity_assessment_policy_missing": "dropped",
    "opportunity_assessment_missing": "dropped",
    "opportunity_assessment_not_passed": "dropped",
    "opportunity_forecast_cost_model_stale": "dropped",
    "opportunity_forecast_expectancy_not_positive": "dropped",
    "opportunity_ranking_member_missing": "dropped",
    "opportunity_ranking_member_not_selected": "dropped",
    "opportunity_ranking_policy_stale": "dropped",
    "expectancy_evidence_missing": "dropped",
    "opportunity_forecast_not_current": "dropped",
    "opportunity_calibration_knowledge_time_invalid": "dropped",
    "opportunity_assessment_stale": "dropped",
}


@dataclass(frozen=True)
class TrialIntent:
    """One authorised trial leg, ready for sizing (slice 2b-ii).

    The non-evidence fields carry ``_Intent``'s names and meanings, so the shared capacity
    arithmetic can read either. There is no forecast, ranking, expectancy or scan field.
    """

    signal_id: int
    strategy_id: str
    strategy_version: str
    instrument_id: int
    symbol: str
    deployment_id: int
    currency: str
    deployment_limit: Decimal
    pool_limit: Decimal
    capital_mode: Literal["fixed", "compound"]
    pool_reserved: Decimal
    mandate_max_drawdown_pct: Decimal
    mandate_max_loss_per_position_pct: Decimal
    mandate_max_daily_loss_pct: Decimal
    mandate_active_risk_budget_pct: Decimal
    mandate_cash_reserve_pct: Decimal
    mandate_max_concurrent_positions: int
    policy_revision: int
    max_ticket_amount: Decimal
    #: The validated decision's stop / target (§8 table), percentage points.
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    max_quote_age_seconds: int
    max_halt_feed_age_seconds: int
    max_cost_age_seconds: int
    max_reconciliation_age_seconds: int
    max_instrument_exposure_pct: Decimal
    max_portfolio_exposure_pct: Decimal
    max_drawdown_pct: Decimal
    cost_stress_multiplier: Decimal
    quote_at: datetime
    ask: Decimal
    halt_feed_at: datetime
    reserved: Decimal
    # Trial identity.
    declaration_id: int
    run_id: int
    decision_id: int
    pair_id: int
    pair_seq: int
    leg: Leg
    session_date: date
    horizon_days: int
    size_tier: Literal["half", "full"]
    #: The ``size_tier``'s ticket (O8: recorded against the actual amount).
    requested_amount: Decimal


def declaration_digest(doc: object) -> str:
    """The digest ``ai_trial_declarations.doc_sha256`` must equal: the canonical-JSON sha256
    of the document AS STORED (O3). The slice 3 writer hashes the value it inserts with this
    function; a JSONB round trip that changes the canonical form fails every load closed."""
    return canonical_sha256(doc)


def _leg_strategy_id(arm_strategy_id: str, leg: str) -> str:
    return arm_strategy_id + ("-control" if leg == "control" else "")


def _session_reason(session_date: date, *, now: datetime) -> str | None:
    today = now.astimezone(_NY).date()
    if session_date < today:
        return "decision_expired"
    if session_date > today:
        return "decision_not_yet_due"
    return None


_TRIAL_INTENT_SQL = f"""
    SELECT s.signal_id, s.strategy_id, s.strategy_version, s.instrument_id, s.fill_bar_date,
           i.symbol, i.is_tradable, e.asset_class,
           link.pair_id, link.leg,
           pair.pair_seq, pair.declaration_id AS pair_declaration_id,
           pair.control_instrument_id, pair.stop_pct AS pair_stop_pct,
           pair.target_pct AS pair_target_pct, pair.horizon_days AS pair_horizon_days,
           pair.size_tier AS pair_size_tier,
           decision.decision_id, decision.verdict AS decision_verdict,
           decision.instrument_id AS decision_instrument_id,
           decision.stop_pct, decision.target_pct, decision.horizon_days, decision.size_tier,
           run.run_id, run.status AS run_status, run.session_date,
           run.declaration_id AS run_declaration_id,
           decl.strategy_id AS declared_strategy_id, decl.strategy_version AS declared_version,
           decl.doc, decl.doc_sha256, prereg.contract_version,
           state.to_state AS trial_state,
           d.deployment_id, d.capital_limit, d.enabled, d.currency,
           pool.enabled AS pool_enabled, pool.capital_limit AS pool_limit,
           pool.capital_mode, pool.risk_profile,
           pool.max_portfolio_drawdown_pct AS mandate_max_drawdown_pct,
           pool.max_loss_per_position_pct AS mandate_max_loss_per_position_pct,
           pool.max_daily_loss_pct AS mandate_max_daily_loss_pct,
           pool.active_risk_budget_pct AS mandate_active_risk_budget_pct,
           pool.cash_reserve_pct AS mandate_cash_reserve_pct,
           pool.max_concurrent_positions AS mandate_max_concurrent_positions,
           p.revision AS policy_revision, p.max_ticket_amount, p.stop_loss_pct AS policy_stop_loss_pct,
           p.max_quote_age_seconds, p.max_halt_feed_age_seconds, p.max_cost_age_seconds,
           p.max_reconciliation_age_seconds, p.max_instrument_exposure_pct,
           p.max_portfolio_exposure_pct, p.max_drawdown_pct, p.cost_stress_multiplier,
           q.quoted_at, q.ask, q.spread_flag,
           h.fetched_at AS halt_feed_at,
           EXISTS (
               SELECT 1 FROM strategy_market_halts mh
               WHERE mh.source = 'nasdaq_trader_rss'
                 AND mh.symbol = {INSTRUMENT_HALT_SYMBOL_SQL} AND mh.resumed_at IS NULL
           ) AS is_halted,
           EXISTS (SELECT 1 FROM strategy_execution_blocks b WHERE b.active) AS execution_blocked,
           (
               SELECT COALESCE(SUM(fd.amount), 0)
               FROM strategy_funding_decisions fd
               LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
               WHERE fd.deployment_id = d.deployment_id AND fd.verdict = 'allocated'
                 AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
           ) AS reserved,
           (
               SELECT COALESCE(SUM(fd.amount), 0)
               FROM strategy_funding_decisions fd
               JOIN strategy_deployments reserved_d
                 ON reserved_d.deployment_id = fd.deployment_id AND reserved_d.mode = 'paper'
               LEFT JOIN strategy_trades st ON st.funding_decision_id = fd.funding_decision_id
               WHERE fd.verdict = 'allocated'
                 AND (st.strategy_trade_id IS NULL OR st.status NOT IN ('closed', 'failed'))
           ) AS pool_reserved
    FROM strategy_signals s
    JOIN instruments i ON i.instrument_id = s.instrument_id
    LEFT JOIN exchanges e ON e.exchange_id = i.exchange
    LEFT JOIN ai_trial_leg_links link ON link.signal_id = s.signal_id
    LEFT JOIN ai_trial_pairs pair ON pair.pair_id = link.pair_id
    LEFT JOIN ai_trial_decisions decision ON decision.decision_id = pair.arm_decision_id
    LEFT JOIN ai_trial_runs run ON run.run_id = decision.run_id
    LEFT JOIN ai_trial_declarations decl ON decl.declaration_id = run.declaration_id
    LEFT JOIN strategy_preregistration_declarations prereg ON prereg.declaration_id = decl.declaration_id
    LEFT JOIN LATERAL (
        SELECT se.to_state FROM ai_trial_state_events se
        WHERE se.declaration_id = run.declaration_id
        ORDER BY se.event_id DESC LIMIT 1
    ) state ON true
    LEFT JOIN strategy_deployments d
      ON d.strategy_id = s.strategy_id AND d.strategy_version = s.strategy_version AND d.mode = 'paper'
    LEFT JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
    LEFT JOIN LATERAL (
        SELECT enabled, capital_limit, capital_mode, risk_profile,
               max_portfolio_drawdown_pct, max_loss_per_position_pct, max_daily_loss_pct,
               active_risk_budget_pct, cash_reserve_pct, max_concurrent_positions
        FROM strategy_paper_pool_events
        ORDER BY strategy_paper_pool_event_id DESC
        LIMIT 1
    ) pool ON true
    LEFT JOIN quotes q ON q.instrument_id = s.instrument_id
    LEFT JOIN strategy_halt_feed_state h ON h.source = 'nasdaq_trader_rss'
    WHERE s.signal_id = %(signal_id)s AND s.signal_kind = 'entry' AND s.verdict = 'fired'
"""


def _positive_finite(value: object) -> bool:
    return value is not None and Decimal(str(value)).is_finite() and Decimal(str(value)) > 0


def _identity_agrees(row: dict[str, Any]) -> bool:
    """Signal, decision, instrument, leg, strategy id/version all agree. ``sql/432``'s link
    trigger checked this at link time; it is re-read here because this is the query that
    authorises the order (§8), and the trigger cannot see later rows."""
    leg = row["leg"]
    leg_instrument = row["decision_instrument_id"] if leg == "arm" else row["control_instrument_id"]
    return (
        row["pair_declaration_id"] == row["run_declaration_id"]
        and row["strategy_id"] == _leg_strategy_id(str(row["declared_strategy_id"]), str(leg))
        and row["strategy_version"] == row["declared_version"]
        and row["instrument_id"] == leg_instrument
        and row["fill_bar_date"] == row["session_date"]
        and (row["pair_stop_pct"], row["pair_target_pct"], row["pair_horizon_days"], row["pair_size_tier"])
        == (row["stop_pct"], row["target_pct"], row["horizon_days"], row["size_tier"])
    )


def _declaration_intact(row: dict[str, Any]) -> bool:
    try:
        digest = declaration_digest(row["doc"])
    except NonCanonicalValue:
        return False
    return digest == row["doc_sha256"] and row["contract_version"] == DECLARATION_CONTRACT_PREFIX + str(
        row["doc_sha256"]
    )


def load_trial_intent(
    conn: psycopg.Connection[Any],
    *,
    signal_id: int,
    now: datetime,
) -> tuple[TrialIntent | None, str | None, bool]:
    """Authorise one trial leg's fired entry, or return the refusal code.

    Same contract as the paper ``_load_intent``: the bool says the halt SQL ran (a fetched
    row), so the caller can stamp the halt identity rule version on a refusal.
    """
    purpose_row = conn.execute("SELECT strategy_id FROM strategy_signals WHERE signal_id = %s", (signal_id,)).fetchone()
    if purpose_row is None:
        return None, "signal_not_fired_entry", False
    if registered_strategy_purpose(str(purpose_row[0])) != "demo_trial":
        return None, "strategy_not_demo_trial", False
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(_TRIAL_INTENT_SQL, {"signal_id": signal_id})
        row = cur.fetchone()
    if row is None:
        return None, "signal_not_fired_entry", False

    # §8 authorisation first: a signal the trial never decided is refused before any
    # market or mandate fact is read into a reason.
    authorisation = (
        (row["pair_id"] is not None, "trial_link_missing"),
        (
            row["decision_verdict"] == "accepted" and row["run_status"] == "decided",
            "trial_decision_not_accepted",
        ),
        (_identity_agrees(row), "trial_identity_mismatch"),
        (_declaration_intact(row), "trial_declaration_not_intact"),
        (row["trial_state"] == "active", "trial_not_active"),
    )
    for passed, reason in authorisation:
        if not passed:
            return None, reason, True
    session_reason = _session_reason(cast(date, row["session_date"]), now=now)
    if session_reason is not None:
        return None, session_reason, True

    # The paper loader's non-evidence gates, same codes, same order (PAPER_GATE_MAP).
    checks = (
        (bool(row["is_tradable"]), "instrument_not_tradable"),
        (row["asset_class"] == "us_equity", "unsupported_market_session"),
        (row["deployment_id"] is not None, "paper_deployment_missing"),
        (row["pool_limit"] is not None, "paper_pool_unconfigured"),
        (bool(row["pool_enabled"]), "paper_pool_disabled"),
        (row["risk_profile"] is not None and row["risk_profile"] != "unconfigured", "portfolio_mandate_unconfigured"),
        (
            all(
                row[field] is not None
                for field in (
                    "mandate_max_drawdown_pct",
                    "mandate_max_loss_per_position_pct",
                    "mandate_max_daily_loss_pct",
                    "mandate_active_risk_budget_pct",
                    "mandate_cash_reserve_pct",
                    "mandate_max_concurrent_positions",
                )
            ),
            "portfolio_mandate_incomplete",
        ),
        (bool(row["enabled"]), "paper_deployment_disabled"),
        (row["currency"] in SUPPORTED_DEPLOYMENT_CURRENCIES, DEPLOYMENT_CURRENCY_UNSUPPORTED),
        # O8: the trial's tickets are USD amounts; widening the supported set must not
        # quietly reinterpret them in another currency.
        (row["currency"] == TRIAL_CURRENCY, "trial_currency_not_usd"),
        (row["policy_revision"] is not None, "execution_policy_missing"),
        # sql/432's CHECK admits only these tiers; refused here rather than a KeyError below
        # should that vocabulary ever widen without this map.
        (row["size_tier"] in TRIAL_TICKET_USD, "decision_size_tier_unknown"),
        # §8 table: the decision's stop is still bounded by the policy stop_loss_pct.
        (
            _positive_finite(row["stop_pct"])
            and _positive_finite(row["target_pct"])
            and _positive_finite(row["policy_stop_loss_pct"])
            and Decimal(str(row["stop_pct"])) <= Decimal(str(row["policy_stop_loss_pct"])),
            "decision_stop_exceeds_policy",
        ),
        (not bool(row["execution_blocked"]), "execution_block_active"),
        (row["quoted_at"] is not None and row["ask"] is not None, "quote_missing"),
        (_positive_finite(row["ask"]), "quote_ask_invalid"),
        (not bool(row["spread_flag"]), "quote_spread_flagged"),
        (row["halt_feed_at"] is not None, "halt_feed_missing"),
        (not bool(row["is_halted"]), "instrument_halted"),
    )
    for passed, reason in checks:
        if not passed:
            return None, reason, True
    quote_at = cast(datetime, row["quoted_at"])
    halt_feed_at = cast(datetime, row["halt_feed_at"])
    if not _age_ok(quote_at, now=now, max_seconds=int(row["max_quote_age_seconds"])):
        return None, "quote_stale", True
    if not _age_ok(halt_feed_at, now=now, max_seconds=int(row["max_halt_feed_age_seconds"])):
        return None, "halt_feed_stale", True
    if not _session_is_open(now):
        return None, "market_session_closed", True
    size_tier = cast(Literal["half", "full"], row["size_tier"])
    return (
        TrialIntent(
            signal_id=signal_id,
            strategy_id=str(row["strategy_id"]),
            strategy_version=str(row["strategy_version"]),
            instrument_id=int(row["instrument_id"]),
            symbol=str(row["symbol"]),
            deployment_id=int(row["deployment_id"]),
            currency=str(row["currency"]),
            deployment_limit=Decimal(str(row["capital_limit"])),
            pool_limit=Decimal(str(row["pool_limit"])),
            capital_mode=cast(Literal["fixed", "compound"], row["capital_mode"]),
            pool_reserved=Decimal(str(row["pool_reserved"])),
            mandate_max_drawdown_pct=Decimal(str(row["mandate_max_drawdown_pct"])),
            mandate_max_loss_per_position_pct=Decimal(str(row["mandate_max_loss_per_position_pct"])),
            mandate_max_daily_loss_pct=Decimal(str(row["mandate_max_daily_loss_pct"])),
            mandate_active_risk_budget_pct=Decimal(str(row["mandate_active_risk_budget_pct"])),
            mandate_cash_reserve_pct=Decimal(str(row["mandate_cash_reserve_pct"])),
            mandate_max_concurrent_positions=int(row["mandate_max_concurrent_positions"]),
            policy_revision=int(row["policy_revision"]),
            max_ticket_amount=Decimal(str(row["max_ticket_amount"])),
            stop_loss_pct=Decimal(str(row["stop_pct"])),
            take_profit_pct=Decimal(str(row["target_pct"])),
            max_quote_age_seconds=int(row["max_quote_age_seconds"]),
            max_halt_feed_age_seconds=int(row["max_halt_feed_age_seconds"]),
            max_cost_age_seconds=int(row["max_cost_age_seconds"]),
            max_reconciliation_age_seconds=int(row["max_reconciliation_age_seconds"]),
            max_instrument_exposure_pct=Decimal(str(row["max_instrument_exposure_pct"])),
            max_portfolio_exposure_pct=Decimal(str(row["max_portfolio_exposure_pct"])),
            max_drawdown_pct=Decimal(str(row["max_drawdown_pct"])),
            cost_stress_multiplier=Decimal(str(row["cost_stress_multiplier"])),
            quote_at=quote_at,
            ask=Decimal(str(row["ask"])),
            halt_feed_at=halt_feed_at,
            reserved=Decimal(str(row["reserved"])),
            declaration_id=int(row["run_declaration_id"]),
            run_id=int(row["run_id"]),
            decision_id=int(row["decision_id"]),
            pair_id=int(row["pair_id"]),
            pair_seq=int(row["pair_seq"]),
            leg=cast(Leg, row["leg"]),
            session_date=cast(date, row["session_date"]),
            horizon_days=int(row["horizon_days"]),
            size_tier=size_tier,
            requested_amount=TRIAL_TICKET_USD[size_tier],
        ),
        None,
        True,
    )


__all__ = [
    "DECLARATION_CONTRACT_PREFIX",
    "PAPER_GATE_MAP",
    "TRIAL_CURRENCY",
    "TRIAL_TICKET_USD",
    "TrialIntent",
    "declaration_digest",
    "load_trial_intent",
]
