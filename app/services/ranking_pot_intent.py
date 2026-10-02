"""Ranking-pot-v1's entry loader and slot ledger (#2842 slice 5b).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §7.2, "The loader, the slot ledger and the
executor". ``load_pot_intent`` authorises one executed-book entry in one query at submission time: the signal, its
lifecycle and ``enter`` row, the ``decided`` attempt (snapshot and ticket re-hashed), the digest-intact declaration,
the policy hash, the target session, the state, ``v1_active``, ``look_pending`` and the activation; then every
non-evidence gate of the paper ``_load_intent`` under its own code (``PAPER_GATE_MAP``). ``slot_occupancy`` reads the
slot ledger and the live book; the executor calls it again inside the authority transaction.

Nothing here touches the broker or writes a row. Policy-hashed (``ranking_pot_policy.POLICY_MODULES``).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from fractions import Fraction
from typing import Any, Final, Literal, cast
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from app.services import ranking_pot as pot
from app.services import ranking_pot_exec as exec_book
from app.services import ranking_pot_look as look
from app.services.ai_trial_pack import NonCanonicalValue, canonical_sha256
from app.services.ranking_pot_policy import RANKING_POT_POLICY_HASH
from app.services.strategy_base_currency import DEPLOYMENT_CURRENCY_UNSUPPORTED, SUPPORTED_DEPLOYMENT_CURRENCIES
from app.services.strategy_control_plane import EFFECTIVE_MAX_CONCURRENT_SQL, registered_strategy_purpose
from app.services.strategy_halt_identity import INSTRUMENT_HALT_SYMBOL_SQL
from app.services.strategy_paper_executor import _age_ok, _session_is_open

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = ZoneInfo("America/New_York")
_CENT: Final = Decimal("0.01")
_BP: Final = Decimal("0.0001")
#: The pot's tickets are USD amounts; its capital is fixed for the declaration (§7.2 "Ledger").
POT_CURRENCY: Final = "USD"
POT_CAPITAL_MODE: Final = "fixed"

#: Write nothing: the lifecycle stays ``entry_pending`` and a later fire this session retries (§7.2 "Deferrals").
DEFERRALS: Final = frozenset({"pot_slot_not_released", "pot_slot_ledger_incomplete"})

#: Every reason code the paper loader ``_load_intent`` can return, and what the pot does with it: ``kept`` = the same
#: gate under the same code; ``replaced:<code>`` = the evidence field is replaced by a pot source, refused under
#: ``<code>``; ``dropped`` = an evidence gate with no pot counterpart. ``tests/test_ranking_pot_intent.py`` parses
#: ``_load_intent``'s source and fails on an unclassified code, and on a kept or replacing code this loader cannot
#: return.
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
    # The scan watermark has no producer here; the attempt's target session is the freshness bound.
    "scan_watermark_missing": "replaced:decision_expired",
    "scan_stale": "replaced:decision_expired",
    # Forecast barriers → the frozen 3-ATR / 2R levels from the ask (§7.2 "Levels and basis").
    "opportunity_forecast_stop_exceeds_policy": "replaced:pot_stop_exceeds_policy",
    "opportunity_forecast_target_barrier_missing": "replaced:protective_levels_invalid",
    "opportunity_forecast_stop_barrier_missing": "replaced:protective_levels_invalid",
    # Evidence gates with no pot counterpart: the ranking is the snapshot's, not a forecast's.
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
class PotIntent:
    """One authorised executed-book entry. The non-evidence fields carry ``_Intent``'s names and meanings, so the
    shared capacity arithmetic reads it (``strategy_paper_executor._SizingIntent``)."""

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
    #: The sizing stop: ``100 × (ask − SL) / ask`` of the SL the executor will send (§7.2).
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
    # Pot identity.
    declaration_id: int
    attempt_id: int
    lifecycle_id: int
    slot: int
    n: int
    target_session: date
    pot_capital: Decimal
    #: The slot's wealth at load (the requested ticket); the authority transaction re-reads it.
    slot_wealth: Decimal
    atr14: Fraction
    #: The SL / TP rates to send: the frozen levels from the ask, rounded down to 6 dp, validated as sent.
    stop_rate: Decimal
    take_rate: Decimal


# ---------------------------------------------------------------------------
# Slot ledger (pure)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SlotLifecycle:
    """A lifecycle of the declaration, as the ledger reads it."""

    lifecycle_id: int
    instrument_id: int
    slot: int
    status: exec_book.Status
    #: ``allocated`` funding decision whose trade is neither ``closed`` nor ``failed`` (capital committed).
    committed: bool
    #: Booked net P&L of a ``closed`` lifecycle; ``None`` = not completely booked (or not closed).
    booked_pnl: Decimal | None
    #: A close event carries ``fees_usd`` ≠ 0 or NULL: whether ``netProfit`` nets it is undocumented.
    fees_ambiguous: bool = False


def slot_wealth(pot_capital: Decimal, n: int, slot: int, lifecycles: Sequence[SlotLifecycle]) -> Decimal | str:
    """W = ``POT_CAPITAL / N`` + Σ booked net P&L of the slot's ``closed`` lifecycles, rounded down to the cent; or
    ``pot_slot_fees_ambiguous`` (permanent until the fee semantics are documented) / ``pot_slot_ledger_incomplete``
    (a booking not yet ingested). ``failed``, ``refused`` and ``expired`` lifecycles add nothing (§7.2)."""
    if n < 1 or not pot_capital.is_finite() or pot_capital <= 0:
        raise ValueError("POT_CAPITAL and N must be positive")
    total = pot_capital / n
    closed = [lc for lc in lifecycles if lc.slot == slot and lc.status == "closed"]
    if any(lc.fees_ambiguous for lc in closed):
        return "pot_slot_fees_ambiguous"
    for lc in closed:
        if lc.booked_pnl is None:
            return "pot_slot_ledger_incomplete"
        total += lc.booked_pnl
    return total.quantize(_CENT, rounding=ROUND_DOWN)


def occupancy_refusal(
    *, lifecycle_id: int, instrument_id: int, slot: int, n: int, lifecycles: Sequence[SlotLifecycle]
) -> str | None:
    """The slot and name checks (§7.2 "Slots"), in order: another lifecycle still holds the slot (a stamped exit not
    yet closed, or a late fill reviving a released one) → the deferral ``pot_slot_not_released``; another live
    lifecycle holds the name → ``pot_name_in_flight``; N lifecycles already commit capital → ``pot_slots_full``."""
    others = [lc for lc in lifecycles if lc.lifecycle_id != lifecycle_id]
    if any(lc.slot == slot and lc.status in exec_book.HELD and lc.committed for lc in others):
        return "pot_slot_not_released"
    if any(lc.instrument_id == instrument_id and lc.status in exec_book.HELD and lc.committed for lc in others):
        return "pot_name_in_flight"
    if sum(1 for lc in others if lc.committed) >= n:
        return "pot_slots_full"
    return None


_SLOT_LEDGER_SQL: Final = """
    SELECT l.lifecycle_id, l.instrument_id, l.slot, a.target_session,
           fd.verdict AS funding_verdict, t.status AS trade_status,
           own.ownerships, own.active_ownerships, own.ownerships_unclosed,
           own.closes, own.realised, own.realised_complete, own.fees_zero
      FROM ranking_pot_exec_lifecycles l
      JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = l.attempt_id
      LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
      LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
      LEFT JOIN LATERAL (
          SELECT count(*) AS ownerships,
                 count(*) FILTER (WHERE o.status = 'active') AS active_ownerships,
                 count(*) FILTER (WHERE c.closes IS NULL) AS ownerships_unclosed,
                 coalesce(sum(c.closes), 0) AS closes,
                 sum(c.realised) AS realised,
                 coalesce(bool_and(c.realised_complete), false) AS realised_complete,
                 coalesce(bool_and(c.fees_zero), true) AS fees_zero
            FROM strategy_position_ownership o
            LEFT JOIN LATERAL (
                SELECT count(*) AS closes, sum(te.realized_pnl_usd) AS realised,
                       bool_and(te.realized_pnl_usd IS NOT NULL) AS realised_complete,
                       bool_and(te.fees_usd IS NOT NULL AND te.fees_usd = 0) AS fees_zero
                  FROM trade_events te
                 WHERE te.position_id = o.broker_position_id AND te.event_kind = 'close'
                HAVING count(*) > 0
            ) c ON true
           WHERE o.strategy_trade_id = t.strategy_trade_id
      ) own ON true
     WHERE l.declaration_id = %(d)s
     ORDER BY l.lifecycle_id
"""


def _booked(r: dict[str, Any]) -> Decimal | None:
    """A ``closed`` trade's net P&L: the sum of its close events' ``netProfit`` (one row per partial close, unique per
    position and close time — ``load_owned_pnl``'s convention), booked only when it owns at least one position, every
    one is released and has close events, and every ``netProfit`` is present and finite."""
    if not (
        int(r["ownerships"] or 0) > 0
        and int(r["active_ownerships"]) == 0
        and int(r["ownerships_unclosed"]) == 0
        and bool(r["realised_complete"])
        and r["realised"] is not None
    ):
        return None
    value = Decimal(str(r["realised"]))
    return value if value.is_finite() else None


def read_slot_lifecycles(conn: Conn, declaration_id: int, *, ny_today: date) -> list[SlotLifecycle]:
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(_SLOT_LEDGER_SQL, {"d": declaration_id}).fetchall()
    out: list[SlotLifecycle] = []
    for r in rows:
        status = exec_book.classify(
            funding_verdict=r["funding_verdict"],
            trade_status=r["trade_status"],
            target_session=r["target_session"],
            ny_today=ny_today,
        )
        out.append(
            SlotLifecycle(
                lifecycle_id=int(r["lifecycle_id"]),
                instrument_id=int(r["instrument_id"]),
                slot=int(r["slot"]),
                status=status,
                committed=r["funding_verdict"] == "allocated" and r["trade_status"] not in ("closed", "failed"),
                booked_pnl=_booked(r) if status == "closed" else None,
                fees_ambiguous=status == "closed" and not bool(r["fees_zero"]),
            )
        )
    return out


@dataclass(frozen=True)
class SlotState:
    wealth: Decimal


def slot_state(
    conn: Conn,
    *,
    declaration_id: int,
    lifecycle_id: int,
    instrument_id: int,
    slot: int,
    n: int,
    pot_capital: Decimal,
    now: datetime,
) -> SlotState | str:
    """The slot ledger and the live book for one lifecycle, or the refusal / deferral code."""
    lifecycles = read_slot_lifecycles(conn, declaration_id, ny_today=now.astimezone(_NEW_YORK).date())
    refusal = occupancy_refusal(
        lifecycle_id=lifecycle_id, instrument_id=instrument_id, slot=slot, n=n, lifecycles=lifecycles
    )
    if refusal is not None:
        return refusal
    wealth = slot_wealth(pot_capital, n, slot, lifecycles)
    if isinstance(wealth, str):
        return wealth
    if wealth <= 0:
        return "pot_slot_depleted"
    return SlotState(wealth=wealth)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------
_POT_INTENT_SQL: Final = f"""
    SELECT s.signal_id, s.strategy_id, s.strategy_version, s.instrument_id, s.signal_bar_date, s.fill_bar_date,
           s.fill_price, s.input_rule_set_versions ->> 'ranking_pot_policy' AS signal_policy_hash,
           i.symbol, i.is_tradable, e.asset_class,
           l.lifecycle_id, l.slot, l.ticket, l.ticket_sha256, l.declaration_id AS lifecycle_declaration_id,
           dr.action AS decision_action, dr.lifecycle_id AS decision_lifecycle_id,
           a.attempt_id, a.outcome, a.target_session, a.policy_hash AS attempt_policy_hash,
           a.declaration_id AS attempt_declaration_id,
           decl.strategy_id AS declared_strategy_id, decl.strategy_version AS declared_version,
           decl.doc, decl.doc_sha256,
           state.to_state AS pot_state,
           act.pot_capital,
           EXISTS (
               SELECT 1 FROM ai_trial_declarations td
                WHERE (SELECT te.to_state FROM ai_trial_state_events te
                        WHERE te.declaration_id = td.declaration_id ORDER BY te.event_id DESC LIMIT 1) = 'active'
           ) AS v1_active,
           pd.close AS current_close,
           (
               EXISTS (
                   SELECT 1 FROM price_series_break psb
                   WHERE psb.instrument_id = s.instrument_id AND psb.break_date > s.signal_bar_date
                     AND psb.break_date <= %(today)s
               )
               OR EXISTS (
                   SELECT 1 FROM price_adjustments pa
                   WHERE pa.instrument_id = s.instrument_id AND pa.superseded_by IS NULL
                     AND pa.effective_date > s.signal_bar_date AND pa.effective_date <= %(today)s
               )
           ) AS scale_changed,
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
    LEFT JOIN ranking_pot_exec_lifecycles l ON l.signal_id = s.signal_id
    LEFT JOIN ranking_pot_exec_decisions dr
      ON dr.attempt_id = l.attempt_id AND dr.instrument_id = l.instrument_id
    LEFT JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = l.attempt_id
    LEFT JOIN ranking_pot_declarations decl ON decl.declaration_id = l.declaration_id
    LEFT JOIN LATERAL (
        SELECT se.to_state FROM ranking_pot_state_events se
        WHERE se.declaration_id = l.declaration_id
        ORDER BY se.event_id DESC LIMIT 1
    ) state ON true
    LEFT JOIN ranking_pot_activations act ON act.declaration_id = l.declaration_id
    LEFT JOIN price_daily pd ON pd.instrument_id = s.instrument_id AND pd.price_date = s.signal_bar_date
    LEFT JOIN strategy_deployments d
      ON d.strategy_id = s.strategy_id AND d.strategy_version = s.strategy_version AND d.mode = 'paper'
    LEFT JOIN strategy_execution_policies p ON p.deployment_id = d.deployment_id
    LEFT JOIN LATERAL (
        SELECT enabled, capital_limit, capital_mode, risk_profile,
               max_portfolio_drawdown_pct, max_loss_per_position_pct, max_daily_loss_pct,
               active_risk_budget_pct, cash_reserve_pct,
               {EFFECTIVE_MAX_CONCURRENT_SQL} AS max_concurrent_positions
        FROM strategy_paper_pool_events
        ORDER BY strategy_paper_pool_event_id DESC
        LIMIT 1
    ) pool ON true
    LEFT JOIN quotes q ON q.instrument_id = s.instrument_id
    LEFT JOIN strategy_halt_feed_state h ON h.source = 'nasdaq_trader_rss'
    WHERE s.signal_id = %(signal_id)s AND s.signal_kind = 'entry' AND s.verdict = 'fired'
"""


def _positive_finite(value: object) -> bool:
    if value is None:
        return False
    d = Decimal(str(value))
    return d.is_finite() and d > 0


def _rehashes(doc: object, digest: object) -> bool:
    try:
        return canonical_sha256(doc) == digest
    except NonCanonicalValue:
        return False


def _snapshot_intact(conn: Conn, attempt_id: int) -> bool:
    """r3-111: the decided snapshot re-hashes to its stored sha256 (read apart from the authorising row: it is
    large)."""
    row = conn.execute(
        "SELECT snapshot, snapshot_sha256 FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s", (attempt_id,)
    ).fetchone()
    return row is not None and row[0] is not None and _rehashes(row[0], row[1])


def _session_reason(target: date, *, now: datetime) -> str | None:
    today = now.astimezone(_NEW_YORK).date()
    if target < today:
        return "decision_expired"
    if target > today:
        return "decision_not_yet_due"
    return None


def _atr14(ticket: object) -> Fraction | None:
    """The snapshot ATR14 the rebalance recorded on the §6 ticket (``planned_levels.atr14``, a ``Fraction`` string)."""
    try:
        value = Fraction(str(cast(dict[str, Any], ticket)["planned_levels"]["atr14"]))
    except KeyError, TypeError, ValueError, ZeroDivisionError:
        return None
    return value if value > 0 else None


def sent_levels(ask: Decimal, atr: Fraction) -> tuple[Decimal, Decimal] | None:
    """The frozen 3-ATR / 2R levels from the ask (``ranking_pot.planned_levels``), rounded DOWN to 6 dp as the paper
    path sends them, then validated AS SENT (``ranking_pot.validate_levels``). ``None`` = ``protective_levels_invalid``.
    """
    planned = pot.planned_levels(ask, atr)
    if isinstance(planned, str):
        return None
    six = Decimal("0.000001")
    stop = (Decimal(planned.stop_loss.numerator) / Decimal(planned.stop_loss.denominator)).quantize(six, ROUND_DOWN)
    take = (Decimal(planned.take_profit.numerator) / Decimal(planned.take_profit.denominator)).quantize(six, ROUND_DOWN)
    if not pot.validate_levels(Fraction(ask), Fraction(stop), Fraction(take), atr):
        return None
    return stop, take


def load_pot_intent(conn: Conn, *, signal_id: int, now: datetime) -> tuple[PotIntent | None, str | None, bool]:
    """Authorise one executed-book entry, or return the refusal code. Same contract as the paper ``_load_intent``:
    the bool says the halt SQL ran, so the caller can stamp the halt identity rule version on a refusal."""
    purpose_row = conn.execute("SELECT strategy_id FROM strategy_signals WHERE signal_id = %s", (signal_id,)).fetchone()
    if purpose_row is None:
        return None, "signal_not_fired_entry", False
    if str(purpose_row[0]) != pot.STRATEGY_ID or registered_strategy_purpose(str(purpose_row[0])) != "demo_trial":
        return None, "strategy_not_ranking_pot", False
    with conn.cursor(row_factory=dict_row) as cur:
        row = cur.execute(
            _POT_INTENT_SQL, {"signal_id": signal_id, "today": now.astimezone(_NEW_YORK).date()}
        ).fetchone()
    if row is None:
        return None, "signal_not_fired_entry", False

    # §7.2 authorisation first: a signal the pot never decided is refused before any market fact is read.
    identity = (
        row["lifecycle_id"] is not None
        and row["decision_action"] == "enter"
        and row["decision_lifecycle_id"] == row["lifecycle_id"]
        and row["outcome"] == "decided"
        and row["attempt_declaration_id"] == row["lifecycle_declaration_id"]
        and row["strategy_id"] == row["declared_strategy_id"] == pot.STRATEGY_ID
        and row["strategy_version"] == row["declared_version"]
        and row["fill_bar_date"] == row["target_session"]
    )
    if not identity:
        return None, "pot_identity_mismatch", True
    if not (
        _rehashes(row["doc"], row["doc_sha256"])
        and _rehashes(row["ticket"], row["ticket_sha256"])
        and _snapshot_intact(conn, int(row["attempt_id"]))
    ):
        return None, "pot_not_intact", True
    if not (
        row["signal_policy_hash"]
        == row["attempt_policy_hash"]
        == row["doc"].get("policy_hash")
        == RANKING_POT_POLICY_HASH
    ):
        return None, "pot_policy_drift", True
    target = cast(date, row["target_session"])
    session_reason = _session_reason(target, now=now)
    if session_reason is not None:
        return None, session_reason, True
    if row["pot_state"] != "executing":
        return None, "pot_not_executing", True
    if bool(row["v1_active"]):
        return None, "v1_active", True
    declaration_id = int(row["lifecycle_declaration_id"])
    if look.look_pending(conn, declaration_id, target) is not None:
        return None, "look_pending", True
    if not _positive_finite(row["pot_capital"]):
        return None, "pot_capital_missing", True
    n = int(row["doc"]["terms"]["n"])
    pot_capital = Decimal(str(row["pot_capital"]))
    # Before the market gates (Codex ckpt-1): an entry whose slot is not yet released DEFERS, and a stale quote at
    # 15:00 must not consume its one attempt first.
    slot = slot_state(
        conn,
        declaration_id=declaration_id,
        lifecycle_id=int(row["lifecycle_id"]),
        instrument_id=int(row["instrument_id"]),
        slot=int(row["slot"]),
        n=n,
        pot_capital=pot_capital,
        now=now,
    )
    if isinstance(slot, str):
        return None, slot, True

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
        (row["currency"] == POT_CURRENCY, "pot_currency_not_usd"),
        (row["policy_revision"] is not None, "execution_policy_missing"),
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
    # §7.2: the split check before any level is priced from the ask.
    if bool(row["scale_changed"]) or not pot.basis_unchanged(
        _decimal(row["fill_price"]), _decimal(row["current_close"])
    ):
        return None, "basis_changed", True
    ask = Decimal(str(row["ask"]))
    atr = _atr14(row["ticket"])
    levels = None if atr is None else sent_levels(ask, atr)
    if atr is None or levels is None:
        return None, "protective_levels_invalid", True
    stop_rate, take_rate = levels
    # The sizing stop rounds UP (a wider stop sizes smaller under `loss_at_stop`); the target is informational.
    stop_pct = ((ask - stop_rate) * 100 / ask).quantize(_BP, ROUND_UP)
    take_pct = ((take_rate - ask) * 100 / ask).quantize(_BP, ROUND_DOWN)
    # The paper policy's stop bound still applies (as v1's `decision_stop_exceeds_policy`).
    if not (_positive_finite(row["policy_stop_loss_pct"]) and stop_pct <= Decimal(str(row["policy_stop_loss_pct"]))):
        return None, "pot_stop_exceeds_policy", True
    if not _age_ok(halt_feed_at, now=now, max_seconds=int(row["max_halt_feed_age_seconds"])):
        return None, "halt_feed_stale", True
    if not _session_is_open(now):
        return None, "market_session_closed", True
    return (
        PotIntent(
            signal_id=signal_id,
            strategy_id=str(row["strategy_id"]),
            strategy_version=str(row["strategy_version"]),
            instrument_id=int(row["instrument_id"]),
            symbol=str(row["symbol"]),
            deployment_id=int(row["deployment_id"]),
            currency=str(row["currency"]),
            deployment_limit=Decimal(str(row["capital_limit"])),
            pool_limit=Decimal(str(row["pool_limit"])),
            capital_mode=POT_CAPITAL_MODE,
            pool_reserved=Decimal(str(row["pool_reserved"])),
            mandate_max_drawdown_pct=Decimal(str(row["mandate_max_drawdown_pct"])),
            mandate_max_loss_per_position_pct=Decimal(str(row["mandate_max_loss_per_position_pct"])),
            mandate_max_daily_loss_pct=Decimal(str(row["mandate_max_daily_loss_pct"])),
            mandate_active_risk_budget_pct=Decimal(str(row["mandate_active_risk_budget_pct"])),
            mandate_cash_reserve_pct=Decimal(str(row["mandate_cash_reserve_pct"])),
            mandate_max_concurrent_positions=int(row["mandate_max_concurrent_positions"]),
            policy_revision=int(row["policy_revision"]),
            max_ticket_amount=Decimal(str(row["max_ticket_amount"])),
            stop_loss_pct=stop_pct,
            take_profit_pct=take_pct,
            max_quote_age_seconds=int(row["max_quote_age_seconds"]),
            max_halt_feed_age_seconds=int(row["max_halt_feed_age_seconds"]),
            max_cost_age_seconds=int(row["max_cost_age_seconds"]),
            max_reconciliation_age_seconds=int(row["max_reconciliation_age_seconds"]),
            max_instrument_exposure_pct=Decimal(str(row["max_instrument_exposure_pct"])),
            max_portfolio_exposure_pct=Decimal(str(row["max_portfolio_exposure_pct"])),
            max_drawdown_pct=Decimal(str(row["max_drawdown_pct"])),
            cost_stress_multiplier=Decimal(str(row["cost_stress_multiplier"])),
            quote_at=quote_at,
            ask=ask,
            halt_feed_at=halt_feed_at,
            reserved=Decimal(str(row["reserved"])),
            declaration_id=declaration_id,
            attempt_id=int(row["attempt_id"]),
            lifecycle_id=int(row["lifecycle_id"]),
            slot=int(row["slot"]),
            n=n,
            target_session=target,
            pot_capital=pot_capital,
            slot_wealth=slot.wealth,
            atr14=cast(Fraction, atr),
            stop_rate=stop_rate,
            take_rate=take_rate,
        ),
        None,
        True,
    )


def _decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


__all__ = [
    "DEFERRALS",
    "PAPER_GATE_MAP",
    "POT_CAPITAL_MODE",
    "POT_CURRENCY",
    "PotIntent",
    "SlotLifecycle",
    "SlotState",
    "load_pot_intent",
    "occupancy_refusal",
    "read_slot_lifecycles",
    "sent_levels",
    "slot_state",
    "slot_wealth",
]
