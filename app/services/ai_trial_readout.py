"""#3471 §9 readout — per-leg net per-trade %, the pair unit *d*, the cluster sign-flip p, the harm
stop's looks and the descriptive tables.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §9 "Unit", "Primary
statistic", "Cohort", "Harm stop", "Readout"; O12 (exit labels); O13 (arithmetic).

- **Unit.** A pair whose ``pair_unit_state`` is ``unit`` (both legs filled, each closed or
  censored) AND whose two legs are valued. A unit a leg of which cannot be valued is counted in the
  ``unvalued`` census, never repaired. *d* = arm net % − control net %.
- **Net per-trade % (closed leg).** 100 × Σ ``realized_pnl_usd`` ÷ Σ ``investment_usd`` over the
  leg's broker close rows (``trade_events``, reached only through ``strategy_position_ownership``).
  Only rows recorded within ``FLOW_WINDOW_SESSIONS`` sessions of their close count; a later row is
  a restatement line. ⚠ Source rule: eToro's trade-history schema documents ``netProfit`` as "the
  net profit of the trade" and ``fees`` as "the fees of the trade" and does not say whether one
  includes the other (``trading--demo/list-trading-history``, read 2026-09-29). Every stored close
  row carries ``fees = 0`` and ``netProfit = (close − open) × units`` (``select count(*),
  count(*) filter (where fees_usd <> 0) from trade_events where event_kind = 'close'``), so the
  question has never been observable. By construction the net is ``netProfit`` and a nonzero fee
  is COUNTED (``fee_rows``), not subtracted — subtracting could double-charge. The schema carries
  no dividend or financing field, so neither is in the net (a real-stock long carries no
  financing; a dividend inside a ≤ 10-session hold is the stated gap).
- **Net per-trade % (censored leg).** §9: valued at its last available price minus half the
  recorded spread, and flagged. Both come from ONE record: the latest ``etoro_rate_observations``
  row observed by the censoring instant (15:00 UTC on the censor session). Its mid less half its
  spread is its bid, so the mark is that bid. The table is append-only, so a rerun cannot see a
  price revised after the instant — ``price_daily`` has no ingest time and could (Codex ckpt-2).
  It is also as-traded, like the entry price, so a split inside the hold cannot mis-scale it. No
  valid quote, a non-USD price or a close slice before the instant (a partial close: marking every
  opened unit would double-count it) leaves the leg unvalued.
- **Exit labels (O12).** An applied engine close carries its ``trigger_code``: ``exit_deadline``
  is the mechanical ``deadline``, anything else is its own non-mechanical label. A broker-side
  close is ``stop`` at or through the recorded stop, ``target`` at or through the target, else
  ``broker_other`` (manual, forced or delisting — non-mechanical). The O12 sensitivity drops a unit
  either leg of which has a non-mechanical label; ``censored`` is mechanical (it is §9's clock).
- **Cohort.** Sessions 1–40 from the first fill session; the readout is due 5 sessions after the
  last cohort pair resolves, and no primary number is computed before that. Later pairs are
  exploratory.
- **Harm looks.** Every ``HARM_LOOK_EVERY``-th unit in entry (``pair_seq``) order, once the
  first 10·k pairs are all valued units (broken and settled-unvalued pairs are outside the
  order); none before ``HARM_MIN_CLUSTERS`` clusters; look k
  halts if the one-sided (``less``) p < ``HARM_ALPHA`` · 2^−k. Computed here, over every pair
  (monitoring continues through exploration); nothing in this module writes a state event.
- **Seed.** The Monte-Carlo flips (above 16 clusters) are drawn from
  ``sha256(declaration sha | "readout")`` — declared by construction, as the §7 draw's seed is.

- **Cohort resolution.** A broken pair whose filled leg has not yet exited still holds the readout
  (``live_legs``); its exit session enters the due-date anchor.

Not computed here (named, so their absence is visible): SPY references (O13), arm-versus-control
exposure, turnover and the fill-versus-ask gap — the executor does not persist the ask it priced a
leg from, so that gap has no stored input.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.services.ai_trial_deadline import TRIAL_EXIT_TIME_UTC, exit_deadline_session, fill_session
from app.services.ai_trial_pair_lifecycle import (
    LEGS,
    TRIAL_CENSOR_SESSIONS,
    PairUnitState,
    pair_unit_state,
)
from app.services.ai_trial_stats import flip_set, sign_flip_p
from app.services.block_bootstrap import BootstrapResult, block_bootstrap_expectancy, cluster_by_date

#: §9 "Cohort": NYSE sessions 1–40, session 1 = the first fill session.
COHORT_SESSIONS: Final = 40
#: §9 "When the readout runs": every cohort pair resolved, plus 5 sessions.
READOUT_WAIT_SESSIONS: Final = 5
#: §9 "Too little data".
MIN_UNITS: Final = 30
MIN_CLUSTERS: Final = 10
#: §9 "Harm stop".
HARM_LOOK_EVERY: Final = 10
HARM_MIN_CLUSTERS: Final = 8
HARM_ALPHA: Final = 0.05
#: §9 "Net per-trade %": only flows posted within 5 sessions of the close count.
FLOW_WINDOW_SESSIONS: Final = 5
#: O12: the mechanical exits. ``censored`` is §9's own clock and counts with them.
MECHANICAL_EXITS: Final = frozenset({"stop", "target", "deadline", "censored"})

INSUFFICIENT: Final = "insufficient evidence"
#: §9 "Reading", verbatim in substance: the p is descriptive and its null is approximate.
READING: Final = (
    "the model's picks, under its own terms, did better than random picks from this shortlist on demo, "
    "beyond what sign-symmetric noise usually produces — only if p is small; not a proof of edge"
)

Status = Literal["no_fills", "not_due", "due"]


def readout_seed(declaration_sha256: str) -> int:
    """The declared Monte-Carlo seed: 64 bits of sha256(declaration sha | "readout")."""
    return int.from_bytes(hashlib.sha256(f"{declaration_sha256}|readout".encode()).digest()[:8], "big")


# --------------------------------------------------------------------------------------------
# Leg valuation (pure)
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CloseRow:
    """One broker close slice of a leg's position (``trade_events``, ``event_kind = 'close'``)."""

    executed_at: datetime
    recorded_at: datetime
    realized_pnl_usd: Decimal | None
    investment_usd: Decimal | None
    fees_usd: Decimal | None
    price: Decimal | None
    stop_rate: Decimal | None
    take_rate: Decimal | None


@dataclass(frozen=True)
class CensorMark:
    """The inputs of a censored leg's mark; any ``None`` leaves the leg unvalued.

    ``bid`` / ``ask`` are the latest ``etoro_rate_observations`` row observed by the censoring
    instant: an append-only record, so a rerun can never see a later price."""

    average_price: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    usd: bool
    #: A broker close slice executed before the censoring instant (a partial close).
    partial_close: bool


@dataclass(frozen=True)
class LegValue:
    net_pct: float | None
    #: USD opened (closed: Σ counted slice investment; censored: units × average price).
    open_amount: float | None
    pnl_usd: float | None
    exit_label: str
    unvalued_reason: str | None = None
    restated_rows: int = 0
    fee_rows: int = 0


def exit_label(rows: Sequence[CloseRow], close_trigger: str | None) -> str:
    """O12 label of a closed leg (long only, §13)."""
    if close_trigger is not None:
        return "deadline" if close_trigger == "exit_deadline" else close_trigger
    last = max(rows, key=lambda row: row.executed_at, default=None)
    if last is None or last.price is None:
        return "broker_other"
    if last.stop_rate is not None and last.stop_rate > 0 and last.price <= last.stop_rate:
        return "stop"
    if last.take_rate is not None and last.take_rate > 0 and last.price >= last.take_rate:
        return "target"
    return "broker_other"


def value_closed_leg(rows: Sequence[CloseRow], close_trigger: str | None) -> LegValue:
    label = exit_label(rows, close_trigger)
    counted: list[CloseRow] = []
    restated = 0
    for row in rows:
        window_end = exit_deadline_session(fill_session(row.executed_at), FLOW_WINDOW_SESSIONS)
        if fill_session(row.recorded_at) <= window_end:
            counted.append(row)
        else:
            restated += 1
    fee_rows = sum(1 for row in counted if row.fees_usd is not None and row.fees_usd != 0)
    if not counted:
        return LegValue(None, None, None, label, "close_flow_missing", restated, fee_rows)
    if any(row.realized_pnl_usd is None or row.investment_usd is None for row in counted):
        return LegValue(None, None, None, label, "close_flow_incomplete", restated, fee_rows)
    invested = sum((row.investment_usd for row in counted if row.investment_usd is not None), Decimal(0))
    pnl = sum((row.realized_pnl_usd for row in counted if row.realized_pnl_usd is not None), Decimal(0))
    if invested <= 0:
        return LegValue(None, None, None, label, "close_flow_incomplete", restated, fee_rows)
    return LegValue(float(100 * pnl / invested), float(invested), float(pnl), label, None, restated, fee_rows)


def value_censored_leg(mark: CensorMark, units: Decimal | None) -> LegValue:
    """§9: the last available price minus half the recorded spread, flagged ``censored``.

    The recorded quote's mid less half its spread is its bid, so the mark IS the recorded bid —
    the price a long exits at."""
    reason: str | None = None
    if not mark.usd:
        reason = "non_usd_price"
    elif mark.partial_close:
        # Marking every opened unit would double-count the slice already closed.
        reason = "partial_close_before_censor"
    elif mark.average_price is None or mark.average_price <= 0 or units is None or units <= 0:
        reason = "entry_missing"
    elif mark.bid is None or mark.ask is None or not 0 < mark.bid <= mark.ask:
        reason = "quote_missing"
    if reason is not None:
        return LegValue(None, None, None, "censored", reason)
    assert mark.bid is not None and mark.average_price is not None and units is not None
    opened = units * mark.average_price
    pnl = units * (mark.bid - mark.average_price)
    return LegValue(float(100 * pnl / opened), float(opened), float(pnl), "censored")


# --------------------------------------------------------------------------------------------
# Statistics (pure)
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class PairRecord:
    pair_seq: int
    #: The cluster: the run's target session, which is the session the pair was entered on.
    session_date: date
    state: PairUnitState
    broken_reasons: tuple[str, ...]
    arm: LegValue | None
    control: LegValue | None
    regime_label: str | None
    confidence: int | None
    #: Session of the latest leg exit (close or censor), over the legs that exited.
    resolved_session: date | None
    #: Legs filled and not yet closed or censored. A broken pair holds one when a leg filled and
    #: the other was refused; it holds the cohort readout until it exits.
    live_legs: int = 0
    #: §7: the control pool's size (exhaustion conditions *d* on control availability).
    pool_size: int | None = None

    @property
    def valued(self) -> bool:
        return (
            self.state == "unit"
            and self.arm is not None
            and self.control is not None
            and self.arm.net_pct is not None
            and self.control.net_pct is not None
        )

    @property
    def d(self) -> float:
        assert self.arm is not None and self.control is not None
        assert self.arm.net_pct is not None and self.control.net_pct is not None
        return self.arm.net_pct - self.control.net_pct

    @property
    def mechanical(self) -> bool:
        return all(leg is not None and leg.exit_label in MECHANICAL_EXITS for leg in (self.arm, self.control))


def cluster_sums(units: Sequence[PairRecord]) -> list[float]:
    sums: dict[date, float] = {}
    for unit in units:
        sums[unit.session_date] = sums.get(unit.session_date, 0.0) + unit.d
    return [sums[session] for session in sorted(sums)]


@dataclass(frozen=True)
class Primary:
    units: int
    clusters: int
    mean_d: float | None
    p: float | None
    verdict: str


def primary(units: Sequence[PairRecord], *, seed: int) -> Primary:
    sums = cluster_sums(units)
    mean_d = sum(unit.d for unit in units) / len(units) if units else None
    if len(units) < MIN_UNITS or len(sums) < MIN_CLUSTERS:
        return Primary(len(units), len(sums), mean_d, None, INSUFFICIENT)
    p = sign_flip_p(sums, flip_set(len(sums), seed=seed), alternative="greater")
    return Primary(len(units), len(sums), mean_d, p, READING)


@dataclass(frozen=True)
class HarmLook:
    k: int
    units: int
    clusters: int
    p_less: float | None
    threshold: float
    halts: bool
    #: ``too_few_clusters`` when the look was due but below ``HARM_MIN_CLUSTERS``.
    skipped: str | None = None


def settled_unvalued(pair: PairRecord, today: date) -> bool:
    """A unit that cannot be valued and never will be: its flow window closed with a leg still
    unvalued, so no later row can count (a late row is a restatement)."""
    return (
        pair.state == "unit"
        and not pair.valued
        and pair.resolved_session is not None
        and today > exit_deadline_session(pair.resolved_session, FLOW_WINDOW_SESSIONS)
    )


def harm_looks(pairs: Sequence[PairRecord], *, seed: int, today: date) -> list[HarmLook]:
    """Every look due so far, as of the session ``today``. A look k is due once the first 10·k
    pairs in entry order are all valued units. Broken and settled-unvalued pairs are outside the
    sequence (census only), and so is a blocked pair until it resolves; an open or still-valuable
    pair ahead holds it back, since it may yet enter."""
    # §9 "Unresolved legs": a blocked pair is excluded until it resolves, then re-enters at its
    # entry position.
    ordered = sorted(
        (pair for pair in pairs if pair.state not in ("broken", "blocked") and not settled_unvalued(pair, today)),
        key=lambda pair: pair.pair_seq,
    )
    ready = 0
    for pair in ordered:
        if not pair.valued:
            break
        ready += 1
    looks: list[HarmLook] = []
    for k in range(1, ready // HARM_LOOK_EVERY + 1):
        subset = ordered[: k * HARM_LOOK_EVERY]
        sums = cluster_sums(subset)
        threshold = HARM_ALPHA * 2.0**-k
        if len(sums) < HARM_MIN_CLUSTERS:
            looks.append(HarmLook(k, len(subset), len(sums), None, threshold, False, "too_few_clusters"))
            continue
        p = sign_flip_p(sums, flip_set(len(sums), seed=seed), alternative="less")
        looks.append(HarmLook(k, len(subset), len(sums), p, threshold, p < threshold))
    return looks


@dataclass(frozen=True)
class ProfitFactor:
    """Σ positive ÷ |Σ negative|; ``None`` with ``losses == 0`` (reported as counts) or no trades."""

    value: float | None
    wins: int
    losses: int
    trades: int


def profit_factor(values: Iterable[float]) -> ProfitFactor:
    items = list(values)
    gains = sum(value for value in items if value > 0)
    losses = sum(value for value in items if value < 0)
    n_losses = sum(1 for value in items if value < 0)
    value = gains / -losses if n_losses else None
    return ProfitFactor(value, sum(1 for value in items if value > 0), n_losses, len(items))


@dataclass(frozen=True)
class LegSummary:
    mean_net_pct: float | None
    profit_factor: ProfitFactor
    #: Σ pnl ÷ Σ open amount, in %.
    capital_weighted_net_pct: float | None


def leg_summary(legs: Sequence[LegValue]) -> LegSummary:
    nets = [leg.net_pct for leg in legs if leg.net_pct is not None]
    opened = sum(leg.open_amount or 0.0 for leg in legs)
    pnl = sum(leg.pnl_usd or 0.0 for leg in legs)
    return LegSummary(
        sum(nets) / len(nets) if nets else None,
        profit_factor(nets),
        100 * pnl / opened if opened > 0 else None,
    )


@dataclass(frozen=True)
class GroupRow:
    group: str
    units: int
    mean_d: float


def by_group(units: Sequence[PairRecord], key: str) -> list[GroupRow]:
    groups: dict[str, list[float]] = {}
    for unit in units:
        raw = getattr(unit, key)
        groups.setdefault("unlabelled" if raw is None else str(raw), []).append(unit.d)
    return [GroupRow(name, len(ds), sum(ds) / len(ds)) for name, ds in sorted(groups.items())]


# --------------------------------------------------------------------------------------------
# Cohort (pure)
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Cohort:
    first_fill_session: date | None
    last_session: date | None
    #: The first session the readout may run, once every cohort pair has resolved.
    due_session: date | None
    status: Status
    detail: str


def cohort(pairs: Sequence[PairRecord], first_fill: date | None, today: date) -> Cohort:
    if first_fill is None:
        return Cohort(None, None, None, "no_fills", "no leg has filled; session 1 has not happened")
    last = exit_deadline_session(first_fill, COHORT_SESSIONS - 1)
    members = [pair for pair in pairs if pair.session_date <= last]
    if today <= last:
        return Cohort(first_fill, last, None, "not_due", f"enrollment runs to {last}")
    # A broken pair is resolved only once its filled leg (if any) has exited too; a unit only once
    # its §9 regime label is written (the lifecycle writer defers it, never guesses it).
    pending = [
        pair.pair_seq
        for pair in members
        if pair.state not in ("broken", "unit")
        or pair.live_legs
        or (pair.state == "unit" and pair.regime_label is None)
    ]
    if pending:
        return Cohort(first_fill, last, None, "not_due", f"cohort pairs not yet resolved: {pending}")
    resolved = [pair.resolved_session for pair in members if pair.resolved_session is not None]
    anchor = max([last, *resolved])
    due = exit_deadline_session(anchor, READOUT_WAIT_SESSIONS)
    # ``today`` is the session ``as_of`` belongs to; the due session must have FINISHED, since a
    # row recorded late on it still counts.
    if today <= due:
        return Cohort(first_fill, last, due, "not_due", f"cash-flow window closes with session {due}")
    return Cohort(first_fill, last, due, "due", f"due since {due}")


# --------------------------------------------------------------------------------------------
# The readout
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Readout:
    declaration_id: int
    strategy_version: str
    as_of: datetime
    seed: int
    cohort: Cohort
    pair_states: dict[str, int]
    broken_reasons: dict[str, int]
    unvalued_reasons: dict[str, int]
    run_census: dict[str, int]
    decision_census: dict[str, int]
    model_cost_usd_total: float
    model_cost_usd_per_run: float | None
    harm_looks: list[HarmLook]
    restated_rows: int
    fee_rows: int
    #: §7: every pair's control-pool size, by ``pair_seq``.
    pool_sizes: dict[int, int | None]
    #: Computed only once the cohort readout is due; ``None`` before.
    primary: Primary | None = None
    #: O12: the primary over units whose legs both exited mechanically.
    mechanical_only: Primary | None = None
    arm: LegSummary | None = None
    #: §9 "Absolute return": the house C3 date-clustered bootstrap of the arm's mean per-trade net.
    #: ⚠ Labelled: it ignores cross-session dependence beyond its blocks, so its coverage is not
    #: reliable, and no profitability claim is made from it.
    arm_interval: BootstrapResult | None = None
    control: LegSummary | None = None
    capital_weighted_d_pct: float | None = None
    per_regime: list[GroupRow] = field(default_factory=list)
    per_confidence: list[GroupRow] = field(default_factory=list)
    exit_labels: dict[str, dict[str, int]] = field(default_factory=dict)
    #: Retained units by ``pair_seq`` parity (§7 submission order): even = arm first.
    order_parity: dict[str, int] = field(default_factory=dict)
    exploratory_units: int = 0


def build_readout(
    *,
    declaration_id: int,
    strategy_version: str,
    declaration_sha256: str,
    pairs: Sequence[PairRecord],
    first_fill: date | None,
    run_census: Mapping[str, int],
    decision_census: Mapping[str, int],
    run_costs: Sequence[float],
    as_of: datetime,
) -> Readout:
    seed = readout_seed(declaration_sha256)
    # The session ``as_of`` belongs to (New York date, or the next session on a closed day), the
    # same mapping that decides which session a close row was recorded in.
    today = fill_session(as_of)
    legs = [leg for pair in pairs for leg in (pair.arm, pair.control) if leg is not None]
    state = cohort(pairs, first_fill, today)
    readout = Readout(
        declaration_id=declaration_id,
        strategy_version=strategy_version,
        as_of=as_of,
        seed=seed,
        cohort=state,
        pair_states=dict(Counter(pair.state for pair in pairs)),
        broken_reasons=dict(Counter(reason for pair in pairs for reason in pair.broken_reasons)),
        unvalued_reasons=dict(
            Counter(
                leg.unvalued_reason
                for pair in pairs
                if pair.state == "unit"
                for leg in (pair.arm, pair.control)
                if leg is not None and leg.unvalued_reason is not None
            )
        ),
        run_census=dict(run_census),
        decision_census=dict(decision_census),
        model_cost_usd_total=sum(run_costs),
        model_cost_usd_per_run=sum(run_costs) / len(run_costs) if run_costs else None,
        harm_looks=harm_looks(pairs, seed=seed, today=today),
        restated_rows=sum(leg.restated_rows for leg in legs),
        fee_rows=sum(leg.fee_rows for leg in legs),
        pool_sizes={pair.pair_seq: pair.pool_size for pair in pairs},
    )
    if state.status != "due" or state.last_session is None:
        return readout
    last = state.last_session
    units = [pair for pair in pairs if pair.valued and pair.session_date <= last]
    arm_legs = [pair.arm for pair in units if pair.arm is not None]
    control_legs = [pair.control for pair in units if pair.control is not None]
    arm, control = leg_summary(arm_legs), leg_summary(control_legs)
    weighted = (
        arm.capital_weighted_net_pct - control.capital_weighted_net_pct
        if arm.capital_weighted_net_pct is not None and control.capital_weighted_net_pct is not None
        else None
    )
    return replace(
        readout,
        primary=primary(units, seed=seed),
        mechanical_only=primary([unit for unit in units if unit.mechanical], seed=seed),
        arm=arm,
        arm_interval=block_bootstrap_expectancy(
            cluster_by_date(
                [unit.arm.net_pct for unit in units if unit.arm and unit.arm.net_pct is not None],
                [unit.session_date for unit in units],
            ),
            seed=seed,
        ),
        control=control,
        capital_weighted_d_pct=weighted,
        per_regime=by_group(units, "regime_label"),
        per_confidence=by_group(units, "confidence"),
        exit_labels={
            "arm": dict(Counter(leg.exit_label for leg in arm_legs)),
            "control": dict(Counter(leg.exit_label for leg in control_legs)),
        },
        order_parity={
            "arm_first": sum(1 for unit in units if unit.pair_seq % 2 == 0),
            "control_first": sum(1 for unit in units if unit.pair_seq % 2 == 1),
        },
        exploratory_units=sum(1 for pair in pairs if pair.valued and pair.session_date > last),
    )


# --------------------------------------------------------------------------------------------
# Loader
# --------------------------------------------------------------------------------------------

_PAIRS_SQL: Final = """
    SELECT p.pair_id, p.pair_seq, r.session_date, d.confidence, lb.regime_label, cardinality(p.pool)
    FROM ai_trial_pairs p
    JOIN ai_trial_decisions d ON d.decision_id = p.arm_decision_id
    JOIN ai_trial_runs r ON r.run_id = d.run_id
    LEFT JOIN ai_trial_pair_labels lb ON lb.pair_id = p.pair_id
    WHERE p.declaration_id = %s
    ORDER BY p.pair_seq
"""

_LEGS_SQL: Final = """
    SELECT tl.pair_id, tl.leg, t.strategy_trade_id, t.instrument_id, t.exit_deadline_session,
           i.currency,
           entry.filled_at, entry.units, entry.average_price,
           (SELECT max(ow.released_at) FROM strategy_position_ownership ow
            WHERE ow.strategy_trade_id = t.strategy_trade_id) AS released_at,
           (SELECT op.trigger_code
            FROM strategy_position_ownership ow
            JOIN strategy_position_operations op ON op.ownership_id = ow.ownership_id
            WHERE ow.strategy_trade_id = t.strategy_trade_id
              AND op.operation_type = 'close' AND op.status = 'applied'
            ORDER BY op.position_operation_id DESC LIMIT 1) AS close_trigger
    FROM ai_trial_trade_links tl
    JOIN ai_trial_pairs p ON p.pair_id = tl.pair_id
    JOIN strategy_trades t ON t.strategy_trade_id = tl.strategy_trade_id
    JOIN instruments i ON i.instrument_id = t.instrument_id
    LEFT JOIN LATERAL (
        SELECT min(e.execution_time) AS filled_at,
               sum(e.opening_units) AS units,
               sum(e.opening_units * e.average_price) / NULLIF(sum(e.opening_units), 0) AS average_price
        FROM strategy_trade_orders sto
        JOIN strategy_order_position_executions e ON e.order_id = sto.order_id
        WHERE sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
          AND e.opening_units > 0 AND e.average_price > 0
    ) entry ON TRUE
    WHERE p.declaration_id = %s
"""

#: A leg's close slices: rows under a position the trade owns, plus eToro's partial-close slices —
#: booked under a NEW position id that shares the entry order's ``orderId`` and that nobody owns
#: (``broker_closed_release``, attended 2026-09-23). One entry order can also yield several OWNED
#: executions (sql/282); those are positions of their own and are reached through ownership.
_CLOSE_ROWS_SQL: Final = """
    SELECT t.strategy_trade_id, ev.executed_at, ev.recorded_at, ev.realized_pnl_usd,
           ev.investment_usd, ev.fees_usd, ev.price,
           ev.raw_payload ->> 'stopLossRate', ev.raw_payload ->> 'takeProfitRate'
    FROM strategy_trades t
    JOIN trade_events ev ON ev.event_kind = 'close' AND (
        ev.position_id IN (SELECT ow.broker_position_id FROM strategy_position_ownership ow
                           WHERE ow.strategy_trade_id = t.strategy_trade_id)
        OR (
            NOT EXISTS (SELECT 1 FROM strategy_position_ownership other
                        WHERE other.broker_position_id = ev.position_id)
            AND EXISTS (
                SELECT 1 FROM strategy_trade_orders sto
                JOIN orders o ON o.order_id = sto.order_id
                WHERE sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
                  AND (ev.order_id::text = o.broker_order_ref OR ev.raw_payload ->> 'orderId' = o.broker_order_ref)
            )
        )
    )
    WHERE t.strategy_trade_id = ANY(%s)
    ORDER BY ev.executed_at
"""


def _decimal(raw: Any) -> Decimal | None:
    if raw is None:
        return None
    try:
        value = Decimal(str(raw))
    except ArithmeticError:
        return None
    return value if value.is_finite() else None


def _censor_mark(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    usd: bool,
    censor_session: date,
    average_price: Decimal | None,
    closes: Sequence[CloseRow],
) -> CensorMark:
    instant = datetime.combine(censor_session, TRIAL_EXIT_TIME_UTC, tzinfo=UTC)
    quote = conn.execute(
        """
        SELECT bid, ask FROM etoro_rate_observations
        WHERE instrument_id = %s AND observed_at <= %s
        ORDER BY observed_at DESC LIMIT 1
        """,
        (instrument_id, instant),
    ).fetchone()
    return CensorMark(
        average_price=average_price,
        bid=_decimal(quote[0]) if quote else None,
        ask=_decimal(quote[1]) if quote else None,
        usd=usd,
        partial_close=any(row.executed_at < instant for row in closes),
    )


def load_pairs(conn: psycopg.Connection[Any], declaration_id: int) -> tuple[list[PairRecord], date | None]:
    """Every pair of the declaration with its legs valued, plus the trial's first fill session."""
    pair_rows = conn.execute(_PAIRS_SQL, (declaration_id,)).fetchall()
    pair_ids = [int(row[0]) for row in pair_rows]
    events: dict[int, list[tuple[str | None, str, list[str] | None]]] = {pair_id: [] for pair_id in pair_ids}
    for pair_id, leg, event, reasons in conn.execute(
        "SELECT pair_id, leg, event, reasons FROM ai_trial_pair_events WHERE pair_id = ANY(%s) ORDER BY event_id",
        (pair_ids,),
    ).fetchall():
        events[int(pair_id)].append((leg, event, reasons))
    legs = {(int(row[0]), row[1]): row for row in conn.execute(_LEGS_SQL, (declaration_id,)).fetchall()}
    close_rows: dict[int, list[CloseRow]] = {}
    trade_ids = [int(row[2]) for row in legs.values()]
    for row in conn.execute(_CLOSE_ROWS_SQL, (trade_ids,)).fetchall():
        close_rows.setdefault(int(row[0]), []).append(
            CloseRow(
                row[1],
                row[2],
                _decimal(row[3]),
                _decimal(row[4]),
                _decimal(row[5]),
                _decimal(row[6]),
                _decimal(row[7]),
                _decimal(row[8]),
            )
        )
    fills = [row[6] for row in legs.values() if row[6] is not None]
    first_fill = min((fill_session(filled) for filled in fills), default=None)

    records: list[PairRecord] = []
    for pair_id, pair_seq, session_date, confidence, regime_label, pool_size in pair_rows:
        history = events[int(pair_id)]
        state = pair_unit_state([(leg, event) for leg, event, _ in history])
        broken = tuple(reason for _, event, reasons in history if event == "broken" for reason in reasons or ())
        values: dict[str, LegValue | None] = {}
        resolved: list[date] = []
        live = 0
        for leg in LEGS:
            leg_events = [event for event_leg, event, _ in history if event_leg == leg]
            live += "filled" in leg_events and not {"closed", "censored"} & set(leg_events)
            row = legs.get((int(pair_id), leg))
            # Any leg that exited is valued, a broken pair's included (O11: it is still reported);
            # only a ``unit`` enters the statistics.
            if row is None or row[6] is None or not {"closed", "censored"} & set(leg_events):
                values[leg] = None
                continue
            (_, _, trade_id, instrument_id, deadline, currency, _, units, avg_price, released_at, trigger) = row
            if "censored" in leg_events:
                censor_session = exit_deadline_session(deadline, TRIAL_CENSOR_SESSIONS)
                mark = _censor_mark(
                    conn,
                    instrument_id=int(instrument_id),
                    usd=currency == "USD",
                    censor_session=censor_session,
                    average_price=_decimal(avg_price),
                    closes=close_rows.get(int(trade_id), []),
                )
                values[leg] = value_censored_leg(mark, _decimal(units))
                resolved.append(censor_session)
            else:
                values[leg] = value_closed_leg(close_rows.get(int(trade_id), []), trigger)
                if released_at is not None:
                    resolved.append(fill_session(released_at))
        records.append(
            PairRecord(
                pair_seq=int(pair_seq),
                session_date=session_date,
                state=state,
                broken_reasons=broken,
                arm=values.get("arm"),
                control=values.get("control"),
                regime_label=regime_label,
                confidence=None if confidence is None else int(confidence),
                resolved_session=max(resolved, default=None),
                live_legs=live,
                pool_size=None if pool_size is None else int(pool_size),
            )
        )
    return records, first_fill


class ReadoutUnavailable(RuntimeError):
    pass


def compute_readout(conn: psycopg.Connection[Any], *, strategy_version: str, as_of: datetime | None = None) -> Readout:
    """The readout for the arm's declaration of ``strategy_version`` (O14: each version is its
    own declaration, so the version is never inferred). Read-only."""
    # Imported here: ai_trial_run → ai_trial_policy → this module (its §9 constants).
    from app.services.ai_trial_run import TRIAL_ARM_STRATEGY_ID

    declaration = conn.execute(
        "SELECT declaration_id, strategy_version, doc_sha256 FROM ai_trial_declarations "
        "WHERE strategy_id = %s AND strategy_version = %s",
        (TRIAL_ARM_STRATEGY_ID, strategy_version),
    ).fetchone()
    if declaration is None:
        raise ReadoutUnavailable(f"no frozen {strategy_version} declaration: the trial has not started (slice 3d)")
    declaration_id = int(declaration[0])
    pairs, first_fill = load_pairs(conn, declaration_id)
    run_census = {
        f"{status}:{reason}" if reason else status: int(count)
        for status, reason, count in conn.execute(
            "SELECT status, refusal_reason, count(*) FROM ai_trial_runs WHERE declaration_id = %s GROUP BY 1, 2",
            (declaration_id,),
        ).fetchall()
    }
    decision_census = {
        f"{verdict}:{reason}" if reason else verdict: int(count)
        for verdict, reason, count in conn.execute(
            """
            SELECT d.verdict, d.reason_code, count(*) FROM ai_trial_decisions d
            JOIN ai_trial_runs r ON r.run_id = d.run_id
            WHERE r.declaration_id = %s GROUP BY 1, 2
            """,
            (declaration_id,),
        ).fetchall()
    }
    costs = [
        float(row[0])
        for row in conn.execute(
            "SELECT cost_usd FROM ai_trial_runs WHERE declaration_id = %s AND cost_usd IS NOT NULL",
            (declaration_id,),
        ).fetchall()
    ]
    return build_readout(
        declaration_id=declaration_id,
        strategy_version=str(declaration[1]),
        declaration_sha256=str(declaration[2]),
        pairs=pairs,
        first_fill=first_fill,
        run_census=run_census,
        decision_census=decision_census,
        run_costs=costs,
        as_of=as_of or datetime.now(UTC),
    )
