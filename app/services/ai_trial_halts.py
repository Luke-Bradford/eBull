"""#3471 §9 — the two terminal halts the engine writes: the loss halt and the harm stop.

``enforce_trial_halts`` runs after every 5-minute paper cycle (``scheduler.strategy_paper_cycle``),
over every arm declaration whose trial is ``active``. Each halt is one ``active → halted_*`` event
through ``ai_trial_protection.halt_active_trial``, the transition O10 already uses. Both states are
terminal (``sql/432``). The intent loader, the executor and the decision run already refuse
``trial_not_active``, so a halt stops new entries on both legs, queued decisions included; open
positions run to their exits.

- **Loss halt (checked first: safety, not statistics).** Either leg's realised + unrealised USD
  loss ≥ ``TRIAL_LOSS_HALT_PCT`` of that leg's capital. The capital is the §8 fixed-mode cap,
  ``TRIAL_MAX_CONCURRENT_PER_LEG`` × the full ticket = $1,000, so the limit is the spec's $200.
  Every trade of the leg in the declaration counts, cohort and exploration alike.
  - Realised = Σ ``realized_pnl_usd`` over the trade's broker close slices (``load_close_rows``,
    the readout's reach, the unowned partial-close sibling included). Every slice counts: the
    5-session flow window is a readout rule, and a late restatement is still money lost.
  - Unrealised = the units still open (opened − Σ closed slice units) × (``quotes.bid`` − the
    entry average price). ``quotes`` is the position manager's own mark. Its age is not gated:
    a bid is a price the position could have exited at when it was quoted, so a loss it shows
    was real at that instant, and the rule is "reaches".
  - A filled trade whose P&L cannot be measured (no bid, a non-USD instrument, a slice without
    P&L or units, or a closed trade whose slices do not yet cover its opened units — history
    ingest lags the close) is counted
    ``unmeasured`` and contributes nothing. It never halts: ``halted_loss`` is terminal, and an
    absent number is not a loss. The count is on the paper cycle's note.
- **A check that cannot run** moves the trial to the resumable ``halted_operator`` (fail closed).
- **Harm stop.** ``compute_readout(...).harm_looks`` (§9 "Harm stop"); the first look that
  ``halts`` and is ``flows_final`` writes ``halted_harm``, naming the look in the reason. A leg is
  valued from its first close slice, and a later slice inside the 5-session flow window can move
  its *d*; a terminal halt must not rest on a provisional p (Codex ckpt-2), so it waits for the
  window, as the cohort readout does.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Final

import psycopg

from app.services.ai_trial_executor import TRIAL_MAX_CONCURRENT_PER_LEG
from app.services.ai_trial_intent import TRIAL_TICKET_USD
from app.services.ai_trial_pair_lifecycle import LEGS
from app.services.ai_trial_protection import EngineHalt, halt_active_trial
from app.services.ai_trial_readout import CloseRow, compute_readout, load_close_rows

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

#: §8 trial caps / §9 "Loss halts": a leg's realised + unrealised loss as a % of its capital.
TRIAL_LOSS_HALT_PCT: Final = Decimal("20")
#: §8: fixed-mode capital, never replenished.
TRIAL_LEG_CAPITAL_USD: Final = TRIAL_MAX_CONCURRENT_PER_LEG * TRIAL_TICKET_USD["full"]
TRIAL_LOSS_LIMIT_USD: Final = TRIAL_LEG_CAPITAL_USD * TRIAL_LOSS_HALT_PCT / 100


@dataclass(frozen=True)
class LegTrade:
    leg: str
    status: str
    usd: bool
    #: ``None`` until the entry order has a stored execution.
    opened_units: Decimal | None
    average_price: Decimal | None
    bid: Decimal | None
    closes: tuple[CloseRow, ...]


def trade_pnl_usd(trade: LegTrade) -> Decimal | None:
    """Realised + unrealised USD P&L; ``None`` = unmeasured. An unfilled trade is 0."""
    if trade.opened_units is None or trade.opened_units <= 0 or trade.average_price is None:
        return Decimal(0)
    if not trade.usd:
        return None
    realised = Decimal(0)
    closed_units = Decimal(0)
    for row in trade.closes:
        if row.realized_pnl_usd is None or row.units is None:
            return None
        realised += row.realized_pnl_usd
        closed_units += row.units
    if trade.status == "closed":
        # Unmeasured until its slices cover the opened units: a slice not yet ingested leaves
        # the realised figure partial (Codex ckpt-2).
        return realised if closed_units >= trade.opened_units else None
    remaining = trade.opened_units - closed_units
    if remaining <= 0:
        return realised
    if trade.bid is None or not trade.bid > 0:
        return None
    return realised + remaining * (trade.bid - trade.average_price)


@dataclass(frozen=True)
class LegLoss:
    leg: str
    pnl_usd: Decimal
    unmeasured: int

    @property
    def loss_usd(self) -> Decimal:
        return -self.pnl_usd


def leg_losses(trades: Sequence[LegTrade]) -> list[LegLoss]:
    losses: list[LegLoss] = []
    for leg in LEGS:
        pnls = [trade_pnl_usd(trade) for trade in trades if trade.leg == leg]
        measured = [pnl for pnl in pnls if pnl is not None]
        losses.append(LegLoss(leg, sum(measured, Decimal(0)), len(pnls) - len(measured)))
    return losses


def loss_breach(losses: Sequence[LegLoss], *, limit_usd: Decimal = TRIAL_LOSS_LIMIT_USD) -> LegLoss | None:
    """§9: the first leg whose loss reached the limit (≥, the spec's own comparison)."""
    return next((leg for leg in losses if leg.loss_usd >= limit_usd), None)


_TRADES_SQL: Final = """
    SELECT tl.leg, t.strategy_trade_id, t.status, i.currency = 'USD', entry.units, entry.average_price, q.bid
    FROM ai_trial_trade_links tl
    JOIN ai_trial_pairs p ON p.pair_id = tl.pair_id
    JOIN strategy_trades t ON t.strategy_trade_id = tl.strategy_trade_id
    JOIN instruments i ON i.instrument_id = t.instrument_id
    LEFT JOIN quotes q ON q.instrument_id = t.instrument_id
    LEFT JOIN LATERAL (
        SELECT sum(e.opening_units) AS units,
               sum(e.opening_units * e.average_price) / NULLIF(sum(e.opening_units), 0) AS average_price
        FROM strategy_trade_orders sto
        JOIN strategy_order_position_executions e ON e.order_id = sto.order_id
        WHERE sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
          AND e.opening_units > 0 AND e.average_price > 0
    ) entry ON TRUE
    WHERE p.declaration_id = %s
"""


def load_leg_trades(conn: Conn, declaration_id: int) -> list[LegTrade]:
    rows = conn.execute(_TRADES_SQL, (declaration_id,)).fetchall()
    closes = load_close_rows(conn, [int(row[1]) for row in rows])
    return [
        LegTrade(
            leg=str(leg),
            status=str(status),
            usd=bool(usd),
            opened_units=None if units is None else Decimal(units),
            average_price=None if average_price is None else Decimal(average_price),
            bid=None if bid is None else Decimal(bid),
            closes=tuple(closes.get(int(trade_id), ())),
        )
        for leg, trade_id, status, usd, units, average_price, bid in rows
    ]


@dataclass(frozen=True)
class HaltCheck:
    declaration_id: int
    #: The state this pass wrote, if any.
    halted: EngineHalt | None
    unmeasured: int
    #: The check raised; its savepoint was rolled back, the trial was moved to the resumable
    #: ``halted_operator`` (``halted`` says whether that write landed) and the others still ran.
    failed: bool = False


_ACTIVE_DECLARATIONS_SQL: Final = """
    SELECT d.declaration_id, d.strategy_version
    FROM ai_trial_declarations d
    WHERE d.strategy_id = %s
      AND (SELECT se.to_state FROM ai_trial_state_events se
           WHERE se.declaration_id = d.declaration_id ORDER BY se.event_id DESC LIMIT 1) = 'active'
    ORDER BY d.declaration_id
"""


def _check_declaration(conn: Conn, declaration_id: int, strategy_version: str, now: datetime | None) -> HaltCheck:
    losses = leg_losses(load_leg_trades(conn, declaration_id))
    unmeasured = sum(leg.unmeasured for leg in losses)
    halted: EngineHalt | None = None
    reason = ""
    breach = loss_breach(losses)
    if breach is not None:
        reason = (
            f"loss_halt:leg={breach.leg}:loss_usd={breach.loss_usd:.2f}"
            f":limit_usd={TRIAL_LOSS_LIMIT_USD:.2f}:unmeasured={breach.unmeasured}"
        )
        if halt_active_trial(conn, declaration_id=declaration_id, to_state="halted_loss", reason=reason):
            halted = "halted_loss"
    else:
        readout = compute_readout(conn, strategy_version=strategy_version, as_of=now)
        look = next((look for look in readout.harm_looks if look.halts and look.flows_final), None)
        if look is not None:
            reason = (
                f"harm_look:k={look.k}:units={look.units}:clusters={look.clusters}"
                f":p={look.p_less:.6g}<{look.threshold:.6g}"
            )
            if halt_active_trial(conn, declaration_id=declaration_id, to_state="halted_harm", reason=reason):
                halted = "halted_harm"
    if halted is not None:
        logger.error("ai trial %s %s (%s)", declaration_id, halted, reason)
    return HaltCheck(declaration_id, halted, unmeasured)


def _fail_closed(conn: Conn, declaration_id: int, exc: Exception) -> HaltCheck:
    try:
        written = halt_active_trial(
            conn,
            declaration_id=declaration_id,
            to_state="halted_operator",
            reason=f"halt_check_failed:{type(exc).__name__}",
        )
    except Exception:
        logger.exception("ai trial %s: the fail-closed halt could not be written either", declaration_id)
        written = False
    return HaltCheck(declaration_id, "halted_operator" if written else None, 0, failed=True)


def enforce_trial_halts(conn: Conn, *, now: datetime | None = None) -> list[HaltCheck]:
    """Check both halts on every active trial and write the first that fires. The caller commits.

    Each declaration runs in its own savepoint: one that raises is rolled back alone and reported
    ``failed``, so it can neither skip a later declaration nor undo a halt an earlier one wrote.

    ⚠ A check that cannot run fails CLOSED: the trial moves to ``halted_operator``, the same
    resumable refusal surface O10 uses for an unprotected leg. A loss or harm halt that cannot be
    evaluated must not let entries continue behind a note on the job (review round 2)."""
    # Imported here: ai_trial_run → ai_trial_policy → this module (its §9 constant).
    from app.services.ai_trial_run import TRIAL_ARM_STRATEGY_ID

    checks: list[HaltCheck] = []
    for declaration_id, strategy_version in conn.execute(_ACTIVE_DECLARATIONS_SQL, (TRIAL_ARM_STRATEGY_ID,)).fetchall():
        try:
            with conn.transaction():
                checks.append(_check_declaration(conn, int(declaration_id), str(strategy_version), now))
        except Exception as exc:
            logger.exception("ai trial %s: halt check failed; halting it for the supervisor", declaration_id)
            checks.append(_fail_closed(conn, int(declaration_id), exc))
    return checks


__all__ = [
    "TRIAL_LEG_CAPITAL_USD",
    "TRIAL_LOSS_HALT_PCT",
    "TRIAL_LOSS_LIMIT_USD",
    "HaltCheck",
    "LegLoss",
    "LegTrade",
    "enforce_trial_halts",
    "leg_losses",
    "load_leg_trades",
    "loss_breach",
    "trade_pnl_usd",
]
