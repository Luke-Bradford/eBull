"""Transaction-cost analysis of strategy entries (#3611): what an order cost against the price it was decided at.

* **Decision price.** The preflight's ``quote_ask``: the price the entry was sized and protected from (the stored
  quote's ask, or the latest OneMinute candle close when eToro delays the name's quote, #3833).
* **Arrival quote.** eToro's bid and ask fetched immediately before the broker write (``record_arrival_quote``,
  ``strategy_order_arrival_quotes``). Its mid is the reference SEC Rule 605 uses for effective spread: the midpoint
  at the time of order receipt.
* **Fill.** The single open event (``trade_events``) of the single broker position the entry's trade owns.

For a buy, with decision price D, arrival mid M and fill F, every figure is a fraction, positive when it cost money:

* ``shortfall = F / D - 1`` (implementation shortfall against the decision price);
* ``delay = M / D - 1`` (the price moved between deciding and submitting);
* ``execution = F / M - 1`` (paid above the arrival mid);
* so ``(1 + shortfall) = (1 + delay) * (1 + execution)`` exactly;
* ``effective_spread = 2 * |F - M| / M`` (Rule 605), comparable with the what-if open cost, whose ``marketSpread``
  matches the full quoted spread (``.claude/skills/data-sources/etoro-api.md``, band census);
* ``quoted_spread = (ask - bid) / M`` at arrival.

An arrival quote older than ``ARRIVAL_MAX_AGE_SECONDS`` at receipt is not used: its mid carries the same drift the
arrival quote exists to remove. There is no source rule for that bound; it is fixed by construction. Entry fills land
seconds after submission, while decision quotes minutes old put fill-vs-decision gaps well beyond the what-if cost
(reproduce: ``scripts/tca_readout.py --since 2026-10-15``, columns ``fill_after_s``, ``decision_age_s``,
``shortfall_bps``, ``what_if_bps``), so a mid older than a minute already measures drift as much as cost.
Descriptive only; nothing here enters a decision.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg
from psycopg.rows import dict_row

from app.providers.market_data import Quote
from app.services.strategy_paper_executor import _age_ok

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

ARRIVAL_MAX_AGE_SECONDS: Final = 60

#: Fetches one instrument's current quote, or None when the provider omits it.
ArrivalQuote = Callable[[int], Quote | None]

#: ``strategy_order_arrival_quotes.outcome`` (sql/500).
ArrivalOutcome = Literal["quoted", "not_quoted", "failed"]

Unmeasured = Literal[
    "no_fill",
    "multiple_fills",
    "fill_price_unusable",
    "arrival_missing",
    "arrival_failed",
    "arrival_not_quoted",
    "arrival_stale",
    "arrival_future_dated",
    "arrival_crossed",
]


def record_arrival_quote(
    conn: Conn,
    *,
    order_id: int,
    instrument_id: int,
    arrival_quote: ArrivalQuote,
    clock: Callable[[], datetime],
) -> None:
    """Fetch and store the instrument's quote for an order about to be submitted. Best-effort and self-contained:
    nothing it raises reaches the caller, and a failed write rolls back only its own savepoint (or transaction, on an
    idle connection), so the submission that follows runs either way."""
    requested_at = clock()
    quote: Quote | None = None
    outcome: ArrivalOutcome = "failed"
    try:
        quote = arrival_quote(instrument_id)
        outcome = "quoted" if quote is not None else "not_quoted"
    except Exception:
        logger.warning("arrival quote for order %s (instrument %s) unavailable", order_id, instrument_id, exc_info=True)
    received_at = clock()
    try:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO strategy_order_arrival_quotes
                    (order_id, requested_at, received_at, outcome, quoted_at, bid, ask)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    requested_at,
                    received_at,
                    outcome,
                    None if quote is None else quote.timestamp,
                    None if quote is None else quote.bid,
                    None if quote is None else quote.ask,
                ),
            )
    except Exception:
        logger.warning("arrival quote for order %s not recorded", order_id, exc_info=True)


@dataclass(frozen=True)
class ArrivalFacts:
    outcome: ArrivalOutcome
    received_at: datetime
    quoted_at: datetime | None
    bid: Decimal | None
    ask: Decimal | None


@dataclass(frozen=True)
class EntryCosts:
    shortfall: Decimal
    delay: Decimal | None
    execution: Decimal | None
    effective_spread: Decimal | None
    quoted_spread: Decimal | None
    arrival_age_seconds: Decimal | None
    #: Why the arrival-based figures are None; None when they are measured.
    arrival_unmeasured: Unmeasured | None


def _arrival_reason(arrival: ArrivalFacts | None) -> Unmeasured | None:
    if arrival is None:
        return "arrival_missing"
    if arrival.outcome == "failed":
        return "arrival_failed"
    if arrival.outcome == "not_quoted":
        return "arrival_not_quoted"
    assert arrival.quoted_at is not None and arrival.bid is not None and arrival.ask is not None
    # The executor's freshness rule: a future stamp beyond its skew allowance is as unusable as an old one.
    if not _age_ok(arrival.quoted_at, now=arrival.received_at, max_seconds=ARRIVAL_MAX_AGE_SECONDS):
        return "arrival_future_dated" if arrival.quoted_at > arrival.received_at else "arrival_stale"
    if arrival.ask < arrival.bid:
        return "arrival_crossed"
    return None


def entry_costs(*, decision: Decimal, fill: Decimal, arrival: ArrivalFacts | None) -> EntryCosts:
    """The module docstring's figures for one filled buy. ``decision`` and ``fill`` must be positive."""
    if decision <= 0 or fill <= 0:
        raise ValueError("decision and fill prices must be positive")
    shortfall = fill / decision - 1
    age = (
        None
        if arrival is None or arrival.quoted_at is None
        else Decimal(str((arrival.received_at - arrival.quoted_at).total_seconds()))
    )
    reason = _arrival_reason(arrival)
    if reason is not None:
        return EntryCosts(shortfall, None, None, None, None, age, reason)
    assert arrival is not None and arrival.bid is not None and arrival.ask is not None
    mid = (arrival.bid + arrival.ask) / 2
    return EntryCosts(
        shortfall=shortfall,
        delay=mid / decision - 1,
        execution=fill / mid - 1,
        effective_spread=2 * abs(fill - mid) / mid,
        quoted_spread=(arrival.ask - arrival.bid) / mid,
        arrival_age_seconds=age,
        arrival_unmeasured=None,
    )


@dataclass(frozen=True)
class EntryTca:
    order_id: int
    instrument_id: int
    symbol: str | None
    submitted_at: datetime
    #: The decision price's own timestamp (the preflight's ``quote_at``).
    decision_at: datetime | None
    price_reference: str | None
    decision: Decimal
    #: The preflight's priced cost per ticket, with the policy's stress multiplier removed.
    what_if_cost: Decimal | None
    fill: Decimal | None
    filled_at: datetime | None
    costs: EntryCosts | None
    #: Why there is no fill to measure; None when ``costs`` is set.
    unmeasured: Unmeasured | None


_ENTRIES_SQL: Final = """
    SELECT o.order_id, o.instrument_id, i.symbol, o.created_at, pf.price_reference, pf.quote_ask,
           pf.stressed_cost_amount, pf.allocated_amount, pol.cost_stress_multiplier,
           pf.quote_at, a.outcome, a.received_at, a.quoted_at, a.bid, a.ask,
           op.n_opens, op.fill, op.filled_at
      FROM orders o
      JOIN strategy_trade_orders sto ON sto.order_id = o.order_id AND sto.purpose = 'entry'
      JOIN strategy_trades t ON t.strategy_trade_id = sto.strategy_trade_id
      JOIN strategy_funding_decisions fd ON fd.funding_decision_id = t.funding_decision_id
      JOIN strategy_entry_preflights pf ON pf.signal_id = fd.signal_id
      -- The revision the entry was evaluated under; the policies table holds only the current one.
      LEFT JOIN strategy_execution_policy_events pol
             ON pol.deployment_id = pf.deployment_id AND pol.revision = pf.policy_revision
      LEFT JOIN instruments i ON i.instrument_id = o.instrument_id
      LEFT JOIN strategy_order_arrival_quotes a ON a.order_id = o.order_id
      CROSS JOIN LATERAL (
            -- min() of a single row is that row; with more than one, the fill is not measured.
            SELECT count(*) AS n_opens, min(te.price) AS fill, min(te.executed_at) AS filled_at
              FROM trade_events te
             WHERE te.environment = o.broker_environment AND te.event_kind = 'open'
               AND te.position_id IN (SELECT ow.broker_position_id FROM strategy_position_ownership ow
                                       WHERE ow.strategy_trade_id = t.strategy_trade_id)
      ) op
     WHERE o.execution_origin = 'strategy' AND o.action = 'BUY' AND o.created_at >= %(since)s
       AND pf.quote_ask IS NOT NULL
     ORDER BY o.created_at, o.order_id
"""


def _what_if_cost(row: dict[str, Any]) -> Decimal | None:
    stressed, amount, multiplier = row["stressed_cost_amount"], row["allocated_amount"], row["cost_stress_multiplier"]
    if stressed is None or not amount or not multiplier:
        return None
    return Decimal(stressed) / Decimal(multiplier) / Decimal(amount)


def entry_tca(row: dict[str, Any]) -> EntryTca:
    """One ``_ENTRIES_SQL`` row, measured."""
    arrival = (
        None
        if row["outcome"] is None
        else ArrivalFacts(row["outcome"], row["received_at"], row["quoted_at"], row["bid"], row["ask"])
    )
    fill: Decimal | None = None
    filled_at: datetime | None = None
    costs: EntryCosts | None = None
    unmeasured: Unmeasured | None
    if row["n_opens"] == 0:
        unmeasured = "no_fill"
    elif row["n_opens"] > 1:
        unmeasured = "multiple_fills"
    else:
        fill, filled_at = row["fill"], row["filled_at"]
        if fill is None or not fill.is_finite() or fill <= 0:
            unmeasured = "fill_price_unusable"
        else:
            costs = entry_costs(decision=Decimal(row["quote_ask"]), fill=fill, arrival=arrival)
            unmeasured = None
    return EntryTca(
        order_id=int(row["order_id"]),
        instrument_id=int(row["instrument_id"]),
        symbol=row["symbol"],
        submitted_at=row["created_at"],
        decision_at=row["quote_at"],
        price_reference=row["price_reference"],
        decision=Decimal(row["quote_ask"]),
        what_if_cost=_what_if_cost(row),
        fill=fill,
        filled_at=filled_at,
        costs=costs,
        unmeasured=unmeasured,
    )


def load_entry_tca(conn: Conn, *, since: date) -> list[EntryTca]:
    """Every strategy entry order created on or after ``since`` (UTC midnight), measured."""
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(_ENTRIES_SQL, {"since": datetime.combine(since, time(), tzinfo=UTC)}).fetchall()
    return [entry_tca(r) for r in rows]


__all__ = [
    "ARRIVAL_MAX_AGE_SECONDS",
    "ArrivalFacts",
    "ArrivalOutcome",
    "ArrivalQuote",
    "EntryCosts",
    "EntryTca",
    "entry_costs",
    "entry_tca",
    "load_entry_tca",
    "record_arrival_quote",
]
