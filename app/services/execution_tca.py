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

**Exits (slice 2).** Every close of a strategy-owned long position since a date, whoever closed it:

* ``engine`` — the strategy submitted the close (an exit order linked to the trade that the broker did not reject).
  Before submission ``exit_evidence`` records the arrival quote, then the close-arm what-if cost bound to that
  position (``strategy_order_close_what_ifs``; the open arm does not bound the close, #2712). The what-if ticket is
  the position's units at the arrival bid, a sell's live executable price: the broker's portfolio carries no current
  mark (``_parse_direct_position`` sets ``current_price`` to the open rate), and the close arm's ``units`` field is
  undocumented and unmeasured. With no usable arrival bid the what-if is not requested (``unpriced``). For a sell,
  with arrival mid M and fill F: ``execution = 1 - F / M`` (sold below the mid), ``effective_spread = 2 * |F - M| /
  M``, and the what-if cost rate is ``cost / ticket``. There is no separate decision price: the close is decided and
  sent in one pass.
* ``stop_loss`` / ``take_profit`` — no engine exit, and the close rate is at or beyond the stop (``F <= stop``) or the
  target (``F >= target``) the closed-position record carries. eToro's trading history has no close-reason field
  (``list-trading-history``, checked 2026-10-17), so the attribution is by price, fixed by construction.
  ``level_cost = 1 - F / level`` (positive when the fill was worse than the level), and ``gapped`` says the session
  opened beyond the level (``price_daily.open`` on the close's New York date), so the fill could not have been the
  level. ``price_daily`` is retroactively split-adjusted while the fill and levels stay as traded, so the candle is
  used only when the fill lies inside its own session's low-high range; a candle on another basis (or an
  extended-hours fill outside the regular session) leaves ``gapped`` unset, with the reason.
* ``unexplained`` — neither: a manual close, or a close inside both levels with no engine order.

Descriptive only; nothing here enters a decision.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerPosition, BrokerProvider, BrokerWhatIfCostResponse, BrokerWhatIfOrder
from app.providers.market_data import Quote
from app.services.cost_model import BANDS, PriceBand, band_for
from app.services.strategy_paper_executor import _age_ok, _component_amount
from app.services.strategy_position_manager import ExitEvidence

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

ARRIVAL_MAX_AGE_SECONDS: Final = 60

#: Fetches one instrument's current quote, or None when the provider omits it.
ArrivalQuote = Callable[[int], Quote | None]

#: ``strategy_order_arrival_quotes.outcome`` (sql/500).
ArrivalOutcome = Literal["quoted", "unusable", "not_quoted", "failed"]

Unmeasured = Literal[
    "no_fill",
    "multiple_fills",
    "fill_price_unusable",
    "arrival_missing",
    "arrival_failed",
    "arrival_not_quoted",
    "arrival_unusable",
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
) -> ArrivalFacts | None:
    """Fetch and store the instrument's quote for an order about to be submitted, and return what was fetched (None
    when nothing was). Best-effort and self-contained: nothing it raises reaches the caller, and a failed write rolls
    back only its own savepoint (or transaction, on an idle connection), so the submission that follows runs either
    way."""
    facts: ArrivalFacts | None = None
    try:
        requested_at = clock()
        quote: Quote | None = None
        outcome: ArrivalOutcome = "failed"
        try:
            fetched = arrival_quote(instrument_id)
            # Classified before the insert, so a bad quote is recorded as such rather than dropped by the CHECK.
            # ``quote`` is set only once classification succeeds: a comparison that raises (a NaN Decimal) leaves a
            # 'failed' row with no quote fields, which is what the CHECK requires.
            if fetched is None:
                outcome = "not_quoted"
            else:
                outcome = "quoted" if fetched.bid > 0 and fetched.ask > 0 else "unusable"
                quote = fetched
        except Exception:
            outcome, quote = "failed", None
            logger.warning(
                "arrival quote for order %s (instrument %s) unavailable", order_id, instrument_id, exc_info=True
            )
        # A clock that steps back between the two reads must not violate CHECK (received_at >= requested_at).
        received_at = max(requested_at, clock())
        facts = ArrivalFacts(
            outcome,
            received_at,
            None if quote is None else quote.timestamp,
            None if quote is None else quote.bid,
            None if quote is None else quote.ask,
        )
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
    return facts


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


def _usable_quote(arrival: ArrivalFacts | None) -> tuple[Decimal, Decimal] | Unmeasured:
    """The arrival bid and ask, or why they cannot be used."""
    if arrival is None:
        return "arrival_missing"
    if arrival.outcome == "failed":
        return "arrival_failed"
    if arrival.outcome == "not_quoted":
        return "arrival_not_quoted"
    if arrival.outcome == "unusable":
        return "arrival_unusable"
    quoted_at, bid, ask = arrival.quoted_at, arrival.bid, arrival.ask
    if quoted_at is None or bid is None or ask is None:
        raise ValueError("a 'quoted' arrival row without its quote")  # sql/500's CHECK forbids it
    # The executor's freshness rule: a future stamp beyond its skew allowance is as unusable as an old one.
    if not _age_ok(quoted_at, now=arrival.received_at, max_seconds=ARRIVAL_MAX_AGE_SECONDS):
        return "arrival_future_dated" if quoted_at > arrival.received_at else "arrival_stale"
    if not (bid.is_finite() and ask.is_finite()):
        return "arrival_unusable"  # an Infinity passes the CHECK's positivity test and would poison the mid
    if ask < bid:
        return "arrival_crossed"
    return bid, ask


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
    usable = _usable_quote(arrival)
    if isinstance(usable, str):
        return EntryCosts(shortfall, None, None, None, None, age, usable)
    bid, ask = usable
    mid = (bid + ask) / 2
    return EntryCosts(
        shortfall=shortfall,
        delay=mid / decision - 1,
        execution=fill / mid - 1,
        effective_spread=2 * abs(fill - mid) / mid,
        quoted_spread=(ask - bid) / mid,
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
             -- A NULL environment predates #3614 (see _EXITS_SQL).
             WHERE te.environment = COALESCE(o.broker_environment, 'demo') AND te.event_kind = 'open'
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
            # quote_ask > 0 is a CHECK (strategy_entry_preflights_quote_ask_positive, sql/438) and the SQL keeps only
            # non-NULL rows, so entry_costs' positivity guard cannot fire on a stored decision price.
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


#: ``strategy_order_close_what_ifs.outcome`` (sql/501).
CloseWhatIfOutcome = Literal["quoted", "undecodable", "failed", "unpriced"]

_TICKET_QUANTUM: Final = Decimal("0.01")

#: ``BrokerWhatIfOrder.order_currency``'s default, the only one eToro's what-if accepts: the ticket is in it, so a
#: cost is comparable with the ticket only in it.
_TICKET_CURRENCY: Final = "USD"


def close_cost(response: BrokerWhatIfCostResponse, *, instrument_id: int) -> Decimal | None:
    """The response's total cost in the ticket's currency, or None when it does not decode: every component must
    carry one field (``amount`` or ``value``, ``_component_amount``), the same field as the others, the ticket's
    currency and a finite non-negative figure. Summed, not filtered: an unfamiliar component is still a cost."""
    if response.instrument_id != instrument_id or not response.costs:
        return None
    total = Decimal("0")
    bases: set[str] = set()
    for component in response.costs:
        resolved = _component_amount(component)
        if resolved is None or component.currency.strip().upper() != _TICKET_CURRENCY:
            return None
        figure, basis = resolved
        if not figure.is_finite() or figure < 0:
            return None
        bases.add(basis)
        total += figure
    return total if len(bases) == 1 else None


def record_close_what_if(
    conn: Conn,
    *,
    order_id: int,
    position: BrokerPosition,
    mark: Decimal | None,
    broker: BrokerProvider,
    clock: Callable[[], datetime],
) -> None:
    """Quote and store the close-arm cost of the exact position an exit order is about to close, for a ticket of its
    units at ``mark`` (the arrival bid). Without a positive mark nothing is requested and the row says ``unpriced``.
    Best-effort and self-contained, as ``record_arrival_quote``: nothing it raises reaches the caller."""
    try:
        requested_at = clock()
        ticket: Decimal | None = None
        if mark is not None and mark.is_finite() and mark > 0 and position.units > 0:
            ticket = (position.units * mark).quantize(_TICKET_QUANTUM)
        outcome: CloseWhatIfOutcome = "unpriced"
        cost: Decimal | None = None
        response: BrokerWhatIfCostResponse | None = None
        if ticket is not None and ticket > 0 and position.position_id is not None:
            try:
                response = broker.get_what_if_costs(
                    BrokerWhatIfOrder(
                        instrument_id=position.instrument_id,
                        transaction="sell",
                        settlement_type="real",
                        amount=ticket,
                        leverage=1,
                        action="close",
                        position_ids=(position.position_id,),
                    )
                )
                cost = close_cost(response, instrument_id=position.instrument_id)
                outcome = "undecodable" if cost is None else "quoted"
            except Exception:
                outcome, cost, response = "failed", None, None
                logger.warning("close what-if for order %s unavailable", order_id, exc_info=True)
        else:
            ticket = None
        received_at = max(requested_at, clock())
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO strategy_order_close_what_ifs
                    (order_id, requested_at, received_at, ticket_amount, outcome, cost, currency, quoted_at,
                     raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    requested_at,
                    received_at,
                    ticket,
                    outcome,
                    cost,
                    None if cost is None else _TICKET_CURRENCY,
                    # Never None on a response: ``BrokerWhatIfCostResponse.last_updated`` is a ``datetime``, and the
                    # eToro parser raises on a missing or naive ``lastUpdated``, which lands in the 'failed' arm above.
                    None if response is None else response.last_updated,
                    None if response is None else Jsonb(response.raw_payload),
                ),
            )
    except Exception:
        logger.warning("close what-if for order %s not recorded", order_id, exc_info=True)


def exit_evidence(
    conn: Conn,
    *,
    broker: BrokerProvider,
    arrival_quote: ArrivalQuote | None,
    clock: Callable[[], datetime],
) -> ExitEvidence:
    """The ``manage_owned_position`` hook that records an exit's arrival quote, then its close-arm cost priced at
    that quote's bid (the module docstring's ticket rule)."""

    def record(order_id: int, position: BrokerPosition) -> None:
        arrival = (
            None
            if arrival_quote is None
            else record_arrival_quote(
                conn, order_id=order_id, instrument_id=position.instrument_id, arrival_quote=arrival_quote, clock=clock
            )
        )
        usable = _usable_quote(arrival)
        mark = None if isinstance(usable, str) else usable[0]
        record_close_what_if(conn, order_id=order_id, position=position, mark=mark, broker=broker, clock=clock)

    return record


ExitKind = Literal["engine", "stop_loss", "take_profit", "unexplained"]

ExitUnmeasured = Literal[
    "multiple_closes",
    "fill_price_unusable",
    "what_if_missing",
    "what_if_failed",
    "what_if_undecodable",
    "what_if_unpriced",
]

#: Why ``gapped`` is unset on a stop or target close.
GapUnmeasured = Literal["session_candle_missing", "session_candle_off_basis"]


def classify_exit(*, engine_order: bool, fill: Decimal, stop: Decimal | None, take: Decimal | None) -> ExitKind:
    """The module docstring's attribution of one long close. A level of zero or below is no level."""
    if engine_order:
        return "engine"
    if stop is not None and stop.is_finite() and stop > 0 and fill <= stop:
        return "stop_loss"
    if take is not None and take.is_finite() and take > 0 and fill >= take:
        return "take_profit"
    return "unexplained"


@dataclass(frozen=True)
class ExitTca:
    strategy_trade_id: int
    broker_position_id: int
    instrument_id: int
    symbol: str | None
    closed_at: datetime
    fill: Decimal | None
    #: The position's single open fill, which keys its cost band (``cost_model``: fixed for the life of a position).
    entry_fill: Decimal | None
    kind: ExitKind | None
    #: The engine's exit order; None for a broker-side close.
    order_id: int | None
    submitted_at: datetime | None
    #: ``engine``: the module docstring's sell figures against the arrival quote, and the close-arm what-if rate.
    execution: Decimal | None
    effective_spread: Decimal | None
    quoted_spread: Decimal | None
    arrival_age_seconds: Decimal | None
    arrival_unmeasured: Unmeasured | None
    what_if_cost: Decimal | None
    #: ``stop_loss`` / ``take_profit``: the level hit, the fill's cost against it, and whether the session opened
    #: beyond it (None, with ``gap_unmeasured``, when the session's candle is missing or on another price basis).
    level: Decimal | None
    level_cost: Decimal | None
    gapped: bool | None
    gap_unmeasured: GapUnmeasured | None
    #: Why a figure the kind calls for is missing; None when nothing is.
    unmeasured: ExitUnmeasured | None


_EXITS_SQL: Final = """
    SELECT ow.strategy_trade_id, ow.broker_position_id, t.instrument_id, i.symbol,
           cl.n_closes, cl.fill, cl.closed_at, cl.stop_level, cl.take_level,
           CASE WHEN op.n_opens = 1 THEN op.entry_fill END AS entry_fill,
           ex.order_id, ex.created_at,
           a.outcome, a.received_at, a.quoted_at, a.bid, a.ask,
           w.outcome AS what_if_outcome, w.cost AS what_if_cost, w.ticket_amount,
           pd.open AS session_open, pd.low AS session_low, pd.high AS session_high
      FROM strategy_position_ownership ow
      JOIN strategy_trades t ON t.strategy_trade_id = ow.strategy_trade_id
      -- Long positions only: the attribution and the signs are a sell's.
      CROSS JOIN LATERAL (
            SELECT o.broker_environment
              FROM strategy_trade_orders sto JOIN orders o ON o.order_id = sto.order_id
             WHERE sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry' AND o.action = 'BUY'
             ORDER BY o.order_id
             LIMIT 1
      ) en
      CROSS JOIN LATERAL (
            -- min() of a single row is that row; with more than one, the close is not measured.
            SELECT count(*) AS n_closes, min(te.price) AS fill, min(te.executed_at) AS closed_at,
                   min((te.raw_payload->>'stopLossRate')::numeric) AS stop_level,
                   min((te.raw_payload->>'takeProfitRate')::numeric) AS take_level
              FROM trade_events te
             -- A NULL environment predates #3614's recording of it, when strategy orders went to demo only (every
             -- trade event of such a position is 'demo' on the dev DB, 2026-10-17).
             WHERE te.environment = COALESCE(en.broker_environment, 'demo') AND te.event_kind = 'close'
               AND te.position_id = ow.broker_position_id
      ) cl
      CROSS JOIN LATERAL (
            SELECT count(*) AS n_opens, min(te.price) AS entry_fill
              FROM trade_events te
             WHERE te.environment = COALESCE(en.broker_environment, 'demo') AND te.event_kind = 'open'
               AND te.position_id = ow.broker_position_id
      ) op
      LEFT JOIN LATERAL (
            -- The latest close the strategy sent that the broker did not refuse.
            SELECT o.order_id, o.created_at
              FROM strategy_trade_orders sto JOIN orders o ON o.order_id = sto.order_id
             WHERE sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'exit' AND o.status <> 'rejected'
             ORDER BY o.created_at DESC, o.order_id DESC
             LIMIT 1
      ) ex ON true
      LEFT JOIN strategy_order_arrival_quotes a ON a.order_id = ex.order_id
      LEFT JOIN strategy_order_close_what_ifs w ON w.order_id = ex.order_id
      LEFT JOIN instruments i ON i.instrument_id = t.instrument_id
      LEFT JOIN price_daily pd
             ON pd.instrument_id = t.instrument_id
            AND pd.price_date = (cl.closed_at AT TIME ZONE %(session_tz)s)::date
     WHERE cl.n_closes > 0 AND cl.closed_at >= %(since)s
     ORDER BY cl.closed_at, ow.broker_position_id
"""

_SESSION_TZ: Final = "America/New_York"


def _what_if_rate(row: dict[str, Any]) -> Decimal | ExitUnmeasured:
    outcome = row["what_if_outcome"]
    if outcome is None:
        return "what_if_missing"
    if outcome == "failed":
        return "what_if_failed"
    if outcome == "unpriced":
        return "what_if_unpriced"
    cost, ticket = row["what_if_cost"], row["ticket_amount"]
    if outcome == "undecodable" or cost is None or not cost.is_finite() or not ticket:
        return "what_if_undecodable"
    return Decimal(cost) / Decimal(ticket)


def _gapped(
    kind: Literal["stop_loss", "take_profit"],
    *,
    level: Decimal,
    fill: Decimal,
    open_: Decimal | None,
    low: Decimal | None,
    high: Decimal | None,
) -> tuple[bool, None] | tuple[None, GapUnmeasured]:
    """Whether the close's session opened beyond the level, judged only on a candle whose range holds the fill."""
    if open_ is None or low is None or high is None:
        return None, "session_candle_missing"
    if not all(x.is_finite() and x > 0 for x in (open_, low, high)) or not low <= fill <= high:
        return None, "session_candle_off_basis"
    return (open_ <= level if kind == "stop_loss" else open_ >= level), None


def exit_tca(row: dict[str, Any]) -> ExitTca:
    """One ``_EXITS_SQL`` row, measured."""
    fill: Decimal | None = row["fill"]
    kind: ExitKind | None = None
    unmeasured: ExitUnmeasured | None = None
    execution = effective_spread = quoted_spread = age = what_if = level = level_cost = None
    arrival_unmeasured: Unmeasured | None = None
    gapped: bool | None = None
    gap_unmeasured: GapUnmeasured | None = None
    if row["n_closes"] > 1:
        fill, unmeasured = None, "multiple_closes"
    elif fill is None or not fill.is_finite() or fill <= 0:
        unmeasured = "fill_price_unusable"
    else:
        stop, take = row["stop_level"], row["take_level"]
        kind = classify_exit(engine_order=row["order_id"] is not None, fill=fill, stop=stop, take=take)
        if kind == "engine":
            arrival = (
                None
                if row["outcome"] is None
                else ArrivalFacts(row["outcome"], row["received_at"], row["quoted_at"], row["bid"], row["ask"])
            )
            if arrival is not None and arrival.quoted_at is not None:
                age = Decimal(str((arrival.received_at - arrival.quoted_at).total_seconds()))
            usable = _usable_quote(arrival)
            if isinstance(usable, str):
                arrival_unmeasured = usable
            else:
                bid, ask = usable
                mid = (bid + ask) / 2
                execution = 1 - fill / mid  # a sell: positive when it sold below the mid
                effective_spread = 2 * abs(fill - mid) / mid
                quoted_spread = (ask - bid) / mid
            rate = _what_if_rate(row)
            if isinstance(rate, str):
                unmeasured = rate
            else:
                what_if = rate
        elif kind in ("stop_loss", "take_profit"):
            level = stop if kind == "stop_loss" else take
            if level is None:
                raise ValueError("classify_exit attributed a close to a missing level")
            level_cost = 1 - fill / level
            gapped, gap_unmeasured = _gapped(
                kind,
                level=level,
                fill=fill,
                open_=row["session_open"],
                low=row["session_low"],
                high=row["session_high"],
            )
    return ExitTca(
        strategy_trade_id=int(row["strategy_trade_id"]),
        broker_position_id=int(row["broker_position_id"]),
        instrument_id=int(row["instrument_id"]),
        symbol=row["symbol"],
        closed_at=row["closed_at"],
        fill=fill,
        entry_fill=row["entry_fill"],
        kind=kind,
        order_id=None if row["order_id"] is None else int(row["order_id"]),
        submitted_at=row["created_at"],
        execution=execution,
        effective_spread=effective_spread,
        quoted_spread=quoted_spread,
        arrival_age_seconds=age,
        arrival_unmeasured=arrival_unmeasured,
        what_if_cost=what_if,
        level=level,
        level_cost=level_cost,
        gapped=gapped,
        gap_unmeasured=gap_unmeasured,
        unmeasured=unmeasured,
    )


def load_exit_tca(conn: Conn, *, since: date) -> list[ExitTca]:
    """Every close of a strategy-owned long position on or after ``since`` (UTC midnight), measured."""
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            _EXITS_SQL, {"since": datetime.combine(since, time(), tzinfo=UTC), "session_tz": _SESSION_TZ}
        ).fetchall()
    return [exit_tca(r) for r in rows]


BandSide = Literal["entry", "exit"]


@dataclass(frozen=True)
class BandRow:
    """One week, side and cost band of measured fills, beside what ``cost_model`` charges that band."""

    #: Monday of the fill's ISO week (UTC).
    week: date
    side: BandSide
    #: The band of the position's entry fill; None when that fill is unknown.
    band: PriceBand | None
    fills: int
    #: Per-side cost against the arrival mid (``execution``), a fraction, comparable with ``model_half_spread``.
    executions: tuple[Decimal, ...]
    #: The broker's what-if cost rate, a fraction, comparable with ``model_spread`` (#2598: the what-if spread
    #: matched the quoted spread at a median 0.995x).
    what_ifs: tuple[Decimal, ...]

    @property
    def model_half_spread(self) -> Decimal | None:
        """``cost_model``'s per-side charge for the band, as a fraction (``PriceBand.half_spread``)."""
        return None if self.band is None else self.band.half_spread

    @property
    def model_spread(self) -> Decimal | None:
        """The band's full p75 spread as a fraction: both sides' charge, from the same fraction property."""
        return None if self.band is None else 2 * self.band.half_spread


def _week(at: datetime) -> date:
    if at.tzinfo is None:
        # astimezone would read a naive stamp as local time. The loaders read TIMESTAMPTZ, so they never send one.
        raise ValueError("a fill time must be timezone-aware")
    day = at.astimezone(UTC).date()
    return day - timedelta(days=day.weekday())


def band_rows(entries: Sequence[EntryTca], exits: Sequence[ExitTca]) -> list[BandRow]:
    """Filled entries and engine exits grouped by fill week, side and the cost band of the position's entry fill.
    Stop and target closes are left out: they are costed against their level, not a mid, so they have no figure
    comparable with a spread."""
    fills: Counter[tuple[date, BandSide, PriceBand | None]] = Counter()
    executions: dict[tuple[date, BandSide, PriceBand | None], list[Decimal]] = defaultdict(list)
    what_ifs: dict[tuple[date, BandSide, PriceBand | None], list[Decimal]] = defaultdict(list)

    def add(
        at: datetime, side: BandSide, keyed_on: Decimal | None, execution: Decimal | None, what_if: Decimal | None
    ) -> None:
        key = (_week(at), side, None if keyed_on is None else band_for(keyed_on))
        fills[key] += 1
        if execution is not None:
            executions[key].append(execution)
        if what_if is not None:
            what_ifs[key].append(what_if)

    for e in entries:
        if e.fill is not None and e.filled_at is not None:
            add(e.filled_at, "entry", e.fill, None if e.costs is None else e.costs.execution, e.what_if_cost)
    for x in exits:
        if x.kind == "engine" and x.fill is not None:
            add(x.closed_at, "exit", x.entry_fill, x.execution, x.what_if_cost)

    def rank(key: tuple[date, BandSide, PriceBand | None]) -> tuple[date, BandSide, int]:
        week, side, band = key
        return week, side, len(BANDS) if band is None else BANDS.index(band)

    return [
        BandRow(key[0], key[1], key[2], fills[key], tuple(executions[key]), tuple(what_ifs[key]))
        for key in sorted(fills, key=rank)
    ]


__all__ = [
    "ARRIVAL_MAX_AGE_SECONDS",
    "ArrivalFacts",
    "ArrivalOutcome",
    "ArrivalQuote",
    "BandRow",
    "BandSide",
    "CloseWhatIfOutcome",
    "EntryCosts",
    "EntryTca",
    "ExitEvidence",
    "ExitKind",
    "ExitTca",
    "GapUnmeasured",
    "band_rows",
    "classify_exit",
    "close_cost",
    "entry_costs",
    "entry_tca",
    "exit_evidence",
    "exit_tca",
    "load_entry_tca",
    "load_exit_tca",
    "record_arrival_quote",
    "record_close_what_if",
]
