"""Trade-events ledger ingest (#1593).

Append-only ``trade_events`` ledger of broker-observed position opens
and closes. Spec: docs/proposals/etl/2026-06-13-etoro-trade-ledger.md.

Writer topology: ``sync_portfolio`` is the ONLY writer — open events
from the portfolio-payload positions, open+close events from the
trade-history rows its callers fetch and pass in. The eBull order path
never writes here (its broker_positions rows carry synthetic negative
position ids, #227); it enqueues an immediate sync instead.

Units contract (spec §1.7): an open event's ``units`` is the position's
ORIGINAL opened size; each close event's ``units`` is that slice's
delta. ``conflict_anomaly`` counts every observation that violates this
model — loud, never silently merged.

⚠ Partial closes: sql/194's comment (and this docstring until #3616
slice 3c) said a partial close reduces the same positionId. On the demo
account it mints a NEW position id; the broker's order lookup lists the
pieces under one order with one shared opening, and the history rows of
the pieces carry ``parentPositionId`` 0, so the order id is the only
stored lineage (slice 3c spec, fact 5). The applied migration's comment
stays as written.

Rows are immutable: conflicts are ``ON CONFLICT DO NOTHING`` against
the two partial unique indexes (sql/194); first observation wins.
Portfolio-sourced opens are preferred within a batch because they carry
``initialUnits`` (history-derived opens are best-effort Σ-of-slices).

Provenance (#3616 slice 3c, sql/485; spec Design 2): every batch is
checked against ``trade_ledger_scope`` (one environment and one account
per ledger) before any row is written, every call writes one
``trade_history_fetches`` row, and an observation that meets an existing
row is retained in ``trade_event_observations`` when it differs from the
latest one retained for that row and source. A batch that is not written
keeps its payloads as ``unscoped`` observations.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import httpx
import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerClosedTrade, BrokerPosition, BrokerProvider
from app.providers.implementations.etoro_broker import TradeHistoryParseError
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn

logger = logging.getLogger(__name__)

EventKind = Literal["open", "close"]
EventSide = Literal["buy", "sell"]
EventSource = Literal["etoro_sync", "etoro_history"]

# Pre-dates any eToro retail account we could hold; the deep-backfill
# minDate for an empty ledger.
#
# ⚠⚠ This KNOWINGLY breaches the documented lookback and cannot stop doing so (#2991).
# The history operation states, on the operation rather than on the `minDate` parameter:
# *"Keep each request's lookback to less than 1 year (maximum 1 year minus 1 day). For
# longer history, split the range into successive windows of at most that length (advance
# `minDate` per batch)"*. That windowing is not implementable here — the operation takes
# `x-request-id`, `minDate` (required), `page` and `pageSize` and **no upper-bound date
# parameter**, so every request ends at the present and advancing `minDate` only SHRINKS
# the returned set. A compliant request can therefore reach back 364 days at most — and
# ledger §4's synthesized-open transform sums every slice of a never-seen position, so the
# batch has to contain slices older than that. ⚠ The constraint is REACH, not one request:
# several requests could be aggregated before ingest, but with no upper bound the only
# request that reaches a slice older than 364 days is an over-length one.
#
# Measured on demo 2026-09-13 rather than assumed: the over-length request was served,
# and it returned a close far outside a one-year cap — which falsifies the only reading
# under which the documented advice tiles a range (a server-side `[minDate, minDate + 1y]`
# cap). The figures are in the observation set, not written down here; reproduce with
# `PYTHONPATH=. uv run python -m scripts.probe_2991_history_lookback`. The guard that
# expires this rebuttal if eToro ever adds `maxDate` is
# `tests/test_2991_history_lookback_contract.py`.
HISTORY_EPOCH = datetime(2017, 1, 1, tzinfo=UTC)

# Re-fetch window behind the watermark — absorbs clock skew and
# late-arriving rows; the unique indexes make the overlap idempotent.
_WATERMARK_OVERLAP = timedelta(days=7)

LedgerEnvironment = Literal["demo", "real"]


def ledger_environment(value: str) -> LedgerEnvironment:
    """The provider's environment as a ledger environment; anything else raises rather than being labelled."""
    if value == "demo":
        return "demo"
    if value == "real":
        return "real"
    raise ValueError(f"unknown broker environment {value!r}")


FetchOutcome = Literal["ok", "failed", "scope_unverified", "scope_refused"]
ObservationKind = Literal["supplemental", "amendment", "lifecycle", "unscoped"]

# The payload fields an observation is compared on: the rows of the slice 3c field table (spec Design 4) for each
# source, lower-cased because keys match case-insensitively (spec Design 4.3, fact 15). A history open's payload is
# its earliest close row (``events_from_history``), so it is compared on the history fields that describe the open.
_SYNC_FIELDS = (
    "cid",
    "orderid",
    "positionid",
    "instrumentid",
    "settlementtypeid",
    "isbuy",
    "ispartiallyaltered",
    "leverage",
    "mirrorid",
    "parentpositionid",
    "initialunits",
    "initialamountindollars",
    "totalexternalfees",
    "totalexternaltaxes",
    "totalfees",
    "openrate",
    "openconversionrate",
    "opendatetime",
)
_HISTORY_OPEN_FIELDS = (
    "orderid",
    "positionid",
    "instrumentid",
    "isbuy",
    "leverage",
    "socialtradeid",
    "parentpositionid",
    "initialinvestment",
    "openrate",
    "opentimestamp",
)
_HISTORY_CLOSE_FIELDS = (
    *_HISTORY_OPEN_FIELDS,
    "units",
    "closetimestamp",
    "investment",
    "netprofit",
    "fees",
    "closerate",
)
_TIMESTAMP_FIELDS = frozenset({"opendatetime", "opentimestamp", "closetimestamp"})
# A difference in this field alone is a lifecycle observation, not an amendment (spec Design 2).
_LIFECYCLE_FIELD = "ispartiallyaltered"


def _compared_fields(source: EventSource, event_kind: EventKind) -> tuple[str, ...]:
    if source == "etoro_sync":
        return _SYNC_FIELDS
    return _HISTORY_CLOSE_FIELDS if event_kind == "close" else _HISTORY_OPEN_FIELDS


def _canonical_decimal(value: Decimal) -> str:
    """``format(d, 'f')`` with trailing fractional zeros and a trailing point removed, ``-0`` written ``0``."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def _normalise_value(field_name: str, value: Any) -> Any:
    """One payload value in comparable JSON form (spec Design 4.3).

    Numbers become canonical decimal strings, so ``1`` and ``1.0`` are one value; a float is read through ``str``, the
    shortest text that round-trips it, which is what the stored JSONB holds (fact 17). Timestamps become UTC. Booleans
    stay booleans (never numbers), and anything else is kept as given.
    """
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, Decimal)):
        return _canonical_decimal(Decimal(value))
    if isinstance(value, float):
        return _canonical_decimal(Decimal(str(value)))
    if isinstance(value, str) and field_name in _TIMESTAMP_FIELDS:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
        if parsed.tzinfo is None:
            return value
        return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return value


def normalise_payload(source: EventSource, event_kind: EventKind, payload: dict[str, Any]) -> dict[str, Any]:
    """The compared fields of one payload. An absent field is absent (never zero); two spellings of one field with
    different values are kept as ``{"conflict": [...]}`` so the disagreement is itself compared."""
    normalised: dict[str, Any] = {}
    for name in _compared_fields(source, event_kind):
        values: list[Any] = []
        for key, raw in payload.items():
            if key.lower() == name:
                value = _normalise_value(name, raw)
                if value not in values:
                    values.append(value)
        if len(values) == 1:
            normalised[name] = values[0]
        elif values:
            normalised[name] = {"conflict": sorted(values, key=repr)}
    return normalised


def classify_observation(
    baseline: dict[str, Any] | None,
    observed: dict[str, Any],
) -> ObservationKind | None:
    """The observation to retain for a payload meeting an existing row, or None to retain nothing (spec Design 2)."""
    if baseline is None:
        return "supplemental"
    if baseline == observed:
        return None
    differing = {k for k in baseline.keys() | observed.keys() if baseline.get(k, _ABSENT) != observed.get(k, _ABSENT)}
    return "lifecycle" if differing == {_LIFECYCLE_FIELD} else "amendment"


_ABSENT = object()


@dataclass(frozen=True)
class TradeHistoryFetch:
    """One history fetch: its request bound and timing, and its rows (None when the fetch failed)."""

    min_date: datetime
    started_at: datetime
    finished_at: datetime
    rows: Sequence[BrokerClosedTrade] | None


def payload_account_id(payload: dict[str, Any]) -> int | None:
    """The payload's ``CID`` when it is one valid id (a positive integer under every spelling), else None."""
    values = [v for k, v in payload.items() if k.lower() == "cid"]
    if not values:
        return None
    first = values[0]
    for value in values:
        # Each spelling on its own: an int, never a bool or a float that compares equal to one.
        if type(value) is not int or value <= 0 or value != first:
            return None
    return first


@dataclass(frozen=True)
class TradeEvent:
    """One broker-observed open or close, ready for insertion."""

    position_id: int
    etoro_instrument_id: int
    event_kind: EventKind
    side: EventSide
    units: Decimal
    price: Decimal | None
    executed_at: datetime
    source: EventSource
    raw_payload: dict[str, Any]
    fees_usd: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    investment_usd: Decimal | None = None
    order_id: int | None = None
    social_trade_id: int | None = None
    parent_position_id: int | None = None


@dataclass
class TradeEventCounters:
    """Closed-set ingest counters (spec §15). Logged by sync_portfolio."""

    inserted: int = 0
    duplicate: int = 0
    unresolved_instrument: int = 0
    null_price: int = 0
    conflict_anomaly: int = 0
    skipped_other: int = 0
    skip_reasons: list[str] = field(default_factory=list)
    fetch_outcome: str = ""

    def log_line(self) -> str:
        line = (
            f"fetch={self.fetch_outcome} "
            f"inserted={self.inserted} duplicate={self.duplicate} "
            f"unresolved_instrument={self.unresolved_instrument} "
            f"null_price={self.null_price} "
            f"conflict_anomaly={self.conflict_anomaly} "
            f"other={self.skipped_other}"
        )
        if self.skip_reasons:
            line += f" reasons={self.skip_reasons}"
        return line


def _side(kind: EventKind, is_buy: bool) -> EventSide:
    """Open leg of a long is a buy and its close a sell; shorts invert."""
    if kind == "open":
        return "buy" if is_buy else "sell"
    return "sell" if is_buy else "buy"


def _positive_or_none(value: Decimal | None) -> Decimal | None:
    """Sentinel guard: a non-positive rate is NOT a valid mark (prevention log).

    Pure mapping — the ``null_price`` counter is incremented at INGEST
    time for rows that actually land, so merge-dropped duplicates never
    inflate it (Codex ckpt-2 MED).
    """
    if value is None or value <= 0:
        return None
    return value


def open_events_from_positions(
    positions: Sequence[BrokerPosition],
    counters: TradeEventCounters,
) -> list[TradeEvent]:
    """Open events for every real-id position in the portfolio payload.

    Idempotent against the ledger (DO NOTHING on the per-position open
    key), so every sync re-emits all opens rather than diffing —
    simpler, and heals any gap. Synthetic negative ids (#227) and
    id-less legacy rows are excluded by construction.

    Fees are NOT stamped on open events: the position's ``totalFees``
    accumulates over its whole life and the authoritative per-trade fee
    arrives on the history close row — stamping both would double-count
    for consumers. The raw payload keeps the running figure.
    """
    events: list[TradeEvent] = []
    for bp in positions:
        if bp.position_id is None or bp.position_id < 0:
            continue
        if bp.open_date_time is None:
            # Cannot fabricate an executed_at (external timestamps must
            # come from the API response, never now() — prevention log).
            counters.skipped_other += 1
            counters.skip_reasons.append(f"open_event position {bp.position_id}: no openDateTime in payload")
            continue
        units = bp.initial_units if bp.initial_units is not None and bp.initial_units > 0 else bp.units
        if units <= 0:
            counters.skipped_other += 1
            counters.skip_reasons.append(f"open_event position {bp.position_id}: non-positive units")
            continue
        events.append(
            TradeEvent(
                position_id=bp.position_id,
                etoro_instrument_id=bp.instrument_id,
                event_kind="open",
                side=_side("open", bp.is_buy),
                units=units,
                price=_positive_or_none(bp.open_price),
                executed_at=bp.open_date_time,
                source="etoro_sync",
                raw_payload=bp.raw_payload,
                investment_usd=bp.initial_amount_in_dollars if bp.initial_amount_in_dollars > 0 else None,
            )
        )
    return events


def events_from_history(
    trades: Sequence[BrokerClosedTrade],
    counters: TradeEventCounters,
) -> list[TradeEvent]:
    """Transform history rows into events: one close per slice plus one
    synthesized open per position (Σ slice units, earliest open leg).

    The synthesized open is only a fallback for positions the ledger
    never saw open in a portfolio payload — merge_events() prefers a
    portfolio-sourced open, and the DB open key keeps whichever landed
    first thereafter.
    """
    by_position: dict[int, list[BrokerClosedTrade]] = {}
    for t in trades:
        if t.position_id < 0:
            # Never observed in probes; guard matches the ledger CHECK.
            counters.skipped_other += 1
            counters.skip_reasons.append(f"history row position {t.position_id}: negative id")
            continue
        by_position.setdefault(t.position_id, []).append(t)

    events: list[TradeEvent] = []
    for position_id, rows in by_position.items():
        rows.sort(key=lambda t: t.open_timestamp)
        earliest = rows[0]
        total_units = sum((t.units for t in rows), Decimal("0"))
        if total_units > 0:
            events.append(
                TradeEvent(
                    position_id=position_id,
                    etoro_instrument_id=earliest.instrument_id,
                    event_kind="open",
                    side=_side("open", earliest.is_buy),
                    units=total_units,
                    price=_positive_or_none(earliest.open_rate),
                    executed_at=earliest.open_timestamp,
                    source="etoro_history",
                    raw_payload=earliest.raw_payload,
                    investment_usd=earliest.initial_investment,
                    order_id=earliest.order_id,
                    social_trade_id=earliest.social_trade_id,
                    parent_position_id=earliest.parent_position_id,
                )
            )
        for t in rows:
            if t.units <= 0:
                counters.skipped_other += 1
                counters.skip_reasons.append(f"history row position {position_id}: non-positive units")
                continue
            events.append(
                TradeEvent(
                    position_id=position_id,
                    etoro_instrument_id=t.instrument_id,
                    event_kind="close",
                    side=_side("close", t.is_buy),
                    units=t.units,
                    price=_positive_or_none(t.close_rate),
                    executed_at=t.close_timestamp,
                    source="etoro_history",
                    raw_payload=t.raw_payload,
                    fees_usd=t.fees,
                    realized_pnl_usd=t.net_profit,
                    investment_usd=t.investment,
                    order_id=t.order_id,
                    social_trade_id=t.social_trade_id,
                    parent_position_id=t.parent_position_id,
                )
            )
    return events


def merge_events(
    portfolio_opens: Sequence[TradeEvent],
    history_events: Sequence[TradeEvent],
) -> tuple[list[TradeEvent], list[TradeEvent]]:
    """Combine the two streams, preferring a portfolio-sourced open for
    any position present in both (it carries initialUnits; the
    history-derived open is Σ-of-slices best-effort).

    Returns ``(merged, set_aside)``: the history opens set aside are
    compared against the kept row and retained like any later
    observation (#3616 slice 3c, spec Design 2)."""
    open_positions_covered = {e.position_id for e in portfolio_opens}
    merged = list(portfolio_opens)
    set_aside: list[TradeEvent] = []
    for e in history_events:
        if e.event_kind == "open" and e.position_id in open_positions_covered:
            set_aside.append(e)
            continue
        merged.append(e)
    return merged, set_aside


def compute_history_min_date(conn: psycopg.Connection[Any]) -> datetime:
    """minDate for the next trade-history fetch.

    The endpoint filters on closeTimestamp (probed, spec §0), so the
    max ingested close is the correct watermark; the overlap re-walks a
    window the unique indexes dedupe. Empty ledger → deep backfill.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute(
            """
            SELECT MAX(executed_at) AS watermark
            FROM trade_events
            WHERE event_kind = 'close' AND source = 'etoro_history'
            """
        ).fetchone()
    watermark = row["watermark"] if row else None
    if watermark is None:
        return HISTORY_EPOCH
    return watermark - _WATERMARK_OVERLAP


_INSERT_OPEN_SQL = """
    INSERT INTO trade_events
        (position_id, etoro_instrument_id, instrument_id, event_kind, side,
         units, price, executed_at, fees_usd, realized_pnl_usd,
         investment_usd, order_id, social_trade_id, parent_position_id,
         source, raw_payload, environment)
    VALUES
        (%(position_id)s, %(etoro_instrument_id)s, %(instrument_id)s,
         %(event_kind)s, %(side)s, %(units)s, %(price)s, %(executed_at)s,
         %(fees_usd)s, %(realized_pnl_usd)s, %(investment_usd)s,
         %(order_id)s, %(social_trade_id)s, %(parent_position_id)s,
         %(source)s, %(raw_payload)s, %(environment)s)
    ON CONFLICT (position_id) WHERE event_kind = 'open' DO NOTHING
"""

_INSERT_CLOSE_SQL = """
    INSERT INTO trade_events
        (position_id, etoro_instrument_id, instrument_id, event_kind, side,
         units, price, executed_at, fees_usd, realized_pnl_usd,
         investment_usd, order_id, social_trade_id, parent_position_id,
         source, raw_payload, environment)
    VALUES
        (%(position_id)s, %(etoro_instrument_id)s, %(instrument_id)s,
         %(event_kind)s, %(side)s, %(units)s, %(price)s, %(executed_at)s,
         %(fees_usd)s, %(realized_pnl_usd)s, %(investment_usd)s,
         %(order_id)s, %(social_trade_id)s, %(parent_position_id)s,
         %(source)s, %(raw_payload)s, %(environment)s)
    ON CONFLICT (position_id, executed_at) WHERE event_kind = 'close' DO NOTHING
"""


def ingest_trade_events(
    conn: psycopg.Connection[Any],
    events: Sequence[TradeEvent],
    counters: TradeEventCounters,
    *,
    environment: LedgerEnvironment,
    fetch_id: int,
    set_aside: Sequence[TradeEvent] = (),
) -> TradeEventCounters:
    """Insert events; immutable semantics (DO NOTHING + loud anomalies).

    Runs on the caller's transaction — never commits (sync_portfolio's
    contract). All I/O happened before this call; pure DB writes here.
    The caller has checked the batch against ``trade_ledger_scope``.

    An event meeting an existing row is passed to
    ``_retain_observation``; so is each history open ``merge_events``
    set aside, after the kept row is inserted.
    """
    if not events and not set_aside:
        return counters

    distinct_ids = sorted({e.etoro_instrument_id for e in events})
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        rows = cur.execute(
            "SELECT instrument_id FROM instruments WHERE instrument_id = ANY(%s)",
            [distinct_ids],
        ).fetchall()
    known_instruments = {row["instrument_id"] for row in rows}

    touched_positions: set[int] = set()
    for event in events:
        resolved = event.etoro_instrument_id if event.etoro_instrument_id in known_instruments else None
        if resolved is None:
            counters.unresolved_instrument += 1
        params = {
            "position_id": event.position_id,
            "etoro_instrument_id": event.etoro_instrument_id,
            "instrument_id": resolved,
            "event_kind": event.event_kind,
            "side": event.side,
            "units": event.units,
            "price": event.price,
            "executed_at": event.executed_at,
            "fees_usd": event.fees_usd,
            "realized_pnl_usd": event.realized_pnl_usd,
            "investment_usd": event.investment_usd,
            "order_id": event.order_id,
            "social_trade_id": event.social_trade_id,
            "parent_position_id": event.parent_position_id,
            "source": event.source,
            "raw_payload": Jsonb(event.raw_payload),
            # Always explicit: sql/485's column DEFAULT exists only for the pre-reload window, and slice 3c
            # part 2's migration drops it.
            "environment": environment,
        }
        sql = _INSERT_OPEN_SQL if event.event_kind == "open" else _INSERT_CLOSE_SQL
        result = conn.execute(sql, params)
        if result.rowcount == 1:
            counters.inserted += 1
            if event.price is None:
                # Landed with NULL price (absent or sentinel rate) —
                # counted here, not at transform time, so merge-dropped
                # duplicates never inflate the figure (spec §15).
                counters.null_price += 1
            touched_positions.add(event.position_id)
            continue
        # Conflict: expected duplicate unless the stored row disagrees
        # on the financial figures — that is an anomaly worth shouting
        # about (advisory check; race-tolerant, logging only).
        existing = _stored_row(conn, event)
        if existing is None:
            # Row vanished between INSERT and SELECT — impossible for an
            # append-only table outside test truncation; count loudly.
            counters.conflict_anomaly += 1
            logger.warning(
                "trade_events: conflict with no existing row (position %d %s) — investigate",
                event.position_id,
                event.event_kind,
            )
            continue
        same_units = Decimal(str(existing["units"])) == event.units
        same_pnl = (existing["realized_pnl_usd"] is None and event.realized_pnl_usd is None) or (
            existing["realized_pnl_usd"] is not None
            and event.realized_pnl_usd is not None
            and Decimal(str(existing["realized_pnl_usd"])) == event.realized_pnl_usd
        )
        if same_units and (event.event_kind == "open" or same_pnl):
            counters.duplicate += 1
        else:
            counters.conflict_anomaly += 1
            logger.warning(
                "trade_events: %s event for position %d conflicts with stored figures "
                "(stored units=%s pnl=%s, incoming units=%s pnl=%s) — first observation kept; "
                "incoming payload: %s",
                event.event_kind,
                event.position_id,
                existing["units"],
                existing["realized_pnl_usd"],
                event.units,
                event.realized_pnl_usd,
                event.raw_payload,
            )
        _retain_observation(conn, event, existing, fetch_id)

    for event in set_aside:
        existing = _stored_row(conn, event)
        if existing is None:
            counters.conflict_anomaly += 1
            logger.warning(
                "trade_events: set-aside history open for position %d has no stored open — investigate",
                event.position_id,
            )
            continue
        _retain_observation(conn, event, existing, fetch_id)

    _check_close_sum_invariant(conn, touched_positions, counters)
    return counters


def _stored_row(conn: psycopg.Connection[Any], event: TradeEvent) -> dict[str, Any] | None:
    """The stored row an event meets on its unique index (sql/194)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        return cur.execute(
            """
            SELECT event_id, source, raw_payload, units, realized_pnl_usd
            FROM trade_events
            WHERE position_id = %(pid)s AND event_kind = %(kind)s
              AND (%(kind)s = 'open' OR executed_at = %(executed_at)s)
            ORDER BY event_id
            LIMIT 1
            """,
            {"pid": event.position_id, "kind": event.event_kind, "executed_at": event.executed_at},
        ).fetchone()


def _retain_observation(
    conn: psycopg.Connection[Any],
    event: TradeEvent,
    stored: dict[str, Any],
    fetch_id: int,
) -> None:
    """Retain an observation of a stored row when it differs from the baseline (spec Design 2).

    The baseline is the latest observation of ``(event_id, source, event_kind)`` of any kind; failing that, the
    stored row if it has this source; otherwise there is none. Whatever is written becomes the next baseline.
    """
    stored_normalised = normalise_payload(stored["source"], event.event_kind, stored["raw_payload"])
    observed_normalised = normalise_payload(event.source, event.event_kind, event.raw_payload)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        latest = cur.execute(
            """
            SELECT observed_normalised
            FROM trade_event_observations
            WHERE event_id = %(event_id)s AND source = %(source)s AND event_kind = %(kind)s
            ORDER BY observation_id DESC
            LIMIT 1
            """,
            {"event_id": stored["event_id"], "source": event.source, "kind": event.event_kind},
        ).fetchone()
    if latest is not None:
        baseline: dict[str, Any] | None = latest["observed_normalised"]
    elif stored["source"] == event.source:
        baseline = stored_normalised
    else:
        baseline = None
    kind = classify_observation(baseline, observed_normalised)
    if kind is None:
        return
    conn.execute(
        """
        INSERT INTO trade_event_observations
            (event_id, position_id, event_kind, executed_at, fetch_id, kind, observed_at, source,
             stored_normalised, observed_normalised, observed_raw)
        VALUES
            (%(event_id)s, %(position_id)s, %(event_kind)s, %(executed_at)s, %(fetch_id)s, %(kind)s, now(),
             %(source)s, %(stored)s, %(observed)s, %(raw)s)
        """,
        {
            "event_id": stored["event_id"],
            "position_id": event.position_id,
            "event_kind": event.event_kind,
            "executed_at": event.executed_at if event.event_kind == "close" else None,
            "fetch_id": fetch_id,
            "kind": kind,
            "source": event.source,
            "stored": Jsonb(stored_normalised),
            "observed": Jsonb(observed_normalised),
            "raw": Jsonb(event.raw_payload),
        },
    )


def _check_close_sum_invariant(
    conn: psycopg.Connection[Any],
    position_ids: set[int],
    counters: TradeEventCounters,
) -> None:
    """Σ(close units) must not exceed the open's units (spec §4)."""
    if not position_ids:
        return
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        rows = cur.execute(
            """
            SELECT o.position_id, o.units AS open_units,
                   SUM(c.units) AS closed_units
            FROM trade_events o
            JOIN trade_events c
              ON c.position_id = o.position_id AND c.event_kind = 'close'
            WHERE o.event_kind = 'open'
              AND o.position_id = ANY(%s)
            GROUP BY o.position_id, o.units
            HAVING SUM(c.units) > o.units
            """,
            [sorted(position_ids)],
        ).fetchall()
    for row in rows:
        counters.conflict_anomaly += 1
        logger.warning(
            "trade_events: position %d closes sum to %s > open units %s — "
            "partial-close model violation, investigate (spec §22.1)",
            row["position_id"],
            row["closed_units"],
            row["open_units"],
        )


# Must equal scheduler.JOB_DAILY_PORTFOLIO_SYNC — duplicated here because
# importing the scheduler would be circular (it imports this module).
# Pinned by tests/test_trade_events.py::test_post_trade_sync_job_name.
POST_TRADE_SYNC_JOB = "daily_portfolio_sync"


def enqueue_post_trade_sync(conn: psycopg.Connection[Any], *, requested_by: str) -> None:
    """Queue an immediate portfolio sync after a successful fill so the
    trade lands in the ledger within seconds instead of the next 5-min
    tick (#1593 spec §1.3). Runs on the caller's transaction — NOTIFY
    fires at commit, so the jobs process wakes only once the fill row
    is durable. If the enqueue is lost, the scheduled tick covers it.
    """
    publish_manual_job_request_with_conn(conn, POST_TRADE_SYNC_JOB, requested_by=requested_by)


def fetch_trade_history_safely(
    broker: BrokerProvider,
    min_date: datetime,
) -> TradeHistoryFetch:
    """Fetch history with the spec §7 error posture; ``rows`` None = skip ingest.

    A failed fetch must never block the position sync: 4xx request-shape
    or permission regressions are loud ERRORs (deterministic — fix code
    or key scopes); transient network/429/5xx are WARNINGs. Either way
    the watermark is untouched, so the next sync re-covers the window.
    The bound and timing are kept either way for ``trade_history_fetches``.
    """
    started_at = datetime.now(UTC)
    rows = _fetch_trade_history_rows(broker, min_date)
    return TradeHistoryFetch(min_date=min_date, started_at=started_at, finished_at=datetime.now(UTC), rows=rows)


def _fetch_trade_history_rows(
    broker: BrokerProvider,
    min_date: datetime,
) -> Sequence[BrokerClosedTrade] | None:
    try:
        return broker.get_trade_history(min_date)
    except httpx.HTTPStatusError as exc:
        log = logger.error if exc.response.status_code in (400, 403) else logger.warning
        log(
            "trade history fetch failed (HTTP %d) — positions still sync, watermark unchanged",
            exc.response.status_code,
            exc_info=True,
        )
        return None
    except httpx.HTTPError:
        logger.warning(
            "trade history fetch failed (network) — positions still sync, watermark unchanged",
            exc_info=True,
        )
        return None
    except TradeHistoryParseError:
        logger.error(
            "trade history response failed to parse — deterministic, fix the parser; "
            "positions still sync, watermark unchanged",
            exc_info=True,
        )
        return None
    except NotImplementedError:
        # BrokerProvider ships a non-abstract default that raises — a
        # provider without an override must degrade to history-skip,
        # never break the position sync (review round-3 WARNING).
        logger.error(
            "broker provider %s does not implement get_trade_history — "
            "positions still sync, ledger gets no history from this provider",
            type(broker).__name__,
            exc_info=True,
        )
        return None


def record_trade_events(
    conn: psycopg.Connection[Any],
    positions: Sequence[BrokerPosition],
    trade_history: TradeHistoryFetch | None,
    *,
    environment: LedgerEnvironment,
    portfolio_fetched_at: datetime,
) -> TradeEventCounters:
    """Single entry point used by sync_portfolio.

    ``trade_history`` None means no history fetch was attempted; a fetch
    whose ``rows`` is None failed. Either way the opens still ingest.

    The batch is checked against ``trade_ledger_scope`` before any row is
    written (#3616 slice 3c, spec Design 2): every real-id sync payload
    must carry one valid ``CID``, all equal. With no scope row, a batch
    with an account establishes the scope; with one, the batch is written
    only if its environment and account equal the scope's. A batch with
    no account (no open positions) or a refused batch writes no ledger
    row: its payloads are retained as ``unscoped`` observations, and the
    watermark, which moves only on written history closes, re-covers the
    window on the next batch that carries the account.
    """
    counters = TradeEventCounters()
    real_positions = [bp for bp in positions if bp.position_id is not None and bp.position_id >= 0]
    account_ids = [payload_account_id(bp.raw_payload) for bp in real_positions]
    # Callers hold sync_portfolio's transaction lock (_SYNC_PORTFOLIO_XACT_LOCK_KEY), so batches are serial; the
    # scope insert below still tolerates a concurrent first writer rather than aborting the sync.
    scope = _load_scope(conn)

    batch_account: int | None = None
    outcome: FetchOutcome
    if any(a is None for a in account_ids) or len(set(account_ids)) > 1:
        outcome = "scope_refused"
    elif not account_ids:
        outcome = "scope_unverified"
    else:
        batch_account = account_ids[0]
        if scope is None:
            established = conn.execute(
                """
                INSERT INTO trade_ledger_scope (singleton, environment, account_id, established_at)
                VALUES (TRUE, %(env)s, %(account)s, now())
                ON CONFLICT (singleton) DO NOTHING
                """,
                {"env": environment, "account": batch_account},
            )
            if established.rowcount == 1:
                logger.info("trade_events: ledger scope established (environment %s)", environment)
            scope = _load_scope(conn)
        if scope is None or scope["environment"] != environment or scope["account_id"] != batch_account:
            outcome = "scope_refused"
        else:
            outcome = "ok" if trade_history is not None and trade_history.rows is not None else "failed"

    fetch_id = _insert_fetch_row(
        conn,
        environment=environment,
        account_id=batch_account,
        portfolio_fetched_at=portfolio_fetched_at,
        portfolio_real_positions=len(real_positions),
        trade_history=trade_history,
        outcome=outcome,
    )
    counters.fetch_outcome = outcome

    history_rows = trade_history.rows if trade_history is not None else None
    history_events = events_from_history(history_rows, counters) if history_rows is not None else []
    if outcome in ("scope_unverified", "scope_refused"):
        logger.warning(
            "trade_events: batch not written (%s, environment %s, %d real position(s)); payloads retained as unscoped",
            outcome,
            environment,
            len(real_positions),
        )
        _retain_unscoped(conn, real_positions, history_events, fetch_id)
        return counters

    opens = open_events_from_positions(real_positions, counters)
    # A scoped payload that forms no open event (no openDateTime, non-positive units) has no row to bind an
    # observation to; it is retained as unscoped, so its values are never lost and derivation treats its order as
    # unmatched (spec Design 2).
    opened = {e.position_id for e in opens}
    unopened = [bp for bp in real_positions if bp.position_id not in opened]
    if unopened:
        _retain_unscoped(conn, unopened, [], fetch_id)
    merged, set_aside = merge_events(opens, history_events)
    return ingest_trade_events(
        conn,
        merged,
        counters,
        environment=environment,
        fetch_id=fetch_id,
        set_aside=set_aside,
    )


def _load_scope(conn: psycopg.Connection[Any]) -> dict[str, Any] | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        return cur.execute("SELECT environment, account_id FROM trade_ledger_scope").fetchone()


def _insert_fetch_row(
    conn: psycopg.Connection[Any],
    *,
    environment: LedgerEnvironment,
    account_id: int | None,
    portfolio_fetched_at: datetime,
    portfolio_real_positions: int,
    trade_history: TradeHistoryFetch | None,
    outcome: FetchOutcome,
) -> int:
    rows = trade_history.rows if trade_history is not None else None
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute(
            """
            INSERT INTO trade_history_fetches
                (environment, account_id, portfolio_fetched_at, portfolio_real_positions,
                 min_date, started_at, finished_at, outcome, row_count)
            VALUES
                (%(env)s, %(account)s, %(fetched_at)s, %(real_positions)s,
                 %(min_date)s, %(started_at)s, %(finished_at)s, %(outcome)s, %(row_count)s)
            RETURNING fetch_id
            """,
            {
                "env": environment,
                "account": account_id,
                "fetched_at": portfolio_fetched_at,
                "real_positions": portfolio_real_positions,
                "min_date": trade_history.min_date if trade_history is not None else None,
                "started_at": trade_history.started_at if trade_history is not None else None,
                "finished_at": trade_history.finished_at if trade_history is not None else None,
                "outcome": outcome,
                "row_count": len(rows) if rows is not None else None,
            },
        ).fetchone()
    if row is None:
        raise RuntimeError("trade_history_fetches insert returned no fetch_id")
    return int(row["fetch_id"])


def _retain_unscoped(
    conn: psycopg.Connection[Any],
    real_positions: Sequence[BrokerPosition],
    history_events: Sequence[TradeEvent],
    fetch_id: int,
) -> None:
    """Retain every payload of a batch not written as an ``unscoped`` observation (spec Design 2).

    An identical repeat for the same ``(position_id, source, event_kind, executed_at)`` is not written again;
    "identical" is on the compared fields, so a sync payload whose only change is its live valuation repeats.
    """
    observations: list[tuple[int, EventSource, EventKind, datetime | None, dict[str, Any]]] = [
        (bp.position_id, "etoro_sync", "open", None, bp.raw_payload)
        for bp in real_positions
        if bp.position_id is not None
    ]
    observations.extend(
        (e.position_id, e.source, e.event_kind, e.executed_at if e.event_kind == "close" else None, e.raw_payload)
        for e in history_events
    )
    for position_id, source, event_kind, executed_at, raw in observations:
        observed = normalise_payload(source, event_kind, raw)
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            latest = cur.execute(
                """
                SELECT observed_normalised
                FROM trade_event_observations
                WHERE kind = 'unscoped' AND position_id = %(pid)s AND source = %(source)s
                  AND event_kind = %(kind)s AND executed_at IS NOT DISTINCT FROM %(executed_at)s
                ORDER BY observation_id DESC
                LIMIT 1
                """,
                {"pid": position_id, "source": source, "kind": event_kind, "executed_at": executed_at},
            ).fetchone()
        if latest is not None and latest["observed_normalised"] == observed:
            continue
        conn.execute(
            """
            INSERT INTO trade_event_observations
                (event_id, position_id, event_kind, executed_at, fetch_id, kind, observed_at, source,
                 stored_normalised, observed_normalised, observed_raw)
            VALUES
                (NULL, %(pid)s, %(kind)s, %(executed_at)s, %(fetch_id)s, 'unscoped', now(), %(source)s,
                 NULL, %(observed)s, %(raw)s)
            """,
            {
                "pid": position_id,
                "kind": event_kind,
                "executed_at": executed_at,
                "fetch_id": fetch_id,
                "source": source,
                "observed": Jsonb(observed),
                "raw": Jsonb(raw),
            },
        )
