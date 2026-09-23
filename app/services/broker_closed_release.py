"""Whole one-shot broker close: may an absent owned position release its ownership? (#2965)

Spec: ``docs/proposals/execution/2026-09-23-broker-closed-ownership-release-rev3.md``.

A mandated SL/TP (#3284) closes the WHOLE position in one execution, and until now that
left ownership ``active`` for ever, so the capital reader raised
``engine_capital_ownership_unwitnessed`` on every resolution.  This module answers one
question: does the stored broker evidence prove that exactly that happened?

⚠ It accepts ONLY the one-shot whole close.  Any partially-altered position stays wedged
for a human.  Two earlier designs tried to prove a multi-slice close whole by arithmetic
and were refused at checkpoint 1; the observed booking (attended 2026-09-23: a partial
slice is written under a NEW position id sharing the opening ``orderId``) is what makes
"no sibling close row under the entry order" a sound partial detector.

The verdict is a pure function over fetched rows.  Every missing, malformed, non-finite or
mistyped field REFUSES; nothing here raises on bad data.

#2979 half a (``2026-09-23-uncertain-close-witness-release.md``): an ENGINE close whose
outcome we lost no longer blocks the release.  Our close verb is addressed to the position
by path and closes it whole, so once the witness proves that position closed whole
exactly once, the close has nothing left to act on -- whoever's close it was.  The
release then says so (``UNCERTAIN_CLOSE_RELEASE_REASON``) and stamps the op; it never
claims the close was ours.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final, LiteralString

import psycopg
import psycopg.rows

RELEASE_REASON = "broker_closed_externally"
#: The same witness, released while one or more of OUR closes on the position stood
#: uncertain.  Not necessarily external, so not ``RELEASE_REASON``.
UNCERTAIN_CLOSE_RELEASE_REASON = "broker_closed_after_uncertain_close"
#: The two codes meaning "our close verb may have been entered; what the broker did is
#: unknown".  Written by ``strategy_position_manager`` (resume of a ``submitting`` close,
#: and ``BrokerPositionMutationUncertain``).  Named in ``sql/414``'s CHECK as well.
UNCERTAIN_CLOSE_ERROR_CODES: Final = ("crash_before_submission_identity", "broker_close_uncertain")

#: An op that the witness may settle: an unstamped uncertain close whose persisted close
#: response (if any) does not name ANOTHER position.  ``broker_close_uncertain`` also
#: covers "response identity does not match intent", and ``persist_response`` stores that
#: body before the raise, so a response naming another position is a broker divergence
#: and keeps blocking.  Any text other than P's decimal counts as "another" (fails closed).
#: Binds ``%(position_text)s`` and ``%(uncertain_codes)s``; qualifies ``op`` / ``o``.
_UNCERTAIN_ELIGIBLE_SQL: Final[LiteralString] = """
    op.operation_type = 'close'
    AND op.status = 'reconcile_required'
    AND op.broker_close_witnessed_at IS NULL
    AND op.last_error_code = ANY(%(uncertain_codes)s)
    AND o.order_id IS NOT NULL
    AND COALESCE(o.raw_payload_json -> 'orderForClose' ->> 'positionID', %(position_text)s)
        = %(position_text)s
"""


@dataclass(frozen=True)
class WholeCloseEvidence:
    broker_position_id: int
    instrument_id: int
    entry_broker_order_refs: Sequence[Any]
    open_rows: Sequence[Mapping[str, Any]]
    close_rows: Sequence[Mapping[str, Any]]
    sibling_close_count: int
    blocking_operation_count: int
    entry_reconciliation_states: Sequence[Any]
    #: #2979: the exact uncertain close ops the witness would settle (see module doc).
    uncertain_close_operation_ids: Sequence[int] = ()


@dataclass(frozen=True)
class WholeCloseVerdict:
    release: bool
    reason_code: str
    released_at: datetime | None = None


def _refuse(reason_code: str) -> WholeCloseVerdict:
    return WholeCloseVerdict(False, reason_code)


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    # ⚠ ``"²".isdigit()`` is True and ``int("²")`` raises, and so does a digit string past
    # Python's int-conversion limit.  "Nothing here raises on bad data" (#2979 r1-30/r2-9).
    if isinstance(value, str) and value.isascii() and value.isdigit():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _positive(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() and number > 0 else None


def _finite(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _instant(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _raw(row: Mapping[str, Any]) -> Mapping[str, Any] | None:
    raw = row.get("raw_payload")
    return raw if isinstance(raw, Mapping) else None


def evaluate_whole_close(evidence: WholeCloseEvidence, *, observed_at: datetime) -> WholeCloseVerdict:
    """Release only when the broker's own records prove one close of the whole position."""
    position_id = evidence.broker_position_id
    instrument_id = evidence.instrument_id

    if evidence.blocking_operation_count != 0:
        return _refuse("operation_unresolved")
    if len(evidence.entry_broker_order_refs) != 1:
        return _refuse("entry_reference_not_unique")
    if list(evidence.entry_reconciliation_states) != ["resolved"]:
        return _refuse("entry_not_resolved")
    entry_ref = _int(evidence.entry_broker_order_refs[0])
    if entry_ref is None or entry_ref <= 0:
        return _refuse("entry_reference_invalid")

    # Open witness: the live snapshot's first sighting of the whole, unaltered position.
    if len(evidence.open_rows) != 1:
        return _refuse("open_witness_not_unique")
    opened = evidence.open_rows[0]
    open_raw = _raw(opened)
    open_units = _positive(opened.get("units"))
    open_at = _instant(opened.get("executed_at"))
    if open_raw is None or open_units is None or open_at is None:
        return _refuse("open_witness_malformed")
    if opened.get("source") != "etoro_sync":
        return _refuse("open_witness_not_snapshot")
    if (
        _int(open_raw.get("positionID")) != position_id
        or _int(open_raw.get("orderID")) != entry_ref
        or _int(open_raw.get("instrumentID")) != instrument_id
        or _int(opened.get("etoro_instrument_id")) != instrument_id
        or _int(opened.get("instrument_id")) != instrument_id
    ):
        return _refuse("open_witness_identity_mismatch")
    if open_raw.get("isBuy") is not True or _int(open_raw.get("leverage")) != 1:
        return _refuse("open_witness_not_unleveraged_long")
    if open_raw.get("isPartiallyAltered") is not False:
        return _refuse("open_witness_partially_altered")
    if _positive(open_raw.get("initialUnits")) != open_units or _positive(open_raw.get("units")) != open_units:
        return _refuse("open_witness_not_whole")
    open_dollars = _positive(open_raw.get("initialAmountInDollars"))
    if open_dollars is None:
        return _refuse("open_witness_malformed")

    # Close witness: exactly one broker-history close of the whole size, no partial ever.
    if evidence.sibling_close_count != 0:
        return _refuse("partial_close_slice_present")
    if len(evidence.close_rows) != 1:
        return _refuse("close_witness_not_unique")
    closed = evidence.close_rows[0]
    close_raw = _raw(closed)
    close_units = _positive(closed.get("units"))
    closed_at = _instant(closed.get("executed_at"))
    if close_raw is None or close_units is None or closed_at is None:
        return _refuse("close_witness_malformed")
    if closed.get("source") != "etoro_history" or closed.get("side") != "sell":
        return _refuse("close_witness_not_history_sell")
    if (
        _int(closed.get("order_id")) != entry_ref
        or _int(close_raw.get("orderId")) != entry_ref
        or _int(close_raw.get("positionId")) != position_id
        or _int(close_raw.get("instrumentId")) != instrument_id
        or _int(closed.get("etoro_instrument_id")) != instrument_id
        or _int(closed.get("instrument_id")) != instrument_id
        or _instant(close_raw.get("openTimestamp")) != open_at
    ):
        return _refuse("close_witness_identity_mismatch")
    if close_raw.get("isBuy") is not True or _int(close_raw.get("leverage")) != 1:
        return _refuse("close_witness_not_unleveraged_long")
    if close_units != open_units or _positive(close_raw.get("units")) != close_units:
        return _refuse("close_not_whole_units")
    investment = _positive(close_raw.get("investment"))
    if investment is None or investment != _positive(close_raw.get("initialInvestment")) or investment != open_dollars:
        return _refuse("close_not_whole_investment")
    if not open_at <= closed_at <= observed_at:
        return _refuse("close_witness_out_of_time")
    if _finite(closed.get("realized_pnl_usd")) is None:
        return _refuse("close_witness_unpriced")
    reason = UNCERTAIN_CLOSE_RELEASE_REASON if evidence.uncertain_close_operation_ids else RELEASE_REASON
    return WholeCloseVerdict(True, reason, closed_at)


_EVENT_COLUMNS = (
    "position_id,event_kind,side,source,units,executed_at,order_id,instrument_id,"
    "etoro_instrument_id,realized_pnl_usd,raw_payload"
)


def load_whole_close_evidence(
    conn: psycopg.Connection[Any],
    *,
    ownership_id: int,
    strategy_trade_id: int,
    broker_position_id: int,
    instrument_id: int,
) -> WholeCloseEvidence:
    """Fetch every row the verdict reads.  Reads only; no casts that could raise."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        entry_rows = cur.execute(
            """
            SELECT o.broker_order_ref, state.state
            FROM strategy_trade_orders link
            JOIN orders o ON o.order_id=link.order_id
            LEFT JOIN strategy_order_reconciliation_state state ON state.order_id=o.order_id
            WHERE link.strategy_trade_id=%s AND link.purpose='entry'
            """,
            (strategy_trade_id,),
        ).fetchall()
        events = cur.execute(
            f"SELECT {_EVENT_COLUMNS} FROM trade_events WHERE position_id=%s",
            (broker_position_id,),
        ).fetchall()
        # #2979: an uncertain close the witness may settle is split out BY ID; every other
        # in-flight op or unstamped `reconcile_required` close still blocks.
        ops = cur.execute(
            """
            SELECT op.position_operation_id AS id,
                   (op.status IN ('intent_persisted','submitting','submitted')
                    OR (op.status='reconcile_required' AND op.operation_type='close'
                        AND op.broker_close_witnessed_at IS NULL)) AS unresolved,
                   COALESCE(("""
            + _UNCERTAIN_ELIGIBLE_SQL
            + """), false) AS eligible
            FROM strategy_position_operations op
            -- LEFT: an edit op may carry no order, and must still count as blocking.
            LEFT JOIN orders o ON o.order_id = op.order_id
            WHERE op.ownership_id=%(ownership_id)s
            """,
            {
                "ownership_id": ownership_id,
                "position_text": str(broker_position_id),
                "uncertain_codes": list(UNCERTAIN_CLOSE_ERROR_CODES),
            },
        ).fetchall()
        siblings = 0
        refs = [row["broker_order_ref"] for row in entry_rows]
        entry_ref = _int(refs[0]) if len(refs) == 1 else None
        if entry_ref is not None:
            sibling_row = cur.execute(
                """
                SELECT count(*) AS n FROM trade_events
                WHERE event_kind='close' AND position_id<>%s
                  AND (order_id=%s OR raw_payload->>'orderId'=%s)
                  -- ⚠ One entry order can yield several OWNED executions (sql/282);
                  -- each is its own position with its own whole close.  A partial
                  -- slice is a NEW id nobody owns.
                  AND position_id NOT IN (
                      SELECT broker_position_id FROM strategy_position_ownership
                      WHERE strategy_trade_id=%s
                  )
                """,
                (broker_position_id, entry_ref, str(entry_ref), strategy_trade_id),
            ).fetchone()
            siblings = int(sibling_row["n"]) if sibling_row is not None else 0
    return WholeCloseEvidence(
        broker_position_id=broker_position_id,
        instrument_id=instrument_id,
        entry_broker_order_refs=refs,
        open_rows=[row for row in events if row["event_kind"] == "open"],
        close_rows=[row for row in events if row["event_kind"] == "close"],
        sibling_close_count=siblings,
        blocking_operation_count=sum(1 for row in ops if row["unresolved"] and not row["eligible"]),
        entry_reconciliation_states=[row["state"] for row in entry_rows],
        uncertain_close_operation_ids=sorted(int(row["id"]) for row in ops if row["eligible"]),
    )


def stamp_uncertain_closes(
    conn: psycopg.Connection[Any],
    *,
    ownership_id: int,
    broker_position_id: int,
    operation_ids: Sequence[int] | None,
) -> int:
    """Stamp ``broker_close_witnessed_at`` on eligible uncertain closes; return the count.

    Call ONLY inside the transaction that releases ``ownership_id`` (``sql/414``).
    ``operation_ids`` restricts the stamp to the ids a verdict evaluated, so the caller can
    compare the count and refuse a set that changed; ``None`` stamps every eligible row
    (``_finish_close``, which holds no evidence set).  The eligibility predicate is
    re-applied either way.
    """
    params: dict[str, Any] = {
        "ownership_id": ownership_id,
        "position_text": str(broker_position_id),
        "uncertain_codes": list(UNCERTAIN_CLOSE_ERROR_CODES),
        "ids": None if operation_ids is None else list(operation_ids),
    }
    stamped = conn.execute(
        """
        UPDATE strategy_position_operations op
        SET broker_close_witnessed_at = now(), updated_at = now()
        FROM orders o
        WHERE o.order_id = op.order_id
          AND op.ownership_id = %(ownership_id)s
          AND (%(ids)s::bigint[] IS NULL OR op.position_operation_id = ANY(%(ids)s::bigint[]))
          AND """
        + _UNCERTAIN_ELIGIBLE_SQL,
        params,
    )
    return stamped.rowcount
