"""Record what the provider NOW says about an intraday bar we ALREADY HOLD.

The harvester has always fetched this evidence and always discarded it:
``strategy_intraday_harvest._fetch_count`` re-requests bars reaching behind the
durable watermark, ``_completed_rth_bars`` keeps them, and the ``new`` filter then
drops every bar at or below the watermark unread.  Nothing has ever compared a
delivered bar against the row already stored.

This module is that comparison.  It writes one row per provider call
(``strategy_intraday_reobservations``) and one row per compared bar
(``strategy_intraday_reobserved_bars``), so that a BRACKET — an observation that
agreed and a later one that diverged — exists as two real rows carrying their own
time bounds.

⚠⚠ Equality is decided at STORAGE precision, by PostgreSQL, not by Python.  The
provider builds ``Decimal(str(raw))`` while ``sql/277`` stores ``double precision``,
so ``Decimal == float`` is evaluated exactly and is False for any price not exactly
representable in binary.  ``float(Decimal)`` would be a *second* conversion path
rather than the writer's own, and it silently maps out-of-range values to ``inf``
where PostgreSQL raises.  So each delivered value is cast by the database with
``::numeric::double precision`` — the identical conversion ``store_intraday_bars``
triggers by passing a ``Decimal`` into a ``double precision`` column.

⚠ A recorded divergence is NOT a re-basing, and the claim this evidence licenses is
"no price change was detected between these two observations" — never "the provider
did not rewrite in that interval".

Refs #2840. Schema: ``sql/404_intraday_reobservation_comparison.sql``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, Protocol

import psycopg

from app.providers.market_data import IntradayBar as ProviderBar

#: Outcomes that record a member reached by the round robin for which no provider
#: call was made at all, so the two time bounds collapse to one instant.
UNCALLED_OUTCOMES: Final = frozenset({"unresolved_member", "not_attempted"})

#: Outcomes that record a call which happened but yielded no comparison.
FAILED_CALL_OUTCOMES: Final = frozenset({"fetch_failed", "invalid_response", "comparison_skipped"})


class ReobservationMember(Protocol):
    """The member fields this module needs.

    A structural type rather than an import of ``HarvestMember``: the harvester
    imports this module, so importing back would be circular.

    Declared as read-only properties, not attributes: a mutable protocol attribute is
    invariant, so ``HarvestMember.timeframe`` (the ``Timeframe`` literal union) would
    not satisfy a plain ``timeframe: str``.
    """

    @property
    def universe_version(self) -> str: ...

    @property
    def ordinal(self) -> int: ...

    @property
    def timeframe(self) -> str: ...

    @property
    def symbol(self) -> str: ...

    @property
    def instrument_id(self) -> int | None: ...


@dataclass(frozen=True)
class ReobservationReport:
    outcome: str
    overlap_bars: int = 0
    compared_bars: int = 0
    agreed_bars: int = 0
    diverged_bars: int = 0
    missing_baseline_bars: int = 0
    invalid_baseline_bars: int = 0


@dataclass(frozen=True)
class _Values:
    """One OHLCV tuple already at storage precision."""

    open: float
    high: float
    low: float
    close: float
    volume: float | None

    def price_differs(self, other: _Values) -> bool:
        return self.open != other.open or self.high != other.high or self.low != other.low or self.close != other.close

    def volume_differs(self, other: _Values) -> bool:
        return self.volume != other.volume


@dataclass(frozen=True)
class _Baseline:
    values: _Values
    source: str
    reobservation_id: int | None
    captured_at: datetime | None


_INSERT_REOBSERVATION: Final = """
    INSERT INTO strategy_intraday_reobservations (
        universe_version, ordinal, timeframe, symbol, instrument_id,
        requested_at, received_at, outcome,
        overlap_bars, compared_bars, agreed_bars, diverged_bars,
        missing_baseline_bars, invalid_baseline_bars, failure_class
    ) VALUES (
        %(universe_version)s, %(ordinal)s, %(timeframe)s, %(symbol)s, %(instrument_id)s,
        %(requested_at)s, %(received_at)s, %(outcome)s,
        %(overlap_bars)s, %(compared_bars)s, %(agreed_bars)s, %(diverged_bars)s,
        %(missing_baseline_bars)s, %(invalid_baseline_bars)s, %(failure_class)s
    )
    RETURNING reobservation_id
"""

_INSERT_REOBSERVED_BAR: Final = """
    INSERT INTO strategy_intraday_reobserved_bars (
        reobservation_id, bar_time, timeframe, instrument_id,
        observed_open, observed_high, observed_low, observed_close, observed_volume,
        baseline_open, baseline_high, baseline_low, baseline_close, baseline_volume,
        baseline_source, baseline_reobservation_id, baseline_captured_at,
        price_changed, volume_changed
    ) VALUES (
        %(reobservation_id)s, %(bar_time)s, %(timeframe)s, %(instrument_id)s,
        %(observed_open)s, %(observed_high)s, %(observed_low)s, %(observed_close)s, %(observed_volume)s,
        %(baseline_open)s, %(baseline_high)s, %(baseline_low)s, %(baseline_close)s, %(baseline_volume)s,
        %(baseline_source)s, %(baseline_reobservation_id)s, %(baseline_captured_at)s,
        %(price_changed)s, %(volume_changed)s
    )
"""

# ⚠ ``r.requested_at < %(before)s`` excludes a concurrently-written row whose bounds
# postdate this call.  It cannot exclude an EARLIER call that has not committed yet —
# see the module note in the spec (§4.3): the harvest job holds a per-process advisory
# lock, so overlapping fires need two daemons, and the residual effect is a duplicate
# transition rather than a corrupted one.
_SELECT_PRIOR_OBSERVATIONS: Final = """
    SELECT DISTINCT ON (b.bar_time)
           b.bar_time, b.observed_open, b.observed_high, b.observed_low,
           b.observed_close, b.observed_volume, b.reobservation_id
    FROM strategy_intraday_reobserved_bars AS b
    JOIN strategy_intraday_reobservations AS r USING (reobservation_id)
    WHERE b.timeframe = %(timeframe)s
      AND b.instrument_id = %(instrument_id)s
      AND b.bar_time = ANY(%(bar_times)s)
      AND r.requested_at < %(before)s
    ORDER BY b.bar_time, r.requested_at DESC, b.reobservation_id DESC
"""

_SELECT_STORED_BARS: Final = """
    SELECT bar_time, open, high, low, close, volume, captured_at
    FROM strategy_intraday_bars
    WHERE timeframe = %(timeframe)s
      AND instrument_id = %(instrument_id)s
      AND bar_time = ANY(%(bar_times)s)
"""

# One round trip per compared bar.  Deliberately not batched: an out-of-range value
# must fail THAT bar (counted as an invalid baseline) rather than the whole call.
_CAST_TO_STORAGE: Final = """
    SELECT %(open)s::numeric::double precision,
           %(high)s::numeric::double precision,
           %(low)s::numeric::double precision,
           %(close)s::numeric::double precision,
           %(volume)s::numeric::double precision
"""


def _member_params(member: ReobservationMember) -> dict[str, Any]:
    return {
        "universe_version": member.universe_version,
        "ordinal": member.ordinal,
        "timeframe": member.timeframe,
        "symbol": member.symbol,
        "instrument_id": member.instrument_id,
    }


def _insert_reobservation(
    conn: psycopg.Connection[Any],
    *,
    member: ReobservationMember,
    requested_at: datetime,
    received_at: datetime,
    report: ReobservationReport,
    failure_class: str | None,
) -> int:
    row = conn.execute(
        _INSERT_REOBSERVATION,
        {
            **_member_params(member),
            "requested_at": requested_at,
            "received_at": received_at,
            "outcome": report.outcome,
            "overlap_bars": report.overlap_bars,
            "compared_bars": report.compared_bars,
            "agreed_bars": report.agreed_bars,
            "diverged_bars": report.diverged_bars,
            "missing_baseline_bars": report.missing_baseline_bars,
            "invalid_baseline_bars": report.invalid_baseline_bars,
            "failure_class": failure_class,
        },
    ).fetchone()
    if row is None:  # pragma: no cover — RETURNING on a successful INSERT always yields a row
        raise RuntimeError("reobservation insert returned no id")
    return int(row[0])


def record_uncalled(
    conn: psycopg.Connection[Any],
    *,
    member: ReobservationMember,
    at: datetime,
    outcome: str,
    failure_class: str | None = None,
) -> ReobservationReport:
    """Record a selected member for which no provider call was made.

    ``unresolved_member`` (no instrument id) or ``not_attempted`` (the call was
    never issued — today, a watermark read that raised).  Both bounds are the same
    instant because there is no call to bracket.
    """
    if outcome not in UNCALLED_OUTCOMES:
        raise ValueError(f"{outcome!r} is not an uncalled outcome")
    report = ReobservationReport(outcome=outcome)
    with conn.transaction():
        _insert_reobservation(
            conn,
            member=member,
            requested_at=at,
            received_at=at,
            report=report,
            failure_class=failure_class,
        )
    return report


def record_failed_call(
    conn: psycopg.Connection[Any],
    *,
    member: ReobservationMember,
    requested_at: datetime,
    received_at: datetime,
    outcome: str,
    failure_class: str,
) -> ReobservationReport:
    """Record a call that happened but produced no comparison.

    ``fetch_failed`` (the provider raised), ``invalid_response`` (the response was
    refused by ``_completed_rth_bars``) or ``comparison_skipped`` (the response was
    usable but the capture or the comparison itself raised).  All three keep the real
    request/response bounds, because the call really did occupy that interval.
    """
    if outcome not in FAILED_CALL_OUTCOMES:
        raise ValueError(f"{outcome!r} is not a failed-call outcome")
    report = ReobservationReport(outcome=outcome)
    with conn.transaction():
        _insert_reobservation(
            conn,
            member=member,
            requested_at=requested_at,
            received_at=received_at,
            report=report,
            failure_class=failure_class,
        )
    return report


def _cast_to_storage(conn: psycopg.Connection[Any], bar: ProviderBar) -> _Values | None:
    """Convert a delivered candle to storage precision, or ``None`` if it cannot be.

    The cast is done by PostgreSQL so it is the identical conversion the bar writer
    triggers.  An out-of-range or non-numeric value raises there rather than becoming
    ``inf`` silently, and the caller counts the bar as an invalid baseline.
    """
    volume = None if bar.volume is None else Decimal(bar.volume)
    try:
        # ⚠ The cast runs inside its own transaction block — a SAVEPOINT when the
        # caller already holds a transaction. Catching the error is not enough:
        # PostgreSQL leaves the enclosing transaction ABORTED, so on a non-autocommit
        # connection every later statement (including the outcome insert and the
        # cursor advance) would fail with InFailedSqlTransaction. Scoping the
        # rollback is what makes "count this one bar invalid and carry on" true.
        with conn.transaction():
            row = conn.execute(
                _CAST_TO_STORAGE,
                {
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": volume,
                },
            ).fetchone()
    except psycopg.DataError:
        return None
    if row is None:  # pragma: no cover — a bare SELECT of literals always yields a row
        return None
    open_, high, low, close, vol = row
    values = _Values(float(open_), float(high), float(low), float(close), None if vol is None else float(vol))
    if not _is_storable(values):
        return None
    return values


def _is_storable(values: _Values) -> bool:
    """Refuse anything ``strategy_intraday_reobserved_bars`` would refuse.

    ⚠ This mirrors EVERY value CHECK on that table, not just finiteness: positivity,
    the OHLC shape, and non-negative volume.  A candle that passes here and then fails
    the insert does not cost one bar — the batch insert is one statement inside one
    transaction, so it would roll back every valid comparison in the call and report
    ``comparison_skipped``.  Turning one malformed candle into a lost call is exactly
    the outcome ``invalid_baseline_bars`` exists to prevent, and the eToro normalizer
    permits both shapes (it skips only candles that fail to parse).

    ``strategy_intraday_bars`` CHECKs ``open > 0``, which excludes NaN but NOT
    ``Infinity``, so a stored baseline can in principle be non-finite too.  Both sides
    are validated, not just the delivered one.
    """
    prices = (values.open, values.high, values.low, values.close)
    if any(value != value or value in (float("inf"), float("-inf")) or value <= 0 for value in prices):
        return False
    if values.high < max(values.open, values.close, values.low):
        return False
    if values.low > min(values.open, values.close, values.high):
        return False
    volume = values.volume
    if volume is not None and (volume != volume or volume in (float("inf"), float("-inf")) or volume < 0):
        return False
    return True


def _stored_baselines(
    conn: psycopg.Connection[Any],
    *,
    timeframe: str,
    instrument_id: int,
    bar_times: Sequence[datetime],
) -> tuple[dict[datetime, _Baseline], set[datetime]]:
    """Seed baselines from the bar table, refusing conflicting duplicates.

    ``sql/278`` dropped the bar table's primary key (index size), so duplicate
    protection there is the watermark convention rather than a constraint.  Identical
    duplicates collapse; conflicting ones are refused for that bar rather than
    arbitrated, because picking one would silently choose which history is true.
    The earliest ``captured_at`` is kept: it is the tighter bound on first observation.
    """
    seen: dict[datetime, _Baseline] = {}
    conflicting: set[datetime] = set()
    for bar_time, open_, high, low, close, volume, captured_at in conn.execute(
        _SELECT_STORED_BARS,
        {"timeframe": timeframe, "instrument_id": instrument_id, "bar_times": list(bar_times)},
    ):
        values = _Values(float(open_), float(high), float(low), float(close), None if volume is None else float(volume))
        prior = seen.get(bar_time)
        if prior is not None:
            if prior.values != values:
                conflicting.add(bar_time)
                continue
            captured_at = min(captured_at, prior.captured_at) if prior.captured_at else captured_at
        seen[bar_time] = _Baseline(values, "stored_bar", None, captured_at)
    for bar_time in conflicting:
        seen.pop(bar_time, None)
    return seen, conflicting


def _prior_baselines(
    conn: psycopg.Connection[Any],
    *,
    timeframe: str,
    instrument_id: int,
    bar_times: Sequence[datetime],
    before: datetime,
) -> dict[datetime, _Baseline]:
    return {
        bar_time: _Baseline(
            _Values(float(open_), float(high), float(low), float(close), None if volume is None else float(volume)),
            "prior_reobservation",
            int(reobservation_id),
            None,
        )
        for bar_time, open_, high, low, close, volume, reobservation_id in conn.execute(
            _SELECT_PRIOR_OBSERVATIONS,
            {
                "timeframe": timeframe,
                "instrument_id": instrument_id,
                "bar_times": list(bar_times),
                "before": before,
            },
        )
    }


def record_comparison(
    conn: psycopg.Connection[Any],
    *,
    member: ReobservationMember,
    rth_bars: Sequence[ProviderBar],
    watermark: datetime | None,
    requested_at: datetime,
    received_at: datetime,
) -> ReobservationReport:
    """Compare the delivered bars we already hold against what we hold, and record it.

    The overlap set is the delivered completed-RTH bars with ``bar_time <= watermark``
    (empty when the watermark is NULL, since then we hold nothing to re-observe).
    Every bar in it lands in exactly one of compared / missing-baseline /
    invalid-baseline, which is what makes ``overlap_bars`` a real denominator rather
    than a count of whatever happened to work.
    """
    if member.instrument_id is None:
        raise ValueError("cannot compare for an unresolved member")
    overlap = (
        []
        if watermark is None
        else [bar for bar in rth_bars if bar.timestamp.astimezone(UTC) <= watermark.astimezone(UTC)]
    )
    if not overlap:
        return _record_empty(conn, member=member, requested_at=requested_at, received_at=received_at)

    bar_times = [bar.timestamp.astimezone(UTC) for bar in overlap]
    prior = _prior_baselines(
        conn,
        timeframe=member.timeframe,
        instrument_id=member.instrument_id,
        bar_times=bar_times,
        before=requested_at,
    )
    stored, conflicting = _stored_baselines(
        conn,
        timeframe=member.timeframe,
        instrument_id=member.instrument_id,
        bar_times=[stamp for stamp in bar_times if stamp not in prior],
    )

    rows: list[dict[str, Any]] = []
    missing = invalid = agreed = diverged = 0
    for bar in overlap:
        stamp = bar.timestamp.astimezone(UTC)
        baseline = prior.get(stamp) or stored.get(stamp)
        if baseline is None:
            invalid += 1 if stamp in conflicting else 0
            missing += 0 if stamp in conflicting else 1
            continue
        observed = _cast_to_storage(conn, bar)
        if observed is None or not _is_storable(baseline.values):
            invalid += 1
            continue
        price_changed = observed.price_differs(baseline.values)
        volume_changed = observed.volume_differs(baseline.values)
        diverged += 1 if price_changed else 0
        agreed += 0 if price_changed else 1
        rows.append(
            {
                "bar_time": stamp,
                "timeframe": member.timeframe,
                "instrument_id": member.instrument_id,
                "observed_open": observed.open,
                "observed_high": observed.high,
                "observed_low": observed.low,
                "observed_close": observed.close,
                "observed_volume": observed.volume,
                "baseline_open": baseline.values.open,
                "baseline_high": baseline.values.high,
                "baseline_low": baseline.values.low,
                "baseline_close": baseline.values.close,
                "baseline_volume": baseline.values.volume,
                "baseline_source": baseline.source,
                "baseline_reobservation_id": baseline.reobservation_id,
                "baseline_captured_at": baseline.captured_at,
                "price_changed": price_changed,
                "volume_changed": volume_changed,
            }
        )

    report = ReobservationReport(
        outcome="compared" if rows else "no_baseline",
        overlap_bars=len(overlap),
        compared_bars=len(rows),
        agreed_bars=agreed,
        diverged_bars=diverged,
        missing_baseline_bars=missing,
        invalid_baseline_bars=invalid,
    )
    # One transaction: a retry either hits the replay constraint having written
    # nothing else, or writes the complete record.  Partial counters are unreachable.
    with conn.transaction():
        reobservation_id = _insert_reobservation(
            conn,
            member=member,
            requested_at=requested_at,
            received_at=received_at,
            report=report,
            failure_class=None,
        )
        if rows:
            with conn.cursor() as cur:
                cur.executemany(
                    _INSERT_REOBSERVED_BAR,
                    [{**row, "reobservation_id": reobservation_id} for row in rows],
                )
    return report


def _record_empty(
    conn: psycopg.Connection[Any],
    *,
    member: ReobservationMember,
    requested_at: datetime,
    received_at: datetime,
) -> ReobservationReport:
    report = ReobservationReport(outcome="no_overlap")
    with conn.transaction():
        _insert_reobservation(
            conn,
            member=member,
            requested_at=requested_at,
            received_at=received_at,
            report=report,
            failure_class=None,
        )
    return report


__all__ = [
    "FAILED_CALL_OUTCOMES",
    "UNCALLED_OUTCOMES",
    "ReobservationMember",
    "ReobservationReport",
    "record_comparison",
    "record_failed_call",
    "record_uncalled",
]
