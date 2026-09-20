"""Census for #2840 arm 2 step 1 item 1 — what a daily series composed from the stored
30m RTH bars would actually be, and what it is not.

Read-only. One ``REPEATABLE READ READ ONLY`` transaction, no row written, no job run, no
broker touched, no strategy evaluated on real bars and no outcome opened.

WHY THIS EXISTS
---------------
The panel declaration was refused at Codex checkpoint 1 because the panel is downstream
of a forward INSTRUMENT that does not exist: at the time, S-12's ``AS_TRADED_UNIVERSES``
token admitted only ``survivorship_free`` (that token was removed in #2840 §6 item 3 and
the equivalent refusal is now the undeclared price basis on the scan path), S-4/S-12 are
DAILY rules, and the intraday panel collects 30m/5m/1m. That pass established one claim
and named two it did not:

* ESTABLISHED — a stored bar is never re-based by THIS writer. ``_INSERT_INTRADAY_BAR`` is
  a plain INSERT and ``store_intraday_bars`` RAISES on any bar at or behind the stored
  watermark (``strategy_observation_storage.py:580``).
  ⚠ That is an application convention, not a schema guarantee: ``sql/276`` carries no
  UPDATE/DELETE prohibition and does permit cascading deletion. It also does not make the
  stored value the FIRST-EVER observation — only the first one this writer accepted.
* NOT ESTABLISHED — that a complete nominal daily series exists.
* UNMEASURED — that the 30m bars are aggregable to daily at all.

This script measures the two open ones. It decides nothing, and every claim it supports is
narrower than the question it was pointed at.

WHAT "NOMINAL" WOULD REQUIRE, AND WHY THIS SCRIPT CANNOT CLOSE IT
-----------------------------------------------------------------
``get_intraday_candles`` takes no date anchor, so a first harvest for a member fetches a
large recent window and reaches back well before its own run. If the provider adjusts
intraday history at fetch time the way the daily path does, a bar captured long after it
completed carries the adjustment that stood at CAPTURE.

⚠⚠ THAT PREMISE IS STILL UNVERIFIED — NARROWED, NOT RESOLVED. See
``scripts/probe_2840_intraday_adjustment_basis.py``. Daily and intraday ARE the same endpoint
with the interval as a path slot (``app/providers/implementations/etoro.py:305-341``), so
intraday would have to be adjusted PER INTERVAL to differ — but shared routing is not shared
adjustment, ``market_data.py:751`` is a repair-heuristic comment rather than a provider
contract, and eToro's candle docs are silent on adjustment. No confirmed split inside a
reachable window has been tested. Four claims that must not share a sentence: *this writer never
re-bases a stored row* (established) · *the provider does not re-base intraday history* (NOT
established) · *these backfilled bars ARE re-based* (also NOT established — and the strong form
would license reversing an assumed factor, which could MANUFACTURE gate eligibility) · *a bar
captured before the next open had no opportunity to be re-based* (arithmetic, and the only one
this script measures). The operative policy is the same either way: nominality unverified for
a backfilled bar, so exclude it from an absolute-price gate.

⚠ THAT FOURTH RULE NOW LIVES IN ``app/services/bar_capture_certificate.py`` and this script
IMPORTS it rather than carrying its own copy. #2840's carrier work needs it as an ADMISSION
input, and a rule living in a census script is reachable by a census and by nothing on a
production path. The behaviour is unchanged — ``tests/test_2840_forward_daily_provenance.py``
still exercises it through here, which is the regression evidence for the move.

Only the third is available without a provider experiment, so it is the only one measured.
A US split takes effect at a session OPEN, so a bar whose every constituent was captured
before the NEXT session's open had no intervening open at which anything could be re-based.
``captured_at`` (``sql/276``, ``DEFAULT now()``) is the witness. ⚠ It is the writing
transaction's start time — not the request time, the response time, or the provider's own
generation time — so it bounds the capture instant rather than recording it.

⚠ AND A SPLIT INSIDE THE WINDOW IS A SEPARATE PROBLEM THAT NOMINALITY DOES NOT SOLVE.
Perfectly contemporaneous captures still put pre- and post-split bars on different scales
inside one series, so a forward reader needs a split-boundary policy for ATR, the breakout
lookback and the holding clock whatever this census says.

NOMINALITY AND TIMELINESS ARE TWO DIFFERENT DEADLINES
-----------------------------------------------------
⚠⚠ AN EARLIER VERSION OF THIS SCRIPT CLAIMED THEY WERE THE SAME NUMBER. They are not, and
the difference runs the wrong way: ``strategy_signal_scan`` is scheduled **daily at 06:45
UTC** (read at run time from ``SCHEDULED_JOBS``, not typed), which is BEFORE the 13:30-UTC
next open in EDT. So a catch-up landing at 10:00 UTC is nominal-by-arithmetic and still too
late for the scan that would have used it. Both are reported, separately:

* ``settled_before_next_open`` — no session open intervened, so nothing could have re-based
  the bar. A NOMINALITY claim.
* ``available_before_next_scan`` — the whole session existed before the first scan that
  would load it. An AVAILABILITY claim, and the stricter of the two here.

⚠ The availability deadline used is the scan's own time on the first calendar day after the
session. The real one is LATER: the scan's registry description records that it runs *"one
bar in ARREARS"* and writes the bar before the modal last bar, so a session is not the
decision date until a further session has completed. The conservative deadline is used on
purpose and the direction of the error is stated rather than hidden — the reported
availability is a LOWER bound.

WHAT IS COMPARED, AND WHAT A DIFFERENCE DOES NOT PROVE
------------------------------------------------------
The composed series is compared against ``price_daily`` for the member, and against
``price_daily`` for the member's exchange-33 variant. ⚠ Neither reference is treated as
truth, and a disagreement does NOT identify its cause — capture-vintage adjustment,
quote-convention differences, rounding and bad data all produce one. What the comparison
supports is that the series are not interchangeable, which is enough to forbid MIXING them
inside one window and not enough to name a session boundary.

Refs #2840. Refs #2477. Refs #2437.
"""

from __future__ import annotations

import argparse
import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Final, cast
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row, tuple_row

# ⚠ THE PRODUCER'S OWN SQL, IMPORTED RATHER THAN MIRRORED (Codex checkpoint 2, P2).
# An earlier revision of this file COPIED the CASE expression. The copy type-checked, ran,
# and agreed with the original — and the drift test beside it compared only the profile
# VOCABULARY, so a change to which exchange maps to which profile would have left the census
# admitting bars the canonical resolver refuses, with every gate green. That is the "two
# texts" shape recorded in the prevention log, committed in the same diff that cites it.
# Importing a private name across the boundary is the lesser cost: a script sits above both
# layers, and there is exactly one derivation.
from app.api.instruments import _SESSION_PROFILE_SQL
from app.config import settings
from app.services.bar_capture_certificate import (
    CAPTURE_CERTIFICATE_VERSION,
    CERTIFIED_BUCKET,
    IMPOSSIBLE_BUCKET,
    NYSE_SESSION_PROFILES,
    capture_certificate,
    capture_semantics_cutover,
    next_session_open_utc,
    nominality_bucket,
)
from app.services.indicator_series import BarSeries
from app.services.market_calendar import us_market_status
from app.services.strategies import s4_volatility_compression_breakout as s4
from app.services.strategies.s12_cheapest_band_price_gated_breakout import PRICE_FLOOR
from app.services.strategy_observation_storage import INTRADAY_TIERS, Timeframe
from app.services.technical_analysis import OHLCVRow
from app.workers.scheduler import SCHEDULED_JOBS

_NY: Final = ZoneInfo("America/New_York")

#: The tier a daily composition would read. 30m is the only tier whose retention (24
#: months) matches the declared forward horizon; 5m expires at 12 months and 1m at 30 days
#: (``INTRADAY_TIERS``). ⚠ That bounds RETROSPECTIVE recomputation from raw bars, not every
#: possible forward design — a derived daily row could be persisted before the raw tier
#: expires.
TIMEFRAME: Final = "30m"

#: Session geometry. ⚠ DUPLICATED FROM ``strategy_intraday_harvest._session_bounds``, which
#: is private. The harvester opens at 09:30 ET and closes at 13:00 on a half day and 16:00
#: otherwise; ``session_geometry_matches_the_harvester`` in the test file pins these against
#: that function's own output rather than leaving the copy unchecked.
_SESSION_OPEN: Final = time(9, 30)
_HALF_DAY_CLOSE: Final = time(13)
_FULL_DAY_CLOSE: Final = time(16)


def session_close(day: date) -> time | None:
    status = us_market_status(day)
    if status == "closed":
        return None
    return _HALF_DAY_CLOSE if status == "half_day" else _FULL_DAY_CLOSE


def expected_bars(day: date, *, timeframe: str = TIMEFRAME) -> int:
    """Completed RTH bars a full harvest would hold for ``day``; 0 when closed.

    ⚠ THIS IS AN ADMISSION RULE, NOT A NECESSITY. A 30m window in which nothing traded can
    leave the session's daily OHLC fully determined by the other twelve, so requiring the
    full set refuses some sessions whose daily bar would have been correct. The rule is
    conservative on purpose — the census cannot tell the two cases apart, and the field a
    missing last bar moves is the CLOSE, which is the one input S-12's absolute gate reads.
    """
    close = session_close(day)
    if close is None:
        return 0
    minutes_per_bar = INTRADAY_TIERS[timeframe].minutes_per_bar  # type: ignore[index]
    open_minutes = _SESSION_OPEN.hour * 60 + _SESSION_OPEN.minute
    close_minutes = close.hour * 60 + close.minute
    return (close_minutes - open_minutes) // minutes_per_bar


def session_slots(day: date, *, timeframe: str = TIMEFRAME) -> tuple[datetime, ...]:
    """Every completed-bar start instant a full harvest would hold for ``day``."""
    count = expected_bars(day, timeframe=timeframe)
    if not count:
        return ()
    minutes = INTRADAY_TIERS[timeframe].minutes_per_bar  # type: ignore[index]
    opened = datetime.combine(day, _SESSION_OPEN, tzinfo=_NY).astimezone(UTC)
    return tuple(opened + timedelta(minutes=minutes * index) for index in range(count))


def session_close_utc(day: date) -> datetime | None:
    close = session_close(day)
    if close is None:
        return None
    return datetime.combine(day, close, tzinfo=_NY).astimezone(UTC)


#: The job whose deadline decides whether a composed bar arrived in time.
SCAN_JOB_NAME: Final = "strategy_signal_scan"


def scan_deadline_utc(day: date) -> datetime:
    """The first ``strategy_signal_scan`` firing that would load ``day``'s bar.

    ⚠ READ FROM ``SCHEDULED_JOBS``, NEVER TYPED. A grep for ``06:45`` could not tell whether
    the census follows the registry or merely agrees with it today, and the cadence is the
    operator's to change.
    """
    specs = [spec for spec in SCHEDULED_JOBS if spec.name == SCAN_JOB_NAME]
    if len(specs) != 1:
        raise RuntimeError(f"expected exactly one {SCAN_JOB_NAME} registry row, found {len(specs)}")
    cadence = specs[0].cadence
    if cadence.kind != "daily":
        raise RuntimeError(f"{SCAN_JOB_NAME} cadence is {cadence.kind!r}; this deadline assumes a daily firing")
    return datetime.combine(
        day + timedelta(days=1),
        time(cadence.hour, cadence.minute),
        tzinfo=UTC,
    )


def first_evaluable_series_length(*, limit: int = 200) -> int:
    """Bars S-4 needs before ONE bar returns anything but ``not_evaluable``.

    ⚠ COMPUTED, NEVER TYPED, AND NOT EQUAL TO ``WARMUP_BARS``. ``WARMUP_BARS = 113`` is the
    first evaluable INDEX, so 114 bars reach it — and the evaluator additionally refuses the
    final bar, which has no successor to fill on. The measured answer is therefore larger
    than the constant, and a census that subtracted the constant would understate the wait.
    Probed here on a synthetic series so the figure moves if either rule does.
    """
    for length in range(1, limit + 1):
        dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(length))
        rows: tuple[OHLCVRow, ...] = tuple(
            OHLCVRow(
                open=Decimal(10),
                high=Decimal(11 + index % 3),
                low=Decimal(9),
                close=Decimal(10 + index % 5),
                volume=None,
            )
            for index in range(length)
        )
        signals = s4.s4_signals(
            BarSeries(dates=dates, rows=rows),
            universe="survivorship_free",
            masked_reason="quarantined_bar",
        )
        if any(signal.verdict != "not_evaluable" for signal in signals):
            return length
    raise RuntimeError(f"no evaluable S-4 verdict inside {limit} bars; the probe's premise is gone")


@dataclass(frozen=True)
class ComposedDay:
    """One composed daily bar, or the refusal that stands in place of one.

    ⚠ ``bar`` is ``None`` unless the session's bars occupy EXACTLY the expected slot set.
    A count check alone admits a duplicated slot and a whole series shifted off the grid,
    both of which compose a plausible wrong daily bar rather than no bar.

    ⚠ OMISSION IS THE CONSERVATIVE TREATMENT, NOT A COMPELLED ONE. ``BarSeries`` allows
    calendar gaps and never interpolates (``indicator_series.py:100``), so a gap is legal —
    but dropping a session in which trading DID happen is not the same object as a holiday:
    it shifts ATR's recursion, the breakout lookback and the holding clock by one position.
    That cost is named rather than assumed away.
    """

    day: date
    observed: int
    expected: int
    off_grid: int
    non_finite: int
    bar: tuple[float, float, float, float, float | None] | None
    settled_before_next_open: bool
    available_before_next_scan: bool
    last_captured_at: datetime | None

    @property
    def status(self) -> str:
        if self.expected == 0:
            return "not_a_session"
        if self.observed == 0:
            return "absent"
        if self.bar is None:
            return "partial"
        return "complete"

    @property
    def composable_and_settled(self) -> bool:
        """Composable AND nominal-by-arithmetic AND in time — the intersection of all three.

        ⚠ The availability conjunct is included because it is the STRICTER deadline here (the
        scan fires at 06:45 UTC, the next open is 13:30 UTC in EDT), so dropping it would
        report a number no forward instrument could use.
        """
        return self.bar is not None and self.settled_before_next_open and self.available_before_next_scan


def compose_day(
    day: date,
    rows: Sequence[Mapping[str, Any]],
    *,
    timeframe: str = TIMEFRAME,
) -> ComposedDay:
    """Compose one session's bars into a daily OHLCV, or refuse a session off the grid.

    ⚠ VOLUME IS SUMMED AND IS ``None`` IF ANY CONSTITUENT IS ``None``. The schema permits a
    NULL volume (``sql/276``), so a missing one propagates rather than being read as zero —
    a partial sum presented as a session volume is the same class of error as a partial OHLC.
    """
    slots = session_slots(day, timeframe=timeframe)
    wanted = set(slots)
    seen: dict[datetime, Mapping[str, Any]] = {}
    off_grid = 0
    for row in rows:
        stamp = row["bar_time"].astimezone(UTC)
        if stamp in wanted:
            seen[stamp] = row
        else:
            off_grid += 1
    # ⚠ NON-FINITE PRICES ARE REFUSED, NOT COMPOSED. ``docs/review-prevention-log.md``
    # records that PostgreSQL orders ``NaN`` ABOVE every other value, so ``open > 0`` and
    # ``high >= GREATEST(...)`` are both TRUE for it and the stored row is reachable — and
    # that "the Python mirror must refuse it too". A ``nan`` close would otherwise compose a
    # complete-looking bar and compare as clearing the price gate.
    non_finite = sum(
        1
        for row in seen.values()
        if not all(math.isfinite(float(row[field])) for field in ("open", "high", "low", "close"))
    )
    captures = [row["captured_at"] for row in rows]
    last_capture = max(captures).astimezone(UTC) if captures else None
    next_open = next_session_open_utc(day)
    settled = last_capture is not None and next_open is not None and last_capture < next_open
    available = last_capture is not None and last_capture < scan_deadline_utc(day)
    composed: tuple[float, float, float, float, float | None] | None = None
    # ⚠ ``off_grid == 0`` is part of the admission rule, not a diagnostic. Without it a
    # session holding every expected slot PLUS an extra timestamp composes, because the extra
    # row never enters ``seen``. That is reachable: the writer's per-day cap is 13 for the 30m
    # tier while a half day expects only 7.
    if slots and not off_grid and not non_finite and seen.keys() == wanted:
        ordered = [seen[slot] for slot in slots]
        volumes = [row["volume"] for row in ordered]
        composed = (
            float(ordered[0]["open"]),
            max(float(row["high"]) for row in ordered),
            min(float(row["low"]) for row in ordered),
            float(ordered[-1]["close"]),
            None if any(value is None for value in volumes) else float(sum(volumes)),
        )
    return ComposedDay(
        day=day,
        observed=len(rows),
        expected=len(slots),
        off_grid=off_grid,
        non_finite=non_finite,
        bar=composed,
        settled_before_next_open=settled,
        available_before_next_scan=available,
        last_captured_at=last_capture,
    )


def absence_attribution(
    slots: Iterable[datetime],
    thirty: Iterable[datetime],
    five_slot_starts: Iterable[datetime],
) -> dict[str, int]:
    """Split missing 30m slots by whether the 5m tier holds anything in the same window.

    ⚠ THIS CANNOT EXONERATE THE COLLECTOR AND IS NOT READ AS DOING SO. Both tiers are
    fetched by the same job, so a shared outage leaves NO discordant witness — which is
    exactly what an all-zero third column looks like. A 5m bar with no 30m bar is evidence
    of a resolution-specific loss; the absence of such cases is evidence of nothing.

    ⚠ ``missing_both`` merges "observed and empty" with "never observed": the 5m tier has
    its own membership dates, its own 12-month retention and a much shorter reach per
    1,000-bar request, so its silence over an old session is expected rather than
    informative.
    """
    slot_set = list(slots)
    have_thirty = set(thirty)
    have_five = set(five_slot_starts)
    out = {"slots": len(slot_set), "with_30m": 0, "missing_30m_5m_present": 0, "missing_both": 0}
    for slot in slot_set:
        if slot in have_thirty:
            out["with_30m"] += 1
        elif slot in have_five:
            out["missing_30m_5m_present"] += 1
        else:
            out["missing_both"] += 1
    return out


def five_minute_slot(stamp: datetime, *, timeframe: str = TIMEFRAME) -> datetime:
    """The 30m slot a finer bar falls in, bucketed in Python rather than in SQL.

    ⚠ ``date_trunc``/``extract`` on a ``timestamptz`` resolve against the SESSION timezone,
    so the same expression produces a different grid under a non-UTC ``TimeZone`` setting.
    Bucketing here removes that dependency instead of documenting it.
    """
    minutes = INTRADAY_TIERS[timeframe].minutes_per_bar  # type: ignore[index]
    utc = stamp.astimezone(UTC)
    floored = utc.replace(minute=0, second=0, microsecond=0)
    return floored + timedelta(minutes=minutes * ((utc.minute) // minutes))


def catchup_capture_days(rows: Iterable[Mapping[str, Any]]) -> list[tuple[date, int, date, date]]:
    """Capture days that wrote bars for MORE THAN ONE session, largest first.

    ⚠ A GROUPING, NOT A CAUSE. Several requests or runs inside one day produce the same
    shape, so this locates when multi-session writes happened and does not establish that
    any particular one was a backfill or an outage repair. The job-run section is the
    independent witness for that.
    """
    by_capture_day: dict[date, list[date]] = defaultdict(list)
    for row in rows:
        capture_day = row["captured_at"].astimezone(_NY).date()
        by_capture_day[capture_day].append(row["bar_time"].astimezone(_NY).date())
    out = [
        (capture_day, len(bar_days), min(bar_days), max(bar_days))
        for capture_day, bar_days in by_capture_day.items()
        if min(bar_days) != max(bar_days)
    ]
    return sorted(out, key=lambda item: item[1], reverse=True)


def prior_close_open_matches(rows: Sequence[Mapping[str, Any]]) -> tuple[int, int]:
    """``(matches, adjacent_session_pairs)`` for ``open(D) == close(D-1)``.

    ⚠ NOT A PROOF OF A NON-RTH OPEN. A real opening print can equal the previous close, so a
    high rate motivates a question rather than answering one. Only CALENDAR-ADJACENT sessions
    are paired, and a pair with a NULL on either side is excluded from both terms.
    """
    ordered = sorted(rows, key=lambda row: row["price_date"])
    matches = 0
    pairs = 0
    for previous, current in zip(ordered, ordered[1:], strict=False):
        cursor = previous["price_date"] + timedelta(days=1)
        while cursor < current["price_date"] and not expected_bars(cursor):
            cursor += timedelta(days=1)
        if cursor != current["price_date"]:
            continue
        if previous["close"] is None or current["open"] is None:
            continue
        pairs += 1
        if abs(float(current["open"]) - float(previous["close"])) < 5e-7:
            matches += 1
    return matches, pairs


def range_escapes(
    composed: tuple[float, float, float, float, float | None],
    reference: Mapping[str, Any],
) -> bool | None:
    """True when the reference range reaches outside the composed RTH range; ``None`` if unknown.

    ⚠ AN ESCAPE DOES NOT IDENTIFY EXTENDED-HOURS TRADING. A capture-vintage adjustment, a
    different quote convention, rounding or bad data produce one too. ``None`` when either
    reference extreme is NULL, so an unknown is never counted as agreement.
    """
    _, composed_high, composed_low, _, _ = composed
    high = reference["high"]
    low = reference["low"]
    if high is None or low is None:
        return None
    return float(high) > composed_high + 5e-7 or float(low) < composed_low - 5e-7


def relative_difference(composed: float, reference: float) -> float | None:
    """``|composed - reference| / |reference|``, or ``None`` on a zero reference."""
    if reference == 0:
        return None
    return abs(composed - reference) / abs(reference)


def percentile(values: Sequence[float], fraction: float) -> float | None:
    """Nearest-rank percentile, NIST definition: rank ``ceil(p*n)``, 1-indexed.

    ⚠ NAMED BECAUSE THE DEFINITIONS DISAGREE and an earlier draft used
    ``round(p*(n-1))``, which returns 3 as the median of ``[1,2,3,4]`` — Python's
    tie-to-even sends ``round(1.5)`` to 2 and indexes the third element. This definition
    returns 2 there, and ``fraction=1.0`` is the maximum.
    """
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


@dataclass(frozen=True)
class MemberCorpus:
    instrument_id: int
    symbol: str
    instrument_type_id: int | None
    resolution_error: str | None
    days: tuple[ComposedDay, ...]
    latency_buckets: Mapping[str, int]

    @property
    def complete(self) -> int:
        return sum(1 for day in self.days if day.status == "complete")

    @property
    def partial(self) -> int:
        return sum(1 for day in self.days if day.status == "partial")

    @property
    def absent(self) -> int:
        return sum(1 for day in self.days if day.status == "absent")

    @property
    def composable_and_settled(self) -> int:
        return sum(1 for day in self.days if day.composable_and_settled)

    def since(self, boundary: date) -> tuple[int, int, int]:
        """``(sessions, composable, composable_and_settled)`` on or after ``boundary``.

        ⚠ THE SPLIT EXISTS BECAUSE THE POOLED SETTLEMENT RATE CONFLATES TWO CAUSES. A
        session before the panel was activated could not have been collected live at all —
        it is backfill by construction — so counting it against settlement measures the
        collector's non-existence, not its reliability. Only the post-activation window can
        say whether live collection settles a session in time.
        """
        window = [day for day in self.days if day.day >= boundary]
        return (
            len(window),
            sum(1 for day in window if day.status == "complete"),
            sum(1 for day in window if day.composable_and_settled),
        )


#: The version that was LIVE AT ``as_of``, not whichever is live now. ⚠ V1 was retired the
#: instant V2 activated, so a historical run against the bare ``status = 'active'`` predicate
#: would analyse V2's membership over a window in which only V1 existed. The partial unique
#: index guarantees at most one ACTIVE row; it guarantees nothing about a past instant, so the
#: interval test is on ``activated_at``/``retired_at`` and the cardinality is asserted.
_UNIVERSE_AT = """
    SELECT universe_version, activated_at, retired_at, status
    FROM strategy_intraday_universe_versions
    WHERE activated_at IS NOT NULL
      AND activated_at <= %(as_of)s
      AND (retired_at IS NULL OR retired_at > %(as_of)s)
    ORDER BY universe_version
"""

#: Member resolution MIRRORS ``strategy_intraday_harvest._active_members``: exact-case
#: ``symbol`` match, ``is_tradable = true``, and EXACTLY ONE match or a resolution error.
#: An earlier draft matched on ``upper(symbol)`` with no tradability filter and silently
#: took whichever row the planner returned, so it could measure a population the collector
#: never touches.
_MEMBERS = """
    SELECT member.ordinal, member.symbol, member.purpose,
           array_agg(instrument.instrument_id ORDER BY instrument.instrument_id)
               FILTER (WHERE instrument.instrument_id IS NOT NULL) AS instrument_ids,
           array_agg(instrument.instrument_type_id ORDER BY instrument.instrument_id)
               FILTER (WHERE instrument.instrument_id IS NOT NULL) AS instrument_type_ids
    FROM strategy_intraday_universe_members AS member
    LEFT JOIN instruments AS instrument
      ON instrument.symbol = member.symbol
     AND instrument.is_tradable = true
    WHERE member.universe_version = %(universe_version)s
      AND member.timeframe = %(timeframe)s
    GROUP BY member.ordinal, member.symbol, member.purpose
    ORDER BY member.ordinal
"""

#: ⚠ EVERY TIMESTAMPED SOURCE IS BOUNDED BY ``as_of``, so a historical run reports the store
#: as it stood then rather than mixing later writes into an earlier cutoff. ``price_daily``
#: and ``instruments`` carry NO as-of column and are therefore NOT boundable — a historical
#: run reads today's values for both, and the report says so rather than implying otherwise.
_BARS = """
    SELECT instrument_id, bar_time, open, high, low, close, volume, captured_at, source
    FROM strategy_intraday_bars
    WHERE timeframe = %(timeframe)s
      AND instrument_id = ANY(%(instrument_ids)s)
      AND captured_at <= %(as_of)s
    ORDER BY instrument_id, bar_time
"""

#: The 5m tier, read ONLY as the (weak, one-directional) absence witness in
#: ``absence_attribution`` — never composed. Raw timestamps, bucketed in Python.
_FIVE_MINUTE_BARS = """
    SELECT instrument_id, bar_time
    FROM strategy_intraday_bars
    WHERE timeframe = '5m'
      AND instrument_id = ANY(%(instrument_ids)s)
      AND captured_at <= %(as_of)s
"""

_DAILY = """
    SELECT instrument_id, price_date, open, high, low, close, volume
    FROM price_daily
    WHERE instrument_id = ANY(%(instrument_ids)s)
      AND price_date >= %(since)s
      AND price_date <= %(as_of_date)s
"""

#: eToro's exchange-33 variant of each member, resolved through ``canonical_instrument_id``
#: and NEVER by stripping a suffix off the symbol. ``docs/review-prevention-log.md`` records
#: that matching each variant to a base "on ``symbol`` alone reported 8 rows whose underlying
#: is different" — ``AMPn`` is Ameriprise while ``AMP`` is a token on eToro's Digital
#: Currency venue. ⚠ The redirect is an OPERATIONAL duplicate mapping populated by
#: constrained suffix matching (``canonical_instrument_redirects.py``), not an independently
#: validated trading-product mapping; and more than one variant per base is possible, so the
#: count is reported rather than assumed to be one.
_VARIANTS = """
    SELECT canonical_instrument_id AS base_instrument_id,
           instrument_id AS variant_instrument_id,
           symbol AS variant_symbol,
           is_tradable
    FROM instruments
    WHERE canonical_instrument_id = ANY(%(instrument_ids)s)
      AND exchange = '33'
    ORDER BY canonical_instrument_id, instrument_id
"""

#: The exchange-33 catalogue figures the write-up cites, emitted rather than hand-copied.
_VARIANT_CATALOGUE = """
    SELECT count(*) AS variants,
           count(*) FILTER (WHERE symbol LIKE '%%.RTH') AS dot_rth_suffixed,
           count(*) FILTER (WHERE canonical_instrument_id IS NOT NULL) AS with_redirect,
           count(*) FILTER (WHERE is_tradable) AS tradable
    FROM instruments
    WHERE exchange = '33'
"""

_REVISIONS = """
    SELECT instrument_id, cause, count(*) AS revisions, min(revised_at) AS first_revised_at
    FROM price_daily_revision
    WHERE instrument_id = ANY(%(instrument_ids)s)
      AND revised_at >= %(since)s
      AND revised_at <= %(as_of)s
    GROUP BY instrument_id, cause
    ORDER BY instrument_id, cause
"""

#: The independent witness for a collection hole: the harvester's own run rows.
_HARVEST_RUNS = """
    SELECT (started_at AT TIME ZONE 'America/New_York')::date AS run_day,
           status, count(*) AS runs
    FROM job_runs
    WHERE job_name = 'strategy_intraday_harvest'
      AND started_at >= %(since)s
      AND started_at <= %(as_of)s
    GROUP BY 1, 2
    ORDER BY 1, 2
"""


def _sessions_between(first: date, last: date) -> list[date]:
    out: list[date] = []
    cursor = first
    while cursor <= last:
        if expected_bars(cursor):
            out.append(cursor)
        cursor += timedelta(days=1)
    return out


def build_corpus(
    members: Sequence[Mapping[str, Any]],
    bars: Iterable[Mapping[str, Any]],
    *,
    as_of: datetime,
) -> list[MemberCorpus]:
    """Group bars into per-member composed sessions over each member's own observed span.

    ⚠ THE DENOMINATOR IS THE MEMBER'S OWN OBSERVED SPAN and it therefore CONDITIONS ON
    SUCCESSFUL OBSERVATION: leading and trailing collection silence is outside it, so the
    rate measures internal coverage and is not a prospective reliability figure for a panel
    that has not been collected yet.

    ⚠ ``as_of`` excludes any session whose close has not passed, so a run during market
    hours does not record the current session as permanently short.
    """
    by_instrument: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in bars:
        by_instrument[int(row["instrument_id"])].append(row)
    out: list[MemberCorpus] = []
    for member in members:
        ids = [int(value) for value in member["instrument_ids"] or []]
        types = [int(value) for value in member["instrument_type_ids"] or [] if value is not None]
        resolution_error = None if len(ids) == 1 else f"expected one tradable instrument, found {len(ids)}"
        instrument_id = ids[0] if len(ids) == 1 else -1
        rows = by_instrument.get(instrument_id, []) if instrument_id > 0 else []
        if not rows:
            out.append(
                MemberCorpus(
                    instrument_id=instrument_id,
                    symbol=str(member["symbol"]),
                    instrument_type_id=types[0] if len(types) == 1 else None,
                    resolution_error=resolution_error,
                    days=(),
                    latency_buckets={},
                )
            )
            continue
        by_day: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
        buckets: dict[str, int] = defaultdict(int)
        for row in rows:
            by_day[row["bar_time"].astimezone(_NY).date()].append(row)
            buckets[nominality_bucket(row["bar_time"], row["captured_at"])] += 1
        settled_days = [
            day
            for day in _sessions_between(min(by_day), max(by_day))
            if (close := session_close_utc(day)) is not None and close <= as_of
        ]
        out.append(
            MemberCorpus(
                instrument_id=instrument_id,
                symbol=str(member["symbol"]),
                instrument_type_id=types[0] if len(types) == 1 else None,
                resolution_error=resolution_error,
                days=tuple(compose_day(day, by_day.get(day, ())) for day in settled_days),
                latency_buckets=dict(buckets),
            )
        )
    return out


def harvest_run_gaps(
    rows: Sequence[Mapping[str, Any]],
    *,
    first: date | None = None,
    last: date | None = None,
) -> list[tuple[date, date, int]]:
    """Runs of sessions inside the MEASURED span that hold NO harvester run row at all.

    ⚠ THE BOUNDS ARE PASSED IN, NOT DERIVED FROM THE ROWS. Deriving ``min..max`` from the rows
    whose absence is being measured hides an outage at either edge: a collector that stopped
    for the last sessions before the cutoff has no row to mark the end of the span, so the
    gap disappears and §1c mislabels those sessions as residuals "with a run row present".
    """
    days = {row["run_day"] for row in rows}
    if (first is None or last is None) and not days:
        return []
    start = first if first is not None else min(days)
    end = last if last is not None else max(days)
    span = _sessions_between(start, end)
    gaps: list[tuple[date, date, int]] = []
    run: list[date] = []
    for day in span:
        if day in days:
            if run:
                gaps.append((run[0], run[-1], len(run)))
                run = []
        else:
            run.append(day)
    if run:
        gaps.append((run[0], run[-1], len(run)))
    return gaps


def _fetch(conn: psycopg.Connection[Any], *, as_of: datetime) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_UNIVERSE_AT, {"as_of": as_of})
        universes = cur.fetchall()
        if len(universes) != 1:
            raise RuntimeError(
                f"expected exactly one intraday universe live at {as_of.isoformat()}, found {len(universes)}"
            )
        universe = universes[0]
        cur.execute(_MEMBERS, {"universe_version": universe["universe_version"], "timeframe": TIMEFRAME})
        members = cur.fetchall()
        instrument_ids = sorted(
            {int(value) for row in members for value in (row["instrument_ids"] or []) if value is not None}
        )
        cur.execute(_BARS, {"timeframe": TIMEFRAME, "instrument_ids": instrument_ids, "as_of": as_of})
        bars = cur.fetchall()
        # ⚠ The NY SESSION date, not the UTC calendar date. Every other boundary in this script
        # is NY-anchored, and a 30m bar at 13:30 UTC on one NY date can carry a different UTC
        # date at other times of year. Only widens the window either way, but a lower bound on
        # a different clock from the sessions it bounds is the kind of inconsistency that is
        # harmless until someone reuses it.
        since = min((row["bar_time"] for row in bars), default=datetime.now(UTC)).astimezone(_NY).date()
        cur.execute(_FIVE_MINUTE_BARS, {"instrument_ids": instrument_ids, "as_of": as_of})
        five = cur.fetchall()
        cur.execute(_DAILY, {"instrument_ids": instrument_ids, "since": since, "as_of_date": as_of.date()})
        daily = cur.fetchall()
        cur.execute(_VARIANTS, {"instrument_ids": instrument_ids})
        variants = cur.fetchall()
        cur.execute(_VARIANT_CATALOGUE)
        catalogue = cur.fetchone()
        variant_daily: list[Mapping[str, Any]] = []
        if variants:
            cur.execute(
                _DAILY,
                {
                    "instrument_ids": sorted({int(row["variant_instrument_id"]) for row in variants}),
                    "since": since,
                    "as_of_date": as_of.date(),
                },
            )
            variant_daily = list(cur.fetchall())
        cur.execute(_REVISIONS, {"instrument_ids": instrument_ids, "since": since, "as_of": as_of})
        revisions = cur.fetchall()
        cur.execute(_HARVEST_RUNS, {"since": since, "as_of": as_of})
        harvest_runs = cur.fetchall()
    return {
        "universe": universe,
        "members": members,
        "bars": bars,
        "five": five,
        "daily": daily,
        "variants": variants,
        "variant_catalogue": catalogue,
        "variant_daily": variant_daily,
        "revisions": revisions,
        "harvest_runs": harvest_runs,
        "since": since,
    }


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.6f}"


def _compare(
    corpus: MemberCorpus,
    reference_by_key: Mapping[tuple[int, date], Mapping[str, Any]],
    reference_id: int,
) -> dict[str, Any]:
    """Composed vs one reference series, counting unusable references separately."""
    closes: list[float] = []
    exact = 0
    paired = 0
    escaped = 0
    escape_known = 0
    unusable = 0
    gate_flips: list[str] = []
    for day in corpus.days:
        if day.bar is None:
            continue
        reference = reference_by_key.get((reference_id, day.day))
        if reference is None:
            continue
        if reference["close"] is None:
            unusable += 1
            continue
        paired += 1
        reference_close = float(reference["close"])
        difference = relative_difference(day.bar[3], reference_close)
        if difference is not None:
            closes.append(difference)
        if abs(day.bar[3] - reference_close) < 5e-7:
            exact += 1
        escape = range_escapes(day.bar, reference)
        if escape is not None:
            escape_known += 1
            escaped += int(escape)
        # ⚠ READ FROM S-12, NEVER TYPED. ``PRICE_FLOOR`` is derived from the cheapest cost
        # band, so a recalibration moves the gate; a literal 100.0 here would silently keep
        # measuring a threshold the rule no longer uses.
        if (day.bar[3] >= PRICE_FLOOR) != (reference_close >= PRICE_FLOOR):
            gate_flips.append(f"{corpus.symbol} {day.day}: composed {day.bar[3]} vs reference {reference_close}")
    return {
        "paired": paired,
        "unusable": unusable,
        "exact": exact,
        "p50": percentile(closes, 0.5),
        "p95": percentile(closes, 0.95),
        "max": percentile(closes, 1.0),
        "escaped": escaped,
        "escape_known": escape_known,
        "gate_flips": gate_flips,
    }


def _report(payload: Mapping[str, Any], *, as_of: datetime) -> str:
    universe = payload["universe"]
    corpora = build_corpus(payload["members"], payload["bars"], as_of=as_of)
    daily_by_key = {(int(row["instrument_id"]), row["price_date"]): row for row in payload["daily"]}
    variant_daily_by_key = {(int(row["instrument_id"]), row["price_date"]): row for row in payload["variant_daily"]}
    needed = first_evaluable_series_length()
    # The MEASURED span: the corpus's own first bar date through the last completed session at
    # the cutoff. Passed to ``harvest_run_gaps`` so an outage at either edge cannot vanish.
    measured_first = payload["since"]
    measured_last = max((day.day for corpus in corpora for day in corpus.days), default=measured_first)
    run_gaps = harvest_run_gaps(payload["harvest_runs"], first=measured_first, last=measured_last)
    out: list[str] = []
    out.append(f"# #2840 forward daily provenance — {TIMEFRAME} tier, universe {universe['universe_version']}")
    out.append(f"activated_at {universe['activated_at']}; corpus from {payload['since']}; as_of {as_of.isoformat()}")
    out.append(
        f"S-4 needs {needed} bars for ONE evaluable verdict (COMPUTED by probe; WARMUP_BARS = "
        f"{s4.WARMUP_BARS} is the first evaluable INDEX and the final bar has no fill successor)"
    )
    out.append("")

    out.append("## 1. Composability and SETTLEMENT, per member, over the member's own observed span")
    out.append("symbol  inst   type  sessions  complete  partial  absent  off_grid  settled+complete  first       last")
    for corpus in corpora:
        if corpus.resolution_error:
            out.append(f"{corpus.symbol:<7} RESOLUTION ERROR: {corpus.resolution_error}")
            continue
        if not corpus.days:
            out.append(
                f"{corpus.symbol:<7} {corpus.instrument_id:<6} {corpus.instrument_type_id!s:<5} NO {TIMEFRAME} BARS"
            )
            continue
        off_grid = sum(day.off_grid for day in corpus.days)
        out.append(
            f"{corpus.symbol:<7} {corpus.instrument_id:<6} {corpus.instrument_type_id!s:<5} "
            f"{len(corpus.days):<9} {corpus.complete:<9} {corpus.partial:<8} {corpus.absent:<7} "
            f"{off_grid:<9} {corpus.composable_and_settled:<17} {corpus.days[0].day}  {corpus.days[-1].day}"
        )
    total_days = sum(len(c.days) for c in corpora)
    total_complete = sum(c.complete for c in corpora)
    total_settled = sum(c.composable_and_settled for c in corpora)
    out.append("")
    if total_days:
        out.append(
            f"panel: {total_complete}/{total_days} composable ({100.0 * total_complete / total_days:.2f}%); "
            f"{total_settled}/{total_days} composable AND settled before the next open "
            f"({100.0 * total_settled / total_days:.2f}%)"
        )
    out.append(
        "  ^ the SECOND figure is the load-bearing one and it is the intersection of THREE conjuncts: "
        "composable, settled before the next open (nominality), and present before the scan deadline "
        "(availability). ⚠ The last two are DIFFERENT deadlines — the scan fires at 06:45 UTC and the next "
        "open is 13:30 UTC in EDT — so a catch-up can be nominal and still too late to have been used."
    )
    out.append("")
    boundary = universe["activated_at"].astimezone(_NY).date()
    out.append(f"### 1b. The same three counts restricted to sessions on or after activation ({boundary})")
    out.append("symbol  sessions  composable  composable+settled  settled_rate")
    window_totals = [0, 0, 0]
    for corpus in corpora:
        if not corpus.days:
            continue
        sessions, composable, settled = corpus.since(boundary)
        window_totals = [window_totals[0] + sessions, window_totals[1] + composable, window_totals[2] + settled]
        rate = f"{settled / sessions:.4f}" if sessions else "n/a"
        out.append(f"{corpus.symbol:<7} {sessions:<9} {composable:<11} {settled:<19} {rate}")
    if window_totals[0]:
        out.append(
            f"panel (post-activation): {window_totals[1]}/{window_totals[0]} composable, "
            f"{window_totals[2]}/{window_totals[0]} composable AND settled "
            f"({100.0 * window_totals[2] / window_totals[0]:.2f}%)"
        )
    out.append(
        "  ^ ⚠ this is the only window in which live collection was POSSIBLE, so it is the only one whose "
        "rate says anything about the design."
    )
    out.append("")
    out.append("### 1c. Are the unusable sessions the SAME sessions as the collector's missing run rows?")
    no_run_days = {day for gap in run_gaps for day in _sessions_between(gap[0], gap[1])}
    out.append("symbol  unusable  ∩ no_run_row  residual (unusable with a run row present)")
    for corpus in corpora:
        if not corpus.days:
            continue
        unusable = {day.day for day in corpus.days if day.day >= boundary and not day.composable_and_settled}
        overlap = unusable & no_run_days
        residual = sorted(unusable - no_run_days)
        out.append(
            f"{corpus.symbol:<7} {len(unusable):<9} {len(overlap):<13} "
            f"{len(residual)}{' — ' + ', '.join(str(day) for day in residual[:6]) if residual else ''}"
        )
    out.append(
        "  ^ ⚠ THE INTERSECTION IS EMITTED RATHER THAN ASSERTED. An earlier draft claimed the shortfall and "
        "the no-run-row window were the same sessions; the counts did not support it, and a RESIDUAL session "
        "— unusable while the collector was running — is a different defect from an outage."
    )
    out.append("")

    out.append(f"## 2. Wait to a first evaluable S-4 verdict ({needed} composable+settled bars)")
    out.append("symbol  composable  composable+settled  short_by(settled)  sessions_spanned")
    for corpus in corpora:
        if not corpus.days:
            continue
        out.append(
            f"{corpus.symbol:<7} {corpus.complete:<11} {corpus.composable_and_settled:<19} "
            f"{max(0, needed - corpus.composable_and_settled):<18} {len(corpus.days)}"
        )
    out.append(
        "  ^ S-4's warm-up counts SERIES POSITIONS, not calendar days, so a gap does not reset it. ⚠ These "
        "counts are of bars ALREADY COLLECTED on the base instrument; they are NOT transferable inventory for a "
        "differently-declared forward panel, and no accrual rate for one is measured here."
    )
    out.append("")

    out.append("## 3. Opportunities a split check would have to rule out, per stored bar")
    buckets = ("before_next_open", "after_1_opens", "after_2_opens", "after_3_opens", "impossible")
    out.append("symbol  " + "".join(f"{name:<18}" for name in buckets) + "before_next_open_share")
    for corpus in corpora:
        if not corpus.latency_buckets:
            continue
        total = sum(corpus.latency_buckets.values())
        counts = [corpus.latency_buckets.get(name, 0) for name in buckets]
        out.append(f"{corpus.symbol:<7} " + "".join(f"{count:<18}" for count in counts) + f"{counts[0] / total:.4f}")
    out.append(
        "  ^ 'before_next_open' had NO opportunity for a re-basing. The others are NOT established as re-based "
        "— the provider's intraday adjustment behaviour is UNTESTED (see the module docstring) and this repo "
        "stores no corporate-action table, so the ruling-out cannot be done from the store at all."
    )
    out.append("")
    out.append("price_daily_revision causes in the window (sql/387 calls cause an UPPER BOUND on attribution;")
    out.append("⚠ it is a per-WRITE branch and is NOT joined to any (bar_time, captured_at] window):")
    for row in payload["revisions"] or []:
        out.append(
            f"  inst {row['instrument_id']}  {row['cause']:<18} {row['revisions']:>5}  first {row['first_revised_at']}"
        )
    if not payload["revisions"]:
        out.append("  (none)")
    out.append("")

    out.append("## 3b. Capture days that wrote MORE THAN ONE session (a grouping, not a cause)")
    bars_by_instrument: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in payload["bars"]:
        bars_by_instrument[int(row["instrument_id"])].append(row)
    for corpus in corpora:
        catchups = catchup_capture_days(bars_by_instrument.get(corpus.instrument_id, []))
        if not catchups:
            continue
        rendered = "; ".join(
            f"{capture_day} wrote {count} bars for {first}..{last}" for capture_day, count, first, last in catchups[:3]
        )
        out.append(f"  {corpus.symbol:<7} {len(catchups)} such days (3 largest) — {rendered}")
    out.append("")
    out.append("## 3c. The independent witness: sessions with NO strategy_intraday_harvest run row")
    gaps = run_gaps
    if gaps:
        for first, last, count in gaps:
            out.append(f"  {first} .. {last}  ({count} session{'s' if count != 1 else ''} with no run row at all)")
    else:
        out.append("  (no session inside the observed span lacks a run row)")
    statuses: dict[str, int] = defaultdict(int)
    for row in payload["harvest_runs"]:
        statuses[str(row["status"])] += int(row["runs"])
    out.append(f"  run rows by status since {payload['since']}: {dict(sorted(statuses.items()))}")
    out.append("")

    out.append("## 3d. Missing 30m slots — a WEAK, one-directional absence witness")
    five_by_instrument: dict[int, set[datetime]] = defaultdict(set)
    for row in payload["five"]:
        five_by_instrument[int(row["instrument_id"])].add(five_minute_slot(row["bar_time"]))
    out.append("symbol  slots  with_30m  missing_but_5m_present  missing_both")
    for corpus in corpora:
        if not corpus.days:
            continue
        slots = [slot for day in corpus.days for slot in session_slots(day.day)]
        thirty = [row["bar_time"].astimezone(UTC) for row in bars_by_instrument.get(corpus.instrument_id, [])]
        attribution = absence_attribution(slots, thirty, five_by_instrument.get(corpus.instrument_id, set()))
        out.append(
            f"{corpus.symbol:<7} {attribution['slots']:<6} {attribution['with_30m']:<9} "
            f"{attribution['missing_30m_5m_present']:<22} {attribution['missing_both']}"
        )
    out.append(
        "  ^ ⚠ AN ALL-ZERO THIRD COLUMN EXONERATES NOTHING. Both tiers are fetched by the same job, so a shared "
        "outage leaves no discordant witness — which looks identical to a healthy collector. The fourth column "
        "merges 'observed and empty' with 'never observed' (5m has its own membership dates, 12-month retention "
        "and a far shorter per-request reach)."
    )
    out.append("")

    out.append("## 4. Composed vs price_daily — the MEMBER instrument, then its exchange-33 variant")
    catalogue = payload["variant_catalogue"] or {}
    out.append(
        f"exchange-33 catalogue: {catalogue.get('variants')} instruments, "
        f"{catalogue.get('dot_rth_suffixed')} '.RTH'-suffixed, {catalogue.get('with_redirect')} carrying a "
        f"canonical_instrument_id, {catalogue.get('tradable')} tradable"
    )
    variants_by_base: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in payload["variants"]:
        variants_by_base[int(row["base_instrument_id"])].append(row)
    out.append("")
    out.append("symbol  ref              paired  unusable  exact  p50        p95        max        escapes")
    all_flips: list[str] = []
    for corpus in corpora:
        if not corpus.days:
            continue
        rows_to_print: list[tuple[str, dict[str, Any]]] = [
            (f"self({corpus.instrument_id})", _compare(corpus, daily_by_key, corpus.instrument_id))
        ]
        candidates = variants_by_base.get(corpus.instrument_id, [])
        if len(candidates) > 1:
            out.append(f"{corpus.symbol:<7} ⚠ {len(candidates)} exchange-33 variants redirect here; all compared")
        for variant in candidates:
            rows_to_print.append(
                (
                    f"{variant['variant_symbol']}{'' if variant['is_tradable'] else ' (untradable)'}",
                    _compare(corpus, variant_daily_by_key, int(variant["variant_instrument_id"])),
                )
            )
        if not candidates:
            rows_to_print.append(("(no exchange-33 variant redirects here)", {}))
        for label, result in rows_to_print:
            if not result:
                out.append(f"{corpus.symbol:<7} {label}")
                continue
            all_flips.extend(result["gate_flips"])
            escapes = f"{result['escaped']}/{result['escape_known']}" if result["escape_known"] else "n/a"
            out.append(
                f"{corpus.symbol:<7} {label:<16} {result['paired']:<7} {result['unusable']:<9} "
                f"{result['exact']:<6} {_fmt(result['p50']):<10} {_fmt(result['p95']):<10} "
                f"{_fmt(result['max']):<10} {escapes}"
            )
    out.append(
        "  ^ 'escapes' = reference range reaching outside the composed RTH range, over the pairs where both "
        "reference extremes are non-NULL. ⚠ An escape does not identify extended-hours trading, and neither "
        "reference is truth. What a persistent difference DOES forbid is MIXING two of these series inside one "
        "window — that is the #2066 split-cliff shape in a new costume."
    )
    out.append("")
    out.append("## 4b. open(D) == close(D-1) on CALENDAR-ADJACENT sessions (a question, not a witness)")
    daily_by_instrument: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in payload["daily"]:
        daily_by_instrument[int(row["instrument_id"])].append(row)
    for corpus in corpora:
        if not corpus.days:
            continue
        matches, pairs = prior_close_open_matches(daily_by_instrument.get(corpus.instrument_id, []))
        share = f"{100.0 * matches / pairs:.1f}%" if pairs else "n/a"
        out.append(f"  {corpus.symbol:<7} {matches}/{pairs} ({share})")
    out.append("  ^ a real opening print CAN equal the previous close; a high rate motivates a question only.")
    out.append("")

    out.append(f"## 5. Does the reference choice flip S-12's >= ${PRICE_FLOOR:g} gate on any paired session?")
    if all_flips:
        for line in all_flips[:20]:
            out.append(f"  FLIP {line}")
        out.append(f"  ({len(all_flips)} flips across all references)")
    else:
        out.append("  no flips on any reference — and that is a fact about THIS PANEL, not about the series:")
    for corpus in corpora:
        latest = next((day for day in reversed(corpus.days) if day.bar is not None), None)
        if latest is None or latest.bar is None:
            continue
        close = latest.bar[3]
        out.append(
            f"    {corpus.symbol:<7} {close:>10.2f}  {'>= gate' if close >= PRICE_FLOOR else '< gate':<8} "
            f"({latest.day}, {abs(close - PRICE_FLOOR) / PRICE_FLOOR * 100:.1f}% from the edge)"
        )
    out.append(
        "  ^ no member sits near the edge, so this panel cannot demonstrate a flip either way, and the census "
        f"around the ${PRICE_FLOOR:g} boundary that would settle the reference choice is NOT available from it."
    )
    return "\n".join(out)


# ⚠ THE SESSION PROFILE IS RESOLVED HERE AND NOT IN THE RULE. ``capture_certificate``
# refuses any listing the NYSE calendar does not describe, and takes the profile as a
# required argument rather than deriving it — a service reaching into ``app.api`` would
# invert the layering.
#
# ⚠ The CASE is INTERPOLATED FROM THE PRODUCER, not restated. It expects the aliases ``i``
# (instruments) and ``e`` (exchanges), which is why they are named that way below. It
# already handles the half that is easy to get wrong: exchange 33 is an RTH-only duplicate
# whose ``asset_class`` is still ``us_equity``, so its test precedes the asset-class one.
#
# ⚠⚠ BOTH JOINS ARE ``LEFT`` AND THAT IS THE LOAD-BEARING PART, NOT TIDINESS. This query
# feeds a CENSUS whose entire output is a set of counts, so an inner join would silently
# shrink the denominator: a bar whose instrument or exchange row is missing would vanish
# from ``scanned`` instead of being reported, and the census would present a subset as the
# whole population. Measured today (dev DB, 2026-09-20): ZERO bars lack an ``instruments``
# row and ZERO instruments lack an ``exchanges`` row — which is exactly why the inner form
# would have looked correct indefinitely and then quietly stopped being so.
#
# With ``LEFT``, a missing row yields NULL, the ``ELSE`` resolves it to ``continuous``, and
# ``capture_certificate`` refuses it under ``non_nyse_trading_calendar``. The population
# stays fixed and the unknown becomes a visible refusal instead of an absence.
_CAPTURE_CERTIFICATE_SQL = f"""
    SELECT b.timeframe,
           b.bar_time,
           b.captured_at,
           {_SESSION_PROFILE_SQL}
    FROM strategy_intraday_bars b
    LEFT JOIN instruments i ON i.instrument_id = b.instrument_id
    LEFT JOIN exchanges e ON e.exchange_id = i.exchange
"""


def _bucket_table(by_timeframe: Mapping[str, Counter[str]]) -> list[str]:
    """One rendered table per bucket vocabulary, totals included."""
    buckets = sorted({bucket for counts in by_timeframe.values() for bucket in counts})
    # ⚠ The header's widths are the SAME format specs as the data rows below, not
    # literals that happen to match. "timeframe" is nine characters, so the literal
    # lined up by coincidence and would have silently skewed the moment the word or
    # the column width changed.
    rows = ["  " + f"{'timeframe':>9}" + "  " + "  ".join(f"{bucket:>30}" for bucket in buckets) + f"  {'total':>9}"]
    for timeframe in sorted(by_timeframe):
        counts = by_timeframe[timeframe]
        rows.append(
            f"  {timeframe:>9}  "
            + "  ".join(f"{counts[bucket]:>30,}" for bucket in buckets)
            + f"  {sum(counts.values()):>9,}"
        )
    totals: Counter[str] = Counter()
    for counts in by_timeframe.values():
        totals.update(counts)
    rows.append(
        "  "
        + "all".rjust(9)
        + "  "
        + "  ".join(f"{totals[bucket]:>30,}" for bucket in buckets)
        + f"  {sum(totals.values()):>9,}"
    )
    return rows


def capture_certificate_report(
    rows: Iterable[tuple[str, datetime, datetime, str]], *, capture_semantics_from: datetime | None
) -> str:
    """Every stored bar under ``bar_capture_certificate``, and what the withdrawn proxy cost.

    ⚠ COMPUTED, NEVER TYPED. The population moves with every harvest, so the module
    docstrings and the proposal cite THIS command instead of freezing a count — the
    ``.claude/CLAUDE.md`` rule that a derived statistic in prose must be computed or
    accompanied by the command that reproduces it.

    The second block is the reason the rule beat the proxy: ``same NY calendar date`` is a
    proxy for ``no session open intervened`` and refuses bars the mechanism certifies. It is
    reported as a DISAGREEMENT COUNT IN BOTH DIRECTIONS, because a one-sided figure cannot
    show that the error is one-directional.
    """
    by_timeframe: dict[str, Counter[str]] = defaultdict(Counter)
    arithmetic_by_timeframe: dict[str, Counter[str]] = defaultdict(Counter)
    disagreements: Counter[tuple[bool, bool]] = Counter()
    impossible_excluded = 0
    non_nyse_excluded = 0
    # ⚠ Counted as we go rather than ``len(rows)``: the caller streams a
    # server-side cursor, so the sequence is consumed once and has no length.
    scanned = 0
    profiles: Counter[str] = Counter()
    for timeframe_text, bar_time, captured_at, session_profile in rows:
        scanned += 1
        profiles[session_profile] += 1
        # ⚠ VALIDATED, NOT CAST. The column is a TEXT with its own CHECK, and a cast would
        # turn a tier added to the table but not to INTRADAY_TIERS into a KeyError several
        # frames away instead of a named refusal here.
        if timeframe_text not in INTRADAY_TIERS:
            raise ValueError(f"stored timeframe {timeframe_text!r} is not a declared tier")
        timeframe = cast("Timeframe", timeframe_text)
        bucket = capture_certificate(
            bar_time,
            captured_at,
            timeframe=timeframe,
            capture_semantics_from=capture_semantics_from,
            session_profile=session_profile,
        )
        by_timeframe[timeframe][bucket] += 1
        # ⚠⚠ NON-NYSE ROWS STOP HERE (Codex checkpoint 2, P2). The ADMISSION table above is
        # total over the population — it has a bucket for them and must keep counting them.
        # The two blocks BELOW are not: both call ``nominality_bucket``, which is NYSE
        # arithmetic, so feeding a foreign bar into them would compute "did a NEW YORK open
        # intervene" for a listing that does not trade on that calendar and then print the
        # answer as "certified by the RULE". That is precisely the fail-open the
        # ``session_profile`` gate was added to close, reintroduced one frame later in the
        # reporting — the gate would read as enforced while the headline figures ignored it.
        #
        # ⚠ Counted rather than skipped. A silently smaller denominator in the arithmetic
        # table is the same defect in the other direction, so the count is printed.
        if session_profile not in NYSE_SESSION_PROFILES:
            non_nyse_excluded += 1
            continue
        # ⚠ The proxy comparison is against the ARITHMETIC half deliberately. It
        # asks "does the same-NY-date proxy agree with the session-open rule",
        # which is a question about the calendar and not about whether this
        # database's stamps are trustworthy. Comparing it to the admission
        # verdict instead makes every cell zero the moment a store predates
        # sql/402 — a dead output block that still reads like a clean result.
        arithmetic = nominality_bucket(bar_time, captured_at, timeframe=timeframe)
        arithmetic_by_timeframe[timeframe][arithmetic] += 1
        # ⚠ ``impossible`` rows are EXCLUDED from the directional comparison, not
        # counted as "refused by the rule". They are same-NY-date by construction
        # (a capture before the bar completed is the same afternoon), so leaving
        # them in makes the must-be-zero cell non-zero for a reason that has
        # nothing to do with the proxy. The schema has no completion-time CHECK,
        # so they are reachable rather than hypothetical.
        if arithmetic != IMPOSSIBLE_BUCKET:
            same_ny_date = captured_at.astimezone(_NY).date() == bar_time.astimezone(_NY).date()
            disagreements[(arithmetic == CERTIFIED_BUCKET, same_ny_date)] += 1
        else:
            impossible_excluded += 1

    out = [
        f"capture certificate ({CAPTURE_CERTIFICATE_VERSION}) over {scanned:,} stored bars",
        # ⚠ "marker unavailable" and NOT "sql/402 not applied". The two come apart
        # in a staged deployment where 402 has run and 403 has not: the new
        # semantics are already active, and naming 402 would misdiagnose it.
        f"  capture semantics trustworthy from: "
        f"{capture_semantics_from or 'UNKNOWN — sql/403 cutover marker unavailable; nothing admissible'}",
        # ⚠ PRINTED, because the NYSE-calendar precondition is otherwise invisible. Every
        # bar in the store is US-listed today, so the refusal below is a no-op and would
        # stay silently a no-op after a foreign member joined the harvested panel — the
        # bucket table only shows a profile once one appears in it. This line shows the
        # composition of the population the rule was applied to, whatever it is.
        "  session profiles scanned: "
        + (", ".join(f"{profile}={count:,}" for profile, count in sorted(profiles.items())) or "none"),
        "",
    ]
    out.append("ADMISSION verdict (capture_certificate) — what a consumer may use:")
    out.extend(_bucket_table(by_timeframe))
    out.append("")
    # ⚠ BOTH TABLES ARE EMITTED, AND THE SECOND IS NOT REDUNDANT. Every stored bar
    # currently predates sql/403, so the admission table above is one column wide and
    # cannot reproduce the per-timeframe distribution the proposal cites this command
    # for. A command that no longer computes the figure its prose points at is the
    # defect the prevention log records under "a rewritten formatter silently falsifies
    # every claim made about its output elsewhere".
    out.append(
        f"ARITHMETIC only (nominality_bucket) — did an open intervene, ignoring the cutover"
        f" [NYSE-calendar listings only; {non_nyse_excluded:,} bars excluded]:"
    )
    out.extend(_bucket_table(arithmetic_by_timeframe))
    out.append("")
    out.append("withdrawn proxy vs the ARITHMETIC rule (ignores the sql/402 cutover, by design):")
    out.append(f"  certified by BOTH                        {disagreements[(True, True)]:,}")
    out.append(
        f"  certified by the RULE, refused by proxy  {disagreements[(True, False)]:,}  <- evidence the proxy discards"
    )
    out.append(
        f"  certified by the PROXY, refused by rule  {disagreements[(False, True)]:,}"
        "  <- must be 0; the error is one-directional"
    )
    out.append(f"  refused by both                          {disagreements[(False, False)]:,}")
    out.append(f"  excluded as 'impossible' (capture before bar completion)  {impossible_excluded:,}")
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        default=None,
        help="ISO instant; sessions whose close is after it are excluded. Defaults to now.",
    )
    parser.add_argument(
        "--capture-certificate",
        action="store_true",
        help="Report every stored intraday bar under bar_capture_certificate, and nothing else.",
    )
    args = parser.parse_args(argv)
    as_of = datetime.fromisoformat(args.as_of).astimezone(UTC) if args.as_of else datetime.now(UTC)
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        if args.capture_certificate:
            # ⚠ A SERVER-SIDE (named) CURSOR, not ``fetchall()``. The report only
            # reduces rows into counters, and at the declared tier caps and
            # retention this table reaches millions of rows — materialising it
            # would make the census die on the corpus it exists to measure.
            cutover = capture_semantics_cutover(conn)
            with conn.cursor(name="capture_certificate_scan", row_factory=tuple_row) as scan:
                scan.itersize = 10_000
                scan.execute(_CAPTURE_CERTIFICATE_SQL)
                print(
                    capture_certificate_report(
                        cast("Iterable[tuple[str, datetime, datetime, str]]", scan),
                        capture_semantics_from=cutover,
                    )
                )
            return 0
        payload = _fetch(conn, as_of=as_of)
    print(_report(payload, as_of=as_of))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())


__all__ = [
    "ComposedDay",
    "MemberCorpus",
    "absence_attribution",
    "build_corpus",
    "catchup_capture_days",
    "compose_day",
    "expected_bars",
    "first_evaluable_series_length",
    "five_minute_slot",
    "harvest_run_gaps",
    "main",
    "next_session_open_utc",
    "nominality_bucket",
    "percentile",
    "prior_close_open_matches",
    "range_escapes",
    "relative_difference",
    "session_close",
    "session_close_utc",
    "session_slots",
]
