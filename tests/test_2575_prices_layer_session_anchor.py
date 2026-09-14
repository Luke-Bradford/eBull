"""#2575 — the `prices` layer is a TRADING-SESSION question, not a wall-clock one.

``_LAYER_QUERIES["prices"]`` is ``MAX(price_date)::timestamptz`` — midnight of the
last trading date. Aged against the old 4-hour threshold it was stale on any
weekday afternoon and for every weekend, on a completely healthy pipeline.
Measured on dev 2026-09-14 12:5x UTC with ``daily_candle_refresh`` succeeding
seven times that day::

    prices  stale  latest=2026-09-11 00:00:00+00:00  age=3 days, 13:13:27  max=4:00:00

``_derive_overall_status`` maps any stale layer to ``degraded``, so that single
verdict held ``/system/status`` off ``ok`` permanently.
``docs/review-prevention-log.md`` records the same measurement on 2026-08-13
(#2624 scope 3) as a reason not to REUSE the signal; the signal was never fixed.

⚠ Every date below is a REAL calendar date, chosen so the assertion fails if the
holiday rules move: 2026-09-07 is Labor Day and 2026-09-11/14 are a genuine
Friday/Monday pair. A synthetic date would pass whatever ``market_calendar`` did.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.ops_monitor import (
    _LAYER_QUERIES,
    _SESSION_ANCHORED_LAYERS,
    _STALENESS_THRESHOLDS,
    ALL_LAYERS,
    check_layer_staleness,
    required_completed_session,
)


def _conn_with_latest(latest: datetime | None) -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = {"latest": latest}
    conn = MagicMock()
    conn.cursor.side_effect = lambda **kwargs: cur
    return conn


def _bar_date(year: int, month: int, day: int) -> datetime:
    """What the layer query returns: a DATE cast to midnight UTC."""
    return datetime(year, month, day, tzinfo=UTC)


def _prices(latest: datetime | None, now: datetime) -> Any:
    return check_layer_staleness(_conn_with_latest(latest), "prices", now=now)


# ---------------------------------------------------------------------------
# The partition — a layer has exactly one freshness rule
# ---------------------------------------------------------------------------


def test_every_layer_has_exactly_one_freshness_rule() -> None:
    """The guard behind ``check_layer_staleness``'s RuntimeError branch.

    Two failure modes, both silent without this: a new layer in neither set
    reaches the raise, and a layer in both gets whichever rule the verdict order
    happens to check first.
    """
    for layer in ALL_LAYERS:
        has_threshold = layer in _STALENESS_THRESHOLDS
        is_session_anchored = layer in _SESSION_ANCHORED_LAYERS
        assert has_threshold != is_session_anchored, (
            f"layer {layer!r} has threshold={has_threshold} session_anchored={is_session_anchored} "
            "— it must have exactly one freshness rule"
        )


def test_prices_has_no_wall_clock_threshold() -> None:
    """The regression this ticket is about. ``price_date`` is a trading date, so
    ANY wall-clock threshold over it is unsatisfiable most of the week."""
    assert "prices" not in _STALENESS_THRESHOLDS
    assert "prices" in _SESSION_ANCHORED_LAYERS


def test_the_prices_query_still_reads_the_bar_date() -> None:
    """⚠ Pins the SOURCE, because the session anchor is only correct for it.

    The verdict compares ``latest.date()`` against an NYSE session. That is
    exact only while ``latest`` is a DATE cast to midnight — swap the query to a
    real timestamp column and the comparison silently changes meaning.
    """
    assert "MAX(price_date)" in _LAYER_QUERIES["prices"]
    assert "::timestamptz" in _LAYER_QUERIES["prices"]


# ---------------------------------------------------------------------------
# The grace comes from the orchestrator, not from a second local constant
# ---------------------------------------------------------------------------


def test_the_grace_is_the_orchestrators_own_declared_candle_cadence() -> None:
    """#2407's defect, one layer over: the old comment claimed prices "refresh
    hourly" while the orchestrator declared a 24h cadence for the layer that
    writes them. A local copy of a cadence is a copy that drifts."""
    from app.services.sync_orchestrator.registry import LAYERS

    _, grace = required_completed_session(datetime(2026, 9, 14, 13, 0, tzinfo=UTC))
    assert grace == LAYERS["candles"].cadence.interval


# ---------------------------------------------------------------------------
# The live case, and the one it replaces
# ---------------------------------------------------------------------------


def test_monday_morning_with_fridays_bars_is_ok() -> None:
    """The exact dev measurement above, inverted. Under the 4-hour threshold
    this read `stale` with age 3d13h."""
    result = _prices(_bar_date(2026, 9, 11), now=datetime(2026, 9, 14, 13, 0, tzinfo=UTC))
    assert result.status == "ok"
    assert result.latest == _bar_date(2026, 9, 11)
    assert "2026-09-11" in result.detail


def test_a_session_behind_is_stale() -> None:
    """The other direction, which the first assertion alone would let drift: a
    rule that says `ok` unconditionally passes it too."""
    result = _prices(_bar_date(2026, 9, 10), now=datetime(2026, 9, 14, 13, 0, tzinfo=UTC))
    assert result.status == "stale"
    assert "2026-09-10" in result.detail
    assert "2026-09-11" in result.detail


def test_no_false_alarm_at_the_closing_bell() -> None:
    """20:05 UTC is five minutes after the 16:00 ET close on 2026-09-14.

    The session just completed, but the provider has not published it and the
    sweep has not run. The grace is what makes this `ok` rather than a daily
    red flash during normal delivery.
    """
    result = _prices(_bar_date(2026, 9, 11), now=datetime(2026, 9, 14, 20, 5, tzinfo=UTC))
    assert result.status == "ok"


def test_a_holiday_is_never_demanded() -> None:
    """2026-09-07 is Labor Day. On the Tuesday after it, the required session is
    the preceding Friday — a weekday-arithmetic rule would demand the Monday and
    report a healthy corpus stale."""
    required, _ = required_completed_session(datetime(2026, 9, 8, 13, 0, tzinfo=UTC))
    assert required == datetime(2026, 9, 4, tzinfo=UTC).date()
    assert _prices(_bar_date(2026, 9, 4), now=datetime(2026, 9, 8, 13, 0, tzinfo=UTC)).status == "ok"


def test_a_long_outage_still_trips_it() -> None:
    """The alert has to keep working. A week of no bars is stale on any day."""
    result = _prices(_bar_date(2026, 9, 4), now=datetime(2026, 9, 14, 13, 0, tzinfo=UTC))
    assert result.status == "stale"


# ---------------------------------------------------------------------------
# Contract shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status_date", [_bar_date(2026, 9, 11), _bar_date(2026, 9, 10)])
def test_max_age_is_none_on_both_verdicts(status_date: datetime) -> None:
    """⚠ Reported as None rather than as the grace, on BOTH branches.

    `LayerHealth.max_age` is the tolerance the verdict used. Reporting a number
    the verdict did not use would put `ok` next to `age > max_age` on the admin
    panel, which reads as a bug. `LayerHealthResponse.max_age_seconds` is
    already `float | None`.
    """
    result = _prices(status_date, now=datetime(2026, 9, 14, 13, 0, tzinfo=UTC))
    assert result.max_age is None
    assert result.age is not None  # the real age is still reported


def test_an_empty_price_table_is_still_empty_not_stale() -> None:
    """The `empty` branch runs before any freshness rule and must stay that way:
    `BootstrapProgress` treats a non-empty layer as evidence bootstrap finished,
    so turning `empty` into `stale` would hide the bootstrap UI."""
    result = _prices(None, now=datetime(2026, 9, 14, 13, 0, tzinfo=UTC))
    assert result.status == "empty"


def test_a_naive_bar_timestamp_is_read_as_utc() -> None:
    """psycopg can hand back a naive datetime for a `date::timestamptz` cast.
    The session comparison must not depend on which."""
    naive = datetime(2026, 9, 11)
    assert naive.tzinfo is None
    result = _prices(naive, now=datetime(2026, 9, 14, 13, 0, tzinfo=UTC))
    assert result.status == "ok"


def test_required_session_moves_with_the_clock_it_is_given() -> None:
    """The captured-clock contract: no wall-clock read inside the resolver, so a
    caller can evaluate every layer at one instant."""
    early = required_completed_session(datetime(2026, 9, 14, 13, 0, tzinfo=UTC))[0]
    # ⚠ 21:00 UTC, not 13:00. One grace back from 2026-09-15 13:00 UTC is
    # 2026-09-14 09:00 ET — Monday has not closed yet, so the required session
    # is still the Friday. The boundary is the 16:00 ET close, not midnight,
    # and a test written at 13:00 would assert the clock had not moved.
    later = required_completed_session(datetime(2026, 9, 15, 21, 0, tzinfo=UTC))[0]
    assert early == datetime(2026, 9, 11, tzinfo=UTC).date()
    assert later == datetime(2026, 9, 14, tzinfo=UTC).date()


def test_a_naive_now_is_refused_rather_than_guessed() -> None:
    """`latest_completed_us_session` raises on a naive instant, and that refusal
    must not be swallowed — a guessed timezone moves the session boundary."""
    with pytest.raises(ValueError):
        required_completed_session(datetime(2026, 9, 14, 13, 0))


def test_the_grace_is_subtracted_not_added() -> None:
    """A sign error here would DEMAND a session that has not closed yet, turning
    the fix into a daily false alarm — the defect inverted. Pinned by comparing
    against the ungraced session at an instant where the two differ."""
    from app.services.market_calendar import latest_completed_us_session

    now = datetime(2026, 9, 14, 20, 5, tzinfo=UTC)  # just after Monday's close
    ungraced = latest_completed_us_session(now)
    graced, _ = required_completed_session(now)
    assert ungraced == datetime(2026, 9, 14, tzinfo=UTC).date()
    assert graced == datetime(2026, 9, 11, tzinfo=UTC).date()
    assert graced < ungraced


def test_an_unpartitioned_layer_raises_rather_than_defaulting(monkeypatch: pytest.MonkeyPatch) -> None:
    """The RuntimeError branch, exercised. Without it a layer in neither set
    would raise KeyError from a dict subscript, or worse, acquire a default."""
    monkeypatch.delitem(_STALENESS_THRESHOLDS, "scores")
    with pytest.raises(RuntimeError, match="neither a staleness threshold nor a session anchor"):
        check_layer_staleness(
            _conn_with_latest(datetime(2026, 9, 14, tzinfo=UTC)),
            "scores",
            now=datetime(2026, 9, 14, 13, 0, tzinfo=UTC),
        )
