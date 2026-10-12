"""#3612 slice 2 — the marked daily P&L figure and its refusals (spec Design 3 and 4). Pure tier."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from app.services import strategy_paper_executor
from app.services.engine_pot_risk import (
    DailyAnchor,
    DailyMark,
    LedgerEvent,
    _price_book,
    daily_anchor_session,
    marked_daily_pnl,
)
from app.services.strategy_engine_capital import EngineCapitalObservationError

NY = ZoneInfo("America/New_York")
S_AT = datetime(2026, 10, 12, 20, 5, tzinfo=UTC)  # Monday's S, 16:05 New York
OBS = datetime(2026, 10, 13, 14, 0, tzinfo=UTC)  # Tuesday 10:00 New York
DECIDED = OBS + timedelta(seconds=5)
OPENED = datetime(2026, 10, 1, 14, 0, tzinfo=UTC)
CLAIMED = OPENED + timedelta(minutes=1)
MAX_AGE = 60


def mark(units: str = "10", pnl: str = "0", at: datetime | None = None, fees: str | None = "0") -> DailyMark:
    return DailyMark(Decimal(units), Decimal(pnl), at, None if fees is None else Decimal(fees))


def s_mark(units: str = "10", pnl: str = "100", fees: str | None = "0") -> DailyMark:
    return mark(units, pnl, S_AT - timedelta(seconds=2), fees)


def live_mark(units: str = "10", pnl: str = "-400", fees: str | None = "0") -> DailyMark:
    return mark(units, pnl, OBS - timedelta(seconds=1), fees)


def anchor(marks: dict[int, DailyMark], *, declared: int | None = None, currency: str | None = "USD") -> DailyAnchor:
    return DailyAnchor(S_AT, 1, currency, len(marks) if declared is None else declared, marks)


def row(position_id: int | None, *, claimed: datetime = CLAIMED, status: str = "open") -> tuple[Any, ...]:
    return (position_id or 999, status, 1, position_id, claimed, None)


def ev(kind: str, at: datetime, units: str = "10") -> LedgerEvent:
    return LedgerEvent(kind, at, Decimal(units))


def run(
    *,
    anchor_: DailyAnchor | None = None,
    live: dict[int, DailyMark] | None = None,
    book: list[tuple[Any, ...]] | None = None,
    events: dict[int, list[LedgerEvent]] | None = None,
    decided_at: datetime = DECIDED,
    max_age: int = MAX_AGE,
    no_anchor: bool = False,
) -> Decimal:
    """Default: position 1 held 10 units, +100 at S, −400 now."""
    return marked_daily_pnl(
        anchor=None if no_anchor else (anchor_ or anchor({1: s_mark()})),
        live={1: live_mark()} if live is None else live,
        book=[row(1)] if book is None else book,
        events={1: [ev("open", OPENED)]} if events is None else events,
        observed_at=OBS,
        decided_at=decided_at,
        max_mark_age_seconds=max_age,
    )


def refuses(code: str, **kwargs: Any) -> None:
    with pytest.raises(EngineCapitalObservationError) as raised:
        run(**kwargs)
    assert raised.value.reason_code == code


# --------------------------------------------------------------------------
# Admitted cases
# --------------------------------------------------------------------------


def test_a_held_position_contributes_its_broker_change_since_the_anchor() -> None:
    assert run() == Decimal("-500")


def test_a_held_short_contributes_its_broker_change_whatever_its_direction() -> None:
    # The figure subtracts the broker's own P&L from itself; direction never enters.
    assert run(anchor_=anchor({1: s_mark(pnl="-30")}), live={1: live_mark(pnl="45")}) == Decimal("75")


def test_a_position_opened_in_the_window_contributes_its_whole_unrealised() -> None:
    opened = S_AT + timedelta(hours=1)
    assert run(
        anchor_=anchor({}),
        live={2: live_mark(units="5")},
        book=[row(2, claimed=opened + timedelta(seconds=30))],
        events={2: [ev("open", opened, "5")]},
    ) == Decimal("-400")


def test_a_historical_position_contributes_nothing() -> None:
    closed = OPENED + timedelta(days=2)
    assert run(
        anchor_=anchor({}), live={}, book=[row(3)], events={3: [ev("open", OPENED), ev("close", closed)]}
    ) == Decimal("0")


def test_positions_outside_the_engine_book_are_ignored() -> None:
    assert run(
        anchor_=anchor({1: s_mark(), 50: s_mark(pnl="9999")}), live={1: live_mark(), 60: live_mark(pnl="-9999")}
    ) == Decimal("-500")


def test_unchanged_non_zero_fees_on_both_sides_pass() -> None:
    assert run(anchor_=anchor({1: s_mark(fees="-1.5")}), live={1: live_mark(fees="-1.5")}) == Decimal("-500")


def test_a_genuinely_empty_anchor_and_book_pass() -> None:
    assert run(anchor_=anchor({}, declared=0), live={}, book=[], events={}) == Decimal("0")


# --------------------------------------------------------------------------
# Refusals, one per case-table cell
# --------------------------------------------------------------------------

AFTER_S = S_AT + timedelta(hours=1)


@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        pytest.param(
            {"events": {1: [ev("open", OPENED), ev("close", AFTER_S, "5")]}, "live": {1: live_mark(units="5")}},
            "engine_pot_daily_close_in_window",
            id="held-partial-close",
        ),
        pytest.param(
            {"events": {1: [ev("open", OPENED), ev("close", AFTER_S)]}, "live": {}},
            "engine_pot_daily_close_in_window",
            id="held-full-close",
        ),
        pytest.param(
            {
                "anchor_": anchor({}),
                "live": {2: live_mark(units="5")},
                "book": [row(2, claimed=AFTER_S)],
                "events": {2: [ev("open", AFTER_S, "10"), ev("close", AFTER_S + timedelta(hours=1), "5")]},
            },
            "engine_pot_daily_close_in_window",
            id="opened-then-partly-closed",
        ),
        pytest.param(
            {
                "anchor_": anchor({}),
                "live": {},
                "book": [row(2, claimed=AFTER_S)],
                "events": {2: [ev("open", AFTER_S), ev("close", AFTER_S + timedelta(hours=1))]},
            },
            "engine_pot_daily_close_in_window",
            id="round-trip",
        ),
        pytest.param({"live": {}}, "engine_pot_daily_event_unwitnessed", id="closed-without-close-event"),
        pytest.param(
            {"anchor_": anchor({}), "live": {2: live_mark()}, "book": [row(2)], "events": {2: []}},
            "engine_pot_daily_event_unwitnessed",
            id="opened-without-open",
        ),
        pytest.param(
            {
                "anchor_": anchor({}),
                "live": {2: live_mark(units="5")},
                "book": [row(2, claimed=AFTER_S)],
                "events": {2: [ev("open", AFTER_S, "6")]},
            },
            "engine_pot_daily_event_unwitnessed",
            id="opened-units-differ",
        ),
        pytest.param(
            {"live": {1: live_mark(units="20")}}, "engine_pot_daily_event_unwitnessed", id="held-split-no-event"
        ),
        pytest.param(
            {"live": {1: live_mark(fees="-0.4")}}, "engine_pot_daily_accounting_unverified", id="held-fees-changed"
        ),
        pytest.param(
            {"live": {1: live_mark(fees=None)}}, "engine_pot_daily_accounting_unverified", id="held-fees-null"
        ),
        pytest.param(
            {
                "anchor_": anchor({}),
                "live": {2: live_mark(units="5", fees="-0.1")},
                "book": [row(2, claimed=AFTER_S)],
                "events": {2: [ev("open", AFTER_S, "5")]},
            },
            "engine_pot_daily_accounting_unverified",
            id="opened-carries-fees",
        ),
        pytest.param(
            {"anchor_": anchor({}), "live": {}, "book": [row(3, claimed=AFTER_S)], "events": {3: []}},
            "engine_pot_daily_event_unwitnessed",
            id="historical-claimed-in-window",
        ),
        pytest.param(
            {"book": [row(1), row(None, status="open")]},
            "engine_capital_population_incomplete",
            id="eligible-trade-without-ownership",
        ),
    ],
)
def test_each_unadmitted_case_refuses_by_name(kwargs: dict[str, Any], code: str) -> None:
    refuses(code, **kwargs)


# --------------------------------------------------------------------------
# Anchor, marks, reconciliation, straddle, the decision window
# --------------------------------------------------------------------------


def test_no_anchor_refuses() -> None:
    refuses("engine_pot_daily_anchor_missing", no_anchor=True)


def test_an_anchor_short_of_its_declared_marks_refuses() -> None:
    refuses("engine_pot_daily_anchor_incomplete", anchor_=anchor({1: s_mark()}, declared=2))


@pytest.mark.parametrize("currency", ["GBP", None])
def test_a_non_usd_or_undocumented_anchor_currency_refuses(currency: str | None) -> None:
    refuses("engine_capital_snapshot_unusable", anchor_=anchor({1: s_mark()}, currency=currency))


@pytest.mark.parametrize(
    "live_at",
    [None, DECIDED - timedelta(seconds=MAX_AGE + 1), OBS + timedelta(seconds=1)],
    ids=["missing", "older-than-bound", "after-receipt"],
)
def test_an_unusable_live_mark_time_refuses(live_at: datetime | None) -> None:
    refuses("engine_pot_daily_mark_stale", live={1: mark(pnl="-400", at=live_at)})


def test_a_live_mark_not_after_its_anchor_mark_refuses() -> None:
    # S captured at 23:59:59 on P, live mark a second earlier.
    late_s = DailyAnchor(S_AT, 1, "USD", 1, {1: mark(pnl="100", at=datetime(2026, 10, 12, 23, 59, 59, tzinfo=NY))})
    live_at = datetime(2026, 10, 12, 23, 59, 58, tzinfo=NY)
    refuses(
        "engine_pot_daily_mark_stale",
        anchor_=late_s,
        live={1: mark(pnl="-400", at=live_at)},
        decided_at=live_at + timedelta(seconds=10),
    )


@pytest.mark.parametrize(("age_at_receipt", "admitted"), [(50, False), (30, True)])
def test_mark_age_is_measured_at_the_decision(age_at_receipt: int, admitted: bool) -> None:
    decided = OBS + timedelta(seconds=20)
    live = {1: mark(pnl="-400", at=OBS - timedelta(seconds=age_at_receipt))}
    if admitted:
        assert run(live=live, decided_at=decided) == Decimal("-500")
    else:
        refuses("engine_pot_daily_mark_stale", live=live, decided_at=decided)


def test_an_anchor_that_already_shows_an_unrecorded_close_refuses() -> None:
    # r8: S holds 5 after a partial close of 10 whose event is not yet recorded; live holds 5.
    refuses(
        "engine_pot_daily_event_unwitnessed", anchor_=anchor({1: s_mark(units="5")}), live={1: live_mark(units="5")}
    )


def test_a_live_side_that_disagrees_with_its_ledger_refuses() -> None:
    refuses(
        "engine_pot_daily_event_unwitnessed",
        anchor_=anchor({1: s_mark(units="10")}),
        events={1: [ev("open", OPENED, "12")]},
    )


@pytest.mark.parametrize(
    "event_at",
    [S_AT - timedelta(seconds=1), OBS - timedelta(milliseconds=500)],
    ids=["anchor-side", "live-side"],
)
def test_an_event_between_a_mark_and_its_receipt_refuses(event_at: datetime) -> None:
    # An open of another lot of the same position cannot exist; any event in the gap refuses.
    events = {1: [ev("open", OPENED), ev("open", event_at, "0.5")]}
    refuses("engine_pot_daily_event_unwitnessed", events=events)


def test_a_close_recorded_between_the_snapshot_and_the_decision_refuses() -> None:
    late_close = OBS + timedelta(seconds=2)
    refuses("engine_pot_daily_close_in_window", events={1: [ev("open", OPENED), ev("close", late_close, "1")]})


def test_a_close_of_a_position_claimed_after_the_snapshot_refuses() -> None:
    claimed = OBS + timedelta(seconds=1)
    refuses(
        "engine_pot_daily_close_in_window",
        book=[row(1), row(4, claimed=claimed)],
        events={1: [ev("open", OPENED)], 4: [ev("open", AFTER_S), ev("close", OBS + timedelta(seconds=3))]},
    )


@pytest.mark.parametrize("units", ["NaN", "Infinity"])
def test_a_non_finite_ledger_event_refuses(units: str) -> None:
    refuses(
        "engine_pot_daily_event_unwitnessed",
        events={1: [ev("open", OPENED), ev("close", OPENED + timedelta(days=1), units)]},
    )


def test_a_close_exactly_at_the_anchor_is_outside_the_window() -> None:
    # Not `close_in_window`; it sits in S's mark-to-receipt gap, so it refuses as unwitnessed.
    refuses(
        "engine_pot_daily_event_unwitnessed",
        anchor_=anchor({1: s_mark(units="5")}),
        live={1: live_mark(units="5")},
        events={1: [ev("open", OPENED), ev("close", S_AT, "5")]},
    )


def test_a_close_exactly_at_the_observation_is_inside_the_window() -> None:
    refuses("engine_pot_daily_close_in_window", events={1: [ev("open", OPENED), ev("close", OBS, "1")]})


# --------------------------------------------------------------------------
# Calendar: D, P
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("observed_ny", "session"),
    [
        (datetime(2026, 10, 12, 10, 0), date(2026, 10, 9)),  # Monday -> Friday
        (datetime(2026, 9, 8, 10, 0), date(2026, 9, 4)),  # Tuesday after Labor Day -> Friday
        (datetime(2026, 11, 30, 10, 0), date(2026, 11, 27)),  # Monday after the 13:00 half day
        (datetime(2026, 3, 8, 3, 30), date(2026, 3, 6)),  # DST starts (Sunday)
        (datetime(2026, 11, 1, 1, 30), date(2026, 10, 30)),  # DST ends (Sunday)
        (datetime(2027, 1, 4, 9, 0), date(2026, 12, 31)),  # across the year boundary and New Year's Day
        (datetime(2026, 4, 6, 9, 0), date(2026, 4, 2)),  # Monday after Good Friday uses Thursday
        (datetime(2026, 10, 9, 23, 59, 59), date(2026, 10, 8)),  # Friday before midnight
        (datetime(2026, 10, 10, 0, 0, 1), date(2026, 10, 9)),  # Saturday after midnight
        (datetime(2026, 10, 10, 23, 59, 59), date(2026, 10, 9)),  # Saturday before midnight
        (datetime(2026, 10, 11, 0, 0, 1), date(2026, 10, 9)),  # Sunday: same P
    ],
)
def test_the_anchor_session_is_the_last_session_completed_before_d(observed_ny: datetime, session: date) -> None:
    assert daily_anchor_session(observed_ny.replace(tzinfo=NY)) == session


def test_a_close_after_fridays_anchor_refuses_through_the_weekend_until_the_next_capture() -> None:
    friday_s = datetime(2026, 10, 9, 20, 5, tzinfo=UTC)
    close = friday_s + timedelta(hours=1)
    for observed in (datetime(2026, 10, 10, 12, 0, tzinfo=UTC), datetime(2026, 10, 12, 14, 0, tzinfo=UTC)):
        assert daily_anchor_session(observed) == date(2026, 10, 9)
        with pytest.raises(EngineCapitalObservationError) as raised:
            marked_daily_pnl(
                anchor=DailyAnchor(friday_s, 1, "USD", 0, {}),
                live={},
                book=[row(1)],
                events={1: [ev("open", OPENED), ev("close", close)]},
                observed_at=observed,
                decided_at=observed,
                max_mark_age_seconds=MAX_AGE,
            )
        assert raised.value.reason_code == "engine_pot_daily_close_in_window"
    # Monday evening's capture is Tuesday's anchor; the close precedes it.
    tuesday = datetime(2026, 10, 13, 14, 0, tzinfo=UTC)
    assert daily_anchor_session(tuesday) == date(2026, 10, 12)
    assert marked_daily_pnl(
        anchor=DailyAnchor(S_AT, 1, "USD", 0, {}),
        live={},
        book=[row(1)],
        events={1: [ev("open", OPENED), ev("close", close)]},
        observed_at=tuesday,
        decided_at=tuesday,
        max_mark_age_seconds=MAX_AGE,
    ) == Decimal("0")


def test_a_close_straddling_an_immutable_anchor_keeps_refusing_the_next_day() -> None:
    # Monday S: mark 16:00:00, close 16:00:01, receipt 16:00:02. Tuesday still measures from it.
    mark_at = datetime(2026, 10, 12, 16, 0, 0, tzinfo=NY)
    straddled = DailyAnchor(mark_at + timedelta(seconds=2), 1, "USD", 1, {1: mark(units="5", pnl="1", at=mark_at)})
    refuses(
        "engine_pot_daily_event_unwitnessed",
        anchor_=straddled,
        live={1: live_mark(units="5")},
        events={1: [ev("open", OPENED), ev("close", mark_at + timedelta(seconds=1), "5")]},
    )


def test_the_realised_mandate_sql_is_byte_identical() -> None:
    """The frozen preview gates (``ai_trial_start_gate``, ``ranking_pot_activation``) import this
    SQL and are hashed into declarations 16 and 23 (spec Design 9)."""
    digest = hashlib.sha256(strategy_paper_executor._MANDATE_OBSERVATION_SQL.encode()).hexdigest()
    assert digest == "a0efff7d36b0587fa6a639632cd9501505919a5c5719a382600808add26baff0"


class _Closes:
    def __init__(self, rows: list[tuple[int, int, int, Decimal]]) -> None:
        self.rows = rows

    def execute(self, *_a: object) -> _Closes:
        return self

    def fetchall(self) -> list[tuple[int, int, int, Decimal]]:
        return self.rows


def test_opposite_infinite_realised_aggregates_refuse_instead_of_raising_invalid_operation() -> None:
    # `realized_pnl_usd` is NUMERIC(20,4) and cannot hold ±Infinity today; the guard is for the
    # aggregate, so it is tested on the reader's own arithmetic.
    released_at = OPENED + timedelta(days=1)
    book = [(1, "closed", 1, 1, CLAIMED, released_at), (2, "closed", 1, 2, CLAIMED, released_at)]
    closes = _Closes([(1, 1, 0, Decimal("Infinity")), (2, 1, 0, Decimal("-Infinity"))])
    with pytest.raises(EngineCapitalObservationError) as raised:
        _price_book(closes, {}, book, OBS)  # type: ignore[arg-type]
    assert raised.value.reason_code == "engine_capital_population_incomplete"
