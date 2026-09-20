"""#2840 arm 2 step 1 item 1 — the daily-composition rule the forward instrument needs.

Pure-logic only, no DB. The census's DB half is one read-only transaction; the decisions all
live in these helpers, so they are the part worth pinning.

⚠ A FIXTURE BUILT FROM ``session_slots`` CANNOT FALSIFY ``session_slots``. Codex checkpoint 1
found the first version of this file reproduced the census's own assumptions throughout, so
the geometry tests below assert LITERAL UTC instants and cross-check against the harvester's
private ``_session_bounds`` — the function this module deliberately duplicates.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from app.services.indicator_series import BarSeries
from app.services.market_calendar import us_market_specials, us_market_status
from app.services.strategies import s4_volatility_compression_breakout as s4
from app.services.strategy_intraday_harvest import _session_bounds
from app.services.strategy_observation_storage import INTRADAY_TIERS
from app.services.technical_analysis import OHLCVRow
from app.workers.scheduler import SCHEDULED_JOBS
from scripts.census_2840_forward_daily_provenance import (
    absence_attribution,
    build_corpus,
    catchup_capture_days,
    compose_day,
    expected_bars,
    first_evaluable_series_length,
    five_minute_slot,
    harvest_run_gaps,
    next_session_open_utc,
    nominality_bucket,
    percentile,
    prior_close_open_matches,
    range_escapes,
    relative_difference,
    scan_deadline_utc,
    session_close_utc,
    session_slots,
)

_NY = ZoneInfo("America/New_York")

#: A Friday, full session, inside US eastern DAYLIGHT time (UTC-4).
_FULL_DAY = date(2026, 9, 18)


def _bar(stamp: datetime, *, captured_at: datetime | None = None, **fields: float | None) -> dict[str, object]:
    row: dict[str, object] = {
        "bar_time": stamp,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 100.0,
        "captured_at": captured_at if captured_at is not None else stamp + timedelta(minutes=35),
    }
    row.update(fields)
    return row


def _full_session(day: date = _FULL_DAY, *, captured_at: datetime | None = None) -> list[dict[str, object]]:
    return [_bar(slot, captured_at=captured_at) for slot in session_slots(day)]


class TestSessionGeometry:
    def test_the_slot_grid_is_asserted_as_literal_utc_instants(self) -> None:
        """No reference to ``session_slots``' own arithmetic — the instants are written out."""
        slots = session_slots(_FULL_DAY)
        assert len(slots) == 13
        assert slots[0] == datetime(2026, 9, 18, 13, 30, tzinfo=UTC)
        assert slots[1] == datetime(2026, 9, 18, 14, 0, tzinfo=UTC)
        assert slots[-1] == datetime(2026, 9, 18, 19, 30, tzinfo=UTC)

    def test_the_geometry_matches_the_harvester_this_module_duplicates(self) -> None:
        """The copy is checked against the original, which is why the copy is allowed.

        ``_session_bounds`` is private, so the census cannot import it at run time without
        reaching into another module's internals; pinning it here is the alternative to an
        unchecked duplicate.
        """
        for day in (_FULL_DAY, *sorted(us_market_specials(2026).half_days)[:1]):
            bounds = _session_bounds(day)
            assert bounds is not None
            opened, closed = bounds
            assert session_slots(day)[0] == opened
            assert session_close_utc(day) == closed
            width = INTRADAY_TIERS["30m"].minutes_per_bar
            assert session_slots(day)[-1] + timedelta(minutes=width) == closed

    def test_a_half_day_expects_the_13_00_close(self) -> None:
        half_days = sorted(us_market_specials(2026).half_days)
        assert half_days, "the calendar declares no 2026 half day; this test's premise is gone"
        for day in half_days:
            assert expected_bars(day) == 7
            assert session_close_utc(day) == datetime.combine(day, datetime.min.time(), tzinfo=_NY).replace(
                hour=13
            ).astimezone(UTC)

    def test_a_closed_day_expects_nothing(self) -> None:
        assert us_market_status(date(2026, 9, 19)) == "closed"  # Saturday
        assert expected_bars(date(2026, 9, 19)) == 0
        assert session_slots(date(2026, 9, 19)) == ()
        assert session_close_utc(date(2026, 9, 19)) is None

    def test_the_expectation_FOLLOWS_the_tier_table_rather_than_a_typed_13(self) -> None:
        thirty = INTRADAY_TIERS["30m"].minutes_per_bar
        five = INTRADAY_TIERS["5m"].minutes_per_bar
        assert expected_bars(_FULL_DAY, timeframe="5m") == expected_bars(_FULL_DAY) * (thirty // five)

    def test_the_next_session_open_skips_a_weekend_and_a_holiday(self) -> None:
        # 2026-09-18 Friday -> 2026-09-21 Monday.
        assert next_session_open_utc(_FULL_DAY) == datetime(2026, 9, 21, 13, 30, tzinfo=UTC)
        # 2026-09-04 Friday -> 2026-09-08 Tuesday, because 09-07 is Labor Day.
        assert us_market_status(date(2026, 9, 7)) == "closed"
        assert next_session_open_utc(date(2026, 9, 4)) == datetime(2026, 9, 8, 13, 30, tzinfo=UTC)

    def test_no_session_inside_the_horizon_returns_none_rather_than_a_guess(self) -> None:
        assert next_session_open_utc(_FULL_DAY, horizon_days=1) is None


class TestComposeDay:
    def test_a_complete_session_composes_from_the_first_and_last_SLOT(self) -> None:
        slots = session_slots(_FULL_DAY)
        rows = [
            _bar(slot, open=10.0 + index, high=20.0 + index, low=5.0 - index / 100, close=15.0 + index, volume=7.0)
            for index, slot in enumerate(slots)
        ]
        composed = compose_day(_FULL_DAY, list(reversed(rows)))  # input order must not matter
        assert composed.status == "complete"
        assert composed.bar is not None
        open_, high, low, close, volume = composed.bar
        assert open_ == 10.0, "the open must come from the 09:30 SLOT, not from the first row handed in"
        assert close == 15.0 + (len(slots) - 1), "the close must come from the last SLOT"
        assert high == 20.0 + (len(slots) - 1)
        assert low == pytest.approx(5.0 - (len(slots) - 1) / 100)
        assert volume == pytest.approx(7.0 * len(slots))

    def test_a_short_session_REFUSES_rather_than_composing_a_partial_bar(self) -> None:
        composed = compose_day(_FULL_DAY, _full_session()[:-1])
        assert composed.status == "partial"
        assert composed.bar is None

    def test_a_DUPLICATED_slot_does_not_pass_as_a_complete_session(self) -> None:
        """The exact defect Codex reproduced against a count-only check.

        Thirteen rows, twelve distinct slots: a count check calls this complete and composes a
        close from whichever row sorted last.
        """
        rows = _full_session()[:-1]
        rows.append(_bar(session_slots(_FULL_DAY)[0], close=999.0))
        assert len(rows) == expected_bars(_FULL_DAY)
        composed = compose_day(_FULL_DAY, rows)
        assert composed.status == "partial"
        assert composed.bar is None
        assert composed.off_grid == 0, "a duplicate of a real slot is on the grid; it is the MISSING slot that refuses"

    def test_a_session_shifted_OFF_the_grid_is_refused_and_counted(self) -> None:
        rows = [_bar(slot - timedelta(hours=1)) for slot in session_slots(_FULL_DAY)]
        composed = compose_day(_FULL_DAY, rows)
        assert composed.status == "partial"
        assert composed.bar is None
        assert composed.off_grid >= 1

    def test_an_EXTRA_row_alongside_a_full_session_is_refused_not_ignored(self) -> None:
        """⚠ Codex checkpoint 2: the slot-SET check alone passed this.

        Every expected slot is present, so ``seen.keys() == wanted`` holds and the extra row
        never enters ``seen`` — the session composed with ``off_grid > 0``. It is reachable
        through the generic writer, whose 30m per-day cap is 13 while a half day expects 7.
        """
        rows = _full_session()
        rows.append(_bar(session_slots(_FULL_DAY)[0] + timedelta(minutes=7)))
        composed = compose_day(_FULL_DAY, rows)
        assert composed.off_grid == 1
        assert composed.bar is None
        assert composed.status == "partial"

    def test_a_NON_FINITE_price_is_refused_rather_than_composed(self) -> None:
        """⚠ A documented repo invariant, not a hypothetical.

        `docs/review-prevention-log.md` records that PostgreSQL orders `NaN` ABOVE every other
        value, so `open > 0` and the OHLC-shape CHECK are both satisfied by it and the stored
        row is reachable — and that "the Python mirror must refuse it too". A `nan` close would
        otherwise compose a complete-looking session and compare as clearing the price gate.
        """
        for field in ("open", "high", "low", "close"):
            rows = _full_session()
            rows[6][field] = float("nan")
            composed = compose_day(_FULL_DAY, rows)
            assert composed.non_finite == 1, field
            assert composed.bar is None, field
        rows = _full_session()
        rows[2]["high"] = float("inf")
        assert compose_day(_FULL_DAY, rows).bar is None

    def test_an_empty_session_is_absent_not_partial(self) -> None:
        composed = compose_day(_FULL_DAY, [])
        assert composed.status == "absent"
        assert composed.settled_before_next_open is False

    def test_a_non_session_day_is_never_composed(self) -> None:
        composed = compose_day(date(2026, 9, 19), [])
        assert composed.status == "not_a_session"
        assert composed.expected == 0

    def test_a_missing_volume_PROPAGATES_instead_of_summing_to_a_smaller_number(self) -> None:
        rows = _full_session()
        rows[4]["volume"] = None
        composed = compose_day(_FULL_DAY, rows)
        assert composed.status == "complete"
        assert composed.bar is not None
        assert composed.bar[4] is None

    def test_nominality_and_availability_are_DIFFERENT_deadlines(self) -> None:
        """⚠ The defect Codex checkpoint 2 found: an earlier version treated them as one.

        The scan fires at 06:45 UTC and the next open is 13:30 UTC in EDT, so there is a
        window in which a capture is nominal-by-arithmetic and already too late to have been
        used. ``composable_and_settled`` must require BOTH.
        """
        next_open = next_session_open_utc(_FULL_DAY)
        deadline = scan_deadline_utc(_FULL_DAY)
        assert next_open is not None
        assert deadline < next_open, "the premise of this test is that the scan deadline is the stricter one"

        between = compose_day(_FULL_DAY, _full_session(captured_at=next_open - timedelta(minutes=1)))
        assert between.status == "complete"
        assert between.settled_before_next_open is True
        assert between.available_before_next_scan is False
        assert between.composable_and_settled is False, "nominal is not sufficient; it also has to be in time"

        in_time = compose_day(_FULL_DAY, _full_session(captured_at=deadline - timedelta(minutes=1)))
        assert in_time.composable_and_settled is True

        late = compose_day(_FULL_DAY, _full_session(captured_at=next_open + timedelta(minutes=1)))
        assert late.settled_before_next_open is False
        assert late.composable_and_settled is False

    def test_the_scan_deadline_FOLLOWS_the_registry_rather_than_a_typed_06_45(self) -> None:
        spec = next(job for job in SCHEDULED_JOBS if job.name == "strategy_signal_scan")
        deadline = scan_deadline_utc(_FULL_DAY)
        assert (deadline.hour, deadline.minute) == (spec.cadence.hour, spec.cadence.minute)
        assert deadline.date() == _FULL_DAY + timedelta(days=1)
        assert deadline.tzinfo == UTC


class TestNominality:
    def test_a_capture_before_the_next_open_had_no_opportunity_to_be_rebased(self) -> None:
        bar_time = datetime(2026, 9, 18, 15, 30, tzinfo=UTC)
        assert nominality_bucket(bar_time, datetime(2026, 9, 18, 20, 5, tzinfo=UTC)) == "before_next_open"

    def test_a_weekend_capture_is_still_before_the_next_open(self) -> None:
        """The boundary is an OPEN, not a calendar day — the earlier draft's buckets were not.

        A Friday bar captured on Saturday has had no session open in between, so no split
        could have taken effect.
        """
        bar_time = datetime(2026, 9, 18, 15, 30, tzinfo=UTC)
        assert nominality_bucket(bar_time, datetime(2026, 9, 19, 12, 0, tzinfo=UTC)) == "before_next_open"

    def test_each_intervening_open_is_counted_as_an_opportunity(self) -> None:
        bar_time = datetime(2026, 9, 14, 14, 0, tzinfo=UTC)
        assert nominality_bucket(bar_time, datetime(2026, 9, 15, 14, 0, tzinfo=UTC)) == "after_1_opens"
        assert nominality_bucket(bar_time, datetime(2026, 9, 16, 14, 0, tzinfo=UTC)) == "after_2_opens"
        assert nominality_bucket(bar_time, datetime(2026, 9, 30, 14, 0, tzinfo=UTC)) == "after_3_opens"

    def test_a_capture_before_the_bar_completed_is_impossible_and_says_so(self) -> None:
        bar_time = datetime(2026, 9, 18, 14, 0, tzinfo=UTC)
        assert nominality_bucket(bar_time, bar_time + timedelta(minutes=10)) == "impossible"


class TestWarmUp:
    def test_the_needed_length_is_MEASURED_and_exceeds_the_declared_constant(self) -> None:
        """⚠ 113 is the first evaluable INDEX; the final bar has no fill successor.

        The census subtracts this figure, so a test that merely re-typed 115 would not catch
        the class of error the earlier draft made — it asserts the RELATION to the constant.
        """
        needed = first_evaluable_series_length()
        assert needed == 115
        assert needed > s4.WARMUP_BARS + 1

    def test_one_bar_fewer_yields_no_evaluable_verdict_at_all(self) -> None:
        for length, expected_evaluable in ((114, 0), (115, 1)):
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
            assert sum(1 for signal in signals if signal.verdict != "not_evaluable") == expected_evaluable


class TestAbsenceAttribution:
    def test_a_slot_missing_at_30m_but_present_at_5m_is_a_resolution_specific_loss(self) -> None:
        slots = session_slots(_FULL_DAY)
        attribution = absence_attribution(slots, slots[:11], {slots[11]})
        assert attribution == {"slots": 13, "with_30m": 11, "missing_30m_5m_present": 1, "missing_both": 1}

    def test_missing_both_is_NOT_counted_as_evidence_that_nothing_traded(self) -> None:
        slots = session_slots(_FULL_DAY)
        attribution = absence_attribution(slots, (), ())
        assert attribution["missing_both"] == 13
        assert attribution["missing_30m_5m_present"] == 0

    def test_a_finer_bar_buckets_into_its_containing_slot_without_a_session_timezone(self) -> None:
        assert five_minute_slot(datetime(2026, 9, 18, 13, 34, tzinfo=UTC)) == datetime(2026, 9, 18, 13, 30, tzinfo=UTC)
        assert five_minute_slot(datetime(2026, 9, 18, 14, 0, tzinfo=UTC)) == datetime(2026, 9, 18, 14, 0, tzinfo=UTC)
        assert five_minute_slot(datetime(2026, 9, 18, 9, 55, tzinfo=_NY)) == datetime(2026, 9, 18, 13, 30, tzinfo=UTC)


class TestCaptureShape:
    def test_only_multi_session_capture_days_are_reported_and_biggest_first(self) -> None:
        live = _full_session()
        capture = datetime(2026, 9, 13, 16, 0, tzinfo=UTC)
        backfill = [
            _bar(slot, captured_at=capture)
            for day in (date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))
            for slot in session_slots(day)
        ]
        catchups = catchup_capture_days(live + backfill)
        assert len(catchups) == 1, "a live capture writes one session on its own day and must not be reported"
        capture_day, count, first, last = catchups[0]
        assert (capture_day, count, first, last) == (date(2026, 9, 13), 39, date(2026, 8, 26), date(2026, 8, 28))

    def test_a_run_of_sessions_with_no_job_row_is_reported_as_one_gap(self) -> None:
        rows = [
            {"run_day": date(2026, 9, 4), "status": "success", "runs": 1},
            {"run_day": date(2026, 9, 14), "status": "success", "runs": 1},
        ]
        # 09-07 is Labor Day, 09-05/06 and 09-12/13 are weekends.
        assert harvest_run_gaps(rows) == [(date(2026, 9, 8), date(2026, 9, 11), 4)]

    def test_no_rows_yields_no_gap_rather_than_a_span_of_everything(self) -> None:
        assert harvest_run_gaps([]) == []

    def test_an_outage_at_the_END_of_the_span_is_not_hidden(self) -> None:
        """⚠ The defect of deriving bounds from the rows whose absence you are measuring.

        A collector that stops before the cutoff leaves no row to mark the span's end, so the
        unbounded form reports no gap at all and §1c mislabels those sessions as residuals
        "with a run row present".
        """
        rows = [{"run_day": date(2026, 9, 14), "status": "success", "runs": 1}]
        assert harvest_run_gaps(rows) == [], "the unbounded form cannot see past its last row"
        bounded = harvest_run_gaps(rows, first=date(2026, 9, 14), last=date(2026, 9, 18))
        assert bounded == [(date(2026, 9, 15), date(2026, 9, 18), 4)]

    def test_an_outage_at_the_START_of_the_span_is_not_hidden_either(self) -> None:
        rows = [{"run_day": date(2026, 9, 18), "status": "success", "runs": 1}]
        bounded = harvest_run_gaps(rows, first=date(2026, 9, 14), last=date(2026, 9, 18))
        assert bounded == [(date(2026, 9, 14), date(2026, 9, 17), 4)]


class TestReferenceSeriesWitnesses:
    def test_only_CALENDAR_ADJACENT_sessions_are_paired(self) -> None:
        """A weekend-spanning pair is adjacent; a pair with a missing session in between is not."""
        adjacent = [
            {"price_date": date(2026, 9, 17), "open": 10.0, "close": 11.0},
            {"price_date": date(2026, 9, 18), "open": 11.0, "close": 12.0},
        ]
        assert prior_close_open_matches(adjacent) == (1, 1)
        across_weekend = [
            {"price_date": date(2026, 9, 18), "open": 10.0, "close": 11.0},
            {"price_date": date(2026, 9, 21), "open": 11.0, "close": 12.0},
        ]
        assert prior_close_open_matches(across_weekend) == (1, 1)
        with_a_hole = [
            {"price_date": date(2026, 9, 16), "open": 10.0, "close": 11.0},
            {"price_date": date(2026, 9, 18), "open": 11.0, "close": 12.0},
        ]
        assert prior_close_open_matches(with_a_hole) == (0, 0), "09-17 is a session, so these are not adjacent"

    def test_a_null_side_leaves_the_pair_out_of_BOTH_terms(self) -> None:
        rows = [
            {"price_date": date(2026, 9, 17), "open": 10.0, "close": None},
            {"price_date": date(2026, 9, 18), "open": 11.0, "close": 12.0},
        ]
        assert prior_close_open_matches(rows) == (0, 0)

    def test_a_reference_reaching_outside_the_rth_range_is_an_escape(self) -> None:
        composed = (10.0, 11.0, 9.0, 10.5, 100.0)
        assert range_escapes(composed, {"high": 11.5, "low": 9.0}) is True
        assert range_escapes(composed, {"high": 11.0, "low": 8.5}) is True
        assert range_escapes(composed, {"high": 11.0, "low": 9.0}) is False

    def test_an_unknown_extreme_is_NONE_rather_than_no_escape(self) -> None:
        composed = (10.0, 11.0, 9.0, 10.5, 100.0)
        assert range_escapes(composed, {"high": None, "low": 9.0}) is None
        assert range_escapes(composed, {"high": 11.0, "low": None}) is None

    def test_a_zero_reference_yields_no_relative_difference(self) -> None:
        assert relative_difference(10.0, 0.0) is None
        assert relative_difference(10.5, 10.0) == pytest.approx(0.05)


class TestPercentile:
    def test_the_median_of_four_values_is_the_SECOND_one(self) -> None:
        """⚠ The exact case the earlier helper got wrong.

        ``round(0.5 * 3)`` is 2 under Python's tie-to-even, which indexes the THIRD element
        and reports 3 as the median of ``[1, 2, 3, 4]``.
        """
        assert percentile([1.0, 2.0, 3.0, 4.0], 0.5) == 2.0

    def test_fraction_one_is_the_maximum_and_an_empty_input_is_none(self) -> None:
        assert percentile([3.0, 1.0, 2.0], 1.0) == 3.0
        assert percentile([], 0.5) is None

    def test_the_input_is_not_required_to_be_sorted(self) -> None:
        assert percentile([4.0, 1.0, 3.0, 2.0], 0.5) == 2.0


class TestBuildCorpus:
    def _member(self, symbol: str, instrument_ids: list[int], types: list[int]) -> dict[str, object]:
        return {
            "ordinal": 1,
            "symbol": symbol,
            "purpose": "test",
            "instrument_ids": instrument_ids,
            "instrument_type_ids": types,
        }

    def test_an_ambiguous_symbol_is_a_resolution_error_not_a_silent_pick(self) -> None:
        """Mirrors ``strategy_intraday_harvest._active_members``, which refuses the same case."""
        corpora = build_corpus(
            [self._member("DUP", [11, 12], [5, 5])],
            [],
            as_of=datetime(2026, 9, 20, tzinfo=UTC),
        )
        assert corpora[0].resolution_error == "expected one tradable instrument, found 2"
        assert corpora[0].days == ()

    def test_a_session_whose_close_has_not_passed_is_excluded_entirely(self) -> None:
        rows = [row | {"instrument_id": 1} for row in _full_session()]
        mid_session = datetime(2026, 9, 18, 17, 0, tzinfo=UTC)
        during = build_corpus([self._member("X", [1], [5])], rows, as_of=mid_session)
        assert during[0].days == (), "a running session must not be recorded as permanently short"
        after = build_corpus([self._member("X", [1], [5])], rows, as_of=datetime(2026, 9, 20, tzinfo=UTC))
        assert [day.day for day in after[0].days] == [_FULL_DAY]

    def test_the_post_activation_window_is_counted_separately_from_the_whole_span(self) -> None:
        rows = [row | {"instrument_id": 1} for row in _full_session(date(2026, 9, 17))]
        rows += [row | {"instrument_id": 1} for row in _full_session(_FULL_DAY)]
        corpus = build_corpus([self._member("X", [1], [5])], rows, as_of=datetime(2026, 9, 20, tzinfo=UTC))[0]
        assert corpus.complete == 2
        assert corpus.since(_FULL_DAY) == (1, 1, 1)
        assert corpus.since(date(2026, 9, 17))[0] == 2
