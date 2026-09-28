"""#1822 slice 1b: the pure half of ``ranking_ablation_reader`` (spec v6 "Prices", "Timeline",
"Termination source", "Grid"). The SQL is pinned in ``test_ranking_ablation_reader_db.py``."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services import ranking_ablation_reader as reader
from app.services.price_quarantine import PROVISIONAL_WINDOW_DAYS, Bar, evaluate_series
from app.services.series_termination import TerminationClass
from app.workers.scheduler import JOB_MORNING_CANDIDATE_REVIEW


def _at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second, tzinfo=UTC)


def _bar(day: date, close: str, *, volume: str | None = "1000", high: str | None = None) -> Bar:
    price = Decimal(close)
    return Bar(
        price_date=day,
        open=price,
        high=Decimal(high) if high is not None else price * Decimal("1.01"),
        low=price * Decimal("0.99"),
        close=price,
        volume=None if volume is None else Decimal(volume),
    )


def _row(
    scored_at: datetime,
    iid: int,
    *,
    type_description: str | None = "Stocks",
    currency: str | None = "USD",
    rank: int | None = 1,
    penalties: object = (),
) -> tuple[object, ...]:
    return (
        scored_at,
        iid,
        rank,
        Decimal("0.5"),
        Decimal("0.4"),
        Decimal("0.3"),
        Decimal("0.2"),
        Decimal("0.1"),
        Decimal("0.5"),
        Decimal("0.35"),
        Decimal("0.35"),
        list(penalties) if isinstance(penalties, tuple) else penalties,
        type_description,
        currency,
        f"S{iid}",
    )


def test_witness_job_is_the_scheduler_constant() -> None:
    assert reader.WITNESS_JOB == JOB_MORNING_CANDIDATE_REVIEW


# --- Population -----------------------------------------------------------


def test_lane_excludes_non_stock_then_non_usd() -> None:
    assert reader.lane_exclusion("Stocks", "USD") is None
    assert reader.lane_exclusion("ETF", "USD") == "non_stock"
    assert reader.lane_exclusion(None, "USD") == "non_stock"
    assert reader.lane_exclusion("Stocks", "GBP") == "non_usd"
    assert reader.lane_exclusion("ETF", "GBP") == "non_stock"


def test_population_counts_each_exclusion_by_row_and_name() -> None:
    first, second = _at(13), _at(14)
    population = reader.build_population(
        [
            _row(first, 1),
            _row(first, 2, type_description="ETF"),
            _row(second, 2, type_description="ETF"),
            _row(first, 3, currency="EUR"),
            _row(first, 4, rank=None),
            _row(first, 5, penalties=None),
            _row(second, 1, penalties=[{"kind": "reward", "addition": 0.03}]),
        ]
    )
    assert set(population.runs) == {first, second}
    assert set(population.runs[first]) == {1}
    assert population.runs[second][1].row.additions == pytest.approx(0.03)
    assert population.runs[first][1].raw_total == pytest.approx(0.35)
    assert population.excluded_rows == {
        "non_stock": 2,
        "non_usd": 1,
        "null_rank": 1,
        "unparseable_penalties_json": 1,
    }
    assert population.excluded_names["non_stock"] == 1
    # Every read symbol is kept, excluded or not (the Q-suffix snapshot needs it).
    assert population.symbols[2] == "S2"


# --- Witness --------------------------------------------------------------


def test_witness_is_the_one_covering_run_bounds_inclusive() -> None:
    window = reader.JobWindow(started_at=_at(13), finished_at=_at(13, 5))
    assert reader.witness(_at(13), [window]) == _at(13, 5)
    assert reader.witness(_at(13, 5), [window]) == _at(13, 5)
    assert reader.witness(_at(13, 5, 1), [window]) == "no_witness"


def test_two_covering_runs_are_ambiguous_not_resolved() -> None:
    windows = [
        reader.JobWindow(started_at=_at(13), finished_at=_at(13, 10)),
        reader.JobWindow(started_at=_at(13, 2), finished_at=_at(13, 4)),
    ]
    assert reader.witness(_at(13, 3), windows) == "ambiguous_witness"


def test_witness_runs_splits_known_from_refused() -> None:
    windows = [reader.JobWindow(started_at=_at(13), finished_at=_at(13, 1))]
    result = reader.witness_runs([_at(15), _at(13)], windows)
    assert [(run.scored_at, run.known_at) for run in result.runs] == [(_at(13), _at(13, 1))]
    assert result.refused == {_at(15): "no_witness"}


# --- Cutoff ---------------------------------------------------------------


def _weekdays(start: date, end: date) -> list[date]:
    days = (start + timedelta(days=offset) for offset in range((end - start).days + 1))
    return [day for day in days if day.weekday() < 5]


def test_cutoff_is_strictly_outside_the_provisional_window() -> None:
    sessions = _weekdays(date(2026, 9, 1), date(2026, 9, 30))
    readout = date(2026, 9, 28)  # Monday; readout − 5 = Wed 09-23, which is provisional
    cutoff = reader.cutoff_session(readout, sessions)
    assert cutoff is not None
    assert cutoff == date(2026, 9, 22)
    bars = [_bar(day, "10") for day in sessions if day <= cutoff]
    verdicts = evaluate_series(bars, "us_equity", as_of=readout)
    assert not any(verdict.provisional for verdict in verdicts.bars)
    # The boundary session itself IS provisional, which is why it is excluded.
    boundary = readout - timedelta(days=PROVISIONAL_WINDOW_DAYS)
    tail = evaluate_series([_bar(boundary - timedelta(days=1), "10"), _bar(boundary, "10")], "us_equity", as_of=readout)
    assert tail.bars[-1].provisional


def test_cutoff_is_calendar_only_and_monotone() -> None:
    sessions = _weekdays(date(2026, 9, 1), date(2026, 10, 30))
    cutoffs = [reader.cutoff_session(date(2026, 9, 10) + timedelta(days=k), sessions) for k in range(40)]
    assert all(c is not None for c in cutoffs)
    assert cutoffs == sorted(cutoffs)  # type: ignore[type-var]
    assert reader.cutoff_session(date(2026, 9, 3), sessions) is None


# --- Prices ---------------------------------------------------------------


def _series() -> list[Bar]:
    days = _weekdays(date(2026, 8, 3), date(2026, 8, 21))
    closes = ["10", "10.2", "10.1", "0", "10.3", "10.4", "10.2", "10.5", "10.6", "10.4", "10.7", "10.8", "10.9", "11"]
    return [_bar(day, close) for day, close in zip(days, closes, strict=False)]


def test_verdicts_are_computed_in_process_as_evaluate_series() -> None:
    bars = _series()
    read = reader.read_series(bars, "us_equity", as_of=date(2026, 9, 28))
    assert read.verdicts == evaluate_series(bars, "us_equity", as_of=date(2026, 9, 28))
    assert read.bars == tuple(bars)


def test_masking_follows_load_masked_series() -> None:
    bars = _series()
    bars[1] = Bar(bars[1].price_date, Decimal("0"), bars[1].high, bars[1].low, bars[1].close, bars[1].volume)
    bars[5] = _bar(bars[5].price_date, "10.4", high="10.0")  # high < close: B3, range only
    read = reader.read_series(bars, "us_equity", as_of=date(2026, 9, 28))
    by_date = {verdict.price_date: verdict for verdict in read.verdicts.bars}
    for bar, masked in zip(bars, read.masked, strict=True):
        verdict = by_date[bar.price_date]
        assert masked.close == (bar.close if verdict.return_usable else None)
        assert masked.high == (bar.high if verdict.range_usable else None)
        assert masked.low == (bar.low if verdict.range_usable else None)
        assert masked.volume == bar.volume
    assert read.masked[1].open is None  # the open on its value, not a verdict
    assert not by_date[bars[3].price_date].return_usable  # close 0 is B1
    assert read.masked[3].close is None
    assert not by_date[bars[5].price_date].range_usable
    assert read.masked[5].close == bars[5].close


def test_mask_refuses_verdicts_of_another_series() -> None:
    bars = _series()
    verdicts = evaluate_series(bars[1:], "us_equity", as_of=date(2026, 9, 28))
    with pytest.raises(ValueError, match="series' own"):
        reader.mask_bars(bars, verdicts)


def test_input_identity_moves_with_any_read_row_or_class() -> None:
    bars = _series()
    base = {7: reader.read_series(bars, "us_equity", as_of=date(2026, 9, 28))}
    same = {7: reader.read_series(list(bars), "us_equity", as_of=date(2026, 9, 28))}
    assert reader.input_sha256(base) == reader.input_sha256(same)
    changed = list(bars)
    changed[2] = _bar(changed[2].price_date, "10.11")
    assert reader.input_sha256(base) != reader.input_sha256(
        {7: reader.read_series(changed, "us_equity", as_of=date(2026, 9, 28))}
    )
    assert reader.input_sha256(base) != reader.input_sha256(
        {7: reader.read_series(bars, "crypto", as_of=date(2026, 9, 28))}
    )


# --- Termination ----------------------------------------------------------


def test_only_a_series_stopping_before_the_cutoff_is_stopped() -> None:
    bars = _series()
    series = {1: reader.read_series(bars, "us_equity", as_of=date(2026, 9, 28))}
    last = bars[-1].price_date
    assert reader.stopped_series(series, last) == {}
    assert reader.stopped_series(series, last + timedelta(days=1)) == {1: last}


def test_termination_class_link_outranks_suffix_and_unknown_is_kept() -> None:
    classes = reader.termination_classes(
        [1, 2, 3, 4],
        links={1: "(b)", 2: "(a)(3)"},
        symbols={1: "FAILQ", 2: "MRGR", 3: "BTAIQ", 4: "QUIET"},
    )
    assert classes == {
        1: TerminationClass.EXCHANGE_FAILURE,
        2: TerminationClass.OPERATION_OF_LAW,
        3: TerminationClass.Q_SUFFIX_OTC,
        4: TerminationClass.UNKNOWN,
    }
