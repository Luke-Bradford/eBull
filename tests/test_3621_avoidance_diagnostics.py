"""#3621 slice 2b: the printed diagnostics, on synthetic paths and flags only."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
from datetime import date

import numpy as np
import pytest

from app.services.avoidance_filters import FILTER_SETS, Filter, MaxMissing, MaxReading, NameFlags
from app.services.factor_book_path import Month, PathResult, Trade, TradeCategory
from scripts.report_3609_baselines import newey_west_lag, ols_newey_west
from scripts.report_3609_step2 import PanelMonth
from scripts.report_3609_step2_segments import PRICE_UNAVAILABLE, band_of
from scripts.report_3621_books import Population
from scripts.report_3621_diagnostics import (
    NW_LAG,
    BookStats,
    DifferentialStats,
    ExcludedName,
    Undefined,
    Window,
    book_stats,
    cell_counts,
    differential,
    formation_counts,
    holding_month,
    names_file,
    newey_west_t,
    screened_targets,
    spread,
    windows,
)

MAX, SUB5, YOUNG, SUB5_YOUNG, ALL3 = FILTER_SETS


def _months(first: Month, last: Month) -> list[Month]:
    out = [first]
    while out[-1] < last:
        y, m = out[-1]
        out.append((y + 1, 1) if m == 12 else (y, m + 1))
    return out


PATH_MONTHS = _months((2014, 10), (2024, 8))


def test_windows_cover_the_path_stages_and_calendar_years() -> None:
    got = {w.label: w for w in windows(PATH_MONTHS)}
    assert [len(got[k].months) for k in ("whole", "stage A", "stage B")] == [119, 80, 39]
    assert got["stage A"].months[-1] == (2021, 5) and got["stage B"].months[0] == (2021, 6)
    assert (len(got["2014"].months), got["2014"].partial) == (3, True)
    assert (len(got["2024"].months), got["2024"].partial) == (8, True)
    assert all(not got[str(y)].partial and len(got[str(y)].months) == 12 for y in range(2015, 2024))


WINDOW = Window("w", ((2020, 1), (2020, 2), (2020, 3), (2020, 4)))


def _path(r: list[float], nonpositive: Month | None = None) -> PathResult:
    path = PathResult(returns=dict(zip(WINDOW.months, r, strict=True)), nonpositive=nonpositive)
    path.turnover.update({(2020, 1): 0.2, (2020, 3): 0.1, (2019, 12): 9.0})
    path.trades.extend(
        [
            Trade((2020, 2), 1, TradeCategory.ENTRY, 10.0, 0.5, 100.0),
            Trade((2020, 2), 2, TradeCategory.ENTRY, 10.0, 0.2, 50.0),
            Trade((2019, 12), 3, TradeCategory.ENTRY, 10.0, 9.0, 1.0),
            Trade((2019, 12), 4, TradeCategory.INITIAL_PURCHASE, 10.0, 0.01, 1.0),  # charged in 2020-01
        ]
    )
    return path


def test_book_stats_on_known_returns() -> None:
    r = [0.10, -0.20, 0.05, 0.0]
    got = book_stats(_path(r), WINDOW)
    assert isinstance(got, BookStats)
    assert got.g == pytest.approx(3.0 * sum(math.log(1 + x) for x in r))
    assert got.arithmetic == pytest.approx(12.0 * sum(r) / 4)
    assert got.volatility == pytest.approx(float(np.std(r, ddof=1)) * math.sqrt(12))
    assert got.max_drawdown == pytest.approx(-0.20)  # from the 1.10 peak
    assert got.turnover_per_year == pytest.approx(0.3 * 3)  # months outside the window are not read
    assert got.cost_per_year == pytest.approx((0.01 + 0.005 + 0.004) * 3)


def test_exhaustion_undefines_windows_that_reach_it_only() -> None:
    path = _path([0.1, 0.1, -1.0, 0.0], nonpositive=(2020, 3))
    got = book_stats(path, WINDOW)
    assert isinstance(got, Undefined) and str(got) == "undefined (wealth exhausted at 2020-03)"
    early = Window("early", WINDOW.months[:2])
    assert isinstance(book_stats(path, early), BookStats)
    assert isinstance(differential(_path([0.0] * 4), path, WINDOW), Undefined)
    assert isinstance(differential(_path([0.0] * 4), path, early), DifferentialStats)


def test_newey_west_t_matches_step_0s_estimator_where_its_lag_rule_gives_3() -> None:
    n = 60
    assert newey_west_lag(n) == NW_LAG
    values = np.random.default_rng(3621).normal(0.001, 0.02, n)
    expected = float(ols_newey_west(values, np.empty((n, 0))).t_stats[0])
    assert newey_west_t(list(values)) == pytest.approx(expected, rel=1e-12)
    short = [0.01, -0.02, 0.03]  # fewer observations than the lag: the kernel stops at n - 1
    e = np.array(short) - np.mean(short)
    s = e @ e + 2 * (0.75 * (e[1:] @ e[:-1]) + 0.5 * (e[2:] @ e[:-2]))
    assert newey_west_t(short) == pytest.approx(np.mean(short) / (math.sqrt(s) / 3))


def test_differential_statistics_and_the_wealth_ratio_drawdown() -> None:
    u, f = [0.01, 0.02, 0.0, 0.01], [0.03, -0.02, 0.01, 0.01]
    got = differential(_path(u), _path(f), WINDOW)
    assert isinstance(got, DifferentialStats)
    d = np.subtract(f, u)
    assert (got.months, got.defined) == (4, 4)
    assert got.mean == pytest.approx(d.mean())
    assert got.ir == pytest.approx(12 * d.mean() / (np.std(d, ddof=1) * math.sqrt(12)))
    ratio = [1.0, 1.03 / 1.01, 1.03 * 0.98 / (1.01 * 1.02)]
    assert got.ratio_max_drawdown == pytest.approx(ratio[2] / ratio[1] - 1.0)
    same = differential(_path(u), _path(u), WINDOW)
    assert isinstance(same, DifferentialStats)
    assert (same.sd, same.ir, same.nw_t, same.ratio_max_drawdown) == (0.0, None, None, 0.0)


def _flags() -> dict[int, NameFlags]:
    def flag(missing: MaxMissing | None, *filters: Filter) -> NameFlags:
        return NameFlags(MaxReading(None if missing else 0.05, 20, 0, missing), frozenset(filters))

    return {
        1: flag(None),
        2: flag(None, Filter.MAX),
        3: flag(MaxMissing.SCREENED, Filter.MAX),
        4: flag(MaxMissing.SHORT, Filter.SUB5),
        5: flag(MaxMissing.ZERO_HEAVY, Filter.YOUNG, Filter.SUB5),
    }


def test_formation_counts_split_max_and_give_excluded_weight() -> None:
    got = formation_counts(frozenset(range(1, 6)), _flags(), SUB5_YOUNG)
    assert (got.admitted, got.max_above_cutoff, got.max_screened, got.max_short, got.max_zero_heavy) == (5, 1, 1, 1, 1)
    assert (got.sub5, got.young, got.excluded_weight) == (2, 1, 0.4)
    assert formation_counts(frozenset(), _flags(), MAX).excluded_weight is None
    assert holding_month(date(2021, 5, 28)) == (2021, 6)
    assert spread([None, 3.0, 1.0, 2.0]) == (1.0, 2.0, 3.0) and spread([None]) is None


def test_screened_only_component_removes_screened_names_only() -> None:
    got = screened_targets(frozenset(range(1, 6)), _flags())
    assert (got.filtered, got.flagged) == ({1, 2, 4, 5}, {3})


def test_cell_counts_cross_size_with_the_cost_band_of_the_raw_close() -> None:
    day = date(2020, 1, 31)
    month = PanelMonth(day, day, {}, {1: 20.0, 2: 20.0, 3: 3.0, 4: 3.0}, {})  # name 5 has no close
    segments = {p: frozenset() for p in Population}
    segments[Population.MICRO] = frozenset({3, 4, 5})
    segments[Population.MEGA] = frozenset({1, 2})
    got = cell_counts(month, segments, _flags(), SUB5)
    assert got == {
        (Population.MICRO, band_of(3.0)): (1, 1),
        (Population.MICRO, PRICE_UNAVAILABLE): (1, 0),
        (Population.MEGA, band_of(20.0)): (0, 2),
    }


def test_names_file_is_canonical_reproducible_and_hashed() -> None:
    rows = [
        ExcludedName(date(2020, 1, 31), 7, "BBB", Population.MICRO, (Filter.YOUNG, Filter.MAX)),
        ExcludedName(date(2019, 12, 31), 9, "AAA", Population.REST, (Filter.SUB5,)),
    ]
    payload, digest = names_file(rows)
    assert (payload, digest) == names_file(reversed(rows))
    assert digest == hashlib.sha256(payload).hexdigest()
    lines = [json.loads(line) for line in gzip.decompress(payload).decode().splitlines()]
    assert lines[0] == {"M": "2019-12-31", "filters": ["sub5"], "name_key": 9, "population": "rest", "symbol": "AAA"}
    assert lines[1]["filters"] == ["max", "young"]
