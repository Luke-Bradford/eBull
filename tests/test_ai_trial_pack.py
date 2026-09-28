"""#3471 slice 1b-ii — the pure shortlist / pack / indicator functions (``app/services/ai_trial_pack``)."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from app.services import ai_trial_pack as p
from app.services.indicator_series import BarSeries, atr_series, rsi_series

AS_OF = datetime(2026, 10, 2, 23, 30, tzinfo=UTC)
FRESH = AS_OF - timedelta(hours=1)


def _cand(
    i: int, score: float | None = 1.0, *, bid: str = "10.00", ask: str = "10.05", **kw: Any
) -> p.ShortlistCandidate:
    base: dict[str, Any] = {
        "instrument_id": i,
        "symbol": f"S{i}",
        "bid": Decimal(bid),
        "ask": Decimal(ask),
        "quoted_at": FRESH,
        "total_score": score,
        "market_cap_usd": Decimal("1e9"),
    }
    base.update(kw)
    return p.ShortlistCandidate(**base)


@pytest.mark.parametrize(
    ("candidate", "eligible"),
    [
        (_cand(1), True),
        (_cand(1, quoted_at=AS_OF), True),  # upper bound inclusive
        (_cand(1, quoted_at=AS_OF - timedelta(hours=24)), False),  # lower bound exclusive
        (_cand(1, quoted_at=AS_OF + timedelta(seconds=1)), False),  # not yet known
        (_cand(1, quoted_at=None), False),
        (_cand(1, bid="2.99", ask="3.00"), False),
        (_cand(1, bid="3.00", ask="3.00"), True),
        (_cand(1, bid="10.00", ask="9.99"), False),
        # spread = 0.1 / 10.05 = 0.995% passes; 0.11 / 10.055 = 1.094% fails
        (_cand(1, bid="10.00", ask="10.10"), True),
        (_cand(1, bid="10.00", ask="10.11"), False),
        (_cand(1, bid="NaN", ask="10.00"), False),
        (_cand(1, score=None), False),
        (_cand(1, score=0.0), False),
        (_cand(1, score=math.nan), False),
        (_cand(1, score=math.inf), False),
    ],
)
def test_eligibility(candidate: p.ShortlistCandidate, eligible: bool) -> None:
    assert p.is_eligible(candidate, as_of=AS_OF) is eligible


def test_shortlist_is_top_30_plus_20_small_caps_from_the_rest() -> None:
    # 40 large caps outrank every small cap, so the small-cap slice is filled from the rest.
    large = [_cand(i, 100.0 - i, market_cap_usd=Decimal("5e9")) for i in range(1, 41)]
    small = [_cand(100 + i, 10.0 - i / 100, market_cap_usd=Decimal("1.5e9")) for i in range(25)]
    no_cap = [_cand(200, 50.0, market_cap_usd=None)]  # rank 41: eligible, but not in either slice
    ineligible = [_cand(300, 1000.0, bid="1.00", ask="1.00")]
    got = p.select_shortlist(large + small + no_cap + ineligible, as_of=AS_OF)

    assert got.eligible_count == 40 + 25 + 1
    assert [n.instrument_id for n in got.names[:30]] == list(range(1, 31))
    assert {n.slice for n in got.names[:30]} == {"top"}
    assert [n.instrument_id for n in got.names[30:]] == [100 + i for i in range(20)]
    assert {n.slice for n in got.names[30:]} == {"small_cap"}


def test_shortlist_ties_break_by_instrument_id_and_a_null_cap_only_leaves_the_small_cap_slice() -> None:
    tied = [_cand(i, 5.0, market_cap_usd=None) for i in (9, 3, 7)]
    got = p.select_shortlist(tied, as_of=AS_OF)
    assert [n.instrument_id for n in got.names] == [3, 7, 9]  # a NULL cap does not block the top slice

    with pytest.raises(ValueError, match="duplicate instrument_id"):
        p.select_shortlist([_cand(1), _cand(1)], as_of=AS_OF)


def test_lazy_market_cap_is_resolved_in_rank_order_only_until_the_small_cap_slice_is_full() -> None:
    large = [_cand(i, 100.0 - i, market_cap_usd=None) for i in range(1, 31)]
    rest = [_cand(100 + i, 10.0 - i / 100, market_cap_usd=None) for i in range(40)]
    caps = {100 + i: (Decimal("5e9") if i % 2 else Decimal("1e9")) for i in range(40)}
    asked: list[int] = []

    def resolve(c: p.ShortlistCandidate) -> Decimal | None:
        asked.append(c.instrument_id)
        return caps.get(c.instrument_id)

    got = p.select_shortlist(large + rest, as_of=AS_OF, market_cap=resolve)
    small = got.names[30:]
    assert [n.instrument_id for n in small] == [100 + i for i in range(0, 40, 2)]
    assert {n.market_cap_usd for n in small} == {Decimal("1e9")}
    assert all(n.market_cap_usd is None for n in got.names[:30])  # the top slice never asks
    assert asked == [100 + i for i in range(39)]  # stops once the 20th small cap is admitted


def _bars(n: int, *, last: date = date(2026, 10, 2)) -> tuple[list[date], list[dict[str, Any]]]:
    dates = [last - timedelta(days=n - 1 - i) for i in range(n)]
    rows = [
        {"open": 10 + i * 0.1, "high": 10.5 + i * 0.1, "low": 9.5 + i * 0.1, "close": 10.2 + i * 0.1, "volume": 1000}
        for i in range(n)
    ]
    return dates, rows


def _with(rows: list[dict[str, Any]], index: int, **kw: Any) -> list[dict[str, Any]]:
    out = [dict(r) for r in rows]
    out[index].update(kw)
    return out


LAST = date(2026, 10, 2)


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda d, r: (d[1:], r[1:]), None),  # exactly 60 bars is complete
        (lambda d, r: (d, _with(r, 5, close=math.nan)), "non_finite_ohlcv"),
        (lambda d, r: (d, _with(r, 5, volume=None)), None),  # NULL volume = not provided, bar stands
        (lambda d, r: (d, _with(r, 5, volume=-1)), "negative_volume"),
        (lambda d, r: (d, _with(r, 5, low=10.6)), "bar_range_inconsistent"),  # low above open 10.5
        (lambda d, r: (d, _with(r, 5, high=10.3)), "bar_range_inconsistent"),  # high below max(open, close)
        (lambda d, r: ([*d[:5], d[4], *d[6:]], r), "duplicate_session"),
        (lambda d, r: ([*d[:4], d[5], d[4], *d[6:]], r), "not_ascending"),
        (lambda d, r: ([x - timedelta(days=1) for x in d], r), "stale_last_bar"),
    ],
)
def test_bar_validity(mutate: Any, reason: str | None) -> None:
    dates, rows = mutate(*_bars(61))
    got = p.build_bar_series(dates, rows, last_session=LAST)
    if reason is None:
        assert isinstance(got, BarSeries)
    else:
        assert got == reason


def test_bars_need_60_and_keep_the_last_260() -> None:
    assert p.build_bar_series(*_bars(59), last_session=LAST) == "too_few_bars"
    dates, rows = _bars(300)
    # A defect OUTSIDE the last 260 bars does not make the name incomplete.
    got = p.build_bar_series(dates, _with(rows, 10, close=math.nan), last_session=LAST)
    assert isinstance(got, BarSeries)
    assert len(got) == 260 and got.dates[0] == dates[40]


def test_indicators_reuse_the_house_wilder_functions_and_null_on_zero_denominators() -> None:
    series = p.build_bar_series(*_bars(120), last_session=LAST)
    assert isinstance(series, BarSeries)
    got = p.indicators(series)
    assert got["sma200"] is None  # fewer than 200 bars
    assert got["sma20"] == pytest.approx(sum(10.2 + i * 0.1 for i in range(100, 120)) / 20)
    assert got["rsi14"] == rsi_series(series, universe="survivor_only").values[-1]
    assert got["atr14"] == atr_series(series, universe="survivor_only").values[-1]
    assert got["volume_ratio20"] == pytest.approx(1.0)
    # constant volume: VWAP20 is the mean typical price of the last 20 bars
    typical = [(10.5 + i * 0.1 + 9.5 + i * 0.1 + 10.2 + i * 0.1) / 3 for i in range(100, 120)]
    assert got["vwap20_proxy"] == pytest.approx(sum(typical) / 20)

    dates, rows = _bars(120)
    zero = p.build_bar_series(dates, [{**r, "volume": 0} for r in rows], last_session=LAST)
    assert isinstance(zero, BarSeries)
    assert p.indicators(zero)["volume_ratio20"] is None
    assert p.indicators(zero)["vwap20_proxy"] is None


def _disc(source_id: int, title: str, days_ago: float, *, known_lag: timedelta = timedelta(0)) -> p.Disclosure:
    event_at = AS_OF - timedelta(days=days_ago)
    return p.Disclosure("filing", source_id, title, event_at, event_at + known_lag)


def test_disclosures_are_point_in_time_newest_first_deduped_and_capped() -> None:
    items = [
        _disc(1, "Old", 31),  # outside the 30-day window
        _disc(2, "Late ingest", 1, known_lag=timedelta(days=2)),  # O2: not yet known at as_of
        _disc(3, "Tie B", 2),
        _disc(4, "Tie A", 2),
        _disc(5, "Dup", 3),
        _disc(6, "Dup", 4),
        _disc(7, "Ctl\x00char\nline\t tab", 5),
        _disc(8, "x" * 250, 6),
        _disc(9, "Sixth", 7),
    ]
    got = p.select_disclosures(items, as_of=AS_OF)
    assert [d.source_id for d in got] == [3, 4, 5, 7, 8]
    assert got[3].title == "Ctl char line tab"
    assert got[4].title == "x" * 200

    news = p.Disclosure("news", 10, "Headline", AS_OF, AS_OF)
    with pytest.raises(ValueError, match="one source per call"):
        p.select_disclosures([*items, news], as_of=AS_OF)


def test_canonical_json() -> None:
    value = {
        "b": Decimal("1.50"),
        "a": [datetime(2026, 10, 2, 19, 30, tzinfo=timezone(timedelta(hours=-4)))],
        "é": date(2026, 10, 2),
    }
    assert p.canonical_json(value) == '{"a":["2026-10-02T23:30:00+00:00"],"b":"1.50","é":"2026-10-02"}'
    assert p.canonical_sha256(value) == p.canonical_sha256(dict(reversed(value.items())))
    for bad in (math.nan, math.inf, Decimal("NaN"), datetime(2026, 10, 2), object()):
        with pytest.raises(p.NonCanonicalValue):
            p.canonical_json({"x": bad})


def test_a_null_volume_keeps_the_name_and_nulls_only_the_volume_indicators() -> None:
    dates, rows = _bars(120)
    old_null = p.build_bar_series(dates, _with(rows, 10, volume=None), last_session=LAST)
    assert isinstance(old_null, BarSeries)
    clean = p.build_bar_series(dates, rows, last_session=LAST)
    assert isinstance(clean, BarSeries)
    full = p.indicators(clean)
    assert p.indicators(old_null) == full  # outside both 20-bar windows: nothing changes

    recent_null = p.build_bar_series(dates, _with(rows, 110, volume=None), last_session=LAST)
    assert isinstance(recent_null, BarSeries)
    got = p.indicators(recent_null)
    assert got["volume_ratio20"] is None and got["vwap20_proxy"] is None
    assert {k: got[k] for k in ("sma20", "sma50", "rsi14", "atr14")} == {
        k: full[k] for k in ("sma20", "sma50", "rsi14", "atr14")
    }
