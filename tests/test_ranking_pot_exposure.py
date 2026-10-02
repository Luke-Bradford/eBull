"""#2842 slice 6c-ii-c-1 — exposures and beta (spec §9.4, "Exposures and beta" paragraph; r3-144)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services import ranking_pot_exposure as ex
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services.market_calendar import us_market_status
from tests.test_ranking_pot_rebalance import _inputs, _name
from tests.test_ranking_pot_step import TABLE

D = Decimal
END = date(2026, 9, 30)
TOL = D("1e-25")


def _sessions(count: int, end: date = END) -> list[date]:
    out: list[date] = []
    d = end
    while len(out) < count:
        if us_market_status(d) != "closed":
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def _series(dates: list[date], scale: int) -> tuple[dict[date, Decimal], dict[date, Decimal]]:
    """SPY alternates ±1%; the name moves ``scale`` times SPY's return each session (exact β = scale)."""
    spy, name = {}, {}
    s = n = D(100)
    for i, d in enumerate(dates):
        if i:
            x = D("0.01") if i % 2 else D("-0.005")
            s, n = s * (1 + x), n * (1 + scale * x)
        spy[d], name[d] = s, n
    return spy, name


def test_beta_is_the_ols_slope_over_paired_sessions() -> None:
    spy, name = _series(_sessions(240), 2)
    b, reason = ex.beta(name, spy, END)
    assert reason is None and b is not None and abs(b - 2) < D("1e-25")

    # A pair needs both sessions in both series: dropping one SPY close removes two pairs.
    sessions = sorted(spy)
    gappy = {d: c for d, c in spy.items() if d != sessions[-10]}
    assert ex.beta(name, gappy, END)[0] is not None
    # p(d) may fall before the window: only d is window-bound (D − 365 < d ≤ D).
    assert ex.beta(name, spy, sessions[-1] + timedelta(days=400)) == (None, "too_few_pairs")


def test_beta_needs_200_paired_returns_not_200_bars() -> None:
    dates = _sessions(201)  # 200 returns
    spy, name = _series(dates, 1)
    assert ex.beta(name, spy, END)[1] is None
    trimmed = {d: c for d, c in name.items() if d != dates[-1]}  # 199 returns
    assert ex.beta(trimmed, spy, END) == (None, "too_few_pairs")
    # Precedence: too few pairs wins over a constant market.
    flat = dict.fromkeys(dates, D(100))
    assert ex.beta(dict(list(name.items())[:50]), flat, END) == (None, "too_few_pairs")
    assert ex.beta(name, flat, END) == (None, "zero_market_variance")


def test_closes_are_close_only_and_strictly_ascending() -> None:
    rows = [{"close": D(1)}, {"close": D(0)}, {"close": D("NaN")}, {"close": None}]
    dates = _sessions(4)
    assert ex.usable_closes(dates, rows) == {dates[0]: D(1)}
    with pytest.raises(rb.SnapshotIntegrityError, match="strictly ascending"):
        ex.usable_closes([dates[0], dates[0]], rows[:2])


def test_characteristics_from_the_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = _inputs(monkeypatch)
    table = ex.characteristics(inputs, [1, 4])
    one, four = table[1], table[4]
    assert one.sector == "XLK"  # SIC 7372 → Information Technology
    assert one.ln_cap is not None and abs(one.ln_cap - D(10**11).ln()) < D("1e-25")
    assert one.beta is None and one.beta_reason == "too_few_pairs"  # 60 bars
    assert one.atr_pct is not None and one.atr_pct > 0
    assert (four.sector, four.ln_cap, four.atr_pct) == (ex.NO_SECTOR, None, None)  # no SIC, no cap, no bars
    with pytest.raises(rb.SnapshotIntegrityError, match="outside the snapshot"):
        ex.characteristics(inputs, [99])

    # A non-finite cap is no size; a full year of bars against SPY's gives a beta.
    dates = _sessions(260, inputs.last_session)
    spy_c, name_c = _series(dates, 3)
    bars = tuple({"open": c, "high": c, "low": c, "close": c, "volume": 1000} for c in name_c.values())
    facts = tuple(
        replace(f, market_cap_usd=D("Infinity"), bar_dates=tuple(dates), bar_rows=bars) if f.instrument_id == 2 else f
        for f in inputs.facts
    )
    spy = replace(inputs.spy, bars=tuple((d, {"close": c}) for d, c in spy_c.items()))
    two = ex.characteristics(replace(inputs, facts=facts, spy=spy), [2])[2]
    assert two.ln_cap is None and two.beta is not None and abs(two.beta - 3) < D("1e-25")
    assert ex.decode_table(ex.encode_table(table)) == table
    with pytest.raises(rb.SnapshotIntegrityError, match="not finite"):
        ex.decode_table([[1, "XLK", None, None, "too_few_pairs", None, "NaN"]])


def _position(iid: int, value: str) -> sim.Position:
    v = D(value)
    return sim.Position(iid, iid, iid - 1, v, END, D(1), v, D("0.5"), D(2), D(1), END, v, 0, D(0))


def test_book_sums_weigh_the_closing_book_by_value() -> None:
    state = replace(sim.new_book(3), positions=(_position(1, "0.6"), _position(2, "0.3"), _position(3, "0.1")))
    sums = ex.book_sums(state, TABLE)
    w = sums.window()
    assert w["sector"] == {"XLF": "0.3", "XLK": "0.6", "none": "0.1"}
    # beta: names 1 (1.5, weight 0.6) and 3 (0.5, weight 0.1); name 2 has none.
    assert abs(D(w["beta"]["value"]) - (D("0.6") * D("1.5") + D("0.1") * D("0.5")) / D("0.7")) < TOL
    assert D(w["beta"]["coverage"]) == D("0.7")
    assert abs(D(w["ln_cap"]["value"]) - (D("0.6") * 20 + D("0.3") * 22) / D("0.9")) < TOL
    assert D(w["ln_cap"]["exp"]) == D(w["ln_cap"]["value"]).exp(sim.CTX)
    assert ex.Sums.of_doc(sums.doc()) == sums

    empty = ex.book_sums(sim.new_book(3), TABLE).window()
    assert empty["sector"] == {} and empty["beta"] == {"value": None, "coverage": None}
    no_beta = ex.book_sums(replace(state, positions=(_position(2, "1"),)), TABLE).window()
    assert no_beta["beta"] == {"value": None, "coverage": "0"}

    with pytest.raises(rb.SnapshotIntegrityError, match="missing from the table"):
        ex.book_sums(replace(state, positions=(_position(9, "1"),)), TABLE)
    with pytest.raises(rb.SnapshotIntegrityError, match="not finite"):
        ex.book_sums(replace(state, positions=(_position(1, "-0.1"),)), TABLE)


def test_window_sums_add_sessions_and_r_t_weighs_members_equally() -> None:
    a = ex.book_sums(replace(sim.new_book(2), positions=(_position(1, "1"),)), TABLE)
    b = ex.book_sums(replace(sim.new_book(2), positions=(_position(3, "3"),)), TABLE)
    a.add(b)
    # Capital-time: the 3.0-capital session counts three times the 1.0 one.
    assert D(a.window()["beta"]["value"]) == (1 * D("1.5") + 3 * D("0.5")) / 4
    r = ex.universe_sums({1, 2, 3}, TABLE, 2).window()
    assert set(r["sector"]) == {"XLF", "XLK", "none"}
    assert all(abs(D(v) - D(1) / 3) < TOL for v in r["sector"].values())
    assert abs(D(r["beta"]["value"]) - 1) < TOL and abs(D(r["beta"]["coverage"]) - D(2) / 3) < TOL
    assert ex.universe_sums(set(), TABLE, 5).capital == 0


# ---------------------------------------------------------------------------
# Slice 6c-ii-c-2a — the yield gap
# ---------------------------------------------------------------------------
def test_dividend_yield_is_per_tradable_unit_and_covered_only_where_the_dps_is_known() -> None:
    def y(dps: str | None, pct: str | None = None, price: str | None = None, close: str | None = "210") -> Any:
        f = _name(1, D("0.9"), dps_ttm=None if dps is None else D(dps))
        f = replace(
            f, valuation_yield_pct=None if pct is None else D(pct), valuation_price=None if price is None else D(price)
        )
        return ex.dividend_yield(f, None if close is None else D(close))

    # An ordinary share: the view's yield 0.5% at 210 → DPS 1.05 per unit.
    assert y("1.05", "0.5", "210") == D("0.005")
    # A 4:1 ADS: DPS 1.05 per ordinary is 4.2 per ADS; the view's ratio-corrected yield is 2% at 210.
    assert y("1.05", "2", "210") == D("0.02")
    assert y("1.05", None, "210") is None  # a ratio-unknown ADR: the view publishes no yield
    assert y("0") == 0  # a covered 0 (not proof of a non-payer), whatever the ratio
    assert y(None, "0.5", "210") is None  # NULL is "not covered", never 0
    assert y("-1") is None and y("NaN") is None
    assert y("1", "0.5", "210", None) is None and y("1", "0.5", "210", "0") is None


def test_div_yield_reaches_the_table_and_its_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = _inputs(monkeypatch)
    facts = tuple(
        replace(f, dps_ttm=D("0.5"), valuation_yield_pct=D(1), valuation_price=D(50)) if f.instrument_id == 1 else f
        for f in inputs.facts
    )
    table = ex.characteristics(replace(inputs, facts=facts), [1, 2])
    close = inputs.facts[0].bar_rows[-1]["close"]
    assert table[1].div_yield is not None and abs(table[1].div_yield - D("0.5") / close) < TOL
    assert table[2].div_yield is None
    assert ex.decode_table(ex.encode_table(table)) == table
