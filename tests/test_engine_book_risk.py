"""#3543: the engine-book risk snapshot's pure core (no DB)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services.engine_book_risk import (
    STRESS_2008,
    STRESS_2020,
    BookInputs,
    HeldPosition,
    MandateLimits,
    compute_snapshot,
    ewma_vol,
    nyse_sessions,
    session_returns,
)

_START = date(2025, 1, 1)
_SPY = 1
_A = 10
_B = 11
_NO_MANDATE = MandateLimits(False, None, None, None)
_MANDATE = MandateLimits(True, Decimal("12"), Decimal("20"), 3)


def _days(n: int) -> list[date]:
    return [_START + timedelta(days=k) for k in range(n)]


def _path(n: int, *, scale: Decimal, seed: int, start: Decimal = Decimal("100")) -> dict[date, Decimal | None]:
    """Deterministic zig-zag closes: alternating returns of +/- scale, perturbed by ``seed``."""
    out: dict[date, Decimal | None] = {}
    price = start
    for k, d in enumerate(_days(n)):
        out[d] = price
        step = scale * (1 + Decimal((k * seed) % 7) / 10)
        price = price * (1 + (step if k % 2 == 0 else -step))
    return out


def _position(position_id: int, instrument_id: int, units: str = "10", **kw: object) -> HeldPosition:
    return HeldPosition(
        trade_id=position_id,
        position_id=position_id,
        instrument_id=instrument_id,
        units=Decimal(units),
        initial_units=kw.get("initial_units", Decimal(units)),  # type: ignore[arg-type]
        initial_amount_usd=kw.get("initial_amount_usd", Decimal("1000")),  # type: ignore[arg-type]
        is_buy=kw.get("is_buy", True),  # type: ignore[arg-type]
    )


def _short(position_id: int, instrument_id: int, units: str = "10", **kw: object) -> HeldPosition:
    return _position(position_id, instrument_id, units, is_buy=False, **kw)


def _scaled(closes: dict[date, Decimal | None], factor: Decimal) -> dict[date, Decimal | None]:
    """A path whose every session return is ``factor`` x that of ``closes`` (e.g. -4 for an inverse 4x)."""
    out: dict[date, Decimal | None] = {}
    days = sorted(closes)
    price = Decimal("100")
    for prev, cur in zip([None, *days], days, strict=False):
        if prev is not None:
            a, b = closes[prev], closes[cur]
            assert a is not None and b is not None
            price = price * (1 + factor * (b / a - 1))
        out[cur] = price
    return out


def _inputs(
    positions: tuple[HeldPosition, ...],
    closes: dict[int, dict[date, Decimal | None]],
    *,
    n: int = 300,
    mandate: MandateLimits = _MANDATE,
    open_trades: int | None = None,
    capital: str = "10000",
) -> BookInputs:
    spy = _path(n, scale=Decimal("0.01"), seed=3)
    return BookInputs(
        session=_days(n)[-1],
        measured_at=datetime(2026, 10, 2, 9, tzinfo=UTC),
        pool_event_id=1,
        capital=Decimal(capital),
        calendar=tuple(_days(n)[-253:]),
        positions=positions,
        open_trade_count=len(positions) if open_trades is None else open_trades,
        mandate=mandate,
        closes=closes,
        spy_closes=spy,
    )


def test_nyse_sessions_skip_closures() -> None:
    sessions = nyse_sessions(date(2026, 1, 5), 3)
    # 2026-01-01 is a holiday and 01-03/04 a weekend.
    assert sessions == (date(2025, 12, 31), date(2026, 1, 2), date(2026, 1, 5))


def test_a_gap_drops_both_returns_it_touches() -> None:
    days = _days(4)
    closes: dict[date, Decimal | None] = {
        days[0]: Decimal("1"),
        days[1]: None,
        days[2]: Decimal("2"),
        days[3]: Decimal("3"),
    }
    assert session_returns(closes, days) == [(days[3], Decimal("0.5"))]


def test_ewma_seeds_with_the_first_squared_return() -> None:
    r = [Decimal("0.01"), Decimal("0.02")]
    expected = (Decimal("0.94") * Decimal("0.0001") + Decimal("0.06") * Decimal("0.0004")).sqrt() * Decimal(252).sqrt()
    assert ewma_vol(r) == expected
    assert ewma_vol([]) is None


def test_empty_book() -> None:
    snap = compute_snapshot(_inputs((), {}, mandate=_NO_MANDATE))
    assert snap.history_status == "empty_book"
    assert (snap.hist_vol_pct, snap.ewma_vol_pct, snap.beta) == (0, 0, 0)
    assert (snap.largest_share_pct, snap.top5_share_pct, snap.hhi) == (None, None, None)
    assert (snap.stress_2008_pct, snap.stress_2020_pct, snap.stress_2022_pct, snap.gross_usd) == (0, 0, 0, 0)
    assert snap.checks["forecast_vol_vs_target"]["status"] == "no_limit"
    assert snap.checks["stale_marks"] == {"status": "evaluated", "value": "0", "limit": "0", "flagged": False}


def test_a_book_tracking_spy_has_beta_one_and_scenario_stress() -> None:
    spy = _path(300, scale=Decimal("0.01"), seed=3)
    snap = compute_snapshot(_inputs((_position(1, _A, units="50"),), {_A: spy}))
    assert snap.history_status == "ok"
    assert snap.vol_n_obs == snap.beta_n_obs == 252
    # Weight = 50 x last close / 10,000; a SPY-identical instrument has beta 1 and the book's beta is its weight.
    last = spy[max(spy)]
    assert last is not None
    weight = 50 * last / Decimal("10000")
    assert snap.beta == (weight).quantize(Decimal("1e-8"))
    assert snap.stress_2008_pct == (weight * STRESS_2008 * 100).quantize(Decimal("1e-8"))
    assert snap.stress_2020_pct == (weight * STRESS_2020 * 100).quantize(Decimal("1e-8"))
    assert (snap.largest_share_pct, snap.top5_share_pct, snap.hhi) == (100, 100, 10000)
    assert snap.beta_defaulted_count == 0


def test_lots_of_one_instrument_aggregate_for_concentration() -> None:
    a, b = _path(300, scale=Decimal("0.01"), seed=3), _path(300, scale=Decimal("0.02"), seed=5)
    snap = compute_snapshot(
        _inputs((_position(1, _A, "10"), _position(2, _A, "10"), _position(3, _B, "20")), {_A: a, _B: b})
    )
    assert (snap.position_count, snap.instrument_count) == (3, 2)
    assert snap.top5_share_pct == 100
    assert snap.largest_share_pct is not None and snap.hhi is not None
    assert 5000 <= snap.hhi < 10000


def test_short_history_is_insufficient_and_defaults_beta() -> None:
    a = {d: c for d, c in _path(300, scale=Decimal("0.01"), seed=3).items() if d >= _days(300)[-30]}
    snap = compute_snapshot(_inputs((_position(1, _A),), {_A: a}))
    assert snap.history_status == "insufficient_history"
    assert (snap.hist_vol_pct, snap.ewma_vol_pct, snap.beta) == (None, None, None)
    assert snap.beta_n_obs == snap.vol_n_obs > 0
    assert snap.beta_defaulted_count == 1 and snap.beta_defaulted_weight_pct > 0
    assert snap.positions[0]["beta"] == "defaulted"
    assert snap.checks["forecast_vol_vs_target"]["status"] == "unknown"


def test_no_close_marks_at_remaining_cost_and_is_stale() -> None:
    snap = compute_snapshot(
        _inputs(
            (_position(1, _A, units="5", initial_units=Decimal("10"), initial_amount_usd=Decimal("1000")),),
            {},
        )
    )
    assert snap.gross_usd == 500
    assert (snap.cost_marked_count, snap.stale_count) == (1, 1)
    assert snap.positions[0]["mark_date"] == "cost"
    assert snap.checks["stale_marks"]["flagged"] is True


def test_a_close_before_the_session_is_stale() -> None:
    a = _path(300, scale=Decimal("0.01"), seed=3)
    del a[max(a)]
    snap = compute_snapshot(_inputs((_position(1, _A),), {_A: a}))
    assert (snap.cost_marked_count, snap.stale_count) == (0, 1)


def test_stress_is_floored_at_a_total_loss_per_instrument() -> None:
    # A 4x-SPY instrument: beta ~4, so 4 x -34% would be -136% without the floor.
    levered = _path(300, scale=Decimal("0.04"), seed=3)
    snap = compute_snapshot(_inputs((_position(1, _A, units="1"),), {_A: levered}, capital="100"))
    assert snap.positions[0]["beta"] != "defaulted"
    weight = Decimal(snap.positions[0]["weight_of_capital_pct"])
    assert snap.stress_2008_pct == snap.stress_2020_pct == -weight


@pytest.mark.parametrize(
    ("mandate", "open_trades", "status", "flagged"),
    [
        (_NO_MANDATE, 9, "no_limit", False),
        (MandateLimits(True, None, None, None), 9, "no_limit", False),
        (_MANDATE, 3, "evaluated", False),
        (_MANDATE, 4, "evaluated", True),
    ],
)
def test_concurrency_check(mandate: MandateLimits, open_trades: int, status: str, flagged: bool) -> None:
    a = _path(300, scale=Decimal("0.01"), seed=3)
    snap = compute_snapshot(_inputs((_position(1, _A),), {_A: a}, mandate=mandate, open_trades=open_trades))
    assert snap.checks["positions_vs_max_concurrent"]["status"] == status
    assert snap.checks["positions_vs_max_concurrent"]["flagged"] is flagged


def test_stress_flag_compares_the_signed_loss() -> None:
    a = _path(300, scale=Decimal("0.01"), seed=3)
    # Fully invested at beta 1: a -34% scenario breaches a 20% drawdown limit, -25% does too; 40% breaches neither.
    full = compute_snapshot(_inputs((_position(1, _A, units="100"),), {_A: a}))
    assert full.checks["stress_2020_vs_max_drawdown"]["flagged"] is True
    # The stored convention the panel renders (EngineBookRiskPanel.checkLimit shows the limit as -limit):
    # the stress value is a signed loss, the drawdown limit is unsigned.
    stored = full.checks["stress_2020_vs_max_drawdown"]
    assert Decimal(stored["value"]) < 0 < Decimal(stored["limit"])
    loose = compute_snapshot(
        _inputs((_position(1, _A, units="100"),), {_A: a}, mandate=MandateLimits(True, Decimal("50"), Decimal("40"), 9))
    )
    assert loose.checks["stress_2020_vs_max_drawdown"]["flagged"] is False
    # -56% breaches a 40% limit at beta 1 where -34% does not.
    assert loose.checks["stress_2008_vs_max_drawdown"]["flagged"] is True
    assert loose.checks["stress_2022_vs_max_drawdown"]["flagged"] is False


def test_zero_variance_spy_is_degenerate() -> None:
    flat: dict[date, Decimal | None] = dict.fromkeys(_days(300), Decimal("100"))
    inputs = _inputs((_position(1, _A),), {_A: flat})
    snap = compute_snapshot(BookInputs(**{**inputs.__dict__, "spy_closes": flat}))
    assert snap.history_status == "degenerate"
    assert snap.beta is None and snap.hist_vol_pct == 0
    assert snap.beta_defaulted_count == 1


def test_stress_2008_reproduces_from_the_spy_closes() -> None:
    # Series 7694 (Intrader SPY) closes at the 2007-10-09 peak and the 2009-03-09 trough (spec "Source rule").
    assert (Decimal("68.110001") / Decimal("156.479996") - 1).quantize(Decimal("1e-8")) == STRESS_2008


# v3 (#3612): signed weights. Spec section "v3: signed weights".
_Q = Decimal("1e-8")


def test_a_pure_short_mirrors_the_long() -> None:
    closes = {_A: _scaled(_path(300, scale=Decimal("0.01"), seed=3), Decimal("0.5"))}
    long = compute_snapshot(_inputs((_position(1, _A, "50"),), closes))
    short = compute_snapshot(_inputs((_short(1, _A, "50"),), closes))
    assert short.positions[0]["side"] == "short"
    assert short.gross_usd == long.gross_usd == long.net_usd == -short.net_usd
    assert Decimal(short.positions[0]["market_value_usd"]) == -Decimal(long.positions[0]["market_value_usd"])
    assert (short.hist_vol_pct, short.ewma_vol_pct) == (long.hist_vol_pct, long.ewma_vol_pct)
    assert long.beta is not None and long.beta > 0 and short.beta == -long.beta
    assert (short.largest_share_pct, short.hhi) == (long.largest_share_pct, long.hhi) == (100, 10000)
    # beta 0.5 x a fall never reaches the floor, so the short gains exactly what the long loses.
    assert short.stress_2020_pct == -long.stress_2020_pct > 0


def test_offsetting_lots_net_in_exposure_but_not_in_gross() -> None:
    a = _path(300, scale=Decimal("0.01"), seed=5)
    closes = {_A: a, _B: dict(a)}
    snap = compute_snapshot(_inputs((_position(1, _A), _short(2, _A), _position(3, _B)), closes))
    alone = compute_snapshot(_inputs((_position(3, _B),), {_B: a}))
    last = a[max(a)]
    assert last is not None
    assert snap.gross_usd == (30 * last).quantize(_Q)
    assert snap.net_usd == (10 * last).quantize(_Q)
    # A holds 2/3 of gross, B 1/3: HHI = 66.67^2 + 33.33^2.
    assert snap.largest_share_pct == (Decimal(200) / 3).quantize(_Q)
    assert snap.hhi == ((Decimal(200) / 3) ** 2 + (Decimal(100) / 3) ** 2).quantize(_Q)
    # A nets to zero, so the book return is B's alone.
    assert (snap.hist_vol_pct, snap.ewma_vol_pct, snap.beta) == (alone.hist_vol_pct, alone.ewma_vol_pct, alone.beta)
    assert (snap.stress_2008_pct, snap.stress_2020_pct) == (alone.stress_2008_pct, alone.stress_2020_pct)
    assert (snap.position_count, snap.instrument_count) == (3, 2)


def test_a_long_short_pair_on_identical_returns_has_zero_vol() -> None:
    a = _path(300, scale=Decimal("0.02"), seed=5)
    snap = compute_snapshot(_inputs((_position(1, _A), _short(2, _B)), {_A: a, _B: dict(a)}))
    assert snap.history_status == "ok"
    assert (snap.hist_vol_pct, snap.ewma_vol_pct, snap.beta, snap.net_usd) == (0, 0, 0, 0)
    assert snap.gross_usd > 0 and snap.largest_share_pct == 50


def test_an_all_offset_book_is_measured_not_empty() -> None:
    # No history at all for the instrument: netting to zero, it needs none.
    snap = compute_snapshot(_inputs((_position(1, _A, initial_units=Decimal("10")), _short(2, _A)), {}))
    assert snap.history_status == "ok"
    assert (snap.hist_vol_pct, snap.ewma_vol_pct, snap.beta, snap.stress_2020_pct, snap.net_usd) == (0, 0, 0, 0, 0)
    assert snap.vol_n_obs == 252
    assert snap.gross_usd == 2000 and snap.largest_share_pct == 100
    # Counts stay per position: both lots are cost-marked and stale.
    assert (snap.cost_marked_count, snap.stale_count) == (2, 2)


def test_an_unmeasured_short_is_assumed_beta_zero_and_counted_at_gross() -> None:
    recent = {d: c for d, c in _path(300, scale=Decimal("0.01"), seed=3).items() if d >= _days(300)[-30]}
    short = compute_snapshot(_inputs((_short(1, _A, "50"),), {_A: recent}))
    assert short.positions[0]["beta"] == "defaulted" and short.positions[0]["beta_assumed"] == "0"
    assert short.stress_2008_pct == short.stress_2020_pct == 0
    long = compute_snapshot(_inputs((_position(1, _A, "50"),), {_A: recent}))
    assert long.positions[0]["beta_assumed"] == "1"
    assert long.beta_defaulted_weight_pct == short.beta_defaulted_weight_pct > 0
    # A defaulted +w and -w pair reports their gross, not a netted 0.
    pair = compute_snapshot(_inputs((_position(1, _A, "50"), _short(2, _B, "50")), {_A: recent, _B: dict(recent)}))
    assert pair.beta_defaulted_count == 2
    # Each figure is rounded to 8 dp on its own.
    assert abs(pair.beta_defaulted_weight_pct - 2 * long.beta_defaulted_weight_pct) <= _Q


def test_a_short_at_cost_is_negative_remaining_cost() -> None:
    partial = compute_snapshot(
        _inputs((_short(1, _A, units="5", initial_units=Decimal("10"), initial_amount_usd=Decimal("1000")),), {})
    )
    assert (partial.net_usd, partial.gross_usd) == (-500, 500)
    unknown = compute_snapshot(_inputs((_short(1, _A, units="5", initial_units=None),), {}))
    assert (unknown.net_usd, unknown.gross_usd) == (-1000, 1000)
    assert Decimal(unknown.positions[0]["weight_of_capital_pct"]) == -10


def test_a_short_gain_is_floored_and_a_short_loss_is_not() -> None:
    spy = _path(300, scale=Decimal("0.01"), seed=3)
    levered = compute_snapshot(_inputs((_short(1, _A, units="1"),), {_A: _scaled(spy, Decimal("4"))}, capital="100"))
    weight = Decimal(levered.positions[0]["weight_of_capital_pct"])
    assert weight < 0
    # beta ~4 x a -34% fall floors the instrument at -100%: the short gains exactly |w|.
    assert levered.stress_2020_pct == -weight
    inverse = compute_snapshot(_inputs((_short(1, _A, units="1"),), {_A: _scaled(spy, Decimal("-4"))}, capital="100"))
    beta = Decimal(inverse.positions[0]["beta"])
    w = Decimal(inverse.positions[0]["weight_of_capital_pct"])
    # beta ~-4: the instrument rises ~136% in 2020, more than 100% of the short's own weight lost.
    assert beta < -3
    assert inverse.stress_2020_pct == (w * beta * STRESS_2020).quantize(_Q)
    assert inverse.stress_2020_pct < w
