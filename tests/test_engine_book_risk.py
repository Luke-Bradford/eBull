"""#3543: the engine-book risk snapshot's pure core (no DB)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services.engine_book_risk import (
    STRESS_2020,
    BookInputs,
    HeldPosition,
    MandateLimits,
    compute_snapshot,
    ewma_vol,
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
    )


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
        positions=positions,
        open_trade_count=len(positions) if open_trades is None else open_trades,
        mandate=mandate,
        closes=closes,
        spy_closes=spy,
    )


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
    assert (snap.stress_2020_pct, snap.stress_2022_pct, snap.gross_usd) == (0, 0, 0)
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
    assert snap.stress_2020_pct == -weight


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
    loose = compute_snapshot(
        _inputs((_position(1, _A, units="100"),), {_A: a}, mandate=MandateLimits(True, Decimal("50"), Decimal("40"), 9))
    )
    assert loose.checks["stress_2020_vs_max_drawdown"]["flagged"] is False
    assert loose.checks["stress_2022_vs_max_drawdown"]["flagged"] is False


def test_zero_variance_spy_is_degenerate() -> None:
    flat: dict[date, Decimal | None] = dict.fromkeys(_days(300), Decimal("100"))
    inputs = _inputs((_position(1, _A),), {_A: flat})
    snap = compute_snapshot(BookInputs(**{**inputs.__dict__, "spy_closes": flat}))
    assert snap.history_status == "degenerate"
    assert snap.beta is None and snap.hist_vol_pct == 0
    assert snap.beta_defaulted_count == 1
