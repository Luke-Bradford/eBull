"""Pure-logic tests for the value-history reconstruction (#1594 PR-B).

These cover the single source of the recompute formula in
``app/services/portfolio_value_history.py`` — units timeline, the MTM
equity identity (the HARD CONSTRAINT: same formula as the EOD snapshot,
not ``close * units``), FX carry-forward, and the persisted overlay.
The DB-backed wiring test lives in ``tests/test_value_history_db.py``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.portfolio_value_history import (
    carry_forward_rate_map,
    native_cost_basis,
    overlay_persisted,
    position_equity,
    reconstruct_units_at_day,
)

D = date


def test_units_open_with_no_closes_is_full_units() -> None:
    assert reconstruct_units_at_day(Decimal("10"), [], D(2025, 1, 5)) == Decimal("10")


def test_units_close_after_day_is_ignored() -> None:
    closes = [(D(2025, 2, 1), Decimal("4"))]
    assert reconstruct_units_at_day(Decimal("10"), closes, D(2025, 1, 5)) == Decimal("10")


def test_units_partial_close_subtracts_slice() -> None:
    closes = [(D(2025, 1, 3), Decimal("4"))]
    assert reconstruct_units_at_day(Decimal("10"), closes, D(2025, 1, 5)) == Decimal("6")


def test_units_multiple_partial_closes_accumulate() -> None:
    closes = [
        (D(2025, 1, 3), Decimal("4")),
        (D(2025, 1, 4), Decimal("3")),
        (D(2025, 1, 10), Decimal("1")),  # after the query day → ignored
    ]
    assert reconstruct_units_at_day(Decimal("10"), closes, D(2025, 1, 5)) == Decimal("3")


def test_units_full_close_zeroes_then_caller_drops() -> None:
    closes = [(D(2025, 1, 3), Decimal("10"))]
    assert reconstruct_units_at_day(Decimal("10"), closes, D(2025, 1, 5)) == Decimal("0")


def test_native_cost_basis_usd_unleveraged_is_open_rate() -> None:
    # 1000 USD invested / 10 units = 100 per unit == open_rate (unleveraged).
    assert native_cost_basis(Decimal("1000"), Decimal("10"), "USD", Decimal("100"), {}) == Decimal("100")


def test_native_cost_basis_usd_leveraged_is_below_open_rate() -> None:
    # 500 USD invested / 10 units = 50 per unit (2x leverage) → MTM equity, not
    # notional: 10*50 + 10*(120-100) = 700, the snapshot's number (Codex P2).
    cpu = native_cost_basis(Decimal("500"), Decimal("10"), "USD", Decimal("100"), {})
    assert cpu == Decimal("50")
    assert position_equity(Decimal("10") * cpu, Decimal("10"), Decimal("100"), Decimal("120")) == Decimal("700")


def test_native_cost_basis_non_usd_converts_at_open_fx() -> None:
    # 1000 USD invested, GBP-native, USD→GBP 0.8 at open → 800 native / 10 = 80.
    rates = {("USD", "GBP"): Decimal("0.8")}
    assert native_cost_basis(Decimal("1000"), Decimal("10"), "GBP", Decimal("100"), rates) == Decimal("80")


def test_native_cost_basis_falls_back_to_open_rate_when_fx_or_investment_missing() -> None:
    # Missing USD→EUR pair → fall back to open_rate (no currency-mix).
    assert native_cost_basis(Decimal("1000"), Decimal("10"), "EUR", Decimal("99"), {}) == Decimal("99")
    # NULL investment → fall back to open_rate.
    assert native_cost_basis(None, Decimal("10"), "USD", Decimal("99"), {}) == Decimal("99")


def test_position_equity_equals_close_times_units_when_unleveraged() -> None:
    """The recompute's native amount basis: amount = units*open_rate, so the
    MTM formula evaluates to close*units (the unleveraged identity) — currency-
    correct and continuous with the snapshot, without using account-ccy
    investment (Codex ckpt-2 P2)."""
    units = Decimal("16.929336")
    open_rate = Decimal("590.69")
    close = Decimal("681.25")
    amount = units * open_rate  # native invested capital (what the endpoint uses)
    assert position_equity(amount, units, open_rate, close) == close * units


def test_position_equity_diverges_from_close_times_units_when_leveraged() -> None:
    """And it must NOT collapse to close*units in general — a smaller committed
    amount (leverage/spread) gives a different equity, which is the whole point
    of mirroring the snapshot formula rather than pricing notional exposure."""
    units = Decimal("10")
    open_rate = Decimal("100")
    close = Decimal("120")
    amount = Decimal("500")  # half the unleveraged 1000 → 2x leverage
    eq = position_equity(amount, units, open_rate, close)
    assert eq == Decimal("700")  # 500 + 10*(120-100)
    assert eq != close * units  # 1200


def test_carry_forward_uses_seed_row_before_window_start() -> None:
    # Rate published before the window start carries into the first day.
    fx = [(D(2025, 1, 1), "USD", "GBP", Decimal("0.80"))]
    days = [D(2025, 1, 6), D(2025, 1, 7)]
    out = carry_forward_rate_map(fx, days)
    assert out[D(2025, 1, 6)][("USD", "GBP")] == Decimal("0.80")
    assert out[D(2025, 1, 7)][("USD", "GBP")] == Decimal("0.80")


def test_carry_forward_holds_last_rate_over_a_gap_and_updates() -> None:
    fx = [
        (D(2025, 1, 1), "USD", "GBP", Decimal("0.80")),
        (D(2025, 1, 3), "USD", "GBP", Decimal("0.82")),
    ]
    days = [D(2025, 1, 1), D(2025, 1, 2), D(2025, 1, 3), D(2025, 1, 4)]
    out = carry_forward_rate_map(fx, days)
    assert out[D(2025, 1, 2)][("USD", "GBP")] == Decimal("0.80")  # carried
    assert out[D(2025, 1, 4)][("USD", "GBP")] == Decimal("0.82")  # updated + carried


def test_carry_forward_absent_before_first_rate() -> None:
    fx = [(D(2025, 1, 5), "USD", "GBP", Decimal("0.80"))]
    days = [D(2025, 1, 1), D(2025, 1, 5)]
    out = carry_forward_rate_map(fx, days)
    assert ("USD", "GBP") not in out[D(2025, 1, 1)]  # day predates the pair → skip
    assert out[D(2025, 1, 5)][("USD", "GBP")] == Decimal("0.80")


def test_overlay_persisted_overrides_matching_currency_day() -> None:
    recomputed = {D(2025, 1, 1): Decimal("100"), D(2025, 1, 2): Decimal("110")}
    snaps = [(D(2025, 1, 2), Decimal("999"), "GBP")]
    out = overlay_persisted(recomputed, snaps, "GBP")
    assert out[D(2025, 1, 1)] == Decimal("100")  # untouched (recompute floor)
    assert out[D(2025, 1, 2)] == Decimal("999")  # persisted authoritative


def test_overlay_persisted_keeps_recompute_on_currency_mismatch() -> None:
    recomputed = {D(2025, 1, 2): Decimal("110")}
    snaps = [(D(2025, 1, 2), Decimal("999"), "USD")]  # snapshot in a different ccy
    out = overlay_persisted(recomputed, snaps, "GBP")
    assert out[D(2025, 1, 2)] == Decimal("110")  # not mislabelled


def test_overlay_persisted_ignores_out_of_range_snapshot() -> None:
    recomputed = {D(2025, 1, 2): Decimal("110")}
    snaps = [(D(2024, 12, 31), Decimal("999"), "GBP")]
    out = overlay_persisted(recomputed, snaps, "GBP")
    assert out == {D(2025, 1, 2): Decimal("110")}
