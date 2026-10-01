"""#3540 engine NAV bridge — pure table tests over ``bridge_intervals``.

Spec: ``docs/proposals/execution/2026-10-01-3540-engine-nav-bridge.md``. The live window has no
partial close, short, refund, round trip or non-USD position, so those are exercised here.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services.engine_nav_bridge import (
    BridgeInterval,
    CloseEvent,
    Mark,
    OpenEvent,
    OwnedPosition,
    PrincipalEvent,
    Snapshot,
    bridge_intervals,
    rate_quantum,
)

D = Decimal
T0 = datetime(2026, 9, 21, 21, 0, tzinfo=UTC)
T1 = T0 + timedelta(days=1)
DAY0, DAY1 = T0.date(), T1.date()
SNAPS = [Snapshot(DAY0, T0), Snapshot(DAY1, T1)]
PRINCIPAL = [PrincipalEvent(T0 - timedelta(days=5), 1, D("500"))]


def _mark(pid: int, units: str, pnl: str, rate: str, **kw: object) -> Mark:
    values: dict[str, object] = {
        "is_buy": True,
        "conversion_rate": D("1"),
        "asset_currency_id": 1,
        "is_partially_altered": False,
        "total_fees": D("0"),
    }
    values.update(kw)
    return Mark(position_id=pid, units=D(units), pnl=D(pnl), close_rate=D(rate), **values)  # type: ignore[arg-type]


def _position(
    pid: int,
    *,
    opened: datetime,
    units: str = "1",
    price: str = "100",
    closes: tuple[CloseEvent, ...] = (),
    currency: int = 1,
    closed_fees: str | None = None,
) -> OwnedPosition:
    return OwnedPosition(
        position_id=pid,
        core_linked=True,
        claimed_at=opened,
        open=OpenEvent(opened, D(units), D(price), D(units) * D(price)),
        closes=closes,
        leverage=1,
        asset_currency_id=currency,
        closed_total_fees=None if closed_fees is None else D(closed_fees),
    )


def _close(
    at: datetime, units: str, net: str, open_rate: str = "100", close_rate: str = "110", **kw: object
) -> CloseEvent:
    values: dict[str, object] = {"fees_usd": D("0"), "is_buy": True}
    values.update(kw)
    return CloseEvent(
        executed_at=at,
        units=D(units),
        net_profit=D(net),
        open_rate=D(open_rate),
        close_rate=D(close_rate),
        **values,  # type: ignore[arg-type]
    )


def _one(marks: dict[date, dict[int, Mark]], positions: list[OwnedPosition], **kw: object) -> BridgeInterval:
    intervals = bridge_intervals(SNAPS, marks, positions, kw.pop("principal", PRINCIPAL), **kw)  # type: ignore[arg-type]
    assert len(intervals) == 1
    return intervals[0]


def _assert_additive(interval: BridgeInterval) -> None:
    assert interval.opening_nav is not None and interval.closing_nav is not None
    parts = (
        interval.flows,
        interval.realised,
        interval.unrealised_released,
        interval.unrealised_opened,
        interval.unrealised_continuing,
    )
    assert all(part is not None for part in parts)
    assert interval.opening_nav + sum(parts, D(0)) == interval.closing_nav  # type: ignore[arg-type]
    assert interval.fx_effect is not None and interval.price_effect is not None
    assert interval.fx_effect + interval.price_effect == interval.unrealised_continuing


def test_continuing_long_closes_and_validates() -> None:
    pos = _position(1, opened=T0 - timedelta(days=2), units="2")
    marks = {DAY0: {1: _mark(1, "2", "10.00", "105")}, DAY1: {1: _mark(1, "2", "16.00", "108")}}
    interval = _one(marks, [pos])
    assert interval.closed and interval.validated
    assert interval.opening_nav == D("510.00") and interval.closing_nav == D("516.00")
    assert interval.unrealised_continuing == D("6.00") and interval.fx_effect == 0
    # First interval: opening marks and earlier opens are checked too.
    assert {r.code for r in interval.residuals} == {"mark_pnl", "units_conserved", "open_investment"}
    _assert_additive(interval)


def test_open_inside_interval_is_unrealised_opened_and_checks_investment() -> None:
    pos = _position(2, opened=T0 + timedelta(hours=3), units="0.5", price="200")
    interval = _one({DAY0: {}, DAY1: {2: _mark(2, "0.5", "1.00", "202")}}, [pos])
    assert interval.validated
    assert interval.unrealised_opened == D("1.00")
    assert any(r.code == "open_investment" and not r.exceeds for r in interval.residuals)
    _assert_additive(interval)


def test_full_close_moves_prior_unrealised_out_and_netprofit_in() -> None:
    close = _close(T0 + timedelta(hours=2), "1", "10.00")
    pos = _position(3, opened=T0 - timedelta(days=3), closes=(close,), closed_fees="0")
    interval = _one({DAY0: {3: _mark(3, "1", "8.00", "108")}, DAY1: {}}, [pos])
    assert interval.validated
    assert interval.realised == D("10.00") and interval.unrealised_released == D("-8.00")
    assert interval.closing_nav == D("510.00")
    codes = {r.code for r in interval.residuals}
    assert codes == {"close_netprofit", "close_open_rate", "mark_pnl", "open_investment"}
    _assert_additive(interval)


def test_partial_close_conserves_units() -> None:
    close = _close(T0 + timedelta(hours=2), "0.4", "4.00")
    pos = _position(4, opened=T0 - timedelta(days=3), closes=(close,))
    marks = {DAY0: {4: _mark(4, "1", "8.00", "108")}, DAY1: {4: _mark(4, "0.6", "6.00", "110")}}
    interval = _one(marks, [pos])
    assert interval.validated
    assert interval.realised == D("4.00") and interval.unrealised_continuing == D("-2.00")
    _assert_additive(interval)


def test_units_changing_without_a_close_is_a_residual() -> None:
    pos = _position(5, opened=T0 - timedelta(days=3))
    marks = {DAY0: {5: _mark(5, "1", "8.00", "108")}, DAY1: {5: _mark(5, "0.9", "7.20", "108")}}
    interval = _one(marks, [pos])
    assert interval.closed and not interval.validated
    assert [r.code for r in interval.residuals if r.exceeds] == ["units_conserved"]


def test_same_interval_round_trip_is_realised_only() -> None:
    close = _close(T0 + timedelta(hours=5), "1", "2.00", close_rate="102")
    pos = _position(6, opened=T0 + timedelta(hours=1), closes=(close,), closed_fees="0")
    interval = _one({DAY0: {}, DAY1: {}}, [pos])
    assert interval.validated
    assert interval.realised == D("2.00")
    assert interval.unrealised_opened == 0 and interval.unrealised_released == 0
    _assert_additive(interval)


def test_unmarked_member_breaks_closure_without_a_number() -> None:
    pos = _position(7, opened=T0 - timedelta(days=3))
    interval = _one({DAY0: {7: _mark(7, "1", "8.00", "108")}, DAY1: {}}, [pos])
    assert not interval.closed and not interval.validated
    assert interval.closing_nav is None and interval.realised is None
    assert ("owned_position_unmarked", 7) in {(s.code, s.position_id) for s in interval.states}


def test_closed_position_still_marked_breaks_closure() -> None:
    close = _close(T0 - timedelta(hours=2), "1", "10.00")
    pos = _position(8, opened=T0 - timedelta(days=3), closes=(close,))
    interval = _one({DAY0: {}, DAY1: {8: _mark(8, "1", "8.00", "108")}}, [pos])
    assert not interval.closed
    assert "closed_position_still_marked" in {s.code for s in interval.states}


@pytest.mark.parametrize(
    ("fees", "memo"),
    [("0", "fees_zero"), ("-0.40", "fees_nonzero_inclusion_unresolved"), (None, "fees_unobserved")],
)
def test_fee_memo_never_chooses_an_inclusion_treatment(fees: str | None, memo: str) -> None:
    pos = _position(9, opened=T0 - timedelta(days=3))
    value = None if fees is None else D(fees)
    marks = {
        DAY0: {9: _mark(9, "1", "8.00", "108", total_fees=value)},
        DAY1: {9: _mark(9, "1", "9.00", "109", total_fees=value)},
    }
    interval = _one(marks, [pos])
    assert interval.fees_memo == memo
    assert interval.closed  # the memo never enters the statement
    assert interval.validated is (memo == "fees_zero")
    assert interval.closing_nav == D("509.00")


def test_closed_position_final_fee_counter_is_read() -> None:
    close = _close(T0 + timedelta(hours=2), "1", "10.00")
    pos = _position(10, opened=T0 - timedelta(days=3), closes=(close,), closed_fees=None)
    interval = _one({DAY0: {10: _mark(10, "1", "8.00", "108")}, DAY1: {}}, [pos])
    assert interval.fees_memo == "fees_unobserved"
    assert (10, None) in interval.fees_detail


def test_short_mark_uses_the_direction_sign() -> None:
    pos = _position(11, opened=T0 - timedelta(days=3))
    marks = {
        DAY0: {11: _mark(11, "1", "-2.00", "102", is_buy=False)},
        DAY1: {11: _mark(11, "1", "5.00", "95", is_buy=False)},
    }
    interval = _one(marks, [pos])
    assert interval.validated
    mark_check = next(r for r in interval.residuals if r.code == "mark_pnl")
    assert mark_check.recomputed == D("5")


def test_non_usd_is_not_recomputable_but_fx_is_split() -> None:
    pos = _position(12, opened=T0 - timedelta(days=3), currency=2)
    marks = {
        DAY0: {12: _mark(12, "10", "0.00", "100", conversion_rate=D("1.25"), asset_currency_id=2)},
        DAY1: {12: _mark(12, "10", "12.60", "100", conversion_rate=D("1.26"), asset_currency_id=2)},
    }
    interval = _one(marks, [pos])
    assert interval.closed and not interval.validated
    assert "not_recomputable" in {s.code for s in interval.states}
    assert interval.fx_effect == D("10.00") and interval.price_effect == D("2.60")


def test_mark_beyond_its_bound_is_named_with_its_amount() -> None:
    pos = _position(13, opened=T0 - timedelta(days=3))
    marks = {DAY0: {13: _mark(13, "1", "8.00", "108")}, DAY1: {13: _mark(13, "1", "9.50", "109")}}
    interval = _one(marks, [pos])
    bad = next(r for r in interval.residuals if r.code == "mark_pnl")
    assert bad.exceeds and bad.amount == D("0.50")
    assert interval.closed and not interval.validated


def test_principal_flow_and_missing_principal() -> None:
    pos = _position(14, opened=T0 - timedelta(days=3))
    marks = {DAY0: {14: _mark(14, "1", "8.00", "108")}, DAY1: {14: _mark(14, "1", "8.00", "108")}}
    flows = [*PRINCIPAL, PrincipalEvent(T0 + timedelta(hours=1), 2, D("40000"))]
    interval = _one(marks, [pos], principal=flows)
    assert interval.flows == D("39500")
    _assert_additive(interval)
    later = [PrincipalEvent(T1 - timedelta(hours=1), 1, D("500"))]
    assert _one(marks, [pos], principal=later).states[0].code == "principal_unobserved"


def test_trade_without_ownership_breaks_closure() -> None:
    pos = _position(15, opened=T0 - timedelta(days=3))
    marks = {DAY0: {15: _mark(15, "1", "8.00", "108")}, DAY1: {15: _mark(15, "1", "8.00", "108")}}
    interval = _one(marks, [pos], trades_without_ownership=1)
    assert not interval.closed


@pytest.mark.parametrize(
    ("rate", "quantum"),
    [("762.8", "0.01"), ("761.59000000", "0.01"), ("1.23456", "0.00001"), ("700", "0.01")],
)
def test_rate_quantum(rate: str, quantum: str) -> None:
    assert rate_quantum(D(rate)) == D(quantum)


def test_owned_position_without_an_open_event_is_named_from_its_claim() -> None:
    orphan = OwnedPosition(16, True, T0 - timedelta(days=1), None, (), 1, 1, None)
    interval = _one({DAY0: {}, DAY1: {}}, [orphan])
    assert not interval.closed
    assert ("open_event_missing", 16) in {(s.code, s.position_id) for s in interval.states}


def test_over_close_breaks_closure() -> None:
    close = _close(T0 + timedelta(hours=2), "1.5", "15.00")
    pos = _position(17, opened=T0 - timedelta(days=3), closes=(close,), closed_fees="0")
    interval = _one({DAY0: {17: _mark(17, "1", "8.00", "108")}, DAY1: {}}, [pos])
    assert not interval.closed
    assert "units_over_closed" in {s.code for s in interval.states}


def test_usd_mark_with_a_non_unit_conversion_is_not_recomputable() -> None:
    pos = _position(18, opened=T0 - timedelta(days=3))
    marks = {
        DAY0: {18: _mark(18, "1", "8.00", "108")},
        DAY1: {18: _mark(18, "1", "8.00", "108", conversion_rate=D("1.01"))},
    }
    interval = _one(marks, [pos])
    assert not interval.validated
    assert "not_recomputable" in {s.code for s in interval.states}


def test_pre_window_close_missing_profit_is_named() -> None:
    old = CloseEvent(T0 - timedelta(days=1), D("1"), None, D("0"), D("100"), D("101"), True)
    gone = _position(19, opened=T0 - timedelta(days=4), closes=(old,), closed_fees="0")
    interval = _one({DAY0: {}, DAY1: {}}, [gone])
    assert not interval.closed
    assert ("close_netprofit_missing", 19) in {(s.code, s.position_id) for s in interval.states}


def test_null_open_price_is_not_recomputable_not_a_crash() -> None:
    pos = OwnedPosition(
        20, True, T0 - timedelta(days=3), OpenEvent(T0 - timedelta(days=3), D("1"), None, None), (), 1, 1, None
    )
    marks = {DAY0: {20: _mark(20, "1", "8.00", "108")}, DAY1: {20: _mark(20, "1", "9.00", "109")}}
    interval = _one(marks, [pos])
    assert interval.closed and not interval.validated
    assert "not_recomputable" in {s.code for s in interval.states}


def test_first_interval_checks_its_opening_marks() -> None:
    pos = _position(21, opened=T0 - timedelta(days=3))
    marks = {DAY0: {21: _mark(21, "1", "9.00", "108")}, DAY1: {21: _mark(21, "1", "8.00", "108")}}
    interval = _one(marks, [pos])
    assert [r.amount for r in interval.residuals if r.code == "mark_pnl" and r.exceeds] == [D("1.00")]
    assert not interval.validated
