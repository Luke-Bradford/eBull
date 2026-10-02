"""#2842 slice 6c-ii-c-2b — executed vs shadow and the executed book's NAV vs SPY (pure).

``tests/test_ranking_pot_executor_db.py`` covers the SQL path."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any

import pytest

from app.services import ranking_pot_exec_readout as xr
from app.services import ranking_pot_loss as loss
from app.services import ranking_pot_sim as sim

T = date(2026, 10, 7)  # a Wednesday session
QUOTE = datetime(2026, 10, 7, 15, 0, tzinfo=UTC)
FILL_AT = datetime(2026, 10, 7, 15, 2, tzinfo=UTC)


def _open(price: str, at: datetime = FILL_AT, units: str = "1") -> xr.OpenFact:
    return xr.OpenFact(Decimal(price), at, Decimal(units))


def _close(at: datetime, net: str, units: str = "1") -> xr.CloseFact:
    return xr.CloseFact(at, loss.PotClose(Decimal(net), Decimal(0), Decimal(units)))


def _entry(**changes: Any) -> xr.EntryFacts:
    base: dict[str, Any] = {
        "lifecycle_id": 1,
        "instrument_id": 2842,
        "slot": 1,
        "half_spread": Fraction(1, 200),
        "funding_verdict": "allocated",
        "reason_code": "allocated",
        "trade_status": "open",
        "requested_amount": Decimal(40),
        "amount": Decimal(40),
        "ask": Decimal(100),
        "quote_at": QUOTE,
        "positions": (xr.PositionFacts(9001, (_open("101"),), ()),),
    }
    return xr.EntryFacts(**(base | changes))


def test_an_entry_compares_its_fill_with_the_ask_and_the_shadow_price() -> None:
    row = xr.entry_row(_entry(), target=T, endpoint=T, close_t=Decimal(100))
    assert row["status"] == "open" and row["refusal"] is None
    assert Decimal(row["fill_vs_ask"]) == Decimal("0.01")
    # The shadow's entry price is close_T × (1 + h) = 100.5.
    with localcontext(sim.CTX):
        assert Decimal(row["fill_vs_reference"]) == Decimal(101) / Decimal("100.5") - 1
    assert Decimal(row["seconds_after_quote"]) == 120 and row["fill_session_is_target"] is True
    # A missing target bar leaves only the shadow comparison undefined.
    assert xr.entry_row(_entry(), target=T, endpoint=T, close_t=None)["fill_vs_reference"] is None
    # A fill on the next New York day is not on the target session.
    late = _entry(positions=(xr.PositionFacts(9001, (_open("101", datetime(2026, 10, 8, 14, tzinfo=UTC)),), ()),))
    assert xr.entry_row(late, target=T, endpoint=T, close_t=Decimal(100))["fill_session_is_target"] is False


@pytest.mark.parametrize(
    ("positions", "reason"),
    [
        ((), "no_position"),
        ((xr.PositionFacts(1, (), ()), xr.PositionFacts(2, (), ())), "multiple_positions"),
        ((xr.PositionFacts(1, (), ()),), "open_event_missing"),
        ((xr.PositionFacts(1, (_open("1"), _open("2")), ()),), "multiple_open_events"),
        ((xr.PositionFacts(1, (_open("0"),), ()),), "fill_price_unusable"),
    ],
)
def test_a_fill_needs_exactly_one_position_and_one_open(positions: tuple[Any, ...], reason: str) -> None:
    row = xr.entry_row(_entry(positions=positions), target=T, endpoint=T, close_t=Decimal(100))
    assert (row["fill"], row["fill_reason"], row["fill_vs_ask"]) == (None, reason, None)


def test_statuses_and_refusals() -> None:
    refused = _entry(funding_verdict="rejected", reason_code="pot_cost_cap", trade_status=None, positions=())
    row = xr.entry_row(refused, target=T, endpoint=T, close_t=Decimal(100))
    assert (row["status"], row["refusal"]) == ("refused", "pot_cost_cap")
    unattempted = _entry(funding_verdict=None, reason_code=None, trade_status=None, positions=())
    assert xr.entry_row(unattempted, target=T, endpoint=T, close_t=None)["status"] == "entry_pending"
    assert xr.entry_row(unattempted, target=T, endpoint=date(2026, 10, 8), close_t=None)["status"] == "expired"


def test_the_rebalance_summary() -> None:
    rb = xr.ExecutedRebalance(
        attempt_id=7,
        target=T,
        state="executing",
        entries_allowed=True,
        v1_active=False,
        detail={"held": 0, "entries": 2, "exits": 0, "slots_unfilled": 23},
        entries=(
            _entry(lifecycle_id=2, instrument_id=2843, funding_verdict="rejected", reason_code="x", positions=()),
            _entry(),
        ),
        shadow_decision={"entries": [2842, 2844, 2845], "slots_unfilled": 22},
        closes={2842: Decimal(100)},
    )
    out = xr.executed_vs_shadow([rb], T)
    per = out["per_rebalance"][0]
    assert [e["lifecycle_id"] for e in per["entries"]] == [1, 2]
    assert per["decided_entry_overlap"] == 1 and per["shadow"] == {"entries": 3, "slots_unfilled": 22}
    assert out["statuses"] == {"open": 1, "refused": 1} and out["refusals"] == {"x": 1}
    assert out["fills"] == 1 and Decimal(out["fill_vs_ask_median"]) == Decimal("0.01")
    assert out["slots_unfilled"] == {"executed": 23, "shadow": 22, "compared": 1, "shadow_missing": 0}
    # Before the shadow's step row exists, its side is null and the sums leave the rebalance out.
    pending = xr.executed_vs_shadow([replace(rb, shadow_decision=None, closes={})], T)
    assert (
        pending["per_rebalance"][0]["shadow"] is None and pending["per_rebalance"][0]["decided_entry_overlap"] is None
    )
    assert pending["slots_unfilled"] == {"executed": 0, "shadow": 0, "compared": 0, "shadow_missing": 1}
    assert pending["fill_vs_reference_count"] == 0


def _mark(pnl: str, units: str = "1", altered: bool = False) -> xr.StoredMark:
    return xr.StoredMark(True, Decimal(units), Decimal(pnl), Decimal(0), altered)


SUBMITTED = datetime(2026, 10, 7, 15, 1, tzinfo=UTC)
P1 = datetime(2026, 10, 7, 23, 0, tzinfo=UTC)
P2 = datetime(2026, 10, 8, 23, 0, tzinfo=UTC)


def _trade(positions: tuple[xr.PositionFacts, ...], status: str = "open") -> xr.PotTrade:
    return xr.PotTrade(1, status, SUBMITTED, positions)


def test_net_at_reads_only_what_had_happened_by_the_instant() -> None:
    pos = xr.PositionFacts(9001, (_open("100"),), (_close(datetime(2026, 10, 8, 16, tzinfo=UTC), "3"),))
    trade = _trade((pos,), status="closed")
    # P1: open, marked, the close still ahead.
    assert xr.net_at([trade], {9001: _mark("2")}, P1) == Decimal(2)
    # P2: fully closed, no mark.
    assert xr.net_at([trade], {}, P2) == Decimal(3)
    # Before the submission nothing is counted.
    assert xr.net_at([trade], {}, datetime(2026, 10, 7, 14, tzinfo=UTC)) == Decimal(0)


def test_net_at_is_strict() -> None:
    # Open by P1 but unmarked and unclosed: unreconciled (the §7.4 brake would charge a bound instead).
    pos = xr.PositionFacts(9001, (_open("100"),), ())
    assert xr.net_at([_trade((pos,))], {}, P1) == "unreconciled"
    # Submitted, no position yet and not failed: the outcome at P1 is unknown.
    assert xr.net_at([_trade(())], {}, P1) == "unreconciled"
    assert xr.net_at([_trade((), status="failed")], {}, P1) == Decimal(0)
    # Filled after P1: nothing was invested at P1.
    later = xr.PositionFacts(9001, (_open("100", datetime(2026, 10, 8, 15, tzinfo=UTC)),), ())
    assert xr.net_at([_trade((later,))], {}, P1) == Decimal(0)
    # A mark whose units disagree with the open: unreconciled; a short mark: a defect.
    assert xr.net_at([_trade((pos,))], {9001: _mark("2", units="2")}, P1) == "unreconciled"
    assert xr.net_at([_trade((pos,))], {9001: xr.StoredMark(False, Decimal(1), Decimal(1), Decimal(0), False)}, P1) == (
        "data_defect"
    )
    # Two opens for one position never reconcile; an owned position with no open event at all is unknown.
    twice = xr.PositionFacts(9001, (_open("100"), _open("100")), ())
    assert xr.net_at([_trade((twice,))], {9001: _mark("2")}, P1) == "unreconciled"
    assert xr.net_at([_trade((xr.PositionFacts(9001, (), ()),))], {}, P1) == "unreconciled"
    # An open without positive units is a defect.
    assert xr.net_at([_trade((xr.PositionFacts(9001, (_open("100", units="0"),), ()),))], {}, P1) == "data_defect"
    # One position owned by two pot trades.
    assert xr.net_at([_trade((pos,)), _trade((pos,))], {9001: _mark("2")}, P1) == "data_defect"


def test_nav_vs_spy() -> None:
    activated = datetime(2026, 10, 6, 12, tzinfo=UTC)  # base: Monday 10-05's close
    pos = xr.PositionFacts(9001, (_open("100"),), ())
    spy = {date(2026, 10, 5): Decimal(500), date(2026, 10, 7): Decimal(510)}
    out = xr.nav_vs_spy(
        activated_at=activated,
        pot_capital=Decimal(1000),
        trades=[_trade((pos,))],
        points=[
            xr.SnapshotPoint(date(2026, 10, 7), P1, {9001: _mark("20")}),
            xr.SnapshotPoint(date(2026, 10, 8), P2, {}),
        ],
        spy_closes=spy,
        endpoint=date(2026, 10, 8),
    )
    first, second = out["points"]
    assert (Decimal(first["nav"]), Decimal(first["spy"])) == (Decimal("1.02"), Decimal("1.02"))
    assert Decimal(first["difference"]) == 0 and (first["nav_reason"], first["spy_reason"]) == (None, None)
    # P2: unmarked → unreconciled; 10-08's SPY close is not stored → SPY null too, each with its own reason.
    assert (second["nav"], second["spy"]) == (None, None)
    assert (second["nav_reason"], second["spy_reason"]) == ("unreconciled", "spy_unavailable")
    assert out["last"] == first and out["pot_capital"] == "1000" and out["spy_base_session"] == "2026-10-05"
    # A point whose SPY session is after E is outside the window.
    early = xr.nav_vs_spy(
        activated_at=activated,
        pot_capital=Decimal(1000),
        trades=[],
        points=[xr.SnapshotPoint(date(2026, 10, 8), P2, {})],
        spy_closes=spy,
        endpoint=date(2026, 10, 7),
    )
    assert early["points"] == [] and early["last"] is None
    # An account not observed as USD (sql/341: NULL = assumed) is never summed into a USD NAV.
    for currency_id in (None, 2):
        other = xr.nav_vs_spy(
            activated_at=activated,
            pot_capital=Decimal(1000),
            trades=[],
            points=[xr.SnapshotPoint(date(2026, 10, 7), P1, {}, currency_id)],
            spy_closes=spy,
            endpoint=T,
        )
        assert (other["points"][0]["nav"], other["points"][0]["nav_reason"]) == (None, "not_usd")
    with pytest.raises(ValueError, match="not positive"):
        xr.nav_vs_spy(activated_at=activated, pot_capital=Decimal(0), trades=[], points=[], spy_closes=spy, endpoint=T)
