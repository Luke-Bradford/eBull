"""#2842 slice 3 — ranking-pot simulator and controls (spec §9.1, §9.2). Pure, no DB."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date
from decimal import Decimal
from fractions import Fraction

import pytest

from app.services import ranking_pot_sim as sim

D = Decimal
S1, S2, S3, S4, S5, S6, S7, S8 = (date(2026, 10, d) for d in (1, 2, 5, 6, 7, 8, 9, 12))


def _close(a: Decimal | None, b: Decimal) -> bool:
    """Equal up to rounding: the module computes at 34 digits, these expectations at Python's default 28."""
    return a is not None and abs(a - b) < D("1e-25")


def _bar(o: str, h: str, low: str, c: str) -> sim.Bar:
    return sim.Bar(D(o), D(h), D(low), D(c))


def _flat(c: str) -> sim.Bar:
    return _bar(c, c, c, c)


def _entry(iid: int, session: date = S1, h: str = "0", atr: str = "5", snap: str = "100") -> sim.PendingEntry:
    return sim.PendingEntry(iid, session, D(h), D(atr), D(snap), date(2026, 9, 30))


def _opened(n: int = 1, *, h: str = "0", atr: str = "5", close: str = "100") -> sim.BookState:
    """A book holding name 1, entered at S1's close."""
    book = sim.apply_rebalance(
        sim.new_book(n), target_session=S1, exits={}, entries=[_entry(1, h=h, atr=atr)], h_exit={}
    )
    return sim.step(book, session=S1, bars={1: _flat(close)}, reference_closes={}).state


# ---------------------------------------------------------------------------
# bars
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("bar", "ok"),
    [
        (_bar("10", "11", "9", "10"), True),
        (_bar("10", "10", "10", "10"), True),
        (_bar("0", "11", "0", "10"), False),  # zero price
        (_bar("-1", "11", "-2", "10"), False),
        (_bar("12", "11", "9", "10"), False),  # open above high
        (_bar("10", "9", "11", "10"), False),  # high < low
        (_bar("NaN", "11", "9", "10"), False),
        (_bar("10", "Infinity", "9", "10"), False),
        (None, False),
    ],
)
def test_valid_bar(bar: sim.Bar | None, ok: bool) -> None:
    assert sim.valid_bar(bar) is ok


# ---------------------------------------------------------------------------
# entry
# ---------------------------------------------------------------------------
def test_entry_fill_levels_and_entry_session_record() -> None:
    book = sim.apply_rebalance(sim.new_book(2), target_session=S1, exits={}, entries=[_entry(1, h="0.01")], h_exit={})
    r = sim.step(book, session=S1, bars={1: _flat("100")}, reference_closes={})
    (p,) = r.state.positions
    assert p.entry_fill == D("101") and p.stop_loss == D("86") and p.take_profit == D("131")
    assert p.slot == 0 and p.invested == D("0.5") and r.state.cash == (D(0), D("0.5"))
    (rec,) = r.position_sessions
    # The entry session carries the entry cost alone: 100 / 101 − 1.
    assert rec.start_value == D("0.5") and _close(rec.net_return(), D(100) / D(101) - 1)
    assert _close(r.nav, D("0.5") + D("0.5") * D(100) / D(101))


def test_entry_refusals() -> None:
    entries = [_entry(1), _entry(2, atr="40"), _entry(3)]
    book = sim.apply_rebalance(sim.new_book(3), target_session=S1, exits={}, entries=entries, h_exit={})
    r = sim.step(book, session=S1, bars={1: None, 2: _flat("100"), 3: _flat("100")}, reference_closes={})
    assert [(x.instrument_id, x.reason) for x in r.refusals] == [
        (1, "entry_bar_missing"),
        (2, "protective_levels_invalid"),  # 100 − 3 × 40 ≤ 0
    ]
    # r3-56: the refused entrants' slots stay cash; name 3 takes the lowest free slot.
    (p,) = r.state.positions
    assert p.instrument_id == 3 and p.slot == 0


def test_entry_session_missed_and_no_free_slot() -> None:
    book = sim.apply_rebalance(sim.new_book(1), target_session=S1, exits={}, entries=[_entry(1), _entry(2)], h_exit={})
    r = sim.step(book, session=S1, bars={1: _flat("100"), 2: _flat("50")}, reference_closes={})
    assert [(x.instrument_id, x.reason) for x in r.refusals] == [(2, "no_free_slot")]
    book2 = sim.apply_rebalance(sim.new_book(1), target_session=S1, exits={}, entries=[_entry(1)], h_exit={})
    r2 = sim.step(book2, session=S2, bars={1: _flat("100")}, reference_closes={})
    assert [(x.instrument_id, x.reason) for x in r2.refusals] == [(1, "entry_session_missed")]
    # An entry for a later session stays pending through an earlier step.
    book3 = sim.apply_rebalance(
        sim.new_book(1), target_session=S2, exits={}, entries=[_entry(1, session=S2)], h_exit={}
    )
    r3 = sim.step(book3, session=S1, bars={}, reference_closes={})
    assert r3.refusals == () and r3.state.pending == book3.pending
    r4 = sim.step(r3.state, session=S2, bars={1: _flat("100")}, reference_closes={})
    assert [p.instrument_id for p in r4.state.positions] == [1]


# ---------------------------------------------------------------------------
# protective exits (SL 85, TP 130 at entry 100, ATR 5)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("bar", "price", "reason"),
    [
        (_bar("80", "90", "79", "88"), "80", "stop_loss_gap"),
        (_bar("135", "140", "120", "125"), "135", "take_profit_gap"),
        (_bar("100", "131", "84", "100"), "85", "stop_loss"),  # both touched: SL by construction
        (_bar("100", "131", "90", "120"), "130", "take_profit"),
    ],
)
def test_protective_gap_ordering(bar: sim.Bar, price: str, reason: str) -> None:
    r = sim.step(_opened(), session=S2, bars={1: bar}, reference_closes={})
    (c,) = r.closed
    assert (c.exit_fill, c.reason) == (D(price), reason)
    assert r.state.positions == () and r.state.cash == (c.proceeds,)


@pytest.mark.parametrize(
    "bar",
    [_bar("80", "90", "79", "88"), _bar("135", "140", "120", "125"), _bar("100", "131", "84", "100")],
)
def test_the_variant_never_fires_a_protective_exit(bar: sim.Bar) -> None:
    """§9.4 slice 6c-ii-a: the no-SL/TP variant marks through every bar the shadow would exit on."""
    r = sim.step(_opened(), session=S2, bars={1: bar}, reference_closes={}, protective=False)
    (p,) = r.state.positions
    assert r.closed == () and p.value == p.units * bar.close and p.stop_loss == D(85)  # levels kept, unread


def test_the_variant_takes_the_entry_the_shadow_refuses_and_keeps_the_other_rules() -> None:
    entries = [_entry(1), _entry(2, atr="40")]  # 100 − 3 × 40 ≤ 0: the shadow refuses name 2
    book = sim.apply_rebalance(sim.new_book(2), target_session=S1, exits={}, entries=entries, h_exit={})
    bars = {1: None, 2: _flat("100")}
    assert [x.reason for x in sim.step(book, session=S1, bars=bars, reference_closes={}).refusals] == [
        "entry_bar_missing",
        "protective_levels_invalid",
    ]
    r = sim.step(book, session=S1, bars=bars, reference_closes={}, protective=False)
    assert [x.reason for x in r.refusals] == ["entry_bar_missing"]
    (p,) = r.state.positions
    assert p.instrument_id == 2 and p.stop_loss == D(-20)
    # A stamped exit still fires at the close, and the 5-session missing-bar exit still runs.
    stamped = sim.apply_rebalance(r.state, target_session=S2, exits={2: "rank_out"}, entries=[], h_exit={})
    out = sim.step(stamped, session=S2, bars={2: _flat("90")}, reference_closes={}, protective=False)
    assert [(c.reason, c.exit_fill) for c in out.closed] == [("rank_out", D(90))]
    steps = [sim.step(r.state, session=S2, bars={}, reference_closes={}, protective=False)]
    for s in (S3, S4, S5, S6):
        steps.append(sim.step(steps[-1].state, session=s, bars={}, reference_closes={}, protective=False))
    assert [len(x.closed) for x in steps] == [0, 0, 0, 0, 1] and steps[-1].closed[0].reason == "missing_bars"


def test_spec_gap_example_tp_at_open() -> None:
    """§10.3: SL 90, TP 120, open 125 → TP at 125."""
    book = _opened()
    (p,) = book.positions
    book = replace(book, positions=(replace(p, stop_loss=D(90), take_profit=D(120)),))
    r = sim.step(book, session=S2, bars={1: _bar("125", "126", "118", "121")}, reference_closes={})
    assert (r.closed[0].exit_fill, r.closed[0].reason) == (D("125"), "take_profit_gap")


def test_no_protective_check_on_entry_session() -> None:
    book = sim.apply_rebalance(sim.new_book(1), target_session=S1, exits={}, entries=[_entry(1)], h_exit={})
    r = sim.step(book, session=S1, bars={1: _bar("100", "200", "10", "100")}, reference_closes={})
    assert r.closed == () and len(r.state.positions) == 1


def test_exit_cost_uses_h_exit() -> None:
    book = _opened(h="0.01")
    book = sim.apply_rebalance(
        book, target_session=S2, exits={1: "rerank_out_of_band"}, entries=[], h_exit={1: D("0.02")}
    )
    r = sim.step(book, session=S2, bars={1: _flat("110")}, reference_closes={})
    (c,) = r.closed
    assert c.reason == "rerank_out_of_band" and c.exit_fill == D("110") * D("0.98")


# ---------------------------------------------------------------------------
# stamped exits, missing bars, wind-down
# ---------------------------------------------------------------------------
def test_protective_precedes_stamped_exit_same_session() -> None:
    book = sim.apply_rebalance(
        _opened(), target_session=S2, exits={1: "ineligible:not_tradable"}, entries=[], h_exit={}
    )
    r = sim.step(book, session=S2, bars={1: _bar("100", "100", "80", "95")}, reference_closes={})
    assert r.closed[0].reason == "stop_loss"


def test_stamped_exit_waits_for_a_valid_bar() -> None:
    book = sim.apply_rebalance(_opened(), target_session=S2, exits={1: "rerank_out_of_band"}, entries=[], h_exit={})
    r = sim.step(book, session=S2, bars={}, reference_closes={})
    assert r.closed == () and r.position_sessions[0].net_return() == 0  # marked at the last close
    r = sim.step(r.state, session=S3, bars={1: _flat("104")}, reference_closes={})
    assert (r.closed[0].exit_session, r.closed[0].exit_fill, r.closed[0].reason) == (S3, D("104"), "rerank_out_of_band")


def test_fifth_missing_session_exits_at_last_close() -> None:
    state = _opened(h="0.01")
    for s in (S2, S3, S4, S5):
        r = sim.step(state, session=s, bars={}, reference_closes={})
        assert r.closed == ()
        state = r.state
    r = sim.step(state, session=S6, bars={}, reference_closes={})
    (c,) = r.closed
    assert (c.exit_session, c.reason, c.exit_fill) == (S6, "missing_bars", D("100") * D("0.99"))


def test_missing_run_resets_on_a_valid_bar() -> None:
    state = _opened()
    for s, bar in ((S2, None), (S3, None), (S4, _flat("100")), (S5, None), (S6, None), (S7, None), (S8, None)):
        r = sim.step(state, session=s, bars={1: bar}, reference_closes={})
        assert r.closed == ()
        state = r.state


def test_wind_down_stamps_and_drops_pending() -> None:
    book = sim.apply_rebalance(_opened(2), target_session=S2, exits={}, entries=[_entry(2, session=S2)], h_exit={})
    book = sim.wind_down(book, session=S2)
    assert book.pending == () and book.positions[0].exit_stamp == (S2, "wind_down")
    r = sim.step(book, session=S2, bars={1: _flat("100")}, reference_closes={})
    assert r.closed[0].reason == "wind_down" and r.state.positions == ()


def test_existing_stamp_is_immutable() -> None:
    book = sim.apply_rebalance(_opened(), target_session=S3, exits={1: "rerank_out_of_band"}, entries=[], h_exit={})
    book = sim.wind_down(book, session=S2)
    assert book.positions[0].exit_stamp == (S3, "rerank_out_of_band")


# ---------------------------------------------------------------------------
# splits
# ---------------------------------------------------------------------------
def test_split_rescale_preserves_value_and_is_applied_once() -> None:
    state = _opened()  # units 1/100·1 slot → value 1
    (p0,) = state.positions
    # 2:1 split: the provider rewrites S1's close to 50.
    r = sim.step(state, session=S2, bars={1: _flat("50")}, reference_closes={1: D("50")})
    (p,) = r.state.positions
    assert r.rescales == ((1, D("0.5")),)
    assert p.units == p0.units * 2 and p.stop_loss == D("42.5") and p.take_profit == D("65")
    assert r.position_sessions[0].net_return() == 0
    # The next step reads S2's stored close (50) against the ledger's 50: no second rescale.
    r = sim.step(r.state, session=S3, bars={1: _flat("50")}, reference_closes={1: D("50")})
    assert r.rescales == ()


def test_split_between_snapshot_and_entry_rescales_atr() -> None:
    book = sim.apply_rebalance(sim.new_book(1), target_session=S1, exits={}, entries=[_entry(1, atr="5")], h_exit={})
    r = sim.step(book, session=S1, bars={1: _flat("50")}, reference_closes={1: D("50")})
    (p,) = r.state.positions
    assert p.stop_loss == D("42.5") and p.take_profit == D("65")


# ---------------------------------------------------------------------------
# rebalance application, ordering, NAV
# ---------------------------------------------------------------------------
def test_apply_rebalance_validation() -> None:
    state = _opened()
    with pytest.raises(ValueError, match="does not hold"):
        sim.apply_rebalance(state, target_session=S2, exits={9: "x"}, entries=[], h_exit={})
    with pytest.raises(ValueError, match="distinct"):
        sim.apply_rebalance(state, target_session=S2, exits={}, entries=[_entry(1, session=S2)], h_exit={})
    with pytest.raises(ValueError, match="another session"):
        sim.apply_rebalance(state, target_session=S2, exits={}, entries=[_entry(2, session=S3)], h_exit={})


def test_h_exit_updates_only_from_valid_spreads() -> None:
    state = _opened(h="0.01")
    state = sim.apply_rebalance(state, target_session=S2, exits={}, entries=[], h_exit={1: D("NaN")})
    assert state.positions[0].h_exit == D("0.01")


def test_each_step_is_the_next_nyse_session() -> None:
    with pytest.raises(ValueError, match="session after"):
        sim.step(_opened(), session=S1, bars={}, reference_closes={})
    with pytest.raises(ValueError, match="session after"):
        sim.step(_opened(), session=S3, bars={}, reference_closes={})  # skips Friday S2
    with pytest.raises(ValueError, match="not an NYSE session"):
        sim.step(_opened(), session=date(2026, 10, 3), bars={}, reference_closes={})  # Saturday
    assert sim.next_session(S2) == S3


def test_protective_exit_slot_is_not_reused_the_same_session() -> None:
    """Slot 0 holds name 1 (protective exit on S2), slot 1 holds name 2 (planned rank exit on S2). The entry takes
    slot 1, the planned one, although slot 0 has the lower index."""
    book = sim.apply_rebalance(sim.new_book(2), target_session=S1, exits={}, entries=[_entry(1), _entry(2)], h_exit={})
    book = sim.step(book, session=S1, bars={1: _flat("100"), 2: _flat("100")}, reference_closes={}).state
    book = sim.apply_rebalance(
        book, target_session=S2, exits={2: "rerank_out_of_band"}, entries=[_entry(3, session=S2)], h_exit={}
    )
    r = sim.step(
        book, session=S2, bars={1: _bar("80", "80", "70", "75"), 2: _flat("110"), 3: _flat("20")}, reference_closes={}
    )
    assert {c.instrument_id: c.slot for c in r.closed} == {1: 0, 2: 1}
    (p,) = r.state.positions
    assert (p.instrument_id, p.slot, p.invested) == (3, 1, D("0.55"))
    assert r.state.cash[0] == D("0.4")  # 0.5 / 100 units × 80 gap fill: stays cash


def test_rank_exit_frees_slot_for_same_session_entry() -> None:
    book = sim.apply_rebalance(
        _opened(), target_session=S2, exits={1: "rerank_out_of_band"}, entries=[_entry(2, session=S2)], h_exit={}
    )
    r = sim.step(book, session=S2, bars={1: _flat("110"), 2: _flat("20")}, reference_closes={})
    (p,) = r.state.positions
    assert p.instrument_id == 2 and p.slot == 0 and p.invested == D("1.1")
    assert r.nav == D("1.1")


# ---------------------------------------------------------------------------
# T, look valuation
# ---------------------------------------------------------------------------
def test_t_statistic_and_liquidation_charge() -> None:
    book = sim.apply_rebalance(sim.new_book(1), target_session=S1, exits={}, entries=[_entry(1, h="0.01")], h_exit={})
    first = sim.step(book, session=S1, bars={1: _flat("100")}, reference_closes={})
    r = sim.step(first.state, session=S2, bars={1: _flat("110")}, reference_closes={})
    records = [*first.position_sessions, *r.position_sessions]
    assert _close(sim.t_statistic(records), ((D(100) / D(101) - 1) + (D(110) / D(100) - 1)) / 2)
    charged = sim.liquidation_charged(r.state, r.position_sessions)
    assert _close(charged[0].net_return(), D("1.1") * D("0.99") - 1)
    assert r.state.positions[0].value == r.position_sessions[0].end_value  # the ledger is untouched
    assert _close(sim.charged_nav(r.state), r.nav * D("0.99"))
    assert sim.t_statistic([]) is None


# ---------------------------------------------------------------------------
# §9.2 draw and p-values
# ---------------------------------------------------------------------------
SEED = sim.control_seed("ab" * 32, "cd" * 32)


def test_draw_is_a_reproducible_bijection() -> None:
    ids = [5, 1, 9, 3, 7, 11, 2]
    a = sim.draw_bijection(ids, seed=SEED, k=1)
    assert a == sim.draw_bijection(list(reversed(ids)), seed=SEED, k=1)  # input order is irrelevant
    assert sorted(a) == sorted(ids) and sorted(a.values()) == sorted(ids)
    assert a != sim.draw_bijection(ids, seed=SEED, k=2)


def test_draw_is_pinned_to_the_documented_encoding() -> None:
    """Freezes r3-7: an independent re-implementation of the module docstring's encoding, plus the literal result."""

    def reference(ids: list[int], seed: bytes, k: int) -> dict[int, int]:
        donors = sorted(ids)
        for j in range(len(donors) - 1, 0, -1):
            c = 0
            while True:
                x = int.from_bytes(
                    hashlib.sha256(seed + k.to_bytes(4, "big") + j.to_bytes(4, "big") + c.to_bytes(4, "big")).digest(),
                    "big",
                )
                if x < (1 << 256) - ((1 << 256) % (j + 1)):
                    break
                c += 1
            i = x % (j + 1)
            donors[i], donors[j] = donors[j], donors[i]
        return dict(zip(sorted(ids), donors, strict=True))

    assert SEED == hashlib.sha256(bytes.fromhex("ab" * 32) + bytes.fromhex("cd" * 32)).digest()
    got = sim.draw_bijection(range(1, 9), seed=SEED, k=1)
    assert got == reference(list(range(1, 9)), SEED, 1)
    assert got == {1: 4, 2: 2, 3: 1, 4: 8, 5: 7, 6: 5, 7: 6, 8: 3}


def test_draw_is_roughly_uniform() -> None:
    counts = [0, 0, 0]
    for k in range(1, 3001):
        counts[sim.draw_bijection([1, 2, 3], seed=SEED, k=k)[1] - 1] += 1
    assert all(900 < c < 1100 for c in counts)


def test_control_seed_validation() -> None:
    with pytest.raises(ValueError):
        sim.control_seed("ab", "cd" * 32)
    with pytest.raises(ValueError):
        sim.draw_bijection([1, 2], seed=SEED, k=0)


def test_p_values() -> None:
    t_obs = D("0.01")
    controls: list[Decimal | None] = [D("0.02"), D("0.01"), D("0.00"), None, D("NaN")]
    p = sim.p_values(t_obs, controls)
    assert p == sim.PValues(p_up=Fraction(1 + 4, 6), p_down=Fraction(1 + 4, 6))
    assert sim.p_values(None, controls) == "unevaluable"
    assert sim.p_values(D("NaN"), controls) == "unevaluable"
    assert sim.p_values(D("1"), [D("0")] * 9) == sim.PValues(Fraction(1, 10), Fraction(1))
