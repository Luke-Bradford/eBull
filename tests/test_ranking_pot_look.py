"""#2842 slice 6b — the §9.3 looks' pure parts (spec §9.3, "The looks" paragraph)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any

import pytest

from app.services import ranking_pot_look as look
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_step as st
from app.services.ranking_pot_policy import HARM_ALPHA, LOOK_MONTHS
from tests.test_ranking_pot_step import FLAT, FRI, MON, THU, TUE, _bars, _book, _rebalance

D = Decimal
SPY = sim.Bar(D(500), D(501), D(499), D(500))
UP = sim.Bar(D(100), D(104), D(100), D(104))


def _rows(controls: int = 2) -> list[look.StepRow]:
    """FRI = T₀ (the shadow and every identity-donor control enter names 1 and 2), MON, TUE = E. Real step outputs."""
    r = {1: "0.9", 2: "0.8", 3: "0.7"}
    rebalance = _rebalance(FRI, THU, r, {1, 2, 3})
    books = [_book() for _ in range(controls + 1)]
    rows = []
    closes = {i: {THU: D(100), FRI: D(100), MON: D(100)} for i in (1, 2)}
    for session, bar in ((FRI, FLAT), (MON, FLAT), (TUE, UP)):
        cols = st.ControlColumns()
        shadow: dict[str, Any] = {}
        for b, book in enumerate(books):
            book, result, decision = st.advance(
                book,
                session=session,
                bars=_bars(session, {1: bar, 2: bar}, closes),
                rebalance=rebalance if session == FRI else None,
                donor_of=None if b == 0 else {1: 1, 2: 2, 3: 3},
                wind=False,
                used=set(),
            )
            books[b] = book
            if b == 0:
                shadow = st.shadow_doc(result, decision)
            else:
                cols.add(result)
        rows.append(look.StepRow(session, False, shadow, cols.doc(), SPY))
    return rows


def _facts(rows: list[look.StepRow], k: int = 2, end: date = TUE) -> look.LookFacts:
    facts = look.LookFacts(t0=FRI, endpoint=end, k=k)
    for row in rows:
        facts.add(row)
    return facts


#: K = 3 in the handmade facts, so the smallest p is 1/4: the efficacy bar is set there to make a pass reachable.
TERMS = look.Terms(n=2, k=3, per_look_alpha=Fraction(1, 4), harm_alpha=HARM_ALPHA)
EXEC_OK = look.Execution(executing=look.EXECUTION_MIN, lifecycles=4)
EXEC_NONE = look.Execution(executing=timedelta(0), lifecycles=0)


def test_endpoints_advance_the_year_and_roll_to_the_next_session() -> None:
    assert look.anniversary(date(2026, 11, 2), 12) == date(2027, 11, 2)
    assert look.anniversary(date(2028, 2, 29), 12) == date(2029, 3, 1)  # 29 February does not exist
    assert look.anniversary(date(2026, 11, 2), 24) == date(2028, 11, 2)
    assert look.endpoint(date(2026, 10, 3), 12) == date(2027, 10, 4)  # Sunday 2027-10-03 → Monday
    with pytest.raises(ValueError):
        look.anniversary(FRI, 6)
    assert LOOK_MONTHS == (12, 24)


def test_identical_ledgers_give_identical_t_and_the_endpoint_is_charged() -> None:
    rows = _rows()
    facts = _facts(rows)
    t_obs = facts.shadow_sum / facts.shadow_count
    assert [s / c for s, c in zip(facts.control_sum, facts.control_count, strict=True)] == [t_obs, t_obs]
    assert facts.shadow_count == 6 and facts.control_count == [6, 6]
    # The endpoint's charged NAV closes the path: below the uncharged NAV it would otherwise carry.
    end = rows[-1].shadow
    assert facts.shadow_path == [D(1), D(rows[0].shadow["nav"]), D(rows[1].shadow["nav"]), D(end["nav_charged"])]
    assert D(end["nav_charged"]) < D(end["nav"])
    # Every lifecycle's terminal value is its charged endpoint value (both still open at E).
    assert {lc: v for lc, v in facts.terminal.items()} == {r[1]: D(r[3]) for r in end["records_charged"]}
    p = sim.p_values(t_obs, [t_obs, t_obs])
    assert p != "unevaluable" and (p.p_up, p.p_down) == (1, 1)  # ties count toward both tails


def test_rows_must_be_contiguous_from_t0() -> None:
    rows = _rows()
    with pytest.raises(ValueError, match="not contiguous"):
        _facts([rows[0], rows[2]])
    with pytest.raises(ValueError, match="not contiguous"):
        _facts(rows[1:])
    with pytest.raises(ValueError, match="10 wide|2 wide"):
        _facts(rows, k=10)


def _handmade(**over: Any) -> look.LookFacts:
    facts = look.LookFacts(t0=FRI, endpoint=TUE, k=3)
    facts.sessions = 3
    facts.shadow_sum, facts.shadow_count = D("0.03"), 3  # T_obs = 0.01
    facts.control_sum = [D("-0.03"), D("-0.06"), D("-0.09")]
    facts.control_count = [3, 3, 3]
    facts.shadow_path = [D(1), D("0.99"), D("1.05"), D("1.10")]
    facts.held_total = 6
    facts.invested = {1: (1, D("0.5")), 2: (2, D("0.5")), 3: (1, D("0.5")), 4: (3, D("0.5"))}
    facts.terminal = {1: D("0.6"), 2: D("0.55"), 3: D("0.45"), 4: D("0.4")}
    facts.spy_closes = [D(500), D(490), D(505)]
    for k, v in over.items():
        setattr(facts, k, v)
    return facts


def test_a_full_pass_needs_execution_and_the_stub_only_withholds_it() -> None:
    passing = look.evaluate(_handmade(), TERMS, h0=D("0.001"), h_end=D("0.001"), execution=EXEC_OK)
    assert (passing.verdict, passing.reasons) == ("pass", ())
    assert passing.detail["p_up"] == "1/4" and passing.detail["conditions"]["1_p_up"] is True
    unproven = look.evaluate(_handmade(), TERMS, h0=D("0.001"), h_end=D("0.001"), execution=EXEC_NONE)
    assert unproven.verdict == "shadow_pass_execution_unproven"
    assert unproven.detail["conditions"]["5_execution"] is False


def test_unevaluable_reasons_accumulate_and_unavailable_conditions_are_null() -> None:
    facts = _handmade(endpoint_forced=True, held_total=2, spy_closes=[None, D(490), D(505)])
    result = look.evaluate(facts, TERMS, h0=D("0.001"), h_end=D("0.001"), execution=EXEC_OK)
    assert result.verdict == "unevaluable"
    assert result.reasons == ("endpoint_forced", "occupancy_below_minimum", "spy_unavailable")
    assert result.detail["conditions"]["2_beats_spy"] is None and result.detail["conditions"]["3_drawdown"] is None
    empty = look.evaluate(
        _handmade(shadow_count=0, shadow_sum=D(0), invested={}, terminal={}),
        TERMS,
        h0=D(0),
        h_end=D(0),
        execution=EXEC_OK,
    )
    assert "t_obs_undefined" in empty.reasons and "lifecycles_below_minimum" in empty.reasons
    assert empty.detail["conditions"]["1_p_up"] is None and empty.detail["conditions"]["4_breadth"] is None
    assert empty.harm is False


def test_harm_is_judged_even_when_the_look_is_unevaluable() -> None:
    k = 79  # p_down = (1 + 0) / 80 ≤ 1/40 when the shadow is the worst book
    facts = _handmade(k=k, endpoint_forced=True)
    facts.shadow_sum, facts.shadow_count = D("-0.3"), 3
    facts.control_sum, facts.control_count = [D("0.03")] * k, [3] * k
    result = look.evaluate(facts, TERMS, h0=D(0), h_end=D(0), execution=EXEC_OK)
    assert result.verdict == "unevaluable" and result.harm is True
    assert result.detail["p_down"] == "1/80"


def test_condition_2_and_3_against_spy() -> None:
    # Shadow ends at +10% with a 1% drawdown; SPY path below. Losing to SPY flips condition 2 alone.
    beaten = look.evaluate(
        _handmade(spy_closes=[D(500), D(500), D(600)]), TERMS, h0=D(0), h_end=D(0), execution=EXEC_OK
    )
    assert beaten.detail["conditions"]["2_beats_spy"] is False and beaten.verdict == "not_passed"
    deep = _handmade(shadow_path=[D(1), D("0.9"), D("1.05"), D("1.10")])  # 10% vs floor 5% × 5/4
    result = look.evaluate(deep, TERMS, h0=D(0), h_end=D(0), execution=EXEC_OK)
    assert result.detail["conditions"]["3_drawdown"] is False


def test_spy_path_charges_entry_and_endpoint_and_marks_invalid_bars_at_the_last_close() -> None:
    path = look.spy_path([D(100), None, D(110)], h0=D("0.01"), h_end=D("0.02"))
    assert path is not None
    with localcontext(sim.CTX):
        assert path[0] == 1 and path[1] == D(100) / D(101) and path[2] == path[1]
        assert path[3] == D(110) * D("0.98") / D(101)
    assert look.spy_path([None, D(100)], h0=D(0), h_end=D(0)) is None
    assert look.max_drawdown([D(1), D("1.2"), D("0.9"), D("1.3")]) == D("0.25")
    assert look.max_drawdown([D(1), D("1.1")]) == 0


def test_breadth_is_the_median_over_names_of_summed_log_returns() -> None:
    inv = {1: (7, D(1)), 2: (7, D(1)), 3: (8, D(1))}
    # Name 7 sums ln(2) + ln(1/2) = 0, name 8 is ln(3/2): median of {0, 0.405…} = 0.2027…
    median = look.breadth_median(inv, {1: D(2), 2: D("0.5"), 3: D("1.5")})
    with localcontext(sim.CTX):
        expected = (D(2).ln() + D("0.5").ln() + D("1.5").ln()) / 2
    assert median is not None and median == expected
    loss = look.breadth_median({1: (7, D(1))}, {1: D("0.9")})
    assert loss is not None and loss < 0
    assert look.breadth_median({}, {}) is None
    with pytest.raises(ValueError, match="non-positive"):
        look.breadth_median({1: (7, D(1))}, {1: D(0)})


def test_executing_time_clips_to_the_window_and_counts_carry_in() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    start, end = t + timedelta(days=10), t + timedelta(days=100)
    events = [
        (t, "shadow_only"),
        (t + timedelta(days=5), "executing"),  # carry-in: counted from the window start
        (t + timedelta(days=20), "halted_loss"),
        (t + timedelta(days=30), "executing"),  # still executing at the end
    ]
    assert look.executing_time(events, start, end) == timedelta(days=10 + 70)
    s, e = look.window_bounds(FRI, TUE)
    assert s.astimezone(look._NEW_YORK).date() == FRI and (e - s) == timedelta(days=5)
    assert look.EXECUTION_MIN == timedelta(days=365.2425 / 2)


def test_wind_down_due_prefers_harm_and_waits_for_the_last_look() -> None:
    def stored(m: int, harm: bool) -> look.StoredLook:
        return look.StoredLook(m, MON, "not_passed", harm)

    assert look.wind_down_due({}) is None
    assert look.wind_down_due({12: stored(12, False)}) is None
    assert look.wind_down_due({12: stored(12, True)}) == "harm"
    assert look.wind_down_due({12: stored(12, False), 24: stored(24, False)}) == "completed_window"
    assert look.wind_down_due({12: stored(12, True), 24: stored(24, False)}) == "harm"


def test_the_activation_fence_reads_the_first_look_month() -> None:
    """``sql/448``'s activation fence hard-codes A_12; it must stay the first look."""
    from pathlib import Path

    sql = (Path(__file__).resolve().parents[1] / "sql" / "448_ranking_pot_looks.sql").read_text()
    assert f"interval '{LOOK_MONTHS[0]} months'" in sql
