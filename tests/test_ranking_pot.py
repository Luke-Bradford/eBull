"""#2842 slice 2 — ranking-pot pure core (spec §5.0, §5.1, §5.2, §6, §7.2). Pure, no DB."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from app.services import ranking_pot as rp
from app.services.market_calendar import us_market_status

LAST = date(2026, 9, 30)
AS_OF = datetime(2026, 9, 30, 23, 30, tzinfo=UTC)


def _sessions(count: int, last: date = LAST) -> tuple[date, ...]:
    out: list[date] = []
    d = last
    while len(out) < count:
        if us_market_status(d) != "closed":
            out.append(d)
        d -= timedelta(days=1)
    return tuple(reversed(out))


def _bars(closes: list[Decimal], last: date = LAST) -> tuple[tuple[date, ...], tuple[dict[str, Any], ...]]:
    dates = _sessions(len(closes), last)
    rows = tuple(
        {"open": c, "high": c * Decimal("1.01"), "low": c * Decimal("0.99"), "close": c, "volume": 1000} for c in closes
    )
    return dates, rows


def _flat_with_jump(jump: Decimal, bars: int = 60) -> list[Decimal]:
    """Flat at 100 then one close-to-close jump of ``jump`` on the last session (MAX = jump)."""
    closes = [Decimal(100)] * (bars - 1)
    return [*closes, Decimal(100) * (1 + jump)]


def _facts(iid: int, **over: Any) -> rp.NameFacts:
    dates, rows = _bars(over.pop("closes", _flat_with_jump(Decimal("0.01"))))
    base: dict[str, Any] = {
        "instrument_id": iid,
        "symbol": f"S{iid}",
        "is_tradable": True,
        "asset_class": "us_equity",
        "total_score": Decimal("0.5"),
        "completeness_tier": "full",
        "filings_status": "analysable",
        "market_cap_usd": Decimal(10**11),
        "bid": Decimal("100.95"),
        "ask": Decimal("101.05"),
        "quoted_at": AS_OF - timedelta(hours=1),
        "bar_dates": dates,
        "bar_rows": rows,
    }
    base.update(over)
    return rp.NameFacts(**base)


NYSE_CAPS: list[Decimal | None] = [Decimal(10**8) * k for k in range(1, rp.MIN_NYSE_CAPS + 1)]


# ---------------------------------------------------------------------------
# helpers: percentiles, sessions, MAX, ATR
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("values", "pct", "expected"),
    [
        ([1, 2, 3, 4, 5], 20, 1),
        ([1, 2, 3, 4, 5], 21, 2),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 90, 9),
        ([7], 90, 7),
        ([], 90, None),
    ],
)
def test_nearest_rank(values: list[int], pct: int, expected: int | None) -> None:
    assert rp.nearest_rank([Fraction(v) for v in values], pct) == (None if expected is None else Fraction(expected))


def test_max_window_skips_holidays() -> None:
    window = rp.max_window_sessions(date(2026, 9, 8))  # Labor Day 2026-09-07 is closed
    assert len(window) == rp.MAX_SESSIONS and window[-1] == date(2026, 9, 8)
    assert date(2026, 9, 7) not in window and date(2026, 9, 4) in window


def test_max_daily_return_exact_and_window_bound() -> None:
    sessions = rp.max_window_sessions(LAST)
    assert rp.max_daily_return(_facts(1, closes=_flat_with_jump(Decimal("0.25"))), sessions) == Fraction(1, 4)
    # A jump 22 closes back is outside the 21 returns.
    closes = [Decimal(100)] * 38 + [Decimal(200)] + [Decimal(200)] * 21
    assert rp.max_daily_return(_facts(1, closes=closes), sessions) == 0


def test_max_daily_return_refuses_gap_stale_or_nonpositive() -> None:
    sessions = rp.max_window_sessions(LAST)
    stale = _facts(1)
    stale_dates, stale_rows = _bars(_flat_with_jump(Decimal("0.01")), last=date(2026, 9, 29))
    assert (
        rp.max_daily_return(
            rp.NameFacts(**{**stale.__dict__, "bar_dates": stale_dates, "bar_rows": stale_rows}), sessions
        )
        is None
    )
    gap = _facts(1)
    dates = list(gap.bar_dates)
    del dates[-5]
    holed = rp.NameFacts(**{**gap.__dict__, "bar_dates": tuple(dates), "bar_rows": gap.bar_rows[1:]})
    assert rp.max_daily_return(holed, sessions) is None
    zero = _facts(1, closes=[Decimal(100)] * 59 + [Decimal(0)])
    assert rp.max_daily_return(zero, sessions) is None


def test_atr14_needs_last_session_and_warmup() -> None:
    assert rp.atr14(_facts(1), last_session=LAST) is not None
    assert rp.atr14(_facts(1), last_session=date(2026, 10, 1)) is None  # stale last bar
    short = _facts(1, closes=[Decimal(100)] * 59)  # below v1's 60-bar floor
    assert rp.atr14(short, last_session=LAST) is None


# ---------------------------------------------------------------------------
# §5.0 universes
# ---------------------------------------------------------------------------
def _population(extra: list[rp.NameFacts]) -> list[rp.NameFacts]:
    """Filler names with MAX 0.01 so the 200-value MAX floor is met; ids from 10_000."""
    return extra + [_facts(10_000 + k) for k in range(rp.MIN_VALID_MAX)]


def test_universe_refusals() -> None:
    assert rp.build_universes(_population([]), nyse_caps=NYSE_CAPS[:-1], as_of=AS_OF, last_session=LAST) == (
        "breakpoint_unavailable"
    )
    few = [_facts(10_000 + k) for k in range(rp.MIN_VALID_MAX - 1)]
    assert rp.build_universes(few, nyse_caps=NYSE_CAPS, as_of=AS_OF, last_session=LAST) == "max_cut_unavailable"
    # A NULL cap (overlay failure) is excluded from the breakpoint population, so it refuses again.
    assert rp.build_universes(_population([]), nyse_caps=[*NYSE_CAPS[:-1], None], as_of=AS_OF, last_session=LAST) == (
        "breakpoint_unavailable"
    )


def test_breakpoint_is_nearest_rank_20th_and_inclusive() -> None:
    u = rp.build_universes(
        _population(
            [
                _facts(1, market_cap_usd=Decimal(10**8) * 100),  # exactly the breakpoint → in R
                _facts(2, market_cap_usd=Decimal(10**8) * 100 - 1),
            ]
        ),
        nyse_caps=NYSE_CAPS,
        as_of=AS_OF,
        last_session=LAST,
    )
    assert isinstance(u, rp.Universes)
    assert u.breakpoint == Decimal(10**8) * 100
    assert 1 in u.r_ids and u.hold_failure[2] == "cap_below_breakpoint"


def test_hold_rules_first_failure_in_frozen_order() -> None:
    u = rp.build_universes(
        _population(
            [
                _facts(1, is_tradable=False, total_score=None),
                _facts(2, total_score=Decimal(0), completeness_tier="insufficient_data"),
                _facts(3, completeness_tier=None),
                _facts(4, filings_status="partial", market_cap_usd=None),
                _facts(5, market_cap_usd=None),
                _facts(6, asset_class="crypto"),
                _facts(7, market_cap_usd=Decimal("NaN")),
            ]
        ),
        nyse_caps=NYSE_CAPS,
        as_of=AS_OF,
        last_session=LAST,
    )
    assert isinstance(u, rp.Universes)
    assert {i: u.hold_failure[i] for i in range(1, 8)} == {
        1: "not_tradable",
        2: "score_not_positive",
        3: "completeness_insufficient",
        4: "filings_not_analysable",
        5: "cap_unavailable",
        6: "not_us_equity",
        7: "cap_unavailable",
    }


def test_max_cut_ties_at_and_above() -> None:
    # 200 fillers at MAX 0.01 plus 30 names at 0.05: 230 values, nearest-rank 90th = the 207th = 0.05.
    # Names AT the cut pass; one name above it fails.
    hot = [_facts(100 + k, closes=_flat_with_jump(Decimal("0.05"))) for k in range(30)]
    above = _facts(200, closes=_flat_with_jump(Decimal("0.06")))
    u = rp.build_universes(_population([*hot, above]), nyse_caps=NYSE_CAPS, as_of=AS_OF, last_session=LAST)
    assert isinstance(u, rp.Universes)
    assert u.max_cut == Fraction(5, 100)
    assert all(100 + k in u.f_ids for k in range(30))
    assert u.entry_failure[200] == "max_above_cut"
    assert 200 in u.r_ids  # MAX never removes a name from R


def test_max_cut_population_is_quote_eligible_only() -> None:
    # 30 high-MAX names with stale quotes do not move the cut, and fail on the quote first.
    stale = [
        _facts(100 + k, closes=_flat_with_jump(Decimal("0.5")), quoted_at=AS_OF - timedelta(days=2)) for k in range(30)
    ]
    u = rp.build_universes(_population(stale), nyse_caps=NYSE_CAPS, as_of=AS_OF, last_session=LAST)
    assert isinstance(u, rp.Universes)
    assert u.max_cut == Fraction(1, 100) and u.max_population == rp.MIN_VALID_MAX
    assert u.entry_failure[100] == "quote_ineligible"


def test_entry_rules() -> None:
    wide_atr = [Decimal(100), Decimal(160)] * 30  # huge true ranges: 3 × ATR14 ≥ ask
    u = rp.build_universes(
        _population(
            [
                _facts(1, closes=[Decimal(100)] * 40),  # 40 bars: MAX valid, too few bars for ATR
                _facts(2, closes=wide_atr, bid=Decimal("159.9"), ask=Decimal("160")),
            ]
        ),
        nyse_caps=NYSE_CAPS,
        as_of=AS_OF,
        last_session=LAST,
    )
    assert isinstance(u, rp.Universes)
    assert u.entry_failure[1] == "bars_incomplete"
    # Its MAX (0.6) is above the cut too, and MAX is evaluated first.
    assert u.entry_failure[2] == "max_above_cut"


# ---------------------------------------------------------------------------
# Orders and ranks
# ---------------------------------------------------------------------------
def test_rank_order_ties_by_recipient_id_and_missing_last() -> None:
    scores = {5: Decimal("0.7"), 3: Decimal("0.7"), 9: Decimal("0.9"), 4: None, 1: Decimal(0)}
    assert rp.rank_order([5, 3, 9, 4, 1], scores.get) == (9, 3, 5, 1, 4)


def _universes(r: dict[int, Decimal], f: set[int], hold_failure: dict[int, rp.HoldRule] | None = None) -> rp.Universes:
    entry_failure: dict[int, rp.EntryRule] = {i: "max_above_cut" for i in r if i not in f}
    return rp.Universes(
        breakpoint=Decimal(1),
        max_cut=Fraction(1, 10),
        r_ids=frozenset(r),
        f_ids=frozenset(f),
        hold_failure=hold_failure or {},
        entry_failure=entry_failure,
        own_score=r,
        max_return={},
        atr={},
        max_population=0,
        nyse_cap_population=0,
    )


def test_control_order_uses_donor_scores() -> None:
    u = _universes({1: Decimal("0.9"), 2: Decimal("0.5"), 3: Decimal("0.1")}, {1, 2, 3})
    donor_of = {1: 3, 2: 1, 3: 4}  # 4 is an S₀ name outside R with no score this run
    run_scores: dict[int, Decimal | None] = {1: Decimal("0.9"), 2: Decimal("0.5"), 3: Decimal("0.1"), 4: None}
    assert rp.real_order(u) == (1, 2, 3)
    assert rp.control_order(u, donor_of, run_scores) == (2, 1, 3)


def test_ranks_reject_non_permutation() -> None:
    u = _universes({1: Decimal("0.9"), 2: Decimal("0.5")}, {1, 2})
    with pytest.raises(ValueError):
        rp.ranks(u, [1])


# ---------------------------------------------------------------------------
# §5.1 / §5.2 decisions (n = 2 → hold band 4)
# ---------------------------------------------------------------------------
def _scores(ids: list[int]) -> dict[int, Decimal]:
    return {iid: Decimal(1) - Decimal(k) / 100 for k, iid in enumerate(ids)}


def _row(d: rp.BookDecision, iid: int) -> tuple[str, str | None]:
    (row,) = [r for r in d.rows if r.instrument_id == iid]
    return row.action, row.reason


def test_frank_n_and_rrank_2n_boundaries() -> None:
    # R order 1..6; F = {4, 5, 6}: F-ranks 4→1, 5→2, 6→3; R-ranks 4, 5, 6.
    u = _universes(_scores([1, 2, 3, 4, 5, 6]), {4, 5, 6})
    d = rp.decide(u, rp.real_order(u), [], recently_exited=frozenset(), entries_allowed=True, n=2)
    assert d.entries == (4,)  # F-rank 1, R-rank 4 = 2N → enters
    assert _row(d, 1) == ("not_selected", "infeasible:max_above_cut")
    assert [r.instrument_id for r in d.rows] == [4, 1, 2, 3]  # 5 (R-rank 5 = 2N+1) gets no row
    assert d.slots_unfilled == 1


def test_frank_above_n_is_out_of_band_not_band_full() -> None:
    u = _universes(_scores([1, 2, 3, 4]), {1, 2, 3})
    d = rp.decide(u, rp.real_order(u), [], recently_exited=frozenset(), entries_allowed=True, n=2)
    assert d.entries == (1, 2)
    assert _row(d, 3) == ("not_selected", "frank_out_of_band")  # F-rank 3 = N+1, slots now full anyway
    u2 = _universes(_scores([1, 2, 3, 4]), {1, 2, 3})
    held = [rp.Holding(1, "open"), rp.Holding(2, "open")]
    d2 = rp.decide(u2, rp.real_order(u2), held, recently_exited=frozenset(), entries_allowed=True, n=2)
    assert d2.entries == () and _row(d2, 3) == ("not_selected", "frank_out_of_band")


def test_band_full() -> None:
    u = _universes(_scores([1, 2, 3, 4]), {1, 2, 3, 4})
    held = [rp.Holding(4, "open")]  # R-rank 4 ≤ 2N: holds and takes a slot
    d = rp.decide(u, rp.real_order(u), held, recently_exited=frozenset(), entries_allowed=True, n=2)
    assert d.entries == (1,)
    assert _row(d, 2) == ("not_selected", "band_full")
    assert _row(d, 4) == ("hold", None)


def test_held_name_failing_only_an_entry_rule_holds() -> None:
    u = _universes(_scores([1, 2, 3]), {1, 2})  # 3 is in R, not in F
    d = rp.decide(u, rp.real_order(u), [rp.Holding(3, "open")], recently_exited=frozenset(), entries_allowed=True, n=2)
    assert _row(d, 3) == ("hold", None)
    assert d.entries == (1,)


def test_held_name_leaving_r_exits_with_its_rule() -> None:
    u = _universes(_scores([1, 2]), {1, 2}, hold_failure={7: "cap_below_breakpoint"})
    d = rp.decide(u, rp.real_order(u), [rp.Holding(7, "open")], recently_exited=frozenset(), entries_allowed=True, n=2)
    assert _row(d, 7) == ("exit", "ineligible:cap_below_breakpoint")
    assert d.exits == (7,) and d.entries == (1, 2)  # the exit frees its slot this rebalance


def test_rerank_out_of_band_exit_includes_pending_entry() -> None:
    u = _universes(_scores([1, 2, 3, 4, 5]), {1, 2, 3, 4, 5})
    held = [rp.Holding(5, "entry_pending")]
    d = rp.decide(u, rp.real_order(u), held, recently_exited=frozenset(), entries_allowed=True, n=2)
    assert _row(d, 5) == ("exit", "rerank_out_of_band")


def test_exit_pending_back_in_band_keeps_its_stamp_and_slot() -> None:
    u = _universes(_scores([1, 2, 3]), {1, 2, 3})
    held = [rp.Holding(1, "open", exit_stamped=True)]  # rank 1, but stamped at an earlier rebalance
    d = rp.decide(u, rp.real_order(u), held, recently_exited=frozenset(), entries_allowed=True, n=2)
    assert _row(d, 1) == ("exit_pending", None)
    assert d.exits == ()
    assert d.entries == (2,) and d.occupied == 1  # r3-99: the stamped slot is not reused before it closes


def test_no_reentry_after_an_exit_since_the_last_rebalance() -> None:
    u = _universes(_scores([1, 2, 3]), {1, 2, 3})
    d = rp.decide(u, rp.real_order(u), [], recently_exited=frozenset({1}), entries_allowed=True, n=2)
    assert _row(d, 1) == ("not_selected", "recently_exited")
    # F-ranks are shared by every book: the exited name keeps F-rank 1, so name 3 (F-rank 3 = N+1) stays out.
    assert d.entries == (2,)
    assert _row(d, 3) == ("not_selected", "frank_out_of_band")


def test_infeasible_outranks_recently_exited() -> None:
    u = _universes(_scores([1, 2]), {2})
    d = rp.decide(u, rp.real_order(u), [], recently_exited=frozenset({1}), entries_allowed=True, n=2)
    assert _row(d, 1) == ("not_selected", "infeasible:max_above_cut")


def test_entries_halted_still_exits() -> None:
    u = _universes(_scores([1, 2, 3, 4, 5]), {1, 2, 3, 4, 5})
    held = [rp.Holding(5, "open")]
    d = rp.decide(u, rp.real_order(u), held, recently_exited=frozenset(), entries_allowed=False, n=2)
    assert d.entries == () and d.exits == (5,)
    assert _row(d, 1) == ("not_selected", "entries_halted")
    assert d.slots_unfilled == 2


def test_empty_r_exits_everything() -> None:
    u = _universes({}, set(), hold_failure={1: "filings_not_analysable", 2: "not_tradable"})
    held = [rp.Holding(1, "open"), rp.Holding(2, "open", exit_stamped=True)]
    d = rp.decide(u, (), held, recently_exited=frozenset(), entries_allowed=True, n=2)
    assert [(r.instrument_id, r.action, r.reason) for r in d.rows] == [
        (1, "exit", "ineligible:filings_not_analysable"),
        (2, "exit_pending", None),
    ]
    assert d.entries == () and d.slots_unfilled == 1


def test_holdings_validation() -> None:
    u = _universes(_scores([1]), {1})
    with pytest.raises(ValueError, match="twice"):
        rp.decide(
            u, (1,), [rp.Holding(1, "open"), rp.Holding(1, "open")], recently_exited=frozenset(), entries_allowed=True
        )
    with pytest.raises(ValueError, match="outside"):
        rp.decide(u, (1,), [rp.Holding(99, "open")], recently_exited=frozenset(), entries_allowed=True)


def test_one_row_per_name_in_precedence_order() -> None:
    u = _universes(_scores([1, 2, 3, 4, 5, 6]), {1, 2, 3, 4, 5, 6}, hold_failure={9: "not_tradable"})
    held = [rp.Holding(9, "open"), rp.Holding(3, "open"), rp.Holding(2, "open", exit_stamped=True)]
    d = rp.decide(u, rp.real_order(u), held, recently_exited=frozenset(), entries_allowed=True, n=2)
    assert [r.action for r in d.rows] == ["exit", "exit_pending", "hold", "not_selected", "not_selected"]
    assert len({r.instrument_id for r in d.rows}) == len(d.rows)


def test_wind_down() -> None:
    rows = rp.wind_down([rp.Holding(2, "open"), rp.Holding(1, "entry_pending", exit_stamped=True)])
    assert [(r.instrument_id, r.action, r.reason) for r in rows] == [
        (2, "exit", "wind_down"),
        (1, "exit_pending", None),
    ]


# ---------------------------------------------------------------------------
# §7.2 levels and asserts
# ---------------------------------------------------------------------------
def test_planned_levels_exact_policy() -> None:
    levels = rp.planned_levels(Decimal("100"), Fraction(2))
    assert isinstance(levels, rp.Levels)
    assert levels == rp.Levels(Fraction(100), Fraction(94), Fraction(112))
    assert rp.stop_loss_pct(levels.basis, levels.stop_loss) == 6
    assert rp.planned_levels(Decimal("5"), Fraction(2)) == "protective_levels_invalid"  # SL ≤ 0
    assert rp.planned_levels(Decimal("0"), Fraction(2)) == "protective_levels_invalid"


@pytest.mark.parametrize(
    ("price", "sl", "tp", "atr", "ok"),
    [
        (100, 94, 112, 2, True),
        (100, 98, 103, 2, True),  # 1 ATR, exactly 1.5R
        (100, 92, 112, 2, True),  # 4 ATR
        (100, 91.5, 120, 2, False),  # > 4 ATR
        (100, 98.5, 110, 2, False),  # < 1 ATR
        (100, 94, 108.9, 2, False),  # TP < 1.5R
        (100, 100, 112, 2, False),
        (100, 94, 100, 2, False),
    ],
)
def test_validate_levels(price: float, sl: float, tp: float, atr: float, ok: bool) -> None:
    f = Fraction
    assert rp.validate_levels(f(str(price)), f(str(sl)), f(str(tp)), f(str(atr))) is ok


def test_half_spread_and_basis() -> None:
    assert rp.half_spread(Decimal("99"), Decimal("101")) == Fraction(1, 100)
    assert rp.half_spread(Decimal("101"), Decimal("99")) is None
    assert rp.half_spread(None, Decimal("1")) is None
    assert rp.basis_unchanged(Decimal("10.50"), Decimal("10.5"))
    assert not rp.basis_unchanged(Decimal("10.5"), Decimal("21"))
    assert not rp.basis_unchanged(Decimal("10.5"), None)
    assert not rp.basis_unchanged(Decimal("NaN"), Decimal("10.5"))


# ---------------------------------------------------------------------------
# §6 ticket
# ---------------------------------------------------------------------------
def _score(**over: Any) -> rp.ScoreBreakdown:
    base: dict[str, Any] = {
        "model_version": "v1.5-balanced",
        "total_score": Decimal("0.6123"),
        "raw_total": Decimal("0.6623"),
        "families": {"quality": Decimal("0.7"), "value": None},
        "penalties_json": [
            {"name": "stale_thesis", "deduction": 0.15, "reason": "r", "kind": "penalty"},
            {"name": "calmar", "addition": 0.1, "reason": "r", "kind": "reward"},
        ],
    }
    base.update(over)
    return rp.ScoreBreakdown(**base)


def test_reconciles() -> None:
    assert rp.reconciles(_score())
    assert not rp.reconciles(_score(total_score=Decimal("0.6200")))
    assert rp.reconciles(_score(raw_total=Decimal("0.05"), total_score=Decimal("0")))  # clipped at 0
    assert not rp.reconciles(_score(raw_total=None))
    assert not rp.reconciles(_score(raw_total=Decimal("NaN")))
    assert not rp.reconciles(_score(penalties_json=[{"name": "x", "deduction": float("nan"), "kind": "penalty"}]))
    # A writer-vocabulary breach is malformed data and raises, whatever the other fields hold.
    with pytest.raises(ValueError):
        rp.reconciles(_score(penalties_json=[{"name": "x", "kind": "bonus"}]))
    with pytest.raises(ValueError):
        rp.reconciles(_score(raw_total=None, penalties_json=[{"name": "x", "kind": "bonus"}]))
    with pytest.raises(KeyError):
        rp.reconciles(_score(penalties_json=[{"name": "x", "kind": "penalty"}]))


def test_entry_ticket_separates_recipient_and_donor() -> None:
    row = rp.DecisionRow(7, "enter", None, 3, 1)
    levels = rp.planned_levels(Decimal("100"), Fraction(2))
    assert isinstance(levels, rp.Levels)
    ticket = rp.entry_ticket(
        book="control:17",
        declaration_id=1,
        instrument_id=7,
        symbol="S7",
        row=row,
        own_score=_score(),
        family_weights={"quality": 0.25},
        order_donor_id=11,
        order_score=Decimal("0.91"),
        thesis=rp.ThesisRef(5, 40, "m", "p1"),
        levels=levels,
        atr=Fraction(2),
        expected_half_spread=Fraction(1, 1000),
    )
    assert ticket["rule_id"] == rp.ENTRY_RULE_ID and ticket["rationale_class"] == "signal"
    assert ticket["order"] == {"donor_instrument_id": 11, "score": Decimal("0.91")}
    assert ticket["score"]["total_score"] == Decimal("0.6123") and ticket["score"]["reconciles"]
    assert ticket["planned_levels"] == {"basis": "100", "stop_loss": "94", "take_profit": "112", "atr14": "2"}
    with pytest.raises(ValueError):
        rp.entry_ticket(**{**_ticket_kwargs(), "row": rp.DecisionRow(7, "hold", None, 3, 1)})


def _ticket_kwargs() -> dict[str, Any]:
    levels = rp.planned_levels(Decimal("100"), Fraction(2))
    assert isinstance(levels, rp.Levels)
    return {
        "book": "shadow",
        "declaration_id": 1,
        "instrument_id": 7,
        "symbol": "S7",
        "row": rp.DecisionRow(7, "enter", None, 3, 1),
        "own_score": _score(),
        "family_weights": None,
        "order_donor_id": 7,
        "order_score": Decimal("0.6123"),
        "thesis": None,
        "levels": levels,
        "atr": Fraction(2),
        "expected_half_spread": Fraction(1, 1000),
    }
