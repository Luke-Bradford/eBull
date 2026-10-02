"""#2842 slice 6c-i — the §9.4 readout's pure parts (spec §9.4, "The readout" paragraph)."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services import ranking_pot_exposure as ex
from app.services import ranking_pot_readout as ro
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_step as st
from tests.test_ranking_pot_step import FLAT, FRI, MON, TABLE, THU, TUE, _bars, _book, _rebalance

D = Decimal
WED = date(2026, 10, 7)
SPY = sim.Bar(D(500), D(501), D(499), D(500))
UP = sim.Bar(D(100), D(104), D(100), D(104))
GAP = sim.Bar(D(96), D(97), D(95), D(96))  # below a 100 − 3 × ATR 1 stop
R = {1: "0.9", 2: "0.8", 3: "0.7"}
#: Control 1 keeps the real order; control 2 reverses it, so it enters 3 and 2.
DONORS: list[dict[int, int] | None] = [None, {1: 1, 2: 2, 3: 3}, {1: 3, 2: 2, 3: 1}]
AS_OF = datetime(2026, 10, 1, 21, 0, tzinfo=UTC)


def _regime(label: ro.Regime) -> ro.RegimeLabel:
    return ro.RegimeLabel(label, THU, date(2025, 10, 1), D(500), D(450))


def _info(attempt: int, target: date, last: date, label: ro.Regime, theses: dict[int, Any] | None = None):
    return ro.RebalanceInfo(attempt, target, last, AS_OF, len(R), theses or {}, _regime(label), frozenset(R))


def _stream() -> tuple[list[ro.ReadoutRow], dict[int, ro.RebalanceInfo]]:
    """FRI = T₀ (attempt 7, regime up): the shadow enters 1 and 2. MON: 1 gaps through its stop. TUE (attempt 8,
    regime down): the shadow holds 2. WED = E. Real step outputs, three books (K = 2) and the no-SL/TP variant, which
    rides name 1's gap and still holds both at E."""
    rebalances = {7: _rebalance(FRI, THU, R, set(R)), 8: replace(_rebalance(TUE, MON, R, set(R)), attempt_id=8)}
    thesis = rb.ThesisUsed(41, AS_OF - timedelta(days=3), "claude-x", "v5")
    infos = {7: _info(7, FRI, THU, "up", {2: thesis}), 8: _info(8, TUE, MON, "down")}
    books = [_book() for _ in [*DONORS, None]]
    variant_at = len(DONORS)
    closes = {i: {d: D(100) for d in (THU, FRI, MON, TUE)} for i in R}
    closes[1][MON] = GAP.close  # the variant still holds name 1 at TUE: its split check reads MON's stored close
    rows = []
    sessions: list[tuple[date, dict[int, sim.Bar | None], int | None]] = [
        (FRI, {1: FLAT, 2: FLAT, 3: FLAT}, 7),
        (MON, {1: GAP, 2: FLAT, 3: FLAT}, None),
        (TUE, {1: FLAT, 2: FLAT, 3: FLAT}, 8),
        (WED, {1: UP, 2: UP, 3: UP}, None),
    ]
    for session, bars, applied in sessions:
        cols = st.ControlColumns()
        shadow: dict[str, Any] = {}
        variant: dict[str, Any] = {}
        reb = None if applied is None else rebalances[applied]
        for b, donor in enumerate([*DONORS, None]):
            book, result, decision = st.advance(
                books[b],
                session=session,
                bars=_bars(session, bars, closes),
                rebalance=reb,
                donor_of=donor,
                wind=False,
                used=set(),
                protective=b != variant_at,
            )
            books[b] = book
            if b == 0:
                shadow = st.shadow_doc(result, decision, session, TABLE)
            elif b == variant_at:
                variant = st.shadow_doc(result, decision, session, TABLE)
            else:
                cols.add(result, session, TABLE)
                if decision is not None and donor is not None and reb is not None:
                    cols.add_decision(decision, reb.missing_donors(donor))
        table = None if applied is None else ex.encode_table(TABLE)
        rows.append(ro.ReadoutRow(session, False, applied, False, shadow, cols.doc(), SPY, variant, table))
    return rows, infos


def _facts(rows: list[ro.ReadoutRow], infos: dict[int, ro.RebalanceInfo], end: date = WED) -> ro.ReadoutFacts:
    acc = ro.ReadoutFacts(t0=FRI, endpoint=end, n=2, k=2, rebalances=infos)
    for row in rows:
        acc.add(row)
    return acc


H = D("0.0002")


def test_the_readout_over_real_step_outputs() -> None:
    rows, infos = _stream()
    acc = _facts(rows, infos)
    out = acc.finish(h0=H, h_end=H)

    # Shadow: lifecycle 1 (name 1) stopped Monday, lifecycle 2 (name 2) open at E. Control 2 held 3 and 2.
    lc = out["lifecycles"]
    assert lc["count"] == 2 and lc["profit_factor"]["losses"] == 1 and lc["profit_factor"]["wins"] == 1
    assert lc["best_1pct"]["selected"] == 1 and lc["skew"] is None
    assert out["exits"]["shadow"]["by_reason"] == {"stop_loss_gap": 1}
    assert out["exits"]["flag"] is False and out["exits"]["controls"]["missing_bars"] == {"median": "0", "max": 0}

    # Turnover: T₀ buys both slots from NAV 1.0; TUE refills the stopped slot (shadow re-enters nothing: name 1
    # exited since the last rebalance, name 3 is F-rank 3 > N).
    per = out["turnover_occupancy"]["per_rebalance"]
    assert [p["attempt_id"] for p in per] == [7, 8]
    assert D(per[0]["shadow"]["turnover"]) == 1 and D(per[1]["shadow"]["turnover"]) == 0
    assert per[0]["shadow"]["entered_per_n"] == "1" and per[1]["shadow"]["decision_slots_unfilled"] == 1
    assert per[0]["missing_donor_share"] == {"r_size": 3, "median": "0", "max": "0"}

    # Occupancy: the first interval (FRI, MON) holds 2 then 1 → 3/4; the second (TUE, WED) holds 1, 1 → 1/2.
    ivs = out["turnover_occupancy"]["intervals"]
    assert [(i["attempt_id"], i["sessions"], D(i["shadow_occupancy"])) for i in ivs] == [
        (7, 2, D("0.75")),
        (8, 2, D("0.5")),
    ]
    assert D(out["turnover_occupancy"]["window"]["shadow_occupancy"]) == D("0.625")

    # The labels' products recover the whole-window path ratios (condition 2's operands).
    cohorts = out["regime_cohorts"]
    assert set(cohorts) == {"up", "down"}
    path = acc.facts.shadow_path
    shadow = (1 + D(cohorts["up"]["shadow_return"])) * (1 + D(cohorts["down"]["shadow_return"]))
    assert abs(shadow - path[-1] / path[0]) < D("1e-25")
    assert cohorts["up"]["lifecycles"]["count"] == 2 and cohorts["down"]["lifecycles"]["count"] == 0
    spy = (1 + D(cohorts["up"]["spy_return"])) * (1 + D(cohorts["down"]["spy_return"]))
    assert abs(spy - (1 - H) / (1 + H)) < D("1e-25")  # flat SPY: only the two half-spreads

    # Thesis provenance: name 2 consumed thesis 41, three days old at the snapshot.
    prov = out["thesis_provenance"]
    assert prov["no_thesis"] == 1 and prov["counts"] == [{"model": "claude-x", "prompt_version": "v5", "count": 1}]
    assert D(prov["age_days_median"]) == 3
    assert out["spy_total_return"] is None and out["spy_total_return_reason"] == "no_ex_dated_distribution_source"

    # The no-SL/TP variant rode name 1's gap (96) back to 104: no exit, both lifecycles open and winning at E.
    var = out["variant"]
    assert var["exits_by_reason"] == {"shadow": {"stop_loss_gap": 1}, "variant": {}}
    assert var["lifecycles"]["count"] == 2 and var["lifecycles"]["profit_factor"]["wins"] == 2
    assert D(var["occupancy"]["variant"]) == 1 and D(var["occupancy"]["shadow"]) == D("0.625")
    assert var["entry_refusals"] == {"shadow": 0, "variant": 0}
    ret = var["nav_return"]
    assert D(ret["shadow_minus_variant"]) < 0
    assert abs(D(ret["shadow_minus_variant"]) - (D(ret["shadow"]) - D(ret["variant"]))) < D("1e-25")
    assert abs(D(ret["shadow"]) - (path[-1] - 1)) < D("1e-25")
    # Both books bought both names at FRI's close, so up to MON the paths agree; the variant's drawdown is MON's mark.
    assert D(var["max_drawdown"]["variant"]) > 0 and var["t"]["variant"] is not None

    # §9.4 exposures. The shadow held name 1 only on FRI (stopped MON) and name 2 throughout: its beta is name 1's
    # alone (name 2 has none), covering only FRI's share of the capital-time.
    exp = out["exposures"]
    sh = exp["shadow"]
    assert D(sh["beta"]["value"]) == D("1.5") and 0 < D(sh["beta"]["coverage"]) < 1
    assert set(sh["sector"]) == {"XLK", "XLF"} and D(sh["atr_pct"]["coverage"]) == 1
    # R_t = {1, 2, 3} at equal weight; two applied tables each count name 2's missing beta.
    r_t = exp["r_t"]
    assert abs(D(r_t["beta"]["value"]) - 1) < D("1e-25") and abs(D(r_t["ln_cap"]["value"]) - 21) < D("1e-25")
    assert abs(D(r_t["sector"]["none"]) - D(1) / 3) < D("1e-25")
    assert exp["beta_null_reasons"] == {"too_few_pairs": 2}
    controls = exp["controls_median"]
    assert controls["controls_with_capital"] == 2 and controls["beta"]["null_controls"] == 0
    # Yield gap: only name 1 is covered (3%), so the shadow's yield is 3%; R_t's is 3% too (coverage 1/3).
    assert abs(D(sh["div_yield"]["value"]) - D("0.03")) < D("1e-25")
    assert abs(D(r_t["div_yield"]["value"]) - D("0.03")) < D("1e-25")
    assert abs(D(exp["yield_gap"]["shadow_minus_r_t"])) < D("1e-25")
    gap = exp["yield_gap"]["shadow_minus_controls_median"]
    assert gap is not None and abs(D(gap)) < D("1e-25")  # the controls hold name 1 (covered) too


def test_an_interim_endpoint_and_the_invariants() -> None:
    rows, infos = _stream()
    interim = _facts(rows[:2], infos, end=MON).finish(h0=H, h_end=H)
    assert interim["endpoint"] == MON.isoformat() and len(interim["turnover_occupancy"]["intervals"]) == 1
    assert set(interim["regime_cohorts"]) == {"up", "down"}
    assert interim["regime_cohorts"]["down"]["intervals"] == 0
    assert interim["regime_cohorts"]["down"]["shadow_return"] is None  # an empty product is null, never 1

    with pytest.raises(ValueError, match="not at the endpoint"):
        _facts(rows[:3], infos).finish(h0=H, h_end=H)
    with pytest.raises(ValueError, match="not a decided snapshot here"):
        _facts(rows, {7: infos[7], 8: replace(infos[8], target_session=WED)})
    with pytest.raises(ValueError, match="before the first applied rebalance"):
        ro.ReadoutFacts(t0=FRI, endpoint=WED, n=2, k=2, rebalances=infos).add(
            replace(rows[0], applied_attempt_id=None, characteristics=None)
        )
    with pytest.raises(ValueError, match="exactly at an applied session"):
        _facts([rows[0], replace(rows[1], characteristics=rows[0].characteristics)], infos)
    narrow = dict(rows[1].controls) | {"bought": ["0"]}
    with pytest.raises(ValueError, match="not 2 wide"):
        _facts([rows[0], replace(rows[1], controls=narrow)], infos)
    # A thesis created after the snapshot has no valid age.
    late = rb.ThesisUsed(41, AS_OF + timedelta(seconds=1), "m", "v")
    with pytest.raises(ValueError, match="no valid age"):
        _facts(rows, {7: replace(infos[7], theses={2: late}), 8: infos[8]}).finish(h0=H, h_end=H)


def test_statistics() -> None:
    assert ro.median([]) is None and ro.median([D(3), D(1)]) == 2 and ro.median([D(5), D(1), D(2)]) == 2
    assert ro.skew([D(1), D(1)]) is None and ro.skew([D(2), D(2), D(2)]) is None
    # g₁ of (0, 0, 3): mean 1, m₂ = 2, m₃ = 2 → 2 / 2^{3/2} = 1/√2.
    assert abs(ro.skew([D(0), D(0), D(3)]) - 1 / D(2).sqrt()) < D("1e-25")  # type: ignore[operator]
    assert ro.profit_factor([D("0.2"), D("-0.1"), D(0)]) == {"value": "2", "wins": 1, "losses": 1, "flat": 1}
    assert ro.profit_factor([D("0.2")])["value"] is None

    def lc(n: int, r: str, iid: int = 1) -> ro.Lifecycle:
        return ro.Lifecycle(n, iid, FRI, D(1), 1 + D(r))

    # 101 lifecycles → the best ⌈1.01⌉ = 2; ties at the top resolve to the lower lifecycle numbers.
    lcs = [lc(i, "0.01") for i in range(1, 100)] + [lc(100, "0.5"), lc(101, "0.5")]
    best = ro.lifecycle_distribution(lcs)["best_1pct"]
    assert best["selected"] == 2 and D(best["mean"]) == D("0.5") and D(best["rest_mean"]) == D("0.01")
    assert ro.lifecycle_distribution([lc(1, "-0.1")])["best_1pct"]["pnl_share"] is None  # total P&L ≤ 0
    assert ro.lifecycle_distribution([])["best_1pct"] is None


def test_regime_follows_the_2901_rule() -> None:
    sun = date(2026, 10, 4)
    closes = {date(2026, 10, 2): D(110), date(2025, 10, 3): D(100)}  # Fri 2026-10-02; Fri 2025-10-03
    # D = Sunday → Friday 2026-10-02; D − 365 = Saturday 2025-10-04 → Friday 2025-10-03.
    got = ro.regime(closes, sun)
    assert (got.label, got.session, got.back_session) == ("up", date(2026, 10, 2), date(2025, 10, 3))
    assert ro.regime(closes | {date(2026, 10, 2): D(90)}, sun).label == "down"
    assert ro.regime(closes | {date(2026, 10, 2): D(100)}, sun).label == "flat"
    assert ro.regime(closes | {date(2026, 10, 2): None}, sun).label == "unavailable"  # masked
    assert ro.regime(closes | {date(2025, 10, 3): D(0)}, sun).label == "unavailable"
    assert ro.regime(closes | {date(2026, 10, 2): D("NaN")}, sun).label == "unavailable"
    assert ro.regime({date(2026, 10, 2): D(110)}, sun).label == "unavailable"  # no nearer session substituted


def test_step_storage_records_entries_and_ineligible_exits() -> None:
    _, result, _ = st.advance(
        _book(),
        session=FRI,
        bars=_bars(FRI, {1: FLAT, 2: FLAT}, {1: {THU: D(100)}, 2: {THU: D(100)}}),
        rebalance=_rebalance(FRI, THU, R, set(R)),
        donor_of=None,
        wind=False,
        used=set(),
    )
    stats = st.EntryStats.of(result, FRI)
    assert (stats.entered, stats.bought, stats.ineligible_exits) == (2, D(1), 0)
    doc = st.shadow_doc(result, None, FRI, TABLE)
    assert (doc["entered"], D(doc["bought"]), doc["ineligible_exits"]) == (2, 1, 0)
    assert st.EntryStats.of(result, MON).entered == 0
    closed = sim.ClosedLifecycle(1, 1, 0, FRI, TUE, D(100), D(100), D("0.5"), D("0.5"), "ineligible:untradable")
    assert st.EntryStats.of(replace(result, closed=(closed,)), TUE).ineligible_exits == 1


def test_ratios_use_the_fixed_context_whatever_the_callers() -> None:
    from decimal import getcontext

    rows, infos = _stream()
    reference = _facts(rows, infos).finish(h0=H, h_end=H)
    saved = getcontext().prec
    getcontext().prec = 6
    try:
        assert _facts(rows, infos).finish(h0=H, h_end=H) == reference
    finally:
        getcontext().prec = saved


def test_deflated_sharpe_matches_the_house_implementation() -> None:
    """§9.4 slice 6c-ii-b: the in-hash re-implementation agrees with ``deflated_sharpe.py`` on the same inputs
    (T = the count, V = 1/T, ρ = 0, one measured trial)."""
    import numpy as np

    from app.services import deflated_sharpe as house

    assert ro.EULER_GAMMA == float(np.euler_gamma)
    returns = [D(x) for x in ("0.031", "-0.012", "0.044", "0.002", "-0.027", "0.019", "0.058", "-0.004")]
    for m in (2, 480):
        got = ro.deflated_sharpe(returns, declared_trials=m, floored_searches=0, register_version="r")
        moments = house.trade_moments([float(x) for x in returns])
        assert moments is not None
        ref = house.deflated_sharpe(
            moments,
            effective_sample_size=float(len(returns)),
            trial_sharpe_variance=1 / len(returns),
            declared_trials=m,
            average_correlation=0.0,
            measured_trials=1,
            trial_register_version="r",
            null_floor_variance=True,
        )
        assert ref is not None and got["reason"] is None
        assert abs(float(got["dsr"]) - ref.deflated_sharpe) < 1e-9
        assert abs(float(got["sr0"]) - ref.expected_max_sharpe) < 1e-12
        assert abs(float(got["sr"]) - moments.sharpe) < 1e-12 and abs(float(got["kurtosis"]) - moments.kurtosis) < 1e-9
        assert (got["t"], got["m"], len(got["returns"]), got["v"]) == (8, m, 8, "0.125")


def test_deflated_sharpe_refusals() -> None:
    def reason(returns: list[str], m: int = 480) -> str | None:
        return ro.deflated_sharpe([D(x) for x in returns], declared_trials=m, floored_searches=0, register_version="r")[
            "reason"
        ]

    assert reason(["0.1", "0.2"]) == "too_few_returns"
    assert reason(["0.1", "0.1", "0.1"]) == "zero_variance"
    assert reason(["0.1", "0.2", "0.4"], m=1) == "register_below_two"
    out = ro.deflated_sharpe(
        [D("0.1"), D("0.2"), D("0.4")], declared_trials=1, floored_searches=0, register_version="r"
    )
    assert out["sr"] is not None and out["dsr"] is None and out["sr0"] is None  # the moments are still reported
    # Codex ckpt-1: exact moments. Identical returns are zero variance however they round, and a two-point series
    # on Pearson's boundary has a bracket of exactly zero (SR₀ is still reported).
    assert reason(["0.1234567890123456789012345678901235"] * 24) == "zero_variance"
    edge = ro.deflated_sharpe(
        [D("0.001"), D("0.001"), D("0.002")], declared_trials=480, floored_searches=0, register_version="r"
    )
    assert edge["reason"] == "degenerate_moments" and edge["sr0"] is not None
    assert reason(["0.1", "0.2", "0.4"], m=10**16) == "quantile_out_of_range"
    # The readout over the two-interval stream: too few returns, with the register's count reported.
    rows, infos = _stream()
    dsr = _facts(rows, infos).finish(h0=H, h_end=H)["deflated_sharpe"]
    assert dsr["reason"] == "too_few_returns" and dsr["t"] == 2 and dsr["m"] >= 2
