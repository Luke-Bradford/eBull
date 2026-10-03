"""#3592 slice 4b-i — v2's looks, pure parts (spec §7 "Looks" and "Verdict"; Appendix A R2-16–20): condition 6
against the v1-reference book (strict T, NAV ≥, minimum, undefined), condition 7 turnover (initial fill excluded),
every ``v2_verdict`` combination, and the fail-closed ``detail.v2`` decode."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_v2_look as v2look
from app.services import ranking_pot_v2_policy as policy
from tests.test_ranking_pot_look import FRI, MON, TERMS, TUE, _handmade

D = Decimal
BAR = policy.TURNOVER_BAR


def _reference(**over: Any) -> look.LookFacts:
    """A reference book over the same three sessions: T = 0.005 (below the shadow's 0.01), path end 1.05."""
    facts = look.LookFacts(t0=FRI, endpoint=TUE, k=0)
    facts.sessions = 3
    facts.shadow_sum, facts.shadow_count = D("0.015"), 3
    facts.shadow_path = [D(1), D("1.00"), D("1.02"), D("1.05")]
    facts.held_total = 6
    facts.invested = {11: (1, D("0.5")), 12: (2, D("0.5")), 13: (4, D("0.5")), 14: (5, D("0.5"))}
    facts.terminal = {11: D("0.5"), 12: D("0.5"), 13: D("0.5"), 14: D("0.5")}
    for k, v in over.items():
        setattr(facts, k, v)
    return facts


def _turn(*entered: int) -> v2look.TurnoverFacts:
    t = v2look.TurnoverFacts(t0=FRI)
    t.add(FRI, True, 2)  # the initial fill: not turnover
    for i, e in enumerate(entered):
        t.add(date(2026, 11 + i, 2), True, e)
    return t


def _eval(shadow: look.LookFacts | None = None, reference: look.LookFacts | None = None, **kw: Any) -> look.LookResult:
    return v2look.evaluate(
        shadow or _handmade(),
        reference or _reference(),
        kw.pop("turn", _turn(1)),
        kw.pop("terms", TERMS),
        h0=D("0.001"),
        h_end=D("0.001"),
        turnover_bar=BAR,
        skipped_months=kw.pop("skipped", 0),
    )


def test_a_research_pass_needs_v1s_shadow_pass_and_conditions_6_and_7() -> None:
    result = _eval()
    assert (result.verdict, result.reasons, result.harm) == ("shadow_pass_execution_unproven", (), False)
    block = result.detail["v2"]
    assert block["conditions"] == {"6_reference": True, "7_turnover": True}
    assert block["v2_verdict"] == "research_pass" and block["reasons"] == []
    assert block["reference"]["t_shadow"] == result.detail["t_obs"]  # one T, v1's
    assert block["reference"]["t_reference"] == "0.005" and block["turnover"]["mean"] == "1/2"
    assert result.detail["conditions"]["5_execution"] is False  # no executed book, by construction
    assert v2look.v2_detail_of(result.detail).v2_verdict == "research_pass"


def test_condition_6_fails_on_a_t_tie_and_on_a_lower_nav() -> None:
    tie = _eval(reference=_reference(shadow_sum=D("0.03")))  # T_ref = T_shadow = 0.01
    assert tie.detail["v2"]["conditions"]["6_reference"] is False
    assert tie.detail["v2"]["v2_verdict"] == "not_passed"
    lower_nav = _eval(reference=_reference(shadow_path=[D(1), D("1.2"), D("1.2"), D("1.11")]))
    assert lower_nav.detail["v2"]["conditions"]["6_reference"] is False  # T higher, NAV 1.10 < 1.11
    equal_nav = _eval(reference=_reference(shadow_path=[D(1), D("1.2"), D("1.2"), D("1.10")]))
    assert equal_nav.detail["v2"]["conditions"]["6_reference"] is True  # NAV is ≥


def test_reference_unevaluable_reasons_live_in_detail_v2_only() -> None:
    thin = _eval(reference=_reference(invested={11: (1, D("0.5"))}, terminal={11: D("0.5")}))
    assert thin.verdict == "shadow_pass_execution_unproven" and thin.reasons == ()  # v1's row fields untouched
    assert thin.detail["v2"]["reasons"] == ["reference_below_minimum"]
    assert thin.detail["v2"]["v2_verdict"] == "unevaluable"
    idle = _eval(reference=_reference(held_total=2))  # occupancy 2 / (3 × 2) < ½
    assert idle.detail["v2"]["reasons"] == ["reference_below_minimum"]
    empty = _eval(reference=_reference(shadow_sum=D(0), shadow_count=0))
    assert "reference_t_undefined" in empty.detail["v2"]["reasons"]
    assert empty.detail["v2"]["conditions"]["6_reference"] is None
    assert v2look.v2_detail_of(empty.detail).v2_verdict == "unevaluable"


def test_every_v2_verdict_combination() -> None:
    # v1 unevaluable → unevaluable, whatever 6 and 7 say.
    v1_thin = _eval(shadow=_handmade(held_total=1))
    assert v1_thin.verdict == "unevaluable" and v1_thin.detail["v2"]["v2_verdict"] == "unevaluable"
    # v1 not passed (condition 2: the shadow ends below SPY) with 6 and 7 holding → not_passed.
    v1_fail = _eval(shadow=_handmade(spy_closes=[D(500), D(490), D(600)]))
    assert v1_fail.verdict == "not_passed" and v1_fail.detail["v2"]["conditions"]["6_reference"] is True
    assert v1_fail.detail["v2"]["v2_verdict"] == "not_passed"
    # v1's shadow pass with 7 failing → not_passed.
    churn = _eval(turn=_turn(2, 1))  # mean (1 + ½) / 2 = ¾ > ½
    assert churn.detail["v2"]["conditions"] == {"6_reference": True, "7_turnover": False}
    assert churn.detail["v2"]["v2_verdict"] == "not_passed"
    # Condition 7 holds at the bar exactly.
    assert _eval(turn=_turn(2, 0)).detail["v2"]["conditions"]["7_turnover"] is True


def test_turnover_excludes_the_initial_fill_and_unapplied_sessions() -> None:
    t = v2look.TurnoverFacts(t0=FRI)
    t.add(FRI, True, 2)
    t.add(MON, False, 0)  # no rebalance applied: not a turnover observation
    assert t.entries == [] and v2look.turnover(t.entries, 2) is None
    t.add(TUE, True, 1)
    assert v2look.turnover(t.entries, 2) == Fraction(1, 2)
    with pytest.raises(ValueError, match="not T0"):
        v2look.TurnoverFacts(t0=FRI).add(MON, True, 2)
    skipped = _eval(turn=_turn(), skipped=3)
    assert skipped.detail["v2"]["turnover"] == {
        "mean": None,
        "bar": "1/2",
        "rebalances": 0,
        "entered": [],
        "skipped_months": 3,
    }
    assert skipped.detail["v2"]["conditions"]["7_turnover"] is None
    assert skipped.detail["v2"]["v2_verdict"] == "not_passed"  # fail closed: an undefined condition never passes


def test_detail_v2_decode_fails_closed() -> None:
    good = _eval().detail
    with pytest.raises(rb.SnapshotIntegrityError, match="without its detail.v2"):
        v2look.v2_detail_of({k: v for k, v in good.items() if k != "v2"})
    bad: list[dict[str, Any]] = [
        {"v2_verdict": "pass"},
        {"reasons": ["t_obs_undefined"]},
        {"conditions": {"6_reference": True}},
        {"conditions": {"6_reference": "yes", "7_turnover": True}},
        {"conditions": {"6_reference": False, "7_turnover": True}},  # a research_pass without condition 6
        {"reasons": ["reference_below_minimum"]},  # a v2 reason on an evaluable verdict
        {"reasons": ["reference_t_undefined"], "v2_verdict": "unevaluable"},  # t_reference is defined
        {"conditions": {"6_reference": None, "7_turnover": True}, "v2_verdict": "not_passed"},  # operands defined
        {"conditions": {"6_reference": True, "7_turnover": None}, "v2_verdict": "not_passed"},
        {"turnover": {"mean": "1/2"}},
        {"reference": None},
        {"extra": 1},
    ]
    for over in bad:
        with pytest.raises(rb.SnapshotIntegrityError):
            v2look.v2_detail_of(good | {"v2": good["v2"] | over})
    missing = {k: v for k, v in good["v2"].items() if k != "turnover"}
    with pytest.raises(rb.SnapshotIntegrityError, match="keys"):
        v2look.v2_detail_of(good | {"v2": missing})


def test_the_reference_facts_carry_no_controls() -> None:
    with pytest.raises(ValueError, match="no controls"):
        _eval(reference=look.LookFacts(t0=FRI, endpoint=TUE, k=1))
