"""#3471 slice v6-3a — pure tests for the v6 schema, guard and control pre-filter.

Spec §16.3 (orders 1–13), §16.4 (pool), §16.11 (baseline) and obligations O-v6-1 (re-derive
from the stored pack), O-v6-2 (self-membership) and O-v6-6 (guard tests).
"""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from app.services.ai_trial_decision import MAX_ENTRIES_PER_RUN, AtrMeasurement, measure_atr
from app.services.ai_trial_guard import (
    LIBRARY_MIN_NAMES,
    LIBRARY_MIN_PLANS,
    NameStructure,
    control_plan,
    control_pool,
    decision_json_schema,
    is_canonical_fraction,
    library_baseline,
    pack_structures,
    plan_pairs,
    validate_response,
)
from app.services.ai_trial_levels import LEVEL_IDS, SETUP_TYPES, SUPPORT_LEVEL_IDS, TARGET_LEVEL_IDS, Level
from app.services.ai_trial_pack import canonical_json
from app.services.ai_trial_pack_reader import load_setup_library
from scripts.ai_trial_synthetic import synthetic_pack

LIBRARY = load_setup_library()["rows"]
DECL = "a" * 64
SESSION = date(2026, 10, 5)
SHORTLIST = {"AAA": 11, "BBB": 22, "CCC": 33}

# close 100, ATR14 2 (2%). Stop = invalidation − 0.5.
ATR: AtrMeasurement | None = measure_atr(2.0, 100.0)
PRICES: dict[str, str | None] = {
    "sma50": "97.5",  # stop 97 → 1.5 ATR, stop 3%
    "swing_low_1": "99.5",  # stop 99 → 0.5 ATR: under every horizon floor
    "sma200": "90",  # stop 89.5 → 5.25 ATR: over the ceiling
    "sma20": "101",  # above close: unavailable
    "donchian20_low": "98.5",  # stop 98 → 1.0 ATR: the 5-session floor exactly
    "range20_projection": "108",  # target 8% → R 8/3 from sma50
    "donchian20_high": "103",  # R 1 from sma50
    "mm_up": "250",  # target 150%: outside the bounds
    "swing_high_1": None,
}


def _structure(
    detected: frozenset[str] = frozenset({"pullback_rising_sma20"}),
    atr: AtrMeasurement | None = ATR,
    prices: dict[str, str | None] | None = None,
) -> NameStructure:
    table = PRICES if prices is None else prices
    levels = {lid: None if table.get(lid) is None else Level(Fraction(str(table[lid])), None) for lid in LEVEL_IDS}
    return NameStructure(atr, levels, detected)


STRUCTURES = {11: _structure(), 22: _structure(), 33: _structure()}


def _decision(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "action": "enter_long",
        "symbol": "AAA",
        "setup_type": "pullback_rising_sma20",
        "invalidation_level_id": "sma50",
        "target_level_id": "range20_projection",
        "horizon_days": 5,
        "size_tier": "full",
        "confidence": 3,
        "thesis": "Filing shows margin expansion the base rate cannot see.",
    }
    base.update(overrides)
    return base


def _validate(
    output: object,
    *,
    held: frozenset[int] = frozenset(),
    max_new: int = 2,
    structures: dict[int, NameStructure] | None = None,
    library: list[dict[str, Any]] | None = None,
):
    return validate_response(
        output,
        shortlist=SHORTLIST,
        structures=STRUCTURES if structures is None else structures,
        library_rows=LIBRARY if library is None else library,
        arm_held_instrument_ids=held,
        max_new_entries=max_new,
    )


def _one(**overrides: Any):
    [verdict] = _validate({"decisions": [_decision(**overrides)], "no_trade_reason": None}).verdicts
    return verdict


def _library_with(setup: str, horizon: int, *, train: object = "-1/10", holdout: object = "-1/20", **half: Any):
    """The real library with one row's halves rewritten (O-v6-6)."""
    rows = copy.deepcopy(LIBRARY)
    [row] = [r for r in rows if r["setup_type"] == setup and r["horizon_days"] == horizon]
    for name, mean in (("train", train), ("holdout", holdout)):
        row["halves"][name]["mean_net_r"] = mean
        row["halves"][name].update(half)
    return rows


class TestSchema:
    def test_schema_forbids_extra_and_caps_items(self) -> None:
        schema = decision_json_schema()
        assert schema["additionalProperties"] is False
        assert schema["properties"]["decisions"]["maxItems"] == MAX_ENTRIES_PER_RUN
        assert set(schema["required"]) == {"decisions", "no_trade_reason"}

    def test_level_roles_are_restricted_by_the_enums(self) -> None:
        item = decision_json_schema()["$defs"]["TrialDecision"]["properties"]
        assert tuple(item["invalidation_level_id"]["enum"]) == SUPPORT_LEVEL_IDS
        assert tuple(item["target_level_id"]["enum"]) == TARGET_LEVEL_IDS
        assert tuple(item["setup_type"]["enum"]) == (*SETUP_TYPES, "none")
        assert "stop_pct" not in item and "target_pct" not in item  # the model never types a level


class TestWholeResponseRefusals:
    def test_missing_structured_output(self) -> None:
        assert _validate(None).whole_refusal == "no_structured_output"

    @pytest.mark.parametrize(
        "output",
        [
            {"decisions": []},  # no_trade_reason key missing
            {"decisions": [], "no_trade_reason": None, "extra": 1},
            {"decisions": [_decision(action="exit")], "no_trade_reason": None},
            {"decisions": [_decision(setup_type="head_and_shoulders")], "no_trade_reason": None},
            # Role restriction: a resistance id as the invalidation, a support id as the target.
            {"decisions": [_decision(invalidation_level_id="swing_high_1")], "no_trade_reason": None},
            {"decisions": [_decision(target_level_id="sma50")], "no_trade_reason": None},
            {"decisions": [_decision(horizon_days=7)], "no_trade_reason": None},
            {"decisions": [_decision(size_tier="double")], "no_trade_reason": None},
            {"decisions": [_decision(confidence=6)], "no_trade_reason": None},
            {"decisions": [_decision(confidence=3.0)], "no_trade_reason": None},  # strict: no float->int
            {"decisions": [_decision(confidence=True)], "no_trade_reason": None},  # strict: no bool->int
            {"decisions": [_decision(thesis="")], "no_trade_reason": None},
            {"decisions": [_decision(thesis="x" * 601)], "no_trade_reason": None},
            {"decisions": [_decision(symbol="X" * 17)], "no_trade_reason": None},
            {"decisions": [{**_decision(), "stop_pct": 5.0}], "no_trade_reason": None},  # v5 field
            {"decisions": [_decision()] * (MAX_ENTRIES_PER_RUN + 1), "no_trade_reason": None},
            ["not", "an", "object"],
        ],
    )
    def test_schema_violations_are_malformed(self, output: object) -> None:
        result = _validate(output)
        assert result.whole_refusal == "malformed_response"
        assert result.verdicts == ()

    def test_over_entry_cap_refuses_whole_response_not_truncate(self) -> None:
        output = {"decisions": [_decision(symbol="AAA"), _decision(symbol="BBB")], "no_trade_reason": None}
        assert _validate(output, max_new=1).whole_refusal == "over_entry_cap"
        assert _validate({"decisions": [], "no_trade_reason": "full"}, max_new=0).whole_refusal is None

    @pytest.mark.parametrize("reason", [None, "", "   "])
    def test_empty_decisions_need_a_reason(self, reason: str | None) -> None:
        assert _validate({"decisions": [], "no_trade_reason": reason}).whole_refusal == "no_trade_reason_missing"


class TestPrecedence:
    def test_accepted_with_derived_figures(self) -> None:
        v = _one()
        assert v.accepted and v.instrument_id == 11
        f = v.plan.figures
        assert (v.plan.invalidation_price, v.plan.target_price, f.stop_price) == (
            Fraction("97.5"),
            Fraction(108),
            Fraction(97),
        )
        assert (f.stop_atr_multiple, f.stop_pct, f.target_pct, f.r_multiple) == (
            Decimal("1.5000"),
            Decimal("3.0000"),
            Decimal("8.0000"),
            Decimal("2.6667"),
        )

    @pytest.mark.parametrize(
        ("overrides", "reason"),
        [
            ({"symbol": "ZZZ"}, "not_in_shortlist"),
            ({"symbol": ""}, "not_in_shortlist"),
            ({"setup_type": "none"}, "no_valid_plan"),
            ({"setup_type": "breakout_donchian20"}, "setup_not_detected"),
            ({"invalidation_level_id": "sma20"}, "level_unavailable"),  # above close
            ({"target_level_id": "swing_high_1"}, "level_unavailable"),  # null level
            ({"invalidation_level_id": "swing_low_1"}, "stop_below_horizon_floor"),
            ({"invalidation_level_id": "sma50", "horizon_days": 20}, "stop_below_horizon_floor"),  # 1.5 < 2
            ({"invalidation_level_id": "sma50", "horizon_days": 10}, None),  # 1.5, inclusive
            ({"invalidation_level_id": "donchian20_low"}, None),  # 1.0 at 5 sessions, inclusive
            ({"invalidation_level_id": "sma200"}, "stop_outside_atr_band"),
            ({"target_level_id": "donchian20_high"}, "reward_risk_below_min"),
            ({"target_level_id": "mm_up"}, "plan_outside_bounds"),
            ({"thesis": "A. B. C. D."}, "thesis_too_long"),
        ],
    )
    def test_orders(self, overrides: dict[str, Any], reason: str | None) -> None:
        assert _one(**overrides).reason_code == reason

    def test_duplicate_and_held(self) -> None:
        result = _validate({"decisions": [_decision(), _decision(confidence=2)], "no_trade_reason": None})
        assert [v.reason_code for v in result.verdicts] == ["duplicate_symbol", "duplicate_symbol"]
        [held] = _validate({"decisions": [_decision()], "no_trade_reason": None}, held=frozenset({11})).verdicts
        assert held.reason_code == "already_held"

    def test_first_failure_names_the_reason(self) -> None:
        # Held, setup none, bad level and long thesis at once: order 3 wins.
        bad = _decision(setup_type="none", invalidation_level_id="sma20", thesis="A. B. C. D.")
        [v] = _validate({"decisions": [bad], "no_trade_reason": None}, held=frozenset({11})).verdicts
        assert v.reason_code == "already_held"

    @pytest.mark.parametrize("atr", [None, measure_atr(0.0, 100.0), measure_atr(0.000001, 100.0)])
    def test_an_invalid_measurement_is_level_unavailable(self, atr: AtrMeasurement | None) -> None:
        assert atr is None  # missing, zero, and a tiny ATR quantizing to 0 are all invalid
        [v] = _validate(
            {"decisions": [_decision()], "no_trade_reason": None}, structures={11: _structure(atr=atr)}
        ).verdicts
        assert v.reason_code == "level_unavailable"
        assert v.plan.atr is None and v.plan.figures.stop_pct is None

    def test_figures_are_recorded_whatever_fails_first_and_signed(self) -> None:
        # Refused at order 4, yet every figure is derived; an invalidation above close gives a
        # negative stop distance, recorded with qs and no R (denominator ≤ 0).
        v = _one(setup_type="none", invalidation_level_id="sma20")
        assert v.reason_code == "no_valid_plan"
        f = v.plan.figures
        assert (f.stop_price, f.stop_atr_multiple, f.stop_pct, f.r_multiple) == (
            Fraction("100.5"),
            Decimal("-0.2500"),
            Decimal("-0.5000"),
            None,
        )
        assert _one(symbol="ZZZ").plan.figures.stop_pct is None

    def test_mixed_response_keeps_positions(self) -> None:
        result = _validate({"decisions": [_decision(symbol="ZZZ"), _decision(symbol="BBB")], "no_trade_reason": None})
        assert [(v.response_position, v.reason_code) for v in result.verdicts] == [(0, "not_in_shortlist"), (1, None)]
        assert [v.instrument_id for v in result.accepted] == [22]


class TestBaseline:
    """O-v6-6: the library row is recorded as the baseline, never a refusal (§16.11)."""

    def test_every_real_row_meets_the_order_6_minimums(self) -> None:
        assert all(library_baseline(LIBRARY, s, h) is not None for s in SETUP_TYPES for h in (5, 10, 20))

    @pytest.mark.parametrize(("train", "holdout"), [("-1/10", "-1/20"), ("1/20", "-1/10"), ("0/1", "1/3")])
    def test_negative_or_mixed_rows_are_accepted_with_both_baselines(self, train: str, holdout: str) -> None:
        library = _library_with("pullback_rising_sma20", 5, train=train, holdout=holdout)
        [v] = _validate({"decisions": [_decision()], "no_trade_reason": None}, library=library).verdicts
        assert v.accepted
        assert v.baseline is not None and (v.baseline.train_mean_net_r, v.baseline.holdout_mean_net_r) == (
            train,
            holdout,
        )

    def test_the_baseline_is_the_rows_verbatim(self) -> None:
        [row] = [r for r in LIBRARY if r["setup_type"] == "pullback_rising_sma20" and r["horizon_days"] == 5]
        baseline = _one().baseline
        assert baseline is not None
        assert baseline.train_mean_net_r == row["halves"]["train"]["mean_net_r"]
        assert baseline.holdout_mean_net_r == row["halves"]["holdout"]["mean_net_r"]

    @pytest.mark.parametrize(
        "overrides",
        [
            {"invalidation_level_id": "sma200"},  # order 10
            {"target_level_id": "donchian20_high"},  # order 11
            {"thesis": "A. B. C. D."},  # order 13
        ],
    )
    def test_a_refusal_at_orders_8_to_13_keeps_both_baselines(self, overrides: dict[str, Any]) -> None:
        v = _one(**overrides)
        assert v.reason_code is not None and v.baseline is not None

    @pytest.mark.parametrize(
        "overrides", [{"symbol": "ZZZ"}, {"setup_type": "none"}, {"setup_type": "breakout_donchian20"}]
    )
    def test_a_refusal_at_orders_1_to_5_stores_none(self, overrides: dict[str, Any]) -> None:
        assert _one(**overrides).baseline is None

    def test_held_stores_none(self) -> None:
        [v] = _validate({"decisions": [_decision()], "no_trade_reason": None}, held=frozenset({11})).verdicts
        assert v.baseline is None

    @pytest.mark.parametrize(
        "library",
        [
            [r for r in LIBRARY if not (r["setup_type"] == "pullback_rising_sma20" and r["horizon_days"] == 5)],
            _library_with("pullback_rising_sma20", 5, plans=LIBRARY_MIN_PLANS - 1),
            _library_with("pullback_rising_sma20", 5, n_names=LIBRARY_MIN_NAMES - 1),
            _library_with("pullback_rising_sma20", 5, pct_stop="NaN"),
            _library_with("pullback_rising_sma20", 5, mean_net_pct="Infinity"),
            _library_with("pullback_rising_sma20", 5, train="2/4"),  # not reduced
            _library_with("pullback_rising_sma20", 5, holdout="1/-3"),
            _library_with("pullback_rising_sma20", 5, holdout=0.05),  # not a string
        ],
    )
    def test_order_6_refuses_an_unusable_row_and_stores_none(self, library: list[dict[str, Any]]) -> None:
        [v] = _validate({"decisions": [_decision()], "no_trade_reason": None}, library=library).verdicts
        assert v.reason_code == "setup_base_rate_missing" and v.baseline is None

    @pytest.mark.parametrize("drop", ["train", "holdout"])
    def test_a_missing_half_is_order_6(self, drop: str) -> None:
        library = copy.deepcopy(LIBRARY)
        [row] = [r for r in library if r["setup_type"] == "pullback_rising_sma20" and r["horizon_days"] == 5]
        del row["halves"][drop]
        assert library_baseline(library, "pullback_rising_sma20", 5) is None

    @pytest.mark.parametrize("key", ["pct_target", "deduped", "avg_win_net_pct"])
    def test_a_missing_statistic_is_order_6(self, key: str) -> None:
        library = copy.deepcopy(LIBRARY)
        [row] = [r for r in library if r["setup_type"] == "pullback_rising_sma20" and r["horizon_days"] == 5]
        del row["halves"]["train"][key]
        assert library_baseline(library, "pullback_rising_sma20", 5) is None

    def test_an_empty_win_subset_is_allowed(self) -> None:
        # r2-47: `avg_win_net_pct` is null over an empty subset — present, not missing.
        library = _library_with("pullback_rising_sma20", 5, avg_win_net_pct=None)
        assert library_baseline(library, "pullback_rising_sma20", 5) is not None

    @pytest.mark.parametrize(
        ("value", "ok"),
        [("0/1", True), ("-3/7", True), ("3/7", True), ("0/5", False), ("6/14", False), ("-0/1", False), ("3", False)],
    )
    def test_canonical_fraction(self, value: str, ok: bool) -> None:
        assert is_canonical_fraction(value) is ok


def _accepted(symbol: str = "AAA", **overrides: Any):
    [v] = _validate({"decisions": [_decision(symbol=symbol, **overrides)], "no_trade_reason": None}).verdicts
    assert v.accepted
    return v


class TestControl:
    def test_pool_filters_by_setup_and_the_arms_ids_on_each_name(self) -> None:
        structures = {
            11: _structure(),
            22: _structure(detected=frozenset({"breakout_donchian20"})),  # setup not detected
            33: _structure(prices={**PRICES, "sma50": "98.5"}),  # stop 1.0 ATR (the floor), R 4 — feasible
            44: _structure(prices={**PRICES, "range20_projection": "104"}),  # R 4/3 — infeasible
            55: _structure(),  # held by the control leg
        }
        [planned] = plan_pairs(
            (_accepted(),),
            shortlist_instrument_ids=[55, 44, 33, 22, 11],
            structures=structures,
            control_held_instrument_ids=frozenset({55}),
            declaration_sha256_hex=DECL,
            session_date=SESSION,
            first_pair_seq=3,
        )
        assert planned.pair is not None and planned.pair.draw.pool == (11, 33)
        assert planned.pair.pair_seq == 3

    def test_the_control_uses_its_own_prices_for_the_arms_ids(self) -> None:
        name = _structure(atr=measure_atr(4.0, 200.0), prices={"sma50": "195", "range20_projection": "216"})
        plan = control_plan(_accepted(), name)
        assert plan is not None
        # stop 195 − 1 = 194 → 1.5 ATR, 3%; target 8%; R 16 / 6.
        assert (plan.invalidation_price, plan.target_price, plan.figures.stop_price) == (195, 216, 194)
        assert (plan.figures.stop_pct, plan.figures.target_pct, plan.figures.r_multiple) == (
            Decimal("3.0000"),
            Decimal("8.0000"),
            Decimal("2.6667"),
        )

    def test_draws_without_replacement_and_numbers_only_created_pairs(self) -> None:
        verdicts = tuple(
            replace(v, response_position=i)
            for i, v in enumerate((_accepted("AAA"), _one(symbol="ZZZ"), _accepted("BBB")))
        )
        planned = plan_pairs(
            verdicts,
            shortlist_instrument_ids=[11, 22, 33],
            structures=STRUCTURES,
            control_held_instrument_ids=frozenset({33}),
            declaration_sha256_hex=DECL,
            session_date=SESSION,
            first_pair_seq=7,
        )
        assert [p.reason_code for p in planned] == [None, "not_in_shortlist", None]
        first, second = planned[0].pair, planned[2].pair
        assert first is not None and second is not None
        assert (first.pair_seq, second.pair_seq) == (7, 8)
        assert 33 not in first.draw.pool
        assert second.draw.pool == tuple(i for i in (11, 22) if i != first.draw.instrument_id)

    def test_exhaustion_refuses_the_arm_and_consumes_no_seq(self) -> None:
        structures = {11: _structure(), 22: _structure(atr=None)}
        [first] = _validate(
            {"decisions": [_decision(symbol="AAA")], "no_trade_reason": None}, structures=structures
        ).verdicts
        planned = plan_pairs(
            (first, replace(first, response_position=1)),
            shortlist_instrument_ids=[11, 22],
            structures=structures,
            control_held_instrument_ids=frozenset(),
            declaration_sha256_hex=DECL,
            session_date=SESSION,
            first_pair_seq=0,
        )
        # Name 22 has no valid measurement (never feasible); name 11 is drawn by the first pair.
        assert planned[0].pair is not None and planned[0].pair.pair_seq == 0
        assert planned[1].reason_code == "control_pool_exhausted" and planned[1].pair is None

    def test_pool_is_ordered_and_keeps_the_arms_own_name(self) -> None:
        pool = control_pool(
            [33, 11, 22, 44, 55],
            control_held_instrument_ids=frozenset({44}),
            drawn_this_run=frozenset({22}),
            feasible_instrument_ids=frozenset({11, 22, 33, 44}),
        )
        assert pool == (11, 33)


SYNTHETIC = synthetic_pack()


def _every_decision(pack: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        _decision(symbol=entry["symbol"], setup_type=s, invalidation_level_id=i, target_level_id=t, horizon_days=h)
        for entry in pack["names"]
        for s in sorted(pack["names"][0]["setups"])
        for i in SUPPORT_LEVEL_IDS
        for t in TARGET_LEVEL_IDS
        for h in (5, 10, 20)
    ]


def _verdicts(pack: dict[str, Any]):
    structures = pack_structures(pack["names"])
    return [
        v
        for d in _every_decision(pack)
        for v in validate_response(
            {"decisions": [d], "no_trade_reason": None},
            shortlist=SYNTHETIC.complete,
            structures=structures,
            library_rows=LIBRARY,
            arm_held_instrument_ids=frozenset(),
            max_new_entries=2,
        ).verdicts
    ]


def test_o_v6_2_every_accepted_arm_is_in_its_own_pool() -> None:
    structures = pack_structures(SYNTHETIC.pack["names"])
    verdicts = _verdicts(SYNTHETIC.pack)
    accepted = [v for v in verdicts if v.accepted]
    assert accepted, "the synthetic pack must admit at least one accepted plan"
    for v in accepted:
        assert v.instrument_id is not None
        own = control_plan(v, structures[v.instrument_id])
        assert own == v.plan
        pool = control_pool(
            sorted(SYNTHETIC.complete.values()),
            control_held_instrument_ids=frozenset(),
            drawn_this_run=frozenset(),
            feasible_instrument_ids=frozenset(iid for iid, n in structures.items() if control_plan(v, n) is not None),
        )
        assert v.instrument_id in pool


def test_o_v6_1_every_figure_re_derives_from_the_stored_pack() -> None:
    # The pack as JSONB stores it (canonical JSON: Decimals as strings, floats as numbers).
    stored = json.loads(canonical_json(SYNTHETIC.pack))
    live, again = _verdicts(SYNTHETIC.pack), _verdicts(stored)
    assert [(v.reason_code, v.plan, v.baseline) for v in live] == [(v.reason_code, v.plan, v.baseline) for v in again]
    assert any(v.accepted for v in again)
