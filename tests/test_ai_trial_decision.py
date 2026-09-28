"""#3471 slices 1a / 2b-ATR — pure tests for the AI-trial decision layer (spec §5-§7 v5, O3/O5)."""

from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from app.services.ai_trial_decision import (
    MAX_ENTRIES_PER_RUN,
    AtrMeasurement,
    ControlPoolExhausted,
    StrictJSONError,
    control_pool,
    count_sentences,
    decision_json_schema,
    decision_metrics,
    derive_control_levels,
    draw_control,
    measure_atr,
    pack_atr_measurements,
    quantize,
    strict_json_loads,
    validate_response,
)

SHORTLIST = {"AAA": 11, "BBB": 22, "CCC": 33}
DECL = "a" * 64
# ATR 2.5% on every name: the default decision (stop 5, target 15) is 2 ATRs and R = 3.
ATR_2_5 = measure_atr(2.5, 100.0)
ATR = {11: ATR_2_5, 22: ATR_2_5, 33: ATR_2_5}


def _atr(pct: str) -> AtrMeasurement:
    measured = measure_atr(float(pct), 100.0)
    assert measured is not None and measured.atr14_pct == Decimal(pct)
    return measured


def _decision(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "action": "enter_long",
        "symbol": "AAA",
        "stop_pct": 5.0,
        "target_pct": 15.0,
        "horizon_days": 10,
        "size_tier": "full",
        "confidence": 3,
        "thesis": "Base breakout on rising volume.",
    }
    base.update(overrides)
    return base


def _validate(
    output: object,
    *,
    held: frozenset[int] = frozenset(),
    max_new: int = 2,
    atr: dict[int, AtrMeasurement | None] | None = None,
):
    return validate_response(
        output,
        shortlist=SHORTLIST,
        atr_by_instrument=ATR if atr is None else atr,
        arm_held_instrument_ids=held,
        max_new_entries=max_new,
    )


class TestStrictJson:
    def test_duplicate_key_refused(self) -> None:
        with pytest.raises(StrictJSONError):
            strict_json_loads('{"a": 1, "a": 2}')

    @pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
    def test_non_finite_constant_refused(self, constant: str) -> None:
        with pytest.raises(StrictJSONError):
            strict_json_loads(f'{{"stop_pct": {constant}}}')

    def test_decoder_limits_are_strict_json_errors(self) -> None:
        with pytest.raises(StrictJSONError):
            strict_json_loads('{"n": ' + "1" * 5000 + "}")
        with pytest.raises(StrictJSONError):
            strict_json_loads("[" * 100_000 + "]" * 100_000)

    def test_plain_json_parses(self) -> None:
        assert strict_json_loads('{"a": [1, 2.5, null]}') == {"a": [1, 2.5, None]}


class TestSentences:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("", 0),
            ("One.", 1),
            ("One. Two.", 2),
            ("One. Two", 2),
            ("Up 3.5% on the day.", 1),
            ("Wow!! Really? Yes.", 3),
            ("No terminator at all", 1),
        ],
    )
    def test_count(self, text: str, expected: int) -> None:
        assert count_sentences(text) == expected


class TestWholeResponseRefusals:
    def test_missing_structured_output(self) -> None:
        assert _validate(None).whole_refusal == "no_structured_output"

    @pytest.mark.parametrize(
        "output",
        [
            {"decisions": []},  # no_trade_reason key missing
            {"decisions": [], "no_trade_reason": None, "extra": 1},
            {"decisions": [_decision(action="exit")], "no_trade_reason": None},
            {"decisions": [_decision(stop_pct=1.9)], "no_trade_reason": None},
            {"decisions": [_decision(stop_pct=25.1)], "no_trade_reason": None},
            {"decisions": [_decision(target_pct=100.5)], "no_trade_reason": None},
            {"decisions": [_decision(horizon_days=7)], "no_trade_reason": None},
            {"decisions": [_decision(size_tier="double")], "no_trade_reason": None},
            {"decisions": [_decision(confidence=6)], "no_trade_reason": None},
            {"decisions": [_decision(confidence=3.0)], "no_trade_reason": None},  # strict: no float->int
            {"decisions": [_decision(confidence=True)], "no_trade_reason": None},  # strict: no bool->int
            {"decisions": [_decision(stop_pct="5")], "no_trade_reason": None},  # strict: no str->float
            {"decisions": [_decision(thesis="")], "no_trade_reason": None},
            {"decisions": [_decision(thesis="x" * 601)], "no_trade_reason": None},
            {"decisions": [_decision(symbol="X" * 17)], "no_trade_reason": None},
            {"decisions": [{**_decision(), "rationale": "extra field"}], "no_trade_reason": None},
            {"decisions": [_decision()] * (MAX_ENTRIES_PER_RUN + 1), "no_trade_reason": None},  # schema maxItems
            ["not", "an", "object"],
        ],
    )
    def test_schema_violations_are_malformed(self, output: object) -> None:
        result = _validate(output)
        assert result.whole_refusal == "malformed_response"
        assert result.verdicts == ()

    def test_int_accepted_for_float_bound(self) -> None:
        result = _validate({"decisions": [_decision(stop_pct=5, target_pct=15)], "no_trade_reason": None})
        assert result.whole_refusal is None

    def test_over_entry_cap_refuses_whole_response_not_truncate(self) -> None:
        output = {"decisions": [_decision(symbol="AAA"), _decision(symbol="BBB")], "no_trade_reason": None}
        result = _validate(output, max_new=1)
        assert result.whole_refusal == "over_entry_cap"
        assert result.verdicts == ()

    def test_zero_free_slots_admits_only_no_trade(self) -> None:
        assert _validate({"decisions": [_decision()], "no_trade_reason": None}, max_new=0).whole_refusal == (
            "over_entry_cap"
        )
        assert _validate({"decisions": [], "no_trade_reason": "full"}, max_new=0).whole_refusal is None

    @pytest.mark.parametrize("reason", [None, "", "   "])
    def test_empty_decisions_need_a_reason(self, reason: str | None) -> None:
        assert _validate({"decisions": [], "no_trade_reason": reason}).whole_refusal == "no_trade_reason_missing"

    def test_no_trade_with_reason_is_valid(self) -> None:
        result = _validate({"decisions": [], "no_trade_reason": "Nothing clears the bar today."})
        assert result.whole_refusal is None
        assert result.verdicts == ()
        assert result.no_trade_reason == "Nothing clears the bar today."


class TestPerDecisionRefusals:
    def test_accepted(self) -> None:
        result = _validate({"decisions": [_decision()], "no_trade_reason": None})
        assert result.whole_refusal is None
        [verdict] = result.verdicts
        assert verdict.accepted and verdict.instrument_id == 11 and verdict.response_position == 0

    def test_not_in_shortlist(self) -> None:
        [v] = _validate({"decisions": [_decision(symbol="ZZZ")], "no_trade_reason": None}).verdicts
        assert v.reason_code == "not_in_shortlist" and v.instrument_id is None

    def test_empty_symbol_is_per_decision_not_whole_response(self) -> None:
        [v] = _validate({"decisions": [_decision(symbol="")], "no_trade_reason": None}).verdicts
        assert v.reason_code == "not_in_shortlist"

    def test_duplicate_refuses_every_copy(self) -> None:
        result = _validate({"decisions": [_decision(), _decision(stop_pct=6.0)], "no_trade_reason": None})
        assert [v.reason_code for v in result.verdicts] == ["duplicate_symbol", "duplicate_symbol"]
        assert result.accepted == ()

    def test_already_held(self) -> None:
        [v] = _validate({"decisions": [_decision()], "no_trade_reason": None}, held=frozenset({11})).verdicts
        assert v.reason_code == "already_held"

    @pytest.mark.parametrize(
        ("stop", "target", "reason"),
        [
            # ATR 2.5%: the band is [2.5, 10], inclusive at both ends.
            (2.4, 15.0, "stop_outside_atr_band"),
            (2.5, 15.0, None),
            (10.0, 15.0, None),
            (10.1, 20.0, "stop_outside_atr_band"),
            # R floor 1.5, inclusive; target <= stop is caught here too (v5: the old code is gone).
            (5.0, 7.5, None),
            (5.0, 7.49, "reward_risk_below_min"),
            (5.0, 5.0, "reward_risk_below_min"),
            (8.0, 6.0, "reward_risk_below_min"),
            # Both fail: the band (order 4) names the reason (codex r2-8).
            (2.0, 2.0, "stop_outside_atr_band"),
        ],
    )
    def test_atr_band_and_reward_risk(self, stop: float, target: float, reason: str | None) -> None:
        [v] = _validate({"decisions": [_decision(stop_pct=stop, target_pct=target)], "no_trade_reason": None}).verdicts
        assert v.reason_code == reason

    @pytest.mark.parametrize("measurement", [None, measure_atr(0.0, 100.0), measure_atr(0.000001, 100.0)])
    def test_an_invalid_measurement_refuses_at_the_band(self, measurement: AtrMeasurement | None) -> None:
        # Missing, zero, and a tiny ATR that quantizes to 0 (codex r2-1): all invalid.
        assert measurement is None
        [v] = _validate({"decisions": [_decision()], "no_trade_reason": None}, atr={11: measurement}).verdicts
        assert v.reason_code == "stop_outside_atr_band"
        assert v.metrics.atr is None and v.metrics.stop_atr_multiple is None

    def test_metrics_are_recorded_whatever_fails_first(self) -> None:
        [unmapped] = _validate({"decisions": [_decision(symbol="ZZZ")], "no_trade_reason": None}).verdicts
        dup, _ = _validate({"decisions": [_decision(), _decision()], "no_trade_reason": None}).verdicts
        assert unmapped.reason_code == "not_in_shortlist" and unmapped.metrics.atr is None
        assert unmapped.metrics.r_multiple == Decimal("3.0000")
        assert dup.reason_code == "duplicate_symbol"
        assert dup.metrics.stop_atr_multiple == Decimal("2.0000") and dup.metrics.atr == ATR_2_5

    def test_thesis_too_long(self) -> None:
        [v] = _validate({"decisions": [_decision(thesis="A. B. C. D.")], "no_trade_reason": None}).verdicts
        assert v.reason_code == "thesis_too_long"

    def test_precedence_first_failure_names_reason(self) -> None:
        # Held AND target<=stop AND long thesis: `already_held` wins (§6 order 3 before 4-6).
        bad = _decision(stop_pct=20.0, target_pct=10.0, thesis="A. B. C. D.")
        [v] = _validate({"decisions": [bad], "no_trade_reason": None}, held=frozenset({11})).verdicts
        assert v.reason_code == "already_held"

    def test_mixed_response_keeps_positions(self) -> None:
        result = _validate({"decisions": [_decision(symbol="ZZZ"), _decision(symbol="BBB")], "no_trade_reason": None})
        assert [(v.response_position, v.reason_code) for v in result.verdicts] == [(0, "not_in_shortlist"), (1, None)]
        assert [v.instrument_id for v in result.accepted] == [22]


class TestSchema:
    def test_schema_forbids_extra_and_caps_items(self) -> None:
        schema = decision_json_schema()
        assert schema["additionalProperties"] is False
        assert schema["properties"]["decisions"]["maxItems"] == MAX_ENTRIES_PER_RUN
        assert set(schema["required"]) == {"decisions", "no_trade_reason"}


class TestQuantizedArithmetic:
    """§6/§7 v5: exact rationals, 4 decimals half up — Codex ckpt-1's counterexamples."""

    def test_quantize_is_half_up_and_exact(self) -> None:
        assert quantize(Fraction(12345, 100000)) == Decimal("0.1235")  # exactly half: up
        assert quantize(Fraction(1, 3)) == Decimal("0.3333")
        # A float would give 0.1 + 0.2 = 0.30000000000000004; the stored reprs are exact.
        assert decision_metrics(0.1, 0.3, None).r_multiple == Decimal("3.0000")

    def test_the_float_r_floor_counterexample_is_decided_exactly(self) -> None:
        # r1-13: in binary64 target/stop = 1.4999999999999998 although target >= 1.5*stop.
        metrics = decision_metrics(2.71, 4.0649999999999995, None)
        assert metrics.r_multiple == Decimal("1.5000")

    def test_control_levels_follow_the_arm_multiples(self) -> None:
        arm = decision_metrics(3.9999, 6.0, _atr("1"))
        assert arm.stop_atr_multiple == Decimal("3.9999")
        levels = derive_control_levels(arm, _atr("0.5001"))
        assert levels is not None
        # r2-4: matched to the quantum, not exactly (the control's own multiple is 3.9998).
        assert levels.stop_pct == Decimal("2.0003")
        assert quantize(Fraction(levels.stop_pct) / Fraction("0.5001")) == Decimal("3.9998")

    def test_same_name_levels_can_move_past_the_fourth_decimal(self) -> None:
        # r2-5: stop 25 / target 100 at ATR 23.0298 -> 25.0012 / 100.0048, and that is placeable?
        arm = decision_metrics(25.0, 100.0, _atr("23.0298"))
        assert (arm.stop_atr_multiple, arm.r_multiple) == (Decimal("1.0856"), Decimal("4.0000"))
        # 25.0012 > 25: the arm's own name is NOT placeable as its control (a selection effect).
        assert derive_control_levels(arm, _atr("23.0298")) is None

    def test_the_arm_name_can_fall_out_of_its_own_pool(self) -> None:
        # r2-7: stop 2 at ATR 1.0341 -> derived stop 1.9999 < 2.
        arm = decision_metrics(2.0, 4.0, _atr("1.0341"))
        assert arm.stop_atr_multiple == Decimal("1.9340")
        assert derive_control_levels(arm, _atr("1.0341")) is None

    @pytest.mark.parametrize(
        ("control_pct", "placeable"),
        [
            ("2.5", True),  # same ATR: same levels
            ("0.99", False),  # derived stop 1.98 < 2
            ("12.5", True),  # stop 25, target 75
            ("12.51", False),  # stop 25.02 > 25
            (None, False),  # invalid measurement
        ],
    )
    def test_placeability(self, control_pct: str | None, placeable: bool) -> None:
        arm = decision_metrics(5.0, 15.0, ATR_2_5)
        levels = derive_control_levels(arm, None if control_pct is None else _atr(control_pct))
        assert (levels is not None) == placeable
        if control_pct == "2.5":
            assert levels is not None and (levels.stop_pct, levels.target_pct) == (
                Decimal("5.0000"),
                Decimal("15.0000"),
            )

    def test_pack_measurements_read_the_latest_bar_close(self) -> None:
        names = [
            {"instrument_id": 11, "indicators": {"atr14": 2.5}, "bars": [{"c": "90"}, {"c": "100"}]},
            {"instrument_id": 22, "indicators": {"atr14": None}, "bars": [{"c": "100"}]},
            {"instrument_id": 33, "indicators": {"atr14": 1.0}, "bars": []},
        ]
        assert pack_atr_measurements(names) == {11: ATR_2_5, 22: None, 33: None}


class TestControlDraw:
    def test_pool_excludes_control_holdings_prior_draws_and_unplaceable_not_arm_picks(self) -> None:
        pool = control_pool(
            [33, 11, 22, 44, 55],
            control_held_instrument_ids=frozenset({44}),
            drawn_this_run=frozenset({22}),
            placeable_instrument_ids=frozenset({11, 22, 33, 44}),
        )
        assert pool == (11, 33)

    def test_draw_is_the_documented_hash(self) -> None:
        pool = (11, 22, 33, 44, 55)
        draw = draw_control(declaration_sha256_hex=DECL, session_date=date(2026, 10, 1), pair_seq=7, pool=pool)
        material = f"{DECL}|2026-10-01|7"
        expected = int.from_bytes(hashlib.sha256(material.encode()).digest(), "big") % len(pool)
        assert draw.seed_material == material
        assert draw.index == expected
        assert draw.instrument_id == pool[expected]

    def test_draw_varies_with_pair_seq(self) -> None:
        pool = tuple(range(1, 51))
        picks = {
            draw_control(declaration_sha256_hex=DECL, session_date=date(2026, 10, 1), pair_seq=k, pool=pool).index
            for k in range(40)
        }
        assert len(picks) > 20

    def test_empty_pool_refuses(self) -> None:
        with pytest.raises(ControlPoolExhausted):
            draw_control(declaration_sha256_hex=DECL, session_date=date(2026, 10, 1), pair_seq=0, pool=())

    @pytest.mark.parametrize("bad", ["A" * 64, "a" * 63, "g" * 64])
    def test_declaration_digest_shape(self, bad: str) -> None:
        with pytest.raises(ValueError):
            draw_control(declaration_sha256_hex=bad, session_date=date(2026, 10, 1), pair_seq=0, pool=(1,))

    def test_negative_pair_seq(self) -> None:
        with pytest.raises(ValueError):
            draw_control(declaration_sha256_hex=DECL, session_date=date(2026, 10, 1), pair_seq=-1, pool=(1,))
