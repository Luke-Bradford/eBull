"""#3471 slice 1a — pure tests for the AI-trial decision layer (spec §5-§7, O3/O5)."""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Any

import pytest

from app.services.ai_trial_decision import (
    MAX_ENTRIES_PER_RUN,
    ControlPoolExhausted,
    StrictJSONError,
    control_pool,
    count_sentences,
    decision_json_schema,
    draw_control,
    strict_json_loads,
    validate_response,
)

SHORTLIST = {"AAA": 11, "BBB": 22, "CCC": 33}
DECL = "a" * 64


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


def _validate(output: object, *, held: frozenset[int] = frozenset(), max_new: int = 2):
    return validate_response(output, shortlist=SHORTLIST, arm_held_instrument_ids=held, max_new_entries=max_new)


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

    @pytest.mark.parametrize(("stop", "target"), [(10.0, 10.0), (20.0, 12.0)])
    def test_target_not_above_stop(self, stop: float, target: float) -> None:
        [v] = _validate({"decisions": [_decision(stop_pct=stop, target_pct=target)], "no_trade_reason": None}).verdicts
        assert v.reason_code == "target_not_above_stop"

    def test_thesis_too_long(self) -> None:
        [v] = _validate({"decisions": [_decision(thesis="A. B. C. D.")], "no_trade_reason": None}).verdicts
        assert v.reason_code == "thesis_too_long"

    def test_precedence_first_failure_names_reason(self) -> None:
        # Held AND target<=stop AND long thesis: `already_held` wins (§6 order 3 before 4, 5).
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


class TestControlDraw:
    def test_pool_excludes_control_holdings_and_prior_draws_not_arm_picks(self) -> None:
        pool = control_pool(
            [33, 11, 22, 44], control_held_instrument_ids=frozenset({44}), drawn_this_run=frozenset({22})
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
