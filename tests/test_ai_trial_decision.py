"""#3471 slice 1a — pure tests for the AI-trial decision primitives (spec §6 measurement, §7 draw, O3/O5).

The v6 schema, guard and control pre-filter are ``test_ai_trial_guard.py``.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from fractions import Fraction

import pytest

from app.services.ai_trial_decision import (
    ControlPoolExhausted,
    StrictJSONError,
    count_sentences,
    draw_control,
    exact,
    measure_atr,
    pack_atr_measurements,
    quantize,
    strict_json_loads,
)

DECL = "a" * 64
ATR_2_5 = measure_atr(2.5, 100.0)


class TestStrictJson:
    def test_duplicate_key_refused(self) -> None:
        with pytest.raises(StrictJSONError):
            strict_json_loads('{"a": 1, "a": 2}')

    @pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
    def test_non_finite_constant_refused(self, constant: str) -> None:
        with pytest.raises(StrictJSONError):
            strict_json_loads(f'{{"stop_pct": {constant}}}')

    def test_decoder_limits_are_strict_json_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(StrictJSONError):
            strict_json_loads('{"n": ' + "1" * 5000 + "}")

        # Whether real deep nesting raises depends on the C stack (3.14 measures it): 100,000
        # levels raise on macOS and parse on a Linux runner. Raise it directly instead.
        def _too_deep(*_args: object, **_kwargs: object) -> object:
            raise RecursionError("maximum recursion depth exceeded")

        monkeypatch.setattr(json, "loads", _too_deep)
        with pytest.raises(StrictJSONError, match="RecursionError"):
            strict_json_loads("[[]]")

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


class TestQuantizedArithmetic:
    """§6: exact rationals, 4 decimals half up."""

    def test_quantize_is_half_up_and_exact(self) -> None:
        assert quantize(Fraction(12345, 100000)) == Decimal("0.1235")  # exactly half: up
        assert quantize(Fraction(1, 3)) == Decimal("0.3333")
        # A float would give 0.1 + 0.2 = 0.30000000000000004; the stored reprs are exact.
        assert quantize(exact(0.3) / exact(0.1)) == Decimal("3.0000")

    def test_pack_measurements_read_the_latest_bar_close(self) -> None:
        names = [
            {"instrument_id": 11, "indicators": {"atr14": 2.5}, "bars": [{"c": "90"}, {"c": "100"}]},
            {"instrument_id": 22, "indicators": {"atr14": None}, "bars": [{"c": "100"}]},
            {"instrument_id": 33, "indicators": {"atr14": 1.0}, "bars": []},
        ]
        assert pack_atr_measurements(names) == {11: ATR_2_5, 22: None, 33: None}


class TestControlDraw:
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
