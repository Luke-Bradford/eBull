"""Pure-logic tests for the thesis LLM eval harness (#1919 PR-C, judge #1995).

No DB, no network: a fake ``LLMClient`` drives the retry/classification
path; aggregation + the judge stage's pairing/adjudication are
table-tested on synthetic rounds.
"""

from __future__ import annotations

import json

import pytest

from app.services.llm_client import LLMCompletion
from scripts.llm_eval_thesis import (
    JUDGE_DIMENSIONS,
    AttemptResult,
    RoundResult,
    _judge_call,
    _validate_judge_output,
    adjudicate_pair,
    aggregate,
    aggregate_judgements,
    classify_attempt,
    pair_writer_outputs,
    run_round,
)

_VALID_WRITER = {
    "thesis_type": "compounder",
    "confidence_score": 0.7,
    "stance": "buy",
    "buy_zone_low": 100.0,
    "buy_zone_high": 120.0,
    "base_value": 150.0,
    "bull_value": 200.0,
    "bear_value": 90.0,
    "break_conditions": ["margin collapse"],
    "memo_markdown": "## Memo\nthree paragraphs...",
}

_VALID_CRITIC = {
    "summary": "Overpriced compounder.",
    "key_risks": ["multiple compression"],
    "hidden_assumptions": ["margins hold"],
    "evidence_gaps": ["no channel checks"],
    "thesis_breakers": ["guidance cut"],
    "verdict": "Moderate challenge",
}


def _completion(text: str, finish_reason: str = "stop", completion_tokens: int | None = 100) -> LLMCompletion:
    return LLMCompletion(
        text=text,
        finish_reason=finish_reason,
        model="test-model",
        prompt_tokens=1000,
        completion_tokens=completion_tokens,
    )


class FakeClient:
    """Scripted LLMClient: pops one canned completion (or exception) per call."""

    provider_name = "openai_compatible"
    model = "test-model"

    def __init__(self, script: list[LLMCompletion | Exception]) -> None:
        self._script = list(script)
        self.calls = 0

    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion:
        self.calls += 1
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


# ---------------------------------------------------------------------------
# classify_attempt
# ---------------------------------------------------------------------------


def test_classify_valid_writer_passes() -> None:
    result, parsed = classify_attempt(_completion(json.dumps(_VALID_WRITER)), 10.0, call="writer")
    assert result.ok
    assert result.category == "pass"
    assert result.enum_ok is True
    assert parsed == _VALID_WRITER
    assert result.tok_s == 10.0  # 100 tokens / 10s


def test_classify_unparseable_json() -> None:
    result, parsed = classify_attempt(_completion("not json {", finish_reason="length"), 5.0, call="writer")
    assert not result.ok
    assert result.category == "invalid_json"
    assert result.enum_ok is None  # never parsed -> excluded from enum metric
    assert result.finish_reason == "length"
    assert parsed is None


def test_classify_json_array_is_invalid_json_category() -> None:
    result, parsed = classify_attempt(_completion("[1, 2]"), 5.0, call="writer")
    assert result.category == "invalid_json"
    assert parsed is None


def test_classify_bad_enum_is_schema_fail_with_enum_flag() -> None:
    bad = dict(_VALID_WRITER, thesis_type="moonshot")
    result, parsed = classify_attempt(_completion(json.dumps(bad)), 5.0, call="writer")
    assert not result.ok
    assert result.category == "schema_fail"
    assert result.enum_ok is False
    assert parsed is None


def test_classify_missing_field_valid_enums() -> None:
    incomplete = {k: v for k, v in _VALID_WRITER.items() if k != "memo_markdown"}
    result, _ = classify_attempt(_completion(json.dumps(incomplete)), 5.0, call="writer")
    assert result.category == "schema_fail"
    # enums themselves were fine; the spec's enum-validity metric must see that
    assert result.enum_ok is True


def test_classify_critic_verdict_enum() -> None:
    result, _ = classify_attempt(_completion(json.dumps(_VALID_CRITIC)), 5.0, call="critic")
    assert result.ok and result.enum_ok is True
    bad = dict(_VALID_CRITIC, verdict="Devastating challenge")
    result, _ = classify_attempt(_completion(json.dumps(bad)), 5.0, call="critic")
    assert result.category == "schema_fail" and result.enum_ok is False


def test_tok_s_none_without_usage() -> None:
    result, _ = classify_attempt(_completion(json.dumps(_VALID_WRITER), completion_tokens=None), 5.0, call="writer")
    assert result.tok_s is None


# ---------------------------------------------------------------------------
# run_round (retry-once shape)
# ---------------------------------------------------------------------------


def test_round_first_attempt_pass_stops() -> None:
    client = FakeClient([_completion(json.dumps(_VALID_WRITER))])
    rnd = run_round(client, symbol="AAPL", call="writer", system="s", user="u", max_tokens=64)
    assert client.calls == 1
    assert rnd.pass_first and rnd.pass_with_retry
    assert rnd.parsed == _VALID_WRITER


def test_round_retry_recovers() -> None:
    client = FakeClient([_completion("garbage"), _completion(json.dumps(_VALID_WRITER))])
    rnd = run_round(client, symbol="AAPL", call="writer", system="s", user="u", max_tokens=64)
    assert client.calls == 2
    assert not rnd.pass_first
    assert rnd.pass_with_retry
    assert rnd.parsed == _VALID_WRITER


def test_round_both_attempts_fail() -> None:
    client = FakeClient([_completion("garbage"), _completion("{}")])
    rnd = run_round(client, symbol="AAPL", call="writer", system="s", user="u", max_tokens=64)
    assert client.calls == 2
    assert not rnd.pass_with_retry
    assert rnd.parsed is None
    assert [a.category for a in rnd.attempts] == ["invalid_json", "schema_fail"]


def test_round_transport_error_ends_round_without_retry() -> None:
    # Prod _call_with_one_retry retries only ValueError (parse/schema);
    # transport errors propagate with NO retry — the harness must not
    # credit a recovery production would never perform.
    client = FakeClient([RuntimeError("connection refused"), _completion(json.dumps(_VALID_WRITER))])
    rnd = run_round(client, symbol="AAPL", call="writer", system="s", user="u", max_tokens=64)
    assert client.calls == 1
    assert len(rnd.attempts) == 1
    assert rnd.attempts[0].category == "transport"
    assert "connection refused" in (rnd.attempts[0].error or "")
    assert not rnd.pass_with_retry
    assert rnd.parsed is None


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------


def _attempt(
    ok: bool, *, category: str = "pass", enum_ok: bool | None = True, finish: str | None = "stop"
) -> AttemptResult:
    return AttemptResult(
        ok=ok,
        category=category if not ok or category != "pass" else "pass",
        enum_ok=enum_ok,
        finish_reason=finish,
        duration_s=10.0,
        completion_tokens=50,
    )


def test_aggregate_gate_math() -> None:
    # 10 writer rounds: 8 pass first, 1 recovers on retry, 1 dead -> 9/10 with retry = gate PASS
    rounds = (
        [RoundResult("AAPL", "writer", [_attempt(True)])] * 8
        + [RoundResult("GME", "writer", [_attempt(False, category="invalid_json", enum_ok=None), _attempt(True)])]
        + [
            RoundResult(
                "HD",
                "writer",
                [
                    _attempt(False, category="schema_fail", enum_ok=False, finish="length"),
                    _attempt(False, category="schema_fail", enum_ok=False, finish="length"),
                ],
            )
        ]
    )
    report = aggregate("m", rounds)
    assert report.writer_rounds == 10
    assert report.writer_pass_first == 8
    assert report.writer_pass_retry == 9
    assert report.gate_passes()
    # 8 first-pass stops + round-9's two attempts (stop, stop) + round-10's two lengths
    assert report.finish_reasons == {"stop": 10, "length": 2}
    # enum metric: 9 passes (True) + 2 schema_fails (False); invalid_json excluded
    assert report.enum_checked == 11
    assert report.enum_ok == 9


def test_aggregate_gate_fails_below_nine_of_ten() -> None:
    rounds = [RoundResult("AAPL", "writer", [_attempt(True)])] * 8 + [
        RoundResult("GME", "writer", [_attempt(False, category="schema_fail")]) for _ in range(2)
    ]
    report = aggregate("m", rounds)
    assert report.writer_pass_retry == 8
    assert not report.gate_passes()


def test_aggregate_separates_writer_and_critic() -> None:
    rounds = [
        RoundResult("AAPL", "writer", [_attempt(True)]),
        RoundResult("AAPL", "critic", [_attempt(False, category="schema_fail"), _attempt(True)]),
    ]
    report = aggregate("m", rounds)
    assert (report.writer_rounds, report.critic_rounds) == (1, 1)
    assert report.critic_pass_first == 0
    assert report.critic_pass_retry == 1
    # writer pass rate unaffected by critic failures
    assert report.writer_pass_retry_rate == 1.0


def test_aggregate_empty_rounds_no_gate() -> None:
    report = aggregate("m", [])
    assert report.writer_rounds == 0
    assert not report.gate_passes()


def test_gate_requires_minimum_sample() -> None:
    # 5/5 (100%) must NOT pass the gate — spec §7 is >=9/10, so fewer than
    # 10 writer rounds is an insufficient sample regardless of rate.
    rounds = [RoundResult("AAPL", "writer", [_attempt(True)])] * 5
    report = aggregate("m", rounds)
    assert report.writer_pass_retry_rate == 1.0
    assert not report.gate_passes()


def test_gate_fails_on_critic_length_failure() -> None:
    # #1987: a truncated critic stores a thesis WITHOUT critic_json in
    # production — the gate must fail on ANY critic finish_reason == "length",
    # even with a perfect writer score.
    rounds = [RoundResult("AAPL", "writer", [_attempt(True)])] * 10 + [
        RoundResult("IEP", "critic", [_attempt(False, category="schema_fail", finish="length")])
    ]
    report = aggregate("m", rounds)
    assert report.writer_pass_retry_rate == 1.0
    assert report.critic_length_failures == 1
    assert not report.gate_passes()


def test_writer_length_does_not_trip_critic_gate() -> None:
    # Writer truncations are covered by the writer pass-rate gate; the
    # critic-length counter must count CRITIC attempts only.
    rounds = (
        [RoundResult("AAPL", "writer", [_attempt(True)])] * 9
        + [RoundResult("GME", "writer", [_attempt(False, category="schema_fail", finish="length"), _attempt(True)])]
        + [RoundResult("AAPL", "critic", [_attempt(True)])]
    )
    report = aggregate("m", rounds)
    assert report.critic_length_failures == 0
    assert report.gate_passes()


# ---------------------------------------------------------------------------
# judge stage (#1995) — pure functions
# ---------------------------------------------------------------------------


def _judge_output(
    winner: str = "A",
    score: int = 4,
    rationale: str = "A grounded every figure; B fabricated FY25 EBITDA.",
) -> dict[str, object]:
    scores = {dim: score for dim in JUDGE_DIMENSIONS}
    return {"scores_a": dict(scores), "scores_b": dict(scores), "winner": winner, "rationale": rationale}


class TestValidateJudgeOutput:
    def test_valid_output_passes(self) -> None:
        _validate_judge_output(_judge_output())

    def test_missing_field_raises(self) -> None:
        bad = _judge_output()
        del bad["winner"]
        with pytest.raises(ValueError, match="missing fields"):
            _validate_judge_output(bad)

    def test_invalid_winner_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid winner"):
            _validate_judge_output(_judge_output(winner="C"))

    def test_out_of_range_score_raises(self) -> None:
        bad = _judge_output()
        bad["scores_a"]["numeric_grounding"] = 6  # type: ignore[index]
        with pytest.raises(ValueError, match="numeric_grounding"):
            _validate_judge_output(bad)

    def test_bool_score_rejected(self) -> None:
        # bool is an int subclass — must not sneak through the range check.
        bad = _judge_output()
        bad["scores_b"]["specificity"] = True  # type: ignore[index]
        with pytest.raises(ValueError, match="specificity"):
            _validate_judge_output(bad)

    def test_empty_rationale_raises(self) -> None:
        with pytest.raises(ValueError, match="rationale"):
            _validate_judge_output(_judge_output(rationale="  "))


def _results_payload() -> dict[str, object]:
    """Minimal run --json-out shape: two models, two symbols x two iterations,
    with one unpaired round (model-b writer failed on GME it2)."""

    def _writer_round(symbol: str, iteration: int, parsed: dict | None) -> dict[str, object]:
        return {"symbol": symbol, "call": "writer", "iteration": iteration, "parsed": parsed, "attempts": []}

    memo = {"memo_markdown": "## memo"}
    return {
        "model-a": {
            "rounds": [
                _writer_round("AAPL", 1, memo),
                _writer_round("AAPL", 2, memo),
                _writer_round("GME", 1, memo),
                _writer_round("GME", 2, memo),
                # critic rounds must be ignored by the pairing.
                {"symbol": "AAPL", "call": "critic", "iteration": 1, "parsed": {"verdict": "x"}, "attempts": []},
            ]
        },
        "model-b": {
            "rounds": [
                _writer_round("AAPL", 1, memo),
                _writer_round("AAPL", 2, memo),
                _writer_round("GME", 1, memo),
                _writer_round("GME", 2, None),  # writer failed — no memo to judge
            ]
        },
    }


class TestPairWriterOutputs:
    def test_pairs_by_symbol_and_iteration(self) -> None:
        pairs = pair_writer_outputs(_results_payload(), "model-a", "model-b")
        keys = [(p["symbol"], p["iteration"]) for p in pairs]
        assert keys == [("AAPL", 1), ("AAPL", 2), ("GME", 1)]  # GME it2 dropped (b failed)

    def test_missing_model_raises(self) -> None:
        with pytest.raises(SystemExit, match="not present"):
            pair_writer_outputs(_results_payload(), "model-a", "model-nope")


class TestAdjudicatePair:
    def test_agreement_yields_model_winner(self) -> None:
        # Pass 1 saw (a, b) and picked A → model a. Pass 2 saw (b, a) and
        # picked B → also model a. Agreement.
        verdict = adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)
        assert verdict.winner == "a"
        assert verdict.agreed is True
        assert verdict.error is None

    def test_positional_disagreement_is_tie(self) -> None:
        # Both passes picked positional A = whichever memo came FIRST →
        # pure position bias → tie, not a win.
        verdict = adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="A"), symbol="AAPL", iteration=1)
        assert verdict.winner == "tie"
        assert verdict.agreed is False

    def test_scores_average_across_orderings(self) -> None:
        first = _judge_output(winner="tie")
        swapped = _judge_output(winner="tie")
        first["scores_a"] = {dim: 5 for dim in JUDGE_DIMENSIONS}  # a graded as A in pass 1
        first["scores_b"] = {dim: 1 for dim in JUDGE_DIMENSIONS}
        swapped["scores_a"] = {dim: 1 for dim in JUDGE_DIMENSIONS}  # b graded as A in pass 2
        swapped["scores_b"] = {dim: 3 for dim in JUDGE_DIMENSIONS}  # a graded as B in pass 2
        verdict = adjudicate_pair(first, swapped, symbol="GME", iteration=1)
        assert verdict.scores_a == {dim: 4.0 for dim in JUDGE_DIMENSIONS}  # (5 + 3) / 2
        assert verdict.scores_b == {dim: 1.0 for dim in JUDGE_DIMENSIONS}  # (1 + 1) / 2

    def test_failed_call_yields_error_verdict(self) -> None:
        verdict = adjudicate_pair(None, _judge_output(), symbol="HD", iteration=1, error="ctx_overflow: ...")
        assert verdict.winner == "tie"
        assert verdict.scores_a is None
        assert verdict.error == "ctx_overflow: ..."


class TestAggregateJudgements:
    def test_counts_and_means(self) -> None:
        v_win_a = adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)
        v_tie = adjudicate_pair(_judge_output(winner="tie"), _judge_output(winner="tie"), symbol="GME", iteration=1)
        v_failed = adjudicate_pair(None, None, symbol="HD", iteration=1, error="boom")
        report = aggregate_judgements([v_win_a, v_tie, v_failed])
        assert report["pairs"] == 3
        assert report["judged"] == 2
        assert report["failed"] == 1
        assert report["wins_a"] == 1
        assert report["wins_b"] == 0
        assert report["ties"] == 1  # real judge ties only — failed pair excluded
        assert report["order_agreement_rate"] == 1.0
        assert report["mean_scores_a"]["numeric_grounding"] == 4.0

    def test_empty_verdicts(self) -> None:
        report = aggregate_judgements([])
        assert report["judged"] == 0
        assert report["order_agreement_rate"] is None
        assert report["mean_scores_a"] == {}


class TestJudgeCall:
    def test_ctx_overflow_fails_never_grades(self) -> None:
        # prompt_tokens + judge max_tokens over the limit → the server
        # silently truncated the prompt; grading it would be dishonest.
        big = LLMCompletion(
            text=json.dumps(_judge_output()),
            finish_reason="stop",
            model="j",
            prompt_tokens=16000,
            completion_tokens=200,
        )
        parsed, error = _judge_call(
            FakeClient([big]),  # type: ignore[arg-type]
            context_prompt="ctx",
            memo_first="a",
            memo_second="b",
            ctx_limit=16384,
        )
        assert parsed is None
        assert error is not None and error.startswith("ctx_overflow")

    def test_retry_recovers_bad_json(self) -> None:
        client = FakeClient([_completion("not json {"), _completion(json.dumps(_judge_output()))])
        parsed, error = _judge_call(
            client,  # type: ignore[arg-type]
            context_prompt="ctx",
            memo_first="a",
            memo_second="b",
            ctx_limit=16384,
        )
        assert error is None
        assert parsed is not None and parsed["winner"] == "A"
        assert client.calls == 2

    def test_transport_error_no_retry(self) -> None:
        client = FakeClient([RuntimeError("connection refused")])
        parsed, error = _judge_call(
            client,  # type: ignore[arg-type]
            context_prompt="ctx",
            memo_first="a",
            memo_second="b",
            ctx_limit=16384,
        )
        assert parsed is None
        assert error is not None and error.startswith("transport")
        assert client.calls == 1


# ---------------------------------------------------------------------------
# #2067 — judge-gate hardening
# ---------------------------------------------------------------------------

from scripts.llm_eval_thesis import (  # noqa: E402
    JUDGE_AGREEMENT_FLOOR,
    combine_panel,
    model_family,
)


class TestModelFamily:
    def test_strips_size_tag(self) -> None:
        assert model_family("qwen3:14b") == "qwen3"
        assert model_family("deepseek-r1:8b") == "deepseek-r1"

    def test_no_tag_is_identity(self) -> None:
        assert model_family("phi4") == "phi4"


class TestAgreementFloor:
    def test_low_agreement_invalidates_round(self) -> None:
        # 1 agreed + 2 positional disagreements = 33% agreement < 60%.
        agreed = adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)
        d1 = adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="A"), symbol="GME", iteration=1)
        d2 = adjudicate_pair(_judge_output(winner="B"), _judge_output(winner="B"), symbol="HD", iteration=1)
        report = aggregate_judgements([agreed, d1, d2])
        assert report["order_agreement_rate"] == pytest.approx(1 / 3)
        assert report["valid"] is False
        assert report["invalid_reason"] is not None and "below floor" in report["invalid_reason"]
        assert report["agreement_floor"] == JUDGE_AGREEMENT_FLOOR

    def test_high_agreement_is_valid(self) -> None:
        verdicts = [
            adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol=s, iteration=1)
            for s in ("AAPL", "GME", "HD")
        ]
        report = aggregate_judgements(verdicts)
        assert report["valid"] is True
        assert report["invalid_reason"] is None

    def test_nothing_judged_is_invalid(self) -> None:
        failed = adjudicate_pair(None, None, symbol="AAPL", iteration=1, error="boom")
        report = aggregate_judgements([failed])
        assert report["valid"] is False
        assert report["invalid_reason"] == "no pairs judged"

    def test_floor_is_configurable(self) -> None:
        agreed = adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)
        d1 = adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="A"), symbol="GME", iteration=1)
        report = aggregate_judgements([agreed, d1], agreement_floor=0.5)
        assert report["valid"] is True


class TestJudgeClip:
    def test_length_finish_classified_and_counted(self) -> None:
        client = FakeClient(
            [
                _completion("truncated {", finish_reason="length"),
                _completion("still truncated {", finish_reason="length"),
            ]
        )
        parsed, error = _judge_call(
            client,  # type: ignore[arg-type]
            context_prompt="ctx",
            memo_first="a",
            memo_second="b",
            ctx_limit=16384,
        )
        assert parsed is None
        assert error is not None and error.startswith("judge_clip")
        assert client.calls == 2  # one retry, then surfaced

        verdict = adjudicate_pair(None, None, symbol="JPM", iteration=1, error=error)
        report = aggregate_judgements([verdict])
        assert report["clipped"] == 1
        assert report["failed"] == 1

    def test_clip_retry_can_recover(self) -> None:
        client = FakeClient(
            [
                _completion("truncated {", finish_reason="length"),
                _completion(json.dumps(_judge_output())),
            ]
        )
        parsed, error = _judge_call(
            client,  # type: ignore[arg-type]
            context_prompt="ctx",
            memo_first="a",
            memo_second="b",
            ctx_limit=16384,
        )
        assert error is None
        assert parsed is not None


class TestCombinePanel:
    def test_unanimous_non_tie_wins(self) -> None:
        j1 = [adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)]
        j2 = [adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)]
        panel = combine_panel({"judge1": j1, "judge2": j2})
        assert panel["wins_a"] == 1
        assert panel["ties"] == 0
        assert panel["unanimous_rate"] == 1.0

    def test_split_verdict_is_tie(self) -> None:
        j1 = [adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)]
        j2 = [adjudicate_pair(_judge_output(winner="B"), _judge_output(winner="A"), symbol="AAPL", iteration=1)]
        panel = combine_panel({"judge1": j1, "judge2": j2})
        assert panel["wins_a"] == 0
        assert panel["wins_b"] == 0
        assert panel["ties"] == 1

    def test_one_judge_tie_blocks_flip(self) -> None:
        j1 = [adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)]
        j2 = [adjudicate_pair(_judge_output(winner="tie"), _judge_output(winner="tie"), symbol="AAPL", iteration=1)]
        panel = combine_panel({"judge1": j1, "judge2": j2})
        assert panel["wins_a"] == 0
        assert panel["ties"] == 1

    def test_errored_pair_never_counts_as_win(self) -> None:
        j1 = [adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)]
        j2 = [adjudicate_pair(None, None, symbol="AAPL", iteration=1, error="judge_clip: ...")]
        panel = combine_panel({"judge1": j1, "judge2": j2})
        assert panel["wins_a"] == 0
        assert panel["ties"] == 1

    def test_missing_judge_coverage_is_tie(self) -> None:
        # judge2 never judged AAPL it2 (e.g. fixture missing) — a pair
        # covered by only one judge must not count as unanimous.
        j1 = [
            adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1),
            adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=2),
        ]
        j2 = [adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1)]
        panel = combine_panel({"judge1": j1, "judge2": j2})
        assert panel["wins_a"] == 1
        assert panel["ties"] == 1

    def test_single_judge_degenerates(self) -> None:
        j1 = [
            adjudicate_pair(_judge_output(winner="A"), _judge_output(winner="B"), symbol="AAPL", iteration=1),
            adjudicate_pair(_judge_output(winner="tie"), _judge_output(winner="tie"), symbol="GME", iteration=1),
        ]
        panel = combine_panel({"judge1": j1})
        assert panel["wins_a"] == 1
        assert panel["ties"] == 1
