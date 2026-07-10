"""Pure-logic tests for the thesis LLM eval harness (#1919 PR-C).

No DB, no network: a fake ``LLMClient`` drives the retry/classification
path; aggregation is table-tested on synthetic rounds.
"""

from __future__ import annotations

import json

from app.services.llm_client import LLMCompletion
from scripts.llm_eval_thesis import (
    AttemptResult,
    RoundResult,
    aggregate,
    classify_attempt,
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
