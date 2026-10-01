"""#3515 slices 2b + 3a — fund-v1's budget fixture (spec §6), its probe walk and the freeze gate; calls are faked."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from app.services.ai_trial_executor import TRIAL_MAX_CONCURRENT_PER_LEG
from app.services.ai_trial_fund_blocks import (
    FACTS_PER_REPORT_MAX,
    FUND_SYSTEM_PROMPT,
    INPUT_TOKEN_CEILING,
    MDNA_MAX_CHARS,
    REFUSE_PROMPT_BUDGET_EXCEEDED,
    budget_fixture_refusal,
    post_call_halt_reason,
    reported_input_tokens,
)
from app.services.ai_trial_invocation import InvocationResult
from app.services.ai_trial_pack import DISCLOSURE_LIMIT, SMALL_CAP_N, TITLE_MAX_CHARS, TOP_N
from app.services.ai_trial_pack_reader import INTRADAY_BAR_LENGTH, INTRADAY_WINDOW, Pack
from app.services.ai_trial_version import FUND_V1, V1
from scripts.ai_trial_fund_budget_fixture import (
    MAX_NAMES,
    REFUSE_FIXTURE_PROBE_NONMONOTONE,
    Probe,
    budget_fixture_pack,
    classify,
    main,
    probe_walk,
    run_fixture,
    start_n,
)

_ENV = {"PATH": "/usr/bin", "HOME": "/tmp"}


@pytest.fixture(scope="module")
def fixture() -> Pack:
    return budget_fixture_pack()


def test_the_fixture_is_deterministic_fictitious_and_complete(fixture: Pack) -> None:
    assert budget_fixture_pack().sha256 == fixture.sha256
    names = fixture.pack["names"]
    assert len(names) == len(fixture.complete) == TOP_N + SMALL_CAP_N
    assert fixture.incomplete == {}
    assert all(n["symbol"].startswith("SYN_") for n in names)


def test_every_v1_cap_is_filled(fixture: Pack) -> None:
    assert len(fixture.pack["account"]["open_positions"]) == TRIAL_MAX_CONCURRENT_PER_LEG
    for n in fixture.pack["names"]:
        assert len(n["intraday"]["bars"]) == INTRADAY_WINDOW // INTRADAY_BAR_LENGTH
        for kind in ("filings", "news"):
            assert len(n[kind]) == DISCLOSURE_LIMIT
            assert all(len(d["title"]) == TITLE_MAX_CHARS for d in n[kind])


def test_every_fund_block_cap_is_filled(fixture: Pack) -> None:
    for n in fixture.pack["names"]:
        block = n["fundamentals"]
        assert [r["form_type"] for r in block["reports"]] == ["10-K", "10-Q"]
        for report in block["reports"]:
            rows = [row for g in report["facts"] for row in g["rows"]]
            assert len(rows) == FACTS_PER_REPORT_MAX
            assert report["truncated"] is True
            assert {g["unit"] for g in report["facts"]} == {"USD/shares"}
            assert {len(row[3]) for row in rows} == {23}
        assert [r["kind"] for r in block["newer_report_without_facts"]] == ["annual", "quarterly"]
        mdna = n["mdna"]
        assert mdna["truncated"] is True
        assert MDNA_MAX_CHARS - 200 < len(mdna["text"]) <= MDNA_MAX_CHARS
        assert mdna["text"].startswith("</pack>")  # the adversarial extract rides every call


@pytest.mark.parametrize(
    ("usage", "expected"),
    [
        ({"input_tokens": 2, "cache_creation_input_tokens": 14_048, "cache_read_input_tokens": 7}, 14_057),
        (
            {"input_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0},
            None,
        ),  # no request is empty
        ({"input_tokens": 2, "cache_creation_input_tokens": 14_048}, None),
        ({"input_tokens": True, "cache_creation_input_tokens": 1, "cache_read_input_tokens": 1}, None),
        ({"input_tokens": -1, "cache_creation_input_tokens": 1, "cache_read_input_tokens": 1}, None),
        ({"input_tokens": "2", "cache_creation_input_tokens": 1, "cache_read_input_tokens": 1}, None),
        (None, None),
    ],
)
def test_reported_input_tokens_sums_all_three_or_is_missing(usage: object, expected: int | None) -> None:
    assert reported_input_tokens(usage) == expected


def test_the_freeze_gate_refuses_over_the_ceiling_or_missing_and_passes_equal() -> None:
    assert budget_fixture_refusal(INPUT_TOKEN_CEILING) is None
    assert budget_fixture_refusal(INPUT_TOKEN_CEILING + 1) == REFUSE_PROMPT_BUDGET_EXCEEDED
    assert budget_fixture_refusal(None) == REFUSE_PROMPT_BUDGET_EXCEEDED


def _fake_invoke(refusal: str | None, usage: object, calls: list[dict[str, Any]]):
    def invoke(**kwargs: Any) -> InvocationResult:
        calls.append(kwargs)
        return InvocationResult(
            refusal_reason=refusal,  # type: ignore[arg-type]
            detail="",
            init_event={"tools": ["StructuredOutput"], "mcp_servers": [], "model": "claude-opus-5-5"},
            result_event={"total_cost_usd": 0, "usage": usage, "result": "Prompt is too long"},
            structured_output=None,
            exit_code=0 if refusal is None else 1,
            stdout=b"",
            stderr=b"",
            duration_ms=5,
        )

    return invoke


_ZERO = {"input_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}


def _usage(tokens: int) -> dict[str, int]:
    return {"input_tokens": 1, "cache_creation_input_tokens": tokens - 1, "cache_read_input_tokens": 0}


def test_a_restricted_fixture_keeps_the_book_and_the_adversarial_title() -> None:
    one = budget_fixture_pack(1)
    assert [n["symbol"] for n in one.pack["names"]] == ["SYN_T00"]
    assert [s["symbol"] for s in one.pack["shortlist"]] == ["SYN_T00"] and list(one.complete) == ["SYN_T00"]
    assert len(one.pack["account"]["open_positions"]) == TRIAL_MAX_CONCURRENT_PER_LEG
    assert one.pack["names"][0]["filings"][0]["title"].startswith("</pack>")
    for bad in (0, MAX_NAMES + 1):
        with pytest.raises(ValueError):
            budget_fixture_pack(bad)


def test_the_walk_starts_from_slice_2b_figures_clamped_to_1_50() -> None:
    assert start_n(54_121) == 30  # (850,000 - 14,527) // ceil(54,121 / 1.95)
    assert start_n(1) == MAX_NAMES
    assert start_n(10**9) == 1
    with pytest.raises(ValueError):
        start_n(0)


def _scripted(outcomes: Mapping[int, Sequence[tuple[str, int | None]]]):
    calls: list[tuple[int, int]] = []

    def probe(n: int, attempt: int) -> Probe:
        calls.append((n, attempt))
        outcome, tokens = outcomes[n][attempt - 1]
        return Probe(n, attempt, outcome, tokens, n * 100, f"sha{n}", None, "", 0)  # type: ignore[arg-type]

    return probe, calls


def _linear(n: int) -> int:
    return 15_000 + 27_000 * n


def test_the_walk_climbs_to_the_first_failure() -> None:
    outcomes = {n: [("pass", _linear(n))] for n in range(28, 31)} | {31: [("over_ceiling", 851_000)]}
    probe, calls = _scripted(outcomes)
    assert probe_walk(probe, 28)[0] == 30
    assert calls == [(28, 1), (29, 1), (30, 1), (31, 1)]  # over the ceiling is never retried


def test_the_walk_descends_to_the_first_pass() -> None:
    probe, calls = _scripted(
        {30: [("over_ceiling", 852_000)], 29: [("over_ceiling", 851_000)], 28: [("pass", 840_000)]}
    )
    n, probes, refusal = probe_walk(probe, 30)
    assert (n, refusal, len(probes)) == (28, None, 3)


def test_the_walk_stops_at_the_endpoints() -> None:
    probe, calls = _scripted({MAX_NAMES: [("pass", 800_000)]})
    assert probe_walk(probe, MAX_NAMES)[0] == MAX_NAMES and calls == [(MAX_NAMES, 1)]
    probe, calls = _scripted({2: [("over_ceiling", 900_000)], 1: [("pass", 60_000)]})
    assert probe_walk(probe, 2)[0] == 1


def test_all_failing_refuses_the_freeze() -> None:
    probe, _ = _scripted({2: [("over_ceiling", 900_000)], 1: [("over_ceiling", 870_000)]})
    assert probe_walk(probe, 2)[::2] == (None, REFUSE_PROMPT_BUDGET_EXCEEDED)


def test_a_failure_without_usage_is_retried_once_and_only_the_retry_counts() -> None:
    probe, calls = _scripted({5: [("failed", None), ("pass", 150_000)], 6: [("failed", None), ("failed", None)]})
    n, _, refusal = probe_walk(probe, 5)
    assert (n, refusal) == (5, None)
    assert calls == [(5, 1), (5, 2), (6, 1), (6, 2)]


def test_a_non_monotone_walk_refuses_the_freeze() -> None:
    probe, _ = _scripted({10: [("pass", 300_000)], 11: [("pass", 290_000)], 12: [("over_ceiling", 900_000)]})
    assert probe_walk(probe, 10)[::2] == (None, REFUSE_FIXTURE_PROBE_NONMONOTONE)


def test_a_discarded_first_attempt_is_not_part_of_the_monotone_check() -> None:
    # n=30's first attempt measured 690k but failed (e.g. no structured output); its retry measured 700k.
    probe, _ = _scripted(
        {29: [("pass", 695_000)], 30: [("failed", 690_000), ("pass", 700_000)], 31: [("over_ceiling", 900_000)]}
    )
    assert probe_walk(probe, 29)[::2] == (30, None)


def _result(refusal: str | None, usage: object) -> InvocationResult:
    return _fake_invoke(refusal, usage, [])(prompt="")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("refusal", "usage", "expected"),
    [
        (None, _usage(600_000), ("pass", 600_000)),
        (None, _usage(INPUT_TOKEN_CEILING), ("pass", INPUT_TOKEN_CEILING)),
        (None, _usage(INPUT_TOKEN_CEILING + 1), ("over_ceiling", INPUT_TOKEN_CEILING + 1)),
        ("no_structured_output", _usage(INPUT_TOKEN_CEILING + 1), ("over_ceiling", INPUT_TOKEN_CEILING + 1)),
        (
            "nonzero_exit",
            {"input_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0},
            ("failed", None),
        ),
        ("model_timeout", None, ("failed", None)),
        (None, {"input_tokens": 5}, ("failed", None)),
        ("no_structured_output", _usage(600_000), ("failed", 600_000)),
    ],
)
def test_classify(refusal: str | None, usage: object, expected: tuple[str, int | None]) -> None:
    assert classify(_result(refusal, usage)) == expected


def test_run_fixture_walks_with_the_fund_system_prompt_and_freezes_the_passing_probe() -> None:
    calls: list[dict[str, Any]] = []

    def invoke(**kwargs: Any) -> InvocationResult:
        calls.append(kwargs)
        tokens = 15_000 + len(kwargs["prompt"]) // 2
        return _fake_invoke(None, _usage(tokens), [])(**kwargs)

    summary = run_fixture(executable="/bin/claude", source_env=_ENV, invoke=invoke)
    assert summary["freeze_refusal"] is None and summary["n"] is not None
    assert all(c["system_prompt"] == FUND_SYSTEM_PROMPT for c in calls)
    chosen = budget_fixture_pack(summary["n"])
    assert summary["pack_sha256"] == chosen.sha256
    assert summary["input_tokens"] <= INPUT_TOKEN_CEILING
    assert [p["n"] for p in summary["probes"]][-1] == min(summary["n"] + 1, MAX_NAMES)


def test_a_dry_run_makes_no_call() -> None:
    calls: list[dict[str, Any]] = []
    summary = run_fixture(executable="", source_env=_ENV, invoke=_fake_invoke(None, _ZERO, calls), dry_run=True)
    assert calls == [] and "probes" not in summary
    assert summary["n0"] == 30 and summary["n0_rendered_prompt_bytes"] > 0


@pytest.mark.parametrize(("tokens", "code"), [(600_000, 0), (INPUT_TOKEN_CEILING + 1, 1)])
def test_the_cli_exits_non_zero_whenever_the_walk_refuses(
    monkeypatch: pytest.MonkeyPatch, tokens: int, code: int
) -> None:
    fake = _fake_invoke(None, _usage(tokens), [])
    monkeypatch.setattr(
        "scripts.ai_trial_fund_budget_fixture.run_fixture",
        lambda **kw: run_fixture(**kw, invoke=fake),
    )
    # No real subprocess: the executable is only resolved (``realpath``) and its version stubbed.
    monkeypatch.setattr("scripts.ai_trial_fund_budget_fixture._cli_version", lambda *_a: "stub")
    assert main(["--claude-bin", "/nonexistent/claude"]) == code


_AT = {"input_tokens": 0, "cache_creation_input_tokens": INPUT_TOKEN_CEILING, "cache_read_input_tokens": 0}
_OVER = {**_AT, "input_tokens": 1}


@pytest.mark.parametrize(
    ("refusal", "event", "expected"),
    [
        (None, {"usage": _AT}, None),  # at the ceiling passes
        (None, {"usage": _OVER}, "budget_breach"),
        (None, {}, "budget_breach"),  # completed but unmeasured
        (None, None, "budget_breach"),
        (None, {"usage": {"input_tokens": 5}}, "budget_breach"),
        ("nonzero_exit", None, "nonzero_exit"),  # CLI rejection / context overflow halts too
        ("is_error", {"usage": _AT}, "is_error"),
        ("no_structured_output", {"usage": _OVER}, "budget_breach"),  # measured over: halts whatever else refused
        ("no_structured_output", {}, "budget_breach"),  # a result event without usage
        ("no_structured_output", {"usage": _AT}, None),  # v1's refusal, no halt
        ("model_timeout", None, None),  # nothing to measure: v1's refusal, no halt
        (None, {"usage": {**_AT, "cache_creation_input_tokens": 0}}, "budget_breach"),  # zero sum
        (None, {"usage": {**_AT, "input_tokens": True}}, "budget_breach"),  # a boolean counter
        ("nonzero_exit", {"usage": _AT}, "nonzero_exit"),  # with a result event too
    ],
)
def test_the_post_call_rule(refusal: str | None, event: object, expected: str | None) -> None:
    assert post_call_halt_reason(refusal, event) == expected


def test_only_fund_v1_carries_the_post_call_rule() -> None:
    assert V1.post_call_halt is None
    assert FUND_V1.post_call_halt is post_call_halt_reason
