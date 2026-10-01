"""#3515 slice 2b — fund-v1's budget fixture (spec §6) and the freeze gate on its call; the model call is faked."""

from __future__ import annotations

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
    reported_input_tokens,
)
from app.services.ai_trial_invocation import InvocationResult
from app.services.ai_trial_pack import DISCLOSURE_LIMIT, SMALL_CAP_N, TITLE_MAX_CHARS, TOP_N
from app.services.ai_trial_pack_reader import INTRADAY_BAR_LENGTH, INTRADAY_WINDOW, Pack
from scripts.ai_trial_fund_budget_fixture import budget_fixture_pack, run_fixture

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
            assert len(report["facts"]) == FACTS_PER_REPORT_MAX
            assert report["truncated"] is True
            assert {f["unit"] for f in report["facts"]} == {"USD/shares"}
            assert {len(f["val"]) for f in report["facts"]} == {23}
        assert [r["kind"] for r in block["newer_report_without_facts"]] == ["annual", "quarterly"]
        mdna = n["mdna"]
        assert mdna["truncated"] is True
        assert MDNA_MAX_CHARS - 200 < len(mdna["text"]) <= MDNA_MAX_CHARS
        assert mdna["text"].startswith("</pack>")  # the adversarial extract rides every call


@pytest.mark.parametrize(
    ("usage", "expected"),
    [
        ({"input_tokens": 2, "cache_creation_input_tokens": 14_048, "cache_read_input_tokens": 7}, 14_057),
        ({"input_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}, 0),
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


def _fake_invoke(refusal: str | None, usage: dict[str, int], calls: list[dict[str, Any]]):
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


def test_a_rejected_call_is_a_missing_measurement_never_a_pass() -> None:
    # Measured 2026-10-01: an over-context call exits 1 with "Prompt is too long" and ZERO usage.
    calls: list[dict[str, Any]] = []
    summary, result = run_fixture(
        executable="/bin/claude", source_env=_ENV, invoke=_fake_invoke("nonzero_exit", _ZERO, calls)
    )
    assert result is not None and not result.ok
    assert summary["input_tokens"] is None
    assert summary["freeze_refusal"] == REFUSE_PROMPT_BUDGET_EXCEEDED
    assert calls[0]["system_prompt"] == FUND_SYSTEM_PROMPT


def test_a_call_within_the_ceiling_passes(fixture: Pack) -> None:
    usage = {"input_tokens": 2, "cache_creation_input_tokens": 600_000, "cache_read_input_tokens": 0}
    calls: list[dict[str, Any]] = []
    summary, _ = run_fixture(executable="/bin/claude", source_env=_ENV, invoke=_fake_invoke(None, usage, calls))
    assert (summary["input_tokens"], summary["freeze_refusal"]) == (600_002, None)
    assert summary["pack_sha256"] == fixture.sha256
    assert summary["rendered_prompt_bytes"] == len(calls[0]["prompt"].encode("utf-8"))


def test_a_dry_run_makes_no_call() -> None:
    calls: list[dict[str, Any]] = []
    summary, result = run_fixture(executable="", source_env=_ENV, invoke=_fake_invoke(None, _ZERO, calls), dry_run=True)
    assert result is None and calls == []
    assert summary["rendered_prompt_bytes"] > 0
