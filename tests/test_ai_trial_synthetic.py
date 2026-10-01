"""#3471 slice 1b-iv — the ``--synthetic`` CLI over fictitious names, with the model call faked."""

from __future__ import annotations

from typing import Any

import pytest

from app.services.ai_trial_invocation import InvocationResult
from app.services.ai_trial_prompt import SYSTEM_PROMPT
from scripts.ai_trial_synthetic import (
    NotFictitious,
    assert_fictitious,
    main,
    run_synthetic,
    synthetic_pack,
)

_ENV = {"PATH": "/usr/bin", "HOME": "/tmp"}


def _decision(symbol: str, **kw: Any) -> dict[str, Any]:
    return {
        "action": "enter_long",
        "symbol": symbol,
        "setup_type": "pullback_rising_sma20",
        "invalidation_level_id": "sma50",
        "target_level_id": "range20_projection",
        "horizon_days": 5,
        "size_tier": "half",
        "confidence": 3,
        "thesis": "Trend intact. The 8-K beat is not in the base rate.",
        **kw,
    }


def _fake_invoke(structured: object, refusal: str | None = None, calls: list[dict[str, Any]] | None = None):
    def invoke(**kwargs: Any) -> InvocationResult:
        if calls is not None:
            calls.append(kwargs)
        return InvocationResult(
            refusal_reason=refusal,  # type: ignore[arg-type]
            detail="",
            init_event={"tools": ["StructuredOutput"], "mcp_servers": [], "model": "claude-opus-5-5"},
            result_event={"total_cost_usd": 0.01, "num_turns": 2},
            structured_output=structured,
            exit_code=0,
            stdout=b"",
            stderr=b"",
            duration_ms=5,
        )

    return invoke


def test_synthetic_pack_is_deterministic_complete_and_fictitious() -> None:
    first, second = synthetic_pack(), synthetic_pack()
    assert first.sha256 == second.sha256
    assert first.incomplete == {}
    assert set(first.complete) == {n["symbol"] for n in first.pack["names"]}
    assert all(s.startswith("SYN_") for s in first.complete)
    titles = [f["title"] for n in first.pack["names"] for f in n["filings"]]
    assert any("</pack>" in t for t in titles)  # the injection fixture is present


def test_assert_fictitious_refuses_a_real_symbol() -> None:
    pack = synthetic_pack().pack
    pack["names"][0]["symbol"] = "AAPL"
    with pytest.raises(NotFictitious, match="AAPL"):
        assert_fictitious(pack)


def test_dry_run_never_calls_the_model() -> None:
    def boom(**_: Any) -> InvocationResult:
        raise AssertionError("dry run called the model")

    outcome = run_synthetic(executable="/nonexistent/claude", source_env=_ENV, invoke=boom, dry_run=True)
    assert outcome.invocation is None
    assert "<pack>" in outcome.summary["rendered_prompt"]


def test_validates_and_pairs_the_accepted_decision() -> None:
    calls: list[dict[str, Any]] = []
    structured = {"decisions": [_decision("SYN_FXTR"), _decision("SYN_ALFA")], "no_trade_reason": None}
    outcome = run_synthetic(
        executable="/nonexistent/claude", source_env=_ENV, invoke=_fake_invoke(structured, calls=calls)
    )
    assert calls[0]["system_prompt"] == SYSTEM_PROMPT
    held, accepted = outcome.summary["decisions"]
    assert held["reason_code"] == "already_held" and "pair" not in held
    assert accepted["reason_code"] is None
    pair = accepted["pair"]
    assert pair["pair_seq"] == 0
    assert 900005 not in pair["pool"]  # the control leg's holding
    assert 900006 in pair["pool"]  # the ARM's holding is not excluded from the control pool
    assert pair["control_instrument_id"] == pair["pool"][pair["index"]]
    # §16.4: BRVO detects no setup, so it is filtered out; the arm's own name stays in (O-v6-2).
    assert 900002 not in pair["pool"] and 900001 in pair["pool"]
    assert accepted["baseline"] is not None and accepted["stop_pct"] is not None
    assert pair["control"]["stop_price"] is not None


def test_injected_symbol_is_refused_not_in_shortlist() -> None:
    structured = {"decisions": [_decision("SYN_ZULU")], "no_trade_reason": None}
    outcome = run_synthetic(executable="/nonexistent/claude", source_env=_ENV, invoke=_fake_invoke(structured))
    assert outcome.summary["decisions"][0]["reason_code"] == "not_in_shortlist"


def test_refused_invocation_stops_before_validation() -> None:
    outcome = run_synthetic(
        executable="/nonexistent/claude", source_env=_ENV, invoke=_fake_invoke(None, refusal="isolation_violation")
    )
    assert outcome.summary["refusal_reason"] == "isolation_violation"
    assert "decisions" not in outcome.summary


def test_cli_requires_the_synthetic_flag() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--dry-run", "--claude-bin", "/nonexistent/claude"])
    assert exc.value.code == 2


def test_dry_run_cli_needs_no_claude_executable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("scripts.ai_trial_synthetic.shutil.which", lambda _: None)
    assert main(["--synthetic", "--dry-run"]) == 0
    assert "rendered_prompt" in capsys.readouterr().out


def test_cli_version_is_never_empty_and_never_raises(tmp_path: Any) -> None:
    from scripts.ai_trial_synthetic import _cli_version

    def script(name: str, body: str) -> str:
        path = tmp_path / name
        path.write_bytes(b"#!/bin/sh\n" + body.encode())
        path.chmod(0o755)
        return str(path)

    assert _cli_version(script("ok", "echo '2.1.285 (Claude Code)'\n"), _ENV) == "2.1.285 (Claude Code)"
    assert _cli_version(script("fails", "echo oops >&2; exit 3\n"), _ENV) == "unavailable: exit 3"
    assert _cli_version(script("silent", "exit 0\n"), _ENV) == "unavailable: exit 0"
    assert _cli_version(script("bytes", "printf '\\377v1\\n'\n"), _ENV).endswith("v1")  # invalid UTF-8 is replaced
    assert _cli_version(str(tmp_path / "missing"), _ENV) == "unavailable: FileNotFoundError"
