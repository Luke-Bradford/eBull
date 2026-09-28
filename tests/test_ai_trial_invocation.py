"""#3471 slice 1a — the §4 invocation wrapper against a FAKE ``claude`` executable.

No network and no model: the fake reads its scenario from stdin (which also proves the prompt
travels on stdin — the env is allowlisted, so a scenario env var could not reach it) and emits
stream-json events shaped like the real CLI's (measured 2026-09-28, CLI 2.1.280).
"""

from __future__ import annotations

import json
import os
import stat
import sys
import textwrap
import time
from pathlib import Path

import pytest

from app.services.ai_trial_invocation import (
    ENV_ALLOWLIST,
    TRIAL_MODEL_ID,
    build_argv,
    build_env,
    invoke_model,
)

FAKE = textwrap.dedent(
    """
    import json, os, subprocess, sys, time
    req = json.loads(sys.stdin.read())
    scen = req["scenario"]
    model = sys.argv[sys.argv.index("--model") + 1]
    def emit(obj):
        sys.stdout.write(json.dumps(obj) + "\\n"); sys.stdout.flush()
    init = {"type": "system", "subtype": "init", "tools": ["StructuredOutput"], "mcp_servers": [],
            "permissionMode": "default", "model": model, "cwd": os.getcwd(),
            "env_keys": sorted(os.environ), "cwd_entries": os.listdir("."),
            "cwd_mode": oct(os.stat(".").st_mode & 0o777)}
    if scen == "etoro_tool":
        init["tools"] = ["StructuredOutput", "mcp__claude_ai_eToro__place-trade"]
    if scen == "mcp_server":
        init["mcp_servers"] = [{"name": "eToro", "status": "connected"}]
    if scen == "bypass":
        init["permissionMode"] = "bypassPermissions"
    if scen == "drift":
        init["model"] = "claude-sonnet-5"
    if scen == "trailing_violation":
        bad = dict(init, tools=["StructuredOutput", "mcp__claude_ai_eToro__place-trade"])
        sys.stdout.write(json.dumps(bad)); sys.stdout.flush()  # no trailing newline
        os.close(1); os.close(2)
        time.sleep(30)
    if scen == "setsid_grandchild":
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        open(req["pidfile"], "w").write(str(child.pid))
        init["tools"] = ["Bash"]
    if scen == "huge_int":
        emit(init)
        sys.stdout.write('{"type": "assistant", "x": ' + "1" * 5000 + "}\\n"); sys.stdout.flush()
        time.sleep(30)
    if scen == "not_init_first":
        emit({"type": "rate_limit_event"})
    if scen == "dup_key":
        sys.stdout.write('{"type": "system", "type": "system", "subtype": "init"}\\n'); sys.stdout.flush()
        time.sleep(30)
    emit(init)
    if scen in ("etoro_tool", "mcp_server", "bypass", "drift", "setsid_grandchild"):
        time.sleep(30)  # the wrapper must kill us long before this ends
    if scen == "timeout":
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        open(req["pidfile"], "w").write(str(child.pid))
        time.sleep(60)
    if scen == "overflow":
        for _ in range(200):
            emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "x" * 1000}]}})
    if scen == "other_tool":
        emit({"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "Bash"}]}})
        time.sleep(30)
    n_calls = 2 if scen == "two_outputs" else 1
    for _ in range(n_calls):
        emit({"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "StructuredOutput"}]}})
    if scen == "two_outputs":
        time.sleep(30)
    result = {"type": "result", "subtype": "success", "is_error": scen == "is_error",
              "result": "boom" if scen == "is_error" else "{}", "num_turns": 2, "total_cost_usd": 0.01,
              "structured_output": None if scen == "no_output" else {"decisions": [], "no_trade_reason": "x"}}
    emit(result)
    sys.exit(3 if scen == "nonzero" else 0)
    """
)


@pytest.fixture
def fake_claude(tmp_path: Path) -> str:
    path = tmp_path / "claude"
    path.write_text(f"#!{sys.executable}\n{FAKE}")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


def _env() -> dict[str, str]:
    return {**os.environ, "DATABASE_URL": "postgresql://secret", "ETORO_API_KEY": "secret"}


def _run(fake: str, scenario: str, **kw: object):
    prompt = json.dumps({"scenario": scenario, **{k: v for k, v in kw.items() if k == "pidfile"}})
    opts = {k: v for k, v in kw.items() if k != "pidfile"}
    return invoke_model(
        executable=fake,
        prompt=prompt,
        system_prompt="You output only the requested JSON.",
        json_schema={"type": "object"},
        source_env=_env(),
        **opts,  # type: ignore[arg-type]
    )


class TestArgvAndEnv:
    def test_argv_carries_every_lockout_flag_and_no_prompt(self) -> None:
        argv = build_argv("/abs/claude", model_id=TRIAL_MODEL_ID, system_prompt="SYS", json_schema={"a": 1})
        assert argv[argv.index("--tools") + 1] == ""
        assert argv[argv.index("--mcp-config") + 1] == '{"mcpServers":{}}'
        assert argv[argv.index("--setting-sources") + 1] == ""
        assert argv[argv.index("--model") + 1] == TRIAL_MODEL_ID
        assert argv[argv.index("--output-format") + 1] == "stream-json"
        assert "--strict-mcp-config" in argv and "--no-session-persistence" in argv
        assert not any("allowedTools" in a or "dangerously" in a or "bypass" in a for a in argv)

    def test_relative_executable_refused(self) -> None:
        with pytest.raises(ValueError):
            build_argv("claude", model_id=TRIAL_MODEL_ID, system_prompt="S", json_schema={})

    def test_env_is_allowlist_only(self) -> None:
        env = build_env({"PATH": "/bin", "HOME": "/h", "USER": "u", "DATABASE_URL": "x", "ETORO_API_KEY": "y"})
        assert env == {"PATH": "/bin", "HOME": "/h", "USER": "u"}
        assert set(env) <= set(ENV_ALLOWLIST)

    @pytest.mark.parametrize("missing", ["PATH", "HOME"])
    def test_env_requires_path_and_home(self, missing: str) -> None:
        source = {"PATH": "/bin", "HOME": "/h"}
        del source[missing]
        with pytest.raises(ValueError):
            build_env(source)


class TestHappyPath:
    def test_ok_call_returns_structured_output_in_isolation(self, fake_claude: str) -> None:
        result = _run(fake_claude, "ok")
        assert result.ok, result.detail
        assert result.structured_output == {"decisions": [], "no_trade_reason": "x"}
        assert result.exit_code == 0
        init = result.init_event
        assert init is not None
        # The fake is itself a Python process: CPython's PEP 538 locale coercion adds LC_CTYPE and
        # macOS adds __CF_USER_TEXT_ENCODING to ITS OWN environ (`env -i PATH=.. HOME=.. python`
        # shows exactly those two). Neither came from us.
        assert set(init["env_keys"]) <= set(ENV_ALLOWLIST) | {"LC_CTYPE", "__CF_USER_TEXT_ENCODING"}
        assert "DATABASE_URL" not in init["env_keys"] and "ETORO_API_KEY" not in init["env_keys"]
        assert init["cwd_entries"] == []
        assert init["cwd_mode"] == "0o700"
        assert not os.path.exists(init["cwd"])  # temp cwd removed afterwards


class TestRefusals:
    @pytest.mark.parametrize(
        ("scenario", "reason"),
        [
            ("etoro_tool", "isolation_violation"),
            ("mcp_server", "isolation_violation"),
            ("bypass", "isolation_violation"),
            ("drift", "model_drift"),
            ("other_tool", "isolation_violation"),
            ("two_outputs", "multiple_structured_outputs"),
            ("dup_key", "protocol_violation"),
        ],
    )
    def test_violation_kills_promptly(self, fake_claude: str, scenario: str, reason: str) -> None:
        started = time.monotonic()
        result = _run(fake_claude, scenario)
        assert result.refusal_reason == reason, result.detail
        assert result.structured_output is None
        assert time.monotonic() - started < 15  # the fake would otherwise sleep 30s
        assert result.exit_code is not None and result.exit_code < 0  # killed by signal

    def test_first_event_must_be_init(self, fake_claude: str) -> None:
        assert _run(fake_claude, "not_init_first").refusal_reason == "protocol_violation"

    def test_timeout_kills_the_whole_group(self, fake_claude: str, tmp_path: Path) -> None:
        pidfile = tmp_path / "grandchild.pid"
        result = _run(fake_claude, "timeout", pidfile=str(pidfile), timeout_seconds=3.0)
        assert result.refusal_reason == "model_timeout"
        grandchild = int(pidfile.read_text())
        deadline = time.monotonic() + 5
        alive = True
        while alive and time.monotonic() < deadline:
            try:
                os.kill(grandchild, 0)
                time.sleep(0.1)
            except ProcessLookupError:
                alive = False
        assert not alive, "grandchild survived the process-group kill"

    def test_output_cap(self, fake_claude: str) -> None:
        assert _run(fake_claude, "overflow", output_cap_bytes=20_000).refusal_reason == "output_overflow"

    @pytest.mark.parametrize(
        ("scenario", "reason"),
        [("nonzero", "nonzero_exit"), ("is_error", "is_error"), ("no_output", "no_structured_output")],
    )
    def test_result_level_refusals(self, fake_claude: str, scenario: str, reason: str) -> None:
        assert _run(fake_claude, scenario).refusal_reason == reason


def _wait_dead(pid: int, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.1)
    return False


class TestCkpt2Regressions:
    def test_violation_in_unterminated_last_line_kills_without_waiting(self, fake_claude: str) -> None:
        started = time.monotonic()
        result = _run(fake_claude, "trailing_violation", timeout_seconds=20.0)
        assert result.refusal_reason == "isolation_violation"
        assert time.monotonic() - started < 10  # not the 20s remaining timeout

    def test_setsid_grandchild_is_killed_too(self, fake_claude: str, tmp_path: Path) -> None:
        pidfile = tmp_path / "escaped.pid"
        result = _run(fake_claude, "setsid_grandchild", pidfile=str(pidfile))
        assert result.refusal_reason == "isolation_violation"
        assert _wait_dead(int(pidfile.read_text())), "a grandchild outside the process group survived"

    def test_decoder_limit_is_a_protocol_violation_not_an_exception(self, fake_claude: str) -> None:
        assert _run(fake_claude, "huge_int").refusal_reason == "protocol_violation"
