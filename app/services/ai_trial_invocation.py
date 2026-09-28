"""AI-discretionary-v1 model invocation (#3471 slice 1a): a tool-less, MCP-less ``claude -p`` call.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §4 and obligation O4.

⚠⚠ WHY THE LOCKOUT IS A SAFETY CONTROL, NOT HYGIENE. The operator's account carries the
claude.ai eToro connector, whose tools include ``place-trade`` and ``place-close``. A headless
call started without ``--tools ""`` and ``--strict-mcp-config`` could reach the broker directly,
around the execution guard. So every call:

1. is built from an argv LIST (no shell), with an allowlisted env and an empty 0700 cwd, so no
   project settings, hooks, ``CLAUDE.md`` or ``.mcp.json`` load;
2. passes ``--tools ""`` + ``--strict-mcp-config`` with an empty config + ``--setting-sources ""``
   and grants no permissions;
3. has its OWN ``init`` event checked before anything else is accepted: tools must be exactly
   ``["StructuredOutput"]`` (the CLI's synthetic schema-output tool; measured 2026-09-28), MCP
   servers empty, permission mode ``default``, model the declared id. Any violation kills the
   process group and refuses the run.

⚠ This is configuration plus per-run detection. It is NOT OS-level privilege isolation: the child
still runs as the operator (spec §14).

Nothing here retries. One call per run; a failed call is a refused run (spec §3 step 3).
"""

from __future__ import annotations

import json
import os
import selectors
import signal
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal

from app.services.ai_trial_decision import StrictJSONError, strict_json_loads

TRIAL_MODEL_ID: Final = "claude-opus-5-5"
EXPECTED_INIT_TOOLS: Final = ("StructuredOutput",)
EXPECTED_PERMISSION_MODE: Final = "default"
MODEL_TIMEOUT_SECONDS: Final = 600.0
#: Combined stdout + stderr cap (O4).
OUTPUT_CAP_BYTES: Final = 2 * 1024 * 1024

#: Measured 2026-09-28 with ``env -i``: PATH + HOME alone gives "Not logged in"; these five
#: authenticate. Nothing else crosses — in particular no DB URL and no broker variable.
ENV_ALLOWLIST: Final = ("PATH", "HOME", "USER", "LOGNAME", "TMPDIR")

InvocationRefusal = Literal[
    "isolation_violation",
    "model_drift",
    "protocol_violation",
    "multiple_structured_outputs",
    "model_timeout",
    "output_overflow",
    "nonzero_exit",
    "is_error",
    "no_structured_output",
]


def build_argv(executable: str, *, model_id: str, system_prompt: str, json_schema: Mapping[str, Any]) -> list[str]:
    """The exact argv (spec §4). The prompt goes on stdin, never in argv."""
    if not os.path.isabs(executable):
        raise ValueError("the claude executable must be an absolute path resolved at deploy time")
    return [
        executable,
        "-p",
        "--model",
        model_id,
        "--tools",
        "",
        "--strict-mcp-config",
        "--mcp-config",
        '{"mcpServers":{}}',
        "--setting-sources",
        "",
        "--system-prompt",
        system_prompt,
        "--no-session-persistence",
        "--output-format",
        "stream-json",
        "--verbose",
        "--json-schema",
        json.dumps(json_schema, sort_keys=True, separators=(",", ":")),
    ]


def build_env(source: Mapping[str, str]) -> dict[str, str]:
    """Allowlist-only env. PATH and HOME are required; the rest pass through when present."""
    missing = [name for name in ("PATH", "HOME") if not source.get(name)]
    if missing:
        raise ValueError(f"required env missing: {', '.join(missing)}")
    return {name: source[name] for name in ENV_ALLOWLIST if source.get(name)}


@dataclass
class _StreamCheck:
    """Incremental fail-closed checks over stream-json events (O4)."""

    model_id: str
    init_event: dict[str, Any] | None = None
    result_event: dict[str, Any] | None = None
    structured_output_calls: int = 0
    refusal: InvocationRefusal | None = None
    detail: str = ""

    def _refuse(self, reason: InvocationRefusal, detail: str) -> None:
        if self.refusal is None:
            self.refusal, self.detail = reason, detail

    def feed(self, line: bytes) -> None:
        if self.refusal is not None or not line.strip():
            return
        try:
            event = strict_json_loads(line.decode("utf-8"))
        except (StrictJSONError, UnicodeDecodeError) as exc:
            self._refuse("protocol_violation", f"unparseable event: {exc}")
            return
        if not isinstance(event, dict):
            self._refuse("protocol_violation", "event is not an object")
            return
        kind, subtype = event.get("type"), event.get("subtype")
        if self.init_event is None:
            if (kind, subtype) != ("system", "init"):
                self._refuse("protocol_violation", f"first event is {kind}/{subtype}, not system/init")
                return
            self.init_event = event
            self._check_init(event)
            return
        if (kind, subtype) == ("system", "init"):
            self._refuse("protocol_violation", "second init event")
        elif kind == "assistant":
            self._check_assistant(event)
        elif kind == "result":
            if self.result_event is not None:
                self._refuse("protocol_violation", "second result event")
            self.result_event = event

    def _check_init(self, event: dict[str, Any]) -> None:
        tools, mcp = event.get("tools"), event.get("mcp_servers")
        if tools != list(EXPECTED_INIT_TOOLS) or mcp != []:
            self._refuse("isolation_violation", f"init tools={tools!r} mcp_servers={mcp!r}")
        elif event.get("permissionMode") != EXPECTED_PERMISSION_MODE:
            self._refuse("isolation_violation", f"init permissionMode={event.get('permissionMode')!r}")
        elif event.get("model") != self.model_id:
            self._refuse("model_drift", f"init model={event.get('model')!r}, declared {self.model_id!r}")

    def _check_assistant(self, event: dict[str, Any]) -> None:
        message = event.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            return
        for item in content:
            if not isinstance(item, dict) or item.get("type") != "tool_use":
                continue
            if item.get("name") not in EXPECTED_INIT_TOOLS:
                self._refuse("isolation_violation", f"tool_use {item.get('name')!r}")
                return
            self.structured_output_calls += 1
            if self.structured_output_calls > 1:
                self._refuse("multiple_structured_outputs", "more than one structured output attempt")
                return


@dataclass(frozen=True)
class InvocationResult:
    refusal_reason: InvocationRefusal | None
    detail: str
    init_event: dict[str, Any] | None
    result_event: dict[str, Any] | None
    structured_output: object
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    duration_ms: int
    argv: tuple[str, ...] = field(default=())

    @property
    def ok(self) -> bool:
        return self.refusal_reason is None


def _descendant_pids(root: int) -> set[int]:
    """Every live descendant of ``root``, from one ``ps`` snapshot (macOS + Linux).

    Walked by parent pid, so it also finds a grandchild that called ``setsid`` and left the
    process group — which ``killpg`` alone would miss (ckpt-2).
    """
    try:
        out = subprocess.run(["ps", "-A", "-o", "pid=,ppid="], capture_output=True, text=True, timeout=10).stdout
    except OSError, subprocess.SubprocessError:
        return set()
    children: dict[int, list[int]] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            children.setdefault(int(parts[1]), []).append(int(parts[0]))
    found: set[int] = set()
    stack = [root]
    while stack:
        for child in children.get(stack.pop(), ()):
            if child not in found:
                found.add(child)
                stack.append(child)
    return found


def _kill_tree(proc: subprocess.Popen[bytes]) -> None:
    """SIGKILL the child's process group AND every descendant found before the kill, then reap.

    ⚠ The snapshot is taken while the child is alive, so descendants are still parented to it.
    A worker crash before this runs leaves any survivor to the next run's sweep, which belongs to
    the job runner (slice 2) — this function cannot run after its own process has died.
    """
    descendants = _descendant_pids(proc.pid)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError, PermissionError:
        pass
    for pid in descendants:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError, PermissionError:
            pass
    proc.wait()


def invoke_model(
    *,
    executable: str,
    prompt: str,
    system_prompt: str,
    json_schema: Mapping[str, Any],
    source_env: Mapping[str, str],
    model_id: str = TRIAL_MODEL_ID,
    timeout_seconds: float = MODEL_TIMEOUT_SECONDS,
    output_cap_bytes: int = OUTPUT_CAP_BYTES,
    clock: Callable[[], float] = time.monotonic,
) -> InvocationResult:
    """Run ONE model call and return its outcome; never raises on a model-side failure.

    The child runs in its own session so a kill reaches every descendant. stdout is checked
    line by line as it arrives, so an isolation violation is acted on before the model's turn
    can complete; a timeout or the combined output cap also kills the group.
    """
    argv = build_argv(executable, model_id=model_id, system_prompt=system_prompt, json_schema=json_schema)
    env = build_env(source_env)
    check = _StreamCheck(model_id=model_id)
    stdout_chunks: list[bytes] = []
    stderr_chunks: list[bytes] = []
    total = 0
    pending = b""
    started = clock()
    timed_out = overflow = False
    with tempfile.TemporaryDirectory(prefix="ai-trial-") as cwd:
        os.chmod(cwd, 0o700)
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None

        def _write_prompt(stdin: Any = proc.stdin) -> None:
            try:
                stdin.write(prompt.encode("utf-8"))
                stdin.close()
            except BrokenPipeError:
                pass

        writer = threading.Thread(target=_write_prompt, daemon=True)
        writer.start()
        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ, "out")
        selector.register(proc.stderr, selectors.EVENT_READ, "err")
        try:
            while selector.get_map():
                remaining = timeout_seconds - (clock() - started)
                if remaining <= 0:
                    timed_out = True
                    break
                for key, _ in selector.select(timeout=min(remaining, 1.0)):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    total += len(chunk)
                    if total > output_cap_bytes:
                        overflow = True
                        break
                    if key.data == "err":
                        stderr_chunks.append(chunk)
                        continue
                    stdout_chunks.append(chunk)
                    pending += chunk
                    *lines, pending = pending.split(b"\n")
                    for line in lines:
                        check.feed(line)
                if overflow or check.refusal is not None:
                    break
            if not (timed_out or overflow or check.refusal is not None) and pending:
                # A final event with no trailing newline is checked BEFORE deciding to wait, so a
                # violation in it kills now rather than after the remaining timeout (ckpt-2).
                check.feed(pending)
                pending = b""
            if timed_out or overflow or check.refusal is not None:
                _kill_tree(proc)
            else:
                try:
                    proc.wait(timeout=max(0.0, timeout_seconds - (clock() - started)))
                except subprocess.TimeoutExpired:
                    timed_out = True
                    _kill_tree(proc)
        finally:
            selector.close()
            if proc.poll() is None:
                # Any unexpected exception above still reaps the tree before propagating.
                _kill_tree(proc)
            writer.join(timeout=1.0)

    exit_code = proc.returncode
    refusal: InvocationRefusal | None = check.refusal
    detail = check.detail
    structured: object = None
    if refusal is None:
        if timed_out:
            refusal, detail = "model_timeout", f"exceeded {timeout_seconds}s"
        elif overflow:
            refusal, detail = "output_overflow", f"exceeded {output_cap_bytes} bytes"
        elif check.init_event is None:
            refusal, detail = "protocol_violation", "no init event"
        elif exit_code != 0:
            refusal, detail = "nonzero_exit", f"exit code {exit_code}"
        elif check.result_event is None:
            refusal, detail = "protocol_violation", "no result event"
        elif check.result_event.get("is_error"):
            refusal, detail = "is_error", str(check.result_event.get("result"))[:500]
        elif check.result_event.get("structured_output") is None:
            refusal, detail = "no_structured_output", "result carries no structured_output"
        else:
            structured = check.result_event["structured_output"]
    return InvocationResult(
        refusal_reason=refusal,
        detail=detail,
        init_event=check.init_event,
        result_event=check.result_event,
        structured_output=structured,
        exit_code=exit_code,
        stdout=b"".join(stdout_chunks),
        stderr=b"".join(stderr_chunks),
        duration_ms=int((clock() - started) * 1000),
        argv=tuple(argv),
    )
