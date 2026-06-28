"""Unit tests for the narrate-waiting Stop hook (#1369).

Pure-logic, no DB. Loads the standalone hook module from ``.claude/hooks/``
(importlib, same pattern as ``tests/lint/test_check_pooled_conn_across_http.py``)
and table-tests the extractor + decision function against real-shape transcript
JSONL fixtures (assistant text nested under ``message.content[].text``).

Spec: docs/specs/tooling/2026-06-28-stop-narrate-waiting-hook.md.
Source: docs/review-prevention-log.md §1656-1667 (banned tokens at §1666).
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
HOOK_PY = REPO_ROOT / ".claude" / "hooks" / "stop_narrate_waiting.py"

_spec = importlib.util.spec_from_file_location("stop_narrate_waiting", HOOK_PY)
assert _spec is not None and _spec.loader is not None
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def _assistant_line(*texts: str) -> str:
    """One real-shape assistant JSONL line with the given text blocks."""
    return json.dumps(
        {
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": t} for t in texts],
            },
        }
    )


def _write_transcript(tmp_path: Path, *lines: str) -> str:
    p = tmp_path / "transcript.jsonl"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


# --- decide(): the pure decision over already-extracted text -----------------


@pytest.mark.parametrize(
    "text",
    [
        "Awaiting bot review verdict. Standing by.",  # trailing punctuation
        "standing by",
        "I am awaiting verdict on the PR",
        "Will resume when CI goes green",
        "Let me know when the build finishes",
        "Let me know if anything breaks",
        "Tell me when you want the next ticket",
    ],
)
def test_banned_tokens_block(text: str) -> None:
    result = hook.decide(text, stop_hook_active=False)
    assert result.get("decision") == "block"
    assert "reason" in result


@pytest.mark.parametrize(
    "text",
    [
        "poll passed; both checks green",
        "Merged via safe_merge; moving to the next ticket.",
        "",
        "I polled gh pr checks and CI is green.",
    ],
)
def test_clean_text_allows(text: str) -> None:
    assert hook.decide(text, stop_hook_active=False) == {}


def test_none_text_allows() -> None:
    assert hook.decide(None, stop_hook_active=False) == {}


def test_stop_hook_active_short_circuits_even_on_match() -> None:
    """The loop-protection flag caps the guard at one nudge per stop-cycle."""
    assert hook.decide("Standing by.", stop_hook_active=True) == {}


# --- extract_last_assistant_text(): transcript parsing -----------------------


def test_extracts_only_last_assistant_message(tmp_path: Path) -> None:
    path = _write_transcript(
        tmp_path,
        _assistant_line("Standing by for the verdict."),  # historical — must be ignored
        json.dumps({"type": "user", "message": {"role": "user", "content": "go"}}),
        _assistant_line("Done. CI green, merged."),  # final — clean
    )
    text = hook.extract_last_assistant_text(path)
    assert text == "Done. CI green, merged."
    assert hook.decide(text, stop_hook_active=False) == {}


def test_banned_only_in_non_final_message_allows(tmp_path: Path) -> None:
    path = _write_transcript(
        tmp_path,
        _assistant_line("Awaiting verdict."),  # historical
        _assistant_line("Polled checks; all green."),  # final
    )
    assert hook.decide(hook.extract_last_assistant_text(path), stop_hook_active=False) == {}


def test_corrupt_historical_line_does_not_disable_enforcement(tmp_path: Path) -> None:
    """A garbage line before a banned final message must still block."""
    path = _write_transcript(
        tmp_path,
        "{ this is not valid json",
        _assistant_line("Standing by."),  # final, banned
    )
    text = hook.extract_last_assistant_text(path)
    assert hook.decide(text, stop_hook_active=False).get("decision") == "block"


def test_concatenates_multiple_text_blocks(tmp_path: Path) -> None:
    path = _write_transcript(tmp_path, _assistant_line("First.", "Awaiting verdict."))
    text = hook.extract_last_assistant_text(path)
    assert hook.decide(text, stop_hook_active=False).get("decision") == "block"


def test_missing_transcript_fails_open() -> None:
    assert hook.extract_last_assistant_text(None) is None
    assert hook.extract_last_assistant_text("/nonexistent/path.jsonl") is None


def test_all_corrupt_transcript_fails_open(tmp_path: Path) -> None:
    path = _write_transcript(tmp_path, "garbage", "{also bad", "still not json")
    assert hook.extract_last_assistant_text(path) is None


def test_tool_use_only_final_message_has_no_text(tmp_path: Path) -> None:
    """A tool_use-only assistant message yields empty text → allow."""
    line = json.dumps(
        {
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{"type": "tool_use", "name": "Bash", "input": {}}],
            },
        }
    )
    path = _write_transcript(tmp_path, line)
    assert hook.extract_last_assistant_text(path) == ""


# --- main(): end-to-end through stdin (the real hook contract) ---------------


def _run_main(payload: dict[str, object]) -> dict[str, object]:
    proc = subprocess.run(
        [sys.executable, str(HOOK_PY)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout or "{}")


def test_main_blocks_on_banned_final_message(tmp_path: Path) -> None:
    path = _write_transcript(tmp_path, _assistant_line("Awaiting verdict. Standing by."))
    out = _run_main({"transcript_path": path, "stop_hook_active": False})
    assert out.get("decision") == "block"


def test_main_allows_clean_and_missing(tmp_path: Path) -> None:
    path = _write_transcript(tmp_path, _assistant_line("CI green; merged."))
    assert _run_main({"transcript_path": path, "stop_hook_active": False}) == {}
    assert _run_main({}) == {}  # no transcript_path → fail-open
    assert _run_main({"foo": "bar"}) == {}


def test_main_honours_stop_hook_active(tmp_path: Path) -> None:
    path = _write_transcript(tmp_path, _assistant_line("Standing by."))
    assert _run_main({"transcript_path": path, "stop_hook_active": True}) == {}
