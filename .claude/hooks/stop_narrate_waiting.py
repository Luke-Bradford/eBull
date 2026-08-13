#!/usr/bin/env python3
"""Stop hook: block turns that end on a narrate-waiting phrase (#1369).

Active enforcement of the review-prevention-log entry "Agent narrates
'waiting' instead of polling" (``docs/review-prevention-log.md`` §1656-1667).
When the agent's FINAL message ends a turn on an open-ended wait phrase
(``Standing by``, ``Awaiting verdict``, …) with no follow-on tool call, this
hook blocks the stop and injects a reminder to poll / arm a Monitor / find
parallel work instead.

Design (see ``docs/specs/tooling/2026-06-28-stop-narrate-waiting-hook.md``):

- Match ONLY the last assistant message — never the whole transcript — so a
  banned phrase in history cannot fire on every later stop.
- Honour ``stop_hook_active``: if the stop is already a continuation from a
  prior stop-hook block, allow it. Caps the guard at one nudge per stop-cycle;
  makes an infinite block impossible in the unattended autonomy loop.
- Fail OPEN: no transcript / no recoverable assistant message / any exception
  → allow the stop (emit ``{}``). A corrupt *historical* line is skipped, not
  fatal — enforcement only disables when no assistant message is recoverable.

Portability: the live hook is invoked as bare ``python3`` (the system
interpreter — 3.9 on this host), NOT the project ``.venv`` (3.14). Keep this
file 3.9-compatible: parenthesised ``except (A, B):`` tuples, no PEP 758
parenless syntax. ``.claude`` is ``extend-exclude``d from ruff (pyproject.toml),
so the repo format gate leaves it alone — do NOT run ``ruff format`` on this
file directly: with ``target-version = py314`` ruff would PEP-758 the except
tuples into ``except A, B:``, which is a SyntaxError under the 3.9 hook runtime.
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any

# Word-boundary anchored, case-insensitive, no literal padding spaces so a token
# followed by punctuation ("Standing by.") still matches. Tokens per
# prevention-log §1666 + symptom §1659.
_BANNED = re.compile(
    r"\b(?:standing by|awaiting verdict|will resume when|"
    r"let me know (?:when|if)|tell me when)\b",
    re.IGNORECASE,
)

_REMINDER = (
    "You ended a turn on a narrate-waiting phrase (e.g. 'Standing by', "
    "'Awaiting verdict', 'Will resume when…', 'Let me know when/if…', "
    "'Tell me when…') with no follow-on tool call. Per "
    "docs/review-prevention-log.md §1656-1667 you MUST NOT sit and wait. "
    "Re-evaluate before stopping: arm a Monitor for the wait condition, OR "
    "actively poll now (gh pr view/checks, BashOutput, job status), OR pick up "
    "useful parallel work, OR — only if genuinely none exists — say so "
    "explicitly. Never end a turn on an open-ended wait phrase."
)


def extract_last_assistant_text(transcript_path: str | None) -> str | None:
    """Return concatenated text of the last assistant message, or None.

    Scans the JSONL from the tail, skipping malformed lines, and returns the
    text of the first ``type == "assistant"`` object found (most recent).
    Returns None when the path is missing/unreadable or no assistant message
    with text is recoverable — the caller treats None as fail-open (allow).
    """
    if not transcript_path:
        return None
    try:
        with open(transcript_path, encoding="utf-8") as fh:
            lines = fh.readlines()
    except (OSError, UnicodeDecodeError):
        return None

    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            obj: Any = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            # Corrupt historical line — skip, keep scanning toward a usable one.
            continue
        if not isinstance(obj, dict) or obj.get("type") != "assistant":
            continue
        message = obj.get("message")
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        texts = [
            block["text"]
            for block in content
            if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str)
        ]
        return "\n".join(texts)
    return None


def decide(text: str | None, *, stop_hook_active: bool) -> dict[str, Any]:
    """Return the Stop-hook response: block dict on a match, else allow ``{}``."""
    if stop_hook_active:
        return {}
    if text and _BANNED.search(text):
        return {"decision": "block", "reason": _REMINDER, "systemMessage": _REMINDER}
    return {}


def main() -> None:
    try:
        data = json.load(sys.stdin)
        stop_hook_active = bool(data.get("stop_hook_active"))
        text = extract_last_assistant_text(data.get("transcript_path"))
        print(json.dumps(decide(text, stop_hook_active=stop_hook_active)))
    except Exception:
        # Fail open — never let the guard wedge a turn.
        print("{}")
    sys.exit(0)


if __name__ == "__main__":
    main()
