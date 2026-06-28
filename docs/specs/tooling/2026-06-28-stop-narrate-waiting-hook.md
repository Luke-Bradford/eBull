# Stop hook: narrate-waiting sentinel (#1369)

## Problem

The review-prevention-log entry "Agent narrates 'waiting' instead of polling"
(`docs/review-prevention-log.md:1656-1667`) pins a behavioural rule but relies on
the agent self-policing. The agent has ignored it in practice (PRs #1364/#1366 —
operator had to nudge twice). Line 1667 tracks the active-enforcement follow-up:
a Stop-event hook that intercepts the banned phrasing before the turn ends.

## Source rule

Not an ownership/filings/metric data rule — this is harness tooling. The
"governing rule" is the prevention-log entry itself:
- Banned phrasing (regression sentinel, `docs/review-prevention-log.md:1666` +
  symptom line 1659): `Standing by`, `Awaiting verdict`, `Will resume when X`,
  `Let me know when…`, `Let me know if…`, `Tell me when…` (case-insensitive).
- Required substitute (line 1660-1665): arm a Monitor / poll / find parallel work
  rather than ending a turn on an open-ended wait.

## Premise check (why a native hook, not a stock hookify rule)

The issue says "hookify rule at `.claude/hooks/…`". Verified against the stock
hookify plugin (`~/.claude/plugins/.../hookify/core/rule_engine.py`):

- A stock hookify Stop rule can only match `field: transcript`, which reads the
  **entire** transcript JSONL and runs `regex.search` over the whole blob. Any
  historical occurrence of a banned phrase would then fire on **every** later
  stop → a permanent block loop. It cannot isolate the final message.
- A simple `pattern:` on `event: stop` infers `field: content`, which is absent
  on Stop input → never matches at all.
- Hookify rule files live at `.claude/hookify.*.local.md` (gitignored) — not a
  shippable PR diff.

eBull already uses **native `type: command` hooks** in the committed
`.claude/settings.json` (a PostToolUse git-push reminder). The correct shape is a
native Stop hook script under the committed `.claude/hooks/` dir that parses
`transcript_path` and greps **only the last assistant message**.

## Design

`.claude/hooks/stop_narrate_waiting.py` — stdin JSON Stop-hook contract:

1. Read `{transcript_path, stop_hook_active, ...}` from stdin.
2. **`stop_hook_active is True` → allow (`{}`)** immediately. This is Claude
   Code's loop-protection flag: the stop is already a continuation from a prior
   stop-hook block. Honouring it caps the guard at exactly one nudge per
   stop-cycle and makes an infinite block impossible in the unattended loop.
3. Extract the **last assistant message** from the JSONL by scanning **from the
   tail**: parse each line, skip malformed lines, and take the first (from the
   end) object with `type == "assistant"`. Concatenate its
   **`message.content[].text`** blocks (verified shape — assistant text is nested
   under `message`, not top-level). At Stop the final message is text-only; a
   tool_use would have continued the turn.
4. Case-insensitively search that text for the banned-token regex.
5. Match → emit `{"decision":"block","reason": <reminder>}`. The reminder tells
   the agent to arm a Monitor / poll / find parallel work / justify no parallel
   work, and never to end a turn on a narrate-waiting phrase with no follow-on
   tool call.
6. No match, no transcript, **no usable assistant message** (every line corrupt),
   or any exception → emit `{}` and exit 0 (**fail-open**, matching the existing
   hooks' always-allow posture). A corrupt *historical* line must NOT disable
   enforcement when a valid final assistant message exists — skip-and-continue,
   fail-open only when no assistant message is recoverable at all.

Wire into `.claude/settings.json` under a new `Stop` array (matcher-less). The
shell command must itself never exit nonzero — a missing `$CLAUDE_PROJECT_DIR` or
an unopenable script would otherwise surface a Stop-hook error:
`python3 "$CLAUDE_PROJECT_DIR/.claude/hooks/stop_narrate_waiting.py" || printf '{}\n'`.

Banned-token regex (case-insensitive, word-boundary anchored, no literal padding
spaces — must match a token with trailing punctuation like "Standing by."):
`(?i)\b(?:standing by|awaiting verdict|will resume when|let me know (?:when|if)|tell me when)\b`

## Why only the last message + stop_hook_active

Two independent guards against wedging the unattended loop:
- last-message-only scoping → no historical false positives;
- `stop_hook_active` short-circuit → at most one block per stop-cycle.

Either alone prevents a runaway; both together is belt-and-suspenders.

## Tests

Pure-logic unit test (`tests/test_stop_narrate_waiting_hook.py`, no DB) over the
extractor + decision function, table-driven, using **real-shape** JSONL fixtures
(`message.content[].text`):
- positive: final message "Awaiting bot review verdict. Standing by." → block
  (also covers trailing-punctuation matching).
- negative: "poll passed; both checks green" → allow.
- negative: banned phrase only in a NON-final assistant message → allow.
- guard: `stop_hook_active=True` with a banned final message → allow.
- robustness: corrupt *historical* line BEFORE a banned final message → block
  (skip-and-continue, enforcement not disabled).
- robustness: missing/empty transcript, all-corrupt JSONL → allow (fail-open).
- token coverage: each of the six banned tokens matches; "let me know if" too.

## Out of scope

- UserPromptSubmit variant (the issue lists it as OR; Stop is the high-value path
  — the agent ends turns on Stop, not on user prompts in the unattended loop).
- Widening tokens beyond the line-1666 list (start minimal; add on false negatives).

## Acceptance / prevention-log link

On merge, update `docs/review-prevention-log.md:1667` with the rule path
(`.claude/hooks/stop_narrate_waiting.py`). Cross-link memory
`[[feedback_post_push_cycle]]` + `[[feedback_no_fake_polling]]`.
