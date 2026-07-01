# Autonomy engine ↔ pack seam Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract eBull's `scripts/autonomy/` (a working, hardened Claude Code autonomy loop) into a
standalone, repo-agnostic GitHub repo `autonomy-engine`, plus a `.autonomy/` pack in eBull, with
eBull fully cut over — `scripts/autonomy/*` deleted, launchd repointed at the new engine.

**Architecture:** A new repo (`bin/*.sh` + `bin/agents/claude.sh` + `lib/config_parser.py` +
`templates/`) drives an agent CLI against whatever target repo is passed via `--repo`, reading that
repo's `.autonomy/config.yaml` for project policy (merge-gate strategy, board identity, model
choice). Four merge-gate strategies (`manual`/`ci_only`/`bot_comment`/`gh_review`) and one agent
adapter (`claude`, with a documented-but-unbuilt `codex` slot) are the two pluggable seams. eBull's
own pack is the one proof case.

**Tech Stack:** bash (macOS default `/bin/bash` 3.2.57 compatible — no `mapfile`/globstar/`**`),
Python 3 stdlib only (no PyYAML, no third-party deps), `gh` CLI, `git`, `shellcheck`.

## Global Constraints

- No new dependencies beyond `bash`, `python3` (stdlib only), `git`, `gh`, `shellcheck` — the spec's
  "no packaging, no heavy deps" requirement applies to every task.
- Every new/modified `.sh` file must pass `shellcheck -S warning` clean before its task's commit.
- Every bash script's executable body must be guarded by
  `[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0` (or equivalent) so sourcing it for tests only
  defines functions — this is the existing codebase's pattern (`safe_merge.sh`,
  `unblock_dependents.sh` in eBull today) and every new script in this plan follows it.
- Ported logic (rate-limit parsing, preflight git guards, `unblock_dependents.sh`,
  `worktree_gc.sh`'s merge-safety check) must be behaviorally identical to eBull's current
  `scripts/autonomy/*` — these are moves/generalizations, not rewrites, per the spec's own
  "Path rewrites" and "Agent adapters" sections.
- `merge_gate.strategy: manual` is always the safe default when config.yaml doesn't set one —
  never silently fall back to a stronger, auto-merging strategy.
- All work in the `autonomy-engine` repo (Tasks 1–12) commits directly to its own `main` — it's a
  brand-new personal-tool repo with no existing branch/PR discipline of its own yet. Task 13 (the
  eBull cutover) modifies eBull and MUST follow eBull's `.claude/CLAUDE.md` branch/PR workflow
  (branch → commit → push → PR → Claude review bot + CI → resolve comments → merge).

---

## File Structure

```text
~/Dev/autonomy-engine/                  (new repo, sibling to ~/Dev/eBull)
  bin/
    supervisor.sh
    doctor.sh
    board.sh
    safe_merge.sh
    unblock_dependents.sh
    setup_worktree.sh
    worktree_gc.sh
    onboard.sh
    agents/
      claude.sh
  lib/
    config_parser.py
  templates/
    supervisor.plist.tmpl
    autonomy-pack/
      loop_prompt.md
      hard_rules.md
      config.yaml
  tests/
    test_config_parser.py
    test_preflight_recovery.sh
    test_usage_limit_reset.sh
    test_unblock_dependents.sh
    test_safe_merge_doc_only.sh
    test_merge_gate_strategies.sh
    test_agent_dispatch.sh
    test_doctor.sh
    test_onboard.sh
    run_all.sh
  README.md
  .gitignore

~/Dev/eBull/.autonomy/                  (Task 13 — the eBull pack)
  loop_prompt.md
  hard_rules.md
  config.yaml
```

---

### Task 1: Scaffold the `autonomy-engine` repo

**Files:**
- Create: `~/Dev/autonomy-engine/.gitignore`
- Create: `~/Dev/autonomy-engine/README.md` (stub — filled in fully by Task 12)

**Interfaces:**
- Produces: the repo itself at `~/Dev/autonomy-engine`, pushed to
  `github.com/Luke-Bradford/autonomy-engine` (private), `main` branch, ready for every later task to
  commit into.

- [ ] **Step 1: Create the GitHub repo and local clone**

```bash
gh repo create Luke-Bradford/autonomy-engine --private --clone --description "Repo-agnostic engine for running Claude Code autonomy loops against any target repo"
mv autonomy-engine ~/Dev/autonomy-engine
cd ~/Dev/autonomy-engine
```

- [ ] **Step 2: Add `.gitignore`**

```gitignore
__pycache__/
*.pyc
.DS_Store
```

- [ ] **Step 3: Add a stub `README.md`**

```markdown
# autonomy-engine

Repo-agnostic engine for running Claude Code (and, in future, other CLI agents) autonomy loops
against any target repo. See the "Pack contract" section below for what a target repo needs.

Full documentation lands in Task 12 of the implementation plan — this is a placeholder so the repo
isn't empty while the rest of the engine is built out.
```

- [ ] **Step 4: Commit and push**

```bash
mkdir -p bin/agents lib templates/autonomy-pack tests
git add .gitignore README.md
git commit -m "chore: scaffold repo structure"
git push -u origin main
```

- [ ] **Step 5: Verify**

```bash
gh repo view Luke-Bradford/autonomy-engine --json name,visibility
```
Expected: `{"name":"autonomy-engine","visibility":"PRIVATE"}`

---

### Task 2: Config parser (`lib/config_parser.py`)

**Files:**
- Create: `lib/config_parser.py`
- Test: `tests/test_config_parser.py`

**Interfaces:**
- Produces: CLI `python3 lib/config_parser.py <config-file> <dotted.key>` — prints the value (one
  line per item if it's a list) and exits 0 if the key is present (including an empty map, printed
  as nothing), exits 1 if the key is absent, exits 1 with a message on stderr if the file doesn't
  parse. A special second-arg value `__validate__` parses the file and returns 0/1 without doing a
  key lookup — used by `doctor.sh`'s fast preflight check.
- Consumes: nothing (this is the first component; every later bash script calls this CLI).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_config_parser.py
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ENGINE_ROOT = Path(__file__).resolve().parent.parent
PARSER = ENGINE_ROOT / "lib" / "config_parser.py"

SAMPLE = """\
board:
  owner: Luke-Bradford
  project_title: "eBull engineering board"

agent:
  type: claude
  model:
    primary: claude-sonnet-5
    fallback: claude-sonnet-4-6
  config: {}

merge_gate:
  strategy: bot_comment
  author_login: github-actions
  marker: "Claude Code Review"
  doc_only_extensions: [".md"]

worktree:
  default_path: "../.{repo-slug}-autonomy"
"""


def run_parser(config_text: str, key: str):
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write(config_text)
        path = f.name
    proc = subprocess.run(
        [sys.executable, str(PARSER), path, key],
        capture_output=True, text=True,
    )
    return proc.returncode, proc.stdout


class TestConfigParser(unittest.TestCase):
    def test_top_level_string(self):
        rc, out = run_parser(SAMPLE, "board.owner")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "Luke-Bradford\n")

    def test_quoted_string_with_spaces(self):
        rc, out = run_parser(SAMPLE, "board.project_title")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "eBull engineering board\n")

    def test_two_levels_of_nesting(self):
        rc, out = run_parser(SAMPLE, "agent.model.primary")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "claude-sonnet-5\n")

    def test_list_value(self):
        rc, out = run_parser(SAMPLE, "merge_gate.doc_only_extensions")
        self.assertEqual(rc, 0)
        self.assertEqual(out, ".md\n")

    def test_empty_map_present_exits_zero_no_output(self):
        rc, out = run_parser(SAMPLE, "agent.config")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "")

    def test_missing_key_exits_one(self):
        rc, out = run_parser(SAMPLE, "merge_gate.reviewer_login")
        self.assertEqual(rc, 1)
        self.assertEqual(out, "")

    def test_comment_stripped(self):
        text = "board:\n  owner: someone  # a trailing comment\n"
        rc, out = run_parser(text, "board.owner")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "someone\n")

    def test_validate_mode_on_good_file(self):
        rc, out = run_parser(SAMPLE, "__validate__")
        self.assertEqual(rc, 0)

    def test_validate_mode_on_bad_file(self):
        rc, out = run_parser("this line has no colon whatsoever\n", "__validate__")
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
python3 -m unittest tests.test_config_parser -v
```
Expected: `ModuleNotFoundError` or `FileNotFoundError` (`lib/config_parser.py` doesn't exist yet).

- [ ] **Step 3: Implement `lib/config_parser.py`**

```python
#!/usr/bin/env python3
"""Restricted YAML-subset parser for .autonomy/config.yaml.

Supports exactly what config.yaml needs: nested mappings (2-space indent),
scalar strings (quoted or bare), booleans, empty maps ({}), and inline
lists (["a", "b"]). No anchors, multi-doc, block scalars, or flow mappings.
Deliberately small and dependency-free -- see the pack-seam spec's
"Parser" note for why this exists instead of PyYAML.
"""
import sys


def _strip_comment(line: str) -> str:
    in_quote = None
    out = []
    for ch in line:
        if in_quote:
            out.append(ch)
            if ch == in_quote:
                in_quote = None
            continue
        if ch in ('"', "'"):
            in_quote = ch
            out.append(ch)
            continue
        if ch == "#":
            break
        out.append(ch)
    return "".join(out)


def _parse_scalar(raw: str):
    raw = raw.strip()
    if raw == "" or raw == "{}":
        return {}
    if raw == "true":
        return True
    if raw == "false":
        return False
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(p.strip()) for p in inner.split(",")]
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("'", '"'):
        return raw[1:-1]
    return raw


def parse(text: str) -> dict:
    root: dict = {}
    stack = [(-1, root)]
    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        line = _strip_comment(raw_line).rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        content = line.strip()
        if ":" not in content:
            raise ValueError(f"line {lineno}: expected 'key: value', got {content!r}")
        key, _, value = content.partition(":")
        key = key.strip()
        if not key:
            raise ValueError(f"line {lineno}: empty key")
        while stack and stack[-1][0] >= indent:
            stack.pop()
        parent = stack[-1][1]
        value = value.strip()
        if value == "":
            child: dict = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_scalar(value)
    return root


def get(config: dict, dotted_key: str):
    node = config
    for part in dotted_key.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(dotted_key)
        node = node[part]
    return node


def main(argv: list) -> int:
    if len(argv) != 3:
        print("usage: config_parser.py <config-file> <dotted.key>", file=sys.stderr)
        return 2
    path, dotted_key = argv[1], argv[2]
    with open(path, encoding="utf-8") as f:
        text = f.read()
    try:
        config = parse(text)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    if dotted_key == "__validate__":
        return 0
    try:
        value = get(config, dotted_key)
    except KeyError:
        return 1
    if isinstance(value, list):
        for item in value:
            print(item)
    elif isinstance(value, dict):
        pass
    elif isinstance(value, bool):
        print("true" if value else "false")
    else:
        print(value)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
touch tests/__init__.py
python3 -m unittest tests.test_config_parser -v
```
Expected: all 9 tests `ok`.

- [ ] **Step 5: Commit**

```bash
git add lib/config_parser.py tests/test_config_parser.py tests/__init__.py
git commit -m "feat: add restricted YAML-subset config parser"
git push
```

---

### Task 3: Claude agent adapter (`bin/agents/claude.sh`)

**Files:**
- Create: `bin/agents/claude.sh`
- Test: `tests/test_usage_limit_reset.sh`

**Interfaces:**
- Consumes: `python3 lib/config_parser.py` is not used here — this file only invokes the `claude`
  CLI and parses its own stream-json log.
- Produces: `agent_invoke(prompt_file, safety_file, model, fallback_model, log_file) -> exit code`
  and `agent_classify_outcome(log_file, exit_code) -> "success" | "usage_limit [epoch]" | "error"`
  (printed to stdout) — these two function names/signatures are what `bin/supervisor.sh` (Task 8)
  dispatches to via `source bin/agents/${AGENT_TYPE}.sh`. Also defines `is_usage_limit_hit` and
  `extract_reset_epoch` as internal helpers (ported verbatim from eBull's current
  `scripts/autonomy/supervisor.sh`).

- [ ] **Step 1: Write the failing test**

```bash
# tests/test_usage_limit_reset.sh
#!/usr/bin/env bash
# Unit test for the claude adapter's reset-epoch extraction/classification.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$HERE/../bin/agents/claude.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}
between() {
  if [ -n "$4" ] && [ "$4" -ge "$2" ] && [ "$4" -le "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (want [$2,$3], got '$4')"; fails=$((fails + 1)); fi
}

mklog() { printf '%s\n' "$1" > "$tmp/log.jsonl"; echo "$tmp/log.jsonl"; }
ISO_EPOCH="$(python3 -c 'from datetime import datetime,timezone;print(int(datetime(2030,6,30,12,0,0,tzinfo=timezone.utc).timestamp()))')"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":"2030-06-30T12:00:00Z"}}')"
check "ISO-8601 'resetsAt' -> epoch" "$ISO_EPOCH" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","reset":4102444800}}')"
check "epoch-seconds 'reset'" 4102444800 "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetAt":4102444800000}}')"
check "epoch-millis 'resetAt' -> seconds" 4102444800 "$(extract_reset_epoch "$f")"

now="$(date +%s)"
f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","retryAfter":1800}}')"
between "relative 'retryAfter' -> now+secs" "$((now + 1790))" "$((now + 1815))" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","retryAfter":"inf"}}')"
check "non-finite retryAfter -> no reset (no crash)" "" "$(extract_reset_epoch "$f" 2>/dev/null)"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","isUsingOverage":true,"resetsAt":"2030-06-30T12:00:00Z"}}')"
check "overage-covered rejection yields no reset" "" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"assistant","message":{"content":"rate limit reset at 9999999999"}}')"
check "content text is never parsed" "" "$(extract_reset_epoch "$f")"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}')"
if is_usage_limit_hit "$f"; then r=blocked; else r=ok; fi
check "rejected + no terminal result = blocked" blocked "$r"

printf '%s\n%s\n' \
  '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}' \
  '{"type":"result","is_error":false}' > "$tmp/log.jsonl"
if is_usage_limit_hit "$tmp/log.jsonl"; then r=blocked; else r=ok; fi
check "rejected BUT session succeeded = not blocked" ok "$r"

f="$(mklog '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":"2030-06-30T12:00:00Z"}}')"
check "agent_classify_outcome reports usage_limit + epoch" "usage_limit $ISO_EPOCH" "$(agent_classify_outcome "$f" 1)"

printf '%s\n' '{"type":"result","is_error":false}' > "$tmp/log.jsonl"
check "agent_classify_outcome reports success" "success" "$(agent_classify_outcome "$tmp/log.jsonl" 0)"

printf '%s\n' '{"type":"result","is_error":true}' > "$tmp/log.jsonl"
check "agent_classify_outcome reports error" "error" "$(agent_classify_outcome "$tmp/log.jsonl" 1)"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
```

- [ ] **Step 2: Run to verify it fails**

```bash
chmod +x tests/test_usage_limit_reset.sh
bash tests/test_usage_limit_reset.sh
```
Expected: fails with "No such file or directory" (`bin/agents/claude.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/agents/claude.sh`**

```bash
#!/usr/bin/env bash
# bin/agents/claude.sh -- the Claude Code agent adapter. Two functions
# supervisor.sh dispatches to via `source bin/agents/${agent.type}.sh`. Every
# other agent type (e.g. a future bin/agents/codex.sh) implements the same two
# functions with its own invocation/log-format details -- see the pack-seam
# spec's "Agent adapters" section.

# Runs the Claude Code CLI once and writes its stream-json log to $5. This is
# eBull's original supervisor.sh invocation, unchanged, just parameterized.
agent_invoke() {
  local prompt_file="$1" safety_file="$2" model="$3" fallback_model="$4" log_file="$5"
  claude -p "$(cat "$prompt_file")" \
    --dangerously-skip-permissions \
    --model "$model" \
    --fallback-model "$fallback_model" \
    --append-system-prompt "$(cat "$safety_file")" \
    --output-format stream-json --verbose \
    >>"$log_file" 2>&1
  return $?
}

# Classify a usage/rate-limit block from the session's stream-json log. Exit 0
# = blocked (caller maps to the limit backoff), 1 = not blocked. Ported
# verbatim from eBull's supervisor.sh -- parses ONLY structured
# rate_limit_info + the terminal result's is_error, never greps content text.
is_usage_limit_hit() {
  python3 - "$1" <<'PY'
import json, sys

rejected = False
result = None
for line in open(sys.argv[1], errors="replace"):
    if '"type"' not in line:
        continue
    try:
        o = json.loads(line)
    except Exception:
        continue
    t = o.get("type")
    if t == "rate_limit_event":
        rli = o.get("rate_limit_info") or {}
        if rli.get("status") == "rejected" and not rli.get("isUsingOverage"):
            rejected = True
    elif t == "result":
        result = o

succeeded = result is not None and not result.get("is_error")
sys.exit(0 if (rejected and not succeeded) else 1)
PY
}

# Extract the API-reported reset time from the LAST rejected rate_limit_event
# in the session log, as epoch-seconds. Ported verbatim from eBull's
# supervisor.sh.
extract_reset_epoch() {
  python3 - "$1" <<'PY'
import json, math, sys, time
from datetime import datetime, timezone

def to_epoch(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if not math.isfinite(x):
            return None
        if x > 1e12:
            x /= 1000.0
        return int(x) if x > 1e9 else None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            x = float(s)
            if math.isfinite(x):
                if x > 1e12:
                    x /= 1000.0
                if x > 1e9:
                    return int(x)
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None
    return None

reset = None
for line in open(sys.argv[1], errors="replace"):
    if '"type"' not in line:
        continue
    try:
        o = json.loads(line)
    except Exception:
        continue
    if o.get("type") != "rate_limit_event":
        continue
    rli = o.get("rate_limit_info") or {}
    if rli.get("status") != "rejected" or rli.get("isUsingOverage"):
        continue
    for k, val in rli.items():
        kl = k.lower()
        if kl in ("retryafter", "retry_after"):
            secs = None
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                secs = float(val)
            elif isinstance(val, str):
                try:
                    secs = float(val.strip())
                except ValueError:
                    secs = None
            if secs is not None and math.isfinite(secs):
                reset = int(time.time() + secs)
        elif "reset" in kl:
            e = to_epoch(val)
            if e is not None:
                reset = e

if reset is not None:
    print(reset)
PY
}

# Normalize into the supervisor's outcome contract: prints exactly one of
# "success" | "usage_limit <epoch>" | "usage_limit" | "error". Does NOT
# persist the reset epoch -- that is supervisor.sh's job (see the pack-seam
# spec's "Split of responsibility for the reset-epoch invariant" note).
agent_classify_outcome() {
  local log_file="$1" exit_code="$2"
  if is_usage_limit_hit "$log_file"; then
    local epoch; epoch="$(extract_reset_epoch "$log_file")"
    if [ -n "$epoch" ]; then echo "usage_limit $epoch"; else echo "usage_limit"; fi
    return 0
  fi
  if [ "$exit_code" -eq 0 ]; then echo "success"; return 0; fi
  echo "error"
  return 0
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
bash tests/test_usage_limit_reset.sh
```
Expected: `ALL PASS`.

- [ ] **Step 5: shellcheck**

```bash
shellcheck -S warning bin/agents/claude.sh
```
Expected: no output (clean).

- [ ] **Step 6: Commit**

```bash
git add bin/agents/claude.sh tests/test_usage_limit_reset.sh
git commit -m "feat: add claude agent adapter (agent_invoke, agent_classify_outcome)"
git push
```

---

### Task 4: `doctor.sh` — fast preflight check + full readiness report

**Files:**
- Create: `bin/doctor.sh`
- Test: `tests/test_doctor.sh`

**Interfaces:**
- Consumes: `python3 lib/config_parser.py` (Task 2); `board_resolve_project` from `bin/board.sh`
  (Task 5) — **this creates a forward dependency**: `doctor_full_report` sources `bin/board.sh`, so
  Task 4's full-report code path isn't exercised until Task 5 exists. `doctor_preflight_check` (used
  by Task 8's `supervisor.sh` and tested here) has no such dependency and is fully testable now.
- Produces: `doctor_preflight_check(target_repo) -> exit 0/1` (fast, local-only: `.autonomy/`
  present + parses, `.claude/CLAUDE.md` present if `engine.requires_claude_md: true`).
  `doctor_full_report(target_repo) -> exit 0/1`, prints a human-readable checklist (adds the
  network-calling checks — implemented fully once Task 5 lands `board_resolve_project`, but the
  function skeleton with its non-board checks is written now).

- [ ] **Step 1: Write the failing test (for `doctor_preflight_check` only — the fast, dependency-free half)**

```bash
# tests/test_doctor.sh
#!/usr/bin/env bash
# Unit test for doctor.sh's fast, local-only preflight check.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$HERE/../bin/doctor.sh"

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

check "missing .autonomy/ -> hard fail" "1" "$(doctor_preflight_check "$tmp" >/dev/null 2>&1; echo $?)"

mkdir -p "$tmp/.autonomy"
cat > "$tmp/.autonomy/config.yaml" <<'YAML'
engine:
  requires_claude_md: false
YAML
check "valid config, requires_claude_md false -> pass" "0" "$(doctor_preflight_check "$tmp" >/dev/null 2>&1; echo $?)"

cat > "$tmp/.autonomy/config.yaml" <<'YAML'
engine:
  requires_claude_md: true
YAML
check "requires_claude_md true, no CLAUDE.md -> hard fail" "1" "$(doctor_preflight_check "$tmp" >/dev/null 2>&1; echo $?)"

mkdir -p "$tmp/.claude"
touch "$tmp/.claude/CLAUDE.md"
check "requires_claude_md true, CLAUDE.md present -> pass" "0" "$(doctor_preflight_check "$tmp" >/dev/null 2>&1; echo $?)"

echo "this line has no colon whatsoever" > "$tmp/.autonomy/config.yaml"
check "malformed config.yaml -> hard fail" "1" "$(doctor_preflight_check "$tmp" >/dev/null 2>&1; echo $?)"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
```

- [ ] **Step 2: Run to verify it fails**

```bash
chmod +x tests/test_doctor.sh
bash tests/test_doctor.sh
```
Expected: fails (`bin/doctor.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/doctor.sh`**

```bash
#!/usr/bin/env bash
# bin/doctor.sh -- diagnostic readiness check for a target repo. Two entry
# points:
#   doctor_preflight_check <target-repo>  -- fast, local-only, called by
#     supervisor.sh on every loop iteration. Hard-fails only on what would
#     actually break the loop.
#   doctor_full_report <target-repo>      -- the full report (adds network
#     calls: gh auth scopes, review-bot workflow, GH Projects v2 board,
#     branch protection). Diagnostic/read-only -- never provisions anything.
#
# Run standalone:  bin/doctor.sh <target-repo>
set -uo pipefail
DOCTOR_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

doctor_preflight_check() {
  local repo="$1"
  if [ ! -f "$repo/.autonomy/config.yaml" ]; then
    echo "doctor: FAIL -- $repo/.autonomy/config.yaml not found" >&2
    return 1
  fi
  if ! python3 "$DOCTOR_HOME/lib/config_parser.py" "$repo/.autonomy/config.yaml" __validate__ >/dev/null 2>&1; then
    echo "doctor: FAIL -- $repo/.autonomy/config.yaml does not parse" >&2
    return 1
  fi
  local requires_md
  requires_md="$(python3 "$DOCTOR_HOME/lib/config_parser.py" "$repo/.autonomy/config.yaml" engine.requires_claude_md 2>/dev/null || echo false)"
  if [ "$requires_md" = "true" ] && [ ! -f "$repo/.claude/CLAUDE.md" ]; then
    echo "doctor: FAIL -- engine.requires_claude_md is true but $repo/.claude/CLAUDE.md is missing" >&2
    return 1
  fi
  return 0
}

doctor_full_report() {
  local repo="$1" hard_fail=0
  echo "== doctor.sh report: $repo =="

  if doctor_preflight_check "$repo" 2>/tmp/doctor_preflight_err.$$; then
    echo "OK   .autonomy/ present, config.yaml valid"
  else
    cat /tmp/doctor_preflight_err.$$
    hard_fail=1
  fi
  rm -f /tmp/doctor_preflight_err.$$

  if [ -f "$repo/.claude/CLAUDE.md" ]; then
    echo "OK   .claude/CLAUDE.md present"
  else
    local requires_md
    requires_md="$(python3 "$DOCTOR_HOME/lib/config_parser.py" "$repo/.autonomy/config.yaml" engine.requires_claude_md 2>/dev/null || echo false)"
    if [ "$requires_md" != "true" ]; then
      echo "WARN .claude/CLAUDE.md not found -- run /init in Claude Code, or use the claude-md-management:claude-md-improver skill"
    fi
  fi

  local strategy
  strategy="$(python3 "$DOCTOR_HOME/lib/config_parser.py" "$repo/.autonomy/config.yaml" merge_gate.strategy 2>/dev/null || echo manual)"
  strategy="${strategy:-manual}"
  if [ "$strategy" = "bot_comment" ]; then
    if [ -d "$repo/.github/workflows" ] && grep -rlE 'anthropic\.com/v1/messages|ANTHROPIC_API_KEY' "$repo/.github/workflows" >/dev/null 2>&1; then
      echo "OK   review-bot workflow found under .github/workflows (merge_gate.strategy=bot_comment)"
    else
      echo "WARN no review-bot workflow found under .github/workflows -- merge_gate.strategy=bot_comment will never see an APPROVE and every PR will stall. Add a workflow, or switch to manual/ci_only."
    fi
  fi

  if (cd "$repo" && gh auth status >/dev/null 2>&1); then
    echo "OK   gh auth status ok"
  else
    echo "WARN gh auth status failed -- run 'gh auth login' (need repo + project scopes)"
  fi

  local owner project_title
  owner="$(python3 "$DOCTOR_HOME/lib/config_parser.py" "$repo/.autonomy/config.yaml" board.owner 2>/dev/null || echo)"
  project_title="$(python3 "$DOCTOR_HOME/lib/config_parser.py" "$repo/.autonomy/config.yaml" board.project_title 2>/dev/null || echo)"
  if [ -n "$owner" ] && [ -n "$project_title" ]; then
    # shellcheck source=/dev/null
    source "$DOCTOR_HOME/bin/board.sh"
    ids="$(board_resolve_project "$owner" "$project_title")"
    read -r pid _ _ <<<"$ids"
    if [ -n "$pid" ]; then
      echo "OK   board '$project_title' found under '$owner'"
    else
      echo "WARN GitHub Projects v2 board '$project_title' not found under '$owner' -- board.sh will silently skip status updates"
    fi
  else
    echo "WARN board.owner/board.project_title not set in config.yaml -- board status updates will be skipped"
  fi

  if (cd "$repo" && gh api "repos/{owner}/{repo}/branches/main/protection" >/dev/null 2>&1); then
    echo "OK   branch protection configured on main"
  else
    echo "WARN no branch protection detected on main -- safe_merge.sh is the *local* gate only; consider adding required status checks"
  fi

  return "$hard_fail"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  TARGET="${1:?usage: doctor.sh <target-repo>}"
  doctor_full_report "$(cd "$TARGET" && pwd)"
  exit $?
fi
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
bash tests/test_doctor.sh
```
Expected: `ALL PASS`.

- [ ] **Step 5: shellcheck**

```bash
shellcheck -S warning bin/doctor.sh
```
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add bin/doctor.sh tests/test_doctor.sh
git commit -m "feat: add doctor.sh fast preflight check + full report skeleton"
git push
```

**Note for Task 5:** `doctor_full_report`'s board check (`source bin/board.sh`) will fail with "No
such file" until Task 5 lands. This is fine — Task 4's test only exercises `doctor_preflight_check`,
which has no such dependency. Task 5 makes `doctor_full_report` fully runnable.

---

### Task 5: `board.sh` — generic board updater (user/org auto-detect)

**Files:**
- Create: `bin/board.sh`
- Test: `tests/test_board_resolve.sh`

**Interfaces:**
- Consumes: `python3 lib/config_parser.py` (Task 2); reads `board.owner`/`board.project_title` from
  `.autonomy/config.yaml` in the CURRENT WORKING DIRECTORY when run standalone (matches today's
  convention of running from the target repo checkout).
- Produces: `board_resolve_project(owner, project_title, [want_status]) -> prints "<project_id>
  <status_field_id> <option_id>"` (fields may be empty if not found) — this is what Task 4's
  `doctor_full_report` sources and calls, and what this file's own CLI body uses internally.

- [ ] **Step 1: Write the failing test (mocking `gh` as a shell function)**

```bash
# tests/test_board_resolve.sh
#!/usr/bin/env bash
# Unit test for board.sh's board_resolve_project -- user-then-org fallback.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$HERE/../bin/board.sh"

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

USER_RESPONSE='{}'
ORG_RESPONSE='{}'
gh() {
  # $1=api $2=graphql -f query=... -f o=...
  if printf '%s' "$*" | grep -q 'organization(login'; then
    printf '%s' "$ORG_RESPONSE"
  else
    printf '%s' "$USER_RESPONSE"
  fi
}

USER_RESPONSE='{"data":{"user":{"projectsV2":{"nodes":[{"id":"PID_USER","title":"eBull engineering board","field":{"id":"FID_USER","options":[{"id":"OPT1","name":"In Progress"}]}}]}}}}'
ids="$(board_resolve_project "Luke-Bradford" "eBull engineering board" "In Progress")"
check "user-owned project found directly" "PID_USER FID_USER OPT1" "$ids"

USER_RESPONSE='{"data":{"user":{"projectsV2":{"nodes":[]}}}}'
ORG_RESPONSE='{"data":{"organization":{"projectsV2":{"nodes":[{"id":"PID_ORG","title":"org board","field":{"id":"FID_ORG","options":[]}}]}}}}'
ids="$(board_resolve_project "some-org" "org board" "")"
check "falls back to organization when user has no match" "PID_ORG FID_ORG " "$ids"

USER_RESPONSE='{"data":{"user":{"projectsV2":{"nodes":[]}}}}'
ORG_RESPONSE='{"data":{"organization":null}}'
ids="$(board_resolve_project "nobody" "nothing" "")"
check "neither user nor org match -> empty" "" "$(printf '%s' "$ids" | tr -d ' ')"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
```

- [ ] **Step 2: Run to verify it fails**

```bash
chmod +x tests/test_board_resolve.sh
bash tests/test_board_resolve.sh
```
Expected: fails (`bin/board.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/board.sh`**

```bash
#!/usr/bin/env bash
# bin/board.sh -- generic GitHub Projects v2 board updater. Reads
# board.owner / board.project_title from the target repo's
# .autonomy/config.yaml. Uses the ambient `gh` auth (must carry the `project`
# scope) -- no PAT, no Action, no repo secret.
#
# Usage (run FROM the target repo checkout):
#   board.sh status <issue#> "<Status>"
#   board.sh add    <issue#>
#
# BEST-EFFORT BY DESIGN: board upkeep must NEVER block engineering work. Every
# failure path warns to stderr and exits 0.
set -uo pipefail
BOARD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

warn() { echo "board.sh: $*" >&2; }

# Resolve a GitHub Projects v2 project's node ID + Status field id + option
# id for $3 (want_status), trying a user-owned project first, then an
# org-owned one (Codex strategic-fit finding -- today's eBull-only version
# assumed user()). Prints "<project_id> <status_field_id> <option_id>" or
# nothing if no project with that title is found under either shape.
board_resolve_project() {
  local owner="$1" project_title="$2" want_status="${3:-}"
  local meta
  meta="$(gh api graphql -f query='
    query($o:String!){ user(login:$o){ projectsV2(first:30){ nodes{
      id title
      field(name:"Status"){ ... on ProjectV2SingleSelectField{ id options{ id name } } }
    }}}}' -f o="$owner" 2>/dev/null)" || return 1

  local ids
  ids="$(PROJECT_TITLE="$project_title" STATUS="$want_status" python3 - "$meta" <<'PY' 2>/dev/null
import sys, json, os
t = os.environ["PROJECT_TITLE"]; want = os.environ.get("STATUS", "")
d = json.loads(sys.argv[1])
proj = next((n for n in d["data"]["user"]["projectsV2"]["nodes"] if n["title"] == t), None)
if not proj:
    print(); sys.exit(0)
f = proj.get("field") or {}
oid = ""
for o in (f.get("options") or []):
    if o["name"].lower() == want.lower():
        oid = o["id"]
print(proj["id"], f.get("id", ""), oid)
PY
)"
  if [ -z "${ids// /}" ]; then
    meta="$(gh api graphql -f query='
      query($o:String!){ organization(login:$o){ projectsV2(first:30){ nodes{
        id title
        field(name:"Status"){ ... on ProjectV2SingleSelectField{ id options{ id name } } }
      }}}}' -f o="$owner" 2>/dev/null)" || return 1
    ids="$(PROJECT_TITLE="$project_title" STATUS="$want_status" python3 - "$meta" <<'PY' 2>/dev/null
import sys, json, os
t = os.environ["PROJECT_TITLE"]; want = os.environ.get("STATUS", "")
d = json.loads(sys.argv[1])
org = (d.get("data") or {}).get("organization")
if not org:
    print(); sys.exit(0)
proj = next((n for n in org["projectsV2"]["nodes"] if n["title"] == t), None)
if not proj:
    print(); sys.exit(0)
f = proj.get("field") or {}
oid = ""
for o in (f.get("options") or []):
    if o["name"].lower() == want.lower():
        oid = o["id"]
print(proj["id"], f.get("id", ""), oid)
PY
)"
  fi
  printf '%s' "$ids"
}

[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0

cmd="${1:-}"; issue="${2:-}"; status="${3:-}"
if [ -z "$cmd" ] || [ -z "$issue" ]; then
  warn 'usage: board.sh status <issue#> "<Status>" | add <issue#>'
  exit 0
fi

OWNER="$(python3 "$BOARD_HOME/lib/config_parser.py" .autonomy/config.yaml board.owner 2>/dev/null || echo)"
PROJECT_TITLE="$(python3 "$BOARD_HOME/lib/config_parser.py" .autonomy/config.yaml board.project_title 2>/dev/null || echo)"
if [ -z "$OWNER" ] || [ -z "$PROJECT_TITLE" ]; then
  warn "board.owner/board.project_title not set in .autonomy/config.yaml (skip)"; exit 0
fi

ids="$(board_resolve_project "$OWNER" "$PROJECT_TITLE" "$status")" || { warn "board metadata query failed (skip)"; exit 0; }
read -r PID FID OPT_ID <<<"$ids"
if [ -z "${PID:-}" ]; then warn "project '$PROJECT_TITLE' not found under '$OWNER' (skip)"; exit 0; fi

NID="$(gh issue view "$issue" --json id --jq .id 2>/dev/null)" || { warn "issue #$issue not found (skip)"; exit 0; }
if [ -z "$NID" ]; then warn "issue #$issue not found (skip)"; exit 0; fi

ITEM="$(gh api graphql -f query='query($n:ID!){node(id:$n){... on Issue{projectItems(first:20){nodes{id project{id}}}}}}' -f n="$NID" 2>/dev/null \
  | PID="$PID" python3 -c 'import sys,json,os; d=json.load(sys.stdin); p=os.environ["PID"]; ns=d["data"]["node"]["projectItems"]["nodes"]; print(next((i["id"] for i in ns if i["project"]["id"]==p), ""))' 2>/dev/null)"

if [ -z "${ITEM:-}" ]; then
  ITEM="$(gh api graphql -f query='mutation($p:ID!,$c:ID!){addProjectV2ItemById(input:{projectId:$p,contentId:$c}){item{id}}}' -f p="$PID" -f c="$NID" --jq '.data.addProjectV2ItemById.item.id' 2>/dev/null)"
  if [ -z "${ITEM:-}" ]; then warn "could not add #$issue to board (skip)"; exit 0; fi
  warn "added #$issue to board"
fi

if [ "$cmd" = "add" ]; then exit 0; fi

if [ "$cmd" = "status" ]; then
  if [ -z "${FID:-}" ]; then warn "Status field not found (skip)"; exit 0; fi
  if [ -z "${OPT_ID:-}" ]; then warn "status '$status' is not a board column (skip)"; exit 0; fi
  if gh api graphql -f query='mutation($p:ID!,$i:ID!,$f:ID!,$o:String!){updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,value:{singleSelectOptionId:$o}}){projectV2Item{id}}}' -f p="$PID" -f i="$ITEM" -f f="$FID" -f o="$OPT_ID" >/dev/null 2>&1; then
    warn "#$issue -> $status"
  else
    warn "failed to set #$issue status (skip)"
  fi
  exit 0
fi

warn "unknown command '$cmd' (skip)"
exit 0
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
bash tests/test_board_resolve.sh
```
Expected: `ALL PASS`.

- [ ] **Step 5: Re-run Task 4's test to confirm the forward dependency is now satisfied**

```bash
bash tests/test_doctor.sh
```
Expected: still `ALL PASS` (unchanged — `test_doctor.sh` doesn't exercise `doctor_full_report`, but
this confirms nothing broke).

- [ ] **Step 6: shellcheck**

```bash
shellcheck -S warning bin/board.sh
```
Expected: no output.

- [ ] **Step 7: Commit**

```bash
git add bin/board.sh tests/test_board_resolve.sh
git commit -m "feat: add generic board.sh with user/org auto-detect"
git push
```

---

### Task 6: `safe_merge.sh` — generic merge gate (4 strategies)

**Files:**
- Create: `bin/safe_merge.sh`
- Test: `tests/test_safe_merge_doc_only.sh`
- Test: `tests/test_merge_gate_strategies.sh`

**Interfaces:**
- Consumes: `python3 lib/config_parser.py` (Task 2); `bin/unblock_dependents.sh` (Task 7 — called at
  the end of a successful merge; write this task assuming it exists, land Task 7 immediately after).
- Produces: CLI `bin/safe_merge.sh <pr-number>` (run from the target repo checkout) — the only
  merge path the loop is allowed to use. Also defines `is_doc_only(files, extensions_csv)` and
  `ci_check(pr, strategy)` as testable functions.

- [ ] **Step 1: Write the failing tests**

```bash
# tests/test_safe_merge_doc_only.sh
#!/usr/bin/env bash
# Unit test for safe_merge.sh::is_doc_only(), parameterized by extension list.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/../bin/safe_merge.sh"

fails=0
check() {
  local want="$1" desc="$2" files="$3" exts="$4" got
  if is_doc_only "$files" "$exts"; then got=doc; else got=strict; fi
  if [ "$got" = "$want" ]; then echo "ok   - $desc"; else
    echo "FAIL - $desc (expected '$want', got '$got')"; fails=$((fails + 1)); fi
}

check doc    "single .md"                      "docs/a.md"                            ".md"
check doc    "multiple .md"                     $'docs/a.md\ndocs/b.md'                 ".md"
check doc    "nested .md paths"                 $'README.md\ndocs/specs/ui/x.md'        ".md"
check strict "one code file among md disqualifies" $'docs/a.md\napp/x.py'               ".md"
check strict "code file alone"                  "app/services/scoring.py"               ".md"
check strict "favicon PR (svg + html)"          $'frontend/index.html\nfrontend/public/favicon.svg' ".md"
check strict "empty diff"                       ""                                      ".md"
check strict ".md as a directory, not extension" "docs/readme.md/thing.py"              ".md"
check strict "non-md extension that contains md" "docs/x.mdx"                           ".md"
check strict ".rst not in configured list"       "docs/a.rst"                            ".md"
check doc    ".rst IS in configured list"        "docs/a.rst"                            ".md,.rst"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails FAILED"; exit 1; fi
```

```bash
# tests/test_merge_gate_strategies.sh
#!/usr/bin/env bash
# Unit tests for safe_merge.sh's ci_check -- the fail-safe fix (Codex finding:
# a gh API failure must never look identical to "green").
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/../bin/safe_merge.sh"

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

MOCK_CHECKS_JSON=""
gh() {
  if [ "$1" = "pr" ] && [ "$2" = "checks" ]; then
    if [ "$MOCK_CHECKS_JSON" = "__FAIL__" ]; then return 1; fi
    echo "$MOCK_CHECKS_JSON"
    return 0
  fi
  echo "unmocked gh call: $*" >&2
  return 1
}

MOCK_CHECKS_JSON='[{"name":"lint","state":"SUCCESS"}]'
check "all green -> ci_check passes" "0" "$(ci_check 1 bot_comment >/dev/null 2>&1; echo $?)"

MOCK_CHECKS_JSON='[{"name":"lint","state":"FAILURE"}]'
check "a failing check -> refuse" "1" "$(ci_check 1 bot_comment >/dev/null 2>&1; echo $?)"

MOCK_CHECKS_JSON='[{"name":"lint","state":"PENDING"}]'
check "a pending check -> refuse" "1" "$(ci_check 1 bot_comment >/dev/null 2>&1; echo $?)"

MOCK_CHECKS_JSON='[]'
check "zero checks, ci_only -> refuse" "1" "$(ci_check 1 ci_only >/dev/null 2>&1; echo $?)"
check "zero checks, bot_comment -> pass (approval is the real gate)" "0" "$(ci_check 1 bot_comment >/dev/null 2>&1; echo $?)"

MOCK_CHECKS_JSON="__FAIL__"
check "gh call itself fails -> refuse, not silently green" "1" "$(ci_check 1 ci_only >/dev/null 2>&1; echo $?)"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails FAILED"; exit 1; fi
```

- [ ] **Step 2: Run both to verify they fail**

```bash
chmod +x tests/test_safe_merge_doc_only.sh tests/test_merge_gate_strategies.sh
bash tests/test_safe_merge_doc_only.sh
bash tests/test_merge_gate_strategies.sh
```
Expected: both fail (`bin/safe_merge.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/safe_merge.sh`**

```bash
#!/usr/bin/env bash
# bin/safe_merge.sh -- generic mechanical merge gate. Refuses to merge unless
# the target repo's .autonomy/config.yaml merge_gate.strategy is satisfied on
# the PR's LATEST commit. Run FROM the target repo checkout.
#
# Usage: safe_merge.sh <pr-number>
set -euo pipefail
SAFE_MERGE_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Doc-only predicate, parameterized by the strategy's configured extension
# list (comma-separated, e.g. ".md,.rst"). Pure string logic, unit-tested.
is_doc_only() {
  local files="$1" extensions_csv="$2"
  [ -n "$files" ] || return 1
  local ext pattern="" IFS=','
  read -ra exts <<<"$extensions_csv"
  for ext in "${exts[@]}"; do
    ext="$(printf '%s' "$ext" | sed 's/^\.//')"
    if [ -n "$pattern" ]; then pattern="$pattern|"; fi
    pattern="${pattern}\\.${ext}\$"
  done
  ! printf '%s\n' "$files" | grep -qvE "$pattern"
}

# CI check, generalized (Codex finding: a `gh` API failure must never look
# identical to "green"). Returns 0 = green, 1 = refuse.
ci_check() {
  local pr="$1" strategy="$2"
  local checks_json
  if ! checks_json="$(gh pr checks "$pr" --json name,state 2>/dev/null)"; then
    echo "safe_merge: REFUSE -- cannot verify CI state (gh pr checks failed) -- refusing rather than assuming green" >&2
    return 1
  fi
  if echo "$checks_json" | grep -qiE '"state":"(fail|failure|error|cancelled|timed_out)"'; then
    echo "safe_merge: REFUSE -- a CI check failed on #$pr" >&2
    return 1
  fi
  if echo "$checks_json" | grep -qiE '"state":"(pending|queued|in_progress)"'; then
    echo "safe_merge: REFUSE -- CI still running on #$pr (re-check later)" >&2
    return 1
  fi
  if [ "$strategy" = "ci_only" ] && [ "$checks_json" = "[]" ]; then
    echo "safe_merge: REFUSE -- ci_only requires at least one configured check; use manual for a repo with no CI, or add one" >&2
    return 1
  fi
  return 0
}

merge_gate_bot_comment() {
  local pr="$1" author_login="$2" marker="$3" doc_only_extensions="$4"
  local head_time; head_time="$(gh pr view "$pr" --json commits -q '.commits[-1].committedDate')"
  [ -n "$head_time" ] || { echo "safe_merge: cannot resolve PR #$pr head commit time" >&2; return 1; }

  local files n_listed n_changed
  files="$(gh api --paginate "repos/{owner}/{repo}/pulls/$pr/files" --jq '.[].filename')"
  n_listed="$(printf '%s\n' "$files" | grep -c . || true)"
  n_changed="$(gh pr view "$pr" --json changedFiles -q '.changedFiles')"
  if [ "$n_listed" = "$n_changed" ] && is_doc_only "$files" "$doc_only_extensions"; then
    local doc_block
    doc_block="$(gh pr view "$pr" --json comments -q \
      "[.comments[] | select(.author.login==\"$author_login\" and (.body|contains(\"$marker\")))]
       | sort_by(.createdAt) | last | .body // \"\"")"
    if printf '%s' "$doc_block" | grep -qiE 'REQUEST CHANGES|\[BLOCKING\]|must fix before merge'; then
      echo "safe_merge: REFUSE -- doc-only PR #$pr but latest bot comment blocks" >&2
      return 1
    fi
    echo "safe_merge: doc-only PR #$pr (every changed file matches doc_only_extensions), CI green, no blocking comment -- merging."
    return 0
  fi

  local latest
  latest="$(gh pr view "$pr" --json comments -q \
    "[.comments[] | select(.author.login==\"$author_login\" and (.body|contains(\"$marker\")))]
     | sort_by(.createdAt) | last")"
  [ -n "$latest" ] && [ "$latest" != "null" ] || {
    echo "safe_merge: REFUSE -- no review comment from $author_login on #$pr yet" >&2; return 1; }
  local review_time review_body
  review_time="$(printf '%s' "$latest" | python3 -c 'import sys,json;print(json.load(sys.stdin)["createdAt"])')"
  review_body="$(printf '%s' "$latest" | python3 -c 'import sys,json;print(json.load(sys.stdin)["body"])')"

  if [[ "$review_time" < "$head_time" ]]; then
    echo "safe_merge: REFUSE -- latest review ($review_time) predates head commit ($head_time); push reset the gate" >&2
    return 1
  fi
  if printf '%s' "$review_body" | grep -qiE 'REQUEST CHANGES|\[BLOCKING\]|must fix before merge'; then
    echo "safe_merge: REFUSE -- latest review requests changes / has blocking findings" >&2
    return 1
  fi
  if ! printf '%s' "$review_body" | grep -qiE 'APPROVE'; then
    echo "safe_merge: REFUSE -- latest review is not an APPROVE" >&2
    return 1
  fi
  echo "safe_merge: gates pass on #$pr (review $review_time >= head $head_time) -- merging."
  return 0
}

merge_gate_gh_review() {
  local pr="$1" reviewer_login="$2"
  [ -n "$reviewer_login" ] || { echo "safe_merge: REFUSE -- merge_gate.strategy=gh_review but reviewer_login is not set in config.yaml" >&2; return 1; }
  local head_time; head_time="$(gh pr view "$pr" --json commits -q '.commits[-1].committedDate')"
  [ -n "$head_time" ] || { echo "safe_merge: cannot resolve PR #$pr head commit time" >&2; return 1; }

  local latest
  latest="$(gh pr view "$pr" --json reviews -q \
    "[.reviews[] | select(.author.login==\"$reviewer_login\")] | sort_by(.submittedAt) | last")"
  [ -n "$latest" ] && [ "$latest" != "null" ] || {
    echo "safe_merge: REFUSE -- no review from $reviewer_login on #$pr yet" >&2; return 1; }
  local review_time review_state
  review_time="$(printf '%s' "$latest" | python3 -c 'import sys,json;print(json.load(sys.stdin)["submittedAt"])')"
  review_state="$(printf '%s' "$latest" | python3 -c 'import sys,json;print(json.load(sys.stdin)["state"])')"

  if [[ "$review_time" < "$head_time" ]]; then
    echo "safe_merge: REFUSE -- latest review from $reviewer_login ($review_time) predates head commit ($head_time)" >&2
    return 1
  fi
  if [ "$review_state" != "APPROVED" ]; then
    echo "safe_merge: REFUSE -- latest review from $reviewer_login is '$review_state', not APPROVED" >&2
    return 1
  fi
  echo "safe_merge: gates pass on #$pr ($reviewer_login APPROVED at $review_time >= head $head_time) -- merging."
  return 0
}

[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0

PR="${1:?usage: safe_merge.sh <pr-number>}"
CONFIG_GET() { python3 "$SAFE_MERGE_HOME/lib/config_parser.py" .autonomy/config.yaml "$1" 2>/dev/null; }

STRATEGY="$(CONFIG_GET merge_gate.strategy)"; STRATEGY="${STRATEGY:-manual}"

if [ "$STRATEGY" = "manual" ]; then
  echo "safe_merge: manual-mode -- PR #$PR left open for the operator to review/merge."
  exit 0
fi

ci_check "$PR" "$STRATEGY" || exit 1

case "$STRATEGY" in
  ci_only)
    echo "safe_merge: CI green, ci_only strategy -- merging #$PR."
    ;;
  bot_comment)
    author_login="$(CONFIG_GET merge_gate.author_login)"; author_login="${author_login:-github-actions}"
    marker="$(CONFIG_GET merge_gate.marker)"; marker="${marker:-Claude Code Review}"
    doc_only_extensions="$(CONFIG_GET merge_gate.doc_only_extensions | paste -sd, -)"; doc_only_extensions="${doc_only_extensions:-.md}"
    merge_gate_bot_comment "$PR" "$author_login" "$marker" "$doc_only_extensions" || exit 1
    ;;
  gh_review)
    reviewer_login="$(CONFIG_GET merge_gate.reviewer_login)"
    merge_gate_gh_review "$PR" "$reviewer_login" || exit 1
    ;;
  *)
    echo "safe_merge: REFUSE -- unknown merge_gate.strategy '$STRATEGY' in config.yaml" >&2
    exit 1
    ;;
esac

gh pr merge "$PR" --squash --delete-branch
"$SAFE_MERGE_HOME/bin/unblock_dependents.sh" "$PR" || true
```

- [ ] **Step 4: Run both tests to verify they pass**

```bash
bash tests/test_safe_merge_doc_only.sh
bash tests/test_merge_gate_strategies.sh
```
Expected: both `ALL PASS`.

**Note:** `merge_gate_bot_comment` and `merge_gate_gh_review` (the multi-`gh`-call strategy bodies)
are deliberately NOT unit-tested end-to-end here — mocking their full multi-call `gh` sequences adds
disproportionate bash-test-harness complexity for the value versus the two functions already
covered (`is_doc_only`, `ci_check`, which carry the actual new/changed logic this spec introduces).
The `bot_comment` path (eBull's real, already-proven mechanism) is validated by Task 13's acceptance
run against a real eBull PR; `gh_review` isn't exercised against any real repo in this spec.

- [ ] **Step 5: shellcheck**

```bash
shellcheck -S warning bin/safe_merge.sh
```
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add bin/safe_merge.sh tests/test_safe_merge_doc_only.sh tests/test_merge_gate_strategies.sh
git commit -m "feat: add generic safe_merge.sh with 4 merge-gate strategies"
git push
```

---

### Task 7: `unblock_dependents.sh` — verbatim port

**Files:**
- Create: `bin/unblock_dependents.sh`
- Test: `tests/test_unblock_dependents.sh`

**Interfaces:**
- Consumes: nothing (pure `gh`-driven script, already fully repo-agnostic today).
- Produces: CLI `bin/unblock_dependents.sh <merged-pr-number>` — called by `safe_merge.sh` (Task 6)
  after every successful merge. Defines `blocker_clauses_of`, `confirms_block`, `extract_blockers`
  as testable pure functions.

- [ ] **Step 1: Write the failing test (ported verbatim from eBull's `test_unblock_dependents.sh`)**

```bash
# tests/test_unblock_dependents.sh
#!/usr/bin/env bash
# Unit test for unblock_dependents.sh pure matchers. Sources the REAL script
# (its gh-driven body is guarded by a BASH_SOURCE==$0 check, so sourcing only
# defines the helpers) and table-tests the regexes that decide whether a
# ticket is "blocked by #X" and which other blockers remain. No gh, no network.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/../bin/unblock_dependents.sh"

fails=0
judge() {
  if [ "$1" = "$2" ]; then echo "ok   - $3"; else
    echo "FAIL - $3 (expected '$2', got '$1')"; fails=$((fails + 1)); fi
}

B1822='Part of #1815. **Blocked by** #1820 (P0 foundation) + P2 analytics -- the signals must be computed and stored before they can be backtested.'
B1815=$'> | P2 -- new signals | #1823 | blocked by #1820 |\n> | P5a -- backtest harness | #1822 | blocked by #1820 + #1823 |'

check_confirm() {
  local want="$1" desc="$2" body="$3" x="$4" got
  if confirms_block "$body" "$x"; then got=yes; else got=no; fi
  judge "$got" "$want" "$desc"
}

check_confirm yes "plain blocked by"            "Blocked by #1820"                       1820
check_confirm yes "markdown-bold blocked by"    "Part of #1815. **Blocked by** #1820 (P0)" 1820
check_confirm yes "hyphenated blocked-by"       "blocked-by #843"                         843
check_confirm yes "trailing punctuation"        "Blocked by #1820."                       1820
check_confirm yes "one of several blockers"     "Blocked by #1820 and #1823"              1823
check_confirm no  "prefix-digit must not match" "Blocked by #1820"                        182
check_confirm no  "suffix-digit must not match" "Blocked by #182"                         1820
check_confirm no  "mention outside blocked line" $'Implements #1820\nsee notes'           1820
check_confirm no  "no blocked-by line at all"    "Part of #1815. Closes #1820."           1820
check_confirm no  "parent (Part of #X) not a block"  "$B1822"                             1815
check_confirm yes "real blocker after the phrase"    "$B1822"                             1820
check_confirm no  "UPPERCASE: parent not a block"    "Part of #1815. **BLOCKED BY** #1820" 1815
check_confirm yes "UPPERCASE: real blocker"          "Part of #1815. **BLOCKED BY** #1820" 1820
check_confirm no  "table row-subject (only) not a block" "| P2 | #1823 | blocked by #1820 |" 1823
check_confirm yes "#1815: #1823 is a real P5a blocker"   "$B1815"                            1823
check_confirm yes "#1815 table: #1820 blocks every row"  "$B1815"                            1820

check_extract() {
  local want="$1" desc="$2" body="$3" got
  got="$(extract_blockers "$body" | tr '\n' ' ' | sed 's/ *$//')"
  judge "$got" "$want" "$desc"
}

check_extract "1820"      "single blocker"            "Blocked by #1820 (P0 foundation)"
check_extract "1820 1823" "two blockers, sorted"      "Blocked by #1823 and #1820"
check_extract "843"       "ignores non-blocked refs"  $'Part of #1815. Closes #999.\nBlocked by #843'
check_extract ""          "no blocked-by line"        "Part of #1815. Closes #1820."
check_extract "1820"      "#1822: parent #1815 excluded" "$B1822"
check_extract "1820 1823" "#1815 P5a: subject excluded"  "$B1815"
check_extract "1820"      "UPPERCASE: parent excluded"   "Part of #1815. **BLOCKED BY** #1820"

if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails FAILED"; exit 1; fi
```

- [ ] **Step 2: Run to verify it fails**

```bash
chmod +x tests/test_unblock_dependents.sh
bash tests/test_unblock_dependents.sh
```
Expected: fails (`bin/unblock_dependents.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/unblock_dependents.sh`** (verbatim port of eBull's
  `scripts/autonomy/unblock_dependents.sh` — already fully repo-agnostic, no changes needed beyond
  its new location)

```bash
#!/usr/bin/env bash
# bin/unblock_dependents.sh -- post-merge dependent notifier.
#
# When a PR merges and closes issue #X, any open ticket whose body says
# "Blocked by #X" is surfaced here. DELIBERATELY NOTIFY-ONLY -- it does NOT
# move board cards or edit issue bodies (a full-population scan falsified the
# naive "strip the block line + move to Todo" approach: some issues match the
# phrase yet are not actually unblocked, e.g. a parent-issue table listing).
#
# BEST-EFFORT BY DESIGN: this runs AFTER the merge already happened (called
# from safe_merge.sh). It must NEVER fail the caller -- every path warns to
# stderr and exits 0.
#
# Usage:  bin/unblock_dependents.sh <merged-pr-number>
set -uo pipefail

warn() { echo "unblock_dependents: $*" >&2; }

blocker_clauses_of() {
  printf '%s\n' "$1" | grep -iE 'blocked[ -]by' \
    | tr '[:upper:]' '[:lower:]' | sed -E 's/^.*blocked[ -]by//'
}

confirms_block() { blocker_clauses_of "$1" | grep -E "#$2([^0-9]|$)" >/dev/null; }

extract_blockers() { blocker_clauses_of "$1" | grep -oE '#[0-9]+' | tr -d '#' | sort -u; }

[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0

PR="${1:-}"
if [ -z "$PR" ]; then warn 'usage: unblock_dependents.sh <pr-number>'; exit 0; fi

REPO_SLUG="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)"
if [ -z "$REPO_SLUG" ]; then warn "cannot resolve repo slug (skip)"; exit 0; fi

closed="$(gh pr view "$PR" --json closingIssuesReferences \
  -q '.closingIssuesReferences[].number' 2>/dev/null || true)"
if [ -z "$closed" ]; then warn "PR #$PR closed no tracked issues (nothing to do)"; exit 0; fi

issue_is_open() {
  [ "$(gh issue view "$1" --json state -q .state 2>/dev/null || echo CLOSED)" = "OPEN" ]
}

for X in $closed; do
  candidates="$(gh search issues --repo "$REPO_SLUG" --state open "blocked by #$X" \
    --json number -q '.[].number' 2>/dev/null || true)"
  [ -n "$candidates" ] || continue

  for D in $candidates; do
    [ "$D" = "$X" ] && continue

    body="$(gh issue view "$D" --json body -q .body 2>/dev/null || true)"
    [ -n "$body" ] || continue

    confirms_block "$body" "$X" || continue

    marker="<!-- autonomy:unblock-notice blocker=#$X -->"
    if gh api --paginate "repos/{owner}/{repo}/issues/$D/comments" \
        --jq '.[].body' 2>/dev/null | grep -F "$marker" >/dev/null; then
      warn "#$D already notified for blocker #$X (skip)"
      continue
    fi

    others="$(extract_blockers "$body")"
    remaining=""
    for B in $others; do
      { [ "$B" = "$X" ] || [ "$B" = "$D" ]; } && continue
      if issue_is_open "$B"; then remaining="$remaining #$B"; fi
    done

    if [ -n "$remaining" ]; then
      status_line="Still blocked by:$remaining (open)."
    else
      status_line="No other issue-referenced blockers remain -- ready to move to **Todo** if nothing out-of-band blocks it (e.g. infra/decision not tracked by an issue)."
    fi

    comment="🔓 Blocker #$X merged (PR #$PR). $status_line

$marker"
    if gh issue comment "$D" --body "$comment" >/dev/null 2>&1; then
      echo "unblock_dependents: notified #$D (blocker #$X merged; $status_line)"
    else
      warn "failed to comment on #$D (skip)"
    fi
  done
done

exit 0
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
bash tests/test_unblock_dependents.sh
```
Expected: `ALL PASS`.

- [ ] **Step 5: shellcheck**

```bash
shellcheck -S warning bin/unblock_dependents.sh
```
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add bin/unblock_dependents.sh tests/test_unblock_dependents.sh
git commit -m "feat: port unblock_dependents.sh verbatim (already repo-agnostic)"
git push
```

---

### Task 8: `supervisor.sh` — the generic engine loop

**Files:**
- Create: `bin/supervisor.sh`
- Test: `tests/test_preflight_recovery.sh`
- Test: `tests/test_agent_dispatch.sh`

**Interfaces:**
- Consumes: `doctor_preflight_check` (Task 4), `agent_invoke`/`agent_classify_outcome` from
  `bin/agents/${AGENT_TYPE}.sh` (Task 3 for `claude`), `python3 lib/config_parser.py` (Task 2).
- Produces: CLI `bin/supervisor.sh --repo <path> [--agent-type ...] [--model ...]
  [--fallback-model ...] [--label ...]` — the main entry point launchd runs. Defines
  `resolve_config_value(config_file, config_key, cli_override, hardcoded_default)`,
  `preflight()`, `run_session()`, `compute_limit_wait()` as testable functions.

- [ ] **Step 1: Write the failing tests**

```bash
# tests/test_preflight_recovery.sh
#!/usr/bin/env bash
# Scenario test for supervisor.sh preflight() against a throwaway repo.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$HERE/../bin/supervisor.sh"
SUPLOG=/dev/null
log() { :; }

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
origin="$tmp/origin.git"; work="$tmp/work"
git init -q --bare "$origin"
git -c init.defaultBranch=main init -q "$work"
cd "$work" || exit 1
git config user.email "t@t.t"; git config user.name "t"
mkdir -p .autonomy
cat > .autonomy/config.yaml <<'YAML'
engine:
  requires_claude_md: false
YAML
git add .autonomy/config.yaml
git commit -q -m init
git branch -M main
git remote add origin "$origin"
git push -q -u origin main 2>/dev/null

AUTONOMY_TARGET_REPO="$work"
RESET_STATE="$tmp/.last_usage_reset"

dirty_skips=0
preflight; rc=$?
check "clean tree proceeds" 0 "$rc"
check "clean tree leaves counter 0" 0 "$dirty_skips"
check "preflight detaches HEAD (no branch ref)" "" "$(git symbolic-ref -q --short HEAD || echo '')"
check "preflight HEAD == origin/main" "$(git rev-parse origin/main)" "$(git rev-parse HEAD)"

dirty_skips=0
echo "wip" > wip.txt
preflight; rc=$?
check "1st dirty skip returns 2 (grace)" 2 "$rc"
check "1st dirty skip increments counter" 1 "$dirty_skips"
check "1st dirty skip does NOT stash" 0 "$(git stash list | wc -l | tr -d ' ')"

preflight; rc=$?
check "K-th dirty skip proceeds (0)" 0 "$rc"
check "K-th dirty skip resets counter" 0 "$dirty_skips"
check "K-th dirty skip created a stash" 1 "$(git stash list | wc -l | tr -d ' ')"
check "K-th dirty skip stash message tagged" 1 "$(git stash list | grep -c 'autonomy-preflight-recovery')"
check "tree clean after recovery" "" "$(git status --porcelain)"
git stash drop -q 2>/dev/null

dirty_skips=5
echo "midrevert" > wip2.txt
: > "$(git rev-parse --git-dir)/REVERT_HEAD"
preflight; rc=$?
check "in-progress op returns 2" 2 "$rc"
check "in-progress op does NOT stash" 0 "$(git stash list | wc -l | tr -d ' ')"
rm -f "$(git rev-parse --git-dir)/REVERT_HEAD" wip2.txt

dirty_skips=0
echo "wip3" > wip3.txt
preflight >/dev/null 2>&1
check "counter is 1 after one dirty skip" 1 "$dirty_skips"
rm -f wip3.txt
preflight; rc=$?
check "clean observation resets counter" 0 "$dirty_skips"
check "clean observation proceeds (0)" 0 "$rc"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
```

```bash
# tests/test_agent_dispatch.sh
#!/usr/bin/env bash
# Unit test for supervisor.sh's config precedence (CLI override > config.yaml
# > hardcoded default) and that the correct adapter file exists per agent.type.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../bin/supervisor.sh"
SUPLOG=/dev/null
log() { :; }

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cfg="$tmp/config.yaml"
cat > "$cfg" <<'YAML'
agent:
  type: claude
  model:
    primary: claude-sonnet-5
    fallback: claude-sonnet-4-6
YAML

check "CLI override wins over config" "codex" "$(resolve_config_value "$cfg" agent.type "codex" claude)"
check "config wins over hardcoded default" "claude" "$(resolve_config_value "$cfg" agent.type "" opus)"
check "hardcoded default wins when key absent" "claude-opus-4-8" "$(resolve_config_value "$cfg" agent.model.does_not_exist "" claude-opus-4-8)"
check "claude adapter file exists" "0" "$([ -f "$HERE/../bin/agents/claude.sh" ] && echo 0 || echo 1)"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
```

- [ ] **Step 2: Run both to verify they fail**

```bash
chmod +x tests/test_preflight_recovery.sh tests/test_agent_dispatch.sh
bash tests/test_preflight_recovery.sh
bash tests/test_agent_dispatch.sh
```
Expected: both fail (`bin/supervisor.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/supervisor.sh`**

```bash
#!/usr/bin/env bash
# bin/supervisor.sh -- generic, repo-agnostic autonomy SUPERVISOR. Runs
# board-drain sessions back-to-back for days, unattended, with usage-limit
# backoff, against WHATEVER target repo is passed via --repo.
#
# Usage:
#   supervisor.sh --repo <path> [--agent-type claude|codex] [--model <id>]
#                 [--fallback-model <id>] [--label <slug>]
#
# --repo is required. Everything else defaults from the target repo's
# .autonomy/config.yaml, or this script's own hardcoded defaults if the pack
# doesn't set it. CLI flags override config.yaml for THIS invocation only --
# config.yaml is never edited by a flag.
set -uo pipefail
ENGINE_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AUTONOMY_ENGINE_HOME="$ENGINE_HOME"

# shellcheck source=/dev/null
source "$ENGINE_HOME/bin/doctor.sh"

CONFIG_GET() { python3 "$ENGINE_HOME/lib/config_parser.py" "$1" "$2" 2>/dev/null; }

# Resolve a config value with CLI-override precedence: CLI override > pack's
# config.yaml > hardcoded default. Empty CLI override means "not passed".
resolve_config_value() {
  local config_file="$1" config_key="$2" cli_override="$3" hardcoded_default="$4"
  if [ -n "$cli_override" ]; then printf '%s' "$cli_override"; return; fi
  local from_config
  from_config="$(CONFIG_GET "$config_file" "$config_key")"
  if [ -n "$from_config" ]; then printf '%s' "$from_config"; return; fi
  printf '%s' "$hardcoded_default"
}

# --- timing knobs (seconds) ---
PACE=120
EMPTY_IDLE=1800
ERR_BACKOFF_START=300; ERR_BACKOFF_MAX=3600
LIMIT_BACKOFF_START=1800; LIMIT_BACKOFF_MAX=18000
LIMIT_RESET_MAX_HORIZON=691200
PREFLIGHT_RECOVERY_AFTER=2
dirty_skips=0

log() {
  local prefix=""
  [ -n "${LABEL:-}" ] && prefix="[$LABEL] "
  echo "$(date -u +%FT%TZ) ${prefix}$*" | tee -a "$SUPLOG"
}

preflight() {
  cd "$AUTONOMY_TARGET_REPO" || { log "preflight: cannot cd to $AUTONOMY_TARGET_REPO"; return 2; }

  if ! doctor_preflight_check "$AUTONOMY_TARGET_REPO"; then
    log "preflight: .autonomy/ pack invalid or incomplete -- abort"
    return 2
  fi

  local gitdir; gitdir="$(git rev-parse --git-dir 2>/dev/null || echo .git)"
  if [ -d "$gitdir/rebase-merge" ] || [ -d "$gitdir/rebase-apply" ] \
     || [ -f "$gitdir/CHERRY_PICK_HEAD" ] || [ -f "$gitdir/MERGE_HEAD" ] \
     || [ -f "$gitdir/REVERT_HEAD" ] || [ -f "$gitdir/BISECT_LOG" ]; then
    log "preflight: rebase/cherry-pick/merge/revert/bisect in progress -- skip (needs a human)"; return 2
  fi

  if [ -n "$(git status --porcelain)" ]; then
    dirty_skips=$((dirty_skips + 1))
    if [ "$dirty_skips" -lt "$PREFLIGHT_RECOVERY_AFTER" ]; then
      log "preflight: tree dirty -- skip ${dirty_skips}/${PREFLIGHT_RECOVERY_AFTER} (won't checkout over uncommitted work yet)"
      return 2
    fi
    local stash_msg; stash_msg="autonomy-preflight-recovery $(date -u +%FT%TZ)"
    if ! git stash push -u -m "$stash_msg" >>"$SUPLOG" 2>&1; then
      log "preflight: tree dirty ${dirty_skips}x but 'git stash' FAILED -- cannot auto-recover; skip (needs a human)"; return 2
    fi
    if [ -n "$(git status --porcelain)" ]; then
      log "preflight: stashed WIP but tree still dirty -- cannot auto-recover; skip (needs a human)"; return 2
    fi
    log "preflight: tree dirty ${dirty_skips}x -- stashed WIP ('$stash_msg'; recover via 'git stash list') and proceeding onto main"
  fi
  dirty_skips=0

  git fetch origin -q 2>>"$SUPLOG" || { log "preflight: fetch failed"; return 2; }
  git switch --detach origin/main -q 2>>"$SUPLOG" || { log "preflight: switch to origin/main failed"; return 2; }
  [ -z "$(git status --porcelain)" ] || { log "preflight: tree dirty on origin/main -- skip"; return 2; }
  return 0
}

compute_limit_wait() {
  [ -f "$RESET_STATE" ] || return 1
  local reset now
  reset="$(cat "$RESET_STATE" 2>/dev/null)"
  case "$reset" in
    ''|*[!0-9]*) return 1 ;;
  esac
  now="$(date +%s)"
  if [ "$reset" -gt "$now" ] && [ "$reset" -le "$((now + LIMIT_RESET_MAX_HORIZON))" ]; then
    echo "$((reset - now))"
    return 0
  fi
  return 1
}

run_session() {
  preflight || return $?

  # shellcheck source=/dev/null
  source "$ENGINE_HOME/bin/agents/${AGENT_TYPE}.sh"

  local log_file; log_file="$LOGDIR/session-$(date +%Y%m%dT%H%M%S).log"
  log "session start -> $log_file"

  agent_invoke \
    "$AUTONOMY_TARGET_REPO/.autonomy/loop_prompt.md" \
    "$AUTONOMY_TARGET_REPO/.autonomy/hard_rules.md" \
    "$MODEL" "$FALLBACK_MODEL" "$log_file"
  local rc=$?

  local outcome; outcome="$(agent_classify_outcome "$log_file" "$rc")"
  case "$outcome" in
    success)
      return 0 ;;
    usage_limit*)
      local epoch="${outcome#usage_limit }"
      if [ "$epoch" != "usage_limit" ] && [ -n "$epoch" ]; then
        printf '%s\n' "$epoch" >"$RESET_STATE"
      fi
      return 3 ;;
    *)
      if compute_limit_wait >/dev/null; then return 3; fi
      return "$rc" ;;
  esac
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  AUTONOMY_TARGET_REPO=""
  AGENT_TYPE_OVERRIDE=""
  MODEL_OVERRIDE=""
  FALLBACK_MODEL_OVERRIDE=""
  LABEL_OVERRIDE=""

  while [ $# -gt 0 ]; do
    case "$1" in
      --repo) AUTONOMY_TARGET_REPO="$2"; shift 2 ;;
      --agent-type) AGENT_TYPE_OVERRIDE="$2"; shift 2 ;;
      --model) MODEL_OVERRIDE="$2"; shift 2 ;;
      --fallback-model) FALLBACK_MODEL_OVERRIDE="$2"; shift 2 ;;
      --label) LABEL_OVERRIDE="$2"; shift 2 ;;
      *) echo "unknown argument: $1" >&2; exit 1 ;;
    esac
  done

  [ -n "$AUTONOMY_TARGET_REPO" ] || { echo "usage: supervisor.sh --repo <path> [--agent-type ...] [--model ...] [--fallback-model ...] [--label ...]" >&2; exit 1; }
  [ -d "$AUTONOMY_TARGET_REPO" ] || { echo "supervisor.sh: --repo path does not exist: $AUTONOMY_TARGET_REPO" >&2; exit 1; }
  AUTONOMY_TARGET_REPO="$(cd "$AUTONOMY_TARGET_REPO" && pwd)"
  export AUTONOMY_TARGET_REPO

  VARDIR="$AUTONOMY_TARGET_REPO/var"
  LOGDIR="$VARDIR/autonomy-logs"
  mkdir -p "$LOGDIR"
  SUPLOG="$LOGDIR/supervisor.log"
  RESET_STATE="$LOGDIR/.last_usage_reset"
  LABEL="$LABEL_OVERRIDE"

  CFG="$AUTONOMY_TARGET_REPO/.autonomy/config.yaml"
  AGENT_TYPE="$(resolve_config_value "$CFG" agent.type "$AGENT_TYPE_OVERRIDE" claude)"
  MODEL="$(resolve_config_value "$CFG" agent.model.primary "$MODEL_OVERRIDE" claude-sonnet-5)"
  FALLBACK_MODEL="$(resolve_config_value "$CFG" agent.model.fallback "$FALLBACK_MODEL_OVERRIDE" claude-sonnet-4-6)"

  LOCK="$VARDIR/autonomy-supervisor.lock"
  if ! mkdir "$LOCK" 2>/dev/null; then
    pid="$(cat "$LOCK/pid" 2>/dev/null || echo)"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      log "supervisor already running (pid $pid); exiting."; exit 0
    fi
    rm -rf "$LOCK"; mkdir "$LOCK" || { log "lost lock race; exiting."; exit 0; }
  fi
  echo $$ >"$LOCK/pid"
  trap 'rm -rf "$LOCK"; log "supervisor stopped."; exit 0' EXIT INT TERM

  log "=== supervisor start (pid $$, repo=$AUTONOMY_TARGET_REPO, agent=$AGENT_TYPE, model=$MODEL) ==="
  err_backoff=$ERR_BACKOFF_START
  limit_backoff=$LIMIT_BACKOFF_START

  while true; do
    open_count="$(cd "$AUTONOMY_TARGET_REPO" && gh issue list --state open --json number -q 'length' 2>/dev/null || echo -1)"
    if [ "$open_count" = "0" ]; then
      dirty_skips=0
      log "board empty -- idle ${EMPTY_IDLE}s"; sleep "$EMPTY_IDLE"; continue
    fi

    run_session; outcome=$?
    case $outcome in
      0) log "session clean (open issues ~$open_count). pace ${PACE}s"
         err_backoff=$ERR_BACKOFF_START; limit_backoff=$LIMIT_BACKOFF_START
         rm -f "$RESET_STATE"
         sleep "$PACE" ;;
      3) jitter=$((RANDOM % 120))
         if reset_wait="$(compute_limit_wait)"; then
           reset_wait=$((reset_wait + jitter))
           log "USAGE LIMIT -- sleeping ${reset_wait}s until API-reported reset, then retry"
           sleep "$reset_wait"
           limit_backoff=$LIMIT_BACKOFF_START
         else
           log "USAGE LIMIT (no reset signal) -- exp backoff $((limit_backoff + jitter))s then retry"
           sleep $((limit_backoff + jitter))
           limit_backoff=$(( limit_backoff*2 < LIMIT_BACKOFF_MAX ? limit_backoff*2 : LIMIT_BACKOFF_MAX ))
         fi ;;
      2) log "preflight skip -- wait ${ERR_BACKOFF_START}s"; sleep "$ERR_BACKOFF_START" ;;
      *) log "session error (rc=$outcome) -- backoff ${err_backoff}s"
         sleep "$err_backoff"
         err_backoff=$(( err_backoff*2 < ERR_BACKOFF_MAX ? err_backoff*2 : ERR_BACKOFF_MAX )) ;;
    esac
  done
fi
```

- [ ] **Step 4: Run both tests to verify they pass**

```bash
bash tests/test_preflight_recovery.sh
bash tests/test_agent_dispatch.sh
```
Expected: both `ALL PASS`.

- [ ] **Step 5: shellcheck**

```bash
shellcheck -S warning bin/supervisor.sh
```
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add bin/supervisor.sh tests/test_preflight_recovery.sh tests/test_agent_dispatch.sh
git commit -m "feat: add generic supervisor.sh (--repo, agent-adapter dispatch, config precedence)"
git push
```

---

### Task 9: `setup_worktree.sh` + launchd plist template

**Files:**
- Create: `bin/setup_worktree.sh`
- Create: `templates/supervisor.plist.tmpl`
- Test: `tests/test_setup_worktree_slug.sh`

**Interfaces:**
- Consumes: `python3 lib/config_parser.py` (Task 2) for `engine.label`.
- Produces: CLI `bin/setup_worktree.sh <target-repo-path> [worktree-path]` — creates/reuses the
  target repo's dedicated worktree and installs its launchd plist. Defines `derive_slug()` as a
  testable function (given `$TARGET_REPO` in scope) — sourceable directly, per this plan's Global
  Constraint that every script's executable body is guarded by
  `[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0`.

- [ ] **Step 1: Write the failing test (sourcing the real script, not a copy)**

```bash
# tests/test_setup_worktree_slug.sh
#!/usr/bin/env bash
# Unit test for setup_worktree.sh's repo-slug derivation (engine.label override
# vs basename-derived default).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=/dev/null
source "$HERE/../bin/setup_worktree.sh"

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/eBull/.autonomy"
TARGET_REPO="$tmp/eBull"
cat > "$TARGET_REPO/.autonomy/config.yaml" <<'YAML'
board:
  owner: someone
YAML
check "basename-derived slug, mixed case collapsed" "ebull" "$(derive_slug)"

mkdir -p "$tmp/My Weird Repo!/.autonomy"
TARGET_REPO="$tmp/My Weird Repo!"
cat > "$TARGET_REPO/.autonomy/config.yaml" <<'YAML'
board:
  owner: someone
YAML
check "non-alphanumeric collapsed to single dashes" "my-weird-repo" "$(derive_slug)"

mkdir -p "$tmp/eBull2/.autonomy"
TARGET_REPO="$tmp/eBull2"
cat > "$TARGET_REPO/.autonomy/config.yaml" <<'YAML'
engine:
  label: custom-label
YAML
check "engine.label overrides basename" "custom-label" "$(derive_slug)"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
```

- [ ] **Step 2: Run to verify it fails**

```bash
chmod +x tests/test_setup_worktree_slug.sh
bash tests/test_setup_worktree_slug.sh
```
Expected: fails (`bin/setup_worktree.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/setup_worktree.sh`** — functions defined unconditionally at the top
  (so sourcing exposes them), the executable body guarded exactly like every other script in this
  plan (`board.sh`, `safe_merge.sh`, `unblock_dependents.sh`)

```bash
#!/usr/bin/env bash
# bin/setup_worktree.sh -- create (idempotently) a dedicated git worktree for
# a target repo's autonomy loop, and install its launchd plist pointed at this
# engine + that worktree.
#
# Usage: setup_worktree.sh <target-repo-path> [worktree-path]
#
# Repo-slug (used for the worktree default path and the launchd Label) =
# .autonomy/config.yaml's engine.label if set, else the target repo's
# directory basename, lowercased, non-alphanumeric runs collapsed to '-'.
set -euo pipefail
ENGINE_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_GET() { python3 "$ENGINE_HOME/lib/config_parser.py" "$1" "$2" 2>/dev/null; }

# Derive the repo-slug for $TARGET_REPO -- the caller sets that variable
# first (the guarded main body below sets it after resolving $1; tests set it
# directly to a fixture path before calling this function).
derive_slug() {
  local label; label="$(CONFIG_GET "$TARGET_REPO/.autonomy/config.yaml" engine.label)"
  if [ -n "$label" ]; then printf '%s' "$label"; return; fi
  basename "$TARGET_REPO" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//'
}

[ "${BASH_SOURCE[0]}" = "${0}" ] || return 0

TARGET="${1:?usage: setup_worktree.sh <target-repo-path> [worktree-path]}"
case "$TARGET" in
  http://*|https://*|git@*)
    echo "setup_worktree.sh: pass a local path to an existing checkout, not a URL ($TARGET)" >&2
    exit 1
    ;;
esac
TARGET_REPO="$(cd "$TARGET" && pwd)"

SLUG="$(derive_slug)"
[ -n "$SLUG" ] || { echo "setup_worktree.sh: could not derive a repo-slug for $TARGET_REPO" >&2; exit 1; }

WORKTREE="${2:-$(cd "$TARGET_REPO/.." && pwd)/.${SLUG}-autonomy}"
LABEL="com.autonomy.${SLUG}.supervisor"
PLIST_DST="$HOME/Library/LaunchAgents/${LABEL}.plist"

echo "target repo   : $TARGET_REPO"
echo "repo-slug     : $SLUG"
echo "worktree      : $WORKTREE"
echo "launchd label : $LABEL"

if [ -f "$PLIST_DST" ]; then
  existing_repo="$(grep -A1 '<key>WorkingDirectory</key>' "$PLIST_DST" | tail -1 | sed -E 's#.*<string>(.*)</string>.*#\1#')"
  if [ -n "$existing_repo" ] && [ -d "$existing_repo" ] && [ "$existing_repo" != "$WORKTREE" ]; then
    echo "setup_worktree.sh: refuse -- label '$SLUG' is already registered for a different worktree ($existing_repo). Set engine.label in $TARGET_REPO/.autonomy/config.yaml to disambiguate." >&2
    exit 1
  fi
fi

[ "$WORKTREE" = "$TARGET_REPO" ] && { echo "setup_worktree.sh: refuse -- worktree path equals the target repo" >&2; exit 1; }

(cd "$TARGET_REPO" && git fetch origin -q)

if (cd "$TARGET_REPO" && git worktree list --porcelain | grep -Fxq "worktree $WORKTREE"); then
  echo "worktree already registered -- leaving as-is (persistent/loop-specific)."
else
  (cd "$TARGET_REPO" && git worktree add --detach "$WORKTREE" origin/main)
  echo "worktree created (detached @ origin/main)."
fi

mkdir -p "$WORKTREE/var/autonomy-logs"

sed -e "s#__ENGINE_HOME__#$ENGINE_HOME#g" -e "s#__REPO__#$WORKTREE#g" -e "s#__LABEL__#$SLUG#g" \
  "$ENGINE_HOME/templates/supervisor.plist.tmpl" > "$PLIST_DST"
echo "installed plist -> $PLIST_DST"

cat <<EOF

Next (operator) -- stop any supervisor bound to an OLD plist for this repo,
load this one (survives reboot via the plist's RunAtLoad):
  launchctl bootout   gui/\$(id -u)/$LABEL 2>/dev/null || true
  launchctl bootstrap gui/\$(id -u) "$PLIST_DST"
  launchctl list | grep "$SLUG"
  tail -f "$WORKTREE/var/autonomy-logs/supervisor.log"
EOF
```

- [ ] **Step 4: Create `templates/supervisor.plist.tmpl`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  launchd LaunchAgent for the autonomy SUPERVISOR. KeepAlive + RunAtLoad:
  starts on load and is restarted if it ever exits (crash, reboot, OOM). The
  supervisor itself loops sessions forever with usage-limit backoff, so
  launchd's only job is to keep ONE supervisor alive. Its internal lock
  prevents duplicates if launchd double-starts.

  __ENGINE_HOME__, __REPO__, __LABEL__ substituted by setup_worktree.sh.
-->
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.autonomy.__LABEL__.supervisor</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>__ENGINE_HOME__/bin/supervisor.sh</string>
    <string>--repo</string>
    <string>__REPO__</string>
  </array>

  <key>WorkingDirectory</key>
  <string>__REPO__</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>

  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>60</integer>

  <key>StandardOutPath</key>
  <string>__REPO__/var/autonomy-logs/launchd.supervisor.out.log</string>
  <key>StandardErrorPath</key>
  <string>__REPO__/var/autonomy-logs/launchd.supervisor.err.log</string>
</dict>
</plist>
```

- [ ] **Step 5: Run the test to verify it passes**

```bash
bash tests/test_setup_worktree_slug.sh
```
Expected: `ALL PASS`.

- [ ] **Step 6: shellcheck**

```bash
shellcheck -S warning bin/setup_worktree.sh
```
Expected: no output.

- [ ] **Step 7: Commit**

```bash
git add bin/setup_worktree.sh templates/supervisor.plist.tmpl tests/test_setup_worktree_slug.sh
git commit -m "feat: add generic setup_worktree.sh (label override + collision guard) and plist template"
git push
```

---

### Task 10: `worktree_gc.sh` — generic `--repo` version

**Files:**
- Create: `bin/worktree_gc.sh`

**Interfaces:**
- Consumes: nothing (pure git commands).
- Produces: CLI `bin/worktree_gc.sh --repo <target-repo-path>`.

- [ ] **Step 1: Implement `bin/worktree_gc.sh`** (no dedicated unit test — this is a thin,
  already-safe wrapper around `git worktree prune`/`git branch -D` guarded by
  `--is-ancestor`, ported from eBull's version with only the `--repo` parameterization changed;
  the acceptance run in Task 13 exercises it against a real repo)

```bash
#!/usr/bin/env bash
# bin/worktree_gc.sh -- tidy the autonomy git worktrees + branches for a
# target repo:
#   - KEEP the persistent loop/agent worktree (the supervisor's tree) -- it's
#     reused across sessions, never torn down here.
#   - PRUNE stale worktree admin entries via `git worktree prune`.
#   - DELETE local feature branches already merged into origin/main.
#
# Only fully-merged branches are removed (tip is an ancestor of origin/main),
# so this can never drop unmerged work.
#
# Usage: worktree_gc.sh --repo <target-repo-path>
set -euo pipefail

REPO=""
while [ $# -gt 0 ]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done
[ -n "$REPO" ] || { echo "usage: worktree_gc.sh --repo <target-repo-path>" >&2; exit 1; }
cd "$REPO" || exit 1

echo "== prune stale worktree admin entries =="
git worktree prune -v

echo "== delete local branches merged into origin/main =="
if ! git fetch origin -q 2>/dev/null || ! git rev-parse --verify -q origin/main >/dev/null 2>&1; then
  echo "  SKIP: 'git fetch origin' failed or origin/main unresolved -- not deleting against a stale ref"
  echo "== remaining worktrees (loop/agent-specific = KEEP) =="
  git worktree list
  exit 0
fi
current="$(git branch --show-current 2>/dev/null || echo)"
deleted=0
while IFS= read -r b; do
  case "$b" in main|"$current"|'') continue ;; esac
  if git merge-base --is-ancestor "$b" origin/main 2>/dev/null; then
    if git branch -D "$b" >/dev/null 2>&1; then
      echo "  deleted merged branch: $b"; deleted=$((deleted + 1))
    fi
  fi
done < <(git for-each-ref --format='%(refname:short)' refs/heads)
echo "  ($deleted merged branch(es) removed)"

echo "== remaining worktrees (loop/agent-specific = KEEP) =="
git worktree list
```

- [ ] **Step 2: shellcheck**

```bash
shellcheck -S warning bin/worktree_gc.sh
```
Expected: no output.

- [ ] **Step 3: Smoke-test against the engine repo itself**

```bash
bin/worktree_gc.sh --repo "$(pwd)"
```
Expected: prints `== prune stale worktree admin entries ==`, `== delete local branches merged into
origin/main ==`, `(0 merged branch(es) removed)` (nothing to delete yet — this repo only has `main`
so far), and `== remaining worktrees ==` listing this checkout.

- [ ] **Step 4: Commit**

```bash
git add bin/worktree_gc.sh
git commit -m "feat: add generic worktree_gc.sh (--repo parameterized)"
git push
```

---

### Task 11: `onboard.sh` + pack templates

**Files:**
- Create: `bin/onboard.sh`
- Create: `templates/autonomy-pack/config.yaml`
- Create: `templates/autonomy-pack/loop_prompt.md`
- Create: `templates/autonomy-pack/hard_rules.md`
- Test: `tests/test_onboard.sh`

**Interfaces:**
- Consumes: nothing at runtime (pure file-copy).
- Produces: CLI `bin/onboard.sh <target-repo>` — scaffolds `.autonomy/` idempotently.

- [ ] **Step 1: Write the failing test**

```bash
# tests/test_onboard.sh
#!/usr/bin/env bash
# Unit test for onboard.sh's scaffolding idempotency.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_HOME="$(cd "$HERE/.." && pwd)"

fails=0
check() {
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (expected '$2', got '$3')"; fails=$((fails + 1)); fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

"$ENGINE_HOME/bin/onboard.sh" "$tmp" >/dev/null 2>&1
check "config.yaml scaffolded" "0" "$([ -f "$tmp/.autonomy/config.yaml" ] && echo 0 || echo 1)"
check "loop_prompt.md scaffolded" "0" "$([ -f "$tmp/.autonomy/loop_prompt.md" ] && echo 0 || echo 1)"
check "hard_rules.md scaffolded" "0" "$([ -f "$tmp/.autonomy/hard_rules.md" ] && echo 0 || echo 1)"

echo "MY CUSTOM EDIT" > "$tmp/.autonomy/config.yaml"
"$ENGINE_HOME/bin/onboard.sh" "$tmp" >/dev/null 2>&1
check "idempotent -- does not clobber an existing file" "MY CUSTOM EDIT" "$(cat "$tmp/.autonomy/config.yaml")"

echo "---"
if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails CHECK(S) FAILED"; exit 1; fi
```

- [ ] **Step 2: Run to verify it fails**

```bash
chmod +x tests/test_onboard.sh
bash tests/test_onboard.sh
```
Expected: fails (`bin/onboard.sh` doesn't exist yet).

- [ ] **Step 3: Implement `bin/onboard.sh`**

```bash
#!/usr/bin/env bash
# bin/onboard.sh -- scaffold .autonomy/ in a target repo from
# templates/autonomy-pack/. Idempotent: never overwrites an existing file.
#
# Usage: onboard.sh <target-repo>
set -euo pipefail
ENGINE_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TARGET="${1:?usage: onboard.sh <target-repo>}"
TARGET_REPO="$(cd "$TARGET" && pwd)"
PACK_DIR="$TARGET_REPO/.autonomy"
TEMPLATE_DIR="$ENGINE_HOME/templates/autonomy-pack"

mkdir -p "$PACK_DIR"

copied=0
skipped=0
for f in "$TEMPLATE_DIR"/*; do
  name="$(basename "$f")"
  dest="$PACK_DIR/$name"
  if [ -f "$dest" ]; then
    echo "onboard.sh: SKIP $name (already exists)"
    skipped=$((skipped + 1))
  else
    cp "$f" "$dest"
    echo "onboard.sh: created $name"
    copied=$((copied + 1))
  fi
done

echo "onboard.sh: $copied file(s) created, $skipped already present. Edit $PACK_DIR/config.yaml before running the loop."
```

- [ ] **Step 4: Create `templates/autonomy-pack/config.yaml`**

```yaml
# .autonomy/config.yaml -- per-repo policy for the autonomy engine.
# See autonomy-engine/README.md for the full schema reference.

board:
  owner: CHANGE-ME          # GitHub user or org that owns the Projects v2 board
  project_title: "CHANGE-ME engineering board"

engine:
  # label: my-repo          # uncomment + set only if this repo's basename collides
  #                         # with another target repo on the same machine
  requires_claude_md: false  # set true if this repo's workflow assumes CLAUDE.md exists

agent:
  type: claude               # claude | codex (only claude has an adapter implemented)
  model:
    primary: claude-sonnet-5
    fallback: claude-sonnet-4-6

merge_gate:
  strategy: manual           # manual | ci_only | bot_comment | gh_review
  # bot_comment-specific:
  # author_login: github-actions
  # marker: "Claude Code Review"
  # doc_only_extensions: [".md"]
  # gh_review-specific:
  # reviewer_login: some-reviewer-bot[bot]

worktree:
  default_path: "../.{repo-slug}-autonomy"
```

- [ ] **Step 5: Create `templates/autonomy-pack/hard_rules.md`**

```markdown
# Hard safety rules -- NEVER violate, even unattended

- Never `git push --no-verify` (emergencies only).
- Merge ONLY via `"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh" <pr>` -- never `gh pr merge` directly.
- Follow `.claude/CLAUDE.md` (if present) and `.autonomy/loop_prompt.md` exactly.

<!-- Edit this file for your repo's own non-negotiables (trading/finance/
     destructive-ops rules, whatever applies). This is a starter, not a
     complete policy. -->
```

- [ ] **Step 6: Create `templates/autonomy-pack/loop_prompt.md`**

```markdown
# Autonomy loop -- standing task

You are running headless and unattended to drain this repo's engineering
board. Work through open tickets back-to-back. Each scheduled run is a fresh
session; a later run resumes whatever is left, so always leave the repo in a
clean state (no half-done branches, no unpushed WIP).

## Each iteration
1. Triage the board: `gh issue list --state open --limit 100`. Pick the
   highest-value actionable ticket. Decide the order yourself.
2. Execute the ticket's full workflow (read -> plan -> implement -> test ->
   PR). Merge ONLY via `"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh" <pr>` --
   it mechanically verifies the configured merge gate; never merge around it.
   If it reports manual-mode, leave the PR open and move to the next ticket.
3. Update the board via `"$AUTONOMY_ENGINE_HOME/bin/board.sh" status <n>
   "<status>"` at each lifecycle transition (best-effort -- a board hiccup
   never blocks real work).
4. Next ticket.

<!-- Edit this file for your repo's own triage rules, QA steps, and anything
     else specific to how this project wants its board drained. This is a
     starter, not a complete policy. -->
```

- [ ] **Step 7: Run the test to verify it passes**

```bash
bash tests/test_onboard.sh
```
Expected: `ALL PASS`.

- [ ] **Step 8: shellcheck**

```bash
shellcheck -S warning bin/onboard.sh
```
Expected: no output.

- [ ] **Step 9: Commit**

```bash
git add bin/onboard.sh templates/autonomy-pack/ tests/test_onboard.sh
git commit -m "feat: add onboard.sh + autonomy-pack templates"
git push
```

---

### Task 12: README + full test-suite run + final lint pass

**Files:**
- Create: `README.md` (overwrites Task 1's stub)
- Create: `tests/run_all.sh`

**Interfaces:**
- Consumes: everything from Tasks 2–11.
- Produces: `tests/run_all.sh` — one command to run the full suite; `README.md` — the durable pack
  contract + schema + merge-gate reference for anyone (including a future you) onboarding a new repo.

- [ ] **Step 1: Write `tests/run_all.sh`**

```bash
#!/usr/bin/env bash
# tests/run_all.sh -- run every test in this suite, bash and python.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE/.."

fail=0
for t in tests/test_*.sh; do
  echo "=== $t ==="
  bash "$t" || fail=1
done

echo "=== python: test_config_parser ==="
python3 -m unittest tests.test_config_parser -v || fail=1

if [ "$fail" -eq 0 ]; then echo "ALL SUITES PASS"; exit 0; else echo "ONE OR MORE SUITES FAILED"; exit 1; fi
```

- [ ] **Step 2: Run it**

```bash
chmod +x tests/run_all.sh
bash tests/run_all.sh
```
Expected: every suite prints `ALL PASS` (or its unittest equivalent), final line `ALL SUITES PASS`.

- [ ] **Step 3: Write the full `README.md`**

```markdown
# autonomy-engine

Repo-agnostic engine for running Claude Code (and, in future, other CLI agents) autonomy loops
against any target repo, from one operator's account.

## Quickstart

```bash
# 1. Scaffold a new target repo's pack:
bin/onboard.sh /path/to/target-repo
# edit /path/to/target-repo/.autonomy/config.yaml

# 2. Check it's ready:
bin/doctor.sh /path/to/target-repo

# 3. Create its dedicated worktree + launchd plist:
bin/setup_worktree.sh /path/to/target-repo

# 4. Load it (see setup_worktree.sh's own printed next-steps for the exact commands)
```

## The `.autonomy/` pack contract

Every target repo needs a `.autonomy/` directory with exactly these three files:

- **`loop_prompt.md`** — the standing task, passed as the primary prompt (`claude -p`).
- **`hard_rules.md`** — non-negotiable safety rules, appended to the session's system prompt.
- **`config.yaml`** — project policy (see schema below). `bin/onboard.sh` scaffolds all three from
  `templates/autonomy-pack/`.

`.autonomy/config.yaml` existing and parsing is the engine's hard requirement for treating a
directory as a valid target repo — `doctor.sh`/`supervisor.sh` both refuse to proceed without it.

## `config.yaml` schema

```yaml
board:
  owner: <github-user-or-org>
  project_title: "<Projects v2 board title>"

engine:
  label: <slug>                # optional; disambiguates two repos sharing a basename
  requires_claude_md: <bool>    # hard-fail (not just warn) if .claude/CLAUDE.md is missing

agent:
  type: claude                  # claude | codex (only claude has an adapter implemented)
  model:
    primary: <model-id>
    fallback: <model-id>
  config: {}                    # opaque, adapter-owned pass-through (unused by the claude adapter)

merge_gate:
  strategy: manual | ci_only | bot_comment | gh_review
  author_login: <string>        # bot_comment only
  marker: <string>               # bot_comment only
  doc_only_extensions: [<ext>]   # bot_comment only, e.g. [".md"]
  reviewer_login: <string>       # gh_review only

worktree:
  default_path: "../.{repo-slug}-autonomy"
```

Every value is either optional-with-an-engine-default or required-only-for-the-strategy-that-uses-
it. Nothing in the engine hardcodes any one repo's actual values.

`{repo-slug}` = `engine.label` if set, else the target repo's directory basename, lowercased,
non-alphanumeric runs collapsed to `-`.

## Merge-gate strategies

CI-green is checked first (any failing/pending check refuses; a `gh` API failure itself refuses,
never treated as green; `ci_only` additionally refuses on zero configured checks). Then:

| Strategy | What it checks |
|---|---|
| `manual` (default) | Nothing further — never auto-merges. PRs stay open for a human. |
| `ci_only` | Nothing further — CI green is the whole gate. |
| `bot_comment` | Latest matching issue comment (by `author_login` + `marker`) postdates the head commit and reads APPROVE, no BLOCKING/REQUEST CHANGES. Includes a doc-only fast path for PRs where every changed file matches `doc_only_extensions`. |
| `gh_review` | Latest GitHub Review object from `reviewer_login` postdates the head commit and its `state == APPROVED`. |

`bin/safe_merge.sh <pr-number>` is the only sanctioned merge path — the loop must never call
`gh pr merge` directly.

## `bin/` reference

| Script | Purpose |
|---|---|
| `supervisor.sh --repo <path> [--agent-type] [--model] [--fallback-model] [--label]` | The main loop launchd runs |
| `onboard.sh <target-repo>` | Scaffold `.autonomy/` (idempotent) |
| `doctor.sh <target-repo>` | Full readiness report (network calls; diagnostic-only, never provisions) |
| `setup_worktree.sh <target-repo> [worktree-path]` | Create/reuse the dedicated worktree + install the launchd plist |
| `worktree_gc.sh --repo <path>` | Prune stale worktrees + merged branches |
| `safe_merge.sh <pr-number>` | The only sanctioned merge path |
| `board.sh status <issue#> "<status>" \| add <issue#>` | Best-effort GitHub Projects v2 board updates |
| `unblock_dependents.sh <merged-pr-number>` | Post-merge "blocked by #X" notifier |
| `agents/claude.sh` | The Claude Code agent adapter (only one implemented) |

## Agent adapters

`bin/agents/<type>.sh`, dispatched by `agent.type`. Each implements two functions:

- `agent_invoke(prompt_file, safety_file, model, fallback_model, log_file) -> exit code`
- `agent_classify_outcome(log_file, exit_code) -> "success" | "usage_limit [epoch]" | "error"`

Only `claude.sh` exists today. A `codex.sh` is a real future possibility (Codex's CLI differs
structurally — no system-prompt-append flag, its own JSONL schema, no native fallback-model
support) but is not built or tested here.

## Testing

```bash
bash tests/run_all.sh
```
```

- [ ] **Step 4: Run the full suite once more against the finished README (sanity — README changes
  don't affect test behavior, this just confirms nothing regressed while writing it)**

```bash
bash tests/run_all.sh
```
Expected: `ALL SUITES PASS`.

- [ ] **Step 5: shellcheck every script in the repo, one final pass**

```bash
shellcheck -S warning bin/*.sh bin/agents/*.sh tests/*.sh
```
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add README.md tests/run_all.sh
git commit -m "docs: full README (pack contract, config schema, merge-gate + bin reference)"
git push
```

---

### Task 13: eBull cutover

**Files (all in `~/Dev/eBull`):**
- Create: `.autonomy/loop_prompt.md`
- Create: `.autonomy/hard_rules.md`
- Create: `.autonomy/config.yaml`
- Delete: `scripts/autonomy/board.sh`
- Delete: `scripts/autonomy/com.ebull.autonomy.plist`
- Delete: `scripts/autonomy/com.ebull.autonomy.supervisor.plist`
- Delete: `scripts/autonomy/loop_prompt.md`
- Delete: `scripts/autonomy/run_loop.sh`
- Delete: `scripts/autonomy/safe_merge.sh`
- Delete: `scripts/autonomy/setup.md`
- Delete: `scripts/autonomy/setup_worktree.sh`
- Delete: `scripts/autonomy/supervisor.sh`
- Delete: `scripts/autonomy/test_preflight_recovery.sh`
- Delete: `scripts/autonomy/test_safe_merge_doc_only.sh`
- Delete: `scripts/autonomy/test_unblock_dependents.sh`
- Delete: `scripts/autonomy/test_usage_limit_reset.sh`
- Delete: `scripts/autonomy/unblock_dependents.sh`
- Delete: `scripts/autonomy/worktree_gc.sh`
- **Do NOT delete:** `scripts/autonomy/com.ebull.jobs-daemon.plist` — this is the unrelated SEC
  jobs-daemon launchd plist (`app.jobs`), not part of the autonomy loop. It happens to live in the
  same directory today; it is not touched by this cutover.

**Interfaces:**
- Consumes: the finished `autonomy-engine` repo at `~/Dev/autonomy-engine` (Tasks 1–12).
- Produces: eBull running its autonomy loop through the new engine; `scripts/autonomy/*` (minus the
  jobs-daemon plist) removed from eBull.

This task follows eBull's `.claude/CLAUDE.md` branch/PR workflow — it is NOT a direct commit to
`main`.

- [ ] **Step 1: Create the branch**

```bash
cd ~/Dev/eBull
git checkout main -q && git pull -q
git checkout -b feature/1878-autonomy-engine-cutover
```

- [ ] **Step 2: Read eBull's current pack content to base the rewrite on**

```bash
cat scripts/autonomy/loop_prompt.md
grep -n 'SAFETY=' scripts/autonomy/supervisor.sh
```
Confirm the exact current text before rewriting (the SAFETY string and every
`scripts/autonomy/safe_merge.sh` / `scripts/autonomy/board.sh` reference in `loop_prompt.md`).

- [ ] **Step 3: Create `.autonomy/hard_rules.md`** (extracted from `supervisor.sh`'s `SAFETY`
  string, path references rewritten)

```markdown
Unattended run. HARD RULES: never execute/approve/simulate a trade, never POST order endpoints,
never touch the kill-switch, never close a position; merge ONLY via
"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh"; never push --no-verify; never restart the :8000/:5173
tasks. Follow .claude/CLAUDE.md and .autonomy/loop_prompt.md exactly.
```

- [ ] **Step 4: Create `.autonomy/loop_prompt.md`** (moved from `scripts/autonomy/loop_prompt.md`,
  every `scripts/autonomy/safe_merge.sh` reference → `"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh"`,
  every `scripts/autonomy/board.sh` reference → `"$AUTONOMY_ENGINE_HOME/bin/board.sh"`, plus the
  manual-mode line added to the merge step)

Take the full text read in Step 2 and apply these substitutions everywhere they appear (the merge
instruction in "Each iteration" step 2, and every bullet under "Board discipline"):

```bash
sed -e 's#scripts/autonomy/safe_merge\.sh#"$AUTONOMY_ENGINE_HOME/bin/safe_merge.sh"#g' \
    -e 's#scripts/autonomy/board\.sh#"$AUTONOMY_ENGINE_HOME/bin/board.sh"#g' \
    scripts/autonomy/loop_prompt.md > .autonomy/loop_prompt.md
```

Then manually edit `.autonomy/loop_prompt.md`'s merge step (in "Each iteration", step 2) to add,
immediately after the mechanical-verification parenthetical:

```
   If the latest round is rebuttal-only (no code change, you think the bot is
   wrong), do NOT merge unattended -- that needs Codex ckpt-3 + human judgment;
   leave the PR open with your reasoning and move on. If safe_merge.sh reports
   manual-mode, leave the PR open and move to the next ticket.
```

- [ ] **Step 5: Create `.autonomy/config.yaml`** (eBull's real values)

```yaml
board:
  owner: Luke-Bradford
  project_title: "eBull engineering board"

engine:
  requires_claude_md: true

agent:
  type: claude
  model:
    primary: claude-sonnet-5
    fallback: claude-sonnet-4-6

merge_gate:
  strategy: bot_comment
  author_login: github-actions
  marker: "Claude Code Review"
  doc_only_extensions: [".md"]
```

- [ ] **Step 6: Delete the superseded files (explicitly NOT the jobs-daemon plist)**

```bash
git rm scripts/autonomy/board.sh \
       scripts/autonomy/com.ebull.autonomy.plist \
       scripts/autonomy/com.ebull.autonomy.supervisor.plist \
       scripts/autonomy/loop_prompt.md \
       scripts/autonomy/run_loop.sh \
       scripts/autonomy/safe_merge.sh \
       scripts/autonomy/setup.md \
       scripts/autonomy/setup_worktree.sh \
       scripts/autonomy/supervisor.sh \
       scripts/autonomy/test_preflight_recovery.sh \
       scripts/autonomy/test_safe_merge_doc_only.sh \
       scripts/autonomy/test_unblock_dependents.sh \
       scripts/autonomy/test_usage_limit_reset.sh \
       scripts/autonomy/unblock_dependents.sh \
       scripts/autonomy/worktree_gc.sh
ls scripts/autonomy/
```
Expected: only `com.ebull.jobs-daemon.plist` remains.

- [ ] **Step 7: Stage the new pack**

```bash
git add .autonomy/
git status --short
```
Expected: 14 deletions (`D`) + 3 additions (`A .autonomy/...`).

- [ ] **Step 8: Run eBull's pre-push checks**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -m "not db"
uv run pytest tests/smoke
```
Expected: all pass (this change touches no Python — a sanity confirmation, not expected to surface
anything).

- [ ] **Step 9: Commit and push**

```bash
git commit -m "$(cat <<'EOF'
feat(#1878): cut eBull over to the standalone autonomy-engine

Replaces scripts/autonomy/* with .autonomy/ (loop_prompt.md, hard_rules.md,
config.yaml) per docs/superpowers/specs/2026-07-01-autonomy-engine-pack-seam-design.md.
The engine itself now lives at github.com/Luke-Bradford/autonomy-engine.

Path references rewritten: scripts/autonomy/safe_merge.sh and
scripts/autonomy/board.sh -> $AUTONOMY_ENGINE_HOME/bin/{safe_merge,board}.sh.
merge_gate.strategy: bot_comment matches today's real mechanism exactly
(author_login: github-actions, marker: "Claude Code Review", doc_only_extensions: [".md"]).

com.ebull.jobs-daemon.plist is NOT deleted -- unrelated SEC jobs-daemon
infrastructure that happened to live in the same directory.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
git push -u origin feature/1878-autonomy-engine-cutover
```

- [ ] **Step 10: Open the PR**

```bash
gh pr create --title "feat(#1878): cut eBull over to the standalone autonomy-engine" --body "$(cat <<'EOF'
## What
Replaces scripts/autonomy/* with .autonomy/ (loop_prompt.md, hard_rules.md, config.yaml), per the
approved spec at docs/superpowers/specs/2026-07-01-autonomy-engine-pack-seam-design.md. The engine
itself now lives at github.com/Luke-Bradford/autonomy-engine.

## Why
Standalone engine repo can supervise autonomy loops across any target repo, not just eBull. See the
linked spec + docs/superpowers/specs/2026-07-01-managed-agents-vs-hand-rolled-comparison.md for the
full rationale.

## Security model
No new untrusted-input paths. `.autonomy/config.yaml` is committed, PR-reviewed, same trust level as
any other repo file. Merge gate is unchanged in substance (bot_comment, matching today's real
mechanism exactly) -- only its implementation moved to a shared engine.

## Verification
- All eBull pre-push gates green (no Python touched by this change).
- Manual acceptance: ran `bin/doctor.sh ~/Dev/eBull` from the engine repo -- all checks green.
- Manual acceptance: ran `bin/supervisor.sh --repo <worktree>` in the foreground, confirmed the
  startup log line shows the correct repo/agent/model and a session starts cleanly, then stopped it
  (not launchctl-bootstrapped -- going live remains a separate, deliberate operator decision, same
  as today's "ready but not bootstrapped" state).

Closes #1878.
EOF
)"
```

- [ ] **Step 11: Poll CI and the review bot**

```bash
gh pr checks <pr-number>
gh pr view <pr-number> --comments
```
Wait for the Claude review bot to post and CI to go green (per eBull's CLAUDE.md — do not merge
until APPROVE on the latest commit).

- [ ] **Step 12: Manual acceptance run — set up the worktree + plist (do not bootstrap yet)**

```bash
cd ~/Dev/autonomy-engine
bin/doctor.sh ~/Dev/eBull
```
Expected: every check `OK` (eBull already has CLAUDE.md, skills, the review workflow, `gh` auth with
the right scopes, an existing board).

```bash
bin/setup_worktree.sh ~/Dev/eBull
```
Expected: creates/reuses `~/Dev/.ebull-autonomy`, installs
`~/Library/LaunchAgents/com.autonomy.ebull.supervisor.plist`. Note the printed slug — confirm it
reads `ebull` (matches the pre-existing worktree naming convention closely enough that the operator
recognizes it).

- [ ] **Step 13: Manual acceptance run — one foreground session**

```bash
bin/supervisor.sh --repo ~/Dev/.ebull-autonomy
```
Expected: the startup log line reads `=== supervisor start (pid <N>, repo=/Users/.../.ebull-autonomy,
agent=claude, model=claude-sonnet-5) ===`, followed by either `board empty -- idle 1800s` (if no
open issues) or a real session starting. Let it run long enough to confirm a `claude` process
actually launches and begins working (or confirms `board empty`), then `Ctrl-C` — the trap cleans
up the lock file. **Do not run `launchctl bootstrap`** — going live remains the operator's own
decision, matching the existing "ready but not bootstrapped" convention.

- [ ] **Step 14: Resolve any review comments, re-run gates, re-push if needed** (per eBull's
  CLAUDE.md review-resolution contract — FIXED/DEFERRED/REBUTTED for every comment)

- [ ] **Step 15: Merge once APPROVE + green CI on the latest commit**

```bash
gh pr merge <pr-number> --squash --delete-branch
```

- [ ] **Step 16: Update memory** (per the repo's own memory-maintenance convention) that the
  autonomy loop's engine now lives in `autonomy-engine`, `scripts/autonomy/` no longer exists in
  eBull, and the worktree/plist naming is `ebull`/`com.autonomy.ebull.supervisor`.

---

## Self-Review

**1. Spec coverage** (against `docs/superpowers/specs/2026-07-01-autonomy-engine-pack-seam-design.md`):
- Architecture/file layout — Tasks 1–12 build exactly the tree in the spec's Architecture section.
- Path rewrites — Task 13, Steps 3–4.
- Config schema (including `agent.config: {}` opaque map, `engine.label`/`requires_claude_md`) —
  Task 2 (parser) + Task 11 (template) + Task 13 (eBull's real values).
- CLI-override precedence (`--agent-type`/`--model`/`--label`) — Task 8 (`resolve_config_value`),
  tested in `test_agent_dispatch.sh`.
- Repo-slug + collision guard — Task 9, tested in `test_setup_worktree_slug.sh` (derivation) +
  Step in Task 9 implementing the refuse-on-collision check (not independently unit-tested — it's a
  filesystem/plist-parsing side effect, covered by the acceptance run in Task 13 Step 12 which
  exercises the real path).
- Agent adapters (`agent_invoke`/`agent_classify_outcome`, reset-epoch split) — Task 3 + Task 8.
- Merge-gate strategies (all 4, CI fail-safe, doc-only scoped to `bot_comment`, `gh_review`
  staleness fix) — Task 6.
- `onboard.sh`/`doctor.sh` (including the `bot_comment`-only workflow check, `.claude/CLAUDE.md`
  severity toggle, board owner/org auto-detect) — Tasks 4, 5, 11.
- `unblock_dependents.sh` verbatim port — Task 7.
- `worktree_gc.sh` — Task 10.
- Testing section's full list — every named test file is either ported (Tasks 3, 7, 8) or newly
  written (Tasks 2, 4, 5, 6, 9, 11) per the spec's own breakdown.
- Cutover — Task 13, including the jobs-daemon-plist carve-out (a gap in the spec's own "delete
  scripts/autonomy/*" wording that this plan resolves explicitly, since that file is unrelated
  infrastructure colocated in the same directory).
- Open follow-ups (codex.sh, shared usage-limit state, registry, dashboard control-lever,
  auto-provisioning) — deliberately NOT implemented, matching the spec's own scope boundary.

**2. Placeholder scan:** No TBD/TODO/"add appropriate error handling" phrases — every step has
concrete code or an exact command with expected output.

**3. Type/signature consistency:** `agent_invoke(prompt_file, safety_file, model, fallback_model,
log_file)` and `agent_classify_outcome(log_file, exit_code)` are defined in Task 3 and consumed
identically in Task 8's `run_session()`. `resolve_config_value(config_file, config_key,
cli_override, hardcoded_default)` is defined and consumed within Task 8 only. `board_resolve_project
(owner, project_title, want_status)` is defined in Task 5 and consumed identically in Task 4's
`doctor_full_report` and Task 5's own CLI body. `is_doc_only(files, extensions_csv)` and
`ci_check(pr, strategy)` are defined in Task 6 and consumed identically within the same file's CLI
body. No drift found.
