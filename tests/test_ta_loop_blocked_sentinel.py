"""The prompt and the driver must name the SAME blocked-pass sentinel.

⚠ This test exists because the first fix (#3293) grepped the TRANSCRIPT for the
phrase "blocked pass". Any rephrasing by the model would have fallen back to the
60-second cooldown and silently restored the spin it was written to stop -- the
same defect one layer down, and invisible until someone noticed the loop had
burned $89.55 producing nothing.

The contract is now a file path, and a path is only a contract if both sides
agree on it. A wording change in either file fails here instead of in
production.
"""

from __future__ import annotations

import os
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DRIVER = REPO / "scripts" / "autonomy" / "ta_loop.sh"
PROMPT = REPO / ".autonomy" / "loop_prompt.md"

#: The one string both sides must contain. Relative to the loop's state dir.
SENTINEL_BASENAME = "BLOCKED_PASS"


def test_driver_defines_the_sentinel_under_the_state_dir() -> None:
    driver = DRIVER.read_text()
    match = re.search(r'BLOCKED_SENTINEL="\$STATE_DIR/([A-Z_]+)"', driver)
    assert match is not None, "ta_loop.sh must define BLOCKED_SENTINEL under $STATE_DIR"
    assert match.group(1) == SENTINEL_BASENAME


def test_driver_consumes_the_sentinel_and_applies_the_long_cooldown() -> None:
    driver = DRIVER.read_text()
    # Consumed on read: a sentinel left in place would make every later pass look blocked.
    assert 'rm -f "$BLOCKED_SENTINEL"' in driver
    assert 'cooldown="$BLOCKED_COOLDOWN_SECONDS"' in driver


def test_prompt_instructs_the_loop_to_write_the_same_sentinel() -> None:
    prompt = PROMPT.read_text()
    assert SENTINEL_BASENAME in prompt, (
        f"the loop prompt must tell the iteration to write {SENTINEL_BASENAME}; "
        "without it the driver cannot distinguish a blocked pass from a normal one "
        "and restarts in 60s"
    )
    # It must be an instruction to CREATE it, not merely a mention.
    assert re.search(rf"touch\s+\S*{SENTINEL_BASENAME}", prompt), (
        f"the prompt must instruct `touch <state dir>/{SENTINEL_BASENAME}`"
    )


def test_the_driver_does_not_fall_back_to_matching_transcript_wording() -> None:
    # The regression this whole contract replaces.
    driver = DRIVER.read_text()
    assert "blocked pass|no ticket taken" not in driver, (
        "the driver must not decide the cooldown by grepping the model's prose"
    )


def _driver_functions() -> str:
    """The driver's ``seconds_until`` and ``clamp_seconds`` definitions, extracted so they run without the loop."""
    driver = DRIVER.read_text()
    out = []
    for name in ("seconds_until", "clamp_seconds"):
        match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", driver, re.S | re.M)
        assert match is not None, f"ta_loop.sh must define {name}()"
        out.append(match.group(0))
    return "\n".join(out)


def _bash(script: str) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, env=env).stdout.strip()


def test_seconds_until_reads_a_utc_wake_time() -> None:
    wake = (datetime.now(UTC) + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    seconds = int(_bash(f"{_driver_functions()}\nseconds_until {wake}"))
    assert 7_100 <= seconds <= 7_200


def test_seconds_until_prints_nothing_for_a_malformed_wake_time() -> None:
    for bad in ("garbage", "2026-10-16T15:20:00", "2026-10-16 15:20", "tomorrow"):
        assert _bash(f"{_driver_functions()}\nseconds_until '{bad}'") == "", bad


def test_clamp_seconds_bounds_the_sleep() -> None:
    functions = _driver_functions()
    assert _bash(f"{functions}\nclamp_seconds -500 60 43200") == "60"
    assert _bash(f"{functions}\nclamp_seconds 999999 60 43200") == "43200"
    assert _bash(f"{functions}\nclamp_seconds 3600 60 43200") == "3600"


def test_driver_sleeps_until_the_wake_time_and_keeps_the_fixed_fallback() -> None:
    driver = DRIVER.read_text()
    assert 'wake="$(head -n 1 "$BLOCKED_SENTINEL"' in driver
    assert 'cooldown="$(clamp_seconds "$until_wake" "$COOLDOWN_SECONDS" "$BLOCKED_MAX_SECONDS")"' in driver
    assert re.search(r'BLOCKED_MAX_SECONDS="\$\{TA_LOOP_BLOCKED_MAX:-\d+\}"', driver)


def test_prompt_shows_how_to_write_a_wake_time() -> None:
    prompt = PROMPT.read_text()
    assert re.search(rf"echo \d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}Z > \S*{SENTINEL_BASENAME}", prompt)
