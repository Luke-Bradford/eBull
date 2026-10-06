"""#3613 item 1: the review check's verdict parse (a copy of safe_merge's ``review_verdict``)."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from scripts import review_verdict as rv

WORKFLOW_PATH = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "claude-review.yml"

APPROVE = "### [NITPICK] — optional\n`a.py:1` — tidy.\n\n### Verdict\n**APPROVE** — all prior findings resolved.\n"


@pytest.mark.parametrize(
    ("body", "verdict"),
    [
        (APPROVE, "approve"),
        ("### Verdict\nAPPROVE\n", "approve"),
        ("Verdict: **APPROVE** — fine.\n", "approve"),
        ("**Verdict:** APPROVE\n", "approve"),
        ("### Verdict\n**REQUEST CHANGES** — one prior finding OPEN.\n", "block"),
        ("### [BLOCKING] — must fix before merge\n`a.py:1` — wrong.\n\n### Verdict\n**APPROVE**\n", "block"),
        ("### Verdict\n**NEEDS DISCUSSION** — scope.\n", "none"),
        # First-round shape that once lacked the header (10-03 #3599): no marker, no verdict.
        ("**APPROVE** — no confirmed problems.\n", "none"),
        ("### Verdict\nCannot **APPROVE** until the test lands.\n", "none"),
        ("Verdict only covers the diff.\n\n### Verdict\n**REQUEST CHANGES**\n", "block"),
        # The last marker wins: a later verdict closes the review.
        ("### Verdict\n**REQUEST CHANGES**\n\n### Verdict\n**APPROVE**\n", "approve"),
        # A prior-findings line naming a blocking item without the header tag does not block.
        ("### Prior findings\n- `a.py` blocking-class issue: RESOLVED.\n\n### Verdict\n**APPROVE**\n", "approve"),
        # The verdict section ends at the next heading.
        ("### Verdict\n\n### Notes\nAPPROVE\n", "none"),
    ],
)
def test_verdicts(body: str, verdict: str) -> None:
    assert rv.review_verdict(body) == verdict


@pytest.mark.parametrize(("body", "code"), [(APPROVE, 0), ("### Verdict\n**REQUEST CHANGES**\n", 1), ("", 1)])
def test_exit_code_is_zero_only_for_approve(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], body: str, code: int
) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO(body))
    assert rv.main() == code
    assert capsys.readouterr().out.strip() == rv.review_verdict(body)


def test_the_review_job_gates_on_this_script_for_every_reviewed_pr() -> None:
    workflow = cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text()))
    steps = cast(list[dict[str, Any]], workflow["jobs"]["review"]["steps"])
    names = [step.get("name") for step in steps]
    gate = steps[names.index("Fail unless the review approves")]
    assert "python3 scripts/review_verdict.py < review.txt" in gate["run"]
    assert "exit 1" in gate["run"]
    # Runs whenever a review was produced, i.e. under the same condition as the posting step, and after it.
    post = steps[names.index("Post review comment")]
    assert gate["if"] == post["if"]
    assert names.index("Fail unless the review approves") > names.index("Post review comment")
