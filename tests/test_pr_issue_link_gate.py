from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_pr_issue_link.sh"


def _run(title: str, body: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PR_TITLE": title, "PR_BODY": body}
    return subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def test_exact_2740_negated_close_incident_is_refused() -> None:
    result = _run(
        "research(#2493): measure arrival",
        "Closes only the census subtask; does not close #2493.\n\nRefs #2493.",
    )
    assert result.returncode == 1
    assert "GitHub ignores negation" in result.stdout


def test_contracted_and_indirect_negated_closing_verbs_are_refused() -> None:
    for body in (
        "This doesn't fix #2741. Refs #2741.",
        "This will not fully resolve #2741. Part of #2741.",
        "Land without closing #2741. Umbrella #2741.",
    ):
        assert _run("fix(#2741): guard links", body).returncode == 1


def test_negation_guard_matches_every_supported_closing_separator() -> None:
    for separator in (" ", ": ", "\t"):
        body = f"This does not close{separator}#2741. Refs #2741."
        assert _run("fix(#2741): guard links", body).returncode == 1


def test_explicit_non_closing_and_closing_forms_pass() -> None:
    assert _run("fix(#2741): guard links", "Refs #2741.").returncode == 0
    assert _run("fix(#2741): guard links", "Closes #2741.").returncode == 0
    assert _run("fix(#2741): guard links", "Closes: #2741.").returncode == 0
    assert _run("fix(#2741): guard links", "Nothing unusual. Closes #2741.").returncode == 0


def test_missing_title_issue_reference_still_fails() -> None:
    result = _run("fix(#2741): guard links", "Refs #9999.")
    assert result.returncode == 1
    assert "no Closes/Fixes/Resolves" in result.stdout


def test_examples_inside_comments_and_code_do_not_trigger_or_satisfy() -> None:
    body = "<!-- does not close #2741 -->\n```text\nCloses #2741\n```\nRefs #2741."
    assert _run("fix(#2741): guard links", body).returncode == 0
