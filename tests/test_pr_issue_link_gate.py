from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_pr_issue_link.sh"


def _run(
    title: str,
    body: str,
    base: str = "",
    default_branch: str = "",
) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "PR_TITLE": title,
        "PR_BODY": body,
        "PR_BASE": base,
        "DEFAULT_BRANCH": default_branch,
    }
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


def test_exact_2373_partial_close_incident_is_refused() -> None:
    result = _run(
        "fix(#2176): an Item 403 row must name a beneficial owner",
        "## Issue reference\n\nRefs #2176.\n\n"
        "Fixes #2176's class 4 (Total rows stored as beneficial owners) and the "
        "residue of class 2. Classes 1 and 3 are untouched, so the issue stays open.",
    )
    assert result.returncode == 1
    assert "describes only PART of it" in result.stdout


def test_exact_1244_partial_close_incident_is_refused() -> None:
    result = _run(
        "feat(#1233): PR8 — N-CSR 730d retention cap",
        "Per spec 6.3 no existing rows shift.\n\nCloses #1233's PR8 milestone.",
    )
    assert result.returncode == 1


def test_partitive_qualifiers_after_a_closing_reference_are_refused() -> None:
    for body in (
        "Fixes #2741 item 3.",
        "Closes #2741 step 2.",
        "Resolves #2741 phase 1.",
        "Fixes #2741 class 4.",
        "Closes #2741 arm B.",
        "Fixes #2741’s second half.",
    ):
        assert _run("fix(#2741): guard links", body).returncode == 1, body


def test_partitive_guard_does_not_fire_across_a_line_break() -> None:
    # PR #2875: "## What this fixes" heading, then "#2833 step 2's pass bar" on a
    # later line. GitHub did not close #2833 and neither should the gate fire.
    body = "## What this fixes\n\n#2833 step 2's pass bar moved.\n\nRefs #2833."
    assert _run("fix(#2833): accumulate the spread sample", body).returncode == 0


def test_partitive_guard_does_not_treat_a_hyphenated_word_as_a_keyword() -> None:
    # PR #2361: "silently un-fixed #2169's own accession" is prose, not a link.
    body = "Refs #2169.\n\nThe first draft silently un-fixed #2169's own accession."
    assert _run("fix(#2169): block markup", body).returncode == 0


def test_plain_closing_and_non_closing_forms_are_unaffected() -> None:
    assert _run("fix(#2741): guard links", "Closes #2741.").returncode == 0
    assert _run("fix(#2741): guard links", "Refs #2741. Part of #2832.").returncode == 0
    assert _run("fix(#2741): guard links", "Closes #2741 and nothing else.").returncode == 0


def test_missing_title_issue_reference_still_fails() -> None:
    result = _run("fix(#2741): guard links", "Refs #9999.")
    assert result.returncode == 1
    assert "no Closes/Fixes/Resolves" in result.stdout


def test_exact_2782_inert_closing_reference_on_a_stacked_base_is_refused() -> None:
    result = _run(
        "fix(#2779): metric-axis integrity follow-up",
        "Closes #2779.",
        base="fix/2697-metric-axis-integrity",
        default_branch="main",
    )
    assert result.returncode == 1
    assert "INERT" in result.stdout
    assert "2779" in result.stdout


def test_every_closing_verb_is_reported_inert_on_a_stacked_base() -> None:
    result = _run(
        "fix(#2779): stacked",
        "Fixes #2779. Resolves #2775. Refs #2783.",
        base="fix/2697-metric-axis-integrity",
        default_branch="main",
    )
    assert result.returncode == 1
    # Sorted, de-duplicated, and Refs is not a closing verb so #2783 is absent.
    assert "2775 2779" in result.stdout
    assert "2783" not in result.stdout


def test_non_closing_references_on_a_stacked_base_pass() -> None:
    result = _run(
        "fix(#2779): stacked",
        "Refs #2779. Part of #2783.",
        base="fix/2697-metric-axis-integrity",
        default_branch="main",
    )
    assert result.returncode == 0


def test_closing_reference_on_the_default_base_is_unaffected() -> None:
    result = _run("fix(#2779): direct", "Closes #2779.", base="main", default_branch="main")
    assert result.returncode == 0


def test_stacked_base_check_is_disabled_when_the_workflow_supplies_no_base() -> None:
    # A local invocation has neither variable; the gate must not guess a base.
    assert _run("fix(#2779): local", "Closes #2779.").returncode == 0


def test_examples_inside_comments_and_code_do_not_trigger_or_satisfy() -> None:
    body = "<!-- does not close #2741 -->\n```text\nCloses #2741\n```\nRefs #2741."
    assert _run("fix(#2741): guard links", body).returncode == 0
