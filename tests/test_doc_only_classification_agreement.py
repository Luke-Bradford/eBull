"""#2849 — the two doc-only classifiers must agree, and nothing enforced that.

A doc-only PR takes two different paths through two different implementations of
one rule:

* ``.github/workflows/claude-review.yml`` (the ``docs_only`` step) decides whether
  to SKIP the engineering review;
* the engine's ``safe_merge.sh::is_doc_only`` decides whether to take the fast
  merge path, reading its lists from ``.autonomy/config.yaml``.

When they disagree in the direction "workflow says doc-only, safe_merge says not",
the PR becomes **permanently unmergeable**: the review is skipped, so the APPROVE
that ``safe_merge.sh`` waits for can never arrive, and the workflow has no
``workflow_dispatch`` to force one. Observed on PR #2848
(``.autonomy/loop_prompt.md``), fixed for that instance in ``cc986fdb``, and still
present afterwards on ``.mdx``/``.rst`` — which no file in the repo happened to
exercise.

Both files carried a comment instructing the reader to keep the lists identical.
That is an obligation written where it cannot be enforced, and it did not hold.
This test is the enforcement: it binds the workflow's inline patterns to the
config's lists, so editing either side alone fails here.

⚠ Scope, stated honestly. This binds the DATA the two implementations use, plus
the precedence rule that exclusions are tested first. It does not execute
``is_doc_only`` — that lives in a separate repository (``AUTONOMY_ENGINE_HOME``)
which is not guaranteed to be checked out beside this one, and a test that skips
when its subject is absent guards nothing on the machine that matters. Divergence
in the predicate's *logic* is therefore still possible; divergence in its *lists*,
which is what has actually happened twice, is not.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / ".autonomy" / "config.yaml"
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "claude-review.yml"

#: The step whose inline bash reimplements the predicate.
CLASSIFIER_STEP_ID = "docs_only"

#: Matches a bash `[[ ... ]]` conditional and the body up to its `fi`. Single-bracket
#: `[ ... ]` tests in the same script (the base-ref and empty-file guards) are
#: deliberately not matched — they are not part of the classification.
#:
#: ⚠ The terminator is `fi` ALONE ON A LINE, not the substring. A bare `fi` would
#: also match inside `file` or `config`, truncating the body at a comment word and
#: silently mis-splitting the blocks — which would make this parser fail in the
#: same latent way the classifier it guards did (review NITPICK on PR #2884).
_IF_BLOCK = re.compile(r"if \[\[(?P<cond>.*?)\]\]; then(?P<body>.*?)\n\s*fi\b", re.DOTALL)

#: Every `"$f" == <pattern>` comparison inside a condition.
_COMPARISON = re.compile(r'"\$f"\s*==\s*(?P<pattern>\S+)')


def _merge_gate() -> dict[str, Any]:
    config = cast(dict[str, Any], yaml.safe_load(CONFIG_PATH.read_text()))
    return cast(dict[str, Any], config["merge_gate"])


def _classifier_script() -> str:
    workflow = cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text()))
    for job in cast(dict[str, Any], workflow["jobs"]).values():
        for step in job.get("steps", []):
            if step.get("id") == CLASSIFIER_STEP_ID:
                return cast(str, step["run"])
    raise AssertionError(f"no step with id {CLASSIFIER_STEP_ID!r} in {WORKFLOW_PATH}")


def _blocks() -> tuple[re.Match[str], re.Match[str]]:
    """Return the (exclusion, doc-test) conditional blocks, in source order.

    They are told apart by what they DO, not by where they sit: the exclusion
    block disqualifies the PR, the doc block accepts the file and moves on.
    """
    script = _classifier_script()
    exclusion: re.Match[str] | None = None
    doc_test: re.Match[str] | None = None
    for match in _IF_BLOCK.finditer(script):
        body = match.group("body")
        if "is_docs_only=false" in body and exclusion is None:
            exclusion = match
        elif "continue" in body and doc_test is None:
            doc_test = match
    assert exclusion is not None, "no exclusion block found in the classifier"
    assert doc_test is not None, "no doc-pattern block found in the classifier"
    return exclusion, doc_test


def _patterns(block: re.Match[str]) -> list[str]:
    return _COMPARISON.findall(block.group("cond"))


def _as_extension(pattern: str) -> str | None:
    """`*.md` -> `.md`. Anything else is not an extension test."""
    return pattern[1:] if pattern.startswith("*.") else None


def _as_prefix(pattern: str) -> str | None:
    """`docs/*` -> `docs/`. Anything else is not a directory test."""
    return pattern[:-1] if pattern.endswith("/*") else None


def test_every_workflow_pattern_is_EXPRESSIBLE_on_the_safe_merge_side() -> None:
    """Guards the guard: an unparseable pattern must fail, never be discarded.

    ⚠ This is the ticket's own failure mode, one level up (Codex ckpt-2 P2).
    ``LICENSE`` was once on the workflow's list and is **inexpressible** in
    ``is_doc_only``: its ``doc_only_paths`` entries are normalised with
    ``p="${p%/}/"``, so an extensionless FILE can only be written as an exclude,
    which does the opposite thing. If a bare ``"$f" == LICENSE`` were re-added
    here, the three list tests below would still pass — the comprehensions only
    look at ``*.ext`` and ``dir/*`` forms and would drop it silently — and the
    unmergeable trap would be back with a green suite.

    So the recognised forms are a closed set, and anything else fails HERE with
    a message rather than widening the classifier behind the tests' back.
    """
    exclusion, doc_test = _blocks()
    for block in (exclusion, doc_test):
        for pattern in _patterns(block):
            assert _as_extension(pattern) or _as_prefix(pattern), (
                f"workflow pattern {pattern!r} is neither `*.ext` nor `dir/*`. "
                "safe_merge.sh::is_doc_only cannot express it (an extensionless file "
                "is only expressible as an exclude, which means the opposite), so the "
                "two classifiers would disagree. Do not widen this test to admit it."
            )


def test_the_config_declares_all_three_lists() -> None:
    """A missing key makes `safe_merge.sh` abort its whole gate silently."""
    merge_gate = _merge_gate()
    for key in ("doc_only_extensions", "doc_only_paths", "doc_only_excludes"):
        assert key in merge_gate, f"{key} missing from merge_gate in {CONFIG_PATH}"
        assert merge_gate[key], f"{key} is empty; declare it explicitly or the engine default applies"


def test_extension_lists_agree() -> None:
    """#2849 second pass: the workflow also accepted `.mdx` and `.rst`.

    An `.rst`-only PR was skipped here and refused by `safe_merge.sh` — the same
    unmergeable trap, on an extension no file in the repo happened to have.
    """
    _, doc_test = _blocks()
    workflow_extensions = {ext for p in _patterns(doc_test) if (ext := _as_extension(p))}
    assert workflow_extensions == set(_merge_gate()["doc_only_extensions"])


def test_doc_path_lists_agree() -> None:
    _, doc_test = _blocks()
    workflow_paths = {prefix for p in _patterns(doc_test) if (prefix := _as_prefix(p))}
    config_paths = {p if p.endswith("/") else f"{p}/" for p in _merge_gate()["doc_only_paths"]}
    assert workflow_paths == config_paths


def test_exclusion_lists_agree() -> None:
    """`.autonomy/` is behaviour wearing a `.md` extension. Never doc-only."""
    exclusion, _ = _blocks()
    workflow_excludes = {prefix for p in _patterns(exclusion) if (prefix := _as_prefix(p))}
    config_excludes = {p if p.endswith("/") else f"{p}/" for p in _merge_gate()["doc_only_excludes"]}
    assert workflow_excludes == config_excludes
    assert ".autonomy/" in workflow_excludes, "the loop's own standing order must never skip review"


def test_exclusions_are_tested_before_the_doc_patterns() -> None:
    """Order is load-bearing, not stylistic.

    `is_doc_only` checks excludes FIRST so they cannot be out-voted. If the
    workflow tested `*.md` first, `.autonomy/loop_prompt.md` would `continue` as
    an ordinary doc and the exclusion would never be reached — the lists would
    agree and the verdicts still would not.
    """
    exclusion, doc_test = _blocks()
    assert exclusion.start() < doc_test.start()


@pytest.mark.parametrize(
    ("path", "expected_doc_only"),
    [
        (".autonomy/loop_prompt.md", False),
        (".autonomy/config.yaml", False),
        ("docs/settled-decisions.md", True),
        ("docs/proposals/ta/whatever.md", True),
        ("README.md", True),
        ("app/api/strategies.py", False),
        ("LICENSE", False),
        ("notes.rst", False),
    ],
)
def test_the_agreed_lists_classify_the_fixed_table_as_documented(path: str, expected_doc_only: bool) -> None:
    """The table #2849 asked for, evaluated against the lists both sides share.

    ⚠ This applies the RULE to the shared lists; it does not re-implement either
    side's predicate (a third implementation is the defect, not the fix). It is
    meaningful only because the tests above pin those lists to both sides.
    """
    merge_gate = _merge_gate()
    excludes = [p if p.endswith("/") else f"{p}/" for p in merge_gate["doc_only_excludes"]]
    prefixes = [p if p.endswith("/") else f"{p}/" for p in merge_gate["doc_only_paths"]]
    extensions = list(merge_gate["doc_only_extensions"])

    if any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in excludes):
        doc_only = False
    elif any(path.endswith(ext) for ext in extensions):
        doc_only = True
    else:
        doc_only = any(path.startswith(prefix) for prefix in prefixes)

    assert doc_only is expected_doc_only
