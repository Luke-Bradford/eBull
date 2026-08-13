"""Shared freeze-time guard for the #2599 preregistration declarations (#2631).

Any script that calls ``freeze_preregistration`` must call
``assert_policy_version_merged()`` first. Pattern: ``scripts/_dev_guard.py``.

⚠⚠ WHY A FREEZE HAS A TIMING HAZARD AT ALL
---------------------------------------------------------------------------
A frozen declaration records the ``STRUCTURAL_REFUSAL_POLICY_VERSION`` it was
written under, and ``prereg_contract.declaration_refusals`` returns
``structural_refusal_policy_superseded`` the moment that string stops matching
the current constant. The row cannot be repaired: ``sql/333`` bars UPDATE and
DELETE outright and holds ``UNIQUE (strategy_id, strategy_version)``, so a
corrected row cannot be inserted either. Freeze under a policy version that then
moves and the trial's outcomes are refused **forever**; the only escape is a new
``strategy_version``, which changes the trial's identity — the one thing
preregistration exists to hold fixed.

⚠ WHAT THIS GUARD DOES, AND — MORE USEFULLY — WHAT IT DOES NOT
---------------------------------------------------------------------------
It refuses a freeze whose policy version is **not the merged one**: the constant
in this working tree against the constant on ``origin/main``, after fetching so
that "main" means main and not a local snapshot of unknown age. That is a state
that is objectively wrong rather than merely unlucky — a declaration frozen
under an unmerged policy version is frozen under a string that may never exist
on main, or exist there meaning something else.

It does **NOT** detect the 2026-08-12 near-miss that #2631 was filed for. That
bump sat on an *unpushed* branch in a different worktree while both main and the
operator's tree were still at v1, and no check that reads this repository can see
a commit that does not exist in it. The mitigation for that case is the
``--dry-run`` output printing the policy version and the operator knowing the
coupling — not this function.

⚠ Two rejected designs, both measured rather than argued (2026-08-12, dev):

* #2631's literal scope item 2 — "refuse when the tree's policy version differs
  from the one recorded on any existing declaration row". The table holds **0
  rows**, so it compares against nothing today; and because a bump supersedes
  *every* pre-existing row, the predicate becomes permanently true after the
  first one and would refuse every future freeze.
* Scanning every git ref not merged into ``origin/main`` for a differing
  constant. Of **31** such refs, 4 contain the constant and **2 differ** — both
  stale branches whose work is already squash-merged. It fires on day one and
  never stops.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Final

from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION

#: The file the constant is defined in, repo-relative — the path ``git show``
#: needs. Kept beside the regex so the two move together.
POLICY_VERSION_SOURCE: Final = "app/services/strategy_result.py"

_POLICY_VERSION_RE: Final = re.compile(
    r'^STRUCTURAL_REFUSAL_POLICY_VERSION\s*(?::[^=]*)?=\s*"([^"]+)"',
    re.MULTILINE,
)

_REPO_ROOT: Final = Path(__file__).resolve().parents[1]


def refresh_main_ref() -> bool:
    """``git fetch origin main``. ``False`` when the ref could not be refreshed.

    ⚠ WITHOUT THIS THE GUARD CHECKS A LOCAL SNAPSHOT, NOT MAIN (Codex checkpoint
    2). ``refs/remotes/origin/main`` is whatever the last fetch left behind; a
    checkout that has not fetched since the policy moved compares v1 against a
    stale v1 and reports a match — permitting exactly the freeze this refuses.

    ⚠ Measured rather than assumed (git 2.54.0, 2026-08-13): plain ``git fetch
    origin main`` DOES advance ``refs/remotes/origin/main``, not just
    ``FETCH_HEAD``. Probed by staling the ref with ``git update-ref`` to the
    prior commit and watching the fetch restore it.
    """
    try:
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
            ["git", "fetch", "--quiet", "origin", "main"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return False
    return completed.returncode == 0


def policy_version_on_main() -> str | None:
    """``STRUCTURAL_REFUSAL_POLICY_VERSION`` as it stands on ``origin/main``.

    ``None`` when it cannot be read at all — no git, no ``origin/main`` ref, no
    such file on that ref, or the constant not matching the expected literal
    form. Every one of those is treated as *unknown*, never as *agrees*.
    """
    try:
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
            ["git", "show", f"origin/main:{POLICY_VERSION_SOURCE}"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    match = _POLICY_VERSION_RE.search(completed.stdout)
    return None if match is None else match.group(1)


def policy_version_report() -> dict[str, object]:
    """The check's findings, for printing. Never raises, never refuses.

    ⚠ Reported by ``--dry-run`` precisely because that is the run whose whole
    job is to show the operator what a real freeze would commit to — including
    the fetch, so a dry run cannot report agreement the real freeze would refuse.

    ⚠ ``..._in_tree`` RATHER THAN ``structural_refusal_policy_version``, which is
    the declaration's own field and arrives in the same JSON from
    ``digest_payload`` (review nitpick, PR #2633). The two are equal for every
    writer that exists, because both freeze scripts build the declaration from
    this constant — but merging two dicts that share a key silently keeps one,
    so if they ever DID diverge the operator would see a single value and no
    sign that anything disagreed. Distinct keys make that visible instead, which
    is the same defect this PR exists to fix, one level down.
    """
    refreshed = refresh_main_ref()
    on_main = policy_version_on_main()
    return {
        "structural_refusal_policy_version_in_tree": STRUCTURAL_REFUSAL_POLICY_VERSION,
        "structural_refusal_policy_version_on_main": on_main,
        "main_ref_refreshed": refreshed,
        "policy_version_matches_main": on_main == STRUCTURAL_REFUSAL_POLICY_VERSION,
    }


def assert_policy_version_merged(*, allow_divergence: bool = False) -> dict[str, object]:
    """Refuse a real freeze unless this tree's policy version is the merged one.

    Returns the same report ``policy_version_report`` does, with the override
    recorded when one was used — an override that does not appear in the output
    is a silent bypass, which is the thing this repo does not do.

    ⚠ FAIL CLOSED on *unknown*. Refusing to freeze is recoverable in one
    command; freezing under the wrong policy version is not recoverable at all.
    """
    report = policy_version_report()
    if report["policy_version_matches_main"] and report["main_ref_refreshed"]:
        return report
    if allow_divergence:
        return {**report, "policy_version_divergence_overridden": True}
    if not report["main_ref_refreshed"]:
        raise SystemExit(
            "refusing to freeze: could not run `git fetch origin main`, so the policy version this "
            "check compared against is a local snapshot of unknown age. A checkout that has not "
            "fetched since the policy moved sees a stale match and would freeze under a version "
            "already superseded upstream — the failure this guard exists to prevent. Fetch and "
            "re-run, or pass --allow-policy-divergence if you have established freshness another way."
        )
    on_main = report["structural_refusal_policy_version_on_main"]
    detail = (
        f"origin/main carries {on_main!r}"
        if on_main is not None
        else f"origin/main's {POLICY_VERSION_SOURCE} could not be read (no git, no ref, or no constant)"
    )
    raise SystemExit(
        "refusing to freeze: this tree's STRUCTURAL_REFUSAL_POLICY_VERSION is "
        f"{STRUCTURAL_REFUSAL_POLICY_VERSION!r} and {detail}. A declaration frozen under an unmerged "
        "policy version is refused forever once the merged one wins, and sql/333 permits no repair "
        "(UPDATE and DELETE barred, the identity key taken). Merge or rebase first, or pass "
        "--allow-policy-divergence if you have established the divergence is harmless."
    )


__all__ = [
    "POLICY_VERSION_SOURCE",
    "assert_policy_version_merged",
    "policy_version_on_main",
    "policy_version_report",
    "refresh_main_ref",
]
