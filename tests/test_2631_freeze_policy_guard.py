"""#2631 — the freeze-time policy-version surface and guard.

A preregistration freeze is the most irreversible write in the repo: ``sql/333``
bars UPDATE and DELETE and holds ``UNIQUE (strategy_id, strategy_version)``, and
``declaration_refusals`` voids the row for good once
``STRUCTURAL_REFUSAL_POLICY_VERSION`` moves. These tests cover the two things
that stand between the operator and that: the dry-run showing the policy version
at all, and the guard refusing a freeze under an unmerged one.
"""

from __future__ import annotations

import json
import re
from dataclasses import fields, replace
from pathlib import Path

import pytest

from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
from scripts import _prereg_freeze_guard as guard
from scripts import freeze_2582_schedule13d_declaration as c4_freeze
from scripts import freeze_2616_precutoff_declarations as precutoff_freeze

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _declaration() -> PreregDeclaration:
    return PreregDeclaration(
        strategy_id="probe-candidate",
        strategy_version="probe-v1",
        contract_version="probe-contract-2026-08-13",
        prereg_purpose="falsification_only",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis="survivor_only",
        declared_carry_unmodelled=True,
        declared_fx_unmodelled=True,
        expected_structural_refusals=(
            "universe_basis_not_survivorship_free",
            "carry_unmodelled",
            "fx_unmodelled",
        ),
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=267,
            min_calendar_weeks=64,
            derivation="probe derivation",
        ),
        declared_by="tests/test_2631_freeze_policy_guard.py",
    )


# --------------------------------------------------------------------------
# The digest payload — what the dry-run now prints
# --------------------------------------------------------------------------


def test_digest_payload_has_one_key_per_declared_field() -> None:
    """A field added to either dataclass and forgotten in the payload fails here.

    ⚠ THIS TEST IS THE REASON THE "by construction" CLAIM HOLDS. ``digest_payload``
    being a property does NOT by itself stop a new field going missing — it would
    then be absent from the digest and the dry-run together, silently. The count
    is what notices.
    """
    payload = _declaration().digest_payload
    declaration_fields = {f.name for f in fields(PreregDeclaration)}
    expected_count = len(declaration_fields) - 1 + len(fields(ForwardShadowFloor))

    assert len(payload) == expected_count
    assert declaration_fields - {"forward_shadow"} <= set(payload)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("strategy_id", "other-candidate"),
        ("strategy_version", "probe-v2"),
        ("contract_version", "probe-contract-2026-08-14"),
        ("prereg_purpose", "capital_candidate"),
        ("structural_refusal_policy_version", "structural-refusal-policy-2999-01-01-v9"),
        ("declared_universe_basis", "survivorship_free"),
        ("declared_carry_unmodelled", False),
        ("declared_fx_unmodelled", False),
        ("expected_structural_refusals", ("carry_unmodelled",)),
        ("declared_by", "somebody else"),
    ],
)
def test_every_declared_field_moves_the_digest(field_name: str, value: object) -> None:
    base = _declaration()
    assert getattr(base, field_name) != value, "the probe value must actually differ"
    assert replace(base, **{field_name: value}).sha256 != base.sha256


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("min_independent_decision_dates", 268),
        ("min_calendar_weeks", 65),
        ("derivation", "a different derivation"),
    ],
)
def test_every_forward_shadow_field_moves_the_digest(field_name: str, value: object) -> None:
    base = _declaration()
    moved = replace(base, forward_shadow=replace(base.forward_shadow, **{field_name: value}))
    assert moved.sha256 != base.sha256


def test_digest_payload_is_a_fresh_dict_each_call() -> None:
    """A cached dict a caller mutated would print one thing and hash another."""
    declaration = _declaration()
    first = declaration.digest_payload
    first["strategy_id"] = "mutated"
    assert declaration.digest_payload["strategy_id"] == "probe-candidate"


# --------------------------------------------------------------------------
# The dry-run surface — #2631's actual symptom
# --------------------------------------------------------------------------


@pytest.mark.parametrize("module", [c4_freeze, precutoff_freeze])
def test_dry_run_prints_the_policy_version_and_every_digest_field(
    module: object, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The regression #2631 was filed for: the version was in neither summary."""
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: True)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: STRUCTURAL_REFUSAL_POLICY_VERSION)

    assert module.main(["--dry-run"]) == 0  # type: ignore[attr-defined]

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert lines, "a dry run must print something"
    for line in lines:
        printed = json.loads(line)
        assert printed["outcome"] == "dry_run"
        assert printed["structural_refusal_policy_version"] == STRUCTURAL_REFUSAL_POLICY_VERSION
        assert printed["policy_version_matches_main"] is True
        assert printed["declaration_sha256"]
        # ⚠ THE DECLARATION'S FIELD AND THE TREE'S CONSTANT ARE SEPARATE KEYS.
        # Merged under one name the dict would keep whichever was applied last
        # and show no sign the other existed — see the guard's docstring.
        assert printed["structural_refusal_policy_version_in_tree"] == STRUCTURAL_REFUSAL_POLICY_VERSION
        assert printed["structural_refusal_policy_version_on_main"] == STRUCTURAL_REFUSAL_POLICY_VERSION
        assert len({k for k in printed if k.startswith("structural_refusal_policy_version")}) == 3
        # Every digest input, not a subset — the payload's own keys.
        assert set(_declaration().digest_payload) <= set(printed)


# --------------------------------------------------------------------------
# The guard
# --------------------------------------------------------------------------


def test_guard_passes_when_the_tree_matches_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: True)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: STRUCTURAL_REFUSAL_POLICY_VERSION)
    report = guard.assert_policy_version_merged()
    assert report["policy_version_matches_main"] is True
    assert "policy_version_divergence_overridden" not in report


def test_guard_refuses_an_unmerged_policy_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: True)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: "structural-refusal-policy-2026-08-12-v1")
    with pytest.raises(SystemExit) as raised:
        guard.assert_policy_version_merged()
    message = str(raised.value)
    assert "structural-refusal-policy-2026-08-12-v1" in message
    assert STRUCTURAL_REFUSAL_POLICY_VERSION in message


def test_guard_fails_closed_when_main_cannot_be_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown is refused, never read as agreement — refusing costs one command."""
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: True)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: None)
    with pytest.raises(SystemExit) as raised:
        guard.assert_policy_version_merged()
    assert "could not be read" in str(raised.value)


def test_the_override_is_recorded_in_the_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """An override absent from the output would be a silent bypass."""
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: True)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: "structural-refusal-policy-2026-08-12-v1")
    report = guard.assert_policy_version_merged(allow_divergence=True)
    assert report["policy_version_divergence_overridden"] is True
    assert report["policy_version_matches_main"] is False


def test_guard_refuses_when_the_main_ref_could_not_be_refreshed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stale tracking ref matches for the same reason a correct one does.

    ⚠ Codex checkpoint 2. Without the fetch this guard compares the tree against
    ``refs/remotes/origin/main`` as the last fetch left it, so a checkout that
    has not fetched since the policy moved sees v1 == stale v1 and freezes under
    a version already superseded upstream — the precise failure it exists to
    stop. An unrefreshed ref is therefore no evidence at all, matching or not.
    """
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: False)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: STRUCTURAL_REFUSAL_POLICY_VERSION)
    with pytest.raises(SystemExit) as raised:
        guard.assert_policy_version_merged()
    assert "git fetch origin main" in str(raised.value)


def test_the_report_carries_the_refresh_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: False)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: STRUCTURAL_REFUSAL_POLICY_VERSION)
    assert guard.policy_version_report()["main_ref_refreshed"] is False
    assert guard.assert_policy_version_merged(allow_divergence=True)["main_ref_refreshed"] is False


@pytest.mark.parametrize(("returncode", "expected"), [(0, True), (1, False), (128, False)])
def test_refreshing_reports_the_fetch_exit_status(
    returncode: int, expected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(guard.subprocess, "run", lambda *a, **k: _Completed(returncode, ""))
    assert guard.refresh_main_ref() is expected


def test_refreshing_reports_false_when_git_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_args: object, **_kwargs: object) -> None:
        raise OSError("git not found")

    monkeypatch.setattr(guard.subprocess, "run", _raise)
    assert guard.refresh_main_ref() is False


class _Completed:
    def __init__(self, returncode: int, stdout: str) -> None:
        self.returncode = returncode
        self.stdout = stdout


@pytest.mark.parametrize(
    ("returncode", "stdout"),
    [
        (128, ""),  # no origin/main ref, or not a git repository
        (0, "# a strategy_result.py with no such constant\n"),  # the constant moved or was renamed
    ],
)
def test_reading_main_returns_none_when_the_constant_cannot_be_recovered(
    returncode: int, stdout: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``None`` is the *unknown* answer, and the guard must never see anything else here.

    ⚠ Probed: returning the local constant from this path instead of ``None``
    turns "I could not check" into "it agrees", and the fail-closed test above
    cannot see that because it patches this function out.
    """
    monkeypatch.setattr(guard.subprocess, "run", lambda *a, **k: _Completed(returncode, stdout))
    assert guard.policy_version_on_main() is None


def test_reading_main_returns_none_when_git_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_args: object, **_kwargs: object) -> None:
        raise OSError("git not found")

    monkeypatch.setattr(guard.subprocess, "run", _raise)
    assert guard.policy_version_on_main() is None


def test_reading_main_recovers_the_constant_from_git_output(monkeypatch: pytest.MonkeyPatch) -> None:
    source = (_REPO_ROOT / guard.POLICY_VERSION_SOURCE).read_text()
    monkeypatch.setattr(guard.subprocess, "run", lambda *a, **k: _Completed(0, source))
    assert guard.policy_version_on_main() == STRUCTURAL_REFUSAL_POLICY_VERSION


def test_the_regex_reads_the_constant_out_of_the_real_source_file() -> None:
    """The extraction is a regex over source; pin it to the actual declaration form.

    ``git show`` hands back the same bytes this reads from disk, so a change to
    how the constant is written (annotation dropped, quotes swapped) fails here
    rather than silently returning ``None`` — which the guard would then read as
    unknown and refuse forever.
    """
    source = (_REPO_ROOT / guard.POLICY_VERSION_SOURCE).read_text()
    match = guard._POLICY_VERSION_RE.search(source)
    assert match is not None
    assert match.group(1) == STRUCTURAL_REFUSAL_POLICY_VERSION


def test_every_freeze_script_calls_the_guard() -> None:
    """A convention a future third freeze script cannot forget silently.

    ⚠ SOURCE-LEVEL, AND ITS LIMITS ARE REAL: it proves the call is written, not
    that it runs before the INSERT, and it only covers files named
    ``scripts/freeze_*.py``. A writer that calls ``freeze_preregistration``
    under another name is not covered — recorded here rather than implied away.
    """
    scripts = sorted((_REPO_ROOT / "scripts").glob("freeze_*.py"))
    assert len(scripts) == 2, f"a new freeze script appeared: {[p.name for p in scripts]}"
    for path in scripts:
        source = path.read_text()
        # ⚠ THE CALL, NOT THE NAME. A bare substring check is satisfied by the
        # import line alone — found by the revert probe, which swapped the call
        # for `policy_version_report()` and watched this test pass.
        assert re.search(r"assert_policy_version_merged\s*\(", source), path.name
        assert re.search(r"freeze_preregistration\(", source), path.name
