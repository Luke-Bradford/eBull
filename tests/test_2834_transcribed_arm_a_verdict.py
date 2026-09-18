"""#2834 ARM A — the committed cost-bar verdict must be the one the declared rule produced.

ARM A's verdict is NOT reproducible on demand, and that is why it is committed rather
than re-derived. `docs/proposals/ta/2026-09-16-arm-a-tilt-selection.md` section 7 lists the
inherited weaknesses that make a re-run a different measurement: eligibility is read live
with no age limit (a credential rotation moves it), observations and proofs are read in
separate statements with no snapshot pin, and the observation table's immutability trigger
bars UPDATE but not DELETE or a backfilled INSERT — either of which changes which five
dates are first. So the artefact is the evidence, and this file is what stops the artefact
drifting away from the declaration and the code that emitted it.

Unlike #2833 there is no transcribed module constant to check: ARM A has no production
consumer (its execution half is refused at `SESSION_SUPPORTED_ASSET_CLASSES`, person-gated
on #2312), so inventing a `SELECTED_TILT_*` nothing reads would be worse than none. Every
assertion here is therefore payload-vs-declaration or payload-vs-source-digest.

⚠ Stated limitation, not covered: the payload pins `declaration_sha256` and
`verifier_sha256` but carries no digest of `scripts/_core_selection_rule.py`, where the
rule actually lives. `assert_verifier_sources_clean` checks that module against `HEAD` at
RUN time, but nothing in the committed result records which version ran. The same gap
applies to #2833's artefact.

Pure-logic tier: reads two committed files and hashes three. No DB, no client harness.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Any

from scripts._core_selection_rule import sha256_of
from scripts.verify_2834_arm_a_selection import (
    DECLARATION_PATH,
    DECLARATION_SHA256,
    VERIFIER_PATH,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

#: The transcribed verdict. Named here rather than imported, because no module imports it
#: — see the docstring. A rename therefore has to change this line, which is the point.
RESULT_PATH = Path("docs/proposals/ta/2026-09-18-arm-a-tilt-selection-result.json")


def _load(path: Path) -> dict[str, Any]:
    resolved = _REPO_ROOT / path
    assert resolved.is_file(), f"committed evidence is missing: {path}"
    with resolved.open(encoding="utf-8") as handle:
        loaded: dict[str, Any] = json.load(handle)
    return loaded


def _payload() -> dict[str, Any]:
    return _load(RESULT_PATH)


def _declaration() -> dict[str, Any]:
    return _load(DECLARATION_PATH)


def _git(*args: str) -> subprocess.CompletedProcess[bytes]:
    """Run git in the repo, never inheriting the caller's cwd.

    The two commit fields are the only payload values nothing else in this file can check
    — every other one is a digest, a declared constant or arithmetic over the candidate
    rows. Checking them costs two subprocess calls and needs real history, which is why it
    lives in the pure-logic tier and not in CI: this repo's CI does not run pytest, so
    these assertions are a pre-push gate. A shallow clone would fail them loudly rather
    than skip, which is the intended direction — a commit this artefact names and the
    checkout cannot resolve is not a provenance claim anyone can act on.
    """
    return subprocess.run(
        ["git", "-C", str(_REPO_ROOT), *args],
        capture_output=True,
        check=False,
        # Bounded so a git that blocks — a credential or submodule prompt on somebody
        # else's checkout — surfaces as a failed test rather than a stalled suite. These
        # are three object-store reads; they take milliseconds or they are wedged.
        timeout=30,
    )


def test_the_payload_is_an_OPENED_verdict_and_not_a_readiness_report() -> None:
    """The verifier emits two shapes and only one of them may be committed.

    Before the boundary it reports readiness — `outcome: "evidence_collecting"`, a
    `common_dates_observed`/`required_common_dates` pair and deliberately NO candidate
    statistic. Keying on the ABSENCE of the readiness fields, rather than on
    `outcome != "evidence_collecting"` alone, is what makes "wrong shape" a failure here
    instead of a `KeyError` in whichever test touched it first.
    """
    payload = _payload()
    assert "candidates" in payload, "a readiness report carries no candidate statistics"
    assert "common_dates_observed" not in payload, "this is the pre-boundary readiness shape"
    assert payload["outcome"] in {"pass", "partial", "fail"}
    assert payload["measured_at"] >= payload["window_dates"][-1]


def test_the_verdict_is_per_candidate_and_names_no_winner() -> None:
    """ARM A holds momentum AND quality AND value. A `selected_instrument_id` would mean
    the payload was produced under #2833's `select_one` rule, which reports a cheapest
    winner — a misleading output for a tilt sleeve, and a different rule than the one
    declared."""
    payload = _payload()
    declaration = _declaration()
    assert payload["verdict_mode"] == "per_candidate"
    assert payload["verdict_mode"] == declaration["verdict_mode"]
    assert payload["schema_version"] == declaration["schema_version"]
    assert "selected_instrument_id" not in payload
    assert "selected_symbol" not in payload


def test_the_payload_pins_the_declaration_bytes_that_are_committed_here() -> None:
    """Three sources for one digest, and all three must agree: the payload's own claim,
    the verifier's frozen constant, and the declaration file as it sits on disk. A
    declaration edited after the verdict opened would otherwise leave the result looking
    fully provenanced while describing a rule nobody can now read."""
    declaration_digest = sha256_of(_REPO_ROOT / DECLARATION_PATH)
    assert _payload()["declaration_sha256"] == declaration_digest
    assert DECLARATION_SHA256 == declaration_digest


def test_the_payload_pins_the_verifier_that_emitted_it() -> None:
    """Deliberately stricter than #2833's equivalent, which only asserts the key is
    non-empty. The verdict is terminal and not reproducible, so "this result came out of
    the code currently committed" is a property worth failing on. If this breaks because
    the verifier was legitimately edited, the fix is to re-open and re-transcribe, not to
    relax the assertion."""
    assert _payload()["verifier_sha256"] == sha256_of(_REPO_ROOT / VERIFIER_PATH)


def test_the_execution_commit_is_in_this_history() -> None:
    """`execution_commit` is the code the verdict was measured at. A hash that is not an
    ancestor of `HEAD` names a state this branch never had — a typo, or a payload copied
    from somewhere else."""
    commit = _payload()["execution_commit"]
    assert _git("cat-file", "-e", f"{commit}^{{commit}}").returncode == 0, (
        f"execution_commit {commit} does not resolve to a commit in this checkout"
    )
    assert _git("merge-base", "--is-ancestor", commit, "HEAD").returncode == 0, (
        f"execution_commit {commit} is not an ancestor of HEAD"
    )


def test_the_declaration_commit_holds_the_declared_bytes() -> None:
    """Stronger than "it matches `git log -1` on the path", and rename-proof: read the
    declaration blob AS IT WAS at `declaration_commit` and hash it. That is the claim the
    field is actually making — this commit contains the declaration this verdict was
    measured against — and it catches a wrong hash that happens to be a real commit."""
    payload = _payload()
    commit = payload["declaration_commit"]
    assert _git("merge-base", "--is-ancestor", commit, "HEAD").returncode == 0, (
        f"declaration_commit {commit} is not an ancestor of HEAD"
    )
    blob = _git("cat-file", "blob", f"{commit}:{DECLARATION_PATH.as_posix()}")
    assert blob.returncode == 0, f"{DECLARATION_PATH} does not exist at {commit}"
    assert hashlib.sha256(blob.stdout).hexdigest() == payload["declaration_sha256"]


def test_the_window_is_the_declared_one() -> None:
    payload = _payload()
    declaration = _declaration()
    window = payload["window_dates"]
    assert len(window) == int(declaration["required_common_utc_dates"])
    assert window == sorted(window), "the first N common dates are taken in order"
    assert len(set(window)) == len(window)
    not_before = str(declaration["evidence_not_before"])[:10]
    assert min(window) >= not_before, "a pre-boundary date cannot consume a window slot"


def test_every_declared_candidate_is_reported_exactly_once() -> None:
    """A candidate silently dropped from the payload would turn a blocked verdict into a
    pass over the remainder — the inherited `readiness is shared` rule exists precisely so
    that cannot happen (spec section 7)."""
    payload = _payload()
    reported = sorted(candidate["instrument_id"] for candidate in payload["candidates"])
    assert reported == sorted(int(value) for value in _declaration()["candidate_ids"])


def test_the_outcome_follows_from_the_candidate_verdicts() -> None:
    """`pass` only when EVERY candidate passes, `partial` when some do, `fail` when none
    do. Asserted against the candidate rows rather than trusting the reported outcome, so
    an internally inconsistent payload says so here."""
    payload = _payload()
    candidates = payload["candidates"]
    passing = [candidate for candidate in candidates if candidate["verdict"] == "PASS"]
    expected = "pass" if len(passing) == len(candidates) else ("partial" if passing else "fail")
    assert payload["outcome"] == expected
    assert payload["passing_instrument_ids"] == sorted(candidate["instrument_id"] for candidate in passing)


def test_every_passing_candidate_is_under_the_declared_bar() -> None:
    """The bar is read from the declaration, never restated. `p75_spread_bps` is carried
    as a string so the comparison stays exact — the refusal boundary is `>`, so a
    candidate exactly at the bar passes."""
    declaration = _declaration()
    bar = Decimal(str(declaration["pass_bar_bps"]))
    over_bar_refusal = f"cost_above_{declaration['pass_bar_bps']}_bps"
    for candidate in _payload()["candidates"]:
        p75 = Decimal(candidate["p75_spread_bps"])
        if candidate["verdict"] == "PASS":
            assert p75 <= bar, f"{candidate['symbol']} PASSes at {p75} bps against a {bar} bps bar"
            assert over_bar_refusal not in candidate["refusals"]
        elif p75 > bar:
            assert over_bar_refusal in candidate["refusals"]


def test_every_failing_candidate_states_why() -> None:
    """A FAIL with no refusal means the payload lost the reason. ARM A's three candidates
    all pass today; this holds the shape for a re-transcription that does not."""
    for candidate in _payload()["candidates"]:
        if candidate["verdict"] == "FAIL":
            assert candidate["refusals"], f"{candidate['symbol']} FAILs with no stated refusal"


def test_the_binding_statistic_is_not_below_the_descriptive_one() -> None:
    """p75 >= p50 by construction for every candidate. Cheap, and it is the one check that
    would catch the two percentile columns being swapped in the emitted payload."""
    for candidate in _payload()["candidates"]:
        assert Decimal(candidate["p75_spread_bps"]) >= Decimal(candidate["median_spread_bps"])


def test_the_eligibility_proof_behind_each_candidate_is_identified() -> None:
    """The verdict is only a cost-bar verdict on a product PROVED to be a real long at x1.
    Without the proof id and response digest the payload cannot be tied back to the
    eligibility call that established that."""
    for candidate in _payload()["candidates"]:
        assert candidate["eligibility_proof_id"], f"{candidate['symbol']} names no eligibility proof"
        assert candidate["eligibility_response_digest"], f"{candidate['symbol']} names no response digest"
        assert candidate["eligibility_observed_at"]
