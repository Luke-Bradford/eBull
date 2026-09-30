"""#3515 §0 — the v1 wind-down rule and the policy guard's verdict, as pure functions."""

from __future__ import annotations

from dataclasses import replace

import pytest

from app.services.ai_trial_wind_down import V1Declaration, wind_down_refusal
from scripts.ai_trial_policy_guard import verdict

HASH = "a" * 64
DONE = V1Declaration(
    declaration_id=16,
    state="halted_harm",
    policy_hash=HASH,
    claimed_runs=0,
    undispatched_legs=0,
    open_trades=0,
    active_ownerships=0,
    unfinished_pairs=0,
)


@pytest.mark.parametrize(
    ("declaration", "expected"),
    [
        (DONE, None),
        (replace(DONE, state="halted_loss"), None),
        (replace(DONE, state="completed"), None),
        (replace(DONE, state="active"), "v1_not_wound_down:trial_active"),
        # Resumable by the supervisor: a pause is not an end (§0 rule 3).
        (replace(DONE, state="halted_operator"), "v1_not_wound_down:trial_halted_operator"),
        (replace(DONE, state="halted_mandate"), "v1_not_wound_down:trial_halted_mandate"),
        (replace(DONE, state=None), "v1_not_wound_down:trial_no_state"),
        (replace(DONE, state="finished"), "v1_not_wound_down:trial_unknown_state"),
        # Terminal, but v1 code still runs the exits.
        (replace(DONE, open_trades=1), "v1_not_wound_down:open_trade"),
        (replace(DONE, active_ownerships=1), "v1_not_wound_down:active_ownership"),
        (replace(DONE, claimed_runs=1), "v1_not_wound_down:claimed_run"),
        (replace(DONE, undispatched_legs=2), "v1_not_wound_down:undispatched_leg"),
        # Every trade closed, but the lifecycle writer still owes an event or a label.
        (replace(DONE, unfinished_pairs=1), "v1_not_wound_down:unfinished_pair"),
        # The state is reported first; every reason is still listed by outstanding().
        (replace(DONE, state="active", open_trades=3), "v1_not_wound_down:trial_active"),
    ],
)
def test_wind_down_refusal(declaration: V1Declaration, expected: str | None) -> None:
    assert wind_down_refusal([declaration]) == expected
    assert declaration.wound_down is (expected is None)


def test_no_v1_declaration_refuses_missing() -> None:
    assert wind_down_refusal([]) == "v1_not_wound_down:declaration_missing"


def test_every_declaration_must_be_wound_down_lowest_id_reported_first() -> None:
    later = replace(DONE, declaration_id=20, state="active")
    earlier = replace(DONE, declaration_id=9, claimed_runs=1)
    assert wind_down_refusal([later, DONE]) == "v1_not_wound_down:trial_active"
    assert wind_down_refusal([later, earlier]) == "v1_not_wound_down:claimed_run"
    assert replace(DONE, state="active", open_trades=3).outstanding() == ("trial_active", "open_trade")


def test_guard_compares_only_declarations_not_wound_down() -> None:
    live = replace(DONE, declaration_id=17, state="active")
    lines, ok = verdict(HASH, [DONE, live])
    assert ok
    assert "declaration 16: wound down (halted_harm), not compared" in lines[1]
    assert "declaration 17: MATCH" in lines[2]

    # A wound-down declaration with a different hash cannot fail the guard: it runs no v1 code.
    lines, ok = verdict("b" * 64, [DONE])
    assert ok and lines[-1] == "OK"


@pytest.mark.parametrize("stored", ["b" * 64, None])
def test_guard_refuses_a_live_declaration_whose_hash_differs(stored: str | None) -> None:
    lines, ok = verdict(HASH, [replace(DONE, state="active", policy_hash=stored)])
    assert not ok
    assert "MISMATCH" in lines[1]
    assert lines[-1].startswith("REFUSED")


def test_guard_passes_with_no_declaration() -> None:
    lines, ok = verdict(HASH, [])
    assert ok and lines[1] == "no v1 declaration: nothing live to protect"
