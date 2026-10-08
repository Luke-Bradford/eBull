"""#3609 step 2's exposure plan is pinned: the committed plan file is the one register r25 cites, and it was written
by the committed planner and its import closure (spec premise 6). File and source hashes only; the plan is not rerun.

Like the declaration's construction hash, these freeze the step 2 code the plan ran: an edit to the planner or a
module it imports fails here until the plan is regenerated and re-pinned."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from app.services.factor_book_declaration import construction_sha256
from app.services.trial_register import TRIAL_REGISTER
from scripts.plan_3609_step2_exposure import PLAN_PATH, REPO

PLANNER = Path(__file__).resolve().parents[1] / "scripts" / "plan_3609_step2_exposure.py"
TRIAL_ID = "3609-step2-exposure-planning-2026-10-08"


def _pinned_sha256() -> str:
    (trial,) = [t for t in TRIAL_REGISTER.trials if t.trial_id == TRIAL_ID]
    (sha,) = re.findall(r"3609-step2-exposure-plan\.json sha256=([0-9a-f]{64})", trial.evidence)
    return sha


def test_the_committed_plan_is_the_one_the_register_pins() -> None:
    assert hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest() == _pinned_sha256()


def test_the_plan_was_written_by_the_committed_planner() -> None:
    plan = json.loads(PLAN_PATH.read_bytes())
    assert plan["script_sha256"] == hashlib.sha256(PLANNER.read_bytes()).hexdigest()
    assert plan["code_closure_sha256"] == construction_sha256([PLANNER], REPO)


def test_the_plan_meets_its_declared_criterion() -> None:
    condition_3 = json.loads(PLAN_PATH.read_bytes())["condition_3"]
    assert condition_3["target"] == 0.8 and condition_3["conjunction_bound"] >= condition_3["target"]
