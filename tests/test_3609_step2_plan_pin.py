"""#3609 step 2's exposure plan is pinned: the committed plan file is the one register r25 cites, and it was written
by the committed planner (spec premise 6). Pure file checks; nothing is recomputed."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from app.services.trial_register import TRIAL_REGISTER
from scripts.plan_3609_step2_exposure import PLAN_PATH

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


def test_the_plan_meets_its_declared_criterion() -> None:
    condition_3 = json.loads(PLAN_PATH.read_bytes())["condition_3"]
    assert condition_3["target"] == 0.8 and condition_3["conjunction_bound"] >= condition_3["target"]
