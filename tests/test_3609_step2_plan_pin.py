"""#3609 step 2's exposure plan is pinned: the committed plan file is the one register r25 cites, and it was written
by the committed planner and its import closure (spec premise 6). File and source hashes only; the plan is not rerun.

The planner and its closure are hashed at the commit that last wrote the plan, not at HEAD: the plan is a record of
the code that ran, and step 2 is concluded (#3609, 2026-10-08), so later edits to a module it imports (#3730's
holding-return clip in the panel builder) do not falsify it. Regenerating the plan moves that commit with it."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
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


def _git(*args: str) -> bytes:
    # Scrubbed of GIT_*: the pre-push hook exports GIT_DIR, which would otherwise redirect the read.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, check=True, env=env).stdout


def test_the_plan_was_written_by_the_committed_planner(tmp_path: Path) -> None:
    """The last commit that changed the plan's bytes is the commit that wrote it: any later edit to the file changes
    its sha256, which the register pins (the test above), so a reformat or regeneration cannot move this point
    without failing there first. A rename moves ``PLAN_PATH`` and its history with it. Needs full history: the
    pre-push hook runs pytest in a full clone, and CI does not run pytest. A shallow clone without the commit fails
    loudly here, which is correct, because the pin then cannot be verified."""
    plan = json.loads(PLAN_PATH.read_bytes())
    commit = _git("log", "-1", "--format=%H", "--", PLAN_PATH.relative_to(REPO).as_posix()).decode().strip()
    assert commit, "the plan file has no commit"
    with tarfile.open(fileobj=io.BytesIO(_git("archive", commit, "app", "scripts"))) as archive:
        archive.extractall(tmp_path, filter="data")
    planner = tmp_path / PLANNER.relative_to(REPO)
    assert plan["script_sha256"] == hashlib.sha256(planner.read_bytes()).hexdigest()
    assert plan["code_closure_sha256"] == construction_sha256([planner], tmp_path)


def test_the_plan_meets_its_declared_criterion() -> None:
    condition_3 = json.loads(PLAN_PATH.read_bytes())["condition_3"]
    assert condition_3["target"] == 0.8 and condition_3["conjunction_bound"] >= condition_3["target"]
