"""Pin the 5-vs-7 ownership category asymmetry between
``sync_all`` (legacy-mirror dispatcher) and ``_CATEGORIES`` (daily
drift-repair sweep).

Why: Architect lens in the Run-#8-readiness 8-lens committee flagged
"funds + esop silently never recompute" as BLOCKING. Investigation
(see ``project_etl_sweep_2026_05_24_rollup.md`` + Architect-lens memo)
confirmed Codex was right: the daily 03:30 UTC sweep covers all 7 via
``_CATEGORIES``; ``sync_all`` is legacy-mirror only. The asymmetry
is BY-DESIGN.

This test pins the shape so a future agent cannot "fix" the perceived
asymmetry by adding ``sync_funds`` / ``sync_esop`` — that would require
inventing legacy mirror sources that do not exist (funds = NPORT
write-through only; esop = transitive via ``sync_def14a``).

See also:

* ``app/services/ownership_observations_sync.py:797`` — ``sync_all``
  docstring explains the asymmetry.
* ``app/jobs/ownership_observations_repair.py:69`` — ``_CATEGORIES``
  has the cross-reference comment.
* ``.claude/skills/data-engineer/SKILL.md`` §write-through.
"""

from __future__ import annotations

from app.jobs.ownership_observations_repair import _CATEGORIES
from app.services.ownership_observations_sync import (
    SyncAllResult,
    sync_blockholders,
    sync_def14a,
    sync_insiders,
    sync_institutions,
    sync_treasury,
)


def test_sync_all_result_has_exactly_five_category_attrs() -> None:
    """``SyncAllResult`` exposes exactly 5 category-named attrs.

    These are the SOLE categories ``sync_all`` dispatches:
    insiders / institutions / blockholders / treasury / def14a.
    """
    expected = {"insiders", "institutions", "blockholders", "treasury", "def14a"}
    actual = {name for name in SyncAllResult.__dataclass_fields__ if name in expected}
    assert actual == expected, (
        f"sync_all categories drifted from canonical 5. expected={expected} actual={actual}. "
        f"If you genuinely need to extend, also update the docstring at "
        f"app/services/ownership_observations_sync.py:797 + the test invariant in "
        f"app/jobs/ownership_observations_repair.py:69 + data-engineer/SKILL.md §write-through."
    )


def test_repair_sweep_categories_has_exactly_seven() -> None:
    """``_CATEGORIES`` exposes exactly 7 categories: the 5 sync_all
    categories + funds + esop. This is the integrity floor for ALL
    ownership categories.
    """
    actual = {row[2] for row in _CATEGORIES}
    expected = {
        "insiders",
        "institutions",
        "blockholders",
        "treasury",
        "def14a",
        "funds",
        "esop",
    }
    assert actual == expected, (
        f"_CATEGORIES drifted from canonical 7. expected={expected} actual={actual}. "
        f"If a new category is added, ensure the daily 03:30 UTC sweep covers it "
        f"AND update the cross-ref comment + the sync_all asymmetry docstring."
    )


def test_categories_asymmetry_is_exactly_funds_and_esop() -> None:
    """Pin the exact 2-element delta. Funds + esop are the ONLY
    categories in ``_CATEGORIES`` but NOT in ``sync_all``.

    Any new category should land in BOTH simultaneously (the post-PR12
    invariant — both writer-paths must converge). Or, if introducing
    another asymmetric "event-driven only" category, this test fails
    loudly so the reviewer remembers to update the docstring.
    """
    sync_all_categories = {"insiders", "institutions", "blockholders", "treasury", "def14a"}
    repair_categories = {row[2] for row in _CATEGORIES}
    asymmetric = repair_categories - sync_all_categories
    assert asymmetric == {"funds", "esop"}, (
        f"Categories in repair sweep but NOT in sync_all drifted. "
        f"expected={{'funds', 'esop'}} actual={asymmetric}. See "
        f"app/services/ownership_observations_sync.py:797 docstring for why."
    )


def test_sync_category_callables_exist() -> None:
    """Belt-and-braces — confirms the 5 canonical sync functions are
    importable. Catches accidental rename.
    """
    assert callable(sync_insiders)
    assert callable(sync_institutions)
    assert callable(sync_blockholders)
    assert callable(sync_treasury)
    assert callable(sync_def14a)
