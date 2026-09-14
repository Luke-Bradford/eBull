"""#2795 — the shared FINRA ingest sentinels. Pure, no DB.

The bimonthly's own behaviour stays pinned by
``tests/test_finra_short_interest_sentinels.py``, which is deliberately UNCHANGED
by the lift. This file covers the shared helper directly, and in particular the
axis that is new: ``retention_floor=None``.

⚠ **The boundary probe below is the one that earns its keep.** #2337's first
attempt built its "exactly at the floor" case as ``ceil(prev * floor)`` —
5145/5716 = 0.90010, which is *near* the boundary, where ``<`` and ``<=`` agree —
so the revert probe flipping the comparison was NOT caught. Integer counts rarely
land exactly on a float threshold, so the counts here are chosen to divide
exactly and the test asserts that they do BEFORE asserting the behaviour.
"""

from __future__ import annotations

import pytest

from app.services.finra_ingest_sentinels import SentinelFinding, evaluate_ingest_sentinels


def _evaluate(
    *,
    failed: bool = False,
    rows_parsed: int = 1_000,
    rows_resolved: int = 900,
    skipped_invalid_row: int = 0,
    previous: tuple[str, int] | None = ("2026-05-01", 1_000),
    retention_floor: float | None = 0.90,
) -> list[SentinelFinding]:
    return evaluate_ingest_sentinels(
        key="2026-05-15/CNMS",
        failed=failed,
        rows_parsed=rows_parsed,
        rows_resolved=rows_resolved,
        skipped_invalid_row=skipped_invalid_row,
        previous=previous,
        retention_floor=retention_floor,
    )


# ----------------------------------------------------------------------
# Silence
# ----------------------------------------------------------------------


def test_a_healthy_file_yields_nothing() -> None:
    assert _evaluate() == []


def test_a_failed_file_yields_nothing() -> None:
    """Already surfaced by the partial-failure RuntimeError contract."""
    assert _evaluate(failed=True, rows_resolved=0, skipped_invalid_row=5) == []


def test_a_zero_parsed_file_yields_nothing() -> None:
    """Header plus a ``0`` footer is a documented success path on every prefix."""
    assert _evaluate(rows_parsed=0, rows_resolved=0) == []


@pytest.mark.parametrize("previous", [None, ("2026-05-01", 0)])
def test_no_usable_baseline_disables_the_retention_arm(previous: tuple[str, int] | None) -> None:
    assert _evaluate(rows_resolved=1, previous=previous) == []


def test_growth_never_fires() -> None:
    """The floor is one-sided — a bigger file is not a fault."""
    assert _evaluate(rows_resolved=999_99, previous=("2026-05-01", 1_000)) == []


# ----------------------------------------------------------------------
# Arm 1 — row shape
# ----------------------------------------------------------------------


def test_one_invalid_row_fires_row_shape() -> None:
    """Healthy value is 0 BY CONSTRUCTION, so the arm has no threshold to tune."""
    findings = _evaluate(skipped_invalid_row=1)
    assert [f.kind for f in findings] == ["row_shape"]
    assert "1 of 1000" in findings[0].detail
    assert findings[0].key == "2026-05-15/CNMS"


# ----------------------------------------------------------------------
# Arm 2 — zero resolution
# ----------------------------------------------------------------------


def test_zero_resolution_fires_and_suppresses_retention() -> None:
    """One fault, reported once.

    0 / anything is below every floor, so letting the retention arm run too would
    report the same fault twice under two different names.
    """
    findings = _evaluate(rows_resolved=0)
    assert [f.kind for f in findings] == ["no_resolution"]


def test_zero_resolution_still_reports_row_shape_alongside() -> None:
    """An entirely invalid body resolves nothing AND is a shape fault.

    Arm 1 firing alongside is what distinguishes "the file is malformed" from
    "the resolver or universe is broken" — arm 2's wording names the likeliest
    cause, not the only one.
    """
    assert [f.kind for f in _evaluate(rows_resolved=0, skipped_invalid_row=1_000)] == [
        "row_shape",
        "no_resolution",
    ]


# ----------------------------------------------------------------------
# Arm 3 — retention, and its absence
# ----------------------------------------------------------------------


def test_retention_below_the_floor_fires() -> None:
    findings = _evaluate(rows_resolved=500, previous=("2026-05-01", 1_000))
    assert [f.kind for f in findings] == ["universe_drift"]
    assert "0.5000 retention" in findings[0].detail
    assert "2026-05-01" in findings[0].detail


def test_retention_exactly_at_the_floor_is_silent() -> None:
    """⚠ Pin the boundary with counts that divide EXACTLY — see the module docstring."""
    resolved, baseline, floor = 900, 1_000, 0.90
    assert resolved / baseline == floor, "the probe must sit ON the boundary, not near it"
    assert (
        evaluate_ingest_sentinels(
            key="k",
            failed=False,
            rows_parsed=1_000,
            rows_resolved=resolved,
            skipped_invalid_row=0,
            previous=("2026-05-01", baseline),
            retention_floor=floor,
        )
        == []
    )
    assert evaluate_ingest_sentinels(
        key="k",
        failed=False,
        rows_parsed=1_000,
        rows_resolved=resolved - 1,
        skipped_invalid_row=0,
        previous=("2026-05-01", baseline),
        retention_floor=floor,
    )


def test_a_none_floor_omits_the_arm_rather_than_loosening_it() -> None:
    """RegSHO's configuration. The arm is ABSENT, not permissive.

    ⚠ This is the axis the lift introduced and the one a regression would land
    on: a ``None`` floor silently coerced to 0.0 would pass every other test in
    this file while reintroducing an uncalibratable arm under a new name. The
    input here would fire at any floor above 0.01.
    """
    assert _evaluate(rows_resolved=1, previous=("2026-05-01", 1_000), retention_floor=None) == []


def test_a_none_floor_leaves_arms_1_and_2_active() -> None:
    """Disabling retention must not disable the boundary arms."""
    assert [f.kind for f in _evaluate(skipped_invalid_row=3, retention_floor=None)] == ["row_shape"]
    assert [f.kind for f in _evaluate(rows_resolved=0, retention_floor=None)] == ["no_resolution"]
