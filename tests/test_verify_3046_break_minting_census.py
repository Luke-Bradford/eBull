"""#3046 — the census's one decision, table-tested.

``_bucket`` is the whole classifier: everything else in the script is SQL and
printing. Pure-logic, no DB, per the repo's lean-test rule.

⚠ The case that actually matters is ``t1_suppressed`` / ``t2_suppressed``: a
transition that CLEARS its class threshold and still carries no T3, because
``price_quarantine.py:486`` refuses to evaluate T3 once T1 or T2 has fired. Those
are the rows that mint no ``price_series_break`` and are invisible to
``price_segments``. A regression that folded them into ``below_threshold`` would
make the blind spot read as zero, which is what a PASS looks like.
"""

from __future__ import annotations

import pytest

from scripts.verify_3046_break_minting_census import _BUCKET_NOTES, BLIND_BUCKETS, bucket_for


@pytest.mark.parametrize(
    ("rules", "clears", "provisional", "expected"),
    [
        # T3 wins outright — it is the arm the break table reconciles against.
        (["T3"], True, False, "t3_minted"),
        # ⚠ A T3 row cannot also carry T1/T2 (line 486 refuses to evaluate it
        # then), but the classifier must not depend on that to stay correct.
        (["T1", "T3"], True, False, "t3_minted"),
        (["T3"], False, False, "t3_minted"),
        # The blind spot.
        (["T1"], True, False, "t1_suppressed"),
        (["T2"], True, False, "t2_suppressed"),
        (["T1", "T2"], True, False, "t1_t2_suppressed"),
        # Same rules UNDER the threshold are not a blind spot — nothing was
        # declined, the move was ordinary.
        (["T1"], False, False, "below_threshold"),
        (["T2"], False, False, "below_threshold"),
        (["T1", "T2"], False, False, "below_threshold"),
        # Empty rules at magnitude: T3 ran and answered, or was deferred.
        ([], True, False, "admitted_back"),
        ([], True, True, "deferred"),
        ([], False, False, "below_threshold"),
        ([], False, True, "below_threshold"),
    ],
)
def test_bucket_assignment(rules: list[str], clears: bool, provisional: bool, expected: str) -> None:
    assert bucket_for(rules, clears, provisional) == expected


def test_admitted_back_is_not_counted_as_blind() -> None:
    """T3 ran and said 'real move'. That is an answer, not a blind spot.

    Folding it in would inflate the suppression figure with the one population
    the classifier DID adjudicate.
    """
    assert "admitted_back" not in BLIND_BUCKETS
    assert "deferred" not in BLIND_BUCKETS
    assert set(BLIND_BUCKETS) == {"t1_suppressed", "t2_suppressed", "t1_t2_suppressed"}


def test_every_bucket_the_classifier_can_emit_has_a_printed_note() -> None:
    """A bucket with no note prints a KeyError instead of a census."""
    emitted = {
        bucket_for(rules, clears, provisional)
        for rules in ([], ["T1"], ["T2"], ["T3"], ["T1", "T2"], ["T1", "T3"])
        for clears in (True, False)
        for provisional in (True, False)
    }
    assert emitted <= set(_BUCKET_NOTES)
