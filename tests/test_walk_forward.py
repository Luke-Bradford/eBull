"""Phase 5e-4 — criterion 5's purged walk-forward split.

Pure tier: no database.

⚠ THE PREDICATES ARE RESTATED FROM §5.3'S WORDING, NEVER IMPORTED. A test that
calls ``role`` to decide what ``role`` should return agrees with a shared
misreading (prevention log, #2240 S-3). Every expectation below is written as a
literal verdict against a hand-drawn axis.

⚠ THE FOLD COUNT AND THE MODEL ID ARE PINNED AS LITERALS. Importing
``FOLD_COUNT`` to assert ``FOLD_COUNT == FOLD_COUNT`` is the tautology the
#2240 5e-1 lesson names; the four is transcribed from the module's own stated
construction (a test fold is criterion 5's 25%).
"""

from __future__ import annotations

from datetime import date, timedelta
from itertools import pairwise

import pytest

from app.services.walk_forward import (
    FOLD_COUNT,
    ROLES,
    WALK_FORWARD_MODEL_ID,
    Fold,
    FoldCensus,
    FoldRecord,
    WalkForwardFolds,
    bar_weighted_folds,
    census,
    role,
    training_embargo_bars,
)

#: Transcribed, never imported — see the module docstring.
SPEC_FOLD_COUNT = 4
SPEC_MODEL_ID = "c5-purged-walk-forward-v1"
SPEC_ROLES = {"test", "train", "purged", "embargoed"}


def test_declared_constants_match_the_spec() -> None:
    assert FOLD_COUNT == SPEC_FOLD_COUNT
    assert WALK_FORWARD_MODEL_ID == SPEC_MODEL_ID
    assert set(ROLES) == SPEC_ROLES


# ---------------------------------------------------------------------------
# bar_weighted_folds
# ---------------------------------------------------------------------------


def test_equal_bars_per_date_cuts_the_axis_evenly() -> None:
    folds = bar_weighted_folds([1] * 12, fold_count=4)
    assert [(f.first_index, f.last_index) for f in folds] == [(0, 2), (3, 5), (6, 8), (9, 11)]
    assert [f.date_count for f in folds] == [3, 3, 3, 3]


def test_the_cut_follows_bars_not_dates() -> None:
    """⚠ §5.2's weighting: an unbalanced panel gets unequal DATE blocks.

    Twelve dates, but the last three carry 10 bars each and the first nine
    carry one. Total 39, so a quarter is 9.75 bars — reached inside the first
    dense date. A date-weighted cut would say (0,2)(3,5)(6,8)(9,11).
    """
    counts = [1] * 9 + [10, 10, 10]
    folds = bar_weighted_folds(counts, fold_count=4)
    assert [(f.first_index, f.last_index) for f in folds] == [(0, 8), (9, 9), (10, 10), (11, 11)]
    assert [sum(counts[f.first_index : f.last_index + 1]) for f in folds] == [9, 10, 10, 10]


def test_the_boundary_date_starts_the_next_fold() -> None:
    """§5.2's selection rule: strictly-exceeds, and that date is the next fold's first.

    Four dates of one bar each into two folds. The cumulative strictly exceeds
    half (2) at index 2, so index 2 opens fold 1 and index 1 closes fold 0.
    """
    folds = bar_weighted_folds([1, 1, 1, 1], fold_count=2)
    assert [(f.first_index, f.last_index) for f in folds] == [(0, 1), (2, 3)]


def test_a_single_dense_date_still_yields_non_empty_folds() -> None:
    """⚠ The clamp. Every bar on date 0 would otherwise repeat the edge."""
    folds = bar_weighted_folds([100, 0, 0, 0], fold_count=4)
    assert [(f.first_index, f.last_index) for f in folds] == [(0, 0), (1, 1), (2, 2), (3, 3)]
    assert all(f.date_count >= 1 for f in folds)


def test_folds_partition_the_axis_with_no_gap_and_no_overlap() -> None:
    folds = bar_weighted_folds([3, 1, 4, 1, 5, 9, 2, 6, 5, 3], fold_count=4)
    assert folds[0].first_index == 0
    assert folds[-1].last_index == 9
    for earlier, later in pairwise(folds):
        assert later.first_index == earlier.last_index + 1


@pytest.mark.parametrize(
    ("counts", "fold_count", "fragment"),
    [
        ([1, 1, 1], 1, "at least 2"),
        ([1, 1], 3, "cannot carry"),
        ([0, 0, 0, 0], 2, "sums to zero"),
        ([1, -1, 1, 1], 2, "negative"),
    ],
)
def test_refused_axes(counts: list[int], fold_count: int, fragment: str) -> None:
    with pytest.raises(ValueError, match=fragment):
        bar_weighted_folds(counts, fold_count=fold_count)


# ---------------------------------------------------------------------------
# role — §5.3's four verdicts on one hand-drawn axis
# ---------------------------------------------------------------------------

#: Indices 10..19 inclusive are the test fold.
_FOLD = Fold(index=1, first_index=10, last_index=19)


@pytest.mark.parametrize(
    ("start", "end", "expected", "why"),
    [
        (10, 12, "test", "starts on the fold's first date"),
        (19, 25, "test", "starts on the fold's last date; its label runs past the end, which is the test set's own"),
        (15, 15, "test", "opens and closes inside"),
        (5, 9, "train", "closes the day before the fold opens"),
        (5, 10, "purged", "closes on the fold's FIRST date — the label was resolved by a fold price"),
        (5, 25, "purged", "spans the fold entirely; an endpoint test would call this training data"),
        (9, 19, "purged", "closes on the fold's last date"),
        (20, 22, "embargoed", "starts the day after the fold"),
        (
            20,
            30,
            "embargoed",
            "⚠ starts inside the embargo but closes past it — the window is keyed on the START, because it is the "
            "entry that inherits the fold's information, not the close",
        ),
        (23, 23, "embargoed", "starts on the last embargoed date — the window is CLOSED on the right"),
        (24, 30, "train", "starts one date past the embargo"),
    ],
)
def test_role_verdicts(start: int, end: int, expected: str, why: str) -> None:
    assert role(start, end, fold=_FOLD, embargo_bars=4) == expected, why


def test_a_zero_embargo_leaves_the_date_after_the_fold_in_training() -> None:
    assert role(20, 21, fold=_FOLD, embargo_bars=0) == "train"


@pytest.mark.parametrize(
    ("start", "end", "embargo", "fragment"),
    [
        (5, 4, 3, "before its start"),
        (5, 6, -1, "non-negative"),
    ],
)
def test_role_refuses_impossible_inputs(start: int, end: int, embargo: int, fragment: str) -> None:
    with pytest.raises(ValueError, match=fragment):
        role(start, end, fold=_FOLD, embargo_bars=embargo)


# ---------------------------------------------------------------------------
# training_embargo_bars
# ---------------------------------------------------------------------------


def test_the_embargo_is_the_widest_span_wholly_outside_the_fold() -> None:
    """⚠ The 30-bar span is PURGED, so it must not set the embargo.

    Observations: a 3-bar training hold, a 30-bar hold that spans the fold
    (purged), a 7-bar training hold after the embargo, and one inside the fold
    (test).
    """
    starts = [0, 5, 30, 15]
    ends = [3, 35, 37, 16]
    assert training_embargo_bars(starts, ends, fold=_FOLD) == 7


def test_an_empty_training_side_measures_zero() -> None:
    assert training_embargo_bars([12], [13], fold=_FOLD) == 0


def test_the_span_is_a_displacement_not_a_bar_count() -> None:
    """A hold that opens and closes on the same date displaces zero bars."""
    assert training_embargo_bars([0, 1], [0, 1], fold=_FOLD) == 0


def test_mismatched_index_lengths_are_refused() -> None:
    with pytest.raises(ValueError, match="against"):
        training_embargo_bars([1, 2], [3], fold=_FOLD)


# ---------------------------------------------------------------------------
# census
# ---------------------------------------------------------------------------


def test_census_counts_every_observation_exactly_once() -> None:
    starts = [10, 5, 5, 20, 24, 0]
    ends = [12, 9, 25, 22, 30, 1]
    counted = census(starts, ends, fold=_FOLD, embargo_bars=4)
    assert counted == FoldCensus(test=1, train=3, purged=1, embargoed=1)
    assert counted.total == len(starts)


def test_census_of_an_empty_population_is_all_zeroes() -> None:
    assert census([], [], fold=_FOLD, embargo_bars=4) == FoldCensus(test=0, train=0, purged=0, embargoed=0)


# ---------------------------------------------------------------------------
# Fold / FoldCensus construction guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"index": -1, "first_index": 0, "last_index": 1}, "non-negative"),
        ({"index": 0, "first_index": -1, "last_index": 1}, "before the axis"),
        ({"index": 0, "first_index": 5, "last_index": 4}, "before its start"),
    ],
)
def test_fold_refuses_impossible_bounds(kwargs: dict[str, int], fragment: str) -> None:
    with pytest.raises(ValueError, match=fragment):
        Fold(**kwargs)


def test_fold_census_refuses_a_negative_count() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        FoldCensus(test=1, train=-1, purged=0, embargoed=0)


# ---------------------------------------------------------------------------
# The composed property the full-population arm asserts
# ---------------------------------------------------------------------------


def test_folds_partition_the_observations_into_test_exactly_once() -> None:
    """F1's second half, on a fixture: each observation is one fold's test row."""
    counts = [1] * 40
    folds = bar_weighted_folds(counts, fold_count=4)
    starts = [0, 3, 11, 12, 19, 22, 30, 39]
    ends = [5, 4, 14, 35, 20, 24, 31, 39]
    test_total = 0
    for fold in folds:
        embargo = training_embargo_bars(starts, ends, fold=fold)
        counted = census(starts, ends, fold=fold, embargo_bars=embargo)
        assert counted.total == len(starts)
        test_total += counted.test
    assert test_total == len(starts)


def test_no_training_observation_survives_the_purge_or_the_embargo() -> None:
    """F2 and F3 on a fixture, with the predicates restated from §5.3."""
    counts = [1] * 40
    starts = [0, 3, 11, 12, 19, 22, 30, 39]
    ends = [5, 4, 14, 35, 20, 24, 31, 39]
    for fold in bar_weighted_folds(counts, fold_count=4):
        embargo = training_embargo_bars(starts, ends, fold=fold)
        for start, end in zip(starts, ends, strict=True):
            if role(start, end, fold=fold, embargo_bars=embargo) != "train":
                continue
            assert not (start <= fold.last_index and end >= fold.first_index), "purge incomplete"
            assert not (fold.last_index < start <= fold.last_index + embargo), "embargo incomplete"


# ---------------------------------------------------------------------------
# WalkForwardFolds — the stored split's shape (stage 5e-5c)
# ---------------------------------------------------------------------------

#: Transcribed from `walk_forward`'s own construction, not imported.
SPEC_WALK_FORWARD_MODEL_ID = "c5-purged-walk-forward-v1"


def build_fold_record(
    index: int,
    first_index: int,
    last_index: int,
    *,
    test: int = 5,
    train: int = 20,
    purged: int = 3,
    embargoed: int = 2,
    embargo_bars: int = 7,
) -> FoldRecord:
    """One fold record whose dates track its indices, day for day.

    ⚠ The default census sums to 30 in EVERY fold, which is the state the type
    requires — each fold classifies the same observation set. A test that wants
    the totals to disagree says so by overriding one bucket.
    """
    return FoldRecord(
        fold=Fold(index=index, first_index=first_index, last_index=last_index),
        first_date=date(2000, 1, 1) + timedelta(days=first_index),
        last_date=date(2000, 1, 1) + timedelta(days=last_index),
        bar_count=(last_index - first_index + 1) * 100,
        embargo_bars=embargo_bars,
        census=FoldCensus(test=test, train=train, purged=purged, embargoed=embargoed),
    )


def build_split(**overrides: object) -> WalkForwardFolds:
    base: dict[str, object] = {
        "model_id": SPEC_WALK_FORWARD_MODEL_ID,
        "folds": (
            build_fold_record(0, 0, 9),
            build_fold_record(1, 10, 19),
            build_fold_record(2, 20, 29),
            build_fold_record(3, 30, 39),
        ),
    }
    base.update(overrides)
    return WalkForwardFolds(**base)  # type: ignore[arg-type]


def test_a_complete_split_constructs_and_reports_one_population() -> None:
    split = build_split()
    assert len(split.folds) == SPEC_FOLD_COUNT
    assert split.observation_count == 30
    assert split.model_id == SPEC_WALK_FORWARD_MODEL_ID


@pytest.mark.parametrize("fold_count", [1, 2, 3, 5])
def test_a_split_that_is_not_four_folds_is_refused(fold_count: int) -> None:
    """⚠ The stored split's count is FROZEN — see FOLD_COUNT's own comment.

    A three-fold set is a cross-validation that stopped early and would read as
    one that finished; a five-fold set is a swept validity gate.
    """
    folds = tuple(build_fold_record(k, k * 10, k * 10 + 9) for k in range(fold_count))
    with pytest.raises(ValueError, match="carries 4 folds"):
        build_split(folds=folds)


def test_a_fold_whose_index_is_not_its_position_is_refused() -> None:
    folds = (
        build_fold_record(0, 0, 9),
        build_fold_record(2, 10, 19),
        build_fold_record(2, 20, 29),
        build_fold_record(3, 30, 39),
    )
    with pytest.raises(ValueError, match="position 1 carries index 2"):
        build_split(folds=folds)


def test_a_split_that_does_not_start_at_the_axis_front_is_refused() -> None:
    """A gap at index 0 is training data no fold ever tested."""
    folds = (
        build_fold_record(0, 1, 9),
        build_fold_record(1, 10, 19),
        build_fold_record(2, 20, 29),
        build_fold_record(3, 30, 39),
    )
    with pytest.raises(ValueError, match="starts at axis index 1"):
        build_split(folds=folds)


def test_a_gap_between_two_folds_is_refused() -> None:
    folds = (
        build_fold_record(0, 0, 9),
        build_fold_record(1, 11, 19),
        build_fold_record(2, 20, 29),
        build_fold_record(3, 30, 39),
    )
    with pytest.raises(ValueError, match="does not follow"):
        build_split(folds=folds)


def test_index_contiguous_folds_that_overlap_in_TIME_are_refused() -> None:
    """⚠ Both axes are checked, because only one of them is derived.

    The indices below are perfectly contiguous; the DATES are not, and a stored
    pair that disagrees means the axis the indices refer to is not the axis the
    dates came from.
    """
    later = build_fold_record(1, 10, 19)
    folds = (
        build_fold_record(0, 0, 9),
        FoldRecord(
            fold=later.fold,
            first_date=date(2000, 1, 5),
            last_date=later.last_date,
            bar_count=later.bar_count,
            embargo_bars=later.embargo_bars,
            census=later.census,
        ),
        build_fold_record(2, 20, 29),
        build_fold_record(3, 30, 39),
    )
    with pytest.raises(ValueError, match="on or before fold 0's last date"):
        build_split(folds=folds)


def test_folds_counting_different_populations_are_refused() -> None:
    """⚠ The check that catches a split assembled from two runs."""
    folds = (
        build_fold_record(0, 0, 9),
        build_fold_record(1, 10, 19, train=21),
        build_fold_record(2, 20, 29),
        build_fold_record(3, 30, 39),
    )
    with pytest.raises(ValueError, match="count different observation populations"):
        build_split(folds=folds)


def test_a_blank_model_id_is_refused() -> None:
    with pytest.raises(ValueError, match="model_id is blank"):
        build_split(model_id="")


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"first_date": date(2000, 2, 1), "last_date": date(2000, 1, 1)}, "before its start"),
        ({"bar_count": -1}, "carries -1 bars"),
        ({"embargo_bars": -1}, "embargo_bars -1"),
    ],
)
def test_fold_record_refuses_impossible_fields(kwargs: dict[str, object], fragment: str) -> None:
    base: dict[str, object] = {
        "fold": Fold(index=0, first_index=0, last_index=9),
        "first_date": date(2000, 1, 1),
        "last_date": date(2000, 1, 10),
        "bar_count": 1000,
        "embargo_bars": 7,
        "census": FoldCensus(test=5, train=20, purged=3, embargoed=2),
    }
    base.update(kwargs)
    with pytest.raises(ValueError, match=fragment):
        FoldRecord(**base)  # type: ignore[arg-type]


def test_a_zero_embargo_is_legal_in_a_stored_split() -> None:
    """⚠ 0 means "nothing measurable on this fold's training side", not "skipped".

    `role`'s header says refusing it would force a caller to invent a number,
    and the last fold — with nothing following it — measures exactly this.
    """
    folds = (
        build_fold_record(0, 0, 9),
        build_fold_record(1, 10, 19),
        build_fold_record(2, 20, 29),
        build_fold_record(3, 30, 39, embargo_bars=0),
    )
    assert build_split(folds=folds).folds[3].embargo_bars == 0
