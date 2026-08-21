"""#2823 — picking a strategy's representative split, and naming its absence.

Pure: every case is a list of row dicts, so none of this needs Postgres. The
decisions under test are the two that can be wrong on this path — WHICH stored
result represents the card, and WHAT is said when none does.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from app.services.strategy_walk_forward_evidence import derive_walk_forward_split

_WINDOW_START = date(1962, 1, 2)
_WINDOW_END = date(2026, 7, 8)


def _fold_row(
    *,
    result_id: int,
    fold_index: int | None,
    quarantine_arm: str = "admitted",
    fold_count: int = 2,
    model_id: str = "c5-purged-walk-forward-v1",
    window_end: date = _WINDOW_END,
    train_count: int = 1_000,
) -> dict[str, Any]:
    """One `_WALK_FORWARD_SPLIT_SQL` row. `fold_index=None` is the LEFT-JOIN miss."""
    row: dict[str, Any] = {
        "strategy_id": "s1-time-series-momentum",
        "result_id": result_id,
        "quarantine_arm": quarantine_arm,
        "window_start": _WINDOW_START,
        "window_end": window_end,
        "fold_index": fold_index,
    }
    if fold_index is None:
        return row | dict.fromkeys(
            (
                "walk_forward_model_id",
                "fold_count",
                "first_date",
                "last_date",
                "bar_count",
                "embargo_bars",
                "test_count",
                "train_count",
                "purged_count",
                "embargoed_count",
            )
        )
    return row | {
        "walk_forward_model_id": model_id,
        "fold_count": fold_count,
        "first_date": date(1990 + fold_index, 1, 1),
        "last_date": date(1990 + fold_index, 12, 31),
        "bar_count": 4_375_006,
        "embargo_bars": 615,
        "test_count": 639_464,
        "train_count": train_count,
        "purged_count": 0,
        "embargoed_count": 122_530,
    }


def _split(*rows: dict[str, Any]) -> Any:
    return derive_walk_forward_split(list(rows))


class TestNamedAbsence:
    def test_no_rows_at_all_is_no_in_sample_result(self) -> None:
        """The usual state for a version whose backtest has not been re-run."""
        split = _split()
        assert split.unavailable_reason == "no_in_sample_result"
        assert split.folds == ()

    def test_a_result_with_no_folds_is_a_different_state(self) -> None:
        """The run happened and the split did not reach storage.

        Collapsing this into `no_in_sample_result` would hide a real gap behind
        the benign one — which is why the SQL LEFT JOINs rather than INNER JOINs.
        """
        split = _split(_fold_row(result_id=1, fold_index=None))
        assert split.unavailable_reason == "no_split_stored"
        assert split.folds == ()

    def test_every_absence_leaves_every_field_null(self) -> None:
        """`derive_fire_rate`'s rule: a value is None iff its reason is not."""
        for split in (_split(), _split(_fold_row(result_id=1, fold_index=None))):
            assert split.folds == ()
            assert split.walk_forward_model_id is None
            assert split.fold_count is None
            assert split.quarantine_arm is None
            assert split.window_start is None
            assert split.window_end is None


class TestInvariantViolations:
    """Contained to the card and NAMED — never raised.

    `read_walk_forward_folds` raises on a mixed split because it reads one
    result. This runs per strategy inside an aggregate endpoint, so a raise
    would blank all ten cards over one bad row.
    """

    def test_two_constructions_on_one_result(self) -> None:
        split = _split(
            _fold_row(result_id=1, fold_index=0, model_id="c5-purged-walk-forward-v1"),
            _fold_row(result_id=1, fold_index=1, model_id="c5-purged-walk-forward-v2"),
        )
        assert split.unavailable_reason == "invariant_violated"
        assert split.folds == ()

    def test_disagreeing_fold_counts(self) -> None:
        split = _split(
            _fold_row(result_id=1, fold_index=0, fold_count=2),
            _fold_row(result_id=1, fold_index=1, fold_count=3),
        )
        assert split.unavailable_reason == "invariant_violated"

    def test_a_missing_fold_is_a_cross_validation_nobody_finished(self) -> None:
        """Two declared, one stored — it would otherwise render as complete."""
        split = _split(_fold_row(result_id=1, fold_index=0, fold_count=2))
        assert split.unavailable_reason == "invariant_violated"

    def test_a_gap_in_the_indices(self) -> None:
        """Right COUNT, wrong SET: 0 and 2 of a 2-fold split is still incomplete."""
        split = _split(
            _fold_row(result_id=1, fold_index=0, fold_count=2),
            _fold_row(result_id=1, fold_index=2, fold_count=2),
        )
        assert split.unavailable_reason == "invariant_violated"


class TestArmSelection:
    def test_admitted_wins_over_masked(self) -> None:
        """The unmasked universe is the wider population, and it is NAMED."""
        split = _split(
            _fold_row(result_id=1, fold_index=0, quarantine_arm="masked", train_count=1_694_103),
            _fold_row(result_id=1, fold_index=1, quarantine_arm="masked", train_count=1_694_103),
            _fold_row(result_id=2, fold_index=0, quarantine_arm="admitted", train_count=1_694_643),
            _fold_row(result_id=2, fold_index=1, quarantine_arm="admitted", train_count=1_694_643),
        )
        assert split.unavailable_reason is None
        assert split.quarantine_arm == "admitted"
        assert [fold.train_count for fold in split.folds] == [1_694_643, 1_694_643]

    def test_masked_alone_is_used_rather_than_refused(self) -> None:
        split = _split(
            _fold_row(result_id=1, fold_index=0, quarantine_arm="masked"),
            _fold_row(result_id=1, fold_index=1, quarantine_arm="masked"),
        )
        assert split.unavailable_reason is None
        assert split.quarantine_arm == "masked"

    def test_a_foldless_result_never_masks_a_stored_split(self) -> None:
        """Ordering is folds-first, so the preferred arm cannot win while empty."""
        split = _split(
            _fold_row(result_id=9, fold_index=None, quarantine_arm="admitted"),
            _fold_row(result_id=1, fold_index=0, quarantine_arm="masked"),
            _fold_row(result_id=1, fold_index=1, quarantine_arm="masked"),
        )
        assert split.unavailable_reason is None
        assert split.quarantine_arm == "masked"
        assert len(split.folds) == 2

    def test_the_newest_window_breaks_a_tie_deterministically(self) -> None:
        """A tie settled by database return order is an accidental API semantic."""
        older = date(2024, 9, 27)
        split = _split(
            _fold_row(result_id=1, fold_index=0, window_end=older),
            _fold_row(result_id=1, fold_index=1, window_end=older),
            _fold_row(result_id=2, fold_index=0, window_end=_WINDOW_END),
            _fold_row(result_id=2, fold_index=1, window_end=_WINDOW_END),
        )
        assert split.window_end == _WINDOW_END


class TestRenderedSplit:
    def test_folds_come_back_in_index_order_whatever_the_row_order(self) -> None:
        split = _split(
            _fold_row(result_id=1, fold_index=1),
            _fold_row(result_id=1, fold_index=0),
        )
        assert [fold.fold_index for fold in split.folds] == [0, 1]

    def test_the_stored_construction_is_reported_not_todays_constant(self) -> None:
        """A split cut under a superseded model stays readable as that model."""
        split = _split(
            _fold_row(result_id=1, fold_index=0, model_id="c5-purged-walk-forward-v0"),
            _fold_row(result_id=1, fold_index=1, model_id="c5-purged-walk-forward-v0"),
        )
        assert split.walk_forward_model_id == "c5-purged-walk-forward-v0"

    def test_the_response_model_carries_exactly_the_derivation_s_fields(self) -> None:
        """Drift between the dataclass and the response model fails HERE.

        `_walk_forward_split_view` copies field by field, so a rename on either
        side is a type error rather than a silent drop — but only for fields that
        still exist on both. This pins the SETS equal, which is what catches a
        field ADDED to the derivation and never surfaced: the endpoint would keep
        returning a valid, quietly incomplete payload, and no request would fail.
        """
        from dataclasses import fields

        from app.api.strategies import WalkForwardFold, WalkForwardSplit
        from app.services.strategy_walk_forward_evidence import (
            StrategyWalkForwardSplit,
            WalkForwardFoldView,
        )

        assert {field.name for field in fields(StrategyWalkForwardSplit)} == set(WalkForwardSplit.model_fields)
        assert {field.name for field in fields(WalkForwardFoldView)} == set(WalkForwardFold.model_fields)

    def test_a_zero_test_count_is_a_measurement_not_a_gap(self) -> None:
        """`sql/269`: a fold spanning a thin era can carry no STARTING observation."""
        thin = _fold_row(result_id=1, fold_index=0) | {"test_count": 0, "embargo_bars": 0}
        split = _split(thin, _fold_row(result_id=1, fold_index=1))
        assert split.unavailable_reason is None
        assert split.folds[0].test_count == 0
        assert split.folds[0].embargo_bars == 0
