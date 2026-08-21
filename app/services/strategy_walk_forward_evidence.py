"""Criterion 5's stored split, shaped for the operator card (#2823).

``sql/269_strategy_result_folds.sql`` calls the consumer it was written for
*"the phase-6 read"*. It was never built: ``store_walk_forward_folds`` has
written folds since #2240 and the only callers of ``read_walk_forward_folds``
are tests and one-off scripts, so the evidence that criterion 5's split was
actually purged and embargoed has never reached ``/strategies``. #2817 closed
the same gap one table over, on the per-regime cohorts.

WHAT THIS MODULE IS FOR, AND WHY IT IS PURE
-------------------------------------------
Choosing WHICH stored split represents a strategy, and deciding what to say when
there is none, are the two decisions on this path that can be wrong. Both are
functions of rows alone, so they are table-testable with no database — the
``strategy_monitoring.derive_fire_rate`` shape, for the same reason.

⚠⚠ THE SPLIT IS PER STRATEGY, NOT PER ARM — AND THAT IS MEASURED, NOT ASSUMED.
A strategy version stores FOUR in-sample results, the 2x2 of ``ambiguity_arm``
(``best_case`` / ``worst_case``) x ``quarantine_arm`` (``masked`` / ``admitted``),
and each carries its own four fold rows. Measured on dev 2026-08-21 for
``s1-time-series-momentum`` at ``strategy-registry-v1+2307ee566d7b`` (result ids
117-120), fold 0 of each:

    arm                       first_date  last_date   bar_count  embargo_bars   train
    best_case  / masked       1962-01-02  1999-09-02  4,375,006           615   1,694,103
    best_case  / admitted     1962-01-02  1999-09-02  4,375,006           615   1,694,643
    worst_case / masked       1962-01-02  1999-09-02  4,375,006           615   1,694,103
    worst_case / admitted     1962-01-02  1999-09-02  4,375,006           615   1,694,643

So the GEOMETRY — dates, bars, embargo — is identical across all four, which is
``bar_weighted_folds`` reading only the panel AXIS (``backtest_run.py``: *"reads
only the axis, so every result…"*). The CENSUS moves with ``quarantine_arm``
alone, because masking changes which instruments are in the universe and
therefore which observations exist. ``ambiguity_arm`` is a fill-price
assumption and cannot move a population, which the table confirms: 117 == 119
and 118 == 120, exactly.

Rendering four near-identical splits would therefore be four copies of one
measurement plus a 0.03% census difference. One split is rendered, and the arm
whose census it is, is NAMED in the payload rather than left implicit.

WHAT A FOLD IS — the four words below are all easy to get backwards, and the
card's copy depends on every one of them (``app/services/walk_forward.py``):

- The design is **purged K-fold over contiguous blocks**, NOT an anchored or
  rolling walk-forward. Both sides of a test block carry training data; an
  anchored design has none after it, which would delete the embargo — *"half of
  what criterion 5 asks for"*.
- ``first_date`` / ``last_date`` bound the **test** block. They are not a
  training interval.
- ``purged`` counts training observations whose LABEL WINDOW overlaps that test
  block. ``embargoed`` counts training observations that START in the window
  immediately FOLLOWING it. They are separate verdicts and not one "dropped"
  bucket, because §5.3's finding is about their relative size.
- ``train`` is what survives both, on BOTH sides of the block.

⚠ THE FOUR COUNTS ARE ONE POPULATION RE-CLASSIFIED PER FOLD.
``FoldCensus.total`` is ``test + train + purged + embargoed`` and asserts that
every observation lands in exactly one bucket — per fold. So a cross-fold sum of
any of them counts the same population ``fold_count`` times over. No total is
derived here and none may be rendered.

⚠ NO PER-FOLD PERFORMANCE EXISTS, BY DESIGN. ``FoldRecord``: *"these strategies
fit no parameters, so the split is a validity GATE and not a training loop. A
per-fold Sharpe would invite exactly the 'which fold did best' search criterion 6
exists to bound."* Nothing here may be described as a return or a score.

⚠ Two zeroes that are measurements rather than gaps, both stated by ``sql/269``:
a fold spanning real dates may carry ``test_count == 0`` (no observation STARTS
inside a thin era), and ``embargo_bars == 0`` means *"nothing to measure on this
fold's training side"*, never "no embargo applied".
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, Literal

__all__ = [
    "QUARANTINE_ARM_PREFERENCE",
    "SplitUnavailableReason",
    "StrategyWalkForwardSplit",
    "WalkForwardFoldView",
    "derive_walk_forward_split",
]

#: Why a card carries no split. The ``ShareUnavailableReason`` contract in
#: ``strategy_monitoring`` one field over, for the same reason it exists there: a
#: silent absence and a measured zero are indistinguishable to a reader, so the
#: absence is NAMED.
#:
#: The three are disjoint by CONSTRUCTION rather than by precedence — they are
#: decided in one pass over a strategy's rows, and each test is reached only when
#: the one before it did not apply:
#:
#: 1. ``no_in_sample_result`` — no in-sample result under the current identity
#:    pins. The usual state for a version whose backtest has not been re-run;
#:    it is NOT "the split was skipped".
#: 2. ``no_split_stored`` — an in-sample result exists and carries no fold rows.
#:    Distinct from 1 on purpose: the run happened and the split did not reach
#:    storage, which is a different thing to chase.
#: 3. ``invariant_violated`` — the stored rows contradict the writer's own
#:    contract. ⚠ The only one that describes US rather than the evidence, and
#:    the reason it is a named state rather than a raise is
#:    ``derive_fire_rate``'s: this runs per strategy inside an aggregate
#:    endpoint, so propagating would blank all ten cards over one bad row. The
#:    corruption is contained to the card it belongs to and named there, because
#:    a corruption visible only in logs is one nobody sees.
SplitUnavailableReason = Literal[
    "no_in_sample_result",
    "no_split_stored",
    "invariant_violated",
]

#: Which arm's census is rendered when more than one is stored. ``admitted`` is
#: the unmasked universe and so the wider population; ``masked`` is the fallback
#: rather than a second panel. ⚠ The CHOICE is not the point — naming it in the
#: payload is. A census whose arm is implicit is a number the operator cannot
#: attribute.
QUARANTINE_ARM_PREFERENCE: Final[tuple[str, ...]] = ("admitted", "masked")


@dataclass(frozen=True)
class WalkForwardFoldView:
    """One stored fold row, as the card reads it.

    ⚠ ``first_date`` / ``last_date`` are the TEST block. See the module header.
    """

    fold_index: int
    first_date: date
    last_date: date
    bar_count: int
    embargo_bars: int
    test_count: int
    train_count: int
    purged_count: int
    embargoed_count: int


@dataclass(frozen=True)
class StrategyWalkForwardSplit:
    """One strategy's split, or the named reason it has none.

    ``folds`` is non-empty exactly when ``unavailable_reason`` is ``None`` — the
    ``derive_fire_rate`` rule that *"a value is None if and only if its reason is
    not"*, so no caller can render a half-populated panel.
    """

    folds: tuple[WalkForwardFoldView, ...] = ()
    #: The STORED construction id, never the current constant: a split cut under
    #: a superseded model must stay readable as that model rather than inherit
    #: today's label (``sql/269``'s own argument for storing it per row).
    walk_forward_model_id: str | None = None
    fold_count: int | None = None
    #: Whose census the counts are. See ``QUARANTINE_ARM_PREFERENCE``.
    quarantine_arm: str | None = None
    #: The in-sample window the folds partition. ⚠ NOT one of the six recent
    #: evidence windows — the folds sit entirely inside the in-sample prefix and
    #: are not the in-sample/hold-out split, so this window is the split's own.
    window_start: date | None = None
    window_end: date | None = None
    unavailable_reason: SplitUnavailableReason | None = None


def _sort_key(row: Mapping[str, Any]) -> tuple[int, int, int]:
    """Preference order over one strategy's candidate in-sample results.

    Least-first, so the caller reads it with ``min``: the preferred quarantine
    arm, then the newest window, then the newest result.

    ⚠ The last two are NOT preferences anyone should have to defend — they exist
    so the choice is DETERMINISTIC. A tie broken by whatever order the database
    happened to return is an accidental API semantic, and it would move under a
    plan change nobody reviewed.

    ⚠ Both "newest" terms are NEGATED ordinals, not the values. A bare
    ``window_end`` sorts ascending under ``min`` and hands back the OLDEST split
    — which is the opposite of the docstring, and is exactly what
    ``test_the_newest_window_breaks_a_tie_deterministically`` caught.
    """
    arm = str(row["quarantine_arm"])
    arm_rank = (
        QUARANTINE_ARM_PREFERENCE.index(arm) if arm in QUARANTINE_ARM_PREFERENCE else len(QUARANTINE_ARM_PREFERENCE)
    )
    window_end: date = row["window_end"]
    return (arm_rank, -window_end.toordinal(), -int(row["result_id"]))


def derive_walk_forward_split(fold_rows: Sequence[Mapping[str, Any]]) -> StrategyWalkForwardSplit:
    """Pick one strategy's representative split from its stored fold rows.

    ``fold_rows`` is every ``strategy_result_folds`` row for ONE strategy's
    current-identity in-sample results, LEFT-joined so a result with no split
    still appears with a null ``fold_index``. That join shape is what makes
    ``no_in_sample_result`` and ``no_split_stored`` distinguishable here rather
    than collapsed into one "no evidence" by the query.
    """
    if not fold_rows:
        return StrategyWalkForwardSplit(unavailable_reason="no_in_sample_result")

    with_folds = [row for row in fold_rows if row.get("fold_index") is not None]
    if not with_folds:
        return StrategyWalkForwardSplit(unavailable_reason="no_split_stored")

    by_result: dict[int, list[Mapping[str, Any]]] = {}
    for row in with_folds:
        by_result.setdefault(int(row["result_id"]), []).append(row)
    chosen = min((rows for rows in by_result.values()), key=lambda rows: _sort_key(rows[0]))

    # One split is one construction. `read_walk_forward_folds` raises on a mixed
    # model id for this reason; a bulk reader cannot raise, so it names it.
    model_ids = {str(row["walk_forward_model_id"]) for row in chosen}
    fold_counts = {int(row["fold_count"]) for row in chosen}
    if len(model_ids) > 1 or len(fold_counts) > 1:
        return StrategyWalkForwardSplit(unavailable_reason="invariant_violated")

    declared = fold_counts.pop()
    indices = sorted(int(row["fold_index"]) for row in chosen)
    # A split missing a fold is a cross-validation nobody ran to the end, and it
    # would render as a complete one. `WalkForwardFolds` makes the same
    # all-or-nothing check on the writer side; this is the read side of it.
    if len(indices) != declared or indices != list(range(declared)):
        return StrategyWalkForwardSplit(unavailable_reason="invariant_violated")

    head = chosen[0]
    return StrategyWalkForwardSplit(
        folds=tuple(
            WalkForwardFoldView(
                fold_index=int(row["fold_index"]),
                first_date=row["first_date"],
                last_date=row["last_date"],
                bar_count=int(row["bar_count"]),
                embargo_bars=int(row["embargo_bars"]),
                test_count=int(row["test_count"]),
                train_count=int(row["train_count"]),
                purged_count=int(row["purged_count"]),
                embargoed_count=int(row["embargoed_count"]),
            )
            for row in sorted(chosen, key=lambda row: int(row["fold_index"]))
        ),
        walk_forward_model_id=model_ids.pop(),
        fold_count=declared,
        quarantine_arm=str(head["quarantine_arm"]),
        window_start=head["window_start"],
        window_end=head["window_end"],
    )
