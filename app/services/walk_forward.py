"""Phase 5e-4 — criterion 5's purged walk-forward, and the embargo around each fold.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.3 and §8 (stage
5e-4), acceptance C5. Parent
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` criterion 5 and
§2.2 — *"a single hold-out is thin; prefer purged walk-forward with an embargo
around each fold boundary"*. Refs #2240.

⚠⚠ THIS IS NOT THE HOLD-OUT SPLIT. §5.2's frozen ``HOLDOUT_BOUNDARY`` withholds
the final 25% of bars and nothing here touches it. Every fold below is cut
INSIDE the in-sample side; the hold-out is not an input to any function in this
module and never becomes one.

SOURCE RULE
-----------
**López de Prado, *Advances in Financial Machine Learning* (2018) ch. 7** —
purging and embargoing — which criterion 5 names by mechanism. Two operations,
both acting on the TRAINING side, and an earlier draft of §5.3 had them
backwards:

- **Purge** — drop training observations whose LABEL WINDOW overlaps the test
  fold. A trade opened before the fold and closed inside it was resolved by
  prices the test fold owns.
- **Embargo** — drop training observations that START in the window immediately
  FOLLOWING the test fold. Serial correlation means a sample drawn just after
  the test window still carries information from it.

⚠ Both sides of the test fold carry training data, so the design is ch. 7's
**purged K-fold** over contiguous time blocks, not a strictly anchored
walk-forward. An anchored design has no training data after the test fold at
all, which would make the embargo — half of what criterion 5 asks for —
unreachable. §2.2's own wording is *"around each fold boundary"*, both sides.

⚠⚠ WHAT AFML FIXES AND WHAT IT DOES NOT.
Ch. 7 fixes the two MECHANISMS. It does not fix the fold count, and its
proportional embargo rule of thumb **cannot be verified from this environment** —
two independent secondary treatments of the chapter were fetched during the
5e-4 scoping pass and neither carries a numeric rule; the commonly-repeated
``pctEmbargo = 0.01`` appears in discussion of the chapter's exercises, not as a
quotation of the rule. So it is NOT cited here and NOT used. Citing it would be
the invented-formulation defect ``.claude/CLAUDE.md`` already has a precedent
for (#2279: an invented BandWidth percentile where Bollinger publishes a
six-month rule). Both free choices below are declared as OURS, fixed by
construction, and frozen in ``WALK_FORWARD_MODEL_ID``.

⚠⚠ THE EMBARGO IS MEASURED ON THE PANEL AXIS, AND §5.3'S OWN CONSTRUCTION IS
WRONG ON THIS POINT.

§5.3 proposes *"the embargo is ``max_hold_bars`` wherever one is declared —
S-3: 10 and S-4: 40"*. Those constants are in an instrument's OWN bars: the
resolver counts forward through the series it was handed. Folds are cut on the
PANEL axis, the union of every instrument's trading dates, and an instrument's
dates are a SUBSET of the panel's. So a hold of 10 instrument bars spans **at
least** 10 panel dates and more whenever the name was halted, delisted-and-
relisted, or simply thinner than the panel. An embargo of 10 panel bars
therefore under-covers by construction, in the direction that leaks.

This is the same defect class 5e-3 recorded — two individually correct numbers
joined on an axis neither of them names — and it is why the embargo here is not
read off a declared constant at all. It is MEASURED, per fold, in panel bars:

    embargo(fold) = max panel-axis hold span over that fold's post-purge,
                    pre-embargo training observations

which is leak-free by construction (every observation it looks at lies wholly
outside the test fold), needs no declared constant for any strategy including
S-1, and subsumes ``max_hold_bars`` rather than contradicting it — a strategy
that declares one must measure a span at least that large, which
``scripts/verify_2240_walk_forward.py`` asserts rather than assumes.

⚠ §5.3 REJECTED TWO CANDIDATES AND THIS IS THE SURVIVING ONE, NOT A THIRD.
*Measured p99* was rejected on correctness — it leaves 1% leaking by
construction AND its measurement spans the test folds. *In-sample p100* was
conceded to be leak-free (*"computed on the training side only"*) and objected
to only on magnitude: *"unbounded above and a single long hold makes the embargo
swallow the fold"*. That objection is a claim about a number, and the number is
measured by ``verify_2240_walk_forward.py --holds`` — reported as ``h /
N_train`` with both its inputs, never as a bare share.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final, Literal, get_args

#: The identity of this construction. Same role as ``BOOTSTRAP_MODEL_ID`` and
#: ``COST_MODEL_ID``: the free choices — the fold count, the bar weighting, the
#: per-fold measured embargo, and the decision to measure it on the panel axis —
#: are frozen behind it, so a split cannot silently change meaning between runs.
WALK_FORWARD_MODEL_ID: Final = "c5-purged-walk-forward-v1"

#: ⚠ OURS, AND FIXED BY CONSTRUCTION RATHER THAN PICKED. AFML ch. 7 takes the
#: fold count as an argument and fixes no value, so a round number here would be
#: invented. The construction reuses a rule this phase already has: criterion 5
#: withholds *"the final 25% of history"*, so **a test fold is the same share of
#: the sample as the hold-out is of the corpus**, which gives four folds. Nothing
#: else in the phase has to learn a second proportion.
#:
#: ⚠ It is a MODULE CONSTANT and not a caller argument, deliberately: a fold
#: count that can be passed in is a fold count that can be swept, and a swept
#: validity gate is a search over validity gates. Changing it moves
#: ``WALK_FORWARD_MODEL_ID`` and is a new evaluation, which is the same rule
#: ``s1_time_series_momentum`` applies to its lookbacks.
FOLD_COUNT: Final = 4

#: What a fold did with one observation. ⚠ ``purged`` and ``embargoed`` are
#: SEPARATE verdicts and not one "dropped" bucket: they are different leaks with
#: different sizes, and collapsing them would make the §5.3 finding — that the
#: embargo removes far less than the purge — unreportable.
Role = Literal["test", "train", "purged", "embargoed"]
ROLES: frozenset[str] = frozenset(get_args(Role))


@dataclass(frozen=True)
class Fold:
    """One contiguous block of the panel axis, held out for testing.

    Indices are positions on the IN-SAMPLE panel axis and both ends are
    INCLUSIVE — the same convention as ``position_builder.Window``, so a reader
    does not have to hold two.
    """

    index: int
    first_index: int
    last_index: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"fold index must be non-negative, got {self.index}")
        if self.first_index < 0:
            raise ValueError(f"fold {self.index} starts at {self.first_index}, before the axis")
        if self.last_index < self.first_index:
            raise ValueError(f"fold {self.index} ends at {self.last_index}, before its start {self.first_index}")

    @property
    def date_count(self) -> int:
        return self.last_index - self.first_index + 1


@dataclass(frozen=True)
class FoldCensus:
    """What one fold did to the whole observation set, counted.

    ⚠ EVERY OBSERVATION LANDS IN EXACTLY ONE BUCKET, and ``total`` asserts it.
    A conservation check is the only thing that catches a role function whose
    branches overlap — the counts alone look plausible under any partition.
    """

    test: int
    train: int
    purged: int
    embargoed: int

    def __post_init__(self) -> None:
        for name in ("test", "train", "purged", "embargoed"):
            value = getattr(self, name)
            if value < 0:
                raise ValueError(f"{name} count must be non-negative, got {value}")

    @property
    def total(self) -> int:
        return self.test + self.train + self.purged + self.embargoed


@dataclass(frozen=True)
class FoldRecord:
    """One fold, as it is STORED (stage 5e-5c): geometry, embargo and census.

    ⚠ THE DATES ARE CARRIED BESIDE THE INDICES ON PURPOSE. An index is a
    position on the in-sample panel axis, and that axis is a property of the
    corpus at the moment the split ran — so a stored index alone is unreadable
    without re-deriving the axis it indexes. §5.3's own table is written in
    dates. Both are stored; the writer refuses a pair that disagree in order.

    ⚠ ``bar_count`` is what makes §5.2's realised share re-derivable from the
    stored rows. ``bar_weighted_folds``' clamp is deliberately silent in the
    library and loud in the caller, and a stored bar count is how it stays loud
    after the run that produced it has gone.

    ⚠ NO PER-FOLD METRIC. §5.3: these strategies fit no parameters, so the split
    is a validity GATE and not a training loop. A per-fold Sharpe would invite
    exactly the "which fold did best" search criterion 6 exists to bound, and no
    rule anywhere says what a per-fold number would be compared against.
    """

    fold: Fold
    first_date: date
    last_date: date
    bar_count: int
    embargo_bars: int
    census: FoldCensus

    def __post_init__(self) -> None:
        if self.last_date < self.first_date:
            raise ValueError(f"fold {self.fold.index} ends {self.last_date}, before its start {self.first_date}")
        if self.bar_count < 0:
            raise ValueError(f"fold {self.fold.index} carries {self.bar_count} bars")
        if self.embargo_bars < 0:
            raise ValueError(f"fold {self.fold.index} has embargo_bars {self.embargo_bars}")


@dataclass(frozen=True)
class WalkForwardFolds:
    """A complete split, and the invariants that make it one.

    ⚠⚠ COMPLETE OR NOTHING, AND THAT IS THE POINT OF THE TYPE. A stored split
    missing a fold is a cross-validation nobody ran to the end, and it would
    read as one — the same all-or-nothing argument ``sql/265`` makes for the
    bootstrap block and ``sql/266`` for the Deflated Sharpe, one grain down. The
    checks below are here rather than in ``result_ledger`` so they hold with no
    database, which is what lets them be revert-probed at the fast tier.

    ⚠ ``len(folds) == FOLD_COUNT`` is asserted, not accepted as an argument.
    ``FOLD_COUNT``'s own comment gives the reason: *a fold count that can be
    passed in is a fold count that can be swept, and a swept validity gate is a
    search over validity gates.* ``bar_weighted_folds`` takes the count so a
    unit test can draw a two-fold axis; a STORED split may not.

    ⚠ ``model_id`` HAS NO DEFAULT. A read reconstructs the id the row was
    written under, which is not necessarily today's constant, so defaulting it
    would silently relabel an older split as this one. ``store_walk_forward_folds``
    refuses anything but ``WALK_FORWARD_MODEL_ID`` on the WRITE side, where the
    asymmetry is the correct one: a write always happens under today's
    construction.
    """

    model_id: str
    folds: tuple[FoldRecord, ...]

    def __post_init__(self) -> None:
        if not self.model_id:
            raise ValueError("model_id is blank — a split with no declared construction records nothing (#2286)")
        if len(self.folds) != FOLD_COUNT:
            raise ValueError(
                f"a stored split carries {FOLD_COUNT} folds, got {len(self.folds)} — a partial split is a "
                "cross-validation that did not finish, and it would read as one that did"
            )
        for position, record in enumerate(self.folds):
            if record.fold.index != position:
                raise ValueError(f"fold at position {position} carries index {record.fold.index}")
        if self.folds[0].fold.first_index != 0:
            raise ValueError(
                f"the split starts at axis index {self.folds[0].fold.first_index}, not 0 — folds partition the "
                "whole in-sample axis, and a gap at the front is training data no fold ever tested"
            )
        for earlier, later in zip(self.folds, self.folds[1:], strict=False):
            if later.fold.first_index != earlier.fold.last_index + 1:
                raise ValueError(
                    f"fold {later.fold.index} starts at index {later.fold.first_index}, which does not follow fold "
                    f"{earlier.fold.index}'s last index {earlier.fold.last_index}"
                )
            if later.first_date <= earlier.last_date:
                raise ValueError(
                    f"fold {later.fold.index} starts {later.first_date}, on or before fold {earlier.fold.index}'s "
                    f"last date {earlier.last_date} — contiguous index blocks cannot overlap in time"
                )
        # ⚠⚠ EVERY FOLD CLASSIFIES EVERY OBSERVATION, so the four buckets sum to
        # the SAME total in each fold. A fold whose total differs was measured
        # over a different population, and every count in the split is then a
        # count of something else — the identical argument
        # `QuarantineCensus.__post_init__` makes for its two arms. It is the one
        # check here that catches a caller assembling folds from two runs.
        totals = {record.census.total for record in self.folds}
        if len(totals) > 1:
            raise ValueError(
                f"the folds count different observation populations {sorted(totals)} — every fold classifies every "
                "observation, so a split whose totals differ was assembled from more than one run"
            )

    @property
    def observation_count(self) -> int:
        """The population every fold classified. See the total check above."""
        return self.folds[0].census.total


def bar_weighted_folds(bar_counts: Sequence[int], *, fold_count: int = FOLD_COUNT) -> tuple[Fold, ...]:
    """Cut the in-sample axis into ``fold_count`` contiguous blocks of equal BARS.

    ``bar_counts[i]`` is how many bars the panel carries on axis date ``i``.

    ⚠⚠ WEIGHTED BY BAR, NOT BY DATE, AND FOR §5.2'S REASON. This panel is
    unbalanced — *"30 series in 1970 against 5,245 in 2026"* — so equal-length
    date blocks would put three of four folds in a thin era carrying almost no
    observations, and the fourth would be the entire modern corpus. §5.2 settled
    exactly this ambiguity for the hold-out split and the answer does not change
    one level down.

    ⚠ THE SELECTION RULE IS §5.2'S, VERBATIM: the boundary is *"the first
    trading date whose CUMULATIVE bar count strictly exceeds"* the target share,
    and that date is the FIRST date of the next fold. Re-deriving it with ``>=``
    or with the boundary on the other side would move every fold edge by a day
    against a rule this repo has already fixed once.

    ⚠ Folds are NOT equal in bars, only as equal as an integer cut of a lumpy
    axis allows — a single date can carry 4,021 bars (§5.2's own measurement).
    The realised share is reported by the verify script rather than asserted
    here, because it is a property of the corpus.
    """
    if fold_count < 2:
        raise ValueError(f"fold_count must be at least 2, got {fold_count} — one fold is not a cross-validation")
    if len(bar_counts) < fold_count:
        raise ValueError(
            f"axis of {len(bar_counts)} dates cannot carry {fold_count} folds — every fold needs at least one date"
        )
    if any(count < 0 for count in bar_counts):
        raise ValueError("bar_counts carries a negative count")
    total = sum(bar_counts)
    if total <= 0:
        raise ValueError("bar_counts sums to zero — an axis with no bars has no bar-weighted cut")

    # ⚠ Integer arithmetic throughout. `cumulative * fold_count > total * k` is
    # `cumulative / total > k / fold_count` without a float, so a boundary
    # cannot move because a ratio landed a bit below its target.
    raw: list[int] = []
    cumulative = 0
    target = 1
    for index, count in enumerate(bar_counts):
        cumulative += count
        while target < fold_count and cumulative * fold_count > total * target:
            # ⚠ `while`, not `if`. One date can carry more than a fold's worth
            # of bars — §5.2 measured a single date carrying 4,021 — and it then
            # crosses several targets at once.
            raw.append(index)
            target += 1
        if target >= fold_count:
            break
    # Reachable when the axis tail carries no bars at all, so the scan ran out
    # of dates before it ran out of targets.
    raw.extend([len(bar_counts) - 1] * (fold_count - 1 - len(raw)))

    # ⚠ CLAMPED, so the postcondition holds on ANY axis: exactly ``fold_count``
    # contiguous non-empty folds covering it. A degenerate axis — every bar on
    # one date — otherwise yields repeated edges, and repeated edges are an
    # empty fold, which is a fold whose training set is the whole sample. The
    # clamp trades bar-equality for non-emptiness and can only fire where
    # bar-equality was unachievable anyway. ⚠ It is silent by design in the
    # LIBRARY and loud in the CALLER: the verify script reports each fold's
    # realised bar share, so a clamped cut is visible as a lopsided share rather
    # than hidden behind an exception nobody triggered.
    edges: list[int] = [0]
    for k, boundary in enumerate(raw, start=1):
        lowest = edges[-1] + 1
        # Leave one date for each remaining fold, this one included.
        highest = len(bar_counts) - (fold_count - k)
        edges.append(min(max(boundary, lowest), highest))

    bounds = [*edges, len(bar_counts)]
    return tuple(Fold(index=k, first_index=bounds[k], last_index=bounds[k + 1] - 1) for k in range(fold_count))


def role(start_index: int, end_index: int, *, fold: Fold, embargo_bars: int) -> Role:
    """Which side of ``fold`` one observation falls on, after purge and embargo.

    ``start_index`` is the observation's ENTRY FILL position on the panel axis
    and ``end_index`` its CLOSE (or, for a position still open at the window
    end, the bar it was marked at). Both inclusive — the label window is
    ``[start_index, end_index]``.

    The order of the branches IS the rule and is not interchangeable:

    1. **test** — the observation STARTS inside the fold. Its label window may
       run past the fold's end; that is the test set's own business and does not
       make it training data.
    2. **purged** — it starts outside but its label window OVERLAPS the fold.
       This is ch. 7's purge, and it is the branch that catches a trade opened
       before the fold and resolved by prices inside it.
    3. **embargoed** — it starts in ``(fold.last_index, fold.last_index +
       embargo_bars]``. ⚠ Half-open on the left and CLOSED on the right: the
       fold's own last date is the fold's, and an embargo of ``h`` bars must
       cover ``h`` dates, not ``h - 1``.
    4. **train** — everything else.

    ⚠ AN OBSERVATION STARTING BEFORE THE FOLD AND ENDING AFTER IT IS PURGED, NOT
    TRAIN. It spans the fold entirely, so every price the fold owns is inside
    its label window. Branch 2's overlap test is written on the interval and not
    on the endpoints for exactly this case.

    ⚠ ``embargo_bars == 0`` IS LEGAL AND MEANS NO EMBARGO. It is what a fold
    whose training side holds no closed observation measures, and refusing it
    would force a caller to invent a number instead of recording that there was
    nothing to measure.
    """
    if end_index < start_index:
        raise ValueError(f"observation ends at {end_index}, before its start {start_index}")
    if embargo_bars < 0:
        raise ValueError(f"embargo_bars must be non-negative, got {embargo_bars}")
    if start_index >= fold.first_index and start_index <= fold.last_index:
        return "test"
    if start_index <= fold.last_index and end_index >= fold.first_index:
        return "purged"
    if fold.last_index < start_index <= fold.last_index + embargo_bars:
        return "embargoed"
    return "train"


def training_embargo_bars(
    start_indices: Sequence[int],
    end_indices: Sequence[int],
    *,
    fold: Fold,
) -> int:
    """The embargo for ``fold``, in PANEL bars, measured off its own training side.

    The maximum label-window span among observations that lie WHOLLY outside the
    fold — the post-purge, pre-embargo training set. See the module header for
    why this is measured rather than read off ``max_hold_bars``.

    ⚠⚠ IT IS NOT CIRCULAR, and the ordering is what makes it so. The purge
    depends only on the fold; the embargo depends on the purge; nothing depends
    on the embargo. Measuring over the PRE-purge candidates instead would use
    the length of a trade whose label window reaches into the test fold, which
    is a peek at how long a fold's own prices took to resolve a position.

    ⚠ Returns 0 when the training side holds no observation at all. That is a
    real state on a lumpy axis, and 0 correctly means "no embargo measurable
    here" — see ``role``'s note on why zero is legal.

    ⚠ The span is ``end - start`` and NOT ``end - start + 1``: it is a
    DISPLACEMENT in bars, the same quantity ``bars_held`` is, and the embargo
    must cover the displacement from a training entry to the test data that
    could still be resolving it. Counting the entry bar itself would embargo one
    date more than the mechanism needs, every fold, silently.
    """
    if len(start_indices) != len(end_indices):
        raise ValueError(f"{len(start_indices)} start indices against {len(end_indices)} end indices")
    widest = 0
    for start, end in zip(start_indices, end_indices, strict=True):
        if role(start, end, fold=fold, embargo_bars=0) != "train":
            continue
        widest = max(widest, end - start)
    return widest


def census(
    start_indices: Sequence[int],
    end_indices: Sequence[int],
    *,
    fold: Fold,
    embargo_bars: int,
) -> FoldCensus:
    """Count every observation's role for one fold.

    ⚠ A counting wrapper and NOT a second implementation — it calls ``role``, so
    a rule fixed in one place cannot drift into two. The full-population sweep
    accumulates through this rather than materialising a role per observation:
    S-1 produces millions of them and a per-observation list is hundreds of MB
    for four integers.
    """
    counts = dict.fromkeys(ROLES, 0)
    for start, end in zip(start_indices, end_indices, strict=True):
        counts[role(start, end, fold=fold, embargo_bars=embargo_bars)] += 1
    return FoldCensus(
        test=counts["test"],
        train=counts["train"],
        purged=counts["purged"],
        embargoed=counts["embargoed"],
    )


__all__ = [
    "FOLD_COUNT",
    "ROLES",
    "WALK_FORWARD_MODEL_ID",
    "Fold",
    "FoldCensus",
    "FoldRecord",
    "Role",
    "WalkForwardFolds",
    "bar_weighted_folds",
    "census",
    "role",
    "training_embargo_bars",
]
