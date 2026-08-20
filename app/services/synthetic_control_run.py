"""§9's random-entry synthetic control, ORCHESTRATED against the shipped run.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §9 (*the harness
itself*). Construction and thresholds: ``app/services/random_entry_cohort.py``
(stage 5e-5b), which this module does not restate. Caller:
``app/services/backtest_run.py``. Refs #2240, #2601.

WHAT WAS MISSING, IN ONE SENTENCE
---------------------------------
``random_entry_cohort`` owns the PLACEMENT (``place_entries``) and the VERDICT
(``evaluate_control``); ``result_ledger`` owns the PERSISTENCE; ``strategy_result``
owns the three refusals. Nothing owned the middle — turning a placement into a
member's equity curve — so ``synthetic_control_not_run`` stood on every stored
row and no invocation could clear it. That middle is this module.

⚠⚠ THE MATCHING AXES ARE NOT REDEFINED HERE. ``random_entry_cohort``'s header
fixes them: *"a member differs from the real sleeve in the ENTRY BARS and in
nothing else"*, the universe / date axis / cost model / sizing rule /
quarantine arm / exit-side accounting all held fixed. This module supplies those
same fixtures from the run that is already computing them, and adds no axis of
its own. The one thing it must DECIDE is the placement SPACE — which bars count
as "a bar this strategy could have opened on" — and that decision is measured
rather than declared; see below.

THE PLACEMENT SPACE IS MEASURED FROM THE RUN'S OWN VERDICTS
-----------------------------------------------------------
``verify_2240_random_entry_cohort.py`` imports each strategy's ``WARMUP_BARS``
constant and starts the eligible space there. That does not generalise: S-2
(``cross_sectional``) exports no such constant, and a manifest field for it
would be the *"per-strategy tuning"* ``strategy_manifest``'s header refuses.

It is also unnecessary, because the run already emits the answer. §3.1 makes
evaluability a property of the STRATEGY decided before any condition runs, so
``strategy_registry.evaluate`` emits ONE verdict PER BAR and stamps the cold
ones ``not_evaluable("insufficient_warmup")``. A bar therefore carries its own
"could this strategy have decided here" flag, for every strategy class, with no
constant to keep in step.

So the eligible FILL bars of a series are, exactly:

1. the ``t+1`` of every ENTRY-leg row whose verdict is not ``not_evaluable`` —
   ``signal_ledger.resolve_fills``' own rule, *"``fill_index = signal_index + 1``,
   always"*, applied to the bars the strategy was live on rather than only to
   the ones it fired on;
2. that carry a usable open (present and ``> 0``) — the ledger's
   ``unusable_fill_price`` refusal;
3. inside the evaluation window and on the panel axis;
4. whose raw and total-return closes are both finite and positive, which is
   ``_absorb``'s ``total_return_price_missing`` exclusion applied to the
   endpoint a permuted leg would be priced at.

⚠ This is a NARROWING relative to the verify script's space (which admitted any
in-window bar with an open, past a fixed warm-up) and it is the correct
direction: every clause above is a condition the real strategy was under, and a
member placing a position where the real one structurally could not is a member
drawn from a wider null than §9 asks for.

⚠⚠ THE CONTROL IS COMPUTED FOR THE ``in_sample`` NAMESPACE ONLY, AND THAT IS A
REQUIREMENT RATHER THAN A SAVING. ``backtest_run``'s header: *"criterion 5's
whole mechanism is that looking at [the hold-out] is rare, deliberate and
logged"*. A cohort is 1,000 evaluations; running one over the withheld side is
1,000 looks at it, which is the audit trail measuring its own automation at a
thousand times the rate §4 already refused at one. A hold-out row therefore
keeps ``synthetic_control_not_run`` — DECLARED (``HOLDOUT_CONTROL_REASON``), not
silently absent.

⚠ The in-sample restriction also makes the placement exact rather than
approximate. ``namespace_for_position`` sends a position SPANNING the boundary
to the hold-out, so an in-sample position has both ends before it; the eligible
ordinals are therefore in-sample bars, and ``entry_ordinal + hold`` cannot leave
the set. A hold-out cohort would have to reproduce a population that MIXES
spanning and wholly-withheld positions, which is a second matching question §9
does not settle.

WHAT THE COHORT MATCHES, AND WHAT IT CANNOT
-------------------------------------------
The permuted population is the strategy's REALISED, COSTED, PLACEABLE positions
— the ones with a close bar, no ``uncosted_reason``, and both endpoints on the
eligible table. Everything else is counted in ``unmatchable`` and reported. That
is stage 5e-5b's own contract (*"REALISED CLOSES ONLY … the cohort matches the
realised population"*) and not a new exclusion: a position marked out at the
window end exits at a CLOSE with one side costed, so a permuted twin of it would
be a differently-priced trade.

⚠ ``MatchResidual.strategy_trade_count`` is therefore the MATCHABLE count and
not ``metrics.trade_count``. The two coincide only when nothing was unmatchable,
and ``CohortResult.unmatchable`` is on the report so the gap is visible rather
than inferred from a residual that looks exact.

THE SEED HIERARCHY
------------------
Root: ``COHORT_ROOT_SEED``, the declared constant stage 5e-5b froze and
``sql/268`` stores. Per member: ``member_seed(index)``, a ``SeedSequence`` keyed
by ``spawn_key=(index,)`` so member ``m``'s stream is a pure function of
``(root, m)`` and no sharding or ordering can move a draw. ⚠ NO SECOND ROOT IS
MINTED PER RUN. A root derived from run identity would make two evaluations of
the same strategy draw different nulls, and §9's *"with the seed recorded"* is
satisfied by a seed that is the same one next time.
"""

from __future__ import annotations

import logging
import os
import resource
import sys
import time
from array import array
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date
from multiprocessing import get_context
from multiprocessing.shared_memory import SharedMemory
from typing import Final, Protocol

import numpy as np
import numpy.typing as npt

from app.services.cost_model import UNKNOWN_NOMINAL_PRICE_BAND
from app.services.equity_curve import LegBook, SharedMarkLegBook, build_equity_curve
from app.services.indicator_series import BarSeries
from app.services.position_builder import Window
from app.services.position_costing import CostedPosition
from app.services.random_entry_cohort import (
    MATCH_QUALITY_POLICY_ID,
    SPEC_COHORT_SIZE,
    MatchResidual,
    MemberOutcome,
    SyntheticControl,
    SyntheticControlMatchQuality,
    evaluate_control,
    match_residual,
    member_seed,
    net_entry_prices,
    net_exit_prices,
    place_entries,
    slack,
)
from app.services.signal_ledger import LedgerRow
from app.services.strategy_result import ResultNamespace, namespace_for_position
from app.services.strategy_statistics import DatedEquityCurve, StrategyMetrics, TradeReturns, compute_metrics

logger = logging.getLogger(__name__)

CohortProgressCallback = Callable[[int, int], None]

#: How the eligible fill bars were selected, frozen. ⚠ A member is only
#: comparable with another member placed into the SAME space, and this space is
#: ours (no source rule fixes it) — so it is named, versioned, and reported
#: beside ``COHORT_MODEL_ID`` rather than left implicit in a loop body.
PLACEMENT_SPACE_ID: Final = "evaluable-entry-fill-bars-v1"

#: The only namespace a control is computed for. See the header.
CONTROL_NAMESPACE: Final[ResultNamespace] = "in_sample"

#: What a hold-out row's ``synthetic_control_not_run`` MEANS, so the refusal is
#: a declaration and not an omission.
HOLDOUT_CONTROL_REASON: Final = (
    "a 1,000-member cohort over the hold-out namespace is 1,000 looks at withheld data, which is what criterion 5's "
    "access log exists to keep rare; the control is computed for the in_sample namespace only"
)

#: The half-spread every leg of this corpus carries. ⚠ NOT a simplification:
#: ``cost_band_for(..., price_basis="split_adjusted")`` returns
#: ``UNKNOWN_NOMINAL_PRICE_BAND`` for EVERY price, because a split-adjusted
#: historical price cannot select a nominal band. The real sleeve is costed on
#: exactly that constant, so reading it here is sharing the cost model rather
#: than assuming one.
_HALF_SPREAD: Final = float(UNKNOWN_NOMINAL_PRICE_BAND.half_spread)

#: A resource bound, not an estimator parameter. Eight workers leave two physical
#: cores on the ten-core development host for the jobs daemon. Collector arrays
#: are attached through shared memory, so workers materialise only their own
#: member books. ``spawn`` is load-bearing: the job is invoked from a
#: ThreadPoolExecutor and forking a multithreaded process would inherit psycopg
#: connections and locks in an undefined state. Each child receives the
#: collector once in its initializer; member tasks carry only their integer
#: index, so the large placement arrays are never pickled 1,000 times.
SYNTHETIC_CONTROL_MAX_WORKERS: Final = 8

#: A production cohort must prove its own scale before the spawned pool is
#: allowed to fan out.  The pilot consumes the exact first member indices, so
#: it is part of the declared cohort rather than a throw-away or a second trial.
SYNTHETIC_CONTROL_SCALE_PILOT_MEMBERS: Final = 3
SYNTHETIC_CONTROL_PROJECTION_SAFETY_FACTOR: Final = 1.5
#: The cumulative wall-time budget for ONE backtest invocation. This is the real
#: guard against a runaway: an invocation is the whole strategy set, so this is
#: the number that says how long the box may be occupied.
SYNTHETIC_CONTROL_MAX_PROJECTED_RUN_S: Final = 4 * 60 * 60.0

#: A single cohort may not eat more than this share of the invocation's budget.
#:
#: ⚠⚠ DERIVED, BECAUSE THE FLAT VERSION DESTROYED A RUN (2026-08-19). It was
#: `20 * 60.0` — itself a recalibration from 15 minutes made after one full-S-1
#: pilot measured 16.4, i.e. a threshold fitted to a single sample with no margin
#: for variance. The variance is real and large: two arms of the SAME strategy
#: projected 862.7s and 1245.1s, a 44% spread. The second one exceeded the bound
#: by 45 seconds — 3.8% — and aborted a full-set invocation that had already
#: completed a 1,000-member cohort, losing about ninety minutes of compute and
#: persisting nothing. The bound prevented a cohort taking 21 minutes instead of
#: 20, at the cost of the entire run.
#:
#: A per-cohort bound is still worth having: it catches ONE pathological cohort
#: early rather than after the cumulative budget has drained. But it must not be
#: an independent invented number, because then two thresholds have to be kept in
#: step by hand and the tighter one silently wins. Expressed as a share of the
#: invocation budget it stays a genuine runaway guard — a cohort claiming a third
#: of the whole invocation is pathological by construction — while it cannot
#: refuse a cohort the invocation could otherwise afford.
#:
#: ⚠ Note the projection it is compared against is itself optimistic: the pilot
#: measures members serially in the parent and divides by the worker count, but
#: workers contend for memory bandwidth and run ~1.7x slower per member than the
#: parent did. Measured 2026-08-19: 862.7s projected against 960.6s actual, 11%
#: over, DESPITE the 1.5x safety factor. So the effective bound is looser than it
#: reads, which is a further reason not to fit it tightly to one sample.
SYNTHETIC_CONTROL_COHORT_BUDGET_SHARE: Final = 1.0 / 3.0
SYNTHETIC_CONTROL_MAX_PROJECTED_COHORT_S: Final = (
    SYNTHETIC_CONTROL_MAX_PROJECTED_RUN_S * SYNTHETIC_CONTROL_COHORT_BUDGET_SHARE
)


def _physical_memory_bytes() -> int:
    """Total RAM on this host, or 0 where the platform will not say."""
    try:
        return int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except ValueError, OSError, AttributeError:  # pragma: no cover - platform dependent
        return 0


#: The share of RAM one backtest invocation's projected unique memory may claim.
#: ⚠⚠ THIS BOUND IS A FUNCTION OF THE MACHINE, NOT A CONSTANT, and that is the
#: whole point. It was `8 * 1024**3` — a flat 8 GiB invented while the projection
#: was being written, with no measurement or published rule behind it. On a
#: 24 GiB host that refused a cohort projecting 8.49 GiB, which is 35% of RAM and
#: in no danger of exhausting anything; the refusal was an artefact of the
#: number's origin rather than of the machine. A ceiling that exists to stop the
#: box thrashing has to be derived from the box.
#:
#: Two thirds leaves the remaining third for PostgreSQL, the jobs daemon and the
#: operating system, which is the co-tenancy this actually shares. Where the
#: platform will not report its memory, the old flat 8 GiB stands as the
#: fallback — an unknown machine is the one case where a conservative constant
#: beats a derived one.
SYNTHETIC_CONTROL_MEMORY_BUDGET_SHARE: Final = 2.0 / 3.0
SYNTHETIC_CONTROL_MAX_PROJECTED_MEMORY_BYTES: Final = (
    int(_physical_memory_bytes() * SYNTHETIC_CONTROL_MEMORY_BUDGET_SHARE) or 8 * 1024**3
)
# Measured spawned workers are ~80-106 MiB before a large member book. This is
# the FLOOR under the per-worker unique-memory figure, which is otherwise
# measured in a freshly spawned child (#2775). ⚠ It was previously an additive
# base on top of a parent-side ``ru_maxrss`` delta, and that delta is zero from
# the second cohort of an invocation onward, so this constant silently became
# the whole per-worker estimate.
SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES: Final = 128 * 1024**2
SYNTHETIC_CONTROL_WORKER_CANARY_MEMBERS: Final = 8
SYNTHETIC_CONTROL_WORKER_CANARY_COUNTS: Final = (1, 2, 4, 8)


class ScaleBudgetExceeded(RuntimeError):
    """Outcome-free refusal raised before an oversized cohort fans out."""


class WorkerCanaryBudgetExceeded(RuntimeError):
    """Outcome-free refusal raised by the fixed worker canary."""


@dataclass(frozen=True)
class WorkerCanaryConfig:
    """Fixed work and resource ceiling for a canary that can never fan out."""

    member_count: int = SYNTHETIC_CONTROL_WORKER_CANARY_MEMBERS
    worker_counts: tuple[int, ...] = SYNTHETIC_CONTROL_WORKER_CANARY_COUNTS
    max_aggregate_peak_rss_bytes: int = 8 * 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.member_count < 1:
            raise ValueError("worker canary member_count must be positive")
        if not self.worker_counts or any(count < 1 for count in self.worker_counts):
            raise ValueError("worker canary counts must be non-empty and positive")
        if len(set(self.worker_counts)) != len(self.worker_counts):
            raise ValueError("worker canary counts must be unique")
        if any(count > SYNTHETIC_CONTROL_MAX_WORKERS for count in self.worker_counts):
            raise ValueError(f"worker canary cannot exceed the production cap of {SYNTHETIC_CONTROL_MAX_WORKERS}")
        if any(self.member_count % count != 0 for count in self.worker_counts):
            raise ValueError("worker canary member_count must divide evenly across every worker count")
        if self.max_aggregate_peak_rss_bytes < 1:
            raise ValueError("worker canary RSS budget must be positive")


@dataclass(frozen=True)
class WorkerCanaryTrial:
    """Outcome-free resource evidence for one spawned worker count."""

    workers: int
    member_count: int
    distinct_worker_pids: int
    startup_and_transfer_s: float
    member_wall_s: float
    total_wall_s: float
    members_per_s: float
    parent_peak_rss_bytes: int
    max_child_peak_rss_bytes: int
    aggregate_peak_rss_bytes: int
    aggregate_unique_peak_bound_bytes: int
    exact_equivalent: bool


@dataclass(frozen=True)
class WorkerCanaryReport:
    """A bounded canary report containing resources and structure, never outcomes."""

    member_indices: tuple[int, ...]
    placement_series: int
    trades_per_member: int
    shared_input_bytes: int
    shared_preparation_s: float
    trials: tuple[WorkerCanaryTrial, ...]
    stopped_before_full_cohort: bool = True


@dataclass(frozen=True)
class LaunchPilotReport:
    """Full-collector resource projection with no member outcomes or fan-out."""

    member_indices: tuple[int, ...]
    placement_series: int
    trades_per_member: int
    shared_input_bytes: int
    pilot_wall_s: float
    seconds_per_member: float
    projected_cohort_s: float
    projected_unique_memory_bytes: int
    max_cohort_s: float
    max_memory_bytes: int
    time_admitted: bool
    memory_admitted: bool
    #: ⚠ The projection's two terms, reported separately so a refusal says WHICH
    #: it is and what would change it. ``projected_unique_memory_bytes`` is
    #: ``parent_peak_bytes + max_workers * per_worker_unique_bytes``, and
    #: ``max_workers`` changes execution only — spawned members are byte-for-byte
    #: the serial ones — so a memory refusal at one worker count can be re-asked
    #: at another from these two numbers alone, without re-running a corpus pass.
    parent_peak_bytes: int = 0
    per_worker_unique_bytes: int = 0
    #: ⚠ ``False`` means ``per_worker_unique_bytes`` is the FLOOR, not a
    #: measurement, and nothing may be extrapolated from it.
    per_worker_measured: bool = False
    projection_max_workers: int = 0
    stopped_before_full_cohort: bool = True

    @property
    def admitted(self) -> bool:
        return self.time_admitted and self.memory_admitted

    def admissible_workers(self, *, max_memory_bytes: int | None = None) -> int | None:
        """The largest worker count whose projection fits the memory ceiling.

        ⚠ ``None`` means the per-worker figure was never measured, so there is
        nothing to divide by — dividing a ceiling by the FLOOR would over-state
        how wide a fan-out fits, which is the one direction this must not err in.
        ⚠ Zero is a different answer: the PARENT alone already exceeds the
        ceiling, which no worker count can fix. That is a corpus-representation
        problem rather than a fan-out one, and the two want different next moves.
        """
        if not self.per_worker_measured or self.per_worker_unique_bytes <= 0:
            return None
        ceiling = self.max_memory_bytes if max_memory_bytes is None else max_memory_bytes
        return max((ceiling - self.parent_peak_bytes) // self.per_worker_unique_bytes, 0)


@dataclass
class SyntheticControlScaleBudget:
    """Cumulative launch budget shared by every control in one backtest."""

    max_cohort_s: float = SYNTHETIC_CONTROL_MAX_PROJECTED_COHORT_S
    max_run_s: float = SYNTHETIC_CONTROL_MAX_PROJECTED_RUN_S
    max_memory_bytes: int = SYNTHETIC_CONTROL_MAX_PROJECTED_MEMORY_BYTES
    projected_run_s: float = 0.0

    def reserve(self, *, label: str, projected_s: float, projected_memory_bytes: int = 0) -> None:
        if projected_s > self.max_cohort_s:
            raise ScaleBudgetExceeded(
                f"synthetic-control scale gate refused {label}: projected cohort wall time "
                f"{projected_s:.1f}s exceeds {self.max_cohort_s:.1f}s budget"
            )
        projected_run_s = self.projected_run_s + projected_s
        if projected_run_s > self.max_run_s:
            raise ScaleBudgetExceeded(
                f"synthetic-control scale gate refused {label}: cumulative projected run wall time "
                f"{projected_run_s:.1f}s exceeds {self.max_run_s:.1f}s budget"
            )
        if projected_memory_bytes > self.max_memory_bytes:
            raise ScaleBudgetExceeded(
                f"synthetic-control scale gate refused {label}: projected unique-memory upper bound "
                f"{projected_memory_bytes:,} bytes exceeds {self.max_memory_bytes:,} byte budget"
            )
        self.projected_run_s = projected_run_s


# ---------------------------------------------------------------------------
# The placement space, accumulated during the run's own corpus pass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeriesPlacement:
    """One instrument's eligible fill bars, and the holds to permute into them.

    ⚠ ONE ``adjusted_open`` ARRAY AND NOT TWO PRICE ARRAYS. The net buy and sell
    prices are the same number times ``(1 ± h)`` with ``h`` constant over this
    corpus, so storing both would double a per-eligible-bar array over 5,266
    series to hold a scalar multiple. The multiply happens per member, where it
    is vectorised anyway.

    ⚠ ``marks`` IS THE RUN'S OWN ARRAY, SHARED NOT COPIED. ``evaluate_arm``
    already retains the total-return close array per instrument to build the
    benchmark; a second copy is ~200 MB of the same floats. It is read-only from
    here on.
    """

    #: Panel-axis index of each eligible fill bar, strictly ascending.
    panel: npt.NDArray[np.int64]
    #: That bar's open, already carried onto the total-return basis by the same
    #: ``wealth_close / raw_close`` factor ``_absorb`` applies to a real leg.
    adjusted_open: npt.NDArray[np.float64]
    #: The realised holds to permute, in ELIGIBLE-ORDINAL units.
    holds: npt.NDArray[np.int64]
    #: Total-return closes, panel-aligned from ``marks_first``.
    #:
    #: ⚠⚠ AN ndarray AND NOT THE CALLER'S ``list``. ``evaluate_arm`` builds this
    #: span as a Python list and then keeps only a compact ``array("d")`` copy of
    #: it; retaining the LIST here would pin ~25M boxed Python floats across the
    #: whole corpus — gigabytes, for the same numbers the run already holds in
    #: 200 MB. Converted at collection, once per series.
    marks: npt.NDArray[np.float64]
    marks_first: int

    def __post_init__(self) -> None:
        if self.panel.size != self.adjusted_open.size:
            raise ValueError(f"{self.panel.size} eligible bars against {self.adjusted_open.size} prices")


@dataclass
class CohortCollector:
    """The cohort's inputs, gathered from the corpus pass the run already makes.

    ⚠⚠ NO SECOND CORPUS READ. §9's control was costed at stage 5e-5b as
    *"~5.5 CPU hours of ARITHMETIC sitting behind ~1.5 hours of DATABASE READ
    that is identical for every member"*, and the verify script paid the read
    once by caching to disk. A job cannot depend on a developer cache, so the
    read is paid by riding the pass ``evaluate_arm`` is making anyway — the
    collector is fed inside its loop and holds only what a member needs.
    """

    window: Window
    placements: list[SeriesPlacement] = field(default_factory=list)
    #: Realised positions the permutation cannot reproduce, by reason.
    unmatchable: Counter[str] = field(default_factory=Counter)
    #: Series whose realised holds do not fit their own eligible space. ⚠ A
    #: CONTRADICTION rather than a rare shape — the real positions are
    #: non-overlapping in this same ordinal space — so the series is refused
    #: whole and counted, never trimmed. Trimming would change the match.
    no_slack_series: int = 0

    def collect(
        self,
        *,
        rows: Sequence[LedgerRow],
        series: BarSeries,
        costed: Sequence[CostedPosition],
        axis_pos: Mapping[date, int],
        raw_closes: Sequence[float],
        wealth_closes: Sequence[float],
        first_axis_index: int,
    ) -> None:
        """Add one instrument's placement space. Called once per series."""
        # ⚠ BUILT ONCE AND PASSED DOWN. Both halves of this method need the
        # date -> bar-index map and the corpus pass calls this ~5,266 times per
        # arm, so building it twice is a second full dict per series for nothing
        # (review bot NITPICK, PR #2619).
        bar_of = {when: index for index, when in enumerate(series.dates)}
        eligible_bar, panel, adjusted = _eligible_fill_bars(
            rows=rows,
            series=series,
            bar_of=bar_of,
            axis_pos=axis_pos,
            window=self.window,
            raw_closes=raw_closes,
            wealth_closes=wealth_closes,
            first_axis_index=first_axis_index,
        )
        if not eligible_bar:
            _count_unmatchable(costed, self.unmatchable)
            return
        ordinal_of = {bar: ordinal for ordinal, bar in enumerate(eligible_bar)}
        holds = _matchable_holds(costed, bar_of=bar_of, ordinal_of=ordinal_of, unmatchable=self.unmatchable)
        if not holds:
            return
        held = np.asarray(holds, dtype=np.int64)
        if slack(eligible=len(eligible_bar), holds=held) < 0:
            self.no_slack_series += 1
            self.unmatchable["series_cannot_carry_its_own_holds"] += len(holds)
            return
        self.placements.append(
            SeriesPlacement(
                panel=np.asarray(panel, dtype=np.int64),
                adjusted_open=np.asarray(adjusted, dtype=np.float64),
                holds=held,
                marks=np.asarray(wealth_closes, dtype=np.float64),
                marks_first=first_axis_index,
            )
        )

    @property
    def matchable_trade_count(self) -> int:
        return sum(int(placement.holds.size) for placement in self.placements)


def _eligible_fill_bars(
    *,
    rows: Sequence[LedgerRow],
    series: BarSeries,
    bar_of: Mapping[date, int],
    axis_pos: Mapping[date, int],
    window: Window,
    raw_closes: Sequence[float],
    wealth_closes: Sequence[float],
    first_axis_index: int,
) -> tuple[list[int], list[int], list[float]]:
    """The four clauses in the header, applied in order. Returns parallel lists.

    ⚠ KEYED ON THE ENTRY LEG ONLY. An exit-leg verdict says nothing about
    whether a position could be OPENED on the following bar, and S-1/S-3 emit
    both legs on every bar — so counting exits would double the space and let a
    member open where the strategy was cold.

    ⚠ ``bar_of`` IS SUPPLIED, not rebuilt. The caller already needs the same
    ``date -> bar index`` map to place the realised holds.
    """
    fills: set[int] = set()
    for row in rows:
        if row.signal_kind != "entry" or row.not_evaluable_reason is not None:
            continue
        signal_index = bar_of.get(row.signal_bar_date)
        if signal_index is None:  # pragma: no cover - rows are resolved from this series
            continue
        fill_index = signal_index + 1
        if fill_index >= len(series):
            # `no_fill_bar` — the series ended, so no decision on that bar could
            # have been acted on. `resolve_fills` refuses it for the real leg.
            continue
        fills.add(fill_index)

    eligible_bar: list[int] = []
    panel: list[int] = []
    adjusted: list[float] = []
    for fill_index in sorted(fills):
        when = series.dates[fill_index]
        if not window.contains(when):
            continue
        slot = axis_pos.get(when)
        if slot is None:
            continue
        if namespace_for_position(when, when) != CONTROL_NAMESPACE:
            continue
        bar_open = series.rows[fill_index].get("open")
        if bar_open is None or bar_open <= 0:
            continue
        offset = slot - first_axis_index
        if not 0 <= offset < len(raw_closes):  # pragma: no cover - slot is inside this series' span
            continue
        raw_close = raw_closes[offset]
        wealth_close = wealth_closes[offset]
        if not _usable(raw_close) or not _usable(wealth_close):
            continue
        eligible_bar.append(fill_index)
        panel.append(slot)
        # The same carry ``_absorb`` applies to a real leg: the net price is
        # scaled by that bar's own total-return factor, so the two are on one
        # basis and the cohort is not comparing an adjusted sleeve with an
        # unadjusted null.
        adjusted.append(float(bar_open) * wealth_close / raw_close)
    return eligible_bar, panel, adjusted


def _usable(value: float) -> bool:
    return bool(np.isfinite(value)) and value > 0.0


def _count_unmatchable(costed: Sequence[CostedPosition], unmatchable: Counter[str]) -> None:
    """Charge every realised in-sample position to ``no_eligible_bar``."""
    for row in costed:
        if namespace_for_position(row.position.entry_fill_bar_date, row.position.close_bar_date) == CONTROL_NAMESPACE:
            unmatchable["no_eligible_bar"] += 1


def _matchable_holds(
    costed: Sequence[CostedPosition],
    *,
    bar_of: Mapping[date, int],
    ordinal_of: Mapping[int, int],
    unmatchable: Counter[str],
) -> list[int]:
    """This series' realised in-sample holds, in eligible-ordinal units.

    ⚠ EVERY REJECTION IS COUNTED UNDER ITS OWN CODE. A silently shorter hold
    list is a cohort matched to a population nobody named, and the residual
    would still read as exact because the permutation preserves whatever it was
    given.
    """
    holds: list[int] = []
    for row in costed:
        position = row.position
        if namespace_for_position(position.entry_fill_bar_date, position.close_bar_date) != CONTROL_NAMESPACE:
            continue
        if row.uncosted_reason is not None:
            unmatchable[row.uncosted_reason] += 1
            continue
        if position.close_bar_date is None:  # pragma: no cover - an in-sample position is closed by construction
            unmatchable["open_at_window_end"] += 1
            continue
        entry_bar = bar_of.get(position.entry_fill_bar_date)
        exit_bar = bar_of.get(position.close_bar_date)
        if entry_bar is None or exit_bar is None:  # pragma: no cover - both are bars of this series
            unmatchable["bar_off_series"] += 1
            continue
        entry_ordinal = ordinal_of.get(entry_bar)
        exit_ordinal = ordinal_of.get(exit_bar)
        if entry_ordinal is None or exit_ordinal is None or exit_ordinal < entry_ordinal:
            # The exit bar is not itself an eligible ENTRY bar — its open is
            # unusable, or its closes are missing. A permuted twin would have to
            # exit on a bar the cohort cannot price, so it is not matched.
            unmatchable["endpoint_not_eligible"] += 1
            continue
        holds.append(exit_ordinal - entry_ordinal)
    return holds


# ---------------------------------------------------------------------------
# Running the cohort
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CohortResult:
    """§9's control, its match residual, and what the run had to exclude."""

    control: SyntheticControl
    residual: MatchResidual
    placement_space_id: str
    #: Realised in-sample positions the permutation could not reproduce.
    unmatchable: Mapping[str, int]
    no_slack_series: int
    series_placed: int
    elapsed_s: float

    @property
    def seconds_per_member(self) -> float:
        return self.elapsed_s / self.control.cohort_size


def run_cohort(
    collector: CohortCollector,
    *,
    axis: Sequence[date],
    strategy_metrics: StrategyMetrics,
    benchmark: DatedEquityCurve | None,
    cohort_size: int = SPEC_COHORT_SIZE,
    progress: CohortProgressCallback | None = None,
    max_workers: int | None = None,
    scale_budget: SyntheticControlScaleBudget | None = None,
    label: str = "cohort",
) -> CohortResult:
    """Place, price and measure ``cohort_size`` members. Pure; reads no database.

    ``axis`` is the real strategy's complete fixed namespace tuple. Every member
    uses it unchanged; random placement may change the path but cannot select a
    more flattering annualisation interval.

    ⚠⚠ ``benchmark`` IS ``None`` ON EVERY REAL CALL AND THAT IS FORCED, not a
    shortcut. ``compute_metrics`` refuses a benchmark whose curve length differs
    from the strategy's (*"the benchmark curve has N points against the
    strategy's M"*), and each member's span is its own — so the sleeve's
    benchmark cannot be reused, and building one per member would compute
    ``return_vs_buy_and_hold_pct``, which NEITHER of §9's two cuts consumes,
    1,000 times. ``compute_metrics`` already documents the null case as an
    explicit absence rather than a silent 0% benchmark. The parameter stays so a
    test with a matching span can exercise the populated path.

    ⚠ NO BLOCK BOOTSTRAP PER MEMBER. §9 reads the cohort's Sharpe and net
    return; criterion 3's correction is a property of the REAL sleeve's trade
    population, and running it 1,000 times would add hours to compute a number
    no threshold consumes. Stage 5e-5b's ``--cohort`` arm made the same call.

    ⚠⚠ THERE IS NO ``root_seed`` PARAMETER, AND ITS ABSENCE IS THE POINT. An
    earlier draft took one, recorded it on the row, and still drew every member
    from ``member_seed(index)`` — which is keyed on ``COHORT_ROOT_SEED`` and
    nothing else. The stored seed would then have described the bootstrap and
    NOT the cohort it sits beside, so §9's *"with the seed recorded"* would be
    satisfied by a number that reproduces neither. (Codex checkpoint 2.) The
    root is the module constant, one value, used for both.

    ``max_workers`` changes execution only. The production default uses a
    bounded spawned pool for the declared 1,000-member cohort and keeps smaller
    diagnostic/test cohorts serial; callers may force a worker count to verify
    equivalence. Member identity and random state remain keyed only by index,
    and aggregation refuses any incomplete or duplicate index set.
    """
    if cohort_size < 1:
        raise ValueError(f"cohort size must be positive, got {cohort_size}")
    if not collector.placements:
        raise ValueError(
            "no series carries a placeable in-sample position — §9's control cannot be built against an empty "
            "placement space, and a control computed from nothing would read as a passed threshold"
        )
    started = time.monotonic()
    expected = collector.matchable_trade_count
    workers = (
        min(SYNTHETIC_CONTROL_MAX_WORKERS, cohort_size)
        if max_workers is None and cohort_size == SPEC_COHORT_SIZE
        else 1
        if max_workers is None
        else max_workers
    )
    if workers < 1:
        raise ValueError(f"max_workers must be positive, got {workers}")
    workers = min(workers, cohort_size)
    inputs = _MemberInputs(
        placements=tuple(collector.placements),
        axis=tuple(axis),
        benchmark=benchmark,
        expected_trade_count=expected,
    )
    production_scale = cohort_size == SPEC_COHORT_SIZE and max_workers is None
    if production_scale and scale_budget is None:
        scale_budget = SyntheticControlScaleBudget()
    frozen = _run_members(
        inputs,
        cohort_size=cohort_size,
        max_workers=workers,
        progress=progress,
        scale_budget=scale_budget if production_scale else None,
        label=label,
    )
    residual = match_residual(
        frozen,
        # ⚠ THE MATCHABLE COUNT, not ``metrics.trade_count``. See the header:
        # the cohort is permuted from the realised, costed, placeable
        # population, and comparing it against a wider one would report a
        # residual for a match nobody attempted.
        strategy_trade_count=expected,
        strategy_exposure_time_pct=strategy_metrics.exposure_time_pct,
        strategy_turnover_annualised=strategy_metrics.turnover_annualised,
    )
    control = replace(
        evaluate_control(
            frozen,
            strategy_sharpe=strategy_metrics.sharpe,
            strategy_return_pct=strategy_metrics.total_return_pct,
            # ⚠ NOT PASSED, so it takes ``evaluate_control``'s own default —
            # which is ``COHORT_ROOT_SEED``, the same constant ``member_seed``
            # keys on. One root, both uses, no way for them to disagree.
        ),
        match_quality=SyntheticControlMatchQuality(
            policy_id=MATCH_QUALITY_POLICY_ID,
            placement_space_id=PLACEMENT_SPACE_ID,
            matchable_trade_count=residual.strategy_trade_count,
            cohort_mean_trade_count=residual.cohort_mean_trade_count,
            unmatchable_by_reason=dict(collector.unmatchable),
            no_slack_series=collector.no_slack_series,
            series_placed=len(collector.placements),
            strategy_exposure_time_pct=residual.strategy_exposure_time_pct,
            cohort_mean_exposure_time_pct=residual.cohort_mean_exposure_time_pct,
            strategy_turnover_annualised=residual.strategy_turnover_annualised,
            cohort_mean_turnover_annualised=residual.cohort_mean_turnover_annualised,
        ),
    )
    return CohortResult(
        control=control,
        residual=residual,
        placement_space_id=PLACEMENT_SPACE_ID,
        unmatchable=dict(collector.unmatchable),
        no_slack_series=collector.no_slack_series,
        series_placed=len(collector.placements),
        elapsed_s=time.monotonic() - started,
    )


@dataclass(frozen=True)
class _MemberInputs:
    """Read-only inputs installed once in each spawned cohort worker."""

    placements: tuple[SeriesPlacement, ...]
    axis: tuple[date, ...]
    benchmark: DatedEquityCurve | None
    expected_trade_count: int


_WORKER_INPUTS: _MemberInputs | None = None
_WORKER_SHARED_HANDLES: tuple[SharedMemory, ...] = ()


@dataclass(frozen=True)
class _PlacementSlice:
    panel_start: int
    panel_size: int
    holds_start: int
    holds_size: int
    marks_start: int
    marks_size: int
    marks_first: int


@dataclass(frozen=True)
class _SharedMemberInputs:
    """Names and offsets only; spawned workers attach instead of unpickling arrays."""

    panel_name: str
    panel_size: int
    adjusted_open_name: str
    adjusted_open_size: int
    holds_name: str
    holds_size: int
    marks_name: str
    marks_size: int
    placements: tuple[_PlacementSlice, ...]
    axis: tuple[date, ...]
    benchmark: DatedEquityCurve | None
    expected_trade_count: int
    shared_input_bytes: int


def _copy_to_shared(values: npt.NDArray[np.generic]) -> SharedMemory:
    if values.nbytes < 1:
        raise ValueError("a shared synthetic-control input cannot be empty")
    shared = SharedMemory(create=True, size=values.nbytes)
    np.ndarray(values.shape, dtype=values.dtype, buffer=shared.buf)[:] = values
    return shared


@contextmanager
def _shared_member_inputs(inputs: _MemberInputs) -> Iterator[_SharedMemberInputs]:
    """Pack four contiguous immutable arrays and release them after the pool."""
    panel = np.concatenate([placement.panel for placement in inputs.placements]).astype(np.int64, copy=False)
    adjusted = np.concatenate([placement.adjusted_open for placement in inputs.placements]).astype(
        np.float64, copy=False
    )
    holds = np.concatenate([placement.holds for placement in inputs.placements]).astype(np.int64, copy=False)
    marks = np.concatenate([placement.marks for placement in inputs.placements]).astype(np.float64, copy=False)
    shared_handles: list[SharedMemory] = []
    try:
        for values in (panel, adjusted, holds, marks):
            shared_handles.append(_copy_to_shared(values))
        panel_cursor = holds_cursor = marks_cursor = 0
        slices: list[_PlacementSlice] = []
        for placement in inputs.placements:
            slices.append(
                _PlacementSlice(
                    panel_start=panel_cursor,
                    panel_size=int(placement.panel.size),
                    holds_start=holds_cursor,
                    holds_size=int(placement.holds.size),
                    marks_start=marks_cursor,
                    marks_size=int(placement.marks.size),
                    marks_first=placement.marks_first,
                )
            )
            panel_cursor += int(placement.panel.size)
            holds_cursor += int(placement.holds.size)
            marks_cursor += int(placement.marks.size)
        yield _SharedMemberInputs(
            panel_name=shared_handles[0].name,
            panel_size=int(panel.size),
            adjusted_open_name=shared_handles[1].name,
            adjusted_open_size=int(adjusted.size),
            holds_name=shared_handles[2].name,
            holds_size=int(holds.size),
            marks_name=shared_handles[3].name,
            marks_size=int(marks.size),
            placements=tuple(slices),
            axis=inputs.axis,
            benchmark=inputs.benchmark,
            expected_trade_count=inputs.expected_trade_count,
            shared_input_bytes=sum(handle.size for handle in shared_handles),
        )
    finally:
        for shared in shared_handles:
            shared.close()
            shared.unlink()


def _attach_shared_member_inputs(shared: _SharedMemberInputs) -> _MemberInputs:
    global _WORKER_SHARED_HANDLES
    handles = tuple(
        SharedMemory(name=name, track=False)
        for name in (shared.panel_name, shared.adjusted_open_name, shared.holds_name, shared.marks_name)
    )
    _WORKER_SHARED_HANDLES = handles
    panel = np.ndarray((shared.panel_size,), dtype=np.int64, buffer=handles[0].buf)
    adjusted = np.ndarray((shared.adjusted_open_size,), dtype=np.float64, buffer=handles[1].buf)
    holds = np.ndarray((shared.holds_size,), dtype=np.int64, buffer=handles[2].buf)
    marks = np.ndarray((shared.marks_size,), dtype=np.float64, buffer=handles[3].buf)
    for values in (panel, adjusted, holds, marks):
        values.setflags(write=False)
    placements = tuple(
        SeriesPlacement(
            panel=panel[item.panel_start : item.panel_start + item.panel_size],
            adjusted_open=adjusted[item.panel_start : item.panel_start + item.panel_size],
            holds=holds[item.holds_start : item.holds_start + item.holds_size],
            marks=marks[item.marks_start : item.marks_start + item.marks_size],
            marks_first=item.marks_first,
        )
        for item in shared.placements
    )
    return _MemberInputs(
        placements=placements,
        axis=shared.axis,
        benchmark=shared.benchmark,
        expected_trade_count=shared.expected_trade_count,
    )


class _Barrier(Protocol):
    def wait(self, timeout: float | None = None) -> int: ...


_WORKER_CANARY_BARRIER: _Barrier | None = None


@dataclass(frozen=True)
class _WorkerCanarySample:
    outcome: MemberOutcome
    pid: int
    measure_started_ns: int
    measure_elapsed_s: float
    peak_rss_bytes: int


def _initialise_member_worker(inputs: _SharedMemberInputs) -> None:
    """Attach the immutable collector once; no placement array is unpickled."""
    global _WORKER_INPUTS
    _WORKER_INPUTS = _attach_shared_member_inputs(inputs)


def _initialise_canary_worker(inputs: _SharedMemberInputs, barrier: _Barrier) -> None:
    global _WORKER_CANARY_BARRIER
    _initialise_member_worker(inputs)
    _WORKER_CANARY_BARRIER = barrier


def _measure_member_in_worker(index: int) -> MemberOutcome:
    if _WORKER_INPUTS is None:  # pragma: no cover - ProcessPoolExecutor owns initialization
        raise RuntimeError("synthetic-control worker started without member inputs")
    return _measure_member(index, _WORKER_INPUTS)


def _peak_rss_bytes() -> int:
    """Normalise ``ru_maxrss`` to bytes on macOS and Linux."""
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _measure_member_peak_in_worker(index: int) -> int:
    """One member in this child, then the child's OWN lifetime peak."""
    if _WORKER_INPUTS is None:  # pragma: no cover - the pool initializer owns it
        raise RuntimeError("synthetic-control worker started without inputs")
    _measure_member(index, _WORKER_INPUTS)
    return _peak_rss_bytes()


def _measure_child_member_peak(inputs: _MemberInputs, *, index: int) -> tuple[int, int]:
    """Measure one member in a FRESH child and return its peak and the shared bytes.

    ⚠⚠ THE PARENT CANNOT MEASURE THIS, AND SUBTRACTING TWO OF ITS READINGS IS
    NOT A MEASUREMENT (#2775). ``ru_maxrss`` is a process LIFETIME high-water
    mark and never decreases, so ``after - before`` around a pilot answers *"how
    much did this pilot push the record up"* rather than *"what does a member
    cost"*. That difference is ZERO as soon as the process has already been that
    large — which is every cohort after the first in one invocation (``run_cohort``
    is called once per ambiguity x quarantine arm against one shared budget), and
    any cohort at all in a daemon that peaked earlier serving another job. The
    per-worker allowance then collapsed to the flat base and the memory arm of
    the gate admitted a fan-out on a measurement of nothing, with every logged
    field internally consistent.

    A freshly spawned child starts with its own mark, so its peak IS the figure
    the projection needs. ⚠ The peak includes the shared pages the child
    touched, so the caller subtracts them exactly as the worker canary does;
    counting them once per worker would over-state unique memory eightfold.

    ⚠ The outcome is DISCARDED rather than accepted. Seeds are a pure function
    of the member index, so this repeats work the parent pilot already did; it
    adds no member, moves no draw and changes no cohort identity.
    """
    with _shared_member_inputs(inputs) as shared:
        with ProcessPoolExecutor(
            max_workers=1,
            mp_context=get_context("spawn"),
            initializer=_initialise_member_worker,
            initargs=(shared,),
        ) as pool:
            child_peak = int(pool.submit(_measure_member_peak_in_worker, index).result())
        return child_peak, int(shared.shared_input_bytes)


def _project_unique_memory_bytes(
    inputs: _MemberInputs,
    *,
    max_workers: int,
    measure_child: bool,
) -> tuple[int, int, bool]:
    """Projected unique memory for the fan-out, and the per-worker figure in it.

    ⚠⚠ ONE COPY OF THIS RULE, DELIBERATELY (#2775). It was written twice — once
    in ``_run_members`` and once in ``run_launch_pilot`` — and the first repair
    fixed only one of them, which is the failure mode the equity-curve harness
    had just been rebuilt around. Both callers now go through here.

    ⚠⚠ THE PARENT PEAK IS READ LAST, AND THAT ORDER IS THE POINT. Building the
    shared inputs costs the parent about TWICE ``shared_input_bytes`` on top of
    the placements it already holds: ``_shared_member_inputs`` concatenates the
    per-series arrays into four contiguous temporaries AND allocates equally
    sized ``SharedMemory`` blocks to copy them into, and the temporaries stay
    referenced for the pool's whole lifetime. A projection that adds
    ``shared_input_bytes`` once under-states the parent by a whole copy of the
    corpus, which on a large one is the difference between admitting and
    refusing. Reading ``ru_maxrss`` AFTER the child probe has built and released
    those blocks measures the real cost instead of modelling it, so there is no
    coefficient to get wrong.

    ⚠ WHETHER TO MEASURE IS THE CALLER'S CALL, not a function of the worker
    count. It used to be gated on ``max_workers > 1`` here, which quietly
    returned the FLOOR for a one-worker projection — harmless while the figure
    was only summed, and unsafe as soon as it was reported, because dividing a
    ceiling by a floor over-states how wide a fan-out would fit. The third
    return value says whether the figure was measured, so no caller can mistake
    the floor for a measurement.

    ⚠ The child's own peak includes the shared pages it mapped, so they are
    subtracted before multiplying: counting them once per worker would over-state
    unique memory eightfold. This is the same accounting the worker canary uses.
    """
    per_worker_unique_bytes = SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES
    if measure_child:
        child_peak_bytes, shared_block_bytes = _measure_child_member_peak(inputs, index=0)
        per_worker_unique_bytes = max(child_peak_bytes - shared_block_bytes, SYNTHETIC_CONTROL_WORKER_BASE_RSS_BYTES)
    parent_peak_bytes = _peak_rss_bytes()
    return parent_peak_bytes + max_workers * per_worker_unique_bytes, per_worker_unique_bytes, measure_child


def _measure_canary_member_in_worker(index: int) -> _WorkerCanarySample:
    if _WORKER_INPUTS is None or _WORKER_CANARY_BARRIER is None:  # pragma: no cover - pool initializer owns it
        raise RuntimeError("synthetic-control canary worker started without inputs or its barrier")
    # One task per worker reaches each barrier generation. That forces all
    # configured children to initialise the collector before any member starts,
    # so a one-process scheduler cannot masquerade as a four-worker canary.
    _WORKER_CANARY_BARRIER.wait(timeout=60.0)
    started_ns = time.monotonic_ns()
    outcome = _measure_member(index, _WORKER_INPUTS)
    elapsed_s = (time.monotonic_ns() - started_ns) / 1_000_000_000.0
    return _WorkerCanarySample(
        outcome=outcome,
        pid=os.getpid(),
        measure_started_ns=started_ns,
        measure_elapsed_s=elapsed_s,
        peak_rss_bytes=_peak_rss_bytes(),
    )


def _measure_member(index: int, inputs: _MemberInputs) -> MemberOutcome:
    """One index-keyed draw through the unchanged placement/curve/metric path."""
    rng = np.random.Generator(np.random.PCG64(member_seed(index)))
    book, returns, entry_dates, exit_dates = _place_member_compact(rng, inputs.placements, axis=inputs.axis)
    if len(book) != inputs.expected_trade_count:
        # ⚠ EQUALITY, per member. The permutation preserves the trade count by
        # construction, so a tolerance would hide exactly the dropped-hold
        # failure this check exists to expose.
        raise RuntimeError(
            f"cohort member {index} placed {len(book):,} legs against the strategy's "
            f"{inputs.expected_trade_count:,} matchable positions — the permutation is supposed to preserve the "
            "count per series"
        )
    curve = build_equity_curve(book, date_count=len(inputs.axis))
    metrics = compute_metrics(
        DatedEquityCurve(dates=inputs.axis, curve=curve),
        trades=TradeReturns(
            net_return_pct=tuple(returns),
            entry_fill_date=tuple(entry_dates),
            exit_bar_date=tuple(exit_dates),
            open_count=0,
            unpriced_count=0,
        ),
        buy_and_hold=inputs.benchmark,
        bootstrap_seed=None,
    )
    return MemberOutcome(
        index=index,
        sharpe=metrics.sharpe,
        total_return_pct=metrics.total_return_pct,
        exposure_time_pct=metrics.exposure_time_pct,
        turnover_annualised=metrics.turnover_annualised,
        trade_count=metrics.trade_count,
    )


def _shared_input_bytes(inputs: _MemberInputs) -> int:
    return sum(
        placement.panel.nbytes + placement.adjusted_open.nbytes + placement.holds.nbytes + placement.marks.nbytes
        for placement in inputs.placements
    )


def run_launch_pilot(
    collector: CohortCollector,
    *,
    axis: Sequence[date],
    benchmark: DatedEquityCurve | None = None,
    max_workers: int = SYNTHETIC_CONTROL_MAX_WORKERS,
    max_cohort_s: float = SYNTHETIC_CONTROL_MAX_PROJECTED_COHORT_S,
    max_memory_bytes: int = SYNTHETIC_CONTROL_MAX_PROJECTED_MEMORY_BYTES,
) -> LaunchPilotReport:
    """Run exactly production members 0..2 and project; never continue."""
    if not collector.placements:
        raise ValueError("launch pilot requires at least one placeable series")
    if not 1 <= max_workers <= SYNTHETIC_CONTROL_MAX_WORKERS:
        raise ValueError(f"launch pilot workers must be inside 1..{SYNTHETIC_CONTROL_MAX_WORKERS}")
    inputs = _MemberInputs(
        placements=tuple(collector.placements),
        axis=tuple(axis),
        benchmark=benchmark,
        expected_trade_count=collector.matchable_trade_count,
    )
    indices = tuple(range(SYNTHETIC_CONTROL_SCALE_PILOT_MEMBERS))
    started = time.monotonic()
    for index in indices:
        _measure_member(index, inputs)
    elapsed = time.monotonic() - started
    shared_bytes = _shared_input_bytes(inputs)
    remaining = SPEC_COHORT_SIZE - len(indices)
    projected_s = (
        elapsed + elapsed / len(indices) * remaining / max_workers * SYNTHETIC_CONTROL_PROJECTION_SAFETY_FACTOR
    )
    # ⚠ The pilot projects a fan-out it will never start, so it measures the
    # child and the post-shared parent peak exactly as the real launch does.
    projected_memory, per_worker_unique_bytes, per_worker_measured = _project_unique_memory_bytes(
        inputs,
        max_workers=max_workers,
        measure_child=True,
    )
    parent_peak_bytes = projected_memory - max_workers * per_worker_unique_bytes
    return LaunchPilotReport(
        member_indices=indices,
        placement_series=len(collector.placements),
        trades_per_member=collector.matchable_trade_count,
        shared_input_bytes=shared_bytes,
        pilot_wall_s=elapsed,
        seconds_per_member=elapsed / len(indices),
        projected_cohort_s=projected_s,
        projected_unique_memory_bytes=projected_memory,
        max_cohort_s=max_cohort_s,
        max_memory_bytes=max_memory_bytes,
        time_admitted=projected_s <= max_cohort_s,
        memory_admitted=projected_memory <= max_memory_bytes,
        parent_peak_bytes=parent_peak_bytes,
        per_worker_unique_bytes=per_worker_unique_bytes,
        per_worker_measured=per_worker_measured,
        projection_max_workers=max_workers,
    )


def run_worker_canary(
    collector: CohortCollector,
    *,
    axis: Sequence[date],
    benchmark: DatedEquityCurve | None = None,
    config: WorkerCanaryConfig | None = None,
) -> WorkerCanaryReport:
    """Measure fixed spawned pools and stop without constructing a control.

    The same member indices run under each worker count. Outcomes exist only
    long enough to prove execution equivalence; the returned report exposes no
    return, Sharpe, pass/fail, or other strategy result. Every pool is newly
    spawned so ``startup_and_transfer_s`` includes interpreter startup plus the
    one initializer transfer of the real collector payload.

    This function has no cohort-size argument and no continuation callback. It
    therefore cannot submit member 4 or transition into the production 1,000.
    """
    config = WorkerCanaryConfig() if config is None else config
    if not collector.placements:
        raise ValueError("worker canary requires at least one placeable series")
    indices = tuple(range(config.member_count))
    inputs = _MemberInputs(
        placements=tuple(collector.placements),
        axis=tuple(axis),
        benchmark=benchmark,
        expected_trade_count=collector.matchable_trade_count,
    )
    parent_peak_rss_bytes = _peak_rss_bytes()
    baseline: tuple[MemberOutcome, ...] | None = None
    trials: list[WorkerCanaryTrial] = []
    shared_started = time.monotonic()
    with _shared_member_inputs(inputs) as shared:
        shared_preparation_s = time.monotonic() - shared_started
        parent_peak_rss_bytes = max(parent_peak_rss_bytes, _peak_rss_bytes())
        for workers in config.worker_counts:
            context = get_context("spawn")
            barrier = context.Barrier(workers)
            pool_started_ns = time.monotonic_ns()
            with ProcessPoolExecutor(
                max_workers=workers,
                mp_context=context,
                initializer=_initialise_canary_worker,
                initargs=(shared, barrier),
            ) as pool:
                pending = {pool.submit(_measure_canary_member_in_worker, index): index for index in indices}
                samples_by_index: dict[int, _WorkerCanarySample] = {}
                try:
                    for future in as_completed(pending):
                        index = pending[future]
                        sample = future.result()
                        if sample.outcome.index != index:
                            raise RuntimeError(
                                f"worker canary task {index} returned member {sample.outcome.index}; identity moved"
                            )
                        samples_by_index[index] = sample
                except BaseException:
                    for future in pending:
                        future.cancel()
                    raise
            total_wall_s = (time.monotonic_ns() - pool_started_ns) / 1_000_000_000.0
            if set(samples_by_index) != set(indices):
                raise RuntimeError("worker canary returned an incomplete fixed member set")
            samples = tuple(samples_by_index[index] for index in indices)
            pids = {sample.pid for sample in samples}
            if len(pids) != workers:
                raise RuntimeError(f"worker canary requested {workers} spawned workers but observed {len(pids)}")

            outcomes = tuple(sample.outcome for sample in samples)
            if baseline is None:
                baseline = outcomes
            exact_equivalent = outcomes == baseline
            if not exact_equivalent:
                raise RuntimeError(f"worker canary changed a member outcome at {workers} workers")

            first_measure_ns = min(sample.measure_started_ns for sample in samples)
            final_measure_ns = max(
                sample.measure_started_ns + round(sample.measure_elapsed_s * 1_000_000_000) for sample in samples
            )
            member_wall_s = max((final_measure_ns - first_measure_ns) / 1_000_000_000.0, 1e-12)
            child_peak_by_pid = {
                pid: max(sample.peak_rss_bytes for sample in samples if sample.pid == pid) for pid in pids
            }
            max_child_peak_rss_bytes = max(child_peak_by_pid.values())
            aggregate_peak_rss_bytes = parent_peak_rss_bytes + sum(child_peak_by_pid.values())
            # RSS counts the same shared pages once per process. This upper bound
            # removes only those known duplicate mappings and retains every
            # private byte plus one full copy of the shared input.
            aggregate_unique_peak_bound_bytes = (
                parent_peak_rss_bytes
                + shared.shared_input_bytes
                + sum(max(peak - shared.shared_input_bytes, 0) for peak in child_peak_by_pid.values())
            )
            trial = WorkerCanaryTrial(
                workers=workers,
                member_count=config.member_count,
                distinct_worker_pids=len(pids),
                startup_and_transfer_s=max((first_measure_ns - pool_started_ns) / 1_000_000_000.0, 0.0),
                member_wall_s=member_wall_s,
                total_wall_s=total_wall_s,
                members_per_s=config.member_count / member_wall_s,
                parent_peak_rss_bytes=parent_peak_rss_bytes,
                max_child_peak_rss_bytes=max_child_peak_rss_bytes,
                aggregate_peak_rss_bytes=aggregate_peak_rss_bytes,
                aggregate_unique_peak_bound_bytes=aggregate_unique_peak_bound_bytes,
                exact_equivalent=True,
            )
            if aggregate_unique_peak_bound_bytes > config.max_aggregate_peak_rss_bytes:
                raise WorkerCanaryBudgetExceeded(
                    f"synthetic-control worker canary refused {workers} workers: aggregate unique-memory upper bound "
                    f"{aggregate_unique_peak_bound_bytes:,} bytes exceeds "
                    f"{config.max_aggregate_peak_rss_bytes:,} byte budget"
                )
            trials.append(trial)

    return WorkerCanaryReport(
        member_indices=indices,
        placement_series=len(collector.placements),
        trades_per_member=collector.matchable_trade_count,
        shared_input_bytes=shared.shared_input_bytes,
        shared_preparation_s=shared_preparation_s,
        trials=tuple(trials),
    )


def _run_members(
    inputs: _MemberInputs,
    *,
    cohort_size: int,
    max_workers: int,
    progress: CohortProgressCallback | None,
    scale_budget: SyntheticControlScaleBudget | None = None,
    label: str = "cohort",
) -> tuple[MemberOutcome, ...]:
    """Measure every member and return the canonical ``0..N-1`` ordering.

    Completion order is deliberately irrelevant to the estimator. Seeds are a
    pure function of the member index, outcomes are keyed by that index, and the
    complete exact index set is checked before aggregation. The callback reports
    completion counts only and therefore cannot leak performance mid-run.
    """

    completed = 0
    by_index: dict[int, MemberOutcome] = {}

    def accept(expected_index: int, outcome: MemberOutcome) -> None:
        nonlocal completed
        if outcome.index != expected_index:
            raise RuntimeError(
                f"cohort task {expected_index} returned member {outcome.index}; execution reordered identity"
            )
        if outcome.index in by_index:
            raise RuntimeError(f"cohort member {outcome.index} completed more than once")
        by_index[outcome.index] = outcome
        completed += 1
        if progress is not None:
            # Transient telemetry only: outcomes remain local and the callback
            # receives counts, never performance. Emit every completion because
            # a member can cross the stale threshold; the DB writer throttles.
            progress(completed, cohort_size)

    first_parallel_index = 0
    if scale_budget is not None:
        pilot_size = min(SYNTHETIC_CONTROL_SCALE_PILOT_MEMBERS, cohort_size)
        pilot_started = time.monotonic()
        for index in range(pilot_size):
            accept(index, _measure_member(index, inputs))
        pilot_elapsed = time.monotonic() - pilot_started
        remaining = cohort_size - pilot_size
        projected_s = pilot_elapsed + (
            pilot_elapsed / pilot_size * remaining / max_workers * SYNTHETIC_CONTROL_PROJECTION_SAFETY_FACTOR
        )
        projected_memory_bytes, per_worker_unique_bytes, _measured = _project_unique_memory_bytes(
            inputs,
            max_workers=max_workers,
            # No pool is spawned in the serial path, so there is no child to
            # measure and nothing to extrapolate from.
            measure_child=max_workers > 1 and pilot_size < cohort_size,
        )
        scale_budget.reserve(
            label=label,
            projected_s=projected_s,
            projected_memory_bytes=projected_memory_bytes,
        )
        logger.info(
            "synthetic-control scale gate admitted %s: %d-member pilot %.1fs, projected cohort %.1fs, "
            "projected memory %.2f GiB (%.0f MiB unique per worker, measured in a fresh child), "
            "cumulative projected run %.1fs",
            label,
            pilot_size,
            pilot_elapsed,
            projected_s,
            projected_memory_bytes / 1024**3,
            per_worker_unique_bytes / 1024**2,
            scale_budget.projected_run_s,
        )
        first_parallel_index = pilot_size

    if max_workers == 1:
        for index in range(first_parallel_index, cohort_size):
            accept(index, _measure_member(index, inputs))
    elif first_parallel_index < cohort_size:
        # ⚠ ``spawn`` rather than ``fork``. Production reaches this code from a
        # ThreadPoolExecutor in a process holding psycopg connections and job
        # locks; a fork would clone those unsafe resources into every child.
        with _shared_member_inputs(inputs) as shared:
            with ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=get_context("spawn"),
                initializer=_initialise_member_worker,
                initargs=(shared,),
            ) as pool:
                pending = {
                    pool.submit(_measure_member_in_worker, index): index
                    for index in range(first_parallel_index, cohort_size)
                }
                try:
                    for future in as_completed(pending):
                        accept(pending[future], future.result())
                except BaseException:
                    for future in pending:
                        future.cancel()
                    raise

    wanted = set(range(cohort_size))
    actual = set(by_index)
    if actual != wanted:
        missing = sorted(wanted - actual)
        extra = sorted(actual - wanted)
        raise RuntimeError(f"cohort member set is incomplete: missing={missing[:20]}, extra={extra[:20]}")
    return tuple(by_index[index] for index in range(cohort_size))


def _place_member(
    rng: np.random.Generator,
    placements: Sequence[SeriesPlacement],
    *,
    axis: Sequence[date],
) -> tuple[LegBook, array[float], list[date], list[date]]:
    """One member: the same holds, redrawn entry bars, everything else fixed."""
    book = LegBook()
    returns: array[float] = array("d")
    entry_dates: list[date] = []
    exit_dates: list[date] = []
    for placement in placements:
        entries, permuted = place_entries(rng, eligible=int(placement.panel.size), holds=placement.holds)
        entry_price = net_entry_prices(placement.adjusted_open[entries], np.full(entries.size, _HALF_SPREAD))
        exit_slot = entries + permuted
        exit_price = net_exit_prices(placement.adjusted_open[exit_slot], np.full(entries.size, _HALF_SPREAD))
        entry_panel = placement.panel[entries]
        exit_panel = placement.panel[exit_slot]
        mark_base = -placement.marks_first
        for leg in range(int(placement.holds.size)):
            entry_index = int(entry_panel[leg])
            exit_index = int(exit_panel[leg])
            book.add(
                entry_index=entry_index,
                exit_index=exit_index,
                entry_price=float(entry_price[leg]),
                exit_price=float(exit_price[leg]),
                half_spread=_HALF_SPREAD,
                realised=True,
                marks=placement.marks[mark_base + entry_index : mark_base + exit_index + 1].tolist(),
            )
            returns.append(float((exit_price[leg] - entry_price[leg]) / entry_price[leg] * 100.0))
            entry_dates.append(axis[entry_index])
            # #2623 gap 1 — the permuted exit bar, already in scope above.
            exit_dates.append(axis[exit_index])
    return book, returns, entry_dates, exit_dates


def _place_member_compact(
    rng: np.random.Generator,
    placements: Sequence[SeriesPlacement],
    *,
    axis: Sequence[date],
) -> tuple[SharedMarkLegBook, array[float], list[date], list[date]]:
    """Place one member without copying a mark span for every selected leg.

    ``_place_member`` remains the deliberately slow reference used by the
    metric-axis legacy arm and differential tests.  This path changes only the
    storage layout: entries, exits, prices and member-index seed are identical,
    while marks point back to the collector's immutable per-series arrays.
    """
    size = sum(int(placement.holds.size) for placement in placements)
    entry_index = np.empty(size, dtype=np.int64)
    exit_index = np.empty(size, dtype=np.int64)
    entry_prices = np.empty(size, dtype=np.float64)
    exit_prices = np.empty(size, dtype=np.float64)
    mark_source = np.empty(size, dtype=np.int32)
    returns: array[float] = array("d")
    entry_dates: list[date] = []
    exit_dates: list[date] = []
    cursor = 0
    for source, placement in enumerate(placements):
        entries, permuted = place_entries(rng, eligible=int(placement.panel.size), holds=placement.holds)
        placed = int(entries.size)
        end = cursor + placed
        net_entries = net_entry_prices(placement.adjusted_open[entries], np.full(placed, _HALF_SPREAD))
        exit_slot = entries + permuted
        net_exits = net_exit_prices(placement.adjusted_open[exit_slot], np.full(placed, _HALF_SPREAD))
        entry_panel = placement.panel[entries]
        exit_panel = placement.panel[exit_slot]
        entry_index[cursor:end] = entry_panel
        exit_index[cursor:end] = exit_panel
        entry_prices[cursor:end] = net_entries
        exit_prices[cursor:end] = net_exits
        mark_source[cursor:end] = source
        returns.extend(((net_exits - net_entries) / net_entries * 100.0).tolist())
        entry_dates.extend(axis[int(index)] for index in entry_panel)
        exit_dates.extend(axis[int(index)] for index in exit_panel)
        cursor = end
    if cursor != size:
        raise RuntimeError(f"compact member placed {cursor:,} legs against its preallocated {size:,}")
    return (
        SharedMarkLegBook(
            entry_index=entry_index,
            exit_index=exit_index,
            entry_price=entry_prices,
            exit_price=exit_prices,
            half_spread=np.full(size, _HALF_SPREAD, dtype=np.float64),
            realised=np.ones(size, dtype=np.bool_),
            mark_source=mark_source,
            marks_by_source=tuple(placement.marks for placement in placements),
            marks_first_by_source=np.asarray([placement.marks_first for placement in placements], dtype=np.int64),
        ),
        returns,
        entry_dates,
        exit_dates,
    )


__all__ = [
    "CONTROL_NAMESPACE",
    "HOLDOUT_CONTROL_REASON",
    "PLACEMENT_SPACE_ID",
    "SYNTHETIC_CONTROL_MAX_WORKERS",
    "SYNTHETIC_CONTROL_MAX_PROJECTED_COHORT_S",
    "SYNTHETIC_CONTROL_MAX_PROJECTED_MEMORY_BYTES",
    "SYNTHETIC_CONTROL_MAX_PROJECTED_RUN_S",
    "SYNTHETIC_CONTROL_SCALE_PILOT_MEMBERS",
    "SYNTHETIC_CONTROL_WORKER_CANARY_COUNTS",
    "SYNTHETIC_CONTROL_WORKER_CANARY_MEMBERS",
    "CohortCollector",
    "CohortProgressCallback",
    "CohortResult",
    "LaunchPilotReport",
    "SeriesPlacement",
    "ScaleBudgetExceeded",
    "SyntheticControlScaleBudget",
    "WorkerCanaryBudgetExceeded",
    "WorkerCanaryConfig",
    "WorkerCanaryReport",
    "WorkerCanaryTrial",
    "run_cohort",
    "run_launch_pilot",
    "run_worker_canary",
]
