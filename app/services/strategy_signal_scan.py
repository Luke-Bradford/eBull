"""§3.1 — the daily signal scan.

Spec: ``docs/proposals/ta/2026-08-08-strategy-signal-scan.md`` (which settles §4
of ``2026-08-08-strategy-runner-and-manifest.md``). Manifest:
``app/services/strategy_manifest.py``. Writer: ``app/services/signal_ledger.py``.
Table: ``sql/255_strategy_signals.sql``. Watermark:
``sql/272_strategy_scan_watermark.sql``. Refs #2240, #2394.

⚠⚠ EVERY LOGICAL ROW THIS JOB WRITES IS TERMINAL, AND THAT IS THE CONSTRAINT THE
WHOLE SHAPE FOLLOWS FROM. Fired detail, retained negative detail and the daily
census all omit ``ON CONFLICT`` — a re-run cannot overwrite a recorded decision,
only a version bump can supersede it. Four consequences, each measured
rather than argued (``scripts/verify_2394_signal_scan_cost.py``, full population,
2026-08-08):

1. **The scan runs in ARREARS.** A signal on the final bar of a series has no
   ``t+1``, so ``evaluate`` stamps it ``not_evaluable`` / ``no_fill_bar``. A
   same-day scan would record **6,185 real fired decisions per day** as
   permanently unevaluable. The write date is the bar *before* the frontier.
2. **The frontier is the MODAL last bar, never ``max(price_date)``.** On the day
   of measurement 7 instruments carried a bar the other 5,783 did not; keying on
   the maximum manufactures thousands of terminal refusals out of a refresh still
   in flight.
3. **No trailing-window recompute.** ``rsi_series`` and ``atr_series`` are
   Wilder-recursive from the series start, so a K-bar window is a *different
   function* and ``StrategyIdentity`` records no K. The measured 0/3290 and
   0/2033 disagreement at K=250/750 is a negative result about this corpus's
   depth, not a licence.
4. **Signals only.** Outcome resolution remains a separate scheduled job. It
   consumes only level-based entries with a manifest-owned ``ExitLevels``
   factory and deliberately leaves an immature window pending: storing it would
   drop that signal out of ``select_pending_fills`` **permanently**.

⚠ IT DOES NOT BACKFILL, AND THE COLD-START RULE IS WHERE THAT IS ENFORCED. See
``write_window_indices``: with no watermark the scan writes the single
frontier-minus-one bar, not the whole history. *"Deriving yesterday's signals
from today's stored bars would reintroduce the look-ahead phase 5 spent itself
removing."*
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

import psycopg

from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries, Universe
from app.services.market_context import MarketContext, MarketContextUnavailable, load_market_context
from app.services.price_masked_bars import (
    MASKED_REASON,
    load_bar_spans,
    load_masked_bars,
    load_union_calendar,
)
from app.services.price_segments import load_unresolved_breaks
from app.services.signal_ledger import LedgerRow, resolve_fills
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyEntry
from app.services.strategy_observation_storage import store_strategy_observations
from app.services.strategy_registry import (
    SignalKind,
    StrategyIdentity,
    StrategySignal,
    Verdict,
)
from app.services.strategy_segmented_evaluation import segmented_member, segmented_signals

logger = logging.getLogger(__name__)

#: Spec §10. ``load_validated_universe`` resolves TODAY's membership, and
#: ``instrument_universe_membership`` (#2290) is live but empty by design until
#: the next ``nightly_universe_sync``, so it cannot answer "was X tradable on
#: date D" for any past date yet. Every row therefore carries the honest label.
#:
#: ⚠ The switch to a point-in-time population is a NEW ``universe`` value, hence
#: a new ``strategy_version``, hence a new track record beside the old one — not
#: an in-place correction. Stated so that nobody later treats it as a bug fix.
#: ``load_validated_universe``'s own docstring: *"Do not quietly drop the filter
#: to make a number look better — that would widen the population without
#: changing the label, which is worse than the bias."*
SCAN_UNIVERSE: Universe = "survivor_only"

#: Spec §3's completeness floor: the modal last-bar date must be held by at least
#: this share of the loadable universe or the scan refuses the day.
#:
#: ⚠ THERE IS NO PUBLISHED RULE FOR THIS AND SAYING SO IS THE HONEST FORM. It is
#: fixed **by construction** and frozen here (the `.claude/CLAUDE.md` rule for
#: exactly this case). It is a JOB parameter, not a strategy parameter — it
#: selects *when* to evaluate and never *what* the verdict is — so it is
#: deliberately outside ``StrategyIdentity`` and criterion 11 does not reach it.
#:
#: ⚠ Held as a ratio of INTEGERS and compared by cross-multiplication below. A
#: float ``2/3`` makes the boundary case a question about binary representation,
#: and the boundary is the one case a floor exists for. Measured on the day the
#: spec was written the modal share was 5,783/6,547 = 88.3%, comfortably clear.
FRONTIER_MODAL_SHARE_FLOOR = (2, 3)

#: Refusing is safe in a way that scanning is not: a skipped day can be picked up
#: tomorrow, a written row cannot be withdrawn.
ScanStatus = Literal[
    "scanned",
    "up_to_date",
    "refused_thin_frontier",
    "refused_empty_universe",
]

StrategyScanStatus = Literal[
    "written",
    "up_to_date",
    "refused_frontier_regressed",
    #: ⚠ A regime-gated strategy whose benchmark could not be built (#2437). It
    #: is a NAMED refusal and not a silent omission, and it is not "failed":
    #: nothing went wrong with the strategy, its market context is missing. The
    #: distinction is what tells an operator to fix the benchmark series rather
    #: than to debug the rule.
    "refused_no_market_context",
    "failed",
]

#: (signal_kind, verdict, not_evaluable_reason or "") -> count.
CensusKey = tuple[SignalKind, Verdict, str]


@dataclass(frozen=True)
class Frontier:
    """The corpus date the scan evaluates against, and how complete it is."""

    bar_date: date
    #: Instruments whose last loadable bar IS ``bar_date``.
    modal_count: int
    #: Instruments the masked loader would return at least one bar for.
    loadable: int

    @property
    def meets_floor(self) -> bool:
        numerator, denominator = FRONTIER_MODAL_SHARE_FLOOR
        return self.modal_count * denominator >= self.loadable * numerator

    @property
    def share_pct(self) -> float:
        """For the operator line only — never for the gate. See ``meets_floor``."""
        return 100.0 * self.modal_count / self.loadable if self.loadable else 0.0


@dataclass(frozen=True)
class StrategyScanResult:
    """What the scan did for one ``(strategy_id, strategy_version)``."""

    strategy_id: str
    strategy_version: str
    status: StrategyScanStatus
    resumed_from: date | None
    rows_written: int = 0
    durable_signal_rows: int = 0
    retained_observation_rows: int = 0
    aggregate_rows: int = 0
    storage_input_bytes: int = 0
    #: Expected rows per leg — the sum of the per-instrument windows the census
    #: is checked against. Spec §9: *"a mismatch is a failure, not a log line"*.
    expected_per_leg: int = 0
    #: Eligible instruments the loader returned a usable series for. ⚠ Checked
    #: against the eligible count, not against ``expected_per_leg``, which is
    #: derived from the same windows and so cannot detect a missing one. NOT the
    #: number that produced a row — an empty window is legitimate.
    instruments_evaluated: int = 0
    census: Mapping[CensusKey, int] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class ScanReport:
    """One run's whole account of itself — spec §9's observability contract.

    ⚠ A strategy-date with zero rows must be VISIBLE, and no index can make it
    so: an absent row indexes to nothing. Zero coverage is only detectable
    against an *expected* count, which is why ``expected_per_leg`` travels beside
    the census rather than being recomputed by a reader.
    """

    status: ScanStatus
    frontier: Frontier | None = None
    universe_size: int = 0
    #: In the validated universe, no bars through the masked loader.
    excluded_no_bars: int = 0
    #: Loadable, but the last bar is not the frontier.
    excluded_stale_series: int = 0
    #: Loadable and at the frontier, but fewer than two bars — no write date.
    excluded_short_series: int = 0
    #: Loaded to fewer than two bars despite the span query saying otherwise —
    #: the series vanished between the two reads. ⚠ A series that merely GREW or
    #: SHRANK is not counted here and is not skipped: the window is bounded on
    #: the frontier, so it is written correctly. Only a vanished one cannot be,
    #: and it fails the strategy through the coverage gate rather than leaving a
    #: hole no later run can reach.
    excluded_moved_mid_scan: int = 0
    eligible_instruments: int = 0
    per_strategy: tuple[StrategyScanResult, ...] = ()

    @property
    def rows_written(self) -> int:
        return sum(result.rows_written for result in self.per_strategy)


# ---------------------------------------------------------------------------
# The two decisions, as pure functions
# ---------------------------------------------------------------------------


def choose_frontier(last_bars: Mapping[int, date]) -> Frontier | None:
    """The modal last bar across the loadable population, ties broken LATER.

    ⚠ NOT ``max``. Spec §3: on the day this was measured 7 instruments carried a
    bar at ``2026-08-08`` and 5,783 did not. A scan keyed on the maximum
    evaluates a date most of the universe is missing and manufactures thousands
    of refusals out of a refresh still in flight — every storage tier's missing
    ``ON CONFLICT`` makes those refusals terminal.

    Ties break on the later date so that a corpus split evenly between two
    consecutive sessions advances rather than stalls.
    """
    if not last_bars:
        return None
    counts = Counter(last_bars.values())
    bar_date, modal_count = max(counts.items(), key=lambda item: (item[1], item[0]))
    return Frontier(bar_date=bar_date, modal_count=modal_count, loadable=len(last_bars))


def write_window_indices(dates: Sequence[date], *, watermark: date | None, frontier: date) -> range:
    """The bar indices this run may write for one instrument.

    Spec §3.1: *"the scan writes, for every eligible instrument, each of its own
    bars strictly **after** the watermark and strictly **before** that
    instrument's last bar."* Both bounds are load-bearing:

    - **strictly before the last bar** is §2's arrears rule. The last bar has no
      ``t+1``, so a decision there is ``no_fill_bar`` and unrewritable.
    - **at or after the watermark** is what makes a re-run a no-op without an
      ``ON CONFLICT``, and what lets a straggler that missed sessions catch up:
      it gets every unwritten bar in the window, not just the newest, so a gap
      does not silently drop a day of its record.

    ⚠⚠ ``>=``, NOT ``>``, AND SPEC §3.1 SAYS *"strictly after the watermark"* —
    WHICH IS AN OFF-BY-ONE IN THE SPEC. The two bounds are measured against
    different things: the watermark names the **frontier** of the last completed
    run, and that run wrote bars strictly *before* its frontier. So the frontier
    bar itself is the first one still owed, and ``>`` skips it. Concretely, with
    a frontier moving F1 → F2 on a series ``…, F0, F1, F2``: ``>`` gives bars in
    ``(F1, F2)``, which is EMPTY, so every run after the first would write
    nothing at all. ``>=`` gives ``{F1}``, the bar before the new frontier, which
    is what §2's arrears rule asks for.

    ⚠ It cannot double-write. The previous run covered ``[its lower bound, F1)``
    and this one covers ``[F1, F2)`` — abutting, disjoint, and the same-frontier
    case never reaches here because ``run_signal_scan`` short-circuits on
    ``watermark == frontier``.

    ⚠⚠ THE UPPER BOUND IS THE **FRONTIER**, NOT THE SERIES END, AND THE
    DIFFERENCE IS A PERMANENT DATA LOSS (Codex, checkpoint 2). The two agree for
    every instrument the scan selected — eligibility *is* "last bar equals the
    frontier". They diverge only when ``daily_candle_refresh`` writes a new bar
    between the span query and this instrument's load. The first version skipped
    that instrument and let the watermark advance to the old frontier anyway, so
    the bar it should have written was then *behind* the watermark and no later
    run could ever reach it. Bounding on the frontier writes it correctly instead
    and leaves the newly arrived bar for tomorrow, which is what the arrears rule
    would have done had the refresh landed a second earlier.

    ⚠ The ``n - 1`` cap stays, for the opposite movement: an instrument that LOST
    bars mid-scan has a last bar behind the frontier, and writing a decision
    there would be the ``no_fill_bar`` refusal the arrears rule exists to avoid.
    The bound is the tighter of the two, always.

    ⚠⚠ THE COLD START IS BOUNDED, AND THAT IS NOT AN OPTIMISATION. With no
    watermark the "strictly after" bound is vacuous and the window would be the
    instrument's ENTIRE history — a backfill, which spec §11 forbids outright:
    *"Signals are a function of what was known on the day. Deriving yesterday's
    signals from today's stored bars would reintroduce the look-ahead phase 5
    spent itself removing."* So a first run writes exactly the one bar before the
    frontier, and every run after it resumes from the recorded frontier.
    """
    n = len(dates)
    if n < 2:
        return range(0)
    # Bars are strictly ascending (BarSeries enforces it), so both bounds are
    # scans from the end — cheap, since the window is one bar on any run that is
    # not catching a gap up.
    end = n - 1
    while end > 0 and dates[end - 1] >= frontier:
        end -= 1
    if end == 0:
        return range(0)
    if watermark is None:
        return range(end - 1, end)
    start = end
    while start > 0 and dates[start - 1] >= watermark:
        start -= 1
    return range(start, end)


# ---------------------------------------------------------------------------
# Watermark storage
# ---------------------------------------------------------------------------


_READ_WATERMARKS = """
    SELECT strategy_id, strategy_version, frontier_date
    FROM strategy_scan_watermark
"""

#: ⚠ The ``WHERE`` clause is a monotonicity BACKSTOP, not the gate. The gate is
#: in ``run_signal_scan``, which refuses a frontier at or behind the watermark
#: before doing any work. If this clause ever suppresses an update the caller
#: asserts on the rowcount and raises — a watermark that silently declined to
#: move would make the next run rewrite rows that already exist, and under a key
#: with no ``ON CONFLICT`` that is an aborted batch.
_ADVANCE_WATERMARK = """
    INSERT INTO strategy_scan_watermark (strategy_id, strategy_version, frontier_date, updated_at)
    VALUES (%(strategy_id)s, %(strategy_version)s, %(frontier_date)s, now())
    ON CONFLICT (strategy_id, strategy_version) DO UPDATE
       SET frontier_date = EXCLUDED.frontier_date,
           updated_at = now()
     WHERE EXCLUDED.frontier_date > strategy_scan_watermark.frontier_date
"""


def read_watermarks(conn: psycopg.Connection[Any]) -> dict[tuple[str, str], date]:
    """Every recorded resume point, keyed ``(strategy_id, strategy_version)``."""
    rows = conn.execute(_READ_WATERMARKS).fetchall()
    return {(str(strategy_id), str(version)): frontier for strategy_id, version, frontier in rows}


def advance_watermark(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    frontier_date: date,
) -> None:
    """Record that this identity has completed ``frontier_date``.

    ⚠ A run that completes with ZERO rows still advances it; a run that raises
    does not. Spec §3.1: *"'Wrote nothing' and 'failed' must remain
    distinguishable, and the watermark alone cannot carry both."* The
    distinction is preserved by calling this inside the same transaction as the
    insert, so a failure rolls both back together.
    """
    with conn.cursor() as cur:
        cur.execute(
            _ADVANCE_WATERMARK,
            {
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "frontier_date": frontier_date,
            },
        )
        moved = cur.rowcount
    if moved != 1:
        raise RuntimeError(
            f"watermark for {strategy_id}/{strategy_version} did not advance to {frontier_date} "
            f"(rowcount {moved}) — a stored frontier at or ahead of it means the run just wrote rows "
            "that a later run would try to write again, and strategy_signals has no ON CONFLICT"
        )


# ---------------------------------------------------------------------------
# The scan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Plan:
    """One strategy that has work to do on this frontier."""

    entry: StrategyEntry
    identity: StrategyIdentity
    version: str
    watermark: date | None


@dataclass
class _PendingMember:
    """One cross-sectional member, held until the whole cross-section is known.

    ⚠⚠ ``series`` IS A TRIMMED SLICE — the window bars plus the ONE bar after
    them — and that is what keeps ``signal_ledger.resolve_fills`` the only code
    that touches bar ``t+1``. S-2's verdict at a decision bar cannot be decided
    until every member has been staged, by which point streaming has moved off
    this instrument; holding the whole series for 6,547 members instead would be
    the full-corpus materialisation the spec calls unsafe.

    The slice is safe because the window is a contiguous suffix ending one bar
    before the series ends, so (a) every window bar still has its true ``t+1``
    inside the slice, and (b) no window bar is the slice's last bar, which is the
    one ``resolve_fills`` stamps ``no_fill_bar``. Indices re-base to ``0..n-2``
    of the slice, checked against the dates below rather than assumed.
    """

    series: BarSeries
    #: Window bar dates, ascending. Position ``j`` here is index ``j`` in
    #: ``series`` — asserted at build time, not trusted.
    window_dates: tuple[date, ...]
    #: Verdicts already decidable without the cross-section, by bar date.
    decided: dict[date, StrategySignal]
    #: Window dates at which this member ranks.
    participating: frozenset[date]


def run_signal_scan(
    conn: psycopg.Connection[Any],
    *,
    manifest: Mapping[str, StrategyEntry] = STRATEGY_MANIFEST,
) -> ScanReport:
    """Evaluate every manifest strategy at the arrears bar and write the ledger.

    One transaction per ``(strategy_id, frontier)`` — the unit the watermark
    names. A strategy's run is all-or-nothing and one strategy's failure does not
    stop the others: a half-written batch under a no-``ON CONFLICT`` key is
    unrecoverable without a delete, so the batch boundary and the watermark unit
    have to be the same thing.

    ⚠ Concurrency is NOT handled here. The caller takes
    ``app/jobs/locks.py::JobLock`` on its own connection — uniqueness catches a
    duplicate only *after* the work is done, and under a raising key that is an
    aborted batch rather than a no-op. Taking the lock inside this connection
    would hold it until the last strategy committed (prevention log:
    *"``pg_advisory_xact_lock`` acquired in a savepoint is absorbed by the parent
    and held until the TOP-LEVEL transaction commits"*).

    ⚠⚠ THE CONNECTION MUST BE ``autocommit=True``, AND THAT IS CHECKED. On a
    non-autocommit connection the reads above open an implicit transaction, so
    ``conn.transaction()`` below yields a **SAVEPOINT** rather than a
    transaction, and every strategy's batch would then commit together when the
    caller's connection block exits. The per-strategy boundary is not a
    preference — it is what makes a half-written batch under a key with no
    ``ON CONFLICT`` impossible (prevention log: *"psycopg3 savepoint ≠ commit"*).
    """
    if not conn.autocommit:
        raise ValueError(
            "run_signal_scan needs an autocommit connection: inside an open transaction "
            "conn.transaction() is a SAVEPOINT, so the per-strategy commit boundary the ledger "
            "depends on would collapse into one batch"
        )
    universe = load_validated_universe(conn)
    # ⚠ Built ONCE, before the instrument loop, and never per instrument. It is
    # one classification of one benchmark shared by every gated strategy and
    # every name they judge — rebuilding it inside the loop would reload and
    # re-classify SPY 6,774 times to get the same answer.
    #
    # ⚠ A failure here does NOT stop the scan. S-1…S-4 read only their own bars
    # and must still run; only the gated strategies are refused, by name, below.
    market: MarketContext | None
    market_context_error: str | None
    try:
        market = load_market_context(conn, universe=SCAN_UNIVERSE)
        market_context_error = None
    except MarketContextUnavailable as exc:
        market, market_context_error = None, str(exc)
        logger.warning("strategy_signal_scan: no market context — regime-gated strategies refused: %s", exc)
    spans = load_bar_spans(conn, universe)
    frontier = choose_frontier({instrument_id: span.last_bar for instrument_id, span in spans.items()})
    if frontier is None:
        logger.warning("strategy_signal_scan: no loadable instruments in a %d-member universe", len(universe))
        return ScanReport(status="refused_empty_universe", universe_size=len(universe))

    if not frontier.meets_floor:
        numerator, denominator = FRONTIER_MODAL_SHARE_FLOOR
        logger.warning(
            "strategy_signal_scan: REFUSING %s — modal share %d/%d (%.1f%%) is below the %d/%d floor; "
            "a refresh still in flight would be recorded as terminal refusals",
            frontier.bar_date,
            frontier.modal_count,
            frontier.loadable,
            frontier.share_pct,
            numerator,
            denominator,
        )
        return ScanReport(
            status="refused_thin_frontier",
            frontier=frontier,
            universe_size=len(universe),
            excluded_no_bars=len(universe) - len(spans),
        )

    eligible = sorted(
        instrument_id for instrument_id, span in spans.items() if span.last_bar == frontier.bar_date and span.bars >= 2
    )
    unresolved_breaks = load_unresolved_breaks(conn, eligible)
    at_frontier = sum(1 for span in spans.values() if span.last_bar == frontier.bar_date)

    watermarks = read_watermarks(conn)
    plans: list[_Plan] = []
    results: list[StrategyScanResult] = []
    for strategy_id in sorted(manifest):
        entry = manifest[strategy_id]
        identity = entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID)
        version = identity.version
        watermark = watermarks.get((strategy_id, version))
        if watermark is not None and watermark >= frontier.bar_date:
            regressed = watermark > frontier.bar_date
            if regressed:
                # ⚠ NOT the same event as "already done", and it must not be
                # logged as one. The corpus went BACKWARDS under a completed
                # scan — a rewash, a restore, a rule-set version bump that
                # emptied the coverage table. Writing again would either
                # duplicate a recorded decision or collide on a key with no
                # ``ON CONFLICT``, so the scan declines and says why.
                logger.warning(
                    "strategy_signal_scan: %s watermark %s is AHEAD of frontier %s — the corpus regressed; "
                    "declining to write",
                    strategy_id,
                    watermark,
                    frontier.bar_date,
                )
            results.append(
                StrategyScanResult(
                    strategy_id=strategy_id,
                    strategy_version=version,
                    status="refused_frontier_regressed" if regressed else "up_to_date",
                    resumed_from=watermark,
                )
            )
            continue
        if entry.requires_market_context and market is None:
            results.append(
                StrategyScanResult(
                    strategy_id=strategy_id,
                    strategy_version=version,
                    status="refused_no_market_context",
                    resumed_from=watermark,
                    error=market_context_error,
                )
            )
            continue
        plans.append(_Plan(entry=entry, identity=identity, version=version, watermark=watermark))

    if not plans:
        logger.info(
            "strategy_signal_scan: frontier %s already covered by every strategy — nothing to do",
            frontier.bar_date,
        )
        return ScanReport(
            status="up_to_date",
            frontier=frontier,
            universe_size=len(universe),
            excluded_no_bars=len(universe) - len(spans),
            excluded_stale_series=len(spans) - at_frontier,
            excluded_short_series=at_frontier - len(eligible),
            eligible_instruments=len(eligible),
            per_strategy=tuple(results),
        )

    panel_dates: frozenset[date] | None = None
    cross_sectional = [plan for plan in plans if plan.entry.strategy_class == "cross_sectional"]
    if cross_sectional:
        # ⚠ The union calendar spans every LOADABLE instrument, not only the
        # frontier-eligible ones: a name that stopped trading last year still
        # contributed the sessions on which the panel rebalanced.
        calendar = load_union_calendar(conn, sorted(spans))
        panel_dates = cross_sectional[0].entry.decision_calendar(calendar)
        if panel_dates is None:  # pragma: no cover — the manifest guarantees one
            raise RuntimeError(
                f"{cross_sectional[0].entry.strategy_id} is cross_sectional but returned no decision calendar"
            )

    rows: dict[str, list[LedgerRow]] = {plan.entry.strategy_id: [] for plan in plans}
    expected: dict[str, int] = {plan.entry.strategy_id: 0 for plan in plans}
    pending: dict[str, dict[int, _PendingMember]] = {plan.entry.strategy_id: {} for plan in cross_sectional}
    scores: dict[str, dict[date, dict[int, float]]] = {plan.entry.strategy_id: {} for plan in cross_sectional}
    moved_mid_scan = 0
    evaluated = 0

    for instrument_id in eligible:
        series = load_masked_bars(conn, instrument_id).series
        # ⚠ The span query and this load are two reads of a corpus
        # ``daily_candle_refresh`` may be writing, so an instrument's series can
        # move between them. A series that GREW or SHRANK is still evaluated —
        # ``write_window_indices`` bounds on the frontier, so it writes exactly
        # the bars this frontier owes and leaves any newer one for tomorrow. Only
        # a series that vanished entirely cannot be evaluated, and that is
        # counted here and then caught by the coverage gate rather than skipped
        # quietly: an eligible instrument with no row is a permanent hole,
        # because the watermark would move past the bar it never wrote.
        if len(series) < 2:
            moved_mid_scan += 1
            continue
        evaluated += 1

        for plan in plans:
            window = write_window_indices(series.dates, watermark=plan.watermark, frontier=frontier.bar_date)
            if not window:
                continue
            expected[plan.entry.strategy_id] += len(window)
            if plan.entry.strategy_class == "per_series":
                _scan_per_series(
                    plan,
                    series,
                    instrument_id,
                    window,
                    rows[plan.entry.strategy_id],
                    unresolved_breaks=unresolved_breaks.get(instrument_id, ()),
                    market=market,
                )
            else:
                assert panel_dates is not None
                _stage_cross_sectional(
                    plan,
                    series,
                    instrument_id,
                    window,
                    panel_dates=panel_dates,
                    pending=pending[plan.entry.strategy_id],
                    scores=scores[plan.entry.strategy_id],
                    unresolved_breaks=unresolved_breaks.get(instrument_id, ()),
                )

    for plan in cross_sectional:
        _resolve_cross_section(
            plan,
            pending=pending[plan.entry.strategy_id],
            scores=scores[plan.entry.strategy_id],
            out=rows[plan.entry.strategy_id],
        )

    for plan in plans:
        strategy_id = plan.entry.strategy_id
        results.append(
            _commit_strategy(
                conn,
                plan,
                rows[strategy_id],
                expected_per_leg=expected[strategy_id],
                instruments_evaluated=evaluated,
                eligible_instruments=len(eligible),
                frontier=frontier,
            )
        )

    report = ScanReport(
        status="scanned",
        frontier=frontier,
        universe_size=len(universe),
        excluded_no_bars=len(universe) - len(spans),
        excluded_stale_series=len(spans) - at_frontier,
        excluded_short_series=at_frontier - len(eligible),
        excluded_moved_mid_scan=moved_mid_scan,
        eligible_instruments=len(eligible),
        per_strategy=tuple(sorted(results, key=lambda result: result.strategy_id)),
    )
    log_report(report)
    return report


def _scan_per_series(
    plan: _Plan,
    series: BarSeries,
    instrument_id: int,
    window: range,
    out: list[LedgerRow],
    *,
    unresolved_breaks: Sequence[date] = (),
    market: MarketContext | None = None,
) -> None:
    """Full-history recompute, filtered to the window, resolved through the writer.

    ⚠ The strategy sees the WHOLE series and the filter is applied to its output.
    Spec §5: a trailing-window recompute is a different function, not a cheaper
    one, and ``StrategyIdentity`` records no window — so two runs at different K
    would be indistinguishable once stored.

    ⚠ The filter is applied BEFORE ``resolve_fills`` and not after. Resolving the
    whole series and discarding it would build ~1,000 validated ``LedgerRow``
    objects per instrument per strategy to keep one, which is 20M objects a run.
    Correctness is unaffected: ``resolve_fills`` indexes ``series`` by
    ``signal_index``, so a subset of signals against the full series resolves
    each fill from the same bar it always would.
    """
    signals = segmented_signals(
        plan.entry,
        series,
        universe=SCAN_UNIVERSE,
        masked_reason=MASKED_REASON,
        unresolved_breaks=unresolved_breaks,
        market=market,
    )
    windowed = [signal for signal in signals if signal.signal_index in window]
    out.extend(resolve_fills(windowed, series=series, identity=plan.identity, instrument_id=instrument_id))


def _stage_cross_sectional(
    plan: _Plan,
    series: BarSeries,
    instrument_id: int,
    window: range,
    *,
    panel_dates: frozenset[date],
    pending: dict[int, _PendingMember],
    scores: dict[date, dict[int, float]],
    unresolved_breaks: Sequence[date] = (),
) -> None:
    """Everything decidable about one member without seeing the others.

    ⚠ ``stage_cross_sectional_member`` is public precisely for this: *"A
    full-corpus census cannot hold every member's bars in memory at once, so it
    stages one series at a time and keeps only ``scores``. Without this split it
    would re-implement the staging pass, which is how a census and the strategy
    it measures come to disagree."*
    """
    staged = segmented_member(
        plan.entry,
        series,
        panel_decision_dates=panel_dates,
        universe=SCAN_UNIVERSE,
        masked_reason=MASKED_REASON,
        unresolved_breaks=unresolved_breaks,
    )

    start = window.start
    trimmed = BarSeries(dates=series.dates[start:], rows=series.rows[start:])
    window_dates = tuple(series.dates[index] for index in window)
    decided: dict[date, StrategySignal] = {}
    participating: set[date] = set()
    for offset, index in enumerate(window):
        when = series.dates[index]
        # ⚠ The re-basing the slice depends on, checked rather than trusted — and
        # NOT an `assert`, which `python -O` deletes. This is the one guard on the
        # off-by-one the slice exists to risk, so it must survive optimisation.
        if trimmed.dates[offset] != when:
            raise RuntimeError(
                f"cross-sectional slice re-based wrongly for instrument {instrument_id}: "
                f"offset {offset} is {trimmed.dates[offset]}, expected {when}"
            )
        verdict = staged.verdicts[index]
        if verdict is None:
            participating.add(when)
            scores.setdefault(when, {})[instrument_id] = staged.scores[when]
        else:
            decided[when] = verdict
    pending[instrument_id] = _PendingMember(
        series=trimmed,
        window_dates=window_dates,
        decided=decided,
        participating=frozenset(participating),
    )


def _resolve_cross_section(
    plan: _Plan,
    *,
    pending: Mapping[int, _PendingMember],
    scores: Mapping[date, Mapping[int, float]],
    out: list[LedgerRow],
) -> None:
    """Rank each window date's cross-section, then write every member's verdict.

    ⚠ ``min_participants`` is the RUNNER's call, mirroring
    ``evaluate_cross_sectional``: below it every participant is
    ``not_evaluable("thin_cross_section")``, because an empty return from
    ``select`` cannot be told apart from "the panel was too thin", and criterion
    8 exists to keep exactly that distinction countable.

    ⚠ ``select`` is handed the WINDOW date, not the frontier. Today's selector
    ignores ``when``, but the ``CrossSectionalSelect`` contract exists so a
    date-aware one needs no signature change — and passing the wrong date would
    be silent until one arrives.
    """
    assert plan.entry.select is not None and plan.entry.min_participants is not None
    winners: dict[date, frozenset[int]] = {}
    thin: set[date] = set()
    for when in sorted(scores):
        at_date = scores[when]
        if len(at_date) < plan.entry.min_participants:
            thin.add(when)
            continue
        selected = frozenset(plan.entry.select(when, at_date))
        unknown = selected - at_date.keys()
        if unknown:
            raise ValueError(
                f"{plan.entry.strategy_id} select returned {sorted(unknown)} on {when}, which did not "
                "participate in that cross-section — every winner must be one of the members offered"
            )
        winners[when] = selected

    for instrument_id, member in sorted(pending.items()):
        signals: list[StrategySignal] = []
        for offset, when in enumerate(member.window_dates):
            decided = member.decided.get(when)
            if decided is not None:
                signals.append(
                    StrategySignal(
                        verdict=decided.verdict,
                        signal_index=offset,
                        kind=decided.kind,
                        reason=decided.reason,
                    )
                )
                continue
            if when in thin:
                signals.append(
                    StrategySignal(
                        verdict="not_evaluable",
                        signal_index=offset,
                        kind="entry",
                        reason="thin_cross_section",
                    )
                )
                continue
            fired = instrument_id in winners[when]
            signals.append(StrategySignal(verdict="fired" if fired else "not_fired", signal_index=offset, kind="entry"))
        out.extend(resolve_fills(signals, series=member.series, identity=plan.identity, instrument_id=instrument_id))


def _census(rows: Sequence[LedgerRow]) -> dict[SignalKind, dict[CensusKey, int]]:
    by_kind: dict[SignalKind, dict[CensusKey, int]] = {}
    for row in rows:
        key: CensusKey = (row.signal_kind, row.verdict, row.not_evaluable_reason or "")
        bucket = by_kind.setdefault(row.signal_kind, {})
        bucket[key] = bucket.get(key, 0) + 1
    return by_kind


def assert_census_complete(
    entry: StrategyEntry,
    census: Mapping[SignalKind, Mapping[CensusKey, int]],
    expected_per_leg: int,
    *,
    instruments_evaluated: int,
    eligible_instruments: int,
) -> None:
    """Every declared leg carries exactly one row per eligible instrument-bar.

    ⚠⚠ A GATE, NOT A LOG LINE (spec §9, acceptance 4). A scan that silently
    covered 4,000 instruments instead of 5,783 is the manifest defect (#2394 §2)
    reappearing at the population layer — and zero coverage is only detectable
    against an *expected* count, because an absent row indexes to nothing. So a
    shortfall raises rather than under-reporting.

    ⚠⚠ TWO CHECKS, AND THE POPULATION ONE IS NOT REDUNDANT. ``expected_per_leg``
    is a sum over the windows the scan actually computed, so a window that came
    out empty lowers the expectation and the row count together — the census
    agrees with itself and reports nothing. ``instruments_evaluated`` is checked
    against the ELIGIBLE population instead, a count taken before any bar was
    loaded, so an instrument the scan could not evaluate at all fails here. Its
    cost is a permanent hole: the watermark would advance past a bar that was
    never written, and nothing after it can reach back.

    ⚠ IT COUNTS INSTRUMENTS **EVALUATED**, NOT INSTRUMENTS THAT PRODUCED A ROW,
    and the distinction is the review's WARNING on the first version. An empty
    window is a legitimate outcome — an instrument rejoining the frontier after a
    stale spell has no bar at or after the watermark to write, and a series that
    shrank mid-scan may be fully caught up already. Gating on "produced a row"
    aborts a healthy batch for both. What is NOT legitimate is an eligible
    instrument the loader could not return two bars for, because that one was
    owed a bar and did not get it; the counter is incremented at load time, so it
    is independent of the window computation rather than derived from it.

    ⚠ It checks the leg set BOTH ways. A short leg is the failure the spec names;
    an *undeclared* leg is the same defect mirrored — a manifest that says S-4 is
    entry-only while the strategy emits exits means a reader filtering on the
    manifest silently drops rows that exist.
    """
    if instruments_evaluated != eligible_instruments:
        raise RuntimeError(
            f"{entry.strategy_id} evaluated {instruments_evaluated} instruments against {eligible_instruments} "
            "eligible — an eligible instrument the loader could not return is a bar this frontier owes and "
            "no later run can reach, because the watermark would advance past it"
        )
    for kind in sorted(entry.signal_kinds):
        counted = sum(census.get(kind, {}).values())
        if counted != expected_per_leg:
            raise RuntimeError(
                f"{entry.strategy_id} {kind} censused {counted} rows against {expected_per_leg} "
                "expected — a leg is short, so some eligible instrument produced no verdict and "
                "nothing downstream could tell"
            )
    unexpected = set(census) - set(entry.signal_kinds)
    if unexpected:
        raise RuntimeError(f"{entry.strategy_id} emitted {sorted(unexpected)} legs it does not declare in the manifest")


def _commit_strategy(
    conn: psycopg.Connection[Any],
    plan: _Plan,
    rows: Sequence[LedgerRow],
    *,
    expected_per_leg: int,
    instruments_evaluated: int,
    eligible_instruments: int,
    frontier: Frontier,
) -> StrategyScanResult:
    """Check the census, then write rows and watermark in ONE transaction.

    ⚠ One strategy's failure does not stop the others. That is why this returns a
    result carrying the error instead of raising: the manifest is iterated, each
    entry's batch commits independently, and the watermark advances per
    ``(strategy_id, strategy_version)``.
    """
    census = _census(rows)
    try:
        assert_census_complete(
            plan.entry,
            census,
            expected_per_leg,
            instruments_evaluated=instruments_evaluated,
            eligible_instruments=eligible_instruments,
        )
        with conn.transaction():
            storage = store_strategy_observations(conn, rows)
            advance_watermark(
                conn,
                strategy_id=plan.entry.strategy_id,
                strategy_version=plan.version,
                frontier_date=frontier.bar_date,
            )
    except Exception as exc:  # noqa: BLE001 — isolation is the contract, see the docstring
        logger.exception("strategy_signal_scan: %s failed at frontier %s", plan.entry.strategy_id, frontier.bar_date)
        return StrategyScanResult(
            strategy_id=plan.entry.strategy_id,
            strategy_version=plan.version,
            status="failed",
            resumed_from=plan.watermark,
            expected_per_leg=expected_per_leg,
            instruments_evaluated=instruments_evaluated,
            census={key: count for bucket in census.values() for key, count in bucket.items()},
            error=f"{type(exc).__name__}: {exc}",
        )

    return StrategyScanResult(
        strategy_id=plan.entry.strategy_id,
        strategy_version=plan.version,
        status="written",
        resumed_from=plan.watermark,
        rows_written=storage.logical_rows,
        durable_signal_rows=storage.fired_rows,
        retained_observation_rows=storage.retained_observation_rows,
        aggregate_rows=storage.aggregate_rows,
        storage_input_bytes=storage.input_payload_bytes,
        expected_per_leg=expected_per_leg,
        instruments_evaluated=instruments_evaluated,
        census={key: count for bucket in census.values() for key, count in bucket.items()},
    )


def log_report(report: ScanReport) -> None:
    """Spec §9's per-run report, emitted by the job rather than by a script."""
    frontier = report.frontier
    logger.info(
        "strategy_signal_scan %s: frontier %s (%d/%d loadable, %.1f%%), eligible %d, "
        "excluded no_bars=%d stale=%d short=%d moved=%d, rows %d",
        report.status,
        frontier.bar_date if frontier else "-",
        frontier.modal_count if frontier else 0,
        frontier.loadable if frontier else 0,
        frontier.share_pct if frontier else 0.0,
        report.eligible_instruments,
        report.excluded_no_bars,
        report.excluded_stale_series,
        report.excluded_short_series,
        report.excluded_moved_mid_scan,
        report.rows_written,
    )
    for result in report.per_strategy:
        logger.info(
            "  %s %s (%s) resumed_from=%s logical_rows=%d durable_fired=%d retained_detail=%d "
            "aggregate_rows=%d input_bytes=%d expected_per_leg=%d instruments=%d%s",
            result.strategy_id,
            result.status,
            result.strategy_version,
            result.resumed_from,
            result.rows_written,
            result.durable_signal_rows,
            result.retained_observation_rows,
            result.aggregate_rows,
            result.storage_input_bytes,
            result.expected_per_leg,
            result.instruments_evaluated,
            f" error={result.error}" if result.error else "",
        )
        for (kind, verdict, reason), count in sorted(result.census.items()):
            suffix = f" ({reason})" if reason else ""
            logger.info("    %s %s %s%s: %d", result.strategy_id, kind, verdict, suffix, count)


__all__ = [
    "FRONTIER_MODAL_SHARE_FLOOR",
    "SCAN_UNIVERSE",
    "Frontier",
    "ScanReport",
    "ScanStatus",
    "StrategyScanResult",
    "advance_watermark",
    "assert_census_complete",
    "choose_frontier",
    "log_report",
    "read_watermarks",
    "run_signal_scan",
    "write_window_indices",
]
