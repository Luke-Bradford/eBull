"""Full-population verification of phase-5e-4's purged walk-forward (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_walk_forward.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

⚠⚠ EVERY FIGURE HERE IS IN-SAMPLE ONLY. The window ends on the last trading date
STRICTLY BEFORE §5.2's frozen ``HOLDOUT_BOUNDARY``, so the hold-out is not read,
not counted and not measured. That is not a convenience — §5.3's whole objection
to a *measured p99* embargo was that the measurement would span the test data,
and a measurement taken over the hold-out would repeat the error one level up.

THE ARMS
--------
``--holds`` — **the measurement §5.3 left open.** S-1 declares no
``max_hold_bars``, and §5.3 rejected *measured p99* on correctness while
conceding *in-sample p100* is leak-free and objecting only to its magnitude:
*"unbounded above and a single long hold makes the embargo swallow the fold"*.
That is a claim about a number, so this arm reports the number — the realised
hold distribution on BOTH axes, and ``h / N_train`` with both of its inputs
rather than as a bare share.

  ⚠⚠ TWO AXES, AND THE DIFFERENCE IS THE FINDING. ``bars_held`` counts the
  INSTRUMENT's own bars. Folds are cut on the PANEL axis — the union of every
  instrument's dates — and an instrument's dates are a subset of it, so a hold
  of ``h`` instrument bars spans ``h`` panel dates or more, never fewer. §5.3's
  own construction (*"the embargo is max_hold_bars … S-3: 10"*) reads an
  instrument-axis constant onto a panel-axis window and under-covers by exactly
  that gap. Both are measured here and the gap is reported.

``--folds`` — the split's invariants over every in-sample observation, asserted
against the definitions rather than against a second implementation of them.

  F1  **Conservation.** Every observation lands in exactly one of test / train /
      purged / embargoed for every fold, and across the four folds each
      observation is in ``test`` exactly once — folds partition the axis, so an
      observation's entry date belongs to one and only one.
  F2  **The purge is complete.** No training observation's label window
      ``[entry, close]`` intersects its fold. ⚠ Checked on the INTERVAL, which
      is what catches a trade spanning the fold entirely — an endpoint test
      calls that one training data.
  F3  **The embargo is complete.** No training observation starts in the
      ``embargo_bars`` dates immediately after its fold.
  F4  ⚠⚠ **The measured panel embargo covers every declared instrument-axis
      bound.** S-3 declares ``max_hold_bars = 10``; if the measured panel span
      of its holds never exceeded 10 this assertion would be vacuous, so the
      arm reports the realised excess as well as asserting the direction.
  F5  **The embargo does not swallow the fold** — the §5.3 objection, restated
      as a property: embargoed observations are a minority of every fold's
      training side, and ``h / N_train`` is printed per fold.

  ⚠ **F0, checked before either arm and for both**: no closed position may have
  a fill or close date absent from the in-sample panel axis. Such a position is
  dropped, and a dropped position shrinks the population every figure is
  computed on — so it FAILS rather than being printed as a count. A printed
  count that nothing gates on is the "reports success while doing nothing"
  shape (`.claude/CLAUDE.md`); found by Codex at checkpoint 2.

⚠ WHAT IS NOT COVERED, STATED SO THE GAP IS A DECISION. Only S-1 and S-3 run,
for phase 5a's reason: S-2 (``calendar``) needs its whole panel resident at once
and S-4 (``level``) needs the resolver over the corpus. Both are covered on
fixtures by ``tests/test_walk_forward.py``, and neither changes a rule this
module applies — the split is a function of dates, not of a close source.
"""

from __future__ import annotations

import argparse
import sys
import time
from array import array
from collections import Counter
from datetime import date

import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.position_builder import Window, build_positions
from app.services.research_price_structure_store import load_masked_series
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s3_mean_reversion_in_trend import MAX_HOLD_BARS as S3_MAX_HOLD_BARS
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import EVALUATION_WINDOW_START, HOLDOUT_BOUNDARY
from app.services.walk_forward import (
    FOLD_COUNT,
    WALK_FORWARD_MODEL_ID,
    Fold,
    bar_weighted_folds,
    census,
    role,
    training_embargo_bars,
)

# ⚠ REUSED, not re-derived. Phase 5a built the corpus→positions path; a second
# copy here would be a second place for the fill rule to drift.
from scripts.verify_2240_position_builder import (
    _S1_REGIME,
    _S3_REGIME,
    UNIVERSE,
    _fills,
    _stamped_versions,
    _to_series,
)

#: The in-sample panel axis and its per-date bar count, in one pass. ⚠ Bounded
#: STRICTLY BELOW the frozen boundary — `< %(boundary)s`, not `<=` — because
#: §5.2 makes that date the FIRST HOLD-OUT BAR.
_AXIS_SQL = """
    SELECT d.bar_date, count(*)
    FROM research_price_series s
    JOIN research_price_daily d ON d.series_id = s.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date >= %(start)s
      AND d.bar_date < %(boundary)s
    GROUP BY d.bar_date
    ORDER BY 1
"""


def _quantile(counts: Counter[int], q: float) -> int:
    """The ``q``-quantile of a value->frequency map, by NEAREST RANK.

    ⚠ Deliberately the same construction as
    ``verify_2240_position_builder._quantile`` and deliberately copied rather
    than imported: that one is a census helper on a different arm, and an
    embargo is a bound. Nearest rank returns an OBSERVED span, never an
    interpolated one — an embargo of 41.5 bars is not a thing this corpus can
    produce, and a bound that falls between two observations is a bound that
    covers neither.
    """
    total = sum(counts.values())
    if total == 0:
        raise ValueError("no observations to take a quantile of")
    target = max(1, min(total, int(-(-total * q // 1))))
    seen = 0
    for value in sorted(counts):
        seen += counts[value]
        if seen >= target:
            return value
    raise AssertionError("unreachable: the loop covers every element")


class _Observations:
    """One strategy's in-sample label windows, on the PANEL axis.

    ⚠ CLOSED POSITIONS ONLY, and the exclusion is a narrowing so it is counted
    rather than asserted harmless. A position still open at the window end has
    an UNRESOLVED label: its close date is not "the end of the axis", it is
    unknown. Feeding it in with an end index at the axis end would hand
    ``training_embargo_bars`` a span the strategy never realised — on early
    folds it would be most of the corpus — and every fold's embargo would be an
    artefact of where the data stops. ⚠ It also biases ``p100`` DOWNWARD, since
    the longest holds are the likeliest to be censored, so the reported figure
    is a lower bound and is described as one.
    """

    def __init__(self, label: str, declared_max_hold_bars: int | None) -> None:
        self.label = label
        self.declared_max_hold_bars = declared_max_hold_bars
        #: Parallel arrays: panel-axis index of the entry fill, and of the close.
        self.starts: array[int] = array("i")
        self.ends: array[int] = array("i")
        #: Panel-axis span, and the instrument-axis `bars_held` beside it.
        self.panel_spans: Counter[int] = Counter()
        self.instrument_spans: Counter[int] = Counter()
        #: How much wider the panel span was than the instrument's own count.
        self.axis_excess: Counter[int] = Counter()
        self.unlabelled_at_window_end = 0
        self.off_axis = 0
        self.problems: list[str] = []

    def absorb(self, positions, *, axis_pos: dict[date, int]) -> None:
        for position in positions:
            if position.close_source is None:
                self.unlabelled_at_window_end += 1
                continue
            assert position.close_bar_date is not None and position.bars_held is not None
            start = axis_pos.get(position.entry_fill_bar_date)
            end = axis_pos.get(position.close_bar_date)
            if start is None or end is None:
                # ⚠⚠ COUNTED AND FAILED, never merely printed. Every fill date
                # is a real bar of an in-universe instrument inside the window,
                # so this must stay zero; a non-zero count means the axis query
                # and the position window disagree, and the arms would then run
                # on a population silently smaller than the corpus. ``main``
                # turns any non-zero count into a property violation — a
                # printed count that nothing gates on is the "job reports
                # success while doing nothing" shape (`.claude/CLAUDE.md`).
                self.off_axis += 1
                continue
            self.starts.append(start)
            self.ends.append(end)
            self.panel_spans[end - start] += 1
            self.instrument_spans[position.bars_held] += 1
            self.axis_excess[(end - start) - position.bars_held] += 1

    def report_holds(self, *, axis_dates: int) -> None:
        print(f"\n  [{self.label}]  declared max_hold_bars = {self.declared_max_hold_bars}")
        print(f"      closed in-sample positions   {len(self.starts):>12,}")
        print(
            f"      unlabelled at window end     {self.unlabelled_at_window_end:>12,}   (excluded — see the class note)"
        )
        print(f"      fill dates off the axis      {self.off_axis:>12,}   (must be 0)")
        if not self.starts:
            print("      *** no closed positions — nothing to measure")
            return
        for name, spans in (("panel-axis span", self.panel_spans), ("instrument bars_held", self.instrument_spans)):
            p100 = max(spans)
            near = sum(count for value, count in spans.items() if value >= p100 * 0.9)
            print(
                f"      {name:<22} p50 {_quantile(spans, 0.50):>6,} · p95 {_quantile(spans, 0.95):>6,} · "
                f"p99 {_quantile(spans, 0.99):>6,} · p100 {p100:>6,}   "
                f"(within 10% of p100: {near:,} of {sum(spans.values()):,})"
            )
        excess_p100 = max(self.axis_excess)
        widened = sum(count for value, count in self.axis_excess.items() if value > 0)
        print(
            f"      panel − instrument span    max {excess_p100:>6,} · widened on {widened:,} of "
            f"{sum(self.axis_excess.values()):,} positions "
            f"({100.0 * widened / max(1, sum(self.axis_excess.values())):.2f}%)"
        )
        if min(self.axis_excess) < 0:
            self.problems.append(
                f"{self.label}: a panel-axis span came out SHORTER than the instrument's own bars_held "
                f"(min excess {min(self.axis_excess)}) — the panel axis is meant to be a superset of every "
                "instrument's dates"
            )
        h = max(self.panel_spans)
        print(f"      §5.3's h (in-sample p100 panel span)  {h:,} bars of {axis_dates:,} in-sample dates")


def _collect(*, limit: int | None) -> tuple[dict[str, _Observations], tuple[date, ...], tuple[int, ...], int]:
    """One sweep of the corpus; both arms report off it."""
    started = time.monotonic()
    s1_version, s3_version, builder_version = _stamped_versions()
    print(f"\n[sweep] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"        {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"        builder {builder_version}   cost model {COST_MODEL_ID}", flush=True)
    print(f"        walk-forward {WALK_FORWARD_MODEL_ID}   folds {FOLD_COUNT}", flush=True)

    observations = {
        "S-1": _Observations("S-1", None),
        "S-3": _Observations("S-3", S3_MAX_HOLD_BARS),
    }
    empty = 0

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        rows = conn.execute(
            _AXIS_SQL,
            {"ids": list(universe), "start": EVALUATION_WINDOW_START, "boundary": HOLDOUT_BOUNDARY},
        ).fetchall()
        axis = tuple(row[0] for row in rows)
        bar_counts = tuple(int(row[1]) for row in rows)
        axis_pos = {when: index for index, when in enumerate(axis)}
        window = Window(start=EVALUATION_WINDOW_START, end=axis[-1])
        print(f"  validated universe   {len(universe):,} instruments", flush=True)
        print(f"  in-sample axis       {len(axis):,} dates, {sum(bar_counts):,} bars", flush=True)
        print(f"  window               {window.start} … {window.end}   (boundary {HOLDOUT_BOUNDARY})", flush=True)

        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        instruments = {int(row[0]) for row in pairs}
        if len(instruments) != len(pairs):
            raise RuntimeError(f"{len(pairs) - len(instruments)} instruments carry more than one research series")
        if limit is not None:
            pairs = pairs[:limit]
            print(f"  ⚠ LIMITED to the first {len(pairs)} series — NOT a full-population figure", flush=True)

        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            masked = load_masked_series(conn, series_id)
            if not masked.bars:
                empty += 1
                continue
            series, _masked_opens = _to_series(masked.bars)
            for label, identity, signals, regime, strategy_id, version in (
                (
                    "S-1",
                    s1_identity,
                    s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S1_REGIME,
                    S1_STRATEGY_ID,
                    s1_version,
                ),
                (
                    "S-3",
                    s3_identity,
                    s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S3_REGIME,
                    S3_STRATEGY_ID,
                    s3_version,
                ),
            ):
                ledger_rows = resolve_fills(
                    signals,
                    series=series,
                    identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
                    instrument_id=int(instrument_id),
                )
                entries, exits = _fills(ledger_rows, int(instrument_id))
                built = build_positions(
                    strategy_id=strategy_id,
                    strategy_version=version,
                    entries=entries,
                    exits=exits,
                    outcomes=[],
                    outcome_pin=None,
                    series={int(instrument_id): series},
                    regime=regime,
                    window=window,
                )
                observations[label].absorb(built.positions, axis_pos=axis_pos)
            if n % 250 == 0:
                print(
                    f"  {n}/{len(pairs)} series, "
                    f"{sum(len(o.starts) for o in observations.values()):,} closed positions "
                    f"({time.monotonic() - started:.0f}s)",
                    flush=True,
                )

    print(f"\n  series with usable bars {len(pairs) - empty:,}   (fail-closed empties: {empty})", flush=True)
    print(f"  sweep elapsed           {time.monotonic() - started:.1f}s", flush=True)
    _stamped_versions()
    return observations, axis, bar_counts, len(axis)


def _report_holds(observations: dict[str, _Observations], *, axis_dates: int) -> list[str]:
    print("\n=== --holds : §5.3's open measurement ===")
    problems: list[str] = []
    for sleeve in observations.values():
        sleeve.report_holds(axis_dates=axis_dates)
        problems.extend(sleeve.problems)
        sleeve.problems = []
    return problems


def _report_folds(
    observations: dict[str, _Observations],
    *,
    axis: tuple[date, ...],
    bar_counts: tuple[int, ...],
) -> list[str]:
    print("\n=== --folds : the purged split, asserted ===")
    problems: list[str] = []
    folds = bar_weighted_folds(bar_counts, fold_count=FOLD_COUNT)
    total_bars = sum(bar_counts)
    print(f"\n  {FOLD_COUNT} bar-weighted folds over {len(axis):,} dates / {total_bars:,} bars")
    for fold in folds:
        bars = sum(bar_counts[fold.first_index : fold.last_index + 1])
        print(
            f"    fold {fold.index}  {axis[fold.first_index]} … {axis[fold.last_index]}   "
            f"{fold.date_count:>6,} dates · {bars:>12,} bars ({100.0 * bars / total_bars:5.2f}%)"
        )

    for sleeve in observations.values():
        if not sleeve.starts:
            problems.append(f"{sleeve.label}: no closed positions — the fold arm has nothing to assert")
            continue
        print(f"\n  [{sleeve.label}]  {len(sleeve.starts):,} in-sample observations")
        test_total = 0
        for fold in folds:
            embargo = training_embargo_bars(sleeve.starts, sleeve.ends, fold=fold)
            counted = census(sleeve.starts, sleeve.ends, fold=fold, embargo_bars=embargo)
            # F1 — conservation.
            if counted.total != len(sleeve.starts):
                problems.append(
                    f"{sleeve.label}/fold {fold.index}: F1 census totals {counted.total:,} against "
                    f"{len(sleeve.starts):,} observations"
                )
            test_total += counted.test
            problems.extend(_assert_purge_and_embargo(sleeve, fold=fold, embargo_bars=embargo))
            # F5 — the §5.3 objection, as a number rather than a fear.
            train_dates = len(axis) - fold.date_count
            share = 100.0 * counted.embargoed / max(1, counted.train + counted.embargoed)
            print(
                f"    fold {fold.index}  embargo h {embargo:>5,} bars   h/N_train {embargo}/{train_dates:,} = "
                f"{100.0 * embargo / train_dates:6.3f}%   "
                f"test {counted.test:>10,} · train {counted.train:>10,} · purged {counted.purged:>8,} · "
                f"embargoed {counted.embargoed:>8,} ({share:5.2f}% of the training side)"
            )
            if counted.embargoed >= counted.train:
                problems.append(
                    f"{sleeve.label}/fold {fold.index}: F5 the embargo removed {counted.embargoed:,} training "
                    f"observations and left {counted.train:,} — it swallowed the fold"
                )
            # F4 — the axis assertion.
            if sleeve.declared_max_hold_bars is not None and embargo < sleeve.declared_max_hold_bars:
                problems.append(
                    f"{sleeve.label}/fold {fold.index}: F4 measured panel embargo {embargo} is below the declared "
                    f"instrument-axis bound {sleeve.declared_max_hold_bars} — the panel axis cannot be shorter"
                )
        # F1's second half: folds partition the axis, so every observation is
        # exactly one fold's test observation.
        if test_total != len(sleeve.starts):
            problems.append(
                f"{sleeve.label}: F1 the four folds hold {test_total:,} test observations between them, against "
                f"{len(sleeve.starts):,} — the folds do not partition the axis"
            )
    return problems


def _assert_purge_and_embargo(sleeve: _Observations, *, fold: Fold, embargo_bars: int) -> list[str]:
    """F2 and F3, checked against the DEFINITIONS rather than against ``role``.

    ⚠ The point of re-deriving the predicates here is that a reference which
    calls the same function agrees with a shared misreading (prevention log,
    #2240 S-3). ``role`` decides; these two lines say what the decision must
    satisfy, written from §5.3's wording.
    """
    problems: list[str] = []
    overlaps = 0
    inside_embargo = 0
    for start, end in zip(sleeve.starts, sleeve.ends, strict=True):
        if role(start, end, fold=fold, embargo_bars=embargo_bars) != "train":
            continue
        if start <= fold.last_index and end >= fold.first_index:
            overlaps += 1
        if fold.last_index < start <= fold.last_index + embargo_bars:
            inside_embargo += 1
    if overlaps:
        problems.append(
            f"{sleeve.label}/fold {fold.index}: F2 {overlaps:,} training observations have a label window "
            f"overlapping the fold — the purge is incomplete"
        )
    if inside_embargo:
        problems.append(
            f"{sleeve.label}/fold {fold.index}: F3 {inside_embargo:,} training observations start inside the "
            f"{embargo_bars}-bar embargo after the fold"
        )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--holds", action="store_true", help="§5.3's open measurement: realised holds on both axes")
    parser.add_argument("--folds", action="store_true", help="assert F1-F5 over the in-sample population")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.holds or args.folds or args.all):
        parser.error("pick at least one arm: --holds, --folds or --all")

    observations, axis, bar_counts, axis_dates = _collect(limit=args.limit)
    problems: list[str] = []
    # ⚠⚠ CHECKED BEFORE EITHER ARM AND FOR BOTH. An off-axis fill silently
    # shrinks the population every figure below is computed on, so a run that
    # dropped observations must FAIL rather than report on what survived —
    # otherwise the gate exits 0 on a smaller corpus than it claims to cover.
    problems.extend(
        f"{sleeve.label}: {sleeve.off_axis:,} closed positions have a fill or close date absent from the in-sample "
        "panel axis — the axis query and the position window disagree, so every figure here is computed on a "
        "silently smaller population"
        for sleeve in observations.values()
        if sleeve.off_axis
    )
    if args.holds or args.all:
        problems.extend(_report_holds(observations, axis_dates=axis_dates))
    if args.folds or args.all:
        problems.extend(_report_folds(observations, axis=axis, bar_counts=bar_counts))

    print(f"\n  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    if len(problems) > 20:
        print(f"    … and {len(problems) - 20} more")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
