"""Full-population verification of phase-5e-5a's quarantine sensitivity arm (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_quarantine_sensitivity.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every assertion passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

WHAT CRITERION 9 ASKS FOR, AND WHICH ARM ANSWERS WHICH HALF
-----------------------------------------------------------
Parent criterion 9: *"Report the count and share of bars/trades excluded per
strategy, and run one sensitivity arm with conservative handling, so exclusion is
visible rather than assumed harmless."* §9 C9 defines the arm: *"re-run with
quarantined bars admitted at their stored values rather than masked, and report
the delta in every C7 metric."*

``--census`` — the counts and shares, over every series in the validated
universe. ⚠ TWO EXCLUSION CHANNELS, not one: the masked FIELDS (a range verdict
drops high/low, a return verdict drops the close) and the SERIES-level
fail-closed refusal (no coverage row, or coverage at a stale rule-set version →
zero bars). The second never appears in a delta, because it excludes the series
from both arms, so a census reporting only the first would describe the smaller
exclusion as the whole of it.

``--arms`` — both arms of the whole pipeline, and the delta in criterion 7's
twelve. Reuses phase 5a's corpus→positions path and 5d's curve layer verbatim;
the only thing this script owns is running it twice and subtracting.

  Q1  **One fetch, two arms.** Every series is read ONCE and both arms are
      derived from those rows (``load_arms``), so the arms cannot differ by
      anything except the arm. Asserted structurally: ``QuarantineCensus``
      refuses a pair whose bar, series or flag counts disagree.
  Q2  **The masking mechanism actually fires.** The number of bars whose
      ``StructureBar`` differs between the arms is counted and must be > 0. ⚠
      Bounded ABOVE by the flagged count and not equal to it: a flagged bar
      whose stored field is already NULL is identical under both arms, so the
      gap is reported rather than asserted away.
  Q3  **Admission cannot manufacture a ``quarantined_bar`` verdict.** That
      reason code is emitted where a close is missing; admitting restores
      closes, so the admitted arm's count is ``<=`` the masked arm's. This is
      the one direction the mechanism guarantees.
  Q4  **All twelve C7 metrics are compared**, and a metric null in one arm is
      reported as a state, never as a zero delta.
  Q5  **Both arms produce a metric set at all.** An arm that refused would make
      every delta absent, which is C9's *"an arm that cannot be defined is an
      arm nobody ran"*.

⚠⚠ THE TRADE DELTA IS SIGNED AND NOTHING ASSERTS ITS DIRECTION. Admitting a
close can remove trades as easily as add them — an earlier exit frees the
instrument for another entry, a later one blocks one (§3.1's pyramiding
collapse). An assertion of direction would fire on correct code.

⚠ WHAT IS NOT COVERED, STATED SO THE GAP IS A DECISION. Only S-1 and S-3 run,
for phase 5a's reason: S-2 (``calendar``) needs its whole panel resident at once
and S-4 (``level``) needs the resolver over the corpus. ⚠ That matters more here
than in the earlier stages: S-4 is the only sleeve reading HIGH and LOW, and the
range verdict is the larger half of this corpus's quarantine. So the metric
delta below is measured against the SMALLER exposure, and the census reports
both verdicts so the untested one is visible rather than absent.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import date

import psycopg

from app.config import settings
from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED
from app.services.equity_curve import SIZING_RULE_ID, LegBook, build_equity_curve
from app.services.position_builder import Window, build_positions
from app.services.position_costing import cost_positions
from app.services.quarantine_sensitivity import (
    SENSITIVITY_MODEL_ID,
    SPEC_CRITERION7_METRICS,
    ArmCensus,
    MetricDelta,
    QuarantineCensus,
    compare_metrics,
)
from app.services.research_price_structure_store import (
    QUARANTINE_RULE_SET_VERSION,
    MaskedSeries,
    QuarantineArm,
    load_arms,
)
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import EVALUATION_WINDOW_END, EVALUATION_WINDOW_START
from app.services.strategy_statistics import METRIC_SET_ID, StrategyMetrics

# ⚠ REUSED, not re-derived. 5a built the corpus→positions path, 5b the costing
# and 5d the curve layer; a second copy of any of them here would be a second
# place for the fill rule to drift. This script owns the SECOND ARM and the
# subtraction, and nothing else.
from scripts.verify_2240_position_builder import (
    _S1_REGIME,
    _S3_REGIME,
    UNIVERSE,
    _fills,
    _stamped_versions,
    _to_series,
)
from scripts.verify_2240_statistics import _AXIS_SQL, _benchmark_leg, _Sleeve

#: The two arms, in the order they are reported. ⚠ ``masked`` first because it is
#: the shipped read: the delta is "what admitting would change", not the reverse.
ARMS: tuple[QuarantineArm, ...] = ("masked", "admitted")

_LABELS = ("S-1", "S-3")


def census() -> int:
    """Criterion 9's counts and shares, over the whole validated universe.

    ⚠ Computed at run time and never written into prose. A hand-copied share
    goes stale silently the moment the quarantine rule set moves, which is the
    #2282 lesson; the command that reproduces it is this function.
    """
    started = time.monotonic()
    print(f"\n[census] quarantine rule set {QUARANTINE_RULE_SET_VERSION}", flush=True)
    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        print(f"  validated universe   {len(universe):>12,} instruments", flush=True)
        print(f"  corpus series in it  {len(pairs):>12,}", flush=True)

        bars = 0
        bars_flagged = 0
        range_flagged = 0
        return_flagged = 0
        series_flagged = 0
        fail_closed = 0
        differing = 0
        already_null = 0
        for n, (_instrument_id, series_id) in enumerate(pairs, start=1):
            # ⚠ `loaded`, not `arms` — a local of that name here would shadow
            # the `arms()` arm below, which is the sort of thing that reads fine
            # and confuses the next reader.
            loaded = load_arms(conn, int(series_id))
            masked, admitted = loaded["masked"], loaded["admitted"]
            if not masked.bars:
                fail_closed += 1
                continue
            bars += len(masked.bars)
            bars_flagged += masked.bars_flagged
            range_flagged += masked.range_flagged
            return_flagged += masked.return_flagged
            series_flagged += 1 if masked.bars_flagged else 0
            changed = _differing_bars(masked, admitted)
            differing += changed
            already_null += masked.bars_flagged - changed
            if n % 1000 == 0:
                print(f"  {n}/{len(pairs)} series ({time.monotonic() - started:.0f}s)", flush=True)

    print(f"\n  series evaluated     {len(pairs) - fail_closed:>12,}")
    # ⚠ The other exclusion channel, and it is NOT an arm: a series the rules
    # never saw contributes zero bars to BOTH arms, so it can never show up in a
    # delta. Counted here or nowhere.
    print(f"  series fail-closed   {fail_closed:>12,}   (no coverage row / stale rule set — excluded from BOTH arms)")
    print(f"  bars read            {bars:>12,}")
    print(f"  bars flagged         {bars_flagged:>12,}   on {series_flagged:,} series")
    # ⚠ "—" and not "0.000000%" on an empty read, which is the rule
    # ``ArmCensus.flagged_bar_share_pct`` enforces for the same reason: a run
    # whose coverage join matched nothing would otherwise print a reassuring
    # zero produced by having read no bars at all.
    rendered_share = "—" if bars == 0 else f"{100.0 * bars_flagged / bars:.6f}%"
    print(f"  flagged bar share    {rendered_share:>12}   ⚠ computed, never transcribed")
    print(f"    range verdicts     {range_flagged:>12,}   (high/low — read by S-4 only, which does not run here)")
    print(f"    return verdicts    {return_flagged:>12,}   (close — read by every sleeve)")
    print(f"  bars the arms differ {differing:>12,}   of {bars_flagged:,} flagged")
    print(f"  flagged but identical{already_null:>12,}   (the stored field was already NULL — masking cost nothing)")
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)

    problems: list[str] = []
    if bars == 0 or len(pairs) - fail_closed == 0:
        problems.append("Q2 the census read no bars — the coverage join or the rule-set version is wrong")
    if bars_flagged == 0:
        problems.append("Q2 no bar is flagged anywhere in the universe: the sensitivity arm has nothing to measure")
    if differing == 0:
        problems.append("Q2 no bar differs between the arms: admission changed nothing, so no delta can be attributed")
    for problem in problems:
        print(f"    *** {problem}")
    return 1 if problems else 0


def _differing_bars(masked: MaskedSeries, admitted: MaskedSeries) -> int:
    """Bars whose fields actually differ between the arms.

    ⚠ ``<= bars_flagged``, never ``==``. A bar flagged on its return verdict
    whose stored close is already NULL reads identically under both arms —
    masking a null costs nothing — so the gap is a real quantity and is
    reported. Asserting equality would fail on correct code.
    """
    return sum(1 for left, right in zip(masked.bars, admitted.bars, strict=True) if left != right)


def arms(*, limit: int | None) -> int:
    """Both arms of the whole pipeline, and the delta in criterion 7's twelve."""
    started = time.monotonic()
    s1_version, s3_version, builder_version = _stamped_versions()
    print(f"\n[arms] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"       {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"       builder {builder_version}   cost model {COST_MODEL_ID}", flush=True)
    print(f"       carry_unmodelled {CARRY_UNMODELLED}   fx_unmodelled {FX_UNMODELLED}", flush=True)
    print(f"       sizing {SIZING_RULE_ID}   metrics {METRIC_SET_ID}   sensitivity {SENSITIVITY_MODEL_ID}", flush=True)
    window = Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)
    print(f"       window {window.start} … {window.end}", flush=True)

    problems: list[str] = []
    # Per arm: one sleeve pair and one benchmark. ⚠ THE BENCHMARK IS PER ARM
    # TOO. `return_vs_buy_and_hold_pct` divides by it, and admitting a close
    # moves the buy-and-hold leg as surely as it moves the strategy's — holding
    # the masked benchmark fixed would charge the admitted arm's numerator
    # against the masked arm's denominator.
    sleeves: dict[QuarantineArm, dict[str, _Sleeve]] = {
        arm: {label: _Sleeve(f"{label}/{arm}") for label in _LABELS} for arm in ARMS
    }
    benchmarks: dict[QuarantineArm, LegBook] = {arm: LegBook() for arm in ARMS}
    signals_seen: dict[QuarantineArm, dict[str, Counter[str]]] = {
        arm: {label: Counter() for label in _LABELS} for arm in ARMS
    }
    # ⚠⚠ ACCUMULATED PER ARM, from EACH arm's own `MaskedSeries`, and not once
    # off the masked one. Q1 compares the two tallies, and a check fed the same
    # numbers twice by construction is a check that cannot fail — the shape this
    # repo's own rules call "reports success while doing nothing". Both arms come
    # off one fetch, so they SHOULD agree; this is what makes that an assertion
    # rather than an arrangement.
    read: dict[QuarantineArm, dict[str, int]] = {
        arm: {"series_evaluated": 0, "fail_closed": 0, "bars": 0, "bars_flagged": 0, "range": 0, "return": 0}
        for arm in ARMS
    }

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        bounds = {"ids": list(universe), "start": EVALUATION_WINDOW_START, "end": EVALUATION_WINDOW_END}
        axis = tuple(row[0] for row in conn.execute(_AXIS_SQL, bounds).fetchall())
        axis_pos = {when: index for index, when in enumerate(axis)}
        print(f"  evaluation axis  {len(axis):,} trading dates", flush=True)

        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        if limit is not None:
            pairs = pairs[:limit]
            print(f"  ⚠ LIMITED to the first {len(pairs)} series — NOT a full-population figure", flush=True)

        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            # ⚠⚠ ONE FETCH, BOTH ARMS. Two `load_masked_series` calls would read
            # the corpus twice, and two reads are two chances for the arms to
            # differ by something other than the arm.
            loaded = load_arms(conn, int(series_id))
            for arm in ARMS:
                tally = read[arm]
                if not loaded[arm].bars:
                    tally["fail_closed"] += 1
                    continue
                tally["series_evaluated"] += 1
                tally["bars"] += len(loaded[arm].bars)
                tally["bars_flagged"] += loaded[arm].bars_flagged
                tally["range"] += loaded[arm].range_flagged
                tally["return"] += loaded[arm].return_flagged
                _absorb_series(
                    loaded[arm],
                    instrument_id=int(instrument_id),
                    axis_pos=axis_pos,
                    window=window,
                    sleeves=sleeves[arm],
                    benchmark=benchmarks[arm],
                    signals_seen=signals_seen[arm],
                    versions=(s1_version, s3_version),
                )
            if n % 250 == 0:
                seen = sum(len(sleeve.problems) for arm_sleeves in sleeves.values() for sleeve in arm_sleeves.values())
                print(f"  {n}/{len(pairs)} series, {seen} problems ({time.monotonic() - started:.0f}s)", flush=True)

    for arm in ARMS:
        tally = read[arm]
        print(
            f"\n  [{arm}] series evaluated {tally['series_evaluated']:,}   fail-closed {tally['fail_closed']:,}   "
            f"bars {tally['bars']:,}   flagged {tally['bars_flagged']:,}",
            flush=True,
        )

    measured: dict[QuarantineArm, dict[str, StrategyMetrics]] = {arm: {} for arm in ARMS}
    for arm in ARMS:
        curve = build_equity_curve(benchmarks[arm], date_count=len(axis))
        print(f"\n  === arm {arm} ===   benchmark legs {len(benchmarks[arm]):,}", flush=True)
        for label in _LABELS:
            sleeve = sleeves[arm][label]
            metrics = sleeve.report(axis=axis, benchmark_curve=curve)
            problems.extend(sleeve.problems)
            if metrics is None:
                # Q5 — an arm that produced no metric set makes every delta
                # absent, which is C9's "an arm nobody ran".
                problems.append(f"Q5 {label}/{arm}: the arm produced no metric set, so no delta can be reported")
                continue
            measured[arm][label] = metrics

    for label in _LABELS:
        problems.extend(
            _report_strategy(
                label,
                read=read,
                sleeves={arm: sleeves[arm][label] for arm in ARMS},
                signals_seen={arm: signals_seen[arm][label] for arm in ARMS},
                measured={arm: measured[arm].get(label) for arm in ARMS},
            )
        )

    print(f"\n  property violations: {len(problems)}")
    for problem in problems[:20]:
        print(f"    {problem}")
    if len(problems) > 20:
        print(f"    … and {len(problems) - 20} more")
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    # ⚠ Re-checked AFTER the sweep as well as before — phase 5a's reason: a probe
    # harness that mutated and restored a source file mid-run would pass an
    # entry check alone.
    _stamped_versions()
    return 1 if problems else 0


def _absorb_series(
    loaded: MaskedSeries,
    *,
    instrument_id: int,
    axis_pos: dict[date, int],
    window: Window,
    sleeves: dict[str, _Sleeve],
    benchmark: LegBook,
    signals_seen: dict[str, Counter[str]],
    versions: tuple[str, str],
) -> None:
    """One series through both strategies, for ONE arm. 5d's loop body, reused."""
    series, _masked_opens = _to_series(loaded.bars)
    indices = [axis_pos[when] for when in series.dates if when in axis_pos]
    if len(indices) < 2:
        return
    first_axis_index, last_axis_index = indices[0], indices[-1]
    closes = [float("nan")] * (last_axis_index - first_axis_index + 1)
    for when, row in zip(series.dates, series.rows, strict=True):
        slot = axis_pos.get(when)
        close = row.get("close")
        if slot is not None and close is not None:
            closes[slot - first_axis_index] = float(close)

    _benchmark_leg(
        benchmark,
        series=series,
        axis_pos=axis_pos,
        closes=closes,
        first_axis_index=first_axis_index,
    )

    s1_version, s3_version = versions
    for label, identity, produced, regime, strategy_id, version in (
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
        # C8's reason census, per arm. ⚠ Counted from the strategy's OWN output
        # rather than reconstructed downstream: a signal the fill resolver
        # dropped never reaches a position, and its reason would vanish.
        for signal in produced:
            if signal.reason is not None:
                signals_seen[label][signal.reason] += 1
        rows = resolve_fills(
            produced,
            series=series,
            identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
            instrument_id=instrument_id,
        )
        entries, exits = _fills(rows, instrument_id)
        built = build_positions(
            strategy_id=strategy_id,
            strategy_version=version,
            entries=entries,
            exits=exits,
            outcomes=[],
            outcome_pin=None,
            series={instrument_id: series},
            regime=regime,
            window=window,
        )
        sleeves[label].absorb(
            list(cost_positions(built.positions, price_basis="split_adjusted")),
            series=series,
            window=window,
            axis_pos=axis_pos,
            closes=closes,
            first_axis_index=first_axis_index,
        )


def _report_strategy(
    label: str,
    *,
    read: dict[QuarantineArm, dict[str, int]],
    sleeves: dict[QuarantineArm, _Sleeve],
    signals_seen: dict[QuarantineArm, Counter[str]],
    measured: dict[QuarantineArm, StrategyMetrics | None],
) -> list[str]:
    """One strategy's census and delta table. Returns its property violations."""
    problems: list[str] = []
    print(f"\n  ### {label} — criterion 9", flush=True)

    arm_censuses: dict[QuarantineArm, ArmCensus] = {}
    for arm in ARMS:
        tally = read[arm]
        arm_censuses[arm] = ArmCensus(
            arm=arm,
            series_evaluated=tally["series_evaluated"],
            series_fail_closed=tally["fail_closed"],
            bars=tally["bars"],
            bars_flagged=tally["bars_flagged"],
            range_flagged=tally["range"],
            return_flagged=tally["return"],
            not_evaluable=dict(signals_seen[arm]),
            trades=sleeves[arm].positions,
        )
    try:
        # Q1 — the controlled-experiment check. Refuses a pair whose populations
        # differ, which would make every delta below uninterpretable.
        pair = QuarantineCensus(strategy=label, masked=arm_censuses["masked"], admitted=arm_censuses["admitted"])
    except ValueError as exc:
        problems.append(f"Q1 {label}: {exc}")
        return problems

    share = pair.masked.flagged_bar_share_pct
    rendered_share = "—" if share is None else f"{share:.6f}%"
    print(f"      bars flagged / share   {pair.masked.bars_flagged:>10,}   {rendered_share}")
    print(f"      trades masked          {pair.masked.trades:>10,}")
    print(f"      trades admitted        {pair.admitted.trades:>10,}")
    delta_share = pair.trade_delta_share_pct
    tail = "" if delta_share is None else f"   {delta_share:+.6f}% of the masked arm"
    print(f"      trade delta            {pair.trade_delta:>+10,}{tail}   ⚠ signed; direction is not guaranteed")

    for arm in ARMS:
        counts = arm_censuses[arm].not_evaluable
        rendered = "  ".join(f"{reason} {count:,}" for reason, count in sorted(counts.items())) or "none"
        print(f"      not_evaluable {arm:<9}{rendered}")

    # Q3 — the one direction the mechanism guarantees. `quarantined_bar` is
    # emitted where a close is missing, and admitting can only restore closes.
    masked_qb = pair.masked.quarantined_bar_signals
    admitted_qb = pair.admitted.quarantined_bar_signals
    if admitted_qb > masked_qb:
        problems.append(
            f"Q3 {label}: the admitted arm emitted MORE quarantined_bar verdicts ({admitted_qb}) than the masked arm "
            f"({masked_qb}) — admission restores closes and cannot manufacture a missing one"
        )

    left, right = measured["masked"], measured["admitted"]
    if left is None or right is None:
        problems.append(f"Q4 {label}: an arm has no metric set, so criterion 7's twelve cannot be compared")
        return problems

    deltas = compare_metrics(left, right)
    # Q4 — all twelve, and the completeness is asserted rather than eyeballed.
    if len(deltas) != len(SPEC_CRITERION7_METRICS):
        problems.append(f"Q4 {label}: {len(deltas)} deltas for {len(SPEC_CRITERION7_METRICS)} criterion-7 metrics")
    _print_deltas(deltas)
    return problems


def _print_deltas(deltas: tuple[MetricDelta, ...]) -> None:
    print(f"      {'metric':<28}{'masked':>18}{'admitted':>18}{'delta':>18}{'rel':>12}")
    for delta in deltas:
        if delta.state != "measured":
            print(f"      {delta.metric:<28}{'—':>18}{'—':>18}{delta.state:>18}{'':>12}")
            continue
        assert delta.masked is not None and delta.admitted is not None and delta.delta is not None
        relative = delta.relative_pct
        rel = "" if relative is None else f"{relative:+.4f}%"
        print(f"      {delta.metric:<28}{delta.masked:>18,.6f}{delta.admitted:>18,.6f}{delta.delta:>+18,.6f}{rel:>12}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", action="store_true", help="criterion 9's counts and shares over the universe")
    parser.add_argument("--arms", action="store_true", help="both arms end to end; the delta in criterion 7's twelve")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.census or args.arms or args.all):
        parser.error("choose --census, --arms or --all")

    status = 0
    if args.census or args.all:
        status |= census()
    if args.arms or args.all:
        status |= arms(limit=args.limit)
    print("\nPASS" if status == 0 else "\nFAIL")
    return status


if __name__ == "__main__":
    sys.exit(main())
