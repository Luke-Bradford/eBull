"""Full-population measurement behind #2394 §3.2 — ``strategy_backtest_run`` (#2240).

    PYTHONPATH=. uv run python scripts/verify_2394_backtest_run.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

WHY THIS EXISTS
---------------
§3.2 of ``docs/proposals/ta/2026-08-08-strategy-runner-and-manifest.md`` is three
sentences: *"Runs the harness for a strategy × namespace × arm and persists via
result_ledger. Manual-trigger only: it is expensive, and its
StrategyIdentity.version must be stable."* Its own §4 question 6 then says the
identity is wider than that. Every number the §3.2 spec quotes is produced here,
so none of them is arithmetic done in prose.

THE ARMS
--------
``--population`` — the corpus ∩ validated-universe slice this job would evaluate,
and the frozen §5.2 split re-derived at BAR level against the literals in
``strategy_result``. ⚠ Re-derived and COMPARED, never re-fitted:
``verify_2240_result_model.py --frozen`` owns the same assertion and this arm
must agree with it.

``--runnable`` — which manifest strategies can produce a stored result TODAY.
Derived from ``STRATEGY_MANIFEST`` rather than from a hand-written list, and the
level-based refusal is demonstrated by CALLING ``build_positions``, not asserted
from the docstring.

``--arms`` — the arm-count arithmetic: how many ``strategy_results`` rows one
"backtest everything" invocation would write, from the manifest and the closed
vocabularies. ⚠ Nothing here is a literal count; each factor is ``len()`` of the
thing that defines it.

``--arm`` — the expensive one. Runs ONE arm end-to-end over the whole corpus
(S-1, masked, whole window) through the real path — ``STRATEGY_MANIFEST`` →
``signal_ledger.resolve_fills`` → ``build_positions`` → ``cost_positions`` →
``build_equity_curve`` → ``compute_metrics`` — and reports:

  A1  the wall-clock cost, split by phase, so "expensive" is a number;
  A2  the §5.2 namespace partition of the positions the SINGLE pass produced —
      which is the finding that decides whether the hold-out arm is a second
      run or a filter;
  A3  the close-source census, which is what makes the ambiguity-arm claim
      measurable rather than structural prose;
  A4  ``check_promotable`` on a WHOLE-WINDOW probe row, so the refusal list the
      operator would see is measured and not predicted. ⚠ The probe is not a
      namespace arm — this script builds no namespace-scoped curve (spec §5) —
      and A4 DEMONSTRATES that the substitution is sound by blanking
      ``effective_sample_size`` and showing the gate reads exactly one metric,
      for presence only.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Final

import psycopg

from app.config import settings
from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED
from app.services.deflated_sharpe import MIN_MEASURED_TRIALS
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID, LegBook
from app.services.indicator_series import BarSeries
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import (
    RULE_SET_VERSION as BUILDER_RULE_SET_VERSION,
)
from app.services.position_builder import (
    EntryFill,
    ExitRegime,
    OutcomePin,
    Window,
    build_positions,
)
from app.services.position_costing import CostedPosition, cost_positions
from app.services.research_price_structure_store import (
    QUARANTINE_ARMS,
    QUARANTINE_RULE_SET_VERSION,
    load_masked_series,
)
from app.services.signal_ledger import resolve_fills
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyEntry
from app.services.strategy_result import (
    AMBIGUITY_ARMS,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    RESULT_NAMESPACES,
    RESULT_SCOPES,
    TOTAL_RETURN_BASIS,
    PromotionCandidate,
    ResultIdentity,
    StrategyResult,
    check_promotable,
    namespace_for_position,
)
from app.services.strategy_statistics import METRIC_SET_ID
from app.services.trial_register import TRIAL_REGISTER
from scripts.verify_2240_position_builder import UNIVERSE, _fills, _to_series
from scripts.verify_2240_statistics import (
    _AXIS_SQL,
    _benchmark_leg,
    _Sleeve,
)

#: ``--arm``'s default. ⚠ A DEFAULT, not a hardwiring — ``--strategy`` selects
#: any per-series manifest entry. S-1 is the default because it is the trial the
#: rest of phase 5 was measured on, so its figures are comparable with the
#: existing run reports.
DEFAULT_ARM_STRATEGY: Final = "s1-time-series-momentum"

_BAR_SPLIT_SQL = """
    SELECT
        count(*) FILTER (WHERE d.bar_date <  %(boundary)s) AS in_sample,
        count(*) FILTER (WHERE d.bar_date >= %(boundary)s) AS hold_out,
        count(DISTINCT s.instrument_id)                    AS instruments,
        min(d.bar_date)                                    AS first_bar,
        max(d.bar_date)                                    AS last_bar
    FROM research_price_series s
    JOIN research_price_daily d ON d.series_id = s.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date BETWEEN %(start)s AND %(end)s
"""

_SERIES_SIDE_SQL = """
    SELECT
        count(*) FILTER (WHERE has_in_sample AND NOT has_hold_out) AS in_sample_only,
        count(*) FILTER (WHERE has_hold_out AND NOT has_in_sample) AS hold_out_only,
        count(*) FILTER (WHERE has_in_sample AND has_hold_out)     AS both
    FROM (
        SELECT
            s.series_id,
            bool_or(d.bar_date <  %(boundary)s) AS has_in_sample,
            bool_or(d.bar_date >= %(boundary)s) AS has_hold_out
        FROM research_price_series s
        JOIN research_price_daily d ON d.series_id = s.series_id
        WHERE s.instrument_id = ANY(%(ids)s)
          AND d.bar_date BETWEEN %(start)s AND %(end)s
        GROUP BY s.series_id
    ) sides
"""


def population() -> int:
    """Arm 1 — the slice, and the frozen split re-derived at bar level."""
    print("\n[population] the corpus ∩ validated-universe slice §3.2 would evaluate", flush=True)
    print(f"  corpus version   {CORPUS_VERSION}", flush=True)
    print(f"  window           {EVALUATION_WINDOW_START} … {EVALUATION_WINDOW_END}", flush=True)
    print(f"  boundary         {HOLDOUT_BOUNDARY}   (FIRST hold-out bar)", flush=True)

    problems: list[str] = []
    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        params = {
            "ids": list(universe),
            "start": EVALUATION_WINDOW_START,
            "end": EVALUATION_WINDOW_END,
            "boundary": HOLDOUT_BOUNDARY,
        }
        row = conn.execute(_BAR_SPLIT_SQL, params).fetchone()
        assert row is not None
        in_sample, hold_out, instruments, first_bar, last_bar = row
        sides = conn.execute(_SERIES_SIDE_SQL, params).fetchone()
        assert sides is not None
        in_only, out_only, both = sides

    total = int(in_sample) + int(hold_out)
    print(f"  validated universe        {len(universe):>12,} instruments", flush=True)
    print(f"  of which the corpus holds {int(instruments):>12,}", flush=True)
    print(f"  bars in window            {total:>12,}   {first_bar} … {last_bar}", flush=True)
    print(f"    in-sample               {int(in_sample):>12,}   {100.0 * in_sample / max(total, 1):.2f}%", flush=True)
    print(f"    hold-out                {int(hold_out):>12,}   {100.0 * hold_out / max(total, 1):.2f}%", flush=True)
    series_total = int(in_only) + int(out_only) + int(both)
    print(f"  series with in-sample bars only  {int(in_only):>7,}", flush=True)
    print(f"  series with hold-out bars only   {int(out_only):>7,}", flush=True)
    print(f"  series spanning the boundary     {int(both):>7,}", flush=True)
    # ⚠ COMPUTED, never written into prose by hand. The two namespaces are over
    # different populations and this is the size of the difference — the number
    # `evaluated_instrument_count` and the gate's subset test differ by.
    print(
        f"  hold-out-only share              {100.0 * out_only / max(series_total, 1):>6.1f}%   "
        f"in-sample population ceiling {int(in_only) + int(both):,} of {series_total:,}",
        flush=True,
    )

    # ⚠ The split is re-derived and COMPARED against the frozen literal, never
    # re-fitted. §5.2: "a recomputed boundary walks forward silently and
    # re-admits hold-out data into training". A drift here is a corpus-version
    # event, so this arm FAILS rather than updating anything.
    share = 100.0 * in_sample / max(total, 1)
    if not 74.0 <= share <= 76.0:
        problems.append(
            f"the frozen boundary now splits the slice {share:.2f}/{100.0 - share:.2f} rather than 75/25 — "
            "the corpus moved under a frozen literal, which is a re-freeze event (§5.2), not a rounding"
        )
    # A series with no in-sample bars contributes to no in-sample result and is
    # not an error; it is reported because the evaluated_instrument_count the
    # gate reads differs per namespace and nothing else says so.
    if int(out_only) == 0 and int(in_only) == 0:
        print("  ⚠ every series spans the boundary — the two namespaces share one population", flush=True)

    for problem in problems:
        print(f"  *** {problem}", flush=True)
    return 1 if problems else 0


#: A calendar to hand ``decision_calendar`` so every entry's ``exit_regime``
#: constructs. ⚠ NOT cosmetic: ``ExitRegime`` refuses an EMPTY
#: ``rebalance_dates`` — "no calendar" and "a calendar with no dates" are kept
#: distinguishable — so S-2's factory raises on ``()``. One year of consecutive
#: days is enough for ``rebalance_dates`` to find its month boundaries, and the
#: regime SHAPE (which this script reads) does not depend on the dates in it.
_PROBE_CALENDAR: Final = tuple(
    date.fromordinal(n) for n in range(date(2020, 1, 1).toordinal(), date(2021, 1, 1).toordinal())
)


def _regime_for(entry: StrategyEntry) -> ExitRegime:
    return entry.exit_regime(entry.decision_calendar(_PROBE_CALENDAR))


def _demonstrate_level_refusal(entry: StrategyEntry) -> str | None:
    """Call ``build_positions`` on a level-based entry with no outcome.

    Returns the refusal message, or ``None`` if it did NOT refuse — which would
    mean §3.2's blocker has gone away and the spec is stale.

    ⚠ THE REFUSAL IS DEMONSTRATED, NOT QUOTED. ``position_builder``'s docstring
    says a level-based entry needs an outcome; a docstring cannot be wrong in
    CI, and the whole reason S-4 is excluded from §3.2 rests on this raise.
    """
    regime: ExitRegime = _regime_for(entry)
    if not regime.level_based:
        return None
    when = date(2020, 1, 2)
    later = date(2020, 1, 3)
    series = BarSeries(
        dates=(when, later),
        rows=(
            {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000},  # type: ignore[typeddict-item]
            {"open": 10.5, "high": 11.5, "low": 9.5, "close": 11.0, "volume": 1000},  # type: ignore[typeddict-item]
        ),
    )
    try:
        build_positions(
            strategy_id=entry.strategy_id,
            strategy_version="probe",
            entries=[
                EntryFill(
                    signal_id=1,
                    instrument_id=1,
                    signal_bar_date=when,
                    fill_bar_date=later,
                    fill_price=Decimal("10.5"),
                )
            ],
            exits=[],
            outcomes=[],
            # ⚠ A pin IS supplied: without one the raise would be the
            # missing-pin check one line earlier, which is a different defect.
            outcome_pin=_OUTCOME_PIN,
            series={1: series},
            regime=regime,
            window=Window(start=when, end=later),
        )
    except ValueError as exc:
        return str(exc)
    return None


#: The pin the level-based demonstration supplies. ⚠ REAL ``OutcomePin`` values,
#: not stand-ins: ``build_positions`` refuses a level-based regime with a null
#: pin one check EARLIER, so a stand-in that failed construction would produce
#: the wrong raise and the demonstration would prove something else.
_OUTCOME_PIN: Final = OutcomePin(
    rule_set_version=OUTCOME_RULE_SET_VERSION,
    input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
)


def runnable() -> int:
    """Arm 2 — which manifest strategies can produce a stored result today."""
    print("\n[runnable] which STRATEGY_MANIFEST entries can produce a result row", flush=True)
    problems: list[str] = []
    blocked: list[str] = []
    for strategy_id, entry in sorted(STRATEGY_MANIFEST.items()):
        regime = _regime_for(entry)
        refusal = _demonstrate_level_refusal(entry)
        state = "RUNNABLE"
        if regime.level_based:
            if refusal is None:
                problems.append(
                    f"{strategy_id} is level_based and build_positions did NOT refuse an entry with no outcome — "
                    "§3.2's exclusion of it rests on that refusal, so the spec is now stale"
                )
                state = "RUNNABLE (refusal gone — see above)"
            else:
                state = "BLOCKED"
                blocked.append(strategy_id)
        print(f"  {strategy_id:<38} {entry.strategy_class:<16} {state}", flush=True)
        print(
            f"      signal_pair={regime.signal_pair} level_based={regime.level_based} "
            f"max_hold_bars={regime.max_hold_bars} rebalance_dates="
            f"{'None' if regime.rebalance_dates is None else len(regime.rebalance_dates)}",
            flush=True,
        )
        if refusal is not None:
            print(f"      build_positions refuses: {refusal}", flush=True)

    print(f"\n  runnable {len(STRATEGY_MANIFEST) - len(blocked)} of {len(STRATEGY_MANIFEST)}", flush=True)
    if blocked:
        print(f"  blocked: {', '.join(blocked)} — every one of them level_based", flush=True)

    # ⚠ Criterion 6's `M` counts DECLARED trials, and a measured trial the
    # register does not declare under-counts the search and RAISES the DSR.
    # ``verify_2240_statistics`` guards that with a hand-written label→trial-id
    # map; this checks the property that makes such a map unnecessary — the
    # manifest key IS the register's trial id.
    print(f"\n  trial register {TRIAL_REGISTER.version}   M = {TRIAL_REGISTER.declared_count}", flush=True)
    undeclared = sorted(set(STRATEGY_MANIFEST) - TRIAL_REGISTER.trial_ids)
    if undeclared:
        problems.append(
            f"manifest strategies {undeclared} are not declared trials in {TRIAL_REGISTER.version} — their Sharpes "
            "would be measured but uncounted in M, which under-counts the search and raises the DSR"
        )
    else:
        print(f"  every one of the {len(STRATEGY_MANIFEST)} manifest ids is a declared trial id", flush=True)

    # ⚠ The blocker behind the level-based refusal, COUNTED rather than grepped
    # in prose. `outcome_resolver.resolve_outcome` needs an `ExitLevels`, and a
    # spec sentence saying "nothing in app/ constructs one" goes stale silently
    # the day something does.
    constructions = _exit_levels_constructions()
    for root in sorted(constructions):
        print(f"  ExitLevels( constructed in {root + '/':<9} {constructions[root]:>3} site(s)", flush=True)
    if constructions.get("app", 0):
        problems.append(
            f"{constructions['app']} ExitLevels( construction(s) now exist in app/ — S-4's blocker may have gone, "
            "and §3 of the spec has to be re-derived rather than trusted"
        )

    # ⚠⚠ THE AMBIGUITY-ARM CLAIM IS STRUCTURAL AND CHECKED HERE, not inferred
    # from one strategy's census. `position_builder` assigns
    # `close_source == "ambiguous"` only inside its `if regime.level_based:`
    # branch, so a runnable (non-level) strategy cannot produce one — which is
    # why the two ambiguity arms of a runnable strategy are one measurement
    # (§6). `--arm`'s close-source census corroborates it on real data; this is
    # what makes it true for the strategies `--arm` does not run.
    level_based_runnable = sorted(
        strategy_id
        for strategy_id, entry in STRATEGY_MANIFEST.items()
        if strategy_id not in blocked and _regime_for(entry).level_based
    )
    if level_based_runnable:
        problems.append(
            f"runnable strategies {level_based_runnable} are level_based — an `ambiguous` close is then reachable "
            "and the two ambiguity arms are NOT one measurement"
        )
    else:
        print(
            "  no runnable strategy is level_based → `ambiguous` is unreachable → the two ambiguity arms of "
            "every runnable strategy are one measurement",
            flush=True,
        )

    for problem in problems:
        print(f"  *** {problem}", flush=True)
    return 1 if problems else 0


def _exit_levels_constructions() -> dict[str, int]:
    """Count ``ExitLevels(`` construction sites per top-level directory.

    ⚠ Counted in Python rather than shelled out to ``grep``: a spec citing a
    grep command is evidence nothing runs, and this arm's exit code is what
    makes the claim hold run to run.
    """
    counts: dict[str, int] = {}
    for root in ("app", "scripts", "tests"):
        found = 0
        for path in Path(root).rglob("*.py"):
            found += path.read_text(encoding="utf-8").count("ExitLevels(")
        counts[root] = found
    return counts


def arms() -> int:
    """Arm 3 — how many result rows one "backtest everything" invocation writes."""
    print("\n[arms] the row count of one full invocation, from the manifest and the vocabularies", flush=True)
    runnable_ids = sorted(
        strategy_id for strategy_id, entry in STRATEGY_MANIFEST.items() if not _regime_for(entry).level_based
    )
    factors = (
        ("runnable strategies", len(runnable_ids), ", ".join(runnable_ids)),
        ("namespaces", len(RESULT_NAMESPACES), ", ".join(sorted(RESULT_NAMESPACES))),
        ("ambiguity arms", len(AMBIGUITY_ARMS), ", ".join(sorted(AMBIGUITY_ARMS))),
        ("quarantine arms", len(QUARANTINE_ARMS), ", ".join(sorted(QUARANTINE_ARMS))),
        ("result scopes", len(RESULT_SCOPES), ", ".join(sorted(RESULT_SCOPES))),
    )
    total = 1
    for label, count, members in factors:
        total *= count
        print(f"  {label:<22} {count:>3}   {members}", flush=True)
    print(f"  {'product':<22} {total:>3}   result rows per full invocation", flush=True)
    sleeve_only = total // len(RESULT_SCOPES)
    print(
        f"  {'sleeve scope only':<22} {sleeve_only:>3}   "
        "(portfolio scope needs a cross-strategy allocator that does not exist)",
        flush=True,
    )

    # ⚠⚠ THE INVOCATION UNIT IS THE STRATEGY SET, NOT ONE STRATEGY, and this is
    # what says so. Criterion 6's DSR deflates by `V[SR_n]` over the MEASURED
    # trials, so it does not exist below `MIN_MEASURED_TRIALS`. A per-strategy
    # invocation therefore cannot fill `deflated_sharpe`, and the gate refuses
    # every row it writes with `deflated_sharpe_not_computed`.
    print(
        f"\n  MIN_MEASURED_TRIALS      {MIN_MEASURED_TRIALS:>3}   "
        "V[SR_n] does not exist below it, so one strategy alone cannot produce a Deflated Sharpe",
        flush=True,
    )
    if len(runnable_ids) < MIN_MEASURED_TRIALS:
        print(
            f"  *** only {len(runnable_ids)} runnable strategies — below MIN_MEASURED_TRIALS, so NO invocation "
            "can produce a promotable row",
            flush=True,
        )
        return 1
    return 0


def arm(*, limit: int | None, strategy_id: str) -> int:
    """Arm 4 — one arm end-to-end over the whole corpus. A1-A4 in the header.

    ⚠ ONE STRATEGY PER INVOCATION, and which one is printed. Every figure this
    arm produces is a statement about the strategy named, not about the set:
    ``--arms`` supplies the multiplier and §6 of the spec supplies the
    structural argument for what generalises. A `cross_sectional` entry is
    refused here rather than silently mis-measured — its panel is resident, not
    streamed, so this loop is the wrong shape for it.
    """
    started = time.monotonic()
    entry = STRATEGY_MANIFEST[strategy_id]
    if entry.signals is None:
        raise RuntimeError(
            f"{strategy_id} is {entry.strategy_class}; this arm streams one series at a time and cannot "
            "measure a strategy whose decision needs the panel resident"
        )
    identity = entry.identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    regime = _regime_for(entry)
    window = Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)

    print(f"\n[arm] {strategy_id} {identity.version}", flush=True)
    print(f"      builder {BUILDER_RULE_SET_VERSION}", flush=True)
    print(
        f"      cost model {COST_MODEL_ID}   carry_unmodelled {CARRY_UNMODELLED}   fx_unmodelled {FX_UNMODELLED}",
        flush=True,
    )
    print(f"      sizing rule {SIZING_RULE_ID}   metric set {METRIC_SET_ID}", flush=True)
    print("      quarantine arm masked   ambiguity arm worst_case   scope sleeve", flush=True)

    load_s = 0.0
    evaluate_s = 0.0
    absorb_s = 0.0
    namespaces: Counter[str] = Counter()
    close_sources: Counter[str] = Counter()
    spanning = 0
    earliest_holdout_entry: date | None = None
    latest_in_sample_close: date | None = None
    in_sample_instruments: set[int] = set()
    problems: list[str] = []

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

        sleeve = _Sleeve(strategy_id)
        benchmark = LegBook()
        empty = 0

        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            mark = time.monotonic()
            masked = load_masked_series(conn, series_id)
            load_s += time.monotonic() - mark
            if not masked.bars:
                empty += 1
                continue
            series, _masked_opens = _to_series(masked.bars)
            indices = [axis_pos[when] for when in series.dates if when in axis_pos]
            if len(indices) < 2:
                empty += 1
                continue
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

            mark = time.monotonic()
            signals = entry.signals(series, universe=UNIVERSE, masked_reason="quarantined_bar")
            rows = resolve_fills(signals, series=series, identity=identity, instrument_id=int(instrument_id))
            entries, exits = _fills(rows, int(instrument_id))
            built = build_positions(
                strategy_id=entry.strategy_id,
                strategy_version=identity.version,
                entries=entries,
                exits=exits,
                outcomes=[],
                outcome_pin=None,
                series={int(instrument_id): series},
                regime=regime,
                window=window,
            )
            costed: list[CostedPosition] = list(cost_positions(built.positions, price_basis="split_adjusted"))
            evaluate_s += time.monotonic() - mark

            # A2 / A3 — the §5.2 partition and the close-source census, taken on
            # the positions the SINGLE pass produced.
            for row_costed in costed:
                position = row_costed.position
                side = namespace_for_position(position.entry_fill_bar_date, position.close_bar_date)
                namespaces[side] += 1
                close_sources[position.close_source or "open_at_window_end"] += 1
                # ⚠ The two extremes below are what decide the per-namespace
                # equity AXIS, which nothing in the tree has ever had to choose:
                # `verify_2240_holdout_namespace.py` counts this partition and
                # builds no curve from it. A spanning position belongs to the
                # hold-out (§5.2) and carries its true entry fill, so the
                # hold-out axis cannot start at the boundary — it has to reach
                # back to the earliest such entry, and how far back that is is
                # the number that says whether the construction is usable.
                if side == "hold_out":
                    if position.entry_fill_bar_date < HOLDOUT_BOUNDARY:
                        spanning += 1
                    if earliest_holdout_entry is None or position.entry_fill_bar_date < earliest_holdout_entry:
                        earliest_holdout_entry = position.entry_fill_bar_date
                else:
                    # ⚠ The IN-SAMPLE instrument set, kept separately because
                    # the gate's subset test is per namespace (§0: the two
                    # populations differ) and an all-instruments set would
                    # verify a claim this row does not make.
                    in_sample_instruments.add(int(instrument_id))
                    if position.close_bar_date is not None and (
                        latest_in_sample_close is None or position.close_bar_date > latest_in_sample_close
                    ):
                        latest_in_sample_close = position.close_bar_date

            mark = time.monotonic()
            sleeve.absorb(
                costed,
                series=series,
                window=window,
                axis_pos=axis_pos,
                closes=closes,
                first_axis_index=first_axis_index,
            )
            absorb_s += time.monotonic() - mark

            if n % 500 == 0:
                print(
                    f"  {n}/{len(pairs)} series, {sleeve.positions:,} positions ({time.monotonic() - started:.0f}s)",
                    flush=True,
                )

    corpus_s = time.monotonic() - started
    print(f"\n  series with usable bars  {len(pairs) - empty:,}   (fail-closed empties: {empty})", flush=True)

    from app.services.equity_curve import build_equity_curve  # noqa: PLC0415 — after the corpus pass

    benchmark_curve = build_equity_curve(benchmark, date_count=len(axis))
    metrics = sleeve.report(axis=axis, benchmark_curve=benchmark_curve)
    total_s = time.monotonic() - started

    # A1 — the cost, split by phase.
    print("\n  [A1] cost of ONE arm over the full corpus", flush=True)
    print(f"      corpus pass            {corpus_s:>10.1f}s", flush=True)
    print(f"        masked bar loading   {load_s:>10.1f}s   {100.0 * load_s / corpus_s:.1f}%", flush=True)
    print(f"        signals→positions    {evaluate_s:>10.1f}s   {100.0 * evaluate_s / corpus_s:.1f}%", flush=True)
    print(f"        curve accumulation   {absorb_s:>10.1f}s   {100.0 * absorb_s / corpus_s:.1f}%", flush=True)
    print(f"      curve + metrics        {total_s - corpus_s:>10.1f}s", flush=True)
    print(f"      TOTAL                  {total_s:>10.1f}s", flush=True)

    # A2 — the partition. This is the finding that decides whether the hold-out
    # arm is a second run or a filter over the same pass.
    positions_total = sum(namespaces.values())
    print("\n  [A2] the §5.2 namespace partition of ONE pass's positions", flush=True)
    for side, count in sorted(namespaces.items()):
        print(f"      {side:<12} {count:>10,}   {100.0 * count / max(positions_total, 1):.2f}%", flush=True)
    holdout_rows = namespaces.get("hold_out", 0)
    print(
        f"      of the hold-out rows, {spanning:,} entered BEFORE the boundary (spanning) — "
        f"{100.0 * spanning / max(holdout_rows, 1):.2f}%",
        flush=True,
    )
    print(f"      earliest hold-out entry   {earliest_holdout_entry}   (the hold-out curve's axis must reach it)")
    if earliest_holdout_entry is not None:
        lead = (HOLDOUT_BOUNDARY - earliest_holdout_entry).days
        print(
            f"      hold-out axis lead        {lead:,} calendar days before the boundary   "
            "(how far back the spanning legs force the axis)",
            flush=True,
        )
    print(f"      latest in-sample close    {latest_in_sample_close}   (boundary is {HOLDOUT_BOUNDARY})", flush=True)
    print(f"      instruments with >=1 in-sample position  {len(in_sample_instruments):,}", flush=True)
    if latest_in_sample_close is not None and latest_in_sample_close >= HOLDOUT_BOUNDARY:
        problems.append(
            f"an in-sample position closes {latest_in_sample_close}, on or after the boundary — "
            "namespace_for_position would have to have mis-classified it"
        )

    # A3 — the close sources. `ambiguous` is reachable only through an outcome,
    # and outcomes only for a level-based regime, so a non-level arm's two
    # ambiguity arms are the same measurement. This is what says so.
    print("\n  [A3] close-source census", flush=True)
    for source, count in close_sources.most_common():
        print(f"      {source:<22} {count:>10,}", flush=True)
    if close_sources.get("ambiguous"):
        problems.append(
            f"{close_sources['ambiguous']} positions closed 'ambiguous' on a non-level arm — the two ambiguity "
            "arms would then differ, and §3.2's claim that they cannot is falsified"
        )

    # A4 — the refusal list the operator would see on the row this job stores.
    if metrics is None:
        problems.append("the metric set did not construct, so no candidate could be gated")
    else:
        result = StrategyResult(
            identity=ResultIdentity(
                strategy_id=entry.strategy_id,
                strategy_version=identity.version,
                result_scope="sleeve",
                namespace="in_sample",
                ambiguity_arm="worst_case",
                quarantine_arm="masked",
                sizing_rule=SIZING_RULE_ID,
                benchmark_rule=BENCHMARK_RULE_ID,
                cost_model_id=COST_MODEL_ID,
                corpus_version=CORPUS_VERSION,
                window_start=EVALUATION_WINDOW_START,
                window_end=EVALUATION_WINDOW_END,
                position_rule_set_version=BUILDER_RULE_SET_VERSION,
                outcome_rule_set_version=_OUTCOME_PIN.rule_set_version,
                input_rule_set_version=_OUTCOME_PIN.input_rule_set_version,
                return_basis=TOTAL_RETURN_BASIS,
            ),
            purpose=entry.purpose,
            metrics=metrics,
            universe_basis=UNIVERSE,
            carry_unmodelled=CARRY_UNMODELLED,
            fx_unmodelled=FX_UNMODELLED,
            # ⚠⚠ THE WHOLE-CORPUS COUNT, matching the WHOLE-WINDOW curve the
            # metrics above came off — not the in-sample subset, which would
            # describe a population these metrics were not computed over. This
            # probe row is NOT a namespace arm; see the note below the census.
            evaluated_instrument_count=len(pairs) - empty,
        )
        candidate = PromotionCandidate(
            result=result,
            evaluated_instrument_ids=frozenset(int(row[0]) for row in pairs),
            validated_universe_ids=frozenset(universe),
        )
        refusals = check_promotable(candidate)
        print("\n  [A4] check_promotable on a WHOLE-WINDOW probe row", flush=True)
        for refusal in refusals:
            print(f"      {refusal}", flush=True)
        print(f"      {len(refusals)} refusals", flush=True)
        if not refusals:
            problems.append(
                "the bare row is PROMOTABLE — §3.2's whole argument is that it cannot be, so either the gate or "
                "this script is wrong"
            )

        # ⚠⚠ WHY A WHOLE-WINDOW ROW IS A VALID PROBE OF A LIST THE JOB WILL
        # PRODUCE PER NAMESPACE, and it is DEMONSTRATED rather than asserted.
        # This arm deliberately builds no namespace-scoped curve (spec §5: none
        # has ever been built, and choosing its axis is the implementation's
        # first decision), so the metric set above spans both namespaces while
        # the identity has to name one. That inconsistency would matter if the
        # gate read any metric VALUE. It reads exactly one metric, and only for
        # PRESENCE — so the refusal list cannot move with the metrics, only with
        # their absence. Blanking `effective_sample_size` must add that one code
        # and change nothing else; anything more means the gate consumes a
        # number this probe got from the wrong population.
        # ⚠ ALL NINE, not just `effective_sample_size`. `StrategyMetrics`
        # enforces criterion 3's block-bootstrap fields as one set — *"present
        # or absent as a whole"* — so blanking the single field the gate reads
        # raises at construction. Found by running this; the group is the unit.
        no_bootstrap = replace(
            metrics,
            effective_sample_size=None,
            expectancy_ci_low_pct=None,
            expectancy_ci_high_pct=None,
            bootstrap_block_length=None,
            bootstrap_cluster_count=None,
            bootstrap_resamples=None,
            bootstrap_seed=None,
            bootstrap_design_effect=None,
            bootstrap_model_id=None,
        )
        blanked = check_promotable(replace(candidate, result=replace(result, metrics=no_bootstrap)))
        added = set(blanked) - set(refusals)
        removed = set(refusals) - set(blanked)
        print(
            f"      metric-blanked probe adds {sorted(added)} and removes {sorted(removed)} — "
            "the gate reads one metric, for presence",
            flush=True,
        )
        if added != {"effective_sample_size_not_computed"} or removed:
            problems.append(
                f"blanking effective_sample_size moved the refusal list by {sorted(added)}/{sorted(removed)} rather "
                "than by exactly that one code — the gate reads a metric VALUE, so a whole-window probe cannot "
                "stand in for a namespace-scoped row and A4 has to build a real one"
            )

    for problem in sleeve.problems:
        print(f"  *** {problem}", flush=True)
    for problem in problems:
        print(f"  *** {problem}", flush=True)
    return 1 if (problems or sleeve.problems) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--population", action="store_true")
    parser.add_argument("--runnable", action="store_true")
    parser.add_argument("--arms", action="store_true")
    parser.add_argument("--arm", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument(
        "--strategy",
        default=DEFAULT_ARM_STRATEGY,
        choices=sorted(STRATEGY_MANIFEST),
        help="--arm only: which manifest strategy to measure. Cross-sectional entries are refused.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="⚠ --arm only, and the output says so: a limited run is NOT a full-population figure",
    )
    args = parser.parse_args()
    if not any((args.population, args.runnable, args.arms, args.arm, args.all)):
        parser.print_help()
        return 2

    status = 0
    if args.all or args.population:
        status |= population()
    if args.all or args.runnable:
        status |= runnable()
    if args.all or args.arms:
        status |= arms()
    if args.all or args.arm:
        status |= arm(limit=args.limit, strategy_id=args.strategy)
    return status


if __name__ == "__main__":
    sys.exit(main())
