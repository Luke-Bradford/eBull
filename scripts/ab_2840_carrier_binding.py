"""#2840 §8 obligation (b) — does the new carrier binding EVER fire on production data?

Read-only. Writes nothing, takes no lock, runs inside one ``REPEATABLE READ``
transaction so every instrument is read against one snapshot.

WHAT THIS IS, AND WHAT IT DELIBERATELY IS NOT
---------------------------------------------
The binding's only new behaviour is a **raise**. So "verdicts are unchanged"
decomposes into three separately checkable claims, and this script is the one of
them that needs the corpus:

a. the guard never fires on production data — **this script**;
b. the constructors' observable output (``values`` / ``not_evaluable_indices``)
   is unchanged — ``tests/test_2840_price_basis_carrier.py::
   test_the_constructors_observable_output_is_unchanged_by_the_binding``, an
   exhaustive table over every ``sql/249`` member and n ∈ {0, 1, 2, 175};
c. nothing else reads ``bar_bindings`` — a repo-wide ``rg``, published on the PR.

⚠⚠ IT IS NOT A VERDICT-LEVEL A/B, and the earlier drafts that claimed to be one
were refused at checkpoint 1 for concrete reasons worth keeping: the baseline
commit has no ``binding_mismatch`` to time, ``except TypeError`` is an unsafe
signature discriminator, two invocations of a script in two worktrees do not
share a snapshot, ``segmented_member`` emits ``None`` for unresolved candidates
so its tuple set is incomplete, and a SET comparison hides duplicate-output
regressions. Claiming an A/B that cannot hold its arms still is worse than
stating the reduction.

⚠ It does not reproduce ``backtest_run._resolve_liquidity_policy``. The certified
route here is driven with the literal ``"unadjusted"``, which is what a pinned
archive resolves to; a run whose policy is WITHHELD takes the refusing branch and
is covered by the undeclared route below.

Refs #2840, #2437.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date

import psycopg

from app.config import settings
from app.services.indicator_series import BarSeries
from app.services.price_masked_bars import load_bar_spans, load_masked_bars
from app.services.price_segments import load_unresolved_breaks, series_segment_bounds
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_price_basis import PriceBasisSeries, from_archive_basis, from_undeclared_source
from app.services.strategy_signal_scan import choose_frontier

logger = logging.getLogger("ab_2840_carrier_binding")

#: The two production routes, named the way the call sites name them.
_ROUTES: dict[str, object] = {
    "undeclared": None,
    "pinned_archive_unadjusted": "unadjusted",
}


@dataclass
class _Counts:
    """⚠ Every denominator this run could quote, so a zero cannot be vacuous.

    A mismatch count of 0 means nothing without the number of carriers that were
    actually built and bars that were actually bound — the failure mode the
    checkpoint called out is a sweep that skipped its population and reported a
    clean zero.
    """

    universe: int = 0
    with_spans: int = 0
    eligible: int = 0
    loaded: int = 0
    skipped_short: int = 0
    carriers: int = 0
    bars_bound: int = 0
    segments: int = 0
    mismatches: int = 0
    examples: list[str] = field(default_factory=list)


def _check(carrier: PriceBasisSeries, series: BarSeries, counts: _Counts, *, label: str) -> None:
    counts.carriers += 1
    counts.bars_bound += len(carrier.bar_bindings)
    if (mismatch := carrier.binding_mismatch(series)) is not None:
        counts.mismatches += 1
        if len(counts.examples) < 5:
            counts.examples.append(f"{label}: {mismatch}")


def _check_segments(
    carrier: PriceBasisSeries,
    series: BarSeries,
    breaks: Sequence[date],
    counts: _Counts,
    *,
    label: str,
) -> None:
    """⚠ The segment path is checked too, because that is the SHAPE the scan uses.

    ``segmented_signals`` slices the carrier per price-scale segment and hands each
    slice to a freshly built ``BarSeries``. A binding that were not remapped with
    the indices would bind the wrong bars — silently, since every length still
    agrees.
    """
    for start, end in series_segment_bounds(series, unresolved_breaks=breaks):
        counts.segments += 1
        segment = BarSeries(dates=series.dates[start:end], rows=series.rows[start:end])
        if (mismatch := carrier.segment(start, end).binding_mismatch(segment)) is not None:
            counts.mismatches += 1
            if len(counts.examples) < 5:
                counts.examples.append(f"{label}[{start}:{end}]: {mismatch}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=0, help="stop after N instruments (0 = the whole population)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    per_route = {route: _Counts() for route in _ROUTES}
    with psycopg.connect(settings.database_url) as conn:
        # ⚠ REPEATABLE READ, not merely "one transaction": under READ COMMITTED
        # the span read and each per-instrument bar read are separate snapshots,
        # so a concurrent daily_candle_refresh could make the population and the
        # bars disagree — and a binding mismatch caused by that would be reported
        # as a finding about this diff.
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        universe = load_validated_universe(conn)
        spans = load_bar_spans(conn, universe)
        frontier = choose_frontier({instrument_id: span.last_bar for instrument_id, span in spans.items()})
        if frontier is None:
            logger.error("no loadable instruments in a %d-member universe", len(universe))
            return 2
        eligible = sorted(
            instrument_id
            for instrument_id, span in spans.items()
            if span.last_bar == frontier.bar_date and span.bars >= 2
        )
        if args.limit:
            eligible = eligible[: args.limit]
        breaks = load_unresolved_breaks(conn, eligible)

        for counts in per_route.values():
            counts.universe = len(universe)
            counts.with_spans = len(spans)
            counts.eligible = len(eligible)

        for seen, instrument_id in enumerate(eligible, start=1):
            series = load_masked_bars(conn, instrument_id).series
            if len(series) < 2:
                for counts in per_route.values():
                    counts.skipped_short += 1
                continue
            instrument_breaks = tuple(breaks.get(instrument_id, ()))
            for route, basis in _ROUTES.items():
                counts = per_route[route]
                counts.loaded += 1
                carrier = (
                    from_undeclared_source(series=series)
                    if basis is None
                    else from_archive_basis(str(basis), series=series)
                )
                label = f"{route}/instrument {instrument_id}"
                _check(carrier, series, counts, label=label)
                _check_segments(carrier, series, instrument_breaks, counts, label=label)
            if seen % 500 == 0:
                logger.info("… %d/%d instruments", seen, len(eligible))

    verdicts: Counter[str] = Counter()
    for route, counts in per_route.items():
        logger.info(
            "%-26s universe=%d spans=%d eligible=%d loaded=%d skipped_short=%d "
            "carriers=%d bars_bound=%d segments=%d MISMATCHES=%d",
            route,
            counts.universe,
            counts.with_spans,
            counts.eligible,
            counts.loaded,
            counts.skipped_short,
            counts.carriers,
            counts.bars_bound,
            counts.segments,
            counts.mismatches,
        )
        for example in counts.examples:
            logger.info("    %s", example)
        verdicts[route] = counts.mismatches

    # ⚠ Non-zero exit on ANY mismatch. A mismatch on production data is a finding
    # that stops the round, not a tolerance to tune.
    return 1 if sum(verdicts.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
