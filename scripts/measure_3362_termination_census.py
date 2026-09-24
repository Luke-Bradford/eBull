"""#3362 acceptance 2 — full-population termination census and reader parity (read-only).

For every harvested ``icyDenev/Intrader`` series:

1. the evidence ``r6_exclusion_trial.load_series_evidence`` reads equals the
   ``TerminationEvidence`` ``universe_selection.load_universe_selection("survivorship_free")`` builds
   on every series it admits as terminating (the two spellings must not drift);
2. the class table and last-bar span counts of the spec's "Full-population verification";
3. with ``--mirror``, the harness-loaded first/last valid bar equals the stored ``first_bar`` /
   ``last_bar`` (the bound ``simulate_under_policies`` refuses on).

    PYTHONPATH=. uv run python -m scripts.measure_3362_termination_census [--mirror <Data/Day dir>]

Exits non-zero on any parity failure.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

import psycopg

from app.config import settings
from app.services.r6_exclusion_trial import load_series_evidence, read_price_series
from app.services.universe_selection import SURVIVORSHIP_FREE_VENDOR, load_universe_selection


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mirror", type=Path, help="Intrader Data/Day directory for the bar-bounds check")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        symbols = [
            str(row[0]).strip().upper()
            for row in conn.execute(
                "SELECT vendor_symbol FROM research_price_series WHERE vendor = %(v)s AND bar_count IS NOT NULL",
                {"v": SURVIVORSHIP_FREE_VENDOR},
            )
        ]
        evidence = load_series_evidence(conn, symbols=symbols)
        selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=frozenset())

    by_series = {item.series_id: item for item in evidence.values()}
    failures = 0
    classes: Counter[str] = Counter()
    spans: Counter[str] = Counter()
    linked_spans: Counter[str] = Counter()
    for admitted in selection.admitted:
        if admitted.termination is None:
            continue
        item = by_series[admitted.series_id]
        if item.evidence != admitted.termination or item.last_bar != admitted.last_bar:
            failures += 1
            print(f"PARITY FAIL series {admitted.series_id}: {item} vs {admitted}")
        classes[str(item.termination_class)] += 1
        span = (
            "before-2013" if item.last_bar.year < 2013 else "2013-2018" if item.last_bar.year <= 2018 else "2019-2024"
        )
        spans[span] += 1
        linked_spans[span] += item.evidence.linked
    print(f"harvested series: {len(evidence)}; terminating (admitted with evidence): {sum(classes.values())}")
    for name, count in classes.most_common():
        print(f"  {name}: {count}")
    for span in ("before-2013", "2013-2018", "2019-2024"):
        print(f"  last bar {span}: {spans[span]} terminations, {linked_spans[span]} Form-25-linked")
    print(f"reader parity failures: {failures}")

    if args.mirror is not None:
        bounds: Counter[str] = Counter()
        for symbol, item in evidence.items():
            series = read_price_series(args.mirror / f"{symbol}.csv")
            ok = (series.bars[0].day, series.bars[-1].day) == (item.first_bar, item.last_bar)
            bounds["match" if ok else "mismatch"] += 1
            if not ok:
                print(f"BOUNDS MISMATCH {symbol}: {series.bars[0].day}..{series.bars[-1].day} vs stored")
        print(f"bar bounds: {dict(bounds)}")
        failures += bounds["mismatch"]
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
