"""S-10 full-population census — both legs over the validated universe (#2437).

What it measures, per leg, on the same loader/masking/regime/segmentation the
production scan uses (streamed via ``segmented_member``, resolved via the
shared ``resolve_participating_bar`` — the census cannot drift from the scan
because they call the same functions):

- verdict/reason distribution over every bar of every loadable instrument;
- fired counts per year + distinct instruments;
- decision-date panel sizes and thin dates;
- **mandatory parity**: every below-SMA decision bar that survived refusal and
  thinness MUST have fired an exit — counted, not assumed (Codex ckpt-1).

Usage
-----
    uv run python -m scripts.verify_2437_s10_census --out /tmp/s10_census.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date

import psycopg

from app.config import settings
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.price_masked_bars import load_masked_bars
from app.services.price_segments import load_unresolved_breaks
from app.services.strategies.s10_relative_strength_leader import (
    MIN_CROSS_SECTION,
    s10_entry_select,
    s10_exit_select,
    s10_rebalance_dates,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_registry import SignalKind, StagedMember, resolve_participating_bar
from app.services.strategy_segmented_evaluation import segmented_member
from app.services.strategy_signal_scan import SCAN_UNIVERSE

S10 = "s10-relative-strength-leader"
SELECTS = {"entry": s10_entry_select, "exit": s10_exit_select}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    entry = STRATEGY_MANIFEST[S10]
    legs: tuple[SignalKind, ...] = ("entry", "exit")

    with psycopg.connect(settings.database_url) as conn:
        universe = sorted(load_validated_universe(conn))
        provider = MarketRegimeProvider.load(conn)
        breaks = load_unresolved_breaks(conn, universe)

        calendar: set[date] = set()
        loaded: dict[int, object] = {}
        skipped_short = 0
        for instrument_id in universe:
            series = load_masked_bars(conn, instrument_id).series
            if len(series) < 2:
                skipped_short += 1
                continue
            loaded[instrument_id] = series
            calendar.update(series.dates)

    panel_dates = s10_rebalance_dates(calendar)

    staged_by_leg: dict[SignalKind, dict[int, StagedMember]] = {leg: {} for leg in legs}
    dates_by_member: dict[int, tuple[date, ...]] = {}
    scores_by_leg: dict[SignalKind, dict[date, dict[int, float]]] = {leg: {} for leg in legs}
    for instrument_id, series in loaded.items():
        dates_by_member[instrument_id] = series.dates  # type: ignore[attr-defined]
        regime = provider.for_dates(series.dates)  # type: ignore[attr-defined]
        for leg in legs:
            staged = segmented_member(
                entry,
                series,  # type: ignore[arg-type]
                panel_decision_dates=panel_dates,
                universe=SCAN_UNIVERSE,
                masked_reason="quarantined_bar",
                unresolved_breaks=tuple(breaks.get(instrument_id, ())),
                regime=regime,
                leg=leg,
            )
            staged_by_leg[leg][instrument_id] = staged
            for when, value in staged.scores.items():
                scores_by_leg[leg].setdefault(when, {})[instrument_id] = value

    result: dict[str, object] = {
        "population": {
            "universe": len(universe),
            "loaded": len(loaded),
            "skipped_short": skipped_short,
            "rebalance_dates": len(panel_dates),
            "min_cross_section": MIN_CROSS_SECTION,
        }
    }
    for leg in legs:
        winners: dict[date, frozenset[int]] = {}
        thin: set[date] = set()
        panel_sizes: dict[str, int] = {}
        for when in sorted(scores_by_leg[leg]):
            at_date = scores_by_leg[leg][when]
            panel_sizes[when.isoformat()] = len(at_date)
            if len(at_date) < MIN_CROSS_SECTION:
                thin.add(when)
                continue
            selected = frozenset(SELECTS[leg](when, at_date))
            unknown = selected - at_date.keys()
            if unknown:
                raise RuntimeError(f"{leg} select named non-participants {sorted(unknown)[:5]} on {when}")
            winners[when] = selected

        verdicts: Counter[str] = Counter()
        fired_by_year: Counter[int] = Counter()
        fired_instruments: set[int] = set()
        mandatory_opportunities = mandatory_fired = 0
        for instrument_id, staged in staged_by_leg[leg].items():
            member_dates = dates_by_member[instrument_id]
            for index, staged_verdict in enumerate(staged.verdicts):
                if staged_verdict is not None:
                    key = staged_verdict.verdict
                    if staged_verdict.reason is not None:
                        key = f"{key}/{staged_verdict.reason}"
                    verdicts[key] += 1
                    continue
                when = member_dates[index]
                if when in thin:
                    verdicts["not_evaluable/thin_cross_section"] += 1
                    continue
                signal = resolve_participating_bar(
                    when=when,
                    index=index,
                    kind=leg,
                    selected=instrument_id in winners[when],
                    admissible_dates=staged.admissible_dates,
                    mandatory_dates=staged.mandatory_dates,
                )
                verdicts[signal.verdict] += 1
                is_mandatory = staged.mandatory_dates is not None and when in staged.mandatory_dates
                if is_mandatory:
                    mandatory_opportunities += 1
                    if signal.verdict == "fired":
                        mandatory_fired += 1
                if signal.verdict == "fired":
                    fired_by_year[when.year] += 1
                    fired_instruments.add(instrument_id)

        result[leg] = {
            "verdicts": dict(verdicts.most_common()),
            "fired_by_year": {str(year): count for year, count in sorted(fired_by_year.items())},
            "distinct_instruments_fired": len(fired_instruments),
            "thin_dates": sorted(when.isoformat() for when in thin),
            "panel_sizes": panel_sizes,
            "mandatory_opportunities": mandatory_opportunities,
            "mandatory_fired": mandatory_fired,
        }
        if mandatory_opportunities != mandatory_fired:
            raise RuntimeError(
                f"{leg}: {mandatory_opportunities} mandatory opportunities but {mandatory_fired} fired — "
                "the below-SMA exit is being suppressed somewhere"
            )

    with open(args.out, "w") as handle:
        json.dump(result, handle, indent=2)
    for leg in legs:
        stats = result[leg]
        print(f"{leg}: fired_by_year={stats['fired_by_year']} distinct={stats['distinct_instruments_fired']}")  # type: ignore[index]
        print(f"{leg}: verdicts={stats['verdicts']}")  # type: ignore[index]
    print(f"population={result['population']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
