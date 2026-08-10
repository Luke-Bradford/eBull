#!/usr/bin/env python3
"""Read-only development evaluation for preregistered candidate #2499.

The contaminated 2026 interval is intentionally inaccessible here.  This first cut
must survive both 2024 and 2025 development folds, masked/admitted quarantine
sensitivity and best/worst daily-bar ambiguity before a separately reviewed
one-read holdout command is added.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from dataclasses import asdict
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import psycopg

from app.config import settings
from app.services.block_bootstrap import block_bootstrap_expectancy, cluster_by_date
from app.services.indicator_series import BarSeries
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.price_segments import load_unresolved_breaks, series_segment_bounds
from app.services.research_comparator_snapshot import SNAPSHOT_ID, load_comparator_closes
from app.services.residual_confluence_candidate import CANDIDATE_VERSION
from app.services.residual_confluence_evaluation import (
    CandidateObservation,
    ExtractionCensus,
    evaluate_anchored_fold,
    extract_segment_observations,
)
from app.services.sector_classification import resolve_sector_spdr
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import CORPUS_VENDORS
from app.services.technical_analysis import OHLCVRow

QuarantineArm = Literal["masked", "admitted"]
AmbiguityArm = Literal["best_case", "worst_case"]
RECENT_START = date(2022, 4, 1)
DEVELOPMENT_END = date(2025, 12, 31)
BOOTSTRAP_SEED = 20260810

_SERIES_SQL = """
    SELECT s.series_id, s.instrument_id, p.sic
    FROM research_price_series s
    LEFT JOIN instrument_sec_profile p ON p.instrument_id = s.instrument_id
    WHERE s.vendor = %(vendor)s
      AND s.comparator_snapshot_id IS NULL
      AND s.instrument_id = ANY(%(instrument_ids)s)
    ORDER BY s.instrument_id, s.series_id
"""

_RECENT_SQL = """
    SELECT d.bar_date, d.open, d.high, d.low, d.close, d.volume,
           COALESCE(q.range_usable, TRUE), COALESCE(q.return_usable, TRUE)
    FROM research_price_daily d
    JOIN research_price_quarantine_coverage cov
      ON cov.series_id = d.series_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN research_bar_quarantine q
      ON q.series_id = d.series_id
     AND q.bar_date = d.bar_date
     AND q.rule_set_version = %(quarantine_version)s
    WHERE d.series_id = %(series_id)s
      AND d.bar_date >= %(recent_start)s
      AND d.bar_date <= %(frontier)s
    ORDER BY d.bar_date
"""


def _series(rows: list[tuple[Any, ...]], *, arm: QuarantineArm) -> BarSeries:
    dates: list[date] = []
    bars: list[OHLCVRow] = []
    admit = arm == "admitted"
    for bar_date, open_, high, low, close, volume, range_usable, return_usable in rows:
        dates.append(bar_date)
        bars.append(
            OHLCVRow(
                # Quarantine intentionally has no open-price verdict axis.
                # The shared masking contract retains positive opens and
                # masks non-positive opens by value (#2354).
                open=(Decimal(open_) if open_ is not None and (admit or open_ > 0) else None),  # type: ignore[typeddict-item]
                high=Decimal(high) if high is not None and (admit or range_usable) else None,  # type: ignore[typeddict-item]
                low=Decimal(low) if low is not None and (admit or range_usable) else None,  # type: ignore[typeddict-item]
                close=Decimal(close) if close is not None and (admit or return_usable) else None,  # type: ignore[typeddict-item]
                volume=None if volume is None else int(volume),
            )
        )
    return BarSeries(dates=tuple(dates), rows=tuple(bars))


def _add_census(left: ExtractionCensus, right: ExtractionCensus) -> ExtractionCensus:
    return ExtractionCensus(
        bars_seen=left.bars_seen + right.bars_seen,
        eligible_features=left.eligible_features + right.eligible_features,
        non_negative_shock=left.non_negative_shock + right.non_negative_shock,
        incomplete_outcome=left.incomplete_outcome + right.incomplete_outcome,
        uneconomic_bracket=left.uneconomic_bracket + right.uneconomic_bracket,
        observations=left.observations + right.observations,
    )


def _bootstrap_payload(returns: tuple[float, ...], dates: tuple[date, ...]) -> dict[str, Any] | None:
    result = block_bootstrap_expectancy(cluster_by_date(returns, dates), seed=BOOTSTRAP_SEED)
    return None if result is None else asdict(result)


def run(*, quarantine_arm: QuarantineArm) -> dict[str, Any]:
    started = time.monotonic()
    observations: list[CandidateObservation] = []
    census = ExtractionCensus()
    skipped_sector = 0
    empty_series = 0
    with psycopg.connect(
        settings.database_url,
        application_name="ebull-verify-2499",
        options="-c default_transaction_read_only=on",
    ) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        universe = load_validated_universe(conn)
        series_rows = conn.execute(
            _SERIES_SQL, {"vendor": CORPUS_VENDORS[0], "instrument_ids": list(universe)}
        ).fetchall()
        breaks = load_unresolved_breaks(conn, universe)
        market = load_comparator_closes(conn, snapshot_id=SNAPSHOT_ID, symbol="SPY", through_date=DEVELOPMENT_END)
        sectors = {
            symbol: load_comparator_closes(conn, snapshot_id=SNAPSHOT_ID, symbol=symbol, through_date=DEVELOPMENT_END)
            for symbol in ("XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY")
        }
        for position, (series_id, instrument_id, sic) in enumerate(series_rows, start=1):
            classification = resolve_sector_spdr(sic)
            if classification is None:
                skipped_sector += 1
                continue
            raw = conn.execute(
                _RECENT_SQL,
                {
                    "series_id": int(series_id),
                    "quarantine_version": QUARANTINE_RULE_SET_VERSION,
                    "recent_start": RECENT_START,
                    "frontier": DEVELOPMENT_END,
                },
            ).fetchall()
            if not raw:
                empty_series += 1
                continue
            loaded = _series(raw, arm=quarantine_arm)
            for start, end in series_segment_bounds(loaded, unresolved_breaks=breaks.get(int(instrument_id), ())):
                segment = BarSeries(dates=loaded.dates[start:end], rows=loaded.rows[start:end])
                extracted, segment_census = extract_segment_observations(
                    instrument_id=int(instrument_id),
                    series=segment,
                    market_closes=market,
                    sector_closes=sectors[classification.spdr_symbol],
                )
                observations.extend(extracted)
                census = _add_census(census, segment_census)
            if position == 1 or position % 100 == 0 or position == len(series_rows):
                print(
                    f"series={position}/{len(series_rows)} observations={len(observations)} "
                    f"elapsed={time.monotonic() - started:.1f}s",
                    file=sys.stderr,
                    flush=True,
                )

    folds: list[dict[str, Any]] = []
    population = tuple(observations)
    year_counts = Counter(item.signal_date.year for item in population)
    print(
        f"observation_years={dict(sorted(year_counts.items()))} "
        f"range={min((item.signal_date for item in population), default=None)}/"
        f"{max((item.signal_date for item in population), default=None)}",
        file=sys.stderr,
        flush=True,
    )
    for ambiguity_arm in ("best_case", "worst_case"):
        for test_start, test_end in (
            (date(2024, 1, 1), date(2024, 12, 31)),
            (date(2025, 1, 1), date(2025, 12, 31)),
        ):
            evaluation = evaluate_anchored_fold(
                population,
                test_start=test_start,
                test_end=test_end,
                ambiguity_arm=ambiguity_arm,
            )
            payload = asdict(evaluation)
            payload.pop("returns")
            payload.pop("entry_dates")
            payload["bootstrap"] = _bootstrap_payload(evaluation.returns, evaluation.entry_dates)
            folds.append(payload)
    return {
        "candidate_version": CANDIDATE_VERSION,
        "corpus_vendor": CORPUS_VENDORS[0],
        "comparator_snapshot": SNAPSHOT_ID,
        "quarantine_rule_set": QUARANTINE_RULE_SET_VERSION,
        "quarantine_arm": quarantine_arm,
        "development_end": DEVELOPMENT_END,
        "terminal_holdout_accessed_by_this_run": False,
        "series_count": len(series_rows),
        "skipped_unmapped_sector": skipped_sector,
        "empty_or_uncovered_series": empty_series,
        "extraction_census": asdict(census),
        "folds": folds,
        "elapsed_seconds": time.monotonic() - started,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarantine-arm", choices=("masked", "admitted"), required=True)
    args = parser.parse_args()
    print(json.dumps(run(quarantine_arm=args.quarantine_arm), sort_keys=True, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
