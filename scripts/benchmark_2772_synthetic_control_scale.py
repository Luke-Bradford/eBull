#!/usr/bin/env python3
"""Outcome-free multi-size scale curve for #2772's cohort engine.

The fixture is constructed, immutable and digest-bound.  Each case runs the
same member indices through the slow flat-mark reference and the production
shared-mark representation, refusing on any outcome mismatch while emitting
only operational measurements.  It reads no database and performs no strategy
selection, so running it cannot consume or expose a research trial.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final, Literal

import numpy as np

from app.services.equity_curve import build_equity_curve
from app.services.random_entry_cohort import member_seed
from app.services.strategy_statistics import DatedEquityCurve, TradeReturns, compute_metrics
from app.services.synthetic_control_run import (
    SeriesPlacement,
    _MemberInputs,
    _place_member,
    _place_member_compact,
)

BENCHMARK_ID: Final = "synthetic-control-scale-v1"
CASES: Final = (
    ("wiring", 8, 16, 1),
    ("small", 32, 64, 2),
    ("medium", 64, 256, 3),
    ("scale", 128, 1024, 3),
)
_START: Final = date(2010, 1, 4)


@dataclass(frozen=True)
class ScaleRecord:
    benchmark_id: str
    fixture_digest: str
    case: str
    engine: Literal["reference", "shared_marks"]
    query_count: int
    decoded_bars: int
    placement_series: int
    trades_per_member: int
    member_count: int
    wall_s: float
    cpu_s: float
    members_per_s: float
    peak_rss_bytes: int
    reference_equivalent: bool


def _axis(length: int) -> tuple[date, ...]:
    return tuple(_START + timedelta(days=index) for index in range(length))


def _fixture(*, series_count: int, trades_per_series: int) -> tuple[_MemberInputs, str, int]:
    bars = trades_per_series + 128
    axis = _axis(bars)
    panel = np.arange(bars, dtype=np.int64)
    placements: list[SeriesPlacement] = []
    digest = hashlib.sha256()
    digest.update(BENCHMARK_ID.encode())
    digest.update(np.asarray((series_count, trades_per_series, bars), dtype=np.int64).tobytes())
    for series_index in range(series_count):
        # Fixed gentle drift, unique by series but independent of any market.
        marks = np.asarray(
            [(50.0 + series_index) * (1.0 + 0.0001 * bar) for bar in range(bars)],
            dtype=np.float64,
        )
        holds = np.ones(trades_per_series, dtype=np.int64)
        placements.append(
            SeriesPlacement(
                panel=panel,
                adjusted_open=marks,
                holds=holds,
                marks=marks,
                marks_first=0,
            )
        )
        digest.update(marks.tobytes())
        digest.update(holds.tobytes())
    trades = series_count * trades_per_series
    return (
        _MemberInputs(
            placements=tuple(placements),
            axis=axis,
            benchmark=None,
            expected_trade_count=trades,
        ),
        digest.hexdigest(),
        bars,
    )


def _outcome(index: int, inputs: _MemberInputs, *, engine: Literal["reference", "shared_marks"]) -> object:
    rng = np.random.Generator(np.random.PCG64(member_seed(index)))
    place = _place_member if engine == "reference" else _place_member_compact
    book, returns, entries, exits = place(rng, inputs.placements, axis=inputs.axis)
    curve = build_equity_curve(book, date_count=len(inputs.axis))
    return compute_metrics(
        DatedEquityCurve(dates=inputs.axis, curve=curve),
        trades=TradeReturns(
            net_return_pct=tuple(returns),
            entry_fill_date=tuple(entries),
            exit_bar_date=tuple(exits),
            open_count=0,
            unpriced_count=0,
        ),
        buy_and_hold=None,
        bootstrap_seed=None,
    )


def _peak_rss_bytes() -> int:
    # macOS reports bytes; Linux reports KiB. The project runs this operational
    # benchmark on macOS, but keep the script honest when invoked in CI/Linux.
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if __import__("sys").platform == "darwin" else value * 1024


def run_case(case: str, series_count: int, trades_per_series: int, member_count: int) -> list[ScaleRecord]:
    inputs, fixture_digest, bars = _fixture(
        series_count=series_count,
        trades_per_series=trades_per_series,
    )
    outcomes: dict[str, tuple[object, ...]] = {}
    records: list[ScaleRecord] = []
    for engine in ("reference", "shared_marks"):
        started_wall = time.monotonic()
        started_cpu = time.process_time()
        measured = tuple(_outcome(index, inputs, engine=engine) for index in range(member_count))
        cpu_s = time.process_time() - started_cpu
        wall_s = time.monotonic() - started_wall
        outcomes[engine] = measured
        equivalent = engine == "reference" or measured == outcomes["reference"]
        if not equivalent:
            raise RuntimeError(f"{case}: shared-mark outcomes differ from the reference")
        records.append(
            ScaleRecord(
                benchmark_id=BENCHMARK_ID,
                fixture_digest=fixture_digest,
                case=case,
                engine=engine,
                query_count=0,
                decoded_bars=series_count * bars,
                placement_series=series_count,
                trades_per_member=inputs.expected_trade_count,
                member_count=member_count,
                wall_s=wall_s,
                cpu_s=cpu_s,
                members_per_s=member_count / wall_s,
                peak_rss_bytes=_peak_rss_bytes(),
                reference_equivalent=equivalent,
            )
        )
    return records


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    records = [record for case in CASES for record in run_case(*case)]
    payload = {
        "benchmark_id": BENCHMARK_ID,
        "outcome_fields_emitted": False,
        "cases": [asdict(record) for record in records],
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
