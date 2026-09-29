"""#3471 spec v6 §16.5 — the per-setup base-rate library (pre-trial TRAINING, not validation).

For every (setup_type, horizon) this measures, on stored daily bars, what the §16.5 library
plan (a structure stop and a structure target, derived exactly as the §16.3 guard derives
them) did after each first feasible firing of the setup: % stop / % target / % time, average
win and loss, mean net % and mean net R, per half. The guard (slice v6-3) refuses a setup
whose row is missing, below the minimums, or negative in either half.

Scope — stated here and in the output header:
- The rows are IN-SAMPLE and DESCRIPTIVE, over a 2023–26 window with a bull drift.
- Overlapping paths and several setups per name make entries dependent: no inference.
- The library's plan rule is deterministic and differs from the arm's own choices (r1-54);
  its universe is not the shortlist (r1-58).
- Run-vintage data: bars and break state as of the run, through the pack's own quarantine
  masking (``load_masked_bars``). Not point-in-time with respect to revisions, adjustments,
  quarantine or listing coverage (r1-60, r2-29..31). The source stream is bound by sha256.
- Entry is close(t); the trial enters at the ask at 15:00 on the next session (§8). That
  timing mismatch is labelled, not modelled.

Method (spec §16.5):
- Each name's bars (≤ ``DATA_END``) are split at its unresolved ``price_series_break`` rows;
  every window and walk stays inside one segment.
- At each bar t with ≥ 260 prior bars, the point-in-time universe is median close ≥ $3 and
  median close × volume ≥ $1M over the trailing 252 bars (exit_map's thresholds). Evaluating
  it before detection is equivalent to the spec's detect → universe order, because
  ``firings`` counts detections restricted to the universe.
- Levels and setups are the pack's own functions on the pack's own input: ``build_bar_series``
  over the 260 bars ending at t, ``indicators``, ``compute_levels``, ``detect_setups``,
  ``measure_atr``. Nothing is reimplemented, so there is no fast path to hold in parity. A
  window the pack would refuse (a quarantined bar) is not evaluated.
- Plan: ``ai_trial_plan.library_plan``; exits by the executor's ``protective_rates`` with
  ask = close(t).
- Walk t+1 … t+h stored bars: open ≤ stop or open ≥ target exits at the open; else low ≤
  stop exits at the stop (a same-bar touch of both is the stop); else high ≥ target exits at
  the target; else close(t+h). A missing OHLC inside the walk excludes the trade (truncated).
- Per horizon, in order: firing → plan (``no_valid_plan``) → completeness (``truncated`` when
  the segment ends before the half does, ``purged`` when the exit would cross the half's end)
  → dedupe (only the first firing in a run of consecutive firings that passes those steps is
  taken; the run resets at a half or segment boundary and on any bar where the setup is not
  detected or the name is outside the universe).
- Net = exit − entry − 0.30% of entry (tariff; spread excluded, r1-65).
  net R = net / (entry − stop).

Exactness: every per-trade figure is a ``Decimal`` quantized half-even to 1e-12 (the quotient
is taken at 60 significant digits first), and each mean is carried EXACTLY as the reduced
fraction of those quantized values (``mean_net_r``). A general rational mean has no finite
decimal expansion, so the reduced fraction is the exact form the §16.5 row contract asks for;
``*_display`` fields are rounded for reading only and the gate must never use them.

Run (background mode, output to a file, never piped):
    PYTHONPATH=. uv run python -m scripts.ai_trial_setup_base_rates > /tmp/3471-library.log 2>&1
It rewrites ``docs/proposals/execution/3471-setup-base-rates.json``.
"""

from __future__ import annotations

import hashlib
import json
import os
import statistics
import sys
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Final, Literal

from app.services.ai_trial_decision import AtrMeasurement, measure_atr
from app.services.ai_trial_levels import SETUP_TYPES, Level, compute_levels, detect_setups
from app.services.ai_trial_pack import INDICATOR_BARS, build_bar_series, indicators
from app.services.ai_trial_plan import library_plan
from app.services.indicator_series import BarSeries
from app.services.price_segments import series_segment_bounds
from app.services.strategy_paper_executor import protective_rates

HalfName = Literal["train", "holdout"]
Outcome = Literal["stop", "target", "time"]

HALVES: Final[tuple[tuple[HalfName, date, date], ...]] = (
    ("train", date(2023, 1, 1), date(2025, 6, 30)),
    ("holdout", date(2025, 7, 1), date(2026, 9, 25)),
)
DATA_END: Final = HALVES[-1][2]
HORIZONS: Final = (5, 10, 20)
MIN_PRIOR_BARS: Final = 260
UNIVERSE_WINDOW: Final = 252
MIN_MEDIAN_CLOSE: Final = Decimal(3)
MIN_MEDIAN_DOLLAR_VOLUME: Final = Decimal(1_000_000)
COST_ROUND_TRIP: Final = Decimal("0.003")
TRADE_QUANTUM: Final = Decimal("1e-12")
DISPLAY_DECIMALS: Final = 6
CAVEAT: Final = "in-sample, run-vintage, dependent entries; not a validated edge"
OUTPUT: Final = Path(__file__).resolve().parents[1] / "docs/proposals/execution/3471-setup-base-rates.json"

#: One bar as the script carries it: (open, high, low, close, volume), masked fields ``None``.
Bar = tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, int | None]


def half_of(day: date) -> tuple[HalfName, date] | None:
    """The half an entry dated ``day`` belongs to, with that half's end date."""
    for name, start, end in HALVES:
        if start <= day <= end:
            return name, end
    return None


def in_universe(bars: Sequence[Bar], t: int) -> bool:
    """§16.5 point-in-time universe at t: ≥ 260 prior bars; median close ≥ $3 and median
    close × volume ≥ $1M over the trailing 252 bars ending at t. Masked or missing values are
    skipped (exit_map's ``nanmedian``); an empty sample fails."""
    if t < MIN_PRIOR_BARS:
        return False
    window = bars[t - UNIVERSE_WINDOW + 1 : t + 1]
    closes = [b[3] for b in window if b[3] is not None]
    dollar = [b[3] * b[4] for b in window if b[3] is not None and b[4] is not None]
    if not closes or not dollar:
        return False
    return statistics.median(closes) >= MIN_MEDIAN_CLOSE and statistics.median(dollar) >= MIN_MEDIAN_DOLLAR_VOLUME


@dataclass(frozen=True)
class Evaluation:
    """The pack's view of one name at bar t: which setups fire, its levels, its ATR."""

    detected: frozenset[str]
    levels: Mapping[str, Level | None]
    atr: AtrMeasurement | None


def _row(bar: Bar) -> dict[str, Any]:
    return {"open": bar[0], "high": bar[1], "low": bar[2], "close": bar[3], "volume": bar[4]}


def evaluate_at(dates: Sequence[date], bars: Sequence[Bar], t: int) -> Evaluation | None:
    """The pack's functions over the pack's own 260-bar window ending at t; ``None`` where the
    pack would refuse the name (``build_bar_series`` returns a ``BarIncomplete`` reason)."""
    lo = t - INDICATOR_BARS + 1
    series = build_bar_series(dates[lo : t + 1], [_row(b) for b in bars[lo : t + 1]], last_session=dates[t])
    if not isinstance(series, BarSeries):
        return None
    ind = indicators(series)
    levels = compute_levels(series, indicators=ind)
    setups = detect_setups(series, levels=levels)
    return Evaluation(
        detected=frozenset(name for name, state in setups.items() if state.detected),
        levels=levels,
        atr=measure_atr(ind["atr14"], series.rows[-1]["close"]),
    )


def walk_exit(
    bars: Sequence[Bar], t: int, horizon: int, stop: Decimal, target: Decimal
) -> tuple[Outcome, Decimal] | None:
    """The §16.5 exit over bars t+1 … t+h; ``None`` when a bar in the walk has a missing
    OHLC. The caller guarantees t + h is inside the segment."""
    for i in range(t + 1, t + horizon + 1):
        o, h, lo, c, _ = bars[i]
        if o is None or h is None or lo is None or c is None:
            return None
        if o <= stop or o >= target:
            return ("stop" if o <= stop else "target"), o
        if lo <= stop:
            return "stop", stop
        if h >= target:
            return "target", target
    close = bars[t + horizon][3]
    assert close is not None
    return "time", close


def _q12(numerator: Decimal, denominator: Decimal) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 60
        return (numerator / denominator).quantize(TRADE_QUANTUM, rounding=ROUND_HALF_EVEN)


@dataclass(frozen=True)
class Trade:
    instrument_id: int
    entry_date: date
    outcome: Outcome
    net_pct: Decimal
    net_r: Decimal


@dataclass
class Cell:
    """One (setup, horizon, half) accumulator."""

    firings: int = 0
    no_valid_plan: int = 0
    truncated: int = 0
    purged: int = 0
    deduped: int = 0
    trades: list[Trade] = field(default_factory=list)

    def merge(self, other: Cell) -> None:
        self.firings += other.firings
        self.no_valid_plan += other.no_valid_plan
        self.truncated += other.truncated
        self.purged += other.purged
        self.deduped += other.deduped
        self.trades.extend(other.trades)


CellKey = tuple[str, int, HalfName]


def run_setup(
    instrument_id: int,
    dates: Sequence[date],
    bars: Sequence[Bar],
    states: Sequence[Evaluation | None],
    setup: str,
    horizon: int,
    cells: dict[CellKey, Cell],
) -> None:
    """One segment, one (setup, horizon): the §16.5 order of operations and run dedupe."""
    last = len(bars) - 1
    taken = False
    previous_half: HalfName | None = None
    for t, state in enumerate(states):
        half = half_of(dates[t])
        if half is None or half[0] != previous_half:
            taken = False
            previous_half = None if half is None else half[0]
        if half is None or state is None or setup not in state.detected:
            taken = False
            continue
        half_name, half_end = half
        cell = cells.setdefault((setup, horizon, half_name), Cell())
        cell.firings += 1
        plan = library_plan(state.levels, state.atr, horizon_days=horizon)
        if plan is None:
            cell.no_valid_plan += 1
            continue
        if t + horizon > last:
            if dates[last] < half_end:
                cell.truncated += 1
            else:
                cell.purged += 1
            continue
        if dates[t + horizon] > half_end:
            cell.purged += 1
            continue
        entry = bars[t][3]
        stop_pct, target_pct = plan.figures.stop_pct, plan.figures.target_pct
        assert entry is not None and stop_pct is not None and target_pct is not None
        stop, target = protective_rates(entry, stop_pct, target_pct)
        exit_ = walk_exit(bars, t, horizon, stop, target)
        if exit_ is None:
            cell.truncated += 1
            continue
        if taken:
            cell.deduped += 1
            continue
        taken = True
        outcome, price = exit_
        net = price - entry - COST_ROUND_TRIP * entry
        cell.trades.append(Trade(instrument_id, dates[t], outcome, _q12(100 * net, entry), _q12(net, entry - stop)))


def evaluate_segment(instrument_id: int, dates: Sequence[date], bars: Sequence[Bar]) -> tuple[dict[CellKey, Cell], int]:
    """Every (setup, horizon) over one price segment; also returns how many in-universe bars
    the pack would have refused (a quarantined bar in the window)."""
    states: list[Evaluation | None] = []
    refused = 0
    for t in range(len(bars)):
        if half_of(dates[t]) is None or not in_universe(bars, t):
            states.append(None)
            continue
        state = evaluate_at(dates, bars, t)
        refused += state is None
        states.append(state)
    cells: dict[CellKey, Cell] = {}
    for setup in SETUP_TYPES:
        for horizon in HORIZONS:
            run_setup(instrument_id, dates, bars, states, setup, horizon, cells)
    return cells, refused


def evaluate_instrument(
    instrument_id: int, dates: Sequence[date], bars: Sequence[Bar], breaks: Sequence[date]
) -> tuple[dict[CellKey, Cell], int]:
    """Split at the unresolved breaks, then evaluate each segment independently."""
    # Masked fields are ``None``, which ``OHLCVRow`` does not declare (see ``load_masked_bars``).
    series = BarSeries(dates=tuple(dates), rows=tuple(_row(b) for b in bars))  # type: ignore[arg-type]
    cells: dict[CellKey, Cell] = {}
    refused = 0
    for start, end in series_segment_bounds(series, unresolved_breaks=breaks):
        segment_cells, segment_refused = evaluate_segment(instrument_id, dates[start:end], bars[start:end])
        refused += segment_refused
        for key, cell in segment_cells.items():
            cells.setdefault(key, Cell()).merge(cell)
    return cells, refused


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def _display(value: Fraction) -> str:
    with localcontext() as ctx:
        ctx.prec = 60
        exact = Decimal(value.numerator) / Decimal(value.denominator)
        return str(exact.quantize(Decimal(1).scaleb(-DISPLAY_DECIMALS), rounding=ROUND_HALF_EVEN))


def _mean(values: Sequence[Decimal]) -> Fraction | None:
    return Fraction(sum(values, Decimal(0))) / len(values) if values else None


def _exact(value: Fraction | None) -> str | None:
    return None if value is None else f"{value.numerator}/{value.denominator}"


def _shown(value: Fraction | None) -> str | None:
    return None if value is None else _display(value)


def summarise(cell: Cell) -> dict[str, Any]:
    """The §16.5 statistics of one half. Every mean over an empty subset is ``null`` (r2-47)."""
    trades = cell.trades
    plans = len(trades)
    wins = [t.net_pct for t in trades if t.net_pct > 0]
    losses = [t.net_pct for t in trades if t.net_pct <= 0]

    def share(outcome: Outcome) -> str | None:
        return _shown(Fraction(100 * sum(t.outcome == outcome for t in trades), plans) if plans else None)

    mean_net_r = _mean([t.net_r for t in trades])
    mean_net_pct = _mean([t.net_pct for t in trades])
    return {
        "firings": cell.firings,
        "no_valid_plan": cell.no_valid_plan,
        "truncated": cell.truncated,
        "purged": cell.purged,
        "deduped": cell.deduped,
        "plans": plans,
        "n_names": len({t.instrument_id for t in trades}),
        "pct_stop": share("stop"),
        "pct_target": share("target"),
        "pct_time": share("time"),
        "avg_win_net_pct": _shown(_mean(wins)),
        "avg_loss_net_pct": _shown(_mean(losses)),
        "mean_net_pct": _exact(mean_net_pct),
        "mean_net_pct_display": _shown(mean_net_pct),
        "mean_net_r": _exact(mean_net_r),
        "mean_net_r_display": _shown(mean_net_r),
    }


def build_rows(cells: Mapping[CellKey, Cell]) -> list[dict[str, Any]]:
    """One row per (setup, horizon) carrying both halves (r2-43..45)."""
    return [
        {
            "setup_type": setup,
            "horizon_days": horizon,
            "halves": {name: summarise(cells.get((setup, horizon, name), Cell())) for name, _, _ in HALVES},
        }
        for setup in SETUP_TYPES
        for horizon in HORIZONS
    ]


# ---------------------------------------------------------------------------
# Data + driver
# ---------------------------------------------------------------------------
def _bar_line(instrument_id: int, day: date, bar: Bar) -> bytes:
    fields = [str(instrument_id), day.isoformat(), *("" if v is None else str(v) for v in bar)]
    return ("|".join(fields) + "\n").encode()


def _load(conn: Any) -> tuple[dict[int, tuple[list[date], list[Bar]]], dict[int, tuple[date, ...]], str, str, int]:
    from app.services.price_masked_bars import load_masked_bars
    from app.services.price_segments import load_unresolved_breaks

    ids = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT instrument_id FROM price_daily WHERE price_date <= %(end)s ORDER BY 1", {"end": DATA_END}
        ).fetchall()
    ]
    breaks = dict(load_unresolved_breaks(conn, ids))
    data: dict[int, tuple[list[date], list[Bar]]] = {}
    source = hashlib.sha256()
    n_bars = 0
    for instrument_id in ids:
        masked = load_masked_bars(conn, instrument_id).series
        dates: list[date] = []
        bars: list[Bar] = []
        for day, row in zip(masked.dates, masked.rows, strict=True):
            if day > DATA_END:
                break
            bar: Bar = (row["open"], row["high"], row["low"], row["close"], row["volume"])
            dates.append(day)
            bars.append(bar)
            source.update(_bar_line(instrument_id, day, bar))
        n_bars += len(bars)
        if len(bars) > MIN_PRIOR_BARS:
            data[instrument_id] = (dates, bars)
    if dict(load_unresolved_breaks(conn, ids)) != breaks:
        raise RuntimeError("unresolved price_series_break rows changed while the bars were read; re-run")
    break_hash = hashlib.sha256(
        "".join(f"{i}|{d.isoformat()}\n" for i in sorted(breaks) for d in breaks[i]).encode()
    ).hexdigest()
    return data, breaks, source.hexdigest(), break_hash, n_bars


def main() -> int:
    import psycopg

    from app.config import settings

    with psycopg.connect(settings.database_url) as conn:
        data, breaks, source_sha, break_sha, n_bars = _load(conn)
    print(f"loaded {len(data)} instruments with > {MIN_PRIOR_BARS} bars; {n_bars} bars read", flush=True)

    cells: dict[CellKey, Cell] = {}
    refused = 0
    workers = max(1, (os.cpu_count() or 2) - 2)
    with ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn")) as pool:
        futures = [
            pool.submit(evaluate_instrument, iid, dates, bars, breaks.get(iid, ()))
            for iid, (dates, bars) in data.items()
        ]
        for done, future in enumerate(futures, start=1):
            instrument_cells, instrument_refused = future.result()
            refused += instrument_refused
            for key, cell in instrument_cells.items():
                cells.setdefault(key, Cell()).merge(cell)
            if done % 500 == 0:
                print(f"{done}/{len(futures)} instruments", flush=True)

    document = {
        "spec": "docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md §16.5",
        "caveat": CAVEAT,
        "scope": [
            "In-sample and descriptive, over a 2023-26 window with a bull drift.",
            "Entries are dependent (overlapping paths, several setups per name); no inference is claimed.",
            "The library's deterministic plan rule differs from the arm's own choices; its universe is not "
            "the shortlist.",
            "Run-vintage data through the pack's quarantine masking: not point-in-time with respect to "
            "revisions, adjustments, quarantine or listing coverage.",
            "Entry is close(t); the trial enters at the ask at 15:00 on the next session.",
        ],
        "parameters": {
            "halves": {name: [start.isoformat(), end.isoformat()] for name, start, end in HALVES},
            "horizons_days": list(HORIZONS),
            "min_prior_bars": MIN_PRIOR_BARS,
            "universe_window_bars": UNIVERSE_WINDOW,
            "min_median_close_usd": str(MIN_MEDIAN_CLOSE),
            "min_median_dollar_volume_usd": str(MIN_MEDIAN_DOLLAR_VOLUME),
            "cost_round_trip_fraction": str(COST_ROUND_TRIP),
            "trade_quantum": str(TRADE_QUANTUM),
            "mean_encoding": "mean_net_r / mean_net_pct are exact reduced fractions 'p/q' of the "
            "per-trade values quantized half-even to trade_quantum; *_display fields are rounded for "
            "reading only",
        },
        "provenance": {
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "source_data_sha256": source_sha,
            "source_data_stream": "(instrument_id|price_date|open|high|low|close|volume) per bar, "
            "load_masked_bars order, masked fields empty, price_date <= data_end",
            "unresolved_breaks_sha256": break_sha,
            "data_end": DATA_END.isoformat(),
            "bars_read": n_bars,
            "instruments_evaluated": len(data),
            "in_universe_windows_refused_by_pack": refused,
        },
        "rows": build_rows(cells),
    }
    OUTPUT.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(f"wrote {OUTPUT}", flush=True)
    for row in document["rows"]:
        halves = row["halves"]
        print(
            f"{row['setup_type']:<26} {row['horizon_days']:>2}d "
            + " | ".join(
                f"{name}: n={halves[name]['plans']:>6} names={halves[name]['n_names']:>5} "
                f"R={halves[name]['mean_net_r_display']}"
                for name, _, _ in HALVES
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
