"""#3621 spec premises: how many names each avoidance filter would flag, measured on the stage-A panel.

Counts only. It reads the published stage-A artefact's admitted rows (ME, ``name_key``, ``series_id``, s(M)), its
frozen daily bars, first admitted bars and JKP NYSE size cutoffs. No holding return, holding status or month-(t+1)
price enters any figure: the MAX window ends at s(M) and the price and seasoning tests read s(M) only.

Per formation, per size segment (JKP NYSE breakpoints: micro < p20 <= small < p50 <= large < p80 <= mega) and for
the step-2 book universe (top 1,000 by ME, ties by ``name_key``), it counts admitted names and the names flagged by:

- ``max``: ``rmax1_21d`` (the largest daily total return over the 21 SPY sessions ending at s(M), at least 15
  returns, a return only between usable bars on adjacent sessions) at or above the top-decile breakpoint of all
  admitted names with a value at M (Bali, Cakici & Whitelaw 2011 sort all stocks). A window holding a return
  outside the panel's daily screen (below -90% or above +300%) has no value, as ``rvol_21d``. ``max_na`` counts
  admitted names without a value;
- ``sub5``: raw close at s(M) below $5 (``factor_book_path.PRICE_FLOOR``);
- ``young``: first admitted bar later than 36 months before s(M) (``factor_book_path.archive_seasoned``);
- ``any``: any of the three.

It prints min, median and max over the 80 stage-A formations, then the full per-formation table.

Usage: ``uv run python -m scripts.measure_3621_filter_premise [artefact_dir]``
"""

from __future__ import annotations

import gzip
import io
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

from app.services.factor_book_path import PRICE_FLOOR, archive_seasoned
from app.services.factor_panel import formation_months
from app.services.factor_panel_prices import RVOL_MIN_RETURNS, RVOL_SESSIONS, SCREEN_RETURN_HIGH, SCREEN_RETURN_LOW
from scripts.build_3609_factor_panel import read_verified_artefact
from scripts.measure_3609_step2_universe import BOOK_UNIVERSE_SIZE, DEFAULT_ARTEFACT, STAGE_A_MANIFEST_SHA256

CUTOFFS = "inputs/reference_snapshot_jkp_nyse_cutoffs.jsonl.gz"
DAILY = "inputs/daily.jsonl.gz"
FIRST_BARS = "inputs/first_bars.jsonl.gz"
SESSIONS = "inputs/spy_sessions.jsonl.gz"
SEGMENTS = ("micro", "small", "large", "mega", "top1000")
FLAGS = ("admitted", "max", "max_na", "sub5", "young", "any")
MAX_DECILE = 0.9


class MeasureError(RuntimeError):
    pass


def _lines(data: bytes) -> list[object]:
    with gzip.open(io.BytesIO(data), "rt") as f:
        return [json.loads(line) for line in f]


def _segment(me: float, p20: float, p50: float, p80: float) -> str:
    if me < p20:
        return "micro"
    if me < p50:
        return "small"
    if me < p80:
        return "large"
    return "mega"


def rmax(adj: dict[date, float], sessions: list[date], k: int) -> float | None:
    """Largest daily return over the ``RVOL_SESSIONS`` sessions ending at ``sessions[k]``; ``None`` when fewer than
    ``RVOL_MIN_RETURNS`` returns exist or any return in the window is outside the daily screen."""
    returns: list[float] = []
    for i in range(k - RVOL_SESSIONS + 1, k + 1):
        before, now = adj.get(sessions[i - 1]), adj.get(sessions[i])
        if before is None or now is None:
            continue
        r = now / before - 1.0
        if r < SCREEN_RETURN_LOW or r > SCREEN_RETURN_HIGH:
            return None
        returns.append(r)
    return max(returns) if len(returns) >= RVOL_MIN_RETURNS else None


def main(artefact: Path) -> None:
    verified = read_verified_artefact(artefact, STAGE_A_MANIFEST_SHA256, keep=[CUTOFFS, DAILY, FIRST_BARS, SESSIONS])
    if verified.manifest.get("stage") != "A":
        raise MeasureError(f"{artefact} is not a stage-A artefact")
    grid = [m.isoformat() for m in formation_months()]

    cutoffs: dict[tuple[str, str], float] = {}
    for key, month, value, *_ in _lines(verified.files[CUTOFFS]):  # type: ignore[misc]
        if key in ("nyse_p20", "nyse_p50", "nyse_p80"):
            cutoffs[(key, month)] = float(value) * 1e6
    sessions = [date.fromisoformat(d) for d in _lines(verified.files[SESSIONS])]  # type: ignore[arg-type]
    first_bar = {sid: date.fromisoformat(day) for sid, day in _lines(verified.files[FIRST_BARS])}  # type: ignore[misc]

    rows: dict[str, list[tuple[float, int, int, date]]] = defaultdict(list)
    with gzip.open(io.BytesIO(verified.rows), "rt") as f:
        for line in f:
            row = json.loads(line)
            if row["exclusion"] is None:
                me = float(row["me"]["value"])
                if not (math.isfinite(me) and me > 0):
                    raise MeasureError(f"ME not finite and positive: {row['M']} {row['name_key']}")
                rows[row["M"]].append((me, row["name_key"], row["series_id"], date.fromisoformat(row["s_M"])))
    if sorted(rows) != grid:
        raise MeasureError("admitted months are not the stage-A grid")
    wanted = {sid for month in rows.values() for _, _, sid, _ in month}
    decision_days = {r[3] for month in rows.values() for r in month}
    position = {d: i for i, d in enumerate(sessions)}
    for day in decision_days:
        if position.get(day, -1) < RVOL_SESSIONS:
            raise MeasureError(f"s(M) {day} is not a loaded SPY session with a full window")

    # One pass over the daily bars: each series' raw close at every decision session and rmax1_21d there.
    close_at: dict[tuple[int, date], float] = {}
    max_at: dict[tuple[int, date], float] = {}
    with gzip.open(io.BytesIO(verified.files[DAILY]), "rt") as f:
        for line in f:
            sid, bars = json.loads(line)
            if sid not in wanted:
                continue
            adj: dict[date, float] = {}
            close: dict[date, float] = {}
            for day, raw, adjusted, _volume, _stamped, usable in bars:
                if usable:
                    d = date.fromisoformat(day)
                    adj[d], close[d] = adjusted, raw
            for day in decision_days:
                if day in close:
                    close_at[(sid, day)] = close[day]
                value = rmax(adj, sessions, position[day])
                if value is not None:
                    max_at[(sid, day)] = value

    table: list[dict[str, dict[str, int]]] = []
    for month in grid:
        names = sorted(rows[month], key=lambda r: (-r[0], r[1]))
        if len(names) < BOOK_UNIVERSE_SIZE:
            raise MeasureError(f"{month}: fewer than {BOOK_UNIVERSE_SIZE} admitted names")
        top = {r[1] for r in names[:BOOK_UNIVERSE_SIZE]}
        p20, p50, p80 = (cutoffs[(k, month)] for k in ("nyse_p20", "nyse_p50", "nyse_p80"))
        values = sorted(max_at[(sid, s)] for _, _, sid, s in names if (sid, s) in max_at)
        breakpoint = values[math.ceil(MAX_DECILE * len(values)) - 1]
        counts: dict[str, dict[str, int]] = {seg: dict.fromkeys(FLAGS, 0) for seg in SEGMENTS}
        for me, key, sid, s in names:
            if (sid, s) not in close_at:
                raise MeasureError(f"{month}: admitted series {sid} has no usable bar at s(M) {s}")
            if sid not in first_bar:
                raise MeasureError(f"{month}: admitted series {sid} has no first admitted bar")
            value = max_at.get((sid, s))
            flags = {
                "admitted": True,
                "max": value is not None and value >= breakpoint,
                "max_na": value is None,
                "sub5": close_at[(sid, s)] < PRICE_FLOOR,
                "young": not archive_seasoned(first_bar[sid], s),
            }
            flags["any"] = flags["max"] or flags["sub5"] or flags["young"]
            for seg in (_segment(me, p20, p50, p80), *(("top1000",) if key in top else ())):
                for flag, hit in flags.items():
                    counts[seg][flag] += int(hit)
        table.append(counts)

    print("segment", "flag", "min", "median", "max", sep="\t")
    for seg in SEGMENTS:
        for flag in FLAGS:
            series = [t[seg][flag] for t in table]
            print(seg, flag, min(series), statistics.median(series), max(series), sep="\t")
    print()
    print("M", *(f"{seg}:{flag}" for seg in SEGMENTS for flag in FLAGS), sep="\t")
    for month, counts in zip(grid, table, strict=True):
        print(month, *(counts[seg][flag] for seg in SEGMENTS for flag in FLAGS), sep="\t")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ARTEFACT)
