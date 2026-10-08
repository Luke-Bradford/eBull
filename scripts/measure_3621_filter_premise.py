"""#3621 spec premises 1 and 2: where the avoidance filters bite on the stage-A panel, and JKP's published series.

Premise 1 is counts only. It reads the published stage-A artefact's admitted rows (ME, ``name_key``, ``series_id``,
s(M)), its frozen daily bars, first admitted bars and JKP NYSE size cutoffs. No holding return, holding status or
month-(t+1) price enters any figure: the MAX window ends at s(M) and the price and seasoning tests read s(M) only.

Per formation, per population (JKP NYSE size segments micro < p20 <= small < p50 <= large < p80 <= mega; step 2's
book universe ``top1000``, the top 1,000 by ME with ties by ``name_key``; and ``rest``, every other admitted name),
it counts admitted names and the names flagged by:

- ``max``: ``rmax1_21d`` at or above the top-decile cutoff, or a screened window (below);
- ``max_screened``: the window holds a return outside the extreme-return screen (below -90% or above +300%) or an
  unstamped ``adj_close / close`` ratio move above 50%, the two screens of ``factor_panel_prices.series_prices``;
- ``max_short``: fewer than 15 daily returns in the window (not flagged);
- ``max_zero_heavy``: 10 or more zero daily returns in the window (not flagged);
- ``sub5``: raw close at s(M) below $5 (``factor_book_path.PRICE_FLOOR``);
- ``young``: first admitted bar later than 36 months before s(M) (``factor_book_path.archive_seasoned``);
- ``any``: any of ``max``, ``sub5``, ``young``.

``rmax1_21d`` follows JKP (``bkelly-lab/ReplicationCrisis`` at ``67174c7f``: ``GlobalFactors/main.sas`` calls
``roll_apply_daily`` with ``__n=1, __min=15`` for the ``_21d`` set; ``GlobalFactors/market_chars.sas``
``roll_apply_daily`` takes ``max(ret)`` over the stock-month's daily returns and drops stock-months with
``zero_obs >= 10``): the largest daily total return over the SPY sessions of s(M)'s calendar month up to s(M), at
least 15 returns, fewer than 10 of them zero. A return exists only between usable bars on adjacent sessions. The
cutoff q is the ``ceil(0.9 N)``-th smallest of the N valid values at M over all admitted names (1-based); a name is
flagged when its value is >= q.

Premise 2 reads the artefact's frozen JKP US monthly capped-value-weight factor returns for ``rmax1_21d``, ``age``
and ``prc``. JKP publishes them already signed (a positive return is the direction JKP Table 9 expects), so no sign
is applied here. For each stated window it prints months, mean, sample sd, annualised IR (mean / sd x sqrt 12) and
the plain t (mean / (sd / sqrt n)).

Usage: ``uv run python -m scripts.measure_3621_filter_premise [artefact_dir]``
"""

from __future__ import annotations

import gzip
import io
import json
import math
import statistics
import sys
from bisect import bisect_left
from collections import defaultdict
from datetime import date
from pathlib import Path

from app.services.factor_book_path import PRICE_FLOOR, archive_seasoned
from app.services.factor_panel import formation_months
from app.services.factor_panel_prices import SCREEN_RATIO_MOVE, SCREEN_RETURN_HIGH, SCREEN_RETURN_LOW
from scripts.build_3609_factor_panel import read_verified_artefact
from scripts.measure_3609_step2_universe import BOOK_UNIVERSE_SIZE, DEFAULT_ARTEFACT, STAGE_A_MANIFEST_SHA256

CUTOFFS = "inputs/reference_snapshot_jkp_nyse_cutoffs.jsonl.gz"
DAILY = "inputs/daily.jsonl.gz"
FIRST_BARS = "inputs/first_bars.jsonl.gz"
SESSIONS = "inputs/spy_sessions.jsonl.gz"
JKP = "inputs/reference_snapshot_jkp_usa_monthly_vw_cap.jsonl.gz"
POPULATIONS = ("micro", "small", "large", "mega", "top1000", "rest")
FLAGS = ("admitted", "max", "max_screened", "max_short", "max_zero_heavy", "sub5", "young", "any")
MAX_DECILE = 0.9
#: JKP ``main.sas`` ``roll_apply_daily(... __n=1, __min=15 ...)`` and ``market_chars.sas`` ``zero_obs < 10``.
MAX_MIN_RETURNS = 15
MAX_ZERO_RETURNS = 10
JKP_WINDOWS = (
    ("rmax1_21d", "1926-01-01", "2014-09-30"),
    ("rmax1_21d", "2011-02-01", "2014-09-30"),
    ("age", "1926-01-01", "2014-09-30"),
    ("prc", "1926-01-01", "2014-09-30"),
    ("prc", "1990-01-01", "2014-09-30"),
)


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


def window_sessions(sessions: list[date], k: int) -> range:
    """Indices of the SPY sessions in s(M)'s calendar month up to s(M) = ``sessions[k]`` (JKP ``__n=1``)."""
    first = k
    while first > 0 and (sessions[first - 1].year, sessions[first - 1].month) == (sessions[k].year, sessions[k].month):
        first -= 1
    return range(first, k + 1)


class SeriesBars:
    """One series' usable session bars and stamp positions, indexed by SPY session."""

    def __init__(self, bars: list[tuple[date, float, float, bool, bool]], sessions: list[date]) -> None:
        position = {d: i for i, d in enumerate(sessions)}
        self.stamps = sorted(bisect_left(sessions, d) for d, _c, _a, stamped, _ok in bars if stamped)
        admitted = [(position[d], c, a) for d, c, a, _st, ok in bars if ok and d in position]
        self.index = [i for i, _c, _a in admitted]
        self.close = [c for _i, c, _a in admitted]
        self.adj = [a for _i, _c, a in admitted]

    def stamped_between(self, i0: int, i1: int) -> bool:
        """A stamp at a session index in (i0, i1]."""
        j = bisect_left(self.stamps, i0 + 1)
        return j < len(self.stamps) and self.stamps[j] <= i1


def rmax(series: SeriesBars, sessions: list[date], k: int) -> float | str:
    """``rmax1_21d`` at s(M) = ``sessions[k]``, or ``"screened"`` / ``"short"`` / ``"zero_heavy"``.

    A daily return exists only between usable bars on adjacent SPY sessions. Screens, as
    ``factor_panel_prices.series_prices``: a return below ``SCREEN_RETURN_LOW`` or above ``SCREEN_RETURN_HIGH``, or an
    ``adj_close / close`` move above ``SCREEN_RATIO_MOVE`` between consecutive usable session bars with no stamp in
    between, screens the window when the pair's later bar is inside it."""
    window = window_sessions(sessions, k)
    first = bisect_left(series.index, window.start)
    last = bisect_left(series.index, k + 1)
    returns: list[float] = []
    zero = 0
    for j in range(max(first, 1), last):
        i0, i1 = series.index[j - 1], series.index[j]
        ratio0 = series.adj[j - 1] / series.close[j - 1]
        ratio1 = series.adj[j] / series.close[j]
        if not series.stamped_between(i0, i1) and abs(math.log(ratio1 / ratio0)) > math.log(1.0 + SCREEN_RATIO_MOVE):
            return "screened"
        if i1 != i0 + 1:
            continue
        r = series.adj[j] / series.adj[j - 1] - 1.0
        if r < SCREEN_RETURN_LOW or r > SCREEN_RETURN_HIGH:
            return "screened"
        returns.append(r)
        zero += r == 0.0
    if len(returns) < MAX_MIN_RETURNS:
        return "short"
    if zero >= MAX_ZERO_RETURNS:
        return "zero_heavy"
    return max(returns)


def jkp_premise(data: bytes) -> None:
    series: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for name, month, value, *_ in _lines(data):  # type: ignore[misc]
        series[name].append((month, float(value)))
    print("factor", "from", "to", "months", "mean", "sd", "ann_IR", "t", sep="\t")
    for name, low, high in JKP_WINDOWS:
        r = [v for m, v in series[name] if low <= m <= high]
        mean, sd = statistics.mean(r), statistics.stdev(r)
        ir, t = mean / sd * math.sqrt(12), mean / (sd / math.sqrt(len(r)))
        print(name, low, high, len(r), f"{mean:.5f}", f"{sd:.5f}", f"{ir:.3f}", f"{t:.2f}", sep="\t")
    for name in ("rmax1_21d", "age", "prc"):
        print(f"{name}\tfirst {series[name][0][0]}\tlast {series[name][-1][0]}")


def main(artefact: Path) -> None:
    verified = read_verified_artefact(
        artefact, STAGE_A_MANIFEST_SHA256, keep=[CUTOFFS, DAILY, FIRST_BARS, SESSIONS, JKP]
    )
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
        if position.get(day, -1) < 31:
            raise MeasureError(f"s(M) {day} is not a loaded SPY session with a full window")

    # One pass over the daily bars: each series' raw close at every decision session and rmax1_21d there.
    close_at: dict[tuple[int, date], float] = {}
    max_at: dict[tuple[int, date], float | str] = {}
    with gzip.open(io.BytesIO(verified.files[DAILY]), "rt") as f:
        for line in f:
            sid, bars = json.loads(line)
            if sid not in wanted:
                continue
            parsed = [(date.fromisoformat(d), c, a, st, ok) for d, c, a, _volume, st, ok in bars]
            close = {d: c for d, c, _a, _st, ok in parsed if ok}
            series = SeriesBars(parsed, sessions)
            for day in decision_days:
                if day in close:
                    close_at[(sid, day)] = close[day]
                max_at[(sid, day)] = rmax(series, sessions, position[day])

    table: list[dict[str, dict[str, int]]] = []
    for month in grid:
        names = sorted(rows[month], key=lambda r: (-r[0], r[1]))
        if len(names) < BOOK_UNIVERSE_SIZE:
            raise MeasureError(f"{month}: fewer than {BOOK_UNIVERSE_SIZE} admitted names")
        top = {r[1] for r in names[:BOOK_UNIVERSE_SIZE]}
        p20, p50, p80 = (cutoffs[(k, month)] for k in ("nyse_p20", "nyse_p50", "nyse_p80"))
        values = sorted(v for _, _, sid, s in names if isinstance(v := max_at[(sid, s)], float))
        if not values:
            raise MeasureError(f"{month}: no admitted name has an rmax1_21d value")
        cutoff = values[math.ceil(MAX_DECILE * len(values)) - 1]
        counts: dict[str, dict[str, int]] = {p: dict.fromkeys(FLAGS, 0) for p in POPULATIONS}
        for me, key, sid, s in names:
            if (sid, s) not in close_at:
                raise MeasureError(f"{month}: admitted series {sid} has no usable bar at s(M) {s}")
            if sid not in first_bar:
                raise MeasureError(f"{month}: admitted series {sid} has no first admitted bar")
            value = max_at[(sid, s)]
            flags = {
                "admitted": True,
                "max": value == "screened" or (isinstance(value, float) and value >= cutoff),
                "max_screened": value == "screened",
                "max_short": value == "short",
                "max_zero_heavy": value == "zero_heavy",
                "sub5": close_at[(sid, s)] < PRICE_FLOOR,
                "young": not archive_seasoned(first_bar[sid], s),
            }
            flags["any"] = flags["max"] or flags["sub5"] or flags["young"]
            for population in (_segment(me, p20, p50, p80), "top1000" if key in top else "rest"):
                for flag, hit in flags.items():
                    counts[population][flag] += int(hit)
        table.append(counts)

    print("population", "flag", "min", "median", "max", sep="\t")
    for population in POPULATIONS:
        for flag in FLAGS:
            series = [t[population][flag] for t in table]
            print(population, flag, min(series), statistics.median(series), max(series), sep="\t")
        share = [t[population]["any"] / t[population]["admitted"] for t in table]
        print(population, "any_share", f"{min(share):.4f}", f"{statistics.median(share):.4f}", f"{max(share):.4f}")
    print()
    jkp_premise(verified.files[JKP])
    print()
    print("M", *(f"{p}:{flag}" for p in POPULATIONS for flag in FLAGS), sep="\t")
    for month, counts in zip(grid, table, strict=True):
        print(month, *(counts[p][flag] for p in POPULATIONS for flag in FLAGS), sep="\t")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ARTEFACT)
