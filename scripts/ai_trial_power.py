"""#3471 §9 planning table — random-vs-random pairs, sign-flip rejection rates at planted shifts.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §9 "Planning table" and
§10. A rough aid for the declaration, NOT a power guarantee.

- **Universe per session:** US-equity instruments (``exchanges.asset_class = 'us_equity'``, the
  shortlist's own filter, tradability NOT required so delisted names stay) with a loadable bar on
  that session whose close is ≥ the §3.1 $3 floor. ⚠ ``price_daily`` is provider back-adjusted and
  carries no as-traded close, so the floor reads the stored close (spec §9, amended in slice 3a).
- **Window:** signal sessions 2024-01-02 to 2026-06-30 on the NYSE calendar
  (``market_calendar.us_market_status``). Each series is cut to NYSE sessions at load — the stored
  corpus carries weekend and holiday bars — so a horizon of h bars is h sessions.
- **Pairs:** ``PAIRS_PER_SESSION`` per sampled session; both legs are uniform random draws from the
  same session's universe (no two legs share an instrument within a session).
- **Levels:** each leg's own, as the §7 v5 control derives them: stop = k × its ATR14% at the signal
  session, target = R × stop, a leg outside the §5 bounds is ineligible and redrawn. The ATR is the
  pack's: ``indicator_series.atr_series`` over the trailing ``INDICATOR_BARS`` bars (at least
  ``MIN_BARS``) inside the signal bar's own price segment, so an old masked bar ages out of the
  window exactly as it does in the live pack instead of poisoning every later session. The grid is
  the supervisor's 2026-09-28 15:30Z rule: k ∈ [1.5, 3] (both ends), R = 2 (the default), crossed
  with the §5 horizons.
- **Levels are quantized exactly as the v5 trial control was:** ``ai_trial_decision.measure_atr``
  then ``v5_levels`` (so the §5 bounds, the v5 ATR band and the v5 R floor all apply on the 4-dp
  values). ⚠ v6 (§16.0) replaced that control rule with structure plans; ``v5_levels`` lives here
  only until slice v6-4 re-specifies this grid under O-v6-5.
- **Execution:** entry at the next session's open; the fill session is session 0 and the deadline is
  session h (``ai_trial_deadline``), so the hold spans h + 1 bars. ``outcome_resolver.resolve_outcome``
  resolves the bracket over the series cut at the deadline (a gap through a level fills at the open).
  Two spec §9 rules differ from that resolver and are applied on top of it: an ``ambiguous`` bar
  takes the stop first, and the horizon exit is the deadline bar's CLOSE (the resolver books the
  next open, which is why the series is cut — no bar after the deadline can decide a leg). A
  series that ends inside the hold before the corpus frontier is a delisting and exits at its last
  close. An unresolved ``price_series_break`` inside the hold ends the segment
  (``price_segments.segment_end_index``), so the leg is refused rather than booked across a scale
  change.
- **Statistic:** d = arm − control per-trade gross %, with the planted shift δ added to the arm;
  one-sided cluster sign-flip p (``ai_trial_stats``); the rejection rate is the share of replicates
  with p ≤ α.

⚠ Stated caveats (§9): gross dispersion only (costs vary by name and hold); the population is not
the ranked shortlist; inventory, refusals, broken pairs, halts and the cohort minimum are ignored;
the rejection rate is not the probability of any verdict; a horizon that spans a coverage hole runs one
session longer, and legs on a hole session come only from names the provider has bars for. The ATR
here is cut to NYSE sessions and to the signal bar's price segment, while the live pack
(``ai_trial_pack_reader.read_bars``) takes the last 260 stored rows unsegmented, so the two differ for
a name with weekend bars or an unresolved break in its window. The script never calls the model
(§10).

Usage::

    PYTHONPATH=. uv run python -m scripts.ai_trial_power [--replicates 1000] [--seed 3471] [--json out.json]
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any, Final, Literal

import numpy as np
import numpy.typing as npt
import psycopg

from app.services.ai_trial_decision import (
    HORIZON_SESSIONS,
    STOP_ATR_MULTIPLE_MAX,
    STOP_PCT_MAX,
    STOP_PCT_MIN,
    TARGET_PCT_MAX,
    TARGET_PCT_MIN,
    AtrMeasurement,
    exact,
    measure_atr,
    quantize,
)
from app.services.ai_trial_pack import INDICATOR_BARS, MIN_BARS, MIN_BID, WILDER_PERIOD
from app.services.ai_trial_stats import EXACT_MAX_CLUSTERS, flip_set, sign_flip_p_batch
from app.services.indicator_series import BarSeries, atr_series
from app.services.market_calendar import us_market_status
from app.services.outcome_resolver import ExitLevels, resolve_outcome
from app.services.price_segments import segment_end_index, segment_for_index

WINDOW_START: Final = date(2024, 1, 2)
WINDOW_END: Final = date(2026, 6, 30)
#: §3.1's $3 floor, applied to the signal-session close (§9).
MIN_CLOSE: Final = MIN_BID
PAIRS_PER_SESSION: Final = 2
CLUSTER_COUNTS: Final = (15, 20, 30)
MC_CLUSTER_COUNTS: Final = tuple(k for k in CLUSTER_COUNTS if k > EXACT_MAX_CLUSTERS)
#: Supervisor rule 2026-09-28 15:30Z (#2437): stop = k × ATR14 with k in [1.5, 3]; target 2R default.
STOP_ATR_MULTIPLES: Final = (Decimal("1.5"), Decimal("3"))
REWARD_RISK: Final = Decimal("2")
#: The v5 §6 band floor and R floor (supervisor 2026-09-28 15:45Z), superseded in the trial by §16.
V5_STOP_ATR_MULTIPLE_MIN: Final = Fraction(1)
V5_R_MULTIPLE_MIN: Final = Fraction(3, 2)
#: Planted shifts in d, per-trade percentage points.
SHIFTS_PCT: Final = (0.0, 1.0, 2.0, 3.0, 5.0)
ALPHA: Final = 0.05
#: Seed offset for the second, independent Monte-Carlo flip set (``flip_set_sensitivity``).
ALT_FLIP_SEED: Final = 1_000_003
#: Load start: at least ``INDICATOR_BARS`` NYSE sessions before ``WINDOW_START``, so the first signal
#: session sees the pack's full ATR window.
WARMUP_START: Final = date(2022, 12, 1)

LegOutcome = Literal["tp_hit", "sl_hit", "expired", "ambiguous_stop", "delisted"]
LegRefusal = Literal[
    "no_next_bar",
    "next_bar_not_next_session",
    "no_fill_open",
    "too_few_bars",
    "atr_invalid",
    "levels_outside_bounds",
    "masked_bar",
    "series_break",
    "corpus_edge",
    "no_exit_close",
]


@dataclass(frozen=True)
class Cell:
    stop_atr_multiple: Decimal
    reward_risk: Decimal
    horizon: int

    @property
    def label(self) -> str:
        return f"k={self.stop_atr_multiple:g} R={self.reward_risk:g} h={self.horizon}"


GRID: Final = tuple(Cell(k, REWARD_RISK, h) for k in STOP_ATR_MULTIPLES for h in HORIZON_SESSIONS)


@dataclass(frozen=True)
class PanelInstrument:
    instrument_id: int
    series: BarSeries
    index: Mapping[date, int]
    #: Unresolved ``price_series_break`` dates (first bar at the new scale), ascending.
    breaks: tuple[date, ...] = ()
    #: Memoised ``atr_at`` by bar index — shared across grid cells.
    atr_cache: dict[int, float | None] = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class Leg:
    return_pct: float | None
    outcome: LegOutcome | None = None
    refusal: LegRefusal | None = None


@dataclass(frozen=True)
class V5Levels:
    stop_pct: Decimal
    target_pct: Decimal


def v5_levels(stop_atr_multiple: Decimal, reward_risk: Decimal, atr: AtrMeasurement | None) -> V5Levels | None:
    """The v5 §7 control rule: ``stop = q(k × atr14_pct)``, ``target = q(R × stop)``; ``None``
    when not placeable (an invalid measurement, the §5 bounds, the ATR band or the R floor)."""
    if atr is None:
        return None
    stop = quantize(Fraction(stop_atr_multiple) * Fraction(atr.atr14_pct))
    target = quantize(Fraction(reward_risk) * Fraction(stop))
    atr_pct, s, t = Fraction(atr.atr14_pct), Fraction(stop), Fraction(target)
    placeable = (
        exact(STOP_PCT_MIN) <= s <= exact(STOP_PCT_MAX)
        and exact(TARGET_PCT_MIN) <= t <= exact(TARGET_PCT_MAX)
        and V5_STOP_ATR_MULTIPLE_MIN * atr_pct <= s <= STOP_ATR_MULTIPLE_MAX * atr_pct
        and t >= V5_R_MULTIPLE_MIN * s
    )
    return V5Levels(stop, target) if placeable else None


def panel_instrument(instrument_id: int, series: BarSeries, breaks: Sequence[date] = ()) -> PanelInstrument:
    return PanelInstrument(instrument_id, series, {d: i for i, d in enumerate(series.dates)}, tuple(breaks))


def atr_at(inst: PanelInstrument, i: int) -> float | None:
    """The pack's ATR14 at bar ``i``: Wilder over the trailing ``INDICATOR_BARS`` bars of ``i``'s own
    price segment. ``nan`` when that segment holds fewer than ``MIN_BARS`` bars up to ``i`` (the
    pack's ``too_few_bars``); ``None`` when the window's last value is unevaluable."""
    if i not in inst.atr_cache:
        segment, local = segment_for_index(inst.series, index=i, unresolved_breaks=inst.breaks)
        lo = max(0, local + 1 - INDICATOR_BARS)
        if local + 1 - lo < MIN_BARS:
            inst.atr_cache[i] = math.nan
        else:
            window = BarSeries(dates=segment.dates[lo : local + 1], rows=segment.rows[lo : local + 1])
            inst.atr_cache[i] = atr_series(window, universe="survivor_only", period=WILDER_PERIOD).values[-1]
    return inst.atr_cache[i]


def in_universe(inst: PanelInstrument, session: date) -> bool:
    """A loadable bar on ``session`` whose close is present and ≥ ``MIN_CLOSE``."""
    i = inst.index.get(session)
    if i is None:
        return False
    close = inst.series.rows[i].get("close")
    return close is not None and close >= MIN_CLOSE


def simulate_leg(inst: PanelInstrument, session: date, next_session: date, cell: Cell, *, frontier: date) -> Leg:
    """One long leg signalled on ``session`` (a universe member), filled at ``next_session``'s open."""
    rows, dates = inst.series.rows, inst.series.dates
    i = inst.index[session]
    f = i + 1
    if f >= len(rows):
        return Leg(None, refusal="no_next_bar")
    if dates[f] != next_session:
        return Leg(None, refusal="next_bar_not_next_session")
    entry = rows[f].get("open")
    if entry is None:
        return Leg(None, refusal="no_fill_open")
    atr, close = atr_at(inst, i), rows[i].get("close")
    if atr is not None and math.isnan(atr):
        return Leg(None, refusal="too_few_bars")
    levels = v5_levels(cell.stop_atr_multiple, cell.reward_risk, measure_atr(atr, close))
    if levels is None:
        return Leg(None, refusal="atr_invalid" if measure_atr(atr, close) is None else "levels_outside_bounds")
    stop = entry * (1 - levels.stop_pct / 100)
    target = entry * (1 + levels.target_pct / 100)
    # The fill session is session 0 and the deadline is session h (``ai_trial_deadline``), so the
    # hold spans bars f..f+h. The series is cut at the deadline: the exit is that bar's close, and
    # no later bar may decide the leg (the resolver's own expiry reads the NEXT open).
    deadline = f + cell.horizon
    outcome = resolve_outcome(
        series=BarSeries(dates=dates[: deadline + 1], rows=rows[: deadline + 1]),
        fill_index=f,
        entry_price=entry,
        levels=ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=cell.horizon + 1),
        masked_bar_reasons={},
        segment_end_index=segment_end_index(inst.series, fill_index=f, unresolved_breaks=inst.breaks),
    )

    def booked(price: Decimal | None, kind: LegOutcome) -> Leg:
        if price is None:
            return Leg(None, refusal="no_exit_close")
        return Leg(float(100 * (price - entry) / entry), outcome=kind)

    if outcome.outcome in ("tp_hit", "sl_hit"):
        return booked(outcome.exit_price, outcome.outcome)
    if outcome.outcome == "ambiguous":
        return booked(stop, "ambiguous_stop")
    if outcome.reason == "window_truncated":
        # Reached only after every bar of the (cut) window was read without a touch.
        if len(rows) > deadline:
            return booked(rows[deadline].get("close"), "expired")
        if dates[-1] >= frontier:
            return Leg(None, refusal="corpus_edge")
        return booked(rows[-1].get("close"), "delisted")
    if outcome.reason == "series_break":
        return Leg(None, refusal="series_break")
    return Leg(None, refusal="masked_bar")


@dataclass
class Census:
    """Per DISTINCT leg (a cached revisit is not recounted) and per distinct thin session."""

    outcomes: Counter[str] = field(default_factory=Counter)
    refusals: Counter[str] = field(default_factory=Counter)
    thin_sessions: set[date] = field(default_factory=set)

    def count(self, leg: Leg) -> None:
        if leg.return_pct is None:
            self.refusals[leg.refusal or "unknown"] += 1
        else:
            self.outcomes[leg.outcome or "unknown"] += 1


def draw_pair_differences(
    rng: np.random.Generator,
    sessions: Sequence[date],
    universes: Sequence[Sequence[int]],
    leg_for: Callable[[int, int], Leg],
    *,
    replicates: int,
    clusters: int,
    census: Census,
) -> npt.NDArray[np.float64]:
    """(replicates × clusters × PAIRS_PER_SESSION) base d = arm − control, no shift.

    ``sessions[j]`` is a signal session with a next session in the panel; ``universes[j]`` its
    members (panel positions); ``leg_for(j, n)`` the leg. Clusters in a replicate are distinct
    sessions. A session whose universe runs out before ``2 × PAIRS_PER_SESSION`` eligible legs —
    in practice one whose NEXT session is a provider coverage hole, so no leg can fill — is replaced
    by another draw and recorded as thin.
    """
    need = 2 * PAIRS_PER_SESSION
    out = np.empty((replicates, clusters, PAIRS_PER_SESSION), dtype=np.float64)
    for rep in range(replicates):
        order = rng.permutation(len(sessions))
        filled = 0
        for j in order:
            if filled == clusters:
                break
            legs: list[float] = []
            for pos in rng.permutation(len(universes[j])):
                leg = leg_for(int(j), universes[j][pos])
                if leg.return_pct is None:
                    continue
                legs.append(leg.return_pct)
                if len(legs) == need:
                    break
            if len(legs) < need:
                census.thin_sessions.add(sessions[j])
                continue
            out[rep, filled] = [legs[2 * p] - legs[2 * p + 1] for p in range(PAIRS_PER_SESSION)]
            filled += 1
        if filled < clusters:
            raise RuntimeError(f"only {filled} of {clusters} sessions have {need} eligible legs")
    return out


def rejection_rates(
    base_d: npt.NDArray[np.float64],
    *,
    cluster_counts: Sequence[int],
    shifts_pct: Sequence[float],
    alpha: float,
    seed: int,
    flips: int,
) -> dict[float, dict[int, float]]:
    """{δ: {K: share of replicates with one-sided p ≤ α}} over the first K clusters of each replicate."""
    out: dict[float, dict[int, float]] = {s: {} for s in shifts_pct}
    for k in cluster_counts:
        fs = flip_set(k, seed=seed + k, flips=flips)
        for shift in shifts_pct:
            sums = (base_d[:, :k, :] + shift).sum(axis=2)
            p = sign_flip_p_batch(sums, fs)
            out[shift][k] = float(np.mean(p <= alpha))
    return out


def flip_set_sensitivity(a: Mapping[float, Mapping[int, float]], b: Mapping[float, Mapping[int, float]]) -> float:
    """Largest |rate_a − rate_b| over the (δ, K) cells ``b`` covers — the Monte-Carlo K only, since
    exact enumeration (K ≤ ``EXACT_MAX_CLUSTERS``) is identical under any seed."""
    return max((abs(a[s][k] - b[s][k]) for s in b for k in b[s]), default=0.0)


# ---------------------------------------------------------------------------
# DB load (read-only) and CLI
# ---------------------------------------------------------------------------

_UNIVERSE_SQL = """
    SELECT i.instrument_id
      FROM instruments i
      JOIN exchanges e ON e.exchange_id = i.exchange
     WHERE e.asset_class = 'us_equity'
"""


def load_panel(conn: psycopg.Connection[Any]) -> tuple[list[PanelInstrument], date]:
    from app.services.price_masked_bars import load_bar_spans, load_masked_bars
    from app.services.price_segments import load_unresolved_breaks

    ids = [int(r[0]) for r in conn.execute(_UNIVERSE_SQL).fetchall()]
    spans = load_bar_spans(conn, ids)
    breaks = load_unresolved_breaks(conn, list(spans))
    frontier = max(s.last_bar for s in spans.values())
    panel: list[PanelInstrument] = []
    for iid in sorted(i for i, s in spans.items() if s.last_bar >= WINDOW_START):
        series = load_masked_bars(conn, iid).series
        keep = [n for n, d in enumerate(series.dates) if d >= WARMUP_START and is_session(d)]
        if not keep:
            continue
        trimmed = BarSeries(dates=tuple(series.dates[n] for n in keep), rows=tuple(series.rows[n] for n in keep))
        panel.append(panel_instrument(iid, trimmed, breaks.get(iid, ())))
    return panel, frontier


def is_session(d: date) -> bool:
    return us_market_status(d) != "closed"


def nyse_sessions(start: date, end: date) -> list[date]:
    return [start + timedelta(days=n) for n in range((end - start).days + 1) if is_session(start + timedelta(days=n))]


def run(panel: Sequence[PanelInstrument], frontier: date, *, replicates: int, seed: int, flips: int) -> dict[str, Any]:
    calendar = nyse_sessions(WARMUP_START, frontier)
    next_of = dict(zip(calendar, calendar[1:], strict=False))
    sessions = [d for d in calendar if WINDOW_START <= d <= WINDOW_END and d in next_of]
    universes = [[n for n, inst in enumerate(panel) if in_universe(inst, s)] for s in sessions]
    report: dict[str, Any] = {
        "window": [WINDOW_START.isoformat(), WINDOW_END.isoformat()],
        "frontier": frontier.isoformat(),
        "instruments": len(panel),
        "sessions": len(sessions),
        "universe_size": {"min": min(map(len, universes)), "max": max(map(len, universes))},
        "replicates": replicates,
        "seed": seed,
        "flips": flips,
        "alpha": ALPHA,
        "cells": [],
    }
    for c, cell in enumerate(GRID):
        cache: dict[tuple[int, int], Leg] = {}
        census = Census()

        def leg_for(
            j: int, n: int, cell: Cell = cell, cache: dict[tuple[int, int], Leg] = cache, census: Census = census
        ) -> Leg:
            key = (j, n)
            if key not in cache:
                cache[key] = simulate_leg(panel[n], sessions[j], next_of[sessions[j]], cell, frontier=frontier)
                census.count(cache[key])
            return cache[key]

        rng = np.random.default_rng(seed * 100 + c)
        base = draw_pair_differences(
            rng, sessions, universes, leg_for, replicates=replicates, clusters=max(CLUSTER_COUNTS), census=census
        )
        rates = rejection_rates(
            base, cluster_counts=CLUSTER_COUNTS, shifts_pct=SHIFTS_PCT, alpha=ALPHA, seed=seed, flips=flips
        )
        # The same draws under an independent flip set: every replicate shares one flip set, so its
        # Monte-Carlo error moves the whole column together. Reported, not assumed away.
        rates_alt = rejection_rates(
            base,
            cluster_counts=MC_CLUSTER_COUNTS,
            shifts_pct=SHIFTS_PCT,
            alpha=ALPHA,
            seed=seed + ALT_FLIP_SEED,
            flips=flips,
        )
        report["cells"].append(
            {
                "cell": cell.label,
                "d_sd_pct": float(base.std(ddof=1)),
                "d_mean_pct": float(base.mean()),
                "leg_outcomes": dict(census.outcomes),
                "leg_refusals": dict(census.refusals),
                "thin_sessions": sorted(d.isoformat() for d in census.thin_sessions),
                "rejection": {str(s): {str(k): r for k, r in by_k.items()} for s, by_k in rates.items()},
                "flip_set_sensitivity": flip_set_sensitivity(rates, rates_alt),
            }
        )
    return report


def render(report: Mapping[str, Any]) -> str:
    lines = [
        f"#3471 §9 planning table — window {report['window'][0]}..{report['window'][1]}, "
        f"{report['instruments']} instruments, {report['sessions']} sessions, universe "
        f"{report['universe_size']['min']}-{report['universe_size']['max']}, {report['replicates']} replicates, "
        f"seed {report['seed']}, {report['flips']} MC flips above 16 clusters, one-sided α = {report['alpha']}",
        "Rejection rate of the cluster sign-flip test (share of replicates with p ≤ α). NOT a verdict probability.",
    ]
    head = "| δ (pp) | " + " | ".join(f"K={k}" for k in CLUSTER_COUNTS) + " |"
    for cell in report["cells"]:
        lines += [
            "",
            f"### {cell['cell']} — sd(d) {cell['d_sd_pct']:.2f} pp, mean(d) {cell['d_mean_pct']:+.3f} pp, "
            f"flip-set sensitivity (Monte-Carlo K only; exact K is seed-free) {cell['flip_set_sensitivity']:.3f}",
            f"distinct legs {cell['leg_outcomes']} · refused {cell['leg_refusals']} · "
            f"thin sessions {len(cell['thin_sessions'])}",
            head,
            "|---|" + "---|" * len(CLUSTER_COUNTS),
        ]
        for shift, by_k in cell["rejection"].items():
            lines.append(f"| {float(shift):g} | " + " | ".join(f"{by_k[str(k)]:.3f}" for k in CLUSTER_COUNTS) + " |")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3471 §9 planning table (never calls the model)")
    parser.add_argument("--replicates", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=3471)
    parser.add_argument("--flips", type=int, default=99_999)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args(argv)

    from app.config import settings

    started = time.monotonic()
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        panel, frontier = load_panel(conn)
    report = run(panel, frontier, replicates=args.replicates, seed=args.seed, flips=args.flips)
    report["elapsed_s"] = round(time.monotonic() - started, 1)
    print(render(report))
    if args.json:
        args.json.write_text(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
