"""#1822 route F: one readout, composed from slice 1a's construction over loaded inputs. Pure.

Spec ``docs/proposals/ta/2026-09-28-1822-ranking-ablation.md`` (v8), "Books", "Cells",
"Grid", "Statistics per family f" and "Prospective subseries". The script loads the
inputs through :mod:`ranking_ablation_reader` after the declaration gate; this module turns
them into per-cell statistics and reads nothing itself.

- **Evaluations** are :data:`ranking_ablation_terms.EVALUATIONS`: the four cells under the
  canonical termination policy, then the canonical cell under each other programme policy.
  Each sensitivity changes one assumption; they are never combined.
- **Populations**: ``pooled`` is every mapped run; ``prospective`` is the runs known after
  the declaration's ``frozen_at``, on their own grid. Retrospective cohorts are never
  sliced into it.
- **Cost**: the band is selected once per position at its entry open and reused at exit
  (``HalfSpread`` is keyed on the entry session), plus ``HUNT_TARIFF``'s proportional
  commission. Canonical is ``split_adjusted`` (the maximum band); ``as_traded`` reads the
  stored level.
- **Refusals**: a refusal of ``arm_full`` or the control refuses all six families in that
  cell; one of ``arm_{−f}`` refuses f only. Cells never affect each other.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final

from app.services import ranking_ablation as ra
from app.services import ranking_ablation_reader as reader
from app.services.cost_model import cost_band_for
from app.services.hunt_evaluator import Book, Grid, HalfSpread, SeriesPrices, select_arm
from app.services.hunt_harness import HUNT_TARIFF
from app.services.hunt_inference import MIN_ARM_FORMATIONS, SHORT_SAMPLE_LAGS, StatRefused, hunt_lag, sparse_arm_count
from app.services.r6_exclusion_trial import PROGRAMME_POLICIES, TerminationPolicy
from app.services.ranking_ablation_terms import CANONICAL_POLICY, EVALUATIONS, POPULATIONS
from app.services.series_termination import TerminationClass

_POLICIES: Final[Mapping[str, TerminationPolicy]] = {policy.label: policy for policy in PROGRAMME_POLICIES}


@dataclass(frozen=True)
class Evaluation:
    """One entry of the inventory's evaluation list: a cell and a termination policy."""

    cell: str
    policy: TerminationPolicy


def parse_evaluation(name: str) -> Evaluation:
    """``canonical`` / ``t3_excluded`` / … (canonical policy), or ``canonical@<policy>``."""
    if name not in EVALUATIONS:
        raise ValueError(f"unknown evaluation {name!r}")
    cell, _, policy = name.partition("@")
    return Evaluation(cell=cell, policy=_POLICIES[policy or CANONICAL_POLICY])


@dataclass(frozen=True)
class Inputs:
    """Everything one readout reads, loaded once in one snapshot."""

    population: reader.Population
    run_map: ra.RunMap
    sessions: tuple[date, ...]
    cutoff: date
    series: Mapping[int, reader.ReadSeries]
    termination: Mapping[int, TerminationClass]
    #: ``ttm_yield_pct`` at readout time (the yield-tilt diagnostic); absent = no summary row.
    yields: Mapping[int, float | None] = field(default_factory=dict)
    #: The benchmark's causal regime per date, or the reason it could not be classified.
    regimes: Mapping[date, str | None] | str = field(default_factory=dict)


def series_prices(
    read: reader.ReadSeries, index: Mapping[date, int], *, terminal_class: TerminationClass | None
) -> SeriesPrices:
    """Masked bars on the session ordinal basis. A terminating series ends at its last bar."""
    opens: dict[int, float] = {}
    closes: dict[int, float] = {}
    for bar in read.masked:
        ordinal = index.get(bar.price_date)
        if ordinal is None:
            continue  # a bar on a non-session date prices nothing on the grid
        if bar.open is not None:
            opens[ordinal] = float(bar.open)
        if bar.close is not None:
            closes[ordinal] = float(bar.close)
    terminal = None
    if terminal_class is not None and terminal_class != TerminationClass.UNKNOWN:
        terminal = index.get(read.bars[-1].price_date)
    return SeriesPrices(opens=opens, closes=closes, dividends={}, terminal_ordinal=terminal)


def _half_spread(series: Mapping[int, SeriesPrices], *, basis: str) -> HalfSpread:
    if HUNT_TARIFF is None:
        raise ValueError("HUNT_TARIFF is unset: the real_stock_long_x1 lane is unpriced")
    commission = HUNT_TARIFF.proportional_commission_per_side
    cache: dict[tuple[int, int], float] = {}

    def half_spread(name: int, e: int, book: Book) -> float:
        key = (name, e)
        band = cache.get(key)
        if band is None:
            price = series[name].price(e, "open")
            if price is None:
                raise ValueError(f"series {name} has no valid open on {e} to price")
            band = cache[key] = float(
                cost_band_for(
                    Decimal(repr(price)), price_basis="as_traded" if basis == "as_traded" else "split_adjusted"
                ).half_spread
            )
        return band + commission

    return half_spread


@dataclass(frozen=True)
class Formations:
    """Per formation t: C(t) and the keys, plus the counts the readout reports."""

    control: Mapping[int, frozenset[int]]
    keys: Mapping[int, Mapping[str | None, Mapping[int, float]]]


def formations(
    inputs: Inputs,
    grid_formations: Sequence[int],
    *,
    cell: str,
    runs: Mapping[date, ra.Run],
    t3_names: frozenset[int],
) -> Formations:
    """C(t) under the cell's bar rule and population, and both keys restricted to it before selection."""
    weights = ra.weights()
    verbatim = cell == "verbatim_bar_rule"
    by_day = {iid: {bar.price_date: bar for bar in read.masked} for iid, read in inputs.series.items()}
    control: dict[int, frozenset[int]] = {}
    keys: dict[int, dict[str | None, dict[int, float]]] = {}
    for t in grid_formations:
        run = runs.get(inputs.sessions[t + ra.LAG])
        if run is None:
            continue
        rows = inputs.population.runs[run.scored_at]
        day = inputs.sessions[t]
        names: set[int] = set()
        for iid in rows:
            if cell == "t3_excluded" and iid in t3_names:
                continue
            bar = by_day.get(iid, {}).get(day)
            if bar is not None and ra.bar_valid(bar.open, bar.high, bar.low, bar.close, bar.volume, verbatim=verbatim):
                names.add(iid)
        if not names:
            continue
        control[t] = frozenset(names)
        keys[t] = {
            drop: {iid: ra.rebuilt_key(rows[iid].row, weights, drop=drop) for iid in names}
            for drop in (None, *ra.FAMILY_ORDER)
        }
    return Formations(control=control, keys=keys)


def _arm(keys: Mapping[int, float], control: frozenset[int]) -> frozenset[int]:
    return select_arm({name: keys[name] for name in control}, sign=+1, fraction=ra.FRACTION)


def _statistics(delta: Sequence[float], lag: int) -> dict[str, Any]:
    result = ra.delta_statistics(delta, lag=lag)
    if isinstance(result, StatRefused):
        return {"refused": result.reason, "detail": result.detail}
    return {
        "observations": result.observations,
        "lag": result.lag,
        "nonzero_sessions": result.nonzero_sessions,
        "mean_annual": result.mean_annual,
        "se_annual": result.se_annual,
        "t": result.t_stat,
        "mde_annual": result.mde_annual,
    }


def evaluate(inputs: Inputs, evaluation: str, *, prospective_after: datetime | None) -> dict[str, Any]:
    """One evaluation over one population. ``prospective_after`` set → runs known after it only."""
    spec = parse_evaluation(evaluation)
    runs = {
        entry: run
        for entry, run in inputs.run_map.entries.items()
        if prospective_after is None or run.known_at > prospective_after
    }
    if not runs:
        return {"refused": "empty_grid", "detail": "no mapped run in this population"}
    index = {day: ordinal for ordinal, day in enumerate(inputs.sessions)}
    first_formation = inputs.sessions[index[min(runs)] - ra.LAG]
    grid = ra.reporting_grid(inputs.sessions, first_formation=first_formation, cutoff=inputs.cutoff)
    if isinstance(grid, StatRefused):
        return {"refused": grid.reason, "detail": grid.detail}

    t3_names = ra.t3_excluded(
        {iid: read.verdicts.transitions for iid, read in inputs.series.items()},
        start=first_formation,
        end=inputs.cutoff,
    )
    built = formations(inputs, grid.formations, cell=spec.cell, runs=runs, t3_names=t3_names)
    names = {iid for control in built.control.values() for iid in control}
    prices = {
        iid: series_prices(inputs.series[iid], index, terminal_class=inputs.termination.get(iid)) for iid in names
    }
    terminal_fractions = {
        iid: spec.policy.terminal_fraction(inputs.termination[iid])
        for iid, series in prices.items()
        if series.terminal_ordinal is not None
    }
    books = ra.ablation_books(
        grid,
        built.control,
        built.keys,
        prices,
        half_spread=_half_spread(prices, basis=spec.cell),
        terminal_fractions=terminal_fractions,
    )
    observations = len(grid.sessions)
    lag = hunt_lag(observations, ra.H)
    out: dict[str, Any] = {
        "first_formation": first_formation,
        "g_k": inputs.sessions[grid.last],
        #: The reported sessions, so a later vintage can compare an already reported session.
        "grid_sessions": [inputs.sessions[d] for d in grid.sessions],
        "T": observations,
        "L": lag,
        "L_ge_T": lag >= observations,
        "2L_ge_T": 2 * lag >= observations,
        "short_sample_T_over_L": observations / lag,
        "short_sample_floor": SHORT_SAMPLE_LAGS,
        "active_formations": len(built.control),
        "t3_excluded_names": len(t3_names) if spec.cell == "t3_excluded" else None,
        "terminating_names_held": len(terminal_fractions),
    }
    if isinstance(books, StatRefused):
        out["refused"] = books.reason
        out["detail"] = books.detail
        return out
    full = books.full
    out["arm_full_minus_control_mean_annual"] = math.fsum(full.active) / observations * ra.ANNUALISATION
    out["sparse_arm_count"] = sparse_arm_count(full.entered_formations, ra.H)
    out["sparse_arm_floor"] = MIN_ARM_FORMATIONS
    out["tallies"] = {
        "arm_full": {"positions": full.tallies["arm"].positions, "mean_net": full.tallies["arm"].mean},
        "control": {"positions": full.tallies["control"].positions, "mean_net": full.tallies["control"].mean},
    }
    families: dict[str, Any] = {}
    for family in ra.FAMILY_ORDER:
        ablated = books.ablated[family]
        if isinstance(ablated, StatRefused):
            families[family] = {"refused": ablated.reason, "detail": ablated.detail}
            continue
        delta = ra.paired_delta(full, ablated)
        symmetric = [
            len(_arm(built.keys[t][None], built.control[t]) ^ _arm(built.keys[t][family], built.control[t]))
            for t in built.control
        ]
        families[family] = {
            "delta": list(delta),
            "canonical_lag": _statistics(delta, lag),
            "double_lag": _statistics(delta, 2 * lag),
            "arm_positions": ablated.tallies["arm"].positions,
            "arm_mean_net": ablated.tallies["arm"].mean,
            "symmetric_difference_mean": math.fsum(symmetric) / len(symmetric) if symmetric else None,
            "symmetric_difference_max": max(symmetric, default=None),
        }
    out["families"] = families
    out["descriptive"] = describe(inputs, grid, built, prices, runs=runs, cell=spec.cell, t3_names=t3_names)
    out["regime_labels"], regime_of = _session_regimes(inputs, grid)
    for family, delta_out in families.items():
        ablated = books.ablated[family]
        if not isinstance(ablated, StatRefused):
            delta_out["regime_split"] = _regime_split(ra.paired_delta(full, ablated), regime_of)
    return out


# ---------------------------------------------------------------------------
# Descriptives: reported beside the statistics, never deciding anything
# ---------------------------------------------------------------------------

#: ``hunt_compute.bar_exclusion``'s codes, in its rule order.
BAR_REASONS: Final = {
    1: "non_positive_ohlc",
    2: "low_above_body",
    3: "high_below_body",
    4: "volume",
    5: "high_not_above_low",
}


def _book_arms(built: Formations) -> dict[str, dict[int, frozenset[int]]]:
    """book → formation → members, for ``arm_full``, every ``arm_-f`` and the control."""
    arms: dict[str, dict[int, frozenset[int]]] = {
        "arm_full": {t: _arm(built.keys[t][None], control) for t, control in built.control.items()}
    }
    for family in ra.FAMILY_ORDER:
        arms[f"arm_-{family}"] = {t: _arm(built.keys[t][family], control) for t, control in built.control.items()}
    arms["control"] = dict(built.control)
    return arms


def _mean(values: Sequence[float]) -> float | None:
    return math.fsum(values) / len(values) if values else None


def _turnover(by_t: Mapping[int, frozenset[int]]) -> float | None:
    """Mean over consecutive formations of 1 − |A(t) ∩ A(prev)| / |A(t)|; an empty book is skipped."""
    ordered = sorted(by_t)
    return _mean(
        [
            1.0 - len(by_t[now] & by_t[before]) / len(by_t[now])
            for before, now in zip(ordered, ordered[1:], strict=False)
            if by_t[now]
        ]
    )


def describe(
    inputs: Inputs,
    grid: Grid,
    built: Formations,
    prices: Mapping[int, SeriesPrices],
    *,
    runs: Mapping[date, ra.Run],
    cell: str,
    t3_names: frozenset[int],
) -> dict[str, Any]:
    """The spec's descriptive items for one evaluation (Route F "Statistics", "Prices", "Grid")."""
    sessions = inputs.sessions
    index = {day: ordinal for ordinal, day in enumerate(sessions)}
    by_day = {iid: {bar.price_date: bar for bar in read.masked} for iid, read in inputs.series.items()}
    arms = _book_arms(built)

    def entered(name: int, t: int) -> bool:
        return prices[name].price(t + ra.LAG, "open") is not None

    def hold_end(name: int, t: int) -> int:
        x = t + ra.LAG + ra.H - 1
        terminal = prices[name].terminal_ordinal
        return x if terminal is None else min(x, terminal)

    # The funnel: every grid formation with a run, active or idle.
    funnel = []
    for t in grid.formations:
        run = runs.get(sessions[t + ra.LAG])
        if run is None:
            continue
        rows = inputs.population.runs[run.scored_at]
        excluded = inputs.population.excluded_by_run.get(run.scored_at, {})
        t3 = [iid for iid in rows if cell == "t3_excluded" and iid in t3_names]
        present = [iid for iid in rows if iid not in t3 and sessions[t] in by_day.get(iid, {})]
        reasons: Counter[str] = Counter()
        for iid in present:
            bar = by_day[iid][sessions[t]]
            code = ra.bar_reason(
                bar.open, bar.high, bar.low, bar.close, bar.volume, verbatim=cell == "verbatim_bar_rule"
            )
            if code:
                reasons[BAR_REASONS[code]] += 1
        funnel.append(
            {
                "formation": sessions[t],
                "ranked": len(rows) + sum(excluded.values()),
                "excluded": dict(excluded),
                "lane_and_valid": len(rows),
                "t3_excluded": len(t3),
                "bars_present": len(present),
                "bar_rule": dict(reasons),
                "control": len(built.control.get(t, ())),
            }
        )

    sizes = {
        book: [
            {"formation": sessions[t], "size": len(members), "never_entered": sum(not entered(i, t) for i in members)}
            for t, members in sorted(by_t.items())
        ]
        for book, by_t in arms.items()
    }
    turnover: dict[str, float | None] = {}
    carried: dict[str, int] = {}
    for book, by_t in arms.items():
        turnover[book] = _turnover(by_t)
        carried[book] = sum(
            1
            for t, members in by_t.items()
            for name in members
            if entered(name, t)
            for s in range(t + ra.LAG, hold_end(name, t) + 1)
            if prices[name].price(s, "close") is None
        )
    deployed = [
        {
            "session": sessions[d],
            "deployed_share": sum(1 for k in range(ra.H) if d - ra.LAG - k in built.control) / ra.H,
        }
        for d in grid.sessions
    ]

    # Entered positions held across a computed T3 transition ("Prices" 1).
    t3_held = []
    for name, read in sorted(inputs.series.items()):
        for transition in read.verdicts.transitions:
            if "T3" not in transition.rules:
                continue
            before, after = index.get(transition.prior_date), index.get(transition.price_date)
            if before is None or after is None:
                continue
            for book, by_t in arms.items():
                for t, members in sorted(by_t.items()):
                    if name in members and entered(name, t) and t + ra.LAG <= before and after <= hold_end(name, t):
                        t3_held.append(
                            {
                                "book": book,
                                "formation": sessions[t],
                                "instrument_id": name,
                                "prior_date": transition.prior_date,
                                "price_date": transition.price_date,
                                "ratio": None if transition.observed_ratio is None else str(transition.observed_ratio),
                            }
                        )

    return {
        "funnel": funnel,
        "book_sizes": sizes,
        "turnover_mean": turnover,
        "carried_v_position_sessions": carried,
        "deployed_share": deployed,
        "t3_held": t3_held,
        "yield_tilt": _yield_tilt(inputs.yields, arms, entered),
    }


def _yield_tilt(
    yields: Mapping[int, float | None],
    arms: Mapping[str, Mapping[int, frozenset[int]]],
    entered: Any,
) -> dict[str, Any]:
    """Per family: mean over formations of (mean yield of arm_full's entered names − arm_-f's).

    Read at readout time, not point-in-time; it neither signs nor bounds the omitted
    dividends' effect on Δ ("Prices" 3).
    """

    def usable(value: float | None) -> bool:
        return value is not None and math.isfinite(value)

    out: dict[str, Any] = {}
    for family in ra.FAMILY_ORDER:
        tilts: list[float] = []
        skipped = 0
        missing: set[int] = set()
        for t, full in arms["arm_full"].items():
            means = []
            for members in (full, arms[f"arm_-{family}"][t]):
                held = [name for name in members if entered(name, t)]
                missing.update(name for name in held if not usable(yields.get(name)))
                means.append(
                    _mean([value for name in held if (value := yields.get(name)) is not None and usable(value)])
                )
            if means[0] is None or means[1] is None:
                skipped += 1
            else:
                tilts.append(means[0] - means[1])
        out[family] = (
            {"refused": "no_formation_with_yields", "formations_skipped": skipped, "names_without_yield": len(missing)}
            if not tilts
            else {
                "mean_tilt_pct": _mean(tilts),
                "formations": len(tilts),
                "formations_skipped": skipped,
                "names_without_yield": len(missing),
            }
        )
    return out


def _session_regimes(inputs: Inputs, grid: Grid) -> tuple[dict[str, Any], dict[int, str | None]]:
    """Session d's label is the benchmark's regime on d − 1 (bars ≤ t only), or None."""
    if isinstance(inputs.regimes, str):
        return {"refused": inputs.regimes}, {}
    regimes = inputs.regimes
    # Ordinal 0 has no prior session; never let d − 1 wrap to the calendar's last day.
    of = {d: regimes.get(inputs.sessions[d - 1]) if d >= 1 else None for d in grid.sessions}
    return {"counts": dict(Counter(str(label) for label in of.values()))}, of


def _regime_split(delta: Sequence[float], regime_of: Mapping[int, str | None]) -> dict[str, Any] | None:
    """Per-label mean Δ (×252), only when the grid spans more than one label; no SE, no inference."""
    labels = sorted({label for label in regime_of.values() if label is not None})
    if len(labels) <= 1:
        return None
    ordered = list(regime_of.values())
    split: dict[str, Any] = {}
    for label in labels:
        values = [d for d, value in zip(delta, ordered, strict=True) if value == label]
        split[label] = {"sessions": len(values), "mean_annual": math.fsum(values) / len(values) * ra.ANNUALISATION}
    return split


def _boundary(population: str, frozen_at: datetime) -> datetime | None:
    return frozen_at if population == "prospective" else None


def readout(inputs: Inputs, *, frozen_at: datetime) -> dict[str, Any]:
    """Every evaluation over both populations, in the inventory's order."""
    return {
        population: {
            evaluation: evaluate(inputs, evaluation, prospective_after=_boundary(population, frozen_at))
            for evaluation in EVALUATIONS
        }
        for population in POPULATIONS
    }


__all__ = [
    "BAR_REASONS",
    "Evaluation",
    "Formations",
    "Inputs",
    "describe",
    "evaluate",
    "formations",
    "parse_evaluation",
    "readout",
    "series_prices",
]
