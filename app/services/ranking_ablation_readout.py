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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final

from app.services import ranking_ablation as ra
from app.services import ranking_ablation_reader as reader
from app.services.cost_model import cost_band_for
from app.services.hunt_evaluator import Book, HalfSpread, SeriesPrices, select_arm
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
            "canonical_lag": _statistics(delta, lag),
            "double_lag": _statistics(delta, 2 * lag),
            "arm_positions": ablated.tallies["arm"].positions,
            "arm_mean_net": ablated.tallies["arm"].mean,
            "symmetric_difference_mean": math.fsum(symmetric) / len(symmetric) if symmetric else None,
            "symmetric_difference_max": max(symmetric, default=None),
        }
    out["families"] = families
    return out


def _boundary(population: str, frozen_at: datetime) -> datetime | None:
    return frozen_at if population == "prospective" else None


def readout(inputs: Inputs, *, frozen_at: datetime) -> dict[str, Any]:
    """Every evaluation over both populations, in the inventory's order."""
    return {
        population: {
            evaluation: evaluate(
                inputs, evaluation, prospective_after=frozen_at if population == "prospective" else None
            )
            for evaluation in EVALUATIONS
        }
        for population in POPULATIONS
    }


__all__ = [
    "Evaluation",
    "Formations",
    "Inputs",
    "evaluate",
    "formations",
    "parse_evaluation",
    "readout",
    "series_prices",
]
