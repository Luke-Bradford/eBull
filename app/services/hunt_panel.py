"""The pattern-hunt panel: the one place a hunt reads research prices.

#3385 slice 3c-iii, spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md`` (v5).
:func:`load_hunt_panel` reads every admitted series once; :func:`compute_trial` hands
the panel to the pure ``hunt_compute.compute_panel``.

#3386 slice 1 (spec ``docs/proposals/ta/2026-09-26-3386-hunt-feature-store.md`` v3): a
DISCOVERY trial computes on parts read from ``hunt_store`` (built from this loader on a
miss), keyed on the registered universe identity, the quarantine flags and the model id.
Validation and holdout load live every time, behind the audited door.

⚠ Imported ONLY by ``hunt_harness``, which calls :func:`compute_trial` after a search is
registered and committed. ``tests/test_sealed_outcome_scripts_are_gated.py`` lists this
module as a research price reader, so a script or hunt module importing it fails that
test. Its code is part of ``HUNT_HARNESS_MODEL_ID``: every line here decides a number.
"""

from __future__ import annotations

import hashlib
import logging
import math
from array import array
from bisect import bisect_right
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

import psycopg

from app.services import hunt_compute, hunt_store, hunt_view, market_calendar
from app.services.hunt_compute import ComputeParams, HuntPanel, PanelOutcome, PanelSeries, bar_exclusion
from app.services.hunt_evaluator import Point
from app.services.hunt_store import PanelParts
from app.services.indicator_series import BarSeries, Universe
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.research_price_structure_store import load_masked_series
from app.services.research_split_adjustment import corrected_price, split_scales
from app.services.research_split_corrected_reader import load_ratio_basis, load_split_factors
from app.services.series_termination import classify_termination
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import load_universe_selection

_LOG = logging.getLogger(__name__)


class HuntPanelError(RuntimeError):
    """The archive cannot supply the panel (an infrastructure error: no outcome, retry)."""


#: The dividend cash of exactly the bars ``load_masked_series`` returns (the same
#: coverage JOIN), so no dividend enters from a bar the quarantine never evaluated.
_DIVIDEND_SQL = """
    SELECT d.bar_date, d.dividend
    FROM research_price_daily d
    JOIN research_price_quarantine_coverage cov
      ON cov.series_id = d.series_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
    WHERE d.series_id = %(series_id)s
      AND d.dividend IS NOT NULL AND d.dividend <> 0
      AND d.bar_date <= %(through_date)s
    ORDER BY d.bar_date
"""

_FIRST_BAR_SQL = "SELECT series_id, first_bar FROM research_price_series WHERE series_id = ANY(%(ids)s)"

#: Where discovery stores live: ``var/`` is gitignored. One directory per key; old keys are
#: kept (spec "Build": deleting them defeats reproduction) and removed by hand.
STORE_ROOT: Path = Path(__file__).resolve().parents[2] / "var" / "hunt_store"

#: The quarantine state ``load_masked_series`` applies, as exact flags (spec "Key"): every
#: coverage row and every flagged bar ≤ ``through`` of the admitted series. No price column.
_QUARANTINE_COVERAGE_SQL = """
    SELECT series_id, first_bar, least(last_bar, %(through)s::date)
    FROM research_price_quarantine_coverage
    WHERE rule_set_version = %(quarantine_version)s
      AND series_id = ANY(%(ids)s)
      AND first_bar <= %(through)s::date
    ORDER BY series_id, first_bar
"""
_QUARANTINE_FLAGS_SQL = """
    SELECT series_id, bar_date, COALESCE(range_usable, TRUE), COALESCE(return_usable, TRUE)
    FROM research_bar_quarantine
    WHERE rule_set_version = %(quarantine_version)s
      AND series_id = ANY(%(ids)s)
      AND bar_date <= %(through)s::date
      AND NOT (COALESCE(range_usable, TRUE) AND COALESCE(return_usable, TRUE))
    ORDER BY series_id, bar_date
"""


def nyse_sessions(start: date, end: date) -> tuple[date, ...]:
    """NYSE sessions in [start, end] (``market_calendar``; half days are sessions)."""
    days = (start + timedelta(days=offset) for offset in range((end - start).days + 1))
    return tuple(day for day in days if market_calendar.us_market_status(day) != "closed")


def _as_float(value: Any) -> float:
    return math.nan if value is None else float(value)


def _regime_labels(conn: psycopg.Connection[Any], sessions: tuple[date, ...]) -> list[str]:
    regimes = MarketRegimeProvider.load_research(conn, through_date=sessions[-1]).for_dates(sessions)
    unobserved = set(regimes.not_evaluable_indices)
    return [
        value.value if value is not None else ("no_benchmark_bar" if index in unobserved else "unclassified")
        for index, value in enumerate(regimes.values)
    ]


def _admitted(conn: psycopg.Connection[Any], universe: Universe) -> dict[int, Any]:
    validated = load_validated_universe(conn)
    selection = load_universe_selection(conn, universe=universe, validated_ids=frozenset(validated))
    return {series.series_id: series for series in selection.admitted}


def quarantine_identity(conn: psycopg.Connection[Any], *, universe: Universe, through: date) -> str:
    """sha256 of the admitted series' quarantine coverage and flags ≤ ``through`` (spec "Key")."""
    params = {
        "ids": sorted(_admitted(conn, universe)),
        "through": through,
        "quarantine_version": QUARANTINE_RULE_SET_VERSION,
    }
    coverage = [
        [series_id, first.isoformat(), last.isoformat()]
        for series_id, first, last in conn.execute(_QUARANTINE_COVERAGE_SQL, params).fetchall()
    ]
    flags = [
        [series_id, bar_date.isoformat(), int(range_usable), int(return_usable)]
        for series_id, bar_date, range_usable, return_usable in conn.execute(_QUARANTINE_FLAGS_SQL, params).fetchall()
    ]
    body = {"rule_set_version": QUARANTINE_RULE_SET_VERSION, "coverage": coverage, "flags": flags}
    return hashlib.sha256(hunt_store.canonical_json(body)).hexdigest()


def load_hunt_panel(conn: psycopg.Connection[Any], *, universe: Universe, through: date) -> HuntPanel:
    """:func:`load_panel_parts` with the regime labels attached (the live path)."""
    return _with_regimes(conn, load_panel_parts(conn, universe=universe, through=through))


def _with_regimes(conn: psycopg.Connection[Any], parts: PanelParts) -> HuntPanel:
    """Regime labels are computed live on every path (spec "Parts"): the key does not cover the benchmark."""
    return HuntPanel(
        sessions=parts.sessions,
        series=parts.series,
        regime_labels=tuple(_regime_labels(conn, parts.sessions)),
        load_counts=parts.load_counts,
    )


def load_panel_parts(conn: psycopg.Connection[Any], *, universe: Universe, through: date) -> PanelParts:
    """Every admitted series' bars ≤ ``through`` on both bases, keyed by NYSE session ordinal.

    Per series: the quarantine-masked read, its split-corrected ratio basis
    (``load_ratio_basis``), and its dividends put on that basis by the reader's own
    scale at the ex-date (spec "Contract decisions", dividends). A bar on a date the
    calendar says was closed is dropped and counted, with any dividend it carried.
    ⚠ Residual: the calendar's ad-hoc closures are transcribed from 1994 on, so before
    that a closure can appear as a session on which no series printed.
    """
    admitted = _admitted(conn, universe)
    first_bar = dict(conn.execute(_FIRST_BAR_SQL, {"ids": sorted(admitted)}).fetchall())
    in_window = {sid: member for sid, member in admitted.items() if (first := first_bar.get(sid)) and first <= through}
    if not in_window:
        raise HuntPanelError(f"universe {universe} admits no series with bars")
    sessions = nyse_sessions(min(first_bar[sid] for sid in in_window), through)
    ordinal_of = {day: ordinal for ordinal, day in enumerate(sessions)}
    counts: dict[str, int] = dict.fromkeys(
        (
            "series_admitted",
            "series_starting_after_window",
            "series_loaded",
            "series_without_evaluated_bars",
            "bars_loaded",
            "off_calendar_bars",
            "dividends_loaded",
            "off_calendar_dividends",
            "terminating_in_window",
        ),
        0,
    )
    counts["series_admitted"] = len(admitted)
    counts["series_starting_after_window"] = len(admitted) - len(in_window)
    panel_series: dict[int, PanelSeries] = {}
    for done, (series_id, member) in enumerate(sorted(in_window.items()), start=1):
        if done % 2000 == 0:
            _LOG.info("hunt panel: %d / %d series loaded", done, len(in_window))
        masked = load_masked_series(conn, series_id, through_date=through)
        if not masked.bars:
            counts["series_without_evaluated_bars"] += 1
            continue
        as_traded = BarSeries(
            dates=tuple(bar.bar_date for bar in masked.bars),
            rows=tuple(
                {"open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume}  # type: ignore[typeddict-item]
                for bar in masked.bars
            ),
        )
        corrected, method = load_ratio_basis(conn, series_id, as_traded, through_date=through)
        if method != "split_corrected":
            raise HuntPanelError(f"series {series_id} reached a ratio basis by {method}; the lane needs as-traded")
        factor_dates, factors, marker = load_split_factors(conn, series_id, through_date=through)
        scale_on = dict(zip(factor_dates, split_scales(factors, stamps_marker=marker), strict=True))

        ordinals, columns = array("l"), [array("d") for _ in range(7)]
        exclusion = bytearray()
        for bar, ratio_row, shares in zip(
            masked.bars, corrected.ratio_basis.rows, corrected.ratio_basis_shares, strict=True
        ):
            ordinal = ordinal_of.get(bar.bar_date)
            if ordinal is None:
                counts["off_calendar_bars"] += 1
                continue
            o, hi, lo, c = _as_float(bar.open), _as_float(bar.high), _as_float(bar.low), _as_float(bar.close)
            volume = _as_float(bar.volume)
            ordinals.append(ordinal)
            for column, value in zip(
                columns,
                (
                    _as_float(ratio_row["open"]),
                    _as_float(ratio_row["high"]),
                    _as_float(ratio_row["low"]),
                    _as_float(ratio_row["close"]),
                    _as_float(shares),
                    o,
                    c,
                ),
                strict=True,
            ):
                column.append(value)
            exclusion.append(bar_exclusion(o, hi, lo, c, volume))
        if not ordinals:
            counts["series_without_evaluated_bars"] += 1
            continue

        dividends: dict[int, float] = {}
        for ex_date, amount in conn.execute(
            _DIVIDEND_SQL,
            {"series_id": series_id, "quarantine_version": QUARANTINE_RULE_SET_VERSION, "through_date": through},
        ).fetchall():
            ordinal = ordinal_of.get(ex_date)
            if ordinal is None:
                counts["off_calendar_dividends"] += 1
                continue
            on_basis = corrected_price(amount, scale_on[ex_date])
            dividends[ordinal] = math.nan if on_basis is None else float(on_basis)

        terminal_ordinal = termination_class = None
        if member.termination is not None and member.last_bar is not None and member.last_bar <= sessions[-1]:
            # The stored last_bar, never the loaded end (#3362); a non-session last bar maps
            # to the session before it.
            terminal_ordinal = bisect_right(sessions, member.last_bar) - 1
            termination_class = classify_termination(member.termination)
            counts["terminating_in_window"] += 1

        panel_series[series_id] = PanelSeries(
            series_id=series_id,
            ratio=hunt_view.Bars(ordinals, columns[0], columns[1], columns[2], columns[3], columns[4]),
            traded_open=columns[5],
            traded_close=columns[6],
            exclusion=bytes(exclusion),
            dividends=MappingProxyType(dividends),
            terminal_ordinal=terminal_ordinal,
            termination_class=termination_class,
        )
        counts["bars_loaded"] += len(ordinals)
        counts["dividends_loaded"] += len(dividends)
    counts["series_loaded"] = len(panel_series)
    return PanelParts(sessions=sessions, series=MappingProxyType(panel_series), load_counts=MappingProxyType(counts))


def compute_trial(
    conn: psycopg.Connection[Any],
    *,
    split: str,
    split_start: date,
    split_end: date | None,
    embargo_sessions: int,
    universe: Universe,
    lag: int,
    h: int,
    entry_point: Point,
    exit_point: Point,
    sign: int,
    selection: float,
    constants: Mapping[str, Any],
    commission: float,
    signal: hunt_compute.Signal,
    universe_identity: Mapping[str, str],
    model_id: str,
    read_universe_identity: Callable[[], Mapping[str, str]],
) -> PanelOutcome:
    """Load the split's panel and compute every cell (embargo 0 for discovery).

    ``universe_identity`` is the REGISTERED identity (``evaluate`` checked it equal to the
    live one before registering); ``read_universe_identity`` re-reads the live one.
    """
    if split_end is None:
        raise HuntPanelError("the holdout end session is frozen in its declaration (#3385 slice 2b)")
    store_key = content_sha256 = quarantine = None
    if split == "discovery":
        quarantine = quarantine_identity(conn, universe=universe, through=split_end)
        store_key = hunt_store.store_key(
            through=split_end, universe_identity=universe_identity, quarantine_identity=quarantine, model_id=model_id
        )

        def verify() -> None:
            if dict(read_universe_identity()) != dict(universe_identity):
                raise HuntPanelError("the universe identity changed while the store was built; retry")
            if quarantine_identity(conn, universe=universe, through=split_end) != quarantine:
                raise HuntPanelError("the quarantine flags changed while the store was built; retry")

        stored = hunt_store.load_or_build(
            STORE_ROOT,
            key=store_key,
            through=split_end,
            build=lambda: load_panel_parts(conn, universe=universe, through=split_end),
            verify=verify,
        )
        content_sha256 = stored.content_sha256
        panel = _with_regimes(conn, stored.parts)
    else:
        panel = load_hunt_panel(conn, universe=universe, through=split_end)
    params = ComputeParams(
        split_start=split_start,
        split_end=split_end,
        embargo=0 if split == "discovery" else embargo_sessions,
        lag=lag,
        h=h,
        entry_point=entry_point,
        exit_point=exit_point,
        sign=sign,
        selection=selection,
        constants=constants,
        commission=commission,
    )
    outcome = hunt_compute.compute_panel(panel, params, signal)
    if quarantine is not None and quarantine_identity(conn, universe=universe, through=split_end) != quarantine:
        raise HuntPanelError("the quarantine flags changed during the computation; retry the registered trial")
    load = {**outcome.statistics["load"], "store_key": store_key, "store_content_sha256": content_sha256}
    return PanelOutcome(outcome.status, {**outcome.statistics, "load": load}, outcome.active_series)


__all__ = [
    "STORE_ROOT",
    "HuntPanelError",
    "compute_trial",
    "load_hunt_panel",
    "load_panel_parts",
    "nyse_sessions",
    "quarantine_identity",
]
