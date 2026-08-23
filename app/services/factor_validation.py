"""Published-reference validation for eBull factor constructions (#2912).

This module produces a diagnostic long-short spread from S-2's exact ranking
inputs. It is a sign/timing identity check, not a tradable arm: no result here
may be promoted or quoted as net strategy performance.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, Literal

import numpy as np
import psycopg
from psycopg.rows import dict_row

from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.strategies.s2_cross_sectional_momentum import (
    DECILE,
    ELIGIBILITY_BARS,
    LOOKBACK_BARS,
    MIN_CLOSE,
    MIN_CROSS_SECTION,
    SKIP_BARS,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import UniverseSelection, load_universe_selection

MIN_OVERLAP_MONTHS: Final = 24
MIN_CONSTRUCTION_CORRELATION: Final = 0.20
MIN_REFERENCE_CONTROL_CORRELATION: Final = 0.50
LAST_COMPLETE_ENTRY_MONTH: Final = date(2024, 8, 31)

ComparisonKind = Literal["construction", "reference_control"]


@dataclass(frozen=True)
class ReferenceSeries:
    source: str
    dataset_key: str
    series_key: str
    snapshot_id: int
    response_sha256: str
    parser_version: str
    unit: str
    values: dict[tuple[int, int], float]


@dataclass(frozen=True)
class MomentumFactorCensus:
    validated_instruments: int
    selection: UniverseSelection
    bars_read: int
    rebalance_dates: int
    rebalances_without_eligible_names: int
    thin_panels: int
    selected_member_legs: int
    usable_member_legs: int
    rejected_member_endpoints: int
    factor_months: int


@dataclass(frozen=True)
class ConstructedFactor:
    values: dict[tuple[int, int], float]
    census: MomentumFactorCensus


@dataclass(frozen=True)
class FactorComparison:
    label: str
    overlap_start: date
    overlap_end: date
    overlap_months: int
    correlation: float | None
    alpha: float
    beta: float
    reference_lag_one_correlation: float | None
    reference_lead_one_correlation: float | None
    passed: bool
    failures: tuple[str, ...]


_MOMENTUM_FACTOR_SQL = """
WITH admitted AS MATERIALIZED (
    SELECT *
    FROM unnest(%(series_ids)s::bigint[], %(name_keys)s::bigint[])
         AS admitted(series_id, name_key)
),
bars AS MATERIALIZED (
    SELECT d.series_id,
           a.name_key,
           d.bar_date,
           CASE WHEN COALESCE(q.return_usable, TRUE) THEN d.close END AS close,
           CASE WHEN COALESCE(q.return_usable, TRUE) THEN d.adj_close END AS adj_close
    FROM admitted a
    JOIN research_price_daily d USING (series_id)
    JOIN research_price_quarantine_coverage cov
      ON cov.series_id = d.series_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN research_bar_quarantine q
      ON q.series_id = d.series_id
     AND q.bar_date = d.bar_date
     AND q.rule_set_version = %(quarantine_version)s
),
weekday_calendar AS (
    SELECT bar_date, lag(bar_date) OVER (ORDER BY bar_date) AS previous
    FROM (
        SELECT DISTINCT bar_date
        FROM bars
        WHERE extract(isodow FROM bar_date) < 6
    ) calendar
),
rebalances AS MATERIALIZED (
    SELECT bar_date,
           lead(bar_date) OVER (ORDER BY bar_date) AS next_bar_date
    FROM weekday_calendar
    WHERE previous IS NOT NULL
      AND date_trunc('month', bar_date) <> date_trunc('month', previous)
),
windowed AS MATERIALIZED (
    SELECT series_id,
           name_key,
           bar_date,
           close,
           adj_close,
           lag(close, %(skip)s) OVER series_window AS c_skip,
           lag(close, %(lookback)s) OVER series_window AS c_back,
           row_number() OVER series_window AS rn
    FROM bars
    WINDOW series_window AS (PARTITION BY series_id ORDER BY bar_date)
),
eligible AS MATERIALIZED (
    SELECT w.series_id,
           w.name_key,
           w.bar_date,
           r.next_bar_date,
           w.adj_close AS entry_adj_close,
           w.c_skip / w.c_back - 1 AS score
    FROM windowed w
    JOIN rebalances r ON r.bar_date = w.bar_date
    WHERE r.bar_date <= %(last_entry)s
      AND w.rn >= %(eligibility)s
      AND w.close IS NOT NULL
      AND w.close >= %(floor)s
      AND w.c_skip IS NOT NULL AND w.c_skip > 0
      AND w.c_back IS NOT NULL AND w.c_back > 0
),
ranked AS MATERIALIZED (
    SELECT e.*,
           row_number() OVER (
               PARTITION BY e.bar_date ORDER BY e.score DESC, e.name_key ASC
           ) AS top_position,
           row_number() OVER (
               PARTITION BY e.bar_date ORDER BY e.score ASC, e.name_key ASC
           ) AS bottom_position,
           count(*) OVER (PARTITION BY e.bar_date) AS participant_count
    FROM eligible e
),
selected AS MATERIALIZED (
    SELECT r.*,
           CASE
             WHEN r.top_position <= r.participant_count / %(decile)s THEN 'top'
             WHEN r.bottom_position <= r.participant_count / %(decile)s THEN 'bottom'
           END AS leg
    FROM ranked r
    WHERE r.participant_count >= %(minimum_panel)s
      AND (r.top_position <= r.participant_count / %(decile)s
           OR r.bottom_position <= r.participant_count / %(decile)s)
),
member_returns AS MATERIALIZED (
    SELECT s.*,
           CASE
             WHEN s.entry_adj_close IS NOT NULL AND s.entry_adj_close > 0
              AND future.adj_close IS NOT NULL AND future.adj_close > 0
             THEN future.adj_close / s.entry_adj_close - 1
           END AS member_return
    FROM selected s
    LEFT JOIN bars future
      ON future.series_id = s.series_id
     AND future.bar_date = s.next_bar_date
),
panel_returns AS (
    SELECT bar_date,
           max(next_bar_date) AS next_bar_date,
           max(participant_count) AS participant_count,
           count(*) FILTER (WHERE leg = 'top') AS top_selected,
           count(*) FILTER (WHERE leg = 'bottom') AS bottom_selected,
           count(member_return) FILTER (WHERE leg = 'top') AS top_usable,
           count(member_return) FILTER (WHERE leg = 'bottom') AS bottom_usable,
           avg(member_return) FILTER (WHERE leg = 'top') AS top_return,
           avg(member_return) FILTER (WHERE leg = 'bottom') AS bottom_return
    FROM member_returns
    GROUP BY bar_date
),
panel_counts AS (
    SELECT bar_date, max(participant_count) AS participant_count
    FROM ranked
    GROUP BY bar_date
)
SELECT p.bar_date,
       p.next_bar_date,
       p.participant_count,
       p.top_selected,
       p.bottom_selected,
       p.top_usable,
       p.bottom_usable,
       p.top_return,
       p.bottom_return,
       (SELECT count(*) FROM rebalances WHERE bar_date <= %(last_entry)s) AS rebalance_dates,
       (SELECT count(*) FROM panel_counts) AS panels_with_eligible,
       (SELECT count(*) FROM panel_counts WHERE participant_count < %(minimum_panel)s) AS thin_panels,
       (SELECT count(*) FROM bars) AS bars_read
FROM panel_returns p
ORDER BY p.bar_date
"""


def construct_s2_momentum_factor(conn: psycopg.Connection[Any]) -> ConstructedFactor:
    """Construct the frozen diagnostic spread over the full admitted corpus."""
    validated = load_validated_universe(conn)
    selection = load_universe_selection(
        conn,
        universe="survivorship_free",
        validated_ids=frozenset(validated),
    )
    series_ids = [item.series_id for item in selection.admitted]
    name_keys = [item.name_key for item in selection.admitted]
    params = {
        "series_ids": series_ids,
        "name_keys": name_keys,
        "quarantine_version": QUARANTINE_RULE_SET_VERSION,
        "skip": SKIP_BARS,
        "lookback": LOOKBACK_BARS,
        "eligibility": ELIGIBILITY_BARS,
        "floor": MIN_CLOSE,
        "decile": DECILE,
        "minimum_panel": MIN_CROSS_SECTION,
        "last_entry": LAST_COMPLETE_ENTRY_MONTH,
    }
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(_MOMENTUM_FACTOR_SQL, params)
        rows = cursor.fetchall()
    if not rows:
        raise RuntimeError("S-2 diagnostic factor query produced no eligible full-population panels")

    values: dict[tuple[int, int], float] = {}
    selected = 0
    usable = 0
    for row in rows:
        selected += int(row["top_selected"]) + int(row["bottom_selected"])
        usable += int(row["top_usable"]) + int(row["bottom_usable"])
        top = row["top_return"]
        bottom = row["bottom_return"]
        if int(row["top_usable"]) == 0 or int(row["bottom_usable"]) == 0 or top is None or bottom is None:
            continue
        when = row["bar_date"]
        factor_return = float(top - bottom)
        if not math.isfinite(factor_return):
            raise RuntimeError(f"non-finite diagnostic factor return at {when}")
        values[(when.year, when.month)] = factor_return

    first = rows[0]
    rebalance_dates = int(first["rebalance_dates"])
    panels_with_eligible = int(first["panels_with_eligible"])
    census = MomentumFactorCensus(
        validated_instruments=len(validated),
        selection=selection,
        bars_read=int(first["bars_read"]),
        rebalance_dates=rebalance_dates,
        rebalances_without_eligible_names=rebalance_dates - panels_with_eligible,
        thin_panels=int(first["thin_panels"]),
        selected_member_legs=selected,
        usable_member_legs=usable,
        rejected_member_endpoints=selected - usable,
        factor_months=len(values),
    )
    return ConstructedFactor(values=values, census=census)


def load_reference_series(
    conn: psycopg.Connection[Any],
    *,
    source: str,
    dataset_key: str,
    series_key: str,
    response_sha256: str,
    parser_version: str,
) -> ReferenceSeries:
    """Load one series from an exact accepted immutable snapshot identity."""
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT snapshot_id, source, response_sha256, parser_version
            FROM reference_data_snapshots
            WHERE source = %(source)s
              AND dataset_key = %(dataset_key)s
              AND response_sha256 = %(response_sha256)s
              AND parser_version = %(parser_version)s
              AND parse_status = 'accepted'
            """,
            {
                "source": source,
                "dataset_key": dataset_key,
                "response_sha256": response_sha256,
                "parser_version": parser_version,
            },
        )
        snapshot = cursor.fetchone()
        if snapshot is None:
            raise RuntimeError(
                f"no accepted reference snapshot for {source}/{dataset_key!r} at {response_sha256}/{parser_version}"
            )
        cursor.execute(
            """
            SELECT observation_date, value, unit
            FROM reference_data_observations
            WHERE snapshot_id = %(snapshot_id)s AND series_key = %(series_key)s
            ORDER BY observation_date
            """,
            {"snapshot_id": snapshot["snapshot_id"], "series_key": series_key},
        )
        rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"snapshot {snapshot['snapshot_id']} has no series {series_key!r}")
    units = {str(row["unit"]) for row in rows}
    if len(units) != 1:
        raise RuntimeError(f"reference series {dataset_key}/{series_key} has mixed units {sorted(units)}")
    values = {(row["observation_date"].year, row["observation_date"].month): float(row["value"]) for row in rows}
    if len(values) != len(rows):
        raise RuntimeError(f"reference series {dataset_key}/{series_key} has duplicate calendar months")
    return ReferenceSeries(
        source=str(snapshot["source"]),
        dataset_key=dataset_key,
        series_key=series_key,
        snapshot_id=int(snapshot["snapshot_id"]),
        response_sha256=str(snapshot["response_sha256"]),
        parser_version=str(snapshot["parser_version"]),
        unit=units.pop(),
        values=values,
    )


def _shift_month(month: tuple[int, int], displacement: int) -> tuple[int, int]:
    ordinal = month[0] * 12 + month[1] - 1 + displacement
    return ordinal // 12, ordinal % 12 + 1


def _paired(
    dependent: dict[tuple[int, int], float],
    reference: dict[tuple[int, int], float],
    *,
    reference_displacement: int = 0,
) -> tuple[np.ndarray, np.ndarray, tuple[tuple[int, int], ...]]:
    months = tuple(sorted(month for month in dependent if _shift_month(month, reference_displacement) in reference))
    y = np.asarray([dependent[month] for month in months], dtype=float)
    x = np.asarray([reference[_shift_month(month, reference_displacement)] for month in months], dtype=float)
    return y, x, months


def _correlation(y: np.ndarray, x: np.ndarray) -> float:
    if len(y) < 2 or float(np.std(y)) == 0.0 or float(np.std(x)) == 0.0:
        raise ValueError("correlation requires at least two non-constant paired observations")
    value = float(np.corrcoef(y, x)[0, 1])
    if not math.isfinite(value):
        raise ValueError("correlation is non-finite")
    return value


def _available_correlation(y: np.ndarray, x: np.ndarray) -> float | None:
    """Return a correlation when defined, otherwise preserve a reportable absence."""
    try:
        return _correlation(y, x)
    except ValueError:
        return None


def compare_factor_series(
    *,
    label: str,
    dependent: dict[tuple[int, int], float],
    reference: dict[tuple[int, int], float],
    kind: ComparisonKind = "construction",
) -> FactorComparison:
    """Apply the declaration's frozen correlation/regression failure rules."""
    y, x, months = _paired(dependent, reference)
    if not months:
        raise ValueError(f"{label}: no overlapping months")
    correlation = _available_correlation(y, x)
    design = np.column_stack((np.ones(len(x)), x))
    alpha, beta = (float(value) for value in np.linalg.lstsq(design, y, rcond=None)[0])
    lag_y, lag_x, _ = _paired(dependent, reference, reference_displacement=-1)
    lead_y, lead_x, _ = _paired(dependent, reference, reference_displacement=1)
    lag = _available_correlation(lag_y, lag_x)
    lead = _available_correlation(lead_y, lead_x)

    threshold = MIN_CONSTRUCTION_CORRELATION if kind == "construction" else MIN_REFERENCE_CONTROL_CORRELATION
    failures: list[str] = []
    if len(months) < MIN_OVERLAP_MONTHS:
        failures.append(f"overlap {len(months)} < {MIN_OVERLAP_MONTHS}")
    if correlation is None:
        failures.append("contemporaneous correlation is unavailable")
    elif correlation < threshold:
        failures.append(f"correlation {correlation:.6f} < {threshold:.2f}")
    if beta <= 0:
        failures.append(f"beta {beta:.6f} is not positive")
    if kind == "construction":
        if lag is None:
            failures.append("reference lag-one correlation is unavailable")
        if lead is None:
            failures.append("reference lead-one correlation is unavailable")
        if correlation is not None and lag is not None and lead is not None:
            displaced_max = max(abs(lag), abs(lead))
            if abs(correlation) < displaced_max:
                failures.append(
                    f"contemporaneous |r| {abs(correlation):.6f} is below displaced max {displaced_max:.6f}"
                )
    return FactorComparison(
        label=label,
        overlap_start=date(months[0][0], months[0][1], 1),
        overlap_end=date(months[-1][0], months[-1][1], 1),
        overlap_months=len(months),
        correlation=correlation,
        alpha=alpha,
        beta=beta,
        reference_lag_one_correlation=lag,
        reference_lead_one_correlation=lead,
        passed=not failures,
        failures=tuple(failures),
    )


__all__ = [
    "LAST_COMPLETE_ENTRY_MONTH",
    "MIN_CONSTRUCTION_CORRELATION",
    "MIN_OVERLAP_MONTHS",
    "MIN_REFERENCE_CONTROL_CORRELATION",
    "ConstructedFactor",
    "FactorComparison",
    "MomentumFactorCensus",
    "ReferenceSeries",
    "compare_factor_series",
    "construct_s2_momentum_factor",
    "load_reference_series",
]
