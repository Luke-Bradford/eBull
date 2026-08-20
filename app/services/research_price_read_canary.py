"""Bounded, outcome-free probe of the research-corpus read/decode path.

This is deliberately not a small backtest.  It measures the database and
Python object work that happens before a strategy is evaluated, then stops.
The fixed selection is spread across the declared ``bar_count`` distribution
and named by a digest so two measurements cannot silently use different data.
"""

from __future__ import annotations

import hashlib
import json
import resource
import sys
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Final

import psycopg

from app.services.research_price_structure_store import (
    QUARANTINE_RULE_SET_VERSION,
    load_arms,
)

READ_DECODE_CANARY_SERIES: Final[int] = 5
READ_DECODE_CANARY_MAX_DECLARED_BARS: Final[int] = 100_000
READ_DECODE_CANARY_MAX_RSS_BYTES: Final[int] = 8 * 1024**3

_SERIES_CENSUS_SQL = """
    SELECT s.series_id,
           s.bar_count,
           s.first_bar,
           s.last_bar,
           (cov.series_id IS NOT NULL
            AND cov.first_bar <= s.first_bar
            AND cov.last_bar >= s.last_bar) AS coverage_current
    FROM research_price_series s
    LEFT JOIN research_price_quarantine_coverage cov
      ON cov.series_id = s.series_id
     AND cov.rule_set_version = %(quarantine_version)s
    WHERE s.bar_count IS NOT NULL
      AND s.bar_count > 0
      AND s.first_bar IS NOT NULL
      AND s.last_bar IS NOT NULL
    ORDER BY s.bar_count, s.series_id
"""


class ReadDecodeCanaryRefused(RuntimeError):
    """The bounded read could not be shown to remain inside its contract."""


@dataclass(frozen=True)
class ReadDecodeCanaryConfig:
    series_count: int = READ_DECODE_CANARY_SERIES
    max_declared_bars: int = READ_DECODE_CANARY_MAX_DECLARED_BARS
    max_rss_bytes: int = READ_DECODE_CANARY_MAX_RSS_BYTES
    through_date: date | None = None
    expected_selection_digest: str | None = None

    def __post_init__(self) -> None:
        if self.series_count < 2:
            raise ValueError("series_count must be at least 2 so the canary spans the corpus")
        if self.max_declared_bars <= 0:
            raise ValueError("max_declared_bars must be positive")
        if self.max_rss_bytes <= 0:
            raise ValueError("max_rss_bytes must be positive")


@dataclass(frozen=True)
class ReadDecodeSeries:
    series_id: int
    declared_bars: int
    first_bar: date
    last_bar: date


@dataclass(frozen=True)
class ReadDecodeCanaryPlan:
    census_series: int
    eligible_series: int
    fail_closed_series: int
    selected: tuple[ReadDecodeSeries, ...]
    selection_digest: str

    @property
    def declared_bars(self) -> int:
        return sum(item.declared_bars for item in self.selected)


@dataclass(frozen=True)
class ReadDecodeCanaryReport:
    plan: ReadDecodeCanaryPlan
    query_count: int
    decoded_bars: int
    arms_identical_shape: bool
    wall_s: float
    cpu_s: float
    peak_rss_bytes: int
    stopped_after_selected_series: bool = True


def _peak_rss_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def plan_read_decode_canary(
    conn: psycopg.Connection[Any],
    *,
    config: ReadDecodeCanaryConfig | None = None,
) -> ReadDecodeCanaryPlan:
    """Select deterministic bar-count strata without touching the bar table."""
    chosen_config = config or ReadDecodeCanaryConfig()
    rows = conn.execute(
        _SERIES_CENSUS_SQL,
        {"quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    eligible_rows = [row for row in rows if bool(row[4])]
    if len(eligible_rows) < chosen_config.series_count:
        raise ReadDecodeCanaryRefused(
            f"only {len(eligible_rows)} eligible series; need {chosen_config.series_count} for the declared strata"
        )

    last = len(eligible_rows) - 1
    indexes = tuple(
        (position * last) // (chosen_config.series_count - 1) for position in range(chosen_config.series_count)
    )
    if len(set(indexes)) != chosen_config.series_count:
        raise ReadDecodeCanaryRefused("the stratified selection did not produce distinct series")
    selected = tuple(
        ReadDecodeSeries(
            series_id=int(eligible_rows[index][0]),
            declared_bars=int(eligible_rows[index][1]),
            first_bar=eligible_rows[index][2],
            last_bar=eligible_rows[index][3],
        )
        for index in indexes
    )
    digest_payload = [
        [item.series_id, item.declared_bars, item.first_bar.isoformat(), item.last_bar.isoformat()] for item in selected
    ]
    digest = hashlib.sha256(json.dumps(digest_payload, separators=(",", ":")).encode()).hexdigest()
    plan = ReadDecodeCanaryPlan(
        census_series=len(rows),
        eligible_series=len(eligible_rows),
        fail_closed_series=len(rows) - len(eligible_rows),
        selected=selected,
        selection_digest=digest,
    )
    if plan.declared_bars > chosen_config.max_declared_bars:
        raise ReadDecodeCanaryRefused(
            f"selected series declare {plan.declared_bars:,} bars, above canary cap "
            f"{chosen_config.max_declared_bars:,}; no bar rows were read"
        )
    expected = chosen_config.expected_selection_digest
    if expected is not None and digest != expected:
        raise ReadDecodeCanaryRefused(
            f"selection digest changed: expected {expected}, observed {digest}; no bar rows were read"
        )
    return plan


def run_read_decode_canary(
    conn: psycopg.Connection[Any],
    *,
    config: ReadDecodeCanaryConfig | None = None,
) -> ReadDecodeCanaryReport:
    """Read both arms for only the selected series and stop unconditionally."""
    chosen_config = config or ReadDecodeCanaryConfig()
    wall_started = time.monotonic()
    cpu_started = time.process_time()
    plan = plan_read_decode_canary(conn, config=chosen_config)
    decoded_bars = 0
    same_shape = True
    for selected in plan.selected:
        arms = load_arms(conn, selected.series_id, through_date=chosen_config.through_date)
        masked = arms["masked"]
        admitted = arms["admitted"]
        same_shape = same_shape and len(masked.bars) == len(admitted.bars)
        decoded_bars += len(masked.bars)
        if len(masked.bars) > selected.declared_bars:
            raise ReadDecodeCanaryRefused(
                f"series {selected.series_id} decoded {len(masked.bars):,} bars but declares "
                f"{selected.declared_bars:,}; census data is stale"
            )

    peak_rss = _peak_rss_bytes()
    if peak_rss > chosen_config.max_rss_bytes:
        raise ReadDecodeCanaryRefused(
            f"process lifetime peak RSS {peak_rss:,} exceeds canary cap {chosen_config.max_rss_bytes:,}"
        )
    if not same_shape:
        raise ReadDecodeCanaryRefused("masked and admitted arms did not decode the same bar shape")
    return ReadDecodeCanaryReport(
        plan=plan,
        query_count=1 + len(plan.selected),
        decoded_bars=decoded_bars,
        arms_identical_shape=same_shape,
        wall_s=time.monotonic() - wall_started,
        cpu_s=time.process_time() - cpu_started,
        peak_rss_bytes=peak_rss,
    )


__all__ = [
    "READ_DECODE_CANARY_MAX_DECLARED_BARS",
    "READ_DECODE_CANARY_MAX_RSS_BYTES",
    "READ_DECODE_CANARY_SERIES",
    "ReadDecodeCanaryConfig",
    "ReadDecodeCanaryPlan",
    "ReadDecodeCanaryRefused",
    "ReadDecodeCanaryReport",
    "ReadDecodeSeries",
    "plan_read_decode_canary",
    "run_read_decode_canary",
]
