"""#3381 slice 1: the daily eToro crowd-positioning recorder.

eToro serves no history for per-instrument crowd positioning (`buyHoldingPct`, `sellHoldingPct`,
`holdingPct`, popularity and trader-change windows; ``.claude/skills/data-sources/etoro-api.md``), so the
recording clock is the dataset. Each call appends ONE ``etoro_crowd_snapshots`` row, failed collections
included, and on success every instrument row of the complete snapshot, raw item and all
(``sql/424_etoro_crowd_observations.sql``).

The clock is ``observed_at``, the fetch time of the page a row arrived on. Nothing here backfills, updates or
de-duplicates: two runs on one day are two snapshots.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import psycopg

from app.providers.market_data import BroadMarketSnapshot, MarketSnapshotInstrument

logger = logging.getLogger(__name__)

SNAPSHOT_COMPLETE: Final = "complete"
SNAPSHOT_FAILED: Final = "failed"

_COPY_OBSERVATIONS_SQL: Final = (
    "COPY etoro_crowd_observations (snapshot_id, instrument_id, observed_at, buy_holding_pct, sell_holding_pct, "
    "holding_pct, popularity_uniques_7d, popularity_uniques_14d, popularity_uniques_30d, traders_change_7d, "
    "traders_change_14d, traders_change_30d, raw) FROM STDIN"
)

_INSERT_SNAPSHOT_SQL: Final = """
INSERT INTO etoro_crowd_snapshots (
    started_at, finished_at, status, request_params, pages, reported_total_items, discarded_items,
    recorded_items, error
) VALUES (
    %(started_at)s, %(finished_at)s, %(status)s, %(request_params)s::jsonb, %(pages)s,
    %(reported_total_items)s, %(discarded_items)s, %(recorded_items)s, %(error)s
)
RETURNING snapshot_id
"""


@dataclass(frozen=True)
class CrowdSnapshotResult:
    snapshot_id: int
    recorded_items: int
    #: Rows with both buy and sell holding % present, the coverage figure the acceptance reads.
    buy_sell_covered: int


class CrowdSnapshotFailed(RuntimeError):
    """The collection failed. Its ``failed`` snapshot row is already committed."""

    def __init__(self, snapshot_id: int, error: str) -> None:
        super().__init__(f"crowd snapshot {snapshot_id} failed: {error}")
        self.snapshot_id = snapshot_id


def _raw_json(record: MarketSnapshotInstrument) -> str:
    # allow_nan=False: a non-JSON value refuses the snapshot instead of storing an unparseable raw.
    return json.dumps(record.raw, allow_nan=False, sort_keys=True, default=str)


def _observation_row(record: MarketSnapshotInstrument) -> tuple[object, ...]:
    if record.observed_at is None:
        raise ValueError(f"instrument {record.instrument_id} has no page fetch time")
    return (
        record.instrument_id,
        record.observed_at,
        record.buy_holding_pct,
        record.sell_holding_pct,
        record.holding_pct,
        record.popularity_uniques_7d,
        record.popularity_uniques_14d,
        record.popularity_uniques_30d,
        record.traders_7d_change,
        record.traders_14d_change,
        record.traders_30d_change,
        _raw_json(record),
    )


def _insert_snapshot(conn: psycopg.Connection[Any], params: Mapping[str, object]) -> int:
    row = conn.execute(_INSERT_SNAPSHOT_SQL, params).fetchone()
    if row is None:  # pragma: no cover - INSERT … RETURNING always returns the row
        raise RuntimeError("etoro_crowd_snapshots insert returned no row")
    return int(row[0])


def record_crowd_snapshot(
    conn: psycopg.Connection[Any],
    fetch: Callable[[], BroadMarketSnapshot],
    *,
    request_params: Mapping[str, object],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> CrowdSnapshotResult:
    """Fetch one broad-market snapshot and append it.

    A failure in the fetch or in building any row commits a ``failed`` snapshot row carrying the error and
    raises :class:`CrowdSnapshotFailed`, so the job run fails too. Rows are written only for a complete
    snapshot, in the same transaction as its header.
    """
    started_at = clock()
    params_json = json.dumps(request_params, sort_keys=True)
    try:
        snapshot = fetch()
        rows = [_observation_row(record) for record in snapshot.instruments]
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        with conn.transaction():
            snapshot_id = _insert_snapshot(
                conn,
                {
                    "started_at": started_at,
                    "finished_at": max(started_at, clock()),
                    "status": SNAPSHOT_FAILED,
                    "request_params": params_json,
                    "pages": None,
                    "reported_total_items": None,
                    "discarded_items": None,
                    "recorded_items": 0,
                    "error": error,
                },
            )
        logger.warning("etoro crowd snapshot %d failed: %s", snapshot_id, error)
        raise CrowdSnapshotFailed(snapshot_id, error) from exc

    with conn.transaction():
        snapshot_id = _insert_snapshot(
            conn,
            {
                "started_at": started_at,
                "finished_at": max(started_at, clock()),
                "status": SNAPSHOT_COMPLETE,
                "request_params": params_json,
                "pages": snapshot.pages,
                "reported_total_items": snapshot.reported_total_items,
                "discarded_items": snapshot.discarded_items,
                "recorded_items": len(rows),
                "error": None,
            },
        )
        with conn.cursor() as cur:
            with cur.copy(_COPY_OBSERVATIONS_SQL) as copy:
                for row in rows:
                    copy.write_row((snapshot_id, *row))
    covered = sum(
        1
        for record in snapshot.instruments
        if record.buy_holding_pct is not None and record.sell_holding_pct is not None
    )
    return CrowdSnapshotResult(snapshot_id=snapshot_id, recorded_items=len(rows), buy_sell_covered=covered)


__all__ = [
    "SNAPSHOT_COMPLETE",
    "SNAPSHOT_FAILED",
    "CrowdSnapshotFailed",
    "CrowdSnapshotResult",
    "record_crowd_snapshot",
]
