"""Read model over ``engine_book_risk_snapshots`` (#3543 slice 2): the latest stored snapshot, the recent
sessions, and the job's last fire.

Reads stored rows only -- nothing is re-measured here, and nothing reads this to refuse, size or rebalance
(the snapshot is measurement only, spec ``docs/proposals/risk/2026-10-02-3543-engine-book-risk-snapshot.md``).
Rows are filtered to the running ``ENGINE_BOOK_RISK_POLICY``: a row measured under another policy version
answers a different question, so it is never shown as the current one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.services.ai_trial_status import JobFire, job_fire
from app.services.engine_book_risk import ENGINE_BOOK_RISK_POLICY
from app.workers.scheduler import JOB_ENGINE_BOOK_RISK_SNAPSHOT

Conn = psycopg.Connection[Any]

#: Sessions listed under the latest snapshot, newest first (the latest included).
RECENT_LIMIT: Final = 20


@dataclass(frozen=True)
class RecentSnapshot:
    session_date: date
    measured_at: datetime
    capital_usd: Decimal
    gross_usd: Decimal
    hist_vol_pct: Decimal | None
    ewma_vol_pct: Decimal | None
    beta: Decimal | None
    stress_2020_pct: Decimal
    stress_2022_pct: Decimal
    stale_count: int
    history_status: str
    #: Names of the checks whose ``flagged`` is true, in stored key order.
    flagged: list[str]


@dataclass(frozen=True)
class LatestSnapshot:
    session_date: date
    measured_at: datetime
    pool_event_id: int
    capital_usd: Decimal
    gross_usd: Decimal
    position_count: int
    instrument_count: int
    open_trade_count: int
    cost_marked_count: int
    stale_count: int
    largest_share_pct: Decimal | None
    top5_share_pct: Decimal | None
    hhi: Decimal | None
    hist_vol_pct: Decimal | None
    ewma_vol_pct: Decimal | None
    beta: Decimal | None
    vol_n_obs: int
    beta_n_obs: int
    sample_first: date | None
    sample_last: date | None
    history_status: str
    beta_defaulted_count: int
    beta_defaulted_weight_pct: Decimal
    stress_2020_pct: Decimal
    stress_2022_pct: Decimal
    checks: dict[str, dict[str, Any]]
    #: The stored per-position rows, each with ``symbol`` added at read time (``None`` when unknown).
    positions: list[dict[str, Any]]


@dataclass(frozen=True)
class EngineBookRiskStatus:
    policy_version: str
    job: JobFire
    latest: LatestSnapshot | None
    recent: list[RecentSnapshot]


_LATEST_FIELDS: Final = tuple(LatestSnapshot.__dataclass_fields__)

_ROWS_SQL: Final = """
    SELECT session_date, measured_at, pool_event_id, capital_usd, gross_usd, position_count, instrument_count,
           open_trade_count, cost_marked_count, stale_count, largest_share_pct, top5_share_pct, hhi,
           hist_vol_pct, ewma_vol_pct, beta, vol_n_obs, beta_n_obs, sample_first, sample_last, history_status,
           beta_defaulted_count, beta_defaulted_weight_pct, stress_2020_pct, stress_2022_pct, checks, positions
    FROM engine_book_risk_snapshots
    WHERE policy_version = %s
    ORDER BY session_date DESC
    LIMIT %s
"""


def _flagged(checks: dict[str, dict[str, Any]]) -> list[str]:
    return [name for name, check in checks.items() if check.get("flagged") is True]


def load_engine_book_risk_status(conn: Conn, *, now: datetime | None = None) -> EngineBookRiskStatus:
    job = job_fire(conn, JOB_ENGINE_BOOK_RISK_SNAPSHOT, now or datetime.now(UTC))
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(_ROWS_SQL, (ENGINE_BOOK_RISK_POLICY, RECENT_LIMIT)).fetchall()
        if not rows:
            return EngineBookRiskStatus(ENGINE_BOOK_RISK_POLICY, job, None, [])
        head = rows[0]
        ids = sorted({int(p["instrument_id"]) for p in head["positions"]})
        cur.execute("SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%s::bigint[])", (ids,))
        symbols = {int(r["instrument_id"]): r["symbol"] for r in cur.fetchall()}
    latest = LatestSnapshot(
        **{
            **{name: head[name] for name in _LATEST_FIELDS if name != "positions"},
            "positions": [{**p, "symbol": symbols.get(int(p["instrument_id"]))} for p in head["positions"]],
        }
    )
    recent = [
        RecentSnapshot(
            session_date=r["session_date"],
            measured_at=r["measured_at"],
            capital_usd=r["capital_usd"],
            gross_usd=r["gross_usd"],
            hist_vol_pct=r["hist_vol_pct"],
            ewma_vol_pct=r["ewma_vol_pct"],
            beta=r["beta"],
            stress_2020_pct=r["stress_2020_pct"],
            stress_2022_pct=r["stress_2022_pct"],
            stale_count=r["stale_count"],
            history_status=r["history_status"],
            flagged=_flagged(r["checks"]),
        )
        for r in rows
    ]
    return EngineBookRiskStatus(ENGINE_BOOK_RISK_POLICY, job, latest, recent)
