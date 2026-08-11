"""Compact official account-equity evidence for Foundation F-0 (#2559)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg

from app.providers.broker import BrokerAccountRiskSnapshot

SOURCE_VERSION = "etoro-pnl-v1"


class AccountEquityEvidenceError(ValueError):
    """An official account snapshot cannot be trusted or persisted."""


@dataclass(frozen=True)
class AccountEquityEvidence:
    status: Literal["unavailable", "collecting"]
    days_collected: int
    snapshot_date: date | None
    observed_at: datetime | None
    currency: str | None
    official_equity: Decimal | None
    official_available_cash: Decimal | None
    official_total_invested: Decimal | None
    official_unrealised_pnl: Decimal | None
    local_eod_currency: str | None
    local_eod_value: Decimal | None
    difference: Decimal | None
    comparable: Literal[False]
    incomplete_reasons: tuple[str, ...]


def _validate_snapshot(snapshot: BrokerAccountRiskSnapshot) -> None:
    values = (
        snapshot.available_cash,
        snapshot.total_invested,
        snapshot.unrealized_pnl,
        snapshot.equity,
    )
    if snapshot.observed_at.tzinfo is None:
        raise AccountEquityEvidenceError("observed_at must be timezone-aware")
    if not all(value.is_finite() for value in values):
        raise AccountEquityEvidenceError("account equity values must be finite")
    if snapshot.available_cash < 0 or snapshot.total_invested < 0 or snapshot.equity <= 0:
        raise AccountEquityEvidenceError("account equity values are outside safe bounds")
    if abs(snapshot.equity - snapshot.available_cash - snapshot.total_invested - snapshot.unrealized_pnl) > Decimal(
        "0.000001"
    ):
        raise AccountEquityEvidenceError("account equity components do not reconcile")


def record_account_equity_snapshot(
    conn: psycopg.Connection[Any],
    *,
    environment: Literal["demo", "real"],
    snapshot: BrokerAccountRiskSnapshot,
) -> bool:
    """Store at most the newest official observation for one UTC day."""
    _validate_snapshot(snapshot)
    observed_at = snapshot.observed_at.astimezone(UTC)
    row = conn.execute(
        """
        INSERT INTO broker_account_equity_snapshots (
            environment,snapshot_date,observed_at,source_version,currency,
            available_cash,total_invested,unrealised_pnl,equity
        ) VALUES (%s,%s,%s,%s,'USD',%s,%s,%s,%s)
        ON CONFLICT (environment,snapshot_date) DO UPDATE SET
            observed_at=EXCLUDED.observed_at,
            source_version=EXCLUDED.source_version,
            available_cash=EXCLUDED.available_cash,
            total_invested=EXCLUDED.total_invested,
            unrealised_pnl=EXCLUDED.unrealised_pnl,
            equity=EXCLUDED.equity,
            recorded_at=now()
        WHERE EXCLUDED.observed_at > broker_account_equity_snapshots.observed_at
          AND broker_account_equity_snapshots.snapshot_date=(now() AT TIME ZONE 'UTC')::date
        RETURNING snapshot_date
        """,
        (
            environment,
            observed_at.date(),
            observed_at,
            SOURCE_VERSION,
            snapshot.available_cash,
            snapshot.total_invested,
            snapshot.unrealized_pnl,
            snapshot.equity,
        ),
    ).fetchone()
    return row is not None


def load_account_equity_evidence(
    conn: psycopg.Connection[Any], *, environment: Literal["demo", "real"]
) -> AccountEquityEvidence:
    """Return the latest official/local comparison with explicit caveats."""
    row = conn.execute(
        """
        WITH latest AS (
            SELECT *,count(*) OVER () AS days_collected
            FROM broker_account_equity_snapshots
            WHERE environment=%s
            ORDER BY snapshot_date DESC
            LIMIT 1
        )
        SELECT latest.days_collected,latest.snapshot_date,latest.observed_at,latest.currency,
               latest.available_cash,latest.total_invested,latest.unrealised_pnl,latest.equity,
               local.display_currency,local.total_value,
               coalesce(local.positions_no_price,0) > 0
                 OR coalesce(local.positions_no_fx,0) > 0
                 OR coalesce(local.cash_no_fx_currencies,0) > 0 AS local_valuation_incomplete
        FROM latest
        LEFT JOIN portfolio_eod_snapshots local ON local.snapshot_date=latest.snapshot_date
        """,
        (environment,),
    ).fetchone()
    if row is None:
        return AccountEquityEvidence(
            status="unavailable",
            days_collected=0,
            snapshot_date=None,
            observed_at=None,
            currency=None,
            official_equity=None,
            official_available_cash=None,
            official_total_invested=None,
            official_unrealised_pnl=None,
            local_eod_currency=None,
            local_eod_value=None,
            difference=None,
            comparable=False,
            incomplete_reasons=("official_account_equity_missing",),
        )

    observed_at = row[2]
    local_value = Decimal(str(row[9])) if row[9] is not None else None
    official_equity = Decimal(str(row[7]))
    reasons: list[str] = []
    if local_value is None:
        reasons.append("same_day_local_eod_snapshot_missing")
    else:
        if row[8] != row[3]:
            reasons.append("local_eod_currency_mismatch")
        if bool(row[10]):
            reasons.append("local_eod_valuation_incomplete")
        # computed_at is when the local job ran, not when its closing prices
        # were effective. Do not call these totals reconciled until the local
        # valuation carries a defensible effective market timestamp.
        reasons.append("local_eod_effective_time_unknown")
    return AccountEquityEvidence(
        status="collecting",
        days_collected=int(row[0]),
        snapshot_date=row[1],
        observed_at=observed_at,
        currency=str(row[3]),
        official_equity=official_equity,
        official_available_cash=Decimal(str(row[4])),
        official_total_invested=Decimal(str(row[5])),
        official_unrealised_pnl=Decimal(str(row[6])),
        local_eod_currency=None if row[8] is None else str(row[8]),
        local_eod_value=local_value,
        difference=official_equity - local_value if local_value is not None and row[8] == row[3] else None,
        comparable=False,
        incomplete_reasons=tuple(reasons),
    )
