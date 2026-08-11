"""Prospective broker account-equity evidence remains compact and fail-closed."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services.account_equity_evidence import (
    AccountEquityEvidenceError,
    load_account_equity_evidence,
    record_account_equity_snapshot,
)


def _snapshot(
    *, observed_at: datetime, cash: str = "500", invested: str = "400", pnl: str = "100"
) -> BrokerAccountRiskSnapshot:
    available_cash = Decimal(cash)
    total_invested = Decimal(invested)
    unrealized_pnl = Decimal(pnl)
    return BrokerAccountRiskSnapshot(
        available_cash=available_cash,
        total_invested=total_invested,
        unrealized_pnl=unrealized_pnl,
        equity=available_cash + total_invested + unrealized_pnl,
        instrument_investments=(),
        observed_at=observed_at,
        raw_payload={"not": "persisted"},
    )


def test_empty_account_equity_evidence_is_explicit(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.status == "unavailable"
    assert evidence.days_collected == 0
    assert evidence.incomplete_reasons == ("official_account_equity_missing",)


def test_newest_same_day_observation_wins_without_appending(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    first = _snapshot(observed_at=now - timedelta(minutes=2))
    latest = _snapshot(observed_at=now, cash="525")
    assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=first)
    assert not record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=replace(first, observed_at=now - timedelta(minutes=3)),
    )
    assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=latest)

    row = ebull_test_conn.execute(
        "SELECT count(*),max(equity) FROM broker_account_equity_snapshots WHERE environment='demo'"
    ).fetchone()
    assert row == (1, Decimal("1025.000000"))


def test_historical_observation_is_immutable(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    yesterday = datetime.now(UTC).replace(microsecond=0) - timedelta(days=1)
    first = _snapshot(observed_at=yesterday)
    assert record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=first)
    assert not record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=yesterday + timedelta(minutes=2), cash="600"),
    )


@pytest.mark.parametrize(
    "snapshot",
    [
        _snapshot(observed_at=datetime.now(UTC), cash="NaN"),
        _snapshot(observed_at=datetime.now(UTC), cash="-1"),
        replace(_snapshot(observed_at=datetime.now(UTC)), equity=Decimal("999")),
        _snapshot(observed_at=datetime.now()),
    ],
)
def test_invalid_official_values_fail_closed(
    ebull_test_conn: psycopg.Connection[tuple], snapshot: BrokerAccountRiskSnapshot
) -> None:
    with pytest.raises(AccountEquityEvidenceError):
        record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)


def test_local_total_remains_diagnostic_until_effective_time_is_known(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    observed = datetime.now(UTC).replace(microsecond=0)
    snapshot = _snapshot(observed_at=observed)
    record_account_equity_snapshot(ebull_test_conn, environment="demo", snapshot=snapshot)
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,computed_at
        ) VALUES (%s,'USD',995,495,500,1,1,%s)
        """,
        (observed.date(), observed + timedelta(minutes=1)),
    )

    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.status == "collecting"
    assert evidence.days_collected == 1
    assert evidence.official_equity == Decimal("1000.000000")
    assert evidence.local_eod_value == Decimal("995.0000")
    assert evidence.local_eod_currency == "USD"
    assert evidence.difference == Decimal("5.000000")
    assert evidence.incomplete_reasons == ("local_eod_effective_time_unknown",)


def test_incomplete_local_valuation_exposes_reasons_not_false_comparison(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    observed = datetime.now(UTC).replace(microsecond=0)
    record_account_equity_snapshot(
        ebull_test_conn,
        environment="demo",
        snapshot=_snapshot(observed_at=observed),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
          snapshot_date,display_currency,total_value,positions_value,cash_value,
          positions_total,positions_priced,positions_no_price,computed_at
        ) VALUES (%s,'GBP',900,400,500,1,0,1,%s)
        """,
        (observed.date(), observed - timedelta(hours=1)),
    )
    evidence = load_account_equity_evidence(ebull_test_conn, environment="demo")
    assert evidence.status == "collecting"
    assert not evidence.comparable
    assert evidence.difference is None
    assert set(evidence.incomplete_reasons) == {
        "local_eod_currency_mismatch",
        "local_eod_valuation_incomplete",
        "local_eod_effective_time_unknown",
    }
