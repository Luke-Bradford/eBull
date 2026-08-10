from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.market_data import MarketDataProvider, Quote
from app.services.strategy_quote_observation import (
    capture_active_universe_quotes,
    retire_quote_observations,
    sample_bucket,
)


def _quote(instrument_id: int, stamp: str, *, bid: str = "99", ask: str = "101") -> Quote:
    return Quote(
        instrument_id=instrument_id,
        timestamp=datetime.fromisoformat(stamp),
        bid=Decimal(bid),
        ask=Decimal(ask),
        last=Decimal("100"),
    )


def _activate_quote_universe(
    conn: psycopg.Connection[tuple], *, include_missing_resolution: bool = False
) -> tuple[int, int]:
    first_id, second_id = 2_485_001, 2_485_002
    conn.execute(
        "UPDATE strategy_intraday_universe_versions SET status = 'retired', retired_at = now() WHERE status = 'active'"
    )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO instruments (
                instrument_id, symbol, company_name, exchange, currency, is_tradable
            ) VALUES (%s, %s, %s, 'test', 'USD', true)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            [(first_id, "QFIRST", "Quote First"), (second_id, "QSECOND", "Quote Second")],
        )
    conn.execute(
        """
        INSERT INTO strategy_intraday_universe_versions (
            universe_version, provider, session_rule, status, rationale
        ) VALUES ('TEST-QUOTE-V1', 'etoro', 'nyse_rth', 'draft', 'quote fixture')
        """
    )
    members = [
        (1, "5m", "QFIRST"),
        (2, "30m", "QFIRST"),
        (3, "5m", "QSECOND"),
    ]
    if include_missing_resolution:
        members.append((4, "5m", "QMISSING"))
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO strategy_intraday_universe_members (
                universe_version, ordinal, timeframe, symbol, purpose
            ) VALUES ('TEST-QUOTE-V1', %s, %s, %s, 'fixture')
            """,
            members,
        )
    conn.execute(
        """
        UPDATE strategy_intraday_universe_versions
        SET status = 'active', activated_at = now()
        WHERE universe_version = 'TEST-QUOTE-V1'
        """
    )
    return first_id, second_id


def test_sample_bucket_is_utc_five_minute_and_requires_timezone() -> None:
    assert sample_bucket(datetime.fromisoformat("2026-08-10T14:37:59.123456+01:00")) == datetime.fromisoformat(
        "2026-08-10T13:35:00+00:00"
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        sample_bucket(datetime(2026, 8, 10, 13, 35))


def test_production_capture_timestamp_is_taken_after_provider_returns(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    first_id, second_id = _activate_quote_universe(ebull_test_conn)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_quotes.return_value = [
        _quote(first_id, "2026-08-10T13:35:09+00:00"),
        _quote(second_id, "2026-08-10T13:35:09+00:00"),
    ]

    class _Clock:
        @staticmethod
        def now(*, tz: object) -> datetime:
            assert tz is UTC
            return datetime.fromisoformat("2026-08-10T13:35:10+00:00")

    monkeypatch.setattr("app.services.strategy_quote_observation.datetime", _Clock)

    capture_active_universe_quotes(ebull_test_conn, provider)

    assert ebull_test_conn.execute(
        "SELECT min(observed_at - quote_at), max(observed_at - quote_at) FROM strategy_quote_observations"
    ).fetchone() == (timedelta(seconds=1), timedelta(seconds=1))


def test_capture_is_unique_per_symbol_not_per_timeframe_and_records_missing(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    first_id, second_id = _activate_quote_universe(ebull_test_conn)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_quotes.return_value = [_quote(first_id, "2026-08-10T13:36:01+00:00")]

    report = capture_active_universe_quotes(
        ebull_test_conn,
        provider,
        observed_at=datetime.fromisoformat("2026-08-10T13:36:05+00:00"),
    )

    assert report.expected == 2
    assert report.observed == 1
    assert report.missing == 1
    assert report.invalid == 0
    assert report.rows_written == 2
    assert report.failures == ()
    provider.get_quotes.assert_called_once_with([first_id, second_id])
    assert ebull_test_conn.execute(
        """
        SELECT instrument_id, observation_status, refusal_reason, spread_bps, source
        FROM strategy_quote_observations ORDER BY instrument_id
        """
    ).fetchall() == [
        (first_id, "observed", None, Decimal("200.00000000"), "etoro/TEST-QUOTE-V1/best_bid_ask"),
        (second_id, "missing", "provider_omitted_quote", None, "etoro/TEST-QUOTE-V1/best_bid_ask"),
    ]


def test_first_observation_in_bucket_is_immutable_and_rerun_does_not_grow(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    first_id, second_id = _activate_quote_universe(ebull_test_conn)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_quotes.return_value = [
        _quote(first_id, "2026-08-10T13:35:10+00:00"),
        _quote(second_id, "2026-08-10T13:35:10+00:00"),
    ]
    capture_active_universe_quotes(
        ebull_test_conn,
        provider,
        observed_at=datetime.fromisoformat("2026-08-10T13:35:12+00:00"),
    )
    provider.get_quotes.return_value = [
        _quote(first_id, "2026-08-10T13:39:00+00:00", bid="100", ask="102"),
        _quote(second_id, "2026-08-10T13:39:00+00:00"),
    ]

    second = capture_active_universe_quotes(
        ebull_test_conn,
        provider,
        observed_at=datetime.fromisoformat("2026-08-10T13:39:05+00:00"),
    )

    assert second.rows_written == 0
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_quote_observations").fetchone() == (2,)
    assert ebull_test_conn.execute(
        "SELECT bid, observed_at FROM strategy_quote_observations WHERE instrument_id = %s", (first_id,)
    ).fetchone() == (Decimal("99.000000"), datetime.fromisoformat("2026-08-10T13:35:12+00:00"))
    with pytest.raises(psycopg.errors.RaiseException, match="quote observations are immutable"):
        with ebull_test_conn.transaction():
            ebull_test_conn.execute(
                "UPDATE strategy_quote_observations SET bid = 98 WHERE instrument_id = %s", (first_id,)
            )


def test_invalid_quote_and_unresolved_member_are_explicit(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    first_id, second_id = _activate_quote_universe(ebull_test_conn, include_missing_resolution=True)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_quotes.return_value = [
        _quote(first_id, "2026-08-10T13:35:01+00:00"),
        _quote(second_id, "2026-08-10T13:35:01+00:00", bid="101", ask="100"),
    ]

    report = capture_active_universe_quotes(
        ebull_test_conn,
        provider,
        observed_at=datetime.fromisoformat("2026-08-10T13:35:02+00:00"),
    )

    assert report.expected == 3
    assert report.observed == 1
    assert report.invalid == 1
    assert report.failures[0].symbol == "QMISSING"
    assert report.failures[0].reason == "universe_resolution: expected one tradable instrument, found 0"
    assert ebull_test_conn.execute(
        "SELECT refusal_reason FROM strategy_quote_observations WHERE instrument_id = %s", (second_id,)
    ).fetchone() == ("crossed_market",)


def test_provider_duplicate_or_out_of_scope_quote_refuses_batch(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    first_id, _ = _activate_quote_universe(ebull_test_conn)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_quotes.return_value = [
        _quote(first_id, "2026-08-10T13:35:01+00:00"),
        _quote(first_id, "2026-08-10T13:35:02+00:00"),
    ]
    with pytest.raises(RuntimeError, match="duplicate quotes"):
        capture_active_universe_quotes(
            ebull_test_conn,
            provider,
            observed_at=datetime.fromisoformat("2026-08-10T13:35:03+00:00"),
        )
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_quote_observations").fetchone() == (0,)


def test_quote_retention_is_exactly_twenty_four_calendar_months(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    first_id, second_id = _activate_quote_universe(ebull_test_conn)
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_quotes.return_value = [
        _quote(first_id, "2024-08-09T13:35:00+00:00"),
        _quote(second_id, "2024-08-09T13:35:00+00:00"),
    ]
    capture_active_universe_quotes(
        ebull_test_conn,
        provider,
        observed_at=datetime.fromisoformat("2024-08-09T13:35:01+00:00"),
    )
    provider.get_quotes.return_value = [
        _quote(first_id, "2024-08-10T13:35:00+00:00"),
        _quote(second_id, "2024-08-10T13:35:00+00:00"),
    ]
    capture_active_universe_quotes(
        ebull_test_conn,
        provider,
        observed_at=datetime.fromisoformat("2024-08-10T13:35:01+00:00"),
    )
    as_of = datetime.fromisoformat("2026-08-10T13:35:00+00:00")

    assert retire_quote_observations(ebull_test_conn, as_of=as_of) == 2
    assert retire_quote_observations(ebull_test_conn, as_of=as_of, dry_run=False) == 2
    assert ebull_test_conn.execute(
        "SELECT sample_bucket FROM strategy_quote_observations ORDER BY sample_bucket LIMIT 1"
    ).fetchone() == (datetime.fromisoformat("2024-08-10T13:35:00+00:00"),)
