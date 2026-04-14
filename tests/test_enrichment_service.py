"""Unit tests for app.services.enrichment."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from app.providers.enrichment import AnalystEstimates, EarningsEvent, InstrumentProfileData
from app.services.enrichment import (
    EnrichmentRefreshSummary,
    _upsert_analyst_estimates,
    _upsert_earnings_events,
    _upsert_profile,
    refresh_enrichment,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)

_PROFILE = InstrumentProfileData(
    symbol="AAPL",
    beta=Decimal("1.20"),
    public_float=15_000_000_000,
    avg_volume_30d=80_000_000,
    market_cap=Decimal("3000000000000"),
    employees=160_000,
    ipo_date=date(1980, 12, 12),
    is_actively_trading=True,
)

_EARNINGS_EVENT = EarningsEvent(
    symbol="AAPL",
    fiscal_date_ending=date(2026, 3, 31),
    reporting_date=date(2026, 4, 30),
    eps_estimate=Decimal("1.50"),
    eps_actual=Decimal("1.62"),
    revenue_estimate=Decimal("93_000_000_000"),
    revenue_actual=Decimal("95_000_000_000"),
    surprise_pct=Decimal("0.08"),
)

_ANALYST_ESTIMATES = AnalystEstimates(
    symbol="AAPL",
    as_of_date=date(2026, 4, 14),
    consensus_eps_fq=Decimal("1.55"),
    consensus_eps_fy=Decimal("6.80"),
    consensus_rev_fq=Decimal("94_000_000_000"),
    consensus_rev_fy=Decimal("400_000_000_000"),
    analyst_count=42,
    buy_count=30,
    hold_count=10,
    sell_count=2,
    price_target_mean=Decimal("220.00"),
    price_target_high=Decimal("260.00"),
    price_target_low=Decimal("180.00"),
)


# ---------------------------------------------------------------------------
# _upsert_profile
# ---------------------------------------------------------------------------


def test_upsert_profile_executes_correct_sql() -> None:
    conn = MagicMock()
    _upsert_profile(conn, "42", _PROFILE, _NOW)

    conn.execute.assert_called_once()
    sql, params = conn.execute.call_args[0]
    assert "INSERT INTO instrument_profile" in sql
    assert "ON CONFLICT (instrument_id) DO UPDATE" in sql
    assert "IS DISTINCT FROM" in sql
    assert params["instrument_id"] == "42"
    assert params["beta"] == _PROFILE.beta
    assert params["fetched_at"] == _NOW


def test_upsert_profile_passes_all_fields() -> None:
    conn = MagicMock()
    _upsert_profile(conn, "7", _PROFILE, _NOW)

    _, params = conn.execute.call_args[0]
    assert params["public_float"] == _PROFILE.public_float
    assert params["avg_volume_30d"] == _PROFILE.avg_volume_30d
    assert params["market_cap"] == _PROFILE.market_cap
    assert params["employees"] == _PROFILE.employees
    assert params["ipo_date"] == _PROFILE.ipo_date
    assert params["is_actively_trading"] == _PROFILE.is_actively_trading


# ---------------------------------------------------------------------------
# _upsert_earnings_events
# ---------------------------------------------------------------------------


def test_upsert_earnings_executes_per_event() -> None:
    conn = MagicMock()
    events = [_EARNINGS_EVENT, _EARNINGS_EVENT]
    _upsert_earnings_events(conn, "42", events)

    assert conn.execute.call_count == 2


def test_upsert_earnings_single_event_sql() -> None:
    conn = MagicMock()
    _upsert_earnings_events(conn, "42", [_EARNINGS_EVENT])

    conn.execute.assert_called_once()
    sql, params = conn.execute.call_args[0]
    assert "INSERT INTO earnings_events" in sql
    assert "ON CONFLICT (instrument_id, fiscal_date_ending)" in sql
    assert "IS DISTINCT FROM" in sql
    assert params["instrument_id"] == "42"
    assert params["fiscal_date_ending"] == _EARNINGS_EVENT.fiscal_date_ending
    assert params["eps_actual"] == _EARNINGS_EVENT.eps_actual


def test_upsert_earnings_empty_list_executes_nothing() -> None:
    conn = MagicMock()
    _upsert_earnings_events(conn, "42", [])
    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# _upsert_analyst_estimates
# ---------------------------------------------------------------------------


def test_upsert_analyst_estimates_executes_insert() -> None:
    conn = MagicMock()
    _upsert_analyst_estimates(conn, "42", _ANALYST_ESTIMATES)

    conn.execute.assert_called_once()
    sql, params = conn.execute.call_args[0]
    assert "INSERT INTO analyst_estimates" in sql
    assert "ON CONFLICT (instrument_id, as_of_date)" in sql
    assert params["instrument_id"] == "42"
    assert params["as_of_date"] == _ANALYST_ESTIMATES.as_of_date
    assert params["analyst_count"] == _ANALYST_ESTIMATES.analyst_count
    assert params["price_target_mean"] == _ANALYST_ESTIMATES.price_target_mean


# ---------------------------------------------------------------------------
# refresh_enrichment — integration-level tests with mocked provider
# ---------------------------------------------------------------------------


def _make_provider(
    profile: InstrumentProfileData | None = _PROFILE,
    events: list[EarningsEvent] | None = None,
    estimates: AnalystEstimates | None = _ANALYST_ESTIMATES,
) -> MagicMock:
    provider = MagicMock()
    provider.get_profile_enrichment.return_value = profile
    provider.get_earnings_calendar.return_value = events if events is not None else [_EARNINGS_EVENT]
    provider.get_analyst_estimates.return_value = estimates
    return provider


def test_refresh_enrichment_counts_correctly() -> None:
    """Two symbols, all provider calls succeed — verify summary counts."""
    provider = _make_provider(events=[_EARNINGS_EVENT, _EARNINGS_EVENT])
    conn = MagicMock()
    symbols = [("AAPL", "1"), ("MSFT", "2")]

    summary = refresh_enrichment(provider, conn, symbols)

    assert isinstance(summary, EnrichmentRefreshSummary)
    assert summary.symbols_attempted == 2
    assert summary.profiles_upserted == 2
    assert summary.earnings_upserted == 4  # 2 events × 2 symbols
    assert summary.estimates_upserted == 2
    assert summary.symbols_skipped == 0


def test_refresh_enrichment_skips_on_provider_failure() -> None:
    """Provider raises on the first symbol; second symbol still processed."""
    provider = MagicMock()
    provider.get_profile_enrichment.side_effect = [
        RuntimeError("provider down"),
        _PROFILE,
    ]
    provider.get_earnings_calendar.return_value = [_EARNINGS_EVENT]
    provider.get_analyst_estimates.return_value = _ANALYST_ESTIMATES
    conn = MagicMock()
    symbols = [("FAIL", "99"), ("AAPL", "1")]

    summary = refresh_enrichment(provider, conn, symbols)

    assert summary.symbols_attempted == 2
    assert summary.symbols_skipped == 1
    assert summary.profiles_upserted == 1
    assert summary.earnings_upserted == 1
    assert summary.estimates_upserted == 1


def test_refresh_enrichment_handles_none_profile() -> None:
    """Provider returns None for profile; earnings and estimates still attempted."""
    provider = _make_provider(profile=None, events=[_EARNINGS_EVENT])
    conn = MagicMock()
    symbols = [("AAPL", "1")]

    summary = refresh_enrichment(provider, conn, symbols)

    assert summary.profiles_upserted == 0
    assert summary.earnings_upserted == 1
    assert summary.estimates_upserted == 1
    assert summary.symbols_skipped == 0


def test_refresh_enrichment_handles_empty_earnings() -> None:
    """Provider returns empty list for earnings; no earnings_events rows written."""
    provider = _make_provider(events=[])
    conn = MagicMock()
    symbols = [("AAPL", "1")]

    summary = refresh_enrichment(provider, conn, symbols)

    assert summary.earnings_upserted == 0
    assert summary.profiles_upserted == 1
    assert summary.estimates_upserted == 1
    assert summary.symbols_skipped == 0


def test_refresh_enrichment_handles_none_estimates() -> None:
    """Provider returns None for estimates; estimates count is 0."""
    provider = _make_provider(estimates=None)
    conn = MagicMock()
    symbols = [("AAPL", "1")]

    summary = refresh_enrichment(provider, conn, symbols)

    assert summary.estimates_upserted == 0
    assert summary.profiles_upserted == 1
    assert summary.earnings_upserted == 1
    assert summary.symbols_skipped == 0
