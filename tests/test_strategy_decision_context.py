from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.instrument_market_classification import reconcile_instrument_market_classification
from app.services.strategy_decision_context import (
    CONTEXT_VERSION,
    DecisionInputs,
    DecisionVix,
    MarketClassification,
    build_decision_context,
    dollar_volume_band_for,
    load_decision_vix,
    load_market_classification,
    price_band_for,
    store_decision_context,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401


def _complete_inputs(**overrides: object) -> DecisionInputs:
    values: dict[str, object] = {
        "as_traded_price": Decimal("49.99"),
        "trailing_mean_share_volume": Decimal("1200000"),
        "trailing_median_share_volume": Decimal("1000000"),
        "trailing_mean_dollar_volume": Decimal("30000000"),
        "trailing_median_dollar_volume": Decimal("24999999"),
        "zero_volume_frequency": Decimal("0"),
        "intraday_coverage": Decimal("1"),
        "relative_volume": Decimal("1.8"),
        "spread_bps": Decimal("7.5"),
        "realised_volatility": Decimal("0.32"),
        "gap_pct": Decimal("-2.1"),
        "market_sector_residual_z": Decimal("-2.7"),
        "vix": DecisionVix(
            Decimal("19.2"),
            date(2026, 8, 7),
            "cboe-vix-daily-close-v1",
        ),
        "as_traded_price_basis": "observed_unadjusted",
    }
    values.update(overrides)
    return DecisionInputs(**values)  # type: ignore[arg-type]


def test_boundaries_are_explicit_and_stable() -> None:
    assert price_band_for(Decimal("4.99")) == "under_5"
    assert price_band_for(Decimal("5")) == "5_to_20"
    assert price_band_for(Decimal("20")) == "20_to_50"
    assert price_band_for(Decimal("50")) == "50_to_150"
    assert price_band_for(Decimal("150")) == "150_plus"
    assert dollar_volume_band_for(Decimal("9999999")) == "1m_to_10m"
    assert dollar_volume_band_for(Decimal("10000000")) == "10m_to_25m"
    assert CONTEXT_VERSION.startswith("decision-context-v3:")


def test_complete_context_is_eligible() -> None:
    context = build_decision_context(
        strategy_id="candidate-1",
        strategy_version="sha256:abc",
        instrument_id=1,
        decision_at=datetime(2026, 8, 10, 14, 35, tzinfo=UTC),
        signal_id=None,
        classification=MarketClassification(
            effective_from=date(2026, 8, 10),
            security_type="common_stock",
            primary_listing_market="nasdaq",
            provider_exchange_id="4",
            instrument_type_id=5,
            provider_industry_id=8,
        ),
        inputs=_complete_inputs(),
    )
    assert context.candidate_verdict == "eligible"
    assert context.refusal_reason is None
    assert context.price_band == "20_to_50"
    assert context.as_traded_price_basis == "observed_unadjusted"
    assert context.dollar_volume_band == "10m_to_25m"
    assert context.volume_lookback_sessions == 20
    assert context.vix == Decimal("19.2")
    assert context.vix_bar_date == date(2026, 8, 7)
    assert context.vix_source_version == "cboe-vix-daily-close-v1"


def test_missing_or_unknown_point_in_time_data_refuses_by_name() -> None:
    context = build_decision_context(
        strategy_id="candidate-1",
        strategy_version="sha256:abc",
        instrument_id=1,
        decision_at=datetime(2026, 8, 10, 14, 35, tzinfo=UTC),
        signal_id=None,
        classification=MarketClassification(
            effective_from=date(2026, 8, 10),
            security_type="common_stock",
            primary_listing_market="unknown",
            provider_exchange_id=None,
            instrument_type_id=5,
            provider_industry_id=8,
        ),
        inputs=_complete_inputs(vix=None, spread_bps=None),
    )
    assert context.candidate_verdict == "refused"
    assert context.refusal_reason == "missing:primary_listing_market,spread_bps,vix"


def test_missing_point_in_time_sector_refuses_instead_of_using_current_metadata() -> None:
    context = build_decision_context(
        strategy_id="candidate-1",
        strategy_version="sha256:abc",
        instrument_id=1,
        decision_at=datetime(2026, 8, 10, 14, 35, tzinfo=UTC),
        signal_id=None,
        classification=MarketClassification(
            effective_from=date(2026, 8, 10),
            security_type="common_stock",
            primary_listing_market="nasdaq",
            provider_exchange_id="4",
            instrument_type_id=5,
            provider_industry_id=None,
        ),
        inputs=_complete_inputs(),
    )
    assert context.candidate_verdict == "refused"
    assert context.refusal_reason == "missing:provider_industry_id"


def test_stale_vix_refuses_by_provenance_reason() -> None:
    context = build_decision_context(
        strategy_id="candidate-1",
        strategy_version="sha256:abc",
        instrument_id=1,
        decision_at=datetime(2026, 8, 11, 14, 35, tzinfo=UTC),
        signal_id=None,
        classification=MarketClassification(
            effective_from=date(2026, 8, 10),
            security_type="common_stock",
            primary_listing_market="nasdaq",
            provider_exchange_id="4",
            instrument_type_id=5,
            provider_industry_id=8,
        ),
        inputs=_complete_inputs(vix=DecisionVix(None, None, None, "stale_source:2026-08-07<expected:2026-08-10")),
    )
    assert context.candidate_verdict == "refused"
    assert context.refusal_reason == "missing:vix_stale_source:2026-08-07<expected:2026-08-10"
    assert context.vix is None and context.vix_bar_date is None


def test_partial_vix_provenance_is_invalid() -> None:
    with pytest.raises(ValueError, match="either complete"):
        DecisionVix(Decimal("19"), None, "cboe-vix-daily-close-v1")


def test_adjusted_or_unproven_price_basis_refuses_cohort_attribution() -> None:
    context = build_decision_context(
        strategy_id="candidate-1",
        strategy_version="sha256:abc",
        instrument_id=1,
        decision_at=datetime(2026, 8, 10, 14, 35, tzinfo=UTC),
        signal_id=None,
        classification=MarketClassification(
            effective_from=date(2026, 8, 10),
            security_type="common_stock",
            primary_listing_market="nasdaq",
            provider_exchange_id="4",
            instrument_type_id=5,
            provider_industry_id=8,
        ),
        inputs=_complete_inputs(as_traded_price_basis="unknown"),
    )
    assert context.candidate_verdict == "refused"
    assert context.refusal_reason == "missing:as_traded_price_basis"


def test_unrecognised_price_basis_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown as_traded_price_basis"):
        build_decision_context(
            strategy_id="candidate-1",
            strategy_version="sha256:abc",
            instrument_id=1,
            decision_at=datetime(2026, 8, 10, 14, 35, tzinfo=UTC),
            signal_id=None,
            classification=None,
            inputs=_complete_inputs(as_traded_price_basis="split_adjusted"),
        )


def test_volume_coverage_outside_fraction_range_is_rejected() -> None:
    with pytest.raises(ValueError, match="intraday_coverage must be inside 0-1"):
        build_decision_context(
            strategy_id="candidate-1",
            strategy_version="sha256:abc",
            instrument_id=1,
            decision_at=datetime(2026, 8, 10, 14, 35, tzinfo=UTC),
            signal_id=None,
            classification=None,
            inputs=_complete_inputs(intraday_coverage=Decimal("1.01")),
        )


def _seed_instrument(conn: psycopg.Connection[tuple[Any, ...]], iid: int) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, sector,
            is_tradable, instrument_type_id, first_seen_at, last_seen_at
        ) VALUES (%s, %s, 'Context Test', '4', 'USD', '8', TRUE, 5, NOW(), NOW())
        """,
        (iid, f"CTX{iid}"),
    )


def _seed_vix(conn: psycopg.Connection[tuple[Any, ...]], *bars: tuple[date, Decimal]) -> None:
    row = conn.execute(
        """
        INSERT INTO research_price_series (
            vendor, vendor_symbol, upstream_source, licence, adjustment_basis,
            first_bar, last_bar, bar_count
        ) VALUES ('cboe', 'VIX', 'other', 'fixture', 'unadjusted', %s, %s, %s)
        RETURNING series_id
        """,
        (bars[0][0], bars[-1][0], len(bars)),
    ).fetchone()
    assert row is not None
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO research_price_daily (series_id, bar_date, open, high, low, close)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [(row[0], when, close, close, close, close) for when, close in bars],
        )


@pytest.mark.integration
def test_vix_loader_requires_exact_prior_nyse_session_and_ignores_same_day(
    ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_vix(
        conn,
        (date(2026, 8, 7), Decimal("17")),
        (date(2026, 8, 10), Decimal("18")),
    )

    # Monday resolves Friday, both before and after Monday's close. The
    # same-date Monday source row cannot leak into either decision.
    before_close = load_decision_vix(conn, decision_at=datetime(2026, 8, 10, 15, tzinfo=UTC))
    after_close = load_decision_vix(conn, decision_at=datetime(2026, 8, 10, 22, tzinfo=UTC))
    assert before_close == after_close == DecisionVix(Decimal("17"), date(2026, 8, 7), "cboe-vix-daily-close-v1")

    tuesday = load_decision_vix(conn, decision_at=datetime(2026, 8, 11, 15, tzinfo=UTC))
    assert tuesday == DecisionVix(Decimal("18"), date(2026, 8, 10), "cboe-vix-daily-close-v1")


@pytest.mark.integration
def test_vix_loader_returns_typed_stale_refusal(
    ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
) -> None:
    _seed_vix(ebull_test_conn, (date(2026, 8, 7), Decimal("17")))
    result = load_decision_vix(
        ebull_test_conn,
        decision_at=datetime(2026, 8, 11, 15, tzinfo=UTC),
    )
    assert result == DecisionVix(None, None, None, "stale_source:2026-08-07<expected:2026-08-10")


@pytest.mark.integration
def test_reconcile_records_prospective_classification_and_same_day_correction(
    ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    iid = 2_508_001
    _seed_instrument(conn, iid)
    stats = reconcile_instrument_market_classification(conn)
    assert stats.opened == 1

    row = conn.execute(
        """
        SELECT primary_listing_market, security_type, provider_industry_id,
               source_event, effective_from,
               (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
        FROM instrument_market_classification_history
        WHERE instrument_id = %s
        """,
        (iid,),
    ).fetchone()
    assert row is not None
    assert row[:4] == ("nasdaq", "common_stock", 8, "imported")
    assert row[4] == row[5]

    conn.execute(
        "UPDATE instruments SET exchange = '5', instrument_type_id = 6, sector = '5' WHERE instrument_id = %s",
        (iid,),
    )
    stats = reconcile_instrument_market_classification(conn)
    assert stats.corrected_same_day == 1
    rows = conn.execute(
        """
        SELECT primary_listing_market, security_type, provider_industry_id
        FROM instrument_market_classification_history
        WHERE instrument_id = %s
        """,
        (iid,),
    ).fetchall()
    assert rows == [("nyse", "etf", 5)]


@pytest.mark.integration
def test_current_metadata_cannot_leak_into_historical_decision(
    ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    iid = 2_508_002
    _seed_instrument(conn, iid)
    reconcile_instrument_market_classification(conn)

    historical = datetime.now(UTC) - timedelta(days=1)
    assert load_market_classification(conn, instrument_id=iid, decision_at=historical) is None
    assert load_market_classification(conn, instrument_id=iid, decision_at=datetime.now(UTC)) is not None


@pytest.mark.integration
def test_later_classification_change_closes_prior_row_without_overlap(
    ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    iid = 2_508_004
    _seed_instrument(conn, iid)
    conn.execute("UPDATE instruments SET exchange = '5', instrument_type_id = 6 WHERE instrument_id = %s", (iid,))
    conn.execute(
        """
        INSERT INTO instrument_market_classification_history (
            instrument_id, effective_from, effective_to, last_confirmed_on,
            provider_exchange_id, primary_listing_market, instrument_type_id,
            security_type, source_event
        ) VALUES (
            %s,
            (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - 1,
            NULL,
            (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date - 1,
            '4', 'nasdaq', 5, 'common_stock', 'imported'
        )
        """,
        (iid,),
    )

    stats = reconcile_instrument_market_classification(conn)
    assert stats.changed == 1
    assert stats.opened == 1
    rows = conn.execute(
        """
        SELECT effective_from, effective_to, primary_listing_market, security_type
        FROM instrument_market_classification_history
        WHERE instrument_id = %s
        ORDER BY effective_from
        """,
        (iid,),
    ).fetchall()
    current_date_row = conn.execute("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date").fetchone()
    assert current_date_row is not None
    current_date = current_date_row[0]
    assert rows == [
        (current_date - timedelta(days=1), current_date - timedelta(days=1), "nasdaq", "common_stock"),
        (current_date, None, "nyse", "etf"),
    ]


@pytest.mark.integration
def test_context_round_trip_and_database_completeness_guard(
    ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    iid = 2_508_003
    _seed_instrument(conn, iid)
    reconcile_instrument_market_classification(conn)
    decision_at = datetime.now(UTC)
    classification = load_market_classification(conn, instrument_id=iid, decision_at=decision_at)
    assert classification is not None
    context = build_decision_context(
        strategy_id="candidate-db",
        strategy_version="sha256:def",
        instrument_id=iid,
        decision_at=decision_at,
        signal_id=None,
        classification=classification,
        inputs=_complete_inputs(),
    )
    context_id = store_decision_context(conn, context)
    assert context_id > 0
    stored_basis = conn.execute(
        """
        SELECT as_traded_price_basis, vix, vix_bar_date, vix_source_version
        FROM strategy_decision_contexts WHERE context_id = %s
        """,
        (context_id,),
    ).fetchone()
    assert stored_basis == (
        "observed_unadjusted",
        Decimal("19.2"),
        date(2026, 8, 7),
        "cboe-vix-daily-close-v1",
    )

    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                "UPDATE strategy_decision_contexts SET provider_industry_id = NULL WHERE context_id = %s",
                (context_id,),
            )

    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                "UPDATE strategy_decision_contexts SET vix_bar_date = NULL WHERE context_id = %s",
                (context_id,),
            )

    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                """
                UPDATE strategy_decision_contexts
                   SET candidate_verdict = 'refused', refusal_reason = 'fixture',
                       vix_bar_date = NULL
                 WHERE context_id = %s
                """,
                (context_id,),
            )

    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO strategy_decision_contexts (
                    strategy_id, strategy_version, instrument_id, decision_at,
                    candidate_verdict, context_version
                ) VALUES ('bad', 'v1', %s, NOW() + interval '1 second', 'eligible', 'v1')
                """,
                (iid,),
            )
