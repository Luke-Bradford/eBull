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
    MarketClassification,
    build_decision_context,
    dollar_volume_band_for,
    load_market_classification,
    price_band_for,
    store_decision_context,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401


def _complete_inputs(**overrides: Decimal | None) -> DecisionInputs:
    values: dict[str, Decimal | None] = {
        "as_traded_price": Decimal("49.99"),
        "trailing_median_share_volume": Decimal("1000000"),
        "trailing_median_dollar_volume": Decimal("24999999"),
        "relative_volume": Decimal("1.8"),
        "spread_bps": Decimal("7.5"),
        "realised_volatility": Decimal("0.32"),
        "gap_pct": Decimal("-2.1"),
        "market_sector_residual_z": Decimal("-2.7"),
        "vix": Decimal("19.2"),
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
    assert CONTEXT_VERSION.startswith("decision-context-v1:")


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
        ),
        inputs=_complete_inputs(),
    )
    assert context.candidate_verdict == "eligible"
    assert context.refusal_reason is None
    assert context.price_band == "20_to_50"
    assert context.dollar_volume_band == "10m_to_25m"


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
        ),
        inputs=_complete_inputs(vix=None, spread_bps=None),
    )
    assert context.candidate_verdict == "refused"
    assert context.refusal_reason == "missing:primary_listing_market,spread_bps,vix"


pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple[Any, ...]], iid: int) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency,
            is_tradable, instrument_type_id, first_seen_at, last_seen_at
        ) VALUES (%s, %s, 'Context Test', '4', 'USD', TRUE, 5, NOW(), NOW())
        """,
        (iid, f"CTX{iid}"),
    )


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
        SELECT primary_listing_market, security_type, source_event
        FROM instrument_market_classification_history
        WHERE instrument_id = %s
        """,
        (iid,),
    ).fetchone()
    assert row == ("nasdaq", "common_stock", "imported")

    conn.execute("UPDATE instruments SET exchange = '5', instrument_type_id = 6 WHERE instrument_id = %s", (iid,))
    stats = reconcile_instrument_market_classification(conn)
    assert stats.corrected_same_day == 1
    rows = conn.execute(
        """
        SELECT primary_listing_market, security_type
        FROM instrument_market_classification_history
        WHERE instrument_id = %s
        """,
        (iid,),
    ).fetchall()
    assert rows == [("nyse", "etf")]


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
            %s, CURRENT_DATE - 1, NULL, CURRENT_DATE - 1,
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
    current_date_row = conn.execute("SELECT CURRENT_DATE").fetchone()
    assert current_date_row is not None
    current_date = current_date_row[0]
    assert rows == [
        (current_date - timedelta(days=1), current_date - timedelta(days=1), "nasdaq", "common_stock"),
        (current_date, None, "nyse", "etf"),
    ]


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
