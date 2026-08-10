"""Outcome-blind point-in-time mechanism classifier tests (#2507)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from app.services.strategy_shock_mechanism import (
    CLASSIFIER_VERSION,
    EventSourceCoverage,
    FactorContext,
    LiquidityContext,
    SecCatalyst,
    classify_shock_mechanism,
    definition_json,
    load_material_sec_catalysts,
    sec_knowledge_at,
)

_DECISION = datetime(2026, 8, 10, 15, 0, tzinfo=UTC)
_WINDOW = _DECISION - timedelta(hours=24)


def _factor(residual_z: float) -> FactorContext:
    volatility = 0.01
    residual = residual_z * volatility
    expected = -0.03
    return FactorContext(
        expected + residual,
        expected,
        residual,
        volatility,
        _DECISION - timedelta(days=1),
        "prior-ols-v1",
        "SPY/frozen-v1",
        "XLK/frozen-v1",
    )


def _coverage(*, complete: bool = True) -> tuple[EventSourceCoverage, ...]:
    status = "complete" if complete else "known_incomplete"
    return tuple(
        EventSourceCoverage(source, status, _WINDOW - timedelta(days=1), _DECISION)
        for source in ("sec_filings", "issuer_releases", "market_news")
    )


def _liquidity(**overrides: object) -> LiquidityContext:
    values: dict[str, object] = {
        "as_traded_price": Decimal("40"),
        "trailing_median_dollar_volume": Decimal("25000000"),
        "relative_volume": Decimal("2.1"),
        "realised_volatility": Decimal("0.025"),
        "spread_bps": Decimal("8"),
        "confirmation_completed_at": _DECISION - timedelta(minutes=5),
        "halt_feed_at": _DECISION - timedelta(seconds=30),
        "active_halt": False,
    }
    values.update(overrides)
    return LiquidityContext(**values)  # type: ignore[arg-type]


def test_definition_is_versioned_and_contains_no_outcome_or_direction() -> None:
    assert CLASSIFIER_VERSION.startswith("shock-mechanism-v1+")
    payload = definition_json()
    assert '"direction_semantics":"none-provenance-only"' in payload
    assert '"max_halt_feed_age_seconds":300' in payload
    assert "profit" not in payload and "outcome_return" not in payload


def test_known_material_event_has_precedence_without_inferring_direction() -> None:
    catalyst = SecCatalyst("a-1", "8-K", _DECISION - timedelta(minutes=10), ("2.02",))
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(catalyst,),
        event_coverage=_coverage(complete=False),
        factor_context=_factor(0.2),
        liquidity_context=None,
    )
    assert result.mechanism == "known_fundamental_catalyst"
    assert result.catalyst_accessions == ("a-1",)
    assert result.reason_code == "material_sec_event_known_before_decision"


def test_future_event_is_never_backfilled_into_the_decision() -> None:
    future = SecCatalyst("future", "10-Q", _DECISION + timedelta(seconds=1))
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(future,),
        event_coverage=_coverage(),
        factor_context=_factor(0.5),
        liquidity_context=_liquidity(),
    )
    assert result.mechanism == "known_market_or_sector_move"
    assert result.catalyst_accessions == ()


def test_prior_fitted_factor_explanation_is_not_a_liquidity_reversal() -> None:
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(),
        event_coverage=_coverage(complete=False),
        factor_context=_factor(1.99),
        liquidity_context=_liquidity(),
    )
    assert result.mechanism == "known_market_or_sector_move"


def test_no_known_catalyst_refuses_incomplete_free_news_coverage() -> None:
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(),
        event_coverage=_coverage(complete=False),
        factor_context=_factor(-2.5),
        liquidity_context=_liquidity(),
    )
    assert result.mechanism == "unknown"
    assert result.reason_code == "event_coverage_incomplete"
    assert result.missing_inputs == ("sec_filings", "issuer_releases", "market_news")


def test_duplicate_event_coverage_is_a_typed_unknown_not_an_exception() -> None:
    coverage = _coverage()
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(),
        event_coverage=(*coverage, coverage[0]),
        factor_context=_factor(-3),
        liquidity_context=_liquidity(),
    )
    assert result.mechanism == "unknown"
    assert result.reason_code == "event_coverage_incomplete"
    assert result.missing_inputs == ("duplicate_event_coverage:sec_filings",)


def test_complete_residual_context_routes_to_research_candidate_not_trade() -> None:
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(),
        event_coverage=_coverage(),
        factor_context=_factor(-2.0),
        liquidity_context=_liquidity(),
    )
    assert result.mechanism == "no_known_catalyst_liquidity_candidate"
    assert result.reason_code == "residual_shock_with_complete_no_known_catalyst_context"


def test_missing_spread_confirmation_or_fresh_halt_state_is_explicit() -> None:
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(),
        event_coverage=_coverage(),
        factor_context=_factor(-3),
        liquidity_context=_liquidity(
            spread_bps=None,
            confirmation_completed_at=_DECISION + timedelta(seconds=1),
            halt_feed_at=_DECISION - timedelta(minutes=6),
        ),
    )
    assert result.mechanism == "unknown"
    assert result.missing_inputs == ("spread_bps", "causal_intraday_confirmation", "fresh_halt_feed")


def test_active_halt_is_a_refusal_even_with_complete_inputs() -> None:
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(),
        event_coverage=_coverage(),
        factor_context=_factor(3),
        liquidity_context=_liquidity(active_halt=True),
    )
    assert result.mechanism == "unknown"
    assert result.reason_code == "active_market_halt"


def test_inconsistent_factor_inputs_are_refused_at_construction() -> None:
    with pytest.raises(ValueError, match="raw minus expected"):
        FactorContext(
            -0.1,
            -0.02,
            -0.01,
            0.01,
            _DECISION - timedelta(days=1),
            "v1",
            "SPY",
            "XLK",
        )


def test_retrospectively_fitted_factor_context_cannot_explain_the_move() -> None:
    future_fit = _factor(0.5)
    future_fit = FactorContext(
        future_fit.raw_return,
        future_fit.expected_market_sector_return,
        future_fit.residual_return,
        future_fit.prior_residual_volatility,
        _DECISION,
        future_fit.model_version,
        future_fit.market_series_id,
        future_fit.sector_series_id,
    )
    result = classify_shock_mechanism(
        decision_at=_DECISION,
        event_window_start=_WINDOW,
        catalysts=(),
        event_coverage=_coverage(),
        factor_context=future_fit,
        liquidity_context=_liquidity(),
    )
    assert result.mechanism == "unknown"
    assert result.reason_code == "factor_context_not_causal"


def test_date_only_sec_filing_becomes_known_at_following_regular_session() -> None:
    # Friday 2026-07-03 is the observed Independence Day closure; the
    # conservative next regular open is Monday 2026-07-06 09:30 ET.
    assert sec_knowledge_at(
        filed_at=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
        accepted_at=None,
    ) == datetime(2026, 7, 6, 13, 30, tzinfo=UTC)


def test_exact_sec_acceptance_is_preserved() -> None:
    accepted = datetime(2026, 8, 10, 14, 2, 3, tzinfo=UTC)
    assert sec_knowledge_at(filed_at=_DECISION, accepted_at=accepted) == accepted


@pytest.mark.integration
def test_material_sec_loader_uses_acceptance_items_and_conservative_fallback(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    instrument_id = 2_507_001
    conn.execute(
        """
        INSERT INTO instruments (instrument_id,symbol,company_name,exchange,currency,is_tradable)
        VALUES (%s,'MECH','Mechanism fixture','NASDAQ','USD',true)
        """,
        (instrument_id,),
    )
    conn.execute(
        """
        INSERT INTO sec_filing_manifest (
          accession_number,cik,form,source,subject_type,subject_id,instrument_id,filed_at,accepted_at
        ) VALUES
          ('mech-10q','0000000001','10-Q','sec_10q','issuer',%s,%s,
           '2026-08-10T13:00:00Z','2026-08-10T14:00:00Z'),
          ('mech-8k-material','0000000001','8-K','sec_8k','issuer',%s,%s,
           '2026-08-10T13:10:00Z','2026-08-10T14:10:00Z'),
          ('mech-8k-unparsed','0000000001','8-K','sec_8k','issuer',%s,%s,
           '2026-08-10T13:20:00Z','2026-08-10T14:20:00Z'),
          ('mech-date-only','0000000001','10-Q','sec_10q','issuer',%s,%s,
           '2026-08-07T00:00:00Z',NULL)
        """,
        tuple(str(instrument_id) for _ in range(4) for _part in range(2)),
    )
    conn.execute(
        """
        INSERT INTO eight_k_filings (
          accession_number,instrument_id,document_type,is_amendment,is_tombstone
        ) VALUES ('mech-8k-material',%s,'8-K',false,false)
        """,
        (instrument_id,),
    )
    conn.execute(
        """
        INSERT INTO eight_k_items (accession_number,item_code,item_label,severity,item_order,body)
        VALUES ('mech-8k-material','2.02','Results','material',1,'fixture')
        """
    )

    catalysts = load_material_sec_catalysts(
        conn,
        instrument_id=instrument_id,
        event_window_start=datetime(2026, 8, 10, 13, 30, tzinfo=UTC),
        decision_at=datetime(2026, 8, 10, 15, 0, tzinfo=UTC),
    )

    assert [(row.accession_number, row.item_codes) for row in catalysts] == [
        ("mech-date-only", ()),
        ("mech-10q", ()),
        ("mech-8k-material", ("2.02",)),
    ]
    assert catalysts[0].knowledge_at == datetime(2026, 8, 10, 13, 30, tzinfo=UTC)
