from decimal import Decimal

import pytest

from app.services.orb_stocks_in_play_candidate import (
    CANDIDATE_VERSION,
    DEFINITION,
    ReplicationReadiness,
    assess_replication_readiness,
)


def _ready(**overrides: object) -> ReplicationReadiness:
    values: dict[str, object] = {
        "point_in_time_membership": True,
        "security_type_mapping_verified": True,
        "atr_implementation_verified": True,
        "expected_prefilter_names": 1_200,
        "scanned_prefilter_names": 1_200,
        "opening_volume_names": 1_200,
        "expected_selected_names": 20,
        "selected_names": 20,
        "selected_paths": 20,
        "as_traded_price_basis_complete": True,
        "decision_quotes_complete": True,
        "shortability_complete": True,
    }
    values.update(overrides)
    return ReplicationReadiness(**values)  # type: ignore[arg-type]


def test_definition_is_the_published_cross_section_not_a_generic_breakout() -> None:
    assert DEFINITION.paper_revision == "2025-04-29"
    assert DEFINITION.security_type == "common_stock"
    assert "pending_paper_crsp_share_code_confirmation" in DEFINITION.security_type_boundary
    assert DEFINITION.listing_markets == ("nyse", "nasdaq")
    assert DEFINITION.minimum_open_price_usd == Decimal("5")
    assert DEFINITION.minimum_prior_mean_share_volume == Decimal("1000000")
    assert DEFINITION.minimum_prior_atr_usd == Decimal("0.50")
    assert DEFINITION.minimum_opening_relative_volume == Decimal("1")
    assert DEFINITION.maximum_daily_rank == 20
    assert DEFINITION.stop_atr_fraction == Decimal("0.10")
    assert DEFINITION.profit_target is None
    assert DEFINITION.gap_filter is None
    assert DEFINITION.ebull_maximum_leverage == Decimal("1")
    assert CANDIDATE_VERSION.startswith("orb-stocks-in-play-v1:")


def test_eight_name_panel_cannot_inherit_a_full_cross_section_result() -> None:
    verdict, reasons = assess_replication_readiness(
        _ready(scanned_prefilter_names=8, opening_volume_names=8, selected_names=8, selected_paths=8)
    )
    assert verdict == "refused"
    assert reasons == (
        "incomplete_prefilter_cross_section",
        "incomplete_opening_volume_cross_section",
        "incomplete_top20_selection",
    )


def test_missing_cost_shortability_and_rule_provenance_refuse() -> None:
    verdict, reasons = assess_replication_readiness(
        _ready(
            point_in_time_membership=False,
            security_type_mapping_verified=False,
            atr_implementation_verified=False,
            as_traded_price_basis_complete=False,
            decision_quotes_complete=False,
            shortability_complete=False,
        )
    )
    assert verdict == "refused"
    assert reasons == (
        "missing_point_in_time_membership",
        "ambiguous_published_security_type_universe",
        "ambiguous_atr_implementation",
        "missing_as_traded_price_basis",
        "missing_decision_time_etoro_quotes",
        "missing_decision_time_shortability",
    )


def test_only_complete_predeclared_inputs_are_ready() -> None:
    assert assess_replication_readiness(_ready()) == ("ready", ())


def test_quiet_complete_session_can_select_fewer_than_twenty() -> None:
    assert assess_replication_readiness(_ready(expected_selected_names=3, selected_names=3, selected_paths=3)) == (
        "ready",
        (),
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("expected_prefilter_names", 0),
        ("expected_selected_names", 21),
        ("selected_names", 21),
        ("selected_paths", -1),
    ),
)
def test_invalid_census_is_rejected(field: str, value: int) -> None:
    with pytest.raises(ValueError, match=field):
        assess_replication_readiness(_ready(**{field: value}))


def test_selected_population_cannot_exceed_prefilter_population() -> None:
    with pytest.raises(ValueError, match="expected_selected_names"):
        assess_replication_readiness(_ready(expected_prefilter_names=3))
