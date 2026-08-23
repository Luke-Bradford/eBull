from __future__ import annotations

import ast
import inspect
from datetime import date
from pathlib import Path
from typing import Any, cast

import pytest

from app.services.research_point_in_time import (
    FIELD_REGISTRY,
    IDENTITY_FAMILIES,
    PROBE_MATRIX,
    REGISTRY_VERSION,
    PointInTimeUnavailableError,
    R6RankingIdentity,
    R6RankingRequest,
    RankingFamily,
    execute_r6_ranking,
    source_date_is_public,
)

EXPECTED_FAMILIES = {
    "research_prices",
    "fundamental_facts",
    "derived_fundamentals",
    "dimensional_xbrl",
    "ownership_observations",
    "filing_red_flags",
    "finra_short_interest",
    "live_etoro_state",
    "historical_population",
}
EXPECTED_PROBES = {
    "F0",
    "F1",
    "F2",
    "D1",
    "X1",
    "O1",
    "O2",
    "O3",
    "R0",
    "R1",
    "N0",
    "N1",
    "N2",
    "L1",
    "H1",
    "H2",
    "H3",
    "P1",
    "P2",
    "P3",
    "P4",
    "P5",
}


def test_registry_is_closed_complete_and_all_historical_families_refuse() -> None:
    assert {family.value for family in RankingFamily} == EXPECTED_FAMILIES
    assert {family.value for family in FIELD_REGISTRY} == EXPECTED_FAMILIES
    assert {probe for cells in PROBE_MATRIX.values() for cell in cells.values() for probe in cell.probes} == (
        EXPECTED_PROBES
    )
    assert all(verdict.status == "refused" for verdict in FIELD_REGISTRY.values())
    assert REGISTRY_VERSION.startswith("r6-pit-registry-v1+")
    assert len(REGISTRY_VERSION.rsplit("+", 1)[1]) == 12


def test_every_identity_owns_a_nonempty_immutable_family_set() -> None:
    assert set(IDENTITY_FAMILIES) == set(R6RankingIdentity)
    for identity, families in IDENTITY_FAMILIES.items():
        assert families, identity
        assert isinstance(families, frozenset)
        assert families <= set(RankingFamily)


@pytest.mark.parametrize("identity", list(R6RankingIdentity))
def test_every_current_r6_ranking_identity_refuses(identity: R6RankingIdentity) -> None:
    with pytest.raises(PointInTimeUnavailableError, match="no admissible historical read path") as exc:
        execute_r6_ranking(R6RankingRequest(identity=identity, decision_session=date(2020, 1, 15)))
    assert identity.value in str(exc.value)


@pytest.mark.parametrize(
    ("decision", "reason"),
    [
        (date(2020, 1, 18), "closed NYSE date"),
        (date(2020, 1, 20), "closed NYSE date"),
    ],
)
def test_closed_decision_dates_refuse_before_field_evaluation(decision: date, reason: str) -> None:
    request = R6RankingRequest(identity=R6RankingIdentity.QUALITY, decision_session=decision)
    with pytest.raises(PointInTimeUnavailableError, match=reason):
        execute_r6_ranking(request)


def test_price_identity_after_frozen_capture_refuses_specific_boundary() -> None:
    request = R6RankingRequest(identity=R6RankingIdentity.MOMENTUM, decision_session=date(2024, 9, 30))
    with pytest.raises(PointInTimeUnavailableError, match="after research-price capture"):
        execute_r6_ranking(request)


def test_empty_unknown_and_underdeclared_requests_are_not_representable() -> None:
    with pytest.raises(PointInTimeUnavailableError, match="non-empty typed"):
        execute_r6_ranking(None)
    with pytest.raises(ValueError, match="unknown R6 ranking identity"):
        R6RankingRequest(identity="not-an-arm", decision_session=date(2020, 1, 15))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        cast(Any, R6RankingRequest)(
            identity=R6RankingIdentity.QUALITY,
            decision_session=date(2020, 1, 15),
            families=frozenset(),
        )


def test_date_resolution_public_clock_excludes_same_date() -> None:
    decision = date(2020, 1, 15)
    assert source_date_is_public(date(2020, 1, 14), decision_session=decision)
    assert not source_date_is_public(decision, decision_session=decision)
    assert not source_date_is_public(date(2020, 1, 16), decision_session=decision)


def test_query_boundary_exposes_no_connection_callback_or_direct_sql() -> None:
    assert tuple(inspect.signature(execute_r6_ranking).parameters) == ("request",)
    module_source = Path("app/services/research_point_in_time.py").read_text()
    tree = ast.parse(module_source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    assert not any(isinstance(call.func, ast.Attribute) and call.func.attr == "execute" for call in calls)

    governed = {
        "financial_facts_raw",
        "financial_periods",
        "ownership_institutions_observations",
        "finra_short_interest_observations",
        "research_price_daily",
        "instruments",
    }
    for path in Path("app/services").glob("r6_*.py"):
        literals = {
            node.value
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        assert not any(table in literal for table in governed for literal in literals), path
