"""#2747 — ambiguity-rule identity and owner binding against real Postgres."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.result_ledger import store_in_sample_result, stored_result_promotion_refusals
from app.services.strategy_result_ambiguity import (
    AMBIGUITY_RULE_VERSION,
    LEGACY_AMBIGUITY_RULE_VERSION,
    AmbiguityRecord,
    store_result_ambiguity,
)
from tests.test_result_ledger import build_result

pytestmark = pytest.mark.integration


def _current_result_id(conn: psycopg.Connection[Any], *, strategy_id: str) -> int:
    return store_in_sample_result(
        conn,
        build_result(
            strategy_id=strategy_id,
            namespace="in_sample",
            ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
        ),
    )


def test_the_current_identity_round_trips_through_the_result_reader(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    result_id = _current_result_id(ebull_test_conn, strategy_id="S-AMBIGUITY-V2-ROUNDTRIP")
    # This path reconstructs ResultIdentity and verifies the stored hash before
    # it returns any gate refusal. A missing positional ledger field raises.
    assert "quarantine_arms_not_compared" in stored_result_promotion_refusals(ebull_test_conn, result_id)


def test_an_ambiguity_record_must_match_its_owning_results_rule(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    result_id = _current_result_id(ebull_test_conn, strategy_id="S-AMBIGUITY-V2-OWNER")
    legacy = AmbiguityRecord(
        ambiguity_rule_version=LEGACY_AMBIGUITY_RULE_VERSION,
        comparison_basis="shared_measurement",
    )
    with pytest.raises(psycopg.errors.ForeignKeyViolation), ebull_test_conn.transaction():
        store_result_ambiguity(ebull_test_conn, result_id=result_id, record=legacy)


def test_a_matching_ambiguity_record_is_accepted(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    result_id = _current_result_id(ebull_test_conn, strategy_id="S-AMBIGUITY-V2-MATCH")
    current = AmbiguityRecord(
        ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
        comparison_basis="shared_measurement",
    )
    store_result_ambiguity(ebull_test_conn, result_id=result_id, record=current)
