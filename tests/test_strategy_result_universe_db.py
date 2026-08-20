"""#2621 — the frozen-universe store against a real Postgres (DB tier)."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.result_ledger import store_in_sample_result
from app.services.strategies.validated_universe import VALIDATED_UNIVERSE_RULE_VERSION
from app.services.strategy_result_universe import (
    load_result_universe,
    record_sha256,
    store_result_universe,
)
from tests.test_result_ledger import build_result
from tests.test_strategy_result_universe import build_universe_record

pytestmark = pytest.mark.integration


def _stored_result_id(conn: psycopg.Connection[Any]) -> int:
    return store_in_sample_result(conn, build_result(namespace="in_sample", evaluated_instrument_count=3))


def test_round_trip(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    result_id = _stored_result_id(conn)
    store_result_universe(conn, result_id=result_id, record=build_universe_record())
    assert load_result_universe(conn, result_id) == build_universe_record()


def test_a_result_without_a_record_loads_none(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    result_id = _stored_result_id(conn)
    assert load_result_universe(conn, result_id) is None


def test_the_record_is_immutable(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    result_id = _stored_result_id(conn)
    store_result_universe(conn, result_id=result_id, record=build_universe_record())
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        with conn.transaction():
            conn.execute(
                "UPDATE strategy_result_universe SET universe_rule_version = 'edited' WHERE result_id = %s",
                (result_id,),
            )
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        with conn.transaction():
            conn.execute("DELETE FROM strategy_result_universe WHERE result_id = %s", (result_id,))


def test_a_tampered_hash_raises_rather_than_refusing(ebull_test_conn: psycopg.Connection[Any]) -> None:
    # Corruption is an integrity failure to surface loudly, not a gate
    # verdict — same contract as load_promotion_evidence.
    conn = ebull_test_conn
    result_id = _stored_result_id(conn)
    conn.execute(
        """
        INSERT INTO strategy_result_universe (
            result_id, universe_rule_version, evaluated_instrument_ids,
            validated_universe_ids, payload_sha256
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (result_id, VALIDATED_UNIVERSE_RULE_VERSION, [1, 2, 3], [1, 2, 3, 4, 5], "0" * 64),
    )
    with pytest.raises(RuntimeError, match="hash mismatch"):
        load_result_universe(conn, result_id)


def test_a_non_canonical_array_raises(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    result_id = _stored_result_id(conn)
    conn.execute(
        """
        INSERT INTO strategy_result_universe (
            result_id, universe_rule_version, evaluated_instrument_ids,
            validated_universe_ids, payload_sha256
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (
            result_id,
            VALIDATED_UNIVERSE_RULE_VERSION,
            [3, 1, 1],
            [1, 2, 3, 4, 5],
            record_sha256(build_universe_record()),
        ),
    )
    with pytest.raises(RuntimeError, match="not sorted-unique"):
        load_result_universe(conn, result_id)
