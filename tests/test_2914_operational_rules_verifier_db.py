from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest
from psycopg.rows import dict_row

from scripts.verify_2914_operational_rules import _CENSUS_SQL, _build_evidence
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def test_reference_census_conserves_every_accepted_observation(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    snapshot_row = ebull_test_conn.execute(
        """
        INSERT INTO reference_data_snapshots (
            source, dataset_key, source_url, response_sha256, payload,
            parser_version, parse_status, parsed_at, row_count, missing_count,
            first_observation, last_observation
        ) VALUES (
            'kenneth_french', 'r6_2914_fixture', 'https://example.test/reference',
            %s, %s, 'fixture-v1', 'accepted', now(), 2, 0,
            '2020-01-31', '2020-02-29'
        )
        RETURNING snapshot_id
        """,
        ("c" * 64, b"fixture"),
    ).fetchone()
    assert snapshot_row is not None
    snapshot_id = snapshot_row[0]
    with ebull_test_conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO reference_data_observations
                (snapshot_id, series_key, observation_date, value, unit)
            VALUES (%s, 'Mom', %s, %s, 'decimal_return')
            """,
            ((snapshot_id, "2020-01-31", "0.01"), (snapshot_id, "2020-02-29", "0.02")),
        )
    with ebull_test_conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(_CENSUS_SQL)
        rows = list(cursor.fetchall())

    evidence = _build_evidence(
        rows,
        measured_at=datetime(2026, 8, 23, tzinfo=UTC),
        execution_commit="d" * 40,
    )
    assert evidence.accepted_snapshot_count == 1
    assert evidence.snapshot_series_count == 1
    assert evidence.observation_count == 2
    assert evidence.eligible_valuation_spread_series == 0
