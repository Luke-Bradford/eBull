"""#3609 step 2 slice 5b: the factor snapshots read in one repeatable-read transaction against their pins.

Fixture snapshots in the test database; the dev database's snapshots 39 and 40 are not read.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import date

import psycopg
import pytest

import scripts.report_3609_step2_inputs as inputs
from scripts.report_3609_step2 import ReportError
from tests.fixtures.ebull_test_db import ebull_test_conn, test_database_url  # noqa: F401

pytestmark = pytest.mark.integration

FIVE, MOMENTUM = "french_five_factor_monthly", "french_momentum_monthly"
FIVE_SERIES = ("Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF")


def _snapshot(
    conn: psycopg.Connection[tuple], dataset: str, series: tuple[str, ...], payload: bytes, unit: str = "decimal_return"
) -> int:
    row = conn.execute(
        """
        INSERT INTO reference_data_snapshots (
            source, dataset_key, source_url, response_sha256, payload,
            parser_version, parse_status, parsed_at, row_count, missing_count,
            first_observation, last_observation
        ) VALUES (
            'kenneth_french', %s, 'https://example.test/reference', %s, %s,
            'fixture-v1', 'accepted', now(), %s, 0, '2020-01-31', '2020-02-29'
        )
        RETURNING snapshot_id
        """,
        (dataset, hashlib.sha256(payload).hexdigest(), payload, 2 * len(series)),
    ).fetchone()
    assert row is not None
    for i, key in enumerate(series):
        for day, value in ((date(2020, 1, 31), f"0.01{i}0"), (date(2020, 2, 29), f"-0.002{i}")):
            conn.execute(
                "INSERT INTO reference_data_observations (snapshot_id, series_key, observation_date, value, unit)"
                " VALUES (%s, %s, %s, %s, %s)",
                (row[0], key, day, value, unit),
            )
    return int(row[0])


@pytest.fixture
def snapshots(ebull_test_conn: psycopg.Connection[tuple]) -> Iterator[dict[str, int]]:  # noqa: F811
    ids = {
        FIVE: _snapshot(ebull_test_conn, FIVE, FIVE_SERIES, b"five"),
        MOMENTUM: _snapshot(ebull_test_conn, MOMENTUM, ("Mom",), b"momentum"),
    }
    ebull_test_conn.commit()
    yield ids


def _fresh() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url())


def test_digests_then_factors_from_the_pinned_snapshots(snapshots: dict[str, int]) -> None:
    with _fresh() as conn:
        pins = inputs.snapshot_digests(conn, snapshots)
    assert pins[FIVE].response_sha256 == hashlib.sha256(b"five").hexdigest()
    with _fresh() as conn:
        seen: list[tuple[object, object]] = []
        real = inputs._read_snapshot

        def spy(c: psycopg.Connection[tuple], dataset: str, snapshot_id: int) -> object:
            seen.append(
                (c.execute("SHOW transaction_isolation").fetchone(), c.execute("SHOW transaction_read_only").fetchone())
            )
            return real(c, dataset, snapshot_id)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(inputs, "_read_snapshot", spy)
            factors = inputs.read_factors(conn, snapshots, pins)
        # Held inside the read, restored after it.
        assert seen == [(("repeatable read",), ("on",))] * 2
        assert conn.isolation_level is None and conn.read_only is None
    assert sorted(factors) == sorted((*FIVE_SERIES, "Mom"))
    assert factors["SMB"] == {(2020, 1): 0.011, (2020, 2): -0.0021}
    assert factors["Mom"] == {(2020, 1): 0.01, (2020, 2): -0.002}


def test_a_changed_observation_refuses(ebull_test_conn: psycopg.Connection[tuple], snapshots: dict[str, int]) -> None:  # noqa: F811
    with _fresh() as conn:
        pins = inputs.snapshot_digests(conn, snapshots)
    ebull_test_conn.execute(
        "UPDATE reference_data_observations SET value = 0.5 WHERE snapshot_id = %s AND series_key = 'Mom'"
        " AND observation_date = '2020-01-31'",
        (snapshots[MOMENTUM],),
    )
    ebull_test_conn.commit()
    with _fresh() as conn, pytest.raises(ReportError, match="differs from its pins"):
        inputs.read_factors(conn, snapshots, pins)


def test_a_payload_that_is_not_its_response_hash_refuses(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    snapshots: dict[str, int],
) -> None:
    ebull_test_conn.execute(
        "UPDATE reference_data_snapshots SET response_sha256 = %s WHERE snapshot_id = %s",
        ("e" * 64, snapshots[FIVE]),
    )
    ebull_test_conn.commit()
    with _fresh() as conn, pytest.raises(ReportError, match="payload sha256"):
        inputs.snapshot_digests(conn, snapshots)


def test_another_dataset_or_a_wrong_unit_refuses(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    five = _snapshot(ebull_test_conn, FIVE, FIVE_SERIES, b"five")
    percent = _snapshot(ebull_test_conn, MOMENTUM, ("Mom",), b"momentum", unit="percent_per_annum")
    ebull_test_conn.commit()
    with _fresh() as conn, pytest.raises(ReportError, match="not accepted"):
        inputs.snapshot_digests(conn, {FIVE: percent, MOMENTUM: five})
    ids = {FIVE: five, MOMENTUM: percent}
    with _fresh() as conn:
        pins = inputs.snapshot_digests(conn, ids)
    with _fresh() as conn, pytest.raises(ReportError, match="unit"):
        inputs.read_factors(conn, ids, pins)


def test_a_non_finite_value_refuses(ebull_test_conn: psycopg.Connection[tuple], snapshots: dict[str, int]) -> None:  # noqa: F811
    ebull_test_conn.execute(
        "UPDATE reference_data_observations SET value = 'NaN' WHERE snapshot_id = %s AND series_key = 'RF'"
        " AND observation_date = '2020-01-31'",
        (snapshots[FIVE],),
    )
    ebull_test_conn.commit()
    with _fresh() as conn:
        pins = inputs.snapshot_digests(conn, snapshots)
    with _fresh() as conn, pytest.raises(ReportError, match="non-finite"):
        inputs.read_factors(conn, snapshots, pins)


def test_a_missing_dataset_refuses_before_any_read(snapshots: dict[str, int]) -> None:
    with _fresh() as conn, pytest.raises(ReportError, match="expected"):
        inputs.snapshot_digests(conn, {FIVE: snapshots[FIVE]})
    with _fresh() as conn:
        pins = inputs.snapshot_digests(conn, snapshots)
    with _fresh() as conn, pytest.raises(ReportError, match="factor pins"):
        inputs.read_factors(conn, snapshots, {FIVE: pins[FIVE]})


def test_a_connection_inside_a_transaction_refuses(snapshots: dict[str, int]) -> None:
    with _fresh() as conn:
        conn.execute("SELECT 1")
        with pytest.raises(ReportError, match="no open transaction"):
            inputs.snapshot_digests(conn, snapshots)


def test_a_value_that_overflows_its_float_refuses(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    snapshots: dict[str, int],
) -> None:
    ebull_test_conn.execute(
        "UPDATE reference_data_observations SET value = 1e400 WHERE snapshot_id = %s AND series_key = 'Mom'"
        " AND observation_date = '2020-01-31'",
        (snapshots[MOMENTUM],),
    )
    ebull_test_conn.commit()
    with _fresh() as conn:
        pins = inputs.snapshot_digests(conn, snapshots)  # pinned after the change: only the float check can refuse
    with _fresh() as conn, pytest.raises(ReportError, match="not finite as a float"):
        inputs.read_factors(conn, snapshots, pins)
