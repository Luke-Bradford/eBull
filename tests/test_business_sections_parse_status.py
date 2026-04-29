"""Tests for `get_parse_status` (#648).

Pins the four empty-state classifications the API serializes into
`BusinessSectionsParseStatus` so the frontend can render distinct
empty-state copy. Runs against `ebull_test`.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest

from app.services.business_summary import get_parse_status
from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test Postgres not reachable",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[object]]:
    c: psycopg.Connection[object] = psycopg.connect(_test_database_url(), autocommit=True)
    try:
        yield c
    finally:
        c.close()


def _seed_instrument(conn: psycopg.Connection[object]) -> int:
    """Insert a unique-symbol instrument row and return its id.

    `instruments.instrument_id` has no DEFAULT in the schema (it's
    populated by the universe sync via provider id mapping). Tests
    pick a high random value with low collision probability.
    """
    sym = f"TEST_{uuid4().hex[:8]}"
    # Random in a high range so collisions with real or other test
    # rows are vanishingly unlikely. UUID4 hex prefix → int in
    # [0, 2^32]; offset by 10^12 to stay well clear of anything the
    # universe sync would assign.
    iid = 10**12 + int(uuid4().hex[:8], 16)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments
                (instrument_id, symbol, company_name, currency,
                 is_tradable, is_primary_listing)
            VALUES (%s, %s, %s, 'USD', TRUE, TRUE)
            RETURNING instrument_id
            """,
            (iid, sym, f"Test Instrument {sym}"),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])  # type: ignore[index]


def _delete_summary(conn: psycopg.Connection[object], instrument_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM instrument_business_summary WHERE instrument_id = %s",
            (instrument_id,),
        )
        # Also delete the instrument row so the next test_case doesn't
        # accumulate seeded rows (FK CASCADE handles dependents).
        cur.execute(
            "DELETE FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )


class TestGetParseStatus:
    def test_not_attempted_when_no_parent_row(self, conn: psycopg.Connection[object]) -> None:
        instrument_id = _seed_instrument(conn)
        try:
            ps = get_parse_status(conn, instrument_id=instrument_id)
            assert ps is not None
            assert ps.state == "not_attempted"
            assert ps.failure_reason is None
            assert ps.next_retry_at is None
            assert ps.last_attempted_at is None
        finally:
            _delete_summary(conn, instrument_id)

    def test_no_item_1_when_failure_reason_is_marker(self, conn: psycopg.Connection[object]) -> None:
        instrument_id = _seed_instrument(conn)
        # last_parsed_at populated explicitly so the assertion below
        # doesn't depend on the schema-level NOT NULL DEFAULT NOW().
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instrument_business_summary
                    (instrument_id, body, source_accession,
                     attempt_count, last_failure_reason, next_retry_at,
                     last_parsed_at)
                VALUES (%s, '', 'acc-1', 1, 'no_item_1_marker',
                        now() + interval '1 day', now())
                """,
                (instrument_id,),
            )
        try:
            ps = get_parse_status(conn, instrument_id=instrument_id)
            assert ps is not None
            assert ps.state == "no_item_1"
            assert ps.failure_reason == "no_item_1_marker"
            assert ps.next_retry_at is not None
            assert ps.last_attempted_at is not None
        finally:
            _delete_summary(conn, instrument_id)

    def test_no_item_1_when_failure_reason_is_body_too_short(self, conn: psycopg.Connection[object]) -> None:
        # body_too_short is the same root cause (Item 1 absent or
        # truncated) so it's grouped with no_item_1 in the UI.
        instrument_id = _seed_instrument(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instrument_business_summary
                    (instrument_id, body, source_accession,
                     attempt_count, last_failure_reason, next_retry_at,
                     last_parsed_at)
                VALUES (%s, '', 'acc-2', 1, 'body_too_short',
                        now() + interval '1 day', now())
                """,
                (instrument_id,),
            )
        try:
            ps = get_parse_status(conn, instrument_id=instrument_id)
            assert ps is not None
            assert ps.state == "no_item_1"
        finally:
            _delete_summary(conn, instrument_id)

    def test_parse_failed_for_real_failures(self, conn: psycopg.Connection[object]) -> None:
        # fetch_other / parse_exception / fetch_timeout are real
        # failures the operator might want to investigate; they map
        # to the parse_failed state distinct from no_item_1.
        instrument_id = _seed_instrument(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instrument_business_summary
                    (instrument_id, body, source_accession,
                     attempt_count, last_failure_reason, next_retry_at,
                     last_parsed_at)
                VALUES (%s, '', 'acc-3', 2, 'parse_exception',
                        now() + interval '7 days', now())
                """,
                (instrument_id,),
            )
        try:
            ps = get_parse_status(conn, instrument_id=instrument_id)
            assert ps is not None
            assert ps.state == "parse_failed"
            assert ps.failure_reason == "parse_exception"
            assert ps.next_retry_at is not None
            assert ps.last_attempted_at is not None
        finally:
            _delete_summary(conn, instrument_id)

    def test_sections_pending_when_body_set_but_no_sections(self, conn: psycopg.Connection[object]) -> None:
        # Parent row has a real body — the splitter just hasn't written
        # children yet. Transient state; the splitter trigger should
        # run shortly.
        instrument_id = _seed_instrument(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instrument_business_summary
                    (instrument_id, body, source_accession, last_parsed_at)
                VALUES (%s, 'real Item 1 body text', 'acc-4', now())
                """,
                (instrument_id,),
            )
        try:
            ps = get_parse_status(conn, instrument_id=instrument_id)
            assert ps is not None
            assert ps.state == "sections_pending"
            assert ps.last_attempted_at is not None
        finally:
            _delete_summary(conn, instrument_id)

    def test_parse_failed_when_failure_reason_is_null(self, conn: psycopg.Connection[object]) -> None:
        # Edge case the bot flagged: a row with body='' but
        # last_failure_reason IS NULL (race window where the ingester
        # wrote the tombstone but failed before stamping the reason,
        # or a manual operator-inserted row). Falls through to
        # parse_failed with failure_reason=None — UI omits the
        # parenthetical so it reads "Parser failed." rather than
        # "Parser failed (None)" or similar awkward fallback.
        instrument_id = _seed_instrument(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instrument_business_summary
                    (instrument_id, body, source_accession,
                     attempt_count, last_parsed_at)
                VALUES (%s, '', 'acc-null-reason', 1, now())
                """,
                (instrument_id,),
            )
        try:
            ps = get_parse_status(conn, instrument_id=instrument_id)
            assert ps is not None
            assert ps.state == "parse_failed"
            assert ps.failure_reason is None
            assert ps.last_attempted_at is not None
        finally:
            _delete_summary(conn, instrument_id)
