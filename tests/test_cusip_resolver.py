"""Integration tests for the 13F CUSIP resolver (#781).

Exercises the full path:
  1. Unresolved CUSIPs land in ``unresolved_13f_cusips`` (populated
     by the 13F-HR ingester on each unresolved holding — verified
     via the existing ingester test path; here we seed directly).
  2. ``resolve_unresolved_cusips`` walks the table, fuzzy-matches
     against ``instruments.company_name``, and either promotes
     into ``external_identifiers`` or tombstones.
  3. Idempotency: a second run on the same data is a no-op.
"""

from __future__ import annotations

import psycopg
import psycopg.rows
import pytest

from app.services.cusip_resolver import (
    MATCH_THRESHOLD,
    _normalise_name,
    iter_pending_unresolved,
    resolve_unresolved_cusips,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str, company_name: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, company_name),
    )


def _seed_unresolved(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    name_of_issuer: str,
    accession: str = "0001234567-25-000001",
    observation_count: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO unresolved_13f_cusips (
            cusip, name_of_issuer, last_accession_number, observation_count
        ) VALUES (%s, %s, %s, %s)
        """,
        (cusip, name_of_issuer, accession, observation_count),
    )


# ---------------------------------------------------------------------------
# Pure normalisation tests (no DB)
# ---------------------------------------------------------------------------


class TestNormaliseName:
    def test_strips_corporate_suffix(self) -> None:
        assert _normalise_name("Apple Inc") == "APPLE"
        assert _normalise_name("Berkshire Hathaway Inc.") == "BERKSHIRE HATHAWAY"
        assert _normalise_name("Alphabet Corporation") == "ALPHABET"
        assert _normalise_name("Vanguard Group LLC") == "VANGUARD"

    def test_strips_share_class_suffix(self) -> None:
        assert _normalise_name("Berkshire Hathaway Inc Class B") == "BERKSHIRE HATHAWAY"
        assert _normalise_name("Alphabet Inc CL A") == "ALPHABET"
        assert _normalise_name("Apple Inc Common Stock") == "APPLE"

    def test_drops_bracketed_qualifiers(self) -> None:
        assert _normalise_name("Apple Inc (NEW)") == "APPLE"

    def test_normalises_punctuation_and_whitespace(self) -> None:
        assert _normalise_name("  Apple,  Inc.  ") == "APPLE"
        assert _normalise_name("BLOCK H&R") == "BLOCK H&R"  # ampersand preserved

    def test_returns_empty_on_pure_suffix(self) -> None:
        assert _normalise_name("Inc") == ""
        assert _normalise_name("LLC") == ""
        assert _normalise_name("") == ""


# ---------------------------------------------------------------------------
# End-to-end resolver tests
# ---------------------------------------------------------------------------


class TestResolver:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=781_001, symbol="AAPL", company_name="Apple Inc")
        _seed_instrument(conn, iid=781_002, symbol="BRK.B", company_name="Berkshire Hathaway Inc")
        _seed_instrument(conn, iid=781_003, symbol="GOOG", company_name="Alphabet Inc")
        conn.commit()
        return conn

    def test_exact_normalised_match_promotes(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        report = resolve_unresolved_cusips(conn)
        conn.commit()

        assert report.promotions == 1
        assert report.tombstones == 0

        # external_identifiers has the new mapping.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id FROM external_identifiers
                WHERE provider='sec' AND identifier_type='cusip' AND identifier_value=%s
                """,
                ("037833100",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 781_001

        # unresolved_13f_cusips no longer carries the row.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM unresolved_13f_cusips WHERE cusip = %s",
                ("037833100",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_share_class_variant_matches(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """``"BERKSHIRE HATHAWAY INC CL B"`` resolves to instrument
        with company_name ``"Berkshire Hathaway Inc"`` — share-class
        suffix stripped by the normaliser."""
        conn = _setup
        _seed_unresolved(
            conn,
            cusip="084670702",
            name_of_issuer="BERKSHIRE HATHAWAY INC CL B",
        )
        conn.commit()

        report = resolve_unresolved_cusips(conn)
        conn.commit()

        assert report.promotions == 1
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id FROM external_identifiers
                WHERE identifier_value = '084670702'
                """,
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 781_002

    def test_no_candidate_match_tombstones(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A CUSIP whose issuer name doesn't fuzzy-match any
        instrument is tombstoned, not silently dropped."""
        conn = _setup
        _seed_unresolved(
            conn,
            cusip="999999999",
            name_of_issuer="WHO KNOWS WHAT THIS IS LLC",
        )
        conn.commit()

        report = resolve_unresolved_cusips(conn)
        conn.commit()

        assert report.promotions == 0
        assert report.tombstones == 1
        assert report.tombstoned_unresolvable == 1
        assert report.tombstoned_ambiguous == 0
        assert report.tombstoned_conflict == 0

        # No external_identifiers row written.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM external_identifiers WHERE identifier_value = '999999999'",
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

        # Source row is tombstoned, not deleted (audit trail
        # preserved; operator can clear status to retry).
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                ("999999999",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["resolution_status"] == "unresolvable"

    def test_re_run_skips_tombstoned_rows(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A second run after tombstoning should not re-evaluate
        the unresolvable rows. iter_pending_unresolved returns
        only NULL-status rows."""
        conn = _setup
        _seed_unresolved(conn, cusip="999999999", name_of_issuer="UNKNOWN")
        conn.commit()

        first = resolve_unresolved_cusips(conn)
        conn.commit()
        assert first.tombstones == 1

        # Second run sees zero pending.
        second = resolve_unresolved_cusips(conn)
        conn.commit()
        assert second.candidates_seen == 0
        assert second.promotions == 0
        assert second.tombstones == 0

    def test_idempotent_promotion_does_not_duplicate_external_identifier(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Running the resolver twice on the same successful match
        produces exactly one external_identifiers row."""
        conn = _setup
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        resolve_unresolved_cusips(conn)
        conn.commit()

        # Re-seed (operator force-retry) and re-run.
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        resolve_unresolved_cusips(conn)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM external_identifiers
                WHERE identifier_type = 'cusip' AND identifier_value = '037833100'
                """,
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_high_observation_count_is_processed_first(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Resolver order is observation_count DESC so high-leverage
        CUSIPs resolve first under a small ``limit``."""
        conn = _setup
        _seed_unresolved(conn, cusip="999999991", name_of_issuer="UNKNOWN A", observation_count=100)
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC", observation_count=5)
        _seed_unresolved(conn, cusip="999999992", name_of_issuer="UNKNOWN B", observation_count=50)
        conn.commit()

        # iter_pending_unresolved confirms ordering.
        pending = list(iter_pending_unresolved(conn, limit=10))
        assert [p["cusip"] for p in pending] == ["999999991", "999999992", "037833100"]

    def test_threshold_floor_blocks_marginal_match(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A CUSIP whose name shares a common prefix with an
        instrument but isn't the same company is tombstoned, not
        promoted. Codex-style false-positive guard."""
        conn = _setup
        # "Apple Hospitality REIT" should NOT match "Apple Inc"
        # despite the shared prefix.
        _seed_unresolved(
            conn,
            cusip="037833777",
            name_of_issuer="APPLE HOSPITALITY REIT",
        )
        conn.commit()

        report = resolve_unresolved_cusips(conn)
        conn.commit()

        assert report.promotions == 0
        assert report.tombstones == 1
        assert report.tombstoned_unresolvable == 1

    def test_share_class_collision_tombstones_as_ambiguous(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``"ALPHABET INC CL A"`` and ``"ALPHABET INC CL C"`` both
        normalise to ``"ALPHABET"`` after the share-class strip, so
        an unresolved CUSIP for either class has two equally-good
        candidates. The resolver refuses to pick arbitrarily and
        tombstones the row as ``ambiguous``. Codex pre-push review
        caught the prior code's first-wins behaviour."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=781_010, symbol="GOOGL", company_name="Alphabet Inc CL A")
        _seed_instrument(conn, iid=781_011, symbol="GOOG", company_name="Alphabet Inc CL C")
        _seed_unresolved(
            conn,
            cusip="02079K305",
            name_of_issuer="ALPHABET INC CL C",
        )
        conn.commit()

        report = resolve_unresolved_cusips(conn)
        conn.commit()

        assert report.promotions == 0
        assert report.tombstoned_ambiguous == 1
        assert report.tombstoned_unresolvable == 0

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                ("02079K305",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["resolution_status"] == "ambiguous"

        # external_identifiers untouched.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM external_identifiers WHERE identifier_value = '02079K305'",
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_pre_existing_conflict_is_preserved(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """If ``external_identifiers`` already maps the CUSIP to a
        DIFFERENT instrument_id, the resolver MUST preserve the
        existing mapping (never silently overwrite) and tombstone
        the source row as ``conflict`` for operator audit. Codex
        pre-push review caught the prior ON-CONFLICT-DO-NOTHING
        path silently treating this as a successful promotion."""
        conn = _setup
        # Pre-existing (wrong) mapping — pretend the curated path
        # wired this CUSIP to BRK.B by mistake.
        conn.execute(
            """
            INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cusip', '037833100', TRUE)
            """,
            (781_002,),  # BRK.B
        )
        # Resolver-side: backlog says this CUSIP is "APPLE INC".
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        report = resolve_unresolved_cusips(conn)
        conn.commit()

        # Resolver did NOT overwrite. Existing (wrong) mapping
        # preserved; backlog row tombstoned as conflict.
        assert report.promotions == 0
        assert report.tombstoned_conflict == 1
        assert report.tombstoned_unresolvable == 0

        with conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM external_identifiers WHERE identifier_value = '037833100'",
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 781_002  # original (wrong) mapping preserved

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                ("037833100",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["resolution_status"] == "conflict"

    def test_pre_existing_same_mapping_counts_as_already_resolved(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """If ``external_identifiers`` already maps this CUSIP to
        the SAME instrument_id (race-loss / curated path beat us),
        the source backlog row is safely deleted and the resolver
        reports ``already_resolved`` instead of ``promotions``."""
        conn = _setup
        # Pre-existing correct mapping.
        conn.execute(
            """
            INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cusip', '037833100', TRUE)
            """,
            (781_001,),  # AAPL
        )
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        report = resolve_unresolved_cusips(conn)
        conn.commit()

        assert report.promotions == 0
        assert report.already_resolved == 1
        assert report.tombstones == 0

        # Backlog row removed; existing mapping unchanged.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM unresolved_13f_cusips WHERE cusip = '037833100'")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_threshold_default_value(self) -> None:
        """Pin the conservative default. Loosening this constant
        is a deliberate change that warrants its own PR."""
        assert MATCH_THRESHOLD == 0.92

    def test_empty_pending_returns_zero_report(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        report = resolve_unresolved_cusips(ebull_test_conn)
        assert report.candidates_seen == 0
        assert report.promotions == 0
        assert report.tombstones == 0


# ---------------------------------------------------------------------------
# Ingester integration: unresolved CUSIPs are tracked
# ---------------------------------------------------------------------------


class TestIngesterTracksUnresolvedCusips:
    """Verifies the change in app/services/institutional_holdings.py
    that records unresolved CUSIPs into the new tracking table.
    Pulls the existing institutional-holdings ingester test fixtures
    by reproducing the minimal seed shape inline."""

    def test_unresolved_cusip_tracked_with_observation_count(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Direct DB-level test: simulating the ingester's call to
        _record_unresolved_cusip via two upserts on the same CUSIP
        bumps the observation count instead of inserting a duplicate.
        Avoids re-mocking the SEC fetcher just to exercise this
        path — the institutional_holdings test suite covers the
        full ingest pipeline."""
        from app.services.institutional_holdings import _record_unresolved_cusip

        conn = ebull_test_conn
        _record_unresolved_cusip(
            conn,
            cusip="037833100",
            name_of_issuer="APPLE INC",
            accession_number="ACC-1",
        )
        _record_unresolved_cusip(
            conn,
            cusip="037833100",
            name_of_issuer="APPLE INC. (NEW)",  # name variant
            accession_number="ACC-2",
        )
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT cusip, name_of_issuer, last_accession_number, observation_count
                FROM unresolved_13f_cusips
                WHERE cusip = '037833100'
                """,
            )
            row = cur.fetchone()
        assert row is not None
        assert row["observation_count"] == 2
        assert row["name_of_issuer"] == "APPLE INC. (NEW)"  # latest wins
        assert row["last_accession_number"] == "ACC-2"
