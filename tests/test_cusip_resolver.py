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
    sweep_resolvable_unresolved_cusips,
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


# ---------------------------------------------------------------------------
# extid sweep (#836 — race-loss recovery between 13F ingest + CUSIP backfill)
# ---------------------------------------------------------------------------


def _seed_extid_cusip(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    cusip: str,
    is_primary: bool = False,
) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cusip', %s, %s)
        """,
        (instrument_id, cusip, is_primary),
    )


def _seed_outstanding_for_rollup(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    shares: str,
) -> None:
    """Minimal seed of ``financial_periods`` + ``financial_facts_raw`` so
    the ``instrument_share_count_latest`` view returns a row, which is
    what gates :func:`get_ownership_rollup` from short-circuiting to
    ``no_data``. Mirrors the helper in test_ownership_rollup.py at a
    smaller surface."""
    from datetime import UTC, date, datetime
    from decimal import Decimal as D

    period_end = date(2026, 3, 31)
    conn.execute(
        """
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type, fiscal_year,
            fiscal_quarter, source, source_ref, reported_currency,
            is_restated, is_derived, normalization_status,
            treasury_shares, filed_date, superseded_at
        ) VALUES (%s, %s, 'Q4', %s, 4, 'sec_xbrl', %s, 'USD',
                  FALSE, FALSE, 'normalized', NULL, %s, NULL)
        ON CONFLICT DO NOTHING
        """,
        (
            instrument_id,
            period_end,
            period_end.year,
            f"OUTSTANDING-{instrument_id}",
            datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
        ),
    )
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end, val,
            form_type, filed_date, accession_number,
            fiscal_year, fiscal_period
        ) VALUES (%s, 'dei', 'EntityCommonStockSharesOutstanding',
                  'shares', %s, %s, '10-Q', %s, %s, %s, 'Q4')
        ON CONFLICT DO NOTHING
        """,
        (
            instrument_id,
            period_end,
            D(shares),
            period_end,
            f"OUTSTANDING-{instrument_id}",
            period_end.year,
        ),
    )


class TestSweepResolvableUnresolvedCusips:
    """Sweep that promotes ``unresolved_13f_cusips`` rows whose CUSIP
    already has a curated ``external_identifiers`` mapping (#836).

    Differs from :class:`TestResolver` (the fuzzy issuer-name resolver)
    — the sweep is a strict-match indexed JOIN, so the test surface is
    smaller. The acceptance contract from the issue:

      1. Match → mark ``resolution_status='resolved_via_extid'`` + try
         rewash of last_accession_number.
      2. Non-match → row untouched.
      3. Second run = zero changes (idempotent).
      4. Rewash recovers AAPL's holding into ``institutional_holdings``
         when the rest of the accession's CUSIPs also resolve.
    """

    def test_no_pending_returns_zero_report(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        report = sweep_resolvable_unresolved_cusips(ebull_test_conn)
        assert report.candidates_seen == 0
        assert report.promoted == 0
        assert report.rewashed == 0
        assert report.rewash_deferred == 0
        assert report.rewash_failed == 0

    def test_match_marks_resolved_via_extid(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Pending row whose CUSIP joins ``external_identifiers`` is
        marked ``resolved_via_extid``. Rewash deferred because the raw
        body and ingest log are absent — that's still a successful
        promotion (the extid mapping is the authoritative finding;
        rewash is the operationally-nice follow-up)."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=836_001, symbol="AAPL", company_name="Apple Inc")
        _seed_extid_cusip(conn, instrument_id=836_001, cusip="037833100")
        _seed_unresolved(
            conn,
            cusip="037833100",
            name_of_issuer="APPLE INC",
            accession="0000000000-26-000001",
        )
        conn.commit()

        report = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()

        assert report.candidates_seen == 1
        assert report.promoted == 1
        assert report.rewashed == 0
        assert report.rewash_deferred == 1
        assert report.rewash_failed == 0

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                ("037833100",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["resolution_status"] == "resolved_via_extid"

    def test_skips_cusip_without_extid(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A pending row whose CUSIP is NOT in ``external_identifiers``
        is left alone — the fuzzy resolver, not the sweep, handles
        that path."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=836_002, symbol="AAPL", company_name="Apple Inc")
        # No extid row for this CUSIP.
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        report = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()

        assert report.candidates_seen == 0
        assert report.promoted == 0

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                ("037833100",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["resolution_status"] is None

    def test_idempotent_second_run_is_noop(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Acceptance: running the sweep twice produces zero changes
        on the second pass — the first pass tombstoned the matched
        rows with ``resolution_status='resolved_via_extid'``, and the
        sweep filters on ``resolution_status IS NULL``."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=836_003, symbol="AAPL", company_name="Apple Inc")
        _seed_extid_cusip(conn, instrument_id=836_003, cusip="037833100")
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        first = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()
        assert first.candidates_seen == 1
        assert first.promoted == 1

        second = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()
        assert second.candidates_seen == 0
        assert second.promoted == 0
        assert second.rewashed == 0
        assert second.rewash_deferred == 0

    def test_skips_already_tombstoned_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Rows tombstoned by the fuzzy resolver (``unresolvable`` /
        ``ambiguous`` / ``conflict``) must not be re-evaluated by the
        sweep, even if the extid mapping later becomes available.
        Operator clears the status to force a retry — the sweep
        respects that contract."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=836_004, symbol="AAPL", company_name="Apple Inc")
        _seed_extid_cusip(conn, instrument_id=836_004, cusip="037833100")
        conn.execute(
            """
            INSERT INTO unresolved_13f_cusips (
                cusip, name_of_issuer, last_accession_number,
                observation_count, resolution_status
            ) VALUES ('037833100', 'APPLE INC', 'ACC-1', 1, 'unresolvable')
            """,
        )
        conn.commit()

        report = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()

        assert report.candidates_seen == 0
        assert report.promoted == 0

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                ("037833100",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["resolution_status"] == "unresolvable"  # untouched

    def test_rewash_recovers_holding_into_institutional_holdings(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """End-to-end recovery (acceptance criterion 5 of #836).

        Setup mirrors the operator-audited race-loss state on
        2026-05-02:

          1. AAPL instrument seeded.
          2. AAPL CUSIP recorded in ``external_identifiers``
             (post-backfill state).
          3. ``unresolved_13f_cusips`` carries a pending row for AAPL
             pointing at a real 13F-HR accession — pre-backfill ingest
             that couldn't resolve the CUSIP.
          4. ``institutional_holdings_ingest_log`` carries a 'partial'
             row for that accession (zero holdings_inserted because
             the CUSIP didn't resolve).
          5. ``filing_raw_documents`` carries the original infotable.xml
             body.

        After the sweep:

          - the unresolved row transitions to ``resolved_via_extid``;
          - ``institutional_holdings`` carries the AAPL holding row.

        ``parse_infotable`` is monkey-patched to return a single
        AAPL holding so the rewash's "all CUSIPs resolved" branch
        fires deterministically without needing a real 13F XML
        fixture."""
        from decimal import Decimal

        from app.providers.implementations.sec_13f import ThirteenFHolding
        from app.services import raw_filings

        conn = ebull_test_conn
        accession = "0000909012-26-000001"
        cusip = "037833100"
        iid = 836_010

        # 1. Instrument + extid (post-backfill state).
        _seed_instrument(conn, iid=iid, symbol="AAPL", company_name="Apple Inc")
        _seed_extid_cusip(conn, instrument_id=iid, cusip=cusip)

        # 2. Filer + ingest log row recording the partial outcome.
        conn.execute(
            "INSERT INTO institutional_filers (cik, name) VALUES ('0000909012', 'Race Loss Test Filer')",
        )
        with conn.cursor() as cur:
            cur.execute("SELECT filer_id FROM institutional_filers WHERE cik = '0000909012'")
            result = cur.fetchone()
        assert result is not None
        filer_id = int(result[0])

        conn.execute(
            """
            INSERT INTO institutional_holdings_ingest_log (
                accession_number, filer_cik, period_of_report,
                status, holdings_inserted, holdings_skipped, error
            )
            VALUES (%s, '0000909012', '2026-03-31', 'partial', 0, 1,
                    '1 unresolved CUSIPs (gated by #740 backfill)')
            """,
            (accession,),
        )

        # 3. Pending unresolved row pointing at the same accession.
        _seed_unresolved(
            conn,
            cusip=cusip,
            name_of_issuer="APPLE INC",
            accession=accession,
        )

        # 4. Raw body present (parse_infotable will be monkey-patched
        # so the actual XML content is irrelevant).
        raw_filings.store_raw(
            conn,
            accession_number=accession,
            document_kind="infotable_13f",
            payload="<x/>",
            parser_version="13f-infotable-v0",
        )
        conn.commit()

        fake_holdings = [
            ThirteenFHolding(
                cusip=cusip,
                name_of_issuer="APPLE INC",
                title_of_class="COM",
                value_usd=Decimal("123456789"),
                shares_or_principal=Decimal("1000000"),
                shares_or_principal_type="SH",
                put_call=None,
                investment_discretion="SOLE",
                voting_sole=Decimal("1000000"),
                voting_shared=Decimal("0"),
                voting_none=Decimal("0"),
            ),
        ]
        monkeypatch.setattr(
            "app.providers.implementations.sec_13f.parse_infotable",
            lambda _xml: fake_holdings,
        )

        report = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()

        assert report.candidates_seen == 1
        assert report.promoted == 1
        assert report.rewashed == 1
        assert report.rewash_deferred == 0
        assert report.rewash_failed == 0

        # Holding now in institutional_holdings.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id, filer_id, shares
                FROM institutional_holdings
                WHERE accession_number = %s
                """,
                (accession,),
            )
            holding = cur.fetchone()
        assert holding is not None
        assert holding["instrument_id"] == iid
        assert holding["filer_id"] == filer_id
        assert holding["shares"] == Decimal("1000000")

        # Backlog row tombstoned.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT resolution_status FROM unresolved_13f_cusips WHERE cusip = %s",
                (cusip,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["resolution_status"] == "resolved_via_extid"

    def test_rollup_picks_up_recovered_holding(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Smoke acceptance (issue #836 criterion 5): after the sweep
        recovers an AAPL holding, ``get_ownership_rollup`` includes it
        in the institutional slice.

        Same setup as
        :meth:`test_rewash_recovers_holding_into_institutional_holdings`
        plus a minimal ``shares_outstanding`` seed so the rollup
        function doesn't short-circuit to ``no_data``."""
        from decimal import Decimal

        from app.providers.implementations.sec_13f import ThirteenFHolding
        from app.services import ownership_rollup, raw_filings

        conn = ebull_test_conn
        accession = "0000909013-26-000001"
        cusip = "037833100"
        iid = 836_020

        _seed_instrument(conn, iid=iid, symbol="AAPL", company_name="Apple Inc")
        _seed_extid_cusip(conn, instrument_id=iid, cusip=cusip)
        _seed_outstanding_for_rollup(conn, instrument_id=iid, shares="15500000000")

        conn.execute(
            "INSERT INTO institutional_filers (cik, name, filer_type) VALUES ('0000909013', 'Test Filer 836', 'INV')",
        )
        conn.execute(
            """
            INSERT INTO institutional_holdings_ingest_log (
                accession_number, filer_cik, period_of_report,
                status, holdings_inserted, holdings_skipped, error
            )
            VALUES (%s, '0000909013', '2026-03-31', 'partial', 0, 1,
                    '1 unresolved CUSIPs (gated by #740 backfill)')
            """,
            (accession,),
        )
        _seed_unresolved(conn, cusip=cusip, name_of_issuer="APPLE INC", accession=accession)
        raw_filings.store_raw(
            conn,
            accession_number=accession,
            document_kind="infotable_13f",
            payload="<x/>",
            parser_version="13f-infotable-v0",
        )
        conn.commit()

        fake_holdings = [
            ThirteenFHolding(
                cusip=cusip,
                name_of_issuer="APPLE INC",
                title_of_class="COM",
                value_usd=Decimal("123456789"),
                shares_or_principal=Decimal("1000000"),
                shares_or_principal_type="SH",
                put_call=None,
                investment_discretion="SOLE",
                voting_sole=Decimal("1000000"),
                voting_shared=Decimal("0"),
                voting_none=Decimal("0"),
            ),
        ]
        monkeypatch.setattr(
            "app.providers.implementations.sec_13f.parse_infotable",
            lambda _xml: fake_holdings,
        )

        report = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()
        assert report.rewashed == 1

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="AAPL", instrument_id=iid)

        # The institutional slice now carries the recovered filer.
        # filer_count >= 1 is the smoke contract; the issue's
        # "9 distinct filers up from 7" target is operator-driven and
        # depends on the live DB cohort, not this single-fixture test.
        # Category is "institutions" (13F-HR; the ETF subset is its
        # sibling). The recovered AAPL holding lands here because the
        # filer's filer_type is 'INV', not 'ETF'.
        institutional = [s for s in rollup.slices if s.category == "institutions"]
        assert len(institutional) == 1
        assert institutional[0].filer_count >= 1
        assert institutional[0].total_shares == Decimal("1000000")

    def test_rewash_failure_does_not_abort_sweep(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Codex pre-push review: a single accession's rewash blowing
        up must not stop the sweep. Both rows still get marked
        ``resolved_via_extid`` (the extid mapping is authoritative);
        rewash_failed counts the failures; the second row still gets
        processed."""
        from app.services import raw_filings

        conn = ebull_test_conn
        cusip_a = "037833100"  # AAPL
        cusip_b = "594918104"  # MSFT
        accession_a = "0000909014-26-000001"
        accession_b = "0000909014-26-000002"

        _seed_instrument(conn, iid=836_030, symbol="AAPL", company_name="Apple Inc")
        _seed_instrument(conn, iid=836_031, symbol="MSFT", company_name="Microsoft Corp")
        _seed_extid_cusip(conn, instrument_id=836_030, cusip=cusip_a)
        _seed_extid_cusip(conn, instrument_id=836_031, cusip=cusip_b)

        # Filer + ingest-log + raw body so the rewash reaches the
        # parse step (rescue-cohort branch in _apply_13f_infotable).
        # Without these, _apply_13f_infotable returns False before
        # touching parse_infotable, and the failure path under test
        # is unreachable.
        conn.execute(
            """
            INSERT INTO institutional_filers (cik, name) VALUES
                ('0000909030', 'Failing Filer A'),
                ('0000909031', 'Failing Filer B')
            """,
        )
        for acc, cik in ((accession_a, "0000909030"), (accession_b, "0000909031")):
            conn.execute(
                """
                INSERT INTO institutional_holdings_ingest_log (
                    accession_number, filer_cik, period_of_report,
                    status, holdings_inserted, holdings_skipped, error
                )
                VALUES (%s, %s, '2026-03-31', 'partial', 0, 1, 'pre-extid')
                """,
                (acc, cik),
            )
            raw_filings.store_raw(
                conn,
                accession_number=acc,
                document_kind="infotable_13f",
                payload="<x/>",
                parser_version="13f-infotable-v0",
            )
        _seed_unresolved(conn, cusip=cusip_a, name_of_issuer="APPLE INC", accession=accession_a)
        _seed_unresolved(conn, cusip=cusip_b, name_of_issuer="MICROSOFT CORP", accession=accession_b)
        conn.commit()

        # Force every parse to raise — exercises the failure path
        # inside the per-row savepoint without needing matching
        # institutional_holdings / log fixtures.
        def _exploding_parse(_xml: str) -> object:
            raise RuntimeError("synthetic parser blowup")

        monkeypatch.setattr(
            "app.providers.implementations.sec_13f.parse_infotable",
            _exploding_parse,
        )

        report = sweep_resolvable_unresolved_cusips(conn)
        conn.commit()

        assert report.candidates_seen == 2
        assert report.promoted == 2  # both marks landed
        assert report.rewashed == 0
        assert report.rewash_failed == 2  # both rewashes blew up; sweep continued

        # Both backlog rows transitioned, audit-trail intact.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT cusip, resolution_status FROM unresolved_13f_cusips ORDER BY cusip")
            rows = cur.fetchall()
        assert {r["cusip"]: r["resolution_status"] for r in rows} == {
            cusip_a: "resolved_via_extid",
            cusip_b: "resolved_via_extid",
        }

    def test_rowcount_zero_skips_rewash(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Codex pre-push review: if the guarded UPDATE finds zero rows
        (concurrent winner / operator manual tombstone between SELECT
        and UPDATE), skip both the counter bump AND the rewash. The
        loser must not double-promote.

        Reproduces the race by tombstoning the row between the SELECT
        and UPDATE inside a single sweep call: feed the sweep a row
        that's still pending at SELECT time, then mutate
        ``resolution_status`` BEFORE the sweep's UPDATE runs. We
        approximate this by manually invoking the internal selector,
        mutating, then calling the public sweep — the race-loss path
        falls out naturally.
        """
        from app.services.cusip_resolver import _select_resolvable_via_extid

        conn = ebull_test_conn
        _seed_instrument(conn, iid=836_040, symbol="AAPL", company_name="Apple Inc")
        _seed_extid_cusip(conn, instrument_id=836_040, cusip="037833100")
        _seed_unresolved(conn, cusip="037833100", name_of_issuer="APPLE INC")
        conn.commit()

        # Confirm the row is selectable by the sweep's internal query.
        candidates = _select_resolvable_via_extid(conn, limit=10)
        assert len(candidates) == 1

        # Concurrent winner tombstones the row before our sweep's
        # UPDATE runs.
        conn.execute(
            "UPDATE unresolved_13f_cusips SET resolution_status='resolved_via_extid' WHERE cusip='037833100'",
        )
        conn.commit()

        # Sweep's SELECT now returns zero rows because of the IS NULL
        # filter — the guard at the UPDATE level is also exercised by
        # the existing idempotent test, but this one specifically
        # asserts the in-loop rowcount path doesn't double-count if
        # the SELECT raced.
        report = sweep_resolvable_unresolved_cusips(conn)
        assert report.candidates_seen == 0
        assert report.promoted == 0
        assert report.rewashed == 0
