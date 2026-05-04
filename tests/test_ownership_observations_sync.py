"""Tests for the legacy → observations sync (#840.E-prep).

Each ``sync_<category>`` reads the legacy typed table, mirrors rows
to ``ownership_<category>_observations``, then refreshes ``_current``.
"""

from __future__ import annotations

from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.ownership_observations_sync import (
    sync_blockholders,
    sync_def14a,
    sync_insiders,
    sync_institutions,
    sync_treasury,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


# ---------------------------------------------------------------------------
# Insiders sync
# ---------------------------------------------------------------------------


class TestSyncInsiders:
    def test_form4_transactions_mirror_to_observations(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_001, symbol="GME")
        accession = "0001767470-26-000001"
        conn.execute(
            """
            INSERT INTO insider_filings (
                accession_number, instrument_id, document_type, issuer_cik
            ) VALUES (%s, %s, '4', '0000000789')
            """,
            (accession, 841_001),
        )
        conn.execute(
            """
            INSERT INTO insider_transactions (
                accession_number, txn_row_num, instrument_id, filer_cik, filer_name,
                txn_date, txn_code, shares, post_transaction_shares, is_derivative
            ) VALUES (%s, 1, %s, '0001767470', 'Cohen Ryan', '2026-01-21', 'P', 100, 38347842, FALSE)
            """,
            (accession, 841_001),
        )
        conn.commit()

        summary = sync_insiders(conn)
        conn.commit()

        assert summary.rows_scanned >= 1
        assert summary.observations_recorded >= 1
        assert summary.instruments_refreshed >= 1
        assert summary.orphans == []

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT holder_cik, source, shares, ownership_nature
                FROM ownership_insiders_current WHERE instrument_id = %s
                """,
                (841_001,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["holder_cik"] == "0001767470"
        assert rows[0]["source"] == "form4"
        assert rows[0]["ownership_nature"] == "direct"
        assert rows[0]["shares"] == Decimal("38347842")


# ---------------------------------------------------------------------------
# Institutions sync — orphan filer_id loud
# ---------------------------------------------------------------------------


class TestSyncInstitutions:
    def test_orphan_filer_logged_not_dropped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Codex plan-review finding #2: filer_id without parent row
        must be flagged loudly, not silently dropped. The sync uses
        an inner JOIN on institutional_filers so a missing parent
        means the row simply never enters the candidate set — but the
        orphan path is reachable when the JOINed cik is blank."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_100, symbol="VG")
        # Filer with blank cik (degenerate seed).
        conn.execute("INSERT INTO institutional_filers (cik, name) VALUES ('   ', 'Blank Cik Filer')")
        with conn.cursor() as cur:
            cur.execute("SELECT filer_id FROM institutional_filers WHERE name = 'Blank Cik Filer'")
            row = cur.fetchone()
        assert row is not None
        filer_id = int(row[0])
        conn.execute(
            """
            INSERT INTO institutional_holdings (
                filer_id, instrument_id, accession_number, period_of_report,
                shares, voting_authority, filed_at
            ) VALUES (%s, %s, 'ACC-BLANK', '2026-03-31', 100, 'SOLE', '2026-04-15')
            """,
            (filer_id, 841_100),
        )
        conn.commit()

        summary = sync_institutions(conn)
        conn.commit()

        assert summary.rows_scanned == 1
        assert summary.observations_recorded == 0
        assert any("blank cik" in o for o in summary.orphans)

    def test_happy_path_records_and_refreshes(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_101, symbol="AAPL")
        conn.execute(
            """
            INSERT INTO institutional_filers (cik, name, filer_type)
            VALUES ('0000102909', 'Vanguard Group Inc', 'ETF')
            """
        )
        with conn.cursor() as cur:
            cur.execute("SELECT filer_id FROM institutional_filers WHERE cik = '0000102909'")
            row = cur.fetchone()
        assert row is not None
        filer_id = int(row[0])
        conn.execute(
            """
            INSERT INTO institutional_holdings (
                filer_id, instrument_id, accession_number, period_of_report,
                shares, market_value_usd, voting_authority, filed_at
            ) VALUES (%s, %s, 'ACC-VG-Q1', '2026-03-31', 1500000000, 250000000000, 'SOLE', '2026-04-15')
            """,
            (filer_id, 841_101),
        )
        conn.commit()

        summary = sync_institutions(conn)
        conn.commit()

        assert summary.observations_recorded == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT filer_cik, shares, exposure_kind, voting_authority
                FROM ownership_institutions_current WHERE instrument_id = %s
                """,
                (841_101,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["filer_cik"] == "0000102909"
        assert rows[0]["shares"] == Decimal("1500000000")
        assert rows[0]["exposure_kind"] == "EQUITY"
        assert rows[0]["voting_authority"] == "SOLE"


# ---------------------------------------------------------------------------
# Blockholders sync — joint reporters collapse
# ---------------------------------------------------------------------------


class TestSyncBlockholders:
    def test_joint_reporters_collapse_per_accession(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Two reporters (joint) under one accession with the SAME
        primary filer must yield ONE observation (per #837 lesson).
        DISTINCT ON in the sync collapses them."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_200, symbol="GME")
        conn.execute("INSERT INTO blockholder_filers (cik, name) VALUES ('0001767470', 'Cohen Ryan')")
        accession = "0000921895-25-000190"
        for reporter_cik in ("0001767470", "0001650235"):
            conn.execute(
                """
                INSERT INTO blockholder_filings (
                    filer_id, accession_number, submission_type, status,
                    instrument_id, issuer_cik, issuer_cusip,
                    reporter_cik, reporter_no_cik, reporter_name,
                    aggregate_amount_owned, filed_at
                )
                SELECT filer_id, %s, 'SCHEDULE 13D/A', 'active', %s,
                       '0000000789', '999999999',
                       %s, FALSE, 'Joint Reporter',
                       36847842, '2025-01-29 00:00:00+00'
                FROM blockholder_filers WHERE cik = '0001767470'
                """,
                (accession, 841_200, reporter_cik),
            )
        conn.commit()

        summary = sync_blockholders(conn)
        conn.commit()

        assert summary.observations_recorded == 1  # joint pair → one row

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT reporter_cik, ownership_nature, source, aggregate_amount_owned
                FROM ownership_blockholders_current WHERE instrument_id = %s
                """,
                (841_200,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["reporter_cik"] == "0001767470"  # primary, not joint
        assert rows[0]["aggregate_amount_owned"] == Decimal("36847842")


# ---------------------------------------------------------------------------
# Treasury sync
# ---------------------------------------------------------------------------


class TestSyncTreasury:
    def test_mirrors_financial_periods_treasury_into_observations(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_300, symbol="JPM")
        conn.execute(
            """
            INSERT INTO financial_periods (
                instrument_id, period_end_date, period_type, fiscal_year,
                fiscal_quarter, source, source_ref, reported_currency,
                is_restated, is_derived, normalization_status,
                treasury_shares, filed_date, superseded_at
            ) VALUES (
                %s, '2026-03-31', 'Q1', 2026, 1, 'sec_xbrl', 'TEST',
                'USD', FALSE, FALSE, 'normalized',
                1425422477, '2026-04-15 00:00:00+00', NULL
            )
            """,
            (841_300,),
        )
        conn.commit()

        summary = sync_treasury(conn)
        conn.commit()

        assert summary.observations_recorded == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT treasury_shares FROM ownership_treasury_current WHERE instrument_id = %s",
                (841_300,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["treasury_shares"] == Decimal("1425422477")


# ---------------------------------------------------------------------------
# DEF 14A sync
# ---------------------------------------------------------------------------


class TestSyncDef14a:
    def test_mirrors_def14a_holdings_into_observations(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_400, symbol="AAPL")
        accession = "ACC-PROXY-2026"
        conn.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                accession_number, issuer_cik, holder_name, holder_role,
                shares, percent_of_class, as_of_date, instrument_id
            ) VALUES (
                %s, '0000320193', 'Tim Cook', 'CEO',
                3300000, 0.02, '2025-12-31', %s
            )
            """,
            (accession, 841_400),
        )
        conn.commit()

        summary = sync_def14a(conn)
        conn.commit()

        assert summary.observations_recorded == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT holder_name, shares FROM ownership_def14a_current
                WHERE instrument_id = %s
                """,
                (841_400,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["holder_name"] == "Tim Cook"
        assert rows[0]["shares"] == Decimal("3300000")


# ---------------------------------------------------------------------------
# Idempotency — re-run is no-op for observations counts
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_re_run_does_not_duplicate_observations(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Each sync_* uses ON CONFLICT DO UPDATE on the natural key.
        Re-running on the same legacy data should leave the
        observations row count unchanged."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_500, symbol="JPM")
        conn.execute(
            """
            INSERT INTO financial_periods (
                instrument_id, period_end_date, period_type, fiscal_year,
                fiscal_quarter, source, source_ref, reported_currency,
                is_restated, is_derived, normalization_status,
                treasury_shares, filed_date, superseded_at
            ) VALUES (
                %s, '2026-03-31', 'Q1', 2026, 1, 'sec_xbrl', 'TEST',
                'USD', FALSE, FALSE, 'normalized',
                1425422477, '2026-04-15 00:00:00+00', NULL
            )
            """,
            (841_500,),
        )
        conn.commit()

        sync_treasury(conn)
        conn.commit()
        sync_treasury(conn)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ownership_treasury_observations WHERE instrument_id = %s",
                (841_500,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
