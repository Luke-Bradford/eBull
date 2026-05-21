"""PR11 #1233 §3.2 chokepoint C — ``sync_blockholders`` retention gate.

These tests pin the 3y retention cap on the observations sync writer:

  * Pre-cap (``bf.filed_at < blockholders_retention_cutoff()``) rows
    MUST NOT mirror into ``ownership_blockholders_observations`` /
    ``ownership_blockholders_current``. Without the gate a steady-state
    sync re-run would repopulate pre-cap rows from any
    ``blockholder_filings`` rows still present in the dev DB
    pre-pre-wipe.

  * The gate uses ``bf.filed_at`` directly (NOT a
    ``LEFT JOIN filing_events ... WHERE fe.filing_date >= cutoff``
    predicate). A LEFT JOIN with that predicate null-rejects rows
    missing a ``filing_events`` entry — the Codex 1a HIGH #4 /
    Codex 1b PR10b lesson for this category of cap.
    ``blockholder_filings.filed_at`` is the source of truth at the raw
    layer (``sql/095_blockholder_filers_filings.sql:124``).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import psycopg.rows
import pytest

from app.services.blockholders import blockholders_retention_cutoff
from app.services.ownership_observations_sync import sync_blockholders
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers (mirror canonical idioms from
# ``tests/test_sec_13dg_discovery.py`` + ``tests/test_ownership_observations_sync.py``).
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[Any], *, iid: int, symbol: str) -> None:
    """Seed an ``instruments`` row. ``company_name`` is NOT NULL per
    ``sql/001_init.sql:1-10`` — omitting it tombstones the fixture."""
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable, country)
        VALUES (%s, %s, %s, '4', 'USD', TRUE, 'US')
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_filer(conn: psycopg.Connection[Any], *, cik: str, name: str) -> None:
    conn.execute(
        """
        INSERT INTO blockholder_filers (cik, name)
        VALUES (%s, %s)
        ON CONFLICT (cik) DO NOTHING
        """,
        (cik, name),
    )


def _seed_blockholder_filing(
    conn: psycopg.Connection[Any],
    *,
    filer_cik: str,
    accession_number: str,
    instrument_id: int,
    filed_at: datetime,
    issuer_cik: str = "0000000789",
    submission_type: str = "SCHEDULE 13D",
    status: str = "active",
    aggregate_amount_owned: int = 1_000_000,
) -> None:
    """Seed one ``blockholder_filings`` row keyed off the primary
    filer's CIK. The submission_type/status pair must satisfy the
    cross-column CHECK from ``sql/095_blockholder_filers_filings.sql``:
    13D/A → active, 13G/A → passive."""
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip,
            reporter_cik, reporter_no_cik, reporter_name,
            aggregate_amount_owned, filed_at
        )
        SELECT filer_id, %s, %s, %s, %s,
               %s, '999999999',
               %s, FALSE, 'Test Reporter',
               %s, %s
        FROM blockholder_filers WHERE cik = %s
        """,
        (
            accession_number,
            submission_type,
            status,
            instrument_id,
            issuer_cik,
            filer_cik,
            aggregate_amount_owned,
            filed_at,
            filer_cik,
        ),
    )


# ---------------------------------------------------------------------------
# Chokepoint C — retention gate tests
# ---------------------------------------------------------------------------


class TestSyncBlockholdersRetentionCap:
    """PR11 spec §3.2 chokepoint C: ``WHERE bf.filed_at >=
    blockholders_retention_cutoff()`` keeps the steady-state sync from
    repopulating pre-cap rows."""

    def test_sync_excludes_pre_cap_filings(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811 — pytest fixture shadow
    ) -> None:
        """Two filings under one primary filer: one filed BEFORE the
        cutoff, one AFTER. Only the post-cap accession produces an
        observation row."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_911_001, symbol="CAPA")
        _seed_filer(conn, cik="0000123450", name="Test Primary Filer")

        cutoff = blockholders_retention_cutoff()
        # Cutoff floor today (any date before 2027-12-18) is the
        # XML-mandate date 2024-12-18. ±30d brackets it cleanly.
        cutoff_midnight = datetime.combine(cutoff, datetime.min.time(), tzinfo=UTC)
        pre_cap_dt = cutoff_midnight - timedelta(days=30)
        post_cap_dt = cutoff_midnight + timedelta(days=30)

        pre_accn = "0000123450-24-000010"
        post_accn = "0000123450-25-000020"
        _seed_blockholder_filing(
            conn,
            filer_cik="0000123450",
            accession_number=pre_accn,
            instrument_id=841_911_001,
            filed_at=pre_cap_dt,
        )
        _seed_blockholder_filing(
            conn,
            filer_cik="0000123450",
            accession_number=post_accn,
            instrument_id=841_911_001,
            filed_at=post_cap_dt,
        )
        conn.commit()

        summary = sync_blockholders(conn)
        conn.commit()

        # Only the post-cap accession is scanned + recorded.
        assert summary.rows_scanned == 1
        assert summary.observations_recorded == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source_accession
                FROM ownership_blockholders_observations
                WHERE instrument_id = %s
                ORDER BY source_accession
                """,
                (841_911_001,),
            )
            accns = [r["source_accession"] for r in cur.fetchall()]
        assert post_accn in accns, f"post-cap accession missing: {accns}"
        assert pre_accn not in accns, f"pre-cap accession leaked through gate: {accns}"

    def test_sync_includes_rows_without_filing_events_entry(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811 — pytest fixture shadow
    ) -> None:
        """A post-cap ``blockholder_filings`` row with NO matching
        ``filing_events`` entry must STILL mirror into observations.
        Pins the Codex 1a HIGH #4 lesson: gating via a LEFT JOIN +
        ``WHERE fe.filing_date >= cutoff`` would null-reject this row
        and silently drop it. The gate is on ``bf.filed_at`` directly,
        so no join → no NULL-reject."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=841_911_002, symbol="CAPB")
        _seed_filer(conn, cik="0000123451", name="Test Primary Filer B")

        cutoff = blockholders_retention_cutoff()
        post_cap_dt = datetime.combine(cutoff, datetime.min.time(), tzinfo=UTC) + timedelta(days=45)

        accn = "0000123451-25-000030"
        _seed_blockholder_filing(
            conn,
            filer_cik="0000123451",
            accession_number=accn,
            instrument_id=841_911_002,
            filed_at=post_cap_dt,
        )
        # Deliberately do NOT insert into filing_events: the gate must
        # not rely on a join to that table.
        conn.commit()

        summary = sync_blockholders(conn)
        conn.commit()

        assert summary.rows_scanned == 1
        assert summary.observations_recorded == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source_accession
                FROM ownership_blockholders_observations
                WHERE instrument_id = %s
                """,
                (841_911_002,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1, f"row without filing_events was dropped: {rows}"
        assert rows[0]["source_accession"] == accn
