"""Integration tests for ``app/services/sec_13dg_discovery.py`` (#1233 PR11).

The discovery layer is a thin HTTP + SELECT + INSERT walker; every
write goes through a real Postgres connection (``ebull_test_conn``)
because the load-bearing logic is multi-row idempotency + atomicity
across ``sec_filing_manifest`` + ``sec_13dg_discovery_issuer_hint`` +
``blockholder_filers``. Mocking psycopg would erase the value of these
tests.

External boundaries:
  * SEC HTTP — substituted by monkeypatching
    ``SecFilingsProvider.fetch_search_index_json`` so tests are
    deterministic and offline.
  * ``blockholder_filers`` / ``blockholder_filings`` — seeded directly
    via SQL (the canonical schema lives in
    ``sql/095_blockholder_filers_filings.sql``).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg
import pytest

from app.services.blockholders import blockholders_retention_cutoff
from app.services.sec_13dg_discovery import _resolve_discovery_startdt

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# DB seeding helpers (mirror the canonical patterns from
# ``tests/test_blockholders_ingester.py`` + ``tests/test_ownership_observations_sync.py``).
# ---------------------------------------------------------------------------


def _seed_instrument(
    conn: psycopg.Connection[Any],
    *,
    iid: int,
    symbol: str,
    is_tradable: bool = True,
    country: str = "US",
) -> None:
    """Seed an ``instruments`` row.

    ``company_name`` is NOT NULL per ``sql/001_init.sql:1-10``; tests
    that omit it tombstone the seeded fixture (Codex 1b MEDIUM lesson
    from the plan v3).
    """
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency,
            country, is_tradable
        )
        VALUES (%s, %s, %s, '4', 'USD', %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Test Co.", country, is_tradable),
    )


def _seed_primary_cik(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    cik: str,
) -> None:
    """Seed the primary ``external_identifiers`` row used by the
    discovery universe SELECT (provider='sec', identifier_type='cik',
    is_primary=TRUE)."""
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        )
        VALUES (%s, 'sec', 'cik', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (instrument_id, cik),
    )


def _seed_filer_and_filing(
    conn: psycopg.Connection[Any],
    *,
    filer_cik: str,
    accession_number: str,
    issuer_cik: str,
    filed_at: datetime,
    issuer_cusip: str = "999999999",
) -> None:
    """Seed a minimal ``blockholder_filers`` + ``blockholder_filings``
    pair used to back-fill the watermark for the steady-state branch
    of ``_resolve_discovery_startdt``."""
    conn.execute(
        """
        INSERT INTO blockholder_filers (cik, name)
        VALUES (%s, %s)
        ON CONFLICT (cik) DO NOTHING
        """,
        (filer_cik, f"Filer {filer_cik}"),
    )
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            issuer_cik, issuer_cusip,
            reporter_cik, reporter_no_cik, reporter_name,
            aggregate_amount_owned, filed_at
        )
        SELECT filer_id, %s, 'SCHEDULE 13G', 'passive',
               %s, %s,
               %s, FALSE, %s,
               1000000, %s
        FROM blockholder_filers WHERE cik = %s
        """,
        (
            accession_number,
            issuer_cik,
            issuer_cusip,
            filer_cik,
            f"Reporter {filer_cik}",
            filed_at,
            filer_cik,
        ),
    )


# ---------------------------------------------------------------------------
# Task 4.2 — _resolve_discovery_startdt watermark helper
# ---------------------------------------------------------------------------


class TestResolveDiscoveryStartdt:
    """Spec §3.5: ``MAX(blockholder_filings.filed_at) WHERE issuer_cik = ?``,
    clamped to ``blockholders_retention_cutoff()``. ``mode='bootstrap'``
    always returns the floor regardless of any watermark."""

    def test_bootstrap_mode_returns_floor(
        self,
        ebull_test_conn: psycopg.Connection[Any],
    ) -> None:
        """Bootstrap dispatch always scans the full 3y window (floored
        by the 2024-12-18 XML-mandate); per-issuer watermark must NOT
        narrow the window even when the chain already has rows."""
        conn = ebull_test_conn
        _seed_filer_and_filing(
            conn,
            filer_cik="0001000001",
            accession_number="0001000001-25-000001",
            issuer_cik="0000320193",
            filed_at=datetime(2026, 4, 1, tzinfo=UTC),
        )
        conn.commit()

        startdt = _resolve_discovery_startdt(conn, mode="bootstrap", issuer_cik="0000320193")

        assert startdt == blockholders_retention_cutoff()
        assert isinstance(startdt, date)

    def test_steady_state_with_no_prior_ingest_returns_floor(
        self,
        ebull_test_conn: psycopg.Connection[Any],
    ) -> None:
        """Steady-state on a fresh issuer (no ``blockholder_filings``
        rows for this issuer_cik) MUST clamp to the floor — narrowing
        on a missing watermark would silently shrink coverage."""
        conn = ebull_test_conn
        conn.commit()

        startdt = _resolve_discovery_startdt(conn, mode="steady_state", issuer_cik="0000320193")

        assert startdt == blockholders_retention_cutoff()

    def test_steady_state_with_watermark_uses_max_filed_at_minus_7d(
        self,
        ebull_test_conn: psycopg.Connection[Any],
    ) -> None:
        """Steady-state with a prior ingest narrows to
        ``MAX(filed_at) - 7d`` per spec §3.5 (7d safety overlap) but
        still clamps to the floor as the absolute lower bound."""
        conn = ebull_test_conn
        floor = blockholders_retention_cutoff()
        # Watermark is comfortably AFTER the floor so the helper returns
        # ``watermark - 7d`` rather than the floor.
        watermark = datetime(floor.year + 1, floor.month, min(floor.day, 28), tzinfo=UTC)
        _seed_filer_and_filing(
            conn,
            filer_cik="0001000002",
            accession_number="0001000002-25-000001",
            issuer_cik="0000320193",
            filed_at=watermark,
        )
        # Also seed an older row + a row for a DIFFERENT issuer to prove
        # the WHERE issuer_cik filter narrows correctly.
        _seed_filer_and_filing(
            conn,
            filer_cik="0001000003",
            accession_number="0001000003-24-000001",
            issuer_cik="0000320193",
            filed_at=watermark - timedelta(days=30),
        )
        _seed_filer_and_filing(
            conn,
            filer_cik="0001000004",
            accession_number="0001000004-25-000001",
            issuer_cik="0000999999",  # different issuer; must NOT affect AAPL
            filed_at=watermark + timedelta(days=180),
        )
        conn.commit()

        startdt = _resolve_discovery_startdt(conn, mode="steady_state", issuer_cik="0000320193")

        expected = max(floor, watermark.date() - timedelta(days=7))
        assert startdt == expected
