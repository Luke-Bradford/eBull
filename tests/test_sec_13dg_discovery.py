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

from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.blockholders import blockholders_retention_cutoff
from app.services.sec_13dg_discovery import (
    DiscoveryResult,
    _extract_filer_set,
    _resolve_discovery_startdt,
    discover_sec_13dg_for_universe,
)

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


# ---------------------------------------------------------------------------
# Task 4.3 — extraction + atomic per-accession ingest helpers + entry point
# ---------------------------------------------------------------------------


class TestExtractFilerSet:
    """Pure-function tests for ``_extract_filer_set`` (no DB)."""

    def test_drops_issuer_and_known_agent_padded_and_unpadded(self) -> None:
        """Issuer CIK matched on the unpadded form; agent CIK matched
        on the padded form; both should be dropped regardless of how
        they appear in the raw efts ``ciks[]`` list."""
        # Issuer "320193" (unpadded), agent "1193125" (unpadded for Donnelley),
        # genuine filer "1067983" (Berkshire-style), duplicate of the same
        # filer to test dedupe.
        result = _extract_filer_set(
            cik_list=["320193", "1193125", "1067983", "0001067983"],
            name_list=["AAPL Inc.", "Donnelley", "Berkshire Hathaway", "Berkshire dup"],
            issuer_cik="0000320193",
        )
        assert result == [("0001067983", "Berkshire Hathaway")]

    def test_aligns_name_by_position_falls_back_to_synthetic(self) -> None:
        """Each filer CIK maps to ``name_list`` at its own positional
        index; missing / empty names fall back to ``"CIK <padded>"``."""
        result = _extract_filer_set(
            cik_list=["0001234567", "0007654321"],
            name_list=["Real Filer", ""],
            issuer_cik="0000999999",
        )
        assert result == [
            ("0001234567", "Real Filer"),
            ("0007654321", "CIK 0007654321"),
        ]


class TestDiscoverSec13dgForUniverse:
    """End-to-end discovery: universe SELECT → efts → manifest + hint
    writes. SEC HTTP is monkeypatched at the class method level so
    tests run offline + deterministically."""

    def test_happy_path_single_issuer_single_filer(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One US tradable issuer with one primary CIK; one efts hit
        with one non-issuer non-agent filer. Expect: 1 manifest row,
        1 hint row, 1 ``DiscoveryResult.manifest_rows_inserted``,
        1 ``hints_written``, 1 ``filers_upserted``."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=8101, symbol="GMEX")
        _seed_primary_cik(conn, instrument_id=8101, cik="0000999100")
        conn.commit()

        accession = "0001234567-25-000010"
        filer_cik = "0001234567"
        fake_payload: dict[str, object] = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "adsh": accession,
                            "form": "SC 13G",
                            "file_date": "2026-04-15",
                            "ciks": ["0000999100", filer_cik],
                            "display_names": ["GMEX Test Co.", "Test Filer"],
                        }
                    }
                ]
            }
        }
        calls: list[dict[str, Any]] = []

        def _fake_fetch(
            self: SecFilingsProvider,
            *,
            ciks: str,
            forms: tuple[str, ...],
            startdt: date,
            enddt: date,
            from_offset: int = 0,
            size: int = 100,
        ) -> dict[str, object] | None:
            calls.append(
                {
                    "ciks": ciks,
                    "forms": forms,
                    "startdt": startdt,
                    "enddt": enddt,
                    "from_offset": from_offset,
                    "size": size,
                }
            )
            # Return the payload on the first call, then empty on the
            # next page to terminate the loop cleanly.
            if from_offset == 0:
                return fake_payload
            return {"hits": {"hits": []}}

        monkeypatch.setattr(SecFilingsProvider, "fetch_search_index_json", _fake_fetch)

        result = discover_sec_13dg_for_universe(conn, mode="bootstrap")
        conn.commit()

        # DiscoveryResult counters
        assert isinstance(result, DiscoveryResult)
        assert result.issuers_scanned == 1
        assert result.accessions_discovered == 1
        assert result.manifest_rows_inserted == 1
        assert result.manifest_rows_skipped_existing == 0
        assert result.hints_written == 1
        assert result.filers_upserted == 1
        assert result.rows_skipped_outside_cap == 0
        assert result.elapsed_seconds >= 0.0

        # Manifest row present with the right shape
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT subject_type, cik, instrument_id, source, form
                FROM sec_filing_manifest
                WHERE accession_number = %s
                """,
                (accession,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "blockholder_filer"
        assert row[1] == filer_cik
        assert row[2] is None  # CHECK constraint: blockholder_filer → NULL
        assert row[3] == "sec_13g"
        assert row[4] == "SC 13G"

        # Hint row present
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id, issuer_cik
                FROM sec_13dg_discovery_issuer_hint
                WHERE accession_number = %s
                """,
                (accession,),
            )
            hint_rows = cur.fetchall()
        assert hint_rows == [(8101, "0000999100")]

        # blockholder_filers seeded for the genuine filer ONLY
        # (issuer + any agent must NOT be auto-seeded).
        with conn.cursor() as cur:
            cur.execute("SELECT cik FROM blockholder_filers ORDER BY cik")
            filers = [r[0] for r in cur.fetchall()]
        assert filers == [filer_cik]

        # Provider called once. The loop terminates after the first page
        # because ``len(hits) == 1 < _PAGE_SIZE`` (efts contract: a
        # short page implies no more results).
        assert len(calls) == 1
        assert calls[0]["ciks"] == "0000999100"
        assert calls[0]["forms"] == ("SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A")
        assert calls[0]["startdt"] == blockholders_retention_cutoff()
        assert calls[0]["from_offset"] == 0
