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

import time
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


# ---------------------------------------------------------------------------
# Task 4.4 — edge-case fixtures
#
# Each test below pins one of the discovery layer's risk-mitigation
# clauses from spec §3.1 (joint filings + filing-agent defence;
# share-class siblings; pagination boundary; re-discovery idempotency;
# issuer-only defensive skip).
# ---------------------------------------------------------------------------


def _install_single_page_provider(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, object],
    calls: list[dict[str, Any]],
) -> None:
    """Monkeypatch ``SecFilingsProvider.fetch_search_index_json`` to
    return ``payload`` on the first call and an empty page on every
    subsequent call. Records call args into ``calls`` for assertions."""

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
        if from_offset == 0:
            return payload
        return {"hits": {"hits": []}}

    monkeypatch.setattr(SecFilingsProvider, "fetch_search_index_json", _fake_fetch)


class TestDiscoveryEdgeCases:
    """Spec §3.1 risks-table coverage + §3.4 test-impact bullets."""

    def test_joint_filing_excludes_agent_cik(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One efts hit with ciks=[issuer, Donnelley agent, filer1, filer2]:

        * manifest ``cik`` MUST be ``filer1`` (first non-issuer
          non-agent CIK — the archive-owner-CIK derivation).
        * ``blockholder_filers`` seeded for ``filer1`` + ``filer2``
          ONLY; the agent CIK 0001193125 (Donnelley) must NOT be
          seeded (filing agents are infrastructure, not reporters).
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=8201, symbol="EDG")
        _seed_primary_cik(conn, instrument_id=8201, cik="0000888200")
        conn.commit()

        accession = "0001193125-25-036431"
        filer1 = "0001067983"
        filer2 = "0001767470"
        agent = "0001193125"  # Donnelley R.R. & Sons — in KNOWN_FILING_AGENT_CIKS
        payload: dict[str, object] = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "adsh": accession,
                            "form": "SC 13D",
                            "file_date": "2026-04-20",
                            "ciks": ["0000888200", agent, filer1, filer2],
                            "display_names": ["EDG Test Co.", "Donnelley", "Berkshire", "Cohen"],
                        }
                    }
                ]
            }
        }
        _install_single_page_provider(monkeypatch, payload, calls=[])

        result = discover_sec_13dg_for_universe(conn, mode="bootstrap")
        conn.commit()

        # Manifest cik is the first non-issuer non-agent CIK (filer1).
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cik, subject_id FROM sec_filing_manifest WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == filer1
        assert row[1] == filer1

        # Auto-seeded filers = filer1 + filer2 ONLY (no agent, no issuer).
        with conn.cursor() as cur:
            cur.execute("SELECT cik FROM blockholder_filers ORDER BY cik")
            filers = [r[0] for r in cur.fetchall()]
        assert filers == sorted([filer1, filer2])
        assert agent not in filers
        assert "0000888200" not in filers

        # Result counters: 2 filers seeded for this single hit.
        assert result.filers_upserted == 2
        assert result.manifest_rows_inserted == 1
        assert result.hints_written == 1

    def test_share_class_siblings_one_manifest_two_hints(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two instruments on a shared issuer CIK (GOOG + GOOGL on
        1652044). One efts call returns one accession; the discovery
        layer writes ONE manifest row + TWO hint rows (one per
        sibling)."""
        conn = ebull_test_conn
        issuer_cik = "0001652044"
        _seed_instrument(conn, iid=8301, symbol="GOOG")
        _seed_primary_cik(conn, instrument_id=8301, cik=issuer_cik)
        _seed_instrument(conn, iid=8302, symbol="GOOGL")
        _seed_primary_cik(conn, instrument_id=8302, cik=issuer_cik)
        conn.commit()

        accession = "0001234567-25-000888"
        filer_cik = "0001234567"
        payload: dict[str, object] = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "adsh": accession,
                            "form": "SC 13G/A",
                            "file_date": "2026-03-10",
                            "ciks": [issuer_cik, filer_cik],
                            "display_names": ["Alphabet Inc.", "Test Filer"],
                        }
                    }
                ]
            }
        }
        calls: list[dict[str, Any]] = []
        _install_single_page_provider(monkeypatch, payload, calls)

        result = discover_sec_13dg_for_universe(conn, mode="bootstrap")
        conn.commit()

        # Exactly ONE efts call for the issuer CIK (siblings collapse).
        assert len(calls) == 1
        assert calls[0]["ciks"] == issuer_cik

        # ONE manifest row keyed on the accession.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number = %s",
                (accession,),
            )
            row = cur.fetchone()
        assert row is not None and row[0] == 1

        # TWO hint rows — one per sibling instrument.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id
                FROM sec_13dg_discovery_issuer_hint
                WHERE accession_number = %s
                ORDER BY instrument_id
                """,
                (accession,),
            )
            hint_instruments = [r[0] for r in cur.fetchall()]
        assert hint_instruments == [8301, 8302]

        # Counters reflect the 1-manifest, 2-hint fan-out.
        assert result.issuers_scanned == 1
        assert result.accessions_discovered == 1
        assert result.manifest_rows_inserted == 1
        # The second sibling sees the manifest row already present (the
        # first sibling's pre-check + UPSERT) → bumps the "skipped
        # existing" counter exactly once.
        assert result.manifest_rows_skipped_existing == 1
        assert result.hints_written == 2

    def test_pagination_boundary_at_exactly_100(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Page 1 returns exactly 100 hits (the SEC page-size ceiling);
        the loop MUST request page 2 (offset=100). Page 2 returns 0
        hits → loop terminates. Total: 2 efts calls,
        ``accessions_discovered == 100``."""
        conn = ebull_test_conn
        issuer_cik = "0000777777"
        _seed_instrument(conn, iid=8401, symbol="PAG")
        _seed_primary_cik(conn, instrument_id=8401, cik=issuer_cik)
        conn.commit()

        filer_cik = "0001234567"
        # 100 distinct accessions so the PK doesn't collide.
        hits = [
            {
                "_source": {
                    "adsh": f"0001234567-25-{i:06d}",
                    "form": "SC 13G",
                    "file_date": "2026-04-15",
                    "ciks": [issuer_cik, filer_cik],
                    "display_names": ["Pag Test Co.", "Test Filer"],
                }
            }
            for i in range(100)
        ]
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
            calls.append({"from_offset": from_offset})
            if from_offset == 0:
                return {"hits": {"hits": hits}}
            # Page 2: empty list → terminates the loop.
            return {"hits": {"hits": []}}

        monkeypatch.setattr(SecFilingsProvider, "fetch_search_index_json", _fake_fetch)

        result = discover_sec_13dg_for_universe(conn, mode="bootstrap")
        conn.commit()

        # Two pages: offset=0 (full page), offset=100 (empty → terminate).
        assert [c["from_offset"] for c in calls] == [0, 100]
        assert result.accessions_discovered == 100
        assert result.manifest_rows_inserted == 100
        assert result.hints_written == 100

    def test_re_discovery_idempotent_with_discovered_at_advancement(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run the same discovery sweep twice. The second run MUST:

        * write 0 new manifest rows (``manifest_rows_inserted == 0``;
          ``manifest_rows_skipped_existing == 1``).
        * write 0 new hint rows (``hints_written == 0``).
        * advance ``sec_13dg_discovery_issuer_hint.discovered_at``
          on the existing hint row — pinning the hint-UPSERT-with-
          ``DO UPDATE SET discovered_at = NOW()`` contract from
          ``sql/159``.
        """
        conn = ebull_test_conn
        issuer_cik = "0000666666"
        _seed_instrument(conn, iid=8501, symbol="IDP")
        _seed_primary_cik(conn, instrument_id=8501, cik=issuer_cik)
        conn.commit()

        accession = "0001234567-25-000500"
        payload: dict[str, object] = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "adsh": accession,
                            "form": "SC 13G",
                            "file_date": "2026-04-15",
                            "ciks": [issuer_cik, "0001234567"],
                            "display_names": ["IDP Test Co.", "Test Filer"],
                        }
                    }
                ]
            }
        }
        _install_single_page_provider(monkeypatch, payload, calls=[])

        first = discover_sec_13dg_for_universe(conn, mode="bootstrap")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT discovered_at
                FROM sec_13dg_discovery_issuer_hint
                WHERE accession_number = %s AND instrument_id = %s
                """,
                (accession, 8501),
            )
            row = cur.fetchone()
        assert row is not None
        first_discovered_at = row[0]
        assert first.manifest_rows_inserted == 1
        assert first.hints_written == 1

        # Sleep a tiny moment so NOW() advances measurably between runs
        # (Postgres TIMESTAMPTZ is microsecond-resolution; on fast hardware
        # the second run can complete inside the same microsecond).
        time.sleep(0.01)

        second = discover_sec_13dg_for_universe(conn, mode="bootstrap")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT discovered_at
                FROM sec_13dg_discovery_issuer_hint
                WHERE accession_number = %s AND instrument_id = %s
                """,
                (accession, 8501),
            )
            row = cur.fetchone()
        assert row is not None
        second_discovered_at = row[0]

        # Counters: second pass writes nothing net-new.
        assert second.manifest_rows_inserted == 0
        assert second.manifest_rows_skipped_existing == 1
        assert second.hints_written == 0

        # discovered_at strictly advanced under the UPSERT-refresh contract.
        assert second_discovered_at > first_discovered_at

    def test_issuer_only_result_skips_with_warn(
        self,
        ebull_test_conn: psycopg.Connection[Any],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An efts hit whose ``ciks[]`` contains ONLY the issuer CIK
        (no non-issuer non-agent CIK extractable) MUST short-circuit
        without writing a manifest row, a hint row, or a blockholder
        filer row. A warning log line records the anomaly."""
        conn = ebull_test_conn
        issuer_cik = "0000555555"
        _seed_instrument(conn, iid=8601, symbol="ISO")
        _seed_primary_cik(conn, instrument_id=8601, cik=issuer_cik)
        conn.commit()

        accession = "0000555555-25-000099"
        payload: dict[str, object] = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "adsh": accession,
                            "form": "SC 13G",
                            "file_date": "2026-04-15",
                            "ciks": [issuer_cik],  # issuer ONLY
                            "display_names": ["ISO Test Co."],
                        }
                    }
                ]
            }
        }
        _install_single_page_provider(monkeypatch, payload, calls=[])

        with caplog.at_level("WARNING", logger="app.services.sec_13dg_discovery"):
            result = discover_sec_13dg_for_universe(conn, mode="bootstrap")
            conn.commit()

        # Nothing written.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number = %s",
                (accession,),
            )
            manifest_count = (cur.fetchone() or (0,))[0]
            cur.execute(
                "SELECT COUNT(*) FROM sec_13dg_discovery_issuer_hint WHERE accession_number = %s",
                (accession,),
            )
            hint_count = (cur.fetchone() or (0,))[0]
            cur.execute("SELECT COUNT(*) FROM blockholder_filers")
            filer_count = (cur.fetchone() or (0,))[0]
        assert manifest_count == 0
        assert hint_count == 0
        assert filer_count == 0

        # Counters: hit was returned by efts (accessions_discovered=1)
        # but every write counter stays at 0.
        assert result.accessions_discovered == 1
        assert result.manifest_rows_inserted == 0
        assert result.manifest_rows_skipped_existing == 0
        assert result.hints_written == 0
        assert result.filers_upserted == 0

        # Warning emitted with the accession + issuer CIK for operator audit.
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any(accession in r.getMessage() and issuer_cik in r.getMessage() for r in warnings)
