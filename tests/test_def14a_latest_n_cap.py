"""DEF 14A latest-2-primary-proxies-per-filer cap — #1233 §4.7 PR5.

Pins the contracts:

1. ``DEF14A_LATEST_PER_FILER_CAP == 2`` and
   ``DEF14A_PRIMARY_FORM_TYPE == 'DEF 14A'`` are the single
   source of truth.
2. ``def14a_within_cap`` helper:
   - True for non-primary form types (DEFA14A / DEFR14A / DEFM14A).
   - True for primary DEF 14A within rank ≤ 2 per issuer CIK.
   - True for primary DEF 14A when issuer CIK is missing
     (CIK-MISSING fast-tombstone preserved).
   - False for primary DEF 14A beyond rank 2.
   - False for accession absent from ``filing_events``
     (out-of-corpus safe default).
3. Discovery (``discover_pending_def14a``):
   - Universe-wide cap latest-2 primary per CIK; supplements pass.
   - Per-instrument cap respects the calling instrument's CIK
     (sibling-aware via ``_resolve_target_cik_for_cap``).
   - Rank computed across ALL accessions (including already-logged
     ones) so a 3rd un-attempted accession is not promoted to
     rank-1 once the top-2 are logged.
   - URL-NULL accessions remain part of the ranking set (rank-1/2
     with NULL URLs do not promote rank-3 with a URL).
4. Parser pre-fetch gate (``_parse_def14a``): rank>2 accession is
   tombstoned BEFORE the SEC HTTP call.
5. Rewash rescue gate (``_apply_def14a``): happy-path uncapped;
   rescue path refuses out-of-cap accessions.

Existing rows are not deleted by the cap (#1233 §6.3 is the only
purge event). These tests assert *insert* behaviour only.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import psycopg
import pytest

from app.services.def14a_ingest import (
    DEF14A_LATEST_PER_FILER_CAP,
    DEF14A_PRIMARY_FORM_TYPE,
    _resolve_target_cik_for_cap,
    def14a_within_cap,
    discover_pending_def14a,
)
from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# ---------------------------------------------------------------------------
# Pure-constant contracts
# ---------------------------------------------------------------------------


class TestConstants:
    def test_cap_is_2(self) -> None:
        assert DEF14A_LATEST_PER_FILER_CAP == 2

    def test_primary_form_is_def_14a(self) -> None:
        assert DEF14A_PRIMARY_FORM_TYPE == "DEF 14A"


# ---------------------------------------------------------------------------
# Integration tests need the dev DB
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seeders
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        )
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_profile(conn: psycopg.Connection[tuple], *, instrument_id: int, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik)
        VALUES (%s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET cik = EXCLUDED.cik
        """,
        (instrument_id, cik),
    )


def _seed_filing_event(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    filing_date: date,
    filing_type: str = "DEF 14A",
    primary_document_url: str | None = "https://example.test/proxy.htm",
) -> None:
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id, primary_document_url
        ) VALUES (%s, %s, %s, 'sec', %s, %s)
        ON CONFLICT (provider, provider_filing_id, instrument_id) DO NOTHING
        """,
        (instrument_id, filing_date, filing_type, accession, primary_document_url),
    )


def _log_ingest_attempt(conn: psycopg.Connection[tuple], *, accession: str, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO def14a_ingest_log (
            accession_number, issuer_cik, status, rows_inserted, rows_skipped
        ) VALUES (%s, %s, 'success', 0, 0)
        ON CONFLICT (accession_number) DO NOTHING
        """,
        (accession, cik),
    )


# ---------------------------------------------------------------------------
# Helper unit tests (§6.1)
# ---------------------------------------------------------------------------


class TestDef14aWithinCap:
    def test_top_two_primary_pass_bottom_three_fail(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 778_001
        _seed_instrument(conn, iid=iid, symbol="CAP")
        _seed_profile(conn, instrument_id=iid, cik="0000778001")
        for i, fdate in enumerate(
            [date(2022, 3, 1), date(2023, 3, 1), date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]
        ):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=f"0000778001-25-{i:06d}",
                filing_date=fdate,
            )
        conn.commit()

        # Latest two (2026, 2025) pass; 2024/2023/2022 fail.
        assert def14a_within_cap(conn, accession_number="0000778001-25-000004", instrument_id=iid)
        assert def14a_within_cap(conn, accession_number="0000778001-25-000003", instrument_id=iid)
        assert not def14a_within_cap(conn, accession_number="0000778001-25-000002", instrument_id=iid)
        assert not def14a_within_cap(conn, accession_number="0000778001-25-000001", instrument_id=iid)
        assert not def14a_within_cap(conn, accession_number="0000778001-25-000000", instrument_id=iid)

    def test_supplemental_forms_uncapped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """DEFA14A / DEFR14A / DEFM14A bypass the cap entirely."""
        conn = ebull_test_conn
        iid = 778_002
        _seed_instrument(conn, iid=iid, symbol="SUP")
        _seed_profile(conn, instrument_id=iid, cik="0000778002")
        # 5 DEFA14A accessions for one filer — all should pass.
        for i, fdate in enumerate(
            [date(2022, 3, 1), date(2023, 3, 1), date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]
        ):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=f"0000778002-25-{i:06d}",
                filing_date=fdate,
                filing_type="DEFA14A",
            )
        conn.commit()

        for i in range(5):
            assert def14a_within_cap(
                conn,
                accession_number=f"0000778002-25-{i:06d}",
                instrument_id=iid,
            )

    def test_mixed_primary_and_supplemental(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """3 DEF 14A + 2 DEFA14A: top-2 primary pass; oldest primary
        fails; both DEFA14As pass unconditionally."""
        conn = ebull_test_conn
        iid = 778_003
        _seed_instrument(conn, iid=iid, symbol="MIX")
        _seed_profile(conn, instrument_id=iid, cik="0000778003")
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="P-OLD",
            filing_date=date(2024, 3, 1),
            filing_type="DEF 14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="P-MID",
            filing_date=date(2025, 3, 1),
            filing_type="DEF 14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="P-NEW",
            filing_date=date(2026, 3, 1),
            filing_type="DEF 14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="A-2024",
            filing_date=date(2024, 4, 1),
            filing_type="DEFA14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="A-2025",
            filing_date=date(2025, 4, 1),
            filing_type="DEFA14A",
        )
        conn.commit()

        # Primaries: top-2 pass, oldest fails.
        assert def14a_within_cap(conn, accession_number="P-NEW", instrument_id=iid)
        assert def14a_within_cap(conn, accession_number="P-MID", instrument_id=iid)
        assert not def14a_within_cap(conn, accession_number="P-OLD", instrument_id=iid)
        # Supplementals: all pass regardless of rank.
        assert def14a_within_cap(conn, accession_number="A-2024", instrument_id=iid)
        assert def14a_within_cap(conn, accession_number="A-2025", instrument_id=iid)

    def test_tie_break_on_provider_filing_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Same filing_date: higher provider_filing_id ranks higher."""
        conn = ebull_test_conn
        iid = 778_004
        _seed_instrument(conn, iid=iid, symbol="TIE")
        _seed_profile(conn, instrument_id=iid, cik="0000778004")
        for acc in ("AAA", "BBB", "CCC"):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=acc,
                filing_date=date(2026, 3, 1),
            )
        conn.commit()

        # ORDER BY filing_date DESC, provider_filing_id DESC →
        # ranks: CCC=1, BBB=2, AAA=3.
        assert def14a_within_cap(conn, accession_number="CCC", instrument_id=iid)
        assert def14a_within_cap(conn, accession_number="BBB", instrument_id=iid)
        assert not def14a_within_cap(conn, accession_number="AAA", instrument_id=iid)

    def test_cik_missing_passes(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Instrument with no profile (and no profile-bearing
        sibling) bypasses the cap — preserves the legacy
        CIK-MISSING tombstone path."""
        conn = ebull_test_conn
        iid = 778_005
        _seed_instrument(conn, iid=iid, symbol="CKM")
        # NO profile.
        for i in range(5):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=f"CKM-{i:06d}",
                filing_date=date(2024, 1, 1).replace(month=(i % 12) + 1),
            )
        conn.commit()

        for i in range(5):
            assert def14a_within_cap(conn, accession_number=f"CKM-{i:06d}", instrument_id=iid)

    def test_share_class_siblings_rank_by_cik(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """5 accessions on a shared CIK with 2 siblings = 5 distinct
        accessions ranked per CIK (not 10). Both siblings see the
        same top-2."""
        conn = ebull_test_conn
        iid_a, iid_b = 778_006, 778_007
        _seed_instrument(conn, iid=iid_a, symbol="SCA")
        _seed_instrument(conn, iid=iid_b, symbol="SCB")
        _seed_profile(conn, instrument_id=iid_a, cik="0000778050")
        _seed_profile(conn, instrument_id=iid_b, cik="0000778050")
        for i, fdate in enumerate(
            [date(2022, 3, 1), date(2023, 3, 1), date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]
        ):
            for iid in (iid_a, iid_b):
                _seed_filing_event(
                    conn,
                    instrument_id=iid,
                    accession=f"SCL-{i:06d}",
                    filing_date=fdate,
                )
        conn.commit()

        # Top-2 latest accessions: SCL-000004 + SCL-000003.
        for iid in (iid_a, iid_b):
            assert def14a_within_cap(conn, accession_number="SCL-000004", instrument_id=iid)
            assert def14a_within_cap(conn, accession_number="SCL-000003", instrument_id=iid)
            assert not def14a_within_cap(conn, accession_number="SCL-000002", instrument_id=iid)

    def test_sibling_fallback_when_calling_instrument_lacks_profile(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Calling instrument has no profile but sibling does — the
        sibling-via-filing_events fallback at §2.6.0 step 2 resolves
        the CIK and the cap applies."""
        conn = ebull_test_conn
        iid_profile, iid_no_profile = 778_008, 778_009
        _seed_instrument(conn, iid=iid_profile, symbol="SBP")
        _seed_instrument(conn, iid=iid_no_profile, symbol="SBN")
        _seed_profile(conn, instrument_id=iid_profile, cik="0000778100")
        # iid_no_profile has NO profile row.

        # 3 accessions per shared CIK, fanned out to both siblings.
        for i, fdate in enumerate([date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]):
            for iid in (iid_profile, iid_no_profile):
                _seed_filing_event(
                    conn,
                    instrument_id=iid,
                    accession=f"SBL-{i:06d}",
                    filing_date=fdate,
                )
        conn.commit()

        # When called for the profile-less sibling, the helper must
        # still cap. Top-2 (SBL-000002, SBL-000001) pass.
        assert def14a_within_cap(conn, accession_number="SBL-000002", instrument_id=iid_no_profile)
        assert def14a_within_cap(conn, accession_number="SBL-000001", instrument_id=iid_no_profile)
        # Oldest (SBL-000000) fails — cap applied via sibling
        # fallback, not bypassed as CIK-MISSING.
        assert not def14a_within_cap(conn, accession_number="SBL-000000", instrument_id=iid_no_profile)

    def test_out_of_corpus_accession_refuses(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Accession not present in filing_events → False (safe
        default; manifest can't dispatch what we can't rank)."""
        conn = ebull_test_conn
        iid = 778_010
        _seed_instrument(conn, iid=iid, symbol="OOC")
        _seed_profile(conn, instrument_id=iid, cik="0000778010")
        # No filing_events rows seeded.
        conn.commit()

        assert not def14a_within_cap(
            conn,
            accession_number="DOES-NOT-EXIST",
            instrument_id=iid,
        )


class TestResolveTargetCikForCap:
    def test_direct_profile(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 778_020
        _seed_instrument(conn, iid=iid, symbol="DRT")
        _seed_profile(conn, instrument_id=iid, cik="0000778020")
        conn.commit()

        assert _resolve_target_cik_for_cap(conn, instrument_id=iid) == "0000778020"

    def test_sibling_fallback(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid_a, iid_b = 778_021, 778_022
        _seed_instrument(conn, iid=iid_a, symbol="SFA")
        _seed_instrument(conn, iid=iid_b, symbol="SFB")
        _seed_profile(conn, instrument_id=iid_b, cik="0000778021")
        # Shared DEF 14A accession on both siblings — sibling_b has profile.
        for iid in (iid_a, iid_b):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession="SF-001",
                filing_date=date(2026, 3, 1),
            )
        conn.commit()

        # iid_a has no profile, sibling iid_b does — should resolve
        # via fallback.
        assert _resolve_target_cik_for_cap(conn, instrument_id=iid_a) == "0000778021"

    def test_truly_missing_returns_none(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 778_023
        _seed_instrument(conn, iid=iid, symbol="MIS")
        # No profile, no filing_events rows.
        conn.commit()

        assert _resolve_target_cik_for_cap(conn, instrument_id=iid) is None


# ---------------------------------------------------------------------------
# Discovery query (§6.2)
# ---------------------------------------------------------------------------


class TestDiscoverPendingDef14aCap:
    def test_universe_returns_top_2_primary_per_cik(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        # Two filers, 5 primary DEF 14A each.
        for filer_idx, base in [(0, 779_001), (1, 779_010)]:
            _seed_instrument(conn, iid=base, symbol=f"U{filer_idx}")
            _seed_profile(conn, instrument_id=base, cik=f"00007790{filer_idx:02d}")
            for i, fdate in enumerate(
                [date(2022, 3, 1), date(2023, 3, 1), date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]
            ):
                _seed_filing_event(
                    conn,
                    instrument_id=base,
                    accession=f"UNI-{filer_idx}-{i:06d}",
                    filing_date=fdate,
                )
        conn.commit()

        result = discover_pending_def14a(conn, limit=100)
        accessions = sorted(r.accession_number for r in result)
        # Top 2 per filer: indices 4 + 3.
        expected = sorted([f"UNI-{f}-{i:06d}" for f in (0, 1) for i in (4, 3)])
        assert accessions == expected

    def test_universe_supplements_pass_unconditionally(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 779_020
        _seed_instrument(conn, iid=iid, symbol="SUP")
        _seed_profile(conn, instrument_id=iid, cik="0000779020")
        # 3 DEF 14A + 2 DEFA14A.
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="P-OLD",
            filing_date=date(2024, 3, 1),
            filing_type="DEF 14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="P-MID",
            filing_date=date(2025, 3, 1),
            filing_type="DEF 14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="P-NEW",
            filing_date=date(2026, 3, 1),
            filing_type="DEF 14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="A-1",
            filing_date=date(2024, 4, 1),
            filing_type="DEFA14A",
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="A-2",
            filing_date=date(2025, 4, 1),
            filing_type="DEFA14A",
        )
        conn.commit()

        result = discover_pending_def14a(conn, limit=100)
        accessions = sorted(r.accession_number for r in result)
        # Top-2 primaries (P-NEW, P-MID) + both DEFA14As.
        assert accessions == ["A-1", "A-2", "P-MID", "P-NEW"]

    def test_per_instrument_returns_top_2_per_cik(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 779_030
        _seed_instrument(conn, iid=iid, symbol="PI")
        _seed_profile(conn, instrument_id=iid, cik="0000779030")
        for i, fdate in enumerate(
            [date(2022, 3, 1), date(2023, 3, 1), date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]
        ):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=f"PI-{i:06d}",
                filing_date=fdate,
            )
        conn.commit()

        result = discover_pending_def14a(conn, instrument_id=iid, limit=100)
        accessions = sorted(r.accession_number for r in result)
        assert accessions == ["PI-000003", "PI-000004"]

    def test_rank_computed_across_logged_accessions(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Log the top-2 accessions. Discovery returns 0 (NOT the
        3rd promoted to rank-1)."""
        conn = ebull_test_conn
        iid = 779_040
        cik = "0000779040"
        _seed_instrument(conn, iid=iid, symbol="LOG")
        _seed_profile(conn, instrument_id=iid, cik=cik)
        for i, fdate in enumerate([date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=f"LOG-{i:06d}",
                filing_date=fdate,
            )
        # Mark the top-2 as already attempted.
        _log_ingest_attempt(conn, accession="LOG-000002", cik=cik)
        _log_ingest_attempt(conn, accession="LOG-000001", cik=cik)
        conn.commit()

        result = discover_pending_def14a(conn, limit=100)
        accessions = [r.accession_number for r in result]
        # LOG-000000 is rank 3 — capped out, NOT promoted to rank-1.
        assert "LOG-000000" not in accessions

    def test_url_null_rows_are_part_of_rank_set(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Latest 2 accessions have NULL URLs; the 3rd (with URL)
        is NOT promoted. We wait for URLs on the top-2 instead."""
        conn = ebull_test_conn
        iid = 779_050
        _seed_instrument(conn, iid=iid, symbol="URL")
        _seed_profile(conn, instrument_id=iid, cik="0000779050")
        # 3 accessions: top-2 NULL URL, oldest has URL.
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="URL-NEW",
            filing_date=date(2026, 3, 1),
            primary_document_url=None,
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="URL-MID",
            filing_date=date(2025, 3, 1),
            primary_document_url=None,
        )
        _seed_filing_event(
            conn,
            instrument_id=iid,
            accession="URL-OLD",
            filing_date=date(2024, 3, 1),
            primary_document_url="https://example.test/old.htm",
        )
        conn.commit()

        result = discover_pending_def14a(conn, limit=100)
        accessions = [r.accession_number for r in result]
        # URL-OLD ranks 3 by date — must NOT be returned even though
        # it's the only one with a URL.
        assert "URL-OLD" not in accessions

    def test_cik_missing_per_instrument_returns_all(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Truly-CIK-missing instrument: per-instrument call returns
        all its accessions uncapped (legacy path)."""
        conn = ebull_test_conn
        iid = 779_060
        _seed_instrument(conn, iid=iid, symbol="CKM")
        # No profile, no sibling — truly CIK-MISSING.
        for i, fdate in enumerate([date(2024, 3, 1), date(2025, 3, 1), date(2026, 3, 1)]):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=f"CKM-{i:06d}",
                filing_date=fdate,
            )
        conn.commit()

        result = discover_pending_def14a(conn, instrument_id=iid, limit=100)
        accessions = sorted(r.accession_number for r in result)
        # All 3 returned — legacy uncapped path.
        assert accessions == ["CKM-000000", "CKM-000001", "CKM-000002"]


# ---------------------------------------------------------------------------
# Manifest-worker parser pre-fetch gate (§6.3)
# ---------------------------------------------------------------------------


class TestParserPreFetchGate:
    """Parser-side cap check tombstones cap-bound accessions BEFORE
    the SEC HTTP call.

    The pre-fetch ordering invariant is enforced by the lint guard
    ``scripts/check_def14a_cap.sh`` invariant C; here we exercise
    the behaviour: a fake fetcher records every call; the cap-bound
    case yields zero calls.
    """

    @pytest.fixture(autouse=True)
    def _clear_parsers(self) -> Any:
        from app.jobs.sec_manifest_worker import clear_registered_parsers

        clear_registered_parsers()
        yield
        clear_registered_parsers()

    def test_cap_bound_accession_tombstones_without_fetch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.providers.implementations.sec_edgar import SecFilingsProvider
        from app.services.manifest_parsers.def14a import _parse_def14a

        conn = ebull_test_conn
        iid = 780_001
        cik = "0000780001"
        _seed_instrument(conn, iid=iid, symbol="PFG")
        _seed_profile(conn, instrument_id=iid, cik=cik)
        # 3 DEF 14A accessions; latest 2 are top-2, oldest is capped.
        seed_data = [
            ("PFG-NEW", date(2026, 3, 1)),
            ("PFG-MID", date(2025, 3, 1)),
            ("PFG-OLD", date(2024, 3, 1)),
        ]
        for acc, fdate in seed_data:
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=acc,
                filing_date=fdate,
                primary_document_url=f"https://example.test/{acc}.htm",
            )
        # Manifest row for the capped (oldest) accession.
        record_manifest_entry(
            conn,
            "PFG-OLD",
            cik=cik,
            form="DEF 14A",
            source="sec_def14a",
            subject_type="issuer",
            subject_id=str(iid),
            instrument_id=iid,
            filed_at=datetime(2024, 3, 1, tzinfo=UTC),
            primary_document_url="https://example.test/PFG-OLD.htm",
        )
        conn.commit()

        # Track any fetch attempts — must be zero.
        calls: list[str] = []

        def _fake_fetch(self: Any, url: str) -> str | None:
            calls.append(url)
            return "<html>UNREACHED</html>"

        monkeypatch.setattr(SecFilingsProvider, "fetch_document_text", _fake_fetch)

        # Fetch the manifest row and run the parser directly.
        from app.services.sec_manifest import get_manifest_row

        manifest_row = get_manifest_row(conn, "PFG-OLD")
        assert manifest_row is not None

        outcome = _parse_def14a(conn, manifest_row)

        assert outcome.status == "tombstoned"
        assert "latest-N primary cap" in (outcome.error or "")
        assert calls == [], "parser must not call fetch_document_text for capped row"

        # No ingest-log row written for the refused accession.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM def14a_ingest_log WHERE accession_number = %s",
                ("PFG-OLD",),
            )
            count = cur.fetchone()[0]  # type: ignore[index]
        assert count == 0


# ---------------------------------------------------------------------------
# Rewash rescue gate (§6.4)
# ---------------------------------------------------------------------------


class TestRewashGate:
    """Rewash carve-out: happy path is uncapped; rescue path is
    capped."""

    def test_rescue_path_refuses_out_of_cap_accession(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.raw_filings import RawFilingDocument
        from app.services.rewash_filings import _apply_def14a

        conn = ebull_test_conn
        iid = 781_001
        cik = "0000781001"
        _seed_instrument(conn, iid=iid, symbol="RWG")
        _seed_profile(conn, instrument_id=iid, cik=cik)
        # 3 DEF 14A: top-2 + 1 out-of-cap. Log the top-2 as already
        # ingested. The 3rd has a stored raw doc + ingest_log row
        # (rescue cohort).
        for i, (acc, fdate) in enumerate(
            [("RWG-NEW", date(2026, 3, 1)), ("RWG-MID", date(2025, 3, 1)), ("RWG-OLD", date(2024, 3, 1))]
        ):
            _seed_filing_event(
                conn,
                instrument_id=iid,
                accession=acc,
                filing_date=fdate,
                primary_document_url=f"https://example.test/{acc}.htm",
            )
        _log_ingest_attempt(conn, accession="RWG-NEW", cik=cik)
        _log_ingest_attempt(conn, accession="RWG-MID", cik=cik)
        # RWG-OLD: tombstoned earlier (status='partial' in real life),
        # but we just need an ingest_log row exists for the rescue
        # fallback to find.
        conn.execute(
            """
            INSERT INTO def14a_ingest_log (
                accession_number, issuer_cik, status, rows_inserted, rows_skipped
            ) VALUES ('RWG-OLD', %s, 'partial', 0, 0)
            ON CONFLICT (accession_number) DO NOTHING
            """,
            (cik,),
        )
        conn.commit()

        # Build a minimal RawFilingDocument shaped like the rewash
        # caller would have read from filing_raw_documents.
        body = "<html>doesn't matter, won't be parsed</html>"
        raw_doc = RawFilingDocument(
            accession_number="RWG-OLD",
            document_kind="def14a_body",
            payload=body,
            byte_count=len(body.encode("utf-8")),
            parser_version="def14a-v0",
            fetched_at=datetime(2024, 3, 1, tzinfo=UTC),
            source_url=None,
        )

        result = _apply_def14a(conn, raw_doc)
        # Rescue path: gate refuses → returns False.
        assert result is False

        # No typed rows written.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM def14a_beneficial_holdings
                WHERE accession_number = %s
                """,
                ("RWG-OLD",),
            )
            count = cur.fetchone()[0]  # type: ignore[index]
        assert count == 0
