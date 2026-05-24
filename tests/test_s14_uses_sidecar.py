"""Tests for ``app.services.sec_submissions_files_walk.walk_files_pages``
(Stream A PR-B T1.3, #1233) — the S14 sidecar consumer that REPLACED the
pre-PR-B "re-fetch primary submissions.json per CIK" behaviour.

Covers:
  * Sidecar-empty → ``ciks_with_empty_sidecar`` + ``parse_errors`` both
    increment for an in-universe CIK that is NOT an agent.
  * Sidecar with only sentinel → ``ciks_with_no_overflow`` increments;
    no HTTP issued; ``parse_errors`` stays 0.
  * Sidecar with real pages → secondary pages fetched (HTTP); fixture
    accessions appended to filing_events.
  * Agent CIK with empty sidecar (expected) → silently skipped; NEITHER
    ``parse_errors`` NOR ``ciks_with_empty_sidecar`` increments.
  * Contract: ZERO PRIMARY ``data.sec.gov/submissions/CIK<10>.json``
    HTTP calls when the sidecar is populated. Pinned via ``respx`` at
    the httpx transport layer so a future caller bypassing
    ``SecFilingsProvider`` would still trip the assertion.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import psycopg.rows
import pytest
import respx
from httpx import Response

from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS
from app.services.sec_submissions_files_walk import walk_files_pages

_REAL_CIK = "0009999998"
_REAL_SYMBOL = "STREAMA"
_OVERFLOW_PAGE = f"CIK{_REAL_CIK}-submissions-001.json"


def _wipe_test_instrument(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (_REAL_CIK,))
        cur.execute(
            "DELETE FROM external_identifiers "
            "WHERE provider = 'sec' AND identifier_type = 'cik' AND identifier_value = %s",
            (_REAL_CIK,),
        )
        cur.execute("DELETE FROM instruments WHERE symbol = %s", (_REAL_SYMBOL,))
    conn.commit()


def _seed_test_instrument(conn: psycopg.Connection[tuple]) -> int:
    """``instruments.instrument_id`` is BIGINT PRIMARY KEY (no DEFAULT,
    no sequence — manually assigned per sql/001:2). Allocate one
    above the current MAX so we don't clash with prod-like data the
    test DB may already carry."""
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(instrument_id), 0) FROM instruments")
        row = cur.fetchone()
        assert row is not None
        iid = int(row[0]) + 1
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable) "
            "VALUES (%s, %s, %s, %s, TRUE)",
            (iid, _REAL_SYMBOL, "Stream A Test Co.", "NASDAQ"),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, _REAL_CIK),
        )
    conn.commit()
    return iid


def _insert_sidecar(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    pages: list[tuple[str, str | None, str | None]],
) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (cik,))
        for name, ff, ft in pages:
            cur.execute(
                "INSERT INTO sec_cik_submissions_files_index "
                "(cik, page_name, filing_from, filing_to, bootstrap_run_id, populate_origin) "
                "VALUES (%s, %s, %s, %s, NULL, 'steady_state')",
                (cik, name, ff, ft),
            )
    conn.commit()


@pytest.fixture
def s14_test_instrument(
    ebull_test_conn: psycopg.Connection[tuple],
) -> Iterator[int]:
    _wipe_test_instrument(ebull_test_conn)
    iid = _seed_test_instrument(ebull_test_conn)
    yield iid
    _wipe_test_instrument(ebull_test_conn)


class TestS14SidecarConsume:
    """All assertions use DELTA against a BASELINE call so the tests are
    robust to other rows in the shared per-worker test DB (per PR #1308
    review bot BLOCKING — exact equality on DB-global counters is a
    flake vector on any non-empty test DB)."""

    @pytest.mark.integration
    def test_empty_sidecar_for_in_universe_cik_is_parse_error(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        # WIPE FIRST so a dirty _REAL_CIK row left by a killed prior
        # run doesn't get counted in baseline; then capture baseline;
        # then seed + measure delta (bot review iter 2 WARNING — baseline-
        # before-wipe was a flake vector).
        _wipe_test_instrument(ebull_test_conn)
        baseline = walk_files_pages(conn=ebull_test_conn)
        _seed_test_instrument(ebull_test_conn)
        try:
            # No sidecar row inserted for _REAL_CIK — empty sidecar branch.
            after = walk_files_pages(conn=ebull_test_conn)

            assert after.ciks_with_empty_sidecar - baseline.ciks_with_empty_sidecar == 1
            assert after.parse_errors - baseline.parse_errors == 1
            assert after.secondary_pages_fetched == baseline.secondary_pages_fetched
        finally:
            _wipe_test_instrument(ebull_test_conn)

    @pytest.mark.integration
    def test_sentinel_row_skips_secondary_walk_silently(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        # Wipe BEFORE baseline (bot review iter 2 WARNING).
        _wipe_test_instrument(ebull_test_conn)
        baseline = walk_files_pages(conn=ebull_test_conn)
        _seed_test_instrument(ebull_test_conn)
        try:
            _insert_sidecar(
                ebull_test_conn,
                cik=_REAL_CIK,
                pages=[("__no_overflow_pages__", None, None)],
            )
            after = walk_files_pages(conn=ebull_test_conn)

            assert after.ciks_with_no_overflow - baseline.ciks_with_no_overflow == 1
            # Sentinel branch never enters the fetch loop.
            assert after.secondary_pages_fetched == baseline.secondary_pages_fetched
            # Sentinel-only is NOT an error.
            assert after.ciks_with_empty_sidecar == baseline.ciks_with_empty_sidecar
            assert after.parse_errors == baseline.parse_errors
        finally:
            _wipe_test_instrument(ebull_test_conn)

    @pytest.mark.integration
    def test_agent_cik_with_empty_sidecar_is_not_an_error(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """An agent CIK in the universe with no sidecar row is EXPECTED
        (populate filters them out). Must NOT increment parse_errors
        or ciks_with_empty_sidecar. Delta-based assertions (per PR #1308
        review bot BLOCKING)."""
        agent_cik = next(iter(KNOWN_FILING_AGENT_CIKS))
        _wipe_test_instrument(ebull_test_conn)

        baseline = walk_files_pages(conn=ebull_test_conn)

        # Seed instrument with the agent CIK.
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(instrument_id), 0) FROM instruments")
            row = cur.fetchone()
            assert row is not None
            iid = int(row[0]) + 1
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable) "
                "VALUES (%s, %s, %s, %s, TRUE)",
                (iid, _REAL_SYMBOL, "Agent CIK Test Co.", "NASDAQ"),
            )
            cur.execute(
                "INSERT INTO external_identifiers "
                "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
                "VALUES (%s, 'sec', 'cik', %s, TRUE)",
                (iid, agent_cik),
            )
        ebull_test_conn.commit()

        try:
            after = walk_files_pages(conn=ebull_test_conn)
            # Delta on the three counters that the agent-CIK branch
            # must NOT touch — robust to any other rows in the test DB.
            assert after.ciks_with_empty_sidecar == baseline.ciks_with_empty_sidecar, (
                "agent CIK empty sidecar must NOT count as error"
            )
            assert after.parse_errors == baseline.parse_errors
            assert after.secondary_pages_fetched == baseline.secondary_pages_fetched
            # And ciks_visited must NOT increment for the agent CIK
            # (Architect IMPORTANT — guard fires before counter).
            assert after.ciks_visited == baseline.ciks_visited
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM external_identifiers "
                    "WHERE provider = 'sec' AND identifier_type = 'cik' AND identifier_value = %s",
                    (agent_cik,),
                )
                cur.execute("DELETE FROM instruments WHERE symbol = %s", (_REAL_SYMBOL,))
            ebull_test_conn.commit()


class TestS14ZeroPrimaryHttpContract:
    """Pinned contract: ``walk_files_pages`` MUST issue ZERO HTTP calls
    to the primary ``data.sec.gov/submissions/CIK<10>.json`` URL when
    the sidecar is populated. Asserted at the httpx transport layer
    via respx so a future code path that bypasses ``SecFilingsProvider``
    would still trip the test."""

    @pytest.mark.integration
    def test_no_primary_fetch_when_sidecar_has_real_pages(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        s14_test_instrument: int,
    ) -> None:
        _insert_sidecar(
            ebull_test_conn,
            cik=_REAL_CIK,
            pages=[(_OVERFLOW_PAGE, "2010-01-15", "2012-06-30")],
        )

        # respx registers wire-level mocks. We register:
        #   * the secondary page URL → 200 with a minimal-shape body
        #     so _normalise_submissions_block returns 0 filings (no
        #     filing_events writes needed for the contract test).
        #   * an explicit assert-not-called on the primary URL.
        primary_url = f"https://data.sec.gov/submissions/CIK{_REAL_CIK}.json"
        secondary_url = f"https://data.sec.gov/submissions/{_OVERFLOW_PAGE}"

        empty_secondary_body: dict[str, Any] = {
            "filings": {
                "accessionNumber": [],
                "filingDate": [],
                "form": [],
                "acceptanceDateTime": [],
                "primaryDocument": [],
            },
        }

        with respx.mock(assert_all_called=False) as mock:
            primary_route = mock.get(primary_url).mock(return_value=Response(200, json={}))
            mock.get(secondary_url).mock(return_value=Response(200, json=empty_secondary_body))

            result = walk_files_pages(conn=ebull_test_conn)

        # The load-bearing assertion — primary URL was NEVER hit.
        assert primary_route.call_count == 0, (
            f"S14 issued {primary_route.call_count} PRIMARY {primary_url} fetch(es); "
            "sidecar exists so the primary refetch contract is violated"
        )
        # secondary_pages_fetched should reflect the real fetch via respx.
        assert result.secondary_pages_fetched >= 1
