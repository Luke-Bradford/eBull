"""Tests for Item 7 (#1233 ``docs/proposals/etl/run-8-readiness-fixes.md``)
— HTTP ``If-Modified-Since`` round-trip via ``external_data_watermarks``
for the per-CIK submissions.json poll and the secondary submissions-
pages walker.

CAVEMAN scope: SEC publishes ``Last-Modified`` on submissions.json +
``CIK*-submissions-NNN.json`` files. Conditional-GET turns "re-fetch
+ re-parse same payload every tick" into "304 + skip" — directly
conserves the 10 req/s SEC budget.

Two surfaces covered:

* ``app/jobs/sec_per_cik_poll.py::run_per_cik_poll`` — per-CIK
  scheduled polling. Watermark namespace
  ``sec.last_modified.per_cik_poll`` / key = 10-digit CIK.
* ``app/services/sec_submissions_files_walk.py::walk_files_pages`` —
  secondary-page walker. Watermark namespace
  ``sec.last_modified.submissions_files`` / key = ``<cik>:<page_name>``.

Both MUST be disjoint from the legacy ``sec.submissions`` namespace
(stores top-accession at ``app/services/fundamentals/__init__.py:2030``).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
import respx
from httpx import Response

from app.jobs.sec_per_cik_poll import run_per_cik_poll
from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS
from app.services.data_freshness import record_poll_outcome
from app.services.sec_submissions_files_walk import walk_files_pages
from app.services.watermarks import get_watermark, set_watermark
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


# Source-key namespaces under test — pinned to fail loudly if a future
# refactor silently renames them and collides with the legacy
# ``sec.submissions`` source.
_SOURCE_KEY_PER_CIK_POLL = "sec.last_modified.per_cik_poll"
_SOURCE_KEY_SUBMISSIONS_FILES = "sec.last_modified.submissions_files"


# -----------------------------------------------------------------
# Per-CIK poll fixtures
# -----------------------------------------------------------------


_TEST_CIK = "0000320193"
_TEST_INSTRUMENT_ID = 1701
_TEST_SYMBOL = "PERCIKLM"


def _aapl_submissions_recent(accession: str = "0000320193-26-000099") -> dict[str, Any]:
    return {
        "cik": "320193",
        "filings": {
            "recent": {
                "accessionNumber": [accession],
                "filingDate": ["2026-04-30"],
                "form": ["8-K"],
                "acceptanceDateTime": ["2026-04-30T16:00:00.000Z"],
                "primaryDocument": ["item502.htm"],
            },
            "files": [],
        },
    }


def _seed_subject(conn: psycopg.Connection[tuple]) -> None:
    """Seed a single AAPL-like CIK + scheduler row past expected_next_at
    so the poller picks it up.
    """
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, 'Per-CIK LM Test', '4', 'USD', TRUE)
        """,
        (_TEST_INSTRUMENT_ID, _TEST_SYMBOL),
    )
    conn.execute(
        "INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (%s, %s)",
        (_TEST_INSTRUMENT_ID, _TEST_CIK),
    )
    conn.commit()

    record_poll_outcome(
        conn,
        subject_type="issuer",
        subject_id=str(_TEST_INSTRUMENT_ID),
        source="sec_8k",
        outcome="current",
        last_known_filing_id="0000320193-25-000001",
        last_known_filed_at=datetime(2025, 1, 1, tzinfo=UTC),
        cik=_TEST_CIK,
        instrument_id=_TEST_INSTRUMENT_ID,
    )
    with conn.cursor() as cur:
        cur.execute("UPDATE data_freshness_index SET expected_next_at = '2024-01-01' WHERE source = 'sec_8k'")
    conn.commit()


def _make_http_get_with_meta(
    *,
    status: int,
    payload: dict[str, Any] | bytes,
    last_modified: str | None,
    capture: list[dict[str, str]] | None = None,
):
    """Test double for ``HttpGetWithMeta``. Captures request headers
    into ``capture`` so the assertion path can read back what was sent.
    """
    body = json.dumps(payload).encode("utf-8") if isinstance(payload, dict) else payload

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes, str | None]:
        if capture is not None:
            capture.append(dict(headers))
        return status, body, last_modified

    return _impl


# -----------------------------------------------------------------
# Per-CIK poll tests
# -----------------------------------------------------------------


class TestPerCikPollConditionalGet:
    """Item 7 (#1233): per-CIK poll round-trips Last-Modified."""

    def test_first_fetch_persists_watermark(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """No prior watermark → unconditional fetch → 200 with Last-
        Modified header → watermark UPSERTed under the namespaced key.
        """
        _seed_subject(ebull_test_conn)
        captured: list[dict[str, str]] = []
        http = _make_http_get_with_meta(
            status=200,
            payload=_aapl_submissions_recent(),
            last_modified="Wed, 30 Apr 2026 16:00:00 GMT",
            capture=captured,
        )

        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=http,
            source="sec_8k",
        )
        ebull_test_conn.commit()

        assert stats.subjects_polled == 1
        # First fetch has NO If-Modified-Since header (watermark absent).
        assert "If-Modified-Since" not in captured[0]

        wm = get_watermark(ebull_test_conn, _SOURCE_KEY_PER_CIK_POLL, _TEST_CIK)
        assert wm is not None, "watermark must be persisted on 200 with Last-Modified"
        assert wm.watermark == "Wed, 30 Apr 2026 16:00:00 GMT"

    def test_second_fetch_sends_if_modified_since(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Pre-seed a watermark; the next fetch MUST inject
        ``If-Modified-Since: <stored watermark>``.
        """
        _seed_subject(ebull_test_conn)
        # Pre-seed the watermark inside an explicit tx — set_watermark
        # requires INTRANS.
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source=_SOURCE_KEY_PER_CIK_POLL,
                key=_TEST_CIK,
                watermark="Tue, 29 Apr 2026 16:00:00 GMT",
                watermark_at=None,
            )
        ebull_test_conn.commit()

        captured: list[dict[str, str]] = []
        http = _make_http_get_with_meta(
            status=200,
            payload=_aapl_submissions_recent(),
            last_modified="Wed, 30 Apr 2026 16:00:00 GMT",
            capture=captured,
        )

        run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=http,
            source="sec_8k",
        )
        ebull_test_conn.commit()

        # Pinned assertion — Item 7 acceptance test.
        assert captured, "http_get_with_meta must have been called at least once"
        assert captured[0].get("If-Modified-Since") == "Tue, 29 Apr 2026 16:00:00 GMT"

        # And the newer watermark replaced the prior value.
        wm = get_watermark(ebull_test_conn, _SOURCE_KEY_PER_CIK_POLL, _TEST_CIK)
        assert wm is not None
        assert wm.watermark == "Wed, 30 Apr 2026 16:00:00 GMT"

    def test_304_skips_parse_and_bumps_watermark_at_only(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """304 path: payload NOT re-parsed; ``watermark`` value unchanged;
        ``watermark_at`` (== fetched_at column row) bumped to NOW.
        """
        _seed_subject(ebull_test_conn)
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source=_SOURCE_KEY_PER_CIK_POLL,
                key=_TEST_CIK,
                watermark="Tue, 29 Apr 2026 16:00:00 GMT",
                watermark_at=None,
            )
        ebull_test_conn.commit()
        wm_before = get_watermark(ebull_test_conn, _SOURCE_KEY_PER_CIK_POLL, _TEST_CIK)
        assert wm_before is not None
        fetched_before = wm_before.fetched_at

        # Payload is intentionally NONSENSE bytes — if the conditional
        # path mistakenly tries to parse it on a 304 the test will
        # throw a JSONDecodeError up the stack. The 304 branch MUST
        # short-circuit before parse_submissions_page is invoked.
        http = _make_http_get_with_meta(
            status=304,
            payload=b"NOT-JSON-WOULD-RAISE-IF-PARSED",
            last_modified=None,
        )

        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=http,
            source="sec_8k",
        )
        ebull_test_conn.commit()

        # No new filings recorded on 304.
        assert stats.new_filings_recorded == 0
        assert stats.poll_errors == 0

        wm_after = get_watermark(ebull_test_conn, _SOURCE_KEY_PER_CIK_POLL, _TEST_CIK)
        assert wm_after is not None
        # watermark VALUE unchanged on 304.
        assert wm_after.watermark == "Tue, 29 Apr 2026 16:00:00 GMT"
        # fetched_at (== watermark_at-stamp) bumped forward.
        assert wm_after.fetched_at >= fetched_before

    def test_200_with_newer_last_modified_reparses_and_updates(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """200 with a newer ``Last-Modified`` than the stored watermark:
        payload IS parsed, manifest row recorded, watermark replaced.
        """
        _seed_subject(ebull_test_conn)
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source=_SOURCE_KEY_PER_CIK_POLL,
                key=_TEST_CIK,
                watermark="Tue, 29 Apr 2026 16:00:00 GMT",
                watermark_at=None,
            )
        ebull_test_conn.commit()

        http = _make_http_get_with_meta(
            status=200,
            payload=_aapl_submissions_recent("0000320193-26-000200"),
            last_modified="Thu, 01 May 2026 16:00:00 GMT",
        )

        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=http,
            source="sec_8k",
        )
        ebull_test_conn.commit()

        # Re-parsed → manifest got the new accession.
        assert stats.new_filings_recorded == 1

        wm = get_watermark(ebull_test_conn, _SOURCE_KEY_PER_CIK_POLL, _TEST_CIK)
        assert wm is not None
        assert wm.watermark == "Thu, 01 May 2026 16:00:00 GMT"


# -----------------------------------------------------------------
# Files-walk fixtures + tests
# -----------------------------------------------------------------


_FW_CIK = "0009999997"
_FW_SYMBOL = "FWLM"
_FW_PAGE = f"CIK{_FW_CIK}-submissions-001.json"


def _wipe_fw_instrument(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sec_cik_submissions_files_index WHERE cik = %s", (_FW_CIK,))
        cur.execute(
            "DELETE FROM external_identifiers "
            "WHERE provider = 'sec' AND identifier_type = 'cik' AND identifier_value = %s",
            (_FW_CIK,),
        )
        cur.execute("DELETE FROM instruments WHERE symbol = %s", (_FW_SYMBOL,))
        cur.execute(
            "DELETE FROM external_data_watermarks WHERE source = %s",
            (_SOURCE_KEY_SUBMISSIONS_FILES,),
        )
    conn.commit()


def _seed_fw_instrument(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(instrument_id), 0) FROM instruments")
        row = cur.fetchone()
        assert row is not None
        iid = int(row[0]) + 1
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable) "
            "VALUES (%s, %s, 'Files-Walk LM Test', 'NASDAQ', TRUE)",
            (iid, _FW_SYMBOL),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, _FW_CIK),
        )
    conn.commit()
    return iid


def _insert_fw_sidecar(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sec_cik_submissions_files_index "
            "(cik, page_name, filing_from, filing_to, bootstrap_run_id, populate_origin) "
            "VALUES (%s, %s, '2010-01-15', '2012-06-30', NULL, 'steady_state')",
            (_FW_CIK, _FW_PAGE),
        )
    conn.commit()


@pytest.fixture
def files_walk_instrument(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[int]:
    _wipe_fw_instrument(ebull_test_conn)
    iid = _seed_fw_instrument(ebull_test_conn)
    _insert_fw_sidecar(ebull_test_conn)
    yield iid
    _wipe_fw_instrument(ebull_test_conn)


_EMPTY_SECONDARY_BODY: dict[str, Any] = {
    "filings": {
        "accessionNumber": [],
        "filingDate": [],
        "form": [],
        "acceptanceDateTime": [],
        "primaryDocument": [],
    },
}


class TestFilesWalkConditionalGet:
    """Item 7 (#1233): secondary-pages walker uses If-Modified-Since."""

    def test_first_fetch_persists_watermark(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        files_walk_instrument: int,
    ) -> None:
        secondary_url = f"https://data.sec.gov/submissions/{_FW_PAGE}"

        with respx.mock(assert_all_called=False) as mock:
            mock.get(secondary_url).mock(
                return_value=Response(
                    200,
                    json=_EMPTY_SECONDARY_BODY,
                    headers={"Last-Modified": "Wed, 30 Apr 2026 16:00:00 GMT"},
                )
            )
            result = walk_files_pages(conn=ebull_test_conn)
        ebull_test_conn.commit()

        # secondary_pages_fetched advances on a 200.
        assert result.secondary_pages_fetched >= 1
        wm = get_watermark(
            ebull_test_conn,
            _SOURCE_KEY_SUBMISSIONS_FILES,
            f"{_FW_CIK}:{_FW_PAGE}",
        )
        assert wm is not None
        assert wm.watermark == "Wed, 30 Apr 2026 16:00:00 GMT"

    def test_second_fetch_sends_if_modified_since(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        files_walk_instrument: int,
    ) -> None:
        # Pre-seed the watermark so the walker reads + sends it.
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source=_SOURCE_KEY_SUBMISSIONS_FILES,
                key=f"{_FW_CIK}:{_FW_PAGE}",
                watermark="Tue, 29 Apr 2026 16:00:00 GMT",
                watermark_at=None,
            )
        ebull_test_conn.commit()

        secondary_url = f"https://data.sec.gov/submissions/{_FW_PAGE}"
        with respx.mock(assert_all_called=False) as mock:
            route = mock.get(secondary_url).mock(
                return_value=Response(
                    200,
                    json=_EMPTY_SECONDARY_BODY,
                    headers={"Last-Modified": "Wed, 30 Apr 2026 16:00:00 GMT"},
                )
            )
            walk_files_pages(conn=ebull_test_conn)
        ebull_test_conn.commit()

        # Pinned assertion — Item 7 acceptance test.
        assert route.call_count == 1
        sent_headers = dict(route.calls.last.request.headers)
        # httpx lowercases header keys on the request object.
        assert sent_headers.get("if-modified-since") == "Tue, 29 Apr 2026 16:00:00 GMT"

        wm = get_watermark(
            ebull_test_conn,
            _SOURCE_KEY_SUBMISSIONS_FILES,
            f"{_FW_CIK}:{_FW_PAGE}",
        )
        assert wm is not None
        assert wm.watermark == "Wed, 30 Apr 2026 16:00:00 GMT"

    def test_304_skips_parse_and_bumps_watermark_at_only(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        files_walk_instrument: int,
    ) -> None:
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source=_SOURCE_KEY_SUBMISSIONS_FILES,
                key=f"{_FW_CIK}:{_FW_PAGE}",
                watermark="Tue, 29 Apr 2026 16:00:00 GMT",
                watermark_at=None,
            )
        ebull_test_conn.commit()
        wm_before = get_watermark(
            ebull_test_conn,
            _SOURCE_KEY_SUBMISSIONS_FILES,
            f"{_FW_CIK}:{_FW_PAGE}",
        )
        assert wm_before is not None

        secondary_url = f"https://data.sec.gov/submissions/{_FW_PAGE}"
        with respx.mock(assert_all_called=False) as mock:
            # 304 with NO body — if the walker tries to parse it the
            # JSON decoder upstream would raise. The 304 branch MUST
            # short-circuit before parse.
            mock.get(secondary_url).mock(return_value=Response(304))
            result = walk_files_pages(conn=ebull_test_conn)
        ebull_test_conn.commit()

        # 304 does NOT count as a fetched page (no payload returned).
        # The dedicated counter MUST increment.
        assert result.secondary_pages_not_modified >= 1
        # parse_errors MUST NOT increment on a 304 short-circuit.
        assert result.parse_errors == 0
        # No new filings written.
        assert result.filings_upserted == 0

        wm_after = get_watermark(
            ebull_test_conn,
            _SOURCE_KEY_SUBMISSIONS_FILES,
            f"{_FW_CIK}:{_FW_PAGE}",
        )
        assert wm_after is not None
        # Watermark VALUE unchanged.
        assert wm_after.watermark == "Tue, 29 Apr 2026 16:00:00 GMT"
        # fetched_at bumped.
        assert wm_after.fetched_at >= wm_before.fetched_at

    def test_200_with_newer_last_modified_reparses_and_updates(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        files_walk_instrument: int,
    ) -> None:
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source=_SOURCE_KEY_SUBMISSIONS_FILES,
                key=f"{_FW_CIK}:{_FW_PAGE}",
                watermark="Tue, 29 Apr 2026 16:00:00 GMT",
                watermark_at=None,
            )
        ebull_test_conn.commit()

        secondary_url = f"https://data.sec.gov/submissions/{_FW_PAGE}"
        with respx.mock(assert_all_called=False) as mock:
            mock.get(secondary_url).mock(
                return_value=Response(
                    200,
                    json=_EMPTY_SECONDARY_BODY,
                    headers={"Last-Modified": "Thu, 01 May 2026 16:00:00 GMT"},
                )
            )
            result = walk_files_pages(conn=ebull_test_conn)
        ebull_test_conn.commit()

        assert result.secondary_pages_fetched >= 1

        wm = get_watermark(
            ebull_test_conn,
            _SOURCE_KEY_SUBMISSIONS_FILES,
            f"{_FW_CIK}:{_FW_PAGE}",
        )
        assert wm is not None
        assert wm.watermark == "Thu, 01 May 2026 16:00:00 GMT"


# -----------------------------------------------------------------
# Namespace-isolation invariant
# -----------------------------------------------------------------


class TestSourceKeyNamespaceIsolation:
    """Pinned invariant: the new ``sec.last_modified.*`` namespaces
    MUST NOT collide with the legacy ``sec.submissions`` source (which
    stores top-accession at ``app/services/fundamentals/__init__.py:2030``,
    NOT HTTP Last-Modified).
    """

    def test_per_cik_poll_writes_under_namespaced_source_only(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Use a CIK that nothing else in the test DB references so we
        # can assert "zero rows under sec.submissions for this key".
        _seed_subject(ebull_test_conn)
        http = _make_http_get_with_meta(
            status=200,
            payload=_aapl_submissions_recent(),
            last_modified="Wed, 30 Apr 2026 16:00:00 GMT",
        )
        run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=http,
            source="sec_8k",
        )
        ebull_test_conn.commit()

        wm_new = get_watermark(ebull_test_conn, _SOURCE_KEY_PER_CIK_POLL, _TEST_CIK)
        wm_legacy = get_watermark(ebull_test_conn, "sec.submissions", _TEST_CIK)

        assert wm_new is not None, "namespaced watermark MUST be written"
        # Legacy ``sec.submissions`` source is owned by the
        # fundamentals planner — Item 7 MUST NOT touch it.
        assert wm_legacy is None, (
            "Item 7 MUST NOT write under the legacy 'sec.submissions' source "
            "— that namespace stores top-accession, not HTTP Last-Modified"
        )

    def test_known_agent_cik_set_unchanged_by_item7(self) -> None:
        """Smoke: KNOWN_FILING_AGENT_CIKS is the documented denylist
        the walker skips at the top of the loop. Item 7 must not have
        removed it from import scope (otherwise the walker would
        silently start polling agent CIKs and waste 10 req/s budget)."""
        assert len(KNOWN_FILING_AGENT_CIKS) > 0
