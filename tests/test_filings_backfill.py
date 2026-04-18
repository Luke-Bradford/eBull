"""Tests for ``app.services.filings_backfill`` (#268 Chunk E).

Uses the real ``ebull_test`` database so the coverage-row write
semantics (gating, _finalise attempts delta, status preservation)
are exercised against actual psycopg3 + SQL behaviour.

The SEC provider is stubbed with a hand-rolled fake that implements
``fetch_submissions``, ``fetch_submissions_page``, and ``get_filing``
— enough surface area for every backfill path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import psycopg
import pytest

from app.providers.filings import FilingEvent, FilingNotFound
from app.services.filings_backfill import (
    BackfillOutcome,
    backfill_filings,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


# ---------------------------------------------------------------------
# Fake provider
# ---------------------------------------------------------------------


@dataclass
class _FakeResponse:
    """Slim shim for ``httpx.Response`` — only status_code / json()."""

    primary: dict[str, object] | None
    pages: dict[str, dict[str, object]]
    filings: dict[str, FilingEvent]
    submissions_raises: BaseException | None = None
    page_raises: dict[str, BaseException] | None = None
    get_filing_raises: dict[str, BaseException] | None = None


class FakeSecProvider:
    """Test double for SecFilingsProvider.

    Only the three methods backfill_filings calls are implemented.
    """

    def __init__(self, resp: _FakeResponse) -> None:
        self._resp = resp
        self.fetch_submissions_calls: list[str] = []
        self.fetch_page_calls: list[str] = []
        self.get_filing_calls: list[str] = []

    def fetch_submissions(self, cik: str) -> dict[str, object] | None:
        self.fetch_submissions_calls.append(cik)
        if self._resp.submissions_raises is not None:
            raise self._resp.submissions_raises
        return self._resp.primary

    def fetch_submissions_page(self, name: str) -> dict[str, object] | None:
        self.fetch_page_calls.append(name)
        if self._resp.page_raises and name in self._resp.page_raises:
            raise self._resp.page_raises[name]
        return self._resp.pages.get(name)

    def get_filing(self, provider_filing_id: str) -> FilingEvent:
        self.get_filing_calls.append(provider_filing_id)
        if self._resp.get_filing_raises and provider_filing_id in self._resp.get_filing_raises:
            raise self._resp.get_filing_raises[provider_filing_id]
        if provider_filing_id not in self._resp.filings:
            raise FilingNotFound(provider_filing_id)
        return self._resp.filings[provider_filing_id]


# ---------------------------------------------------------------------
# Helpers: seeding
# ---------------------------------------------------------------------


def _seed(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    cik: str,
    symbol: str = "TEST",
    filings_status: str | None = None,
    attempts: int = 0,
    last_at: datetime | None = None,
    last_reason: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, symbol, symbol),
    )
    conn.execute(
        """
        INSERT INTO coverage (
            instrument_id, coverage_tier,
            filings_status, filings_backfill_attempts,
            filings_backfill_last_at, filings_backfill_reason
        ) VALUES (%s, 3, %s, %s, %s, %s)
        """,
        (instrument_id, filings_status, attempts, last_at, last_reason),
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', %s, TRUE)",
        (instrument_id, cik),
    )
    conn.commit()


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    filing_date: date,
    filing_type: str,
    accession: str,
) -> None:
    conn.execute(
        "INSERT INTO filing_events "
        "(instrument_id, filing_date, filing_type, provider, provider_filing_id) "
        "VALUES (%s, %s, %s, 'sec', %s)",
        (instrument_id, filing_date, filing_type, accession),
    )
    conn.commit()


def _coverage(
    conn: psycopg.Connection[tuple], instrument_id: int
) -> tuple[str | None, int, datetime | None, str | None]:
    row = conn.execute(
        "SELECT filings_status, filings_backfill_attempts, "
        "filings_backfill_last_at, filings_backfill_reason "
        "FROM coverage WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchone()
    conn.commit()
    assert row is not None
    return (row[0], int(row[1]), row[2], row[3])


def _build_submissions(
    *,
    recent_filings: list[tuple[str, str, date]],
    files: list[dict[str, Any]] | None = None,
) -> dict[str, object]:
    """Build a primary submissions.json payload.

    ``recent_filings`` is a list of ``(accession, form, filing_date)``.
    ``files`` is the optional ``filings.files[]`` metadata list.
    """
    return {
        "cik": "0000000001",
        "filings": {
            "recent": {
                "accessionNumber": [f[0] for f in recent_filings],
                "filingDate": [f[2].isoformat() for f in recent_filings],
                "form": [f[1] for f in recent_filings],
                "primaryDocument": [""] * len(recent_filings),
                "reportDate": [""] * len(recent_filings),
            },
            "files": files or [],
        },
    }


def _page(recent_filings: list[tuple[str, str, date]]) -> dict[str, object]:
    """Build a secondary page payload (same shape as ``recent``)."""
    return {
        "accessionNumber": [f[0] for f in recent_filings],
        "filingDate": [f[2].isoformat() for f in recent_filings],
        "form": [f[1] for f in recent_filings],
        "primaryDocument": [""] * len(recent_filings),
        "reportDate": [""] * len(recent_filings),
    }


def _analysable_recent(today: date) -> list[tuple[str, str, date]]:
    """Return 2 × 10-K + 4 × 10-Q that satisfy the analysable bar."""
    return [
        ("AAAA-26-000001", "10-K", today - timedelta(days=400)),
        ("AAAA-24-000001", "10-K", today - timedelta(days=700)),
        ("AAAA-26-000002", "10-Q", today - timedelta(days=30)),
        ("AAAA-26-000003", "10-Q", today - timedelta(days=120)),
        ("AAAA-25-000004", "10-Q", today - timedelta(days=210)),
        ("AAAA-25-000005", "10-Q", today - timedelta(days=300)),
    ]


def _http_error() -> httpx.HTTPError:
    return httpx.ConnectError("simulated")


# ---------------------------------------------------------------------
# Gating tests (1-4b)
# ---------------------------------------------------------------------


class TestGating:
    def test_1_attempts_cap_with_http_error_skips(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="insufficient",
            attempts=3,
            last_at=datetime.now(UTC) - timedelta(days=30),
            last_reason=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR.value,
        )
        provider = FakeSecProvider(_FakeResponse(primary=None, pages={}, filings={}))

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.SKIPPED_ATTEMPTS_CAP
        assert provider.fetch_submissions_calls == []
        # Coverage unchanged (attempts still 3, status still insufficient).
        status, attempts, _, _ = _coverage(ebull_test_conn, 1)
        assert status == "insufficient"
        assert attempts == 3

    def test_2_backoff_window_skips(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="insufficient",
            attempts=1,
            last_at=datetime.now(UTC) - timedelta(days=3),
            last_reason=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR.value,
        )
        provider = FakeSecProvider(_FakeResponse(primary=None, pages={}, filings={}))

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.SKIPPED_BACKOFF_WINDOW
        assert provider.fetch_submissions_calls == []

    def test_3_cap_does_not_bite_on_exhausted_last_reason(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="insufficient",
            attempts=5,
            last_at=datetime.now(UTC) - timedelta(days=30),
            last_reason=BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED.value,
        )
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(recent_filings=_analysable_recent(date.today())),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.COMPLETE_OK
        assert len(provider.fetch_submissions_calls) == 1

    def test_4_cap_does_not_bite_on_structurally_young_last_reason(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="structurally_young",
            attempts=3,
            last_at=datetime.now(UTC) - timedelta(days=30),
            last_reason=BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG.value,
        )
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(
                    recent_filings=[
                        ("A-26-000001", "10-K", date.today() - timedelta(days=60)),
                    ]
                ),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        # Not gated; proceeds to a young classification.
        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG

    def test_4b_young_row_exempt_from_cap_even_with_http_error_reason(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """v5-H1 regression: a young row must not be frozen at cap
        when last_reason is retryable — otherwise an aged-out issuer
        never gets its clean EXHAUSTED demote."""
        today = date.today()
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="structurally_young",
            attempts=3,
            last_at=datetime.now(UTC) - timedelta(days=8),  # outside backoff
            last_reason=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR.value,
        )
        # Clean response, aged-out issuer (earliest filing 20 months ago).
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(
                    recent_filings=[
                        ("A-23-000001", "10-K", today - timedelta(days=600)),  # ~20mo
                    ]
                ),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        # Not skipped; aged-out → EXHAUSTED demotes to insufficient.
        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED
        status, _, _, _ = _coverage(ebull_test_conn, 1)
        assert status == "insufficient"


# ---------------------------------------------------------------------
# Terminal-outcome tests (5-15)
# ---------------------------------------------------------------------


class TestTerminalOutcomes:
    def test_5_complete_ok_on_page_zero(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(recent_filings=_analysable_recent(date.today())),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.COMPLETE_OK
        assert result.pages_fetched == 1
        assert result.filings_upserted == 6
        status, attempts, last_at, reason = _coverage(ebull_test_conn, 1)
        assert status == "analysable"
        assert attempts == 0
        assert last_at is not None
        assert reason == BackfillOutcome.COMPLETE_OK.value

    def test_6_complete_ok_with_pagination(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        today = date.today()
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        # Page 0: 1 × 10-K + 2 × 10-Q (below bar)
        # Page 1: +1 × 10-K + 2 × 10-Q (meets bar)
        primary = _build_submissions(
            recent_filings=[
                ("A-26-000001", "10-K", today - timedelta(days=400)),
                ("A-26-000002", "10-Q", today - timedelta(days=30)),
                ("A-26-000003", "10-Q", today - timedelta(days=120)),
            ],
            files=[
                {
                    "name": "page-001.json",
                    "filingCount": 3,
                    "filingFrom": (today - timedelta(days=730)).isoformat(),
                    "filingTo": (today - timedelta(days=200)).isoformat(),
                }
            ],
        )
        page_001 = _page(
            [
                ("A-24-000001", "10-K", today - timedelta(days=700)),
                ("A-25-000004", "10-Q", today - timedelta(days=210)),
                ("A-25-000005", "10-Q", today - timedelta(days=300)),
            ]
        )
        provider = FakeSecProvider(
            _FakeResponse(
                primary=primary,
                pages={"page-001.json": page_001},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.COMPLETE_OK
        assert result.pages_fetched == 2
        assert result.filings_upserted == 6

    def test_7_complete_fpi(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        today = date.today()
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(
                    recent_filings=[
                        ("F-26-000001", "20-F", today - timedelta(days=100)),
                        ("F-26-000002", "6-K", today - timedelta(days=50)),
                    ]
                ),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.COMPLETE_FPI
        status, attempts, _, reason = _coverage(ebull_test_conn, 1)
        assert status == "fpi"
        assert attempts == 0
        assert reason == BackfillOutcome.COMPLETE_FPI.value

    def test_8_structurally_young(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        today = date.today()
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="insufficient",
            attempts=2,
        )
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(
                    recent_filings=[
                        # Earliest filing 6 months ago — clearly young.
                        ("Y-26-000001", "10-K", today - timedelta(days=180)),
                        ("Y-26-000002", "10-Q", today - timedelta(days=90)),
                    ]
                ),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG
        status, attempts, _, reason = _coverage(ebull_test_conn, 1)
        assert status == "structurally_young"
        assert attempts == 2  # unchanged
        assert reason == BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG.value

    def test_9_exhausted_old_issuer(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        today = date.today()
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="insufficient",
            attempts=2,
        )
        provider = FakeSecProvider(
            _FakeResponse(
                # Earliest filing 5 years ago, below bar (1 × 10-K).
                primary=_build_submissions(
                    recent_filings=[
                        ("E-22-000001", "10-K", today - timedelta(days=1800)),
                    ]
                ),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED
        status, attempts, _, reason = _coverage(ebull_test_conn, 1)
        assert status == "insufficient"
        assert attempts == 2  # unchanged
        assert reason == BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED.value

    def test_10_exhausted_zero_filings(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(recent_filings=[]),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        # Zero filings → can't prove youth → EXHAUSTED (v2-H3).
        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED

    def test_11_http_error_on_fetch_submissions(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="insufficient",
            attempts=1,
        )
        provider = FakeSecProvider(
            _FakeResponse(
                primary=None,
                pages={},
                filings={},
                submissions_raises=_http_error(),
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR
        status, attempts, _, reason = _coverage(ebull_test_conn, 1)
        assert status == "insufficient"  # preserved — was already insufficient
        assert attempts == 2  # incremented
        assert reason == BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR.value

    def test_11b_http_error_preserves_structurally_young(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """v4-H2 regression: retryable error must not demote young."""
        _seed(
            ebull_test_conn,
            instrument_id=1,
            cik="0000000001",
            filings_status="structurally_young",
            attempts=1,
        )
        provider = FakeSecProvider(
            _FakeResponse(
                primary=None,
                pages={},
                filings={},
                submissions_raises=_http_error(),
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR
        status, attempts, _, _ = _coverage(ebull_test_conn, 1)
        assert status == "structurally_young"  # preserved
        assert attempts == 2

    def test_12_http_error_on_secondary_page(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        today = date.today()
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        primary = _build_submissions(
            recent_filings=[("A-26-000001", "10-K", today - timedelta(days=30))],
            files=[
                {
                    "name": "page-001.json",
                    "filingCount": 1,
                    "filingFrom": (today - timedelta(days=1000)).isoformat(),
                    "filingTo": (today - timedelta(days=500)).isoformat(),
                }
            ],
        )
        provider = FakeSecProvider(
            _FakeResponse(
                primary=primary,
                pages={},
                filings={},
                page_raises={"page-001.json": _http_error()},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR
        # Page-0 upsert should be durable.
        row = ebull_test_conn.execute("SELECT COUNT(*) FROM filing_events WHERE instrument_id = 1").fetchone()
        ebull_test_conn.commit()
        assert row is not None
        assert int(row[0]) == 1

    def test_13_404_on_fetch_submissions_classifies_http_error(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        provider = FakeSecProvider(_FakeResponse(primary=None, pages={}, filings={}))

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR

    def test_14_parse_error_json_decode(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        provider = FakeSecProvider(
            _FakeResponse(
                primary=None,
                pages={},
                filings={},
                submissions_raises=json.JSONDecodeError("bad", "", 0),
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR

    def test_15_parse_error_missing_recent(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        # No 'filings' key → KeyError path.
        provider = FakeSecProvider(
            _FakeResponse(
                primary={"cik": "0000000001"},
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR


# ---------------------------------------------------------------------
# 8-K gap check (16-17)
# ---------------------------------------------------------------------


class TestEightKGap:
    def test_16_defensive_gap_fill_succeeds(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Simulate a DB gap by deleting a 10-Q row between page-upsert
        and 8-K reconciliation isn't possible; instead exercise the
        8-K gap path by seeding a scenario where fetched 8-Ks aren't
        in DB — the easiest trigger is to pre-delete after upsert.

        Instead: test that the 8-K reconciliation query fires at all
        and that get_filing is called for any genuine gap. We seed a
        primary whose 8-Ks WILL be upserted in step 3; that path
        leaves no gap, so get_filing is not called. This test
        primarily guards the happy path (step 4 doesn't raise)."""
        today = date.today()
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(
                    recent_filings=[
                        *_analysable_recent(today),
                        ("K-26-000100", "8-K", today - timedelta(days=10)),
                        ("K-26-000101", "8-K", today - timedelta(days=60)),
                    ]
                ),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.COMPLETE_OK
        assert result.eight_k_gap_filled == 0  # no gap
        assert provider.get_filing_calls == []

    def test_16b_db_only_8k_is_not_treated_as_gap(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Documents the one-directional semantics of step 4:
        the reconciliation is defensive (``fetched - db``), NOT
        external-truth-detection (``sec_truth - our_db``). An 8-K
        already in DB that is NOT in seen_filings must NOT trigger
        a spurious gap-fill attempt — ground-truth completeness
        is guaranteed by step 3's pagination loop.

        PR #307 review PREVENTION item.
        """
        today = date.today()
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")
        # Pre-seed an 8-K directly in DB (from some prior daily-index
        # ingest) that SEC's submissions.json response does NOT include.
        _seed_filing(
            ebull_test_conn,
            instrument_id=1,
            filing_date=today - timedelta(days=100),
            filing_type="8-K",
            accession="K-26-PRE-DB",
        )
        # Provider returns analysable filings but none of the 8-Ks
        # the DB already has.
        provider = FakeSecProvider(
            _FakeResponse(
                primary=_build_submissions(recent_filings=_analysable_recent(today)),
                pages={},
                filings={},
            )
        )

        result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

        assert result.outcome == BackfillOutcome.COMPLETE_OK
        assert result.eight_k_gap_filled == 0
        # get_filing must NOT be called for the pre-existing DB 8-K —
        # otherwise we'd be re-fetching filings we already have.
        assert provider.get_filing_calls == []

    def test_17_http_error_on_gap_fill_classifies_retryable(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """When step 3 leaves a genuine 8-K gap (seen_filings but
        DB missing), get_filing is called; an HTTP error here must
        classify as HTTP_ERROR."""
        today = date.today()
        _seed(ebull_test_conn, instrument_id=1, cik="0000000001", filings_status="insufficient")

        # Stub _upsert_filing to drop 8-Ks so a gap exists in DB.
        # Simpler: pre-seed filing_events with just the 10-K/10-Q
        # filings + use fetch_submissions returning the analysable set
        # minus the 10-K/10-Q (so upsert adds 8-Ks; no gap). Need a
        # different approach to FORCE a gap — directly delete 8-Ks
        # after backfill upserts them but before step 4 queries? That
        # requires monkey-patching.
        #
        # Instead: use conn.execute to DELETE 8-Ks after the first
        # upsert. We can't interleave inside backfill_filings without
        # a monkey-patch. For this test we rely on the fact that
        # step 4's SQL SELECT runs AFTER all upserts, so a separate
        # connection can't race. Skip the direct-gap branch here and
        # cover it via integration by pre-deleting.
        #
        # Workaround: stage the state so no 8-Ks match the DB query
        # but seen_filings contains one via get_filing-on-gap-fill.
        # Simulate by returning empty primary + a files[] entry
        # whose page contains 8-Ks — but the upsert of that page
        # writes them to DB, same no-gap outcome.
        #
        # Cleanest: patch _upsert_filing at module level to skip 8-Ks
        # (simulates a silent ON CONFLICT block).
        import app.services.filings_backfill as backfill_module

        orig_upsert = backfill_module._upsert_filing

        def skip_8k_upsert(conn: Any, iid: Any, provider_name: Any, result: Any) -> None:
            if result.filing_type == "8-K":
                return
            orig_upsert(conn, iid, provider_name, result)

        backfill_module._upsert_filing = skip_8k_upsert
        try:
            provider = FakeSecProvider(
                _FakeResponse(
                    primary=_build_submissions(
                        recent_filings=[
                            *_analysable_recent(today),
                            ("K-26-000100", "8-K", today - timedelta(days=10)),
                        ]
                    ),
                    pages={},
                    filings={},
                    get_filing_raises={"K-26-000100": _http_error()},
                )
            )

            result = backfill_filings(ebull_test_conn, provider, "0000000001", 1)  # type: ignore[arg-type]

            assert result.outcome == BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR
            assert provider.get_filing_calls == ["K-26-000100"]
        finally:
            backfill_module._upsert_filing = orig_upsert


# ---------------------------------------------------------------------
# Normaliser regression (18-19)
# ---------------------------------------------------------------------


class TestNormaliserRegression:
    def test_18_normalise_recent_block(self) -> None:
        from app.providers.implementations.sec_edgar import _normalise_submissions_block

        today = date.today()
        block: dict[str, object] = {
            "accessionNumber": ["A-26-000001"],
            "filingDate": [today.isoformat()],
            "form": ["10-K"],
            "primaryDocument": ["main.htm"],
            "reportDate": [""],
        }

        results = _normalise_submissions_block(block, "0000000001")

        assert len(results) == 1
        assert results[0].provider_filing_id == "A-26-000001"
        assert results[0].filing_type == "10-K"

    def test_19_normalise_page_block_same_shape(self) -> None:
        """A files[] page carries the same arrays as `recent`."""
        from app.providers.implementations.sec_edgar import _normalise_submissions_block

        today = date.today()
        page = _page([("A-20-000001", "10-K", today - timedelta(days=2000))])

        results = _normalise_submissions_block(page, "0000000001")  # type: ignore[arg-type]

        assert len(results) == 1
        assert results[0].provider_filing_id == "A-20-000001"
