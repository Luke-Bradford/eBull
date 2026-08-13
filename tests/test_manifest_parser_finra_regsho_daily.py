"""Tests for the ``finra_regsho_daily`` synth no-op manifest-worker
parser (G6/#916).

Mirror of ``test_manifest_parser_finra_short_interest.py`` — fourth
adopter of the sec-edgar §11.5.1 synth no-op pattern. FINRA RegSHO
daily data lands via the ``finra_regsho_daily_refresh`` ScheduledJob;
the manifest dispatch path exists for audit only.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    registered_parser_sources,
    run_manifest_worker,
)
from app.services.manifest_parsers.finra_regsho_daily import (
    PARSER_VERSION,
    _parse_finra_regsho_daily,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_pending_finra_regsho(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    filed_at: datetime | None = None,
) -> None:
    """Seed a ``finra_regsho_daily`` manifest row in ``pending`` state."""
    if filed_at is None:
        filed_at = datetime(2026, 5, 15, tzinfo=UTC)
    # accession shape: FINRA_REGSHO_{PREFIX}_{YYYYMMDD}.
    tail = accession[len("FINRA_REGSHO_") :]
    prefix, date_compact = tail.rsplit("_", 1)
    primary_doc_url = f"https://cdn.finra.org/equity/regsho/daily/{prefix}shvol{date_compact}.txt"
    record_manifest_entry(
        conn,
        accession,
        cik="FINRA_REGSHO",
        form="REGSHO",
        source="finra_regsho_daily",
        subject_type="finra_universe",
        subject_id="FINRA_REGSHO",
        instrument_id=None,
        filed_at=filed_at,
        primary_document_url=primary_doc_url,
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def _count_filing_raw_documents(conn: psycopg.Connection[tuple], accession: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM filing_raw_documents WHERE accession_number = %s",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_synth_no_op_marks_parsed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Happy path: pending finra_regsho_daily row drains to ``parsed``;
    synth no-op stores no raw payload (ScheduledJob is sole writer).
    """
    conn = ebull_test_conn
    accession = "FINRA_REGSHO_CNMS_20260515"
    _seed_pending_finra_regsho(conn, accession=accession)

    stats = run_manifest_worker(conn, source="finra_regsho_daily", max_rows=5)
    assert stats.parsed == 1
    assert stats.failed == 0
    assert stats.tombstoned == 0
    assert stats.skipped_no_parser == 0

    final = get_manifest_row(conn, accession)
    assert final is not None
    assert final.ingest_status == "parsed"
    assert final.parser_version == PARSER_VERSION == "finra-regsho-daily-v1"
    assert _count_filing_raw_documents(conn, accession) == 0


def test_register_all_parsers_includes_finra_regsho_daily() -> None:
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()

    sources = registered_parser_sources()
    assert "finra_regsho_daily" in sources
    # Spot-check the bimonthly sibling is also still registered.
    assert "finra_short_interest" in sources


class _NoTouchConnection:
    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.execute")

    def cursor(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.cursor")

    def transaction(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("synth no-op must not call conn.transaction")


class _NoTouchRow:
    accession_number = "FINRA_REGSHO_CNMS_20260515"


def test_parser_does_not_touch_db_or_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.providers.implementations import sec_edgar
    from app.services import raw_filings
    from app.services.manifest_parsers import (
        finra_regsho_daily as finra_regsho_daily_module,
    )

    def _raise_store_raw(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call store_raw")

    def _raise_fetch(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("synth no-op must not call fetch_document_text")

    monkeypatch.setattr(raw_filings, "store_raw", _raise_store_raw)
    monkeypatch.setattr(finra_regsho_daily_module, "store_raw", _raise_store_raw, raising=False)
    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _raise_fetch)

    outcome = _parse_finra_regsho_daily(_NoTouchConnection(), _NoTouchRow())  # type: ignore[arg-type]
    assert outcome.status == "parsed"
    assert outcome.parser_version == PARSER_VERSION == "finra-regsho-daily-v1"
    assert outcome.raw_status is None
    assert outcome.error is None
