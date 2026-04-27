"""get_business_sections respects optional accession filter (#559)."""

from __future__ import annotations

import psycopg
import pytest

from app.services.business_summary import (
    ParsedBusinessSection,
    get_business_sections,
    upsert_business_sections,
)


def _seed_instrument(conn: psycopg.Connection[tuple], symbol: str = "GSE2", iid: int = 199) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, symbol, "Test Co Accession"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _section(body: str) -> ParsedBusinessSection:
    return ParsedBusinessSection(
        section_order=0,
        section_key="general",
        section_label="General",
        body=body,
        cross_references=(),
    )


@pytest.mark.integration
def test_get_returns_latest_when_no_accession_provided(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """No accession → returns the newer snapshot."""
    iid = _seed_instrument(ebull_test_conn, symbol="TACC1", iid=5591)
    upsert_business_sections(
        ebull_test_conn,
        instrument_id=iid,
        source_accession="acc-old",
        sections=(_section("Old body"),),
    )
    upsert_business_sections(
        ebull_test_conn,
        instrument_id=iid,
        source_accession="acc-new",
        sections=(_section("New body"),),
    )
    rows = get_business_sections(ebull_test_conn, instrument_id=iid)
    assert len(rows) == 1
    assert rows[0].body == "New body"
    assert rows[0].source_accession == "acc-new"


@pytest.mark.integration
def test_get_with_accession_returns_that_filings_sections(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """accession='acc-old' → returns the older snapshot, not the latest."""
    iid = _seed_instrument(ebull_test_conn, symbol="TACC2", iid=5592)
    upsert_business_sections(
        ebull_test_conn,
        instrument_id=iid,
        source_accession="acc-old",
        sections=(_section("Old body"),),
    )
    upsert_business_sections(
        ebull_test_conn,
        instrument_id=iid,
        source_accession="acc-new",
        sections=(_section("New body"),),
    )
    rows = get_business_sections(ebull_test_conn, instrument_id=iid, accession="acc-old")
    assert len(rows) == 1
    assert rows[0].body == "Old body"
    assert rows[0].source_accession == "acc-old"
