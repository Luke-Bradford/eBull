"""Tests for the Atom feed fast-lane job (#867)."""

from __future__ import annotations

import psycopg
import pytest

from app.jobs.sec_atom_fast_lane import (
    ResolvedSubject,
    run_atom_fast_lane,
)
from app.providers.implementations.sec_getcurrent import parse_getcurrent_atom
from app.services.sec_manifest import get_manifest_row
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_ATOM_SAMPLE = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <updated>2026-04-30T16:30:00-04:00</updated>
  <entry>
    <title>4 - Apple Inc. (0000320193) (Filer)</title>
    <updated>2026-04-30T16:30:00-04:00</updated>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/320193/0000320193-26-000042-index.htm"/>
    <category term="4" />
    <id>urn:tag:sec.gov,2008:accession-number=0000320193-26-000042</id>
  </entry>
  <entry>
    <title>13F-HR - BlackRock Inc. (0001364742) (Filer)</title>
    <updated>2026-04-30T16:00:00-04:00</updated>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/1364742/0001364742-26-000099-index.htm"/>
    <category term="13F-HR" />
    <id>urn:tag:sec.gov,2008:accession-number=0001364742-26-000099</id>
  </entry>
  <entry>
    <title>S-1 - Some IPO Co (0009999999) (Filer)</title>
    <updated>2026-04-30T15:00:00-04:00</updated>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/9999999/0009999999-26-000001-index.htm"/>
    <category term="S-1" />
    <id>urn:tag:sec.gov,2008:accession-number=0009999999-26-000001</id>
  </entry>
</feed>
"""


def _fake_get(status: int, body: bytes):
    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return status, body

    return _impl


class TestParser:
    def test_parses_atom_entries(self) -> None:
        rows = list(parse_getcurrent_atom(_ATOM_SAMPLE))
        assert len(rows) == 3
        assert rows[0].accession_number == "0000320193-26-000042"
        assert rows[0].cik == "0000320193"
        assert rows[0].form == "4"
        assert rows[0].source == "sec_form4"
        assert rows[1].accession_number == "0001364742-26-000099"
        assert rows[1].cik == "0001364742"
        assert rows[1].source == "sec_13f_hr"
        assert rows[2].source is None  # S-1 not in source enum

    def test_skips_malformed_xml(self) -> None:
        rows = list(parse_getcurrent_atom(b"not xml"))
        assert rows == []


class TestFastLaneJob:
    def test_filters_to_universe_and_upserts(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Resolver returns issuer for AAPL CIK; institutional for
        # BlackRock; None for the unknown S-1 filer.
        def resolver(conn, cik):
            if cik == "0000320193":
                return ResolvedSubject(subject_type="issuer", subject_id="1701", instrument_id=1701)
            if cik == "0001364742":
                return ResolvedSubject(subject_type="institutional_filer", subject_id=cik, instrument_id=None)
            return None

        # Seed instruments row so issuer FK is satisfied
        ebull_test_conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
            """
        )
        ebull_test_conn.commit()

        stats = run_atom_fast_lane(
            ebull_test_conn,
            http_get=_fake_get(200, _ATOM_SAMPLE),
            subject_resolver=resolver,
        )
        ebull_test_conn.commit()

        assert stats.feed_rows == 3
        assert stats.matched_in_universe == 2  # AAPL + BlackRock
        assert stats.upserted == 2
        assert stats.skipped_unmapped_form == 1  # S-1
        assert stats.skipped_unknown_subject == 0
        # S-1 is filtered before resolver; resolver returns None
        # only when source mapped but CIK not in universe.

        # Verify manifest rows
        aapl_row = get_manifest_row(ebull_test_conn, "0000320193-26-000042")
        assert aapl_row is not None
        assert aapl_row.subject_type == "issuer"
        assert aapl_row.instrument_id == 1701
        assert aapl_row.ingest_status == "pending"

        blackrock_row = get_manifest_row(ebull_test_conn, "0001364742-26-000099")
        assert blackrock_row is not None
        assert blackrock_row.subject_type == "institutional_filer"
        assert blackrock_row.instrument_id is None

    def test_atom_discovery_seeds_freshness_index(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #956: every manifest discovery write must also seed
        # data_freshness_index. Pre-fix Atom fast-lane left new
        # (subject, source) triples scheduler-invisible until the
        # next bulk seed_scheduler_from_manifest invocation.
        def resolver(conn, cik):
            if cik == "0000320193":
                return ResolvedSubject(subject_type="issuer", subject_id="1701", instrument_id=1701)
            if cik == "0001364742":
                return ResolvedSubject(subject_type="institutional_filer", subject_id=cik, instrument_id=None)
            return None

        ebull_test_conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
            """
        )
        ebull_test_conn.commit()

        run_atom_fast_lane(
            ebull_test_conn,
            http_get=_fake_get(200, _ATOM_SAMPLE),
            subject_resolver=resolver,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT subject_type, subject_id, source, state, last_known_filing_id
                FROM data_freshness_index
                ORDER BY subject_type, subject_id, source
                """
            )
            rows = cur.fetchall()

        # Both Atom-discovered triples are now scheduler-visible.
        triples = {(r[0], r[1], r[2]) for r in rows}
        assert ("issuer", "1701", "sec_form4") in triples
        assert ("institutional_filer", "0001364742", "sec_13f_hr") in triples
        # And state is 'current' (manifest evidence shows the subject
        # has filed) so the per-CIK poll picks them up.
        assert all(r[3] == "current" for r in rows)
        # last_known_filing_id matches the just-discovered accession.
        for stype, sid, _src, _state, last_id in rows:
            if (stype, sid) == ("issuer", "1701"):
                assert last_id == "0000320193-26-000042"
            elif (stype, sid) == ("institutional_filer", "0001364742"):
                assert last_id == "0001364742-26-000099"

    def test_unknown_subject_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        def resolver(conn, cik):
            return None  # everything out-of-universe

        stats = run_atom_fast_lane(
            ebull_test_conn,
            http_get=_fake_get(200, _ATOM_SAMPLE),
            subject_resolver=resolver,
        )
        ebull_test_conn.commit()
        assert stats.upserted == 0
        # AAPL form4 + BlackRock 13F both pass form filter, fail subject
        assert stats.skipped_unknown_subject == 2

    def test_idempotent_on_redelivery(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        def resolver(conn, cik):
            if cik == "0000320193":
                return ResolvedSubject(subject_type="issuer", subject_id="1701", instrument_id=1701)
            return None

        ebull_test_conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
            """
        )
        ebull_test_conn.commit()

        for _ in range(3):
            run_atom_fast_lane(
                ebull_test_conn,
                http_get=_fake_get(200, _ATOM_SAMPLE),
                subject_resolver=resolver,
            )
            ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sec_filing_manifest WHERE accession_number = %s",
                ("0000320193-26-000042",),
            )
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 1
